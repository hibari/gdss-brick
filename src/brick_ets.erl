%%%-------------------------------------------------------------------
%%% Copyright (c) 2007-2015 Hibari developers.  All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%% File    : brick_ets.erl
%%% Purpose : ETS implementation of brick_server
%%%-------------------------------------------------------------------

%% @doc An ETS implementation of brick_server.
%%
%% This implementation does not do a very good separation of concerns
%% regarding truly-ETS-specific stuff and more general brick stuff.
%% So, this module may require quite a bit of refactoring.
%%
%% At the moment, this module takes care of the following:
%% <ul>
%% <li> ETS table management for the brick. </li>
%% <li> Enforcement of the Hibari-style, within-a-brick transaction logic. </li>
%% <li> Logging updates to disk for persistence purposes (including
%%   sync updates similar to "group commit"). </li>
%% <li> Dirty key management: avoid accessing keys that are currently being
%%   logged to disk. </li>
%% </ul>
%%
%% With review and inevitable dev growth, I expect several of these
%% tasks to move out of this module, probably back into
%% `brick_server.erl'.  Also, a lot of this code is brute-force and
%% may need some beautification.
%%

-module(brick_ets).

-behaviour(gen_server).
-compile({inline_size,24}).    % 24 is the default

-include("brick.hrl").
-include("brick_public.hrl").
-include("brick_hash.hrl").
-include("gmt_hlog.hrl").

-include_lib("kernel/include/file.hrl").

-define(TIME, gmt_time_otp18).

-define(CHECK_NAME, "CHECK").
-define(CHECK_FILE, ?CHECK_NAME ++ ".LOG").

-define(FOO_TIMEOUT, 10*000).

%% "production use"?
%% -define(BIGDIR_BITS1, 8).
%% -define(BIGDIR_BITS2, 4).
%% Debug use.
-define(BIGDIR_BITS1, 3).
-define(BIGDIR_BITS2, 3).
-define(BIGDIR_BITS1_MOD, (1 bsl ?BIGDIR_BITS1)).
-define(BIGDIR_BITS2_MOD, (1 bsl ?BIGDIR_BITS2)).

-export([verbose_expiration/2]).

%% External exports
-export([start_link/2,
         stop/1,
         dump_state/1,
         sync_stats/2,
         brick_name2data_dir/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% Brick callbacks
-export([bcb_handle_do/3,
         bcb_handle_info/2,
         bcb_force_flush/1,
         bcb_async_flush_log_serial/2,
         bcb_keys_for_squidflash_priming/2,
         bcb_keys_for_squidflash_priming/3,
         bcb_squidflash_primer/3,
         bcb_log_mods/3,
         bcb_map_mods_to_storage/2,
         bcb_get_many/3,
         bcb_get_list/2, bcb_repair_loop/3,
         bcb_repair_diff_round1/3,
         bcb_repair_diff_round2/3,
         bcb_delete_remaining_keys/2,
         bcb_set_read_only_mode/2,
         bcb_get_metadata/2,
         bcb_set_metadata/3,
         bcb_delete_metadata/2,
         bcb_add_mods_to_dirty_tab/2,
         bcb_dirty_keys_in_range/3,
         bcb_retry_dirty_key_ops/1,
         bcb_lookup_key/3,
         bcb_lookup_key_storetuple/3,
         bcb_start_time/1,
         bcb_val_switcharoo/3,
         bcb_filter_mods_for_downstream/2,
         bcb_make_log_replay_noop/1,
         bcb_incr_logging_serial/1,
         bcb_peek_logging_serial/1,
         bcb_peek_logging_op_q_len/1,
         bcb_get_logging_op_q/1,
         bcb_status/1,
         bcb_common_log_sequence_file_is_bad/2
        ]).
-export([disk_log_fold/3,
         disk_log_fold_bychunk/3
        ]).
-export([externtuple_to_storetuple/1,
         storetuple_key/1,
         storetuple_val/1,
         storetuple_ts/1
        ]).

%% For gmt_hlog_common's scavenger support.
-export([really_cheap_exclusion/3,
         scavenger_get_keys/5,
         temp_path_to_seqnum/1,
         which_path/3,
         copy_one_hunk/6
        ]).

%% For brick_admin fast sync support
-export([storetuple_make/6]).

%% For gmt_hlog_common's use
-export([sequence_file_is_bad_common/6
        ]).

-export([file_input_fun/2,
         file_output_fun/1]).

%% Debug/temp/benchmarking exports
%% -export([sync_pid_start/1]).

%% -export([sort_test0/0,
%%          slurp_log_chunks/1
%%         ]).

%% -export([debug_scan/1,
%%          debug_scan/2,
%%          debug_scan2/1,
%%          debug_scan2/2,
%%          debug_scan3/2,
%%          debug_scan3/3,
%%          debug_scan4/2,
%%          debug_scan4/3
%%         ]).

%%%
%%% ETS Tables (Section 2.3.14.3 of Hibari Contributor's Guide)
%%%
%%% #state.ctab, the contents table:
%%%    All data about a key lives in this table as a "store tuple".
%%%
%%% #state.dirty_tab, the dirty table:
%%%    If a key has been updated but not yet flushed to disk, the key
%%%    appears here. Necessary for any update, inside or outside of a
%%%    micro-transaction, where race conditions are possible. See
%%%    Section 2.3.14.6, "The dirty keys table".
%%%
%%% #state.etab, the expiry table:
%%%    If a key has a non-zero expiry time associated with it (an
%%%    integer in UNIX time_t form), then the expiry time appears in
%%%    this table.
%%%
%%% #state.mdtab, the brick private metadata table:
%%%    Used for private state management during data migration and
%%%    other tasks.
%%%

%%%
%%% Request Path (write: head brick, read: official tail brick)
%%%
%%% (@TODO: Refactor the path. Functions are nested too deep and
%%%         all levels have almost the same abstruction level.
%%%         It's very hard to understand the flow.
%%%
%%%   1.  brick_server:get/5, set/7, etc.
%%%   2.    brick_server:do/5
%%%   3.      brick_server:handle_call({do, _, _, _}, _, _)
%%%   4.        brick_server:handle_call_do_prescreen/3
%%%   5.          brick_server:handle_call_do_prescreen2/3
%%%   6.            brick_server:handle_call_do/3
%%%   7.              brick_server:preprocess_fold/4  (ssf: server side fun)
%%%   8.                {ok, ...} -> brick_server:handle_call_via_impl({do, _, _, _}=Msg, _, _)
%%%  10.                    brick_ets:bcb_handle_do/6
%%%  11.                      brick_ets:do_do1b/7 (This may call squidflash_primer/3,
%%%                                                and may return {noreply_now, state()})
%%%  12.                        brick_ets:do_do2/3
%%%  13.                          brick_ets:do_txnlist/3 or do_dolist/4
%%%  14.                            brick_ets:get_key/3, set_key/6, etc.
%%%  15.  (e.g. set_key/6)            brick_ets:my_insert/6
%%%  16.                                brick_ets:my_insert2/6
%%%  17.                                  brick_ets:bigdata_dir_store_val/3
%%%  18.  (return to #16 my_insert2)    -> add a do_mod() to #state.thisdo_mod.
%%%  19.  (return to #13 do_txnlist or do_dolist)
%%%                                     -> processes all DoList and builds up #state.thisdo_mod.)
%%%       (return to #9 handle_call)
%%%  17.                    brick_ets:log_mods/2
%%%  18.                      brick_ets:write_metadata_term/4 (with IsHeadBrick=true)
%%%  19.                        brick_metadata_*:write_metadata/1
%%%  20.  (return to #9 handle_call)

%%%  21a.                   {goahead, state_r()} ->
%%%                           brick_ets:map_mods_into_ets/2
%%%  22a.                       brick_ets:my_insert_ignore_logging/3 or my_delete_ignore_logging/4
%%%  23a. (e.g. my_insert_ig..)   brick_ets:delete_prior_expirf/2
%%%  24a.                         brick_ets:my_insert_ignore_logging10/3
%%%  25a.                           brick_ets:my_insert_ignore_logging3/3
%%%  26a.                             brick_ets:exptime_insert/3
%%%  27a.                             ets:insert/2 (ctab)
%%%  28a.                     (return reply for propagating to next brick in the chain)

%%%  21b.                   {wait, brick_hlog_wal:callback_ticket(), state_r()} ->
%%%  22b.                     brick_ets:add_mods_to_dirty_tab/2
%%%  23b.                       ets:insert/2  (dirty_tab)
%%%  24b.                     Add #log_q{} to #state.logging.op_q
%%%  25b.                     (later, bcb_handle_info({wal_sync, ...}, ...) will be called,
%%%                           then, do map_mods_into_ets/2 and clear_dirty_tab/2

%%%  29. (return to #8 handle_call_via_impl)
%%%  30a.                  {up1, ToDos, OrigReturn} ->
%%%  31a.                    brick_server:do_chain_todos_iff_empty_log_q/2
%%%  32a.                      lists:foldl/3,
%%%  33a.                        brick_server:do_a_chain_todo_iff_empty_log_q/2
%%%  34a.                          brick_server:chain_send_downstream_iff_empty_log_q/6
%%%  35a.                            brick_server:chain_send_downstream/7
%%%  36a.                    brick_server:calc_original_return/2

%%%  30b.                  OrigReturn ->
%%%  31b.                    brick_server:calc_original_return/2


%%% Request path (write: middle and tail bricks)
%%%
%%%   1. brick_server:handle_cast({ch_log_replay_v2, ...})
%%%   2.   brick_server:exit_if_bad_serial_from_upstream/4
%%%   3.   brick_server:chain_do_log_replay/5
%%%   4.     brick_ets:bcb_log_mods/3
%%%   5.       brick_ets:filter_mods_from_upstream/2
%%%   6.         brick_ets:bigdata_dir_store_val/3
%%%   8.         brick_ets:write_metadata_term/4 (with IsHeadBrick=false)
%%%   9.           brick_metadata_*:write_metadata/2
%%%  10a.   {goahed, _, _} ->
%%%  11a.     brick_ets:bcb_map_mods_to_storage/2

%%%    a.     brick_server:chain_send_downstream/6

%%%  10b.   {wait, _, _} ->
%%%  11b.     brick_ets:bcb_async_flush_log_serial/2

%%%    b.     brick_ets:bcb_add_mods_to_dirty_tab/2


%%%----------------------------------------------------------------------
%%% Types/Specs/Records
%%%----------------------------------------------------------------------

-export_type([store_tuple/0,
              extern_tuple/0,
              do_mod/0,
              state_r/0
             ]).

-type proplist() :: [ atom() | {any(),any()} ].

%% @doc Use variable-sized tuples to try to avoid storing unnecessary
%% common values.
%%
%% Store tuple formats:
%% <ul>
%% <!--  1    2       3      4         5        6        -->
%% <li> {Key, TStamp, Value, ValueLen}                 </li>
%% <li> {Key, TStamp, Value, ValueLen, Flags}          </li>
%% <li> {Key, TStamp, Value, ValueLen, ExpTime}        </li>
%% <li> {Key, TStamp, Value, ValueLen, ExpTime, Flags} </li>
%% </ul>

-type storetuple_val() :: val() | {integer(), integer()}.

-type store_tuple() :: {key(), ts(), storetuple_val(), integer()} |
                       {key(), ts(), storetuple_val(), integer(), exp_time()} |
                       {key(), ts(), storetuple_val(), integer(), flags_list()} |
                       {key(), ts(), storetuple_val(), integer(), exp_time(), flags_list()}.

%% extern_tuple() is used for inter-bricks communication.
-type extern_tuple() :: {key(), ts(), storetuple_val()} |
                        {key(), ts(), storetuple_val(), exp_time(), flags_list()}.

%% an internal representation of a do op
-type do_mod() :: {insert, store_tuple()} |
                  %% KV with value_in_ram, or a table with no bigdata_dir
                  {insert_value_into_ram, store_tuple()} |
                  %% ?VALUE_REMAINS_CONSTANT
                  %% or ?VALUE_SWITCHAROO (e.g. bcb_val_switcharoo used by the scavenger)
                  {insert_constant_value, store_tuple()} |
                  %% ?KEY_SWITCHAROO (e.g. rename)
                  {insert_existing_value, store_tuple(), key(), ts()} | %% key and timestamp of OldKey

                  {delete, key(), ts(), exp_time()} |  %% Hibari 0.3 or newer
                  {delete, key(), exp_time()} |        %% Hibari 0.1.x (gdss_client still uses this)

                  %% Only used by chain migration(?)
                  {delete_noexptime, key(), ts()} |    %% Hibari 0.3 or newer
                  %% {delete_noexptime, key()} |       %% Hibari 0.1.x

                  %% Brick Privete Metadata
                  {md_insert, tuple()} |
                  {md_delete, key()} |

                  %% Log Directive
                  {log_directive, sync_override, false} |
                  {log_directive, map_sleep, non_neg_integer()} | %% non_neg_integer is for Delay
                  {log_noop}.

-record(state, {
          name :: atom(),                       % Registered name
          options :: proplist(),
          start_time :: {non_neg_integer(), non_neg_integer(), non_neg_integer()},
          do_logging = true :: boolean(),
          do_sync = true :: boolean(),
          %% log_dir :: string(),
          %% bigdata_dir :: string(),

          %% @TODO: gdss-brick >> GH9 - Refactor #state record in brick_ets
          %% The following operation counters can be moved to the process dictionary.
          n_add = 0 :: non_neg_integer(),
          n_replace = 0 :: non_neg_integer(),
          n_set = 0 :: non_neg_integer(),
          n_rename = 0 :: non_neg_integer(),
          n_get = 0 :: non_neg_integer(),
          n_get_many = 0 :: non_neg_integer(),
          n_delete = 0 :: non_neg_integer(),
          n_txn = 0 :: non_neg_integer(),
          n_do = 0 :: non_neg_integer(),
          n_expired = 0 :: non_neg_integer(),

          %% CHAIN TODO: Do these 2 items really belong here??
          logging_op_serial = 42 :: non_neg_integer(),  % Serial # of logging op
          %% Queue of logged ops for replay when log sync is done.
          %% type: gb_trees(term(), hibari_queue(#log_q{}))
          logging_op_q = gb_trees:empty() :: term(),

          %% n_log_replay = 0,
          md_store :: term(),   %% @TODO: Need a type?
          blob_store :: term(), %% @TODO: Need a type?

          %% @TODO: gdss-brick >> GH9 - Refactor #state record in brick_ets
          %% e.g. thisdo_mods and wait_on_dirty_q will be updated multiple times
          %% within a gen server call.
          %% Move this kind of frequently updated fields into a sub record, so that
          %% we don't have to shallow-copy all 45 fields is this record to a new record
          %% too many times.
          thisdo_mods :: [do_mod()],              % List of mods by this 'do'

          ctab :: table_name(),                   % ETS table for cache
          etab :: table_name(),                   % Expiry index table
          mdtab :: table_name(),                  % Private metadata table
          dirty_tab :: table_name(),              % ETS table: dirty key search
          wait_on_dirty_q :: hibari_queue(),      % Queue of ops waiting on
                                                  % dirty keys
          read_only_p = false :: boolean(),       % Mirror of upper-level's var.
          expiry_tref :: reference(),             % Timer ref for expiry events
          scavenger_pid :: 'undefined' | pid(),   % undefined | pid()
          scavenger_tref :: reference(),          % tref()

          do_expiry_fun :: fun(),                 % fun
          max_log_size = 0 :: non_neg_integer()   % max log size
         }).
-type state_r() :: #state{}.


-include("brick_specs.hrl").
-include("gmt_hlog.hrl").

-type server_ref() :: atom() | {atom(), atom()} | {global, term()} | pid().
-type log_fold_fun() :: fun((tuple(),tuple()) -> tuple()).

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------
start_link(ServerName, Options) ->
    gen_server:start_link(?MODULE, [ServerName, Options], []).

stop(Server) ->
    gen_server:call(Server, {stop}).

-spec dump_state(server_ref() | atom()) -> {integer(), list(), state_r()}.
dump_state(Server) ->
    gen_server:call(Server, {dump_state}).

sync_stats(Server, Seconds) when seconds >= 0 ->
    gen_server:call(Server, {sync_stats, Seconds}).

-spec brick_name2data_dir(atom()) -> nonempty_string().
brick_name2data_dir(ServerName) ->
    {ok, FileDir} = application:get_env(gdss_brick, brick_default_data_dir),
    filename:join([FileDir, "hlog." ++ atom_to_list(ServerName)]).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%%----------------------------------------------------------------------
init([ServerName, Options]) ->
    process_flag(trap_exit, true),
    ?E_INFO("Initializing brick server: ~w~n\t~p", [ServerName, Options]),

    %% WalMod = proplists:get_value(wal_log_module, Options, gmt_hlog_local),
    %% MdStoreModule = brick_metadata_store_leveldb,
    %% BlobStoreModule = brick_blob_store_hlog,

    %% Avoid race conditions where a quick restart of the brick
    %% catches up with the not-yet-finished death of the previous log proc.
    %% catch exit(whereis(WalMod:log_name2reg_name(ServerName)), kill),

    DoLogging = proplists:get_value(do_logging, Options, true),
    DoSync = proplists:get_value(do_sync, Options, true),
    MaxLogSize =
        case proplists:get_value(max_log_size, Options) of
            undefined ->
                {ok, Max} = application:get_env(gdss_brick, brick_max_log_size_mb),
                Max * 1024 * 1024;
            Max ->
                Max * 1024 * 1024
        end,
    %% MinLogSize =
    %%     case proplists:get_value(min_log_size, Options) of
    %%         undefined ->
    %%             {ok, Min} = application:get_env(gdss_brick, brick_min_log_size_mb),
    %%             Min * 1024 * 1024;
    %%         Min ->
    %%             Min * 1024 * 1024
    %%     end,
    %% BigDataDir = proplists:get_value(bigdata_dir, Options),

    TabName = list_to_atom(atom_to_list(ServerName) ++ "_store"),
    CTab = ets:new(TabName, [ordered_set, protected, named_table]),
    ETabName = list_to_atom(atom_to_list(ServerName) ++ "_exp"),
    ETab = ets:new(ETabName, [ordered_set, protected, named_table]),
    MDTabName = list_to_atom(atom_to_list(ServerName) ++ "_md"),
    MDTab = ets:new(MDTabName, [ordered_set, protected, named_table]),

    {ok, MdStore} = brick_metadata_store:get_metadata_store(ServerName),
    {ok, BlobStore} = brick_blob_store:get_blob_store(ServerName),

    DirtyTabName = list_to_atom(atom_to_list(ServerName) ++ "_dirty"),
    DirtyTab = ets:new(DirtyTabName, [ordered_set, protected, named_table]),
    WaitOnDirty = queue:new(),

    DefaultExpFun = fun delete_keys_immediately/2,
    DoExpFun =
        case application:get_env(gdss_brick, brick_expiration_processor) of
            {ok, "["++_ = ExpConfig} ->
                case brick_server:get_tabprop_via_str(ServerName, ExpConfig) of
                    undefined ->
                        DefaultExpFun;
                    XF ->
                        XF
                end;
            _ ->
                DefaultExpFun
        end,

    %% Brick's name vs. that brick's log's name:
    %%     brick name: X
    %%     log's directory name: hlog.X
    %%     log's registered name: X_hlog
    %% LogDir = WalMod:log_name2data_dir(ServerName),
    State0 = #state{name=ServerName,
                    options=Options,
                    start_time=?TIME:timestamp(),
                    do_logging=DoLogging,
                    do_sync=DoSync,
                    md_store=MdStore,
                    blob_store=BlobStore,
                    ctab=CTab,
                    etab=ETab,
                    mdtab=MDTab,
                    dirty_tab=DirtyTab,
                    wait_on_dirty_q=WaitOnDirty,
                    do_expiry_fun=DoExpFun,
                    max_log_size=MaxLogSize - 32 % TODO: remove fudge factor!
                   },
    self() ! do_init_second_half,
    {ok, State0}.

%%----------------------------------------------------------------------
%% Func: handle_call/3
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_call({status}, _From, State) ->
    Reply = do_status(State),
    {reply, Reply, State};
handle_call({state}, _From, State) ->
    {reply, State, State};
handle_call({first_key}, _From, State) ->
    case ets:first(State#state.ctab) of
        '$end_of_table' ->
            {reply, key_not_exist, State};
        Key ->
            {reply, {ok, Key}, State}
        end;
handle_call({last_key}, _From, State) ->
    case ets:last(State#state.ctab) of
        '$end_of_table' ->
            {reply, key_not_exist, State};
        Key ->
            {reply, {ok, Key}, State}
        end;
handle_call({flush_all}, _From, State) ->
    {Reply, NewState} = do_flush_all(State),
    {reply, Reply, NewState};
handle_call({dump_state}, _From, State) ->
    L = ets:tab2list(State#state.ctab),
    {reply, {length(L), L, State}, State};
handle_call({set_do_sync, NewValue}, _From, State) ->
    {reply, State#state.do_sync, State#state{do_sync = NewValue}};
handle_call({set_do_logging, NewValue}, _From, State) ->
    {reply, State#state.do_logging, State#state{do_logging = NewValue}};
handle_call(Request, _From, State) ->
    ?E_ERROR("handle_call: Request = ~P", [Request, 20]),
    Reply = err,
    {reply, Reply, State}.

%%----------------------------------------------------------------------
%% Func: handle_cast/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_cast({incr_expired, Amount}, #state{n_expired = N} = State) ->
    {noreply, State#state{n_expired = N + Amount}};
handle_cast(Msg, State) ->
    ?E_ERROR("handle_cast: ~p: Msg = ~P", [State#state.name, Msg, 20]),
    exit({handle_cast, Msg}),                   % QQQ TODO debugging only
    {noreply, State}.

%%----------------------------------------------------------------------
%% Func: handle_info/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_info(check_expiry, State) ->
    case whereis(brick_server:make_expiry_regname(State#state.name)) of
        undefined ->
            {noreply, do_check_expiry(State)};
        _ ->
            {noreply, State}
    end;
handle_info({'EXIT', Pid, Done}, State)
  when Pid =:= State#state.scavenger_pid andalso
       (Done =:= done orelse element(3, Done) =:= done) -> % smart_exceptions
    {noreply, State#state{scavenger_pid = undefined}};
handle_info({'EXIT', Pid, Reason}, State)
  when Pid =:= State#state.scavenger_pid ->
    ?E_WARNING("~p: scavenger ~p exited with ~p",
               [State#state.name, State#state.scavenger_pid, Reason]),
    {noreply, State#state{scavenger_pid = undefined}};
%% handle_info({'EXIT', Pid, Reason}, State) when Pid =:= State#state.log ->
%%     ?E_WARNING("~p: log process ~p exited with ~p",
%%                [State#state.name, State#state.log, Reason]),
%%     {stop, Reason, State};
handle_info(_Info, State) ->
    ?E_ERROR("Unhandled handle_info message: ~P (~p)", [_Info, 20, ?MODULE]),
    {noreply, State}.

%%----------------------------------------------------------------------
%% Func: terminate/2
%% Purpose: Shutdown the server
%% Returns: any (ignored by gen_server)
%%----------------------------------------------------------------------
terminate(Reason, #state{name=Name, ctab=CTab, etab=ETab,
                         md_store=_MdStore, blob_store=_BlobStore}) ->
    ?E_INFO("Stopping brick server: ~w, reason ~w", [Name, Reason]),
    catch ets:delete(CTab),
    catch ets:delete(ETab),
    %% @TODO (new hlog) Temporary disabled.
    %% catch brick_metadata_store:stop(MdStorePid),
    %% catch brick_blob_store:stop(BlobStorePid),
    ok.

%%----------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%%----------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

do_status(S) ->
    {ok, [
          {start_time, S#state.start_time},
          {options, S#state.options},
          {ets, [{A, ets:info(S#state.ctab, A)} || A <- [size, memory]]},
          {ets_table_name, S#state.ctab},
          {do_logging, S#state.do_logging},
          {do_sync, S#state.do_sync},
          {n_add, S#state.n_add},
          {n_replace, S#state.n_replace},
          {n_set, S#state.n_set},
          {n_rename, S#state.n_rename},
          {n_get, S#state.n_get},
          {n_get_many, S#state.n_get_many},
          {n_delete, S#state.n_delete},
          {n_txn, S#state.n_txn},
          {n_do, S#state.n_do},
          {n_expired, S#state.n_expired},
          {wait_on_dirty_q, S#state.wait_on_dirty_q}
         ]}.

do_flush_all(#state{ctab=CTab, etab=ETab}) ->
    ?DBG_GEN("Deleting all table data!", []),
    ets:delete_all_objects(CTab),
    ets:delete_all_objects(ETab).

%% @doc Do a 'do', phase 1: Check to see if all keys are local to this brick.
-spec do_do(do_op_list(), flags_list(), state_r(), check_dirty|false) ->
                   has_dirty_keys | {no_dirty_keys, {do_res(), state_r()}}.
do_do(DoList, DoFlags, State, CheckDirty) ->
    try
        do_do1b(DoList, DoFlags, State, CheckDirty)
    catch
        throw:silently_drop_reply -> % Instead, catch 1 caller higher? {shrug}
            silently_drop_reply
    end.

%% @doc Do a 'do', phase 1b: Check for any dirty keys.

do_do1b(DoList, DoFlags, State, check_dirty)
  when State#state.do_logging =:= true ->
    DoKeys = brick_server:harvest_do_keys(DoList),
    case any_dirty_keys_p(DoKeys, State) of
        true ->
            has_dirty_keys;
        false ->
            {no_dirty_keys, do_do2(DoList, DoFlags, State)}
    end;
do_do1b(DoList, DoFlags, State, _) ->
    {no_dirty_keys, do_do2(DoList, DoFlags, State)}.

%% @doc Do a 'do', phase 2: If a 'txn' is present, check txn preconditions,
%% then (in all cases) perform the do's.

do_do2([txn|L], DoFlags, State) ->
    %% do_txnlist checks to see if a transaction's conditions are
    %% OK.  If all are OK, then do_txnlist() will call do_dolist()
    %% to apply the changes.
    do_txnlist(L, DoFlags, State);
do_do2(L, DoFlags, State) ->
    do_dolist(L, State, DoFlags, []).

%%% TODO Good candidate for replacement with lists:map() or fold*()?
do_dolist([], State, _DoFlags, Acc) ->
    {lists:reverse(Acc), State};
do_dolist([{add, Key, Timestamp, Value, ExpTime, Flags} | T], State, DoFlags, Acc) ->
    EffTimestamp = make_timestamp_if_zero(Timestamp),
    {Res, NewState} = add_key(Key, EffTimestamp, Value, ExpTime, Flags, State),
    N_add = NewState#state.n_add,
    do_dolist(T, NewState#state{n_add = N_add + 1}, DoFlags, [Res|Acc]);
do_dolist([{replace, Key, Timestamp, Value, ExpTime, Flags}|T], State, DoFlags, Acc) ->
    EffTimestamp = make_timestamp_if_zero(Timestamp),
    {Res, NewState} = replace_key(Key, EffTimestamp, Value, ExpTime, Flags, State),
    N_replace = NewState#state.n_replace,
    do_dolist(T, NewState#state{n_replace = N_replace + 1}, DoFlags, [Res|Acc]);
do_dolist([{set, Key, Timestamp, Value, ExpTime, Flags} | T], State, DoFlags, Acc) ->
    EffTimestamp = make_timestamp_if_zero(Timestamp),
    {Res, NewState} = set_key(Key, EffTimestamp, Value, ExpTime, Flags, State),
    N_set = NewState#state.n_set,
    do_dolist(T, NewState#state{n_set = N_set + 1}, DoFlags, [Res|Acc]);
do_dolist([{rename, Key, Timestamp, NewKey, ExpTime, Flags} | T], State, DoFlags, Acc) ->
    EffTimestamp = make_timestamp_if_zero(Timestamp),
    {Res, NewState} = rename_key(Key, EffTimestamp, NewKey, ExpTime, Flags, State),
    N_rename = NewState#state.n_rename,
    do_dolist(T, NewState#state{n_rename = N_rename + 1}, DoFlags, [Res|Acc]);
do_dolist([{get, Key, Flags} | T], State, DoFlags, Acc) ->
    Res = get_key(Key, Flags, State),
    N_get = State#state.n_get,
    do_dolist(T, State#state{n_get = N_get + 1}, DoFlags, [Res|Acc]);
do_dolist([{delete, Key, Flags} | T], State, DoFlags, Acc) ->
    %% This {delete, key(), flags()} is an old form for Hibari 0.1
    %% but brick_simple is still using it.
    EffTimestamp = make_timestamp_if_zero(0),
    {Res, NewState} = delete_key(Key, EffTimestamp, Flags, State),
    N_delete = NewState#state.n_delete,
    do_dolist(T, NewState#state{n_delete = N_delete + 1}, DoFlags, [Res|Acc]);
do_dolist([{delete, Key, Timestamp, Flags} | T], State, DoFlags, Acc) ->
    EffTimestamp = make_timestamp_if_zero(Timestamp),
    {Res, NewState} = delete_key(Key, EffTimestamp, Flags, State),
    N_delete = NewState#state.n_delete,
    do_dolist(T, NewState#state{n_delete = N_delete + 1}, DoFlags, [Res|Acc]);
do_dolist([{get_many, Key, Flags} | T], State, DoFlags, Acc) ->
    {Res0, NewState} = get_many1(Key, Flags, true, DoFlags, State),
    Res = {ok, Res0},
    N_get_many = NewState#state.n_get_many,
    do_dolist(T, NewState#state{n_get_many = N_get_many + 1}, DoFlags, [Res|Acc]);
do_dolist([next_op_is_silent, DoOp | T], State, DoFlags, Acc) ->
    {_ResList, NewState} = do_dolist([DoOp], State, DoFlags, []),
    do_dolist(T, NewState, DoFlags, Acc);
do_dolist([Unknown | T], State, DoFlags, Acc) ->
    %% Unknown is an error code from a do_txnlist() step, or perhaps
    %% some other return/status code ... pass it on.
    do_dolist(T, State, DoFlags, [Unknown|Acc]).

-spec make_timestamp_if_zero(ts()) -> ts().
make_timestamp_if_zero(0) ->
    brick_server:make_timestamp();
make_timestamp_if_zero(Timestamp) ->
    Timestamp.

do_txnlist(L, DoFlags, State) ->
    %% It's a bit clumsy to have a separate accumulator for errors
    %% (arg 6), but I guess we'll live with it.
    do_txnlist(L, State, [], true, 1, DoFlags, []).

do_txnlist([], State, Acc, true, _N, DoFlags, []) ->
    N_txn = State#state.n_txn,
    {_, Reply} = do_do(lists:reverse(Acc), DoFlags,
                       State#state{n_txn=N_txn + 1}, false),
    Reply;
do_txnlist([], State, _Acc, false, _, _DoFlags, ErrAcc) ->
    {{txn_fail, lists:reverse(ErrAcc)}, State};
do_txnlist([{add, Key, TStamp, _Value, _ExpTime, Flags} = H|T], State,
           Acc, Good, N, DoFlags, ErrAcc) ->
    if TStamp =:= 0 ->
            EffTStamp = brick_server:make_timestamp(),
            EffH = setelement(3, H, EffTStamp);
       true ->
            EffH = H
    end,
    case key_exists_p(Key, Flags, State) of
        {Key, TS, _, _, _, _} ->
            Err = {key_exists, TS},
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc]);
        {ts_error, _} = Err ->
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc]);
        _ ->
            do_txnlist(T, State, [EffH|Acc], Good, N+1, DoFlags, ErrAcc)
    end;
do_txnlist([{replace, Key, TStamp, Value, _ExpTime, Flags} = H|T], State,
           Acc, Good, N, DoFlags, ErrAcc) ->
    if TStamp =:= 0 ->
            EffTStamp = brick_server:make_timestamp(),
            EffH = setelement(3, H, EffTStamp);
       true ->
            EffTStamp = TStamp,
            EffH = H
    end,
    case key_exists_p(Key, Flags, State, true, EffTStamp, Value) of
        {Key, _, _, _, _, _} ->
            do_txnlist(T, State, [EffH|Acc], Good, N+1, DoFlags, ErrAcc);
        {ts_error, _} = Err ->
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc]);
        _ ->
            Err = key_not_exist,
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc])
    end;
do_txnlist([{set, Key, TStamp, Value, _ExpTime, Flags} = H|T], State,
           Acc, Good, N, DoFlags, ErrAcc) ->
    if TStamp =:= 0 ->
            EffTStamp = brick_server:make_timestamp(),
            EffH = setelement(3, H, EffTStamp);
       true ->
            EffTStamp = TStamp,
            EffH = H
    end,
    Num = lists:foldl(fun(Flag, Sum) -> case check_flaglist(Flag, Flags) of
                                            {true, _} -> Sum + 1;
                                            _         -> Sum
                                        end
                      end, 0, [must_exist, must_not_exist]),
    if Num =:= 0 ->
            case key_exists_p(Key, Flags, State, false, EffTStamp, Value) of
                {ts_error, _} = Err ->
                    do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc]);
                _ ->
                    do_txnlist(T, State, [EffH|Acc], Good, N+1, DoFlags, ErrAcc)
            end;
       true ->
            Err = invalid_flag_present,
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc])
    end;
do_txnlist([{rename, Key, TStamp, NewKey, _ExpTime, Flags} = H|T], State,
           Acc, Good, N, DoFlags, ErrAcc) ->
    if TStamp =:= 0 ->
            EffTStamp = brick_server:make_timestamp(),
            EffH = setelement(3, H, EffTStamp);
       true ->
            EffTStamp = TStamp,
            EffH = H
    end,
    case key_exists_p(Key, Flags, State, true, EffTStamp, NewKey) of
        {NewKey, _, _, _, _, _} ->
            %% special case when Key =:= NewKey
            Err = key_not_exist,
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc]);
        {Key, _, _, _, _, _} ->
            do_txnlist(T, State, [EffH|Acc], Good, N+1, DoFlags, ErrAcc);
        {ts_error, _} = Err ->
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc]);
        _ ->
            Err = key_not_exist,
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc])
    end;
do_txnlist([{Primitive, Key, Flags} = H|T], State, Acc, Good, N, DoFlags, ErrAcc)
  when Primitive =:= get ; Primitive =:= delete ; Primitive =:= get_many ->
    case common_testset_must_mustnot(Key, Flags, State) of
        ok ->
            do_txnlist(T, State, [H|Acc], Good, N+1, DoFlags, ErrAcc);
        {error, Err} ->
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc])
    end;
do_txnlist([next_op_is_silent = H|T], State, Acc, Good, N, DoFlags, ErrAcc) ->
    do_txnlist(T, State, [H|Acc], Good, N+1, DoFlags, ErrAcc);
do_txnlist([Unknown|T], State, Acc, Good, N, DoFlags, ErrAcc) ->
    %% Unknown is an error code from a do_txnlist() step, or perhaps
    %% some other return/status code ... pass it on.
    do_txnlist(T, State, [Unknown|Acc], Good, N+1, DoFlags, ErrAcc).

common_testset_must_mustnot(Key, Flags, S) ->
    case check_flaglist(testset, Flags) of
        {true, _} ->
            %% If someone specified must_not_exist and {testset, TS},
            %% then they're silly, because testset only makes sense if
            %% the record *does* exist.
            case key_exists_p(Key, Flags, S) of
                {Key, _, _, _, _, _} ->
                    ok;
                {ts_error, _} = Err ->
                    {error, Err};
                _ ->
                    {error, key_not_exist}
            end;
        _ ->
            WantedExist = case check_flaglist(must_exist, Flags) of
                              {true, _} ->
                                  true;
                              _ ->
                                  case check_flaglist(must_not_exist, Flags) of
                                      {true, _} -> false;
                                      _         -> dont_care
                                  end
                          end,
            {ActualExist, TS} =  case key_exists_p(Key, Flags, S) of
                                     {Key, TSx, _, _, _, _} -> {true, TSx};
                                     _                      -> {false, unused}
                                 end,
            if WantedExist =:= dont_care ; WantedExist =:= ActualExist ->
                    ok;
               WantedExist =:= true, ActualExist =:= false ->
                    {error, key_not_exist};
               WantedExist =:= false, ActualExist =:= true ->
                    {error, {key_exists, TS}};
               true ->
                    {error, invalid_flag_present}
            end
    end.

-spec apply_exp_time_directive(flags_list(), exp_time(), exp_time(), Default:: keep | replace) ->
                                      {ok, exp_time()} | {error, invalid_flag_present}.
apply_exp_time_directive(Flags, ExpTime, PreviousExpTime, Default) ->
    case check_flaglist(exp_time_directive, Flags) of
        false when Default =:= keep ->
            {ok, PreviousExpTime};
        false when Default =:= replace ->
            {ok, ExpTime};
        {true, keep} ->
            {ok, PreviousExpTime};
        {true, replace} ->
            {ok, ExpTime};
        {true, _} ->
            {error, invalid_flag_present}
    end.

-spec apply_attrib_directive(flags_list(), flags_list(), Default:: keep | replace) ->
                                    {ok, flags_list()} | {error, invalid_flag_present}.
apply_attrib_directive(Flags, PreviousFlags, Default) ->
    case check_flaglist(attrib_directive, Flags) of
        false when Default =:= keep ->
            apply_attrib_directive1(Flags, PreviousFlags);
        false when Default =:= replace ->
            {ok, Flags};
        {true, keep} ->
            apply_attrib_directive1(Flags, PreviousFlags);
        {true, replace} ->
            {ok, Flags};
        {true, _} ->
            {error, invalid_flag_present}
    end.

-spec apply_attrib_directive1(flags_list(), flags_list()) -> {ok, flags_list()}.
apply_attrib_directive1([], PreviousFlags) ->
    {ok, PreviousFlags};
apply_attrib_directive1(Flags, []) ->
    {ok, Flags};
apply_attrib_directive1(Flags, PreviousFlags) ->
    F = fun({Key, _}=Flag, Acc) ->
                gb_trees:enter(Key, Flag, Acc);
           (Flag, Acc) when is_atom(Flag) ->
                gb_trees:enter(Flag, Flag, Acc)
        end,
    NewFlags0 = lists:foldl(F, gb_trees:empty(), PreviousFlags),
    NewFlags1 = lists:foldl(F, NewFlags0, Flags),
    {ok, gb_trees:values(NewFlags1)}.

key_exists_p(Key, Flags, State) ->
    key_exists_p(Key, Flags, State, true).

key_exists_p(Key, Flags, State, MustHaveVal_p) ->
    key_exists_p(Key, Flags, State, MustHaveVal_p, undefined).

key_exists_p(Key, Flags, State, MustHaveVal_p, TStamp) ->
    key_exists_p(Key, Flags, State, MustHaveVal_p, TStamp, undefined).

key_exists_p(Key, Flags, State, MustHaveVal_p, TStamp, Val) ->
    case my_lookup(State, Key, MustHaveVal_p) of
        [] ->
            false;
        [StoreTuple] ->
            Key =        storetuple_key(StoreTuple),  %% NOTE: crashes if =/=
            KeyTStamp =  storetuple_ts(StoreTuple),
            case proplists:get_value(testset, Flags, KeyTStamp) of
                KeyTStamp ->
                    KeyVal =     storetuple_val(StoreTuple),
                    KeyValLen =  storetuple_vallen(StoreTuple),
                    KeyExpTime = storetuple_exptime(StoreTuple),
                    KeyFlags =   storetuple_flags(StoreTuple),

                    if TStamp =:= undefined orelse TStamp > KeyTStamp ->
                            {Key, KeyTStamp, KeyVal, KeyValLen, KeyExpTime, KeyFlags};
                       TStamp =:= KeyTStamp ->
                            if Val =:= undefined orelse Val =:= KeyVal
                               orelse element(1, Val) =:= ?VALUE_SWITCHAROO ->
                                    {Key, KeyTStamp, KeyVal, KeyValLen, KeyExpTime, KeyFlags};
                               true ->
                                    {ts_error, KeyTStamp}
                            end;
                       true ->
                            {ts_error, KeyTStamp}
                    end;
                _ ->
                    {ts_error, KeyTStamp}
            end
    end.

add_key(Key, TStamp, Value, ExpTime, Flags, State) ->
    %% TODO: Add same val checking here as replace_key() has, for idempotence?
    case key_exists_p(Key, Flags, State, false) of
        {Key, TS, _, _, _, _} ->
            {{key_exists, TS}, State};
        {ts_error, _} = Err ->
            {Err, State};
        _ ->
            case my_insert(State, Key, TStamp, Value, ExpTime, Flags) of
                {val_error, _} = Err ->
                    {Err, State};
                {{ok, _}, _} = Ok ->
                    Ok
            end
    end.

replace_key(Key, TStamp, Value, ExpTime, Flags, State) ->
    case key_exists_p(Key, Flags, State, false) of
        {Key, TS, _, _, PreviousExp, PreviousFlags} ->
            case apply_exp_time_directive(Flags, ExpTime, PreviousExp, replace) of
                {ok, NewExpTime} ->
                    case apply_attrib_directive(Flags, PreviousFlags, replace) of
                        {ok, NewFlags} ->
                            replace_key2(Key, TStamp, Value, NewExpTime, NewFlags, State, TS);
                        {error, Err} ->
                            {Err, State}
                    end;
                {error, Err} ->
                    {Err, State}
            end;
        {ts_error, _} = Err ->
            {Err, State};
        _ ->
            {key_not_exist, State}
    end.

replace_key2(Key, TStamp, Value, ExpTime, Flags, State, TS) ->
    if
        TStamp > TS ->
            case my_insert(State, Key, TStamp, Value, ExpTime, Flags) of
                {val_error, _} = Err ->
                    {Err, State};
                {{ok, _}, _} = Ok ->
                    Ok
            end;
        TStamp =:= TS, element(1, Value) =:= ?VALUE_SWITCHAROO ->
            case my_insert(State, Key, TStamp, Value, ExpTime, Flags) of
                {val_error, _} = Err ->
                    {Err, State};
                {{ok, _}, _} = Ok ->
                    Ok
            end;
        TStamp =:= TS ->
            %% We need to get the val blob.  {sigh}
            {Key, TS, Val, _, _, _} = key_exists_p(Key, Flags, State),
            if Value =:= Val ->
                    %% In this case, the new timestamp and value is
                    %% identical to what is already stored here ... so
                    %% we will return OK without changing anything.
                    {{ok, TS}, State};
               true ->
                    {{ts_error, TS}, State}
            end;
        true ->
            {{ts_error, TS}, State}
    end.

set_key(Key, TStamp, Value, ExpTime, Flags, State) ->
    case key_exists_p(Key, Flags, State, false) of
        {Key, TS, _, _, PreviousExp, PreviousFlags} ->
            %% Now, we have same case as replace_key when exists ...
            %% so reuse the lower level of replace_key().
            case apply_exp_time_directive(Flags, ExpTime, PreviousExp, replace) of
                {ok, NewExpTime} ->
                    case apply_attrib_directive(Flags, PreviousFlags, replace) of
                        {ok, NewFlags} ->
                            replace_key2(Key, TStamp, Value, NewExpTime, NewFlags, State, TS);
                        {error, Err} ->
                            {Err, State}
                    end;
                {error, Err} ->
                    {Err, State}
            end;
        {ts_error, _} = Err ->
            {Err, State};
        _X ->
            case my_insert(State, Key, TStamp, Value, ExpTime, Flags) of
                {val_error, _} = Err ->
                    {Err, State};
                {{ok, _}, _} = Ok ->
                    Ok
            end
    end.

rename_key(Key, TStamp, NewKey, ExpTime, Flags, State) ->
    case rename_key_check_oldkey(Key, TStamp, NewKey, ExpTime, Flags, State) of
        {ok, PreviousExp, PreviousFlags} ->
            case rename_key_check_newkey(Key, TStamp, NewKey, ExpTime, Flags, State) of
               ok ->
                    case rename_key_apply_params(ExpTime, PreviousExp, Flags, PreviousFlags) of
                        {ok, NewExp, NewFlags} ->
                            case is_sequence_frozen(Key, State) of
                                true ->
                                    case rename_key_with_get_set_delete(
                                           Key, TStamp, NewKey, NewExp, NewFlags, State) of
                                        {{ok, _}, _} = Ok ->
                                            Ok;
                                        Err ->
                                            Err
                                    end;
                                false ->
                                    %% utilize metadata operation
                                    case my_insert(State, Key, TStamp, {?KEY_SWITCHAROO, NewKey},
                                                   NewExp, NewFlags) of
                                        {val_error, _} = Err ->
                                            {Err, State};
                                        {{ok, _}, _} = Ok ->
                                            Ok
                                    end
                            end;
                        Err ->
                            {Err, State}
                    end;
                Err ->
                    {Err, State}
            end;
        Err ->
            {Err, State}
    end.

rename_key_check_oldkey(Key, TStamp, NewKey, _ExpTime, Flags, State) ->
    case key_exists_p(Key, Flags, State, false) of
        {NewKey, _TS, _, _, _PreviousExp, _} ->
            %% special case when Key =:= NewKey
            key_not_exist;
        {Key, TS, _, _, PreviousExp, PreviousFlags} when TStamp > TS ->
            {ok, PreviousExp, PreviousFlags};
        {Key, TS, _, _, _PreviousExp, _PreviousFlags} ->
            {ts_error, TS};
        {ts_error, _} = Err ->
            Err;
        _ ->
            key_not_exist
    end.

rename_key_check_newkey(_Key, TStamp, NewKey, _ExpTime, _Flags, State) ->
    case key_exists_p(NewKey, [], State, false) of
        false ->
            %% NewKey doesn't exist
            ok;
        {NewKey, OldTS, _, _, _, _} when TStamp > OldTS ->
            %% NewKey does exist and TStamp is OK
            ok;
        {NewKey, OldTS, _, _, _, _} ->
            %% Otherwise TStamp is not ok
            {ts_error, OldTS}
    end.

rename_key_apply_params(ExpTime, PreviousExp, Flags, PreviousFlags) ->
    case apply_exp_time_directive(Flags, ExpTime, PreviousExp, keep) of
        {ok, NewExp} ->
            case apply_attrib_directive(Flags, PreviousFlags, keep) of
                {ok, NewFlags} ->
                    {ok, NewExp, NewFlags};
                {error, Err} ->
                    Err
            end;
        {error, Err} ->
            Err
    end.

rename_key_with_get_set_delete(Key, TStamp, NewKey, ExpTime, Flags, State) ->
    %% Drop exp_time_directive and attrib_directive so that set_key/6
    %% won't be affected by these directives.
    NewFlags = lists:filter(
                 fun({exp_time_directive, _}) -> false;
                    ({attrib_directive,   _}) -> false;
                    (_)                       -> true
                 end, Flags),

    %% Get the old value. It should have been loaded to RAM by
    %% the squidflash primer.
    [StoreTuple] = my_lookup(State, Key, true),
    Value = storetuple_val(StoreTuple),
    case set_key(NewKey, TStamp, Value, ExpTime, NewFlags, State) of
        {{ok, _}, NewState1} ->
            DeleteTS = brick_server:make_timestamp(),
            case delete_key(Key, DeleteTS, [], NewState1) of
                {ok, NewState2} ->
                    {{ok, TStamp}, NewState2};
                Err ->
                    Err
            end;
        Err ->
            Err
    end.

get_key(Key, Flags, State) ->
    case key_exists_p(Key, Flags, State) of
        {Key, KeyTStamp, Value, ValueLen, ExpTime, KeyFlags} ->
            GetAllAttribs = check_flaglist(get_all_attribs, Flags),
            case check_flaglist(witness, Flags) of
                {true, _} ->
                    case GetAllAttribs of
                        false ->
                            {ok, KeyTStamp};
                        {true, _} ->
                            Flags2 = [{val_len, ValueLen}|KeyFlags],
                            {ok, KeyTStamp, ExpTime, Flags2}
                    end;
                _ ->
                    case GetAllAttribs of
                        false ->
                            {ok, KeyTStamp, Value};
                        {true, _} ->
                            Flags2 = [{val_len, ValueLen}|KeyFlags],
                            {ok, KeyTStamp, Value, ExpTime, Flags2}
                    end
            end;
        {ts_error, _} = Err ->
            Err;
        _ ->
            key_not_exist
    end.

%% @spec (term(), list(), boolean(), proplist(), state_r()) ->
%%       {{list(), true | false}, state_r()}
%% @doc Get a list of keys, starting with Key, in order specified by
%%      IncreasingOrderp.
%%
%% ```
%%  |<--                         Total Key Space                         -->|
%%  |                      SweepA            SweepZ                         |
%%  |                      |<--  "Sweep Zone"  -->|                         |
%%  |    |<- Case A ->|    .                      .                         |
%%  |               |<- Case B ->|                .                         |
%%  |                      .    |<- Case C ->|    .                         |
%%  |                      .               |<- Case D ->|                   |
%%  |                      .                      .       |<- Case E ->|    |
%%  |                 |<-  .        Case F        . qq->|                   |
%%  |                      .                      .                         |
%% '''
%%
%% Here is an enumeration of all the get_many scenarios relative to where the
%% "sweep zone" is located.  Note that `SweepA' and `SweepZ' are individual
%% keys.
%% <ul>
%% <li> Case A ... Start key says to forward the request to the destination
%%                 chain.  Query can be safely handled by the tail of the
%%                 destination chain.  No problem.  </li>
%% <li> Case B ... Start key says to forward the request to the destination
%%                 chain.  Keys in the first half (`< SweepA') are
%%                 no problem here.  Keys in the last half (`> SweepA') are
%%                 inside the sweep zone; keys in the sweep zone are
%%                 frozen/dirty, so also no problem here. </li>
%% <li> Case C ... Start key is inside the sweep zone: brick will forward
%%                 query to itself until the sweep zone moves.  No problem.</li>
%% <li> Case D ... Same as Case C, no problem. </li>
%% <li> Case E ... Start key says to forward the request to the source chain.
%%                 Query can be safely handled by the tail of the source
%%                 chain.  No problem.  </li>
%% <li> Case F ... Start key says to forward the request to the destination
%%                 chain.  Problem here: destination chain doesn't have
%%                 correct set of keys in the area marked by `qq'.   </li>
%% </ul>
%%
%% Therefore, for Case F, we must remove all keys that fall inside the bad
%% `qq' range.

get_many1(Key, Flags, IncreasingOrderP, DoFlags, State) ->
    Result = {{Rs, _Bool}, NewState} =
        get_many1b(Key, Flags, IncreasingOrderP, State),
    case {Rs, proplists:get_value(internal___sweep_zone___, DoFlags)} of
        {[], _} ->
            Result;
        {_, undefined} ->
            Result;
        {_, {SweepA, SweepZ}} ->
            %% Hint: we know Rs =/= [].
            %%
            %% Now we need to check ... if the first key is =< than SweepA,
            %% and if the last key is greater than SweepZ, then we need to
            %% filter out all keys >= SweepZ.  This is case 'F', see above.
            Ks = [element(1, Tuple) || Tuple <- Rs],
            case {hd(Ks) =< SweepA, lists:last(Ks) >= SweepZ} of
                {true, true} ->
                    Rs2 = lists:takewhile(
                            fun(T) when element(1, T) =< SweepZ -> true;
                               (_)                              -> false
                            end, Rs),
                    %% io:format("DROP: sweep ~p - ~p", [SweepA, SweepZ]),
                    %% io:format("DROP: ~P", [[KKK || {KKK, _} <- (Rs -- Rs2)], 10]),
                    {{Rs2, true}, NewState};
                _ ->
                    Result
            end
    end.
-spec get_many1b(key(), flags_list(), boolean(), tuple()) ->
                        {{list(extern_tuple()), boolean()}, tuple()}.
get_many1b(Key, Flags, IncreasingOrderP, State) ->
    MaxNum =
        case proplists:get_value(max_num, Flags, 10) of
            N when is_integer(N) ->
                N;
            _ ->
                10
        end,
    DoWitnessP =
        case check_flaglist(witness, Flags) of
            {true, true} ->
                witness;
            _ ->
                undefined
        end,
    DoAllAttrP =
        case check_flaglist(get_all_attribs, Flags) of
            {true, true} ->
                all_attribs;
            _ ->
                undefined
        end,
    RawStoreTupleP =
        case check_flaglist(get_many_raw_storetuples, Flags) of
            {true, true} ->
                raw_storetuples;
            _ ->
                undefined
        end,
    ResultFlavor = {DoWitnessP, DoAllAttrP, RawStoreTupleP},
    BinPrefix =
        case proplists:get_value(binary_prefix, Flags) of
            Prefix when is_binary(Prefix) ->
                {size(Prefix), Prefix};
            _ ->
                undefined
        end,
    MaxBytes =
        case proplists:get_value(max_bytes, Flags) of
            N2 when is_integer(N2) ->
                N2;
            _ ->
                2 * 1024 * 1024 * 1024
        end,
    ScanDirection =
        case check_flaglist(reversed, Flags) of
            {true, true} ->
                backward;
            _ ->
                forward
        end,
    %% Whether the start key is inclusive or not. (Default: exclusive)
    IncludeStartKeyP =
        case check_flaglist(include_start_key, Flags) of
            {true, true} ->
                include_start_key;
            _ ->
                undefined
        end,

    Res = get_many2(ets_start_key(State#state.ctab, Key, ScanDirection, IncludeStartKeyP),
                    MaxNum, MaxBytes, ResultFlavor,
                    BinPrefix, ScanDirection, [], State),

    if
        IncreasingOrderP ->        %% dialyzer: can never succeed
            %% ManyList is backward, must reverse it.
            {{ManyList, TorF}, State2} = Res,
            {{lists:reverse(ManyList), TorF}, State2};
        true ->
            %% Result list is backward, that's OK.
            Res
    end.

%% @doc Get a list of keys, starting with Key, in increasing order
%%
%% The tuples returned here are in external tuple format.
%%
%% NOTE: We are *supposed* to return our results in reversed order.

-spec get_many2(key() | '$end_of_table',
                non_neg_integer(), non_neg_integer(),
                {'witness'|'undefined', 'all_attribs'|'undefined', 'raw_storetuples'|'undefined'},
                {non_neg_integer(), binary()}|'undefined', 'forward'|'backward',
                Acc::[extern_tuple()], State::term()) ->
                       {{[extern_tuple()], boolean()}, term()}.
get_many2('$end_of_table', _MaxNum, _MaxBytes, _ResultFlavor, _BPref, _ScanDirection, Acc, State) ->
    {{Acc, false}, State};
get_many2(_Key, 0, _MaxBytes, _ResultFlavor, _BPref, _ScanDirection, Acc, State) ->
    {{Acc, true}, State};
get_many2(Key, MaxNum, MaxBytes, ResultFlavor, BPref, ScanDirection, Acc, State)
  when MaxBytes > 0 ->
    Cont = case BPref of
               undefined ->
                   ok;
               {PrefixLen, Prefix} ->
                   case Key of
                       <<Prefix:PrefixLen/binary, _Rest/binary>> ->
                           ok;
                       _ ->
                           skip
                   end
           end,
    case Cont of
        ok ->
            CTab = State#state.ctab,
            [Tuple] = ets:lookup(CTab, Key),
            Bytes = storetuple_vallen(Tuple),
            Item = make_many_result(Tuple, ResultFlavor, State),
            get_many2(ets_next_key(CTab, Key, ScanDirection), MaxNum - 1,
                      MaxBytes - Bytes, ResultFlavor,
                      BPref, ScanDirection, [Item|Acc], State);
        skip ->
            get_many2('$end_of_table', MaxNum, MaxBytes, ResultFlavor,
                      BPref, ScanDirection, Acc, State)
    end;
get_many2(_Key, _MaxNum, _MaxBytes, _ResultFlavor, _BPref, _ScanDirection, Acc, State) ->
    {{Acc, true}, State}.

-spec ets_start_key(atom(), key(),
                    'forward' | 'backward',
                    'include_start_key' | undefined) -> key().
ets_start_key(Tab, ?BRICK__GET_MANY_FIRST, forward, _) ->
    ets:first(Tab);
ets_start_key(Tab, Key, forward, include_start_key) ->
    case ets:member(Tab, Key) of
        true ->
            Key;
        false ->
            ets:next(Tab, Key)
    end;
ets_start_key(Tab, Key, forward, undefined) ->
    ets:next(Tab, Key);
ets_start_key(Tab, Key, backward, include_start_key) ->
    case ets:member(Tab, Key) of
        true ->
            Key;
        false ->
            ets:prev(Tab, Key)
    end;
ets_start_key(Tab, Key, backward, undefined) ->
    ets:prev(Tab, Key).

-spec ets_next_key(atom(), key(), 'forward' | 'backword') -> key() | '$end_of_table'.
ets_next_key(Tab, Key, forward) ->
    ets:next(Tab, Key);
ets_next_key(Tab, Key, backward) ->
    ets:prev(Tab, Key).

%% @doc Create the result tuple for a single get_many result.
%%
%% Note about separation of ETS and disk storage:
%% the get_many iterators, in both the normal case and the shadowtab
%% case, are free to use ets:lookup().  If a get_many request is
%% 'witness' flavored, then we we most definitely do not want to hit the
%% disk for a tuple val.
%%
%% Arg note: (StoreTuple, {DoWitnessP, DoAllAttrP, RawStoreTupleP}, State).

-spec make_many_result(store_tuple(),
                       {'witness'|'undefined', 'all_attribs'|'undefined', 'raw_storetuples'|'undefined'},
                       State::term()) -> extern_tuple().
make_many_result(StoreTuple, {witness, undefined, undefined}, _S) ->
    {storetuple_key(StoreTuple), storetuple_ts(StoreTuple)};
make_many_result(StoreTuple, {witness, all_attribs, undefined}, S) ->
    {storetuple_key(StoreTuple), storetuple_ts(StoreTuple),
     make_extern_flags(storetuple_val(StoreTuple),
                       storetuple_vallen(StoreTuple),
                       storetuple_flags(StoreTuple), S)};
make_many_result(StoreTuple, {undefined, undefined, undefined}, S) ->
    make_many_result2(StoreTuple, S);
make_many_result(StoreTuple, {undefined, all_attribs, undefined}, S) ->
    make_many_result3(StoreTuple, S);
make_many_result(StoreTuple, {_DoWitness, _DoAllAttr, raw_storetuples}, _S) ->
    StoreTuple.

make_many_result2(StoreTuple, S) ->
    {Key, TStamp, Val0, _ExpTime, _Flags} =
        storetuple_to_externtuple(StoreTuple, S),
    ValLen = storetuple_vallen(StoreTuple),
    Val = bigdata_dir_get_val(Key, Val0, ValLen, S),
    {Key, TStamp, Val}.

make_many_result3(StoreTuple, S) ->
    {Key, TStamp, Val0, ExpTime, Flags} =
        storetuple_to_externtuple(StoreTuple, S),
    ValLen = storetuple_vallen(StoreTuple),
    Val = bigdata_dir_get_val(Key, Val0, ValLen, S),
    {Key, TStamp, Val, ExpTime, Flags}.

delete_key(Key, Timestamp, Flags, State) ->
    case key_exists_p(Key, Flags, State, false) of
        {Key, _KeyTStamp, _Value, _ValueLen, ExpTime, _} ->
            NewState = my_delete(State, Key, Timestamp, ExpTime),
            {ok, NewState};
        {ts_error, _} = Err ->
            {Err, State};
        _ ->
            {key_not_exist, State}
    end.

%% TODO: Really, this could be proplists:something(), right?

check_flaglist(_Flag, []) ->
    false;
check_flaglist(Flag, [Flag|_T]) ->
    {true, true};
check_flaglist(Flag, [{Flag,FlagVal}|_T]) ->
    {true, FlagVal};
check_flaglist(Flag, [_H|T]) ->
    check_flaglist(Flag, T).

-spec storetuple_make(key(), ts(), term(), integer(), exp_time(), flags_list()) -> store_tuple().
storetuple_make(Key, TStamp, Value, ValueLen, 0, []) ->
    {Key, TStamp, Value, ValueLen};
storetuple_make(Key, TStamp, Value, ValueLen, 0, Flags)
  when is_list(Flags), Flags =/= [] ->
    {Key, TStamp, Value, ValueLen, Flags};
storetuple_make(Key, TStamp, Value, ValueLen, ExpTime, [])
  when is_integer(ExpTime), ExpTime =/= 0 ->
    {Key, TStamp, Value, ValueLen, ExpTime};
storetuple_make(Key, TStamp, Value, ValueLen, ExpTime, Flags) ->
    {Key, TStamp, Value, ValueLen, ExpTime, Flags}.

-spec storetuple_replace_val(store_tuple(), storetuple_val()) -> store_tuple().
storetuple_replace_val(StoreTuple, NewVal) ->
    setelement(3, StoreTuple, NewVal).

storetuple_replace_flags({Key, TStamp, Value, ValueLen}, NewFlags) ->
    storetuple_make(Key, TStamp, Value, ValueLen, 0, NewFlags);
storetuple_replace_flags({Key, TStamp, Value, ValueLen, OldFlags},
                         NewFlags) when is_list(OldFlags) ->
    storetuple_make(Key, TStamp, Value, ValueLen, 0, NewFlags);
storetuple_replace_flags({Key, TStamp, Value, ValueLen, ExpTime},
                         NewFlags) when is_integer(ExpTime) ->
    storetuple_make(Key, TStamp, Value, ValueLen, ExpTime, NewFlags);
storetuple_replace_flags({Key, TStamp, Value, ValueLen, ExpTime, _OldFlags},
                         NewFlags) ->
    storetuple_make(Key, TStamp, Value, ValueLen, ExpTime, NewFlags).

-spec storetuple_key(store_tuple() | extern_tuple() ) -> key().
storetuple_key(StoreTuple) ->
    element(1, StoreTuple).

-spec storetuple_ts(store_tuple()) -> ts().
storetuple_ts(StoreTuple) ->
    element(2, StoreTuple).

-spec storetuple_val(store_tuple()) -> storetuple_val().
storetuple_val(StoreTuple) ->
    element(3, StoreTuple).

-spec storetuple_vallen(store_tuple()) -> integer().
storetuple_vallen(StoreTuple) ->
    element(4, StoreTuple).

%% storetuple_vallen(StoreTuple) when is_tuple(element(3, StoreTuple)) ->
%%     element(4, StoreTuple);
%% storetuple_vallen(StoreTuple) when is_binary(element(3, StoreTuple)) ->
%%     size(element(3, StoreTuple)).

storetuple_exptime({_, _, _, _, ExpTime}) when is_integer(ExpTime) ->
    ExpTime;
storetuple_exptime({_, _, _, _, ExpTime, _}) ->
    ExpTime;
storetuple_exptime(_) ->
    0.

storetuple_flags({_, _, _, _, Flags}) when is_list(Flags) ->
    Flags;
storetuple_flags({_, _, _, _, _, Flags}) ->
    Flags;
storetuple_flags(_) ->
    [].

storetuple_to_externtuple({Key, TStamp, Value, ValueLen}, S) ->
    {Key, TStamp, Value, 0, make_extern_flags(Value, ValueLen, [], S)};
storetuple_to_externtuple({Key, TStamp, Value, ValueLen, Flags}, S)
  when is_list(Flags) ->
    {Key, TStamp, Value, 0, make_extern_flags(Value, ValueLen, Flags, S)};
storetuple_to_externtuple({Key, TStamp, Value, ValueLen, ExpTime}, S)
  when is_integer(ExpTime) ->
    {Key, TStamp, Value, ExpTime, make_extern_flags(Value, ValueLen, [], S)};
storetuple_to_externtuple({Key, TStamp, Value, ValueLen, ExpTime, Flags}, S) ->
    {Key, TStamp, Value, ExpTime, make_extern_flags(Value, ValueLen, Flags, S)}.

make_extern_flags(_Value, ValueLen, Flags, _S) ->
    [{val_len, ValueLen}|Flags].

externtuple_to_storetuple({Key, TStamp, Value}) ->
    storetuple_make(Key, TStamp, Value, gmt_util:io_list_len(Value), 0, []);
externtuple_to_storetuple({Key, TStamp, Value, 0, []}) ->
    storetuple_make(Key, TStamp, Value, gmt_util:io_list_len(Value), 0, []);
externtuple_to_storetuple({Key, TStamp, Value, 0, Flags})
  when is_list(Flags) ->
    storetuple_make(Key, TStamp, Value, gmt_util:io_list_len(Value), 0, Flags);
externtuple_to_storetuple({Key, TStamp, Value, ExpTime, []})
  when is_integer(ExpTime) ->
    storetuple_make(Key, TStamp, Value, gmt_util:io_list_len(Value), ExpTime, []);
externtuple_to_storetuple({Key, TStamp, Value, ExpTime, Flags}) ->
    storetuple_make(Key, TStamp, Value, gmt_util:io_list_len(Value), ExpTime, Flags).

exptime_insert(_ETab, _Key, 0) ->
    ok;
exptime_insert(ETab, Key, ExpTime) ->
    ?DBG_ETS("exptime_insert: ~w, {~w, ~w}", [ETab, ExpTime, Key]),
    ets:insert(ETab, {{ExpTime, Key}}).

exptime_delete(_ETab, _Key, 0) ->
    ok;
exptime_delete(ETab, Key, ExpTime) ->
    ?DBG_ETSx({qqq_exp_delete, ETab, {ExpTime, Key}}),
    case ets:lookup(ETab, {ExpTime, Key}) of
        [] -> ?DBG_ETSx({qqq_exp_delete, missing, ETab, {ExpTime, Key}});
        _  -> ok
    end,
    ets:delete(ETab, {ExpTime, Key}).

my_insert(State, Key, TStamp, Value, ExpTime, Flags) ->
    %% Filter all of the flags that we definitely must not store.
    Flags2 = lists:filter(
               fun({testset, _})            -> false;
                  ({max_num, _})            -> false;
                  ({binary_prefix, _})      -> false;
                  ({val_len, _})            -> false;
                  ({exp_time_directive, _}) -> false;
                  ({attrib_directive, _})   -> false;
                  ({_, _})                  -> true;
                  (must_exist)              -> false;
                  (must_not_exist)          -> false;
                  (witness)                 -> false;
                  (get_all_attribs)         -> false;
                  %% We'd normally filter 'value_in_ram' here, but we
                  %% need to let it pass through to a lower level.
                  (A) when is_atom(A)       -> true;
                  (_)                       -> false
               end, Flags),
    my_insert2(State, Key, TStamp, Value, ExpTime, Flags2).

my_insert2(#state{thisdo_mods=Mods, max_log_size=MaxLogSize}=S, Key, TStamp, Value, ExpTime, Flags) ->
    case Value of
        ?VALUE_REMAINS_CONSTANT ->
            %% @TODO - value_in_ram support?
            [ST] = my_lookup(S, Key, false),
            CurVal = storetuple_val(ST),
            CurValLen = storetuple_vallen(ST),
            %% This is a royal pain, but I don't see a way around
            %% this ... we need to avoid reading the value for
            %% this key *and* avoid sending that value down the
            %% chain.  For the latter, we need to indicate to the
            %% downstream that we didn't include the value in this
            %% update.  {sigh}
            Mod = {insert_constant_value,
                   storetuple_make(Key, TStamp, CurVal, CurValLen, ExpTime, Flags)},
            {{ok, TStamp}, S#state{thisdo_mods = [Mod|Mods]}};
        {?VALUE_SWITCHAROO, OldVal, NewVal} ->
            %% @TODO - value_in_ram support?
            %% NOTE: If testset has been specified, it has already
            %%       been validated and stripped from Flags.
            [ST] = my_lookup(S, Key, false),
            CurVal = storetuple_val(ST),
            CurValLen = storetuple_vallen(ST),

            case OldVal of
                {OldSeq, _} ->  %% old format
                    ?E_WARNING("value (storage location) is in old format. key: ~p, value: ~p",
                               [Key, OldVal]),
                    {CurSeq, _} = CurVal,
                    Mod =
                        if abs(OldSeq) =:= abs(CurSeq) ->
                                {insert_constant_value,
                                 storetuple_make(Key, TStamp, NewVal, CurValLen,
                                                 ExpTime, Flags)};
                           true ->
                                %% Someone else won an honest race for this
                                %% key.  Here, we can't change the value
                                %% returned to the client.  However, we can
                                %% change what we scribble to the local log.
                                {log_noop}
                        end;
                _ ->
                    CurTS = storetuple_ts(ST),
                    Mod =
                        if CurTS =:= TStamp -> %% @TODO: CHECKME: Do I need this check?
                                {insert_constant_value,
                                 storetuple_make(Key, TStamp, NewVal, CurValLen,
                                                 ExpTime, Flags)};
                           true ->
                                {log_noop}
                        end
            end,
            {{ok, TStamp}, S#state{thisdo_mods = [Mod|Mods]}};
        {?KEY_SWITCHAROO, NewKey} ->
            %% This clause should never match in Hibair v0.1.x
            ?E_ERROR("BUG: Error ~w ?KEY_SWITCHAROO is used", [S#state.name]),

            %% @TODO - value_in_ram support?
            [ST] = my_lookup(S, Key, false),
            CurTStamp = storetuple_ts(ST),
            CurVal = storetuple_val(ST),
            CurValLen = storetuple_vallen(ST),
            CurExpTime = storetuple_exptime(ST),
            %% Same as ?VALUE_REMAINS_CONSTANT except using NewKey and
            %% then deleting Key
            Mod = {insert_existing_value,
                   storetuple_make(NewKey, TStamp, CurVal, CurValLen, ExpTime, Flags),
                   Key, CurTStamp},
            DelTStamp = brick_server:make_timestamp(),
            {{ok, TStamp}, my_delete(S#state{thisdo_mods = [Mod|Mods]}, Key, DelTStamp, CurExpTime)};
        _ ->
            BinValue = gmt_util:bin_ify(Value),
            ValSize = byte_size(BinValue),
            if ValSize > MaxLogSize ->
                    {val_error, {value_size_exceeded_brick_max_log_size_mb,
                                 ValSize / 1024 / 1024}};
               true ->
                    UseRamP = lists:member(value_in_ram, Flags),
                    ST = storetuple_make(Key, TStamp, BinValue, ValSize,
                                         ExpTime, Flags -- [value_in_ram]),
                    Mod =
                        if UseRamP ->
                                {insert_value_into_ram, ST};
                           true ->
                                Loc = bigdata_dir_store_val(Key, BinValue, S),
                                {insert, ST, storetuple_replace_val(ST, Loc)}
                        end,
                    {{ok, TStamp}, S#state{thisdo_mods = [Mod|Mods]}}
            end
    end.

my_insert_ignore_logging(State, Key, StoreTuple) ->
    delete_prior_expiry(State, Key),
    my_insert_ignore_logging10(State, Key, StoreTuple).

my_insert_ignore_logging10(State, Key, StoreTuple) ->
    Val = storetuple_val(StoreTuple),
    if is_list(Val) ->
            %% If bigdata_dir is undefined, we need to collapse
            %% io_lists() to a single binary in order to get same
            %% semantics as bigdata_dir.
            my_insert_ignore_logging3(
              State, Key, storetuple_replace_val(StoreTuple,
                                                 list_to_binary(Val)));
       true ->
            my_insert_ignore_logging3(State, Key, StoreTuple)
    end.

my_insert_ignore_logging3(State, Key, StoreTuple) ->
    exptime_insert(State#state.etab, Key, storetuple_exptime(StoreTuple)),
    ets:insert(State#state.ctab, StoreTuple).

delete_prior_expiry(S, Key) ->
    case my_lookup(S, Key, false) of
        [ST] -> ExpTime = storetuple_exptime(ST),
                exptime_delete(S#state.etab, Key, ExpTime);
        _    -> ok
    end.

my_delete(#state{thisdo_mods=Mods}=State, Key, Timestamp, ExpTime)
  when State#state.do_logging =:= true ->
    %% Logging is in effect.  Save key in list of keys for later
    %% addition to dirty_keys list.
    State#state{thisdo_mods=[{delete, Key, Timestamp, ExpTime} | Mods]};
my_delete(#state{thisdo_mods=Mods}=State, Key, Timestamp, ExpTime) ->
    my_delete_ignore_logging(State, Key, Timestamp, ExpTime),
    State#state{thisdo_mods=[{delete, Key, Timestamp, ExpTime} | Mods]}.

my_delete_ignore_logging(State, Key, Timestamp, ExpTime) ->
    delete_prior_expiry(State, Key),
    bigdata_dir_delete_val(Key, State),
    my_delete_ignore_logging2(State, Key, Timestamp, ExpTime).

my_delete_ignore_logging2(State, Key, _Timestamp, ExpTime) ->
    exptime_delete(State#state.etab, Key, ExpTime),
    ets:delete(State#state.ctab, Key).

%% If MustHaveVal_p is true, it will read the value from bigdata_dir.
-spec my_lookup(state_r(), key(), MustHaveVal_p::boolean()) -> store_tuple().
my_lookup(State, Key, _MustHaveVal_p = false) ->
    my_lookup2(State, Key);
my_lookup(State, Key, true) ->
    case my_lookup2(State, Key) of
        [StoreTuple] ->
            Val0 = storetuple_val(StoreTuple),
            ValLen = storetuple_vallen(StoreTuple),
            Val = bigdata_dir_get_val(Key, Val0, ValLen, State),
            [storetuple_replace_val(StoreTuple, Val)];
        Res ->
            Res
    end.

%% @doc Perform ETS-specific lookup function.
%%
%% Note about separation of ETS and disk storage: Our caller is always
%% my_lookup(), which is responsible for doing any disk access for the
%% "real" value of val.

my_lookup2(State, Key) ->
    ets:lookup(State#state.ctab, Key).

%% REMINDER: It is the *caller's responsibility* to manage the
%%           logging_op_serial counter.

-spec log_mods(tuple(), boolean() | undefined) ->
                      {goahead, state_r()}
                          | {wait, brick_hlog_wal:callback_ticket(), state_r()}.
log_mods(#state{do_logging=false}=S, _SyncOverride) ->
    {goahead, S#state{thisdo_mods = []}};
log_mods(#state{thisdo_mods=[]}=S, _SyncOverride) ->
    {goahead, S};
log_mods(#state{thisdo_mods=ThisDoMods}=S, SyncOverride) ->
    write_metadata_term(ThisDoMods, S, SyncOverride, true).

load_rec_from_log_common_insert(StoreTuple, S) ->
    Key = storetuple_key(StoreTuple),
    load_rec_clean_up_etab(Key, S),
    my_insert_ignore_logging(S, Key, StoreTuple).

load_rec_clean_up_etab(Key, #state{ctab=CTab, etab=ETab}) ->
    case ets:lookup(CTab, Key) of
        [DelST] ->
            DelExp = storetuple_exptime(DelST),
            exptime_delete(ETab, Key, DelExp);
        _ ->
            ok
    end.

purge_recs_by_seqnum(SeqNum, #state{name=Name, ctab=CTab}=S) ->
    ?E_NOTICE("~s: purging keys with sequence ~p, size ~p",
              [Name, SeqNum, ets:info(CTab, size)]),
    N1 = purge_rec_from_log(0, ets:first(CTab), abs(SeqNum), 0, S),
    {N1, S}.

purge_rec_from_log(1000000, Key, SeqNum, Count, S) ->
    flush_gen_server_calls(),   % Clear any mailbox backlog
    flush_gen_cast_calls(S),    % Clear any mailbox backlog
    purge_rec_from_log(0, Key, SeqNum, Count, S);
purge_rec_from_log(_Iters, '$end_of_table', _SeqNum, Count, _S) ->
    Count;
purge_rec_from_log(Iters, Key, SeqNum, Count, S) ->
    [ST] = my_lookup(S, Key, false),
    case storetuple_val(ST) of
        {STSeqNum, _Offset} when abs(STSeqNum) =:= SeqNum ->
            Timestamp = brick_server:make_timestamp(),
            ExpTime = storetuple_exptime(ST),
            my_delete_ignore_logging(S, Key, Timestamp, ExpTime),
            ?E_NOTICE("~s: purged a key from log. key ~p", [S#state.name, Key]),
            purge_rec_from_log(Iters + 1, ets:next(S#state.ctab, Key), SeqNum,
                               Count + 1, S);
        _ ->
            purge_rec_from_log(Iters + 1,
                               ets:next(S#state.ctab, Key), SeqNum, Count, S)
    end.

filter_mods_for_downstream(Thisdo_Mods) ->
    lists:map(fun({insert, ChainStoreTuple, _StoreTuple}) ->
                      %%?DBG_GEN("ChainStoreTuple = ~p", [ChainStoreTuple]),
                      %%?DBG_GEN("_StoreTuple = ~p", [_StoreTuple]),
                      {insert, ChainStoreTuple};
                 ({log_directive, sync_override, _}) ->
                      {log_noop};
                 (X) ->
                      X
              end, Thisdo_Mods).

filter_mods_from_upstream(Thisdo_Mods, #state{name=Name}=State) ->
    lists:map(fun({insert, ST}) ->
                      Key = storetuple_key(ST),
                      Val = storetuple_val(ST),
                      Loc = bigdata_dir_store_val(Key, Val, State),
                      {insert, ST, storetuple_replace_val(ST, Loc)};
                 %% Do not modify {insert_value_into_ram,...} tuples here.
                 ({insert, ST, BigDataDirThing}) ->
                      ?E_CRITICAL("BUG: Error ~w bad_mod_from_upstream: ~p, ~p",
                                  [Name, ST, BigDataDirThing]),
                      exit({bug, Name, bad_mod_from_upstream, ST, BigDataDirThing});
                 %% ({insert_constant_value, ST}) ->
                 %%      Key = storetuple_key(ST),
                 %%      TS = storetuple_ts(ST),
                 %%      [CurST] = my_lookup(S, Key, false),
                 %%      CurVal = storetuple_val(CurST),
                 %%      CurValLen = storetuple_vallen(CurST),
                 %%      Exp = storetuple_exptime(ST),
                 %%      Flags = storetuple_flags(ST),
                 %%      NewST = storetuple_make(
                 %%                Key, TS, CurVal, CurValLen, Exp, Flags),
                 %%      {insert_constant_value, NewST};
                 ({insert_existing_value, ST, OldKey, OldTimestamp}) ->
                      case is_sequence_frozen(OldKey, State) of
                          true ->
                              %% This will read the actual value from disk and block
                              %% brick_server's main event loop. The squidflash primer
                              %% technique cannot be used here as it will reorder the
                              %% modifications from upstream by delaying the read.
                              %% The hlog sequence is only frozen while it is being
                              %% scavenged, so in the most of the cases, this case
                              %% clause will not be executed.
                              [CurSt] = my_lookup(State, OldKey, true),
                              CurVal = storetuple_val(CurSt),
                              Key = storetuple_key(ST),
                              Loc = bigdata_dir_store_val(Key, CurVal, State),
                              %% ?E_DBG("insert_exsiting_value - sequence_frozen", []),
                              {insert, ST, storetuple_replace_val(ST, Loc)};
                          false ->
                              [CurST] = my_lookup(State, OldKey, false),
                              CurVal = storetuple_val(CurST),
                              CurValLen = storetuple_vallen(CurST),
                              Key = storetuple_key(ST),
                              TS = storetuple_ts(ST),
                              Exp = storetuple_exptime(ST),
                              Flags = storetuple_flags(ST),
                              NewST = storetuple_make(
                                        Key, TS, CurVal, CurValLen, Exp, Flags),
                              %% ?E_DBG("insert_exsiting_value - ok", []),
                              {insert_existing_value, NewST, OldKey, OldTimestamp}
                      end;
                 (X) ->
                      X
              end, Thisdo_Mods).

map_mods_into_ets([{insert, StoreTuple} | Tail], S) ->
    Key = storetuple_key(StoreTuple),
    my_insert_ignore_logging(S, Key, StoreTuple),
    map_mods_into_ets(Tail, S);
map_mods_into_ets([{insert, _ChainStoreTuple, StoreTuple} | Tail], S) ->
    %% Lazy, reuse....
    map_mods_into_ets([{insert, StoreTuple} | Tail], S);
map_mods_into_ets([{insert_value_into_ram, StoreTuple} | Tail], S) ->
    %% Lazy, reuse....
    map_mods_into_ets([{insert, StoreTuple} | Tail], S);
map_mods_into_ets([{delete, Key, Timestamp, ExpTime} | Tail], S) ->
    my_delete_ignore_logging(S, Key, Timestamp, ExpTime),
    map_mods_into_ets(Tail, S);
map_mods_into_ets([{delete_noexptime, Key, Timestamp} | Tail], S) ->
    %% This item only used during migration.
    ExpTime = case my_lookup(S, Key, false) of
                  [StoreTuple] ->
                      storetuple_exptime(StoreTuple);
                  [] ->
                      0
              end,
    map_mods_into_ets([{delete, Key, Timestamp, ExpTime}|Tail], S);
map_mods_into_ets([{insert_constant_value, StoreTuple}|Tail], S) ->
    map_mods_into_ets([{insert, StoreTuple}|Tail], S);
map_mods_into_ets([{insert_existing_value, StoreTuple, _OldKey, _OldTimestamp}|Tail], S) ->
    map_mods_into_ets([{insert, StoreTuple}|Tail], S);
map_mods_into_ets([], _S) ->
    ok;
map_mods_into_ets([{log_directive, map_sleep, N}|Tail], S) ->
    timer:sleep(N),
    map_mods_into_ets(Tail, S);
map_mods_into_ets([{log_directive, sync_override, _}|Tail], S) ->
    map_mods_into_ets(Tail, S);
map_mods_into_ets([{log_noop}|Tail], S) ->
    map_mods_into_ets(Tail, S);
map_mods_into_ets([H|Tail], S)
  when is_tuple(H), element(1, H) =:= chain_send_downstream ->
    exit(should_not_be_happening_bad_scott_1),
    map_mods_into_ets(Tail, S);
map_mods_into_ets(NotList, _S) when not is_list(NotList) ->
    %% This is when applying mods to head, when head is a sweep tuple.
    ok.

%% log_mods2_b(Thisdo_Mods0, #state{do_sync=Sync}=S, SyncOverride) ->
%%     Thisdo_Mods = lists:map(
%%                     fun({insert, _ChainStoreTuple, StoreTuple}) ->
%%                             {insert, StoreTuple};
%%                        (X) ->
%%                             X
%%                     end, Thisdo_Mods0),
%%     %% @TODO (new hlog) Handle {hunk_too_big, ...} and {error, ..}.
%%     ok = wal_write_metadata_term(Thisdo_Mods, S),

%%     case {Sync, SyncOverride} of
%%         {true, false} ->
%%             %% NOTICE: This clobbering of #state.thisdo_mods
%%             %% should not cause problems when called outside
%%             %% the context of a handle_call({do, ...}, ...)
%%             %% call, right?
%%             {goahead, S#state{thisdo_mods = []}};
%%         {true, _TrueOrUndefined} ->
%%             {wait, S};
%%         {false, true} ->
%%             {wait, S};
%%         {false, _FalseOrUndefined} ->
%%             {goahead, S#state{thisdo_mods = []}}
%%     end.

%% @doc Write a set of Thisdo_Mods to disk, with the option of overriding sync.
-spec write_metadata_term([do_mod()], state_r(), boolean() | undefined, boolean()) ->
                                 {goahead, state_r()}
                                     | {wait, state_r()}
                                     | {wait, brick_hlog_wal:callback_ticket(), state_r()}.
write_metadata_term([], S, _, _) ->               % dialyzer: can never match...
    {goahead, S#state{thisdo_mods = []}};
write_metadata_term(Thisdo_Mods, #state{md_store=MdStore}=S, _SyncOverride=false, _IsHeadBrick) ->
    case MdStore:write_metadata(remove_chain_store_tuple(Thisdo_Mods)) of
        ok ->
            {goahead, S#state{thisdo_mods = []}};
        Other ->
            %% @TODO (new hlog) Handle {hunk_too_big, ...} and {error, ..}.
            error(Other)
    end;
write_metadata_term(Thisdo_Mods, #state{do_sync=DoSync, md_store=MdStore}=S, SyncOverride, true)
  when DoSync =:= true; SyncOverride =:= true ->
    %% This is a head brick.
    case MdStore:write_metadata_group_commit(remove_chain_store_tuple(Thisdo_Mods)) of
        {ok, WALSyncTicket} ->
            {wait, WALSyncTicket, S};
        Other ->
            %% @TODO (new hlog) Handle {hunk_too_big, ...} and {error, ..}.
            error(Other)
    end;
write_metadata_term(Thisdo_Mods, #state{do_sync=DoSync, md_store=MdStore}=S, SyncOverride, false)
  when DoSync =:= true; SyncOverride =:= true ->
    %% This is a meddle or official/reparing tail brick.
    case MdStore:write_metadata(remove_chain_store_tuple(Thisdo_Mods)) of
        ok ->
            {wait, S};
        Other ->
            %% @TODO (new hlog) Handle {hunk_too_big, ...} and {error, ..}.
            error(Other)
    end;
write_metadata_term(Thisdo_Mods, #state{md_store=MdStore}=S, _FalseOrUndefined, _IsHeadBrick) ->
    case MdStore:write_metadata(remove_chain_store_tuple(Thisdo_Mods)) of
        ok ->
            {goahead, S#state{thisdo_mods = []}};
        Other ->
            %% @TODO (new hlog) Handle {hunk_too_big, ...} and {error, ..}.
            error(Other)
    end.

%% -spec remove_chain_store_tuple([thisdo_mod()]) -> [thisdo_mod()]
remove_chain_store_tuple(Thisdo_Mods) ->
    lists:map(fun({insert, _ChainStoreTuple, StoreTuple}) ->
                      {insert, StoreTuple};
                 (X) ->
                      X
              end, Thisdo_Mods).

any_dirty_keys_p(DoKeys, State) ->
    lists:any(fun({_, K}) -> check_dirty_key(K, State) end, DoKeys).

check_dirty_key(Key, State) ->
    ets:member(State#state.dirty_tab, Key).

add_mods_to_dirty_tab([{InsertLike, StoreTuple}|Tail], S)
  when InsertLike =:= insert; InsertLike =:= insert_constant_value;
       InsertLike =:= insert_value_into_ram ->
    Key = storetuple_key(StoreTuple),
    ?DBG_ETS("add_to_dirty ~w: ~w", [S#state.name, Key]),
    ets:insert(S#state.dirty_tab, {Key, insert, StoreTuple}),
    add_mods_to_dirty_tab(Tail, S);
add_mods_to_dirty_tab([{insert_existing_value, StoreTuple, _OldKey, _OldTimestamp}|Tail], S) ->
    Key = storetuple_key(StoreTuple),
    ?DBG_ETS("add_to_dirty ~w: ~w", [S#state.name, Key]),
    ets:insert(S#state.dirty_tab, {Key, insert, StoreTuple}),
    add_mods_to_dirty_tab(Tail, S);
add_mods_to_dirty_tab([{insert, StoreTuple, _BigDataTuple}|Tail], S) ->
    add_mods_to_dirty_tab([{insert, StoreTuple}|Tail], S);
add_mods_to_dirty_tab([{delete, Key, _Timestamp, _ExpTime} | Tail], S) ->
    ?DBG_ETS("add_to_dirty ~w: ~w", [S#state.name, Key]),
    ets:insert(S#state.dirty_tab, {Key, delete, delete}),
    add_mods_to_dirty_tab(Tail, S);
add_mods_to_dirty_tab([_|Tail], S) ->
    %% Log replay may have weird terms in the list that we can ignore.
    add_mods_to_dirty_tab(Tail, S);
add_mods_to_dirty_tab([], _S) ->
    ok.

clear_dirty_tab([{insert, StoreTuple}|Tail], S) ->
    Key = storetuple_key(StoreTuple),
    ?DBG_ETS("clear_dirty ~w: ~w", [S#state.name, Key]),
    ets:delete(S#state.dirty_tab, Key),
    clear_dirty_tab(Tail, S);
clear_dirty_tab([{insert, _ChainStoreTuple, StoreTuple}|Tail], S) ->
    clear_dirty_tab([{insert, StoreTuple}|Tail], S);
clear_dirty_tab([{insert_value_into_ram, StoreTuple}|Tail], S) ->
    %% Lazy, reuse....
    clear_dirty_tab([{insert, StoreTuple}|Tail], S);
clear_dirty_tab([{delete, Key, _Timestamp, _ExpTime} | Tail], S) ->
    ?DBG_ETS("clear_dirty ~w: ~w", [S#state.name, Key]),
    ets:delete(S#state.dirty_tab, Key),
    clear_dirty_tab(Tail, S);
clear_dirty_tab([{delete_noexptime, Key, _Timestamp} | Tail], S) ->
    ?DBG_ETS("clear_dirty_noexptime ~w: ~w", [S#state.name, Key]),
    ets:delete(S#state.dirty_tab, Key),
    clear_dirty_tab(Tail, S);
clear_dirty_tab([{insert_constant_value, StoreTuple}|Tail], S) ->
    clear_dirty_tab([{insert, StoreTuple}|Tail], S);
clear_dirty_tab([{insert_existing_value, StoreTuple, _OldKey, _OldTimestamp}|Tail], S) ->
    clear_dirty_tab([{insert, StoreTuple}|Tail], S);
clear_dirty_tab([], _S) ->
    ok;
clear_dirty_tab([{log_directive, _, _}|Tail], S) ->
    clear_dirty_tab(Tail, S);
clear_dirty_tab([{log_noop}|Tail], S) ->
    clear_dirty_tab(Tail, S);
clear_dirty_tab([H|Tail], S)
  when is_tuple(H), element(1, H) =:= chain_send_downstream ->
    exit(should_not_be_happening_bad_scott_2),
    clear_dirty_tab(Tail, S);
clear_dirty_tab(NotList, _S) when not is_list(NotList) ->
    ok.

retry_dirty_key_ops(S) ->
    case queue:is_empty(S#state.wait_on_dirty_q) of
        true ->
            {S, []};
        false ->
            F = fun(#dirty_q{from = From, do_op = DoOp}, {InterimS, ToDos}) ->
                        case bcb_handle_do(DoOp, From, InterimS) of
                            %% case_clause: {up1, list(), reply()} ...
                            %% CHAIN TODO: See comment "ISSUE001".
                            {up1, TDs, OrigReply} ->
                                %%?DBG_GEN("QQQ ISSUE001: up todos = ~p", [get(issue001)]),
                                %%?DBG_GEN("QQQ ISSUE001: TDs = ~p", [TDs]),
                                %%?DBG_GEN("QQQ ISSUE001: OrigReply = ~p", [OrigReply]),
                                %% timer:sleep(5*1000), exit(asdlkfasdf);
                                NewToDos = TDs ++ ToDos,
                                case OrigReply of
                                    {noreply, NewInterimS} ->
                                        {NewInterimS, NewToDos}
                                end;
                            %% Below: dialyzer: can never match.  Leave it be.
                            {reply, Reply, NewInterimS} ->
                                gen_server:reply(From, Reply),
                                {NewInterimS, ToDos};
                            {noreply, NewInterimS} ->
                                {NewInterimS, ToDos};
                            %% Really rare case ... but yes, it happens.
                            {up1_read_only_mode, From, DoOp, NewInterimS} ->
                                {NewInterimS, ToDos}
                        end
                end,
            {NewS, ToDoList} =
                lists:foldl(F, {S#state{wait_on_dirty_q = queue:new()}, []},
                            queue:to_list(S#state.wait_on_dirty_q)),
            {NewS, ToDoList}
    end.

%%
%% Repair utilities
%%

%% @spec (list(key()), key() | brick__get_many_last(), list(key()),
%%        integer(), state_r()) ->
%%       {key(), list(key()), integer(), state_r()}
%% @doc Figure out which keys in a round1 repair that we don't have, and
%%      delete keys that we know the upstream doesn't have (using MyLastKey
%%      from the previous round1 iteration as a vital starting hint).

repair_diff_round1([], MyLastKey, Unknown, Ds, S) ->
    {MyLastKey, Unknown, Ds, S};
repair_diff_round1([Tuple|Tail], ?BRICK__GET_MANY_LAST = MyLastKey, Unknown, Ds, S) ->
    repair_diff_round1(Tail, MyLastKey, [storetuple_key(Tuple) | Unknown], Ds, S);
repair_diff_round1([Tuple|Tail] = UpList, MyKey, Unknown, Ds, S) ->
    UpKey = storetuple_key(Tuple),
    if
        UpKey < MyKey ->
            %% UpKey is unknown, add it to the list
            repair_diff_round1(Tail, MyKey, [UpKey | Unknown], Ds, S);
        UpKey =:= MyKey ->
            %% UpKey is known; check the timestamp and add to the list if diff
            UpTS  = storetuple_ts(Tuple),
            case my_lookup(S, MyKey, false) of
                [MyTuple] ->
                    MyKey = storetuple_key(MyTuple),
                    MyTS  = storetuple_ts(MyTuple),
                    if
                        UpTS =:= MyTS ->
                            repair_diff_round1(Tail,
                                               ets:next(S#state.ctab, MyKey),
                                               Unknown, Ds, S);
                        true ->
                            repair_diff_round1(Tail,
                                               ets:next(S#state.ctab, MyKey),
                                               [UpKey|Unknown], Ds, S)
                    end;
                [] ->
                    %% On more review for BZ 27632, I don't expect
                    %% this to happen, but I'll leave the alarm in
                    %% anyway.
                    gmt_util:set_alarm({debug_alarm, {a_24622, MyKey}},
                                       "Contact Hibari developer",
                                       fun() -> ok end),
                    repair_diff_round1(Tail,
                                       ets:next(S#state.ctab, MyKey),
                                       Unknown, Ds, S)
            end;
        UpKey > MyKey ->
            %% MyKey is unknown upstream, delete it now!
            case my_lookup(S, MyKey, false) of
                [MyTuple] ->
                    MyExpTime  = storetuple_exptime(MyTuple),
                    ok = repair_loop_delete(MyKey, MyExpTime, S),
                    repair_diff_round1(UpList,
                                       ets:next(S#state.ctab, MyKey),
                                       Unknown, Ds + 1, S);
                [] ->
                    %% BZ 27632: stumble across bug in BZ 24622 patch, oops.
                    repair_diff_round1(UpList,
                                       ets:next(S#state.ctab, MyKey),
                                       Unknown, Ds, S)
            end
    end.

repair_diff_round1(UpList, MyLastKey, S) ->
    repair_diff_round1(UpList, MyLastKey, [], 0, S).

repair_diff_round2([], LastKey, Is, Ds, S) ->
    {LastKey, Is, Ds, S};
repair_diff_round2([Tuple|Tail], LastKey, Is, Ds, S) ->
    UpKey = storetuple_key(Tuple),
    ?DBG_REPAIR("rep_c: upkey ~w, lastkey ~w", [UpKey, LastKey]),
    ok = repair_loop_insert(Tuple, S),
    NewLastKey =
        if UpKey > LastKey ->
                UpKey;
           true ->
                LastKey
        end,
    repair_diff_round2(Tail, NewLastKey, Is + 1, Ds, S).

repair_diff_round2(UpList, Ds, S) ->
    repair_diff_round2(UpList, "", 0, Ds, S).

repair_loop(UpstreamList, MyKey, S) ->
    repair_loop(UpstreamList, MyKey, 0, 0, S).

repair_loop([], LastKey, Is, Ds, S) ->
    ?DBG_REPAIR("rep_a: ~w", [LastKey]),
    {LastKey, Is, Ds, S};
repair_loop([StoreTuple|Tail], ?BRICK__GET_MANY_LAST = MyKey, Is, Ds, S) ->
    ?DBG_REPAIR("rep_b: ~w", [storetuple_key(StoreTuple)]),
    ok = repair_loop_insert(StoreTuple, S),
    repair_loop(Tail, MyKey, Is + 1, Ds, S);
repair_loop([StoreTuple|Tail] = UpList, MyKey, Is, Ds, S) ->
    UpKey = storetuple_key(StoreTuple),
    if UpKey < MyKey ->
            ?DBG_REPAIR("rep_c: upkey ~w, mykey ~w", [UpKey, MyKey]),
            ok = repair_loop_insert(StoreTuple, S),
            repair_loop(Tail, MyKey, Is + 1, Ds, S);
       UpKey =:= MyKey ->
            ?DBG_REPAIR("rep_d: upkey ~w, mykey ~w", [UpKey, MyKey]),
            UpTS  = storetuple_ts(StoreTuple),
            UpVal = storetuple_val(StoreTuple),
            %% ets:lookup() here is OK, we'll fetch val later, if necessary.
            [MyStoreTuple] = ets:lookup(S#state.ctab, MyKey),
            MyKey = storetuple_key(MyStoreTuple),
            MyTS  = storetuple_ts(MyStoreTuple),
            %% Delay fetching MyVal until we really need it.
            if UpTS < MyTS ->
                    ?E_ERROR("TODO: Weird, but we got a repair "
                             "update for ~p with older ts", [UpKey]),
                    exit(hibari_inconceivable_2398200);
               true ->
                    ok
            end,
            if UpTS =:= MyTS ->
                    %% We're repairing, so it's impossible to have active
                    %% shadowtab right now.
                    Val0 = storetuple_val(StoreTuple),
                    ValLen = storetuple_vallen(StoreTuple),
                    MyVal =  bigdata_dir_get_val(UpKey, Val0, ValLen, S),
                    if UpVal =/= MyVal ->
                            ?E_INFO("TODO: Weird, but we got a repair "
                                    "update for ~p with same ts but "
                                    "different values", [UpKey]),
                            throw(hibari_inconceivable_2398223);
                       true ->
                            %% Nothing to do
                            ?DBG_REPAIR("repair [do_nothing] upkey ~w", [UpKey]),
                            repair_loop(Tail, ets:next(S#state.ctab, MyKey),
                                        Is, Ds, S)
                    end;
               %% UpTS < MyTS has already been tested!
               UpTS > MyTS ->
                    %% Upstream's value trumps us every time.
                    ?DBG_REPAIR("upkey ~w, upts ~w, upval ~w", [UpKey, UpTS, UpVal]),
                    ok = repair_loop_insert(StoreTuple, S),
                    repair_loop(Tail, ets:next(S#state.ctab, MyKey),
                                Is + 1, Ds, S)
            end;
       UpKey > MyKey ->
            ?DBG_REPAIR("rep_e: upkey ~w, mykey ~w", [UpKey, MyKey]),
            %% ets:lookup() here is OK, we don't need val.
            [MyStoreTuple] = ets:lookup(S#state.ctab, MyKey),
            MyExpTime  = storetuple_exptime(MyStoreTuple),
            ok = repair_loop_delete(MyKey, MyExpTime, S),
            repair_loop(UpList, ets:next(S#state.ctab, MyKey), Is, Ds + 1, S)
    end.

repair_loop_insert(Tuple, S) ->
    StoreTuple = externtuple_to_storetuple(Tuple),
    Key = storetuple_key(StoreTuple),
    Flags = storetuple_flags(StoreTuple),
    ?DBG_REPAIR("repair [insert] ~w", [Key]),
    Thisdo_Mods = case lists:member(value_in_ram, Flags) of
                      true ->
                          [{insert_value_into_ram,
                            storetuple_replace_flags(StoreTuple,
                                                     Flags -- [value_in_ram])}];
                      false ->
                          [{insert, StoreTuple}]
                  end,
    LocalMods = filter_mods_from_upstream(Thisdo_Mods, S),
    _ = write_metadata_term(LocalMods, S, undefined, false),
    %% Cannot use Tuple or Thisdo_Mods here: bigdata_dir storage
    %% locations in our local log (inside LocalMods) must be used!
    ST = case LocalMods of
             [{insert, _ChainStoreTuple, ST2}] ->
                 ST2;
             [{insert, ST2}] ->
                 ST2;
             [{insert_value_into_ram, ST2}] ->
                 ST2
         end,
    ?DBG_REPAIR("repair_loop_insert: ~w", [ST]),
    my_insert_ignore_logging(S, Key, ST),
    ok.

repair_loop_delete(Key, ExpTime, S) ->
    ?DBG_REPAIR("repair [delete] ~w", [Key]),
    Timestamp = brick_server:make_timestamp(),
    Thisdo_mods = [{delete, Key, Timestamp, ExpTime}],
    _ = write_metadata_term(Thisdo_mods, S, undefined, false),
    my_delete_ignore_logging(S, Key, Timestamp, ExpTime),
    ok.

delete_remaining_keys(LastKey, S) ->
    delete_remaining_keys(LastKey, S, 0).

delete_remaining_keys(?BRICK__GET_MANY_LAST, S, Deletes) ->
    {Deletes, S};
delete_remaining_keys(Key, S, Deletes) ->
    %% ets:lookup() here is OK, we don't need val.
    [MyStoreTuple] = ets:lookup(S#state.ctab, Key),
    MyExpTime  = storetuple_exptime(MyStoreTuple),
    ok = repair_loop_delete(Key, MyExpTime, S),
    delete_remaining_keys(ets:next(S#state.ctab, Key), S, Deletes + 1).

%%
%% Disk storage extension stuff
%%

bigdata_dir_store_val(_Key, Value, #state{blob_store=BlobStore}) when is_binary(Value) ->
    case BlobStore:write_value(Value) of
        {ok, StorageLocation} ->
            StorageLocation;
        {error, Err} ->
            error(Err)
    end;
bigdata_dir_store_val(Key, _, _) ->
    error({invalid_value_type_for_key, Key}).

bigdata_dir_delete_val(_Key, _S) ->
    %% LTODO: anything to do here?
    ok.

bigdata_dir_get_val(_Key, Val, ValLen, S) ->
    bigdata_dir_get_val(_Key, Val, ValLen, true, S).

%% TODO: This bad boy will crash if the file doesn't exist.

bigdata_dir_get_val(_Key, no_blob, _ValLen, _CheckMD5_p, _S) ->
    <<>>;
bigdata_dir_get_val(Key, StorageLocation, _ValLen, _CheckMD5_p,
                    #state{blob_store=BlobStore}=S)
  when is_tuple(StorageLocation) ->
    case BlobStore:read_value(StorageLocation) of
        {ok, Blob} ->
            Blob;
        %% {error, Reason} when Reason =:= system_limit ->
        %% We shouldn't mark the file as bad: in this
        %% clause we aborted before reads were entirely
        %% successful.  Logging an error probably won't
        %% work because we're probably pretty hosed
        %% (e.g. out of file descriptors), but we should
        %% try anyway.
        %%
        %% TODO: What other erors here that we need to look for?
        %%
        %%     ?E_ERROR("~s: read error ~p at seq ~p offset ~p for the stored value of key ~p",
        %%              [S#state.name, Reason, SeqNum, Offset, Key]),
        %%     throw(silently_drop_reply);
        eof when S#state.do_sync =:= false ->
            %% The brick is in async mode.  If we're trying to
            %% read something that was written very very
            %% recently, then the commonLogServer may have
            %% buffered it while a sync for some other brick
            %% is in progress.  If that's true, then the data
            %% we're looking for hasn't been written to the OS
            %% yet, and trying to read from the promised file
            %% & offset will yield 'eof'.  Doctor, it hurts
            %% when I read async-written data too early....
            ?E_WARNING("~s: read error ~p at ~p the stored value of key ~p",
                       [S#state.name, async_eof, StorageLocation, Key]),
            throw(silently_drop_reply);
        Error ->
            ?E_WARNING("~s: read error ~p at ~p the stored value of key ~p",
                       [S#state.name, Error, StorageLocation, Key]),
            %% sequence_file_is_bad(SeqNum, Offset, S),
            throw(silently_drop_reply)
    end;
bigdata_dir_get_val(_Key, Val, _ValLen, _CheckMD5_p, _S)
  when is_binary(Val) ->
    %% OK, if we're here, this would be a bug, normally: we're only
    %% called when bigdata_dir mode is true, and we shouldn't ever
    %% have is_binary(Val) = true.
    %%
    %% HOWEVER, there is a case where this situation can indeed
    %% happen, at least in the regression test regime:
    %%  1. Run chain regression tests with:
    %%     [{bigdata_dir,undefined}, {do_logging,true}, {do_sync,true}]
    %%  2. Run chain regression tests again with:
    %%     [{bigdata_dir, "./regression-bigdata-dir"},
    %%      {do_logging,false}, {do_sync,false}]
    %%
    %% If all traces of run #1 aren't deleted, then run #2 will slurp
    %% in binary data in the storetuples instead of bigdata_dir
    %% pointer-tuples.
    %%
    %% If we permit this, then it's possible to switch a brick from
    %% bigdata_dir mode on and off.  I really really don't recommend
    %% doing that, but I don't think it's worth a crash.
    %%
    %% Also, it's 100% OK if we're mixing RAM & disk val blob storage
    %% in the same table.
    Val;
bigdata_dir_get_val(_Key, Val0, _ValLen, _CheckMD5_p, _S) ->
    exit({ltodo_2_when_does_this_happen, _Key, Val0}).

%% @TODO (new hlog) Temporary disabled.
%% sequence_file_is_bad(SeqNum, Offset, S)
%%   when S#state.wal_mod =:= gmt_hlog ->
%%     write_bad_sequence_hunk(S#state.wal_mod, S#state.log, S#state.name,
%%                             SeqNum, Offset),
%%     sequence_file_is_bad_common(S#state.log_dir, S#state.wal_mod, S#state.log,
%%                                 S#state.name, SeqNum, Offset);
%% sequence_file_is_bad(SeqNum, Offset, S)
%%   when S#state.wal_mod =:= gmt_hlog_local ->
%%     gmt_hlog_common:sequence_file_is_bad(SeqNum, Offset).

sequence_rename(OldPath1, OldPath2, NewPath) ->
    case file:rename(OldPath1, NewPath) of
        ok ->
            ok;
        {error, enoent} ->
            file:rename(OldPath2, NewPath);
        Err ->
            Err
    end.

%% @TODO (new hlog) Temporary disabled.
%% write_bad_sequence_hunk(WalMod, Log, Name, SeqNum, Offset) ->
%%     try
%%         _ = WalMod:advance_seqnum(Log, 2),
%%         _ = WalMod:write_hunk(
%%               Log, Name, metadata, <<"k:sequence_file_is_bad">>,
%%               ?LOGTYPE_BAD_SEQUENCE, [term_to_binary({SeqNum, Offset})], []),
%%         _ = wal_sync(Log, WalMod)
%%     catch
%%         X:Y ->
%%             ?E_ERROR("sequence_file_is_bad: ~p ~p -> ~p ~p (~p)",
%%                      [SeqNum, Offset, X, Y, erlang:get_stacktrace()])
%%     end.

-spec sequence_file_is_bad_common(nonempty_string(), atom(), pid(), atom(),integer(),integer()) -> ok.
sequence_file_is_bad_common(LogDir, WalMod, _Log, Name, SeqNum, Offset) ->
    BadDir = LogDir ++ "/BAD-CHECKSUM",
    _ = file:make_dir(BadDir),
    OldPath1 = (WalMod):log_file_path(LogDir, SeqNum),
    OldPath2 = (WalMod):log_file_path(LogDir, -SeqNum),
    NewPath = BadDir ++ "/" ++ integer_to_list(SeqNum) ++ ".at.offset." ++
        integer_to_list(Offset),
    case sequence_rename(OldPath1, OldPath2, NewPath) of
        ok ->
            ?E_WARNING("Brick ~p bad sequence file: seq ~p offset ~p: "
                       "renamed to ~s",
                       [Name, SeqNum, Offset, NewPath]);
        {error, Reason} ->
            ?E_WARNING("Brick ~p bad sequence file: seq ~p offset ~p: "
                       "rename ~s or ~s to ~s failed: ~p",
                       [Name, SeqNum, Offset, OldPath1, OldPath2,
                        NewPath, Reason])
    end,
    ok.

%%
%% Write-ahead log helper functions
%%

%% @doc Returns a 2-tuple: {accumulator from the fold fun, list of errors}
%%
%% WARNING: This function will purge our mailbox of all '$gen_call' and
%%          '$gen_cast' messages.  For use only within the context of
%%          gen_server's init() callback function!

-spec load_metadata(state_r()) -> {non_neg_integer(), [tuple()]}.
load_metadata(#state{name=BrickName}=State) ->
    %% put(wal_scan_cast_count, 0),
    {ok, MdStore} = brick_metadata_store:get_metadata_store(BrickName),
    {ok, MetadataDB} = MdStore:get_leveldb(),
    FirstMDBKey = h2leveldb:first_key(MetadataDB),
    load_metadata1(BrickName, MetadataDB, FirstMDBKey, State, {0, []}).

load_metadata1(_BrickName, _MetadataDB, end_of_table, _State, {Count, Errors}) ->
    {Count, lists:reverse(Errors)};
load_metadata1(BrickName, MetadataDB, {ok, MDBKey}, State, {Count, Errors}=PrevResult) ->
    %% %% This function may run for hours or perhaps even days.
    %% flush_gen_server_calls(),       % Clear any mailbox backlog
    %% flush_gen_cast_calls(S),        % Clear any mailbox backlog
    Result =
        try sext:decode(MDBKey) of
            {_Key, _ReversedTimestamp} ->
                case h2leveldb:get(MetadataDB, MDBKey) of
                    {ok, Bin} when is_binary(Bin) ->
                        StoreTuple = binary_to_term(Bin),
                        load_rec_from_log_common_insert(StoreTuple, State),
                        {Count + 1, Errors};
                    {error, Err} ->
                        {Count, [{MDBKey, Err}|Errors]}
                end;
            Command when is_atom(Command) ->
                PrevResult
        catch
            _:_=Err ->
                {Count, [{MDBKey, Err}|Errors]}
    end,
    NextMDBKey = h2leveldb:next_key(MetadataDB, MDBKey),
    load_metadata1(BrickName, MetadataDB, NextMDBKey, State, Result).

%% wal_sync(Pid, WalMod) when is_pid(Pid) ->
%%     WalMod:sync(Pid).

flush_gen_server_calls() ->
    receive {'$gen_call', _, _} ->
            flush_gen_server_calls()
    after 0 ->
            ok
    end.

flush_gen_cast_calls(#state{name=Name}=S) ->
    receive {'$gen_cast', Msg} ->
            Count = case get(wal_scan_cast_count) of
                        undefined ->
                            0;
                        N ->
                            N
                    end,
            if
                Count < 10 ->
                    ?E_INFO("~p: flushing $gen_cast message: ~P", [Name, Msg, 9]);
                true ->
                    ok
            end,
            put(wal_scan_cast_count, Count + 1),
            flush_gen_cast_calls(S)
    after 0 ->
            ok
    end.

%% @doc A "primer" function to force disk pages into RAM, like priming
%% a pump.
%%
%% What's this "squidflash" stuff?  It's a hack that both the Squid
%% HTTP proxy and the Flash HTTP server use to avoid blocking on local
%% disk I/O: use another OS process (or Pthread?) to do any
%% possibly-blocking local disk I/O.  In this case, we'll choose
%% Flash's behavior because Flash has less to do than Squid....
%%
%% For any read request, Flash will ask an external process to touch
%% each page of the requested file.  Any pages not in the OS buffer
%% cache will be page faulted.  When the external proc's scan is done,
%% it informs Flash that it's done.  Then Flash can read(2) (?) the
%% entire file with exteremely low risk of blocking on a page fault.
%%
%% We're going to kludge the same thing: use third-party Erlang procs
%% to read the bigdata_dir blobs for us, then signal us when it's all
%% done.  We won't bother trying to cache the actual read results:
%% we'll just re-read the data again from the OS, and yes it's extra
%% overhead, but we're *bottlenecked* on disk I/O, so the extra few
%% microseconds won't matter.
%%
%% If the key's val blob is stored in RAM instead of on disk, then no
%% problem: we will not "prime" it at all.  (That's the decision of
%% the accumulate_maybe() function.)
%%
%% Our technique for signalling completion is: "re-inject" the DoOp
%% into the main gen_server proc is crude (and ugly and interface-
%% contaminating), but effective.  This may add extra latency to the
%% op, because the {do, ...}  message will be added at the end of the
%% gen_server's mailbox for handle_call() processing.  For now, we'll
%% assume this extra latency won't be a big deal.  If it is, we'll
%% either have to re-send using a special message that jumps to the
%% head of the queue, or (much harder) continue processing outside of
%% the gen_server (consistency problems!)
%%
%% In all race cases we don't care:
%% * key deleted -> no problem
%% * key replaced -> almost certainly in OS buffer cache
%% * key relocated by scavenger -> almost certainly in OS buffer cache

bcb_squidflash_primer(KsRaws, ResubmitFun,
                      #state{name=Name, blob_store=BlobStore}) ->
    FakeS = #state{name=Name, blob_store=BlobStore},
    spawn(fun() ->
                  squidflash_doit(KsRaws, ResubmitFun, FakeS)
          end).

squidflash_doit(KsRaws, ResubmitFun, FakeS) ->
    Start = brick_metrics:histogram_timed_begin(read_priming_latencies),
    Me = self(),
    KRV_Refs = [{X, make_ref()} || X <- KsRaws],
    [catch gmt_parallel_limit:enqueue(
             brick_primer_limit,
             fun() ->
                     catch squidflash_prime1(Key, RawVal, ValLen, Me, FakeS),
                     Me ! Ref,
                     exit(normal)
             end)
     || {{Key, RawVal, ValLen}, Ref} <- KRV_Refs],
    [receive
         Ref ->
             ok
     after 10000 ->  % should be impossible, but...
             ok
     end
     || {_, Ref} <- KRV_Refs],

    %% Call ResubmitFun to resubmit the original request back to
    %% brick_ets:handle_call/3 or handle_cast/2.
    ResubmitFun(),
    brick_metrics:histogram_timed_notify(Start),
    exit(normal).

squidflash_prime1(Key, RawVal, ValLen, ReplyPid, FakeS) ->
    case (catch bigdata_dir_get_val(Key, RawVal, ValLen, false, FakeS)) of
        {'EXIT', _X} ->
            ?E_ERROR("cache prime: ~p: ~p at ~p: ~p",
                     [FakeS#state.name, Key, RawVal, _X]);
        _ -> ok
    end,
    ReplyPid ! {squidflash_prime_done, self()},
    exit(normal).

do_check_expiry(S) ->
    Now = gmt_time:time_t(),
    ExpKeys = expiry_iter_keys(S#state.etab, Now, ets:first(S#state.etab)),
    Filtered = [Tuple || {_, Key} = Tuple <- ExpKeys,
                         not check_dirty_key(Key, S)],
    KeysTs = lists:foldl(
               fun({ExpTime, Key}, Acc) ->
                       case my_lookup(S, Key, false) of
                           [ST] ->
                               TS = storetuple_ts(ST),
                               ExpTime2 = storetuple_exptime(ST),
                               if ExpTime =:= ExpTime2 ->
                                       [{Key, TS}|Acc];
                                  true ->
                                       Acc
                               end;
                           [] ->
                               Acc
                       end
               end, [], Filtered),
    (S#state.do_expiry_fun)(S#state.name, KeysTs),
    S.

-spec expiry_iter_keys(table_name(), integer(), {integer(), key()} | '$end_of_table') -> list().
expiry_iter_keys(_ETab, _Now, '$end_of_table') ->
    [];
expiry_iter_keys(_ETab, Now, {ExpTime, _Key}) when ExpTime > Now ->
    [];
expiry_iter_keys(ETab, Now, {_ExpTime, _Key} = Tuple) ->
    [Tuple|expiry_iter_keys(ETab, Now, ets:next(ETab, Tuple))].

delete_keys_immediately(ServerName, KeysTs) ->
    Parent = self(),
    Ops =  [brick_server:make_delete(Key, [{testset, TS}]) ||
               {Key, TS} <- KeysTs],
    if Ops =/= [] ->
            %% We could try to pass the do list directly to ourself,
            %% but there's too much complication with chain
            %% replication and 'up1' processing.  {sigh} So be lazy
            %% and let an external proc do the work for us.  This
            %% means that a key could get dirty or be deleted before
            %% this request arrives, but that's OK, we'll create
            %% another one very shortly.  :-)
            spawn(fun() ->
                          %% Register a common name to avoid overload
                          %% by long-running expiration child procs.
                          register(brick_server:make_expiry_regname(ServerName),
                                   self()),
                          _ = brick_server:do(ServerName, node(), Ops,
                                              [], 5*1000),
                          gen_server:cast(Parent, {incr_expired, length(Ops)})
                  end);
       true ->
            ok
    end.

verbose_expiration(ServerName, KeysTs) ->
    if KeysTs =/= [] -> ?E_INFO("Verbose expiry: ~p ~p", [ServerName, KeysTs]);
       true         -> ok
    end,
    delete_keys_immediately(ServerName, KeysTs).

%% @spec (fun(), term(), list(), fun(), integer) -> term()
%% @doc See gmt_util:list_chunkfoldl/5.

foldl_lump(F_inner, Acc, L, F_outer, Count) ->
    gmt_util:list_chunkfoldl(F_inner, Acc, L, F_outer, Count).

%% slurp_log_chunks(Log) ->
%%     lists:append(slurp_log_chunks(disk_log:chunk(Log, start), Log, [])).

%% slurp_log_chunks(eof, _Log, Acc) ->
%%     lists:reverse(Acc);
%% slurp_log_chunks({error, _} = Err, _Log, _Acc) ->
%%     exit({slurp_log_chunks, Err});
%% slurp_log_chunks({Cont, Ts}, Log, Acc) ->
%%     slurp_log_chunks(disk_log:chunk(Log, Cont), Log, [Ts|Acc]).

-spec disk_log_fold(log_fold_fun(),tuple()|integer(),term()) -> tuple()|integer().
disk_log_fold(Fun, Acc, Log) ->
    disk_log_fold_2(disk_log:chunk(Log, start), Fun, Acc, Log,
                    fun(X) -> X end).

-spec disk_log_fold_bychunk(fun(([tuple()],integer())->integer()),integer(),term()) -> integer().
disk_log_fold_bychunk(Fun, Acc, Log) ->
    disk_log_fold_2(disk_log:chunk(Log, start), Fun, Acc, Log,
                    fun(X) -> [X] end).

disk_log_fold_2(eof, _Fun, Acc, _Log, _XForm) ->
    Acc;
disk_log_fold_2({error, _} = Err, _Fun, _Acc, Log, _XForm) ->
    exit({disk_log_fold_2, Log, Err});
disk_log_fold_2({Cont, Terms}, Fun, Acc, Log, XForm) ->
    NewAcc = lists:foldl(Fun, Acc, XForm(Terms)),
    disk_log_fold_2(disk_log:chunk(Log, Cont), Fun, NewAcc, Log, XForm).

-spec temp_path_to_seqnum(nonempty_string()) -> integer().
temp_path_to_seqnum(Path) ->
    list_to_integer(string:substr(Path, string:rchr(Path, $/) + 1)).

-spec which_path(_,atom() | tuple(),_) -> {_,atom() | [any()]}.
which_path(Dir, WalMod, SeqNum) ->
    P1 = WalMod:log_file_path(Dir, SeqNum),
    case file:read_file_info(P1) of
        {ok, _} ->
            {SeqNum, P1};
        _ ->
            P2 = WalMod:log_file_path(Dir, -SeqNum),
            case file:read_file_info(P2) of
                {ok, _} ->                      % Only one pattern!
                    {-SeqNum, P2}
            end
    end.

-spec scavenger_get_keys(brick_server:brick_name(),brick_server:flags_list_many(),
                         key(),
                         function(), function()) -> ok.
scavenger_get_keys(Name, Fs, FirstKey, F_k2d, F_lump) ->
    {ok, Retry} = application:get_env(gdss_brick, scavenger_get_many_retry),
    {ok, Max} =  application:get_env(gdss_brick, scavenger_get_many_max),
    {ok, TimeOut} =  application:get_env(gdss_brick, scavenger_get_many_timeout),
    {ok, Sleep} =  application:get_env(gdss_brick, scavenger_get_many_sleep),
    GetManyConf = {Max,TimeOut,Retry,Sleep},
    scavenger_get_keys(Name, Fs, GetManyConf,
                       scavenger_get_many(Name,FirstKey,Fs,GetManyConf),
                       [], F_k2d, F_lump, 1).

scavenger_get_keys(Name, Fs, GMC, Res, Acc, F_k2d, F_lump, Iters)
  when Iters rem 4 =:= 0 ->
    foldl_lump(F_k2d, dict:new(), Acc, F_lump, 100*1000),
    scavenger_get_keys(Name, Fs, GMC,
                       Res, [], F_k2d, F_lump, Iters + 1);
scavenger_get_keys(Name, _Fs, _GMC,
                   {ok, {Rs, false}}, Acc, F_k2d, F_lump, _Iters) ->
    foldl_lump(F_k2d, dict:new(), prepend_rs(Name, Rs, Acc), F_lump, 100*1000),
    ok;
scavenger_get_keys(Name, Fs, GMC,
                   {ok, {Rs, true}}, Acc, F_k2d, F_lump, Iters) ->
    K = storetuple_key(lists:last(Rs)),
    scavenger_get_keys(Name, Fs, GMC,
                       scavenger_get_many(Name, K, Fs, GMC),
                       prepend_rs(Name, Rs, Acc), F_k2d, F_lump, Iters + 1).

scavenger_get_many(Name, Key, Flags, {Max, TimeOut, 0, Sleep}) ->
    [Res] = brick_server:do(Name, node(),
                            [brick_server:make_get_many(Key, Max, Flags)],
                            [ignore_role], TimeOut),
    timer:sleep(Sleep),
    Res;
scavenger_get_many(Name, Key, Flags, {Max, TimeOut, Retry, Sleep}) ->
    case catch brick_server:do(Name, node(),
                               [brick_server:make_get_many(Key, Max, Flags)],
                               [ignore_role], TimeOut) of
        [Res] ->
            timer:sleep(Sleep),
            Res;
        _Err ->
            timer:sleep(Sleep),
            scavenger_get_many(Name, Key, Flags, {Max, TimeOut, Retry-1, Sleep})
    end.

prepend_rs(Name, L1, L2) ->
    %% NOTE: Calling storetuple_val/1 twice for each tuple, I don't
    %% think it's worth optimizing....  Rearrange to be the same order
    %% as storetuple internal.
    [{Name, storetuple_key(T), storetuple_ts(T),
      begin {SeqNum, Offset} = storetuple_val(T),
            {abs(SeqNum), Offset}
      end,
      storetuple_vallen(T), storetuple_exptime(T), storetuple_flags(T)} ||
        T <- L1, is_tuple(storetuple_val(T))] ++ L2.

%% @spec(file_handle(), integer(), fun()) ->
%%      {{integer(), integer()}, integer()} | error
-spec copy_one_hunk(tuple(), term(), integer(), integer(), integer(), fun()) ->
                           {{integer(), integer()}, integer()} | error.
copy_one_hunk(#scav{name=Name, wal_mod=WalMod, log=Log}, FH, Key, SeqNum, Offset, Fread_blob) ->
    %% TODO: Use a non-zero length hint here.
    try WalMod:read_hunk_summary(FH, SeqNum, Offset, 0, Fread_blob) of
        Blob when is_binary(Blob) ->
            case WalMod:write_hunk(Log, Name, bigblob_longterm,
                                   Key, ?LOGTYPE_BLOB, [Blob], []) of
                {ok, SeqNum2, Offset2} ->
                    {{SeqNum2, Offset2}, size(Blob)};
                _ ->
                    error
            end;
        NotBlob ->
            ?E_ERROR("Got non blob: ~P", [NotBlob, 10]),
            error
    catch
        _X:_Y ->
            %% Be more helpful and add key to error msg.
            ?E_ERROR("copy_one_hunk: ~p: ~p ~p for key ~p seq ~p Offset ~p (~p)",
                     [Name, _X, _Y, Key, SeqNum, Offset,
                      %% hack: this info isn't too helpful: erlang:get_stacktrace()]),
                      []]),
            error
    end.


%% @doc Once this function returns, the process will be registered
%% with ExclAtom.  If the process is already registered with that name
%% or any other name, it will wait forever.  If the process will live
%% beyond the reasonable scope of the exclusion, the process must
%% unregister the name or merely die to free the name.

-spec really_cheap_exclusion(atom(),fun(),fun()) -> fun().
really_cheap_exclusion(ExclAtom, Fwait, Fgo) ->
    gmt_loop:do_while(
      fun(Count) ->
              case (catch register(ExclAtom, self())) of
                  true ->
                      Fgo(),
                      {false, Count};
                  _ ->
                      if Count =:= 0 -> Fwait();
                         true       -> ok
                      end,
                      timer:sleep(1000),
                      {true, Count + 1}
              end
      end, 0).

%% get_bw_ticket(Bytes) ->
%%     brick_ticket:get(get(zzz_throttle_pid), Bytes).

-spec file_input_fun(term(),term()) -> fun((atom())->end_of_input|{error,term()}).
file_input_fun(Log, Cont) ->
    fun(close) ->
            ok;
       (read) ->
            case disk_log:chunk(Log, Cont) of
                {error, Reason} ->
                    {error, Reason};
                {Cont2, Terms} ->
                    {Terms, file_input_fun(Log, Cont2)};
                {Cont2, Terms, _Badbyte} ->
                    {Terms, file_input_fun(Log, Cont2)};
                eof ->
                    end_of_input
            end
    end.

-spec file_output_fun(term()) -> fun((term()) -> ok | fun()).
file_output_fun(Log) ->
    fun(close) ->
            disk_log:close(Log);
       (Terms) ->
            ok = disk_log:log_terms(Log, Terms),
            file_output_fun(Log)
    end.

%% sort_test0() ->
%%     {ok, TmpLog} = disk_log:open([{name, foo}, {file, "/tmp/footest"}]),
%%     _ = [disk_log:log(TmpLog, {xo, X, Y})
%%          || X <- lists:seq(1, 50), Y <- lists:seq(1,100)],
%%     ok = disk_log:close(TmpLog),
%%     {ok, InLog} = disk_log:open([{name, in}, {file, "/tmp/footest"}, {mode, read_only}]),
%%     {ok, OutLog} = disk_log:open([{name, out}, {file, "/tmp/footest.out"}]),
%%     X = file_sorter:sort(file_input_fun(InLog, start), file_output_fun(OutLog),
%%                          [{format, term},
%%                           {order, fun({_,_,A}, {_,_,B}) -> A < B end}]),
%%     ok = disk_log:close(InLog),
%%     ok = disk_log:close(OutLog),
%%     X.


%%
%% bcb: Brick Callback Functions
%%

bcb_handle_do({do, _SentAt, DoOpList, DoFlags}=DoOp, From, S) ->
    %% do_do/6 reads/writes blob values from/to disk, and build
    %% the metadata modification list (thisdo_mods).
    case do_do(DoOpList, DoFlags, S#state{thisdo_mods=[]}, check_dirty) of
        has_dirty_keys ->
            DQI = #dirty_q{from=From, do_op=DoOp},
            DirtyQ = S#state.wait_on_dirty_q,
            {noreply, S#state{wait_on_dirty_q=queue:in(DQI, DirtyQ)}};
        {no_dirty_keys, {Reply, S1}} ->
            if S1#state.read_only_p, S1#state.thisdo_mods =/= [] ->
                    %% This is a tail brick.
                    {up1_read_only_mode, From, DoOp, S1};
               true ->
                    %% #state.thisdo_mods will be in reverse order of DoOpList!
                    Thisdo_Mods = lists:reverse(S1#state.thisdo_mods),

                    %% Write the metadata modifications to the common log.
                    SyncOverride = proplists:get_value(sync_override, DoFlags),
                    case log_mods(S1, SyncOverride) of
                        {goahead, #state{logging_op_serial=LoggingSerial, n_do=N_do}=S2} ->
                            %% Apply the metadata modifications to ctab
                            ok = map_mods_into_ets(Thisdo_Mods, S2),
                            case proplists:get_value(local_op_only_do_not_forward, DoFlags) of
                                true ->
                                    {reply, Reply, S2};
                                _ ->
                                    %% Send the modifications to downstream
                                    ToDos = [{chain_send_downstream, LoggingSerial,
                                              DoFlags, From, Reply, Thisdo_Mods}],

                                    %% If we've got modifications, we should be a head
                                    %% brick. Therefore we need to increment LoggingSerial.
                                    %% If we're other brick, we must not change LiggingSerial.
                                    LoggingSerial2 =
                                        case Thisdo_Mods of
                                            [] ->
                                                LoggingSerial;
                                            _ ->
                                                LoggingSerial + 1
                                        end,
                                    {up1, ToDos,
                                     {noreply, S2#state{n_do=N_do + 1,
                                                        logging_op_serial=LoggingSerial2}}}
                            end;
                        {wait, WALSyncTicket, #state{logging_op_serial=LoggingSerial, n_do=N_do}=S2} ->
                            %% Apply the metadata modifications to dirty tab
                            add_mods_to_dirty_tab(Thisdo_Mods, S2),

                            LQI = #log_q{
                                     time=brick_metrics:histogram_timed_begin(logging_op_latencies),
                                     logging_serial=LoggingSerial,
                                     thisdo_mods=Thisdo_Mods,
                                     doflags=DoFlags,
                                     from=From, reply=Reply},
                            S3 = push_logging_op_q(LQI, WALSyncTicket, S2),

                            %% {wait, _, _} means we've got modifications and we should
                            %% be a head brick. Therefore we need to increment LoggingSerial.
                            {noreply, S3#state{n_do=N_do + 1,
                                               logging_op_serial=LoggingSerial + 1}}
                    end
            end;
        {noreply_now, S1} ->
            {noreply, S1};
        silently_drop_reply ->
            %% Somewhere deep in the bowels of the do list processing,
            %% we hit some bad data.  We can't form a good answer, so
            %% don't reply at all.
            {noreply, S}
    end.

bcb_handle_info({wal_sync, WALSyncTicket, ok}, State) ->
    case pop_logging_op_q(WALSyncTicket, State) of
        none ->
            error({logging_op_queue, missing, WALSyncTicket});
        {LogQ_All, LogQ_ToDos, State1} ->
            [ ok = map_mods_into_ets(DoOpList, State1) ||
                #log_q{thisdo_mods = DoOpList} <- LogQ_ToDos ],
            [ ok = clear_dirty_tab(DoOpList, State1) ||
                #log_q{thisdo_mods = DoOpList} <- LogQ_ToDos ],
            lists:foreach(
              fun(#log_q{time=Begin}) ->
                      try
                          brick_metrics:histogram_timed_notify(Begin)
                      catch
                          _:_=Err ->
                              ?E_WARNING("Failed to record logging_op_latencies metrics. ~p, ~p",
                                         [Begin, Err])
                      end
              end, LogQ_All),
            ToDos = [ {chain_send_downstream,
                       LQI#log_q.logging_serial,
                       LQI#log_q.doflags,
                       LQI#log_q.from,
                       LQI#log_q.reply,
                       LQI#log_q.thisdo_mods} || LQI <- LogQ_ToDos ],
            %% CHAIN TODO: ISSUE001: Does the ToDos list (above) contain mods
            %%             for the dirty keys?  Or is the attempt below to
            %%             resubmit the dirty keys truly independent??
            %% put(issue001, ToDos), ... looks like they're independent, whew!
            {State2, ToDos2} = retry_dirty_key_ops(State1),
            %% ?DBG_TLOG("sync_done_dirty_q ~w, ~w", [State2#state.name, State2#state.wait_on_dirty_q]),

            LastLogSerial = (lists:last(LogQ_All))#log_q.logging_serial,
            {up1_sync_done, LastLogSerial, ToDos ++ ToDos2, State2}
    end;
bcb_handle_info(do_init_second_half, #state{name=Name}=State) ->
    ?E_INFO("do_init_second_half: ~w", [Name]),

    ok = brick_hlog_writeback:full_writeback(),
    ?E_INFO("Loading metadata records for brick ~w", [Name]),
    Start = os:timestamp(),
    {LoadCount, ErrList} = load_metadata(State),
    Elapse = timer:now_diff(os:timestamp(), Start) div 1000,

    ZeroDiskErrorsP = (ErrList =:= []),
    if
        ZeroDiskErrorsP ->
            ?E_INFO("Finished loading metadata records for brick ~w [~w ms]. "
                    "key count: ~w, no error",
                    [Name, Elapse, LoadCount]);
        true ->
            ErrorCount = length(ErrList),
            Message =
                lists:flatten(
                  io_lib:format("Could net load ~w metadata records for brick ~w, "
                                "must recover via chain replication.~n"
                                "\tloaded key count: ~w~n"
                                "\terror key count:  ~w~n"
                                "\terror details:    ~p~n"
                                "\telapse: ~w ms",
                                [ErrorCount, Name, LoadCount, ErrorCount,
                                 ErrList, Elapse])),
            ?E_CRITICAL(Message, []),
            gmt_util:set_alarm({load_metadata, Name}, Message, fun() -> ok end)
    end,

    %% For recently bigdata_dir files, sync them all in a big safety blatt.
    %% ... Except that it also seems to have the potential to slam the
    %% ... disk system really, really hard, which can (in turn) create
    %% ... extremely bad timeout conditions.  {sigh}  So, for now,
    %% ... comment out the sync.
    %% os:cmd("sync"),

    %% Set these timers only after the WAL scan is finished.
    %% brick_itimer:send_interval(1*1000, qqq_debugging_only),
    {ok, ExpiryTRef} = brick_itimer:send_interval(1 * 1000, check_expiry),

    ?E_INFO("do_init_second_half: ~p finished", [State#state.name]),
    self() ! {storage_layer_init_finished, State#state.name, ZeroDiskErrorsP},
    {noreply, State#state{expiry_tref = ExpiryTRef,
                          scavenger_tref = undefined}};
bcb_handle_info(Msg, State) ->
    ?E_ERROR("========================== Unhandled bcb_handle_info message: ~P (~p)", [Msg, 40, ?MODULE]),
    ?MODULE:handle_info(Msg, State).


%% @doc Flush all volatile data to stable storage <b>synchronously</b>.

bcb_force_flush(#state{md_store=_MdStore}) ->
    %% @TODO (new hlog) Temporary disabled.
    %% catch MdStore:sync().
    ok.

%% @doc Flush chain replication logging serial # stable storage asynchronously.

bcb_async_flush_log_serial(LoggingSerial, #state{md_store=MdStore}=S) ->
    WALSyncTicket = MdStore:request_group_commit(),
    LQI = #log_q{
             time=brick_metrics:histogram_timed_begin(logging_op_latencies),
             logging_serial=LoggingSerial,
             is_bcb_async_flush_request=true
            },
    push_logging_op_q(LQI, WALSyncTicket, S).

-spec push_logging_op_q(log_q(), brick_hlog_wal:callback_ticket(), state_r()) -> state_r().
push_logging_op_q(LoggingOp, WALSyncTicket, #state{logging_op_q=LoggingOpQueue}=S) ->
    QueueForTicket =
        case gb_trees:lookup(WALSyncTicket, LoggingOpQueue) of
            none ->
                queue:new();
            {value, Q} ->
                Q
        end,
    LoggingOpQueue2 = gb_trees:enter(WALSyncTicket,
                                     queue:in(LoggingOp, QueueForTicket),
                                     LoggingOpQueue),
    S#state{logging_op_q=LoggingOpQueue2}.

-spec pop_logging_op_q(brick_hlog_wal:callback_ticket(), state_r()) ->
                              none() | {LogQ_All::[log_q()], LogQ_ToDos::[log_q()], state_r()}.
pop_logging_op_q(WALSyncTicket, #state{logging_op_q=LoggingOpQueue}=S) ->
    case gb_trees:lookup(WALSyncTicket, LoggingOpQueue) of
        none ->
            none;
        {value, QueueForTicket} ->
            LogQ_All = queue:to_list(QueueForTicket),
            LogQ_ToDos = lists:filter(fun(#log_q{is_bcb_async_flush_request=Bool}) ->
                                              not Bool
                                      end, LogQ_All),
            S1 = S#state{logging_op_q=gb_trees:delete(WALSyncTicket, LoggingOpQueue)},
            {LogQ_All, LogQ_ToDos, S1}
    end.

bcb_keys_for_squidflash_priming(DoList, DoFlags, State) ->
    SF_resubmit = proplists:get_value(squidflash_resubmit, DoFlags) =:= true, %% true or undefined
    case SF_resubmit of
        true ->
            %% ?E_DBG("squidflash_resubmit", []),
            [];
        false ->
            lists:foldl(
              fun({get, Key, Flags}, Acc) ->
                      case {proplists:get_value(witness, Flags),
                            my_lookup(State, Key, false)} of
                          {true, _} ->
                              Acc;
                          {undefined, []} ->
                              Acc;
                          {undefined, [StoreTuple]} ->
                              accumulate_maybe(Key, StoreTuple, Acc)
                      end;

                 ({get_many, Key, Flags}, Acc) ->
                      case (proplists:get_value(witness, Flags, false) orelse
                            proplists:get_value(get_many_raw_storetuples, Flags, false)) of
                          true ->
                              Acc;
                          false ->
                              %% Bummer, get_many1() won't give us raw val
                              %% tuples.
                              {X, _S2} = get_many1(Key, [witness|Flags],
                                                   true, DoFlags, State),
                              {L, _MoreP} = X,
                              F_look = fun(K, Acc2) ->
                                               [StoreTuple] = my_lookup(State, K, false),
                                               accumulate_maybe(Key, StoreTuple, Acc2)
                                       end,
                              lists:foldl(fun({K, _TS}, Acc2) ->
                                                  F_look(K, Acc2);
                                             ({K, _TS, _Flags}, Acc2) ->
                                                  F_look(K, Acc2)
                                          end, [], L) ++ Acc
                      end;

                 ({rename, Key, _Timestamp, _NewKey, _ExpTime, _Flags}, Acc) ->
                      case my_lookup(State, Key, false) of
                          [] ->
                              Acc;
                          [StoreTuple] ->
                              case is_sequence_frozen(StoreTuple) of
                                  true ->
                                      accumulate_maybe(Key, StoreTuple, Acc);
                                  false ->
                                      Acc
                              end;
                          false ->
                              Acc
                      end;

                 (_, Acc) ->
                      Acc
              end, [], DoList)
    end.

bcb_keys_for_squidflash_priming(DoMods, State) ->
    lists:foldl(
      fun({insert_existing_value, _StoreTuple, OldKey, _OldTimestamp}, Acc) ->
              case is_sequence_frozen(OldKey, State) of
                  true ->
                      case my_lookup(State, OldKey, false) of
                          [] ->

                              Acc;
                          [StoreTuple] ->
                              accumulate_maybe(OldKey, StoreTuple, Acc)
                      end;
                  false ->
                      Acc
              end;
         (_, Acc) ->
              Acc
      end, [], DoMods).

accumulate_maybe(Key, StoreTuple, Acc) ->
    case storetuple_val(StoreTuple) of
        Val when is_tuple(Val) ->
            ValLen = storetuple_vallen(StoreTuple),
            [{Key, Val, ValLen}|Acc];
        no_blob ->
            Acc;
        Blob when is_binary(Blob) ->
            Acc
    end.

is_sequence_frozen(StoreTuple) ->
    case storetuple_val(StoreTuple) of
        Tuple when is_tuple(Tuple) ->  %% record #w{} or #p{}
            %% @TODO: Implement brick_blob_store:is_sequence_frozen/1.
            true;
        no_blob ->
            false;
        Blob when is_binary(Blob) ->
            false
    end.

is_sequence_frozen(Key, State) ->
    case key_exists_p(Key, [], State, false) of
        StoreTuple when is_tuple(StoreTuple) ->
            is_sequence_frozen(StoreTuple);
        _ ->
            false
    end.

%% @doc Write values and Thisdo_Mods list to disk.
bcb_log_mods(Thisdo_Mods, LoggingSerial, S) when is_record(S, state) ->
    ?DBG_ETS("bcb_log_mods_to_storage ~w: ~w", [S#state.name, Thisdo_Mods]),
    LocalMods = filter_mods_from_upstream(Thisdo_Mods, S),
    case write_metadata_term(LocalMods, S, undefined, false) of
        {goahead, S2} ->
            {goahead, S2#state{logging_op_serial = LoggingSerial + 1}, LocalMods};
        {wait, S2} ->
            {wait, S2#state{logging_op_serial = LoggingSerial + 1}, LocalMods}
    end.

%% @doc Convert Thisdo_Mods list to internal storage.

bcb_map_mods_to_storage(Thisdo_Mods, S) when is_record(S, state) ->
    ?DBG_ETS("bcb_map_mods_to_storage ~w: ~w", [S#state.name, Thisdo_Mods]),
    ok = map_mods_into_ets(Thisdo_Mods, S),
    ok = clear_dirty_tab(Thisdo_Mods, S),
    S.

%% @doc Get {key,ts,val,exptime,flags} for each key in list.
%%
%% This function is used for the "diff" style of repair.  We have to
%% cope with the cases where the key was deleted between rounds 1 and
%% 2.  The tuple we return must be a valid storetuple!
%%
%% Because this function is used *only* for repair, we take advantage:
%%
%% The 'value_in_ram' atom doesn't belong in the Flags list.  However,
%% for repair purposes, we want all downstream bricks to use the same
%% val blob storage method, RAM or disk, that the head brick uses.
%% We'll add the 'value_in_ram' atom to the Flags list if we're
%% storing the val in RAM, and rely on the downstream to do the right
%% thing.

bcb_get_list(Keys, S) when is_record(S, state) ->
    F_ram = fun(Key, Flags) ->
                    [ST] = my_lookup(S, Key, false),
                    case storetuple_val(ST) of
                        Bin when is_binary(Bin) ->
                            [value_in_ram|Flags];
                        _ ->
                            Flags
                    end
            end,
    lists:reverse(lists:foldl(
                    fun(Key, Acc) ->
                            %% NOTE: get_all_attribs + witness is not honored.
                            case get_key(Key, [get_all_attribs], S) of
                                {ok, Ts, Val, ExpTime, Flags} ->
                                    Fs0 = lists:keydelete(val_len, 1, Flags),
                                    Fs = F_ram(Key, Fs0),
                                    [{Key, Ts, Val, ExpTime, Fs}|Acc];
                                key_not_exist ->
                                    Acc
                            end
                    end, [], Keys)).

%% @spec (term(), list(), state_r()) ->
%%       {{list(), true | false}, state_r()}
%% @doc Get a keys, starting with Key, in reverse order.
%%
%% NOTE: There is no argument for DoFlags: it is hardcoded as `[]'.
%%       Therefore bcb_get_many should never be called when sweep zone
%%       key filtering is assumed to be done by this function.

bcb_get_many(Key, Flags, S) when is_record(S, state) ->
    DoFlags = [],
    %% The externally-visible atoms ?BRICK__GET_MANY_FIRST and
    %% ?BRICK__GET_MANY_LAST are the same atoms used by ETS for
    %% beginning and end of a table ... no need to remap them here.
    get_many1(Key, Flags, false, DoFlags, S).

%% @doc Perform an iteration of the repair loop, beginning with FirstKey.

bcb_repair_loop(RepairList, FirstKey, S) when is_record(S, state) ->
    repair_loop(RepairList, FirstKey, S).

%% @doc Perform an iteration of round 1 the diff repair loop, beginning with FirstKey.

bcb_repair_diff_round1(RepairList, FirstKey, S) when is_record(S, state) ->
    ?DBG_TLOG("qqq_bcb_repair_diff_round1 ~w: ~w", [S#state.name, RepairList]),
    repair_diff_round1(RepairList, FirstKey, S).

%% @doc Perform an iteration of round 2 the diff repair loop

bcb_repair_diff_round2(RepairList, Deletes, S) when is_record(S, state) ->
    ?DBG_REPAIR("qqq_bcb_repair_diff_round2 ~w: ~w", [S#state.name, RepairList]),
    repair_diff_round2(RepairList, Deletes, S).

%% @doc Delete all remaining keys, starting with Key.

bcb_delete_remaining_keys(?BRICK__GET_MANY_FIRST, S)
  when is_record(S, state) ->
    delete_remaining_keys(ets:first(S#state.ctab), S);
bcb_delete_remaining_keys(Key, S) ->
    delete_remaining_keys(Key, S).

%% @doc Enable/disable read-only mode.

bcb_set_read_only_mode(ReadOnlyP, S) when is_record(S, state) ->
    ?DBG_GEN("bcb_set_read_only_mode ~w: ~w", [S#state.name, ReadOnlyP]),
    S#state{read_only_p = ReadOnlyP}.

%% @doc Get a brick-private metadata key/value pair.

bcb_get_metadata(Key, S) when is_record(S, state) ->
    ets:lookup(S#state.mdtab, Key).

%% @doc Set a brick-private metadata key/value pair.
%%
%% WARNING: This is a blocking, synchronous disk I/O operation.

bcb_set_metadata(Key, Val,
                 #state{mdtab=MdTab, do_sync=DoSync, md_store=_MdStore}=S) ->
    T = {Key, Val},
    ets:insert(MdTab, T),
    Thisdo_Mods = [{md_insert, T}],
    {_, NewS} = write_metadata_term(Thisdo_Mods, S, undefined, false),
    if DoSync ->
            %% @TODO (new hlog) Temporary disabled.
            %% catch MdStore:sync();
            ok;
       true ->
            ok
    end,
    NewS.

%% @doc Delete a brick-private metadata key/value pair.
%%
%% WARNING: This is a blocking, synchronous disk I/O operation.

bcb_delete_metadata(Key,
                    #state{mdtab=MdTab, do_sync=DoSync, md_store=_MdStore}=S) ->
    ets:delete(MdTab, Key),
    Thisdo_Mods = [{md_delete, Key}],
    {_, NewS} = write_metadata_term(Thisdo_Mods, S, undefined, false),
    if DoSync ->
            %% @TODO (new hlog) Temporary disabled.
            %% catch MdStore:sync();
            ok;
       true ->
            ok
    end,
    NewS.

%% @doc Callback to add thisdo_mods() to the implementation's dirty key table.

bcb_add_mods_to_dirty_tab(Thisdo_Mods, S) when is_record(S, state) ->
    add_mods_to_dirty_tab(Thisdo_Mods, S),
    %%?E_WARNING("EEE: ~w new dirty = ~p", [S#state.name, ets:tab2list(S#state.dirty_tab)]),
    S.

%% @spec (term(), term(), state_r()) ->
%%       list({term(), insert, store_tuple()} | {term(), delete, delete})
%% @doc Return a list of dirty_tab entries for any key equal to or between
%% StartKey and EndKey.

bcb_dirty_keys_in_range(?BRICK__GET_MANY_FIRST, EndKey, S)
  when is_record(S, state) ->
    bcb_dirty_keys_in_range(<<>>, EndKey, S);
bcb_dirty_keys_in_range(StartKey, EndKey, S)
  when is_record(S, state) ->
    DirtyList = ets:tab2list(S#state.dirty_tab),
    ?DBG_GEN("bcb_dirty_keys_in_range ~w: startkey ~w, endkey ~w, dirty ~w",
             [S#state.name, StartKey, EndKey, DirtyList]),
    L = if EndKey =:= ?BRICK__GET_MANY_LAST ->
                [Dirty || {Key,_,_} = Dirty <- DirtyList,
                          Key >= StartKey];
           true ->
                [Dirty || {Key,_,_} = Dirty <- DirtyList,
                          Key >= StartKey, Key =< EndKey]
        end,
    lists:map(fun({Key, insert, T}) ->
                      {Key, insert, storetuple_to_externtuple(T, S)};
                 (X) ->
                      X
              end, L).

%% @spec (state_r()) -> {state_r(), todo_list()}
%% @doc Callback kludge to avoid having do ops being stranded forever
%%      (or until the next op that triggers a log write) after a
%%      migration sweep made the same do op key dirty.

bcb_retry_dirty_key_ops(S) when is_record(S, state) ->
    retry_dirty_key_ops(S).

%% @spec (term(), boolean(), state_r()) -> list()
%% @doc Callback to lookup a key in the implementation's back-end.
%%
%% It isn't advised to use MustHaveVal_p = true, because it's quite
%% possible that a deadlock or other nasty business might happen.

bcb_lookup_key(Key, MustHaveVal_p, S) when is_record(S, state) ->
    case my_lookup(S, Key, MustHaveVal_p) of
        [T] ->
            [storetuple_to_externtuple(T, S)];
        [] ->
            []
    end.

%% @spec (term(), boolean(), state_r()) -> list({boolean(), tuple()})
%% @doc Callback to lookup a key in the implementation's back-end.

bcb_lookup_key_storetuple(Key, false = MustHaveVal_p, S)
  when is_record(S, state) ->
    my_lookup(S, Key, MustHaveVal_p);
bcb_lookup_key_storetuple(Key, true = _MustHaveVal_p, S)
  when is_record(S, state) ->
    case my_lookup(S, Key, false) of
        [ST] ->
            [ST2] = my_lookup(S, Key, true),
            RawVal = storetuple_val(ST),
            if
                is_binary(RawVal) ->
                    [{true, ST2}];
                true ->
                    [{false, ST2}]
            end;
        [] ->
            []
    end.

%% @spec (state_r()) -> time()
%% @doc Return the brick's start time.

bcb_start_time(S) when is_record(S, state) ->
    S#state.start_time.

%% @spec (state_r(), term(), term()) -> term()
%% @doc Return an opaque term to be used when an upper layer wishes to update
%%      a key while preserving the key's current value (e.g. to update
%%      flags only).

bcb_val_switcharoo(Old, New, S) when is_record(S, state) ->
    {?VALUE_SWITCHAROO, Old, New}.

%% @spec (thisdo_mods(), state_r()) -> thisdo_mods()
%% @doc Callback for filter_mods_for_downstream().

bcb_filter_mods_for_downstream(Thisdo_Mods, S) when is_record(S, state) ->
    filter_mods_for_downstream(Thisdo_Mods).

%% @spec (state_r()) -> {term(), state_r()}
%% @doc Return an opaque term (to upper layers) that (in our context)
%% will be a thisdo_mod {log_noop}.

bcb_make_log_replay_noop(S) when is_record(S, state) ->
    {Serial, NewS} = bcb_incr_logging_serial(S),
    %% TODO: This is too cozy with brick_server.erl's role, clean up
    %%       and separate it.
    {{ch_log_replay_v2, {S#state.name, node()}, Serial, [{log_noop}], <<"no">>,
      list_to_binary(["no", integer_to_list(Serial)]), ignoreme_fixme},
     NewS}.

%% @spec (state_r()) -> {integer(), state_r()}
%% @doc Increment the ImplState's logging serial number.

bcb_incr_logging_serial(S) when is_record(S, state) ->
    Serial = S#state.logging_op_serial,
    {Serial, S#state{logging_op_serial = Serial + 1}}.

%% @spec (state_r()) -> integer()
%% @doc Return the current value the ImplState's logging serial number.

bcb_peek_logging_serial(S) when is_record(S, state) ->
    S#state.logging_op_serial.


%% @doc Return the current length the ImplState's logging op queue
-spec bcb_peek_logging_op_q_len(state_r()) -> non_neg_integer().
bcb_peek_logging_op_q_len(#state{logging_op_q=LoggingOpQueue}) ->
    lists:foldl(fun(Q, Acc) ->
                        Acc + queue:len(Q)
                end, 0, gb_trees:values(LoggingOpQueue)).

%% @doc Return the current value the ImplState's logging op queue in list form.
-spec bcb_get_logging_op_q(state_r()) -> [log_q()].
bcb_get_logging_op_q(#state{logging_op_q=LoggingOpQueue}) ->
    Queues = lists:foldl(fun(Q, Acc) ->
                                 [queue:to_list(Q) | Acc]
                end, [], gb_trees:values(LoggingOpQueue)),
    lists:flatten(lists:reverse(Queues)).

%% @spec (state_r()) -> proplist()
%% @doc Return the ImplState's status in proplist form (the same proplist
%% that is returned by the brick_server:status() call).

bcb_status(S) when is_record(S, state) ->
    {ok, Ps} = do_status(S),
    Ps.

%% @spec (integer(), state_r()) -> integer()
%% @doc Purge all records involving log sequence number SeqNum.

bcb_common_log_sequence_file_is_bad(SeqNum, S) when is_record(S, state) ->
    purge_recs_by_seqnum(SeqNum, S).


%% %% On my machine at home:
%% %% dict_test1 is about twice as slow as dict_test2, according to timer:tc().

%% %% D1 = dict:store({key, key1}, <<"valvalval">>, dict:new()).
%% %% timer:tc(brick_server, dict_test1, [{key, key1}, {key, key2}, D1]).
%% %% Result: 9-10 usec.

%% dict_test1(K1, K2, Dict) ->
%%     Xa1 = dict:find(K1, Dict),
%%     Xa2 = dict:find(K1, Dict),
%%     Xa3 = dict:find(K1, Dict),
%%     Xa4 = dict:find(K1, Dict),
%%     Xa5 = dict:find(K1, Dict),
%%     Xb1 = dict:find(K2, Dict),
%%     Xb2 = dict:find(K2, Dict),
%%     Xb3 = dict:find(K2, Dict),
%%     Xb4 = dict:find(K2, Dict),
%%     Xb5 = dict:find(K2, Dict),
%%     {Xa1, Xa2, Xa3, Xa4, Xa5, Xb1, Xb2, Xb3, Xb4, Xb5}.

%% %% put({key, key1}, <<"asdflkj asfdlkj asdf">>).
%% %% timer:tc(brick_server, dict_test2, [{key, key1}, {key, key2}]).
%% %% Result: 5-6 usec.

%% dict_test2(K1, K2) ->
%%     Xa1 = get(K1),
%%     Xa2 = get(K1),
%%     Xa3 = get(K1),
%%     Xa4 = get(K1),
%%     Xa5 = get(K1),
%%     Xb1 = get(K2),
%%     Xb2 = get(K2),
%%     Xb3 = get(K2),
%%     Xb4 = get(K2),
%%     Xb5 = get(K2),
%%     {Xa1, Xa2, Xa3, Xa4, Xa5, Xb1, Xb2, Xb3, Xb4, Xb5}.

%% debug_scan(Dir) ->
%%     debug_scan(Dir, gmt_hlog).

%% debug_scan(Dir, WalMod) ->
%%     WalMod:fold(
%%       shortterm, Dir,
%%       fun(H, FH, Acc) ->
%%               io:format("Hunk: ~p ~p", [H#hunk_summ.seq, H#hunk_summ.off]),
%%               if H#hunk_summ.type =:= ?LOGTYPE_METADATA ->
%%                       CB = WalMod:read_hunk_member_ll(FH, H, md5, 1),
%%                       io:format("~P", [binary_to_term(CB), 40]);
%%                  H#hunk_summ.type =:= ?LOGTYPE_BLOB ->
%%                       io:format("blob len: ~p", [hd(H#hunk_summ.c_len)]);
%%                  true ->
%%                       io:format("Unknown hunk type: ~p, ~p", [H#hunk_summ.type, H])
%%               end,
%%               Acc + 1
%%       end, 0).

%% debug_scan2(Dir) ->
%%     debug_scan2(Dir, gmt_hlog).

%% debug_scan2(Dir, WalMod) ->
%%     WalMod:fold(
%%       shortterm, Dir,
%%       fun(H, FH, {{Is,Ds,DAs}=Foo, MDs, Bls, Os}) ->
%%               if H#hunk_summ.type =:= ?LOGTYPE_METADATA ->
%%                       CB = WalMod:read_hunk_member_ll(FH, H, md5, 1),
%%                       Mods = binary_to_term(CB),
%%                       {I, D, DA} =
%%                           lists:foldl(
%%                             fun(T, {Ix, Dx, DAx}) when element(1, T) =:= insert ->
%%                                     {Ix+1, Dx, DAx};
%%                                (T, {Ix, Dx, DAx}) when element(1, T) =:= delete
%%                                                        orelse element(1, T) =:= delete_noexptime ->
%%                                     {Ix, Dx+1, DAx};
%%                                (T, {Ix, Dx, DAx}) when element(1, T) =:= delete_all_table_items ->
%%                                     {Ix, Dx, DAx+1};
%%                                (_, Acc) ->
%%                                     Acc
%%                             end, {0, 0, 0}, Mods),
%%                       {{Is+I, Ds+D, DAs+DA}, MDs+1, Bls, Os};
%%                  H#hunk_summ.type =:= ?LOGTYPE_BLOB ->
%%                       CB1 = WalMod:read_hunk_member_ll(FH, H, md5, 1),
%%                       H2 = H#hunk_summ{c_blobs = [CB1]},
%%                       true = gmt_hlog:md5_checksum_ok_p(H2),
%%                       {Foo, MDs, Bls+1, Os};
%%                  true ->
%%                       CB2 = WalMod:read_hunk_member_ll(FH, H, md5, 1),
%%                       H2 = H#hunk_summ{c_blobs = [CB2]},
%%                       true = gmt_hlog:md5_checksum_ok_p(H2),
%%                       {Foo, MDs, Bls, Os+1}
%%               end
%%       end, {{0,0,0}, 0, 0, 0}).

%% debug_scan3(Dir, EtsTab) ->
%%     debug_scan3(Dir, EtsTab, gmt_hlog).

%% debug_scan3(Dir, EtsTab, WalMod) ->
%%     WalMod:fold(
%%       shortterm, Dir,
%%       fun(H, FH, _Acc) ->
%%               if H#hunk_summ.type =:= ?LOGTYPE_METADATA ->
%%                       CB = WalMod:read_hunk_member_ll(FH, H, md5, 1),
%%                       Mods = binary_to_term(CB),
%%                       lists:foreach(
%%                         fun(T) when element(1, T) =:= insert ->
%%                                 Key = element(1, element(2, T)),
%%                                 ets:insert(EtsTab, {Key, x});
%%                            (T) when element(1, T) =:= delete orelse element(1, T) =:= delete_noexptime ->
%%                                 Key = element(1, element(2, T)),
%%                                 ets:delete(EtsTab, Key);
%%                            (T) when element(1, T) =:= delete_all_table_items ->
%%                                 ets:delete_all_objects(EtsTab);
%%                            (_) ->
%%                                 blah
%%                         end, Mods),
%%                       blah;
%%                  H#hunk_summ.type =:= ?LOGTYPE_BLOB ->
%%                       blah;
%%                  true ->
%%                       blah
%%               end
%%       end, unused).

%% debug_scan4(Dir, OutFile) ->
%%     debug_scan4(Dir, OutFile, gmt_hlog).

%% debug_scan4(Dir, OutFile, WalMod) ->
%%     {ok, OutFH} = file:open(OutFile, [write, delayed_write]),
%%     X = WalMod:fold(
%%           shortterm, Dir,
%%           fun(H, FH, _Acc) ->
%%                   if H#hunk_summ.type =:= ?LOGTYPE_METADATA ->
%%                           CB = WalMod:read_hunk_member_ll(FH, H, md5, 1),
%%                           Mods = binary_to_term(CB),
%%                           lists:foreach(
%%                             fun(T) when element(1, T) =:= insert orelse element(1, T) =:= insert_value_into_ram ->
%%                                     Key = element(1, element(2, T)),
%%                                     io:format(OutFH, "i ~p", [Key]);
%%                                (T) when element(1, T) =:= delete orelse element(1, T) =:= delete_noexptime ->
%%                                     Key = element(2, T),
%%                                     io:format(OutFH, "d ~p", [Key]);
%%                                (T) when element(1, T) =:= delete_all_table_items ->
%%                                     io:format(OutFH, "delete_all_table_items ", []);
%%                                (T) ->
%%                                     io:format(OutFH, "? ~p", [T])
%%                             end, Mods),
%%                           blah;
%%                      H#hunk_summ.type =:= ?LOGTYPE_BLOB ->
%%                           blah;
%%                      true ->
%%                           blah
%%                   end
%%           end, unused),
%%     ok = file:close(OutFH),
%%     X.
