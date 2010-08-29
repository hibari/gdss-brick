%%%-------------------------------------------------------------------
%%% Copyright: (c) 2007-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%% <li> Table checkpointing (though not fully implemented, e.g. automatic
%%   checkpoints at prescribed times or when logs get too big or .... ) </li>
%% </ul>
%%
%% With review and inevitable dev growth, I expect several of these
%% tasks to move out of this module, probably back into
%% `brick_server.erl'.  Also, a lot of this code is brute-force and
%% may need some beautification.
%%

-module(brick_ets).
-include("applog.hrl").


-behaviour(gen_server).
-compile({inline_size,24}).                     % 24 is the default

-include("brick.hrl").
-include("brick_public.hrl").
-include("brick_hash.hrl").
-include("gmt_hlog.hrl").

-include_lib("kernel/include/file.hrl").

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

-export([debug_scan/1, debug_scan/2, debug_scan2/1, debug_scan2/2,
         debug_scan3/2, debug_scan3/3, debug_scan4/2, debug_scan4/3]).
-export([verbose_expiration/2]).

%% External exports
-export([start_link/2, stop/1, dump_state/1, sync_stats/2, brick_name2data_dir/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Brick callbacks
-export([bcb_force_flush/1, bcb_async_flush_log_serial/2,
         bcb_log_mods/3, bcb_map_mods_to_storage/2,
         bcb_get_many/3, bcb_get_list/2, bcb_repair_loop/3,
         bcb_repair_diff_round1/3, bcb_repair_diff_round2/3,
         bcb_delete_remaining_keys/2,
         bcb_set_read_only_mode/2,
         bcb_get_metadata/2, bcb_set_metadata/3, bcb_delete_metadata/2,
         bcb_add_mods_to_dirty_tab/2, bcb_dirty_keys_in_range/3,
         bcb_retry_dirty_key_ops/1,
         bcb_lookup_key/3, bcb_lookup_key_storetuple/3,
         bcb_start_time/1,
         bcb_val_switcharoo/3, bcb_filter_mods_for_downstream/2,
         bcb_make_log_replay_noop/1,
         bcb_incr_logging_serial/1, bcb_peek_logging_serial/1,
         bcb_peek_logging_op_q_len/1, bcb_get_logging_op_q/1,
         bcb_status/1, bcb_common_log_sequence_file_is_bad/2]).
-export([disk_log_fold/3, disk_log_fold_bychunk/3]).

%% Internal exports
-export([sync_pid_loop/1, checkpoint_start/4]).
%% Debug/temp/benchmarking exports
-export([sync_pid_start/1]).
-export([file_input_fun/2, file_output_fun/1, sort_test0/0, slurp_log_chunks/1]).
%% For gmt_hlog_common's scavenger support.
-export([make_info_log_fun/1, really_cheap_exclusion/3, scavenger_get_keys/5,
         count_live_bytes_in_log/1, temp_path_to_seqnum/1, which_path/3,
         delete_seq/2, scavenge_one_seq_file_fun/4, copy_one_hunk/6]).
%% For brick_admin fast sync support
-export([storetuple_make/6]).
%% For gmt_hlog_common's use
-export([sequence_file_is_bad_common/6, append_external_bad_sequence_file/2,
         delete_external_bad_sequence_file/1]).


%%%
%%% ctab format:
%%%    {Key = binary()|term(),         binary is preferred
%%%     TStamp = bignum(),             whatever make_timestamp() makes
%%%     Val = binary()|term(),         binary is preferred
%%%    }
%%%    Flags and ExpTime are _not_ stored at this time.

%%%----------------------------------------------------------------------
%%% Types/Specs/Records
%%%----------------------------------------------------------------------

-export_type([extern_tuple/0, state_r/0]).

-type proplist() :: [ atom() | {any(),any()} ].

-record(state, {
          name :: atom(),                       % My registered name
          options :: proplist(),
          start_time :: {non_neg_integer(),non_neg_integer(),non_neg_integer()},
          do_logging = true :: boolean(),
          do_sync = true :: boolean(),
          log_dir :: string(),
          bigdata_dir :: string(),
          n_add = 0 :: non_neg_integer(),
          n_replace = 0 :: non_neg_integer(),
          n_set = 0 :: non_neg_integer(),
          n_get = 0 :: non_neg_integer(),
          n_get_many = 0 :: non_neg_integer(),
          n_delete = 0 :: non_neg_integer(),
          n_txn = 0 :: non_neg_integer(),
          n_do = 0 :: non_neg_integer(),
          n_expired = 0 :: non_neg_integer(),
          %% CHAIN TODO: Do these 2 items really belong here??
          logging_op_serial = 42 :: non_neg_integer(), % Serial # of logging op
          logging_op_q = queue:new() :: queue(),  % Queue of logged ops for
                                                  % replay when log sync is done.
          %% n_log_replay = 0,
          log :: pid(),                           % disk_log for data mod cmds
          thisdo_mods :: [atom()],                % List of mods by this 'do'
          check_pid :: pid(),                     % Pid of checkpoint helper
          check_lastseqnum :: seqnum(),           % integer() seq # of last checkpoint
          ctab :: table_name(),                   % ETS table for cache
          shadowtab :: table_name(),              % Checkpoint shadow for ctab
          shadowtab_name :: table_name(),         % atom(), name shortcut only
          bypass_shadowtab = false :: boolean(),  % Flag for fold_shadow_into_ctab()
          etab :: table_name(),                   % Expiry index table
          mdtab :: table_name(),                  % Private metadata table
          sync_pid :: pid(),                      % Pid of log sync process
          checkpoint_timer :: reference(),        % Timer for checkpoint
          checkpoint_opts :: proplist(),          % Proplist for checkpoint
          dirty_tab :: table_name(),              % ETS table: dirty key search
          wait_on_dirty_q :: queue(),             % Queue of ops waiting on
                                                  % dirty keys
          read_only_p = false :: boolean(),       % Mirror of upper-level's var.
          expiry_tref :: reference(),             % Timer ref for expiry events
          scavenger_pid :: 'undefined' | pid(),   % undefined | pid()
          scavenger_tref :: reference(),          % tref()
          syncsum_count = 0 :: non_neg_integer(), % DEBUG?? syncsum stats
          syncsum_msec = 0 :: non_neg_integer(),
          syncsum_len = 0 :: integer(),
          syncsum_tref :: reference(),
          do_expiry_fun :: fun(),                 % fun
          wal_mod :: atom()                       % atom()
                     }).


-record(syncpid_arg, {
          parent_pid :: pid(),                  % Pid of syncpid's parent brick
          wal_pid :: pid(),                     % Pid of wal server.
          wal_mod :: atom(),
          name :: atom()                        % Name of brick
                  }).

-include("brick_specs.hrl").
-include("gmt_hlog.hrl").

-type log_fold_fun() :: fun((tuple(),tuple()) -> tuple()).
-type server_ref() :: atom() | {atom(), atom()} | {global, term()} | pid().
-type state_r() :: tuple().

-spec append_external_bad_sequence_file(atom(),integer()) -> ok.
-spec brick_name2data_dir(atom()) -> nonempty_string().
-spec copy_one_hunk(tuple(), term(), integer(), integer(), integer(), fun()) -> {{integer(), integer()}, integer()} | error.
-spec count_live_bytes_in_log(file:fd()) -> integer().
-spec delete_seq(#scav{},integer()) -> ok.
-spec disk_log_fold(log_fold_fun(),tuple()|integer(),term()) -> tuple()|integer().
-spec disk_log_fold_bychunk(fun(([tuple()],integer())->integer()),integer(),term()) -> integer().
-spec file_input_fun(term(),term()) -> fun((atom())->end_of_input|{error,term()}).
-spec file_output_fun(term()) -> fun((term()) -> ok | fun()).
-spec make_info_log_fun(list()) -> fun((_,_) -> 'ok').
-spec really_cheap_exclusion(atom(),fun(),fun()) -> fun().
-spec scavenger_get_keys(brick_server:brick_name(),brick_server:flags_list_many(),
                         key() | ?BRICK__GET_MANY_FIRST,
                         function(), function()) -> ok.
-spec sequence_file_is_bad_common(nonempty_string(), atom(), pid(), atom(),integer(),integer()) -> ok.
-spec storetuple_make(key(), ts(), term(), integer(), exp_time(), flags_list()) -> store_tuple().
-spec temp_path_to_seqnum(nonempty_string()) -> integer().
-spec which_path(_,atom() | tuple(),_) -> {_,atom() | [any()]}.

-spec dump_state(server_ref() | atom()) -> {integer(), list(), state_r()}.

%% #scav moved to brick.hrl

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------
start_link(ServerName, Options) ->
    gen_server:start_link(?MODULE, [ServerName, Options], []).

stop(Server) ->
    gen_server:call(Server, {stop}).

dump_state(Server) ->
    gen_server:call(Server, {dump_state}).

sync_stats(Server, Seconds) when seconds >= 0 ->
    gen_server:call(Server, {sync_stats, Seconds}).

brick_name2data_dir(ServerName) ->
    "hlog." ++ atom_to_list(ServerName).

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
    ?DBG_GENx({qqq_starting, ServerName}),

    process_flag(trap_exit, true),
    ?E_INFO("top of init: ~p, ~p\n", [ServerName, Options]),

    %% Avoid race conditions where a quick restart of the brick
    %% catches up with the not-yet-finished death of the previous log
    %% proc.
    %% io:format("\n\nUSING gmt_hlog, old-school....\n\n\n"), timer:sleep(1000),
    %% WalMod = proplists:get_value(wal_log_module, Options, gmt_hlog),
    WalMod = proplists:get_value(wal_log_module, Options, gmt_hlog_local),
    catch exit(whereis(WalMod:log_name2reg_name(ServerName)), kill),

    DoLogging = proplists:get_value(do_logging, Options, true),
    DoSync = proplists:get_value(do_sync, Options, true),
    MaxLogSize =
        case proplists:get_value(max_log_size, Options) of
            undefined ->
                M = gmt_config:get_config_value_i(brick_max_log_size_mb,100),
                M * 1024 * 1024;
            M ->
                M * 1024 * 1024
        end,
    BigDataDir = proplists:get_value(bigdata_dir, Options),

    TabName = list_to_atom(atom_to_list(ServerName) ++ "_store"),
    CTab = ets:new(TabName, [ordered_set, protected, named_table]),
    ETabName = list_to_atom(atom_to_list(ServerName) ++ "_exp"),
    ETab = ets:new(ETabName, [ordered_set, protected, named_table]),
    MDTabName = list_to_atom(atom_to_list(ServerName) ++ "_md"),
    MDTab = ets:new(MDTabName, [ordered_set, protected, named_table]),
    ShadowTabName = list_to_atom(atom_to_list(ServerName) ++ "_shadow"),

    {ok, LogPid} = WalMod:start_link([{name, ServerName},
                                      {file_len_limit, MaxLogSize}]),

    DirtyTabName = list_to_atom(atom_to_list(ServerName) ++ "_dirty"),
    DirtyTab = ets:new(DirtyTabName, [ordered_set, protected, named_table]),
    WaitOnDirty = queue:new(),

    SyncPidArg = #syncpid_arg{parent_pid = self(), wal_pid = LogPid,
                              wal_mod = WalMod, name = ServerName},
    SyncPid = spawn_link(fun() -> sync_pid_start(SyncPidArg) end),
    LogOpQ = queue:new(),

    DefaultExpFun = fun delete_keys_immediately/2,
    DoExpFun =
        case gmt_config_svr:get_config_value(brick_expiration_processor,
                                             "[].") of
            {ok, "["++_ = ExpConfig} ->
                case brick_server:get_tabprop_via_str(ServerName, ExpConfig) of
                    undefined -> DefaultExpFun;
                    XF        -> XF
                end;
            _ ->
                DefaultExpFun
        end,

    %% Brick's name vs. that brick's log's name:
    %%     brick name: X
    %%     log's directory name: hlog.X
    %%     log's registered name: X_hlog
    LogDir = WalMod:log_name2data_dir(ServerName),
    State0 = #state{name = ServerName, options = Options, start_time = now(),
                    do_logging = DoLogging, do_sync = DoSync,
                    log_dir = LogDir, bigdata_dir = BigDataDir,
                    log = LogPid, ctab = CTab, etab = ETab, mdtab = MDTab,
                    shadowtab = undefined, shadowtab_name = ShadowTabName,
                    sync_pid = SyncPid,
                    dirty_tab = DirtyTab, wait_on_dirty_q = WaitOnDirty,
                    logging_op_q = LogOpQ, do_expiry_fun = DoExpFun,
                    wal_mod = WalMod
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
handle_call({do, _SentAt, L, DoFlags} = DoOp, From, State) ->
    %% This func is getting too-deeply indented ... but I really want all
    %% this stuff easy-to-see in a single func body.
    ?DBG_OPx(DoOp),
    case do_do(L, DoFlags, DoOp, From, State#state{thisdo_mods = []}, check_dirty) of
        has_dirty_keys ->
            ?DBG_OPx({?MODULE, do, State#state.name, has_dirty_keys, DoOp}),
            DQI = #dirty_q{from = From, do_op = DoOp},
            DirtyQ = State#state.wait_on_dirty_q,
            {noreply, State#state{wait_on_dirty_q = queue:in(DQI, DirtyQ)}};
        {no_dirty_keys, {Reply, NewState}} ->
            %% #state.thisdo_mods will be in reverse order of DoOpList!
            Thisdo_Mods = lists:reverse(NewState#state.thisdo_mods),
            if NewState#state.read_only_p, Thisdo_Mods /= [] ->
                    ?DBG_OPx({?MODULE, do, State#state.name, read_only_mode, DoOp}),
                    {up1_read_only_mode, From, DoOp, NewState};
               true ->
                    SyncOverride = proplists:get_value(sync_override, DoFlags),
                    case log_mods(NewState, SyncOverride) of
                        {goahead, NewState3} ->
                            ?DBG_TLOGx({?MODULE, do, State#state.name, goahead, DoOp}),
                            ok = map_mods_into_ets(Thisdo_Mods,
                                                   NewState3),
                            ?DBG_OPx({State#state.name, send_downstream}),
                            LoggingSerial =
                                NewState3#state.logging_op_serial,
                            ToDos = [{chain_send_downstream, LoggingSerial,
                                      DoFlags, From, Reply, Thisdo_Mods}],
                            ?DBG_TLOGx({?MODULE, do, State#state.name, todos_going_up, ToDos}),
                            N_do = NewState3#state.n_do,
                            %% !@#$!%! Grrr.....
                            %% If we're a head brick, we can increment
                            %% LoggingSerial all we want.  However, we
                            %% don't have carte blanche if we're a
                            %% tail brick.  If we're a tail,
                            %% incrementing this serial creates very
                            %% weird, bad situations with out-of-order
                            %% sync'ing.  So, only increment the
                            %% serial if we've actually got mods.
                            NL = if Thisdo_Mods == [] -> LoggingSerial;
                                    true              -> LoggingSerial + 1
                                 end,
                            case proplists:get_value(
                                   local_op_only_do_not_forward, DoFlags) of
                                true ->
                                    {reply, Reply, NewState3};
                                _ ->
                                    {up1, ToDos,
                                     {noreply, NewState3#state{
                                                 n_do = N_do + 1,
                                                 logging_op_serial = NL}}}
                            end;
                        {wait, NewState3} ->
                            ?DBG_TLOGx({?MODULE, do, State#state.name, wait, DoOp}),
                            add_mods_to_dirty_tab(Thisdo_Mods,
                                                  NewState3),
                            LoggingSerial =
                                NewState3#state.logging_op_serial,
                            SyncMsg = {sync_msg, LoggingSerial},
                            NewState3#state.sync_pid ! SyncMsg,
                            LQI = #log_q{logging_serial = LoggingSerial,
                                         thisdo_mods = Thisdo_Mods,
                                         doflags = DoFlags,
                                         from = From, reply = Reply},
                            NewQ = queue:in(LQI,
                                            NewState3#state.logging_op_q),
                            N_do = NewState3#state.n_do,
                            %% If we had to wait for logging, then
                            %% we're waiting because there's something
                            %% we actually *are* logging.  So
                            %% increasing the serial here is good.
                            NL = LoggingSerial + 1,
                            NewState4 =
                                NewState3#state{n_do = N_do + 1,
                                                logging_op_serial = NL,
                                                logging_op_q = NewQ},
                            {noreply, NewState4}
                    end
            end;
        {noreply_now, NewState} ->
            {noreply, NewState};
        silently_drop_reply ->
            %% Somewhere deep in the bowels of the do list processing,
            %% we hit some bad data.  We can't form a good answer, so
            %% don't reply at all.
            {noreply, State}
    end;
handle_call({status}, _From, State) ->
    Reply = do_status(State),
    {reply, Reply, State};
handle_call({state}, _From, State) ->
    {reply, State, State};
handle_call({flush_all}, _From, State) ->
    {Reply, NewState} = do_flush_all(State),
    {reply, Reply, NewState};
handle_call({checkpoint, Options}, _From, State) ->
    {Reply, NewState} = do_checkpoint(State, Options),
    {reply, Reply, NewState};
handle_call({dump_state}, _From, State) ->
    L = ets:tab2list(State#state.ctab),
    {reply, {length(L), L, State}, State};
handle_call({sync_stats, Seconds}, _From, State) ->
    catch brick_itimer:cancel(State#state.syncsum_tref),
    {ok, TRef} = if Seconds > 0 -> brick_itimer:send_interval(Seconds*1000,
                                                              log_sync_stats);
                    true        -> {ok, undefined}
           end,
    {reply, Seconds, State#state{syncsum_count = 0, syncsum_msec = 0,
                                 syncsum_len = 0, syncsum_tref = TRef}};
handle_call({set_do_sync, NewValue}, _From, State) ->
    {reply, State#state.do_sync, State#state{do_sync = NewValue}};
handle_call({set_do_logging, NewValue}, _From, State) ->
    {reply, State#state.do_logging, State#state{do_logging = NewValue}};
handle_call(Request, _From, State) ->
    ?E_ERROR("~s handle_call: Request = ~P\n", [?MODULE, Request, 20]),
    Reply = err,
    {reply, Reply, State}.

%%----------------------------------------------------------------------
%% Func: handle_cast/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_cast({checkpoint_last_seqnum, SeqNum}, State) ->
    {noreply, State#state{check_lastseqnum = SeqNum}};
handle_cast({incr_expired, Amount}, #state{n_expired = N} = State) ->
    {noreply, State#state{n_expired = N + Amount}};
handle_cast(Msg, State) ->
    ?E_ERROR("~s handle_cast: ~p: Msg = ~P\n",
             [?MODULE, State#state.name, Msg, 20]),
    exit({handle_cast, Msg}),                   % QQQ TODO debugging only
    {noreply, State}.

%%----------------------------------------------------------------------
%% Func: handle_info/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_info({sync_done, Pid, LastLogSerial}, State)
  when Pid == State#state.sync_pid ->
    ?DBG_TLOGx({sync_done, State#state.name, lastlogserial, LastLogSerial}),
    ?DBG_TLOGx({sync_done, State#state.name, logging_op_serial, State#state.logging_op_serial}),
    ?DBG_TLOGx({sync_done, State#state.name, logging_op_q, State#state.logging_op_q}),
    {LQI_List, State2} =
        pull_items_out_of_logging_queue(LastLogSerial, State),
    [ok = map_mods_into_ets(DoOpList, State2) ||
        #log_q{thisdo_mods = DoOpList} <- LQI_List],
    [ok = clear_dirty_tab(DoOpList, State2) ||
        #log_q{thisdo_mods = DoOpList} <- LQI_List],
    ToDos = [{chain_send_downstream, LQI#log_q.logging_serial,
              LQI#log_q.doflags, LQI#log_q.from, LQI#log_q.reply,
              LQI#log_q.thisdo_mods}
             || LQI <- LQI_List],
    %% CHAIN TODO: ISSUE001: Does the ToDos list (above) contain mods
    %%             for the dirty keys?  Or is the attempt below to
    %%             resubmit the dirty keys truly independent??
    %% put(issue001, ToDos), ... looks like they're independent, whew!
    {State3, ToDos2} = retry_dirty_key_ops(State2),
    ?DBG_TLOGx({sync_done_dirty_q, State#state.name, State#state.wait_on_dirty_q}),
    {up1_sync_done, LastLogSerial, ToDos ++ ToDos2, State3};
handle_info(check_expiry, State) ->
    case whereis(brick_server:make_expiry_regname(State#state.name)) of
        undefined ->
            {noreply, do_check_expiry(State)};
        _ ->
            {noreply, State}
    end;
handle_info(check_checkpoint, State) ->
    {noreply, do_check_checkpoint(State)};
handle_info({'EXIT', Pid, Reason}, State) when Pid == State#state.check_pid ->
    case Reason of
        done ->
            ok;
        {{?MODULE,_,_}, {line, _}, done} ->     % smart exceptions too helpful!
            ok;
       _ ->
            ?E_WARNING("checkpoint: pid ~p died with ~p\n", [Pid, Reason])
    end,
    NewState = fold_shadow_into_ctab(State),
    {noreply, NewState#state{check_pid = undefined}};
handle_info({'EXIT', Pid, Reason}, State) when Pid == State#state.sync_pid ->
    ?APPLOG_ALERT(?APPLOG_APPM_048,"~s: sync pid ~p died ~p\n",
                  [State#state.name, Pid, Reason]),
    {stop, normal, State};
handle_info({'EXIT', Pid, Done}, State)
  when Pid == State#state.scavenger_pid andalso
       (Done == done orelse element(3, Done) == done) -> % smart_exceptions
    {noreply, State#state{scavenger_pid = undefined}};
handle_info({'EXIT', Pid, Reason}, State)
  when Pid == State#state.scavenger_pid ->
    ?E_WARNING("QQQ: ~p: scavenger ~p exited with ~p\n",
               [State#state.name, State#state.scavenger_pid, Reason]),
    {noreply, State#state{scavenger_pid = undefined}};
handle_info({'EXIT', Pid, Reason}, State) when Pid == State#state.log ->
    ?E_WARNING("~p: log process ~p exited with ~p\n",
               [State#state.name, State#state.log, Reason]),
    {stop, Reason, State};
handle_info(qqq_debugging_only, S) ->
    ?DBG_GENx({debug, S#state.name, logging_op_serial, S#state.logging_op_serial}),
    ?DBG_GENx({debug, S#state.name, logging_op_q, S#state.logging_op_q}),
    ?DBG_GENx({debug, S#state.name, wait_on_dirty_q, S#state.wait_on_dirty_q}),
    ?DBG_GENx({debug, S#state.name, dirty_tab, ets:tab2list(S#state.dirty_tab)}),
    {noreply, S};
handle_info({syncpid_stats, _Name, MSec, ms, Length}, State) ->
    {noreply, State#state{syncsum_count = State#state.syncsum_count + 1,
                          syncsum_msec = State#state.syncsum_msec + MSec,
                          syncsum_len = State#state.syncsum_len + Length}};
handle_info(log_sync_stats, State) ->
    AvgTime = if State#state.syncsum_count == 0 -> 0;
                 true -> State#state.syncsum_msec / State#state.syncsum_count
              end,
    AvgLen = if State#state.syncsum_count == 0 -> 0;
                true -> State#state.syncsum_len / State#state.syncsum_count
             end,
    ?E_INFO("sync summary: ~p: ~p syncs ~p msec avg ~p len avg\n",
            [State#state.name, State#state.syncsum_count, AvgTime, AvgLen]),
    {noreply, State#state{syncsum_count = 0, syncsum_msec = 0,
                          syncsum_len = 0}};
handle_info(do_init_second_half, State) ->
    ?E_INFO("do_init_second_half: ~p\n", [State#state.name]),

    ok = gmt_hlog_common:full_writeback(),

    MinimumSeqNum = read_checkpoint_num(State#state.log_dir),
    {_LTODO_x, ErrList} = wal_scan_all(State, MinimumSeqNum),
    ?DBG_GEN("log ~p _LTODO_x = ~p ErrList = ~p\n",
             [State#state.wal_mod, _LTODO_x, ErrList]),

    if ErrList == [] ->
            ok;
       true ->
            wal_scan_failed(ErrList, State)
    end,
    ZeroDiskErrorsP = (ErrList == []),

    %% If the commonlog has told us that there are checksum errors,
    %% purge all records used by those bad log sequence files.
    %%
    %% TODO: It's quite possible that most of the file is not corrupt.
    %%       We're throwing out some of the baby with the bath water when
    %%       we discard all keys that refer to the bad file(s).  If we're
    %%       part of a chain with length > 1, things are still OK.
    %%       But if we're in standalone mode, then there's no one to repair
    %%       the missing keys, which could make us regret deleting keys
    %%       as many keys as we are doing here.
    [begin {Purged, _S} = purge_recs_by_seqnum(SeqNum, true, State),
           error_logger:info_msg("~s: purged ~p keys from sequence ~p\n",
                                 [State#state.name, Purged, SeqNum]),
           Purged
     end || SeqNum <- read_external_bad_sequence_file(State#state.name)],

    %% For recently bigdata_dir files, sync them all in a big safety blatt.
    %% ... Except that it also seems to have the potential to slam the
    %% ... disk system really, really hard, which can (in turn) create
    %% ... extremely bad timeout conditions.  {sigh}  So, for now,
    %% ... comment out the sync.
    %% os:cmd("sync"),

    %% Set these timers only after the WAL scan is finished.
    {ok, CheckTimer} = brick_itimer:send_interval(30*1000, check_checkpoint),
    brick_itimer:send_interval(1*1000, qqq_debugging_only),
    {ok, ExpiryTRef} = brick_itimer:send_interval(1*1000, check_expiry),

    ?E_INFO("do_init_second_half: ~p finished\n", [State#state.name]),
    self() ! {storage_layer_init_finished, State#state.name, ZeroDiskErrorsP},
    {noreply, State#state{checkpoint_timer = CheckTimer,
                          expiry_tref = ExpiryTRef,
                          scavenger_tref = undefined,
                          check_lastseqnum = MinimumSeqNum}};
handle_info(_Info, State) ->
    ?E_ERROR("Hey: ~s handle_info: Info = ~P\n", [?MODULE, _Info, 20]),
    {noreply, State}.

%%----------------------------------------------------------------------
%% Func: terminate/2
%% Purpose: Shutdown the server
%% Returns: any (ignored by gen_server)
%%----------------------------------------------------------------------
terminate(Reason, State) ->
    ?DBG_GENx({qqq_stopping, State#state.name, Reason}),
    ?DBG_GEN("Hey: terminate ~p: reason = ~p\n", [State#state.name, Reason]),
    catch ets:delete(State#state.ctab),
    catch ets:delete(State#state.etab),
    catch ets:delete(State#state.shadowtab),
    _X = (catch (State#state.wal_mod):stop(State#state.log)),
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
          {n_get, S#state.n_get},
          {n_get_many, S#state.n_get_many},
          {n_delete, S#state.n_delete},
          {n_txn, S#state.n_txn},
          {n_do, S#state.n_do},
          {n_expired, S#state.n_expired},
          {checkpoint, S#state.check_pid},
          {wait_on_dirty_q, S#state.wait_on_dirty_q}
         ]}.

do_flush_all(State) ->
    ?DBG_GEN("\n\n\nDeleting all table data!\n\n\n", []),
    ets:delete_all_objects(State#state.ctab),
    ets:delete_all_objects(State#state.etab),
    catch ets:delete_all_objects(State#state.shadowtab),
    do_checkpoint(State, []).

%% @spec (list(), list(), do_op(), from_spec(), state_r(), check_dirty | term()) ->
%%       has_dirty_keys | {no_dirty_keys, {reply_term(), new_state_r()}} |
%%       {reply_now, term(), new_state_r()} | {noreply_now, new_state_r()}
%% @doc Do a 'do', phase 1: Check to see if all keys are local to this brick.

do_do(L, DoFlags, DoOp, From, State, CheckDirty) ->
    %% CHAIN TODO: I think the not_calculated is probably not a good
    %%             idea, since chain processing has already harvested
    %%             the keys.  However ... chain processing is *supposed*
    %%             to be orthogonal & independent.  :-)
    try
        do_do1b(not_calculated, L, DoFlags, DoOp, From, State, CheckDirty)
    catch
        throw:silently_drop_reply -> % Instead, catch 1 caller higher? {shrug}
            silently_drop_reply
    end.

%% @doc Do a 'do', phase 1b: Check for any dirty keys.

do_do1b(DoKeys0, L, DoFlags, DoOp, From, State, check_dirty)
  when State#state.do_logging == true ->
    DoKeys = if DoKeys0 == not_calculated -> brick_server:harvest_do_keys(L);
                true                      -> DoKeys0
             end,
    case any_dirty_keys_p(DoKeys, State) of
        true ->
            has_dirty_keys;
        false ->
            if State#state.bigdata_dir == undefined ->
                    %% QQQ TODO Is it a good idea to stash DoKeys
                    %% inside State at this point, or do it someplace
                    %% else?  Earlier?  Later?  But later would mean
                    %% changing arity on do_do2() and more?  Well, it
                    %% turns out that we don't need to keep DoKeys and
                    %% the read/write info it stores ... we can use
                    %% instead: if Thisdo_Mods == [], then all ops
                    %% were read-only.
                    {no_dirty_keys, do_do2(L, DoFlags, State)};
               true ->
                    ReadP = any_readonly_keys_p(DoKeys),
                    Resub = proplists:get_value(squidflash_resubmit, DoFlags),
                    if ReadP == true andalso Resub == undefined ->
                            case squidflash_primer(DoOp, From, State) of
                                goahead ->
                                    {no_dirty_keys, do_do2(L, DoFlags, State)};
                                _Else ->
                                    {noreply_now, State}
                            end;
                        true ->
                            {no_dirty_keys, do_do2(L, DoFlags, State)}
                    end
            end
    end;
do_do1b(_DoKeys, L, DoFlags, _DoOp, _From, State, _) ->
    %% No logging, so no worries about ops on dirty keys.
    {no_dirty_keys, do_do2(L, DoFlags, State)}.

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
do_dolist([{add, Key, TStamp, Value, ExpTime, Flags}|T], State, DoFlags, Acc) ->
    {Res, NewState} = add_key(Key, TStamp, Value, ExpTime, Flags, State),
    N_add = NewState#state.n_add,
    do_dolist(T, NewState#state{n_add = N_add + 1}, DoFlags, [Res|Acc]);
do_dolist([{replace, Key, TStamp, Value, ExpTime, Flags}|T], State, DoFlags, Acc) ->
    {Res, NewState} = replace_key(Key, TStamp, Value, ExpTime, Flags, State),
    N_replace = NewState#state.n_replace,
    do_dolist(T, NewState#state{n_replace = N_replace + 1}, DoFlags, [Res|Acc]);
do_dolist([{set, Key, TStamp, Value, ExpTime, Flags}|T], State, DoFlags, Acc) ->
    {Res, NewState} = set_key(Key, TStamp, Value, ExpTime, Flags, State),
    N_set = NewState#state.n_set,
    do_dolist(T, NewState#state{n_set = N_set + 1}, DoFlags, [Res|Acc]);
do_dolist([{get, Key, Flags}|T], State, DoFlags, Acc) ->
    Res = get_key(Key, Flags, State),
    N_get = State#state.n_get,
    do_dolist(T, State#state{n_get = N_get + 1}, DoFlags, [Res|Acc]);
do_dolist([{delete, Key, Flags}|T], State, DoFlags, Acc) ->
    {Res, NewState} = delete_key(Key, Flags, State),
    N_delete = NewState#state.n_delete,
    do_dolist(T, NewState#state{n_delete = N_delete + 1}, DoFlags, [Res|Acc]);
do_dolist([{get_many, Key, Flags}|T], State, DoFlags, Acc) ->
    {Res0, NewState} = get_many1(Key, Flags, true, DoFlags, State),
    Res = {ok, Res0},
    N_get_many = NewState#state.n_get_many,
    do_dolist(T, NewState#state{n_get_many = N_get_many + 1}, DoFlags,
              [Res|Acc]);
do_dolist([next_op_is_silent, DoOp|T], State, DoFlags, Acc) ->
    {_ResList, NewState} = do_dolist([DoOp], State, DoFlags, []),
    do_dolist(T, NewState, DoFlags, Acc);
do_dolist([Unknown|T], State, DoFlags, Acc) ->
    %% Unknown is an error code from a do_txnlist() step, or perhaps
    %% some other return/status code ... pass it on.
    do_dolist(T, State, DoFlags, [Unknown|Acc]).

do_txnlist(L, DoFlags, State) ->
    %% It's a bit clumsy to have a separate accumulator for errors
    %% (arg 6), but I guess we'll live with it.
    do_txnlist(L, State, [], true, 1, DoFlags, []).

do_txnlist([], State, Acc, true, _N, DoFlags, []) ->
    N_txn = State#state.n_txn,
    {_, Reply} = do_do(lists:reverse(Acc), DoFlags,
                       doop_not_used, from_not_used,
                       State#state{n_txn = N_txn + 1}, false),
    Reply;
do_txnlist([], State, _Acc, false, _, _DoFlags, ErrAcc) ->
    {{txn_fail, lists:reverse(ErrAcc)}, State};
do_txnlist([{add, Key, _TStamp, _Value, _ExpTime, Flags} = H|T], State,
           Acc, Good, N, DoFlags, ErrAcc) ->
    case key_exists_p(Key, Flags, State) of
        {Key, TS, _, _, _, _} ->
            Err = {key_exists, TS},
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N,Err}|ErrAcc]);
        {ts_error, _} = Err ->
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N,Err}|ErrAcc]);
        _ ->
            do_txnlist(T, State, [H|Acc], Good, N+1, DoFlags, ErrAcc)
    end;
do_txnlist([{replace, Key, _TStamp, _Value, _ExpTime, Flags} = H|T], State,
           Acc, Good, N, DoFlags, ErrAcc) ->
    case key_exists_p(Key, Flags, State) of
        {Key, _, _, _, _, _} ->
            do_txnlist(T, State, [H|Acc], Good, N+1, DoFlags, ErrAcc);
        {ts_error, _} = Err ->
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc]);
        _ ->
            Err = key_not_exist,
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc])
    end;
do_txnlist([{set, _Key, _TStamp, _Value, _ExpTime, Flags} = H|T], State,
           Acc, Good, N, DoFlags, ErrAcc) ->
    Num = lists:foldl(fun(Flag, Sum) -> case check_flaglist(Flag, Flags) of
                                            {true, _} -> Sum + 1;
                                            _         -> Sum
                                        end
                      end, 0, [must_exist, must_not_exist]),
    if Num == 0 ->
            do_txnlist(T, State, [H|Acc], Good, N+1, DoFlags, ErrAcc);
       true ->
            Err = invalid_flag_present,
            do_txnlist(T, State, [Err|Acc], false, N+1, DoFlags, [{N, Err}|ErrAcc])
    end;
do_txnlist([{Primitive, Key, Flags} = H|T], State,
           Acc, Good, N, DoFlags, ErrAcc)
  when Primitive == get ; Primitive == delete ; Primitive == get_many ->
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
            if WantedExist == dont_care ; WantedExist == ActualExist ->
                    ok;
               WantedExist == true, ActualExist == false ->
                    {error, key_not_exist};
               WantedExist == false, ActualExist == true ->
                    {error, {key_exists, TS}};
               true ->
                    {error, invalid_flag_present}
            end
    end.

key_exists_p(Key, Flags, State) ->
    key_exists_p(Key, Flags, State, true).

key_exists_p(Key, Flags, State, MustHaveVal_p) ->
    case my_lookup(State, Key, MustHaveVal_p) of
        [] ->
            false;
        [StoreTuple] ->
            Key =        storetuple_key(StoreTuple),
            KeyTStamp =  storetuple_ts(StoreTuple),
            KeyVal =     storetuple_val(StoreTuple),
            KeyValLen =  storetuple_vallen(StoreTuple),
            KeyExpTime = storetuple_exptime(StoreTuple),
            KeyFlags =   storetuple_flags(StoreTuple),
            case check_flaglist(testset, Flags) of
                {true, TestSetTStamp} ->
                    if
                        KeyTStamp == TestSetTStamp ->
                            {Key, KeyTStamp, KeyVal, KeyValLen, KeyExpTime, KeyFlags};
                        true ->
                            {ts_error, KeyTStamp}
                    end;
                _ ->
                    {Key, KeyTStamp, KeyVal, KeyValLen, KeyExpTime, KeyFlags}
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
            NewState = my_insert(State, Key, TStamp, Value, ExpTime, Flags),
            {ok, NewState}
    end.

replace_key(Key, TStamp, Value, ExpTime, Flags, State) ->
    case key_exists_p(Key, Flags, State, false) of
        {Key, TS, _, _, _PreviousExp, _} ->
            replace_key2(Key, TStamp, Value, ExpTime, Flags, State, TS);
        {ts_error, _} = Err ->
            {Err, State};
        _ ->
            {key_not_exist, State}
    end.

replace_key2(Key, TStamp, Value, ExpTime, Flags, State, TS) ->
    if
        TStamp > TS ->
            NewState = my_insert(State, Key, TStamp, Value, ExpTime, Flags),
            {ok, NewState};
        TStamp == TS, element(1, Value) == ?VALUE_SWITCHAROO ->
            NewState = my_insert(State, Key, TStamp, Value, ExpTime, Flags),
            {ok, NewState};
        TStamp == TS ->
            %% We need to get the val blob.  {sigh}
            {Key, TS, Val, _, _, _} = key_exists_p(Key, Flags, State),
            if Value == Val ->
                    %% In this case, the new timestamp and value is
                    %% identical to what is already stored here ... so
                    %% we will return OK without changing anything.
                    {ok, State};
               true ->
                    {{ts_error, TS}, State}
            end;
        true ->
            {{ts_error, TS}, State}
    end.

set_key(Key, TStamp, Value, ExpTime, Flags, State) ->
    case key_exists_p(Key, Flags, State, false) of
        {Key, TS, _, _, _PreviousExp, _} ->
            %% Now, we have same case as replace_key when exists ...
            %% so reuse the lower level of replace_key().
            replace_key2(Key, TStamp, Value, ExpTime, Flags, State, TS);
        {ts_error, _} = Err ->
            {Err, State};
        _X ->
            NewState = my_insert(State, Key, TStamp, Value, ExpTime, Flags),
            {ok, NewState}
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
                            {ok, KeyTStamp, Flags2}
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

ets_next_wrapper(Tab, ?BRICK__GET_MANY_FIRST) ->
    ets:first(Tab);
ets_next_wrapper(Tab, Key) ->
    ets:next(Tab, Key).

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
            %% Hint: we know Rs /= [].
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
                    %io:format("DROP: sweep ~p - ~p\n", [SweepA, SweepZ]),
                    %io:format("DROP: ~P\n", [[KKK || {KKK, _} <- (Rs -- Rs2)], 10]),
                    {{Rs2, true}, NewState};
                _ ->
                    Result
            end
    end.
-spec get_many1b(key(), flags_list(), boolean(), tuple()) ->
                        {{list(extern_tuple()), boolean()}, tuple()}.
get_many1b(Key, Flags, IncreasingOrderP, State) ->
    MaxNum = case proplists:get_value(max_num, Flags, 10) of
                 N when is_integer(N) -> N;
                 _                    -> 10
             end,
    DoWitnessP = case check_flaglist(witness, Flags) of
                     {true, true} -> true;
                     _            -> false
                 end,
    DoAllAttrP = case check_flaglist(get_all_attribs, Flags) of
                     {true, true} -> true;
                     _            -> false
                 end,
    RawStoreTupleP = case check_flaglist(get_many_raw_storetuples, Flags) of
                         {true, true} -> true;
                         _            -> false
                     end,
    ResultFlavor = {DoWitnessP, DoAllAttrP, RawStoreTupleP},
    BPref = case proplists:get_value(binary_prefix, Flags) of
                Prefix when is_binary(Prefix) -> {size(Prefix), Prefix};
                _                             -> 0
            end,
    MaxBytes = case proplists:get_value(max_bytes, Flags) of
                   N2 when is_integer(N2) -> N2;
                   _                      -> 2*1024*1024*1024
               end,
    Res = if State#state.shadowtab == undefined ->
                  get_many2(ets_next_wrapper(State#state.ctab, Key),
                            MaxNum, MaxBytes, ResultFlavor, BPref, [], State);
             BPref == 0 ->
                  get_many_shadow(ets_next_wrapper(State#state.ctab, Key),
                                  ets_next_wrapper(State#state.shadowtab, Key),
                                  MaxNum, MaxBytes, ResultFlavor, [], State);
             true ->
                  {{Xs, Bool}, NewState} =
                      get_many_shadow(ets_next_wrapper(State#state.ctab, Key),
                                      ets_next_wrapper(State#state.shadowtab,
                                                       Key),
                                      MaxNum, MaxBytes, ResultFlavor, [],State),
                  %% Need to filter after-the-fact.
                  {PfxLen, Pfx} = BPref,
                  if Xs == [] ->
                          {{Xs, Bool}, NewState};
                     true ->
                          Key_0 = element(1, hd(Xs)),
                          case Key_0 of
                              <<Pfx:PfxLen/binary, _/binary>> -> %exported!
                                  {{Xs, Bool}, NewState};
                              _ ->
                                  {{drop_prefixes(Xs, Pfx, PfxLen), false},
                                   NewState}
                          end
                  end
          end,
    if IncreasingOrderP ->                      % dialyzer: can never succeed
            %% ManyList is backward, must reverse it.
            {{ManyList, TorF}, State2} = Res,
            {{lists:reverse(ManyList), TorF}, State2};
       true ->
            Res                         % Result list is backward, that's OK.
    end.

drop_prefixes(Xs, Prefix, PrefixLen) ->
    lists:dropwhile(fun(X) ->
                            Key = element(1, X),
                            case Key of
                                <<Prefix:PrefixLen/binary, _/binary>> -> false;
                                _                                     -> true
                            end
                    end, Xs).

%% @doc Get a list of keys, starting with Key, in increasing order when
%% our current state is not checkpointing.
%%
%% The tuples returned here are in external tuple format.
%%
%% NOTE: We are *supposed* to return our results in reversed order.

-spec get_many2(key() | ?BRICK__GET_MANY_LAST, any(), any(), any(), any(), any(), any())
               -> {{list(extern_tuple()), boolean()}, any()}.
get_many2('$end_of_table', _MaxNum, _MaxBytes, _ResultFlavor, _BPref, Acc, State) ->
    {{Acc, false}, State};
get_many2(_Key, 0, _MaxBytes, _ResultFlavor, _BPref, Acc, State) ->
    {{Acc, true}, State};
get_many2(Key, MaxNum, MaxBytes, ResultFlavor, BPref, Acc, State)
  when MaxBytes > 0 ->
    Cont = case BPref of
               0 ->
                   ok;
               {PrefixLen, Prefix} ->
                   case Key of
                       <<Prefix:PrefixLen/binary, _Rest/binary>> ->
                           ok;
                       _ ->
                           skip %{{lists:reverse(Acc), false}, State}
                   end
           end,
    if
        Cont == ok ->
            [Tuple] = ets:lookup(State#state.ctab, Key),
            Bytes = storetuple_vallen(Tuple),
            Item = make_many_result(Tuple, ResultFlavor, State),
            get_many2(ets:next(State#state.ctab, Key), MaxNum - 1,
                      MaxBytes - Bytes,
                      ResultFlavor, BPref, [Item|Acc], State);
        Cont == skip ->
            get_many2('$end_of_table', MaxNum, MaxBytes, ResultFlavor, BPref,
                      Acc, State)
    end;
get_many2(_Key, _MaxNum, _MaxBytes, _ResultFlavor, _BPref, Acc, State) ->
    {{Acc, true}, State}.

%% @doc Create the result tuple for a single get_many result.
%%
%% Note about separation of ETS and disk storage:
%% the get_many iterators, in both the normal case and the shadowtab
%% case, are free to use ets:lookup().  If a get_many request is
%% 'witness' flavored, then we we most definitely do not want to hit the
%% disk for a tuple val.
%%
%% Arg note: (StoreTuple, {DoWitnessP, DoAllAttrP, RawStoreTupleP}, State).

-spec make_many_result(store_tuple(), {boolean(), boolean(), boolean()}, any()) ->
                              extern_tuple().
make_many_result(StoreTuple, {true, false, false}, _S) ->
    {storetuple_key(StoreTuple), storetuple_ts(StoreTuple)};
make_many_result(StoreTuple, {true, true, false}, S) ->
    {storetuple_key(StoreTuple), storetuple_ts(StoreTuple),
     make_extern_flags(storetuple_val(StoreTuple),
                       storetuple_vallen(StoreTuple),
                       storetuple_flags(StoreTuple), S)};
make_many_result(StoreTuple, {false, _DoAllAttr, false}, S) ->
    make_many_result2(StoreTuple, S);
make_many_result(StoreTuple, {_DoWitness, _DoAllAttr, true}, _S) ->
    StoreTuple.

make_many_result2(StoreTuple, S) when S#state.bigdata_dir == undefined ->
    storetuple_to_externtuple(StoreTuple, S);
make_many_result2(StoreTuple, S) ->
    {Key, TStamp, Val0, ExpTime, Flags} = storetuple_to_externtuple(
                                            StoreTuple, S),
    ValLen = storetuple_vallen(StoreTuple),
    Val = bigdata_dir_get_val(Key, Val0, ValLen, S),
    {Key, TStamp, Val, ExpTime, Flags}.

%% @spec (term(), term(), integer(), integer(), flavor_tuple(), list(), state_r()) ->
%%       {{list(), true | false}, state_r()}
%% @doc Get a list of keys, starting with Key, in increasing order when
%% our current state is in the middle of a checkpoint operation.
%%
%% The tuples returned here are in external tuple format.
%%
%% Creates a result list for 'get_many' by merging the data
%% in both the normal S#state.ctab table and deltas in the
%% S#state.shadowtab table.
%%
%% This function is a pain, but I can't think of an easier way to
%% do it without similar pain.  Each case describes:
%% <ol>
%% <li> MaxCount counter drops to zero. </li>
%% <li> Traversal of both tables has reached the end. </li>
%% <li> Traversal of the regular table has reached the end, but
%%      entries remain in the shadowtab.  If the current shadow entry
%%      is delete, continue, otherwise add the record and continue. </li>
%% <li> Traversal of the Shadow table has reached the end, but
%%      entries remain in the regular table.
%%      Add the record and continue. </li>
%% <li> Traversal of both tables is in the middle, regular table key
%%      is less than shadow key.  Add the record and continue. </li>
%% <li> Traversal of both tables is in the middle, regular table key
%%      is equal to the shadow key.  Depending on the value of the
%%      shadow record, either do nothing or add the record, then
%%      continue. </li>
%% <li> Traversal of both tables is in the middle, regular table key
%%      is greater than the shadow key.  Depending on the value of the
%%      shadow record, either do nothing or add the record, then
%%      continue. </li>
%% </ol>
%%
%% NOTE: We are *supposed* to return our results in reversed order.

get_many_shadow(_Key, _SKey, MaxNum, MaxBytes, _ResultFlavor, Acc, S)
  when MaxNum =< 0 ; MaxBytes =< 0 ->
    %%?DBG_ETSx('1z'),
    {{Acc, true}, S};
get_many_shadow('$end_of_table', '$end_of_table', _MaxNum, _MaxBytes,
                _ResultFlavor, Acc,S) ->
    %%?DBG_ETSx('2z'),
    {{Acc, false}, S};
get_many_shadow('$end_of_table', SKey, MaxNum, MaxBytes, ResultFlavor, Acc, S) ->
    [Shadow] = ets:lookup(S#state.shadowtab, SKey),
    case Shadow of
        {_Key, insert, StoreTuple} ->
            %%?DBG_ETSx({'3a', '$end_of_table', SKey}),
            Bytes = storetuple_vallen(StoreTuple),
            Item = make_many_result(StoreTuple, ResultFlavor, S),
            get_many_shadow('$end_of_table',
                            ets:next(S#state.shadowtab, SKey),
                            MaxNum - 1, MaxBytes - Bytes,
                            ResultFlavor, [Item|Acc], S);
        {_Key, delete, _ExpTime} ->
            %%?DBG_ETSx({'3b', '$end_of_table', SKey}),
            %% If SKey is a delete, SKey *might* also be present in the
            %% frozen table ... but we've reached the end of the frozen
            %% table.  Therefore, the series of events is:
            %%   1. checkpoint starts
            %%   2. SKey inserted
            %%   3. SKey deleted
            %%   4. This getmany arrives.
            %% Continue on our merry way, ignoring this deleted item.
            get_many_shadow('$end_of_table',
                            ets:next(S#state.shadowtab, SKey),
                            MaxNum, MaxBytes, ResultFlavor, Acc, S)
    end;
get_many_shadow(Key, '$end_of_table', MaxNum, MaxBytes, ResultFlavor, Acc, S) ->
    [Tuple] = ets:lookup(S#state.ctab, Key),
    Bytes = storetuple_vallen(Tuple),
    Item = make_many_result(Tuple, ResultFlavor, S),
    %%?DBG_ETSx({'4z', Key, '$end_of_table'}),
    get_many_shadow(ets:next(S#state.ctab, Key),
                    '$end_of_table',
                    MaxNum - 1, MaxBytes - Bytes, ResultFlavor, [Item|Acc], S);
get_many_shadow(Key, SKey, MaxNum, MaxBytes, ResultFlavor, Acc, S)
  when Key < SKey ->
    [Tuple] = ets:lookup(S#state.ctab, Key),
    Bytes = storetuple_vallen(Tuple),
    Item = make_many_result(Tuple, ResultFlavor, S),
    %%?DBG_ETSx({'5z', Key, SKey}),
    get_many_shadow(ets:next(S#state.ctab, Key),
                    SKey,
                    MaxNum - 1, MaxBytes - Bytes, ResultFlavor, [Item|Acc], S);
get_many_shadow(Key, SKey, MaxNum, MaxBytes, ResultFlavor, Acc, S)
  when Key == SKey ->
    [Shadow] = ets:lookup(S#state.shadowtab, SKey),
    case Shadow of
        {_Key, delete, _ExpTime} ->
            %%?DBG_ETSx({'6a', Key, SKey}),
            get_many_shadow(ets:next(S#state.ctab, Key),
                            ets:next(S#state.shadowtab, SKey),
                            MaxNum, MaxBytes, ResultFlavor, Acc, S);
        {_Key, insert, StoreTuple} ->
            %%?DBG_ETSx({'6b', Key, SKey}),
            Bytes = storetuple_vallen(StoreTuple),
            Item = make_many_result(StoreTuple, ResultFlavor, S),
            get_many_shadow(ets:next(S#state.ctab, Key),
                            ets:next(S#state.shadowtab, SKey),
                            MaxNum - 1, MaxBytes - Bytes,
                            ResultFlavor, [Item|Acc], S)
    end;
get_many_shadow(Key, SKey, MaxNum, MaxBytes, ResultFlavor, Acc, S)
  when Key > SKey ->
    [Shadow] = ets:lookup(S#state.shadowtab, SKey),
    case Shadow of
        {_Key, insert, StoreTuple} ->
            %%?DBG_ETSx({'7a', Key, SKey}),
            Bytes = storetuple_vallen(StoreTuple),
            Item = make_many_result(StoreTuple, ResultFlavor, S),
            get_many_shadow(Key,
                            ets:next(S#state.shadowtab, SKey),
                            MaxNum - 1, MaxBytes - Bytes,
                            ResultFlavor, [Item|Acc], S);
        {_Key, delete, _ExpTime} ->
            %%?DBG_ETSx({'7b', Key, SKey}),
            %% If SKey is a delete, then SKey should also be present
            %% in the frozen table ... but somehow we skipped the
            %% case where Key == SKey, which ought to be impossible.
            %%
            %% throw({get_many_shadow, impossible, Key, SKey})

            %% This is a subtle case.  Naively, we would expect that a
            %% previous iteration of this func would have caught the
            %% Key == SKey case before we got to this point.  However,
            %% that is the naive view.
            %% It is possible that the following sequence of events
            %% happened:
            %%  1. Checkpoint started
            %%  2. Key is inserted into the table, when Key did not
            %%     already exist.
            %%  3. Key is deleted from the table.
            %%  4. We reach this point, while the checkpoint is still
            %%  in progress.
            get_many_shadow(Key,
                            ets:next(S#state.shadowtab, SKey),
                            MaxNum, MaxBytes, ResultFlavor, Acc, S)
    end.

delete_key(Key, Flags, State) ->
    case key_exists_p(Key, Flags, State, false) of
        {Key, _KeyTStamp, _Value, _ValueLen, ExpTime, _} ->
            NewState = my_delete(State, Key, ExpTime),
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

%% @doc Use variable-sized tuples to try to avoid storing unnecessary
%% common values.
%%
%% Store tuple formats:
%% <ul>
%% <!--  1    2       3      4         5        6        -->
%% <li> {Key, TStamp, Value, ValueLen}                 </li>
%% <li> {Key, TStamp, Value, ValueLen, ExpTime}        </li>
%% <li> {Key, TStamp, Value, ValueLen, Flags}          </li>
%% <li> {Key, TStamp, Value, ValueLen, ExpTime, Flags} </li>
%% </ul>

-type storetuple_val() :: val() | {integer(), integer()}.

-type store_tuple() :: {key(), ts(), storetuple_val(), integer()} |
                       {key(), ts(), storetuple_val(), integer(), exp_time()} |
                       {key(), ts(), storetuple_val(), integer(), flags_list()} |
                       {key(), ts(), storetuple_val(), integer(), exp_time(), flags_list()}.
-type extern_tuple() :: {key(), ts(), storetuple_val()} |
                        {key(), ts(), storetuple_val(), exp_time()} |
                        {key(), ts(), storetuple_val(), flags_list()} |
                        {key(), ts(), storetuple_val(), exp_time(), flags_list()}.

storetuple_make(Key, TStamp, Value, ValueLen, 0, []) ->
    {Key, TStamp, Value, ValueLen};
storetuple_make(Key, TStamp, Value, ValueLen, ExpTime, [])
  when is_integer(ExpTime), ExpTime =/= 0 ->
    {Key, TStamp, Value, ValueLen, ExpTime};
storetuple_make(Key, TStamp, Value, ValueLen, 0, Flags)
  when is_list(Flags), Flags /= [] ->
    {Key, TStamp, Value, ValueLen, Flags};
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
storetuple_to_externtuple({Key, TStamp, Value, ValueLen, ExpTime}, S)
  when is_integer(ExpTime) ->
    {Key, TStamp, Value, ExpTime, make_extern_flags(Value, ValueLen, [], S)};
storetuple_to_externtuple({Key, TStamp, Value, ValueLen, Flags}, S)
  when is_list(Flags) ->
    {Key, TStamp, Value, 0, make_extern_flags(Value, ValueLen, Flags, S)};
storetuple_to_externtuple({Key, TStamp, Value, ValueLen, ExpTime, Flags}, S) ->
    {Key, TStamp, Value, ExpTime, make_extern_flags(Value, ValueLen, Flags, S)}.

make_extern_flags(Value, ValueLen, Flags, S) ->
    if is_binary(Value), S#state.bigdata_dir /= undefined ->
            [{val_len, ValueLen}, value_in_ram|Flags];
       true ->
            [{val_len, ValueLen}|Flags]
    end.

externtuple_to_storetuple({Key, TStamp, Value}) ->
    storetuple_make(Key, TStamp, Value, gmt_util:io_list_len(Value), 0, []);
externtuple_to_storetuple({Key, TStamp, Value, ExpTime})
  when is_integer(ExpTime) ->
    storetuple_make(Key, TStamp, Value, gmt_util:io_list_len(Value), ExpTime, []);
externtuple_to_storetuple({Key, TStamp, Value, Flags})
  when is_list(Flags) ->
    storetuple_make(Key, TStamp, Value, gmt_util:io_list_len(Value), 0, Flags);
externtuple_to_storetuple({Key, TStamp, Value, ExpTime, Flags}) ->
    storetuple_make(Key, TStamp, Value, gmt_util:io_list_len(Value), ExpTime, Flags).

exptime_insert(_ETab, _Key, 0) ->
    ok;
exptime_insert(ETab, Key, ExpTime) ->
    ?DBG_ETSx({qqq_exp_insert, ETab, {ExpTime, Key}}),
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
               fun({testset, _})       -> false;
                  ({max_num, _})       -> false;
                  ({binary_prefix, _}) -> false;
                  ({val_len, _})       -> false;
                  ({_, _})             -> true;
                  (must_exist)         -> false;
                  (must_not_exist)     -> false;
                  (witness)            -> false;
                  (get_all_attribs)    -> false;
                  %% We'd normally filter 'value_in_ram' here, but we
                  %% need to let it pass through to a lower level.
                  (A) when is_atom(A)  -> true;
                  (_)                  -> false
               end, Flags),
    my_insert2(State, Key, TStamp, Value, ExpTime, Flags2).

my_insert2(S, Key, TStamp, Value, ExpTime, Flags) ->
    Mods = S#state.thisdo_mods,
    Mod =
        case Value of
            ?VALUE_REMAINS_CONSTANT ->
                [ST] = my_lookup(S, Key, false),
                CurVal = storetuple_val(ST),
                CurValLen = storetuple_vallen(ST),
                %% This is a royal pain, but I don't see a way around
                %% this ... we need to avoid reading the value for
                %% this key *and* avoid sending that value down the
                %% chain.  For the latter, we need to indicate to the
                %% downstream that we didn't include the value in this
                %% update.  {sigh}
                {insert_constant_value,
                 storetuple_make(Key, TStamp, CurVal, CurValLen, ExpTime,
                                 Flags)};
            {?VALUE_SWITCHAROO, OldVal, NewVal} ->
                %% NOTE: If testset has been specified, it has already
                %%       been validated and stripped from Flags.
                [ST] = my_lookup(S, Key, false),
                CurVal = storetuple_val(ST),
                CurValLen = storetuple_vallen(ST),
                {OldSeq, _} = OldVal,
                {CurSeq, _} = CurVal,
                if abs(OldSeq) == abs(CurSeq) ->
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
                Val = gmt_util:bin_ify(Value),
                UseRamP = lists:member(value_in_ram, Flags),
                ST = storetuple_make(Key, TStamp, Val, size(Val),
                                     ExpTime, Flags -- [value_in_ram]),
                if UseRamP ; S#state.bigdata_dir == undefined ->
                        {insert_value_into_ram, ST};
                   true ->
                        Loc = bigdata_dir_store_val(Key, Value, S),
                        {insert, ST, storetuple_replace_val(ST, Loc)}
                end
        end,
    S#state{thisdo_mods = [Mod|Mods]}.

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

my_insert_ignore_logging3(State, Key, StoreTuple)
  when State#state.bypass_shadowtab == true
       orelse
       State#state.shadowtab == undefined ->
    ?DBG_ETSx({qqq_ins, State#state.name, regular, Key}),
    exptime_insert(State#state.etab, Key, storetuple_exptime(StoreTuple)),
    ets:insert(State#state.ctab, StoreTuple);
my_insert_ignore_logging3(State, Key, StoreTuple) ->
    ?DBG_ETSx({qqq_ins, State#state.name, shadow, Key}),
    exptime_insert(State#state.etab, Key, storetuple_exptime(StoreTuple)),
    ets:insert(State#state.shadowtab, {Key, insert, StoreTuple}).

delete_prior_expiry(S, Key) ->
    case my_lookup(S, Key, false) of
        [ST] -> ExpTime = storetuple_exptime(ST),
                exptime_delete(S#state.etab, Key, ExpTime);
        _    -> ok
    end.

my_delete(State, Key, ExpTime) when State#state.do_logging == true ->
    %% Logging is in effect.  Save key in list of keys for later
    %% addition to dirty_keys list.
    Mods = State#state.thisdo_mods,
    State#state{thisdo_mods = [{delete, Key, ExpTime}|Mods]
               };
my_delete(State, Key, ExpTime) ->
    my_delete_ignore_logging(State, Key, ExpTime),
    Mods = State#state.thisdo_mods,
    State#state{thisdo_mods = [{delete, Key, ExpTime}|Mods]}.

my_delete_ignore_logging(State, Key, ExpTime) ->
    delete_prior_expiry(State, Key),
    if State#state.bigdata_dir == undefined ->
            ok;
       true ->
            bigdata_dir_delete_val(Key, State)
    end,
    my_delete_ignore_logging2(State, Key, ExpTime).

my_delete_ignore_logging2(State, Key, ExpTime)
  when State#state.bypass_shadowtab == true
       orelse
       State#state.shadowtab == undefined ->
    ?DBG_ETSx({qqq_del, State#state.name, regular, Key}),
    exptime_delete(State#state.etab, Key, ExpTime),
    ets:delete(State#state.ctab, Key);
my_delete_ignore_logging2(State, Key, ExpTime) ->
    ?DBG_ETSx({qqq_del, State#state.name, shadow, Key}),
    exptime_delete(State#state.etab, Key, ExpTime),
    ets:insert(State#state.shadowtab, {Key, delete, ExpTime}).

my_lookup(State, Key, _MustHaveVal_p = false) ->
    my_lookup2(State, Key);
my_lookup(State, Key, true) ->
    case my_lookup2(State, Key) of
        [StoreTuple] = Res ->
            if State#state.bigdata_dir == undefined ->
                    Res;
               true ->
                    Val0 = storetuple_val(StoreTuple),
                    ValLen = storetuple_vallen(StoreTuple),
                    Val = bigdata_dir_get_val(Key, Val0, ValLen, State),
                    [storetuple_replace_val(StoreTuple, Val)]
            end;
        Res ->
            Res
    end.

%% @doc Perform ETS-specific lookup function.
%%
%% Note about separation of ETS and disk storage: Our caller is always
%% my_lookup(), which is responsible for doing any disk access for the
%% "real" value of val.

my_lookup2(State, Key) when State#state.shadowtab == undefined ->
    ets:lookup(State#state.ctab, Key);
my_lookup2(State, Key) ->
    case ets:lookup(State#state.shadowtab, Key) of
        [{Key, delete, _ExpTime}] ->
            [];
        [{Key, insert, StoreTuple}] ->
            [StoreTuple];
        [] ->
            ets:lookup(State#state.ctab, Key)
    end.

my_delete_all_objects(Tab) ->
    ets:delete_all_objects(Tab).

%% REMINDER: It is the *caller's responsibility* to manage the
%%           logging_op_serial counter, not log_mods() or log_mods2().

-spec log_mods(tuple(), boolean() | undefined) -> {goahead, tuple()} | {wait, tuple()}.
log_mods(S, _SyncOverride) when S#state.do_logging == false ->
    {goahead, S#state{thisdo_mods = []}};
log_mods(S, SyncOverride) ->
    if S#state.thisdo_mods == [] ->
            {goahead, S};
       true ->
            log_mods2(S#state.thisdo_mods, SyncOverride, S)
    end.

load_rec_from_log({insert, StoreTuple}, S) ->
    load_rec_from_log_common_insert(StoreTuple, S);
load_rec_from_log({insert_value_into_ram, StoreTuple}, S) ->
    load_rec_from_log_common_insert(StoreTuple, S);
load_rec_from_log({delete, Key, ExpTime}, S) ->
    load_rec_clean_up_etab(Key, S),
    my_delete_ignore_logging(S, Key, ExpTime);
load_rec_from_log({delete_noexptime, Key}, S) ->
    case my_lookup(S, Key, false) of
        [StoreTuple] ->
            load_rec_clean_up_etab(Key, S),
            ExpTime = storetuple_exptime(StoreTuple),
            my_delete_ignore_logging(S, Key, ExpTime);
        [] ->
            ok
    end;
load_rec_from_log({insert_constant_value, StoreTuple}, S) ->
    Key = storetuple_key(StoreTuple),
    my_insert_ignore_logging(S, Key, StoreTuple);
load_rec_from_log({delete_all_table_items}, S) ->
    my_delete_all_objects(S#state.mdtab),
    my_delete_all_objects(S#state.etab),
    my_delete_all_objects(S#state.ctab);
load_rec_from_log({md_insert, Tuple}, S) ->
    ets:insert(S#state.mdtab, Tuple);
load_rec_from_log({md_delete, Key}, S) ->
    ets:delete(S#state.mdtab, Key);
load_rec_from_log({log_directive, _, _}, _S) ->
    ok;
load_rec_from_log({log_noop}, _S) ->
    ok.

load_rec_from_log_common_insert(StoreTuple, S) ->
    Key = storetuple_key(StoreTuple),
    load_rec_clean_up_etab(Key, S),
    my_insert_ignore_logging(S, Key, StoreTuple).

load_rec_clean_up_etab(Key, S) ->
    case ets:lookup(S#state.ctab, Key) of
        [DelST] -> DelExp = storetuple_exptime(DelST),
                   exptime_delete(S#state.etab, Key, DelExp);
        _       -> ok
    end.

purge_recs_by_seqnum(SeqNum, CheckpointNotRunning_p, S) ->
    error_logger:info_msg("~s: purging keys with sequence ~p, size ~p\n",
                          [S#state.name, SeqNum, ets:info(S#state.ctab, size)]),
    if CheckpointNotRunning_p ->
            undefined = S#state.shadowtab; % sanity
       true ->
            ok
    end,
    N1 = purge_rec_from_log(0, ets:first(S#state.ctab), abs(SeqNum), 0, S),
    N2 = purge_rec_from_shadowtab(abs(SeqNum), S#state.shadowtab, S),
    {N1 + N2, S}.

purge_rec_from_log(1000000, Key, SeqNum, Count, S) ->
    flush_gen_server_calls(),   % Clear any mailbox backlog
    flush_gen_cast_calls(S),    % Clear any mailbox backlog
    purge_rec_from_log(0, Key, SeqNum, Count, S);
purge_rec_from_log(_Iters, '$end_of_table', _SeqNum, Count, _S) ->
    Count;
purge_rec_from_log(Iters, Key, SeqNum, Count, S) ->
    [ST] = my_lookup(S, Key, false),
    case storetuple_val(ST) of
        {STSeqNum, _Offset} when abs(STSeqNum) == SeqNum ->
            ExpTime = storetuple_exptime(ST),
            my_delete_ignore_logging(S, Key, ExpTime),
            purge_rec_from_log(Iters + 1, ets:next(S#state.ctab, Key), SeqNum,
                               Count + 1, S);
        _ ->
            purge_rec_from_log(Iters + 1,
                               ets:next(S#state.ctab, Key), SeqNum, Count, S)
    end.

%% TODO: merge purge_rec_from_log() and purge_rec_from_shadowtab()

purge_rec_from_shadowtab(_SeqNum, undefined, _S) ->
    0;
purge_rec_from_shadowtab(SeqNum, ShadowTab, S) ->
    ets:foldl(fun({Key, insert, ST}, Acc) ->
                      case storetuple_val(ST) of
                          {STSeqNum, _Off} when abs(STSeqNum) == SeqNum ->
                              ExpTime = storetuple_exptime(ST),
                              my_delete_ignore_logging(S, Key, ExpTime),
                              Acc + 1;
                          _ ->
                              Acc
                      end;
                 (_, Acc) ->
                      Acc + 1
              end, 0, ShadowTab).

filter_mods_for_downstream(Thisdo_Mods) ->
    lists:map(fun({insert, ChainStoreTuple, _StoreTuple}) ->
                      %%?DBG_GEN("ChainStoreTuple = ~p\n", [ChainStoreTuple]),
                      %%?DBG_GEN("_StoreTuple = ~p\n", [_StoreTuple]),
                      {insert, ChainStoreTuple};
                 ({log_directive, sync_override, _}) ->
                      {log_noop};
                 (X) ->
                      X
              end, Thisdo_Mods).

filter_mods_from_upstream(Thisdo_Mods, S)
  when S#state.bigdata_dir == undefined ->
    Thisdo_Mods;
filter_mods_from_upstream(Thisdo_Mods, S) ->
    Ms = lists:map(fun({insert, ST}) ->
                           Key = storetuple_key(ST),
                           Val = storetuple_val(ST),
                           Loc = bigdata_dir_store_val(Key, Val, S),
                           {insert, ST, storetuple_replace_val(ST, Loc)};
                      %% Do not modify {insert_value_into_ram,...} tuples here.
                      ({insert, ST, BigDataDirThing}) ->
                           ?DBG_OPx({error, S#state.name, bad_mod_from_upstream, ST, BigDataDirThing}),
                           ?E_ERROR("BUG: ~p\n", [{error, S#state.name, bad_mod_from_upstream, ST, BigDataDirThing}]),
                           exit({bug, S#state.name, bad_mod_from_upstream,
                                 ST, BigDataDirThing});
                      ({insert_constant_value, ST}) ->
                           Key = storetuple_key(ST),
                           TS = storetuple_ts(ST),
                           [CurST] = my_lookup(S, Key, false),
                           CurVal = storetuple_val(CurST),
                           CurValLen = storetuple_vallen(CurST),
                           Exp = storetuple_exptime(ST),
                           Flags = storetuple_flags(ST),
                           NewST = storetuple_make(
                                     Key, TS, CurVal, CurValLen, Exp, Flags),
                           {insert_constant_value, NewST};
                      (X) ->
                           X
              end, Thisdo_Mods),
    %%?DBG_GEN("WWWW: 2: ~p\n", [Ms]),
    Ms.

do_checkpoint(S, _Options) when S#state.check_pid /= undefined ->
    {sorry, S};
do_checkpoint(S, Options) ->
    {ok, NewLogSeq} = (S#state.wal_mod):advance_seqnum(S#state.log, 2),
    Pid = spawn_link(?MODULE, checkpoint_start,
                     [S, NewLogSeq - 1, self(), Options]),
    ShadowTab = ets:new(S#state.shadowtab_name, [ordered_set, protected,
                                                 named_table]),
    {ok, S#state{check_pid = Pid, shadowtab = ShadowTab}}.

checkpoint_start(S_ro, DoneLogSeq, ParentPid, Options) ->
    ?DBG_GENx({checkpoint_start, S_ro#state.name, start}),
    case gmt_config:get_config_value_i(brick_check_checkpoint_throttle_bytes, 1000*1000) of
        25001001 ->
            gmt_util:set_alarm({err, brick_check_checkpoint_throttle_bytes},
                               "Contact Gemini technical support"),
            ok;
        _T_bytes ->
            ok
    end,

    _LogProps = (S_ro#state.wal_mod):get_proplist(S_ro#state.log),
    Dir = (S_ro#state.wal_mod):log_name2data_dir(S_ro#state.name),
    ServerProps = S_ro#state.options,
    Finfolog = make_info_log_fun(Options),

    CheckName = ?CHECK_NAME ++ "." ++ atom_to_list(S_ro#state.ctab),
    CheckFile = Dir ++ "/" ++ CheckName ++ ".tmp",
    Finfolog("checkpoint: ~p: starting\n", [S_ro#state.name]),
    _ = file:delete(CheckFile),

    %% Allow checkpoint requester to alter our behavior, e.g. for
    %% testing purposes.
    timer:sleep(proplists:get_value(start_sleep_time, Options, 0)),

    {ok, CheckFH} = file:open(CheckFile, [binary, write]),
    ok = (S_ro#state.wal_mod):write_log_header(CheckFH),

    %% To avoid icky failure scenarios, we'll add a magic
    %% {delete_all_table_items} tuple, which effectively tells the
    %% recovery mechanism to ignore any logs that have been read prior
    %% to this one.
    DelAll = term_to_binary([{delete_all_table_items}]),
    {_, Bin1} = (S_ro#state.wal_mod):create_hunk(?LOGTYPE_METADATA,
                                                 [DelAll], []),
    file:write(CheckFH, Bin1),

    %% Dump data from the private metadata table.
    MDs = term_to_binary([{md_insert, T} ||
                             T <- ets:tab2list(S_ro#state.mdtab)]),
    {_, Bin2} = (S_ro#state.wal_mod):create_hunk(?LOGTYPE_METADATA, [MDs], []),
    file:write(CheckFH, Bin2),

    %% Dump all the "normal" data.
    ThrottleSvr = case proplists:get_value(throttle_bytes, Options) of
                      undefined ->
                          cp_throttle;
                      Num ->
                          {ok, TPid} = brick_ticket:start_link(undefined, Num),
                          TPid
                  end,
    put(zzz_throttle_pid, ThrottleSvr),
    dump_items(S_ro#state.ctab, S_ro#state.wal_mod, CheckFH),

    case proplists:get_value(checkpoint_sync_before_close, Options, true) of
        true -> ok = file:sync(CheckFH);
        _    -> ok
    end,
    timer:sleep(
      proplists:get_value(checkpoint_sleep_before_close, Options, 0)),

    ok = file:close(CheckFH),
    ok = file:rename(CheckFile,
                     (S_ro#state.wal_mod):log_file_path(Dir, DoneLogSeq)),
    ok = save_checkpoint_num(Dir, DoneLogSeq),

    OldSeqs = [X || X <- (S_ro#state.wal_mod):find_current_log_seqnums(Dir),
                    X < DoneLogSeq],
    DeleteP = case proplists:get_value(bigdata_dir, ServerProps) of
                  undefined -> true;
                  _         -> S_ro#state.wal_mod /= gmt_hlog_common
              end,
    if DeleteP ->
            %%
            %% Here's a not-perfect solution for a race condition.
            %% The race condition is between us and the
            %% gmt_hlog_common writeback process.  It's quite possible
            %% that the writeback process can try to writeback some
            %% very-recently-written data for a file that we are about
            %% to delete here.
            %%
            %% Our solution:
            %%   1. Sleep a few seconds before deleting the files.  That
            %%      will make it much less likely that gmt_hlog_common will
            %%      re-create the file sometime later.
            %%   2. We'll do it in a worker proc so that we can notify our
            %%      parent now and exit.
            %%
            Finfolog("checkpoint: ~p: deleting ~p log files\n",
                     [S_ro#state.name, length(OldSeqs)]),
            spawn(fun() ->
                          %% We have really weird intermittent
                          %% problems with both QuickCheck and with
                          %% the regression tests where very
                          %% occasionally files get deleted when they
                          %% shouldn't.
                          link(ParentPid),
                          timer:sleep(5*1000),
                          ?DBG_TLOGx({checkpoint, S_ro#state.name, async_delete,
                                      OldSeqs}),
                          [_ = file:delete((S_ro#state.wal_mod):log_file_path(
                                             Dir, X, Suffix)) ||
                              X <- OldSeqs, Suffix <- ["HLOG"]],
                          unlink(ParentPid),
                          exit(normal)
                  end);
       true ->
            Finfolog("checkpoint: ~p: moving ~p log "
                     "files to long-term archive\n",
                     [S_ro#state.name, length(OldSeqs)]),
            [(S_ro#state.wal_mod):move_seq_to_longterm(S_ro#state.log, X) ||
                X <- OldSeqs],
            _ = file:delete((S_ro#state.wal_mod):log_file_path(
                              Dir,S_ro#state.check_lastseqnum))
    end,

    %% All "bad-sequence" processing is done at brick startup.  (Bad
    %% file notifications that arrive later either have 0 keys
    %% affected (i.e. do nothing) or have > 0 keys affected (i.e. flip
    %% to 'disk_error' state).  Therefore, at this point, ETS refers
    %% to 0 keys that have known bad pointers.  So it's safe to delete
    %% now ... though there is a race that's possible with the
    %% commonLogServer processing a new bad sequence file
    %% notification.  If that race happens, it's possible to forget
    %% about that sequence file, but someone we'll re-discover the
    %% error ourselves at some future time.
    delete_external_bad_sequence_file(S_ro#state.name),

    if is_pid(ThrottleSvr) ->
            brick_ticket:stop(ThrottleSvr);
       true ->
            ok
    end,
    gen_server:cast(ParentPid, {checkpoint_last_seqnum, DoneLogSeq}),
    Finfolog("checkpoint: ~p: finished\n", [S_ro#state.name]),
    ?DBG_GENx({checkpoint_start, S_ro#state.name, done}),
    exit(done).

dump_items(Tab, WalMod, Log) ->
    dump_items2(ets:first(Tab), Tab, WalMod, Log, 0, []).

%% @doc Main iterator loop for dumping/writing a table to a disk log.
%%
%% Note about separation of ETS and disk storage: Direct use of
%% ets:lookup() here is OK: we don't need to dump "real" values of val
%% here.

dump_items2('$end_of_table', _Tab, WalMod, LogFH, _AccNum, Acc) ->
    {_, Bin} = WalMod:create_hunk(?LOGTYPE_METADATA,
                                  [term_to_binary(lists:reverse(Acc))], []),
    file:write(LogFH, Bin),
    ok;
dump_items2(Key, Tab, WalMod, LogFH, AccNum, Acc)
  when AccNum > 200 ->
    {Bytes, Bin} = WalMod:create_hunk(?LOGTYPE_METADATA,
                                      [term_to_binary(lists:reverse(Acc))], []),
    ok = get_bw_ticket(Bytes),
    file:write(LogFH, Bin),
    dump_items2(Key, Tab, WalMod, LogFH, 0, []);
dump_items2(Key, Tab, WalMod, LogFH, AccNum, Acc) ->
    [ST] = ets:lookup(Tab, Key),
    %% The insert_value_into_ram flavor is not required here: the
    %% storetuple is already in the {SeqNum, OffSet} or binary() form
    %% that we need for a local checkpoint dump.
    I = {insert, ST},
    dump_items2(ets:next(Tab, Key), Tab, WalMod, LogFH, AccNum + 1, [I|Acc]).

save_checkpoint_num(Dir, SeqNum) ->
    FinalPath = Dir ++ "/last-checkpoint",
    TmpPath = FinalPath ++ ".tmp",
    {ok, FH} = file:open(TmpPath, [write, binary]), % {sigh}
    try
        ok = file:write(FH, [integer_to_list(SeqNum), "\n"]),
        ok = file:sync(FH),
        ok = file:rename(TmpPath, FinalPath)
    catch X:Y ->
        ?E_ERROR("Error renaming ~p -> ~p: ~p ~p (~p)\n",
                 [TmpPath, FinalPath, X, Y, erlang:get_stacktrace()])
    after
        ok = file:close(FH)
    end.

read_checkpoint_num(Dir) ->
    try
        {ok, Bin} = file:read_file(Dir ++ "/last-checkpoint"),
        {SeqNum, _} = string:to_integer(binary_to_list(Bin)),
        SeqNum
    catch _:_ ->
        1                                       % Get 'em all
    end.

sync_pid_start(SPA) ->
    process_flag(priority, high),
    link(SPA#syncpid_arg.wal_pid),      % share fate
    Interval = gmt_config:get_config_value_i(brick_sync_interval_msec, 500),
    brick_itimer:send_interval(Interval, force_sync),
    sync_pid_loop(SPA).

sync_pid_loop(SPA) ->
    %% The 3rd param to collect_sync_requests() is a minor (?)
    %% tunable, values lower than 5 start hurting on my laptop 10-15%
    %% but larger than 5 seem to help very little (and add latency in
    %% extremely low load situations).
    L = collect_sync_requests([], SPA, 5),
    if L /= [] ->
            LastSerial = sync_get_last_serial(L),
            ?DBG_GEN("DBG: SPA ~p requesting sync last serial = ~p\n",
                     [SPA#syncpid_arg.name, LastSerial]),
            Start = now(),                      %qqq debug
            {ok, _X, _Y} = wal_sync(SPA#syncpid_arg.wal_pid,
                                    SPA#syncpid_arg.wal_mod),
            DiffMS = timer:now_diff(now(), Start) div 1000, %qqq debug
            SPA#syncpid_arg.parent_pid !
                {syncpid_stats, SPA#syncpid_arg.name, DiffMS, ms, length(L)},
            ?DBG_GEN("DBG: SPA ~p sync_done at ~p,~p, my last serial = ~p\n",
                     [SPA#syncpid_arg.name, _X, _Y, LastSerial]),
            SPA#syncpid_arg.parent_pid ! {sync_done, self(), LastSerial};
       true ->
            ok
    end,
    ?MODULE:sync_pid_loop(SPA).

collect_sync_requests([], SPA, Timeout) ->
    %% Nobody should be sending us anything other than sync request
    %% tuples Block forever waiting for the first request, then slurp
    %% in as many as we can without blocking.
    receive
        T when is_tuple(T) ->
            collect_sync_requests([T], SPA, Timeout);
        force_sync ->
            []                                  % return empty list to caller
    end;
collect_sync_requests(Acc, SPA, Timeout) ->
    receive
        T when is_tuple(T) ->
            collect_sync_requests([T|Acc], SPA, Timeout);
        force_sync ->
            %% We can still collect requests when this message
            %% arrives, but we won't wait any longer for stragglers.
            collect_sync_requests(Acc, SPA, 0)
    after Timeout ->
            lists:reverse(Acc)
    end.

sync_get_last_serial(L) ->
    sync_get_last_serial(L, 0).

sync_get_last_serial([{sync_msg, Serial}|T], _LastSerial) ->
    sync_get_last_serial(T, Serial);
sync_get_last_serial([], LastSerial) ->
    LastSerial.

%% @spec (state_r()) -> state_r()

%% LTODO: unfinished
do_check_checkpoint(S)
  when S#state.check_pid == undefined ->
    LogDir = S#state.log_dir,
    LogFiles = (S#state.wal_mod):find_current_log_files(LogDir),
    Sum =
        lists:foldl(fun(File, Acc) ->
                            %% TODO: Implementation detail leak below.
                            case file:read_file_info(LogDir ++ "/s/" ++ File) of
                                {ok, FI} ->
                                    FI#file_info.size + Acc;
                                {error, enoent} ->
                                    %% In the 2-tier CommonLog scheme, this
                                    %% can happen occasionally, don't log.
                                    %% error_logger:error_msg("do_check_checkpoint: ~p: File ~p in dir ~p doesn't exist\n", [S#state.name, File, LogDir]),
                                    Acc
                            end
                    end, 0, LogFiles),
    SumMB = Sum / (1024*1024),
    MaxMB =
        case re:run(atom_to_list(S#state.name), "bootstrap_") of
            nomatch ->
                gmt_config:get_config_value_i(brick_check_checkpoint_max_mb,5);
            {match, _} ->
                %% Use a very small value for bootstrap bricks: they
                %% aren't supposed to be storing very much, and quick
                %% startup time is essential.
                40
        end,
    if SumMB > MaxMB ->
            ?E_INFO("table ~p: SumMB ~p > MaxMB ~p\n",
                    [S#state.name, SumMB, MaxMB]),
            {ok, NewS} = do_checkpoint(S, S#state.options),
            NewS;
       true ->
            S
    end;
do_check_checkpoint(S) ->
    %% Do not perform checkpoint until my_repair_state = ok.
    S.

fold_shadow_into_ctab(S) ->
    %% To reuse my_insert_ignore_logging() and
    %% my_delete_ignore_logging(), set a special magic flag
    fold_shadow_into_ctab(ets:first(S#state.shadowtab),
                          S#state{bypass_shadowtab = true}).

%% @doc Main iterator loop for folding table modifications recorded in
%% the shadowtab table back into the main table.
%%
%% Note about separation of ETS and disk storage:
%% Direct use of ets:lookup() here is OK: we don't need to copy "real"
%% values of val here.
%% <b>However</b>, we do need to move each shadow val disk file to its
%% proper location.

fold_shadow_into_ctab('$end_of_table', S) ->
    ets:delete(S#state.shadowtab),
    S#state{shadowtab = undefined, bypass_shadowtab = false};
fold_shadow_into_ctab(Key, S) ->
    [Shadow] = ets:lookup(S#state.shadowtab, Key),
    case Shadow of
        {Key, insert, StoreTuple} ->
            my_insert_ignore_logging(S, Key, StoreTuple),
            fold_shadow_into_ctab(ets:next(S#state.shadowtab, Key), S);
        {Key, delete, ExpTime} ->
            my_delete_ignore_logging(S, Key, ExpTime),
            fold_shadow_into_ctab(ets:next(S#state.shadowtab, Key), S);
        Err ->
            ?E_WARNING("fold_shadow: Bad term for ~p: ~p\n", [Key, Err]),
            timer:sleep(1000),
            exit({fold, Key, Err})
    end.

map_mods_into_ets([{insert, StoreTuple}|Tail], S) ->
    Key = storetuple_key(StoreTuple),
    my_insert_ignore_logging(S, Key, StoreTuple),
    map_mods_into_ets(Tail, S);
map_mods_into_ets([{insert, _ChainStoreTuple, StoreTuple}|Tail], S) ->
    %% Lazy, reuse....
    map_mods_into_ets([{insert, StoreTuple}|Tail], S);
map_mods_into_ets([{insert_value_into_ram, StoreTuple}|Tail], S) ->
    %% Lazy, reuse....
    map_mods_into_ets([{insert, StoreTuple}|Tail], S);
map_mods_into_ets([{delete, Key, ExpTime}|Tail], S) ->
    my_delete_ignore_logging(S, Key, ExpTime),
    map_mods_into_ets(Tail, S);
map_mods_into_ets([{delete_noexptime, Key}|Tail], S) ->
    %% This item only used during migration.
    ExpTime = case my_lookup(S, Key, false) of
                  [StoreTuple] ->
                      storetuple_exptime(StoreTuple);
                  [] ->
                      0
              end,
    map_mods_into_ets([{delete, Key, ExpTime}|Tail], S);
map_mods_into_ets([{insert_constant_value, StoreTuple}|Tail], S) ->
    %% Lazy, reuse....
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
  when is_tuple(H), element(1, H) == chain_send_downstream ->
    exit(should_not_be_happening_bad_scott_1),
    map_mods_into_ets(Tail, S);
map_mods_into_ets(NotList, _S) when not is_list(NotList) ->
    %% This is when applying mods to head, when head is a sweep tuple.
    ok.

pull_items_out_of_logging_queue(LastLogSerial, S) ->
    Q = S#state.logging_op_q,
    pull_items_out_of_logging_queue(queue:out(Q), [], Q,
                                     LastLogSerial, -1, S).

pull_items_out_of_logging_queue({{value, LQI}, NewQ}, Acc, _OldQ,
                                 LastLogSerial, UpstreamSerial, S)
  when is_record(LQI, log_q), LQI#log_q.logging_serial =< LastLogSerial ->
    pull_items_out_of_logging_queue(queue:out(NewQ), [LQI|Acc], NewQ,
                                     LastLogSerial, UpstreamSerial, S);
%%
pull_items_out_of_logging_queue({{value, {upstream_serial, NewUS}}, NewQ}, Acc,
                                _OldQ, LastLogSerial, _UpstreamSerial, S) ->
    pull_items_out_of_logging_queue(queue:out(NewQ), Acc, NewQ,
                                     LastLogSerial, NewUS, S);
%%
pull_items_out_of_logging_queue(_, Acc, Q, _LastLogSerial,
                                _UpstreamSerial, S) ->
    {lists:reverse(Acc), S#state{logging_op_q = Q}}.

%% @spec (list(thisdo_mods()), true | false | undefined, state_r()) ->
%%       {goahead, state_r()} | {wait, state_r()}
%% @doc Write a set of Thisdo_Mods to disk, with the option of overriding sync.

log_mods2(Thisdo_Mods, SyncOverride, S) ->
    log_mods2_b(Thisdo_Mods, S, SyncOverride).  % thin shim nowadays

log_mods2_b([], S, _) ->                        % dialyzer: can never match...
    {goahead, S#state{thisdo_mods = []}};
log_mods2_b(Thisdo_Mods0, S, SyncOverride) ->
    Thisdo_Mods = lists:map(
                    fun({insert, _ChainStoreTuple, StoreTuple}) ->
                            {insert, StoreTuple};
                       (X) ->
                            X
                    end, Thisdo_Mods0),
    {ok, _, _} = wal_write_metadata_term(Thisdo_Mods, S),
    if S#state.do_sync == true ->
            %% Guard test SyncOverride::'undefined' == 'true' can never succeed
            if SyncOverride == false ->
                    %% NOTICE: This clobbering of #state.thisdo_mods
                    %% should not cause problems when called outside
                    %% the context of a handle_call({do, ...}, ...)
                    %% call, right?
                    {goahead, S#state{thisdo_mods = []}};
               true ->
                    {wait, S}
            end;
       S#state.do_sync == false ->
            %% Guard test SyncOverride::'undefined' == 'true' can never succeed
            if SyncOverride == true ->
                    {wait, S};
               true ->
                    {goahead, S#state{thisdo_mods = []}}
            end
    end.

any_dirty_keys_p(DoKeys, State) ->
    lists:any(fun({_, K}) -> check_dirty_key(K, State) end, DoKeys).

any_readonly_keys_p(DoKeys) ->
    lists:any(fun({read,  _}) -> true;
                 ({write, _}) -> false end,
              DoKeys).

check_dirty_key(Key, State) ->
    ets:member(State#state.dirty_tab, Key).

add_mods_to_dirty_tab([{InsertLike, StoreTuple}|Tail], S)
  when InsertLike == insert; InsertLike == insert_constant_value;
       InsertLike == insert_value_into_ram ->
    Key = storetuple_key(StoreTuple),
    ?DBG_ETSx({add_to_dirty, S#state.name, Key}),
    ets:insert(S#state.dirty_tab, {Key, insert, StoreTuple}),
    add_mods_to_dirty_tab(Tail, S);
add_mods_to_dirty_tab([{insert, StoreTuple, _BigDataTuple}|Tail], S) ->
    %% Lazy, reuse...
    add_mods_to_dirty_tab([{insert, StoreTuple}|Tail], S);
add_mods_to_dirty_tab([{delete, Key, _}|Tail], S) ->
    ?DBG_ETSx({add_to_dirty, S#state.name, Key}),
    ets:insert(S#state.dirty_tab, {Key, delete, delete}),
    add_mods_to_dirty_tab(Tail, S);
add_mods_to_dirty_tab([_|Tail], S) ->
    %% Log replay may have weird terms in the list that we can ignore.
    add_mods_to_dirty_tab(Tail, S);
add_mods_to_dirty_tab([], _S) ->
    ok.

clear_dirty_tab([{insert, StoreTuple}|Tail], S) ->
    Key = storetuple_key(StoreTuple),
    ?DBG_ETSx({clear_dirty, S#state.name, Key}),
    ets:delete(S#state.dirty_tab, Key),
    clear_dirty_tab(Tail, S);
clear_dirty_tab([{insert, _ChainStoreTuple, StoreTuple}|Tail], S) ->
    %% Lazy, reuse....
    clear_dirty_tab([{insert, StoreTuple}|Tail], S);
clear_dirty_tab([{insert_value_into_ram, StoreTuple}|Tail], S) ->
    %% Lazy, reuse....
    clear_dirty_tab([{insert, StoreTuple}|Tail], S);
clear_dirty_tab([{delete, Key, _}|Tail], S) ->
    ?DBG_ETSx({clear_dirty, S#state.name, Key}),
    ets:delete(S#state.dirty_tab, Key),
    clear_dirty_tab(Tail, S);
clear_dirty_tab([{delete_noexptime, Key}|Tail], S) ->
    ?DBG_ETSx({clear_dirty_noexptime, S#state.name, Key}),
    ets:delete(S#state.dirty_tab, Key),
    clear_dirty_tab(Tail, S);
clear_dirty_tab([{insert_constant_value, StoreTuple}|Tail], S) ->
    %% Lazy, reuse....
    clear_dirty_tab([{insert, StoreTuple}|Tail], S);
clear_dirty_tab([], _S) ->
    ok;
clear_dirty_tab([{log_directive, _, _}|Tail], S) ->
    clear_dirty_tab(Tail, S);
clear_dirty_tab([{log_noop}|Tail], S) ->
    clear_dirty_tab(Tail, S);
clear_dirty_tab([H|Tail], S)
  when is_tuple(H), element(1, H) == chain_send_downstream ->
    exit(should_not_be_happening_bad_scott_2),
    clear_dirty_tab(Tail, S);
clear_dirty_tab(NotList, _S) when not is_list(NotList) ->
    ok.

retry_dirty_key_ops(S) ->
    case queue:is_empty(S#state.wait_on_dirty_q) of
        true ->
            ?DBG_OPx({retry_dirty_key_ops, S#state.name, queue_empty}),
            {S, []};
        false ->
            F = fun(#dirty_q{from = From, do_op = DoOp}, {InterimS, ToDos}) ->
                        ?DBG_OPx({retry_dirty_key_ops, S#state.name, DoOp}),
                        case handle_call(DoOp, From, InterimS) of
                            %% case_clause: {up1, list(), reply()} ...
                            %% CHAIN TODO: See comment "ISSUE001".
                            {up1, TDs, OrigReply} ->
                                %%?DBG_GEN("QQQ ISSUE001: up todos = ~p\n", [get(issue001)]),
                                %%?DBG_GEN("QQQ ISSUE001: TDs = ~p\n", [TDs]),
                                %%?DBG_GEN("QQQ ISSUE001: OrigReply = ~p\n", [OrigReply]),
                                %% timer:sleep(5*1000), exit(asdlkfasdf);
                                NewToDos = TDs ++ ToDos,
                                case OrigReply of
                                    %% Dialyzer says this clause is impossible.
                                    %% 12 Sept 2008.
                                    {reply, Reply, NewInterimS} ->
                                        gen_server:reply(From, Reply),
                                        {NewInterimS, NewToDos};
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
                                       "Contact Gemini technical support",
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
    ?DBG_REPAIRx({rep_c, UpKey, LastKey}),
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
    ?DBG_REPAIRx({rep_a, LastKey}),
    {LastKey, Is, Ds, S};
repair_loop([StoreTuple|Tail], ?BRICK__GET_MANY_LAST = MyKey, Is, Ds, S) ->
    ?DBG_REPAIRx({rep_b, storetuple_key(StoreTuple)}),
    ok = repair_loop_insert(StoreTuple, S),
    repair_loop(Tail, MyKey, Is + 1, Ds, S);
repair_loop([StoreTuple|Tail] = UpList, MyKey, Is, Ds, S) ->
    UpKey = storetuple_key(StoreTuple),
    if UpKey < MyKey ->
            ?DBG_REPAIRx({rep_c, UpKey, MyKey}),
            ok = repair_loop_insert(StoreTuple, S),
            repair_loop(Tail, MyKey, Is + 1, Ds, S);
       UpKey =:= MyKey ->
            ?DBG_REPAIRx({rep_d, UpKey, MyKey}),
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
                    MyVal = if S#state.bigdata_dir == undefined ->
                                    storetuple_val(MyStoreTuple);
                               true ->
                                    Val0 = storetuple_val(StoreTuple),
                                    ValLen = storetuple_vallen(StoreTuple),
                                    bigdata_dir_get_val(UpKey, Val0, ValLen, S)
                            end,
                    if UpVal =/= MyVal ->
                            ?E_INFO(
                              "TODO: Weird, but we got a repair "
                              "update for ~p with same ts but "
                              "different values", [UpKey]),
                            throw(hibari_inconceivable_2398223);
                       true ->
                            %% Nothing to do
                            ?DBG_REPAIRx({repair, do_nothing, UpKey}),
                            repair_loop(Tail, ets:next(S#state.ctab, MyKey),
                                        Is, Ds, S)
                    end;
               %% UpTS < MyTS has already been tested!
               UpTS > MyTS ->
                    %% Upstream's value trumps us every time.
                    ?DBG_REPAIRx({UpKey, UpTS, UpVal}),
                    ok = repair_loop_insert(StoreTuple, S),
                    repair_loop(Tail, ets:next(S#state.ctab, MyKey),
                                Is + 1, Ds, S)
            end;
       UpKey > MyKey ->
            ?DBG_REPAIRx({rep_e, UpKey, MyKey}),
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
    ?DBG_REPAIRx({repair, insert, Key}),
    Thisdo_Mods = case lists:member(value_in_ram, Flags) of
                      true ->
                          [{insert_value_into_ram,
                            storetuple_replace_flags(StoreTuple,
                                                     Flags -- [value_in_ram])}];
                      false ->
                          [{insert, StoreTuple}]
                  end,
    LocalMods = filter_mods_from_upstream(Thisdo_Mods, S),
    _ = log_mods2(LocalMods, undefined, S),
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
    ?DBG_REPAIR("\nrepair_loop_insert: ~p\n", [ST]),
    my_insert_ignore_logging(S, Key, ST),
    ok.

repair_loop_delete(Key, ExpTime, S) ->
    ?DBG_REPAIRx({repair, delete, Key}),
    Thisdo_mods = [{delete, Key, ExpTime}],
    _ = log_mods2(Thisdo_mods, undefined, S),
    my_delete_ignore_logging(S, Key, ExpTime),
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

bigdata_dir_store_val(_Key, <<>>, _S) ->
    {0, 0};                                     % Special case for 0 len binary
bigdata_dir_store_val(Key, Val, S) ->
    {ok, Seq, Off} = (S#state.wal_mod):write_hunk(
                       S#state.log, S#state.name, bigblob, Key,
                       ?LOGTYPE_BLOB, [Val],[]),
    ?DBG_GEN("DBG: store_val ~p at ~p,~p\n", [Key, Seq, Off]),
    {Seq, Off}.

bigdata_dir_delete_val(_Key, _S) ->
    %% LTODO: anything to do here?
    ok.

bigdata_dir_get_val(_Key, Val, ValLen, S) ->
    bigdata_dir_get_val(_Key, Val, ValLen, true, S).

%% TODO: This bad boy will crash if the file doesn't exist.

bigdata_dir_get_val(_Key, Val, ValLen, CheckMD5_p, S)
  when is_tuple(Val) ->
    case Val of
        {0, 0} ->
            <<>>;
        {SeqNum, Offset} ->
            %% {sigh} So much for 100% module API interop.
            case if S#state.wal_mod == gmt_hlog_local ->
                         (S#state.wal_mod):read_bigblob_hunk_blob(
                           SeqNum, Offset, CheckMD5_p, ValLen);
                    true ->
                         (S#state.wal_mod):read_bigblob_hunk_blob(
                           S#state.log_dir, SeqNum, Offset, CheckMD5_p,
                           ValLen)
                 end of
                Blob when is_binary(Blob) ->
                    Blob;
                {error, Reason} when Reason == system_limit ->
                    %% We shouldn't mark the file as bad: in this
                    %% clause we aborted before reads were entirely
                    %% successful.  Logging an error probably won't
                    %% work because we're probably pretty hosed
                    %% (e.g. out of file descriptors), but we should
                    %% try anyway.
                    %%
                    %% TODO: What other erors here that we need to look for?
                    %%
                    ?E_ERROR("~s: read error ~p at ~p ~p\n",
                             [S#state.name, Reason, SeqNum, Offset]),
                    throw(silently_drop_reply);
                eof when S#state.do_sync == false ->
                    %% The brick is in async mode.  If we're trying to
                    %% read something that was written very very
                    %% recently, then the commonLogServer may have
                    %% buffered it while a sync for some other brick
                    %% is in progress.  If that's true, then the data
                    %% we're looking for hasn't been written to the OS
                    %% yet, and trying to read from the promised file
                    %% & offset will yield 'eof'.  Doctor, it hurts
                    %% when I read async-written data too early....
                    throw(silently_drop_reply);
                Error ->
                    ?E_WARNING("~s: error ~p at ~p ~p\n",
                               [S#state.name, Error, SeqNum, Offset]),
                    sequence_file_is_bad(SeqNum, Offset, S),
                    throw(silently_drop_reply)
            end
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

sequence_file_is_bad(SeqNum, Offset, S)
  when S#state.wal_mod == gmt_hlog ->
    write_bad_sequence_hunk(S#state.wal_mod, S#state.log, S#state.name,
                            SeqNum, Offset),
    sequence_file_is_bad_common(S#state.log_dir, S#state.wal_mod, S#state.log,
                                S#state.name, SeqNum, Offset);
sequence_file_is_bad(SeqNum, Offset, S)
  when S#state.wal_mod == gmt_hlog_local ->
    append_external_bad_sequence_file(S#state.name, SeqNum),
    gmt_hlog_common:sequence_file_is_bad(SeqNum, Offset).

sequence_rename(OldPath1, OldPath2, NewPath) ->
    case file:rename(OldPath1, NewPath) of
        ok ->
            ok;
        {error, enoent} ->
            file:rename(OldPath2, NewPath);
        Err ->
            Err
    end.

write_bad_sequence_hunk(WalMod, Log, Name, SeqNum, Offset) ->
    try
        _ = WalMod:advance_seqnum(Log, 2),
        _ = WalMod:write_hunk(
              Log, Name, metadata, <<"k:sequence_file_is_bad">>,
              ?LOGTYPE_BAD_SEQUENCE, [term_to_binary({SeqNum, Offset})], []),
        _ = wal_sync(Log, WalMod)
    catch
        X:Y ->
            ?E_ERROR("sequence_file_is_bad: ~p ~p -> ~p ~p (~p)\n",
                     [SeqNum, Offset, X, Y, erlang:get_stacktrace()])
    end.

append_external_bad_sequence_file(Name, SeqNum) ->
    {ok, FH} = file:open(external_bad_sequence_path(Name), [append]),
    io:format(FH, "~p.\n", [abs(SeqNum)]),
    ok = file:close(FH).

delete_external_bad_sequence_file(Name) ->
    file:delete(external_bad_sequence_path(Name)).

read_external_bad_sequence_file(Name) ->
    case file:consult(external_bad_sequence_path(Name)) of
        {ok, L} ->
            lists:usort([abs(X) || X <- L]);
        {error, enoent} ->
            []
    end.

external_bad_sequence_path(Name) ->
    gmt_hlog:log_name2data_dir(Name) ++ "/bad-sequence".

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
                       "renamed to ~s\n",
                       [Name, SeqNum, Offset, NewPath]);
        {error, Reason} ->
            ?E_WARNING("Brick ~p bad sequence file: seq ~p offset ~p: "
                       "rename ~s or ~s to ~s failed: ~p\n",
                       [Name, SeqNum, Offset, OldPath1, OldPath2,
                        NewPath, Reason])
    end.

%%
%% Write-ahead log helper functions
%%

%% @doc Returns a 2-tuple: {accumulator from the fold fun, list of errors}
%%
%% WARNING: This function will purge our mailbox of all '$gen_call' and
%%          '$gen_cast' messages.  For use only within the context of
%%          gen_server's init() callback function!

wal_scan_all(S, MinimumSeqNum) ->
    put(wal_scan_cast_count, 0),
    F = fun(H, FH, Acc) when H#hunk_summ.type == ?LOGTYPE_METADATA ->
                %% This function may run for hours or perhaps even days.
                flush_gen_server_calls(),       % Clear any mailbox backlog
                flush_gen_cast_calls(S),        % Clear any mailbox backlog

                [_] = H#hunk_summ.c_len,        % sanity
                []  = H#hunk_summ.u_len,        % sanity

                CB = (S#state.wal_mod):read_hunk_member_ll(FH, H, md5, 1),
                %% LTODO: Don't crash if there's a failure.
                %% Hibari: TODO: Be robust here.  If the checksum does not match,
                %%      then we have a problem to solve.  If we're not the
                %%      only in the chain, we can punt.  If we are the
                %%      only/best of the chain, then we have a data loss
                %%      problem that we need to report, somehow.
                true = (S#state.wal_mod):md5_checksum_ok_p(H#hunk_summ{c_blobs = [CB]}),
                wal_load_recs_from_log(binary_to_term(CB), S),
                Acc + 1;
           (H, _FH, Acc) when H#hunk_summ.type == ?LOGTYPE_BLOB ->
                Acc;
           (H, FH, Acc) when H#hunk_summ.type == ?LOGTYPE_BAD_SEQUENCE ->
                [_] = H#hunk_summ.c_len,        % sanity
                []  = H#hunk_summ.u_len,        % sanity
                CB = (S#state.wal_mod):read_hunk_member_ll(FH, H, md5, 1),
                case (S#state.wal_mod):md5_checksum_ok_p(H#hunk_summ{c_blobs = [CB]}) of
                    true ->
                        {SeqNum, Offset} = binary_to_term(CB),
                        wal_purge_recs_by_seqnum(SeqNum, Offset, S),
                        Acc + 1;
                    false ->
                        %% Hibari: TODO: How do we escalate the bad news??
                        ?E_ERROR("Bad checksum on hunk type ~p at ~p ~p\n",
                                 [H#hunk_summ.type, H#hunk_summ.seq,
                                  H#hunk_summ.off]),
                        Acc
                end;
           (H, _FH, Acc) ->
                ?E_ERROR("Unknown hunk type at ~p ~p: ~p\n",
                         [H#hunk_summ.seq, H#hunk_summ.off, H#hunk_summ.type]),
                Acc
        end,
    Ffilter = fun(N) -> N >= MinimumSeqNum end,
    Res = (S#state.wal_mod):fold(shortterm, S#state.log_dir, F, Ffilter, 0),
    %% erase(wal_scan_cast_count),
    Res.

wal_load_recs_from_log([H], S) ->
    load_rec_from_log(H, S);
wal_load_recs_from_log([H|T], S) ->
    load_rec_from_log(H, S),
    wal_load_recs_from_log(T, S);
wal_load_recs_from_log([], _S) ->
    ok.

wal_write_metadata_term(Term, S) ->
    (S#state.wal_mod):write_hunk(
      S#state.log, S#state.name, metadata, <<"k:md">>, ?LOGTYPE_METADATA,
      [term_to_binary(Term)], []).

wal_sync(Pid, WalMod) when is_pid(Pid) ->
    WalMod:sync(Pid);
wal_sync(S, WalMod) when is_record(S, state) ->
    WalMod:sync(S#state.log).

wal_purge_recs_by_seqnum(SeqNum, Offset, S) ->
    ?E_ERROR("A bad log sequence file, ~p number ~p, has been detected "
             "(error at offset ~p).\n", [S#state.name, SeqNum, Offset]),
    {NumRecs, _NewS} = purge_recs_by_seqnum(SeqNum, true, S),
    ?E_ERROR("Log sequence file ~p ~p contained ~p keys which are now lost.\n",
             [S#state.name, SeqNum, NumRecs]),
    gmt_util:set_alarm({data_lost, {S#state.name, seq, SeqNum, keys, NumRecs}},
                       "Number of keys lost = " ++ integer_to_list(NumRecs) ++
                       ", must recover via chain replication.",
                       fun() -> ok end),
    NumRecs.

wal_scan_failed(ErrList, S) ->
    ?E_WARNING("WAL scan by brick ~p had errors: ~p\n",[S#state.name, ErrList]),
    _Msg = lists:flatten(
            io_lib:format("Number of keys lost = unknown, must recover "
                          "via chain replication.  Error detail = ~p.",
                          [ErrList])),
    %% gmt_util:set_alarm({wal_scan, S#state.name}, _Msg, fun() -> ok end),
    ok.

flush_gen_server_calls() ->
    receive {'$gen_call', _, _} -> flush_gen_server_calls()
    after 0                     -> ok
    end.

flush_gen_cast_calls(S) ->
    receive {'$gen_cast', Msg} ->
            Count = case get(wal_scan_cast_count) of undefined -> 0;
                                                     N ->         N
                    end,
            if Count < 10 ->
                    ?E_INFO("~p: flushing $gen_cast message: ~P\n",
                            [S#state.name, Msg, 9]);
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

squidflash_primer({do, _SentAt, Dos, DoFlags} = DoOp, From, S) ->
    KsRaws = lists:foldl(
               fun({get, Key, Flags}, Acc) ->
                       case {proplists:get_value(witness, Flags),
                             my_lookup(S, Key, false)} of
                           {true, _} ->
                               Acc;
                           {undefined, []} ->
                               Acc;
                           {undefined, [ST]} ->
                               accumulate_maybe(Key, ST, Acc)
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
                                                    true, DoFlags, S),
                               {L, _MoreP} = X,
                               F_look = fun(K, Acc2) ->
                                                [ST] = my_lookup(S, K, false),
                                                accumulate_maybe(Key, ST, Acc2)
                                        end,
                               lists:foldl(fun({K, _TS}, Acc2) ->
                                                   F_look(K, Acc2);
                                              ({K, _TS, _Flags}, Acc2) ->
                                                   F_look(K, Acc2)
                                           end, [], L) ++ Acc
                       end;
                  (_, Acc) ->
                       Acc
               end, [], Dos),
    if KsRaws == [] ->
            goahead;
       true ->
            %% OK, this state hack pretty ugly, but because we're
            %% spawning a proc to do our dirty work, options are
            %% limited.  And I'm lazy.
            FakeS = #state{name = S#state.name,
                           log = S#state.log, log_dir = S#state.log_dir,
                           wal_mod = S#state.wal_mod},
            Me = self(),
            spawn(fun() -> squidflash_doit(KsRaws, DoOp, From, Me, FakeS) end)
    end.

accumulate_maybe(Key, ST, Acc) ->
    case storetuple_val(ST) of
        Val when is_tuple(Val) ->
            ValLen = storetuple_vallen(ST),
            [{Key, Val, ValLen}|Acc];
        Blob when is_binary(Blob) ->
            Acc
    end.

squidflash_doit(KsRaws, DoOp, From, ParentPid, FakeS) ->
    Me = self(),
    KRV_Refs = [{X, make_ref()} || X <- KsRaws],
    [gmt_parallel_limit:enqueue(
       brick_primer_limit,
       fun() ->
               catch squidflash_prime1(Key, RawVal, ValLen, Me, FakeS),
               Me ! Ref,
               exit(normal)
       end) || {{Key, RawVal, ValLen}, Ref} <- KRV_Refs],
    [receive
         Ref -> ok
     after 10000 ->                             % should be impossible, but...
             ok
     end || {_, Ref} <- KRV_Refs],
    %% TODO: This is also an ugly kludge ... hide it somewhere at
    %% least?  The alternative is to have a handle_call() ->
    %% handle_cast() conversion doodad ... this is lazier.
    {do, _SentAt, Dos, DoFlags} = DoOp,
    ParentPid ! {'$gen_call', From, {do, now(), Dos, [squidflash_resubmit|DoFlags]}},
    exit(normal).

squidflash_prime1(Key, RawVal, ValLen, ReplyPid, FakeS) ->
    case (catch bigdata_dir_get_val(Key, RawVal, ValLen, false, FakeS)) of
        {'EXIT', _X} ->
            ?E_ERROR("cache prime: ~p: ~p at ~p: ~p\n",
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
                               if ExpTime == ExpTime2 ->
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

-spec expiry_iter_keys(table_name(), integer(), {integer(), key()}) -> list().
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
    if Ops /= [] ->
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
    if KeysTs /= [] -> ?E_INFO("Verbose expiry: ~p ~p\n", [ServerName, KeysTs]);
       true         -> ok
    end,
    delete_keys_immediately(ServerName, KeysTs).

%% @spec (fun(), term(), list(), fun(), integer) -> term()
%% @doc See gmt_util:list_chunkfoldl/5.

foldl_lump(F_inner, Acc, L, F_outer, Count) ->
    gmt_util:list_chunkfoldl(F_inner, Acc, L, F_outer, Count).

slurp_log_chunks(Log) ->
    lists:append(slurp_log_chunks(disk_log:chunk(Log, start), Log, [])).

slurp_log_chunks(eof, _Log, Acc) ->
    lists:reverse(Acc);
slurp_log_chunks({error, _} = Err, _Log, _Acc) ->
    exit({slurp_log_chunks, Err});
slurp_log_chunks({Cont, Ts}, Log, Acc) ->
    slurp_log_chunks(disk_log:chunk(Log, Cont), Log, [Ts|Acc]).

count_live_bytes_in_log(Log) ->
    disk_log_fold(fun({live_bytes, Bs}, Sum) -> Sum + Bs;
                     (_               , Sum) -> Sum
                  end, 0, Log).

disk_log_fold(Fun, Acc, Log) ->
    disk_log_fold_2(disk_log:chunk(Log, start), Fun, Acc, Log,
                    fun(X) -> X end).

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

temp_path_to_seqnum(Path) ->
    list_to_integer(string:substr(Path, string:rchr(Path, $/) + 1)).

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

%% TODO: Should this be moved to gmt_hlog.erl?

delete_seq(SA, SeqNum) when SA#scav.destructive == true ->
    [_ = file:delete((SA#scav.wal_mod):log_file_path(SA#scav.log_dir,
                                                     SeqNum, "HLOG")),
     _ = file:delete((SA#scav.wal_mod):log_file_path(SA#scav.log_dir,
                                                     -SeqNum, "HLOG"))];
delete_seq(_, _) ->
    ok.

scavenger_get_keys(Name, Fs, FirstKey, F_k2d, F_lump) ->
    scavenger_get_keys(Name, Fs,
                       scavenger_get_many(Name, FirstKey, Fs), [],
                       F_k2d, F_lump, 1).

scavenger_get_keys(Name, Fs, Res, Acc, F_k2d, F_lump, Iters)
  when Iters rem 4 == 0 ->
    foldl_lump(F_k2d, dict:new(), Acc, F_lump, 100*1000),
    scavenger_get_keys(Name, Fs, Res, [], F_k2d, F_lump, Iters + 1);
scavenger_get_keys(Name, _Fs, {ok, {Rs, false}}, Acc, F_k2d, F_lump, _Iters) ->
    foldl_lump(F_k2d, dict:new(), prepend_rs(Name, Rs, Acc), F_lump, 100*1000),
    ok;
scavenger_get_keys(Name, Fs, {ok, {Rs, true}}, Acc, F_k2d, F_lump, Iters) ->
    K = storetuple_key(lists:last(Rs)),
    scavenger_get_keys(Name, Fs, scavenger_get_many(Name, K, Fs),
                       prepend_rs(Name, Rs, Acc), F_k2d, F_lump, Iters + 1).

scavenger_get_many(Name, Key, Flags) ->
    [Res] = brick_server:do(Name, node(),
                            [brick_server:make_get_many(Key, 1000, Flags)],
                            [ignore_role], 5000),
    Res.

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
copy_one_hunk(SA, FH, Key, SeqNum, Offset, Fread_blob) ->
    try begin
            Blob = if SA#scav.skip_reads == true ->
                           <<>>;
                      true ->
                           %% TODO: Use a non-zero length hint here.
                           (SA#scav.wal_mod):read_hunk_summary(
                             FH, SeqNum, Offset, 0, Fread_blob)
                   end,
            if not is_binary(Blob) -> ?APPLOG_ALERT(?APPLOG_APPM_049,"DEBUG: Blob = ~P\n", [Blob, 10]); true -> ok end,
            true = is_binary(Blob),
            if SA#scav.destructive == true ->
                    case (SA#scav.wal_mod):write_hunk(
                           SA#scav.log, SA#scav.name, bigblob_longterm,
                           Key, ?LOGTYPE_BLOB, [Blob], []) of
                        {ok, SeqNum2, Offset2} ->
                            {{SeqNum2, Offset2}, size(Blob)};
                        _ ->
                            error
                    end;
               true ->
                    {{SeqNum, Offset}, size(Blob)}
            end
        end
    catch
        _X:_Y ->
            %% Be more helpful and add key to error msg.
            ?E_ERROR("copy_one_hunk: ~p: ~p ~p for key ~p seq ~p Offset ~p (~p)\n",
                     [SA#scav.name, _X, _Y, Key, SeqNum, Offset,
                      %% hack: this info isn't too helpful: erlang:get_stacktrace()]),
                      []]),
            error
    end.

scavenge_one_seq_file_fun(TempDir, SA, Fread_blob, Finfolog) ->
    fun({SeqNum, _Bytes}, {Hs, Bs, Es}) ->
            %%LEFT OFF HERE: we need to read the disk_log file instead
            DInPath = TempDir ++ "/" ++ integer_to_list(SeqNum),
            DOutPath = TempDir ++ "/" ++ integer_to_list(SeqNum) ++ ".out",
            {ok, DInLog} = disk_log:open([{name, DInPath},
                                          {file, DInPath}, {mode,read_only}]),
            {ok, DOutLog} = disk_log:open([{name, DOutPath}, {file, DOutPath}]),
            {ok, FH} = (SA#scav.wal_mod):open_log_file(SA#scav.log_dir, SeqNum,
                                                       [read, binary]),
            {Hunks, Bytes, Errs} =
                disk_log_fold(fun({Offset, BrickName, Key, TS},
                                  {Hs1, Bs1, Es1}) ->
                                      OldLoc = {SeqNum, Offset},
                                      case copy_one_hunk(SA, FH, Key, SeqNum,
                                                         Offset,
                                                         Fread_blob) of
                                          error ->
                                              {Hs1, Bs1, Es1 + 1};
                                          {NewLoc, Size} ->
                                              %% We want a tuple sortable first
                                              %% by brick name.
                                              Tpl = {BrickName, Key, TS,
                                                     OldLoc, NewLoc},
                                              ok = disk_log:log(DOutLog, Tpl),
                                              {Hs1 + 1, Bs1 + Size, Es1}
                                      end;
                                 ({live_bytes, _}, Acc) ->
                                      Acc
                              end, {0, 0, 0}, DInLog),
            file:close(FH),
            disk_log:close(DInLog),
            disk_log:close(DOutLog),
            ?E_INFO("SCAV: ~p middle of step 10\n", [SA#scav.name]),
            if Errs == 0, SA#scav.destructive == true ->
                    {ok, DOutLog} = disk_log:open([{name, DOutPath},
                                                   {file, DOutPath}]),
                    case (SA#scav.update_locations)(SA, DOutLog) of
                        NumUpdates when is_integer(NumUpdates) ->
                            Finfolog(
                              "scavenger: ~p sequence ~p: UpRes ok, "
                              "~p updates\n",
                              [SA#scav.name, SeqNum, NumUpdates]),
                            spawn(fun() ->
                                          timer:sleep(40*1000),
                                          delete_seq(SA, SeqNum)
                                  end);
                       UpRes ->
                            Finfolog(
                              "scavenger: ~p sequence ~p: UpRes ~p\n",
                              [SA#scav.name, SeqNum, UpRes])
                    end;
               true ->
                    ok
            end,
            disk_log:close(DOutLog),
            {Hs + Hunks, Bs + Bytes, Es + Errs}
    end.

make_info_log_fun(PropList) ->
    SilentP = proplists:get_value(silent, PropList, false),
    fun(Fmt, Args) ->
            if SilentP -> ok;
               true    -> ?E_INFO(Fmt, Args)
            end
    end.

%% @doc Once this function returns, the process will be registered
%% with ExclAtom.  If the process is already registered with that name
%% or any other name, it will wait forever.  If the process will live
%% beyond the reasonable scope of the exclusion, the process must
%% unregister the name or merely die to free the name.

really_cheap_exclusion(ExclAtom, Fwait, Fgo) ->
    gmt_loop:do_while(
      fun(Count) ->
              case (catch register(ExclAtom, self())) of
                  true ->
                      Fgo(),
                      {false, Count};
                  _ ->
                      if Count == 0 -> Fwait();
                         true       -> ok
                      end,
                      timer:sleep(1000),
                      {true, Count + 1}
              end
      end, 0).

get_bw_ticket(Bytes) ->
    brick_ticket:get(get(zzz_throttle_pid), Bytes).

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

file_output_fun(Log) ->
    fun(close) ->
            disk_log:close(Log);
       (Terms) ->
            ok = disk_log:log_terms(Log, Terms),
            file_output_fun(Log)
    end.

sort_test0() ->
    {ok, TmpLog} = disk_log:open([{name, foo}, {file, "/tmp/footest"}]),
    [disk_log:log(TmpLog, {xo, X, Y}) || X <- lists:seq(1, 50),
                                         Y <- lists:seq(1,100)],
    disk_log:close(TmpLog),
    {ok, InLog} = disk_log:open([{name, in}, {file, "/tmp/footest"}, {mode, read_only}]),
    {ok, OutLog} = disk_log:open([{name, out}, {file, "/tmp/footest.out"}]),
    X = file_sorter:sort(file_input_fun(InLog, start), file_output_fun(OutLog),
                         [{format, term},
                          {order, fun({_,_,A}, {_,_,B}) -> A < B end}]),
    disk_log:close(InLog),
    disk_log:close(OutLog),
    X.

%%
%% Brick callback functions
%%

%% @doc Flush all volatile data to stable storage <b>synchronously</b>.

bcb_force_flush(S) when is_record(S, state) ->
    catch (S#state.wal_mod):sync(S#state.log).

%% @doc Flush chain replication logging serial # stable storage asynchronously.

bcb_async_flush_log_serial(Serial, S) when is_record(S, state) ->
    S#state.sync_pid ! {sync_msg, Serial},
    S.

%% @doc Write Thisdo_Mods list to disk.

bcb_log_mods(Thisdo_Mods, Serial, S) when is_record(S, state) ->
    ?DBG_ETSx({bcb_log_mods_to_storage, S#state.name, Thisdo_Mods}),
    LocalMods = filter_mods_from_upstream(Thisdo_Mods, S),
    case log_mods2(LocalMods, undefined, S) of
        {goahead, S2} ->
            {goahead, S2#state{logging_op_serial = Serial + 1}, LocalMods};
        {wait, S2} ->
            {wait, S2#state{logging_op_serial = Serial + 1}, LocalMods}
    end.

%% @doc Convert Thisdo_Mods list to internal storage.

bcb_map_mods_to_storage(Thisdo_Mods, S) when is_record(S, state) ->
    ?DBG_ETSx({bcb_map_mods_to_storage, S#state.name, Thisdo_Mods}),
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
                    if S#state.bigdata_dir == undefined ->
                            Flags;
                       true ->
                            [ST] = my_lookup(S, Key, false),
                            case storetuple_val(ST) of
                                Bin when is_binary(Bin) ->
                                    [value_in_ram|Flags];
                                _ ->
                                    Flags
                            end
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
    ?DBG_TLOGx({qqq_bcb_repair_diff_round1, S#state.name, RepairList}),
    repair_diff_round1(RepairList, FirstKey, S).

%% @doc Perform an iteration of round 2 the diff repair loop

bcb_repair_diff_round2(RepairList, Deletes, S) when is_record(S, state) ->
    ?DBG_REPAIRx({qqq_bcb_repair_diff_round2, S#state.name, RepairList}),
    repair_diff_round2(RepairList, Deletes, S).

%% @doc Delete all remaining keys, starting with Key.

bcb_delete_remaining_keys(?BRICK__GET_MANY_FIRST, S)
  when is_record(S, state) ->
    delete_remaining_keys(ets:first(S#state.ctab), S);
bcb_delete_remaining_keys(Key, S) ->
    delete_remaining_keys(Key, S).

%% @doc Enable/disable read-only mode.

bcb_set_read_only_mode(ReadOnlyP, S) when is_record(S, state) ->
    ?DBG_GENx({?MODULE, bcb_set_read_only_mode, S#state.name, ReadOnlyP}),
    S#state{read_only_p = ReadOnlyP}.

%% @doc Get a brick-private metadata key/value pair.

bcb_get_metadata(Key, S) when is_record(S, state) ->
    ets:lookup(S#state.mdtab, Key).

%% @doc Set a brick-private metadata key/value pair.
%%
%% WARNING: This is a blocking, synchronous disk I/O operation.

bcb_set_metadata(Key, Val, S) when is_record(S, state) ->
    T = {Key, Val},
    ets:insert(S#state.mdtab, T),
    Thisdo_Mods = [{md_insert, T}],
    {_, NewS} = log_mods2(Thisdo_Mods, undefined, S),
    if S#state.do_sync ->
            catch (S#state.wal_mod):sync(S#state.log);
       true ->
            ok
    end,
    NewS.

%% @doc Delete a brick-private metadata key/value pair.
%%
%% WARNING: This is a blocking, synchronous disk I/O operation.

bcb_delete_metadata(Key, S) when is_record(S, state) ->
    ets:delete(S#state.mdtab, Key),
    Thisdo_Mods = [{md_delete, Key}],
    {_, NewS} = log_mods2(Thisdo_Mods, undefined, S),
    if S#state.do_sync ->
            catch (S#state.wal_mod):sync(S#state.log);
       true ->
            ok
    end,
    NewS.

%% @doc Callback to add thisdo_mods() to the implementation's dirty key table.

bcb_add_mods_to_dirty_tab(Thisdo_Mods, S) when is_record(S, state) ->
    add_mods_to_dirty_tab(Thisdo_Mods, S),
    %%?E_WARNING("EEE: ~w new dirty = ~p\n", [S#state.name, ets:tab2list(S#state.dirty_tab)]),
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
    ?DBG_GENx({bcb_dirty_keys_in_range, S#state.name, startk, StartKey,
               endk, EndKey, dirty, DirtyList}),
    L = if EndKey == ?BRICK__GET_MANY_LAST ->
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
            if S#state.bigdata_dir /= undefined, is_binary(RawVal) ->
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

%% @spec (state_r()) -> integer()
%% @doc Return the current length the ImplState's logging op queue

bcb_peek_logging_op_q_len(S) when is_record(S, state) ->
    queue:len(S#state.logging_op_q).

%% @spec (state_r()) -> integer()
%% @doc Return the current value the ImplState's logging op queue in list form.

bcb_get_logging_op_q(S) when is_record(S, state) ->
    queue:to_list(S#state.logging_op_q).

%% @spec (state_r()) -> proplist()
%% @doc Return the ImplState's status in proplist form (the same proplist
%% that is returned by the brick_server:status() call).

bcb_status(S) when is_record(S, state) ->
    {ok, Ps} = do_status(S),
    Ps.

%% @spec (integer(), state_r()) -> integer()
%% @doc Purge all records involving log sequence number SeqNum.

bcb_common_log_sequence_file_is_bad(SeqNum, S) when is_record(S, state) ->
    purge_recs_by_seqnum(SeqNum, false, S).


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

debug_scan(Dir) ->
    debug_scan(Dir, gmt_hlog).

debug_scan(Dir, WalMod) ->
    WalMod:fold(
      shortterm, Dir,
      fun(H, FH, Acc) ->
              io:format("Hunk: ~p ~p\n", [H#hunk_summ.seq, H#hunk_summ.off]),
              if H#hunk_summ.type == ?LOGTYPE_METADATA ->
                      CB = WalMod:read_hunk_member_ll(FH, H, md5, 1),
                      io:format("~P\n\n", [binary_to_term(CB), 40]);
                 H#hunk_summ.type == ?LOGTYPE_BLOB ->
                      io:format("blob len: ~p\n\n", [hd(H#hunk_summ.c_len)]);
                 true ->
                      io:format("Unknown hunk type: ~p\n~p\n\n", [H#hunk_summ.type, H])
              end,
              Acc + 1
      end, 0).

debug_scan2(Dir) ->
    debug_scan2(Dir, gmt_hlog).

debug_scan2(Dir, WalMod) ->
    WalMod:fold(
      shortterm, Dir,
      fun(H, FH, {{Is,Ds,DAs}=Foo, MDs, Bls, Os}) ->
              if H#hunk_summ.type == ?LOGTYPE_METADATA ->
                      CB = WalMod:read_hunk_member_ll(FH, H, md5, 1),
                      Mods = binary_to_term(CB),
                      {I, D, DA} =
                          lists:foldl(
                            fun(T, {Ix, Dx, DAx}) when element(1, T) == insert ->
                                    {Ix+1, Dx, DAx};
                               (T, {Ix, Dx, DAx}) when element(1, T) == delete orelse element(1, T) == delete_noexptime ->
                                    {Ix, Dx+1, DAx};
                               (T, {Ix, Dx, DAx}) when element(1, T) == delete_all_table_items ->
                                    {Ix, Dx, DAx+1};
                               (_, Acc) ->
                                    Acc
                            end, {0, 0, 0}, Mods),
                      {{Is+I, Ds+D, DAs+DA}, MDs+1, Bls, Os};
                 H#hunk_summ.type == ?LOGTYPE_BLOB ->
                      CB1 = WalMod:read_hunk_member_ll(FH, H, md5, 1),
                      H2 = H#hunk_summ{c_blobs = [CB1]},
                      true = gmt_hlog:md5_checksum_ok_p(H2),
                      {Foo, MDs, Bls+1, Os};
                 true ->
                      CB2 = WalMod:read_hunk_member_ll(FH, H, md5, 1),
                      H2 = H#hunk_summ{c_blobs = [CB2]},
                      true = gmt_hlog:md5_checksum_ok_p(H2),
                      {Foo, MDs, Bls, Os+1}
              end
      end, {{0,0,0}, 0, 0, 0}).

debug_scan3(Dir, EtsTab) ->
    debug_scan3(Dir, EtsTab, gmt_hlog).

debug_scan3(Dir, EtsTab, WalMod) ->
    WalMod:fold(
      shortterm, Dir,
      fun(H, FH, _Acc) ->
              if H#hunk_summ.type == ?LOGTYPE_METADATA ->
                      CB = WalMod:read_hunk_member_ll(FH, H, md5, 1),
                      Mods = binary_to_term(CB),
                      lists:foreach(
                            fun(T) when element(1, T) == insert ->
                                    Key = element(1, element(2, T)),
                                    ets:insert(EtsTab, {Key, x});
                               (T) when element(1, T) == delete orelse element(1, T) == delete_noexptime ->
                                    Key = element(1, element(2, T)),
                                    ets:delete(EtsTab, Key);
                               (T) when element(1, T) == delete_all_table_items ->
                                    ets:delete_all_objects(EtsTab);
                               (_) ->
                                    blah
                            end, Mods),
                      blah;
                 H#hunk_summ.type == ?LOGTYPE_BLOB ->
                      blah;
                 true ->
                      blah
              end
      end, unused).

debug_scan4(Dir, OutFile) ->
    debug_scan4(Dir, OutFile, gmt_hlog).

debug_scan4(Dir, OutFile, WalMod) ->
    {ok, OutFH} = file:open(OutFile, [write, delayed_write]),
X = WalMod:fold(
      shortterm, Dir,
      fun(H, FH, _Acc) ->
              if H#hunk_summ.type == ?LOGTYPE_METADATA ->
                      CB = WalMod:read_hunk_member_ll(FH, H, md5, 1),
                      Mods = binary_to_term(CB),
                      lists:foreach(
                            fun(T) when element(1, T) == insert orelse element(1, T) == insert_value_into_ram ->
                                    Key = element(1, element(2, T)),
                                    io:format(OutFH, "i ~p\n", [Key]);
                               (T) when element(1, T) == delete orelse element(1, T) == delete_noexptime ->
                                    Key = element(2, T),
                                    io:format(OutFH, "d ~p\n", [Key]);
                               (T) when element(1, T) == delete_all_table_items ->
                                    io:format(OutFH, "delete_all_table_items \n", []);
                               (T) ->
                                    io:format(OutFH, "? ~p\n", [T])
                            end, Mods),
                      blah;
                 H#hunk_summ.type == ?LOGTYPE_BLOB ->
                      blah;
                 true ->
                      blah
              end
      end, unused),
    file:close(OutFH),
    X.
