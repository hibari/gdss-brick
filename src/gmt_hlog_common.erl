%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2013 Hibari developers.  All rights reserved.
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
%%% File    : gmt_hlog_common.erl
%%% Purpose : Common Hunk log server.
%%%-------------------------------------------------------------------

%% IMPORTANT: Be careful about gen_server names:
%% - ?GMT_HLOG_COMMON_LOG_NAME -> the name of the 'gmt_hlog_common' gen_server
%% - gmt_hlog:log_name2reg_name(?GMT_HLOG_COMMON_LOG_NAME) returns ->
%%   the name for the 'gmt_hlog' gen_server
%%
%% gmt_hlog_common's #state.hlog_pid holds the pid of 'gmt_hlog' gen_server.

-module(gmt_hlog_common).

-behaviour(gen_server).

-include("brick.hrl").
-include("brick_public.hrl").
-include("gmt_hlog.hrl").
-include_lib("kernel/include/file.hrl").

-define(WB_BATCH_SIZE, 500). % For write_back buffering.
-define(WAIT_BEFORE_EXIT, timer:sleep(1500)). %% milliseconds

%% API
-export([start_link/1,
         hlog_pid/1,
         stop/1,
         register_local_brick/2,
         %% unregister_local_brick/2,
         %% permanently_unregister_local_brick/2,
         full_writeback/0,
         full_writeback/1,
         get_all_registrations/0,
         get_all_registrations/1
        ]).

%% Tempolary export for brick_hlog_scavenger
-export([read_flush_file/1]).

%% Checksum error API
-export([sequence_file_is_bad/2]).

%% Use start/1 only if you know what you're doing; otherwise use start_link/1.
-export([start/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% Pass-through to gmt_hlog.
-export([log_file_path/2,
         log_file_path/3,
         log_file_info/2,
         open_log_file/3,
         read_hunk_summary/5,
         write_hunk/7
        ]).

%% Tool
-export([tool_list_md/2]).

%%%----------------------------------------------------------------------
%%% Types/Specs/Records
%%%----------------------------------------------------------------------

-type do_mod() :: brick_ets:do_mod().
-type store_tuple() :: brick_ets:store_tuple().
-type write_batch() :: leveldb:batch_write().

-type from() :: {pid(), term()}.

%% -type orddict() :: list().

-type props() :: list({common_log_name, servername()}).

-record(state, {
          name                            :: file:name(),
          hlog_name                       :: file:name(),
          hlog_pid                        :: pid(),                    %% Pid of the gmt_hlog
          hlog_dir                        :: dirname(),
          last_seqnum                     :: seqnum(),                 %% SeqNum of last writeback
          last_offset                     :: offset(),                 %% Offset of last writeback
          %% brick_registory=orddict:new()   :: orddict(),                %% Registered local bricks
          tref                            :: timer:tref(),
          scavenger_server_pid            :: pid(),
          async_writeback_pid             :: pid() | undefined,
          async_writeback_reqs=[]         :: [from()],                 %% requesters of current async writeback
          async_writebacks_next_round=[]  :: [from()],                 %% requesters of next async writeback
          dirty_buffer_wait               :: non_neg_integer(),        %% seconds
          short_long_same_dev_p           :: boolean(),
          first_writeback                 :: boolean()
         }).
%% -type state()   :: #state{}.
-type state_readonly() :: #state{}.  %% Read-only

%% write-back info
-record(wb, {
          exactly_count = 0   :: non_neg_integer(),  % number of metadata tuples to write-back
          exactly_ts = []     :: [metadata_tuple()]  % metadata tuples to write-back
         }).
-type wb_r() :: #wb{}.

-define(BRICK_HLOG_SCAVENGER, brick_hlog_scavenger).


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(props()) -> {ok,pid()} | {error,term()} | ignore.
start_link(PropList) ->
    gen_server:start_link(?MODULE, PropList, []).

-spec start(props()) -> {ok,pid()} | {error,term()} | ignore.
start(PropList) ->
    gen_server:start(?MODULE, PropList, []).

-spec hlog_pid(server()) -> pid().
hlog_pid(Server) ->
    gen_server:call(Server, {hlog_pid}, 300*1000).

-spec stop(server()) -> ok | {error,term()}.
stop(Server) ->
    gen_server:call(Server, {stop}).

-spec register_local_brick(server(), brickname()) -> ok | {error,term()}.
register_local_brick(Server, LocalBrick) when is_atom(LocalBrick) ->
    gen_server:call(Server, {register_local_brick, LocalBrick}).

%% -spec unregister_local_brick(server(), brickname()) -> ok.
%% unregister_local_brick(Server, LocalBrick) when is_atom(LocalBrick) ->
%%     gen_server:call(Server, {unregister_local_brick, LocalBrick}).

%% -spec permanently_unregister_local_brick(server(), brickname()) -> ok.
%% permanently_unregister_local_brick(Server, LocalBrick) when is_atom(LocalBrick) ->
%%     gen_server:call(Server, {permanently_unregister_local_brick, LocalBrick}).

full_writeback() ->
    full_writeback(?GMT_HLOG_COMMON_LOG_NAME).

-spec full_writeback(server()) -> ok | {error,term()}.
full_writeback(Server) ->
    gen_server:call(Server, {full_writeback}, 300*1000).

-spec get_all_registrations() -> list(atom()).
get_all_registrations() ->
    get_all_registrations(?GMT_HLOG_COMMON_LOG_NAME).

-spec get_all_registrations(server()) -> list(atom()).
get_all_registrations(Server) ->
    gen_server:call(Server, {get_all_registrations}, 300*1000).

-spec log_file_path(dirname(), seqnum()) -> dirname().
log_file_path(Dir, SeqNum) ->
    gmt_hlog:log_file_path(Dir, SeqNum).

log_file_path(Dir, SeqNum, Suffix) ->
    gmt_hlog:log_file_path(Dir, SeqNum, Suffix).

-spec open_log_file(dirname(), seqnum(), openmode()) -> {ok, file:fd()} | {error, atom()}.
open_log_file(Dir, SeqNum, Mode) ->
    gmt_hlog:open_log_file(Dir, SeqNum, Mode).

-spec log_file_info(dirname(), seqnum()) -> {ok, filepath(), file:file_info()} | {error, atom()}.
log_file_info(LogDir, SeqNum) ->
    gmt_hlog:log_file_info(LogDir, SeqNum).

read_hunk_summary(A, B, C, D, E) ->
    gmt_hlog:read_hunk_summary(A, B, C, D, E).

-spec write_hunk(server(), brickname(), hlogtype(), key(), typenum(),
                 CBlobs::blobs(), UBlobs::blobs()) ->
                        {ok, seqnum(), offset()} | {hunk_too_big, len()} | no_return().
write_hunk(A, B, C, D, E, F, G) ->
    gmt_hlog:write_hunk(A, B, C, D, E, F, G).

-spec sequence_file_is_bad(seqnum(), offset()) -> ok.
sequence_file_is_bad(SeqNum, Offset) ->
    gen_server:call(?GMT_HLOG_COMMON_LOG_NAME,
                    {sequence_file_is_bad, SeqNum, Offset}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initiates the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(PropList) ->
    %% The registered name of the gmt_hlog_common gen_server.
    CName = proplists:get_value(common_log_name, PropList, commonLogDefault),
    undefined = proplists:get_value(dir, PropList), % sanity: do not use
    try
        case whereis(CName) of
            undefined ->
                register(CName, self()),
                process_flag(priority, high),
                process_flag(trap_exit, true),

                %% The registered name of the gmt_hlog gen_server.
                HLogName = gmt_hlog:log_name2reg_name(CName),
                (catch exit(whereis(HLogName), kill)),
                timer:sleep(150),
                %% @TODO: ENHANCEME: Very confusing. The *internal name* of
                %% the gmt_hlog gen_server is the *registered name* of
                %% the gmt_hlog_common gen_server!
                {ok, Log} = gmt_hlog:start_link([{name, CName}|PropList]),

                LogDir = gmt_hlog:log_name2data_dir(CName),
                {SeqNum, Off} = read_flush_file(LogDir),
                self() ! do_sync_writeback,
                %% Use timer instead of brick_itimer: it allows QC testing
                %% without GDSS app running, and there's only one of
                %% these things per app.
                WriteBackInterval = 30000,  %% 30 secs.  @TODO: Configurable
                {ok, TRef} = timer:send_interval(WriteBackInterval, do_async_writeback),

                %% Scavenger Server
                SupressScavenger = prop_or_application_env_bool(
                                     brick_scavenger_suppress,
                                     suppress_scavenger, PropList,
                                     false),

                case SupressScavenger of
                    true ->
                        ScavengerServer = undefined;
                    false ->
                        {ok, ScavengerServer} = ?BRICK_HLOG_SCAVENGER:start_link(PropList)
                end,

                {ok, DirtySec} = application:get_env(gdss_brick, brick_dirty_buffer_wait),
                SameDevP = short_long_same_dev_p(LogDir),

                {ok, #state{name=CName,
                            hlog_pid=Log,
                            hlog_name=CName,
                            hlog_dir=LogDir,
                            last_seqnum=SeqNum,
                            last_offset=Off,
                            tref=TRef,
                            scavenger_server_pid=ScavengerServer,
                            dirty_buffer_wait=DirtySec,
                            short_long_same_dev_p=SameDevP,
                            first_writeback=true
                           }};
            _Pid ->
                ignore
        end
    catch
        _X:_Y ->
            ?E_ERROR("init error: ~p ~p at ~p", [_X, _Y, erlang:get_stacktrace()]),
            ignore
    end.

prop_or_application_env_bool(ConfName, PropName, PropList, Default) ->
    gmt_util:boolean_ify(prop_or_application_env(ConfName, PropName, PropList, Default)).

prop_or_application_env(ConfName, PropName, PropList, Default) ->
    case proplists:get_value(PropName, PropList, not_in_list) of
        not_in_list ->
            case application:get_env(gdss_brick, ConfName) of
                undefined ->
                    Default;
                {ok, Res} ->
                    Res
            end;
        Prop ->
            Prop
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({hlog_pid}, _From, State) ->
    {reply, State#state.hlog_pid, State};
handle_call({register_local_brick, Brick}, From, State) ->
    case do_register_local_brick(Brick, State) of
        %% This is a hack.  We're assuming that the amount of time
        %% required for a full writeback of everyone is pretty short.
        %% So this will writeback everything to Brick as a
        %% side-effect, then Brick can do its WAL fold in peace &
        %% harmony.
        {ok, NewState} ->
            {noreply, schedule_async_writeback(From, NewState)}
    end;
%% handle_call({unregister_local_brick, Brick}, _From, State) ->
%%     {Reply, NewState} = do_unregister_local_brick(Brick, State),
%%     {reply, Reply, NewState};
%% handle_call({permanently_unregister_local_brick, Brick}, _From, State) ->
%%     {Reply, NewState} = do_permanently_unregister_local_brick(Brick, State),
%%     {reply, Reply, NewState};
handle_call({full_writeback}, From, State) ->
    NewState = schedule_async_writeback(From, State),
    {noreply, NewState};
handle_call({get_all_registrations}, _From, State) ->
    {reply, do_get_all_registrations(State), State};
handle_call({sequence_file_is_bad, SeqNum, Offset}, _From, State) ->
    NewState = do_sequence_file_is_bad(SeqNum, Offset, State),
    {reply, ok, NewState};
handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};
handle_call(_X, _From, State) ->
    {reply, {go_away_you_hooligan, _X}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(do_sync_writeback, State) ->
    %%io:format("TOP: info: do_sync_writeback\n"),
    NewState = do_sync_writeback(State),
    {noreply, NewState#state{first_writeback = false}};
handle_info(do_async_writeback, #state{async_writeback_pid = undefined,
                                       async_writeback_reqs = [],
                                       async_writebacks_next_round = NextReqs} = State) ->
    ParentPid = self(),
    %% Pattern-matching should assure that do_sync_writeback() has
    %% encountered no errors if it returns. A pattern match failure
    %% will kill the child, so it can't update our parent.
    {Pid, _Ref} = spawn_monitor(fun() ->
                                        S = do_sync_writeback(State),
                                        ParentPid ! {async_writeback_finished,
                                                     self(),
                                                     S#state.last_seqnum,
                                                     S#state.last_offset},
                                        exit(normal)
                                end),
    {noreply, State#state{async_writeback_pid=Pid,
                          async_writeback_reqs=NextReqs,
                          async_writebacks_next_round = []}};
handle_info(do_async_writeback, #state{async_writeback_pid=Pid,
                                       async_writeback_reqs=Reqs}=State)
  when is_pid(Pid) ->
    if Reqs =:= [] ->
            ?E_WARNING("async writeback proc ~p hasn't finished yet", [Pid]);
       true ->
            %% don't warn if in progress due to an external request
            noop
    end,
    {noreply, State};
handle_info({async_writeback_finished, Pid, NewSeqNum, NewOffset},
            #state{async_writeback_pid=Pid,
                   async_writeback_reqs=Reqs,
                   last_seqnum=SeqNum,
                   last_offset=Offset}=State) ->
    %% update state
    NewState = if NewSeqNum > SeqNum
                  orelse (NewSeqNum =:= SeqNum andalso NewOffset > Offset) ->
                       State#state{last_seqnum=NewSeqNum, last_offset=NewOffset};
                  NewSeqNum =:= SeqNum andalso NewOffset =:= Offset ->
                       State;
                  true ->
                       ?E_NOTICE("async_writeback_finished - Illegal position: "
                                 "last seq/off ~p ~p, new seq/off ~p ~p",
                               [SeqNum, Offset, NewSeqNum, NewOffset]),
                       State
               end,
    %% reply to callers
    _ = [ gen_server:reply(From, ok) || From <- Reqs ],
    {noreply, NewState#state{async_writeback_reqs = []}};
handle_info({'DOWN', _Ref, _, Pid, Reason},
            #state{async_writeback_pid=Pid, async_writeback_reqs=Reqs}=State) ->
    %% schedule next round
    _ = schedule_async_writeback(State),
    %% if any, reply to callers with error
    _ = [ gen_server:reply(From, {error, Reason}) || From <- Reqs ],
    {noreply, State#state{async_writeback_pid=undefined, async_writeback_reqs=[]}};
%% handle_info({'DOWN', Ref, _, _, _}, #state{brick_registory=Dict}=State) ->
%%     NewDict = orddict:filter(fun(_K, V) when V =/= Ref -> true;
%%                                 (_, _)                -> false
%%                              end, Dict),
%%     {noreply, State#state{brick_registory=NewDict}};
handle_info({'DOWN', _Ref, _, _, _}, State) ->
    {noreply, State};
handle_info({'EXIT', Pid, Reason}, #state{hlog_pid = Pid} = State) ->
    {stop,Reason,State#state{hlog_pid=undefined}};
handle_info(_Info, State) ->
    ?E_ERROR("~p got msg ~p", [self(), _Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{hlog_pid=HLogPid,
                          tref=TRef,
                          scavenger_server_pid=ScavengerServer}) ->
    if ScavengerServer =/= undefined ->
            ?BRICK_HLOG_SCAVENGER:stop(ScavengerServer);
       true ->
            noop
    end,
    timer:cancel(TRef),
    if HLogPid =/= undefined ->
            gmt_hlog:stop(HLogPid);
       true ->
            noop
    end,
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

flush_path(S_or_Str) ->
    flush_log_dir(S_or_Str) ++ "/flush".

flush_tmpsuffix() ->
    gmt_util:list_ify(brick_server:make_timestamp()). % Lazy

flush_log_dir(S) when is_record(S, state) ->
    S#state.hlog_dir;
flush_log_dir(Str) when is_list(Str) ->
    Str.

read_flush_file(S_or_Str) ->
    try
        {ok, [Tuple]} = file:consult(flush_path(S_or_Str)),
        Tuple
    catch _X:_Y ->
            {0, 0}
    end.

write_flush_file(SeqNum, Offset, S) ->
    RealPath = flush_path(S),
    Suffix = flush_tmpsuffix(),
    Fun = fun(FH) -> ok = io:format(FH, "~w.\n", [{SeqNum, Offset}]) end,
    ok = brick_server:replace_file_sync(RealPath, Suffix, Fun).

schedule_async_writeback(#state{async_writebacks_next_round=NextReqs}=S) ->
    if NextReqs =/= [] ->
            self() ! do_async_writeback;
       true ->
            noop
    end,
    S.

schedule_async_writeback(From, #state{async_writebacks_next_round=NextReqs}=S) ->
    if NextReqs =:= [] ->
            self() ! do_async_writeback;
       true ->
            noop
    end,
    S#state{async_writebacks_next_round=[From|NextReqs]}.

do_sync_writeback(S) ->
    %% Oi oi oi, this was a pernicious bug.  !@#$!
    %%
    %% Symptom: Intermittent chunks of NUL bytes in various local log
    %%          shortterm files, approx 100-a few Kbyte large.  It seems as
    %%          if small numbers of contiguous chunks aren't copied from
    %%          CommonLog shortterm to local shortterm.
    %% Find via: hlog_local_qc.erl:nul_check().
    %%
    %% If GDSS were shut down less than 60 seconds after the NULs
    %% appear, then remove all local shortterm files, remove the
    %% hlog.commonLogServer/flush file, then restart GDSS, *then* run
    %% nul_check(), the errors would disappear.  Which means that all
    %% the hunks were written to CommonLog shortterm correctly, but
    %% somehow this function wasn't reading 100% of them.
    %%
    %% There's a nasty, evil race between the hunk buffering that we
    %% do and the updating of the #state.last_offset value?  Because
    %% if we ask for CommonLog's physical file position at this
    %% moment, the chunks of NUL bytes never appear.

    {EndSeqNum, EndOffset} =
        gmt_hlog:get_current_seqnum_and_file_position(S#state.hlog_pid),
    %% ?DBG_TLOG("do_sync_writeback: end_seq ~w, end_offset ~w", [EndSeqNum, EndOffset]),
    {ok, Count1} = do_metadata_hunk_writeback(S#state.last_seqnum,
                                              S#state.last_offset,
                                              EndSeqNum, EndOffset, S),
    {ok, Bytes2} = do_bigblob_hunk_writeback(S#state.last_seqnum,
                                             S#state.last_offset, S),
    %% ?DBG_TLOG("do_sync_writeback: counts ~w, ~w", [Count1, Bytes2]),

    %% OK, we're written everything back to where they need to be.
    %% However, all of those writes were asynchronous and probably
    %% have not reached stable storage.  Therefore, we wait for the OS
    %% to flush it sometime between now and S#state.dirty_buffer_wait
    %% seconds from now.  We'll cheat by simply recording this
    %% particular writeback that many seconds in the future.
    if Count1 + Bytes2 > 0 ->
            _ = spawn_future_tasks_after_dirty_buffer_wait(EndSeqNum, EndOffset, S),
            ok;
       true ->
            ok
    end,
    S#state{last_seqnum = EndSeqNum, last_offset = EndOffset}.

do_metadata_hunk_writeback(OldSeqNum, OldOffset, StopSeqNum, StopOffset, S)->
    FiltFun = fun(N) ->
                      N >= OldSeqNum
              end,
    Fun = fun(#hunk_summ{seq=SeqNum, off=Offset}=_H, _FH, _WB)
                when SeqNum =:= OldSeqNum, Offset < OldOffset ->
                  %% It would be really cool if we could advance the
                  %% file pos of FH, but our caller is keeping track
                  %% of its own file offsets, so calling
                  %% file:position/2 on this file handle doesn't do
                  %% anything useful, alas. Instead, we'll use the
                  %% magic return value to tell fold_a_file() to use a
                  %% new offset for the next iteration.
                  {{{new_offset, OldOffset}}};
             (#hunk_summ{seq=SeqNum, off=Offset}=_H, _FH, WB)
                when {SeqNum, Offset} >= {StopSeqNum, StopOffset} ->
                  %% Do nothing here: our folding has moved past where
                  %% we need to process ... this hunk will be
                  %% processed later.
                  WB;
             (#hunk_summ{type=?LOCAL_RECORD_TYPENUM, u_len=[BLen]}=H, FH,
              #wb{exactly_count=Count, exactly_ts=Ts}=WB) ->
                  UBlob = gmt_hlog:read_hunk_member_ll(FH, H, undefined, 1),
                  if size(UBlob) =/= BLen ->
                          %% This should never happen.
                          QQQPath = "/tmp/foo.QQQbummer." ++ integer_to_list(element(3, now())),
                          DebugInfo = [{args, [OldSeqNum, OldOffset, StopSeqNum, StopOffset, S]},
                                       {h, H}, {wb, WB}, {info, process_info(self())}],
                          ok = file:write_file(QQQPath, term_to_binary(DebugInfo)),
                          ?E_CRITICAL("Error during write back: ~p ~p wanted blob size ~p but got ~p",
                                      [H#hunk_summ.seq, H#hunk_summ.off, BLen, size(UBlob)]);
                     true ->
                          ok
                  end,
                  T = binary_to_term(UBlob),
                  %% This tuple is sortable the way we need it, as-is.
                  WB#wb{exactly_count= Count + 1, exactly_ts= [T|Ts]};
             (_H, _FH, WB) ->
                  %% These are copied by do_bigblob_hunk_writeback() instead.
                  WB
          end,

    case gmt_hlog:fold(shortterm, S#state.hlog_dir, Fun, FiltFun, #wb{}) of
        {#wb{exactly_count=0}, []} ->
            {ok, 0};

        {#wb{exactly_count=Count}=WB, []} ->
            ?E_DBG("~w metadata hunks to write back", [Count]),
            Start = os:timestamp(),
            ok = write_back_to_stable_storege(WB, S),
            Elapse = timer:now_diff(os:timestamp(), Start) div 1000,
            ?E_INFO("Wrote back ~w metadata hunks to stable storage. [~w ms]",
                    [Count, Elapse]),
            {ok, Count};

        {#wb{exactly_count=Count}, ErrList} ->
            %% The fold that we just finished is: a). data that's
            %% likely less than 1 second old, and b). data that has
            %% some sanity checking that will be checked later:
            %%
            %%   1. A value blob that remains in the CommonLog.  In
            %%   this case, an attempt to fetch the blob will cause a
            %%   failure that can be repaired later.
            %%
            %%   2. A metadata blob that requires copying to the owner
            %%   brick's private log.  In the event that a checkpoint
            %%   happens later, then we'll never have to read the
            %%   missing data, and life is good.  If a brick's init +
            %%   WAL scan hits the missing data, it can crash to cause
            %%   a failure that can be repaired later.
            ?ELOG_WARNING("ErrList = ~p", [ErrList]),
            ErrStr = lists:flatten(io_lib:format("~P", [ErrList, 25])),
            gmt_util:set_alarm({?MODULE, ErrStr},
                               "Potential data-corrupting error occurred, "
                               "check all log files for details"),
            if S#state.first_writeback =:= false ->
                    ?WAIT_BEFORE_EXIT,
                    exit({error_list, S#state.hlog_dir, ErrList});
               true ->
                    %% If we exit() here when we're doing our initial
                    %% writeback after init time, then we can never
                    %% start. It's better to start than to never be
                    %% able to start.
                    %% {ok, WB2#wb.exactly_count}
                    {ok, Count}
            end
    end.

-spec write_back_to_stable_storege(wb_r(), state_readonly()) -> ok | {error, [term()]}.
write_back_to_stable_storege(#wb{exactly_ts=MetadataTuples}, State) ->
    %% Sort metadata tuples by brickname, seqnum, and offset.
    GroupedByBrick = group_store_tuples_by_brick(MetadataTuples),
    Errors0 = [write_back_to_stable_storege1(BrickName, Tuples, State, [])
               || {BrickName, Tuples} <- GroupedByBrick ],
    case lists:filter(fun(Error) -> Error =/= [] end, Errors0) of
        [] ->
            ok;
        Errors1 ->
            {error, Errors1}
    end.

%% @doc Returns a list of {brickname(), [metadata_tuple()]}. Each store tuples
%% are sorted by seqnum() and offset().
-spec group_store_tuples_by_brick([metadata_tuple()]) -> [{brickname(), [metadata_tuple()]}].
group_store_tuples_by_brick(MetaDataTuples) ->
    GroupedByBrick = lists:foldl(fun({eee, BrickName, _SeqNum, _Offset, _Key, _TypeNum,
                                      _HunkLen, _HunkBytes}=Tuple, Dict) ->
                                         case Dict:find(BrickName) of
                                             {ok, Tuples} ->
                                                 Dict:store(BrickName, [Tuple|Tuples]);
                                             error ->
                                                 Dict:store(BrickName, [Tuple])
                                         end
                                 end, dict:new(), MetaDataTuples),
    [ {BrickName, lists:sort(Tuples)} || {BrickName, Tuples} <- dict:to_list(GroupedByBrick) ].





%% -- WIP Start ----------------------------------------------------

%% @TODO gdss-brick >> GH17
%%       "Redesign disk storage and checkpoint/scavenger processes"
%%
%%  1. Add timestamp to the do_mods for 'delete' and 'delete_noexptime',
%%     so that it can store delete-markers. - *DONE*
%%  2. Add oldTimestamp to the do_mods for 'insert_existing_value'.
%%     - *DONE*
%%  3. Finish implementing the metabata write-back process (including
%%     LevelDB disk sync). Test with chain length = 3. - *DONE*
%%  4. Update the scavenger logic to support frozen log.
%%  5. Rewrite brick_ets:wal_scan_all/2 to utilize the metadata DB.
%%     - *DONE* except test cases
%%  6. Remove the brick local log and checkpoint process.
%%  7. Revise WAL format.
%%  8. Make write-back process more intelligent; every N hunks have
%%     been written to the common log or N secs interval.
%%  9. Implement per brick value blob store.
%% 10. Implement the new scavenger logic.
%% 11. Run lengthy tests (and develop some tools) to ensure that the
%%     metadata DB is working correctly.
%%

%% New Scavenger Logic
%%
%%    (SCV: Scavenger, WBP: Write-Back Process)
%%  1. SCV: Ensure no housekeeping process is running.
%%  2. SCV: List log sequence numbers eligible for scavenging.
%%  3. WBP: Start to record do_mods with insert_existing_value (for
%%          the sequence numbers) to separate log file(s). - (A)
%%  4. SCV: Dump store tuples and sort them by log-seq and position.
%%  5. SCV: Move live hunks (up-to the batch size and bandwidth limit)
%%          to new log-seqs and update their locations in brick servers.
%%          Repeat this step until all live hunks are moved.
%%          NOTE: Change Scavenger so that each brick server has its
%%          own blob sequence.
%%  6. WBP: Record the current timestamp. - (B)
%%  7. WBP: When it sees a do_mod having a newer timestamp than (B),
%%         stop recording on (A).
%%  8. SCV: Scan (A) and update their locations in brick servers.
%%  9. WBP: Delete (A).
%% 10. SCV: Delete the freed log sequences.
%%
%% NOTE: The repair process should restart
%%

%% New Housekeeping Logic (In a future release)
%%
%% Periodically do the followings.
%% (every N inserts/deletes or every N minutes.)
%%
%%    (HKP: House Keeping Process)
%%  1. HKP: Ensure no scavenger process is running.
%%  2. HKP: Start to run a full key-scan in a metadata DB (with a
%%          bandwidth limit).
%%  3. HKP: If it finds duplicate keys with different timestamps, or
%%          a delete-marker, update the live hunk statistics table
%%          (which is a LevelDB table) for free hunk/bytes.
%%  4. HKP: Delete the older keys. (In a future version, keep this
%%          log de-allocation info in somewhere?)
%%

%% NOTE: a rename op does two ops in this order:
%%
%% (1) delete OldKey
%% (2) insert_existing_value for NewKey
%%
%% 11:49:15.729 ... delete - key: <<49,50,47,0,0,2,68>>, ts: 1388720955419232
%% 11:49:15.730 ... insert_existing_value - key: <<49,50,47,0,0,1,218>>, ts: 1388720955419214,
%%                                          oldkey: <<49,50,47,0,0,2,68>>
%%

%% @TODO: To limit the amount of RAM required for a write-back process,
%%        try not to read all hunks for the entire write-back. Maybe the
%%        wrapper hunk needs to be changed.
%%        e.g. first UBlob has blick name, location and size;
%%             second UBlob has actual metadata hunk.
%% @TODO: Write as many as metadata records in one batch. (track size and numbers for MDs)
%% @TODO: Change to foldl rather than tail recursion.
%% @TODO: Try not to expose the #state record to this fun.
-spec write_back_to_stable_storege1(brickname(), [metadata_tuple()],
                                    state_readonly(), [term()]) -> [term()].
write_back_to_stable_storege1(_BrickName, [], _State, Errors) ->
    lists:reverse(Errors);
write_back_to_stable_storege1(BrickName,
                              [{eee, _BrickName, _SeqNum, _Offset, _Key, _TypeNum,
                                _H_Len, [Summary, CBlobs, _UBlobs]} | RemainingMetaDataTuples],
                              #state{hlog_pid=HLog}=State,
                              Errors) ->
    case gmt_hlog:parse_hunk_summary(Summary) of
        #hunk_summ{c_len=CLen, u_len=ULen}=ParsedSummary ->
            [_] = CLen,        % sanity
            []  = ULen,        % sanity
            MetadataBin = gmt_hlog:get_hunk_cblob_member(ParsedSummary, CBlobs, 1),
            case gmt_hlog:md5_checksum_ok_p(ParsedSummary#hunk_summ{c_blobs=[MetadataBin]}) of
                true ->
                    MetadataDB = gmt_hlog:get_metadata_db(HLog, BrickName),
                    DoMods = binary_to_term(MetadataBin),
                    IsLastBatch = RemainingMetaDataTuples =:= [],
                    ok = write_back_to_metadata_db(MetadataDB, BrickName, DoMods, IsLastBatch);
                false ->
                    %% @TODO: Do not crash, and do sync
                    error({invalid_checksum, {_BrickName, _SeqNum, _Offset, _Key}})
            end;
        too_big ->
            %% @TODO: Do not crash, and do sync
            error({hunk_is_too_big_to_parse, {_BrickName, _SeqNum, _Offset, _Key}})
    end,
    write_back_to_stable_storege1(BrickName, RemainingMetaDataTuples, State, Errors).

-spec write_back_to_metadata_db(leveldb:db(), brickname(), [do_mod()], boolean()) -> ok.
write_back_to_metadata_db(MetadataDB, _BrickName, DoMods, IsLastBatch) ->
    Batch = lists:foldl(fun add_metadata_db_op/2, leveldb:new_write_batch(), DoMods),
    IsEmptyBatch = leveldb:is_empty_batch(Batch),
    case {IsLastBatch, IsEmptyBatch} of
        {true, true} ->
            %% Write something to sync.
            Batch1 = [leveldb:mk_put(sext:encode(control_sync), <<"sync">>)],
            WriteOptions = [sync];
        {true, false} ->
            Batch1 = Batch,
            WriteOptions = [sync];
        {false, true} ->
            Batch1 = [],   %% This will not write anything to LevelDB and that is OK.
            WriteOptions = [];
        {false, false} ->
            Batch1 = Batch,
            WriteOptions = []
    end,
    true = leveldb:write(MetadataDB, Batch1, WriteOptions),
    ok.

-spec add_metadata_db_op(do_mod(), write_batch()) -> write_batch().
add_metadata_db_op({insert, StoreTuple}, Batch) ->
    leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({insert_value_into_ram, StoreTuple}, Batch) ->
    leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({insert_constant_value, StoreTuple}, Batch) ->
    leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({insert_existing_value, StoreTuple, _OldKey, _OldTimestamp}, Batch) ->
    leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({delete, _Key, 0, _ExpTime}=Op, _Batch) ->
    error({timestamp_is_zero, Op});
add_metadata_db_op({delete, Key, _Timestamp, _ExpTime}, Batch) ->
%%     DeleteMarker = make_delete_marker(Key, Timestamp),
%%     leveldb:add_put(metadata_db_key(Key, Timestamp),
%%                     term_to_binary(DeleteMarker), Batch);
    leveldb:add_delete(metadata_db_key(Key, 0), Batch);
add_metadata_db_op({delete_noexptime, _Key, 0}=Op, _Batch) ->
    error({timestamp_is_zero, Op});
add_metadata_db_op({delete_noexptime, Key, _Timestamp}, Batch) ->
%%     DeleteMarker = make_delete_marker(Key, Timestamp),
%%     leveldb:add_put(metadata_db_key(Key, Timestamp),
%%                     term_to_binary(DeleteMarker), Batch);
    leveldb:add_delete(metadata_db_key(Key, 0), Batch);
add_metadata_db_op({delete_all_table_items}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({md_insert, _Tuple}=Op, _Batch) ->
    %% @TODO: CHECKME: brick_ets:checkpoint_start/4, which was deleted after
    %% commit #b2952a393, had the following code to dump brick's private
    %% metadata. Check when metadata will be written and implement
    %% add_metadata_db_op/2 for it.
    %% ----
    %% %% Dump data from the private metadata table.
    %% MDs = term_to_binary([{md_insert, T} ||
    %%                       T <- ets:tab2list(S_ro#state.mdtab)]),
    %% {_, Bin2} = WalMod:create_hunk(?LOGTYPE_METADATA, [MDs], []),
    %% ok = file:write(CheckFH, Bin2),
    %% ----
    error({writeback_not_implemented, Op});
add_metadata_db_op({md_delete, _Key}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({log_directive, sync_override, false}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({log_directive, map_sleep, _Delay}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({log_noop}, Batch) ->
    Batch. %% noop

%% As for Hibari 0.3.0, metadata DB key is {Key, 0}. (The reversed
%% timestamp is always zero.)
-spec metadata_db_key(store_tuple()) -> binary().
metadata_db_key(StoreTuple) ->
    Key = brick_ets:storetuple_key(StoreTuple),
    %% Timestamp = brick_ets:storetuple_ts(StoreTuple),
    %% metadata_db_key(Key, Timestamp).
    metadata_db_key(Key, 0).

-spec metadata_db_key(key(), ts()) -> binary().
metadata_db_key(Key, _Timestamp) ->
    %% NOTE: Using reversed timestamp, so that Key-values will be sorted
    %%       in LevelDB from newer to older.
    %% ReversedTimestamp = -(Timestamp),
    ReversedTimestamp = -0,
    sext:encode({Key, ReversedTimestamp}).

%% -spec make_delete_marker(key(), ts()) -> tuple().
%% make_delete_marker(Key, Timestamp) ->
%%     {Key, Timestamp, delete_marker}.


%% 17: --------------------
%% MDKey: <<16,0,0,0,2,18,152,203,224,16,8,15,88,8,8,255,255,255,254,128,127,255,
%%          113,24,93,94,221,93,158,128,8,255>>
%% Metadata: {<<49,47,0,0,1,214>>,
%%            1388634474390210,
%%            {6,86448},
%%            100,
%%            [{md5,<<43,87,53,201,138,7,67,41,164,170,33,154,221,246,102,237>>}]}
%% 18: --------------------
%% MDKey: <<16,0,0,0,2,18,152,203,224,16,8,15,88,8,8,255,255,255,254,128,127,255,
%%          113,24,93,107,131,154,236,0,8,255>>
%% Metadata: {<<49,47,0,0,1,214>>,
%%            1388634418603303,
%%            {4,211443},
%%            100,
%%            [{md5,<<65,63,13,176,42,0,17,128,57,86,28,146,8,108,154,0>>}]}

%% After a Scavenger run:
%%
%% 17: --------------------
%% MDKey: <<16,0,0,0,2,18,152,203,224,16,8,15,88,8,8,255,255,255,254,128,127,255,
%%          113,24,93,94,221,93,158,128,8,255>>
%% Metadata: {<<49,47,0,0,1,214>>,
%%            1388634474390210,
%%            {-11,9072},
%%            100,
%%            [{md5,<<43,87,53,201,138,7,67,41,164,170,33,154,221,246,102,237>>}]}
%% 18: --------------------
%% MDKey: <<16,0,0,0,2,18,152,203,224,16,8,15,88,8,8,255,255,255,254,128,127,255,
%%          113,24,93,107,131,154,236,0,8,255>>
%% Metadata: {<<49,47,0,0,1,214>>,
%%            1388634418603303,
%%            {4,211443},
%%            100,
%%            [{md5,<<65,63,13,176,42,0,17,128,57,86,28,146,8,108,154,0>>}]}

%% @TODO Move the tool to a separate module.

tool_list_md(BrickName, Limit) when is_atom(BrickName), is_integer(Limit), Limit > 0 ->
    HLog = gmt_hlog:log_name2reg_name(?GMT_HLOG_COMMON_LOG_NAME),
    DB = gmt_hlog:get_metadata_db(HLog, BrickName),
    FirstKey = leveldb:first(DB, []),
    tool_list_md1(DB, FirstKey, 1, Limit).

tool_list_md1(_DB, _Key, Count, Limit) when Count > Limit ->
    ok;
tool_list_md1(_DB, '$end_of_table', _Count, _Limit) ->
    ok;
tool_list_md1(DB, Key, Count, Limit) ->
    Metadata = binary_to_term(leveldb:get(DB, Key, [])),
    io:format("~w: --------------------~nMDKey: ~w~nMetadata: ~p~n",
              [Count, Key, Metadata]),
    tool_list_md1(DB, leveldb:next(DB, Key, []), Count + 1, Limit).

%% -- WIP End ----------------------------------------------------




%% do_register_local_brick(Brick, #state{brick_registory = _Dict} = S) ->
do_register_local_brick(Brick, S) ->
    _ = file:make_dir(brick_registration_dir(S)),
    ok = create_registration_file(Brick, S),
    {ok, S}.

%% do_unregister_local_brick(_Brick, #state{brick_registory = _Dict} = S) ->
%%     {ok, S}.

%% do_permanently_unregister_local_brick(Brick, S) ->
%%     case lists:member(Brick, do_get_all_registrations(S)) of
%%         true ->
%%             ok = delete_registration_file(Brick, S),
%%             {ok, S};
%%         false ->
%%             {not_found, S}
%%     end.

spawn_future_tasks_after_dirty_buffer_wait(EndSeqNum, EndOffset, S) ->
    %% Advancing the common log's sequence number isn't really a
    %% future task, but doing it asyncly is a good idea.
    spawn(fun() ->
                  {ok, MinMB} = application:get_env(gdss_brick, brick_min_log_size_mb),
                  if EndOffset > MinMB * 1024 * 1024 div 2 ->
                          ?DBG_TLOG("spawn_future_tasks_after_dirty_buffer_wait [advance, 1]", []),
                          %% NOTE: not checking for success or failure
                          %% ... it doesn't matter
                          _ = gmt_hlog:advance_seqnum(S#state.hlog_pid, 1),
                          ok;
                     true ->
                          ok
                  end,
                  timer:sleep(S#state.dirty_buffer_wait * 1000),
                  ?DBG_TLOG("spawn_future_tasks_after_dirty_buffer_wait [flush]", []),
                  ok = write_flush_file(EndSeqNum, EndOffset, S),

                  if EndSeqNum > S#state.last_seqnum ->
                          _ = clean_old_seqnums(EndSeqNum, S),
                          ok;
                     true ->
                          ok
                  end,
                  ?DBG_TLOG("spawn_future_tasks_after_dirty_buffer_wait [exit]", []),
                  ?WAIT_BEFORE_EXIT,
                  exit(normal)
          end).

clean_old_seqnums(EndSeqNum, S) ->
    OldSeqs = [N || N <- gmt_hlog:find_current_log_seqnums(S#state.hlog_dir),
                    N < EndSeqNum],
    if S#state.short_long_same_dev_p ->
            ?DBG_TLOG("clean_old_seqnums [same] ~w", [OldSeqs]),
            _ = [gmt_hlog:move_seq_to_longterm(S#state.hlog_pid, N) ||
                    N <- OldSeqs];
       true ->
            ?DBG_TLOG("clean_old_seqnums [diff] ~w", [OldSeqs]),
            _ = [file:delete(gmt_hlog:log_file_path(S#state.hlog_dir, N)) ||
                    N <- OldSeqs],
            ?DBG_TLOG("clean_old_seqnums [del_major_done]", [])
    end.

short_long_same_dev_p(HLogDir) ->
    ShortDir = filename:dirname(gmt_hlog:log_file_path(HLogDir, 1)),
    LongDir = filename:dirname(gmt_hlog:log_file_path(HLogDir, -1)),
    {ok, ShortFI} = file:read_file_info(ShortDir),
    {ok, LongFI} = file:read_file_info(LongDir),
    ShortFI#file_info.major_device =:= LongFI#file_info.major_device andalso
        ShortFI#file_info.minor_device =:= LongFI#file_info.minor_device.

do_bigblob_hunk_writeback(_LastSeqNum, _LastOffset,
                          #state{short_long_same_dev_p = true}) ->
    %% We don't need to do any copying here: shortterm and longterm
    %% dirs are on the same device and thus in the same file system
    %% and thus everything will be moved to longterm storage by
    %% gmt_hlog:move_seq_to_longterm().
    {ok, 0};
do_bigblob_hunk_writeback(LastSeqNum, LastOffset,
                          #state{hlog_dir = HLogDir} = S) ->
    CurSeqs = [N || N <- gmt_hlog:find_current_log_seqnums(HLogDir),
                    N >= LastSeqNum],
    SeqSizes = lists:map(fun(SeqNum) ->
                                 Path = gmt_hlog:log_file_path(HLogDir, SeqNum),
                                 {ok, FI} = file:read_file_info(Path),
                                 {SeqNum, FI#file_info.size}
                         end, CurSeqs),
    ToDos = lists:map(fun({Seq, Size}) when Seq =:= LastSeqNum ->
                              {Seq, LastOffset, Size - LastOffset};
                         ({Seq, Size}) ->
                              {Seq, 0, Size}
                      end, SeqSizes),
    copy_parts(ToDos, S).

copy_parts(ToDos, #state{hlog_dir = HLogDir} = _S) ->
    N = lists:foldl(
          fun({_SeqNum, _ByteOffset, 0}, Acc) ->
                  Acc;
             ({SeqNum, ByteOffset, NumBytes}, Acc) ->
                  try
                      SrcPath = gmt_hlog:log_file_path(HLogDir, SeqNum),
                      DstPath = gmt_hlog:log_file_path(HLogDir, -SeqNum),
                      %% Hrm, I dunno if I like this dict kludge, either.
                      {ok, SrcFH} = file:open(SrcPath, [read, binary, raw]),
                      put(srcfh___, SrcFH),
                      {ok, DstFH} = file:open(DstPath, [read, write, binary, raw]),
                      put(dstfh___, DstFH),
                      {ok, Bin} = file:pread(SrcFH, ByteOffset, NumBytes),
                      ok = file:pwrite(DstFH, ByteOffset, Bin),
                      Acc + size(Bin)
                  catch
                      _X:_Y ->
                          ?E_ERROR("copy_parts ~p ~p ~p: ~p ~p",
                                   [SeqNum, ByteOffset, NumBytes, _X, _Y]),
                          ?WAIT_BEFORE_EXIT,
                          exit({copy_parts, _X, _Y})
                  after begin
                            erlang:garbage_collect(),
                            _ = [catch file:close(erase(X)) || X <- [srcfh___,dstfh___]]
                        end
                  end
          end, 0, ToDos),
    {ok, N}.

brick_registration_dir(S) ->
    S#state.hlog_dir ++ "/register".

brick_registration_file(BrickName, S) ->
    brick_registration_dir(S) ++ "/" ++ atom_to_list(BrickName).

create_registration_file(BrickName, S) ->
    {ok, FH} = file:open(brick_registration_file(BrickName, S), [write]),
    ok = file:close(FH).

%% delete_registration_file(BrickName, S) ->
%%     file:delete(brick_registration_file(BrickName, S)).

do_get_all_registrations(S) ->
    Paths = filelib:fold_files(brick_registration_dir(S),
                               ".*", false, fun(F, Acc) -> [F|Acc] end, []),
    [list_to_atom(filename:basename(X)) || X <- Paths].

do_sequence_file_is_bad(SeqNum, Offset, S) ->
    {CurSeqNum, _CurOffset} =
        gmt_hlog:get_current_seqnum_and_file_position(S#state.hlog_pid),
    brick_ets:sequence_file_is_bad_common(
      S#state.hlog_dir, gmt_hlog, S#state.hlog_pid, S#state.name,
      SeqNum, Offset),
    LocalBricks = do_get_all_registrations(S),
    if SeqNum =:= CurSeqNum ->
            ?E_CRITICAL("Fatal error: common log: current sequence file is bad: ~p",
                        [SeqNum]),
            spawn(fun() -> application:stop(gdss_brick) end),
            ?WAIT_BEFORE_EXIT,
            exit({current_sequence_is_bad, SeqNum});
       true ->
            _ = [begin
                     brick_ets:append_external_bad_sequence_file(Brick, SeqNum),
                     spawn(fun() -> brick_server:common_log_sequence_file_is_bad(
                                      Brick, node(), SeqNum)
                           end)
                 end || Brick <- LocalBricks],
            S
    end.
