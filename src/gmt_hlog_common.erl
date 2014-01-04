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

-define(WB_BATCH_SIZE, 200). % For write_back buffering.
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

%% Scavenger API
-export([start_scavenger_commonlog/1,
         stop_scavenger_commonlog/0,
         scavenger_commonlog/1                  % Not commonly used
        ]).

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

-type byte_size() :: non_neg_integer().
-type seqnum_hunk_size() :: {seqnum(), byte_size()}.

-record(state, {
          name                            :: file:name(),
          hlog_name                       :: file:name(),
          hlog_pid                        :: pid(),                    %% Pid of the gmt_hlog
          hlog_dir                        :: dirname(),
          last_seqnum                     :: seqnum(),                 %% SeqNum of last writeback
          last_offset                     :: offset(),                 %% Offset of last writeback
          %% brick_registory=orddict:new()   :: orddict(),                %% Registered local bricks
          tref                            :: timer:tref(),
          scavenger_tref                  :: timer:tref() | undefined,
          async_writeback_pid             :: pid() | undefined,
          async_writeback_reqs=[]         :: [from()],                 %% requesters of current async writeback
          async_writebacks_next_round=[]  :: [from()],                 %% requesters of next async writeback
          dirty_buffer_wait               :: non_neg_integer(),        %% seconds
          short_long_same_dev_p           :: boolean(),
          first_writeback                 :: boolean(),
          should_record_rename_ops        :: boolean()
         }).
%% -type state()   :: #state{}.
-type state_readonly() :: #state{}.  %% Read-only

%% write-back info
-record(wb, {
          exactly_count = 0   :: non_neg_integer(),  % number of metadata tuples to write-back
          exactly_ts = []     :: [metadata_tuple()]  % metadata tuples to write-back
         }).
-type wb_r() :: #wb{}.

-record(scav_progress, {
          copied_hunks = 0    :: non_neg_integer(),
          copied_bytes = 0    :: byte_size(),
          errors       = 0    :: non_neg_integer()
         }).
-type scav_progress_r() :: #scav_progress{}.


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

                %% @TODO: Just for now
                _ = gmt_hlog:create_rename_op_db(Log),

                LogDir = gmt_hlog:log_name2data_dir(CName),
                {SeqNum, Off} = read_flush_file(LogDir),
                self() ! do_sync_writeback,
                %% Use timer instead of brick_itimer: it allows QC testing
                %% without GDSS app running, and there's only one of
                %% these things per app.
                WriteBackInterval = 30000,  %% 30 secs.  @TODO: Configurable
                {ok, TRef} = timer:send_interval(WriteBackInterval, do_async_writeback),
                SupressScavenger = prop_or_application_env_bool(
                                     brick_scavenger_suppress,
                                     suppress_scavenger, PropList,
                                     false),
                ScavengerTRef =
                    case SupressScavenger of
                        false ->
                            {ok, STR} = schedule_next_daily_scavenger(),
                            STR;
                        true ->
                            undefined
                    end,
                {ok, DirtySec} = application:get_env(gdss_brick, brick_dirty_buffer_wait),
                SameDevP = short_long_same_dev_p(LogDir),

                {ok, #state{name=CName, hlog_pid=Log, hlog_name=CName,
                            hlog_dir=LogDir,
                            last_seqnum=SeqNum, last_offset=Off,
                            tref=TRef,
                            scavenger_tref=ScavengerTRef,
                            dirty_buffer_wait=DirtySec,
                            short_long_same_dev_p=SameDevP,
                            first_writeback=true,
                            %% @TODO: Just for now
                            should_record_rename_ops=true
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
    NewState = if NewSeqNum > SeqNum orelse
                  (NewSeqNum =:= SeqNum andalso NewOffset > Offset) ->
                       State#state{last_seqnum=NewSeqNum, last_offset=NewOffset};
                  (NewSeqNum =:= SeqNum andalso NewOffset =:= Offset) ->
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
handle_info(start_daily_scavenger, State) ->
    timer:sleep(2 * 1000),
    {ok, ScavengerTRef} = schedule_next_daily_scavenger(),
    {ok, Percent} = application:get_env(gdss_brick, brick_skip_live_percentage_greater_than),
    {ok, WorkDir} = application:get_env(gdss_brick, brick_scavenger_temp_dir),
    PropList = [{skip_live_percentage_greater_than, Percent},
                {work_dir, WorkDir}],
    %% Self-deadlock with get_all_registrations(), must use worker.
    spawn(fun() -> start_scavenger_commonlog(PropList) end),
    {noreply, State#state{scavenger_tref=ScavengerTRef}};
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
terminate(_Reason, #state{hlog_pid=HLogPid,tref=TRef,scavenger_tref=ScavengerTRef}) ->
    _ = if ScavengerTRef =/= undefined ->
                timer:cancel(ScavengerTRef);
           true ->
                noop
        end,
    _ = timer:cancel(TRef),
    _ = if HLogPid =/= undefined ->
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

        {#wb{exactly_ts=Ts, exactly_count=Count}=WB, []} ->
            ?E_DBG("~w hunks to write back", [Count]),
            ok = write_back_exactly_to_logs(Ts, S),
            ok = write_back_to_stable_storege(WB, S),
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

peek_first_brick_name(Ts) ->
    case Ts of
        [T|_] -> element(2, T);
        []    -> ''
    end.

write_back_exactly_to_logs(Ts, S) ->
    %% Sort metadata tuples by brickname, seqnum, and offset.
    SortedTs = lists:sort(Ts),
    FirstBrickName = peek_first_brick_name(SortedTs),
    write_back_to_local_log(SortedTs, undefined, 0, undefined,
                            FirstBrickName, [], ?WB_BATCH_SIZE, S).

%% -spec write_back_to_local_log([metadata_tuple()], state_readonly()) -> ????.
write_back_to_local_log([{eee, LocalBrickName, SeqNum, Offset, _Key, _TypeNum,
                          H_Len, H_Bytes}|Ts] = AllTs,
                        LogSeqNum,
                        LogFH_pos,
                        I_LogFH,
                        LastBrickName,
                        I_TsAcc,
                        Count,
                        S)
  when Count > 0,
       LocalBrickName =:= LastBrickName ->

    {LogFH, TsAcc} =
        if LogSeqNum =:= SeqNum andalso I_LogFH =/= undefined ->
                {I_LogFH, I_TsAcc};
           true ->
                if I_LogFH =/= undefined ->
                        write_back_metadata_hunk(I_LogFH, lists:reverse(I_TsAcc));
                   true ->
                        ok
                end,
                %% ?DBG_TLOG("write_back_to_local_log [close] ~w", [I_LogFH]),
                (catch file:close(I_LogFH)),

                LogDir = gmt_hlog:log_name2data_dir(LocalBrickName),
                {ok, Lfh} = open_log_file_mkdir(LogDir, SeqNum, [read,write,binary]),
                case check_hlog_header(Lfh) of
                    created ->
                        ?E_INFO("Created local log file with sequence ~w: ~s",
                                [SeqNum, gmt_hlog:log_file_path(LogDir, SeqNum)]);
                    ok ->
                        ok
                end,
                {Lfh, []}
        end,
    if TsAcc =:= [] ->
            %% ?E_DBG("1a", []),
            {ok, Offset} = file:position(LogFH, {bof, Offset}),
            write_back_to_local_log(Ts, SeqNum, Offset + H_Len,
                                    LogFH, LocalBrickName,
                                    [H_Bytes|TsAcc],
                                    Count - 1, S);

       LogSeqNum =:= SeqNum, LogFH_pos =:= Offset ->
            %% ?E_DBG("1b", []),
            write_back_to_local_log(Ts, SeqNum, Offset + H_Len,
                                    LogFH, LocalBrickName,
                                    [H_Bytes|TsAcc],
                                    Count - 1, S);
       true ->
            %% ?E_DBG("1c", []),
            %% Writeback!
            write_back_to_local_log(AllTs, SeqNum, LogFH_pos, LogFH,
                                    LastBrickName, TsAcc, 0, S)
    end;

%% Writeback what we have at the current LogFH file position (already
%% set!), then reset accumulators & counter and resume iteration.
write_back_to_local_log(AllTs, SeqNum, _LogFH_pos, LogFH,
                        LastBrickName, TsAcc, _Count, S)
  when LogFH =/= undefined ->
    %% ?E_DBG("2", []),
    write_back_metadata_hunk(LogFH, lists:reverse(TsAcc)),
    ?E_DBG("Wrote ~w hunks for ~w", [length(TsAcc), LastBrickName]),
    case peek_first_brick_name(AllTs) of
        LastBrickName ->
            %% ?E_DBG("2a", []),
            write_back_to_local_log(AllTs, SeqNum, 0, LogFH,
                                    LastBrickName, [], ?WB_BATCH_SIZE, S);
        OtherBrickName ->
            %% ?E_DBG("2b", []),
            (catch file:close(LogFH)),
            write_back_to_local_log(AllTs, undefined, 0, undefined,
                                    OtherBrickName, [], ?WB_BATCH_SIZE, S)
    end;

%% No more input, perhaps one last writeback?
write_back_to_local_log([] = AllTs, SeqNum, FH_pos, LogFH,
                        LastBrickName, TsAcc, _Count, S) ->
    %% ?E_DBG("3", []),
    if TsAcc =:= [] ->
            %% ?E_DBG("3a", []),
            (catch file:close(LogFH)),
            ok;
       true ->
            %% ?E_DBG("3b", []),
            %% Writeback one last time.
            write_back_to_local_log(AllTs, SeqNum, FH_pos, LogFH,
                                    LastBrickName, TsAcc, 0, S)
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
%%  3. Finish implementing the metabata write-back process (including
%%     LevelDB disk sync). Test with chain length = 3.
%%  4. Implement the new scavenger logic.
%%  5. Implement the new housekeeping logic.
%%  6. Run lengthy tests (and develop some tools) to ensure that the
%%     metadata DB is working correctly.
%%  7. Rewrite brick_ets:wal_scan_all/2 to utilize the metadata DB.
%%  8. Remove the brick local log and checkpoint process.
%%  9. Make write-back process more intelligent; every N hunks have
%%     been written to the common log or N secs interval.
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

%% New Housekeeping Logic
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
                              #state{hlog_pid=HLog,
                                     should_record_rename_ops=ShouldRecordRenameOps}=State,
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
                    ok = write_back_to_metadata_db(MetadataDB, BrickName, DoMods, IsLastBatch),
                    case ShouldRecordRenameOps of
                        true ->
                            RenameOpDB = gmt_hlog:get_rename_op_db(HLog),
                            ok = record_rename_ops(RenameOpDB, BrickName, DoMods, IsLastBatch);
                        false ->
                            ok
                        end;
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
write_back_to_metadata_db(MetadataDB, BrickName, DoMods, IsLastBatch) ->
    Batch = lists:foldl(fun add_metadata_db_op/2, leveldb:new_write_batch(), DoMods),
    IsEmptyBatch = leveldb:is_empty_batch(Batch),
    case {IsLastBatch, IsEmptyBatch} of
        {true, true} ->
            %% Write something to sync.
            Batch1 = [leveldb:mk_put(<<"control sync">>, <<"sync">>)],
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
    if
        IsLastBatch orelse not IsEmptyBatch ->
            ?E_DBG("Wrote one hunk for ~s with write options ~w",
                   [BrickName, WriteOptions]);
        true ->
            ok
    end,
    ok.

-spec add_metadata_db_op(do_mod(), write_batch()) -> write_batch().
add_metadata_db_op({insert, StoreTuple}, Batch) ->
    Key = brick_ets:storetuple_key(StoreTuple),
    Timestamp = brick_ets:storetuple_ts(StoreTuple),
    ?E_DBG("store_tuple: insert - key: ~p, ts: ~w", [Key, Timestamp]),
    leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({insert_value_into_ram, StoreTuple}, Batch) ->
    Key = brick_ets:storetuple_key(StoreTuple),
    Timestamp = brick_ets:storetuple_ts(StoreTuple),
    ?E_DBG("store_tuple: insert_value_into_ram - key: ~p, ts: ~w", [Key, Timestamp]),
    leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({insert_constant_value, StoreTuple}, Batch) ->
    Key = brick_ets:storetuple_key(StoreTuple),
    Timestamp = brick_ets:storetuple_ts(StoreTuple),
    ?E_DBG("store_tuple: insert_constant_value - key: ~p, ts: ~w", [Key, Timestamp]),
    leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({insert_existing_value, StoreTuple, OldKey}, Batch) ->
    Key = brick_ets:storetuple_key(StoreTuple),
    Timestamp = brick_ets:storetuple_ts(StoreTuple),
    ?E_DBG("store_tuple: insert_existing_value - key: ~p, ts: ~w, oldkey: ~p",
           [Key, Timestamp, OldKey]),
    leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({delete, _Key, 0, _ExpTime}=Op, _Batch) ->
    error({timestamp_is_zero, Op});
add_metadata_db_op({delete, Key, Timestamp, _ExpTime}, Batch) ->
    ?E_DBG("store_tuple: delete - key: ~p, ts: ~w", [Key, Timestamp]),
    DeleteMarker = make_delete_marker(Key, Timestamp),
    leveldb:add_put(metadata_db_key(Key, Timestamp),
                    term_to_binary(DeleteMarker), Batch);
add_metadata_db_op({delete_noexptime, _Key, 0}=Op, _Batch) ->
    error({timestamp_is_zero, Op});
add_metadata_db_op({delete_noexptime, Key, Timestamp}, Batch) ->
    ?E_DBG("store_tuple: delete_noexptime - key: ~p", [Key]),
    DeleteMarker = make_delete_marker(Key, Timestamp),
    leveldb:add_put(metadata_db_key(Key, Timestamp),
                    term_to_binary(DeleteMarker), Batch);
add_metadata_db_op({delete_all_table_items}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({md_insert, _Tuple}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({md_delete, _Key}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({log_directive, sync_override, false}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({log_directive, map_sleep, _Delay}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({log_noop}, Batch) ->
    Batch. %% noop

-spec metadata_db_key(store_tuple()) -> binary().
metadata_db_key(StoreTuple) ->
    Key = brick_ets:storetuple_key(StoreTuple),
    Timestamp = brick_ets:storetuple_ts(StoreTuple),
    metadata_db_key(Key, Timestamp).

-spec metadata_db_key(key(), ts()) -> binary().
metadata_db_key(Key, Timestamp) ->
    %% NOTE: Using reversed timestamp, so that Key-values will be sorted
    %%       in LevelDB from newer to older.
    ReversedTimestamp = -(Timestamp),
    sext:encode({Key, ReversedTimestamp}).

-spec make_delete_marker(key(), ts()) -> tuple().
make_delete_marker(Key, Timestamp) ->
    {Key, Timestamp, delete_marker}.

-spec record_rename_ops(leveldb:db(), brickname(), [do_mod()], boolean()) -> ok.
record_rename_ops(RenameOpDB, BrickName, DoMods, IsLastBatch) ->
    BrickName_DoMods = [ {BrickName, M} || M <- DoMods ],
    Batch = lists:foldl(fun add_rename_op/2, leveldb:new_write_batch(),
                        BrickName_DoMods),
    IsEmptyBatch = leveldb:is_empty_batch(Batch),
    case {IsLastBatch, IsEmptyBatch} of
        {true, true} ->
            %% Write something to sync.
            Batch1 = [leveldb:mk_put(<<"control sync">>, <<"sync">>)],
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
    true = leveldb:write(RenameOpDB, Batch1, WriteOptions),
    if
        IsLastBatch ->
            ?E_DBG("fsynced the rename op DB", []);
        true ->
            ok
    end,
    ok.

-spec add_rename_op({brickname(), do_mod()}, write_batch()) -> write_batch().
add_rename_op({BrickName, {insert_existing_value, StoreTuple, OldKey}}, Batch) ->
    Key = brick_ets:storetuple_key(StoreTuple),
    Timestamp = brick_ets:storetuple_ts(StoreTuple),
    ?E_DBG("rename_op_db: insert_existing_value - key: ~p, ts: ~w, oldkey: ~p",
           [Key, Timestamp, OldKey]),
    leveldb:add_put(rename_op_db_key(BrickName, StoreTuple),
                    term_to_binary({StoreTuple, OldKey}), Batch);
add_rename_op(_, Batch) ->
    Batch. %% noop

-spec rename_op_db_key(brickname(), store_tuple()) -> binary().
rename_op_db_key(BrickName, StoreTuple) ->
    Key = brick_ets:storetuple_key(StoreTuple),
    Timestamp = brick_ets:storetuple_ts(StoreTuple),
    sext:encode({BrickName, Key, Timestamp}).



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




-spec check_hlog_header(file:fd()) -> ok | created.
check_hlog_header(FH) ->
    FileHeader = gmt_hlog:file_header_version_1(),
    {ok, 0} = file:position(FH, {bof, 0}),
    case file:read(FH, erlang:iolist_size(FileHeader)) of
        %% Kosher cases only
        {ok, FileHeader} ->
            ok;
        eof -> % File is 0 bytes or at least smaller than header
            {ok, 0} = file:position(FH, {bof, 0}),
            ok = file:write(FH, FileHeader),
            created
    end.

-spec open_log_file_mkdir(dirname(), seqnum(), openmode()) -> {ok, file:fd()} | {error, atom()}.
open_log_file_mkdir(Dir, SeqNum, Options) when SeqNum > 0 ->
    case gmt_hlog:open_log_file(Dir, SeqNum, Options) of
        {error, enoent} ->
            ok = file:make_dir(Dir), % FIX later: ICKY assumption!! SeqNum > 0.
            ok = file:make_dir(Dir ++ "/s"), % FIX later: ICKY assumption!!
            ?DBG_TLOG("open_log_file_mkdir ~s", [Dir]),
            open_log_file_mkdir(Dir, SeqNum, Options);
        Res ->
            Res
    end.

-spec write_back_metadata_hunk(file:fd(), iodata()) -> ok.
write_back_metadata_hunk(LogFH, LogBytes) ->
    ok = file:write(LogFH, LogBytes),
    ok.

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


%% TODO: Perhaps have multiple procs working on copying from different
%%       sequence files?  Just in case we have too much idle disk I/O
%%       capacity and wish to use it....
%% TODO: In an ideal world, the brick walks would include the size of the
%%       value blob so that copy_one_hunk could be a bit more efficient.

start_scavenger_commonlog(PropList0) ->
    RegBricks = gmt_hlog_common:get_all_registrations(),
    BrOpts = [{Br, case {catch brick_server:status(Br, node()),
                         catch brick_server:chain_get_my_repair_state(Br, node())} of
                       {{ok, Ps}, ok} ->
                           Is = proplists:get_value(implementation, Ps),
                           proplists:get_value(options, Is);
                       _ ->
                           error
                   end} || Br <- RegBricks],
    case [Br || {Br, error} <- BrOpts] of
        [] ->
            BrNds = [{Br, node()} || Br <- RegBricks],
            _ = [gmt_util:clear_alarm({scavenger, T}) || T <- BrNds],
            Bigs = [Br || {Br, Os} <- BrOpts,
                          proplists:get_value(bigdata_dir, Os, false) =/= false],
            ?E_INFO("SCAV: Started with ~p", [Bigs]),
            do_start_scavenger_commonlog2(Bigs, PropList0);
        ErrBricks ->
            ErrNds = [{Br, node()} || Br <- ErrBricks],
            Msg = "Scavenger may not execute until all bricks are running.",
            _ = [gmt_util:set_alarm({scavenger, T}, Msg) || T <- ErrNds],
            ?E_ERROR("Bricks ~p are not available, scavenger aborted", [ErrNds]),
            {error, ErrNds}
    end.

do_start_scavenger_commonlog2(Bricks, PropList) ->
    CommonLogSvr = gmt_hlog_common:hlog_pid(?GMT_HLOG_COMMON_LOG_NAME),
    {ok, CurSeq0} = gmt_hlog:advance_seqnum(CommonLogSvr, 1),
    {CurSeq1, _} = read_flush_file(gmt_hlog:log_name2data_dir(
                                     ?GMT_HLOG_COMMON_LOG_NAME)),
    %% CurSeq1 is the largest sequence number that the writeback
    %% process (plus time spent waiting for the
    %% 'brick_dirty_buffer_wait' interval) has flushed to disk.  Don't
    %% mess with any sequence at or above CurSeq1.
    CurSeq = erlang:min(CurSeq0, CurSeq1),

    MaximumLivePt = case proplists:get_value(skip_live_percentage_greater_than,
                                             PropList) of
                        N when N >= 0, N =< 100 -> N;
                        _                       -> 100 % Examine all files
                    end,
    ThrottleBytes = case proplists:get_value(throttle_bytes, PropList) of
                        T when is_integer(T), T >= 0 ->
                            T;
                        _ ->
                            {ok, T} = application:get_env(gdss_brick, brick_scavenger_throttle_bytes),
                            T
                    end,
    SorterSize = proplists:get_value(sorter_size, PropList, 16*1024*1024),
    WorkDir = proplists:get_value(work_dir, PropList, "./scavenger-work-dir"),

    SA = #scav{options = PropList,
               work_dir = WorkDir,
               wal_mod = gmt_hlog_common,
               name = ?GMT_HLOG_COMMON_LOG_NAME,
               log = gmt_hlog:log_name2reg_name(?GMT_HLOG_COMMON_LOG_NAME),
               log_dir = gmt_hlog:log_name2data_dir(?GMT_HLOG_COMMON_LOG_NAME),
               %% For common log, any sequence number at or beyond
               %% CurSeq is off-limits.
               last_check_seq = CurSeq,
               skip_live_percentage_greater_than = MaximumLivePt,
               sorter_size = SorterSize, bricks = Bricks,
               throttle_bytes = ThrottleBytes,
               bottom_fun = fun scavenger_commonlog_bottom/1},
    ?DBG_TLOG("scavenger ~w, last_check_seq ~w", [SA#scav.name, CurSeq]),
    Fdoit = fun() -> scavenger_commonlog(SA),
                     exit(normal)
            end,
    Pid = spawn(fun() -> link_catch_shutdowns(Fdoit) end),
    {ok, Pid}.

link_catch_shutdowns(Fun) ->
    try
        %% We're probably spawned without a link.  It would be nice if
        %% someone would stop us if something big happened, e.g. the
        %% GDSS app were shut down.  We'll link ourselves to a
        %% well-known supervisor that we know must be running if the
        %% bricks we're scavenging are running.
        link(whereis(brick_brick_sup)),
        process_flag(priority, low),
        Fun(),
        exit(normal)
    catch
        exit:Fine when Fine =:= normal; Fine =:= stopping_on_shutdown_request;
                       Fine =:= exclusive_wait_exit ->
            ok;
        exit:{_, _, Fine}
          when Fine =:= normal; Fine =:= stopping_on_shutdown_request;
               Fine =:= exclusive_wait_exit ->
            ok; % smart_exceptions
        X:Y ->
            ?E_ERROR("Scavenger ~p error: ~p ~p @ ~p",
                     [self(), X, Y, erlang:get_stacktrace()])
    end.

scav_excl_name() ->
    the_scavenger_proc.

scav_stop_signal_p() ->
    receive please_stop_now -> true
    after 0                 -> false
    end.

stop_scavenger_commonlog() ->
    case whereis(scav_excl_name()) of
        undefined -> scavenger_not_running;
        Pid       -> Pid ! please_stop_now,
                     {shutdown_requested, Pid}
    end.

-spec scav_check_shutdown(scav_r(), scav_progress_r()) -> ok | no_return().
scav_check_shutdown(#scav{name=Name}=SA, Progress) ->
    case scav_stop_signal_p() of
        true ->
            ?E_INFO("SCAV: ~w - Received shutdown request.",
                    [Name, self()]),
            scav_print_summary(SA, Progress),
            scav_delete_work_files(SA),
            ?WAIT_BEFORE_EXIT,
            exit(stopping_on_shutdown_request);
        false ->
            ok
    end.

-spec scav_print_summary(scav_r(), 'undefined' | scav_progress_r()) -> ok.
scav_print_summary(SA, undefined) ->
    scav_print_summary(SA, #scav_progress{});
scav_print_summary(#scav{name=Name, options=Options,
                         dead_paths=DeadPaths,
                         dead_seq_bytes=DeadSeqBytes,
                         live_seq_bytes=LiveSeqBytes,
                         live_hunk_sizes=LiveHunkSizesGroupBySeq},
                   #scav_progress{copied_hunks=CopiedHunks,
                                  copied_bytes=CopiedBytes,
                                  errors=Errors
                                 }) ->
    ?E_INFO("Scavenger ~w finished:\n"
            "\tOptions: ~p\n"
            "\tTotal ~p bytes reclaimed.\n"
            "\tLogs deleted without scavenging: ~p (~p bytes)\n"
            "\tLogs scavenged: ~p (~p bytes)\n"
            "\t\tCopied: hunks, bytes, errs = ~p, ~p, ~p",
            [Name,
             Options,
             DeadSeqBytes + (LiveSeqBytes - CopiedBytes),
             length(DeadPaths), DeadSeqBytes,
             length(LiveHunkSizesGroupBySeq), (LiveSeqBytes - CopiedBytes),
             CopiedHunks, CopiedBytes, Errors]),
    ok.

-spec scav_delete_work_files(scav_r()) -> ok.
scav_delete_work_files(#scav{work_dir=WorkDir}) ->
    _ = os:cmd("/bin/rm -rf " ++ WorkDir),
    ok.

scav_exit_if_someone_else() ->
    F_excl_wait = fun() ->
                          ?E_NOTICE("Scavenger ~p exiting. Another scavenger process is running.",
                                   [self()]),
                          ?WAIT_BEFORE_EXIT,
                          exit(exclusive_wait_exit)
                  end,
    F_excl_go = fun() ->
                        ?E_INFO("Scavenger ~p starting now.", [self()])
                end,
    brick_ets:really_cheap_exclusion(scav_excl_name(), F_excl_wait, F_excl_go).

-spec scavenger_commonlog(scav_r()) -> 'normal'.
scavenger_commonlog(#scav{name=Name, work_dir=WorkDir,
                          log=Log, exclusive_p=ExclusiveP,
                          last_check_seq=LastCheckSeq,
                          bottom_fun=BottomFun}=SA) ->

    %% Make certain this is the only scavenger running.
    if
        ExclusiveP ->
            _ = scav_exit_if_someone_else(),
            ok;
        true ->
            ok
    end,

    %% Step 1 - Get all sequence numbers eligible for scavenging.
    AllSeqs = lists:usort([N || N <- gmt_hlog:get_all_seqnums(Log),
                                abs(N) < LastCheckSeq]),

    if
        AllSeqs =:= [] ->
            ?E_INFO("SCAV: ~w - Finished. No log sequence files to scavenge.", [Name]),
            normal;
        true ->
            ?E_INFO("SCAV: ~w - Log sequence numbers eligible for scavenging: ~w",
                   [Name, AllSeqs]),

            %% Clear the WorkDir
            scav_delete_work_files(SA),
            ok = file:make_dir(WorkDir),

            %% Step 2 Find all keys and their raw storage locations.
            scavenger_commonlog_save_storage_locations(SA, AllSeqs),
            scav_check_shutdown(SA, undefined),

            %% @TODO Run step 5 before step 3
            %% - Move the part to scan for live hunk bytes from step 3 to 5
            %% - Update step 5 to delete the work files for the sequences that
            %%   have grater live hunk percentages than the threshold
            %% - Move step 3 under step 5

            %% Step 3 Sort store tuples in each work file.
            HunkSizesGroupBySeq = scavenger_sort_storage_locations(SA),
            scav_check_shutdown(SA, undefined),

            %% Step 4 Identify sequences that contains 0 live hunks.
            {DeadSeqs, DeadPaths} = scavenger_find_dead_sequences(SA, AllSeqs, HunkSizesGroupBySeq),
            scav_check_shutdown(SA, undefined),

            %% Step 5 Filter out any live sequences that contain
            %% more than the maximum amount of live/in-use space.
            LiveSeqs = AllSeqs -- DeadSeqs,
            LiveHunkSizesGroupBySeq = scavenger_find_live_sequences(SA, LiveSeqs, HunkSizesGroupBySeq),
            scav_check_shutdown(SA, undefined),

            %% Step 6
            {DeadSeqBytes, LiveSeqBytes} =
                scavenger_count_all_bytes_in_sequences(SA, DeadPaths, LiveSeqs),
            scav_check_shutdown(SA, undefined),

            SA2 = SA#scav{dead_paths=DeadPaths,
                          dead_seq_bytes=DeadSeqBytes,
                          live_seq_bytes=LiveSeqBytes,
                          live_hunk_sizes=LiveHunkSizesGroupBySeq
                         },
            BottomFun(SA2)
    end.

%% @doc Scavenger step 2: Find all keys and their raw storage
%% locations via get_many hackery
-spec scavenger_commonlog_save_storage_locations(scav_r(), [seqnum()]) -> 'ok'.
scavenger_commonlog_save_storage_locations(#scav{name=Name, work_dir=WorkDir,
                                                 bricks=Bricks}=SA, AllSeqs) ->
    ?E_INFO("SCAV: ~w - Saving all keys and their raw storage locations.", [Name]),

    %% This function creates a dict with the followings:
    %%     key = Sequence #
    %%     val = {LiveBytes, [StoreTuple,...]}
    FirstKey = ?BRICK__GET_MANY_FIRST,
    Fs = [get_many_raw_storetuples],
    F_k2d = fun({_BrickName, _Key, _TS, {0, 0}}, Dict) ->
                    Dict;
               ({BrickName, Key, TS, {SeqNum, Offset}, ValLen, ExpTime, Flags}, Dict) ->
                    case lists:member(SeqNum, AllSeqs) of
                        true ->
                            {Bytes, L} = case dict:find(SeqNum, Dict) of
                                             {ok, {B_, L_}} -> {B_, L_};
                                             error          -> {0, []}
                                         end,
                            %% StoreTuples will be sorted later by ascending offset
                            StoreTuple = {Offset, BrickName, Key, TS, ValLen, ExpTime, Flags},
                            dict:store(SeqNum, {Bytes + ValLen, [StoreTuple|L]}, Dict);
                        false ->
                            Dict
                    end
            end,
    %% This function creates disk_log file, one per log sequence file,
    %% and write the following terms:
    %%    {live_bytes, Bytes}
    %%    StoreTuple
    F_lump = fun(Dict) ->
                     scav_check_shutdown(SA, undefined),
                     Sequences = dict:to_list(Dict),
                     lists:foreach(
                       fun({Seq, {LiveBytes, StoreTuples}}) ->
                               Path = WorkDir ++ "/" ++ integer_to_list(Seq) ++ ".",
                               {ok, WorkFile} = disk_log:open([{name,Path},
                                                               {file,Path},
                                                               {mode, read_write}]),
                               ok = disk_log:log(WorkFile, {live_bytes, LiveBytes}),
                               ok = disk_log:log_terms(WorkFile, StoreTuples),
                               ok = disk_log:close(WorkFile),
                               ?E_DBG("SCAV: ~w - Saved storage locations for sequence ~w. "
                                      "~w live hunks (~w bytes)",
                                      [Name, Seq, length(StoreTuples), LiveBytes]),
                               ok
                       end, Sequences),
                     dict:new()
             end,
    _ = [ ok = brick_ets:scavenger_get_keys(Br, Fs, FirstKey, F_k2d, F_lump) || Br <- Bricks ],
    ok.

%% @doc Scavenger step 3: Sort store tuples in each work file.
-spec scavenger_sort_storage_locations(scav_r()) -> [seqnum_hunk_size()].
scavenger_sort_storage_locations(#scav{name=Name, work_dir=WorkDir,
                                       sorter_size=SorterSize}=SA) ->
    ?E_INFO("SCAV: ~w - Sorting store tuples for each sequence", [Name]),

    %% Do this step in a child process, to try to avoid some
    %% accumulation of garbage, despite the fact that explicitly
    %% calling erlang:garbage_collect().

    %% This function sorts store tuples by ascending offsets.
    %%
    %% The store tuple in the work file is:
    %%     {Offset, BrickName, Key, TS, ValLen, ExpTime, Flags}
    Sorter = fun(InPath, Acc) ->
                     try
                         scav_check_shutdown(SA, undefined),

                         %% Sort store tuples.
                         {ok, Log} = disk_log:open([{name,InPath},
                                                    {file,InPath}, {mode,read_only}]),
                         OutPath = string:strip(InPath, right, $.),
                         {ok, OLog} = disk_log:open([{name,OutPath}, {file,OutPath}]),
                         ok = file_sorter:sort(brick_ets:file_input_fun(Log, start),
                                               brick_ets:file_output_fun(OLog),
                                               [{format, term},
                                                {size, SorterSize}]),
                         ok = disk_log:close(Log),
                         erlang:garbage_collect(),

                         {ok, #file_info{size=OutSize}} = file:read_file_info(OutPath),
                         ?E_INFO("SCAV: ~w - Sorted the contents of work file: ~s "
                                 "(file size: ~w bytes)",
                                 [Name, OutPath, OutSize]),

                         %% Count live bytes in Log.
                         {ok, Log} = disk_log:open([{name,InPath},
                                                    {file,InPath}, {mode,read_only}]),
                         Bytes = count_live_bytes_in_log(Log),
                         ok = disk_log:close(Log),
                         ok = file:delete(InPath),

                         erlang:garbage_collect(),
                         ?E_INFO("SCAV: ~w - Scanned the work file: ~s (live hunks: ~w bytes)",
                                 [Name, OutPath, Bytes]),

                         SeqNum = brick_ets:temp_path_to_seqnum(OutPath),
                         [{SeqNum, Bytes}|Acc]

                     catch Err1:Err2 ->
                             OutPath2 = string:strip(InPath, right, $.),
                             ?E_ERROR("SCAV: ~w - Error processing ~s and ~s: ~p ~p at ~p",
                                      [Name, InPath, OutPath2, Err1, Err2,
                                       erlang:get_stacktrace()]),
                             ?WAIT_BEFORE_EXIT,
                             exit(abort)
                     end
             end,
    ParentPid = self(),
    Pid = spawn_opt(fun() ->
                            X = filelib:fold_files(WorkDir, ".*", false, Sorter, []),
                            ParentPid ! {self(), X},
                            unlink(ParentPid),
                            exit(normal)
                    end, [link, {priority, low}]),
    receive {X, RemoteVal} when X =:= Pid-> RemoteVal end.

-spec count_live_bytes_in_log(file:fd()) -> integer().
count_live_bytes_in_log(Log) ->
    brick_ets:disk_log_fold(fun({live_bytes, Bs}, Sum) -> Sum + Bs;
                               (_               , Sum) -> Sum
                            end, 0, Log).

%% @doc Scavenger step 4: Identify sequences that contains 0 live hunks.
-spec scavenger_find_dead_sequences(scav_r(), [seqnum()], [seqnum_hunk_size()]) ->
                                           {[seqnum()], [file:name()]}.
scavenger_find_dead_sequences(#scav{name=Name, log_dir=LogDir, wal_mod=WalMod},
                              AllSeqs, HunkSizesGroupBySeq) ->
    %% Note: Because of movement of sequence #s from shortterm to longterm
    %%       areas, we need to check both positive & negative
    LiveDict = dict:from_list(HunkSizesGroupBySeq),
    DeadSeqs = lists:sort([ Seq || Seq <- AllSeqs,
                                   dict:find( Seq, LiveDict) =:= error,
                                   dict:find(-Seq, LiveDict) =:= error ]),
    DeadPaths = [ brick_ets:which_path(LogDir, WalMod, Seq) || Seq <- DeadSeqs ],
    if
        DeadPaths =/= [] ->
            ?E_INFO("SCAV: ~w - Found log sequences containing no live hunks: ~w",
                    [Name, DeadSeqs]);
        true ->
            ok
    end,
    {DeadSeqs, DeadPaths}.

%% @doc Scavenger step 5: Filter out any live sequences that contain
%% more than the maximum amount of live/in-use space. This calculation
%% is a bit inaccurate because the in-memory byte counts do not include
%% gmt_hlog overhead (header descriptions, etc.), but we'll be
%% close enough.

-spec scavenger_find_live_sequences(scav_r(), [seqnum()], [seqnum_hunk_size()]) ->
                                           [seqnum_hunk_size()].
scavenger_find_live_sequences(_SA, [], _HunkSizesGroupBySeq) ->
    [];
scavenger_find_live_sequences(#scav{name=Name, wal_mod=WalMod, log_dir=LogDir,
                                    skip_live_percentage_greater_than=SkipLivePercentage},
                              LiveSeqs, HunkSizesGroupBySeq) ->
    ?E_INFO("SCAV: ~w - Checking live hunks percentage for log sequences ~w",
            [Name, LiveSeqs]),

    LiveSeqsAbs = lists:sort([abs(N) || N <- LiveSeqs]),
    LiveBytesInLiveSeqs = lists:sort([T || {SeqNum, _} = T <- HunkSizesGroupBySeq,
                                           lists:member(SeqNum, LiveSeqsAbs)]),
    SeqSizes = lists:map(
                 fun(SeqNum) ->
                         {_, Path} = brick_ets:which_path(LogDir, WalMod, SeqNum),
                         {ok, FI} = file:read_file_info(Path),
                         {SeqNum, FI#file_info.size}
                 end, LiveSeqsAbs),

    {_, LiveHunkSizesGroupBySeq} =
        lists:unzip(lists:filter(
                      fun({{SeqNum1, FileSize}, {SeqNum2, Bytes}})
                            when abs(SeqNum1) =:= abs(SeqNum2) ->
                              LivePercentage = Bytes / FileSize,
                              ShouldScavenge =
                                  LivePercentage =< (SkipLivePercentage / 100),
                              ?E_INFO("SCAV: ~w - Live hunks percentage for log sequence ~w - ~.2f% "
                                      "live/total bytes: ~w/~w ~s",
                                      [Name, SeqNum1, LivePercentage * 100,
                                       Bytes, FileSize,
                                       if ShouldScavenge -> "";
                                          true ->           "(won't be scavenged)"
                                       end]),
                              ShouldScavenge
                      end, lists:zip(SeqSizes, LiveBytesInLiveSeqs))),
    LiveHunkSizesGroupBySeq.

%% @doc Scavenger step 6: Count all byets in sequences
-spec scavenger_count_all_bytes_in_sequences(scav_r(),
                                             DeadPaths::[file:name()], LiveSeqs::[seqnum()]) ->
                                                    {DeadSeqBytes::byte_size(),
                                                     LiveSeqBytes::byte_size()}.
scavenger_count_all_bytes_in_sequences(#scav{log_dir=LogDir, wal_mod=WalMod}, DeadPaths, LiveSeqs) ->
    Calculator = fun({_Seq, Path}, TotalBytes) ->
                         case file:read_file_info(Path) of
                             {ok, #file_info{size=Size}} ->
                                 TotalBytes + Size;
                             Err ->
                                 ?E_WARNING("Can't read file info for ~p (error ~p)", [Path, Err]),
                                 TotalBytes
                         end
                 end,

    DeadSeqBytes = lists:foldl(Calculator, 0, DeadPaths),
    LiveSeqBytes = lists:foldl(Calculator, 0,
                               [ brick_ets:which_path(LogDir, WalMod, Seq) || Seq <- LiveSeqs ]),
    {DeadSeqBytes, LiveSeqBytes}.


%% @doc Bottom half of scavenger.
%%
%% The arguments are an eclectic mix of stuff that's a result of a
%% previous refactoring to split the old refactoring function into
%% several pieces.  Bytes1, Del1, and BytesBefore are used only for
%% reporting at the end of this func.

-spec scavenger_commonlog_bottom(scav_r()) -> 'normal' | 'stopping_on_shutdown_request'.
scavenger_commonlog_bottom(#scav{name=Name,
                                 log=HLog,
                                 throttle_bytes=ThrottleBytes,
                                 dead_paths=DeadPaths,  %% DeadSeqs will be enough
                                 live_hunk_sizes=LiveHunkSizesGroupBySeq}=SA) ->

    %% Step 7: Delete sequence files with no live hunks
    lists:foreach(fun({SeqNum, Path}) ->
                          case delete_log_file(SA, SeqNum) of
                              {ok, _Path, Size} ->
                                  ?E_INFO("SCAV: ~w - Deleted a log sequence ~w "
                                          "with no live hunk: ~s (~w bytes)",
                                          [Name, SeqNum, Path, Size]);
                              {error, _}=Err ->
                                  ?E_WARNING("SCAV: ~w - Failed to delete a log sequence ~w "
                                             "with no live hunk: ~s (~p)",
                                             [Name, SeqNum, Path, Err])
                          end
                  end, DeadPaths),

    %% Step 8: Copy hunks to a new long-term sequence. Advance the
    %% long-term counter to avoid chance of writing to the same
    %% sequence that we read from.
    if
        LiveHunkSizesGroupBySeq =/= [] ->
            ?E_INFO("SCAV: ~w - Copying hunks to a new long-term log sequence", [Name]),
            {ok, _} = gmt_hlog:advance_seqnum(HLog, -1),

            {ok, ThrottlePid} = brick_ticket:start_link(undefined, ThrottleBytes),
            SA1 = SA#scav{throttle_pid=ThrottlePid},
            ScavProgress = lists:foldl(scavenge_one_seq_file_fun(SA1),
                                       #scav_progress{}, LiveHunkSizesGroupBySeq),
            ?E_INFO("SCAV: ~w - Finished copying hunks", [Name]),
            brick_ticket:stop(ThrottlePid),
            scav_print_summary(SA1, ScavProgress);

        true ->
            scav_print_summary(SA, undefined)
    end,

    scav_delete_work_files(SA),
    normal.

-spec scavenge_one_seq_file_fun(scav_r()) ->
                                       fun(({seqnum(), byte_size()}, scav_progress_r()) ->
                                                  scav_progress_r()).
scavenge_one_seq_file_fun(#scav{name=Name, work_dir=WorkDir,
                                wal_mod=WalMod,
                                log_dir=LogDir,
                                throttle_pid=ThrottlePid}=SA) ->

    BlobReader = fun(Su, FH) ->
                         ?LOGTYPE_BLOB = Su#hunk_summ.type,
                         [Bytes] = Su#hunk_summ.c_len,
                         brick_ticket:get(ThrottlePid, Bytes),
                         Bin = gmt_hlog:read_hunk_member_ll(FH, Su, md5, 1),
                         Su2 = Su#hunk_summ{c_blobs = [Bin]},
                         %% TODO: In case of failure, don't crash.
                         true = gmt_hlog:md5_checksum_ok_p(Su2),
                         Bin
                 end,

    fun({SeqNum, Bytes}, #scav_progress{copied_hunks=Hs, copied_bytes=Bs, errors=Es}=Progress) ->
            ?E_INFO("SCAV: ~w - Coping live hunks in log sequence ~w to the latest log: "
                    "~w bytes to copy",
                    [Name, SeqNum, Bytes]),
            DPath = WorkDir ++ "/" ++ integer_to_list(SeqNum),
            {ok, DiskLog} = disk_log:open([{name, DPath},
                                          {file, DPath}, {mode,read_only}]),
            {ok, InHLog} = WalMod:open_log_file(LogDir, SeqNum, [read, binary]),

            try scavenger_move_hunks(SA, SeqNum, BlobReader, InHLog, DiskLog) of
                #scav_progress{copied_hunks=HunkCount, copied_bytes=CopiedBytes, errors=Errors} ->
                    ok = disk_log:close(DiskLog),
                    ok = file:close(InHLog),
                    if
                        Errors =:= 0 ->
                            ?E_INFO("SCAV: ~w - Finished coping live hunks in log sequence ~w "
                                    "to the latest log: ~w hunks, ~w bytes",
                                    [Name, SeqNum, HunkCount, CopiedBytes]),
                            {ok, SleepTimeSec} = application:get_env(gdss_brick,
                                                                     brick_dirty_buffer_wait),
                            spawn_log_file_eraser(SA, SeqNum, SleepTimeSec);
                        true ->
                            ?E_ERROR("SCAV: ~w sequence ~p: ~p errors", [Name, SeqNum, Errors])
                    end,
                    #scav_progress{copied_hunks=Hs + HunkCount,
                                   copied_bytes=Bs + CopiedBytes,
                                   errors=Es + Errors}
            catch E1:E2 ->
                    ?E_ERROR("SCAV: ~w sequence ~p: ~p ~p", [Name, SeqNum, E1, E2]),
                    _ = disk_log:close(DiskLog),
                    _ = file:close(InHLog),
                    Progress#scav_progress{errors=Es + 1}
            end
    end.

-spec scavenger_move_hunks(scav_r(), SeqNum::seqnum(),
                           BlobReader::fun(), InHLog::file:fd(), DiskLog::term()) ->
                                  scav_progress_r().
scavenger_move_hunks(SA, SeqNum, BlobReader, InHLog, DiskLog) ->
    scavenger_move_hunks1(SA, SeqNum, BlobReader, InHLog, DiskLog,
                          disk_log:chunk(DiskLog, start), #scav_progress{}).

-spec scavenger_move_hunks1(scav_r(), SeqNum::seqnum(),
                            BlobReader::fun(), InHLog::file:fd(), DiskLog::term(),
                            Chunk::term(),
                            Progress::scav_progress_r()) -> scav_progress_r().
scavenger_move_hunks1(_SA, _SeqNum, _BlobReader, _InHLog, _DiskLog, eof, Progress) ->
    Progress;
scavenger_move_hunks1(#scav{name=Name}, SeqNum, _BlobReader, _InHLog, _DiskLog, {error, _}=Err,
                      #scav_progress{errors=Errors}=Progress) ->
    ?E_ERROR("SCAV: ~w - Error occured while reading the workfile for sequence ~w: ~p, "
             "Canceled coping live hunks to the latest log",
             [Name, SeqNum, Err]),
    Progress#scav_progress{errors=Errors + 1};
scavenger_move_hunks1(SA, SeqNum, BlobReader, InHLog, DiskLog, {Count, Hunks},
                      #scav_progress{copied_hunks=TotalHunkCount,
                                     copied_bytes=TotalBytes,
                                     errors=TotalErrorCount}=Progress) ->

    %% Copy live hunks to the latest common hlog
    CopyOneHunkFun = fun({Offset, BrickName, Key, TS, _ValLen, _ExpTime, _Flags},
                         {Locations, Hs1, Bs1, Es1}) ->
                             case brick_ets:copy_one_hunk(SA, InHLog, Key,
                                                          SeqNum, Offset, BlobReader) of
                                 error ->
                                     {Hs1, Bs1, Es1 + 1};
                                 {NewLoc, Size} ->
                                     %% We want a tuple sortable first by
                                     %% brick name.
                                     OldLoc = {SeqNum, Offset},
                                     Tpl = {BrickName, Key, TS, OldLoc, NewLoc},
                                     {[Tpl|Locations], Hs1 + 1, Bs1 + Size, Es1}
                             end;
                        ({live_bytes, _}, Acc) ->
                             Acc
                     end,
    {Locations, HunkCount, MovedBytes, Errors} = lists:foldl(CopyOneHunkFun, {[], 0, 0, 0}, Hunks),

    %% Update storage locations
    case (catch update_locations_commonlog(SA, Locations)) of
        NumUpdates when is_integer(NumUpdates) ->
            Progress1 = Progress#scav_progress{copied_hunks=TotalHunkCount + HunkCount,
                                               copied_bytes=TotalBytes + MovedBytes},
            if
                Errors =:= 0 ->
                    scav_check_shutdown(SA, Progress1),
                    scavenger_move_hunks1(SA, SeqNum, BlobReader, InHLog, DiskLog,
                                          disk_log:chunk(DiskLog, Count), Progress1);
                true ->
                    Progress1
            end;
        _Err ->
            #scav_progress{copied_hunks=TotalHunkCount + HunkCount,
                           copied_bytes=TotalBytes + MovedBytes,
                           errors=TotalErrorCount + 1}
    end.

%% -spec
update_locations_commonlog(SA, Locations) ->
    NewLocs = gmt_util:list_keypartition(1, lists:keysort(1, Locations)),
    Updates = [update_locations_on_brick(SA, Brick, Locs) ||
                  {Brick, Locs} <- NewLocs],
    lists:sum(Updates).

%% -spec update_locations_on_brick(scav_r(), brick(),
%%                                 [{brick_name(), key(), timestamp(), value(), value()}])
%%                                 -> {ok, SuccessCount::non_neg_integer()}
%%                                        | {error, SuccessCount::non_neg_integer()}.
update_locations_on_brick(#scav{name=Name}, Brick, NewLocs) ->
    Dos = [make_update_location(NL) || NL <- NewLocs],
    DoRes = brick_server:do(Brick, node(), Dos,
                            [ignore_role,
                             {sync_override, false},
                             local_op_only_do_not_forward],
                            60 * 1000),
    AnyError = lists:any(fun({ok, _})       -> false;
                            (key_not_exist) -> false;
                            ({ts_error, _}) -> false;
                            (_X)            -> true
                         end, DoRes),
    if AnyError ->
            ?E_ERROR("Failed to update locations. brick ~p"
                     "~nkeys = ~p~nreturn = ~p~n",
               [Brick, [Key || {_, Key, _, _, _} <- NewLocs], DoRes]),
            error; % Yes, returning a non-integer is bad.
       true ->
            UpdateCount = length(Dos),
            ?E_DBG("SCAV: ~w - Updated storage locations for ~w keys on brick ~w",
                    [Name, UpdateCount, Brick]),
            UpdateCount
    end.

%% -spec make_update_location({brick_name(), key(), timestamp(), value(), value()})
%%                            -> fun().
make_update_location({_BrickName, Key, OrigTS, OrigVal, NewVal}) ->
    F = fun(Key0, _DoOp, _DoFlags, S0) ->
                O2 = case brick_server:ssf_peek(Key0, false, S0) of
                         [] ->
                             key_not_exist;
                         [{_Key, OrigTS, _OrigVal, OldExp, OldFlags}] ->
                             {ImplMod, ImplState} = brick_server:ssf_impl_details(S0),
                             Val0 = ImplMod:bcb_val_switcharoo(OrigVal, NewVal, ImplState),
                             Fs = [{testset, OrigTS}|OldFlags],
                             %% make_replace doesn't allow custom TS.
                             brick_server:make_op6(replace, Key0, OrigTS, Val0, OldExp, Fs);
                         [{_Key, RaceTS, _RaceVal, _RaceExp, _RaceFlags}] ->
                             {ts_error, RaceTS}
                     end,
                {ok, [O2]}
        end,
    brick_server:make_ssf(Key, F).

-spec spawn_log_file_eraser(scav_r(), seqnum(), non_neg_integer()) -> pid().
spawn_log_file_eraser(#scav{name=Name}=SA, SeqNum, SleepTimeSec) ->
    spawn(fun() ->
                  timer:sleep(SleepTimeSec * 1000),
                  case delete_log_file(SA, SeqNum) of
                      {ok, Path, Size} ->
                          ?E_INFO("SCAV: ~w - Deleted a log sequence ~w: ~s (~w bytes)",
                                  [Name, SeqNum, Path, Size]);
                      {error, Err} ->
                          ?E_ERROR("SCAV: ~w - Error deleting a log sequence ~w: (~p)",
                                   [Name, SeqNum, Err])
                  end
          end).

-spec delete_log_file(scav_r(), seqnum()) ->
                             {ok, Path::file:name(), FileSize::byte_size()} | {error, term()}.
delete_log_file(#scav{wal_mod=WalMod, log_dir=LogDir}, SeqNum) ->
    case WalMod:log_file_info(LogDir, SeqNum) of
        {ok, Path, #file_info{size=Size}} ->
            case file:delete(Path) of
                ok ->
                    {ok, Path, Size};
                {error, _}=Err ->
                    Err
            end;
        {error, _}=Err ->
                Err
    end.

schedule_next_daily_scavenger() ->
    %% NowSecs = calendar:time_to_seconds(time()),
    %% {ok, StartStr} = application:get_env(gdss_brick, brick_scavenger_start_time),
    %% StartSecs = calendar:time_to_seconds(parse_hh_mm(StartStr)),
    %% WaitSecs = if NowSecs < StartSecs ->
    %%                    (StartSecs - NowSecs);
    %%               true ->
    %%                    StartSecs + (86400 - NowSecs)
    %%            end,
    WaitSecs = 2 * 60,
    ?E_INFO("Scheduling next scavenger ~p seconds from now.", [WaitSecs]),
    timer:send_after(WaitSecs * 1000, start_daily_scavenger).

%% parse_hh_mm(Str) ->
%%     [HH, MM|_] = string:tokens(Str, ":"),
%%     {list_to_integer(HH), list_to_integer(MM), 0}.

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
