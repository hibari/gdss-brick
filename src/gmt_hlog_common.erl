%%%-------------------------------------------------------------------
%%% Copyright: (c) 2009-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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

-module(gmt_hlog_common).
-include("applog.hrl").


-behaviour(gen_server).

-include("brick.hrl").
-include("brick_public.hrl").
-include("gmt_hlog.hrl").
-include_lib("kernel/include/file.hrl").

-define(WB_COUNT, 200). % For write_back buffering.

%% API
-export([start_link/1, hlog_pid/1, stop/1,
         register_local_brick/2, unregister_local_brick/2,
         permanently_unregister_local_brick/2,
         full_writeback/0, full_writeback/1,
         get_all_registrations/0, get_all_registrations/1
        ]).
%% Scavenger API
-export([start_scavenger_commonlog/1, stop_scavenger_commonlog/0,
         resume_scavenger_commonlog/1, resume_scavenger_commonlog/2,
         scavenger_commonlog/1                  % Not commonly used
        ]).

%% Checksum error API
-export([sequence_file_is_bad/2]).

%% Use start/1 only if you know what you're doing; otherwise use start_link/1.
-export([start/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Pass-through to gmt_hlog.
-export([log_file_path/2, log_file_path/3, open_log_file/3,
         read_hunk_summary/5, write_hunk/7]).

%%%----------------------------------------------------------------------
%%% Types/Specs/Records
%%%----------------------------------------------------------------------

-opaque from() :: {pid(),term()}.

-record(state, {
          name                            :: file:name(),
          hlog_name                       :: file:name(),
          hlog_pid                        :: pid(),
          hlog_dir                        :: dirname(),
          last_seqnum                     :: seqnum(),                 % SeqNum of last writeback
          last_offset                     :: offset(),                 % Offset of last writeback
          reg_dict=orddict:new()          :: orddict(),
          tref                            :: timer:tref(),
          scavenger_tref                  :: timer:tref() | undefined,
          async_writeback_pid             :: pid() | undefined,
          async_writeback_reqs=[]         :: list(from()),
          async_writebacks_next_round=[]  :: list(from()),
          dirty_buffer_wait               :: non_neg_integer(),        % seconds
          short_long_same_dev_p           :: boolean(),
          first_writeback                 :: boolean()
                                             }).

-record(wb, {
          exactly_count = 0           :: non_neg_integer(),
          exactly_ts = []             :: list(eee()),
          relocate_count = 0          :: non_neg_integer(),
          relocate_ts = []            :: list(eee())
                                         }).

-type orddict() :: list().

-type props() :: list({common_log_name,servername()}).

-spec start_link(props()) -> {ok,pid()} | {error,term()} | ignore.
-spec start(props()) -> {ok,pid()} | {error,term()} | ignore.
-spec hlog_pid(server()) -> pid().
-spec stop(server()) -> ok | {error,term()}.
-spec register_local_brick(server(), brickname()) -> ok | {error,term()}.
-spec unregister_local_brick(server(), brickname()) -> ok.
-spec permanently_unregister_local_brick(server(), brickname()) -> ok.
-spec full_writeback(server()) -> ok | {error,term()}.
-spec get_all_registrations() -> list(atom()).
-spec get_all_registrations(server()) -> list(atom()).

-spec scavenger_commonlog(#scav{}) -> term().

-spec sequence_file_is_bad(seqnum(), offset()) -> ok.

-spec open_log_file(dirname(), seqnum(), openmode()) -> {ok, file:fd()} | {error, atom()}.
-spec write_hunk(server(), brickname(), hlogtype(), key(), typenum(), CBlobs::blobs(), UBlobs::blobs()) -> {ok, seqnum(), offset()} | {hunk_too_big, len()} | no_return().

-spec log_file_path(dirname(), seqnum()) -> dirname().


%%%===================================================================
%%% API
%%%===================================================================

start_link(PropList) ->
    gen_server:start_link(?MODULE, PropList, []).

start(PropList) ->
    gen_server:start(?MODULE, PropList, []).

hlog_pid(Server) ->
    gen_server:call(Server, {hlog_pid}, 300*1000).

stop(Server) ->
    gen_server:call(Server, {stop}).

register_local_brick(Server, LocalBrick) when is_atom(LocalBrick) ->
    gen_server:call(Server, {register_local_brick, LocalBrick}).

unregister_local_brick(Server, LocalBrick) when is_atom(LocalBrick) ->
    gen_server:call(Server, {unregister_local_brick, LocalBrick}).

permanently_unregister_local_brick(Server, LocalBrick) when is_atom(LocalBrick) ->
    gen_server:call(Server, {permanently_unregister_local_brick, LocalBrick}).

full_writeback() ->
    full_writeback(?GMT_HLOG_COMMON_LOG_NAME).

full_writeback(Server) ->
    gen_server:call(Server, {full_writeback}, 300*1000).

get_all_registrations() ->
    get_all_registrations(?GMT_HLOG_COMMON_LOG_NAME).

get_all_registrations(Server) ->
    gen_server:call(Server, {get_all_registrations}, 300*1000).

log_file_path(A, B) ->
    gmt_hlog:log_file_path(A, B).

log_file_path(A, B, C) ->
    gmt_hlog:log_file_path(A, B, C).

open_log_file(A, B, C) ->
    gmt_hlog:open_log_file(A, B, C).

read_hunk_summary(A, B, C, D, E) ->
    gmt_hlog:read_hunk_summary(A, B, C, D, E).

write_hunk(A, B, C, D, E, F, G) ->
    gmt_hlog:write_hunk(A, B, C, D, E, F, G).

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
    CName = proplists:get_value(common_log_name, PropList, commonLogDefault),
    undefined = proplists:get_value(dir, PropList), % sanity: do not use
    try
        case whereis(CName) of
            undefined ->
                register(CName, self()),
                process_flag(priority, high),
                process_flag(trap_exit, true),
                (catch exit(whereis(gmt_hlog:log_name2reg_name(CName)), kill)),
                timer:sleep(150),
                {ok, Log} = gmt_hlog:start_link([{name, CName}|PropList]),
                LogDir = gmt_hlog:log_name2data_dir(CName),
                {SeqNum, Off} = read_flush_file(LogDir),
                self() ! do_sync_writeback,
                %% Use timer instead of brick_itimer: it allows QC testing
                %% without GDSS app running, and there's only one of
                %% these things per app.
                {ok, TRef} = timer:send_interval(1000, do_async_writeback),
                ScavengerTRef =
                    case proplists:get_value(suppress_scavenger, PropList) of
                        undefined ->
                            {ok, STR} = schedule_next_daily_scavenger(),
                            STR;
                        _ ->
                            undefined
                    end,
                {ok, DirtySec} = gmt_config_svr:get_config_value_i(
                                   brick_dirty_buffer_wait, 60),
                SameDevP = short_long_same_dev_p(LogDir),

                {ok, #state{name = CName, hlog_pid = Log, hlog_name = CName,
                            hlog_dir = LogDir,
                            last_seqnum = SeqNum, last_offset = Off,
                            tref = TRef,
                            scavenger_tref = ScavengerTRef,
                            dirty_buffer_wait = DirtySec,
                            short_long_same_dev_p = SameDevP,
                            first_writeback = true
                           }};
            _Pid ->
                ignore
        end
    catch
        _X:_Y ->
            ?APPLOG_ALERT(?APPLOG_APPM_076,"~s: init error: ~p ~p at ~p\n",
                          [?MODULE, _X, _Y, erlang:get_stacktrace()]),
            ignore
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
handle_call({unregister_local_brick, Brick}, _From, State) ->
    {Reply, NewState} = do_unregister_local_brick(Brick, State),
    {reply, Reply, NewState};
handle_call({permanently_unregister_local_brick, Brick}, _From, State) ->
    {Reply, NewState} = do_permanently_unregister_local_brick(Brick, State),
    {reply, Reply, NewState};
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
handle_info(do_async_writeback, #state{async_writeback_pid = undefined
                                       , async_writeback_reqs = []
                                       , async_writebacks_next_round = NextReqs
                                      } = State) ->
    ParentPid = self(),
    %% Pattern-matching should assure that do_sync_writeback() has
    %% encountered no errors if it returns.  A pattern match failure
    %% will kill the child, so it can't update our parent.
    {Pid, _Ref} = spawn_monitor(fun() ->
                                        S = do_sync_writeback(State),
                                        ParentPid ! {async_writeback_finished,
                                                     self(),
                                                     S#state.last_seqnum,
                                                     S#state.last_offset},
                                        exit(normal)
                                end),
    {noreply, State#state{async_writeback_pid = Pid, async_writeback_reqs = NextReqs, async_writebacks_next_round = []}};
handle_info(do_async_writeback, #state{async_writeback_pid = Pid, async_writeback_reqs = Reqs} = State)
  when is_pid(Pid) ->
    if Reqs =:= [] ->
            %% Last async writeback proc hasn't finished yet.
            ?APPLOG_WARNING(?APPLOG_APPM_077,"~s: async writeback proc ~p hasn't finished yet\n",
                            [?MODULE, Pid]);
       true ->
            %% don't warn if in progress due to an external request
            noop
    end,
    {noreply, State};
handle_info({async_writeback_finished, Pid, NewSeqNum, NewOffset},
            #state{async_writeback_pid = Pid, async_writeback_reqs = Reqs
                   , last_seqnum = SeqNum, last_offset = Offset} = State) ->
    %% update state
    NewState = if NewSeqNum > SeqNum orelse
                  (NewSeqNum == SeqNum andalso NewOffset > Offset) ->
                       State#state{last_seqnum = NewSeqNum, last_offset = NewOffset};
                  (NewSeqNum == SeqNum andalso NewOffset == Offset) ->
                       State;
                  true ->
                       ?E_INFO("Notice: last seq/off ~p ~p new seq/off ~p ~p\n",
                               [SeqNum, Offset, NewSeqNum, NewOffset]),
                       %% exit({hibari_debug, SeqNum, Offset, NewSeqNum, NewOffset}),
                       State
               end,
    %% reply to callers
    [ gen_server:reply(From, ok) || From <- Reqs ],
    {noreply, NewState#state{async_writeback_reqs = []}};
handle_info(start_daily_scavenger, State) ->
    timer:sleep(2*1000),
    {ok, ScavengerTRef} = schedule_next_daily_scavenger(),
    Percent = gmt_config:get_config_value_i(
                brick_skip_live_percentage_greater_than, 90),
    WorkDir = gmt_config:get_config_value(
                brick_scavenger_temp_dir, "/tmp"),
    PropList = [destructive,
                {skip_live_percentage_greater_than, Percent},
                {work_dir, WorkDir}],
    %% Self-deadlock with get_all_registrations(), must use worker.
    spawn(fun() -> start_scavenger_commonlog(PropList) end),
    {noreply, State#state{scavenger_tref = ScavengerTRef}};
handle_info({'DOWN', _Ref, _, Pid, Reason},
            #state{async_writeback_pid = Pid, async_writeback_reqs = Reqs} = State) ->
    %% schedule next round
    schedule_async_writeback(State),
    %% if any, reply to callers with error
    [ gen_server:reply(From, {error,Reason}) || From <- Reqs ],
    {noreply, State#state{async_writeback_pid = undefined, async_writeback_reqs = []}};
handle_info({'DOWN', Ref, _, _, _}, #state{reg_dict = Dict} = State) ->
    NewDict = orddict:filter(fun(_K, V) when V /= Ref -> true;
                                (_, _)                -> false
                             end, Dict),
    {noreply, State#state{reg_dict = NewDict}};
handle_info({'EXIT', Pid, Reason}, #state{hlog_pid = Pid} = State) ->
    {stop,Reason,State#state{hlog_pid = undefined}};
handle_info(_Info, State) ->
    ?APPLOG_ALERT(?APPLOG_APPM_078,"~s:handle_info: ~p got msg ~p\n",
                  [?MODULE, self(), _Info]),
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
    if ScavengerTRef =/= undefined ->
            timer:cancel(ScavengerTRef);
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

schedule_async_writeback(From,#state{async_writebacks_next_round=NextReqs}=S) ->
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
    ?DBG_TLOGx({do_sync_writeback, end_seq_and_offset, EndSeqNum, EndOffset}),
    {ok, Count1} = do_metadata_hunk_writeback(S#state.last_seqnum,
                                              S#state.last_offset,
                                              EndSeqNum, EndOffset, S),
    {ok, Bytes2} = do_bigblob_hunk_writeback(S#state.last_seqnum,
                                             S#state.last_offset, S),
    ?DBG_TLOGx({do_sync_writeback, counts, Count1, Bytes2}),

    %% OK, we're written everything back to where they need to be.
    %% However, all of those writes were asynchronous and probably
    %% have not reached stable storage.  Therefore, we wait for the OS
    %% to flush it sometime between now and S#state.dirty_buffer_wait
    %% seconds from now.  We'll cheat by simply recording this
    %% particular writeback that many seconds in the future.
    if Count1 + Bytes2 > 0 ->
            spawn_future_tasks_after_dirty_buffer_wait(EndSeqNum, EndOffset, S);
       true ->
            ok
    end,
    S#state{last_seqnum = EndSeqNum, last_offset = EndOffset}.

do_metadata_hunk_writeback(OldSeqNum, OldOffset, StopSeqNum, StopOffset, S_ro)->
    FiltFun = fun(N) ->
                      N >= OldSeqNum
              end,
    Fun = fun(#hunk_summ{seq = SeqNum, off = Offset} = _H, _FH, _WB)
                when SeqNum == OldSeqNum, Offset < OldOffset ->
                  %% It would be really cool if we could advance the
                  %% file pos of FH, but our caller is keeping track
                  %% of its own file offsets, so calling
                  %% file:position/2 on this file handle doesn't do
                  %% anything useful, alas.  Instead, we'll use the
                  %% magic return value to tell fold_a_file() to use a
                  %% new offset for the next iteration.
                  ?DBG_TLOGx({do_metadata_hunk_writeback, new_offset, SeqNum, OldOffset}),
                  {{{new_offset, OldOffset}}};
             (#hunk_summ{seq = SeqNum, off = Offset} = _H, _FH, WB)
                when {SeqNum, Offset} >= {StopSeqNum, StopOffset} ->
                  %% Do nothing here: our folding has moved past where
                  %% we need to process ... this hunk will be
                  %% processed later.
                  ?DBG_TLOGx({do_metadata_hunk_writeback, stop, SeqNum, Offset}),
                  WB;
             (#hunk_summ{type = ?LOCAL_RECORD_TYPENUM, u_len = [BLen]} = H, FH,
              #wb{exactly_count = Count, exactly_ts = Ts} = WB) ->
                  ?DBG_TLOGx({do_metadata_hunk_writeback, metadata, H#hunk_summ.seq, H#hunk_summ.off}),
                  UBlob = gmt_hlog:read_hunk_member_ll(FH, H, undefined, 1),
                  if size(UBlob) /= BLen ->
                          %% This should never happen.
                          QQQPath = "/tmp/foo.QQQbummer."++integer_to_list(element(3,now())),
                          file:write_file(QQQPath, term_to_binary([{args, [OldSeqNum, OldOffset, StopSeqNum, StopOffset, S_ro]}, {h, H}, {wb, WB}, {info, process_info(self())}])),
                          ?APPLOG_WARNING(?APPLOG_APPM_079,"DBG: See ~p\n", [QQQPath]),
                          ?APPLOG_WARNING(?APPLOG_APPM_080,
                                          "DBG: ~p ~p wanted blob size ~p but got ~p\n",
                                          [H#hunk_summ.seq,H#hunk_summ.off,BLen,size(UBlob)]);
                     true ->
                          ok
                  end,
                  T = binary_to_term(UBlob),
                  %% This tuple is sortable the way we need it, as-is.
                  WB#wb{exactly_count = Count + 1, exactly_ts = [T|Ts]};
             (_H, _FH, WB) ->
                  ?DBG_TLOGx({do_metadata_hunk_writeback, bigblob_hunk, _H#hunk_summ.seq, _H#hunk_summ.off}),
                  %% These are copied by do_bigblob_hunk_writeback() instead.
                  WB
          end,
    %% START = now(),  % Test pathological GC

    {WB2, ErrList} =
        gmt_hlog:fold(shortterm, S_ro#state.hlog_dir, Fun, FiltFun, #wb{}),

    %% END = now(),
    %%io:format("exactly_ts length = ~p, elapsed ~p\n", [length(WB2#wb.exactly_ts), timer:now_diff(END, START)]),
    %% with R13B04: exactly_ts length = 83053, elapsed   3816906
    %% with R13B03: exactly_ts length = 83053, elapsed 156089269

    if ErrList == [] ->
            if not is_record(WB2, wb) -> % Sanity check
                    ?APPLOG_ALERT(?APPLOG_APPM_081,"~s: fold term: ~p\n",
                                  [?MODULE, WB2]);
               true ->
                    ok
            end,
            ok = write_back_exactly_to_logs(WB2#wb.exactly_ts, S_ro),
            {ok, WB2#wb.exactly_count};
       true ->
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
            ?APPLOG_WARNING(?APPLOG_APPM_082,"~s: ErrList = ~p\n", [?MODULE, ErrList]),
            ErrStr = lists:flatten(io_lib:format("~P", [ErrList, 25])),
            gmt_util:set_alarm({?MODULE, ErrStr},
                               "Potential data-corrupting error occurred, "
                               "check all log files for details"),
            if S_ro#state.first_writeback == false ->
                    exit({error_list, S_ro#state.hlog_dir, ErrList});
               true ->
                    %% If we exit() here when we're doing our initial
                    %% writeback after init time, then we can never
                    %% start.  It's better to start than to never be
                    %% able to start.
                    {ok, WB2#wb.exactly_count}
            end
    end.

peek_first_brick_name(Ts) ->
    case Ts of [T|_] -> element(2, T);
        []    -> ''
    end.

write_back_exactly_to_logs(Ts, S_ro) ->
    SortTs = lists:sort(Ts),
    FirstBrickName = peek_first_brick_name(SortTs),
    write_back_to_local_log(SortTs, undefined, 0,
                            undefined, FirstBrickName,
                            [], ?WB_COUNT, S_ro).

%%
%% TODO: This func/Aegean stable needs a major cleanup/refactoring/Hercules.
%%

write_back_to_local_log([{eee, LocalBrickName, SeqNum, Offset, _Key, _TypeNum,
                          H_Len, H_Bytes}|Ts] = AllTs, LogSeqNum, LogFH_pos,
                        I_LogFH, LastBrickName,
                        I_TsAcc, Count, S_ro)
  when Count > 0, LocalBrickName == LastBrickName ->
    {LogFH, TsAcc} =
        if LogSeqNum == SeqNum, I_LogFH /= undefined ->
                {I_LogFH, I_TsAcc};
           true ->
                if I_LogFH /= undefined ->
                        write_stuff(I_LogFH, lists:reverse(I_TsAcc));
                   true ->
                        ok
                end,
                ?DBG_TLOGx({write_back_to_local_log, close, I_LogFH}),
                (catch file:close(I_LogFH)),
                LPath = gmt_hlog:log_name2data_dir(
                          LocalBrickName),
                {ok, Lfh} = open_log_file_mkdir(LPath, SeqNum,
                                                [read,write,binary]),
                _NBytes = check_hlog_header(Lfh),
                ?DBG_TLOGx({write_back_to_local_log, open, Lfh, _NBytes}),
                {Lfh, []}
        end,
    if TsAcc == [] ->
            {ok, Offset} = file:position(LogFH, {bof, Offset}),
            ?DBG_TLOGx({write_back_to_local_log, new_pos, LogFH, Offset}),
            write_back_to_local_log(Ts, SeqNum, Offset + H_Len,
                                    LogFH, LocalBrickName,
                                    [H_Bytes|TsAcc],
                                    Count - 1, S_ro);

       LogSeqNum == SeqNum, LogFH_pos == Offset ->
            ?DBG_TLOGx({write_back_to_local_log, append_pos, LogFH, Offset}),
            write_back_to_local_log(Ts, SeqNum, Offset + H_Len,
                                    LogFH, LocalBrickName,
                                    [H_Bytes|TsAcc],
                                    Count - 1, S_ro);
       true ->
            %% Writeback!
            ?DBG_TLOGx({write_back_to_local_log, writeback, LogFH, Offset}),
            write_back_to_local_log(AllTs, SeqNum, LogFH_pos, LogFH,
                                    LastBrickName, TsAcc, 0, S_ro)
    end;

%% Writeback what we have at the current LogFH file position (already
%% set!), then reset accumulators & counter and resume iteration.
write_back_to_local_log(AllTs, SeqNum, _LogFH_pos, LogFH,
                        LastBrickName, TsAcc, _Count, S_ro)
  when LogFH /= undefined ->
    write_stuff(LogFH, lists:reverse(TsAcc)),
    case peek_first_brick_name(AllTs) of
        LastBrickName ->
            ?DBG_TLOGx({write_back_to_local_log, peek_last, LogFH}),
            write_back_to_local_log(AllTs, SeqNum, 0, LogFH,
                                    LastBrickName, [], ?WB_COUNT, S_ro);
        OtherBrickName ->
            ?DBG_TLOGx({write_back_to_local_log, other_close, LogFH}),
            (catch file:close(LogFH)),
            write_back_to_local_log(AllTs, undefined, 0, undefined,
                                    OtherBrickName, [], ?WB_COUNT, S_ro)
    end;

%% No more input, perhaps one last writeback?
write_back_to_local_log([] = AllTs, SeqNum, FH_pos, LogFH,
                        LastBrickName, TsAcc, _Count, S_ro) ->
    if TsAcc == [] ->
            ?DBG_TLOGx({write_back_to_local_log, empty_close, LogFH}),
            (catch file:close(LogFH)),
            ok;
       true ->
            %% Writeback one last time.
            ?DBG_TLOGx({write_back_to_local_log, one_last_time, LogFH}),
            write_back_to_local_log(AllTs, SeqNum, FH_pos, LogFH,
                                    LastBrickName, TsAcc, 0, S_ro)
    end.

check_hlog_header(FH) ->
    FileHeader = gmt_hlog:file_header_version_1(),
    file:position(FH, {bof, 0}),
    case file:read(FH, erlang:iolist_size(FileHeader)) of
        %% Kosher cases only
        {ok, FileHeader} ->
            ok;
        eof -> % File is 0 bytes or at least smaller than header
            file:position(FH, {bof, 0}),
            ok = file:write(FH, FileHeader),
            created
    end.

open_log_file_mkdir(Dir, SeqNum, Options) when SeqNum > 0 ->
    case gmt_hlog:open_log_file(Dir, SeqNum, Options) of
        {error, enoent} ->
            io:format("HRM, e ~p ~p, ", [Dir, SeqNum]),
            file:make_dir(Dir), % FIX later: ICKY assumption!! SeqNum > 0.
            file:make_dir(Dir ++ "/s"), % FIX later: ICKY assumption!!
            ?DBG_TLOGx({open_log_file_mkdir, Dir}),
            open_log_file_mkdir(Dir, SeqNum, Options);
        Res ->
            Res
    end.

write_stuff(LogFH, LogBytes) ->
    ?DBG_TLOGx({write_stuff, LogFH, erlang:iolist_size(LogBytes)}),
    ok = file:write(LogFH, LogBytes).

do_register_local_brick(Brick, #state{reg_dict = _Dict} = S) ->
    _ = file:make_dir(brick_registration_dir(S)),
    ok = create_registration_file(Brick, S),
    {ok, S}.

do_unregister_local_brick(_Brick, #state{reg_dict = _Dict} = S) ->
    {ok, S}.

do_permanently_unregister_local_brick(Brick, S) ->
    case lists:member(Brick, do_get_all_registrations(S)) of
        true ->
            ok = delete_registration_file(Brick, S),
            {ok, S};
        false ->
            {not_found, S}
    end.

spawn_future_tasks_after_dirty_buffer_wait(EndSeqNum, EndOffset, S) ->
    %% Advancing the common log's sequence number isn't really a
    %% future task, but doing it asyncly is a good idea.
    spawn(fun() ->
                  {ok, MaxMB} = gmt_config_svr:get_config_value_i(
                                  brick_max_log_size_mb, 100),
                                                %if EndOffset > MaxMB * 1024 * 1024 ->
                  if EndOffset > MaxMB * 1024 * 1024 div 2 ->
                          ?DBG_TLOGx({spawn_future_tasks_after_dirty_buffer_wait, advance,1}),
                          %% NOTE: not checking for success or failure
                          %% ... it doesn't matter
                          _ = gmt_hlog:advance_seqnum(S#state.hlog_pid, 1);
                     true ->
                          ok
                  end,
                  timer:sleep(S#state.dirty_buffer_wait * 1000),
                  ?DBG_TLOGx({spawn_future_tasks_after_dirty_buffer_wait, flush}),
                  ok = write_flush_file(EndSeqNum, EndOffset, S),

                  if EndSeqNum > S#state.last_seqnum ->
                          clean_old_seqnums(EndSeqNum, S);
                     true ->
                          ok
                  end,
                  ?DBG_TLOGx({spawn_future_tasks_after_dirty_buffer_wait, exit}),
                  exit(normal)
          end).

clean_old_seqnums(EndSeqNum, S) ->
    OldSeqs = [N || N <- gmt_hlog:find_current_log_seqnums(S#state.hlog_dir),
                    N < EndSeqNum],
    if S#state.short_long_same_dev_p ->
            ?DBG_TLOGx({clean_old_seqnums, same, OldSeqs}),
            [gmt_hlog:move_seq_to_longterm(S#state.hlog_pid, N) ||
                N <- OldSeqs];
       true ->
            ?DBG_TLOGx({clean_old_seqnums, diff, OldSeqs}),
            [file:delete(gmt_hlog:log_file_path(S#state.hlog_dir, N)) ||
                N <- OldSeqs],
            ?DBG_TLOGx({clean_old_seqnums, del_major_done})
    end.

short_long_same_dev_p(HLogDir) ->
    ShortDir = filename:dirname(gmt_hlog:log_file_path(HLogDir, 1)),
    LongDir = filename:dirname(gmt_hlog:log_file_path(HLogDir, -1)),
    {ok, ShortFI} = file:read_file_info(ShortDir),
    {ok, LongFI} = file:read_file_info(LongDir),
    ShortFI#file_info.major_device == LongFI#file_info.major_device andalso
        ShortFI#file_info.minor_device == LongFI#file_info.minor_device.

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
    ToDos = lists:map(fun({Seq, Size}) when Seq == LastSeqNum ->
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
                          ?APPLOG_ALERT(?APPLOG_APPM_083,
                                        "~s: copy_parts ~p ~p ~p: ~p ~p\n",
                                        [?MODULE, SeqNum, ByteOffset, NumBytes, _X, _Y]),
                          exit({copy_parts, _X, _Y})
                  after begin
                            erlang:garbage_collect(),
                            [catch file:close(erase(X)) || X <- [srcfh___,dstfh___]]
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

delete_registration_file(BrickName, S) ->
    file:delete(brick_registration_file(BrickName, S)).

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
    Finfolog = brick_ets:make_info_log_fun(PropList0),
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
            [gmt_util:clear_alarm({scavenger, T}) || T <- BrNds],
            Bigs = [Br || {Br, Os} <- BrOpts,
                          proplists:get_value(bigdata_dir, Os, false) /= false],
            Finfolog("SCAV: start with ~p\n", [Bigs]),
            do_start_scavenger_commonlog2(Bigs, PropList0);
        ErrBricks ->
            ErrNds = [{Br, node()} || Br <- ErrBricks],
            Msg = "Scavenger may not execute until all bricks are running.",
            [gmt_util:set_alarm({scavenger, T}, Msg) || T <- ErrNds],
            ?APPLOG_ALERT(?APPLOG_APPM_084,"Bricks ~p are not available, scavenger "
                          "aborted\n",
                          [ErrNds]),
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

    Destructive = proplists:get_value(destructive, PropList, true),
    SkipReads = case proplists:get_value(skip_reads, PropList, false) of
                    true -> not Destructive;
                    _    -> false
                end,
    MaximumLivePt = case proplists:get_value(skip_live_percentage_greater_than,
                                             PropList) of
                        N when N >= 0, N =< 100 -> N;
                        _                       -> 100 % Examine all files
                    end,
    ThrLimit = 600*1000*1000,
    ThrottleBytes = case proplists:get_value(throttle_bytes, PropList) of
                        T when is_integer(T), T >= 0 ->
                            T;
                        _ -> element(
                               2, gmt_config_svr:get_config_value_i(
                                    brick_scavenger_throttle_bytes, ThrLimit))
                    end,
    SorterSize = proplists:get_value(sorter_size, PropList, 16*1024*1024),
    WorkDir = proplists:get_value(work_dir, PropList, "./scavenger-work-dir"),
    Finfolog = brick_ets:make_info_log_fun(PropList),

    SA = #scav{options = PropList,
               work_dir = WorkDir,
               wal_mod = gmt_hlog_common,
               destructive = Destructive,
               skip_reads = SkipReads,
               name = ?GMT_HLOG_COMMON_LOG_NAME,
               log = gmt_hlog:log_name2reg_name(?GMT_HLOG_COMMON_LOG_NAME),
               log_dir = gmt_hlog:log_name2data_dir(?GMT_HLOG_COMMON_LOG_NAME),
               %% For common log, any sequence number at or beyond
               %% CurSeq is off-limits.
               last_check_seq = CurSeq,
               skip_live_percentage_greater_than = MaximumLivePt,
               sorter_size = SorterSize, bricks = Bricks,
               throttle_bytes = ThrottleBytes,
               log_fun = Finfolog,
               phase10_fun = fun scavenger_commonlog_bottom10/7,
               update_locations = fun update_locations_commonlog/2},
    ?DBG_TLOGx({scavenger, SA#scav.name, last_check_seq, CurSeq}),
    Fdoit = fun() -> scavenger_commonlog(SA),
                     exit(normal)
            end,
    Pid = spawn(fun() -> link_catch_shutdowns(Fdoit) end),
    {ok, Pid}.

resume_scavenger_commonlog(WorkDir) ->
    resume_scavenger_commonlog(WorkDir, fun scavenger_commonlog_bottom10/7).

resume_scavenger_commonlog(WorkDir, Phase10Func) ->
    case scav_read_resume(WorkDir) of
        {SA, Finfolog, TempDir, TmpList4_a, Bytes1, Del1, BytesBefore} ->
            %% Bottom status file contains seqnum that was in progress
            %% when shutdown/interrupted/crashed/whatever.
            TmpList4 = case file:consult(scav_bottomstatus_path(WorkDir)) of
                           {ok, [DoneSeq]} ->
                               lists:dropwhile(
                                 fun({X, _}) when X /= DoneSeq -> true;
                                    (_) -> false
                                 end, TmpList4_a);
                           _ ->
                               TmpList4_a
                       end,
            Finfolog("SCAV: resuming at sequence ~p\n",
                     [if TmpList4 == [] -> []; true -> hd(TmpList4) end]),
            Fdoit = fun() ->
                            scav_exit_if_someone_else(Finfolog),
                            Phase10Func(SA, Finfolog, TempDir, TmpList4,
                                        Bytes1, Del1, BytesBefore)
                    end,
            Pid = spawn(fun() -> link_catch_shutdowns(Fdoit) end),
            {ok, Pid};
        _ ->
            unable_to_resume
    end.

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
        exit:Fine when Fine == normal; Fine == stopping_on_shutdown_request;
                       Fine == exclusive_wait_exit ->
            ok;
        exit:{_, _, Fine}
          when Fine == normal; Fine == stopping_on_shutdown_request;
               Fine == exclusive_wait_exit ->
            ok; % smart_exceptions
        X:Y ->
            ?APPLOG_ALERT(?APPLOG_APPM_085,"Scavenger ~p error: ~p ~p @ ~p\n",
                          [self(), X, Y, erlang:get_stacktrace()])
    end.

scav_resumeok_path(WorkDir) ->
    WorkDir ++ "/resume-ok".

scav_bottomstatus_path(WorkDir) ->
    WorkDir ++ "/working".

scav_write_resume(SA, Finfolog, TempDir, TmpList4, Bytes1, Del1, BytesBefore)->
    T = {SA, Finfolog, TempDir, TmpList4, Bytes1, Del1, BytesBefore},
    Tbin = term_to_binary(T),
    Fres = fun(FH) -> ok = file:write(FH, Tbin) end,
    ok = brick_server:replace_file_sync(scav_resumeok_path(TempDir), Fres).

scav_read_resume(WorkDir) ->
    case file:read_file_info(scav_resumeok_path(WorkDir)) of
        {ok, _} ->
            {ok, Bin} = file:read_file(scav_resumeok_path(WorkDir)),
            T = binary_to_term(Bin),
            true = (7 == size(T)), % Sanity check
            T;
        _ ->
            error
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

scav_check_shutdown() ->
    case scav_stop_signal_p() of
        true ->
            ?APPLOG_INFO(?APPLOG_APPM_086,
                         "SCAV: pid ~p received shutdown request\n", [self()]),
            exit(stopping_on_shutdown_request);
        false ->
            ok
    end.

scav_exit_if_someone_else(Finfolog) ->
    F_excl_wait = fun() ->
                          Finfolog("Scavenger ~p exiting.\n", [self()]),
                          exit(exclusive_wait_exit)
                  end,
    F_excl_go = fun() ->
                        Finfolog("Scavenger ~p starting now.\n", [self()])
                end,
    brick_ets:really_cheap_exclusion(scav_excl_name(), F_excl_wait, F_excl_go).

scavenger_commonlog(SA) ->
    ?DBG_TLOGx({scavenger_start, SA#scav.name, start}),
    WalMod = SA#scav.wal_mod,
    Finfolog = SA#scav.log_fun,
    Ffile_size_iter = fun({_SeqNum, Path}, {D1, B1}) ->
                              {ok, I} = file:read_file_info(Path),
                              {D1 + 1, B1 + I#file_info.size}
                      end,

    %% Step -1: Make certain we're the only scavenger running.
    if SA#scav.exclusive_p -> scav_exit_if_someone_else(Finfolog);
       true                -> ok
    end,

    %% Step 0: Set up.
    TempDir = SA#scav.work_dir,
    os:cmd("/bin/rm -rf " ++ TempDir),
    {ok, TempDir} = {file:make_dir(TempDir), TempDir},
    scav_check_shutdown(),

    %% Step 1: Find all keys and their raw storage locations via
    %% get_many hackery.
    %% Dict key = Sequence #
    %% Dict val = {LiveBytes, [{BrickName, Key, TS, Offset}, ...]}
    FirstKey = ?BRICK__GET_MANY_FIRST,
    Fs = [get_many_raw_storetuples],
    F_k2d = fun({_BrickName, _Key, _TS, {0, 0}}, Dict) ->
                    Dict;
               ({BrickName, Key, TS, {SeqNum, Offset}, ValLen, ExpTime, Flags},
                Dict) ->
                    {Bytes, L} = case dict:find(SeqNum, Dict) of
                                     {ok, {B_, L_}} -> {B_, L_};
                                     error          -> {0, []}
                                 end,
                    %% Tpl sorts naturally as we need it to if Offset is 1st
                    Tpl = {Offset, BrickName, Key, TS, ValLen, ExpTime, Flags},
                    dict:store(SeqNum, {Bytes + ValLen, [Tpl|L]}, Dict)
            end,
    %% Each disk_log file, one per log sequence file, will contain
    %% terms that match one of these patterns:
    %%    {live_bytes, Bytes}
    %%    {BrickName, Key, TS, Offset}

    F_lump = fun(Dict) ->
                     scav_check_shutdown(),
                     List = dict:to_list(Dict),
                     lists:foreach(
                       fun({K_seqnum, {LiveBytes, L_BrKeyTsOff}}) ->
                               Path = TempDir ++ "/" ++
                                   integer_to_list(K_seqnum) ++ ".",
                               {ok, Log} = disk_log:open([{name,Path},
                                                          {file,Path},
                                                          {mode, read_write}]),
                               ok = disk_log:log(Log, {live_bytes, LiveBytes}),
                               ok = disk_log:log_terms(Log, L_BrKeyTsOff),
                               ok = disk_log:close(Log)
                       end, List),
                     dict:new()
             end,
    [{Br, ok} =
         {Br, brick_ets:scavenger_get_keys(Br, Fs, FirstKey, F_k2d, F_lump)}
     || Br <- SA#scav.bricks],
    Finfolog("SCAV: ~p finished step 1\n", [SA#scav.name]),
    scav_check_shutdown(),

    %% Step 3: Filter sequence numbers that are not eligible for scavenging.
    FindOut = list_to_binary(os:cmd("find hlog.commonLogServer -name \\*.HLOG")),
    AllSeqs = lists:usort([N || N <- gmt_hlog:get_all_seqnums(SA#scav.log),
                                abs(N) < SA#scav.last_check_seq]),
    Finfolog("SCAV: ~p finished step 3\n", [SA#scav.name]),
    scav_check_shutdown(),

    %% Step 4: Sort each sequence's list.

    %% The original idea was to sort by by descending offset
    %% (i.e. backwards) under the assumption that we'd be truncating
    %% the sequence as we go along to free space quicker than
    %% unlink'ing would.  I didn't want to risk a bug in truncating a
    %% file if an error happened somewhere along the line.  So, we'll
    %% sort ascending, and perhaps OS read-ahead will help a bit?
    %% {shrug}
    %%
    %% Reminder: the tuple created by F_k2d above is:
    %%     {Offset, BrickName, Key, TS, ValLen, ExpTime, Flags}
    Fsort = fun(Path, Acc) ->
                    try
                        scav_check_shutdown(),
                        {ok, Log} = disk_log:open([{name,Path},
                                                   {file,Path}, {mode,read_only}]),
                        Path2 = string:strip(Path, right, $.),
                        {ok, OLog} = disk_log:open([{name,Path2}, {file,Path2}]),
                        ok = file_sorter:sort(brick_ets:file_input_fun(Log, start),
                                              brick_ets:file_output_fun(OLog),
                                              [{format, term},
                                               {size, SA#scav.sorter_size}]),
                        ok = disk_log:close(Log),
                        {ok, Log} = disk_log:open([{name,Path},
                                                   {file,Path}, {mode,read_only}]),
                        Bytes = brick_ets:count_live_bytes_in_log(Log),
                        ok = disk_log:close(Log),
                        ok = file:delete(Path),
                        erlang:garbage_collect(),
                        [{brick_ets:temp_path_to_seqnum(Path2), Bytes}|Acc]
                    catch Err1:Err2 ->
                            ?APPLOG_ALERT(?APPLOG_APPM_109, "SCAV: ~p error processing ~s: ~p ~p at ~p\n",
                                     [SA#scav.name, Path, Err1, Err2,
                                      erlang:get_stacktrace()]),
                            exit(abort)
                    end
            end,
    %% Do this next step in a child process, to try to avoid some
    %% accumulation of garbage, despite the fact that I'm explicitly
    %% calling erlang:garbage_collect().  {sigh}
    ParentPid = self(),
    LsAL1 = list_to_binary(os:cmd("ls -al " ++ TempDir)),
    Pid = spawn_opt(fun() ->
                            X = filelib:fold_files(TempDir, ".*", false, Fsort, []),
                            ParentPid ! {self(), X},
                            unlink(ParentPid),
                            exit(normal)
                    end, [link, {priority, low}]),
    LiveSeqNum_BytesList = receive {X, RemoteVal} when X == Pid-> RemoteVal end,
    LsAL2 = list_to_binary(os:cmd("ls -al " ++ TempDir)),
    LiveDict = dict:from_list(LiveSeqNum_BytesList),
    Finfolog("SCAV: ~p finished step 4: external child proc, sizes ~p and ~p\n", [SA#scav.name, erts_debug:flat_size(LiveSeqNum_BytesList), erts_debug:flat_size(LiveDict)]),
    scav_check_shutdown(),

    %% Step 5: Identify sequences that contains 0 live hunks.
    %% Note: Because of movement of sequence #s from shortterm to longterm
    %%       areas, we need to check both positive & negative
    DeadSeqs = lists:sort([Seq || Seq <- AllSeqs,
                                  dict:find(Seq, LiveDict) == error
                                      andalso dict:find(-Seq, LiveDict) == error]),
    ?DBG_TLOGx({scavenger, SA#scav.name, dead_seqs, DeadSeqs}),
    DeadPaths = lists:map(fun(SeqNum) ->
                                  brick_ets:which_path(SA#scav.log_dir,
                                                       WalMod, SeqNum)
                          end, DeadSeqs),
    {Del1, Bytes1} = lists:foldl(Ffile_size_iter, {0, 0}, DeadPaths),
    %% Don't actually delete the files here, pass them to bottom half.
    Finfolog("SCAV: ~p finished step 5\n", [SA#scav.name]),
    scav_check_shutdown(),

    %% Step 6: Filter out any live sequences that contain more than
    %% the maximum amount of live/in-use space.  This calculation is a
    %% bit inaccurate because the in-memory byte counts do not include
    %% gmt_hlog overhead (header descriptions, etc.), but we'll be
    %% close enough.
    LiveSeqs = AllSeqs -- DeadSeqs,
    LiveSeqsAbs = lists:sort([abs(N) || N <- LiveSeqs]),
    ?DBG_TLOGx({scavenger, SA#scav.name, all_seqs, AllSeqs}),
    ?DBG_TLOGx({scavenger, SA#scav.name, live_seqs, LiveSeqs}),
    TmpList3 = lists:sort([T || {SeqNum, _} = T <- LiveSeqNum_BytesList,
                                lists:member(SeqNum, LiveSeqsAbs)]),
    SeqSizes = lists:map(
                 fun(SeqNum) ->
                         {_, Path} = brick_ets:which_path(SA#scav.log_dir,
                                                          WalMod, SeqNum),
                         {ok, FI} = file:read_file_info(Path),
                         {SeqNum, FI#file_info.size}
                 end, LiveSeqsAbs),
    put(write_lim, 0),
    {_, TmpList4} =
        lists:unzip(lists:filter(
                      fun({{SeqNum1, FileSize}, {SeqNum2, Bytes}})
                            when abs(SeqNum1) == abs(SeqNum2) ->
                              Finfolog("SCAV: ~p: seq ~p "
                                       "percent ~p",
                                       [SA#scav.name, SeqNum1,
                                        (Bytes / FileSize)*100]),
                              Lim = get(write_lim),
                              if Lim rem 200 == 0 -> timer:sleep(1000);
                                 true             -> ok
                              end,
                              put(write_lim, Lim + 1),
                              (Bytes / FileSize) <
                                  (SA#scav.skip_live_percentage_greater_than / 100)
                      end, lists:zip(SeqSizes, TmpList3))),
    erase(write_lim),
    ok = file:write_file(
           TempDir ++ "/debug1",
           term_to_binary([
                           {"FindOut", FindOut},
                           {"AllSeqs", AllSeqs},
                           {"LsAL1", LsAL1},
                           {"LSN_BL", LiveSeqNum_BytesList},
                           {"LsAL2", LsAL2},
                           {"LiveDict", LiveDict},
                           {"DeadSeqs", DeadSeqs},
                           {"LiveSeqsAbs", LiveSeqsAbs},
                           {"TmpList3", TmpList3},
                           {"SeqSizes", SeqSizes},
                           {"TmpList4", TmpList4}
                          ])),
    Finfolog("SCAV: ~p finished step 6\n", [SA#scav.name]),
    scav_check_shutdown(),

    %% Step 7: Count all bytes in all scavenged files, used for stats later.
    {_, BytesBefore} = lists:foldl(Ffile_size_iter, {0, 0},
                                   [brick_ets:which_path(SA#scav.log_dir,
                                                         WalMod, Seq) ||
                                       {Seq, _} <- TmpList4]),
    Finfolog("SCAV: ~p finished step 7\n", [SA#scav.name]),
    scav_check_shutdown(),

    SA2 = SA#scav{options = [{dead_paths, DeadPaths}|SA#scav.options]},
    (SA2#scav.phase10_fun)(SA2, Finfolog, TempDir, TmpList4, Bytes1,
                           Del1, BytesBefore).

%% @spec (scav_r(), fun(), string(), list(), integer(), integer(), integer()) ->
%%       normal
%% @doc Bottom half (step #10) of scavenger.
%%
%% The arguments are an eclectic mix of stuff that's a result of a
%% previous refactoring to split the old refactoring function into
%% several pieces.  Bytes1, Del1, and BytesBefore are used only for
%% reporting at the end of this func.

scavenger_commonlog_bottom10(SA0, Finfolog, TempDir, TmpList4, Bytes1,
                             Del1, BytesBefore) ->

    %% We're now eligible for resuming if we're interrupted/shutdown.
    scav_write_resume(SA0, Finfolog, TempDir, TmpList4, Bytes1,
                      Del1, BytesBefore),

    {ok,ThrottlePid} = brick_ticket:start_link(
                         undefined, SA0#scav.throttle_bytes),
    SA = SA0#scav{throttle_pid = ThrottlePid},

    %% Step 5 (going backward): it isn't kosher to have the top have
    %% doing destructive things like deleting files, so we need to
    %% delete those files here.
    DeadPaths = proplists:get_value(dead_paths, SA#scav.options),
    [brick_ets:delete_seq(SA, SeqNum) || {SeqNum, _Path} <- DeadPaths],

    %% Step 10: Copy hunks to a new long-term sequence.  Advance the
    %% long-term counter to avoid chance of writing to the same
    %% sequence that we read from.
    {ok, _} = gmt_hlog:advance_seqnum(SA#scav.log, -1),
    Fread_blob = fun(Su, FH) ->
                         ?LOGTYPE_BLOB = Su#hunk_summ.type,
                         [Bytes] = Su#hunk_summ.c_len,
                         brick_ticket:get(SA#scav.throttle_pid, Bytes),
                         Bin = gmt_hlog:read_hunk_member_ll(FH, Su, md5, 1),
                         Su2 = Su#hunk_summ{c_blobs = [Bin]},
                         %% TODO: In case of failure, don't crash.
                         true = gmt_hlog:md5_checksum_ok_p(Su2),
                         Bin
                 end,
    F1seq = scavenge_one_seq_file_fun(TempDir, SA, Fread_blob, Finfolog),
    {_, Hunks, Bytes, Errs} = lists:foldl(F1seq, {TmpList4, 0, 0, 0}, TmpList4),
    Finfolog("SCAV: ~p finished step 10\n", [SA#scav.name]),

    OptsNoDead = lists:filter(fun({dead_paths, _}) -> false;
                                 (_)               -> true
                              end, SA#scav.options),
    Finfolog("Scavenger finished:\n"
             "\tOptions: ~p\n"
             "\tLogs deleted without scavenging: ~p (~p bytes)\n"
             "\tLogs scavenged: ~p\n"
             "\tCopied: hunks bytes errs = ~p ~p ~p\n"
             "\tReclaimed bytes = ~p\n",
             [OptsNoDead,
              Del1, Bytes1, length(TmpList4),
              Hunks, Bytes, Errs, BytesBefore - Bytes]),
    if SA#scav.destructive == true -> os:cmd("/bin/rm -rf " ++ TempDir);
       true                        -> ok
    end,
    ?DBG_TLOGx({scavenger_start, SA#scav.name, done}),
    brick_ticket:stop(SA#scav.throttle_pid),
    normal.

update_locations_commonlog(_SA, DiskLog) ->
    Res = brick_ets:disk_log_fold_bychunk(
            fun(NewLocs0, Acc) ->
                    scav_check_shutdown(),
                    NewLocs = gmt_util:list_keypartition(
                                1, lists:keysort(1, NewLocs0)),
                    Updates = [update_locations_on_brick(Brick, Locs) ||
                                  {Brick, Locs} <- NewLocs],
                    Acc + lists:sum(Updates)
            end, 0, DiskLog),
    Res.

make_update_location({_BrickName, Key, OrigTS, OrigVal, NewVal}) ->
    F = fun(Key0, _DoOp, _DoFlags, S0) ->
                O2 = case brick_server:ssf_peek(Key0, false, S0) of
                         [] ->
                             key_not_exist;
                         [{_Key, OrigTS, _OrigVal, OldExp, OldFlags}] ->
                             {ImplMod, ImplState} =
                                 brick_server:ssf_impl_details(S0),
                             Val0 = ImplMod:bcb_val_switcharoo(
                                      OrigVal, NewVal, ImplState),
                             Fs = [{testset, OrigTS}|OldFlags],
                             %% make_replace doesn't allow custom TS.
                             brick_server:make_op6(replace, Key0, OrigTS,
                                                   Val0, OldExp, Fs);
                         [{_Key, RaceTS, _RaceVal, _RaceExp, _RaceFlags}] ->
                             %%{ts_error_qqq, Name, racets, RaceTS, origts, OrigTS, raceval, _RaceVal, origval, OrigVal}
                             {ts_error, RaceTS}
                     end,
                {ok, [O2]}
        end,
    brick_server:make_ssf(Key, F).

update_locations_on_brick(Brick, NewLocs) ->
    Dos = [make_update_location(NL) ||
              NL <- NewLocs],
    DoRes = brick_server:do(Brick, node(), Dos,
                            [ignore_role, {sync_override, false},
                             local_op_only_do_not_forward], 60*1000),
    Any = lists:any(fun(ok)            -> false;
                       (key_not_exist) -> false;
                       ({ts_error, _}) -> false;
                       (_X)            -> true
                    end, DoRes),
    if Any == false ->
            length(Dos);
       true ->
            ?E_ERROR(
               "update_locations: ~p: keys = ~p, return = ~p\n",
               [Brick, [Key || {_, Key, _, _, _} <- NewLocs], DoRes]),
            error % Yes, returning a non-integer is bad.
    end.

scavenge_one_seq_file_fun(TempDir, SA, Fread_blob, Finfolog) ->
    {ok, SleepTimeSec} = gmt_config_svr:get_config_value_i(brick_dirty_buffer_wait, 60),
    fun({SeqNum, Bytes}, {SeqNums, Hs, Bs, Es}) ->
            scav_check_shutdown(),

            %% Write status file, to resume later: store the sequence
            %% number we're working on but have not finished yet.
            %%
            %% Will need to change if we end up doing these copies
            %% with multiple procs.
            {MySeqNum, _} = hd(SeqNums),
            WFun = fun(FH2) -> ok = io:format(FH2, "~w.\n", [MySeqNum]) end,
            ok = brick_server:replace_file_sync(scav_bottomstatus_path(TempDir),
                                                WFun),

            DInPath = TempDir ++ "/" ++ integer_to_list(SeqNum),
            DOutPath = TempDir ++ "/" ++ integer_to_list(SeqNum) ++ ".out",
            {ok, DInLog} = disk_log:open([{name, DInPath},
                                          {file, DInPath}, {mode,read_only}]),
            {ok, DOutLog} = disk_log:open([{name, DOutPath}, {file, DOutPath}]),
            {ok, FH} = (SA#scav.wal_mod):open_log_file(SA#scav.log_dir, SeqNum,
                                                       [read, binary]),
            %% NOTE: Bytes is bound var!
            {Hunks, Bytes__, Errs} =
                brick_ets:disk_log_fold(
                  fun({Offset, BrickName, Key, TS, _ValLen, _ExpTime, _Flags},
                      {Hs1, Bs1, Es1}) ->
                          scav_check_shutdown(),
                          OldLoc = {SeqNum, Offset},
                          case brick_ets:copy_one_hunk(SA, FH, Key, SeqNum,
                                                       Offset, Fread_blob) of
                              error ->
                                  {Hs1, Bs1, Es1 + 1};
                              {NewLoc, Size} ->
                                  %% We want a tuple sortable first by
                                  %% brick name.
                                  Tpl = {BrickName, Key, TS, OldLoc, NewLoc},
                                  ok = disk_log:log(DOutLog, Tpl),
                                  {Hs1 + 1, Bs1 + Size, Es1}
                          end;
                     ({live_bytes, _}, Acc) ->
                          Acc
                  end, {0, 0, 0}, DInLog),
            file:close(FH),
            disk_log:close(DInLog),
            disk_log:close(DOutLog),
            if Errs == 0, SA#scav.destructive == true ->
                    Finfolog("SCAV: Updating locations for sequence ~p (~p)\n",
                             [SeqNum, Bytes == Bytes__]),
                    {ok, DOutLog} = disk_log:open([{name, DOutPath},
                                                   {file, DOutPath}]),
                    case (catch (SA#scav.update_locations)(SA, DOutLog)) of
                        NumUpdates when is_integer(NumUpdates) ->
                            Finfolog(
                              "SCAV: ~p sequence ~p: UpRes ok, "
                              "~p updates\n",
                              [SA#scav.name, SeqNum, NumUpdates]),
                            spawn(fun() ->
                                          timer:sleep(SleepTimeSec*1000),
                                          brick_ets:delete_seq(SA, SeqNum)
                                  end);
                       UpRes ->
                            Finfolog(
                              "SCAV: ~p sequence ~p: UpRes ~p\n",
                              [SA#scav.name, SeqNum, UpRes])
                    end;
               Errs == 0, SA#scav.destructive == false ->
                    Finfolog("SCAV: zero errors for sequence ~p\n",
                             [SeqNum]);
               Errs > 0 ->
                       ?E_ERROR("SCAV: ~p sequence ~p: ~p errors\n",
                                [SA#scav.name, SeqNum, Errs]);
               true ->
                    ok
            end,
            disk_log:close(DOutLog),

            {tl(SeqNums), Hs + Hunks, Bs + Bytes, Es + Errs}
    end.

schedule_next_daily_scavenger() ->
    NowSecs = calendar:time_to_seconds(time()),
    StartStr = gmt_config:get_config_value(brick_scavenger_start_time, "3:00"),
    StartSecs = calendar:time_to_seconds(parse_hh_mm(StartStr)),
    WaitSecs = if NowSecs < StartSecs ->
                       (StartSecs - NowSecs);
                  true ->
                       StartSecs + (86400 - NowSecs)
               end,
    ?E_INFO("Scheduling next scavenger ~p seconds from now.", [WaitSecs]),
    timer:send_after(WaitSecs * 1000, start_daily_scavenger).

parse_hh_mm(Str) ->
    [HH, MM|_] = string:tokens(Str, ":"),
    {list_to_integer(HH), list_to_integer(MM), 0}.

do_sequence_file_is_bad(SeqNum, Offset, S) ->
    {CurSeqNum, _CurOffset} =
        gmt_hlog:get_current_seqnum_and_file_position(S#state.hlog_pid),
    brick_ets:sequence_file_is_bad_common(
      S#state.hlog_dir, gmt_hlog, S#state.hlog_pid, S#state.name,
      SeqNum, Offset),
    LocalBricks = do_get_all_registrations(S),
    if SeqNum == CurSeqNum ->
            ?APPLOG_WARNING(?APPLOG_APPM_087,
                            "Fatal error: common log: current sequence file is bad: ~p\n",
                            [SeqNum]),
            spawn(fun() -> application:stop(gdss) end),
            timer:sleep(200),
            exit({current_sequence_is_bad, SeqNum});
       true ->
            [begin
                 brick_ets:append_external_bad_sequence_file(Brick, SeqNum),
                 spawn(fun() -> brick_server:common_log_sequence_file_is_bad(
                                  Brick, node(), SeqNum)
                       end)
             end || Brick <- LocalBricks],
            S
    end.

