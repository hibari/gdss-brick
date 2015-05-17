%%%-------------------------------------------------------------------
%%% Copyright (c) 2014-2015 Hibari developers.  All rights reserved.
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
%%% File    : brick_hlog_scavenger.erl
%%% Purpose : Scavenger (compactor) server for value blob hunk logs.
%%%-------------------------------------------------------------------

-module(brick_hlog_scavenger).

-behaviour(gen_server).

-include("brick.hrl").
-include("brick_public.hrl").
-include("gmt_hlog.hrl").
%% -include_lib("kernel/include/file.hrl").

%% API
-export([start_link/1,
         stop/1
        ]).

%% Scavenger API
-export([start_scavenger_value_blob/1,
         stop_scavenger_value_blob/0
        ]).

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


%%%----------------------------------------------------------------------
%%% Types/Specs/Records
%%%----------------------------------------------------------------------

-type props() :: list({common_log_name, servername()}).
-type byte_size() :: non_neg_integer().
-type seqnum_hunk_size() :: {seqnum(), byte_size()}.

-record(state, {
          name                            :: file:name(),
          hlog_common_pid                 :: pid(),                    %% Pid of the gmt_hlog
          hlog_dir                        :: dirname(),
          scavenger_tref                  :: timer:tref() | undefined,
          dirty_buffer_wait               :: non_neg_integer()         %% seconds
         }).
%% -type state()   :: #state{}.
%% -type state_readonly() :: #state{}.  %% Read-only

-record(scav_progress, {
          copied_hunks = 0    :: non_neg_integer(),
          copied_bytes = 0    :: byte_size(),
          errors       = 0    :: non_neg_integer()
         }).
-type scav_progress_r() :: #scav_progress{}.

-define(SCAVENGER_REG_NAME, hlog_scavenger).
-define(WAIT_BEFORE_EXIT, timer:sleep(1500)). %% milliseconds


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(props()) -> {ok,pid()} | {error,term()} | ignore.
start_link(PropList) ->
    gen_server:start_link(?MODULE, PropList, []).

-spec start(props()) -> {ok,pid()} | {error,term()} | ignore.
start(PropList) ->
    gen_server:start(?MODULE, PropList, []).

-spec stop(server()) -> ok | {error,term()}.
stop(Server) ->
    gen_server:call(Server, {stop}).

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
    RegName = ?SCAVENGER_REG_NAME,
    try
        case whereis(RegName) of
            undefined ->
                register(RegName, self()),
                process_flag(trap_exit, true),

                CommonLogName = proplists:get_value(common_log_name, PropList, commonLogDefault),
                CommonLogPid = whereis(CommonLogName),
                LogDir = gmt_hlog:log_name2data_dir(CommonLogName),
                {ok, DirtySec} = application:get_env(gdss_brick, brick_dirty_buffer_wait),

                ScavengerTRef = schedule_next_scavenger(),
                ?E_INFO("Scavenger server ~w started.", [RegName]),

                {ok, #state{name=RegName,
                            hlog_common_pid=CommonLogPid,
                            hlog_dir=LogDir,
                            scavenger_tref=ScavengerTRef,
                            dirty_buffer_wait=DirtySec
                           }};
            _Pid ->
                ignore
        end
    catch
        _X:_Y ->
            ?E_ERROR("init error: ~p ~p at ~p", [_X, _Y, erlang:get_stacktrace()]),
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
handle_call({stop}, _From, State) ->
    {stop, normal, ok, State}.

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
handle_info(start_scavenger, State) ->
    timer:sleep(2 * 1000),
    {ok, Percent} = application:get_env(gdss_brick, brick_skip_live_percentage_greater_than),
    {ok, WorkDir} = application:get_env(gdss_brick, brick_scavenger_temp_dir),
    PropList = [{skip_live_percentage_greater_than, Percent},
                {work_dir, WorkDir}],
    spawn(fun() -> start_scavenger_value_blob(PropList) end),
    {ok, ScavengerTRef} = schedule_next_scavenger(),
    {noreply, State#state{scavenger_tref=ScavengerTRef}};
handle_info({'DOWN', _Ref, _, _, _}, State) ->
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
terminate(_Reason, #state{name=RegName, scavenger_tref=ScavengerTRef}) ->
    if ScavengerTRef =/= undefined ->
            timer:cancel(ScavengerTRef);
       true ->
            noop
    end,
    ?E_INFO("Scavenger server ~w stopped.", [RegName]),
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

%% TODO: In an ideal world, the brick walks would include the size of the
%%       value blob so that copy_one_hunk could be a bit more efficient.

start_scavenger_value_blob(PropList0) ->
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
            do_start_scavenger_value_blob2(Bigs, PropList0);
        ErrBricks ->
            ErrNds = [{Br, node()} || Br <- ErrBricks],
            Msg = "Scavenger may not execute until all bricks are running.",
            _ = [gmt_util:set_alarm({scavenger, T}, Msg) || T <- ErrNds],
            ?E_ERROR("Bricks ~p are not available, scavenger aborted", [ErrNds]),
            {error, ErrNds}
    end.

do_start_scavenger_value_blob2(Bricks, PropList) ->
    CommonLogSvr = gmt_hlog_common:hlog_pid(?GMT_HLOG_COMMON_LOG_NAME),
    {ok, CurSeq0} = gmt_hlog:advance_seqnum(CommonLogSvr, 1),
    {CurSeq1, _} = gmt_hlog_common:read_flush_file(gmt_hlog:log_name2data_dir(
                                                     ?GMT_HLOG_COMMON_LOG_NAME)),
    %% CurSeq1 is the largest sequence number that the writeback
    %% process (plus time spent waiting for the
    %% 'brick_dirty_buffer_wait' interval) has flushed to disk.
    %% Don't mess with any sequence at or above CurSeq1.
    CurSeq = erlang:min(CurSeq0, CurSeq1),

    MaximumLivePt = case proplists:get_value(skip_live_percentage_greater_than,
                                             PropList) of
                        N when N >= 0, N =< 100 ->
                            N;
                        _ ->
                            100 % Examine all files
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

    SA = #scav{options=PropList,
               work_dir=WorkDir,
               wal_mod=gmt_hlog_common,
               name=?GMT_HLOG_COMMON_LOG_NAME,
               log=gmt_hlog:log_name2reg_name(?GMT_HLOG_COMMON_LOG_NAME),
               log_dir=gmt_hlog:log_name2data_dir(?GMT_HLOG_COMMON_LOG_NAME),
               %% For common log, any sequence number at or beyond
               %% CurSeq is off-limits.
               last_check_seq=CurSeq,
               skip_live_percentage_greater_than=MaximumLivePt,
               sorter_size=SorterSize,
               bricks=Bricks,
               throttle_bytes=ThrottleBytes,
               bottom_fun = fun scavenger_value_blob_bottom/1},
    ?DBG_TLOG("scavenger ~w, last_check_seq ~w", [SA#scav.name, CurSeq]),
    Fdoit = fun() -> scavenger_value_blob(SA),
                     exit(normal)
            end,
    Pid = spawn(fun() -> link_catch_shutdowns(Fdoit) end),
    {ok, Pid}.

link_catch_shutdowns(Fun) ->
    try
        %% We're probably spawned without a link.  It would be nice if
        %% someone would stop us if something big happened, e.g. the
        %% Hibari app were shut down.  We'll link ourselves to a
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

stop_scavenger_value_blob() ->
    case whereis(scav_excl_name()) of
        undefined ->
            scavenger_not_running;
        Pid ->
            Pid ! please_stop_now,
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

-spec scavenger_value_blob(scav_r()) -> 'normal'.
scavenger_value_blob(#scav{name=Name, work_dir=WorkDir,
                          log=Log, exclusive_p=ExclusiveP,
                          last_check_seq=LastCheckSeq,
                          bottom_fun=BottomFun}=SA) ->

    %% Make certain this is the only scavenger running.
    if
        ExclusiveP ->
            _ = scav_exit_if_someone_else();
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
            scavenger_value_blob_save_storage_locations(SA, AllSeqs),
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
-spec scavenger_value_blob_save_storage_locations(scav_r(), [seqnum()]) -> 'ok'.
scavenger_value_blob_save_storage_locations(#scav{name=Name, work_dir=WorkDir,
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

-spec scavenger_value_blob_bottom(scav_r()) -> 'normal' | 'stopping_on_shutdown_request'.
scavenger_value_blob_bottom(#scav{name=Name,
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

schedule_next_scavenger() ->
    %% NowSecs = calendar:time_to_seconds(time()),
    %% {ok, StartStr} = application:get_env(gdss_brick, brick_scavenger_start_time),
    %% StartSecs = calendar:time_to_seconds(parse_hh_mm(StartStr)),
    %% WaitSecs = if NowSecs < StartSecs ->
    %%                    (StartSecs - NowSecs);
    %%               true ->
    %%                    StartSecs + (86400 - NowSecs)
    %%            end,
    WaitSecs = 30 * 60,
    ?E_INFO("Scheduling next scavenger ~p seconds from now.", [WaitSecs]),
    timer:send_after(WaitSecs * 1000, start_scavenger).

%% parse_hh_mm(Str) ->
%%     [HH, MM|_] = string:tokens(Str, ":"),
%%     {list_to_integer(HH), list_to_integer(MM), 0}.
