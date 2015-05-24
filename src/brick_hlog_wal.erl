%%%-------------------------------------------------------------------
%%% Copyright (c) 2014-2015 Hibari developers. All rights reserved.
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
%%% File    : brick_hlog_wal.erl
%%% Purpose :
%%%-------------------------------------------------------------------



%% @TODO: Reorganize the sync timer?
%% @TODO: Monitor the disk status (free space etc.) and adjust the throttles



-module(brick_hlog_wal).

%% API
-export([start_link/1,
         write_hunk/1,
         write_hunk_group_commit/2,
         request_group_commit/1,
         open_wal_for_read/1,
         get_all_seqnums/0,
         get_current_seqnum_and_offset/0
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% DEBUG
-export([test1/0,
         test2/0
        ]).

-include("gmt_hlog.hrl").
-include("brick.hrl").     % for ?E_ macros

%% DEBUG
-include("brick_hlog.hrl").


%% ====================================================================
%% types and records
%% ====================================================================

%% -define(SYNC_DELAY_MILLS, 5).
-define(SYNC_DELAY_MILLS, 20).
-define(TIMEOUT, 60 * 1000).

-type prop() :: {name, servername()}
              | {file_len_max, len()}
              | {file_len_min, len()}.

-type hunk_iodata() :: iodata().

-type count() :: non_neg_integer().
-type set(_A) :: term().

%% -type commit_notification() :: {wal_sync, callback_ticket(), ok}
%%                              | {wal_sync, callback_ticket(), {error, term()}}.

-record(state, {
          wal_dir                       :: dirname(),
          file_len_max                  :: len(),                    % WAL file size max
          file_len_min                  :: len(),                    % WAL file size min
          hunk_count_min                :: count(),
          cur_seq                       :: seqnum(),                 % current sequence #
          cur_pos                       :: offset(),                 % current position
          cur_fh                        :: file:fd(),                % current file
          cur_hunk_count=0              :: count(),
          %% cur_hunk_overhead=0           :: non_neg_integer(),
          sync_listeners=gb_sets:new()  :: set(pid()),
          hunk_count_in_group_commit=0  :: non_neg_integer(),
          callback_ticket               :: callback_ticket(),
          sync_timer                    :: undefined | timer:tref(),
          sync_proc=undefined           :: undefined | {pid(), reference()},
          write_backlog=[]              :: [hunk_bytes()]
         }).
-type state() :: #state{}.


%% ====================================================================
%% API
%% ====================================================================

-spec start_link([prop()]) -> {ok, pid()} | {error, term()} | ignore.
start_link(PropList) ->
    gen_server:start_link({local, ?WAL_SERVER_REG_NAME}, ?MODULE, PropList, []).

-spec write_hunk(hunk_iodata())
                -> {ok, seqnum(), offset()} | {hunk_too_big, len()} | {error, term()}.
write_hunk(HunkBytes) when is_binary(HunkBytes) ->
    gen_server:call(wal_server(), {write_hunk, HunkBytes}, ?TIMEOUT);
write_hunk(HunkBytes) when is_list(HunkBytes) ->
    gen_server:call(wal_server(), {write_hunk, list_to_binary(HunkBytes)}, ?TIMEOUT).

-spec write_hunk_group_commit(hunk_iodata(), pid())
                -> {ok, seqnum(), offset(), callback_ticket()}
                       | {hunk_too_big, len()}
                       | {error, term()}.
write_hunk_group_commit(HunkBytes, Caller) when is_binary(HunkBytes) ->
    gen_server:call(wal_server(),
                    {write_hunk_group_commit, HunkBytes, Caller}, ?TIMEOUT);
write_hunk_group_commit(HunkBytes, Caller) when is_list(HunkBytes) ->
    gen_server:call(wal_server(),
                    {write_hunk_group_commit, list_to_binary(HunkBytes), Caller}, ?TIMEOUT).

-spec request_group_commit(pid()) -> callback_ticket().
request_group_commit(Requester) ->
    gen_server:call(wal_server(), {request_group_commit, Requester}, ?TIMEOUT).

-spec open_wal_for_read(seqnum()) -> {ok, file:fd()} | {error, term()} | not_available.
open_wal_for_read(SeqNum) ->
    Dir = gen_server:call(wal_server(), get_wal_dir, ?TIMEOUT),
    do_open_wal_for_read(Dir, SeqNum).

-spec get_all_seqnums() -> [seqnum()].
get_all_seqnums() ->
    Dir = gen_server:call(wal_server(), get_wal_dir, ?TIMEOUT),
    HLogFiles = filelib:wildcard("*.hlog", Dir),
    [ list_to_integer(filename:basename(N, ".hlog")) || N <- lists:sort(HLogFiles) ].

-spec get_current_seqnum_and_offset() -> {seqnum(), offset()}.
get_current_seqnum_and_offset() ->
    gen_server:call(wal_server(), get_current_seqnum_and_offset, ?TIMEOUT).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init(Options) ->
    process_flag(priority, high),

    Dir = "data/wal_hlog",
    _ = (catch file:make_dir(Dir)),

    DefaultLenMax = 1.5 * 1024 * 1024 * 1024, %% 1.5GB
    DefaultLenMin =  64 * 1024 * 1024,        %%  64MB
    LenMax = proplists:get_value(file_len_max, Options, DefaultLenMax),
    LenMin = min(LenMax - 1, proplists:get_value(file_len_min, Options, DefaultLenMin)),
    %% Minimum hunk count. If each blob hunk is 10MB and corresponding metadata
    %% is 100 bytes, the minimum WAL size will be 4.9GB. But it's capped by LenMax
    %% which is 1.5GB by default.
    HunkCountMin = proplists:get_value(hunk_count_min, Options, 1000),

    CurSeq = case filelib:wildcard("*.hlog", Dir) of
                 [] ->
                     1;
                 HLogFiles ->
                     LastFile = filename:basename(lists:max(HLogFiles), ".hlog"),
                     list_to_integer(LastFile) + 1
             end,
    {CurFH, CurPos} = create_wal(Dir, CurSeq),

    ?E_INFO("The WAL server ~w started. current sequence: ~w, "
            "minimum hunk count: ~w hunks, "
            "minimum file length: ~w bytes, maximum file length: ~w bytes",
            [?WAL_SERVER_REG_NAME, CurSeq, HunkCountMin, LenMin, LenMax]),

    {ok, #state{wal_dir=Dir,
                file_len_max=LenMax,
                file_len_min=LenMin,
                hunk_count_min=HunkCountMin,
                cur_seq=CurSeq,
                cur_fh=CurFH,
                cur_pos=CurPos,
                callback_ticket=make_ref()
               }}.

%% @TODO Create batch write request also (write_batch)
handle_call({write_hunk, HunkBytes}, _From, State) ->
    %% if
    %%     H_Len > FileLenMax ->
    %%     {{hunk_too_big, H_Len}, S};

    Start = os:timestamp(),
    %% @TODO Accumulate hunk overhead
    case do_write_hunk(HunkBytes, State) of
        {sync_in_progress, Seq, Pos, State1} ->
            %% @TODO: Record metrics
            %% _Elapse = timer:now_diff(os:timestamp(), Start),
            {reply, {ok, Seq, Pos}, State1};
        {done, Seq, Pos, State1} ->
            %% @TODO: Record metrics
            Elapse = timer:now_diff(os:timestamp(), Start),
            if
                Elapse > 50000 ->
                    ?ELOG_INFO("Write to WAL ~p took ~p ms",
                               [State1#state.wal_dir, Elapse div 1000]);
                true ->
                    ok
            end,
            {reply, {ok, Seq, Pos}, State1}
    end;
handle_call({write_hunk_group_commit, HunkBytes, Caller}, _From,
            #state{sync_timer=SyncTimer}=State) ->
    %% if
    %%     H_Len > FileLenMax ->
    %%     {{hunk_too_big, H_Len}, S};

    Start = os:timestamp(),
    {CommitTicket, State1} = do_register_group_commit(Caller, State),
    %% @TODO Accumulate hunk overhead
    case do_write_hunk(HunkBytes, State1) of
        {sync_in_progress, Seq, Pos, State2} ->
            %% @TODO: Record metrics
            %% _Elapse = timer:now_diff(os:timestamp(), Start),
            %% @TODO: Refactoring
            case SyncTimer of
                undefined ->
                    Timer = new_sync_wal_timer(),
                    {reply, {ok, Seq, Pos, CommitTicket}, State2#state{sync_timer=Timer}};
                _ ->
                    {reply, {ok, Seq, Pos, CommitTicket}, State2}
            end;
        {done, Seq, Pos, State2} ->
            %% @TODO: Record metrics
            Elapse = timer:now_diff(os:timestamp(), Start),
            if
                Elapse > 50000 ->
                    ?ELOG_INFO("Write to WAL ~p took ~p ms",
                               [wal_path(State2#state.wal_dir, Seq), Elapse div 1000]);
                true ->
                    ok
            end,
            %% @TODO: Refactoring
            case SyncTimer of
                undefined ->
                    Timer = new_sync_wal_timer(),
                    {reply, {ok, Seq, Pos, CommitTicket}, State2#state{sync_timer=Timer}};
                _ ->
                    {reply, {ok, Seq, Pos, CommitTicket}, State2}
            end
    end;
handle_call({request_group_commit, Requester}, _From,
            #state{sync_timer=SyncTimer}=State) ->
    {CommitTicket, State1} = do_register_group_commit(Requester, State),
    %% @TODO: Refactoring
    case SyncTimer of
        undefined ->
            Timer = new_sync_wal_timer(),
            {reply, CommitTicket, State1#state{sync_timer=Timer}};
        _ ->
            {reply, CommitTicket, State1}
    end;
handle_call(get_wal_dir, _From, #state{wal_dir=Dir}=State) ->
    {reply, Dir, State};
handle_call(get_current_seqnum_and_offset, _From,
            #state{cur_seq=CurSeq, cur_pos=CurPos}=State) ->
    {reply, {CurSeq, CurPos}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sync_wal, State) ->
    State1 = do_sync_wal(State),
    {noreply, State1};
handle_info({'DOWN', _Ref, _, Pid, Reason} = _Msg, #state{sync_proc={SyncPid, _}}=State)
  when Pid =:= SyncPid ->
    State1 = do_sync_done(Pid, Reason, State),
    {noreply, State1}.

terminate(_Reason, #state{wal_dir=Dir, cur_seq=SeqNum, cur_fh=FH}=State) ->
    catch write_backlog(State),
    catch close_wal(Dir, SeqNum, FH),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

wal_server() ->
    ?WAL_SERVER_REG_NAME.

-spec do_write_hunk(hunk_bytes(), state())
                   -> {sync_in_progress | ok, seqnum(), offset(), state()}.
do_write_hunk(HunkBytes, #state{write_backlog=Backlog, sync_proc=SyncProcess,
                                cur_seq=CurSeq, cur_pos=Pos1,
                                cur_hunk_count=HunkCount}=State)
  when SyncProcess =/= undefined; Backlog =/= [] ->
    %% do_sync_wal/1 or do_sync_done/3 is running. Do not write the hunk to
    %% the file; instead, put the hunk to the waiting list (write_backlog).
    Pos2 = Pos1 + byte_size(HunkBytes),
    State1 = State#state{cur_pos=Pos2, write_backlog=[HunkBytes | Backlog],
                         cur_hunk_count=HunkCount + 1},
    {sync_in_progress, CurSeq, Pos1, State1};
do_write_hunk(HunkBytes, State) ->
    State1 =
        case should_advance_seqnum(State) of
            true ->
                do_advance_seqnum(1, State);
            false ->
                State
        end,
    FH        = State1#state.cur_fh,
    Pos1      = State1#state.cur_pos,
    HunkCount = State1#state.cur_hunk_count,

    HunkSize = byte_size(HunkBytes),
    assert_file_position(prewrite, FH, Pos1),
    Start = brick_metrics:histogram_timed_begin(wal_write_latencies),
    ok = file:write(FH, HunkBytes),
    brick_metrics:histogram_timed_notify(Start),
    Pos2 = Pos1 + HunkSize,
    %% DEBUG: assert_file_position(post_write, FH, Pos2),

    {done, State1#state.cur_seq, Pos1, State1#state{cur_pos=Pos2, cur_hunk_count=HunkCount + 1}}.


%% -spec assert_file_position(atom(), file:fd(), offset()) -> ok | no_return().
assert_file_position(Tag, FH, ExpectedPosition) ->
    case file:position(FH, cur) of
        {ok, ExpectedPosition} ->  %% This is a bound variable.
            ok;
        {ok, ActualPosition} ->
            error({Tag, {expected, ExpectedPosition}, {actual, ActualPosition}})
    end.

-spec do_register_group_commit(pid(), state())
                              -> {callback_ticket(), state()}.
do_register_group_commit(Pid, #state{callback_ticket=Ticket,
                                     sync_listeners=Listeners,
                                     hunk_count_in_group_commit=Count}=State) ->
    Listeners1 =
        case gb_sets:is_member(Pid, Listeners) of
            true ->
                Listeners;
            false ->
                gb_sets:insert(Pid, Listeners)
        end,
    {Ticket, State#state{sync_listeners=Listeners1,
                         hunk_count_in_group_commit=Count + 1}}.

do_sync_wal(#state{sync_proc=undefined,
                   callback_ticket=Ticket, sync_listeners=Listeners,
                   hunk_count_in_group_commit=Count,
                   wal_dir=Dir, cur_seq=CurSeq}=State) ->
    case gb_sets:is_empty(Listeners) of
        true ->
            State;
        false ->
            ListenerList = gb_sets:to_list(Listeners),
            Pid_Ref = spawn_sync_wal(Ticket, ListenerList, Count, Dir, CurSeq),
            State#state{sync_proc=Pid_Ref, callback_ticket=make_ref(),
                        sync_listeners=gb_sets:new(),
                        hunk_count_in_group_commit=0}
    end;
do_sync_wal(State) ->
    State.       %% Do nothing because another sync process might be still running.

-spec spawn_sync_wal(callback_ticket(), [pid()], non_neg_integer(),
                     dirname(), seqnum()) -> {pid(), reference()}.
spawn_sync_wal(Ticket, ListenerList, Count, Dir, CurSeq) ->
    spawn_monitor(
      fun() ->
              Start1 = brick_metrics:histogram_timed_begin(wal_sync_latencies),
              Start2 = os:timestamp(),
              Path = "././" ++ Dir,  %% @TODO: FIXME
              %% use 'append' mode because 'write' mode will truncate the file.
              {ok, FH} = open_wal(Path, CurSeq, [append]),
              try
                  %% @TODO: CHECKME: Do we really need this check?
                  %% This code block was copied from gmt_hlog.erl.
                  {ok, #file_info{size=Size}} = file:read_file_info(Path),
                  if Size =:= 0 ->
                          ?E_ERROR("fsync ~s, size 0", [Path]);
                     true ->
                          ok
                  end,
                  Res = file:sync(FH),
                  ElapseMillis = timer:now_diff(os:timestamp(), Start2) div 1000,
                  brick_metrics:histogram_timed_notify(Start1),
                  brick_metrics:notify({wal_sync_requests, length(ListenerList)}),

                  %% Notify the listners.
                  Notification = {wal_sync, Ticket, Res},
                  lists:foreach(fun(Pid) ->
                                        Pid ! Notification
                                end, ListenerList),

                  %% NOTE: ElapseMillis does not include the time for sending notifications.
                  if ElapseMillis > 200 ->   %% 200 ms
                          ?ELOG_INFO("sync was ~p msec for ~p writes",
                                     [ElapseMillis, Count]);
                     true ->
                          ok
                  end
              after
                  ok = file:close(FH)
              end,
              normal
      end).

do_sync_done(Pid, _Reason, #state{sync_proc={Pid, _}}=State) ->
    do_pending_writes(State#state{sync_proc=undefined}).

do_pending_writes(#state{write_backlog=[], sync_timer=Timer}=State) ->
    cancel_sync_wal_timer(Timer),
    State#state{sync_timer=undefined};
do_pending_writes(State) ->
    write_backlog(State),
    State#state{write_backlog=[]}.

write_backlog(#state{cur_fh=FH, cur_pos=Pos, write_backlog=Backlog}) ->
    ok = file:write(FH, lists:reverse(Backlog)),
    assert_file_position(write_backlog, FH, Pos),        %% @TODO: DEBUG DESABLEME
    %% @TODO: advance seq if necessary

    ok.

-spec new_sync_wal_timer() -> timer:tref().
new_sync_wal_timer() ->
    {ok, SyncTimer} = timer:send_interval(?SYNC_DELAY_MILLS, sync_wal),
    SyncTimer.

cancel_sync_wal_timer(undefined) ->
    ok;
cancel_sync_wal_timer(TimerRef) ->
    timer:cancel(TimerRef),
    ok.

-spec should_advance_seqnum(state()) -> boolean().
should_advance_seqnum(#state{cur_pos=Position, file_len_max=MaxLen, file_len_min=MinLen,
                             cur_hunk_count=HunkCount, hunk_count_min=MinHunkCount}) ->
    Position >= MaxLen
        orelse (Position >= MinLen andalso HunkCount >= MinHunkCount).

-spec do_advance_seqnum(non_neg_integer(), state()) -> state().
do_advance_seqnum(Incr, #state{sync_proc=undefined, write_backlog=[],
                               wal_dir=Dir, cur_seq=CurSeq, cur_fh=CurFH,
                               cur_pos=Position, cur_hunk_count=HunkCount}=State) ->
    ?E_INFO("Switching WAL from sequence ~w: ~s (~w hunks, ~w bytes written)",
            [CurSeq, wal_path(Dir, CurSeq), HunkCount, Position]),
    close_wal(Dir, CurSeq, CurFH),
    NewSeq = CurSeq + Incr,
    {NewFH, NewPosition} = create_wal(Dir, NewSeq),
    State#state{cur_seq=NewSeq, cur_fh=NewFH, cur_pos=NewPosition, cur_hunk_count=0};
do_advance_seqnum(_Incr, #state{sync_proc={Pid, Ref}}) ->
    error({do_advance_seqnum, {sync_proc, Pid, Ref}}).

-spec do_open_wal_for_read(dirname(), seqnum()) -> {ok, file:fd()} | {error, term()} | not_available.
do_open_wal_for_read(Dir, SeqNum) when is_integer(SeqNum), SeqNum =/= 0 ->
    %% @TODO: read ahead?
    Path = wal_path(Dir, SeqNum),
    case file:open(Path, [binary, raw, read, read_ahead]) of
        {ok, _}=Res ->
            Res;
        {error, enoent} ->
            not_available;
        Res ->
            ?E_CRITICAL("Couldn't open log file ~s for read by ~w", [Path, Res]),
            Res
    end.

-spec create_wal(dirname(), seqnum()) ->
                                 {file:fd(), Size::non_neg_integer()} | no_return().
create_wal(Dir, SeqNum) when is_integer(SeqNum), SeqNum =/= 0 ->
    Path = wal_path(Dir, SeqNum),
    {SeqNum, {error, enoent}} = {SeqNum, file:read_file_info(Path)},  %% sanity
    %% @TODO: write buffer?
    %% 'read' option is required otherwise the file will be truncated.
    {ok, FH} = open_wal(Dir, SeqNum, [read, write, delayed_write]),
    try
        %% ok = write_log_header(FH),
        ?E_INFO("Created WAL with sequence ~w: ~s", [SeqNum, Path]),
        {ok, FI} = file:read_file_info(Path),
        {FH, FI#file_info.size}
    catch
        _:_=Err ->
            ok = file:close(FH),
            error(Err)
    end.

-spec open_wal(dirname(), seqnum(), openmode()) -> {ok, file:fd()} | {error, atom()}.
open_wal(Dir, SeqNum, Options) ->
    Path = wal_path(Dir, SeqNum),
    case file:open(Path, [binary, raw | Options]) of
        {ok, _}=Res ->
            Res;
        Res ->
            ?E_CRITICAL("Couldn't open log file ~w ~s by ~w", [Options, Path, Res]),
            Res
    end.

-spec close_wal(dirname(), seqnum(), file:fd()) -> ok | {error, term()}.
close_wal(Dir, SeqNum, FH) ->
    catch file:sync(FH),
    {Path, SizeStr} =
        case wal_info(Dir, SeqNum) of
            {ok, Path0, #file_info{size=Size0}} ->
                {Path0, io_lib:format("(~w bytes)", [Size0])};
            {error, Err0} ->
                {"", io_lib:format("(error ~p)", [Err0])}
        end,
    try file:close(FH) of
        ok ->
            ?E_INFO("Closed WAL with sequence ~w: ~s ~s",
                    [SeqNum, Path, SizeStr]),
            ok;
        {error, Reason}=Err1 ->
            ?E_ERROR("Failed to close WAL with sequence ~w: ~s (error ~p)",
                     [SeqNum, Path, Reason]),
            Err1
    catch E0:E1 ->
            ?E_ERROR("Failed to close WAL with sequence ~w: ~s (~p ~p)",
                     [SeqNum, Path, E0, E1]),
            {error, E1}
    end.

-spec wal_info(dirname(), seqnum()) -> {ok, filepath(), file:file_info()} | {error, atom()}.
wal_info(Dir, N) ->
    Path = wal_path(Dir, N),
    case file:read_file_info(Path) of
        {ok, FI} ->
            {ok, Path, FI};
        Res ->
            Res
    end.

-spec wal_path(dirname(), seqnum()) -> filepath().
wal_path(Dir, SeqNum) ->
    filename:join([Dir, seqnum2file(SeqNum)]).

-spec seqnum2file(seqnum()) -> string().
seqnum2file(SeqNum) ->
    lists:flatten([gmt_util:left_pad(integer_to_list(SeqNum), 12, $0), ".hlog"]).


%% DEBUG STUFF (@TODO: eunit / quickcheck cases)

test1() ->
    Brick = table1_ch1_b1,
    StoreTuple1 = term_to_binary({<<"key1">>, brick_server:make_timestamp(), <<"val1">>}),
    StoreTuple2 = term_to_binary({<<"key2">>, brick_server:make_timestamp(), <<"val2">>}),
    Blobs = [StoreTuple1, StoreTuple2],
    {HunkBytes, _Size, _Overhead, _BlobIndex} =
        brick_hlog_hunk:create_hunk_iolist(
          #hunk{type=metadata, brick_name=Brick, blobs=Blobs}),
    write_hunk(HunkBytes).

test2() ->
    Brick = table1_ch1_b1,
    StoreTuple1 = term_to_binary({<<"key1">>, brick_server:make_timestamp(), <<"val1">>}),
    StoreTuple2 = term_to_binary({<<"key2">>, brick_server:make_timestamp(), <<"val2">>}),
    Blobs = [StoreTuple1, StoreTuple2],
    {HunkBytes, _Size, _Overhead, _BlobIndex} =
        brick_hlog_hunk:create_hunk_iolist(
          #hunk{type=metadata, brick_name=Brick, blobs=Blobs}),
    Caller = self(),

    Tickets = lists:foldl(
                fun(_, Acc) ->
                        {_, _, _, Ticket} = write_hunk_group_commit(HunkBytes, Caller),
                        case gb_sets:is_member(Ticket, Acc) of
                            true ->
                                Acc;
                            false ->
                                io:format("New ticket: ~p~n", [Ticket]),
                                gb_sets:insert(Ticket, Acc)
                        end
                end, gb_sets:new(), lists:seq(1, 10000)),
    test_receive_notifications(Tickets).

test_receive_notifications(Tickets) ->
    case gb_sets:is_empty(Tickets) of
        true ->
            io:format("Done.~n"),
            ok;
        false ->
            receive
                {wal_sync, Ticket, _Res}=Notification ->
                    case gb_sets:is_member(Ticket, Tickets) of
                        true ->
                            io:format("Received ticket: ~p~n", [Notification]),
                            test_receive_notifications(gb_sets:delete(Ticket, Tickets));
                        false ->
                            io:format("Received UNKNOWN ticket: ~p~n", [Notification]),
                            test_receive_notifications(Tickets)
                    end
            after
                60000 ->
                    io:format("Timed out~n")
            end
    end.

