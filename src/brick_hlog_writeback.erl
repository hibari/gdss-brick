%%%-------------------------------------------------------------------
%%% Copyright (c) 2014-2016 Hibari developers. All rights reserved.
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
%%% File    : brick_hlog_writeback.erl
%%% Purpose :
%%%-------------------------------------------------------------------

-module(brick_hlog_writeback).

-behaviour(gen_server).

%% -include("brick_specs.hrl").
-include("brick_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% API
-export([start_link/1,
         stop/0,
         %% register_local_brick/1,
         full_writeback/0
         %% get_all_registrations/0
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).


%% ====================================================================
%% types, specs and records
%% ====================================================================

-type from() :: {pid(), term()}.
-type prop() :: [].
-type blocksize() :: non_neg_integer().  %% bytes

-record(state, {
          writeback_blocksize          :: blocksize(),  %% bytes
          writeback_timer              :: timer:tref(),
          writeback_pid                :: pid(),
          writeback_reqs=[]            :: [from()],  %% requesters of current async writeback
          writeback_reqs_next_round=[] :: [from()],  %% requesters of next async writeback
          last_seq=0                   :: seqnum(),
          last_pos=0                   :: offset()
         }).

-define(HUNK, brick_hlog_hunk).
-define(WAL,  brick_hlog_wal).


-define(FULL_WRITEBACK_TIMEOUT, 5 * 60000).  %% 5 minutes


%% ====================================================================
%% API
%% ====================================================================

-spec start_link([prop()]) -> {ok,pid()} | ignore | {error,term()}.
start_link(PropList) ->
    gen_server:start_link({local, ?WRITEBACK_SERVER_REG_NAME},
                          ?MODULE, [PropList], []).

-spec stop() -> ok | {error, term()}.
stop() ->
    gen_server:call(?WRITEBACK_SERVER_REG_NAME, stop).

%% -spec register_local_brick(brickname()) -> ok | {error, term()}.
%% register_local_brick(LocalBrick) when is_atom(LocalBrick) ->
%%     gen_server:call(?WRITEBACK_SERVER_REG_NAME, {register_local_brick, LocalBrick}).

%% Caller is blocked until this finishes.
-spec full_writeback() -> ok.
full_writeback() ->
    gen_server:call(?WRITEBACK_SERVER_REG_NAME, full_writeback, ?FULL_WRITEBACK_TIMEOUT).

%% -spec get_all_registrations() -> [brickname()].
%% get_all_registrations() ->
%%     get_all_registrations(?WRITEBACK_SERVER_REG_NAME).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([_Options]) ->
    WritebackBlockSize = 20 * 1024 * 1024,  %% 20MB.     @TODO: Configurable
    WritebackInterval  = 30000,             %% 30 secs.  @TODO: Configurable
    {ok, TRef} = timer:send_interval(WritebackInterval, request_async_writeback),
    {ok, #state{writeback_blocksize=WritebackBlockSize, writeback_timer=TRef}}.

handle_call(full_writeback, From, #state{writeback_pid=undefined,
                                         writeback_blocksize=BlockSize,
                                         writeback_reqs=[],
                                         writeback_reqs_next_round=NextRoundReqs,
                                         last_seq=LastSeqNum,
                                         last_pos=LastOffset}=State) ->
    Pid = schedule_async_writeback(LastSeqNum, LastOffset, BlockSize),
    {noreply, State#state{writeback_pid=Pid,
                          writeback_reqs=[From | NextRoundReqs],
                          writeback_reqs_next_round=[]
                         }};
handle_call(full_writeback, From, #state{writeback_pid=Pid,
                                         writeback_reqs_next_round=NextRoundReqs}=State)
  when Pid =/= undefined ->
    {noreply, State#state{writeback_reqs_next_round=[From| NextRoundReqs]}}.

handle_cast({writeback_finished, LastSeqNum, LastOffset},
            #state{writeback_reqs=Reqs, writeback_reqs_next_round=NextRoundReqs}=State) ->
    lists:foreach(fun(From) ->
                          gen_server:reply(From, ok)
                  end, Reqs),
    _ = ?WAL:writeback_finished(LastSeqNum, LastOffset),
    if
        NextRoundReqs =/= [] ->
            Delay = 200, %% 0.2 secs
            _ = timer:apply_after(Delay, ?MODULE, full_writeback, []);
        true ->
            ok
    end,
    {noreply, State#state{writeback_pid=undefined, writeback_reqs=[],
                          last_seq=LastSeqNum, last_pos=LastOffset}};
handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(request_async_writeback, #state{writeback_pid=undefined,
                                            writeback_blocksize=BlockSize,
                                            writeback_reqs=[],
                                            writeback_reqs_next_round=NextRoundReqs,
                                            last_seq=LastSeqNum,
                                            last_pos=LastOffset}=State) ->
    Pid = schedule_async_writeback(LastSeqNum, LastOffset, BlockSize),
    {noreply, State#state{writeback_pid=Pid,
                          writeback_reqs=NextRoundReqs,
                          writeback_reqs_next_round=[]
                         }};
handle_info(request_async_writeback, #state{writeback_pid=Pid}=State) when Pid =/= undefined ->
    %% an a write-back process is already running. Ignore the request.
    {noreply, State};
%% @TODO: Add handle_info clause to handle writeback process's error
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
    {noreply, State}.

terminate(_Reason, #state{writeback_timer=TRef}) ->
    timer:cancel(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

-spec schedule_async_writeback(seqnum(), offset(), blocksize()) -> pid().
schedule_async_writeback(LastSeqNum, LastOffset, BlockSize) ->
    {Pid, _Ref} =
        spawn_monitor(
          fun() ->
                  case do_writeback(LastSeqNum, LastOffset, BlockSize) of
                      {ok, NewSeqNum, NewOffset} ->
                          gen_server:cast(?WRITEBACK_SERVER_REG_NAME,
                                          {writeback_finished, NewSeqNum, NewOffset}),
                          exit(normal)
                  end
          end),
    Pid.

%% @TODO: IMPORTANT: Return end offsets of all wroteback sequence files so that
%% WAL module can ensure each sequence file has been fully processed.
-spec do_writeback(seqnum(), offset(), blocksize()) -> {ok, seqnum(), offset()}.
do_writeback(LastSeqNum, LastOffset, BlockSize) ->
    SeqNums0 = ?WAL:get_all_seqnums(),
    {CurSeqNum, CurOffset} = ?WAL:get_current_seqnum_and_disk_offset(),
    {ok, EndSeqNum, EndOffset} =
        if
            LastSeqNum =:= CurSeqNum ->
                do_writeback_wal(CurSeqNum, LastOffset, CurOffset, BlockSize);
            true ->
                SeqNums1 = lists:filter(fun(Seq) ->
                                                LastSeqNum < Seq andalso Seq < CurSeqNum
                                        end, SeqNums0),
                if
                    0 < LastSeqNum ->
                        do_writeback_wal(LastSeqNum, LastOffset, undefined, BlockSize);
                    true ->
                        ok
                end,
                lists:foreach(fun(Seq) ->
                                      do_writeback_wal(Seq, 0, undefined, BlockSize)
                              end, SeqNums1),
                do_writeback_wal(CurSeqNum, 0, CurOffset, BlockSize)
        end,
    if
        CurSeqNum =:= EndSeqNum, CurOffset =:= EndOffset ->
            ok;
        true ->
            ?ELOG_DEBUG("Different seqnums and/or offsets. "
                        "expected: {~w, ~w}, actual: {~w, ~w}",
                        [CurSeqNum, CurOffset, EndSeqNum, EndOffset])
    end,
    {ok, EndSeqNum, EndOffset}.

%% @TODO: Return error when necessary.
-spec do_writeback_wal(seqnum(), offset(), offset() | undefined, blocksize()) ->
                              {ok, seqnum(), offset()}.
do_writeback_wal(SeqNum, StartOffset, EndOffset, BlockSize) ->
    {ok, FH} = ?WAL:open_wal_for_read(SeqNum),
    try file:position(FH, StartOffset) of
        {ok, StartOffset} ->
            case do_writeback_wal_block(SeqNum, FH, StartOffset, EndOffset, BlockSize, <<>>) of
                {SeqNum, EndOffset2, <<>>, maybe_ok} ->
                    if
                        StartOffset =:= EndOffset2 ->
                            %% ?ELOG_DEBUG(
                            %%    "No hunks to writeback in WAL sequence ~w (offset: ~w)",
                            %%    [SeqNum, StartOffset]),
                            {ok, SeqNum, StartOffset};
                        true ->
                            ?ELOG_DEBUG(
                               "Wrote hunks from WAL sequence ~w (offsets: ~w..~w) "
                               "to metadata and blob storages",
                               [SeqNum, StartOffset, EndOffset2 - 1]),
                            {ok, SeqNum, EndOffset2}
                        end;
                {SeqNum, EndOffset2, Remainder, maybe_ok} ->
                    ?ELOG_WARNING(
                       "Wrote hunks from WAL sequence ~w (offsets: ~w..~w) "
                       "to metadata and blob storages, "
                       "but there are un-parsed bytes: ~p",
                       [SeqNum, StartOffset, EndOffset2 - 1, Remainder]),
                    {ok, SeqNum, EndOffset2 - byte_size(Remainder)};
                {SeqNum, EndOffset2, _Remainder, Err} ->
                    ?ELOG_CRITICAL(
                       "Failed to write hunks from WAL sequence ~w (offsets: ~w..~w) "
                       "to metadata and blob storages. error: ~p",
                       [SeqNum, StartOffset, EndOffset2 - 1, Err]),
                    {ok, SeqNum, EndOffset2}
            end;
        {ok, OtherOffset} ->
            ?ELOG_CRITICAL("Different offset. Skipping. expected: ~w, actual: ~w",
                           [StartOffset, OtherOffset]),
            error({different_offsets, {expected, StartOffset}, {actual, OtherOffset}});
        {error, _}=Err ->
            throw(Err)
    after
        _ = (catch file:close(FH))
    end.

-spec do_writeback_wal_block(seqnum(), file:fd(),
                             offset(), offset() | undefined, blocksize(), binary()) ->
                                    {seqnum(), offset(), binary(), maybe_ok | {error, term()}}.
do_writeback_wal_block(SeqNum, _FH, Offset, EndOffset, _BlockSize, Remainder)
  when EndOffset =/= undefined, Offset >= EndOffset ->
    {SeqNum, Offset, Remainder, maybe_ok};
do_writeback_wal_block(SeqNum, FH, Offset, EndOffset, BlockSize, Remainder) ->
    %% ?ELOG_DEBUG("SeqNum: ~w, Offset: ~w, EndOffset: ~w, BlockSize: ~w, byte_size(Remainder): ~w",
    %%             [SeqNum, Offset, EndOffset, BlockSize, byte_size(Remainder)]),
    BytesToRead = if EndOffset =:= undefined -> BlockSize;
                     true                    -> min(BlockSize, EndOffset - Offset)
                  end,
    case file:read(FH, BytesToRead) of
        {error, Err1} ->
            {SeqNum, Offset, Remainder, {error, {Err1, offset, Offset}}};
        eof ->
            {SeqNum, Offset, Remainder, maybe_ok};
        {ok, Bin} ->
            Bin2 = if Remainder =:= <<>> -> Bin;
                      true               -> <<Remainder/binary, Bin/binary>>
                   end,
            %% @TODO ENHANCEME: Perhaps just parse hunk headers? (to save RAM)
            case ?HUNK:parse_hunks(Bin2) of
                {ok, Hunks, Remainder2} ->
                    ok = do_writeback_hunks(Hunks),
                    ReadSize = byte_size(Bin),
                    Offset2 = Offset + ReadSize,
                    if
                        ReadSize < BytesToRead ->
                            {SeqNum, Offset2, Remainder2, maybe_ok};
                        true ->
                            do_writeback_wal_block(SeqNum, FH,
                                                   Offset2, EndOffset, BlockSize,
                                                   Remainder2)
                    end
            end
    end.

-spec do_writeback_hunks([hunk()]) -> ok.
do_writeback_hunks(Hunks) ->
    %% GBB: Group By Brick-name
    {MetadataHunksGBB, BlobHunksGBB} = group_hunks(Hunks),

    %% write-back metadata
    lists:foreach(fun({BrickName, Hunks1}) ->
                          %% ?ELOG_DEBUG("metadata: ~w - ~w hunks", [BrickName, length(Hunks1)]),
                          IsLastBatch = true,
                          {ok, MdStore} = brick_metadata_store:get_metadata_store(BrickName),
                          ok = MdStore:writeback_to_stable_storage(Hunks1, IsLastBatch)
                  end, MetadataHunksGBB),

    %% write blob location info
    lists:foreach(fun({BrickName, Hunks1}) ->
                          case brick_metadata_store:extract_location_info(Hunks1) of
                              [] ->
                                  ok;
                              Locations ->
                                  %% ?ELOG_DEBUG("location: ~w - ~w locations",
                                  %%             [BrickName, length(Locations)]),
                                  {ok, BlobStore} = brick_blob_store:get_blob_store(BrickName),
                                  ok = BlobStore:write_location_info(Locations)
                          end
                  end, MetadataHunksGBB),

    %% write-back blobs
    lists:foreach(fun({BrickName, Hunks1}) ->
                          %% ?ELOG_DEBUG("blob:     ~w - ~w hunks", [BrickName, length(Hunks1)]),
                          {ok, BlobStore} = brick_blob_store:get_blob_store(BrickName),
                          ok = BlobStore:writeback_to_stable_storage(Hunks1),
                          ok = BlobStore:sync()
                  end, BlobHunksGBB),
    ok.

-spec group_hunks([hunk()]) -> {[{brickname(), [hunk()]}], [{brickname(), [hunk()]}]}.
group_hunks(Hunks) ->
    %% GBB: Group By Brick-name
    {MetadataHunksGBBDict, BlobHunksGBBDict} =
        lists:foldl(fun(#hunk{type=metadata, brick_name=BrickName}=Hunk, {MDs, Blobs}) ->
                            {dict:append(BrickName, Hunk, MDs), Blobs};
                       (#hunk{type=blob_wal, brick_name=BrickName}=Hunk, {MDs, Blobs})->
                            {MDs, dict:append(BrickName, Hunk, Blobs)}
                    end, {dict:new(), dict:new()}, Hunks),
    {dict:to_list(MetadataHunksGBBDict), dict:to_list(BlobHunksGBBDict)}.
