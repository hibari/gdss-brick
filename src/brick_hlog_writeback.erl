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

-record(state, {
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
    WriteBackInterval = 30000,  %% 30 secs.  @TODO: Configurable
    {ok, TRef} = timer:send_interval(WriteBackInterval, schedule_async_writeback),
    {ok, #state{writeback_timer=TRef}}.

handle_call(full_writeback, From, #state{writeback_pid=undefined,
                                         writeback_reqs=[],
                                         writeback_reqs_next_round=NextRoundReqs,
                                         last_seq=LastSeqNum,
                                         last_pos=LastOffset}=State) ->
    Pid = schedule_async_writeback(LastSeqNum, LastOffset),
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

handle_info(schedule_async_writeback, #state{writeback_pid=undefined,
                                             writeback_reqs=[],
                                             writeback_reqs_next_round=NextRoundReqs,
                                             last_seq=LastSeqNum,
                                             last_pos=LastOffset}=State) ->
    Pid = schedule_async_writeback(LastSeqNum, LastOffset),
    {noreply, State#state{writeback_pid=Pid,
                          writeback_reqs=NextRoundReqs,
                          writeback_reqs_next_round=[]
                         }};
handle_info(schedule_async_writeback, #state{writeback_pid=Pid}=State) when Pid =/= undefined ->
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

-spec schedule_async_writeback(seqnum(), offset()) -> pid().
schedule_async_writeback(LastSeqNum, LastOffset) ->
    {Pid, _Ref} =
        spawn_monitor(
          fun() ->
                  case do_writeback(LastSeqNum, LastOffset) of
                      {NewSeqNum, NewOffset} ->
                          gen_server:cast(?WRITEBACK_SERVER_REG_NAME,
                                          {writeback_finished, NewSeqNum, NewOffset}),
                          exit(normal)
                  end
          end),
    Pid.

-spec do_writeback(seqnum(), offset()) -> {seqnum(), offset()}.
do_writeback(LastSeqNum, LastOffset) ->
    SeqNums0 = ?WAL:get_all_seqnums(),
    {CurSeqNum, CurOffset} = ?WAL:get_current_seqnum_and_offset(),
    if
        LastSeqNum =:= CurSeqNum ->
            do_writeback_wal(CurSeqNum, LastOffset, CurOffset);
        true ->
            SeqNums1 = lists:filter(fun(Seq) ->
                                            LastSeqNum < Seq andalso Seq < CurSeqNum
                                    end, SeqNums0),
            if
                0 < LastSeqNum ->
                    do_writeback_wal(LastSeqNum, LastOffset, undefined);
                true ->
                    ok
            end,
            lists:foreach(fun(Seq) ->
                                  do_writeback_wal(Seq, 0, undefined)
                          end, SeqNums1),
            do_writeback_wal(CurSeqNum, 0, CurOffset)
    end,
    {CurSeqNum, CurOffset}.

-spec do_writeback_wal(seqnum(), offset(), offset() | undefined) -> ok.
do_writeback_wal(SeqNum, _StartOffset, _EndOffset) ->
    %% @TODO Read only a configurable block (e.g. 1MB) at once.
    BlockSize = 500 * 1024 * 1024,  %% 500MB!
    {ok, FH} = ?WAL:open_wal_for_read(SeqNum),
    try file:read(FH, BlockSize) of
        {ok, Bin} ->
            {ok, Hunks, <<>>} = ?HUNK:parse_hunks(Bin),
            do_writeback_hunks(Hunks),
            ?ELOG_INFO("Finished writing back HLog seqence: ~w", [SeqNum]),
            ok;
        eof ->
            ?ELOG_INFO("Skipped writing back HLog seqence: ~w (empty log)", [SeqNum]),
            ok
    after
        catch file:close(FH)
    end.

-spec do_writeback_hunks([hunk()]) -> ok.
do_writeback_hunks(Hunks) ->
    %% GBB: Group By Brick-name
    {MetadataHunksGBB, BlobHunksGBB} = group_hunks(Hunks),
    lists:foreach(fun({BrickName, Hunks1}) ->
                          ?ELOG_DEBUG("metadata: ~w - ~w hunks.", [BrickName, length(Hunks1)]),
                          IsLastBatch = true,
                          %% @TODO Maintain a cache of MdStores
                          {ok, MdStore} = brick_metadata_store:get_metadata_store(BrickName),
                          ok = MdStore:writeback_to_stable_storage(Hunks1, IsLastBatch)
                  end, MetadataHunksGBB),
    lists:foreach(fun({BrickName, Hunks1}) ->
                          ?ELOG_DEBUG("blob:     ~w - ~w hunks.", [BrickName, length(Hunks1)])
                          %% IsLastBatch = true,
                          %% ?BlobStore:writeback_to_stable_storage(BrickName, Hunks1, IsLastBatch)
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
