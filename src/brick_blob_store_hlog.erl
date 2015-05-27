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
%%% File    : brick_value_store_hlog.erl
%%% Purpose : an brick_value_store implementation using hunk log
%%%-------------------------------------------------------------------

%% One gen_server process per brick_server process.

-module(brick_blob_store_hlog).

-behaviour(gen_server).

-include("brick_specs.hrl").
-include("brick_hlog.hrl").
-include("brick_blob_store_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% API for Brick Server
-export([start_link/2,
         read_value/2,
         write_value/2
        ]).

%% API for Write-back and Compaction Modules
-export([writeback_to_stable_storage/3,
         write_location_info/3,
         open_location_info_file_for_read/3,
         read_location_info/5,
         close_location_info_file/3,
         sync/1
        ]).

%% brick_blob_store_hlog only API
-export([open_key_sample_file_for_read/3,
         read_key_samples/5,
         close_key_sample_file/3,
         copy_hunks/5
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
%% types and records
%% ====================================================================

%% -type dict(_A, _B) :: term().

-type wal_entry() :: term().


%% @doc
%% Storage location #w{}, aka wal() type, is used to locate a value
%% blob in a WAL hunk (hunk type 'blob_wal') in a WAL file or a write-
%% backed hunk (hunk type 'blob_single') in a brick private log file.
%% These hunks store only one value blob in one hunk, so they are not
%% efficient in space.

-record(w, {
          wal_seqnum        :: seqnum(),
          wal_hunk_pos      :: offset(),   %% position of the hunk in WAL
          private_seqnum    :: seqnum(),
          private_hunk_pos  :: offset(),   %% position of the hunk in private log
          val_offset        :: offset(),   %% offset of the value from hunk_pos
          val_len           :: len()
         }).
-type wal() :: #w{}.

%% NOTE: (R16B03, 64 bit)
%%
%% > Wal = #w{wal_seqnum=1000, wal_hunk_pos=1000000000,
%%            private_seqnum=1000, private_hunk_pos=1000000000,
%%            val_offset=50, val_len=200}.
%% > erts_debug:size(Wal) * erlang:system_info(wordsize).
%% 64
%% > byte_size(term_to_binary(Wal)).
%% 31
%%
%%
%% Hibari v0.1.x
%%
%% > Wal = {1000, 1000000000}.
%% {1000,1000000000}
%% > erts_debug:size(Wal) * erlang:system_info(wordsize).
%% 24
%% > byte_size(term_to_binary(Wal)).
%% 13
%%


%% @doc
%% Storage location #p{}, aka private_hlog() type is used to locate a
%% value blob in a brick private log after scavenger process. An hunk
%% type will be selected from 'blob_single' and 'blob_multi' depending
%% on its size. If the size is larger than a threshold (around 4KB?),
%% then it will be stored in a blob_single hunk. Otherwise, it will be
%% stored together with other small value blobs in one blob_multi
%% hunk, so that it can avoid overhead of having the hunk enclosure.

-record(p, {
          seqnum      :: seqnum(),
          hunk_pos    :: offset(),     %% position of the hunk in private log
          val_offset  :: offset(),     %% offset of the value from hunk_pos
          val_len     :: len()
         }).
-type private_hlog() :: #p{}.

%% NOTE: (R16B03, 64 bit)
%% > Priv = #p{seqnum=1000, hunk_pos=1000000000,
%%             val_offset=50, val_len=200}.
%% > erts_debug:size(Priv) * erlang:system_info(wordsize).
%% 48
%% byte_size(term_to_binary(Priv)).
%% 21
%%

-type count() :: non_neg_integer().

-type storage_location_hlog() :: private_hlog() | wal() | no_blob.

-record(state, {
          %% name                         :: atom(),
          brick_name                   :: brickname(),
          %% brick_pid                    :: pid(), %% or reg name?
          %% log_dir                      :: file:directory(),
          file_len_max                 :: len(),                    % WAL file size max
          file_len_min                 :: len(),                    % WAL file size min
          hunk_count_min               :: count(),
          head_seqnum                  :: seqnum(),
          head_position                :: offset(),
          head_seqnum_hunk_count=0     :: count(),
          head_seqnum_hunk_overhead=0  :: non_neg_integer()
          %% hunk_overhead=dict:new()     :: dict(seqnum(), non_neg_integer())
         }).
-type state() :: #state{}.

-define(SAMPLING_RATE, 0.05).
-define(SEQNUM_DIGITS, 12).

-define(TIMEOUT, 60 * 1000).
-define(HUNK, brick_hlog_hunk).
-define(WAL, brick_hlog_wal).


%% ====================================================================
%% API
%% ====================================================================

%% -spec start_link() ->
start_link(BrickName, Options) ->
    RegName = list_to_atom(atom_to_list(BrickName) ++ "_blob_store"),
    gen_server:start_link({local, RegName}, ?MODULE, [BrickName, Options], []).

-spec read_value(brickname(), storage_location_hlog()) ->
                        {ok, val()} | eof | {error, term()}.
read_value(BrickName,
           #w{wal_seqnum=WalSeqNum, wal_hunk_pos=WalPos,
              private_seqnum=PrivateSeqNum, private_hunk_pos=PrivatePos,
              val_offset=ValOffset, val_len=ValLen}) ->
    case ?WAL:open_wal_for_read(WalSeqNum) of
        {ok, FH} ->
            %% ?E_DBG("WAL opened for read. SeqNum: ~w, FH: ~p", [WalSeqNum, FH]),
            try
                ?HUNK:read_blob_directly(FH, WalPos, ValOffset, ValLen)
            after
                catch file:close(FH)
            end;
        {error, _}=Err ->
            Err;
        not_available ->
            case open_private_log_for_read(BrickName, PrivateSeqNum) of
                {ok, FH} ->
                    %% ?E_DBG("Private log opened for read. SeqNum: ~w, FH: ~p", [PrivateSeqNum, FH]),
                    try
                        ?HUNK:read_blob_directly(FH, PrivatePos, ValOffset, ValLen)
                    after
                        catch file:close(FH)
                    end;
                {error, _}=Err ->
                    Err
            end
    end;
read_value(BrickName,
           #p{seqnum=SeqNum, hunk_pos=HunkPos,
              val_offset=ValOffset, val_len=ValLen}) ->
    case open_private_log_for_read(BrickName, SeqNum) of
        {ok, FH} ->
            try
                ?HUNK:read_blob_directly(FH, HunkPos, ValOffset, ValLen)
            after
                catch file:close(FH)
            end;
        {error, _}=Err ->
            Err
    end.

-spec write_value(pid(), val()) -> {ok, storage_location_hlog()} | {error, term()}.
write_value(_Pid, <<>>) ->
    {ok, no_blob};
write_value(Pid, Value) ->
    gen_server:call(Pid, {write_value, Value}, ?TIMEOUT).

%% @TODO CHECKME: Is wal_entry() actually an hunk()? -> yes
-spec writeback_to_stable_storage(pid(), brickname(), [wal_entry()]) -> ok | {error, term()}.
writeback_to_stable_storage(_Pid, BrickName, WalEntries) ->
    case writeback_values(BrickName, WalEntries, undefined, -1, -1, 0) of
        {ok, _CurrentWBSeqNum, _CurrentWBPos, _SuccessCount} ->
            ok;
        Err ->
            Err
    end.

-spec write_location_info(pid(), brickname(), [{key(), ts(), storage_location_hlog()}]) ->
                                 ok | {error, term()}.
write_location_info(_Pid, BrickName, Locations) ->
    LocationsGroupBySeqNum = convert_to_location_tuples(Locations),
    %% @TODO ENHANCEME: Support full write-back
    Result =
        lists:foldl(
          fun({SeqNum, LocationsForSeqNum}, {SC1, undefined}) ->
                  case open_private_location_files_for_write(BrickName, SeqNum) of
                      {ok, LocationFile, KeySampleFile} ->
                          KeySamples = key_samples(LocationsForSeqNum, ?SAMPLING_RATE),
                          try disk_log:log_terms(KeySampleFile, KeySamples) of
                              ok ->
                                  case disk_log:log_terms(LocationFile, LocationsForSeqNum) of
                                      ok ->
                                          %% {SC1 + length(LocationsForSeqNum), undefined};
                                          {0, undefined};
                                      Err1 ->
                                          {SC1, Err1}
                                  end;
                              Err2 ->
                                  {SC1, Err2}
                          after
                              _ = (catch disk_log:close(LocationFile)),
                              _ = (catch disk_log:close(KeySampleFile))
                          end;
                      Err3 ->
                          {SC1, Err3}
                  end;
             (_, Acc) ->
                  Acc   %% skip the rest of location info
          end, {0, undefined}, LocationsGroupBySeqNum),
    case Result of
        {_SuccessCount, undefined} ->
            ok;
        {_SuccessCount, Err} ->
            Err
    end.

-spec open_location_info_file_for_read(pid(), brickname(), seqnum()) ->
                                              {ok, location_info_file()} | {err, term()}.
open_location_info_file_for_read(_Pid, BrickName, SeqNum) ->
    open_private_location_file_for_read(BrickName, SeqNum).

-spec read_location_info(pid(), brickname(), location_info_file(),
                         'start' | continuation(), non_neg_integer()) ->
                                {ok, continuation(), [{key(), ts(), storage_location_hlog()}]}
                                    | 'eof'
                                    | {error, term()}.
read_location_info(_Pid, _BrickName, DiskLog, Cont, MaxRecords) ->
    case disk_log:chunk(DiskLog, Cont, MaxRecords) of
        eof ->
            eof;
        {error, _}=Err ->
            Err;
        {NewCont, Locations} ->
            {ok, NewCont, Locations}
        %% @TODO {Cont, Locations, BadTypes} ->
    end.

-spec close_location_info_file(pid(), brickname(), location_info_file()) -> ok.
close_location_info_file(_Pid, _BrickName, DiskLog) ->
    disk_log:close(DiskLog).

-spec open_key_sample_file_for_read(pid(), brickname(), seqnum()) ->
                                              {ok, key_sample_file()} | {err, term()}.
open_key_sample_file_for_read(_Pid, BrickName, SeqNum) ->
    open_private_key_sample_file_for_read(BrickName, SeqNum).

-spec read_key_samples(pid(), brickname(), key_sample_file(),
                       'start' | continuation(), non_neg_integer()) ->
                              {ok, continuation(), [{key(), ts()}]}
                                  | 'eof'
                                  | {error, term()}.
read_key_samples(_Pid, _BrickName, DiskLog, Cont, MaxRecords) ->
    %% @TODO: Exact same to read_location_info/4.
    case disk_log:chunk(DiskLog, Cont, MaxRecords) of
        eof ->
            eof;
        {error, _}=Err ->
            Err;
        {NewCont, Locations} ->
            {ok, NewCont, Locations}
        %% @TODO {Cont, Locations, BadTypes} ->
    end.

-spec close_key_sample_file(pid(), brickname(), key_sample_file()) -> ok.
close_key_sample_file(_Pid, _BrickName, DiskLog) ->
    disk_log:close(DiskLog).

-spec copy_hunks(pid(), brickname(), seqnum(), [location_info()], blob_age()) -> [brick_ets:store_tuple()].
copy_hunks(Pid, BrickName, SeqNum, Locations, AgeThreshold) ->
    {ok, SourceHLog} = open_private_log_for_read(BrickName, SeqNum),
    try
        %% @TODO Read only certain bytes (e.g. 20MB)
        {ok, HunksAndLocations} = read_hunks(SourceHLog, Locations),
        {YoungHunksAndLocations, _OldHunksAndLocations} = group_hunks(HunksAndLocations, AgeThreshold),
        StoreTuples1 = write_hunks(Pid, BrickName, short_term, YoungHunksAndLocations, []),
        %% write_hunks(Pid, BrickName, long_term, OldHunksAndLocations, StoreTuples1)
        StoreTuples1   %% @TODO: DELETEME
    after
        _ = (catch file:close(SourceHLog))
    end.

-spec sync(pid()) -> ok.
sync(_Pid) ->
    ok.  %% @TODO


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([BrickName, Options]) ->
    process_flag(trap_exit, true),
    %% process_flag(priority, high),

    DefaultLenMax = 1.5 * 1024 * 1024 * 1024, %% 1.5GB
    %% DefaultLenMin =  64 * 1024 * 1024,        %%  64MB
    DefaultLenMin =   2 * 1024 * 1024,        %%  2MB
    LenMax = proplists:get_value(file_len_max, Options, DefaultLenMax),
    LenMin = min(LenMax - 1, proplists:get_value(file_len_min, Options, DefaultLenMin)),
    %% Minimum hunk count. If each blob hunk is 10MB and corresponding metadata
    %% is 100 bytes, the minimum WAL size will be 4.9GB. But it's capped by LenMax
    %% which is 1.5GB by default.
    HunkCountMin = proplists:get_value(hunk_count_min, Options, 100),
    %% HunkCountMin = proplists:get_value(hunk_count_min, Options, 1000),

    Dir = blob_dir(BrickName),
    CurSeq = case filelib:wildcard("*.hlog", Dir) of
                 [] ->
                     1;
                 HLogFiles ->
                     LastFile = filename:basename(lists:max(HLogFiles), ".hlog"),
                     %% substr's start position is 1-based index
                     LastSeqNumStr = string:substr(LastFile, length(LastFile) - ?SEQNUM_DIGITS + 1),
                     list_to_integer(LastSeqNumStr) + 1
             end,
    Position = create_private_log(BrickName, CurSeq),
    {ok, #state{brick_name=BrickName,
                file_len_max=LenMax,
                file_len_min=LenMin,
                hunk_count_min=HunkCountMin,
                head_seqnum=CurSeq,
                head_position=Position}}.

handle_call({write_value, Value}, _From, #state{brick_name=BrickName}=State)
  when is_binary(Value) ->
    State1 =
        case should_advance_seqnum(State) of
            true ->
                do_advance_seqnum(1, State);
            false ->
                State
        end,
    SeqNum2      = State1#state.head_seqnum,
    Position2    = State1#state.head_position,
    HunkCount    = State1#state.head_seqnum_hunk_count,
    HunkOverhead = State1#state.head_seqnum_hunk_overhead,

    %% @TODO: Maybe store the key as well
    WALBlobs = [Value, term_to_binary({SeqNum2, Position2})],
    Flags = [],
    {WALHunkIOList, _, _, [WALValOffset, _]} =
        ?HUNK:create_hunk_iolist(#hunk{type=blob_wal, flags=Flags,
                                       brick_name=BrickName, blobs=WALBlobs}),

    case ?WAL:write_hunk(WALHunkIOList) of
        {ok, WALSeqNum, WALPosition} ->
            ValLen = byte_size(Value),
            StoreLoc =
                #w{wal_seqnum=WALSeqNum, wal_hunk_pos=WALPosition,
                   private_seqnum=SeqNum2, private_hunk_pos=Position2,
                   val_offset=WALValOffset, val_len=ValLen},
            {RawSize, _, PaddingSize, Overhead} =
                ?HUNK:calc_hunk_size(blob_single, Flags, 0, 1, ValLen),
            State2 =
                State1#state{
                  head_seqnum=SeqNum2, head_position=Position2 + RawSize + PaddingSize,
                  head_seqnum_hunk_count=HunkCount + 1,
                  head_seqnum_hunk_overhead=HunkOverhead + Overhead},
            {reply, {ok, StoreLoc}, State2};
        Err ->
            {reply, Err, State1}
    end;
handle_call({reserve_positions, LogType, Hunks}, _From, State) ->
    {Positions, State1} = lists:foldl(reserve_position_fun(LogType), {[], State}, Hunks),
    {reply, lists:reverse(Positions), State1};
handle_call(_, _From, State) ->
    {reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions - supporting functions for gen_server callbacks
%% ====================================================================

-spec reserve_position_fun('short_term' | 'long_term')
                          -> fun((hunk(), {[private_hlog()], state()}) -> {[private_hlog()], state()}).
reserve_position_fun(short_term) ->
    fun(#hunk{flags=Flags, blobs=[Value]=Blobs}, {StoreLocList, State}) ->
            State1 =
                case should_advance_seqnum(State) of
                    true ->
                        do_advance_seqnum(1, State);
                    false ->
                        State
                end,
            SeqNum       = State1#state.head_seqnum,
            Position     = State1#state.head_position,
            HunkCount    = State1#state.head_seqnum_hunk_count,
            HunkOverhead = State1#state.head_seqnum_hunk_overhead,

            ValLen = byte_size(Value),
            {[ValOffset], _} = ?HUNK:create_blob_index(Blobs),
            StoreLoc =
                #p{seqnum=SeqNum, hunk_pos=Position, val_offset=ValOffset, val_len=ValLen},
            {RawSize, _, PaddingSize, Overhead} =
                ?HUNK:calc_hunk_size(blob_single, Flags, 0, 1, ValLen),
            State2 = State1#state{
                       head_seqnum=SeqNum, head_position=Position + RawSize + PaddingSize,
                       head_seqnum_hunk_count=HunkCount + 1,
                       head_seqnum_hunk_overhead=HunkOverhead + Overhead},
            {[StoreLoc | StoreLocList], State2}
    end;
reserve_position_fun(long_term) ->
    %% @TODO
    fun(_, Acc) ->
            Acc
    end.

%% ====================================================================
%% Internal functions - write-back
%% ====================================================================

%% @TODO: Rewrite like write_location_info/3 (take lists:foldl style rather than recursive calls)?
-spec writeback_values(brickname(), [hunk()], file:fd(), seqnum(), offset(), non_neg_integer())
                       -> {ok, seqnum(), offset(), non_neg_integer()} | {error, term()}.
writeback_values(_BrickName, [], FH, CurrentWBSeqNum, CurrentWBPos, SuccessCount) ->
    if
        FH =/= undefined ->
            _ = (catch file:close(FH));
        true ->
            ok
    end,
    {ok, CurrentWBSeqNum, CurrentWBPos, SuccessCount};
writeback_values(BrickName, [#hunk{flags=Flags, blobs=[Value, LocationBin]} | WALEntries],
                 FH, CurrentWBSeqNum, CurrentWBPos, SuccessCount) ->
    {WBSeqNum, WBPos} = binary_to_term(LocationBin),
    case get_private_log_for_writeback(BrickName, FH, WBSeqNum, CurrentWBSeqNum) of
        {error, Err1} ->
            {error, {Err1, WBSeqNum, WBPos, SuccessCount}};
        {ok, FH1} ->
            {HunkIOList, HunkSize, _, _} =
                ?HUNK:create_hunk_iolist(#hunk{type=blob_single, flags=Flags,
                                               blobs=[Value], blob_ages=[0]}),
            Pos = if
                      WBSeqNum =:= CurrentWBSeqNum, WBPos =:= CurrentWBPos ->
                          undefined;
                      true ->
                          WBPos
                  end,

            try write_value_to_private_log(FH1, Pos, list_to_binary(HunkIOList)) of
                ok ->
                    %% repeat
                    writeback_values(BrickName, WALEntries, FH1,
                                     WBSeqNum, WBPos + HunkSize, SuccessCount + 1);
                {error, Err2} ->
                    _ = (catch file:close(FH1)),
                    {error, {Err2, CurrentWBSeqNum, WBPos, SuccessCount}}
            catch
                _:_ = Err3 ->
                    _ = (catch file:close(FH1)),
                    {error, {Err3, CurrentWBSeqNum, WBPos, SuccessCount}}
            end
    end.

-spec get_private_log_for_writeback(brickname(), file:fd() | undefined, seqnum(), seqnum()) ->
                                           {ok, file:fd()} | {error, term()}.
get_private_log_for_writeback(_BrickName, FH, SeqNum, SeqNum) when FH =/= undefined ->
    {ok, FH};
get_private_log_for_writeback(BrickName, undefined, SeqNum, _CurrentWBSeqNum) ->
    open_private_log_for_write(BrickName, SeqNum);
get_private_log_for_writeback(BrickName, FH, SeqNum, _CurrentWBSeqNum) ->
    _ = (catch file:close(FH)),
    open_private_log_for_write(BrickName, SeqNum).

write_value_to_private_log(FH, undefined, HunkBytes) ->
    file:write(FH, HunkBytes);
write_value_to_private_log(FH, WBPos, HunkBytes) ->
    case file:position(FH, WBPos) of
        {ok, WBPos} ->
            file:write(FH, HunkBytes);
        Err ->
            ?ELOG_ERROR("~p", [Err]),
            Err
    end.


%% ====================================================================
%% Internal functions - location info
%% ====================================================================

-spec convert_to_location_tuples([{key(), ts(), storage_location_hlog()}]) ->
                                        {seqnum(), [location_info()]}.
convert_to_location_tuples(Locations) ->
    LocationGroupBySeqNum =
        lists:foldl(
          fun({Key, TS, StorageLocation}, Dict) ->
                  {SeqNum, LocationTuple} = location_tuple(Key, TS, StorageLocation),
                  dict:append(SeqNum, LocationTuple, Dict)
          end, dict:new(), Locations),
    dict:to_list(LocationGroupBySeqNum).

-spec location_tuple(key(), ts(), storage_location()) -> {seqnum(), location_info()}.
location_tuple(Key, TS, #w{private_seqnum=SeqNum, private_hunk_pos=HunkPos,
                           val_offset=ValOffset, val_len=ValLen}) ->
    {SeqNum, #l{hunk_pos=HunkPos, val_offset=ValOffset, val_len=ValLen,
                key=Key, timestamp=TS}};
location_tuple(Key, TS, #p{seqnum=SeqNum, hunk_pos=HunkPos,
                           val_offset=ValOffset, val_len=ValLen}) ->
    {SeqNum, #l{hunk_pos=HunkPos, val_offset=ValOffset, val_len=ValLen,
                key=Key, timestamp=TS}}.

-spec key_samples([location_info()], float()) -> [{key(), ts()}].
key_samples(Locations, SamplingRate) ->
    %% Returns keys in reverse order
    lists:foldl(
      fun(Location, Acc) ->
              case random:uniform() =< SamplingRate of
                  true ->
                      #l{key=Key, timestamp=TS} = Location,
                      [{Key, TS} | Acc];
                  false ->
                      Acc
              end
      end, [], Locations).


%% ====================================================================
%% Internal functions - compaction (copy hunks)
%% ====================================================================

%% @TODO: MOVEME: Move to brick_hlog_hunk module
-spec read_hunks(file:fd(), [location_info()]) ->
                        {ok, [{hunk(), location_info()}]} | {error, term()}.
read_hunks(FH, Locations) ->
    HunksAndLocations =
        lists:map(
          fun(#l{hunk_pos=Position, val_offset=ValOffset, val_len=ValLen}=Location) ->
                  %% @TODO: Use the acculate number instead of 64
                  Length = ValOffset + ValLen + 64,
                  {ok, Bin} = file:pread(FH, Position, Length),
                  %% @TODO: This won't work with blob_multi.
                  {ok, Hunks, _Remainder} = ?HUNK:parse_hunks(Bin),
                  {hd(Hunks), Location}
          end, Locations),
    {ok, HunksAndLocations}.

-spec group_hunks([{hunk(), location_info()}], blob_age()) ->
                         {Young::[{hunk(), location_info()}],
                          Old::  [{hunk(), location_info()}]}.
group_hunks(HunksAndLocations, AgeThreshold) ->
    %% @TODO: Move this to hlog_hunk module and implement in-place age update.
    %% @TODO: Support blob_multi (e.g. new function hlog_hunk:merge/unmerge_hunks)
    HunksAndLocations1 =
        lists:map(fun({#hunk{type=blob_single, blob_ages=[Age]}=Hunk, Location}) ->
                               {Hunk#hunk{blob_ages=[Age + 1]}, Location}
                       end, HunksAndLocations),
    lists:partition(fun({#hunk{type=blob_single, blob_ages=[Age]}, _Location}) ->
                            Age < AgeThreshold
                    end, HunksAndLocations1).

-spec write_hunks(pid(), brickname(), 'short_term' | 'long_term', [{hunk(), location_info()}],
                  AppendTo::[location_info()]) -> [brick_ets:store_tuple()].
write_hunks(Pid, BrickName, HLogType, HunksAndLocations, AppendTo) ->
    Hunks = [ Hunk || {Hunk, _} <- HunksAndLocations ],
    Positions = gen_server:call(Pid, {reserve_positions, HLogType, Hunks}, ?TIMEOUT),
    {StoreTuples, FH, _} =
        lists:foldl(
          fun({{Hunk, #l{val_len=ValLen, key=Key, timestamp=TS}},
               #p{seqnum=SeqNum, hunk_pos=Position}=StoreLoc},
              {StoreTuples0, FH0, CurSeqNum}) ->
                  {HunkIOList, _, _, _} = ?HUNK:create_hunk_iolist(Hunk),
                  {ok, FH1} = get_private_log_for_writeback(BrickName, FH0, SeqNum, CurSeqNum),
                  ok = write_value_to_private_log(FH1, Position, list_to_binary(HunkIOList)),
                  StoreTuple = brick_ets:storetuple_make(Key, TS, StoreLoc, ValLen, 0, []),
                  %% ?ELOG_DEBUG("~n~s", [storetuple_to_display_iolist(StoreTuple)]),
                  {[StoreTuple | StoreTuples0], FH1, SeqNum}
          end, {lists:reverse(AppendTo), undefined, -1}, lists:zip(HunksAndLocations, Positions)),
    if
        FH =/= undefined ->
            _ = (catch file:close(FH));
        true ->
            ok
    end,
    lists:reverse(StoreTuples).

%% -spec storetuple_to_display_iolist(brick_ets:store_tuple()) -> iolist().
%% storetuple_to_display_iolist(StoreTuple) ->
%%     Key = safe_binary_to_string(brick_ets:storetuple_key(StoreTuple)),
%%     TS  = brick_ets:storetuple_ts(StoreTuple),
%%     Val = brick_ets:storetuple_val(StoreTuple),
%%     io_lib:format("key: ~s, ts: ~w, val: ~p", [Key, TS, Val]).

%% -spec safe_binary_to_string(binary()) -> string().
%% safe_binary_to_string(Bin) ->
%%     try
%%         Term = binary_to_term(Bin),
%%         lists:flatten(io_lib:format("~p", [Term]))
%%     catch
%%         error:badarg ->
%%             lists:flatten(io_lib:format("~p", [Bin]))
%%     end.


%% ====================================================================
%% Internal functions - files
%% ====================================================================

-spec should_advance_seqnum(state()) -> boolean().
should_advance_seqnum(#state{head_position=Position, file_len_max=MaxLen, file_len_min=MinLen,
                             head_seqnum_hunk_count=HunkCount, hunk_count_min=MinHunkCount}) ->
    Position >= MaxLen
        orelse (Position >= MinLen andalso HunkCount >= MinHunkCount).

-spec do_advance_seqnum(non_neg_integer(), state()) -> state().
do_advance_seqnum(Incr, #state{brick_name=BrickName, head_seqnum=SeqNum}=State) ->
    NewSeqNum = SeqNum + Incr,
    NewPosition = create_private_log(BrickName, NewSeqNum),
    State#state{head_seqnum=NewSeqNum, head_position=NewPosition,
                head_seqnum_hunk_count=0, head_seqnum_hunk_overhead=0}.

%% @TODO: Better error handling
-spec create_private_log(brickname(), seqnum()) -> offset().
create_private_log(BrickName, SeqNum) ->
    Path = blob_path(BrickName, blob_dir(BrickName), SeqNum),
    {error, enoent} = file:read_file_info(Path),  %% sanity
    case open_private_log_for_write(BrickName, SeqNum) of
        {ok, FH} ->
            _ = (catch file:close(FH)),
            try
                %% ok = write_log_header(FH),
                ?E_INFO("Created brick private log with sequence ~w: ~s", [SeqNum, Path]),
                {ok, FI} = file:read_file_info(Path),
                FI#file_info.size
            catch
                _:_=Err ->
                    error(Err)
            end;
        Err ->
            error(Err)
    end.

-spec open_private_log_for_read(brickname(), seqnum()) -> {ok, file:fd()} | {error, term()}.
open_private_log_for_read(BrickName, SeqNum) ->
    Path = blob_path(BrickName, blob_dir(BrickName), SeqNum),
    case file:open(Path, [binary, raw, read, read_ahead]) of
        {ok, _}=Res ->
            Res;
        Res ->
            ?E_CRITICAL("Couldn't open blob file [read] ~s by ~w", [Path, Res]),
            Res
    end.

-spec open_private_log_for_write(brickname(), seqnum()) -> {ok, file:fd()} | {error, term()}.
open_private_log_for_write(BrickName, SeqNum) ->
    BlobDir = blob_dir(BrickName),
    Path = blob_path(BrickName, BlobDir, SeqNum),
    %% 'read' option is required otherwise the file will be truncated.
    case file:open(Path, [binary, raw, read, write, delayed_write]) of
        {ok, _FH}=Res ->
            Res;
        {error, enoent}=Err ->
            case filelib:is_dir(BlobDir) of
                true ->
                    ?E_CRITICAL("Couldn't open blob file [write] ~s by ~w", [Path, Err]),
                    Err;
                false ->
                    filelib:ensure_dir(BlobDir),
                    file:make_dir(BlobDir),
                    %% @TODO: Add retry count to avoid an infinite loop.
                    %% retry
                    open_private_log_for_write(BrickName, SeqNum)
            end;
        {error, _}=Err ->
            Err
    end.

-spec open_private_location_file_for_read(brickname(), seqnum()) ->
                                                 {ok, disk_log:log()} | {error, term()}.
open_private_location_file_for_read(BrickName, SeqNum) ->
    BlobDir = blob_dir(BrickName),
    Path = blob_location_file_path(BrickName, BlobDir, SeqNum),
    disk_log:open([{name, Path}, {file, Path}, {mode, read_only}]).

-spec open_private_key_sample_file_for_read(brickname(), seqnum()) ->
                                                   {ok, disk_log:log()} | {error, term()}.
open_private_key_sample_file_for_read(BrickName, SeqNum) ->
    BlobDir = blob_dir(BrickName),
    Path = blob_key_sample_file_path(BrickName, BlobDir, SeqNum),
    disk_log:open([{name, Path}, {file, Path}, {mode, read_only}]).

-spec open_private_location_files_for_write(brickname(), seqnum()) ->
                                                   {ok,
                                                    LocationInfoFile::disk_log:log(),
                                                    KeySampleFile::disk_log:log()}
                                                       | {error, term()}.
open_private_location_files_for_write(BrickName, SeqNum) ->
    BlobDir = blob_dir(BrickName),
    {LPath, SPath} = blob_location_and_key_sample_paths(BrickName, BlobDir, SeqNum),
    case disk_log:open([{name, LPath}, {file, LPath}, {mode, read_write}]) of
        {ok, FH1} ->
            case disk_log:open([{name, SPath}, {file, SPath}, {mode, read_write}]) of
                {ok, FH2} ->
                    {ok, FH1, FH2};
                {error, Err} ->
                    {error, {key_sample_file, SPath, Err}}
            end;
        {error, enoent}=Err ->
            case filelib:is_dir(BlobDir) of
                true ->
                    ?E_CRITICAL("Couldn't open blob location file [write] ~s by ~w", [LPath, Err]),
                    Err;
                false ->
                    filelib:ensure_dir(BlobDir),
                    file:make_dir(BlobDir),
                    %% @TODO: Add retry count to avoid an infinite loop.
                    %% retry
                    open_private_location_files_for_write(BrickName, SeqNum)
            end;
        {error, Err} ->
            {error, {location_info_file, LPath, Err}}
    end.

-spec blob_dir(brickname()) -> dirname().
blob_dir(BrickName) ->
    %% @TODO: Get the data_dir from #state{}.
    {ok, FileDir} = application:get_env(gdss_brick, brick_default_data_dir),
    filename:join([FileDir, atom_to_list(BrickName), "blob"]).

-spec blob_path(brickname(), dirname(), seqnum()) -> filepath().
blob_path(BrickName, Dir, SeqNum) ->
    filename:join(Dir, seqnum2file(BrickName, SeqNum, ".hlog")).

-spec blob_location_file_path(brickname(), dirname(), seqnum()) -> filepath().
blob_location_file_path(BrickName, Dir, SeqNum) ->
    filename:join(Dir, seqnum2file(BrickName, SeqNum, ".location")).

-spec blob_key_sample_file_path(brickname(), dirname(), seqnum()) -> filepath().
blob_key_sample_file_path(BrickName, Dir, SeqNum) ->
    filename:join(Dir, seqnum2file(BrickName, SeqNum, ".keysample")).

-spec blob_location_and_key_sample_paths(brickname(), dirname(), seqnum()) -> filepath().
blob_location_and_key_sample_paths(BrickName, Dir, SeqNum) ->
    {P1, P2} = seqnum2files(BrickName, SeqNum, ".location", ".keysample"),
    {filename:join(Dir, P1), filename:join(Dir, P2)}.

-spec seqnum2file(brickname(), seqnum(), string()) -> string().
seqnum2file(BrickName, SeqNum, Suffix) ->
    lists:flatten([atom_to_list(BrickName), "-",
                   gmt_util:left_pad(integer_to_list(SeqNum), ?SEQNUM_DIGITS, $0), Suffix]).

-spec seqnum2files(brickname(), seqnum(), string(), string()) -> {string(), string()}.
seqnum2files(BrickName, SeqNum, Suffix1, Suffix2) ->
    BaseName = lists:flatten([atom_to_list(BrickName), "-",
                              gmt_util:left_pad(integer_to_list(SeqNum), ?SEQNUM_DIGITS, $0)]),
    {BaseName ++ Suffix1, BaseName ++ Suffix2}.
