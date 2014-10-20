%%%-------------------------------------------------------------------
%%% Copyright (c) 2008-2014 Hibari developers. All rights reserved.
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
%% brick name + alpha is used to identify a process.

-module(brick_blob_store_hlog).

-behaviour(gen_server).

-include("brick_specs.hrl").
-include("brick_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% API for Brick Server
-export([start_link/2,
         read_value/1,
         write_value/2
        ]).

%% API for Write-back Module
-export([writeback_to_stable_storage/2,
         sync/1
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
-export([test_start_link/0,
         test1/0,
         test2/0
        ]).


%% ====================================================================
%% types and records
%% ====================================================================

-type dict(_A, _B) :: term().

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


-type storage_location_hlog() :: private_hlog() | wal() | no_blob.

-record(state, {
          name                         :: atom(),
          brick_name                   :: brickname(),
          brick_pid                    :: pid(), %% or reg name?
          log_dir                      :: file:directory(),
          head_seqnum                  :: seqnum(),
          head_position                :: offset(),
          writeback_seqnum             :: seqnum(),
          writeback_position           :: offset(),
          cur_seqnum_hunk_overhead=0   :: non_neg_integer(),
          hunk_overhead=dict:new()     :: dict(seqnum(), non_neg_integer()),
          writeback_pid                :: pid()
         }).

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

-spec read_value(storage_location_hlog()) ->
                        {ok, val()} | eof | {error, term()}.
read_value(#w{wal_seqnum=WalSeqNum, wal_hunk_pos=WalPos,
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
            case open_private_log_for_read(PrivateSeqNum) of
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
read_value(#p{seqnum=SeqNum, hunk_pos=HunkPos,
              val_offset=ValOffset, val_len=ValLen}) ->
    case open_private_log_for_read(SeqNum) of
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

%% @TODO CHECKME: Is wal_entry() actually an hunk()?
-spec writeback_to_stable_storage(pid(), [wal_entry()]) -> ok | {error, term()}.
writeback_to_stable_storage(Pid, WALEntries) ->
    case gen_server:call(Pid, begin_writeback) of
        {error, _}=Err1 ->
            Err1;
        {ok, WBSeqNum, WBPos} ->
            case catch writeback_value(WALEntries, undefined, WBSeqNum, WBPos, 0) of
                {ok, NextWBSeqNum, NextWBPos, _SuccessCount} ->
                    ok;
                {error, Err2, NextWBSeqNum, NextWBPos, _SuccessCount} ->
                    {error, Err2}
            end,
            gen_server:call(Pid, {end_writeback, NextWBSeqNum, NextWBPos})
    end.

-spec sync(pid()) -> ok.
sync(_Pid) ->
    ok.


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([BrickName, _Options]) ->
    process_flag(trap_exit, true),
    %% process_flag(priority, high),

    CurSeq = 1,
    CurPos = 0,
    WBSeq  = 1,
    WBPos  = 0,

    {ok, #state{brick_name=BrickName,
                head_seqnum=CurSeq,
                head_position=CurPos,
                writeback_seqnum=WBSeq,
                writeback_position=WBPos}}.

handle_call({write_value, Value}, _From,
            #state{brick_name=BrickName, head_seqnum=SeqNum, head_position=Position,
                   cur_seqnum_hunk_overhead=HunkOverhead}=State)
  when is_binary(Value) ->
    %% @TODO: Advance seq num if necessary
    %% if
    SeqNum2 = SeqNum,
    Position2 = Position,

    %% @TODO: Maybe store the key as well
    WALBlobs = [Value, term_to_binary({SeqNum2, Position2})],
    Flags = [],
    {WALHunkIOList, _, _, [WALValOffset, _]} =
        ?HUNK:create_hunk_iolist(#hunk{type=blob_wal, flags=Flags,
                                       brick_name=BrickName, blobs=WALBlobs}),

    case ?WAL:write_hunk(WALHunkIOList) of
        {ok, WALSeqNum, WALPosition} ->
            ValLen = byte_size(Value),
            {_, RawSize, PaddingSize, Overhead} = ?HUNK:calc_hunk_size(Flags, 0, 1, ValLen),
            StoreLoc = #w{wal_seqnum=WALSeqNum, wal_hunk_pos=WALPosition,
                          private_seqnum=SeqNum2, private_hunk_pos=Position2,
                          val_offset=WALValOffset, val_len=ValLen},
            State1 = State#state{head_seqnum=SeqNum2, head_position=Position2 + RawSize + PaddingSize,
                                 cur_seqnum_hunk_overhead=HunkOverhead + Overhead},
            {reply, {ok, StoreLoc}, State1};
        Err ->
            {reply, Err, State}
    end;
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
%% Internal functions
%% ====================================================================

open_private_log_for_read(_SeqNum) ->
    error(not_implemented).

open_private_log_for_append(_WBSeqNum, _WBPos) ->
    error(not_implemented).

write_value_to_private_log(_FH, _WBSeqNum, _WBPos, _Flags, _Value) ->
    error(not_implemented).

writeback_value([], FH, CurrentWBSeqNum, CurrentWBPos, SuccessCount) ->
    catch file:close(FH),
    {ok, CurrentWBSeqNum, CurrentWBPos, SuccessCount};
writeback_value([#hunk{flags=Flags, blobs=[Value, LocationBin]} | WALEntries],
                FH, CurrentWBSeqNum, CurrentWBPos, SuccessCount) ->
    {WBSeqNum, WBPos} = binary_to_term(LocationBin),
    if
        FH =:= undefined; WBSeqNum =/= CurrentWBSeqNum ->
            if
                FH =/= undefined ->
                    catch file:close(FH);
                true ->
                    ok
            end,
            case open_private_log_for_append(WBSeqNum, WBPos) of
                {ok, FH1} ->
                    ok,
                    ErrorResponse = undefined;
                {error, bad_position=Err1} ->
                    FH1 = undefined,
                    ErrorResponse = {error, Err1, CurrentWBSeqNum, CurrentWBPos, SuccessCount}
            end;
        true ->
            if
                WBSeqNum =:= CurrentWBSeqNum, WBPos =:= CurrentWBPos ->
                    FH1 = FH,
                    ErrorResponse = undefined;
                true ->
                    catch file:close(FH),
                    FH1 = undefined,
                    ErrorResponse = {error, bad_position, CurrentWBSeqNum, CurrentWBPos, SuccessCount}
            end
    end,

    if
        ErrorResponse =/= undefined ->
            ErrorResponse;
        true ->
            try write_value_to_private_log(FH1, WBSeqNum, WBPos, Flags, Value) of
                {ok, NextWBSeqNum, NextWBPos} ->
                    writeback_value(WALEntries, FH1, NextWBSeqNum, NextWBPos, SuccessCount + 1);
                {error, Err2} ->
                    catch file:close(FH1),
                    {error, Err2, CurrentWBSeqNum, CurrentWBPos, SuccessCount}
            catch
                _:_ = Err3 ->
                    catch file:close(FH1),
                    {error, Err3, CurrentWBSeqNum, CurrentWBPos, SuccessCount}
            end
    end.


%% DEBUG

%% -define(BLOBSTORE1, table_ch1_b1_blob_store).

test_start_link() ->
    {ok, _Pid} = start_link(table1_ch1_b1, []).

test1() ->
    write_value(blob_store, <<>>).

test2() ->
    write_value(blob_store, <<"value1">>).


