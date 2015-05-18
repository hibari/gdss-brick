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

%% -behaviour(gen_server).

%% -include("brick_specs.hrl").
-include("brick_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% API
-export([start_link/1,
         stop/0
         %% register_local_brick/1,
         %% full_writeback/0,
         %% get_all_registrations/0
        ]).

%% gen_server callbacks
%% -export([init/1,
%%          handle_call/3,
%%          handle_cast/2,
%%          handle_info/2,
%%          terminate/2,
%%          code_change/3
%%         ]).

%% DEBUG
-export([read_wal_entries/0,
         read_wal_entry/0
        ]).


%% ====================================================================
%% types, specs and records
%% ====================================================================

%% -type from() :: {pid(), term()}.
-type prop() :: [].

%% -type brickname() :: atom().

%% -record(state, {
%%           registory=orddict:new() :: orddict(brickname())
%%          }).

-define(HUNK, brick_hlog_hunk).
-define(WAL, brick_hlog_wal).

%% ====================================================================
%% API
%% ====================================================================

-spec start_link([prop()]) -> {ok,pid()} | ignore | {error,term()}.
start_link(PropList) ->
    gen_server:start_link(?MODULE, PropList, []).

-spec stop() -> ok | {error, term()}.
stop() ->
    gen_server:call(?WRITEBACK_SERVER_REG_NAME, stop).

%% -spec register_local_brick(brickname()) -> ok | {error, term()}.
%% register_local_brick(LocalBrick) when is_atom(LocalBrick) ->
%%     gen_server:call(?WRITEBACK_SERVER_REG_NAME, {register_local_brick, LocalBrick}).

%% full_writeback() ->
%%     full_writeback(?WRITEBACK_SERVER_REG_NAME).

%% -spec get_all_registrations() -> [brickname()].
%% get_all_registrations() ->
%%     get_all_registrations(?WRITEBACK_SERVER_REG_NAME).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================


%% ====================================================================
%% Internal functions
%% ====================================================================

%% MOVEME: to brick_metadata_store leveldb module.

%% -spec write_back_to_stable_storege(brickname(), [metadata_tuple()]) -> ok.
%% write_back_to_stable_storege(_BrickName, [], Errors) ->
%%     lists:reverse(Errors);
%% write_back_to_stable_storege(BrickName,
%%                              [{eee, _BrickName, _SeqNum, _Offset, _Key, _TypeNum,
%%                                _H_Len, [Summary, CBlobs, _UBlobs]} | RemainingMetaDataTuples],
%%                              #state{hlog_pid=HLog}=State,
%%                              Errors) ->
%%     case gmt_hlog:parse_hunk_summary(Summary) of
%%         #hunk_summ{c_len=CLen, u_len=ULen}=ParsedSummary ->
%%             [_] = CLen,        sanity
%%             []  = ULen,        sanity
%%             MetadataBin = gmt_hlog:get_hunk_cblob_member(ParsedSummary, CBlobs, 1),
%%             case gmt_hlog:md5_checksum_ok_p(ParsedSummary#hunk_summ{c_blobs=[MetadataBin]}) of
%%                 true ->
%%                     MetadataDB = gmt_hlog:get_metadata_db(HLog, BrickName),
%%                     DoMods = binary_to_term(MetadataBin),
%%                     IsLastBatch = RemainingMetaDataTuples =:= [],
%%                     ok = write_back_to_metadata_db(MetadataDB, BrickName, DoMods, IsLastBatch);
%%                 false ->
%%                     @TODO: Do not crash, and do sync
%%                     error({invalid_checksum, {_BrickName, _SeqNum, _Offset, _Key}})
%%             end;
%%         too_big ->
%%             @TODO: Do not crash, and do sync
%%             error({hunk_is_too_big_to_parse, {_BrickName, _SeqNum, _Offset, _Key}})
%%     end,
%%     write_back_to_stable_storege1(BrickName, RemainingMetaDataTuples, State, Errors).


%% ====================================================================
%% Internal functions - Debugging
%% ====================================================================

%% DEBUG
read_wal_entries() ->
    {ok, FH} = ?WAL:open_wal_for_read(1),
    try file:read(FH, 100 * 1024 * 1024) of  %% 100MB!
        {ok, Binary} ->
            ?HUNK:parse_hunks(Binary)
    after
        catch file:close(FH)
    end.

%% DEBUG
read_wal_entry() ->
    {ok, FH} = ?WAL:open_wal_for_read(1),
    try file:read(FH, 100 * 1024 * 1024) of  %% 100MB!
        {ok, Binary} ->
            <<16#90, 16#7F,
              Type:1/binary,
              Flags:1/unit:8,
              BrickNameSize:2/unit:8,
              NumberOfBlobs:2/unit:8,
              TotalBlobSize:4/unit:8,
              Rest/binary>> = Binary,
            DecodedType  = ?HUNK:decode_type(Type),
            DecodedFlags = ?HUNK:decode_flags(Flags),
            {FooterSize, _RawSize, PaddingSize, _Overhead} =
                ?HUNK:calc_hunk_size(DecodedFlags, BrickNameSize, NumberOfBlobs, TotalBlobSize),
            ?ELOG_DEBUG("parse_hunk_iodata -  Type: ~p, Flags: ~p, BrickNameSize: ~w, NumberOfBlobs: ~w, "
                        "TotalBlobSize: ~w, FooterSize: ~w, RawSize: ~w, PaddingSize: ~w, Overhead: ~w~n",
              [DecodedType, DecodedFlags, BrickNameSize, NumberOfBlobs,
               TotalBlobSize, FooterSize, _RawSize, PaddingSize, _Overhead]),
            BodyBin   = binary:part(Rest, 0, TotalBlobSize),
            FooterBin = binary:part(Rest, TotalBlobSize, FooterSize),

            {BodyBin, FooterBin}
    after
        catch file:close(FH)
    end.
