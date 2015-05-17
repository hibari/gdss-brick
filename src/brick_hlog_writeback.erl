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

%% API
%% -export([start_link/1,
%%          stop/0,
%%          register_local_brick/1,
%%          full_writeback/0,
%%          get_all_registrations/0
%%         ]).

%% gen_server callbacks
%% -export([init/1,
%%          handle_call/3,
%%          handle_cast/2,
%%          handle_info/2,
%%          terminate/2,
%%          code_change/3
%%         ]).


%% ====================================================================
%% types, specs and records
%% ====================================================================

%% -type from() :: {pid(), term()}.
%% -type prop() :: [].

%% ====================================================================
%% API
%% ====================================================================

%% -spec start_link([prop()]) -> {ok,pid()} | ignore | {error,term()}.
%% start_link(PropList) ->
%%     gen_server:start_link(?MODULE, PropList, []).

%% -spec stop() -> ok | {error, term()}.
%% stop() ->
%%     gen_server:call(?Server, stop).

%% -spec register_local_brick(brickname()) -> ok | {error, term()}.
%% register_local_brick(LocalBrick) when is_atom(LocalBrick) ->
%%     gen_server:call(?Server, {register_local_brick, LocalBrick}).

%% full_writeback() ->
%%     full_writeback(?Server).

%% -spec get_all_registrations() -> [brickname()].
%% get_all_registrations() ->
%%     get_all_registrations(?Server).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================


%% ====================================================================
%% Internal functions
%% ====================================================================



%% MOVEME: to brick_metadata_store leveldb module.

%% -spec write_back_to_stable_storege1(brickname(), [metadata_tuple()],
%%                                     state_readonly(), [term()]) -> [term()].
%% write_back_to_stable_storege1(_BrickName, [], _State, Errors) ->
%%     lists:reverse(Errors);
%% write_back_to_stable_storege1(BrickName,
%%                               [{eee, _BrickName, _SeqNum, _Offset, _Key, _TypeNum,
%%                                 _H_Len, [Summary, CBlobs, _UBlobs]} | RemainingMetaDataTuples],
%%                               #state{hlog_pid=HLog}=State,
%%                               Errors) ->
%%     case gmt_hlog:parse_hunk_summary(Summary) of
%%         #hunk_summ{c_len=CLen, u_len=ULen}=ParsedSummary ->
%%             [_] = CLen,        % sanity
%%             []  = ULen,        % sanity
%%             MetadataBin = gmt_hlog:get_hunk_cblob_member(ParsedSummary, CBlobs, 1),
%%             case gmt_hlog:md5_checksum_ok_p(ParsedSummary#hunk_summ{c_blobs=[MetadataBin]}) of
%%                 true ->
%%                     MetadataDB = gmt_hlog:get_metadata_db(HLog, BrickName),
%%                     DoMods = binary_to_term(MetadataBin),
%%                     IsLastBatch = RemainingMetaDataTuples =:= [],
%%                     ok = write_back_to_metadata_db(MetadataDB, BrickName, DoMods, IsLastBatch);
%%                 false ->
%%                     %% @TODO: Do not crash, and do sync
%%                     error({invalid_checksum, {_BrickName, _SeqNum, _Offset, _Key}})
%%             end;
%%         too_big ->
%%             %% @TODO: Do not crash, and do sync
%%             error({hunk_is_too_big_to_parse, {_BrickName, _SeqNum, _Offset, _Key}})
%%     end,
%%     write_back_to_stable_storege1(BrickName, RemainingMetaDataTuples, State, Errors).

%% -spec write_back_to_metadata_db(h2leveldb:db(), brickname(), [do_mod()], boolean()) -> ok.
%% write_back_to_metadata_db(MetadataDB, _BrickName, DoMods, IsLastBatch) ->
%%     Batch = lists:foldl(fun add_metadata_db_op/2, h2leveldb:new_write_batch(), DoMods),
%%     IsEmptyBatch = h2leveldb:is_empty_batch(Batch),
%%     case {IsLastBatch, IsEmptyBatch} of
%%         {true, true} ->
%%             %% Write something to sync.
%%             Batch1 = [h2leveldb:make_put(sext:encode(control_sync), <<"sync">>)],
%%             WriteOptions = [sync];
%%         {true, false} ->
%%             Batch1 = Batch,
%%             WriteOptions = [sync];
%%         {false, true} ->
%%             Batch1 = [],   %% This will not write anything to LevelDB and that is OK.
%%             WriteOptions = [];
%%         {false, false} ->
%%             Batch1 = Batch,
%%             WriteOptions = []
%%     end,
%%     ok = h2leveldb:write(MetadataDB, Batch1, WriteOptions),
%%     ok.

%% -spec add_metadata_db_op(do_mod(), write_batch()) -> write_batch().
%% add_metadata_db_op({insert, StoreTuple}, Batch) ->
%%     h2leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
%% add_metadata_db_op({insert_value_into_ram, StoreTuple}, Batch) ->
%%     h2leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
%% add_metadata_db_op({insert_constant_value, StoreTuple}, Batch) ->
%%     h2leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
%% add_metadata_db_op({insert_existing_value, StoreTuple, _OldKey, _OldTimestamp}, Batch) ->
%%     h2leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
%% add_metadata_db_op({delete, _Key, 0, _ExpTime}=Op, _Batch) ->
%%     error({timestamp_is_zero, Op});
%% add_metadata_db_op({delete, Key, _Timestamp, _ExpTime}, Batch) ->
%% %%     DeleteMarker = make_delete_marker(Key, Timestamp),
%% %%     leveldb:add_put(metadata_db_key(Key, Timestamp),
%% %%                     term_to_binary(DeleteMarker), Batch);
%%     h2leveldb:add_delete(metadata_db_key(Key, 0), Batch);
%% add_metadata_db_op({delete_noexptime, _Key, 0}=Op, _Batch) ->
%%     error({timestamp_is_zero, Op});
%% add_metadata_db_op({delete_noexptime, Key, _Timestamp}, Batch) ->
%% %%     DeleteMarker = make_delete_marker(Key, Timestamp),
%% %%     leveldb:add_put(metadata_db_key(Key, Timestamp),
%% %%                     term_to_binary(DeleteMarker), Batch);
%%     h2leveldb:add_delete(metadata_db_key(Key, 0), Batch);
%% add_metadata_db_op({delete_all_table_items}=Op, _Batch) ->
%%     error({writeback_not_implemented, Op});
%% add_metadata_db_op({md_insert, _Tuple}=Op, _Batch) ->
%%     %% @TODO: CHECKME: brick_ets:checkpoint_start/4, which was deleted after
%%     %% commit #b2952a393, had the following code to dump brick's private
%%     %% metadata. Check when metadata will be written and implement
%%     %% add_metadata_db_op/2 for it.
%%     %% ----
%%     %% %% Dump data from the private metadata table.
%%     %% MDs = term_to_binary([{md_insert, T} ||
%%     %%                       T <- ets:tab2list(S_ro#state.mdtab)]),
%%     %% {_, Bin2} = WalMod:create_hunk(?LOGTYPE_METADATA, [MDs], []),
%%     %% ok = file:write(CheckFH, Bin2),
%%     %% ----
%%     error({writeback_not_implemented, Op});
%% add_metadata_db_op({md_delete, _Key}=Op, _Batch) ->
%%     error({writeback_not_implemented, Op});
%% add_metadata_db_op({log_directive, sync_override, false}=Op, _Batch) ->
%%     error({writeback_not_implemented, Op});
%% add_metadata_db_op({log_directive, map_sleep, _Delay}=Op, _Batch) ->
%%     error({writeback_not_implemented, Op});
%% add_metadata_db_op({log_noop}, Batch) ->
%%     Batch. %% noop

%% %% As for Hibari 0.3.0, metadata DB key is {Key, 0}. (The reversed
%% %% timestamp is always zero.)
%% -spec metadata_db_key(store_tuple()) -> binary().
%% metadata_db_key(StoreTuple) ->
%%     Key = brick_ets:storetuple_key(StoreTuple),
%%     %% Timestamp = brick_ets:storetuple_ts(StoreTuple),
%%     %% metadata_db_key(Key, Timestamp).
%%     metadata_db_key(Key, 0).

%% -spec metadata_db_key(key(), ts()) -> binary().
%% metadata_db_key(Key, _Timestamp) ->
%%     %% NOTE: Using reversed timestamp, so that Key-values will be sorted
%%     %%       in LevelDB from newer to older.
%%     %% ReversedTimestamp = -(Timestamp),
%%     ReversedTimestamp = -0,
%%     sext:encode({Key, ReversedTimestamp}).

%% %% -spec make_delete_marker(key(), ts()) -> tuple().
%% %% make_delete_marker(Key, Timestamp) ->
%% %%     {Key, Timestamp, delete_marker}.
