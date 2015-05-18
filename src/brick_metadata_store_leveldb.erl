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
%%% File    : brick_metadata_store_leveldb.erl
%%% Purpose :
%%%-------------------------------------------------------------------

-module(brick_metadata_store_leveldb).

-behaviour(gen_server).

-include("brick_specs.hrl").
-include("brick_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% API for Brick Server
-export([start_link/2,
         stop/1,
         %% read_metadata/1,
         write_metadata/3,
         write_metadata_group_commit/3,
         request_group_commit/1
        ]).

%% API for Write-back Module
-export([writeback_to_stable_storage/3
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

-type wal_entry() :: term().

-record(state, {
          brick_name                   :: brickname(),
          leveldb                      :: h2leveldb:db()
         }).
-type state() :: #state{}.

-define(TIMEOUT, 60 * 1000).
-define(HUNK, brick_hlog_hunk).
-define(WAL, brick_hlog_wal).


%% ====================================================================
%% API
%% ====================================================================

-spec start_link(brickname(), [term()])
                -> {ok, pid()} | ignore | {error, term()}.
start_link(BrickName, Options) ->
    RegName = reg_name(BrickName),
    case gen_server:start_link({local, RegName}, ?MODULE, [BrickName, Options], []) of
        {ok, _Pid}=Res ->
            ?E_INFO("Metadata store ~w started.", [RegName]),
            Res;
        ErrorOrIgnore ->
            ErrorOrIgnore
    end.

-spec stop(pid()) -> ok | {error, term()}.
stop(Pid) ->
    gen_server:cast(Pid, stop).

%% -spec read_metadata(pid(), key(), impl()) -> storetuple().


%% Called by brick_ets:write_metadata_term(Term, #state{md_store})
-spec write_metadata(pid(), brickname(), [brick_ets:store_tuple()])
                    -> ok | {hunk_too_big, len()} | {error, term()}.
write_metadata(_Pid, BrickName, MetadataList) ->
    Blobs = [ term_to_binary(Metadata) || Metadata <- MetadataList ],
    {HunkIOList, _HunkSize, _Overhead, _BlobIndex} =
        ?HUNK:create_hunk_iolist(#hunk{type=metadata, flags=[],
                                       brick_name=BrickName, blobs=Blobs}),
    case ?WAL:write_hunk(HunkIOList) of
        {ok, _WALSeqNum, _WALPosition} ->
            ok;
        Err ->
            Err
    end.

-spec write_metadata_group_commit(pid(), brickname(), [brick_ets:store_tuple()])
                                 -> {ok, callback_ticket()}
                                        | {hunk_too_big, len()}
                                        | {error, term()}.
write_metadata_group_commit(_Pid, BrickName, MetadataList) ->
    Caller = self(),
    Blobs = [ term_to_binary(Metadata) || Metadata <- MetadataList ],
    {HunkIOList, _HunkSize, _Overhead, _BlobIndex} =
        ?HUNK:create_hunk_iolist(#hunk{type=metadata, flags=[],
                                       brick_name=BrickName, blobs=Blobs}),
    case ?WAL:write_hunk_group_commit(HunkIOList, Caller) of
        {ok, _WALSeqNum, _WALPosition, CallbackTicket} ->
            {ok, CallbackTicket};
        Err ->
            Err
    end.

-spec request_group_commit(pid()) -> callback_ticket().
request_group_commit(_Pid) ->
    Caller = self(),
    brick_hlog_wal:request_group_commit(Caller).

-spec writeback_to_stable_storage(pid(), [wal_entry()], boolean()) -> ok | {error, term()}.
writeback_to_stable_storage(Pid, WalEntries, IsLastBatch) ->
    MetadataDB = gen_server:call(Pid, get_leveldb, ?TIMEOUT),
    writeback_to_leveldb(MetadataDB, WalEntries, IsLastBatch),
    ok.


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([BrickName, _Options]) ->
    process_flag(trap_exit, true),
    {ok, MetadataDB} = open_metadata_db(BrickName),
    {ok, #state{brick_name=BrickName, leveldb=MetadataDB}}.

handle_call(get_leveldb, _From, #state{leveldb=MetadataDB}=State) ->
    {reply, MetadataDB, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    catch close_metadata_db(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions - misc
%% ====================================================================

-spec reg_name(brickname()) -> atom().
reg_name(BrickName) ->
    list_to_atom("hibari_md_store_" ++ atom_to_list(BrickName)).


%% ====================================================================
%% Internal functions - Metadata DB
%% ====================================================================

-spec metadata_dir(brickname()) -> dirname().
metadata_dir(BrickName) ->
    %% @TODO: Get the data_dir from #state{}.
    {ok, FileDir} = application:get_env(gdss_brick, brick_default_data_dir),
    filename:join([FileDir, "metadata2." ++ atom_to_list(BrickName)]).

-spec open_metadata_db(brickname()) -> ok | {error, term()}.
open_metadata_db(BrickName) ->
    MDBDir = metadata_dir(BrickName),
    catch file:make_dir(MDBDir),

    %% @TODO Create a function to return the metadata DB path.
    MDBPath = filename:join(MDBDir, "leveldb"),

    _RepairResult = repair_metadata_db(BrickName),
    ?E_DBG("Called repair_metadata_db. result: ~w", [_RepairResult]),
    MetadataDB = h2leveldb:get_db(MDBPath),
    ?ELOG_INFO("Opened metadata DB: ~s", [MDBPath]),
    {ok, MetadataDB}.

-spec repair_metadata_db(brickname()) -> ok | {error, term()}.
repair_metadata_db(BrickName) ->
    MDBDir = metadata_dir(BrickName),
    MDBPath = filename:join(MDBDir, "leveldb"),
    catch file:make_dir(MDBDir),
    h2leveldb:repair_db(MDBPath).

-spec close_metadata_db(state()) -> ok.
close_metadata_db(#state{brick_name=BrickName}) ->
    %% @TODO Create a function to return the metadata DB path.
    MDBPath = filename:join(metadata_dir(BrickName), "leveldb"),
    try h2leveldb:close_db(MDBPath) of
        ok ->
            ?ELOG_INFO("Closed metadata DB: ~s", [MDBPath]),
            ok;
        {error, _}=Error ->
            ?ELOG_WARNING("Failed to close metadata DB: ~s (Error: ~p)",
                          [MDBPath, Error]),
            ok
    catch _:_=Error1 ->
            ?ELOG_WARNING("Failed to close metadata DB: ~s (Error: ~p)",
                          [MDBPath, Error1]),
            ok
    end.


%% ====================================================================
%% Internal functions - Write-Back
%% ====================================================================

-spec writeback_to_leveldb(h2leveldb:db(), [wal_entry()], boolean()) -> ok.
writeback_to_leveldb(MetadataDB, DoMods, IsLastBatch) ->
    Batch = lists:foldl(fun add_metadata_db_op/2, h2leveldb:new_write_batch(), DoMods),
    IsEmptyBatch = h2leveldb:is_empty_batch(Batch),
    case {IsLastBatch, IsEmptyBatch} of
        {true, true} ->
            %% Write something to sync.
            Batch1 = [h2leveldb:make_put(sext:encode(control_sync), <<"sync">>)],
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
    ok = h2leveldb:write(MetadataDB, Batch1, WriteOptions),
    ok.

-spec add_metadata_db_op(brick_ets:do_mod(), h2leveldb:batch_write()) -> h2leveldb:batch_write().
add_metadata_db_op({insert, StoreTuple}, Batch) ->
    h2leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({insert_value_into_ram, StoreTuple}, Batch) ->
    h2leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({insert_constant_value, StoreTuple}, Batch) ->
    h2leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({insert_existing_value, StoreTuple, _OldKey, _OldTimestamp}, Batch) ->
    h2leveldb:add_put(metadata_db_key(StoreTuple), term_to_binary(StoreTuple), Batch);
add_metadata_db_op({delete, _Key, 0, _ExpTime}=Op, _Batch) ->
    error({timestamp_is_zero, Op});
add_metadata_db_op({delete, Key, _Timestamp, _ExpTime}, Batch) ->
%%     DeleteMarker = make_delete_marker(Key, Timestamp),
%%     leveldb:add_put(metadata_db_key(Key, Timestamp),
%%                     term_to_binary(DeleteMarker), Batch);
    h2leveldb:add_delete(metadata_db_key(Key, 0), Batch);
add_metadata_db_op({delete_noexptime, _Key, 0}=Op, _Batch) ->
    error({timestamp_is_zero, Op});
add_metadata_db_op({delete_noexptime, Key, _Timestamp}, Batch) ->
%%     DeleteMarker = make_delete_marker(Key, Timestamp),
%%     leveldb:add_put(metadata_db_key(Key, Timestamp),
%%                     term_to_binary(DeleteMarker), Batch);
    h2leveldb:add_delete(metadata_db_key(Key, 0), Batch);
add_metadata_db_op({delete_all_table_items}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({md_insert, _Tuple}=Op, _Batch) ->
    %% @TODO: CHECKME: brick_ets:checkpoint_start/4, which was deleted after
    %% commit #b2952a393, had the following code to dump brick's private
    %% metadata. Check when metadata will be written and implement
    %% add_metadata_db_op/2 for it.
    %% ----
    %% %% Dump data from the private metadata table.
    %% MDs = term_to_binary([{md_insert, T} ||
    %%                       T <- ets:tab2list(S_ro#state.mdtab)]),
    %% {_, Bin2} = WalMod:create_hunk(?LOGTYPE_METADATA, [MDs], []),
    %% ok = file:write(CheckFH, Bin2),
    %% ----
    error({writeback_not_implemented, Op});
add_metadata_db_op({md_delete, _Key}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({log_directive, sync_override, false}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({log_directive, map_sleep, _Delay}=Op, _Batch) ->
    error({writeback_not_implemented, Op});
add_metadata_db_op({log_noop}, Batch) ->
    Batch. %% noop

%% As for Hibari 0.3.0, metadata DB key is {Key, 0}. (The reversed
%% timestamp is always zero.)
-spec metadata_db_key(brick_ets:store_tuple()) -> binary().
metadata_db_key(StoreTuple) ->
    Key = brick_ets:storetuple_key(StoreTuple),
    %% Timestamp = brick_ets:storetuple_ts(StoreTuple),
    %% metadata_db_key(Key, Timestamp).
    metadata_db_key(Key, 0).

-spec metadata_db_key(key(), ts()) -> binary().
metadata_db_key(Key, _Timestamp) ->
    %% NOTE: Using reversed timestamp, so that Key-values will be sorted
    %%       in LevelDB from newer to older.
    %% ReversedTimestamp = -(Timestamp),
    ReversedTimestamp = -0,
    sext:encode({Key, ReversedTimestamp}).

%% -spec make_delete_marker(key(), ts()) -> tuple().
%% make_delete_marker(Key, Timestamp) ->
%%     {Key, Timestamp, delete_marker}.


%% ====================================================================
%% Internal functions -- Tests
%% ====================================================================

%% DEBUG (@TODO: eunit / quickcheck cases)

test_start_link() ->
    {ok, _Pid} = start_link(table1_ch1_b1, []).

test1() ->
    Brick = tab1_ch1_b1,
    StoreTuple1 = {<<"key1">>,  brick_server:make_timestamp(), <<"val1">>},
    StoreTuple2 = {<<"key12">>, brick_server:make_timestamp(), <<"val12">>},
    MetadataList = [StoreTuple1, StoreTuple2],
    write_metadata(metadata_store, Brick, MetadataList).

test2() ->
    Brick = tab1_ch1_b1,
    StoreTuple1 = {<<"key1">>,  brick_server:make_timestamp(), <<"val1">>},
    StoreTuple2 = {<<"key12">>, brick_server:make_timestamp(), <<"val12">>},
    MetadataList = [StoreTuple1, StoreTuple2],
    write_metadata_group_commit(metadata_store, Brick, MetadataList).
