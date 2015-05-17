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
         %% read_metadata/1,
         write_metadata/2,
         write_metadata_group_commit/2,
         request_group_commit/1
        ]).

%% API for Write-back Module
-export([writeback_to_stable_storage/2
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

%% -spec read_metadata(pid(), key(), impl()) -> storetuple().


%% Called by brick_ets:write_metadata_term(Term, #state{md_store})
-spec write_metadata(pid(), [brick_ets:store_tuple()])
                    -> ok | {hunk_too_big, len()} | {error, term()}.
write_metadata(Pid, MetadataList) ->
    gen_server:call(Pid, {write_metadata, MetadataList}, ?TIMEOUT).

-spec write_metadata_group_commit(pid(), [brick_ets:store_tuple()])
                                 -> {ok, callback_ticket()}
                                        | {hunk_too_big, len()}
                                        | {error, term()}.
write_metadata_group_commit(Pid, MetadataList) ->
    Caller = self(),
    gen_server:call(Pid, {write_metadata_group_commit, MetadataList, Caller}, ?TIMEOUT).

-spec writeback_to_stable_storage(pid(), [wal_entry()]) -> ok | {error, term()}.
writeback_to_stable_storage(_Pid, _WalEntries) ->
    %% gen_server:call(Pid, {writeback_value, WalEntries}).
    ok.

-spec request_group_commit(pid()) -> callback_ticket().
request_group_commit(_Pid) ->
    Caller = self(),
    brick_hlog_wal:request_group_commit(Caller).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([BrickName, _Options]) ->
    process_flag(trap_exit, true),
    %% process_flag(priority, high),
    {ok, MetadataDB} = open_metadata_db(BrickName),
    {ok, #state{brick_name=BrickName, leveldb=MetadataDB}}.

handle_call({write_metadata, MetadataList}, _From,
            #state{brick_name=BrickName}=State) ->
    Blobs = [ term_to_binary(Metadata) || Metadata <- MetadataList ],
    {HunkIOList, _HunkSize, _Overhead, _BlobIndex} =
        ?HUNK:create_hunk_iolist(#hunk{type=metadata, flags=[],
                                       brick_name=BrickName, blobs=Blobs}),
    case ?WAL:write_hunk(HunkIOList) of
        {ok, _WALSeqNum, _WALPosition} ->
            {reply, ok, State};
        Err ->
            {reply, Err, State}
    end;
handle_call({write_metadata_group_commit, MetadataList, Caller}, _From,
            #state{brick_name=BrickName}=State) ->
    Blobs = [ term_to_binary(Metadata) || Metadata <- MetadataList ],
    {HunkIOList, _HunkSize, _Overhead, _BlobIndex} =
        ?HUNK:create_hunk_iolist(#hunk{type=metadata, flags=[],
                                       brick_name=BrickName, blobs=Blobs}),
    case ?WAL:write_hunk_group_commit(HunkIOList, Caller) of
        {ok, _WALSeqNum, _WALPosition, CallbackTicket} ->
            {reply, {ok, CallbackTicket}, State};
        Err ->
            {reply, Err, State}
    end.

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
%% Internal functions
%% ====================================================================

-spec reg_name(brickname()) -> atom().
reg_name(BrickName) ->
    list_to_atom("hibari_md_store_" ++ atom_to_list(BrickName)).

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


%% DEBUG (@TODO: eunit / quickcheck cases)

test_start_link() ->
    {ok, _Pid} = start_link(table1_ch1_b1, []).

test1() ->
    StoreTuple1 = {<<"key1">>, brick_server:make_timestamp(), <<"val1">>},
    StoreTuple2 = {<<"key12">>, brick_server:make_timestamp(), <<"val12">>},
    MetadataList = [StoreTuple1, StoreTuple2],
    write_metadata(metadata_store, MetadataList).

test2() ->
    StoreTuple1 = {<<"key1">>, brick_server:make_timestamp(), <<"val1">>},
    StoreTuple2 = {<<"key12">>, brick_server:make_timestamp(), <<"val12">>},
    MetadataList = [StoreTuple1, StoreTuple2],
    write_metadata_group_commit(metadata_store, MetadataList).
