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
         write_metadata_group_commit/2
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
          name                         :: atom(),
          brick_name                   :: brickname(),
          brick_pid                    :: pid()  %% or reg name?
         }).

-define(TIMEOUT, 60 * 1000).
-define(HUNK, brick_hlog_hunk).
-define(WAL, brick_hlog_wal).


%% ====================================================================
%% API
%% ====================================================================

%% -spec start_link() ->
start_link(BrickName, Options) ->
    RegName = metadata_store,
    gen_server:start_link({local, RegName}, ?MODULE, [BrickName, Options], []).

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

-spec writeback_to_stable_storage(pid(), wal_entry()) -> ok | {error, term()}.
writeback_to_stable_storage(_Pid, _WalEntry) ->
    %% gen_server:call(Pid, {writeback_value, WalEntry}).
    ok.


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([BrickName, _Options]) ->
    process_flag(trap_exit, true),
    %% process_flag(priority, high),

    {ok, #state{brick_name=BrickName}}.

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

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================




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

