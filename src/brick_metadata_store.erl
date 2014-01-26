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
%%% File    : brick_metadata_store.erl
%%% Purpose :
%%%-------------------------------------------------------------------

-module(brick_metadata_store).

-include("brick_specs.hrl").
-include("brick_hlog.hrl").

%% API for Brick Server
-export([start_link/3,
         read_metadata/2,
         write_metadata/2,
         write_metadata_group_commit/2
        ]).

%% API for Write-back Module
-export([writeback_to_stable_storage/2
        ]).


%% ====================================================================
%% types and records
%% ====================================================================

-record(?MODULE, {impl_mod :: module(), pid :: pid()}).

-type impl() :: #?MODULE{}.
-type brickname() :: atom().
-type wal_entry() :: term().


%% ====================================================================
%% API
%% ====================================================================

%% @TODO Define brick_metadata_store behaviour.


-spec start_link(brickname(), [term()], module())
                -> {ok, impl()} | ignore | {error, term()}.
start_link(BrickName, Options, ImplMod) ->
    case ImplMod:start_link(BrickName, Options) of
        {ok, Pid} ->
            #?MODULE{impl_mod=ImplMod, pid=Pid};
        Err ->
            Err
    end.

%% Called by brick_ets:read_metadata_term(...)
-spec read_metadata(key(), impl()) -> brick_ets:store_tuple().
read_metadata(Key, #?MODULE{impl_mod=ImplMod}) ->
    ImplMod:read_metadata(Key).

%% Called by brick_ets:write_metadata_term(Term, #state{md_store})
-spec write_metadata([brick_ets:store_tuple()], impl())
                    -> ok | {hunk_too_big, len()} | {error, term()}.
write_metadata(MetadataList, #?MODULE{impl_mod=ImplMod, pid=Pid}) ->
    ImplMod:write_metadata(Pid, MetadataList).

-spec write_metadata_group_commit([brick_ets:store_tuple()], impl())
                                 -> {ok, callback_ticket()}
                                        | {hunk_too_big, len()}
                                        | {error, term()}.
write_metadata_group_commit(MetadataList, #?MODULE{impl_mod=ImplMod, pid=Pid}) ->
    ImplMod:write_metadata_group_commit(Pid, MetadataList).

%% Called by the WAL write-back process.
-spec writeback_to_stable_storage(wal_entry(), impl()) -> ok | {error, term()}.
writeback_to_stable_storage(WalEntry, #?MODULE{impl_mod=ImplMod, pid=Pid}) ->
    ImplMod:writeback_to_stable_storage(Pid, WalEntry).
