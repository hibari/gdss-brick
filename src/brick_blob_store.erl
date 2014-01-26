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
%%% File    : brick_value_store.erl
%%% Purpose : the common interface of value store implementations
%%%-------------------------------------------------------------------

-module(brick_blob_store).

-include("brick_specs.hrl").

%% API for Brick Server
-export([start_link/3,
         read_value/2,
         write_value/2
        ]).

%% API for Write-back Module
-export([writeback_to_stable_storage/2,
         sync/1
        ]).

%% ====================================================================
%% types and records
%% ====================================================================

-record(?MODULE, {impl_mod :: module(), pid :: pid()}).

-type impl() :: #?MODULE{}.
-type brickname() :: atom().
-type storage_location() :: term().
-type wal_entry() :: term().


%% ====================================================================
%% API
%% ====================================================================

%% @TODO Define brick_value_store behaviour.

-spec start_link(brickname(), [term()], module())
                -> {ok, impl()} | ignore | {error, term()}.
start_link(BrickName, Options, ImplMod) ->
    case ImplMod:start_link(BrickName, Options) of
        {ok, Pid} ->
            #?MODULE{impl_mod=ImplMod, pid=Pid};
        Err ->
            Err
    end.

%% Called by brick_ets:bigdata_dir_get_val(Key, Loc, Len, ...)
-spec read_value(storage_location(), impl()) -> val().
read_value(Location, #?MODULE{impl_mod=ImplMod}) ->
    ImplMod:read_value(Location).

%% Called by brick_ets:bigdata_dir_store_val(Key, Val, State)
-spec write_value(val(), impl()) -> storage_location().
write_value(Value, #?MODULE{impl_mod=ImplMod, pid=Pid}) ->
    ImplMod:write_value(Pid, Value).

%% Called by the WAL write-back process.
-spec writeback_to_stable_storage(wal_entry(), impl()) -> ok | {error, term()}.
writeback_to_stable_storage(WalEntry, #?MODULE{impl_mod=ImplMod, pid=Pid}) ->
    ImplMod:writeback_to_stable_storage(Pid, WalEntry).

-spec sync(impl()) -> ok | {error, term()}.
sync(#?MODULE{impl_mod=ImplMod, pid=Pid}) ->
    ImplMod:sync(Pid).
