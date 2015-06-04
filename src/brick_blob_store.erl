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
%%% File    : brick_value_store.erl
%%% Purpose : the common interface of value store implementations
%%%-------------------------------------------------------------------

-module(brick_blob_store).

-behaviour(gen_server).

-include("brick_specs.hrl").
-include("brick_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% Common API
-export([list_blob_stores/0,
         get_blob_store/1,
         get_impl_info/1
        ]).

%% API for brick_data_sup module
-export([start_link/2,
         stop/0
        ]).

%% API for brick server
-export([read_value/2,
         write_value/2
        ]).

%% API for write-back and compaction modules
-export([writeback_to_stable_storage/2,
         write_location_info/2,
         open_location_info_file_for_read/2,
         read_location_info/4,
         close_location_info_file/2,
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


%% ====================================================================
%% types and records
%% ====================================================================

%% @TODO: Use registered name rather than pid. pid will change when a process crashes.
-record(?MODULE, {
           impl_mod   :: module(),
           brick_name :: brickname(),
           pid        :: pid()
          }).
-type impl() :: #?MODULE{}.

-type wal_entry() :: term().
-type orddict(_A) :: term().  %% orddict in stdlib
-type location_info_file() :: term().
-type continuation() :: term().


-record(state, {
          impl_mod                :: module(),
          registory=orddict:new() :: orddict(impl())  %% Registory of metadata_store impl
         }).

-define(TIMEOUT, 60 * 1000).


%% ====================================================================
%% API
%% ====================================================================

%% @TODO Define brick_value_store behaviour.

-spec start_link(module(), [term()])
                -> {ok, pid()} | ignore | {error, term()}.
start_link(ImplMod, Options) ->
    gen_server:start_link({local, ?BRICK_BLOB_STORE_REG_NAME},
                          ?MODULE, [ImplMod, Options], []).

-spec stop() -> ok.
stop() ->
    gen_server:cast(?BRICK_BLOB_STORE_REG_NAME, stop),
    ok.

-spec list_blob_stores() -> [{brickname(), impl()}].
list_blob_stores() ->
    gen_server:call(?BRICK_BLOB_STORE_REG_NAME, list_blob_store_impls, ?TIMEOUT).

-spec get_blob_store(brickname()) -> {ok, impl()} | {error, term()}.
get_blob_store(BrickName) ->
    gen_server:call(?BRICK_BLOB_STORE_REG_NAME,
                    {get_or_start_blob_store_impl, BrickName}, ?TIMEOUT).

-spec get_impl_info(impl()) -> {module(), pid()}.
get_impl_info(#?MODULE{impl_mod=ImplMod, pid=Pid}) ->
    {ImplMod, Pid}.

%% Called by brick_ets:bigdata_dir_get_val(Key, Loc, Len, ...)
-spec read_value(storage_location(), impl()) -> val().
read_value(Location, #?MODULE{impl_mod=ImplMod, brick_name=BrickName}) ->
    ImplMod:read_value(BrickName, Location).

%% Called by brick_ets:bigdata_dir_store_val(Key, Val, State)
-spec write_value(val(), impl()) -> {ok, storage_location()} | {error, term()}.
write_value(Value, #?MODULE{impl_mod=ImplMod, pid=Pid}) ->
    ImplMod:write_value(Pid, Value).

%% Called by the WAL write-back process.
-spec writeback_to_stable_storage([wal_entry()], impl()) -> ok | {error, term()}.
writeback_to_stable_storage(WalEntries, #?MODULE{impl_mod=ImplMod, brick_name=BrickName, pid=Pid}) ->
    ImplMod:writeback_to_stable_storage(Pid, BrickName, WalEntries).

-spec write_location_info([{key(), storage_location()}], impl()) -> ok | {error, term()}.
write_location_info(Locations, #?MODULE{impl_mod=ImplMod, brick_name=BrickName, pid=Pid}) ->
    ImplMod:write_location_info(Pid, BrickName, Locations).

-spec open_location_info_file_for_read(seqnum(), impl()) ->
                                              {ok, location_info_file()} | {err, term()}.
open_location_info_file_for_read(SeqNum, #?MODULE{impl_mod=ImplMod, brick_name=BrickName, pid=Pid}) ->
    ImplMod:open_location_info_file_for_read(Pid, BrickName, SeqNum).

-spec read_location_info(location_info_file(),
                         'start' | continuation(), non_neg_integer(), impl()) ->
                                {ok, continuation(), [{key(), storage_location()}]}
                                    | 'eof'
                                    | {error, term()}.
read_location_info(DiskLog, Cont, MaxRecords, #?MODULE{impl_mod=ImplMod, brick_name=BrickName, pid=Pid}) ->
    ImplMod:read_location_info(Pid, BrickName, DiskLog, Cont, MaxRecords).

-spec close_location_info_file(location_info_file(), impl()) -> ok.
close_location_info_file(DiskLog, #?MODULE{impl_mod=ImplMod, brick_name=BrickName, pid=Pid}) ->
    ImplMod:close_location_info_file(Pid, BrickName, DiskLog).

-spec sync(impl()) -> ok | {error, term()}.
sync(#?MODULE{impl_mod=ImplMod, pid=Pid}) ->
    ImplMod:sync(Pid).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([ImplMod, _Options]) ->
    process_flag(trap_exit, true),
    %% process_flag(priority, high),
    {ok, #state{impl_mod=ImplMod}}.

handle_call(list_blob_store_impls, _From, #state{registory=Registory}=State) ->
    {reply, orddict:to_list(Registory), State};
handle_call({get_or_start_blob_store_impl, BrickName}, _From,
            #state{impl_mod=ImplMod, registory=Registory}=State) ->
    case orddict:find(BrickName, Registory) of
        {ok, _Impl}=Res ->
            {reply, Res, State};
        error ->
            Options = [],
            %% @TODO: Manage this process under the supervisor tree
            case ImplMod:start_link(BrickName, Options) of
                {ok, Pid} ->
                    Impl = #?MODULE{
                               impl_mod=ImplMod,
                               brick_name=BrickName,
                               pid=Pid
                              },
                    Registory1 = orddict:store(BrickName, Impl, Registory),
                    {reply, {ok, Impl}, State#state{registory=Registory1}};
                ignore ->
                    {reply, {error, {inconsistent_blob_registory, ImplMod, BrickName}}, State};
                Err ->
                    {reply, Err, State}
            end
    end.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_, State) ->
    {noreply, State}.

%% @TODO: Handle exit from the gen_servers of metadata_store impl
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, #state{impl_mod=ImplMod, registory=Registory}) ->
    orddict:fold(fun(_BrickName, #?MODULE{pid=Pid}, _Acc) ->
                         catch ImplMod:stop(Pid),
                         ok
                 end, undefined, Registory),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
