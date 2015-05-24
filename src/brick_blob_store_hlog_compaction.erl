%%%-------------------------------------------------------------------
%%% Copyright (c) 2015 Hibari developers. All rights reserved.
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
%%% File    : brick_blob_store_hlog_compaction.erl
%%% Purpose :
%%%-------------------------------------------------------------------

%% (later) [compaction] freeze the hlog file (prohibit metadata-only copy kv for this file)
%% (later) [brick] write an acknowledge record to the WAL
%% (later) [write-back] when find the acknowledge record, notify the compaction
%% [compaction] on each location info record, check if Hibari {key, timestamp} still exists
%% [compaction] if it exists, copy it to short-term or long-term hlog files depending on its age
%% [compaction] write metadata record with updated blob location to the WAL
%% [write-back] check if Hibari {key, timestamp} still exists
%% [write-back] if it exists, write the new metadata with the updated blob location
%% [compaction] when finish processing the hlog file, write delete hlog file command to the WAL
%% [write-back] when find the delete hlog file command, notify the blob store server
%% [blob store] delete the hlog file, and notify the hlog table

-module(brick_blob_store_hlog_compaction).

-behaviour(gen_server).

%% DEBUG
-compile(export_all).

%% -include("brick_specs.hrl").
-include("brick_hlog.hrl").
%% -include("brick.hrl").      % for ?E_ macros

%% API
-export([start_link/1,
         stop/0
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
%% types, specs and records
%% ====================================================================

-type prop() :: {atom(), term()}.

-record(state, {}).

%% -define(HUNK, brick_hlog_hunk).
%% -define(WAL,  brick_hlog_wal).


%% ====================================================================
%% API
%% ====================================================================

-spec start_link([prop()]) -> {ok,pid()} | ignore | {error,term()}.
start_link(PropList) ->
    gen_server:start_link({local, ?COMPACTION_SERVER_REG_NAME},
                          ?MODULE, [PropList], []).

-spec stop() -> ok | {error, term()}.
stop() ->
    gen_server:call(?COMPACTION_SERVER_REG_NAME, stop).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([_Options]) ->
    {ok, #state{}}.

handle_call(_Cmd, _From, #state{}=State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(_Cmd, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% ====================================================================
%% Internal functions - tests and debug
%% ====================================================================

test1() ->
    BrickName = bootstrap_copy1,
    SeqNum = 1,
    MaxLocations = 1000,
    {ok, MetadataStore} = brick_metadata_store:get_metadata_store(BrickName),
    {ok, BlobStore} = brick_blob_store:get_blob_store(BrickName),
    {ok, LocationFile} = BlobStore:open_location_info_file_for_read(SeqNum),
    try
        test1_check_live_keys(MetadataStore, BlobStore, LocationFile, start, MaxLocations)
    after
        _ = (catch BlobStore:close_location_info_file(LocationFile))
    end.

test1_check_live_keys(MetadataStore, BlobStore, LocationFile, Cont, MaxLocations) ->
    case BlobStore:read_location_info(LocationFile, Cont, MaxLocations) of
        {ok, NewCont, Locations} ->
            Keys = [ {Key, TS} || {_, _, _, Key, TS} <- Locations ],
            {ok, LiveKeys} = MetadataStore:live_keys(Keys),
            FormatKey = fun({Key, TS}) -> io:format("~p (~w)~n", [binary_to_term(Key), TS]) end,
            io:format("Keys::::~n"),
            lists:foreach(FormatKey, Keys),
            io:format("LiveKeys::::~n"),
            lists:foreach(FormatKey, LiveKeys),
            test1_check_live_keys(MetadataStore, BlobStore, LocationFile, NewCont, MaxLocations);
        eof ->
            ok;
        {error, _}=Err ->
            Err
    end.
