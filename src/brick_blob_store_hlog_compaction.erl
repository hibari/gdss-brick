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


%% Copy Phase - Copy live blob hunks from an old hlog to the current hlog file
%%
%% Brick with on-disk metadata DB
%%  1. [compaction] Select an hlog file who has a low live hunk ratio
%%  2. (later) [compaction] freeze the hlog file (prohibit
%%     metadata-only copy kv for this file)
%%  3. (later) [brick] write an acknowledge record to the WAL
%%  4. (later) [write-back] when find the acknowledge record, notify
%%     the compaction
%%  5. [compaction] on each location info record, check if {key,
%%     timestamp} still exists and also its storage-location is
%%     pointing to the hlog file being compacted.
%%  6. [compaction] if it exists, copy it to short-term or long-term
%%     hlog files depending on its age
%%  7. [compaction] write metadata record with updated blob location
%%     to the WAL
%%  8. [write-back] check if {key, timestamp} still exists
%%  9. [write-back] if it exists, write the new metadata with the
%%     updated blob location
%% 10. [compaction] when finish processing the hlog file, write delete
%%     hlog file command to the WAL
%% 11. [write-back] when find the delete hlog file command, notify the
%%     blob store server
%% 12. [blob store] delete the hlog file
%%
%% Brick with in-memory metadata DB (brick_ets)
%%  1. [compaction] Select an hlog file who has a low live hunk ratio
%%  2. (later) [compaction] freeze the hlog file (prohibit
%%     metadata-only copy kv for this file)
%%  3. (later) [brick] write an acknowledge record to the WAL
%%  4. (later) [write-back] when find the acknowledge record, notify
%%     the compaction
%%  5. [compaction] on each location info record, check if {key,
%%     timestamp} still exists and also its storage-location is
%%     pointing to the hlog file being compacted.
%%  6. [compaction] if it exists, copy it to short-term or long-term
%%     hlog files depending on its age
%%  7. [compaction] notify brick_ets with updated blob location
%%  8. [brick_ets] check if {key, timestamp} still exists
%%  9. [brick_ets] if it exists, write the new metadata with the
%%     updated blob location
%% 10. [compaction] when finish processing the hlog file, notify the
%%     blob store server
%% 11. [blob store] delete the hlog file


-module(brick_blob_store_hlog_compaction).

-behaviour(gen_server).

%% DEBUG
-compile(export_all).

-include("brick_specs.hrl").
-include("brick_hlog.hrl").
-include("brick_blob_store_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% API
-export([start_link/1,
         stop/0,
         show_hlog_files/0,  %% temporary API
         list_hlog_files/0,  %% temporary API
         estimate_live_hunk_ratio/2,
         compact_hlog_file/2
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

-type mdstore() :: term().
-type blobstore() :: term().
-type blobstore_impl_info() :: {module(), pid()}.

-type prop() :: {atom(), term()}.

-record(state, {}).

-define(METADATA, brick_metadata_store).
-define(BLOB,     brick_blob_store).


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

%% @TODO: This is a temporary API
-spec show_hlog_files() -> ok.
show_hlog_files() ->
    lists:foreach(
      fun({B, S, unknown}) ->
              io:format("~s (~w): unknown~n", [B, S]);
         ({B, S, R}) ->
              io:format("~s (~w): ~.2f%~n", [B, S, R * 100])
      end, brick_blob_store_hlog_compaction:list_hlog_files()).

%% @TODO: This is a temporary API
-spec list_hlog_files() -> [{brickname(), seqnum(), LiveHunkRatio::float()}].
list_hlog_files() ->
    BlobImpls = [ {BrickName, ?BLOB:get_impl_info(BlobStore)}
                  || {BrickName, BlobStore} <- ?BLOB:list_blob_stores() ],
    Result =
        lists:map(
          fun({BrickName, {ImplMod, Pid}}) ->
                  SeqNums = ImplMod:list_seqnums(Pid, BrickName),
                  lists:reverse(lists:foldl(
                                  fun(SeqNum, Acc) ->
                                          case estimate_live_hunk_ratio(BrickName, SeqNum) of
                                              {ok, Ratio} ->
                                                  [{BrickName, SeqNum, Ratio} | Acc];
                                              unknown ->
                                                  [{BrickName, SeqNum, unknown} | Acc];
                                              _Err ->
                                                  Acc
                                          end
                                  end, [], SeqNums))
          end, BlobImpls),
    lists:flatten(Result).

-spec estimate_live_hunk_ratio(brickname(), seqnum()) ->
                                      {ok, float()} | unknown | {error, term()}.
estimate_live_hunk_ratio(BrickName, SeqNum) ->
    do_estimate_live_hunk_ratio(BrickName, SeqNum).

-spec compact_hlog_file(brickname(), seqnum()) -> ok | ignore | {error, term()}.
compact_hlog_file(BrickName, SeqNum) ->
    {ok, BlobStore} = ?BLOB:get_blob_store(BrickName),
    {BlobImplMod, BlobImplPid}=BlobStoreInfo = BlobStore:get_impl_info(),
    case BlobImplMod:current_writeback_seqnum(BlobImplPid, BrickName) of
        WBSeqNum when SeqNum >= WBSeqNum ->
            ?ELOG_INFO("Ignoring a compaction request for brick private blob file "
                       "~w with sequence ~w. The file might be still opened for write.",
                       [BrickName, SeqNum]),
            ignore;
        WBSeqNum when SeqNum < WBSeqNum ->
            case BlobImplMod:get_blob_file_info(BlobImplPid, BrickName, SeqNum) of
                {ok, _Path, #file_info{size=Size}} ->
                    case do_compact_hlog_file(BrickName, BlobStore, BlobStoreInfo, SeqNum) of
                        ok ->
                            %% @TODO: Enhance this check. Maybe send a signal to blob store
                            %% to tell the seqnum is being compactied.
                            case BlobImplMod:get_blob_file_info(BlobImplPid, BrickName, SeqNum) of
                                {ok, _Path, #file_info{size=Size}} -> %% Size is a bound variable
                                    SleepTimeSec = 30,  %% @TODO: Use the configuration value
                                    _Pid = BlobImplMod:schedule_blob_file_deletion(
                                             BlobImplPid, BrickName, SeqNum, SleepTimeSec),
                                    ok;
                                {ok, _Path, #file_info{size=NewSize}} ->
                                    ?ELOG_NOTICE("Brick private blob file ~w with sequence ~w "
                                                 "might be still opened for write. "
                                                 "Not deleting for now. "
                                                 "(old size: ~w, new size: ~w)",
                                                 [BrickName, SeqNum, Size, NewSize]),
                                    skip
                            end;
                        {error, _}=Err ->
                            Err
                    end;
                {error, _}=Err ->
                    Err
            end
    end.


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

-spec do_estimate_live_hunk_ratio(brickname(), seqnum()) ->
                                         {ok, float()} | unknown | {error, term()}.
do_estimate_live_hunk_ratio(BrickName, SeqNum) ->
    MaxKeys = 1000,
    {ok, MetadataStore} = ?METADATA:get_metadata_store(BrickName),
    {ok, BlobStore} = ?BLOB:get_blob_store(BrickName),
    {BlobImplMod, BlobPid}=BlobStoreInfo = BlobStore:get_impl_info(),

    case BlobImplMod:open_key_sample_file_for_read(BlobPid, BrickName, SeqNum) of
        {ok, KeySampleFile} ->
            FilterFun = BlobImplMod:live_keys_filter_fun(SeqNum),
            try count_live_hunks(BrickName, MetadataStore,
                                 BlobStoreInfo, FilterFun,
                                 KeySampleFile, start, MaxKeys,
                                 0, 0) of
                 {ok, 0, 0} ->
                    %% There are no key samples yet.
                    unknown;
                 {ok, SampleCount, LiveCount} ->
                    %% @TODO: Return unknown when SampleCount is too small (e.g. < 10).
                    %% In such a condition, the ratio will get very low accuracy.
                    {ok, LiveCount / SampleCount}
            after
                _ = (catch BlobImplMod:close_key_sample_file(BlobPid, BrickName, KeySampleFile))
            end;
        {error, {file_error, _FileName, enoent}} ->
            %% Key sample file is not created yet.
            unknown;
        Err ->
            Err
    end.

%% @TODO: Add throttle
-spec count_live_hunks(brickname(), mdstore(),
                       blobstore_impl_info(), live_keys_filter_function(),
                       key_sample_file(), continuation(), non_neg_integer(),
                       non_neg_integer(), non_neg_integer()) ->
                              {ok,
                               SampleCount::non_neg_integer(),
                               LiveCount::non_neg_integer()}
                                  | {error, term()}.
count_live_hunks(BrickName, MetadataStore,
                 {BlobImplMod, BlobPid}=BlobStoreInfo, FilterFun,
                 KeySampleFile, Cont, MaxKeys,
                 SampleCount, LiveCount) ->
    case BlobImplMod:read_key_samples(BlobPid, BrickName, KeySampleFile, Cont, MaxKeys) of
        {ok, NewCont, Keys} ->
            {ok, LiveKeys} = MetadataStore:live_keys(Keys, FilterFun),
            %% repeat
            count_live_hunks(BrickName, MetadataStore,
                             BlobStoreInfo, FilterFun,
                             KeySampleFile, NewCont, MaxKeys,
                             SampleCount + length(Keys), LiveCount + length(LiveKeys));
        eof ->
            {ok, SampleCount, LiveCount};
        {error, _}=Err ->
            Err
    end.

%% @TODO: Add throttle
-spec do_compact_hlog_file(brickname(), blobstore(),
                           blobstore_impl_info(), seqnum()) -> ok | {error, term()}.
do_compact_hlog_file(BrickName, BlobStore, {BlobImplMod, _}=BlobStoreInfo, SeqNum) ->
    MaxLocations = 1000,
    {ok, MetadataStore} = ?METADATA:get_metadata_store(BrickName),
    {ok, LocationFile} = BlobStore:open_location_info_file_for_read(SeqNum),
    FilterFun = BlobImplMod:live_keys_filter_fun(SeqNum),
    try
        find_and_copy_live_hunks(BrickName, MetadataStore,
                                 BlobStore, BlobStoreInfo, SeqNum, FilterFun,
                                 LocationFile, start, MaxLocations)
    after
        _ = (catch BlobStore:close_location_info_file(LocationFile))
    end.

-spec find_and_copy_live_hunks(brickname(), mdstore(),
                               blobstore(), blobstore_impl_info(),
                               seqnum(), live_keys_filter_function(),
                               location_info_file(), continuation(),
                               non_neg_integer()) -> ok | {error, term()}.
find_and_copy_live_hunks(BrickName, MetadataStore,
                         BlobStore, {BlobImplMod, BlobPid}=BlobStoreInfo,
                         SeqNum, FilterFun,
                         LocationFile, Cont,
                         MaxLocations) ->
    case BlobStore:read_location_info(LocationFile, Cont, MaxLocations) of
        {ok, NewCont, Locations} ->
            %% @TODO: Change this back to 3 when long-term log is implemented.
            %% AgeThreshold = 3,
            AgeThreshold = 200,
            LiveHunkLocations = live_hunk_locations(MetadataStore, Locations, FilterFun),
            StoreTuples = BlobImplMod:copy_hunks(BlobPid, BrickName, SeqNum,
                                                 LiveHunkLocations, AgeThreshold),
            _ = MetadataStore:update_blob_locations(StoreTuples),
            %% repeat
            find_and_copy_live_hunks(BrickName, MetadataStore,
                                     BlobStore, BlobStoreInfo, SeqNum, FilterFun,
                                     LocationFile, NewCont, MaxLocations);
        eof ->
            ok;
        {error, _}=Err ->
            Err
    end.

-spec live_hunk_locations(mdstore(), [location_info()],
                          live_keys_filter_function()) -> [location_info()].
live_hunk_locations(MetadataStore, Locations, FilterFun) ->
    Keys = [ {Key, TS} || #l{key=Key, timestamp=TS} <- Locations ],
    {ok, LiveKeys} = MetadataStore:live_keys(Keys, FilterFun),
    LiveKeysSet = gb_sets:from_list(LiveKeys),
    [ Location || #l{key=Key, timestamp=TS}=Location <- Locations,
                  gb_sets:is_member({Key, TS}, LiveKeysSet) ].
