%%%-------------------------------------------------------------------
%%% Copyright (c) 2011-2017 Hibari developers.  All rights reserved.
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
%%% File    : brick_data_sup.erl
%%% Purpose : Long-term data supervisor
%%%-------------------------------------------------------------------

%% @doc The supervisor for all brick non-admin functions.

-module(brick_data_sup).

-include("gmt_hlog.hrl").
-include("brick_hlog.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% Description: Whenever a supervisor is started using
%% supervisor:start_link/[2,3], this function is called by the new process
%% to find out about restart strategy, maximum restart frequency and child
%% specifications.
%%--------------------------------------------------------------------
init([]) ->
    %% Hint:
    %% Child_spec = [Name, {M, F, A},
    %%               Restart, Shutdown_time, Type, Modules_used]

    H2LevelDB =
        {h2leveldb, {h2leveldb, start_link, [[]]},
         permanent, 2000, worker, [h2leveldb]},

    BrickMetadataStore =
        {?METADATA_STORE_REG_NAME,
         {brick_metadata_store, start_link, [brick_metadata_store_leveldb, []]},
         permanent, 2000, worker, [brick_metadata_store]},

    BrickBlobStoreRegistory =
        {?HLOG_REGISTORY_SERVER_REG_NAME,
         {brick_blob_store_hlog_registory, start_link, [[]]},
         permanent, 2000, worker, [brick_blob_store_hlog_registory]},

    BrickBlobStore =
        {?BRICK_BLOB_STORE_REG_NAME,
         {brick_blob_store, start_link, [brick_blob_store_hlog, []]},
         permanent, 2000, worker, [brick_blob_store]},

    BrickBlobStoreCompaction =
        {?COMPACTION_SERVER_REG_NAME,
         {brick_blob_store_hlog_compaction, start_link, [[]]},
         permanent, 2000, worker, [brick_blob_store_hlog_compaction]},

    MaxMB = get_env(brick_max_log_size_mb),
    MinMB = get_env(brick_min_log_size_mb),
    MinHC = get_env(brick_min_hunk_count),

    WALArgs =
        [ {file_len_max, MaxMB * 1024*1024} || MaxMB =/= undefined ]
        ++ [ {file_len_min, MinMB * 1024*1024} || MinMB =/= undefined ]
        ++ [ {hunk_count_min, MinHC} || MinHC =/= undefined ],
    WAL =
        {?WAL_SERVER_REG_NAME,
         {brick_hlog_wal, start_link, [WALArgs]},
         permanent, 2000, worker, [brick_hlog_wal]},

    WALWriteBack =
        {?WRITEBACK_SERVER_REG_NAME,
         {brick_hlog_writeback, start_link, [[]]},
         permanent, 2000, worker, [brick_hlog_writeback]},

    BrickBrickSup =
        {brick_brick_sup, {brick_brick_sup, start_link, []},
         permanent, 2000, supervisor, [brick_brick_sup]},
    BrickShepherd =
        {brick_server, {brick_shepherd, start_link, []},
         permanent, 2000, worker, [brick_shepherd]},
    BrickMboxMon =
        {brick_mboxmon, {brick_mboxmon, start_link, []},
         permanent, 2000, worker, [brick_mboxmon]},

    {ok, PrimerRate} = application:get_env(gdss_brick, brick_max_primers),
    BrickPrimerThrottle =
        {brick_primer_limit, {gmt_parallel_limit, start_link,
                             [brick_primer_limit, PrimerRate]},
         permanent, 2000, worker, [gmt_parallel_limit]},

    BrickMetrics =
        {brick_metrics, {brick_metrics, start_link, []},
         permanent, 2000, worker, [brick_metrics]},

    {ok, {{rest_for_one, 3, 60}, [
        %% It's important that all bricks restart if the CommonLog crashes.
                                  H2LevelDB,
                                  BrickMetadataStore,
                                  BrickBlobStoreRegistory,
                                  BrickBlobStore,
                                  BrickBlobStoreCompaction,
                                  WAL,
                                  WALWriteBack,
                                  BrickBrickSup,
                                  BrickShepherd,
                                  BrickMboxMon,
                                  BrickPrimerThrottle,
                                  BrickMetrics
                                ]}}.

%%====================================================================
%% Internal functions
%%====================================================================

-spec get_env(atom()) -> undefined | term().
get_env(PropertyKey) ->
    case application:get_env(gdss_brick, PropertyKey) of
        undefined ->
            undefined;
        {ok, Value} ->
            Value
    end.
