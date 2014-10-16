%%%-------------------------------------------------------------------
%%% Copyright (c) 2011-2014 Hibari developers.  All rights reserved.
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

    {ok, MaxMB} = application:get_env(gdss_brick, brick_max_log_size_mb),
    {ok, MinMB} = application:get_env(gdss_brick, brick_min_log_size_mb),
    WALArgs = [[{file_len_max, MaxMB * 1024*1024},
                {file_len_min, MinMB * 1024*1024}]],
    WAL =
        {?WAL_SERVER_REG_NAME,
         {brick_hlog_wal, start_link, WALArgs},
         permanent, 2000, worker, [brick_hlog_wal]},

    CommonLogArgs = [[{common_log_name, ?GMT_HLOG_COMMON_LOG_NAME},
                      {file_len_max, MaxMB * 1024*1024},
                      {file_len_min, MinMB * 1024*1024}]],
    CommonLog =
        {common_log, {gmt_hlog_common, start_link, CommonLogArgs},
         permanent, 2000, worker, [gmt_hlog_common]},
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
                                  WAL,
                                  CommonLog,
                                  BrickBrickSup,
                                  BrickShepherd,
                                  BrickMboxMon,
                                  BrickPrimerThrottle,
                                  BrickMetrics
                                ]}}.

%%====================================================================
%% Internal functions
%%====================================================================
