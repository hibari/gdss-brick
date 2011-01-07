%%%----------------------------------------------------------------------
%%% Copyright (c) 2009-2011 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File     : brick_cinfo.erl
%%% Purpose  : Cluster info/postmortem callback: GDSS config, stats, etc.
%%%----------------------------------------------------------------------

-module(brick_cinfo).

%% Registration API
-export([register/0]).

%% Mandatory callbacks.
-export([cluster_info_init/0, cluster_info_generator_funs/0]).

register() ->
    cluster_info:register_app(?MODULE).

cluster_info_init() ->
    ok.

cluster_info_generator_funs() ->
    [
     {"GDSS: package version", fun package_version/1},
     {"GDSS: application status", fun application_status/1},
     {"GDSS: Partition detector", fun partition_detector/1},
     {"GDSS: Local brick status", fun local_brick_status/1},
     {"GDSS: contents of .../var/data directory", fun var_data_dir/1},
     {"GDSS: NTP peers", fun ntp_peers/1}
    ].

application_status(C) ->
    RunP = lists:keymember(gdss_brick, 1, application:which_applications()),
    cluster_info:format(C, " GDSS application running: ~p\n", [RunP]).

local_brick_status(C) ->
    Bricks = brick_shepherd:list_bricks(),
    [cluster_info:format(C, " Brick ~p status:\n ~p\n\n",
                         [Brick, brick_server:status(Brick, node())]) ||
        Brick <- lists:sort(Bricks)].

ntp_peers(C) ->
    cluster_info:send(C, os:cmd("ntpq -p")).

package_version(C) ->
    cluster_info:send(C, os:cmd("unknown")).

partition_detector(C) ->
    Mod = partition_detector_server,
    cluster_info:format(C, " Active: ~p\n", [catch Mod:is_active()]),
    cluster_info:format(C, " Beacons: ~p\n",
                        [catch lists:sort(Mod:get_last_beacons())]),
    cluster_info:format(C, " State: ~p\n", [catch Mod:get_state()]).

var_data_dir(C) ->
    {ok, Cwd} = file:get_cwd(),
    cluster_info:send(C, [" cwd: ", Cwd, "\n",
                          " ls -l: \n", os:cmd("ls -l")]).
