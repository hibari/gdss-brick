%%%----------------------------------------------------------------------
%%% Copyright: (c) 2009-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File     : gmt_cinfo_basic.erl
%%% Purpose  : Cluster info/postmortem callback: GDSS config, stats, etc.
%%%----------------------------------------------------------------------

-module(brick_cinfo).
-include("applog.hrl").


-include("brick_admin.hrl").

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
     {"GDSS: Admin Server status", fun admin_server_status/1},
     {"GDSS: Bootstrap brick config", fun bootstrap_config/1},
     {"GDSS: Admin Server schema", fun admin_server_schema/1},
     {"GDSS: Partition detector", fun partition_detector/1},
     {"GDSS: Admin Status top", fun admin_status_top/1},
     {"GDSS: Admin Status client monitors", fun admin_status_client_mons/1},
     {"GDSS: Local brick status", fun local_brick_status/1},
     {"GDSS: History dump", fun history_dump/1},
     {"GDSS: contents of .../var/data directory", fun var_data_dir/1},
     {"GDSS: NTP peers", fun ntp_peers/1}
    ].

admin_server_schema(C) ->
    Schema = (catch brick_admin:get_schema()),
    cluster_info:format(C, " Schema brick list:\n ~p\n\n",
                        [Schema#schema_r.schema_bricklist]),
    cluster_info:format(C, " Table definitions:\n ~p\n\n",
                        [dict:to_list(Schema#schema_r.tabdefs)]),
    cluster_info:format(C, " Chain to table mapping:\n ~p\n\n",
                        [dict:to_list(Schema#schema_r.chain2tab)]).

admin_server_status(C) ->
    cluster_info:format(C, " My node: ~p\n", [node()]),
    cluster_info:format(C, " Admin Server node: ~p\n",
                        [catch node(global:whereis_name(brick_admin))]),
    {ok, Nodes} =
        gmt_config_svr:get_config_value(admin_server_distributed_nodes, ""),
    cluster_info:format(C, " Admin Server eligible nodes: ~p\n", [Nodes]).

admin_status_top(C) ->
    admin_http_to_text(C, "/").

admin_status_client_mons(C) ->
    admin_http_to_text(C, "/change_client_monitor.html").

application_status(C) ->
    RunP = lists:keymember(gdss, 1, application:which_applications()),
    cluster_info:format(C, " GDSS application running: ~p\n", [RunP]).

bootstrap_config(C) ->
    {ok, Bin} = file:read_file("Schema.local"),
    cluster_info:send(C, Bin).

history_dump(C) ->
    Tmp = lists:flatten(io_lib:format("/tmp/history.~p", [now()])),
    Res = try
              mod_admin:dump_history(Tmp),
              {ok, Out} = file:read_file(Tmp),
              Out
          catch X:Y ->
                  io_lib:format("Error ~p ~p at ~p\n",
                                [X, Y, erlang:get_stacktrace()])
          after
              file:delete(Tmp)
          end,
    cluster_info:send(C, Res).

local_brick_status(C) ->
    Bricks = brick_shepherd:list_bricks(),
    [cluster_info:format(C, " Brick ~p status:\n ~p\n\n",
                         [Brick, brick_server:status(Brick, node())]) ||
        Brick <- lists:sort(Bricks)].

ntp_peers(C) ->
    cluster_info:send(C, os:cmd("ntpq -p")).

package_version(C) ->
    cluster_info:send(C, os:cmd("sh -c \"egrep 'echo.*HOME' "
                                "/etc/init.d/tcl /etc/init.d/ert "
                                "/etc/init.d/gdss\"")).

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

%%%%%%%%%%

admin_http_to_text(C, UriSuffix) ->
    URL = "http://localhost:" ++
        gmt_util:list_ify(mod_admin:admin_server_http_port()) ++
        UriSuffix,
    Prog = case os:cmd("which lynx") of
               [$/|_] ->
                   "lynx -dump -width 130 ";
               _ ->
                   case os:cmd("which elinks") of
                       [$/|_] ->
                           "elinks -dump -dump-width 130 ";
                       _ ->
                           "echo Not available: "
                   end
           end,
    cluster_info:send(C, os:cmd(Prog ++ URL)).



