%%%-------------------------------------------------------------------
%%% Copyright: (c) 2006-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : brick_admin_sup.erl
%%% Purpose : Admin supervisor
%%%-------------------------------------------------------------------

%% @doc Supervisor of all admin-related processes.
%%
%% NOTE: TODO It's quite important that only one admin be running in
%% any particular cluster that also shares the same bootstrap info
%% storage bricks.  Right now, there is no mechanism to avoid running
%% more than one.  It's probably a very good idea to use `gen_leader'
%% as the smarts to avoid such chaos.
%%
%% It may also be a good idea to move the admin tasks into a separate
%% Erlang/OTP application.

-module(brick_admin_sup).
-include("applog.hrl").


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

    %% The monitor supervisor is the first permanent child.
    MonitorSup =
        {brick_mon_sup, {brick_mon_sup, start_link, []},
         permanent, 2000, supervisor, [brick_mon_sup]},

    WebAdminSup =
        {web_admin_sup, {web_admin_sup, start_link, [ignored_arg]},
         permanent, 2000, supervisor, [web_admin_sup]},

    {ok, {{one_for_one, 12, 60}, [
                                 MonitorSup,
                                 WebAdminSup
                                ]}}.

%%====================================================================
%% Internal functions
%%====================================================================
