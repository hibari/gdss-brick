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
%%% File    : brick_sup.erl
%%% Purpose : Top-level brick supervisor
%%%-------------------------------------------------------------------

%% @doc Supervisor for this node's individual brick processes.
%%
%% The brick_sup supervisor has (at the moment) one child:
%%
%% <ul>
%% <li> The brick_data_sup supervisor, which is responsible only for
%%      managing local bricks and therefore unaware (mostly) of other
%%      bricks. </li>
%% </ul>

-module(brick_sup).
-include("applog.hrl").


-behaviour(supervisor).

%% API
-export([start_link/1]).

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
start_link([]) ->
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

    %% In this example, we'll run brick:server:start_link().
    BrickItimer =
        {brick_timer, {brick_itimer, start_link, []},
         permanent, 2000, worker, [brick_itimer]},
    DataSup =
        {brick_data_sup, {brick_data_sup, start_link, []},
         permanent, 2000, supervisor, [brick_data_sup]},

    {ok, {{one_for_all, 0, 1}, [
                                BrickItimer,
                                DataSup
                               ]}}.

%%====================================================================
%% Internal functions
%%====================================================================
