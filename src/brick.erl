%%%----------------------------------------------------------------------
%%% Copyright: (c) 2007-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File     : brick.erl
%%% Purpose  : brick top-level application startup
%%%----------------------------------------------------------------------

%% @doc Main application for brick storage: start the `brick_sup'
%% supervisor.

-module(brick).
-include("applog.hrl").


-behaviour(application).

%% application callbacks
-export([start/0, start/2, stop/1]).
-export([start_phase/3, prep_stop/1, config_change/3]).

%%%----------------------------------------------------------------------
%%% Callback functions from application
%%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Func: start/2
%% Returns: {ok, Pid}        |
%%          {ok, Pid, State} |
%%          {error, Reason}
%%----------------------------------------------------------------------
start() ->
    start(xxxwhocares, []).

start(_Type, StartArgs) ->
    %% Set up GMT custom error handler, shutdown on error handler exception.
    gmt_event_h:start_singleton_report_handler(brick, generic, []),
    gmt_event_h:add_exception_fun(fun(_Err) -> application:stop(gdss) end),

    gmt_cinfo_basic:register(),
    brick_cinfo:register(),

    io:format("DEBUG: ~s:start(~p, ~p)\n", [?MODULE, _Type, StartArgs]),
    io:format("DEBUG: ~s: application:start_type() = ~p\n",
              [?MODULE, application:start_type()]),

    case brick_sup:start_link(StartArgs) of
        {ok, Pid} ->
            io:format("DEBUG: ~s:start_phase: self() = ~p, sup pid = ~p\n",
                      [?MODULE, self(), Pid]),
            {ok, Pid};
        Error ->
            io:format("DEBUG: ~s:start bummer: ~w\n", [?MODULE, Error]),
            Error
    end.

%% Lesser-used callbacks....

start_phase(_Phase, _StartType, _PhaseArgs) ->
    io:format("DEBUG: ~s:start_phase(~p, ~p, ~p)\n",
              [?MODULE, _Phase, _StartType, _PhaseArgs]),
    ok.

prep_stop(State) ->
    ?APPLOG_INFO(?APPLOG_APPM_046,"~s:prep_stop(~p)\n", [?MODULE, State]),

    Bs = [Brick || Brick <- brick_shepherd:list_bricks()],
    {FirstBricks, BootstrapBricks} =
        lists:partition(
          fun(Br) ->
                  string:substr(atom_to_list(Br), 1, 10) /= "bootstrap_"
          end, Bs),
    Fstop = fun(Br) ->
                    ?APPLOG_INFO(?APPLOG_APPM_047,"Stopping local brick ~p\n", [Br]),
                    brick_shepherd:add_do_not_restart_brick(Br, node()),
                    brick_shepherd:stop_brick(Br)
            end,
    [catch Fstop(Br) || Br <- FirstBricks],
    timer:sleep(2000),

    %% Finally, shut down any bootstrap bricks.
    [catch Fstop(Br) || Br <- BootstrapBricks],
    State.

config_change(_Changed, _New, _Removed) ->
    io:format("DEBUG: ~s:config_change(~p, ~p, ~p)\n",
              [?MODULE, _Changed, _New, _Removed]),
    ok.


%%----------------------------------------------------------------------
%% Func: stop/1
%% Returns: any
%%----------------------------------------------------------------------
stop(_State) ->
    io:format("DEBUG: ~s:stop(~p)\n", [?MODULE, _State]),
    ok.

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------
