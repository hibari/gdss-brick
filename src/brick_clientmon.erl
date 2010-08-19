%%%-------------------------------------------------------------------
%%% Copyright: (c) 2009 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : brick_clientmon.erl
%%% Purpose : Monitor for brick clients
%%%-------------------------------------------------------------------

-module(brick_clientmon).
-include("applog.hrl").


-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          node,                                 % atom()
          app_name,                             % atom()
          fun_up,                               % fun/2
          fun_down,                             % fun/2
          tref,                                 % timer_ref()
          is_up                                 % boolean()
         }).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Node, AppName, FunUp, FunDown)
  when is_function(FunUp, 2), is_function(FunDown, 2) ->
    gen_server:start_link(?MODULE, [Node, AppName, FunUp, FunDown], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Node, AppName, FunUp, FunDown]) ->
    put(i_am_monitoring, {Node, AppName}),
    {ok, TRef} = brick_itimer:send_interval(990, check_status),
    %% Start is_up = false so that we'll NOT set alarm if node is already down.
    {ok, #state{node = Node, app_name = AppName, fun_up = FunUp,
                fun_down = FunDown, tref = TRef, is_up = false}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(check_status, State) ->
    {noreply, do_check_status(State)};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

do_check_status(S) ->
    try
        pong = net_adm:ping(S#state.node),
        As = rpc:call(S#state.node, application, which_applications, []),
        case [yes || {App, _, _} <- As, App == S#state.app_name] of
            [yes] ->
                do_up(S),
		flush_mailbox_extra_check_status(),
                S#state{is_up = true}
        end
    catch
        _X:_Y ->
            do_down(S),
	    flush_mailbox_extra_check_status(),
            S#state{is_up = false}
    end.

do_up(S) when not S#state.is_up ->
    case (catch (S#state.fun_up)(S#state.node, S#state.app_name)) of
        {'EXIT', Reason} ->
            ?APPLOG_ALERT(?APPLOG_APPM_044,"~s:do_up: EXIT for ~p ~p -> ~p\n",
                          [?MODULE, S#state.node, S#state.app_name,
                           Reason]);
        _ ->
            ok
    end;
do_up(_) ->
    ok.

do_down(S) when S#state.is_up ->
    case (catch (S#state.fun_down)(S#state.node, S#state.app_name)) of
        {'EXIT', Reason} ->
            ?APPLOG_ALERT(?APPLOG_APPM_045,"~s:do_down: EXIT for ~p ~p -> ~p\n",
                          [?MODULE, S#state.node, S#state.app_name,
                           Reason]);
        _ ->
            ok
    end;
do_down(_) ->
    ok.

flush_mailbox_extra_check_status() ->
    receive
	check_status ->
	    flush_mailbox_extra_check_status()
    after 0 ->
	    ok
    end.
