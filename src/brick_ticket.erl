%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2013 Hibari developers.  All rights reserved.
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
%%% File    : brick_ticket.erl
%%%
%%% @doc A simple gen_server-based ticket server without a dependence
%%%      on the OTP 'global' naming service.
%%%
%%% The main reason for using gen_server for this simple server is to
%%% allow remote callers to detect if the remote server isn't running.
%%%-------------------------------------------------------------------

-module(brick_ticket).

-include("gmt_hlog.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2, start_link/3, start_link/4, get/2, stop/1,
         get_rate/1, change_rate/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          name,                 % undefined | atom()
          count,                % integer() counter reset amount
          current,              % integer() current counter
          timeout,              % infinity | integer() milliseconds of timeout
          tref                  % brick_itimer tref
         }).

%%%===================================================================
%%% API
%%%===================================================================

-type serverref() :: file:name() | {file:name(),atom()} | {global,atom()} | pid().

-spec get(serverref(),integer()) -> ok.
-spec start_link(atom(),integer()) -> ignore | {error,term()} | {ok,pid()}.
-spec stop(serverref()) -> any().

start_link(Name, Count) ->
    start_link(Name, Count, 1000).

start_link(Name, Count, Interval) ->
    start_link(Name, Count, Interval, infinity).

%% @spec start_link(undefined | atom(), integer(), integer(), integer()) ->
%%       {ok, Pid} | ignore | {error, Error}

start_link(Name, Count, Interval, Timeout) ->
    gen_server:start_link(?MODULE, [Name, Count, Interval, Timeout], []).

get(Server, N) when N >= 0 ->
    try case gen_server:call(Server, {get, N}) of
            Got when is_integer(Got) ->
                if Got < N ->
                        timer:sleep(250),
                        get(Server, N - Got);
                   true ->
                        ok
                end
        end
    catch _X:_Y ->
            %% Server doesn't exist, pretend there's an infinite amount.
            ok
    end.

stop(Server) ->
    gen_server:call(Server, {stop}).

get_rate(Server) ->
    gen_server:call(Server, {get_rate}).

change_rate(Server, NewRate) ->
    gen_server:call(Server, {change_rate, NewRate}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initiates the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Name, Count, Interval, Timeout]) ->
    try
        if Name =:= undefined ->
                ok;
           true ->
                true = register(Name, self())
        end,
        _ = brick_itimer:send_interval(Interval, refresh_counter),
        {ok, #state{name = Name, count = Count, current = 0,
                    timeout = Timeout}}
    catch
        _:_ ->
            ignore
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({get, N}, _From, State) ->
    case State#state.current - N of
        New when New >= 0 ->
            {reply, N, State#state{current = New}, State#state.timeout};
        _X when State#state.current =:= 0 ->
            {reply, 0, State, State#state.timeout};
        _X when N =< 0 ->
            {reply, 0, State, State#state.timeout};
        _X ->
            {reply, State#state.current, State#state{current = 0},
             State#state.timeout}
    end;
handle_call({get_rate}, _From, State) ->
    {reply, State#state.count, State};
handle_call({change_rate, NewRate}, _From, State) ->
    {reply, ok, State#state{count = NewRate}};
handle_call({stop}, _From, State) ->
    {stop, normal, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(refresh_counter, State) ->
    {noreply, State#state{current = State#state.count}};
handle_info(timeout, State) ->
    {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
