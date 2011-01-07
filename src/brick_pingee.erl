%%%----------------------------------------------------------------------
%%% Copyright (c) 2011 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : brick_pingee.erl
%%% Purpose : Target of a brick pinger.
%%%----------------------------------------------------------------------

-module(brick_pingee).

-behaviour(gen_server).

%% External exports
-export([start_link/2, stop/2, stop/3,
         status/3, set_chain_ok_time/3, make_name/1,
         set_start_time/3,
         set_repair_state/3, set_repair_state/4,
         get_repair_state/2, get_repair_state/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%----------------------------------------------------------------------
%%% Types/Specs/Records
%%%----------------------------------------------------------------------

-record(state,
             { start_time={0,0,0}     :: brick_bp:nowtime()
             , chain_ok_time          :: brick_bp:nowtime() | undefined
             , repair_state=unknown   :: brick_server:repair_state_name()
             }).

-spec init({atom(), brick_bp:nowtime()}) -> {ok, #state{}} | no_return().

-spec set_chain_ok_time(brick_bp:brick_name(), node(), brick_bp:nowtime() | undefined) -> ok.
-spec set_start_time(brick_bp:brick_name(), node(), brick_bp:nowtime()) -> ok.
-spec set_repair_state(brick_bp:brick_name(), node(), brick_server:repair_state_name()) -> ok.
-spec set_repair_state(brick_bp:brick_name(), node(), brick_server:repair_state_name(), timeout()) -> ok.
-spec get_repair_state(brick_bp:brick_name(), node()) -> brick_server:repair_state_name().
-spec get_repair_state(brick_bp:brick_name(), node(), timeout()) -> brick_server:repair_state_name().

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------
start_link(RegName, StartTime) ->
    gen_server:start_link(?MODULE, {RegName, StartTime}, []).

stop(Brick, Node) ->
    stop(Brick, Node, 5*1000).

stop(Brick, Node, Timeout) ->
    gen_server:call({make_name(Brick), Node}, {stop}, Timeout).

status(Brick, Node, Timeout) ->
    gen_server:call({make_name(Brick), Node}, {status}, Timeout).

set_chain_ok_time(Brick, Node, Time) ->
    gen_server:cast({make_name(Brick), Node}, {set_chain_ok_time, Time}).

set_start_time(Brick, Node, Time) ->
    gen_server:call({make_name(Brick), Node}, {set_start_time, Time}).

set_repair_state(Brick, Node, RState) ->
    set_repair_state(Brick, Node, RState, 5*1000).

set_repair_state(Brick, Node, RState, Timeout) ->
    gen_server:call({make_name(Brick), Node}, {set_repair_state, RState}, Timeout).

get_repair_state(Brick, Node) ->
    get_repair_state(Brick, Node, 5*1000).

get_repair_state(Brick, Node, Timeout) ->
    gen_server:call({make_name(Brick), Node}, {get_repair_state}, Timeout).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%%----------------------------------------------------------------------
init({RegName, StartTime}) ->
    register(make_name(RegName), self()),
    process_flag(priority, high),               % our main reason for being
    process_flag(trap_exit, true),              % in case brick exits normally
    {ok, #state{start_time = StartTime}}.

%%----------------------------------------------------------------------
%% Func: handle_call/3
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};
handle_call({status}, _From, State) ->
    {reply, {ok, State#state.start_time, State#state.chain_ok_time}, State};
handle_call({set_start_time, Time}, _From, State) ->
    {reply, ok, State#state{start_time = Time}};
handle_call({set_repair_state, RState}, _From, State) ->
    {reply, ok, State#state{repair_state = RState}};
handle_call({get_repair_state}, _From, State) ->
    {reply, State#state.repair_state, State};
handle_call(_Request, _From, State) ->
    Reply = 'wha???',
    {reply, Reply, State}.

%%----------------------------------------------------------------------
%% Func: handle_cast/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_cast({set_chain_ok_time, Time}, State) ->
    {noreply, State#state{chain_ok_time = Time}};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%----------------------------------------------------------------------
%% Func: handle_info/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_info({'EXIT', _, _} = _Info, State) ->
    io:format("\n\n\n\n: ~p got ~p\n\n\n\n", [self(), _Info]),
    {stop, normal, State};                  % Assume parent brick died
handle_info(_Info, State) ->
    io:format("\n\n\n\n: ~p got ~p\n\n\n\n", [self(), _Info]),
    {noreply, State}.

%%----------------------------------------------------------------------
%% Func: terminate/2
%% Purpose: Shutdown the server
%% Returns: any (ignored by gen_server)
%%----------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%----------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%%----------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

make_name(Atom) when is_atom(Atom) ->
    list_to_atom(atom_to_list(Atom) ++ "_pingee").

