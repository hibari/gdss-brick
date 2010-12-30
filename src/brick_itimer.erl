%%%-------------------------------------------------------------------
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
%%% File    : brick_itimer.erl
%%% Purpose : Interval timer
%%%-------------------------------------------------------------------

%% @doc To replace the use of timer:send_interval().
%%
%% The OTP timer server doesn't scale to many hundreds of timers in the
%% send_interval style, which GDSS uses extensively.
%%
%% Using 1/2 the tables of the NTT-R app, with 54 chains of length 3 each,
%% the timer servier is an obvious bottleneck, roughly 1.5 orders of magnitude
%% more reductions than the next process.
%%

-module(brick_itimer).

-behaviour(gen_server).

-include("brick.hrl").

%% API
-export([start_link/0, send_interval/2, send_interval/3, cancel/1,
         dump/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
%% Internal export
-export([start_interval_loop/1, interval_loop/1]).

-define(SERVER, ?MODULE).

-record(state, {
          interval_d=dict:new() :: dict() % key: interval size, val: pid
         }).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> ignore | {error, term()} | {ok,pid()}.
-spec send_interval(integer(),term()) -> any().
-spec send_interval(integer(),pid(),term()) -> any().
-spec cancel(term()) -> any().


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

send_interval(Interval, Msg) ->
    send_interval(Interval, self(), Msg).

send_interval(Interval, Pid, Msg) ->
    gen_server:call(?SERVER, {send_interval, Interval, Pid, Msg}).

cancel(MRef) ->
    gen_server:call(?SERVER, {cancel, MRef}).

dump() ->
    gen_server:cast(?SERVER, dump).

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
init([]) ->
    {ok, #state{}}.

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
handle_call({send_interval, Interval, Pid, Msg}, From, State) ->
    NewState = do_send_interval(From, Interval, Pid, Msg, State),
    {noreply, NewState};
handle_call({cancel, MRef}, _From, State) ->
    Parent = self(),
    Pids = [begin Pid ! {cancel, MRef, Parent}, Pid end ||
               {_Interval, Pid} <- dict:to_list(State#state.interval_d)],
    Xs = [X || Pid <- Pids, X <- [receive {cancel_reply, Pid, Res} -> Res end],
               X /= sorry],
    case Xs of
        [_] ->
            {reply, ok, State};
        _Hrm ->
            {reply, error, State}
    end.

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
handle_cast(dump, State) ->
    [begin
         ?E_INFO("~s: dump: interval ~p, pid ~p\n", [?MODULE, Interval, Pid]),
         Pid ! dump
     end || {Interval, Pid} <- dict:to_list(State#state.interval_d)],
    {noreply, State};
handle_cast(_Msg, State) ->
    ?E_ERROR("~s: handle_cast: ~P\n", [?MODULE, _Msg, 20]),
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
handle_info(_Info, State) ->
    ?E_ERROR("~s: handle_info: ~P\n", [?MODULE, _Info, 20]),
    {noreply, State}.

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
terminate(_Reason, State) ->
    [Pid ! stop || {_I, Pid} <- dict:to_list(State#state.interval_d)],
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

-spec do_send_interval(pid(), integer(), pid(), term(), #state{}) -> #state{}.
do_send_interval(From, Interval, Pid, Msg, #state{interval_d=IntD}=S) ->
    case dict:find(Interval, IntD) of
        {ok, Server} ->
            Server ! {send_interval, From, Pid, Msg},
            S;
        error ->
            Server = start_interval_server(Interval),
            NewIntD = dict:store(Interval, Server, IntD),
            do_send_interval(From, Interval, Pid, Msg, S#state{interval_d=NewIntD})
    end.

-spec start_interval_server(integer()) -> pid().
start_interval_server(Interval) ->
    spawn_link(?MODULE, start_interval_loop, [Interval]).

-spec start_interval_loop(integer()) -> no_return().
start_interval_loop(Interval) ->
    put(my_interval, Interval),
    timer:send_interval(Interval, tick),
    interval_loop([]).

-spec interval_loop([{pid(), term(), term()}]) -> no_return().
interval_loop(Clients) ->
    receive
        tick ->
            [catch (Pid ! Msg) || {Pid, Msg, _MRef} <- Clients],
            ?MODULE:interval_loop(Clients);
        {send_interval, From, Pid, Msg} ->
            case gmt_util:make_monitor(Pid) of
                {ok, MRef} = Reply ->
                    gen_server:reply(From, Reply),
                    ?MODULE:interval_loop([{Pid, Msg, MRef}|Clients]);
                Err ->
                    gen_server:reply(From, Err),
                    ?MODULE:interval_loop(Clients)
            end;
        {cancel, MRef, Parent} ->
            case lists:keytake(MRef, 3, Clients) of
                {value, _, NewClients} ->
                    Parent ! {cancel_reply, self(), ok},
                    ?MODULE:interval_loop(NewClients);
                false ->
                    Parent ! {cancel_reply, self(), sorry},
                    ?MODULE:interval_loop(Clients)
            end;
        {'DOWN', MRef, _Type, _Pid, _Info} ->
            NewClients = lists:keydelete(MRef, 3, Clients),
            ?MODULE:interval_loop(NewClients);
        dump ->
            UMsgs = lists:usort([Msg || {_Pid, Msg, _MRef} <- Clients]),
            ?E_INFO("~s: pid ~p, clients ~p, types ~p\n",
                    [?MODULE, self(), length(Clients), UMsgs]),
            ?MODULE:interval_loop(Clients);
        stop ->
            exit(normal)
    end.

