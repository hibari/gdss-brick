%%%-------------------------------------------------------------------
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
%%% File    : brick_sb.erl
%%% Purpose : Brick scoreboard
%%%-------------------------------------------------------------------

%% @doc A "scoreboard" for a bricks: a snapshot of brick and chain
%% health in the very recent past.
%%
%% Note: This scoreboard is mostly passive, it relies on other entities to
%% feed information to it.  If those entities are driven by time polling
%% (and both the brick pinger and chain monitors are), then the scoreboard
%% can contain info that's a little out-of-sync with reality.
%%
%% @todo See the `brick_squorum' comments `handle_cast()' for a
%% source of problems if one or more "simple quorum" bricks dies.
%%
%% NOTE: This module's `init()' function relies on the
%% `brick_admin' server to be running first.

-module(brick_sb).
-include("applog.hrl").


-behaviour(gen_server).

-include("brick.hrl").
-include("brick_admin.hrl").

-ifdef(debug_sb).
-define(gmt_debug, true).
-endif.
-include("gmt_debug.hrl").

%% API
-export([start_link/0,
         get_status/2, get_status/3, get_multiple_statuses/1,
         report_brick_status/4, report_chain_status/3,
         get_brick_history/2, get_all_history/1,
         get_chain_history/1,
         delete_brick_history/2, delete_chain_history/1,
         link_me_to_sb/0, sb_pid/0,
         %% Debugging only
         state/0, dump_status/0, dump_history/1, dump_history/2
        ]).
%% For limited use only, caveat emptor!
-export([report_brick_status_general/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          schema_bricks = [],                   % list(brick())
          status,                               % dict:new()
          history,
          tref                                  % timer ref
         }).

%% History event record, 'hevent', moved to brick_admin.hrl

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

report_brick_status(Brick, Node, Status, PropList) when is_list(PropList) ->
    report_brick_status2(Brick, Node, state_change, Status, PropList).

report_brick_status_general(Brick, Node, What, Status, PropList)
  when is_list(PropList) ->
    report_brick_status2(Brick, Node, What, Status, PropList).

report_brick_status2(Brick, Node, What, Status, PropList)
  when is_list(PropList) ->
    %% Important: don't block the caller/client, so use cast.

    %% Caller should not be using Status = {last_key, _} .
    case Status of
        {last_key, _} -> ?E_ERROR("report_brick_status(~p, ~p, ~p, ~p)\n",
                                  [Brick, Node, Status, PropList]);
        _             -> ok
    end,
    gen_server:cast(?MODULE, {report_status, {brick, {Brick, Node}},
                              What, Status, PropList}).

report_chain_status(Chain, Status, PropList) when is_list(PropList) ->
    %% Important: don't block the caller/client, so use cast.
    gen_server:cast(?MODULE, {report_status, {chain, Chain},
                              state_change, Status, PropList}).

get_brick_history(Brick, Node) ->
    %% Important: don't block the caller/client, but this must be a sync
    %% call, so the server implementation *must* be fast.
    gen_server:call(?MODULE, {get_brick_history, Brick, Node}, 300*1000).

get_chain_history(Chain) ->
    %% Important: don't block the caller/client, but this must be a sync
    %% call, so the server implementation *must* be fast.
    gen_server:call(?MODULE, {get_chain_history, Chain}, 300*1000).

get_all_history(N) ->
    gen_server:call(?MODULE, {get_all_history, N}, 300*1000).

%% @doc Delete a brick's history *asynchronously*.

delete_brick_history(Brick, Node) ->
    gen_server:cast(?MODULE, {delete_history, {brick, {Brick, Node}}}).

delete_chain_history(ChainName) ->
    gen_server:cast(?MODULE, {delete_history, {chain, ChainName}}).

%% @spec () -> true
%% @doc Force a link to the scoreboard proc.

link_me_to_sb() ->
    true = gen_server:call(?MODULE, {link_me_to_sb, self()}).

%% @spec () -> pid()
%% @doc Return the PID of the scoreboard server, throw an exception if
%%      it isn't registered.

sb_pid() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) -> Pid
    end.

get_status(Type, Name) ->
    gen_server:call(?MODULE, {get_status, {Type, Name}}, 300*1000).

get_status(brick, Brick, Node) ->
     gen_server:call(?MODULE, {get_status, {brick, {Brick, Node}}}, 300*1000).

get_multiple_statuses(TypeList) ->
    gen_server:call(?MODULE, {get_multiple_statuses, TypeList}, 300*1000).

state() ->
    gen_server:call(?MODULE, {state}).

dump_status() ->
    gen_server:call(?MODULE, {dump_status}).

dump_history(Key) ->
    dump_history(Key, 10).

dump_history(Key, MaxNum) ->
    gen_server:call(?MODULE, {dump_history, Key, MaxNum}).

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
init([]) ->
    process_flag(trap_exit, true),
    self() ! get_schema_bricks_first_time,
    {ok, TRef} = brick_itimer:send_interval(30*1000, get_schema_bricks),
    self() ! get_history,
    {ok, #state{status = dict:new(), tref = TRef}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({state}, _From, State) ->
    {reply, State, State};
handle_call({dump_status}, _From, State) ->
    S1 = lists:foldl(
           fun({Key, Val}, Acc) ->
                   [io_lib:format("Key: ~w, Val = ~w\n", [Key, Val])|Acc]
           end, "",
           lists:reverse(lists:sort(dict:to_list(State#state.status)))),
    {reply, lists:flatten(S1), State};
handle_call({dump_history, Key, MaxNum}, _From, State) ->
    S1 = case dict:find(Key, State#state.history) of
             {ok, Val} ->
                 [io_lib:format("Key: ~w\nHistory (most recent event first):\n", [Key]),
                  [io_lib:format("\t~w what ~w detail ~w props ~w\n", [calendar:now_to_local_time(Time), What, Detail, Props]) || #hevent{time = Time, what = What, detail = Detail, props = Props} <- lists:sublist(Val, MaxNum)]
                  ];
             error ->
                 "Key does not exist"
         end,
    {reply, lists:flatten(S1), State};
handle_call({get_status, {_Type, _Name} = Key}, _From, State) ->
    Reply = case dict:find(Key, State#state.status) of
                error -> does_not_exist;
                X     -> X
            end,
    {reply, Reply, State};
handle_call({get_multiple_statuses, TypeList}, From, State) ->
    Xs = [handle_call({get_status, X}, From, State) || X <- TypeList],
    {reply, {ok, [Reply || {reply, Reply, _S} <- Xs]}, State};
handle_call({get_brick_history, Brick, Node}, _From, State) ->
    Key = {brick, {Brick, Node}},
    Reply = dict:find(Key, State#state.history),
    {reply, Reply, State};
handle_call({get_chain_history, Chain}, _From, State) ->
    Key = {chain, Chain},
    Reply = dict:find(Key, State#state.history),
    {reply, Reply, State};
handle_call({get_all_history, N}, _From, State) ->
    R1 = dict:fold(
           fun(Key_brick_or_chain, Val, Acc) ->
                   [[{H, Key_brick_or_chain} || H <- Val] | Acc]
           end, [], State#state.history),
    Reply = lists:sublist(lists:reverse(lists:sort(lists:append(R1))), N),
    {reply, Reply, State};
handle_call({link_me_to_sb, Pid}, _From, State) ->
    link(Pid),
    {reply, true, State};
handle_call(_Request, _From, State) ->
    ?APPLOG_WARNING(?APPLOG_APPM_059,"Hey: ~s handle_call: Request = ~w\n", [?MODULE, _Request]),
    Reply = err,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({report_status, _Key, _What, _Status, _PropList} = Msg, State) ->
    ?DBG({report_status, Key, Status, PropList}),
    Msgs = [Msg|get_other_report_status_msgs()],
    {OxsRev, NewState} = lists:foldl(fun(M, {OxList, S}) ->
                                             case make_status_report_op(M, S) of
                                                 {no_op, S2} ->
                                                     {OxList, S2};
                                                 {Ox, S2} ->
                                                     true = (S2 /= S),
                                                     {[Ox|OxList], S2}
                                             end
                                     end, {[], State}, Msgs),
    %% TODO: If there's a quorum error ... losing history
    %% isn't fatal, at least it isn't fatal *immediately*
    %% ... but it can be bad.
    %%
    %% TODO: This call could block us for several seconds, if
    %% one or more of the bootstrap bricks have failed
    %% recently.  Such blocking could cause problems for us,
    %% in the event that there are a lot of other status
    %% reports coming in at the same time (Murphy's Law is
    %% likely).
    case squorum_multiset(State#state.schema_bricks, lists:reverse(OxsRev)) of
        ok  ->
            {noreply, NewState};
        Err ->
            ?APPLOG_ALERT(?APPLOG_APPM_060, "~s: squorum set failure: ~P: ~w\n",
                          [?MODULE, OxsRev, 10, Err]),
            {noreply, NewState}
    end;
handle_cast({delete_history, Key}, State) ->
    case dict:find(Key, State#state.history) of
        {ok, _} ->
            NewStatus = dict:erase(Key, State#state.status),
            NewHistory = dict:erase(Key, State#state.history),
            case squorum_delete(State#state.schema_bricks, Key) of
                ok            -> ok;
                key_not_exist -> ok  end,
            {noreply, State#state{status = NewStatus, history = NewHistory}};
        error ->
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    ?APPLOG_WARNING(?APPLOG_APPM_061,"Hey: ~s handle_cast: Msg = ~w\n", [?MODULE, _Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(get_schema_bricks_first_time = Msg, State) ->
    %% Admin Server is started after us
    case whereis(brick_admin) of
        undefined ->
            ?E_INFO("~s: Admin Server not registered yet, retrying\n",
                    [?MODULE]),
            timer:sleep(1000),
            handle_info(Msg, State);
        _ ->
            Bricks = brick_admin:get_schema_bricks(),
            {noreply, State#state{schema_bricks = Bricks}}
    end;
handle_info(get_schema_bricks, State) ->
    Bricks = brick_admin:get_schema_bricks(),
    {noreply, State#state{schema_bricks = Bricks}};
handle_info(get_history, State) ->
    %% This message sent to us by ourself via init(), used to avoid possibly
    %% blocking for a long time during init().  Otherwise this code would
    %% be in init(), not here.  :-)

    %% Avoid race: wait for brick_admin to be registered.
    gmt_loop:do_while(
      fun(Acc) -> case whereis(brick_admin) of
                      undefined -> timer:sleep(100), {true, Acc};
                      _         -> {false, Acc}
                end
      end, foo),
    %% Load our historical data
    History = load_historical_data(State#state.schema_bricks),
    {noreply, State#state{history = History}};
handle_info({'EXIT', Pid, Why}, State) ->
    ?E_INFO("~s: pid ~p exit reason ~p", [?MODULE, Pid, Why]),
    {noreply, State};
handle_info(_Info, State) ->
    ?APPLOG_WARNING(?APPLOG_APPM_062,"Hey: ~s handle_info: Info = ~w\n", [?MODULE, _Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    catch brick_itimer:cancel(State#state.tref),
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

load_historical_data(Bricks) ->
    Filter1 = fun({history, _}) -> true;
                 (_)            -> false end,
    KeysVals = brick_admin:load_bootstrap_data(Bricks, Filter1),
    HKeysVals = lists:map(
                  fun({{history, HKey}, Val}) -> {HKey, Val} end, KeysVals),
    dict:from_list(HKeysVals).

squorum_multiset(Bricks, KVs) ->
    Ops = [brick_server:make_set(term_to_binary(K),
                                 term_to_binary(V, [{compressed,1}])) ||
              {K, V} <- KVs],
    Start = now(),
    Res = brick_squorum:multiset(Bricks, Ops),
    End = now(),
    case timer:now_diff(End, Start) of
        N when N > 100*1000 ->
            ?E_INFO("~s:squorum_set(ops len ~p) was ~p msec\n",
                     [?MODULE, length(KVs), N div 1000]);
        _ ->
            ok
    end,
    Res.

squorum_delete(Bricks, Key) ->
    brick_squorum:delete(Bricks, term_to_binary(Key)).

get_other_report_status_msgs() ->
    get_other_report_status_msgs([]).

get_other_report_status_msgs(Msgs) ->
    receive
        {'$gen_cast', {report_status, _Key, _What, _Status, _PropList} = Msg} ->
            get_other_report_status_msgs([Msg|Msgs])
    after 0 ->
            lists:reverse(Msgs)
    end.

make_status_report_op({report_status, {Type, Name} = Key,
                       What, Status, PropList}, State) ->
    if Type == chain ->
            ok = brick_admin:chain_status_change(Name, Status, PropList);
       true ->
            skip
    end,

    OldHList = case dict:find(Key, State#state.history) of
                   {ok, HList} -> lists:sublist(HList, 100); % TODO configknob?
                   error       -> []
               end,
    case OldHList of
        [LastE|_] when is_record(LastE, hevent),
        LastE#hevent.detail == repairing,
        Status == repairing ->
            {no_op, State};            % Don't record this same event again
        _ ->
            NewStatus = if What == state_change ->
                                dict:store(Key, Status, State#state.status);
                           true ->
                                State#state.status
                        end,
            HEvent = #hevent{time = now(), what = What, detail = Status,
                             props = PropList},
            NewHList = [HEvent|OldHList],
            NewHistory = dict:store(Key, NewHList, State#state.history),
            {
             {{history, Key}, NewHList},                            % Ox
             State#state{status = NewStatus, history = NewHistory}  % State
            }
    end.
