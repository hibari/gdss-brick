%%%-------------------------------------------------------------------
%%% Copyright: (c) 2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : brick_chainmon.erl
%%% Purpose : Chain status monitor
%%%-------------------------------------------------------------------

%% @doc The brick chain status monitor.
%%
%% This module is implemented as a state machine with the following
%% major states (not definitive, read the code!):
%% <ul>
%% <li> `init', the initial state </li>
%% <li> `unknown', the state of the chain is unknown.  Information
%%   regarding chain members is unavailable. </li>
%% <li> `unknown_timeout', information regarding one or more chain members
%%   remains unavailable after a certain period of time. </li>
%% <li> `healthy', all members of the chain are running and in sync. </li>
%% <li> `degraded', one or more members of the chain are either stopped
%%   or are in the process of joining the chain and re-syncing the
%%   chain's data. </li>
%% <li> `stopped', all members of the chain are stopped/crashed/dead. </li>
%% </ul>
%%
%% A 1-second periodic timer is used to send a `check_status' message
%% to the FSM.  The `do_check_status()' function implements its own 10
%% second simulated-timeout in the event that the FSM has been in the
%% `unknown` state during that time.  Otherwise, `do_check_status()'
%% will attempt to calculate the current state of each chain member.
%%
%% Chain member status is tricky to calculate correctly.  Any chain
%% monitor process may crash at any time (e.g. due to bugs), or the
%% entire machine hosting the monitor may crash.  When the monitor has
%% restarted, it doesn't know how many times chain members may have
%% changed state.
%%
%% Each monitor queries the "scoreboard" to get the current status of
%% each chain member.  (Remember: The scoreboard's info may be
%% slightly out-of-date!)  Each current status is compared with the
%% monitor's in-memory history of the status during the last check.
%% (All bricks start in the `unknown' state.)
%% If there's a difference, then suitable action is taken.
%%
%% Each brick's scoreboard status is converted to an internal status:
%%
%% <ul>
%% <li> `unknown', the brick's status is not known. </li>
%% <li> `disk_error', the brick has hit a disk checksum error and has
%%   not been able to initialize itself 100%. </li>
%% <li> `pre_init', the brick is running and ping'able, but the
%%   brick is not in service, and the state of the brick's local
%%   storage is unknown. </li>
%% <li> `repairing', the monitor has chosen this brick to be the next
%%   brick to resume service in the chain.  Its local storage is actively
%%   being repaired by chain's current tail. </li>
%% <li> `repair_overload', if a brick was in `repairing' state and was
%%   determined to be overloaded (usually by too much disk I/O),
%%   the node can be switched to this state to halt repair. </li>
%% <li> `ok', the brick is fully in-sync with the rest of the chain
%%   and is in service in its correct chain role. </li>
%% </ul>
%%
%% Also, any change in brick or chain status is also reported to the
%% scoreboard.
%%
%% It may be worth exploring whether to make new processes for each
%% brick/chain member, which polls the scoreboard for its own status,
%% then informs its chain monitor of changes in its state?  I didn't
%% do that originally over worries about race conditions with various
%% processes crashing.  Also, that kind of polling seemed a bit close
%% to what the "brick pinger" processes do anyway, which could then
%% call into question the role/wisdom of the scoreboard.

-module(brick_chainmon).
-include("applog.hrl").


-behaviour(gen_fsm).

-include("brick.hrl").
-include("brick_admin.hrl").
-include("brick_hash.hrl").

-define(EMPTY_CHAIN_NOW, {chain_now, []}).

%% API
-export([start_link/2, force_best_first_brick/3]).

-export([start_mon/1, state/1, chain2name/1]).

%% gen_fsm callbacks
-export([init/1, unknown/2, unknown/3, unknown_timeout/2, unknown_timeout/3,
         healthy/2, healthy/3, degraded/2, degraded/3, stopped/2, stopped/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%% Unit testing exports (not public API functions)
-export([set_all_chain_roles/1,
         stitch_op_state/1, figure_out_current_chain_op_status/1]).

-export_type([brick_list/0]).

-type internal_status() :: unknown | pre_init | ok | repairing | repair_overload | disk_error.
-type brick_list() :: list({brick_bp:brick_name(), node()}).
-type brickstatus_list() :: list({brick_server:brick(), internal_status() | unknown2}).

-record(state, {
          chain                :: brick_server:chain_name(),
          bricks               :: brick_list(),
          chainlen             :: integer(),
          tref                 :: reference(),
          last_bricks_status   :: brickstatus_list(),
          chain_now            :: brick_list() | undefined,
          repairing_brick      :: {brick_bp:brick_name(), node()} | undefined, %% brick in degraded state
          num_checks = 0       :: integer(),
          sb_pid                           :: pid()     %% pid of scoreboard server
         }).

%% For opconf_r record, see brick_admin.hrl

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> ok,Pid} | ignore | {error,Error}
%% Description:Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this function
%% does not return until Module:init/1 has returned.
%%--------------------------------------------------------------------
start_link(Chain, Bricks) ->
    Name = chain2name(Chain),
    gen_fsm:start_link({local, Name}, ?MODULE, [Chain, Bricks], []).

%% @spec (chain_name(), brick_name(), node()) -> ok
%% @doc Force a chain member to be the considered the "best" brick with
%% the most up-to-date copy of the chain's data.
%%
%% When a chain is in 'stopped' state, the chain monitor's current
%% implementation will calculate the "best" brick to start first.  The
%% best brick is the one that has the most up-to-date copy of the
%% chain's data.  However, due to hardware failures or other disaster
%% scenarios, it may be desirable to force the chain monitor to use
%% another brick to get the chain out of 'stopped' state and into
%% 'degraded' state.

force_best_first_brick(Chain, Brick, Node) ->
    case brick_server:status(Brick, Node) of
        {ok, Status} ->
            Ps = proplists:get_value(chain, Status, []),
            ChainRole = proplists:get_value(chain_role, Ps),
            RepairState = proplists:get_value(chain_my_repair_state, Ps),
            case {ChainRole, RepairState} of
                {undefined, pre_init} ->
                    brick_sb:report_brick_status_general(
                      Brick, Node, administrative_intervention,
                      force_best_first_brick, [{chain, Chain}]),
                    ok = brick_server:chain_set_my_repair_state(Brick, Node, ok),
                    brick_sb:report_brick_status(
                      Brick, Node, ok, [force_best_first_brick,{chain, Chain}]);
                _ ->
                    {error, [{role, ChainRole}, {repair_state, RepairState}]}
            end
    end.

start_mon({Chain, Bricks}) ->
    ArgList = [Chain, Bricks],
    Name = chain2name(Chain),
    ChildSpec =
        {Name, {brick_chainmon, start_link, ArgList},
         transient, 2000, worker, [brick_chainmon]},
    case supervisor:start_child(brick_mon_sup, ChildSpec) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        Err                           -> Err
    end.

state(ServerRef) ->
    gen_fsm:sync_send_all_state_event(ServerRef, {state}).

%%====================================================================
%% gen_fsm callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, StateName, State} |
%%                         {ok, StateName, State, Timeout} |
%%                         ignore                              |
%%                         {stop, StopReason}
%% Description:Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/3,4, this function is called by the new process to
%% initialize.
%%--------------------------------------------------------------------
init([Chain, Bricks]) ->
    self() ! finish_init_tasks,
    random:seed(now()),
    {ok, Time} = gmt_config_svr:get_config_value_i(admin_server_chain_poll, 4000),
    {ok, TRef} = brick_itimer:send_interval(Time, check_status),
    SBPid = brick_sb:sb_pid(),
    {ok, _} = gmt_util:make_monitor(SBPid),
    {ok, unknown, #state{chain = Chain, bricks = Bricks,
                         chainlen = length(Bricks), tref = TRef,
                         sb_pid = SBPid}}.

%%--------------------------------------------------------------------
%% Function:
%% state_name(Event, State) -> {next_state, NextStateName, NextState}|
%%                             {next_state, NextStateName,
%%                                NextState, Timeout} |
%%                             {stop, Reason, NewState}
%% Description:There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same name as
%% the current state name StateName is called to handle the event. It is also
%% called if a timeout occurs.
%%--------------------------------------------------------------------
unknown(_Event, S) ->
    ?E_ERROR("~s: unknown 2: ~p\n", [?MODULE, _Event]),
    {next_state, unknown, S}.

unknown_timeout(evt_timeout_during_unknown, S) ->
    do_unknown_timeout_decision(S).

healthy(_Event, S) ->
    {next_state, healthy, S}.

%% Remember: degraded means at least 1 brick is up and ok, but not all bricks.

degraded({evt_degraded, __N, __ChangingBrick} = _Event, S) ->
    ?DBG_CHAINx({degraded, _Event}),
    ChainNow = S#state.chain_now,
    NotInNow = S#state.bricks -- ChainNow,
    if length(ChainNow) == S#state.chainlen ->
            %% The chain is complete, so use the configured brick order.
            {ok, GH} = brick_admin:get_gh_by_chainname(S#state.chain),
            if GH#g_hash_r.migrating_p ->
                    ?E_INFO(
                      "Chain ~w: degraded -> healthy, migration in effect, "
                      "keeping chain order as ~w\n",[S#state.chain, ChainNow]),
                    brick_sb:report_chain_status(S#state.chain, healthy,
                                                 [{chain_now, ChainNow}]),
                    timer:sleep(1000),
                    ok = set_all_chain_roles(ChainNow, S);
               true ->
                    ?E_INFO(
                      "Chain ~w: degraded -> healthy, no migration, resetting "
                      "chain order as ~w\n", [S#state.chain, S#state.bricks]),
                    brick_sb:report_chain_status(S#state.chain, healthy,
                                                 [{chain_now, S#state.bricks}]),
                    timer:sleep(1000),
                    ok = set_all_chain_roles_reorder(
                           S#state.chain, S#state.bricks, S#state.chain_now, S)
            end,
            {next_state, healthy, S#state{chain_now = S#state.bricks,
                                          repairing_brick = undefined}};
       true ->
            brick_sb:report_chain_status(S#state.chain, degraded,
                                             [{chain_now, ChainNow}]),
            %% We won't choose a new brick to start.  Instead, we'll play a
            %% game with the last_bricks_status: all bricks that are not part
            %% of the chain will be set to unknown state, and the next status
            %% check will take care of identifying the next candidate brick.
            ?DBG_CHAINx(S#state.bricks),
            ?DBG_CHAINx(ChainNow),
            ?DBG_CHAINx(NotInNow),
            ?DBG_CHAINx(S#state.last_bricks_status),
            Repairing = S#state.repairing_brick,
            LBS = lists:map(fun({Brick, _} = X) when Brick == Repairing ->
                                    X;          % Don't play with this status!
                               ({Brick, _} = X) ->
                                    case lists:member(Brick, NotInNow) of
                                        true -> {Brick, unknown};
                                        _    -> X
                                    end
                            end, S#state.last_bricks_status),
            ?DBG_CHAINx(LBS),
            {next_state, degraded, S#state{last_bricks_status = LBS}}
    end;
degraded(_Event, S) ->
    ?E_ERROR("~s: degraded 2: ~p\n", [?MODULE, _Event]),
    {next_state, degraded, S}.

stopped(_Event, S) ->
    ?E_ERROR("~s: stopped 2: ~p\n", [?MODULE, _Event]),
    {next_state, stopped, S}.

%%--------------------------------------------------------------------
%% Function:
%% state_name(Event, From, State) -> {next_state, NextStateName, NextState} |
%%                                   {next_state, NextStateName,
%%                                     NextState, Timeout} |
%%                                   {reply, Reply, NextStateName, NextState}|
%%                                   {reply, Reply, NextStateName,
%%                                    NextState, Timeout} |
%%                                   {stop, Reason, NewState}|
%%                                   {stop, Reason, Reply, NewState}
%% Description: There should be one instance of this function for each
%% possible state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/2,3, the instance of this function with the same
%% name as the current state name StateName is called to handle the event.
%%--------------------------------------------------------------------
unknown(_Event, _From, State) ->
    ?E_ERROR("~s: unknown 3: ~p\n", [?MODULE, _Event]),
    Reply = ok,
    {reply, Reply, unknown, State}.

unknown_timeout(_Event, _From, State) ->
    ?E_ERROR("~s: unknown_timeout 3: ~p\n", [?MODULE, _Event]),
    Reply = ok,
    {reply, Reply, unknown_timeout, State}.

healthy(_Event, _From, State) ->
    ?E_ERROR("~s: healthy 3: ~p\n", [?MODULE, _Event]),
    Reply = ok,
    {reply, Reply, healthy, State}.

degraded(_Event, _From, State) ->
    ?E_ERROR("~s: degraded 3: ~p\n", [?MODULE, _Event]),
    Reply = ok,
    {reply, Reply, degraded, State}.

stopped(_Event, _From, State) ->
    ?E_ERROR("~s: stopped 3: ~p\n", [?MODULE, _Event]),
    Reply = ok,
    {reply, Reply, stopped, State}.

%%--------------------------------------------------------------------
%% Function:
%% handle_event(Event, StateName, State) -> {next_state, NextStateName,
%%                                                NextState} |
%%                                          {next_state, NextStateName,
%%                                                NextState, Timeout} |
%%                                          {stop, Reason, NewState}
%% Description: Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%--------------------------------------------------------------------
handle_event(Event, StateName, State) ->
    ?E_ERROR("~s: handle_event: ~P in ~p\n",
                           [?MODULE, Event, 20, StateName]),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% Function:
%% handle_sync_event(Event, From, StateName,
%%                   State) -> {next_state, NextStateName, NextState} |
%%                             {next_state, NextStateName, NextState,
%%                              Timeout} |
%%                             {reply, Reply, NextStateName, NextState}|
%%                             {reply, Reply, NextStateName, NextState,
%%                              Timeout} |
%%                             {stop, Reason, NewState} |
%%                             {stop, Reason, Reply, NewState}
%% Description: Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/2,3, this function is called to handle
%% the event.
%%--------------------------------------------------------------------
handle_sync_event({state}, _From, StateName, State) ->
    Ps = state_to_proplist(State),
    {reply, [{state_name, StateName}|Ps], StateName, State};
handle_sync_event(Event, _From, StateName, State) ->
    ?E_ERROR("~s: handle_sync_event: ~P in ~p\n",
                           [?MODULE, Event, 20, StateName]),
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% Function:
%% handle_info(Info,StateName,State)-> {next_state, NextStateName, NextState}|
%%                                     {next_state, NextStateName, NextState,
%%                                       Timeout} |
%%                                     {stop, Reason, NewState}
%% Description: This function is called by a gen_fsm when it receives any
%% other message than a synchronous or asynchronous event
%% (or a system message).
%%--------------------------------------------------------------------
-spec handle_info(term(), atom(), #state{}) -> {next_state, atom(), #state{}}.
handle_info(check_status, StateName, State) ->
    timer:sleep(random:uniform(500)),
    do_check_status(StateName, State);
handle_info(finish_init_tasks, unknown = StateName, State) ->
    brick_sb:report_chain_status(State#state.chain, unknown,
                                 [?EMPTY_CHAIN_NOW]),
    LastBricksStatus = [{B, unknown} || B <- State#state.bricks],
    {next_state, StateName, State#state{last_bricks_status = LastBricksStatus}};
handle_info({'DOWN', _MRef, _Type, Pid, Info}, _StateName, State)
  when Pid == State#state.sb_pid ->
    ?E_INFO("~s: ~p: scoreboard process ~p stopped, ~p\n",
            [?MODULE, State#state.chain, Pid, Info]),
    timer:sleep(1000), % Allow some time for scoreboard to recover
    {stop, scoreboard_down, State};
handle_info(_Info, StateName, State) ->
    ?E_ERROR("~s: handle_info: ~P\n", [?MODULE, _Info, 20]),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, StateName, State) -> void()
%% Description:This function is called by a gen_fsm when it is about
%% to terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Function:
%% code_change(OldVsn, StateName, State, Extra) -> {ok, StateName, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

-spec do_check_status(atom(), #state{}) -> {next_state, atom(), #state{}}.
do_check_status(unknown, S) when S#state.num_checks == 10 ->
    gen_fsm:send_event(self(), evt_timeout_during_unknown),
    brick_sb:report_chain_status(S#state.chain, unknown_timeout,
                                 [?EMPTY_CHAIN_NOW]),
    {next_state, unknown_timeout, S};
do_check_status(StateName, S) ->
    do_check_status2(StateName, S).

-spec calculate_any_best_first_brick(#state{}) -> list(brick_server:brick()).
calculate_any_best_first_brick(S) ->
    Fpoll = fun({Br, Nd} = B) ->
                    case brick_sb:get_brick_history(Br, Nd) of
                        {ok, HList} ->
                            {false, HList};
                        _  ->
                            ?DBG_CHAIN("CCC p1 ~w ", [B]),
                            timer:sleep(200),
                            {true, B}
                    end
            end,
    Hist1 = lists:map(
              fun({Br, Nd} = B) ->
                      HList = gmt_loop:do_while(Fpoll, {Br, Nd}),
                      {HList, B}
              end, lists:sort(S#state.bricks)),
    SortHist = calculate_best_first_brick2(Hist1),
    ?DBG_CHAINx(SortHist),
    [Br || {_LastDownTime, Br} <- SortHist].

-spec calculate_best_first_brick(#state{}) -> brick_server:brick().
calculate_best_first_brick(S) ->
    hd(calculate_any_best_first_brick(S)).

%% Hack test:
%% H1 = [{hevent,{1181,261593,319478},state_change,ok,[]},{hevent,{1181,261588,416246},state_change,pre_init,[]},{hevent,{1181,261588,331630},state_change,unknown,[]},{hevent,{1181,261393,122845},state_change,ok,[]},{hevent,{1181,261388,219181},state_change,pre_init,[]},{hevent,{1181,261388,134819},state_change,unknown,[]},{hevent,{1181,261279,191694},state_change,pre_init,[]},{hevent,{1181,261279,122251},state_change,unknown,[]}].
%% brick_chainmon:calculate_best_first_brick2(foo, [{ lists:sublist(H1, 3, 3) ++ H1, {hd,baz}}, {H1, {mid,foo}}, {[lists:last(H1)], {tl, bar}}]).

%% @spec (list({list(hevent()), brick_t()})) -> {term(), brick_t()}
%% @doc Calculate the best first brick to start in a chain.
%%
%% The calculation is done by finding the last not-ok event that occurs
%% after the last ok event for each brick in the chain.  Then we use
%% a bit of Erlang term comparison ordering hack to make the code smaller:
%%
%% <code>
%%     number &lt; atom &lt; reference &lt; fun &lt; port &lt; pid &lt; tuple &lt; list &lt; binary
%% </code>
%%
%% So, there are three cases:
%% <ul>
%% <li> Brick has never been in 'ok' state.  We assign the atom
%%      never_been_up as the last up "time" for this brick.  Any
%%      atom will sort before any tuple (i.e. a legit erlang:now() 3-tuple)
%%      or a binary.</li>
%% <li> A brick has been in an 'ok' state, but then it changed to a non-ok
%%      state, e.g. 'unknown'.  After the last 'ok' event, it's the
%%      first state transition <b>after that</b> that tells us when the
%%      last time the brick had up-to-date data.  So we use the erlang:now()
%%      3-tuple for our sorting. </li>
%% <li> A brick has been in an 'ok' state, but the history does not include
%%      any non-ok state afterward.  So, as far as we know, that brick has
%%      always been up and therefore should be 100% up-to-date.  So we use
%%      the binary &lt;&lt;"No record of down"&gt;&gt; so it will always sort
%%      ahead of the other two cases. </li>
%% </ul>
-spec calculate_best_first_brick2(list({list(#hevent{}), brick_server:brick()})) -> list({atom()|binary()|brick_bp:nowtime(), brick_server:brick()}).
calculate_best_first_brick2(Hist1) ->
    Fis_an_ok = fun(#hevent{what = state_change, detail = ok}) ->
                        true;
                   (_) ->
                        false
                end,
    Fis_not_an_ok = fun(X) -> not Fis_an_ok(X) end,
    Hist2 = lists:map(
              fun ({HList0, B}) ->
                      %% For this calculation, 'pre_init' state
                      %% changes can make a difference between case #2
                      %% and case #3, if we leave them in and use the
                      %% algorithm below.  As far as we're concerned,
                      %% 'pre_init' isn't a state change that helps us
                      %% determine a best first brick.
                      HList = lists:filter(
                                fun(#hevent{what = state_change,
                                            detail = pre_init}) ->
                                        false;
                                   (_) ->
                                        true
                                end, HList0),
                      case lists:any(Fis_an_ok, HList) of
                          false ->
                              %% Case 1.
                              {never_been_up, B};
                          true ->
                              case lists:takewhile(Fis_not_an_ok, HList) of
                                  [_|_] = HList2 ->
                                      %% Case 2
                                      Last = lists:last(HList2),
                                      {Last#hevent.time, B};
                                  [] ->
                                      %% Case 3
                                      {<<"No record of down">>, B}
                              end
                      end
              end, Hist1),
    %% We want binaries first, then tuples, then atoms ... so reverse the
    %% default Erlang term sort order.
    lists:reverse(lists:sort(Hist2)).

-spec do_check_status2(atom(), #state{}) -> {next_state, atom(), #state{}}.
do_check_status2(StateName, S) when is_record(S, state) ->
    LastBricksStatus = S#state.last_bricks_status,
    NewBricksStatus = get_brickstatus(S#state.bricks),
    DiffList = diff_brickstatus(LastBricksStatus, NewBricksStatus),
    check_difflist_for_disk_error(DiffList, S),
    {NewStateName, NewS} =
        if DiffList /= [] ->
                %%?E_INFO("QQQ ZZZ: StateName = ~p, diff = ~p\n", [StateName, DiffList]),
                %%?E_INFO("QQQ ZZZ: repairing_brick = ~p\n", [S#state.repairing_brick]),
                process_brickstatus_diffs(StateName, DiffList,
                                         LastBricksStatus, NewBricksStatus, S);
           true ->
                {StateName, S}
        end,
    NumChecks =
        if NewStateName /= StateName ->
                Extra = if NewS#state.chain_now == [] ->
                                BestBrick = calculate_best_first_brick(S),
                                [{best_brick, BestBrick}];
                           true ->
                                []
                        end,
                brick_sb:report_chain_status(
                  NewS#state.chain, NewStateName,
                  [qqq_delme,
                   {difflist, DiffList},
                   {updated_last_bricks_status, NewS#state.last_bricks_status},
                   {chain_now, NewS#state.chain_now}] ++ Extra),
                if NewStateName == unknown ->
                        0;
                   true ->
                        NewS#state.num_checks + 1
                end;
           true ->
                NewS#state.num_checks + 1
        end,
    {next_state, NewStateName, NewS#state{num_checks = NumChecks}}.

-spec chain2name(brick_server:chain_name()) -> atom().
chain2name(Chain) ->
    list_to_atom(lists:flatten(io_lib:format("chmon_~w", [Chain]))).

-spec get_brickstatus(brick_list()) -> list({brick_server:brick(), internal_status()}).
get_brickstatus(Bricks) ->
    {ok, Statuses} = brick_sb:get_multiple_statuses(
                       [{brick, Br} || Br <- Bricks]),
    BrsStats = lists:zip(Bricks, Statuses),
    [{B, external_to_internal_status(B, Status)} || {B, Status} <- BrsStats].

%% Note weird quirk in {ok, _} wrapper around Status!!

-spec external_to_internal_status(brick_server:brick(), {ok, brick_bp:state_name()} | atom()) -> internal_status().
external_to_internal_status(_B, Status) ->
    case Status of
        {ok, X} when X == unknown; X == pre_init; X == ok ; X == repairing;
                     X == repair_overload; X == disk_error ->
            X;
        does_not_exist ->
            unknown;
        _X ->
            ?DBG_CHAINx({external_to_internal_status, _B, Status}),
            ?E_INFO("external_to_internal_status: ~p status ~p, mapping to unknown\n", [_B, Status]),
            unknown
    end.

-spec diff_brickstatus([{brick_server:brick(), internal_status()}], [{brick_server:brick(), internal_status()}]) -> [{brick_server:brick(), {internal_status(), internal_status()}}].
diff_brickstatus(L1, L2) ->
    lists:foldl(
      fun({{B, Status}, {B, Status}}, Acc) ->
              Acc;
         ({{B, StatusX}, {B, StatusY}}, Acc) ->
              [{B, {StatusX, StatusY}}|Acc]
      end, [], lists:zip(lists:sort(L1), lists:sort(L2))).


%% Do not call brick_sb:report_chain_status() here, our caller
%% will do it for us.
-spec process_brickstatus_diffs(atom(), [{brick_server:brick(), {internal_status(), internal_status()}}] | arg_unused, brickstatus_list() | arg_unused, brickstatus_list(), #state{}) -> {atom(), #state{}}.
process_brickstatus_diffs(StateName, [] = _DiffList, _LastBricksStatus,
                          _NewBricksStatus, S) ->
    %% There are some conditions where we call ourselves recursively,
    %% but the diff list is now empty.
    {StateName, S};
%-
process_brickstatus_diffs(StateName, _DiffList, _LastBricksStatus,
                          NewBricksStatus, S)
  when StateName == unknown orelse StateName == stopped orelse
       StateName == unknown_timeout,
       is_record(S, state) ->
    AnyUnknown_p = lists:any(fun is_brick_unknown/1, NewBricksStatus),
    AllOK_p = lists:all(fun is_brick_ok/1, NewBricksStatus),
    AnyOK_p = lists:any(fun is_brick_ok/1, NewBricksStatus),
    AllPreInit_p = lists:all(fun is_brick_pre_init/1, NewBricksStatus),
    OK_Bricks = [B || {B, _} <- lists:filter(fun is_brick_ok/1,
                                             NewBricksStatus)],
    %%
    BestBrick = calculate_best_first_brick(S),
    BestStatus = case lists:keysearch(BestBrick, 1, NewBricksStatus) of
                     {value, {BestBrick, Val}} -> Val;
                     _                         -> unknown
                 end,
    BestOK_p = (BestStatus == ok),
    BestPre_Init_p = (BestStatus == pre_init),

    %% NOTE: If we're going to enter degraded state from here, then we will
    %% always return NewBricksStatus so that that up-to-date status list is
    %% immediately available in S#state.last_bricks_status.  We'll need
    %% up-to-date info for making which-brick-is-next decisions, and
    %% multiple bricks may have changed state at the same time.  However,
    %% we also need to use substitute_prop() because we *know* that the
    %% first brick we started is now in ok state.

    if AnyUnknown_p == false
       orelse
       (BestOK_p orelse BestPre_Init_p) ->
            %% OK, we've heard reports from all bricks.  Now what?
            %% Or, we haven't heard reports from all bricks (some are still
            %% not responding), but we know that the best brick (the one we
            %% care most about) is either pre_init or ok.
            if AllOK_p == true ->
                    %% Just in case we crashed in the middle of something,
                    %% and current roles are incorrect, set all roles again.
                    {ok, GH} = brick_admin:get_gh_by_chainname(S#state.chain),
                    if GH#g_hash_r.migrating_p == false ->
                            ?E_INFO(
                              "Chain ~w: New chain is healthy, migration "
                              "is NOT in effect, using chain order ~w\n",
                              [S#state.chain, S#state.bricks]),
                            ok = set_all_chain_roles_reorder(
                                   S#state.chain, S#state.bricks,
                                   S#state.bricks, S),
                            {healthy, S#state{last_bricks_status =
                                              NewBricksStatus,
                                              chain_now = S#state.bricks,
                                              repairing_brick = undefined}};
                       GH#g_hash_r.migrating_p == true ->
                            NewChain = if S#state.chain_now == undefined
                                          orelse
                                          S#state.chain_now == [] ->
                                               S#state.bricks;
                                          is_list(S#state.chain_now) ->
                                               S#state.chain_now
                                       end,
                            ?E_INFO(
                              "Chain ~w: New chain is healthy, migration "
                              "IS in effect, using order ~w\n",
                              [S#state.chain, NewChain]),
                            ok = set_all_chain_roles(NewChain, S),
                            {healthy, S#state{last_bricks_status =
                                              NewBricksStatus,
                                              chain_now = NewChain,
                                              repairing_brick = undefined}}
                    end;
               AnyOK_p ->
                    if BestOK_p, S#state.chainlen == 1 ->
                            %% QQQ TODO: This clause is redundant now, right?
                            {healthy,
                             S#state{last_bricks_status = NewBricksStatus,
                                     chain_now = S#state.bricks,
                                     repairing_brick = undefined}};
                       true ->
                            process_brickstatus_some_ok(
                              StateName, NewBricksStatus, OK_Bricks, S)
                    end;
               AllPreInit_p andalso S#state.chainlen == 1 ->
                    ?E_INFO("Chain ~w: adding ~w as standalone\n",
                                          [S#state.chain, BestBrick]),
                    ok = go_start_1chain_sync(BestBrick),
                    %% Remain in unknown/stopped state.  We will
                    %% transition when we find out that the repair
                    %% state has switched to ok.
                    {StateName, S#state{last_bricks_status = NewBricksStatus}};
               BestPre_Init_p ->
                    %% This is almost exactly the same as previous
                    %% case (pre_init and chain len == 1), but our
                    %% new state name will be different.
                    ?E_INFO("Chain ~w: adding best brick ~w as temporary head of the chain\n",
                                          [S#state.chain, BestBrick]),
                    ok = go_start_1chain_sync(BestBrick),
                    NBS = substitute_prop(NewBricksStatus, BestBrick, ok),
                    gen_fsm:send_event(self(), {evt_degraded, 1, BestBrick}),
                    {degraded, S#state{last_bricks_status = NBS,
                                       chain_now = [BestBrick],
                                       repairing_brick = undefined}};
               true ->
                    %% TODO: Nothing we can do here but wait?
                    {StateName, S#state{last_bricks_status = NewBricksStatus}}
            end;
       true ->
            %% We'll wait until we hear about other bricks, or if we timeout.
            {StateName, S#state{last_bricks_status = NewBricksStatus}}
    end;
%-
process_brickstatus_diffs(healthy = _StateName, DiffList, LastBricksStatus,
                          _NewBricksStatus, S)
  when is_record(S, state) ->
    %% TODO: Well, any change of status when we're healthy is not good.
    %% For now, just transition to unknown, and let unknown deal with it.
    %% To avoid icky races, we'll take just the 1st item in the DiffList,
    %% and apply it.
    DownBricks = lists:map(fun({Brick, _}) -> Brick end, DiffList),
    DownBricksNewStatus = lists:map(fun({Brick, {ok, _NewStatus}}) ->
                                            %% See commentary below labelled
                                            %% "races suck".
                                            {Brick, unknown};
                                       ({Brick, {_, NewStatus}}) ->
                                            {Brick, NewStatus} end,
                                    DiffList),
    ?DBG_CHAINx({was_healthy, S#state.chain, DownBricks}),
    NBS = lists:foldl(
            fun({Brick, NewStatus}, NBS_acc) ->
                    substitute_prop(NBS_acc, Brick, NewStatus)
            end, LastBricksStatus, DownBricksNewStatus),
    NewChain = S#state.bricks -- DownBricks,
    RepairingBrick = S#state.repairing_brick,
    case {RepairingBrick, lists:member(RepairingBrick, DownBricks)} of
        {undefined, _} ->
            ok;
        {_, false} ->
            ?E_INFO("Chain ~p was healthy, bricks now down = ~p, repairing "
                    "brick = ~p\n", [S#state.chain, DownBricks,RepairingBrick]),
            {R_Br, R_Nd} = RepairingBrick,
            %% Force repair to start over again (in later poll cycle).
            brick_server:chain_set_my_repair_state(R_Br, R_Nd, pre_init);
        _ ->
            ok
    end,
    if length(NewChain) == 0 ->
            {stopped, S#state{last_bricks_status = NBS, chain_now = []}};
       true ->
            %% TODO: If multiple bricks have failed, then our attempt
            %% to reconfig the chain will fail (timeout).
            ok = set_all_chain_roles(NewChain, S),
            gen_fsm:send_event(self(), {evt_degraded, -1, DownBricks}),
            {degraded, S#state{last_bricks_status = NBS,
                               chain_now = NewChain}}
    end;
%-
process_brickstatus_diffs(degraded = StateName, DiffList, LastBricksStatus,
                          NewBricksStatus, S)
  when is_record(S, state) ->

    {Brick, {OldStatus, NewStatus0}} = hd(DiffList),
    {BrName, NdName} = Brick,
    %% TODO: I don't know why, but sometimes our query to the scoreboard
    %% gives us out-of-date status.  I haven't been able to figure it out.
    %% Perhaps the scoreboard proc is blocking while trying to do something
    %% else?  Or the real status update's delivery to the scoreboard is
    %% delayed?
    %% But we can double-check, with the brick itself, if this backward
    %% status is real or imaginary.
    %% TODO: Copy this fix to the healthy state clause?
    %% TODO: Or just fix this for real.
    %% (
    %%  2008-01-15: Answer: it's plain and simple, honest races.
    %%  TODO: Fix it for real.
    %% -Hibari)

    NewStatus =
        case backward_status_but_alive_p(OldStatus, NewStatus0) of
            true ->
                Status = (catch brick_server:chain_get_my_repair_state(
                              BrName, NdName)),
                St = external_to_internal_status(Brick, {ok, Status}),
                ?E_INFO("Status adjustment: Brick ~w: ~w -> ~w -> ~w\n",
                        [Brick, OldStatus, NewStatus0, St]),
                if St == OldStatus ->
                        %% TODO: So, we've had a ficticious (I hope!)  report
                        %% of state X -> something worse, but the brick is
                        %% really still at state X.  So, patch up both status
                        %% lists, in case someone else cares.
                        LBS2 = substitute_prop(LastBricksStatus, Brick, St),
                        NBS2 = substitute_prop(NewBricksStatus, Brick, St),
                        process_brickstatus_diffs(StateName, tl(DiffList),
                                                  LBS2, NBS2, S);
                   true ->
                        St                      % Use really-current status
                end;
            false ->
                if OldStatus == ok, NewStatus0 /= ok ->
                        %% label: "races suck"
                        %% If this brick used to be ok, then we don't
                        %% *really* know what its status is.  We're liable to
                        %% races with the brick pinger and scoreboard
                        %% updates, which sucks.  If we say that the new
                        %% status is unknown, then a subsequent poll of the
                        %% scoreboard can tell us something /= unknown, and
                        %% that diff will trigger us to take action at that
                        %% time.
                        %% TODO: create a better fix for these races?
                        unknown;
                   true ->
                        NewStatus0          % Keep the original status
                end
        end,
    NBS = substitute_prop(LastBricksStatus, Brick, NewStatus),
    LastBrickInChainNow = lists:last(S#state.chain_now),
    RepairingBrick = S#state.repairing_brick,
    if Brick == LastBrickInChainNow, RepairingBrick /= undefined ->
            ?E_INFO("Chain ~p was degraded, brick down = ~p, repairing "
                    "brick = ~p\n", [S#state.chain, Brick, RepairingBrick]),
            {R_Br, R_Nd} = RepairingBrick,
            %% Force repair to start over again (in later poll cycle).
            %% Use catch this time: RepairingBrick may also be dead.
            catch brick_server:chain_set_my_repair_state(R_Br, R_Nd, pre_init);
       true ->
            ok
    end,
    BrickIsInChainNow_p = lists:member(Brick, S#state.chain_now),
    if Brick == S#state.repairing_brick, NewStatus == repairing ->
            ?DBG_CHAINx({qwer, 4}),
            {StateName, S#state{last_bricks_status = NBS}};
       Brick == S#state.repairing_brick, NewStatus == ok ->
            ?DBG_CHAINx({qwer, 5}),
            ?E_INFO("Chain ~w: added ~w to end of chain, repair finished\n",
                    [S#state.chain, Brick]),
            %% Avoid a race condition where we tell the Admin Server
            %% that the chain has Brick at the tail, but Brick's role
            %% doesn't have the correct official_tail setting.
            NewChain = S#state.chain_now ++ [Brick],
            ok = set_all_chain_roles(NewChain, S),
            gen_fsm:send_event(self(), {evt_degraded, 2, Brick}),
            {StateName, S#state{last_bricks_status = NBS,
                                chain_now = NewChain,
                                repairing_brick = undefined}};
       Brick == S#state.repairing_brick, NewStatus /= ok ->
            %% The brick that we're trying to start & repair has failed.
            %% Bummer.
            brick_sb:report_chain_status(S#state.chain, degraded,
                                         [{repairing_brick_failed,
                                           {Brick, NewStatus}},
                                          {chain_now, S#state.chain_now}]),
            ?E_INFO("Chain ~w: repairing_brick ~w failed, status = ~w\n",
                    [S#state.chain, S#state.repairing_brick, NewStatus]),
            gen_fsm:send_event(self(), {evt_degraded, 0, Brick}),
            ?DBG_CHAINx({qwer, m1}),
            ok = set_all_chain_roles(S#state.chain_now, S),
            {StateName, S#state{last_bricks_status = NBS,
                                repairing_brick = undefined}};
       BrickIsInChainNow_p ->
            %% Someone in the current chain changed state, can't be good.
            if NewStatus /= ok ->
                    if length(S#state.chain_now) == 1 ->
              %% TODO: We probably have the S#state.started brick doing
              %% something ... what do we do here?  There are two (?) possible
              %% states:
              %% 1. The started brick is still in repair, but its upstream has
              %% failed, so repair will fail.  Hopefully, our logic is correct
              %% so that the brick that just failed is the best brick, so we'll
              %% block waiting for that brick to return to pre_init.
              %% 2. The started brick finished repair.  We should then get an
              %% OK event from the brick, and when we're in the stopped state,
              %% that should get us back to degraded, right?
                            ?DBG_CHAINx({qwer, a1}),
                            {stopped, S#state{last_bricks_status = NBS,
                                              chain_now = []}};
                       true ->
                            ?DBG_CHAINx({qwer, b1}),
                            NewChain = S#state.chain_now  -- [Brick],
                            ok = set_all_chain_roles(NewChain, S),
                            gen_fsm:send_event(self(),
                                               {evt_degraded, -1, [Brick]}),
                            {degraded, S#state{last_bricks_status = NBS,
                                               chain_now = NewChain}}
                    end;
               true ->
                    exit({process_brickstatus_diffs, todo_should_never_happen, hd(DiffList)})
            end;
       S#state.repairing_brick == undefined, NewStatus == pre_init ->
            %% We now have the next brick to re-join the chain.
            ?E_INFO("Chain ~w: current = ~w, adding ~w to end of chain, starting repair\n",
                                  [S#state.chain, S#state.chain_now, Brick]),
            ?DBG_CHAINx({qqq_adding_to_end_of_chain,Brick,NBS}),
            ok = add_repair_brick_to_end(Brick, S),
            ?DBG_CHAINx({qwer, 2}),
            brick_sb:report_chain_status(S#state.chain, degraded,
                                             [{repairing_brick, Brick},
                                              {chain_now, S#state.chain_now}]),
            gen_fsm:send_event(self(), {evt_degraded, repairing_brick, Brick}),
            {StateName, S#state{last_bricks_status = NBS,
                                repairing_brick = Brick}};
       Brick /= S#state.repairing_brick,
       OldStatus == unknown, NewStatus == ok ->
            %% BZ 27591.  The scenario there:
            %% 1. Brick A starts repairing brick B.
            %% 2. Brick B goes comatose, e.g. bcb_delete_remaining_keys
            %%    that takes many minutes to finish.
            %% 3. Chmon says repairing brick has failed, but chmon doesn't
            %%    kill bricks, only the pinger does (so far).  We believe
            %%    that B's last status is now 'unknown'.
            %% 4. Chmon may or may not do other things with other bricks in
            %%    the chain.
            %% 5. Brick B starts responding to the outside world again,
            %%    and it says its health is 'ok'.
            %% 6. If we let the last clause below handle this state
            %%    transition, we will ignore it forever.
            %%
            %% Fix: specifically force the wayward brick back to pre_init
            %% state.
            error_logger:info_msg("Chain monitor: ~p: force ~p from ok -> pre_init\n",
                                  [S#state.chain, Brick]),
            ok = brick_server:chain_set_my_repair_state(
                   BrName, NdName, force_ok_to_pre_init),
            PingerName = brick_bp:make_pinger_registered_name(BrName),
            brick_sb:report_brick_status(BrName, NdName, pre_init,
                                         [chain_monitor_adjustment]),
            error_logger:info_msg("Chain monitor: ~p: restart ~p\n",
                                  [S#state.chain, PingerName]),
            exit(whereis(PingerName), kill),
            PreInitNBS = substitute_prop(LastBricksStatus, Brick, unknown2),
%           PreInitNBS = LastBricksStatus, % Which is the correct one?
            ?DBG_CHAINx({qwer, 6}),
            {StateName, S#state{last_bricks_status = PreInitNBS}};
       Brick /= S#state.repairing_brick ->
            %% Someone else changed state.  We want to ignore this event,
            %% but if there are other diffs, we should examine them first.
            %% TODO: The above comment isn't 100% true?  Investigate this:
            %% 1. Stop a node that will host a brick, B.
            %% 2. Create a new table with a chain of length 2, and one of the
            %%    brick members on host B.
            %% 3. Start the node B.
            %% 4. We end up here, where S#state.repairing_brick,
            %%    length(DiffList) == 1.
            %% Short-term fix: kill one of the two bricks, to trigger recovery.
            if length(DiffList) > 1 ->
                    ?DBG_CHAINx({qwer, a3, Brick, LastBricksStatus, NewStatus}),
                    process_brickstatus_diffs(StateName, tl(DiffList),
                                              LastBricksStatus,
                                              NewBricksStatus, S);
               true ->
                    ?DBG_CHAINx({qwer, b3, Brick, LastBricksStatus, NewStatus}),
                    {StateName, S#state{last_bricks_status = LastBricksStatus}}
               end
    end;
%-
process_brickstatus_diffs(_StateName, _DiffList, _LastBricksStatus,
                          _NewBricksStatus, S)
  when is_record(S, state) ->
    ?E_WARNING("TODO: process_brickstatus_diffs(StateName = ~p\n_DiffList = ~p\n_LastBricksStatus = ~p\n_NewBricksStatus = ~p\n",
                             [_StateName, _DiffList, _LastBricksStatus, _NewBricksStatus]),
    qqqqqqqqqqqqqqq_unfinished2.

-spec process_brickstatus_some_ok(atom(), brickstatus_list(), brick_list(), #state{}) -> {atom(), #state{}}.
process_brickstatus_some_ok(StateName, NewBricksStatus, OK_Bricks, S) ->
    case (catch figure_out_current_chain_op_status(OK_Bricks)) of
        {ok, ChainList, RepairingBrick} ->
            ?E_INFO("Chain ~w: current operating state (ok "
                                  "bricks) is ~w with repairing brick ~w\n",
                                  [S#state.chain, ChainList, RepairingBrick]),
            if length(NewBricksStatus) == length(ChainList),
               RepairingBrick == undefined ->
                    NBS = [{Br, ok} || Br <- ChainList],
                    %% Mutual recursion to re-solve this problem.
                    process_brickstatus_diffs(StateName, arg_unused,
                                              arg_unused, NBS, S);
               true ->
                    %%process_brickstatus_some_ok_fallback(
                    %%  StateName, NewBricksStatus, OK_Bricks, S)
                    gen_fsm:send_event(self(), {evt_degraded, unused, unused}),
                    NonChainBricks = S#state.bricks -- ChainList,
                    %% Using 'unknown' status will almost certainly trigger a
                    %% state diff on our next iteration.
                    NBS =
                        [{Br, ok}      || Br <- ChainList] ++
                        [{Br, unknown} || Br <- NonChainBricks],
                    {degraded, S#state{last_bricks_status = NBS,
                                       chain_now = ChainList,
                                       repairing_brick = RepairingBrick}}
            end;
        _X ->
            process_brickstatus_some_ok_fallback(
              StateName, NewBricksStatus, OK_Bricks, S)
    end.

%% So, we're going to do the dumb, simple thing:
%%
%% 1. Set the first OK brick to standalone mode.
%% 2. Stop all others.
%% 3. Someone else can add the others to the chain as the bricks return to
%%    pre_init state.
-spec process_brickstatus_some_ok_fallback(atom(), brickstatus_list(), brick_list(), #state{}) -> {degraded, #state{}}.
process_brickstatus_some_ok_fallback(_StateName, NewBricksStatus,
                                     OK_Bricks, S) ->
    B1 = whittle_chain_to_1(OK_Bricks, S),
    ?E_INFO("Chain ~w: adding ok status brick ~w as temporary "
                          "head of the chain\n", [S#state.chain, B1]),
    ?E_INFO("Chain ~w: last_bricks_status = ~p\n",
                          [S#state.chain, S#state.last_bricks_status]),
    ?E_INFO("Chain ~w: NewBricksStatus = ~p\n",
                          [S#state.chain, NewBricksStatus]),
    NBS = substitute_prop(NewBricksStatus, B1, ok),
    gen_fsm:send_event(self(), {evt_degraded, 1, B1}),
    {degraded, S#state{last_bricks_status = NBS,
                       chain_now = [B1],
                       repairing_brick = undefined}}.

%% @spec (list({atom(), atom()})) ->
%%       {ok, list({atom(), atom()}), undefined | {atom(), atom()}} |
%%       exit|throw|term()
%% @doc Calculate the actual operating state of a set of bricks that
%% were in 'ok' state before calling this function.
%%
%% We do everything in a brute-force, positive-case-only style.  If something
%% doesn't smell exactly right, this function won't return {ok, ...}, period.
%%
%% We expect that the caller is catch'ing errors or, if not, is not
%% afraid to crash.

-spec figure_out_current_chain_op_status(brick_list()) -> {ok, brick_list(), brick_server:brick() | undefined}.
figure_out_current_chain_op_status(OK_Bricks) ->
    OpCs = lists:map(
             fun({Br, Nd} = B) ->
                     {ok, Ps} = brick_server:status(Br, Nd),
                     Cs = proplists:get_value(chain, Ps),
                     Role = proplists:get_value(chain_role, Cs),
                     UpStream = proplists:get_value(chain_upstream, Cs),
                     DnStream = proplists:get_value(chain_downstream, Cs),
                     OffTail  = proplists:get_value(chain_official_tail, Cs),
                     ok = proplists:get_value(chain_my_repair_state, Cs),
                     #opconf_r{me = B, role = Role, upstream = UpStream,
                               downstream = DnStream, off_tail_p = OffTail}
             end, OK_Bricks),
    ?DBG_CHAIN("CCC: OpCs = ~p\n", [OpCs]),
    {BrickList, RepairingBrick} = stitch_op_state(OpCs),
    {ok, BrickList, RepairingBrick}.

%% @spec (list(opconf_r())) ->
%%       {list({atom(), atom()}), undefined | {atom(), atom()}}
%% @doc Given an unsorted list of opconf_r records, sort them into a
%% well-formed chain list and also the repairing brick past the end
%% of the chain (undefined if no such repairing brick).
%%
%% We expect that the caller is catch'ing errors or, if not, is not
%% afraid to crash.
%%
%% The multi-brick case is a pain because the brute-force code of my
%% implementation causes ETS table leaks (due to not cleaning up digraph
%% state correctly).  So, use a separate proc to do the calculation so any
%% table leaks will automatically be cleaned up.

-spec stitch_op_state(list(#opconf_r{})) -> {brick_list(), brick_server:brick() | undefined}.
stitch_op_state([OpC]) ->
    %% Sanity checking for a standalone brick.
    standalone = OpC#opconf_r.role,
    undefined = OpC#opconf_r.upstream,
    undefined = OpC#opconf_r.downstream,
    true = OpC#opconf_r.off_tail_p,
    {[OpC#opconf_r.me], undefined};
stitch_op_state(OpCs) ->
    Me = self(),
    {Child, MRef} = erlang:spawn_monitor(fun() ->
                                                 stitch_op_state_2(OpCs, Me)
                                         end),
    %% !@#$!, monitors are a pain/verbose.
    Res = receive
              {Pid, Reply} when Pid == Child -> % !@#$! compiler warning
                  Reply;
              {'DOWN', X, _, _, _} when X == MRef -> % !@#$! compiler warning
                  exit(some_kind_of_error)
          end,
    erlang:demonitor(MRef),
    receive
        {'DOWN', MRef, _, _, _} ->
            ok
    after 0 ->
            ok
    end,
    Res.

-spec stitch_op_state_2(list(#opconf_r{}), pid()) -> no_return().
stitch_op_state_2(OpCs, ParentPid) ->
    Reply = stitch_op_state_3(OpCs),
    ParentPid ! {self(), Reply},
    exit(normal).

-spec stitch_op_state_3(list(#opconf_r{})) -> {brick_list(), brick_server:brick() | undefined}.
stitch_op_state_3(OpCs) ->
    %% Small sanity checks here.
    true = (length(OpCs) == length(list_uniq(lists:sort(OpCs)))),
    false = lists:any(fun(O) -> O#opconf_r.role /= chain_member end, OpCs),

    DownG = digraph:new(),
    UpG = digraph:new(),
    lists:map(
      fun(OpC) ->
              if OpC#opconf_r.downstream /= undefined ->
                      digraph:add_vertex(DownG, OpC#opconf_r.me),
                      digraph:add_vertex(DownG, OpC#opconf_r.downstream),
                      digraph:add_edge(DownG, OpC#opconf_r.me,
                                       OpC#opconf_r.downstream);
                 true ->
                      ok
              end,
              if OpC#opconf_r.upstream /= undefined ->
                      digraph:add_vertex(UpG, OpC#opconf_r.me),
                      digraph:add_vertex(UpG, OpC#opconf_r.upstream),
                      digraph:add_edge(UpG, OpC#opconf_r.me,
                                       OpC#opconf_r.upstream);
                 true ->
                      ok
              end
      end, OpCs),
    ?DBG_CHAIN("topsort down   = ~p\n", [digraph_utils:topsort(DownG)]),
    ?DBG_CHAIN("rev topsort up = ~p\n", [catch lists:reverse(digraph_utils:topsort(UpG))]),
    Tdown = digraph_utils:topsort(DownG),
    Tup = digraph_utils:topsort(UpG),
    Tup_rev = if Tup == false   -> false;
                 Tdown == false -> false;
                 true           -> lists:reverse(digraph_utils:topsort(UpG))
              end,
    digraph:delete(DownG),
    digraph:delete(UpG),
    %% Coding for expected cases only, all others will fail with
    %% badmatch or if_clause or something.
    %%
    %% If Tdown == Tup_rev, then the total sequence of downstream links is
    %% exactly the same as the reverse of the sequence of upstream links.  If
    %% either sequence is not exactly equivalent to a single linked list
    %% (e.g. cyclic, tree'ish, multiple disjoint graphs, whatever), then the
    %% digraph_utils:topsort/1 function will fail (return 'false').  We also
    %% check for exactly 1 official tail and that the official tail is either
    %% at the end (i.e. no repairing brick) or 2nd-from-the-end (i.e. a
    %% repairing brick).
    if Tdown /= false, Tdown == Tup_rev ->
            Sorted = [OpC || X <- Tdown, OpC <- OpCs, OpC#opconf_r.me == X],
            [OffTail] = lists:filter(fun(O) ->
                                             O#opconf_r.off_tail_p == true
                                     end, Sorted),
            case lists:dropwhile(fun(O) -> O /= OffTail end, Sorted) of
                [OT] when OT == OffTail ->
                    {[O#opconf_r.me || O <- Sorted], undefined};
                [OT, Repairing] when OT == OffTail ->
                    OffChain = lists:sublist(Sorted, length(Sorted) - 1),
                    {[O#opconf_r.me || O <- OffChain], Repairing#opconf_r.me}
            end
    end.

-spec substitute_prop(brickstatus_list(), brick_server:brick(), internal_status() | unknown2) -> brickstatus_list().
substitute_prop(PropList, Key, NewVal) ->
    lists:keyreplace(Key, 1, PropList, {Key, NewVal}).

-spec is_brick_x(internal_status()) -> fun((brick_server:brick()) -> boolean()).
is_brick_x(Status) ->
    fun({_B, S}) when S == Status -> true;
       (_)                        -> false
    end.

-spec is_brick_unknown(brick_server:brick()) -> boolean().
is_brick_unknown(B) -> (is_brick_x(unknown))(B).

-spec is_brick_pre_init(brick_server:brick()) -> boolean().
is_brick_pre_init(B) -> (is_brick_x(pre_init))(B).

-spec is_brick_ok(brick_server:brick()) -> boolean().
is_brick_ok(B) -> (is_brick_x(ok))(B).

-spec go_start_1chain_sync(brick_server:brick()) -> ok.
go_start_1chain_sync({Brick, Node}) ->
    ?DBG_CHAINx({go_start_1chain_sync, {Brick, Node}}),
    ok = do_role_ok(chain_role_standalone, [Brick, Node]),
    standalone = brick_server:chain_get_role(Brick, Node),
    ok = brick_server:chain_set_my_repair_state(Brick, Node, ok),
    %%
    %% To avoid an annoying race condition with communicating via the
    %% scoreboard, we'll tell the scoreboard now that Brick's status
    %% is ok.  That fixes a race condition where, on our next
    %% check_status iteration, we read stale status from the
    %% scorebard.
    brick_sb:report_brick_status(Brick, Node, ok, []),
    timer:sleep(500),                           % TODO: hrm, good idea?
                                                % Force report to sb as *sync*?
    %% Sanity check.
    ok = brick_server:chain_get_my_repair_state(Brick, Node),
    ?DBG_CHAINx({go_start_1chain_sync, done, {Brick, Node}}),
    ok.

%% TODO: If AnyOK_p is actually true, then why did we bother waiting so
%% long to put the chain into degraded mode?  We couldn't put the OK
%% brick(s) into service right away.
-spec do_unknown_timeout_decision(#state{}) -> {next_state, degraded|stopped, #state{}}.
do_unknown_timeout_decision(S) when is_record(S, state) ->
    AnyOK_p = lists:any(fun is_brick_ok/1, S#state.last_bricks_status),
    BestBrick = calculate_best_first_brick(S),
    BestBrickIsPreInit_p =
        lists:keymember(BestBrick, 1,
                        lists:filter(fun is_brick_pre_init/1, S#state.last_bricks_status)),
    if AnyOK_p == true ->
            OK_Bricks = [B || {B, _} <-
                                  lists:filter(fun is_brick_ok/1,
                                               S#state.last_bricks_status)],
            B1 = whittle_chain_to_1(OK_Bricks, S),
            brick_sb:report_chain_status(S#state.chain, degraded,
                                             [{hrm, B1}, ?EMPTY_CHAIN_NOW]),
            gen_fsm:send_event(self(), {evt_degraded, 1, B1}),
            {next_state, degraded, S#state{chain_now = [B1]}};
       BestBrickIsPreInit_p ->
            %% A brick in the chain has been down forever (so we can't
            %% make up our mind & then timeout), but the best brick is
            %% not OK either, but it *is* pre_init.  Because it's the
            %% best brick, we want to start that one.  Then the change
            %% of its status out of pre_init will get our attention on
            %% the next iteration.
            ok = go_start_1chain_sync(BestBrick),
            {next_state, stopped, S};
       AnyOK_p == false ->
            [BestStatus] = [BS || {BB, BS} <- S#state.last_bricks_status,
                                  BB == BestBrick],
            ?E_INFO("unknown_timeout_decision: ~p: "
                                  "best brick is ~p but its status is ~p\n",
                                  [S#state.chain, BestBrick, BestStatus]),
            brick_sb:report_chain_status(S#state.chain, stopped,
                                         [?EMPTY_CHAIN_NOW,
                                          {best_brick_is, BestBrick},
                                          {best_brick_status_is, BestStatus}]),
            {next_state, stopped, S}
    end.

%% @doc Whittle down the chain to only a single brick.
%%
%% Avoid this situation:
%% 1. This func sets the head brick B_h from head -> standalone.
%% 2. Client A successfully deletes key K.  The delete op is done on B_h.
%% 3. Client B tries to read K; its global hash says to use B_t.
%%    B_t is still active, still think's it's the official tail, so
%%    the read succeeds.
%% 4. This func shuts down B_t and all other/middle bricks.
%%
%% So, we first shut down all other bricks, starting with (we hope, but isn't
%% critical) the tail.

-spec whittle_chain_to_1(brick_list(), #state{}) -> brick_server:brick().
whittle_chain_to_1(OK_Bricks, S) ->
    {Brick, Node} = B1 = hd(OK_Bricks),
    Other_OK_Bricks = tl(OK_Bricks),
    [_ = (catch brick_admin:stop_brick_only(B)) || B <- lists:reverse(Other_OK_Bricks)],
    ok = set_all_chain_roles([{Brick, Node}], S),
    B1.

do_role_ok(Func, Args) ->
    case apply(brick_server, Func, Args) of
        {error,already_standalone,{}} -> ok;
        Else                          -> Else
    end.

%% Side-effect: set_all_chain_roles() will turn off read-only mode, if
%% it's on for any brick before calling this function.
%%
%% set_all_chain_roles/1 is used when we do not know the complete state of
%% each brick in the chain.  set_all_chain_roles/2 is used when we know
%% 100% (as well as we can known in an async network) that each brick is
%% available and in the ok state that we believe it is.

-spec set_all_chain_roles(brick_list()) -> ok.
set_all_chain_roles(BrickList) ->
    set_all_chain_roles(BrickList, #state{chain = no_chain_name_available}).

-spec set_all_chain_roles(brick_list(), #state{}) -> ok.
set_all_chain_roles(BrickList, S) ->
    set_all_chain_roles(BrickList, skip, S).

-spec set_all_chain_roles(brick_list(), brick_list() | skip, #state{}) -> ok.
set_all_chain_roles(BrickList, OldBrickList, S) ->
    ?E_INFO("set_all_chain_roles: ~p: top\n", [S#state.chain]),
    ?DBG_CHAINx({set_all_chain_roles, S#state.chain, read_only, 1}),
    ok = set_all_read_only(BrickList, true),
    ?E_INFO("set_all_chain_roles: ~p: brick read-only 1\n", [S#state.chain]),
    if is_list(OldBrickList), length(OldBrickList) > 0 ->
            %% In this case, we're not certain if all log replay has
            %% propagated down the entire chain, because we're not
            %% 100% certain what the configuration of the chain *is*
            %% (what is the head?).  We could calculate what the
            %% current chain config is (sync_down_the_chain() only
            %% works if we're 100% certain what the head of the chain
            %% is!).  Instead, we'll poll each of the bricks in the
            %% old chain to see that the downstream serial ack # is
            %% current.
            ?DBG_CHAINx({set_all_chain_roles, S#state.chain, read_only, 2}),
            ok = set_all_read_only(OldBrickList, true),
            ?E_INFO("set_all_chain_roles: ~p: brick read-only 2\n", [S#state.chain]),
            lists:map(
              fun({Br, Nd}) ->
                      %%?E_WARNING("ZZZ: ~p: call poll_for_full_sync(~p, ~p) for 2sec\n", [S#state.chain, Br, Nd]),
                      %%?E_WARNING("ZZZ: ~p: OldBrickList ~p\n", [S#state.chain, OldBrickList]),
                      case poll_for_full_sync(Br, Nd, 15*1000) of
                          ok ->
                              ?E_INFO(
                                "Downstream serial ack for {~w, ~w} is good\n",
                                [Br, Nd]);
                          Res ->
                              ?E_ERROR(
                                "Downstream serial ack for {~w, ~w} was ~w\n",
                                [Br, Nd, Res]),
                              exit({poll_for_full_sync, Br, Nd, Res})
                      end
              end, OldBrickList);
       true ->
            ok
    end,
    ?DBG_CHAINx({set_all_chain_roles, S#state.chain, roles2}),
    ok = set_all_chain_roles2(BrickList, S),

    %% Take care of the case where a middle brick died.
    ?DBG_CHAINx({set_all_chain_roles, S#state.chain, bricklist, BrickList}),
    if BrickList == [] ->
            nothing_to_do;
       true ->
            {NewHead, NewNode} = hd(BrickList),
            {ChainDownSerial, ChainDownAcked} =
                brick_server:chain_get_downstream_serials(NewHead, NewNode),
            if ChainDownSerial /= ChainDownAcked ->
                    ?E_INFO("New head {~w,~w}: ChainDownSerial ~w /= ChainDownAcked ~w, reflushing log downstream\n", [NewHead, NewNode, ChainDownSerial, ChainDownAcked]),
                    ?DBG_CHAINx({set_all_chain_roles, S#state.chain, flush_log, {NewHead, NewNode}}),
                    ok = brick_server:chain_flush_log_downstream(NewHead,
                                                                 NewNode),
                    %%?E_WARNING("ZZZ: ~p: call poll_for_full_sync(~p, ~p) for 5sec\n", [S#state.chain, NewHead, NewNode]),
                    _ = brick_server:sync_down_the_chain(NewHead, NewNode, []),
                    Res = poll_for_full_sync(NewHead, NewNode, 5*1000),
                    ?E_INFO("New head {~w,~w}: flush was ~w\n",
                                           [NewHead, NewNode, Res]),
                    Res = ok;                   % sanity
               true ->
                    ?DBG_CHAINx({set_all_chain_roles, S#state.chain, flush_log, none}),
                    ok
            end
    end,

    ?DBG_CHAINx({set_all_chain_roles, S#state.chain, read_only, false}),
    ok = set_all_read_only(BrickList, false).

-spec set_all_chain_roles2(brick_list(), #state{}) -> ok.
set_all_chain_roles2([], S) ->
    ?E_WARNING("set_all_chain_roles2: empty list for ~p\n",
                             [S#state.chain]),
    ok;
set_all_chain_roles2([{Name, Node}], _S) ->
    ok = do_role_ok(chain_role_standalone, [Name, Node]),
    ok;
set_all_chain_roles2([{Head, NodeHead}, {Tail, NodeTail}] = _BrickList, _S) ->
    ok = do_role_ok(chain_role_tail, [Tail, NodeTail, Head, NodeHead,
                                      [{official_tail, true}]]),
    ok = do_role_ok(chain_role_head, [Head, NodeHead, Tail, NodeTail,
                                      [{official_tail, false}]]),
    ok;
set_all_chain_roles2(BrickList, _S) ->
    [{Head, NodeHead}, {FirstMid, NodeFirstMid}|_] = BrickList,
    RevBrickList = lists:reverse(BrickList),
    [{Tail, NodeTail}, {LastMid, NodeLastMid}|_] = RevBrickList,
    NumBricks = length(BrickList),
    Triples = lists:map(
                fun(Pos) -> lists:sublist(BrickList, Pos - 1, 3) end,
                lists:seq(2, 1 + (NumBricks -2 ))),

    %% Define the final roles, starting at the tail of the chain.
    ok = do_role_ok(chain_role_tail, [Tail, NodeTail,
                                      LastMid, NodeLastMid,
                                      [{official_tail, true}]]),
    lists:foreach(
      fun([{Up, UpNode}, {Mid, MidNode}, {Down, DownNode}]) ->
              ok = do_role_ok(chain_role_middle,
                              [Mid, MidNode, Up, UpNode, Down, DownNode,
                               [{official_tail, false}]])
      end, lists:reverse(Triples)),
    ok = do_role_ok(chain_role_head, [Head, NodeHead,
                                      FirstMid, NodeFirstMid,
                                      [{official_tail, false}]]),
    ok.

%% @doc Set all chain roles, possibly re-ordering the chain in the process.
%%
%% This is !@#$! tricky, because in order to do this correctly, we need
%% an accurate global hash record from the admin server, and the admin
%% server gets its events *asynchronously* from the scoreboard.
%% Hrm, so to solve this problem, we:
%% <ul>
%% <li> Set all bricks in the chain to read-only mode, to avoid doing
%%      something bad. </li>
%% <li> Call set_all_chain_roles() with the <em>current chain order</em>.</li>
%% <li> Report the chain status as healthy. </li>
%% <li> Poll the admin server for the global hash record associated
%%      with our chain. </li>
%% <li> The admin server *will* spam the global hash for this chain when
%%      it hears that the status has changed to healthy. </li>
%% <li> There is a subtle race condition here that can catch read-only
%%      ops that are sent to the just-added brick at the tail, T_ja.
%%      That brick's role is tail, but it does not yet have official_tail
%%      status.  When the admin server spams the new GH, it's possible
%%      for clients to send read queries to T_ja before the chain is
%%      re-roled so that T_ja gets official_role status.  A read-only
%%      query @ T_ja will be accepted and processed (the global hash
%%      says that T_ja is the tail of the chain), but it isn't the
%%      official tail, so it will not send a reply to the read client. </li>
%% <li> Solution: Call set_all_chain_roles() with the current chain
%%                ordering first (step #2).</li>
%% <li> </li>
%% <li> </li>
%% <li> Finally, set all the chain roles using the desired chain order. </li>
%% </ul>

-spec set_all_chain_roles_reorder(brick_server:chain_name(), brick_list(), brick_list(), #state{}) -> ok.
set_all_chain_roles_reorder(ChainName, BrickList, OldBrickList, S) ->
    ?DBG_CHAINx({set_all_chain_roles_reorder, S#state.chain, read_only, true}),
    ok = set_all_read_only(BrickList, true),
    ?DBG_CHAINx({set_all_chain_roles_reorder, S#state.chain, set_roles1, OldBrickList}),
    ok = set_all_chain_roles(OldBrickList, S),
    Fpoll = fun(Acc) ->
                    {ok, GH} = brick_admin:get_gh_by_chainname(ChainName),
                    Cs = brick_hash:all_chains(GH, current) ++
                        brick_hash:all_chains(GH, new),
                    BL = proplists:get_value(ChainName, Cs),
                    ?DBG_CHAIN("CCC: chain ~w BL = ~p\n", [ChainName, BL]),
                    if BL == BrickList ->
                            {false, Acc};
                       Acc > 12*50 ->
                            exit({set_all_chain_roles_reorder, timeout,
                                  ChainName, BL, BrickList});
                       true ->
                            ?DBG_CHAIN("CCC p2", []),
                            timer:sleep(100),
                            {true, Acc + 1}
                    end
            end,
    ?DBG_CHAIN("\n\n\nCCC: We've got sync for ~w!\n\n", [ChainName]),

    ?DBG_CHAINx({set_all_chain_roles_reorder, S#state.chain, set_roles2, BrickList}),
    %% Remember: a side-effect is turning off read-only mode.
    ok = set_all_chain_roles(BrickList, OldBrickList, S),
    %% This status report will trigger a GH spamming using the new
    %% chain order, which is an OK thing now that the chain is
    %% *actually* using the new chain order.
    brick_sb:report_chain_status(ChainName, healthy, [{chain_now, BrickList}]),
    _ = gmt_loop:do_while(Fpoll, 0),
    ok.

-spec set_all_read_only(brick_list(), true | false) -> ok.
set_all_read_only(BrickList, Mode_p) when is_list(BrickList) ->
    [ok = brick_server:chain_set_read_only_mode(Brick, Node, Mode_p) ||
        {Brick, Node} <- BrickList],
    ok.

-spec add_repair_brick_to_end(brick_server:brick(), #state{}) -> ok.
add_repair_brick_to_end({NewTail, NewTailNode} = _NewTailBrick, S) ->
    ChainNow = S#state.chain_now,
    {NowLast1, NowLast1Node} = NowLast1Brick = lists:last(ChainNow),
    %%?DBG_CHAINx({brick_to_end, tail, NewTail, NewTailNode, NowLast1, NowLast1Node}),
    ok = brick_server:chain_role_tail(NewTail, NewTailNode,
                                          NowLast1, NowLast1Node,
                                          [{official_tail, false}]),

    %% During migration, the {plog_sweep, ...} tuples require that the
    %% receiver has a global hash set for itself.  Give it the current
    %% one before we have the upstream/repairing brick starts to send
    %% stuff to our downstream/repairee.
    %% If any client sends stuff to the tail, it will have the wrong
    %% role (tail + official_tail=false) to do anything harmful.
    %% We might not be migrating now, but we might be in the future.
    %% If that future happens, no one will tell NewTail about it,
    %% which means that NewTail won't have a valid #g_hash_r.migr_dict.
    %% So we'll create one.
    {ok, GH0} = brick_admin:get_gh_by_chainname(S#state.chain),
    GH = brick_hash:add_migr_dict_if_missing(GH0),
    ok = set_global_hash_wrapper(NewTail, NewTailNode, GH),

    HeadBrick = hd(S#state.chain_now),
    if HeadBrick == NowLast1Brick ->
            %%?DBG_CHAINx({brick_to_end, head, NowLast1, NowLast1Node, NewTail, NewTailNode}),
            ok = brick_server:chain_role_head(NowLast1, NowLast1Node,
                                                  NewTail, NewTailNode,
                                                  [{official_tail, true}]),
            %%?DBG_CHAINx({brick_to_end, repair, NowLast1, NowLast1Node}),
            ok = brick_server:chain_start_repair(NowLast1, NowLast1Node);
       true ->
            [{NowLast1, NowLast1Node}, {NowLast2, NowLast2Node}|Others] =
                lists:reverse(ChainNow),
            %%?DBG_CHAINx({brick_to_end, middle, NowLast1, NowLast1Node, NowLast2, NowLast2Node, NewTail, NewTailNode}),
            ok = brick_server:chain_role_middle(NowLast1, NowLast1Node,
                                                    NowLast2, NowLast2Node,
                                                    NewTail, NewTailNode,
                                                    [{official_tail, true}]),
            case Others of
                [] ->
                    %% NowLast2 -> NowLast1 -> NewTail
                    ok = brick_server:chain_role_head(NowLast2, NowLast2Node,
                                                      NowLast1, NowLast1Node,
                                                    [{official_tail, false}]);
                [{NowLast3, NowLast3Node}|_] ->
                    %% brickX -> NowLast3 -> NowLast2 -> NowLast1 -> NewTail
                    ok = brick_server:chain_role_middle(
                           NowLast2, NowLast2Node, % to-be middle brick
                           NowLast3, NowLast3Node, % to-be upstream
                           NowLast1, NowLast1Node, % to-be downstream
                           [{official_tail, false}])
                end,
            %% Sanity check.
            pre_init = brick_server:chain_get_ds_repair_state(NowLast1,
                                                                  NowLast1Node),
            %%?DBG_CHAINx({brick_to_end, repair, NowLast1, NowLast1Node}),
            ok = brick_server:chain_start_repair(NowLast1, NowLast1Node)
    end,
    ok.

-spec poll_for_full_sync(brick_bp:brick_name(), node(), integer()) -> ok | {poll_for_full_sync, timeout, integer(), integer()}.
poll_for_full_sync(Brick, Node, TimeLimit_0) ->
    TimeLimit = TimeLimit_0 * 1000,             % Convert to usec.
    Start = now(),
    SyncPoll =
        fun(Acc) ->
          case brick_server:chain_get_downstream_serials(Brick, Node) of
              {X, X} ->
                  %%io:format("p11 ~p ~p\n", [X, X]),
                  {false, Acc};
              {X, Y} when is_integer(X), is_integer(Y) ->
                  case timer:now_diff(now(), Start) of
                      D when D > TimeLimit ->
                          {false, {poll_for_full_sync, timeout, X, Y}};
                      _   ->
                          {ok, Ps} = brick_server:status(Brick, Node),
                          Cs = proplists:get_value(chain, Ps),
                          case proplists:get_value(chain_downstream, Cs) of
                              undefined when Acc < 4 ->
                                  %% TODO: There's no downstream, is it
                                  %% worth polling again here?
                                  %%io:format("p9 "),
                                  timer:sleep(200),
                                  {true, Acc + 1};
                              undefined ->
                                  %%io:format("p9x "),
                                  {false, Acc};
                              {DnBr, DnNd} ->
                                  case (catch brick_server:status(DnBr, DnNd)) of
                                      {ok, _} ->
                                          %%io:format("p10 ~p ~p ~p ", [X, Y, DnBr]),
                                          timer:sleep(200),
                                          {true, Acc + 1};
                                      _ ->
                                          %% The downstream brick is not alive,
                                          %% the downstream serials from Brick
                                          %% are not going to change.
                                          ?E_INFO("poll_for_full_sync: ~w ~w: downstream brick ~w ~w is not alive\n", [Brick, Node, DnBr, DnNd]),
                                          {false, Acc}
                                  end
                          end
                  end;
              Err ->
                  ?E_ERROR("poll_for_full_sync: ~w ~w: ~p\n",
                           [Brick, Node, Err]),
                  {false, Err}
          end
        end,
    case gmt_loop:do_while(SyncPoll, 0) of
        N when is_integer(N) -> ok;
        Err                  -> Err
    end.

-spec backward_status_but_alive_p(internal_status(), internal_status()) -> boolean().
backward_status_but_alive_p(OldStatus, NewStatus) ->
    OldNum = status_to_num(OldStatus),
    NewNum = status_to_num(NewStatus),
    if NewNum < OldNum, NewNum > 0 ->
            true;
       true ->
            false
    end.

-spec status_to_num(internal_status()) -> 0 | 10 | 20 | 30.
status_to_num(pre_init)  -> 10;
status_to_num(repairing) -> 20;
status_to_num(ok)        -> 30;
status_to_num(_)         -> 0.

list_uniq([X,X|Xs]) -> list_uniq([X|Xs]);
list_uniq([X|Xs])   -> [X|list_uniq(Xs)];
list_uniq([])       -> [].

%% -spec state_to_proplist(#state{}) -> list({atom(), term()}).
state_to_proplist(S) when is_record (S, state) ->
    [{Name,element(Pos, S)} || {Pos, Name} <- info_state_r()].

%% -spec info_state_r() -> list({integer(), atom()}).
info_state_r() ->
    Es = record_info(fields, state),
    lists:zip(lists:seq(2, length(Es) + 1), Es).

-spec check_difflist_for_disk_error(brick_list(), #state{}) -> ok.
check_difflist_for_disk_error(DiffList, _S) ->
    lists:foreach(
      fun({{_Br, Nd} = Brick, {OldState, NewState}}) ->
              if NewState == disk_error, OldState /= NewState ->
                      rpc:call(Nd, gmt_util, set_alarm,
                               [{disk_error, Brick},
                               "Administrator intervention is required."]);
                 OldState == disk_error, OldState /= NewState ->
                      rpc:call(Nd, gmt_util, clear_alarm,
                               [{disk_error, Brick}]);
                 true ->
                      ok
              end
      end, DiffList).

-spec set_global_hash_wrapper(brick_bp:brick_name(), node(), #g_hash_r{}) -> ok.
set_global_hash_wrapper(Brick, Node, GH) ->
    case brick_server:chain_hack_set_global_hash(Brick, Node, GH) of
        ok ->
            ok;
        {error, N} when is_integer(N), N > GH#g_hash_r.minor_rev ->
            %% Race: The global hash for this table has already been updated.
            %% N is the new revision number.  We're OK here.
            ok
    end.

%%%
%%% Misc edoc stuff
%%%

%% @type brick_name() = atom()
%% @type chain_name() = atom()
%% @type node() = atom()
