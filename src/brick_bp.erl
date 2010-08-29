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
%%% File    : brick_bp.erl
%%% Purpose : Brick pinger/status monitor.
%%%-------------------------------------------------------------------

%% @doc A simple brick "pinger"/status monitor.
%%
%% A 1-second periodic timer is used to check the status of this
%% proc's ward, a single storage brick process.
%%
%% Any major changes in status will be sent to the "scoreboard" proc
%% as a proplist.
%%
%% See also: TODO comments at the top of `handle_event()' in the
%% `check_status' clause.

-module(brick_bp).
-include("applog.hrl").


-behaviour(gen_fsm).

-include("brick.hrl").
-include("brick_admin.hrl").

-ifdef(debug_bp).
-define(gmt_debug, true).
-endif.
%%-define(gmt_debug, true).                     %QQQXXXYYYZZZ debugging
-include("gmt_debug.hrl").

%% API
-export([start_link/4, stop_pinger/1,
         make_pinger_registered_name/1]).

%% gen_fsm callbacks
-export([init/1,
         unknown/2, unknown/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4,
         state/1]).

%% Non-FSM-related API
-export([start_pingers/2]).

%% Exported purely to stop dialyzer warnings - not for use externally
-export([get_brick_repair_state/2,call_with_timeout_repeat/3,brick_monitor_simple/3]).

-export_type([brick_name/0, nowtime/0, proplist/0, state_name/0]).

-type brick_name() :: atom().
-type nowtime() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
-type state_name() :: brick_server:repair_state_name_base() | repairing.
-type proplist() :: [atom() | {atom(), term()}].

-record(state,
              { brick            :: brick_admin:brick()
              , node             :: node()
              , brick_options=[] :: proplist()
              , tref             :: reference()    % timer ref
              , start_time       :: nowtime() | unknown  % Last successful start time.
              , out_of_unknown_p :: boolean() % Flag: if we've seen this
                                              % brick outside of unknown.
              , monitor_pid      :: pid()     % pid of monitoring proc
              , sb_pid           :: pid()     % pid of scoreboard server
              , sleep_random     :: integer() % sleep random for check_status
              }).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> ok,Pid} | ignore | {error,Error}
%% Description:Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this function
%% does not return until Module:init/1 has returned.
%%--------------------------------------------------------------------
start_link(Name, Brick, Node, BrickOptions) ->
    %%X = (catch 1/0), throw({'hey_who_is_using_this_fuction?', X}),
    gen_fsm:start_link({local, Name}, ?MODULE,
                       [Name, Brick, Node, BrickOptions], []).

start_pingers(Bricks, BrickOptions) ->
    ?DBG({start_pingers, Bricks}),
    lists:foreach(
      fun({Brick, Node} = _B) ->
              Name = make_pinger_registered_name(Brick),
              ArgList = [Name, Brick, Node, BrickOptions],
              ChildSpec =
                  {Name, {brick_bp, start_link, ArgList},
                   transient, 2000, worker, [brick_bp]},
              ok = case supervisor:start_child(brick_mon_sup, ChildSpec) of
                       {ok, _}                       -> ok;
                       {error, {already_started, _}} -> ok;
                       {error, already_present}      -> ok;
                       Err                           -> Err
                   end
      end, Bricks),
    ok.

%% @spec (atom()) -> ok | {error, term()}
%% @doc Stop an individual pinger, must be run on same node as pinger.

stop_pinger(Name) ->
    RegName = make_pinger_registered_name(Name),
    case (catch supervisor:terminate_child(brick_mon_sup, RegName)) of
        ok ->
            catch supervisor:delete_child(brick_mon_sup, RegName);
        Err ->
            Err
    end.

state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, {state}).

% %% @doc Given a brick's name, find it's pinger's Pid.
% %%
% %% NOTE: We assume that the caller is on the same node as the admin app
% %%       and therefore the same node as *all* of the pingers.

% find_pinger_pid(Brick) ->
%     whereis(make_pinger_registered_name(Brick)).

% request_check(Brick) ->
%     find_pinger_pid(Brick) ! check_status.

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
init([_Name, Brick, Node, BrickOptions] = _Arg) ->
    random:seed(now()),
    {ok, Time} = gmt_config_svr:get_config_value_i(admin_server_brick_poll, 1400),
    {ok, TRef} = brick_itimer:send_interval(Time, check_status),
    {ok, SleepRnd} = gmt_config_svr:get_config_value_i(admin_server_brick_pinger_sleep_random, 900),
    self() ! send_initial_status_report,
    StName = unknown,
    ?DBG({?MODULE, _Arg}),
    {ok, StName, #state{brick = Brick, node = Node,
                        brick_options = BrickOptions, tref = TRef,
                        out_of_unknown_p = false, sleep_random = SleepRnd}}.

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
unknown(_Event, State) ->
    {next_state, unknown, State}.

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
    Reply = not_implemented,
    {reply, Reply, unknown, State}.

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
handle_event(check_status, StateName, State) ->
    %% TODO: This is less code to write ... but is the meaning clear enough?
    %% This could alternatively be implemented via converting our periodic
    %% status polls into events such as:
    %%   am_unknown, am_undefined, am_repairing, am_ok
    %% ... and then write explicit state transitions from each state to
    %% the new state.
    %%
    %% As this gen_fsm is written now, we might as well be a gen_server,
    %% because we aren't really using FSM states for any useful purpose....
    timer:sleep(random:uniform(State#state.sleep_random)),
    Brick = State#state.brick,
    Node = State#state.node,
    catch brick_admin:start_brick_only(Brick, Node, State#state.brick_options),
    MPid = if State#state.monitor_pid == undefined ->
                   case gmt_util:make_monitor({Brick, Node}) of
                       {ok, M} ->
                           %% We know that the remote brick is alive now.
                           erlang:demonitor(M),
                           PPid = self(),
                           proc_lib:spawn_link(fun() ->
                                      brick_monitor_simple(Brick, Node, PPid)
                                      end);
                       error ->
                           undefined
                   end;
              true ->
                   State#state.monitor_pid
           end,
    State2 = State#state{monitor_pid = MPid},
    F_msg = fun() -> ?E_ERROR("Brick pinger: killing brick ~p\n", [Brick]) end,
    F_kill = fun() ->
                     F_msg(),
                     catch exit(whereis(Brick), kill)
             end,
    OldSTime = State#state.start_time,
    {NewStateName, PropList, NewState} =
        case get_brick_repair_state(Brick, Node) of
            ok ->
                case get_brick_start_time(Brick, Node) of
                    error ->
                        ?E_ERROR("ERROR: Get start time for ~p ~p at ~p, killing\n",
                                 [State#state.brick, State#state.node, StateName]),
                        F_msg(),
                        spawn(Node, F_kill),
                        {unknown, [], State2#state{start_time = OldSTime}};
                    {STime, OkTime} when is_tuple(OkTime) ->
                        %% TODO Um, what happens if the repair status is
                        %% not OK?
                        if StateName /= ok,
                           STime == State#state.start_time,
                           State#state.out_of_unknown_p ->
                                %% TODO: Is there anything else that we need
                                %% to do here, or is the kill good enough?
                                ?E_ERROR(
                                  "Brick {~w,~w}: old state = ~w, new "
                                  "state = ok, bad transition with same "
                                  "start time: ~w\n",
                                  [State#state.brick, State#state.node,
                                   StateName, STime]),
                                F_msg(),
                                spawn(Node, F_kill),
                                ?E_ERROR(
                                  "Brick {~w,~w}: killed asynchronously from StateName ~p",
                                  [State#state.brick, State#state.node, StateName]),
                                Report = [{killed, now()},
                                          {old_state, StateName},
                                          {info, "Attempted transition back "
                                           "to 'ok' but brick has not been "
                                           "restarted in between"}],
                                %% Report status here because maybe_send
                                %% will only report if there's a state
                                %% change, and there probably isn't a change.
                                brick_sb:report_brick_status(
                                  State#state.brick, State#state.node,
                                  unknown, Report),
                                {unknown, Report,
                                 State2#state{start_time = OldSTime}};
                           true ->
                                {ok, [{start_time, STime},
                                      {chain_my_repair_ok_time, OkTime}],
                                 State2#state{start_time = STime}}
                        end;
                    {_, undefined} ->
                        {unknown, [], State2#state{start_time = OldSTime}}
                end;
            disk_error ->
                {disk_error, [{health_monitor, disk_error}],
                 State2#state{start_time = unknown}};
            RepairState ->
                case get_brick_start_time(Brick, Node) of
                    {STime, Tuple} when STime == OldSTime,
                                        is_tuple(Tuple),
                                        RepairState == pre_init ->
                        {pre_init, [{health_monitor, pre_init}],
                         State2#state{start_time = STime}};
                    {_STime, Tuple} when is_tuple(Tuple),
                                         RepairState == pre_init ->
                        %% Honest race condition: the brick just
                        %% changed to ok repair status.  Recursion!
                        timer:sleep(1100),
                        {next_state, StateName2, State3} =
                            handle_event(check_status, StateName, State2),
                        {StateName2, [{pre_init_change, true}], State3};
                    {_STime, Tuple} when is_tuple(Tuple),
                                         RepairState == unknown ->
                        ?E_ERROR("ERROR: Brick ~p ~p did not respond to repair "
                                 "status request, killing\n",
                                 [State#state.brick, State#state.node]),
                        F_msg(),
                        spawn(Node, F_kill),
                        {unknown, [], State2#state{start_time = OldSTime}};
                    _Tuple ->
                        {RepairState, [], State2#state{start_time = OldSTime}}
                end
        end,
    maybe_send_status_report(StateName, NewStateName, PropList, NewState),
    OOUP = if not NewState#state.out_of_unknown_p,
              NewStateName /= unknown ->
                   ?E_INFO("pinger: transition of ~p from unknown to ~p\n",
                           [NewState#state.brick, NewStateName]),
                   true;
              true ->
                   NewState#state.out_of_unknown_p
           end,
    %% We may have taken many seconds of wall clock time.  Consume the
    %% extra check_status() reminders that may build up while we waited.
    flush_mailbox_extra_check_status(),
    {next_state, NewStateName, NewState#state{out_of_unknown_p = OOUP}};
handle_event(Event, StateName, State) ->
    ?E_ERROR("~s: handle_event: ~p in ~p\n", [?MODULE, Event, StateName]),
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
    {reply, State, StateName, State};
handle_sync_event(Event, _From, StateName, State) ->
    ?E_ERROR("~s: handle_sync_event: ~p in ~p\n", [?MODULE, Event, StateName]),
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
handle_info(send_initial_status_report, StateName, State) ->
    %% Don't actually send a status report now, because we'll be
    %% reporting 'unknown'.  When the admin server starts a length=1
    %% chain, it will report 'ok' directly to the scoreboard, so sending
    %% an 'unknown' report here is a bad thing.
    %% brick_sb:report_brick_status(State#state.brick, State#state.node,
    %%                               StateName, []),
    %% Get history to find the last known start time.
    STime =
        case brick_sb:get_brick_history(State#state.brick, State#state.node) of
            {ok, Hs} ->
                case lists:dropwhile(fun(H) when is_record(H, hevent),
                                                 H#hevent.what == state_change,
                                                 H#hevent.detail == ok ->
                                             false;
                                        (_) -> true
                                     end, Hs) of
                    [] ->
                        unknown;
                    [Hist|_] ->
                        proplists:get_value(start_time, Hist#hevent.props,
                                            unknown)
                end;
            _ ->
                %% TODO: Are we giving up too easily here?
                unknown
        end,
    SBPid = brick_sb:sb_pid(),
    {ok, _} = gmt_util:make_monitor(SBPid),
    ?E_INFO("Brick ~w ~w: last start_time in history = ~w\n",
            [State#state.brick, State#state.node, STime]),
    %% Worry about 'DOWN' monitoring with first check_status processing.
    {next_state, StateName, State#state{start_time = STime, sb_pid = SBPid}};
handle_info(check_status, StateName, State) ->
    %% TODO: This is probably a redirection kludge worth removing?
    gen_fsm:send_all_state_event(self(), check_status),
    {next_state, StateName, State};
handle_info({Ref, _Reply}, StateName, State) when is_reference(Ref) ->
    %% Late gen_server reply, ignore it.
    {next_state, StateName, State};
handle_info({monitor_stop, MPid, Reason}, _StateName, State)
  when MPid == State#state.monitor_pid ->
    Fmt = "~s: handle_info: monitor pid ~p stopped, target brick "
           "~p stopped for reason ~p\n",
    Args = [?MODULE, MPid, State#state.brick, Reason],
    if Reason == normal;
       Reason == shutdown -> ?E_INFO(Fmt, Args);
       true               -> ?E_ERROR(Fmt, Args)
    end,
    %% Kludge to assist honest-but-not-helpful race condition with
    %% scoreboard.  The need for delays like this (e.g. when a brick
    %% recovers too quickly for a scoreboard poll by someone else to
    %% notice the state change) is a good reason to rewrite this code
    %% on a larger scale someday.
    timer:sleep(3000),
    {next_state, unknown, State#state{monitor_pid = undefined}};
handle_info({'DOWN', _MRef, _Type, Pid, Info}, _StateName, State)
  when Pid == State#state.sb_pid ->
    ?E_INFO("~s: ~p: scoreboard process ~p stopped, ~p\n",
            [?MODULE, State#state.brick, Pid, Info]),
    timer:sleep(1000), % Allow some time for scoreboard to recover
    {stop, scoreboard_down, State};
handle_info(_Info, StateName, State) ->
    ?E_ERROR("~s: handle_info: ~p\n", [?MODULE, _Info]),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, StateName, State) -> void()
%% Description:This function is called by a gen_fsm when it is about
%% to terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, State) ->
    catch (State#state.monitor_pid ! shutdown_please),
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

-spec maybe_send_status_report(state_name() | was_alive, state_name(), proplist(), #state{}) -> ok.
-spec make_pinger_registered_name(brick_name()) -> brick_name().
-spec brick_monitor_simple(brick_name(), node(), pid()) -> normal.
-spec get_brick_repair_state(brick_name(), node()) -> state_name().
-spec get_brick_start_time(brick_name(), node()) -> {nowtime(), nowtime() | undefined} | error.
-spec flush_mailbox_extra_check_status() -> ok.
-spec call_with_timeout_repeat(fun(), non_neg_integer(), timeout()) -> any().

maybe_send_status_report(StateName, StateName, _PropList, _S) ->
    ok;
maybe_send_status_report(_StateName, NewStateName, PropList, S) when is_list(PropList) ->
    brick_sb:report_brick_status(S#state.brick, S#state.node,
                                     NewStateName, PropList).

make_pinger_registered_name(Brick) ->
    list_to_atom("pinger_" ++ atom_to_list(Brick)).

brick_monitor_simple(Brick, Node, ParentPid) ->
    %% Drat, race condition to deal with here, bleh.
    case gmt_util:make_monitor({Brick, Node}) of
        {ok, MRef} ->
            receive
                {'DOWN', MRef, _Type, _Object, Info} ->
                    ?E_INFO(
                      "Brick ~p Mref ~p, Type ~p, Object ~p, Info ~p\n",
                      [{Brick, Node}, MRef, _Type, _Object, Info]),
                    FakeState = #state{brick = Brick, node = Node},
                    maybe_send_status_report(was_alive, unknown,
                                             [{monitor_reason, Info}], FakeState),
                    ParentPid ! {monitor_stop, self(), Info},
                    unlink(ParentPid),
                    exit(normal);
                shutdown_please ->
                    normal
            end;
        Err ->
            ?E_INFO("Brick ~p monitor failed: ~P\n", [{Brick, Node}, Err, 20]),
            exit(ParentPid)
    end.

%% NOTE: Both get_brick_repair_state() and get_brick_start_time() use longer
%%       timeout values because we now rely on the partition_detector app and
%%       brick_admin_event_h.erl to detect loss of UDP heartbeats and use
%%       net_kernel:disconnect/1 to interrupt calls to dead nodes.

get_brick_repair_state(Brick, Node) ->
    Timeout = 5*1000,
    Fdoit = fun() ->
                    brick_pingee:get_repair_state(Brick, Node, Timeout)
            end,
    case (catch call_with_timeout_repeat(Fdoit, 4, Timeout)) of % About 20 sec.
        {'EXIT', _} ->
            unknown;
        {last_key, _} ->
            repairing;
        Else ->
            Else
    end.

get_brick_start_time(Brick, Node) ->
    Timeout = 5*1000,
    Fdoit = fun() ->
                    brick_pingee:status(Brick, Node, Timeout)
            end,
    case (catch call_with_timeout_repeat(Fdoit, 4, Timeout)) of % About 20 sec.
          {ok, StartTime, MaybeRepairTime}  ->
                 {StartTime, MaybeRepairTime};
          _ ->
                 error
         end.

flush_mailbox_extra_check_status() ->
    receive
        {'$gen_all_state_event',check_status} ->
            flush_mailbox_extra_check_status();
        check_status ->
            flush_mailbox_extra_check_status()
    after 0 ->
            ok
    end.

%% @doc Emulate a single gen_server-type call with several attempts with a
%% short timeout.  Use in places where you want quicker timeout response
%% from a gen_server proc that consumes '$gen_call' messages when it's
%% too busy.

call_with_timeout_repeat(Fun, 0, _Timeout) ->
    Fun();
call_with_timeout_repeat(Fun, N, Timeout) when N > 0 ->
    try
        Fun()
    catch
        exit:{timeout, _} ->
            call_with_timeout_repeat(Fun, N - 1, Timeout);
        exit:{noproc, _} ->
            timer:sleep(Timeout),
            call_with_timeout_repeat(Fun, N - 1, Timeout)
    end.
