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
%%% File    : brick_mboxmon.erl
%%% Purpose : Mailbox monitor for brick servers
%%%-------------------------------------------------------------------

%% @doc Todo

-module(brick_mboxmon).
-include("applog.hrl").


-behaviour(gen_server).

-include("brick.hrl").
-include("brick_public.hrl").
-include("brick_hash.hrl").

-define(MY_TIMEOUT, 500).


%% API
-export([start_link/0,
         mark_self_repairing/0,
         mark_self_done_repairing/0,
         is_pid_repairing/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          brick_map,
          repair_high_water = 999888,
          high_water = 999888,
          low_water = 999888,
          above_high = []
         }).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------

%% @spec () -> {ok, pid()}
%% @doc Start the brick shepherd.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

mark_self_repairing() ->
    put(brick_mboxmon_repairing, true).

mark_self_done_repairing() ->
    erase(brick_mboxmon_repairing).

is_pid_repairing(undefined) ->
    false;
is_pid_repairing(Name) when is_atom(Name) ->
    is_pid_repairing(whereis(Name));
is_pid_repairing(Pid) when is_pid(Pid) ->
    case process_info(Pid, dictionary) of
        {dictionary, D} ->
            lists:keymember(brick_mboxmon_repairing, 1, D);
        _ ->
            false
    end.

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
    net_kernel:monitor_nodes(true, [{node_type, visible}, nodedown_reason]),
    {ok, RepairHigh} = gmt_config_svr:get_config_value_i(brick_mbox_repair_high_water, 1500),
    {ok, High} = gmt_config_svr:get_config_value_i(brick_mbox_high_water, 500),
    {ok, Low} = gmt_config_svr:get_config_value_i(brick_mbox_low_water, 100),
    {ok, #state{repair_high_water = RepairHigh,
                high_water = High,
                low_water = Low,
                brick_map = orddict:new()}, ?MY_TIMEOUT}.

%%--------------------------------------------------------------------
%% Function:
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(Request, From, State) ->
    io:format("DEBUG: ~s:handle_call: unknown Request ~w from ~w\n",
              [?MODULE, Request, From]),
    Reply = unknown_call,
    {reply, Reply, State, ?MY_TIMEOUT}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({new_high_low_water, RepairHigh, High, Low}, State) ->
    {noreply, State#state{repair_high_water = RepairHigh,
                          high_water = High,
                          low_water = Low}, ?MY_TIMEOUT};
handle_cast({water_report, Type, RepairingP, Brick, N,
             please_tell, HeadBrick}, State) ->
    do_water_report(Type, RepairingP, Brick, N, HeadBrick),
    {noreply, State, ?MY_TIMEOUT};
handle_cast(Msg, State) ->
    ?E_ERROR("DEBUG: ~s:handle_cast: unknown Msg ~w\n", [?MODULE, Msg]),
    {noreply, State, ?MY_TIMEOUT}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(timeout, State) ->
    NewState = timeout_poll(State),
    {noreply, NewState, ?MY_TIMEOUT};
handle_info({nodeup, Node, Extra}, State) ->
    ?APPLOG_INFO(?APPLOG_APPM_051,"~s net_kernel: node ~p up Extra ~w\n",
                 [?MODULE, Node, Extra]),
    {noreply, State, ?MY_TIMEOUT};
handle_info({nodedown, Node, Extra}, State) ->
    ?APPLOG_INFO(?APPLOG_APPM_052,"~s net_kernel: node ~p down Extra ~w\n",
                 [?MODULE, Node, Extra]),
    {noreply, State, ?MY_TIMEOUT};
handle_info(Info, State) ->
    ?APPLOG_INFO(?APPLOG_APPM_053,"DEBUG: ~s:handle_info: Info ~w\n", [?MODULE, Info]),
    {noreply, State, ?MY_TIMEOUT}.

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

timeout_poll(S) ->
    try
        check_mboxes(S)
    catch _X:_Y ->
            ?E_ERROR("DEBUG: ~s: ~p ~p at ~p\n", [?MODULE, _X, _Y, erlang:get_stacktrace()]),
            S
    end.

make_brick_map() ->
    case global:whereis_name(brick_admin) of
        undefined ->
            orddict:new();
        AdminServer ->
            Tables = brick_admin:get_tables(AdminServer),
            TabsGHs = [{Tab, GH} || Tab <- Tables,
                                    {ok, _Vers, GH} <- [brick_simple:get_gh(Tab)]],
            %% We need all bricks in all chains, not merely the active bricks.
            GHsChains =
                lists:flatten(
                  lists:map(
                    fun({_Tab, GH}) ->
                            Cs = (GH#g_hash_r.current_h_desc)#hash_r.healthy_chainlist ++
                                (GH#g_hash_r.new_h_desc)#hash_r.healthy_chainlist,
                            [{GH, Chain} || Chain <- Cs]
                    end, TabsGHs)),
            %% Filter: find bricks only on this node
            BrsChsGHs = [{Br, Ch, GH} || {GH, {Ch, Brs}} <- GHsChains,
                                         {Br, Nd} <- Brs, Nd == node()],
            %% Now find the head brick each chain in AllBrChs
            BrsHds = [{Br, brick_hash:chain2brick(Ch, write, GH)} ||
                         {Br, Ch, GH} <- BrsChsGHs],
            orddict:from_list(BrsHds)
    end.

check_mboxes(S) ->
    Bricks = [Brick || Brick <- brick_shepherd:list_bricks(),
                       whereis(Brick) /= undefined],
    Rs = check_mboxes(Bricks, S),
    UnderBricks = [Br || {below, Br, _} <- Rs],
    AboveBricks = [Br || {above, Br, _, _} <- Rs],
    NewAboveHigh = ((S#state.above_high -- UnderBricks) -- AboveBricks)
        ++ AboveBricks,
    S#state{above_high = NewAboveHigh}.

check_mboxes(Bricks, S) ->
    case lists:map(fun(Brick) -> check_an_mbox(Brick, S) end, Bricks) of
        [] ->
            [];
        Rs ->
            BrickMap = make_brick_map(),
            [set_repair_overload(Brick, N, S#state.repair_high_water) ||
                {above, Brick, true, N} <- Rs],
            [report_mbox_above_water(Brick, RepairingP, BrickMap, N) ||
                {above, Brick, RepairingP, N} <- Rs],
            [report_mbox_below_water(Brick, BrickMap, N) ||
                {below, Brick, N} <- Rs],
            Rs
    end.


check_an_mbox(Brick, S) ->
    RepairingP = is_pid_repairing(Brick),
    case (catch mbox_too_big(Brick, S)) of
        {false, N} ->
            case lists:member(Brick, S#state.above_high) of
                true ->
                    {below, Brick, N};
                false ->
                    {Brick, N}
            end;
        {true, N} ->
            {above, Brick, RepairingP, N};
        _ ->
            {Brick, 0}
    end.

%% mbox_too_big(tab1_ch2_b2, _S) -> % debugging...
%%     case element(2, now()) rem 10 of
%%      N when N == 0; N == 1 -> {true, 99999999};
%%      _ -> {false, 0}
%%     end;
mbox_too_big(Brick, S) ->
    try
        N = get_mbox_size(Brick),
        if N < S#state.low_water ->
                {false, N};
           N < S#state.high_water ->
                case lists:member(Brick, S#state.above_high) of
                    true ->
                        {true, N};
                    false ->
                        {false, N}
                end;
           true ->
                {true, N}
        end
    catch _X:_Y ->
            ?E_ERROR("~s: Error for brick ~p: ~p ~p\n", [?MODULE, Brick, _X, _Y]),
            {false, 0}
    end.

report_mbox_above_water(Brick, RepairingP, Map, N) ->
    report_mbox_common(Brick, RepairingP, Map, N, above_water).

report_mbox_below_water(Brick, Map, N) ->
    report_mbox_common(Brick, false, Map, N, below_water).

report_mbox_common(Brick, RepairingP, Map, N, Type) ->
    case orddict:find(Brick, Map) of
        {ok, {HeadBr, HeadNd}} ->
            if HeadBr == Brick ->
                    ?E_ERROR("~s: Local brick ~p has mbox size ~p, type ~p\n",
                             [?MODULE, Brick, N, Type]),
                    if Type == above_water ->
                            throttle_brick(Brick, RepairingP);
                       Type == below_water ->
                            unthrottle_brick(Brick)
                    end;
               true ->
                    ?E_ERROR("~s: brick ~p has mbox size ~p, type ~p "
                             "(repairing ~p), notifying ~p\n",
                             [?MODULE, Brick, N, Type, RepairingP, HeadNd]),
                    Msg = {water_report, Type, RepairingP, Brick, N,
                           please_tell, HeadBr},
                    gen_server:cast({?MODULE, HeadNd}, Msg)
            end;
        error ->
            ok                                  % Our map is out of date
    end.

do_water_report(Type, RepairingP, WaterBrick, Num, ThrottleBrick) ->
    RegName = list_to_atom(atom_to_list(ThrottleBrick) ++ "_mboxmon"),
    ParentPid = self(),
    Pid = spawn_link(fun() ->
                             start_water_report_loop(
                               ParentPid, ThrottleBrick, RegName)
                     end),
    Msg = {water_update, Type, RepairingP, {WaterBrick, Num}, ThrottleBrick},
    Pid ! Msg,
    catch (RegName ! Msg).

%% NOTE: In an earlier draft, I'd needed this per-throttled-brick proc
%% to clean up and un-throttle a brick after a short period of
%% inactivity.  Then I added the timestamp to the ETS table (see
%% brick_server:throttle_brick()) as a self-enforcing limit.  So, I
%% think that this extra process isn't required?  This would be a good
%% refactoring exercise for a developer who's new to Erlang?

start_water_report_loop(ParentPid, ThrottleBrick, RegName) ->
    %% Use registered name to make certain that there are only one
    %% of these running per throttled brick.
    case (catch register(RegName, self())) of
        true -> water_report_loop(ParentPid, ThrottleBrick);
        _    -> unlink(ParentPid),
                exit(normal)
    end.

water_report_loop(ParentPid, ThrottleBrick) ->
    receive
        {water_update, above_water, RepairingP, {WaterBrick, Num},
         ThrottleBrick} ->
            ?E_ERROR("water_loop: ~p throttle by ~p @ ~p msgs (repairing ~p)\n",
                     [ThrottleBrick, WaterBrick, Num, RepairingP]),
            throttle_brick(ThrottleBrick, RepairingP),
            water_report_loop(ParentPid, ThrottleBrick);
        {water_update, below_water, _RepairingP, {WaterBrick, Num},
         ThrottleBrick} ->
            ?E_ERROR("water_loop: ~p throttle is below water by ~p @ ~p\n",
                     [ThrottleBrick, WaterBrick, Num]),
            unthrottle_brick(ThrottleBrick);
        Any ->
            ?E_ERROR("UNKNOWN message: ~p\n", [Any]),
            water_report_loop(ParentPid, ThrottleBrick)
    after 2000 ->
            ok
    end,
    unlink(ParentPid),
    exit(normal).

get_mbox_size(Brick) ->
    {message_queue_len, N} = process_info(whereis(Brick), message_queue_len),
    N.

set_repair_overload(Brick, N, RepairHigh) ->
    ?E_INFO("~s: Brick ~p mailbox at ~p > ~p\n", [?MODULE, Brick, N, RepairHigh]),
    {ok, ResumeSecs} = gmt_config_svr:get_config_value_i(
                         brick_mbox_repair_overload_resume_interval, 300),
    %% The overloaded brick is running on this node, so we're able to
    %% call brick_server:set_repair_overload_key().
    brick_server:set_repair_overload_key(Brick),
    ResumeName = list_to_atom(atom_to_list(Brick) ++ "_mboxmon_resume"),
    spawn(fun() ->
                  case whereis(ResumeName) of
                      undefined ->
                          register(ResumeName, self()); % Only one of us!
                      _ ->
                          exit(normal)
                  end,
                  _Rx = (catch rpc:call(node(global:whereis_name(brick_admin)),
                                        brick_sb, report_brick_status,
                                        [Brick, node(), repair_overload,
                                         [{backlog, N},
                                          {repair_high_water, RepairHigh}]])),
                  catch brick_server:chain_set_my_repair_state(
                          Brick, node(), repair_overload),
                  ?E_ERROR("Brick ~p has mailbox size ~p > repair high water "
                           "mark ~p, changing brick status, resume in ~p "
                           "seconds (proc ~p)\n",
                           [Brick, N, RepairHigh, ResumeSecs, ResumeName]),
                  put(start_date_time, {date(), time()}),
                  put(resume_secs, ResumeSecs),
                  timer:sleep(ResumeSecs * 1000),
                  case brick_server:chain_get_my_repair_state(
                         Brick, node()) of
                      repair_overload ->
                          ?E_ERROR("~p: Change brick ~p to pre_init status\n",
                                   [ResumeName, Brick]),
                          brick_server:chain_set_my_repair_state(
                            Brick, node(), pre_init);
                      _ ->
                          ok
                  end,
                  exit(normal)
          end).

throttle_brick(ThrottleBrick, RepairingP) ->
    ?E_INFO("~s: throttle ~p repairing ~p\n",
            [?MODULE, ThrottleBrick, RepairingP]),
    brick_server:throttle_brick(ThrottleBrick, RepairingP).

unthrottle_brick(ThrottleBrick) ->
    ?E_INFO("~s: un-throttle ~p\n", [?MODULE, ThrottleBrick]),
    brick_server:unthrottle_brick(ThrottleBrick).

