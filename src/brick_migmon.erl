%%%----------------------------------------------------------------------
%%% Copyright: (c) 2008-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : brick_migmon.erl
%%% Purpose : Brick cluster migration monitor server
%%%----------------------------------------------------------------------

-module(brick_migmon).
-include("applog.hrl").


-behaviour(gen_fsm).

-include("brick_admin.hrl").
-include("brick_hash.hrl").

%% External exports
-export([start_link/2]).

%% gen_fsm callbacks
-export([init/1,
         chains_starting/2, chains_starting/3,
         migrating/2, migrating/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {
          tab,                                  % table_r()
          tref,                                 % timer_ref()
          chain_names = [],
          options = []
         }).

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------
start_link(T, Options) when is_record(T, table_r) ->
    gen_fsm:start_link(brick_migmon, [T, Options], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_fsm
%%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, StateName, StateData}          |
%%          {ok, StateName, StateData, Timeout} |
%%          ignore                              |
%%          {stop, StopReason}
%%----------------------------------------------------------------------
init([T, Options]) ->
    register(list_to_atom("migmon_" ++ atom_to_list(T#table_r.name)), self()),
    Tref = brick_itimer:send_interval(990, do_check_migration),
    gen_fsm:send_event(self(), trigger),
    {ok, chains_starting, #state{tab = T, tref = Tref, options = Options}}.

%%----------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%----------------------------------------------------------------------
chains_starting(trigger, S) when is_record(S, state) ->
    ?APPLOG_INFO(?APPLOG_APPM_054,"Migration monitor: ~w: chains starting\n",
                 [(S#state.tab)#table_r.name]),
    Start = now(),
    T = S#state.tab,
    GH = T#table_r.ghash,

    AllChains = lists:usort(brick_hash:all_chains(GH, current)
                            ++ brick_hash:all_chains(GH, new)),
    ?APPLOG_INFO(?APPLOG_APPM_055,"~s: AllChains = ~p\n", [?MODULE, AllChains]),
    lists:map(
      fun({ChainName, _}) ->
              gmt_loop:do_while(
                fun(X) ->
                        case brick_sb:get_status(chain, ChainName) of
                            {ok, healthy}  -> {false, X};
                            {ok, degraded} -> {false, X};
                            _X             -> ?APPLOG_INFO(?APPLOG_APPM_056,"Migration monitor: ~w status ~w\n", [ChainName, _X]),
                                              timer:sleep(1000),
                                              {true, X}
                        end
                end, x)
      end, AllChains),
    gen_fsm:send_event(self(), trigger),
    case timer:now_diff(now(), Start) of
        N when N < 1*1000*1000 ->
            ?APPLOG_INFO(?APPLOG_APPM_057,"Migration monitor: ~w: sweeps starting\n",
                         [(S#state.tab)#table_r.name]),
            {next_state, migrating, S};
        _ ->
            timer:sleep(1000),
            {next_state, chains_starting, S}
    end.

migrating(trigger, S) when is_record(S, state) ->
    case do_check_migration(S) of
        ok ->
            {stop, normal, S};
        NewS when is_record(NewS, state) ->
            {next_state, migrating, NewS}
    end.

%%----------------------------------------------------------------------
%% Func: StateName/3
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%----------------------------------------------------------------------
chains_starting(_Event, _From, StateData) ->
    Reply = invalid,
    {reply, Reply, chains_starting, StateData}.

migrating(_Event, _From, StateData) ->
    Reply = invalid,
    {reply, Reply, chains_starting, StateData}.

%%----------------------------------------------------------------------
%% Func: handle_event/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%----------------------------------------------------------------------
handle_event(_Event, StateName, StateData) ->
    {next_state, StateName, StateData}.

%%----------------------------------------------------------------------
%% Func: handle_sync_event/4
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%----------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, StateData) ->
    Reply = invalid,
    {reply, Reply, StateName, StateData}.

%%----------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%----------------------------------------------------------------------
handle_info(_Info, StateName, StateData) ->
    gen_fsm:send_event(self(), trigger),
    {next_state, StateName, StateData}.

%%----------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%%----------------------------------------------------------------------
terminate(_Reason, _StateName, _StateData) ->
    ok.

%%----------------------------------------------------------------------
%% Func: code_change/4
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState, NewStateData}
%%----------------------------------------------------------------------
code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

do_check_migration(S) ->
    T = S#state.tab,
    TableName = T#table_r.name,
    [{FirstChainName, _}|_] = brick_hash:all_chains(T#table_r.ghash, current),
    %% Don't use T's GH, it's out-of-date.
    {ok, GH} = brick_admin:get_gh_by_chainname(FirstChainName),
    AllActiveChains = lists:usort(brick_hash:all_chains(GH, current) ++
                                  brick_hash:all_chains(GH, new)),
    Cookie = GH#g_hash_r.cookie,
    MyTimeout = 1*1000,
    try
        begin
            N = lists:foldl(
                  fun({_ChainName, []}, DoneCount) ->
                          bummer_no_active_bricks_right_now,
                          DoneCount;
                     ({_ChainName, [A]}, DoneCount) when is_atom(A) ->
                          bummer_no_active_bricks_right_now,
                          DoneCount;
                     ({ChainName, [{HeadBrick, HeadNode} = _Br|_]}, DoneCount) ->
                          %% TODO: replace [] with real Options list.
                          case (catch brick_server:migration_start_sweep(
                                        HeadBrick, HeadNode, GH#g_hash_r.cookie,
                                        ChainName, S#state.options, MyTimeout)) of
                              {error, migration_in_progress, Cookie} -> %exported
                                  ok;
                              ok ->
                                  ok;
                              _Err ->
                                  ok
                                  %% TODO: what else?
                                  %%error_logger:error_msg(
                                  %%  "check_migration: table ~p: brick ~p: ~p\n",
                                  %%  [TableName, _Br, _Err])
                          end,
                          case (catch brick_server:status(HeadBrick, HeadNode,
                                                          MyTimeout)) of
                              {ok, Ps} ->
                                  Ss = proplists:get_value(sweep, Ps),
                                  case proplists:get_value(status, Ss) of
                                      done ->
                                          DoneCount + 1;
                                      _ ->
                                          DoneCount
                                  end;
                              _ ->
                                  DoneCount
                          end
                  end, 0, AllActiveChains),
                                                %       io:format("N = ~p, wanted = ~p, AllActiveChains = ~p\n",
                                                %                 [N, length(AllActiveChains), AllActiveChains]),
            if N == length(AllActiveChains) ->
                    brick_admin:table_finished_migration(TableName),
                    ok;
               true ->
                    NewT = T#table_r{ghash = GH},
                    S#state{tab = NewT}
            end
        end
    catch
        X:Y ->
            ?APPLOG_ALERT(?APPLOG_APPM_058,"check_migration_table: ~p: ~p ~p\n", [TableName, X, Y]),
            S
    end.

