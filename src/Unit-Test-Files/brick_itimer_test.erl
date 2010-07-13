%%%----------------------------------------------------------------------
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
%%% File    : brick_itimer_test.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(brick_itimer_test).

-export([start_test/0]).

%% 1000 consumers, interval 2 sec, running for 1 min, brick_itimer uses cpu < 1%
%% vs original timer uses 30%
%%
%% If the consumers go up to 10-20k, the intervals of brick_itimer seems off
%% vs orginal timer is still accurate.
-define(HOW_MANY_CONSUMER, 2000).
-define(HOW_LONG, 60000).
-define(INTERVAL, 2000).
%-define(INTERVAL_TOLERANT, gmt_util:int_ify(?INTERVAL + (?INTERVAL / 10))). % 10%
-define(INTERVAL_TOLERANT, gmt_util:int_ify(?INTERVAL + 20)).   % 1%
-define(STOP_AFTER_ITER, ?HOW_LONG / ?INTERVAL).
%-define(TIMER, timer).
-define(TIMER, brick_itimer).


start_test() ->
  ok = start_brick_itimer(),

  ok = interval_test(),
  ok = cancel_test(),

  ok = stop_brick_itimer(),
  ok.

%% @doc test record is removed from dict when cancel is called
cancel_test() ->
  {ok, MRef} = ?TIMER:send_interval(?INTERVAL, "Cancel me"),
  ok = listen_a_few_times(3),
  ok = cancel_brick_itimer(MRef),

  % @todo(gki) need to check dict here

  ng = listen_a_few_times(3),
  ok.

listen_a_few_times(0) ->
  ok;
listen_a_few_times(Iter) ->
  receive
    "Cancel me" -> listen_a_few_times(Iter - 1);
    _ -> ng
  after ?INTERVAL_TOLERANT ->
    ng
  end.


%% @doc spam many consumers, test if they all receive message on time
interval_test() ->
  ok = spawn_consumers(?HOW_MANY_CONSUMER),
  timer:sleep(?HOW_LONG),
  ok.

start_brick_itimer() ->
  {ok, _Pid} = brick_itimer:start_link(),
  io:format("brick_itimer started at ~p~n", [_Pid]),
  ok.

cancel_brick_itimer(MRef) ->
  ?TIMER:cancel(MRef),
  ok.

stop_brick_itimer() ->
  ok.

spawn_consumers(0) ->
  ok;
spawn_consumers(HowMany) when HowMany > 0 ->
  spawn(fun() -> consumer() end),
  spawn_consumers(HowMany - 1).

consumer() ->
  consumer_loop("P" ++ pid_to_list(self())),
  ok.

consumer_loop(Msg) ->
  {ok, MRef} = ?TIMER:send_interval(?INTERVAL, Msg),
  consumer_loop(0, Msg, MRef).

consumer_loop(?STOP_AFTER_ITER, _, MRef) ->
  cancel_brick_itimer(MRef);
consumer_loop(Iter, Msg, MRef) ->
  receive
    Msg ->
       consumer_loop(Iter + 1, Msg, MRef);

    Bad ->
      io:format("~p has an error: ~p~n", [self(), Bad])

  after ?INTERVAL_TOLERANT ->
    case Iter of
      0 -> consumer_loop(Iter + 1, Msg, MRef);
      _ -> io:format("~p has existed INTERVAL_TOLERANT after ~p loops.  Stopping~n", [self(), Iter])
    end
  end.
