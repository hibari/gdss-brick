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

-module(test_reliablefifo).

-include("brick.hrl").
-include("brick_admin.hrl").
-include("brick_hash.hrl").
-include("brick_public.hrl").
-include("gmt_hlog.hrl").

-ifdef(debug_brick_test0).
-define(gmt_debug, true).
-endif.
%%-define(gmt_debug, true).                     %QQQXXXYYYZZZ debugging
-include("gmt_debug.hrl").

-define(M, brick_server).

-define(HEAD, reliablefifo___ch1_b1).
-define(TAIL, reliablefifo___ch1_b2).
-define(CHAIN, reliablefifo___ch1).
-define(TAB, reliablefifo__).
-define(DownPeriod, 3*1000).

-define(BT0, brick_test0).
-define(SetInterval, 10).
-define(NetTick, 20).

-compile(export_all).

brick0() ->
    register(test0, self()),
    application:start(gdss),
    start_brick0().

start_brick0() ->
    dbg:tracer(),
    dbg:p(all, call),
    %%    dbg:tpl(application,get_env, [{'_', [], [{return_trace}]}]),
    loop0().

loop0() ->
    receive
        {sync, X} ->
            io:format(":::sync:::~n"),
            X ! ok,
            loop0();
        {ets, X, Role} ->
            NameETS = list_to_atom(atom_to_list(Role) ++ "_store"),
            Ret = (catch ?BT0:xform_ets_dump(Role, ets:tab2list(NameETS))),
            %%      io:format(":::ETS:::~p~n", [Ret]),
            io:format("::: checking ets~n"),
            X ! {ets,Ret},
            loop0();
        {disconnect, Node} ->
            io:format(":::net_kernel:disconnect: ~p:::~n",[net_kernel:disconnect(Node)]),
            loop0();
        restart ->
            ?M:stop(whereis(?TAIL)),
            loop0();
        stop ->
            dbg:stop(),
            io:format(":::fin~n"),
            ok_exit
    end.

stop_bricks() ->
    X = {test0, tailnode()},
    Y = {test0, headnode()},
    io:format("::: stopping ~p~n", [X]),
    X ! stop,
    Y ! stop,
    ok.

tailnode() ->
    list_to_atom("gdss_dev2@"++gmt_util:node_right()).

headnode() ->
    list_to_atom("gdss_dev3@"++gmt_util:node_right()).

adminnode() ->
    list_to_atom("gdss_dev4@"++gmt_util:node_right()).

cl_chain_reliablefifo() ->
    dbg:tracer(),
    dbg:p(all, call),
    %%    dbg:tpl(brick_server, handle_call_do, [{'_', [], [{return_trace}]}]),
    %%dbg:tpl(?MODULE, chain_reliablefifo, [{'_', [], [{return_trace}]}]),
    application:start(gdss),

    %% wait for the tail starts
    timer:sleep(1000),
    ok = bootstrap_chain(),

    %% repeat the same test 100 times for each type of disconnection
    [ok = chain_reliablefifo([], 100, fun start_net_kernel0/0, StopFun) ||
        StopFun <- [fun stop_net_kernel0/0
                    %% fun stop_net_kernel1/0,
                    %% fun stop_net_kernel2/0,
                    %% fun stop_net_kernel3/0,
                    %% fun stop_net_kernel4/0,
                    %% fun stop_net_kernel5/0,
                    %% fun stop_net_kernel6/0
                   ]],
    dbg:stop(),
    io:format("reliablefifo tests PASS\n").

chain_reliablefifo(_OptionList, 0, _, _) ->
    stop_bricks();
chain_reliablefifo(OptionList, Repeat, Start, Stop) ->
    NodeHead = headnode(),
    NodeTail = tailnode(),

    %% sync with brciks
    %% timer:sleep(1000),
    pong = net_adm:ping(NodeHead),
    pong = net_adm:ping(NodeTail),
    io:format(":::syncing:::~n"),
    {test0, NodeHead} ! {sync, self()},
    ok = receive ok -> ok end,
    io:format(":::sync head ok:::~n"),
    {test0, NodeTail} ! {sync, self()},
    ok = receive ok -> ok end,
    io:format(":::sync tail ok:::~n"),

    %% Set some stuff
    {set0, ok} = {set0, brick_simple:set(?TAB, "zoo1", "the zoo 1")},

    Bar = spawn_to_send(10000),

    %% wait a bit before closing the connection
    timer:sleep(1000),

    %% closes connection between Head/Admin node and Tail node
    %% brick_pinger on Admin node restarts the Tail node
    ok = down_and_up(Start,Stop),

    %% stop bar
    Bar ! {self(), stop},
    receive stopped ->
            ok
    end,

    %% wait until all transactions finish and chain becomes healthy
    timer:sleep(10*1000),

    sync_brick(?TAIL, NodeTail, {tail, {?HEAD, NodeHead}}),
    sync_brick(?HEAD, NodeHead, {head, {?TAIL, NodeTail}}),

    {test0, NodeHead} ! {ets, self(), ?HEAD},
    io:format(":::checking head keys~n"),
    HM2 = receive {ets,X} -> X end,

    {test0, NodeTail} ! {ets, self(), ?TAIL},
    io:format(":::checking tail keys~n"),
    TM2 = receive {ets,Y} -> Y end,
    io:format(":::ets done~n"),

    ?DBG(HM2),
    ?DBG(TM2),
    case HM2 of
        TM2 ->
            io:format("~n:::GOOD(~p), both bricks are equal(~p keys).\n",
                      [Repeat, length(TM2)]);
        _ ->
            io:format("~nHM2 = ~p", [HM2]),
            io:format("~nTM2 = ~p", [TM2]),
            io:format("~nHM2 -- TM2 = ~p~n", [HM2 -- TM2]),
            stop_bricks(),
            exit(fin)

    end,

    %% sleep a while between each iteration
    timer:sleep(1000),
    chain_reliablefifo(OptionList, Repeat -1, Start, Stop).


spawn_to_send(N) ->
    spawn(fun () ->
                  send0(N)
          end).


send0(0) ->
    io:format("fin~n");
send0(Num) ->
    %%    io:format(","),
    receive {Parent,stop} ->
            io:format("~n:::bar: stopped~n"),
            Parent ! stopped
    after 0 ->
            Key = "bar" ++ integer_to_list(Num),
            Value = "bar: " ++ integer_to_list(Num),
            spawn(fun() ->
                          brick_simple:set(?TAB, Key, Value)
                  end),
            io:format("."),
            %% interval between async set operations
            timer:sleep(?SetInterval),
            send0(Num - 1)
    end.

down_and_up(Start, Stop) ->
    Stop(),
    io:format("down"),

    %% sleep between down and up
    timer:sleep(?DownPeriod),

    Start(),

    io:format("up"),
    ok.

start_if() ->
    [] = os:cmd("/sbin/ifconfig lo up").
stop_if() ->
    [] = os:cmd("/sbin/ifconfig lo down").

start_net_kernel0() ->
    %% net_kernel "auto connect" is enabled by default
    ok.
stop_net_kernel0() -> %% disconnect Head<->Tail
    NodeHead = headnode(),
    NodeTail = tailnode(),
    {test0, NodeHead} ! {disconnect, NodeTail},
    io:format(":::net_kernel disconnected ~p/~p~n",[NodeHead,NodeTail]).

stop_net_kernel1() -> %% disconnect Admin<->Tail
    io:format(":::net_kernel:disconnect: ~p:::~n",[net_kernel:disconnect(tailnode())]),
    io:format(":::net_kernel disconnected~n").

stop_net_kernel2() -> %% disconnect Admin<->Head
    io:format(":::net_kernel:disconnect: ~p:::~n",[net_kernel:disconnect(headnode())]),
    io:format(":::net_kernel disconnected~n").

stop_net_kernel3() -> %% disconnect Admin<->Head and Admin<->Tail
    io:format(":::net_kernel:disconnect: ~p:::~n",[net_kernel:disconnect(headnode())]),
    io:format(":::net_kernel:disconnect: ~p:::~n",[net_kernel:disconnect(tailnode())]),
    io:format(":::net_kernel disconnected~n").

stop_net_kernel4() -> %% disconnect Admin<->Head and Head<->Tail
    NodeHead = headnode(),
    NodeTail = tailnode(),
    io:format(":::net_kernel:disconnect: ~p:::~n",[net_kernel:disconnect(headnode())]),
    {test0, NodeHead} ! {disconnect, NodeTail},
    io:format(":::net_kernel disconnected ~p/~p~n",[NodeHead,NodeTail]).

stop_net_kernel5() -> %% disconnect Admin<->Tail and Head<->Tail
    NodeHead = headnode(),
    NodeTail = tailnode(),
    io:format(":::net_kernel:disconnect: ~p:::~n",[net_kernel:disconnect(tailnode())]),
    {test0, NodeHead} ! {disconnect, NodeTail},
    io:format(":::net_kernel disconnected ~p/~p~n",[NodeHead,NodeTail]).

stop_net_kernel6() -> %% disconnect Admin<->Head and Admin<->Tail and Head<->Tail
    NodeHead = headnode(),
    NodeTail = tailnode(),
    io:format(":::net_kernel:disconnect: ~p:::~n",[net_kernel:disconnect(headnode())]),
    io:format(":::net_kernel:disconnect: ~p:::~n",[net_kernel:disconnect(tailnode())]),
    {test0, NodeHead} ! {disconnect, NodeTail},
    io:format(":::net_kernel disconnected ~p/~p~n",[NodeHead,NodeTail]).




bootstrap_chain() ->
    MyBase = atom_to_list(?TAB),
    SchemaFile = MyBase ++ "schema.txt",
    Boot0 = "bootstrap" ++ MyBase,
    NodeHead = headnode(),
    NodeTail = tailnode(),

    ChainDesc = [{?CHAIN,[
                          {?HEAD,NodeHead},
                          {?TAIL,NodeTail}
                         ]}],

    [] = os:cmd("rm -rf hlog." ++ Boot0),
    [] = os:cmd("rm -f " ++ SchemaFile),
    {1,{ok,_}} = {1,brick_admin:create_new_schema([{list_to_atom(Boot0),
                                                    adminnode()}],
                                                  SchemaFile)},
    io:format(":::admin starting with ~p~n",[SchemaFile]),
    {2,ok} = {2,brick_admin:start(SchemaFile)},
    io:format(":::admin started with ~p~n",[SchemaFile]),

    %% wait for admin starts
    timer:sleep(250),
    {3,ok} = {3,brick_admin:add_table(brick_admin, ?TAB, ChainDesc)},

    %% wait for the bootstrap finishes with the table
    timer:sleep(15*1000),

    sync_brick(?TAIL, NodeTail, {tail, {?HEAD, NodeHead}}),
    sync_brick(?HEAD, NodeHead, {head, {?TAIL, NodeTail}}),

    _X0 = ?M:chain_start_repair(?HEAD, NodeHead),
    io:format(":::repair started = ~p~n",[_X0]),

    ok.


print_history() ->
    NodeHead = headnode(),
    NodeTail = tailnode(),
    io:format("::: head history 0 :::~p~n",
              [brick_sb:get_brick_history(?HEAD, NodeHead)]),
    io:format("::: tail history 0 :::~p~n",
              [brick_sb:get_brick_history(?TAIL, NodeTail)]),
    io:format("::: chain history 0 :::~p~n",
              [brick_sb:get_chain_history(?CHAIN)]).

sync_brick(Brick, Node, Pred) ->
    PollFun = fun(Acc) ->
                      case ?M:chain_get_role(Brick, Node) of
                          Pred ->
                              {false, Acc};
                          _St ->
                              io:format(":::polling ~p: ~p~n",
                                        [Pred, _St]),
                              timer:sleep(2500),
                              {true, Acc + 1}
                      end
              end,
    gmt_loop:do_while(PollFun, 0).
