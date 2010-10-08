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
%%% File    : test_scav.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(test_scav).

-export([test_all/0, node2/0, node3/0]).
-export([test_scav/1, test_get_keys/1, get_busy/0, test_get_many/1]).

-record(hunk_summ, {seq,off,type,len,first_off,c_len,u_len,md5s,c_blobs,u_blobs}).

-define(BRICK__GET_MANY_FIRST, '$start_of_table').
-define(BIGKEY, <<"big one">>).
-define(BIGSIZE, (1000*1000)).
-define(TOUTSEC, 5*1000).

test_all() ->
    register(test0, self()),
    %% gmt_util:dbgon(brick_ets, scavenger_get_many),
    %% gmt_util:dbgon(brick_ets),
    %% gmt_util:dbgadd(gmt_hlog_common, spawn_future_tasks_after_dirty_buffer_wait),

    Tab = init0(),

    %% note: run only one of tests below(node3 for get_busy() runs only once)
    %% test_get_many(Tab),
    test_get_keys(Tab),
    %% test_scav(Tab),

    ok.

get_busy() ->
    register(test0, self()),

    MyBase = "testtable",
    Tab = list_to_atom(MyBase),
    Br = list_to_atom(atom_to_list(Tab)++"_"++"ch1_b1"),

    receive node2_ready ->
        noop
    end,
    io:format("### test0 node2_ready received ###~n"),
    get_busy0(Br).

get_busy0(Br) ->
    Key = ?BIGKEY,
    Flags = [],
    Message = {do, now(), [brick_server:make_get(Key, Flags)], []},
%%    Message = {status},
    io:format(":::message=~p:::~n",[Message]),
    lists:foreach(fun(_) ->
              {Br, node2()} ! {'$gen_call', {self(),node()}, Message}
          end, lists:seq(1,1000)),
    timer:sleep(10*1000),
    {messages,L} = process_info(self(), messages),
    io:format(":::qlen=~p:::~n",[length(L)]),
    receive_all(),
    io:format(":::all received:::~n"),
    {test0, node2()} ! node3_done,
    receive
    node2_ready ->
        io:format(":::node2 done:::~n")
    after 300*1000 ->
        io:format(":::fin:::~n"),
        exit(normal)
    end,
    get_busy0(Br).

receive_all() ->
    %% receive _R ->
    %%      io:format(":::ttt:::~p:::~n",[size(term_to_binary(_R))])
    %% after 0 ->
    %%      io:format(":::timeout:::~n")
    %% end.
    receive
        node2_ready ->
        ok;
        _ ->
            receive_all()
    after 0 ->
            ok
    end.

test_get_many(Tab) ->
    TestFun0 =
    fun(_) ->
        case catch brick_simple:get_many(Tab, <<"1">>, 1,[get_many_raw_storetuples,ignore_role], ?TOUTSEC) of
            {ok,_} ->
            true;
            {'EXIT', {timeout, _}} ->
            io:format(":::get_many: timeout ################~n"),
            false;
            _E ->
            io:format(":::get_many: error(~p):::~n", [_E]),
            false
        end
    end,
    TestFun1 =
    fun() ->
        %% gmt_util:dbgon(brick_ets, get_many1),
        %% gmt_util:dbgadd(brick_ets, get_many1b),
        %% gmt_util:dbgadd(brick_ets, handle_call),
        %% gmt_util:dbgadd(brick_ets, do_do),
        %% gmt_util:dbgadd(brick_ets, log_mods),
        %% gmt_util:dbgadd(brick_simple, get_many),
        %% gmt_util:dbgadd(brick_simple),
        %% gmt_util:dbgadd(brick_server, handle_call),
        %% gmt_util:dbgadd(brick_server, handle_call_do_prescreen),
        %% gmt_util:dbgadd(brick_server, handle_call_do_prescreen2),
        %% gmt_util:dbgadd(gen_server, call),

        StartT = erlang:now(),
        io:format("::: get_many starting:::~n"),
        lists:takewhile(TestFun0, lists:seq(1,1000)),
        EndT = erlang:now(),
        Diff = timer:now_diff(EndT,StartT),
        io:format("::: get_many finished in ~B.~3..0B seconds-------~n",
              [Diff div (1000*1000), (Diff div 1000) rem 1000])
    end,
    ThreadsList = [0,0,0,0,0],
    StartKeysList = [0],
    NumKeys = 100,
    BinSize = 10,
    Percent = 50,

    start_test(TestFun1,NumKeys,BinSize,Percent,Tab,
           ThreadsList,StartKeysList),
    ok.

test_get_keys(Tab) ->
    %% NumKeys = 100*1000,
    %% BinSize = 10*1000,
    %% Percent = 50,
    NumKeys = 100*1000,
    BinSize = 10*1000,
    Percent = 50,

    %% without checkpoint
    TestFun1 = fun() -> get_keys0(Tab,false) end,
    %% with checkpoint
    %% TestFun2 = fun() -> get_keys0(Tab,true) end,

    ThreadsList = [0,200,400,600,800,1000],
    StartKeysList = [0, NumKeys, 2*NumKeys, 3*NumKeys,
                 4*NumKeys, 5*NumKeys, 6*NumKeys,
                 7*NumKeys, 8*NumKeys, 9*NumKeys
                ],
    %% ThreadsList = [1000],
    %% StartKeysList = [0],

    start_test(TestFun1,NumKeys,BinSize,Percent,Tab,
           ThreadsList,StartKeysList),
    %% start_test(TestFun2,NumKeys,BinSize,Percent,Tab,
    %%         ThreadsList,StartKeysList),
    ok.

test_scav(Tab) ->

    %% NumKeys = 16*1000,
    %% BinSize = 100*1000,
    %% NumKeys = 160*1000,

    NumKeys = 10*1000,
    BinSize = 100*1000,
    Percent = 20,
    TestFun = fun test0/0,

    ThreadsList = [0],
    StartKeysList = [0],

    start_test(TestFun,NumKeys,BinSize,Percent,Tab,
           ThreadsList,StartKeysList).


start_test(TestFun,NumKeys,BinSize,Percent,Tab,
       ThreadsList,StartKeysList) ->
    lists:foreach(
      fun(StartKey) ->
          {NewNumKeys,Keys} =
          init1(NumKeys,BinSize,Percent,StartKey,Tab),

          %% eprof:start(),
          %% eprof:start_profiling([testtable_ch1_b1],{brick_server,'_','_'}),
          %% eprof:start_profiling([testtable_ch1_b1],{brick_ets,'_','_'}),
          %% fprof:trace([start, {procs, whereis(testtable_ch1_b1)}]),
          lists:foreach(
        fun(Threads) ->
            io:format(":::===starting test with ~p keys, ~p threads, ~p bytes binary===~n",
                  [NewNumKeys,Threads,BinSize]),

            Br = list_to_atom(atom_to_list(Tab)++"_"++"ch1_b1"),
            TooOld = gen_server:call({Br, node2()}, {get_too_old}),
            io:format(":::too old1: ~p~n", [TooOld]),

            if Threads > 0 ->
                start_clients(NewNumKeys,Percent,Tab,Keys,Threads,BinSize),
                timer:sleep(1000); %% wait clients start
               true ->
                noop
            end,
            {test0, node3()} ! node2_ready,
            TestFun(),
            io:format(":::too old2: ~p~n", [TooOld]),
            if Threads > 0 ->
                sync_messages(set2_done,1),
                sync_messages(get2_done,1);
               true ->
                noop
            end,
            io:format(":::too old3: ~p~n", [TooOld]),
            receive
                node3_done ->
                io:format(":::node3 done:::~n")
            end
        end,
        ThreadsList)

          %% ,eprof:stop_profiling(),
          %% eprof:log("/tmp/profile1"),
          %% eprof:analyze(total),
          %% eprof:stop()
          %% ,fprof:trace([stop]),
          %% fprof:profile(),
          %% fprof:analyse(),
          %% fprof:stop()
      end,
      StartKeysList),
    ok.


get_keys0(Tab,StartCheckpoint) ->
    Br = list_to_atom(atom_to_list(Tab)++"_"++"ch1_b1"),
    Fs = [get_many_raw_storetuples],
    First = ?BRICK__GET_MANY_FIRST,
    F_k2d = fun (_Stuple,_Acc) ->
            noop
        end,

    F_lump = fun (_Dict) ->
             noop
         end,

    if StartCheckpoint ->
        brick_server:checkpoint(Br,node());
       true ->
        noop
    end,

    io:format("::: starting get_keys ------------~n"),
    StartT = erlang:now(),
    case catch brick_ets:scavenger_get_keys(Br, Fs, First, F_k2d, F_lump) of
    ok ->
        noop;
    {'EXIT', {timeout, _}} ->
        io:format(":::get_keys0: timeout ################~n");
    _E ->
        io:format(":::get_keys0: error(~p):::~n", [_E])
    end,
    EndT = erlang:now(),
    Diff = timer:now_diff(EndT,StartT),
    io:format("::: get_keys finished in ~B.~3..0B seconds-------~n",
         [Diff div (1000*1000), (Diff div 1000) rem 1000]),
    ok.

init0() ->
    MyBase = "testtable",
    SchemaFile = MyBase ++ "schema.txt",
    Tab = list_to_atom(MyBase),

    BigBin = list_to_binary(lists:duplicate(?BIGSIZE,$b)),

    start_admin(list_to_atom(MyBase++"_"++"bootstrap"),SchemaFile),
    create_table(Tab),
    timer:sleep(5000),
    ok = brick_simple:add(Tab, ?BIGKEY, BigBin, ?TOUTSEC),
    Tab.

init1(Num,BinSize,PerCent,StartKey,Tab) ->
    io:format(":::starting init1=========================~n"),
    check_log("./hlog.commonLogServer"),

    Bin = list_to_binary(lists:duplicate(BinSize,$t)),
    io:format(":::blob size = ~p bytes:::~n", [size(Bin)]),
    Keys = lists:seq(StartKey,StartKey+Num-1),

    io:format(":::adding ~p keys----------------------~n", [Num]),
    ok = add(Tab,split(Keys,100),Bin), %% use only 100 threads
    check_log("./hlog.commonLogServer"),

    if StartKey==0 ->
        io:format(":::setting ~p keys----------------------~n",
              [Num*PerCent div 100]),
        ok = set(Tab,split(Keys,100),Bin,PerCent), %% use only 100 threads
        check_log("./hlog.commonLogServer");
       true ->
        noop
    end,

    {StartKey+Num,Keys}.


test0() ->
    io:format(":::moving to the longterm----------------------~n"),
    timer:sleep(10*1000), %% waite for the move to finish
    check_log("./hlog.commonLogServer"),

    io:format(":::starting scavenger----------------------~n"),
    gmt_hlog_common:start_scavenger_commonlog([]),


    timer:sleep(1*1000), %% wait scav to start
    scav_wait(),
    io:format(":::--- scavenger finished --------------------~n"),

    io:format(":::============================================~n"),
    check_log("./hlog.commonLogServer"),
    io:format(":::============================================~n"),
    ok.


start_clients(Num,Percent,Tab,Keys,Threads,BinSize) ->
    Bin = list_to_binary(lists:duplicate(BinSize,$t)),
    KeysSplit = split(Keys, Threads),

    Parent = self(),
    spawn(
      fun() ->
          io:format(":::setting ~p keys again while running scavenger---~n",
            [Num*Percent div 100]),
          ok = set(Tab,KeysSplit,Bin,Percent),
          io:format(":::--- set finished --------------------~n"),
          Parent ! set2_done
      end),
    spawn(
      fun() ->
          io:format(":::getting ~p keys while running scavenger---~n",
            [Num*Percent div 100]),
          ok = get(Tab,KeysSplit,Percent),
          io:format(":::--- get finished --------------------~n"),
          Parent ! get2_done
      end).



add(Tab,KeysSplit,Bin) ->
    Parent = self(),
    AddFun =
    fun(Keys) ->
        fun() ->
            lists:foreach(
              fun(N) ->
                  B=list_to_binary(
                      integer_to_list(N)),
                  case catch brick_simple:add(Tab, B, Bin, ?TOUTSEC) of
                      ok ->
                      noop;
                      {'EXIT', {timeout, _}} ->
                      io:format(":::add timeout:::~n"),
                      timer:sleep(10*1000);
                      E ->
                      io:format(":::add error: ~p:::~n",[E]),
                      timer:sleep(10*1000)
                  end
              end, Keys),
            Parent ! add_done
        end
    end,

    PidList =
    lists:foldl(
      fun(Keys,Acc) ->
          Pid = spawn(AddFun(Keys)),
          [Pid|Acc]
      end,
      [], KeysSplit),
    sync_messages(add_done, length(PidList)).

sync_messages(_Msg,0) ->
    io:format(":::~p sync done:::~n", [_Msg]),
    ok;
sync_messages(Msg,Num) ->
%%    io:format(":::~p/~p syncing:::~n", [Msg,Num]),
    receive Msg ->
        sync_messages(Msg,Num-1)
    end.

set(Tab,KeysSplit,Bin,PourCent) ->
    Parent = self(),
    SetFun =
    fun(Keys) ->
        fun() ->
            lists:foreach(
              fun(N) ->
                  case N rem 100 of
                      X when X < PourCent ->
                      B=list_to_binary(
                          integer_to_list(N)),
                      case catch brick_simple:set(Tab, B, Bin, ?TOUTSEC) of
                          ok ->
                          noop;
                          {'EXIT', {timeout, _}} ->
                          io:format(":::set timeout:::~n");
                          E ->
                          io:format(":::set error: ~p:::~n",[E])
                      end;
                      _ ->
                      noop
                  end
              end, Keys),
            Parent ! set_done
        end
    end,
    PidList =
    lists:foldl(
      fun(Keys,Acc) ->
          Pid = spawn(SetFun(Keys)),
          [Pid|Acc]
      end,
      [], KeysSplit),
    sync_messages(set_done, length(PidList)).

get(Tab,KeysSplit,PourCent) ->
    Parent = self(),
    GetFun =
    fun(Keys) ->
        fun() ->
            lists:foreach(
              fun(N) ->
                  case N rem 100 of
                      X when X < PourCent ->
                      B=list_to_binary(
                          integer_to_list(N)),
                      case catch brick_simple:get(Tab, B, ?TOUTSEC) of
                          {ok,_,_} ->
                          noop;
                          {'EXIT', {timeout, _}} ->
                          io:format(":::get timeout:::~n");
                          E ->
                          io:format(":::get error: ~p:::~n",[E])
                      end;
                      _ ->
                      noop
                  end
              end, Keys),
            Parent ! get_done
        end
    end,
    PidList =
    lists:foldl(
      fun(Keys,Acc) ->
          Pid = spawn(GetFun(Keys)),
          [Pid|Acc]
      end,
      [], KeysSplit),
    sync_messages(get_done, length(PidList)).

check_log0(Dir,SorL) ->
    gmt_hlog:fold(SorL, Dir,
          fun(#hunk_summ{c_len=CL0, u_len=UL0},_FH,
              {CCount,CBytes,UCount,UBytes}=_TTT) ->
              CL =
                  case CL0 of
                  CL0 when is_list(CL0) -> CL0;
                  _ -> []
                  end,
              UL =
                  case UL0 of
                  UL0 when is_list(UL0) -> UL0;
                  _ -> []
                  end,

              {CCount + length(CL),
               CBytes + lists:foldl(fun (X,Acc) ->
                            X+Acc
                        end,0,
                        CL),
               UCount + length(UL),
               UBytes + lists:foldl(fun (X,Acc) ->
                            X+Acc
                        end,0,
                        UL)
              }
          end, {0,0,0,0}).

check_log(Dir) ->
    {{LongC, LongCBytes, LongU, LongUBytes},[]} =
        check_log0(Dir,longterm),

    io:format("~n:::~p cblobs(~p bytes total):::long~n",
              [LongC, LongCBytes]),
    io:format("~n:::~p ublobs(~p bytes total):::long~n",
              [LongU, LongUBytes]),

    {{ShortC, ShortCBytes, ShortU, ShortUBytes},[]} =
        check_log0(Dir,shortterm),

    io:format("~n:::~p cblobs(~p bytes total):::short~n",
              [ShortC, ShortCBytes]),
    io:format("~n:::~p ublobs(~p bytes total):::short~n",
              [ShortU, ShortUBytes]),

    ok.



start_admin(Bootstrap,SchemaFile) ->
    {ok, _} = brick_admin:create_new_schema(
                [{Bootstrap,node()}],
        SchemaFile),
    ok = brick_admin:start(SchemaFile),
    poll_admin_server_registered().

create_table(Tab) ->
    ChDescr = brick_admin:make_chain_description(
                 Tab, 1, [node()]),
    brick_admin:add_table(Tab, ChDescr,
              [{bigdata_dir,"cwd"},
               {do_logging,true},
               {do_sync,true}
              ]).


poll_admin_server_registered() ->
    Max = 80,
    Fpoll = fun(N) ->
                    case whereis(brick_admin) of
                        undefined when N < Max ->
                            timer:sleep(100),
                            {true, N+1};
                        _ ->
                            {false, N}
                    end
            end,
    gmt_loop:do_while(Fpoll, 0) < Max.

scav_wait() ->
    F_excl_wait =
    fun() ->
        noop
    end,
    F_excl_go =
    fun() ->
        io:format(":::scavenger finished----------~n")
    end,
    brick_ets:really_cheap_exclusion(excl_atom(),
                     F_excl_wait, F_excl_go),
    unregister(excl_atom()). %% for next iteration of test0()

split(List,N) ->
    NN = length(List) div N,
    lists:foldl(fun (X,[]) ->
            [[X]];
            (X,Acc) when (X rem NN)==0 ->
            [[X]|Acc];
            (X,[Car|Cdr]) ->
            [[X|Car]|Cdr]
        end, [], List).

excl_atom() ->
    the_scavenger_proc.

node2() ->
    list_to_atom("gdss_dev2@"++gmt_util:node_right()).

node3() ->
    list_to_atom("gdss_dev3@"++gmt_util:node_right()).

