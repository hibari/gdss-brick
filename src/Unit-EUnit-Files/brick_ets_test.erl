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

-module(brick_ets_test).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(MAXKEYS,5000).

-define(KILLTIME,30000).

do_eunit() ->
    S = "setup",
    ?debugVal(S),
    case eunit:test({timeout, 300, ?MODULE}) of
        ok -> ok;
        _ -> erlang:halt(1)
    end.

%%%----------------------------------------------------------------------
%%% TESTS
%%%----------------------------------------------------------------------

all_tests_test_() ->
    {setup,
     fun test_setup/0,
     fun test_teardown/1,
     [
      ?_test(test_scavenger_get_keys()),
      ?_test(test_data_integrity())
     ]}.

test_setup() ->
    random:seed(erlang:now()),
    gmt_config_svr:set_config_value(brick_max_log_size_mb,"1"),
    brick_admin:bootstrap_local([], true, $/, 3, 1, 1, []),
    Nodes = [node()],
    GDSSAdmin = node(),
    ChainLen = 1,
    create_tables(Nodes, ChainLen, GDSSAdmin, 0, 0),
    wait_for_tables(),
    [ T= ets:new(T,[ordered_set,named_table,public]) || {T,[],true} <- all_tables() ],
    offset_table = ets:new(offset_table,[ordered_set,named_table,public]),
    load_tables(?KILLTIME),
    ok.

test_data_integrity() ->
    random:seed(erlang:now()),
    [ ok = test_data_integrity(T,10) || {T,[],true} <- all_tables()].

test_data_integrity(_T,0) ->
    ok;
test_data_integrity(T,N) ->
    K = make_key(random:uniform(?MAXKEYS)),
    [{K,V}] = ets:lookup(T,K),
    ?debugVal(V),
    ?debugVal(T),
    ?debugVal(K),
    {ok,_,V2} = brick_simple:get(T,K),
    ?debugVal(V2),
    ?assert(binary_to_list(V) == binary_to_list(V2)),
    test_data_integrity(T,N-1)
    .

test_read_hlog() ->
    FirstOff = 12+16,
    Hlogs = filelib:wildcard("hlog.commonLogServer/s/*.HLOG"),
    Sizes = lists:map(fun filelib:file_size/1,Hlogs),
    LgsAndSz = lists:zip(Hlogs,Sizes),
    {LogFile,_Max} = lists:foldl(fun({File,Size},{F,S}) ->
                                         if Size > S -> 
                                                 {File,Size};
                                            true ->
                                                 {F,S}
                                            end
                                         end,{"",0},LgsAndSz),
    Hdr = gmt_hlog:file_header_version_1(),
    {ok, FH} = file:open(LogFile, [binary, read, raw]),
    {ok,Hdr} = file:pread(FH,0,size(Hdr)),
    {ok,Bin} = file:pread(FH,size(Hdr),16),
    {_HunkLen,LenBinT,_TypeNum} =  match_hnk_hdr(Bin),
    {HL,NewOff} = get_new_hunk_off(FH, FirstOff, LenBinT),
    _Res = test_read_hlog(FH,NewOff,HL),
    ok.

match_hnk_hdr(Bin) ->
  <<_:4/binary, HunkLen:32, LenBinT:32, TypeNum:32>> = Bin,
    {HunkLen,LenBinT,TypeNum}.

get_new_hunk_off(FH, Off, LenBinT) ->
    case read_offset_size(FH,Off,LenBinT) of
        {[],[],[B]} ->
            {[read_offset_size(FH,Off+LenBinT,B)],Off+LenBinT+B};
        {[],[C],[]} ->
            {[read_offset_size(FH,Off+LenBinT,C)],Off+LenBinT+C};
        {[],[C],[B]} ->
            {[read_offset_size(FH,Off+LenBinT,C),
              read_offset_size(FH,Off+LenBinT+C,B)],Off+LenBinT+C+B};
        {[Md5],[C],[]} ->
            {ok,Bin} = file:pread(FH,Off+LenBinT,C),
            ets:insert(offset_table,{Off+LenBinT,Md5,Bin}),
            {[{Md5,Bin}],Off+LenBinT+C}
    end.


test_read_hlog(FH,Off,Acc) ->
    case file:pread(FH,Off,16) of
        {ok,Bin} ->
            {_HunkLen,LenBinT,_TypeNum} =  match_hnk_hdr(Bin),
            {HL,NewOff} = get_new_hunk_off(FH, Off+16, LenBinT),
             test_read_hlog(FH,NewOff,lists:append(Acc,HL))

                ;
        END -> [END|Acc]
    end.

read_offset_size(FH,Off,Sz) ->
    {ok,Bin} = file:pread(FH,Off,Sz),
    binary_to_term(Bin).

test_teardown(_) ->
    application:stop(gdss),
    ok.

make_data_set() ->
     lists:map(fun(I) ->
                       { list_to_binary(lists:flatten(io_lib:format("/~w",[I]))),
                         list_to_binary(lists:flatten(io_lib:format("~w",[random:uniform(?MAXKEYS)]))) }
               end,
               lists:seq(1,?MAXKEYS)).

load_table(Tab,Pid) ->
     ets:foldl(fun({K,V},_Acc) ->
                        brick_simple:set(Tab,K,V)
               end,[],Tab),
    Pid ! Tab.

load_tables(N) ->
    _KP = spawn(?MODULE,restart_gdss,[N]),
    [ true = ets:insert(T,make_data_set()) || { T, [], true} <- all_tables() ],
    Pids = [ spawn(?MODULE,load_table,[T,self()])
             ||  { T, [], true} <- all_tables() ],
    wait_receive(length(Pids)).

wait_receive(0) ->
    ok;
wait_receive(N) ->
    receive

        t_1_tab ->
            wait_receive(N-1);
        t_2_tab ->
            wait_receive(N-1);
        t_3_tab ->
            wait_receive(N-1);
        t_4_tab ->
            wait_receive(N-1)
    after
        60000 ->
            application:start(gdss)
    end.

test_scavenger_get_keys() ->

    ok.

restart_gdss(N) ->
    timer:sleep(N),
    application:stop(gdss).
 

all_tables() ->
    [
     {t_1_tab, [], true},
     {t_2_tab, [], true},
     {t_3_tab, [], true},
     {t_4_tab, [], true}
    ].

create_tables(Nodes, ChainLen, GDSSAdmin, NumNodesPerBlock, BlockMultFactor)
  when is_list(Nodes),
       is_integer(NumNodesPerBlock), is_integer(BlockMultFactor) ->
    lists:map(
      fun({Tab, _Opts, BigP}) ->
              ChDesc = rpc:call(GDSSAdmin, brick_admin, make_chain_description,
                                [Tab, ChainLen, Nodes,
                                 NumNodesPerBlock, BlockMultFactor]),
              ChWeights = [{Ch, 100} || {Ch, _} <- ChDesc],
              ok = rpc:call(GDSSAdmin, brick_admin, add_table,
                            [{global,brick_admin},
                             Tab,
                             ChDesc,
                             [{hash_init, fun brick_hash:chash_init/3},
                              {new_chainweights, ChWeights},
                              {prefix_method, var_prefix},
                              {prefix_separator, $/},
                              {num_separators, 2},
                              {implementation_module, brick_ets},
                              if BigP -> {bigdata_dir, "."};
                                 true -> ignored_property
                              end,
                              {do_logging, true},
                              {do_sync, true}]
                            ])
      end, all_tables()),
    ok = rpc:call(GDSSAdmin, brick_admin, spam_gh_to_all_nodes, []),
    ok.

wait_for_tables() ->
    wait_for_tables(node()).

wait_for_tables(GDSSAdmin) ->
    [ gmt_loop:do_while(fun poll_table/1, {GDSSAdmin,not_ready,Tab})
      || {Tab,_,_} <- all_tables() ].

poll_table({GDSSAdmin,not_ready,Tab}) ->
    TabCh = gmt_util:atom_ify(gmt_util:list_ify(Tab) ++ "_ch1"),
    case rpc:call(GDSSAdmin, brick_sb, get_status, [chain, TabCh]) of
        {ok, healthy} ->
            {false, ok};
        _ ->
            ok = timer:sleep(250),
            {true, {GDSSAdmin,not_ready,Tab}}
    end.

make_key(I) when is_integer(I) ->
    list_to_binary(lists:flatten(io_lib:format("/~w",[I]))).



