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
%%% File    : test_xref.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(test_xref).

-compile(export_all).

%%%----------------------------------------------------------------------

genAll() ->
    catch xref:stop(xref),
    {ok, XRef, Mods} = start(),

    %% cleanup old ones
    os:cmd(io_lib:format("rm -f ./callgraphs; mkdir -p ./callgraphs", [])),

    %% generate new ones
    [ begin
          CG = cgA(XRef, [Mod]),
          cg2dot(CG, io_lib:format("./callgraphs/~s.dot", [Mod])),
          os:cmd(io_lib:format("dot -Tsvg ./callgraphs/~s.dot -o ./callgraphs/~s.svg", [Mod, Mod]))
      end || Mod <- Mods ].


%%%----------------------------------------------------------------------

%% @spec () -> reference()
%% @doc start xref tool (for GDSS)

start() ->
    %% hard-coded
    Root = "../../../../",
    Dirs = ["src/erl-tools/gmt-bom__HEAD/ebin"
            , "src/erl-tools/gmt-eqc__HEAD/ebin"
            , "src/erl-apps/gmt-util__HEAD/ebin"
            , "src/erl-apps/partition-detector__HEAD/ebin"
            , "src/erl-apps/cluster-info__HEAD/ebin"
           ],
    Mods = [brick_admin
            , brick_admin_event_h
            , brick_admin_sup
            , brick_bp
            , brick_brick_sup
            , brick_chainmon
            , brick_cinfo
            , brick_client_data_sup
            , brick_client
            , brick_clientmon
            , brick_client_sup
            , brick_data_sup
            , brick
            , brick_ets
            , brick_hash
            , brick_itimer
            , brick_mboxmon
            , brick_migmon
            , brick_mon_sup
            , brick_pingee
            , brick_sb
            , brick_server
            , brick_shepherd
            , brick_simple
            , brick_squorum
            , brick_sub_sup
            , brick_sup
            , brick_ticket
            , gmt_hlog_common
            , gmt_hlog
            , gmt_hlog_local
           ],
    {ok,XRef} = xref:start(xref),
    xref:set_default(XRef, [{verbose,false}, {warnings,false}]),
    xref:add_release(XRef, code:lib_dir(), {name,otp}),
    [ {ok,_} = xref:add_directory(XRef, Root ++ Dir) || Dir <- Dirs ],
    [ {ok,_} = xref:add_module(XRef, "../ebin/" ++ atom_to_list(Mod) ++ ".beam") || Mod <- Mods ],
    {ok, XRef, Mods}.


%%%----------------------------------------------------------------------

cgA(XRef, Mods) ->
    {ok,CG} = xref:q(XRef, "E ||| (" ++ gmt_util:join([ atom_to_list(M) || M <- Mods ], "+") ++ ")"),
    CG.


%%%----------------------------------------------------------------------

cg2dot(CG, Filename) ->
    SortedCG = lists:usort(CG),

    %% break into module buckets
    Fun1 = fun({{FMod,FFun,FArity},{TMod,TFun,TArity}}, Acc) ->
                   dict:append(TMod, {TFun,TArity}, dict:append(FMod, {FFun,FArity}, Acc))
           end,
    Dict = lists:foldl(Fun1, dict:new(), SortedCG),

    %% sort values of module buckets
    Fun2 = fun(Key, Value, Acc) ->
                   [{Key, lists:usort(Value)}|Acc]
           end,
    List = dict:fold(Fun2, [], Dict),

    %% open Filename
    case file:open(Filename, [write]) of
        {ok, FD} ->
            %% header
            io:fwrite(FD, "digraph cg { nodesep=0.5; ~n", []),

            %% nodes and internal edges
            [ begin
                  io:fwrite(FD, "  subgraph cluster~s { ~n", [Mod]),
                  io:fwrite(FD, "    label=\"~4294967295p\"; ~n", [Mod]),
                  io:fwrite(FD, "    color=blue; ~n", []),

                  [ io:fwrite(FD, "    \"~4294967295p:~4294967295p/~4294967295p\" [label=\"~4294967295p/~4294967295p\"]; ~n",
                              [Mod, Fun, Arity, Fun, Arity]) || {Fun, Arity} <- FAs ],

                  [ io:fwrite(FD, "    \"~4294967295p:~4294967295p/~4294967295p\" ->  \"~4294967295p:~4294967295p/~4294967295p\"; ~n",
                              [Mod, FFun, FArity, Mod, TFun, TArity])
                    || {{FMod,FFun,FArity},{TMod,TFun,TArity}} <- SortedCG, FMod =:= Mod, TMod =:= Mod ],

                  io:fwrite(FD, "  } ~n", [])
              end || {Mod, FAs} <- List ],


            %% external edges
            [ io:fwrite(FD, "  \"~4294967295p:~4294967295p/~4294967295p\" ->  \"~4294967295p:~4294967295p/~4294967295p\"; ~n",
                        [FMod, FFun, FArity, TMod, TFun, TArity])
              || {{FMod,FFun,FArity},{TMod,TFun,TArity}} <- SortedCG, FMod =/= TMod ],

            %% footer
            io:fwrite(FD, "} ~n", []),

            %% close Filename
            file:close(FD);
        Err ->
            exit(Err)
    end.
