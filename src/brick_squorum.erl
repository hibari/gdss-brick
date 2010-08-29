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
%%% File    : brick_squorum.erl
%%% Purpose : brick client with simple quorum redundancy
%%%-------------------------------------------------------------------

%% @doc An brick client with with simple quorum redundancy.
%%
%% The intent of this module is provide naive and simple yet robust
%% storage for use by an cluster admin/manager server.  Such a server
%% has limited requirements for persistent data, but it doesn't make
%% sense to store that data on a local disk.  For availability, that
%% data should be spread across multiple bricks.  However, there's a
%% chicken-and-the-egg problem for a cluster manager: if the cluster
%% must be running in order to serve data, how do you start the
%% cluster?
%%
%% The answer (for now) is to have any machine capable of running an
%% administrator to have a statically configured list of bricks that
%% store cluster manager data.  A very simple quorum technique is used
%% for robust storage.
%%
%% No transaction support is provided, since we assume that the
%% manager will use some other mechanism for preventing multiple
%% managers from running simultaneously.
%%
%% NOTE: The other major clients for this module are `brick_sb',
%% the "score board" module.

-module(brick_squorum).
-include("applog.hrl").


-ifdef(debug_squorum).
-define(gmt_debug, true).
-endif.
-include("gmt_debug.hrl").
-include("brick_specs.hrl").

-define(FOO_TIMEOUT, 15*1000).
-define(SINGLE, brick_server).

-export([set/3, set/4, set/6, get/2, get/3, delete/2, delete/3,
         get_keys/3, get_keys/4]).
-export([multiset/2]).
-export([t0/0]).                                % Unit tests

-export_type([brick_list/0, set_res/0]).

-type brick_list() :: list(brick_server:brick()).
-type set_res() :: brick_server:set_reply() | error | quorum_error.
-type get_res() :: brick_server:get_reply() | error | quorum_error.
-type delete_res() :: brick_server:delete_reply() | error | quorum_error.
-type get_keys_res() :: {ok, list(), boolean()}.

-spec set(brick_list(), bin_key(), val()) -> set_res().
%%-spec set(brick_list(), bin_key(), val(), flags_list() | integer()) -> set_res().
-spec set(brick_list(), bin_key(), val(), integer(), flags_list(), integer()) -> set_res().

set(Bricks, Key, Value) ->
    set(Bricks, Key, Value, 0, [], ?FOO_TIMEOUT).

set(Bricks, Key, Value, []) ->
    set(Bricks, Key, Value, 0, [], ?FOO_TIMEOUT);
set(Bricks, Key, Value, Timeout) when is_integer(Timeout) ->
    set(Bricks, Key, Value, 0, [], Timeout).

set(Bricks, Key, Value, ExpTime, Flags, Timeout)
  when is_list(Bricks) ->                       % Assume a list of bricks
    do1(Bricks, ?SINGLE:make_set(Key, Value, ExpTime, Flags),
        Timeout, 0).

multiset(Bricks, Ops) ->
    do_multi(Bricks, Ops, ?FOO_TIMEOUT, 0).

-spec get(brick_list(), bin_key()) -> get_res().
%%-spec get(brick_list(), bin_key(), flags_list() | integer()) -> get_res().
-spec get(brick_list(), bin_key(), flags_list(), integer()) -> get_res().

get(Bricks, Key) ->
    get(Bricks, Key, [], ?FOO_TIMEOUT).

get(Bricks, Key, []) ->
    get(Bricks, Key, [], ?FOO_TIMEOUT);
get(Bricks, Key, Timeout) when is_integer(Timeout) ->
    get(Bricks, Key, [], Timeout).

get(Bricks, Key, [], Timeout)
  when is_list(Bricks) ->                       % Assume a list of bricks
    do1(Bricks, ?SINGLE:make_get(Key, []), Timeout, 0).

-spec delete(brick_list(), bin_key()) -> delete_res().
%%-spec delete(brick_list(), bin_key(), flags_list() | integer()) -> delete_res().
-spec delete(brick_list(), bin_key(), flags_list(), integer()) -> delete_res().

delete(Bricks, Key) ->
    delete(Bricks, Key, [], ?FOO_TIMEOUT).

delete(Bricks, Key, []) ->
    delete(Bricks, Key, [], ?FOO_TIMEOUT);
delete(Bricks, Key, Timeout) when is_integer(Timeout) ->
    delete(Bricks, Key, [], Timeout).

delete(Bricks, Key, [], Timeout)
  when is_list(Bricks) ->                       % Assume a list of bricks
    do1(Bricks, ?SINGLE:make_delete(Key, []), Timeout, 0).

-spec get_keys(brick_list(), bin_key(), integer()) -> get_keys_res().

get_keys(Bricks, Key, Num) ->
    get_keys(Bricks, Key, Num, ?FOO_TIMEOUT).

%% @doc Sort-of like brick_server:get_many() but more relaxed.
%%
%% See comments on primitive_do() for brick filtering assumptions.

get_keys(Bricks, Key, Num, Timeout)
  when is_list(Bricks) ->                       % Assume a list of bricks
    BricksUp = filter_up_bricks_only(Bricks),
    KsLs = lists:map(
             fun({Brick, Node}) ->
                     Flags = [witness],
                     case catch ?SINGLE:get_many(Brick, Node, Key, Num,
                                                 Flags, Timeout) of
                         {ok, {Ks, _}} ->
                             [K || {K, _TS} <- Ks];
                         _ ->
                             []
                     end
             end, BricksUp),
    case ordsets:from_list(lists:append(KsLs)) of
        [] ->
            {ok, [], false};
        Keys ->
            {ok, Keys, true}
    end.

do1(Bricks, Op, Timeout, Resubs)
  when is_list(Bricks), is_integer(Timeout) ->
    MultiCallRes = primitive_do(Bricks, Op, Timeout, true),
    %%?DBG({do1, Op}),
    case catch calc_quorum(MultiCallRes, Op, Bricks) of
        re_submit ->
            %% TODO Make number of resubmissions configurable somehow.
            if Resubs >= 10 ->
                    exit({error, {max_retries, Resubs}});
               true ->
                    %% debugging/testing only: timer:sleep(100),
                    do1(Bricks, Op, Timeout, Resubs + 1)
            end;
        Res ->
            Res
    end.

do_multi(Bricks, OpList, Timeout, _Resubs)
  when is_list(Bricks), is_integer(Timeout) ->
    {NsRs, BadNodes} = primitive_do(Bricks, OpList, Timeout, false),
    Rs = [Res || {_Node, Res} <- NsRs],
    NumBricks = length(Bricks),
    MinQuorum = if NumBricks == 1 -> 1;
                   NumBricks == 2 -> 1;
                   true           -> calc_min_quorum(NumBricks)
               end,
    if length(Rs) >= MinQuorum ->
            case lists:usort(Rs) of
                [_] ->
                    ok;
                _ ->
                    {bummer, do_multi, 1, Rs, BadNodes}
            end;
       true ->
            {bummer, do_multi, 2, Rs, BadNodes}
    end.

%% @doc Mimic gen_server:multi_call() but without the same-registered-name
%% limitation.
%%
%% We filter the list of bricks to include only the bricks that appear
%% in [node()|nodes()].  If this filtering results in too few bricks
%% for a quorum, that's OK: calc_quorum() will figure that out.
%%
%% We rely on some external mechanism for making certain that bricks
%% are added promptly back to our nodes() list.  Specifically, we'll
%% rely on brick_admin's admin_scan_loop() daemon process.

primitive_do(Bricks, Op, Timeout, _StripP) when not is_list(Op) ->
    primitive_do(Bricks, [Op], Timeout, true);
primitive_do(Bricks, OpList, Timeout, StripP) ->
    Self = self(),
    BricksUp = filter_up_bricks_only(Bricks),
    Pids = [spawn(fun() ->
                         Res = (catch ?SINGLE:do(Brick, Name, OpList, Timeout)),
                         Self ! {self(), Res}
                       end) || {Brick, Name} <- BricksUp],
    PidsBricksUp = lists:zip(Pids, BricksUp),
    lists:foldl(
      fun({Pid, Brick}, {ResList, ResBad}) ->
              receive
                  {Pid, {'EXIT', _X}} ->
                      %%?DBG({Pid, _X}),
                      {ResList, [Brick|ResBad]};
                  {Pid, X0} ->
                      X = if StripP -> hd(X0);
                             true   -> X0
                          end,
                      %%?DBG({Pid, X}),
                      {[{Brick, X}|ResList], ResBad}
              end
      end, {[], []}, PidsBricksUp).

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

calc_quorum(MultiCallRes, Op, Servers) when element(1, Op) == set ->
    strict_quorum_answer_set(MultiCallRes, Servers, Op);
calc_quorum(MultiCallRes, Op, Servers) when element(1, Op) == delete ->
    strict_quorum_answer_delete(MultiCallRes, Servers, Op);
calc_quorum(MultiCallRes, Op, Servers) when element(1, Op) == get ->
    strict_quorum_answer_get(MultiCallRes, Servers, Op).

strict_quorum_answer_set(MultiCallRes, Servers, _Op) ->
    %% Rs = [{node(), result()}, ...]
    {Rs, BadNodes} = MultiCallRes,
    {Count, Answer} = popular_answer(Rs, BadNodes),
    NumServers = length(Servers),
    MinQuorum = calc_min_quorum(NumServers),
    NonConformingBricks = find_nonconforming_bricks(Answer, Rs),
    if NonConformingBricks /= [],
       Count >= MinQuorum ->
            %% WARNING: We're going to accept this, but someone else
            %%          is going to be responsible for repairing the
            %%          non-conforming brick(s).
            %% For example:
            %% WHA? Rs = [{{bootstrap_copy2,brick_dev@newbb},ok},
            %%            {{bootstrap_copy1,brick_dev@newbb},ok},
            %%            {{bootstrap_copy3,test_down@newbb},{error,current_role,undefined}}]
            Answer;
       NonConformingBricks /= [] ->
            if length(NonConformingBricks) == length(Servers) ->
                    failed;
               true ->
                    ?APPLOG_INFO(?APPLOG_APPM_067,"quorum_error: Op = ~P\nNonConformingBricks = ~p\nServers = ~p\n", [_Op, 20, NonConformingBricks, Servers]),
                    quorum_error
            end;
       Count >= MinQuorum ->
            Answer;
       true ->
            ?APPLOG_INFO(?APPLOG_APPM_068,"quorum_error: Op = ~P\n"
                         "true/default clause A: ~p ~p ~p ~p\n",
                         [_Op, 20, Count, MinQuorum, Rs, BadNodes]),
            quorum_error
    end.

strict_quorum_answer_delete(MultiCallRes, Servers, Op) ->
    {Rs, BadNodes} = MultiCallRes,
    {Count, Answer} = popular_answer(Rs, BadNodes),
    NumServers = length(Servers),
    MinQuorum = calc_min_quorum(NumServers),
    NonConformingBricks = find_nonconforming_bricks(Answer, Rs),
    if NonConformingBricks /= [] ->
            BadRs = lists:map(
                      fun(Brick) ->
                              {value, T} = lists:keysearch(Brick, 1, Rs),
                              T
                      end, NonConformingBricks),
            case sort_answers_by_popularity(BadRs) of
                [{_, _}] ->
                    %% If all non-conforming say key_not_exist, then the key
                    %% didn't exist ... and still doesn't exist, as far as
                    %% we're concerned, so the popular answer is OK.
                    %% If all non-conforming say ok, then the key was
                    %% successfully deleted on those bricks ... and are
                    %% still deleted, as far as we're concerned, so the
                    %% popular answer is still OK.
                    Answer;
                _X ->
                    ?APPLOG_ALERT(?APPLOG_APPM_069,"WHA? Op = ~p\n", [Op]),
                    ?APPLOG_ALERT(?APPLOG_APPM_070,"WHA? Rs = ~p\n", [Rs]),
                    ?APPLOG_ALERT(?APPLOG_APPM_071,"WHA? NonConformingBricks = ~p\n", [NonConformingBricks]),
                    ?APPLOG_ALERT(?APPLOG_APPM_072,"WHA? _X = ~p\n", [_X]),
                    foo_error
            end;
       Count >= MinQuorum ->
            Answer;
       true ->
            ?APPLOG_INFO(?APPLOG_APPM_073,"quorum_error: Op = ~P\n"
                         "true/default clause B: ~p ~p ~p ~p\n",
                         [Op, 20, Count, MinQuorum, Rs, BadNodes]),
            quorum_error
    end.

strict_quorum_answer_get(MultiCallRes, Servers, Op) ->
    {Rs, BadNodes} = MultiCallRes,
    {Count, Answer} = popular_answer(Rs, BadNodes),
    NumServers = length(Servers),
    MinQuorum = calc_min_quorum(NumServers),
    NonConformingBricks = find_nonconforming_bricks(Answer, Rs),
    if Count >= MinQuorum,
       NonConformingBricks /= [] ->
            %% Strategy: Repair one single non-conforming brick, then
            %% rely on the re-submission mechanism to retry and, if
            %% necessary, fix any other non-conforming bricks.
            %%
            %% We need to be very careful to avoid clobbering data
            %% that is updated via races with other entities.  We must
            %% use 'testset' to delete/replace only the thing that we
            %% want to delete/replace and not delete/replace anything
            %% newer.
            %%
            %% This strategy allows us not to care if the repair op
            %% succeeds or fails: the re-submission and quorum
            %% mechanism will sort everything out.
            %%
            Key = element(2, Op),
            BadBrick = hd(NonConformingBricks),
            {value, {BadBrick, BadAnswer}} = lists:keysearch(BadBrick, 1, Rs),
            {BadBr, BadNd} = BadBrick,
            case Answer of
                {ok, TS, Val} ->                % This key should exist
                    FixOp = case BadAnswer of
                                {ok, BadTS, _} when BadTS > TS ->
                                    %% Minority has newer timestamp: an update
                                    %% was interrupted somehow.
                                    ?SINGLE:delete(BadBr, BadNd, Key,
                                                   [{testset, BadTS}]),
                                    ?SINGLE:make_op6(add, Key, TS, Val, 0, []);
                                {ok, BadTS, _} ->
                                    ?SINGLE:make_op6(replace, Key, TS, Val, 0,
                                                     [{testset, BadTS}]);
                                key_not_exist ->
                                    ?SINGLE:make_op6(add, Key, TS, Val, 0, []);
                                Res1 ->
                                    throw({quorum_error1, Res1})
                            end,
                    %%?DBG(element(3, FixOp)),
                    %%?DBG(element(6, FixOp)),
                    %%?DBG({BadBr, BadNd}),
                    %%?DBG(?SINGLE:do(BadBr, BadNd, [FixOp])),
                    ?SINGLE:do(BadBr, BadNd, [FixOp]);
                key_not_exist ->                % This key should be deleted
                    FixOp = case BadAnswer of
                                {ok, BadTS, _} ->
                                    ?SINGLE:make_delete(Key,
                                                        [{testset, BadTS}]);
                                %%                              key_not_exist ->
                                %%                                  %% This is the correct answer.
                                Res2 ->
                                    throw({quorum_error2, Res2})
                            end,
                    %%?DBG(FixOp),
                    ?SINGLE:do(BadBr, BadNd, [FixOp]);
                Res ->
                    %% Don't throw an error here: we may have had something
                    %% happen like catching a brick that's in state or role
                    %% transition.
                    ?APPLOG_INFO(?APPLOG_APPM_074,"quorum get.3: ~p: ~p\n",
                                 [BadBrick, Res]),
                    timer:sleep(250)
            end,
            re_submit;
       Count >= MinQuorum ->
            Answer;
       true ->
            ?APPLOG_INFO(?APPLOG_APPM_075,"quorum_error: Op = ~P\n"
                         "true/default clause C: ~p ~p ~p ~p\n",
                         [Op, 20, Count, MinQuorum, Rs, BadNodes]),
            quorum_error
    end.

%% @doc Calculate the most popular answer.
%%
%% The sort_answers_by_popularity() func, together with Erlang term sorting
%% rules, do not give us the behavior that we want for the special case of
%% length(Servers) == 2.  If there are two servers, and if we get answers from
%% both of them (2nd arg = BadNodes == []), and if one says key_not_exist and
%% the other says, {ok, ...}, then we want {ok, ...} to be "most popular".
%%
%% Translation: for quorums of exactly 2, if only 1 brick has key X, do not
%% delete X on the other brick, replicate it.

popular_answer([{_, key_not_exist}, {_, T}], [])
  when T == ok orelse element(1, T) == ok ->
    {2, T};
popular_answer([{_, T}, {_, key_not_exist}], [])
  when T == ok orelse element(1, T) == ok ->
    {2, T};
popular_answer([], _BadNodes) ->
    {0, error};
popular_answer(L, _BadNodes) ->
    hd(sort_answers_by_popularity(L)).

%% Mimic "cat L | sort | uniq -c | sort -nr | head -1"
sort_answers_by_popularity(L) ->
    L2 = sort_answers(L),
    lists:sort(fun({X, _}, {Y, _}) -> X > Y end, uniq_minus_c(L2)).

sort_answers(L) ->
    lists:sort(fun({_, X}, {_, Y}) -> X > Y end, L).

uniq_minus_c([{_, First}|T]) ->
    uniq_minus_c(T, First, 1).

uniq_minus_c([], Last, Count) ->
    [{Count, Last}];
uniq_minus_c([{_, Last}|T], Last, Count) ->
    uniq_minus_c(T, Last, Count + 1);
uniq_minus_c([{_, Current}|T], Last, Count) ->
    [{Count, Last}|uniq_minus_c(T, Current, 1)].

find_nonconforming_bricks(Answer, Rs) ->
    lists:foldl(fun({_Brick, A}, Acc) when A == Answer   -> Acc;
                   ({Brick, _BadA}, Acc)                 -> [Brick|Acc]
                end, [], Rs).

t0() ->
    Rs0 = [],
    Rs1 = [{h1, ok}],
    Rs2a = [{h1, {ok, foo}}, {h2, {ok, foo}}],
    Rs2b = [{h1, key_not_exist}, {h2, {ok, foo}}],
    Rs2c = [{h1, key_not_exist}, {h2, key_not_exist}],
    Rs3a = [{h1, ok}, {h2, ok}, {h3, ok}],
    Rs3b = [{h1, ok}, {h2, err}, {h3, ok}],
    Rs3c = [{h1, ok}, {h2, err}, {h3, err}],
    Rs3d = [{h1, err}, {h2, err}, {h3, err}],
    Times = lists:seq(1,100),

    [{0, error} = popular_answer(shuffle(Rs0), []) || _ <- Times],

    [{1, ok} = popular_answer(shuffle(Rs1), []) || _ <- Times],

    %% Special case of QSize = 2
    [{2, {ok, foo}} = popular_answer(shuffle(Rs2a), []) || _ <- Times],
    [{2, {ok, foo}} = popular_answer(shuffle(Rs2b), []) || _ <- Times],
    [{2, key_not_exist} = popular_answer(shuffle(Rs2c), []) || _ <- Times],
    %% These are not really QSize = 2, they're paranoid tests of non-empty
    %% failed node list (for total = 3).
    H3 = {h3, timeout},
    [{2, {ok, foo}} = popular_answer(shuffle(Rs2a), [H3]) || _ <- Times],
    [{1, key_not_exist} = popular_answer(shuffle(Rs2b), [H3]) || _ <- Times],
    [{2, key_not_exist} = popular_answer(shuffle(Rs2c), [H3]) || _ <- Times],

    [{3, ok} = popular_answer(shuffle(Rs3a), []) || _ <- Times],
    [{2, ok} = popular_answer(shuffle(Rs3b), []) || _ <- Times],
    [{2, err} = popular_answer(shuffle(Rs3c), []) || _ <- Times],
    [{3, err} = popular_answer(shuffle(Rs3d), []) || _ <- Times],

    ok.

shuffle(L) ->
    lists:sort(fun(_, _) -> random:uniform(100) =< 50 end, L).

-spec filter_up_bricks_only(brick_list()) -> brick_list().
filter_up_bricks_only(Bricks) ->
    Nodes = [node()|nodes()],
    lists:filter(fun({_, N}) -> lists:member(N, Nodes) end, Bricks).

calc_min_quorum(NumBricks) ->
    ((NumBricks - 1) div 2) + 1.
