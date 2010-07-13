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
%%% File    : brick_test0.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(brick_test0).

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
-define(SQ, brick_squorum).
-define(TEST_NAME,      regression_test0).
-define(TEST_NAME_STR, "regression_test0").
-define(BIGDATA_DIR,   "./regression-bigdata-dir").

-define(NUM_QC_TESTS_DEFAULT_A, '500').

-compile(export_all).

%% * Ad hoc developer testing.  Examples:
%%
%% * Run single-brick regression test
%%
%% ok = brick_test0:single_brick_regression().
%% ok = brick_test0:chain_all().
%%
%% * Start remote bricks via:
%%
%% brick_shepherd:start_brick(regression_test0, []).
%%
%% * Then:
%%
%% brick_test0:old_distrib_test(['brick_dev@bb2-2', 's2@bb2-2'],
%%                                  regression_test0, 10, simple).
%%
%% brick_test0:simple_distrib_test(['brick_dev@bb2-2', 's2@bb2-2'],
%%                                     regression_test0, 50, 5, simple).
%%
-export([single_brick_regression/0, multiple_brick_regression/2,
         simple_distrib_test/5, old_distrib_test/4]).

%% Command line ("erl -s") testing of same
-export([cl_chain_t35/0, cl_single_brick_regression/0, cl_chain_all/0]).
-export([cl_simple_distrib_test/1, cl_old_distrib_test/1]).

%% Command line ("erl -s") testing of QuickCheck-based tests.
-export([cl_qc_hlog_local_qc__t1/0, cl_qc_hlog_local_qc__t1/1,
         cl_qc_hlog_qc__t1/0, cl_qc_hlog_qc__t1/1,
         cl_qc_hlog_qc__t2/0, cl_qc_hlog_qc__t2/1,
         cl_qc_hlog_qc_blackbox__t1/0, cl_qc_hlog_qc_blackbox__t1/1,
         cl_qc_my_pread_qc__t1/0, cl_qc_my_pread_qc__t1/1,
         cl_qc_repair_qc__t1/0, cl_qc_repair_qc__t1/1,
         cl_qc_repair_qc__t2/0, cl_qc_repair_qc__t2/1,
         cl_qc_simple_qc__t1/0, cl_qc_simple_qc__t1/1,
         cl_qc_squorum_qc__t1/0, cl_qc_squorum_qc__t1/1]).

-export([t1/2, t2/2, t3/2, t4/2, t5/2, t7/2, t8/2, t9/2, t10/2,
         t50/4, t51/5, t60/3, t90/2, t91/2, t91/3]).
-export([chain_all/0, chain_all/1, chain_all2/1,
         chain_t0/1, chain_t10/1, chain_t10/7,
         chain_t20/1, chain_t25/1, chain_t30/1, chain_t31/1, chain_t35/1,
         chain_t40/1, chain_t41/1, chain_t50/1, chain_t50_common/3,
         chain_t51/1, chain_t51_common/3, chain_t54/1, chain_t55/0,
         chain_t55/1, chain_t56/1,
         chain_t60/1, chain_t61/1, chain_t62/1]).
-export([chain_t100/0, chain_t101/0, chain_t102/0, chain_t103/0]).
-export([chain_start_entire_chain/3, chain_stop_entire_chain/1]).
%% Debug
-export([chop_list/2, perms/1, keyuniq/2]).
-export([exp0/0, exp0/3]).
-export([make_brick_option_lists/0, make_brick_option_lists/1]).
-export([xform_ets_dump/2,
         get_chain_last_ack/2, get_chain_last_ack_different/4]).

%% Internal exports
-export([loopN/2, loopNverbose/2, loop_msg_stop/2]).

single_brick_regression() ->
    catch file:make_dir(?BIGDATA_DIR),
    OptLists = make_brick_option_lists(),

    start_and_flush_common_log(),
    [ok = single_brick_regression2(OptList) || OptList <- OptLists],
    io:format("Single brick regression tests PASS\n").

make_brick_option_lists() ->
    make_brick_option_lists(
      [{do_logging, true}, {do_logging, false},
       {do_sync, true}, {do_sync, false},
       {bigdata_dir, undefined}, {bigdata_dir, ?BIGDATA_DIR}
      ]).

make_brick_option_lists(AllProps) ->
    Perms = perms(AllProps),
    %% A bit dorky, but lazy: find all unique permutations.
    %% Fortunately for us, order of each proplist doesn't matter.
    OptLists0 = [lists:sort(keyuniq(1, PropList)) || PropList <- Perms],
    lists:usort(OptLists0).

single_brick_regression2(OptionList) ->
    io:format("Single brick regression tests starting with brick options ~p\n\n", [OptionList]),

    Files = filelib:wildcard("*." ++ ?TEST_NAME_STR ++ ",*.LOG", "."),
    [file:delete(F) || F <- Files],

    os:cmd("rm -rf hlog.*" ++ ?TEST_NAME_STR ++ "*"),
    Fstart = fun() ->
                     start_and_flush_common_log(),
                     os:cmd("rm -rf hlog.*" ++ atom_to_list(?TEST_NAME) ++ "*"),
                     {ok, _Server} = safe_start_link(?TEST_NAME, OptionList),
                     ok = ?M:chain_role_standalone(?TEST_NAME, node()),
                     ok = ?M:chain_set_my_repair_state(?TEST_NAME, node(), ok)
             end,
    ok = Fstart(),

    Fseveral = fun() ->
                       ok = t1(?TEST_NAME, node()),
                       ok = t4(?TEST_NAME, node()),
                       ok = t5(?TEST_NAME, node()),
                       ok = t9(?TEST_NAME, node()),
                       ok
               end,
    ok = Fseveral(),

    %% Do the same while in the middle of a checkpoint.
    %% Fseveral() should still work correctly.
    brick_server:checkpoint(?TEST_NAME, node(),
                                [{start_sleep_time, 1000}]),
    timer:sleep(200),
    ok = Fseveral(),

    %% get_many testing
    ok = t2(?TEST_NAME, node()),
    ok = t3(?TEST_NAME, node()),
    ok = t4(?TEST_NAME, node()),

    %% must_exist/must_not_exist testing and ssf (server-side fun).
    ok = t7(?TEST_NAME, node()),

    %% Expiry regression
    ok = t8(?TEST_NAME, node()),
    %% BZ 22015 regression (ssf and ?VALUE_REMAINS_CONSTANT bug)
    ok = t10(?TEST_NAME, node()),

    %% Items 90-99 can stop and start the test brick.
    ok = t90(?TEST_NAME, node(), OptionList, false),
    ok = t90(?TEST_NAME, node(), OptionList, true),
    ok = Fstart(),
    ok = t91(?TEST_NAME, node(), OptionList),

    %% Scavenger regress

    catch ?M:stop(?TEST_NAME),
    io:format("Single brick regression tests PASS with brick options ~p\n\n", [OptionList]),
    ok.

multiple_brick_regression(Nodes, BrickName) ->
    ok = t60(Nodes, BrickName, 666).            % definitely broken

old_distrib_test(Nodes, BrickName, NumIters, RedundancyArg)
  when RedundancyArg == simple ->
    t50(Nodes, BrickName, NumIters, RedundancyArg).

simple_distrib_test(Nodes, BrickName, NumProcs, PhaseTime, RedundancyArg)
  when RedundancyArg == simple ->
    io:format("WARNING: running this test with too many procs\n"),
    io:format("*and* too short a runtime to create at least 10 records\n"),
    io:format("per thread can result in bogus crashes.\n"),
    t51(Nodes, BrickName, NumProcs, PhaseTime, RedundancyArg).

t1(BrickName, Node) ->
    io:format("Test t1: start\n"),

    %% Test basic add, replace, set

    ok = ?M:set(BrickName, Node, "foo", "foo2"),
    %S1 = ?M:dump_state(Node),
    %io:format("State 1 = ~w\n", [S1]),
    {key_exists, _} = ?M:add(BrickName, Node, "foo", "foo2"),

    key_not_exist = ?M:replace(BrickName, Node, "notexist", "foo2"),
    ok = ?M:replace(BrickName, Node, "foo", "foo22"),

    ok = ?M:set(BrickName, Node, "foo", "foo222"),
    ok = ?M:set(BrickName, Node, "bar", "bar2"),

    %% Test basic get
    {ok, TS0, <<"foo222">>} = ?M:get(BrickName, Node, "foo"),
    {ok, TS1, <<"bar2">>} = ?M:get(BrickName, Node, "bar"),
    key_not_exist = ?M:get(BrickName, Node, "fooA"),
    {ok, TS0} = ?M:get(BrickName, Node, "foo", [witness]),
    {ok, TS1} = ?M:get(BrickName, Node, "bar", [witness]),
    key_not_exist = ?M:get(BrickName, Node, "fooA", [witness]),

    %% Test basic replace, set with test-and-set flag
    ok = ?M:replace(BrickName, Node, "foo", "foo2222", [{testset, TS0}]),
    {ts_error, _} = ?M:replace(BrickName, Node, "foo", "foo666", [{testset, TS0 + 1}]),
    ok = ?M:replace(BrickName, Node, "bar", "bar22", [{testset, TS1}]),
    {ts_error, _} = ?M:replace(BrickName, Node, "bar", "bar666", [{testset, TS1 + 1}]),

    %% Basic add with test-and-set flag
    ok = ?M:add(BrickName, Node, "baz", "baz2", [{testset, 42}]),
    {ok, TS2} = ?M:get(BrickName, Node, "baz", [witness]),
    {key_exists, TS2} = ?M:add(BrickName, Node, "baz", "baz666", [{testset, TS2}]),
    {ts_error, TS2} = ?M:add(BrickName, Node, "baz", "baz22", [{testset, TS2 - 1}]),

    %% Basic set with test-and-set flag
    ok = ?M:set(BrickName, Node, "baz", <<"baz3">>, [{testset, TS2}]),
    {ts_error, TS3} = ?M:set(BrickName, Node, "baz", <<"baz4">>, [{testset, TS2}]),
    %% Timestamp TS3 is correct for test-and-set, but this set is
    %% using TS3 again for the new timestamp, and the new & different
    %% value should trigger ts_error.
    [{ts_error, TS3}] = ?M:do(BrickName, Node,
                            [?M:make_op6(set, "baz", TS3, <<"baz5">>, 0,
                                         [{testset, TS3}])]),
    %% Same thing, but using the same timestamp and same value as currently
    %% stored by the brick, should succeed.
    [ok] = ?M:do(BrickName, Node,
                            [?M:make_op6(set, "baz", TS3, <<"baz3">>, 0,
                                         [{testset, TS3}])]),

    %% Basic delete
    ok = ?M:add(BrickName, Node, "delme", "delme1"),
    {ok, TSD1, <<"delme1">>} = ?M:get(BrickName, Node, "delme"),
    key_not_exist = ?M:delete(BrickName, Node, "delmefoo"),
    {ts_error, TSD1} = ?M:delete(BrickName, Node, "delme", [{testset, TSD1 + 1}]),
    ok = ?M:delete(BrickName, Node, "delme", [{testset, TSD1}]),
    key_not_exist = ?M:delete(BrickName, Node, "delme", [{testset, TSD1}]),
    ok = ?M:add(BrickName, Node, "delme-again", "delme-again1"),
    {ok, _TSD2, <<"delme-again1">>} = ?M:get(BrickName, Node, "delme-again"),
    ok = ?M:delete(BrickName, Node, "delme-again"),
    key_not_exist = ?M:delete(BrickName, Node, "delme-again"),

    %% Basic multiple queries
    [ok, ok, {key_exists, _}] =
        ?M:do(BrickName, Node, [?M:make_add("m-a1a", "m-a1a-v"),
                    ?M:make_add("m-a1b", "m-a1b-v"),
                    ?M:make_add("foo", "foo66")]),
    io:format("NOTE: mult query tests are incomplete!\n"),

    %% Basic txn
    {txn_fail, [{3, {key_exists, _}}]} =
        ?M:do(BrickName, Node, [?M:make_txn(),
                    ?M:make_add("m-a1c", "m-a1c-v"),
                    ?M:make_add("m-a1d", "m-a1d-v"),
                    ?M:make_add("foo", "foo66")]),
    [ok, ok, {ok, _, _}, ok]  =
        ?M:do(BrickName, Node, [?M:make_txn(),
                    ?M:make_add("m-a1c", "m-a1c-v"),
                    ?M:make_add("m-a1d", "m-a1d-v"),
                    ?M:make_get("foo"),
                    ?M:make_replace("foo", "foo67")]),
    io:format("NOTE: txn tests are incomplete!\n"),

    %% Test the timestamp checking on the server: add a key, then
    %% try to replace/set it with a timestamp that's older than
    %% what is already there.

    ok = ?M:set(BrickName, Node, "age-key", <<"age-value">>),
    ok = ?M:set(BrickName, Node, "age-key", <<"age-value2">>),
    ok = ?M:replace(BrickName, Node, "age-key", <<"age-value-good">>),
    TooOldTS = 42,                              % 42 usecs after 01/01/1970
    OpListSet1 = [?M:make_op6(set, "age-key", TooOldTS,
                              <<"age-value4">>, 0, [])],
    [{ts_error, _}] = ?M:do(BrickName, Node, OpListSet1),
    OpListReplace1 = [?M:make_op6(replace, "age-key", TooOldTS,
                                  <<"age-value5">>, 0, [])],
    [{ts_error, _}] = ?M:do(BrickName, Node, OpListReplace1),
    {ok, ExactTS1} = ?M:get(BrickName, Node, "age-key", [witness]),
    OpListSet2 = [?M:make_op6(set, "age-key", ExactTS1,
                              <<"age-value-bad">>, 0, [])],
    [{ts_error, _}] = ?M:do(BrickName, Node, OpListSet2),
    OpListSet3 = [?M:make_op6(set, "age-key", ExactTS1,
                              <<"age-value-good">>, 0, [])],
    [ok] = ?M:do(BrickName, Node, OpListSet3),
    OpListReplace2 = [?M:make_op6(set, "age-key", ExactTS1,
                                  <<"age-value-bad">>, 0, [])],
    [{ts_error, _}] = ?M:do(BrickName, Node, OpListReplace2),
    OpListReplace3 = [?M:make_op6(set, "age-key", ExactTS1,
                                  <<"age-value-good">>, 0, [])],
    [ok] = ?M:do(BrickName, Node, OpListReplace3),

    %% Stupid (but effective!) test for a certain kind of race condition.
    RunIters = 10*1000,
    io:format("Running tight async set+get loop for ~w iterations\n", [RunIters]),
    OldSyncVal = brick_server:set_do_sync(BrickName, false),
    [ok = begin
              Val = list_to_binary("val"++integer_to_list(X)),
              brick_server:set(BrickName, Node, "foo", Val),
              {Val, {ok, _, Val}} = {Val, brick_server:get(BrickName, Node, "foo")},
              ok = brick_server:delete(BrickName, Node, "foo"),
              key_not_exist = brick_server:delete(BrickName, Node, "foo"),
              ok
          end || X <- lists:seq(1, RunIters)],
    brick_server:set_do_sync(BrickName, OldSyncVal),

    io:format("Test t1: end\n"),
    ok.

t2(BrickName, Node) ->
    io:format("Test t2: start (will have nested t1() output!)\n"),

    %% t9() deletes everything, so let's repopulate with t1().
    %% Then try get-many both without and with a checkpoint.
    ok = t1(BrickName, Node),
    io:format("Test t2: resuming...\n"),

    {ok, {ManyRes1, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),

    %% Do the same while in the middle of a checkpoint.
    %% Fseveral() should still work correctly.
    brick_server:checkpoint(BrickName, Node,
                                [{start_sleep_time, 1000}]),
    timer:sleep(200),
    {ok, {ManyRes2, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    ManyRes2 = ManyRes1,

    %% We're still in the middle of a checkpoint.

    %% Try deleting something, then try again.
    HeadKey1 = element(1, hd(ManyRes1)),
    ok = ?M:delete(BrickName, Node, HeadKey1),
    {ok, {ManyRes3, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    ManyRes3 = tl(ManyRes1),

    %% Add a key that we know will be at the end of the list.
    ManyRes3Len = length(ManyRes3),
    [] = lists:nthtail(ManyRes3Len, ManyRes3),
    ok = ?M:set(BrickName, Node, "zzz1", "zzz1-value"),
    {ok, {ManyRes4, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    [{<<"zzz1">>, _, <<"zzz1-value">>, 0, [{val_len, 10}]}] = lists:nthtail(ManyRes3Len, ManyRes4),

    %% Check the same thing as ManyRes4, but with different flags:
    %% witness and witness+get_all_attribs.
    {ok, {ManyRes5, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111, [witness]),
    [{<<"zzz1">>, _}] = lists:nthtail(ManyRes3Len, ManyRes5),
    {ok, {ManyRes6, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111, [witness, get_all_attribs]),
    [{<<"zzz1">>, _, [{val_len, 10}]}] = lists:nthtail(ManyRes3Len, ManyRes6),

    %% Test the next_op_is_silent do op.
    OpsNoSilent = [?M:make_get("bar"), ?M:make_get("baz")],
    [{ok, _, <<"bar22">>}, {ok, _, <<"baz3">>}] = ?M:do(BrickName, Node, OpsNoSilent),

    OpsSilent1 = [?M:make_get("bar"),
                  ?M:make_next_op_is_silent(),
                  ?M:make_get("baz")],
    [{ok, _, <<"bar22">>}] = ?M:do(BrickName, Node, OpsSilent1),

    OpsSilent2 = [?M:make_txn()|OpsSilent1],
    [{ok, _, <<"bar22">>}] = ?M:do(BrickName, Node, OpsSilent2),

    OpsSilent3 = [?M:make_txn(), ?M:make_next_op_is_silent(),
                  ?M:make_get("baz")],
    [] = ?M:do(BrickName, Node, OpsSilent3),

    OpsSilent4 = [?M:make_txn(), ?M:make_next_op_is_silent(),
                  ?M:make_get("baz", [{testset, -44}])],
    {txn_fail, _} = ?M:do(BrickName, Node, OpsSilent4),

    io:format("Test t2: end (will have nested t1() output!)\n"),
    ok.

t3(BrickName, Node) ->
    io:format("Test t3: start\n"),

    io:format("Wait until prior tests' checkpoint(s) stuff is done..."),
    wait_for_checkpoint_to_finish(BrickName, Node),
    io:format(" DONE\n"),

    %% Delete all entries in the table.
    {ok, {ManyRes1, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    [ok = ?M:delete(BrickName, Node, Key) || {Key, _, _, _, _} <- ManyRes1],
    {ok, {[], false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),

    Fbin = fun(L) -> [list_to_binary(X) || X <- L] end,

    %% Populate the table
    ManyPopKeys = Fbin(["15", "16", "17", "30", "31", "32", "55", "56", "57"]),
    [ok = ?M:set(BrickName, Node, Key, Key) || Key <- ManyPopKeys],
    {ok, {ManyRes2, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    ManyRes2Keys = [Key || {Key, _, _, _, _} <- ManyRes2],
    ManyPopKeys = ManyRes2Keys,

    %% Start a checkpoint
    brick_server:checkpoint(BrickName, Node, [{start_sleep_time,1000}]),
    timer:sleep(200),

    %% Modify the table to create enough cases where
    %% we put brick_server:get_many_merge() through all of its cases.
    ok = ?M:set(BrickName, Node, "50", "50"),
    ok = ?M:delete(BrickName, Node, "32"),
    {ok, {ManyRes3, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    ManyRes3Keys = [Key || {Key, _, _, _, _} <- ManyRes3],
    ManyRes3Keys = Fbin(["15", "16", "17", "30", "31", "50", "55", "56", "57"]),
    ?DBG(res3_done),

    ok = ?M:delete(BrickName, Node, "30"),
    ok = ?M:set(BrickName, Node, "32", "32-new"),
    {ok, {ManyRes4, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    ManyRes4Keys = [Key || {Key, _, _, _, _} <- ManyRes4],
    ManyRes4Keys = Fbin(["15", "16", "17", "31", "32", "50", "55", "56", "57"]),

    ok = ?M:delete(BrickName, Node, "57"),
    ok = ?M:set(BrickName, Node, "60", "60"),
    {ok, {ManyRes5, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    ManyRes5Keys = [Key || {Key, _, _, _, _} <- ManyRes5],
    ManyRes5Keys = Fbin(["15", "16", "17", "31", "32", "50", "55", "56", "60"]),

    %% Verify that we're still in the middle of our checkpoint.
    case brick_server:checkpoint(BrickName, Node, []) of
        sorry ->
            wait_for_checkpoint_to_finish(BrickName, Node);
        ok ->
            %% It's possible that OS scheduling delays may have caused
            %% us to miss catching the checkpoint while still running.
            %% Let it slide....
            ok
    end,

    %% Now that checkpoint is done, verify key list is the same as Res5.
    {ok, {ManyRes6, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    ManyRes6Keys = [Key || {Key, _, _, _, _} <- ManyRes6],
io:format("ManyRes5Keys = ~p\n", [ManyRes5Keys]),
io:format("ManyRes6Keys = ~p\n", [ManyRes6Keys]),
    ManyRes6Keys = ManyRes5Keys,

    %% Delete all entries again, populate a few from scratch, checkpoint.
    [ok = ?M:delete(BrickName, Node, Key) || {Key, _, _, _, _} <- ManyRes6],
    {ok, {[], false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    [ok = ?M:set(BrickName, Node, Key, Key) || Key <- ManyPopKeys],
    brick_server:checkpoint(BrickName, Node, [{start_sleep_time,1000}]),
    timer:sleep(200),

    %% Try to hit clause 3 of get_many_merge
    [ok = ?M:set(BrickName, Node, Key, Key) || Key <- ManyPopKeys],
    ok = ?M:set(BrickName, Node, "99", "99"),
    {ok, {ManyRes7, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    ManyRes7Keys = [Key || {Key, _, _, _, _} <- ManyRes7],
    ManyRes7Keys = ManyPopKeys ++ Fbin(["99"]),

    %% Verify that we're still in the middle of our checkpoint.
    case brick_server:checkpoint(BrickName, Node, []) of
        sorry ->
            wait_for_checkpoint_to_finish(BrickName, Node);
        ok ->
            %% It's possible that OS scheduling delays may have caused
            %% us to miss catching the checkpoint while still running.
            %% Let it slide....
            ok
    end,

    io:format("Test t3: end\n"),
    ok.

t4(BrickName, Node) ->
    io:format("Test t4: start\n"),
    %% Test the binary_prefix option.

%     io:format("Wait until prior tests' checkpoint(s) stuff is done..."),
%     wait_for_checkpoint_to_finish(BrickName, Node),
%     io:format(" DONE\n"),

    P0 = [{binary_prefix, <<"0">>}],
    P1 = [{binary_prefix, <<"1">>}],
    P2 = [{binary_prefix, <<"2">>}],
    P5 = [{binary_prefix, <<"5">>}, witness],
    P9 = [{binary_prefix, <<"9">>}],

    %% Delete all entries in the table.
    {ok, {_ManyRes1a, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111, [witness]),
    {ok, {ManyRes1, false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    [ok = ?M:delete(BrickName, Node, Key) || {Key, _, _, _, _} <- ManyRes1],
    {ok, {[], false}} = ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111),
    {ok, {[], false}} =
        ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111, P1),
    {ok, {[], false}} =
        ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111, P2),
    {ok, {[], false}} =
        ?M:get_many(BrickName, Node, ?BRICK__GET_MANY_FIRST, 1111, P5),

    %% Populate the table
    ManyPopKeys =
        [list_to_binary(X) ||
            X <- ["15", "16", "17", "30", "31", "32", "55", "56", "57"]],
    [ok = ?M:set(BrickName, Node, Key, Key) || Key <- ManyPopKeys],

    {ok, {[], false}} =
        ?M:get_many(BrickName, Node, <<"0">>, 1111, P0),
    {ok, {[], false}} =
        ?M:get_many(BrickName, Node, <<"2">>, 1111, P2),
    {ok, {[], false}} =
        ?M:get_many(BrickName, Node, <<"9">>, 1111, P9),

    {ok, {[_,_,_], false}} = ?M:get_many(BrickName, Node, <<"1">>, 1111, P1),
    {ok, {[_], true}} = ?M:get_many(BrickName, Node, <<"1">>, 1, P1),
    %% Both of the following will find <<"17">> only.
    {ok, {[_], true}} = ?M:get_many(BrickName, Node, <<"16">>, 1, P1),
    {ok, {[_], false}} = ?M:get_many(BrickName, Node, <<"16">>, 2, P1),

    {ok, {[], false}} = ?M:get_many(BrickName, Node, <<"2">>, 1111, P2),

    %% Just to be paranoid, test at the end of the table.
    {ok, {[_,_,_], false}} = ?M:get_many(BrickName, Node, <<"5">>, 1111, P5),
    {ok, {[_], true}} = ?M:get_many(BrickName, Node, <<"5">>, 1, P5),
    %% Both of the following will find <<"57">> only.
    {ok, {[_], true}} = ?M:get_many(BrickName, Node, <<"55555555555">>, 1, P5),
    {ok, {[_], false}} = ?M:get_many(BrickName, Node, <<"56">>, 2, P5),
    {ok, {[], false}} = ?M:get_many(BrickName, Node, <<"59">>, 2, P5),

    io:format("Test t4: end\n"),
    ok.

t5(BrickName, Node) ->
    %% QQQ incomplete!  This will be a test of simultaneous operations
    %% on the same key(s) by multiple procs.
    %% This should stress both the timestamp checking (though we aren't
    %% doing extremely strict checking here) and dirty key consistency
    %% processing in the server (much more important, though this test
    %% is simply trying to trigger a brick server deadlock).

    io:format("Test t5: start\n"),

    Key1  = <<"contended-key1">>,
    Val1a = <<"Whatever, man">>,
    Val1b = <<"Dude, it's time for something slightly newer.">>,
    ok = ?M:set(BrickName, Node, Key1, Val1a),

    [ok = t5_helper(BrickName, Node, Key1, Val1b, 40, 60) ||
        _ <- lists:seq(1, 20)],

    ok = ?M:delete(BrickName, Node, Key1),
    io:format("Test t5: end\n"),
    timer:sleep(500),
    ok.

t5_helper(BrickName, Node, Key, Val, RandomSleepInterval, NumProcs) ->
    ParentPid = self(),
    F1 = fun() ->
                 %% Damn, this is harder than it looks.  Bind the
                 %% NewTS right away: each proc's will be slightly
                 %% different.  Only *then* should we go to sleep for
                 %% the random amount of time.

                 NewTS = ?M:make_timestamp(),
                 ToSleep = receive {to_sleep, N} -> N end,
                 timer:sleep(ToSleep),

                 {ok, _TS1} = ?M:get(BrickName, Node, Key, [witness]),
                 DoList = [?M:make_op6(set, Key, NewTS, Val, 0, [])],
                 case ?M:do(BrickName, Node, DoList) of
                     [ok] ->
                         ParentPid ! win,
                         ok;
                     [{ts_error, _}] ->
                         ParentPid ! lose,
                         ok;
                     Err ->
                         io:format("ERROR: What is this?? ~p\n", [Err]),
                         ParentPid ! Err,
                         throw(Err)
                 end
         end,
    flush_mailbox(),
    ChildPids = [spawn(F1) || _ <- lists:seq(1, NumProcs)],
    [Pid ! {to_sleep, random:uniform(RandomSleepInterval)} ||
        Pid <- ChildPids],
    {Wins, Losses, Errs} =
        lists:foldl(fun(_Pid, {W, L, Es}) ->
                            receive
                                win  -> {W+1, L, Es};
                                lose -> {W, L+1, Es};
                                Err  -> {W, L, [Err|Es]}
                            end
                    end, {0, 0, []}, ChildPids),
    io:format("Wins ~w losses ~w errors ~p\n", [Wins, Losses, Errs]),
    true = (Wins >= 1),
    Errs = [],
    ok.

t7(BrickName, Node) ->
    io:format("Test t7: start\n"),

    Ks = [<<"t1">>, <<"t2">>, <<"t3">>],
    [_ = ?M:delete(BrickName, Node, K) || K <- Ks],

    %% Test must_exist
    Do1 = [?M:make_txn(), ?M:make_get("t1", [must_exist])],
    {txn_fail, [{1, key_not_exist}]} = ?M:do(BrickName, Node, Do1),
    Do2 = [?M:make_txn(), ?M:make_delete("t1", [must_exist])],
    {txn_fail, [{1, key_not_exist}]} = ?M:do(BrickName, Node, Do2),
    Do3 = [?M:make_txn(), ?M:make_replace("t1", "v1")],
    {txn_fail, [{1, key_not_exist}]} = ?M:do(BrickName, Node, Do3),

    %% Test add vs set vs replace
    Do10 = [?M:make_txn(), ?M:make_add("t1", "v1")],
    [ok] = ?M:do(BrickName, Node, Do10),
    {txn_fail, [{1, {key_exists, _}}]} = ?M:do(BrickName, Node, Do10),
    Do11 = [?M:make_txn(), ?M:make_replace("t1", "v1")],
    [ok] = ?M:do(BrickName, Node, Do11),
    Do12 = [?M:make_txn(), ?M:make_set("t1", "v1")],
    [ok] = ?M:do(BrickName, Node, Do12),

    %% Test must_not_exist
    Do20 = [?M:make_txn(), ?M:make_get("t1", [must_not_exist])],
    {txn_fail, [{1, {key_exists, _}}]} = ?M:do(BrickName, Node, Do20),
    Do21 = [?M:make_txn(), ?M:make_delete("t1", [must_not_exist])],
    {txn_fail, [{1, {key_exists, _}}]} = ?M:do(BrickName, Node, Do21),

    %% I don't think it makes sense to test must_exist/must_not_exist
    %% in conjunction with get_many.

    %% ssf testing

    [ok = ?M:set(BrickName, Node, list_to_binary(K), list_to_binary(V)) ||
        {K, V} <- [{"t1", "v1"}, {"t2", "v2"}, {"t3", "v3"}, {"t4", "v4"}]],
    Fget1 = fun(Key, _DoOp, _DoFlags, S) ->
                    %% Also exercise the ssf_* helper funcs.
                    [{<<"t1">>, _TS, <<"v1">>, _Exp, _Fs}] =
                        ?M:ssf_peek(<<"t1">>, true,S),
                    D1 = ?M:make_get(Key, [get_all_attribs]),
                    {ok, [D1]}
            end,
    [{ok, _TS, <<"v1">>, 0, _}] =
        ?M:do(BrickName, Node, [?M:make_ssf(<<"t1">>, Fget1)]),
    [key_not_exist] =
        ?M:do(BrickName, Node, [?M:make_ssf(<<"t1-This-Key-FUBar">>, Fget1)]),
    Fget2 = fun(Key, _DoOp, _DoFlags, S) ->
                    X = ?M:ssf_peek_many(Key, 3, [], S),
                    {ok, [{custom_return, X}]}
            end,
    [{custom_return, {[_, _, _], true}}] =
        ?M:do(BrickName, Node, [?M:make_ssf(<<"t0">>, Fget2)]),

    io:format("Test t7: end\n"),
    ok.

%% BZ 21733 regression test.

t8(BrickName, Node) ->
    io:format("Test t8: start\n"),

    %% Cleanup first.
    {ok, {Rs, false}} = ?M:get_many(BrickName, Node,
                                    ?BRICK__GET_MANY_FIRST, 9999),
    OldKeys = [element(1, X) || X <- Rs],
    [ok = ?M:delete(BrickName, Node, OldKey) || OldKey <- OldKeys],

    S_tab = list_to_atom(atom_to_list(BrickName) ++ "_store"),
    E_tab = list_to_atom(atom_to_list(BrickName) ++ "_exp"),
    Flen = fun(Tab) -> length(rpc:call(Node, ets, tab2list, [Tab])) end,
    Ftest = fun() -> Flen(S_tab) == Flen(E_tab) end,

    Key = <<"t1">>,
    Exp = 9*1000*1000*1000,
    %% NOTE: For best testing, use both exact same expiry value and
    %% different expiry values.  There used to be a bug where ordering
    %% was wrong (when using same expiry).  Also, latent bug when
    %% using different expiry values.
    Fdo = fun(Incr) ->
                  ok = ?M:set(BrickName, Node, Key, <<>>, Exp, [], 5*1000),
                  ok = ?M:set(BrickName, Node, Key, <<>>, Exp+(Incr*1), [], 5*1000),
                  true = Ftest(),
                  ok = ?M:replace(BrickName, Node, Key, <<>>, Exp+(Incr*2), [], 5*1000),
                  true = Ftest(),
                  ok = ?M:delete(BrickName, Node, Key),
                  true = Ftest(),
                  ok
          end,
    ok = Fdo(0),
    ok = Fdo(1),

    io:format("Test t8: end\n"),
    ok.

t50(BrickName, Node, NumIters, DoSet) ->
    io:format("Test t50: start\n"),

    ok = ?M:set(BrickName, Node, "t50-s1", "t50-s1-val"),
    {key_exists, _} = ?M:add(BrickName, Node, "t50-s1", "t50-s1-val666"),
    ok = ?M:replace(BrickName, Node, "t50-s1", "t50-s1-val2"),

    Val2K = list_to_binary(lists:duplicate(2*1024, $x)),
    Set1 = fun(N) ->
                   K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [node(), N])),
                   ok = ?M:set(BrickName, Node, K, Val2K)
           end,
    if DoSet == true ->
            io:format("\n"),
            io:format("Starting ~w iterations of Set1 ... ", [NumIters]),
            {T1, _R1} = timer:tc(?MODULE, loopN,  [NumIters, Set1]),
            io:format("done in ~w secs (~w op/sec)\n", [T1 / 1000000, round(NumIters / (T1 / 1000000))]);
       true ->
            skip_set
    end,

    Get1 = fun(N) ->
                   K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [node(), N])),
                   {ok, _, Val2K} = ?M:get(BrickName, Node, K)
           end,
    io:format("Starting ~w iterations of Get1 ... ", [NumIters]),
    {T2, _R2} = timer:tc(?MODULE, loopN, [NumIters, Get1]),
    io:format("done in ~w secs (~w op/sec)\n", [T2 / 1000000, round(NumIters / (T2 / 1000000))]),

    Wit1 = fun(N) ->
                   K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [node(), N])),
                   {ok, _} = ?M:get(BrickName, Node, K, [witness])
           end,
    io:format("Starting ~w iterations of Wit1 ... ", [NumIters]),
    {T3, _R3} = timer:tc(?MODULE, loopN, [NumIters, Wit1]),
    io:format("done in ~w secs (~w op/sec)\n", [T3 / 1000000, round(NumIters / (T3 / 1000000))]),

%%     Fun1 = fun(Node) ->
%%                 io:format("Intentionally update only the server ~w\n", [Node]),
%%                 Val4B = list_to_binary("foo" ++ atom_to_list(Node)),
%%                 F1 = fun(N) ->
%%                              K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [node(), N])),
%%                              [ok] = ?M:set([Node], BrickName, K, Val4B)
%%                      end,
%%                 {T1a, _R} = timer:tc(?MODULE, loopN, [NumIters, F1]),
%%                 io:format("done in ~w secs (~w op/sec)\n", [T1a / 1000000, NumIters / (T1a / 1000000)]),
%%                 %%
%%                 io:format("Use multi_get to 'scan' the ~w items and fix them\n", [NumIters]),
%%                 F2 = fun(N) ->
%%                              K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [node(), N])),
%%                              %% This soon-to-go-away func does not return list
%%                              {ok, _, Val4B} = ?M:multi_get(BrickName, Node, K, [])
%%                      end,
%%                 {T2a, _R} = timer:tc(?MODULE, loopN, [NumIters, F2]),
%%                 io:format("done in ~w secs (~w op/sec)\n", [T2a / 1000000, NumIters / (T2a / 1000000)])
%%         end,
%%     lists:map(Fun1, Node),

    io:format("Test t50: end\n"),
    ok.


t51(BrickName, Node, NumProcs, PhaseTime, DoSet) ->
    io:format("Test t51: start\n"),

    ok = ?M:set(BrickName, Node, "t50-s1", "t50-s1-val"),
    {key_exists, _} = ?M:add(BrickName, Node, "t50-s1", "t50-s1-val666"),
    ok = ?M:replace(BrickName, Node, "t50-s1", "t50-s1-val2"),

    Val2K = list_to_binary(lists:duplicate(2*1024, $x)),
    _Val2K_y = list_to_binary(lists:duplicate(2*1024, $y)),

    if DoSet == true ->
            io:format("\n"),
            io:format("Starting ~w procs of Set1 for ~w secs ... ",
                      [NumProcs, PhaseTime]),
            Set1 = fun(N, ProcNum) ->
                           K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [ProcNum, N - 1])),
                           ok = ?M:set(BrickName, Node, K, Val2K)
                   end,
            SetPids = [spawn(fun() -> loop_msg_stop(Set1, PN) end) ||
                          PN <- lists:seq(1, NumProcs)],
            timer:sleep(PhaseTime * 1000),
            SetCount = collect_counts(SetPids),
            io:format(" ~w ops, ~w ops/sec\n", [SetCount, round(SetCount / PhaseTime)]);
       true ->
            skip_set_ops
    end,

    Get1 = fun(N, ProcNum) ->
                   %%io:format("~w ", [ProcNum]),
                   %% QQQ Kludge: all threads will get the same 10 records.
                   K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [ProcNum, N rem 10])),
                   {ok, _, Val2K} = ?M:get(BrickName, Node, K)
           end,
    io:format("Starting ~w procs of Get1 for ~w secs ... ",
              [NumProcs, PhaseTime]),
    GetPids = [spawn(fun() -> loop_msg_stop(Get1, PN) end) ||
                  PN <- lists:seq(1, NumProcs)],
    timer:sleep(PhaseTime * 1000),
    GetCount = collect_counts(GetPids),
    io:format(" ~w ops, ~w ops/sec\n", [GetCount, round(GetCount / PhaseTime)]),

    Wit1 = fun(N, ProcNum) ->
                   %%io:format("~w ", [ProcNum]),
                   %% QQQ Kludge: all threads will get the same 10 records.
                   K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [ProcNum, N rem 10])),
                   {ok, _} = ?M:get(BrickName, Node, K, [witness])
           end,
    io:format("Starting ~w procs of Wit1 for ~w secs ... ",
              [NumProcs, PhaseTime]),
    WitPids = [spawn(fun() -> loop_msg_stop(Wit1, PN) end) ||
                  PN <- lists:seq(1, NumProcs)],
    timer:sleep(PhaseTime * 1000),
    WitCount = collect_counts(WitPids),
    io:format(" ~w ops, ~w ops/sec\n", [WitCount, round(WitCount / PhaseTime)]),

%     Wit1 = fun(N, ProcNum) ->
%                  %% QQQ Kludge: all threads will get the same 10 records.
%                  K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [ProcNum, N rem 10])),
%                  [{ok, _}] = ?M:get(BrickName, Node, K, [witness])
%          end,
%     io:format("Starting ~w procs of Wit1 for ~w secs ... ",
%             [NumProcs, PhaseTime]),
%     WitPids = [spawn(fun() -> loop_msg_stop(Wit1, PN) end) ||
%                 PN <- lists:seq(1, NumProcs)],
%     timer:sleep(PhaseTime * 1000),
%     WitCount = collect_counts(WitPids),
%     io:format(" ~w ops, ~w ops/sec\n", [WitCount, WitCount / PhaseTime]),

%     [{ok, KeyList0, _}] = ?M:get_many([hd(Node)], ?BRICK__GET_MANY_FIRST, 20000, [witness]),
%     BlastNum = 444,
%     Keys0 = [K || {K, _TS} <- KeyList0],
%     ChoppedKeys1 = chop_list(Keys0, BlastNum),
%     Node1 = hd(Node),
%     io:format("Intentionally update only the server ~w\n", [Node1]),
%     Parent = self(),
%     Fbogus_update =
%       fun(Keys) ->
%               [?M:set([Node1], K, Val2K_y) || K <- Keys],
%               Parent ! {done, self()}
%       end,
%     Pids1a = [spawn(fun() -> Fbogus_update(Keys) end) || Keys <- ChoppedKeys1],
%     lists:foreach(fun(Pid) -> receive {done, Pid} -> ok end end, Pids1a),

%     io:format("Repair via multi_get ... "),
%     Fmulti_get_repair =
%       fun(Keys) ->
%               %%[{io:format("multi get(~s)\n", [K]), {ok, _, Val2K_y} = ?M:multi_get(BrickName, Node, K, [])} || K <- Keys],
%               [[{ok, _, Val2K_y}] = ?M:multi_get(BrickName, Node, K, []) || K <- Keys],
%               Parent ! {done, self()}
%       end,
%     Start1b = now(),
%     Pids1b = [spawn(fun() -> Fmulti_get_repair(Keys) end) || Keys <- ChoppedKeys1],
%     lists:foreach(fun(Pid) -> receive {done, Pid} -> ok end end, Pids1b),
%     Stop1b = now(),
%     DiffSec = timer:now_diff(Stop1b, Start1b) / (1000*1000),
%     io:format("done in ~w sec, ~w ops/sec\n", [DiffSec, length(Keys0) / DiffSec]),

%     Fun1 = fun(Node) ->
%                  io:format("Intentionally update only the server ~w\n", [Node]),
%                  Val4B = list_to_binary("foo" ++ atom_to_list(Node)),
%                  F1 = fun(N) ->
%                               K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [node(), N])),
%                               [ok] = ?M:set([Node], K, Val4B)
%                       end,
%                  {T1a, _R} = timer:tc(?MODULE, loopN, [Num, F1]),
%                  io:format("done in ~w secs (~w op/sec)\n", [T1a / 1000000, Num / (T1a / 1000000)]),
%                  %%
%                  io:format("Use multi_get to 'scan' the ~w items and fix them\n", [Num]),
%                  F2 = fun(N) ->
%                               K = lists:flatten(io_lib:format("t50-Fun1-~w-~w", [node(), N])),
%                               [{ok, _, Val4B}] = ?M:multi_get(BrickName, Node, K, [])
%                       end,
%                  {T2a, _R} = timer:tc(?MODULE, loopN, [Num, F2]),
%                  io:format("done in ~w secs (~w op/sec)\n", [T2a / 1000000, Num / (T2a / 1000000)])
%          end,
%     lists:map(Fun1, Node),

    io:format("Test t50: end\n"),
    ok.

t60(Nodes, BrickName, RS = _Summary) ->
    %% Delete all entries in each brick.
    [ok = ?M:flush_all(BrickName, Node) || Node <- Nodes],
    [wait_for_checkpoint_to_finish(BrickName, Node) || Node <- Nodes],

    ok = t60_get(Nodes, BrickName, RS),
    ok = t60_get_longer(Nodes, BrickName, RS),

    fail_t60_incomplete.

t60_get(Nodes, BrickName, RS = Summary) ->
    Key1 = "foo1",
    Val1 = "foo1-value",
    Key2 = "foo2",
    Val2 = "foo2-value",
    TS1 = 100,
    TS2 = 200,

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Perfect case: All bricks are available.
    %% Check all combinations of servers "damaged" via delete or replace.
    %% Check all combinations of witness or not.

    Put_a   = ?M:make_op6(set, Key1, TS1, Val1, 0, []),
    Put_a_1 = ?M:make_op6(set, Key1, TS1 - 5, Val2, 0, []),
    Put_a_2 = ?M:make_op6(set, Key1, TS2, Val2, 0, []),
    Fput_a = fun() ->
                     [_ = ?M:delete(BrickName, Node, Key1) || Node <- Nodes],
                     [[ok] = ?M:do(BrickName, Node, [Put_a]) || Node <- Nodes]
             end,

    %% Sanity: check that Fput_a() works.
    Fput_a(),
    [{ok, _TS, Val1}] = ?M:get(Nodes, BrickName, Key1, RS),

    Fdamage_del =
        fun(Node, BName, Key) ->
                ok = ?M:delete(Node, BName, Key)
        end,
    Fdamage_put_earlier =
        fun(Node, BName, Key) ->
                ok = ?M:delete(Node, BName, Key),
                [ok] = ?M:do(Node, BName, [Put_a_1]),
                ok
        end,
    Fdamage_put_later =
        fun(Node, BName, Key) ->
                ok = ?M:delete(Node, BName, Key),
                [ok] = ?M:do(Node, BName, [Put_a_2]),
                ok
        end,
    Fdel_combinations =
        fun({DamageNodes, DamageFun, Flags}) ->
                Fput_a(),
                io:format("** Damaging Key ~p on nodes ~w, Flags = ~w\n", [Key1, DamageNodes, Flags]),
                [ok = DamageFun(BrickName, Node, Key1) || Node <- DamageNodes],
                XX = ?M:make_get(Key1, Flags),
                XY = ?M:make_get(Key2, Flags),
                case proplists:get_value(witness, Flags, false) of
                    true ->
                        [key_not_exist, key_not_exist, ResWit] =
                            ?M:do(Nodes, BrickName, [XY, XY, XX], RS),
                        case {DamageFun, ResWit} of
                            {_,                 {ok, TS1}} -> ok;
                            {Fdamage_put_later, {ok, TS2}} -> ok;
                            _                           -> throw(witness_error)
                        end;
                    false ->
                        [key_not_exist, key_not_exist, ResRead] =
                            ?M:do(Nodes, BrickName, [XY, XY, XX], RS),
                        case {DamageFun, ResRead} of
                            {_,                 {ok, TS1, Val1}} -> ok;
                            {Fdamage_put_later, {ok, TS2, Val2}} -> ok;
                            _                           -> throw(witness_error)
                        end
                end,
                ok
        end,

    DamageNodeVariations = gmt_util:combinations(Nodes) --
        gmt_util:permutations(Nodes),
    DamageFuns = [Fdamage_del, Fdamage_put_earlier, Fdamage_put_later],
    WitnessVariations = [[], [witness]],
    Damage_plus_Flags_Vars = [{D, DF, F} || D  <- DamageNodeVariations,
                                            DF <- DamageFuns,
                                            F  <- WitnessVariations],
    _R = lists:map(Fdel_combinations, Damage_plus_Flags_Vars),
    ?DBG(Damage_plus_Flags_Vars),
    ?DBG(_R),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Not-so-perfect case: Some bricks available.
    %% Check all combinations of servers down (not "damaged" as previous test).
    %% Check all combinations of witness or not.
    Fdown_combinations =
        fun({DownNodes, _DamageFun, Flags}) ->
                ?DBG({DownNodes, Flags}),
                [_ = rpc:call(Node, brick_shepherd, start_brick,
                              [BrickName, []]) || Node <- Nodes],
                timer:sleep(100),
                [brick_server:status(BrickName, Node) || Node <- Nodes],
                Fput_a(),
                [_ = rpc:call(Node, brick_shepherd, stop_brick,
                              [BrickName]) || Node <- DownNodes],
                timer:sleep(100),

                %% Almost exact cut-and-paste from Fdel_combinations()
                %% (missing Fput_a() and delete()).
                XX = ?M:make_get(Key1, Flags),
                XY = ?M:make_get(Key2, Flags),
                case proplists:get_value(witness, Flags, false) of
                    true ->
                        [key_not_exist, key_not_exist, {ok, _}] =
                            ?M:do(Nodes, BrickName, [XY, XY, XX], RS);
                    false ->
                        [key_not_exist, key_not_exist, {ok, _, _}] =
                            ?M:do(Nodes, BrickName, [XY, XY, XX], RS)
                end,
                ok
        end,

    %% TODO: get a better way of extracting info out of #summary record.
    if element(2, Summary) /= simple ->
            %% TODO: A future, stricter quorum enforcement mechanism might
            %% want to refuse a get if only 1/3 of total nodes are available.
            io:format("\n\n"
                      "WARNING: Test does not support non-simple types!\n"
                      "Fdown_combinations test will be skipped.\n"
                      "\n\n"),
            timer:sleep(4000);
       true ->
            _R2 = lists:map(Fdown_combinations, Damage_plus_Flags_Vars),
            ?DBG(_R2),
            %% Start all bricks again, to put everyone into known state
            [rpc:call(Node, brick_shepherd, start_brick,
                      [BrickName, []]) || Node <- Nodes]
    end,

    ok.

t60_get_longer(Nodes, BrickName, RS = _Summary) ->
    Key1 = "foo1",
    Val1 = "foo1-value",
    _Key2 = "foo2",
    _Val2 = "foo2-value",
    TS1 = 100,
    _TS2 = 200,
    LoopSeconds = 10,

    %% Set our guinea pig value.
    Put_a = ?M:make_op6(set, Key1, TS1, Val1, 0, []),
    [[ok] = ?M:do(BrickName, Node, [Put_a]) || Node <- Nodes],

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Loop: While one proc attempts repeated get ops, another proc
    %%       (the "meddler") will delete one copy of the key.
    MeddleTarget = lists:last(Nodes),
    Fmeddling_deleter1 =
        fun() ->
                gmt_loop:do_while(
                  fun(DelCount) ->
                          receive
                              {stop, FromPid} ->
                                  FromPid ! {ok, self(), DelCount},
                                  {false, DelCount}
                          after 0 ->
                                  _X = ?M:delete(MeddleTarget, BrickName,Key1),
                                  if DelCount rem 25 == 0 -> timer:sleep(10);
                                     true                 -> ok
                                  end,
                                  {true, DelCount + 1}
                          end
                  end, 0),
                exit(normal)
        end,
    Meddler1Pid = spawn_link(Fmeddling_deleter1),
    Fget_loop1 =
        fun({StartTime, Meddler, Count}) ->
                case timer:now_diff(now(), StartTime) of
                    N when N > LoopSeconds*1000*1000 ->
                        Meddler ! {stop, self()},
                        DelCount = receive {ok, Meddler, Dels} -> Dels end,
                        %% One last get to repair Key1, if necessary.
                        [{ok, _TS, Val1}] = ?M:get(Nodes, BrickName,
                                                     Key1, RS),
                        {false, {DelCount, Count}};
                    _ ->
                        [{ok, _TS, Val1}] = ?M:get(Nodes, BrickName,
                                                     Key1, RS),
                        {true, {StartTime, Meddler, Count+1}}
                end
        end,
    {DelCount1, ReadCount1} =
        gmt_loop:do_while(Fget_loop1, {now(), Meddler1Pid, 0}),
    io:format("t60_get_longer: 1: delete count = ~w\n", [DelCount1]),
    io:format("t60_get_longer: 1: read count   = ~w\n", [ReadCount1]),
    timer:sleep(1000),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Loop: While one proc attempts repeated get ops, another proc
    %%       (the "meddler") will delete one copy of the key and replace
    %%       the key with an older TS value.  Each get op should return
    %%       only the {TS, value} data.
    Fmeddling_deleter2 =
        fun() ->
                Put_foo = ?M:make_op6(set, Key1, TS1 - 5, "", 0, []),
                gmt_loop:do_while(
                  fun(DelCount) ->
                          receive
                              {stop, FromPid} ->
                                  FromPid ! {ok, self(), DelCount},
                                  {false, DelCount}
                          after 0 ->
                                  _ = ?M:delete(MeddleTarget, BrickName, Key1),
                                  _ = ?M:do(MeddleTarget,BrickName, [Put_foo]),
                                  if DelCount rem 25 == 0 -> timer:sleep(10);
                                     true                 -> ok
                                  end,
                                  {true, DelCount + 1}
                          end
                  end, 0),
                exit(normal)
        end,
    Meddler2Pid = spawn_link(Fmeddling_deleter2),
    {DelCount2, ReadCount2} =
        gmt_loop:do_while(Fget_loop1, {now(), Meddler2Pid, 0}),
    io:format("t60_get_longer: 2: delete count = ~w\n", [DelCount2]),
    io:format("t60_get_longer: 2: read count   = ~w\n", [ReadCount2]),
    timer:sleep(1000),

    ok.

t9(BrickName, Node) ->
    io:format("Test t9: start\n"),

    Status0 = ?M:status(BrickName, Node),
    io:format("Status before flush_all = ~p\n", [Status0]),
    ok = ?M:flush_all(BrickName, Node),
    %%
    ok = ?M:add(BrickName, Node, "delme", <<"delme1">>),
    {ok, _TSD1, <<"delme1">>} = ?M:get(BrickName, Node, "delme"),
    key_not_exist = ?M:delete(BrickName, Node, "delmefoo"),
    ok = ?M:delete(BrickName, Node, "delme"),
    %%
    Status1 = ?M:status(BrickName, Node),
    io:format("Status after flush_all = ~p\n", [Status1]),

    io:format("Test t9: end\n"),
    ok.

t10(BrickName, Node) ->
    io:format("Test t10: start\n"),

    Key = <<"/bz22015/foo">>,
    ok = ?M:set(BrickName, Node, Key, <<"BZ22015 regression test">>),
    F = fun(_Key0, _DoOp, _DoFlags, _S0) ->
                Val = ?VALUE_REMAINS_CONSTANT,
                Mod = brick_server:make_replace(Key, Val, 0, []),
                {ok, [Mod]}
        end,
    Do = brick_server:make_ssf(Key, F),
    %% If buggy, we'll get crash instead of good match.
    [ok] = ?M:do(BrickName, Node, [Do], [], 5*1000),

    io:format("Test t10: end\n"),
    ok.

t90(BrickName, Node) ->
    ok = t90(BrickName, Node, [], false),
    ok = t90(BrickName, Node, [], true).

t90(BrickName0, Node, OptionList, DoCheckpointP) ->
    io:format("Test t90: start, DoCheckpointP = ~p\n", [DoCheckpointP]),
    %% Test the brick private metadata key/value store.
    %% And while we're going to the effort of stopping and restarting
    %% the DUT brick, we'll also test get_many() values before and
    %% after the restart.

    BrickName = list_to_atom(atom_to_list(BrickName0) ++ "_t90"),
    %% Only do this at the top of this func, not inside Fstart fun.
    start_and_flush_common_log(),
    os:cmd("rm -rf hlog.*" ++ atom_to_list(BrickName) ++ "*"),
    Fstart = fun() ->
                     {ok, NewPid} = safe_start_link(BrickName, OptionList),
                     io:format("TTT: t90: 2: ~p, or, whereis(~p) = ~p\n", [NewPid, BrickName, whereis(BrickName)]),
                     ok = ?M:chain_role_standalone(BrickName, node()),
                     ok = ?M:chain_set_my_repair_state(BrickName, node(), ok)
             end,
    Fstop = fun() ->
                    BrickPid = rpc:call(Node, erlang, whereis, [BrickName]),
                    io:format("TTT: t90: stop: ~p pid is ~p\n", [BrickName, BrickPid]),
                    catch erlang:unlink(BrickPid),
                    catch exit(BrickPid, kill),
                    timer:sleep(50),
                    io:format("TTT: t90: ~p better be stopped now\n", [BrickName]),
                    ok
            end,
    Fset = fun() ->
                 [ok = ?M:set(BrickName, Node, K, V) ||
                     {K, V} <- [{"k1", "v1"}, {"k2", "v2"}, {<<"k3">>, <<"v3">>}]]
           end,

    K1 = <<"$$$_t90_key1">>,
    V1 = term_to_binary({this, is, a, test, "Hello, world!"}),
    K2 = <<"$$$_t90_key2">>,
    V2 = term_to_binary(<<"This is a spiffy thing">>),
    K3 = <<"$$$_t90_key3">>,
    V3 = term_to_binary("Well, it better darn well work, else we'll crash!"),
    Ks = [K1, K2, K3],
    KVs = [{K1, V1}, {K2, V2}, {K3, V3}],
    Ks_bad = [<<"$$$_does">>, <<"$$$_not">>, <<"$$$_exist">>],

    _ = Fstop(),
    ok = Fstart(),
    io:format("TTT: t90: options = ~p\n", [OptionList]),
    Fset(),
    GetManyBefore = ?M:get_many(BrickName, Node,
                                ?BRICK__GET_MANY_FIRST, 9999),

    [ok = ?M:md_test_delete(BrickName, Node, K) || K <- Ks],
    [ok = ?M:md_test_set(BrickName, Node, K, V) || {K, V} <- KVs],
    [[KV] = ?M:md_test_get(BrickName, Node, K) || {K, _} = KV <- KVs],
    [[] = ?M:md_test_get(BrickName, Node, K) || K <- Ks_bad],

    if DoCheckpointP ->
            %% Trigger a checkpoint, wait until it's done.
            ?M:checkpoint(BrickName, Node),
            wait_for_checkpoint_to_finish(BrickName, Node);
       true ->
            ok
    end,

    ok = Fstop(),
%     io:format("\n\n\nDBG: TEST0 DUMP of hlog.~p\n\n\n", [BrickName]),
%     brick_ets:debug_scan("hlog.regression_test0_t90"),
    start_and_flush_common_log(),
%     io:format("\n\n\nDBG: TEST0 DUMP, after flush, of hlog.~p\n\n\n", [BrickName]),
%     brick_ets:debug_scan("hlog.regression_test0_t90"),
    ok = Fstart(),

    %% Check a fix for checkpoint + bigdata_dir bug: values are
    %% corrupted after a checkpoint!  However, it only makes sense to
    %% call this when logging is enabled.  Otherwise, after the
    %% shutdown, the brick's normal data will (of course!) be forgotten.
    case proplists:get_value(do_logging, OptionList) of
        true ->
            GetManyAfter = ?M:get_many(BrickName, Node,
                                       ?BRICK__GET_MANY_FIRST, 9999),
            GetManyBefore = GetManyAfter;
        _ ->
            ok
    end,

    %% Check a fix for checkpoint + private brick metadata: values
    %% are not logged in a checkpoint and therefore forgotten!
    io:format("\n\nDELME: proplists:get_value(do_logging, OptionList) = ~p\n\n", [proplists:get_value(do_logging, OptionList)]),
    [[KV] = ?M:md_test_get(BrickName, Node, K) || {K, _} = KV <- KVs],
    [[] = ?M:md_test_get(BrickName, Node, K) || K <- Ks_bad],
    [ok = ?M:md_test_delete(BrickName, Node, K) || K <- tl(Ks)],

    ok = Fstop(),
    io:format("Test t90: end\n"),
    ok.

t91(BrickName, Node) ->
    t91(BrickName, Node, [{do_logging, true}, {bigdata_dir, ?BIGDATA_DIR}]).

t91(BrickName, Node, OptionList) ->
    case proplists:get_value(do_logging, OptionList) of
        true  -> t91b(BrickName, Node, OptionList);
        false -> ok
    end.

t91b(BrickName, Node, OptionList) ->
    io:format("Test t91: start: ~p\n", [OptionList]),

    Key = <<"foo">>,
    Val = <<"fooval">>,

    ok = ?M:set(BrickName, Node, Key, Val),
    ok = ?M:checkpoint(BrickName, Node),
    timer:sleep(500),
    %% io:format("dump 0: ~p\n", [ets:tab2list(zoo_store)]),
    _ = ?M:start_scavenger(BrickName, Node, []),
    timer:sleep(500),
    %% io:format("dump 1: ~p\n", [ets:tab2list(zoo_store)]),
    %% For our regression, yes, we really do need this 2nd scavenger.
    _ = ?M:start_scavenger(BrickName, Node, []),
    timer:sleep(500),
    %% io:format("dump 2: ~p\n", [ets:tab2list(zoo_store)]),
    unlink(whereis(BrickName)),
    exit(whereis(BrickName), kill),
    timer:sleep(100),
    {ok, _} = safe_start_link(BrickName, OptionList),
    timer:sleep(500),
    ok = ?M:chain_role_standalone(BrickName, node()),
    ok = ?M:chain_set_my_repair_state(BrickName, node(), ok),

    {ok, _TS, Val} = ?M:get(BrickName, Node, Key),

    io:format("Test t91: end: ~p\n", [OptionList]),
    ok.


cl_chain_t35() ->
    ok = reliable_gdss_stop(),
    os:cmd("rm -fr Schema.local hlog.*"),
    application:start(gdss),
    brick_admin:bootstrap_local([], true, $/, 3, 1, 1, []),

    chain_t35([]),

    ok = reliable_gdss_stop(),
    os:cmd("rm -fr Schema.local hlog.*").


cl_single_brick_regression() ->
    application:start(gdss),
    ok = single_brick_regression().

cl_chain_all() ->
    application:start(gdss),
    ok = brick_test0:chain_all().

cl_simple_distrib_test(_X) ->
    ?DBG(_X).

cl_old_distrib_test(_X) ->
    ?DBG(_X).

loopN(N, Fun) ->
    loopN2(N, Fun, false).
loopNverbose(N, Fun) ->
    loopN2(N, Fun, true).

loopN2(0, _Fun, _Verbose) ->
    ok;
loopN2(N, Fun, Verbose) ->
    %io:format("QQQ: 1\n"),
    Fun(N),
    %io:format("QQQ: 2\n"),
    if Verbose, N rem 1000 == 0 -> io:format("Progress: N = ~w\n", [N]);
       true -> ok
    end,
    %io:format("QQQ: 3\n"),
    loopN2(N - 1, Fun, Verbose).

loop_msg_stop(Fun, Extra) ->
    loop_msg_stop(1, Fun, Extra).

loop_msg_stop(N, Fun, Extra) ->
    receive
        {stop, Parent} ->
            Parent ! {self(), N},
            exit(normal)
    after 0 ->
            ok
    end,
    Fun(N, Extra),
    loop_msg_stop(N + 1, Fun, Extra).

collect_counts(Pids) ->
    [P ! {stop, self()} || P <- Pids],
    lists:foldl(
      fun(P, Acc) -> receive {P, Count} -> Acc + Count end end,
      0, Pids).

%% WARNING: May *not* return exactly NumPieces lists, may be more or less!

chop_list(L, NumPieces) ->
    Len = length(L),
    NumEach = round(Len / NumPieces),
    chop_list(L, NumEach, [], [], NumEach).

chop_list([], _Small, SmallAcc, BigAcc, _NumEach) ->
    lists:reverse([lists:reverse(SmallAcc) | BigAcc]);
chop_list(L, 0, SmallAcc, BigAcc, NumEach) ->
    chop_list(L, NumEach, [], [lists:reverse(SmallAcc)|BigAcc], NumEach);
chop_list([H|T], Small, SmallAcc, BigAcc, NumEach) ->
    chop_list(T, Small - 1, [H|SmallAcc], BigAcc, NumEach).

wait_for_checkpoint_to_finish(BrickName, Node) ->
    Fwait = fun(X) ->
                    {ok, Status} = brick_server:status(BrickName, Node),
                    Is = proplists:get_value(implementation, Status),
                    case proplists:get_value(checkpoint, Is) of
                        undefined ->
                            {false, done};
                        Pid ->
                            if Pid /= X -> ?DBG(Pid); true -> ok end,
                            timer:sleep(100),
                            {true, Pid}
                    end
            end,
    done = gmt_loop:do_while(Fwait, x),
    ok.

chain_all() ->
    chain_all(make_brick_option_lists()).

chain_all(OptLists) ->
    application:start(gdss),
    start_and_flush_common_log(),
    [ok = chain_all2(OptList) || OptList <- OptLists],
    ok = chain_t100(),
    ok = chain_t101(),
    ok = chain_t102(),
    io:format("Chain regression tests PASS (don't forget manual checks!)\n").

chain_individual_tests() ->
    [chain_t0, chain_t10, chain_t20, chain_t25, chain_t30, chain_t31, chain_t35,
     chain_t40, chain_t41, chain_t50, chain_t51, chain_t54, chain_t55,
     chain_t56, chain_t60, chain_t61, chain_t62].

chain_all2(OptionList) ->
    io:format("Chain regression tests starting at ~p with brick options ~w\n\n", [time(), OptionList]),

    [ok = ?MODULE:Test(OptionList) || Test <- chain_individual_tests()],

    io:format("Chain regression tests PASS with brick options ~w\n\n", [OptionList]),
    ok.

chain_t0(_OptionList) ->
    ChainList1 = [],
    {'EXIT', _} = (catch brick_hash:naive_init(ChainList1)),

    %% Shouldn't matter if chains have asymmetric lengths.
    ChainList2 = [{chain1, [{b1, n1}]}, {chain2, [{b2, n2}, {b3, n3}]}],
    LH2 = brick_hash:naive_init(ChainList2),
    GH2 = brick_hash:init_global_hash_state(
            false, unused, 1, LH2, ChainList2, LH2, ChainList2),
    [{_,_} = brick_hash:key_to_brick(RW, K, GH2) ||
        RW <- [read, write],
        K <- [key1, key2, key3, <<"key1">>, <<"key2">>, <<"key3">>]],

    ok.

chain_t10(OptionList) ->
    NameHead = regression_head,
    NameTail = regression_tail,
    chain_t10(NameHead, NameTail, true, true, true, true, OptionList).

chain_t10(NameHead, NameTail, StartupP, InsertP, EtsTestP, KillP, OptionList) ->
    %% NOTE: We're only testing chains of length = 2.
    Node = node(),
    NameHeadETS = list_to_atom(atom_to_list(NameHead) ++ "_store"),
    NameTailETS = list_to_atom(atom_to_list(NameTail) ++ "_store"),

    if StartupP == true ->
            catch exit(whereis(NameHead), byebye),
            FilesH = filelib:wildcard("*." ++ atom_to_list(NameHead) ++ ",*.LOG", "."),
            [file:delete(F) || F <- FilesH],
            start_and_flush_common_log(),
            os:cmd("rm -rf hlog.*" ++ atom_to_list(NameHead) ++ "*"),
            {ok, _HeadPid} = safe_start_link(NameHead, OptionList),

            catch exit(whereis(NameTail), byebye),
            FilesT = filelib:wildcard("*." ++ atom_to_list(NameTail) ++ ",*.LOG", "."),
            [file:delete(F) || F <- FilesT],
            start_and_flush_common_log(),
            os:cmd("rm -rf hlog.*" ++ atom_to_list(NameTail) ++ "*"),
            {ok, _TailPid} = safe_start_link(NameTail, OptionList);
       true ->
            ok
    end,

    Odds = [{"foo1", "one"},
            {"foo3", "three"},
            {"foo5", "five"},
            {"foo7", "seven"},
            {"foo9", "nine"}],
    ManyEvens = [{"foo0", "zero"},
                 {"foo2", "two"},
                 %% {"foo4", "four"},
                 %% {"foo6", "six"},
                 {"foo8", "eight"}],
    StuffAtEnd = [{"zzzzzzzzzz1", "foo"},
                  {"zzzzzzzzzz9", "foo bar"}],

    if InsertP == true ->
            %% Put some "old" stuff into the will-be-tail
            ?M:chain_role_standalone(NameTail, Node),
            ?M:chain_set_my_repair_state(NameTail, Node, ok),
            [ok = ?M:set(NameTail, Node, K, V) || {K, V} <- ManyEvens],
            [ok = ?M:set(NameTail, Node, K, V) || {K, V} <- StuffAtEnd],

            %% Put some "new" stuff into the will-be-head.
            ?M:chain_role_standalone(NameHead, Node),
            ?M:chain_set_my_repair_state(NameHead, Node, ok),
            [ok = ?M:set(NameHead, Node, K, V) || {K, V} <- Odds];
       true ->
            ok
    end,

    if StartupP == true ->
            %% Set the will-be-tail => tail
            ?M:chain_set_my_repair_state(NameTail, Node, testing_harness_force_pre_init),
            ok = poll_brick_status(NameTail, Node),
            ?M:chain_role_tail(NameTail, Node, NameHead, Node, []),
            pre_init = ?M:chain_get_my_repair_state(NameTail, Node),

            %% Set the will-be-head => head
            ok = poll_brick_status(NameHead, Node),
            ?M:chain_role_head(NameHead, Node, NameTail, Node, []),
            ok = ?M:chain_get_my_repair_state(NameHead, Node),
            pre_init = ?M:chain_get_ds_repair_state(NameHead, Node);
       true ->
            ok
    end,

    if InsertP == true ->
            %% Set some stuff in head before starting repair.  These things
            %% should be propagated to tail now.
            ok = ?M:set(NameHead, Node, "zoo1", "the zoo 1"),
            timer:sleep(200),
            ok = ?M:set(NameHead, Node, "zoo2", "the zoo 2"),
            timer:sleep(200);
       true ->
            ok
    end,

    if StartupP == true ->
            ok = ?M:chain_start_repair(NameHead, Node),
            {error, _, _} = ?M:chain_start_repair(NameHead, Node),
            %% We're vulnerable to race conditions here, 200ms may not
            %% be enough.  To fix, we'd have to poll repair state of
            %% remote bricks.
            timer:sleep(500);
       true ->
            ok
    end,

    if EtsTestP == true ->
            HM2 = xform_ets_dump(NameHead, ets:tab2list(NameHeadETS)),
            TM2 = xform_ets_dump(NameTail, ets:tab2list(NameTailETS)),
            ?DBG(HM2),
            ?DBG(TM2),
            HM2 = TM2,
            io:format("GOOD, both bricks are equal.\n");
       true ->
            ok
    end,

    if KillP == true ->
            ?M:stop(whereis(NameHead)),
            ?M:stop(whereis(NameTail));
       true ->
            ok
    end,

    ok.

chain_t20(OptionList) ->
    %% NOTE: We're only testing chains of length = 2.
    io:format("Test chain_t20: start\n"),
    Ch1Head = ch1_head,
    Ch1Tail = ch1_tail,
    Node = node(),
    _ = flush_mailbox(),
    chain_t10(Ch1Head, Ch1Tail, true, true, true, false, OptionList),

    {ok, _, <<"seven">>} = ?M:get(Ch1Head, Node, "foo7"),
    key_not_exist = ?M:get(Ch1Head, Node, "foo8"),

    ChainList = [{chain1, [{Ch1Head, Node}, {Ch1Tail, Node}]}],
    LH = brick_hash:naive_init(ChainList),
    GH = brick_hash:init_global_hash_state(
            false, unused, 1, LH, ChainList, LH, ChainList),

    %% There's only 1 chain, so we can predict exactly which brick will
    %% be used for write vs. read.
    {Ch1Head, Node} = brick_hash:key_to_brick(write, key_foobar, GH),
    {Ch1Tail, Node} = brick_hash:key_to_brick(read, key_foobar, GH),

    %% Set the global hash for each brick, so each should pay attention
    %% to its read vs. write role.
    ok = ?M:chain_hack_set_global_hash(Ch1Head, Node, GH),
    ok = ?M:chain_hack_set_global_hash(Ch1Tail, Node, GH),

    Get_foo7 = ?M:make_get("foo7"),
    Do_get_foo7 = [Get_foo7],
    Set_zzz9 = ?M:make_set("zzz9", <<"junk stuff">>),
    Do_set_zzz9 = [Set_zzz9],

    %% Reads to the tail are OK.
    [{ok, _, <<"seven">>}] = ?M:do(Ch1Tail, Node, Do_get_foo7, [], 5*1000),
    [{ok, _, <<"seven">>}] = ?M:do(Ch1Tail, Node, Do_get_foo7, [ignore_role], 5*1000),

    %% Sets to the head are OK.
    [ok] = ?M:do(Ch1Head, Node, Do_set_zzz9, [], 5*1000),
    %% QQQ = ?M:do(Ch1Head, Node, [?M:make_get("zzz9")], [], 5*1000), io:format("QQQ: QQQ = ~p\n", [QQQ]), io:format("QQQ: head tab = ~p\n", [ets:tab2list(ch1_head_store)]), io:format("QQQ: tail tab = ~p\n", [ets:tab2list(ch1_tail_store)]),
    [ok] = ?M:do(Ch1Head, Node, Do_set_zzz9, [ignore_role], 5*1000),

    %% Sets to the tail are wrong for the tail's role.

    %% Normally, the tail should forward the call to the head.
    [ok] = ?M:do(Ch1Tail, Node, Do_set_zzz9, [], 5*1000),
    %% This is dangerously-silly to send a set + ignore_role to the tail,
    %% but we'll do it here for the heck of it.
    [ok] = ?M:do(Ch1Tail, Node, Do_set_zzz9, [ignore_role], 5*1000),
    %% Definitely fail.
    {wrong_brick, _} = ?M:do(Ch1Tail, Node, Do_set_zzz9, [fail_if_wrong_role], 5*1000),

    %% Reads to the head are wrong for the head's role.

    %% Normally, the head should forward the call to the tail.
    [{ok, _, <<"seven">>}] = ?M:do(Ch1Head, Node, Do_get_foo7, [], 5*1000),
    %% This is safe if violating strong consistency.
    [{ok, _, <<"seven">>}] = ?M:do(Ch1Head, Node, Do_get_foo7, [ignore_role], 5*1000),
    %% Definitely fail.
    {wrong_brick, _} = ?M:do(Ch1Head, Node, Do_get_foo7, [fail_if_wrong_role], 5*1000),

    %% Simple, non-exhaustive testing of brick_simple interface.

    brick_simple:start_link(),
    Tab = 'my-testing-tab',
    brick_simple:unset_gh(Node, Tab),
    brick_simple:set_gh(Node, Tab, GH),
    %%
    {ok, _, <<"five">>} = brick_simple:get(Tab, "foo5"),
    {key_exists, _} = brick_simple:add(Tab, "foo5", "gonna-fail"),
    ok = brick_simple:replace(Tab, "foo5", "ah hah!"),
    {ok, _, <<"ah hah!">>} = brick_simple:get(Tab, "foo5"),
    ok = brick_simple:delete(Tab, "foo5"),
    key_not_exist = brick_simple:replace(Tab, "foo5", "ah hah!"),
    ok = brick_simple:add(Tab, "foo5", "ah hah!"),
    {key_exists, _} = brick_simple:add(Tab, "foo5", "ah hah!"),
    %% Put it back to the way it was before messing with it.
    ok = brick_simple:replace(Tab, "foo5", "five"),

    %% Create bad hash state...
    BogusBrick = {blarf, zoof@zoof},
    ChainListBad = [{chain1b, [BogusBrick]}],
    LHBad = brick_hash:naive_init(ChainListBad),
    GHBad_0 = brick_hash:init_global_hash_state(
                false, unused, 1, LHBad, ChainListBad, LHBad, ChainListBad),
    GHBad = GHBad_0#g_hash_r{minor_rev = GH#g_hash_r.minor_rev + 1},
    %% ... then throw it in.
    ok = ?M:chain_hack_set_global_hash(Ch1Head, Node, GHBad),
    ok = ?M:chain_hack_set_global_hash(Ch1Tail, Node, GHBad),
    %% ... and see the failures.
    {wrong_brick, [BogusBrick]} =
        ?M:do(Ch1Head, Node, Do_get_foo7, [fail_if_wrong_role], 5*1000),
    {wrong_brick, [BogusBrick]} =
        ?M:do(Ch1Tail, Node, Do_set_zzz9, [fail_if_wrong_role], 5*1000),
    %% If we try these, we'll get timeout errors instead: the called
    %% brick will forward the call via gen_server:cast/2, and we'll never
    %% get any reply from zoof@zoof.
    %% {wrong_brick, _} = ?M:set(Ch1Head, Node, "foo7", "gonna-fail"),
    %% {wrong_brick, _} = ?M:set(Ch1Tail, Node, "foo7", "gonna-fail"),

    %% There should be no extra messages waiting for us in our mailbox!
    %% Did someone sent a veryvery bad duplicate reply?
    timer:sleep(200),
    [] = flush_mailbox(),

    ?M:stop(whereis(Ch1Head)),
    ?M:stop(whereis(Ch1Tail)),
    io:format("Test chain_t20: end\n"),
    ok.

chain_t25(OptionList) ->
    %% NOTE: Smoke test only for chain length = 3
    io:format("Test chain_t25: start\n"),
    Ch1Head = ch1_head,
    Ch1Middle = ch1_middle,
    Ch1Tail = ch1_tail,
    Node = node(),

    %% Make a 2-chain first.
    chain_t10(Ch1Head, Ch1Middle, true, true, true, false, OptionList),
    %% Sanity test.
    {ok, _, <<"seven">>} = ?M:get(Ch1Head, Node, "foo7"),
    key_not_exist = ?M:get(Ch1Head, Node, "foo8"),

    %% Rearrange the 2-chain to 3-chain.
    start_and_flush_common_log(),
    os:cmd("rm -rf hlog.*" ++ atom_to_list(Ch1Tail) ++ "*"),
    {ok, _TailPid} = safe_start_link(Ch1Tail, OptionList),
    pre_init = ?M:chain_get_my_repair_state(Ch1Tail, Node),
    ok = ?M:chain_set_my_repair_state(Ch1Middle, Node, testing_harness_force_pre_init),
    ok = ?M:chain_role_tail(Ch1Tail, Node, Ch1Middle, Node, []),
    ok = ?M:chain_role_middle(Ch1Middle, Node,
                              Ch1Head, Node, Ch1Tail, Node, []),
    ok = ?M:chain_role_head(Ch1Head, Node, Ch1Middle, Node, []),

    %% Repairs don't (yet?) cascade more than 1, so we do this one at a time.
    ok = ?M:chain_start_repair(Ch1Head, Node),
    _ = repair_poll(Ch1Head, Node, 100),
    ok = ?M:chain_start_repair(Ch1Middle, Node),
    _Gloo = repair_poll(Ch1Middle, Node, 100),
    ok = ?M:chain_role_tail(Ch1Tail, Node, Ch1Middle, Node, []),

    ChainList = [{chain1,
                  [{Ch1Head, Node}, {Ch1Middle, Node}, {Ch1Tail, Node}]}],
    LH = brick_hash:naive_init(ChainList),
    GH = brick_hash:init_global_hash_state(
            false, unused, 1, LH, ChainList, LH, ChainList),
    ok = ?M:chain_hack_set_global_hash(Ch1Head, Node, GH),
    ok = ?M:chain_hack_set_global_hash(Ch1Middle, Node, GH),
    ok = ?M:chain_hack_set_global_hash(Ch1Tail, Node, GH),

    %% Simple, non-exhaustive testing of brick_simple interface.
    %% QQQ stolen from chain_t20()

    F_stolen_from_chain_t20 =
        fun(MyGH) ->
                brick_simple:start_link(),
                Tab = 'my-testing-tab',
                brick_simple:unset_gh(Node, Tab),
                brick_simple:set_gh(Node, Tab, MyGH),
                %%
                {ok, _, <<"five">>} = brick_simple:get(Tab, "foo5"),
                {key_exists, _} = brick_simple:add(Tab, "foo5", "gonna-fail"),
                ok = brick_simple:replace(Tab, "foo5", "ah hah!"),
                {ok, _, <<"ah hah!">>} = brick_simple:get(Tab, "foo5"),
                ok = brick_simple:delete(Tab, "foo5"),
                key_not_exist = brick_simple:replace(Tab, "foo5", "ah hah!"),
                ok = brick_simple:add(Tab, "foo5", "ah hah!"),
                {key_exists, _} = brick_simple:add(Tab, "foo5", "ah hah!"),
                %% Put it back to the way it was before messing with it.
                ok = brick_simple:replace(Tab, "foo5", "five"),

                ok
        end,

    ok = F_stolen_from_chain_t20(GH),

    %% Sanity checking
    Get_foo5 = ?M:make_get("foo5"),
    Do_get_foo5 = [Get_foo5],
    %% Everyone should have the same value for "foo5"
    [{ok, _, <<"five">>}] = ?M:do(Ch1Head, Node, Do_get_foo5, [ignore_role], 5*1000),
    [{ok, _, <<"five">>}] = ?M:do(Ch1Middle, Node, Do_get_foo5, [ignore_role], 5*1000),
    [{ok, _, <<"five">>}] = ?M:do(Ch1Tail, Node, Do_get_foo5, [ignore_role], 5*1000),

    %% Chop off the tail.
    %% Don't worry about "freezing"/"read-only" mode: we're the only writer.

    ok = ?M:chain_role_tail(Ch1Middle, Node, Ch1Head, Node, []),
    ok = ?M:chain_role_undefined(Ch1Tail, Node),
    ok = ?M:chain_set_my_repair_state(Ch1Middle, Node, testing_harness_force_pre_init),
    ok = ?M:chain_set_ds_repair_state(Ch1Head, Node, pre_init),
    ok = ?M:chain_start_repair(Ch1Head, Node),
    _ = repair_poll(Ch1Head, Node, 100),

    %% Reset global hash state.
    ChainList1 = [{chain1,
                   [{Ch1Head, Node}, {Ch1Middle, Node}]}],
    LH1 = brick_hash:naive_init(ChainList1),
    GH1_0 = brick_hash:init_global_hash_state(
              false, unused, 1, LH1, ChainList1, LH1, ChainList1),
    GH1 = GH1_0#g_hash_r{minor_rev = GH#g_hash_r.minor_rev + 1},
    ok = ?M:chain_hack_set_global_hash(Ch1Head, Node, GH1),
    ok = ?M:chain_hack_set_global_hash(Ch1Middle, Node, GH1),

?DBG(?M:chain_get_my_repair_state(Ch1Head, Node)),
?DBG(?M:chain_get_ds_repair_state(Ch1Head, Node)),
?DBG(?M:chain_get_my_repair_state(Ch1Middle, Node)),

    %% Try again with the shorter tail.
    ok = F_stolen_from_chain_t20(GH1),

    %% Grow back to a 3-chain.
    %% Try sending queries during the entire process.

    %% We need to use the proplist now to fiddle with the "official_tail"
    %% status: we need the middle to continue acting as a tail while
    %% the repair is taking place, and we need the tail to *not* respond
    %% to queries like a tail would.
    ok = ?M:chain_set_my_repair_state(Ch1Tail, Node, testing_harness_force_pre_init),
    ok = ?M:chain_role_tail(Ch1Tail, Node, Ch1Middle, Node,
                            [{official_tail, false}]),
    ok = ?M:chain_set_ds_repair_state(Ch1Middle, Node, pre_init),
    ok = ?M:chain_role_middle(Ch1Middle, Node, Ch1Head, Node, Ch1Tail, Node,
                              [{official_tail, true}]),
    % Sanity
    [{ok, _, <<"five">>}] = ?M:do(Ch1Head, Node, Do_get_foo5, [ignore_role], 5*1000),
    [{ok, _, <<"five">>}] = ?M:do(Ch1Middle, Node, Do_get_foo5, [ignore_role], 5*1000),
    [{error,current_repair_state,pre_init}] =
        ?M:do(Ch1Tail, Node, Do_get_foo5, [ignore_role], 5*1000),
    %% Remember: Ch1Head and Ch1Middle still think they're head & tail.
    {ok, _, <<"five">>} = ?M:get(Ch1Head, Node, "foo5"), % will forward to middle
    {ok, _, <<"five">>} = ?M:get(Ch1Middle, Node, "foo5"),
    {error,current_repair_state,pre_init} = ?M:get(Ch1Tail, Node, "foo5"),

    Parent = self(),
    SendQs =
        fun(Acc) ->
                {ok, _, <<"five">>} = ?M:get(Ch1Head, Node, "foo5"),
                {ok, _, <<"five">>} = ?M:get(Ch1Middle, Node, "foo5"),
                if Acc < -5 ->
                        Parent ! ok_i_have_stopped,
                        {false, Acc};
                   Acc < 0 ->
                        {true, Acc - 1};
                   true ->
                        receive please_stop ->
                                io:format("ZZZ: SendQs looped ~w times\n", [Acc]),
                                {true, -1}
                        after 0 ->
                                {true, Acc + 1}
                        end
                end
        end,
    Pid1 =
        spawn(fun() ->
                      gmt_loop:do_while(SendQs, 0),
                      unlink(Parent)
              end),
    ok = ?M:chain_start_repair(Ch1Middle, Node),
    NumPolls = repair_poll(Ch1Middle, Node, 100),
    io:format("ZZZ: RepairPoll looped ~w times\n", [NumPolls]),

    %% The SendQs loop should still be working 100% successfully while
    %% we make these changes.
    ok = ?M:chain_role_tail(Ch1Tail, Node, Ch1Middle, Node, []),
    timer:sleep(20),
    ok = ?M:chain_role_middle(Ch1Middle, Node, Ch1Head, Node, Ch1Tail, Node,
                              []),
    timer:sleep(20),
    %% Recall: GH has a single 3-chain
    GH2 = GH#g_hash_r{minor_rev = GH1#g_hash_r.minor_rev + 1},
    ok = ?M:chain_hack_set_global_hash(Ch1Head, Node, GH2),
    timer:sleep(20),
    ok = ?M:chain_hack_set_global_hash(Ch1Middle, Node, GH2),
    timer:sleep(20),
    ok = ?M:chain_hack_set_global_hash(Ch1Tail, Node, GH2),
    {ok, _, <<"five">>} = ?M:get(Ch1Head, Node, "foo5"),
    {ok, _, <<"five">>} = ?M:get(Ch1Middle, Node, "foo5"),
    {ok, _, <<"five">>} = ?M:get(Ch1Tail, Node, "foo5"),

    Pid1 ! please_stop,
    %% Pid1 will loop 5 more times after getting this message.
    receive ok_i_have_stopped -> ok after 5000 -> throw(t25_err_1) end,

    %% Uncomment for additional sanity hand-holding, if needed, but will
    %% only be helpful if gmt_debug = true over on the server side is
    %% also true.
%%     io:format("\n\n\n"),
%%     io:format("-------- Sending get to head:\n"),
%%     {ok, _, "five"} = ?M:get(Ch1Head, Node, "foo5"),
%%     io:format("-------- Sending get to middle:\n"),
%%     {ok, _, "five"} = ?M:get(Ch1Middle, Node, "foo5"),
%%     io:format("-------- Sending get to tail:\n"),
%%     {ok, _, "five"} = ?M:get(Ch1Tail, Node, "foo5"),
%%     io:format("\n\n\n"),

    ?M:stop(whereis(Ch1Head)),
    ?M:stop(whereis(Ch1Middle)),
    ?M:stop(whereis(Ch1Tail)),
    io:format("Test chain_t25: end\n"),
    ok.

chain_t30(OptionList) ->
    io:format("Test chain_t30: start: ~p\n", [OptionList]),

    start_and_flush_common_log(),
    os:cmd("rm -rf hlog.test_ch*_*"),
    brick_simple:start_link(),
    brick_shepherd:start_link(),

    Tab1 = table1,
    Tab2 = table2,
    Tab3 = table3,
    Tabs = [Tab1, Tab2, Tab3],
    ChainGrp1 = brick_hash:invent_nodelist(5, 2, node(), 10),
    ChainGrp2 = brick_hash:invent_nodelist(3, 4, node(), 20),
    ChainGrp3 = brick_hash:invent_nodelist(2, 1, node(), 37),
    ChainGrps = [ChainGrp1, ChainGrp2, ChainGrp3],
    AllBricks = lists:append(
                  [BrickList || ChGrp <- ChainGrps, {_, BrickList} <- ChGrp]),
    LH1 = brick_hash:naive_init(ChainGrp1),
    LH2 = brick_hash:naive_init(ChainGrp2),
    LH3 = brick_hash:naive_init(ChainGrp3),
    _LHs = [LH1, LH2, LH3],
    GH1 = brick_hash:init_global_hash_state(false, unused, 1,
                                                LH1, ChainGrp1,
                                                LH1, ChainGrp1),
    GH2 = brick_hash:init_global_hash_state(false, unused, 1,
                                                LH2, ChainGrp2,
                                                LH2, ChainGrp2),
    GH3 = brick_hash:init_global_hash_state(false, unused, 1,
                                                LH3, ChainGrp3,
                                                LH3, ChainGrp3),
    GHs = [GH1, GH2, GH3],
    TestKeys = ["key" ++ integer_to_list(I) || I <- lists:seq(1, 20)],
    KsVs1 = [{list_to_binary(K ++ "_table1"),
              list_to_binary("Table 1 value for " ++ K)} || K <- TestKeys],
    KsVs2 = [{list_to_binary(K ++ "_table2"),
              list_to_binary("Table 2 value for " ++ K)} || K <- TestKeys],
    KsVs3 = [],
    KsVss = [KsVs1, KsVs2, KsVs3],

    %% Start all the chains
    [brick_test0:chain_start_entire_chain(C, OptionList, true) ||
        ChGrp <- ChainGrps, C <- ChGrp],

    %% Create a list of tuples: {ChainName, BrickName, Node, GlobalHash}
    Z1 = lists:zip(ChainGrps, GHs),
    Tmp0 = lists:append(
             lists:map(
               fun({ChainList, Gh}) ->
                       lists:map(
                         fun({ChName, ChNds}) ->
                                 [{ChName, Br, Nd, Gh} || {Br, Nd} <- ChNds]
                         end, ChainList)
               end, Z1)),
    Tmp1 = lists:append(Tmp0),                  % Flatten one more level.

    %% Set each brick's view if their table's global hash.
    [ok = brick_server:chain_hack_set_global_hash(Brick, Node, TheGH) ||
        {_ChainName, Brick, Node, TheGH} <- Tmp1],

    %% Set brick_simple's view of tables.
    Tmp2 = lists:zip(Tabs, GHs),
    [begin
         brick_simple:unset_gh(node(), Tab),
         brick_simple:set_gh(node(), Tab, TheGH)
     end || {Tab, TheGH} <- Tmp2],

    %% Create a list of tuples: {Tab, Key, Val}
    ToStore = lists:append(
                lists:map(
                  fun({Tab, KVList, Gh}) ->
                          [{Tab, K, V, Gh} || {K, V} <- KVList] end,
                  lists:zip3(Tabs, KsVss, GHs))),

    %% Put some stuff into each table
    [ok = brick_simple:set(Tab, Key, Val) ||
        {Tab, Key, Val, _} <- ToStore],

    %% Verify that each value is where we expect it ...
    %% and that it doesn't exist on any other brick.
    lists:foreach(
      fun({Tab, KVList, GH}) ->
              lists:foreach(
                fun({Key, Val}) ->
                        {ok, _, Val} = brick_simple:get(Tab, Key),
                        [key_not_exist = brick_simple:get(OtherT, Key) ||
                            OtherT <- Tabs -- [Tab]],
                        ChainName = brick_hash:key_to_chain(Key, GH),
                        {value, {ChainName, ChainBricks}} =
                            lists:keysearch(ChainName, 1,
                                            lists:append(ChainGrps)),
                        lists:foreach(
                          fun({Br, Nd} = Brick) ->
                                  %% If we use brick_server:get/3, and if
                                  %% we're unlucky to send to another chain for
                                  %% the same table, then the automatic request
                                  %% forwarding will send our query to the
                                  %% proper tail, and then we'll get an ok
                                  %% answer.  So we must use ignore_role for
                                  %% this check.
                                  DoList = [brick_server:make_get(Key,[])],
                                  Ret = brick_server:do(Br, Nd, DoList,
                                                          [ignore_role], 1000),
                                  case lists:member(Brick, ChainBricks) of
                                      true ->
                                          [{ok, _, Val}] = Ret;
                                      false ->
                                          {Key, Br, Nd, ChainName, ChainBricks, [key_not_exist]} =
                                              {Key, Br, Nd, ChainName, ChainBricks, Ret}
                                  end
                          end, AllBricks),
                        ok
                end, KVList)
      end, lists:zip3(Tabs, KsVss, GHs)),

    [rpc:call(Node, brick_shepherd, stop_brick, [Brick]) ||
        {Brick, Node} <- AllBricks],
    io:format("Test chain_t30: end\n"),
    ok.

chain_t31(OptionList) ->
    %% Hm, this is sharing quite a bit of setup with chain_t30(), hrm.
    io:format("Test chain_t31: start\n"),

    start_and_flush_common_log(),
    os:cmd("rm -rf hlog.test_ch*_*"),
    brick_simple:start_link(),
    Tab1 = table1,
    Tab2 = table2,
    Tabs = [Tab1, Tab2],
    ChainGrp1 = brick_hash:invent_nodelist(2, 3, node(), 10),
    ChainGrp2 = brick_hash:invent_nodelist(3, 3, node(), 20),
    ChainGrps = [ChainGrp1, ChainGrp2],
    LH1 = brick_hash:naive_init(ChainGrp1),
    LH2 = brick_hash:naive_init(ChainGrp2),
    GH1 = brick_hash:init_global_hash_state(false, unused, 1,
                                                LH1, ChainGrp1,
                                                LH1, ChainGrp1),
    GH2 = brick_hash:init_global_hash_state(false, unused, 1,
                                                LH2, ChainGrp2,
                                                LH2, ChainGrp2),
    GHs = [GH1, GH2],
    TestKeys = ["key" ++ integer_to_list(I) || I <- lists:seq(1, 20)],
    KsVs1 = [{list_to_binary(K ++ "_table311"),
              list_to_binary("Table 311 value for " ++ K)} || K <- TestKeys],
    KsVs2 = [{list_to_binary(K ++ "_table312"),
              list_to_binary("Table 312 value for " ++ K)} || K <- TestKeys],
    KsVss = [KsVs1, KsVs2],

    [brick_test0:chain_start_entire_chain(C, OptionList, true) ||
        ChGrp <- ChainGrps, C <- ChGrp],

    %% Create a list of tuples: {ChainName, BrickName, Node, GlobalHash}
    Fzip_chbrndgh =
        fun(ChainGrps_0, GHs_0) ->
                Z1 = lists:zip(ChainGrps_0, GHs_0),
                Tmp0 = lists:append(
                         lists:map(
                           fun({ChainList, Gh}) ->
                                   lists:map(
                                     fun({ChName, ChNds}) ->
                                             [{ChName, Br, Nd, Gh} || {Br, Nd} <- ChNds]
                                     end, ChainList)
                           end, Z1)),
                lists:append(Tmp0)              % Flatten one more level.
        end,
    Tmp1 = Fzip_chbrndgh(ChainGrps, GHs),

    %% Set each brick's view if their table's global hash.
    [ok = brick_server:chain_hack_set_global_hash(Brick, Node, TheGH) ||
        {_ChainName, Brick, Node, TheGH} <- Tmp1],

    %% Set brick_simple's view of tables.
    Tmp2 = lists:zip(Tabs, GHs),
    [begin
         brick_simple:unset_gh(node(), Tab),
         brick_simple:set_gh(node(), Tab, TheGH)
     end || {Tab, TheGH} <- Tmp2],

    %% Create a list of tuples: {Tab, Key, Val}
    ToStore = lists:append(
                lists:map(
                  fun({Tab, KVList, Gh}) ->
                          [{Tab, K, V, Gh} || {K, V} <- KVList] end,
                  lists:zip3(Tabs, KsVss, GHs))),

    %% Put some stuff into each table
    [ok = brick_simple:set(Tab, Key, Val) ||
        {Tab, Key, Val, _} <- ToStore],

    %% Verify that each value is where we expect it ...
    %% and that it doesn't exist on any other brick.
    F_verify = fun() ->
                       lists:foreach(
                         fun({Tab, KVList, _GH}) ->
                                 lists:foreach(
                                   fun({Key, Val}) ->
                                           {ok, _, Val} = brick_simple:get(Tab, Key),
                                           io:format("."),
                                           ok
                                   end, KVList)
                         end, lists:zip3(Tabs, KsVss, GHs)),
                       ok
               end,
    ok = F_verify(), io:format("F_verify 1 done\n"),

    F_choptail = fun(ChainGrp) ->
                         [{Chain, tl(Bricks)} || {Chain, Bricks} <- ChainGrp]
                 end,
    ChainGrp1b = F_choptail(ChainGrp1),
    ChainGrp2b = F_choptail(ChainGrp2),
    ChainGrpsb = [ChainGrp1b, ChainGrp2b],
    LH1b = brick_hash:naive_init(ChainGrp1b),
    LH2b = brick_hash:naive_init(ChainGrp2b),
    GH1b = brick_hash:init_global_hash_state(false, unused, 1,
                                                 LH1b, ChainGrp1b,
                                                 LH1b, ChainGrp1b),
    GH2b = brick_hash:init_global_hash_state(false, unused, 1,
                                                 LH2b, ChainGrp2b,
                                                 LH2b, ChainGrp2b),
    GHsb = [GH1b, GH2b],
    Tmp1b = Fzip_chbrndgh(ChainGrps, GHs),

    %% Start all the chains
    [brick_test0:chain_start_entire_chain(C, OptionList, false) ||
        ChGrp <- ChainGrpsb, C <- ChGrp],

    %% Set each brick's view if their table's global hash.
    [ok = brick_server:chain_hack_set_global_hash(Brick, Node, TheGH) ||
        {_ChainName, Brick, Node, TheGH} <- Tmp1b],

    %% Set brick_simple's view of tables.
    Tmp2b = lists:zip(Tabs, GHsb),
    [begin
         brick_simple:unset_gh(node(), Tab),
         brick_simple:set_gh(node(), Tab, TheGH)
     end || {Tab, TheGH} <- Tmp2b],

    ok = F_verify(), io:format("F_verify 2 done\n"),

    %% Shut down and go home.
    AllBricks = lists:append(
                  [BrickList || ChGrp <- ChainGrps, {_, BrickList} <- ChGrp]),
    [rpc:call(Node, brick_shepherd, stop_brick, [Brick]) ||
        {Brick, Node} <- AllBricks],
    io:format("Test chain_t31: end\n"),
    ok.

chain_t35(OptionList) ->
    %% This is not a chain test per se, but the simple quorum stuff must
    %% be bug-free in order to support the work of the chain admin server.
    io:format("Test chain_t35: start: ~w\n", [OptionList]),

    Bricks = [{'e8mP1', node()}, {'e8mP2', node()}, {'e8mP3', node()},
              {'e8mP4', node()}, {'e8mP5', node()}, {'e8mP6', node()},
              {'e8mP7', node()}, {'e8mP8', node()}, {'e8mP9', node()}],
    lists:foreach(fun({Brick, Node}) ->
                          start_and_flush_common_log(),
                          os:cmd("rm -f *e8mP*.LOG"),
                          os:cmd("rm -rf hlog.*" ++ atom_to_list(Brick) ++ "*"),
                          {ok, _HeadPid} = safe_start_link(Brick, OptionList),
%                         brick_server:set_do_sync({Brick, Node}, false),
                          ok = ?M:chain_role_standalone(Brick, Node),
                          ok = ?M:chain_set_my_repair_state(Brick, Node, ok)
                  end, Bricks),
    Ks = [list_to_binary([X]) || X <- lists:seq($1, $5)],

    Vs = [list_to_binary(lists:duplicate(10, K)) || K <- Ks],
    KsVs = lists:zip(Ks, Vs),

    %% Test repairs when single-brick deletions destroy < quorum copies.
    lists:foreach(
      fun(QSize) ->
              Bricks0 = lists:sublist(Bricks, QSize),
              lists:foreach(
                fun({K, V}) ->
                        MinQuorum = ((QSize - 1) div 2) + 1,
                        NumFails = if QSize == 2 ->
                                           1; % Special case: we don't want 0.
                                      true ->
                                           MinQuorum - 1
                                   end,
                        FailBricks = lists:sublist(shuffle(Bricks0), NumFails),
                        ok = ?SQ:set(Bricks0, K, V),
                        {ok, _, V} = ?SQ:get(Bricks0, K),
                        [ok = ?M:delete(Br, Nd, K) || {Br, Nd} <- FailBricks],
                        {ok, _, V} = ?SQ:get(Bricks0, K),
                        [ok = ?M:delete(Br, Nd, K) || {Br, Nd} <- FailBricks],
                        {ok, QSize, MinQuorum, NumFails} =
                            {?SQ:delete(Bricks0, K), QSize, MinQuorum, NumFails},
                        [key_not_exist = ?M:delete(Br, Nd, K) ||
                            {Br, Nd} <- FailBricks]
                end, KsVs)
      end, lists:seq(1, 9, 2) ++ [2]),

    %% Test repairs when single-brick deletions destroy > quorum copies.
    lists:foreach(
      fun(QSize) ->
              Bricks0 = lists:sublist(Bricks, QSize),
              lists:foreach(
                fun({K, V}) ->
                        MinQuorum = ((QSize - 1) div 2) + 1,
                        NumFails = if QSize == 1 ->
                                           1; % Need at least 1 of 1 failures
                                      QSize == 2 ->
                                           2; % Special case
                                      true ->
                                           MinQuorum
                                   end,
                        FailBricks = lists:sublist(shuffle(Bricks0), NumFails),
                        ok = ?SQ:set(Bricks0, K, V),
                        [ok = ?M:delete(Br, Nd, K) || {Br, Nd} <- FailBricks],
                        {key_not_exist, QSize, MinQuorum, NumFails} =
                            {?SQ:get(Bricks0, K), QSize, MinQuorum, NumFails},
                        %% Should not exist now on *any* of the bricks.
                        [{key_not_exist, MinQuorum, NumFails} =
                         {?M:get(Br, Nd, K), MinQuorum, NumFails} ||
                            {Br, Nd} <- Bricks0],
                        ok = ?SQ:set(Bricks0, K, V),
                        [ok = ?M:delete(Br, Nd, K) || {Br, Nd} <- FailBricks],
                        key_not_exist = ?SQ:delete(Bricks0, K)
                end, KsVs)
      end, lists:seq(1, 9, 2) ++ [2]),

    [ok = ?SQ:set(Bricks, K, V) || {K, V} <- KsVs],
%% io:format("XXX: Bricks = ~p\n", [Bricks]),
%% Me = garn,
%% register(Me, self()),
%% io:format("XXX: Waiting for 'go' at Me = ~w\n", [Me]), receive go -> ok end,
%% unregister(Me),
    {ok, [<<"1">>, <<"2">>], true} =
        ?SQ:get_keys(Bricks, ?BRICK__GET_MANY_FIRST, 2),
    {ok, [<<"3">>, <<"4">>, <<"5">>], true} =
        ?SQ:get_keys(Bricks, <<"2">>, 50),
    {ok, [], false} = ?SQ:get_keys(Bricks, <<"5">>, 50),

    %% Last thing: We have 9 bricks, so we'll have 1 brick failure, one
    %% key missing, one key out-of-date ... and with 3 total failures,
    %% everything will still work.

    [ok = ?SQ:set(Bricks, K, V) || {K, V} <- KsVs],
    [_, {FooB0, _FooN0}|_] = Bricks,
    ?M:stop(whereis(FooB0)),
    lists:foreach(
      fun({K, V}) ->
              {ok, CorrectTS, V} = ?SQ:get(Bricks, K),
              [_, _, {FooB1, FooN1}, {FooB2, FooN2}|_] = Bricks,
              ok = ?M:delete(FooB1, FooN1, K),
              ok = ?M:delete(FooB2, FooN2, K),
              Op = ?M:make_op6(set, K, 666, "bogus value", 0, []),
              [ok] = ?M:do(FooB2, FooN2, [Op]),
              {ok, CorrectTS, V} = ?SQ:get(Bricks, K)
      end, KsVs),

    [catch ?M:stop(whereis(Brick)) || {Brick, _} <- Bricks],

    io:format("Test chain_t35: end\n"),
    ok.

chain_t40(OptionList) ->
    %% Test the read-only feature that I just added to brick_server.erl
    io:format("Test chain_t40: start\n"),
    ?DBG_GENx({?MODULE,?LINE,top_of_test,OptionList}),

    ChainGrp = [{_, [{BrickName, Node}|_]}] = brick_hash:invent_nodelist(1, 3, node(), 40),
    ?DBG_GENx({?MODULE,?LINE,ChainGrp}),
    AllBricks = lists:append([BrickList || {_, BrickList} <- ChainGrp]),
    [rpc:call(NodeX, brick_shepherd, stop_brick, [Brick]) ||
        {Brick, NodeX} <- AllBricks],
    start_and_flush_common_log(),
    os:cmd("rm -rf hlog.*test_ch40_*"),
    Key1  = <<"key_t40">>,
    Val1 = <<"Whatever, man">>,
    Val2 = <<"Whatever, man #2 should be delayed and time out">>,
    Val3 = <<"Whatever, man #3 should be delayed a little bit">>,
    TimeOut = 1000,

    [brick_test0:chain_start_entire_chain(C, OptionList, true) || C <- ChainGrp],
    timer:sleep(1000),                          % Let everything settle
    ?DBG_GENx({?MODULE,?LINE,?M:chain_get_downstream_serials(BrickName, Node)}),

    ?DBG_GENx({?MODULE,?LINE}),
    ok = ?M:chain_set_read_only_mode(BrickName, Node, false),
    ?DBG_GENx({?MODULE,?LINE}),
    QQQres = (catch ?M:set(BrickName, Node, Key1, Val1, 30*1000)),
    ?DBG_GENx({?MODULE,?LINE,QQQres}),
    ok = QQQres,
    ?DBG_GENx({?MODULE,?LINE}),
    ok = ?M:chain_set_read_only_mode(BrickName, Node, true),
    ?DBG_GENx({?MODULE,?LINE}),
    {'EXIT', {timeout, _}} =
        (catch ?M:set(BrickName, Node, Key1, Val2, 500)),
    ?DBG_GENx({?MODULE,?LINE}),
    StartTime1 = now(),
    Parent = self(),
    MinTime = TimeOut * 1,
    MaxTime = TimeOut * 15,
    Child = spawn(fun() ->
    ?DBG_GENx({?MODULE,?LINE}),
                          Res = (catch ?M:set(BrickName, Node, Key1, Val3,
                                              MaxTime)),
                          DiffMS = timer:now_diff(now(), StartTime1) / 1000,
                          Parent ! {self(), Res, DiffMS}
                  end),
    %% If we sleep for MinTime, then turn of read-only mode, our child
    %% must have waited at least MinTime ... but not MaxTime!
    ?DBG_GENx({?MODULE,?LINE}),
    timer:sleep(MinTime),
    ?DBG_GENx({?MODULE,?LINE}),
    ok = ?M:chain_set_read_only_mode(BrickName, Node, false),
    ?DBG_GENx({?MODULE,?LINE}),
    receive
        {Child, ok, DiffMS1} when DiffMS1 >= MinTime, DiffMS1 =< MinTime*3 ->
            ok;
        {Child, ok, DiffMS} ->
            exit({bummer1, DiffMS});
        {Child, _, _} = Err ->
            exit({bummer1b, Err})
    after MaxTime ->
            exit(bummer2)
    end,

    ?DBG_GENx({?MODULE,?LINE,?M:chain_get_downstream_serials(BrickName, Node)}),
    %%DELME? timer:sleep(1000),
    ?DBG_GENx({?MODULE,?LINE,?M:chain_get_downstream_serials(BrickName, Node)}),
    key_not_exist = ?M:get(BrickName, Node, asdflkjasdfkljsdafkjasdfjklasdfjklasfd),
    ?DBG_GENx({?MODULE,?LINE,?M:chain_get_downstream_serials(BrickName, Node)}),
    timer:sleep(1000),
    ?DBG_GENx({?MODULE,?LINE,?M:chain_get_downstream_serials(BrickName, Node)}),

%% ?DBG(gggggggggggggggggggggggggggggggggggggggg),
%%     [spawn(fun() -> ?M:set(BrickName, Node, X, X) end) ||
%%      X <- lists:seq(1,250)],
%%      timer:sleep(3000),
%% ?DBG(?M:chain_get_downstream_serials(BrickName, Node)),
%% timer:sleep(1000),
%% ?DBG(?M:chain_get_downstream_serials(BrickName, Node)),
%% ?DBG(?M:status(BrickName, Node)),

    AllBricks = lists:append([BrickList || {_, BrickList} <- ChainGrp]),
    [rpc:call(NodeX, brick_shepherd, stop_brick, [Brick]) ||
        {Brick, NodeX} <- AllBricks],
    io:format("Test chain_t40: end\n"),
    ?DBG_GENx({?MODULE,?LINE,bottom_of_test,OptionList}),
    ok.

chain_t41(OptionList) ->
    io:format("Test chain_t41: start\n"),

    K_noexist = <<"SADFLKJSADFKLJsadfkljasdflkjasdf">>,
    K1 = <<"key1">>,
    V1 = <<"key1 value">>,
    K2 = <<"key2">>,
    V2 = <<"key2 value">>,
    K3 = <<"key3">>,
    V3 = <<"key3 value">>,

    ChainGrp = brick_hash:invent_nodelist(1, 4, node(), 98),
    ok = brick_test0:chain_start_entire_chain(hd(ChainGrp), OptionList, true),
    [{_ChName, Bs}] = ChainGrp,
    [{B1,N1}, {_B2,_N2}, {B3,N3}, {_B4,_N4}] = Bs,

    %% Set up: delete the keys, kill a middle server, set keys async,
    %% repair, then see what happens.
    [brick_server:delete(B1, N1, K) || K <- [K_noexist, K1, K2, K3]],
    _Syncs1 = sync_poll(B1, N1, 100),
    %%io:format("QQQ: Syncs1 = ~w\n", [_Syncs1]),
    %%
    brick_server:stop({B3, N3}),
    Parent = self(),
    _Pids = [spawn(fun() -> brick_server:set(B1, N1, K, V),
                            Parent ! {done, self()}
                   end) || {K, V} <- [{K1, V1}, {K2, V2}]],
    timer:sleep(250),                           % Ugly but helpful?
    DegradedBs_1 = Bs -- [{B3, N3}],
    brick_chainmon:set_all_chain_roles(DegradedBs_1),
    _Syncs2 = sync_poll(B1, N1, 100),
    %%io:format("QQQ: Syncs2 = ~w\n", [_Syncs2]),

    ok = brick_server:set(B1, N1, K3, V3),

    _Syncs3 = sync_poll(B1, N1, 100),
    %%io:format("QQQ: Syncs3 = ~w\n", [_Syncs3]),
    %% Make certain that all 3 keys are present with correct values.
    [[{ok, _TS, V}] = brick_server:do(Br, Nd, [brick_server:make_get(K)], [ignore_role], 1000) ||
        {Br, Nd} <- DegradedBs_1, {K, V} <- [{K1, V1}, {K2, V2}, {K3, V3}]],

    [catch brick_server:stop(B) || B <- Bs],
    io:format("Test chain_t41: end\n"),
    ok.

chain_t50(OptionList) ->
    io:format("Test chain_t50: start\n"),

    %% Two variations of the same test: migration where all chains
    %% are length 1.
    %% All keys are initially stored in Chain_1_1.
    %% The migration will move all keys from Chain_1_1 to Chain_3_1 members.
    %% The migration is interrupted in the middle, either by killing:
    %%    a. The only brick member in Chain_1_1
    %%    b. A brick member of Chain_3_1.
    %% Because all keys start in Chain_1_1, scenario a) kills the only
    %% chain that's sending migration updates.
    %% Scenario b) kills a receiver of those updates.

    ok = chain_t50_common(kill_in_chain_3, false, OptionList),
    ok = chain_t50_common(kill_in_chain_1, false, OptionList),

    io:format("Test chain_t50: end\n"),
    ok.

chain_t50_common(WhoToKill, ExitAfterKillP, OptionList) ->
    case proplists:get_value(do_logging, OptionList) of
        true ->
            chain_t50_common2(WhoToKill, ExitAfterKillP, OptionList);
        A when A == false; A == undefined ->
            io:format("Skipping chain_t50_common: logging not enabled\n"),
            ok
    end.

chain_t50_common2(WhoToKill, ExitAfterKillP, OptionList) ->
    %% You wouldn't really want to name chains this way, with weird
    %% chain numberings, 910 vs. 920, and with a single "test_" prefix
    %% for use by a single table not called "test": too confusing!
    %%
    %% However, for testing purposes, "single table" semantics aren't
    %% really that important.  Testing failure scenarios *is*.

    Chains_1_1 = brick_hash:invent_nodelist(1, 1, node(), 910),
    Chains_3_1 = brick_hash:invent_nodelist(3, 1, node(), 920),
    [catch brick_test0:chain_stop_entire_chain(OneChain) || OneChain <- Chains_1_1],
    [catch brick_test0:chain_stop_entire_chain(OneChain) || OneChain <- Chains_3_1],
    %% Kludge!
    start_and_flush_common_log(),
    os:cmd("rm *test_ch9??_stand*LOG"),
    os:cmd("rm -rf hlog.*test_ch9??_stand"),
    Fstart_chains =
        fun() ->
                [catch brick_test0:chain_start_entire_chain(C, OptionList, true) ||
                    C <- Chains_1_1],
                [catch brick_test0:chain_start_entire_chain(C, OptionList, true) ||
                    C <- Chains_3_1]
        end,
    Fstart_chains(),
    io:format("All chains started\n"),
    timer:sleep(1*1000),

    LH_v1 = brick_hash:naive_init(Chains_1_1),
    AllBricks1 = [Br || {_, Brs} <- Chains_1_1, Br <- Brs],
    GH_v1 = brick_hash:init_global_hash_state(false, unused, 1,
                                              LH_v1, Chains_1_1,
                                              LH_v1, Chains_1_1),

    Fset_global_hash =
        fun(TheGH, Bricks) ->
                [brick_server:chain_hack_set_global_hash(Br, Nd, TheGH) ||
                    {Br, Nd} <- Bricks]
        end,
    Fset_global_hash(GH_v1, AllBricks1),
    brick_simple:unset_gh(node(), testtab),
    brick_simple:set_gh(node(), testtab, GH_v1),

    NumKeys = 100,
    [begin
         Sfx = integer_to_list(N),
         ok = brick_simple:set(testtab, gmt_util:bin_ify("key " ++ Sfx),
                               gmt_util:bin_ify("val " ++ Sfx))
     end || N <- lists:seq(1, NumKeys)],
    io:format("Put keys finished\n"),

    LH_v2 = brick_hash:naive_init(Chains_3_1),
    GH_v2 = brick_hash:init_global_hash_state(true, unused, 1,
                                              LH_v1, Chains_1_1,
                                              LH_v2, Chains_3_1),

    AllBricks3 = [Br || {_, Brs} <- Chains_3_1, Br <- Brs],
    AllBricks = AllBricks1 ++ AllBricks3,

    [brick_server:chain_hack_set_global_hash(Br, node(), GH_v2) ||
        {_, Brs} <- Chains_1_1 ++ Chains_3_1, Br <- Brs],
    brick_simple:unset_gh(node(), testtab),
    brick_simple:set_gh(node(), testtab, GH_v2),

    PropDelay = 30,
    lists:map(
      fun({ChainName, Brs}) ->
              [brick_server:migration_start_sweep(
                 Br, node(), foocookie, ChainName,
                 [{max_keys_per_iter, 5}, {propagation_delay, PropDelay}]) ||
                  Br <- Brs]
      end, Chains_1_1 ++ Chains_3_1),

    io:format("Migrations started\n"),
    %% We'll definitely still be in the middle of migration
    %% when this sleep is done.
    timer:sleep(PropDelay * 9),

    %% exit(ccc),

    {BrickNameToKill, ChainNameToKill} =
        if WhoToKill == kill_in_chain_3 ->
                {test_ch920_stand, test_ch920};
           WhoToKill == kill_in_chain_1 ->
                {test_ch910_stand, test_ch910}
        end,

    exit(whereis(BrickNameToKill), kill),
    timer:sleep(100),
    io:format("Brick killed 100 msec ago, be patient...\n"),
    if ExitAfterKillP ->
            exit(exit_after_kill);
       true ->
            ok
    end,
    timer:sleep(6*1000),

    %% Restart chains via brute-force.
    Fstart_chains(),

    %% Set GH via brute-force.
    Fset_global_hash(GH_v2, AllBricks),
    %% Restart migration (same cookie!) via hardcoded kludge.
    ok = brick_server:migration_start_sweep(
           BrickNameToKill, node(), foocookie, ChainNameToKill,
           [{max_keys_per_iter, 5}, {propagation_delay, 50}]),
    X = brick_server:migration_start_sweep(
           BrickNameToKill, node(), bad_bad_cookie, ChainNameToKill,
           [{max_keys_per_iter, 5}, {propagation_delay, 50}]),
    true = not (X == ok),
    io:format("\n\nQQQQQQQ ~p: X = ~p\n\n\n", [BrickNameToKill, X]),

    Fpoll = fun(Bricks) ->
                    NS = lists:foldl(
                           fun({BrName, BrNode}, Num) ->
                                   {ok, Ps} = brick_server:status(BrName,
                                                                  BrNode),
                                   Ss = proplists:get_value(sweep, Ps),
                                   case proplists:get_value(status, Ss) of
                                       done -> Num + 0;
                                       _    -> Num + 1
                                   end
                           end, 0, Bricks),
                    if NS == 0 ->
                            {false, Bricks};
                       true ->
                            timer:sleep(100),
                            {true, Bricks}
                    end
            end,
    gmt_loop:do_while(Fpoll, AllBricks),
    [{ok, _, _} = brick_server:migration_clear_sweep(Br, Nd) || {Br, Nd} <- AllBricks],
    Fitems = fun({Br, Nd}, Acc) ->
                     {ok, Ps} = brick_server:status(Br, Nd),
                     Is = proplists:get_value(implementation, Ps),
                     Es = proplists:get_value(ets, Is),
                     Size = proplists:get_value(size, Es),
                     Acc + Size
             end,
    QQQ1 = lists:foldl(Fitems, 0, AllBricks1),
    QQQ3 = lists:foldl(Fitems, 0, AllBricks3),
io:format("QQQ1 ~p QQQ3 ~p\n", [QQQ1, QQQ3]),
    0 = lists:foldl(Fitems, 0, AllBricks1),
    NumKeys = lists:foldl(Fitems, 0, AllBricks3),
    io:format("Yes, all keys moved from old to new bricks\n"),

    io:format("Stopping all chains\n"),
    [brick_test0:chain_stop_entire_chain(Ch) || Ch <- Chains_1_1++Chains_3_1],

    ok.

chain_t51(OptionList) ->
    io:format("Test chain_t51: start\n"),

    %% Cut-and-paste-then-edit kludge variation of chain_50(), but
    %% chains are length 3.
    %% The brick kills in the middle of each run will usually catch
    %% each migration sweep during phase 2 (phase 1 doesn't have
    %% an artificial delay), so testing an interruption during phase 1
    %% isn't done here.  Also, only a middle brick is killed!

    ok = chain_t51_common(kill_in_chain_3, false, OptionList),
    ok = chain_t51_common(kill_in_chain_1, false, OptionList),

    io:format("Test chain_t51: end\n"),
    ok.

chain_t51_common(WhoToKill, ExitAfterKillP, OptionList) ->
    %% You wouldn't really want to name chains this way, with weird
    %% chain numberings, 910 vs. 920, and with a single "test_" prefix
    %% for use by a single table not called "test": too confusing!
    %%
    %% However, for testing purposes, "single table" semantics aren't
    %% really that important.  Testing failure scenarios *is*.

    Chains_1_3 = brick_hash:invent_nodelist(1, 3, node(), 910),
    Chains_3_3 = brick_hash:invent_nodelist(3, 3, node(), 920),
    [catch brick_test0:chain_stop_entire_chain(OneChain) || OneChain <- Chains_1_3],
    [catch brick_test0:chain_stop_entire_chain(OneChain) || OneChain <- Chains_3_3],
    timer:sleep(200),
    [io:format("\n================================ Whereis ~p -> ~p\n", [X, whereis(X)]) || X <- [test_ch910_head, test_ch910_middle, test_ch910_tail, test_ch920_head]],
    %% Kludge!
    start_and_flush_common_log(),
    os:cmd("rm *test_ch9??_*LOG"),
    os:cmd("rm -rf hlog.*test_ch9??_*"),

    Fstart_chains =
        fun(C_one, C_two, FullStartP) ->
                start_and_flush_common_log(),
                [catch brick_test0:chain_start_entire_chain(C, OptionList, FullStartP) ||
                    C <- C_one],
                [catch brick_test0:chain_start_entire_chain(C, OptionList, FullStartP) ||
                    C <- C_two]
        end,
    Fstart_chains(Chains_1_3, Chains_3_3, true),
    io:format("All chains started\n"),
    timer:sleep(1*1000),
    io:format("\n\n\n\n================================All chains started\n\n\n\n"),

    LH_v1 = brick_hash:naive_init(Chains_1_3),
    AllBricks1 = [Br || {_, Brs} <- Chains_1_3, Br <- Brs],
    AllHeadBricks1 = [hd(Brs) || {_, Brs} <- Chains_1_3],
    GH_v1 = brick_hash:init_global_hash_state(false, unused, 1,
                                              LH_v1, Chains_1_3,
                                              LH_v1, Chains_1_3),

    Fset_global_hash =
        fun(TheGH, Bricks) ->
                [brick_server:chain_hack_set_global_hash(Br, Nd, TheGH) ||
                    {Br, Nd} <- Bricks]
        end,
    Fset_global_hash(GH_v1, AllBricks1),
    brick_simple:unset_gh(node(), testtab),
    brick_simple:set_gh(node(), testtab, GH_v1),

    NumKeys = 100,
    [begin
         Sfx = integer_to_list(N),
         ok = brick_simple:set(testtab, gmt_util:bin_ify("key " ++ Sfx),
                               gmt_util:bin_ify("val " ++ Sfx))
     end || N <- lists:seq(1, NumKeys)],
    io:format("Put keys finished\n"),

    LH_v2 = brick_hash:naive_init(Chains_3_3),
    GH_v2 = brick_hash:init_global_hash_state(true, unused, 1,
                                              LH_v1, Chains_1_3,
                                              LH_v2, Chains_3_3),

    AllBricks3 = [Br || {_, Brs} <- Chains_3_3, Br <- Brs],
    AllHeadBricks3 = [hd(Brs) || {_, Brs} <- Chains_3_3],
    AllBricks = AllBricks1 ++ AllBricks3,
    AllHeadBricks = AllHeadBricks1 ++ AllHeadBricks3,

    [brick_server:chain_hack_set_global_hash(Br, node(), GH_v2) ||
        {_, Brs} <- Chains_1_3 ++ Chains_3_3, Br <- Brs],
    brick_simple:unset_gh(node(), testtab),
    brick_simple:set_gh(node(), testtab, GH_v2),

    PropDelay = 30,
    lists:map(
      fun({ChainName, Brs}) ->
              [brick_server:migration_start_sweep(
                 Br, node(), foocookie, ChainName,
                 [{max_keys_per_iter, 5}, {propagation_delay, PropDelay}]) ||
                  Br <- Brs]
      end, Chains_1_3 ++ Chains_3_3),

    io:format("Migrations started\n"),
    %% We'll definitely still be in the middle of migration
    %% when this sleep is done.
    timer:sleep(PropDelay * 20),

    %% exit(ccc),

    %% Kill a middle brick.
    {BrickNameToKill, ChainNameToKill, KilledChainHead} =
        if WhoToKill == kill_in_chain_3 ->
                {test_ch920_middle1, test_ch920, test_ch920_head};
           WhoToKill == kill_in_chain_1 ->
                {test_ch910_middle1, test_ch910, test_ch910_head}
        end,

    exit(whereis(BrickNameToKill), kill),
    timer:sleep(100),
    io:format("Brick killed 100 msec ago, be patient...\n"),
    if ExitAfterKillP ->
            exit(exit_after_kill);
       true ->
            ok
    end,
    timer:sleep(6*1000),

    Ffilt = fun({Ch, Nds}) -> {Ch, Nds -- [{BrickNameToKill, node()}]} end,
    Chains_1_3b = lists:map(Ffilt, Chains_1_3),
    Chains_3_3b = lists:map(Ffilt, Chains_3_3),
    LH_v1b = brick_hash:naive_init(Chains_1_3b),
    LH_v2b = brick_hash:naive_init(Chains_3_3b),
    GH_v2b = brick_hash:init_global_hash_state(true, unused, 1,
                                               LH_v1b, Chains_1_3b,
                                               LH_v2b, Chains_3_3b),
    AllBricks_b = AllBricks -- [{BrickNameToKill, node()}],

    %% Restart chains via brute-force.
    Fstart_chains(Chains_1_3b, Chains_3_3b, false),

    %% Set GH via brute-force.
    Fset_global_hash(GH_v2b, AllBricks_b),
    %% Restart migration (same cookie!) via hardcoded kludge.
    _ = brick_server:migration_start_sweep(
           KilledChainHead, node(), foocookie, ChainNameToKill,
           [{max_keys_per_iter, 5}, {propagation_delay, 50}]),

    poll_migration_finished(AllHeadBricks),
    [{ok, _, _} = brick_server:migration_clear_sweep(Br, Nd) ||
        {Br, Nd} <- AllHeadBricks],
    timer:sleep(1000),

    Fitems = fun({Br, Nd}, Acc) ->
                     {ok, Ps} = brick_server:status(Br, Nd),
                     Is = proplists:get_value(implementation, Ps),
                     Es = proplists:get_value(ets, Is),
                     Size = proplists:get_value(size, Es),
                     Acc + Size
             end,
    0 = lists:foldl(Fitems, 0, AllHeadBricks1),
    NumKeys = lists:foldl(Fitems, 0, AllHeadBricks3),
    io:format("Yes, all keys moved from old to new bricks\n"),

    io:format("Stopping all chains\n"),
    [brick_test0:chain_stop_entire_chain(Ch) || Ch <- Chains_1_3++Chains_3_3],

    ok.

chain_t54(_OptionList) ->
    io:format("Test chain_t54: start\n"),

    Bad =
   [
    %% len=0
    [],
    %% len=1
    %% bad role
    [#opconf_r{me = a, role = chain_member, off_tail_p = true}],
    %% bad off_tail
    [#opconf_r{me = a, role = standalone, off_tail_p = false}],
    %% NOTE: off_tail is false by default
    %% bad down
    [#opconf_r{me = a, role = standalone, downstream = b, off_tail_p = true}],
    %% bad up
    [#opconf_r{me = a, role = standalone, upstream = b, off_tail_p = true}],
    %% len=2
    %% bad role
    [#opconf_r{me = a, downstream = b, role = standalone},
     #opconf_r{me = b, upstream = a, role = chain_member, off_tail_p = true}],
    %% no official tail
    [#opconf_r{me = a, downstream = b, role = chain_member},
     #opconf_r{me = b, upstream = a, role = chain_member}],
    %% zero official tails
    [#opconf_r{me = a, downstream = b, role = chain_member},
     #opconf_r{me = b, upstream = a, role = chain_member}],
    %% two official tails
    [#opconf_r{me = a, downstream = b, role = chain_member, off_tail_p = true},
     #opconf_r{me = b, upstream = a, role = chain_member, off_tail_p = true}],
    %% a up+down
    [#opconf_r{me = a, upstream = b, downstream = b, role = chain_member},
     #opconf_r{me = b, upstream = a, role = chain_member, off_tail_p = true}],
    %% b down=self
    [#opconf_r{me = a, downstream = b, role = chain_member},
     #opconf_r{me = b, upstream = b, role = chain_member, off_tail_p = true}],
    %% b down=c
    [#opconf_r{me = a, downstream = b, role = chain_member},
     #opconf_r{me = b, upstream = c, role = chain_member, off_tail_p = true}],
    %% len=3
    %% cycle
    [#opconf_r{me = a, downstream = b, role = chain_member},
     #opconf_r{me = b, upstream = a, downstream = c, role = chain_member, off_tail_p = true},
     #opconf_r{me = c, downstream = a, role = chain_member}],
    %% c bad up
    [#opconf_r{me = a, downstream = b, role = chain_member},
     #opconf_r{me = b, upstream = a, downstream = c, role = chain_member, off_tail_p = true},
     #opconf_r{me = c, upstream = d, role = chain_member}],
    %% b bad down
    [#opconf_r{me = a, downstream = c, role = chain_member},
     #opconf_r{me = b, upstream = a, downstream = c, role = chain_member},
     #opconf_r{me = c, upstream = a, role = chain_member, off_tail_p = true}],
    %% down chain /= up chain
    [#opconf_r{me = a, downstream = b, role = chain_member},
     #opconf_r{me = b, upstream = a, downstream = c, role = chain_member},
     #opconf_r{me = c, upstream = a, role = chain_member, off_tail_p = true}],
    %% len=4
    %% down chain a,b,c,d /= rev(up) chain a,c,d,b
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b, downstream = c, upstream = c, role = chain_member},
     #opconf_r{me = c, downstream = d, upstream = a, role = chain_member},
     #opconf_r{me = d,                 upstream = b, role = chain_member, off_tail_p = true}],
    %% off-tail wrong pos
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b, downstream = c, upstream = a, role = chain_member, off_tail_p = true},
     #opconf_r{me = c, downstream = d, upstream = b, role = chain_member},
     #opconf_r{me = d,                 upstream = c, role = chain_member}],
    %% off-tail duplicate
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b, downstream = c, upstream = a, role = chain_member},
     #opconf_r{me = c, downstream = d, upstream = b, role = chain_member, off_tail_p = true},
     #opconf_r{me = d,                 upstream = c, role = chain_member, off_tail_p = true}],
    %% Adding repairing brick interrupted in middle.
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b, downstream = c, upstream = a, role = chain_member},
     #opconf_r{me = c,                 upstream = b, role = chain_member, off_tail_p = true},
     #opconf_r{me = d,                 upstream = c, role = chain_member}],
    %% Disjoint chains #1
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b,                 upstream = a, role = chain_member, off_tail_p = true},
     #opconf_r{me = c, downstream = d,                role = chain_member},
     #opconf_r{me = d,                 upstream = c, role = chain_member, off_tail_p = true}],
    %% Disjoint chains #2
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b, downstream = c, upstream = a, role = chain_member},
     #opconf_r{me = c,                 upstream = b, role = chain_member, off_tail_p = true},
     #opconf_r{me = z, role = standalone, off_tail_p = true}],

    %% keep me (no trailing comma)
    []
   ],

    Good =
   [
    [#opconf_r{me = z, role = standalone, off_tail_p = true}],
    [#opconf_r{me = a, downstream = b, role = chain_member},
     #opconf_r{me = b, upstream = a, role = chain_member, off_tail_p = true}],
    [#opconf_r{me = a, downstream = b, role = chain_member, off_tail_p = true},
     #opconf_r{me = b, upstream = a, role = chain_member}],
    %% len=3
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b, downstream = c, upstream = a, role = chain_member},
     #opconf_r{me = c,                 upstream = b, role = chain_member, off_tail_p = true}],
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b, downstream = c, upstream = a, role = chain_member, off_tail_p = true},
     #opconf_r{me = c,                 upstream = b, role = chain_member}],
    %% len=4
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b, downstream = c, upstream = a, role = chain_member},
     #opconf_r{me = c, downstream = d, upstream = b, role = chain_member},
     #opconf_r{me = d,                 upstream = c, role = chain_member, off_tail_p = true}],
    [#opconf_r{me = a, downstream = b,               role = chain_member},
     #opconf_r{me = b, downstream = c, upstream = a, role = chain_member},
     #opconf_r{me = c, downstream = d, upstream = b, role = chain_member, off_tail_p = true},
     #opconf_r{me = d,                 upstream = c, role = chain_member}],

    %% keep me (no trailing comma)
    [#opconf_r{me = z, role = standalone, off_tail_p = true}]
   ],

    Ftest = fun(TestCase) ->
                    case (catch brick_chainmon:stitch_op_state(TestCase)) of
                        {L, undefined} when is_list(L), length(L) > 0 ->
                            ok;
                        %% For real usage, the repairing brick would be
                        %% {atom(), atom()} instead of simply atom()
                        {L, A} when is_list(L), length(L) > 0, is_atom(A) ->
                            ok;
                        _X ->
                            %%io:format("~p -> ~p\n", [TestCase, _X]),
                            error
                    end
            end,
    %% Run each test multiple times, due to randomness.
    %% Randomness adds non-determinism to test, but that
    %% only makes it tougher to figure out who failed.
    %% It doesn't affect pass/fail result.
    [error = Ftest(shuffle(T)) || T <- Bad,  _ <- lists:seq(1,100)],
    [ok    = Ftest(shuffle(T)) || T <- Good, _ <- lists:seq(1,100)],

    io:format("Test chain_t54: end\n"),
    ok.

chain_t55() ->
    Rs = [{brick_test0:chain_t55(Os), Os} || Os <- brick_test0:make_brick_option_lists()],
    case lists:any(fun({ok, _}) -> false;
                      (_)       -> true
                   end, Rs) of
        false -> ok;
        true  -> {error, Rs}
    end.

chain_t55(OptionList) ->
    io:format("Test chain_t55: start with ~w\n", [OptionList]),

    %% Test chain lengthening and shortening code.
    %% Doing this properly really requires the help of the admin server.
    %% And testing the admin server isn't something I've had much practice
    %% with before ... so this is going to be brute-force code.
    MyBase = "temp_chain_t55__",
    SchemaFile = MyBase ++ "schema.txt",
    Tab = list_to_atom(MyBase),
    Chain1 = list_to_atom(MyBase ++ "_ch1"),
    Ch1B1 = list_to_atom(MyBase ++ "_ch1_b1"),
    Ch1B2 = list_to_atom(MyBase ++ "_ch1_b2"),
    Ch1B3 = list_to_atom(MyBase ++ "_ch1_b3"),
    Ch1B1ets = list_to_atom(MyBase ++ "_ch1_b1_store"),
    Ch1B2ets = list_to_atom(MyBase ++ "_ch1_b2_store"),
    Ch1B3ets = list_to_atom(MyBase ++ "_ch1_b3_store"),

    catch ok = reliable_gdss_stop(),
    timer:sleep(200),
    os:cmd("rm -rf hlog.commonLogServer"),
    os:cmd("rm *" ++ MyBase ++ "*"),
    os:cmd("rm -rf hlog.*" ++ MyBase ++ "*"),
    ok = application:start(gdss),
    timer:sleep(1000),
    brick_admin:create_new_schema([{list_to_atom("bootstrap"++MyBase),node()}],
                                  SchemaFile),
    brick_admin:start(SchemaFile),
    timer:sleep(250),
    brick_admin:hack_all_tab_setup(Tab, 1, [node()], OptionList, true),
    timer:sleep(5*1000),                    % Timing-sensitive, sorry.

    io:format("Putting initial keys... "),
    ok = put_rand_keys_via_simple(Tab, 100, 100, "", "key-suffix", 50, 0, false),
    io:format("done\n"),

    io:format("Adding brick ~w\n", [Ch1B2]),
    ok = brick_admin:change_chain_length(
           Chain1, [{Ch1B1, node()}, {Ch1B2, node()}]),
    _X1 = repair_poll(Ch1B1, node(), 250),
    io:format("repair polls _X1 = ~p\n", [_X1]),
    io:format("Adding brick ~w has finished\n", [Ch1B2]),

    gmt_loop:do_while(
      fun(_) ->
              %io:format("ZZZ DBG1: Ch1B1 = ~p\n", [ets:tab2list(Ch1B1ets)]),
              %io:format("ZZZ DBG1: Ch1B2 = ~p\n", [ets:tab2list(Ch1B2ets)]),
              Ch1_List = xform_ets_dump(Ch1B1, ets:tab2list(Ch1B1ets)),
              Ch2_List = xform_ets_dump(Ch1B2, ets:tab2list(Ch1B2ets)),
              if Ch1_List /= Ch2_List ->
                      io:format("ZZZ: What's up?  ~p /= ~p\n", [Ch1_List, Ch2_List]),
                      timer:sleep(100),
                      {true, x};
                 true ->
                      {false, x}
              end
      end, x),

    true = (xform_ets_dump(Ch1B1, ets:tab2list(Ch1B1ets)) ==
            xform_ets_dump(Ch1B2, ets:tab2list(Ch1B2ets))),
    io:format("ETS tables are identical\n"),

    %% Ok, time to grow the chain to 3, while updates are happening.

    Pid1 = spawn_link(
             fun () ->
                     put_rand_keys_via_simple(Tab, 100, 999, "", "", 50, 60, true)
             end),

    io:format("Adding brick ~w\n", [Ch1B3]),
    ok = brick_admin:change_chain_length(
           Chain1, [{Ch1B1, node()}, {Ch1B2, node()}, {Ch1B3, node()}]),
    timer:sleep(250),
    _X2 = my_repair_poll(Ch1B3, node(), 250),
    io:format("my repair polls _X2 = ~p\n", [_X2]),
    io:format("Adding brick ~w has finished\n", [Ch1B3]),

    unlink(Pid1),
    exit(Pid1, kill),
    _X3 = sync_poll(Ch1B1, node(), 100),
    io:format("sync polls _X3 = ~p\n", [_X3]),
    %% There's a really intermittent bug here, let's try allowing some
    %% extra flush time.  Analysis of the first dump files created
    %% below says that of the 88 keys in the Ch1B1_list, only 3
    %% differed (and only by timestamp).
    timer:sleep(1100),
    Ch1B1_list = ets:tab2list(Ch1B1ets),
    Ch1B2_list = ets:tab2list(Ch1B2ets),
    Ch1B3_list = ets:tab2list(Ch1B3ets),
    %io:format("ZZZ DBG2: Ch1B1 = ~p\n", [ets:tab2list(Ch1B1ets)]),
    %io:format("ZZZ DBG2: Ch1B2 = ~p\n", [ets:tab2list(Ch1B2ets)]),
    %io:format("ZZZ DBG2: Ch1B3 = ~p\n", [ets:tab2list(Ch1B3ets)]),
    Ch1B1_dump = xform_ets_dump(Ch1B1, Ch1B1_list),
    Ch1B2_dump = xform_ets_dump(Ch1B2, Ch1B2_list),
    Ch1B3_dump = xform_ets_dump(Ch1B3, Ch1B3_list),
    if Ch1B1_dump == Ch1B2_dump ->
            ok;
       true ->
            file:write_file("zzz.ch1b1.list.out", term_to_binary(Ch1B1_list)),
            file:write_file("zzz.ch1b2.list.out", term_to_binary(Ch1B2_list)),
            file:write_file("zzz.ch1b1.dump.out", term_to_binary(Ch1B1_dump)),
            file:write_file("zzz.ch1b2.dump.out", term_to_binary(Ch1B2_dump)),
            io:format("ERROR: Ch1B1Dump /= Ch1B2Dump, see zzz.*.out files\n"),
            exit({b1_vs_b2_not_equal, OptionList})
    end,
    if Ch1B1_dump == Ch1B3_dump ->
            ok;
       true ->
            file:write_file("zzz.ch1b1.list.out", term_to_binary(Ch1B1_list)),
            file:write_file("zzz.ch1b3.list.out", term_to_binary(Ch1B3_list)),
            file:write_file("zzz.ch1b1.dump.out", term_to_binary(Ch1B1_dump)),
            file:write_file("zzz.ch1b3.dump.out", term_to_binary(Ch1B3_dump)),
            io:format("ERROR: Ch1B1Dump /= Ch1B3Dump, see zzz.*.out files\n"),
            exit({b1_vs_b3_not_equal, OptionList})
    end,
    io:format("ETS tables are identical\n"),

    %% Ok, time to shrink the chain to 2, while updates are happening.

    Pid2 = spawn_link(
             fun () ->
                     put_rand_keys_via_simple(Tab, 100, 999, "", "", 50, 60, true)
             end),

    io:format("Removing brick ~w\n", [Ch1B3]),
    ok = brick_admin:change_chain_length(
           Chain1, [{Ch1B1, node()}, {Ch1B2, node()}]),
    io:format("Removing brick ~w has finished\n", [Ch1B3]),

    unlink(Pid2),
    exit(Pid2, kill),
    io:format("\n\nPid2 ~w is dead\n\n", [Pid2]), timer:sleep(5000),
    _X5 = sync_poll(Ch1B1, node(), 100),
    io:format("sync polls _X5 = ~p\n", [_X5]),
    %io:format("ZZZ DBG3: Ch1B1 = ~p\n", [ets:tab2list(Ch1B1ets)]),
    %io:format("ZZZ DBG3: Ch1B2 = ~p\n", [ets:tab2list(Ch1B2ets)]),
    true = (xform_ets_dump(Ch1B1, ets:tab2list(Ch1B1ets)) ==
            xform_ets_dump(Ch1B2, ets:tab2list(Ch1B2ets))),
    io:format("ETS tables are identical\n"),

    %% Ok, time to shrink the chain to 1 (no updates)

    io:format("Removing brick ~w\n", [Ch1B2]),
    ok = brick_admin:change_chain_length(
           Chain1, [{Ch1B1, node()}]),
    io:format("Removing brick ~w has finished\n", [Ch1B2]),

    io:format("Test chain_t55: stopping brick application\n"),
    catch ok = reliable_gdss_stop(),
    application:start(gdss),            % start again for following tests.
    timer:sleep(100),                           % allow info reports to appear
    io:format("Test chain_t55: end\n"),
    ok.

chain_t56(OptionList) ->
    io:format("Test chain_t56: start: ~p\n", [OptionList]),

    io:format("\n"),
    io:format("We're testing a difficult race condition here, chain_t56.\n"
              "I don't know how well it is going to stand the test of time.\n"
              "It is going to try to cause a second brick failure while a\n"
              "chain repair is in the middle.  The problem is getting the\n"
              "timing correct.\n"
              "\n"
              "I recommend using chain_t56([test]) every now and then to\n"
              "verify that something hasn't drastically broken.\n\n"),
    timer:sleep(3000),

    MyBase = "temp_chain_t56",
    SchemaFile = MyBase ++ "schema.txt",
    Tab = list_to_atom(MyBase),
    Ch1B1 = list_to_atom(MyBase ++ "_ch1_b1"),
    Ch1B2 = list_to_atom(MyBase ++ "_ch1_b2"),
    F_stopstart = fun() ->
                          catch flush_common_log(),
                          error_logger:info_msg("Test stopping gdss app\n"),
                          StopRes = (catch ok = reliable_gdss_stop()),
                          error_logger:info_msg("Test stopping gdss res: ~p\n",
                                                 [StopRes]),
                          os:cmd("rm *" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.*" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.commonLogServer"),
                          error_logger:info_msg("Test starting gdss app\n"),
                          ok = application:start(gdss)
                  end,
    F_stopstart(),
    {ok, _} = brick_admin:create_new_schema(
                [{list_to_atom("bootstrap"++MyBase),node()}], SchemaFile),
    ok = brick_admin:start(SchemaFile),
    true = poll_admin_server_registered(),
    brick_admin:hack_all_tab_setup(Tab, 2, [node(), node()], OptionList, true),
    timer:sleep(5*1000),                        % Timing-sensitive, sorry.

    io:format("Putting initial keys...\n"),
    [ok = brick_simple:set(Tab, "goo"++integer_to_list(X), "val!") ||
        X <- lists:seq(1, 20)],

    error_logger:info_msg(
      "Kill ~p, wait a little bit, then kill ~p while ~p is still\n"
      " under repair.\n", [Ch1B1, Ch1B2, Ch1B1]),
    file:write_file("debug_chain_props",
                    term_to_binary([{max_keys_per_iter, 1},
                                    {repair_ack_sleep, 500},
                                    {repair_round1_ack_sleep, 500}])),
    exit(whereis(Ch1B1), kill),
    timer:sleep(4*1000),
    exit(whereis(Ch1B2), kill),
    timer:sleep(14*1000),
    file:delete("debug_chain_props"),

    %% If BZ 24607 is a problem, and if our timing is as we believe it
    %% is, these pattern matches will fail.
    io:format("\n\nDEBUG: test56: before repair state 1 at ~p\n\n", [time()]),
    _ = my_repair_poll(Ch1B1, node(), 500),
    ok = brick_server:chain_get_my_repair_state(Ch1B1, node()),
    io:format("\n\nDEBUG: test56: before repair state 2 at ~p\n\n", [time()]),
    _ = my_repair_poll(Ch1B2, node(), 500),
    ok = brick_server:chain_get_my_repair_state(Ch1B2, node()),
    io:format("\n\nDEBUG: test56: after repair state 2\n\n"),
    error_logger:info_msg("Kill test passed\n"),

    case proplists:get_value(test, OptionList) of
        undefined ->
            F_stopstart(),
            timer:sleep(500);
        _ ->
            ok
    end,
    io:format("Test chain_t56: end\n"),
    ok.

%% Regression test for BZ 27591.  We're out here in the 100's because
%% this is a test that doesn't matter what kind of options the
%% individual bricks use.

chain_t100() ->
    OptionList = [],
    io:format("Test chain_t100: start: ~p\n", [OptionList]),

    ExtraOpts = [{repair_max_keys, 5}],
    OptionList2 = ExtraOpts ++ OptionList,
    MyBase = "temp_chain_t100",
    SchemaFile = MyBase ++ "schema.txt",
    Tab = list_to_atom(MyBase),
    Ch1B1 = list_to_atom(MyBase ++ "_ch1_b1"),
    Ch1B2 = list_to_atom(MyBase ++ "_ch1_b2"),
    F_stopstart = fun() ->
                          catch flush_common_log(),
                          error_logger:info_msg("Test stopping gdss app\n"),
                          StopRes = (catch ok = reliable_gdss_stop()),
                          error_logger:info_msg("Test stopping gdss res: ~p\n",
                                                 [StopRes]),
                          os:cmd("rm *" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.*" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.commonLogServer"),
                          error_logger:info_msg("Test starting gdss app\n"),
                          ok = application:start(gdss)
                  end,
    F_stopstart(),
    {ok, _} = brick_admin:create_new_schema(
                [{list_to_atom("bootstrap"++MyBase),node()}], SchemaFile),
    ok = brick_admin:start(SchemaFile),
    true = poll_admin_server_registered(),
    brick_admin:hack_all_tab_setup(Tab, 2, [node(), node()], OptionList2, true),
    timer:sleep(5*1000),                    % Timing-sensitive, sorry.

    io:format("Putting initial keys...\n"),
    [ok = brick_simple:set(Tab, "goo"++integer_to_list(X), "val!") ||
        X <- lists:seq(1, 200)],

    error_logger:info_msg(
      "Kill ~p, then wait a little bit, then suspend ~p, "
      "then see what happens\n", [Ch1B2, Ch1B2]),
    %% The 5 x 100 x 100 delay factors will have repair take roughly 2 secs.
    file:write_file("debug_chain_props",
                    term_to_binary([{repair_max_keys, 5},
                                    {repair_ack_sleep, 100},
                                    {repair_round1_ack_sleep, 100},
                                    {repair_finished_sleep, 35*1000}])),
    exit(whereis(Ch1B2), kill),
    error_logger:warning_msg("\n\nKill of ~p done\n\n\n\n", [Ch1B2]),
    timer:sleep(15*1000),
    %% Don't forget to delete this temp debugging file!
    %% Besides, we don't want the extra delay for the second round of
    %% repairs.
    file:delete("debug_chain_props"),
    timer:sleep(45*1000),
    error_logger:warning_msg("\n\nOK, done with sleep\n\n\n\n", []),
    timer:sleep(500),

    %% {sigh}  Hopefully I've got the timing of this check correct.
    Node = node(),
    ok = brick_server:chain_get_my_repair_state(Ch1B1, Node),
    ok = brick_server:chain_get_my_repair_state(Ch1B2, Node),
    {head, {Ch1B2, Node}} = brick_server:chain_get_role(Ch1B1, Node),
    {tail, {Ch1B1, Node}} = brick_server:chain_get_role(Ch1B2, Node),
    {ok, Ps1} = brick_server:status(Ch1B1, Node),
    Cs1 = proplists:get_value(chain, Ps1),
    false = proplists:get_value(chain_official_tail, Cs1),
    {ok, Ps2} = brick_server:status(Ch1B2, Node),
    Cs2 = proplists:get_value(chain, Ps2),
    true = proplists:get_value(chain_official_tail, Cs2),

    io:format("If this test fails, a manual check is required: check HTTP\n"
              "status page at http://localhost:23080/ to verify that\n"
              "temp_chain_t100_ch1 is in a healthy state and that both\n"
              "bricks have their proper roles.\n\n"),
    io:format("Test chain_t100: end\n"),
    ok.

%% Regression test for BZ 27632.

chain_t101() ->
    OptionList = [],
    io:format("Test chain_t101: start: ~p\n", [OptionList]),
    TabOptions = [{hash_init, fun brick_hash:chash_init/3},
                  {prefix_method,var_prefix},
                  {prefix_separator,47},
                  {num_separators,3},
                  {bigdata_dir,"cwd"},
                  {do_logging,true},
                  {do_sync,true},
                  {new_chainweights, [{tab101_ch1, 1}, {tab101_ch2, 1},
                                      {tab101_ch3, 1}, {tab101_ch4, 1}]}
                 ],
    MyBase = "tab101",
    SchemaFile = MyBase ++ "schema.txt",
    Tab = list_to_atom(MyBase),
    _Ch1B1 = list_to_atom(MyBase ++ "_ch1_b1"),
    _Ch1B2 = list_to_atom(MyBase ++ "_ch1_b2"),
    file:delete("debug_chain_props"),
    F_stopstart = fun() ->
                          catch flush_common_log(),
                          error_logger:info_msg("Test stopping gdss app\n"),
                          StopRes = (catch ok = reliable_gdss_stop()),
                          error_logger:info_msg("Test stopping gdss res: ~p\n",
                                                 [StopRes]),
                          os:cmd("rm *" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.*" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.commonLogServer"),
                          error_logger:info_msg("Test starting gdss app\n"),
                          ok = application:start(gdss)
                  end,
    F_stopstart(),
    {ok, _} = brick_admin:create_new_schema(
                [{list_to_atom("bootstrap"++MyBase),node()}], SchemaFile),
    ok = brick_admin:start(SchemaFile),
    true = poll_admin_server_registered(),
    ChDescr = brick_admin:make_chain_description(
                Tab, 4, lists:duplicate(4, node())),
    brick_admin:add_table(Tab, ChDescr, TabOptions ++ OptionList),
    timer:sleep(25*1000),
    error_logger:info_msg("\n\nOK, all table setup done\n\n\n"),

    %% More setup, to get the debug_chain_props into brick #3.
    exit(whereis(tab101_ch3_b3), kill),
    %% This is going to take a while.
    timer:sleep(25*1000),
    error_logger:info_msg("\n\nOK, done with sleep waiting for brick #3\n\n\n\n", []),
    timer:sleep(1*1000),

    [brick_server:set_do_sync({Br, node()}, false) || Br <- [tab101_ch3_b1, tab101_ch3_b2, tab101_ch3_b3, tab101_ch3_b4]],
    [brick_simple:set(tab101, "/zar/zar/"++integer_to_list(X), "foo!") || X <- lists:seq(1,100)],
    file:write_file("debug_chain_props",
                    term_to_binary([{repair_max_keys, 2},
                                    {repair_round1_ack_sleep, 250}])),

    %% OK, now start the real work.
    [brick_server:set_do_sync({Br, node()}, false) || Br <- [tab101_ch3_b1, tab101_ch3_b2, tab101_ch3_b3, tab101_ch3_b4]],
    exit(whereis(tab101_ch3_b4), kill),
    timer:sleep(100),
    [brick_simple:set(tab101, "/zar/zar/"++integer_to_list(X), "BAR!") ||
        X <- lists:seq(1,100)],
    timer:sleep(11*1000),
    error_logger:info_msg("AAAAAA\n"),
    [brick_simple:delete(tab101, "/zar/zar/"++integer_to_list(X)) ||
        X <- lists:seq(25,90)],

    error_logger:info_msg("\n\nOK, done with sleep\n\n\n\n", []),
    timer:sleep(11*1000),
    file:delete("debug_chain_props"),
    error_logger:info_msg("\n\nOK, done with sleep again, deleted debug file\n\n\n\n", []),

    Dump1 = xform_ets_dump(tab101_ch3_b1, ets:tab2list(tab101_ch3_b1_store)),
    Dump2 = xform_ets_dump(tab101_ch3_b2, ets:tab2list(tab101_ch3_b2_store)),
    Dump3 = xform_ets_dump(tab101_ch3_b3, ets:tab2list(tab101_ch3_b3_store)),
    Dump4 = xform_ets_dump(tab101_ch3_b4, ets:tab2list(tab101_ch3_b4_store)),
    Dump1 = Dump2,
    Dump1 = Dump3,
    Dump1 = Dump4,
    io:format("Test chain_t101: end\n"),
    ok.

%% Regression test for BZ 27830

chain_t102() ->
    OptionList = [],
    io:format("Test chain_t102: start: ~p\n", [OptionList]),
    TabOptions = [{hash_init, fun brick_hash:chash_init/3},
                  {prefix_method,var_prefix},
                  {prefix_separator,47},
                  {num_separators,3},
                  {bigdata_dir,"cwd"},
                  {do_logging,true},
                  {do_sync,true},
                  {new_chainweights, [{tab102_ch1, 1}, {tab102_ch2, 1},
                                      {tab102_ch3, 1}, {tab102_ch4, 1},
                                      {tab102_ch5, 1}, {tab102_ch6, 1}]}
                 ],
    MyBase = "tab102",
    SchemaFile = MyBase ++ "schema.txt",
    Tab = list_to_atom(MyBase),
    Ch1B1 = list_to_atom(MyBase ++ "_ch1_b1"),
    Ch1B2 = list_to_atom(MyBase ++ "_ch1_b2"),
    Ch1B3 = list_to_atom(MyBase ++ "_ch1_b3"),
    Node = node(),
    F_stopstart = fun() ->
                          catch flush_common_log(),
                          error_logger:info_msg("Test stopping gdss app\n"),
                          StopRes = (catch ok = reliable_gdss_stop()),
                          error_logger:info_msg("Test stopping gdss res: ~p\n",
                                                 [StopRes]),
                          os:cmd("rm *" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.*" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.commonLogServer"),
                          error_logger:info_msg("Test starting gdss app\n"),
                          ok = application:start(gdss)
                  end,
    F_stopstart(),
    {ok, _} = brick_admin:create_new_schema(
                [{list_to_atom("bootstrap"++MyBase),Node}], SchemaFile),
    ok = brick_admin:start(SchemaFile),
    true = poll_admin_server_registered(),
    ChDescr = brick_admin:make_chain_description(
                Tab, 3, lists:duplicate(6, Node)),
    brick_admin:add_table(Tab, ChDescr, TabOptions ++ OptionList),
    timer:sleep(15*1000),
    error_logger:info_msg("\n\nOK, all table setup done\n\n\n"),

    %% OK, now the final setup steps.
    ok = brick_shepherd:add_do_not_restart_brick(Ch1B1, Node),
    ok = brick_shepherd:add_do_not_restart_brick(Ch1B3, Node),
    exit(whereis(Ch1B1), kill),
    timer:sleep(5500),
    exit(whereis(Ch1B3), kill),
    timer:sleep(5500),

    standalone = brick_server:chain_get_role(Ch1B2, Node),
    {ok, Ps2} = brick_server:status(Ch1B2, Node),
    Cs2 = proplists:get_value(chain, Ps2),
    true = proplists:get_value(chain_official_tail, Cs2),

    io:format("Test chain_t102: end\n"),
    ok.

chain_t103() ->
    OptionList = [],
    io:format("Test chain_t103: start: ~p\n", [OptionList]),
    TabOptions = [{hash_init, fun brick_hash:chash_init/3},
                  {prefix_method,var_prefix},
                  {prefix_separator,47},
                  {num_separators,2},
                  {bigdata_dir,"cwd"},
                  {do_logging,true},
                  {do_sync,true}
                 ],
    TabWeights1 = [{new_chainweights, [{tab103_ch1, 1}]}],
    TabWeights2 = [{new_chainweights, [{tab103_ch1, 1}, {tab103_ch2, 1}]}],

    MyBase = "tab103",
    SchemaFile = MyBase ++ "schema.txt",
    Tab = list_to_atom(MyBase),
    Node = node(),
    F_stopstart = fun() ->
                          catch flush_common_log(),
                          error_logger:info_msg("Test stopping gdss app\n"),
                          StopRes = (catch ok = reliable_gdss_stop()),
                          error_logger:info_msg("Test stopping gdss res: ~p\n",
                                                 [StopRes]),
                          os:cmd("rm *" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.*" ++ MyBase ++ "*"),
                          os:cmd("rm -rf hlog.commonLogServer"),
                          error_logger:info_msg("Test starting gdss app\n"),
                          ok = application:start(gdss)
                  end,
    F_stopstart(),
    {ok, _} = brick_admin:create_new_schema(
                [{list_to_atom("bootstrap"++MyBase),Node}], SchemaFile),
    ok = brick_admin:start(SchemaFile),
    true = poll_admin_server_registered(),
    ChDescr1 = brick_admin:make_chain_description(
                 Tab, 1, lists:duplicate(1, Node)),
    ChDescr2 = brick_admin:make_chain_description(
                 Tab, 1, lists:duplicate(2, Node)),
    ok = brick_admin:add_table(Tab, ChDescr1,
                               TabOptions ++ TabWeights1 ++ OptionList),
    timer:sleep(8*1000),
    error_logger:info_msg("\n\nOK, all table setup done\n\n\n"),

    NumKeysPerY = 100,
    [brick_simple:set(Tab, "/x"++integer_to_list(X)++"/y"++integer_to_list(Y), "foo!") ||
        X <- lists:seq(0,2), Y <- lists:seq(1, NumKeysPerY)],
    LH2 = brick_hash:chash_init(via_proplist, ChDescr2,
                                TabOptions ++ TabWeights2 ++ OptionList),
    error_logger:warning_msg("Starting migration\n"),
    {ok, _MigTime} = brick_admin:start_migration(
                      Tab, LH2,
                      [{max_keys_per_iter, 20}, {propagation_delay, 500}]),
    [begin
         timer:sleep(250),
         NNN0 = slurp_all(Tab, <<"/x0/">>, 100),
         NNN = lists:sort(NNN0), % won't return keys in order, it's dumb.
         %error_logger:warning_msg("SLURP got ~p: ~p - ~p\n", [length(NNN0), element(1,hd(NNN)), element(1,lists:last(NNN))]),
         %% Here's the main sanity check: correct length?
         NumKeysPerY = length(NNN)
     end || _ <- lists:seq(1, 30)],
    timer:sleep(5*1000), % Need some time to allow migration cleanup.

    io:format("Test chain_t103: end\n"),
    ok.

chain_t60(OptionList) ->
    NameHead = regression_head,
    NameTail = regression_tail,
    chain_t60(NameHead, NameTail, true, true, true, true, OptionList).

chain_t60(NameHead, NameTail, StartupP, InsertP, EtsTestP, KillP, OptionList) ->
    io:format("Test chain_t60: enter\n"),

    %% NOTE: We're only testing chains of length = 2.
    Node = node(),
    NameHeadETS = list_to_atom(atom_to_list(NameHead) ++ "_store"),
    NameTailETS = list_to_atom(atom_to_list(NameTail) ++ "_store"),

    if
        StartupP == true ->
            catch exit(whereis(NameHead), byebye),
            FilesH = filelib:wildcard("*." ++ atom_to_list(NameHead) ++ ",*.LOG", "."),
            [file:delete(F) || F <- FilesH],
            start_and_flush_common_log(),
            os:cmd("rm -rf hlog.*" ++ atom_to_list(NameHead) ++ "*"),
            {ok, _HeadPid} = safe_start_link(NameHead, OptionList),

            catch exit(whereis(NameTail), byebye),
            FilesT = filelib:wildcard("*." ++ atom_to_list(NameTail) ++ ",*.LOG", "."),
            [file:delete(F) || F <- FilesT],
            start_and_flush_common_log(),
            os:cmd("rm -rf hlog.*" ++ atom_to_list(NameTail) ++ "*"),
            {ok, _TailPid} = safe_start_link(NameTail, OptionList);
        true ->
            ok
    end,

    Odds =
        [{<<"foo1">>, 1, <<"one">>},
         {<<"foo3">>, 3, <<"three">>},
         {<<"foo5">>, 5, <<"five">>},
         {<<"foo7">>, 7, <<"seven">>},
         {<<"foo9">>, 9, <<"nine">>}],
    Evens =
        [{<<"foo0">>, 0, <<"zero">>},
         {<<"foo2">>, 2, <<"two">>},
         {<<"foo4">>, 4, <<"four">>},
         {<<"foo6">>, 6, <<"six">>},
         {<<"foo8">>, 8, <<"eight">>}],

    if
        InsertP == true ->
            %% Put both sets into tail; add one to T for odds
            ?M:chain_role_standalone(NameTail, Node),
            ?M:chain_set_my_repair_state(NameTail, Node, ok),
            [[ok] = ?M:do(NameTail, Node, [?M:make_op6(set, K, T, V, 0, [])]) || {K, T, V} <- Evens],
            [[ok] = ?M:do(NameTail, Node, [?M:make_op6(set, K, T+1, V, 0, [])]) || {K, T, V} <- Odds],

            %% Put odds into the will-be-head.
            ?M:chain_role_standalone(NameHead, Node),
            ?M:chain_set_my_repair_state(NameHead, Node, ok),
            [[ok] = ?M:do(NameHead, Node, [?M:make_op6(set, K, T, V, 0, [])]) || {K, T, V} <- Odds];
        true ->
            ok
    end,

    if
        StartupP == true ->
            %% Set the will-be-tail => tail
            ?M:chain_set_my_repair_state(NameTail, Node, testing_harness_force_pre_init),
            ?M:chain_role_tail(NameTail, Node, NameHead, Node, []),
            pre_init = ?M:chain_get_my_repair_state(NameTail, Node),

            %% Set the will-be-head => head
            ?M:chain_role_head(NameHead, Node, NameTail, Node, []),
            ok = ?M:chain_get_my_repair_state(NameHead, Node),
            pre_init = ?M:chain_get_ds_repair_state(NameHead, Node);
        true ->
            ok
    end,

    if
        InsertP == true ->
            %% Set some stuff in head before starting repair.  These things
            %% should be propagated to tail now.
            ok = ?M:set(NameHead, Node, <<"fib">>, <<"11235813213455">>),
            timer:sleep(200),
            ok = ?M:set(NameHead, Node, <<"pi">>, <<"314159265358979323846264338">>),
            timer:sleep(200);
        true ->
            ok
    end,

    if
        StartupP == true ->
            ok = ?M:chain_start_repair(NameHead, Node),
            {error, _, _} = ?M:chain_start_repair(NameHead, Node),
            %% We're vulnerable to race conditions here, 200ms may not
            %% be enough.  To fix, we'd have to poll repair state of
            %% remote bricks.
            timer:sleep(500);
        true ->
            ok
    end,

    if
        EtsTestP == true ->
            HM2 = xform_ets_dump(NameHead, ets:tab2list(NameHeadETS)),
            TM2 = xform_ets_dump(NameTail, ets:tab2list(NameTailETS)),
            ?DBG(HM2),
            ?DBG(TM2),
            HM2 = TM2,
            io:format("GOOD, both bricks are equal:~n~p~n~p~n.\n", [HM2, TM2]);
        true ->
            ok
    end,

    if
        KillP == true ->
            ?M:stop(whereis(NameHead)),
            ?M:stop(whereis(NameTail));
        true ->
            ok
    end,

    io:format("Test chain_t60: leave\n"),

    ok.

chain_t61(OptionList) ->
    io:format("Test chain_t61: start\n"),

    Tab = regression,
    Name1 = regression_1,
    Name2 = regression_2,
    Node = node(),
    Chain1Name = ch1,
    Chain1List = [{Chain1Name, [{Name1, Node}]}],
    Chain2Name = ch2,
    Chain2List = [{Chain2Name, [{Name2, Node}]}],
    FirstBrick = [{Name1, Node}],
    BothBricks = [{Name1, Node}, {Name2, Node}],
    Fstop_start = fun(N) ->
                          rpc:call(Node, brick_server, stop, [N]),
                          catch exit(whereis(N), byebye),
                          FilesH = filelib:wildcard("*." ++ atom_to_list(N) ++
                                                    ",*.LOG", "."),
                          [file:delete(F) || F <- FilesH],
                          start_and_flush_common_log(),
                          os:cmd("rm -rf hlog.*" ++ atom_to_list(N) ++ "*"),
                          {ok, _Pid} = safe_start_link(N, OptionList)
                  end,

    Fstop_start(Name1),
    timer:sleep(300),
    ?M:chain_role_standalone(Name1, Node),
    ?M:chain_set_my_repair_state(Name1, Node, ok),

    LHmig1 = brick_hash:naive_init(Chain1List),
    GHmig1 = brick_hash:init_global_hash_state(true, pre, 1,
                                               LHmig1, Chain1List,
                                               LHmig1, Chain1List),
    brick_server:chain_hack_set_global_hash(Name1, Node, GHmig1),
    brick_server:migration_start_sweep(Name1, Node, foocookie, Chain1Name, []),
    brick_simple:unset_gh(Node, Tab),
    brick_simple:set_gh(Node, Tab, GHmig1),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Race condition #1:
    %% a. Send sweep including K.
    %% b. Send set for K, before step a has finished logging to disk.
    %% We really only need one brick for this test.
    Race1Key = <<"1">>,
    Race1Sweep = {ch_sweep_from_other, self(), regression_chFOO,
                  [{insert, {<<"0">>, 42, <<"v 0">>, 0, []}},
                   {insert, {Race1Key, 42, <<"v 1">>, 0, []}},
                   {insert, {<<"2">>, 42, <<"v 2">>, 0, []}}],
                  <<"2">>},
    gen_server:cast({Name1, Node}, Race1Sweep),
    ok = brick_simple:set(Tab, Race1Key, <<"v 1b">>),
    poll_migration_finished(FirstBrick),
    [{ok, _, _} = brick_server:migration_clear_sweep(Br, Nd) ||
        {Br, Nd} <- FirstBrick],
    error_logger:info_msg("Test race1 finished\n"),

%     Race condition #2:
%     a. Send set for K.
%     b. Send sweep including K, before step a has finished logging to disk.
%     Use 2 bricks this time.
%     Fstop_start(Name1),
%     Fstop_start(Name2),
%     timer:sleep(300),
%     ?M:chain_role_standalone(Name1, Node),
%     ?M:chain_set_my_repair_state(Name1, Node, ok),
%     ?M:chain_role_standalone(Name2, Node),
%     ?M:chain_set_my_repair_state(Name2, Node, ok),

%     LHmig2a = brick_hash:naive_init(Chain1List),
%     LHmig2b = brick_hash:naive_init(Chain2List),
%     GHmig2 = brick_hash:init_global_hash_state(true, pre, 1,
%                                              LHmig2a, Chain1List,
%                                              LHmig2b, Chain2List),
%     brick_server:chain_hack_set_global_hash(Name1, Node, GHmig2),
%     brick_server:chain_hack_set_global_hash(Name2, Node, GHmig2),
%     brick_simple:unset_gh(Node, Tab),
%     brick_simple:set_gh(Node, Tab, GHmig2),

%     brick_server:migration_start_sweep(Name1, Node, foocookie, Chain1Name, []),
%     brick_server:migration_start_sweep(Name2, Node, foocookie, Chain2Name, []),
%     {Name1, Node} ! kick_next_sweep,
%     {Name2, Node} ! kick_next_sweep,
%     timer:sleep(20),
%     OK, now let's do the race, using the same bits from race #1.
%     spawn(fun() -> ok = brick_simple:set(Tab, Race1Key, <<"v 1b">>) end),
%     erlang:yield(),
%     timer:sleep(10),
%     gen_server:cast({Name1, Node}, Race1Sweep),
%     timer:sleep(1000),
%     _X = brick_simple:get(Tab, Race1Key), io:format("_X = ~p\n", [_X]),

    %% Race #3: setup 2 bricks, chains 1 & 2 migrating -> 1

    Fstop_start(Name1),
    Fstop_start(Name2),
    timer:sleep(500),
    ?M:chain_role_standalone(Name1, Node),
    ?M:chain_set_my_repair_state(Name1, Node, ok),
    ?M:chain_role_standalone(Name2, Node),
    ?M:chain_set_my_repair_state(Name2, Node, ok),

    LHmig3a = brick_hash:naive_init(Chain1List ++ Chain2List),
    LHmig3b = brick_hash:naive_init(Chain1List),
    GHmig3 = brick_hash:init_global_hash_state(true, pre, 1,
                                               LHmig3a, Chain1List ++ Chain2List,
                                               LHmig3b, Chain1List),
    brick_server:chain_hack_set_global_hash(Name1, Node, GHmig3),
    brick_server:chain_hack_set_global_hash(Name2, Node, GHmig3),
    brick_simple:unset_gh(Node, Tab),
    brick_simple:set_gh(Node, Tab, GHmig3),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Race condition #3 continued: ping-pong
    %% a. Two chains, C1 & C2, are migrating to a single chain, C1.
    %% b. Both chains are empty.
    %% c. Migration sweeps of both tables finish while they're still empty.
    %% d. Any operation attempts to access either chain.

    brick_server:migration_start_sweep(Name1, Node, foocookie, Chain1Name, []),
    brick_server:migration_start_sweep(Name2, Node, foocookie, Chain2Name, []),
    {Name1, Node} ! kick_next_sweep,
    {Name2, Node} ! kick_next_sweep,
    timer:sleep(200),
    key_not_exist = brick_simple:get(Tab, <<"0007">>),
    poll_migration_finished(BothBricks),
    [{ok, _, _} = brick_server:migration_clear_sweep(Br, Nd) ||
        {Br, Nd} <- BothBricks],
    error_logger:info_msg("Test race3 finished\n"),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Race condition #4: Start a slow migration, kill a brick, then
    %% have the restart work correctly.
    %% At the start we have 1 chain of length 1 via GHmig3.
    NumRace3 = 30,
    [begin KV = integer_to_list(X), ok = brick_simple:set(Tab, KV, KV) end ||
        X <- lists:seq(0, NumRace3)],
    GHmig4 = brick_hash:init_global_hash_state(true, pre, 1,
                                               LHmig3b, Chain1List,
                                               LHmig3a, Chain1List ++ Chain2List),
    brick_server:chain_hack_set_global_hash(Name1, Node, GHmig4),
    brick_server:chain_hack_set_global_hash(Name2, Node, GHmig4),
    brick_simple:unset_gh(Node, Tab),
    brick_simple:set_gh(Node, Tab, GHmig4),
    SlowOpts = [{max_keys_per_iter, 5}, {propagation_delay, 500}],
    ok = brick_server:migration_start_sweep(Name1, Node, foocookie, Chain1Name,
                                            SlowOpts),
    ok = brick_server:migration_start_sweep(Name2, Node, foocookie, Chain2Name,
                                            SlowOpts),

    %% If 30 keys, 5 keys per migration sweep, 500ms sweep delay, then
    %% sleeping 2 sec is in the middle and safely away from the end of
    %% the migration.
    timer:sleep(2000),
    %% Do not use Fstop_start() here: it will delete our brick's state!
% io:format("DBG: dumping Name1 log\n"),
% brick_ets:debug_scan("hlog.regression_1"),
    error_logger:info_msg("Stopping ~p\n", [Name1]),
    ?M:stop(Name1),
    catch exit(whereis(Name1), kill), timer:sleep(50),
    io:format("DBG: whereis(Name1) = ~p\n", [whereis(Name1)]),

    %% Restart everything.
    {ok, _} = safe_start_link(Name1, OptionList),
    ?M:chain_role_standalone(Name1, Node),
    ?M:chain_set_my_repair_state(Name1, Node, ok),
    brick_server:chain_hack_set_global_hash(Name1, Node, GHmig4),
    error_logger:info_msg("Startup of ~p finished, start sweeping\n", [Name1]),
% io:format("DBG: dumping Name1 log AGAIN\n"),
% brick_ets:debug_scan("hlog.regression_1"),
    ok = brick_server:migration_start_sweep(Name1, Node, foocookie, Chain1Name,
                                            SlowOpts),
    %% Nothing more to do here ... need to look at app log for
    %% "twiddle" log messages.  It should say 'true' for all
    %% {do_logging, true} bricks.  Otherwise, the absense of a timeout
    %% during this test or any other is a sign that the feature is
    %% working correctly.

    poll_migration_finished(BothBricks),
    [{ok, _, _} = brick_server:migration_clear_sweep(Br, Nd) ||
        {Br, Nd} <- BothBricks],
    error_logger:info_msg("Test race4 finished\n"),

    catch brick_server:stop(Name1),
    catch brick_server:stop(Name2),
    io:format("Test chain_t61: end\n"),
    ok.

chain_t62(OptionList) ->
    io:format("Test chain_t62: start ~p\n", [OptionList]),

    NameHead = regression_head,
    NameTail = regression_tail,
    Node = node(),
    Chain = {t62_chain, [{NameHead, Node}, {NameTail, Node}]},
    if 1 == 1 ->
            catch rpc:call(Node, brick_shepherd, stop_brick, [NameHead]),
            catch exit(whereis(NameHead), byebye),
            FilesH = filelib:wildcard("*." ++ atom_to_list(NameHead) ++ ",*.LOG", "."),
            [file:delete(F) || F <- FilesH],
            start_and_flush_common_log(),
            os:cmd("rm -rf hlog.*" ++ atom_to_list(NameHead) ++ "*"),
            {ok, _HeadPid} = safe_start_link(NameHead, OptionList),

            catch rpc:call(Node, brick_shepherd, stop_brick, [NameHead]),
            catch exit(whereis(NameTail), byebye),
            FilesT = filelib:wildcard("*." ++ atom_to_list(NameTail) ++ ",*.LOG", "."),
            [file:delete(F) || F <- FilesT],
            start_and_flush_common_log(),
            os:cmd("rm -rf hlog.*" ++ atom_to_list(NameTail) ++ "*"),
            {ok, _TailPid} = safe_start_link(NameTail, OptionList)
    end,

%%     chain_start_entire_chain(Chain, OptionList, true),
    if true ->
            ?M:chain_role_standalone(NameHead, Node),
            ?M:chain_set_my_repair_state(NameHead, Node, ok),
            ?M:chain_set_my_repair_state(NameTail, Node, testing_harness_force_pre_init),
            ?M:chain_role_tail(NameTail, Node, NameHead, Node, []),
            pre_init = ?M:chain_get_my_repair_state(NameTail, Node),

            %% Set the will-be-head => head
            ?M:chain_role_head(NameHead, Node, NameTail, Node, []),
            ok = ?M:chain_get_my_repair_state(NameHead, Node),
            pre_init = ?M:chain_get_ds_repair_state(NameHead, Node)
    end,
    if true ->
            ok = ?M:chain_start_repair(NameHead, Node),
            _ = repair_poll(NameHead, Node, 100)
    end,

    Key = <<"/contended-key">>,
    OldVal = <<"old-val">>,
    MidVal = <<"mid-val">>,
    _NewVal = <<"new-val">>,
    ok = ?M:set(NameHead, Node, Key, OldVal),
    {ok, _, OldVal} = ?M:get(NameTail, Node, Key),
    Ack0 = get_chain_last_ack(NameHead, Node),
    Ack1 = get_chain_last_ack_different(NameHead, Node, Ack0, 10),
    io:format("DEBUG: ~p ~p\n", [Ack0, Ack1]),

    ?M:delete(NameHead, Node, Key),
    spawn(fun() -> ?M:set(NameHead, Node, Key, MidVal) end),
    %%
    %% This is timing-dependent {sigh}.  Values lower than 10 are not
    %% repeatable on Scott's laptop.  At 10, the failure rate is only
    %% 80-90%.
    %%
    timer:sleep(10),
    brick_server:chain_flush_log_downstream(NameHead, Node),
    %% ok = ?M:set(NameHead, Node, Key, NewVal),
    Ack2 = get_chain_last_ack_different(NameHead, Node, Ack1, 10),
    io:format("DEBUG: ~p ~p\n", [Ack1, Ack2]),

    _GetRes = ?M:get(NameTail, Node, Key),
    {ok, _, MidVal} = ?M:get(NameTail, Node, Key),

    catch chain_stop_entire_chain(Chain),
    catch brick_server:stop(NameHead),
    catch brick_server:stop(NameTail),
    io:format("Test chain_t62: end\n"),
    ok.

%%
%% Internal/helper functions
%%

flush_mailbox() ->
    receive M -> [M|flush_mailbox()]
    after 0   -> []
    end.

repair_poll(Brick, Node, PollMs) ->
    repair_poll(Brick, Node, PollMs, chain_get_ds_repair_state).

my_repair_poll(Brick, Node, PollMs) ->
    repair_poll(Brick, Node, PollMs, chain_get_my_repair_state).

repair_poll(Brick, Node, PollMs, PollFunc) ->
    RepairPoll = fun(Acc) ->
                         case (catch ?M:PollFunc(Brick, Node)) of
                             ok ->
                                 ?DBG({repair_poll, Brick, Node, ok}),
                                 {false, Acc};
                             _St ->
                                 io:format("rp ~w ~w ~w ~w, ", [PollFunc, Brick, Node, _St]),
                                 ?DBG({repair_poll, Brick, Node, _St}),
                                 timer:sleep(PollMs),
                                 {true, Acc + 1}
                         end
                 end,
    gmt_loop:do_while(RepairPoll, 0).

sync_poll(Brick, Node, PollMs) ->
    SyncPoll = fun(Acc) ->
                       case ?M:chain_get_downstream_serials(Brick, Node) of
                           {X, X} ->
                               ?DBG({sync_poll, Brick, Node, ok}),
                               {false, Acc};
                           _St ->
                               io:format("sp: ~p\n", [_St]),
                               ?DBG({sync_poll, Brick, Node, _St}),
                               timer:sleep(PollMs),
                               {true, Acc + 1}
                       end
               end,
    gmt_loop:do_while(SyncPoll, 0).

get_chain_last_ack(Brick, Node) ->
    {ok, Ps} = brick_server:status(Brick, Node),
    Cs = proplists:get_value(chain, Ps),
    proplists:get_value(chain_last_ack, Cs).

get_chain_last_ack_different(Brick, Node, LastAck, SleepAmount) ->
    gmt_loop:do_while(
      fun(_) ->
              NewLastAck = get_chain_last_ack(Brick, Node),
              if NewLastAck /= LastAck ->
                      {false, NewLastAck};
                 true ->
                      timer:sleep(SleepAmount),
                      {true, ignored}
              end
      end, ignored).

chain_start_entire_chain({ChainName, BrickList}, Options, FullStartP)
  when is_atom(ChainName), is_list(BrickList) ->
    %% Individual bricks don't care about chain name.
    chain_start_entire_chain(BrickList, Options, FullStartP);
chain_start_entire_chain(BrickList, Options, FullStartP) ->
    start_and_flush_common_log(),
    if length(BrickList) == 1 ->
            chain_start_entire_1(BrickList, Options, FullStartP);
       length(BrickList) == 2 ->
            chain_start_entire_2(BrickList, Options, FullStartP);
       true ->
            chain_start_entire_N(BrickList, Options, FullStartP)
    end.

chain_start_entire_1([{Name, Node}] = BrickList, Options, FullStartP) ->
    if FullStartP ->
            start_bricklist(BrickList, Options),
            ok = brick_server:chain_set_my_repair_state(Name, Node, ok);
       true ->
            skip
    end,
    ok = brick_server:chain_role_standalone(Name, Node),
    ok.

chain_start_entire_2([{Head, NodeHead}, {Tail, NodeTail}] = BrickList, Options, FullStartP) ->
    if FullStartP ->
            start_bricklist(BrickList, Options),
            %% Get the head running first, then build the chain backward.
            ok = brick_server:chain_set_my_repair_state(Head, NodeHead, ok),
            ok = brick_server:chain_role_head(Head, NodeHead,
                                                  Tail, NodeTail,
                                                  [{official_tail, true}]),
            ok = brick_server:chain_role_tail(Tail, NodeTail,
                                                  Head, NodeHead,
                                                  [{official_tail, false}]),
            ok = brick_server:chain_start_repair(Head, NodeHead),
            _NumPolls = repair_poll(Head, NodeHead, 100),
            io:format("ZZZ: repair ~w -> ~w took ~w polls\n",
                      [{Head, NodeHead}, {Tail, NodeTail}, _NumPolls]);
       true ->
            skip
    end,
    ok = brick_server:chain_role_tail(Tail, NodeTail, Head, NodeHead, []),
    ok = brick_server:chain_role_head(Head, NodeHead, Tail, NodeTail, []),
    ok.

chain_start_entire_N(BrickList, Options, FullStartP) when length(BrickList) > 2 ->
    [{Head, NodeHead}, {FirstMid, NodeFirstMid}|_] = BrickList,
    RevBrickList = lists:reverse(BrickList),
    [{Tail, NodeTail}, {LastMid, NodeLastMid}|_] = RevBrickList,
    NumBricks = length(BrickList),
    Triples = lists:map(
                fun(Pos) -> lists:sublist(BrickList, Pos - 1, 3) end,
                lists:seq(2, 1 + (NumBricks -2 ))),

    if FullStartP ->
            start_bricklist(BrickList, Options),

            %% Get the head running first, then build the chain backward.
            ok = brick_server:chain_set_my_repair_state(Head, NodeHead, ok),
            ok = brick_server:chain_role_head(Head, NodeHead,
                                                  FirstMid, NodeFirstMid,
                                                  [{official_tail, true}]),
            %% Tail
            ok = brick_server:chain_role_tail(Tail, NodeTail,
                                                  LastMid, NodeLastMid,
                                                  [{official_tail, false}]),
            lists:foreach(
              fun([{Up, UpNode}, {Mid, MidNode}, {Down, DownNode}] =_QQQ) ->
?DBG(_QQQ),
                      ok = brick_server:chain_role_middle(Mid, MidNode,
                                                              Up, UpNode,
                                                              Down, DownNode,
                                                              [{official_tail, false}])
              end, Triples),

            ok = brick_server:chain_get_my_repair_state(Head, NodeHead),
            [pre_init = brick_server:chain_get_ds_repair_state(B, N) ||
                {B, N} <- lists:sublist(BrickList, 1, NumBricks - 1)],

            %% Ok, now start repair.
            io:format("ZZZ: Repair starting at ~w ~w\n", [Head, NodeHead]),
            ok = brick_server:chain_start_repair(Head, NodeHead),
            _NumPolls = repair_poll(Head, NodeHead, 100),
            io:format("ZZZ: repair ~w -> ~w took ~w polls\n",
                      [{Head, NodeHead}, {FirstMid, NodeFirstMid}, _NumPolls]),
            %%io:format("\n\n---sleep---\n"),timer:sleep(2000),
            lists:foreach(
              fun([_, {Mid, NodeMid}, _]) ->
                      io:format("ZZZ: Repair starting at ~w ~w\n", [Mid, NodeMid]),
                      ok = brick_server:chain_start_repair(Mid, NodeMid),
                      _NumPollsM = repair_poll(Mid, NodeMid, 100),
                      io:format("ZZZ: repair at ~w took ~w polls\n",
                                [{Mid, NodeMid}, _NumPollsM])
                      %%,io:format("\n\n---sleep middle---\n"),timer:sleep(2000)
              end, Triples);
       true ->
            skip
    end,

    %% Define the final roles, starting at the tail of the chain.
    ok = brick_server:chain_role_tail(Tail, NodeTail,
                                          LastMid, NodeLastMid,
                                          []),
    lists:foreach(
      fun([{Up, UpNode}, {Mid, MidNode}, {Down, DownNode}]) ->
              ok = brick_server:chain_role_middle(Mid, MidNode,
                                                      Up, UpNode,
                                                      Down, DownNode,
                                                      [])
      end, lists:reverse(Triples)),
    ok = brick_server:chain_role_head(Head, NodeHead,
                                          FirstMid, NodeFirstMid,
                                          []),
    ok.

start_bricklist(BrickList, Options) ->
    lists:foreach(
      fun({Name, Node}) ->
              rpc:call(Node, brick_shepherd, start_brick, [Name, Options]),
              ok = poll_brick_status(Name, Node)
      end, BrickList).

chain_stop_entire_chain({ChainName, BrickList})
  when is_atom(ChainName), is_list(BrickList) ->
    chain_stop_entire_chain(BrickList);
chain_stop_entire_chain(BrickList) ->
    lists:foreach(
      fun({Name, Node}) ->
              rpc:call(Node, brick_shepherd, stop_brick, [Name])
      end, BrickList).

shuffle(L) ->
    lists:sort(fun(_, _) -> random:uniform(100) =< 50 end, L).

perms([]) ->
    [[]];
perms(L) ->
    [[H|T] || H <- L, T <- perms(L -- [H])].

keyuniq(_N, []) ->
    [];
keyuniq(N, [H|T]) ->
    Key = element(N, H),
    [H | keyuniq(N, lists:keydelete(Key, N, T))].

put_rand_keys_via_simple(Tab, MaxKey, Num, KeyPrefix, KeySuffix, ValLen,
                         SleepTime, DoAsyncP) ->
    Val = list_to_binary(lists:duplicate($v, ValLen)),
    [ok = begin
              Parent = self(),
              F = fun() ->
                          {A, B, C} = now(),
                          random:seed(A, B, C),
                          RandL = integer_to_list(random:uniform(MaxKey)),
                          Key = gmt_util:bin_ify(KeyPrefix ++ RandL ++ KeySuffix),
                          case brick_simple:set(Tab, Key, Val, 60*1000) of
                              ok -> ok;
                              {ts_error, _} -> ok
                          end,
                          if (DoAsyncP) ->
                                  io:format("set ~w ", [Parent]),
                                  unlink(Parent),
                                  exit(normal);
                             true ->
                                  ok
                          end
                  end,
              if DoAsyncP -> spawn_link(F);
                 true     -> F()
              end,
              if SleepTime > 0 -> timer:sleep(SleepTime);
                 true          -> ok  end,
              ok
          end || _ <- lists:seq(1, Num)],
    ok.

xform_ets_dump(TabName, [H|T]) ->
    if is_tuple(element(3, H)) ->
            {SeqNum, Offset} = element(3, H),
            Fun = fun(Su, FH) ->
                          Bin = gmt_hlog:read_hunk_member_ll(FH, Su, md5, 1),
                          Su#hunk_summ{c_blobs = [Bin]}
                  end,
            %% OLD OLD: LogDir = "hlog." ++ atom_to_list(TabName),
            LogDir = gmt_hlog:log_name2data_dir(?GMT_HLOG_COMMON_LOG_NAME),
            Summ = gmt_hlog:read_hunk_summary(LogDir, SeqNum, Offset, 0, Fun),
            NewVal = hd(Summ#hunk_summ.c_blobs),
            [setelement(3, H, NewVal)|xform_ets_dump(TabName, T)];
       true ->
            [H|xform_ets_dump(TabName, T)]
    end;
xform_ets_dump(_TabName, []) ->
    [].

safe_start_link(Brick, OptionList) ->
    catch unlink(whereis(Brick)),
    catch exit(whereis(Brick), kill),
    timer:sleep(50),
    {ok, _} = Res = ?M:start_link(Brick, OptionList),
    ok = poll_brick_status(Brick, node()),
    Res.

poll_brick_status(Brick, Node) ->
    poll_brick_status(Brick, Node, 100).

poll_brick_status(_Brick, _Node, 0) ->
    error;
poll_brick_status(Brick, Node, N) ->
    case (catch ?M:status(Brick, Node, 20)) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(10),
            poll_brick_status(Brick, Node, N - 1)
    end.

poll_migration_finished(AllHeadBricks) ->
    Fpoll = fun(Bricks) ->
                    NS = lists:foldl(
                           fun({BrName, BrNode} = _Br, Num) ->
                                   {ok, Ps} = brick_server:status(BrName,
                                                                  BrNode),
                                   Ss = proplists:get_value(sweep, Ps),
                                   case proplists:get_value(status, Ss) of
                                       done -> Num + 0;
                                       _X   -> Num + 1
                                   end
                           end, 0, Bricks),
                    if NS == 0 ->
                            {false, Bricks};
                       true ->
                            timer:sleep(1000),
                            {true, Bricks}
                    end
            end,
    gmt_loop:do_while(Fpoll, AllHeadBricks).

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

start_and_flush_common_log() ->
    start_common_log(),
    flush_common_log().

flush_common_log() ->
    gmt_hlog_common:full_writeback(?GMT_HLOG_COMMON_LOG_NAME).

start_common_log() ->
    Ps = [{common_log_name, ?GMT_HLOG_COMMON_LOG_NAME}],
    (catch gmt_hlog_common:start(Ps)). % Avoid linking, use start()

exp0() ->
    exp0(true, 2, []).

exp0(SetUpBricksP, NumMigratingToView, Options)
  when NumMigratingToView == 1; NumMigratingToView == 2 ->
    %% Shorthand for a cheetsheet test case.
    ChList20 = brick_hash:invent_nodelist(2, 1, node(), 20),
    [Ch20, Ch21] = ChList20,
    {Chain20, [{Brick20, _Node20}]} = Ch20,
    {Chain21, [{Brick21, _Node21}]} = Ch21,
    if not SetUpBricksP ->
            ok;
       true ->
            [begin
                 {ok, Pid} = brick_server:start_link(Br, []),
                 unlink(Pid)
             end ||
                Br <- [Brick20, Brick21]],
            [brick_server:chain_role_standalone(Br, node()) ||
                Br <- [Brick20, Brick21]],
            [brick_server:chain_set_my_repair_state(Br, node(), ok) ||
                Br <- [Brick20, Brick21]],
            %% Set some initial values (104 total)
            KVs = [{foo, foo1}, {bar, bar1}, {baz, baz1}, {zoo, zoo1}],
            [brick_server:delete(Brick20, node(), K) || {K, _V} <- KVs],
            [brick_server:add(Brick20, node(), K, V) || {K, V} <- KVs],
            [begin
                 K = "key " ++ integer_to_list(N),
                 V = "value " ++ integer_to_list(N),
                 brick_server:delete(Brick20, node(), K),
                 brick_server:add(Brick20, node(), K, V)
             end || N <- lists:seq(0,99)]
    end,

    LH1 = brick_hash:naive_init([Ch20]),
%     GH1 = brick_hash:init_global_hash_state(false, unused, 1,
%                                               LH1, [Ch20], LH1, [Ch20]),
    LH2 = brick_hash:naive_init(ChList20),

    {LHcur, LHnew} =
        if NumMigratingToView == 2 ->
                {LH1, LH2};
           NumMigratingToView == 1 ->
                {LH2, LH1}
        end,
    %% Using a chain list that's too long (list len = 2, LH1 uses only 1)
    %% is OK.
    GHnew = brick_hash:init_global_hash_state(true, unused, 1,
                                                  LHcur, ChList20,
                                                  LHnew, ChList20),

    [brick_server:chain_hack_set_global_hash(Br, node(), GHnew) ||
        Br <- [Brick20, Brick21]],

    X = brick_server:migration_start_sweep(Brick20, node(), foocookie, Chain20,
                                           [{max_keys_per_iter, 5}|Options]),
    Y = brick_server:migration_start_sweep(Brick21, node(), foocookie, Chain21,
                                           [{max_keys_per_iter, 5}|Options]),
    io:format("start sweep X = ~p\n", [X]),
    io:format("start sweep Y = ~p\n", [Y]),
    ok_go_watch_it.

slurp_all(Tab, Key, SleepTime) ->
    slurp_all(Tab, Key, Key, SleepTime,
              brick_simple:get_many(Tab, Key, 500, [witness,{binary_prefix,Key}]), 0).

slurp_all(Tab, Key, Prefix, SleepTime, {ok, {Ks, Bool}}, Num) ->
    %%io:format("\nSlurp: read at ~p got ~p for ~p\n", [Key, length(Ks), Bool]),
    if Bool == false ->
            Ks;
       true ->
            LastKey = if length(Ks) == 0 ->
                              %%io:format("GOT ZERO, "),
                              Key;
                         true ->
                              element(1, lists:last(Ks))
                      end,
            timer:sleep(SleepTime),
            Ks ++ slurp_all(Tab, LastKey, Prefix, SleepTime,
                            brick_simple:get_many(Tab, LastKey, 500, [witness,{binary_prefix,Prefix}]),
                            Num + length(Ks))
    end.

reliable_gdss_stop() ->
    catch exit(whereis(brick_admin_sup), kill),
    catch application:stop(gdss),
    ok.

reliable_gdss_stop_and_start() ->
    ok = reliable_gdss_stop(),
    ok = application:start(gdss).

qc_check(NumTests, NumMore, Props) ->
    true = eqc:quickcheck(
             eqc_gen:noshrink(
               eqc:numtests(
                 NumTests, eqc_statem:more_commands(NumMore, Props)))).

cl_qc_hlog_local_qc__t1() ->
    cl_qc_hlog_local_qc__t1([?NUM_QC_TESTS_DEFAULT_A]).

cl_qc_hlog_local_qc__t1([TestsA]) ->
    Tests = list_to_integer(atom_to_list(TestsA)),
    %% GDSS app must not be running for this test.
    ok = reliable_gdss_stop(),
    true = qc_check(Tests, 10, hlog_local_qc:prop_local_log()),
    ok.

cl_qc_hlog_qc__t1() ->
    cl_qc_hlog_qc__t1([?NUM_QC_TESTS_DEFAULT_A]).

cl_qc_hlog_qc__t1([TestsA]) ->
    Tests = list_to_integer(atom_to_list(TestsA)),
    ok = reliable_gdss_stop_and_start(),
    true = qc_check(Tests, 10, hlog_qc:prop_log(false)),
    ok.

cl_qc_hlog_qc__t2() ->
    cl_qc_hlog_qc__t1([?NUM_QC_TESTS_DEFAULT_A]).

cl_qc_hlog_qc__t2([TestsA]) ->
    Tests = list_to_integer(atom_to_list(TestsA)),
    ok = reliable_gdss_stop_and_start(),
    true = qc_check(Tests, 10, hlog_qc:prop_log(true)),
    ok.

cl_qc_hlog_qc_blackbox__t1() ->
    cl_qc_hlog_qc_blackbox__t1([?NUM_QC_TESTS_DEFAULT_A]).

cl_qc_hlog_qc_blackbox__t1([TestsA]) ->
    Tests = list_to_integer(atom_to_list(TestsA)),
    ok = reliable_gdss_stop_and_start(),
    true = qc_check(Tests, 10, hlog_qc_blackbox:prop_commands()),
    ok.

cl_qc_my_pread_qc__t1() ->
    cl_qc_my_pread_qc__t1([?NUM_QC_TESTS_DEFAULT_A]).

cl_qc_my_pread_qc__t1([TestsA]) ->
    %% my_pread_qc tests are much shorter on average than most other QC tests.
    Tests = list_to_integer(atom_to_list(TestsA)) * 5,
    ok = reliable_gdss_stop_and_start(),
    true = qc_check(Tests, 10, my_pread_qc:prop_pread()),
    ok.

cl_qc_repair_qc__t1() ->
    cl_qc_repair_qc__t1([?NUM_QC_TESTS_DEFAULT_A]).

cl_qc_repair_qc__t1([TestsA]) ->
    %% Tests for qc_repair.erl are typically much slower than other QC tests.
    Tests = case list_to_integer(atom_to_list(TestsA)) div 5 of
                0 -> 1;
                N -> N
            end,
    io:format("\n\n\nNOTE: Adjusted repair tests = ~p\n\n\n\n", [Tests]),
    timer:sleep(3000),

    ok = reliable_gdss_stop(),
    os:cmd("rm -r Schema.local"),
    os:cmd("rm -rf hlog.*"),
    ok = reliable_gdss_stop_and_start(),
    %% Now need to create tab1 with chains of length 2
    ok = brick_admin:bootstrap_local([], true, $/, 3, 2, 2, [node()]),
    timer:sleep(5000),

    Gen = repair_qc:prop_repair("./Unit-Quick-Files/repair.3k-keys",
                                tab1_ch1, tab1_ch1_b1, [tab1_ch1_b2]),
    true = qc_check(Tests, 1, Gen),
    ok.

cl_qc_repair_qc__t2() ->
    cl_qc_repair_qc__t2([?NUM_QC_TESTS_DEFAULT_A]).

cl_qc_repair_qc__t2([TestsA]) ->
    %% Tests for qc_repair.erl are typically much slower than other QC tests.
    Tests = case list_to_integer(atom_to_list(TestsA)) div 5 of
                0 -> 1;
                N -> N
            end,
    io:format("\n\n\nNOTE: Adjusted repair tests = ~p\n\n\n\n", [Tests]),
    timer:sleep(3000),

    ok = reliable_gdss_stop(),
    os:cmd("rm -r Schema.local"),
    os:cmd("rm -rf hlog.*"),
    ok = reliable_gdss_stop_and_start(),
    %% Now need to create tab1 with chains of length 3
    ok = brick_admin:bootstrap_local([], true, $/, 3, 3, 3, [node()]),
    timer:sleep(25000),

    Gen = repair_qc:prop_repair("./Unit-Quick-Files/repair.3k-keys",
                                tab1_ch1, tab1_ch1_b1, [tab1_ch1_b2,tab1_ch1_b3]),
    true = qc_check(Tests, 1, Gen),
    ok.

cl_qc_simple_qc__t1() ->
    cl_qc_simple_qc__t1([?NUM_QC_TESTS_DEFAULT_A]).

cl_qc_simple_qc__t1([TestsA]) ->
    Tests = list_to_integer(atom_to_list(TestsA)),

    ok = reliable_gdss_stop(),
    os:cmd("rm -r Schema.local"),
    os:cmd("rm -rf hlog.*"),
    ok = reliable_gdss_stop_and_start(),
    %% Now need to create tab1
    ok = brick_admin:bootstrap_local([], true, $/, 3, 1, 1, [node()]),
    timer:sleep(5000),

    true = qc_check(Tests, 10, simple_qc:prop_simple1()),
    ok.

cl_qc_squorum_qc__t1() ->
    cl_qc_squorum_qc__t1([?NUM_QC_TESTS_DEFAULT_A]).

cl_qc_squorum_qc__t1([TestsA]) ->
    Tests = list_to_integer(atom_to_list(TestsA)),
    ok = reliable_gdss_stop_and_start(),
    [ok] = lists:usort(squorum_qc:start_bricks()),
    true = qc_check(Tests, 10, squorum_qc:prop_squorum1()),
    ok.
