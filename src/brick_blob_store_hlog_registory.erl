%%%-------------------------------------------------------------------
%%% Copyright (c) 2015-2016 Hibari developers. All rights reserved.
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
%%% File    : brick_blob_store_hlog_registory.erl
%%% Purpose :
%%%-------------------------------------------------------------------

-module(brick_blob_store_hlog_registory).

-behaviour(gen_server).

%% DEBUG
-compile(export_all).

%% -include("brick_specs.hrl").
-include("brick_hlog.hrl").
-include("brick_blob_store_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% API
-export([start_link/1,
         stop/0,
         get_blob_file_info_for_brick/2,
         set_blob_file_info/1,
         get_blob_file_info/2,
         delete_blob_file_info/2,
         update_score_and_scan_time/5,
         get_top_scores/1,
         get_live_hunk_scan_time_records/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).


%% ====================================================================
%% types, specs and records
%% ====================================================================

-type prop() :: {atom(), term()}.
-type count() :: non_neg_integer().

-record(state, {
          leveldb :: h2leveldb:db()
         }).
%% -type state() :: #state{}.

%% sext-encoded binaries are ordered by number of tuple elements, first element of tuple.
%%
%% i  blob_file_info         3 elements  {i, brickname(), seqnum()}
%% h  live_hunk_scan_time    4 elements  {h, system_time_seconds(), brickname(), seqnum()}
%% m  scrub_scan_time (md5)  4 elements  {m, system_time_seconds(), brickname(), seqnum()}
%% s  score:                 4 elements  {s, -(float()), brickname(), seqnum()}

-define(LIVE_HUNK_SCAN_TIME_FIRST_KEY, {g, 0, 0, 0}).
-define(LIVE_HUNK_SCAN_TIME_LAST_KEY,  {i, 0, 0, 0}).
%% -define(SCRUB_SCAN_TIME_FIRST_KEY,     {l, 0, 0, 0}).
%% -define(SCRUB_SCAN_TIME_LAST_KEY,      {n, 0, 0, 0}).
-define(SCORE_FIRST_KEY,               {r, 0, 0, 0}).

-define(UNDEFINED_SYSTEM_TIME, -1).

-define(TIMEOUT, 60 * 1000).

-define(TIME, gmt_time_otp18).

%% ====================================================================
%% API
%% ====================================================================

-spec start_link([prop()]) -> {ok,pid()} | ignore | {error,term()}.
start_link(PropList) ->
    gen_server:start_link({local, ?HLOG_REGISTORY_SERVER_REG_NAME},
                          ?MODULE, [PropList], []).

-spec stop() -> ok | {error, term()}.
stop() ->
    gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, stop).

-spec get_blob_file_info_for_brick(brickname(), count())
                                  -> {ok, IsTruncated::boolean(), [blob_file_info()]}
                                         | not_exist | {error, term()}.
get_blob_file_info_for_brick(BrickName, MaxRecords) ->
    {ok, DB} = gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, get_leveldb, ?TIMEOUT),
    Fun = fun({_Key, Value}, Records) ->
                  [ binary_to_term(Value) | Records ]
          end,
    h2leveldb:fold_kvs(DB, Fun, [],
                            first_blob_file_info_key_for_brick(BrickName),
                            last_blob_file_info_key_for_brick(BrickName),
                            MaxRecords).

-spec set_blob_file_info(blob_file_info()) -> ok | {error, term()}.
set_blob_file_info(#blob_file_info{live_hunk_scaned=undefined}=BlobFileInfo) ->
    set_blob_file_info(BlobFileInfo#blob_file_info{live_hunk_scaned=?UNDEFINED_SYSTEM_TIME});
set_blob_file_info(#blob_file_info{brick_name=BrickName, seqnum=SeqNum}=BlobFileInfo) ->
    {ok, DB} = gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, get_leveldb, ?TIMEOUT),
    BIKey = blob_file_info_key(BrickName, SeqNum),
    Mutations0 =
        case h2leveldb:get(DB, BIKey) of
            {ok, Bin} ->
                BlobFileInfo0 = binary_to_term(Bin),
                [h2leveldb:make_delete(BIKey),
                 h2leveldb:make_delete(score_key(BlobFileInfo0)),
                 h2leveldb:make_delete(live_hunk_scan_time_key(BlobFileInfo0))];
            _ ->
                []
        end,
    Mutations1 = Mutations0 ++
        [h2leveldb:make_put(BIKey, term_to_binary(BlobFileInfo)),
         h2leveldb:make_put(score_key(BlobFileInfo), <<>>),
         h2leveldb:make_put(live_hunk_scan_time_key(BlobFileInfo), <<>>)],
    h2leveldb:write(DB, Mutations1).

-spec get_blob_file_info(brickname(), seqnum()) ->
                                {ok, blob_file_info()} | not_exist | {error, term()}.
get_blob_file_info(BrickName, SeqNum) ->
    {ok, DB} = gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, get_leveldb, ?TIMEOUT),
    case h2leveldb:get(DB, blob_file_info_key(BrickName, SeqNum)) of
        {ok, Bin} ->
            {ok, binary_to_term(Bin)};
        key_not_exist ->
            not_exist;
        {error, _}=Err ->
            Err
    end.

-spec delete_blob_file_info(brickname(), seqnum()) -> ok | {error, term()}.
delete_blob_file_info(BrickName, SeqNum) ->
    {ok, DB} = gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, get_leveldb, ?TIMEOUT),
    BIKey = blob_file_info_key(BrickName, SeqNum),
    case h2leveldb:get(DB, BIKey) of
        {ok, Bin} ->
            BlobFileInfo = binary_to_term(Bin),
            Mutations = [h2leveldb:make_delete(BIKey),
                         h2leveldb:make_delete(score_key(BlobFileInfo)),
                         h2leveldb:make_delete(live_hunk_scan_time_key(BlobFileInfo))],
            h2leveldb:write(DB, Mutations);
        key_not_exist ->
            ok;
        {error, _}=Err ->
            Err
    end.

-spec update_score_and_scan_time(brickname(), seqnum(), float(), float(), system_time_seconds()) ->
                                        ok | key_not_exist | {error, term()}.
update_score_and_scan_time(BrickName, SeqNum, Score, LiveHunkRatio, undefined) ->
    update_score_and_scan_time(BrickName, SeqNum, Score, LiveHunkRatio, ?UNDEFINED_SYSTEM_TIME);
update_score_and_scan_time(BrickName, SeqNum, Score, LiveHunkRatio, LiveHunkScaned) ->
    {ok, DB} = gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, get_leveldb, ?TIMEOUT),
    BIKey = blob_file_info_key(BrickName, SeqNum),
    case h2leveldb:get(DB, BIKey) of
        {ok, Bin} ->
            BlobFileInfo0 = binary_to_term(Bin),
            BlobFileInfo1 = BlobFileInfo0#blob_file_info{
                              score=Score,
                              estimated_live_hunk_ratio=LiveHunkRatio,
                              live_hunk_scaned=LiveHunkScaned
                             },
            Mutations = [h2leveldb:make_delete(score_key(BlobFileInfo0)),
                         h2leveldb:make_delete(live_hunk_scan_time_key(BlobFileInfo0)),
                         h2leveldb:make_put(BIKey, term_to_binary(BlobFileInfo1)),
                         h2leveldb:make_put(score_key(BlobFileInfo1), <<>>),
                         h2leveldb:make_put(live_hunk_scan_time_key(BlobFileInfo1), <<>>)],
            h2leveldb:write(DB, Mutations);
        key_not_exist ->
            key_not_exist;
        {error, _}=Err ->
            Err
    end.

-spec get_top_scores(count()) -> {ok, [score()]} | {error, term()}.
get_top_scores(MaxScores) when MaxScores > 0 ->
    {ok, DB} = gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, get_leveldb, ?TIMEOUT),
    Fun = fun(Key, Scores) ->
                  {s, NegativeScore, BrickName, SeqNum} = sext:decode(Key),
                  Score = #score{score=-(NegativeScore),
                                 brick_name=BrickName,
                                 seqnum=SeqNum},
                  [Score | Scores]
          end,
    case h2leveldb:fold_keys(DB, Fun, [], first_key_for_score(), undefined, MaxScores) of
        {ok, Scores, _IsTruncated} ->
            {ok, Scores};
        Other ->
            Other
    end.

-spec get_live_hunk_scan_time_records(count()) -> {ok, [live_hunk_scan_time()]} | {error, term()}.
get_live_hunk_scan_time_records(MaxRecords) ->
    {ok, DB} = gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, get_leveldb, ?TIMEOUT),
    Fun = fun(Key, Records) ->
                  case sext:decode(Key) of
                      {h, ?UNDEFINED_SYSTEM_TIME, BrickName, SeqNum} ->
                          TS = undefined;
                      {h, TS, BrickName, SeqNum} ->
                          ok
                      end,
                  Record = #live_hunk_scan_time{
                              live_hunk_scaned=TS,
                              brick_name=BrickName,
                              seqnum=SeqNum},
                  [Record | Records]
          end,
    case h2leveldb:fold_keys(DB, Fun, [],
                             first_key_for_live_hunk_scan_time(),
                             last_key_for_live_hunk_scan_time(),
                             MaxRecords) of
        {ok, Records, _IsTruncated} ->
            {ok, Records};
        Other ->
            Other
    end.


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([_Options]) ->
    process_flag(trap_exit, true),
    {ok, RegistoryDB} = open_registory_db(),
    {ok, #state{leveldb=RegistoryDB}}.

handle_call(get_leveldb, _From, #state{leveldb=MetadataDB}=State) ->
    {reply, MetadataDB, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(_Cmd, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

-spec blob_file_info_key(blob_file_info()) -> binary().
blob_file_info_key(#blob_file_info{brick_name=BrickName, seqnum=SeqNum}) ->
    sext:encode({i, BrickName, SeqNum}).

-spec blob_file_info_key(brickname(), seqnum()) -> binary().
blob_file_info_key(BrickName, SeqNum) ->
    sext:encode({i, BrickName, SeqNum}).

-spec first_blob_file_info_key_for_brick(brickname()) -> binary().
first_blob_file_info_key_for_brick(BrickName) ->
    %% There is no negative (or zero) seqnum.
    sext:encode({i, BrickName, -1}).

-spec last_blob_file_info_key_for_brick(brickname()) -> binary().
last_blob_file_info_key_for_brick(BrickName) ->
    %% atom is bigger than any seqnum (integer).
    sext:encode({i, BrickName, stop}).

-spec score_key(blob_file_info()) -> binary().
score_key(#blob_file_info{brick_name=BrickName, seqnum=SeqNum, score=Score}) ->
    sext:encode({s, -(Score), BrickName, SeqNum}).

-spec first_key_for_score() -> binary().
first_key_for_score() ->
    sext:encode(?SCORE_FIRST_KEY).

-spec live_hunk_scan_time_key(blob_file_info()) -> binary().
live_hunk_scan_time_key(#blob_file_info{brick_name=BrickName, seqnum=SeqNum,
                                        live_hunk_scaned=TS}) ->
    sext:encode({h, TS, BrickName, SeqNum}).

-spec first_key_for_live_hunk_scan_time() -> binary().
first_key_for_live_hunk_scan_time() ->
    sext:encode(?LIVE_HUNK_SCAN_TIME_FIRST_KEY).

-spec last_key_for_live_hunk_scan_time() -> binary().
last_key_for_live_hunk_scan_time() ->
    sext:encode(?LIVE_HUNK_SCAN_TIME_LAST_KEY).


%% ====================================================================
%% Internal functions - Registory DB (LevelDB)
%% ====================================================================

-spec registory_dir() -> dirname().
registory_dir() ->
    %% @TODO: Get the data_dir from #state{}.
    {ok, FileDir} = application:get_env(gdss_brick, brick_default_data_dir),
    filename:join([FileDir, "blob_hlog_registory"]).

-spec open_registory_db() -> ok | {error, term()}.
open_registory_db() ->
    RegDBDir = registory_dir(),
    _ = (catch filelib:ensure_dir(RegDBDir)),
    _ = (catch file:make_dir(RegDBDir)),

    %% @TODO Create a function to return the registory DB path.
    RegDBPath = filename:join(RegDBDir, "leveldb"),

    _RepairResult = repair_registory_db(),
    ?ELOG_DEBUG("Called repair_registory_db. result: ~w", [_RepairResult]),
    Options = [{max_open_files, 50}, {filter_policy, {bloom, 10}}], %% @TODO: Tune the numbers
    RegistoryDB = h2leveldb:get_db(RegDBPath, Options),
    ?ELOG_INFO("Opened registory DB: ~s", [RegDBPath]),
    {ok, RegistoryDB}.

-spec repair_registory_db() -> ok | {error, term()}.
repair_registory_db() ->
    RegDBDir = registory_dir(),
    _ = (catch file:ensure_dir(RegDBDir)),
    RegDBPath = filename:join(RegDBDir, "leveldb"),
    h2leveldb:repair_db(RegDBPath).

-spec close_registory_db() -> ok.
close_registory_db() ->
    %% @TODO Create a function to return the registory DB path.
    RegDBPath = filename:join(registory_dir(), "leveldb"),
    try h2leveldb:close_db(RegDBPath) of
        ok ->
            ?ELOG_INFO("Closed blob store registory DB: ~s", [RegDBPath]),
            ok;
        {error, _}=Error ->
            ?ELOG_WARNING("Failed to close blob store registory DB: ~s (Error: ~p)",
                          [RegDBPath, Error]),
            ok
    catch _:_=Error1 ->
            ?ELOG_WARNING("Failed to close blob store registory DB: ~s (Error: ~p)",
                          [RegDBPath, Error1]),
            ok
    end.


%% ====================================================================
%% Internal functions - tests and debug
%% ====================================================================

%% -define(BLOB_HLOG_REG, brick_blob_store_hlog_registory).

%% test1() ->
%%     BrickName1 = perf1_ch1_b1,
%%     BrickName2 = perf1_ch1_b2,

%%     BI1 = #blob_file_info{
%%              brick_name=BrickName1,
%%              seqnum=1,
%%              short_term=true,
%%              byte_size=1000000,
%%              total_hunks=10000,
%%              estimated_live_hunk_ratio=0.28,
%%              score=192.3,
%%              live_hunk_scaned=?TIME:erlang_system_time(seconds),
%%              scrub_scaned=?TIME:erlang_system_time(seconds)
%%             },
%%     BI2 = #blob_file_info{
%%              brick_name=BrickName1,
%%              seqnum=2,
%%              short_term=true,
%%              byte_size=1000000,
%%              total_hunks=10000,
%%              estimated_live_hunk_ratio=0.85,
%%              score=54.8
%%             },
%%     BI3 = #blob_file_info{
%%              brick_name=BrickName2,
%%              seqnum=1,
%%              short_term=true,
%%              byte_size=1000000,
%%              total_hunks=10000,
%%              estimated_live_hunk_ratio=0.54,
%%              score=100.0
%%             },

%%     %% Set in random order
%%     ok = ?BLOB_HLOG_REG:set_blob_file_info(BI2),
%%     ok = ?BLOB_HLOG_REG:set_blob_file_info(BI3),
%%     ok = ?BLOB_HLOG_REG:set_blob_file_info(BI1),

%%     {ok, BI1r} = ?BLOB_HLOG_REG:get_blob_file_info(BrickName1, 1),
%%     {ok, BIs, false} = ?BLOB_HLOG_REG:get_blob_file_info_for_brick(BrickName1, 10),
%%     io:format("Bi1r: ~p~nBIs: ~p~n", [BI1r, BIs]),

%%     {ok, Top1Score} = ?BLOB_HLOG_REG:get_top_scores(1),
%%     {ok, Top2Scores} = ?BLOB_HLOG_REG:get_top_scores(2),
%%     io:format("Top1Score: ~p~nTop2Scores: ~p~n", [Top1Score, Top2Scores]),

%%     {ok, Top2LiveHunkScanTime} = ?BLOB_HLOG_REG:get_live_hunk_scan_time_records(2),
%%     io:format("Top2LiveHunkScanTime: ~p~n", [Top2LiveHunkScanTime]),
%%     ok.
