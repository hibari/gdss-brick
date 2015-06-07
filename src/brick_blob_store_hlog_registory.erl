%%%-------------------------------------------------------------------
%%% Copyright (c) 2015 Hibari developers. All rights reserved.
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
%% -include("brick_blob_store_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% API
-export([start_link/1,
         stop/0,
         get_blob_file_info_for_brick/2,
         set_blob_file_info/1,
         get_blob_file_info/2,
         delete_blob_file_info/2,
         update_score/3,
         get_top_scores/1
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

%% -type mdstore() :: term().
%% -type blobstore() :: term().
%% -type blobstore_impl_info() :: {module(), pid()}.

-type prop() :: {atom(), term()}.
-type count() :: non_neg_integer().
-type erlang_monotonic_time() :: integer().

-record(state, {
          leveldb :: h2leveldb:db()
         }).
%% -type state() :: #state{}.

-record(blob_file_info, {
          brick_name                    :: brickname(),
          seqnum                        :: seqnum(),
          short_term=true               :: boolean(),
          byte_size                     :: byte_size(),
          total_hunks                   :: non_neg_integer(),
          estimated_live_hunk_ratio=1.0 :: float(),
          score=0.0                     :: float(),
          last_live_hunk_scan           :: undefined | erlang_monotonic_time(),
          last_scrub_scan               :: undefined | erlang_monotonic_time()
         }).
-type blob_file_info() :: #blob_file_info{}.

-record(score, {
          score      :: float(),
          brick_name :: brickname(),
          seqnum     :: seqnum()
         }).
-type score() :: #score{}.

-define(SCORE_FIRST_KEY, {r, 0, 0, 0}).
-define(TIMEOUT, 60 * 1000).
%% -define(METADATA, brick_metadata_store).
%% -define(BLOB,     brick_blob_store).


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
set_blob_file_info(BlobFileInfo) when is_record(BlobFileInfo, blob_file_info) ->
    {ok, DB} = gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, get_leveldb, ?TIMEOUT),
    Mutations = [h2leveldb:make_put(blob_file_info_key(BlobFileInfo), term_to_binary(BlobFileInfo)),
                 h2leveldb:make_put(score_key(BlobFileInfo), <<>>)],
    h2leveldb:write(DB, Mutations).

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
                         h2leveldb:make_delete(score_key(BlobFileInfo))],
            h2leveldb:write(DB, Mutations);
        key_not_exist ->
            ok;
        {error, _}=Err ->
            Err
    end.

-spec update_score(brickname(), seqnum(), float()) -> ok | key_not_exist | {error, term()}.
update_score(BrickName, SeqNum, Score) ->
    {ok, DB} = gen_server:call(?HLOG_REGISTORY_SERVER_REG_NAME, get_leveldb, ?TIMEOUT),
    BIKey = blob_file_info_key(BrickName, SeqNum),
    case h2leveldb:get(DB, BIKey) of
        {ok, Bin} ->
            BlobFileInfo0 = binary_to_term(Bin),
            BlobFileInfo1 = BlobFileInfo0#blob_file_info{score=Score},
            Mutations = [h2leveldb:make_put(BIKey), term_to_binary(BlobFileInfo1),
                         h2leveldb:make_put(score_key(BlobFileInfo1), <<>>)],
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
    sext:encode(?SCORE_FIRST_KEY).  %% 'r' is bigger than 'i' and smaller than 's'.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% get_top_scores1(_, _, MaxScores, Scores) when MaxScores =< 0 ->
%%     {ok, lists:reverse(Scores)};
%% get_top_scores1(_, end_of_table, _, Scores) ->
%%     {ok, lists:reverse(Scores)};
%% get_top_scores1(_, {error, _}=Err, _, _) ->
%%     Err;
%% get_top_scores1(DB, {ok, Key}, MaxScores, Scores) ->
%%     %% We can safely omit case clauses for ?SCORE_FIRST_KEY and {i, _, _}
%%     %% because they should never come from h2leveldb:next_key(DB, ?SCORE_FIRST_KEY).
%%     case sext:decode(Key) of
%%         {s, NegativeScore, BrickName, SeqNum} ->
%%             Score = #score{
%%                        score=-(NegativeScore),
%%                        brick_name=BrickName,
%%                        seqnum=SeqNum
%%                       },
%%             Scores2 = [Score | Scores],
%%             NextKey = h2leveldb:next_key(DB, Key),
%%             get_top_scores1(DB, NextKey, MaxScores - 1, Scores2);
%%         {error, _}=Err ->
%%             Err
%%     end.


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
    RegistoryDB = h2leveldb:get_db(RegDBPath),
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

-define(BLOB_HLOG_REG, brick_blob_store_hlog_registory).

test1() ->
    BrickName1 = perf1_ch1_b1,
    BrickName2 = perf1_ch1_b2,

    BI1 = #blob_file_info{
             brick_name=BrickName1,
             seqnum=1,
             short_term=true,
             byte_size=1000000,
             total_hunks=10000,
             estimated_live_hunk_ratio=0.28,
             score=192.3
             %% last_live_hunk_scan=time_compat:sytem_time(),
             %% last_scrub_scan=time_compat:system_time(),
            },
    BI2 = #blob_file_info{
             brick_name=BrickName1,
             seqnum=2,
             short_term=true,
             byte_size=1000000,
             total_hunks=10000,
             estimated_live_hunk_ratio=0.85,
             score=54.8
             %% last_live_hunk_scan=time_compat:sytem_time(),
             %% last_scrub_scan=time_compat:system_time(),
            },
    BI3 = #blob_file_info{
             brick_name=BrickName2,
             seqnum=1,
             short_term=true,
             byte_size=1000000,
             total_hunks=10000,
             estimated_live_hunk_ratio=0.54,
             score=100.0
             %% last_live_hunk_scan=time_compat:sytem_time(),
             %% last_scrub_scan=time_compat:system_time(),
            },

    %% Set in random order
    ok = ?BLOB_HLOG_REG:set_blob_file_info(BI2),
    ok = ?BLOB_HLOG_REG:set_blob_file_info(BI3),
    ok = ?BLOB_HLOG_REG:set_blob_file_info(BI1),

    {ok, BI1r} = ?BLOB_HLOG_REG:get_blob_file_info(BrickName1, 1),
    {ok, BIs, false} = ?BLOB_HLOG_REG:get_blob_file_info_for_brick(BrickName1, 10),
    io:format("Bi1r: ~p~nBIs: ~p~n", [BI1r, BIs]),

    {ok, Top1Score} = ?BLOB_HLOG_REG:get_top_scores(1),
    {ok, Top2Scores} = ?BLOB_HLOG_REG:get_top_scores(2),
    io:format("Top1Score: ~p~nTop2Scores: ~p~n", [Top1Score, Top2Scores]),
    ok.
