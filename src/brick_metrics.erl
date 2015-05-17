%%%-------------------------------------------------------------------
%%% Copyright (c) 2014-2015 Hibari developers. All rights reserved.
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
%%% File    : brick_metrics.erl
%%% Purpose : collect and aggregate metrics in brick server
%%%-------------------------------------------------------------------

-module(brick_metrics).

-behaviour(gen_server).

-include("brick.hrl").

%% API
-export([start_link/0,
         notify/1,
         histogram_timed_begin/1,
         histogram_timed_notify/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-record(state, {
          report_interval :: non_neg_integer()
         }).


%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec notify({metric(), integer()}) -> ok.
notify({Metric, Time}) ->
    folsom_metrics:notify({Metric, Time}).

-spec histogram_timed_begin(metric()) -> timed_begin().
histogram_timed_begin(Metric) ->
    folsom_metrics:histogram_timed_begin(Metric).

-spec histogram_timed_notify(timed_begin()) -> ok.
histogram_timed_notify(Begin) ->
    folsom_metrics:histogram_timed_notify(Begin).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits so we have a chance to flush data
    process_flag(trap_exit, true),
    process_flag(priority, high),

    %% Spin up folsom
    folsom:start(),

    %% @TODO: Add cache hit ratio for read_priming
    Metrics = [%% do_ok_latencies,
               %% do_ok_length,
               %% do_error_latencies,
               %% do_error_length,
               read_priming_latencies,
               logging_op_latencies,
               wal_sync_latencies,
               wal_sync_requests
              ],

    %% Setup a histogram and counter for each operation -- we only track latencies on
    %% successful operations
    [ folsom_metrics:new_histogram(Metric, exdec) || Metric <- Metrics ],

    %% Schedule next write/reset of data
    ReportInterval = 60000,
    timer:send_interval(ReportInterval, report),

    {ok, #state{report_interval=ReportInterval}}.

handle_call(_, _From, State) ->
    {reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(report, State) ->
    consume_report_msgs(),
    process_stats(),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

process_stats() ->
    %% {DoOkMedian, DoOkP95} = get_statistics(do_ok_latencies, true),
    %% {DoOkLenMedian, DoOkLenP95} = get_statistics(do_ok_length, false),
    %% {DoErrMedian, DoErrP95} = get_statistics(do_error_latencies, true),
    %% {DoErrLenMedian, DoErrLenP95} = get_statistics(do_error_length, false),
    {ReadPrimingMedian, ReadPrimingP95} = get_statistics(read_priming_latencies, true),
    {LoggingOpMedian, LoggingOpP95} = get_statistics(logging_op_latencies, true),
    {WalSyncMedian, WalSyncP95} = get_statistics(wal_sync_latencies, true),
    {WalSyncReqsMedian, WalSyncReqsP95} = get_statistics(wal_sync_requests, false),

    ?E_INFO("statistics report~n"
            %% "\tdo ok - ~w, ~w, len ~w, ~w~n"
            %% "\tdo error - ~w, ~w, len ~w, ~w~n"
            "\t(read)  read prminig  median: ~w ms, 95 percentile: ~w ms~n"
            "\t(write) logging wait  median: ~w ms, 95 percentile: ~w ms~n"
            "\t(write) wal sync      median: ~w ms, 95 percentile: ~w ms, reqs ~w, ~w~n",
            [
             %% DoOkMedian, DoOkP95, DoOkLenMedian, DoOkLenP95,
             %% DoErrMedian, DoErrP95, DoErrLenMedian, DoErrLenP95,
             ReadPrimingMedian, ReadPrimingP95,
             LoggingOpMedian, LoggingOpP95,
             WalSyncMedian, WalSyncP95, WalSyncReqsMedian, WalSyncReqsP95
            ]).

%% unit: ms
-spec get_statistics(metric(), IsTime::boolean())
                    -> {Median::number(), Percentile95::number()}.
get_statistics(Metric, false) ->
    Stats = folsom_metrics:get_histogram_statistics(Metric),
    Median = proplists:get_value(median, Stats),
    Percentiles = proplists:get_value(percentile, Stats),
    Percentile95 = proplists:get_value(95, Percentiles),
    {Median, Percentile95};
get_statistics(Metric, true) ->
    {Median, Percentile95} = get_statistics(Metric, false),
    {Median / 1000.0, Percentile95 / 1000.0}.


 %% {min,28508},
 %% {max,1103126},
 %% {arithmetic_mean,104489.85811467444},
 %% {geometric_mean,95361.42234038128},
 %% {harmonic_mean,88537.04909688153},
 %% {median,94839},
 %% {variance,3763084064.802805},
 %% {standard_deviation,61343.98148802215},
 %% {skewness,7.653485086477976},
 %% {kurtosis,105.25044875678809},
 %% {percentile,[{50,94839},
 %%              {75,117130},
 %%              {90,143626},
 %%              {95,203653},
 %%              {99,286695},
 %%              {999,964040}]},

consume_report_msgs() ->
    receive
        report ->
            consume_report_msgs()
    after 0 ->
            ok
    end.
