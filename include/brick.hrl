%%%----------------------------------------------------------------------
%%% Copyright (c) 2007-2013 Hibari developers.  All rights reserved.
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
%%% File    : brick.hrl
%%% Purpose : ETS brick common stuff
%%%----------------------------------------------------------------------

-ifndef(ets_brick_hrl).
-define(ets_brick_hrl, true).

-include("gmt_hlog.hrl").

-record(log_q, {
          time,                                     % erlang:time() QQQ debugging only
          logging_serial = 0,                       % logging_op_serial
          thisdo_mods = [],                         % ops for this do
          doflags,                                  % DoFlags for this do
          from,                                     % gen_server:reply() to
          reply                                     % gen_server:reply() info
         }).

-record(dirty_q, {
          from,                                     % gen_server:reply info
          do_op                                     % {do, SentAt, DoList, DoFlags}
         }).

-record(scav, {
          options = [],                             % prop_list()
          work_dir :: string(),                     % Working files dir
          wal_mod :: module(),
          skip_live_percentage_greater_than = 0 :: non_neg_integer(), % skip threshold
          name :: brickname(),
          log :: pid(),                             % hlog server
          log_dir :: string(),                      % hlog dir path
          cur_seq :: seqnum(),                      % current sequence #
          last_check_seq :: seqnum(),               % last checkpoint seq#
          throttle_bytes = 0:: non_neg_integer(),   % throttle byte rate
          throttle_pid :: pid(),                    % private throttle server
          sorter_size = 0 :: non_neg_integer(),     % bytes
          bricks = [] :: [brickname()],             % CommonLog bricks
          dead_paths = [] :: [string()],            % paths to dead sequences
          dead_seq_bytes = 0 :: non_neg_integer(),
          live_seq_bytes = 0 :: non_neg_integer(),
          exclusive_p = true :: boolean(),          % avoid parallel runs
          bottom_fun :: fun()                       % scavenger botton half implementation
         }).
-type scav_r() :: #scav{}.

%% Write-ahead log hunk types
-define(LOGTYPE_METADATA,     1).
-define(LOGTYPE_BLOB,         2).
-define(LOGTYPE_BAD_SEQUENCE, 3).

%%
%% gmt_elog stuff
%%

-include("gmt_elog.hrl").

%% Any component
-define(CAT_GENERAL,              (1 bsl  0)). % General: init, terminate, ...
%% brick_ets, mostly, except where there's cross-over purpose.
-define(CAT_OP,                   (1 bsl  1)). % Op processing
-define(CAT_ETS,                  (1 bsl  2)). % ETS table
-define(CAT_TLOG,                 (1 bsl  3)). % Txn log
-define(CAT_REPAIR,               (1 bsl  4)). % Repair
%% brick_server, mostly, except where there's cross-over purpose.
-define(CAT_CHAIN,                (1 bsl  5)). % Chain-related
-define(CAT_MIGRATE,              (1 bsl  6)). % Migration-related
-define(CAT_HASH,                 (1 bsl  7)). % Hash-related

-define(E_EMERGENCY(Fmt, Args),
        ?ELOG_EMERGENCY(?CAT_GENERAL, Fmt, Args)).
-define(E_ALERT(Fmt, Args),
        ?ELOG_ALERT(?CAT_GENERAL, Fmt, Args)).
-define(E_CRITICAL(Fmt, Args),
        ?ELOG_CRITICAL(?CAT_GENERAL, Fmt, Args)).
-define(E_ERROR(Fmt, Args),
        ?ELOG_ERROR(?CAT_GENERAL, Fmt, Args)).
-define(E_WARNING(Fmt, Args),
        ?ELOG_WARNING(?CAT_GENERAL, Fmt, Args)).
-define(E_NOTICE(Fmt, Args),
        ?ELOG_NOTICE(?CAT_GENERAL, Fmt, Args)).
-define(E_INFO(Fmt, Args),
        ?ELOG_INFO(?CAT_GENERAL, Fmt, Args)).
-define(E_DBG(Fmt, Args),
        ?ELOG_DEBUG(?CAT_GENERAL, Fmt, Args)).
-define(E_TRACE(Cat, Fmt, Args),
        ?ELOG_TRACE(Cat, Fmt, Args)).

-define(DBG_GEN(Fmt, Args),
        ?ELOG_TRACE(?CAT_GENERAL, Fmt, Args)).
-define(DBG_OP(Fmt, Args),
        ?ELOG_TRACE(?CAT_OP, Fmt, Args)).
-define(DBG_ETS(Fmt, Args),
        ?ELOG_TRACE(?CAT_ETS, Fmt, Args)).
-define(DBG_TLOG(Fmt, Args),
        ?ELOG_TRACE(?CAT_TLOG, Fmt, Args)).
-define(DBG_REPAIR(Fmt, Args),
        ?ELOG_TRACE(?CAT_REPAIR, Fmt, Args)).
-define(DBG_CHAIN(Fmt, Args),
        ?ELOG_TRACE(?CAT_CHAIN, Fmt, Args)).
-define(DBG_CHAIN_TLOG(Fmt, Args),
        ?ELOG_TRACE((?CAT_CHAIN bor ?CAT_TLOG), Fmt, Args)).
-define(DBG_MIGRATE(Fmt, Args),
        ?ELOG_TRACE(?CAT_MIGRATE, Fmt, Args)).
-define(DBG_HASH(Fmt, Args),
        ?ELOG_TRACE(?CAT_HASH, Fmt, Args)).

%% Hold-over from brick_simple:dumblog/1, which only took a single arg.

-define(DBG_GENx(Arg),
        ?ELOG_TRACE(?CAT_GENERAL, "~w", [Arg])).
-define(DBG_OPx(Arg),
        ?ELOG_TRACE(?CAT_OP, "~w", [Arg])).
-define(DBG_ETSx(Arg),
        ?ELOG_TRACE(?CAT_ETS, "~w", [Arg])).
-define(DBG_TLOGx(Arg),
        ?ELOG_TRACE(?CAT_TLOG, "~w", [Arg])).
-define(DBG_REPAIRx(Arg),
        ?ELOG_TRACE(?CAT_REPAIR, "~w", [Arg])).
-define(DBG_CHAINx(Arg),
        ?ELOG_TRACE(?CAT_CHAIN, "~w", [Arg])).
-define(DBG_CHAIN_TLOGx(Arg),
        ?ELOG_TRACE((?CAT_CHAIN bor ?CAT_TLOG), "~w", [Arg])).
-define(DBG_MIGRATEx(Arg),
        ?ELOG_TRACE(?CAT_MIGRATE, "~w", [Arg])).
-define(DBG_HASHx(Arg),
        ?ELOG_TRACE(?CAT_HASH, "~w", [Arg])).

-endif. % -ifndef(ets_brick_hrl).
