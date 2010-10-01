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
%%% File    : brick_server.erl
%%% Purpose : brick server (implementation-generic portions)
%%%-------------------------------------------------------------------

%% @doc The implementation-generic interface for a storage brick.
%%
%% == Brick Roles ==
%% This module takes care of two different roles that a brick
%% implementation is expected to play:
%% <ul>
%% <li> Stand-alone storage brick, completely ignorant of other bricks
%%   in the world.</li>
%% <li> Participant in a larger cluster of bricks that implement the
%%   "chain replication" technique of state-machine-based replication. </li>
%% </ul>
%%
%% == Brick Implementation Layers ==
%% The matter of implementing brick storage can further subdivided into
%% two areas of code:
%% <ul>
%% <li> An implementation-specific part: how an individual key/value pair
%%      is actually stored: in memory, on disk, ....  This part is
%%      commonly referred to below as the "implementation-dependent"
%%      code and sometimes referred to as the "lower" storage layer.
%%      The name of the Erlang module that implements the lower layer
%%      is stored in #state.impl_mod. </li>
%% <li> An generic part that isn't dependent on local storage methods.
%%      This part is commonly referred to below as the
%%      "implementation-independent" code and is sometimes referred to
%%      as the "upper" storage layer. </li>
%% </ul>
%%
%% === This Layer: Implementation-Independent ===
%%
%% This source module is intended to include all
%% implementation-independent code.  The split between storage-aware
%% and storage-unware code is quite imperfect, unfortunately.  The
%% original code was written with such a split in mind and has
%% required a large refactoring effort to split it apart. TODO: Review
%% by someone other than Hibari is almost certainly a good idea.
%%
%% One of the artifacts of the original implementation is that the
%% same gen_server callbacks (handle_call(), handle_cast(),
%% handle_info()) are used to implement both lower and upper storage
%% layers.  This strategy requires that the upper layer maintain the
%% gen_server calling convention whenever calling a lower layer's
%% callback; this must include all of the lower layer's state, without
%% contamination from the upper layer's state.  At first glance, this
%% seems like a hassle, but it presents a unified gen_server interface
%% to the outside world, and leaves us the flexibility to implement
%% any particular callback at the upper layer, lower, or combination
%% of both.  (Aside: this is something that an Object-Oriented
%% framework would take care of for us as a matter of subclassing, but
%% since Erlang isn't O-O, we had to roll our own solution.)
%%
%% == Usage in a Standalone Role ==
%%
%% Usage in the first role, stand-alone and completely ignorant of any
%% other bricks, is valuable in its own right.  Such a brick can be be
%% used as-is.  It can also be used in part of a cluster where any
%% distribution/cluster activity is handled 100% by the brick clients,
%% e.g. in an environment where data redundancy is implemented by
%% consensus.
%%
%% == Chain Replication ==
%%
%% For the second role, as a participant in a cluster of bricks using
%% chain replication, the first role is still used for cluster
%% bootstrap information.  A small set of bricks, called bootstrap
%% bricks, is used to store basic cluster configuration and
%% operational historic data.
%%
%% Detailed information on the "chain replication" technique can be found at:
%% <ul>
%% <li> [https://inet.geminimobile.com/twiki/pub/Main/StorageBrickSep2007/chain-replication.renesse.pdf]
%%       Published in the proceedings of OSDI 2004:
%%       "Chain Replication for Supporting High Throughput and Availability"
%%       by van Renesse and Schneider. </li>
%% <li> The local Gemini twiki topic:
%%      [https://inet.geminimobile.com/twiki/bin/view/Main/StorageBrickSep2007]
%%      </li>
%% </ul>
%%
%% === Chain Replication Role State Names ===
%%
%% A brick's chain role status is denoted by one of the following names:
%%
%% <ul>
%% <li> undefined ... The role is unknown.  Nobody should be querying
%%      this brick. </li>
%% <li> standalone ... The brick is operating in a chain of length 1.  </li>
%% <li> head ... The brick is operating in a chain of at least length 2,
%%      and it is the first in the chain. </li>
%% <li> middle ... The brick is operating in a chain of at least length 2,
%%      and it is one of the middle bricks in the chain. </li>
%% <li> tail ... The brick is operating in a chain of at least length 2,
%%      and it is the last in the chain. </li>
%% </ul>
%%
%% In addition, there are some sub-properties that can also be assigned
%% to a brick:
%% <ul>
%% <li> official_tail ... This brick plays the role of the tail of the
%%      chain, even though there may be a brick downstream.
%%      In the case of an official_tail brick, the downstream brick must
%%      be in the middle of the repair process and thus not an official
%%      member of the chain.  Therefore the official_tail brick is the
%%      chain member to handle all update request and to send all replies
%%      to clients. </li>
%% </ul>
%%
%% === Chain Replication Repair State Names ===
%%
%% A brick's chain repair status is denoted by one of following names:
%% <ul>
%% <li> disk_error ... A disk error happened while reading the write-ahead
%%      log. </li>
%% <li> pre_init ... The initial state.  So named to avoid atom confusion
%%      with 'init'.  The overall quality of the brick's data is unknown. </li>
%% <li> {last_key, Key} ... The brick is in the middle of the
%%      repair process.  Key is where the repair key sweep is currently
%%      located. </li>
%% <li> ok ... The brick's data is 100% in sync with other members of
%%      the chain. </li>
%% </ul>
%%
%% During a brick's lifecycle, the order of the states is the same as
%% the order presented above.
%%
%% == Chain Administration ==
%%
%% Administration of the cluster of chains is performed by a separate
%% administration application.  That application is broadly responsible for:
%% <ul>
%% <li> Defining membership in the cluster.  </li>
%% <li> Monitoring the state of cluster members and, when necessary,
%%      sending cluster state updates to the rest of the cluster. </li>
%% <li> Maintaining operational/historical data (some of it necessary for
%%      data integrity in failure cases). </li>
%% <li> Presenting interfaces via Erlang shell, HTTP, and others for
%%      human administrator intervention. </li>
%% </ul>
%%
%% The health monitoring component of the admin application is
%% critical: when a chain member fails, the chain is partially or
%% completely disfunctional until it can be reconfigured.
%%
%% == Brick Repair via Chain Replication ==
%%
%% This is a sketch of the brick repair procedure.  The actual
%% implementation is in {@link brick_chainmon:add_repair_brick_to_end/2}.
%% <ul>
%% <li> Assume a chain C with members B1 and B2.  Assume B3 is the
%%      brick to be added to chain C. </li>
%% <li> Brick B3 is started.  Its repair status is pre_init, and its chain
%%      role is set to tail + {official_tail, false}. </li>
%% <li> Brick B3 is added to the end of the chain.   </li>
%% <li> Brick B2's chain role is set to middle + {official_tail, true}.  </li>
%% <li> Brick B2 is instructed to start repair.   </li>
%% <li> Loop: B2 starts scanning its list of keys and sends the first list
%%      of them to B3. </li>
%%   <ul>
%%   <li> See {@link chain_start_repair/1} for initiation.  The current
%%        repair strategy is called 'replace'.  Call a new diff-based
%%        strategy 'repair_diff'?  Use #repair_r.repair_method to select
%%        implementation func (not really used like that now). </li>
%%   <li> Diff method: 1 round of repair is done in 2 volleys: first
%%        volley sends {Key, TS} (like a witness tuple) downstream,
%%        repairee responds with list of keys that it needs updates for.
%%        Second volley sends two things: the new repair list with
%%        timestamps only (i.e. witness only) and a list of full storetuples
%%        (including value term, expiration, flags) for only the
%%        keys requested in the response to volley one.  The repairee
%%        then applies the diff.  Beware of the case where a key is
%%        deleted between rounds 1 and 2 and where a key has been updated
%%        between the two rounds. </li>
%%   <li> See {@link send_next_repair/1}  </li>
%%   </ul>
%% <li> B3 applies the data received from B2 to its local store and sends an
%%      ack back to B2.  Repeat loop. </li>
%%   <ul>
%%   <li> See {@link chain_do_repair/3} and
%%        {@link brick_server:bcb_repair_loop/3}. </li>
%%   </ul>
%% <li> When B2 has finished its scan, it sends ch_repair_finished message
%%      to B3.  B3 changes its repair status to 'ok'. </li>
%% <li> When the chain's monitor has noticed that B3's repair status is 'ok',
%%      it reconfigures the chain so that B2 = middle and
%%      B3 = tail + official_tail. </li>
%% </ul>
%% Here's the sketch in MSC sequence form,
%% <em>do not interpret as definitive, be-all, end-all documentation</em>
%% <p><img src="mscgen/repair.png"></img></p>
%%
%% == Chain Health Status ==
%%
%% Inside the administrative application, each chain has a "chain
%% monitor" process.  This process is responsible for monitoring the
%% overall health of the chain and of each member brick.  The possible
%% status names of the chain's health are: are:
%%
%% <ul>
%% <li> unknown ... The chain monitor has just started and has not yet
%%      queried the state of each brick, or one or more of the bricks
%%      is down and cannot be queried. </li>
%% <li> stopped ... All bricks in the chain are down.   </li>
%% <li> healthy ... All bricks in the chain are up and have
%%      repair status of 'ok'. </li>
%% <li> degraded ... At least one of the member bricks is down or is
%%      up but undergoing repair. </li>
%% </ul>
%%
%% == Migration Rules, Notes, etc. ==
%%
%% There are two phases to a local migration sweep iteration
%% (*NOT* to be confused with a brick's #g_hash_r.phase = (pre|migrating)
%%  state!!):
%% <ul>
%% <li> Setup: Given the current sweep key, K0, scan the local table to find
%%             the new sweep key, K1 (the code calls it LastKey).
%%             As an optimization, this calculation is not done in a
%%             single/central place in the code.  If the chain is of
%%             length=1, then phase 1 of this algorithm is not necessary.
%%             Due to the differences of doing a key scan with or without
%%             each key's value term (with val term = more expensive),
%%             chains with length > 1 will perform this scan without
%%             value terms. </li>
%% <li> Phase 1: For chains of length > 1, send the K1/LastKey value
%%               down the chain.  This is necessary to preserve strong
%%               consistency: the chain's tail must know the current
%%               sweep iteration's K1/LastKey value to avoid directly
%%               answering queries regarding keys immediately less than
%%               K1/LastKey.  This info is written synchronously to disk
%%               by each brick in the chain: we don't know how the chain
%%               will be reconfigured if/after chain members fail. </li>
%% <li> Phase 2: When the ack from the tail for Phase 1 is received by the
%%               head, Phase 2 begins.  For each key between the last
%%               iteration's last key and the current LastKey, use the
%%               global hash to figure out which keys remain in the local
%%               chain, C_l, and which ones must migrate to a new chain.
%%               For each remote chain that requires notification, a list of
%%               new keys is sent along with the LastKey term.  Each remote
%%               chain, C_r, stores this LastKey term so it can make local
%%               decisions about whether queries should be processed by
%%               itself (C_r) or forwarded back to C_l. </li>
%% </ul>
%%
%% Important rule: The local brick's global hash <em>must</em> be
%% updated with the current sweep iteration's LastKey before calling
%% starting the 2nd phase of the local migration sweep computation!
%%
%% In a local sweep iteration, the local chain's global hash is
%% updated at the beginning of Phase 1 with the iteration's new
%% K1/LastKey of the sweep iteration.  Each chain member does the
%% same.
%%
%% A result of this early global hash updated is that any operation on
%% a key K on a local chain C_l where last iter's K1 &lt; K &lt; this
%% iter's K1 will be forwarded to a remote chain C_r.  However, we
%% don't know if C_r has received this iter's sweep update yet: in
%% phase 1, this iter's updates haven't been sent to remote chains
%% yet!  So we might see a bit of query ping-pong, being forwarded
%% from C_l to C_r then back to C_l (and perhaps again!).
%%
%% This ping-pong can only affect a small number of keys, only one
%% iteration's worth.  And without brick failures, all updates will
%% propagate with sub-second speed.  This extra latency seems a small
%% price to pay for much easier logic and code while maintaining
%% strong consistency guarantees.
%%
%% TODO: fix the bug described below.
%% The "get_many" operation is the one that's complicated by a
%% migration sweep.  It's possible that the starting key for a
%% get_many operation (with a max of N keys), K_0, is outside of a
%% sweep iteration, but key K_N (and perhaps other keys K_n-1, K_n-2,
%% ...) <em>would be affected</em> by the current iteration.
%%
%% == A Note About Data/Transaction Logging and Serial Numbers ==
%%
%% I have now (I hope) finished doing what I should have done in the
%% initial implementation: all bricks will use the same logging serial
%% number for the same update.
%%
%% The logging serial number is set by the head brick via
%% #ImplementationStateRecord.logging_op_serial.  All middle and tail
%% bricks will use the same serial number given to them by their
%% upstream neighbor.
%%
%% The wrinkle in this is a migration sweep, which uses a "pseudo log"
%% modification type, a tuple, instead a thisdo_mods(), which is a
%% list.
%%
%% There was a hellacious-to-find-bug that involved race conditions
%% with sweep phase 1 messages (which typically aren't sync'ed to
%% disk) and sweep post-phase-2 messages (which involves deleting keys
%% and thus are sync'ed to disk) causing all sorts of out-of-order
%% serial numbers.  Both on a head node and on middle nodes.  {sigh} I
%% hope I've got all of them ironed out now.

%% <ul>
%% <li>    </li>
%% <li>    </li>
%% </ul>

-module(brick_server).
-include("applog.hrl").


-behaviour(gen_server).

-include("brick.hrl").
-include("brick_public.hrl").
-include("brick_hash.hrl").
-include("gmt_hlog.hrl"). % BZDEBUG

%%-define(gmt_debug, true).                     %QQQXXXYYYZZZ
-ifdef(debug_server).
-define(gmt_debug, true).
-endif.
-include("gmt_debug.hrl").
-include("brick_specs.hrl").

-include_lib("kernel/include/file.hrl").

-define(CHECK_NAME, "CHECK").
-define(CHECK_FILE, ?CHECK_NAME ++ ".LOG").

%% For bigdata_dir storage, missing keys will be slurped all at once,
%% and since we don't have smarts to limit the total size of the
%% slurp, we'll limit the max number of keys instead.  For
%% non-bigdata_dir, we'll just make a lot more round trips.
-define(REPAIR_MAX_KEYS, 100).
%%%%%%%%%%%%%%% -define(REPAIR_MAX_KEYS, 2).

%% Limit the total number of bytes of data that we can send in a
%% single repair volley.
-define(REPAIR_MAX_BYTES, (65*1000*1000)).

%% Limit the total number of parallel value blob primer processes for
%% a single repair volley.
-define(REPAIR_MAX_PRIMERS, 7).

-define(FOO_TIMEOUT, 5000).
-define(ADMIN_PERIODIC, 1000).                  % Longer than 1 sec not good.
-define(DO_OP_TOO_OLD, 1500).                   % Msec that's too old (1.5sec)
-define(NEG_LIMBO_SEQNUM, -4242).               % Role change or repair done.

%% External exports
-export([start_link/2, stop/1, dump_state/1]).
-export([add/4, add/5, add/7, replace/4, replace/5, replace/7,
         set/4, set/5, set/7, get/3, get/4, get/5,
         delete/3, delete/4, get_many/4, get_many/5, get_many/6, get_many/7,
         do/3, do/4, do/5]).

%% Quota management
-export([get_quota/3, set_quota/5, resum_quota/3]).

-export([make_add/2, make_add/4, make_add/5,
         make_set/2, make_set/4, make_set/5,
         make_replace/2, make_replace/4, make_replace/5,
         make_get/1, make_get/2,
         make_delete/1, make_delete/2,
         make_txn/0,
         make_next_op_is_silent/0,
         make_get_many/2, make_get_many/3,
         make_ssf/2,
         make_get_quota/1, make_set_quota/3, make_resum_quota/1]).
-export([make_op2/3, make_op5/5, make_op6/6]).  % Use wisely, Grasshopper.
-export([make_timestamp/0, make_now/1, get_op_ts/1]).   % Use wisely, Grasshopper.

%% Server-side fun helper functions
-export([ssf_peek/3, ssf_peek_many/4, ssf_check_quota_root/2,
         ssf_impl_details/1]).
%% Server-side preprocess functions (export for "fun Mod:Name/Arity" usage)
-export([quotas_preprocess/3, ssf_preprocess/3]).

%% Chain admin & related API
-export([chain_role_standalone/2, chain_role_undefined/2,
         chain_set_my_repair_state/3, chain_set_ds_repair_state/3,
         chain_role_head/5, chain_role_tail/5, chain_role_middle/7,
         chain_get_role/2,
         chain_get_my_repair_state/2, chain_get_my_repair_state/3,
         chain_get_ds_repair_state/2,
         chain_start_repair/2,
         chain_hack_set_global_hash/3, chain_hack_set_global_hash/4,
         chain_get_global_hash/2,
         chain_set_read_only_mode/3, chain_get_downstream_serials/2,
         chain_flush_log_downstream/2, chain_add_to_proplist/3]).
-export([extract_do_list_keys_find_bricks/2, harvest_do_keys/1]).
-export([throttle_tab_name/1,
         throttle_brick/2, unthrottle_brick/1, set_repair_overload_key/1]).

%% Migration admin API
-export([migration_start_sweep/5, migration_start_sweep/6,
         migration_clear_sweep/2, migration_clear_sweep/3]).

%% Admin task API
-export([status/2, status/3, state/2, flush_all/2, checkpoint/2, checkpoint/3,
         checkpoint_bricks/1, checkpoint_bricks_and_wait/1,
         sync_down_the_chain/3, sync_down_the_chain/4,
         set_do_sync/2, set_do_logging/2, start_scavenger/3]).

%% Other utility functions
-export([get_tabprop_via_str/2, make_expiry_regname/1,
         replace_file_sync/2, replace_file_sync/3]).

%% Debugging only helper functions
-export([md_test_get/3, md_test_set/4, md_test_delete/3]).

%% For gmt_hlog_common use.
-export([common_log_sequence_file_is_bad/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Exports to stop dialyzer compliants due to limited internal usage of functions
-export([chain_start_repair/1]).

%%%----------------------------------------------------------------------
%%% Types/Specs/Records
%%%----------------------------------------------------------------------

-export_type([brick/0
              , brick_name/0
              , chain_name/0
              , delete_reply/0
              , flags_list_many/0
              , get_reply/0
              , global_hash_r/0
              , node_name/0
              , prop_list/0
              , repair_state_name/0
              , repair_state_name_base/0
              , role/0
              , set_reply/0
             ]).

%% depending on impl module %%%%%%%%%%%%%%%%%%%%%%%%%%
-type impl_tuple() :: brick_ets:extern_tuple().
-type impl_state_r() :: brick_ets:state_r().
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type orddict() :: list().

-type role() :: undefined | standalone | chain_member.
-type brick() :: {brick_name(), node_name()}.
-type brick_name() :: atom().
-type chain_name() :: atom().
-type do_list() :: list(do1_op() | {ssf, binary(), flags_list()}).
-type do_op() :: {do, brick_bp:nowtime(), do_list(), flags_list()}.
-type global_hash_r() :: #g_hash_r{}.
-type node_name() :: atom().
-type prop_list() :: list(atom() | {atom(), term()}).
-type rw_key_tuple() :: {read | write, term()}.
-type serial_number() :: no_serial | repair_overload | integer().
-type gen_server_handle_call_reply_tuple() :: tuple().
%% base is used by brick_bp who only uses a subset.
-type repair_state_name_base() :: pre_init | force_ok_to_pre_init | repair_overload | disk_error | ok | unknown.
-type repair_state_name() :: repair_state_name_base() | {last_key, term()}.
-type do_op_flag2() :: do_op_flag() | get_many_raw_storetuples.
-type flags_list_many() :: list(do_op_flag2()).
-type flags_or_fun_list() :: list(do_op_flag2() | function()).

-type do_reply() :: list() | {txn_fail, list()} | {wrong_brick, term()}.
-type syncdown_reply() :: list({state_r(), node_name()}).
-type add_reply() :: {key_exists, integer()} | {ts_error, ts()} | ok.
-type set_reply() :: {ts_error, ts()} | ok.
-type replace_reply() :: {ts_error, ts()} | key_not_exist | ok.
-type quota_reply() :: {ts_error, ts()} | ok.
-type get_reply() :: {ts_error, ts()} | key_not_exist |
                     {ok, ts()} | {ok, ts(), flags_list()} | {ok, ts(), val()} |
                     {ok, ts(), val(), exp_time(), flags_list()}.
-type delete_reply() :: {ts_error, ts()} | key_not_exist | ok.
-type getmany_reply() :: {list(impl_tuple()), boolean()}.
-type status_reply() :: {ok, prop_list()}.
-type state_reply() :: impl_state_r().
-type flushall_reply() :: sorry | ok.
-type checkpoint_reply() :: sorry | ok.
-type rolestandalone_reply() :: {ok, state_r()} | {error, term(), term()}.
-type roleundefined_reply() :: {ok, state_r()}.
-type rolehead_reply() :: ok | {error, term(), term()}.
-type roletail_reply() :: ok | {error, term(), term()}.
-type rolemiddle_reply() :: ok | {error, term(), term()}.
-type getrole_reply() :: undefined | standalone | {head, brick()} | {middle, brick(), brick()} | {tail, brick()}.
-type setmyrepair_reply() :: ok | {error, term(), term()}.

-type serial() :: integer().
-type sweep_key() :: key() | ?BRICK__GET_MANY_LAST | ?BRICK__GET_MANY_FIRST.
-type stage() :: top
               | priming_value_blobs
               | {notify_down_old, brick_bp:nowtime()}
               | {collecting_phase2_replies, brick_bp:nowtime(), list()}  %% list is move_dict_list()
               | {done, brick_bp:nowtime(), brick_bp:proplist()}.

%% Repair state machine used by an upstream/ok to a downstream/repairing brick.
-record(repair_r,
                { started                         :: brick_bp:nowtime()             % now()
                , started_serial                  :: serial()                       % integer()
                , repair_method = replace         :: replace | repair_diff          % repair_diff | replace
                , max_keys                        :: non_neg_integer()              % Max keys to send at once
                , max_bytes                       :: non_neg_integer()              % Max bytes to send at once
                , key = ?BRICK__GET_MANY_FIRST    :: key()                          % Key to resume repair
                                                   | ?BRICK__GET_MANY_LAST
                                                   | ?BRICK__GET_MANY_FIRST
                , last_repair_serial              :: serial()                       % Serial # of last sent
                , final_repair_serial             :: serial()                       % Serial # of final repair sent
                , values_sent = 0                 :: non_neg_integer()
                , repair_deletes = 0              :: non_neg_integer()
                , repair_inserts = 0              :: non_neg_integer()
                , last_ack                        :: brick_bp:nowtime()             % now()
        }).

%% Chain state
-record(chain_r,
               { role = undefined            :: role()
               , proplist = []               :: brick_bp:proplist()            % General init options
               , upstream = undefined        :: brick() | undefined            % {BrickName, Node}
               , downstream = undefined      :: brick() | undefined            % {BrickName, Node}
               , official_tail = false       :: boolean()                      % Admin says we're tail, thus
                                                                               % authorized to send replies.
               , my_repair_state = unknown   :: repair_state_name()            % unknown | pre_init |
                                                                               % {last_key, K} | ok
               , ds_repair_state = pre_init  :: pre_init | ok | #repair_r{}    % pre_init | repair_r() | ok
                                                                               % State of downstream repair.
               , my_repair_ok_time = {0,0,0} :: brick_bp:nowtime() | undefined % Time of repair ok transition
               , up_serial = 0               :: integer()                      % Last serial number received
                                                                               % received from upstream.
               , repair_last_time = {0,0,0}  :: brick_bp:nowtime()             % Time of last repair
               , down_serial = 0             :: serial()                       % Serial # for items sent
                                                                               % downstream
               , down_acked = 0              :: integer()                      % Serial # for last ack'ed item
                                                                               % by downstream.
               , down_unacked                :: queue()                        % queue() of un-acked
                                                                               %   ch_log_replay messages
               , last_ack = undefined        :: brick_bp:nowtime() | undefined % now() time of last ack by
                                                                               % downstream.
               , unknown_acks = []                       :: orddict()              % orddict() (init cheating!)
               %% The clock_tref timer is used for:
               %% * For a downstream brick, to send chain_admin_periodic msgs.
               , clock_tref                  :: reference()                    % Timer ref
               %% Note: read_only_p does not affect update propagation.  It
               %% only affects the client API such as 'do'.
               %%
               %% I'm not 100% happy with my choice of making the storage
               %% level aware of read-only mode.  However, I think having
               %% the lower storage level aware of read-only mode is
               %% valuable: since it is the level that calculates the list
               %% of Thisdo_Mods for a do list, and a txn is read-only iff
               %% Thisdo_Mods == [] ... I think the current compromise, use
               %% a special up1_* tag to punt to the upper levels, is ok
               %% enough.
               , read_only_p = false         :: boolean()                      % true | false
               , read_only_waiters = []      :: list(#dirty_q{})               % list(#dirty_q)
               %% In the case that this brick restarts while a migration is
               %% in progress, we need both role info (from brick_admin)
               %% and migration sweep start (from brick_migmon).  Those two
               %% are decoupled, so there's a sync problem.  Use this state
               %% data to solve the problem.
               , waiting_for_mig_start_p = true    :: boolean()               % bool()
               , mig_start_waiters = []            :: list(#dirty_q{})        % list(#dirty_q)
               , max_primers = ?REPAIR_MAX_PRIMERS :: non_neg_integer()       % Max parallel primer procs
               , upstream_monitor = undefined      :: undefined | reference()
               , upstream_monitor_down_p = false   :: boolean()
         }).

-type chain_r() :: #chain_r{}.

%% Migration sweep state (most fields used by chain head only)
-record(sweep_r,
        { cookie                             :: term()                        % term()
        , started                            :: brick_bp:nowtime()            % now()
        , options                            :: brick_bp:proplist()           % proplist()
        , bigdata_dir_p                      :: boolean()                     % boolean()
        , tref                               :: reference()                   % Timer ref
        , max_keys_per_iter                  :: integer()                     % integer()
        , max_keys_per_chain                 :: integer()                     % integer()
        , prop_delay                         :: integer()                     % integer()
        , chain_name                         :: chain_name()                  % atom()
        %% done_sweep_key is the key we've confirmed have all been migrated.
        %% K where done_sweep_key < K <= sweep_key are keys in progress.
        , done_sweep_key                     :: sweep_key()                   % term()
        , sweep_key                          :: sweep_key()                   % term()
        , stage                              :: stage()
        , phase2_replies                     :: list()                        % list(ChainName)
        , num_phase2_replies                 :: integer()                     % integer()
        , phase2_needed_replies              :: integer()                     % integer()
        , phase2_del_thisdo_mods             :: list()                        % list(Thisdo_Mods)
        , val_prime_lastkey                  :: bin_key() | ?BRICK__GET_MANY_LAST % binary() | '$end_of_table'?
        }).

-record(state,
             { name                     :: brick_bp:brick_name()     % My registered name
             , options                  :: brick_bp:proplist()
             , impl_mod                 :: atom()                    % implementation module name
             , impl_state               :: tuple()                   % state record of implementation module
             %% Chain replication stuff.
             , chainstate               :: #chain_r{}                % Chain state
             , logging_op_q             :: queue()                   % Queue of logged ops for
                                                                     % replay when log sync is done.
             , globalhash = undefined   :: #g_hash_r{} | undefined   % Global hash state
             , sweepstate               :: #sweep_r{} | undefined    % Migration sweep state
             , do_op_too_old_usec       :: integer()                 % do op too old (microseconds)
             , n_too_old = 0            :: integer()
             , bad_pid                  :: pid()                     % An intentionally bogus pid
             , do_list_preprocess = []  :: [] | [fun()]              % Preprocessing fun for do list
             , pingee_pid               :: pid()                     % pid()
             , retry_send_repair        :: reference() | undefined
             , bz_debug_check_hunk_summ :: boolean()                 % BZDEBUG
             , bz_debug_check_hunk_blob :: boolean()                 % BZDEBUG
             , throttle_tab             :: ets:tab()                 % ETS table
             }).

-type state_r() :: #state{}.

%% Persistent brick metadata: sweep checkpoint info.

-record(sweepcheckp_r,
           { cookie                     :: term()
           , chain_name                 :: chain_name()
           , done_sweep_key             :: sweep_key()
           , sweep_key                  :: sweep_key()
           }).

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------

%% @spec (brick_name(), prop_list()) -> ok | ignore | {error, Reason}
%% @doc Start a brick server on the local node.

-spec start_link(brick_name(), prop_list())
                ->ignore | {ok, pid()} | {error, term()}.
start_link(ServerName, Options) ->
    gen_server:start_link(?MODULE, [ServerName, Options], []).

%% @spec (server_ref()) -> term()
%% @doc Stop a brick server (local or remote).

stop(Server) ->
    gen_server:call(Server, {stop}).

%% @spec (server_ref()) -> term()
%% @doc Ask a brick to return a raw dump of its gen_server #state record,
%%      plus some extra stuff (maybe).

dump_state(Server) ->
    gen_server:call(Server, {dump_state}).

%% @spec (brick_name(), node_name(), io_list(), io_list())
%%    -> zzz_add_reply()
%% @equiv add(ServerName, Node, Key, Value, 0, [], DefaultTimeout)
%% @doc Add a Key/Value pair to a brick, failing if Key already exists.

-spec add(brick_name(), node_name(), key(), val()) -> add_reply().
add(ServerName, Node, Key, Value) ->
    add(ServerName, Node, Key, Value, 0, [], foo_timeout()).

%% @spec (brick_name(), node_name(), io_list(), io_list(), prop_list() | timeout())
%%    -> zzz_add_reply()
%% @equiv add(ServerName, Node, Key, Value, 0, Flags, DefaultTimeoutOrFlags)
%% @doc Add a Key/Value pair to a brick, failing if Key already exists.

-spec add(brick_name(), node_name(), key(), val(), flags_list0() | non_neg_integer()) -> add_reply().
add(ServerName, Node, Key, Value, Flags) when is_list(Flags) ->
    add(ServerName, Node, Key, Value, 0, Flags, foo_timeout());
add(ServerName, Node, Key, Value, Timeout) when is_integer(Timeout) ->
    add(ServerName, Node, Key, Value, 0, [], Timeout).

%% @spec (brick_name(), node_name(), io_list(), io_list(), integer(), prop_list(), timeout())
%%    -> zzz_add_reply()
%% @doc Add a Key/Value pair to a brick, failing if Key already exists.

-spec add(brick_name(), node_name(), key(), val(), integer(), flags_list(), timeout()) -> add_reply().
add(ServerName, Node, Key, Value, ExpTime, Flags, Timeout)
  when not is_list(Node) ->
    case do(ServerName, Node, [make_add(Key, Value, ExpTime, Flags)],
            Timeout) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name(), io_list(), io_list())
%%    -> zzz_add_reply()
%% @equiv replace(ServerName, Node, Key, Value, 0, [], DefaultTimeout)
%% @doc Replace a Key/Value pair in a brick, failing if Key does not already exist.

-spec replace(brick_name(), node_name(), key(), val()) -> replace_reply().
replace(ServerName, Node, Key, Value) ->
    replace(ServerName, Node, Key, Value, 0, [], foo_timeout()).

%% @spec (brick_name(), node_name(), io_list(), io_list(), prop_list() | timeout())
%%    -> zzz_add_reply()
%% @equiv replace(ServerName, Node, Key, Value, 0, Flags, DefaultTimeoutOrFlags)
%% @doc Replace a Key/Value pair in a brick, failing if Key does not already exist.

-spec replace(brick_name(), node_name(), key(), val(), flags_list0() | timeout()) -> replace_reply().
replace(ServerName, Node, Key, Value, Flags) when is_list(Flags) ->
    replace(ServerName, Node, Key, Value, 0, Flags, foo_timeout());
replace(ServerName, Node, Key, Value, Timeout) when is_integer(Timeout) ->
    replace(ServerName, Node, Key, Value, 0, [], Timeout).

%% @spec (brick_name(), node_name(), io_list(), io_list(), integer(), prop_list(), timeout())
%%    -> zzz_add_reply()
%% @doc Replace a Key/Value pair in a brick, failing if Key does not already exist.

-spec replace(brick_name(), node_name(), key(), val(), integer(), flags_list(), timeout()) -> replace_reply().
replace(ServerName, Node, Key, Value, ExpTime, Flags, Timeout)
  when not is_list(Node) ->
    case do(ServerName, Node, [make_replace(Key, Value, ExpTime, Flags)],
            Timeout) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name(), io_list(), io_list())
%%    -> zzz_add_reply()
%% @equiv set(ServerName, Node, Key, Value, 0, [], DefaultTimeout)
%% @doc Add a Key/Value pair to a brick.

-spec set(brick_name(), node_name(), key(), val()) -> set_reply().
set(ServerName, Node, Key, Value) ->
    set(ServerName, Node, Key, Value, 0, [], foo_timeout()).

%% @spec (brick_name(), node_name(), io_list(), io_list(), prop_list() | timeout())
%%    -> zzz_add_reply()
%% @equiv set(ServerName, Node, Key, Value, 0, Flags, DefaultTimeoutOrFlags)
%% @doc Add a Key/Value pair to a brick.

-spec set(brick_name(), node_name(), key(), val(), flags_list0() | non_neg_integer()) -> set_reply().
set(ServerName, Node, Key, Value, Flags) when is_list(Flags) ->
    set(ServerName, Node, Key, Value, 0, Flags, foo_timeout());
set(ServerName, Node, Key, Value, Timeout) when is_integer(Timeout) ->
    set(ServerName, Node, Key, Value, 0, [], Timeout).

%% @spec (brick_name(), node_name(), io_list(), io_list(), integer(), prop_list(), timeout())
%%    -> zzz_add_reply()
%% @doc Add a Key/Value pair to a brick.

-spec set(brick_name(), node_name(), key(), val(), integer(), flags_list(), timeout()) -> set_reply().
set(ServerName, Node, Key, Value, ExpTime, Flags, Timeout)
  when not is_list(Node) ->                     % Assume a list of nodes
    case do(ServerName, Node, [make_set(Key, Value, ExpTime, Flags)],
            Timeout) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name(), io_list())
%%    -> zzz_get_reply()
%% @equiv get(ServerName, Node, Key, [], DefaultTimeout)
%% @doc Get a Key/Value pair from a brick.

-spec get(brick_name(), node_name(), key()) -> get_reply().
get(ServerName, Node, Key) ->
    get(ServerName, Node, Key, [], foo_timeout()).

%% @spec (brick_name(), node_name(), io_list(), prop_list() | timeout())
%%    -> zzz_get_reply()
%% @equiv get(ServerName, Node, Key, [], DefaultTimeoutOrFlags)
%% @doc Get a Key/Value pair from a brick.

-spec get(brick_name(), node_name(), key(), flags_list0() | non_neg_integer()) -> get_reply().
get(ServerName, Node, Key, Flags) when is_list(Flags) ->
    get(ServerName, Node, Key, Flags, foo_timeout());
get(ServerName, Node, Key, Timeout) when is_integer(Timeout) ->
    get(ServerName, Node, Key, [], Timeout).

%% @spec (brick_name(), node_name(), io_list(), prop_list(), timeout())
%%    -> zzz_get_reply()
%% @doc Get a Key/Value pair from a brick.

-spec get(brick_name(), node_name(), key(), flags_list(), timeout()) -> get_reply().
get(ServerName, Node, Key, Flags, Timeout)
  when not is_list(Node) ->
    case do(ServerName, Node, [make_get(Key, Flags)], Timeout) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name(), io_list())
%%    -> zzz_delete_reply()
%% @equiv delete(ServerName, Node, Key, [], DefaultTimeout)
%% @doc Delete a Key/Value pair from a brick.

-spec delete(brick_name(), node_name(), key()) -> delete_reply().
delete(ServerName, Node, Key) ->
    delete(ServerName, Node, Key, [], foo_timeout()).

%% @spec (brick_name(), node_name(), io_list(), prop_list() | timeout())
%%    -> zzz_delete_reply()
%% @equiv delete(ServerName, Node, Key, [], DefaultTimeoutOrFlags)
%% @doc Delete a Key/Value pair from a brick.

-spec delete(brick_name(), node_name(), key(), flags_list0() | non_neg_integer()) -> delete_reply().
delete(ServerName, Node, Key, Flags) when is_list(Flags) ->
    delete(ServerName, Node, Key, Flags, foo_timeout());
delete(ServerName, Node, Key, Timeout) when is_integer(Timeout) ->
    delete(ServerName, Node, Key, [], Timeout).

%% @spec (brick_name(), node_name(), io_list(), prop_list(), timeout())
%%    -> zzz_delete_reply()
%% @doc Delete a Key/Value pair from a brick.

-spec delete(brick_name(), node_name(), key(), flags_list(), timeout()) -> delete_reply().
delete(ServerName, Node, Key, Flags, Timeout)
  when not is_list(Node) ->
    case do(ServerName, Node, [make_delete(Key, Flags)], Timeout) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name(), io_list() | ?BRICK__GET_MANY_FIRST, integer())
%%    -> zzz_getmany_reply()
%% @equiv getmany(ServerName, Node, Key, MaxNum, [], DefaultTimeout)
%% @doc Get many Key/Value pairs from a brick, up to MaxNum.

-spec get_many(brick_name(), node_name(), key() | ?BRICK__GET_MANY_FIRST, integer()) -> getmany_reply().
get_many(ServerName, Node, Key, MaxNum) ->
    get_many(ServerName, Node, Key, MaxNum, [], foo_timeout()).

%% @spec (brick_name(), node_name(), io_list() | ?BRICK__GET_MANY_FIRST, integer(), prop_list() | timeout())
%%    -> zzz_getmany_reply()
%% @equiv getmany(ServerName, Node, Key, MaxNum, [], DefaultTimeoutOrFlags)
%% @doc Get many Key/Value pairs from a brick, up to MaxNum.

-spec get_many(brick_name(), node_name(), key() | ?BRICK__GET_MANY_FIRST, integer(), flags_list() | timeout()) -> getmany_reply().
get_many(ServerName, Node, Key, MaxNum, Flags) when is_list(Flags) ->
    get_many(ServerName, Node, Key, MaxNum, Flags, foo_timeout());
get_many(ServerName, Node, Key, MaxNum, Timeout) when is_integer(Timeout) ->
    get_many(ServerName, Node, Key, MaxNum, [], Timeout).

%% @spec (brick_name(), node_name(), io_list() | ?BRICK__GET_MANY_FIRST, integer(), prop_list(), timeout())
%%    -> zzz_getmany_reply()
%% @doc Get many Key/Value pairs from a brick, up to MaxNum.

-spec get_many(brick_name(), node_name(), key() | ?BRICK__GET_MANY_FIRST, integer(), flags_list(), timeout()) -> getmany_reply().
get_many(ServerName, Node, Key, MaxNum, Flags, Timeout)
  when not is_list(Node), is_integer(MaxNum),
       is_list(Flags), is_integer(Timeout) ->
    case do(ServerName, Node, [make_get_many(Key, MaxNum, Flags)], Timeout) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name(), io_list() | ?BRICK__GET_MANY_FIRST, integer(), prop_list(), prop_list(), timeout())
%%    -> zzz_getmany_reply()
%% @doc Get many Key/Value pairs from a brick, up to MaxNum.

-spec get_many(brick_name(), node_name(), key() | ?BRICK__GET_MANY_FIRST, integer(), flags_list(), prop_list(), timeout()) -> getmany_reply().
get_many(ServerName, Node, Key, MaxNum, Flags, DoFlags, Timeout)
  when not is_list(Node), is_integer(MaxNum),
       is_list(Flags), is_list(DoFlags), is_integer(Timeout) ->
    case do(ServerName, Node, [make_get_many(Key, MaxNum, Flags)], DoFlags,
            Timeout) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name(), do_list())
%%    -> zzz_do_reply() | {error, mumble(), mumble2()}
%% @equiv do(ServerName, Node, OpList, [], DefaultTimeout)
%% @doc Send a list of do ops to a brick.

-spec do(brick_name(), node_name(), do_list()) -> do_reply().
do(ServerName, Node, DoList) ->
    do(ServerName, Node, DoList, [], foo_timeout()).

%% @spec (brick_name(), node_name(), do_list(), prop_list() | timeout())
%%    -> zzz_do_reply() | {error, mumble(), mumble2()}
%% @equiv do(ServerName, Node, DoList, [], DefaultTimeoutOrFlags)
%% @doc Send a list of do ops to a brick.

-spec do(brick_name(), node_name(), do_list(), prop_list() | timeout()) -> do_reply().
do(ServerName, Node, {do, _SentAt, DoList, DoFlags}, Timeout)
  when not is_list(Node), is_list(DoList), is_list(DoFlags),
       is_integer(Timeout) ->
    %% This form is used internally by this module
    %% (e.g. resubmit_all_queued_updates()) and is not recommended for
    %% external use.
    do(ServerName, Node, DoList, DoFlags, Timeout);
do(ServerName, Node, DoList, Timeout)
  when not is_list(Node), is_list(DoList), is_integer(Timeout) ->
    do(ServerName, Node, DoList, [], Timeout).

%% @spec (brick_name(), node_name(), do_list(), prop_list(), timeout())
%%    -> zzz_do_reply() | {txn_fail, list()} | {wrong_brick, term()}
%% @doc Send a list of do ops to a brick.
%%
%% Include the current timestamp in the command tuple, to allow the
%% server the option of shedding load if the server is too busy by
%% ignoring requests that are "too old".
%%
%% Valid options for DoFlags:
%% <ul>
%% <li> fail_if_wrong_role ... If this DoList was sent to the wrong brick
%%      (i.e. the brick's role is wrong vis a vis read vs. write ops),
%%      then immediately fail the transaction.  (Default behavior is to
%%      forward the DoList to the correct brick.) </li>
%% <li> ignore_role ... Ignore the head/tail role of the brick, forcing
%%      processing (e.g. read-only ops on a head brick) and replying
%%      directly (i.e. not sending the op down the chain for replication). </li>
%% <li> term() ... any SSF preprocessing function may see the DoFlags
%%      and modify them, if it wishes.  Neither reading nor altering are
%%      commonly done.  TODO: Hrm, I think that an SSF func can only
%%      see them, that the SSF preprocess iterator funcs don't actually
%%      pass through modifications? </li>
%% <li> sync_override ... (TODO untested, not recommended for external use)
%%      Override the brick's sync/async defaults for this DoList only. </li>
%% <li> local_op_only_do_not_forward ... For internal use only, do not use.</li>
%% <li> squidflash_resubmit ... For internal use only, do not use. </li>
%% <li> {forward_hops, N} ... For internal use only, do not use. </li>
%% </ul>

-spec do(brick_name(), node_name(), do_list(), prop_list(), timeout()) -> do_reply().
do(ServerName, Node, DoList, DoFlags, Timeout)
  when not is_list(Node), is_list(DoList), is_list(DoFlags),
       is_integer(Timeout) ->
    case gen_server:call({ServerName, Node}, {do, now(), DoList, DoFlags},
                         Timeout) of
        %% These clauses are here as an aid of Dialyzer's type inference.
        L when is_list(L) ->
            L;
        {txn_fail, L} = Err when is_list(L) ->
            Err;
        {wrong_brick, _} = Err2 ->
            Err2
    end.

%% @spec (brick_name(), node_name(), io_list())
%%    -> key_not_exist | {quota_root, root(), ts(), val(), exptime(), flags()}
%% @doc Fetch a quota root's name and status info, if the root exists.

-spec get_quota(brick_name(), node_name(), key())
               -> key_not_exist | {quota_root, key(), ts(), val(), exp_time(), flags_list()}.
get_quota(ServerName, Node, Key) ->
    Op = make_get_quota(Key),
    case do(ServerName, Node, [Op], foo_timeout()*4) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name(), io_list(), integer(), integer()) ->
%%       zzz_add_reply()
%% @doc Create or modify a quota root.
%%
%% NOTE: If keys already exist "underneath" the quota root, then
%% creating a quota root with this function will result in incorrect
%% usage values (zero for both item and byte usage counts).  Use the
%% recalculate_quota() function to recalculate the usage counts.
%%
%% Quota implementation details:
%% <ul>
%% <li> Quotas are enforced at a "root" path (similar to an IMAP quota
%% root). </li>
%% <li> The longest matching quota root path is chosen.</li>
%% <li> Nested quota roots are not cumulative (i.e. given roots /foo/
%% and /foo/bar/, an update to Key /foo/bar/baz will only update usage
%% sums at root /root/bar/ and will not also update sums at /foo/.) </li>
%% </ul>
%%
%% This fun needs to access ImplMod &amp; ImplState so it cannot be moved
%% outside of this module easily.

-spec set_quota(brick_name(), node_name(), key(), integer(), integer()) ->
                       quota_reply().
set_quota(ServerName, Node, Key, Items, Bytes) ->
    Op = make_set_quota(Key, Items, Bytes),
    case do(ServerName, Node, [Op], foo_timeout()*4) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name(), binary()) ->
%%       ok | key_not_exist
%% @doc Recalculate a quota root's usage sums,  failing if the quota
%% root does not already exist.
%%
%% This fun needs to access ImplMod &amp; ImplState so it cannot be moved
%% outside of this module easily.

-spec resum_quota(brick_name(), node_name(), key()) ->
                         ok | key_not_exist.
resum_quota(ServerName, Node, Key) when is_binary(Key) ->
    Op = make_resum_quota(Key),
    case do(ServerName, Node, [Op], foo_timeout()*4) of
        [Res] -> Res;
        Else  -> Else
    end.

%% @spec (brick_name(), node_name())
%%    -> zzz_status_reply()
%% @doc Request a brick's status.

-spec status(brick_name(), node_name()) -> status_reply().
status(ServerName, Node) when not is_list(Node) ->
    status(ServerName, Node, foo_timeout()).

-spec status(brick_name(), node_name(), integer()) -> status_reply().
status(ServerName, Node, Timeout) when not is_list(Node) ->
    gen_server:call({ServerName, Node}, {status}, Timeout).

%% @spec (brick_name(), node_name())
%%    -> zzz_state_reply()
%% @doc Request a brick's internal gen_server state.

-spec state(brick_name(), node_name()) -> state_reply().
state(ServerName, Node) when not is_list(Node) ->
    gen_server:call({ServerName, Node}, {state}, foo_timeout()).

%% @spec (brick_name(), node_name())
%%    -> zzz_flushall_reply()
%% @doc Delete all keys from a brick ... requires +18 constitution to use.

-spec flush_all(brick_name(), node_name()) -> flushall_reply().
flush_all(ServerName, Node) when not is_list(Node) ->
    gen_server:call({ServerName, Node}, {flush_all}, foo_timeout()).

%% @spec (brick_name(), node_name())
%%    -> zzz_checkpoint_reply()
%% @equiv checkpoint(ServerName, Node, [])
%% @doc Start a checkpoint operation to flush all terms to disk (and
%%      delete old transaction logs).

-spec checkpoint(brick_name(), node_name()) -> checkpoint_reply().
checkpoint(ServerName, Node) ->
    checkpoint(ServerName, Node, []).

%% @spec (brick_name(), node_name(), prop_list())
%%    -> zzz_checkpoint_reply()
%% @doc Start a checkpoint operation to flush all terms to disk (and
%%      delete old transaction logs).
%%
%% Valid options:
%% <ul>
%% <li> <tt>{checkpoint_sleep_before_close, MSec}</tt> (default = 0) ...
%%      Sleep before closing checkpoint file. </li>
%% <li> <tt>{checkpoint_sync_before_close, Boolean}</tt> (default = true) ...
%%      Use fsync before closing checkpoint file. </li>
%% <li> <tt>{start_sleep_time, MilliSeconds}</tt> ... (default = 0) ...
%%      Start checkpoint after sleeping for MilliSeconds. </li>
%% <li> <tt>silent</tt> (default = false) ... Do not write LOG_INFO messages
%%      to the app log. </li>
%% <li> <tt>{throttle_bytes, N}</tt> (default = use common `cp_throttle' proc) ...
%%      override the throttle value of central.conf's
%%      "brick_check_checkpoint_throttle_bytes" config knob and create
%%      a separate throttle process for use by this checkpoint only. </li>
%% </ul>

-spec checkpoint(brick_name(), node_name(), prop_list()) -> checkpoint_reply().
checkpoint(ServerName, Node, Options) when not is_list(Node) ->
    gen_server:call({ServerName, Node}, {checkpoint, Options}, 300*1000).

checkpoint_bricks(Node) when is_atom(Node) ->
    checkpoint_bricks([{Br, Node} || Br <- brick_shepherd:list_bricks(Node)]);
checkpoint_bricks(BrickList) when is_list(BrickList) ->
    [brick_server:checkpoint(Br, Nd) || {Br, Nd} <- BrickList].

checkpoint_bricks_and_wait(Node) when is_atom(Node) ->
    checkpoint_bricks_and_wait(
      [{Br, Node} || Br <- brick_shepherd:list_bricks(Node)]);
checkpoint_bricks_and_wait(BrickList) when is_list (BrickList) ->
    checkpoint_bricks(BrickList),
    poll_checkpointing_bricks(BrickList).

%% @spec (brick_name(), node_name(), prop_list())
%%    -> zzz_syncdown_reply()
%% @equiv sync_down_the_chain(ServerName, Node, Options, DefaultTimeout)
%% @doc Send a sync message down the chain.

-spec sync_down_the_chain(brick_name(), node_name(), prop_list()) -> syncdown_reply().
sync_down_the_chain(ServerName, Node, Options) when is_list(Options) ->
    sync_down_the_chain(ServerName, Node, Options, foo_timeout()).

%% @spec (brick_name(), node_name(), prop_list(), timeout())
%%    -> zzz_syncdown_reply()
%% @doc Send a sync message down the chain.
%%
%% A side-effect of the sync message is that the official_tail of the
%% chain will immediately send an serial ack message back upstream.
%% This can be useful to reduce the latency in the head getting an ack
%% from its downstream.
%%
%% Valid options:
%% <ul>
%% <li> stop_at_official_tail ... do not send the sync request past the
%%      official tail of the chain. </li>
%% </ul>

-spec sync_down_the_chain(brick_name(), node_name(), prop_list(), integer()) -> syncdown_reply().
sync_down_the_chain(ServerName, Node, Options, Timeout) when is_list(Options)->
    ReplyTag = make_ref(),
    gen_server:cast({ServerName, Node},
                    {sync_down_the_chain, Options, self(), ReplyTag, []}),
    receive
        {Tag, Reply} when Tag =:= ReplyTag ->
            Reply
    after Timeout ->
            exit(timeout)
    end.

%% @spec (server(), boolean()) -> boolean()
%% @doc Set value of do_sync, returns previous value of do_sync.

-spec set_do_sync(server(), boolean()) -> boolean().
set_do_sync(Server, NewValue) ->
    gen_server:call(Server, {set_do_sync, NewValue}).

%% @spec (server(), boolean()) -> boolean()
%% @doc Set value of do_logging, returns previous value of do_logging.

-spec set_do_logging(server(), boolean()) -> boolean().
set_do_logging(Server, NewValue) ->
    gen_server:call(Server, {set_do_logging, NewValue}).

%% @spec (brick_name(), node_name(), proplist()) -> ok
%% @doc Set value of do_logging, returns previous value of do_logging.
%%
%% For the <tt>brick_ets</tt> backend, the following properties may be
%% used:
%% <ul>
%% <li> <tt>destructive</tt> (default = true) If false, the scavenger will not
%%      make any permanent updates to disk. </li>
%% <li> <tt>silent</tt> (default = false) If true, the scavenger will write
%%      any LOG_INFO messages to the app log. </li>
%% <li> <tt>skip_reads</tt> (default = false) If true, and if
%%      destructive is false, then no hunks will be read by the scavenger
%%      (which invalidates the byte counts but requires even less resources
%%      to make a debugging scavenger run). </li>
%% <li> <tt>skip_live_percentage_greater_than</tt> (default = 0) Valid for ranges of
%%      0-100, any sequence log file that has a higher amount of
%%      "live"/in-use data will be skipped. </li>
%% </ul>

-spec start_scavenger(brick_name(), node_name(), prop_list()) -> ok.
start_scavenger(ServerName, Node, PropList) ->
    gen_server:call({ServerName, Node}, {start_scavenger, PropList}).

%% Chain-related API

%% @spec (brick_name(), node_name())
%%    -> zzz_rolestandalone_reply()
%% @doc Set a brick's chain role to "standalone".

-spec chain_role_standalone(brick_name(), node_name()) -> rolestandalone_reply().
chain_role_standalone(ServerName, Node) ->
    gen_server:call({ServerName, Node}, {chain_role, standalone},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name())
%%    -> zzz_roleundefined_reply()
%% @doc Set a brick's chain role to "undefined".

-spec chain_role_undefined(brick_name(), node_name()) -> roleundefined_reply().
chain_role_undefined(ServerName, Node) ->
    gen_server:call({ServerName, Node}, {chain_role, undefined},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name(), brick_name(), node_name(), prop_list())
%%    -> zzz_rolehead_reply()
%% @doc Set a brick's chain role to "head".

-spec chain_role_head(brick_name(), node_name(), brick_name(), node_name(), prop_list()) -> rolehead_reply().
chain_role_head(ServerName, Node, DownNode, DownServerName, PropList) ->
    PL2 = [head, {downstream_node, {DownNode, DownServerName}}] ++ PropList,
    PL3 = get_debug_chain_props(),
    gen_server:call({ServerName, Node}, {chain_role, PL2++PL3}, foo_timeout() * 1).

%% @spec (brick_name(), node_name(), brick_name(), node_name(), prop_list())
%%    -> zzz_roletail_reply()
%% @doc Set a brick's chain role to "tail".

-spec chain_role_tail(brick_name(), node_name(), brick_name(), node_name(), prop_list()) -> roletail_reply().
chain_role_tail(ServerName, Node, UpNode, UpServerName, PropList) ->
    PL2 = [tail, {upstream_node, {UpNode, UpServerName}}] ++ PropList,
    PL3 = get_debug_chain_props(),
    gen_server:call({ServerName, Node}, {chain_role, PL2++PL3}, foo_timeout() * 1).

%% @spec (brick_name(), node_name(), brick_name(), node_name(), brick_name(), node_name(), prop_list())
%%    -> zzz_rolemiddle_reply()
%% @doc Set a brick's chain role to "middle".

-spec chain_role_middle(brick_name(), node_name(), brick_name(), node_name(), brick_name(), node_name(), prop_list()) -> rolemiddle_reply().
chain_role_middle(ServerName, Node, UpNode, UpServerName,
                  DownNode, DownServerName, PropList) ->
    PL2 = [middle, {upstream_node, {UpNode, UpServerName}},
           {downstream_node, {DownNode, DownServerName}}] ++ PropList,
    PL3 = get_debug_chain_props(),
    gen_server:call({ServerName, Node}, {chain_role, PL2++PL3}, foo_timeout() * 1).

%% @spec (brick_name(), node_name())
%%    -> zzz_getrole_reply()
%% @doc Get a brick's chain role.

-spec chain_get_role(brick_name(), node_name()) -> getrole_reply().
chain_get_role(ServerName, Node) ->
    gen_server:call({ServerName, Node}, {chain_get_role},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name(), repair_state_name())
%%    -> zzz_setmyrepair_reply()
%% @doc Set a brick's repair state (relative to the rest of the chain).

-spec chain_set_my_repair_state(brick_name(), node_name(), repair_state_name())
                               -> setmyrepair_reply().
chain_set_my_repair_state(ServerName, Node, Val) ->
    gen_server:call({ServerName, Node}, {chain_set_my_repair_state, Val},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name(), repair_state_name())
%%    -> zzz_setmyrepair_reply()
%% @doc Set a brick's notion of its immediate downstream brick's repair state.

-spec chain_set_ds_repair_state(brick_name(), node_name(), repair_state_name())
                               -> setmyrepair_reply().
chain_set_ds_repair_state(ServerName, Node, Val) ->
    gen_server:call({ServerName, Node}, {chain_set_ds_repair_state, Val},
                    foo_timeout() * 1).

%% @ equiv (BrickName, NodeName, foo_timeout() * 1)

-spec chain_get_my_repair_state(brick_name(), node_name()) -> repair_state_name().
chain_get_my_repair_state(ServerName, Node) ->
    chain_get_my_repair_state(ServerName, Node, foo_timeout() * 1).

%% @spec (brick_name(), node_name(), integer())
%%    -> repair_state_name()
%% @doc Get a brick's repair state (relative to the rest of the chain).

-spec chain_get_my_repair_state(brick_name(), node_name(), integer()) -> repair_state_name().
chain_get_my_repair_state(ServerName, Node, Timeout) ->
    gen_server:call({ServerName, Node}, {chain_get_my_repair_state}, Timeout).

%% @spec (brick_name(), node_name())
%%    -> repair_state_name()
%% @doc Get the repair state of a brick's immediate downstream brick.

-spec chain_get_ds_repair_state(brick_name(), node_name()) -> repair_state_name().
chain_get_ds_repair_state(ServerName, Node) ->
    gen_server:call({ServerName, Node}, {chain_get_ds_repair_state},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name())
%%    -> repair_state_name()
%% @doc Initiate the repair process on a brick.

-spec chain_start_repair(brick_name(), node_name()) -> ok | {error, not_chain_member | no_downstream | ds_repair_state, term()}.
chain_start_repair(ServerName, Node) ->
    gen_server:call({ServerName, Node}, {chain_start_repair},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name(), global_hash_r())
%%    -> ok | error
%% @doc Set the global hash structure for a brick, unconditionally
%%      replacing the brick's current GH structure.

-spec chain_hack_set_global_hash(brick_name(), node_name(), global_hash_r()) -> ok | {error, integer()}.
chain_hack_set_global_hash(ServerName, Node, GH) ->
    chain_hack_set_global_hash(ServerName, Node, GH, foo_timeout() * 1).

chain_hack_set_global_hash(ServerName, Node, GH, Timeout) ->
    gen_server:call({ServerName, Node},
                    {chain_hack_set_global_hash, GH, false},
                    Timeout).

%% @spec (brick_name(), node_name())
%%    -> global_hash_r()
%% @doc Get the global hash record from a brick.

-spec chain_get_global_hash(brick_name(), node_name()) -> global_hash_r().
chain_get_global_hash(ServerName, Node) ->
    gen_server:call({ServerName, Node}, {chain_get_global_hash},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name(), true | false)
%%    -> ok
%% @doc Set read-only mode on a brick.

-spec chain_set_read_only_mode(brick_name(), node_name(), true | false) -> ok.
chain_set_read_only_mode(ServerName, Node, On_p)
  when On_p == true; On_p == false ->
    gen_server:call({ServerName, Node}, {chain_set_read_only_mode, On_p},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name())
%%    -> {integer(), integer()}
%% @doc Get the list of a brick's downstream serial numbers: the last serial
%% number sent and the last serial number ack'ed by the downstream,
%% respectively.

-spec chain_get_downstream_serials(brick_name(), node_name()) -> {integer(), integer()}.
chain_get_downstream_serials(ServerName, Node) ->
    gen_server:call({ServerName, Node}, {chain_get_downstream_serials},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name())
%%    -> ok
%% @doc Request a flush of a brick's un-ack'ed log to the downstream brick.
%%
%% This step is necessary when a brick in the middle of a chain
%% crashes: the brick immediately upstream of the failed brick must
%% re-send any parts of its log that have not yet been ack'ed to the new
%% immediately downstream brick.

-spec chain_flush_log_downstream(brick_name(), node_name()) -> ok.
chain_flush_log_downstream(ServerName, Node) ->
    gen_server:call({ServerName, Node}, {chain_flush_log_downstream},
                    foo_timeout() * 5).

%% @spec (brick_name(), node_name(), proplist())
%%    -> ok
%% @doc Prepend the proplist() to a brick's chain state property list
%%      (for debugging purposes, mostly).

-spec chain_add_to_proplist(brick_name(), node_name(), prop_list()) -> ok.
chain_add_to_proplist(ServerName, Node, PropList) when is_list(PropList) ->
    gen_server:call({ServerName, Node}, {chain_add_to_proplist, PropList},
                    foo_timeout() * 1).

%% @spec (brick_name(), node_name(), term(), atom(), proplist())
%%    -> term() | {error, Reason, ExtraInfo}
%% @doc Initiate a data migration sweep, see {@link start_sweep/3}
%% for Options details.
-spec migration_start_sweep(brick_name(), node_name(), term(), atom(), prop_list()) -> term() | {error, term(), term()}.
migration_start_sweep(ServerName, Node, Cookie, YourChainName, Options) ->
    migration_start_sweep(ServerName, Node, Cookie, YourChainName, Options,
                          foo_timeout() * 5).

-spec migration_start_sweep(brick_name(), node_name(), term(), atom(), prop_list(), integer()) -> term() | {error, term(), term()}.
migration_start_sweep(ServerName, Node, Cookie, YourChainName, Options,
                      Timeout) ->
    gen_server:call({ServerName, Node},
                    {migration_start_sweep, Cookie, YourChainName, Options},
                    Timeout).

%% @spec (brick_name(), node_name())
%%    -> {ok, now(), prop_list()} |
%%       {error, not_migrating | migrating, term()}
%% @doc Clear data migration sweep status for a brick that has
%%      finished the migration process.
%%
%% The ok now() value is the time the migration process finished in
%% this chain; prop_list() is an optional list of
%% attributes/statistics associated with the chain's migration sweep.

-spec migration_clear_sweep(brick_name(), node_name())
                           -> {ok, integer(), prop_list()} |
                                  {error, not_migrating | migrating, term()}.
migration_clear_sweep(ServerName, Node) ->
    migration_clear_sweep(ServerName, Node, foo_timeout() * 5).

-spec migration_clear_sweep(brick_name(), node_name(), integer())
                           -> {ok, integer(), prop_list()} |
                                  {error, not_migrating | migrating, term()}.
migration_clear_sweep(ServerName, Node, Timeout) ->
    gen_server:call({ServerName, Node}, {migration_clear_sweep}, Timeout).

%% @spec (brick_name(), node_name(), term())
%%    -> list({Key, Val})
%% @doc Brick private metadata testing: get Key.

-spec md_test_get(brick_name(), node_name(), key()) -> list({key(), val()}).
md_test_get(ServerName, Node, Key) ->
    gen_server:call({ServerName, Node}, {md_test_get, Key},
                    foo_timeout() * 5).

%% @spec (brick_name(), node_name(), term(), term())
%%    -> ok
%% @doc Brick private metadata testing: set a Key/Val pair.

-spec md_test_set(brick_name(), node_name(), key(), val()) -> ok.
md_test_set(ServerName, Node, Key, Val) ->
    gen_server:call({ServerName, Node}, {md_test_set, Key, Val},
                    foo_timeout() * 5).

%% @spec (brick_name(), node_name(), term())
%%    -> list(tuple())
%% @doc Brick private metadata testing: delete Key.

-spec md_test_delete(brick_name(), node_name(), key()) -> list(tuple()).
md_test_delete(ServerName, Node, Key) ->
    gen_server:call({ServerName, Node}, {md_test_delete, Key},
                    foo_timeout() * 5).

%% @spec (brick_name(), node_name(), integer()) -> sorry | ok
%% @doc Func used by CommonLog server to inform a brick that one of the
%% common log sequence files is bad.

-spec common_log_sequence_file_is_bad(brick_name(), node_name(), integer()) -> sorry | ok.
common_log_sequence_file_is_bad(ServerName, Node, SeqNum) ->
    gen_server:call({ServerName, Node},
                    {common_log_sequence_file_is_bad, SeqNum}).


%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% @doc gen_server init/1 callback.
%%
%% Valid options (see also {@link brick_ets:init/1}):
%% <ul>
%% <li> {implementation_module, Mod} ... Name of the local storage
%%      implementation module, default = 'brick_ets'. </li>
%% <li> do_not_initiate_serial_ack ... If true, do not initiated sending
%%      'ch_serial_ack' messages to any upstream brick, default = none. </li>
%% </ul>
%%----------------------------------------------------------------------
init([ServerName, Options]) ->
    %% Register name as very first thing, brick_ets is assuming it's done.
    register(ServerName, self()),
    catch exit(whereis(brick_pingee:make_name(ServerName)), kill),

    ImplMod = proplists:get_value(implementation_module, Options,
                                  brick_ets),
    Options2 = [{implementation_module, ImplMod}|Options],
    BadPid = spawn(fun() -> ok end),            % Will die very shortly
    PreprocList =
        try
            case gmt_config_svr:get_config_value(brick_preprocess_method, "") of
                {ok, "none"}     ->
                    [];
                {ok, "ssf_only"} ->
                    [fun ssf_preprocess/3];
                {ok, "["++_ = Px} ->
                        case brick_server:get_tabprop_via_str(ServerName, Px) of
                            none              -> [];
                            ssf_only          -> [fun ssf_preprocess/3];
                            L when is_list(L) -> L
                        end
                end
        catch
            _:_ ->
                [fun quotas_preprocess/3, fun ssf_preprocess/3]
        end,
    ?E_INFO("~s:init preprocess ~p\n", [?MODULE, PreprocList]),

    %% NOTE: Throttle table must be named and public.
    ThrottleTab = ets:new(throttle_tab_name(ServerName),
                          [public, named_table, ordered_set]),

    {ok, BZsumm} = gmt_config_svr:get_config_value_boolean(debug_check_hunk_summ, "false"),
    {ok, BZblob} = gmt_config_svr:get_config_value_boolean(debug_check_hunk_blob, "false"),
    if BZsumm orelse BZblob ->
            ?E_WARNING("NOTE: debug_check_hunk_summ = ~p, debug_check_hunk_blob = ~p\n", [BZsumm, BZblob]);
       true ->
            ok
    end,

    CS = #chain_r{unknown_acks = orddict:new()},
    MyS0 = #state{name = ServerName, options = Options2,
                  impl_mod = ImplMod,
                  logging_op_q = queue:new(), bad_pid = BadPid,
                  do_list_preprocess = PreprocList,
                  bz_debug_check_hunk_summ = BZsumm,
                  bz_debug_check_hunk_blob = BZblob,
                  throttle_tab = ThrottleTab},
    MyS = get_do_op_too_old_timeout(MyS0),
    put(handle_call_do_prescreen, 0),
    put(bz_debug_get_fh, {0, element(2, file:open("/dev/null", [write]))}), %BZDEBUG
    case ImplMod:init([ServerName, Options2]) of
        {ok, S} ->
            StartTime = ImplMod:bcb_start_time(S),
            {ok, PPid} = brick_pingee:start_link(ServerName, StartTime),
            {ok, MyS#state{impl_state = S, pingee_pid = PPid,
                           chainstate = CS}};
        {ok, S, Timeout} ->
            StartTime = ImplMod:bcb_start_time(S),
            {ok, PPid} = brick_pingee:start_link(ServerName, StartTime),
            {ok, MyS#state{impl_state = S, pingee_pid = PPid,
                           chainstate = CS}, Timeout};
        Err ->
            Err
    end.

%%----------------------------------------------------------------------
%% Func: handle_call/3
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% @doc gen_server handle_call/3 callback.
%%----------------------------------------------------------------------
handle_call({do, _SentAt, _, _} = Msg, From, State) ->
    handle_call_do_prescreen(Msg, From, State);
handle_call(Msg, From, State)
  when element(1, Msg) == state;
       element(1, Msg) == flush_all;
       element(1, Msg) == dump_state;
       element(1, Msg) == sync_stats;           % debugging? useful?
       element(1, Msg) == set_do_sync;
       element(1, Msg) == set_do_logging;
       element(1, Msg) == start_scavenger ->
    ?DBG_GENx({State#state.name, Msg}),
    handle_call_via_impl(Msg, From, State);
handle_call({checkpoint, _} = Msg, From, State) ->
    if (State#state.chainstate)#chain_r.my_repair_state == ok ->
            handle_call_via_impl(Msg, From, State);
       true ->
            {reply, sorry, State}
    end;
handle_call({status} = _Msg, _From, State) ->
    ImplStatus = get_implementation_status(State),
    {Reply, NewState} = do_status(State, ImplStatus),
    {reply, Reply, NewState};
handle_call({chain_role, standalone}, _From, State) ->
    ?DBG_CHAINx({chain_role, State#state.name, standalone}),
    {Reply, NewState} = chain_set_role_standalone(State),
    {reply, Reply, NewState};
handle_call({chain_role, undefined}, _From, State) ->
    ?DBG_CHAINx({chain_role, State#state.name, undefined}),
    {Reply, NewState} = chain_set_role_undefined(State),
    {reply, Reply, NewState};
handle_call({chain_role, PropList}, _From, State) ->
    ?DBG_CHAINx({chain_role, State#state.name, PropList}),
    {Reply, NewState} = chain_set_role(State, PropList),
    {reply, Reply, NewState};
handle_call({chain_get_role}, _From, State) ->
    CS = State#state.chainstate,
    Reply = if CS#chain_r.role == undefined; CS#chain_r.role == standalone ->
                    CS#chain_r.role;
               CS#chain_r.upstream == undefined ->
                    {head, CS#chain_r.downstream};
               CS#chain_r.downstream == undefined ->
                    {tail, CS#chain_r.upstream};
               true ->
                    {middle, CS#chain_r.upstream, CS#chain_r.downstream}
            end,
    {reply, Reply, State};
handle_call({chain_set_my_repair_state, Val}, _From, State) ->
    {Reply, NewState} = chain_set_my_repair_state(State, Val),
    {reply, Reply, NewState};
handle_call({chain_set_ds_repair_state, Val}, _From, State) ->
    {Reply, NewState} = chain_set_ds_repair_state(State, Val),
    {reply, Reply, NewState};
handle_call({chain_get_my_repair_state}, _From, State) ->
    {reply, (State#state.chainstate)#chain_r.my_repair_state, State};
handle_call({chain_get_ds_repair_state}, _From, State) ->
    RS = (State#state.chainstate)#chain_r.ds_repair_state,
    {reply, RS, State};
handle_call({chain_start_repair}, _From, State) ->
    {Reply, NewState} = chain_start_repair(State),
    {reply, Reply, NewState};
handle_call({chain_hack_set_global_hash, GH_0, false}, _From, State) ->
    ?DBG_CHAINx({set_global_hash, State#state.name, set_gh, GH_0}),
    MyGH = State#state.globalhash,
    if not is_record(MyGH, g_hash_r) ->
            {reply, ok, State#state{globalhash = GH_0}};
       GH_0#g_hash_r.minor_rev < MyGH#g_hash_r.minor_rev ->
            {reply, {error, MyGH#g_hash_r.minor_rev}, State};
       true ->
            GH = make_new_global_hash(State#state.globalhash, GH_0, State),
            {reply, ok, State#state{globalhash = GH}}
    end;
handle_call({chain_get_global_hash}, _From, State) ->
    {reply, State#state.globalhash, State};
handle_call({get_too_old}, _From, State) ->
    {reply, State#state.n_too_old, State};
handle_call({chain_set_read_only_mode, On_p}, _From, State) ->
    {ImplMod, ImplState} = impl_details(State),
    CS = State#state.chainstate,
    ImplState2 = ImplMod:bcb_set_read_only_mode(On_p, ImplState),
    Ws = if CS#chain_r.read_only_p, not On_p ->
                 %% Replay all of the updates that we've received and queued.
                 ?DBG_CHAINx({chain_set_read_only_mode, State#state.name, read_only_waiters, CS#chain_r.read_only_waiters}),
                 resubmit_all_queued_update_ops(State#state.name,
                                                CS#chain_r.read_only_waiters),
                 [];
            true ->
                 CS#chain_r.read_only_waiters
         end,
    {reply, ok, State#state{impl_state = ImplState2,
                            chainstate = CS#chain_r{read_only_p = On_p,
                                                    read_only_waiters = Ws}}};
handle_call({chain_get_downstream_serials}, _From, State) ->
    CS = State#state.chainstate,
    {reply, {CS#chain_r.down_serial, CS#chain_r.down_acked}, State};
handle_call({chain_flush_log_downstream}, _From, State) ->
    CS = State#state.chainstate,
    if CS#chain_r.role == standalone orelse
       (CS#chain_r.role == chain_member andalso
        CS#chain_r.downstream /= undefined) ->
            NewState = do_chain_flush_log_downstream(State),
            {reply, ok, NewState};
       true ->
            {reply, {bad_role, CS#chain_r.role}, State}
    end;
handle_call({chain_add_to_proplist, PropList}, _From, State) ->
    CS = State#state.chainstate,
    Ps = CS#chain_r.proplist,
    {reply, ok,
     State#state{chainstate = CS#chain_r{proplist = PropList ++ Ps}}};
handle_call({migration_start_sweep, Cookie, MyChainName, Options} = _Msg,
            _From, State) ->
    {Reply, NewState} = do_migration_start_sweep(Cookie, MyChainName,
                                                 Options, State),
    {reply, Reply, NewState};
handle_call({migration_clear_sweep}, _From, State) ->
    {Reply, NewState} = do_migration_clear_sweep(State),
    {reply, Reply, NewState};
handle_call({md_test_get, Key}, _From, State) ->
    {ImplMod, ImplState} = impl_details(State),
    Reply = ImplMod:bcb_get_metadata(Key, ImplState),
    {reply, Reply, State};
handle_call({md_test_set, Key, Val}, _From, State) ->
    {ImplMod, ImplState} = impl_details(State),
    ImplState2 = ImplMod:bcb_set_metadata(Key, Val, ImplState),
    {reply, ok, State#state{impl_state = ImplState2}};
handle_call({md_test_delete, Key}, _From, State) ->
    {ImplMod, ImplState} = impl_details(State),
    ImplState2 = ImplMod:bcb_delete_metadata(Key, ImplState),
    {reply, ok, State#state{impl_state = ImplState2}};
handle_call({stop}, _From, State) ->
    {ImplMod, ImplState} = impl_details(State),
    Reply = ImplMod:terminate(normal, ImplState),
    {stop, normal, Reply, State};
handle_call({common_log_sequence_file_is_bad, SeqNum}, _From, State) ->
    case impl_mod_uses_bigdata_dir(State) of
        false ->
            {reply, sorry, State};
        true ->
            case {(State#state.chainstate)#chain_r.my_repair_state,
                  lookup_sequence_file_is_bad_key(State#state.throttle_tab,
                                                  SeqNum)} of
                {_, true} ->
                    %% We've seen this one before, nothing else to do here.
                    {reply, ok, State};
                {{last_key, _}, _} ->
                    %% We are undergoing repair *and* someone has found
                    %% another bad sequence file.  Bummer.  We cannot purge
                    %% keys now, so the only thing to do is exit.  When we
                    %% restart, we'll see the bad sequence number in the
                    %% bad-sequence file and purge keys at that time, when
                    %% it's safe to do so.
                    ?E_ERROR("~s: repair in progress, sequence file ~p is bad",
                             [State#state.name, SeqNum]),
                    {stop, normal, ok, State};
                {_, _} ->
                    NewState =
                        do_common_log_sequence_file_is_bad(SeqNum, State),
                    {reply, ok, NewState}
            end
    end.

handle_call_do_prescreen(Msg, From, State) ->
    case lookup_throttle_key(State#state.throttle_tab) of
        false ->
            put(handle_call_do_prescreen, 0),
            handle_call_do_prescreen2(Msg, From, State);
        true ->
            NNN = get(handle_call_do_prescreen),
            if NNN < 20 ->
                    ?E_ERROR("handle_call_do_prescreen: ~s forwarding!\n", [State#state.name]);
               true ->
                    ok
            end,
            put(handle_call_do_prescreen, NNN+1),
            %% We have non-repair throttling in effect.  Forward the
            %% message to ourself, after a small delay.  Exponential
            %% delay might be better, but it requires more
            %% bookkeeping.  This gen_server tuple mimicry is a kludge.
            spawn(fun() ->
                          timer:sleep(250),
                          State#state.name ! {'$gen_call', From, Msg}
                  end),
            {noreply, State}
    end.

handle_call_do_prescreen2({do, _SentAt, _DoList, _DoFlags} = Msg, From, State)
  when (State#state.chainstate)#chain_r.waiting_for_mig_start_p ->
    #chain_r{mig_start_waiters = Ws} = CS = State#state.chainstate,
    DQI = #dirty_q{from = From, do_op = Msg},
    {noreply,
     State#state{chainstate = CS#chain_r{mig_start_waiters = [DQI|Ws]}}};
handle_call_do_prescreen2({do, SentAt, _, _} = Msg, From, State) ->
    case chain_check_my_status(State) of
        ok ->
            CS = State#state.chainstate,
            if CS#chain_r.my_repair_state /= ok ->
                    %% CHAIN TODO: What error should be returned instead?
                    exit({should_never_happen_paranoia_1,
                          CS#chain_r.my_repair_state});
               true ->
                    ok
            end,
            case timer:now_diff(now(), SentAt) of
                TooOld when TooOld > State#state.do_op_too_old_usec ->
                    ?E_DBG(?CAT_OP, "~p: msg ~p too old by ~p usec\n",
                           [State#state.name, Msg, TooOld]),
                    NTO = State#state.n_too_old + 1,
                    {noreply, State#state{n_too_old = NTO}};
                _ ->
                    handle_call_do(Msg, From, State)
            end;
        Err ->
            %% Not yet, breaks unit tests: {reply, Err, State}
            {do, _, DoList, _} = Msg,
            %% Give the same error to every element of the do list L.
            {reply, lists:duplicate(length(DoList), Err), State}
    end.

%% @spec (do_op(), term(), state_r()) -> gen_server_handle_call_reply_tuple()
%% @doc Perform the calculation originally done by handle_call() for
%% a single do op.
%%
%% This code was moved to a separate function to avoid the distracting
%% clutter in handle_call().

-spec handle_call_do(do_op(), term(), state_r()) -> gen_server_handle_call_reply_tuple().
handle_call_do({do, _SentAt, [] = _DoList, _DoFlags}, _From, State) ->
    {reply, [], State};
handle_call_do({do, SentAt, DoList, DoFlags0} = _Msg, From, State) ->
    ?DBG_OPx({do, State#state.name, DoList}),
    DoFlags = case get_sweep_start_and_end(State) of
                  undefined ->
                      [];
                  {SweepA, SweepZ} ->
                      [{internal___sweep_zone___, {SweepA, SweepZ}}]
              end ++ DoFlags0,
    case chain_all_do_list_keys_local_p(DoList, DoFlags, State) of
        {true, _DoKeys} ->
            %% QQQ: _DoKeys = [{read,<<"foo">>}]
            case preprocess_fold(DoList, DoFlags,
                                 State#state.do_list_preprocess, State) of
                {ok, DoList2, DoFlags2, State2} ->
                    handle_call_via_impl({do, SentAt, DoList2, DoFlags2},
                                         From, State2);
                {reply, _, _} = Reply ->
                    Reply
            end;
        {false, BrickNodePairs} ->
            case proplists:get_value(fail_if_wrong_role, DoFlags) of
                true ->
                    {reply, {wrong_brick, BrickNodePairs}, State};
                undefined ->
                    if length(BrickNodePairs) < 2 ->
                            %% If len = 1, forward to that brick.
                            %% If len = 0, forward to ourselves (conflict of
                            %% global hash vs. current role!), use forwarding
                            %% to hope that the conflict will be fixed in the
                            %% near future.
                            Server = case BrickNodePairs of
                                         [DestBrick] ->
                                             DestBrick;
                                         [] ->
                                             {State#state.name, node()}
                                     end,
                            Hops = 1 + proplists:get_value(forward_hops,
                                                           DoFlags, 0),
                            DoFlags2 = [{forward_hops, Hops}|DoFlags],
                            SleepTime = if Hops < 3 -> 25;
                                           Hops < 6 -> (Hops - 2) * 75;
                                           true     -> (Hops - 5) * 175
                                        end,
                            if SleepTime > 0 -> ?E_DBG(?CAT_CHAIN, "~p: forward to ~p after ~p: ~p: ~p\n", [now(), Server, SleepTime, State#state.name, _Msg]); true -> ok end,
                            ?DBG_CHAINx({fwd, State#state.name, Server, {hops,Hops}, DoList}),
                            spawn(fun() -> timer:sleep(SleepTime),
                                           FwdMsg = {forward_call,
                                                  {do, now(), DoList, DoFlags2},
                                                  From, Hops},
                                           gen_server:cast(Server, FwdMsg)
                                  end),
                            {noreply, State};
                       true ->
                            {reply, {wrong_brick, BrickNodePairs}, State}
                    end
            end
    end.

%%----------------------------------------------------------------------
%% Func: handle_cast/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% @doc gen_server handle_cast/2 callback.
%%----------------------------------------------------------------------
handle_cast({ch_log_replay_v2, UpstreamBrick, Serial, _Thisdo_Mods, _From,
                                     _Reply, _LastUpstreamSerial} = _Msg, State)
  when (State#state.chainstate)#chain_r.my_repair_state == ok,
       (State#state.chainstate)#chain_r.upstream /= UpstreamBrick ->
    CS = State#state.chainstate,
    ?E_ERROR("log replay: ~p: Got bad replay from ~p serial ~p, "
             "my upstream is ~p ... ignoring!\n",
             [State#state.name, UpstreamBrick, Serial, CS#chain_r.upstream]),
    ?DBG_CHAIN_TLOGx({ch_log_replay_v2, State#state.name, UpstreamBrick, Serial, my_upstream_is, CS#chain_r.upstream}),
    {noreply, State};
handle_cast({ch_log_replay_v2, UpstreamBrick, Serial, Thisdo_Mods, From,
             Reply, LastUpstreamSerial} = Msg, State) ->
    ?DBG_CHAIN_TLOGx({ch_log_replay_v2, State#state.name, UpstreamBrick, Serial}),
    State2 = exit_if_bad_serial_from_upstream(Serial, LastUpstreamSerial,
                                              Msg, State),
    {NewState, _} =
        chain_do_log_replay(Serial, Thisdo_Mods, From, Reply, State2),
    {noreply, NewState};
%% % % % % %
handle_cast({ch_serial_ack, Serial, BrickName, Node, Props} = Msg, State) ->
    CS = State#state.chainstate,
    if CS#chain_r.downstream == {BrickName, Node} ->
            %% Intentionally no 'true' catch-all.
            NewProps = [process_info(self(), message_queue_len)|Props],
            D_serial =
                if Serial == 0, CS#chain_r.down_acked > 0 ->
                        ?E_INFO("Ack from downstream ~p is serial 0, "
                                "role change?\n", [CS#chain_r.downstream]),
                        Serial;
                   Serial < 0
                   andalso CS#chain_r.down_acked == ?NEG_LIMBO_SEQNUM ->
                        %% Race condition here: we've very recently
                        %% finished with repair.  The downstream is
                        %% acking with a negative number, which
                        %% happens only during repair.  Such an ack
                        %% mismatch is ok as long as we're at
                        %% ?NEG_LIMBO_SEQNUM.
                        CS#chain_r.down_acked;
                   Serial < CS#chain_r.down_acked ->
                        ?E_WARNING("bad ch_serial_ack: ~p has down_acked ~p "
                                   "got ~p in ~P\n",
                                   [State#state.name, CS#chain_r.down_acked,
                                    Serial, Msg, 20]),
                        ?E_WARNING("bad ch_serial_ack: ~p will kill "
                                   "brick ~p on node ~p\n",
                                   [State#state.name, BrickName, Node]),
                        spawn(Node, fun() ->
                                            ?E_WARNING("Bad serial ack rec'd "
                                                       "by ~p, killing ~p\n",
                                                       [State#state.name,
                                                        BrickName]),
                                            exit(whereis(BrickName), kill)
                                    end),
                        CS#chain_r.down_acked;
                   Serial >= CS#chain_r.down_acked ->
                        if CS#chain_r.upstream /= undefined ->
                                ?DBG_REPAIRx({before_send_serial_ack, State#state.name, loc_a_serial, Serial}),
                                send_serial_ack(CS, Serial,
                                                State#state.name, node(),
                                                NewProps);
                           true ->
                                ok
                        end,
                        Serial
                end,
            NewQ = pull_items_out_of_unacked(D_serial, CS#chain_r.down_unacked),
            {noreply, State#state{chainstate =
                                  CS#chain_r{down_acked = D_serial,
                                             down_unacked = NewQ,
                                             last_ack = now()}}};
       CS#chain_r.read_only_p ->
            %% If we're in read-only mode, we're probably having our role in
            %% the chain reshuffled, so acks from an unexpected place isn't an
            %% error (and perhaps isn't even worth commenting about at the
            %% INFO level).
            ?E_INFO("~p: Unknown ack while in read-only mode: ~W, "
                    "my role = ~p, my ds = ~p\n",
                    [State#state.name, Msg, 20,
                     CS#chain_r.role, CS#chain_r.downstream]),
            {noreply, State};
       0 =< Serial, Serial =< 1, CS#chain_r.role == standalone ->
            %% We're probably in a role change: the new downstream has
            %% sent us an ack, but our role hasn't changed yet.
            {noreply, State};
       true ->
            UA1 = CS#chain_r.unknown_acks,
            MAX = 60,
            case orddict:find(BrickName, UA1) of
                {ok, Num} when Num rem MAX == (MAX-1) ->
                    ?E_ERROR("{~p,~p}: Unknown ack: ~W, my role = ~p, my ds = ~p\n",
                             [State#state.name, self(), Msg, 20,
                              CS#chain_r.role, CS#chain_r.downstream]);
                _ ->
                    ok
            end,
            UA2 = orddict:update_counter(BrickName, 1, UA1),
            {noreply, State#state{chainstate = CS#chain_r{unknown_acks = UA2}}}
    end;
handle_cast({ch_repair, Serial, RepairList} = RepairMsg, State) ->
    %% If repair message is lost, it will be resent, to use arity-3 flavor.
    exit_if_bad_serial_from_upstream(Serial, RepairMsg, State),
    ?DBG_REPAIRx({State#state.name, RepairMsg}),
    brick_mboxmon:mark_self_repairing(),
    RepairOverloadP = lookup_repair_overload_key(State#state.throttle_tab),
    if RepairOverloadP ->
            case chain_set_my_repair_state(State, repair_overload) of
                {ok, NewState} ->
                    {noreply, NewState};
                _ ->
                    {noreply, State}
            end;
       (State#state.chainstate)#chain_r.my_repair_state == repair_overload ->
            CS = State#state.chainstate,
            send_repair_ack(CS, repair_overload, State#state.name, 0, 0),
            {noreply, State};
       (State#state.chainstate)#chain_r.my_repair_state /= ok ->
            NewState = chain_do_repair(Serial, RepairList, State),
            {noreply, NewState};
       true ->
            ?E_ERROR("Received repair message when "
                     "in wrong state, Msg = ~W\n, me = ~p ~p, state ~p",
                     [RepairMsg, 20, State#state.name, node(), (State#state.chainstate)#chain_r.my_repair_state]),
            {noreply, State}
    end;
handle_cast({ch_repair_diff_round1, Serial, RepairList} = RepairMsg, State) ->
    %% If repair message is lost, it will be resent, to use arity-3 flavor.
    exit_if_bad_serial_from_upstream(Serial, RepairMsg, State),
    ?DBG_REPAIRx({State#state.name, RepairMsg}),
    brick_mboxmon:mark_self_repairing(),
    RepairOverloadP = lookup_repair_overload_key(State#state.throttle_tab),
    if RepairOverloadP ->
            case chain_set_my_repair_state(State, repair_overload) of
                {ok, NewState} ->
                    {noreply, NewState};
                _ ->
                    {noreply, State}
            end;
       (State#state.chainstate)#chain_r.my_repair_state == repair_overload ->
            CS = State#state.chainstate,
            send_repair_ack(CS, repair_overload, State#state.name, 0, 0),
            {noreply, State};
       (State#state.chainstate)#chain_r.my_repair_state /= ok ->
            NewState = chain_do_repair_diff_round1(Serial, RepairList, State),
            {noreply, NewState};
       true ->
            ?E_ERROR("Received repair message when "
                     "in wrong state, Msg = ~W\n, me = ~p ~p, state ~p",
                     [RepairMsg, 20, State#state.name, node(), (State#state.chainstate)#chain_r.my_repair_state]),
            {noreply, State}
    end;
handle_cast({ch_repair_diff_round1_ack, Serial, _BrickName, _Node, Unknown, Ds} = RepairMsg, State) ->
    %% TODO augment message to include sender's name & node, add checking.
    ?DBG_REPAIRx({State#state.name, RepairMsg}),
    CS = State#state.chainstate,
    R = CS#chain_r.ds_repair_state,
    if not is_record(R, repair_r) ->
            {noreply, State};
       Serial == repair_overload ->
            ?E_INFO("~s: downstream/repairing brick is overloaded, "
                    "interrupting repair process\n", [State#state.name]),
            {noreply, State};
       Serial /= 0, Serial /= R#repair_r.last_repair_serial ->
            {stop, {bad_repair_ack_from_downstream,
                    {got, Serial}, {last, R#repair_r.last_repair_serial}},
             State};
       Unknown == [] ->
            NewState = send_diff_round2(Serial, Unknown, Ds, State),
            NewCS = NewState#state.chainstate,
            NewR = (NewCS#chain_r.ds_repair_state)#repair_r{last_ack = now()},
            {noreply, NewState#state{chainstate =
                                     NewCS#chain_r{ds_repair_state = NewR}}};
       true ->
            spawn_val_prime_worker_for_repair(Serial, Unknown, Ds, State),
            {noreply, State}
    end;
handle_cast({ch_repair_diff_round2, Serial, RepairList, Ds} = RepairMsg, State) ->
    %% If repair message is lost, it will be resent, to use arity-3 flavor.
    exit_if_bad_serial_from_upstream(Serial, RepairMsg, State),
    ?DBG_REPAIRx({State#state.name, RepairMsg}),

    NewState = chain_do_repair_diff_round2(Serial, RepairList, Ds, State),
    {noreply, NewState};
handle_cast({ch_repair_ack, Serial, BrickName, Node,
             Inserted, Deleted} = _Msg, State) ->
    CS = State#state.chainstate,
    R = CS#chain_r.ds_repair_state,
    if not is_record(R, repair_r) ->
            {noreply, State};
        Serial /= 0, Serial /= R#repair_r.last_repair_serial ->
            {stop, {bad_repair_ack_from_downstream,
                    {got, Serial}, {last, R#repair_r.last_repair_serial}},
             State};
       {BrickName, Node} /= CS#chain_r.downstream ->
            ?E_ERROR("~s: ch_repair_ack from ~p ~p, my downstream is ~p\n",
                    [State#state.name, BrickName, Node, CS#chain_r.downstream]),
            {noreply, State};
       true ->
            if R#repair_r.key == ?BRICK__GET_MANY_LAST ->
                    RepairTime = timer:now_diff(now(), R#repair_r.started),
                    Is = R#repair_r.repair_inserts + Inserted,
                    Ds = R#repair_r.repair_deletes + Deleted,
                    ?E_INFO(
                      "Repair finished between ~p -> ~p\n"
                      "\tTime:    ~.2f sec\n"
                      "\tRounds:  ~p\n"
                      "\tInserts: ~p\n"
                      "\tDeletes: ~p\n",
                      [{State#state.name, node()},
                       CS#chain_r.downstream,
                       RepairTime / 1000000,
                       if not is_integer(R#repair_r.final_repair_serial) ->
                               0;
                          true ->
                               R#repair_r.final_repair_serial -
                                   R#repair_r.started_serial
                       end,
                       Is, Ds]),
                    CS2 = CS#chain_r{ds_repair_state = ok,
                                     down_acked = Serial,
                                     last_ack = now()},
                    {noreply, State#state{chainstate = CS2}};
               true ->
                    NewState = send_next_repair(State),
                    CS2 = NewState#state.chainstate,
                    R2 = CS2#chain_r.ds_repair_state,
                    Dels = R2#repair_r.repair_deletes,
                    Ins = R2#repair_r.repair_inserts,
                    NewR2 = R2#repair_r{repair_deletes = Dels + Deleted,
                                        repair_inserts = Ins + Inserted,
                                        last_ack = now()},
                    {noreply, NewState#state{chainstate =
                                         CS2#chain_r{ds_repair_state = NewR2,
                                                     down_acked = Serial,
                                                     last_ack = now()}}}
            end
    end;
handle_cast({ch_repair_finished, Brick, Node, Checkpoint_p, NumKeys}=RepairMsg,
            State) ->
    CS = State#state.chainstate,
    ?DBG_REPAIRx({State#state.name, RepairMsg}),
    brick_mboxmon:mark_self_done_repairing(),
    %% Sanity
    true = ({repair_finished, CS#chain_r.upstream} ==
            {repair_finished, {Brick, Node}}),

    if (State#state.chainstate)#chain_r.my_repair_state /= ok ->
            LastKey = case CS#chain_r.my_repair_state of
                          {last_key, Key} -> Key;
                          pre_init        -> ?BRICK__GET_MANY_FIRST
                      end,
            ?E_INFO("~s: got ch_repair_finished, calling "
                    "bcb_delete_remaining_keys with key ~p / ~p\n",
                    [State#state.name, CS#chain_r.my_repair_state, LastKey]),
            {ImplMod, ImplState} = impl_details(State),
            {Deletes, ImplState2} =
                ImplMod:bcb_delete_remaining_keys(LastKey, ImplState),
            {MyNumKeys, _} = get_implementation_ets_info(State),
            if Checkpoint_p == true ->
                    ?E_ERROR("INFO: ~p: Chain repair finished, upstream brick "
                             "is checkpointing, upstream keys = ~p, "
                             "my keys = ~p\n",
                             [State#state.name, NumKeys, MyNumKeys]);
               MyNumKeys /= NumKeys ->
                    ?E_ERROR("INFO ~p: Repair keys difference, "
                             "probably due to uncommitted updates: "
                             "upstream keys = ~p, my keys = ~p\n",
                             [State#state.name, NumKeys, MyNumKeys]),
                    %% Don't exit here, it's too difficult to count keys
                    %% exactly on both upstream & downstream bricks.
                    %% TODO: Try to calculate exact counts.
                    ok;
               true ->
                    ?E_INFO("~p: ~p keys after repair\n",
                            [State#state.name, MyNumKeys])
            end,
            case proplists:get_value(repair_finished_sleep, CS#chain_r.proplist) of
                undefined ->
                    ok;
                MS when is_integer(MS) ->
                    error_logger:info_msg("DEBUG: ~p: repair finished, "
                                          "sleeping for ~p ms\n",
                                          [State#state.name, MS]),
                    timer:sleep(MS)
            end,
            catch ImplMod:bcb_force_flush(ImplState2),
            send_repair_ack(CS, 0, State#state.name, 0, Deletes),
            %% delme?? catch brick_itimer:cancel(CS#chain_r.clock_tref),
            false = size(ImplState2) == record_info(size, state), %sanity
            OkTime = now(),
            delete_repair_overload_key(State#state.name),
            brick_pingee:set_chain_ok_time(State#state.name, node(), OkTime),
            CS2 = set_my_repair_member(State, CS, ok),
            NewState = State#state{chainstate =
                                   CS2#chain_r{my_repair_ok_time = OkTime,
                                              repair_last_time = {0,0,0},
                                              up_serial = 0,
                                              down_serial = -1,
                                              down_acked = ?NEG_LIMBO_SEQNUM,
                                              last_ack = now()},
                                   impl_state = ImplState2},
            {noreply, NewState};
       true ->
            ?E_ERROR("~s: Received repair finished message when "
                     "in wrong state (~p), Msg = ~W\n",
                     [State#state.name,
                      (State#state.chainstate)#chain_r.my_repair_state,
                      RepairMsg, 20]),
            {noreply, State}
    end;
handle_cast({forward_call, _Call, _From, Hops} = _QQQ, State)
  when Hops > 18 ->
    %% TODO: Add a unit test case to check that this silly thing even works.
    %% TODO: Make the number of hops configurable.
    %% TODO: Make the number of hops less reliant on the internals
    %%       of the 'do' op and its DoFlags proplist.
    %% TODO: Issue an info message if we're here?
    ?DBG_CHAINx({State#state.name, _QQQ}),
    {noreply, State};
handle_cast({forward_call, Call, From, ____Hops} = _QQQ, State) ->
    ?DBG_CHAINx({State#state.name, _QQQ}),
    case handle_call(Call, From, State) of
        {reply, Reply, NewState} ->
            gen_server:reply(From, Reply),
            {noreply, NewState};
        Other ->
            Other
    end;
handle_cast({sync_down_the_chain, Options, From, ReplyTag, Acc}, State) ->
    CS = State#state.chainstate,
    StopOffTail_p = proplists:get_value(stop_at_official_tail, Options, false),
    {ImplMod, ImplState} = impl_details(State),
    catch ImplMod:bcb_force_flush(ImplState),
    NewAcc = [{State#state.name,node()}|Acc],
    if CS#chain_r.official_tail == true -> self() ! chain_admin_periodic;
       true                             -> ok
    end,
    if CS#chain_r.downstream == undefined ->
            From ! {ReplyTag, lists:reverse(NewAcc)};
       CS#chain_r.downstream /= undefined,
       StopOffTail_p, CS#chain_r.official_tail == true ->
            From ! {ReplyTag, lists:reverse(NewAcc)};
       true ->
            gen_server:cast(CS#chain_r.downstream,
                        {sync_down_the_chain, Options, From, ReplyTag, NewAcc})
    end,
    {noreply, State};
handle_cast({ch_sweep_from_other, ChainHeadPid, ChainName, Thisdo_Mods,
             LastKey}= _Msg, State) ->
    ?DBG_MIGRATEx({ch_sweep_from_other, ChainName, State#state.name, Thisdo_Mods, LastKey}),
    CS = State#state.chainstate,
    if CS#chain_r.role == standalone
       orelse
       (CS#chain_r.role == chain_member andalso
        CS#chain_r.upstream   == undefined andalso
        CS#chain_r.downstream /= undefined) ->
            if CS#chain_r.read_only_p ->
                    ?E_INFO("~s: in read-only mode, got sweep from ~p\n",
                            [State#state.name, ChainName]),
                    {noreply, State};
               true ->
                    do_ch_sweep_from_other(ChainHeadPid, ChainName, Thisdo_Mods,
                                           LastKey, State)
            end;
       true ->
            %% TODO: So, what happens when a chain's order is being
            %%       rearranged by the admin server because the last
            %%       brick is now up (chain state: degraded -> ok)
            %%       and this message is received by the node that
            %%       used to be head but no longer is?
            ?E_INFO("ch_sweep_from_other: ~p got sweep while "
                     "role is: ~p: ~p ~p ~p, dropping it\n",
                     [State#state.name, CS#chain_r.role,
                      CS#chain_r.upstream, CS#chain_r.downstream,
                      CS#chain_r.official_tail]),
            {noreply, State}
    end;
handle_cast({sweep_phase1_done, Key} = _Msg, State) ->
    ?DBG_MIGRATEx({sweep_phase1_done, State#state.name, Key}),
    Sw = State#state.sweepstate,
    if not is_record(Sw, sweep_r) ->
            ?E_ERROR("~p: got {sweep_phase1_done, ~p}: Sw ~p\n",
                     [State#state.name, Key, Sw]),
            {noreply, State};
       true ->
           if Key /= Sw#sweep_r.sweep_key ->
                   ?E_INFO("sweep_phase1_done: ~s expected ~W got ~W\n",
                         [State#state.name, Sw#sweep_r.sweep_key, 60, Key, 60]),
                   {noreply, State};
              true ->
                   NewState = kick_next_sweep_do_phase2(Sw, Key, State),
                   {noreply, NewState}
           end
    end;
handle_cast({sweep_phase2_done, Key, PropList} = _Msg, State) ->
    ?DBG_MIGRATEx({sweep_phase2_done, State#state.name, Key, PropList}),
    Sw = State#state.sweepstate,
    %% Sanity checking
    if not is_record(Sw, sweep_r) ->
            ?E_INFO(
              "~w got sweep_phase2_done for ~p my Sw is ~p\n",
              [State#state.name, Key, Sw]),
            {noreply, State};
       Sw#sweep_r.stage == top ->
            ?E_WARNING(
              "TODO: 1: got sweep_phase2_done for ~p, mine are ~p ~p\n",
              [Key, Sw#sweep_r.done_sweep_key, Sw#sweep_r.sweep_key]),
            {noreply, State};
       is_tuple(Sw#sweep_r.stage),
       element(1, Sw#sweep_r.stage) == done ->
            %% The message we've received is a repeat, probably
            %% because of a role change which forced some un-acked
            %% serials downstream a second time ... so we get the
            %% reply (via cast!) a second time.  We can ignore this.
            {noreply, State};
       true ->
            if Key =:= Sw#sweep_r.sweep_key ->
                    do_sweep_phase2_done(Key, PropList, State);
               true ->
                    %%?E_INFO(
                    %%   "migration sweep: chain ~p got sweep_phase2_done "
                    %%   "for ~p, mine are ~p ~p\n",
                    %%   [Sw#sweep_r.chain_name, Key, Sw#sweep_r.done_sweep_key, Sw#sweep_r.sweep_key]),
                    {noreply, State}
            end
    end;
%% This is an "old" style ch_log_replay message that needs conversion to
%% a "new" style message.
handle_cast({ch_log_replay, UpstreamBrick, Serial, Thisdo_Mods, From,
             Reply}, State) ->
    handle_cast({ch_log_replay_v2, UpstreamBrick, Serial, Thisdo_Mods, From,
                 Reply, undefined}, State);
handle_cast(Msg, State) ->
    {ImplMod, ImplState} = impl_details(State),
    case ImplMod:handle_cast(Msg, ImplState) of
        {noreply, S} ->
            false = size(S) == record_info(size, state), %sanity
            {noreply, State#state{impl_state = S}};
        {noreply, S, Timeout} ->
            false = size(S) == record_info(size, state), %sanity
            {noreply, State#state{impl_state = S}, Timeout};
        {stop, Reason, S} ->
            false = size(S) == record_info(size, state), %sanity
            {stop, Reason, State#state{impl_state = S}}
    end.

%%----------------------------------------------------------------------
%% Func: handle_info/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% @doc gen_server handle_info/2 callback.
%%----------------------------------------------------------------------
handle_info(chain_admin_periodic, State) ->
    CS = State#state.chainstate,
    case CS#chain_r.my_repair_state of
        {last_key, _} ->
            Now = now(),
            case timer:now_diff(Now, CS#chain_r.repair_last_time) of
                N when (N > 15*1000*1000) andalso (N < 16*1100*1100)
                  orelse
                       (N > 30*1000*1000) andalso (N < 31*1100*1100)
                  orelse
                       (N > 45*1000*1000) andalso (N < 46*1100*1100)
                  orelse
                       (N > 60*1000*1000) andalso
                       ((N div 1000000) rem 60 == 0) ->
                    ?E_INFO("~p: repair_last_time diff is ~w seconds\n",
                           [State#state.name, N div 1000000]);
                N when N > 10*60*1000*1000 ->      % TODO make configurable
                    exit({repair_timeout, CS#chain_r.repair_last_time, Now});
                _ ->
                    ok
            end;
        _ ->
            ok
    end,
    {noreply, get_do_op_too_old_timeout(do_chain_admin_periodic(State))};
handle_info(kick_next_sweep, State) ->
    eat_kicks(),
    NewState = do_kick_next_sweep(State),
    {noreply, NewState};
handle_info({priming_value_blobs_done_for_sweep, LastKey}, State) ->
    Sw = State#state.sweepstate,
    if Sw#sweep_r.val_prime_lastkey == LastKey ->
            %% Do *not* undefine val_prime_lastkey here.  We go to
            %% kick_next_sweep_do_phase2() because we may have had a
            %% role change while the squidflash-style val blob priming
            %% was in progress.  Or someone may have inserted a new
            %% key in the middle of the range we're working on.  If we
            %% short-circuit to some lower-level function, who knows
            %% what kind of trouble we might get into?  It's more
            %% inefficient, but it's also safer.
            ?DBG_MIGRATEx({kick__phase2_prime_done, State#state.name, LastKey}),
            NewState = State#state{sweepstate =
                                  Sw#sweep_r{stage = {notify_down_old, now()}}},
            {noreply, kick_next_sweep_do_phase2(Sw, LastKey, NewState)};
       true ->
            ?E_ERROR("~p: priming_value_blobs_done_for_sweep: expected ~p, got ~p\n",
                     [Sw#sweep_r.val_prime_lastkey, LastKey]),
            {noreply, State}
    end;
handle_info({priming_value_blobs_done_for_repair, Serial, Unknown, Ds}, State) ->
    NewState = send_diff_round2(Serial, Unknown, Ds, State),
    {noreply, NewState};
handle_info({retry_all_dirty_send_next_repair, RetryRef}, State) ->
    if RetryRef == State#state.retry_send_repair ->
            {noreply,
             send_next_repair(State#state{retry_send_repair = undefined})};
       true ->
            ?E_ERROR("retry_send_next_repair: ~p got bogus ref, ignoring\n",
                     [State#state.name]),
            {noreply, State}
    end;
handle_info({after_delay_send_next_repair, RepairStartTime}, State) ->
    CS = State#state.chainstate,
    R = CS#chain_r.ds_repair_state,
    if R#repair_r.started == RepairStartTime ->
            ?E_INFO("~s: after_delay_send_next_repair\n", [State#state.name]),
            {noreply, send_next_repair(State)};
       true ->
            %% We're no longer repairing anybody, or there's been a
            %% race and we're repairing someone but it isn't the same
            %% repair session!
            {noreply, State}
    end;
handle_info({storage_layer_init_finished, Name, ZeroDiskErrorsP}, State)
  when Name == State#state.name ->
    CS = State#state.chainstate,
    MyRepairState = if ZeroDiskErrorsP == true  -> pre_init;
                       ZeroDiskErrorsP == false -> disk_error
                    end,
    {ImplMod, ImplState} = impl_details(State),
    {WaitingP, _Waiters} =
        case ImplMod:bcb_get_metadata(sweep_checkpoint, ImplState) of
            [] ->
                resubmit_all_queued_update_ops(State#state.name,
                                               CS#chain_r.mig_start_waiters),
                {false, []};
            [_] ->
                {true, CS#chain_r.mig_start_waiters}
        end,
    CS2 = set_my_repair_member(State, CS, MyRepairState),
    {noreply, State#state{chainstate =
                          CS2#chain_r{waiting_for_mig_start_p = WaitingP,
                                      mig_start_waiters       = []}}};
handle_info(check_expiry = Msg, State) ->
    %% This is a brick_ets message, but we need to intercept it here first.
    case role_is_headlike_p(State) of
        true ->
            {ImplMod, ImplState} = impl_details(State),
            {noreply, ImplState2} = ImplMod:handle_info(Msg, ImplState),
            {noreply, State#state{impl_state = ImplState2}};
        false ->
            {noreply, State}
    end;
handle_info({'DOWN', MRef, _Type, _Pid, _Info} = Msg, State) ->
    CS = State#state.chainstate,
    if CS#chain_r.upstream_monitor == MRef ->
            ?E_ERROR("Brick ~p got monitor 'DOWN' notice for upstream brick\n",
                     [State#state.name]),
            CS2 = CS#chain_r{upstream_monitor_down_p = true},
            {noreply, State#state{chainstate = CS2}};
       true ->
            ?E_ERROR("Brick ~p got unknown monitor 'DOWN' notice: ~p\n",
                     [State#state.name, Msg]),
            {noreply, State}
    end;
handle_info(Msg, State) ->
    {ImplMod, ImplState} = impl_details(State),
    false = size(ImplState) == record_info(size, state), %sanity
    case ImplMod:handle_info(Msg, ImplState) of
        {up1, ToDos, OrigReturn} ->
            State2 = do_chain_todos_iff_empty_log_q(ToDos, State),
            calc_original_return(OrigReturn, State2);
        {up1_sync_done, LastLogSerial, ToDos2, ImplState2} ->
            %% OLD style: DBG: ok = bz_debug_check_todos(ToDos2, State),
            %% TODO: Determine if this LQI management stuff is really
            %%       necessary here, or if it's a relic of the ets/server
            %%       source code split.
            %% Partial answer: It's still necessary, for now.  In the event
            %% of a middle brick getting updates and passing them even
            %% further downstream, the log_q at this level is used.
            %% I don't think that's ideal, but it's what we've got right now.
            %% E.g. for a middle brick that replays a log:
            %%      ToDos1 = [], ToDos2 = [Stuff].
            %%
            {LQI_List, State2} =
                pull_items_out_of_logging_queue(LastLogSerial, State),
            ?DBG_TLOGx({up1_sync_done, State#state.name, lastlogserial, LastLogSerial}),
            ?DBG_TLOGx({up1_sync_done, State#state.name, logging_op_q, State#state.logging_op_q}),
            %%
            %% Logically speaking, the items in LQI_List are serials that
            %% are smaller than the ones we've just received from the
            %% {up1_sync_done, ...} response from our lower layer.
            %% The number suffix, ToDos1 & ToDos2, is the order in which
            %% we need to send *downstream*.
            %%
            ToDos1 = [{chain_send_downstream, LQI#log_q.logging_serial,
                       LQI#log_q.doflags, LQI#log_q.from,
                       LQI#log_q.reply, LQI#log_q.thisdo_mods}
                      || LQI <- LQI_List],
            ?DBG_TLOGx({up1_sync_done, State#state.name, todos1, ToDos1}),
            ?DBG_TLOGx({up1_sync_done, State#state.name, todos2, ToDos2}),
            false = size(ImplState2) == record_info(size, state), %sanity
            ImplState3 =
                lists:foldl(
                  fun(D_Op_L, St) ->
                          ?DBG_TLOGx(
                            {handle_info, calling, bcb_map_mods_to_storage}),
                          ImplMod:bcb_map_mods_to_storage(D_Op_L, St)
                  end, ImplState2,
                  [DoOpList || #log_q{thisdo_mods = DoOpList} <- LQI_List]),
            %% TODO: The call below to bcb_retry_dirty_key_ops is a kludge,
            %% but I can't find a way to avoid it.  If you have a situation
            %% like Race #1 in brick_test0:chain_t61(), if a sweep arrives
            %% at the right time, it's possible to have a do op get stuck in
            %% the dirty queue.  The call to bcb_map_mods_to_storage() above
            %% may have un-dirtied the key in question, but there may not be
            %% another log sync event to jiggle it out of the dirty queue.
            %% So we'll try to jiggle it here....
            {ImplState4, ToDos3} = ImplMod:bcb_retry_dirty_key_ops(ImplState3),
            ?DBG_TLOGx({up1_sync_done, State#state.name, todos3, ToDos3}),
            %%
            %% NOTE: This sort is a big concession that log event ordering
            %%       in this implementation is difficult.  Because we punt
            %%       here, we're assuming that the lowest level code that's
            %%       assigning serial numbers is 100% trustworthy.
            BigList = lists:sort(
                        fun(A, B) -> element(2, A) < element(2, B) end,
                        ToDos1 ++ ToDos2 ++ ToDos3),
            ?DBG_TLOGx({up1_sync_done, State#state.name, biglist, BigList}),
            State3 = do_chain_todos(BigList,
                                    State2#state{impl_state = ImplState4}),
            {noreply, State3};
        OrigReturn ->
            calc_original_return(OrigReturn, State)
    end.

%%----------------------------------------------------------------------
%% Func: terminate/2
%% Purpose: Shutdown the server
%% Returns: any (ignored by gen_server)
%% @doc gen_server terminate/2 callback.
%%----------------------------------------------------------------------
terminate(Reason, State) ->
    {ImplMod, ImplState} = impl_details(State),
    ImplMod:terminate(Reason, ImplState),
    catch brick_pingee:stop(State#state.name, node()).

%%----------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% @doc gen_server code_change/3 callback.
%%----------------------------------------------------------------------
code_change(OldVsn, State, Extra) ->
    {ImplMod, ImplState} = impl_details(State),
    ImplMod:code_change(OldVsn, ImplState, Extra).

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

%% @spec () -> do_op()
%% @doc Create a "start of transaction" do op.
%%
%% QQQ: It isn't clear to me that putting a 'txn' atom at the beginning of
%% the op list is a good idea ... but that's what I'm going with right now.

make_txn() ->
    txn.

%% @spec () -> do_op()
%% @doc Create a "next op is silent" do op.
%%
%% I don't expect that this op should be used by app coders, but it is
%% legit to use internally, e.g. for quota implementation.

make_next_op_is_silent() ->
    next_op_is_silent.

%% @spec (term(), term()) -> do_op()
%% @equiv make_add(Key, Value, 0, [])

-spec make_add(key(), val()) -> add().
make_add(Key, Value) ->
    make_add(Key, Value, 0, []).

%% @spec (term(), term(), integer(), prop_list()) -> do_op()
%% @doc Create an "add" do op (see encode_op_flags() for valid flags).

-spec make_add(key(), val(), exp_time(), prop_list()) -> add().
make_add(Key, Value, ExpTime, Flags) ->
    make_op5(add, Key, Value, ExpTime, Flags).

%% @spec (term(), integer(), term(), integer(), prop_list()) -> do_op()
%% @doc Create an "add" do op (see encode_op_flags() for valid flags).

-spec make_add(key(), integer(), val(), exp_time(), prop_list()) -> add().
make_add(Key, TStamp, Value, ExpTime, Flags) ->
    make_op6(add, Key, TStamp, Value, ExpTime, Flags).

%% @spec (term(), term()) -> do_op()
%% @equiv make_replace(Key, Value, 0, [])

-spec make_replace(key(), val() | ?VALUE_REMAINS_CONSTANT) -> replace().
make_replace(Key, Value) ->
    make_replace(Key, Value, 0, []).

%% @spec (term(), term(), integer(), prop_list()) -> do_op()
%% @doc Create an "replace" do op (see encode_op_flags() for valid flags).

-spec make_replace(key(), val() | ?VALUE_REMAINS_CONSTANT, exp_time(), flags_list()) -> replace().
make_replace(Key, Value, ExpTime, Flags) ->
    make_op5(replace, Key, Value, ExpTime, Flags).

%% @spec (term(), integer(), term(), integer(), prop_list()) -> do_op()
%% @doc Create an "replace" do op (see encode_op_flags() for valid flags).

-spec make_replace(key(), integer(), val() | ?VALUE_REMAINS_CONSTANT, exp_time(), flags_list()) -> replace().
make_replace(Key, TStamp, Value, ExpTime, Flags) ->
    make_op6(replace, Key, TStamp, Value, ExpTime, Flags).

%% @spec (term(), term()) -> do_op()
%% @equiv make_set(Key, Value, 0, [])

-spec make_set(key(), val()) -> 'set__'().
make_set(Key, Value) ->
    make_set(Key, Value, 0, []).

%% @spec (term(), term(), integer(), prop_list()) -> do_op()
%% @doc Create an "set" do op (see encode_op_flags() for valid flags).

-spec make_set(key(), val(), exp_time(), prop_list()) -> 'set__'().
make_set(Key, Value, ExpTime, Flags) ->
    make_op5(set, Key, Value, ExpTime, Flags).

%% @spec (term(), integer(), term(), integer(), prop_list()) -> do_op()
%% @doc Create an "set" do op (see encode_op_flags() for valid flags).

-spec make_set(key(), integer(), val(), exp_time(), prop_list()) -> 'set__'().
make_set(Key, TStamp, Value, ExpTime, Flags) ->
    make_op6(set, Key, TStamp, Value, ExpTime, Flags).

%% @spec (term()) -> do_op()
%% @equiv make_get(Key, [])

-spec make_get(key()) -> get().
make_get(Key) ->
    make_get(Key, []).

%% @spec (term(), prop_list()) -> do_op()
%% @doc Create an "get" do op (see encode_op_flags() for valid flags).

-spec make_get(key(), prop_list()) -> get().
make_get(Key, Flags) ->
    make_op2(get, Key, Flags).

%% @spec (term()) -> do_op()
%% @equiv make_delete(Key, [])

-spec make_delete(key()) -> delete().
make_delete(Key) ->
    make_delete(Key, []).

%% @spec (term(), prop_list()) -> do_op()
%% @doc Create an "delete" do op (see encode_op_flags() for valid flags).

-spec make_delete(key(), prop_list()) -> delete().
make_delete(Key, Flags) ->
    make_op2(delete, Key, Flags).

%% @spec (term(), integer()) -> do_op()
%% @equiv make_get_many(Key, MaxNum, [])

-spec make_get_many(key() | ?BRICK__GET_MANY_FIRST, integer()) -> get_many().
make_get_many(Key, MaxNum) ->
    make_get_many(Key, MaxNum, []).

%% @spec (term(), integer(), prop_list()) -> do_op()
%% @doc Create an "get_many" do op (see encode_op_flags() for valid flags).

-spec make_get_many(key() | ?BRICK__GET_MANY_FIRST, integer(), flags_list_many()) -> get_many().
make_get_many(Key, MaxNum, Flags) ->
    make_op2(get_many, Key, [{max_num, MaxNum}|Flags]).

%% @spec (term(), function()) -> do_op()
%% @doc Make a "server-side fun" do op.

-spec make_ssf(key(), function()) ->
                           {ssf, binary(), flags_list()}.
make_ssf(Key, Fun) when is_function(Fun, 4) ->
    make_op2(ssf, Key, [Fun]).

%% @spec (term()) -> do_op()
%% @doc Make a "get_quota" do op via ssf.

-spec make_get_quota(key()) ->
                            {ssf, binary(), flags_list()}.
make_get_quota(Key) ->
    %% ssf_check_quota_root() won't find the quota root if Key =:= QRoot.
    %% So we tack on one extra byte so that the check can strip it off again.
    %% If the key isn't a binary or list-of-bytes, use Key as-is.
    Key2 = list_to_binary([Key, $z]),
    F = fun(Key0, _DoOp, _DoFlags, S) ->
                case ?MODULE:ssf_check_quota_root(Key0, S) of
                    key_not_exist ->
                        {ok, [key_not_exist]};
                    {ok, QRoot, {_K, TS, Val, Exp, Fs}} ->
                        %% We'll use a custom return value, to avoid confusion
                        %% with a get() return like {ok, ...}
                        {ok, [{quota_root, QRoot, TS, Val, Exp, Fs}]}
                end
        end,
    make_ssf(Key2, F).

-spec make_set_quota(key(), integer(), integer()) ->
                            {ssf, binary(), flags_list()}.
make_set_quota(Key, Items, Bytes) ->
    Key2 = gmt_util:bin_ify(Key),
    F = fun(Key0, _DoOp, _DoFlags, S) ->
                Op = case ssf_peek(Key0, false, S) of
                         [] ->
                             make_add(Key0, <<>>, 0,
                                      [{quota_items, Items},
                                       {quota_bytes, Bytes},
                                       {quota_items_used, 0},
                                       {quota_bytes_used, 0}]);
                         [{_Key, TS, _Val, _Exp, KeyFlags}] ->
                             ItemsU = proplists:get_value(quota_items_used,
                                                          KeyFlags, 0),
                             BytesU = proplists:get_value(quota_bytes_used,
                                                          KeyFlags, 0),
                             Val = ?VALUE_REMAINS_CONSTANT,
                             make_replace(Key0, Val, 0,
                                          [{testset, TS},
                                           {quota_items, Items},
                                           {quota_bytes, Bytes},
                                           {quota_items_used, ItemsU},
                                           {quota_bytes_used, BytesU}])
                     end,
                {ok, [Op]}
        end,
    make_ssf(Key2, F).

-spec make_resum_quota(key()) ->
                            {ssf, binary(), flags_list()}.
make_resum_quota(KeyArg) ->
    Key = gmt_util:bin_ify(KeyArg),
    Key2 = list_to_binary([Key, $z]),
    PeekFlags = [{binary_prefix, Key}],
    Fsum = fun(Key0, S) ->
                   gmt_loop:do_while(
                     fun({Is, Bs, K}) ->
                             {ManyL, MoreP} = ?MODULE:ssf_peek_many(
                                                 K, 1, PeekFlags, S),
                             {IsIs, BsBs, LastK} =
                                 lists:foldl(
                                   fun({Kk, _TS,_Val,_Exp, Fs}, {Ii, Bb, _}) ->
                                           B2 = proplists:get_value(val_len,
                                                                    Fs),
                                           {Ii + 1, Bb + B2, Kk}
                                   end, {0, 0, placeholder}, ManyL),
                             {MoreP, {Is + IsIs, Bs + BsBs, LastK}}
                     end, {0, 0, Key0})
           end,
    F = fun(Key0, _DoOp, _DoFlags, S) ->
                case ?MODULE:ssf_check_quota_root(Key0, S) of
                    {ok, QRoot, {_K, TS, _Val, _Exp, QFlags}}
                      when QRoot == Key ->      % not Key0
                        Items = proplists:get_value(quota_items, QFlags),
                        Bytes = proplists:get_value(quota_bytes, QFlags),
                        {ItemsU, BytesU, _} = Fsum(Key, S), % not Key0
                        Val = ?VALUE_REMAINS_CONSTANT,
                        {ok, [make_replace(Key, Val, 0, % not Key0
                                     [{testset, TS},
                                      {quota_items, Items},
                                      {quota_bytes, Bytes},
                                      {quota_items_used, ItemsU},
                                      {quota_bytes_used, BytesU}])]};
                    _X ->
                        {ok, [key_not_exist]}
                end
        end,
    make_ssf(Key2, F).

%% @spec (atom(), term(), prop_list()) -> do_op()
%% @doc Create a 2-argument do op (see encode_op_flags() for valid flags).

-spec make_op2(atom(), key() | ?BRICK__GET_MANY_FIRST, flags_or_fun_list()) ->
                      {atom(), binary(), flags_or_fun_list()}.
make_op2(OpName, Key, Flags) ->
    OpKey = if Key == ?BRICK__GET_MANY_FIRST -> Key;
               true                          -> gmt_util:bin_ify(Key)
            end,
    {OpName, OpKey, encode_op_flags(Flags)}.

%% @spec (atom(), term(), term(), integer(), prop_list()) -> do_op()
%% @doc Create a 5-argument do op (see encode_op_flags() for valid flags).

-spec make_op5(atom(), key(), val() | ?VALUE_REMAINS_CONSTANT, exp_time(), flags_or_fun_list()) ->
                      {atom(), binary(), integer(), val(), exp_time(), flags_or_fun_list()}.
make_op5(OpName, Key, Value0, ExpTime, Flags) ->
    TStamp = make_timestamp(),
    EFlags = encode_op_flags(Flags),
    Value = check_value_type(Value0),
    {OpName, gmt_util:bin_ify(Key), TStamp, Value, ExpTime, EFlags}.

%% @spec (atom(), term(), integer(), term(), integer(), prop_list()) -> do_op()
%% @doc Create a 6-argument do op (see encode_op_flags() for valid flags).

-spec make_op6(atom(), key(), integer(), val() | ?VALUE_REMAINS_CONSTANT, exp_time(), flags_or_fun_list()) ->
                      {atom(), binary(), integer(), val(), exp_time(), flags_or_fun_list()}.
make_op6(OpName, Key, TStamp, Value0, ExpTime, Flags) ->
    EFlags = encode_op_flags(Flags),
    Value = check_value_type(Value0),
    {OpName, gmt_util:bin_ify(Key), TStamp, Value, ExpTime, EFlags}.

%% @spec(prop_list()) -> prop_list()
%% @doc Verify that a do op's property list is valid, throwing a function
%% clause exception if it is invalid.
%%
%% Valid options that will be interpreted by the receiving brick:
%% <ul>
%% <li> {binary_prefix, binary()} ... binary prefix for get_many op. </li>
%% <li> {max_bytes, integer()} ... the maximum number of bytes of value blobs
%%      for a get_many op (with or without 'witness') to return.
%%      This isn't an absolute limit: {max_bytes, 1} will always return a
%%      single key (if one matches the other criteria). </li>
%% <li> {max_num, integer()} ... the maximum number of keys for a get_many op
%%      to return. </li>
%% <li> {testset, integer()} ... aka "test and set": the key's timestamp value
%%      must match this property's timestamp exactly. </li>
%% <li> witness ... this get/get_many operation is in "witness" mode:
%%      do not return the key's value. </li>
%% <li> must_exist ... the key must exist. </li>.
%% <li> must_not_exist ... the key must not exist. </li>.
%% <li> get_all_attribs ... Get all attributes of a key.  If this option
%%      is not present, then only the value and timestamp attributes will
%%      be returned. </li>.
%% <li> Fun of arity 4 ... used only by internal "ssf" encoding. </li>
%% </ul>
%%
%% NOTE: If the options 'witness' and 'get_all_attribs' are both present
%%       in a 'get' or 'get_many' call, then the tuple returned will be:
%%       {ok, TStamp::integer(), Flags::prop_list()}

encode_op_flags([{_,_}=H|T]) ->
    [H|encode_op_flags(T)];
encode_op_flags([Atom|T]) when is_atom(Atom) ->
    [Atom|encode_op_flags(T)];
encode_op_flags([Fun|T]) when is_function(Fun, 4) ->
    %% This form is used by internal "ssf" encoding.
    [Fun|encode_op_flags(T)];
encode_op_flags([]) ->
    [].

%% @spec (iolist() | VALUE_REMAINS_CONSTANT) ->
%%        iolist() | VALUE_REMAINS_CONSTANT

check_value_type(Val) ->
    case (catch iolist_size(Val)) of
        N when is_integer(N) ->
            Val;
        {'EXIT', _} ->
            case Val of
                ?VALUE_REMAINS_CONSTANT ->
                    Val;
                {?VALUE_SWITCHAROO, _, _} ->
                    Val;
                _ ->
                    exit({?MODULE, ?LINE, val_is, Val})
            end
    end.

%% @spec () -> integer()
%% @doc Create a timestamp based on the current time (erlang:now()).

make_timestamp() ->
    {MSec, Sec, USec} = now(),
    (MSec * 1000000 * 1000000) + (Sec * 1000000) + USec.

%% @spec (integer()) -> {integer(), integer(), integer()}
%% @doc Convert a timestamp from make_timestamp/1 back into erlang:now() format.

make_now(Ts) ->
    MSec = Ts div (1000000 * 1000000),
    MTs = Ts rem (1000000 * 1000000),
    Sec = MTs div 1000000,
    STs = MTs rem 1000000,
    USec = STs,
    {MSec, Sec, USec}.

%% @spec (do_op()) -> integer()
%% @doc Extract a timestamp from a do op, use only for do ops containing
%% a timestamp!

get_op_ts({_, _, TS, _, _}) ->
    TS;
get_op_ts({_, _, TS, _, _, _}) ->
    TS.

%% @spec (do_list(), global_hash()) ->
%%       {list(brick()), list(rw_key_tuple()), read | write}
%% @equiv extract_do_list_keys_find_bricks(DoList, GH, no_state_available)

-spec extract_do_list_keys_find_bricks(do_list(), global_hash_r())
                                      -> {list(brick()), list(rw_key_tuple()), read | write}.
extract_do_list_keys_find_bricks(DoList, GH) ->
    extract_do_list_keys_find_bricks(DoList, GH, no_state_available).

%% @spec (do_list(), global_hash(), state_r()) ->
%%       {list(brick()), list(rw_key_tuple()), read | write}
%% @doc Extract the list of all keys in a do list together with the
%% list of bricks responsible for those keys (ideal length = 1).
%%
%% TODO: For get_many(?BRICK__GET_MANY_FIRST), when a GH is assigned,
%% extract_do_list_keys_find_bricks will probably re-direct us to another
%% brick, and redirection/forwarding is exactly *not* what we want for
%% get_many ops.  We'll insert a hack for the most frequent case, but this
%% thing is broken in the general case.

extract_do_list_keys_find_bricks([{get_many, ?BRICK__GET_MANY_FIRST, _}],
                                 _GH, S)->
    {[{S#state.name, node()}], [{read, <<>>}], read};
extract_do_list_keys_find_bricks(DoList, GH, S) ->
    RdWr_Keys = harvest_do_keys(DoList, S),
    WhereList =
        [brick_hash:key_to_brick(RdRw, K, GH) || {RdRw, K} <- RdWr_Keys],
    ReadOrWrite = case lists:any(fun({write, _}) -> true;
                                    ({read,  _}) -> false
                                 end, RdWr_Keys) of
                      true  -> write;
                      false -> read
                  end,
    {gmt_util:list_unique(WhereList), RdWr_Keys, ReadOrWrite}.

%% @spec (do_list()) -> list()
%% @equiv harvest_do_keys(DoList, no_state_available)

harvest_do_keys(DoList) ->
    harvest_do_keys(DoList, no_state_available).

%% @spec (do_list(), state_r() | term()) -> list()
%% @equiv lists:reverse(harvest_do_keys(DoList, [], S))

harvest_do_keys(DoList, S) ->
    lists:reverse(harvest_do_keys(DoList, [], S)).

%% @spec (do_list(), list(), state_r()) -> list(rw_key_tuple())
%% @doc Harvest/extract the keys from each of the ops in a do list, returning
%% them in <tt>{read|write, term()}</tt> form.

harvest_do_keys([{OpName, Key, _, _, _, _}|T], Acc, S)
  when OpName == add; OpName == replace; OpName == set ->
    harvest_do_keys(T, [{write, Key}|Acc], S);
harvest_do_keys([{delete, Key, _}|T], Acc, S) ->
    harvest_do_keys(T, [{write, Key}|Acc], S);
harvest_do_keys([{get, Key, _}|T], Acc, S) ->
    harvest_do_keys(T, [{read, Key}|Acc], S);
harvest_do_keys([{get_many, Key, _}|T], Acc, S) ->
    harvest_do_keys(T, [{read, Key}|Acc], S);
harvest_do_keys([{ssf, Key, _}|T], Acc, S) ->
    harvest_do_keys(T, [{write, Key}|Acc], S);
harvest_do_keys([_|T], Acc, S) ->
    harvest_do_keys(T, Acc, S);
harvest_do_keys([], Acc, _S) ->
    Acc.

%% Non-quota case.
%% harvest_do_keys_modif(Key, T, Acc, _S) ->
%%     harvest_do_keys(T, [{write, Key}|Acc]).

%% harvest_do_keys_modif(Key, T, Acc, S) ->
%%     case check_key_for_quota_root(Key, S) of
%%      {ok, QuotaRoot} ->
%%          harvest_do_keys(T, [{write, QuotaRoot}, {write, Key}|Acc], S);
%%      not_found ->
%%          harvest_do_keys(T, [{write, Key}|Acc], S)
%%     end.

throttle_tab_name(Brick) ->
    list_to_atom(atom_to_list(Brick) ++ "_throttle").

check_key_for_quota_root(<<>>, _S) ->
    not_found;
check_key_for_quota_root(Key, S) when is_binary(Key), is_record(S, state) ->
    {ImplMod, ImplState} = impl_details(S),
    K0 = gmt_util:bin_ify(Key),                 % Should already be bin!
    %% Remove last byte, in case that byte is a slash.
    Kminus1 = size(K0) - 1,
    <<K:Kminus1/binary, _/binary>> = K0,
    SlashOffsets = find_in_bin_bw(K, $/),
    lists:foldl(
      fun(_, {ok, _, _} = Acc) ->
              Acc;
         (Offset, not_found) ->
              Off1 = Offset + 1,
              <<Prefix:Off1/binary, _/binary>> = Key,
              case ImplMod:bcb_lookup_key(Prefix, false, ImplState) of
                  [Tuple] ->
                      {ok, Prefix, Tuple};
                  [] ->
                      not_found
              end
      end, not_found, SlashOffsets);
check_key_for_quota_root(_Key, _Not_a_state_record) ->
    not_found.

%% bw = backward ... we return list of indexes in reverse
%%                   (right to left) order.

find_in_bin_bw(B, Char) ->
    find_in_bin_bw(B, Char, 0, []).

find_in_bin_bw(B, _Char, Offset, Acc) when Offset >= size(B) ->
    Acc;
find_in_bin_bw(B, Char, Offset, Acc) ->
    <<_:Offset/binary, C:8, _/binary>> = B,
    if Char == C ->
            find_in_bin_bw(B, Char, Offset + 1, [Offset|Acc]);
       true ->
            find_in_bin_bw(B, Char, Offset + 1, Acc)
    end.

%% @spec (term(), term(), state_r()) -> gen_server_handle_call_reply_tuple()
%% @doc Call the brick's implementation-specific handle_call() callback func.
%%
%% This function centralizes a few complications involved in calling an
%% implementation-specific callback function: the callback may request
%% that further calculation be performed by this module, the
%% implementation-<b>independent</b> module.  This indication is noted
%% by returning a tuple where the first element is an atom that begins
%% with the pattern <tt>up1</tt>.
%%
%% <ul>
%% <li> up1 ... returns a "to do" list, todo_list(), for processing at the
%%      implementation-independent layer. </li>
%% <li> up1_sync_done ... TODO unfinished?? error?? </li>
%% <li> up1_read_only_mode ... the brick is in read-only mode, and the
%%      do list contained at least one op that would modify a key.
%%      In this case, we queue the op for later processing, with the
%%      assumption that the period spent in read-only mode will be very
%%      quick, so the op can be re-processed immediately after read-only
%%      mode is cancelled. </li>
%% </ul>

handle_call_via_impl(Msg, From, State) ->
    {ImplMod, ImplState} = impl_details(State),
    false = size(ImplState) == record_info(size, state), %sanity
    case ImplMod:handle_call(Msg, From, ImplState) of
        {up1, ToDos, OrigReturn} ->
            State2 = do_chain_todos_iff_empty_log_q(ToDos, State),
            calc_original_return(OrigReturn, State2);
        {up1_sync_done, _LastLogSerial, _ToDos, _OrigReturn} ->
            throw(inconceivable_1239875);
        {up1_read_only_mode, From, DoOp, S} ->
            DQI = #dirty_q{from = From, do_op = DoOp},
            CS = State#state.chainstate,
            Ws = [DQI|CS#chain_r.read_only_waiters],
            CS2 = CS#chain_r{read_only_waiters = Ws},
            ?DBG_OPx({up1_read_only_mode, State#state.name, read_only_waiters, Ws}),
            {noreply, State#state{chainstate = CS2, impl_state = S}};
        OrigReturn ->
            calc_original_return(OrigReturn, State)
    end.

%% @spec (gen_server_handle_call_reply_tuple(), state_r())
%%    -> gen_server_handle_call_reply_tuple()
%% @doc Given a gen_server handle_call() reply tuple returned by a
%% brick implementation-dependent module, calculate the gen_server reply
%% term required by the implementation-independent layer.
%%
%% The implementation-dependent's #state record is always stored in
%% #state.impl_state prior to returning.

calc_original_return({reply, Reply, S}, State) ->
    false = size(S) == record_info(size, state), %sanity
    {reply, Reply, State#state{impl_state = S}};
calc_original_return({reply, Reply, S, Timeout}, State) ->
    false = size(S) == record_info(size, state), %sanity
    {reply, Reply, State#state{impl_state = S}, Timeout};
calc_original_return({noreply, S}, State) ->
    false = size(S) == record_info(size, state), %sanity
    {noreply, State#state{impl_state = S}};
calc_original_return({noreply, S, Timeout}, State) ->
    false = size(S) == record_info(size, state), %sanity
    {noreply, State#state{impl_state = S}, Timeout};
calc_original_return({stop, Reason, Reply, S}, State) ->
    false = size(S) == record_info(size, state), %sanity
    {stop, Reason, Reply, State#state{impl_state = S}};
calc_original_return({stop, Reason, S}, State) ->
    false = size(S) == record_info(size, state), %sanity
    {stop, Reason, State#state{impl_state = S}}.

%% @spec (state_r(), term()) -> gen_server_handle_call_reply_tuple()
%% @doc Return a prop_list() of terms that describe the brick's current
%% status.
%%
%% The prop_list has two properties, the value each also being a prop_list():
%% <tt>chain</tt> (chain-related status properties) and
%% <tt>implementation</tt> (implementation-dependent status properties).

do_status(S, ImplStatus) ->
    C = S#state.chainstate,
    {{ok, [{chain, [
                    %% Chain state stuff
                    {chain_role, C#chain_r.role},
                    {chain_proplist, C#chain_r.proplist},
                    {chain_upstream, C#chain_r.upstream},
                    {chain_downstream, C#chain_r.downstream},
                    {chain_official_tail, C#chain_r.official_tail},
                    {chain_my_repair_state, C#chain_r.my_repair_state},
                    {chain_ds_repair_state, C#chain_r.ds_repair_state},
                    {chain_my_repair_ok_time, C#chain_r.my_repair_ok_time},
                    {chain_up_serial, C#chain_r.up_serial},
                    {chain_down_serial, C#chain_r.down_serial},
                    {chain_down_acked, C#chain_r.down_acked},
                    {chain_down_unacked_len,
                     case catch queue:len(C#chain_r.down_unacked) of
                        {'EXIT', _} -> undefined;
                        N           -> N
                     end},
                    {chain_last_ack, C#chain_r.last_ack},
                    {chain_clock_tref, C#chain_r.clock_tref},
                    {chain_read_only_p, C#chain_r.read_only_p},
                    {chain_read_only_waiters, length(C#chain_r.read_only_waiters)},
                    {chain_waiting_for_mig_start_p, C#chain_r.waiting_for_mig_start_p},
                    {chain_mig_start_waiters, length(C#chain_r.mig_start_waiters)}
                   ]},
           {sweep, if not is_record(S#state.sweepstate, sweep_r) ->
                           [];
                      true ->
                           Sw = S#state.sweepstate,
                           [{cookie, Sw#sweep_r.cookie},
                            {started, Sw#sweep_r.started},
                            {options, Sw#sweep_r.options},
                            {status, if is_tuple(Sw#sweep_r.stage) ->
                                             element(1, Sw#sweep_r.stage);
                                        true ->
                                             Sw#sweep_r.stage
                                     end},
                            {sweep_key, Sw#sweep_r.sweep_key}
                           ] ++
                            if is_record(S#state.globalhash, g_hash_r) ->
                                    [{migr_dict_list,
                                      dict:to_list(
                                        (S#state.globalhash)#g_hash_r.migr_dict)}];
                               true ->
                                    []
                            end ++
                            [{raw, S#state.sweepstate}]
                   end
           },
           {hash, S#state.globalhash},
           {implementation, ImplStatus},
           {n_too_old, S#state.n_too_old}
          ]},
     S}.

%%
%% Chain API management
%%

%% @spec (state_r())
%%    -> {{error, Reason, MoreInfo}, state_r()} | {ok, state_r()}
%% @doc Set the brick's chain role to "standalone".

chain_set_role_standalone(S)
  when (S#state.chainstate)#chain_r.role == standalone ->
    {{error, already_standalone, {}}, S};
chain_set_role_standalone(S) ->
    CS = S#state.chainstate,
    catch brick_itimer:cancel(CS#chain_r.clock_tref),
    catch gmt_util:unmake_monitor(CS#chain_r.upstream_monitor),
    %% We want to keep: my_repair_state, r/o
    ?E_INFO("ROLE: ~p: standalone, official tail true\n", [S#state.name]),
    {ok, S#state{chainstate =
                 set_my_repair_member(S,
                     #chain_r{role = standalone,
                          official_tail = true,
                          my_repair_ok_time = CS#chain_r.my_repair_ok_time,
                          read_only_p = CS#chain_r.read_only_p,
                          read_only_waiters = CS#chain_r.read_only_waiters,
                          waiting_for_mig_start_p = CS#chain_r.waiting_for_mig_start_p,
                          mig_start_waiters = CS#chain_r.mig_start_waiters,
                          %% Avoid serial # bugs when a chain of length > 1
                          %% is shortened to a standalone.
                          up_serial = 0,
                          down_serial = 0,
                          down_acked = 0,
                          last_ack = undefined,
                          upstream_monitor = undefined,
                          upstream_monitor_down_p = false
                         }, CS#chain_r.my_repair_state)}}.

%% @spec (state_r()) -> {ok, state_r()}
%% @doc Set the brick's chain role to "undefined".

chain_set_role_undefined(S) ->
    CS = S#state.chainstate,
    catch brick_itimer:cancel(CS#chain_r.clock_tref),
    catch gmt_util:unmake_monitor(CS#chain_r.upstream_monitor),
    %% We want to keep: my_repair_state, r/o
    ?E_INFO("ROLE: ~p: undefined\n", [S#state.name]),
    {ok, S#state{chainstate =
                 set_my_repair_member(S,
                     #chain_r{role = undefined,
                          my_repair_ok_time = CS#chain_r.my_repair_ok_time,
                          read_only_p = CS#chain_r.read_only_p,
                          read_only_waiters = CS#chain_r.read_only_waiters,
                          waiting_for_mig_start_p = CS#chain_r.waiting_for_mig_start_p,
                          mig_start_waiters = CS#chain_r.mig_start_waiters,
                          upstream_monitor = undefined,
                          upstream_monitor_down_p = false
                         }, CS#chain_r.my_repair_state)}}.

%% @spec (state_r(), prop_list())
%%    -> {{error, Reason, MoreInfo}, state_r()} | {ok, state_r()}
%% @doc Front-end function to parse PropList enough to decide if the
%% brick's chain role should be "head", "middle", or "tail".
%%
%% Valid PropList options:
%% <ul>
%% <li> {downstream_node, brick()} ... name of the new downstream brick.
%%      NOTE: When setting the middle role, both downstream_node and
%%      upstream_node must be present. </li>
%% <li> head ... new chain role is "head". </li>
%% <li> middle ... new chain role is "middle". </li>
%% <li> official_tail ... if present, this brick will be the chain's
%%      new "official tail" of the chain. </li>
%% <li> tail ... new chain role is "tail". </li>
%% <li> {upstream_node, brick()} ... name of the new upstream brick. </li>
%% </ul>
%%
%% TODO: Try to avoid this similar-but-not-quite-the-same code in some/all
%%       of the three clauses?

chain_set_role(S, PropList) ->
    case {proplists:get_bool(head, PropList),
          proplists:get_bool(middle, PropList),
          proplists:get_bool(tail, PropList)} of
        {true, false, false} ->
            chain_set_role(head, S, PropList);
        {false, true, false} ->
            chain_set_role(middle, S, PropList);
        {false, false, true} ->
            chain_set_role(tail, S, PropList);
        _ ->
            {{error, bad_role, PropList}, S}
    end.

%% @spec (head | middle | tail, state_r(), prop_list())
%%    -> {{error, Reason, MoreInfo}, state_r()} | {ok, state_r()}
%% @doc Back-end function:
%%      set the brick's chain role to "head", "middle", or "tail".
%%
%% This role-specific function is also responsible for tasks such as:
%%   chain-related timer management,
%%   preserving repair state,
%%   preserving serial number propagation &amp; ack'ed state.

chain_set_role(head, S, PropList) ->
    case proplists:get_value(downstream_node, PropList) of
        undefined ->
            {{error, no_downstream, PropList}, S};
        {BrickName, Node} = TwoTuple ->
            case (catch brick_server:status(BrickName, Node)) of
                {ok, _} ->
                    CS = S#state.chainstate,
                    catch brick_itimer:cancel(CS#chain_r.clock_tref),
                    catch gmt_util:unmake_monitor(CS#chain_r.upstream_monitor),
                    TRef = start_chain_admin_periodic_timer(S),
                    AmOfficialTail = proplists:get_value(official_tail,
                                                         PropList, false),
                    {ImplMod, ImplState} = impl_details(S),
                    NewSerial = CS#chain_r.down_serial,
                    {NoopLog, ImplState2} =
                        ImplMod:bcb_make_log_replay_noop(ImplState),
                    UnQ = case CS#chain_r.down_unacked of
                              undefined ->
                                  queue:from_list([NoopLog]);
                              Q ->
                                  case queue:is_empty(Q) of
                                      true  -> queue:from_list([NoopLog]);
                                      false -> Q
                                  end
                          end,
                    CS2 = set_my_repair_member(S,
                            #chain_r{
                              role = chain_member,
                              proplist = PropList,
                              downstream = TwoTuple,
                              official_tail = AmOfficialTail,
                              my_repair_ok_time = CS#chain_r.my_repair_ok_time,
                              %% TODO: Optimize so that down_unacked queue is
                              %% only used if there is at least one middle
                              %% between this brick and the tail.
                              down_serial = NewSerial,
                              down_acked = ?NEG_LIMBO_SEQNUM,
                              down_unacked = UnQ,
                              last_ack = now(),
                              clock_tref = TRef,
                              read_only_p = CS#chain_r.read_only_p,
                              read_only_waiters = CS#chain_r.read_only_waiters,
                              waiting_for_mig_start_p = CS#chain_r.waiting_for_mig_start_p,
                              mig_start_waiters = CS#chain_r.mig_start_waiters,
                              upstream_monitor = undefined,
                              upstream_monitor_down_p = false
                            }, CS#chain_r.my_repair_state),
                    ?E_INFO("ROLE: ~p: head, official tail ~p\n",
                          [S#state.name, AmOfficialTail]),
                    {ok, S#state{chainstate = CS2, impl_state = ImplState2}};
                Err ->
                    {{error, bad_peer, {TwoTuple, Err}}, S}
            end
    end;
chain_set_role(tail, S, PropList) ->
    case proplists:get_value(upstream_node, PropList) of
        undefined ->
            {{error, no_upstream, PropList}, S};
        {BrickName, Node} = TwoTuple ->
            case (catch brick_server:status(BrickName, Node)) of
                {ok, _} ->
                    CS = S#state.chainstate,
                    catch brick_itimer:cancel(CS#chain_r.clock_tref),
                    TRef = start_chain_admin_periodic_timer(S),
                    AmOfficialTail = proplists:get_value(official_tail,
                                                         PropList, true),
                    LastUpSerial = 0,
                    {UpStrMon, UpStrMonDown} =
                        if TwoTuple == CS#chain_r.upstream ->
                                {CS#chain_r.upstream_monitor,
                                 CS#chain_r.upstream_monitor_down_p};
                           true ->
                                catch gmt_util:unmake_monitor(CS#chain_r.upstream_monitor),
                                {ok, NewMon} = gmt_util:make_monitor(TwoTuple),
                                {NewMon, false}
                        end,
                    CS2 = set_my_repair_member(S,
                            #chain_r{
                              role = chain_member,
                              proplist = PropList,
                              upstream = TwoTuple,
                              official_tail = AmOfficialTail,
                              my_repair_ok_time = CS#chain_r.my_repair_ok_time,
                              up_serial = LastUpSerial,
                              down_serial = -1,
                              %% down_acked & down_unacked not needed for tail
                              down_acked = ?NEG_LIMBO_SEQNUM,
                              down_unacked = undefined,
                              last_ack = now(),
                              clock_tref = TRef,
                              read_only_p = CS#chain_r.read_only_p,
                              read_only_waiters = CS#chain_r.read_only_waiters,
                              waiting_for_mig_start_p = CS#chain_r.waiting_for_mig_start_p,
                              mig_start_waiters = CS#chain_r.mig_start_waiters,
                              upstream_monitor = UpStrMon,
                              upstream_monitor_down_p = UpStrMonDown
                           }, CS#chain_r.my_repair_state),
                    ?E_INFO("ROLE: ~p: tail, official tail ~p, up_serial ~p, my serial ~p\n",
                          [S#state.name, AmOfficialTail, LastUpSerial, peek_impl_logging_serial(S)]),
                    put(zzz_upstream_checks, 0), % qqq debug!!!!!
                    {ok, S#state{chainstate = CS2}};
                Err ->
                    {{error, bad_peer, {TwoTuple, Err}}, S}
            end
    end;
chain_set_role(middle, S, PropList) ->
    case {proplists:get_value(upstream_node, PropList),
          proplists:get_value(downstream_node, PropList)} of
        {undefined, _} ->
            {{error, no_upstream, PropList}, S};
        {_, undefined} ->
            {{error, no_downstream, PropList}, S};
        {{UpBrickName, UpNode}=UpTuple, {DownBrickName, DownNode}=DownTuple} ->
            case {(catch brick_server:status(UpBrickName, UpNode)),
                  (catch brick_server:status(DownBrickName, DownNode))} of
                {{ok, _}, {ok, _}} ->
                    CS = S#state.chainstate,
                    catch brick_itimer:cancel(CS#chain_r.clock_tref),
                    TRef = start_chain_admin_periodic_timer(S),
                    AmOfficialTail = proplists:get_value(official_tail,
                                                         PropList, false),
                    {_ImplMod, _ImplState} = impl_details(S),
                    UnQ = if CS#chain_r.down_unacked == undefined ->
                                  queue:new();
                             true ->
                                  CS#chain_r.down_unacked
                          end,
                    LastUpSerial = 0,
                    NewSerial = CS#chain_r.down_serial,
                    {UpStrMon, UpStrMonDown} =
                        if UpTuple == CS#chain_r.upstream ->
                                {CS#chain_r.upstream_monitor,
                                 CS#chain_r.upstream_monitor_down_p};
                           true ->
                                catch gmt_util:unmake_monitor(CS#chain_r.upstream_monitor),
                                {ok, NewMon} = gmt_util:make_monitor(UpTuple),
                                {NewMon, false}
                        end,
                    CS2 = set_my_repair_member(S,
                            #chain_r{
                              role = chain_member,
                              proplist = PropList,
                              upstream = UpTuple,
                              downstream = DownTuple,
                              official_tail = AmOfficialTail,
                              my_repair_ok_time = CS#chain_r.my_repair_ok_time,
                              up_serial = LastUpSerial,
                              %% TODO: Optimize so that down_unacked queue is
                              %% only used if there is at least one middle
                              %% between this brick and the tail.
                              down_serial = NewSerial,
                              down_acked = ?NEG_LIMBO_SEQNUM,
                              down_unacked = UnQ,
                              last_ack = now(),
                              clock_tref = TRef,
                              read_only_p = CS#chain_r.read_only_p,
                              read_only_waiters = CS#chain_r.read_only_waiters,
                              waiting_for_mig_start_p = CS#chain_r.waiting_for_mig_start_p,
                              mig_start_waiters = CS#chain_r.mig_start_waiters,
                              upstream_monitor = UpStrMon,
                              upstream_monitor_down_p = UpStrMonDown
                           }, CS#chain_r.my_repair_state),
                    ?E_INFO("ROLE: ~p: middle, official tail ~p\n",
                          [S#state.name, AmOfficialTail]),
                    {ok, S#state{chainstate = CS2}};
                {Err1, Err2} ->
                    {{error, bad_peer, {{UpTuple,Err1}, {DownTuple,Err2}}}, S}
            end
    end.

%% @spec (state_r()) -> ok | {error, Reason, MoreInfo}
%% @doc Sanity check: we're ok to be servicing queries.
%%
%% TODO: The return value of this function will probably be passed directly
%% to the calling client, so take care with possible pattern matching
%% that the client may have to do.

chain_check_my_status(S) ->
    CS = S#state.chainstate,
    if CS#chain_r.role == undefined ->
            {error, current_role, {S#state.name, CS#chain_r.role, S#state.globalhash}}; %QQQ debugging only! TODO return commented value.
            %%{error, current_role, CS#chain_r.role};
       CS#chain_r.my_repair_state /= ok ->
            {error, current_repair_state, CS#chain_r.my_repair_state};
       true ->
            ok
    end.

%% @spec (state_r(), repair_state_name()) -> ok | {error, Reason, MoreInfo}
%% @doc Set this brick's repair state.

chain_set_my_repair_state(S, NewVal) ->
    CS = S#state.chainstate,
    case {CS#chain_r.my_repair_state, NewVal} of
        {{last_key, _}, pre_init} ->
            %% We were repairing, now we're told to go back to
            %% pre_init, so that's OK.
            {ok, S#state{chainstate = set_my_repair_member(S, CS, NewVal)}};
        {repair_overload, pre_init} ->
            %% We were repairing and then overloaded, now we're told
            %% to go back to pre_init, so that's OK.
            delete_repair_overload_key(S#state.name),
            {ok, S#state{chainstate = set_my_repair_member(S, CS, NewVal)}};
        {disk_error, pre_init} ->
            %% We had a disk error that we couldn't recover from on
            %% our own.  We have been told by a human administrator to
            %% move to pre_init, so that's OK.
            brick_sb:report_brick_status_general(
              S#state.name, node(), administrative_intervention,
              repair_state_override,
              [{old_state, disk_error}, {new_state, pre_init}]),
            {ok, S#state{chainstate = set_my_repair_member_and_chain_ok_time(
                                        S, CS, NewVal, undefined)}};
        {ok, force_ok_to_pre_init} ->
            %% We were ok, now we're told *specifically* to go back to
            %% pre_init, so that's OK: whoever is doing this should
            %% know exactly what he/she/it is doing.
            %%
            %% Also, we need to adjust our start time to fake out the
            %% pinger.  Why?  Well, we probably don't want to take the
            %% time to die and go through our entire init process.
            %% But the pinger really, really expects that if a process
            %% gets into pre_init, that it has a start time different
            %% from when the pinger last saw the brick start.  {sigh}
            Start = now(),
            ok = brick_pingee:set_start_time(S#state.name, node(), Start),
            {ok, S#state{chainstate = set_my_repair_member_and_chain_ok_time(
                                        S, CS, pre_init, undefined)}};
        {{last_key, _}, repair_overload} ->
            %% We were repairing, so overload state is OK.
            %% We're configured as a tail, but we need to stop sending
            %% chain updates upstream because the chain monitor is
            %% going to drop us from the chain very shortly.  We'll
            %% use the undefined role as a base state, then keep the
            %% repair_overload bit.
            {ok, NewS} = chain_set_role_undefined(S),
            NewCS = NewS#state.chainstate,
            {ok, NewS#state{chainstate = set_my_repair_member(NewS, NewCS,
                                                              NewVal)}};
        {_, testing_harness_force_pre_init} ->
            %% Used only by regression tests.
            {ok, S#state{chainstate = set_my_repair_member_and_chain_ok_time(
                                        S, CS, pre_init, undefined)}};
        {_, force_disk_error} ->
            %% This option should not be used manually!  It's used by a brick
            %% to force itself out of 'ok' state into 'disk_error' state,
            %% i.e. in response to the CommonLog server telling it that one
            %% of the value blob log files has had a checksum error.
            %% The brick pinger ought to notice the status change within a
            %% second and remove the brick from the chain.
            brick_pingee:set_chain_ok_time(S#state.name, node(), undefined),
            {ok, S#state{chainstate = set_my_repair_member(S, CS, disk_error)}};
        {OldRState, ok} when OldRState /= disk_error ->
            OkTime = now(),
            {ok, S#state{chainstate = set_my_repair_member_and_chain_ok_time(
                                        S, CS, NewVal, OkTime)}};
        _ ->
            {{error, invalid, CS#chain_r.my_repair_state, to, NewVal}, S}
    end.

%% @spec (state_r(), repair_state_name()) -> ok | {error, Reason, MoreInfo}
%% @doc Set a brick's notion of its immediate downstream brick's repair state.

chain_set_ds_repair_state(S, Val) ->
    CS = S#state.chainstate,
    case Val of
        pre_init ->
            {ok, S#state{chainstate = CS#chain_r{ds_repair_state = Val}}};
        _ ->
            {{error, invalid, Val}, S}
    end.

%% @spec (prop_list(), state_r()) -> true | false
%% @doc Calculate whether or not this brick should send a direct reply
%%      back to the client (false = send the op &amp; reply down the chain
%%      for further processing).

chain_reply_directly_p(DoFlags, S) ->
    CS = S#state.chainstate,
    IgnoreRoleP = proplists:get_value(ignore_role, DoFlags, false),
    if IgnoreRoleP                      -> true;
       CS#chain_r.official_tail == true -> true;
       true                             -> false
    end.

%% @spec (integer(), prop_list(), From, term(), thisdo_mods(), state_r()) ->
%%       state_r()
%% @doc Send the op downstream, if there is a downstream, and sending a
%%      reply to the client.
%%
%% TODO: This is a case where the ChainRep paper suggests that we
%% have some flexibility at the expense of strong consistency
%% guarantees.  If we are not the tail, and if Thisdo_Mods == [],
%% then this query was read-only and if we reply directly, it's
%% possible that our reply could result in inconsistency if the
%% proper number of failures occur.  For strong consistency, all
%% replies must be sent by the tail.
%%
%% If <tt>From</tt> is a binary, then no reply will be sent, regardless of the
%% chain's role or other configuration.
%%
%% If <tt>From</tt> matches {sweep_cast, pid()}, then gen_server:cast/2 is
%% used to send the reply to the pid.

chain_send_downstream(Serial, DoFlags, From, Reply, Thisdo_Mods, S) ->
    ok = bz_debug_check_todos(Thisdo_Mods, S),
    CS = S#state.chainstate,
    ReplyDirectly = chain_reply_directly_p(DoFlags, S)
        orelse Thisdo_Mods == [],
    %% From may be an invalid sender, such as a binary, so don't send
    %% anything to such an invalid sender.
    if ReplyDirectly == true, is_tuple(From), element(1, From) == sweep_cast ->
            ?DBG_CHAINx({chain_send_downstream, S#state.name, sweep_cast, From, Reply, Thisdo_Mods}),
            {sweep_cast, Pid} = From,
            gen_server:cast(Pid, Reply);
       ReplyDirectly == true, not is_binary(From) ->
            ?DBG_CHAINx({chain_send_downstream, S#state.name, reply, From, Reply, Thisdo_Mods}),
            gen_server:reply(From, Reply);
       true ->
            ?DBG_CHAINx({chain_send_downstream, S#state.name, not_replying, From, Reply, Thisdo_Mods, {replydirectly, ReplyDirectly}}),
            ok
    end,

    ReplyDirectlyAndReadOnlyP =                 % !@#$! guard limitations
        ReplyDirectly == true andalso Thisdo_Mods == [],
    if CS#chain_r.downstream /= undefined,
       not ReplyDirectlyAndReadOnlyP ->
            {ImplMod, ImplState} = impl_details(S),
            Thisdo_Mods_filt = filter_mods_for_downstream(Thisdo_Mods,
                                                          ImplMod, ImplState),
            LastSerial = CS#chain_r.down_serial,
            Msg_filt = {ch_log_replay_v2, {S#state.name, node()}, Serial,
                        Thisdo_Mods_filt, From, Reply, LastSerial}, % for downstream
            Msg_mine = {ch_log_replay_v2, {S#state.name, node()}, Serial,
                        Thisdo_Mods, From, Reply, LastSerial},      % for me
            ?DBG_CHAINx({chain_send_downstream, S#state.name, serial, Serial, casting_downstream, Msg_filt}),
            gen_server:cast(CS#chain_r.downstream, Msg_filt),
            %% The official tail brick does not need to keep track
            %% of unacknowledged mods that we send downstream: if the
            %% brick downstream of the O.T. crashes, we have another
            %% method to fix it: the normal repair mechanism.
            NewQ = if CS#chain_r.official_tail == false ->
                           Q = CS#chain_r.down_unacked,
                           queue:in(Msg_mine, Q);
                      true ->
                           CS#chain_r.down_unacked
                   end,
            S#state{chainstate = CS#chain_r{down_serial = Serial,
                                            down_unacked = NewQ}};
       true ->
            S
    end.

chain_send_downstream_iff_empty_log_q(Serial, DoFlags, From, Reply,
                                      Thisdo_Mods, S) ->
    {ImplMod, ImplState} = impl_details(S),
    %% If Thisdo_mods is empty, then there's no reason to hold this
    %% operation (probably a get or get_many) hostage to stuff that's
    %% really been logged to disk and is waiting for a sync.
    case (queue:is_empty(S#state.logging_op_q) andalso
          ImplMod:bcb_peek_logging_op_q_len(ImplState) == 0)
         orelse
         Thisdo_Mods == [] of
        true ->
            chain_send_downstream(Serial, %qqq_xxx_yyy2,
                                  [], From, Reply, Thisdo_Mods, S);
        false ->
            %% We'll have to wait for a round-trip with the sync'er before
            %% sending downstream.
            OL = queue:to_list(S#state.logging_op_q),
            LQI = #log_q{time = time(),         % QQQ debugging
                         logging_serial = Serial,
                         thisdo_mods = Thisdo_Mods, doflags = DoFlags,
                         from = From, reply = Reply},
            NL = sort_by_serial(OL ++ [LQI]),
            %% We haven't really written this term to the local log (or
            %% perhaps the caller did), but we definitely *do* need a trigger
            %% to get this new item out of the logging_op_q.
            ImplState2 =
                ImplMod:bcb_async_flush_log_serial(Serial, ImplState),
            S#state{logging_op_q = queue:from_list(NL),
                    impl_state = ImplState2}
    end.

%% @spec (serial_number(), thisdo_mods(), From, Reply, state_r()) ->
%%       {state_r(), goahead|wait}
%% @equiv chain_do_log_replay(Serial, [], Thisdo_Mods, From, Reply, S)

chain_do_log_replay(Serial, Thisdo_Mods, From, Reply, S) ->
    chain_do_log_replay(Serial, [], Thisdo_Mods, From, Reply, S).

% chain_do_log_replay_async(Serial, Thisdo_Mods0, From, Reply, S) ->
%     Thisdo_Mods = [{log_directive, sync_override, false}|Thisdo_Mods0],
%     chain_do_log_replay(Serial, [], Thisdo_Mods, From, Reply, S).

%% @spec (serial_number(), prop_list(), thisdo_mods() | tuple(), From, Reply, state_r()) ->
%%       {state_r(), goahead|wait}
%% @doc Replay a brick log operation, typically as sent to this brick by
%%      its upstream brick, and then send it downstream.
%%
%% The actual logging op is performed by the implementation-dependent module.
%% The return value from ImplMod:bcb_map_mods_to_storage() can tell us:
%% <ul>
%% <li> wait ... The logging operation is not finished (typically due to
%%      synchronous disk I/O operation that is still running).  The
%%      lower layer will signal later when the logging op has finished. </li>
%% <li> goahead ... No further delay is required for data logging.  </li>
%% </ul>
%%
%% If Thisdo_Mods is not a list, then the upstream is requesting some
%% kind of flow-down-the-chain behavior but not the typical
%% log-stuff-locally-then-replay-it-locally-then-send-downstream
%% behavior.
%%
%% The returned boolean() value is true if the return from
%% ImplMod:bcb_log_mods/2 was {goahead, _}, false if it was {wait, _}.

chain_do_log_replay(ChainSerial, DoFlags, Thisdo_Mods, From, Reply, S)
  when is_list(Thisdo_Mods) ->
    {ImplMod, ImplState} = impl_details(S),
    false = size(ImplState) == record_info(size, state), %sanity
    case ImplMod:bcb_log_mods(Thisdo_Mods, ChainSerial, ImplState) of
        {goahead, ImplState2, LocalMods} ->
            ?DBG_CHAIN_TLOGx({chain_do_log_replay, S#state.name, calling, bcb_map_mods_to_storage}),
            ImplState3 = ImplMod:bcb_map_mods_to_storage(LocalMods,
                                                         ImplState2),
            false = size(ImplState3) == record_info(size, state), %sanity
            %% TODO: Should this be chain_send_downstream_iff_empty_log_q()??
            S2 = chain_send_downstream(ChainSerial,
                                       DoFlags, From, Reply, Thisdo_Mods,
                                       S#state{impl_state = ImplState3}),
            CS = S2#state.chainstate,
            if ChainSerial == no_serial ->
                    {S2, goahead};
               is_integer(ChainSerial) ->
                    {S2#state{chainstate = CS#chain_r{up_serial = ChainSerial}},
                     goahead}
            end;
        {wait, ImplState2, LocalMods} ->
            ImplState3 = ImplMod:bcb_async_flush_log_serial(ChainSerial,
                                                            ImplState2),
            %% In normal GDSS operation, a middle or tail brick doesn't need
            %% to add the keys from log replay to the dirty table.  However,
            %% in the case of data migration, we must (see BZ 26644).
            ImplState4 = ImplMod:bcb_add_mods_to_dirty_tab(
                           Thisdo_Mods, ImplState3),
            LQI = #log_q{time = time(), % QQQ time debugging
                         logging_serial = ChainSerial,
                         thisdo_mods = LocalMods,
                         doflags = [],
                         from = From, reply = Reply},
            NewQ1 = if is_integer(ChainSerial) ->
                            queue:in({upstream_serial, ChainSerial},
                                     S#state.logging_op_q);
                       true ->
                            S#state.logging_op_q
            end,
            NewQ2 = queue:in(LQI, NewQ1),
            ?DBG_CHAIN_TLOGx({chain_do_log_replay,
                              S#state.name, wait, newq2, NewQ2}),
            CS = S#state.chainstate,
            {S#state{logging_op_q = NewQ2,
                     chainstate = CS#chain_r{up_serial = ChainSerial},
                     impl_state = ImplState4},
             wait}
    end;
chain_do_log_replay(ChainSerial, DoFlags, Tuple_mod_thing, From, Reply, S)
  when is_tuple(Tuple_mod_thing),
       element(1, Tuple_mod_thing) == plog_sweep ->
    case Tuple_mod_thing of
        {plog_sweep, PLogType, SwC}
        when is_record(SwC, sweepcheckp_r),
             is_record(S#state.globalhash, g_hash_r) ->
            S2 = chain_send_downstream_iff_empty_log_q(
                   ChainSerial, DoFlags, From, Reply, Tuple_mod_thing, S),
            %% Only write checkpoint for our own chain's sweep state.
            %% S2b = if Sw#sweep_r.chain_name == SwC#sweepcheckp_r.chain_name ->
            S2b = if PLogType == phase1_sweep_info ->
                          write_sweep_cp(SwC, S2);
                     PLogType == from_other ->
                          S2
                  end,
            S3 = if not (S#state.chainstate)#chain_r.waiting_for_mig_start_p ->
                         S2b;
                    true ->
                         OldCS = S#state.chainstate,
                         resubmit_all_queued_update_ops(
                           S2b#state.name, OldCS#chain_r.mig_start_waiters),
                         NewCS = OldCS#chain_r{waiting_for_mig_start_p = false,
                                               mig_start_waiters = []},
                         S2b#state{chainstate = NewCS}
                 end,
            {sanity, true} = {sanity, is_record(S3#state.globalhash, g_hash_r)},
            ?DBG_MIGRATEx({set_chain_sweep_key, S#state.name, SwC#sweepcheckp_r.chain_name,
                           log_replay, SwC#sweepcheckp_r.sweep_key}),
            GH = brick_hash:add_migr_dict_if_missing(S3#state.globalhash),
            NewGH = brick_hash:set_chain_sweep_key(
                      SwC#sweepcheckp_r.chain_name,
                      SwC#sweepcheckp_r.sweep_key,
                      GH#g_hash_r{migrating_p = true, phase = migrating}),
            if ChainSerial == no_serial ->
                    {S3#state{globalhash = NewGH}, goahead};
               is_integer(ChainSerial) ->
                    CS = S3#state.chainstate,
                    {S3#state{chainstate = CS#chain_r{up_serial = ChainSerial},
                              globalhash = NewGH},
                     goahead}
            end;
        {plog_sweep, _, SwC}
        when is_record(SwC, sweepcheckp_r),
             not is_record(S#state.globalhash, g_hash_r) ->
            ?E_ERROR("Error: ~p got plog_sweep record when GH = ~p\n",
                     [S#state.name, S#state.globalhash]),
            {S, goahead}
    end.

%% @spec (state_r()) -> ok | {error, Reason, MoreInfo}
%% @doc Start the repair process: send all data from this brick to
%%      the immediate downstream brick.
%%
%% The choice of starting serial number is somewhat arbitrary: the
%% number should be negative, to avoid complications of a serial number
%% going backward (big positive to smaller positive) after repair is
%% finished.  It should be small enough so that it's impossible to have
%% the serial number reach 0 before repair is complete.
-spec chain_start_repair(#state{}) -> {ok | {error, not_chain_member | no_downstream | ds_repair_state, term()},#state{}}.
chain_start_repair(S) ->
    CS = S#state.chainstate,
    if CS#chain_r.role /= chain_member ->
            {{error, not_chain_member, {role, CS#chain_r.role}}, S};
       CS#chain_r.downstream == undefined ->
            {{error, no_downstream, []}, S};
       CS#chain_r.ds_repair_state /= pre_init ->
            {{error, ds_repair_state, CS#chain_r.ds_repair_state}, S};
       true ->
            RepairM = proplists:get_value(repair_method, CS#chain_r.proplist,
                                          repair_diff),
            MaxKeys = proplists:get_value(repair_max_keys, CS#chain_r.proplist,
                                          ?REPAIR_MAX_KEYS),
            MaxBytes = prop_or_central_conf_i(
                         brick_repair_max_bytes,
                         repair_max_bytes, CS#chain_r.proplist,
                         ?REPAIR_MAX_BYTES),
            MaxPrimers = prop_or_central_conf_i(
                           brick_repair_max_primers,
                           repair_max_primers, CS#chain_r.proplist,
                           ?REPAIR_MAX_PRIMERS),
            StartingSerial = -888777666555444,
            Repair = #repair_r{started = now(),
                               started_serial = StartingSerial,
                               last_repair_serial = StartingSerial,
                               repair_method = RepairM,
                               max_keys = MaxKeys,
                               max_bytes = MaxBytes,
                               key = ?BRICK__GET_MANY_FIRST,
                               last_ack = now()},
            NewS = S#state{chainstate = CS#chain_r{ds_repair_state = Repair,
                                                   max_primers = MaxPrimers}},
            NewS2 = send_next_repair(NewS),
            {ok, NewS2}
    end.

%% @spec (state_r()) -> state_r()
%% @doc Perform the periodic administration tasks associated with this
%%      brick's current role.
%%
%% Almost all actual execution is done by {@link cheap_monad/2}.

do_chain_admin_periodic(S)
  when (S#state.chainstate)#chain_r.role /= chain_member ->
    S;
do_chain_admin_periodic(S) ->
    InitialCS = S#state.chainstate,
    Fsend_serial_ack = fun(CS) ->
                              Props = [process_info(self(), message_queue_len)],
                               ?DBG_REPAIRx({before_send_serial_ack, S#state.name, loc_c_serial, CS#chain_r.up_serial}),
                              send_serial_ack(CS, CS#chain_r.up_serial,
                                              S#state.name, node(), Props)
                       end,
    Now = now(),
    DoNotInitiate_p =
        proplists:get_value(do_not_initiate_serial_ack, S#state.options, false),
    InitiateAckUpstream =
        fun(CS) ->
                %% In dev & debugging, I discovered a problem of
                %% backward-counting acks when more than one downstream brick
                %% initiates acks.  Really, only the tail should be
                %% initiating acks.
                %%
                %% When altering chain membership, we could have a situation
                %% where two bricks have the same official_tail = true
                %% status.  So, we should alter chain membership when we're
                %% in a read-only mode and when all changes have propagated
                %% all the way down the new chain.
                %%
                %% During migration testing, I discovered that we need
                %% to send acks for non-repair-related log replay
                %% *while* we are being repaired (i.e. normal log
                %% replay activity) ... or else our upstream/official
                %% tail can wait forever during role changes.
                if (CS#chain_r.upstream /= undefined andalso
                    CS#chain_r.downstream == undefined)
                   andalso
                   not DoNotInitiate_p ->
                        Fsend_serial_ack(CS),
                        CS;
                   true ->
                        CS
                end
        end,
    CheckRepairAck =
        fun(CS) ->
                if CS#chain_r.downstream /= undefined andalso
                   (CS#chain_r.ds_repair_state == pre_init orelse
                    (CS#chain_r.ds_repair_state == ok)) ->
                        CS;
                   CS#chain_r.downstream /= undefined,
                   is_record(CS#chain_r.ds_repair_state, repair_r) ->
                        R = CS#chain_r.ds_repair_state,
                        Diff = timer:now_diff(Now, R#repair_r.last_ack),
                        if Diff > 2*1000*1000 -> % TODO make configurable
                                ?E_INFO(
                                   "No repair ack from ~p in ~p usec\n",
                                   [CS#chain_r.downstream, Diff]),
                                CS;
                           true ->
                                CS
                        end;
                   true ->
                        CS
                end
        end,
    CheckDownstreamAck =
        fun(CS) ->
                if CS#chain_r.downstream /= undefined,
                   CS#chain_r.ds_repair_state == ok ->
                        Diff = timer:now_diff(Now, CS#chain_r.last_ack),
                        if Diff > 20*1000*1000 -> % TODO make configurable
                                ?E_ERROR(
                                  "~s: No downstream ack from ~p in ~p usec\n",
                                  [S#state.name, CS#chain_r.downstream, Diff]),
                                CS;
                           true ->
                                CS
                        end;
                   true ->
                        CS
                end
        end,
    Funs = [InitiateAckUpstream, CheckRepairAck, CheckDownstreamAck],
    S#state{chainstate = cheap_monad(Funs, InitialCS)}.

%% @spec (term(), integer(), integer(), state_r(), boolean()) ->
%%       {list(term()), term()}
%% @doc Given a starting Key, create a list of keys to be used for repair
%% and the last key in the list.
%%
%% The advantage also returning the last key in the list is that the
%% last list item is cheap to obtain here, O(1), instead of later,
%% O(n).
%%
%% We filter out dirty keys in the range that we find initially.  The
%% idea is that the operations for those dirty keys will find their
%% way downstream without the repair process to help them along.  And
%% because they are dirty and not yet written stably to disk, we
%% really don't want to propagate them now.

get_repair_tuples(Key, Max, WitnessP, MaxBytes, S) ->
    {FwdList, LastKey} = get_sweep_tuples(Key, Max, WitnessP, MaxBytes, S),

    %% Must make certain that no dirty keys slip in at the end of the table!
    {ImplMod, ImplState} = impl_details(S),
    DirtyList = ImplMod:bcb_dirty_keys_in_range(Key, LastKey, ImplState),
    FwdList2 = if DirtyList == [] ->
                       FwdList;
                  true ->
                       merge_dirtylist_mods(FwdList, DirtyList)
               end,
    {FwdList2, LastKey}.

merge_dirtylist_mods(ScanList, DirtyList) ->
    DeleteKeys = [Key || {Key, delete, _} <- DirtyList],
    InsertTuples = [T || {_Key, insert, T} <- DirtyList],
    R1 = lists:foldl(fun(Key, Acc) -> lists:keydelete(Key, 1, Acc) end,
                     ScanList, DeleteKeys),
    R2 = lists:ukeymerge(1, InsertTuples, R1),
    R2.

-spec get_sweep_tuples(key(), integer(), boolean(), tuple()) ->
                              {list(), key()}.
get_sweep_tuples(Key, Max, WitnessP, S) ->
    %% Usually, this arity is called by migration sweep.
    get_sweep_tuples(Key, Max, WitnessP, 64*1024*1024, S).

get_sweep_tuples('$end_of_table', _Max, _WitnessP, _MaxBytes, _S) ->
    {[], '$end_of_table'};
get_sweep_tuples(Key, Max, WitnessP, MaxBytes, S) ->
    Fs = case MaxBytes of
             N when N > 0 -> [{max_bytes, MaxBytes}];
             _            -> []
         end,
    {ImplMod, ImplState} = impl_details(S),
    {{ReverseList, _}, _IS} =
        ImplMod:bcb_get_many(Key, Fs ++ [{max_num, Max}, {witness, WitnessP}],
                             ImplState),
    LastKey = if ReverseList /= [] ->
                      [LastTuple|_] = ReverseList,
                      %% TODO: major contamination??
                      element(1, LastTuple);
                 ReverseList == [] ->
                      ?BRICK__GET_MANY_LAST
              end,
    {lists:reverse(ReverseList), LastKey}.

%% Use *ONLY* in cases where we don't know the last serial number or
%% we don't really care what the upstream's last serial number was.

exit_if_bad_serial_from_upstream(Serial, Msg, S) ->
    exit_if_bad_serial_from_upstream(Serial, undefined, Msg, S).

%% @spec (serial_number(), serial_number(), term(), state_r()) -> state_r() | exit
%% @doc Sanity check: is the serial number received from upstream OK?

exit_if_bad_serial_from_upstream(_Serial, _LastUpstreamSerial, _Msg, S)
  when (S#state.chainstate)#chain_r.my_repair_state /= ok ->
    S;
exit_if_bad_serial_from_upstream(Serial, LastUpstreamSerial, Msg, S)
  when is_integer(Serial) ->
    CS = S#state.chainstate,
    LastSerial = CS#chain_r.up_serial,
    if LastSerial == 0, Serial > 0 ->
            %% Should be no_serial_yet ... this is the first serial we've seen.
            ok;
       Serial =< LastSerial ->
            Info = lists:flatten(io_lib:format("Msg = ~P", [Msg, 20])),
            exit({S#state.name, received_bad_serial,
                  {got, Serial}, {last, LastSerial}, Info});
       true ->
            ok
    end,
    CS2 = if is_integer(LastUpstreamSerial) ->
                  if LastUpstreamSerial == LastSerial ->
                          if CS#chain_r.upstream_monitor_down_p ->
                                  %% Reset the upstream_monitor_down_p flag.
                                  %% We are past the risk window where a
                                  %% log replay message might have been
                                  %% dropped.
                                  CS#chain_r{upstream_monitor_down_p = false};
                             true ->
                                  CS
                          end;
                     CS#chain_r.upstream_monitor_down_p ->
                          %% This is the bad situation: we got a {'DOWN',...}
                          %% message from the monitor of the upstream brick,
                          %% then we got a serial # larger than we expect.
                          %% It is possible that the upstream did one of its
                          %% occasional increment by more than one (see next
                          %% clause below), but we will be paranoid and crash
                          %% here to force consistency again.
                          ?APPLOG_WARNING(?APPLOG_APPM_108,"restarting brick_server for unmatched serial(~p ~p)",
                                       [LastUpstreamSerial, LastSerial]),
                          exit({upstream_monitor_and_serial_check_failure,
                                LastUpstreamSerial, LastSerial});
                     true ->
                          %% It's possible for the upstream to increment its log
                          %% serial number by more than 1.  But we didn't get a
                          %% monitor {'DOWN',...} message, so that kind of
                          %% more than 1 skip is OK.
                          CS
                  end;
             true ->
                  CS
          end,

    %% When we send downstream, we're going to increment, so to have our
    %% downstream match what we got from upstream, subtract one here.
    SerialM1 = Serial - 1,
    if SerialM1 > CS2#chain_r.down_serial ->
            S#state{chainstate = CS2#chain_r{down_serial = SerialM1}};
       true ->
            S#state{chainstate = CS2}
    end;
exit_if_bad_serial_from_upstream(no_serial, _LastUpstreamSerial, _Msg, S) ->
    S.

%% @spec (state_r()) -> state_r()
%% @doc Send the next set of repair tuples downstream, if there is no
%% repair throttling in effect.

send_next_repair(S) ->
    case lookup_repair_throttle_key(S#state.throttle_tab) of
        false ->
            send_next_repair2(S);
        true ->
            ?E_ERROR("DEBUG: ~s: send_next_repair delay\n", [S#state.name]),
            CS = S#state.chainstate,
            R = CS#chain_r.ds_repair_state,
            Msg = {after_delay_send_next_repair, R#repair_r.started},
            erlang:send_after(1000, self(), Msg),
            S
    end.

%% @spec (state_r()) -> state_r()
%% @doc Send the next set of repair tuples downstream.
%%
%% If all keys have been scanned, then send a ch_repair_finished message
%% downstream instead.

send_next_repair2(S) ->
    CS = S#state.chainstate,
    R = CS#chain_r.ds_repair_state,

    WitnessP = if
                   R#repair_r.repair_method == replace ->
                       false;
                   R#repair_r.repair_method == repair_diff ->
                       true
               end,
    %%
    %% TODO: Now that we are in the era of bigdata_dir, we really
    %%       should investigate a squidflash-like way of avoid long
    %%       disk latency here.  It wouldn't be at exactly this point
    %%       in the code (it would be after in send_diff_round2()),
    %%       but the need definitely exists.
    %%
    {KeyValTS_list, LastKey} =
        get_repair_tuples(R#repair_r.key, R#repair_r.max_keys, WitnessP,
                          R#repair_r.max_bytes, S),
    VSent = R#repair_r.values_sent,
    Len = length(KeyValTS_list),
    NewR =
        if
            KeyValTS_list == [], LastKey /= ?BRICK__GET_MANY_LAST ->
                {ImplMod, ImplState} = impl_details(S),
                ?E_INFO("Repair: ~p: dirty_key_inteference: ~p ~p ~p\n",
                        [S#state.name, R#repair_r.key, LastKey,
                         ImplMod:bcb_dirty_keys_in_range(R#repair_r.key,LastKey,
                                                         ImplState)]),
                ParentPid = self(),
                RetryRef = make_ref(),
                spawn(fun() -> timer:sleep(50),
                               ParentPid ! {retry_all_dirty_send_next_repair,
                                            RetryRef}
                      end),
                S#state{retry_send_repair = RetryRef};
            KeyValTS_list == [], LastKey == ?BRICK__GET_MANY_LAST ->
                ?DBG_REPAIRx({S#state.name, node(), sending_ch_repair_finished,
                              CS#chain_r.downstream}),
                Serial = CS#chain_r.up_serial,
                MySerial = peek_impl_logging_serial(S),
                ?E_INFO("send repair finished: ~p: MySerial ~p, up_serial ~p\n", [S#state.name, MySerial, CS#chain_r.up_serial]),

                FinalSerial = R#repair_r.last_repair_serial,
                ImplStatus = get_implementation_status(S),
                Cp_p = case proplists:get_value(checkpoint, ImplStatus) of
                           undefined -> false;
                           _         -> true
                       %% TODO: If we are indeed checkpointing, then the
                       %% 100% proper thing to do is to calculate the total
                       %% number of keys that we've got.  Unfortunately, the
                       %% shadowtab's mechanism doesn't allow us to do that
                       %% without a lot of work.
                       end,
                {NumKeys, _} = get_implementation_ets_info(S),
                gen_server:cast(CS#chain_r.downstream,
                                {ch_repair_finished, S#state.name, node(),
                                 Cp_p, NumKeys}),
                R#repair_r{key = ?BRICK__GET_MANY_LAST,
                           last_repair_serial = Serial,
                           final_repair_serial = FinalSerial,
                           values_sent = VSent + Len};
            R#repair_r.repair_method == repair_diff ->
                Serial = R#repair_r.last_repair_serial + 1,
                gen_server:cast(CS#chain_r.downstream,
                                {ch_repair_diff_round1, Serial, KeyValTS_list}),
                R#repair_r{key = LastKey,
                           last_repair_serial = Serial,
                           values_sent = VSent + Len};
            R#repair_r.repair_method == replace ->
                ?E_WARNING("\n\nSSS: ~p: SHOULD NOT BE HAPPENING HERE: send_next_repair: repair_r key was ~p -> LastKey = ~p\n\n", [S#state.name, R#repair_r.key, LastKey]),
                Serial = R#repair_r.last_repair_serial + 1,
                gen_server:cast(CS#chain_r.downstream,
                                {ch_repair, Serial, KeyValTS_list}),
                R#repair_r{key = LastKey,
                           last_repair_serial = Serial,
                           values_sent = VSent + Len}
           end,
    %% TODO: I think that updating chain_r.down_serial here means that it's
    %% effectively impossible for this code to add a new chain member &
    %% repair at any point other than the tail.
    S#state{chainstate = CS#chain_r{ds_repair_state = NewR,
                                    down_serial = NewR#repair_r.last_repair_serial}}.

%% @spec (serial_number(), list(), integer(), state_r()) -> state_r()
%% @doc Send the second round of diff repair tuples downstream.  These tuples correspond
%% to the keys returned from round 1 of chain diff repair.  Pass on the number of deleted
%% tuples so we can call back into the non-diff ch_repair_ack codepath.

send_diff_round2(_Serial, [] = _Unknown, _Ds, S) ->
    %% Downstream brick had everything exactly, no differences.
    %%
    %% But we'll update the last_ack time here, because it's possible
    %% for thousands of repair round1 with nothing to do, and if we
    %% don't update the last_ack time, we'll start to spit out (bogus)
    %% errors about not getting acks from our downstream repairee.
    CS = S#state.chainstate,
    send_next_repair(S#state{chainstate = CS#chain_r{last_ack = now()}});
send_diff_round2(_Serial, Unknown, Ds, S) ->
    CS = S#state.chainstate,
    R = CS#chain_r.ds_repair_state,
    {ImplMod, ImplState} = impl_details(S),

    KeyValTS_list = ImplMod:bcb_get_list(Unknown, ImplState),
    NewSerial = R#repair_r.last_repair_serial + 1,
    gen_server:cast(CS#chain_r.downstream,
                    {ch_repair_diff_round2, NewSerial, KeyValTS_list, Ds}),
    NewR = R#repair_r{last_repair_serial = NewSerial},
    %% TODO: I think that updating chain_r.down_serial here means that it's
    %% effectively impossible for this code to add a new chain member &
    %% repair at any point other than the tail.
    S#state{chainstate = CS#chain_r{ds_repair_state = NewR,
                                    down_serial = NewR#repair_r.last_repair_serial,
                                    last_ack = now()}}.

%% @spec (serial_number(), list(), state_r()) -> state_r()
%% @doc Process a list of repair tuples that we've received from upstream.
%%
%% The FirstKey calculation is complicated by the need to figure out
%% what the current brick's first key is.  After all, the current brick's
%% data set may not be empty.  The value FirstKey is necessary for the
%% diff'ing algorithm used by ImplMod:bcb_repair_loop().
%%
%% TODO: The more I think about it, the more I wonder if the lower
%% bcb_repair_loop() logic should be moved up to this
%% independent/higher layer.  Such a move would require more callbacks
%% to expose implementation-dependent stuff to this layer, but perhaps
%% the overall simplification would be worth it?

chain_do_repair(Serial, RepairList, S) ->
    CS = S#state.chainstate,
    {ImplMod, ImplState} = impl_details(S),
    FirstKey = case CS#chain_r.my_repair_state of
                   pre_init ->
                       {{ReverseList, _}, _IS} =
                           ImplMod:bcb_get_many(?BRICK__GET_MANY_FIRST,
                                                [witness, {max_num, 1}],
                                                ImplState),
                       case ReverseList of
                           [Tuple1] ->
                               %% TODO: major contamination??
                               Key1 = element(1, Tuple1),
                               Key1;
                           [] ->
                               ?BRICK__GET_MANY_LAST
                       end;
                   {last_key, K} ->
                       K
               end,
    {LastKey, Inserts, Deletes, ImplState2} =
        ImplMod:bcb_repair_loop(RepairList, FirstKey, ImplState),
    false = size(ImplState2) == record_info(size, state), %sanity
    NewS = S#state{impl_state = ImplState2},
    send_repair_ack(CS, Serial, NewS#state.name, Inserts, Deletes),
    RS = {last_key, LastKey},
    CS2 = set_my_repair_member(NewS, NewS#state.chainstate, RS),
    NewS#state{chainstate = CS2#chain_r{repair_last_time = now()
                                       }}.

%% @spec (serial_number(), list(), state_r()) -> state_r()
%% @doc Generate a list of tuples unknown to this brick from the upstream list.
%%

chain_do_repair_diff_round1(Serial, RepairList, S) ->
    CS = S#state.chainstate,
    {ImplMod, ImplState} = impl_details(S),
    FirstKey =
        case CS#chain_r.my_repair_state of
            ST when ST == pre_init; ST == repair_overload ->
                {{ReverseList, _}, _IS} =
                    ImplMod:bcb_get_many(?BRICK__GET_MANY_FIRST,
                                         [witness, {max_num, 1}],
                                         ImplState),
                case ReverseList of
                    [Tuple1] ->
                        %% TODO: major contamination??
                        Key1 = element(1, Tuple1),
                        Key1;
                    [] ->
                        ?BRICK__GET_MANY_LAST
                end;
            {last_key, K} ->
                K
        end,
    {MyLastKey, Unknown, Ds, ImplState2} =
        ImplMod:bcb_repair_diff_round1(RepairList, FirstKey, ImplState),
    false = size(ImplState2) == record_info(size, state), %sanity
    NewS = S#state{impl_state = ImplState2},
    send_repair_diff_round1_ack(CS, Serial, NewS#state.name, Unknown, Ds),
    RS = {last_key, MyLastKey},
    CS2 = set_my_repair_member(NewS, NewS#state.chainstate, RS),
    TRef = case CS2#chain_r.my_repair_state of
               {last_key, _} ->
                   CS2#chain_r.clock_tref;
               _ ->
                   catch brick_itimer:cancel(CS#chain_r.clock_tref),
                   start_chain_admin_periodic_timer(NewS)
           end,
    NewS#state{chainstate = CS2#chain_r{clock_tref = TRef,
                                        repair_last_time = now()
                                       }}.

%% @spec (serial_number(), list(), integer(), state_r()) -> state_r()
%% @doc Generate a list of tuples unknown to this brick from the upstream list.
%%

chain_do_repair_diff_round2(Serial, RepairList, Ds, S) ->
    CS = S#state.chainstate,
    {ImplMod, ImplState} = impl_details(S),
    {_____ignoring_this_is_ok_yes_no__LastKey, Is, Ds, ImplState2} = ImplMod:bcb_repair_diff_round2(RepairList, Ds, ImplState),
    false = size(ImplState2) == record_info(size, state), %sanity
    NewS = S#state{impl_state = ImplState2},
    send_repair_ack(CS, Serial, NewS#state.name, Is, Ds),
    CS2 = set_my_repair_member(NewS, NewS#state.chainstate, CS#chain_r.my_repair_state),
    TRef = case CS2#chain_r.my_repair_state of
               {last_key, _} ->
                   CS2#chain_r.clock_tref;
               _ ->
                   catch brick_itimer:cancel(CS#chain_r.clock_tref),
                   start_chain_admin_periodic_timer(NewS)
           end,
    %% keep LastKey from round1 %%{last_key, LastKey},
    NewS#state{chainstate = CS2#chain_r{clock_tref = TRef,
                                        repair_last_time = now()
                                       }}.

%% @spec (do_list(), prop_list(), state_r()) -> {true | false, list()}
%% @doc Predicate: are all of the keys in the do list stored by this brick?
%%
%% If the predicate is false, then the list(brick()) is the list of
%% bricks to which the do_list() should be forwarded.  If that list is
%% empty, then we might have conflicting config info (which is certainly
%% possible if a chain is being reconfigured by the Admin Server):
%% <ul>
%% <li> The global hash (#g_hash_r()) assigned to use says that that we're
%%      the correct brick to handle this do_list().  </li>
%% <li> Our current role says that we cannot handle this do_list().</li>
%% <li> No conflicting config, but one or more keys is in the migration
%%      sweep zone, so we must delay processing for a while. </li>
%% </ul>
%%
%% In any of those cases, the empty brick list mean to forward the query
%% to ourself: hopefully things will change shortly.
%%
%% We check the sweep zone first to prevent races.  In older versions of this
%% function, during a migration, we (the current chain) could decide to
%% forward an op X to the new chain when the op's key was in the sweep zone.
%% However, if the new chain had a problem and the sweep ack didn't return to
%% us, we would time out and then resend the keys in the current sweep zone.
%% Those resent keys could clobber whatever op X had done over on the new
%% chain.
%%
%% If we determine that an op's key is in the sweep zone, we need to wait.

chain_all_do_list_keys_local_p(DoList, _DoFlags, S)
  when S#state.globalhash == undefined ->
    {true, harvest_do_keys(DoList, S)};
chain_all_do_list_keys_local_p(DoList, DoFlags, S) ->
    GH = S#state.globalhash,
    {BrickList, RdWr_Keys, _ReadOrWrite} = ExtractRes =
        extract_do_list_keys_find_bricks(DoList, GH, S),
    OutsideSweepRes = chain_all_keys_outside_sweep_zone(RdWr_Keys, S),
    case proplists:get_value(ignore_role, DoFlags) of
        true ->
            OutsideSweepRes;
        _ ->
            case OutsideSweepRes of
                {false, _FwdBricks} ->
                    OutsideSweepRes;
                {true, _} ->
                    %% We used to have an efficiency check here to do a little
                    %% less work if the squidflash_resubmit flag was present.
                    %% But that caused a problem: if the squidflash_resubmit
                    %% property was added by some other brick and then
                    %% forwarded to us due to migration, then bypassing some
                    %% paranoid checks is a bad, bad idea.  Besides, I think
                    %% it's possible to have this race with ourselves: the
                    %% sweep key advances during a squidflash priming.
                    Me = {S#state.name, node()},
                    case ExtractRes of
                        {[X], _RdWr_Keys, ReadOrWrite} when X == Me ->
                            case op_type_permitted_by_role_p(ReadOrWrite, S) of
                                true ->
                                    OutsideSweepRes;
                                false ->
                                    %% Perhaps our role will change soon?
                                    %% Forward to ourself to stall for time.
                                    {false, []}
                            end;
                        _ ->
                            Filtered = [X || X <- lists:usort(BrickList),
                                             X /= Me],
                            {false, Filtered}
                    end
            end
    end.

%% @spec (list({atom(), binary()}), state_r()) -> {true | false, list()}
%% @doc Continue the processing that chain_all_do_list_keys_local_p() was
%%      doing, but this time check if any keys fall inside the brick's
%%      current sweep zone.

chain_all_keys_outside_sweep_zone(RdWr_Keys, S) ->
    if not is_record(S#state.sweepstate, sweep_r) ->
            ?DBG_MIGRATEx({chain_all_keys_outside_sweep_zone, S#state.name,
                           not_sweeping, RdWr_Keys}),
            {true, RdWr_Keys};
       true ->
            {SweepA, SweepZ} = get_sweep_start_and_end(S),
            ?DBG_MIGRATEx({chain_all_keys_outside_sweep_zone, S#state.name,
                           SweepA, RdWr_Keys, SweepZ}),
            case [x || {_RdWr, K} <- RdWr_Keys, SweepA =< K, K =< SweepZ] of
                [] ->
                    {true, RdWr_Keys};
                [_|_] ->
                    %%?E_INFO("chain_all_keys_outside_sweep_zone: "
                    %%        "~p caught one: ~p =< ~p =< ~p\n",
                    %%        [S#state.name, SweepA, RdWr_Keys, SweepZ]),
                    %% At least ony key is in the sweep zone/range.  Return
                    %% empty list to signal forwarding to ourself.
                    {false, []}
            end
    end.

%% @spec (state_r()) -> undefined | {binary(), binary()}

get_sweep_start_and_end(S) ->
    if not is_record(S#state.sweepstate, sweep_r) ->
            undefined;
       true ->
            Sw = S#state.sweepstate,
            SweepA = sweep_zone_convert(Sw#sweep_r.done_sweep_key),
            SweepZ = sweep_zone_convert(Sw#sweep_r.sweep_key),
            {SweepA, SweepZ}
    end.

sweep_zone_convert(Key) ->
    if Key == ?BRICK__GET_MANY_FIRST ->
            ?BRICK__SMALLEST_KEY;
       Key == ?BRICK__GET_MANY_LAST ->
            ?BRICK__BIGGEST_KEY;
       true ->
            Key
    end.

%% @spec (state_r()) -> state_r()
%% @doc Process a request to flush unack'ed items in the log downstream.

do_chain_flush_log_downstream(S) ->
    CS = S#state.chainstate,
    Log1 = queue:to_list(CS#chain_r.down_unacked),
    {ImplMod, ImplState} = impl_details(S),
    Log2a = ImplMod:bcb_get_logging_op_q(ImplState),
    Log2 = [{ch_log_replay_v2, {S#state.name, node()}, LQI#log_q.logging_serial,
             LQI#log_q.thisdo_mods, LQI#log_q.from, LQI#log_q.reply, ignoreme_fixme}
            || LQI <- Log2a],
    ?DBG_CHAIN_TLOGx({chain_flush_log_downstream, S#state.name, down_unacked, {Log1, Log2}}),
    if true ->
            NewS =
                lists:foldl(
                  fun({ch_log_replay_v2, _UpstreamBrick,
                       _Serial, Thisdo_Mods, From, Reply, _ignore_fixme}, ST) ->
                          %% Assign new serial numbers to each replayed msg.
                          {ImplMod, IS} = impl_details(ST),
                          {Serial, IS2} = ImplMod:bcb_incr_logging_serial(IS),
                          ?DBG_CHAIN_TLOG("do_chain_flush_log_downstream: ~p: serial ~p\nMods = ~P\n", [S#state.name, Serial, Thisdo_Mods, 10]),
                          ST2 = ST#state{impl_state = IS2},
                          chain_send_downstream_iff_empty_log_q(
                            Serial, [], From, Reply, Thisdo_Mods, ST2)
                  end, S, Log1 ++ Log2),
            NewS
    end.

%% @spec (list(fun), term()) -> term()
%% @doc A cheap monad-like thing: given an initial state and a list of
%% functions, apply each function to that evolving state.
%%
%% This function is almost the same as foldl, almost.

cheap_monad(FunList, InitialAcc) ->
    lists:foldl(fun(Fun, Acc) -> Fun(Acc) end, InitialAcc, FunList).

%% @spec (chain_r(), serial_number, brick_name(), integer(), integer()) -> ok
%% @doc Send a repair ack message (asynchronously) to the upstream brick.

-spec send_repair_ack(chain_r(), serial_number(), brick_name(), integer(), integer())
                      -> ok.
send_repair_ack(CS, Serial, MyName, Inserts, Deletes) ->
    timer:sleep(proplists:get_value(repair_ack_sleep,
                                    CS#chain_r.proplist, 0)),
    Msg = {ch_repair_ack, Serial, MyName, node(), Inserts, Deletes},
    ?DBG_REPAIRx({send_repair_ack, MyName, Msg}),
    gen_server:cast(CS#chain_r.upstream, Msg).

send_repair_diff_round1_ack(CS, Serial, MyName, Unknown, Ds) ->
    timer:sleep(proplists:get_value(repair_round1_ack_sleep,
                                    CS#chain_r.proplist, 0)),
    gen_server:cast(CS#chain_r.upstream, {ch_repair_diff_round1_ack, Serial,
                                          MyName, node(),
                                          Unknown, Ds}).

%% @spec (chain_r(), serial_number(), brick_name(), atom(), proplist()) -> ok
%% @doc Send a serial number ack message (asynchronously) to the
%% upstream brick.
%%
%% The proplist in the upstream message was originally added as part
%% of a congestion control implementation, but if a brick's mailbox
%% gets overwhelmed with 1,000's of messages, the congestion control
%% data can be handled far too late to be effective.  I'm leaving it
%% in, as a general-purpose upstream communication channel.

send_serial_ack(CS, Serial, MyName, MyNode, Props) ->
    Msg = {ch_serial_ack, Serial, MyName, MyNode, Props},
    ?DBG_CHAINx({send_serial_ack, MyName, Msg}),
    gen_server:cast(CS#chain_r.upstream, Msg).

%% @spec (brick(), list()) -> void()
%% @doc For all do ops that would have modified data but the brick was in
%% read-only mode, resubmit those ops (after the brick has moved out of
%% read-only mode).

resubmit_all_queued_update_ops(Brick, Ws) ->
    %% Assume that Ws was maintained by caller in reverse order of arrival.
    lists:foreach(
      fun(#dirty_q{from = From, do_op = DoOp}) ->
              spawn(fun() ->
                            %% Remember: do/4 is a client-side API func.
                            case catch brick_server:do(Brick, node(), DoOp, 5*1000) of
                                {'EXIT', _X} ->
                                    exit(normal);
                                Res ->
                                    gen_server:reply(From, Res)
                            end
                    end)
      end, lists:reverse(Ws)).

%% @spec (serial_number(), undefined | queue()) -> queue()
%% @doc After the ack for Serial has been received from downstream, remove
%%      from Queue all updates before or equal to Serial.

pull_items_out_of_unacked(_Serial, undefined) ->
    undefined;
pull_items_out_of_unacked(Serial, Q) ->
    case queue:is_empty(Q) of
        true  -> Q;
        false -> pull_items_out_of_unacked(queue:out(Q), Serial, false, Q)
    end.

%% @spec (term(), serial_number(), boolean(), queue()) -> queue()
%% @doc See {@link pull_items_out_of_unacked/2}.

pull_items_out_of_unacked(
  {{value, {ch_log_replay_v2, _, Ser, _, _, _, _fixme}}, NewQ},
  Serial, _, _BeforeQ)
  when Ser =< Serial ->
    pull_items_out_of_unacked(queue:out(NewQ), Serial, Ser == Serial, NewQ);
pull_items_out_of_unacked({empty, NewQ}, _Serial, true, _BeforeQ) ->
    NewQ;
pull_items_out_of_unacked(_, _Serial, _Bool, BeforeQ) ->
    BeforeQ.

%% @spec (todo_list(), state_r()) -> state_r()
%% @doc For each "to do" in ToDos, send it downstream.

do_chain_todos(ToDos, S) ->
    lists:foldl(fun(T, S_) -> do_a_chain_todo(T, S_) end, S, ToDos).

%% @spec (todo(), state_r()) -> state_r()
%% @doc Send a single "to do" item downstream.

do_a_chain_todo({chain_send_downstream, Serial,
                 DoFlags, From, Reply, Thisdo_Mods},
                S) when is_record(S, state) ->
    chain_send_downstream(Serial, DoFlags, From, Reply, Thisdo_Mods, S).

do_chain_todos_iff_empty_log_q(ToDos, S) ->
    lists:foldl(fun(T, S_) -> do_a_chain_todo_iff_empty_log_q(T, S_) end,
                S, ToDos).

%% @spec (todo(), state_r()) -> state_r()
%% @doc Send a single "to do" item downstream.

do_a_chain_todo_iff_empty_log_q({chain_send_downstream, Serial,
                 DoFlags, From, Reply, Thisdo_Mods},
                S) when is_record(S, state) ->
    chain_send_downstream_iff_empty_log_q(Serial, DoFlags, From, Reply,
                                          Thisdo_Mods, S).

%%

%% @spec (serial_number(), state_r()) -> {list(), state_r()}
%% @doc Given the LastLogSerial number, remove all logging items out of
%% State's logging queue and update State's logging queue accordingly.

pull_items_out_of_logging_queue(LastLogSerial, S) ->
    Q = S#state.logging_op_q,
    pull_items_out_of_logging_queue(
      queue:out(Q), [], Q, LastLogSerial, -1, S).

%% @spec (term(), list(), queue(), serial_number(), serial_number(), state_r())
%%    -> {list(), state_r()}
%% @doc see {@link pull_items_out_of_logging_queue/2}.

pull_items_out_of_logging_queue({{value, LQI}, NewQ}, Acc, _OldQ,
                                 LastLogSerial, UpstreamSerial, S)
  when is_record(LQI, log_q), LQI#log_q.logging_serial =< LastLogSerial ->
    pull_items_out_of_logging_queue(
      queue:out(NewQ), [LQI|Acc], NewQ, LastLogSerial, UpstreamSerial, S);
pull_items_out_of_logging_queue({{value, {upstream_serial, NewUS}}, NewQ}, Acc,
                                _OldQ, LastLogSerial, _UpstreamSerial, S) ->
    pull_items_out_of_logging_queue(
      queue:out(NewQ), Acc, NewQ, LastLogSerial, NewUS, S);
pull_items_out_of_logging_queue(_, Acc, Q, _LastLogSerial,
                                _UpstreamSerial, S) ->
    {lists:reverse(Acc), S#state{logging_op_q = Q}}.

%%
%% Migration stuff
%%

%% @doc UNFINISHED.

do_migration_start_sweep(Cookie, MyChainName, Options, S) ->
    CS = S#state.chainstate,
    GH = S#state.globalhash,
    if S#state.sweepstate /= undefined ->
            Sw = S#state.sweepstate,
            {{error, migration_in_progress, Sw#sweep_r.cookie}, S};
       not is_record(S#state.globalhash, g_hash_r) ->
            {{error, bad_globalhash, S#state.globalhash}, S};
       not GH#g_hash_r.migrating_p ->
            {{error, gh_not_migrating, GH#g_hash_r.migrating_p}, S};
       CS#chain_r.role == standalone
         orelse
       (CS#chain_r.role == chain_member andalso
        CS#chain_r.upstream == undefined) ->
            {ImplMod, ImplState} = impl_details(S),
            Flog = fun() -> ?E_INFO("start_sweep: ~w starting to sweep ~p",
                                    [S#state.name, MyChainName])
                   end,
            resubmit_all_queued_update_ops(S#state.name,
                                           CS#chain_r.mig_start_waiters),
            CS2 = CS#chain_r{waiting_for_mig_start_p = false,
                             mig_start_waiters = []},

            case ImplMod:bcb_get_metadata(sweep_checkpoint, ImplState) of
                [] ->
%% DELME (brick_test0 tests (t61) break)
%                   %% Sanity check: Before telling each brick to
%                   %% start sweeping, a global hash record must be
%                   %% spammed to all bricks with the new_h_desc to
%                   %% prepare for the migration but the phase &
%                   %% migration status must be valid.  (The
%                   %% make_new_global_hash() func helps a bit with
%                   %% phase.)
%                   {phase, pre} = {phase, GH#g_hash_r.phase},

                    Sw = make_init_sweep(Cookie, MyChainName, Options, S),
                    Flog(),
                    GH2 = brick_hash:set_chain_sweep_key(
                            MyChainName, ?BRICK__GET_MANY_FIRST, GH),
                    {ok, S#state{chainstate = CS2,
                                 sweepstate = Sw,
                                 globalhash = GH2#g_hash_r{migrating_p = true,
                                                           phase = migrating}}};
                [{_, SwC}] when is_record(SwC, sweepcheckp_r),
                          SwC#sweepcheckp_r.cookie == Cookie,
                          SwC#sweepcheckp_r.chain_name == MyChainName ->
                    Sw = make_init_sweep(Cookie, MyChainName, Options, S),
                    %% We don't know the state of sweeping after
                    %% SwC#sweepcheckp_r.done_sweep_key, so we'll force
                    %% it to be done again because we *know* everything up
                    %% to SwC#sweepcheckp_r.done_sweep_key has been swept
                    %% already.
                    DoneKey =
                       if SwC#sweepcheckp_r.done_sweep_key
                           == '$end_of_table' ->
                               ?BRICK__BIGGEST_KEY;
                          true ->
                               SwC#sweepcheckp_r.done_sweep_key
                       end,
                    Sw2 = Sw#sweep_r{done_sweep_key = DoneKey,
                                     sweep_key      = DoneKey},
                    Flog(),
                    GH2 = brick_hash:set_chain_sweep_key(
                            MyChainName, DoneKey, GH),
                    {ok, S#state{chainstate = CS2,
                                 sweepstate = Sw2,
                                 globalhash = GH2#g_hash_r{migrating_p = true,
                                                           phase = migrating}}};
                [{_, SwC}] when is_record(SwC, sweepcheckp_r) ->
                    {{error, sweep_record_exists, SwC#sweepcheckp_r.cookie},S};
                [{_, X}] ->
                    {{error, sweep_record_bad, X}, S}
            end;
       true ->
            {{error, bad_role_my_upstream_is,
              {CS#chain_r.role, CS#chain_r.upstream}}, S}
    end.

%% @spec (state_r()) -> {{ok, now(), prop_list()}, state_r()} |
%%                      {{error, not_migrating | migrating, term()}, state_r()}
%% @doc Clear #state.sweepstate, iff migration has finished.

do_migration_clear_sweep(S) ->
    Sw = S#state.sweepstate,
    if not is_record(Sw, sweep_r) ->
            NewS = delete_sweep_cp(S),
            {{error, not_migrating, []}, NewS};
       true ->
            case Sw#sweep_r.stage of
                {done, When, PropList} ->
                    NewS = delete_sweep_cp(S),
                    {{ok, When, PropList}, NewS#state{sweepstate = undefined}};
                Stage ->
                    {{error, migrating, Stage}, S}
            end
    end.

%% @doc Start a migration sweep of this chain, assuming that all basic
%%      sanity checks (whether to start) have already been done.
%%
%% <ul>
%% <li> {interval, integer()} ... Interval to send kick_next_sweep
%%      messages.</li>
%% <li> {max_keys_per_iter, integer()} ... Maximum number of keys
%%      to examine per sweep iteration.  </li>
%% <li> {max_keys_per_chain, integer()} ... Maximum number of keys
%%      to send to any particular chain.  (Not yet implemented) </li>
%% <li> {propagation_delay, integer()} ... Number of milliseconds
%%      to delay for each brick's logging operation.  </li>
%% </ul>


make_init_sweep(Cookie, MyChainName, Options, S) ->
    BigDataDirP = impl_mod_uses_bigdata_dir(S),
    MaxKeysDefault =
        if BigDataDirP == false -> 500;  % Vals in RAM, no latency problem.
           true                 ->  25   % Vals on disk, be conservative.
        end,
    Interval = proplists:get_value(interval, Options, 50),
    MaxKeysPI = proplists:get_value(max_keys_per_iter, Options, MaxKeysDefault),
    MaxKeysPC = proplists:get_value(max_keys_per_chain, Options, 100),
    PropDelay = proplists:get_value(propagation_delay, Options, 0),
    {ok, TRef} = brick_itimer:send_interval(Interval, kick_next_sweep),
    #sweep_r{cookie = Cookie,
             started = now(),
             options = Options,
             bigdata_dir_p = BigDataDirP,
             tref = TRef,
             max_keys_per_iter = MaxKeysPI,
             max_keys_per_chain = MaxKeysPC,
             prop_delay = PropDelay,
             chain_name = MyChainName,
             done_sweep_key = ?BRICK__GET_MANY_FIRST,
             sweep_key = ?BRICK__GET_MANY_FIRST,
             stage = top}.

%% @doc UNFINISHED.

eat_kicks() ->
    receive kick_next_sweep -> eat_kicks()
    after 0                 -> ok
    end.

%% @doc UNFINISHED.

do_kick_next_sweep(S) ->
    do_kick_next_sweep((S#state.sweepstate)#sweep_r.stage, S).

do_kick_next_sweep(top, S)
  when (S#state.chainstate)#chain_r.read_only_p ->
    ?E_INFO("~s: in read-only mode, postponing next sweep\n", [S#state.name]),
    S;
do_kick_next_sweep(top, S) ->
    Sw = S#state.sweepstate,
    if Sw#sweep_r.sweep_key == ?BRICK__GET_MANY_LAST ->
            ?DBG_MIGRATEx({finished_sweeping, S#state.name}),

            %% We may be done sweeping ourselves, but other bricks may
            %% not be, and if we get a ch_sweep_from_other from one of
            %% them, the undefined will cause us to puke.

            brick_itimer:cancel(Sw#sweep_r.tref),

            %% Don't delete sweep checkpoint here.  If the chain
            %% crashes after deleting it, then when the chain will sweep
            %% the entire table again when restarted.
            %% Instead, update the sweep checkpoint with the last key sign.

            %% Update GH to avoid someone sneaking in an update at the very
            %% end of the table: if we update GH with ?BRICK__GET_MANY_LAST
            %% as the sweep key, such a race is impossible.
            ?DBG_MIGRATEx({set_chain_sweep_key, S#state.name, Sw#sweep_r.chain_name,
                           kick_next_sweep, ?BRICK__GET_MANY_LAST}),
            NewGH = brick_hash:set_chain_sweep_key(Sw#sweep_r.chain_name,
                                                   ?BRICK__GET_MANY_LAST,
                                                   S#state.globalhash),
            Stage = {done, now(), []},
            Last = ?BRICK__GET_MANY_LAST,
            NewS = S#state{sweepstate = Sw#sweep_r{stage = Stage,
                                                   tref = undefined,
                                                   done_sweep_key = Last,
                                                   sweep_key = Last},
                           globalhash = NewGH},
            SwC = #sweepcheckp_r{cookie = Sw#sweep_r.cookie,
                                 chain_name = Sw#sweep_r.chain_name,
                                 done_sweep_key = Last,
                                 sweep_key = Last},
            Mod = {plog_sweep, phase1_sweep_info, SwC},
            %% Note: phase1 key announcement doesn't need
            %% logging, just send downstream.
            {LoggingSerial, NewS1} = incr_impl_logging_serial(NewS),
            ?DBG_MIGRATEx({do_kick_next_sweep1, S#state.name, LoggingSerial}),
            NewS2 = chain_send_downstream_iff_empty_log_q(
                      LoggingSerial, [], <<"no 1">>, <<"no 2">>, Mod, NewS1),
            NewS3 = write_sweep_cp(SwC, NewS2),
            NewS3;
       true ->
            CS = S#state.chainstate,
            if CS#chain_r.role == standalone
               orelse
               (CS#chain_r.role == chain_member andalso
                CS#chain_r.official_tail == true) ->
                    %% We have nobody downstream of us, so no need for
                    %% a pre-sweep announcement down our chain.
                    kick_next_sweep_only_one_phase(Sw, S);
               true ->
                    %% We need to do phase 1 of the sweep: send an
                    %% announcement of the sweep's last key down our chain
                    %% to keep strong consistency in pathological cases.
                    {KeyTS_list, LastKey} = get_sweep_tuples(
                                              Sw#sweep_r.sweep_key,
                                              Sw#sweep_r.max_keys_per_iter,
                                              true, S),
                    ?DBG_MIGRATEx({kick__phase1, S#state.name, Sw#sweep_r.done_sweep_key, Sw#sweep_r.sweep_key, KeyTS_list, LastKey}),
                    if KeyTS_list == [] ->
                            ?E_INFO("do_kick_next_sweep: ~p: END!", [S#state.name]),
                            do_kick_next_sweep_keyts_list_empty(S);
                       true ->
                            %% TODO: This sweep notification doesn't have
                            %% to be sent strictly down the chain: it could be
                            %% broadcast in parallel to all chain members.
                            %% However, I don't want to complicate a first
                            %% draft too much ... this way is easier, hrm??
                            %% Also note: we're not sending a thisdo_mod
                            %% *LIST*, we're sending a *TUPLE*.  The
                            %% receiver has special code to handle a *TUPLE*.
                            %%
                            SwC = #sweepcheckp_r{cookie = Sw#sweep_r.cookie,
                                                 chain_name = Sw#sweep_r.chain_name,
                                                 done_sweep_key =
                                                     Sw#sweep_r.done_sweep_key,
                                                 sweep_key = LastKey},
                            From = {sweep_cast, self()},
                            Mod = {plog_sweep, phase1_sweep_info, SwC},
                            Reply = {sweep_phase1_done, LastKey},
                            %% Note: phase1 key announcement doesn't need
                            %% logging, just send downstream.
                            {LoggingSerial, NewS} = incr_impl_logging_serial(S),
                            ?DBG_MIGRATEx({do_kick_next_sweep2, S#state.name, LoggingSerial}),
                            %% OK, here's the site of the start of a really,
                            %% *really* pernicious bug, took 3 days to find.
                            %% If the log op number LS = LoggingSerial - 1 is
                            %% currently in #state.logging_op_q, then we
                            %% *must not* send this sweep now!  If we do, we'll
                            %% send out-of-order serials to our downstream:
                            %% Mod might leapfrog ahead of one or more ops
                            %% currently in logging_op_q.
                            NewS1 = chain_send_downstream_iff_empty_log_q(
                                      LoggingSerial, [], From,
                                      Reply, Mod, NewS),
                            NewS2 = write_sweep_cp(SwC, NewS1),
                            Stage = {notify_down_old, now()},
                            %% Do not set the global hash's sweep key here.
                            NewS2#state{sweepstate =
                                         Sw#sweep_r{stage = Stage,
                                                    sweep_key = LastKey}}
                    end
            end
    end;
do_kick_next_sweep({done, _, _}, S) ->
    S;
do_kick_next_sweep(priming_value_blobs, S) ->
    %% Still waiting for disk I/O to finish.
    S;
do_kick_next_sweep(_Stage, S) ->
    %% io:format("QQQ: ~p HERE IN LAST CLAUSE\n", [S#state.name]),
    Sw = S#state.sweepstate,
    StageName = element(1, Sw#sweep_r.stage),
    UpdateSent = element(2, Sw#sweep_r.stage),
    case timer:now_diff(now(), UpdateSent) of
        N when N > 2*1000*1000 ->               % TODO: configurable!
            ReKey = Sw#sweep_r.done_sweep_key,
            ?E_INFO("do_kick_next_sweep: brick ~p, stage name "
                     "= ~p, update timeout error, resending key ~p (~p)\n",
                     [S#state.name, StageName, ReKey, begin {ImplMod, ImplState} = impl_details(S), element(1, ImplMod:bcb_get_many(ReKey, [witness, {max_num, 1}], ImplState)) end]),
            Sw2 = Sw#sweep_r{stage = top,
                             val_prime_lastkey = undefined,
                             done_sweep_key = ReKey,
                             sweep_key = ReKey},
            %% Unlike earlier comments in this place, I've now
            %% determined that it is *not* safe to wait for the next
            %% kick sweep to arrive.  We must re-calculate the sweep
            %% now.
            %%
            %% Recursion is our friend, to save us from the perils of
            %% a race condition with a 'do' op that arrives before the
            %% next kick.  If we use the Sw2 above and we wait for a
            %% kick, and if a 'do' op arrives, then the two sweep keys
            %% are the same, so chain_all_keys_outside_sweep_zone()
            %% will incorrectly assume that the 'do' op's key is OK
            %% when it might not be.....
            do_kick_next_sweep(S#state{sweepstate = Sw2});
        _ ->
            S
    end.

do_kick_next_sweep_keyts_list_empty(S) ->
    %% Avoid race condition by using mutual recursion.
    %% If we returned control to the gen_server framework, and we
    %% merely waited for next kick msg to call this func with
    %% sweep_key = ?BRICK__GET_MANY_LAST, then there's a race
    %% condition!
    %% Someone could insert a key *after* the table's
    %% current last key, and that key would never be swept.
    %%
    %% By calling sweep_move_or_keep() with LastKey = ?BRICK__GET_MANY_LAST,
    %% we'll do The Right Thing to avoid the race.
    %%
    %% Furthermore, we'll avoid a ping-pong condition: see the
    %% empty table ping-pong bugfix (brick_test0:chain_t61() race #3).
    ?DBG_MIGRATEx({do_kick_next_sweep_keyts_list_empty, S#state.name}),
    sweep_move_or_keep([], ?BRICK__GET_MANY_LAST, S).

%% This version called when sweep phase1 was not required (chain length = 1).
%% We retrieve full key/val terms.

kick_next_sweep_only_one_phase(Sw, S) ->
    FirstKey = Sw#sweep_r.done_sweep_key,
    {KeyTS_list, LastKey} = get_sweep_tuples(
                              FirstKey, Sw#sweep_r.max_keys_per_iter,
                              true, S),
    ?DBG_MIGRATEx({kick__only_one_phase, S#state.name, Sw#sweep_r.done_sweep_key, Sw#sweep_r.sweep_key, KeyTS_list, LastKey}),
    %% Do not set the global hash's sweep key here.
    SwC = #sweepcheckp_r{cookie = Sw#sweep_r.cookie,
                         chain_name = Sw#sweep_r.chain_name,
                         done_sweep_key = Sw#sweep_r.done_sweep_key,
                         sweep_key = LastKey},
    NewS = write_sweep_cp(SwC, S),
    kick_next_sweep_phase2b(NewS, KeyTS_list, FirstKey, LastKey).

%% This version called when sweep phase1 was required (chain length > 1).
%% LastKey is the key advertised down our chain by the sweep phase1.
%% We start the computation for the 2nd phase of the sweep.
%%
%% This implementation is complicated by the fact that we do not
%% restrict the add/delete operations on the brick while the phase 1
%% message was propagating down the chain.  Therefore, if we call
%% get_sweep_tuples with the same max keys arg, we may get a different
%% list.  We need to double-check:
%%   * if the list contains keys > LastKey ... need to prune the list.
%%   * if the list's last key is not LastKey ... need to add to the list.

-spec kick_next_sweep_do_phase2(#sweep_r{}, sweep_key(), #state{}) -> #state{} | no_return().
kick_next_sweep_do_phase2(Sw, Phase1LastKey, S) ->
    FirstKey = Sw#sweep_r.done_sweep_key,
    {KeyTS_list0, LastKey} = get_sweep_tuples(
                               FirstKey, Sw#sweep_r.max_keys_per_iter,
                               true, S),
    ?DBG_MIGRATEx({kick__phase2, S#state.name, Sw#sweep_r.done_sweep_key,
                   Sw#sweep_r.sweep_key, KeyTS_list0, LastKey,
                   ph1last, Phase1LastKey}),
    %% io:format("QQQ: ~p phase2: list = ~p\n", [S#state.name, KeyTS_list0]),
    if LastKey =:= Phase1LastKey
       orelse
       Phase1LastKey == ?BRICK__GET_MANY_LAST ->
            ?DBG_MIGRATEx({kick__phase2__t, S#state.name}),
            kick_next_sweep_phase2b(S, KeyTS_list0, FirstKey, Phase1LastKey);
       LastKey > Phase1LastKey ->
            KeyTS_list = lists:filter(
                           fun({X, _TS}) when X =< Phase1LastKey -> true;
                              ({_, _TS})                         -> false
                           end, KeyTS_list0),
            ?DBG_MIGRATEx({kick__phase2__u, S#state.name, filtered, KeyTS_list}),
            kick_next_sweep_phase2b(S, KeyTS_list, FirstKey, Phase1LastKey);
       LastKey < Phase1LastKey ->
            %% Holy cow, I've spent a lot of time debugging this simple
            %% loop.  It's amazing how many bugs-while-fixing-other-bugs
            %% that I've managed to introduce here.  {sigh} Perhaps the
            %% presents of all the ?DBG macros is a hint?
            ?DBG_MIGRATEx({kick__phase2__v, S#state.name, LastKey, Phase1LastKey}),
            F = fun({Acc, LK}) ->
                        ?DBG_MIGRATEx({kick__phase2__ww, S#state.name, LK, Acc}),
                        {KTSL, NewLK} = get_sweep_tuples(LK, 1, true, S),
                        if NewLK == ?BRICK__GET_MANY_LAST; KTSL == [] ->
                                ?DBG_MIGRATEx({kick__phase2__xx, S#state.name, KTSL, NewLK}),
                                {false, Acc ++ KTSL};
                           NewLK =< Phase1LastKey ->
                                ?DBG_MIGRATEx({kick__phase2__yy, S#state.name, KTSL, NewLK}),
                                {true, {Acc ++ KTSL, NewLK}};
                           true ->
                                ?DBG_MIGRATEx({kick__phase2__zz, S#state.name, KTSL, NewLK}),
                                {false, Acc}
                        end
                end,
            KeyTS_list = KeyTS_list0 ++ gmt_loop:do_while(F, {[], LastKey}),
            kick_next_sweep_phase2b(S, KeyTS_list, FirstKey, Phase1LastKey)
    end.

-spec kick_next_sweep_phase2b(#state{}, list(tuple()), sweep_key(), sweep_key()) -> #state{} | no_return().
kick_next_sweep_phase2b(S, KeyTS_list, FirstKey, LastKey) ->
    {ImplMod, ImplState} = impl_details(S),
    ?DBG_MIGRATEx({kick__phase2b, S#state.name, top, KeyTS_list, FirstKey, LastKey}),
    case ImplMod:bcb_dirty_keys_in_range(FirstKey, LastKey, ImplState) of
        [] ->
            kick_next_sweep_phase2c(S, KeyTS_list, LastKey);
        [_|_] = Ds ->
            %% We're trying to sweep some dirty keys, so postpone what
            %% we were going to do and let the next kick get us again.
            ?DBG_MIGRATEx({kick__phase2b, S#state.name, dirty, FirstKey, LastKey, Ds}),
            ?E_INFO("migration: ~p: ~p dirty keys, pausing to finish logging",
                    [S#state.name, length(Ds)]),
            S
    end.

-spec kick_next_sweep_phase2c(#state{}, list(tuple()), sweep_key()) -> #state{} | no_return().
kick_next_sweep_phase2c(S, KeyTS_list, LastKey) ->
    Sw = S#state.sweepstate,
    ?DBG_MIGRATEx({kick__phase2c, S#state.name, Sw#sweep_r.done_sweep_key, Sw#sweep_r.sweep_key, KeyTS_list, LastKey}),
    if LastKey == ?BRICK__GET_MANY_LAST, KeyTS_list == [] ->
            ?E_INFO("kick_next_sweep_phase2c: ~p: END!", [S#state.name]),
            do_kick_next_sweep_keyts_list_empty(S);
       true ->
            ListWithBricks = sweep_calc_bricks(KeyTS_list, S),
            %% io:format("QQQ: ~p LwB ~p\n", [S#state.name, ListWithBricks]),
            %%?DBG(ListWithBricks),
            sweep_move_or_keep(ListWithBricks, LastKey, S)
    end.

%% TODO: In theory, we only need to calculate the *new* hash, because
%% 100% of the *current* hash->chain should be for this very chain.
%% The only reason that we'd want to calculate the old hash->chain is
%% to be paranoid to catch bugs.
-spec sweep_calc_bricks(list(tuple()), #state{}) -> list({chain_name(), chain_name(), tuple()}).
sweep_calc_bricks(List, S) ->
    GH = S#state.globalhash,
    ?DBG_MIGRATEx({sweep_calc_bricks, S#state.name, minor_rev, GH#g_hash_r.minor_rev}),
    %% Create temporary GH records to get the behavior we need.

    %% If/when you examine the gmt_elog traces, don't be fooled by the
    %% fact that brick_hash.erl thinks that migration isn't in effect.
    GHcurrent = GH#g_hash_r{migrating_p = false, phase = pre},
    GHnew = GH#g_hash_r{migrating_p = false, phase = pre,
                        current_h_desc = GH#g_hash_r.new_h_desc,
                        current_chain_dict = GH#g_hash_r.new_chain_dict},
    %% io:format("QQQ: new desc ~p\n", [GH#g_hash_r.new_h_desc]),
    sweep_calc_bricks(List, GHcurrent, GHnew, []).

-spec sweep_calc_bricks(list(tuple()), #g_hash_r{}, #g_hash_r{}, list({chain_name(), chain_name(), tuple()})) -> list({chain_name(), chain_name(), tuple()}).
sweep_calc_bricks([Item|T], GHcurrent, GHnew, Acc) ->
    Key = element(1, Item),
    ChainCurrent = brick_hash:key_to_chain(Key, GHcurrent),
    ChainNew = brick_hash:key_to_chain(Key, GHnew),
    sweep_calc_bricks(T, GHcurrent, GHnew,
                      [{ChainCurrent, ChainNew, Item}|Acc]);
sweep_calc_bricks([], _GHc, _GHn, Acc) ->
    lists:reverse(Acc).

%% TODO: Replace orddict with something more scalable.  Benchmarking
%%       probably needed (for occasional add (max ~100) and
%%       lots of replace ops (max ~500).
-spec sweep_move_or_keep(list({chain_name(), chain_name(), tuple()}), sweep_key(), #state{}) -> #state{} | no_return().
sweep_move_or_keep(ListWithBricks, LastKey, S) ->
    Sw = S#state.sweepstate,

    %% Sort keys based on move or keep calc made earlier.
    {MoveDict0, Thisdo_Mods} = sweep_move_or_keep2(ListWithBricks,
                                                   Sw#sweep_r.chain_name, S),
    %% io:format("QQQ: ~p MoveDict0 ~p\n", [S#state.name, MoveDict0]),

    if Sw#sweep_r.bigdata_dir_p == true
       andalso Sw#sweep_r.val_prime_lastkey == undefined ->
            %% Thisdo_Mods is the list of keys that we would be
            %% deleting after phase 2 of the sweep.  They're the keys
            %% that we need to prime: slurp them off of slow slow disk
            %% and get them into the OS page cache.
            Keys = lists:map(fun({delete_noexptime, K}) -> K end, Thisdo_Mods),
            %% io:format("QQQ: ~p: priming keys: ~p\n", [S#state.name, Keys]),
            ?DBG_MIGRATEx({kick__phase2_prime, S#state.name, Keys, LastKey}),
            spawn_val_prime_worker_for_sweep(Keys, LastKey, S),
            S#state{sweepstate =
                    Sw#sweep_r{stage = priming_value_blobs,
                               val_prime_lastkey = LastKey}};
       Sw#sweep_r.bigdata_dir_p == false
       orelse
       (Sw#sweep_r.val_prime_lastkey /= undefined)  ->
            %% io:format("QQQ: ~p: post priming: ~p\n", [S#state.name, MoveDict0]),
            %% io:format("QQQ: ~p: post priming: keys = ~p\n", [S#state.name, ListWithBricks]),
            %% We've got witness-style tuples, but we need value
            %% blobs, expiration times, and flags.  Go get the extra
            %% stuff before sending them anywhere.
            Tmp = lists:map(
                    fun({Chain, WitnessTs}) ->
                            {Chain,
                             [witness_tuple_to_insert_tuple(WT, S) ||
                                 {insert, WT} <- WitnessTs]}
                    end, orddict:to_list(MoveDict0)),
            MoveDict = orddict:from_list(Tmp),
            ?DBG_MIGRATEx({kick__phase2_sweep_or_move, S#state.name, ListWithBricks, LastKey}),
            sweep_move_or_keep3(LastKey, MoveDict, Thisdo_Mods, S);
       true ->
            ?E_WARNING("~p: stage ~p, bigdata ~p, prime_lastkey ~p\n",
                       [S#state.name, Sw#sweep_r.stage,
                        Sw#sweep_r.bigdata_dir_p,Sw#sweep_r.val_prime_lastkey]),
            exit({sweep_move_or_keep, S#state.name, Sw#sweep_r.stage,
                  Sw#sweep_r.bigdata_dir_p,Sw#sweep_r.val_prime_lastkey})
    end.

-spec sweep_move_or_keep3(sweep_key(), orddict(), list({delete_noexptime, term()}), #state{}) -> #state{}.
sweep_move_or_keep3(LastKey, MoveDict, Thisdo_Mods, S) ->
    Sw = S#state.sweepstate,

    %%?DBG(LastKey),
    %%?DBG(MoveDict),
    %%?DBG(Thisdo_Mods),

    %% Cannot send changes for this chain downstream: if there's
    %% a failure in this chain, we need to have the keys/vals
    %% available to reconstruct & resend.

    PropDelay = Sw#sweep_r.prop_delay,

    %% Send changes to all other chains.  For all chains that
    %% don't have real updates, send an update anyway (with an
    %% empty thisdo_mods list).
    %%
    MoveDictList_0 = orddict:to_list(MoveDict),
    MoveChains = [Chain || {Chain, _} <- MoveDictList_0],
    AllChains_0 = brick_hash:all_chains(S#state.globalhash, new),
    AllChains = [ChainName || {ChainName, _BrickList} <- AllChains_0],
    NoUpdateChains = AllChains -- MoveChains,
    MoveDictList_1 = [{Ch, []} || Ch <- NoUpdateChains] ++ MoveDictList_0,

    %% Do not send ch_sweep_from_other to myself.
    MeBrick = {S#state.name, node()},
    MoveDictList =
        lists:foldl(fun({ChainName, Mods_0}, Acc) ->
                            Brick = brick_hash:chain2brick(
                                 ChainName, write, S#state.globalhash, new),
                            if Brick == MeBrick ->
                                    Acc;
                               true ->
                                    [{Mods_0, Brick}|Acc]
                            end
                    end, [], MoveDictList_1),
    lists:map(
      fun({Mods_0, Brick}) ->
              Mods = if PropDelay == 0 -> Mods_0;
                        true           -> [{log_directive, map_sleep,
                                            PropDelay}|Mods_0]
                     end,
              Msg = {ch_sweep_from_other, self(),
                     Sw#sweep_r.chain_name, Mods, LastKey},
              gen_server:cast(Brick, Msg)
      end, MoveDictList),

    ?DBG_MIGRATEx({set_chain_sweep_key, S#state.name, Sw#sweep_r.chain_name,
                   sweep_move_or_keep3, LastKey}),
    NewGH = brick_hash:set_chain_sweep_key(
              Sw#sweep_r.chain_name, LastKey, S#state.globalhash),
    NeedReplies = length(MoveDictList),
    if NeedReplies == 0 ->
            %% Well, there's nothing to do for this scan.  We will
            %% wait for the next scheduled kick to trigger a new scan.

            %%{todo_bummer2399, []} = {todo_bummer2399, Thisdo_Mods}, % sanity!
            true = (Thisdo_Mods == []),         % sanity!
            SwC = #sweepcheckp_r{cookie = Sw#sweep_r.cookie,
                                 chain_name = Sw#sweep_r.chain_name,
                                 done_sweep_key = LastKey,
                                 sweep_key = LastKey},
            NewS = write_sweep_cp(SwC, S),
            %% io:format("QQQ: sweep_move_or_keep3: ~p NOREPLIES\n", [S#state.name]),
            NewS#state{sweepstate = Sw#sweep_r{sweep_key = LastKey,
                                               done_sweep_key = LastKey,
                                               stage = top,
                                               val_prime_lastkey = undefined},
                       globalhash = NewGH};
       true ->
            Stage = {collecting_phase2_replies, now(), MoveDictList},
            TDM = if PropDelay == 0 ->
                          Thisdo_Mods;
                     true ->
                          [{log_directive, map_sleep, PropDelay}|Thisdo_Mods]
                  end,
            S#state{sweepstate = Sw#sweep_r{sweep_key = LastKey,
                                            stage = Stage,
                                            phase2_replies = [],
                                            num_phase2_replies = 0,
                                            phase2_needed_replies =
                                                                  NeedReplies,
                                            phase2_del_thisdo_mods = TDM},
                    globalhash = NewGH}
    end.

-spec sweep_move_or_keep2(list({chain_name(), chain_name(),tuple()}), chain_name(), #state{}) -> {orddict(), list({delete_noexptime, term()})}.
sweep_move_or_keep2(ListWithBricks, ChainName, S) ->
    sweep_move_or_keep2(ListWithBricks, orddict:new(), [], ChainName, S).

-spec sweep_move_or_keep2(list({chain_name(), chain_name(),tuple()}), orddict(), list({delete_noexptime, term()}), chain_name(), #state{}) -> {orddict(), list({delete_noexptime, term()})}.
sweep_move_or_keep2([{CHcur, CHnew, Item}|T], Dict, Thisdo_Mods, MyChainName,
                    S) ->
    if MyChainName /= CHcur, MyChainName /= CHnew ->
            Sw = S#state.sweepstate,
            ?E_ERROR("BAD: Item ~p stored by brick ~p in "
                     "chain ~p, but should be stored in "
                     "current chain ~p or new chain ~p.\n"
                     "val_prime_lastkey = ~p\n"
                     "GH = ~p\n",
                     [Item, S#state.name, MyChainName,
                      CHcur, CHnew, Sw#sweep_r.val_prime_lastkey,
                      S#state.globalhash]),
            sweep_move_or_keep2(T, Dict, Thisdo_Mods, MyChainName, S);
       MyChainName == CHnew ->
            sweep_move_or_keep2(T, Dict, Thisdo_Mods, MyChainName, S);
       true ->
            X = {insert, Item},
            NewL = case orddict:find(CHnew, Dict) of
                       {ok, L} -> [X|L];
                       error   -> [X]
                   end,
            %% TODO: major contamination??
            Key = element(1, Item),
            %% To create a real thisdo mod to delete a key, we also
            %% need to know its expiration time.  However, we don't
            %% have that info available.  So we introduce a new kind
            %% of thisdo mod tuple: delete-but-we-don't-know-the-exptime.
            NewThisdo_Mods = [{delete_noexptime, Key}|Thisdo_Mods],
            sweep_move_or_keep2(T, orddict:store(CHnew, NewL, Dict),
                               NewThisdo_Mods, MyChainName, S)
    end;
sweep_move_or_keep2([], Dict, Thisdo_Mods, _MyChainName, _S) ->
    {Dict, Thisdo_Mods}.

witness_tuple_to_insert_tuple({Key, _TS}, S) ->
    {ImplMod, ImplState} = impl_details(S),
    [{ValRamP, ST}] = ImplMod:bcb_lookup_key_storetuple(Key, true, ImplState),
    if ValRamP == false -> {insert, ST};
       ValRamP == true  -> {insert_value_into_ram, ST}
    end.

spawn_val_prime_worker_for_sweep(Keys, LastKey, S) ->
    spawn_val_prime_worker(
      Keys, {priming_value_blobs_done_for_sweep, LastKey}, S).

spawn_val_prime_worker_for_repair(Serial, Unknown, Ds, S) ->
    spawn_val_prime_worker(
      Unknown, {priming_value_blobs_done_for_repair, Serial, Unknown, Ds}, S).

spawn_val_prime_worker(Keys, DoneMsg, S) ->
    CS = S#state.chainstate,
    MaxPrimers = CS#chain_r.max_primers,
    ParentPid = self(),
    spawn(fun() ->
                  val_prime_worker(Keys, ParentPid, DoneMsg, MaxPrimers, S)
          end).

%% TOM REVIEW the types here. That any in particular.
-spec val_prime_worker([key()], pid(), any(), integer(), #state{}) -> no_return().
val_prime_worker(Keys, ParentPid, DoneMsg, MaxPrimers, S) ->
    _ = gmt_pmap:pmap(fun(Key) -> squidflash_bcb_prime_key(Key, S) end,
                      Keys, MaxPrimers),
    ParentPid ! DoneMsg,
    unlink(ParentPid),
    exit(normal).

%% TOM REVIEW the types here. That any in particular.
-spec squidflash_bcb_prime_key(key(), #state{}) ->  [any()].
squidflash_bcb_prime_key(Key, S) ->
    {ImplMod, ImplState} = impl_details(S),
    ImplMod:bcb_lookup_key(Key, true, ImplState).

%% @spec (pid(), atom(), thisdo_mods(), term(), state_r()) ->
%%       gen_server_handle_cast_reply()
%% @doc Process a ch_sweep_from_other message.
%%
%% This is a bit of a tricky thing.  There's a lot of code in the
%% implementation-specific layer that manages dirty keys *and* scanning
%% of keys for migration purposes.
%%
%% There's a tricky interaction with log replay and sync logging: no
%% changes are made to the implementation-specific's storage until the
%% sync logging op is complete.  So it's possible to see this sequence
%% of events:
%%
%% 0. Key K exists in brick B1, K does not exist on B2.
%% 0b. Migration starts.  Key needs to move eventually from B1 to B2.
%% 1. A ch_sweep_from_other msg arrives at brick B2 with an insert for key K.
%% 2. Client wishes to delete K, sends request to B1.
%% 3. B2 has already scanned past K, so B1 forwards the request to B2.
%% 4. B2 looks in impl-specific storage, finds that K isn't there, and
%%    therefore returns key_not_exist.
%% 5. B2's log sync op finishes, so B2 applies the Thisdo_Mods to its
%%    local storage.
%% 6. K exists on B2 when it should *not*.
%%
%% The root problem: The never-never-land of an op that's been written
%% to the log but not yet sync'ed and thus not yet applied to local
%% storage.  We use the "dirty key" mechanism to avoid this problem for
%% regular 'do' ops.  The log replay mechanism doesn't have the
%% never-never-land problem in a non-migrating chain because middle
%% and tail bricks do not have a dirty key race problem.
%%
%% The solution: Change the sweep insert mods into 'do' ops, then process
%% the 'do' ops as usual ... while sending the same
%% reply back to the sweeping/sending brick.
%%
%% QQQQQQQQQ the problem: how to get the {sweep_phase2_done, ...} reply sent!!
%% Answer: 1. Use bogus pid as From for impl-specific do processing.
%%            incorrect [ok, ok, ...] reply will not be sent.
%%         2. Use a separate log replay for real answer.
%% Perhaps a small amount of latency, perhaps across two sync ops instead
%% of one?
-spec do_ch_sweep_from_other(pid(), chain_name(), list(), sweep_key(), #state{}) -> {noreply, #state{}}.
do_ch_sweep_from_other(ChainHeadPid, ChainName, Thisdo_Mods, LastKey, S) ->
    GH = S#state.globalhash,
    if not GH#g_hash_r.migrating_p ->
            ?E_ERROR(
              "Brick ~p received ch_sweep_from_other from chain ~p (~p) "
              "while we're not migrating\n",
              [S#state.name, ChainName, ChainHeadPid]),
            {noreply, S};
       not is_record(S#state.sweepstate, sweep_r) ->
            ?E_INFO(
              "do_ch_sweep_from_other: ~w got sweep from chain ~p, "
              "not sweeping here, dropping it\n", [S#state.name, ChainName]),
            {noreply, S};
       true ->
            NewS = if GH#g_hash_r.phase == pre ->
                           ?E_INFO(
                             "Brick ~p got a ch_sweep_from_other message "
                             "before being told to start its own "
                             "migration sweep\n", [S#state.name]),
                           MigGH = GH#g_hash_r{phase = migrating},
                           S#state{globalhash = MigGH};
                      true ->
                           S
                   end,
            Sw = NewS#state.sweepstate,
            %% Craft a reply term, using the sweep_cast method to ...
            From = {sweep_cast, ChainHeadPid},
            Reply = {sweep_phase2_done, LastKey,
                     %% The 'done_chain' prop is probably badly named.
                     %% It names the chain that has processed this sweep from
                     %% the ChainName chain.
                     [{done_chain, Sw#sweep_r.chain_name},
                      {done_brick, {NewS#state.name, node()}}]},
            %% ... send it down the chain.
            {LoggingSerial, NewS1} = incr_impl_logging_serial(NewS),
            ?DBG_MIGRATEx({do_ch_sweep_from_other, S#state.name, LoggingSerial}),
            {NewS2, LogWriteIsWaitingForSync} =
                chain_do_log_replay(LoggingSerial, [], Thisdo_Mods,
                                    From, Reply, NewS1),
            NewS3 = if LogWriteIsWaitingForSync == wait ->
                            {ImplMod, ImplState} = impl_details(NewS2),
                            ImplState2 = ImplMod:bcb_add_mods_to_dirty_tab(
                                           Thisdo_Mods, ImplState),
                            NewS2#state{impl_state = ImplState2};
                       true ->
                            NewS2
                    end,
            ?DBG_MIGRATEx({set_chain_sweep_key, S#state.name, ChainName,
                           ch_sweep_from_other, LastKey}),
            NewGH = brick_hash:set_chain_sweep_key(ChainName, LastKey,
                                                   NewS3#state.globalhash),
            {LoggingSerial2, NewS4} = incr_impl_logging_serial(NewS3),
            %% We need to send this other chain's sweep info downstream
            %% so that we don't get endless ping-pong fwd'ing.
            %% ALSO: Our downstream must not confuse this sweep key with
            %%       its own chain sweep key.  Use a variation of
            %%       the plog_sweep:
            %%       {plog_sweep, from_other, #sweepcheckp_r}
            SwC = #sweepcheckp_r{cookie = Sw#sweep_r.cookie,
                                 chain_name = ChainName,
                                 done_sweep_key = LastKey,
                                 sweep_key = LastKey},
            From2 = <<"">>, % binary From == noreply! by chain_send_downstream()
            Mod2 = {plog_sweep, from_other, SwC},
            Reply2 = no_reply_sent,
            NewS5 = chain_send_downstream_iff_empty_log_q(
                      LoggingSerial2, [], From2, Reply2, Mod2, NewS4),
            {noreply, NewS5#state{globalhash = NewGH}}
    end.

-spec do_sweep_phase2_done(sweep_key(), brick_bp:proplist(), #state{}) -> {noreply, #state{}}.
do_sweep_phase2_done(Key, PropList, S) ->
    Sw = S#state.sweepstate,
    case Sw#sweep_r.stage of
        {collecting_phase2_replies, _, _} ->
            do_sweep_phase2_done2(Sw, Key, PropList, S);
        _Else ->
            %% We're in another phase of migration.  But someone else had
            %% a forced flush downstream, so we get a 2nd copy of the
            %% phase2 done message.  Nothing to do here.
            {noreply, S}
    end.

-spec do_sweep_phase2_done2(#sweep_r{}, sweep_key(), brick_bp:proplist(), #state{}) -> {noreply, #state{}}.
do_sweep_phase2_done2(Sw, Key, PropList, S) ->
    {collecting_phase2_replies, _Time, _MoveDictList} = Sw#sweep_r.stage,
    {qqq_sweep_key, Key} = {qqq_sweep_key, Sw#sweep_r.sweep_key},

    DoneChain = proplists:get_value(done_chain, PropList),
    ReplyList = [DoneChain|Sw#sweep_r.phase2_replies],
    NumReplies = Sw#sweep_r.num_phase2_replies + 1,
    if NumReplies < Sw#sweep_r.phase2_needed_replies ->
            {noreply, S#state{sweepstate =
                              Sw#sweep_r{phase2_replies = ReplyList,
                                         num_phase2_replies = NumReplies}}};
       true ->
            ActualReplies = gmt_util:list_unique(lists:sort(ReplyList)),
            if length(ActualReplies) == Sw#sweep_r.phase2_needed_replies ->
                    LoggingSerial = peek_impl_logging_serial(S),
                    {NewS1, _} =
                        chain_do_log_replay(LoggingSerial, [], %qqq_xxx_yyy_fixed_I_hope
                                            Sw#sweep_r.phase2_del_thisdo_mods,
                                            <<"no from">>, ignored, S),
                    Sw2 = Sw#sweep_r{stage = top,
                                     val_prime_lastkey = undefined,
                                     done_sweep_key = Key,
                                     sweep_key = Key,
                                     phase2_del_thisdo_mods = []},
                    NewS2 = do_kick_next_sweep(NewS1#state{sweepstate = Sw2}),
                    {noreply, NewS2};
               true ->
                    {noreply, S#state{sweepstate =
                                      Sw#sweep_r{phase2_replies = ReplyList,
                                             num_phase2_replies = NumReplies}}}
            end
    end.

op_type_permitted_by_role_p(ReadOrWrite, S) ->
    CS = S#state.chainstate,
    if ReadOrWrite == read ->
            if CS#chain_r.role == standalone
               orelse
               (CS#chain_r.role == chain_member andalso
                CS#chain_r.official_tail == true) ->
                    true;
               true ->
                    false
            end;
       ReadOrWrite == write ->
            if CS#chain_r.role == standalone
               orelse
               (CS#chain_r.role == chain_member andalso
                CS#chain_r.downstream /= undefined andalso
                CS#chain_r.upstream == undefined) ->
                    true;
               true ->
                    false
            end
    end.

role_is_headlike_p(S) ->
    op_type_permitted_by_role_p(write, S).

%% @spec (state_r()) -> {atom(), term()}
%% @doc Extract the brick's implementation-dependent module name and state.

impl_details(S) when is_record(S, state) ->
    {S#state.impl_mod, S#state.impl_state};
impl_details(S) ->
    exit({bummer_99932, impl_details, bad_record, S}).

%% @spec (term(), term(), state_r()) -> state_r()
%% @doc Convenience function to set brick-specific metadata.

set_metadata_helper(Key, Val, S) ->
    {ImplMod, ImplState} = impl_details(S),
    ImplState2 = ImplMod:bcb_set_metadata(Key, Val, ImplState),
    S#state{impl_state = ImplState2}.

%% @spec (term(), state_r()) -> state_r()
%% @doc Convenience function to delete brick-specific metadata.

delete_metadata_helper(Key, S) ->
    {ImplMod, ImplState} = impl_details(S),
    ImplState2 = ImplMod:bcb_delete_metadata(Key, ImplState),
    S#state{impl_state = ImplState2}.

%% @spec (sweepcheckp_r(), state_r()) -> state_r()
%% @doc Write a complete sweep metadata checkpoint record.

write_sweep_cp(SwC, S)
  when is_record(SwC, sweepcheckp_r), is_record(S, state) ->
    ?DBG_MIGRATEx({write_sweep_cp, S#state.name, SwC}),
    set_metadata_helper(sweep_checkpoint, SwC, S).

delete_sweep_cp(S) ->
    delete_metadata_helper(sweep_checkpoint, S).

make_new_global_hash(OldGH, NewGH, _S) when not is_record(OldGH, g_hash_r) ->
    NewGH;
make_new_global_hash(OldGH, NewGH, S) ->
    if OldGH#g_hash_r.migrating_p == true,
       NewGH#g_hash_r.migrating_p == true,
       OldGH#g_hash_r.current_rev == NewGH#g_hash_r.current_rev ->
            ?DBG_MIGRATEx({make_new_global_hash,S#state.name,migrating,OldGH#g_hash_r.phase}),
            NewGH#g_hash_r{phase = OldGH#g_hash_r.phase,
                           migr_dict = OldGH#g_hash_r.migr_dict};
       true ->
            ?DBG_MIGRATEx({make_new_global_hash,S#state.name,not_both_migrating}),
            NewGH
    end.

%% DoList preprocessing functions.

%% @spec (do_list(), proplist(), state_r()) ->
%%       {ok, do_list(), proplist(), state_r()} | {reply, term(), state_r()}
%%
%% @doc Perform the DoList preprocessing required for storage quota
%% features and enforcement.

quotas_preprocess(DoList, DoFlags, S) when is_record(S, state) ->
    DoList2 = quotas_pre_op_flags(DoList, S),
    case quotas_pre_enforce_quota(DoList2, S) of
        DoList3 when is_list(DoList3) ->
            {ok, DoList3, DoFlags, S};
        {quota_error, _} = Error ->
            {reply, Error, S}
    end.

quotas_pre_op_flags([H|T], S) ->
    %% TODO: worthwhile to rewrite using lists:map?
    Flags0 = get_op_flags(H),
    Flags1 = lists:map(
               fun({<<"quota_items">>, N}) -> {quota_items, gmt_util:int_ify(N)};
                  ({<<"quota_bytes">>, N}) -> {quota_bytes, gmt_util:int_ify(N)};
                  %% These two should be rare or never used, but include anyway
                  ({<<"quota_items_used">>, N}) -> {quota_items_used, gmt_util:int_ify(N)};
                  ({<<"quota_bytes_used">>, N}) -> {quota_bytes_used, gmt_util:int_ify(N)};
                  (X)                      -> X
             end, Flags0),
    if Flags0 == Flags1 ->
            [H|quotas_pre_op_flags(T, S)];
       true ->
            [set_op_flags(H, Flags1)|quotas_pre_op_flags(T, S)]
    end;
quotas_pre_op_flags([], _S) ->
    [].

quotas_pre_enforce_quota(DoList, S) ->
    case quotas_pre_enforce_quota2(DoList, S) of
        L when is_list(L) ->
            DoList2 = lists:reverse(L),
            if DoList /= DoList2 ->
                    case DoList2 of
                        [txn|_] ->
                            DoList2;
                        _ ->
                            [txn|DoList2] % Not a txn, so make it one.
                    end;
               true ->
                    DoList
            end;
        {quota_error, _} = Error ->
            Error
    end.

quotas_pre_enforce_quota2(DoList, S) ->
    {ImplMod, ImplState} = impl_details(S),
    Q_names = [quota_items_used, quota_bytes_used],
    %%
    %% Maintain a orddict of "dirty" quota roots.  Otherwise, we won't be
    %% able to handle properly a do list that has two ops under the same
    %% quota root.  {sigh}
    %%
    case
        lists:foldl(
          fun(DoOp, {OpsAcc, QDict, KDict}) when is_tuple(DoOp), size(DoOp) > 2 ->
                  OpName = element(1, DoOp),
                  Key = element(2, DoOp),
                  Check = check_key_for_quota_root(Key, S),
                  if Check /= not_found andalso
                     (OpName == add orelse OpName == replace orelse
                      OpName == set orelse OpName == delete) ->
                          {ok, QPrefix, QuotaTuple0} = Check,
                          QuotaTuple =
                              case orddict:find(QPrefix, QDict) of
                                  error ->
                                      QuotaTuple0;
                                  {ok, QT} ->
                                      QT
                              end,
                          {_QKey, QTS, _QVal, _, QFlags} = QuotaTuple,
                          {Q_iu, Q_bu, OtherQFlags} =
                              case proplists:split(QFlags, Q_names) of
                                  {[[{quota_items_used, X1}],
                                    [{quota_bytes_used, X2}]], RemFls} ->
                                      {X1, X2, RemFls};
                                  {_, RemFls} ->
                                      %% Oh oh, someone didn't have both
                                      %% quota used flags like we expected.
                                      %% TODO: error msg?
                                      {0, 0, RemFls}
                              end,
                          quota_more_ops(DoOp, OpName, Key, QPrefix, QTS,
                                         Q_iu, Q_bu, OtherQFlags,
                                         ImplMod, ImplState, OpsAcc,
                                         QDict, KDict);
                     true ->
                          {[DoOp|OpsAcc], QDict, KDict}
                  end;
             (DoOp, {OpsAcc, QDict, KDict}) ->
                  {[DoOp|OpsAcc], QDict, KDict};
             (_DoOp, {quota_error, _} = Acc) ->
                  Acc
          end, {[], orddict:new(), orddict:new()}, DoList) of
        {L2, _QRootDict, _KeyDict} ->
            L2;
        {quota_error, _Stuff} = Res ->
            Res
    end.

quota_more_ops(DoOp, OpName, Key, QPrefix, QTS,
               Q_iu, Q_bu, OtherQFlags, ImplMod, ImplState, OpsAcc,
               QDict, KDict) ->
    %%
    %% Our job is a little trickier in that one or more ops in this
    %% transaction may involve a dirty key.  We don't know that yet,
    %% and so processing this txn may be delayed by the underlying
    %% ImplMod.  Therefore we cannot assume that a quota record will
    %% stay constant, or that the item we're updating remains
    %% constant.  Therefore, we make liberal use of 'must_not_exist'
    %% and 'testset' to make certain that all parties don't change in
    %% the middle of things.
    %%
    %% NOTE: In some cases, we must manufacture the mandatory 'val_len'
    %%       property that the ImplMod will always provide for us.
    %%
    Q_i = proplists:get_value(quota_items, OtherQFlags, 0),
    Q_b = proplists:get_value(quota_bytes, OtherQFlags, 0),
    SilentOp = make_next_op_is_silent(),
    QVal = ?VALUE_REMAINS_CONSTANT,
    KeyRecList = case orddict:find(Key, KDict) of
                     error ->
                         ImplMod:bcb_lookup_key(Key, false, ImplState);
                     {ok, {delete, _}} ->
                         [];
                     {ok, T} ->
                         [T]
                 end,
    case KeyRecList of
        [] ->
            NotExistOp = make_get(Key, [must_not_exist]),
            if OpName == add ; OpName == replace ; OpName == set ->
                    %% Adding a key that doesn't exist, so calculating
                    %% new quota usage is easy.
                    NewVal = element(4, DoOp),
                    NewValLen = gmt_util:io_list_len(NewVal),
                    NewQ_iu = Q_iu + 1,
                    NewQ_bu = Q_bu + NewValLen,
                    if (Q_i > 0 andalso NewQ_iu + 0 > Q_i) %qqq QC test spot: 0
                       orelse
                       (Q_b > 0 andalso NewQ_bu > Q_b) ->
                            {quota_error, QPrefix};
                       true ->
                            QFlags = [{quota_items_used, NewQ_iu},
                                      {quota_bytes_used, NewQ_bu},
                                      {testset, QTS}|OtherQFlags],
                            QRootOp2 = make_replace(QPrefix, QVal, 0, QFlags),
                            QTuple2 = {QPrefix, QTS, QVal, 0, QFlags},
                            {_Op, Key, KTS, KVal, KExp, KFlags0} = DoOp,
                            KFlags = [{val_len,
                                       gmt_util:io_list_len(KVal)}|KFlags0],
                            KTuple2 = {Key, KTS, KVal, KExp, KFlags},
                            %% Tricky, this will be reversed eventually...
                            {[DoOp, NotExistOp, SilentOp,
                              QRootOp2, SilentOp|OpsAcc],
                             orddict:store(QPrefix, QTuple2, QDict),
                             orddict:store(Key, KTuple2, KDict)}
                    end;
               OpName == delete ->
                    %% Deleting a non-existent key.
                    %% Just verify that the key still doesn't exist.
                    {[DoOp, NotExistOp, SilentOp|OpsAcc],
                     QDict, KDict}
            end;
        [{_Key, KeyTS, _Val, _Exp, KeyFlags}] ->
            ExistsOp = make_get(Key, [{testset, KeyTS}]),
            KeyValLen = proplists:get_value(val_len, KeyFlags),
            {PlusItem, PlusBytes} =
                if OpName == delete ->
                        {0, 0};
                   true ->
                        NewVal = element(4, DoOp),
                        NewValLen = gmt_util:io_list_len(NewVal),
                        {1, NewValLen}
                end,
            NewQ_iu = floor0(Q_iu - 1 + PlusItem),
            NewQ_bu = floor0(Q_bu - KeyValLen + PlusBytes),
            if OpName /= delete
               andalso
               ((Q_i > 0 andalso NewQ_iu > Q_i)
                orelse
                (Q_b > 0 andalso NewQ_bu + 0 > Q_b)) -> % qqq QC test spot: 0
                    {quota_error, QPrefix};
               true ->
                    QFlags = [{quota_items_used, NewQ_iu},
                              {quota_bytes_used, NewQ_bu},
                              {testset, QTS}|OtherQFlags],
                    QRootOp2 = make_replace(QPrefix, QVal, 0, QFlags),
                    QTuple2 = {QPrefix, QTS, QVal, 0, QFlags},
                    KTuple2 =
                        if OpName == delete ->
                                {delete, Key};
                           true ->
                                {_Op, Key, KTS, KVal, KExp, KFlags0} = DoOp,
                                KFlags = [{val_len,
                                          gmt_util:io_list_len(KVal)}|KFlags0],
                                {Key, KTS, KVal, KExp, KFlags}
                        end,
                    {[DoOp, ExistsOp, SilentOp, QRootOp2, SilentOp|OpsAcc],
                     orddict:store(QPrefix, QTuple2, QDict),
                     orddict:store(Key, KTuple2, KDict)}
            end
    end.

%% @spec (do_list(), proplist(), state_r()) ->
%%       {ok, do_list(), proplist(), state_r()} | {reply, term(), state_r()}
%%
%% @doc Perform the DoList preprocessing required for "server-side funs",
%% funs that are executed on the brick server.
%%
%% NOTE: These funs should be careful not to create keys that aren't
%%       stored by the receiving brick server.  If such a mistake is
%%       made, it will be caught by the brick's later processing, in
%%       the "are all the keys stored by the local brick" stage.

ssf_preprocess(DoList, DoFlags, S) when is_record(S, state) ->
    case ssf_preprocess2(DoList, DoFlags, S) of
        DoList2 when is_list(DoList2) ->
            {ok, DoList2, DoFlags, S};
        Reply ->
            {reply, Reply, S}
    end.

ssf_preprocess2([DoOp|DoOps], DoFlags, S) when is_tuple(DoOp),
                                               is_record(S, state) ->
    case DoOp of
        {ssf, Key, [Fun]} when is_function(Fun, 4) ->
            case (catch Fun(Key, DoOp, DoFlags, S)) of
                {ok, NewDoOps} ->
                    NewDoOps ++ ssf_preprocess2(DoOps, DoFlags, S);
                Error ->
                    {error, lists:flatten(io_lib:format("~P", [Error, 20]))}
            end;
        _ ->
            [DoOp|ssf_preprocess2(DoOps, DoFlags, S)]
    end;
ssf_preprocess2([DoOp|DoOps], DoFlags, S) ->
    [DoOp|ssf_preprocess2(DoOps, DoFlags, S)];
ssf_preprocess2([], _DoFlags, _S) ->
    [].

%% @spec (term(), boolean(), state_r()) -> [] | [tuple()]
%% @doc See return value for ImplMod's bcb_lookup_key/3.

ssf_peek(Key, MustHaveVal_p, S) when is_record(S, state) ->
    {ImplMod, ImplState} = impl_details(S),
    ImplMod:bcb_lookup_key(Key, MustHaveVal_p, ImplState).

%% @spec (term(), integer(), proplist(), state_r()) -> {list(term()), boolean()}
%% @doc See return value for ImplMod's bcb_get_many/3.

ssf_peek_many(Key, MaxNum, Flags, S) when is_record(S, state) ->
    {ImplMod, ImplState} = impl_details(S),
    {{L, MoreP}, _S2} = ImplMod:bcb_get_many(Key, [{max_num, MaxNum}|Flags],
                                             ImplState),
    {lists:reverse(L), MoreP}.

%% @spec (term(), state_r()) -> key_not_exist | {ok, term(), tuple()}
%% @doc The ssf interface for the check_key_for_quota_root() function.

ssf_check_quota_root(Key, S) when is_record(S, state) ->
    case check_key_for_quota_root(Key, S) of
        not_found ->
            key_not_exist;
        Res ->
            Res
    end.

%% @spec (state_r()) -> key_not_exist | {term(), term()}
%% @doc Helper function to return the ImplMod and ImplState terms hidden
%% inside the server state.

ssf_impl_details(S) when is_record(S, state) ->
    impl_details(S).

get_op_flags({OpName, _, _, _, _, Flags})
  when OpName == add ; OpName == replace ; OpName == set ->
    Flags;
get_op_flags({OpName, _, Flags})
  when OpName == get ; OpName == delete ; OpName == get_many ->
    Flags;
get_op_flags(_) ->
    [].

set_op_flags({OpName, _, _, _, _, _} = Do, NewFlags)
  when OpName == add ; OpName == replace ; OpName == set ->
    setelement(6, Do, NewFlags);
set_op_flags({OpName, _, _} = Do, NewFlags)
  when OpName == get ; OpName == delete ; OpName == get_many ->
    setelement(3, Do, NewFlags);
set_op_flags(X, _NewFlags) ->
    X.

floor0(X) when X > 0 ->
    X;
floor0(_) ->
    0.

preprocess_fold(DoList, DoFlags, Funs, S) ->
    lists:foldl(
      fun(Fun, {ok, Dos, Fls, St}) ->
              Fun(Dos, Fls, St);
         (_Fun, {reply, _, _} = Reply) ->
              Reply
      end, {ok, DoList, DoFlags, S}, Funs).

incr_impl_logging_serial(S) ->
    {ImplMod, ImplState} = impl_details(S),
    {LoggingSerial, ImplState2} = ImplMod:bcb_incr_logging_serial(ImplState),
    {LoggingSerial, S#state{impl_state = ImplState2}}.

peek_impl_logging_serial(S) ->
    {ImplMod, ImplState} = impl_details(S),
    ImplMod:bcb_peek_logging_serial(ImplState).

sort_by_serial(L) ->
    L1 = lists:map(fun(X) when size(X) == 2 -> {element(2, X), X};
                      (X)                   -> {element(3, X), X}
                   end, L),
    L2 = lists:map(fun({_, X}) -> X end,
                   lists:sort(L1)),
    L2.

filter_mods_for_downstream(Tuple, _ImplMod, _ImplState) when is_tuple(Tuple) ->
    Tuple;
filter_mods_for_downstream(Thisdo_Mods,
                           ImplMod, ImplState) when is_list(Thisdo_Mods) ->
    ImplMod:bcb_filter_mods_for_downstream(Thisdo_Mods, ImplState).

impl_mod_uses_bigdata_dir(S) ->
    {ImplMod, ImplState} = impl_details(S),
    Ps = ImplMod:bcb_status(ImplState),
    %% Is = proplists:get_value(implementation, Ps),
    Os = proplists:get_value(options, Ps),
    case proplists:get_value(bigdata_dir, Os) of
        undefined -> false;
        _         -> true
    end.

get_debug_chain_props() ->
    try
        {ok, B} = file:read_file("debug_chain_props"),
        Ps = binary_to_term(B),
        ?APPLOG_INFO(?APPLOG_APPM_064,"debug_chain_props: adding ~p to chain props\n",
                     [Ps]),
        Ps
    catch _:_ ->
            []
    end.

start_chain_admin_periodic_timer(S) ->
    ?E_INFO("DEBUG: start_chain_admin_periodic_timer: ~p\n", [S#state.name]),
    {ok, T} = brick_itimer:send_interval(?ADMIN_PERIODIC, chain_admin_periodic),
    T.

prop_or_central_conf_i(ConfName, PropName, PropList, Default) ->
    gmt_util:int_ify(
      prop_or_central_conf(ConfName, PropName, PropList, Default)).

prop_or_central_conf(ConfName, PropName, PropList, Default) ->
    case proplists:get_value(PropName, PropList, not_in_list) of
        not_in_list ->
            case gmt_config_svr:get_config_value(ConfName, "") of
                {ok, ""} ->
                    Default;
                {ok, Res} ->
                    Res
            end;
        Prop ->
            Prop
    end.

get_tabprop_via_str(ServerName, PropStr) ->
    try
        Tab0 = regexp_replace(atom_to_list(ServerName), "_copy[0-9]+", ""),
        TabMaybe = regexp_replace(Tab0, "_ch[0-9]+_b[0-9]+", ""),
        Props = gmt_util:parse_erlang_single_expr(PropStr),
        proplists:get_value(list_to_atom(TabMaybe), Props)
    catch _:_ ->
            undefined
    end.

regexp_replace(Subject, RE, Replacement) ->
    re:replace(Subject, RE, Replacement, [{return, list}]).

make_expiry_regname(A) when is_atom(A) ->
    list_to_atom(atom_to_list(A) ++ "_expiry").

poll_checkpointing_bricks(BrickList) ->
    %% There is no big value in doing this in parallel: we'll still
    %% have to wait for the slowest brick.  The only thing parallelism
    %% would do is perhaps avoid waiting for a 2nd checkpoint that
    %% starts after the first one has finished.  {shrug}
    lists:foreach(
      fun({Br, Nd}) ->
              gmt_loop:do_while(
                fun(_) ->
                        {ok, Ps} = brick_server:status(Br, Nd),
                        Is = proplists:get_value(implementation, Ps),
                        Cp = proplists:get_value(checkpoint, Is),
                        if is_pid(Cp) ->
                                timer:sleep(100),
                                {true, acc_is_not_used};
                           true ->
                                {false, acc_is_not_used}
                        end
                end, acc_is_not_used)
      end, BrickList).

throttle_brick(ThrottleBrick, RepairingP) ->
    ETS = brick_server:throttle_tab_name(ThrottleBrick),
    case RepairingP of true  -> ets:insert(ETS, {repair_throttle_key, now()});
                       false -> ets:insert(ETS, {throttle_key, now()})
    end.

unthrottle_brick(ThrottleBrick) ->
    ETS = brick_server:throttle_tab_name(ThrottleBrick),
    ets:delete(ETS, throttle_key),
    ets:delete(ETS, repair_throttle_key).

set_repair_overload_key(Brick) ->
    ETS = brick_server:throttle_tab_name(Brick),
    ets:insert(ETS, {repair_overload_key, now()}).

delete_repair_overload_key(Brick) ->
    ETS = brick_server:throttle_tab_name(Brick),
    ets:delete(ETS, repair_overload_key).

set_sequence_file_is_bad_key(Brick, SeqNum) ->
    %% NOTE: We use the same convention of now() @ 2nd tuple element,
    %%       but we don't use timed_key_exists_p on bad sequence file keys.
    ETS = brick_server:throttle_tab_name(Brick),
    ets:insert(ETS, {{sequence_file_is_bad, SeqNum}, now()}).

%% This key used for throttling updates at the head of a chain.

lookup_throttle_key(ETS) ->
    timed_key_exists_p(ETS, throttle_key).

%% This key used for throttling repair events at the official tail of
%% a chain before sending them down to the repairing brick.

lookup_repair_throttle_key(ETS) ->
    timed_key_exists_p(ETS, repair_throttle_key).

%% This key is used to inform the repairing brick that it has been
%% overloaded and needs to change its repair status to
%% 'repair_overload'.  Unlike the other two types of throttling keys,
%% we don't automatically expire this one: the timestamp is there for
%% info purposes only.

lookup_repair_overload_key(ETS) ->
    case ets:lookup(ETS, repair_overload_key) of []  -> false;
                                                 [_] -> true
    end.

lookup_sequence_file_is_bad_key(ETS, SeqNum) ->
    case ets:lookup(ETS, {sequence_file_is_bad, SeqNum}) of []  -> false;
                                                            [_] -> true
    end.

timed_key_exists_p(ETS, Key) ->
    case ets:lookup(ETS, Key) of
        [] ->
            false;
        [{_, InsertTime}] ->
            case timer:now_diff(now(), InsertTime) of
                N when N > 2*1000*1000 ->
                    %% Race conditions are OK.  Re-insertion is done
                    %% frequently, if there is indeed a race.
                    ets:delete(ETS, Key),
                    false;
                _ ->
                    true
            end
    end.

%% @spec (string(), fun()) -> ok
%% @doc See replace_file_sync/3.

replace_file_sync(PermPath, WriteFun) ->
    replace_file_sync(PermPath, ".tmp", WriteFun).

%% @spec (string(), string(), fun()) -> ok
%% @doc Replace file PermPath with the contents generated by WriteFun,
%%      using file:sync() before renaming.
%%
%% The WriteFun's arg is a file handle.  WriteFun must return 'ok'.

replace_file_sync(PermPath, Suffix, WriteFun) ->
    TmpPath = PermPath ++ Suffix,
    {ok, FH} = file:open(TmpPath, [write]),
    ok = WriteFun(FH),
    ok = file:sync(FH),
    ok = file:close(FH),
    ok = file:rename(TmpPath, PermPath).

get_do_op_too_old_timeout(S) ->
    {ok, MSec} = gmt_config_svr:get_config_value_i(brick_do_op_too_old_timeout, ?DO_OP_TOO_OLD),
    S#state{do_op_too_old_usec = MSec * 1000}.

%% @doc Force all changes to #chain_r.my_repair_state to also tell the pingee.

set_my_repair_member(S, CS, RState) ->
    brick_pingee:set_repair_state(S#state.name, node(), RState),
    CS#chain_r{my_repair_state = RState}.

set_my_repair_member_and_chain_ok_time(S, CS, RState, ChainOkTime) ->
    CS2 = set_my_repair_member(S, CS, RState),
    brick_pingee:set_chain_ok_time(S#state.name, node(), ChainOkTime),
    CS2#chain_r{my_repair_ok_time = ChainOkTime}.

get_implementation_status(S) ->
    {ImplMod, ImplState} = impl_details(S),
    {reply, {ok, ImplStatus}, IS2}
        = ImplMod:handle_call({status}, from_unused, ImplState),
    ImplState = IS2,            % sanity, so we know we can ignore IS2
    ImplStatus.

get_implementation_ets_info(S) ->
    ImplStatus = get_implementation_status(S),
    Es = proplists:get_value(ets, ImplStatus),
    {proplists:get_value(size, Es), proplists:get_value(memory, Es)}.

do_common_log_sequence_file_is_bad(SeqNum, S) ->
    {ImplMod, ImplState} = impl_details(S),
    {NumPurged, ImplState2} =
        ImplMod:bcb_common_log_sequence_file_is_bad(SeqNum, ImplState),
    set_sequence_file_is_bad_key(S#state.name, SeqNum),
    ?APPLOG_INFO(?APPLOG_APPM_063,
                 "~s:common_log_sequence_file_is_bad: ~p: ~w purged keys for log ~p\n",
                 [?MODULE, S#state.name, NumPurged, SeqNum]),
    if NumPurged == 0 ->
            S#state{impl_state = ImplState2};
       true ->
            %% If we're repairing someone, we should stop now.  Otherwise
            %% we could tell the downstream to delete stuff that it doesn't
            %% necessarily have to delete.
            {ok, NewS} = chain_set_ds_repair_state(
                           S#state{impl_state = ImplState2}, pre_init),
            {ok, NewS2} = chain_set_my_repair_state(NewS, force_disk_error),
            NewS2
    end.

bz_debug_check_todos(_ToDos, #state{bz_debug_check_hunk_summ = false}) ->
    ok;
bz_debug_check_todos([], _S) ->
    ok;
bz_debug_check_todos(ToDos, #state{bz_debug_check_hunk_summ = true,
                                   bz_debug_check_hunk_blob = CheckBlob} = S)
  when is_list(ToDos) ->
    Is = [element(3, X) || X <- ToDos,
                           element(1, X) == insert andalso size(X) == 3],
    [try
         put(bz_debug_justincase, I),
         FH = bz_debug_get_fh(SeqNum),
         if CheckBlob == false ->
                 HS = gmt_hlog:read_hunk_summary(FH, SeqNum, Offset, 100,
                                                 fun(X, _) -> X end),
                 SeqNum = HS#hunk_summ.seq,
                 Offset = HS#hunk_summ.off;
            true ->
                 true = gmt_hlog:read_hunk_summary(FH, SeqNum, Offset,
                                                   BlobSize, fun bz_debug_chk/2)
         end
     catch E1:E2 ->
             ImplStatus = get_implementation_status(S),
             case proplists:get_value(do_sync, ImplStatus)
                  andalso proplists:get_value(do_logging, ImplStatus) of
                 true ->
                     exit({bz_check, E1, E2, get(bz_debug_justincase),
                           erlang:get_stacktrace()});
                 _ ->
                     %% Either do_sync or do_logging = false can cause
                     %% race problems.
                     ok
             end
     end || {_Key, _TS, {SeqNum, Offset}, BlobSize} = I <- Is,
            SeqNum /= 0],
    ok;
bz_debug_check_todos(_, _S) ->
    ok.                                         % e.g. {plog_sweep, ...} tuple

bz_debug_get_fh(SeqNum) ->
    case get(bz_debug_get_fh) of
        {SeqNum, FH} ->
            FH;
        {_Other, OtherFH} ->
            file:close(OtherFH),
            {ok, FH} = gmt_hlog:open_log_file("hlog.commonLogServer", SeqNum,
                                              [read,raw,binary]),
            put(bz_debug_get_fh, {SeqNum, FH}),
            FH
    end.

%% Sortof cut-and-paste from gmt_hlog:read_bigblob_hunkblob()

bz_debug_chk(Su, FH) ->
    Blob = gmt_hlog:read_hunk_member_ll(FH, Su, md5, 1),
    Su2 = Su#hunk_summ{c_blobs = [Blob]},
    true = gmt_hlog:md5_checksum_ok_p(Su2).


foo_timeout() ->
    {ok, MilliSec} = gmt_config_svr:get_config_value_i(brick_server_default_timeout, ?FOO_TIMEOUT),
    MilliSec.
    

%%%
%%% Misc edoc stuff
%%%

%% @type brick() = {brick_name(), node_name()}.  A brick name in {Name, Node} form.
%% @type brick_name() = atom().  Name portion of a brick's {Name, Node}.
%% @type chain_r() = tuple().  A #chain_r record.
%% @type do_list() = list(term()).  A list of brick "do" operations.
%% @type do_op() = term().  A "do" operation, aka a do op.
%% @type global_hash_r() = term().  A global hash record, #g_hash_r.
%%       See brick_hash.hrl for definition.
%% @type node_name() = atom().  Node portion of a brick's {Name, Node}.
%% @type prop_list() = list(atom() | {atom(), term()}).
%%       See proplists reference for more details.
%% @type repair_state_name() = pre_init | {last_key, term()} | ok.
%%       Enumeration of valid brick repair state names.
%% @type rw_key_tuple() = {read | write, term()}. Intermediate term used by key-to-brick mapping functions.
%% @type serial_number() = no_serial | integer().  A serial number,
%%       where the atom no_serial indicates that no serial number accompanied
%%       the request.
%% @type server_ref() = atom() | {atom(), atom()} | {global, atom()} | pid().
%%       See gen_server reference for more details.
%% @type state_r() = term(). A local gen_server #state record.
%% @type thisdo_mods() = list(term()). A list of brick data modifications
%%       that result from processing a do list.
%% @type todo() = tuple().  A "to do" item is an action requested by an
%%       implementation-dependent brick layer that must be executed by the
%%       implementation-independent layer.
%%       See {@link do_chain_todos/2} for details.
%% @type todo_list() = list(todo()). A "to do" list.
%% @type timeout() = integer() | infinity.
%% @type zzz_add_reply() = term().  TODO.
%% @type zzz_checkpoint_reply() = term().  TODO.
%% @type zzz_delete_reply() = term().  TODO.
%% @type zzz_do_reply() = term().  TODO.
%% @type zzz_flushall_reply() = term().  TODO.
%% @type zzz_get_reply() = term().  TODO.
%% @type zzz_getdsrepair_reply() = term().  TODO.
%% @type zzz_getmany_reply() = term().  TODO.
%% @type zzz_getmyrepair_reply() = term().  TODO.
%% @type zzz_getrole_reply() = term().  TODO.
%% @type zzz_rolehead_reply() = term().  TODO.
%% @type zzz_rolemiddle_reply() = term().  TODO.
%% @type zzz_rolestandalone_reply() = term().  TODO.
%% @type zzz_roletail_reply() = term().  TODO.
%% @type zzz_roleundefined_reply() = term().  TODO.
%% @type zzz_setdsrepair_reply() = term().  TODO.
%% @type zzz_setmyrepair_reply() = term().  TODO.
%% @type zzz_state_reply() = term().  TODO.
%% @type zzz_status_reply() = term().  TODO.
%% @type zzz_syncdown_reply() = term().  TODO.
