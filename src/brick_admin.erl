%%%-------------------------------------------------------------------
%%% Copyright: (c) 2008-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : brick_admin.erl
%%% Purpose : Brick cluster administration server
%%%-------------------------------------------------------------------

%% @doc TODO list (not necessarily for this module)
%%
%% XX When a brick is repairing, double-check its role.  It must *not*
%% be in a state where it will answer any query.  No one should be sending
%% queries to a repairing brick, but there's no reason not to have extra
%% server-side paranoia.
%% Answer: When a brick is repairing, official_tail = false and
%% downstream = undefined, so chain_send_downstream() will do nothing.
%%
%% XX Need to add a 'cool_yer_jets' quiet period during a chain reconfig.
%% Otherwise, it's possible for a query to the wrong brick (according to
%% the pre-reconfig config) to be forwarded to a really-wrong brick.
%% NOTE: I think there's a race condition such that the quiet period must
%%       apply to both writes *and* reads?
%%
%% XX Add proper repair procedure for failure of a middle brick.
%%
%% XX Add interposition layer to allow for different brick implementations.
%%
%% XX Add migration
%%
%% XX Add by-witness method option to repair process.
%%
%% ?? Address the todo list on the brick_server: async logging, no logging,
%% changing such modes on-the-fly, etc.
%% See the "not implemented/TODO list" in brick_server.erl
%%
%% XX Must xfer the new global hash prior to or together with the end of
%% a cool_yer_jets period?  If the GH is assigned to brick after a role
%% change, they might forward queries to the wrong brick?  (If a chain
%% is radically changed, e.g. B -> D -> C -> A ==> A -> B -> C -> D ??)
%%
%% __ Add robust support for active/standby support of this server!
%% This could be as easy (where are the loopholes??) as checking for:
%% 1. network partition via the network partition detector, and
%% (and/or??) 2. Is a less-than-majority of the bootstrap bricks also
%% available?  If no to one/both(??) questions, then it's not safe to
%% run the admin server?  (Especially if two of the bootstrap replicas
%% are collocated on the nodes that are eligible to run the admin
%% server?  Then the remaining bootstrap bricks help form a
%% tiebreaker?)
%%
%% __ Add Lamport clock for all admin changes: brick status changes, chain
%% status changes, etc.
%%
%% __ Add Lamport clock support for all ops, read and write.
%% Enable on a per-table basis, perhaps?
%%

-module(brick_admin).
-include("applog.hrl").


-behaviour(gen_server).

-include("brick.hrl").
-include("brick_public.hrl").
-include("brick_hash.hrl").
-include("brick_admin.hrl").
-include("gmt_hlog.hrl").
-include("partition_detector.hrl").

-ifdef(debug_admin).
-define(gmt_debug, true).
-endif.
-include("gmt_debug.hrl").
-include("brick_specs.hrl").

-define(S3_TABLE, 'tab1').

%% API
-export([start/1, stop/0, start_remote/1, % original API
         start/2, stop/1, % OTP application API
         create_new_schema/2, create_new_schema/3,
         copy_new_schema/2, copy_new_schema/3, do_copy_new_schema3/2,
         start_bootstrap_scan/0, get_schema_bricks/0, get_schema_bricks/1,
         get_schema/0, get_schema/1,
         get_all_bootstrap_keys/1]).

%% API (once we're initially configured and started).
-export([get_tables/0, get_tables/1, get_table_info/1, get_table_info/2,
         get_table_info/3,
         get_table_info_by_chain/1, get_table_info_by_chain/2,
         get_table_chain_list/1, get_table_chain_list/2,
         add_table/2, add_table/3, add_table/4, load_bootstrap_data/2,
         get_gh_by_chainname/1, get_gh_by_chainname/2,
         get_gh_by_table/1, get_gh_by_table/2]).

%% General/misc helper API
-export([make_chain_description/3, make_chain_description/5,
         hack_all_tab_setup/5, hack_all_tab_setup/6,
         spam_gh_to_all_nodes/0, spam_gh_to_all_nodes/1,
         spam_gh_to_all_nodes/2, set_gh_minor_rev/0]).

%% Cluster reconfiguration API
-export([change_chain_length/2, change_chain_length/3,
         start_migration/2, start_migration/3,
         rsync_brick_to_brick/2]).

%% Schema bootstrap API (e.g. for mod_admin)
-export([bootstrap/8, bootstrap/9, bootstrap_local/7, bootstrap_local/8,
         make_common_table_opts/5, add_bootstrap_copy/1]).

%% Internal process signalling API, not for wide (ab)use
-export([start_er_up/1, table_finished_migration/1]).

%% Client monitoring API
-export([add_client_monitor/1, delete_client_monitor/1,
         get_client_monitor_list/0, run_client_monitor_procs/0]).

-export([fast_sync/3, fast_sync/4,
         fast_sync_clone_node/2, fast_sync_clone_node/3,
         %% Internal use only
         fast_sync_scav/6]).

%% Scoreboard API
-export([chain_status_change/3, chain_status_change/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Schema transformation
-export([upgrade_20090320/1, fix_gh_20090320/1]).

%% Debugging
-export([state/0]).

%% Internal use only
-export([bootstrap_scan_loop/0, bootstrap_scan_loop2/0]).

%% Useful funcs, TODO relocate or what?
-export([start_brick_only/1, start_brick_only/2, start_brick_only/3,
         stop_brick_only/1, stop_brick_only/2]).

-record(state, {
          schema = undefined,                               % dict:new()
          mig_mons = undefined,                             % migration monitor 2-tuples
          start_time = undefined,                           % now()
          extra_term = undefined,                           % term()
          mbox_highwater = undefined
         }).

%%====================================================================
%% API
%%====================================================================

-export_type([brick/0]).

-type deeplist() :: [char() | atom() | deeplist()].
-type brick() :: {atom(),atom()}.
-type bricklist() :: [brick()].
-type chainlist() ::  [{atom(),bricklist()}].
-type proplist() :: [ atom() | {any(),any()} ].
-type error() :: {error,any()}.
-type add_table_ret() :: ok | no_exists | error().
-type create_new_schema_ret() :: {ok,any()} | error().
-type serverref() :: file:name() | {file:name(),atom()} | {global,atom()} | pid().

-spec add_client_monitor(atom()) -> ok.
-spec add_table(atom(),chainlist()) -> add_table_ret().
-spec add_table(serverref(),chainlist()|atom(),deeplist()) -> add_table_ret().
-spec add_table(serverref(),atom(),chainlist(),deeplist()) -> add_table_ret().
-spec bootstrap(file:name(),proplist(),boolean(),char(),integer(),[atom()],integer(),[any()],proplist()) -> no_exists | ok.
-spec bootstrap(file:name(),proplist(),boolean(),char(),integer(),[atom()],integer(),[any()]) -> no_exists | ok.
-spec bootstrap_local(proplist(),boolean(),char(),integer(),non_neg_integer(),integer(),_) -> no_exists | ok.
-spec bootstrap_local(proplist(),boolean(),char(),integer(),non_neg_integer(),integer(),any(),proplist()) -> no_exists | ok.
-spec add_bootstrap_copy(bricklist()) -> ok | error().
-spec change_chain_length(atom(),bricklist()) -> ok | error().
-spec create_new_schema(bricklist(), file:name()) -> create_new_schema_ret().
-spec create_new_schema(bricklist(), proplist(), file:name()) -> create_new_schema_ret().
-spec delete_client_monitor(node()) ->  brick_server:set_reply().
-spec get_all_bootstrap_keys(bricklist()) -> [any()].
-spec get_all_bootstrap_keys({'ok',[any()],boolean()},bricklist(),[any()]) -> [any()].
-spec get_client_monitor_list() -> [any()].
-spec get_gh_by_chainname(atom()) -> {ok,#g_hash_r{}}.
-spec get_gh_by_table(atom()) -> {ok,#g_hash_r{}}.
-spec get_schema() -> term().
-spec get_schema_bricks() -> bricklist().
-spec get_table_chain_list(atom()) -> {ok,chainlist()}.
-spec get_table_chain_list(serverref(),atom()) -> {ok,chainlist()}.
-spec get_table_info(atom()) -> {ok,proplist()}.
-spec get_table_info(serverref(),atom()) -> {ok,proplist()}.
-spec get_table_info(serverref(),atom(),non_neg_integer()) -> {ok,proplist()}.
-spec get_table_info_by_chain(atom()) -> {ok,proplist()}.
-spec get_table_info_by_chain(atom(),atom()) -> {ok,proplist()}.
-spec get_tables() -> any().
-spec get_tables(serverref()) -> any().
-spec load_bootstrap_data(bricklist(),fun(({any(),any()})->boolean())) -> term().
-spec make_chain_description(atom(),non_neg_integer(),[atom()]) -> [{atom(),any()}].
-spec make_common_table_opts(proplist(),boolean(),char(),non_neg_integer(),chainlist()) -> proplist().
-spec spam_gh_to_all_nodes() -> ok.
-spec spam_gh_to_all_nodes(pid()) -> ok.
-spec start([file:name()]|file:name()) -> ok | error().
-spec start_brick_only({brick(),atom()}) -> ok.
-spec start_brick_only(brick(),atom()) -> ok.
-spec start_brick_only(brick(),atom(),proplist()) -> ok.
-spec stop_brick_only(brick(),atom()) -> ok.
-spec table_finished_migration(atom()) -> {table_finished_migration,any()}.


%% @doc Start the admin server underneath the brick_sup supervisor.
%% @spec start([path()]|path()) -> ok | {error, Reason}

start([BootstrapFile]) when is_atom(BootstrapFile) ->
    %% Assume usage via cmd line's -s
    start(atom_to_list(BootstrapFile));
start([BootstrapFile]) when is_list(BootstrapFile) ->
    %% Assume usage via cmd line's -run
    start(BootstrapFile);
start(BootstrapFile)
  when is_list(BootstrapFile), is_integer(hd(BootstrapFile)) ->
    %% Start the brick processes that should only run on the
    %% (globally unique!) admin host.

    case file:read_file(BootstrapFile) of
        {ok, _} ->

            %% Start the scoreboard.
            ChildSpec1 =
                {brick_sb, {brick_sb, start_link, []},
                 permanent, 2000, worker, [brick_sb]},
            supervisor:start_child(brick_admin_sup, ChildSpec1),

            %% Start ourself.
            ChildSpec2 =
                {brick_admin, {brick_admin, start_er_up, [BootstrapFile]},
                 permanent, 2000, worker, [brick_admin]},
            supervisor:start_child(brick_admin_sup, ChildSpec2),

            timer:sleep(5000),
            case whereis(brick_admin) of undefined -> error;
                _Pid      -> ok
            end;
        Err ->
            Err
    end.

%% @doc Start Admin Server on remote node, then halt local node.

-spec start_remote(list(atom())) -> no_return().
start_remote([Node, BootstrapFileAtom]) ->
    case rpc:call(Node, ?MODULE, start, [atom_to_list(BootstrapFileAtom)]) of
        ok -> erlang:halt(0);
        _  -> erlang:halt(1)
    end.

%% @doc For internal use only.

start_er_up(BootstrapFile) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [BootstrapFile],
                          [{timeout, 20*1000}]).

stop() ->
    ?APPLOG_INFO(?APPLOG_APPM_001,"Admin server shutdown requested\n", []),
    %% TODO: This needs to be much nicer: stop all workers and sub-supervisors
    %% nicely, because this simple mechanism was originally written in a
    %% different supervisor hierarchy.
    exit(whereis(brick_admin_sup), kill).

%% @doc Add a monitor process for a Hibari client node.
%%
%% For a description of client node monitors, see the "Hibari Sysadmin Guide",
%% "Adding/Removing Client Nodes" section.

add_client_monitor(Node) when is_atom(Node) ->
    do_add_client_monitor(Node).

%% @doc Remove a monitor process for a Hibari client node.
%%
%% For a description of client node monitors, see the "Hibari Sysadmin Guide",
%% "Adding/Removing Client Nodes" section.

delete_client_monitor(Node) when is_atom(Node) ->
    do_delete_client_monitor(Node).

%% @doc Get the list for a Hibari client nodes monitored by the ADmin Server.
%%
%% For a description of client node monitors, see the "Hibari Sysadmin Guide",
%% "Adding/Removing Client Nodes" section.

get_client_monitor_list() ->
    {_, _, List} = get_client_monitor_details(),
    List.

%%
%% start/stop API for OTP application behavior.
%%

start(normal, Args) ->
    ?APPLOG_INFO(?APPLOG_APPM_002,"~s: normal start, Args = ~p\n", [?MODULE, Args]),
    HackPid = spawn_link(fun() ->
                                 brick_admin:start("Schema.local"),
                                 receive goo -> ok end
                         end),
    {ok, HackPid};
start(StartMethod, Args) ->
    ?APPLOG_INFO(?APPLOG_APPM_003,"~s: ~p start, Args = ~p\n",
                 [?MODULE, StartMethod, Args]),
    start(normal, Args).

stop(Args) ->
    ?APPLOG_INFO(?APPLOG_APPM_004,"~s: stop, Args = ~p\n", [?MODULE, Args]),
    ok.

create_new_schema(InitialBrickList, File) ->
    create_new_schema(InitialBrickList, [], File).

create_new_schema(InitialBrickList, PropList, File) ->
    do_create_new_schema(InitialBrickList, PropList, File).

%% @doc Copy "Schema.local" file to all bricks in InitialBrickList.

copy_new_schema(InitialBrickList, File) ->
    copy_new_schema(InitialBrickList, [], File).

%% @doc Copy "Schema.local" file to all bricks in InitialBrickList.

copy_new_schema(InitialBrickList, PropList, File) ->
    do_copy_new_schema(InitialBrickList, PropList, File).

state() ->
    gen_server:call(?MODULE, {state}, infinity).

%% @doc Start the bootstrap brick scan &amp; repair loop.

start_bootstrap_scan() ->
    Pid = spawn_link(?MODULE, bootstrap_scan_loop, []),
    {ok, Pid}.

%% @doc Get the list of schema bricks (i.e. the "bootstrap_copy*" bricks).

get_schema_bricks() ->
    get_schema_bricks(?MODULE).

%% @doc Get the list of schema bricks (i.e. the "bootstrap_copy*" bricks).

get_schema_bricks(ServerRef) ->
    gen_server:call(ServerRef, {get_schema_bricks}, 90*1000).

%% @doc Get the Admin Server's #schema record.

get_schema() ->
    get_schema(?MODULE).

%% @doc Get the Admin Server's #schema record.

get_schema(ServerRef) ->
    gen_server:call(ServerRef, {get_schema}, 90*1000).

%% @doc Get the Admin Server's list of all tables.

get_tables() ->
    get_tables(?MODULE).

%% @doc Get the Admin Server's list of all tables.

get_tables(ServerRef) ->
    gen_server:call(ServerRef, {get_tables}).

%% @doc Get the Admin Server's table info for the specified table.

get_table_info(TableName) ->
    get_table_info(?MODULE, TableName).

%% @doc Get the Admin Server's table info for the specified table.

get_table_info(ServerRef, TableName) ->
    gen_server:call(ServerRef, {get_table_info, TableName}).

%% @doc Get the Admin Server's table info for the specified table.

get_table_info(ServerRef, TableName, Timeout) ->
    gen_server:call(ServerRef, {get_table_info, TableName}, Timeout).

%% @doc Get the Admin Server's table info for the specified table that
%% owns the specified chain.

get_table_info_by_chain(ChainName) ->
    get_table_info_by_chain(?MODULE, ChainName).

%% @doc Get the Admin Server's table info for the specified table that
%% owns the specified chain.

get_table_info_by_chain(ServerRef, ChainName) ->
    gen_server:call(ServerRef, {get_table_info_by_chain, ChainName}).

%% @doc Get the Admin Server's list of chains for the specified table.
%% @spec (atom()) -> {ok, chain_list()} | no_exists
%% @equiv get_table_chain_list(MODULE, TableName)

get_table_chain_list(TableName) ->
    get_table_chain_list(?MODULE, TableName).

%% @doc Get the Admin Server's list of chains for the specified table.
%% @spec (server_ref(), atom()) -> {ok, chain_list()} | no_exists

get_table_chain_list(ServerRef, TableName) ->
    gen_server:call(ServerRef, {get_table_chain_list, TableName}).

%% @doc Add a new table.
%%
%% For description, see the "Hibary Developer's Guide",
%% "Add a New Table: brick_admin:add_table()" section.

add_table(Name, ChainList) ->
    add_table(?MODULE, Name, ChainList).

%% @doc Add a new table.
%%
%% For description, see the "Hibary Developer's Guide",
%% "Add a New Table: brick_admin:add_table()" section.

add_table(Name, ChainList, BrickOptions) when is_atom(Name), is_list(ChainList), is_list(BrickOptions)->
    add_table(?MODULE, Name, ChainList, BrickOptions);
add_table(ServerRef, Name, ChainList) when is_atom(Name), is_list(ChainList) ->
    add_table(ServerRef, Name, ChainList, []).

%% @doc Add a new table.
%%
%% For description, see the "Hibary Developer's Guide",
%% "Add a New Table: brick_admin:add_table()" section.

add_table(ServerRef, Name, ChainList, BrickOptions) ->
    gen_server:call(ServerRef, {add_table, Name, ChainList, BrickOptions},
                    300*1000).

%% @doc Load all keys &amp; values from bootstrap bricks.

load_bootstrap_data(Bricks, FilterFun) ->
    AllKeys =
        gmt_loop:do_while(
          fun(_) ->
                  case catch get_all_bootstrap_keys(Bricks) of
                      {'EXIT', _} ->
                          {true, unused};
                      L when is_list(L) ->
                          {false, L}
                  end
          end, unused),
    Keys = lists:filter(FilterFun, AllKeys),
    lists:foldl(
      fun(Key, Acc) ->
              case squorum_get(Bricks, Key) of
                  {ok, _TS, Val} -> [{Key, Val}|Acc];
                  %% A rare race might have deleted it.
                  _              -> Acc
              end
      end, [], Keys).

%% @doc Given a chain name, find its global hash.

get_gh_by_chainname(ChainName) ->
    get_gh_by_chainname(?MODULE, ChainName).

%% @doc Given a chain name, find its global hash.

get_gh_by_chainname(ServerRef, ChainName) ->
    gen_server:call(ServerRef, {get_gh_by_chainname, ChainName}).

%% @doc Given a table name, find its global hash.

get_gh_by_table(TableName) ->
    get_gh_by_table(?MODULE, TableName).

%% @doc Given a table name, find its global hash.

get_gh_by_table(ServerRef, TableName) ->
    gen_server:call(ServerRef, {get_gh_by_table, TableName}).

%% @doc Change the length/membership of a single chain.

change_chain_length(ChainName, BrickList) ->
    change_chain_length(?MODULE, ChainName, BrickList).

%% @doc Change the length/membership of a single chain.

change_chain_length(ServerRef, ChainName, BrickList) ->
    gen_server:call(ServerRef, {change_chain_length, ChainName, BrickList}, 45*1000).

%% @doc Start a data migration to add/remove/reweight chains.

start_migration(TableName, LH) when is_record(LH, hash_r) ->
    start_migration(TableName, LH, []).

%% @doc Start a data migration to add/remove/reweight chains.

start_migration(TableName, LH, Options) when is_record(LH, hash_r) ->
    gen_server:call(?MODULE, {start_migration, TableName, LH, Options},
                    45*1000).

%% @doc Send a chain status notification message to the Admin Server (wrapper).

chain_status_change(ChainName, Status, PropList) ->
    chain_status_change(?MODULE, ChainName, Status, PropList).

%% @doc Send a chain status notification message to the Admin Server (wrapper).

chain_status_change(ServerRef, ChainName, Status, PropList) ->
    chain_status_change(ServerRef, ChainName, Status, PropList, 0).

%% @doc Send a chain status notification message to the Admin Server,
%% retrying if overload conditions are detected.

chain_status_change(ServerRef, ChainName, Status, PropList, Iters) ->
    X = case (catch gen_server:call(
                      ServerRef,
                      {chain_status_change, ChainName, Status, PropList},
                      45*1000)) of
            {'EXIT', _} -> overload;
            overload    -> overload;
            Res         -> Res
        end,
    if X == overload ->
            ?E_INFO("~s: chain_status_change: retry for chain ~p status ~p\n",
                    [?MODULE, ChainName, Status]),
            timer:sleep((Iters * 2) * 1000 + random:uniform(1000)),
            chain_status_change(ServerRef, ChainName, Status, PropList,Iters+1);
       true ->
            X
    end.

%% @doc Deprecated function

rsync_brick_to_brick(SrcBrNd, DstBrNd) ->
    do_rsync_brick_to_brick(SrcBrNd, DstBrNd).

%% @doc Notify the Admin Server that a data migration has finished successfully.

table_finished_migration(TableName) ->
    ?MODULE ! {table_finished_migration, TableName}.

%% @doc NOTE: The caller is responsible for making certain that the
%% Nodes list does not contain adjacent machines.
%% Does not do well creating atoms with non-atom-safe chars in name.

make_chain_description(TableName, ChainLength, Nodes)
  when is_integer(ChainLength), ChainLength =< length(Nodes) ->
    make_chain_description2(TableName, ChainLength, Nodes, 1).

make_chain_description2(TableName, ChainLength, Nodes, StartChainNum) ->
    Ns = Nodes ++ lists:sublist(Nodes, ChainLength - 1),
    lists:map(
      fun(Start) ->
              Q = lists:zip(lists:seq(1, ChainLength),
                            lists:sublist(Ns, Start, ChainLength)),
              ChainName = list_to_atom(lists:flatten(io_lib:format("~w_ch~w", [TableName, Start + StartChainNum - 1]))),
              {ChainName, [{list_to_atom(
                              lists:flatten(
                                io_lib:format("~w_b~w",[ChainName,L]))),N} ||
                              {L, N} <- Q]}
      end, lists:seq(1, length(Nodes))).

%% @doc Like make_chain_description/3, but allows for grouping chains into
%%      "blocks" where chain striping is done only across blocks and not
%%      across all bricks in the Nodes list.

make_chain_description(TableName, ChainLength, Nodes,
                       NumNodesPerBlock, BlockMultFactor)
  when NumNodesPerBlock < 2; BlockMultFactor < 1 ->
    %% Don't use the block chain striping scheme.
    make_chain_description(TableName, ChainLength, Nodes);
make_chain_description(TableName, ChainLength, Nodes,
                       NumNodesPerBlock, BlockMultFactor)
  when is_integer(ChainLength), ChainLength =< length(Nodes) ->
    if length(Nodes) rem NumNodesPerBlock /= 0;
       NumNodesPerBlock < ChainLength ->
            badarg;
       true ->
            Hop = NumNodesPerBlock * BlockMultFactor,
            EndSeq = ((Hop * length(Nodes)) div NumNodesPerBlock) - 1,
            Z = lists:zip(gmt_util:list_chop(Nodes, NumNodesPerBlock),
                          lists:seq(1, EndSeq, Hop)),
            lists:append(
              [make_chain_description2(
                 TableName, ChainLength,
                 lists:append(lists:duplicate(BlockMultFactor, Ns)),
                 StartChNum) || {Ns, StartChNum} <- Z])
    end.

%% @doc This is a quick hack, just to put the proof-of-concept code somewhere.  DO NOT USE!

hack_all_tab_setup(TableName, ChainLength, Nodes, BrickOptions, DoCreateAdd) ->
    hack_all_tab_setup(TableName, ChainLength, Nodes, BrickOptions, DoCreateAdd,
                       fun brick_hash:naive_init/1).

%% @doc This is a quick hack, just to put the proof-of-concept code somewhere.

hack_all_tab_setup(TableName, ChainLength, Nodes, BrickOptions, DoCreateAdd,
                   LocalHashInitFun) ->
    ChDescr = make_chain_description(TableName, ChainLength, Nodes),
    if DoCreateAdd ->
            case ?MODULE:add_table(brick_admin, TableName, ChDescr, BrickOptions) of
                ok -> ok;
                no_exists -> ok;
                Err  -> throw(Err)
            end;
       true ->
            ?APPLOG_ALERT(?APPLOG_APPM_005,"hack_all_tab_setup: DoCreateAdd is false.  My caller better be a debug/test function only, because this function should not be used by anything the admin server does!\n",[]),
            timer:sleep(2*1000),
            ok
    end,
    LH = LocalHashInitFun(ChDescr),
    GH = brick_hash:init_global_hash_state(false, unused, 1,
                                           LH, ChDescr, LH, ChDescr),
    AllBricks = [B || {_, Bs} <- ChDescr, B <- Bs],
    %% Give some time for pingers to get their bricks running.
    timer:sleep(5000),
    [_ = brick_server:chain_hack_set_global_hash(Br, Nd, GH) ||
        {Br, Nd} <- AllBricks],
    timer:sleep(1000),
    %% Note: could use multicall here, but this whole func is a kludge.
    %% Consider using multicall elsewhere.
    [catch brick_simple:set_gh(Nd, TableName, GH) || Nd <- [node()|nodes()]],
    ok.

%% @doc "Spam" all connected nodes with the GH (global hash) for all tables.

spam_gh_to_all_nodes() ->
    spam_gh_to_all_nodes(global:whereis_name(?MODULE)).

%% @doc "Spam" all connected nodes with the GH (global hash) for all tables.

spam_gh_to_all_nodes(ServerRef) ->
    spam_gh_to_all_nodes(ServerRef, '$$all_tables$$').

%% @doc "Spam" all connected nodes with the GH (global hash) for the
%%      specified table.

spam_gh_to_all_nodes(ServerRef, TableName) ->
    ServerRef ! {do_spam_gh_to_all_nodes, TableName},
    ok.

%% @doc Update the global hash minor revision number for all tables
set_gh_minor_rev() ->
    set_gh_minor_rev(?MODULE).

set_gh_minor_rev(ServerRef) ->
    gen_server:call(ServerRef, {set_gh_minor_rev}, 90*1000).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([BootstrapFile]) ->
    %% we may decide to restart ourselves while initializing, so it's
    %% important to finish this pre-init quickly and let the supervisor
    %% assume its role over us. Since nobody knows us yet, its safe to
    %% assume we will get the finish_init_tasks message first. An empty
    %% state should crash us though if that is ever not true.
    self() ! {finish_init_tasks, BootstrapFile},
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({chain_status_change, ChainName, unknown, _PropList}, _From, State) ->
    ?APPLOG_INFO(?APPLOG_APPM_011,"~s: handle_cast: chain ~p in unknown state\n",
                 [?MODULE, ChainName]),
    {reply, ok, State};
handle_call({chain_status_change, ChainName, Status, PropList}, _From, State) ->
    case process_info(self(), message_queue_len) of
        {_, N} when N > State#state.mbox_highwater ->
            {reply, overload, State};
        _ ->
            NewState = do_chain_status_change(ChainName, Status, PropList,
                                              State),
            {reply, ok, NewState}
    end;
handle_call({get_schema_bricks}, _From, State) ->
    {reply, (State#state.schema)#schema_r.schema_bricklist, State};
handle_call({get_schema}, _From, State) ->
    {reply, State#state.schema, State};
handle_call({get_tables}, _From, State) ->
    Keys = dict:fold(fun(K, _V, Acc) -> [K|Acc] end, [],
                     (State#state.schema)#schema_r.tabdefs),
    {reply, Keys, State};
handle_call({get_table_info, TableName}, _From, State) ->
    Reply = do_get_table_info(TableName, State),
    {reply, Reply, State};
handle_call({get_table_info_by_chain, ChainName}, _From, State) ->
    Schema = State#state.schema,
    Reply = case dict:find(ChainName, Schema#schema_r.chain2tab) of
                {ok, TableName} ->
                    do_get_table_info(TableName, State);
                Err ->
                    Err
            end,
    {reply, Reply, State};
handle_call({get_table_chain_list, TableName}, _From, State) ->
    Reply = do_get_table_chain_list(TableName, State),
    {reply, Reply, State};
handle_call({add_table, Name, ChainList, BrickOptions}, _From, State) ->
    {Reply, NewState} = do_add_table(Name, ChainList, BrickOptions, State),
    {reply, Reply, NewState};
handle_call({state}, _From, State) ->
    {reply, State, State};
handle_call({get_gh_by_chainname, ChainName}, _From, State) ->
    Reply = do_get_gh_by_chainname(ChainName, State),
    {reply, Reply, State};
handle_call({get_gh_by_table, TableName}, _From, State) ->
    Schema = State#state.schema,
    Reply = case dict:find(TableName, Schema#schema_r.tabdefs) of
                {ok, T} ->
                    {ok, T#table_r.ghash};
                _ ->
                    {error, not_found}
            end,
    {reply, Reply, State};
handle_call({set_gh_minor_rev}, _From, State) ->
    {Reply, NewState} = do_set_gh_minor_rev(State),
    {reply, Reply, NewState};
handle_call({change_chain_length, ChainName, BrickList}, _From, State) ->
    case (catch do_change_chain_length(ChainName, BrickList, State)) of
        {Reply, NewState} when is_record(NewState, state) ->
            {reply, Reply, NewState};
        Err -> {reply, Err, State}
    end;
handle_call({start_migration, TableName, LH, Options}, _From, State) ->
    case (catch do_start_migration(TableName, LH, Options, State)) of
        {Reply, NewState} when is_record(NewState, state) ->
            {reply, Reply, NewState};
        Err -> {reply, Err, State}
    end;
handle_call({add_bootstrap_copy, [{B,N}|_T]=BrickList}, _From, State) when is_atom(B), is_atom(N) ->
    {Reply, NewState} = do_add_bootstrap_copy(BrickList, State),
    {reply, Reply, NewState};
handle_call(_Request, _From, State) ->
    ?APPLOG_ALERT(?APPLOG_APPM_010,"~s: handle_call: ~p\n", [?MODULE, _Request]),
    Reply = err_bad_call,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    ?APPLOG_ALERT(?APPLOG_APPM_012,"~s: handle_cast: ~P\n", [?MODULE, _Msg, 20]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({finish_init_tasks, BootstrapFile}, State) ->
    %% Kludge: We will ping all of the bootstrap bricks, to try to
    %% establish a wider net of nodes in the cluster.  Then we'll try
    %% to register a global name as our mutual exclusion tactic.
    %% Not good enough in the face of a network partition, but good enough
    %% as a quick kludge.
    %% NOTE: Do not use bootstrap_existing_schema() here, r-o ops only!
    DiskBrickList = get_bricklist_from_disk(BootstrapFile),
    [spawn(fun() -> net_adm:ping(Nd) end)|| {_, Nd} <- DiskBrickList],
    timer:sleep(1000),

    {ok, MboxHigh} = gmt_config_svr:get_config_value_i(
                       brick_admin_mbox_high_water, 100),
    %% Cross-app dependency: we need the partition_detector app running.
    %% If not, complain loudly.
    StartTime = now(),
    ExtraStarting = {brick_admin, {starting, StartTime, node(), self()}},
    ExtraRunning = {brick_admin, {running, StartTime, node(), self()}},
    case (catch partition_detector_server:is_active()) of
        true ->
            partition_detector_server:add_to_beacon_extra(ExtraStarting),
            case check_for_other_admin_server_beacons(first_time, StartTime) of
                0 ->
                    ok;
                _N ->
                    partition_detector_server:del_from_beacon_extra(
                      ExtraStarting),
                    exit(another_admin_server_running)
            end,
            partition_detector_server:replace_beacon_extra(ExtraStarting,
                                                           ExtraRunning),
            gen_event:add_sup_handler(?EVENT_SERVER, brick_admin_event_h, []),
            0 = check_for_other_admin_server_beacons(3, StartTime),
            ?APPLOG_INFO(?APPLOG_APPM_006,"Finished checking for Admin Server "
                         "beacons\n", []);
        _ ->
            gmt_util:set_alarm({app_disabled, partition_detector},
                               "This application must run in production environments.",
                               fun() -> ok end),
            ?APPLOG_WARNING(?APPLOG_APPM_007,"ERROR: partition_detector application "
                            "is not running!  This should not happen "
                            "in a production environment.",[])
    end,

    case global:whereis_name(?MODULE) of
        undefined ->
            ok;
        APid ->
            ?APPLOG_ALERT(?APPLOG_APPM_008,"~s: admin server already running on ~p\n",
                          [?MODULE, node(APid)]),
            exit({global_name_already_in_use, ?MODULE})
    end,
    yes = global:register_name(?MODULE, self()), % Honest racing here.

    %% Use bootstrap hint file to find our schema.  Read/write is not yet ok.
    {DiskBrickList2, Schema} = bootstrap_existing_schema(BootstrapFile),

    %% Update on-disk schema location hints, if needed.
    if DiskBrickList2 /= Schema#schema_r.schema_bricklist ->
            OldFile = BootstrapFile ++ integer_to_list(gmt_time:time_t()),
            file:rename(BootstrapFile, OldFile),
            ok = write_schema_bootstrap_file(BootstrapFile, Schema#schema_r.schema_bricklist),
            %% After repairing, we should restart to ensure proper quorum available
            exit({restarting, repaired_hint_file, BootstrapFile});
       true ->
            ok
    end,

    %% read/write ops are now ok

    %% Start thread that will eventually repair out-of-date bootstrap replicas.
    %% Do this asynchronously to avoid supervisor deadlock.
    spawn(fun() ->
                  ChildSpec =
                      {brick_bootstrap_scan,
                       {?MODULE, start_bootstrap_scan, []},
                       permanent, 100, worker, [?MODULE]},
                  supervisor:start_child(brick_mon_sup, ChildSpec)
          end),

    %% Start chain monitors and brick pingers
    %% Do this asynchronously to avoid supervisor deadlock.
    spawn_link(fun() -> start_pingers_and_chmons(Schema) end),
    %% Start client node monitors
    spawn(fun() -> run_client_monitor_procs() end),

    %% Start the migration monitors.
    MMs = lists:foldl(fun({TableName, T}, Acc) ->
                              if not (T#table_r.ghash)#g_hash_r.migrating_p ->
                                      Acc;
                                 true ->
                                      Options = T#table_r.migration_options,
                                      {ok, Pid} = brick_migmon:start_link(
                                                    T, Options),
                                      [{TableName, Pid}|Acc]
                              end
                      end, [], dict:to_list(Schema#schema_r.tabdefs)),

    %% Add an event handler to the SASL alarm_handler to watch for
    %% alarm events from the partition detector.
    ok = brick_admin_event_h:add_handler(alarm_handler),

    %% HACK: Send a reminder to ourselves to set up the
    %% brick_simple stuff.
    timer:apply_after( 1*1000, ?MODULE, spam_gh_to_all_nodes, [self()]),
    timer:apply_after(10*1000, ?MODULE, spam_gh_to_all_nodes, [self()]),
    brick_itimer:send_interval(5*1000, restart_pingers_and_chmons),

    %% Start client monitor processes (must be done async/after our init())
    spawn(fun() -> timer:sleep(200), run_client_monitor_procs() end),

    ?APPLOG_INFO(?APPLOG_APPM_009,"~s: admin server now running on ~p\n",
                 [?MODULE, node()]),
    {noreply, State#state{schema = Schema, mig_mons = MMs,
                          start_time = StartTime, extra_term = ExtraRunning,
                          mbox_highwater = MboxHigh}};
handle_info({do_spam_gh_to_all_nodes, TableName} = Msg, State) ->
    gmt_loop:do_while(fun(_) -> receive Msg -> {true, x}
                                after 0     -> {false, x}
                                end
                      end, x),
    do_spam_gh_to_all_nodes(State, TableName),
    {noreply, State};
handle_info({table_finished_migration, TableName} = Msg, State) ->
    gmt_loop:do_while(fun(_) -> receive Msg -> {true, x}
                                after 0     -> {false, x}
                                end
                      end, x),
    {noreply, do_table_almost_finished_migration(TableName, State)};
handle_info({clear_migmon_pid, TableName}, #state{mig_mons = MMs} = State) ->
    ?E_INFO("Clearing final migration state for table ~p\n", [TableName]),
    {noreply, State#state{mig_mons = lists:keydelete(TableName, 1, MMs)}};
handle_info({Ref, _Reply}, State) when is_reference(Ref) ->
    %% Late gen_server reply, ignore it.
    {noreply, State};
handle_info(restart_pingers_and_chmons, State) ->
    async_start_pingers_and_chmons(State#state.schema),
    {noreply, State};
handle_info(_Info, State) ->
    ?APPLOG_ALERT(?APPLOG_APPM_013,"~s: handle_info: ~P\n", [?MODULE, _Info, 20]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% @doc Start all brick health pingers and chain monitor processes.

start_pingers_and_chmons(Schema) ->
    brick_bp:start_pingers(Schema#schema_r.schema_bricklist, []),
    %% ?DBG(schema_to_proplists(Schema)),
    lists:map(
      fun({_TableName, Props}) ->
              ?DBG(Props),
              BrickOptions = proplists:get_value(brick_options, Props, []),
              ?DBG(BrickOptions),
              GH = proplists:get_value(ghash, Props),
              %% AllChains will contain duplicate chains and perhaps
              %% duplicate bricks within each chain, be prepared.
              %%
              %% We're interested in all alive bricks,
              %% not all dead-or-alive bricks.
              AllChains =
                  (GH#g_hash_r.current_h_desc)#hash_r.healthy_chainlist ++
                  (GH#g_hash_r.new_h_desc)#hash_r.healthy_chainlist,
              AllBricks = [B || {_ChainName,Bricks} <- AllChains, B <- Bricks],
              %%io:format("ZZZ: AllChains = ~p\n", [AllChains]),
              %%io:format("ZZZ: AllBricks = ~p\n", [AllBricks]),%erlang:halt(),
              ?DBG(AllChains),
              [ok = brick_chainmon:start_mon(C) || C <- lists:usort(AllChains)],
              ?DBG(AllBricks),
              ok = brick_bp:start_pingers(lists:usort(AllBricks), BrickOptions)
      end, schema_to_proplists(Schema)).

%% @doc Asynchronously start all brick pinger and chain monitor processes.

async_start_pingers_and_chmons(Schema) ->
    spawn(fun() -> start_pingers_and_chmons(Schema) end).

%% @spec (path()) -> {list(brick_t()), schema()}
%% @doc Bootstrap an existing schema from standalone bricks specifically
%% designated to store bootstrap information.

bootstrap_existing_schema(File) ->
    BrickList = get_bricklist_from_disk(File),
    bootstrap_existing_schema2(BrickList).

bootstrap_existing_schema2(BrickList) ->
    %% Get schema-storing nodes running, as many as possible.
    %% We don't care if we encounter most errors.
    [catch start_standalone_brick(Brick) || Brick <- BrickList],

    {ok, _TS, Schema} = squorum_get(BrickList, ?BKEY_SCHEMA_DEFINITION),
    {BrickList, Schema}.

%% @spec (path()) -> list(brick_t())
%% @doc Read the list of brick from a file, for bootstrapping purposes.
%%
%% If the file File doesn't exist, try File ++ ".old".

get_bricklist_from_disk(File) ->
    case catch get_bricklist_from_disk2(File) of
        {'EXIT', _} -> get_bricklist_from_disk2(File ++ ".old");
        L           -> L
    end.

get_bricklist_from_disk2(File) ->
    {ok, [L]} = file:consult(File),
    false = lists:any(
              fun({B, N}) when is_atom(B), is_atom(N) -> false;
                 (_)                                  -> true
              end, L),
    L.

%% @doc Start a brick and put it into standalone mode &amp; ok repair state.

start_standalone_brick({Brick, Node}) ->
    %% TODO: Is there any reason (ease of diagnostics/cluster debugging?)
    %% why we'd want more error checking and/or logging and/or tracing
    %% in here?
    %% ?DBG(start_standalone_brick0),
    %%     net_adm:ping(Node),
    ?DBG(start_standalone_brick2),
    case rpc:call(Node, brick_shepherd, start_brick, [Brick, []], 1*1000) of
        {badrpc, nodedown} ->
                                                % if the service is down, can't really do anything below can it?
                                                % anything else let it crash.  bz 25933
            ?APPLOG_WARNING(?APPLOG_APPM_014,"~p is down ~n", [{Brick, Node}]);

        _ ->
            %% Don't wait too long here, or else we can block the
            %% starting of some other node.  The other option is to do
            %% this in parallel, bu that's the caller's decision to do
            %% or not do.
            PollRes = poll_brick_status(Brick, Node, 8),
            ?E_INFO("~s: brick ~p ~p started: ~P\n",
                    [?MODULE, Brick, Node, PollRes, 7]),
            ?DBG({start_standalone_brick, Brick, Node, _R3}),
            RoleRes = brick_server:chain_role_standalone(Brick, Node),
            ?E_INFO("~s: brick ~p ~p role set: ~p\n",
                    [?MODULE, Brick, Node, RoleRes]),
            ?DBG(start_standalone_brick4),
            RepairRes = brick_server:chain_set_my_repair_state(Brick, Node, ok),
            ?E_INFO("~s: brick ~p ~p state set: ~p\n",
                    [?MODULE, Brick, Node, RepairRes])
    end,
    ok.

%% @doc Start a remote brick via the remote node's brick_shepherd service.

start_brick_only({Brick, Node}) ->
    start_brick_only(Brick, Node).

%% @doc Start a remote brick via the remote node's brick_shepherd service.

start_brick_only(Brick, Node) ->
    start_brick_only(Brick, Node, []).

%% @doc Start a remote brick via the remote node's brick_shepherd service.

start_brick_only(Brick, Node, BrickOptions) ->
    %% TODO: Is there any reason (ease of diagnostics/cluster debugging?)
    %% why we'd want more error checking and/or logging and/or tracing
    %% in here?
    net_adm:ping(Node),
    %%No, let sup start! rpc:call(Node, brick_shepherd, start, []),
    rpc:call(Node, brick_shepherd, start_brick, [Brick, BrickOptions]),
    ok.

%% @doc Stop a brick via its local brick_shepherd service.

stop_brick_only({Brick, Node}) ->
    stop_brick_only(Brick, Node).

%% @doc Stop a brick via its local brick_shepherd service.

stop_brick_only(Brick, Node) ->
    catch rpc:call(Node, brick_shepherd, stop_brick, [Brick]).

%% @spec (list(brick_t()), bool(), path()) -> {list(brick_t()), list(brick_t())}

do_create_new_schema(InitialBrickList, PropList, File) ->
    {_, _, {error, enoent}} =
        {should_not_exist, File, file:read_file_info(File)},
    Res = do_create_new_schema2(InitialBrickList, PropList),
    case separate_good_from_bad(Res) of
        {[], _} = Res2 ->                       % None successful?
            {error, Res2};
        Res2 ->
            ok = write_schema_bootstrap_file(File, InitialBrickList),
            {ok, Res2}
    end.

do_create_new_schema2(InitialBrickList, _PropList) ->
    Schema = #schema_r{schema_bricklist = InitialBrickList,
                       tabdefs = dict:new(),
                       chain2tab = dict:new()},
    {_TS, Res} = write_bootstrap_kv(InitialBrickList,
                                    ?BKEY_SCHEMA_DEFINITION, Schema, add),
    Res.

%% @doc Write a key/value pair to the Admin Server's bootstrap bricks,
%%      where both the key and value are an arbitrary Erlang term.
%% @spec (list(), term(), term(), add | {replace, OldTS}) ->
%%       {timestamp(), {list(), list()}}

write_bootstrap_kv(BrickList, Key0, Val0, add) ->
    Key = term_to_binary(Key0),
    Val = term_to_binary(Val0),
    Op = my_make_add(Key, Val),
    TS = brick_server:get_op_ts(Op),
    Res = lists:map(
            fun({Brick, Node} = B) ->
                    catch start_standalone_brick(B),
                    case catch brick_server:do(Brick, Node, [Op]) of
                        {'EXIT', {Reason, _}} ->
                            {B, Reason};
                        [Res] ->
                            {B, Res}
                    end
            end, BrickList),
    {TS, Res}.

separate_good_from_bad(Res) ->
    {Good, Bad} =  lists:partition(fun({_Brick, ok}) -> true;
                                      (_)            -> false
                                   end, Res),
    Fbrick_names = fun(List) -> [Brick || {Brick, _} <- List] end,
    {Fbrick_names(Good), Fbrick_names(Bad)}.


%% @spec (list(brick_t()), bool(), path()) -> {list(brick_t()), list(brick_t())}

do_copy_new_schema(InitialBrickList, PropList, File) ->
    {ok, FileDir} = gmt_config_svr:get_config_value(application_data_dir, "/dev/null"),
    File1 = filename:join([FileDir, File]),
    {_, _, {ok, FileData}} =
        {should_exist, File1, file:read_file(File1)},
    Res = do_copy_new_schema2(File, FileData, InitialBrickList, PropList),
    case separate_good_from_bad(Res) of
        {[], []} = Res2 ->
            {ok, Res2};
        {[], _} = Res2 ->                       % None successful?
            {error, Res2};
        Res2 ->
            {ok, Res2}
    end.

do_copy_new_schema2(File, FileData, InitialBrickList, _PropList) ->
    [ {B, rpc:call(B, ?MODULE, do_copy_new_schema3, [File, FileData])}
      || B <- InitialBrickList ].

do_copy_new_schema3(File, FileData) ->
    {ok, FileDir} = gmt_config_svr:get_config_value(application_data_dir, "/dev/null"),
    File1 = filename:join([FileDir, File]),
    {_, _, {error, enoent}} =
        {should_not_exist, File1, file:read_file_info(File1)},
    ok = file:write_file(File1, FileData),
    {_, _, {ok, FileData}} =
        {should_exist, File1, file:read_file(File1)},
    ok.

%% @spec (term(), term()) -> term()
%% @doc A common make_add() so all uses of this op have the same tstamp.

my_make_add(Key, Value) ->
    brick_server:make_add(Key, Value).

% my_make_replace(Key, Value, OldTS) ->
%     brick_server:make_replace(Key, Value, exp_unused, [{testset, OldTS}]).

%% %% TODO move to brick_server?
%% get_ts_from_op6(Op6) when is_tuple(Op6), size(Op6) == 6 ->
%%     element(3, Op6).

%% @doc Sanity checking:
%% <ul>
%% <li> Make certain that chain names and brick names are all unique. </li>
%% <li> Make certain all nodes are running. </li>
%% <li> Make certain that the bricks don't already exist on any of
%%      the nodes. </li>
%% </ul>

do_add_table(Name, ChainList, BrickOptions, S) ->
    try begin
            {NewChainNames, NewBricks_0} = lists:unzip(ChainList),
            NewBricks = lists:append(NewBricks_0),
            Schema = S#state.schema,
            TabDs = dict:to_list(Schema#schema_r.tabdefs),
            OldChainsBricks =
                [ChBrs || {_TableName, T} <- TabDs,
                          ChBrs <- brick_hash:all_chains(
                                     T#table_r.ghash, current) ++
                              brick_hash:all_chains(T#table_r.ghash, new)],
            {OldChainNames, OldBricks_0} = lists:unzip(OldChainsBricks),
            OldBricks = lists:append(OldBricks_0),
            case ordsets:intersection(ordsets:from_list(OldChainNames),
                                      ordsets:from_list(NewChainNames)) of
                []      -> ok;
                Common1 -> exit({chain_names_in_use, Common1})
            end,
            case ordsets:intersection(ordsets:from_list(OldBricks),
                                      ordsets:from_list(NewBricks)) of
                []      -> ok;
                Common2 -> exit({bricks_in_use, Common2})
            end,
            lists:foreach(
              fun({_ChainName, Bricks} = _Ch) ->
                      ?DBG(_Ch),
                      lists:foreach(
                        fun({Brick, Node}) ->
                                %% 1. Node is running.
                                %% Not necessary to explicitly ping, but
                                %% it feels warm & fuzzy to do it anyway.
                                {Node, pong} = {Node, net_adm:ping(Node)},

                                %% 3. Brick is not running.
                                %% TODO: It's possible that we're re-using
                                %% an old brick name, and the brick may have
                                %% old log files lying around with old data.
                                %% We probably don't want to see that old stuff
                                case catch brick_server:status(Brick,
                                                               Node) of
                                    {'EXIT', {noproc, _}} ->
                                        ok;
                                    {ok, _} ->
                                        throw({brick_already_running,
                                               Brick, Node})
                                end,
                                %% 4. We can start & stop brick successfully.
                                %% Don't care about brick options here.
                                ok = start_brick_only(Brick, Node),
                                ok = stop_brick_only(Brick, Node),
                                _ = brick_sb:delete_brick_history(Brick, Node)
                        end, Bricks)
              end, ChainList),
            %% If we made it to here, there were no errors.
            do_add_table2(Name, ChainList, BrickOptions, S)
        end
    catch
        X:Y ->
            {{error, X, Y}, S}
    end.

%% @doc Continue adding table, assuming all sanity checking was done by caller.

do_add_table2(TableName, ChainList, BrickOptions, S) ->
    Schema = S#state.schema,
    case dict:find(TableName, Schema#schema_r.tabdefs) of
        {ok, _} ->
            {no_exists, S};
        error ->
            %% TODO: sanity checking?

            %% get hash init function from brick options
            Fun = proplists:get_value(hash_init, BrickOptions, fun brick_hash:naive_init/1),
            LHash = if is_function(Fun, 1) ->
                            Fun(ChainList);
                       is_function(Fun, 3) ->
                            Fun(via_proplist, ChainList, BrickOptions)
                    end,

            GHash = brick_hash:init_global_hash_state(
                      false, phase_unused, 1, LHash, ChainList,
                      LHash, ChainList),
            %% QQQ TODO unfinished.
            T = #table_r{name = TableName,
                         brick_options = BrickOptions,
                         current_rev = 1,
                         ghash = GHash},
            TDefs = dict:store(TableName, T, Schema#schema_r.tabdefs),
            NewC2T = lists:foldl(
                       fun({ChainName, _Bricks}, Dict) ->
                               dict:store(ChainName, TableName, Dict)
                       end, Schema#schema_r.chain2tab, ChainList),
            NewSchema = Schema#schema_r{tabdefs = TDefs, chain2tab = NewC2T},

            %% Create the new bricks.  We are assuming that the nodes that
            %% will be hosting each of the bricks is already running.
            %% TODO: If that assumption is false, then we may crash?

            case squorum_set(Schema#schema_r.schema_bricklist,
                             ?BKEY_SCHEMA_DEFINITION, NewSchema) of
                ok ->
                    spam_gh_to_all_nodes(self(), TableName),
                    async_start_pingers_and_chmons(NewSchema),
                    %% Ok, we're finally done.
                    {ok, S#state{schema = NewSchema}};
                error ->
                    {error, S}
            end
    end.

write_schema_bootstrap_file(File, BrickList) ->
    {ok, FH} = file:open(File, [write]),
    io:format(FH, "%% This is the list of standalone bricks that "
              "store this system's schema.\n"
              "%% Do not edit unless you know what you are doing.\n"
              "%% Written on: ~p ~p\n",
              [date(), time()]),
    io:format(FH, "~p.\n", [BrickList]),
    file:close(FH),
    ok.

%% @doc Convert the elements of a #schema record to a proplist.

schema_to_proplists(Schema) ->
    lists:map(fun({TableName, T}) ->
                      {TableName,
                       [{Name,element(Pos,T)} || {Pos,Name} <- info_table_r()]}
              end, dict:to_list(Schema#schema_r.tabdefs)).

%% @doc Entry point for the bootstrap brick scan &amp; repair loop.

bootstrap_scan_loop() ->
    register(bootstrap_scan_loop,self()),
    bootstrap_scan_loop2().

%% @doc Bootstrap brick scan &amp; repair loop.
%%
%% On each iteration of this loop, each bootstrap brick will be
%% started (if it isn't already).  Then all keys will be fetched, with
%% a side-effect of the fetch being that any inconsistencies in the
%% bricks will be fixed according to quorum agreement.

bootstrap_scan_loop2() ->
    timer:sleep(4*1000),
    Bricks = get_schema_bricks(?MODULE),
    %% Some schema bricks may not be running yet.
    Self = self(),
    spawn_link(fun() ->
                       %% TODO: In theory, the default option for a brick
                       %% should be 100% safety wrt logging & disk sync,
                       %% so theory says no brick options are necessary here.
                       %% However, will that always be true?
                       [_ = (catch start_brick_only(B)) || B <- Bricks],
                       [_ = {(catch brick_server:chain_set_my_repair_state(
                                      Br, Nd, ok)),
                             (catch brick_server:chain_role_standalone(
                                      Br, Nd))} || {Br, Nd} <- Bricks],
                       unlink(Self),
                       exit(normal)
               end),
    case catch get_all_bootstrap_keys(Bricks) of
        {'EXIT', _} ->
            noop;
        AllKeys ->
            %% If there was a delete happening in the middle of the
            %% get_all_bootstrap_keys() gathering process, then the
            %% passage of a bit of time will help us avoid most of the
            %% racing with the delete, and thus lower the chance that
            %% we'll re-create a deleted key.  We don't need the
            %% chance to be zero, because the 'schema_definition' key
            %% isn't deleted, it's only replaced, and our most crucial
            %% info is stored with that key.
            timer:sleep(1000),
            [_ = squorum_get(Bricks, K) || K <- AllKeys]
    end,
    ?MODULE:bootstrap_scan_loop2().

%% @doc Get all bootstrap keys (to assist scan &amp; repair loop).

get_all_bootstrap_keys(Bricks) ->
    get_all_bootstrap_keys(
      squorum_get_keys(Bricks, ?BRICK__GET_MANY_FIRST, 100),
      Bricks, []).

get_all_bootstrap_keys({ok, Keys, false}, _Bricks, Acc) ->
    lists:append(lists:reverse([Keys|Acc]));
get_all_bootstrap_keys({ok, Keys, true}, Bricks, Acc) ->
    LastKey = lists:last(Keys),
    get_all_bootstrap_keys(squorum_get_keys(Bricks, LastKey, 100),
                           Bricks, [Keys|Acc]).

do_get_table_info(TableName, S) when is_record(S, state) ->
    case dict:find(TableName, (S#state.schema)#schema_r.tabdefs) of
        {ok, T} ->
            Ps = [{Name, element(Pos,T)} || {Pos, Name} <- info_table_r()],
            {ok, Ps};
        X ->
            X
    end.

do_get_table_chain_list(TableName, S) when is_record(S, state) ->
    try
        Schema = S#state.schema,
        {ok, T} = dict:find(TableName, Schema#schema_r.tabdefs),
        GH = T#table_r.ghash,
        {ok, lists:usort(brick_hash:all_chains(GH, current) ++
                         brick_hash:all_chains(GH, new))}
    catch _:_ ->
            no_exists
    end.

%% @spec (atom(), atom(), proplist(), state_r()) -> state_r()

do_chain_status_change(ChainName, Status, PropList, S) ->
    %% ChainNow is the list of bricks currently in the chain.
    ChainNow = proplists:get_value(chain_now, PropList,
                                   {nevervalid, ChainName, Status, PropList}),
    Schema = S#state.schema,
    case dict:find(ChainName, Schema#schema_r.chain2tab) of
        {ok, TableName} ->
            {ok, T} = dict:find(TableName, Schema#schema_r.tabdefs),
            ?APPLOG_INFO(?APPLOG_APPM_015,"status_change: Chain ~p status ~p belongs to tab ~p\n", [ChainName, Status, TableName]),
            GH = T#table_r.ghash,
            NewGH = brick_hash:update_chain_dicts(GH, ChainName, ChainNow),
            NewSchema = update_tab_ghash(TableName, T, NewGH, Schema),
            ok = squorum_set(Schema#schema_r.schema_bricklist,
                             ?BKEY_SCHEMA_DEFINITION, NewSchema),
            spam_gh_to_all_nodes(self(), TableName),
            S#state{schema = NewSchema};
        _ ->
            S
    end.

do_set_gh_minor_rev(State) ->
    Schema = State#state.schema,
    %% Use one timestamp for all tables.
    Timestamp = brick_server:make_timestamp(),
    %% Update all global hashes in all tables and get a new schema
    {NewSchema, Reply} = dict:fold(fun(TableName, Table, {S, RList}) ->
                                       GH = Table#table_r.ghash,
                                       MR = GH#g_hash_r.minor_rev,
                                       NewGH = GH#g_hash_r{minor_rev=Timestamp},
                                       NewS = update_tab_ghash(TableName, Table, NewGH, S),
                                       {NewS, [{TableName, MR, Timestamp}|RList]}
                                   end, {Schema, []}, Schema#schema_r.tabdefs),
    %% Save the schema to the quorum
    ok = squorum_set(NewSchema#schema_r.schema_bricklist, ?BKEY_SCHEMA_DEFINITION, NewSchema),
    %% Update the state and reply
    {Reply, State#state{schema = NewSchema}}.

%% Add a list of new bricks as bootstrap bricks
do_add_bootstrap_copy(BrickList, State) ->
    OldSchema = State#state.schema,
    OldBootList = OldSchema#schema_r.schema_bricklist,
    NewBootList = OldBootList ++ BrickList,
    case contains_duplicate_bricks(NewBootList) of
        true ->
            {{error, duplicates}, State};
        false ->
            NewSchema = OldSchema#schema_r{schema_bricklist=NewBootList},
            %% start new bootstrap bricks.
            [catch start_standalone_brick(Brick) || Brick <- BrickList],
            %% Update the schema hint file
            BootstrapFile = "Schema.local",
            OldFile = BootstrapFile ++ integer_to_list(gmt_time:time_t()),
            file:rename(BootstrapFile, OldFile),
            ok = write_schema_bootstrap_file(BootstrapFile, NewBootList),
            %% Save the schema to the combined new bricklist quorum
            ok = squorum_set(NewSchema#schema_r.schema_bricklist, ?BKEY_SCHEMA_DEFINITION, NewSchema),
            {{ok,NewBootList}, State#state{schema = NewSchema}}
    end.

%% returns true if BrickList contains duplicate BrickNames
contains_duplicate_bricks(BrickList) ->
    UniqueBrickNames = length(lists:usort([B || {B,_} <- BrickList])),
    UniqueBrickNames =/= length(BrickList).

%% info_schema_r() ->
%%     Es = record_info(fields, schema_r),
%%     lists:zip(lists:seq(2, length(Es) + 1), Es).

info_table_r() ->
    Es = record_info(fields, table_r),
    lists:zip(lists:seq(2, length(Es) + 1), Es).

do_spam_gh_to_all_nodes(S, DesiredTableName) ->
    %% TODO: We're spamming all tables' GH.  In theory, we only need to
    %%       spam once, all others are single table GH updates.
    ?APPLOG_INFO(?APPLOG_APPM_016,"do_spam_gh_to_all_nodes: top\n",[]),
    AllTabDefs = dict:to_list((S#state.schema)#schema_r.tabdefs),
    TabDefs = if DesiredTableName == '$$all_tables$$' ->
                      AllTabDefs;
                 true ->
                      [X || {TableName, _} = X <- AllTabDefs,
                            TableName == DesiredTableName]
              end,
    Fs = lists:foldl(fun({TableName, T}, Acc) ->
                             GH = T#table_r.ghash,
                             ?APPLOG_INFO(?APPLOG_APPM_106,
                                          "do_spam_gh_to_all_nodes: minor_rev=~p\n",
                                          [GH#g_hash_r.minor_rev]),
                             AllChains = lists:usort(
                                           brick_hash:all_chains(GH, current)
                                           ++ brick_hash:all_chains(GH, new)),
                             %% We're interested in all alive bricks,
                             %% not all dead-or-alive bricks.
                             Blist = [B || {_, Bs} <- AllChains, B <- Bs],
                             AllBricks = lists:usort(Blist),
                             %% We weren't checking the results of these calls,
                             %% so we'll use absurdly short timeouts, 1 msec,
                             %% and do them in serial instead of in parallel,
                             %% where we definitely run the risk of spawning
                             %% too many procs.
                             [fun() ->
                                 [catch brick_server:chain_hack_set_global_hash(
                                    Br, Nd, GH, 1) || {Br, Nd} <- AllBricks]
                              end ] ++
                              [fun() ->
                                  %% Timer hack to allow bricks to be set first
                                  timer:sleep(200),
                                  [catch brick_simple:set_gh(
                                           Nd, TableName, GH, 1)
                                         || Nd <- [node()|nodes()]]
                               end] ++ Acc
                     end, [], TabDefs),
    [spawn_opt(F, [{priority, high}]) || F <- Fs].

%% @doc Try to find the global hash record associated with a chain.

do_get_gh_by_chainname(ChainName, S) ->
    Schema = S#state.schema,
    case dict:find(ChainName, Schema#schema_r.chain2tab) of
        {ok, TableName} ->
            {ok, T} = dict:find(TableName, Schema#schema_r.tabdefs),
            {ok, T#table_r.ghash};
        _ ->
            {error, not_found}
    end.

%% NOTE: Caller is catch'ing us, *but* must call exit() on error because
%%       the smart_exceptions parse transform is doing weird things to us!!

do_change_chain_length(_ChainName, [], _S) ->
    exit({error, empty_chain_list});
do_change_chain_length(ChainName, NewBrickList, S) ->
    case do_get_gh_by_chainname(ChainName, S) of
        {error, _} = Err ->
            {Err, S};
        {ok, OldGH} ->
            AllChains =
                (OldGH#g_hash_r.current_h_desc)#hash_r.healthy_chainlist ++
                (OldGH#g_hash_r.new_h_desc)#hash_r.healthy_chainlist,
            OldBrickList = proplists:get_value(ChainName, AllChains),
            if OldBrickList == NewBrickList -> exit({error, same_list});
               true                         -> ok  end,
            OldBSet = ordsets:from_list(OldBrickList),
            NewBSet = ordsets:from_list(NewBrickList),
            case ordsets:intersection(OldBSet, NewBSet) of
                [] -> exit({error, no_intersection});
                _  -> ok  end,
            ok = brick_hash:verify_chain_list([{ChainName, NewBrickList}],
                                              false),
            NewOnlyBricks = NewBSet -- OldBSet,
            OldOnlyBricks = OldBSet -- NewBSet,
            lists:foreach(
              fun({Br, Nd} = B) ->
                      case (catch brick_server:status(Br, Nd)) of
                          {ok, _} ->
                              exit({error, {brick_running, B}});
                          _ ->
                              _ = brick_sb:delete_brick_history(Br, Nd)
                      end
              end, NewOnlyBricks),
            Schema = S#state.schema,
            {ok, TableName} = dict:find(ChainName, Schema#schema_r.chain2tab),
            {ok, T} = dict:find(TableName, Schema#schema_r.tabdefs),
            NewCurDesc = brick_hash:desc_substitute_chain(
                           OldGH#g_hash_r.current_h_desc, ChainName, NewBrickList),
            NewNewDesc = brick_hash:desc_substitute_chain(
                           OldGH#g_hash_r.new_h_desc, ChainName, NewBrickList),
            %%io:format("x NewCurDesc = ~p\n", [NewCurDesc]),
            %%io:format("x NewNewDesc = ~p\n", [NewNewDesc]), timer:sleep(2000),
            NewGH_0 = brick_hash:init_global_hash_state(
                        OldGH#g_hash_r.migrating_p, OldGH#g_hash_r.phase,
                        OldGH#g_hash_r.current_rev,
                        NewCurDesc, NewCurDesc#hash_r.healthy_chainlist,
                        NewNewDesc, NewNewDesc#hash_r.healthy_chainlist),

            %% Tricky bit: in the event that we're shrinking the chain,
            %% we have the following problem if we use the old GH chain
            %% dicts are they currently are:
            %%  1. assume old chain = B1, B2, B3
            %%  2. assume new chain = B1, B2
            %% If we keep the current chain dicts, tail will still be B3.
            %% When we spam the resulting GH, clients will send read queries
            %% to B3.  If we have a race with an update that propagates down
            %% the chain and stop at tail + official_tail B2, then the
            %% query to B3 will return stale data.
            %%
            %% Our problem: we aren't the chain monitor, so we don't have
            %% full insight into the most-current status of the chain.  We
            %% have a slightly out-of-date notion of the current chain, the
            %% notion stored in the current_chain_dict and new_chain_dict.
            %%
            %% Solution: we fabricate a lie.  We'll take the current & new
            %% chain dicts, filter out the OldOnlyBricks, then use the first
            %% brick in the resulting chain as both the head and tail.  This
            %% may send some read queries to the wrong brick, but that brick
            %% will do the forwarding for us until the new chainmon process
            %% can inform us of its new decision.
            [CurrentChDictBrs_0] =
                [Brs || {Ch, Brs} <- brick_hash:all_chains(OldGH, current),
                        Ch == ChainName],
            CurrentChDictBrs = CurrentChDictBrs_0 -- OldOnlyBricks,
            [NewChDictBrs_0] =
                [Brs || {Ch, Brs} <- brick_hash:all_chains(OldGH, new),
                        Ch == ChainName],
            NewChDictBrs = NewChDictBrs_0 -- OldOnlyBricks,
            {CurrentHead, NewHead} =
                case {CurrentChDictBrs, NewChDictBrs} of
                    {[CurHd|_], [NewHd|_]} ->
                        {CurHd, NewHd};
                    _ ->
                        exit({error, current_chains_too_short})
                end,
            NewGH_1 = brick_hash:update_chain_dicts(NewGH_0, ChainName,
                                                    [CurrentHead]),
            NewGH_2 = brick_hash:update_chain_dicts(NewGH_1, ChainName,
                                                    [NewHead]),
            NewGH_3 = NewGH_2#g_hash_r{minor_rev = OldGH#g_hash_r.minor_rev+1},

            NewSchema = update_tab_ghash(TableName, T, NewGH_3, Schema),
            ok = squorum_set(NewSchema#schema_r.schema_bricklist,
                             ?BKEY_SCHEMA_DEFINITION, NewSchema),
            spam_gh_to_all_nodes(self()),
            NewS = S#state{schema = NewSchema},
            %% Use this try/catch to *always* return NewS, which is
            %% quite important now that the schema has been written to
            %% the bootstrap bricks.
            try begin
                    %% OK, now that we've updated our schema in
                    %% persistent storage, time to take action.
                    ChainMonName = brick_chainmon:chain2name(ChainName),
                    ok = supervisor:terminate_child(brick_mon_sup, ChainMonName),
                    ok = supervisor:delete_child(brick_mon_sup, ChainMonName),
                    timer:sleep(1*1000),
                    %% This will do a lot of extra work for stuff
                    %% already running, but that's OK.
                    _ = start_pingers_and_chmons(NewSchema),
                    timer:sleep(1*1000),        % TODO: figure out necessity
                    ?APPLOG_INFO(?APPLOG_APPM_017,"OldOnlyBricks = ~p\n", [OldOnlyBricks]),
                    [spawn(fun() ->
                                   %% Give monitors time to die & restart.
                                   timer:sleep(2*1000), % TODO: figure out necessity
                                   ok = stop_bp(Br, Nd),
                                   _ = stop_brick_del_history(Br, Nd)
                           end) || {Br, Nd} <- OldOnlyBricks],
                    {ok, NewS}
                end
            catch
                X:Y ->
                    ?APPLOG_ALERT(?APPLOG_APPM_018,
                                  "do_change_chain_length: End: ~p ~p\n", [X, Y]),
                    {{error, {X, Y}}, NewS}
            end
    end.

%% NOTE: Caller is catch'ing us, *but* must call exit() on error because
%%       the smart_exceptions parse transform is doing weird things to us!!

do_start_migration(TableName, NewLH, Options, S) ->
    Schema = S#state.schema,
    case dict:find(TableName, (S#state.schema)#schema_r.tabdefs) of
        {ok, T} ->
            OldGH = T#table_r.ghash,
            HasMigMonP = lists:keymember(TableName, 1, S#state.mig_mons),
            if OldGH#g_hash_r.migrating_p; HasMigMonP ->
                    exit({already_migrating, OldGH#g_hash_r.current_rev,
                          OldGH#g_hash_r.cookie});
               true ->
                    ok
            end,
            CurLH = OldGH#g_hash_r.current_h_desc,
            CurChainList = CurLH#hash_r.healthy_chainlist,
            NewChainList = NewLH#hash_r.healthy_chainlist,
            CurChNames = [Ch || {Ch, _} <- CurChainList],
            NewChNames = [Ch || {Ch, _} <- NewChainList],
            BothChNames = ordsets:intersection(ordsets:from_list(CurChNames),
                                               ordsets:from_list(NewChNames)),
            lists:map(
              fun(ChName) ->
                      CurBrs = proplists:get_value(ChName, CurChainList),
                      NewBrs = proplists:get_value(ChName, NewChainList),
                      if CurBrs /= NewBrs ->
                              exit({chain_membership_differs, ChName});
                         true ->
                              ok
                      end
              end, BothChNames),
            NewOnlyChNames = NewChNames -- CurChNames,
            NewCh2Tab = lists:foldl(fun(ChName, D) ->
                                            dict:store(ChName, TableName, D)
                                    end,
                                    Schema#schema_r.chain2tab, NewOnlyChNames),
            %% Rely on some sanity checking in init_global...
            NewGH_0 = brick_hash:init_global_hash_state(
                        true, pre, OldGH#g_hash_r.current_rev,
                        CurLH, CurChainList, NewLH, NewChainList, false),
            Cookie = now(),
            NewGH_1 = NewGH_0#g_hash_r{cookie = Cookie,
                                       minor_rev = OldGH#g_hash_r.minor_rev+1},
            NewGH_2 = lists:foldl(
                        fun({ChName, Bs}, GHx) ->
                                brick_hash:update_chain_dicts(GHx, ChName, Bs)
                        end, NewGH_1, brick_hash:all_chains(OldGH, current)),
            NewGH = lists:foldl(
                      fun(ChName, GHx) ->
                              brick_hash:update_chain_dicts(GHx, ChName, [])
                      end, NewGH_2, NewOnlyChNames),
            ?APPLOG_INFO(?APPLOG_APPM_019,
                         "Migration number ~p is starting with cookie ~p\n",
                         [NewGH#g_hash_r.current_rev, Cookie]),
            %% This call to update_tab_ghash() is not quite like the others
            %% because we need to update current_phase and chain2tab.
            T2 = T#table_r{current_phase = migrating, ghash = NewGH,
                           migration_options = Options},
            Schema2 = Schema#schema_r{chain2tab = NewCh2Tab},
            NewSchema = update_tab_ghash(TableName, T2, NewGH, Schema2),
            ok = squorum_set(Schema#schema_r.schema_bricklist,
                             ?BKEY_SCHEMA_DEFINITION, NewSchema),
            spam_gh_to_all_nodes(self(), TableName),
            async_start_pingers_and_chmons(NewSchema),
            {ok, MPid} = brick_migmon:start_link(T2, Options),
            MMs = S#state.mig_mons,
            {{ok, Cookie}, S#state{schema = NewSchema,
                                   mig_mons = [{TableName, MPid}|MMs]}};
        _ ->
            exit({unknown_table, TableName})
    end.


do_table_almost_finished_migration(TableName, S) ->
    Schema = S#state.schema,
    {ok, T} = dict:find(TableName, Schema#schema_r.tabdefs),
    OldGH = T#table_r.ghash,
    AllActiveChains = lists:usort(brick_hash:all_chains(OldGH, current) ++
                                  brick_hash:all_chains(OldGH, new)),
    CurLH = OldGH#g_hash_r.current_h_desc,
    NewLH = OldGH#g_hash_r.new_h_desc,
    CurChainList = CurLH#hash_r.healthy_chainlist,
    NewChainList = NewLH#hash_r.healthy_chainlist,
    CurChNames = [Ch || {Ch, _} <- CurChainList],
    NewChNames = [Ch || {Ch, _} <- NewChainList],
    OldOnlyChNames = CurChNames -- NewChNames,
    OldOnlyBricks = [Br || {Ch, Brs} <- CurChainList,
                           lists:member(Ch, OldOnlyChNames),
                           Br <- Brs],
    AllBricks = [Br || {_Ch, Brs} <- CurChainList ++ NewChainList, Br <- Brs],
    try
        begin
            N = lists:foldl(
                  fun({_ChainName, []}, DoneCount) ->
                          bummer_no_active_bricks_right_now,
                          DoneCount;
                     ({_ChainName, [{HeadBrick, HeadNode}|_]}, DoneCount) ->
                          %% TODO: replace [] with real Options list.
                          case (catch brick_server:migration_clear_sweep(
                                        HeadBrick, HeadNode)) of
                              {ok, _When, _PropList} ->
                                  %% TODO: harvest stuff in _PropList?
                                  DoneCount + 1;
                              {error, not_migrating, _} ->
                                  %% Perhaps down last time, now up?
                                  DoneCount + 1;
                              _ ->
                                  DoneCount
                          end
                  end, 0, AllActiveChains),
            io:format("done N = ~p, wanted = ~p\n", [N, length(AllActiveChains)]),
            if N == length(AllActiveChains) ->
                    ?APPLOG_INFO(?APPLOG_APPM_020,"Migration number ~p almost finished\n",
                                 [OldGH#g_hash_r.current_rev]),
                    do_table_finished_migration_cleanup(
                      TableName, OldOnlyChNames, OldOnlyBricks, AllBricks, S);
               true ->
                    S
            end
        end
    catch
        X:Y ->
            io:format("do_table_finished_migration: ~p: ~p ~p\n", [TableName, X, Y]),
            %% Return the status quo
            S
    end.

do_table_finished_migration_cleanup(TableName, OldOnlyChNames, OldOnlyBricks,
                                    AllBricks, S) ->
    Schema = S#state.schema,
    {ok, T} = dict:find(TableName, Schema#schema_r.tabdefs),
    OldGH = T#table_r.ghash,

    NewRev = OldGH#g_hash_r.current_rev + 1,
    NewDesc = OldGH#g_hash_r.new_h_desc,
    NewGH_0 = brick_hash:init_global_hash_state(
                false, pre, NewRev,
                NewDesc, NewDesc#hash_r.healthy_chainlist,
                NewDesc, NewDesc#hash_r.healthy_chainlist),
    NewGH_1 = NewGH_0#g_hash_r{minor_rev =
                               OldGH#g_hash_r.minor_rev+1},
    NewGH = lists:foldl(
              fun({ChName, Bs}, GHx) ->
                      brick_hash:update_chain_dicts(GHx, ChName, Bs)
              end, NewGH_1, brick_hash:all_chains(OldGH, new)),
    T2 = T#table_r{current_rev = NewRev, current_phase = pre},
    NewSchema = update_tab_ghash(TableName, T2, NewGH, Schema),
    ok = squorum_set(NewSchema#schema_r.schema_bricklist,
                     ?BKEY_SCHEMA_DEFINITION, NewSchema),
    spam_gh_to_all_nodes(self(), TableName),
    ParentPid = self(),
    spawn(fun() ->
                  timer:sleep(2*1000),
                  [(catch brick_server:migration_clear_sweep(
                            Br, Nd)) || {Br, Nd} <- AllBricks],
                  stop_old_chains_and_bricks(
                    OldOnlyChNames, OldOnlyBricks),
                  ParentPid ! {clear_migmon_pid, TableName},
                  exit(normal)
          end),
    ?APPLOG_INFO(?APPLOG_APPM_021,"Migration number ~p finished\n",
                 [OldGH#g_hash_r.current_rev]),
    S#state{schema = NewSchema}.

update_tab_ghash(TableName, T, NewGH, Schema) ->
    NewT = T#table_r{ghash = NewGH},
    NewTDefs = dict:store(TableName, NewT, Schema#schema_r.tabdefs),
    Schema#schema_r{tabdefs = NewTDefs}.

%% @doc Stop all of the bricks (and associated health monitoring procs)
%%      in chains that are no longer needed after migration has finished.

stop_old_chains_and_bricks(OldOnlyChNames, OldOnlyBricks) ->
    ?APPLOG_INFO(?APPLOG_APPM_022,"migration cleanup: old chains: ~p\n",
                 [OldOnlyChNames]),
    [ok = stop_chmon(Ch) || Ch <- OldOnlyChNames],
    [_ = (catch brick_sb:delete_chain_history(Ch)) || Ch <- OldOnlyChNames],
    ?APPLOG_INFO(?APPLOG_APPM_023,"migration cleanup: old bricks: ~p\n",
                 [OldOnlyBricks]),
    [_ = (catch stop_bp(Br, Nd)) || {Br, Nd} <- OldOnlyBricks],
    timer:sleep(1*1000),
    [_ = (catch stop_brick_del_history(Br, Nd)) || {Br, Nd} <- OldOnlyBricks],
    ok.

%% @doc Stop a chain monitor process.

stop_chmon(ChainName) ->
    MonName = brick_chainmon:chain2name(ChainName),
    ?APPLOG_INFO(?APPLOG_APPM_024,"stop_chmon: stopping ~p\n", [MonName]),
    ok = sup_stop_status(supervisor:terminate_child(brick_mon_sup, MonName)),
    ok = sup_stop_status(supervisor:delete_child(brick_mon_sup, MonName)),
    ok.

%% @doc Stop a brick pinger process.

stop_bp(BrickName, BrickNode) ->
    MonName = brick_bp:make_pinger_registered_name(BrickName),
    ?APPLOG_INFO(?APPLOG_APPM_025,"stop_bp: stopping ~p on ~p\n", [MonName,BrickNode]),
    ok = sup_stop_status(supervisor:terminate_child({brick_mon_sup, BrickNode}, MonName)),
    ok = sup_stop_status(supervisor:delete_child({brick_mon_sup, BrickNode}, MonName)),
    ok.

%% @doc Stop a brick and delete its local log.

stop_brick_del_history(BrickName, BrickNode) ->
    ?APPLOG_INFO(?APPLOG_APPM_026,"Deleting brick history: {~p,~p}\n",
                 [BrickName, BrickNode]),
    _ = brick_sb:delete_brick_history(BrickName, BrickNode),
    ?APPLOG_INFO(?APPLOG_APPM_027,"Stopping old brick: {~p,~p}\n",
                 [BrickName, BrickNode]),
    catch brick_shepherd:stop_brick(BrickName, BrickNode),
    catch gmt_hlog_common:full_writeback(?GMT_HLOG_COMMON_LOG_NAME),
    catch rpc:call(BrickNode,
                   gmt_hlog_common, full_writeback,[?GMT_HLOG_COMMON_LOG_NAME]),
    catch rpc:call(BrickNode,
                   gmt_hlog_common, permanently_unregister_local_brick,
                   [?GMT_HLOG_COMMON_LOG_NAME, BrickName]),
    RmCmd = "rm -rf " ++ gmt_hlog:log_name2data_dir(BrickName),
    ?APPLOG_INFO(?APPLOG_APPM_028,"Stopping old brick, cmd = '" ++RmCmd++ "'\n",[]),
    _ = rpc:call(BrickNode, os, cmd, [RmCmd]),
    ok.

sup_stop_status(ok) ->                 ok;
sup_stop_status({error, not_found}) -> ok;
sup_stop_status(X) ->                  X.

%% keep_old_gh_chain_dicts(GH, NewGH_0) ->
%%     NewGH_0#g_hash_r{current_chain_dict = GH#g_hash_r.current_chain_dict,
%%                   new_chain_dict = GH#g_hash_r.new_chain_dict}.

%% @doc Local wrapper for brick_squorum:get()

-spec squorum_get(brick_squorum:brick_list(),
                  key() | ?BKEY_CLIENT_MONITOR | ?BKEY_SCHEMA_DEFINITION) ->
                         {ok, ts(), #schema_r{}} |
                             {ts_error, ts()} | key_not_exist | error | quorum_error.
squorum_get(Bricks, Key) ->
    case brick_squorum:get(Bricks, term_to_binary(Key)) of
        {ok, TS, Val} ->
            {ok, TS, binary_to_term(Val)};
        Res ->
            Res
    end.

%% @doc Local wrapper for brick_squorum:set()

-spec squorum_set(brick_squorum:brick_list(), ?BKEY_SCHEMA_DEFINITION, #schema_r{}) ->
                         brick_squorum:set_res().
squorum_set(Bricks, Key, Val) ->
    brick_squorum:set(Bricks, term_to_binary(Key), term_to_binary(Val)).

squorum_set(Bricks, Key, Val, ExpTime, Flags, Timeout) ->
    brick_squorum:set(Bricks, term_to_binary(Key), term_to_binary(Val),
                      ExpTime, Flags, Timeout).

%% @doc Local wrapper for brick_squorum:get_keys()

-spec squorum_get_keys(brick_squorum:brick_list(),
                       key() | ?BRICK__GET_MANY_FIRST,
                       integer()) ->
                              {ok, list(), boolean()}.
squorum_get_keys(Bricks, Key, MaxNum) ->
    {ok, Keys, Bool} = brick_squorum:get_keys(Bricks, term_to_binary(Key), MaxNum),
    {ok, [binary_to_term(K) || K <- Keys], Bool}.

%% @doc Check partition detector beacons for signs of another Admin
%% Server running, returning the # of beacons seen (0 = no other
%% A.S. instances were observed).
%%
%% Intervals = first_time | integer()

check_for_other_admin_server_beacons(Intervals, MyStartTime) ->
    {ok, FailureInterval} = gmt_config_svr:get_config_value_i(
                              heartbeat_failure_interval, 15),
    FailUSecs = FailureInterval * 1000*1000,
    check_for_other_admin_server_beacons(Intervals, MyStartTime, FailUSecs).

check_for_other_admin_server_beacons(first_time, MyStartTime, FailUSecs) ->
    case running_admin_beacons_p(FailUSecs, MyStartTime) of
        false ->
            ?APPLOG_INFO(?APPLOG_APPM_029,"First check for an Admin Server beacon "
                         "found zero beacons.\n", []),
            ok;
        true ->
            %% Oh oh, we see a beacon from a running admin server.  We
            %% need to wait for the cluster timeout interval before
            %% we go any further.
            ?APPLOG_WARNING(?APPLOG_APPM_030,"Beacon detected from another "
                            "Admin Server, sleeping for "
                            "heartbeat_failure_interval seconds.\n",
                            []),
            timer:sleep(FailUSecs div 1000)
    end,
    check_for_other_admin_server_beacons(3, MyStartTime);
check_for_other_admin_server_beacons(Intervals, MyStartTime, FailUSecs) ->
    {ok, IntervalTime} = gmt_config_svr:get_config_value_i(
                           heartbeat_beacon_interval, 1000),
    WaitT = ((Intervals * IntervalTime) div 1000) + 1,
    NowT = gmt_time:time_t(),
    N = gmt_loop:do_while(
          fun(Acc) ->
                  OthersP = running_admin_beacons_p(FailUSecs, MyStartTime),
                  timer:sleep(500),
                  Now = gmt_time:time_t(),
                  NewAcc = Acc + if not OthersP -> 0;
                                    true        -> 1
                                 end,
                  io:format("OthersP ~p, cond ~p, NewAcc ~p\n", [OthersP, (Now < NowT + WaitT) andalso NewAcc == 0, NewAcc]),
                  {(Now < NowT + WaitT) andalso NewAcc == 0, NewAcc}
          end, 0),
    N.

%% @doc Return true if any running Admin Server beacons are recent.

running_admin_beacons_p(USecs, MyStartTime) ->
    Bs0 = partition_detector_server:get_last_beacons(),
    io:format("Bs0 ~p\n", [Bs0]),
    Now = now(),
    Bs = lists:filter(fun(B) ->
                              io:format("Diff ~p ~p\n", [timer:now_diff(Now, B#beacon.time), B]),
                              timer:now_diff(Now, B#beacon.time) < USecs
                      end, Bs0),
    io:format("Bs ~p\n", [Bs]),
    Es = lists:flatten([B#beacon.extra || B <- Bs]),
    io:format("Es ~p\n", [Es]),
    lists:any(fun({brick_admin, {Phase, StartTime, Node, _Pid}}) ->
                      Diff = timer:now_diff(StartTime, MyStartTime),
                      Node /= node()
                          andalso (Phase == running orelse Diff < 0)
              end, Es).

%% @spec (list(), bool(), bool(), bool(), chain_list()) -> list()
%% @doc Make a list of brick options based on the passed booleans.
%%
%% Valid properties in DataProps list (all other props ignored):
%% <ul>
%% <li> maketab_bigdata ... boolean, determines presence/absence
%%      of the brick {bigdata_dir, string()} property. </li>
%% <li> maketab_do_logging ... boolean, determines the brick
%%      do_logging property. </li>
%% <li> maketab_do_sync ... boolean, determines the brick
%%      do_sync property. </li>
%% </ul>

make_common_table_opts(DataProps, VarPrefixP, PrefixSep, NumSep, Chains)
  when is_list(DataProps) ->
    HashOpts = if VarPrefixP ->
                       [{hash_init, fun brick_hash:chash_init/3},
                        {prefix_method, var_prefix},
                        {prefix_separator, PrefixSep},
                        {num_separators, NumSep}];
                  true ->
                       [{prefix_method, all}]
               end,
    DataOpts =
        case proplists:get_value(maketab_bigdata, DataProps) of
            true ->
                [{bigdata_dir, "cwd"}];
            _ ->
                []
        end ++
        case proplists:get_value(maketab_do_logging, DataProps, true) of
            true -> [{do_logging, true}];
            _    -> [{do_logging, false}]
        end ++
        case proplists:get_value(maketab_do_sync, DataProps, true) of
            true -> [{do_sync, true}];
            _    -> [{do_sync, false}]
        end,
    HistoryOpts = [{created_date, date()}, {created_time, time()}],
    ChainWeights = [{Ch, 100} || {Ch, _Brs} <- Chains],
    [{hash_init, fun brick_hash:chash_init/3},
     {old_float_map, []},
     {new_chainweights, ChainWeights}] ++ HashOpts ++ DataOpts ++ HistoryOpts.

%% @doc Bootstrap a cluster from the passed options.
%%
%% For description, see the "Hibary Developer's Guide",
%% "Add a New Table: brick_admin:add_table()" section.

bootstrap(Schema, DataProps, VarPrefixP, PrefixSep, NumSep, NodeList,
          BricksPerChain, BootstrapNodes) ->
    bootstrap(Schema, DataProps, VarPrefixP, PrefixSep, NumSep, NodeList,
              BricksPerChain, BootstrapNodes, []).

%% @spec (string(), proplist(), bool(), integer(), integer(), list(), integer(), integer(), list()) -> {ok, state_r()}
%% @doc Bootstrap a cluster from the passed options.
%%
%% DataProps is a proplist for data storage aspects of the brick:
%% bigdata_dir on/off, disk logging on/off, sync writes on/off, etc.
%%
%% For description, see the "Hibary Developer's Guide",
%% "Add a New Table: brick_admin:add_table()" section.

bootstrap(Schema, DataProps, VarPrefixP, PrefixSep, NumSep, NodeList,
          BricksPerChain, BootstrapNodes, Props)
  when is_list(DataProps) ->
    Rs = [catch net_adm:ping(Nd) || Nd <- NodeList ++ BootstrapNodes],
    case lists:usort(Rs) of
        [pong] ->
            bootstrap1(Schema, DataProps, VarPrefixP, PrefixSep, NumSep,
                       NodeList, BricksPerChain, BootstrapNodes, Props);
        _ ->
            throw({node_error,
                   [Nd || {Nd, R} <- lists:zip(NodeList ++ BootstrapNodes, Rs),
                          R /= pong]})
    end.

bootstrap1(Schema, DataProps, VarPrefixP, PrefixSep, NumSep, NodeList,
           BricksPerChain, BootstrapNodes, Props) ->
    NsNds = lists:zip(lists:seq(1, length(BootstrapNodes)), BootstrapNodes),
    BootsNodes = [{list_to_atom("bootstrap_copy" ++ integer_to_list(N)) , Nd}
                  || {N, Nd} <- NsNds],
    {ok, _} = brick_admin:create_new_schema(BootsNodes, Schema),
    brick_admin:start(Schema),
    timer:sleep(1000),

    Table = ?S3_TABLE,
    NumNodesPerBlock = proplists:get_value(num_nodes_per_block, Props, 0),
    BlockMultFactor = proplists:get_value(block_mult_factor, Props, 0),
    Chains = brick_admin:make_chain_description(Table, BricksPerChain, NodeList,
                                                NumNodesPerBlock,
                                                BlockMultFactor),
    BrickOpts = make_common_table_opts(
                  DataProps, VarPrefixP, PrefixSep, NumSep, Chains),

    brick_admin:add_table(Table, Chains, BrickOpts).

%% @doc Bootstrap a cluster using the local node as the only storage node.

bootstrap_local(DataProps, VarPrefixP, PrefixSep, NumSep, Bricks,
                BricksPerChain, BootstrapNodes) ->
    bootstrap_local(DataProps, VarPrefixP, PrefixSep, NumSep, Bricks,
                    BricksPerChain, BootstrapNodes, []).

%% @spec (atom(), bool(), integer(), integer(), integer(), integer(), list(), list()) -> {ok, state_r()}
%% @doc Bootstrap a cluster using the local node as the only storage node.

bootstrap_local(DataProps, VarPrefixP, PrefixSep, NumSep, Bricks,
                BricksPerChain, _BootstrapNodes, Props) ->
    NodeList = lists:duplicate(Bricks, node()),
    bootstrap("Schema.local", DataProps, VarPrefixP, PrefixSep, NumSep,
              NodeList, BricksPerChain, [node()], Props).

%% add the given bricks as bootstrap_copy bricks.
add_bootstrap_copy(BrickList) ->
    %% confirm nodes pingable before calling into the gen_server
    NodePongs = [{N,net_adm:ping(N)} || {_,N} <- BrickList],
    case lists:keyfind(pang, 2, NodePongs) of
        {Node, pang} ->
            {error, nodedown, Node};
        false ->
            gen_server:call(?MODULE, {add_bootstrap_copy, BrickList}, 90*1000)
    end.

do_add_client_monitor(Node) ->
    Res = do_mod_client_monitor(Node, fun(Nd, OldList) ->
                                              [Nd] ++ (OldList -- [Nd]) end),
    spawn(fun() -> run_client_monitor_procs() end),
    Res.

do_delete_client_monitor(Node) ->
    Res = do_mod_client_monitor(Node, fun(Nd, OldList) -> OldList -- [Nd] end),
    ?APPLOG_INFO(?APPLOG_APPM_031,"Deleting client monitors for node ~p: ~p\n",
                 [Node, Res]),
    [begin
         Name = client_monitor_name(Node, AppName),
         case (catch supervisor:terminate_child(brick_mon_sup, Name)) of
             ok ->
                 gmt_util:clear_alarm({client_node_down, Node}, fun() -> ok end),
                 catch supervisor:delete_child(brick_mon_sup, Name);
             Err ->
                 Err
         end
     end || AppName <- client_mon_app_names()],
    Res.

client_mon_app_names() ->
    [gdss_client].

do_mod_client_monitor(Node, Fun) ->
    {SBricks, Flags, OldList} = get_client_monitor_details(),
    NewList = Fun(Node, OldList),
    squorum_set(SBricks, ?BKEY_CLIENT_MONITOR, NewList, 0, Flags, 5*1000).

get_client_monitor_details() ->
    SBricks = get_schema_bricks(),
    case squorum_get(SBricks, ?BKEY_CLIENT_MONITOR) of
        {ok, TS, V1}  -> {SBricks, [{testset, TS}], V1};
        key_not_exist -> {SBricks, [], []}
    end.

%% @doc Start local client monitor processes under the brick_clientmon
%%      supervisor.

run_client_monitor_procs() ->
    Nodes = get_client_monitor_list(),
    NoOp = fun() -> ok end,
    FunUp = fun(Node, App) -> ?APPLOG_INFO(?APPLOG_APPM_032,
                                           "Node ~p application ~p is now running\n",
                                           [Node, App]),
                              brick_admin:spam_gh_to_all_nodes(),
                              gmt_util:clear_alarm({client_node_down, Node},
                                                   NoOp)
            end,
    FunDn = fun(Node, App) -> ?APPLOG_ALERT(?APPLOG_APPM_033,
                                            "Node ~p application ~p is stopped\n",
                                            [Node, App]),
                              gmt_util:set_alarm({client_node_down, Node},
                                                 "GDSS application is not "
                                                 "running on this node.", NoOp)
            end,
    lists:foreach(
      fun(Node) ->
              [begin Name = client_monitor_name(Node, AppName),
                     Spec = {Name, {brick_clientmon, start_link,
                                    [Node, AppName, FunUp, FunDn]},
                             permanent, 2000, worker, [brick_clientmon]},
                     catch supervisor:start_child(brick_mon_sup, Spec)
               end || AppName <- client_mon_app_names()]
      end, Nodes).

%% @doc Create monitor name atom.

client_monitor_name(Node, AppName) ->
    list_to_atom("cl_mon_" ++ atom_to_list(Node) ++ "_" ++
                 atom_to_list(AppName)).

%% @doc Iteratively poll brick for status via brick_server:status().

poll_brick_status(Brick, Node, PollSeconds) ->
    poll_brick_status2(Brick, Node, (PollSeconds*1000) div 100,
                       last_not_available).

poll_brick_status2(Brick, Node, 0, LastErr) ->
    ?E_ERROR("~s:poll_brick_status2: ~p ~p -> ~p\n",
             [?MODULE, Brick, Node, LastErr]),
    {error, LastErr};
poll_brick_status2(Brick, Node, N, _LastErr) ->
    case (catch brick_server:status(Brick, Node, 100)) of
        {ok, _} = Res ->
            Res;
        {'EXIT', {timeout, _}} = Err ->
            poll_brick_status2(Brick, Node, N - 1, Err);
        Err ->
            timer:sleep(100),
            poll_brick_status2(Brick, Node, N - 1, Err)
    end.

%% @doc Deprecated function

do_rsync_brick_to_brick(Same, Same) ->
    {error, same_brick};
do_rsync_brick_to_brick({_SrcBrick, SameNode}, {_DstBrick, SameNode}) ->
    {error, same_node};
do_rsync_brick_to_brick({SrcBrick, SrcNode} = Src, {DstBrick, DstNode} =_Dst) ->
    RsyncOpts = "--delete --partial --bwlimit=KBytesPerSec",
    AllTables = brick_admin:get_tables(),
    AllBricks = lists:flatten(
                  [begin {ok, Chs} = brick_admin:get_table_chain_list(Tab),
                         [Brs || {_Chain, Brs} <- Chs]
                   end || Tab <- AllTables]),
    case lists:member(Src, AllBricks) of
        false ->
            {error, unknown_src_brick};
        true ->
            SrcHost = node_hostname(SrcNode),
            DstHost = node_hostname(DstNode),
            {ok, SrcDataDir} = remote_config_val(SrcNode, application_data_dir),
            {ok, DstDataDir} = remote_config_val(DstNode, application_data_dir),
            %% TODO: The brickname2() func needs to be moved elsewhere!
            SrcBrickDir = brick_ets:brick_name2data_dir(SrcBrick),
            DstBrickDir = brick_ets:brick_name2data_dir(DstBrick),
            io:format("\nRun this command on ~s:\n\n"
                      "  rsync -e ssh -aHvz ~s root@~s:~s/~s ~s\n",
                      [DstHost, RsyncOpts, SrcHost, SrcDataDir, SrcBrickDir,
                       DstDataDir]),
            io:format("\n** Repeat several times, until data copied and "
                      "runtime is minimal.\n"),
            io:format("\nThen run this command on ~s:\n"
                      "  mv ~s/~s ~s/~s.old\n",
                      [DstHost, DstDataDir, DstBrickDir,
                       DstDataDir, DstBrickDir]),
            io:format("\nRun this command on ~s:\n"
                      "  mv ~s/~s ~s/~s\n",
                      [DstHost, DstDataDir, SrcBrickDir,
                       DstDataDir, DstBrickDir]),
            io:format("\n"),
            ok
    end.

%% @doc Strip hostname from Erlang node name.

node_hostname(Node) when is_atom(Node) ->
    re:replace(atom_to_list(Node), ".*@", "", [{return, list}]).

%% @doc Get GMT config server configuration value from remote node.

remote_config_val(Node, Item) ->
    Nonsense = "This is simply & obviously not a valid response",
    case rpc:call(Node, gmt_config_svr, get_config_value, [Item, Nonsense]) of
        {ok, Nonsense} ->
            {error, not_found};
        Resp ->
            Resp
    end.

%% @doc Use "fast sync" to copy all brick data from bricks of DownNode
%%      to NewNode.

fast_sync_clone_node(DownNode, NewNode) ->
    fast_sync_clone_node(DownNode, NewNode, []).

%% @spec (atom(), atom(), proplist()) -> list(pid())
%% @doc Use "fast sync" to copy all brick data from bricks of DownNode
%%      to NewNode.
%%
%% See {@link fast_sync/4} for valid options and for a brief
%% description of the naming conventions used/expected.

fast_sync_clone_node(DownNode, NewNode, Opts) ->
    Bricks = [{Br, DownNode} ||
                 Br <- lists:sort(brick_simple:node_to_bricks(DownNode))],
    Map = [begin
               Tab = brick_simple:brick_to_table(Brick),
               Chain = brick_simple:brick_to_chain(Brick),
               {ok, GH} = brick_admin:get_gh_by_table(Tab),
               Tail = case brick_hash:chain2brick(Chain, read, GH, current) of
                          chain2brick_error ->
                              brick_hash:chain2brick(Chain, read, GH, new);
                          Res ->
                              Res
                      end,
               if Tail == chain_is_zero_length ->
                       io:format(
                         "ERROR: Table ~p chain ~p is currently length 0\n",
                         [Tab, Chain]),
                       exit(error);
                  true ->
                       ok
               end,
               {Brick, Tab, Chain, Tail}
           end || Brick <- Bricks],
    Opts2 = start_throttle_server_maybe(NewNode, Opts),
    [begin
         {ok, Pid} = fast_sync({DownBr, DownNode}, TailBrick, NewNode, Opts2),
         Pid
     end || {{DownBr, _}, _Tab, _Chain, TailBrick} <- Map].

%% @doc Perform a "fast sync" bulk data copy operation: based on the
%% top/common half of the scavenger, find all live keys for DownBrick in
%% TailBrick's replica and copy those keys to NewNode (into a brick called
%% DownBrick).
%%
%% Valid options:
%% <ul>
%% <li> {brick_options, PropList} ... additional brick init options,
%%      see brick_server and brick_ets.  All brick options currently in
%%      use by other bricks within the same chain are automatically
%%      retrieved and used, so this option will rarely be needed. </li>
%% <li> dry_run ... Do not send any data to NewNode, default = false. </li>
%% <li> {do_not_initiate_serial_ack, false} ... Do not disable the
%%      NewNode's transmission of {ch_serial_ack, ...} casts to its
%%      upstream brick.  The upstream isn't expecting acks from NewNode,
%%      so this option will rarely be needed. </li>
%% <li> stop_new_brick ... Stop the new brick on NewNode when the data
%%      copy is finished, default = false. </li>
%% <li> {throttle_bytes, integer()} ... Bytes/second limit for data from
%%      all sending bricks combined. </li>
%% <li> {throttle_pid, pid() | atom()} ... Pid or server spec for a
%%      previously started `brick_ticket' server. </li>
%% <li> {work_dir, Path} ... Directory to store scavenger's top half temp
%%      and working files, Path must exist, default = "/tmp". </li>
%% </ul>

fast_sync({DownBrick, DownNode}, {TailBrick, TailNode}, NewNode) ->
    fast_sync({DownBrick, DownNode}, {TailBrick, TailNode}, NewNode, []).

fast_sync({DownBrick, DownNode}, {TailBrick, TailNode}, NewNode, Opts) ->
    case net_adm:ping(DownNode) of
        pang -> io:format("\n\nNode ~p is down\n", [DownNode]);
        pong -> io:format("\n\nWARNING: Node ~p is running.\n", [DownNode]),
                timer:sleep(3000)
    end,
    pong = net_adm:ping(NewNode),

    Opts2 = start_throttle_server_maybe(NewNode, Opts),

    %% TODO: robustify startup
    BrOpts =
        proplists:get_value(brick_options, Opts, []) ++
        [do_not_initiate_serial_ack, {do_sync, false}] ++
        begin
            %% Note: cut-and-paste from gmt_hlog_common.
            {ok, Ps} = brick_server:status(TailBrick, TailNode),
            Is = proplists:get_value(implementation, Ps),
            proplists:get_value(options, Is)
        end,
    {ok, _} = brick_shepherd:start_brick(DownBrick, NewNode, BrOpts),
    %% TODO: robustify.
    {ok, _PollRes} = poll_brick_status(DownBrick, NewNode, 80),
    ok = brick_server:chain_set_my_repair_state(DownBrick, NewNode, ok),
    ok = brick_server:chain_role_tail(DownBrick, NewNode, TailBrick, TailNode,
                                      [{official_tail, false}]),
    rpc:call(TailNode, brick_admin, fast_sync_scav,
             [node(), TailBrick, TailNode, DownBrick, NewNode, Opts2]).

%% @spec (atom(), proplist()) -> proplist()
%% @doc Start a throttle server if one isn't already present in the Opts
%%      proplist.

start_throttle_server_maybe(Node, Opts) ->
    case proplists:get_value(throttle_pid, Opts) of
        undefined ->
            {ThrottleBytes, ThrottlePid} = start_throttle_server(Node, Opts),
            [{throttle_pid, ThrottlePid},
             {throttle_bytes,ThrottleBytes}|Opts];
        _ ->
            Opts
    end.

%% @doc Start throttle server under the brick_admin_sup supervisor.

start_throttle_server(Node, Opts) ->
    BwThrottle = proplists:get_value(throttle_bytes, Opts, 999*1000*1000),
    Hour = 1*60*60*1000,
    Spec = {now(), {brick_ticket, start_link,
                    [undefined, BwThrottle, 1000, Hour]},
            temporary, 2000, worker, [brick_ticket]},
    {ok, ThrottlePid} =
        supervisor:start_child({brick_admin_sup, Node}, Spec),
    {BwThrottle, ThrottlePid}.

%% @doc Perform a "fast sync" operation, with this func running on the remote
%% replica, returning {ok, Pid} of the worker proc that will do the actual
%% data read &amp; cast to NewNode.

fast_sync_scav(StartNode, UpBrick, UpNode, NewBrick, NewNode, Opts) ->
    CommonLogSvr = gmt_hlog_common:hlog_pid(?GMT_HLOG_COMMON_LOG_NAME),
    {ok, CurSeq} = gmt_hlog:advance_seqnum(CommonLogSvr, 1),
    ThrottlePid = proplists:get_value(throttle_pid, Opts),
    ThrottleBytes = proplists:get_value(throttle_bytes, Opts),
    StopNewBrick_p = proplists:get_value(stop_new_brick, Opts, false),
    Destructive_p = not proplists:get_value(dry_run, Opts, false),

    SA = #scav{options = [{new_brick, NewBrick}, {new_node, NewNode},
                          {up_brick, UpBrick}, {up_node, UpNode}|Opts],
               work_dir = proplists:get_value(work_dir, Opts, "/tmp") ++
               "/fast_sync." ++ atom_to_list(NewBrick),
               wal_mod = gmt_hlog_common,
               name = {fast_sync, UpBrick},
               log = gmt_hlog:log_name2reg_name(?GMT_HLOG_COMMON_LOG_NAME),
               log_dir = gmt_hlog:log_name2data_dir(?GMT_HLOG_COMMON_LOG_NAME),
               %% For common log, there is only one seqnum that's
               %% off-limits: the current one, CurSeq.
               last_check_seq = CurSeq,
               destructive = Destructive_p,
               skip_live_percentage_greater_than = 100, % no skipping
               sorter_size = 16*1024*1024,

               %% The #scav.bricks list is what will limit the first/common
               %% part of the scavenger's code to look only at the brick
               %% we're interested in.
               bricks = [UpBrick],

               throttle_pid = ThrottlePid,
               exclusive_p = false,
               log_fun = fun(Fmt, Args) -> ?APPLOG_INFO(?APPLOG_APPM_105,Fmt, Args) end,
               phase10_fun = fun fast_sync_bottom10/7},
    Fdoit = fun() ->
                    [rpc:call(Nd, error_logger, info_msg,
                              ["Fast sync by ~p starting for ~p ~p -> ~p ~p\n",
                               [self(), UpBrick, UpNode, NewBrick, NewNode]]) ||
                        Nd <- lists:usort([StartNode, UpNode, NewNode])],
                    put(throttle_bytes_hack, ThrottleBytes),
                    gmt_hlog_common:scavenger_commonlog(SA),
                    [rpc:call(Nd, error_logger, info_msg,
                              ["Fast sync by ~p finished for ~p ~p -> ~p ~p\n",
                               [self(), UpBrick, UpNode, NewBrick, NewNode]]) ||
                        Nd <- lists:usort([StartNode, UpNode, NewNode])],
                    if StopNewBrick_p ->
                            brick_shepherd:stop_brick(NewBrick, NewNode);
                       true ->
                            ok
                    end,
                    exit(normal)
            end,
    Pid = spawn(Fdoit),
    {ok, Pid}.

%% @doc This is the "bottom half" of the scavenger, but instead copying data
%% from one brick (UpBrick) to another (NewBrick).
%%
%% TmpList4 is a list of {SeqNum, Bytes} which represents that contains at
%% least one value blob that we need to fast-sync.  The top of the scavenger
%% has created a disk_log-formatted file that gives us the info we need to
%% find each key and copy it to the new node.  We use lists:foldl/3 to
%% process this list of tuples, and in turn the corresponding disk_log file
%% will be processed by brick_ets:disk_log_fold/3.

fast_sync_bottom10(SA, Finfolog, TempDir, TmpList4, _Bytes1,
                   _Del1, _BytesBefore) ->
    UpBrick = proplists:get_value(up_brick, SA#scav.options),
    UpNode = proplists:get_value(up_node, SA#scav.options),
    NewBrick = proplists:get_value(new_brick, SA#scav.options),
    NewNode = proplists:get_value(new_node, SA#scav.options),

    Fsync_a_log_seq_file = fast_sync_one_seq_file_fun(
                             TempDir, SA, unused, Finfolog,
                             UpBrick, UpNode, NewBrick, NewNode, SA),
    {Hunks, Bytes, Errs} = lists:foldl(Fsync_a_log_seq_file,
                                       {0, 0, 0}, TmpList4),
    OptsNoDead = lists:filter(fun({dead_paths, _}) -> false;
                                 (_)               -> true
                              end, SA#scav.options),
    Finfolog("Fast sync finished:\n"
             "\tOptions: ~p\n"
             "\tCopied: hunks bytes errs = ~p ~p ~p\n",
             [OptsNoDead, Hunks, Bytes, Errs]),
    os:cmd("rm -rf " ++ TempDir),
    normal.

%% @doc Set up the disk_log machinery to iterate over it via
%% brick_ets:disk_log_fold/3.

fast_sync_one_seq_file_fun(TempDir, SA, ____Fread_blob, Finfolog,
                           UpBrick, UpNode, NewBrick, NewNode, SA) ->
    fun({SeqNum, Bytes}, {Hs, Bs, Es}) ->
            DInPath = TempDir ++ "/" ++ integer_to_list(SeqNum),
            {ok, DInLog} = disk_log:open([{name, DInPath},
                                          {file, DInPath}, {mode,read_only}]),
            {ok, FH} = gmt_hlog_common:open_log_file(SA#scav.log_dir, SeqNum,
                                                     [read, binary]),
            Fread_and_send =
                read_hunk_send_replay_fun(UpBrick, UpNode, NewBrick, NewNode,
                                          SA#scav.destructive, FH),
            %% Actual read & cast to new node is done here.
            {Hunks, Bytes, Errs, _, _} =
                brick_ets:disk_log_fold(
                  Fread_and_send, {0, 0, 0, SA#scav.throttle_pid, 0}, DInLog),
            file:close(FH),
            disk_log:close(DInLog),
            Finfolog("fast sync: Finished sequence ~p\n", [SeqNum]),
            {Hs + Hunks, Bs + Bytes, Es + Errs}
    end.

%% @doc Make closure to encapsulate Up* and New* parameters.

read_hunk_send_replay_fun(UpBrick, UpNode, NewBrick, NewNode, Send_p, FH) ->
    fun({Offset, _BrickName, Key, TS, ValLen, ExpTime, Flags},
        {Hs1, Bs1, Es1, ThrottlePid, ThrottleBs}) ->
            case gmt_hlog:read_hunk_summary(FH, seqnum_unused, Offset,
                                            ValLen, fun read_a_hunk/2) of
                H when is_record(H, hunk_summ) ->
                    Val = hd(H#hunk_summ.c_blobs),
                    if Send_p ->
                            send_log_replay(Key, TS, ExpTime, Val, Flags,
                                            UpBrick, UpNode, NewBrick, NewNode);
                       true ->
                            ok
                    end,
                    NewBs1 = Bs1 + size(Val),
                    BatchBsHack = get(throttle_bytes_hack),
                    ThrBs = case ThrottleBs + size(Val) of
                                N when N < BatchBsHack ->
                                    N;
                                N ->
                                    brick_ticket:get(ThrottlePid, N),
                                    0
                            end,
                    {Hs1 + 1, NewBs1, Es1, ThrottlePid, ThrBs};
                _Err ->
                    {Hs1 + 1, Bs1, Es1 + 1, ThrottlePid, ThrottleBs}
            end;
       ({live_bytes, _}, Acc) ->
            Acc
    end.

%% @doc Read a hunk and fetch its value blob.
%%
%% NOTE: This is an nearly-verbatim copy of a brick_ets (?) function.

read_a_hunk(Su, FH) ->
    ?LOGTYPE_BLOB = Su#hunk_summ.type,
    [_Bytes] = Su#hunk_summ.c_len,
    %% throttle_get(SA#scav.throttle_pid, _Bytes),
    Bin = gmt_hlog:read_hunk_member_ll(FH, Su, md5, 1),
    Su2 = Su#hunk_summ{c_blobs = [Bin]},
    true = gmt_hlog:md5_checksum_ok_p(Su2),
    Su2.

%% @doc Send a chain replication {ch_log_reply, ...} tuple to NewBrick.
%%
%% NOTE: This function has cross-module encapsulation violations.

send_log_replay(Key, TS, Exp, Val, Flags, UpBrick, UpNode, NewBrick, NewNode) ->
    ST = brick_ets:storetuple_make(Key, TS, Val, size(Val), Exp, Flags),
    Serial = brick_server:make_timestamp(), % Yeah it's cheating, it works
    gen_server:cast({NewBrick, NewNode},
                    {ch_log_replay_v2, {UpBrick, UpNode}, Serial, [{insert, ST}],
                     <<>>, <<>>, lastserial_unused}).

%%
%% Schema upgrade items
%%

-spec upgrade_20090320(list(atom() | list())) -> no_return().
upgrade_20090320([A]) when is_atom(A) ->
    case catch rpc:call(A, ?MODULE, fix_gh_20090320, ["Schema.local"]) of
        ok ->
            io:format("Upgrade successful.\n"),
            timer:sleep(250),
            erlang:halt(0);
        Err ->
            io:format("Upgrade error: ~p\n", [Err]),
            timer:sleep(250),
            erlang:halt(1)
    end;
upgrade_20090320([L]) when is_list(L) ->
    upgrade_20090320([list_to_atom(L)]).

%% @doc Example of GH schema upgrade function.

fix_gh_20090320(File) ->
    BrickList = get_bricklist_from_disk(File),
    {ok, TS, Schema} = squorum_get(BrickList, ?BKEY_SCHEMA_DEFINITION),
    Tables = [Tab || {Tab, _} <- dict:to_list(Schema#schema_r.tabdefs)],
    Schema2 = lists:foldl(
                fun(Tab, Sch) ->
                        {ok, T} = dict:find(Tab, Sch#schema_r.tabdefs),
                        GH = T#table_r.ghash,
                        CurLH2 = fix_lh_20090320(GH#g_hash_r.current_h_desc),
                        NewLH2 = fix_lh_20090320(GH#g_hash_r.new_h_desc),
                        GH2 = GH#g_hash_r{current_h_desc = CurLH2,
                                          new_h_desc     = NewLH2},
                        update_tab_ghash(Tab, T, GH2, Sch)
                end, Schema, Tables),
    ok = brick_squorum:set(BrickList, term_to_binary(?BKEY_SCHEMA_DEFINITION),
                           term_to_binary(Schema2), 0, [{testset, TS}], 5000).

%% @doc Example of LH schema upgrade function.

fix_lh_20090320(#hash_r{opaque = Opaque} = LH)
  when element(1, Opaque) == var_prefix,
       size(Opaque) == 4 ->
    %% num_separators added @ element 5, default = 2
    Opaque2 = list_to_tuple(tuple_to_list(Opaque) ++ [2]),
    io:format("Upgrade var_prefix\n"),
    LH#hash_r{opaque = Opaque2};
fix_lh_20090320(#hash_r{opaque = Opaque} = LH)
  when element(1, Opaque) == chash,
       size(Opaque) == 12 ->
    %% num_separators added @ element 6, default = 2
    L = tuple_to_list(Opaque),
    Opaque2 = list_to_tuple(lists:sublist(L, 1, 5) ++ [2] ++
                            lists:sublist(L, 6, 12)),
    io:format("Upgrade chash\n"),
    LH#hash_r{opaque = Opaque2};
fix_lh_20090320(LH) ->
    io:format("Pass-through\n"),
    LH.

%% @type bigdata_option()    = {'bigdata_dir', string()}.
%% @type brick()             = {logical_brick(), node()}.
%% @type brick_option()      = chash_prop() |
%%                             custom_prop() |
%%                             fixed_prefix_prop() |
%%                             {'hash_init', fun_of_arity_3()} |
%%                             var_prefix_prop().
%% @type brick_options()     = [brick_option].
%% @type chain_list()        = {chain_name(), [brick()]}.
%% @type chain_name()        = atom().
%% @type chash_prop()        = {'new_chainweights', chain_weights()} |
%%                             {'num_separators', integer()} |
%%                             {'old_float_map', float_map()} |
%%                             {'prefix_is_integer_hack', boolean()} |
%%                             {'prefix_length', integer()} |
%%                             {'prefix_method', 'all' | 'var_prefix' | 'fixed_prefix'} |
%%                             {'prefix_separator', integer()}.
%% @type chain_weights()     = [{chain_name, integer()}].
%% @type custom_prop()       = proplists_property().
%% @type fixed_prefix_prop() = {'prefix_is_integer_hack', boolean()} |
%%                             {'prefix_length', integer()}.
%% @type logging_option()    = {'do_logging', boolean()}.
%% @type logical_brick()     = atom().
%% @type node()              = atom().
%% @type sync_option()       = {'do_sync', boolean()}.
%% @type table()             = atom().
%% @type var_prefix_prop()   = {'num_separators', integer()} |
%%                             {'prefix_separator', integer()}.
