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
%%% File    : brick_hash.erl
%%% Purpose : brick hashing upper layer.
%%%-------------------------------------------------------------------

%% @doc An upper (?) layer for distributed storage hashing.
%%
%% I've attempted to split my grand vision for distributed storage into
%% two different layers:
%% <ul>
%% <li> A lower storage level of an individual brick.  Each brick should
%%   be completely unaware of other bricks, to the full extent possible. </li>
%% <li> An upper storage level that uses hashing techniques to determine
%%   which keys are stored on any particular storage brick. </li>
%% </ul>
%%
%% This upper layer has two mostly-separate parts:
%% <ul>
%% <li> A mapping from key -&gt; chain name. </li>
%% <li> A mapping from chain name -&gt; individual bricks. </li>
%% </ul>
%%
%% == What is a #hash_r record? ==
%%
%% A `#hash_r' record is used for two things:
%% <ul>
%% <li> The algorithm used to choose what part of the key will be used
%% for hashing: e.g. fixed-length prefix, variable length prefix  </li>
%% <li> The algorithm for the actual hashing: e.g. naive.  </li>
%% </ul>
%%
%% NOTE: Those two tasks should be quite separate and, at the moment,
%% are blurred together in (sometimes) bad ways.  TODO: refactor!
%%
%% == Note #hash_r init functions ==
%%
%% The #hash_r record initialization functions, e.g. naive_init() and
%% var_prefix_init(), must provide a couple of well-known arities to
%% external users:
%% <ul>
%% <li> (ChainList) ... specifying only the chains' definition.  All
%%      other options, if any, are set to default. </li>
%% <li> (via_proplist, ChainList, PropList) ... The proplist method is
%%      used (at first) by brick_admin:add_table().  Such (ab)use of
%%      proplists is a common technique for presenting a fixed-arity
%%      API while allowing extremely flexibility in implementing same. </li>
%% </ul>
%%
%% == What is a #g_hash_r record? ==
%%
%% A `#g_hash_r' record contains "global" hashing info for a
%% <b>single GDSS table</b>.  (Perhaps "global" is a bad word?  {shrug})
%%
%% The `#g_hash_r' is a collection of two `#hash_r' records, plus some
%% extra stuff.  The two `#hash_r' records describe:
%% <ul>
%% <li> The hashing scheme used in the current hash configuration. </li>
%% <li> The hashing scheme used in the <b>new</b> hash configuration. </li>
%% </ul>
%%
%% Most of the time, the current and new hash configurations are the
%% same.  However, if the cluster configuration changes, then the
%% difference betwen current &amp; new hashes are used to "migrate"
%% data from the current scheme to the new scheme.
%%
%% The `#g_hash_r' is data that is distributed to all the
%% `brick_simple' gen_servers (one per client node), to assist with
%% `brick_simple' calculation of {table + key} =&gt; brick mapping.
%% The `#g_hash_r' is also distributed to all bricks, so that they can
%% independently verify that that brick is truly responsible for the
%% keys sent to it.  If the client's `#g_hash_r' info is stale, that's
%% OK: the incorrect brick will forward the request to the correct
%% brick.
%%
%% == Note on #g_hash_r.phase State Names ==
%%
%% The two valid state names for #g_hash_r.phase are:
%% <ul>
%% <li> pre ... the brick is prepared to sweep itself and to receive sweep
%%      updates from other bricks, but should initiate no migration
%%      action.</li>
%% <li> migrating ... The brick is actively migrating.  It can enter this
%%      state (from 'pre') by:
%%   <ul>
%%   <li> Being explicitly told to start sweeping itself. </li>
%%   <li> Receiving a sweep update from another brick B, because
%%        that brick got the "start sweeping" command before we did. </li>
%%   </ul>
%% </li>
%% </ul>

%% Bugs found with Quickcheck:
%% * discovered URI parsing prefix set to $_ (ASCII 95) instead of
%%   expected $/ (ASCII 47), after configuring tab1 to use 2 chains instead
%%   of the default 1 chain.

-module(brick_hash).
-include("applog.hrl").


-include("brick.hrl").
-include("brick_hash.hrl").
-include("brick_public.hrl").

-ifdef(debug_hash).
-define(gmt_debug, true).
-endif.
-include("gmt_debug.hrl").

-define(PHASH2_BIGGEST_VAL,              134217727). % (2^27)-1
-define(SMALLEST_SIGNIFICANT_FLOAT_SIZE, 0.1e-12).

%% This is one level of hash management:
%% mapping from key -> chain name.

%% Exports for first hash method, 'naive'.
-export([naive_init/1, naive_init/2, naive_key_to_chain/3]).

%% Exports for second hash method, 'var_prefix'.
%% It hashes the same way as 'naive', but only operates on a
%% variable-length prefix string.
-export([var_prefix_init/1, var_prefix_init/2, var_prefix_init/3,
         var_prefix_key_to_chain/3]).

%% Exports another hash method, 'fixed_prefix_numhack'.
%% It hashes similarly to 'var_prefix', but only operates on a
%% uses a constant-length prefix *and*
-export([fixed_prefix_init/1, fixed_prefix_init/3,
         fixed_prefix_key_to_chain/3]).

%% API for consistent hashing.  We'll do what we should've done
%% with previous efforts: separate:
%%    * mapping of key -> raw hash val
%%    * mapping of raw hash val -> chain name or brick name.
-export([chash_init/3, chash_key_to_chain/3]).
-export([chash_make_float_map/1, chash_make_float_map/2,
         chash_float_map_to_nextfloat_list/1,
         chash_nextfloat_list_to_gb_tree/1, chash_gb_next/2,
         chash_scale_to_int_interval/2,         % debugging func
         chash_extract_new_float_map/1, chash_extract_old_float_map/1,
         chash_extract_new_chainweights/1, chash_extract_old_props/1]).

%% This is the other level of hash management:
%% mapping from chain names -> individual bricks.
%%
%% This layer does *not* make any key -> chain name mapping decisions.

-export([add_migr_dict_if_missing/1,
         init_global_hash_state/7, init_global_hash_state/8,
         key_to_brick/3,
         key_to_chain/2,                        % Not for general use
         chain2brick/3, chain2brick/4
        ]).

%% API for miscellaneous/helper functions.

-export([set_chain_sweep_key/3,
         all_chains/2,
         update_chain_dicts/3, desc_substitute_chain/3,
         verify_chain_list/2
        ]).

%% Useful mostly for debugging: invent a list of chains that will all
%% live on the same node.
-export([invent_nodelist/2, invent_nodelist/4]).


%% Chain descriptor
-record(chaindesc_r, {
          name,
          length,
          head,
          middles,
          tail
         }).

%% Opaque record for naive method.
%% TODO? Move naive functions & records into separate module?
-record(naive, {
          num_chains,
          map
         }).

%% Opaque record for the variable-length prefix method.
-record(var_prefix, {
          num_chains,
          map,
          separator,
          num_separators
         }).

%% Opaque record for the fixed-length prefix method.
-record(fixed_prefix, {
          num_chains,
          map,
          prefix_len,
          prefix_is_integer_hack_p
         }).

%% Opaque record for consistent hashing method.
-record(chash, {
          num_chains,
          map,
          %% Key prefix mash-up
          prefix_method,                        % all | var_prefix | fixed_prefix,
          separator,                            % for var prefix
          num_separators,                       % for var prefix
          prefix_len,                           % for fixed-length prefix
          prefix_is_integer_hack_p,             % for fixed-length prefix
          %% chash guts
          chash_map,                            % tuple(ChainName)
          old_float_map,                        % float_map()
          new_float_map,                        % float_map()
          new_chainweights,                     % chainweight_list()
          %% administrivia
          old_props                             % proplist()
         }).

-export_type([chash_r/0, fixed_prefix_r/0, naive_r/0, var_prefix_r/0]).

%% declare types for easy ref showing _r as record
-type naive_r() :: #naive{}.
-type var_prefix_r() :: #var_prefix{}.
-type fixed_prefix_r() :: #fixed_prefix{}.
-type chash_r() :: #chash{}.

%% QQQ Re-write call sig!
-type brick() :: {atom(),atom()}.
-type bricklist() :: [brick()].
-type chainlist() ::  [{atom(),bricklist()}].
-type phase() ::  pre | migrating | post | phase_unused | unused.
-type chain_error() :: chain2brick_error | chain_is_zero_length.

-spec add_migr_dict_if_missing(#g_hash_r{}) -> #g_hash_r{}.
-spec all_chains(#g_hash_r{}, current | new ) -> [{any(),any()}].
-spec chain2brick(atom(),read|write,#g_hash_r{}) -> chain_error() |brick().
-spec chain2brick(atom(),read|write,#g_hash_r{},current|new) -> chain_error()|brick().
-spec chash_extract_old_props(#hash_r{}|#g_hash_r{}) -> [{any(),any()}].
-spec chash_init(via_proplist,[{atom(),[atom()]}],[{any(),any()}]) -> #hash_r{}.
-spec desc_substitute_chain(#hash_r{},atom(),[atom()]) -> #hash_r{}.
-spec doo_iter_chain([{any(),float()}],integer(),integer()) -> ok.
-spec fixed_prefix_init(chainlist()) -> #hash_r{}.
-spec fixed_prefix_init(via_proplist,chainlist(),[{any(),any()}]) -> #hash_r{}.
-spec init_global_hash_state(atom(), phase(), integer(),#hash_r{},chainlist(),#hash_r{},chainlist()) -> #g_hash_r{}.
-spec init_global_hash_state(atom(), phase(), integer(),
              #hash_r{}, chainlist(),#hash_r{},chainlist(),true|false) -> #g_hash_r{}.
-spec key_to_brick(read|write,term(),#g_hash_r{}) -> chain_error()| brick().
-spec key_to_chain(term(),#g_hash_r{}) -> atom().
-spec naive_init(chainlist(),true|false) -> #hash_r{}.
-spec set_chain_sweep_key(atom(),term(),#g_hash_r{}) -> #g_hash_r{}.
-spec update_chain_dicts(#g_hash_r{},atom(),[any()]) -> #g_hash_r{}.
-spec verify_chain_list(chainlist()) -> ok.
-spec verify_chain_list(chainlist(),true|false) -> ok.

init_global_hash_state(MigratingP, Phase, CurrentRev,
                       CurrentHashDesc, CurrentChainList,
                       NewHashDesc, NewChainList) ->
    init_global_hash_state(MigratingP, Phase, CurrentRev,
                           CurrentHashDesc, CurrentChainList,
                           NewHashDesc, NewChainList, false).

init_global_hash_state(MigratingP, Phase, CurrentRev,
                       CurrentHashDesc, CurrentChainList,
                       NewHashDesc, NewChainList, ZeroLengthOK)
  when is_integer(CurrentRev), CurrentRev > 0,
       is_record(CurrentHashDesc, hash_r),
       is_record(NewHashDesc, hash_r) ->
    ok = verify_chain_list(CurrentChainList, ZeroLengthOK),
    ok = verify_chain_list(NewChainList, ZeroLengthOK),
    #g_hash_r{migrating_p = MigratingP,
              phase = if Phase == pre; Phase == migrating; Phase == post ->
                              Phase;
                         true ->
                              pre
                      end,
              current_rev = CurrentRev,
              current_h_desc = CurrentHashDesc,
              new_h_desc = NewHashDesc,
              current_chain_dict = chain_list2dict(CurrentChainList),
              new_chain_dict = chain_list2dict(NewChainList),
              migr_dict = if MigratingP -> dict:new();
                             true       -> undefined end
             }.

add_migr_dict_if_missing(GH) when is_record(GH, g_hash_r) ->
    if GH#g_hash_r.migr_dict == undefined ->
            GH#g_hash_r{migr_dict = dict:new()};
       true ->
            GH
    end.

%% @spec (read | write, term(), g_hash_r()) -> {brick(), node()} | error | exit

key_to_brick(ReadOrWrite, Key, GH) ->
    key_to_chainsweep(ReadOrWrite, Key, GH, fun chain2brick/4).

key_to_chainsweep(ReadOrWrite, Key, GH, FinalFun)
  when ReadOrWrite == read; ReadOrWrite == write ->
    #g_hash_r{current_rev = CurRev, minor_rev = MinorRev, current_h_desc = H} =
        GH,
    ChainName = (H#hash_r.mod):(H#hash_r.func)(
                  ReadOrWrite, Key, H#hash_r.opaque),
    if GH#g_hash_r.migrating_p == false orelse
       (GH#g_hash_r.migrating_p == true andalso GH#g_hash_r.phase == pre) ->
            B = FinalFun(ChainName, ReadOrWrite, GH, current),
            Debs = [{migrating_p, GH#g_hash_r.migrating_p},
                    {phase, GH#g_hash_r.phase}],
            ?DBG_HASHx({ChainName, Key, no_mig, CurRev, MinorRev, B, Debs}),
            B;
       true ->
            HNew = GH#g_hash_r.new_h_desc,
            ChainNameNew = (HNew#hash_r.mod):(HNew#hash_r.func)(
                             ReadOrWrite, Key, HNew#hash_r.opaque),
            if ChainName == ChainNameNew ->
                    B = FinalFun(ChainName, ReadOrWrite, GH, current),
                    ?DBG_HASHx({Key, mig_same, CurRev, MinorRev, B}),
                    B;
               true ->
                    case where_is_key_relative_to_sweep(Key, ChainName, GH) of
                        in_front ->
                            B = FinalFun(ChainName, ReadOrWrite, GH, current),
                            ?DBG_HASHx({Key, mig_current, CurRev, MinorRev, B}),
                            B;
                        behind ->
                            B = FinalFun(ChainNameNew, ReadOrWrite, GH, new),
                            ?DBG_HASHx({Key, mig_new, CurRev, MinorRev, B}),
                            B
                    end
            end
    end.

%% @spec (term(), g_hash_r()) -> atom() | error | exit

key_to_chain(Key, GH) ->
    key_to_chainsweep(read, Key, GH,
                      fun (ChainName, _, _, _) -> ChainName end).

chain2brick(ChainName, ReadOrWrite, GH) ->
    chain2brick(ChainName, ReadOrWrite, GH, current).

chain2brick(ChainName, ReadOrWrite, GH, WhichDict) ->
    Dict = if WhichDict == current -> GH#g_hash_r.current_chain_dict;
              WhichDict == new     -> GH#g_hash_r.new_chain_dict
           end,
    case dict:find(ChainName, Dict) of
        {ok, CH} ->
            %% TODO: What to do if CH#chaindesc_r.length == 0?
            if ReadOrWrite == read -> CH#chaindesc_r.tail;
               true                -> CH#chaindesc_r.head
            end;
        _ ->
            chain2brick_error
    end.

%% @spec (atom(), term(), g_hash_r()) -> g_hash_r()
%% @doc Store the new migration sweep key Key for chain ChainName in the
%%      global hash's migration dictionary.

set_chain_sweep_key(ChainName, Key, GH) when is_record(GH, g_hash_r) ->
    NewD = dict:store(ChainName, Key, GH#g_hash_r.migr_dict),
    GH#g_hash_r{migr_dict = NewD};
set_chain_sweep_key(_ChainName, _Key, GH) ->
    %% Just return GH as-is.  Odds are high that GH = undefined.
    %% If GH = undefined, we're in a pathological situation where we
    %% don't know what the global hash is.  If that's true, we definitely
    %% cannot update it, but crashing here isn't a good idea either, so
    %% we will just pass it through.
    GH.

%% @spec (g_hash_r(), current | new) -> list({atom(), list()})
%% @doc Return the list of all {chain name, brick list} (active bricks only!)
%%      for the current/new hash descriptor in a global hash record.

all_chains(GH, WhichDescriptor) ->
    Dict = if WhichDescriptor == current -> GH#g_hash_r.current_chain_dict;
              WhichDescriptor == new     -> GH#g_hash_r.new_chain_dict
           end,
    [{ChainName,
      if D#chaindesc_r.head == D#chaindesc_r.tail ->
              [D#chaindesc_r.head];
         true ->
              [D#chaindesc_r.head] ++ D#chaindesc_r.middles ++
                  [D#chaindesc_r.tail]
      end}
     || {ChainName, D} <- dict:to_list(Dict)].

update_chain_dicts(GH, ChainName, BrickList) ->
    Desc = make_chaindesc(ChainName, BrickList),
    NewCur = case dict:find(ChainName, GH#g_hash_r.current_chain_dict) of
                 {ok, _D1} ->
                     dict:store(ChainName,Desc,GH#g_hash_r.current_chain_dict);
                 _ ->
                     GH#g_hash_r.current_chain_dict
             end,
    NewNew = case dict:find(ChainName, GH#g_hash_r.new_chain_dict) of
                 {ok, _D2} ->
                     dict:store(ChainName,Desc,GH#g_hash_r.new_chain_dict);
                 _ ->
                     GH#g_hash_r.new_chain_dict
             end,
    MinorRev = GH#g_hash_r.minor_rev + 1,
    GH#g_hash_r{minor_rev = MinorRev,
                current_chain_dict = NewCur, new_chain_dict = NewNew}.

desc_substitute_chain(Desc, ChainName, BrickList) ->
    case proplists:get_value(ChainName, Desc#hash_r.healthy_chainlist) of
        undefined ->
            Desc;
        _ ->
            NewCL = lists:keyreplace(
                      ChainName, 1, Desc#hash_r.healthy_chainlist,
                      {ChainName, BrickList}),
            if Desc#hash_r.method == naive ->
                    naive_init(NewCL);
               Desc#hash_r.method == var_prefix ->
                    Separator = (Desc#hash_r.opaque)#var_prefix.separator,
                    NumSeps = (Desc#hash_r.opaque)#var_prefix.num_separators,
                    var_prefix_init(NewCL, false, Separator, NumSeps);
               Desc#hash_r.method == fixed_prefix ->
                    Len = (Desc#hash_r.opaque)#fixed_prefix.prefix_len,
                    HackP = (Desc#hash_r.opaque)#fixed_prefix.prefix_is_integer_hack_p,
                    fixed_prefix_init(via_proplist, NewCL,
                                      [{prefix_length, Len},
                                       {prefix_is_integer_hack, HackP}]);
               Desc#hash_r.method == chash ->
                    Props = (Desc#hash_r.opaque)#chash.old_props,
                    chash_init(via_proplist, NewCL, Props)
            end
    end.

where_is_key_relative_to_sweep(Key, ChainName, GH) ->
    case dict:find(ChainName, GH#g_hash_r.migr_dict) of
        {ok, SweepKey} ->
            ?DBG_HASHx({relative_to_sweep, Key, ChainName, found, SweepKey}),
            if SweepKey == ?BRICK__GET_MANY_LAST  -> behind;
               SweepKey == ?BRICK__GET_MANY_FIRST -> in_front;
               Key =< SweepKey                    -> behind;
               true                               -> in_front
            end;
        _ ->
            ?DBG_HASHx({relative_to_sweep, Key, ChainName, not_found}),
            in_front
    end.

%% QQQ Hrm, I'm thinking that it's going to be difficult to standardize the
%% API for a specific hash algorithm's init() func?

%% @spec (list({brick_name(), list(node_name())})) -> hash_r()

naive_init(ChainList) ->
    naive_init(ChainList, false).

naive_init(ChainList, ZeroLengthOK) ->
    ok = verify_chain_list(ChainList, ZeroLengthOK),
    Opaque = #naive{num_chains = length(ChainList),
                    map = list_to_tuple(ChainList)},
    #hash_r{method = naive,
            mod = ?MODULE,
            %%REAL VALUE: func = naive_key_to_chain,
            %%DEBUG VALUE: func = naive_key_to_chain_DEBUG,             %DEBUGGING ONLY!!!
            func = naive_key_to_chain,
            healthy_chainlist = ChainList,
            opaque = Opaque}.

naive_key_to_chain(_ReadOrWrite, Key, Opaque) ->
    N = naive_hash(Key),
    ChainI = (N rem Opaque#naive.num_chains) + 1,
    {ChainName, _ChainMembers} = element(ChainI, Opaque#naive.map),
    ChainName.

naive_hash(Key) ->
    erlang:phash2(Key).

naive_key_to_chain_DEBUG(_ReadOrWrite, Key, Opaque) ->
    IgnoreBytes = size(Key) - 1,
    <<_:IgnoreBytes/binary, Last:8>> = Key,
    N = Last - $0,
    ChainI = (N rem Opaque#naive.num_chains) + 1,
    {ChainName, _ChainMembers} = element(ChainI, Opaque#naive.map),
    ChainName.

%% @spec (list({brick_name(), list(node_name())})) -> hash_r()

var_prefix_init(ChainList) ->
    var_prefix_init(ChainList, false).

var_prefix_init(ChainList, ZeroLengthOK) ->
    var_prefix_init(ChainList, ZeroLengthOK, ?HASH_PREFIX_SEPARATOR, 1).

%% @spec (via_proplist, ChainList::chain_list(), Props::prop_list()) -> hash_r()
%% @doc Return a #hash_r record for a variable-length prefix hashing scheme,
%%      throwing an exception if sanity checks fail.
%%
%% When using the via_proplist version of this function, the following
%% properties are examined:
%% <ul>
%% <li> zero_length_ok ... should never be true when defining a
%%      new table. </li>
%% <li> prefix_separator ... the single byte ASCII value of the byte
%%      that separates the key's prefix from the rest of the key.
%%      Default is ?HASH_PREFIX_SEPARATOR. </li>
%% <li> num_separators ... number of prefix_separator components that are
%%      included in the key's prefix.
%%      Default is 2 (assuming key looks like "/prefix/not-prefix-stuff").</li>
%% </ul>

var_prefix_init(via_proplist, ChainList, Props) ->
    ZLok = proplists:get_value(zero_length_ok, Props, false),
    Sep = proplists:get_value(prefix_separator, Props, ?HASH_PREFIX_SEPARATOR),
    NumSeps = proplists:get_value(num_separators, Props, 2),
    var_prefix_init(ChainList, ZLok, Sep, NumSeps).

var_prefix_init(ChainList, ZeroLengthOK, PrefixSeparator, NumSeps) ->
    ok = verify_chain_list(ChainList, ZeroLengthOK),
    Opaque = #var_prefix{num_chains = length(ChainList),
                         map = list_to_tuple(ChainList),
                         separator = PrefixSeparator,
                         num_separators = NumSeps},
    #hash_r{method = var_prefix,
            mod = ?MODULE,
            func = var_prefix_key_to_chain,
            healthy_chainlist = ChainList,
            opaque = Opaque}.

var_prefix_key_to_chain(_ReadOrWrite, Key, Opaque) ->
    N = var_prefix_hash(Key, Opaque#var_prefix.separator,
                        Opaque#var_prefix.num_separators),
    ChainI = (N rem Opaque#var_prefix.num_chains) + 1,
    {ChainName, _ChainMembers} = element(ChainI, Opaque#var_prefix.map),
    ChainName.

var_prefix_hash(Key, Separator, NumSeps) ->
    HashKey = find_var_prefix(Key, Separator, 0, NumSeps),
    erlang:phash2(HashKey).

%% @spec (list({brick_name(), list(node_name())})) -> hash_r()

fixed_prefix_init(ChainList) ->
    fixed_prefix_init(via_proplist, ChainList, []).

%% @spec (via_proplist, chain_list(), prop_list()) -> hash_r()
%% @doc Return a #hash_r record for a variable-length prefix hashing scheme,
%%      throwing an exception if sanity checks fail.
%%
%% When using the via_proplist version of this function, the following
%% properties are examined:
%% <ul>
%% <li> zero_length_ok ... should never be true when defining a
%%      new table. </li>
%% <li> prefix_length ... length of the prefix, default = 4 bytes. </li>
%% <li> prefix_is_integer_hack ... if true, the prefix should be interpreted
%%      as an ASCII representation of a base 10 integer for use as the
%%      hash calculation. </li>
%% </ul>

fixed_prefix_init(via_proplist, ChainList, Props) ->
    ZeroLengthOK = proplists:get_value(zero_length_ok, Props, false),
    Len = proplists:get_value(prefix_length, Props, 4),
    PrefixIsIntegerP = proplists:get_value(prefix_is_integer_hack, Props, false),
    ok = verify_chain_list(ChainList, ZeroLengthOK),
    Opaque = #fixed_prefix{num_chains = length(ChainList),
                           map = list_to_tuple(ChainList),
                           prefix_len = Len,
                           prefix_is_integer_hack_p = PrefixIsIntegerP},
    #hash_r{method = fixed_prefix,
            mod = ?MODULE,
            func = fixed_prefix_key_to_chain,
            healthy_chainlist = ChainList,
            opaque = Opaque}.

fixed_prefix_key_to_chain(_ReadOrWrite, Key, Opaque) ->
    N = fixed_prefix_hash(Key, Opaque#fixed_prefix.prefix_len,
                          Opaque#fixed_prefix.prefix_is_integer_hack_p),
    ChainI = (N rem Opaque#fixed_prefix.num_chains) + 1,
    {ChainName, _ChainMembers} = element(ChainI, Opaque#fixed_prefix.map),
    ChainName.

fixed_prefix_hash(Key, PrefixLen, IntegerHackP) ->
    Len = PrefixLen,
    Prefix = if is_binary(Key), size(Key) > Len ->
                     <<P:Len/binary, _/binary>> = Key,
                     P;
                is_list(Key), length(Key) > Len ->
                     lists:sublist(Key, Len);
                true ->
                     Key
             end,
    if IntegerHackP ->
            list_to_integer(gmt_util:list_ify(Prefix));
       true ->
            erlang:phash2(Prefix)
    end.

%% @spec (via_proplist, chain_list(), prop_list()) -> hash_r()
%% @doc Return a #hash_r record for the consistent hashing scheme,
%%      throwing an exception if sanity checks fail.
%%
%% The following properties are examined for key preprocessing:
%% <ul>
%% <li> prefix_method ... all | var_prefix | fixed_prefix.  This property
%%      is mandatory.  Its value will affect whether or not the other
%%      properties in this section must also be present. </li>
%% <li> zero_length_ok ... should never be true when defining a
%%      new table.  If undefined, assumed to be false. </li>
%% <li> prefix_length ... length of the prefix, default = 4 bytes. </li>
%% <li> prefix_is_integer_hack ... if true, the prefix should be interpreted
%%      as an ASCII representation of a base 10 integer for use as the
%%      hash calculation. </li>
%% <li> prefix_separator ... the single byte ASCII value of the byte
%%      that separates the key's prefix from the rest of the key.
%%      Default is ?HASH_PREFIX_SEPARATOR. </li>
%% <li> num_separators ... number of prefix_separator components that are
%%      included in the key's prefix.
%%      Default is 2 (assuming key looks like "/prefix/not-prefix-stuff").</li>
%% </ul>
%%
%% The following properties are used for the consistent hash map creation:
%% <ul>
%% <li> old_float_map ... The old/current float_map.  If missing, we assume
%%      we're starting from scratch and will use [{unused, 1.0}]. </li>
%% <li> new_chainweights ... list({ChainName::atom(), Weight::integer()}).
%%      This item is mandatory: an obscure exception will be thrown if
%%      it is missing. </li>
%% </ul>
%%

chash_init(via_proplist, ChainList, Props) ->
    ZeroLengthOK = proplists:get_value(zero_length_ok, Props, false),
    ok = verify_chain_list(ChainList, ZeroLengthOK),
    ChainListChains = lists:usort([Ch || {Ch, _Brs} <- ChainList]),
    NewWeights = proplists:get_value(new_chainweights, Props, 'missing_bad!'),

    WeightChains = lists:usort([Ch || {Ch, _Wt} <- NewWeights]),
    if ChainListChains == WeightChains -> ok end, % sanity

    %% Key prefix config items
    Method = case proplists:get_value(prefix_method, Props) of
                 M when M == all; M == var_prefix; M == fixed_prefix -> M
             end,
    Sep = proplists:get_value(prefix_separator, Props, ?HASH_PREFIX_SEPARATOR),
    NumSeps = proplists:get_value(num_separators, Props, 2),
    Len = proplists:get_value(prefix_length, Props, 4),
    PrefixIsIntegerP = proplists:get_value(prefix_is_integer_hack, Props, false),

    %% Start the real work.
    OldFloatMap = proplists:get_value(old_float_map, Props, []),
    NewFloatMap = chash_make_float_map(OldFloatMap, NewWeights),
    NextFloatList = chash_float_map_to_nextfloat_list(NewFloatMap),

    Opaque = #chash{num_chains = length(ChainList),
                    map = list_to_tuple(ChainList),
                    prefix_method = Method,
                    separator = Sep,
                    num_separators = NumSeps,
                    prefix_len = Len,
                    prefix_is_integer_hack_p = PrefixIsIntegerP,
                    chash_map = chash_nextfloat_list_to_gb_tree(NextFloatList),
                    old_float_map = OldFloatMap,
                    new_float_map = NewFloatMap,
                    new_chainweights = NewWeights,
                    old_props = Props
                   },
    #hash_r{method = chash,
            mod = ?MODULE,
            func = chash_key_to_chain,
            healthy_chainlist = ChainList,
            opaque = Opaque}.

chash_key_to_chain(_ReadOrWrite, Key, Opaque) ->
    N = if Opaque#chash.prefix_method == all ->
                naive_hash(Key) / ?PHASH2_BIGGEST_VAL;
           Opaque#chash.prefix_method == var_prefix ->
                var_prefix_hash(Key, Opaque#chash.separator,
                                Opaque#chash.num_separators) / ?PHASH2_BIGGEST_VAL;
           Opaque#chash.prefix_method == fixed_prefix ->
                X = fixed_prefix_hash(Key, Opaque#chash.prefix_len,
                                      Opaque#chash.prefix_is_integer_hack_p),
                X / ?PHASH2_BIGGEST_VAL;
           true ->
                ?APPLOG_ALERT(?APPLOG_APPM_050,"pid ~p: Unknown Opaque: ~p (~p)\n",
                              [self(), element(1, Opaque), Opaque]),
                timer:sleep(1000),
                exit({unknown_prefix_method, Opaque#chash.prefix_method})
        end,
    {_Float, Chain} = chash_gb_next(N, Opaque#chash.chash_map),
    Chain.

%% @doc Not used directly, but can give a developer an idea of how well
%% chash_float_map_to_nextfloat_list will do for a given value of Max.
%%
%% For example:
%% <verbatim>
%%     NewFloatMap = chash_make_float_map([{unused, 1.0}],
%%                                        [{a,100}, {b, 100}, {c, 10}]),
%%     ChashMap = chash_scale_to_int_interval(NewFloatMap, 100),
%%     io:format("QQQ: int int = ~p\n", [ChashIntInterval]),
%% -> [{a,1,47},{b,48,94},{c,94,100}]
%% </verbatim>
%%
%% Interpretation: out of the 100 slots:
%% <ul>
%% <li> 'a' uses the slots 1-47 </li>
%% <li> 'b' uses the slots 48-94 </li>
%% <li> 'c' uses the slots 95-100 </li>
%% </ul>

chash_scale_to_int_interval(NewFloatMap, Max) ->
    chash_scale_to_int_interval(NewFloatMap, 0, Max).

chash_scale_to_int_interval([{Ch, _Wt}], Cur, Max) ->
    [{Ch, Cur, Max}];
chash_scale_to_int_interval([{Ch, Wt}|T], Cur, Max) ->
    Int = trunc(Wt * Max),
    [{Ch, Cur + 1, Cur + Int}|chash_scale_to_int_interval(T, Cur + Int, Max)].

%% chash_scale_to_int_interval(NewFloatMap, Max) ->
%%     chash_scale_to_int_interval(NewFloatMap, 0, Max).

%% chash_scale_to_int_interval([{Ch, _Wt}], _Cur, Max) ->
%%     [{Max, Ch}];
%% chash_scale_to_int_interval([{Ch, Wt}|T], Cur, Max) ->
%%     Int = trunc(Wt * Max),
%%     [{Cur + Int, Ch}|chash_scale_to_int_interval(T, Cur + Int, Max)].

%% TODO: Would it ever be necessary to extract new_h_desc from a g_hash_r()?

%% @spec (hash_r() | g_hash_r()) -> float_map()
%% @doc Given a consistent-hash-flavored #hash_r or #g_hash_r, return the
%% new float_map list (i.e. the float_map used for weighting calculations).

chash_extract_new_float_map(LH) when is_record(LH, hash_r) ->
    (LH#hash_r.opaque)#chash.new_float_map;
chash_extract_new_float_map(GH) when is_record(GH, g_hash_r) ->
    chash_extract_new_float_map(GH#g_hash_r.current_h_desc).

%% @spec (hash_r() | g_hash_r()) -> float_map()
%% @doc Given a consistent-hash-flavored #hash_r or #g_hash_r, return the
%% old float_map list (i.e. the "prior" float_map that was used as an input
%% to chash_make_float_map/2 to create the "new" float_map).

chash_extract_old_float_map(LH) when is_record(LH, hash_r) ->
    (LH#hash_r.opaque)#chash.old_float_map;
chash_extract_old_float_map(GH) when is_record(GH, g_hash_r) ->
    chash_extract_old_float_map(GH#g_hash_r.current_h_desc).

%% @spec (hash_r() | g_hash_r()) -> float_map()
%% @doc Given a consistent-hash-flavored #hash_r or #g_hash_r, return the
%% new chain weights list that was used to create the new float_map list
%% (i.e. the float_map used for weighting calculations).

chash_extract_new_chainweights(LH) when is_record(LH, hash_r) ->
    (LH#hash_r.opaque)#chash.new_chainweights;
chash_extract_new_chainweights(GH) when is_record(GH, g_hash_r) ->
    chash_extract_new_chainweights(GH#g_hash_r.current_h_desc).

%% @spec (hash_r() | g_hash_r()) -> proplist()
%% @doc Given a consistent-hash-flavored #hash_r or #g_hash_r, return the
%% old_props properties list, used for initializing the #hash_r.

chash_extract_old_props(LH) when is_record(LH, hash_r) ->
    (LH#hash_r.opaque)#chash.old_props;
chash_extract_old_props(GH) when is_record(GH, g_hash_r) ->
    chash_extract_old_props(GH#g_hash_r.current_h_desc).

%% @spec (list({chain_name, list({brick_name(), node_name()})}))
%%    -> ok | {error, term()}
%% @doc This is a naive chain single (!) list checker.

verify_chain_list(ChainList) ->
    verify_chain_list(ChainList, false).

verify_chain_list(ChainList, ZeroLengthOK) ->
    case (catch verify_chain_list_2(ChainList, ZeroLengthOK)) of
        {'EXIT', Err} ->
            {error, Err};
        Res ->
            Res
    end.

verify_chain_list_2(ChainList, ZeroLengthOK)
  when is_list(ChainList), length(ChainList) > 0 ->
    lists:map(
      fun({ChainName, ChainMembers}) when is_atom(ChainName) ->
              SortedChainMembers = lists:sort(ChainMembers),
              if length(ChainMembers) < 1, not ZeroLengthOK ->
                      exit({error, ChainName});
                 true ->
                      ok
              end,
              case {SortedChainMembers, list_uniq(SortedChainMembers)} of
                  {Sl, Sl} -> ok;
                  _        -> exit({error, duplicate_bricks_in_chain})
              end,
              %% Check for valid 2-tuples for brick name.
              lists:map(
                fun({Br, Nd}) when is_atom(Br), is_atom(Nd) ->
                        ok;
                   (X) ->
                        exit({error, X})
                end, ChainMembers),
              ok;
         (X) ->
              exit({error, X})
      end, ChainList),
    ChainNames = [Ch || {Ch, _} <- ChainList],
    SortedChainNames = lists:sort(ChainNames),
    case {SortedChainNames, list_uniq(SortedChainNames)} of
        {Sl0, Sl0} -> ok;
        _          -> exit({error, duplicate_chain_names})
    end,
    BrickNames = [Br || {_, Brs} <- ChainList, Br <- Brs],
    SortedBrickNames = lists:sort(BrickNames),
    case {SortedBrickNames, list_uniq(SortedBrickNames)} of
        {Sl1, Sl1} -> ok;
        _          -> exit({error, duplicate_brick_names})
    end,
    ok;
verify_chain_list_2(ChainList, true)
  when is_list(ChainList), length(ChainList) =:= 0 ->
    %% Added this case when brick_admin server is started very early,
    %% and none of the chains are currently available.
    %% TODO: This is harmless/a good idea, right?
    ok;
verify_chain_list_2(Bad, _) ->
    {error, {bad_list, Bad}}.

chain_list2dict(ChainList) ->
    lists:foldl(
      fun({ChainName, []}, Dict) ->
              V = #chaindesc_r{name = ChainName, length = 0,
                               head = no_such_head, middles = [],
                               tail = no_such_tail},
              dict:store(ChainName, V, Dict);
         ({ChainName, ChainMembers}, Dict) ->
              V = make_chaindesc(ChainName, ChainMembers),
              dict:store(ChainName, V, Dict)
      end, dict:new(), ChainList).

make_chaindesc(ChainName, []) ->
    #chaindesc_r{name = ChainName, length = 0,
                 head = chain_is_zero_length, middles = [],
                 tail = chain_is_zero_length};
make_chaindesc(ChainName, ChainMembers) ->
    Head = hd(ChainMembers),
    Tail = lists:last(ChainMembers),
    L = length(ChainMembers),
    Middles = if L > 2 ->
                      Num = L - 2,
                      lists:sublist(ChainMembers, 2, Num);
                 true ->
                      []
              end,
    #chaindesc_r{name = ChainName, length = L,
                 head = Head, middles = Middles, tail = Tail}.

invent_nodelist(NumChains, ChainLen) ->
    invent_nodelist(NumChains, ChainLen, node(), 0).

invent_nodelist(NumChains, ChainLen, Node, BaseChainNum) ->
    [invent_nodelist2(ChainLen, "test_ch" ++ integer_to_list(I), Node) ||
        I <- lists:seq(BaseChainNum, BaseChainNum + NumChains - 1)].

invent_nodelist2(1, NameBase, Node) ->
    ChainName = list_to_atom(NameBase),
    Brick1 = list_to_atom(NameBase ++ "_stand"),
    {ChainName, [{Brick1, Node}]};
invent_nodelist2(N, NameBase, Node) ->
    ChainName = list_to_atom(NameBase),
    Head = list_to_atom(NameBase ++ "_head"),
    Tail = list_to_atom(NameBase ++ "_tail"),
    NumMiddles = N - 2,
    Middles = if NumMiddles == 0 ->
                      [];
                 true ->
                      lists:map(
                        fun(I) ->
                                Mid = list_to_atom(NameBase ++ "_middle" ++
                                                   integer_to_list(I)),
                                {Mid, Node}
                        end, lists:seq(1, NumMiddles))
              end,
    {ChainName, [{Head, Node}] ++ Middles ++ [{Tail, Node}]}.

find_var_prefix(Key, Separator, N, Num) when N < size(Key), Num > 0 ->
    case Key of
        <<Prefix:N/binary, Separator, _/binary>> ->
            if Num == 1 ->
                    Prefix;
               true ->
                    find_var_prefix(Key, Separator, N + 1, Num - 1)
            end;
        _ ->
            find_var_prefix(Key, Separator, N + 1, Num)
    end;
find_var_prefix(Key, _, _, _) ->
    Key.

list_uniq([X,X|Xs]) -> list_uniq([X|Xs]);
list_uniq([X|Xs])   -> [X|list_uniq(Xs)];
list_uniq([])       -> [].

%%
%% Consistent hashing API and helper funcs
%%

chash_make_float_map(NewChainWeights) ->
    chash_make_float_map([], NewChainWeights).

chash_make_float_map([], NewChainWeights) ->
    Sum = add_all_weights(NewChainWeights),
    DiffMap = [{Ch, Wt/Sum} || {Ch, Wt} <- NewChainWeights],
    chash_make_float_map2([{unused, 1.0}], DiffMap, NewChainWeights);
chash_make_float_map(OldFloatMap, NewChainWeights) ->
    NewSum = add_all_weights(NewChainWeights),
    %% Normalize to unit interval
    %% NewChainWeights2 = [{Ch, Wt / NewSum} || {Ch, Wt} <- NewChainWeights],

    %% Reconstruct old chain weights (will be normalized to unit interval)
    SumOldFloatsDict =
        lists:foldl(fun({Ch, Wt}, OrdDict) ->
                            orddict:update_counter(Ch, Wt, OrdDict)
                    end, orddict:new(), OldFloatMap),
    OldChainWeights = orddict:to_list(SumOldFloatsDict),
    OldSum = add_all_weights(OldChainWeights),

    OldChs = [Ch || {Ch, _} <- OldChainWeights],
    NewChs = [Ch || {Ch, _} <- NewChainWeights],
    OldChsOnly = OldChs -- NewChs,

    %% Mark any space in by a deleted chain as unused.
    OldFloatMap2 = lists:map(
                     fun({Ch, Wt} = ChWt) ->
                                 case lists:member(Ch, OldChsOnly) of
                                     true  ->
                                         {unused, Wt};
                                     false ->
                                         ChWt
                                 end
                     end, OldFloatMap),

    %% Create a diff map of changing chains and added chains
    DiffMap = lists:map(fun({Ch, NewWt}) ->
                                case orddict:find(Ch, SumOldFloatsDict) of
                                    {ok, OldWt} ->
                                        {Ch, (NewWt / NewSum) -
                                             (OldWt / OldSum)};
                                    error ->
                                        {Ch, NewWt / NewSum}
                                end
                        end, NewChainWeights),
    chash_make_float_map2(OldFloatMap2, DiffMap, NewChainWeights).

chash_make_float_map2(OldFloatMap, DiffMap, _NewChainWeights) ->
    FloatMap = apply_diffmap(DiffMap, OldFloatMap),
    XX = combine_neighbors(collapse_unused_in_float_map(FloatMap)),
    XX.

apply_diffmap(DiffMap, FloatMap) ->
    SubtractDiff = [{Ch, abs(Diff)} || {Ch, Diff} <- DiffMap, Diff < 0],
    AddDiff = [D || {_Ch, Diff} = D <- DiffMap, Diff > 0],
    TmpFloatMap = iter_diffmap_subtract(SubtractDiff, FloatMap),
    iter_diffmap_add(AddDiff, TmpFloatMap).

add_all_weights(ChainWeights) ->
    lists:foldl(fun({_Ch, Weight}, Sum) -> Sum + Weight end, 0.0, ChainWeights).

iter_diffmap_subtract([{Ch, Diff}|T], FloatMap) ->
    iter_diffmap_subtract(T, apply_diffmap_subtract(Ch, Diff, FloatMap));
iter_diffmap_subtract([], FloatMap) ->
    FloatMap.

iter_diffmap_add([{Ch, Diff}|T], FloatMap) ->
    iter_diffmap_add(T, apply_diffmap_add(Ch, Diff, FloatMap));
iter_diffmap_add([], FloatMap) ->
    FloatMap.

apply_diffmap_subtract(Ch, Diff, [{Ch, Wt}|T]) ->
    if Wt == Diff ->
            [{unused, Wt}|T];
       Wt > Diff ->
            [{Ch, Wt - Diff}, {unused, Diff}|T];
       Wt < Diff ->
            [{unused, Wt}|apply_diffmap_subtract(Ch, Diff - Wt, T)]
    end;
apply_diffmap_subtract(Ch, Diff, [H|T]) ->
    [H|apply_diffmap_subtract(Ch, Diff, T)];
apply_diffmap_subtract(_Ch, _Diff, []) ->
    [].

apply_diffmap_add(Ch, Diff, [{unused, Wt}|T]) ->
    if Wt == Diff ->
            [{Ch, Wt}|T];
       Wt > Diff ->
            [{Ch, Diff}, {unused, Wt - Diff}|T];
       Wt < Diff ->
            [{Ch, Wt}|apply_diffmap_add(Ch, Diff - Wt, T)]
    end;
apply_diffmap_add(Ch, Diff, [H|T]) ->
    [H|apply_diffmap_add(Ch, Diff, T)];
apply_diffmap_add(_Ch, _Diff, []) ->
    [].

combine_neighbors([{Ch, Wt1}, {Ch, Wt2}|T]) ->
    combine_neighbors([{Ch, Wt1 + Wt2}|T]);
combine_neighbors([H|T]) ->
    [H|combine_neighbors(T)];
combine_neighbors([]) ->
    [].

collapse_unused_in_float_map([{Ch, Wt1}, {unused, Wt2}|T]) ->
    collapse_unused_in_float_map([{Ch, Wt1 + Wt2}|T]);
collapse_unused_in_float_map([{unused, _}] = L) ->
    L;                                          % Degenerate case only
collapse_unused_in_float_map([H|T]) ->
    [H|collapse_unused_in_float_map(T)];
collapse_unused_in_float_map([]) ->
    [].

chash_float_map_to_nextfloat_list(FloatMap) when length(FloatMap) > 0 ->
    %% QuickCheck found a bug ... need to weed out stuff smaller than
    %% ?SMALLEST_SIGNIFICANT_FLOAT_SIZE here.
    FM1 = [P || {_X, Y} = P <- FloatMap, Y > ?SMALLEST_SIGNIFICANT_FLOAT_SIZE],
    {_Sum, NFs0} = lists:foldl(fun({Name, Amount}, {Sum, List}) ->
                                       {Sum+Amount, [{Sum+Amount, Name}|List]}
                               end, {0, []}, FM1),
    lists:reverse(NFs0).

chash_nextfloat_list_to_gb_tree([]) ->
    gb_trees:balance(gb_trees:from_orddict([]));
chash_nextfloat_list_to_gb_tree(NextFloatList) ->
    {_FloatPos, Name} = lists:last(NextFloatList),
    %% QuickCheck found a bug ... it really helps to add a catch-all item
    %% at the far "right" of the list ... 42.0 is much greater than 1.0.
    NFs = NextFloatList ++ [{42.0, Name}],
    gb_trees:balance(gb_trees:from_orddict(orddict:from_list(NFs))).

chash_gb_next(X, {_, GbTree}) ->
    chash_gb_next1(X, GbTree).

chash_gb_next1(X, {Key, Val, Left, _Right}) when X < Key ->
    case chash_gb_next1(X, Left) of
        nil ->
            {Key, Val};
        Res ->
            Res
    end;
chash_gb_next1(X, {Key, _Val, _Left, Right}) when X >= Key ->
    chash_gb_next1(X, Right);
chash_gb_next1(_X, nil) ->
    nil.


%% @type float_map() = list({brick(), float()}).  A float_map is a
%% definition of brick assignments over the unit interval [0.0, 1.0].
%% The sum of all floats must be 1.0.
%% For example, [{{br1, nd1}, 0.25}, {{br2, nd1}, 0.5}, {{br3, nd1}, 0.25}].

%% @type nextfloat_list() = list({float(), brick()}).  A nextfloat_list
%% differs from a float_map in two respects: 1) nextfloat_list contains
%% tuples with the brick name in 2nd position, 2) the float() at each
%% position I_n > I_m, for all n, m such that n > m.
%% For example, a nextfloat_list of the float_map example above,
%% [{0.25, {br1, nd1}}, {0.75, {br2, nd1}}, {1.0, {br3, nd1}].

%%
%% Doodling
%%

doo_make_chain_weights(Len) ->
    [{list_to_atom([Name]), 100} || Name <- lists:seq($a, $a + Len - 1)].

%% doo_make_chain(Weights) ->
%%     [{Name, [{x,y}]} || {Name, _Wt} <- Weights].

doo_iter_chain(StartFloatMap, StartLen, Iters) ->
    lists:foldl(
      fun(ChainLen, FloatMap) ->
              NewWeights = doo_make_chain_weights(ChainLen),
%%            NewChain = doo_make_chain(NewWeights),
              chash_make_float_map(FloatMap, NewWeights)
      end, StartFloatMap, lists:seq(StartLen + 1, StartLen + Iters)).

%% length(brick_hash:doo_iter_chain([], 0, 100)) -> 4265
%% And, to see the smallest subdivisions:
%% io:format("~P\n", [lists:keysort(2, brick_hash:doo_iter_chain([], 0, 100)), 150]).
%% [{'\257',3.46945e-18},
%%  {'\230',3.46945e-18},
%%  {'\223',1.04083e-17},
%% ...
%%  {'\246',9.36751e-17},
%%  {'\221',9.71445e-17},
%%  {'\276',1.04083e-16},
%% ...
%%  {'\257',7.26849e-16},
%%  {'\300',3.01631e-7},
%%  {'\301',3.38759e-7},
%%
%% So, even after 100 migrations of equal size, the smallest
%% significant partition is still as large as 3.01e-7.  The sum of all
%% partitions smaller than that (less or equal to 1.04083e-16) is only
%% 1.32810e-14, so they're pretty safe to ignore.  I've added the
%% constant ?SMALLEST_SIGNIFICANT_FLOAT_SIZE to keep track of this.

doo_weight1() ->
    [{a, 0.1}, {b, 0.025}, {c, 0.125}, {d, 0.125}, {e, 0.025}, {f, 0.1},
     {g, 0.125}, {h, 0.125}, {i, 0.05}, {j, 0.075}, {k, 0.125}].
