%%%----------------------------------------------------------------------
%%% Copyright (c) 2009-2013 Hibari developers.  All rights reserved.
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
%%% File    : brick_hash.hrl
%%% Purpose : brick hash stuff
%%%----------------------------------------------------------------------

%% Hash descriptor for a *single* table.
-record(hash_r, {
          method                :: naive | var_prefix | fixed_prefix | chash,    % name of method,
          %% Mod:Func(read | write, Key, Opaque) -> ChainName
          mod                   :: atom(),                                       % atom()
          func                  :: atom(),                                       % atom()
          healthy_chainlist     :: brick_bp:proplist(),
          opaque                :: brick_hash:naive_r() | brick_hash:var_prefix_r() | brick_hash:fixed_prefix_r() | brick_hash:chash_r()
         }).

%% Global hash state for a *single* table.
-record(g_hash_r, {
          %% Stuff that stays constant as bricks run/crash/restart
          current_rev = 1           :: integer(),
          current_h_desc            :: #hash_r{},
          new_h_desc                :: #hash_r{},

          %% Stuff that changes as bricks run/crash/restart
          minor_rev = 1             :: integer(),           % updated each time that
                                                            % either of the 2 dicts below
                                                            % is updated
          current_chain_dict        :: dict(),              % dict keyed by chain name
          new_chain_dict            :: dict(),

          %% Migration-related stuff, maintained locally by each brick.
          migrating_p = false       :: boolean(),
          phase = pre               :: pre | migrating,
          cookie                    :: term(),
          migr_dict                 :: dict() | undefined   % migration dictionary
                                                            % dict keyed by chain name
         }).

%% Character that separates the hashable prefix
-define(HASH_PREFIX_SEPARATOR, $/).

