%%%----------------------------------------------------------------------
%%% Copyright (c) 2009-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : brick_admin.hrl
%%% Purpose : brick admin stuff
%%%----------------------------------------------------------------------

-define(BKEY_SCHEMA_DEFINITION, schema_definition).
-define(BKEY_CLIENT_MONITOR,    client_monitor_list).
-define(BKEY_MIGRATION_STATE,   migration_state).

-record(hevent, {
          time                 :: brick_bp:nowtime(),       % now()
          what                 :: atom(),                   % term()
          detail               :: atom(),                   % term()
          props = []           :: brick_bp:proplist()       % proplist()
         }).

%% Used by brick_chainmon.erl and brick_test0.erl

-record(opconf_r, {
          me                   :: brick_server:brick(),
          role                 :: brick_server:role(),
          upstream             :: brick_server:brick(),
          downstream           :: brick_server:brick(),
          off_tail_p = false   :: boolean()                     % Do not change default!
         }).

%% The #schema_r() contains all persistent state

-record(schema_r, {
          %% Where are all redundant copies of this schema stored?
          schema_bricklist      :: brick_chainmon:brick_list(),                 % list(brick_t())

          %% Dictionary of all table definitions
          %% Key = table name, Val = table_r()
          tabdefs                               :: dict(),                         % dict:new()

          %% Mapping of chain to table name
          chain2tab                             :: dict()                          % dict:new()
         }).

-record(table_r, {
          %% Hard state ... save persistently upon any change

          %% Table name
          name                                      :: atom(),
          %% List of options for each brick
          brick_options                         :: brick_bp:proplist(),
          %% Current view revision ... also in #g_hash_r, bad duplication??
          current_rev                           :: integer(),
          %% Current phase ... also in #g_hash_r, bad duplication??
          current_phase                         :: pre | migrating,
          %% Migration options
          migration_options             :: brick_bp:proplist(),
          %% Global hash state
          ghash                     :: brick_server:global_hash_r()
         }).

