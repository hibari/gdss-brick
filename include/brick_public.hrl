%%%----------------------------------------------------------------------
%%% Copyright (c) 2009-2014 Hibari developers.  All rights reserved.
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
%%% File    : brick_public.hrl
%%% Purpose : brick public stuff
%%%----------------------------------------------------------------------

-define(BRICK__GET_MANY_FIRST, '$start_of_table').
-define(BRICK__GET_MANY_LAST, '$end_of_table').

%% Store tuple value to signify that the desired value is on disk
%% and should the current op does not modify the key's value.
-define(VALUE_REMAINS_CONSTANT, '$disk_remains_constant/$').

%% Used by scavenger to re-locate a bigdata_dir value hunk.  This is
%% the magic atom to use.  For more security despite the obscurity,
%% we'll require that the client give the correct current hunk
%% location, which is a piece of data that's harder to leak out of the
%% system.  (Sssh!  Don't tell anyone about the
%% 'get_many_raw_storetuples' flag!)
-define(VALUE_SWITCHAROO, '$value_switch-a-roo/$').

%% Used to rename a oldkey/value pair to a newkey/value pair.
-define(KEY_SWITCHAROO, '$key_switch-a-roo/$').

-define(BRICK__SMALLEST_KEY, <<>>).
-define(BRICK__BIGGEST_KEY,  <<255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255>>). % list_to_binary(lists:duplicate(255, 255)).

-define(SUB_BRICK_SUP, brick_brick_sup).

