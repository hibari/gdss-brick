%%%----------------------------------------------------------------------
%%% Copyright (c) 2006-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : gdss.app
%%% Purpose : gdss application
%%%----------------------------------------------------------------------

%% For further documentation, see:
%%
%%   http://www.erlang.org/doc/doc-5.5/doc/system_principles/
%%     Section 1 for a big(ger) picture overview
%%
%%   http://www.erlang.org/doc/doc-5.5/doc/design_principles/
%%     Section 7 for description of an OTP "application", incl. the .app file

{application, gdss,
 [
  %% Description of this application
  {description, "Gemini Distributed Storage Service"},

  %% Version number
  {vsn, "0.01"},

  %% List of modules ... used only for formal OTP packaging (not used yet)
  {modules, [
	     %% TODO: fill in this list, perhaps
            ]
  },

  %% List of registered process names ... used only for formal OTP
  %% packaging (not used yet)
  {registered, [
		%% TODO: fill in this list, perhaps
	       ]
  },

  %% Applications which must be started before this application starts.
  %% NOTE: do not list applications which are load-only!
  {applications, [ kernel, stdlib, sasl, crypto ] },
  {mod, {brick, []} }
 ]
}.
