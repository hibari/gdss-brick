%%%----------------------------------------------------------------------
%%% Copyright (c) 2008-2014 Hibari developers.  All rights reserved.
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
%%% File    : gmt_hlog.hrl
%%% Purpose : GMT hunk logger
%%%----------------------------------------------------------------------

-ifndef(gmt_hlog).
-define(gmt_hlog, true).

-include("brick_specs.hrl").

-include_lib("kernel/include/file.hrl").

-define(GMT_HLOG_COMMON_LOG_NAME, commonLogServer).
-define(LOCAL_RECORD_TYPENUM, 7742).

-record(hunk_summ, {
          seq=undefined :: seqnum() | undefined,   % log sequence #
          off=undefined :: offset() | undefined,   % byte offset
          type          :: typenum(),              % hunk type
          len           :: len(),                  % hunk size
          first_off     :: offset(),               % offset of hd(c_len)
          c_len=[]      :: list(len()),            % checksummed
          u_len=[]      :: list(len()),            % un-checksummed
          md5s=[]       :: list(binary()),         % MD5 values
          c_blobs=[]    :: list(binary()),         % optional!
          u_blobs=[]    :: list(binary())          % optional!
          }).

-type servername() :: atom().
-type server() :: servername() | {atom(),node()} | {global,atom()} | pid().

-type blobs() :: [] | list(bytes()).
-type brickname() :: atom().
-type bytes() :: iodata().
-type checkmd5() :: boolean().
-type dirname() :: nonempty_string().
-type eee() :: {eee, brickname(), seqnum(), offset(), key(), typenum(), len(), bytes()}.
-type filtfun() :: fun((seqnum()) -> boolean()).
-type foldacc() :: term().
-type foldfun() :: fun((#hunk_summ{}, file:fd(), foldacc()) -> foldacc()).
-type foldret() :: {foldacc(), list({term(), [{seq, seqnum(), err, term()}]})} | no_return().
-type hlogtype() :: 'metadata' | 'bigblob' | 'bigblob_longterm'.
-type incr() :: integer().
-type lenhint() :: len().
-type lenhintORxformfun() :: lenhint() | xformfun().
-type nth() :: non_neg_integer().
-type offset() :: non_neg_integer().
-type openmode() :: list('append' | 'binary' | 'compressed' | 'delayed_write' | 'raw' | 'read' | 'read_ahead' | 'write' | {'read_ahead',pos_integer()} | {'delayed_write',pos_integer(),non_neg_integer()}).
-type seqnum() :: integer().
-type typenum() :: integer().
-type xformfun() :: fun((Su::#hunk_summ{}, FH::file:fd()) -> binary()).

-endif. % -ifndef(gmt_hlog)
