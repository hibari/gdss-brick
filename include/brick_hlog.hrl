%%%----------------------------------------------------------------------
%%% Copyright (c) 2008-2014 Hibari developers. All rights reserved.
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
%%% File    : brick_hlog.hrl
%%% Purpose : specs for log hunk.
%%%----------------------------------------------------------------------

-ifndef(brick_hlog_hrl).
-define(brick_hlog_hrl, true).

%% Reg Names

-define(WAL_SERVER_REG_NAME,       hibari_wal_server).
-define(METADATA_STORE_REG_NAME,   hibari_metadata_store_registory).
-define(BRICK_BLOB_STORE_REG_NAME, hibari_blob_store_registory).


%% Types

-type hunk_bytes() :: binary().

%% Need fixed-length blob types for better space utilization?
%% Parhaps smaller blob (< 16 bytes or so) should be embedded into
%% its metadata (= value_in_ram).

-type hunk_type() ::
        %% WAL Hunk
        metadata |     %% metadata, many values per hunk
        blob_wal |     %% value blob, one value per hunk

        %% Brick Private Hunk
        blob_single |  %% value blob, one value per hunk
        blob_multi.    %% value blob, many values per hunk

-type hunk_flag() :: deleted  %% Not used for now
                   | no_md5.


%% NOTE: An upper layer (scavenger) should pack multiple values into
%% one blob_multi hunk (e.g. nearly 4KB) so that it can avoid the
%% overhead of having the hunk enclosure (especially md5 on each
%% value). brick_hlog_hunk:read_blob_directly/4 is used to read a blob
%% in a blob_multi hunk.

-record(hunk, {
          type       :: hunk_type() | undefined,
          flags= []  :: [hunk_flag()],
          brick_name :: atom() | undefined,     %% Do not set this value for
                                                %% blob_single or blob_multi types
          blobs= []  :: [binary()],
          md5        :: binary() | undefined
         }).
-type hunk() :: #hunk{}.

-type callback_ticket() :: reference().

-endif. % -ifndef(brick_hlog_hrl).
