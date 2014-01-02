%%%----------------------------------------------------------------------
%%% Copyright (c) 2008-2013 Hibari developers.  All rights reserved.
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
%%% File    : gmt_hlog.erl
%%% Purpose : Hunk log server.
%%%----------------------------------------------------------------------

%% @doc A hunk-based log server, a partial replacement for the
%% Erlang/OTP `disk_log' module, plus support for random access to
%% hunks stored within the log.
%%
%% It's really, really useful to be able to have random access into
%% the middle of a transaction log file.  The Berkeley DB logging
%% subsystem provides such a feature.  The Erlang/OTP `disk_log'
%% module does not.
%%
%% I considered glueing in support to Berkeley DB, just to access its
%% logging subsystem.  However, it's still a little too tightly tied
%% to other BDB subsystems ... so I implemented my own.  {sigh}
%%
%% But I did get to add a couple of features that are not present in
%% the BDB's loggging:
%% <ul>
%% <li> Each hunk written to the log can contain multiple blobs. </li>
%% <li> Within a hunk, each blob may also have an MD5 checkum stored
%% within the hunk.  A hunk can store multiple checksummed and
%% un-checksummed blobs. </li>
%% </ul>
%%
%% TODO: See SVN commit log for "Unfinished Stuff".
%%
%% == Terminology ==
%%
%% <ul>
%% <li> <b>Log</b>: A collection of log files. </li>
%% <li> <b>Log file</b>: A single file that stores hunks as part of a larger
%%      collection known as a "log". </li>
%% <li> <b>Hunk</b>: A collection of blobs that is appended as an atomic
%%      unit to the end of the log's latest log file.  </li>
%% <li> <b>{SeqNum, Offset}</b>:  This tuple uniquely identifies a hunk.
%%      It is directly analogous to the Berkeley DB "LSN" (Log Sequence
%%      Number).  Together with the directory path for the log, any
%%      part of the hunk can be retrieved via random access.</li>
%% <li> <b>Hunk type</b>: A 32-bit integer that describes the type of hunk.
%%      Mostly useful for application use.</li>
%% <li> <b>Blob</b>: An Erlang binary term.  </li>
%% <li> <b>CBlob/c_blob</b>: A blob that also has an MD5 checksum stored inside
%%      the hunk. </li>
%% <li> <b>UBlob/u_blob</b>: A blob without an MD5 checksum.  </li>
%% </ul>

%% !@#$!, this is only 10% faster than gmt_hlog2.erl

%% os:cmd("rm -rf hlog.zzz").
%% os:cmd("rm gdss_dev@bb2-2.zzz,*").
%% brick_server:start_link(zzz, []).
%% brick_server:chain_role_standalone(zzz, node()).
%% brick_server:chain_set_my_repair_state(zzz, node(), ok).
%% brick_server:set_do_sync({zzz, node()}, false).
%% [brick_server:set(zzz, node(), term_to_binary(N), term_to_binary(N)) || N <- lists:seq(1, 64*1024)].
%% [begin (catch unlink(whereis(zzz))), (catch exit(whereis(zzz), kill)), timer:tc(brick_server, start_link, [zzz,[]]) end || _ <- lists:seq(1,8)].

%% disk_log ("original" implementation):
%% [{384837,{ok,<0.478.0>}},
%%  {372438,{ok,<0.518.0>}},
%%  {364133,{ok,<0.560.0>}},
%%  {382041,{ok,<0.604.0>}},
%%  {375758,{ok,<0.650.0>}},
%%  {385051,{ok,<0.698.0>}},
%%  {636888,{ok,<0.748.0>}},
%%  {383953,{ok,<0.800.0>}}]

%% gmt_hlog2:
%% [{1771579,{ok,<0.107.0>}},
%%  {1123872,{ok,<0.115.0>}},
%%  {1092433,{ok,<0.124.0>}},
%%  {1140370,{ok,<0.134.0>}},
%%  {1240544,{ok,<0.145.0>}},
%%  {1321349,{ok,<0.157.0>}},
%%  {1137310,{ok,<0.170.0>}},
%%  {1125928,{ok,<0.184.0>}}]

%% gmt_hlog3:
%% [{881524,{ok,<0.225.0>}},
%%  {841287,{ok,<0.233.0>}},
%%  {1231971,{ok,<0.242.0>}},
%%  {915891,{ok,<0.252.0>}},
%%  {954251,{ok,<0.263.0>}},
%%  {933328,{ok,<0.275.0>}},
%%  {939230,{ok,<0.288.0>}},
%%  {917481,{ok,<0.302.0>}}]


%% Bugs found with Quickcheck:
%% * rollover file handle not created correctly (opened with wrong func)
%% * attempted reuse of old log file's file handle
%% * when pread cache was added, found bug where entire blob larger than
%%   pread cache size would fail.
%% * found bug in optimization added to read_hunk_member_ll() to tweak
%%   typical brick_ets usage but broke the general case.
%% * numerous glitches when refactoring (early Aug 2008) for short-term
%%   and long term file paths.
%% * A scribble in the middle of an unchecksummed blob needs extra checking.
%% * A scribble into the hunk header's LenBinT value that makes it
%%   bigger than all remaining data in the file could result in an
%%   infinite loop.
%% * A scribble into a length value in the hunk summary of the last hunk
%%   in the sequence file needs extra checking.
%%   - A scribble into the length value of an unchecksummed blob in the
%%     last hunk of the file that makes the blob bigger than it's supposed
%%     to be can give you the wrong blob.  But if you cared enough about
%%     data integrity, you wouldn't have stored the blob as unchecksummed.


-module(gmt_hlog).

-behaviour(gen_server).

-include("gmt_hlog.hrl").
-include("brick.hrl").                          % for ?E_ macros

%% TODO: This stuff should be configurable at runtime, but it's not
%%       convenient to solve the problem of how to propagate such
%%       config numbers.
-define(ARCH_H1_SIZE, 3).
-define(ARCH_H2_SIZE, 7).

%% TODO: Tune this value?  I've witnessed problems with
%%       move_seq_to_longterm/2 hitting timeout when the disk has been
%%       overloaded by checkpoint-related disk I/O.
-define(TIMEOUT, 60*1000).

%% TODO: Do we need to make this MY_PREAD_MODEST_READAHEAD_BYTES value more flexible?
%%
%% A #hunk_summ{} with 1 MD5 checksum and small bignums for the offsets
%% is about 93 bytes, so round up.
%% size(term_to_binary(#hunk_summ{seq = 999999999999, off = 999999999999, type = 1, len = 999999999999, first_off = 999999999999, c_len = [999999999999], md5s = [erlang:md5("")]}))
%% ... then round up to a typical file system page.
%%
-define(MY_PREAD_MODEST_READAHEAD_BYTES, 4096).
-define(MAX_HUNK_HEADER_BYTES,            512).

%% External exports
-export([start_link/1, stop/1,
         find_current_log_files/1,
         find_current_log_seqnums/1,
         find_longterm_log_seqnums/3,
         find_mostest_longterm_sequence_num/3,
         find_mostest_sequence_num/3,
         write_hunk/7,
         read_hunk/3,
         read_hunk/4,
         read_hunk/5,
         read_hunk_summ_ll/2,
         read_hunk_summ_ll/3,
         read_hunk_member_ll/4,
         read_hunk_member_ll/5,
         read_hunk_summary/3,
         read_hunk_summary/5,
         parse_hunk_summary/1,
         get_hunk_cblob_member/3,
         get_current_seqnum/1,
         get_current_seqnum_and_offset/1,
         get_current_seqnum_and_file_position/1,
         get_all_seqnums/1,
         advance_seqnum/2,
         move_seq_to_longterm/2,
         sync/1,
         sync/2,
         get_proplist/1,
         log_name2data_dir/1,
         log_name2reg_name/1,
         fold/3,
         fold/4,
         fold/5,
         md5_checksum_ok_p/1,
         perm_config_file/1,
         old_perm_config_file/1
        ]).

%% For use by gmt_hlog_local.erl only!
-export([write_hunk_internalwrapper/7,
         file_header_version_1/0,
         open_log_file/3
        ]).
%% API to be used on local node only (match gmt_hlog_local.erl exports)
-export([read_bigblob_hunk_blob/2,
         read_bigblob_hunk_blob/3,
         read_bigblob_hunk_blob/4,
         read_bigblob_hunk_blob/5]).

%% Non-gen_server-related exports (so, is this ugly or not?)
-export([create_hunk/3,
         write_log_header/1,
         log_file_path/2,
         log_file_path/3,
         log_file_info/2
        ]).

%% For my_pread QC testing only!
-export([my_pread/4,
         my_pread_start/0
        ]).

%% Debugging
-export([debug_fold_a_file/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%%%----------------------------------------------------------------------
%%% Types/Specs/Records
%%%----------------------------------------------------------------------

-type props() :: list({name,servername()}
                      | {file_len_max,len()}
                      | {file_len_min,len()}
                      | {long_h1_size,integer()}
                      | {long_h2_size,integer()}).

-type from() :: {pid(),term()}.

-record(state, {
          props                       :: props(),
          name                        :: atom(),
          dir                         :: dirname(),                % storage dir
          syncers_next_round=[]       :: list(from()),
          syncer_pid=undefined        :: pid() | undefined,
          syncer_ref=undefined        :: reference() | undefined,
          write_backlog=[]            :: list(bytes()),
          syncer_advance_reqs=[]      :: list({integer(), from()}),
          debug_write_sleep=0         :: non_neg_integer(),
          debug_sync_sleep=0          :: non_neg_integer(),
          file_len_max                :: len(),                    % log file size max
          file_len_min                :: len(),                    % log file size min
          long_h1_size                :: integer(),                % longterm dir #1 size
          long_h2_size                :: integer(),                % longterm dir #2 size
          last_seq                    :: integer(),                % last sequence #
          %% short-term state
          cur_seqS                    :: integer(),                % current sequence #
          cur_offS                    :: integer(),                % current offset
          cur_fhS                     :: file:fd(),                % current file
          %% long-term state
          cur_seqL                    :: integer(),                % current sequence #
          cur_offL                    :: integer(),                % current offset
          cur_fhL                     :: file:fd()                 % current file
         }).
-type state_r() :: #state{}.


%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------

%% @doc PropList flags: See init() docs.

-spec start_link(props()) -> {ok,pid()} | {error,term()} | ignore.
start_link(PropList) ->
    gen_server:start_link(?MODULE, PropList, []).

-spec stop(server()) -> ok | {error,term()} | ignore.
stop(Server) ->
    gen_server:call(Server, {stop}, ?TIMEOUT).

%% @spec (server_spec(), atom(), metadata|bigblob|bigblob_longterm, io_list(), integer(), list(io_list()), list(io_list())) ->
%%       {ok, integer(), integer()} | {hunk_too_big, integer()}
%% @doc Write a hunk to the log.
%%
%% HLogType: 'metadata' and 'bigblob' will write to shortterm.
%%           'bigblob_longterm' will write to longterm.

-spec write_hunk(server(), brickname(), hlogtype(), key(), typenum(),
                 CBlobs::blobs(), UBlobs::blobs()) ->
                        {ok, seqnum(), offset()} | {hunk_too_big, len()} | no_return().
write_hunk(Server, LocalBrickName, HLogType, Key, TypeNum, CBlobs, UBlobs)
  when is_atom(HLogType), is_integer(TypeNum),
       is_list(CBlobs), is_list(UBlobs) ->
    if HLogType =:= metadata          -> ok;
       HLogType =:= bigblob           -> ok;
       HLogType =:= bigblob_longterm  -> ok % For use by scavenger
    end,
    {H_Len, H_Bytes} = create_hunk(TypeNum, CBlobs, UBlobs),
    write_hunk_internalwrapper(Server, LocalBrickName, HLogType, Key,
                               TypeNum, H_Len, H_Bytes).

-spec write_hunk_internalwrapper(server(), brickname(), hlogtype(),
                                 key(), typenum(), len(), bytes()) ->
                                        {ok, seqnum(), offset()} | {hunk_too_big, len()} | no_return().
write_hunk_internalwrapper(Server, LocalBrickName, HLogType, Key,
                           TypeNum, H_Len, H_Bytes) ->
    gen_server:call(Server, {write_hunk_bytes, LocalBrickName, HLogType, Key,
                             TypeNum, H_Len, H_Bytes}, ?TIMEOUT).

%% @spec (server_spec(), integer(), integer()) ->
%%       {ok, binary()} | term()
%% @doc Read the first MD5-summed blob out of the specified hunk.

-spec read_hunk(server(), seqnum(), offset()) -> {ok, binary()} | no_return().
read_hunk(Server, SeqNum, Offset)
  when is_integer(SeqNum), is_integer(Offset) ->
    Fun = fun(Su, FH) -> {ok, read_hunk_member_ll(FH, Su, md5, 1)} end,
    read_hunk(Server, SeqNum, Offset, 0, Fun).

%% @spec (server_spec(), integer(), integer(), integer() | fun()) ->
%%       {ok, integer(), integer()} | {hunk_too_big, integer()}
%% @doc Read the summary of the specified hunk, then apply the transform
%%      func to that summary.

-spec read_hunk(server(), seqnum(), offset(), lenhintORxformfun()) -> {ok, binary()} | no_return().
read_hunk(Server, SeqNum, Offset, LenHint)
  when is_integer(SeqNum), is_integer(Offset), is_integer(LenHint) ->
    Fun = fun(Su, FH) -> {ok, read_hunk_member_ll(FH, Su, md5, 1)} end,
    read_hunk(Server, SeqNum, Offset, LenHint, Fun);
read_hunk(Server, SeqNum, Offset, XFormFunc)
  when is_integer(SeqNum), is_integer(Offset), is_function(XFormFunc, 2) ->
    read_hunk(Server, SeqNum, Offset, 0, XFormFunc).

%% @spec (server_spec(), integer(), integer(), integer(), fun()) ->
%%       {ok, integer(), integer()} | {hunk_too_big, integer()}
%% @doc Read the summary of the specified hunk, then apply the transform
%%      func to that summary.

-spec read_hunk(server(), seqnum(), offset(), lenhint(), xformfun()) -> {ok, binary()} | no_return().
read_hunk(Server, SeqNum, Offset, LenHint, XFormFunc)
  when is_integer(SeqNum), is_integer(Offset), is_integer(LenHint),
       is_function(XFormFunc, 2) ->
    gen_server:call(Server, {read_hunk, SeqNum, Offset, LenHint, XFormFunc}, ?TIMEOUT).

-spec fold(dirname(), foldfun(), foldacc()) -> foldret().
fold(Dir, Fun, Acc) when is_function(Fun, 3) ->
    fold(shortterm, Dir, Fun, fun(_) -> true end, Acc).

-spec fold(shortterm | longterm, dirname(), foldfun(), foldacc()) -> foldret().
fold(ShortLong, Dir, Fun, Acc) when is_function(Fun, 3) ->
    fold(ShortLong, Dir, Fun, fun(_) -> true end, Acc).

-spec fold(shortterm | longterm, dirname(), foldfun(), filtfun(), foldacc()) -> foldret().
fold(ShortLong, Dir, Fun, FiltFun, Acc)
  when (ShortLong =:= shortterm orelse ShortLong =:= longterm) andalso
       is_function(Fun, 3) andalso is_function(FiltFun, 1) ->
    fold2(ShortLong, Dir, Fun, FiltFun, Acc).

%% @spec (server_spec()) -> integer()
%% @doc Get the log server's current sequence number for short-term
%%      storage and sequence number of the last checkpoint, respectively.

-spec get_current_seqnum(server()) -> seqnum().
get_current_seqnum(Server) ->
    gen_server:call(Server, {get_current_seqnum}, ?TIMEOUT).

%% @spec (server_spec()) -> {integer(), integer()}
%% @doc Get the log server's current sequence number &amp; offset for short-term
%%      storage.

-spec get_current_seqnum_and_offset(server()) -> {seqnum(), offset()}.
get_current_seqnum_and_offset(Server) ->
    gen_server:call(Server, {get_current_seqnum_and_offset}, ?TIMEOUT).

%% @spec (server_spec()) -> {integer(), integer()}
%% @doc Get the log server's current sequence number &amp; physical file offset
%%      for short-term storage.

-spec get_current_seqnum_and_file_position(server()) -> {seqnum(), offset()}.
get_current_seqnum_and_file_position(Server) ->
    gen_server:call(Server, {get_current_seqnum_and_file_position}, ?TIMEOUT).

%% @spec (server_spec()) -> integer()
%% @doc Get a list (order is undefined) of all of the log server's
%%      sequence numbers (short-term and long-term).

-spec get_all_seqnums(server()) -> list(seqnum()).
get_all_seqnums(Server) ->
    gen_server:call(Server, {get_all_seqnums}, 120*1000).

%% @spec (server_spec(), integer()) -> {ok, integer()} | error
%% @doc Advance the sequence number by Incr amount, the short-term log
%%      if Incr is positive, the long-term log if Incr is negative.

-spec advance_seqnum(server(), incr()) -> {ok, seqnum()} | error | no_return().
advance_seqnum(Server, Incr)
  when is_integer(Incr), Incr =/= 0 ->
    gen_server:call(Server, {advance_seqnum, Incr}, ?TIMEOUT);
advance_seqnum(_Server, _Incr) ->
    erlang:error(badarg).

%% @spec (server_spec(), integer()) -> ok | error
%% @doc Move a sequence file from short-term to long-term/longterm
%%      storage location.

-spec move_seq_to_longterm(server(), seqnum()) -> ok | error | {error,seqnum()} | no_return().
move_seq_to_longterm(Server, SeqNum)
  when is_integer(SeqNum), SeqNum > 0 ->
    gen_server:call(Server, {move_seq_to_longterm, SeqNum}, ?TIMEOUT);
move_seq_to_longterm(_Server, _SeqNum) ->
    erlang:error(badarg).

-spec sync(server()) -> {ok, seqnum(), offset()}.
sync(Server) ->
    sync(Server,shortterm).

-spec sync(server(),shortterm | longterm) -> {ok, seqnum(), offset()}.
sync(Server, ShortLong) when ShortLong == shortterm; ShortLong == longterm ->
    gen_server:call(Server, {sync, ShortLong}, ?TIMEOUT).

get_proplist(Server) ->
    gen_server:call(Server, {get_proplist}, ?TIMEOUT).

-spec read_bigblob_hunk_blob(seqnum(), offset()) -> no_return().
read_bigblob_hunk_blob(_SeqNum, _Offset) ->
    exit({badarg, ?MODULE}). % Only makes sense for gmt_hlog_local.erl

-spec read_bigblob_hunk_blob(seqnum(), offset(), checkmd5()) -> no_return().
read_bigblob_hunk_blob(_SeqNum, _Offset, _CheckMD5_p) ->
    exit({badarg, ?MODULE}). % Only makes sense for gmt_hlog_local.erl

%% @doc See docs for gmt_hlog_local:read_bigblob_hunk_blob/3.

-spec read_bigblob_hunk_blob(dirname(), seqnum(), offset(), checkmd5()) ->
                                    #hunk_summ{} | eof | {error, term()}.
read_bigblob_hunk_blob(Dir, SeqNum, Offset, CheckMD5_p) ->
    read_bigblob_hunk_blob(Dir, SeqNum, Offset, CheckMD5_p, 0).

-spec read_bigblob_hunk_blob(dirname(), seqnum(), offset(), checkmd5(), lenhint()) ->
                                    #hunk_summ{} | eof | {error, term()}.
read_bigblob_hunk_blob(Dir, SeqNum, Offset, CheckMD5_p, ValLen) ->
    Fun = fun(Su, FH) ->
                  Blob = read_hunk_member_ll(FH, Su, md5, 1),
                  Su2 = Su#hunk_summ{c_blobs = [Blob]},
                  if CheckMD5_p ->
                          true = md5_checksum_ok_p(Su2);
                     true ->
                          ok
                  end,
                  Blob
          end,
    read_hunk_summary(Dir, SeqNum, Offset, ValLen, Fun).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

%% @doc Gen_server init.
%%
%% NOTE: Due to inter-operability assumptions with gmt_hlog_common
%%       and gmt_hlog_local, I'm eliminating the feature that a hunk
%%       log could be placed in any arbitrary directory.  All three
%%       modules now assume that the top-level directory of all hunk
%%       logs will be located in the VM's current working directory.
%%
%% Property list options:
%% {name, Name::atom()}
%% {file_len_max, Bytes::integer()}
%% {file_len_min, Bytes::integer()}
%% {long_h1_size, Num::integer()}
%% {long_h2_size, Num::integer()}

init(PropList) ->
    process_flag(priority, high),
    Name = proplists:get_value(name, PropList, foofoo),
    RegName = log_name2reg_name(Name),
    register(RegName, self()),

    %% Variable properties: may change once a log dir is created.
    undefined = proplists:get_value(dir, PropList), % sanity: avoid old-style!
    Dir = log_name2data_dir(Name),
    catch file:make_dir(Dir),
    catch file:make_dir(Dir ++ "/s"), % short term file path
    LenMax = proplists:get_value(file_len_max, PropList, 64*1024*1024),
    LenMin = proplists:get_value(file_len_min, PropList, LenMax),
    WriteSleep = proplists:get_value(debug_write_sleep, PropList, 0),
    SyncSleep = proplists:get_value(debug_sync_sleep, PropList, 0),

    %% Permanent properties: cannot change once a log dir is created.
    _PermProps = case file:read_file_info(perm_config_file(Dir)) of
                     {ok, _} ->
                         {ok, Bin} = file:read_file(perm_config_file(Dir)),
                         binary_to_term(Bin);
                     _ ->
                         PropList
                 end,
    %%H1_Size = proplists:get_value(long_h1_size, PermProps, 3),
    %%H2_Size = proplists:get_value(long_h2_size, PermProps, 7),
    H1_Size = ?ARCH_H1_SIZE,
    H2_Size = ?ARCH_H2_SIZE,
    create_longterm_dirs(Dir, H1_Size, H2_Size),

    %% Save the permanent properties.
    Configs = [{long_h1_size, H1_Size}, {long_h2_size, H2_Size}],
    ok = write_permanent_config_maybe(Dir, Configs),

    %% Find run-time values.
    LastSeq = find_mostest_sequence_num(Dir, H1_Size, H2_Size),
    {CurSeqS, CurSeqL} = {LastSeq + 1, -(LastSeq + 2)},
    {CurFHS, CurOffS} = create_new_log_file(Dir, CurSeqS),
    {CurFHL, CurOffL} = create_new_log_file(Dir, CurSeqL),

    {ok, #state{props = PropList, dir = Dir, name = Name,
                file_len_max = LenMax,
                file_len_min = LenMin,
                long_h1_size = H1_Size, long_h2_size = H2_Size,
                debug_write_sleep = WriteSleep, debug_sync_sleep = SyncSleep,
                last_seq = LastSeq + 2,
                cur_seqS = CurSeqS, cur_fhS = CurFHS, cur_offS = CurOffS,
                cur_seqL = CurSeqL, cur_fhL = CurFHL, cur_offL = CurOffL}}.

%%----------------------------------------------------------------------
%% Func: handle_call/3
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_call({write_hunk_bytes, _LocalBrickName, HLogType, _Key,
             TypeNum, H_Len, H_Bytes}, _From, S) ->
    Start1 = now(),
    {Reply, NewS} = do_write_hunk(HLogType, TypeNum, H_Len, H_Bytes, S),
    End1 = now(),
    case timer:now_diff(End1, Start1) div 1000 of
        N when N > 50 ->
            ?ELOG_INFO("Write to ~p took ~p ms", [S#state.dir, N]);
        _ ->
            ok
    end,
    {reply, Reply, NewS};
handle_call({read_hunk, SeqNum, Offset, LenHint, XFormFunc}, From, S) ->
    spawn(fun() ->
                  R = (catch read_hunk_summary(S#state.dir, SeqNum, Offset,
                                               LenHint, XFormFunc)),
                  gen_server:reply(From, R)
          end),
    {noreply, S};
handle_call({move_seq_to_longterm, SeqNum}, _From, S) ->
    {Reply, NewS} = do_move_seq_to_longterm(SeqNum, S),
    {reply, Reply, NewS};
handle_call({sync, shortterm}, From, State) ->
    NewState = do_sync_asyncly(From, State),
    {noreply, NewState};
handle_call({sync, longterm}, From, State) ->
    do_sync_longterm_asyncly(From, State),
    {noreply, State};
handle_call({get_proplist}, _From, State) ->
    {reply, State#state.props, State};
handle_call({get_current_seqnum}, _From, State) ->
    {reply, State#state.cur_seqS, State};
handle_call({get_current_seqnum_and_offset}, _From, State) ->
    {reply, {State#state.cur_seqS, State#state.cur_offS}, State};
handle_call({get_current_seqnum_and_file_position}, _From, State) ->
    {ok, FilePos} = file:position(State#state.cur_fhS, cur),
    {reply, {State#state.cur_seqS, FilePos}, State};
handle_call({get_all_seqnums}, From, State) ->
    spawn(fun() -> Res = find_current_log_seqnums(State#state.dir) ++
                       find_longterm_log_seqnums(State#state.dir,
                                                 State#state.long_h1_size,
                                                 State#state.long_h2_size),
                   gen_server:reply(From, Res)
          end),
    {noreply, State};
handle_call({advance_seqnum, Incr}, From, State) ->
    NewState = do_advance_seqnum(Incr, From, State),
    {noreply, NewState};
handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    ?ELOG_ERROR("~p ~p", [_Request, _From]),
    {reply, 'wha?????', State}.

%%----------------------------------------------------------------------
%% Func: handle_cast/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%----------------------------------------------------------------------
%% Func: handle_info/2
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%----------------------------------------------------------------------
handle_info({'DOWN', _Ref, _, Pid, Reason} = _Msg, State)
  when Pid == State#state.syncer_pid ->
    {noreply, asyncly_done(Pid, Reason, State)};
handle_info(_Info, State) ->
    ?ELOG_INFO("~p got ~p", [State#state.dir, _Info]),
    {noreply, State}.

%%----------------------------------------------------------------------
%% Func: terminate/2
%% Purpose: Shutdown the server
%% Returns: any (ignored by gen_server)
%%----------------------------------------------------------------------
terminate(_Reason, S) ->
    write_shortterm_backlog(S, false),
    catch safe_log_close(S),
    ok.

%%----------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%%----------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

%% @spec (string(), term()) -> ok | file_exists
%% @doc Write a permanent config file, if it does not already exist.
%%
%% NOTE: We aren't safe against race conditions, but the VM doesn't expose
%% O_EXCL {sigh}.

write_permanent_config_maybe(Dir, Term) ->
    PermConfig = perm_config_file(Dir),
    case file:read_file_info(PermConfig) of
        {ok, _} ->
            ok;
        _ ->
            ok = file:write_file(PermConfig, term_to_binary(Term))
    end.

%% @spec (string()) -> string()
%% @doc Return the name of the log's permanent config file.

perm_config_file(Dir) ->
    Dir ++ "/s/Config".

%% @spec (string()) -> string()
%% @doc Return the name of the log's permanent config file's old path.

old_perm_config_file(Dir) ->
    Dir ++ "/Config".

%% @spec (string(), integer(), integer()) -> integer()
%% @doc Return the sequence number for the "mostest" log file: the
%%      largest abs(SeqNum), so we look in both the current and
%%      longterm areas.

find_mostest_sequence_num(Dir, H1, H2) ->
    Short = find_mostest_current_sequence_num(Dir),
    Long = find_mostest_longterm_sequence_num(Dir, H1, H2),
    if Short > abs(Long) -> Short;
       true              -> abs(Long)
    end.

find_mostest_current_sequence_num(Dir) ->
    case find_current_log_seqnums(Dir) of
        [] ->
            0;
        Nums1 ->
            lists:last(Nums1)
    end.

find_mostest_longterm_sequence_num(Dir, H1, H2) ->
    case find_longterm_log_seqnums(Dir, H1, H2) of
        [] ->
            0;
        Nums2 ->
            lists:last(Nums2)
    end.

%% @spec (string()) -> list(string())

-spec find_current_log_files(dirname()) -> list(dirname()).
find_current_log_files(Dir) ->
    filelib:wildcard("*.HLOG", Dir ++ "/s").

%% @spec (string(), integer(), integer()) -> list(string())

find_longterm_log_files(Dir, H1, H2) ->
    lists:append([filelib:wildcard("*.HLOG", longterm_dir(Dir, N1, N2)) ||
                     N1 <- lists:seq(1, H1), N2 <- lists:seq(1, H2)]).

%% @spec (string()) -> list(integer())
%% @doc Return a sorted list of all sequence numbers in the current log area.

-spec find_current_log_seqnums(dirname()) -> list(seqnum()).
find_current_log_seqnums(Dir) ->
    [file2seqnum(F) || F <- find_current_log_files(Dir)].

%% @spec (string(), integer(), integer()) -> list(integer())
%% @doc Return a sorted list of all sequence numbers in the longterm log area.

-spec find_longterm_log_seqnums(dirname(), seqnum(), seqnum()) -> list(seqnum()).
find_longterm_log_seqnums(Dir, H1, H2) ->
    Files = find_longterm_log_files(Dir, H1, H2),
    lists:sort([abs(file2seqnum(F)) || F <- Files]).

%% @spec (file:fd()) -> term()
-spec write_log_header(file:fd()) -> ok | {error, term()}.
write_log_header(FH) ->
    file:write(FH, file_header_version_1()).   % Header for version #1 file

%% @spec (shortterm | longterm, integer(), blobs(), blobs(), state_r()) ->
%%       {term(), state_r()}

do_write_hunk(HLogType, _TypeNum, H_Len, H_Bytes,
              #state{file_len_max=FileLenMax, file_len_min=FileLenMin}=S) ->
    debug_sleep(S#state.debug_write_sleep),
    H_Len = iolist_size(H_Bytes), % TODO: This sanity check isn't needed??
    %% ASSUMPTION: file_len_max is larger than 32
    if H_Len > (FileLenMax - 32) -> % TODO: remove fudge factor!
            {{hunk_too_big, H_Len}, S};
       true ->
            LongTermP = HLogType =:= bigblob_longterm,
            {SyncNoProblemP, Incr, CurOffset} =
                if LongTermP ->
                        {true,
                         -1, S#state.cur_offL};
                   true ->
                        {S#state.write_backlog =:= [] andalso S#state.syncer_pid =:= undefined,
                         1, S#state.cur_offS}
                end,
            S2 = if SyncNoProblemP ->
                         S1 = if (H_Len + CurOffset) > FileLenMin ->
                                      do_advance_seqnum(Incr, undefined, S);
                                 true ->
                                      S
                              end,
                         {MyFH, MyOffset} =
                             if LongTermP ->
                                     {S1#state.cur_fhL, S1#state.cur_offL};
                                true ->
                                     {S1#state.cur_fhS, S1#state.cur_offS}
                             end,
                         %% ?DBG_GEN("do_write_hunk: hlog type ~w, bytes ~w (grep tag=sync)",
                         %%          [HLogType, H_Len]),
                         {ok, MyOffset0} = file:position(MyFH, cur), %BZDEBUG
                         if MyOffset =/= MyOffset0 ->
                                 erlang:error({prewrite, MyOffset, MyOffset0, H_Len});
                            true ->
                                 noop
                         end,
                         ok = file:write(MyFH, H_Bytes),
                         NewOffsetA = MyOffset + H_Len,              %BZDEBUG
                         {ok, NewOffsetB} = file:position(MyFH, cur),%BZDEBUG
                         if NewOffsetA =/= NewOffsetB ->
                                 erlang:error({postwrite, NewOffsetA, NewOffsetB, H_Len});
                            true ->
                                 noop
                         end,
                         S1;
                    true ->
                         ?DBG_GEN("do_write_hunk [sync_problem] hlog type ~w, bytes ~w (grep tag=sync)",
                                  [HLogType, H_Len]),
                         Wb = S#state.write_backlog,
                         S#state{write_backlog = [H_Bytes|Wb]}
                 end,
            if LongTermP ->
                    Reply = {ok, S2#state.cur_seqL, S2#state.cur_offL},
                    {Reply, S2#state{cur_offL =  S2#state.cur_offL + H_Len}};
               true ->
                    Reply = {ok, S2#state.cur_seqS, S2#state.cur_offS},
                    {Reply, S2#state{cur_offS =  S2#state.cur_offS + H_Len}}
            end
    end.

%% @doc Create a hunk.
%%      CBlobs: A list of blobs that also has an MD5 checksum stored inside the hunk.
%%      UBlobs: A list of blobs without an MD5 checksum.
-spec create_hunk(typenum(), CBlobs::blobs(), UBlobs::blobs()) -> {len(), bytes()}.
create_hunk(TypeNum, CBlobs, UBlobs) ->
    Hdr = hunk_header_v1(),
    MD5s = case get(disable_md5_hack) of
               no ->
                   [do_md5(Bl) || Bl <- CBlobs];
               yes ->
                   [];
               undefined ->
                   File = "./disable-md5",
                   case (catch file:read_file(File)) of
                       {ok, _} ->
                           ?E_WARNING("~s: MD5 DISABLED by ~p",
                                      [?MODULE, File]),
                           put(disable_md5_hack, yes),
                           [];
                       _ ->
                           put(disable_md5_hack, no),
                           [do_md5(Bl) || Bl <- CBlobs]
                   end
           end,
    LenAllCB = [gmt_util:io_list_len(Bl) || Bl <- CBlobs],
    LenAllUB = [gmt_util:io_list_len(Bl) || Bl <- UBlobs],
    LenAll = my_sum(LenAllCB ++ LenAllUB),
    T = {MD5s, LenAllCB, LenAllUB},
    BinT = term_to_binary(T),
    LenBinT = size(BinT),
    Hunk_Len = size(Hdr) + 4 + 4 + 4 + LenBinT + LenAll,
    %% TODO: In an ideal world, we should be checking ?MAX_HUNK_HEADER_BYTES
    %%       here.  But throwing an exception here can cause weird
    %%       problems, such as bootstrapping a single simple developer
    %%       node can fail.
    Hunk = [<<Hdr/binary, Hunk_Len:32, LenBinT:32, TypeNum:32, BinT/binary>>,
            CBlobs, UBlobs],
    {Hunk_Len, Hunk}.


%% YIKES, I was bitten by a really pernicious bug.  Took about 6 hours
%% to find.  Recall that we're now buffering write_hunk requests when
%% an fsync is in progress.  Well, the fsync isn't done yet, and then
%% we get a request to advance the sequence number, *if* we advance
%% the seqnum immediately and open a new hunk file, then something
%% very bad happens: all of the buffered writes that we said would be
%% written in hunk file SeqNum will instead be written in hunk file
%% SeqNum+N when the fsync is finally finished.
%%
%% Solution: If we get an advance request while a syncer_pid is
%% active, we buffer that request also.  As a consequence, if a
%% syncer_pid is not active, we need to call gen_server:reply()
%% ourself.
%%
%% If we're in the middle of a sync, do not queue an advance seqnum
%% request onto syncer_advance_reqs if Incr < 0: that's for longterm
%% storage, which isn't affected by the shortterm sync procedure.

do_advance_seqnum(Incr, From, #state{syncer_pid = SyncPid,
                                     write_backlog = Wbacklog} = S)
  when (Wbacklog =:= [] andalso SyncPid =:= undefined) orelse (Incr < 0) ->
    NextSeq = S#state.last_seq + abs(Incr),
    NewSeq = if Incr > 0 -> ok = safe_log_close_short(S),
                            NextSeq;
                Incr < 0 -> ok = safe_log_close_long(S),
                            -NextSeq
             end,
    {FH, Off} = create_new_log_file(S#state.dir, NewSeq),
    if From =:= undefined ->
            ok;
       true ->
            gen_server:reply(From, {ok, NewSeq})
    end,
    if Incr > 0 ->
            S#state{last_seq = NextSeq,
                    cur_seqS = NewSeq, cur_fhS = FH, cur_offS = Off};
       Incr < 0 ->
            S#state{last_seq = NextSeq,
                    cur_seqL = NewSeq, cur_fhL = FH, cur_offL = Off}
    end;
do_advance_seqnum(Incr, From, #state{syncer_advance_reqs = Rs} = S) ->
    S#state{syncer_advance_reqs = [{Incr, From}|Rs]}.

%% @spec (file:fd(), integer()) ->
%%       {ok, hunk_summ_r()} | eof | {bof, integer} | {error, term()}

-spec read_hunk_summ_ll(file:fd(), offset()) ->
                               {ok, #hunk_summ{}} | eof | {bof, offset()} | {error, term(), term()}.
read_hunk_summ_ll(FH, 0) ->
    my_pread_start(),
    Hdr1 = file_header_version_1(),
    Hdr1Len = gmt_util:io_list_len(Hdr1),
    case my_pread(FH, 0, Hdr1Len, 0) of
        {ok, X} when X == Hdr1 ->
            {bof, Hdr1Len};
        _X ->
            {error, unknown_file_header, _X}
    end;
read_hunk_summ_ll(FH, Offset) ->
    my_pread_start(),
    read_hunk_summ_ll(FH, Offset, 0).

-spec read_hunk_summ_ll(file:fd(), offset(), lenhint()) ->
                               {ok, #hunk_summ{}} | eof | {bof, offset()} | {error, term(), term()}.
read_hunk_summ_ll(FH, Offset, HunkLenHint) ->
    my_pread_start(),
    read_hunk_summ_ll_middle(FH, Offset, HunkLenHint).

read_hunk_summ_ll_middle(FH, Offset, HunkLenHint) ->
    ReadAheadBytes = HunkLenHint + ?MY_PREAD_MODEST_READAHEAD_BYTES,
    read_hunk_summ_ll2(my_pread(FH, Offset, ?MAX_HUNK_HEADER_BYTES,
                                ReadAheadBytes),
                       FH, Offset).

read_hunk_summ_ll2({ok, Bin}, FH, Offset) ->
    Hdr = hunk_header_v1(),
    case Bin of
        <<Foo:4/binary, _/binary>> when Foo == Hdr -> % !@#$! compiler warning
            <<_:4/binary, HunkLen:32, LenBinT:32, TypeNum:32,
              Rest/binary>> = Bin,
            HdrLen = gmt_util:io_list_len(Hdr),
            First_off = HdrLen + 4 + 4 + 4 + LenBinT,
            if LenBinT > size(Rest) ->
                    my_pread_iter(),
                    %% Use middle to avoid resetting the my_pread loop counter
                    read_hunk_summ_ll_middle(FH, Offset, First_off);
               true ->
                    <<BinT:LenBinT/binary, _/binary>> = Rest,
                    {MD5s, LenAllCB, LenAllUB} = binary_to_term(BinT),
                    {ok, #hunk_summ{len = HunkLen, type = TypeNum,
                                    c_len = LenAllCB, u_len = LenAllUB,
                                    md5s = MD5s,
                                    first_off = First_off}}
            end;
        _X ->
            {error, bad_hunk_header, _X}
    end;
read_hunk_summ_ll2(Result, _FH, _Offset) ->
    Result. %% eof, {error, loop_broken}, ...

%% TODO: Finish support for partial blob reading, started here
%%       in early July 2009.
-spec read_hunk_member_ll(file:fd(), #hunk_summ{}, md5 | undefined, nth()) -> binary().
read_hunk_member_ll(FH, Summ, MemberType, MemberNum) ->
    read_hunk_member_ll(FH, Summ, MemberType, MemberNum, 0).

-spec read_hunk_member_ll(file:fd(), #hunk_summ{}, md5 | undefined, nth(), offset()) -> binary().
read_hunk_member_ll(FH, Summ, md5, 1, ExtraOffset)
  when length(Summ#hunk_summ.c_len) =:= 1 ->
    ByteOff = Summ#hunk_summ.off + Summ#hunk_summ.first_off,
    [ByteLen] = Summ#hunk_summ.c_len,
    {ok, Bin} = my_pread(FH, ByteOff + ExtraOffset, ByteLen - ExtraOffset, 0),
    Bin;
read_hunk_member_ll(FH, Summ, MemberType, MemberNum, ExtraOffset) ->
    Nth = if MemberType == md5 ->
                  MemberNum;
             true ->
                  length(Summ#hunk_summ.c_len) + MemberNum
          end,
    AllLens = Summ#hunk_summ.c_len ++ Summ#hunk_summ.u_len,
    NthOff = lists:foldl(fun(X, Sum) -> X + Sum end,
                         0, lists:sublist(AllLens, Nth - 1)),
    ByteOff = Summ#hunk_summ.off + Summ#hunk_summ.first_off + NthOff,
    ByteLen = lists:nth(Nth, AllLens),
    {ok, Bin} = my_pread(FH, ByteOff + ExtraOffset, ByteLen - ExtraOffset, 0),
    Bin.

%% @spec (string() | file:fd(), integer(), integer()) ->
%%       hunk_summ_r() | eof | {error, term()}

-spec read_hunk_summary(dirname(), seqnum(), offset()) -> #hunk_summ{} | eof | {error, term()};
                       (file:fd(), seqnum_unused, offset()) -> #hunk_summ{} | eof | {error, term()}.
read_hunk_summary(Dir, N, Offset) when is_list(Dir) ->
    read_hunk_summary(Dir, N, Offset, 0, fun(Summ, _FH) -> Summ end).

%% @spec (string() | file:fd(), integer(), integer(), integer(), fun()) ->
%%       hunk_summ_r() | eof | {error, term()}

read_hunk_summary(Dir, N, Offset, HunkLenHint, XformFun)
  when is_list(Dir), is_function(XformFun, 2) ->
    case open_log_file(Dir, N, [read]) of
        {ok, FH} ->
            try
                read_hunk_summary(FH, N, Offset, HunkLenHint, XformFun)
            catch
                X:Y ->
                    ?E_ERROR("Error: ~p ~p at ~p ~p for ~p bytes:~p",
                             [X, Y, N, Offset, HunkLenHint, erlang:get_stacktrace()]),
                    {error, {X, Y}}
            after
                ok = file:close(FH)
            end;
        Err ->
            Err
    end;
read_hunk_summary(FH, N, Offset, HunkLenHint, XformFun) ->
    case read_hunk_summ_ll(FH, Offset, HunkLenHint) of
        {ok, H} ->
            XformFun(H#hunk_summ{seq = N, off = Offset}, FH);
        {bof, NewOffset} ->
            read_hunk_summary(FH, N, NewOffset, HunkLenHint, XformFun);
        eof ->
            eof;
        Err ->
            Err
    end.

-spec parse_hunk_summary(iodata()) -> hunk_summ() | too_big.
parse_hunk_summary(L) when is_list(L) ->
    parse_hunk_summary(list_to_binary(L));
parse_hunk_summary(Bin) ->
    Hdr = <<(16#feedbeef):32>>,  %% gmt_hlog:hunk_header_v1()
    case Bin of
        <<Hdr2:4/binary, HunkLen:32, LenBinT:32, TypeNum:32, Rest/binary>>
          when Hdr =:= Hdr2 ->
            HdrLen = gmt_util:io_list_len(Hdr),
            FirstOff = HdrLen + 4 + 4 + 4 + LenBinT,
            if LenBinT > size(Rest) ->
                    too_big;
               true ->
                    <<BinT:LenBinT/binary, _/binary>> = Rest,
                    {MD5s, LenAllCB, LenAllUB} = binary_to_term(BinT),
                    #hunk_summ{len=HunkLen,
                               type=TypeNum,
                               c_len=LenAllCB,
                               u_len=LenAllUB,
                               md5s=MD5s,
                               first_off=FirstOff
                              }
            end;
        _ ->
            error(invalid_hunk_summary)
    end.

-spec get_hunk_cblob_member(hunk_summ(), iodata(), non_neg_integer()) -> binary().
get_hunk_cblob_member(HunkSummary, CBlob, MemberNum)
  when is_list(CBlob) ->
    get_hunk_cblob_member(HunkSummary, list_to_binary(CBlob), MemberNum);
get_hunk_cblob_member(#hunk_summ{c_len=[ByteLen]}, CBlob, 1) ->
    binary:part(CBlob, 0, ByteLen);
get_hunk_cblob_member(#hunk_summ{c_len=CLens}, CBlob, MemberNum) ->
    [Len1, Len2] = lists:nthtail(MemberNum, [0 | CLens]),
    Offset = lists:sum(Len1),
    Len = hd(Len2),
    binary:part(CBlob, Offset, Len).

do_move_seq_to_longterm(SeqNum, S) when SeqNum >= S#state.cur_seqS ->
    {{error, S#state.cur_seqS}, S};
do_move_seq_to_longterm(SeqNum, S) ->
    ShortLog = log_file_path(S#state.dir, SeqNum),
    LongLog = log_file_path(S#state.dir, -SeqNum),
    Res =
        case file:read_file_info(LongLog) of
            {error, enoent} ->
                case file:rename(ShortLog, LongLog) of
                    ok -> ok;
                    _  -> error
                end;
            _ ->
                error
        end,
    {Res, S}.

%% LTODO: This error handling sucks.  Common success is great, of
%% course.  But if we hit an error, it's due to bug (unknown hunk
%% type, a good way of testing!) or data corruption on disk.  It would
%% be really good to try to continue reading the file, if at all
%% possible.

fold2(ShortLong, Dir, Fun, FiltFun, InitialAcc) ->
    Hdr = hunk_header_v1(),
    List = if ShortLong == shortterm ->
                   find_current_log_seqnums(Dir);
              ShortLong == longterm ->
                   find_longterm_log_seqnums(Dir, ?ARCH_H1_SIZE, ?ARCH_H2_SIZE)
           end,
    SeqNums = lists:filter(FiltFun, List),
    ?DBG_TLOG("fold2: ~w, ~s ~w", [ShortLong, Dir, SeqNums]),
    lists:foldl(
      fun(N, {Acc00, ErrList}) ->
              {ok, FH} = open_log_file(Dir, N, [read]),
              try
                  put(fold_seq, N),
                  put(fold_off, 0),
                  {{ok, _Path, FI}, _, _} = {log_file_info(Dir, N), Dir, N},
                  if FI#file_info.size >= size(Hdr) ->
                          Res = fold_a_file(FH, FI#file_info.size, N, Fun, Acc00),
                          ?DBG_TLOG("fold2: done (~w) -> ~w", [N, Res]),
                          {Res, ErrList};
                     true ->
                          {Acc00, ErrList}
                  end
              catch
                  X:Y ->
                      Seq = get(fold_seq),
                      Off = get(fold_off),
                      ?DBG_TLOG("fold2: error ~w:~w seq ~w off ~w", [X, Y, Seq, Off]),
                      fold_warning("WAL fold of dir ~s sequence ~w offset ~w. "
                                   "error ~p:~p ~p",
                                   [Dir, Seq, Off, X, Y, erlang:get_stacktrace()]),
                      Err = {seq, N, err, Y},
                      {Acc00, [Err|ErrList]}
              after
                  ok = file:close(FH)
              end
      end, {InitialAcc, []}, SeqNums).

fold_warning(Fmt, Args) ->
    case get(qc_verbose_hack_hlog_qc) of
        undefined ->
            ?E_WARNING(Fmt, Args);
        _ ->
            ok
    end.

-spec fold_a_file(file:fd(), len(), seqnum(), offset(), foldfun(), foldacc()) -> foldret().
fold_a_file(FH, LogSize, SeqNum, Fun, Acc) ->
    fold_a_file(FH, LogSize, SeqNum, 0, Fun, Acc).

fold_a_file(FH, LogSize, SeqNum, Offset, Fun, Acc) ->
    case read_hunk_summ_ll(FH, Offset) of
        {ok, H} ->
            H2 = H#hunk_summ{seq = SeqNum, off = Offset},
            put(fold_seq, SeqNum),
            put(fold_off, Offset),
            %% !@#$!%! Why doesn't try ... catch throw:{new_offset,X} work??
            %%           {Acc2, NewOff} = try
            %%                                {Fun(H2, FH, Acc),
            %%                                 H2#hunk_summ.off + H2#hunk_summ.len}
            %%                            catch
            %%                                _X:{new_offset, X} ->
            %%                                    io:format("CAUGHT ~p", [X]),
            %%                                    {Acc, X}
            %%                            end,
            {Acc2, NewOffset} = case Fun(H2, FH, Acc) of
                                 {{{new_offset, NewO}}} ->
                                     {Acc, NewO};
                                 Aa ->
                                     {Aa, H2#hunk_summ.off + H2#hunk_summ.len}
                             end,
            %% These two "if" tests are for sanity purposes.
            %% TODO: Failures here are probably legit.  But it's possible
            %%       that they could screw up brick startup if the disks
            %%       scribbled some nonsense at the end of the log after
            %%       a power failure.
            if NewOffset >= LogSize ->
                    %% Two things have happened:
                    %%   1. An incomplete write at the end of the file.
                    %%   2. The file is being actively written to (e.g.
                    %%      CommonLog case).
                    %% In either case, it's OK to stop here.
                    ?DBG_TLOG("fold_a_file: seq ~w, too_big, new_offset ~w, log_size ~w",
                              [SeqNum, NewOffset, LogSize]),
                    Acc2;
               true ->
                    AllBSize = lists:sum(H2#hunk_summ.c_len ++ H2#hunk_summ.u_len),
                    if (Offset + H2#hunk_summ.first_off + AllBSize) > LogSize ->
                            throw({bad_c_len_u_len});
                       true ->
                            ok
                    end,
                    fold_a_file(FH, LogSize, SeqNum, NewOffset, Fun, Acc2)
            end;
        {bof, NewOffset} ->
            ?DBG_TLOG("fold_a_file: seq ~w, bof, new_offset ~w", [SeqNum, NewOffset]),
            fold_a_file(FH, LogSize, SeqNum, NewOffset, Fun, Acc);
        eof ->
            ?DBG_TLOG("fold_a_file: seq ~w, eof", [SeqNum]),
            Acc;
        Err ->
            ?E_ERROR("fold_a_file: seq ~p offset ~p: ~p", [SeqNum, Offset, Err]),
            throw(Err)
    end.

%% @spec (string()) -> integer()

file2seqnum(LogNumPlusSuffix) ->
    {N, _} = string:to_integer(LogNumPlusSuffix),
    N.

seqnum2file(N, Suffix) when N >= 0 ->
    gmt_util:left_pad(integer_to_list(N), 12, $0) ++ "." ++ Suffix;
seqnum2file(N, Suffix) ->
    "-" ++ gmt_util:left_pad(integer_to_list(abs(N)), 12, $0) ++ "." ++ Suffix.

%% @spec (string(), integer(), proplist()) ->
%%       {ok, file:fd()} | {error, term()}

-spec open_log_file(dirname(), seqnum(), openmode()) -> {ok, file:fd()} | {error, atom()}.
open_log_file(Dir, N, Options) ->
    Path = log_file_path(Dir, N),
    case file:open(Path, [binary, raw|Options]) of
        {error, enoent} when N > 0 ->
            %% Retry: perhaps this file was moved to long-term storage?
            open_log_file(Dir, -N, Options);
        {ok, _}=Res ->
            Res;
        Res ->
            ?E_CRITICAL("Couldn't open log file ~w ~s by ~w", [Options, Path, Res]),
            Res
    end.

%% @doc
%% Note: This function is only used for creating a common hlog file.
%% For creating a local hlog file, check the following functions:
%% - gmt_hlog_common:write_back_to_local_log/8
%%   * gmt_hlog_common:open_log_file_mkdir/3
%%   * gmt_hlog_common:check_hlog_header/1
%% - brick_ets:checkpoint_start/4
%%   * file:rename/2

-spec create_new_log_file(dirname(), seqnum()) ->
                                 {file:fd(), Size::non_neg_integer()} | no_return().
create_new_log_file(Dir, N) when is_integer(N), N =/= 0 ->
    Path = log_file_path(Dir, N),
    %% sanity
    {N, {error, enoent}} = {N, file:read_file_info(Path)},
    %% extra sanity
    M = -N,
    {M, {error, enoent}} = {M, file:read_file_info(log_file_path(Dir, M))},
    {ok, FH} = open_log_file(Dir, N, [read, write]),
    try
        ok = write_log_header(FH),
        {ok, FI} = stat_log_file(Dir, N),
        Type = if N > 0 -> "short-term";
                  true ->  "long-term"
               end,
        ?E_INFO("Created ~s log file with sequence ~w: ~s",
                [Type, N, Path]),
        {FH, FI#file_info.size}
    catch
        X:Y ->
            %% cleanup
            ok = file:close(FH),
            %% exit with a similiar error
            erlang:error({X,Y})
    end.

%% @spec (string(), integer) -> {ok, file_info_r()} | term()

stat_log_file(Dir, N) ->
    file:read_file_info(log_file_path(Dir, N)).

-spec log_file_path(dirname(), seqnum()) -> filepath().
log_file_path(Dir, N) ->
    log_file_path(Dir, N, "HLOG").

-spec log_file_path(dirname(), seqnum(), string()) -> filepath().
log_file_path(Dir, N, Suffix) when N > 0 ->
    short_path(Dir ++ "/s", N, Suffix);
log_file_path(Dir, N, Suffix) when N < 0 ->
    {N1, N2} = hash_longterm_seqnum(N),
    short_path(longterm_dir(Dir, N1, N2), N, Suffix).

%% @spec (string(), integer()) ->
%%       {ok, string(), file:fd()} | {error, term()}

-spec log_file_info(dirname(), seqnum()) -> {ok, filepath(), file:file_info()} | {error, atom()}.
log_file_info(Dir, N) ->
    Path = log_file_path(Dir, N),
    case file:read_file_info(Path) of
        {ok, FI} ->
            {ok, Path, FI};
        {error, enoent} when N > 0 ->
            %% Retry: perhaps this file was moved to long-term storage?
            log_file_info(Dir, -N);
        Res ->
            Res
    end.

hash_longterm_seqnum(N) ->
    {(abs(N) rem ?ARCH_H1_SIZE) + 1, (abs(N) rem ?ARCH_H2_SIZE) + 1}.

short_path(Dir, N, Suffix) ->
    Dir ++ "/" ++ seqnum2file(N, Suffix).

longterm_dir(Dir, N1, N2) ->
    Dir ++ "/" ++ integer_to_list(N1) ++ "/" ++ integer_to_list(N2).

-spec file_header_version_1() -> <<_:96>>.
file_header_version_1() ->
    <<"LogVersion1\n">>.

my_sum([X]) ->
    X;
my_sum(L) ->
    lists:foldl(fun(X, Sum) -> X + Sum end, 0, L).

my_pread_start() ->
    put(pread_loop, 0).

my_pread_iter() ->
    put(pread_loop, get(pread_loop) + 1).

my_pread(FH, Offset, Len, LenExtra) when Len >= 0 ->
    case get(pread_loop) of
        N when N < 5 -> my_pread2(FH, Offset, Len, LenExtra);
        _            -> {error, loop_broken}
    end;
my_pread(FH, Offset, Len, LenExtra) when Len < 0 ->
    my_pread(FH, Offset, 0, LenExtra).

my_pread2(FH, Offset, Len, LenExtra) ->
    case get(pread_hack) of
        {FH, _BinOff, eof} ->
            _ = my_pread_miss(FH, Offset, Len + LenExtra),
            case get(pread_hack) of % This Offset is probably not the old one.
                {_, _, eof} ->
                    eof; % Avoid infinite loop: new offset is also at/past eof.
                _ ->
                    my_pread(FH, Offset, Len, 0)
            end;
        {FH, BinOff, {ok, Bin}} ->
            BinLen = size(Bin),
            if Offset >= BinOff andalso Offset+Len =< BinOff+BinLen ->
                    PrefixLen = Offset - BinOff,
                    <<_:PrefixLen/binary, B:Len/binary, _/binary>> = Bin,
                    {ok, B};
               true ->
                    case my_pread_miss(FH, Offset, Len + LenExtra) of
                        {ok, Bin} when is_binary(Bin) ->
                            BinSize = size(Bin),
                            if BinSize >= Len ->
                                    <<Bin2:Len/binary, _/binary>> = Bin,
                                    {ok, Bin2};
                               BinSize < Len ->
                                    {ok, Bin};
                               true ->
                                    {ok, Bin}
                            end;
                        Err ->
                            Err
                    end
            end;
        undefined ->
            _ = my_pread_miss(FH, Offset, Len + LenExtra),
            my_pread(FH, Offset, Len, 0);
        {OtherFH, _, _} when OtherFH /= FH ->
            _ = my_pread_miss(FH, Offset, Len + LenExtra),
            my_pread(FH, Offset, Len, 0);
        {_, _, Res} ->
            Res
    end.

my_pread_miss(FH, Offset, Len) ->
    erase(pread_hack),
    AheadLen = 0,
    HackLen = AheadLen + Len,
    X = file:pread(FH, Offset, HackLen),
    put(pread_hack, {FH, Offset, X}),
    X.

-spec safe_log_close(state_r()) -> ok | {error, term()}.
safe_log_close(S) ->
    safe_log_close_short(S),
    safe_log_close_long(S).

-spec safe_log_close_short(state_r()) -> ok | {error, term()}.
safe_log_close_short(#state{dir=Dir, cur_seqS=SeqNum, cur_fhS=FH}) ->
    safe_log_close0("short-term", Dir, SeqNum, FH).

-spec safe_log_close_long(state_r()) -> ok | {error, term()}.
safe_log_close_long(#state{dir=Dir, cur_seqL=SeqNum, cur_fhL=FH}) ->
    safe_log_close0("long-term", Dir, SeqNum, FH).

-spec safe_log_close0(string(), dirname(), seqnum(), file:handle()) -> ok | {error, term()}.
safe_log_close0(Type, Dir, SeqNum, FH) ->
    catch file:sync(FH),
    {Path, SizeStr} = case log_file_info(Dir, SeqNum) of
                          {ok, Path0, #file_info{size=Size0}} ->
                              {Path0, io_lib:format("(~w bytes)", [Size0])};
                          {error, Err0} ->
                              {"", io_lib:format("(error ~p)", [Err0])}
                      end,
    try file:close(FH) of
        ok ->
            ?E_INFO("Closed ~s log with sequence ~w: ~s ~s",
                    [Type, SeqNum, Path, SizeStr]),
            ok;
        {error, Reason}=Err1 ->
            ?E_ERROR("Failed to close ~s log with sequence ~w: ~s (error ~p)",
                     [Type, SeqNum, Path, Reason]),
            Err1
    catch E0:E1 ->
            ?E_ERROR("Failed to close ~s log with sequence ~w: ~s (~p ~p)",
                     [Type, SeqNum, Path, E0, E1]),
            {error, E1}
    end.

-spec hunk_header_v1() -> binary().
hunk_header_v1() ->
    <<(16#feedbeef):32>>.
%% !@#$! Leave this as last func, Emacs mode indentation gets confused.

create_longterm_dirs(Dir, H1_Size, H2_Size) ->
    _ = [begin
             Tmp = longterm_dir(Dir, N1, N2),
             Path = lists:sublist(Tmp, length(Tmp) - 2),
             file:make_dir(Path)
         end || N1 <- lists:seq(1, H1_Size), N2 <- [0]],
    _ = [file:make_dir(longterm_dir(Dir, N1, N2))
         || N1 <- lists:seq(1, H1_Size), N2 <- lists:seq(1, H2_Size)],
    ok.

-spec md5_checksum_ok_p(#hunk_summ{}) -> boolean().
md5_checksum_ok_p(Su) ->
    case get(disable_md5_hack) of
        yes ->
            true;
        _ ->
            case Su#hunk_summ.md5s of
                [] ->
                    %% If the list of md5s is empty, then we assume
                    %% that the hunk was written at a time when
                    %% checksum generation was disabled.
                    true;
                _ ->
                    lists:all(fun({MD5, Bin}) -> MD5 == do_md5(Bin) end,
                              lists:zip(Su#hunk_summ.md5s,Su#hunk_summ.c_blobs))
            end
    end.

do_md5(X) ->
    Key = gmt_hlog_use_md5_bif,
    case get(Key) of
        undefined ->
            File = "./use-md5-bif",
            case (catch file:read_file(File)) of
                {ok, _} ->
                    ?E_WARNING("~s: MD5 BIF in use by ~p",
                               [?MODULE, self()]),
                    put(Key, yes),
                    do_md5(X);
                _ ->
                    put(Key, no),
                    do_md5(X)
            end;
        yes ->
            erlang:md5(X);
        no ->
            crypto:hash(md5, X)
    end.

do_sync_asyncly(From, #state{syncer_pid = undefined} = S) ->
    ThisRound = [From],
    {Pid, Ref} = spawn_sync_asyncly(ThisRound, S),
    ?DBG_GEN("do_sync_asyncly [one_waiter] ~s", [S#state.dir]),
    S#state{syncer_pid = Pid, syncer_ref = Ref, syncers_next_round = [],
            syncer_advance_reqs = []};
do_sync_asyncly(From, #state{syncers_next_round = NextRound} = S) ->
    ?DBG_GEN("do_sync_asyncly [many_waiters] ~s", [S#state.dir]),
    S#state{syncers_next_round = [From|NextRound]}.

spawn_sync_asyncly(Froms, S) ->
    Start = now(),
    spawn_monitor(
      fun() ->
              ?DBG_GEN("spawn_sync_asyncly [top] ~s seq ~w off ~w",
                       [S#state.dir, S#state.cur_seqS, S#state.cur_offS]),
              %% Warning: [write] => truncate file.
              Path = "././" ++ S#state.dir,
              {ok, FH} = open_log_file(Path, S#state.cur_seqS, [append]),
              _ = try
                      {ok, FI} = file:read_file_info(Path),
                      if FI#file_info.size =:= 0 ->
                              ?E_ERROR("fsync ~s, size 0", [Path]);
                         true ->
                              ok
                      end,
                      ?DBG_GEN("spawn_sync_asyncly [after_open_log_file] ~s", [S#state.dir]),
                      Res = file:sync(FH),
                      debug_sleep(S#state.debug_sync_sleep),
                      ?DBG_GEN("spawn_sync_asyncly [after_sync] ~s froms ~w", [S#state.dir, Froms]),
                      [gen_server:reply(
                         From, {Res, S#state.cur_seqS, S#state.cur_offS}) ||
                          From <- Froms]
                  after
                      ok = file:close(FH)
                  end,
              MSec = timer:now_diff(now(), Start) div 1000,
              if MSec > 200 ->
                      ?ELOG_INFO("~s sync was ~p msec for ~p callers",
                                 [S#state.name, MSec, length(Froms)]);
                 true ->
                      ok
              end,
              ?DBG_GEN("spawn_sync_asyncly [bottom] ~s ~w", [S#state.dir, MSec]),
              normal
      end).

asyncly_done(Pid, _Reason, S) when S#state.syncer_pid == Pid ->
    ?DBG_GEN("asyncly_done: ~s backlog ~w next_round ~w",
             [S#state.dir, length(S#state.write_backlog), length(S#state.syncers_next_round)]),
    do_pending_syncs(do_pending_writes(S#state{syncer_pid = undefined,
                                               syncer_ref = undefined}));
asyncly_done(_Pid, _Reason, S) ->
    ?ELOG_ERROR("asyncly_done: ~p ~p when expecting ~p",
                [_Pid, _Reason, S#state.syncer_pid]),
    S.

do_pending_writes(S) ->
    if S#state.write_backlog /= [] ->
            %% io:format("bl ~p ", [length(S#state.write_backlog)]),
            ?DBG_GEN("do_pending_writes: buffer write bytes ~p total (grep tag=sync)",
                     [erlang:iolist_size(S#state.write_backlog)]),
            write_shortterm_backlog(S, true);
       true ->
            ok
    end,
    S#state{write_backlog = []}.

write_shortterm_backlog(S, true) ->
    ok = file:write(S#state.cur_fhS, lists:reverse(S#state.write_backlog)),
    MyOffset = S#state.cur_offS,                          %BZDEBUG
    {ok, MyOffset} = file:position(S#state.cur_fhS, cur), %BZDEBUG
    ok;
write_shortterm_backlog(S, false) ->
    catch write_shortterm_backlog(S, true). % failure is ok.

do_pending_syncs(S) ->
    NewS = lists:foldl(fun({Incr, From}, AccState) ->
                               do_advance_seqnum(Incr, From, AccState)
                       end, S, S#state.syncer_advance_reqs),
    if NewS#state.syncers_next_round == [] ->
            NewS#state{syncer_advance_reqs = []};
       true ->
            {NewPid, NewRef} =
                spawn_sync_asyncly(S#state.syncers_next_round, S),
            NewS#state{syncer_pid = NewPid, syncer_ref = NewRef,
                       syncers_next_round = [], syncer_advance_reqs = []}
    end.

do_sync_longterm_asyncly(From, S) ->
    spawn(
      fun() ->
              Path = "././" ++ S#state.dir,
              case open_log_file(Path, S#state.cur_seqL, [append]) of
                  {ok, FH} ->
                      try
                          {ok, FI} = file:read_file_info(Path),
                          if FI#file_info.size =:= 0 ->
                                  ?E_ERROR("fsync ~s, size 0", [Path]);
                             true ->
                                  ok
                          end,
                          Res = file:sync(FH),
                          gen_server:reply(From, {Res, S#state.cur_seqL, S#state.cur_offL})
                      catch
                          _:_ ->
                              gen_server:reply(From, {retrylater, S#state.cur_seqL, S#state.cur_offL})
                      after
                          ok = file:close(FH)
                      end;
                  Err ->
                      gen_server:reply(From, {Err, S#state.cur_seqL, S#state.cur_offL})
              end
      end).

-spec log_name2data_dir(servername()) -> dirname().
log_name2data_dir(ServerName) ->
    {ok, FileDir} = application:get_env(gdss_brick, brick_default_data_dir),
    filename:join([FileDir, "hlog." ++ atom_to_list(ServerName)]).

-spec log_name2reg_name(atom()) -> atom().
log_name2reg_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "_hlog").

debug_sleep(0) ->
    ok;
debug_sleep(N) ->
    timer:sleep(N).

debug_fold_a_file(FH) ->
    F = fun(A, _, Acc) ->
                case read_hunk_summ_ll(FH, element(3,A)) of
                    {error,_,_} ->
                        [element(3,A)|Acc];
                    {ok, Summ} ->
                        try
                            Offset = element(3,A),
                            Summ2 = Summ#hunk_summ{seq = 100603,
                                                   off = Offset},
                            B = read_hunk_member_ll(FH, Summ2, md5, 1),
                            <<_/binary>> = B,
                            Acc
                            %%  if abs(Offset - 6119206) < 10*1000 ->
                            %%          [{maybe, Offset - 6119206, Summ2}|Acc];
                            %%     true ->
                            %%          Acc
                            %%  end
                        catch X:Y ->
                                [{X,Y,Summ}|Acc]
                        end
                end
        end,
    fold_a_file(FH, 63206003, 100603, 0, F, []).

