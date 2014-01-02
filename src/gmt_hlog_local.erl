%%%-------------------------------------------------------------------
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
%%% File    : gmt_hlog_local.erl
%%% Purpose : Local GMT hunk log server.
%%%-------------------------------------------------------------------

-module(gmt_hlog_local).

%% @doc The API module for a performance-enhanced hunk log.
%%
%% In the original gmt_hlog.erl implementation, write_hunk() made a
%% distinction between two types of storage:
%%
%%   * short-term: Any short-term data is scanned by a brick at startup
%%       for key metadata info. However, because only short-term log
%%       can be fsync'ed for stable storage, bigdata_dir blobs
%%       (i.e. values blobs to be stored on disk) must also be written
%%       to short-term storage. This allows a single file:sync() to
%%       get both the value blob hunk and the key metadata hunk.
%%       (After a checkpoint is finished, the brick will then move
%%       short-term hunk files into longterm storage area.)
%%
%%       Hunks stored in the short-term area have SeqNum values > 0.
%%
%%   * long-term: Data in long-term storage cannot be scanned by a
%%       brick's startup sequence, which can reduce brick startup time
%%       dramatically.
%%
%%       Hunks stored in the long-term area have SeqNum values &lt; 0.
%%
%% In this new scheme, logs are further subdivided into "local" and
%% "common". The details of "where" physically a hunk is stored shall
%% be hidden from the client (e.g. brick_ets.erl) at all times.
%% However, we need the client to give us more information than
%% 'shortterm' or 'longterm' to be able to make physical storage
%% location decisions. The HLogType argument gives us that extra
%% info:
%%
%%   * metadata: For storing Hibari hunks of type ?LOGTYPE_METADATA and
%%               ?LOGTYPE_BAD_SEQUENCE (and perhaps others in the
%%               future). brick_ets will load all data of this type in
%%               RAM for quick access at all times.
%%
%%   * bigblob: For storing Hibari hunks of type ?LOGTYPE_BLOB. The
%%              size of these blobs makes storage in RAM infeasible
%%              and thus will always be accessed from disk.
%%
%% All hunks, of both 'metadata' and 'bigblob' types, will be written
%% and fsync'ed (if necessary) to the common log first. A lazy
%% write-back process will copy 'metadata' hunks to their owner local
%% log storage. The same write-back process will copy 'bigblob' types
%% to longterm storage within the *common* log if gmt_hlog_common's
%% #state.short_long_same_dev_p is set to false. Both types of copy will
%% be asynchronous (i.e. not use fsync()).
%%
%% Hunks of type 'bigblob' can (and will) still be stored in the
%% short-term area of the common log: the underlying gmt_hlog's sync
%% behavior has not changed. (Recall, only short-term data can be
%% fsynced.)  The common log's write-back process will move those blobs
%% to longterm storage and then notify their respective owner bricks
%% that they've been relocated (using the same update mechanism that
%% the scavenger uses).
%%
%% One effect of the specialization of 'metadata' and 'bigblob' types
%% is that this module, gmt_hlog_local.erl, is much less generic than
%% gmt_hlog.erl is, and it is also much more closely tied to a single
%% application, Hibari, than gmt_hlog.erl is.

-include("gmt_hlog.hrl").
-include("brick.hrl").

-behaviour(gen_server).

-define(GLOBAL_LOG, global_log).
-define(GLOBAL_LOG_NAME, global_log_sync).

%% API
-export([start_link/1]).
%% gen_server proxy
-export([stop/1,
         advance_seqnum/2,
         get_proplist/1,
         get_metadata_db/1,
         sync/1,
         write_hunk/7,
         get_current_seqnum/1
        ]).
%% Simple passthrough
-export([write_log_header/1,
         log_file_path/2,
         log_file_path/3,
         log_file_info/2,
         create_hunk/3,
         find_current_log_seqnums/1,
         find_longterm_log_seqnums/3,
         read_hunk_member_ll/4,
         md5_checksum_ok_p/1,
         read_hunk_summary/3,
         read_hunk_summary/5,
         fold/4,
         fold/5,
         open_log_file/3,
         move_seq_to_longterm/2,
         find_current_log_files/1,
         get_all_seqnums/1,
         log_name2data_dir/1,
         log_name2reg_name/1
        ]).
%% API to be used on local node only
-export([read_bigblob_hunk_blob/2,
         read_bigblob_hunk_blob/3,
         read_bigblob_hunk_blob/4,
         read_bigblob_hunk_blob/5
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% TODO: Tune this value?  I've witnessed problems with
%%       move_seq_to_longterm/2 hitting timeout when the disk has been
%%       overloaded by checkpoint-related disk I/O.
-define(TIMEOUT, 60*1000).

%%%----------------------------------------------------------------------
%%% Types/Specs/Records
%%%----------------------------------------------------------------------

-record(state, {
          props                       :: proplist(),
          name                        :: atom(),
          dir                         :: dirname(),                % storage dir
          file_len_max                :: len(),
          file_len_min                :: len(),
          common_server               :: servername(),
          common_log                  :: pid() | undefined,
          last_seq=0                  :: seqnum(),
          offset=0                    :: offset(),
          metadata_db                 :: term()
         }).

-type proplist() :: list({atom(),term()}).


%%%===================================================================
%%% API
%%%===================================================================

-spec get_metadata_db(server()) -> term().
get_metadata_db(Pid) ->
    gen_server:call(Pid, {get_metadata_db}).

start_link(PropList) ->
    gen_server:start_link(?MODULE, PropList, []).

-spec stop(server()) -> ok | {error,term()}.
stop(Pid) ->
    gen_server:call(Pid, {stop}).

-spec advance_seqnum(pid(), incr()) -> {ok, seqnum()} | error | no_return().
advance_seqnum(Pid, Num)
  when is_integer(Num), Num =/= 0 ->
    gen_server:call(Pid, {advance_seqnum, Num});
advance_seqnum(_Pid, _Num) ->
    erlang:error(badarg).

-spec get_proplist(pid()) -> proplist().
get_proplist(Pid) ->
    gen_server:call(Pid, {get_proplist}).

%% @doc Write a hunk to a log.
%%      CBlobs: A list of blobs that also has an MD5 checksum stored inside the hunk.
%%      UBlobs: A list of blobs without an MD5 checksum.
-spec write_hunk(server(), brickname(), hlogtype(), key(), typenum(),
                 CBlobs::blobs(), UBlobs::blobs()) ->
                        {ok, seqnum(), offset()} | {hunk_too_big, len()} | no_return().
write_hunk(Server, LocalLogName, HLogType, Key, TypeNum, CBlobs, UBlobs)
  when is_atom(HLogType), is_integer(TypeNum),
       is_list(CBlobs), is_list(UBlobs) ->
    if
        HLogType =:= metadata orelse HLogType =:= bigblob ->
            {H_Len, H_Bytes} = gmt_hlog:create_hunk(TypeNum, CBlobs, UBlobs),
            gen_server:call(Server, {write_hunk_bytes, LocalLogName, HLogType, Key,
                                     ?LOCAL_RECORD_TYPENUM, H_Len, H_Bytes}, ?TIMEOUT);
        true ->
            {error, {invalid_hlog_type, HLogType}}
    end.

-spec sync(server()) -> {ok, seqnum(), offset()}.
sync(Pid) ->
    gmt_hlog:sync(Pid).

get_current_seqnum(Pid) ->
    gmt_hlog:get_current_seqnum(Pid).

%% pass through

-spec write_log_header(file:fd()) -> ok | {error, term()}.
write_log_header(FH) ->
    gmt_hlog:write_log_header(FH).

-spec log_file_path(dirname(), seqnum()) -> dirname().
log_file_path(A, B) ->
    gmt_hlog:log_file_path(A, B).

log_file_path(A, B, C) ->
    gmt_hlog:log_file_path(A, B, C).

-spec create_hunk(typenum(), CBlobs::blobs(), UBlobs::blobs()) -> {len(), bytes()}.
create_hunk(A, B, C) ->
    gmt_hlog:create_hunk(A, B, C).

-spec find_current_log_seqnums(dirname()) -> list(seqnum()).
find_current_log_seqnums(A) ->
    gmt_hlog:find_current_log_seqnums(A).

find_longterm_log_seqnums(A, B, C) ->
    gmt_hlog:find_longterm_log_seqnums(A, B, C).

-spec read_hunk_member_ll(file:fd(), #hunk_summ{}, md5 | undefined, nth()) -> binary().
read_hunk_member_ll(A, B, C, D) ->
    gmt_hlog:read_hunk_member_ll(A, B, C, D).

-spec md5_checksum_ok_p(#hunk_summ{}) -> boolean().
md5_checksum_ok_p(A) ->
    gmt_hlog:md5_checksum_ok_p(A).

read_hunk_summary(A, B, C) ->
    gmt_hlog:read_hunk_summary(A, B, C).

read_hunk_summary(A, B, C, D, E) ->
    gmt_hlog:read_hunk_summary(A, B, C, D, E).

fold(A, B, C, D) ->
    gmt_hlog:fold(A, B, C, D).

-spec fold(shortterm | longterm, dirname(), foldfun(), filtfun(), foldacc()) -> foldret().
fold(A, B, C, D, E) ->
    gmt_hlog:fold(A, B, C, D, E).

open_log_file(A, B, C) ->
    gmt_hlog:open_log_file(A, B, C).

-spec log_file_info(dirname(), seqnum()) -> {ok, filepath(), file:file_info()} | {error, atom()}.
log_file_info(LogDir, SeqNum) ->
    gmt_hlog:log_file_info(LogDir, SeqNum).

-spec move_seq_to_longterm(server(), seqnum()) -> ok | error | {error,seqnum()} | no_return().
move_seq_to_longterm(A, B) ->
    gmt_hlog:move_seq_to_longterm(A, B).

-spec find_current_log_files(dirname()) -> list(dirname()).
find_current_log_files(A) ->
    gmt_hlog:find_current_log_files(A).

get_all_seqnums(A) ->
    gmt_hlog:get_all_seqnums(A).

-spec log_name2data_dir(servername()) -> dirname().
log_name2data_dir(A) ->
    gmt_hlog:log_name2data_dir(A).

-spec log_name2metadata_dir(servername()) -> dirname().
log_name2metadata_dir(A) ->
    gmt_hlog:log_name2metadata_dir(A).

log_name2reg_name(A) ->
    gmt_hlog:log_name2reg_name(A).

-spec read_bigblob_hunk_blob(seqnum(), offset()) -> no_return().
read_bigblob_hunk_blob(SeqNum, Offset) ->
    read_bigblob_hunk_blob(SeqNum, Offset, true).

%% @doc Read a 'bigblob' hunk's first MD5-checksummed blob from the
%% common log.
%%
%% NOTE: This function can be used only on the same node as the common
%%       log gen_server process is running. Furthermore, it can only
%%       be used to read hunks of type 'bigblob'.
%%
%%       Question: How do you read hunks of type 'metadata'?
%%       Answer: You don't, not individually. Because our API is
%%               tightly tied to the Hibari application, and the only
%%               times that Hibari reads 'metadata' blobs is at brick
%%               startup, and because startup uses fold() to read all
%%               shortterm 'metadata' blobs sequentially, there is no
%%               need to provide a random access API function.

-spec read_bigblob_hunk_blob(seqnum(), offset(), checkmd5()) -> no_return().
read_bigblob_hunk_blob(SeqNum, Offset, CheckMD5_p) ->
    %% We don't know the blob size, but that's just a hint anyway.
    read_bigblob_hunk_blob(SeqNum, Offset, CheckMD5_p, 0).

-spec read_bigblob_hunk_blob(seqnum(), offset(), checkmd5(), lenhint()) ->
                                    #hunk_summ{} | eof | {error, term()}.
read_bigblob_hunk_blob(SeqNum, Offset, CheckMD5_p, ValLen) ->
    Dir = get_common_server_dir(),
    read_bigblob_hunk_blob(Dir, SeqNum, Offset, CheckMD5_p, ValLen).

-spec read_bigblob_hunk_blob(dirname(), seqnum(), offset(), checkmd5(), lenhint()) ->
                                    #hunk_summ{} | eof | {error, term()}.
read_bigblob_hunk_blob(Dir, SeqNum, Offset, CheckMD5_p, ValLen) ->
    gmt_hlog:read_bigblob_hunk_blob(Dir, SeqNum, Offset, CheckMD5_p, ValLen).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initiates the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(PropList) ->
    Name = proplists:get_value(name, PropList),
    RegName = log_name2reg_name(Name),
    register(RegName, self()),
    %%io:format("DBG: ~s: self ~p registered as ~p\n", [?MODULE, self(), Name]),

    process_flag(priority, high),
    Dir = log_name2data_dir(Name),
    FileLenMax = proplists:get_value(file_len_max, PropList, 100*1024*1024),
    FileLenMin = proplists:get_value(file_len_min, PropList, FileLenMax),
    CServerName = get_or_start_common_log(PropList),
    %%io:format("DBG: ~s: common info: ~p\n", [?MODULE, CServerName]),
    LastSeq = read_last_sequence_number(Dir),
    self() ! finish_init_tasks,

    catch file:make_dir(Dir),
    catch file:make_dir(Dir ++ "/s"),

    MDBDir = log_name2metadata_dir(Name),
    catch file:make_dir(MDBDir),

    %% @TODO Create a function to return the metadata DB path.
    MDBPath = filename:join(MDBDir, "leveldb"),
    MetadataDB = leveldb:open_db(MDBPath, [create_if_missing]),
    ?ELOG_INFO("Opened the metadata DB: ~s", [MDBPath]),

    S = #state{props = PropList,
               name = Name,
               dir = Dir,
               file_len_max = FileLenMax,
               file_len_min = FileLenMin,
               common_server = CServerName,
               last_seq = LastSeq,
               metadata_db = MetadataDB
              },
    {_, NewS} = do_advance_seqnum(1, S),
    %%io:format("DBG: ~s: init done\n", [?MODULE]),
    {ok, NewS}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({write_hunk_bytes, LocalLogName, metadata, Key, TypeNum, H_Len, H_Bytes},
            _From,
            #state{common_log=CommonLogPid,
                   last_seq=LastSeq, offset=LastOffset,
                   file_len_min=FileLenMin}=State) ->

    %% Write to the common hlog. This tuple must be sortable by gmt_hlog_common
    %% by LocalLogName, LastSeq, LastOffset, so the original hunk bytes (H_Bytes)
    %% has to be wrapped with such info. Later, gmt_hlog_common:write_back_to_local_log/8
    %% will unwrap H_Bytes and copy it to the local hlog.
    T = make_metadata_tuple(LocalLogName, LastSeq, LastOffset, Key, TypeNum, H_Len, H_Bytes),
    {RealLen, RealBytes} =
        gmt_hlog:create_hunk(?LOCAL_RECORD_TYPENUM, [], [term_to_binary(T)]),

    case gmt_hlog:write_hunk_internalwrapper(CommonLogPid, LocalLogName, metadata,
                                             Key, TypeNum, RealLen, RealBytes) of
        {ok, _RemoteSeq, _RemoteOff} ->
            Reply = {ok, LastSeq, LastOffset},
            NewOffset = LastOffset + H_Len,
            if NewOffset > FileLenMin ->
                    {_, NewState} = do_advance_seqnum(1, State),
                    {reply, Reply, NewState};
               true ->
                    {reply, Reply, State#state{offset=NewOffset}}
            end;
        Err ->
            {reply, Err, State}
    end;
handle_call({write_hunk_bytes, LocalLogName, bigblob, Key, TypeNum, H_Len, H_Bytes},
            _From, #state{common_log=CommonLogPid}=State) ->
    case gmt_hlog:write_hunk_internalwrapper(CommonLogPid, LocalLogName, bigblob,
                                             Key, TypeNum, H_Len, H_Bytes) of
        {ok, RemoteSeq, RemoteOff} ->
            {reply, {ok, RemoteSeq, RemoteOff}, State};
        Err ->
            {reply, Err, State}
    end;
handle_call({get_all_seqnums}, From, State) ->
    spawn(fun() -> Res = find_current_log_seqnums(State#state.dir),
                   %% We don't do local longterm storage, so don't bother trying to find longterms.
                   %% find_longterm_log_seqnums(State#state.dir,
                   %%                          State#state.long_h1_size,
                   %%                          State#state.long_h2_size),
                   gen_server:reply(From, Res)
          end),
    {noreply, State};
handle_call({advance_seqnum, Num}, _From, State) ->
    {Reply, NewState} = do_advance_seqnum(Num, State),
    {reply, Reply, NewState};
handle_call({sync, ShortLong}, From, State)
  when ShortLong == shortterm; ShortLong == longterm ->
    ParentPid = self(),
    Reply = {ok, State#state.last_seq, State#state.offset},
    _Pid = spawn_link(fun() ->
                              {ok,_X,_Y} = gmt_hlog:sync(State#state.common_log,
                                                         ShortLong),
                              ?DBG_GEN("~w ~p -> common sync: ~w, ~w", [State#state.name, self(), _X, _Y]),
                              gen_server:reply(From, Reply),
                              unlink(ParentPid), % avoid smart_exceptions prob.
                              exit(normal)
                      end),
    {noreply, State};
handle_call({get_current_seqnum}, _From, State) ->
    Res = State#state.last_seq,
    {reply, Res, State};
handle_call({get_common_log_name}, _From, State) ->
    Res = State#state.common_log,
    {reply, Res, State};
handle_call({get_proplist}, _From, State) ->
    Res = State#state.props,
    {reply, Res, State};
handle_call({get_metadata_db}, _From, #state{metadata_db=MetadataDB}=State) ->
    {reply, MetadataDB, State};
handle_call({stop}, _From, State) ->
    {stop, normal, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    ?ELOG_ERROR("~p", [_Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(finish_init_tasks, State) ->
    CommonLogPid = gmt_hlog_common:hlog_pid(State#state.common_server),
    %%io:format("DBG: ~s: Common hlog pid ~p\n", [?MODULE, CommonLogPid]),
    ok = gmt_hlog_common:register_local_brick(State#state.common_server,
                                              State#state.name),
    {noreply, State#state{common_log = CommonLogPid}};
handle_info(_Info, State) ->
    ?ELOG_ERROR("~p", [_Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{name=Name, metadata_db=MetadataDB}) ->
    %% @TODO Create a function to return the metadata DB path.
    MDBPath = filename:join(log_name2metadata_dir(Name), "leveldb"),
    try leveldb:close_db(MetadataDB) of
        true ->
            ?ELOG_INFO("Closed the metadata DB: ~s", [MDBPath])
    catch _:_=Error ->
            ?ELOG_WARNING("Failed to close the metadata DB: ~s (Error: ~p)",
                          [MDBPath, Error])
    end,
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec make_metadata_tuple(brickname(), seqnum(), offset(), key(), typenum(), len(), bytes()) ->
                                 metadata_tuple().
make_metadata_tuple(LocalLogName, LastSeq, LastOffset, Key, TypeNum, Len, Bytes) ->
    {eee, LocalLogName, LastSeq, LastOffset, Key, TypeNum, Len, Bytes}.

do_advance_seqnum(Incr, S) ->
    NewSeq = S#state.last_seq + abs(Incr),
    PermPath = sequence_number_path(S#state.dir),
    ok = brick_server:replace_file_sync(
           PermPath, fun(FH) -> ok = io:format(FH, "~w", [NewSeq]) end),
    Offset = erlang:iolist_size(gmt_hlog:file_header_version_1()),
    {{ok, NewSeq}, S#state{last_seq = NewSeq, offset = Offset}}.

read_last_sequence_number(Dir) ->
    try
        {ok, Bin} = file:read_file(sequence_number_path(Dir)),
        gmt_util:int_ify(Bin)
    catch _X:_Y ->
            0
    end.

sequence_number_path(Dir) ->
    Dir ++ "/sequence_number".

get_or_start_common_log(PropList) ->
    CName = proplists:get_value(common_log_name, PropList,
                                ?GMT_HLOG_COMMON_LOG_NAME),
    case whereis(CName) of
        undefined ->
            Ps = [{common_log_name, CName}],
            %% Use start() func to avoid linking to the new proc.
            ?ELOG_INFO("Trying to start ~w", [CName]),
            case (catch gmt_hlog_common:start(Ps)) of
                {ok, _Pid} ->
                    ok;
                ignore ->
                    ok;
                {'EXIT', _Err} ->
                    %% race with init & register
                    timer:sleep(100),
                    get_or_start_common_log(PropList)
            end;
        _ ->
            ok
    end,
    %% rest_for_one behavior of supervisor of our parent/owner brick
    %% will kill us if ?GMT_HLOG_COMMON_LOG_NAME dies.
    %% link(whereis(CName)),
    CName.

get_common_server_dir() ->
    log_name2data_dir(?GMT_HLOG_COMMON_LOG_NAME).
