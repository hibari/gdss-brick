%%%----------------------------------------------------------------------
%%% Copyright (c) 2014-2015 Hibari developers. All rights reserved.
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
%%% File    : brick_hlog_hunk.erl
%%% Purpose : log hunk used by brick_hlog.
%%%----------------------------------------------------------------------



%% @TODO: Turn on the compiler option to check the efficiency of
%%        binary operations.



%% (@TODO Add BrickName in the super block or trailer block?)
%%
%% ubint: unsigned, big-endian, integer
%%
%% Log File Layout
%% - Superblock
%%   * Signature     (8 bytes)                   <<16#8A, "HLG", "\r", "\n", 16#1A, "\n">>
%%   * Version       (2 bytes, ubint)            <<0, 1>>
%%   * Reserved      (6 bytes)                   <<"Hibari">>
%% - Hunk 1
%%   * Header        (wal: 12 bytes, blob store: 8 or 10 bytes)
%%   * Body          (variable length)
%%   * Footer        (variable length)
%%     ** ...
%%     ** Padding    (variable length)           Hunk size is aligned to 8 bytes
%% - Hunk 2
%%   * ...
%% - ...
%% - Hunk N
%%   * ...
%% - Trailer block
%%   * Magic         (4 bytes)
%%   * Hunk Overhead (4 bytes)                   The sum of the sizes of headers and
%%                                               footers of all hunks.
%%
%% ubint: unsigned, big-endian, integer
%%
%% Hunk Layout - Header for all blob types
%% - Header (12 bytes, fixed length)
%%   * Header Magic Number (2 bytes)                    <<16#90, 16#7F>>  %% no meaning
%%   * Type (1 byte)
%%   * Flags (has_md5, etc.) (1 byte)
%%   * BrickNameSize (2 bytes, ubint)                    0 for non-WAL hunks
%%   * NumberOfBlobs (2 bytes, ubnit)
%%   * TotalBlobSize (4 bytes, ubnit)                    Max value size is (4 GB - 1 byte)
%%
%%
%% Hunk Layout - metadata; many blobs in one hunk
%% - Header (12 bytes, fixed length, see above for details)
%% - Body (variable length)
%%   * BrickName (binary)
%%   * Blob1 (binary)
%%   * Blob2 (binary)
%%   * ...
%%   * BlobN (binary)
%% - Footer (variable length)
%%   * Footer Magic Number (2 bytes)                    <<16#07, 16#E3>>  %% no meaning
%%   * Blob Checksum (md5) (16 bytes) (optional)
%%   * BrickName (binary)
%%   * Blob Index (4 bytes * NumberOfBlobs, ubint)
%%   * Padding                                          Total hunk size is aligned to 8 bytes
%%
%%
%% Hunk Layout - blob_wal; many blobs in one hunk
%% - Header (12 bytes, fixed length, see above for details)
%% - Body (variable length)
%%   * Blob1 (binary)
%%   * Blob2 (binary)
%%   * ...
%%   * BlobN (binary)
%% - Footer (variable length)
%%   * Footer Magic Number (2 bytes)                    <<16#07, 16#E3>>  %% no meaning
%%   * Blob Checksum (md5) (16 bytes) (optional)
%%   * BrickName (binary)
%%   * Blob Index (4 bytes * NumberOfBlobs, ubint)
%%   * Padding                                          Total hunk size is aligned to 8 bytes
%%
%%
%% Hunk Layout - blob_single; one blob in one hunk
%% - Header (12 bytes, fixed length, see above for details)
%% - Body (variable length)
%%   * Blob (binary)
%% - Footer (variable length)
%%   * Footer Magic Number (2 bytes)                    <<16#07, 16#E3>>  %% no meaning
%%   * Blob Checksum (md5) (16 bytes) (optional)
%%   * Blob Age (1 byte)
%%   * Padding                                          Total hunk size is aligned to 8 bytes
%%
%%
%% Hunk Layout - blob_multi; many blobs in one hunk
%% - Header (12 bytes, fixed length, see above for details)
%% - Body (variable length)
%%   * Blob1 (binary)
%%   * Blob2 (binary)
%%   * ...
%%   * BlobN (binary)
%% - Footer (variable length)
%%   * Footer Magic Number (2 bytes)                    <<16#07, 16#E3>>  %% no meaning
%%   * Blob Checksum (md5) (16 bytes) (optional)
%%   * Blob Index (4 bytes * NumberOfBlobs, ubint)
%%   * Blob Ages  (1 byte * NumberOfBlobs, ubint))
%%   * Padding                                          Total hunk size is aligned to 8 bytes
%%

-module(brick_hlog_hunk).

-include("brick_hlog.hrl").
-include("brick.hrl").      % for ?E_ macros

%% DEBUG
-compile(export_all).

-export([calc_hunk_size/1,
         calc_hunk_size/5,
         create_hunk_iolist/1,
         parse_hunks/1,
         parse_hunk_iodata/1,
         %% read_hunk/2,
         %% read_hunk/3,
         read_blob_directly/4
        ]).

%% DEBUG
-export([test1/0,
         test2/0,
         test3/0,
         test4/0,
         test5/0,
         test6/0,
         test7/0
        ]).


%% ====================================================================
%% types and records
%% ====================================================================

%% Need fixed-length types for better space utilization? Perhaps
%% smaller blob (< 16 bytes or so) might be embedded into its metadata
%% (= value_in_ram).

%% For a small blob, an upper layer (write-back and scavenge
%% processes) should pack multiple values into one hunk (~= 4KB
%% so that it can avoid the overhead of hunk enclosure.

-define(FILE_SIGNATURE, <<16#8A, "HLG", "\r", "\n", 16#1A, "\n">>).

-define(HUNK_HEADER_SIZE,     12).
-define(HUNK_MIN_FOOTER_SIZE,  2).
-define(HUNK_ALIGNMENT,        8).

-define(HUNK_HEADER_MAGIC, 16#90, 16#7F).   %% 144, 127
-define(HUNK_FOOTER_MAGIC, 16#07, 16#E3).   %%   7, 227

-define(TYPE_METADATA,     <<"m">>).
-define(TYPE_BLOB_WAL,     <<"w">>).
-define(TYPE_BLOB_SINGLE,  <<"s">>).
-define(TYPE_BLOB_MULTI,   <<"p">>).   %% "p" stands for "packed" blobs. ("m" is already taken)

%% Flags is 1 byte, so we can store up to 8 flags.
-define(FLAG_NO_MD5,       16#01).


%% ====================================================================
%% API
%% ====================================================================

-spec calc_hunk_size(binary()) ->
                            {RawSize::non_neg_integer(),
                             FooterSize::non_neg_integer(),
                             PaddingSize::non_neg_integer(),
                             Overhead::non_neg_integer()}.
calc_hunk_size(<<?HUNK_HEADER_MAGIC, Bin:(?HUNK_HEADER_SIZE - 2)/binary>>) ->
    calc_hunk_size(Bin);
calc_hunk_size(<<Type:1/binary, Flags:1/unit:8, BrickNameSize:2/unit:8,
                 NumberOfBlobs:2/unit:8, TotalBlobSize:4/unit:8>>) ->
    DecodedType  = decode_type(Type),
    DecodedFlags = decode_flags(Flags),
    calc_hunk_size(DecodedType, DecodedFlags, BrickNameSize, NumberOfBlobs, TotalBlobSize).

-spec calc_hunk_size(hunk_type(), [hunk_flag()],
                     BrickNameSize::non_neg_integer(),
                     NumberOfBlobs::non_neg_integer(),
                     TotalBlobSize::non_neg_integer()) ->
                            {RawSize::non_neg_integer(),
                             FooterSize::non_neg_integer(),
                             PaddingSize::non_neg_integer(),
                             Overhead::non_neg_integer()}.
calc_hunk_size(Type, Flags, BrickNameSize, NumberOfBlobs, TotalBlobSize) ->
    MD5Size =
        case has_md5(Flags) of
            true ->
                16;
            false ->
                0
        end,
    BlobIndexSize =
        if
            Type =:= blob_single ->
                0;
            true ->
                4 * NumberOfBlobs
        end,
    BlobAgeSize =
        if
            Type =:= blob_single; Type =:= blob_multi ->
                NumberOfBlobs;
            true ->
                0
        end,
    FooterSize = ?HUNK_MIN_FOOTER_SIZE + MD5Size + BrickNameSize + BlobIndexSize + BlobAgeSize,
    %% RawSize includes FooterSize.
    RawSize = ?HUNK_HEADER_SIZE + TotalBlobSize + FooterSize,
    Rem = RawSize rem ?HUNK_ALIGNMENT,
    PaddingSize =
        if
            Rem =:= 0 ->
                0;
            true ->
                ?HUNK_ALIGNMENT - Rem
        end,
    Overhead = RawSize + PaddingSize - TotalBlobSize,
    {RawSize, FooterSize, PaddingSize, Overhead}.

-spec create_hunk_iolist(hunk())
                        -> {Hunk::iodata(),
                            HunkSize::non_neg_integer(), Overhead::non_neg_integer(),
                            BlobIndex::[non_neg_integer()]}.
create_hunk_iolist(#hunk{type=metadata, brick_name=BrickName}=Hunk)
  when BrickName =/= undefined ->
    create_hunk_iolist1(Hunk);
create_hunk_iolist(#hunk{type=blob_wal, brick_name=BrickName}=Hunk)
  when BrickName =/= undefined ->
    create_hunk_iolist1(Hunk);
create_hunk_iolist(#hunk{type=blob_single, brick_name=undefined,
                         blobs=[_Blob], blob_ages=[_Age]}=Hunk) ->
    create_hunk_iolist1(Hunk);
create_hunk_iolist(#hunk{type=blob_multi, brick_name=undefined,
                         blobs=Blobs, blob_ages=Ages}=Hunk)
  when length(Blobs) =:= length(Ages) ->
    create_hunk_iolist1(Hunk).

-spec parse_hunks(binary()) -> {ok, [hunk()], Remainder::binary()}
                                   | {error, Reason::term(), [hunk()]}.
parse_hunks(Hunks) when is_binary(Hunks) ->
    parse_hunks1(Hunks, []).

-spec parse_hunk_iodata(iodata()) -> {ok, hunk(), Remainder::binary()} | {error, term()}.
parse_hunk_iodata(Hunk) when is_list(Hunk) ->
    parse_hunk_iodata(list_to_binary(Hunk));
parse_hunk_iodata(<<?HUNK_HEADER_MAGIC, Type:1/binary, Flags:1/unit:8, BrickNameSize:2/unit:8,
                    NumberOfBlobs:2/unit:8, TotalBlobSize:4/unit:8, Rest/binary>>) ->
    RestSize     = byte_size(Rest),
    DecodedType  = decode_type(Type),
    DecodedFlags = decode_flags(Flags),
    {_RawSize, FooterSize, PaddingSize, _Overhead} =
        calc_hunk_size(DecodedType, DecodedFlags, BrickNameSize, NumberOfBlobs, TotalBlobSize),
    RemainderPos  = TotalBlobSize + FooterSize + PaddingSize,
    RemainderSize = RestSize - RemainderPos,

    if
        RemainderSize < 0 ->
            {error, {incomplete_input, ?HUNK_HEADER_SIZE + RestSize}};
        true ->
            BodyBin   = binary:part(Rest, 0, TotalBlobSize),
            FooterBin = binary:part(Rest, TotalBlobSize, FooterSize),
            Remainder = binary:part(Rest, RemainderPos, RemainderSize),

            case parse_hunk_footer(DecodedType, has_md5(DecodedFlags),
                                   BrickNameSize, NumberOfBlobs, FooterBin) of
                {error, _}=Err ->
                    Err;
                {ok, Md5, BrickName, BlobIndexBin, BlobAgesBin} ->
                    if
                        DecodedType =:= blob_single ->
                            {ok, #hunk{type=DecodedType, flags=DecodedFlags,
                                       blobs=[BodyBin], blob_ages=[BlobAgesBin],
                                       md5=Md5},
                             Remainder};
                        true ->
                            Blobs = parse_hunk_body(BodyBin, BlobIndexBin),
                            BlobAges = parse_blob_ages(BlobAgesBin),
                            {ok, #hunk{type=DecodedType, flags=DecodedFlags,
                                       brick_name=BrickName,
                                       blobs=Blobs, blob_ages=BlobAges,
                                       md5=Md5},
                             Remainder}
                    end
            end
    end;
parse_hunk_iodata(<<Bin:?HUNK_HEADER_SIZE, _Remainder/binary>>)
  when byte_size(Bin) >= ?HUNK_HEADER_SIZE ->
    {error, {invalid_format, hunk_header, Bin}};
parse_hunk_iodata(Bin) ->
    {error, {incomplete_input, byte_size(Bin)}}.

-spec read_blob_directly(file:fd(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
                                {ok, binary()} | eof | {error, term()}.
read_blob_directly(FH, HunkOffset, BlobOffset, BlobSize) ->
    %% file:pread(FH, HunkOffset + BlobOffset, BlobSize).
    %% @TODO: DEBUG Removeme
    case file:pread(FH, HunkOffset, 2) of
        {ok, <<?HUNK_HEADER_MAGIC>>} ->
            file:pread(FH, HunkOffset + BlobOffset, BlobSize);
        {ok, Other} ->
            error({wrong_position, HunkOffset, BlobOffset, BlobSize, Other});
        eof ->
            eof;
        {error, _}=Err ->
            Err
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

-spec create_hunk_iolist1(hunk())
                         -> {Hunk::iodata(),
                             HunkSize::non_neg_integer(), Overhead::non_neg_integer(),
                             BlobIndex::[non_neg_integer()]}.
create_hunk_iolist1(#hunk{type=Type, flags=Flags, brick_name=BrickName,
                          blobs=Blobs, blob_ages=BlobAges}) ->
    {EncodedBrickName, BrickNameSize} = encode_brick_name(BrickName),
    TotalBlobSize = total_blob_size(Blobs),
    {BlobIndex, NumberOfBlobs} = create_blob_index(Blobs),
    {RawSize, _FooterSize, PaddingSize, Overhead} =
        calc_hunk_size(Type, Flags, BrickNameSize, NumberOfBlobs, TotalBlobSize),
    HunkHeader = create_hunk_header(Type, Flags, BrickNameSize, NumberOfBlobs, TotalBlobSize),
    HunkFooter = create_hunk_footer(Type, Flags, EncodedBrickName,
                                    Blobs, BlobIndex, BlobAges, PaddingSize),
    Hunk = HunkHeader ++ Blobs ++ HunkFooter,
    {Hunk, RawSize + PaddingSize, Overhead, BlobIndex}.

-spec create_hunk_header(hunk_type(), [hunk_flag()],
                         non_neg_integer(), non_neg_integer(), non_neg_integer()) -> binary().
create_hunk_header(Type, Flags, BrickNameSize, NumberOfBlobs, TotalBlobSize) ->
    EncodedFlags = encode_flags(Flags),
    [<<?HUNK_HEADER_MAGIC>>,
     encode_type(Type),
     <<EncodedFlags:1/unit:8>>,
     <<BrickNameSize:2/unit:8>>,
     <<NumberOfBlobs:2/unit:8>>,
     <<TotalBlobSize:4/unit:8>>].

-spec create_hunk_footer(hunk_type(), [hunk_flag()],
                         binary(), [binary()], [non_neg_integer()], [non_neg_integer()] | undefined,
                         non_neg_integer()) -> binary().
create_hunk_footer(Type, Flags, EncodedBrickName, Blobs, BlobIndex, undefined, PaddingSize) ->
    create_hunk_footer(Type, Flags, EncodedBrickName, Blobs, BlobIndex, [], PaddingSize);
create_hunk_footer(Type, Flags, EncodedBrickName, Blobs, BlobIndex, BlobAges, PaddingSize) ->
    [<<?HUNK_FOOTER_MAGIC>>]
        ++ [ crypto:hash(md5, list_to_binary(Blobs)) || has_md5(Flags) ]
        ++ [ EncodedBrickName || Type =:= metadata orelse Type =:= blob_wal ]
        ++ [ <<Offset:4/unit:8>> || Offset <- BlobIndex, Type =/= blob_single ]
        ++ [ <<Age:1/unit:8>> || Age <- BlobAges,
                                 Type =:= blob_single orelse Type =:= blob_multi ]
        ++ [<<0:PaddingSize/unit:8>>].

-spec create_blob_index([binary()]) -> {BlobIndex::[non_neg_integer()],
                                        NumberOfBlobs::non_neg_integer()}.
create_blob_index(Blobs) ->
    NumberOfBlobs = length(Blobs),
    Sizes = [ byte_size(Blob) || Blob <- Blobs ],
    {SizesWithoutLastBlob, _} = lists:split(NumberOfBlobs - 1, Sizes),
    {_, BlobIndex} =
        lists:foldl(
          fun(Size, {Offset, Index}) ->
                  Offset1 = Offset + Size,
                  {Offset1, [Offset1 | Index]}
          end, {?HUNK_HEADER_SIZE, []}, [0|SizesWithoutLastBlob]),
    {lists:reverse(BlobIndex), NumberOfBlobs}.

-spec parse_hunks1(binary(), [hunk()])
                  -> {ok, [hunk()], Remainder::binary()}
                         | {error, Reason::term(), [hunk()]}.
parse_hunks1(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
parse_hunks1(Hunks, Acc) ->
    case parse_hunk_iodata(Hunks) of
        {ok, Hunk, Remainder} ->
            parse_hunks1(Remainder, [Hunk | Acc]);
        {error, {incomplete_input, _Size}} ->
            {ok, lists:reverse(Acc), Hunks};
        {error, Reason} ->
            {error, Reason, lists:reverse(Acc)}
    end.

-spec parse_hunk_body(binary(), binary()) -> [binary()].
parse_hunk_body(Blobs, <<Offset:4/unit:8, Rest/binary>>) ->
    parse_hunk_body1(Blobs, Offset - ?HUNK_HEADER_SIZE, Rest, []).

-spec parse_hunk_body1(binary(), non_neg_integer(), binary(), [binary()]) -> [binary()].
parse_hunk_body1(Blobs, RelativeOffset1, <<>>, Acc) ->
    Blob = binary:part(Blobs, RelativeOffset1, byte_size(Blobs) - RelativeOffset1),
    lists:reverse([Blob | Acc]);
parse_hunk_body1(Blobs, RelativeOffset1, <<Offset2:4/unit:8, Rest/binary>>, Acc) ->
    RelativeOffset2 = Offset2 - ?HUNK_HEADER_SIZE,
    Blob = binary:part(Blobs, RelativeOffset1, RelativeOffset2 - RelativeOffset1),
    parse_hunk_body1(Blobs, RelativeOffset2, Rest, [Blob | Acc]).

-spec parse_blob_ages(undefined | binary()) -> [non_neg_integer()].
parse_blob_ages(undefined) ->
    [];
parse_blob_ages(BlobAgesBin) ->
    parse_blob_ages1(BlobAgesBin, []).

-spec parse_blob_ages1(binary(), [non_neg_integer()]) -> [non_neg_integer()].
parse_blob_ages1(<<>>, Acc) ->
    lists:reverse(Acc);
parse_blob_ages1(<<BlobAge:1/unit:8, Rest/binary>>, Acc) ->
    parse_blob_ages1(Rest, [BlobAge | Acc]).

-spec parse_hunk_footer(hunk_type(), boolean(), non_neg_integer(), non_neg_integer(), binary())
                       -> {ok,
                           Md5::binary() | undefined,
                           BrickName::atom() | undefined,
                           BlobIndexBinary::binary(),
                           BlobAges::binary() | non_neg_integer() | undefined }
                              | {error, term()}.
%% type: metadata or blob_wal
parse_hunk_footer(Type, HasMd5, BrickNameSize, NumberOfBlobs, <<?HUNK_FOOTER_MAGIC, Bin/binary>>)
  when Type =:= metadata; Type =:= blob_wal ->
    BlobIndexSize = 4 * NumberOfBlobs,
    case {HasMd5, Bin} of
        {true, <<Md5:16/binary, BrickName:BrickNameSize/binary, BlobIndexBin:BlobIndexSize/binary>>} ->
            {ok, Md5, decode_brick_name(BrickName), BlobIndexBin, undefined};
        {false, <<BrickName:BrickNameSize/binary, BlobIndexBin:BlobIndexSize/binary>>} ->
            {ok, undefined, decode_brick_name(BrickName), BlobIndexBin, undefined};
        _ ->
            {error, {invalid_format, hunk_footer, Bin}}
    end;
%% type: blob_multi
parse_hunk_footer(blob_multi, HasMd5, 0, NumberOfBlobs, <<?HUNK_FOOTER_MAGIC, Bin/binary>>) ->
    BlobIndexSize = 4 * NumberOfBlobs,
    BlobAgesSize  = NumberOfBlobs,
    case {HasMd5, Bin} of
        {true, <<Md5:16/binary, BlobIndexBin:BlobIndexSize/binary, BlobAgesBin:BlobAgesSize/binary>>} ->
            {ok, Md5, undefined, BlobIndexBin, BlobAgesBin};
        {false, <<BlobIndexBin:BlobIndexSize/binary, BlobAgesBin:BlobAgesSize/binary>>} ->
            {ok, undefined, undefined, BlobIndexBin, BlobAgesBin};
        _ ->
            {error, {invalid_format, hunk_footer, Bin}}
    end;
%% type: blob_single
parse_hunk_footer(blob_single, true, 0, 1,
                  <<?HUNK_FOOTER_MAGIC, Md5:16/binary, BlobAge:1/unit:8>>) ->
    {ok, Md5, undefined, <<>>, BlobAge};
parse_hunk_footer(blob_single, false, 0, 1,
                  <<?HUNK_FOOTER_MAGIC, BlobAge:1/unit:8>>) ->
    {ok, undefined, undefined, <<>>, BlobAge}.

-spec encode_type(hunk_type()) -> binary().
encode_type(metadata) ->
    ?TYPE_METADATA;
encode_type(blob_wal) ->
    ?TYPE_BLOB_WAL;
encode_type(blob_single) ->
    ?TYPE_BLOB_SINGLE;
encode_type(blob_multi) ->
    ?TYPE_BLOB_MULTI.

-spec decode_type(binary()) -> hunk_type().
decode_type(?TYPE_METADATA) ->
    metadata;
decode_type(?TYPE_BLOB_WAL) ->
    blob_wal;
decode_type(?TYPE_BLOB_SINGLE) ->
    blob_single;
decode_type(?TYPE_BLOB_MULTI) ->
    blob_multi.

-spec encode_flags([hunk_flag()]) -> non_neg_integer().
encode_flags(Flags) when is_list(Flags) ->
    lists:foldl(fun(no_md5, Acc) ->
                        Acc bor ?FLAG_NO_MD5;
                   (Flag, _Acc) ->
                        error({invalid_flag, Flag})
                end, 0, Flags).

-spec decode_flags(non_neg_integer()) -> [hunk_flag()].
decode_flags(Flags) when is_integer(Flags) ->
    lists:foldl(fun(no_md5, Acc) ->
                        if
                            Flags band ?FLAG_NO_MD5 =/= 0 ->
                                [no_md5 | Acc];
                            true ->
                                Acc
                        end
                end, [], [no_md5]).

-spec has_md5([hunk_flag()]) -> boolean().
has_md5(Flags) ->
    not lists:member(no_md5, Flags).

-spec encode_brick_name(undefined | atom()) -> {binary(), non_neg_integer()}.
encode_brick_name(undefined) ->
    {<<>> ,0};
encode_brick_name(BrickName) ->
    EncodedBrickName = list_to_binary(atom_to_list(BrickName)),
    BrickNameSize = byte_size(EncodedBrickName),
    {EncodedBrickName, BrickNameSize}.

-spec decode_brick_name(binary()) -> undefined | atom().
decode_brick_name(<<>>) ->
    undefined;
decode_brick_name(EncodedBrickName) ->
    list_to_atom(binary_to_list(EncodedBrickName)).

-spec total_blob_size([binary()]) -> non_neg_integer().
total_blob_size(Blobs) ->
    Sizes = [ byte_size(Blob) || Blob <- Blobs ],
    lists:sum(Sizes).


%% ====================================================================
%% Internal functions - Tools
%% ====================================================================

-spec dump_hlog(file:path()) -> ok.
dump_hlog(Path) ->
    %% @TODO: read ahead?
    case file:open(Path, [binary, raw, read, read_ahead]) of
        {ok, FH} ->
            dump_hlog_loop(FH, 1, 25, false);
        Err ->
            Err
    end.

-spec dump_hlog_loop(file:fd(), offset(), non_neg_integer(), non_neg_integer()) -> ok.
dump_hlog_loop(FH, Count, BatchSize, false) when Count rem BatchSize =:= 0 ->
    {ok, [Reply]} = io:fread("Continue? [y/n]: ", "~s"),
    case string:to_lower(string:strip(Reply)) of
        "y" ->
            dump_hlog_loop(FH, Count, BatchSize, true);
        "n" ->
            bye_bye;
        _ ->
            dump_hlog_loop(FH, Count, BatchSize, false)
    end;
dump_hlog_loop(FH, Count, BatchSize, _Continue) ->
    {ok, Position} = file:position(FH, cur),
    %% Read an hunk header.
    case file:read(FH, ?HUNK_HEADER_SIZE) of
        eof ->
            reached_the_eof;
        {ok, Header} when byte_size(Header) =:= 12 ->
            {RawSize, FooterSize, PaddingSize, _Overhead} = calc_hunk_size(Header),
            %% Read hunk body and footer.
            RestSize = RawSize - ?HUNK_HEADER_SIZE + PaddingSize,
            case file:read(FH, RestSize) of
                {ok, Rest} when byte_size(Rest) =:= RestSize ->
                    {ok, Hunk, Remainder} = parse_hunk_iodata(<<Header/binary, Rest/binary>>),
                    io:format("Position: ~w::::::~n~s~n~n", [Position, hunk_to_string(Hunk)]),
                    if
                        Remainder =/= <<>> ->
                            io:format("WARNING: There is a remainder binary: ~p~n", [Remainder]);
                        true ->
                            ok
                    end,
                    dump_hlog_loop(FH, Count + 1, BatchSize, false);
                {ok, IncompleteRest} ->
                    BodySize = RawSize - ?HUNK_HEADER_SIZE - FooterSize,
                    Size1 = byte_size(IncompleteRest),
                    IBodySize = min(BodySize, Size1),
                    Size2 = Size1 - IBodySize,
                    IFooterSize =
                        if
                            Size2 =< 0 ->
                                0;
                            true ->
                                min(FooterSize, Size2)
                        end,
                    Size3 = Size2 - IFooterSize,
                    IPaddingSize =
                        if
                            Size3 =< 0 ->
                                0;
                            true ->
                               Size3
                        end,
                    io:format("WARNING: There is an incomplete body and/or hooter.~n"
                              "  Expected sizes:  body ~w, footer ~w, and padding ~w~n,"
                              "  Available bytes: body ~w, footer ~w, and padding ~w~n",
                              [ BodySize,  FooterSize,  PaddingSize,
                               IBodySize, IFooterSize, IPaddingSize]);
                Err1 ->
                    Err1
            end;
        {ok, IncompleteHeader} ->
            io:format("WARNING: There is an incomplete header: ~p. (expected ~w bytes)~n",
                      [IncompleteHeader, ?HUNK_HEADER_SIZE]);
        Err2 ->
            Err2
    end.

-spec hunk_to_string(hunk()) -> string().
hunk_to_string(#hunk{type=Type, flags=Flags, brick_name=BrickName,
                     blobs=Blobs, blob_ages=BlobAges, md5=MD5}) ->
    {_, BrickNameSize} = encode_brick_name(BrickName),
    {RawSize, FooterSize, PaddingSize, _Overhead} =
        calc_hunk_size(Type, Flags, BrickNameSize, length(Blobs), total_blob_size(Blobs)),
    BrickNameDisp =
        if
            BrickName =:= undefined ->
                n_a;       %% atom
            true ->
                BrickName  %% atom
        end,
    MD5Disp =
        if
            MD5 =:= undefined ->
                n_a;    %% atom
            true ->
                MD5     %% binary
        end,
    lists:flatten(
      io_lib:format("Sizes (bytes): header: ~w, body: ~w, footer: ~w, padding: ~w~n"
                    "BrickName:     ~p~n"
                    "Type:          ~p~n"
                    "Flags:         ~p~n"
                    "#Blobs:        ~w~n"
                    "Blobs MD5:     ~w~n"
                    "BlobAges:      ~w~n"
                    "Blobs:~n~p",
                    [?HUNK_HEADER_SIZE,
                     (RawSize - ?HUNK_HEADER_SIZE - FooterSize),
                     FooterSize, PaddingSize,
                     BrickNameDisp, Type, Flags, length(Blobs), MD5Disp,
                     BlobAges, Blobs])).


%% ====================================================================
%% Test and debug (@TODO: eunit and quickcheck)
%% ====================================================================

test1() ->
    Brick = table1_ch1_b1,
    StoreTuple1 = term_to_binary({<<"key1">>,   brick_server:make_timestamp(), <<"val1">>}),
    StoreTuple2 = term_to_binary({<<"key12">>,  brick_server:make_timestamp(), <<"val12">>}),
    StoreTuple3 = term_to_binary({<<"key123">>, brick_server:make_timestamp(), <<"val123">>}),
    Blobs = [StoreTuple1, StoreTuple2, StoreTuple3],
    create_hunk_iolist(#hunk{type=metadata, brick_name=Brick, blobs=Blobs}).

test2() ->
    Brick = table1_ch1_b1,
    StoreTuple1 = term_to_binary({<<"key1">>,   brick_server:make_timestamp(), <<"val1">>}),
    StoreTuple2 = term_to_binary({<<"key12">>,  brick_server:make_timestamp(), <<"val12">>}),
    StoreTuple3 = term_to_binary({<<"key123">>, brick_server:make_timestamp(), <<"val123">>}),
    Blobs = [StoreTuple1, StoreTuple2, StoreTuple3],
    create_hunk_iolist(#hunk{type=metadata, flags=[no_md5],brick_name=Brick, blobs=Blobs}).

test3() ->
    Brick = table1_ch1_b1,
    Value1 = <<"val1">>,
    Blobs = [Value1],
    create_hunk_iolist(#hunk{type=blob_wal, brick_name=Brick, blobs=Blobs}).

test4() ->
    Value1 = <<"val1">>,
    Blobs = [Value1],
    create_hunk_iolist(#hunk{type=blob_single, blobs=Blobs}).

test5() ->
    Value1 = <<"val1">>,
    Value2 = <<"val12">>,
    Value3 = <<"val123">>,
    Blobs = [Value1, Value2, Value3],
    create_hunk_iolist(#hunk{type=blob_multi, blobs=Blobs}).

test6() ->
    Value1 = <<"val1">>,
    Value2 = <<"val12">>,
    Value3 = <<"val123">>,
    Blobs = [Value1, Value2, Value3],
    create_hunk_iolist(#hunk{type=blob_multi, flags=[no_md5], blobs=Blobs}).

test7() ->
    Value1 = <<"val1">>,
    Value2 = <<"val12">>,
    Value3 = <<"val123">>,
    Blobs = [Value1, Value2, Value3],
    {HunkIOData, _HunkSize, _Overhead, _BlobIndex} =
        create_hunk_iolist(#hunk{type=blob_multi, flags=[no_md5], blobs=Blobs}),
    parse_hunk_iodata(HunkIOData).

