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
%%% File    : brick_hlog_hunk.erl
%%% Purpose : log hunk used by brick_hlog.
%%%----------------------------------------------------------------------



%% @TODO: Turn on the compiler option to check the efficiency of
%%        binary operations.



%% @doc A hunk-based log server, a partial replacement for the
%% Erlang/OTP `disk_log' module, plus support for random access to
%% hunks stored within the log.
%% @end

%%
%% Log File Layout
%% - Superblock
%%   * Signature     (8 bytes)                   <<16#8A, "HLG", "\r", "\n", 16#1A, "\n">>
%%   * Version       (2 bytes, unsigned integer) <<0, 1>>
%%   * Reserved      (6 bytes)                   <<"Hibari">>
%% - Hunk 1
%%   * Header        (12 bytes)
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
%% Hunk Layout - Common Header
%% - Header (12 bytes, fixed length)
%%   * Header Magic Number (2 bytes)                    <<16#90, 16#7F>>  %% no meaning
%%   * Type (1 byte)
%%   * Flags (deleted, etc.) (1 byte)
%%   * BrickNameSize (2 bytes, ubint)                    0 for non-WAL hunks
%%   * NumberOfBlobs (2 bytes, ubnit)
%%   * TotalBlobSize (4 bytes, ubnit)                    Max value size is (4 GB - 1 byte)
%%
%% Hunk Layout - Body for metadata; many blobs in one hunk
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
%% Hunk Layout - Body for blob_wal; many blobs in one hunk
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
%% Hunk Layout - Body for blob_single; one blob in one hunk
%% - Body (variable length)
%%   * Blob (binary)
%% - Footer (variable length)
%%   * Footer Magic Number (2 bytes)                    <<16#07, 16#E3>>  %% no meaning
%%   * Blob Checksum (md5) (16 bytes) (optional)
%%   * Blob Index (4 bytes * 1, ubint)
%%   * Padding                                          Total hunk size is aligned to 8 bytes
%%
%% Hunk Layout - Body for blob_multi; many blobs in one hunk
%% - Body (variable length)
%%   * Blob1 (binary)
%%   * Blob2 (binary)
%%   * ...
%%   * BlobN (binary)
%% - Footer (variable length)
%%   * Footer Magic Number (2 bytes)                    <<16#07, 16#E3>>  %% no meaning
%%   * Blob Checksum (md5) (16 bytes) (optional)
%%   * Blob Index (4 bytes * NumberOfBlobs, ubint)
%%   * Padding                                          Total hunk size is aligned to 8 bytes
%%

-module(brick_hlog_hunk).

%% -include("gmt_hlog.hrl").
%% -include("brick.hrl").      % for ?E_ macros
-include("brick_hlog.hrl").

-export([calc_hunk_size/4,
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

-define(HUNK_HEADER_MAGIC, 16#90, 16#7F).
-define(HUNK_FOOTER_MAGIC, 16#07, 16#E3).

-define(TYPE_METADATA,     <<"m">>).
-define(TYPE_BLOB_WAL,     <<"w">>).
-define(TYPE_BLOB_SINGLE,  <<"s">>).
-define(TYPE_BLOB_MULTI,   <<"p">>).   %% "p" stands for "packed" blobs. ("m" is already taken)

-define(FLAG_DELETED,      16#01).
-define(FLAG_NO_MD5,       16#02).


%% ====================================================================
%% API
%% ====================================================================

-spec calc_hunk_size(hunk_type(), [hunk_flag()], non_neg_integer(), non_neg_integer())
                    -> {FooterSize::non_neg_integer(),
                        RawSize::non_neg_integer(),
                        PaddingSize::non_neg_integer(),
                        Overhead::non_neg_integer()}.
calc_hunk_size(Flags, BrickNameSize, NumberOfBlobs, TotalBlobSize) ->
    MD5Size =
        case has_md5(Flags) of
            true ->
                16;
            false ->
                0
        end,
    FooterSize =
        ?HUNK_MIN_FOOTER_SIZE
        + MD5Size
        + BrickNameSize
        + NumberOfBlobs * 4,
    RawSize = ?HUNK_HEADER_SIZE + TotalBlobSize + FooterSize,
    PaddingSize = ?HUNK_ALIGNMENT - RawSize rem ?HUNK_ALIGNMENT,
    Overhead = RawSize + PaddingSize - TotalBlobSize,
    {FooterSize, RawSize, PaddingSize, Overhead}.

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
create_hunk_iolist(#hunk{type=blob_single, brick_name=undefined, blobs=[_Blob]}=Hunk) ->
    create_hunk_iolist1(Hunk);
create_hunk_iolist(#hunk{type=blob_multi, brick_name=undefined}=Hunk) ->
    create_hunk_iolist1(Hunk).

-spec parse_hunks(binary()) -> {ok, [hunk()]} | {error, Reason::term(), [hunk()]}.
parse_hunks(Hunks) when is_binary(Hunks) ->
    parse_hunks1(Hunks, []).

-spec parse_hunk_iodata(iodata()) -> {ok, hunk(), Remainder::binary()} | {error, term()}.
parse_hunk_iodata(Hunk) when is_list(Hunk) ->
    parse_hunk_iodata(list_to_binary(Hunk));
parse_hunk_iodata(<<?HUNK_HEADER_MAGIC,
                    Type:1/binary,
                    Flags:1/unit:8,
                    BrickNameSize:2/unit:8,
                    NumberOfBlobs:2/unit:8,
                    TotalBlobSize:4/unit:8,
                    Rest/binary>>) ->
    DecodedType  = decode_type(Type),
    DecodedFlags = decode_flags(Flags),
    {FooterSize, _, PaddingSize, _} =
        calc_hunk_size(DecodedFlags, BrickNameSize, NumberOfBlobs, TotalBlobSize),
    RemainderPos  = TotalBlobSize + FooterSize + PaddingSize,
    RemainderSize = byte_size(Rest) - RemainderPos,

    BodyBin   = binary:part(Rest, 0, TotalBlobSize),
    FooterBin = binary:part(Rest, TotalBlobSize, FooterSize),
    Remainder = binary:part(Rest, RemainderPos, RemainderSize),

    case parse_hunk_footer(DecodedType, has_md5(DecodedFlags),
                           BrickNameSize, NumberOfBlobs, FooterBin) of
        {error, _}=Err ->
            Err;
        {ok, Md5, BrickName, BlobIndexBin} ->
            if
                DecodedType =:= blob_wal; DecodedType =:= blob_single ->
                    {ok, #hunk{type=DecodedType,
                               flags=DecodedFlags,
                               brick_name=BrickName,
                               blobs=[BodyBin],
                               md5=Md5},
                     Remainder};
                true ->
                    case parse_hunk_body(BodyBin, BlobIndexBin) of
                        {ok, Blobs} ->
                            {ok, #hunk{type=DecodedType,
                                       flags=DecodedFlags,
                                       brick_name=BrickName,
                                       blobs=Blobs,
                                       md5=Md5},
                             Remainder};
                        Err ->
                            Err
                    end
            end
    end;
parse_hunk_iodata(_) ->
    {error, invalid_format}.

%% read_hunk(FH, HunkOffset) ->
%%     Header = read_hunk_header_bytes(FH, HunkOffset),
%%     case parse_hunk_header(Header) of
%%         {ok, Type, Flags, BlobSize} ->
%%             BodyAndFooter = read_hunk_body_bytes(FH, HunkOffset, BlobSize),
%%             case parse_hunk_body(BodyAndFooter, BlobSize) of
%%                 {ok, Blobs, Md5} ->
%%                     {ok, #hunk{type=Type, flags=Flags, blobs=Blobs, md5=Md5}};
%%                 {error, _}=Err ->
%%                     Err
%%             end;
%%         {error, _}=Err ->
%%             Err
%%     end.

-spec read_blob_directly(file:fd(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
                                {ok, binary()} | eof | {error, term()}.
read_blob_directly(FH, HunkOffset, BlobOffset, BlobSize) ->
    file:pread(FH, HunkOffset + BlobOffset, BlobSize).


%% ====================================================================
%% Internal functions
%% ====================================================================

-spec create_hunk_iolist1(hunk())
                         -> {Hunk::iodata(),
                             HunkSize::non_neg_integer(), Overhead::non_neg_integer(),
                             BlobIndex::[non_neg_integer()]}.
create_hunk_iolist1(#hunk{type=Type, flags=Flags, brick_name=BrickName, blobs=Blobs}) ->
    {EncodedBrickName, BrickNameSize} = encode_brick_name(BrickName),
    {BlobIndex, NumberOfBlobs, TotalBlobSize} = create_blob_index(Blobs),
    {_FooterSize, RawSize, PaddingSize, Overhead} =
        calc_hunk_size(Flags, BrickNameSize, NumberOfBlobs, TotalBlobSize),
    HunkHeader = create_hunk_header(Type, Flags, BrickNameSize, NumberOfBlobs, TotalBlobSize),
    HunkFooter = create_hunk_footer(Type, Flags, EncodedBrickName, Blobs, BlobIndex, PaddingSize),
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
                         binary(), [binary()], [non_neg_integer()], non_neg_integer()) -> binary().
create_hunk_footer(Type, Flags, EncodedBrickName, Blobs, BlobIndex, PaddingSize) ->
    [<<?HUNK_FOOTER_MAGIC>>]
        ++ [ crypto:hash(md5, list_to_binary(Blobs)) || has_md5(Flags) ]
        ++ [ EncodedBrickName || Type =:= metadata orelse Type =:= blob_wal ]
        ++ [ <<Offset:4/unit:8>> || Offset <- BlobIndex ]
        ++ [<<0:PaddingSize/unit:8>>].

-spec create_blob_index([binary()])
                       -> {BlobIndex::[non_neg_integer()],
                           NumberOfBlobs::non_neg_integer(),
                           TotalBlobSize::non_neg_integer()}.
create_blob_index(Blobs) ->
    NumberOfBlobs = length(Blobs),
    Sizes = [ byte_size(Blob) || Blob <- Blobs ],
    TotalBlobSize = lists:sum(Sizes),
    {SizesWithoutLastBlob, _} = lists:split(NumberOfBlobs - 1, Sizes),
    {_, BlobIndex} =
        lists:foldl(
          fun(Size, {Offset, Index}) ->
                  Offset1 = Offset + Size,
                  {Offset1, [Offset1 | Index]}
          end, {?HUNK_HEADER_SIZE, []}, [0|SizesWithoutLastBlob]),
    {lists:reverse(BlobIndex), NumberOfBlobs, TotalBlobSize}.

parse_hunks1(<<>>, Acc) ->
    lists:reverse(Acc);
parse_hunks1(Hunks, Acc) ->
    case parse_hunk_iodata(Hunks) of
        {ok, Hunk, Remainder} ->
            parse_hunks1(Remainder, [Hunk | Acc]);
        {error, Reason} ->
            {error, Reason, lists:reverse(Acc)}
    end.

-spec parse_hunk_body(binary(), binary()) -> [binary()].
parse_hunk_body(Blobs, <<Offset:4/unit:8, Rest/binary>>) ->
    case parse_hunk_body1(Blobs, Offset - ?HUNK_HEADER_SIZE, Rest, []) of
        BlobList when is_list(BlobList) ->
            {ok, BlobList};
        Err ->
            Err
    end.

-spec parse_hunk_body1(binary(), non_neg_integer(), binary(), [binary()]) -> [binary()].
parse_hunk_body1(Blobs, RelativeOffset1, <<>>, Acc) ->
    Blob = binary:part(Blobs, RelativeOffset1, byte_size(Blobs) - RelativeOffset1),
    lists:reverse([Blob | Acc]);
parse_hunk_body1(Blobs, RelativeOffset1, <<Offset2:4/unit:8, Rest/binary>>, Acc) ->
    RelativeOffset2 = Offset2 - ?HUNK_HEADER_SIZE,
    Blob = binary:part(Blobs, RelativeOffset1, RelativeOffset2 - RelativeOffset1),
    parse_hunk_body1(Blobs, RelativeOffset2, Rest, [Blob | Acc]).

-spec parse_hunk_footer(hunk_type(), boolean(), non_neg_integer(), non_neg_integer(), binary())
                       -> {ok, Md5::binary(), BrickName::atom(), BlobIndexBinary::binary()}
                              | {error, term()}.
parse_hunk_footer(metadata, true, BrickNameSize, _NumberOfBlobs,
                  <<?HUNK_FOOTER_MAGIC, Bin/binary>>) ->
    case Bin of
        <<Md5:16/binary, BrickName:BrickNameSize/binary, BlobIndexBin/binary>> ->
            {ok, Md5, decode_brick_name(BrickName), BlobIndexBin};
        _ ->
            {error, invalid_format}
    end;
parse_hunk_footer(metadata, false, BrickNameSize, _NumberOfBlobs,
                  <<?HUNK_FOOTER_MAGIC, Bin>>) ->
    case Bin of
        <<BrickName:BrickNameSize/binary, BlobIndexBin/binary>> ->
            {ok, undefined, decode_brick_name(BrickName), BlobIndexBin};
        _ ->
            {error, invalid_format}
    end;
parse_hunk_footer(blob_wal, HasMd5, BrickNameSize, _, <<?HUNK_FOOTER_MAGIC, Bin/binary>>) ->
    case {HasMd5, Bin} of
        {true, <<Md5:16/binary, BrickName:BrickNameSize/binary>>} ->
            {ok, Md5, decode_brick_name(BrickName), <<>>};
        {false, <<BrickName:BrickNameSize/binary>>} ->
            {ok, undefined, decode_brick_name(BrickName), <<>>};
        _ ->
            {error, invalid_format}
    end;
parse_hunk_footer(blob_single, true, 0, 1, <<?HUNK_FOOTER_MAGIC, Md5:16/binary, BlobIndexBin/binary>>) ->
    {ok, Md5, undefined, BlobIndexBin};
parse_hunk_footer(blob_single, false, 0, 1, <<?HUNK_FOOTER_MAGIC, BlobIndexBin/binary>>) ->
    {ok, undefined, undefined, BlobIndexBin};
parse_hunk_footer(blob_multi, true, 0, _NumberOfBlobs,
                  <<?HUNK_FOOTER_MAGIC, Md5:16/binary, BlobIndexBin/binary>>) ->
    {ok, Md5, undefined, BlobIndexBin};
parse_hunk_footer(blob_multi, false, 0, _NumberOfBlobs,
                  <<?HUNK_FOOTER_MAGIC, BlobIndexBin/binary>>) ->
    {ok, undefined, undefined, BlobIndexBin}.

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
    lists:foldl(fun(deleted, Acc) ->
                        Acc bor ?FLAG_DELETED;
                   (no_md5, Acc) ->
                        Acc bor ?FLAG_NO_MD5;
                   (Flag, _Acc) ->
                        error({invalid_flag, Flag})
                end, 0, Flags).

-spec decode_flags(non_neg_integer()) -> [hunk_flag()].
decode_flags(Flags) when is_integer(Flags) ->
    lists:foldl(fun(deleted, Acc) ->
                        if
                            Flags band ?FLAG_DELETED =/= 0 ->
                                [deleted | Acc];
                            true ->
                                Acc
                        end;
                   (no_md5, Acc) ->
                        if
                            Flags band ?FLAG_NO_MD5 =/= 0 ->
                                [no_md5 | Acc];
                            true ->
                                Acc
                        end
                end, [], [deleted, no_md5]).

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


%% DEBUG STUFF (@TODO: eunit and quickcheck)

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

