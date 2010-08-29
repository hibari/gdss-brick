%%%-------------------------------------------------------------------
%%% Copyright: (c) 2008-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : mod_admin.erl
%%% Purpose : Web based administration for inets/httpd and storage bricks.
%%%-------------------------------------------------------------------

%% @doc An inets/httpd module to provide an admin interface for storage bricks.
%%
%% This module provides a web based administration interface for storage bricks.
%%
%% At the moment, this module does the following:
%% <ul>
%% </ul>

-module(mod_admin).
-include("applog.hrl").


-compile([binary_comprehension]).

%% External API
-export([make_proplist/3]).

%% EWSAPI API
-export([do/1, load/2]).

%% Testing API
-export([escape/1, uri_decode/1]).

%% Experimental API, move to another module?
-export([dump_history/0, dump_history/1, dump_history_in_columns/0,
         admin_server_http_port/0]).

-ifdef(new_inets).
-include_lib("inets/src/http_server/httpd.hrl").
-else.
-include_lib("inets/src/httpd.hrl").
-endif.
-include("brick_hash.hrl").
-include("brick_admin.hrl").

-define(VMODULE,"admin").
-define(ADMIN_MAX_KEYS, 55555).
-define(ADMIN_TABLE, 'tab1').

%% @spec (mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc EWSAPI request callback.

do(ModData) ->
    Me = node(),
    AdminServer = case global:whereis_name(brick_admin) of
                      undefined -> node();
                      APid      -> node(APid)
                  end,
    if Me /= AdminServer ->
            [_, Host] = string:tokens(atom_to_list(AdminServer), "@"),
            URL = "http://" ++ Host ++ ":23080/",
            {proceed, [{response,{response,[{code, 307},
                                            {location, URL}],
                                  ["Please go to <a href=\"", URL, "\">",
                                   URL, "</a>\n"]}}]};
       true ->
            do2(ModData)
    end.

do2(ModData) ->
    try
        case proplists:get_value(status, ModData#mod.data) of
            {_StatusCode, _PhraseArgs, _Reason} ->
                {proceed, ModData#mod.data};
            undefined ->
                case proplists:get_value(response, ModData#mod.data) of
                    undefined ->
                        case ModData#mod.method of
                            "GET" ->
                                do_get(ModData);
                            "POST" ->
                                do_post(ModData);
                            "HEAD" ->
                                do_head(ModData);
                            _ ->
                                {proceed, ModData#mod.data}
                        end;
                    _Response ->
                        {proceed, ModData#mod.data}
                end
        end
    catch
         Type:Reason ->
             Stack = erlang:get_stacktrace(),
             Msg = io_lib:format("mod_admin encountered an error: ~p:~p at ~p", [Type, Reason, Stack]),
             write_error(500, "ApplicationError", Msg, ModData)
    end.

%% @spec (string(), list()) ->  eof |
%%                     ok |
%%                     {ok, list()} |
%%                     {ok, list(), tuple()} |
%%                     {ok, list(), list()} |
%%                     {error, term()}
%% @doc EWSAPI config callback.
load("AdminOptFoo " ++ AuthArg, [])->
    case catch list_to_atom(httpd_conf:clean(AuthArg)) of
        true ->
            {ok, [], {admin_opt_foo, true}};
        false ->
            {ok, [], {admin_opt_foo, false}};
        _ ->
            {error, ?NICE(httpd_conf:clean(AuthArg) ++ " is an invalid AdminOptFoo directive")}

    end;

load("AdminOptBar " ++ AuthArg, [])->
    case catch list_to_atom(httpd_conf:clean(AuthArg)) of
        true ->
            {ok, [], {admin_opt_bar, true}};
        false ->
            {ok, [], {admin_opt_bar, false}};
        _ ->
            {error, ?NICE(httpd_conf:clean(AuthArg) ++ " is an invalid AdminOptBar directive")}

    end.

%% @spec (mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Handle all GET requests.
do_get(ModData) ->
    case string:tokens(ModData#mod.request_uri, "?") of
        [Path, Query] ->
            if
                Path == "/history" ->
                    get_history(Query, ModData);
                Path == "/table" ->
                    get_table(Query, ModData);
                Path == "/chain" ->
                    get_chain(Query, ModData);
                Path == "/brick" ->
                    get_brick(Query, ModData);
                Path == "/node" ->
                    get_node(Query, ModData);
                Path == "/bootstrap" ->
                    get_bootstrap(Query, ModData);
                Path == "/add_table" ->
                    get_add_table(Query, ModData);
                Path == "/change_client_monitor" ->
                    get_change_client_monitor(Query, ModData);
                Path == "/dump_history" ->
                    get_dump_history(Query, ModData);
                true ->
                    {proceed, ModData#mod.data}
            end;
        [Path] ->
            if
                Path == "/" ->
                    get_root(ModData);
                Path == "/add_table.html" ->
                    get_add_table_html(ModData);
                Path == "/change_client_monitor.html" ->
                    get_change_client_monitor_html(ModData);
                Path == "/dump_history.html" ->
                    get_dump_history_html(ModData);
                true ->
                    {proceed, ModData#mod.data}
            end
    end.


%% @spec (mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Handle all HEAD requests.
do_head(ModData) ->
    {proceed, ModData#mod.data}.

%% @spec (mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Handle all POST requests.
do_post(ModData) ->
    _Path = ModData#mod.request_uri,
    if
        true ->
            {proceed, ModData#mod.data}
    end.

%% @spec (string(), list()) -> string()
%% @doc Escape HTML special characters.
escape([], Acc) -> lists:flatten(lists:reverse(Acc));
escape([C|R], Acc) -> escape(R, [escape(C) | Acc]).

%% @spec (string()) -> string()
%% @doc Escape HTML special characters.
escape(L) when is_list(L) -> escape(L, []);

%% @spec (integer()) -> string()
%% @doc Escape HTML special characters.
escape(C) when is_integer(C)-> [$&, $#, integer_to_list(C), $;].

uri_decode([], Acc) ->
    lists:flatten(lists:reverse(Acc));
uri_decode([37, X1, X2 | Rest], Acc) ->
    Hex = [X1, X2],
    Int = httpd_util:hexlist_to_integer(Hex),
    uri_decode(Rest, [Int | Acc]);
uri_decode([$+ | Rest], Acc) ->
    uri_decode(Rest, [" " | Acc]);
uri_decode([C | Rest], Acc) ->
    uri_decode(Rest, [C | Acc]).

uri_decode(Str) ->
    uri_decode(Str, []).

%% @spec (integer(), string(), string(), mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Write a generic error to the HTTP response, and return to EWSAPI.
write_error(StatusCode, Code, Message, ModData) ->
    Msg = [
<<"<html>\r\n">>,
<<"  <head><title>">>, Code, <<"</title></head>\r\n">>,
<<"  <body>\r\n">>,
<<"    <h1>">>, Code, <<"</h1>\r\n">>,

<<"    <pre>">>, Message, <<"</pre>\r\n">>,
<<"  </body>\r\n">>,
<<"</html>\r\n">>],

    httpd_response:send_header(ModData, StatusCode, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, StatusCode, 0}} | ModData#mod.data]}.

%% @spec (time()) -> string()
%% @doc Format a timestamp into a string (including microseconds).

make_date(Now) ->
    {_Mega, _Sec, Micro} = Now,
    {Date, Time} = calendar:now_to_local_time(Now),
    {Year, Month, Day} = Date,
    {Hour, Minute, Second} = Time,

    io_lib:format("~4.10b-~2.10.0b-~2.10.0b ~2.10.0b:~2.10.0b:~2.10.0b.~6.10.0b", [Year, Month, Day, Hour, Minute, Second, Micro]).

make_brick_header() ->
    make_brick_header(<<"">>, true).

make_brick_header(FullP) ->
    make_brick_header(<<"">>, FullP).

make_brick_header(Header, FullP) ->
    [<<"<table><tr class='head'><th>Name</th>">>, Header,
     if FullP ->
             [<<"<th>Role</th><th>OT</th><th>CRS</th><th>CDRS</th><th>CUS</th><th>CDS</th><th>CRO</th><th>Size</th><th>Memory</th><th>Log</th><th>Sync</th><th>A</th><th>R</th><th>S</th><th>G</th><th>M</th><th>D</th><th>T</th><th>O</th><th>Old</th><th>Exp</th><th>Checkpoint</th>">>];
        true ->
             [<<"<th>Role</th><th>Status</th><th>ChkP</th><th>Keys</th><th>Mem</th>">>]
     end,
     <<"<th>Node</th></tr>\r\n">>].

make_brick_footer() ->
    make_brick_footer(true).

make_brick_footer(FullP) ->
    [<<"</table>\r\n">>,
     if FullP ->
             [<<"<p>Role=chain role, OT=chain official tail, CRS=chain repair state, CDRS=chain downstream repair state, CUS=chain upstream serial, CDS=chain downstream serial, CRO=chain read only, Size=number of keys, Memory=memory used, Log=logging enabled Sync=synchronous writes enabled, A=number of add ops, R=number of replace ops, S=number of set ops, G=number of get ops, M=number of get_many ops, D=number of delete ops, T=number of transactions, O=number of do ops, Old=number of 'too old' ops, Exp=number of expired keys, Checkpoint=checkpoint status</p>\r\n">>];
        true ->
             []
     end].

make_data(Status) ->
    make_data(Status, "warn").

make_data(Status, DefaultClass) ->
    Class = case Status of
                ok -> <<" class='ok'">>;
                healthy -> <<" class='ok'">>;
                undefined -> <<" class='ok'">>;
                pre -> <<" class='ok'">>;
                {sweep_key, _} -> <<" class='ok'">>;
                degraded -> <<" class='warn'">>;
                error -> <<" class='error'">>;
                stopped -> <<" class='error'">>;
                unknown -> <<" class='error'">>;
                unknown_timeout -> <<" class='error'">>;
                disk_error -> <<" class='error'">>;
                _ -> [" class='", DefaultClass, "'"]
            end,
    List = if is_atom(Status) ->
                   atom_to_list(Status);
              true ->
                   io_lib:format("~p", [Status])
           end,

    [<<"<td">>, Class, <<">">>, List, <<"</td>">>].

make_brick_row_parallel(Bricks, FullP) ->
    {Pre, _, _, _} = gmt_pmap:pmap(fun({Br, Nd}) ->
                                           make_brick_row(Br, Nd, FullP)
                                   end, Bricks),
    [Res || {_, Res} <- Pre].

make_brick_row(Brick, Node, FullP) ->
    make_brick_row(Brick, Node, FullP, <<"">>).

make_brick_row(Brick, Node, FullP, Data0) ->
    try
        {ok, Status} = brick_server:status(Brick, Node),

        Chain = proplists:get_value(chain, Status, []),
        _Hash = proplists:get_value(hash, Status, []),
        Impl = proplists:get_value(implementation, Status, []),
        Sweep = proplists:get_value(sweep, Status, []),

        ChainRole = proplists:get_value(chain_role, Chain, unknown),
        ChainProp = proplists:get_value(chain_proplist, Chain, []),

        COT = proplists:get_value(chain_official_tail, Chain, unknown),
        CRS = proplists:get_value(chain_my_repair_state, Chain, unknown),
        CDRS = proplists:get_value(chain_ds_repair_state, Chain, unknown),

        CUS = proplists:get_value(chain_up_serial, Chain, 0),
        CDS = proplists:get_value(chain_down_serial, Chain, 0),

        CRO = proplists:get_value(chain_read_only_p, Chain, unknown),

        CR = case ChainRole of
                 chain_member ->
                     case ChainProp of
                         [] ->
                             ChainRole;
                         [Role | _Props] ->
                             Role
                     end;
                 _ ->
                     ChainRole
             end,

        _IStart = proplists:get_value(start_time, Impl, unknown),
        _IOptions = proplists:get_value(options, Impl, []),
        IEts = proplists:get_value(ets, Impl, []),

        IES = proplists:get_value(size, IEts, 0),
        IEM = proplists:get_value(memory, IEts, 0),

        IDL = proplists:get_value(do_logging, Impl, unknown),
        IDS = proplists:get_value(do_sync, Impl, unknown),
        INA = proplists:get_value(n_add, Impl, 0),
        INR = proplists:get_value(n_replace, Impl, 0),
        INS = proplists:get_value(n_set, Impl, 0),
        ING = proplists:get_value(n_get, Impl, 0),
        INGM = proplists:get_value(n_get_many, Impl, 0),
        IND = proplists:get_value(n_delete, Impl, 0),
        INT = proplists:get_value(n_txn, Impl, 0),
        INDO = proplists:get_value(n_do, Impl, 0),
        ITooOld = proplists:get_value(n_too_old, Status, 0),
        INExp = proplists:get_value(n_expired, Impl, 0),
        IX = case proplists:get_value(checkpoint, Impl) of
                 undefined ->
                     case proplists:get_value(sweep_key, Sweep) of
                         undefined -> undefined;
                         SweepKey  -> {sweep_key, SweepKey}
                     end;
                 CP_val ->
                     CP_val
             end,

        [<<"<tr class='data'>">>,
         Data0,
         <<"<td><a href='/brick?name=">>, atom_to_list(Brick), <<"&node=">>, atom_to_list(Node), <<"'>">>, atom_to_list(Brick), <<"</a></td>">>,
         if FullP ->
                 [<<"<td>">>, atom_to_list(CR), <<"</td>">>,
                  <<"<td>">>, atom_to_list(COT), <<"</td>">>,
                  make_data(CRS),
                  make_data(CDRS),
                  <<"<td>">>, integer_to_list_or_no_serial(CUS), <<"</td>">>,
                  <<"<td>">>, integer_to_list(CDS), <<"</td>">>,
                  <<"<td>">>, atom_to_list(CRO), <<"</td>">>,
                  <<"<td>">>, integer_to_list(IES), <<"</td>">>,
                  <<"<td>">>, integer_to_list(IEM), <<"</td>">>,
                  <<"<td>">>, atom_to_list(IDL), <<"</td>">>,
                  <<"<td>">>, atom_to_list(IDS), <<"</td>">>,
                  <<"<td>">>, integer_to_list(INA), <<"</td>">>,
                  <<"<td>">>, integer_to_list(INR), <<"</td>">>,
                  <<"<td>">>, integer_to_list(INS), <<"</td>">>,
                  <<"<td>">>, integer_to_list(ING), <<"</td>">>,
                  <<"<td>">>, integer_to_list(INGM), <<"</td>">>,
                  <<"<td>">>, integer_to_list(IND), <<"</td>">>,
                  <<"<td>">>, integer_to_list(INT), <<"</td>">>,
                  <<"<td>">>, integer_to_list(INDO), <<"</td>">>,
                  <<"<td>">>, integer_to_list(ITooOld), <<"</td>">>,
                  <<"<td>">>, integer_to_list(INExp), <<"</td>">>,
                  make_data(IX)];

            true ->
                 [<<"<td>">>, atom_to_list(CR),
                  if COT -> <<" + official_tail">>;
                     true -> <<"">>
                                 end,
                  <<"</td>">>,
                  make_data(CRS),
                  make_data(IX),
                  <<"<td>">>, integer_to_list(IES), <<"</td>">>,
                  <<"<td>">>, integer_to_list(IEM), <<"</td>">>]
         end,
         <<"<td><a href='/node?name=">>, atom_to_list(Node), <<"'>">>, atom_to_list(Node), <<"</a></td>">>,
         <<"</tr>\r\n">> ]
    catch
        _:_ ->
            [<<"<tr class='error'>">>,
             Data0,
             <<"<td><a href='/brick?name=">>, atom_to_list(Brick), <<"&node=">>, atom_to_list(Node), <<"'>">>, atom_to_list(Brick), <<"</a></td>">>,
             if
                 FullP ->
                     [<<"<td colspan='20'>Unable to read brick status!</td>">>];
                 true ->
                     [<<"<td colspan='4'>Unable to read brick status!</td>">>]
             end,
             <<"<td><a href='/node?name=">>, atom_to_list(Node), <<"'>">>, atom_to_list(Node), <<"</a></td>">>,
             <<"</tr>">>
            ]
    end.

%% @spec () -> iolist()
%% @doc Write HTML table for bricks from passed chain, or all chains if passed 'all'.
make_brick_table() ->
    Tables = lists:sort(brick_admin:get_tables()),

    [make_brick_header(false),
     make_brick_table_parallel(Tables),
     make_brick_row_parallel(brick_admin:get_schema_bricks(), false),
     make_brick_footer(false)].

make_brick_table_parallel(Tables) ->
    {Pre, _, _, _} =
        gmt_pmap:pmap(
          fun(Table) ->
                  {ok, Info} = brick_admin:get_table_info(Table),
                  GHash = proplists:get_value(ghash, Info, {no_hash}),
                  ChainList = get_all_chains(GHash),
                  Bricks = [BrickList || {_ChainName, BrickList} <- ChainList],
                  make_brick_row_parallel(lists:sort(lists:append(Bricks)),
                                          false)
          end, Tables),
    [Res || {_, Res} <- Pre].

make_history_header() ->
    make_history_header(<<"">>).

make_history_header(Col0) ->
    [<<"<table>\r\n">>,
     <<"  <tr class='head'>">>, Col0, <<"<th>Event</th><th>State</th><th>Properties</th><th>Date</th></tr>\r\n">>].

make_history_footer() ->
    [<<"</table>">>].

make_history_row(Event) ->
    make_history_row(Event, <<"">>).

make_history_row(Event, Col0) ->
    {hevent, Time, Type, State, Props} = Event,
    Date = make_date(Time),

    [<<"<tr class='data'>">>,
     Col0,
     <<"<td>">>, atom_to_list(Type), <<"</td>">>,
     make_data(State),
     <<"<td>">>, escape(io_lib:format("~p", [Props])), <<"</td>">>,
     <<"<td>">>, Date, <<"</td></tr>">>].


make_brick_history_table(Brick, Node) ->
    {ok, EventList} = brick_sb:get_brick_history(Brick, Node),
    make_history_table(EventList).

make_chain_history_table(Chain) ->
    {ok, EventList} = brick_sb:get_chain_history(Chain),
    make_history_table(EventList).

make_history_table(EventList) ->
    [make_history_header(),
     [make_history_row(Event) || Event <- EventList],
     make_history_footer()].

make_root_tables(TableList) ->
    AdminStr = case global:whereis_name(brick_admin) of
                   undefined -> "not running";
                   Pid       -> io_lib:format("~p", [node(Pid)])
               end,
    AdminServer = [ <<"<h2>Admin Server</h2>\n">>,
                    <<"<p> The GDSS Admin Server is running on node: ">>,
                    AdminStr,
                    <<"\n\n">>,
                    "<p> The current date and time is: ",
                    io_lib:format("~p ~p\n", [date(), time()])
                   ],

    Alarms =
        case lists:sort(get_all_nodes_alarms()) of
            [] ->
                "<h2>No Alarms</h2>\n";
            As ->
                ["<h2>Alarms Are Set</h2>\n",
                 "<table><tr class='head'><th>Node</th><th>Name</th><th>Data</th></tr>\n",
                 [["<tr class='data'><th>", nice_fmt(Node), "</th>",
                   "<th>", nice_fmt(AName), "</th>",
                   "<th>", nice_fmt(AData), "</th></tr>"]
                  || {Node, AName, AData} <- As],
                 "</table>\n"
                ]
        end,

    Tables = [ <<"<h2>Tables</h2><table><tr class='head'><th>Name</th><th>Rev</th><th>Phase</th><th>Chains</th><th>Brick Options</th></tr>">>,
               try
                   [ begin
                         {ok, Table} = brick_admin:get_table_info(Name),
                         {ok, _, GH} = brick_simple:get_gh(Name),
                         LH = GH#g_hash_r.new_h_desc,
                         BrickOptions =
                             if LH#hash_r.method == chash ->
                                     %% Hrm, hard work to preserve order.
                                     P1=brick_hash:chash_extract_old_props(GH),
                                     P2 = proplists:unfold(P1),
                                     Ks = gmt_util:list_unique_u(
                                            [K || {K, _} <- P2]),
                                     [{K, proplists:get_value(K, P2)} ||
                                         K <- Ks];
                                true ->
                                     %% This list is when the table was created
                                     %% not its most recent settings.
                                     proplists:get_value(brick_options,
                                                         Table, [])
                             end,
                         Rev = proplists:get_value(current_rev, Table, []),
                         Phase = proplists:get_value(current_phase, Table, []),
                         GHash = proplists:get_value(ghash, Table, {no_hash}),
                         CChains = brick_hash:all_chains(GHash, current),
                         NChains = brick_hash:all_chains(GHash, new),

                         [
                          <<"<tr class='data'><td><a href='/table?name=">>, atom_to_list(Name), <<"'>">>, atom_to_list(Name), <<"</a></td><td>">>,
                          integer_to_list(Rev), <<"</td>">>, make_data(Phase), <<"<td>">>, integer_to_list(length(CChains)), <<" / ">>,
                          integer_to_list(length(NChains)), <<"</td><td>">>, escape(io_lib:format("~p", [BrickOptions])), <<"</td></tr>">>]


                     end || Name <- lists:sort(TableList)]
               catch
                   _X:_Y ->
                       [<<"<tr class='error'><td colspan='5'>Error reading table data!</td></tr>">>, "<p>\n", io_lib:format("~p:~p: ~p\n", [_X, _Y, erlang:get_stacktrace()])]
               end,
               <<"</table">> ],

    Chains = [ <<"<h2>Chains</h2><table><tr class='head'><th>Name</th><th>Bricks</th><th>Status</th></tr>">>,
               try
                   [ begin
                         {ok, Table} = brick_admin:get_table_info(Name),
                         GHash = proplists:get_value(ghash, Table, {no_hash}),
                         ChainList = lists:sort(get_all_chains(GHash)),
                         Chains = [Ch || {Ch, _} <- ChainList],
                         {ok, Results0} = brick_sb:get_multiple_statuses(
                                            [{chain, Ch} || Ch <- Chains]),
                         Results = lists:map(fun({ok, X}) -> X;
                                                (X)       -> X
                                             end, Results0),
                         Dict = dict:from_list(lists:zip(Chains, Results)),
                         [try begin
                              Status = dict:fetch(ChainName, Dict),
                              [<<"<tr class='data'><td><a href='/chain?name=">>, atom_to_list(ChainName), <<"'>">>, atom_to_list(ChainName), <<"</a></td><td>">>, integer_to_list(length(BrickList)), <<"</td>">>, make_data(Status), <<"</tr>">>]
                          end
                          catch _:_ ->
                                  ["<tr class='data'><td><a href='/chain?name=", atom_to_list(ChainName), "'>", atom_to_list(ChainName), "</a></td><td>Status failure</td></tr>"]
                          end
                          || {ChainName, BrickList} <- ChainList ]
                     end || Name <- lists:sort(TableList)]
               catch
                   _X2:_Y2 ->
                       [<<"<tr class='error'><td colspan='3'>Error reading chain data!</td></tr>">>, "<p>\n", io_lib:format("~p:~p: ~p\n", [_X2, _Y2, erlang:get_stacktrace()])]
               end,
               <<"</table">> ],

    Bricks = [ <<"<h2>Bricks</h2>\r\n">>, make_brick_table() ],

    NodeList =
        try
            lists:flatten([ begin
                                {ok, Table} = brick_admin:get_table_info(Name),
                                GHash = proplists:get_value(ghash, Table, {no_hash}),
                                ChainList = get_all_chains(GHash),

                                [ [Node || {_Brick, Node} <- BrickList] || {_ChainName, BrickList} <- ChainList ]

                            end || Name <- TableList])
        catch
            _:_ ->
                []
        end,

    NodeSet = sets:from_list(NodeList),

    Nodes = [ <<"<h2>Nodes</h2><table><tr class='head'><th>Name</th></tr>">>,
              [ [<<"<tr class='data'><td><a href='/node?name=">>, atom_to_list(Node), <<"'>">>, atom_to_list(Node), <<"</a></td></tr>">>] || Node <- lists:sort(sets:to_list(NodeSet))],
              <<"</table>">> ],

    Exp1 = ["<h2>Experimental</h2>\n",
            "<ul>\n",
            "<li> <a href=\"/add_table.html\">Add a table.</a>\n"
            "<li> <a href=\"/change_client_monitor.html\">Add/Delete a client node monitor.</a>\n",
            "<li> <a href=\"/dump_history.html\">Dump history.</a>\n"
            "</ul>\n"
           ],

    [AdminServer, Alarms, Tables, Chains, Bricks, Nodes, Exp1].

make_bool_select(Name) ->
    [<<"<td><select name='">>, Name, <<"'><option>true</option><option>false</option></select></td>">>].

make_select(Name, Items) ->
    [
     "<td><select name='", Name, "'>",
     [["<option>", I, "</option>"] || I <- Items],
     "</select></td>"
    ].

make_text(Name, Size, Val) ->
    [<<"<td><input type='text' size='">>, integer_to_list(Size), <<"' name='">>, Name, <<"' value='">>, Val, <<"'></input></td>">>].

make_text(Name) ->
    [<<"<td><input class='expand' type='text' name='">>, Name, <<"'></input></td>">>].

make_submit(Name) ->
    [<<"<td><input type='submit' value='">>, Name, <<"'></input></td>">>].

make_reset(Name) ->
    ["<td><input type='reset' value='", Name, "'></input></td>"].

make_bootstrap_tables() ->
    {AdminStr, Boots} = read_bootstrap_nodes(),
    M1 = if Boots == [] ->
                 ["<h2>WARNING: Bad config value for 'admin_server_distributed_nodes'</h2>\n",
                  "<p> The current value for the central.conf parameter "
                  "'admin_server_distributed_nodes' is invalid: "
                  "<b>", AdminStr, "</b>.\n"
                  "<p> If this GDSS cluster will remain a single node "
                  "(i.e. for testing or development work), then you may "
                  "continue with the form below.  CAUTION: The value of "
                  "the 'Local' attribute should be 'true'!\n",
                  "<p> If this GDSS cluster will be larger than a single "
                  "node, then <b>this config option must be corrected before ",
                  "continuing any further</b>.  The exact same string must ",
                  "be used in the central.conf file for each of the GDSS "
                  "nodes in the cluster.\n",
                  "<hr><hr><hr>\n"
                 ];
            true ->
                 AAL = [{Nd, catch rpc:call(Nd, gmt_config, get_config_value,
                                            [admin_server_distributed_nodes, ""])}
                        || Nd <- Boots],
                 AllEqualP = case lists:usort([Str || {_Nd, Str} <- AAL]) of
                                 [Str] when Str == AdminStr -> true;
                                 _                          -> false
                             end,
                 AllBootstrapRunningP =
                     case lists:usort([catch net_adm:ping(Nd) || Nd<- Boots]) of
                         [pong] -> true;
                         _      -> false
                     end,
                 ["<h2>Checking config attribute 'admin_server_distributed_nodes'</h2>\n",
                  "<p> <ul>\n",
                  [ io_lib:format("<li> central.conf on node ~p: ~p\n",
                                  [Nd, Res])
                    || {Nd, Res} <- AAL],
                  "</ul>",
                  if AllEqualP and AllBootstrapRunningP ->
                          ["<p> Congratulations, the value of ",
                           "'admin_server_distributed_nodes' is the same on "
                           "all ", integer_to_list(length(AAL)), " node(s).\n",
                           "<hr>\n"
                          ];
                     true ->
                          ["<p>\n",
                           "<b>The value of 'admin_server_distributed_nodes' "
                           "is not the same on all nodes.  This problem must "
                           "be fixed before continuing with the bootstrap "
                           "process.</b>\n"] ++
                              if not AllBootstrapRunningP ->
                                      ["<p> NOTICE: Not all nodes specified by ",
                                       "the 'admin_server_distributed_nodes' "
                                       "attribute are running.  All such nodes "
                                       "must be running before the bootstrap "
                                       "process can succeed.\n"];
                                 true ->
                                      ""
                              end ++
                              ["<p> If it is your intent to have a single-node "
                               "GDSS cluster, please edit central.conf to set "
                               "the value of 'admin_server_distributed_nodes' to ",
                               "be the node name of the single node.\n",
                               lists:duplicate(64, "<hr>")]
                  end
                 ]
         end,
    ["<h2>Unable to Contact Admin Server</h2>\n",
     "<p>The cluster is not yet configured and running.  If you believe ",
     "that the cluster's Admin Server has been configured, please wait ",
     "30 seconds, then reload this page to fetch new cluster info.\n",
     "<hr>\n",
     M1,
     <<"<h2>Bootstrap Cluster</h2>\r\n">>,
     <<"<p>There are several options for bootstrapping the cluster:</p>\r\n">>,
     add_table_html_form("tab1", "/bootstrap")
    ].

%% @spec (mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Get toplevel page; allow user to select from tables, chains, bricks, nodes
get_root(ModData) ->
    Head = [
            <<"<html>\r\n">>,
            <<"  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>Storage Brick Web Administration</title></head>\r\n">>,
            <<"  <body>\r\n">>,
            <<"    <h1>Storage Brick Web Administration</h1>\r\n">>],

    Tail = [
            <<"  </body>\r\n">>,
            <<"</html>\r\n">>],

    Body =
        try
            TableList = brick_admin:get_tables(),
            make_root_tables(TableList)
        catch
            _X:_Y ->
                ?APPLOG_WARNING(?APPLOG_APPM_102,"table gen: ~p ~p at ~p\n", [_X, _Y, erlang:get_stacktrace()]),
                make_bootstrap_tables()
        end,

    Msg = [Head, Body, Tail],

    httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, 200, iolist_size(Msg)}} | ModData#mod.data]}.

%% @spec (atom(), list(tuple())) -> iolist()
%% @doc Recursively create rows for passed chain name/brick list
make_chain_rows(_ChainName, []) -> [];
make_chain_rows(ChainName, [{Brick, Node} | BrickList]) ->
    NameBG = case length(atom_to_list(ChainName)) of
                 0 -> <<" class='blank'">>;
                 _ -> <<"">>
             end,

    [make_brick_row(Brick, Node, true, [<<"<td">>, NameBG, <<">">>, atom_to_list(ChainName), <<"</td>">>]), make_chain_rows('', BrickList)].

make_proplist(String, ListSep, TokSep) ->
    %% -- "  ..." hack is brutally simple for removing spaces in the string.
    %% 39 = single quote, in case someone quoted an atom in a form
    %% or in central.conf.  If someone wanted to use single quote (or space)
    %% for something else, tough luck.
    Tokens = string:tokens(String -- "                              ", ListSep),
    KVList =
        [
          begin
              case string:tokens(Token, TokSep) of
                  [Key, Val] ->
                      {list_to_atom(string:strip(Key, both, 39)), Val};
                  [Key] ->
                      list_to_atom(string:strip(Key, both, 39))
              end
          end
          || Token <- Tokens],
    KVList.

%% @spec (string(), mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Get admin information regarding the passed table.
get_table(Query, ModData) ->
    KVList = make_proplist(Query, "&", "="),
    Name = proplists:get_value(name, KVList, []),

    Head = [
<<"<html>\r\n">>,
<<"  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>Table Administration for ">>, Name, <<"</title></head>\r\n">>,
<<"  <body>\r\n">>,
<<"    <h1>Table ">>, Name, <<"</h1>\r\n">>,
<<"    <h2>Chains</h2>\r\n">>],

    Tail = [
<<"  </body>\r\n">>,
<<"</html>\r\n">>],

    {ok, Table} = brick_admin:get_table_info(list_to_atom(Name)),
    GHash = proplists:get_value(ghash, Table, {no_hash}),
    ChainList = get_all_chains(GHash),

    Body = [make_brick_header(<<"<th>Brick</th>">>, true), [ make_chain_rows(ChainName, BrickList) || {ChainName, BrickList} <- ChainList], make_brick_footer()],

    Msg = [Head, Body, Tail],

    httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, 200, 0}} | ModData#mod.data]}.


%% @spec (tuple()) -> iolist()
%% @doc Recursively create rows for passed brick list
make_chain_rows([]) -> [];
make_chain_rows([{Brick, Node} | BrickList]) ->
    [make_brick_row(Brick, Node, true), make_chain_rows(BrickList)].

%% @spec (string(), mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Get history for {Brick,Node} passed through query string.
get_chain(Query, ModData) ->
    KVList = make_proplist(Query, "&", "="),

    Name = proplists:get_value(name, KVList, []),

    Head = [
<<"<html>\r\n">>,
<<"  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>Chain Administration for ">>, Name, <<"</title></head>\r\n">>,
<<"  <body>\r\n">>,
<<"    <h1>Chain ">>, Name, <<"</h1>\r\n">>,
<<"    <h2>Bricks</h2>\r\n">>],

    Tail = [
<<"  </body>\r\n">>,
<<"</html>\r\n">>],

    {ok, Table} = brick_admin:get_table_info_by_chain(list_to_atom(Name)),
    GHash = proplists:get_value(ghash, Table, {no_hash}),
    ChainList = get_all_chains(GHash),

    History =
        [<<"    <h2>History of chain '">>, Name, <<"'</h2>\r\n">>,
         make_chain_history_table(list_to_atom(Name))],

    Body = [make_brick_header(), [ make_chain_rows(BrickList) || {ChainName, BrickList} <- ChainList, ChainName == list_to_atom(Name)], make_brick_footer(), History],

    Msg = [Head, Body, Tail],

    httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, 200, 0}} | ModData#mod.data]}.


%% @spec (string(), mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Get history for {Brick,Node} passed through query string.
get_brick(Query, ModData) ->
    KVList = make_proplist(Query, "&", "="),

    Name = proplists:get_value(name, KVList, []),
    Node = proplists:get_value(node, KVList, []),

    Head = [
<<"<html>\r\n">>,
<<"  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>Brick ">>, Name, <<"</title></head>\r\n">>,
<<"  <body>\r\n">>,
<<"    <h1>Brick ">>, Name, <<"</h1>\r\n">>],

    Tail = [
<<"  </body>\r\n">>,
<<"</html>\r\n">>],

    Properties =
        [<<"    <h2>Properties</h2>\r\n">>, make_brick_header(), make_brick_row(list_to_atom(Name), list_to_atom(Node), true), make_brick_footer()],

    History =
        [<<"    <h2>History on node '">>, Node, <<"'</h2>\r\n">>,
         make_brick_history_table(list_to_atom(Name), list_to_atom(Node))],

    Msg = [Head, Properties, History, Tail],

    httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, 200, 0}} | ModData#mod.data]}.


%% @spec (atom(), list(tuple())) -> iolist()
%% @doc Recursively create rows for passed brick name/event list
make_node_rows(_BrickName, []) -> [];
make_node_rows(BrickName, [Event | EventList]) ->
    Brick = atom_to_list(BrickName),
    NameBG = case length(Brick) of
                 0 -> <<" class='blank'">>;
                 _ -> <<"">>
             end,

    [make_history_row(Event, [<<"<td">>, NameBG, <<">">>, Brick, <<"</td>">>]), make_node_rows('', EventList)].

%% @spec (string(), mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Get history for {Brick,Node} passed through query string.
get_node(Query, ModData) ->
    KVList = make_proplist(Query, "&", "="),

    Name = proplists:get_value(name, KVList, []),

    Head = [
<<"<html>\r\n">>,
<<"  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>Node ">>, Name, <<"</title></head>\r\n">>,
<<"  <body>\r\n">>,
<<"    <h1>Node ">>, Name, <<"</h1>\r\n">>],

    Tail = [
<<"  </body>\r\n">>,
<<"</html>\r\n">>],


    Tables = brick_admin:get_tables(),
    Bricks =
        [<<"    <h2>Bricks</h2>\r\n">>,
         make_brick_header(),
         [begin
              {ok, Info} = brick_admin:get_table_info(TName),
              GHash = proplists:get_value(ghash, Info, {no_hash}),
              ChainList = get_all_chains(GHash),

              [begin
                   make_brick_row(Brick, Node, true)
               end || {_ChainName, BrickList} <- ChainList,
                      {Brick, Node} <- BrickList,
                      Node == list_to_atom(Name)]
          end || TName <- Tables],
         make_brick_footer()],

    History =
        [<<"    <h2>History</h2>\r\n">>,
         make_history_header([<<"<th>Brick</th>">>]),
         [begin
              {ok, Table} = brick_admin:get_table_info(TableName),
              GHash = proplists:get_value(ghash, Table, {no_hash}),
              ChainList = get_all_chains(GHash),

              [
               [ begin
                     {ok, EventList} = brick_sb:get_brick_history(Brick, Node),
                     make_node_rows(Brick, EventList)
                 end || {Brick, Node} <- BrickList, Node == list_to_atom(Name)]
               || {_ChainName, BrickList} <- ChainList ]
          end || TableName <- brick_admin:get_tables()],
         make_history_footer()],

    Msg = [Head, Bricks, History, Tail],

    httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, 200, 0}} | ModData#mod.data]}.


%% @spec (string(), mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Bootstrap the cluster based on the values passed in the query string.
get_bootstrap(Query, ModData) ->
    {KVList, _TabName, LocalP, VarPrefixP, PrefixSep, NumSep, DataProps, BPC,
     _NumNodesPerBlock, _BlockMultFactor} =
        parse_query_for_add_table(Query),
    {_, Boots} = read_bootstrap_nodes(),
    LocalBtFile = "Schema.local",

    try if
            LocalP ->
                Bricks = list_to_integer(proplists:get_value('Bricks', KVList, 0)),
                case brick_admin:bootstrap_local(DataProps, VarPrefixP,
                                                 PrefixSep, NumSep, Bricks,
                                                 BPC, Boots) of
                    ok  -> ok;
                    Err -> exit(Err)
                end;
            true ->
                Nodes = uri_decode(proplists:get_value('NodeList', KVList, [])),
                NodeList = make_proplist(Nodes, ",", "="),
                case brick_admin:bootstrap(LocalBtFile, DataProps,
                                           VarPrefixP, PrefixSep, NumSep,
                                           NodeList, BPC, Boots) of
                    ok  -> ok;
                    Err -> exit(Err)
                end
        end,
        {ok, BtFile} = file:read_file(LocalBtFile),
        MyNode = node(),
        [begin spawn(Nd, fun() -> file:write_file(LocalBtFile, BtFile),
                                  ?APPLOG_INFO(?APPLOG_APPM_103,
                                               "Schema hint file ~s copied from ~p\n",
                                               [LocalBtFile, MyNode]), timer:sleep(200)
                         end),
               ?APPLOG_INFO(?APPLOG_APPM_104,"Schema hint file ~s copied to ~p\n",
                            [LocalBtFile, Nd])
         end || Nd <- Boots],
        Head = [
                <<"<html>\r\n">>,
                <<"  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>Bootstrap was successful</title><meta http-equiv='REFRESH' content='5;url=/' /></head>\r\n">>,
                <<"  <body>\r\n">>,
                <<"    <h1>Bootstrap was successful!</h1>\r\n">>],

        Tail = [
                <<"  </body>\r\n">>,
                <<"</html>\r\n">>],

        Body = [<<"<p>Go to the <a href='/'>main page</a> to view the cluster.  You will be automatically redirected in 5 seconds.</p>">>],

        Msg = [Head, Body, Tail],

        httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
        httpd_response:send_chunk(ModData, Msg, true),
        %% httpd_response:send_final_chunk(ModData, true),

        {proceed, [{response, {already_sent, 200, 0}} | ModData#mod.data]}
    catch _:Why ->
            EMsg = ["<h1>Error</h1>\n", io_lib:format("<p> ~p\n", [Why])],
            httpd_response:send_header(ModData, 599, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(EMsg))}]),
            httpd_response:send_chunk(ModData, EMsg, true),
            {proceed, [{response, {already_sent, 599, 0}} | ModData#mod.data]}
    end.

%% @spec (string(), mod()) -> {proceed, binary()} | {break, binary()} | done
%% @doc Get history for {Brick,Node} passed through query string.
get_history(Query, ModData) ->
    KVList = make_proplist(Query, "&", "="),

    Brick = proplists:get_value(brick, KVList, []),
    Node = proplists:get_value(node, KVList, []),

    Name = ["brick ", Brick, " on node ", Node],

    Head = [
<<"<html>\r\n">>,
<<"  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>History for ">>, Name, <<"</title></head>\r\n">>,
<<"  <body>\r\n">>,
<<"    <h1>History for ">>, Name, <<"</h1>\r\n">>,
<<"    <table border=1>\r\n">>,
<<"      <tr class='head'><th>Time</th><th>Type</th><th>State</th><th>Properties</th></tr>\r\n">>],

    Tail = [
<<"    </table>\r\n">>,
<<"  </body>\r\n">>,
<<"</html>\r\n">>],

    {ok, EventList} = brick_sb:get_brick_history(list_to_atom(Brick), list_to_atom(Node)),

    Body = [
            begin
                {hevent, Time, Type, State, Props} = Event,
                Date = make_date(Time),

                [<<"<tr><td>">>, Date, <<"</td><td>">>, atom_to_list(Type), <<"</td><td>">>, atom_to_list(State), <<"</td><td>">>, escape(io_lib:format("~p", [Props])), <<"</td></tr>">>]
            end || Event <- EventList],

    Msg = [Head, Body, Tail],

    httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, 200, 0}} | ModData#mod.data]}.

get_all_chains(GHash) ->
    lists:usort((GHash#g_hash_r.current_h_desc)#hash_r.healthy_chainlist ++
                (GHash#g_hash_r.new_h_desc)#hash_r.healthy_chainlist).

integer_to_list_or_no_serial(no_serial) ->
    "no_serial";
integer_to_list_or_no_serial(CUS) ->
    integer_to_list(CUS).

read_bootstrap_nodes() ->
    AdminStr = gmt_config:get_config_value(admin_server_distributed_nodes, ""),
    if AdminStr == "";
       AdminStr == "'gdss1@machineA', 'gdss1@machine-B-with-hyphens'" ->
            {AdminStr, []};
       true ->
            %% Use binary for flattening perhaps improper list.
            ALB = re:replace(AdminStr, " ", "", [global, {return, list}]),
            %% | is a nonsense split char.
            {AdminStr, make_proplist(lists:flatten(ALB), ",", "|")}
    end.

add_table_html_form(TabName, SubmitURI) ->
    [
     <<"<table><tr class='head'><th>Option</th><th>Type</th><th>Description</th></tr>\r\n">>,
     <<"<tr class='data'><td>Local</td><td>boolean</td><td>Controls whether the cluster will use the current node for all bricks (true) or will use the listed remote nodes (false).  A local cluster is mostly useful for testing, since it provides neither performance nor reliability over a standalone node.</td></tr>">>,
     <<"<tr class='data'><td>BigData</td><td>boolean</td><td>Controls whether the cluster will store values on disk (true) or in memory (false)</td></tr>">>,
     <<"<tr class='data'><td>DiskLogging</td><td>boolean</td><td>Controls whether the cluster will log updates to disk to preserve data in case of a cluster-wide power failure (true = write to disk, false = no writing to disk)</td></tr>">>,
     <<"<tr class='data'><td>SyncWrites</td><td>boolean</td><td>Controls whether the cluster will log updates to disk synchonously (true) or asynchronously (false)</td></tr>">>,
     <<"<tr class='data'><td>VarPrefix</td><td>boolean</td><td>Controls whether the hashing algorithm operates on a variable length prefix (true), or the entire key (false).  Some applications (e.g. Gemini HCS) depend on having VarPrefixP be true to work correctly.</td></tr>\r\n">>,
     "<tr class='data'><td>VarPrefix Separator Character</td><td>integer</td><td>If VarPrefix is true, the ASCII value of the separator character (/ = 47).</td></tr>\r\n",
     "<tr class='data'><td>VarPrefix Num Separators</td><td>integer</td><td>If VarPrefix is true, the number of prefix separator characters to include in a key's hash prefix.</td></tr>\r\n",
     <<"<tr class='data'><td>Bricks</td><td>integer</td><td>Number of bricks to use for a local cluster.  When creating a non-local cluster, this option is ignored.</td></tr>\r\n">>,
     <<"<tr class='data'><td>BPC</td><td>integer</td><td>Number of bricks to use for each chain.  This option is used for local and non-local clusters.</td></tr>\r\n">>,
     <<"<tr class='data'><td>NodeList</td><td>list</td><td>A comma delineated list of erlang nodes which will be used as the other nodes in the cluster.  When creating a local cluster, this option is ignored.</td></tr>\r\n">>,
     <<"<tr class='data'><td>NumNodesPerBlock</td><td>integer</td><td>Number of nodes to use for striping chains.  Use 0 to ignore this attribute.</td></tr>\r\n">>,
     <<"<tr class='data'><td>BlockMultFactor</td><td>integer</td><td>Multiplication factor, for use with NumNodesPerBlock.  Use 0 to ignore this attribute.</td></tr>\r\n">>,
     <<"</table>\r\n">>,
     <<"<p />\r\n">>,
     <<"<form action='">>, SubmitURI, <<"' method='get'>\r\n">>,
     <<"<table class='wide'><tr class='head'>\r\n">>,
     if TabName == "" -> "<th>Table Name</th>\r\n";
        true          -> ""
     end,
     "<th>Local</th><th>Big Data</th><th>Disk Logging</th><th>Sync Writes</th><th>Var Prefix</th><th>VarPrefix Separator Char ASCII Value</th><th>VarPrefix Num Separators</th><th>Bricks</th><th>BPC</th><th>NodeList</th><th>NumNodes PerBlock</th><th>BlockMult Factor</th><th>Action</th></tr>\r\n",
     <<"<tr class='data'>">>,
     if TabName == "" ->
             make_text("TableName", 5, "");
        true ->
             ["<input type=hidden name=TableName value=", TabName, ">\r\n"]
     end,
     make_bool_select("LocalP"), make_bool_select("BigDataP"), make_bool_select("DiskLoggingP"), make_bool_select("SyncWritesP"), make_bool_select("VarPrefixP"), make_text("VarPrefixChar", 3, "47"), make_text("NumSeparators", 1, "3"), make_text("Bricks", 5, "1"), make_text("BPC", 3, "1"), make_text("NodeList"), make_text("NumNodesPerBlock", 3, "0"), make_text("BlockMultFactor", 3, "0"), make_submit("Bootstrap"), <<"</tr>\r\n">>,
     <<"</table>\r\n">>,
     <<"</form>\r\n">>
    ].

parse_query_for_add_table(Query) ->
    KVList = make_proplist(Query, "&", "="),
    TabName = proplists:get_value('TableName', KVList, "tab1"),
    LocalP = list_to_atom(proplists:get_value('LocalP', KVList, "true")),
    VarPrefixP = list_to_atom(proplists:get_value('VarPrefixP', KVList, "true")),
    PrefixSep = list_to_integer(proplists:get_value('VarPrefixChar', KVList, "47")),
    NumSep = list_to_integer(proplists:get_value('NumSeparators', KVList, "3")),
    BigDataP = list_to_atom(proplists:get_value('BigDataP', KVList, "true")),
    DiskLoggingP = list_to_atom(proplists:get_value('DiskLoggingP', KVList, "true")),
    SyncWritesP = list_to_atom(proplists:get_value('SyncWritesP', KVList, "true")),
    BPC = list_to_integer(proplists:get_value('BPC', KVList, "0")),
    MakeOpts = [{maketab_bigdata, BigDataP}, {maketab_do_logging, DiskLoggingP},
                {maketab_do_sync, SyncWritesP}],
    NumNodesPerBlock = list_to_integer(proplists:get_value('NumNodesPerBlock', KVList, "0")),
    BlockMultFactor = list_to_integer(proplists:get_value('BlockMultFactor', KVList, "0")),
    {KVList, TabName, LocalP, VarPrefixP, PrefixSep, NumSep, MakeOpts, BPC,
     NumNodesPerBlock, BlockMultFactor}.

get_add_table_html(ModData) ->
    Head = [
            <<"<html>\r\n">>,
            <<"  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>Add a Table</title></head>\r\n">>,
            <<"  <body>\r\n">>,
            <<"    <h1>Add a Table</h1>\r\n">>],
    Body = add_table_html_form("", "/add_table"),
    Tail = [
            <<"  </body>\r\n">>,
            <<"</html>\r\n">>],

    Msg = [Head, Body, Tail],

    httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, 200, iolist_size(Msg)}} | ModData#mod.data]}.

get_change_client_monitor_html(ModData) ->
    Title = "Add/Delete a Client Node Monitor",
    SubmitURI = "/change_client_monitor",
    Head = [
            "<html>\r\n",
            "  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>", Title, "</title></head>\r\n",
            "  <body>\r\n",
            "    <h1>", Title, "</h1>\r\n"],
    Body = [
            "<table>\n",
            "<tr class='head'><th>Current Contents of Client Monitor List</th></tr>\n",
            [["<tr class='data'><th>", atom_to_list(Nd), "</th></tr>"] ||
                Nd <- lists:sort(brick_admin:get_client_monitor_list())],
            "</table>\n",
            "<h2>Change Client Monitor</h2>\n",
            "<form action='", SubmitURI, "' method='get'>\n",
            "<table class='wide'>\n",
            "<tr class='head'><th>Add/Delete</th><th>Node Name</th></tr>\n",
            "<tr class='data'>",
            make_select("Op", ["add", "delete"]), make_text("Node", 16, ""),
            make_submit("Submit"), "</tr>",
            "</table></form>\n"
           ],
    Tail = [
            <<"  </body>\r\n">>,
            <<"</html>\r\n">>],

    Msg = [Head, Body, Tail],

    httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, 200, iolist_size(Msg)}} | ModData#mod.data]}.



get_add_table(Query, ModData) ->
    {KVList, TabName, LocalP, VarPrefixP, PrefixSep, NumSep, DataProps, BPC,
     NumNodesPerBlock, BlockMultFactor} =
        parse_query_for_add_table(Query),
    try
        NodeList = if
                       LocalP ->
                           Bricks = list_to_integer(proplists:get_value(
                                                      'Bricks', KVList, 0)),
                           lists:duplicate(Bricks, node());
                       true ->
                           Nodes = uri_decode(proplists:get_value(
                                                'NodeList', KVList, [])),
                           make_proplist(Nodes, ",", "=")
                   end,
        Table = list_to_atom(TabName),
        Chains =
            if NumNodesPerBlock < 1; BlockMultFactor < 1 ->
                    brick_admin:make_chain_description(Table, BPC, NodeList);
               true ->
                    brick_admin:make_chain_description(Table, BPC, NodeList,
                                                       NumNodesPerBlock,
                                                       BlockMultFactor)
            end,
    io:format("QQQ: Chains for ~p: ~p\n", [Table, Chains]),
        BrickOpts = brick_admin:make_common_table_opts(
                      DataProps, VarPrefixP, PrefixSep, NumSep, Chains),
        case brick_admin:add_table(Table, Chains, BrickOpts) of
                    ok  -> ok;
                    Err -> exit(Err)
        end,
        Head = [
                <<"<html>\r\n">>,
                <<"  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>Add Table was successful</title><meta http-equiv='REFRESH' content='5;url=/' /></head>\r\n">>,
                <<"  <body>\r\n">>,
                <<"    <h1>Add Table was successful!</h1>\r\n">>],

        Tail = [
                <<"  </body>\r\n">>,
                <<"</html>\r\n">>],

        Body = [<<"<p>Go to the <a href='/'>main page</a> to view the cluster.  You will be automatically redirected in 5 seconds.</p>">>],

        Msg = [Head, Body, Tail],

        httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
        httpd_response:send_chunk(ModData, Msg, true),
        %% httpd_response:send_final_chunk(ModData, true),

        {proceed, [{response, {already_sent, 200, 0}} | ModData#mod.data]}
    catch _:Why ->
            EMsg = ["<h1>Error</h1>\n", io_lib:format("<p> ~p\n", [Why])],
            httpd_response:send_header(ModData, 599, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(EMsg))}]),
        httpd_response:send_chunk(ModData, EMsg, true),
            {proceed, [{response, {already_sent, 599, 0}} | ModData#mod.data]}
    end.

get_change_client_monitor(Query, ModData) ->
    KVList = make_proplist(Query, "&", "="),
    try
        Op = proplists:get_value('Op', KVList, "op_missing"),
        Node =
            case string:strip(uri_decode(proplists:get_value('Node', KVList, "")), both) of
                "" ->
                    throw(missing_node);
                Nd ->
                    case re:run(Nd, "^[^@]+@[^@]+$") of
                        {match, _} ->
                            list_to_atom(Nd);
                        _ ->
                            throw(invalid_node_name)
                    end
            end,
        if Op == "add" ->
                ok = brick_admin:add_client_monitor(Node);
           Op == "delete" ->
                ok = brick_admin:delete_client_monitor(Node);
           true ->
                throw({bad_op, Op})
        end,
        Head = [
                "<html>\r\n",
                "  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>Operation was successful</title></head>\r\n"],
        Tail = ["  </body>\r\n",
                "</html>\r\n"],
        Body = ["<h1>Operation Was Successful</h1>\n",
                "<a href='/'>Return to top</a>"],

        Msg = [Head, Body, Tail],
        httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
        httpd_response:send_chunk(ModData, Msg, true),
        {proceed, [{response, {already_sent, 200, 0}} | ModData#mod.data]}
    catch _:Why ->
            EMsg = ["<h1>Error</h1>\n", io_lib:format("<p> ~p\n", [Why])],
            httpd_response:send_header(ModData, 599, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(EMsg))}]),
        httpd_response:send_chunk(ModData, EMsg, true),
            {proceed, [{response, {already_sent, 599, 0}} | ModData#mod.data]}
    end.

get_dump_history_html(ModData) ->
    Title = "Dump History",
    SubmitURI = "/dump_history",
    Head = [
            "<html>\r\n",
            "  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>", Title, "</title></head>\r\n",
            "  <body>\r\n",
            "    <h1>", Title, "</h1>\r\n"],
    Body = [
            "<form action='", SubmitURI, "' method='get'>\n",
            "<table class='wide'>\n",
            "<tr class='head'><th>Sort by Time</th><th>Regular Expression</th></tr>\n",
            "<tr class='data'>",
            make_select("Order", ["ascending", "descending"]),
            make_text("Regexp", 28, ".*"),
            make_submit("Submit"), make_reset("Reset!"), "</tr>",
            "</table></form>\n"
           ],
    Tail = [
            <<"  </body>\r\n">>,
            <<"</html>\r\n">>],

    Msg = [Head, Body, Tail],

    httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
    httpd_response:send_chunk(ModData, Msg, true),
    %% httpd_response:send_final_chunk(ModData, true),

    {proceed, [{response, {already_sent, 200, iolist_size(Msg)}} | ModData#mod.data]}.

get_dump_history(Query, ModData) ->
    KVList = make_proplist(Query, "&", "="),
    try
        Regexp = string:strip(uri_decode(proplists:get_value('Regexp', KVList,
                                                             ".*")), both),
        {Order, Cols} =
        case proplists:get_value('Order', KVList, "descending") of
            "ascending" -> {"Ascending", dump_history_in_columns()};
            _           -> {"Descending", lists:reverse(dump_history_in_columns())}
        end,
        Head = [
                "<html>\r\n",
                "  <head><link rel='stylesheet' type='text/css' href='/css/admin.css' /><title>History Dump: Key Matches '", Regexp, "'</title></head>\r\n"],
        Tail = ["  </body>\r\n",
                "</html>\r\n"],
        Body = ["<h1>History Dump: Key Matches '", Regexp, "'</h1>\n",
                "<p> Sort order: ", Order, "\n",
                "<table class='wide'>\n",
                "<tr class='head'><th>Time</th><th>Key</th><th>Change</th><th>Summary</th><th>Details</th></tr>\n",
                [["<tr class='data'><td>", C1, "</td><td>", C2, "</td><td>",
                  C3, "</td>", dump_color_summ(C4), "<td>", C5, "</td></tr>\n"]
                 || {C1, C2, C3, C4, C5} <- Cols, check_regexp_p(C2, Regexp)],
                "</table>\n"
               ],

        Msg = [Head, Body, Tail],
        httpd_response:send_header(ModData, 200, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(Msg))}]),
        httpd_response:send_chunk(ModData, Msg, true),
        {proceed, [{response, {already_sent, 200, 0}} | ModData#mod.data]}
    catch _:Why ->
            EMsg = ["<h1>Error</h1>\n", io_lib:format("<p> ~p\n", [Why])],
            httpd_response:send_header(ModData, 599, [{"content-type", "text/html"}, {"content-length", integer_to_list(iolist_size(EMsg))}]),
        httpd_response:send_chunk(ModData, EMsg, true),
            {proceed, [{response, {already_sent, 599, 0}} | ModData#mod.data]}
    end.

nice_fmt(Term) ->
    case io_lib:printable_list(Term) of
        true  -> io_lib:format("~s", [Term]);
        false -> io_lib:format("~p", [Term])
    end.

check_regexp_p(Str, Regexp) ->
    case re:run(Str, Regexp) of
        {match, _} -> true;
        _          -> false
    end.

dump_color_summ("pre_init" = _Str) ->
    make_data(pre_init, "warn");
dump_color_summ([C|_] = Str) when $a =< C, C =< $z ->
    make_data(list_to_atom(Str), "data");
dump_color_summ(S) ->
    ["<td>", S, "</td>"].


get_all_nodes_alarms() ->
    Nodes = lists:usort([node()] ++
                        [Nd || {_Br, Nd} <- get_all_tables_all_bricks()]),
    [{Nd, Ax, Ay} || Nd <- Nodes,
                     {Ax, Ay} <- rpc:call(Nd, gmt_util, get_alarms, [])].

get_all_tables_all_bricks() ->
    lists:append(
      lists:map(fun(Table) ->
                        {ok, Info} = brick_admin:get_table_info(Table),
                        GHash = proplists:get_value(ghash, Info, {no_hash}),
                        ChainList = get_all_chains(GHash),
                        [BrickList || {_ChainName, BrickList} <- ChainList]
                end, brick_admin:get_tables())).

%%
%% Experimental
%%

%% public API

dump_history() ->
    io:put_chars(line_pad_history()).

dump_history(Filename) ->
    file:write_file(Filename, line_pad_history()).

dump_history_in_columns() ->
    history_to_string_columns(get_history()).

admin_server_http_port() ->
    {ok, Port} =
        gmt_config_svr:get_config_value_i(brick_admin_http_tcp_port, 23080),
    Port.

%% helper funcs.

get_history() ->
    Boots = brick_admin:get_schema_bricks(),
    HKs = [T || {history, _} = T <- brick_admin:get_all_bootstrap_keys(Boots)],
    HEs = lists:flatten([begin
                             Kbin = term_to_binary(Key),
                             {ok, _TS, Lbin} = brick_squorum:get(Boots, Kbin),
                             {history, Thingie} = Key,
                             [{Thingie, HE} || HE <- binary_to_term(Lbin)]
                         end || Key <- HKs]),
    lists:sort(fun({_K1, HE1}, {_K2, HE2}) ->
                       HE1#hevent.time < HE2#hevent.time
               end, HEs).

line_pad_history() ->
    Es = get_history(),
    Cols = history_to_string_columns(Es),
    lists:map(
      fun({C1, C2, C3, C4, C5}) ->
              io_lib:format("~s | ~s | ~s | ~s | ~s\n",
                            [C1, gmt_util:right_pad(C2, 45, 32),
                             gmt_util:right_pad(C3, 12, 32),
                             C4, C5])
      end, Cols).

history_to_string_columns(Es) ->
    lists:append(lists:map(
                   fun({Key, HE}) ->
                           columnify_1_event(Key, HE)
                   end, Es)).

columnify_1_event(Key, HE) ->
    [columnify_1_line(Key, dump_filter_props(HE))] ++
    case catch proplists:get_value(chain_now, HE#hevent.props) of
        undefined ->
            [];
        {'EXIT', _} ->
            [];
        Members ->
            [columnify_1_line(Key, HE#hevent{what = chain_now, detail =
                                             Members, props = []})]
     end.

columnify_1_line(Key, HE) ->
    {dump_fmt_date(HE#hevent.time), string_ify(Key),
     string_ify(HE#hevent.what), string_ify(HE#hevent.detail),
     if HE#hevent.props == [] -> "";
        true                  -> string_ify(HE#hevent.props)
     end}.

string_ify(Term) ->
    lists:flatten(io_lib:print(Term, 1, 9999, -1)).

%% HE#hevent.detail,
%%      io_lib:print(HE#hevent.props, 1, 9999, -1)])).
%%     lists:flatten(
%%       io_lib:format("~s | ~s | ~s | ~w | ~s\n",
%%                  [dump_fmt_date(HE#hevent.time), dump_fmt_width(Key, 45),
%%                   dump_fmt_width(HE#hevent.what, 12),
%%                   HE#hevent.detail,
%%                   io_lib:print(HE#hevent.props, 1, 9999, -1)])).

dump_fmt_date({_, _, MSec} = Now) ->
    {{YYYY,MM,DD},{Hour,Min,Sec}} = calendar:now_to_local_time(Now),
    Date =
        io_lib:format("~.2.0w/~.3s/~.4w ~.2.0w:~.2.0w:~.2.0w.~.3.0w",
                      [DD, httpd_util:month(MM), YYYY, Hour, Min, Sec,
                       MSec div 1000]),
    lists:flatten(Date).

dump_fmt_width(Term, Width) ->
    gmt_util:right_pad(lists:flatten(io_lib:format("~p", [Term])), Width, 32).

dump_filter_props(HE) ->
    HE#hevent{props = lists:foldl(
                        fun(qqq_delme, Acc) ->
                                Acc;
                           ({chain_now, _}, Acc) ->
                                Acc;
                           ({T, Now}, Acc) when T == start_time;
                                                T == chain_my_repair_ok_time ->
                                TStr = dump_fmt_date(Now),
                                [{T, TStr}|Acc];
                           (X, Acc) ->
                                [X|Acc]
                        end, [], if is_list(HE#hevent.props) -> HE#hevent.props;
                                    true -> [HE#hevent.props]
                                 end)}.

watch_mailbox_size(Secs) ->
    proc_lib:spawn(fun() ->
                  brick_itimer:send_interval(1000, click),
                  watch_mailbox_size_loop(Secs)
          end).

watch_mailbox_size_loop(0) ->
    io:format("watch_mailbox_size_loop pid ~p exiting\n", [self()]),
    exit(normal);
watch_mailbox_size_loop(N) ->
    receive click -> ok end,
    Pids0 = [Pid || Pid <- processes(),
                    try element(2, process_info(Pid, message_queue_len))
                    catch _:_ ->
                            0
                    end > 500],
    Pids = lists:filter(
             fun(Pid) ->
                     case process_info(Pid, registered_name) of
                         {registered_name, brick_shepherd} -> false;
                         _                                 -> true
                     end
             end, Pids0),
    if Pids == [] ->
            ok;
       true ->
            try
                TimeT = gmt_time:time_t(),
                Path = "/tmp/mbox-size." ++ integer_to_list(TimeT),
                {ok, FH} = file:open(Path, [write]),
                [io:format(FH, "Pid ~p\n\nProcess info:\n~p\n\nBacktrace:\n~s\n\n",
                           [Pid, process_info(Pid), element(2, process_info(Pid, backtrace))])
                 || Pid <- Pids],
                file:close(FH),
                io:format("Watcher: ~s ~p\n", [Path, Pids])
            catch X:Y ->
                    io:format("Watch ERROR: ~p:~p\n", [X, Y])
            end
    end,
    watch_mailbox_size_loop(N-1).


