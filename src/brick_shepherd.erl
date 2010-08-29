%%%-------------------------------------------------------------------
%%% Copyright: (c) 2007-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : brick_shepherd.erl
%%% Purpose : Shepherd for brick servers
%%%-------------------------------------------------------------------

%% @doc This shepherd takes care of stopping and starting brick
%% servers, including being a repository for default config state.
%%
%% Default config state is limited at the moment:
%% <ul>
%% <li> <b>default_data_dir</b>: Obtained via
%%   <tt>gmt_config:get_config_value()</tt>, see <tt>init()</tt> for
%%   details.</li>
%% </ul>
%%
%% In addition, there are a very few options which can be passed via
%% the proplist in <tt>start_brick()</tt> that we care about:
%% <ul>
%% <li> <b>restart_strategy</b>: Used to configure the worker type that
%%   is given to the <tt>brick_brick_sup</tt> supervisor. </li>
%% </ul>

-module(brick_shepherd).
-include("applog.hrl").


-behaviour(gen_server).

-include("brick_public.hrl").

%% API
-export([start/0, start_link/0,
         start_brick/2, start_brick/3, stop_brick/1, stop_brick/2,
         list_bricks/0, list_bricks/1, list_bricks_all_nodes/0,
         get_start_time/1,
         add_do_not_restart_brick/2, delete_do_not_restart_brick/2,
         list_do_not_restart_bricks/1,
         make_shepherd_pid/0, is_shepherd_pid_alive/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          do_not_restart_list = [],
          default_options,
          start_time
         }).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------

%% @spec () -> {ok, pid()}
%% @doc Start the brick shepherd.

-spec start() -> {ok, pid()}.
start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

%% @spec () -> {ok, pid()}
%% @doc Start the brick shepherd.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @spec (atom(), proplist()) -> {ok, pid()} | {error, term()}
%% @doc Start an individual brick.

-spec start_brick(brick_server:brick_name(), brick_server:prop_list())
                 -> {ok, pid()} | {error, term()}.
start_brick(Name, Options) ->
    gen_server:call(?MODULE, {start_brick, Name, Options}).

%% @spec (atom(), atom(), proplist()) -> {ok, pid()} | {error, term()}
%% @doc Start an individual brick on Node.

-spec start_brick(brick_server:brick_name(), brick_server:node_name(), brick_server:prop_list())
                 -> {ok, pid()} | {error, term()}.
start_brick(Name, Node, Options) ->
    gen_server:call({?MODULE, Node}, {start_brick, Name, Options}).

%% @spec (atom()) -> ok | {error, term()}
%% @doc Stop an individual brick.

-spec stop_brick(brick_server:brick_name()) -> ok | {error, term()}.
stop_brick(Name) ->
    gen_server:call(?MODULE, {stop_brick, Name}).

%% @spec (atom(), atom()) -> ok | {error, term()}
%% @doc Stop an individual brick on Node.

-spec stop_brick(brick_server:brick_name(), brick_server:node_name())
                -> ok | {error, term()}.
stop_brick(Name, Node) ->
    gen_server:call({?MODULE, Node}, {stop_brick, Name}).

%% @spec () -> list(atom())
%% @doc Return list of names of all bricks running on the local node.

-spec list_bricks() -> list(brick_server:brick_name()).
list_bricks() ->
    list_bricks(node()).

%% @spec (atom()) -> list(atom())
%% @doc Return list of names of all bricks running on the specified node.

-spec list_bricks(brick_server:node_name()) -> list(brick_server:brick_name()).
list_bricks(Node) ->
    gen_server:call({?MODULE, Node}, {list_bricks}).

%% @spec () -> list({atom(), atom()})
%% @doc Return list of {Name, Node} of all bricks running on all
%%      Erlang nodes.

-spec list_bricks_all_nodes() -> list(brick_server:brick()).
list_bricks_all_nodes() ->
    [{Br, Nd} || Nd <- [node()|nodes()],
                 Br <- try case rpc:call(Nd, ?MODULE, list_bricks, []) of
                               L when is_list(L) -> L;
                               _                 -> []
                           end
                       catch _:_ -> []
                       end].

%% @spec (atom()) -> {integer(), integer(), integer()}
%% @doc Return the erlang:now() when the brick_shepherd process was started.

-spec get_start_time(brick_server:node_name()) -> brick_bp:nowtime().
get_start_time(Node) ->
    gen_server:call({?MODULE, Node}, {get_start_time}).

%% @spec (atom(), atom()) -> already_added | ok
%% @doc Add a brick to the local brick shepherd's "do not restart" list
%% of bricks.
%%
%% NOTE: The "do not restart" list of bricks is not stored to stable
%% storage.  It will be reset to the empty list whenever the shepherd
%% restarts.

-spec add_do_not_restart_brick(brick_server:brick_name(), brick_server:node_name())
                              -> already_added | ok.
add_do_not_restart_brick(Brick, Node) when is_atom(Brick), is_atom(Node) ->
    case lists:member(Brick, list_do_not_restart_bricks(Node)) of
        true ->
            already_added;
        false ->
            gen_server:call({?MODULE, Node},
                            {add_do_not_restart_brick, Brick})
    end.

%% @spec (atom(), atom()) -> not_added | ok
%% @doc Add a brick to the local brick shepherd's "do not restart" list
%% of bricks.
%%
%% NOTE: The "do not restart" list of bricks is not stored to stable
%% storage.  It will be reset to the empty list whenever the shepherd
%% restarts.

-spec delete_do_not_restart_brick(brick_server:brick_name(), brick_server:node_name())
                              -> not_added | ok.
delete_do_not_restart_brick(Brick, Node) when is_atom(Brick), is_atom(Node) ->
    case lists:member(Brick, list_do_not_restart_bricks(Node)) of
        false ->
            not_added;
        true ->
            gen_server:call({?MODULE, Node},
                            {delete_do_not_restart_brick, Brick})
    end.

-spec list_do_not_restart_bricks(brick_server:node_name())
                               -> list(brick_server:brick_name()).
list_do_not_restart_bricks(Node) when is_atom(Node) ->
    gen_server:call({?MODULE, Node}, {list_do_not_restart_bricks}).

%% @doc Create a "shepherd pid", which can be useful for storing
%% in a table and then later checking if that pid is still alive.

-spec make_shepherd_pid() -> {brick_bp:nowtime(), pid()}.
make_shepherd_pid() ->
    {get_start_time(node()), self()}.

%% @doc Check to see if a "shepherd pid" process is still alive
%% (to guard against pid wrap-around after a few VM restarts).

-spec is_shepherd_pid_alive({brick_bp:nowtime(), pid()}) -> boolean().
is_shepherd_pid_alive({ShepTime, Pid}) ->
    StartTime = get_start_time(node(Pid)),
    if StartTime /= ShepTime ->
            false;
       true ->
            rpc:call(node(Pid), erlang, is_process_alive, [Pid])
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    DefDataDir =
        gmt_config:get_config_value(brick_default_data_dir, "."),

    Os = [{default_data_dir, DefDataDir}],
    {ok, #state{do_not_restart_list = [],
                default_options = Os, start_time = now()}}.

%%--------------------------------------------------------------------
%% Function:
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({start_brick, Name, Options}, _From, State) ->
    case lists:member(Name, State#state.do_not_restart_list) of
        true ->
            {reply, do_not_restart, State};
        false ->
            do_start_brick(Name, Options, State)
    end;
handle_call({stop_brick, Name}, _From, State) ->
    Reply = case (catch supervisor:terminate_child(?SUB_BRICK_SUP, Name)) of
                ok ->
                    catch supervisor:delete_child(?SUB_BRICK_SUP, Name);
                Err ->
                    Err
            end,
    {reply, Reply, State};
handle_call({list_bricks}, _From, State) ->
    Cs = supervisor:which_children(?SUB_BRICK_SUP),
    Reply = [Id || {Id, _Child, _Type, _Modules} <- Cs],
    {reply, Reply, State};
handle_call({get_start_time}, _From, State) ->
    {reply, State#state.start_time, State};
handle_call({add_do_not_restart_brick, Brick}, _From, State) ->
    {reply, ok, State#state{do_not_restart_list =
                            [Brick|State#state.do_not_restart_list]}};
handle_call({delete_do_not_restart_brick, Brick}, _From, State) ->
    {reply, ok, State#state{do_not_restart_list =
                            State#state.do_not_restart_list -- [Brick]}};
handle_call({list_do_not_restart_bricks}, _From, State) ->
    {reply, State#state.do_not_restart_list, State};
handle_call(Request, From, State) ->
    io:format("DEBUG: ~s:handle_call: unknown Request ~w from ~w\n",
              [?MODULE, Request, From]),
    Reply = unknown_call,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
    io:format("DEBUG: ~s:handle_cast: unknown Msg ~w\n", [?MODULE, Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    ?APPLOG_INFO(?APPLOG_APPM_065,"DEBUG: ~s:handle_info: Info ~w\n", [?MODULE, Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

do_start_brick(Name, Options, S) ->
    Os = Options ++ S#state.default_options,
    RestartStrategy =
        case proplists:get_value(restart_strategy, Os) of
            undefined ->
                %% By default, we don't want a brick in a chain to restart
                %% automatically.  We want the brick admin to restart it.
                temporary;
            X when X == permanent; X == transient; X == temporary ->
                X
        end,
    ChildSpec =
        {Name, {brick_server, start_link, [Name, Os]},
         RestartStrategy, 2000, worker, [?MODULE]},

    Reply = case (catch supervisor:start_child(?SUB_BRICK_SUP, ChildSpec)) of
                %% If the child was transient or temporary, and if the
                %% child died, the supervisor still remembers that child
                %% and won't add it again.
                %% We will delete and restart, in case the Options list
                %% is different this time.
                {error, already_present} = Err ->
                    case (catch supervisor:delete_child(?SUB_BRICK_SUP, Name)) of
                        running ->
                            Err;
                        ok ->
                            catch supervisor:start_child(?SUB_BRICK_SUP, ChildSpec);
                        Err2 ->
                            Err2
                    end;
                Res ->
                    Res
            end,
    {reply, Reply, S}.

