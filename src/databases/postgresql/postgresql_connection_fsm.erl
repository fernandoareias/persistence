-module(postgresql_connection_fsm).

-behaviour(gen_statem).
-behaviour(poolboy_worker).

-include_lib("epgsql/include/epgsql.hrl").

-record(connection_state, {
    connection = undefined :: undefined | epgsql:connection(),
    host = "localhost" :: string(),
    port = 5432 :: integer(),
    database = "" :: string(),
    username = "" :: string(),
    password = "" :: string(),
    reconnect_attempts = 0 :: integer(),
    max_reconnect_attempts = 5 :: integer()
}).

-export([start_link/1, start/1, stop/0, stop/1, get_connection/0, get_connection/1]).

-export([init/1, terminate/3, code_change/4, callback_mode/0]).

-export([disconnected/3, connecting/3, connected/3]).

-define(SERVER_NAME, postgresql_connection_fsm).
-define(RECONNECT_DELAY, 5000). 

%%====================================================================
%% API
%%====================================================================

start_link(Config) ->
    logger:info("Starting Connection FSM..."),
    gen_statem:start_link(?MODULE, Config, []).

start(Config) ->
    gen_statem:start(?MODULE, Config, []).

stop() ->
    stop(?SERVER_NAME).

stop(FSM) ->
    logger:info("Stoping Connection..."),
    gen_statem:stop(FSM).

get_connection() ->
    get_connection(?SERVER_NAME).

get_connection(FSM) ->
    gen_statem:call(FSM, get_connection, 5000).

%%====================================================================
%% gen_statem callbacks
%%====================================================================

callback_mode() ->
    state_functions.

init(Config) ->
    logger:info("Initing Connection FSM..."),

    Host = proplists:get_value(host, Config, "localhost"),
    Port = proplists:get_value(port, Config, 5432),
    Database = proplists:get_value(database, Config, ""),
    Username = proplists:get_value(username, Config, ""),
    Password = proplists:get_value(password, Config, ""),
    MaxReconnectAttempts = proplists:get_value(max_reconnect_attempts, Config, 5),

    Data = #connection_state{
        host = Host,
        port = Port,
        database = Database,
        username = Username,
        password = Password,
        max_reconnect_attempts = MaxReconnectAttempts
    },

    {ok, disconnected, Data, [{next_event, internal, connect}]}.

terminate(_Reason, _State, #connection_state{connection = Conn}) ->
    case Conn of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            try
                logger:info("Closing connection..."),
                epgsql:close(Pid)
            catch
                _:_ -> ok
            end
    end.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%====================================================================
%% State callbacks
%%====================================================================

disconnected(internal, connect, Data) ->
    {next_state, connecting, Data, [{next_event, internal, attempt_connect}]};

disconnected({call, From}, get_connection, _Data) ->
    {keep_state_and_data, [{reply, From, {error, disconnected}}]};

disconnected(EventType, Event, Data) ->
    handle_common_event(EventType, Event, Data).

connecting(internal, attempt_connect, #connection_state{
    host = Host,
    port = Port,
    database = Database,
    username = Username,
    password = Password,
    reconnect_attempts = Attempts,
    max_reconnect_attempts = MaxAttempts
} = Data) ->
    ConnectOpts = #{
        host => Host,
        port => Port,
        database => Database,
        username => Username,
        password => Password,
        timeout => 5000
    },


    case epgsql:connect(ConnectOpts) of
        {ok, Conn} ->
            logger:info("[PostgreSQL] Connected successfully to ~s:~p/~s~n", [Host, Port, Database]),
            NewData = Data#connection_state{
                connection = Conn,
                reconnect_attempts = 0
            },
            {next_state, connected, NewData};

        {error, Reason} when Attempts < MaxAttempts ->
            logger:info("[PostgreSQL] Connection failed (attempt ~p/~p): ~p. Retrying in ~p ms~n",
                     [Attempts + 1, MaxAttempts, Reason, ?RECONNECT_DELAY]),
            NewData = Data#connection_state{
                reconnect_attempts = Attempts + 1
            },
            {keep_state, NewData, [{state_timeout, ?RECONNECT_DELAY, reconnect}]};

        {error, Reason} ->
            logger:info("[PostgreSQL] Connection failed after ~p attempts: ~p~n", [MaxAttempts, Reason]),
            {keep_state, Data#connection_state{reconnect_attempts = 0}}
    end;

connecting(state_timeout, reconnect, Data) ->
    {keep_state, Data, [{next_event, internal, attempt_connect}]};

connecting({call, From}, get_connection, _Data) ->
    {keep_state_and_data, [{reply, From, {error, connecting}}]};

connecting(EventType, Event, Data) ->
    handle_common_event(EventType, Event, Data).

connected({call, From}, get_connection, #connection_state{connection = Conn} = Data) ->
    case is_process_alive(Conn) of
        true ->
            {keep_state_and_data, [{reply, From, {ok, Conn}}]};
        false ->
            logger:info("[PostgreSQL] Connection process died, reconnecting~n"),
            NewData = Data#connection_state{
                connection = undefined,
                reconnect_attempts = 0
            },
            {next_state, disconnected, NewData, [
                {reply, From, {error, connection_lost}},
                {next_event, internal, connect}
            ]}
    end;

connected(info, {'EXIT', Pid, Reason}, #connection_state{connection = Pid} = Data) ->
    logger:info("[PostgreSQL] Connection lost: ~p. Reconnecting~n", [Reason]),
    NewData = Data#connection_state{
        connection = undefined,
        reconnect_attempts = 0
    },
    {next_state, disconnected, NewData, [{next_event, internal, connect}]};

connected(EventType, Event, Data) ->
    handle_common_event(EventType, Event, Data).

%%====================================================================
%% Internal functions
%%====================================================================

handle_common_event(info, Msg, _Data) ->
    logger:info("[PostgreSQL] Unhandled info: ~p~n", [Msg]),
    keep_state_and_data;

handle_common_event(_EventType, _Event, _Data) ->
    keep_state_and_data.
