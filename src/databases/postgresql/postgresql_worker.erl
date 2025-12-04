-module(postgresql_worker).
-behaviour(gen_server).

-include_lib("epgsql/include/epgsql.hrl").

-export([start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    connection :: epgsql:connection(),
    query :: string(),
    caller :: pid() | undefined
}).

%%====================================================================
%% API Functions
%%====================================================================

start_link(Connection, Query, Caller) ->
    gen_server:start_link(?MODULE, [Connection, Query, Caller], []).

%%====================================================================
%% gen_server Callbacks
%%====================================================================

init([Connection, Query, Caller]) ->
    logger:info("PostgreSQL Worker started - Query: ~p", [Query]),
    self() ! execute_query,
    {ok, #state{connection = Connection, query = Query, caller = Caller}}.

handle_info(execute_query, State = #state{connection = Conn, query = Query, caller = Caller}) ->
    logger:info("Executing query: ~p", [Query]),
    
    Result = execute_query_internal(Conn, Query),
    
    logger:info("Query result: ~p", [Result]),

    %% Tenta enviar via gen_server:cast, se falhar envia mensagem direta
    try
        gen_server:cast(Caller, {query_result, self(), Result}),
        logger:info("Result sent via cast to caller ~p", [Caller])
    catch
        _:_ ->
            %% Fallback: envia mensagem direta para processos simples
            Caller ! {query_result, self(), Result},
            logger:info("Result sent via direct message to caller ~p", [Caller])
    end,

    {stop, normal, State};

handle_info(_Info, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    logger:info("PostgreSQL Worker terminated"),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

execute_query_internal(Connection, Query) ->
    %% Executa query usando epgsql
    case epgsql:squery(Connection, Query) of
        {ok, Columns, Rows} ->
            {ok, format_result(Columns, Rows)};
        {ok, Count} when is_integer(Count) ->
            %% Para INSERT/UPDATE/DELETE que retornam apenas count
            {ok, Count};
        {error, Error} ->
            {error, Error}
    end.

format_result(Columns, Rows) ->
    ColumnNames = [Name || #column{name = Name} <- Columns],
    [maps:from_list(lists:zip(ColumnNames, tuple_to_list(Row))) || Row <- Rows].
