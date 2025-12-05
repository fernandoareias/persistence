-module(persistence).

-export([
    query/1,          % Simple query
    query/2,          % Prepared statement
    equery/2,         % Alias for prepared statement
    transaction/1,    % Transaction
    batch/1           % Batch queries
]).

-include_lib("epgsql/include/epgsql.hrl").

%%====================================================================
%% API
%%====================================================================

-spec query(string()) -> {ok, list()} | {ok, integer()} | {error, term()}.
query(Query) ->
    poolboy:transaction(
        postgresql_pool,
        fun(FSM) -> execute_simple(FSM, Query) end,
        5000
    ).

-spec query(string(), list()) -> {ok, list()} | {ok, integer()} | {error, term()}.
query(Query, Params) ->
    equery(Query, Params).

-spec equery(string(), list()) -> {ok, list()} | {ok, integer()} | {error, term()}.
equery(Query, Params) ->
    poolboy:transaction(
        postgresql_pool,
        fun(FSM) -> execute_prepared(FSM, Query, Params) end,
        5000
    ).

-spec transaction(fun((epgsql:connection()) -> term())) ->
    {ok, term()} | {error, term()}.
transaction(Fun) ->
    poolboy:transaction(
        postgresql_pool,
        fun(FSM) ->
            case postgresql_connection_fsm:get_connection(FSM) of
                {ok, Conn} ->
                    epgsql:with_transaction(Conn, Fun);
                {error, Reason} ->
                    {error, {connection_unavailable, Reason}}
            end
        end,
        10000  % Longer timeout for transactions
    ).

-spec batch(list({string(), list()})) ->
    {ok, list()} | {error, term()}.
batch(Queries) ->
    transaction(fun(Conn) ->
        Results = [epgsql:equery(Conn, Q, P) || {Q, P} <- Queries],
        {ok, Results}
    end).

%%====================================================================
%% Internal Functions
%%====================================================================

execute_simple(FSM, Query) ->
    case postgresql_connection_fsm:get_connection(FSM) of
        {ok, Conn} ->
            case epgsql:squery(Conn, Query) of
                {ok, Columns, Rows} ->
                    {ok, format_result(Columns, Rows)};
                {ok, Count} when is_integer(Count) ->
                    {ok, #{affected_rows => Count}};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Reason} ->
            {error, {connection_unavailable, Reason}}
    end.

execute_prepared(FSM, Query, Params) ->
    case postgresql_connection_fsm:get_connection(FSM) of
        {ok, Conn} ->
            case epgsql:equery(Conn, Query, Params) of
                {ok, Columns, Rows} ->
                    {ok, format_result(Columns, Rows)};
                {ok, Count} when is_integer(Count) ->
                    {ok, #{affected_rows => Count}};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Reason} ->
            {error, {connection_unavailable, Reason}}
    end.

format_result(Columns, Rows) ->
    ColumnNames = [Name || #column{name = Name} <- Columns],
    [maps:from_list(lists:zip(ColumnNames, tuple_to_list(Row))) || Row <- Rows].
