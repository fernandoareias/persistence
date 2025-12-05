-module(postgresql_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    %% TODO: Load these from application config
    DbConfig = [
        {host, "localhost"},
        {port, 5432},
        {database, "senff_db"},
        {username, "postgres"},
        {password, "postgres"},
        {max_reconnect_attempts, 5}
    ],

    %% Poolboy configuration
    %% TODO: Load these from application config :)
    PoolConfig = [
        {name, {local, postgresql_pool}},
        {worker_module, postgresql_connection_fsm},
        {size, 20},           % 10 persistent connections
        {max_overflow, 10},    % Up to 5 temporary extra connections
        {strategy, fifo}      % First-in-first-out
    ],

    SupervisorSpecification = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 60
    },

    ChildSpecifications = [
        poolboy:child_spec(postgresql_pool, PoolConfig, DbConfig),

        #{
            id => postgresql_worker_manager_sup,
            start => {postgresql_worker_manager_sup, start_link, []},
            restart => permanent,
            shutdown => 2000,
            type => supervisor,
            modules => [postgresql_worker_manager_sup]
        }
    ],

    {ok, {SupervisorSpecification, ChildSpecifications}}.