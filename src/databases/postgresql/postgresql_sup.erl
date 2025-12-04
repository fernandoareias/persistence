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

    SupervisorSpecification = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },

    ChildSpecifications = [
        #{
            id => postgresql_connection_fsm,
            start => {postgresql_connection_fsm, start_link, [DbConfig]},
            restart => permanent,
            shutdown => 2000,
            type => worker,
            modules => [postgresql_connection_fsm]
        },
        #{
            id => postgresql_worker_manager_sup,
            start => {postgresql_worker_manager_sup, start_link, []},
            restart => permanent,
            shutdown => 2000,
            type => worker,
            modules => [postgresql_worker_manager_sup]
        }
    ],

    {ok, {SupervisorSpecification, ChildSpecifications}}.