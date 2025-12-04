-module(postgresql_worker_manager_sup).

-behaviour(supervisor).

-export([start_link/0, start_worker/3]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_worker(Connection, Query, Caller) ->
    supervisor:start_child(?MODULE, [Connection, Query, Caller]).

init(_Args) ->
    SupervisorSpecification = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 60
    },

    ChildSpecifications = [
        #{
            id => postgresql_worker,
            start => {postgresql_worker, start_link, []},
            restart => temporary,
            shutdown => 2000,
            type => worker,
            modules => [postgresql_worker]
        }
    ],

    {ok, {SupervisorSpecification, ChildSpecifications}}.
