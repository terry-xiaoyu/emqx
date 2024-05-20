%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Supervisor that contains all the processes that belong to a
%% given builtin DS database.
-module(emqx_ds_builtin_db_sup).

-behaviour(supervisor).

%% API:
-export([
    start_db/2,
    start_shard/1,
    start_egress/1,
    stop_shard/1,
    terminate_storage/1,
    restart_storage/1,
    ensure_shard/1,
    ensure_egress/1
]).
-export([which_dbs/0, which_shards/1]).

%% behaviour callbacks:
-export([init/1]).

%% internal exports:
-export([start_link_sup/2]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(via(REC), {via, gproc, {n, l, REC}}).

-define(db_sup, ?MODULE).
-define(shards_sup, emqx_ds_builtin_db_shards_sup).
-define(egress_sup, emqx_ds_builtin_db_egress_sup).
-define(shard_sup, emqx_ds_builtin_db_shard_sup).

-record(?db_sup, {db}).
-record(?shards_sup, {db}).
-record(?egress_sup, {db}).
-record(?shard_sup, {db, shard}).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_db(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) -> {ok, pid()}.
start_db(DB, Opts) ->
    start_link_sup(#?db_sup{db = DB}, Opts).

-spec start_shard(emqx_ds_storage_layer:shard_id()) ->
    supervisor:startchild_ret().
start_shard({DB, Shard}) ->
    supervisor:start_child(?via(#?shards_sup{db = DB}), shard_spec(DB, Shard)).

-spec start_egress(emqx_ds_storage_layer:shard_id()) ->
    supervisor:startchild_ret().
start_egress({DB, Shard}) ->
    supervisor:start_child(?via(#?egress_sup{db = DB}), egress_spec(DB, Shard)).

-spec stop_shard(emqx_ds_storage_layer:shard_id()) -> ok.
stop_shard({DB, Shard}) ->
    Sup = ?via(#?shards_sup{db = DB}),
    ok = supervisor:terminate_child(Sup, Shard),
    ok = supervisor:delete_child(Sup, Shard).

-spec terminate_storage(emqx_ds_storage_layer:shard_id()) -> ok | {error, _Reason}.
terminate_storage({DB, Shard}) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    supervisor:terminate_child(Sup, {Shard, storage}).

-spec restart_storage(emqx_ds_storage_layer:shard_id()) -> {ok, _Child} | {error, _Reason}.
restart_storage({DB, Shard}) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    supervisor:restart_child(Sup, {Shard, storage}).

-spec ensure_shard(emqx_ds_storage_layer:shard_id()) ->
    ok | {error, _Reason}.
ensure_shard(Shard) ->
    ensure_started(start_shard(Shard)).

-spec ensure_egress(emqx_ds_storage_layer:shard_id()) ->
    ok | {error, _Reason}.
ensure_egress(Shard) ->
    ensure_started(start_egress(Shard)).

-spec which_shards(emqx_ds:db()) ->
    [_Child].
which_shards(DB) ->
    supervisor:which_children(?via(#?shards_sup{db = DB})).

%% @doc Return the list of builtin DS databases that are currently
%% active on the node.
-spec which_dbs() -> [emqx_ds:db()].
which_dbs() ->
    Key = {n, l, #?db_sup{_ = '_', db = '$1'}},
    gproc:select({local, names}, [{{Key, '_', '_'}, [], ['$1']}]).

%%================================================================================
%% behaviour callbacks
%%================================================================================

init({#?db_sup{db = DB}, DefaultOpts}) ->
    %% Spec for the top-level supervisor for the database:
    logger:notice("Starting DS DB ~p", [DB]),
    emqx_ds_builtin_metrics:init_for_db(DB),
    Opts = emqx_ds_replication_layer_meta:open_db(DB, DefaultOpts),
    ok = start_ra_system(DB, Opts),
    Children = [
        sup_spec(#?shards_sup{db = DB}, []),
        sup_spec(#?egress_sup{db = DB}, []),
        shard_allocator_spec(DB)
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, Children}};
init({#?shards_sup{db = _DB}, _}) ->
    %% Spec for the supervisor that manages the supervisors for
    %% each local shard of the DB:
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, []}};
init({#?egress_sup{db = _DB}, _}) ->
    %% Spec for the supervisor that manages the egress proxy processes
    %% managing traffic towards each of the shards of the DB:
    SupFlags = #{
        strategy => one_for_one,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, []}};
init({#?shard_sup{db = DB, shard = Shard}, _}) ->
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 100
    },
    Opts = emqx_ds_replication_layer_meta:db_config(DB),
    Children = [
        shard_storage_spec(DB, Shard, Opts),
        shard_replication_spec(DB, Shard, Opts)
    ],
    {ok, {SupFlags, Children}}.

start_ra_system(DB, Opts) ->
    ReplicationOpts = maps:get(replication_options, Opts, #{}),
    DataDir = filename:join([emqx_ds:base_dir(), DB, dsrepl]),
    Config = lists:foldr(fun maps:merge/2, #{}, [
        ra_system:default_config(),
        #{
            name => DB,
            data_dir => DataDir,
            wal_data_dir => DataDir,
            names => ra_system:derive_names(DB)
        },
        maps:with(
            [
                wal_max_size_bytes,
                wal_max_batch_size,
                wal_write_strategy,
                wal_sync_method,
                wal_compute_checksums
            ],
            ReplicationOpts
        )
    ]),
    case ra_system:start(Config) of
        {ok, _System} ->
            ok;
        {error, {already_started, _System}} ->
            ok
    end.

%%================================================================================
%% Internal exports
%%================================================================================

start_link_sup(Id, Options) ->
    supervisor:start_link(?via(Id), ?MODULE, {Id, Options}).

%%================================================================================
%% Internal functions
%%================================================================================

sup_spec(Id, Options) ->
    #{
        id => element(1, Id),
        start => {?MODULE, start_link_sup, [Id, Options]},
        type => supervisor,
        shutdown => infinity
    }.

shard_spec(DB, Shard) ->
    #{
        id => Shard,
        start => {?MODULE, start_link_sup, [#?shard_sup{db = DB, shard = Shard}, []]},
        shutdown => infinity,
        restart => permanent,
        type => supervisor
    }.

shard_storage_spec(DB, Shard, Opts) ->
    #{
        id => {Shard, storage},
        start => {emqx_ds_storage_layer, start_link, [{DB, Shard}, Opts]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

shard_replication_spec(DB, Shard, Opts) ->
    #{
        id => {Shard, replication},
        start => {emqx_ds_replication_layer_shard, start_link, [DB, Shard, Opts]},
        shutdown => 10_000,
        restart => permanent,
        type => worker
    }.

shard_allocator_spec(DB) ->
    #{
        id => shard_allocator,
        start => {emqx_ds_replication_shard_allocator, start_link, [DB]},
        restart => permanent,
        type => worker
    }.

egress_spec(DB, Shard) ->
    #{
        id => Shard,
        start => {emqx_ds_replication_layer_egress, start_link, [DB, Shard]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

ensure_started(Res) ->
    case Res of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
