%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Metadata storage for the builtin sharded database.
%%
%% Currently metadata is stored in mria; that's not ideal, but
%% eventually we'll replace it, so it's important not to leak
%% implementation details from this module.
-module(emqx_ds_replication_layer_meta).

-compile(inline).

-behaviour(gen_server).

%% API:
-export([
    shards/1,
    my_shards/1,
    shard_info/2,
    allocate_shards/1,
    replica_set/2,
    sites/0,
    node/1,
    this_site/0,
    print_status/0
]).

%% DB API:
-export([
    open_db/2,
    db_config/1,
    update_db_config/2,
    drop_db/1
]).

%% Site / shard allocation:
-export([
    join_db_site/2,
    leave_db_site/2,
    assign_db_sites/2,
    replica_set_transitions/2,
    update_replica_set/3,
    db_sites/1,
    target_set/2
]).

%% Subscriptions to changes:
-export([
    subscribe/2,
    unsubscribe/1
]).

%% gen_server
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([
    open_db_trans/2,
    allocate_shards_trans/1,
    assign_db_sites_trans/2,
    modify_db_sites_trans/2,
    update_replica_set_trans/3,
    update_db_config_trans/2,
    drop_db_trans/1,
    claim_site/2,
    n_shards/1
]).

-export_type([
    site/0,
    transition/0,
    subscription_event/0,
    update_cluster_result/0
]).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SERVER, ?MODULE).

-define(SHARD, emqx_ds_builtin_metadata_shard).
%% DS database metadata:
-define(META_TAB, emqx_ds_builtin_metadata_tab).
%% Mapping from Site to the actual Erlang node:
-define(NODE_TAB, emqx_ds_builtin_node_tab).
%% Shard metadata:
-define(SHARD_TAB, emqx_ds_builtin_shard_tab).

-record(?META_TAB, {
    db :: emqx_ds:db(),
    db_props :: emqx_ds_replication_layer:builtin_db_opts()
}).

-record(?NODE_TAB, {
    site :: site(),
    node :: node(),
    misc = #{} :: map()
}).

-record(?SHARD_TAB, {
    shard :: {emqx_ds:db(), emqx_ds_replication_layer:shard_id()},
    %% Sites that currently contain the data:
    replica_set :: [site()],
    %% Sites that should contain the data when the cluster is in the
    %% stable state (no nodes are being added or removed from it):
    target_set :: [site()] | undefined,
    misc = #{} :: map()
}).

%% Persistent ID of the node (independent from the IP/FQDN):
-type site() :: binary().

%% Membership transition of shard's replica set:
-type transition() :: {add | del, site()}.

-type update_cluster_result() ::
    ok
    | {error, {nonexistent_db, emqx_ds:db()}}
    | {error, {nonexistent_sites, [site()]}}
    | {error, {too_few_sites, [site()]}}
    | {error, _}.

%% Subject of the subscription:
-type subject() :: emqx_ds:db().

%% Event for the subscription:
-type subscription_event() ::
    {changed, {shard, emqx_ds:db(), emqx_ds_replication_layer:shard_id()}}.

%% Peristent term key:
-define(emqx_ds_builtin_site, emqx_ds_builtin_site).

%% Make Dialyzer happy
-define(NODE_PAT(),
    %% Equivalent of `#?NODE_TAB{_ = '_'}`:
    erlang:make_tuple(record_info(size, ?NODE_TAB), '_')
).

-define(SHARD_PAT(SHARD),
    %% Equivalent of `#?SHARD_TAB{shard = SHARD, _ = '_'}`
    erlang:make_tuple(record_info(size, ?SHARD_TAB), '_', [{#?SHARD_TAB.shard, SHARD}])
).

%%================================================================================
%% API funcions
%%================================================================================

-spec print_status() -> ok.
print_status() ->
    io:format("THIS SITE:~n~s~n", [this_site()]),
    io:format("~nSITES:~n", []),
    Nodes = [node() | nodes()],
    lists:foreach(
        fun(#?NODE_TAB{site = Site, node = Node}) ->
            Status =
                case lists:member(Node, Nodes) of
                    true -> up;
                    false -> down
                end,
            io:format("~s    ~p    ~p~n", [Site, Node, Status])
        end,
        eval_qlc(mnesia:table(?NODE_TAB))
    ),
    io:format(
        "~nSHARDS:~nId                             Replicas~n", []
    ),
    lists:foreach(
        fun(#?SHARD_TAB{shard = {DB, Shard}, replica_set = RS}) ->
            ShardStr = string:pad(io_lib:format("~p/~s", [DB, Shard]), 30),
            ReplicasStr = string:pad(io_lib:format("~p", [RS]), 40),
            io:format("~s ~s~n", [ShardStr, ReplicasStr])
        end,
        eval_qlc(mnesia:table(?SHARD_TAB))
    ).

-spec this_site() -> site().
this_site() ->
    persistent_term:get(?emqx_ds_builtin_site).

-spec n_shards(emqx_ds:db()) -> pos_integer().
n_shards(DB) ->
    [#?META_TAB{db_props = #{n_shards := NShards}}] = mnesia:dirty_read(?META_TAB, DB),
    NShards.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec shards(emqx_ds:db()) -> [emqx_ds_replication_layer:shard_id()].
shards(DB) ->
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    [Shard || #?SHARD_TAB{shard = {_, Shard}} <- Recs].

-spec shard_info(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    #{replica_set := #{site() => #{status => up | joining}}}
    | undefined.
shard_info(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [] ->
            undefined;
        [#?SHARD_TAB{replica_set = Replicas}] ->
            ReplicaSet = maps:from_list([
                begin
                    %% TODO:
                    ReplInfo = #{status => up},
                    {I, ReplInfo}
                end
             || I <- Replicas
            ]),
            #{replica_set => ReplicaSet}
    end.

-spec my_shards(emqx_ds:db()) -> [emqx_ds_replication_layer:shard_id()].
my_shards(DB) ->
    Site = this_site(),
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    [Shard || #?SHARD_TAB{shard = {_, Shard}, replica_set = RS} <- Recs, lists:member(Site, RS)].

allocate_shards(DB) ->
    case mria:transaction(?SHARD, fun ?MODULE:allocate_shards_trans/1, [DB]) of
        {atomic, Shards} ->
            {ok, Shards};
        {aborted, {shards_already_allocated, Shards}} ->
            {ok, Shards};
        {aborted, {insufficient_sites_online, Needed, Sites}} ->
            {error, #{reason => insufficient_sites_online, needed => Needed, sites => Sites}}
    end.

-spec sites() -> [site()].
sites() ->
    eval_qlc(qlc:q([Site || #?NODE_TAB{site = Site} <- mnesia:table(?NODE_TAB)])).

-spec node(site()) -> node() | undefined.
node(Site) ->
    case mnesia:dirty_read(?NODE_TAB, Site) of
        [#?NODE_TAB{node = Node}] ->
            Node;
        [] ->
            undefined
    end.

%%===============================================================================
%% DB API
%%===============================================================================

-spec db_config(emqx_ds:db()) -> emqx_ds_replication_layer:builtin_db_opts().
db_config(DB) ->
    case mnesia:dirty_read(?META_TAB, DB) of
        [#?META_TAB{db_props = Opts}] ->
            Opts;
        [] ->
            #{}
    end.

-spec open_db(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
open_db(DB, DefaultOpts) ->
    transaction(fun ?MODULE:open_db_trans/2, [DB, DefaultOpts]).

-spec update_db_config(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts() | {error, nonexistent_db}.
update_db_config(DB, DefaultOpts) ->
    transaction(fun ?MODULE:update_db_config_trans/2, [DB, DefaultOpts]).

-spec drop_db(emqx_ds:db()) -> ok.
drop_db(DB) ->
    transaction(fun ?MODULE:drop_db_trans/1, [DB]).

%%===============================================================================
%% Site / shard allocation API
%%===============================================================================

%% @doc Join a site to the set of sites the DB is replicated across.
-spec join_db_site(emqx_ds:db(), site()) -> update_cluster_result().
join_db_site(DB, Site) ->
    transaction(fun ?MODULE:modify_db_sites_trans/2, [DB, [{add, Site}]]).

%% @doc Make a site leave the set of sites the DB is replicated across.
-spec leave_db_site(emqx_ds:db(), site()) -> update_cluster_result().
leave_db_site(DB, Site) ->
    transaction(fun ?MODULE:modify_db_sites_trans/2, [DB, [{del, Site}]]).

%% @doc Assign a set of sites to the DB for replication.
-spec assign_db_sites(emqx_ds:db(), [site()]) -> update_cluster_result().
assign_db_sites(DB, Sites) ->
    transaction(fun ?MODULE:assign_db_sites_trans/2, [DB, Sites]).

%% @doc List the sites the DB is replicated across.
-spec db_sites(emqx_ds:db()) -> [site()].
db_sites(DB) ->
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    list_db_sites(Recs).

%% @doc List the sequence of transitions that should be conducted in order to
%% bring the set of replicas for a DB shard in line with the target set.
-spec replica_set_transitions(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    [transition()] | undefined.
replica_set_transitions(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [#?SHARD_TAB{target_set = TargetSet, replica_set = ReplicaSet}] ->
            compute_transitions(TargetSet, ReplicaSet);
        [] ->
            undefined
    end.

%% @doc Update the set of replication sites for a shard.
%% To be called after a `transition()` has been conducted successfully.
-spec update_replica_set(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), transition()) -> ok.
update_replica_set(DB, Shard, Trans) ->
    case mria:transaction(?SHARD, fun ?MODULE:update_replica_set_trans/3, [DB, Shard, Trans]) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.

%% @doc Get the current set of replication sites for a shard.
-spec replica_set(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    [site()] | undefined.
replica_set(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [#?SHARD_TAB{replica_set = ReplicaSet}] ->
            ReplicaSet;
        [] ->
            undefined
    end.

%% @doc Get the target set of replication sites for a DB shard.
%% Target set is updated every time the set of replication sites for the DB changes.
%% See `join_db_site/2`, `leave_db_site/2`, `assign_db_sites/2`.
-spec target_set(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    [site()] | undefined.
target_set(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [#?SHARD_TAB{target_set = TargetSet}] ->
            TargetSet;
        [] ->
            undefined
    end.

%%================================================================================

subscribe(Pid, Subject) ->
    gen_server:call(?SERVER, {subscribe, Pid, Subject}, infinity).

unsubscribe(Pid) ->
    gen_server:call(?SERVER, {unsubscribe, Pid}, infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    subs = #{} :: #{pid() => {subject(), _Monitor :: reference()}}
}).

init([]) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => [ds, meta]}),
    ensure_tables(),
    ensure_site(),
    S = #s{},
    {ok, _Node} = mnesia:subscribe({table, ?SHARD_TAB, simple}),
    {ok, S}.

handle_call({subscribe, Pid, Subject}, _From, S) ->
    {reply, ok, handle_subscribe(Pid, Subject, S)};
handle_call({unsubscribe, Pid}, _From, S) ->
    {reply, ok, handle_unsubscribe(Pid, S)};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({mnesia_table_event, {write, #?SHARD_TAB{shard = {DB, Shard}}, _}}, S) ->
    ok = notify_subscribers(DB, {shard, DB, Shard}, S),
    {noreply, S};
handle_info({'DOWN', _MRef, process, Pid, _Reason}, S) ->
    {noreply, handle_unsubscribe(Pid, S)};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{}) ->
    persistent_term:erase(?emqx_ds_builtin_site),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

-spec open_db_trans(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
open_db_trans(DB, CreateOpts) ->
    case mnesia:wread({?META_TAB, DB}) of
        [] ->
            mnesia:write(#?META_TAB{db = DB, db_props = CreateOpts}),
            CreateOpts;
        [#?META_TAB{db_props = Opts}] ->
            Opts
    end.

-spec allocate_shards_trans(emqx_ds:db()) -> [emqx_ds_replication_layer:shard_id()].
allocate_shards_trans(DB) ->
    Opts = #{n_shards := NShards} = db_config_trans(DB),
    NSites = maps:get(n_sites, Opts, 1),
    Nodes = mnesia:match_object(?NODE_TAB, ?NODE_PAT(), read),
    case length(Nodes) of
        N when N >= NSites ->
            ok;
        _ ->
            mnesia:abort({insufficient_sites_online, NSites, Nodes})
    end,
    case mnesia:match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'}), write) of
        [] ->
            ok;
        Records ->
            ShardsAllocated = [Shard || #?SHARD_TAB{shard = {_DB, Shard}} <- Records],
            mnesia:abort({shards_already_allocated, ShardsAllocated})
    end,
    Shards = gen_shards(NShards),
    Sites = [S || #?NODE_TAB{site = S} <- Nodes],
    Allocation = compute_allocation(Shards, Sites, Opts),
    lists:map(
        fun({Shard, ReplicaSet}) ->
            Record = #?SHARD_TAB{
                shard = {DB, Shard},
                replica_set = ReplicaSet
            },
            ok = mnesia:write(Record),
            Shard
        end,
        Allocation
    ).

-spec assign_db_sites_trans(emqx_ds:db(), [site()]) -> ok.
assign_db_sites_trans(DB, Sites) ->
    Opts = db_config_trans(DB),
    case [S || S <- Sites, mnesia:read(?NODE_TAB, S, read) == []] of
        [] when length(Sites) == 0 ->
            mnesia:abort({too_few_sites, Sites});
        [] ->
            ok;
        NonexistentSites ->
            mnesia:abort({nonexistent_sites, NonexistentSites})
    end,
    %% TODO
    %% Optimize reallocation. The goals are:
    %% 1. Minimize the number of membership transitions.
    %% 2. Ensure that sites are responsible for roughly the same number of shards.
    Shards = mnesia:match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'}), write),
    Reallocation = compute_allocation(Shards, Sites, Opts),
    lists:foreach(
        fun({Record, ReplicaSet}) ->
            ok = mnesia:write(Record#?SHARD_TAB{target_set = ReplicaSet})
        end,
        Reallocation
    ).

-spec modify_db_sites_trans(emqx_ds:db(), [transition()]) -> ok.
modify_db_sites_trans(DB, Modifications) ->
    Shards = mnesia:match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'}), write),
    Sites0 = list_db_target_sites(Shards),
    Sites = lists:foldl(fun apply_transition/2, Sites0, Modifications),
    case Sites of
        Sites0 ->
            ok;
        _Changed ->
            assign_db_sites_trans(DB, Sites)
    end.

update_replica_set_trans(DB, Shard, Trans) ->
    case mnesia:read(?SHARD_TAB, {DB, Shard}, write) of
        [Record = #?SHARD_TAB{replica_set = ReplicaSet0, target_set = TargetSet0}] ->
            %% NOTE
            %% It's possible to complete a transition that's no longer planned. We
            %% should anticipate that we may stray _away_ from the target set.
            TargetSet1 = emqx_maybe:define(TargetSet0, ReplicaSet0),
            ReplicaSet = apply_transition(Trans, ReplicaSet0),
            case lists:usort(TargetSet1) of
                ReplicaSet ->
                    TargetSet = undefined;
                TS ->
                    TargetSet = TS
            end,
            mnesia:write(Record#?SHARD_TAB{replica_set = ReplicaSet, target_set = TargetSet});
        [] ->
            mnesia:abort({nonexistent_shard, {DB, Shard}})
    end.

-spec update_db_config_trans(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
update_db_config_trans(DB, UpdateOpts) ->
    Opts = db_config_trans(DB, write),
    %% Since this is an update and not a reopen,
    %% we should keep the shard number and replication factor
    %% and not create a new shard server
    ChangeableOpts = maps:without([n_shards, n_sites, replication_factor], UpdateOpts),
    EffectiveOpts = maps:merge(Opts, ChangeableOpts),
    ok = mnesia:write(#?META_TAB{
        db = DB,
        db_props = EffectiveOpts
    }),
    EffectiveOpts.

-spec db_config_trans(emqx_ds:db()) -> emqx_ds_replication_layer:builtin_db_opts().
db_config_trans(DB) ->
    db_config_trans(DB, read).

db_config_trans(DB, LockType) ->
    case mnesia:read(?META_TAB, DB, LockType) of
        [#?META_TAB{db_props = Config}] ->
            Config;
        [] ->
            mnesia:abort({nonexistent_db, DB})
    end.

-spec drop_db_trans(emqx_ds:db()) -> ok.
drop_db_trans(DB) ->
    mnesia:delete({?META_TAB, DB}),
    [mnesia:delete({?SHARD_TAB, Shard}) || Shard <- shards(DB)],
    ok.

-spec claim_site(site(), node()) -> ok.
claim_site(Site, Node) ->
    mnesia:write(#?NODE_TAB{site = Site, node = Node}).

%%================================================================================
%% Internal functions
%%================================================================================

ensure_tables() ->
    ok = mria:create_table(?META_TAB, [
        {rlog_shard, ?SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?META_TAB},
        {attributes, record_info(fields, ?META_TAB)}
    ]),
    ok = mria:create_table(?NODE_TAB, [
        {rlog_shard, ?SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?NODE_TAB},
        {attributes, record_info(fields, ?NODE_TAB)}
    ]),
    ok = mria:create_table(?SHARD_TAB, [
        {rlog_shard, ?SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?SHARD_TAB},
        {attributes, record_info(fields, ?SHARD_TAB)}
    ]),
    ok = mria:wait_for_tables([?META_TAB, ?NODE_TAB, ?SHARD_TAB]).

ensure_site() ->
    Filename = filename:join(emqx_ds:base_dir(), "emqx_ds_builtin_site.eterm"),
    case file:consult(Filename) of
        {ok, [Site]} ->
            ok;
        _ ->
            Site = binary:encode_hex(crypto:strong_rand_bytes(8)),
            logger:notice("Creating a new site with ID=~s", [Site]),
            ok = filelib:ensure_dir(Filename),
            {ok, FD} = file:open(Filename, [write]),
            io:format(FD, "~p.", [Site]),
            file:close(FD)
    end,
    {atomic, ok} = mria:transaction(?SHARD, fun ?MODULE:claim_site/2, [Site, node()]),
    persistent_term:put(?emqx_ds_builtin_site, Site),
    ok.

%% @doc Returns sorted list of sites shards are replicated across.
-spec list_db_sites([_Shard]) -> [site()].
list_db_sites(Shards) ->
    flatmap_sorted_set(fun get_shard_sites/1, Shards).

-spec list_db_target_sites([_Shard]) -> [site()].
list_db_target_sites(Shards) ->
    flatmap_sorted_set(fun get_shard_target_sites/1, Shards).

-spec get_shard_sites(_Shard) -> [site()].
get_shard_sites(#?SHARD_TAB{replica_set = ReplicaSet}) ->
    ReplicaSet.

-spec get_shard_target_sites(_Shard) -> [site()].
get_shard_target_sites(#?SHARD_TAB{target_set = Sites}) when is_list(Sites) ->
    Sites;
get_shard_target_sites(#?SHARD_TAB{target_set = undefined} = Shard) ->
    get_shard_sites(Shard).

-spec compute_allocation([Shard], [Site], emqx_ds_replication_layer:builtin_db_opts()) ->
    [{Shard, [Site, ...]}].
compute_allocation(Shards, Sites, Opts) ->
    NSites = length(Sites),
    ReplicationFactor = maps:get(replication_factor, Opts),
    NReplicas = min(NSites, ReplicationFactor),
    ShardsSorted = lists:sort(Shards),
    SitesSorted = lists:sort(Sites),
    {Allocation, _} = lists:mapfoldl(
        fun(Shard, SSites) ->
            {ReplicaSet, _} = emqx_utils_stream:consume(NReplicas, SSites),
            {_, SRest} = emqx_utils_stream:consume(1, SSites),
            {{Shard, ReplicaSet}, SRest}
        end,
        emqx_utils_stream:repeat(emqx_utils_stream:list(SitesSorted)),
        ShardsSorted
    ),
    Allocation.

compute_transitions(undefined, _ReplicaSet) ->
    [];
compute_transitions(TargetSet, ReplicaSet) ->
    Additions = TargetSet -- ReplicaSet,
    Deletions = ReplicaSet -- TargetSet,
    intersperse([{add, S} || S <- Additions], [{del, S} || S <- Deletions]).

%% @doc Apply a transition to a list of sites, preserving sort order.
-spec apply_transition(transition(), [site()]) -> [site()].
apply_transition({add, S}, Sites) ->
    lists:usort([S | Sites]);
apply_transition({del, S}, Sites) ->
    lists:delete(S, Sites).

gen_shards(NShards) ->
    [integer_to_binary(I) || I <- lists:seq(0, NShards - 1)].

eval_qlc(Q) ->
    case mnesia:is_transaction() of
        true ->
            qlc:eval(Q);
        false ->
            {atomic, Result} = mria:ro_transaction(?SHARD, fun() -> qlc:eval(Q) end),
            Result
    end.

transaction(Fun, Args) ->
    case mria:transaction(?SHARD, Fun, Args) of
        {atomic, Result} ->
            Result;
        {aborted, Reason} ->
            {error, Reason}
    end.

%%====================================================================

handle_subscribe(Pid, Subject, S = #s{subs = Subs0}) ->
    case maps:is_key(Pid, Subs0) of
        false ->
            MRef = erlang:monitor(process, Pid),
            Subs = Subs0#{Pid => {Subject, MRef}},
            S#s{subs = Subs};
        true ->
            S
    end.

handle_unsubscribe(Pid, S = #s{subs = Subs0}) ->
    case maps:take(Pid, Subs0) of
        {{_Subject, MRef}, Subs} ->
            _ = erlang:demonitor(MRef, [flush]),
            S#s{subs = Subs};
        error ->
            S
    end.

notify_subscribers(EventSubject, Event, #s{subs = Subs}) ->
    maps:foreach(
        fun(Pid, {Subject, _MRef}) ->
            Subject == EventSubject andalso
                erlang:send(Pid, {changed, Event})
        end,
        Subs
    ).

%%====================================================================

%% @doc Intersperse elements of two lists.
%% Example: intersperse([1, 2], [3, 4, 5]) -> [1, 3, 2, 4, 5].
-spec intersperse([X], [Y]) -> [X | Y].
intersperse(L1, []) ->
    L1;
intersperse([], L2) ->
    L2;
intersperse([H1 | T1], L2) ->
    [H1 | intersperse(L2, T1)].

%% @doc Map list into a list of sets and return union, as a sorted list.
-spec flatmap_sorted_set(fun((X) -> [Y]), [X]) -> [Y].
flatmap_sorted_set(Fun, L) ->
    ordsets:to_list(
        lists:foldl(
            fun(X, Acc) -> ordsets:union(ordsets:from_list(Fun(X)), Acc) end,
            ordsets:new(),
            L
        )
    ).
