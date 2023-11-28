%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% An trie implementation uses persistent_term as backend
%% To make this trie work in a cluster, the emqx_channel_conn table
%% should be replicated to each of the node.

-module(emqx_preloaded_sub).
-behaviour(gen_server).

-include("logger.hrl").
-include("emqx_mqtt.hrl").

%% APIs
-export([
    is_enabled/0,
    get_subscription/2,
    get_subopts/2,
    load/1,
    delete/2,
    clear/0,
    match/1,
    reload_trie/0,
    get_trie/0
]).

-export([parse_sub_info_files/1]).

-export([start_link/0]).

%% Callbacks
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

-record(client_sub_info, {
    key :: {emqx_types:clientid(), emqx_types:topic()},
    subopts :: emqx_types:subopts()
}).

-type subscribers() :: #{
    emqx_types:clientid() => #{topic := emqx_types:topic(), subopts := emqx_types:subopts()}
}.

%% <<"+">>, <<"#">>, <<"foo">>, <<"bar">>...
-type word() :: binary().

-type trie() ::
    #{
        word() := trie(),
        subscribers => subscribers()
    }
    | nil.

-type client_sub_info() :: #client_sub_info{}.
-type client_sub_info_source() ::
    [client_sub_info()] | {files, [file:name_all()]} | {dir, file:name_all()}.

-define(SYNC_INTERVAL, 60000).
-define(TRIE_SERVICE, ?MODULE).
-define(CLIENT_SUB_INFO_TAB, emqx_client_sub_info).
-define(RLOG_SHARD, emqx_client_sub_info_shard).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------
mnesia(boot) ->
    ok = mria:create_table(?CLIENT_SUB_INFO_TAB, [
        {type, ordered_set},
        {rlog_shard, ?RLOG_SHARD},
        {storage, disc_copies},
        {record_name, client_sub_info},
        {attributes, record_info(fields, client_sub_info)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]).

%%=============================================================================
%% APIs
%%=============================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

is_enabled() ->
    emqx_config:get([emqx_preloaded_sub, enable], false).

get_subscription(ClientId, TopicFilter) ->
    ets:lookup(?CLIENT_SUB_INFO_TAB, {ClientId, TopicFilter}).

get_subopts(ClientId, TopicFilter) ->
    case get_subscription(ClientId, TopicFilter) of
        [#client_sub_info{subopts = SubOpts}] ->
            SubOpts;
        [] ->
            undefined
    end.

-spec load(client_sub_info_source()) -> ok.
load(ClientSubInfoSource) ->
    call({do_load, ClientSubInfoSource}).

delete(ClientId, TopicFilter) ->
    call({do_delete, ClientId, TopicFilter}).

clear() ->
    call(do_clear).

match(Topic) ->
    do_match(Topic).

%%%=============================================================================
%%% GenServer callbacks

init([]) ->
    _ = mria:wait_for_tables([?CLIENT_SUB_INFO_TAB]),
    {ok, _} = mnesia:subscribe({table, ?CLIENT_SUB_INFO_TAB, detailed}),
    {ok, _} = mnesia:subscribe(activity),
    {ok, #{client_sub_info_written => #{}}, {continue, reload_trie}}.

handle_continue(reload_trie, State) ->
    ok = reload_trie(),
    {noreply, State, hibernate}.

handle_call({do_load, ClientSubInfoSource}, _From, LoopState) ->
    {reply, do_load(ClientSubInfoSource), LoopState};
handle_call({do_delete, ClientId, TopicFilter}, _From, LoopState) ->
    {reply, do_delete(ClientId, TopicFilter), LoopState};
handle_call(do_clear, _From, LoopState) ->
    {reply, do_clear(), LoopState};
handle_call(_Request, _From, LoopState) ->
    {reply, ok, LoopState}.

handle_cast(Msg, LoopState) ->
    ?SLOG(warning, #{msg => unexpected_cast, info => Msg}),
    {noreply, LoopState}.

handle_info(
    {mnesia_activity_event, {complete, ActivityId}},
    #{client_sub_info_written := SubInfoWritten} = LoopState
) ->
    case maps:find(ActivityId, SubInfoWritten) of
        {ok, {AddedSubInfo, RemovedSubInfo}} ->
            ?SLOG(debug, #{msg => sub_info_change_complete, activity_id => ActivityId}),
            Trie1 = lists:foldl(
                fun(#client_sub_info{key = {ClientId, TopicFilter}}, TrieAcc) ->
                    do_delete_trie(TopicFilter, ClientId, TrieAcc)
                end,
                get_trie(),
                RemovedSubInfo
            ),
            Trie2 = lists:foldl(
                fun(ClientSubInfo, TrieAcc) ->
                    do_add_trie(ClientSubInfo, TrieAcc)
                end,
                Trie1,
                AddedSubInfo
            ),
            ok = put_trie(Trie2);
        error ->
            ok
    end,
    {noreply, LoopState#{client_sub_info_written => maps:remove(ActivityId, SubInfoWritten)}};
handle_info({mnesia_table_event, {write, schema, _, _, _}}, LoopState) ->
    {noreply, LoopState};
handle_info(
    {mnesia_table_event, {write, _, NewClientSubInfo, OldClientSubInfos, ActivityId}},
    #{client_sub_info_written := SubInfoWritten} = LoopState
) ->
    ?SLOG(debug, #{
        msg => sub_info_updated,
        new_sub_info => NewClientSubInfo,
        old_sub_info => OldClientSubInfos,
        activity_id => ActivityId
    }),
    {AddedSubInfo, RemovedSubInfo} = maps:get(ActivityId, SubInfoWritten, {[], []}),
    {noreply, LoopState#{
        client_sub_info_written => SubInfoWritten#{
            ActivityId =>
                {[NewClientSubInfo | AddedSubInfo], OldClientSubInfos ++ RemovedSubInfo}
        }
    }};
handle_info(
    {mnesia_table_event, {delete, schema, {schema, ?CLIENT_SUB_INFO_TAB}, _, _}}, LoopState
) ->
    put_trie(new_trie()),
    {noreply, LoopState};
handle_info(
    {mnesia_table_event, {delete, _, _, OldClientSubInfos, ActivityId}},
    #{client_sub_info_written := SubInfoWritten} = LoopState
) ->
    ?SLOG(debug, #{
        msg => sub_info_deleted,
        old_sub_info => OldClientSubInfos,
        activity_id => ActivityId
    }),
    {AddedSubInfo, RemovedSubInfo} = maps:get(ActivityId, SubInfoWritten, {[], []}),
    {noreply, LoopState#{
        client_sub_info_written => SubInfoWritten#{
            ActivityId =>
                {AddedSubInfo, OldClientSubInfos ++ RemovedSubInfo}
        }
    }};
handle_info(Info, LoopState) ->
    ?SLOG(warning, #{msg => unexpected_info, info => Info}),
    {noreply, LoopState}.

terminate(_Reason, _LoopState) ->
    ok.

code_change(_OldVsn, LoopState, _Extra) ->
    {ok, LoopState}.

%%%=============================================================================
%%% Internal functions
-define(WC_NUM_NOT_AT_END, <<"The '#' must be at the last level of the topic">>).
call(Req) ->
    call(Req, infinity).
call(Req, Timeout) ->
    gen_server:call(?MODULE, Req, Timeout).

-spec do_load(client_sub_info_source()) -> ok.
do_load(ClientSubInfoList) when is_list(ClientSubInfoList) ->
    {atomic, ok} = mria:transaction(?RLOG_SHARD, fun() ->
        lists:foreach(
            fun(#client_sub_info{} = ClientSubInfo) ->
                case mnesia:wread({?CLIENT_SUB_INFO_TAB, ClientSubInfo#client_sub_info.key}) of
                    [_] -> ok;
                    [] -> ok = mnesia:write(?CLIENT_SUB_INFO_TAB, ClientSubInfo, write)
                end
            end,
            ClientSubInfoList
        )
    end),
    ok;
do_load({files, []}) ->
    ?SLOG(warning, #{msg => "load_sub_file_failed", reason => nothing_to_load});
do_load({files, FileNameList}) ->
    case parse_sub_info_files(FileNameList) of
        {ok, ClientSubInfoList} ->
            do_load(ClientSubInfoList);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "load_sub_file_failed",
                files => FileNameList,
                reason => Reason
            })
    end;
do_load({dir, Path}) ->
    do_load({files, filelib:wildcard(filename:join(Path, "*.conf"))}).

reload_trie() ->
    put_trie(
        mria:async_dirty(?RLOG_SHARD, fun() ->
            mnesia:foldl(
                fun(ClientSubInfo, TrieAcc) ->
                    do_add_trie(ClientSubInfo, TrieAcc)
                end,
                new_trie(),
                ?CLIENT_SUB_INFO_TAB
            )
        end)
    ).

-spec do_add_trie(client_sub_info(), trie()) -> trie().
do_add_trie(#client_sub_info{key = {ClientId, TopicFilter}, subopts = SubOpts}, Trie) ->
    #{root := RootTrie, non_wildcard_subs := NonWildcardSubs} = Trie,
    Words = emqx_topic:tokens(TopicFilter),
    case has_wildcard(Words) of
        true ->
            Trie#{root => do_add_trie(Words, ClientId, TopicFilter, SubOpts, RootTrie)};
        false ->
            Subs = maps:get(TopicFilter, NonWildcardSubs, #{}),
            Trie#{
                non_wildcard_subs => NonWildcardSubs#{
                    TopicFilter => Subs#{
                        ClientId => #{topic => TopicFilter, subopts => SubOpts}
                    }
                }
            }
    end.

do_add_trie([], ClientId, TopicFilter, SubOpts, Trie) ->
    Subsbrs = maps:get(subscribers, Trie, #{}),
    Trie#{subscribers => Subsbrs#{ClientId => #{topic => TopicFilter, subopts => SubOpts}}};
do_add_trie([<<"#">> | Words], _, TopicFilter, _, _) when Words =/= [] ->
    throw({invalid_topic, #{topic => TopicFilter, reason => ?WC_NUM_NOT_AT_END}});
do_add_trie([W | Words], ClientId, TopicFilter, SubOpts, Trie) ->
    SubTrie = maps:get(W, Trie, #{}),
    Trie#{W => do_add_trie(Words, ClientId, TopicFilter, SubOpts, SubTrie)}.

-spec do_delete(emqx_types:clientid(), emqx_types:topic()) -> ok.
do_delete(ClientId, TopicFilter) ->
    {atomic, ok} = mria:transaction(?RLOG_SHARD, fun() ->
        ok = mnesia:delete({?CLIENT_SUB_INFO_TAB, {ClientId, TopicFilter}})
    end),
    ok.

do_delete_trie(TopicFilter, ClientId, Trie) when is_binary(TopicFilter) ->
    #{root := RootTrie, non_wildcard_subs := NonWildcardSubs} = Trie,
    Words = emqx_topic:tokens(TopicFilter),
    case has_wildcard(Words) of
        true ->
            Trie#{root => do_delete_trie(Words, ClientId, RootTrie)};
        false ->
            Subs = maps:get(TopicFilter, NonWildcardSubs, #{}),
            case maps:remove(ClientId, Subs) of
                Subsbrs when map_size(Subsbrs) > 0 ->
                    Trie#{non_wildcard_subs => maps:put(TopicFilter, Subsbrs, NonWildcardSubs)};
                _ ->
                    Trie#{non_wildcard_subs => maps:remove(TopicFilter, NonWildcardSubs)}
            end
    end;
do_delete_trie([], ClientId, Trie) ->
    case maps:remove(ClientId, maps:get(subscribers, Trie, #{})) of
        Subscribers when map_size(Subscribers) > 0 ->
            Trie#{subscribers => Subscribers};
        _ ->
            maps:remove(subscribers, Trie)
    end;
do_delete_trie([W | Words], ClientId, Trie) ->
    case maps:get(W, Trie, #{}) of
        SubTrie when map_size(SubTrie) > 0 ->
            case do_delete_trie(Words, ClientId, SubTrie) of
                NewSubTrie when map_size(NewSubTrie) > 0 ->
                    Trie#{W => NewSubTrie};
                _ ->
                    maps:remove(W, Trie)
            end;
        _ ->
            maps:remove(W, Trie)
    end.

do_match(#share{topic = Topic}) ->
    do_match(Topic);
do_match(Topic) when is_binary(Topic) ->
    #{root := RootTrie, non_wildcard_subs := NonWildcardSubs} = get_trie(),
    maps:to_list(maps:get(Topic, NonWildcardSubs, #{})) ++
        case map_size(RootTrie) > 0 of
            true -> do_match(emqx_topic:tokens(Topic), RootTrie, []);
            false -> []
        end.

do_match([], Trie, Acc) ->
    Subsbrs = maps:get(subscribers, Trie, #{}),
    'maybe_append_#_subsbrs'(Trie, maps:to_list(Subsbrs) ++ Acc);
do_match([W | Words], Trie, Acc) ->
    ExactlySubsbrs = do_match_word(W, Words, Trie, Acc),
    WildcardPlusSubsbrs = do_match_word(<<"+">>, Words, Trie, Acc),
    'maybe_append_#_subsbrs'(Trie, ExactlySubsbrs ++ WildcardPlusSubsbrs).

'maybe_append_#_subsbrs'(Trie, Subsbrs) ->
    case maps:get(<<"#">>, Trie, #{}) of
        #{subscribers := WildcardNumSubsbrs} ->
            maps:to_list(WildcardNumSubsbrs) ++ Subsbrs;
        _ ->
            Subsbrs
    end.

do_match_word(W, Words, Trie, Acc) ->
    case maps:find(W, Trie) of
        {ok, SubTrie} ->
            do_match(Words, SubTrie, Acc);
        error ->
            Acc
    end.

do_clear() ->
    {atomic, ok} = mria:clear_table(?CLIENT_SUB_INFO_TAB),
    ok.

new_trie() ->
    #{root => #{}, non_wildcard_subs => #{}}.

get_trie() ->
    persistent_term:get(?MODULE, new_trie()).

put_trie(Trie) ->
    persistent_term:put(?MODULE, Trie).

parse_sub_info_files(FileName) ->
    case hocon:files(FileName) of
        {ok, #{<<"subscriptions">> := Subs}} when is_list(Subs) ->
            try
                {ok, parse_sub_info_list(Subs)}
            catch
                throw:Reason ->
                    {error, Reason}
            end;
        {ok, _Subs} ->
            {error, #{
                reason => invalid_sub_info_format,
                details => <<"the 'subscriptions' field is not found or not a array">>
            }};
        {error, Reason} ->
            {error, #{reason => invalid_hocon_file, details => Reason}}
    end.

parse_sub_info_list(Subs) ->
    lists:map(fun parse_sub_info/1, Subs).

parse_sub_info(
    #{
        <<"clientid">> := ClientId,
        <<"topic_filter">> := TopicFilter,
        <<"qos">> := Qos
    } = SubInfo
) ->
    #client_sub_info{
        key = {ClientId, TopicFilter},
        subopts = maybe_with_subid(
            #{
                qos => Qos,
                rh => maps:get(<<"retain_handling">>, SubInfo, 0),
                rap => maps:get(<<"retain_as_published">>, SubInfo, 0),
                nl => maps:get(<<"no_local">>, SubInfo, 0)
            },
            maps:get(<<"subid">>, SubInfo, undefined)
        )
    };
parse_sub_info(SubInfo) ->
    throw(#{
        reason => invalid_sub_info,
        details =>
            <<"one of the mandatory fields missing: 'clientid', 'topic_filter', 'qos'">>,
        sub_info => SubInfo
    }).

maybe_with_subid(SubOpts, undefined) ->
    SubOpts;
maybe_with_subid(SubOpts, SubId) ->
    SubOpts#{subid => SubId}.

-spec has_wildcard(emqx_types:words()) -> true | false.
has_wildcard([<<"#">> | _]) ->
    true;
has_wildcard([<<"+">> | _]) ->
    true;
has_wildcard([_H | T]) ->
    has_wildcard(T);
has_wildcard([]) ->
    false.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

load_trie_test() ->
    mria:start(),
    mnesia(boot),
    {ok, _} = emqx_preloaded_sub:start_link(),
    ?assertEqual(
        ok,
        load([
            #client_sub_info{key = {<<"c1">>, <<"t">>}, subopts = #{qos => 0}},
            #client_sub_info{key = {<<"c2">>, <<"t/a">>}, subopts = #{qos => 0}},
            #client_sub_info{key = {<<"c3">>, <<"t/a/b">>}, subopts = #{qos => 0}},
            #client_sub_info{key = {<<"c4">>, <<"t/a/+">>}, subopts = #{qos => 0}},
            #client_sub_info{key = {<<"c5">>, <<"t/a/+/c">>}, subopts = #{qos => 0}},
            #client_sub_info{key = {<<"c6">>, <<"t/#">>}, subopts = #{qos => 0}},
            #client_sub_info{key = {<<"c7">>, <<"/t/a/b/">>}, subopts = #{qos => 0}},
            #client_sub_info{key = {<<"c8">>, <<"/t/a/b/#">>}, subopts = #{qos => 0}},
            #client_sub_info{key = {<<"c9">>, <<"/t//a/b">>}, subopts = #{qos => 0}},
            #client_sub_info{key = {<<"c10">>, <<"/t//+/b">>}, subopts = #{qos => 0}}
        ])
    ),
    %% wait for the trie to be loaded
    _ = sys:get_state(emqx_preloaded_sub),
    io:format("CLIENT_SUB_INFO_TAB: ~p~n", [ets:tab2list(?CLIENT_SUB_INFO_TAB)]),
    io:format("get_trie(): ~p~n", [get_trie()]),

    ?assertMatch(
        [
            {<<"c1">>, #{topic := <<"t">>}},
            {<<"c6">>, #{topic := <<"t/#">>}}
        ],
        do_match(<<"t">>)
    ),

    MatchR1 = do_match(<<"t/a/b">>),
    ?assertEqual(3, length(MatchR1)),
    ?assertMatch(#{topic := <<"t/a/b">>}, proplists:get_value(<<"c3">>, MatchR1)),
    ?assertMatch(#{topic := <<"t/a/+">>}, proplists:get_value(<<"c4">>, MatchR1)),
    ?assertMatch(#{topic := <<"t/#">>}, proplists:get_value(<<"c6">>, MatchR1)),

    MatchR2 = do_match(<<"t/a/b/c">>),
    ?assertEqual(2, length(MatchR2)),
    ?assertMatch(#{topic := <<"t/a/+/c">>}, proplists:get_value(<<"c5">>, MatchR2)),
    ?assertMatch(#{topic := <<"t/#">>}, proplists:get_value(<<"c6">>, MatchR2)),

    MatchR3 = do_match(<<"/t/a/b/">>),
    ?assertEqual(2, length(MatchR3)),
    ?assertMatch(#{topic := <<"/t/a/b/">>}, proplists:get_value(<<"c7">>, MatchR3)),
    ?assertMatch(#{topic := <<"/t/a/b/#">>}, proplists:get_value(<<"c8">>, MatchR3)),

    MatchR4 = do_match(<<"/t/a/b">>),
    ?assertEqual(1, length(MatchR4)),
    ?assertMatch(#{topic := <<"/t/a/b/#">>}, proplists:get_value(<<"c8">>, MatchR4)),

    MatchR5 = do_match(<<"/t//a/b">>),
    ?assertEqual(2, length(MatchR5)),
    ?assertMatch(#{topic := <<"/t//a/b">>}, proplists:get_value(<<"c9">>, MatchR5)),
    ?assertMatch(#{topic := <<"/t//+/b">>}, proplists:get_value(<<"c10">>, MatchR5)),

    ok = do_delete(<<"c1">>, <<"t">>),
    timer:sleep(50),
    ?assertMatch([{<<"c6">>, #{topic := <<"t/#">>}}], do_match(<<"t">>)),

    ok = do_delete(<<"c6">>, <<"t/#">>),
    timer:sleep(50),
    ?assertEqual([], do_match(<<"t">>)),

    MatchR6 = do_match(<<"t/a/b">>),
    ?assertEqual(2, length(MatchR6)),
    ?assertMatch(#{topic := <<"t/a/b">>}, proplists:get_value(<<"c3">>, MatchR6)),
    ?assertMatch(#{topic := <<"t/a/+">>}, proplists:get_value(<<"c4">>, MatchR6)),

    MatchR7 = do_match(<<"t/a/b/c">>),
    ?assertEqual(1, length(MatchR7)),
    ?assertMatch(#{topic := <<"t/a/+/c">>}, proplists:get_value(<<"c5">>, MatchR7)),

    ok = do_delete(<<"c2">>, <<"t/a">>),
    ok = do_delete(<<"c3">>, <<"t/a/b">>),
    ok = do_delete(<<"c4">>, <<"t/a/+">>),
    ok = do_delete(<<"c5">>, <<"t/a/+/c">>),
    ok = do_delete(<<"c7">>, <<"/t/a/b/">>),
    ok = do_delete(<<"c8">>, <<"/t/a/b/#">>),
    ok = do_delete(<<"c9">>, <<"/t//a/b">>),
    ok = do_delete(<<"c10">>, <<"/t//+/b">>),
    timer:sleep(50),
    ?assertEqual(new_trie(), get_trie()),
    ok.

-endif.
