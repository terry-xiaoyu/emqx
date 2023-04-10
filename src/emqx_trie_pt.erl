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
-module(emqx_trie_pt).
-behaviour(gen_server).

%% Trie APIs
-export([ load/1
        , delete/2
        , clear/0
        , match/1
        ]).

-export([start_link/0]).

%% Callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-type subscriber() :: #{
    clientid := emqx_types:clientid(),
    topic := emqx_types:topic(),
    subopts := emqx_types:subopts()
}.

-type word() :: binary(). %% <<"+">>, <<"#">>, <<"foo">>, <<"bar">>...

-type trie() :: #{
    word() := trie(),
    ref_count := pos_integer(),
    subscribers => [subscriber()]
} | nil.

-type sub_info() :: {emqx_types:topic(), emqx_types:subopts()}.

-type client_sub_info() :: {emqx_types:clientid(), sub_info()}.

%%%=============================================================================
%%% APIs
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

load(ClientSubInfoList) ->
    call({do_load, ClientSubInfoList}).

delete(ClientId, Topic) ->
    call({do_delete, ClientId, Topic}).

clear() ->
    call(do_clear).

match(Topic) ->
    do_match(Topic).

%%%=============================================================================
%%% GenServer callbacks

init([]) ->
    {ok, #{}}.

handle_call({do_load, ClientSubInfoList}, _From, LoopState) ->
    {reply, do_load(ClientSubInfoList), LoopState};

handle_call({do_delete, ClientId, Topic}, _From, LoopState) ->
    {reply, do_delete(ClientId, Topic), LoopState};

handle_call(do_clear, _From, LoopState) ->
    {reply, do_clear(), LoopState};

handle_call(_Request, _From, LoopState) ->
    {reply, ok, LoopState}.

handle_cast(_Msg, LoopState) ->
    {noreply, LoopState}.

handle_info(_Info, LoopState) ->
    {noreply, LoopState}.

terminate(_Reason, _LoopState) ->
    ok.

code_change(_OldVsn, LoopState, _Extra) ->
    {ok, LoopState}.

%%%=============================================================================
%%% Internal functions
-define(WC_NUM_NOT_AT_END, <<"The '#' must be at the last level of the topic">>).
call(Req) ->
    gen_server:call(?MODULE, Req, infinity).

-spec do_load([client_sub_info()]) -> ok.
do_load(ClientSubInfoList) ->
    put_trie(
        lists:foldl(fun do_add/2, get_trie(), ClientSubInfoList)).

-spec do_add(client_sub_info(), trie()) -> trie().
do_add({ClientId, {Topic, SubOpts}}, Trie) ->
    do_add(emqx_topic:words(Topic), ClientId, Topic, SubOpts, Trie).

do_add([], ClientId, Topic, SubOpts, Trie) ->
    Subsbrs = maps:get(subscribers, Trie, []),
    Subscriber = #{clientid => ClientId, topic => Topic, subopts => SubOpts},
    Trie#{subscribers => lists:reverse([Subscriber | lists:reverse(Subsbrs)])};
do_add(['#' | Words], _, Topic, _, _) when Words =/= [] ->
    throw({invalid_topic, #{topic => Topic, reason => ?WC_NUM_NOT_AT_END}});
do_add([W | Words], ClientId, Topic, SubOpts, Trie) ->
    SubTrie = maps:get(W, Trie, #{}),
    RefCnt = maps:get(ref_count, SubTrie, 0),
    NewSubTrie = do_add(Words, ClientId, Topic, SubOpts, SubTrie),
    Trie#{W => NewSubTrie#{ref_count => RefCnt + 1}}.

-spec do_delete(emqx_types:clientid(), emqx_types:topic()) -> ok.
do_delete(ClientId, Topic) ->
    do_delete(emqx_topic:words(Topic), ClientId, get_trie()).

do_delete([], ClientId, Trie) ->
    Subsbrs = maps:get(subscribers, Trie, []),
    Trie#{subscribers => [Subsbrs || #{clientid := Id} <- Subsbrs, Id =/= ClientId]};
do_delete([W | Words], ClientId, Trie) ->
    SubTrie = maps:get(W, Trie, #{}),
    case maps:get(ref_count, SubTrie, 0) of
        RefCnt when RefCnt > 0 ->
            NewSubTrie = do_delete(Words, ClientId, SubTrie),
            Trie#{W => NewSubTrie#{ref_count => RefCnt - 1}};
        _ ->
            maps:remove(W, Trie)
    end.

do_match(Topic) ->
    do_match(emqx_topic:words(Topic), get_trie(), []).

do_match([], Trie, Acc) ->
    Subsbrs = maps:get(subscribers, Trie, []),
    'maybe_append_#_subsbrs'(Trie, Subsbrs ++ Acc);
do_match([W | Words], Trie, Acc) ->
    ExactlySubsbrs = do_match_word(W, Words, Trie, Acc),
    WildcardPlusSubsbrs = do_match_word('+', Words, Trie, Acc),
    'maybe_append_#_subsbrs'(Trie, ExactlySubsbrs ++ WildcardPlusSubsbrs).

'maybe_append_#_subsbrs'(Trie, Subsbrs) ->
    case maps:get('#', Trie, #{}) of
        #{subscribers := WildcardNumSubsbrs} ->
            Subsbrs ++ WildcardNumSubsbrs;
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
    clear_trie().

get_trie() ->
    persistent_term:get(?MODULE, #{}).

put_trie(Trie) ->
    persistent_term:put(?MODULE, Trie).

clear_trie() ->
    persistent_term:erase(?MODULE),
    ok.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

load_trie_test() ->
    ?assertEqual(ok, do_load([
            {<<"c1">>, {<<"t">>, #{qos => 0}}},
            {<<"c2">>, {<<"t/a">>, #{qos => 0}}},
            {<<"c3">>, {<<"t/a/b">>, #{qos => 0}}},
            {<<"c4">>, {<<"t/a/+">>, #{qos => 0}}},
            {<<"c5">>, {<<"t/a/+/c">>, #{qos => 0}}},
            {<<"c6">>, {<<"t/#">>, #{qos => 0}}}
        ])),
    ?assertEqual([
            <<"t/a/b">>,
            <<"t/a/+">>,
            <<"t/#">>
        ], do_match(<<"t/a/b">>)),
    ?assertEqual([
            <<"t/a/+/c">>,
            <<"t/#">>
        ], do_match(<<"t/a/b/c">>)).

-endif.