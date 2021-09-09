%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Bridge works in two layers (1) batching layer (2) transport layer
%% The `bridge' batching layer collects local messages in batches and sends over
%% to remote MQTT node/cluster via `connection' transport layer.
%% In case `REMOTE' is also an EMQX node, `connection' is recommended to be
%% the `gen_rpc' based implementation `emqx_bridge_rpc'. Otherwise `connection'
%% has to be `emqx_bridge_mqtt'.
%%
%% ```
%% +------+                        +--------+
%% | EMQX |                        | REMOTE |
%% |      |                        |        |
%% |   (bridge) <==(connection)==> |        |
%% |      |                        |        |
%% |      |                        |        |
%% +------+                        +--------+
%% '''
%%
%%
%% This module implements 2 kinds of APIs with regards to batching and
%% messaging protocol. (1) A `gen_statem' based local batch collector;
%% (2) APIs for incoming remote batches/messages.
%%
%% Batch collector state diagram
%%
%% [idle] --(0) --> [connecting] --(2)--> [connected]
%%                  |        ^                 |
%%                  |        |                 |
%%                  '--(1)---'--------(3)------'
%%
%% (0): auto or manual start
%% (1): retry timeout
%% (2): successfuly connected to remote node/cluster
%% (3): received {disconnected, Reason} OR
%%      failed to send to remote node/cluster.
%%
%% NOTE: A bridge worker may subscribe to multiple (including wildcard)
%% local topics, and the underlying `emqx_bridge_connect' may subscribe to
%% multiple remote topics, however, worker/connections are not designed
%% to support automatic load-balancing, i.e. in case it can not keep up
%% with the amount of messages comming in, administrator should split and
%% balance topics between worker/connections manually.
%%
%% NOTES:
%% * Local messages are all normalised to QoS-1 when exporting to remote

-module(emqx_bridge_worker).
-behaviour(gen_statem).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% APIs
-export([ start_link/1
        , register_metrics/0
        , stop/1
        ]).

%% gen_statem callbacks
-export([ terminate/3
        , code_change/4
        , init/1
        , callback_mode/0
        ]).

%% state functions
-export([ idle/3
        , connected/3
        ]).

%% management APIs
-export([ ensure_started/1
        , ensure_stopped/1
        , status/1
        , ping/1
        ]).

-export([ get_forwards/1
        ]).

-export([ get_subscriptions/1
        ]).

%% Internal
-export([msg_marshaller/1]).

-export_type([ config/0
             , batch/0
             , ack_ref/0
             ]).

-type id() :: atom() | string() | pid().
-type qos() :: emqx_mqtt_types:qos().
-type config() :: map().
-type batch() :: [emqx_bridge_msg:exp_msg()].
-type ack_ref() :: term().
-type topic() :: emqx_topic:topic().

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").


%% same as default in-flight limit for emqtt
-define(DEFAULT_BATCH_SIZE, 32).
-define(DEFAULT_RECONNECT_DELAY_MS, timer:seconds(5)).
-define(DEFAULT_SEG_BYTES, (1 bsl 20)).
-define(DEFAULT_MAX_TOTAL_SIZE, (1 bsl 31)).

%% @doc Start a bridge worker. Supported configs:
%% start_type: 'manual' (default) or 'auto', when manual, bridge will stay
%%      at 'idle' state until a manual call to start it.
%% connect_module: The module which implements emqx_bridge_connect behaviour
%%      and work as message batch transport layer
%% reconnect_interval: Delay in milli-seconds for the bridge worker to retry
%%      in case of transportation failure.
%% max_inflight: Max number of batches allowed to send-ahead before receiving
%%       confirmation from remote node/cluster
%% mountpoint: The topic mount point for messages sent to remote node/cluster
%%      `undefined', `<<>>' or `""' to disable
%% forwards: Local topics to subscribe.
%% replayq.batch_bytes_limit: Max number of bytes to collect in a batch for each
%%      send call towards emqx_bridge_connect
%% replayq.batch_count_limit: Max number of messages to collect in a batch for
%%      each send call towards emqx_bridge_connect
%% replayq.dir: Directory where replayq should persist messages
%% replayq.seg_bytes: Size in bytes for each replayq segment file
%%
%% Find more connection specific configs in the callback modules
%% of emqx_bridge_connect behaviour.
start_link(Opts) when is_list(Opts) ->
    start_link(maps:from_list(Opts));
start_link(Opts) ->
    case maps:get(name, Opts, undefined) of
        undefined ->
            gen_statem:start_link(?MODULE, Opts, []);
        Name ->
            Name1 = name(Name),
            gen_statem:start_link({local, Name1}, ?MODULE, Opts#{name => Name1}, [])
    end.

ensure_started(Name) ->
    gen_statem:call(name(Name), ensure_started).

%% @doc Manually stop bridge worker. State idempotency ensured.
ensure_stopped(Name) ->
    gen_statem:call(name(Name), ensure_stopped, 5000).

stop(Pid) -> gen_statem:stop(Pid).

status(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, status);
status(Name) ->
    gen_statem:call(name(Name), status).

ping(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, ping);
ping(Name) ->
    gen_statem:call(name(Name), ping).

%% @doc Return all forwards (local subscriptions).
-spec get_forwards(id()) -> [topic()].
get_forwards(Name) -> gen_statem:call(name(Name), get_forwards, timer:seconds(1000)).

%% @doc Return all subscriptions (subscription over mqtt connection to remote broker).
-spec get_subscriptions(id()) -> [{emqx_topic:topic(), qos()}].
get_subscriptions(Name) -> gen_statem:call(name(Name), get_subscriptions).

callback_mode() -> [state_functions].

%% @doc Config should be a map().
init(#{name := Name} = ConnectOpts) ->
    ?LOG(info, "starting bridge worker for ~p", [Name]),
    erlang:process_flag(trap_exit, true),
    ConnectModule = conn_type(maps:get(conn_type, ConnectOpts)),
    Queue = open_replayq(Name, maps:get(replayq, ConnectOpts, #{})),
    State = init_state(ConnectOpts),
    self() ! idle,
    {ok, idle, State#{
        connect_module => ConnectModule,
        connect_opts => pre_process_opts(ConnectOpts),
        replayq => Queue
    }}.

init_state(Opts) ->
    IfRecordMetrics = maps:get(if_record_metrics, Opts, true),
    ReconnDelayMs = maps:get(reconnect_interval, Opts, ?DEFAULT_RECONNECT_DELAY_MS),
    StartType = maps:get(start_type, Opts, manual),
    Mountpoint = maps:get(forward_mountpoint, Opts, undefined),
    MaxInflightSize = maps:get(max_inflight, Opts, ?DEFAULT_BATCH_SIZE),
    BatchSize = maps:get(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
    Name = maps:get(name, Opts, undefined),
    #{start_type => StartType,
      reconnect_interval => ReconnDelayMs,
      batch_size => BatchSize,
      mountpoint => format_mountpoint(Mountpoint),
      inflight => [],
      max_inflight => MaxInflightSize,
      connection => undefined,
      if_record_metrics => IfRecordMetrics,
      name => Name}.

open_replayq(Name, QCfg) ->
    Dir = maps:get(dir, QCfg, undefined),
    SegBytes = maps:get(seg_bytes, QCfg, ?DEFAULT_SEG_BYTES),
    MaxTotalSize = maps:get(max_total_size, QCfg, ?DEFAULT_MAX_TOTAL_SIZE),
    QueueConfig = case Dir =:= undefined orelse Dir =:= "" of
        true -> #{mem_only => true};
        false -> #{dir => filename:join([Dir, node(), Name]),
                   seg_bytes => SegBytes, max_total_size => MaxTotalSize}
    end,
    replayq:open(QueueConfig#{sizer => fun emqx_bridge_msg:estimate_size/1,
                              marshaller => fun ?MODULE:msg_marshaller/1}).

pre_process_opts(#{subscriptions := InConf, forwards := OutConf} = ConnectOpts) ->
    ConnectOpts#{subscriptions => pre_process_in_out(InConf),
                 forwards => pre_process_in_out(OutConf)}.

pre_process_in_out(undefined) -> undefined;
pre_process_in_out(Conf) when is_map(Conf) ->
    Conf1 = pre_process_conf(local_topic, Conf),
    Conf2 = pre_process_conf(remote_topic, Conf1),
    Conf3 = pre_process_conf(payload, Conf2),
    Conf4 = pre_process_conf(qos, Conf3),
    pre_process_conf(retain, Conf4).

pre_process_conf(Key, Conf) ->
    case maps:find(Key, Conf) of
        error -> Conf;
        {ok, Val} when is_binary(Val) ->
            Conf#{Key => emqx_plugin_libs_rule:preproc_tmpl(Val)};
        {ok, Val} ->
            Conf#{Key => Val}
    end.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _StateName, #{replayq := Q} = State) ->
    _ = disconnect(State),
    _ = replayq:close(Q),
    ok.

%% ensure_started will be deprecated in the future
idle({call, From}, ensure_started, State) ->
    case do_connect(State) of
        {ok, State1} ->
            {next_state, connected, State1, [{reply, From, ok}, {state_timeout, 0, connected}]};
        {error, Reason, _State} ->
            {keep_state_and_data, [{reply, From, {error, Reason}}]}
    end;
%% @doc Standing by for manual start.
idle(info, idle, #{start_type := manual}) ->
    keep_state_and_data;
%% @doc Standing by for auto start.
idle(info, idle, #{start_type := auto} = State) ->
    connecting(State);
idle(state_timeout, reconnect, State) ->
    connecting(State);

idle(Type, Content, State) ->
    common(idle, Type, Content, State).

connecting(#{reconnect_interval := ReconnectDelayMs} = State) ->
    case do_connect(State) of
        {ok, State1} ->
            {next_state, connected, State1, {state_timeout, 0, connected}};
        _ ->
            {keep_state_and_data, {state_timeout, ReconnectDelayMs, reconnect}}
    end.

connected(state_timeout, connected, #{inflight := Inflight} = State) ->
    case retry_inflight(State#{inflight := []}, Inflight) of
        {ok, NewState} ->
            {keep_state, NewState, {next_event, internal, maybe_send}};
        {error, NewState} ->
            {keep_state, NewState}
    end;
connected(internal, maybe_send, State) ->
    {_, NewState} = pop_and_send(State),
    {keep_state, NewState};

connected(info, {disconnected, Conn, Reason},
          #{connection := Connection, name := Name, reconnect_interval := ReconnectDelayMs} = State) ->
    ?tp(info, disconnected, #{name => Name, reason => Reason}),
    case Conn =:= maps:get(client_pid, Connection, undefined)  of
        true ->
            {next_state, idle, State#{connection => undefined}, {state_timeout, ReconnectDelayMs, reconnect}};
        false ->
            keep_state_and_data
    end;
connected(info, {batch_ack, Ref}, State) ->
    NewState = handle_batch_ack(State, Ref),
    {keep_state, NewState, {next_event, internal, maybe_send}};
connected(Type, Content, State) ->
    common(connected, Type, Content, State).

%% Common handlers
common(StateName, {call, From}, status, _State) ->
    {keep_state_and_data, [{reply, From, StateName}]};
common(_StateName, {call, From}, ping, #{connection := Conn,
                                         connect_module := ConnectModule} =_State) ->
    Reply = ConnectModule:ping(Conn),
    {keep_state_and_data, [{reply, From, Reply}]};
common(_StateName, {call, From}, ensure_stopped, #{connection := undefined} = _State) ->
    {keep_state_and_data, [{reply, From, ok}]};
common(_StateName, {call, From}, ensure_stopped, #{connection := Conn,
                                                   connect_module := ConnectModule} = State) ->
    Reply = ConnectModule:stop(Conn),
    {next_state, idle, State#{connection => undefined}, [{reply, From, Reply}]};
common(_StateName, {call, From}, get_forwards, #{connect_opts := #{forwards := Forwards}}) ->
    {keep_state_and_data, [{reply, From, Forwards}]};
common(_StateName, {call, From}, get_subscriptions, #{connection := Connection}) ->
    {keep_state_and_data, [{reply, From, maps:get(subscriptions, Connection, #{})}]};
common(_StateName, info, {deliver, _, Msg},
       State = #{replayq := Q, if_record_metrics := IfRecordMetric}) ->
    Msgs = collect([Msg]),
    bridges_metrics_inc(IfRecordMetric,
                        'bridge.mqtt.message_received',
                        length(Msgs)
                       ),
    NewQ = replayq:append(Q, Msgs),
    {keep_state, State#{replayq => NewQ}, {next_event, internal, maybe_send}};
common(_StateName, info, {'EXIT', _, _}, State) ->
    {keep_state, State};
common(StateName, Type, Content, #{name := Name} = State) ->
    ?LOG(notice, "Bridge ~p discarded ~p type event at state ~p:~p",
          [Name, Type, StateName, Content]),
    {keep_state, State}.

do_connect(#{connect_module := ConnectModule,
             connect_opts := ConnectOpts = #{forwards := Forwards},
             inflight := Inflight,
             name := Name} = State) ->
    case Forwards of
        undefined -> ok;
        #{subscribe_local_topic := Topic} -> subscribe_local_topic(Topic, Name)
    end,
    case ConnectModule:start(ConnectOpts) of
        {ok, Conn} ->
            ?tp(info, connected, #{name => Name, inflight => length(Inflight)}),
            {ok, State#{connection => Conn}};
        {error, Reason} ->
            ConnectOpts1 = obfuscate(ConnectOpts),
            ?LOG(error, "Failed to connect with module=~p\n"
                 "config=~p\nreason:~p", [ConnectModule, ConnectOpts1, Reason]),
            {error, Reason, State}
    end.

collect(Acc) ->
    receive
        {deliver, _, Msg} ->
            collect([Msg | Acc])
    after
        0 ->
            lists:reverse(Acc)
    end.

%% Retry all inflight (previously sent but not acked) batches.
retry_inflight(State, []) -> {ok, State};
retry_inflight(State, [#{q_ack_ref := QAckRef, batch := Batch} | Rest] = OldInf) ->
    case do_send(State, QAckRef, Batch) of
        {ok, State1} ->
            retry_inflight(State1, Rest);
        {error, #{inflight := NewInf} = State1} ->
            {error, State1#{inflight := NewInf ++ OldInf}}
    end.

pop_and_send(#{inflight := Inflight, max_inflight := Max} = State) ->
    pop_and_send_loop(State, Max - length(Inflight)).

pop_and_send_loop(State, 0) ->
    ?tp(debug, inflight_full, #{}),
    {ok, State};
pop_and_send_loop(#{replayq := Q, connect_module := Module} = State, N) ->
    case replayq:is_empty(Q) of
        true ->
            ?tp(debug, replayq_drained, #{}),
            {ok, State};
        false ->
            BatchSize = case Module of
                emqx_bridge_rpc -> maps:get(batch_size, State);
                _ -> 1
            end,
            Opts = #{count_limit => BatchSize, bytes_limit => 999999999},
            {Q1, QAckRef, Batch} = replayq:pop(Q, Opts),
            case do_send(State#{replayq := Q1}, QAckRef, Batch) of
                {ok, NewState} -> pop_and_send_loop(NewState, N - 1);
                {error, NewState} -> {error, NewState}
            end
    end.

%% Assert non-empty batch because we have a is_empty check earlier.
do_send(#{connect_opts := #{forwards := undefined}}, _QAckRef, Batch) ->
    ?LOG(error, "cannot forward messages to remote broker as 'bridge.mqtt.<name>.in' not configured, msg: ~p", [Batch]);
do_send(#{inflight := Inflight,
          connect_module := Module,
          connection := Connection,
          mountpoint := Mountpoint,
          connect_opts := #{forwards := Forwards},
          if_record_metrics := IfRecordMetrics} = State, QAckRef, [_ | _] = Batch) ->
    Vars = emqx_bridge_msg:make_pub_vars(Mountpoint, Forwards),
    ExportMsg = fun(Message) ->
                    bridges_metrics_inc(IfRecordMetrics, 'bridge.mqtt.message_sent'),
                    emqx_bridge_msg:to_remote_msg(Module, Message, Vars)
                end,
    ?LOG(debug, "publish to remote broker, msg: ~p, vars: ~p", [Batch, Vars]),
    case Module:send(Connection, [ExportMsg(M) || M <- Batch]) of
        {ok, Refs} ->
            {ok, State#{inflight := Inflight ++ [#{q_ack_ref => QAckRef,
                                                   send_ack_ref => map_set(Refs),
                                                   batch => Batch}]}};
        {error, Reason} ->
            ?LOG(info, "mqtt_bridge_produce_failed ~p", [Reason]),
            {error, State}
    end.

%% map as set, ack-reference -> 1
map_set(Ref) when is_reference(Ref) ->
    %% QoS-0 or RPC call returns a reference
    map_set([Ref]);
map_set(List) ->
    map_set(List, #{}).

map_set([], Set) -> Set;
map_set([H | T], Set) -> map_set(T, Set#{H => 1}).

handle_batch_ack(#{inflight := Inflight0, replayq := Q} = State, Ref) ->
    Inflight1 = do_ack(Inflight0, Ref),
    Inflight = drop_acked_batches(Q, Inflight1),
    State#{inflight := Inflight}.

do_ack([], Ref) ->
    ?LOG(debug, "stale_batch_ack_reference ~p", [Ref]),
    [];
do_ack([#{send_ack_ref := Refs} = First | Rest], Ref) ->
    case maps:is_key(Ref, Refs) of
        true ->
            NewRefs = maps:without([Ref], Refs),
            [First#{send_ack_ref := NewRefs} | Rest];
        false ->
            [First | do_ack(Rest, Ref)]
    end.

%% Drop the consecutive header of the inflight list having empty send_ack_ref
drop_acked_batches(_Q, []) ->
    ?tp(debug, inflight_drained, #{}),
    [];
drop_acked_batches(Q, [#{send_ack_ref := Refs,
                         q_ack_ref := QAckRef} | Rest] = All) ->
    case maps:size(Refs) of
        0 ->
            %% all messages are acked by bridge target
            %% now it's safe to ack replayq (delete from disk)
            ok = replayq:ack(Q, QAckRef),
            %% continue to check more sent batches
            drop_acked_batches(Q, Rest);
        _ ->
            %% the head (oldest) inflight batch is not acked, keep waiting
            All
    end.

subscribe_local_topic(Topic, Name) ->
    do_subscribe(Topic, Name).

topic(T) -> iolist_to_binary(T).

validate(RawTopic) ->
    Topic = topic(RawTopic),
    try emqx_topic:validate(Topic) of
        _Success -> Topic
    catch
        error:Reason ->
            error({bad_topic, Topic, Reason})
    end.

do_subscribe(RawTopic, Name) ->
    TopicFilter = validate(RawTopic),
    {Topic, SubOpts} = emqx_topic:parse(TopicFilter, #{qos => ?QOS_2}),
    emqx_broker:subscribe(Topic, Name, SubOpts).

disconnect(#{connection := Conn,
             connect_module := Module
            } = State) when Conn =/= undefined ->
    Module:stop(Conn),
    State#{connection => undefined};
disconnect(State) ->
        State.

%% Called only when replayq needs to dump it to disk.
msg_marshaller(Bin) when is_binary(Bin) -> emqx_bridge_msg:from_binary(Bin);
msg_marshaller(Msg) -> emqx_bridge_msg:to_binary(Msg).

format_mountpoint(undefined) ->
    undefined;
format_mountpoint(Prefix) ->
    binary:replace(iolist_to_binary(Prefix), <<"${node}">>, atom_to_binary(node(), utf8)).

name(Id) -> list_to_atom(str(Id)).

register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1,
                  ['bridge.mqtt.message_sent',
                   'bridge.mqtt.message_received'
                  ]).

bridges_metrics_inc(true, Metric) ->
    emqx_metrics:inc(Metric);
bridges_metrics_inc(_IsRecordMetric, _Metric) ->
    ok.

bridges_metrics_inc(true, Metric, Value) ->
    emqx_metrics:inc(Metric, Value);
bridges_metrics_inc(_IsRecordMetric, _Metric, _Value) ->
    ok.

obfuscate(Map) ->
    maps:fold(fun(K, V, Acc) ->
                      case is_sensitive(K) of
                          true -> [{K, '***'} | Acc];
                          false -> [{K, V} | Acc]
                      end
              end, [], Map).

is_sensitive(password) -> true;
is_sensitive(_) -> false.

conn_type(rpc) ->
    emqx_bridge_rpc;
conn_type(mqtt) ->
    emqx_bridge_mqtt;
conn_type(Mod) when is_atom(Mod) ->
    Mod.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.