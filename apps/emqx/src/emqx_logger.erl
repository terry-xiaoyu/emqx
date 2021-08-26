%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_logger).

-compile({no_auto_import, [error/1]}).

-behaviour(gen_server).
-behaviour(emqx_config_handler).

%% gen_server callbacks
-export([ start_link/0
        , init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% Logs
-export([ debug/1
        , debug/2
        , debug/3
        , info/1
        , info/2
        , info/3
        , warning/1
        , warning/2
        , warning/3
        , error/1
        , error/2
        , error/3
        , critical/1
        , critical/2
        , critical/3
        ]).

%% Configs
-export([ set_metadata_peername/1
        , set_metadata_clientid/1
        , set_proc_metadata/1
        , set_primary_log_level/1
        , set_log_handler_level/2
        , set_log_level/1
        , set_all_log_handlers_level/1
        ]).

-export([ get_primary_log_level/0
        , tune_primary_log_level/0
        , get_log_handlers/0
        , get_log_handlers/1
        , get_log_handler/1
        ]).

-export([ start_log_handler/1
        , stop_log_handler/1
        ]).

-export([post_config_update/4]).

-type(peername_str() :: list()).
-type(logger_dst() :: file:filename() | console | unknown).
-type(logger_handler_info() :: #{
        id := logger:handler_id(),
        level := logger:level(),
        dst := logger_dst(),
        status := started | stopped
      }).

-define(stopped_handlers, {?MODULE, stopped_handlers}).
-define(CONF_PATH, [log]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    ok = emqx_config_handler:add_handler(?CONF_PATH, ?MODULE),
    {ok, #{}}.

handle_call({update_config, AppEnvs}, _From, State) ->
    OldEnvs = application:get_env(kernel, logger, []),
    NewEnvs = proplists:get_value(logger, proplists:get_value(kernel, AppEnvs, []), []),
    ok = application:set_env(kernel, logger, NewEnvs),
    _ = [logger:remove_handler(HandlerId) || {handler, HandlerId, _Mod, _Conf} <- OldEnvs],
    _ = [logger:add_handler(HandlerId, Mod, Conf) || {handler, HandlerId, Mod, Conf} <- NewEnvs],
    ok = tune_primary_log_level(),
    {reply, ok, State};

handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = emqx_config_handler:remove_handler(?CONF_PATH),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% emqx_config_handler callbacks
%%--------------------------------------------------------------------
post_config_update(_Req, _NewConf, _OldConf, AppEnvs) ->
    gen_server:call(?MODULE, {update_config, AppEnvs}, 5000).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
-spec(debug(unicode:chardata()) -> ok).
debug(Msg) ->
    logger:debug(Msg).

-spec(debug(io:format(), [term()]) -> ok).
debug(Format, Args) ->
    logger:debug(Format, Args).

-spec(debug(logger:metadata(), io:format(), [term()]) -> ok).
debug(Metadata, Format, Args) when is_map(Metadata) ->
    logger:debug(Format, Args, Metadata).


-spec(info(unicode:chardata()) -> ok).
info(Msg) ->
    logger:info(Msg).

-spec(info(io:format(), [term()]) -> ok).
info(Format, Args) ->
    logger:info(Format, Args).

-spec(info(logger:metadata(), io:format(), [term()]) -> ok).
info(Metadata, Format, Args) when is_map(Metadata) ->
    logger:info(Format, Args, Metadata).


-spec(warning(unicode:chardata()) -> ok).
warning(Msg) ->
    logger:warning(Msg).

-spec(warning(io:format(), [term()]) -> ok).
warning(Format, Args) ->
    logger:warning(Format, Args).

-spec(warning(logger:metadata(), io:format(), [term()]) -> ok).
warning(Metadata, Format, Args) when is_map(Metadata) ->
    logger:warning(Format, Args, Metadata).


-spec(error(unicode:chardata()) -> ok).
error(Msg) ->
    logger:error(Msg).
-spec(error(io:format(), [term()]) -> ok).
error(Format, Args) ->
    logger:error(Format, Args).
-spec(error(logger:metadata(), io:format(), [term()]) -> ok).
error(Metadata, Format, Args) when is_map(Metadata) ->
    logger:error(Format, Args, Metadata).


-spec(critical(unicode:chardata()) -> ok).
critical(Msg) ->
    logger:critical(Msg).

-spec(critical(io:format(), [term()]) -> ok).
critical(Format, Args) ->
    logger:critical(Format, Args).

-spec(critical(logger:metadata(), io:format(), [term()]) -> ok).
critical(Metadata, Format, Args) when is_map(Metadata) ->
    logger:critical(Format, Args, Metadata).

-spec(set_metadata_clientid(emqx_types:clientid()) -> ok).
set_metadata_clientid(<<>>) ->
    ok;
set_metadata_clientid(ClientId) ->
    try
        %% try put string format client-id metadata so
        %% so the log is not like <<"...">>
        Id = unicode:characters_to_list(ClientId, utf8),
        set_proc_metadata(#{clientid => Id})
    catch
        _: _->
            ok
    end.

-spec(set_metadata_peername(peername_str()) -> ok).
set_metadata_peername(Peername) ->
    set_proc_metadata(#{peername => Peername}).

-spec(set_proc_metadata(logger:metadata()) -> ok).
set_proc_metadata(Meta) ->
    logger:update_process_metadata(Meta).

-spec(get_primary_log_level() -> logger:level()).
get_primary_log_level() ->
    #{level := Level} = logger:get_primary_config(),
    Level.

-spec tune_primary_log_level() -> ok.
tune_primary_log_level() ->
    LowestLevel = lists:foldl(fun(#{level := Level}, OldLevel) ->
            case logger:compare_levels(Level, OldLevel) of
                lt -> Level;
                _ -> OldLevel
            end
        end, get_primary_log_level(), get_log_handlers()),
    set_primary_log_level(LowestLevel).

-spec(set_primary_log_level(logger:level()) -> ok | {error, term()}).
set_primary_log_level(Level) ->
    logger:set_primary_config(level, Level).

-spec(get_log_handlers() -> [logger_handler_info()]).
get_log_handlers() ->
    get_log_handlers(started) ++ get_log_handlers(stopped).

-spec(get_log_handlers(started | stopped) -> [logger_handler_info()]).
get_log_handlers(started) ->
    [log_hanlder_info(Conf, started) || Conf <- logger:get_handler_config()];
get_log_handlers(stopped) ->
    [log_hanlder_info(Conf, stopped) || Conf <- list_stopped_handler_config()].

-spec(get_log_handler(logger:handler_id()) -> logger_handler_info()).
get_log_handler(HandlerId) ->
    case logger:get_handler_config(HandlerId) of
        {ok, Conf} ->
            log_hanlder_info(Conf, started);
        {error, _} ->
            case read_stopped_handler_config(HandlerId) of
                error -> {error, {not_found, HandlerId}};
                {ok, Conf} -> log_hanlder_info(Conf, stopped)
            end
    end.

-spec(start_log_handler(logger:handler_id()) -> ok | {error, term()}).
start_log_handler(HandlerId) ->
    case lists:member(HandlerId, logger:get_handler_ids()) of
        true -> ok;
        false ->
            case read_stopped_handler_config(HandlerId) of
                error -> {error, {not_found, HandlerId}};
                {ok, Conf = #{module := Mod}} ->
                    case logger:add_handler(HandlerId, Mod, Conf) of
                        ok -> remove_stopped_handler_config(HandlerId);
                        {error, _} = Error -> Error
                    end
            end
    end.

-spec(stop_log_handler(logger:handler_id()) -> ok | {error, term()}).
stop_log_handler(HandlerId) ->
    case logger:get_handler_config(HandlerId) of
        {ok, Conf} ->
            case logger:remove_handler(HandlerId) of
                ok -> save_stopped_handler_config(HandlerId, Conf);
                Error -> Error
            end;
        {error, _} ->
            {error, {not_started, HandlerId}}
    end.

-spec(set_log_handler_level(logger:handler_id(), logger:level()) -> ok | {error, term()}).
set_log_handler_level(HandlerId, Level) ->
    case logger:set_handler_config(HandlerId, level, Level) of
        ok -> ok;
        {error, _} ->
            case read_stopped_handler_config(HandlerId) of
                error -> {error, {not_found, HandlerId}};
                {ok, Conf} ->
                    save_stopped_handler_config(HandlerId, Conf#{level => Level})
            end
    end.

%% @doc Set both the primary and all handlers level in one command
-spec(set_log_level(logger:handler_id()) -> ok | {error, term()}).
set_log_level(Level) ->
    case set_primary_log_level(Level) of
        ok -> set_all_log_handlers_level(Level);
        {error, Error} -> {error, {primary_logger_level, Error}}
    end.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

log_hanlder_info(#{id := Id, level := Level, module := logger_std_h,
                   config := #{type := Type}}, Status) when
                Type =:= standard_io;
                Type =:= standard_error ->
    #{id => Id, level => Level, dst => console, status => Status};
log_hanlder_info(#{id := Id, level := Level, module := logger_std_h,
                   config := Config = #{type := file}}, Status) ->
    #{id => Id, level => Level, status => Status,
      dst => maps:get(file, Config, atom_to_list(Id))};

log_hanlder_info(#{id := Id, level := Level, module := logger_disk_log_h,
                   config := #{file := Filename}}, Status) ->
    #{id => Id, level => Level, dst => Filename, status => Status};
log_hanlder_info(#{id := Id, level := Level, module := _OtherModule}, Status) ->
    #{id => Id, level => Level, dst => unknown, status => Status}.

%% set level for all log handlers in one command
set_all_log_handlers_level(Level) ->
    set_all_log_handlers_level(get_log_handlers(), Level, []).

set_all_log_handlers_level([#{id := ID, level := Level} | List], NewLevel, ChangeHistory) ->
    case set_log_handler_level(ID, NewLevel) of
        ok -> set_all_log_handlers_level(List, NewLevel, [{ID, Level} | ChangeHistory]);
        {error, Error} ->
            rollback(ChangeHistory),
            {error, {handlers_logger_level, {ID, Error}}}
    end;
set_all_log_handlers_level([], _NewLevel, _NewHanlder) ->
    ok.

rollback([{ID, Level} | List]) ->
    _ = set_log_handler_level(ID, Level),
    rollback(List);
rollback([]) -> ok.

save_stopped_handler_config(HandlerId, Config) ->
    case persistent_term:get(?stopped_handlers, undefined) of
        undefined ->
            persistent_term:put(?stopped_handlers, #{HandlerId => Config});
        ConfList ->
            persistent_term:put(?stopped_handlers, ConfList#{HandlerId => Config})
    end.
read_stopped_handler_config(HandlerId) ->
    case persistent_term:get(?stopped_handlers, undefined) of
        undefined -> error;
        ConfList -> maps:find(HandlerId, ConfList)
    end.
remove_stopped_handler_config(HandlerId) ->
    case persistent_term:get(?stopped_handlers, undefined) of
        undefined -> ok;
        ConfList ->
            case maps:find(HandlerId, ConfList) of
                error -> ok;
                {ok, _} ->
                    persistent_term:put(?stopped_handlers, maps:remove(HandlerId, ConfList))
            end
    end.
list_stopped_handler_config() ->
    case persistent_term:get(?stopped_handlers, undefined) of
        undefined -> [];
        ConfList -> maps:values(ConfList)
    end.
