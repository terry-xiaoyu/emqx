%%--------------------------------------------------------------------
%% Copyright (c) 2017-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_post_upgrade).

%% PR#12781
-export([
    pr12781_init_db_metrics/1,
    pr12781_termiate_db_metrics/1
]).

%% PR#12765
-export([
    pr12765_update_stats_timer/1,
    pr12765_revert_stats_timer/1
]).

-include("logger.hrl").

%%------------------------------------------------------------------------------
%% Hot Upgrade Callback Functions.
%%------------------------------------------------------------------------------
pr12781_init_db_metrics(_FromVsn) ->
    MetricsWorker = emqx_ds_builtin_metrics:child_spec(),
    {ok, _} = supervisor:start_child(emqx_ds_builtin_sup, MetricsWorker),
    Key = {n, l, {emqx_ds_builtin_db_sup, '$1'}},
    [DB] = gproc:select({local, names}, [{{Key, '_', '_'}, [], ['$1']}]),
    ok = emqx_ds_builtin_metrics:init_for_db(DB).

pr12781_termiate_db_metrics(_ToVsn) ->
    #{id := Id} = emqx_ds_builtin_metrics:child_spec(),
    ok = ensure_child_deleted(emqx_ds_builtin_sup, Id).

pr12765_update_stats_timer(_FromVsn) ->
    emqx_stats:update_interval(broker_stats, fun emqx_broker_helper:stats_fun/0).

pr12765_revert_stats_timer(_ToVsn) ->
    emqx_stats:update_interval(broker_stats, fun emqx_broker:stats_fun/0).

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------
ensure_child_deleted(Sup, Id) ->
    case supervisor:terminate_child(Sup, Id) of
        ok -> supervisor:delete_child(Sup, Id);
        {error, not_found} -> ok;
        Err -> Err
    end.
