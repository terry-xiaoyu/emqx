%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_assembler_sup).

-export([start_link/0]).
-export([ensure_child/4]).

-behaviour(supervisor).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

ensure_child(Storage, Transfer, Size, Opts) ->
    Childspec = #{
        id => Transfer,
        start => {emqx_ft_assembler, start_link, [Storage, Transfer, Size, Opts]},
        restart => temporary
    },
    case supervisor:start_child(?MODULE, Childspec) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid}
    end.

init(_) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1000
    },
    {ok, {SupFlags, []}}.