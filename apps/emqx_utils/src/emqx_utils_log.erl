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

-module(emqx_utils_log).

-export([with_hint_msg/1]).

with_hint_msg(#{hint_msg := _} = Data) ->
    Data;
with_hint_msg(#{msg := Msg} = Data) ->
    case hint_msg(Msg) of
        undefined -> Data;
        HintMsg -> maps:put(hint_msg, HintMsg, Data)
    end;
with_hint_msg(Data) ->
    Data.

hint_msg("puback_packetId_not_found") ->
    <<
        "EMQX received PUBACK but packet id not found from the inflight queue. "
        "It could either be due to receiving duplicate PUBACK messages or an incorrect "
        "PacketID in the PUBACK message. "
    >>;
hint_msg(_) ->
    undefined.
