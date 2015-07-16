%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Supervisor for Machi FLU servers and their related support
%% servers.
%%
%% See {@link machi_flu_psup} for an illustration of the entire Machi
%% application process structure.

-module(machi_flu_sup).

-behaviour(supervisor).

-include("machi_verbose.hrl").

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-include_lib("pulse_otp/include/pulse_otp.hrl").
-endif.

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    erlang:display({flu_sup,self()}),
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Ps = get_initial_flus(),
    FLU_specs = [machi_flu_psup:make_package_spec(FluName, TcpPort,
                                                  DataDir, Props) ||
                    {FluName, TcpPort, DataDir, Props} <- Ps],

    {ok, {SupFlags, FLU_specs}}.

-ifdef(PULSE).
get_initial_flus() ->
    [].
-else.  % PULSE
get_initial_flus() ->
    application:get_env(machi, initial_flus, []).
-endif. % PULSE
