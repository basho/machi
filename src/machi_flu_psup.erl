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

-module(machi_flu_psup).

-behaviour(supervisor).

%% External API
-export([start_flu_package/4, stop_flu_package/1]).
%% Internal API
-export([start_link/4]).

%% Supervisor callbacks
-export([init/1]).

start_flu_package(FluName, TcpPort, DataDir, Props) ->
    Spec = {FluName, {machi_flu_psup, start_link,
                      [FluName, TcpPort, DataDir, Props]},
            permanent, 5000, supervisor, []},
    {ok, _SupPid} = supervisor:start_child(machi_flu_sup, Spec).

stop_flu_package(FluName) ->
    case supervisor:terminate_child(machi_flu_sup, FluName) of
        ok ->
            ok = supervisor:delete_child(machi_flu_sup, FluName);
        Else ->
            Else
    end.

start_link(FluName, TcpPort, DataDir, Props) ->
    supervisor:start_link({local, make_p_regname(FluName)}, ?MODULE,
                          [FluName, TcpPort, DataDir, Props]).

init([FluName, TcpPort, DataDir, Props0]) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    ProjRegName = make_proj_supname(FluName),
    Props = [{projection_store_registered_name, ProjRegName},
             {use_partition_simulator,false}|Props0],
    ProjSpec = {ProjRegName,
               {machi_projection_store, start_link,
                [ProjRegName, DataDir, zarfus_todo]},
               permanent, 5000, worker, []},
    MgrSpec = {make_mgr_supname(FluName),
               {machi_chain_manager1, start_link,
                [FluName, [], Props]},
               permanent, 5000, worker, []},
    FluSpec = {FluName,
               {machi_flu1, start_link,
                [ [{FluName, TcpPort, DataDir}|Props] ]},
               permanent, 5000, worker, []},
    {ok, {SupFlags, [ProjSpec, MgrSpec, FluSpec]}}.

make_p_regname(FluName) when is_atom(FluName) ->
    list_to_atom("flusup_" ++ atom_to_list(FluName)).

make_mgr_supname(MgrName) when is_atom(MgrName) ->
    list_to_atom(atom_to_list(MgrName) ++ "_s").

make_proj_supname(ProjName) when is_atom(ProjName) ->
    list_to_atom(atom_to_list(ProjName) ++ "_pstore").
