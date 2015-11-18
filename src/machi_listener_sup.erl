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

%% @doc This is the supervisor to hold ranch listener for sigle FLU,
%% holds at most one child worker.

%% TODO: This supervisor is maybe useless. First introduced by workaround
%% to start listener  dynamically in flu1 initialization time.
%% Because psup is blocked in flu1 initialization time, adding a child
%% to psup leads to deadlock.
%% By the result of refactoring process, if initialization can be done
%% only by static arguments, then this supervisor should be removed
%% and add listener as a direct child of psup.

-module(machi_listener_sup).
-behaviour(supervisor).

%% public API
-export([start_link/1,
         start_listener/6,
         stop_listener/1,
         make_listener_sup_name/1,
         make_listener_name/1]).

%% supervisor callback
-export([init/1]).

-define(BACKLOG, 8192).

start_link(FluName) ->
    supervisor:start_link({local, make_listener_sup_name(FluName)}, ?MODULE, []).

start_listener(FluName, TcpPort, Witness, DataDir, EpochTab, ProjStore) ->
    supervisor:start_child(make_listener_sup_name(FluName),
                           child_spec(FluName, TcpPort, Witness, DataDir,
                                      EpochTab, ProjStore)).

stop_listener(FluName) ->
    SupName = make_listener_sup_name(FluName),
    ListenerName = make_listener_name(FluName),
    ok = supervisor:terminate_child(SupName, ListenerName),
    ok = supervisor:delete_child(SupName, ListenerName).

make_listener_sup_name(FluName) when is_atom(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_listener_sup").

make_listener_name(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_listener").

init([]) ->
    SupFlags = {one_for_one, 1000, 10},
    {ok, {SupFlags, []}}.

child_spec(FluName, TcpPort, Witness, DataDir, EpochTab, ProjStore) ->
    ListenerName = make_listener_name(FluName),
    NbAcceptors = 100,
    TcpOpts = [{port, TcpPort}, {backlog, ?BACKLOG}],
    ProtoOpts = [FluName, Witness, DataDir, EpochTab, ProjStore],
    ranch:child_spec(ListenerName, NbAcceptors,
                     ranch_tcp, TcpOpts,
                     machi_pb_protocol, ProtoOpts).
