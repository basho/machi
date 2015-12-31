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

%% @doc A supervisor to hold dynamic processes inside single
%% FLU service, ranch listener and append server.

%% TODO: This supervisor is maybe useless. First introduced for
%% workaround to start listener dynamically in flu1 initialization
%% phase.  Because `machi_flu_psup' is being blocked in flu1
%% initialization time, adding a child to the supervisor leads to
%% deadlock.  If initialization can be done only by static arguments,
%% then this supervisor should be removed and added as a direct child
%% of `machi_flu_psup'.

-module(machi_flu1_subsup).
-behaviour(supervisor).

%% public API
-export([start_link/1,
         start_append_server/4,
         stop_append_server/1,
         start_listener/7,
         stop_listener/1,
         subsup_name/1,
         listener_name/1]).

%% supervisor callback
-export([init/1]).

-include("machi_projection.hrl").

-define(SHUTDOWN, 5000).
-define(BACKLOG, 8192).

-spec start_link(pv1_server()) -> {ok, pid()}.
start_link(FluName) ->
    supervisor:start_link({local, subsup_name(FluName)}, ?MODULE, []).

-spec start_append_server(pv1_server(), boolean(), boolean(),
                          undefined | machi_dt:epoch_id()) ->
                                 {ok, pid()}.
start_append_server(FluName, Witness_p, Wedged_p, EpochId) ->
    supervisor:start_child(subsup_name(FluName),
                           append_server_spec(FluName, Witness_p, Wedged_p, EpochId)).

-spec stop_append_server(pv1_server()) -> ok.
stop_append_server(FluName) ->
    SubSup = listener_name(FluName),
    ok = supervisor:terminate_child(SubSup, FluName),
    ok = supervisor:delete_child(SubSup, FluName).

-spec start_listener(pv1_server(), inet:port_number(), boolean(),
                     string(), ets:tab(), atom() | pid(),
                     proplists:proplist()) -> {ok, pid()}.
start_listener(FluName, TcpPort, Witness, DataDir, EpochTab, ProjStore,
               Props) ->
    supervisor:start_child(subsup_name(FluName),
                           listener_spec(FluName, TcpPort, Witness, DataDir,
                                         EpochTab, ProjStore, Props)).

-spec stop_listener(pv1_server()) -> ok.
stop_listener(FluName) ->
    SupName = subsup_name(FluName),
    ListenerName = listener_name(FluName),
    ok = supervisor:terminate_child(SupName, ListenerName),
    ok = supervisor:delete_child(SupName, ListenerName).

-spec subsup_name(pv1_server()) -> atom().
subsup_name(FluName) when is_atom(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_flu1_subsup").

-spec listener_name(pv1_server()) -> atom().
listener_name(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_listener").

%% Supervisor callback

init([]) ->
    SupFlags = {one_for_all, 1000, 10},
    {ok, {SupFlags, []}}.

%% private

-spec listener_spec(pv1_server(), inet:port_number(), boolean(),
                    string(), ets:tab(), atom() | pid(),
                    proplists:proplist()) -> supervisor:child_spec().
listener_spec(FluName, TcpPort, Witness, DataDir, EpochTab, ProjStore, Props) ->
    ListenerName = listener_name(FluName),
    NbAcceptors = 10,
    TcpOpts = [{port, TcpPort}, {backlog, ?BACKLOG}],
    NetServerOpts = [FluName, Witness, DataDir, EpochTab, ProjStore, Props],
    ranch:child_spec(ListenerName, NbAcceptors,
                     ranch_tcp, TcpOpts,
                     machi_flu1_net_server, NetServerOpts).

-spec append_server_spec(pv1_server(), boolean(), boolean(),
                         undefined | machi_dt:epoch_id()) -> supervisor:child_spec().
append_server_spec(FluName, Witness_p, Wedged_p, EpochId) ->
    {FluName, {machi_flu1_append_server, start_link,
               [FluName, Witness_p, Wedged_p, EpochId]},
     permanent, ?SHUTDOWN, worker, [machi_flu1_append_server]}.
