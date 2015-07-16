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
%% Our parent supervisor, {@link machi_flu_sup}, is responsible for
%% managing FLUs as a single entity.  However, the actual
%% implementation of a FLU includes three major Erlang processes (not
%% including support/worker procs): the FLU itself, the FLU's
%% projection store, and the FLU's local chain manager.  This
%% supervisor is responsible for managing those three major services
%% as a single "package", to be started &amp; stopped together.
%%
%% The illustration below shows the OTP process supervision tree for
%% the Machi application.  Two FLUs are running, called `a' and `b'.
%% The chain is configured for a third FLU, `c', which is not running
%% at this time.
%%
%% <img src="/machi/{@docRoot}/images/supervisor-2flus.png"></img>
%%
%% <ul>
%% <li> The FLU process itself is named `a'.
%% </li>
%% <li> The projection store process is named `a_pstore'.
%% </li>
%% <li> The chain manager process is named `a_chmgr'.  The three
%%      linked subprocesses are long-lived {@link
%%      machi_proxy_flu1_client} processes for communicating to all
%%      chain participants' projection stores (including the local
%%      store `a_pstore').
%% </li>
%% <li> A fourth major process, `a_listener', which is responsible for
%%      listening on a TCP socket and creating new connections.
%%      Currently, each listener has two processes handling incoming
%%      requests, one from each chain manager proxy.
%% </li>
%% <li> Note that the sub-supervisor parent of `a' and `a_listener' does
%%      not have a registered name.
%% </li>
%% </ul>

-module(machi_flu_psup).

-behaviour(supervisor).

-include("machi_verbose.hrl").

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-include_lib("pulse_otp/include/pulse_otp.hrl").
-define(SHUTDOWN, infinity).
-else.
-define(SHUTDOWN, 5000).
-endif.

%% External API
-export([make_package_spec/4, start_flu_package/4, stop_flu_package/1]).
%% Internal API
-export([start_link/4,
         make_p_regname/1, make_mgr_supname/1, make_proj_supname/1]).

%% Supervisor callbacks
-export([init/1]).

make_package_spec(FluName, TcpPort, DataDir, Props) ->
    {FluName, {machi_flu_psup, start_link,
               [FluName, TcpPort, DataDir, Props]},
     permanent, ?SHUTDOWN, supervisor, []}.

start_flu_package(FluName, TcpPort, DataDir, Props) ->
    Spec = make_package_spec(FluName, TcpPort, DataDir, Props),
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
    Props = Props0 ++ [{projection_store_registered_name, ProjRegName},
                       {use_partition_simulator,false}],
    ProjSpec = {ProjRegName,
               {machi_projection_store, start_link,
                [ProjRegName, DataDir, FluName]},
               permanent, ?SHUTDOWN, worker, []},
    MgrSpec = {make_mgr_supname(FluName),
               {machi_chain_manager1, start_link,
                [FluName, [], Props]},
               permanent, ?SHUTDOWN, worker, []},
    FluSpec = {FluName,
               {machi_flu1, start_link,
                [ [{FluName, TcpPort, DataDir}|Props] ]},
               permanent, ?SHUTDOWN, worker, []},
    {ok, {SupFlags, [ProjSpec, MgrSpec, FluSpec]}}.

make_p_regname(FluName) when is_atom(FluName) ->
    list_to_atom("flusup_" ++ atom_to_list(FluName)).

make_mgr_supname(MgrName) when is_atom(MgrName) ->
    machi_chain_manager1:make_chmgr_regname(MgrName).

make_proj_supname(ProjName) when is_atom(ProjName) ->
    list_to_atom(atom_to_list(ProjName) ++ "_pstore").
