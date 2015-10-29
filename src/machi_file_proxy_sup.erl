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

%% @doc This is the main supervisor for the file proxies.
-module(machi_file_proxy_sup).
-behaviour(supervisor).

%% public API
-export([
    child_spec/1,
    start_link/1,
    start_proxy/3
]).

%% supervisor callback
-export([
    init/1
]).

child_spec(FluName) ->
    Name = make_proxy_name(FluName),
    {Name,
     {?MODULE, start_link, [FluName]},
     permanent, 5000, supervisor, [?MODULE]}.

start_link(FluName) ->
    supervisor:start_link({local, make_proxy_name(FluName)}, ?MODULE, []).

start_proxy(FluName, DataDir, Filename) ->
    supervisor:start_child(make_proxy_name(FluName),
                           [FluName, Filename, DataDir]).

init([]) ->
    SupFlags = {simple_one_for_one, 1000, 10},
    ChildSpec = {unused, {machi_file_proxy, start_link, []}, 
                    temporary, 2000, worker, [machi_file_proxy]},
    {ok, {SupFlags, [ChildSpec]}}.

make_proxy_name(FluName) when is_atom(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_file_proxy_sup").
