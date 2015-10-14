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

%% @doc This is the supervisor for the collection of metadata
%% managers. It's started out of `machi_flu_psup'. It reads an
%% application environment variable named `metadata_manager_count'
%% with a default of 10 if it is not set.

-module(machi_flu_metadata_mgr_sup).
-behaviour(supervisor).

%% public API
-export([
    child_spec/3, 
    start_link/3
]).

%% supervisor callback
-export([init/1]).

child_spec(FluName, DataDir, N) ->
    {make_sup_name(FluName), 
     {?MODULE, start_link, [FluName, DataDir, N]}, 
     permanent, 5000, supervisor, [?MODULE]}.

start_link(FluName, DataDir, N) ->
    supervisor:start_link({local, make_sup_name(FluName)}, ?MODULE, [FluName, DataDir, N]).

init([FluName, DataDir, N]) ->
    Restart = one_for_one,
    MaxRestarts = 1000,
    SecondsBetween = 3600,
    SupFlags = {Restart, MaxRestarts, SecondsBetween},

    Children = 
        [ machi_flu_metadata_mgr:child_spec(FluName, C, DataDir, N) || C <- lists:seq(1,N) ],

    {ok, {SupFlags, Children}}.

make_sup_name(FluName) when is_atom(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_metadata_mgr_sup").

