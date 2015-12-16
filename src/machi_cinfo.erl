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

%% @doc cluster_info callback module for machi specific information
%% gathering.

-module(machi_cinfo).

%% cluster_info callbacks
-export([register/0, cluster_info_init/0, cluster_info_generator_funs/0]).

%% for debug in interactive shell
-export([dump/0,
         public_projection/1, private_projection/1,
         chain_manager/1, fitness/1, flu1/1]).

-include("machi_projection.hrl").

-spec register() -> ok.
register() ->
    ok = cluster_info:register_app(?MODULE).

-spec cluster_info_init() -> ok.
cluster_info_init() ->
    ok.

-spec cluster_info_generator_funs() -> [{string(), fun((pid()) -> ok)}].
cluster_info_generator_funs() ->
    FluNames = [Name || {Name, _, _, _} <- supervisor:which_children(machi_flu_sup)],
    lists:flatten([generator_funs_package(Name) || Name <- FluNames]).

generator_funs_package(FluName) ->
    [{"Public projection of FLU " ++ atom_to_list(FluName),
      cinfo_wrapper(fun public_projection/1, FluName)},
     {"Private projection of FLU " ++ atom_to_list(FluName),
      cinfo_wrapper(fun private_projection/1, FluName)},
     {"Chain manager status of FLU " ++ atom_to_list(FluName),
      cinfo_wrapper(fun chain_manager/1, FluName)},
     {"Fitness server status of FLU " ++ atom_to_list(FluName),
      cinfo_wrapper(fun fitness/1, FluName)},
     {"FLU1 status of FLU " ++ atom_to_list(FluName),
      cinfo_wrapper(fun flu1/1, FluName)}].

dump() ->
    {{Y,M,D},{HH,MM,SS}} = calendar:local_time(),
    Filename = lists:flatten(io_lib:format(
                               "machi-ci-~4..0B~2..0B~2..0B-~2..0B~2..0B~2..0B.html",
                               [Y,M,D,HH,MM,SS])),
    cluster_info:dump_local_node(Filename).

-spec public_projection(atom()) -> [{atom(), term()}].
public_projection(FluName) ->
    projection(FluName, public).

-spec private_projection(atom()) -> [{atom(), term()}].
private_projection(FluName) ->
    projection(FluName, private).

-spec chain_manager(atom()) -> term().
chain_manager(FluName) ->
    Mgr = machi_flu_psup:make_mgr_supname(FluName),
    sys:get_status(Mgr).

-spec fitness(atom()) -> term().
fitness(FluName) ->
    Fitness = machi_flu_psup:make_fitness_regname(FluName),
    sys:get_status(Fitness).

-spec flu1(atom()) -> [{atom(), term()}].
flu1(FluName) ->
    State = machi_flu1_append_server:current_state(FluName),
    machi_flu1_append_server:format_state(State).

%% Internal functions

projection(FluName, Kind) ->
    ProjStore = machi_flu1:make_projection_server_regname(FluName),
    {ok, Projection} = machi_projection_store:read_latest_projection(
                         whereis(ProjStore), Kind),
    Fields = record_info(fields, projection_v1),
    [_Name | Values] = tuple_to_list(Projection),
    lists:zip(Fields, Values).

cinfo_wrapper(Fun, FluName) ->
    fun(C) ->
            cluster_info:format(C, "~p", [Fun(FluName)])
    end.
