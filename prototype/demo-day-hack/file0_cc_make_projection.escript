#!/usr/bin/env escript
%% -*- erlang -*-
%%! +A 0 -smp disable -noinput -noshell -pz .

%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(file0_cc_make_projection).
-compile(export_all).
-mode(compile). % for escript use

-define(NO_MODULE, true).
-include("./file0.erl").

%% LastProjFile :: "new" : "/path/to/projection/file.proj"
%% NewWeightMapFile :: "/path/to/weight/map/file.weight"
%% NewProjectionPath :: Output file path, ".proj" suffix is recommended

main([]) ->
    io:format("Use:  Make a projection description file.\n"),
    io:format("Args: 'new'|ProjectionPath File.weight NewProjectionPath\n"),
    erlang:halt(1);
main([LastProjPath, NewWeightMapPath, NewProjectionPath]) ->
    LastP = read_projection_file(LastProjPath),
    LastFloatMap = get_float_map(LastP),
    NewWeightMap = read_weight_map_file(NewWeightMapPath),
    io:format("LastFloatMap: ~p\n", [LastFloatMap]),
    NewFloatMap = if LastFloatMap == undefined ->
                          szone_chash:make_float_map(NewWeightMap);
                     true ->
                          szone_chash:make_float_map(LastFloatMap, NewWeightMap)
                  end,
    io:format("NewFloatMap: ~p\n", [NewFloatMap]),

    NewP = #projection{epoch=LastP#projection.epoch + 1,
                       last_epoch=LastP#projection.epoch,
                       float_map=NewFloatMap,
                       last_float_map=LastFloatMap},
    ok = write_projection(NewP, NewProjectionPath).
