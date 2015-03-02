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

-module(file0_cc_map_prefix).
-compile(export_all).
-mode(compile). % for escript use

-define(NO_MODULE, true).
-include("./file0.erl").

%% Map a prefix into a projection.
%% Return a list of host/ip pairs, one per line.  If migrating,
%% and if the current & last projections do not match, then the
%% servers for new, old, & new will be given.
%% The # of output lines can be limited with an optional 3rd arg.

main([]) ->
    io:format("Use:  Map a file prefix to a chain via projection.\n"),
    io:format("Args: ProjectionPath Prefix [MaxReturnResults]\n"),
    erlang:halt(1);
main([ProjectionPathOrDir, Prefix]) ->
    main([ProjectionPathOrDir, Prefix, "3"]);
main([ProjectionPathOrDir, Prefix, MaxNumStr]) ->
    P = read_projection_file(ProjectionPathOrDir),
    Chains = lists:sublist(hash_and_query(Prefix, P),
                           list_to_integer(MaxNumStr)),
    ChainMap = read_chain_map_file(ProjectionPathOrDir),
    [io:format("~s ~s ~w\n", [Chain, Host, Port]) ||
        Chain  <- Chains,
        {Host, Port} <- orddict:fetch(Chain, ChainMap)
    ].


