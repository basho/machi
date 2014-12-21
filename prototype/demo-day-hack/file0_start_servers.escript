#!/usr/bin/env escript
%% -*- erlang -*-
%%! +A 0 -smp disable -noinput -noshell

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

-module(file0_start_servers).
-compile(export_all).

-define(NO_MODULE, true).
-include("./file0.erl").

main([]) ->
    io:format("Use:  Demonstrate the commands required to start all servers on a host.\n"),
    io:format("Args: ServerMapPath Host\n"),
    erlang:halt(1);
main([ServerMapPath, HostStr]) ->
    Host = list_to_binary(HostStr),
    {ok, Map} = file:consult(ServerMapPath),
    io:format("Run the following commands to start all servers:\n\n"),
    [begin
         DataDir = proplists:get_value(data_dir, Ps),
         io:format("    file0_server.escript file0_server ~w ~s\n",
                   [Port, DataDir])
     end || {{HostX, Port}, Ps} <- Map, HostX == Host].
