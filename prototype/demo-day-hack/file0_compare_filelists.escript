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

-module(file0_compare_servers).
-compile(export_all).
-mode(compile). % for escript use

-define(NO_MODULE, true).
-include("./file0.erl").

main([]) ->
    io:format("Use:  Compare file lists on two servers and calculate missing files.\n"),
    io:format("Args: Host1, Port1, Host2, Port2 [<no arg> | 'null' | OutputPath]\n"),
    erlang:halt(1);
main([Host1, PortStr1, Host2, PortStr2|Args]) ->
    Sock1 = escript_connect(Host1, PortStr1),
    Sock2 = escript_connect(Host2, PortStr2),
    escript_compare_servers(Sock1, Sock2, {Host1, PortStr1}, {Host2, PortStr2},
                            Args).

