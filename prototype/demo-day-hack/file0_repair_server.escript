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

-module(file0_repair_server).
-compile(export_all).
-mode(compile). % for escript use

-define(NO_MODULE, true).
-include("./file0.erl").

main([]) ->
    io:format("Use:  Repair a server, *uni-directionally* from source -> destination.\n"),
    io:format("Args: SrcHost SrcPort DstHost DstPort [ verbose check repair delete-source noverbose nodelete-source ]\n"),
    erlang:halt(1);
main([SrcHost, SrcPortStr, DstHost, DstPortStr|Args]) ->
    Src = {SrcHost, SrcPortStr},
    SockSrc = escript_connect(SrcHost, SrcPortStr),
    %% TODO is SockSrc2 necessary?  Is SockDst2 necessary?  If not, delete!
    SockSrc2 = escript_connect(SrcHost, SrcPortStr),
    ok = inet:setopts(SockSrc2, [{packet, raw}]),
    SockDst = escript_connect(DstHost, DstPortStr),
    SockDst2 = escript_connect(DstHost, DstPortStr),
    Ps = make_repair_props(Args),
    case proplists:get_value(mode, Ps) of
        undefined -> io:format("NOTICE: default mode = check\n"),
                     timer:sleep(2*1000);
        _         -> ok
    end,
    case proplists:get_value(verbose, Ps) of
        true ->
            io:format("Date & Time: ~p ~p\n", [date(), time()]),
            io:format("Src: ~s ~s\n", [SrcHost, SrcPortStr]),
            io:format("Dst: ~s ~s\n", [DstHost, DstPortStr]),
            io:format("\n");
        _ ->
            ok
    end,
    %% Dst = {DstHost, DstPortStr},

    X = escript_compare_servers(SockSrc, SockDst,
                                {SrcHost, SrcPortStr}, {DstHost, DstPortStr},
                                fun(_FileName) -> true end,
                                [null]),
    [repair(File, Size, MissingList,
            proplists:get_value(mode, Ps, check),
            proplists:get_value(verbose, Ps, t),
            SockSrc, SockSrc2, SockDst, SockDst2, Src) ||
        {File, {Size, MissingList}} <- X].
