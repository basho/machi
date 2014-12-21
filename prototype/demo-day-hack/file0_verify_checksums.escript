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

-module(file0_server).
-compile(export_all).
-mode(compile). % for escript use

-define(NO_MODULE, true).
-include("./file0.erl").

main([]) ->
    io:format("Use:  Verify all chunk checksums of all files on a server.\n"),
    io:format("Args: Host Port File\n"),
    erlang:halt(1);
main([Host, PortStr, File]) ->
    Sock1 = escript_connect(Host, PortStr),
    Sock2 = escript_connect(Host, PortStr),
    TmpFile = "/tmp/verify-checksums." ++ os:getpid(),
    try
        check_checksums(Sock1, Sock2, File, TmpFile)
    after
        _ = (catch file:delete(TmpFile))
    end.

check_checksums(Sock1, Sock2, File, _TmpFile) ->
    FileBin = list_to_binary(File),
    put(count, 0),
    Proc = fun(<<Offset:16/binary, _:1/binary, Len:8/binary, _:1/binary,
                 CSumHex:32/binary, "\n">>) ->
                   Where = <<Offset/binary, " ", Len/binary, " ",
                             FileBin/binary, "\n">>,
                   ChunkProc =
                       fun(Chunk2) when is_binary(Chunk2) ->
                               CSum = hexstr_to_bin(CSumHex),
                               CSum2 = checksum(Chunk2),
                               if CSum == CSum2 ->
                                       put(count, get(count) + 1),
                                       ok; %% io:format(".");
                                  true ->
                                       CSumNow = bin_to_hexstr(CSum2),
                                       put(status, failed),
                                       io:format("~s ~s ~s CHECKSUM-ERROR ~s ~s\n",
                                                 [Offset, Len, File,
                                                  CSumHex, CSumNow])
                               end;
                          (_Else) ->
                               ok % io:format("Chunk ~P\n", [Else, 10])
                       end,
                   escript_download_chunks(Sock2, {{{Where}}}, ChunkProc);
              (<<"OK", _/binary>>) ->
                   ok; %% io:format("top:");
              (<<".\n">>) ->
                   ok %% io:format("bottom\n")
           end,
    escript_checksum_list(Sock1, FileBin, line_by_line, Proc),
    case get(status) of
        ok ->
            io:format("OK, ~w chunks are good\n", [get(count)]),
            erlang:halt();
        _ ->
            erlang:halt(1)
    end.
