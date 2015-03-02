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

-module(file0_test).
-compile(export_all).
-mode(compile).

-define(NO_MODULE, true).
-include("./file0.erl").

main([]) ->
    io:format("Use:  unit test script.\n"),

    Port1 = "7061",
    Port2 = "7062",
    Dir1 = "/tmp/file0_test1",
    Dir2 = "/tmp/file0_test2",
    Pfx1 = "testing-prefix1",
    Pfx2 = "testing-prefix2",
    application:set_env(kernel, verbose, false),

    os:cmd("rm -rf " ++ Dir1),
    ok = start_server(["file0_test", Port1, Dir1]),
    os:cmd("rm -rf " ++ Dir2),
    ok = start_server(["server2", Port2, Dir2]),
    timer:sleep(250),

    %% Pattern match assumes that /etc/hosts exists and is at least 11 bytes.
    [_,_|_] = Chunks1 =
        test_file_write_client(["localhost", Port1, "5", Pfx1, "/etc/hosts", "/dev/null"]),
    io:format("write: pass\n"),

    [_,_|_] = test_chunk_read_client(["localhost", Port1], Chunks1),
    [{error,_}|_] = (catch test_chunk_read_client(["localhost", Port2], Chunks1)),
    io:format("read: pass\n"),

    [_] = test_list_client(["localhost", Port1, "/dev/null"]),
    []  = test_list_client(["localhost", Port2, "/dev/null"]),
    io:format("list: pass\n"),

    %% Pattern match assumes that /etc/hosts exists and is at least 11 bytes.
    [_,_,_|_] = _Chunks2 =
        test_1file_write_redundant_client(
          ["5", Pfx2, "/etc/hosts", "silent", "localhost", Port1, "localhost", Port2]),
    [_,_] = test_list_client(["localhost", Port1, "/dev/null"]),
    [_]  = test_list_client(["localhost", Port2, "/dev/null"]),
    io:format("write-redundant: pass\n"),

    [PoorChunk]  = test_list_client(["localhost", Port2, "/dev/null"]),
    PoorFile = re:replace(PoorChunk, ".* ", "", [{return,binary}]),
    ok = test_delete_client(["localhost", Port2, PoorFile]),
    error = test_delete_client(["localhost", Port2, PoorFile]),
    io:format("delete: pass\n"),

    ok.
    
start_server([_RegNameStr,_PortStr,_Dir] = Args) ->
    spawn(fun() -> main2(["server" | Args]) end),
    ok.

test_file_write_client([_Host,_PortStr,_ChunkSizeStr,_PrefixStr,_LocalPath,_Output] = Args) ->
    main2(["file-write-client" | Args]).

test_1file_write_redundant_client([_ChunkSizeStr,_PrefixStr,_LocalPath|_] = Args) ->
    main2(["1file-write-redundant-client" | Args]).

test_chunk_read_client([_Host,_PortStr] = Args, Chunks) ->
    ChunkFile = "/tmp/chunkfile." ++ os:getpid(),
    {ok, FH} = file:open(ChunkFile, [write]),
    [begin
         OffsetHex = bin_to_hexstr(<<Offset:64/big>>),
         SizeHex = list_to_binary(bin_to_hexstr(<<Size:32/big>>)),
         io:format(FH, "~s ~s ~s\n", [OffsetHex, SizeHex, File])
     end || {Offset, Size, File} <- Chunks],
    file:close(FH),
    try
        main2(["chunk-read-client" | Args] ++ [ChunkFile, "/dev/null"])
    after
        file:delete(ChunkFile)
    end.

test_list_client([_Host,_PortStr,_OutputFile] = Args) ->
    main2(["list-client" | Args]).

test_delete_client([_Host,_PortStr,_File] = Args) ->
    main2(["delete-client" | Args]).
