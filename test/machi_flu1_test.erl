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

-module(machi_flu1_test).
-compile(export_all).

-ifdef(TEST).
-include("machi.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(FLU, machi_flu1).
-define(FLU_C, machi_flu1_client).

setup_test_flu(RegName, TcpPort, DataDir) ->
    setup_test_flu(RegName, TcpPort, DataDir, []).

setup_test_flu(RegName, TcpPort, DataDir, DbgProps) ->
    clean_up_data_dir(DataDir),

    {ok, FLU1} = ?FLU:start_link([{RegName, TcpPort, DataDir},
                                  {dbg, DbgProps}]),
    FLU1.

flu_smoke_test() ->
    Host = "localhost",
    TcpPort = 32957,
    DataDir = "./data",
    Prefix = <<"prefix!">>,
    BadPrefix = BadFile = "no/good",

    FLU1 = setup_test_flu(smoke_flu, TcpPort, DataDir),
    try
        {error, no_such_file} = ?FLU_C:checksum_list(Host, TcpPort,
                                                     "does-not-exist"),
        {error, bad_arg} = ?FLU_C:checksum_list(Host, TcpPort, BadFile),

        {ok, []} = ?FLU_C:list_files(Host, TcpPort),

        Chunk1 = <<"yo!">>,
        {ok, {Off1,Len1,File1}} = ?FLU_C:append_chunk(Host, TcpPort,
                                                      Prefix, Chunk1),
        {ok, Chunk1} = ?FLU_C:read_chunk(Host, TcpPort, File1, Off1, Len1),
        {ok, [{_,_,_}]} = ?FLU_C:checksum_list(Host, TcpPort, File1),
        {error, bad_arg} = ?FLU_C:append_chunk(Host, TcpPort,
                                               BadPrefix, Chunk1),
        {ok, [{_,File1}]} = ?FLU_C:list_files(Host, TcpPort),
        Len1 = size(Chunk1),
        {error, no_such_file} = ?FLU_C:read_chunk(Host, TcpPort,
                                                  File1, Off1*983, Len1),
        {error, partial_read} = ?FLU_C:read_chunk(Host, TcpPort,
                                                  File1, Off1, Len1*984),

        Chunk2 = <<"yo yo">>,
        Len2 = byte_size(Chunk2),
        Off2 = ?MINIMUM_OFFSET + 77,
        File2 = "smoke-prefix",
        ok = ?FLU_C:write_chunk(Host, TcpPort, File2, Off2, Chunk2),
        {error, bad_arg} = ?FLU_C:write_chunk(Host, TcpPort,
                                              BadFile, Off2, Chunk2),
        {ok, Chunk2} = ?FLU_C:read_chunk(Host, TcpPort, File2, Off2, Len2),
        {error, no_such_file} = ?FLU_C:read_chunk(Host, TcpPort,
                                                  "no!!", Off2, Len2),
        {error, bad_arg} = ?FLU_C:read_chunk(Host, TcpPort,
                                             BadFile, Off2, Len2),

        %% We know that File1 still exists.  Pretend that we've done a
        %% migration and exercise the delete_migration() API.
        ok = ?FLU_C:delete_migration(Host, TcpPort, File1),
        {error, no_such_file} = ?FLU_C:delete_migration(Host, TcpPort, File1),
        {error, bad_arg} = ?FLU_C:delete_migration(Host, TcpPort, BadFile),

        %% We know that File2 still exists.  Pretend that we've done a
        %% migration and exercise the trunc_hack() API.
        ok = ?FLU_C:trunc_hack(Host, TcpPort, File2),
        ok = ?FLU_C:trunc_hack(Host, TcpPort, File2),
        {error, bad_arg} = ?FLU_C:trunc_hack(Host, TcpPort, BadFile),

        ok = ?FLU_C:quit(machi_util:connect(Host, TcpPort))
    after
        ok = ?FLU:stop(FLU1)
    end.

clean_up_data_dir(DataDir) ->
    Dir1 = DataDir ++ "/config",
    Fs1 = filelib:wildcard(Dir1 ++ "/*"),
    [file:delete(F) || F <- Fs1],
    _ = file:del_dir(Dir1),
    Fs2 = filelib:wildcard(DataDir ++ "/*"),
    [file:delete(F) || F <- Fs2],
    _ = file:del_dir(DataDir),
    ok.

-endif. % TEST
