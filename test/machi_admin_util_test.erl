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

-module(machi_admin_util_test).
-compile(export_all).

-ifdef(TEST).

-include("machi.hrl").

-define(FLU, machi_flu1).
-define(FLU_C, machi_flu1_client).

verify_file_checksums_remote_test() ->
    Host = "localhost",
    TcpPort = 32958,
    DataDir = "./data",
    FLU1 = machi_flu1_test:setup_test_flu(verify1_flu, TcpPort, DataDir),
    Sock1 = machi_util:connect(Host, TcpPort),
    try
        Prefix = <<"verify_prefix">>,
        [{ok, _} = ?FLU_C:append_chunk(Sock1, Prefix, <<X:(X*8)/big>>) ||
            X <- lists:seq(1,10)],
        {ok, [{_FileSize,File}]} = ?FLU_C:list_files(Sock1),
        {ok, []} = machi_admin_util:verify_file_checksums_remote(
                     Host, TcpPort, File),

        Path = DataDir ++ "/" ++ binary_to_list(File),
        {ok, FH} = file:open(Path, [read,write]),
        {ok, _} = file:position(FH, ?MINIMUM_OFFSET),
        ok = file:write(FH, "y"),
        ok = file:write(FH, "yo"),
        ok = file:write(FH, "yo!"),
        ok = file:close(FH),
        {ok, [_,_,_]} = machi_admin_util:verify_file_checksums_remote(
                          Host, TcpPort, File)
    after
        catch ?FLU_C:quick(Sock1),
        ok = ?FLU:stop(FLU1)
    end.

-endif. % TEST

