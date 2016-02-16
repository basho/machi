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
-ifndef(PULSE).

-include_lib("eunit/include/eunit.hrl").

-include("machi.hrl").
-include("machi_projection.hrl").

-define(FLU, machi_flu1).
-define(FLU_C, machi_flu1_client).

verify_file_checksums_test_() ->
    {setup,
     fun() -> os:cmd("rm -rf ./data") end,
     fun(_) -> os:cmd("rm -rf ./data")  end,
     {timeout, 60, fun() -> verify_file_checksums_test2() end}
    }.

verify_file_checksums_test2() ->
    Host = "localhost",
    TcpPort = 32958,
    DataDir = "./data",
    W_props = [{initial_wedged, false}],
    NSInfo = undefined,
    NoCSum = <<>>,
    try
        machi_test_util:start_flu_package(verify1_flu, TcpPort, DataDir,
                                          W_props),
        Sock1 = ?FLU_C:connect(#p_srvr{address=Host, port=TcpPort}),
        try
            Prefix = <<"verify_prefix">>,
            NumChunks = 10,
            [{ok, _} = ?FLU_C:append_chunk(Sock1, NSInfo, ?DUMMY_PV1_EPOCH,
                                           Prefix, <<X:(X*8)/big>>, NoCSum) ||
                X <- lists:seq(1, NumChunks)],
            {ok, [{_FileSize,File}]} = ?FLU_C:list_files(Sock1, ?DUMMY_PV1_EPOCH),
            ?assertEqual({ok, []},
                         machi_admin_util:verify_file_checksums_remote(
                           Host, TcpPort, ?DUMMY_PV1_EPOCH, File)),

            %% Clobber the first 3 chunks, which are sizes 1/2/3.
            {_, Path} = machi_util:make_data_filename(DataDir,binary_to_list(File)),
            {ok, FH} = file:open(Path, [read,write]),
            {ok, _} = file:position(FH, ?MINIMUM_OFFSET),
            ok = file:write(FH, "y"),
            ok = file:write(FH, "yo"),
            ok = file:write(FH, "yo!"),
            ok = file:close(FH),

            %% Check the local flavor of the API: should be 3 bad checksums
            {ok, Res1} = machi_admin_util:verify_file_checksums_local(
                           Host, TcpPort, ?DUMMY_PV1_EPOCH, Path),
            3 = length(Res1),

            %% Check the remote flavor of the API: should be 3 bad checksums
            {ok, Res2} = machi_admin_util:verify_file_checksums_remote(
                           Host, TcpPort, ?DUMMY_PV1_EPOCH, File),
            3 = length(Res2),

            ok
        after
            catch ?FLU_C:quit(Sock1)
        end
    after
        catch machi_test_util:stop_flu_package()
    end.

-endif. % !PULSE
-endif. % TEST

