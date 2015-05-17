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

-module(machi_proxy_flu1_client_test).
-compile(export_all).

-include("machi_projection.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MUT, machi_proxy_flu1_client).

-ifdef(TEST).

api_smoke_test() ->
    RegName = api_smoke_flu,
    Host = "localhost",
    TcpPort = 57124,
    DataDir = "./data.api_smoke_flu",
    W_props = [{initial_wedged, false}],
    FLU1 = machi_flu1_test:setup_test_flu(RegName, TcpPort, DataDir,
                                          W_props),
    erase(flu_pid),

    try
        I = #p_srvr{name=RegName, address=Host, port=TcpPort},
        {ok, Prox1} = ?MUT:start_link(I),
        try
            FakeEpoch = ?DUMMY_PV1_EPOCH,
            [{ok, {_,_,_}} = ?MUT:append_chunk(Prox1,
                                          FakeEpoch, <<"prefix">>, <<"data">>,
                                          infinity) || _ <- lists:seq(1,5)],
            %% Stop the FLU, what happens?
            machi_flu1:stop(FLU1),
            {error,_} = ?MUT:append_chunk(Prox1,
                                FakeEpoch, <<"prefix">>, <<"data">>,
                                infinity),
            {error,partition} = ?MUT:append_chunk(Prox1,
                                FakeEpoch, <<"prefix">>, <<"data">>,
                                infinity),
            %% Start the FLU again, we should be able to do stuff immediately
            FLU1b = machi_flu1_test:setup_test_flu(RegName, TcpPort, DataDir,
                                                   [save_data_dir|W_props]),
            put(flu_pid, FLU1b),
            MyChunk = <<"my chunk data">>,
            {ok, {MyOff,MySize,MyFile}} =
                ?MUT:append_chunk(Prox1, FakeEpoch, <<"prefix">>, MyChunk,
                                  infinity),
            {ok, MyChunk} = ?MUT:read_chunk(Prox1, FakeEpoch, MyFile, MyOff, MySize),
            MyChunk2 = <<"my chunk data, yeah, again">>,
            {ok, {MyOff2,MySize2,MyFile2}} =
                ?MUT:append_chunk_extra(Prox1, FakeEpoch, <<"prefix">>,
                                        MyChunk2, 4242, infinity),
            {ok, MyChunk2} = ?MUT:read_chunk(Prox1, FakeEpoch, MyFile2, MyOff2, MySize2),

            %% Alright, now for the rest of the API, whee
            BadFile = <<"no-such-file">>,
            {error, no_such_file} = ?MUT:checksum_list(Prox1, FakeEpoch, BadFile),
            {ok, [_|_]} = ?MUT:list_files(Prox1, FakeEpoch),
            {ok, {false, _}} = ?MUT:wedge_status(Prox1),
            {ok, FakeEpoch} = ?MUT:get_latest_epoch(Prox1, public),
            {error, not_written} = ?MUT:read_latest_projection(Prox1, public),
            {error, not_written} = ?MUT:read_projection(Prox1, public, 44),
            P_a = #p_srvr{name=a, address="localhost", port=6622},
            P1 = machi_projection:new(1, a, [P_a], [], [a], [], []),
            ok = ?MUT:write_projection(Prox1, public, P1),
            {ok, P1} = ?MUT:read_projection(Prox1, public, 1),
            {ok, [P1]} = ?MUT:get_all_projections(Prox1, public),
            {ok, [1]} = ?MUT:list_all_projections(Prox1, public),
            ok
        after
            _ = (catch ?MUT:quit(Prox1))
        end
    after
        (catch machi_flu1:stop(FLU1)),
        (catch machi_flu1:stop(get(flu_pid)))
    end.
    
-endif. % TEST
