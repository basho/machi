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
    FLU1 = machi_flu1_test:setup_test_flu(RegName, TcpPort, DataDir),
    erase(flu_pid),

    try
        I = #p_srvr{name=RegName, proto=ipv4, address=Host, port=TcpPort},
        {ok, Prox1} = ?MUT:start_link(I),
        try
            FakeEpoch = {-1, <<0:(20*8)/big>>},
            [{ok, {_,_,_}} = ?MUT:append_chunk(Prox1,
                                          FakeEpoch, <<"prefix">>, <<"data">>,
                                          infinity) || _ <- lists:seq(1,5)],
            %% Stop the FLU, what happens?
            machi_flu1:stop(FLU1),
            {error,_} = ?MUT:append_chunk(Prox1,
                                FakeEpoch, <<"prefix">>, <<"data">>,
                                infinity),
            {error,not_connected} = ?MUT:append_chunk(Prox1,
                                FakeEpoch, <<"prefix">>, <<"data">>,
                                infinity),
            %% Start the FLU again, we should be able to do stuff immediately
            FLU1b = machi_flu1_test:setup_test_flu(RegName, TcpPort, DataDir,
                                                   [save_data_dir]),
            put(flu_pid, FLU1b),
            MyChunk = <<"my chunk data">>,
            {ok, {MyOff,MySize,MyFile}} =
                ?MUT:append_chunk(Prox1, FakeEpoch, <<"prefix">>, MyChunk,
                             infinity),
            {ok, MyChunk} = ?MUT:read_chunk(Prox1, FakeEpoch, MyFile, MyOff, MySize),

            %% Alright, now for the rest of the API, whee
            BadFile = <<"no-such-file">>,
            {error, no_such_file} = ?MUT:checksum_list(Prox1, FakeEpoch, BadFile),
            {ok, [_]} = ?MUT:list_files(Prox1, FakeEpoch),
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
