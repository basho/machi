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

-include("machi.hrl").
-include("machi_projection.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MUT, machi_proxy_flu1_client).

-ifdef(TEST).
-ifndef(PULSE).

api_smoke_test() ->
    RegName = api_smoke_flu,
    Host = "localhost",
    TcpPort = 57124,
    DataDir = "./data.api_smoke_flu",
    W_props = [{initial_wedged, false}],
    Prefix = <<"prefix">>,
    FLU1 = machi_flu1_test:setup_test_flu(RegName, TcpPort, DataDir,
                                          W_props),
    erase(flu_pid),

    try
        I = #p_srvr{name=RegName, address=Host, port=TcpPort},
        {ok, Prox1} = ?MUT:start_link(I),
        try
            FakeEpoch = ?DUMMY_PV1_EPOCH,
            [{ok, {_,_,_}} = ?MUT:append_chunk(Prox1,
                                          FakeEpoch, Prefix, <<"data">>,
                                          infinity) || _ <- lists:seq(1,5)],
            %% Stop the FLU, what happens?
            machi_flu1:stop(FLU1),
            [{error,partition} = ?MUT:append_chunk(Prox1,
                                FakeEpoch, Prefix, <<"data-stopped1">>,
                                infinity) || _ <- lists:seq(1,3)],
            %% Start the FLU again, we should be able to do stuff immediately
            FLU1b = machi_flu1_test:setup_test_flu(RegName, TcpPort, DataDir,
                                                   [save_data_dir|W_props]),
            put(flu_pid, FLU1b),
            MyChunk = <<"my chunk data">>,
            {ok, {MyOff,MySize,MyFile}} =
                ?MUT:append_chunk(Prox1, FakeEpoch, Prefix, MyChunk,
                                  infinity),
            {ok, MyChunk} = ?MUT:read_chunk(Prox1, FakeEpoch, MyFile, MyOff, MySize),
            MyChunk2 = <<"my chunk data, yeah, again">>,
            {ok, {MyOff2,MySize2,MyFile2}} =
                ?MUT:append_chunk_extra(Prox1, FakeEpoch, Prefix,
                                        MyChunk2, 4242, infinity),
            {ok, MyChunk2} = ?MUT:read_chunk(Prox1, FakeEpoch, MyFile2, MyOff2, MySize2),
            MyChunk_badcs = {<<?CSUM_TAG_CLIENT_SHA:8, 0:(8*20)>>, MyChunk},
            {error, bad_checksum} = ?MUT:append_chunk(Prox1, FakeEpoch,
                                                      Prefix, MyChunk_badcs),
            {error, bad_checksum} = ?MUT:write_chunk(Prox1, FakeEpoch,
                                                     <<"foo-file">>, 99832,
                                                     MyChunk_badcs),

            %% Put kick_projection_reaction() in the middle of the test so
            %% that any problems with its async nature will (hopefully)
            %% cause problems later in the test.
            ok = ?MUT:kick_projection_reaction(Prox1, []),

            %% Alright, now for the rest of the API, whee
            BadFile = <<"no-such-file">>,
            {error, no_such_file} = ?MUT:checksum_list(Prox1, FakeEpoch, BadFile),
            {ok, [_|_]} = ?MUT:list_files(Prox1, FakeEpoch),
            {ok, {false, _}} = ?MUT:wedge_status(Prox1),
            {ok, FakeEpoch} = ?MUT:get_latest_epochid(Prox1, public),
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

flu_restart_test() ->
    RegName = api_smoke_flu,
    Host = "localhost",
    TcpPort = 57125,
    DataDir = "./data.api_smoke_flu2",
    W_props = [{initial_wedged, false}],
    erase(flu_pid),
    put(flu_pid, []),
    FLU1 = machi_flu1_test:setup_test_flu(RegName, TcpPort, DataDir,
                                          W_props),
    put(flu_pid, [FLU1|get(flu_pid)]),

    try
        I = #p_srvr{name=RegName, address=Host, port=TcpPort},
        {ok, Prox1} = ?MUT:start_link(I),
        try
            FakeEpoch = ?DUMMY_PV1_EPOCH,
            Data = <<"data!">>,
            {ok, {Off1,Size1,File1}} = ?MUT:append_chunk(Prox1,
                                          FakeEpoch, <<"prefix">>, Data,
                                          infinity),
            P_a = #p_srvr{name=a, address="localhost", port=6622},
            P1 = machi_projection:new(1, a, [P_a], [], [a], [], []),
            EpochID = {P1#projection_v1.epoch_number,
                       P1#projection_v1.epoch_csum},
            ok = ?MUT:write_projection(Prox1, public, P1),
            ok = ?MUT:write_projection(Prox1, private, P1),
            {ok, EpochID} = ?MUT:get_epoch_id(Prox1),
            {ok, EpochID} = ?MUT:get_latest_epochid(Prox1, public),
            {ok, EpochID} = ?MUT:get_latest_epochid(Prox1, private),
            ok = machi_flu1:stop(FLU1), timer:sleep(50),

            %% Now that the last proxy op was successful and only
            %% after did we stop the FLU, let's check that both the
            %% 1st & 2nd ops-via-proxy after FLU is restarted are
            %% successful.  And immediately after stopping the FLU,
            %% both 1st & 2nd ops-via-proxy should always fail.
            %%
            %% Some of the expectations have unbound variables, which
            %% makes the code a bit convoluted.  (No LFE or
            %% Elixir macros here, alas, they'd be useful.)

            ExpectedOps = 
                [
                 fun(run) -> {ok, EpochID} = ?MUT:get_epoch_id(Prox1),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:get_epoch_id(Prox1) end,
                 fun(run) -> {ok, EpochID} =
                                 ?MUT:get_latest_epochid(Prox1, public),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:get_latest_epochid(Prox1, public)
                 end,
                 fun(run) -> {ok, EpochID} = 
                                 ?MUT:get_latest_epochid(Prox1, private),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:get_latest_epochid(Prox1, private)
                 end,
                 fun(run) -> {ok, P1} =
                                 ?MUT:read_projection(Prox1, public, 1),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:read_projection(Prox1, public, 1)
                 end,
                 fun(run) -> {ok, P1} = 
                                 ?MUT:read_projection(Prox1, private, 1),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:read_projection(Prox1, private, 1)
                 end,
                 fun(run) -> {error, not_written} =
                                 ?MUT:read_projection(Prox1, private, 7),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:read_projection(Prox1, private, 7)
                 end,
                 fun(run) -> {error, written} =
                                 ?MUT:write_projection(Prox1, public, P1),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_projection(Prox1, public, P1)
                 end,
                 fun(run) -> {error, written} =
                                 ?MUT:write_projection(Prox1, private, P1),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_projection(Prox1, private, P1)
                 end,
                 fun(run) -> {ok, [_]} =
                                 ?MUT:get_all_projections(Prox1, public),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:get_all_projections(Prox1, public)
                 end,
                 fun(run) -> {ok, [_]} =
                                 ?MUT:get_all_projections(Prox1, private),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:get_all_projections(Prox1, private)
                 end,
                 fun(run) -> {ok, {_,_,_}} =
                                 ?MUT:append_chunk(Prox1, FakeEpoch,
                                                <<"prefix">>, Data, infinity),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:append_chunk(Prox1, FakeEpoch,
                                                <<"prefix">>, Data, infinity)
                 end,
                 fun(run) -> {ok, {_,_,_}} =
                                 ?MUT:append_chunk_extra(Prox1, FakeEpoch,
                                             <<"prefix">>, Data, 42, infinity),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:append_chunk_extra(Prox1, FakeEpoch,
                                             <<"prefix">>, Data, 42, infinity)
                 end,
                 fun(run) -> {ok, Data} =
                                 ?MUT:read_chunk(Prox1, FakeEpoch,
                                                 File1, Off1, Size1),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:read_chunk(Prox1, FakeEpoch,
                                                 File1, Off1, Size1)
                 end,
                 fun(run) -> {ok, KludgeBin} =
                                 ?MUT:checksum_list(Prox1, FakeEpoch, File1),
                             true = is_binary(KludgeBin),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:checksum_list(Prox1, FakeEpoch, File1)
                 end,
                 fun(run) -> {ok, _} =
                                 ?MUT:list_files(Prox1, FakeEpoch),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:list_files(Prox1, FakeEpoch)
                 end,
                 fun(run) -> {ok, _} =
                                 ?MUT:wedge_status(Prox1),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:wedge_status(Prox1)
                 end,
                 %% NOTE: When write-once enforcement is enabled, this test
                 %% will fail: change ok -> {error, written}
                 fun(run) -> %% {error, written} =
                             ok =
                                 ?MUT:write_chunk(Prox1, FakeEpoch, File1, Off1,
                                                  Data, infinity),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_chunk(Prox1, FakeEpoch, File1, Off1,
                                               Data, infinity)
                 end
                ],

            [begin
                 FLU2 = machi_flu1_test:setup_test_flu(
                          RegName, TcpPort, DataDir,
                          [save_data_dir|W_props]),
                 put(flu_pid, [FLU2|get(flu_pid)]),
                 _ = Fun(line),
                 ok = Fun(run),
                 ok = Fun(run),
                 ok = machi_flu1:stop(FLU2),
                 {error, partition} = Fun(stop),
                 {error, partition} = Fun(stop),
                 ok
             end || Fun <- ExpectedOps ],
            ok
        after
            _ = (catch ?MUT:quit(Prox1))
        end
    after
        [catch machi_flu1:stop(Pid) || Pid <- get(flu_pid)]
    end.
    
-endif. % !PULSE
-endif. % TEST
