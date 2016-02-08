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
    TcpPort = 17124,
    DataDir = "./data.api_smoke_flu",
    W_props = [{active_mode, false},{initial_wedged, false}],
    Prefix = <<"prefix">>,
    NSInfo = undefined,
    NoCSum = <<>>,

    try
        {[I], _, _} = machi_test_util:start_flu_package(
                        RegName, TcpPort, DataDir, W_props),
        {ok, Prox1} = ?MUT:start_link(I),
        try
            FakeEpoch = ?DUMMY_PV1_EPOCH,
            [{ok, {_,_,_}} = ?MUT:append_chunk(
                                          Prox1, NSInfo, FakeEpoch,
                                          Prefix, <<"data">>, NoCSum) ||
                _ <- lists:seq(1,5)],
            %% Stop the FLU, what happens?
            machi_test_util:stop_flu_package(),
            [{error,partition} = ?MUT:append_chunk(Prox1, NSInfo,
                                FakeEpoch, Prefix, <<"data-stopped1">>,
                                NoCSum) || _ <- lists:seq(1,3)],
            %% Start the FLU again, we should be able to do stuff immediately
            machi_test_util:start_flu_package(RegName, TcpPort, DataDir,
                                              [no_cleanup|W_props]),
            MyChunk = <<"my chunk data">>,
            {ok, {MyOff,MySize,MyFile}} =
                ?MUT:append_chunk(Prox1, NSInfo, FakeEpoch, Prefix, MyChunk,
                                  NoCSum),
            {ok, {[{_, MyOff, MyChunk, _MyChunkCSUM}], []}} =
                ?MUT:read_chunk(Prox1, NSInfo, FakeEpoch, MyFile, MyOff, MySize, undefined),
            MyChunk2_parts = [<<"my chunk ">>, "data", <<", yeah, again">>],
            MyChunk2 = iolist_to_binary(MyChunk2_parts),
            Opts1 = #append_opts{chunk_extra=4242},
            {ok, {MyOff2,MySize2,MyFile2}} =
                ?MUT:append_chunk(Prox1, NSInfo, FakeEpoch, Prefix,
                                  MyChunk2_parts, NoCSum, Opts1, infinity),
            [{ok, {[{_, MyOff2, MyChunk2, _}], []}} =
                ?MUT:read_chunk(Prox1, NSInfo, FakeEpoch, MyFile2, MyOff2, MySize2, DefaultOptions) ||
                DefaultOptions <- [undefined, noopt, none, any_atom_at_all] ],

            BadCSum = {?CSUM_TAG_CLIENT_SHA, crypto:hash(sha, "...................")},
            {error, bad_checksum} = ?MUT:append_chunk(Prox1, NSInfo, FakeEpoch,
                                                      Prefix, MyChunk, BadCSum),
            {error, bad_checksum} = ?MUT:write_chunk(Prox1, NSInfo, FakeEpoch,
                                                     MyFile2,
                                                     MyOff2 + size(MyChunk2),
                                                     MyChunk, BadCSum,
                                                     infinity),

            %% Put kick_projection_reaction() in the middle of the test so
            %% that any problems with its async nature will (hopefully)
            %% cause problems later in the test.
            ok = ?MUT:kick_projection_reaction(Prox1, []),

            %% Alright, now for the rest of the API, whee
            BadFile = <<"no-such-file">>,
            {error, bad_arg} = ?MUT:checksum_list(Prox1, BadFile),
            {ok, [_|_]} = ?MUT:list_files(Prox1, FakeEpoch),
            {ok, {false, _,_,_}} = ?MUT:wedge_status(Prox1),
            {ok, {0, _SomeCSum}} = ?MUT:get_latest_epochid(Prox1, public),
            {ok, #projection_v1{epoch_number=0}} =
                 ?MUT:read_latest_projection(Prox1, public),
            {error, not_written} = ?MUT:read_projection(Prox1, public, 44),
            P_a = #p_srvr{name=a, address="localhost", port=6622},
            P1 = machi_projection:new(1, a, [P_a], [], [a], [], []),
            ok = ?MUT:write_projection(Prox1, public, P1),
            {ok, P1} = ?MUT:read_projection(Prox1, public, 1),
            {ok, [#projection_v1{epoch_number=0},P1]} =
                ?MUT:get_all_projections(Prox1, public),
            {ok, [0,1]} = ?MUT:list_all_projections(Prox1, public),

            ok
        after
            _ = (catch ?MUT:quit(Prox1))
        end
    after
        (catch machi_test_util:stop_flu_package())
    end.

flu_restart_test_() ->
    {timeout, 1*60, fun() -> flu_restart_test2() end}.

flu_restart_test2() ->
    RegName = a,
    TcpPort = 17125,
    DataDir = "./data.api_smoke_flu2",
    W_props = [{initial_wedged, false}, {active_mode, false}],
    NSInfo = undefined,
    NoCSum = <<>>,

    try
        {[I], _, _} = machi_test_util:start_flu_package(
                        RegName, TcpPort, DataDir, W_props),
        {ok, Prox1} = ?MUT:start_link(I),
        try
            FakeEpoch = ?DUMMY_PV1_EPOCH,
            Data   = <<"data!">>,
            Dataxx = <<"Fake!">>,
            {ok, {Off1,Size1,File1}} = ?MUT:append_chunk(Prox1, NSInfo,
                                         FakeEpoch, <<"prefix">>, Data, NoCSum),
            P_a = #p_srvr{name=a, address="localhost", port=6622},
            P1   = machi_projection:new(1, RegName, [P_a], [], [RegName], [], []),
            P1xx = P1#projection_v1{dbg2=["dbg2 changes are ok"]},
            P1yy = P1#projection_v1{dbg=["not exactly the same as P1!!!"]},
            EpochID = {P1#projection_v1.epoch_number,
                       P1#projection_v1.epoch_csum},
            ok = ?MUT:write_projection(Prox1, public, P1),
            ok = ?MUT:write_projection(Prox1, private, P1),
            {ok, EpochID} = ?MUT:get_epoch_id(Prox1),
            {ok, EpochID} = ?MUT:get_latest_epochid(Prox1, public),
            {ok, EpochID} = ?MUT:get_latest_epochid(Prox1, private),
            ok = machi_test_util:stop_flu_package(), timer:sleep(50),

            %% Now that the last proxy op was successful and only
            %% after did we stop the FLU, let's check that both the
            %% 1st & 2nd ops-via-proxy after FLU is restarted are
            %% successful.  And immediately after stopping the FLU,
            %% both 1st & 2nd ops-via-proxy should always fail.
            %%
            %% Some of the expectations have unbound variables, which
            %% makes the code a bit convoluted.  (No LFE or
            %% Elixir macros here, alas, they'd be useful.)

            AppendOpts1 = #append_opts{chunk_extra=42},
            ExpectedOps = 
                [
                 fun(run) -> ?assertEqual({ok, EpochID}, ?MUT:get_epoch_id(Prox1)),
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
                 fun(run) -> ok =
                                 ?MUT:write_projection(Prox1, public, P1),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_projection(Prox1, public, P1)
                 end,
                 fun(run) -> ok =
                                 ?MUT:write_projection(Prox1, private, P1),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_projection(Prox1, private, P1)
                 end,
                 fun(run) -> {error, written} =
                                 ?MUT:write_projection(Prox1, public, P1xx),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_projection(Prox1, public, P1xx)
                 end,

                 fun(run) -> ok = %% P1xx is difference only in dbg2
                                 ?MUT:write_projection(Prox1, private, P1xx),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_projection(Prox1, private, P1xx)
                 end,
                 fun(run) -> {error, bad_arg} = % P1yy has got bad checksum
                                 ?MUT:write_projection(Prox1, private, P1yy),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_projection(Prox1, private, P1yy)
                 end,

                 fun(run) -> {ok, [#projection_v1{epoch_number=0}, #projection_v1{epoch_number=1}]} =
                                 ?MUT:get_all_projections(Prox1, public),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:get_all_projections(Prox1, public)
                 end,
                 fun(run) -> {ok, [#projection_v1{epoch_number=0}, #projection_v1{epoch_number=1}]} =
                                 ?MUT:get_all_projections(Prox1, private),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:get_all_projections(Prox1, private)
                 end,
                 fun(run) -> {ok, {_,_,_}} =
                                 ?MUT:append_chunk(Prox1, NSInfo, FakeEpoch,
                                                <<"prefix">>, Data, NoCSum),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:append_chunk(Prox1, NSInfo, FakeEpoch,
                                                <<"prefix">>, Data, NoCSum)
                 end,
                 fun(run) -> {ok, {_,_,_}} =
                                 ?MUT:append_chunk(Prox1, NSInfo, FakeEpoch,
                                                   <<"prefix">>, Data, NoCSum,
                                                   AppendOpts1, infinity),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:append_chunk(Prox1, NSInfo, FakeEpoch,
                                                   <<"prefix">>, Data, NoCSum,
                                                   AppendOpts1, infinity)
                 end,
                 fun(run) -> {ok, {[{_, Off1, Data, _}], []}} =
                                 ?MUT:read_chunk(Prox1, NSInfo, FakeEpoch,
                                                 File1, Off1, Size1, undefined),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:read_chunk(Prox1, NSInfo, FakeEpoch,
                                              File1, Off1, Size1, undefined)
                 end,
                 fun(run) -> {ok, KludgeBin} =
                                 ?MUT:checksum_list(Prox1, File1),
                             true = is_binary(KludgeBin),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:checksum_list(Prox1, File1)
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
                 fun(run) ->
                             ok =
                                 ?MUT:write_chunk(Prox1, NSInfo, FakeEpoch, File1, Off1,
                                                  Data, NoCSum, infinity),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_chunk(Prox1, NSInfo, FakeEpoch, File1, Off1,
                                               Data, NoCSum, infinity)
                 end,
                 fun(run) -> 
                         {error, written} =
                                 ?MUT:write_chunk(Prox1, NSInfo, FakeEpoch, File1, Off1,
                                                  Dataxx, NoCSum, infinity),
                             ok;
                    (line) -> io:format("line ~p, ", [?LINE]);
                    (stop) -> ?MUT:write_chunk(Prox1, NSInfo, FakeEpoch, File1, Off1,
                                               Dataxx, NoCSum, infinity)
                 end
                ],

            [begin
                 machi_test_util:start_flu_package(
                          RegName, TcpPort, DataDir,
                          [no_cleanup|W_props]),
                 _ = Fun(line),
                 ok = Fun(run),
                 ok = Fun(run),
                 ok = machi_test_util:stop_flu_package(),
                 {error, partition} = Fun(stop),
                 {error, partition} = Fun(stop),
                 ok
             end || Fun <- ExpectedOps ],
            ok
        after
            _ = (catch ?MUT:quit(Prox1))
        end
    after
        (catch machi_test_util:stop_flu_package())
    end.

-endif. % !PULSE
-endif. % TEST
