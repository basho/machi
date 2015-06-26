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
-include("machi_projection.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(FLU, machi_flu1).
-define(FLU_C, machi_flu1_client).

clean_up_data_dir(DataDir) ->
    [begin
         Fs = filelib:wildcard(DataDir ++ Glob),
         [file:delete(F) || F <- Fs],
         [file:del_dir(F) || F <- Fs]
     end || Glob <- ["*/*/*/*", "*/*/*", "*/*", "*"] ],
    _ = file:del_dir(DataDir),
    ok.

setup_test_flu(RegName, TcpPort, DataDir) ->
    setup_test_flu(RegName, TcpPort, DataDir, []).

setup_test_flu(RegName, TcpPort, DataDir, DbgProps) ->
    case proplists:get_value(save_data_dir, DbgProps) of
        true ->
            ok;
        _ ->
            clean_up_data_dir(DataDir)
    end,

    {ok, FLU1} = ?FLU:start_link([{RegName, TcpPort, DataDir},
                                  {dbg, DbgProps}]),
    %% TODO the process structuring/racy-ness of the various processes
    %% of the FLU needs to be deterministic to remove this sleep race
    %% "prevention".
    timer:sleep(10),
    FLU1.

-ifndef(PULSE).

flu_smoke_test() ->
    Host = "localhost",
    TcpPort = 32957,
    DataDir = "./data",
    Prefix = <<"prefix!">>,
    BadPrefix = BadFile = "no/good",

    W_props = [{initial_wedged, false}],
    FLU1 = setup_test_flu(smoke_flu, TcpPort, DataDir, W_props),
    try
        Msg = "Hello, world!",
        Msg = ?FLU_C:echo(Host, TcpPort, Msg),
        {error, no_such_file} = ?FLU_C:checksum_list(Host, TcpPort,
                                                     ?DUMMY_PV1_EPOCH,
                                                     "does-not-exist"),
        {error, bad_arg} = ?FLU_C:checksum_list(Host, TcpPort,
                                                ?DUMMY_PV1_EPOCH, BadFile),

        {ok, []} = ?FLU_C:list_files(Host, TcpPort, ?DUMMY_PV1_EPOCH),
        {ok, {false, _}} = ?FLU_C:wedge_status(Host, TcpPort),

        Chunk1 = <<"yo!">>,
        {ok, {Off1,Len1,File1}} = ?FLU_C:append_chunk(Host, TcpPort,
                                                      ?DUMMY_PV1_EPOCH,
                                                      Prefix, Chunk1),
        {ok, Chunk1} = ?FLU_C:read_chunk(Host, TcpPort, ?DUMMY_PV1_EPOCH,
                                         File1, Off1, Len1),
        %% TODO: when checksum_list() is refactored, restore this test!
        %% {ok, [{_,_,_}]} = ?FLU_C:checksum_list(Host, TcpPort,
        %%                                        ?DUMMY_PV1_EPOCH, File1),
        {ok, _} = ?FLU_C:checksum_list(Host, TcpPort,
                                               ?DUMMY_PV1_EPOCH, File1),
        {error, bad_arg} = ?FLU_C:append_chunk(Host, TcpPort,
                                               ?DUMMY_PV1_EPOCH,
                                               BadPrefix, Chunk1),
        {ok, [{_,File1}]} = ?FLU_C:list_files(Host, TcpPort, ?DUMMY_PV1_EPOCH),
        Len1 = size(Chunk1),
        {error, not_written} = ?FLU_C:read_chunk(Host, TcpPort,
                                                  ?DUMMY_PV1_EPOCH,
                                                  File1, Off1*983829323, Len1),
        {error, partial_read} = ?FLU_C:read_chunk(Host, TcpPort,
                                                  ?DUMMY_PV1_EPOCH,
                                                  File1, Off1, Len1*9999),

        {ok, {Off1b,Len1b,File1b}} = ?FLU_C:append_chunk(Host, TcpPort,
                                                         ?DUMMY_PV1_EPOCH,
                                                         Prefix, Chunk1),
        Extra = 42,
        {ok, {Off1c,Len1c,File1c}} = ?FLU_C:append_chunk_extra(Host, TcpPort,
                                                         ?DUMMY_PV1_EPOCH,
                                                         Prefix, Chunk1, Extra),
        {ok, {Off1d,Len1d,File1d}} = ?FLU_C:append_chunk(Host, TcpPort,
                                                         ?DUMMY_PV1_EPOCH,
                                                         Prefix, Chunk1),
        if File1b == File1c, File1c == File1d ->
                true = (Off1c == Off1b + Len1b),
                true = (Off1d == Off1c + Len1c + Extra);
           true ->
                exit(not_mandatory_but_test_expected_same_file_fixme)
        end,

        Chunk1_cs = {<<?CSUM_TAG_NONE:8, 0:(8*20)>>, Chunk1},
        {ok, {Off1e,Len1e,File1e}} = ?FLU_C:append_chunk(Host, TcpPort,
                                                         ?DUMMY_PV1_EPOCH,
                                                         Prefix, Chunk1_cs),

        Chunk2 = <<"yo yo">>,
        Len2 = byte_size(Chunk2),
        Off2 = ?MINIMUM_OFFSET + 77,
        File2 = "smoke-whole-file",
        ok = ?FLU_C:write_chunk(Host, TcpPort, ?DUMMY_PV1_EPOCH,
                                File2, Off2, Chunk2),
        {error, bad_arg} = ?FLU_C:write_chunk(Host, TcpPort, ?DUMMY_PV1_EPOCH,
                                              BadFile, Off2, Chunk2),
        {ok, Chunk2} = ?FLU_C:read_chunk(Host, TcpPort, ?DUMMY_PV1_EPOCH,
                                         File2, Off2, Len2),
        {error, not_written} = ?FLU_C:read_chunk(Host, TcpPort,
                                                 ?DUMMY_PV1_EPOCH,
                                                 "no!!", Off2, Len2),
        {error, bad_arg} = ?FLU_C:read_chunk(Host, TcpPort,
                                             ?DUMMY_PV1_EPOCH,
                                             BadFile, Off2, Len2),

        %% We know that File1 still exists.  Pretend that we've done a
        %% migration and exercise the delete_migration() API.
        ok = ?FLU_C:delete_migration(Host, TcpPort, ?DUMMY_PV1_EPOCH, File1),
        {error, no_such_file} = ?FLU_C:delete_migration(Host, TcpPort,
                                                        ?DUMMY_PV1_EPOCH, File1),
        {error, bad_arg} = ?FLU_C:delete_migration(Host, TcpPort,
                                                   ?DUMMY_PV1_EPOCH, BadFile),

        %% We know that File2 still exists.  Pretend that we've done a
        %% migration and exercise the trunc_hack() API.
        ok = ?FLU_C:trunc_hack(Host, TcpPort, ?DUMMY_PV1_EPOCH, File2),
        ok = ?FLU_C:trunc_hack(Host, TcpPort, ?DUMMY_PV1_EPOCH, File2),
        {error, bad_arg} = ?FLU_C:trunc_hack(Host, TcpPort,
                                             ?DUMMY_PV1_EPOCH, BadFile),

        ok = ?FLU_C:quit(?FLU_C:connect(#p_srvr{address=Host,
                                                port=TcpPort}))
    after
        ok = ?FLU:stop(FLU1)
    end.

flu_projection_smoke_test() ->
    Host = "localhost",
    TcpPort = 32959,
    DataDir = "./data",

    FLU1 = setup_test_flu(projection_test_flu, TcpPort, DataDir),
    try
        [begin
             {ok, {0,_}} = ?FLU_C:get_latest_epochid(Host, TcpPort, T),
             {error, not_written} =
                 ?FLU_C:read_latest_projection(Host, TcpPort, T),
             {ok, []} = ?FLU_C:list_all_projections(Host, TcpPort, T),
             {ok, []} = ?FLU_C:get_all_projections(Host, TcpPort, T),

             P_a = #p_srvr{name=a, address="localhost", port=4321},
             P1 = machi_projection:new(1, a, [P_a], [], [a], [], []),
             ok = ?FLU_C:write_projection(Host, TcpPort, T, P1),
             {error, written} = ?FLU_C:write_projection(Host, TcpPort, T, P1),
             {ok, P1} = ?FLU_C:read_projection(Host, TcpPort, T, 1),
             {ok, {1,_}} = ?FLU_C:get_latest_epochid(Host, TcpPort, T),
             {ok, P1} = ?FLU_C:read_latest_projection(Host, TcpPort, T),
             {ok, [1]} = ?FLU_C:list_all_projections(Host, TcpPort, T),
             {ok, [P1]} = ?FLU_C:get_all_projections(Host, TcpPort, T),
             {error, not_written} = ?FLU_C:read_projection(Host, TcpPort, T, 2)
         end || T <- [public, private] ]
    after
        ok = ?FLU:stop(FLU1)
    end.

bad_checksum_test() ->
    Host = "localhost",
    TcpPort = 32960,
    DataDir = "./data",

    FLU1 = setup_test_flu(projection_test_flu, TcpPort, DataDir),
    try
        Prefix = <<"some prefix">>,
        Chunk1 = <<"yo yo yo">>,
        Chunk1_badcs = {<<?CSUM_TAG_CLIENT_SHA:8, 0:(8*20)>>, Chunk1},
        {error, bad_checksum} = ?FLU_C:append_chunk(Host, TcpPort,
                                                    ?DUMMY_PV1_EPOCH,
                                                    Prefix, Chunk1_badcs),
        {error, bad_checksum} = ?FLU_C:write_chunk(Host, TcpPort,
                                                   ?DUMMY_PV1_EPOCH,
                                                   <<"foo-file">>, 99832,
                                                   Chunk1_badcs),
        ok
    after
        ok = ?FLU:stop(FLU1)
    end.

%% The purpose of timing_pb_encoding_test_ and timing_bif_encoding_test_ is
%% to show the relative speed of the PB encoding of something like a
%% projection store command is about 35x slower than simply using the Erlang
%% BIFs term_to_binary() and binary_to_term().  We try to do enough work, at
%% least a couple of seconds, so that any dynamic CPU voltage adjustment
%% might kick into highest speed, in theory.

timing_pb_encoding_test_() ->
    {timeout, 60, fun() -> timing_pb_encoding_test2() end}.

timing_pb_encoding_test2() ->
    P_a = #p_srvr{name=a, address="localhost", port=4321},
    P1 = machi_projection:new(1, a, [P_a], [], [a], [], []),
    DoIt1 = fun() ->
                    Req = machi_pb_wrap:make_projection_req(
                            <<1,2,3,4>>, {write_projection, public, P1}),
                    Bin = list_to_binary(machi_pb:encode_mpb_ll_request(Req)),
                    ZZ = machi_pb:decode_mpb_ll_request(Bin),
                    _ = machi_pb_wrap:unmake_projection_req(ZZ)
            end,
    XX = lists:seq(1,70*1000),
    erlang:garbage_collect(),
    RUN1 = timer:tc(fun() -> begin [_ = DoIt1() || _ <- XX], ok end end),
    erlang:garbage_collect(),

    DoIt2 = fun() ->
                   Req = term_to_binary({
                           <<1,2,3,4>>, {write_projection, public, P1}}),
                   _ = binary_to_term(Req)
           end,
    erlang:garbage_collect(),
    RUN2 = timer:tc(fun() -> begin [_ = DoIt2() || _ <- XX], ok end end),
    erlang:garbage_collect(),
    Factor = (element(1, RUN1) / element(1, RUN2)),
    io:format(user, " speed factor=~.2f ", [Factor]),
    ok.

-endif. % !PULSE
-endif. % TEST
