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

get_env_vars(App, Ks) ->
    Raw = [application:get_env(App, K) || K <- Ks],
    Old = lists:zip(Ks, Raw),
    {App, Old}.

clean_up_env_vars({App, Old}) ->
    [case Res of
         undefined ->
             application:unset_env(App, K);
         {ok, V} ->
             application:set_env(App, K, V)
     end || {K, Res} <- Old].

filter_env_var({ok, V}) -> V;
filter_env_var(Else)    -> Else.

clean_up_data_dir(DataDir) ->
    [begin
         Fs = filelib:wildcard(DataDir ++ Glob),
         [file:delete(F) || F <- Fs],
         [file:del_dir(F) || F <- Fs]
     end || Glob <- ["*/*/*/*", "*/*/*", "*/*", "*"] ],
    _ = file:del_dir(DataDir),
    ok.

start_flu_package(RegName, TcpPort, DataDir) ->
    start_flu_package(RegName, TcpPort, DataDir, []).

start_flu_package(RegName, TcpPort, DataDir, Props) ->
    case proplists:get_value(save_data_dir, Props) of
        true ->
            ok;
        _ ->
            clean_up_data_dir(DataDir)
    end,

    maybe_start_sup(),
    machi_flu_psup:start_flu_package(RegName, TcpPort, DataDir, Props).

stop_flu_package(FluName) ->
    machi_flu_psup:stop_flu_package(FluName),
    Pid = whereis(machi_sup),
    exit(Pid, normal),
    machi_util:wait_for_death(Pid, 100).

maybe_start_sup() ->
    case whereis(machi_sup) of
        undefined ->
            machi_sup:start_link(),
            %% evil but we have to let stuff start up
            timer:sleep(10),
            maybe_start_sup();
        Pid -> Pid
    end.

-ifndef(PULSE).

flu_smoke_test() ->
    Host = "localhost",
    TcpPort = 12957,
    DataDir = "./data",
    NSInfo = undefined,
    NoCSum = <<>>,
    Prefix = <<"prefix!">>,
    BadPrefix = BadFile = "no/good",
    W_props = [{initial_wedged, false}],
    {_, _, _} = machi_test_util:start_flu_package(smoke_flu, TcpPort, DataDir, W_props),
    try
        Msg = "Hello, world!",
        Msg = ?FLU_C:echo(Host, TcpPort, Msg),
        {error, bad_arg} = ?FLU_C:checksum_list(Host, TcpPort,"does-not-exist"),
        {error, bad_arg} = ?FLU_C:checksum_list(Host, TcpPort, BadFile),

        {ok, []} = ?FLU_C:list_files(Host, TcpPort, ?DUMMY_PV1_EPOCH),
        {ok, {false, _,_,_}} = ?FLU_C:wedge_status(Host, TcpPort),

        Chunk1 = <<"yo!">>,
        {ok, {Off1,Len1,File1}} = ?FLU_C:append_chunk(Host, TcpPort, NSInfo,
                                                      ?DUMMY_PV1_EPOCH,
                                                      Prefix, Chunk1, NoCSum),
        {ok, {[{_, Off1, Chunk1, _}], _}} = ?FLU_C:read_chunk(Host, TcpPort,
                                                       NSInfo, ?DUMMY_PV1_EPOCH,
                                                       File1, Off1, Len1,
                                                       noopt),
        {ok, KludgeBin} = ?FLU_C:checksum_list(Host, TcpPort, File1),
        true = is_binary(KludgeBin),
        {error, bad_arg} = ?FLU_C:append_chunk(Host, TcpPort, NSInfo,
                                               ?DUMMY_PV1_EPOCH,
                                               BadPrefix, Chunk1, NoCSum),
        {ok, [{_,File1}]} = ?FLU_C:list_files(Host, TcpPort, ?DUMMY_PV1_EPOCH),
        Len1 = size(Chunk1),
        {error, not_written} = ?FLU_C:read_chunk(Host, TcpPort,
                                                 NSInfo, ?DUMMY_PV1_EPOCH,
                                                 File1, Off1*983829323, Len1,
                                                 noopt),
        %% XXX FIXME
        %%
        %% This is failing because the read extends past the end of the file.
        %% I guess the semantic here is that we should consider any read which
        %% *starts* at a valid offset to be a partial read, even if the length
        %% of the read will cause it to fail.
        %%
        %% {error, partial_read} = ?FLU_C:read_chunk(Host, TcpPort,
        %%                                           NSInfo, ?DUMMY_PV1_EPOCH,
        %%                                           File1, Off1, Len1*9999),

        {ok, {Off1b,Len1b,File1b}} = ?FLU_C:append_chunk(Host, TcpPort, NSInfo,
                                                         ?DUMMY_PV1_EPOCH,
                                                         Prefix, Chunk1,NoCSum),
        Extra = 42,
        Opts1 = #append_opts{chunk_extra=Extra},
        {ok, {Off1c,Len1c,File1c}} = ?FLU_C:append_chunk(Host, TcpPort, NSInfo,
                                                         ?DUMMY_PV1_EPOCH,
                                                         Prefix, Chunk1, NoCSum,
                                                         Opts1, infinity),
        {ok, {Off1d,Len1d,File1d}} = ?FLU_C:append_chunk(Host, TcpPort,
                                                         NSInfo,
                                                         ?DUMMY_PV1_EPOCH,
                                                         Prefix, Chunk1,NoCSum),
        if File1b == File1c, File1c == File1d ->
                true = (Off1c == Off1b + Len1b),
                true = (Off1d == Off1c + Len1c + Extra);
           true ->
                exit(not_mandatory_but_test_expected_same_file_fixme)
        end,

        Chunk2 = <<"yo yo">>,
        Len2 = byte_size(Chunk2),
        Off2 = ?MINIMUM_OFFSET + 77,
        File2 = "smoke-whole-file^^0^1^1",
        ok = ?FLU_C:write_chunk(Host, TcpPort, NSInfo, ?DUMMY_PV1_EPOCH,
                                File2, Off2, Chunk2, NoCSum),
        {error, bad_arg} = ?FLU_C:write_chunk(Host, TcpPort, NSInfo, ?DUMMY_PV1_EPOCH,
                                              BadFile, Off2, Chunk2, NoCSum),
        {ok, {[{_, Off2, Chunk2, _}], _}} =
            ?FLU_C:read_chunk(Host, TcpPort, NSInfo, ?DUMMY_PV1_EPOCH, File2, Off2, Len2, noopt),
        {error, bad_arg} = ?FLU_C:read_chunk(Host, TcpPort,
                                                 NSInfo, ?DUMMY_PV1_EPOCH,
                                                 "no!!", Off2, Len2, noopt),
        {error, bad_arg} = ?FLU_C:read_chunk(Host, TcpPort,
                                             NSInfo, ?DUMMY_PV1_EPOCH,
                                             BadFile, Off2, Len2, noopt),

        %% Make a connected socket.
        Sock1 = ?FLU_C:connect(#p_srvr{address=Host, port=TcpPort}),

        %% Let's test some cluster version enforcement.
        Good_EpochNum = 0,
        Good_NSVersion = 0,
        Good_NS = <<>>,
        {ok, {false, {Good_EpochNum,_}, Good_NSVersion, GoodNS}} =
            ?FLU_C:wedge_status(Sock1),
        NS_good = #ns_info{version=Good_NSVersion, name=Good_NS},
        {ok, {[{_, Off2, Chunk2, _}], _}} =
            ?FLU_C:read_chunk(Sock1, NS_good, ?DUMMY_PV1_EPOCH,
                              File2, Off2, Len2, noopt),
        NS_bad_version = #ns_info{version=1, name=Good_NS},
        NS_bad_name = #ns_info{version=Good_NSVersion, name= <<"foons">>},
        {error, bad_epoch} =
            ?FLU_C:read_chunk(Sock1, NS_bad_version, ?DUMMY_PV1_EPOCH,
                              File2, Off2, Len2, noopt),
        {error, bad_arg} =
            ?FLU_C:read_chunk(Sock1, NS_bad_name, ?DUMMY_PV1_EPOCH,
                              File2, Off2, Len2, noopt),

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

        ok = ?FLU_C:quit(Sock1)
    after
        machi_test_util:stop_flu_package()
    end.

flu_projection_smoke_test() ->
    Host = "localhost",
    TcpPort = 12959,
    DataDir = "./data.projst",
    {_,_,_} = machi_test_util:start_flu_package(projection_test_flu, TcpPort, DataDir),
    try
        [ok = flu_projection_common(Host, TcpPort, T) ||
            T <- [public, private] ]
%% ,        {ok, {false, EpochID1,_,_}} = ?FLU_C:wedge_status(Host, TcpPort),
%% io:format(user, "EpochID1 ~p\n", [EpochID1])
    after
        machi_test_util:stop_flu_package()
    end.

flu_projection_common(Host, TcpPort, T) ->
    {ok, {0,_}} = ?FLU_C:get_latest_epochid(Host, TcpPort, T),
    {ok, #projection_v1{epoch_number=0}} =
        ?FLU_C:read_latest_projection(Host, TcpPort, T),
    {ok, [0]} = ?FLU_C:list_all_projections(Host, TcpPort, T),
    {ok, [#projection_v1{epoch_number=0}]} =
        ?FLU_C:get_all_projections(Host, TcpPort, T),

    P_a = #p_srvr{name=a, address="localhost", port=4321},
    P1 = machi_projection:new(1, a, [P_a], [], [a], [], []),
    ok = ?FLU_C:write_projection(Host, TcpPort, T, P1),
    case ?FLU_C:write_projection(Host, TcpPort, T, P1) of
        {error, written} when T == public  -> ok;
        ok               when T == private -> ok
    end,
    {ok, P1} = ?FLU_C:read_projection(Host, TcpPort, T, 1),
    {ok, {1,_}} = ?FLU_C:get_latest_epochid(Host, TcpPort, T),
    {ok, P1} = ?FLU_C:read_latest_projection(Host, TcpPort, T),
    {ok, [0,1]} = ?FLU_C:list_all_projections(Host, TcpPort, T),
    {ok, [_,P1]} = ?FLU_C:get_all_projections(Host, TcpPort, T),
    {error, not_written} = ?FLU_C:read_projection(Host, TcpPort, T, 2),
    ok.

bad_checksum_test() ->
    Host = "localhost",
    TcpPort = 12960,
    DataDir = "./data.bct",
    Opts = [{initial_wedged, false}],
    {_,_,_} = machi_test_util:start_flu_package(projection_test_flu, TcpPort, DataDir, Opts),
    NSInfo = undefined,
    try
        Prefix = <<"some prefix">>,
        Chunk1 = <<"yo yo yo">>,
        BadCSum = {?CSUM_TAG_CLIENT_SHA, crypto:hash(sha, ".................")},
        {error, bad_checksum} = ?FLU_C:append_chunk(Host, TcpPort, NSInfo,
                                                    ?DUMMY_PV1_EPOCH,
                                                    Prefix,
                                                    Chunk1, BadCSum),
        ok
    after
        machi_test_util:stop_flu_package()
    end.

witness_test() ->
    Host = "localhost",
    TcpPort = 12961,
    DataDir = "./data.witness",
    Opts = [{initial_wedged, false}, {witness_mode, true}],
    {_,_,_} = machi_test_util:start_flu_package(projection_test_flu, TcpPort, DataDir, Opts),
    NSInfo = undefined,
    NoCSum = <<>>,
    try
        Prefix = <<"some prefix">>,
        Chunk1 = <<"yo yo yo">>,

        %% All of the projection commands are ok.
        [ok = flu_projection_common(Host, TcpPort, T) ||
            T <- [public, private] ],

        %% Projection has moved beyond initial 0, so get the current EpochID
        {ok, EpochID1} = ?FLU_C:get_latest_epochid(Host, TcpPort, private),

        %% Witness-protected ops all fail
        {error, bad_arg} = ?FLU_C:append_chunk(Host, TcpPort, NSInfo, EpochID1,
                                               Prefix, Chunk1, NoCSum),
        File = <<"foofile">>,
        {error, bad_arg} = ?FLU_C:read_chunk(Host, TcpPort, NSInfo, EpochID1,
                                             File, 9999, 9999, noopt),
        {error, bad_arg} = ?FLU_C:checksum_list(Host, TcpPort, File),
        {error, bad_arg} = ?FLU_C:list_files(Host, TcpPort, EpochID1),
        {ok, {false, EpochID1,_,_}} = ?FLU_C:wedge_status(Host, TcpPort),
        {ok, _} = ?FLU_C:get_latest_epochid(Host, TcpPort, public),
        {ok, _} = ?FLU_C:read_latest_projection(Host, TcpPort, public),
        {error, not_written} = ?FLU_C:read_projection(Host, TcpPort,
                                                      public, 99999),
        %% write_projection already tested by flu_projection_common
        {ok, _} = ?FLU_C:get_all_projections(Host, TcpPort, public),
        {ok, _} = ?FLU_C:list_all_projections(Host, TcpPort, public),

        ok
    after
        machi_test_util:stop_flu_package()
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
                    Req = machi_pb_translate:to_pb_request(
                            <<1,2,3,4>>,
                            {low_proj, {write_projection, public, P1}}),
                    Bin = list_to_binary(machi_pb:encode_mpb_ll_request(Req)),
                    ZZ = machi_pb:decode_mpb_ll_request(Bin),
                    _ = machi_pb_translate:from_pb_request(ZZ)
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
    io:format(" speed factor=~.2f ", [Factor]),
    ok.

-endif. % !PULSE
-endif. % TEST
