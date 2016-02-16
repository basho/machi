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

-module(machi_pb_high_client_test).
-compile(export_all).

-ifdef(TEST).
-ifndef(PULSE).

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(C, machi_pb_high_client).

smoke_test_() ->
    {timeout, 5*60, fun() -> smoke_test2() end}.

smoke_test2() ->
    PortBase = 5720,
    ok = application:set_env(machi, max_file_size, 1024*1024),
    try
        {Ps, MgrNames, Dirs} = machi_test_util:start_flu_packages(
                                 1, PortBase, "./data.", []),
        D = orddict:from_list([{P#p_srvr.name, P} || P <- Ps]),
        M0 = hd(MgrNames),
        ok = machi_chain_manager1:set_chain_members(M0, D),
        [machi_chain_manager1:trigger_react_to_env(M0) || _ <-lists:seq(1,5)],

        {ok, Clnt} = ?C:start_link(Ps),
        try
            true = ?C:connected_p(Clnt),
            String = "yo, dawgggggggggggggggggggggggggggggggggg",
            String = ?C:echo(Clnt, String),

            %% TODO: auth() is not implemented.  Auth requires SSL.
            %% Probably ought to put client stuff that relies on SSL into
            %% a separate test module?  Or separate test func?
            {error, _} = ?C:auth(Clnt, "foo", "bar"),

            Prefix = <<"prefix">>,
            Chunk1 = <<"Hello, chunk!">>,
            NS = "",
            NoCSum = <<>>,
            Opts1 = #append_opts{},
            {ok, {Off1, Size1, File1}} =
                ?C:append_chunk(Clnt, NS, Prefix, Chunk1, NoCSum, Opts1),
            true = is_binary(File1),
            Chunk2 = "It's another chunk",
            CSum2 = {client_sha, machi_util:checksum_chunk(Chunk2)},
            {ok, {Off2, Size2, File2}} =
                ?C:append_chunk(Clnt, NS, Prefix, Chunk2, CSum2, Opts1),
            Chunk3 = ["This is a ", <<"test,">>, 32, [["Hello, world!"]]],
            File3 = File2,
            Off3 = Off2 + iolist_size(Chunk2),
            Size3 = iolist_size(Chunk3),
            ok = ?C:write_chunk(Clnt, File2, Off3, Chunk3, none),

            Reads = [{iolist_to_binary(Chunk1), File1, Off1, Size1},
                     {iolist_to_binary(Chunk2), File2, Off2, Size2},
                     {iolist_to_binary(Chunk3), File3, Off3, Size3}],
            [begin
                 File = Fl,
                 ?assertMatch({ok, {[{File, Off, Ch, _}], []}},
                              ?C:read_chunk(Clnt, Fl, Off, Sz, undefined))
             end || {Ch, Fl, Off, Sz} <- Reads],

            {ok, KludgeBin} = ?C:checksum_list(Clnt, File1),
            true = is_binary(KludgeBin),
            {ok, [{File1Size,File1}]} = ?C:list_files(Clnt),
            true = is_integer(File1Size),

            File1Bin = binary_to_list(File1),
            [begin
                 #p_srvr{name=Name, props=Props} = P,
                 Dir = proplists:get_value(data_dir, Props),
                 ?assertEqual({ok, [File1Bin]},
                              file:list_dir(filename:join([Dir, "data"]))),
                 FileListFileName = filename:join([Dir, "known_files_" ++ atom_to_list(Name)]),
                 {ok, Plist} = machi_plist:open(FileListFileName, []),
                 ?assertEqual([], machi_plist:all(Plist))
             end || P <- Ps],

            [begin
                 ok = ?C:trim_chunk(Clnt, Fl, Off, Sz)
             end || {_Ch, Fl, Off, Sz} <- Reads],
            [begin
                 {ok, {[], Trimmed}} =
                        ?C:read_chunk(Clnt, Fl, Off, Sz, #read_opts{needs_trimmed=true}),
                 Filename = Fl,
                 ?assertEqual([{Filename, Off, Sz}], Trimmed)
             end || {_Ch, Fl, Off, Sz} <- Reads],

            LargeBytes = binary:copy(<<"x">>, 1024*1024),
            LBCsum = {client_sha, machi_util:checksum_chunk(LargeBytes)},
            {ok, {Offx, Sizex, Filex}} =
                ?C:append_chunk(Clnt, NS,
                                Prefix, LargeBytes, LBCsum, Opts1),
            ok = ?C:trim_chunk(Clnt, Filex, Offx, Sizex),

            %% Make sure everything was trimmed
            File = binary_to_list(Filex),
            [begin
                 #p_srvr{name=Name, props=Props} = P,
                 Dir = proplists:get_value(data_dir, Props),
                 ?assertEqual({ok, []},
                              file:list_dir(filename:join([Dir, "data"]))),
                 FileListFileName = filename:join([Dir, "known_files_" ++ atom_to_list(Name)]),
                 {ok, Plist} = machi_plist:open(FileListFileName, []),
                 ?assertEqual([File], machi_plist:all(Plist))
             end || P <- Ps],

            [begin
                 {error, trimmed} =
                        ?C:read_chunk(Clnt, Fl, Off, Sz, undefined)
             end || {_Ch, Fl, Off, Sz} <- Reads],
            ok
        after
            (catch ?C:quit(Clnt))
        end
    after
        machi_test_util:stop_flu_packages()
    end.

-endif. % !PULSE
-endif. % TEST
