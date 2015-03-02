#!/usr/bin/env escript
%% -*- erlang -*-
%%! +A 0 -smp disable -noinput -noshell -pz .

%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(file0_cc_ec_encode).
-compile(export_all).
-mode(compile). % for escript use

-define(NO_MODULE, true).
-include("./file0.erl").

-include_lib("kernel/include/file.hrl").

-define(ENCODER, "./jerasure.`uname -s`/bin/enc-dec-wrapper.sh encoder").
-define(ENCODER_RS_10_4_ARGS, "10 4 cauchy_good 8 10240 102400").

main([]) ->
    io:format("Use:  Erasure code a file and store all sub-chunks to a different projection (specifically for EC use)\n"),
    io:format("Args: RegularProjection LocalDataPath EncodingTmpDir EC_Projection [verbose | check | repair | progress | delete-source | nodelete-tmp ]\n"),
    erlang:halt(1);
main([RegularProjection, LocalDataPath, TmpDir0, EcProjection|Args]) ->
    Ps = make_repair_props(Args),
    TmpDir = TmpDir0 ++ "/mytmp." ++ os:getpid(),
    %% Hash on entire file name, *not* prefix
    FileStr = filename:basename(LocalDataPath),
    {_Chain, StripeMembers0} = calc_chain(write, EcProjection, FileStr),
    StripeMembers = [{binary_to_list(Host), integer_to_list(Port)} ||
                        {Host, Port} <- StripeMembers0],
    verb("Stripe members: ~p\n", [StripeMembers]),
    os:cmd("rm -rf " ++ TmpDir),
    filelib:ensure_dir(TmpDir ++ "/unused"),

    try
        {Ks, Ms, Summary} = encode_rs_10_4(TmpDir, LocalDataPath),
        verb("Work directory: ~s\n", [TmpDir]),
        verb("Summary header:\n~s\n", [iolist_to_binary(Summary)]),
        verb("Data files:\n"),
        [verb("    ~s\n", [F]) || F <- Ks],
        verb("Parity files:\n"),
        [verb("    ~s\n", [F]) || F <- Ms],
        case proplists:get_value(mode, Ps) of
            repair ->
                verb("Writing stripes to remote servers: "),
                ok = write_rs_10_4_stripes(lists:zip(Ks ++ Ms, StripeMembers),
                                           Ps),
                verb("done\n"),
                File = list_to_binary(filename:basename(LocalDataPath)),
                ok = write_ec_summary(RegularProjection, File, Summary, Ps);
            _ ->
                ok
        end
    after
        case proplists:get_value(delete_tmp, Ps) of
            false ->
                io:format("Not deleting data in dir ~s\n",
                          [TmpDir]);
            _ ->
                os:cmd("rm -rf " ++ TmpDir)
        end
    end,
    erlang:halt(0).

write_rs_10_4_stripes([], _Ps) ->
    ok;
write_rs_10_4_stripes([{Path, {Host, PortStr}}|T], Ps) ->
    verb("."),
    Sock = escript_connect(Host, PortStr),
    try
        File = list_to_binary(filename:basename(Path)),
        ok = write_stripe_file(Path, File, Sock),
        write_rs_10_4_stripes(T, Ps)
    after
        ok = gen_tcp:close(Sock)
    end.

write_stripe_file(Path, File, Sock) ->
    {ok, FH} = file:open(Path, [raw, binary, read]),
    try
        ok = write_stripe_file(0, file:read(FH, 1024*1024),
                               FH, Path, File, Sock)
    after
        file:close(FH)
    end.
    
    
write_stripe_file(Offset, {ok, Chunk}, FH, Path, File, Sock) ->
    %% OffsetHex = bin_to_hexstr(<<Offset:64/big>>),
    %% Len = byte_size(Chunk),
    %% LenHex = bin_to_hexstr(<<Len:32/big>>),
    {_,_,_} = upload_chunk_write(Sock, Offset, File, Chunk),
    write_stripe_file(Offset + byte_size(Chunk), file:read(FH, 1024*1024),
                      FH, Path, File, Sock);
write_stripe_file(_Offset, eof, _FH, _Path, _File, _Sock) ->
    ok.

encode_rs_10_4(TmpDir, LocalDataPath) ->
    Cmd = ?ENCODER ++ " " ++ TmpDir ++ " " ++ LocalDataPath ++ " " ++
        ?ENCODER_RS_10_4_ARGS,
    case os:cmd(Cmd) of
        "0\n" ->
            Ks = [TmpDir ++ "/" ++ X || X <- filelib:wildcard("*_k??", TmpDir)],
            Ms = [TmpDir ++ "/" ++ X || X <- filelib:wildcard("*_m??", TmpDir)],
            [Meta] = filelib:wildcard("*_meta.txt", TmpDir),
            {ok, MetaBin} = file:read_file(TmpDir ++ "/" ++ Meta),
            Summary = make_ec_summary(rs_10_4_v1, MetaBin, Ks, Ms),
            {Ks, Ms, Summary};
        Else ->
            io:format("XX ~s\n", [Else]),
            {error, Else}
    end.

make_ec_summary(rs_10_4_v1, MetaBin, Ks, _Ms) ->
    [Meta1Path, OrigFileLenStr|Metas] =
        string:tokens(binary_to_list(MetaBin), [10]),
    Meta1 = filename:basename(Meta1Path),
    OrigFileLen = list_to_integer(OrigFileLenStr),
    {ok, FI} = file:read_file_info(hd(Ks)),
    StripeWidth = FI#file_info.size,
    Body = iolist_to_binary([string:join([Meta1,OrigFileLenStr|Metas], "\n"),
                             "\n"]),
    BodyLen = byte_size(Body),
    Hdr1 = iolist_to_binary(["a ", bin_to_hexstr(<<BodyLen:16/big>>),
                             " ",
                             bin_to_hexstr(<<StripeWidth:64/big>>),
                             " ",
                             bin_to_hexstr(<<OrigFileLen:64/big>>),
                             " ",
                             "rs_10_4_v1"
                            ]),
    SpacePadding = 80 - 1 - byte_size(Hdr1),
    Hdr2 = lists:duplicate(SpacePadding, 32),
    [Hdr1, Hdr2, 10, Body].

write_ec_summary(ProjectionPath, File, Summary, _Ps) ->
    Prefix = re:replace(File, "\\..*", "", [{return, binary}]),
    {_Chain, RawHPs} = calc_chain(write, ProjectionPath, Prefix),
    HP_tuples = [{binary_to_list(Host), integer_to_list(Port)} ||
                    {Host, Port} <- RawHPs],
    verb("Writing coding summary records: "),
    [begin
         verb("<"),
         Sock = escript_connect(Host, Port),
         try
             {_,_,_} = upload_chunk_write(Sock, 0, File,
                                          iolist_to_binary(Summary)),
             ok = gen_tcp:send(Sock, [<<"TRUNC-hack--- ">>, File, <<"\n">>]),
             inet:setopts(Sock, [packet, line]),
             {ok, <<"OK\n">>} = gen_tcp:recv(Sock, 0)
         after
             verb(">"),
             gen_tcp:close(Sock)
         end
     end || {Host, Port} <- HP_tuples],
    verb(" done\n"),
    ok.

