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

-module(machi_flu1).

-include_lib("kernel/include/file.hrl").

-include("machi.hrl").

-export([start_link/1, stop/1]).

start_link([{FluName, TcpPort, DataDir}])
  when is_atom(FluName), is_integer(TcpPort), is_list(DataDir) ->
    {ok, spawn_link(fun() -> main2(FluName, TcpPort, DataDir) end)}.

stop(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! forever,
            ok;
        false ->
            error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

main2(RegName, TcpPort, DataDir) ->
    _Pid1 = start_listen_server(RegName, TcpPort, DataDir),
    _Pid2 = start_append_server(RegName, DataDir),
    receive forever -> ok end.

start_listen_server(RegName, TcpPort, DataDir) ->
    spawn_link(fun() -> run_listen_server(RegName, TcpPort, DataDir) end).

start_append_server(Name, DataDir) ->
    spawn_link(fun() -> run_append_server(Name, DataDir) end).

run_listen_server(RegName, TcpPort, DataDir) ->
    SockOpts = [{reuseaddr, true},
                {mode, binary}, {active, false}, {packet, line}],
    {ok, LSock} = gen_tcp:listen(TcpPort, SockOpts),
    listen_server_loop(RegName, LSock, DataDir).

run_append_server(Name, DataDir) ->
    register(Name, self()),
    append_server_loop(DataDir).

listen_server_loop(RegName, LSock, DataDir) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    spawn(fun() -> net_server_loop(RegName, Sock, DataDir) end),
    listen_server_loop(RegName, LSock, DataDir).

append_server_loop(DataDir) ->
    receive
        {seq_append, From, Prefix, Chunk, CSum} ->
            spawn(fun() -> append_server_dispatch(From, Prefix, Chunk, CSum,
                                                  DataDir) end),
            append_server_loop(DataDir)
    end.

net_server_loop(RegName, Sock, DataDir) ->
    ok = inet:setopts(Sock, [{packet, line}]),
    case gen_tcp:recv(Sock, 0, 60*1000) of
        {ok, Line} ->
            %% machi_util:verb("Got: ~p\n", [Line]),
            PrefixLenLF = byte_size(Line)   - 2 - 8 - 1 - 1,
            PrefixLenCRLF = byte_size(Line) - 2 - 8 - 1 - 2,
            FileLenLF = byte_size(Line)   - 2 - 16 - 1 - 8 - 1 - 1,
            FileLenCRLF = byte_size(Line) - 2 - 16 - 1 - 8 - 1 - 2,
            CSumFileLenLF = byte_size(Line) - 2 - 1,
            CSumFileLenCRLF = byte_size(Line) - 2 - 2,
            WriteFileLenLF = byte_size(Line) - 7 - 16 - 1 - 8 - 1 - 1,
            DelFileLenLF = byte_size(Line) - 14 - 1,
            case Line of
                %% For normal use
                <<"A ", LenHex:8/binary, " ",
                  Prefix:PrefixLenLF/binary, "\n">> ->
                    do_net_server_append(RegName, Sock, LenHex, Prefix);
                <<"A ", LenHex:8/binary, " ",
                  Prefix:PrefixLenCRLF/binary, "\r\n">> ->
                    do_net_server_append(RegName, Sock, LenHex, Prefix);
                <<"R ", OffsetHex:16/binary, " ", LenHex:8/binary, " ",
                  File:FileLenLF/binary, "\n">> ->
                    do_net_server_read(Sock, OffsetHex, LenHex, File, DataDir);
                <<"R ", OffsetHex:16/binary, " ", LenHex:8/binary, " ",
                  File:FileLenCRLF/binary, "\r\n">> ->
                    do_net_server_read(Sock, OffsetHex, LenHex, File, DataDir);
                <<"L\n">> ->
                    do_net_server_listing(Sock, DataDir);
                <<"L\r\n">> ->
                    do_net_server_listing(Sock, DataDir);
                <<"C ", File:CSumFileLenLF/binary, "\n">> ->
                    do_net_server_checksum_listing(Sock, File, DataDir);
                <<"C ", File:CSumFileLenCRLF/binary, "\n">> ->
                    do_net_server_checksum_listing(Sock, File, DataDir);
                <<"QUIT\n">> ->
                    catch gen_tcp:close(Sock),
                    exit(normal);
                <<"QUIT\r\n">> ->
                    catch gen_tcp:close(Sock),
                    exit(normal);
                %% For "internal" replication only.
                <<"W-repl ", OffsetHex:16/binary, " ", LenHex:8/binary, " ",
                  File:WriteFileLenLF/binary, "\n">> ->
                    do_net_server_write(Sock, OffsetHex, LenHex, File, DataDir);
                %% For data migration only.
                <<"DEL-migration ", File:DelFileLenLF/binary, "\n">> ->
                    do_net_server_delete_migration_only(Sock, File, DataDir);
                %% For erasure coding hackityhack
                <<"TRUNC-hack--- ", File:DelFileLenLF/binary, "\n">> ->
                    do_net_server_truncate_hackityhack(Sock, File, DataDir);
                _ ->
                    machi_util:verb("Else Got: ~p\n", [Line]),
                    gen_tcp:send(Sock, "ERROR SYNTAX\n"),
                    catch gen_tcp:close(Sock),
                    exit(normal)
            end,
            net_server_loop(RegName, Sock, DataDir);
        _ ->
            catch gen_tcp:close(Sock),
            exit(normal)
    end.

append_server_dispatch(From, Prefix, Chunk, CSum, DataDir) ->
    Pid = write_server_get_pid(Prefix, DataDir),
    Pid ! {seq_append, From, Prefix, Chunk, CSum},
    exit(normal).

do_net_server_append(RegName, Sock, LenHex, Prefix) ->
    %% TODO: robustify against other invalid path characters such as NUL
    case sanitize_file_string(Prefix) of
        ok ->
            do_net_server_append2(RegName, Sock, LenHex, Prefix);
        _ ->
            ok = gen_tcp:send(Sock, <<"ERROR BAD-ARG">>)
    end.

sanitize_file_string(Str) ->
    case re:run(Str, "/") of
        nomatch ->
            ok;
        _ ->
            error
    end.

do_net_server_append2(RegName, Sock, LenHex, Prefix) ->
    <<Len:32/big>> = machi_util:hexstr_to_bin(LenHex),
    ok = inet:setopts(Sock, [{packet, raw}]),
    {ok, Chunk} = gen_tcp:recv(Sock, Len, 60*1000),
    CSum = machi_util:checksum(Chunk),
    try
        RegName ! {seq_append, self(), Prefix, Chunk, CSum}
    catch error:badarg ->
            error_logger:error_msg("Message send to ~p gave badarg, make certain server is running with correct registered name\n", [?MODULE])
    end,
    receive
        {assignment, Offset, File} ->
            OffsetHex = machi_util:bin_to_hexstr(<<Offset:64/big>>),
            Out = io_lib:format("OK ~s ~s\n", [OffsetHex, File]),
            ok = gen_tcp:send(Sock, Out)
    after 10*1000 ->
            ok = gen_tcp:send(Sock, "TIMEOUT\n")
    end.

do_net_server_read(Sock, OffsetHex, LenHex, FileBin, DataDir) ->
    DoItFun = fun(FH, Offset, Len) ->
                      case file:pread(FH, Offset, Len) of
                          {ok, Bytes} when byte_size(Bytes) == Len ->
                              gen_tcp:send(Sock, ["OK\n", Bytes]);
                          {ok, Bytes} ->
                              machi_util:verb("ok read but wanted ~p  got ~p: ~p @ offset ~p\n",
                                  [Len, size(Bytes), FileBin, Offset]),
                              ok = gen_tcp:send(Sock, "ERROR PARTIAL-READ\n");
                          eof ->
                              perhaps_do_net_server_ec_read(Sock, FH);
                          _Else2 ->
                              machi_util:verb("Else2 ~p ~p ~P\n",
                                              [Offset, Len, _Else2, 20]),
                              ok = gen_tcp:send(Sock, "ERROR BAD-READ\n")
                      end
              end,
    do_net_server_readwrite_common(Sock, OffsetHex, LenHex, FileBin, DataDir,
                                   [read, binary, raw], DoItFun).

do_net_server_readwrite_common(Sock, OffsetHex, LenHex, FileBin, DataDir,
                               FileOpts, DoItFun) ->
    case sanitize_file_string(FileBin) of
        ok ->
            do_net_server_readwrite_common2(Sock, OffsetHex, LenHex, FileBin,
                                            DataDir, FileOpts, DoItFun);
        _ ->
            ok = gen_tcp:send(Sock, <<"ERROR BAD-ARG\n">>)
    end.

do_net_server_readwrite_common2(Sock, OffsetHex, LenHex, FileBin, DataDir,
                                FileOpts, DoItFun) ->
    <<Offset:64/big>> = machi_util:hexstr_to_bin(OffsetHex),
    <<Len:32/big>> = machi_util:hexstr_to_bin(LenHex),
    {_, Path} = machi_util:make_data_filename(DataDir, FileBin),
    OptsHasWrite = lists:member(write, FileOpts),
    case file:open(Path, FileOpts) of
        {ok, FH} ->
            try
                DoItFun(FH, Offset, Len)
            after
                file:close(FH)
            end;
        {error, enoent} when OptsHasWrite ->
            ok = filelib:ensure_dir(Path),
            do_net_server_readwrite_common(
              Sock, OffsetHex, LenHex, FileBin, DataDir,
              FileOpts, DoItFun);
        _Else ->
            %%%%%% keep?? machi_util:verb("Else ~p ~p ~p ~p\n", [Offset, Len, Path, _Else]),
            ok = gen_tcp:send(Sock, <<"ERROR BAD-IO\n">>)
    end.


do_net_server_write(Sock, OffsetHex, LenHex, FileBin, DataDir) ->
    CSumPath = machi_util:make_checksum_filename(DataDir, FileBin),
    case file:open(CSumPath, [append, raw, binary, delayed_write]) of
        {ok, FHc} ->
            do_net_server_write2(Sock, OffsetHex, LenHex, FileBin, DataDir, FHc);
        {error, enoent} ->
            ok = filelib:ensure_dir(CSumPath),
            do_net_server_write(Sock, OffsetHex, LenHex, FileBin, DataDir)
    end.

do_net_server_write2(Sock, OffsetHex, LenHex, FileBin, DataDir, FHc) ->
    DoItFun = fun(FHd, Offset, Len) ->
                      ok = inet:setopts(Sock, [{packet, raw}]),
                      {ok, Chunk} = gen_tcp:recv(Sock, Len),
                      CSum = machi_util:checksum(Chunk),
                      case file:pwrite(FHd, Offset, Chunk) of
                          ok ->
                              CSumHex = machi_util:bin_to_hexstr(CSum),
                              CSum_info = [OffsetHex, 32, LenHex, 32, CSumHex, 10],
                              ok = file:write(FHc, CSum_info),
                              ok = file:close(FHc),
                              gen_tcp:send(Sock, <<"OK\n">>);
                          _Else3 ->
                              machi_util:verb("Else3 ~p ~p ~p\n",
                                              [Offset, Len, _Else3]),
                              ok = gen_tcp:send(Sock, "ERROR BAD-PWRITE\n")
                      end
              end,
    do_net_server_readwrite_common(Sock, OffsetHex, LenHex, FileBin, DataDir,
                                   [write, read, binary, raw], DoItFun).

perhaps_do_net_server_ec_read(Sock, FH) ->
    case file:pread(FH, 0, ?MINIMUM_OFFSET) of
        {ok, Bin} when byte_size(Bin) == ?MINIMUM_OFFSET ->
            decode_and_reply_net_server_ec_read(Sock, Bin);
        {ok, _AnythingElse} ->
            ok = gen_tcp:send(Sock, "ERROR PARTIAL-READ2\n");
        _AnythingElse ->
            ok = gen_tcp:send(Sock, "ERROR BAD-PREAD\n")
    end.

decode_and_reply_net_server_ec_read(Sock, <<"a ", Rest/binary>>) ->
    decode_and_reply_net_server_ec_read_version_a(Sock, Rest);
decode_and_reply_net_server_ec_read(Sock, <<0:8, _/binary>>) ->
    ok = gen_tcp:send(Sock, <<"ERROR NOT-ERASURE\n">>).

decode_and_reply_net_server_ec_read_version_a(Sock, Rest) ->
    %% <<BodyLenHex:4/binary, " ", StripeWidthHex:16/binary, " ",
    %%   OrigFileLenHex:16/binary, " ", _/binary>> = Rest,
    HdrLen = 80 - 2 - 4 - 1,
    <<BodyLenHex:4/binary, " ", Hdr:HdrLen/binary, Rest2/binary>> = Rest,
    <<BodyLen:16/big>> = machi_util:hexstr_to_bin(BodyLenHex),
    <<Body:BodyLen/binary, _/binary>> = Rest2,
    ok = gen_tcp:send(Sock, ["ERASURE ", BodyLenHex, " ", Hdr, Body]).

do_net_server_listing(Sock, DataDir) ->
    Files = filelib:wildcard("*", DataDir) -- ["config"],
    Out = ["OK\n",
           [begin
                {ok, FI} = file:read_file_info(DataDir ++ "/" ++ File),
                Size = FI#file_info.size,
                SizeBin = <<Size:64/big>>,
                [machi_util:bin_to_hexstr(SizeBin), <<" ">>,
                 list_to_binary(File), <<"\n">>]
            end || File <- Files],
           ".\n"
          ],
    ok = gen_tcp:send(Sock, Out).

do_net_server_checksum_listing(Sock, File, DataDir) ->
    case sanitize_file_string(File) of
        ok ->
            do_net_server_checksum_listing2(Sock, File, DataDir);
        _ ->
            ok = gen_tcp:send(Sock, <<"ERROR BAD-ARG\n">>)
    end.

do_net_server_checksum_listing2(Sock, File, DataDir) ->
    ok = sync_checksum_file(File),

    CSumPath = machi_util:make_checksum_filename(DataDir, File),
    case file:open(CSumPath, [read, raw, binary]) of
        {ok, FH} ->
            {ok, FI} = file:read_file_info(CSumPath),
            Len = FI#file_info.size,
            LenHex = list_to_binary(machi_util:bin_to_hexstr(<<Len:64/big>>)),
            %% Client has option of line-by-line with "." terminator,
            %% or using the offset in the OK message to slurp things
            %% down by exact byte size.
            ok = gen_tcp:send(Sock, [<<"OK ">>, LenHex, <<"\n">>]),
            do_net_copy_bytes(FH, Sock),
            ok = file:close(FH),
            ok = gen_tcp:send(Sock, ".\n");
        {error, enoent} ->
            ok = gen_tcp:send(Sock, "ERROR NO-SUCH-FILE\n");
        _ ->
            ok = gen_tcp:send(Sock, "ERROR\n")
    end.

sync_checksum_file(File) ->
    Prefix = re:replace(File, "\\..*", "", [{return, binary}]),
    case write_server_find_pid(Prefix) of
        undefined ->
            ok;
        Pid ->
            Ref = make_ref(),
            Pid ! {sync_stuff, self(), Ref},
            receive
                {sync_finished, Ref} ->
                    ok
            after 5000 ->
                    case write_server_find_pid(Prefix) of
                        undefined ->
                            ok;
                        Pid2 when Pid2 /= Pid ->
                            ok;
                        _Pid2 ->
                            error
                    end
            end
    end.

do_net_copy_bytes(FH, Sock) ->
    case file:read(FH, 1024*1024) of
        {ok, Bin} ->
            ok = gen_tcp:send(Sock, Bin),
            do_net_copy_bytes(FH, Sock);
        eof ->
            ok
    end.

do_net_server_delete_migration_only(Sock, File, DataDir) ->
    case sanitize_file_string(File) of
        ok ->
            do_net_server_delete_migration_only2(Sock, File, DataDir);
        _ ->
            ok = gen_tcp:send(Sock, <<"ERROR BAD-ARG\n">>)
    end.

do_net_server_delete_migration_only2(Sock, File, DataDir) ->
    {_, Path} = machi_util:make_data_filename(DataDir, File),
    case file:delete(Path) of
        ok ->
            ok = gen_tcp:send(Sock, "OK\n");
        {error, enoent} ->
            ok = gen_tcp:send(Sock, "ERROR NO-SUCH-FILE\n");
        _ ->
            ok = gen_tcp:send(Sock, "ERROR\n")
    end.

do_net_server_truncate_hackityhack(Sock, File, DataDir) ->
    case sanitize_file_string(File) of
        ok ->
            do_net_server_truncate_hackityhack2(Sock, File, DataDir);
        _ ->
            ok = gen_tcp:send(Sock, <<"ERROR BAD-ARG\n">>)
    end.

do_net_server_truncate_hackityhack2(Sock, File, DataDir) ->
    {_, Path} = machi_util:make_data_filename(DataDir, File),
    case file:open(Path, [read, write, binary, raw]) of
        {ok, FH} ->
            try
                {ok, ?MINIMUM_OFFSET} = file:position(FH, ?MINIMUM_OFFSET),
                ok = file:truncate(FH),
                ok = gen_tcp:send(Sock, "OK\n")
            after
                file:close(FH)
            end;
        {error, enoent} ->
            ok = gen_tcp:send(Sock, "ERROR NO-SUCH-FILE\n");
        _ ->
            ok = gen_tcp:send(Sock, "ERROR\n")
    end.

write_server_get_pid(Prefix, DataDir) ->
    case write_server_find_pid(Prefix) of
        undefined ->
            start_seq_append_server(Prefix, DataDir),
            timer:sleep(1),
            write_server_get_pid(Prefix, DataDir);
        Pid ->
            Pid
    end.

write_server_find_pid(Prefix) ->
    RegName = machi_util:make_regname(Prefix),
    whereis(RegName).

start_seq_append_server(Prefix, DataDir) ->
    spawn_link(fun() -> run_seq_append_server(Prefix, DataDir) end).

run_seq_append_server(Prefix, DataDir) ->
    true = register(machi_util:make_regname(Prefix), self()),
    ok = filelib:ensure_dir(DataDir ++ "/unused"),
    ok = filelib:ensure_dir(DataDir ++ "/config/unused"),
    run_seq_append_server2(Prefix, DataDir).

run_seq_append_server2(Prefix, DataDir) ->
    FileNum = machi_util:read_max_filenum(DataDir, Prefix) + 1,
    case machi_util:increment_max_filenum(DataDir, Prefix) of
        ok ->
            machi_util:increment_max_filenum(DataDir, Prefix),
            machi_util:info_msg("start: ~p server at file ~w\n",
                                [Prefix, FileNum]),
            seq_append_server_loop(DataDir, Prefix, FileNum);
        Else ->
            error_logger:error_msg("start: ~p server at file ~w: ~p\n",
                                   [Prefix, FileNum, Else]),
            exit(Else)

    end.

seq_append_server_loop(DataDir, Prefix, FileNum) ->
    SequencerNameHack = lists:flatten(io_lib:format(
                                        "~.36B~.36B",
                                        [element(3,now()),
                                         list_to_integer(os:getpid())])),
    {File, FullPath} = machi_util:make_data_filename(
                         DataDir, Prefix, SequencerNameHack, FileNum),
    {ok, FHd} = file:open(FullPath,
                          [write, binary, raw]),
                          %% [write, binary, raw, delayed_write]),
    CSumPath = machi_util:make_checksum_filename(
                 DataDir, Prefix, SequencerNameHack, FileNum),
    {ok, FHc} = file:open(CSumPath, [append, raw, binary, delayed_write]),
    seq_append_server_loop(DataDir, Prefix, File, {FHd,FHc}, FileNum,
                           ?MINIMUM_OFFSET).

seq_append_server_loop(DataDir, Prefix, _File, {FHd,FHc}, FileNum, Offset)
  when Offset > ?MAX_FILE_SIZE ->
    ok = file:close(FHd),
    ok = file:close(FHc),
    machi_util:info_msg("rollover: ~p server at file ~w offset ~w\n",
                        [Prefix, FileNum, Offset]),
    run_seq_append_server2(Prefix, DataDir);    
seq_append_server_loop(DataDir, Prefix, File, {FHd,FHc}=FH_, FileNum, Offset) ->
    receive
        {seq_append, From, Prefix, Chunk, CSum} ->
            ok = file:pwrite(FHd, Offset, Chunk),
            From ! {assignment, Offset, File},
            Len = byte_size(Chunk),
            OffsetHex = machi_util:bin_to_hexstr(<<Offset:64/big>>),
            LenHex = machi_util:bin_to_hexstr(<<Len:32/big>>),
            CSumHex = machi_util:bin_to_hexstr(CSum),
            CSum_info = [OffsetHex, 32, LenHex, 32, CSumHex, 10],
            ok = file:write(FHc, CSum_info),
            seq_append_server_loop(DataDir, Prefix, File, FH_,
                                   FileNum, Offset + Len);
        {sync_stuff, FromPid, Ref} ->
            file:sync(FHc),
            FromPid ! {sync_finished, Ref},
            seq_append_server_loop(DataDir, Prefix, File, FH_,
                                   FileNum, Offset)
    after 30*1000 ->
            ok = file:close(FHd),
            ok = file:close(FHc),
            machi_util:info_msg("stop: ~p server at file ~w offset ~w\n",
                                [Prefix, FileNum, Offset]),
            exit(normal)
    end.

