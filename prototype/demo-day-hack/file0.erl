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

-ifndef(NO_MODULE).
-module(file0).
-compile(export_all).
-endif.
%% -mode(compile). % for escript use

-include("cc.hrl").
-include_lib("kernel/include/file.hrl").

-define(MAX_FILE_SIZE, 256*1024*1024).     % 256 MBytes
%% -define(DATA_DIR, "/Volumes/SAM1/seq-tests/data").
-define(DATA_DIR, "./data").
-define(MINIMUM_OFFSET, 1024).

append(Server, Prefix, Chunk) when is_binary(Prefix), is_binary(Chunk) ->
    CSum = checksum(Chunk),
    Server ! {seq_append, self(), Prefix, Chunk, CSum},
    receive
        {assignment, Offset, File} ->
            {Offset, File}
    after 10*1000 ->
            bummer
    end.

start_append_server() ->
    start_append_server(?MODULE).

start_append_server(Name) ->
    start_append_server(Name, ?DATA_DIR).

start_append_server(Name, DataDir) ->
    spawn_link(fun() -> run_append_server(Name, DataDir) end).

start_listen_server(Port) ->
    start_listen_server(Port, ?DATA_DIR).

start_listen_server(Port, DataDir) ->
    spawn_link(fun() -> run_listen_server(Port, DataDir) end).

run_append_server(Name, DataDir) ->
    register(Name, self()),
    append_server_loop(DataDir).

run_listen_server(Port, DataDir) ->
    SockOpts = [{reuseaddr, true},
                {mode, binary}, {active, false}, {packet, line}],
    {ok, LSock} = gen_tcp:listen(Port, SockOpts),
    listen_server_loop(LSock, DataDir).

append_server_loop(DataDir) ->
    receive
        {seq_append, From, Prefix, Chunk, CSum} ->
            spawn(fun() -> append_server_dispatch(From, Prefix, Chunk, CSum,
                                                  DataDir) end),
            append_server_loop(DataDir)
    end.

listen_server_loop(LSock, DataDir) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    spawn(fun() -> net_server_loop(Sock, DataDir) end),
    listen_server_loop(LSock, DataDir).

net_server_loop(Sock, DataDir) ->
    ok = inet:setopts(Sock, [{packet, line}]),
    case gen_tcp:recv(Sock, 0, 60*1000) of
        {ok, Line} ->
            %% verb("Got: ~p\n", [Line]),
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
                    do_net_server_append(Sock, LenHex, Prefix);
                <<"A ", LenHex:8/binary, " ",
                  Prefix:PrefixLenCRLF/binary, "\r\n">> ->
                    do_net_server_append(Sock, LenHex, Prefix);
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
                    verb("Else Got: ~p\n", [Line]),
                    gen_tcp:send(Sock, "ERROR SYNTAX\n"),
                    catch gen_tcp:close(Sock),
                    exit(normal)
            end,
            net_server_loop(Sock, DataDir);
        _ ->
            catch gen_tcp:close(Sock),
            exit(normal)
    end.

do_net_server_append(Sock, LenHex, Prefix) ->
    <<Len:32/big>> = hexstr_to_bin(LenHex),
    ok = inet:setopts(Sock, [{packet, raw}]),
    {ok, Chunk} = gen_tcp:recv(Sock, Len, 60*1000),
    CSum = checksum(Chunk),
    try
        ?MODULE ! {seq_append, self(), Prefix, Chunk, CSum}
    catch error:badarg ->
            error_logger:error_msg("Message send to ~p gave badarg, make certain server is running with correct registered name\n", [?MODULE])
    end,
    receive
        {assignment, Offset, File} ->
            OffsetHex = bin_to_hexstr(<<Offset:64/big>>),
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
                              verb("ok read but wanted ~p  got ~p: ~p @ offset ~p\n",
                                  [Len, size(Bytes), FileBin, Offset]),
                              ok = gen_tcp:send(Sock, "ERROR PARTIAL-READ\n");
                          eof ->
                              perhaps_do_net_server_ec_read(Sock, FH);
                          _Else2 ->
                              verb("Else2 ~p ~p ~P\n",
                                        [Offset, Len, _Else2, 20]),
                              ok = gen_tcp:send(Sock, "ERROR BAD-READ\n")
                      end
              end,
    do_net_server_readwrite_common(Sock, OffsetHex, LenHex, FileBin, DataDir,
                                   [read, binary, raw], DoItFun).

do_net_server_readwrite_common(Sock, OffsetHex, LenHex, FileBin, DataDir,
                               FileOpts, DoItFun) ->
    <<Offset:64/big>> = hexstr_to_bin(OffsetHex),
    <<Len:32/big>> = hexstr_to_bin(LenHex),
    {_, Path} = make_data_filename(DataDir, FileBin),
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
            %%%%%% keep?? verb("Else ~p ~p ~p ~p\n", [Offset, Len, Path, _Else]),
            ok = gen_tcp:send(Sock, "ERROR BAD-IO\n")
    end.


do_net_server_write(Sock, OffsetHex, LenHex, FileBin, DataDir) ->
    CSumPath = make_checksum_filename(DataDir, FileBin),
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
                      CSum = checksum(Chunk),
                      case file:pwrite(FHd, Offset, Chunk) of
                          ok ->
                              CSumHex = bin_to_hexstr(CSum),
                              CSum_info = [OffsetHex, 32, LenHex, 32, CSumHex, 10],
                              ok = file:write(FHc, CSum_info),
                              ok = file:close(FHc),
                              gen_tcp:send(Sock, <<"OK\n">>);
                          _Else3 ->
                              verb("Else3 ~p ~p ~p\n",
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

do_net_server_listing(Sock, DataDir) ->
    Files = filelib:wildcard("*", DataDir) -- ["config"],
    Out = ["OK\n",
           [begin
                {ok, FI} = file:read_file_info(DataDir ++ "/" ++ File),
                Size = FI#file_info.size,
                SizeBin = <<Size:64/big>>,
                [bin_to_hexstr(SizeBin), <<" ">>,
                 list_to_binary(File), <<"\n">>]
            end || File <- Files],
           ".\n"
          ],
    ok = gen_tcp:send(Sock, Out).

do_net_server_checksum_listing(Sock, File, DataDir) ->
    CSumPath = make_checksum_filename(DataDir, File),
    case file:open(CSumPath, [read, raw, binary]) of
        {ok, FH} ->
            {ok, FI} = file:read_file_info(CSumPath),
            Len = FI#file_info.size,
            LenHex = list_to_binary(bin_to_hexstr(<<Len:64/big>>)),
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

do_net_copy_bytes(FH, Sock) ->
    case file:read(FH, 1024*1024) of
        {ok, Bin} ->
            ok = gen_tcp:send(Sock, Bin),
            do_net_copy_bytes(FH, Sock);
        eof ->
            ok
    end.

do_net_server_delete_migration_only(Sock, File, DataDir) ->
    {_, Path} = make_data_filename(DataDir, File),
    case file:delete(Path) of
        ok ->
            ok = gen_tcp:send(Sock, "OK\n");
        {error, enoent} ->
            ok = gen_tcp:send(Sock, "ERROR NO-SUCH-FILE\n");
        _ ->
            ok = gen_tcp:send(Sock, "ERROR\n")
    end.

do_net_server_truncate_hackityhack(Sock, File, DataDir) ->
    {_, Path} = make_data_filename(DataDir, File),
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

decode_and_reply_net_server_ec_read(Sock, <<"a ", Rest/binary>>) ->
    decode_and_reply_net_server_ec_read_version_a(Sock, Rest);
decode_and_reply_net_server_ec_read(Sock, <<0:8, _/binary>>) ->
    ok = gen_tcp:send(Sock, <<"ERROR NOT-ERASURE\n">>).

decode_and_reply_net_server_ec_read_version_a(Sock, Rest) ->
    %% <<BodyLenHex:4/binary, " ", StripeWidthHex:16/binary, " ",
    %%   OrigFileLenHex:16/binary, " ", _/binary>> = Rest,
    HdrLen = 80 - 2 - 4 - 1,
    <<BodyLenHex:4/binary, " ", Hdr:HdrLen/binary, Rest2/binary>> = Rest,
    <<BodyLen:16/big>> = hexstr_to_bin(BodyLenHex),
    <<Body:BodyLen/binary, _/binary>> = Rest2,
    ok = gen_tcp:send(Sock, ["ERASURE ", BodyLenHex, " ", Hdr, Body]).

write_server_get_pid(Prefix, DataDir) ->
    RegName = make_regname(Prefix),
    case whereis(RegName) of
        undefined ->
            start_seq_append_server(Prefix, DataDir),
            timer:sleep(1),
            write_server_get_pid(Prefix, DataDir);
        Pid ->
            Pid
    end.

append_server_dispatch(From, Prefix, Chunk, CSum, DataDir) ->
    Pid = write_server_get_pid(Prefix, DataDir),
    Pid ! {seq_append, From, Prefix, Chunk, CSum},
    exit(normal).

start_seq_append_server(Prefix, DataDir) ->
    spawn(fun() -> run_seq_append_server(Prefix, DataDir) end).

run_seq_append_server(Prefix, DataDir) ->
    true = register(make_regname(Prefix), self()),
    ok = filelib:ensure_dir(DataDir ++ "/unused"),
    ok = filelib:ensure_dir(DataDir ++ "/config/unused"),
    run_seq_append_server2(Prefix, DataDir).

run_seq_append_server2(Prefix, DataDir) ->
    FileNum = read_max_filenum(DataDir, Prefix) + 1,
    ok = increment_max_filenum(DataDir, Prefix),
    info_msg("start: ~p server at file ~w\n", [Prefix, FileNum]),
    seq_append_server_loop(DataDir, Prefix, FileNum).

seq_append_server_loop(DataDir, Prefix, FileNum) ->
    SequencerNameHack = lists:flatten(io_lib:format(
                                        "~.36B~.36B",
                                        [element(3,now()),
                                         list_to_integer(os:getpid())])),
    {File, FullPath} = make_data_filename(DataDir, Prefix, SequencerNameHack,
                                          FileNum),
    {ok, FHd} = file:open(FullPath,
                          [write, binary, raw]),
                          %% [write, binary, raw, delayed_write]),
    CSumPath = make_checksum_filename(DataDir, Prefix, SequencerNameHack,
                                      FileNum),
    {ok, FHc} = file:open(CSumPath, [append, raw, binary, delayed_write]),
    seq_append_server_loop(DataDir, Prefix, File, {FHd,FHc}, FileNum,
                           ?MINIMUM_OFFSET).

seq_append_server_loop(DataDir, Prefix, _File, {FHd,FHc}, FileNum, Offset)
  when Offset > ?MAX_FILE_SIZE ->
    ok = file:close(FHd),
    ok = file:close(FHc),
    info_msg("rollover: ~p server at file ~w offset ~w\n",
             [Prefix, FileNum, Offset]),
    run_seq_append_server2(Prefix, DataDir);    
seq_append_server_loop(DataDir, Prefix, File, {FHd,FHc}=FH_, FileNum, Offset) ->
    receive
        {seq_append, From, Prefix, Chunk, CSum} ->
            ok = file:pwrite(FHd, Offset, Chunk),
            From ! {assignment, Offset, File},
            Len = byte_size(Chunk),
            OffsetHex = bin_to_hexstr(<<Offset:64/big>>),
            LenHex = bin_to_hexstr(<<Len:32/big>>),
            CSumHex = bin_to_hexstr(CSum),
            CSum_info = [OffsetHex, 32, LenHex, 32, CSumHex, 10],
            ok = file:write(FHc, CSum_info),
            seq_append_server_loop(DataDir, Prefix, File, FH_,
                                   FileNum, Offset + Len)
    after 30*1000 ->
            ok = file:close(FHd),
            ok = file:close(FHc),
            info_msg("stop: ~p server at file ~w offset ~w\n",
                     [Prefix, FileNum, Offset]),
            exit(normal)
    end.

make_regname(Prefix) ->
    erlang:binary_to_atom(Prefix, latin1).

make_config_filename(DataDir, Prefix) ->
    lists:flatten(io_lib:format("~s/config/~s", [DataDir, Prefix])).

make_checksum_filename(DataDir, Prefix, SequencerName, FileNum) ->
    lists:flatten(io_lib:format("~s/config/~s.~s.~w.csum",
                                [DataDir, Prefix, SequencerName, FileNum])).

make_checksum_filename(DataDir, FileName) ->
    lists:flatten(io_lib:format("~s/config/~s.csum", [DataDir, FileName])).

make_data_filename(DataDir, File) ->
    FullPath = lists:flatten(io_lib:format("~s/~s",  [DataDir, File])),
    {File, FullPath}.

make_data_filename(DataDir, Prefix, SequencerName, FileNum) ->
    File = erlang:iolist_to_binary(io_lib:format("~s.~s.~w",
                                                 [Prefix, SequencerName, FileNum])),
    FullPath = lists:flatten(io_lib:format("~s/~s",  [DataDir, File])),
    {File, FullPath}.

read_max_filenum(DataDir, Prefix) ->
    case file:read_file_info(make_config_filename(DataDir, Prefix)) of
        {error, enoent} ->
            0;
        {ok, FI} ->
            FI#file_info.size
    end.

increment_max_filenum(DataDir, Prefix) ->
    {ok, FH} = file:open(make_config_filename(DataDir, Prefix), [append]),
    ok = file:write(FH, "x"),
    %% ok = file:sync(FH),
    ok = file:close(FH).

hexstr_to_bin(S) when is_list(S) ->
  hexstr_to_bin(S, []);
hexstr_to_bin(B) when is_binary(B) ->
  hexstr_to_bin(binary_to_list(B), []).

hexstr_to_bin([], Acc) ->
  list_to_binary(lists:reverse(Acc));
hexstr_to_bin([X,Y|T], Acc) ->
  {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
  hexstr_to_bin(T, [V | Acc]).

bin_to_hexstr(<<>>) ->
    [];
bin_to_hexstr(<<X:4, Y:4, Rest/binary>>) ->
    [hex_digit(X), hex_digit(Y)|bin_to_hexstr(Rest)].

hex_digit(X) when X < 10 ->
    X + $0;
hex_digit(X) ->
    X - 10 + $a.

%%%%%%%%%%%%%%%%%

%%% escript stuff

main2(["file-write-client"]) ->
    io:format("Use:  Write a local file to a single server.\n"),
    io:format("Args: Host Port BlockSize Prefix LocalFile [OutputPath|'console']\n"),
    erlang:halt(1);
main2(["file-write-client", Host, PortStr, BlockSizeStr, PrefixStr, LocalFile|Output]) ->
    Sock = escript_connect(Host, PortStr),
    BlockSize = list_to_integer(BlockSizeStr),
    Prefix = list_to_binary(PrefixStr),
    Res = escript_upload_file(Sock, BlockSize, Prefix, LocalFile),
    FH = open_output_file(case Output of [] -> "console";
                                          _ -> Output end),
    print_upload_details(FH, Res),
    (catch file:close(FH)),
    Res;

main2(["1file-write-redundant-client"]) ->
    io:format("Use:  Write a local file to a series of servers.\n"),
    io:format("Args: BlockSize Prefix LocalFilePath [silent] [Host Port [Host Port ...]]\n"),
    erlang:halt(1);
main2(["1file-write-redundant-client", BlockSizeStr, PrefixStr, LocalFile|HPs0]) ->
    BlockSize = list_to_integer(BlockSizeStr),
    Prefix = list_to_binary(PrefixStr),
    {Out, HPs} = case HPs0 of
                     ["silent"|Rest] -> {silent, Rest};
                     _               -> {not_silent, HPs0}
                 end,
    Res = escript_upload_redundant(HPs, BlockSize, Prefix, LocalFile),
    if Out /= silent ->
            print_upload_details(user, Res);
       true ->
            ok
    end,
    Res;

main2(["chunk-read-client"]) ->
    io:format("Use:  Read a series of chunks for a single server.\n"),
    io:format("Args: Host Port LocalChunkDescriptionPath [OutputPath|'console']\n"),
    erlang:halt(1);
main2(["chunk-read-client", Host, PortStr, ChunkFileList]) ->
    main2(["chunk-read-client", Host, PortStr, ChunkFileList, "console"]);
main2(["chunk-read-client", Host, PortStr, ChunkFileList, OutputPath]) ->
    FH = open_output_file(OutputPath),
    OutFun = make_outfun(FH),
    try
        main2(["chunk-read-client2", Host, PortStr, ChunkFileList, OutFun])
    after
        (catch file:close(FH))
    end;
main2(["chunk-read-client2", Host, PortStr, ChunkFileList, ProcFun]) ->
    Sock = escript_connect(Host, PortStr),
    escript_download_chunks(Sock, ChunkFileList, ProcFun);

main2(["list-client"]) ->
    io:format("Use:  List all files on a single server.\n"),
    io:format("Args: Host Port [OutputPath]\n"),
    erlang:halt(1);
main2(["list-client", Host, PortStr]) ->
    Sock = escript_connect(Host, PortStr),
    escript_list(Sock);
main2(["list-client", Host, PortStr, OutputPath]) ->
    Sock = escript_connect(Host, PortStr),
    escript_list(Sock, OutputPath);

main2(["checksum-list-client"]) ->
    io:format("Use:  List all chunk sizes & checksums for a single file.\n"),
    io:format("Args: [ 'line-by-line' ] Host Port File\n"),
    erlang:halt(1);
main2(["checksum-list-client-line-by-line", Host, PortStr, File]) ->
    Sock = escript_connect(Host, PortStr),
    ProcFun = make_outfun(user),
    escript_checksum_list(Sock, File, line_by_line, ProcFun);
main2(["checksum-list-client", Host, PortStr, File]) ->
    Sock = escript_connect(Host, PortStr),
    ProcFun = make_outfun(user),
    escript_checksum_list(Sock, File, fast, ProcFun);

main2(["delete-client"]) ->
    io:format("Use:  Delete a file (NOT FOR GENERAL USE)\n"),
    io:format("Args: Host Port File\n"),
    erlang:halt(1);
main2(["delete-client", Host, PortStr, File]) ->
    Sock = escript_connect(Host, PortStr),
    escript_delete(Sock, File);

main2(["server"]) ->
    io:format("Use:  Run a file server + TCP listen port service.\n"),
    io:format("Args: Port DataDirPath\n"),
    erlang:halt(1);
main2(["server", RegNameStr, PortStr, DataDir]) ->
    Port = list_to_integer(PortStr),
    %% application:start(sasl),
    _Pid1 = start_listen_server(Port, DataDir),
    _Pid2 = start_append_server(list_to_atom(RegNameStr), DataDir),
    receive forever -> ok end;

%%%% cc flavors %%%%

main2(["cc-1file-write-redundant-client"]) ->
    io:format("Use:  Write a local file to a chain via projection.\n"),
    io:format("Args: BlockSize Prefix LocalFilePath ProjectionPath\n"),
    erlang:halt(1);
main2(["cc-1file-write-redundant-client", BlockSizeStr, PrefixStr, LocalFile, ProjectionPath]) ->
    BlockSize = list_to_integer(BlockSizeStr),
    Prefix = list_to_binary(PrefixStr),
    {_Chain, RawHPs} = calc_chain(write, ProjectionPath, PrefixStr),
    HPs = convert_raw_hps(RawHPs),
    Res = escript_upload_redundant(HPs, BlockSize, Prefix, LocalFile),
    print_upload_details(user, Res),
    Res;

main2(["cc-chunk-read-client"]) ->
    io:format("Use:  Read a series of chunks from a chain via projection.\n"),
    io:format("Args: ProjectionPath ChunkFileList [OutputPath|'console' \\\n\t[ErrorCorrection_ProjectionPath]]\n"),
    erlang:halt(1);
main2(["cc-chunk-read-client", ProjectionPathOrDir, ChunkFileList]) ->
    main3(["cc-chunk-read-client", ProjectionPathOrDir, ChunkFileList,"console",
           undefined]);
main2(["cc-chunk-read-client", ProjectionPathOrDir, ChunkFileList, OutputPath]) ->
    main3(["cc-chunk-read-client", ProjectionPathOrDir, ChunkFileList, OutputPath,
           undefined]);
main2(["cc-chunk-read-client", ProjectionPathOrDir, ChunkFileList, OutputPath,
       EC_ProjectionPath]) ->
    main3(["cc-chunk-read-client", ProjectionPathOrDir, ChunkFileList, OutputPath,
           EC_ProjectionPath]).

main3(["cc-chunk-read-client",
       ProjectionPathOrDir, ChunkFileList, OutputPath, EC_ProjectionPath]) ->
    P = read_projection_file(ProjectionPathOrDir),
    ChainMap = read_chain_map_file(ProjectionPathOrDir),
    FH = open_output_file(OutputPath),
    ProcFun = make_outfun(FH),
    Res = try
              escript_cc_download_chunks(ChunkFileList, P, ChainMap, ProcFun,
                                         EC_ProjectionPath)
          after
              (catch file:close(FH))
          end,
    Res.

escript_connect(Host, PortStr) ->
    Port = list_to_integer(PortStr),
    {ok, Sock} = gen_tcp:connect(Host, Port, [{active,false}, {mode,binary},
                                              {packet, raw}]),
    Sock.

escript_upload_file(Sock, BlockSize, Prefix, File) ->
    {ok, FH} = file:open(File, [read, raw, binary]),
    try
        escript_upload_file2(file:read(FH, BlockSize), FH,
                             BlockSize, Prefix, Sock, [])
    after
        file:close(FH)
    end.

escript_upload_file2({ok, Chunk}, FH, BlockSize, Prefix, Sock, Acc) ->
    {OffsetHex, LenHex, File} = upload_chunk_append(Sock, Prefix, Chunk),
    verb("~s ~s ~s\n", [OffsetHex, LenHex, File]),
    <<Offset:64/big>> = hexstr_to_bin(OffsetHex),
    <<Size:32/big>> = hexstr_to_bin(LenHex),
    OSF = {Offset, Size, File},
    escript_upload_file2(file:read(FH, BlockSize), FH, BlockSize, Prefix, Sock,
                        [OSF|Acc]);
escript_upload_file2(eof, _FH, _BlockSize, _Prefix, _Sock, Acc) ->
    lists:reverse(Acc).        

upload_chunk_append(Sock, Prefix, Chunk) ->
    %% _ = crypto:hash(md5, Chunk),
    Len = byte_size(Chunk),
    LenHex = list_to_binary(bin_to_hexstr(<<Len:32/big>>)),
    Cmd = <<"A ", LenHex/binary, " ", Prefix/binary, "\n">>,
    ok = gen_tcp:send(Sock, [Cmd, Chunk]),
    {ok, Line} = gen_tcp:recv(Sock, 0),
    PathLen = byte_size(Line) - 3 - 16 - 1 - 1,
    <<"OK ", OffsetHex:16/binary, " ", Path:PathLen/binary, _:1/binary>> = Line,
    {OffsetHex, LenHex, Path}.

upload_chunk_write(Sock, Offset, File, Chunk) when is_integer(Offset) ->
    OffsetHex = list_to_binary(bin_to_hexstr(<<Offset:64/big>>)),
    upload_chunk_write(Sock, OffsetHex, File, Chunk);
upload_chunk_write(Sock, OffsetHex, File, Chunk) when is_binary(OffsetHex) ->
    %% _ = crypto:hash(md5, Chunk),
    Len = byte_size(Chunk),
    LenHex = list_to_binary(bin_to_hexstr(<<Len:32/big>>)),
    Cmd = <<"W-repl ", OffsetHex/binary, " ",
            LenHex/binary, " ", File/binary, "\n">>,
    ok = gen_tcp:send(Sock, [Cmd, Chunk]),
    {ok, Line} = gen_tcp:recv(Sock, 0),
    <<"OK\n">> = Line,
    {OffsetHex, LenHex, File}.

escript_upload_redundant([Host, PortStr|HPs], BlockSize, Prefix, LocalFile) ->
    Sock = escript_connect(Host, PortStr),
    ok = inet:setopts(Sock, [{packet, line}]),
    OSFs = try
               escript_upload_file(Sock, BlockSize, Prefix, LocalFile)
           after
               gen_tcp:close(Sock)
           end,
    escript_upload_redundant2(HPs, OSFs, LocalFile, OSFs).

escript_upload_redundant2([], _OSFs, _LocalFile, OSFs) ->
    OSFs;
escript_upload_redundant2([Host, PortStr|HPs], OSFs, LocalFile, OSFs) ->
    Sock = escript_connect(Host, PortStr),
    {ok, FH} = file:open(LocalFile, [read, binary, raw]),
    try
        [begin
             {ok, Chunk} = file:read(FH, Size),
             _OSF2 = upload_chunk_write(Sock, Offset, File, Chunk)
             %% verb("~p: ~p\n", [{Host, PortStr}, OSF2])
         end || {Offset, Size, File} <- OSFs]
    after
        gen_tcp:close(Sock),
        file:close(FH)
    end,
    escript_upload_redundant2(HPs, OSFs, LocalFile, OSFs).

escript_download_chunks(Sock, {{{ChunkLine}}}, ProcFun) ->
    escript_download_chunk({ok, ChunkLine}, invalid_fd, Sock, ProcFun);
escript_download_chunks(Sock, ChunkFileList, ProcFun) ->
    {ok, FH} = file:open(ChunkFileList, [read, raw, binary]),
    escript_download_chunk(file:read_line(FH), FH, Sock, ProcFun).

escript_download_chunk({ok, Line}, FH, Sock, ProcFun) ->
    ChunkOrError = escript_cc_download_chunk2(Sock, Line),
    ProcFun(ChunkOrError),
    [ChunkOrError|
     escript_download_chunk((catch file:read_line(FH)), FH, Sock, ProcFun)];
escript_download_chunk(eof, _FH, _Sock, ProcFun) ->
    ProcFun(eof),
    [];
escript_download_chunk(_Else, _FH, _Sock, ProcFun) ->
    ProcFun(eof),
    [].

escript_cc_download_chunks({{{ChunkLine}}}, P, ChainMap, ProcFun,
                           EC_ProjectionPath) ->
    escript_cc_download_chunk({ok,ChunkLine}, invalid_fd, P, ChainMap, ProcFun,
                              EC_ProjectionPath);
escript_cc_download_chunks(ChunkFileList, P, ChainMap, ProcFun,
                           EC_ProjectionPath) ->
    {ok, FH} = file:open(ChunkFileList, [read, raw, binary]),
    escript_cc_download_chunk(file:read_line(FH), FH, P, ChainMap, ProcFun,
                              EC_ProjectionPath).

escript_cc_download_chunk({ok, Line}, FH, P, ChainMap, ProcFun,
                          EC_ProjectionPath) ->
    RestLen = byte_size(Line) - 16 - 1 - 8 - 1 - 1,
    <<_Offset:16/binary, " ", _Len:8/binary, " ", Rest:RestLen/binary, "\n">>
        = Line,
    Prefix = re:replace(Rest, "\\..*", "", [{return, binary}]),
    {_Chains, RawHPs} = calc_chain(read, P, ChainMap, Prefix),
    Chunk = lists:foldl(
              fun(_RawHP, Bin) when is_binary(Bin) -> Bin;
                 (RawHP, _) ->
                      [Host, PortStr] = convert_raw_hps([RawHP]),
                      Sock = get_cached_sock(Host, PortStr),
                      case escript_cc_download_chunk2(Sock, Line) of
                          Bin when is_binary(Bin) ->
                              Bin;
                          {error, _} = Error ->
                              Error;
                          {erasure_encoded, _} = EC_info ->
                              escript_cc_download_ec_chunk(EC_info,
                                                           EC_ProjectionPath)
                      end
              end, undefined, RawHPs),
    ProcFun(Chunk),
    [Chunk|escript_cc_download_chunk((catch file:read_line(FH)),
                                     FH, P, ChainMap, ProcFun,
                                     EC_ProjectionPath)];
escript_cc_download_chunk(eof, _FH, _P, _ChainMap, ProcFun,
                          _EC_ProjectionPath) ->
    ProcFun(eof),
    [];
escript_cc_download_chunk(Else, _FH, _P, _ChainMap, ProcFun,
                          _EC_ProjectionPath) ->
    ProcFun(Else),
    [].

escript_cc_download_chunk2(Sock, Line) ->
    %% Line includes an LF, so we can be lazy.
    CmdLF = [<<"R ">>, Line],
    ok = gen_tcp:send(Sock, CmdLF),
    case gen_tcp:recv(Sock, 3) of
        {ok, <<"OK\n">>} ->
            {_Offset, Size, _File} = read_hex_size(Line),
            {ok, Chunk} = gen_tcp:recv(Sock, Size),
            Chunk;
        {ok, Else} ->
            {ok, OldOpts} = inet:getopts(Sock, [packet]),
            ok = inet:setopts(Sock, [{packet, line}]),
            {ok, Else2} = gen_tcp:recv(Sock, 0),
            ok = inet:setopts(Sock, OldOpts),
            case Else of
                <<"ERA">> ->
                    escript_cc_parse_ec_info(Sock, Line, Else2);
                _ ->
                    {error, {Line, <<Else/binary, Else2/binary>>}}
            end
    end.

escript_cc_parse_ec_info(Sock, Line, Else2) ->
    ChompLine = chomp(Line),
    {Offset, Size, File} = read_hex_size(ChompLine),
    <<"SURE ", BodyLenHex:4/binary, " ", StripeWidthHex:16/binary, " ",
      OrigFileLenHex:16/binary, " rs_10_4_v1", _/binary>> = Else2,
    <<BodyLen:16/big>> = hexstr_to_bin(BodyLenHex),
    {ok, SummaryBody} = gen_tcp:recv(Sock, BodyLen),

    <<StripeWidth:64/big>> = hexstr_to_bin(StripeWidthHex),
    <<OrigFileLen:64/big>> = hexstr_to_bin(OrigFileLenHex),
    NewFileNum = (Offset div StripeWidth) + 1,
    NewOffset = Offset rem StripeWidth,
    if Offset + Size > OrigFileLen ->
            %% Client's request is larger than original file size, derp
            {error, bad_offset_and_size};
       NewOffset + Size > StripeWidth ->
            %% Client's request straddles a stripe boundary, TODO fix me
            {error, todo_TODO_implement_this_with_two_reads_and_then_glue_together};
       true ->
            NewOffsetHex = bin_to_hexstr(<<NewOffset:64/big>>),
            LenHex = bin_to_hexstr(<<Size:32/big>>),
            NewSuffix = file_suffix_rs_10_4_v1(NewFileNum),
            NewFile = iolist_to_binary([File, NewSuffix]),
            NewLine = iolist_to_binary([NewOffsetHex, " ", LenHex, " ",
                                        NewFile, "\n"]),
            {erasure_encoded, {Offset, Size, File, NewOffset, NewFile,
                               NewFileNum, NewLine, SummaryBody}}
    end.

%% TODO: The EC method/version/type stuff here is loosey-goosey
escript_cc_download_ec_chunk(EC_info, undefined) ->
    EC_info;
escript_cc_download_ec_chunk({erasure_encoded,
                              {_Offset, _Size, _File, _NewOffset, NewFile,
                               NewFileNum, NewLine, SummaryBody}},
                             EC_ProjectionPath) ->
    {P, ChainMap} = get_cached_projection(EC_ProjectionPath),
    %% Remember: we use the whole file name for hashing, not the prefix
    {_Chains, RawHPs} = calc_chain(read, P, ChainMap, NewFile),
    RawHP = lists:nth(NewFileNum, RawHPs),
    [Host, PortStr] = convert_raw_hps([RawHP]),
    Sock = get_cached_sock(Host, PortStr),
    case escript_cc_download_chunk2(Sock, NewLine) of
        Chunk when is_binary(Chunk) ->
            Chunk;
        {error, _} = Else ->
            io:format("TODO: EC chunk get failed:\n\t~s\n", [NewLine]),
            io:format("Use this info to reconstruct:\n\t~p\n\n", [SummaryBody]),
            Else
    end.

get_cached_projection(EC_ProjectionPath) ->
    case get(cached_projection) of
        undefined ->
            P = read_projection_file(EC_ProjectionPath),
            ChainMap = read_chain_map_file(EC_ProjectionPath),
            put(cached_projection, {P, ChainMap}),
            get_cached_projection(EC_ProjectionPath);
        Stuff ->
            Stuff
    end.

file_suffix_rs_10_4_v1(1)  -> <<"_k01">>;
file_suffix_rs_10_4_v1(2)  -> <<"_k02">>;
file_suffix_rs_10_4_v1(3)  -> <<"_k03">>;
file_suffix_rs_10_4_v1(4)  -> <<"_k04">>;
file_suffix_rs_10_4_v1(5)  -> <<"_k05">>;
file_suffix_rs_10_4_v1(6)  -> <<"_k06">>;
file_suffix_rs_10_4_v1(7)  -> <<"_k07">>;
file_suffix_rs_10_4_v1(8)  -> <<"_k08">>;
file_suffix_rs_10_4_v1(9)  -> <<"_k09">>;
file_suffix_rs_10_4_v1(10) -> <<"_k10">>.

escript_list(Sock) ->
    escript_list2(Sock, make_outfun(user)).

escript_list(Sock, OutputPath) ->
io:format("OutputPath ~p\n", [OutputPath]),
    {ok, FH} = file:open(OutputPath, [write]),
    try
        escript_list2(Sock, make_outfun(FH))
    after
        file:close(FH)
    end.

escript_list2(Sock, ProcFun) ->
    ok = gen_tcp:send(Sock, <<"L\n">>),
    ok = inet:setopts(Sock, [{packet, line}]),
    {ok, <<"OK\n">>} = gen_tcp:recv(Sock, 0),
    Res = escript_list2(gen_tcp:recv(Sock, 0), Sock, ProcFun),
    ok = inet:setopts(Sock, [{packet, raw}]),
    Res.

escript_list2({ok, <<".\n">>}, _Sock, ProcFun) ->
    ProcFun(eof),
    [];
escript_list2({ok, Line}, Sock, ProcFun) ->
    ProcFun(Line),
    [Line|escript_list2(gen_tcp:recv(Sock, 0), Sock, ProcFun)];
escript_list2(Else, _Sock, ProcFun) ->
    ProcFun(io_lib:format("ERROR: ~p\n", [Else])),
    {error, Else}.

escript_checksum_list(Sock, File, Method, ProcFun) ->
    ok = gen_tcp:send(Sock, [<<"C ">>, File, <<"\n">>]),
    ok = inet:setopts(Sock, [{packet, line}]),
    case gen_tcp:recv(Sock, 0) of
        {ok, <<"OK ", Rest/binary>> = Line} ->
            put(status, ok),                    % may be unset later
            ProcFun(Line),
            if Method == line_by_line ->
                    escript_checksum_list_line_by_line(Sock, ProcFun);
               Method == fast ->
                    RestLen = byte_size(Rest) - 1,
                    <<LenHex:RestLen/binary, _:1/binary>> = Rest,
                    <<Len:64/big>> = hexstr_to_bin(LenHex),
                    ok = inet:setopts(Sock, [{packet, raw}]),
                    escript_checksum_list_fast(Sock, Len, ProcFun)
            end;
        {ok, _BAH} ->
            put(status, error),
            io:format("ERROR: ~p\n", [_BAH])
    end.

escript_checksum_list_line_by_line(Sock, ProcFun) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, <<".\n">> = Line} ->
            ProcFun(Line),
            ok;
        {ok, Line} ->
            ProcFun(Line),
            escript_checksum_list_line_by_line(Sock, ProcFun)
    end.

escript_checksum_list_fast(Sock, 0, ProcFun) ->
    {ok, <<".\n">> = Line} = gen_tcp:recv(Sock, 2),
    ProcFun(Line),
    ok;
escript_checksum_list_fast(Sock, Remaining, ProcFun) ->
    Num = erlang:min(Remaining, 1024*1024),
    {ok, Bytes} = gen_tcp:recv(Sock, Num),
    ProcFun(Bytes),
    escript_checksum_list_fast(Sock, Remaining - byte_size(Bytes), ProcFun).

escript_delete(Sock, File) ->
    ok = gen_tcp:send(Sock, [<<"DEL-migration ">>, File, <<"\n">>]),
    ok = inet:setopts(Sock, [{packet, line}]),
    case gen_tcp:recv(Sock, 0) of
        {ok, <<"OK\n">>} ->
            ok;
        {ok, <<"ERROR", _/binary>>} ->
            error
    end.

escript_compare_servers(Sock1, Sock2, H1, H2, Args) ->
    FileFilterFun = fun(_) -> true end,
    escript_compare_servers(Sock1, Sock2, H1, H2, FileFilterFun, Args).

escript_compare_servers(Sock1, Sock2, H1, H2, FileFilterFun, Args) ->
    All = [H1, H2],
    put(mydict, dict:new()),
    Fetch1 = make_fetcher(H1),
    Fetch2 = make_fetcher(H2),
    
    Fmt = case Args of
              [] ->
                  fun(eof) -> ok; (Str) -> io:format(user, Str, []) end;
              [null] ->
                  fun(_) -> ok end;
              [OutFile] ->
                  {ok, FH} = file:open(OutFile, [write]),
                  fun(eof) -> file:close(FH);
                     (Str) -> file:write(FH, Str)
                  end
          end,

    _X1 = escript_list2(Sock1, Fetch1),
    _X2 = escript_list2(Sock2, Fetch2),
    FoldRes = lists:sort(dict:to_list(get(mydict))),
    Fmt("{legend, {file, list_of_servers_without_file}}.\n"),
    Fmt(io_lib:format("{all, ~p}.\n", [All])),
    Res = [begin
               {GotIt, Sizes} = lists:unzip(GotSizes),
               Size = lists:max(Sizes),
               Missing = {File, {Size, All -- GotIt}},
               verb("~p.\n", [Missing]),
               Missing
           end || {File, GotSizes} <- FoldRes, FileFilterFun(File)],
    (catch Fmt(eof)),
    Res.

make_fetcher(Host) ->
    fun(eof) ->
            ok;
       (<<SizeHex:16/binary, " ", Rest/binary>>) ->
            <<Size:64/big>> = hexstr_to_bin(SizeHex),
            FileLen = byte_size(Rest) - 1,
            <<File:FileLen/binary, _/binary>> = Rest,
            NewDict = dict:append(File, {Host, Size}, get(mydict)),
            put(mydict, NewDict)
    end.

checksum(Bin) when is_binary(Bin) ->
    crypto:hash(md5, Bin).

verb(Fmt) ->
    verb(Fmt, []).

verb(Fmt, Args) ->
    case application:get_env(kernel, verbose) of
        {ok, true} -> io:format(Fmt, Args);
        _          -> ok
    end.

info_msg(Fmt, Args) ->
    case application:get_env(kernel, verbose) of {ok, false} -> ok;
                                                 _     -> error_logger:info_msg(Fmt, Args)
    end.

repair(File, Size, [], Mode, V, SrcS, SrcS2, DstS, DstS2, _Src) ->
    verb("~s: present on both: ", [File]),
    repair_both_present(File, Size, Mode, V, SrcS, SrcS2, DstS, DstS2);
repair(File, Size, MissingList, Mode, V, SrcS, SrcS2, DstS, _DstS2, Src) ->
    case lists:member(Src, MissingList) of
        true ->
            verb("~s -> ~p, skipping: not on source server\n", [File, MissingList]);
        false when Mode == check ->
            verb("~s -> ~p, copy ~s MB (skipped)\n", [File, MissingList, mbytes(Size)]);
        false ->
            verb("~s -> ~p, copy ~s MB ", [File, MissingList, mbytes(Size)]),
            ok = copy_file(File, SrcS, SrcS2, DstS, V),
            verb("done\n", [])
    end.

copy_file(File, SrcS, SrcS2, DstS, Verbose) ->
    %% Use the *second* source socket to copy each chunk.
    ProcChecksum = copy_file_proc_checksum_fun(File, SrcS2, DstS, Verbose),
    %% Use the *first source socket to enumerate the chunks & checksums.
    escript_checksum_list(SrcS, File, line_by_line, ProcChecksum).

copy_file_proc_checksum_fun(File, SrcS, DstS, _Verbose) ->
    fun(<<OffsetHex:16/binary, " ", LenHex:8/binary, " ",
          CSumHex:32/binary, "\n">>) ->
            <<Len:32/big>> = hexstr_to_bin(LenHex),
            DownloadChunkBin = <<OffsetHex/binary, " ", LenHex/binary, " ",
                                 File/binary, "\n">>,
            [Chunk] = escript_download_chunks(SrcS, {{{DownloadChunkBin}}},
                                              fun(_) -> ok end),
            CSum = hexstr_to_bin(CSumHex),
            CSum2 = checksum(Chunk),
            if Len == byte_size(Chunk), CSum == CSum2 ->
                    {_,_,_} = upload_chunk_write(DstS, OffsetHex, File, Chunk),
                    ok;
               true ->
                    io:format("ERROR: ~s ~s ~s csum/size error\n",
                              [File, OffsetHex, LenHex]),
                    error
            end;
       (_Else) ->
            ok
    end.

repair_both_present(File, Size, Mode, V, SrcS, _SrcS2, DstS, _DstS2) ->
    Tmp1 = lists:flatten(io_lib:format("/tmp/sort.1.~w.~w.~w", tuple_to_list(now()))),
    Tmp2 = lists:flatten(io_lib:format("/tmp/sort.2.~w.~w.~w", tuple_to_list(now()))),
    J_Both = lists:flatten(io_lib:format("/tmp/join.3-both.~w.~w.~w", tuple_to_list(now()))),
    J_SrcOnly = lists:flatten(io_lib:format("/tmp/join.4-src-only.~w.~w.~w", tuple_to_list(now()))),
    J_DstOnly = lists:flatten(io_lib:format("/tmp/join.5-dst-only.~w.~w.~w", tuple_to_list(now()))),
    S_Identical = lists:flatten(io_lib:format("/tmp/join.6-sort-identical.~w.~w.~w", tuple_to_list(now()))),
    {ok, FH1} = file:open(Tmp1, [write, raw, binary]),
    {ok, FH2} = file:open(Tmp2, [write, raw, binary]),
    try
        K = md5_ctx,
        MD5_it = fun(Bin) ->
                         {FH, MD5ctx1} = get(K),
                         file:write(FH, Bin),
                         MD5ctx2 = crypto:hash_update(MD5ctx1, Bin),
                         put(K, {FH, MD5ctx2})
                 end,
        put(K, {FH1, crypto:hash_init(md5)}),
        ok = escript_checksum_list(SrcS, File, fast, MD5_it),
        {_, MD5_1} = get(K),
        SrcMD5 = crypto:hash_final(MD5_1),
        put(K, {FH2, crypto:hash_init(md5)}),
        ok = escript_checksum_list(DstS, File, fast, MD5_it),
        {_, MD5_2} = get(K),
        DstMD5 = crypto:hash_final(MD5_2),
        if SrcMD5 == DstMD5 ->
                verb("identical\n", []);
           true ->
                ok = file:close(FH1),
                ok = file:close(FH2),
                _Q1 = os:cmd("./REPAIR-SORT-JOIN.sh " ++ Tmp1 ++ " " ++ Tmp2 ++ " " ++ J_Both ++ " " ++ J_SrcOnly ++ " " ++ J_DstOnly ++ " " ++ S_Identical),
                case file:read_file_info(S_Identical) of
                    {ok, _} ->
                        verb("identical (secondary sort)\n", []);
                    {error, enoent} ->
                      io:format("differences found:"),
                      repair_both(File, Size, V, Mode,
                                  J_Both, J_SrcOnly, J_DstOnly,
                                  SrcS, DstS)
                end
        end
    after
        catch file:close(FH1),
        catch file:close(FH2),
        [(catch file:delete(FF)) || FF <- [Tmp1,Tmp2,J_Both,J_SrcOnly,J_DstOnly,
                                           S_Identical]]
    end.

repair_both(File, _Size, V, Mode, J_Both, J_SrcOnly, J_DstOnly, SrcS, DstS) ->
    AccFun = if Mode == check ->
                     fun(_X, List) ->    List  end;
                Mode == repair ->
                     fun( X, List) -> [X|List] end
             end,
    BothFun = fun(<<_OffsetSrcHex:16/binary, " ",
                    LenSrcHex:8/binary, " ", CSumSrcHex:32/binary, " ",
                    LenDstHex:8/binary, " ", CSumDstHex:32/binary, "\n">> =Line,
                  {SameB, SameC, DiffB, DiffC, Ds}) ->
                      <<Len:32/big>> = hexstr_to_bin(LenSrcHex),
                      if LenSrcHex == LenDstHex,
                         CSumSrcHex == CSumDstHex ->
                              {SameB + Len, SameC + 1, DiffB, DiffC, Ds};
                         true ->
                              %% D = {OffsetSrcHex, LenSrcHex, ........
                              {SameB, SameC, DiffB + Len, DiffC + 1,
                               AccFun(Line, Ds)}
                      end;
                 (_Else, Acc) ->
                      Acc
              end,
    OnlyFun = fun(<<_OffsetSrcHex:16/binary, " ", LenSrcHex:8/binary, " ",
                    _CSumHex:32/binary, "\n">> = Line,
                  {DiffB, DiffC, Ds}) ->
                      <<Len:32/big>> = hexstr_to_bin(LenSrcHex),
                      {DiffB + Len, DiffC + 1, AccFun(Line, Ds)};
                 (_Else, Acc) ->
                      Acc
              end,
    {SameBx, SameCx, DiffBy, DiffCy, BothDiffs} =
        file_folder(BothFun, {0,0,0,0,[]}, J_Both),
    {DiffB_src, DiffC_src, Ds_src} = file_folder(OnlyFun, {0,0,[]}, J_SrcOnly),
    {DiffB_dst, DiffC_dst, Ds_dst} = file_folder(OnlyFun, {0,0,[]}, J_DstOnly),
    if Mode == check orelse V == true ->
            io:format("\n\t"),
            io:format("BothR ~p, ", [{SameBx, SameCx, DiffBy, DiffCy}]),
            io:format("SrcR ~p, ", [{DiffB_src, DiffC_src}]),
            io:format("DstR ~p", [{DiffB_dst, DiffC_dst}]),
            io:format("\n");
       true -> ok
    end,
    if Mode == repair ->
            ok = repair_both_both(File, V, BothDiffs, SrcS, DstS),
            ok = repair_copy_chunks(File, V, Ds_src, DiffB_src, DiffC_src,
                                    SrcS, DstS),
            ok = repair_copy_chunks(File, V, Ds_dst, DiffB_dst, DiffC_dst,
                                    DstS, SrcS);
       true ->
            ok
    end.

repair_both_both(_File, _V, [_|_], _SrcS, _DstS) ->
    %% TODO: fetch both, check checksums, hopefully only exactly one
    %%       is correct, then use that one to repair the other.  And if the
    %%       sizes are different, hrm, there may be an extra corner case(s)
    %%       hiding there.
    io:format("WHOA! We have differing checksums or sizes here, TODO not implemented, but there's trouble in the little village on the river....\n"),
    timer:sleep(3*1000),
    ok;
repair_both_both(_File, _V, [], _SrcS, _DstS) ->
    ok.

repair_copy_chunks(_File, _V, [], _DiffBytes, _DiffCount, _SrcS, _DstS) ->
    ok;
repair_copy_chunks(File, V, ToBeCopied, DiffBytes, DiffCount, SrcS, DstS) ->
    verb("\n", []),
    verb("Starting copy of ~p chunks/~s MBytes to \n    ~s: ",
           [DiffCount, mbytes(DiffBytes), File]),
    InnerCopyFun = copy_file_proc_checksum_fun(File, SrcS, DstS, V),
    FoldFun = fun(Line, ok) ->
                      ok = InnerCopyFun(Line) % Strong sanity check
              end,
    ok = lists:foldl(FoldFun, ok, ToBeCopied),
    verb(" done\n", []),
    ok.

file_folder(Fun, Acc, Path) ->
    {ok, FH} = file:open(Path, [read, raw, binary]),
    try
        file_folder2(Fun, Acc, FH)
    after
        file:close(FH)
    end.

file_folder2(Fun, Acc, FH) ->
    file_folder2(file:read_line(FH), Fun, Acc, FH).

file_folder2({ok, Line}, Fun, Acc, FH) ->
    Acc2 = Fun(Line, Acc),
    file_folder2(Fun, Acc2, FH);
file_folder2(eof, _Fun, Acc, _FH) ->
    Acc.

make_repair_props(["check"|T]) ->
    [{mode, check}|make_repair_props(T)];
make_repair_props(["repair"|T]) ->
    [{mode, repair}|make_repair_props(T)];
make_repair_props(["verbose"|T]) ->
    application:set_env(kernel, verbose, true),
    [{verbose, true}|make_repair_props(T)];
make_repair_props(["noverbose"|T]) ->
    [{verbose, false}|make_repair_props(T)];
make_repair_props(["progress"|T]) ->
    [{progress, true}|make_repair_props(T)];
make_repair_props(["delete-source"|T]) ->
    [{delete_source, true}|make_repair_props(T)];
make_repair_props(["nodelete-source"|T]) ->
    [{delete_source, false}|make_repair_props(T)];
make_repair_props(["nodelete-tmp"|T]) ->
    [{delete_tmp, false}|make_repair_props(T)];
make_repair_props([X|T]) ->
    io:format("Error: skipping unknown option ~p\n", [X]),
    make_repair_props(T);
make_repair_props([]) ->
    %% Proplist defaults
    [{mode, check}, {delete_source, false}].

mbytes(0) ->
    "0.0";
mbytes(Size) ->
    lists:flatten(io_lib:format("~.1.0f", [max(0.1, Size / (1024*1024))])).

chomp(Line) when is_binary(Line) ->
    LineLen = byte_size(Line) - 1,
    <<ChompLine:LineLen/binary, _/binary>> = Line,
    ChompLine.    

make_outfun(FH) ->
    fun({error, _} = Error) ->
            file:write(FH, io_lib:format("Error: ~p\n", [Error]));
       (eof) ->
            ok;
       ({erasure_encoded, Info} = _Erasure) ->
            file:write(FH, "TODO/WIP: erasure_coded:\n"),
            file:write(FH, io_lib:format("\t~p\n", [Info]));
       (Bytes) when is_binary(Bytes) orelse is_list(Bytes) ->
            file:write(FH, Bytes)
    end.

open_output_file("console") ->
    user;
open_output_file(Path) ->
    {ok, FH} = file:open(Path, [write]),
    FH.

print_upload_details(_, {error, _} = Res) ->
    io:format("Error: ~p\n", [Res]),
    erlang:halt(1);
print_upload_details(FH, Res) ->
    [io:format(FH, "~s ~s ~s\n", [bin_to_hexstr(<<Offset:64/big>>),
                                  bin_to_hexstr(<<Len:32/big>>),
                                  File]) ||
        {Offset, Len, File} <- Res].

%%%%%%%%%%%%%%%%%

read_projection_file("new") ->
    #projection{epoch=0, last_epoch=0,
                float_map=undefined, last_float_map=undefined};
read_projection_file(Path) ->
    case filelib:is_dir(Path) of
        true ->
            read_projection_file_loop(Path ++ "/current.proj");
        false ->
            case filelib:is_file(Path) of
                true ->
                    read_projection_file2(Path);
                false ->
                    error({bummer, Path})
            end
    end.

read_projection_file2(Path) ->
    {ok, [P]} = file:consult(Path),
    true = is_record(P, projection),
    FloatMap = P#projection.float_map,
    LastFloatMap = if P#projection.last_float_map == undefined ->
                           FloatMap;
                      true ->
                           P#projection.last_float_map
                   end,
    P#projection{migrating=(FloatMap /= LastFloatMap),
                 tree=szone_chash:make_tree(FloatMap),
                 last_tree=szone_chash:make_tree(LastFloatMap)}.

read_projection_file_loop(Path) ->
    read_projection_file_loop(Path, 100).

read_projection_file_loop(Path, 0) ->
    error({bummer, Path});
read_projection_file_loop(Path, N) ->
    try
        read_projection_file2(Path)
    catch
        error:{badmatch,{error,enoent}} ->
            timer:sleep(100),
            read_projection_file_loop(Path, N-1)
    end.

write_projection(P, Path) when is_record(P, projection) ->
    {error, enoent} = file:read_file_info(Path),
    {ok, FH} = file:open(Path, [write]),
    WritingP = P#projection{tree=undefined, last_tree=undefined},
    io:format(FH, "~p.\n", [WritingP]),
    ok = file:close(FH).    

read_weight_map_file(Path) ->
    {ok, [Map]} = file:consult(Path),
    true = is_list(Map),
    true = lists:all(fun({Chain, Weight})
                           when is_binary(Chain),
                                is_integer(Weight), Weight >= 0 ->
                             true;
                        (_) ->
                             false
                     end, Map),
    Map.

%% Assume the file "chains.map" in whatever dir that stores projections.
read_chain_map_file(DirPath) ->
    L = case filelib:is_dir(DirPath) of
            true ->
                {ok, Map} = file:consult(DirPath ++ "/chains.map"),
                Map;
            false ->
                Dir = filename:dirname(DirPath),
                {ok, Map} = file:consult(Dir ++ "/chains.map"),
                Map
        end,
    orddict:from_list(L).

get_float_map(P) when is_record(P, projection) ->
    P#projection.float_map.

get_last_float_map(P) when is_record(P, projection) ->
    P#projection.last_float_map.

hash_and_query(Key, P) when is_record(P, projection) ->
    <<Int:(20*8)/unsigned>> = crypto:hash(sha, Key),
    Float = Int / ?SHA_MAX,
    {_, Current} = szone_chash:query_tree(Float, P#projection.tree),
    if P#projection.migrating ->
            {_, Last} = szone_chash:query_tree(Float, P#projection.last_tree),
            if Last == Current ->
                    [Current];
               true ->
                    [Current, Last, Current]
            end;
       true ->
            [Current]
    end.

calc_chain(write=Op, ProjectionPathOrDir, PrefixStr) ->
    P = read_projection_file(ProjectionPathOrDir),
    ChainMap = read_chain_map_file(ProjectionPathOrDir),
    calc_chain(Op, P, ChainMap, PrefixStr);
calc_chain(read=Op, ProjectionPathOrDir, PrefixStr) ->
    P = read_projection_file(ProjectionPathOrDir),
    ChainMap = read_chain_map_file(ProjectionPathOrDir),
    calc_chain(Op, P, ChainMap, PrefixStr).

calc_chain(write=_Op, P, ChainMap, PrefixStr) ->
    %% Writes are easy: always use the new location.
    [Chain|_] = hash_and_query(PrefixStr, P),
    {Chain, orddict:fetch(Chain, ChainMap)};
calc_chain(read=_Op, P, ChainMap, PrefixStr) ->
    %% Reads are slightly trickier: reverse each chain so tail is tried first.
    Chains = hash_and_query(PrefixStr, P),
    {Chains, lists:flatten([lists:reverse(orddict:fetch(Chain, ChainMap)) ||
                               Chain <- Chains])}.

convert_raw_hps([{HostBin, Port}|T]) ->
    [binary_to_list(HostBin), integer_to_list(Port)|convert_raw_hps(T)];
convert_raw_hps([]) ->
    [].

get_cached_sock(Host, PortStr) ->
    K = {socket_cache, Host, PortStr},
    case erlang:get(K) of
        undefined ->
            Sock = escript_connect(Host, PortStr),
            Krev = {socket_cache_rev, Sock},
            erlang:put(K, Sock),
            erlang:put(Krev, {Host, PortStr}),
            Sock;
        Sock ->
            Sock
    end.

invalidate_cached_sock(Sock) ->
    (catch gen_tcp:close(Sock)),
    Krev = {socket_cache_rev, Sock},
    case erlang:get(Krev) of
        undefined ->
            ok;
        {Host, PortStr} ->
            K = {socket_cache, Host, PortStr},
            erlang:erase(Krev),
            erlang:erase(K),
            ok
    end.

%%%%%%%%%%%%%%%%%

%%% basho_bench callbacks

-define(SEQ, ?MODULE).
-define(DEFAULT_HOSTIP_LIST, [{{127,0,0,1}, 7071}]).

-record(bb, {
          host,
          port_str,
          %% sock,
          proj_check_ticker_started=false,
          proj_path,
          proj,
          chain_map
         }).

new(1 = Id) ->
    start_append_server(),
    case basho_bench_config:get(file0_start_listener, no) of
        no ->
            ok;
        {Port, DataDir} ->
            start_listen_server(Port, DataDir)
    end,
    timer:sleep(100),
    new_common(Id);
new(Id) ->
    new_common(Id).

new_common(Id) ->
    random:seed(now()),
    ProjectionPathOrDir =
        basho_bench_config:get(file0_projection_path, undefined),

    Servers = basho_bench_config:get(file0_ip_list, ?DEFAULT_HOSTIP_LIST),
    NumServers = length(Servers),
    {Host, Port} = lists:nth((Id rem NumServers) + 1, Servers),
    State0 = #bb{host=Host, port_str=integer_to_list(Port),
                 proj_path=ProjectionPathOrDir},
    {ok, read_projection_info(State0)}.

run(null, _KeyGen, _ValueGen, State) ->
    {ok, State};
run(keygen_valuegen_then_null, KeyGen, ValueGen, State) ->
    _Prefix = KeyGen(),
    _Value = ValueGen(),
    {ok, State};
run(append_local_server, KeyGen, ValueGen, State) ->
    Prefix = KeyGen(),
    Value = ValueGen(),
    {_, _} = ?SEQ:append(?SEQ, Prefix, Value),
    {ok, State};
run(append_remote_server, KeyGen, ValueGen, State) ->
    Prefix = KeyGen(),
    Value = ValueGen(),
    bb_do_write_chunk(Prefix, Value, State#bb.host, State#bb.port_str, State);
run(cc_append_remote_server, KeyGen, ValueGen, State0) ->
    State = check_projection_check(State0),
    Prefix = KeyGen(),
    Value = ValueGen(),
    {_Chain, ModHPs} = calc_chain(write, State#bb.proj, State#bb.chain_map,
                                  Prefix),
    FoldFun = fun({Host, PortStr}, Acc) ->
                      case bb_do_write_chunk(Prefix, Value, Host, PortStr,
                                             State) of
                          {ok, _} ->
                              Acc + 1;
                          _ ->
                              Acc
                      end
              end,
    case lists:foldl(FoldFun, 0, ModHPs) of
        N when is_integer(N), N > 0 ->
            {ok, State};
        0 ->
            {error, oh_some_problem_yo, State}
    end;
run(read_raw_line_local, KeyGen, _ValueGen, State) ->
    {RawLine, Size, _File} = setup_read_raw_line(KeyGen),
    bb_do_read_chunk(RawLine, Size, State#bb.host, State#bb.port_str, State);
run(cc_read_raw_line_local, KeyGen, _ValueGen, State0) ->
    State = check_projection_check(State0),
    {RawLine, Size, File} = setup_read_raw_line(KeyGen),
    Prefix = re:replace(File, "\\..*", "", [{return, binary}]),
    {_Chain, ModHPs} = calc_chain(read, State#bb.proj, State#bb.chain_map,
                                  Prefix),
    FoldFun = fun(_, {ok, _}=Acc) ->
                      Acc;
                 ({Host, PortStr}, _Acc) ->
                      bb_do_read_chunk(RawLine, Size, Host, PortStr, State)
              end,
    lists:foldl(FoldFun, undefined, ModHPs).

bb_do_read_chunk(RawLine, Size, Host, PortStr, State) ->
    try
        Sock = get_cached_sock(Host, PortStr),
        try
            ok = gen_tcp:send(Sock, [RawLine, <<"\n">>]),
            read_chunk(Sock, Size, State)
        catch X2:Y2 ->
                invalidate_cached_sock(Sock),
                {error, {X2,Y2}, State}
        end
    catch X:Y ->
            {error, {X,Y}, State}
    end.

bb_do_write_chunk(Prefix, Value, Host, PortStr, State) ->
    try
        Sock = get_cached_sock(Host, PortStr),
        try
            {_, _, _} = upload_chunk_append(Sock, Prefix, Value),
            {ok, State}
        catch X2:Y2 ->
                invalidate_cached_sock(Sock),
                {error, {X2,Y2}, State}
        end
    catch X:Y ->
            {error, {X,Y}, State}
    end.

read_chunk(Sock, Size, State) ->
    {ok, <<"OK\n">>} = gen_tcp:recv(Sock, 3),
    {ok, _Chunk} = gen_tcp:recv(Sock, Size),
    {ok, State}.

setup_read_raw_line(KeyGen) ->
    RawLine = KeyGen(),
    <<"R ", Rest/binary>> = RawLine,
    {_Offset, Size, File} = read_hex_size(Rest),
    {RawLine, Size, File}.

read_hex_size(Line) ->
    <<OffsetHex:16/binary, " ", SizeHex:8/binary, " ", File/binary>> = Line,
    <<Offset:64/big>> = hexstr_to_bin(OffsetHex),
    <<Size:32/big>> = hexstr_to_bin(SizeHex),
    {Offset, Size, File}.

read_projection_info(#bb{proj_path=undefined}=State) ->
    State;
read_projection_info(#bb{proj_path=ProjectionPathOrDir}=State) ->
    Proj = read_projection_file(ProjectionPathOrDir),
    ChainMap = read_chain_map_file(ProjectionPathOrDir),
    ModChainMap =
        [{Chain, [{binary_to_list(Host), integer_to_list(Port)} ||
                     {Host, Port} <- Members]} ||
            {Chain, Members} <- ChainMap],
    State#bb{proj=Proj, chain_map=ModChainMap}.

check_projection_check(#bb{proj_check_ticker_started=false} = State) ->
    timer:send_interval(5*1000 - random:uniform(500), projection_check),
    check_projection_check(State#bb{proj_check_ticker_started=true});
check_projection_check(#bb{proj_check_ticker_started=true} = State) ->
    receive
        projection_check ->
            read_projection_info(State)
    after 0 ->
            State
    end.
