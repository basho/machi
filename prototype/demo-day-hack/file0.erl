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

-include_lib("kernel/include/file.hrl").

-define(MAX_FILE_SIZE, 256*1024*1024*1024).     % 256 GBytes
%% -define(DATA_DIR, "/Volumes/SAM1/seq-tests/data").
-define(DATA_DIR, "./data").

append(Server, Prefix, Chunk) when is_binary(Prefix), is_binary(Chunk) ->
    Server ! {seq_append, self(), Prefix, Chunk},
    receive
        {assignment, File, Offset} ->
            {File, Offset}
    after 10*1000 ->
            bummer
    end.

append_direct(Prefix, Chunk) when is_binary(Prefix), is_binary(Chunk) ->
    RegName = make_regname(Prefix),
    append(RegName, Prefix, Chunk).

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
        {seq_append, From, Prefix, Chunk} ->
            spawn(fun() -> append_server_dispatch(From, Prefix, Chunk,
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
            %% fmt("Got: ~p\n", [Line]),
            PrefixLenLF = byte_size(Line)   - 2 - 8 - 1 - 1,
            PrefixLenCRLF = byte_size(Line) - 2 - 8 - 1 - 2,
            FileLenLF = byte_size(Line)   - 2 - 16 - 1 - 8 - 1 - 1,
            FileLenCRLF = byte_size(Line) - 2 - 16 - 1 - 8 - 1 - 2,
            WriteFileLenLF = byte_size(Line) - 7 - 16 - 1 - 8 - 1 - 1,
            DelFileLenLF = byte_size(Line) - 14 - 1,
            case Line of
                %% For normal use
                <<"A ", HexLen:8/binary, " ",
                  Prefix:PrefixLenLF/binary, "\n">> ->
                    do_net_server_append(Sock, HexLen, Prefix);
                <<"A ", HexLen:8/binary, " ",
                  Prefix:PrefixLenCRLF/binary, "\r\n">> ->
                    do_net_server_append(Sock, HexLen, Prefix);
                <<"R ", HexOffset:16/binary, " ", HexLen:8/binary, " ",
                  File:FileLenLF/binary, "\n">> ->
                    do_net_server_read(Sock, HexOffset, HexLen, File, DataDir);
                <<"R ", HexOffset:16/binary, " ", HexLen:8/binary, " ",
                  File:FileLenCRLF/binary, "\r\n">> ->
                    do_net_server_read(Sock, HexOffset, HexLen, File, DataDir);
                <<"L\n">> ->
                    do_net_server_listing(Sock, DataDir);
                <<"L\r\n">> ->
                    do_net_server_listing(Sock, DataDir);
                <<"QUIT\n">> ->
                    catch gen_tcp:close(Sock),
                    exit(normal);
                <<"QUIT\r\n">> ->
                    catch gen_tcp:close(Sock),
                    exit(normal);
                %% For "internal" replication only.
                <<"W-repl ", HexOffset:16/binary, " ", HexLen:8/binary, " ",
                  File:WriteFileLenLF/binary, "\n">> ->
                    do_net_server_write(Sock, HexOffset, HexLen, File, DataDir);
                %% For data migration only.
                <<"DEL-migration ", File:DelFileLenLF/binary, "\n">> ->
                    do_net_server_delete_migration_only(Sock, File, DataDir);
                _ ->
                    fmt("Else Got: ~p\n", [Line]),
                    gen_tcp:send(Sock, "ERROR\n"),
                    catch gen_tcp:close(Sock),
                    exit(normal)
            end,
            net_server_loop(Sock, DataDir);
        _ ->
            catch gen_tcp:close(Sock),
            exit(normal)
    end.

do_net_server_append(Sock, HexLen, Prefix) ->
    <<Len:32/big>> = hexstr_to_bin(HexLen),
    ok = inet:setopts(Sock, [{packet, raw}]),
    {ok, Chunk} = gen_tcp:recv(Sock, Len, 60*1000),
    ?MODULE ! {seq_append, self(), Prefix, Chunk},
    receive
        {assignment, File, Offset} ->
            OffsetHex = bin_to_hexstr(<<Offset:64/big>>),
            Out = io_lib:format("OK ~s ~s\n", [OffsetHex, File]),
            ok = gen_tcp:send(Sock, Out)
    after 10*1000 ->
            ok = gen_tcp:send(Sock, "TIMEOUT\n")
    end.

do_net_server_read(Sock, HexOffset, HexLen, FileBin, DataDir) ->
    DoItFun = fun(FH, Offset, Len) ->
                      case file:pread(FH, Offset, Len) of
                          {ok, Bytes} when byte_size(Bytes) == Len ->
                              gen_tcp:send(Sock, ["OK\n", Bytes]);
                          _Else2 ->
                              fmt("Else2 ~p ~p ~p\n",
                                        [Offset, Len, _Else2]),
                              ok = gen_tcp:send(Sock, "ERROR\n")
                      end
              end,
    do_net_server_readwrite_common(Sock, HexOffset, HexLen, FileBin, DataDir,
                                   [read, binary, raw], DoItFun).

do_net_server_readwrite_common(Sock, HexOffset, HexLen, FileBin, DataDir,
                               FileOpts, DoItFun) ->
    <<Offset:64/big>> = hexstr_to_bin(HexOffset),
    <<Len:32/big>> = hexstr_to_bin(HexLen),
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
              Sock, HexOffset, HexLen, FileBin, DataDir,
              FileOpts, DoItFun);
        _Else ->
            fmt("Else ~p ~p ~p ~p\n", [Offset, Len, Path, _Else]),
            ok = gen_tcp:send(Sock, "ERROR\n")
    end.


do_net_server_write(Sock, HexOffset, HexLen, FileBin, DataDir) ->
    DoItFun = fun(FH, Offset, Len) ->
                      ok = inet:setopts(Sock, [{packet, raw}]),
                      {ok, Chunk} = gen_tcp:recv(Sock, Len),
                      case file:pwrite(FH, Offset, Chunk) of
                          ok ->
                              gen_tcp:send(Sock, <<"OK\n">>);
                          _Else2 ->
                              fmt("Else2 ~p ~p ~p\n",
                                        [Offset, Len, _Else2]),
                              ok = gen_tcp:send(Sock, "ERROR\n")
                      end
              end,
    do_net_server_readwrite_common(Sock, HexOffset, HexLen, FileBin, DataDir,
                                   [write, read, binary, raw], DoItFun).

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

do_net_server_delete_migration_only(Sock, File, DataDir) ->
    {_, Path} = make_data_filename(DataDir, File),
    case file:delete(Path) of
        ok ->
            ok = gen_tcp:send(Sock, "OK\n");
        _ ->
            ok = gen_tcp:send(Sock, "ERROR\n")
    end.

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

append_server_dispatch(From, Prefix, Chunk, DataDir) ->
    %% _ = crypto:hash(md5, Chunk),
    Pid = write_server_get_pid(Prefix, DataDir),
    Pid ! {seq_append, From, Prefix, Chunk},
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
    {File, FullPath} = make_data_filename(DataDir, Prefix, FileNum),
    {ok, FH} = file:open(FullPath,
                         [write, binary, raw]),
                         %% [write, binary, raw, delayed_write]),
    seq_append_server_loop(DataDir, Prefix, File, FH, FileNum, 0).

seq_append_server_loop(DataDir, Prefix, _File, FH, FileNum, Offset)
  when Offset > ?MAX_FILE_SIZE ->
    ok = file:close(FH),
    info_msg("rollover: ~p server at file ~w offset ~w\n",
             [Prefix, FileNum, Offset]),
    run_seq_append_server2(Prefix, DataDir);    
seq_append_server_loop(DataDir, Prefix, File, FH, FileNum, Offset) ->
    receive
        {seq_append, From, Prefix, Chunk} ->
            ok = file:pwrite(FH, Offset, Chunk),
            From ! {assignment, File, Offset},
            Size = byte_size(Chunk),
            seq_append_server_loop(DataDir, Prefix, File, FH,
                                   FileNum, Offset + Size)
    after 30*1000 ->
            info_msg("stop: ~p server at file ~w offset ~w\n",
                     [Prefix, FileNum, Offset]),
            exit(normal)
    end.

make_regname(Prefix) ->
    erlang:binary_to_atom(Prefix, latin1).

make_config_filename(DataDir, Prefix) ->
    lists:flatten(io_lib:format("~s/config/~s", [DataDir, Prefix])).

make_data_filename(DataDir, File) ->
    FullPath = lists:flatten(io_lib:format("~s/~s",  [DataDir, File])),
    {File, FullPath}.

make_data_filename(DataDir, Prefix, FileNum) ->
    File = erlang:iolist_to_binary(io_lib:format("~s.~w", [Prefix, FileNum])),
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

bin_to_hexstr(Bin) ->
  lists:flatten([io_lib:format("~2.16.0B", [X]) ||
    X <- binary_to_list(Bin)]).

%%%%%%%%%%%%%%%%%

%%% escript stuff

main2(["file-write-client", Host, PortStr, BlockSizeStr, PrefixStr, LocalFile]) ->
    Sock = escript_connect(Host, PortStr),
    BlockSize = list_to_integer(BlockSizeStr),
    Prefix = list_to_binary(PrefixStr),
    escript_upload_file(Sock, BlockSize, Prefix, LocalFile);
main2(["1file-write-redundant-client", BlockSizeStr, PrefixStr, LocalFile|HPs]) ->
    BlockSize = list_to_integer(BlockSizeStr),
    Prefix = list_to_binary(PrefixStr),
    escript_upload_redundant(HPs, BlockSize, Prefix, LocalFile);
main2(["chunk-read-client", Host, PortStr, ChunkFileList]) ->
    Sock = escript_connect(Host, PortStr),
    escript_download_chunks(Sock, ChunkFileList);
main2(["list-client", Host, PortStr]) ->
    Sock = escript_connect(Host, PortStr),
    escript_list(Sock);
main2(["delete-client", Host, PortStr, File]) ->
    Sock = escript_connect(Host, PortStr),
    escript_delete(Sock, File);
main2(["server", RegNameStr, PortStr, DataDir]) ->
    Port = list_to_integer(PortStr),
    %% application:start(sasl),
    _Pid1 = start_listen_server(Port, DataDir),
    _Pid2 = start_append_server(list_to_atom(RegNameStr), DataDir),
    receive forever -> ok end.

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

escript_upload_file2({ok, Bin}, FH, BlockSize, Prefix, Sock, Acc) ->
    {OffsetHex, SizeHex, File} = upload_chunk_append(Sock, Prefix, Bin),
    fmt("~s ~s ~s\n", [OffsetHex, SizeHex, File]),
    <<Offset:64/big>> = hexstr_to_bin(OffsetHex),
    <<Size:32/big>> = hexstr_to_bin(SizeHex),
    OSF = {Offset, Size, File},
    escript_upload_file2(file:read(FH, BlockSize), FH, BlockSize, Prefix, Sock,
                        [OSF|Acc]);
escript_upload_file2(eof, _FH, _BlockSize, _Prefix, _Sock, Acc) ->
    lists:reverse(Acc).        

upload_chunk_append(Sock, Prefix, Bin) ->
    %% _ = crypto:hash(md5, Bin),
    Size = byte_size(Bin),
    SizeHex = list_to_binary(bin_to_hexstr(<<Size:32/big>>)),
    Cmd = <<"A ", SizeHex/binary, " ", Prefix/binary, "\n">>,
    ok = gen_tcp:send(Sock, [Cmd, Bin]),
    {ok, Line} = gen_tcp:recv(Sock, 0),
    PathLen = byte_size(Line) - 3 - 16 - 1 - 1,
    <<"OK ", OffsetHex:16/binary, " ", Path:PathLen/binary, _:1/binary>> = Line,
    %% <<Offset:64/big>> = hexstr_to_bin(OffsetHex),
    {OffsetHex, SizeHex, Path}.

upload_chunk_write(Sock, Offset, File, Bin) ->
    %% _ = crypto:hash(md5, Bin),
    Size = byte_size(Bin),
    OffsetHex = list_to_binary(bin_to_hexstr(<<Offset:64/big>>)),
    SizeHex = list_to_binary(bin_to_hexstr(<<Size:32/big>>)),
    Cmd = <<"W-repl ", OffsetHex/binary, " ",
            SizeHex/binary, " ", File/binary, "\n">>,
    ok = gen_tcp:send(Sock, [Cmd, Bin]),
    {ok, Line} = gen_tcp:recv(Sock, 0),
    <<"OK\n">> = Line,
    {OffsetHex, SizeHex, File}.

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
             %% fmt("~p: ~p\n", [{Host, PortStr}, OSF2])
         end || {Offset, Size, File} <- OSFs]
    after
        gen_tcp:close(Sock),
        file:close(FH)
    end,
    escript_upload_redundant2(HPs, OSFs, LocalFile, OSFs).

escript_download_chunks(Sock, ChunkFileList) ->
    {ok, FH} = file:open(ChunkFileList, [read, raw, binary]),
    escript_dowload_chunk(file:read_line(FH), FH, Sock).

escript_dowload_chunk({ok, Line}, FH, Sock) ->
    %% Line includes an LF, so we can be lazy.
    CmdLF = ["R ", Line],
    ok = gen_tcp:send(Sock, CmdLF),
    {ok, <<"OK\n">>} = gen_tcp:recv(Sock, 3),
    Size = read_hex_size(Line),
    {ok, _Chunk} = gen_tcp:recv(Sock, Size),
    fmt("ok\n"),
    escript_dowload_chunk(file:read_line(FH), FH, Sock);
escript_dowload_chunk(eof, _FH, _Sock) ->    
    ok.

escript_list(Sock) ->
    ok = gen_tcp:send(Sock, <<"L\n">>),
    ok = inet:setopts(Sock, [{packet, line}]),
    {ok, <<"OK\n">>} = gen_tcp:recv(Sock, 0),
    Res = escript_list(gen_tcp:recv(Sock, 0), Sock),
    ok = inet:setopts(Sock, [{packet, raw}]),
    Res.

escript_list({ok, <<".\n">>}, _Sock) ->
    [];
escript_list({ok, Line}, Sock) ->
    fmt("~s", [Line]),
    [Line|escript_list(gen_tcp:recv(Sock, 0), Sock)];
escript_list(Else, _Sock) ->
    fmt("ERROR: ~p\n", [Else]),
    {error, Else}.

escript_delete(Sock, File) ->
    ok = gen_tcp:send(Sock, [<<"DEL-migration ">>, File, <<"\n">>]),
    ok = inet:setopts(Sock, [{packet, line}]),
    case gen_tcp:recv(Sock, 0) of
        {ok, <<"OK\n">>} ->
            ok;
        {ok, <<"ERROR\n">>} ->
            error
    end.

fmt(Fmt) ->
    fmt(Fmt, []).

fmt(Fmt, Args) ->
    case application:get_env(kernel, verbose) of {ok, false} -> ok;
                                                 _     -> io:format(Fmt, Args)
    end.

info_msg(Fmt, Args) ->
    case application:get_env(kernel, verbose) of {ok, false} -> ok;
                                                 _     -> error_logger:info_msg(Fmt, Args)
    end.

%%%%%%%%%%%%%%%%%

%%% basho_bench callbacks

-define(SEQ, ?MODULE).
-define(DEFAULT_HOSTIP_LIST, [{{127,0,0,1}, 7071}]).

-record(bb, {
          sock
         }).

new(1 = Id) ->
    start_append_server(),
    case basho_bench_config:get(file0_start_listener, no) of
        no ->
            ok;
        Port ->
            start_listen_server(Port)
    end,
    timer:sleep(100),
    new_common(Id);
new(Id) ->
    new_common(Id).

new_common(Id) ->
    Servers = basho_bench_config:get(file0_ip_list, ?DEFAULT_HOSTIP_LIST),
    NumServers = length(Servers),
    {Host, Port} = lists:nth((Id rem NumServers) + 1, Servers),
    SockOpts = [{mode, binary}, {packet, raw}, {active, false}],
    {ok, Sock} = gen_tcp:connect(Host, Port, SockOpts),
    {ok, #bb{sock=Sock}}.

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
    {_, _, _} = upload_chunk_append(State#bb.sock, Prefix, Value),
    {ok, State};
run(read_raw_line_local, KeyGen, _ValueGen, State) ->
    {RawLine, Size} = setup_read_raw_line(KeyGen),
    ok = gen_tcp:send(State#bb.sock, [RawLine, <<"\n">>]),
    {ok, <<"OK\n">>} = gen_tcp:recv(State#bb.sock, 3),
    {ok, _Chunk} = gen_tcp:recv(State#bb.sock, Size),
    {ok, State}.

setup_read_raw_line(KeyGen) ->
    RawLine = KeyGen(),
    <<"R ", Rest/binary>> = RawLine,
    Size = read_hex_size(Rest),
    {RawLine, Size}.

read_hex_size(Line) ->
    <<_Offset:16/binary, " ", SizeBin:8/binary, _/binary>> = Line,
    <<Size:32/big>> = hexstr_to_bin(SizeBin),
    Size.
