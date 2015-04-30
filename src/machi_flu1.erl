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

%% @doc The Machi FLU file server + file location sequencer.
%%
%% This module implements only the Machi FLU file server and its
%% implicit sequencer.  
%% Please see the EDoc "Overview" for details about the FLU as a
%% primitive file server process vs. the larger Machi design of a FLU
%% as a sequencer + file server + chain manager group of processes.
%%
%% For the moment, this module also implements a rudimentary TCP-based
%% protocol as the sole supported access method to the server,
%% sequencer, and projection store.  Conceptually, those three
%% services are independent and ought to have their own protocols.  As
%% a practical matter, there is no need for wire protocol
%% compatibility.  Furthermore, from the perspective of failure
%% detection, it is very convenient that all three FLU-related
%% services are accessed using the same single TCP port.
%%
%% The FLU is named after the CORFU server "FLU" or "FLash Unit" server.
%%
%% TODO There is a major missing feature in this FLU implementation:
%% there is no "write-once" enforcement for any position in a Machi
%% file.  At the moment, we rely on correct behavior of the client
%% &amp; the sequencer to avoid overwriting data.  In the Real World,
%% however, all Machi file data is supposed to be exactly write-once
%% to avoid problems with bugs, wire protocol corruption, malicious
%% clients, etc.
%%
%% TODO The per-file metadata tuple store is missing from this implementation.
%%
%% TODO Section 4.1 ("The FLU") of the Machi design doc suggests that
%% the FLU keep track of the epoch number of the last file write (and
%% perhaps last metadata write), as an optimization for inter-FLU data
%% replication/chain repair.
%%
%% TODO Section 4.2 ("The Sequencer") says that the sequencer must
%% change its file assignments to new & unique names whenever we move
%% to wedge state.  This is not yet implemented.  In the current
%% Erlang process scheme (which will probably be changing soon), a
%% simple implementation would stop all existing processes that are
%% running run_seq_append_server().

-module(machi_flu1).

-include_lib("kernel/include/file.hrl").

-include("machi.hrl").
-include("machi_projection.hrl").

-export([start_link/1, stop/1]).

-record(state, {
          flu_name        :: atom(),
          proj_store      :: pid(),
          append_pid      :: pid(),
          tcp_port        :: non_neg_integer(),
          data_dir        :: string(),
          wedge = true    :: 'disabled' | boolean(),
          my_epoch_id     :: 'undefined',
          dbg_props = []  :: list(), % proplist
          props = []      :: list()  % proplist
         }).

start_link([{FluName, TcpPort, DataDir}|Rest])
  when is_atom(FluName), is_integer(TcpPort), is_list(DataDir) ->
    {ok, spawn_link(fun() -> main2(FluName, TcpPort, DataDir, Rest) end)}.

stop(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! killme,
            ok;
        false ->
            error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

main2(FluName, TcpPort, DataDir, Rest) ->
    S0 = #state{flu_name=FluName,
                tcp_port=TcpPort,
                data_dir=DataDir,
                props=Rest},
    AppendPid = start_append_server(S0),
    {_ProjRegName, ProjectionPid} =
        case proplists:get_value(projection_store_registered_name, Rest) of
            undefined ->
                RN = make_projection_server_regname(FluName),
                {ok, PP} =
                    machi_projection_store:start_link(RN, DataDir, AppendPid),
                {RN, PP};
            RN ->
                {RN, whereis(RN)}
        end,
    S1 = S0#state{append_pid=AppendPid,
                  proj_store=ProjectionPid},
    S2 = case proplists:get_value(dbg, Rest) of
             undefined ->
                 S1;
             DbgProps ->
                 S1#state{wedge=disabled,
                          dbg_props=DbgProps,
                          props=lists:keydelete(dbg, 1, Rest)}
         end,
    ListenPid = start_listen_server(S2),

    Config_e = machi_util:make_config_filename(DataDir, "unused"),
    ok = filelib:ensure_dir(Config_e),
    {_, Data_e} = machi_util:make_data_filename(DataDir, "unused"),
    ok = filelib:ensure_dir(Data_e),
    Projection_e = machi_util:make_projection_filename(DataDir, "unused"),
    ok = filelib:ensure_dir(Projection_e),

    put(flu_flu_name, FluName),
    put(flu_append_pid, AppendPid),
    put(flu_projection_pid, ProjectionPid),
    put(flu_listen_pid, ListenPid),
    receive killme -> ok end,
    (catch exit(AppendPid, kill)),
    (catch exit(ProjectionPid, kill)),
    (catch exit(ListenPid, kill)),
    ok.

start_listen_server(S) ->
    proc_lib:spawn_link(fun() -> run_listen_server(S) end).

start_append_server(S) ->
    FluPid = self(),
    proc_lib:spawn_link(fun() -> run_append_server(FluPid, S) end).

%% start_projection_server(S) ->
%%     spawn_link(fun() -> run_projection_server(S) end).

run_listen_server(#state{flu_name=FluName, tcp_port=TcpPort}=S) ->
    register(make_listener_regname(FluName), self()),
    SockOpts = [{reuseaddr, true},
                {mode, binary}, {active, false}, {packet, line}],
    {ok, LSock} = gen_tcp:listen(TcpPort, SockOpts),
    listen_server_loop(LSock, S).

run_append_server(FluPid, #state{flu_name=Name}=S) ->
    register(Name, self()),
    append_server_loop(FluPid, S).

listen_server_loop(LSock, S) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    spawn_link(fun() -> net_server_loop(Sock, S) end),
    listen_server_loop(LSock, S).

append_server_loop(FluPid, #state{data_dir=DataDir}=S) ->
    AppendServerPid = self(),
    receive
        {seq_append, From, Prefix, Chunk, CSum} ->
            spawn(fun() -> append_server_dispatch(From, Prefix, Chunk, CSum,
                                                  DataDir, AppendServerPid) end),
                                                  %% DataDir, FluPid) end),
            append_server_loop(FluPid, S);
        {wedge_state_change, Boolean} ->
            append_server_loop(FluPid, S#state{wedge=Boolean})
    end.

-define(EpochIDSpace, (4+20)).

net_server_loop(Sock, #state{flu_name=FluName, data_dir=DataDir}=S) ->
    ok = inet:setopts(Sock, [{packet, line}]),
    case gen_tcp:recv(Sock, 0, 600*1000) of
        {ok, Line} ->
            %% machi_util:verb("Got: ~p\n", [Line]),
            PrefixLenLF = byte_size(Line) - 2 - ?EpochIDSpace - 8 - 1,
            FileLenLF = byte_size(Line)   - 2 - ?EpochIDSpace - 16 - 8 - 1,
            CSumFileLenLF = byte_size(Line) - 2 - ?EpochIDSpace - 1,
            WriteFileLenLF = byte_size(Line) - 7 - ?EpochIDSpace - 16 - 8 - 1,
            DelFileLenLF = byte_size(Line) - 14 - ?EpochIDSpace - 1,
            case Line of
                %% For normal use
                <<"A ",
                  _EpochIDRaw:(?EpochIDSpace)/binary,
                  LenHex:8/binary,
                  Prefix:PrefixLenLF/binary, "\n">> ->
                    do_net_server_append(FluName, Sock, LenHex, Prefix);
                <<"R ",
                  _EpochIDRaw:(?EpochIDSpace)/binary,
                  OffsetHex:16/binary, LenHex:8/binary,
                  File:FileLenLF/binary, "\n">> ->
                    do_net_server_read(Sock, OffsetHex, LenHex, File, DataDir);
                <<"L ", _EpochIDRaw:(?EpochIDSpace)/binary, "\n">> ->
                    do_net_server_listing(Sock, DataDir);
                <<"C ",
                  _EpochIDRaw:(?EpochIDSpace)/binary,
                  File:CSumFileLenLF/binary, "\n">> ->
                    do_net_server_checksum_listing(Sock, File, DataDir);
                <<"QUIT\n">> ->
                    catch gen_tcp:close(Sock),
                    exit(normal);
                <<"QUIT\r\n">> ->
                    catch gen_tcp:close(Sock),
                    exit(normal);
                %% For "internal" replication only.
                <<"W-repl ",
                  _EpochIDRaw:(?EpochIDSpace)/binary,
                  OffsetHex:16/binary, LenHex:8/binary,
                  File:WriteFileLenLF/binary, "\n">> ->
                    do_net_server_write(Sock, OffsetHex, LenHex, File, DataDir);
                %% For data migration only.
                <<"DEL-migration ",
                  _EpochIDRaw:(?EpochIDSpace)/binary,
                  File:DelFileLenLF/binary, "\n">> ->
                    do_net_server_delete_migration_only(Sock, File, DataDir);
                %% For erasure coding hackityhack
                <<"TRUNC-hack--- ",
                  _EpochIDRaw:(?EpochIDSpace)/binary,
                  File:DelFileLenLF/binary, "\n">> ->
                    do_net_server_truncate_hackityhack(Sock, File, DataDir);
                <<"PROJ ", LenHex:8/binary, "\n">> ->
                    do_projection_command(Sock, LenHex, S);
                _ ->
                    machi_util:verb("Else Got: ~p\n", [Line]),
                    gen_tcp:send(Sock, "ERROR SYNTAX\n"),
                    catch gen_tcp:close(Sock),
                    exit(normal)
            end,
            net_server_loop(Sock, S);
        _ ->
            catch gen_tcp:close(Sock),
            exit(normal)
    end.

append_server_dispatch(From, Prefix, Chunk, CSum, DataDir, LinkPid) ->
    Pid = write_server_get_pid(Prefix, DataDir, LinkPid),
    Pid ! {seq_append, From, Prefix, Chunk, CSum},
    exit(normal).

do_net_server_append(FluName, Sock, LenHex, Prefix) ->
    %% TODO: robustify against other invalid path characters such as NUL
    case sanitize_file_string(Prefix) of
        ok ->
            do_net_server_append2(FluName, Sock, LenHex, Prefix);
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

do_net_server_append2(FluName, Sock, LenHex, Prefix) ->
    <<Len:32/big>> = machi_util:hexstr_to_bin(LenHex),
    ok = inet:setopts(Sock, [{packet, raw}]),
    {ok, Chunk} = gen_tcp:recv(Sock, Len, 60*1000),
    CSum = machi_util:checksum_chunk(Chunk),
    try
        FluName ! {seq_append, self(), Prefix, Chunk, CSum}
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
                      CSum = machi_util:checksum_chunk(Chunk),
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
    {_, WildPath} = machi_util:make_data_filename(DataDir, ""),
    Files = filelib:wildcard("*", WildPath),
    Out = ["OK\n",
           [begin
                {ok, FI} = file:read_file_info(WildPath ++ "/" ++ File),
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

write_server_get_pid(Prefix, DataDir, LinkPid) ->
    case write_server_find_pid(Prefix) of
        undefined ->
            start_seq_append_server(Prefix, DataDir, LinkPid),
            timer:sleep(1),
            write_server_get_pid(Prefix, DataDir, LinkPid);
        Pid ->
            Pid
    end.

write_server_find_pid(Prefix) ->
    FluName = machi_util:make_regname(Prefix),
    whereis(FluName).

start_seq_append_server(Prefix, DataDir, AppendServerPid) ->
    proc_lib:spawn_link(fun() ->
                                %% The following is only necessary to
                                %% make nice process relationships in
                                %% 'appmon' and related tools.
                                put('$ancestors', [AppendServerPid]),
                                put('$initial_call', {x,y,3}),
                                link(AppendServerPid),
                                run_seq_append_server(Prefix, DataDir)
                        end).

run_seq_append_server(Prefix, DataDir) ->
    true = register(machi_util:make_regname(Prefix), self()),
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

-spec seq_name_hack() -> string().
seq_name_hack() ->
    lists:flatten(io_lib:format("~.36B~.36B",
                                [element(3,now()),
                                 list_to_integer(os:getpid())])).

seq_append_server_loop(DataDir, Prefix, FileNum) ->
    SequencerNameHack = seq_name_hack(),
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
            machi_util:info_msg("stop: ~p server ~p at file ~w offset ~w\n",
                                [Prefix, self(), FileNum, Offset]),
            exit(normal)
    end.

do_projection_command(Sock, LenHex, S) ->
    try
        Len = machi_util:hexstr_to_int(LenHex),
        ok = inet:setopts(Sock, [{packet, raw}]),
        {ok, ProjCmdBin} = gen_tcp:recv(Sock, Len),
        ok = inet:setopts(Sock, [{packet, line}]),
        ProjCmd = binary_to_term(ProjCmdBin),
        put(hack, ProjCmd),
        Res = handle_projection_command(ProjCmd, S),
        ResBin = term_to_binary(Res),
        ResLenHex = machi_util:int_to_hexbin(byte_size(ResBin), 32),
        ok = gen_tcp:send(Sock, [<<"OK ">>, ResLenHex, <<"\n">>, ResBin])
    catch
        What:Why ->
            io:format(user, "OOPS ~p\n", [get(hack)]),
            io:format(user, "OOPS ~p ~p ~p\n", [What, Why, erlang:get_stacktrace()]),
            WHA = list_to_binary(io_lib:format("TODO-YOLO.~w:~w-~w",
                                               [What, Why, erlang:get_stacktrace()])),
            _ = (catch gen_tcp:send(Sock, [<<"ERROR ">>, WHA, <<"\n">>]))
    end.

handle_projection_command({get_latest_epoch, ProjType},
                          #state{proj_store=ProjStore}) ->
    machi_projection_store:get_latest_epoch(ProjStore, ProjType);
handle_projection_command({read_latest_projection, ProjType},
                          #state{proj_store=ProjStore}) ->
    machi_projection_store:read_latest_projection(ProjStore, ProjType);
handle_projection_command({read_projection, ProjType, Epoch},
                          #state{proj_store=ProjStore}) ->
    machi_projection_store:read(ProjStore, ProjType, Epoch);
handle_projection_command({write_projection, ProjType, Proj},
                          #state{proj_store=ProjStore}) ->
    machi_projection_store:write(ProjStore, ProjType, Proj);
handle_projection_command({get_all_projections, ProjType},
                          #state{proj_store=ProjStore}) ->
    machi_projection_store:get_all_projections(ProjStore, ProjType);
handle_projection_command({list_all_projections, ProjType},
                          #state{proj_store=ProjStore}) ->
    machi_projection_store:list_all_projections(ProjStore, ProjType);
handle_projection_command(Else, _S) ->
    {error, unknown_cmd, Else}.

make_listener_regname(BaseName) ->
    list_to_atom(atom_to_list(BaseName) ++ "_listener").

make_projection_server_regname(BaseName) ->
    list_to_atom(atom_to_list(BaseName) ++ "_pstore2").
