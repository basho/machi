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
%% The FLU is named after the CORFU server "FLU" or "FLash Unit" server.
%%
%% === Protocol origins ===
%%
%% The protocol implemented here is an artisanal, hand-crafted, silly
%% thing that was very quick to put together for a "demo day" proof of
%% concept.  It will almost certainly be replaced with something else,
%% both in terms of wire format and better code separation of
%% serialization/deserialization vs. network transport management,
%% etc.
%%
%% For the moment, this module implements a rudimentary TCP-based
%% protocol as the sole supported access method to the server,
%% sequencer, and projection store.  Conceptually, those three
%% services are independent and ought to have their own protocols.  As
%% a practical matter, there is no need for wire protocol
%% compatibility.  Furthermore, from the perspective of failure
%% detection, it is very convenient that all three FLU-related
%% services are accessed using the same single TCP port.
%%
%% === TODO items ===
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
%% change its file assignments to new &amp; unique names whenever we move
%% to wedge state.  This is not yet implemented.  In the current
%% Erlang process scheme (which will probably be changing soon), a
%% simple implementation would stop all existing processes that are
%% running run_seq_append_server().

-module(machi_flu1).

-include_lib("kernel/include/file.hrl").

-include("machi.hrl").
-include("machi_projection.hrl").

-define(SERVER_CMD_READ_TIMEOUT, 600*1000).

-export([start_link/1, stop/1,
         update_wedge_state/3]).
-export([make_listener_regname/1, make_projection_server_regname/1]).

-record(state, {
          flu_name        :: atom(),
          proj_store      :: pid(),
          append_pid      :: pid(),
          tcp_port        :: non_neg_integer(),
          data_dir        :: string(),
          wedged = true   :: boolean(),
          etstab          :: ets:tid(),
          epoch_id        :: 'undefined' | machi_dt:epoch_id(),
          dbg_props = []  :: list(), % proplist
          props = []      :: list()  % proplist
         }).

-record(http_goop, {
          len,                                  % content-length
          x_csum                                % x-checksum
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

update_wedge_state(PidSpec, Boolean, EpochId)
  when (Boolean == true orelse Boolean == false), is_tuple(EpochId) ->
    PidSpec ! {wedge_state_change, Boolean, EpochId}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ets_table_name(FluName) when is_atom(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_epoch").
%% ets_table_name(FluName) when is_binary(FluName) ->
%%     list_to_atom(binary_to_list(FluName) ++ "_epoch").

main2(FluName, TcpPort, DataDir, Rest) ->
    {Props, DbgProps} =  case proplists:get_value(dbg, Rest) of
                             undefined ->
                                 {Rest, []};
                             DPs ->
                                 {lists:keydelete(dbg, 1, Rest), DPs}
                         end,
    {SendAppendPidToProj_p, ProjectionPid} =
        case proplists:get_value(projection_store_registered_name, Rest) of
            undefined ->
                RN = make_projection_server_regname(FluName),
                {ok, PP} =
                    machi_projection_store:start_link(RN, DataDir, undefined),
                {true, PP};
            RN ->
                {false, whereis(RN)}
        end,
    InitialWedged_p = proplists:get_value(initial_wedged, DbgProps),
    ProjRes = machi_projection_store:read_latest_projection(ProjectionPid,
                                                            private),
    {Wedged_p, EpochId} =
        if InitialWedged_p == undefined,
           is_tuple(ProjRes), element(1, ProjRes) == ok ->
                {ok, Proj} = ProjRes,
                {false, {Proj#projection_v1.epoch_number,
                         Proj#projection_v1.epoch_csum}};
           InitialWedged_p == false ->
                {false, ?DUMMY_PV1_EPOCH};
           true ->
                {true, undefined}
        end,
    S0 = #state{flu_name=FluName,
                proj_store=ProjectionPid,
                tcp_port=TcpPort,
                data_dir=DataDir,
                wedged=Wedged_p,
                etstab=ets_table_name(FluName),
                epoch_id=EpochId,
                dbg_props=DbgProps,
                props=Props},
    AppendPid = start_append_server(S0, self()),
    receive
        append_server_ack -> ok
    end,
    if SendAppendPidToProj_p ->
            machi_projection_store:set_wedge_notify_pid(ProjectionPid,
                                                        AppendPid);
       true ->
            ok
    end,
    S1 = S0#state{append_pid=AppendPid},
    ListenPid = start_listen_server(S1),

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

start_append_server(S, AckPid) ->
    FluPid = self(),
    proc_lib:spawn_link(fun() -> run_append_server(FluPid, AckPid, S) end).

%% start_projection_server(S) ->
%%     spawn_link(fun() -> run_projection_server(S) end).

run_listen_server(#state{flu_name=FluName, tcp_port=TcpPort}=S) ->
    register(make_listener_regname(FluName), self()),
    SockOpts = [{reuseaddr, true},
                {mode, binary}, {active, false}, {packet, line}],
    case gen_tcp:listen(TcpPort, SockOpts) of
        {ok, LSock} ->
            listen_server_loop(LSock, S);
        Else ->
            error_logger:warning_msg("~s:run_listen_server: "
                                     "listen to TCP port ~w: ~w\n",
                                     [?MODULE, TcpPort, Else]),
            exit({?MODULE, run_listen_server, tcp_port, TcpPort, Else})
    end.

run_append_server(FluPid, AckPid, #state{flu_name=Name,
                                         wedged=Wedged_p,epoch_id=EpochId}=S) ->
    %% Reminder: Name is the "main" name of the FLU, i.e., no suffix
    register(Name, self()),
    TID = ets:new(ets_table_name(Name),
                  [set, protected, named_table, {read_concurrency, true}]),
    %% InitialWedged = proplists:get_value(initial_wedged, DbgProps, true),
    %% ets:insert(TID, {epoch, {InitialWedged, {-65, <<"bogus epoch, yo">>}}}),
    ets:insert(TID, {epoch, {Wedged_p, EpochId}}),
    AckPid ! append_server_ack,
    append_server_loop(FluPid, S#state{etstab=TID}).

listen_server_loop(LSock, S) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    spawn_link(fun() -> net_server_loop(Sock, S) end),
    listen_server_loop(LSock, S).

append_server_loop(FluPid, #state{data_dir=DataDir,wedged=Wedged_p}=S) ->
    AppendServerPid = self(),
    receive
        {seq_append, From, _Prefix, _Chunk, _CSum, _Extra} when Wedged_p ->
            From ! wedged,
            append_server_loop(FluPid, S);
        {seq_append, From, Prefix, Chunk, CSum, Extra} ->
            spawn(fun() -> append_server_dispatch(From, Prefix,
                                                 Chunk, CSum, Extra,
                                                 DataDir, AppendServerPid) end),
            append_server_loop(FluPid, S);
        {wedge_state_change, Boolean, EpochId} ->
            true = ets:insert(S#state.etstab, {epoch, {Boolean, EpochId}}),
            append_server_loop(FluPid, S#state{wedged=Boolean,
                                               epoch_id=EpochId});
        {wedge_status, FromPid} ->
            #state{wedged=Wedged_p, epoch_id=EpochId} = S,
            FromPid ! {wedge_status_reply, Wedged_p, EpochId},
            append_server_loop(FluPid, S);
        Else ->
            io:format(user, "append_server_loop: WHA? ~p\n", [Else]),
            append_server_loop(FluPid, S)
    end.

-define(EpochIDSpace, ((4*2)+(20*2))).          % hexencodingwhee!
-define(CSumSpace, ((1*2)+(20*2))).             % hexencodingwhee!

decode_epoch_id(EpochIDHex) ->
    <<EpochNum:(4*8)/big, EpochCSum/binary>> =
        machi_util:hexstr_to_bin(EpochIDHex),
    {EpochNum, EpochCSum}.

net_server_loop(Sock, #state{flu_name=FluName, data_dir=DataDir}=S) ->
    ok = inet:setopts(Sock, [{packet, line}]),
    %% TODO: Add testing control knob to adjust this timeout and/or inject
    %% timeout condition.
    case gen_tcp:recv(Sock, 0, ?SERVER_CMD_READ_TIMEOUT) of
        {ok, Line} ->
            %% machi_util:verb("Got: ~p\n", [Line]),
            PrefixLenLF = byte_size(Line) - 2 - ?EpochIDSpace - ?CSumSpace
                                                                    - 8 - 8 - 1,
            FileLenLF = byte_size(Line)   - 2 - ?EpochIDSpace - 16 - 8 - 1,
            CSumFileLenLF = byte_size(Line) - 2 - ?EpochIDSpace - 1,
            WriteFileLenLF = byte_size(Line) - 7 - ?EpochIDSpace - ?CSumSpace
                                                                   - 16 - 8 - 1,
            DelFileLenLF = byte_size(Line) - 14 - ?EpochIDSpace - 1,
            case Line of
                %% For normal use
                <<"A ",
                  EpochIDHex:(?EpochIDSpace)/binary,
                  CSumHex:(?CSumSpace)/binary,
                  LenHex:8/binary, ExtraHex:8/binary,
                  Prefix:PrefixLenLF/binary, "\n">> ->
                    _EpochID = decode_epoch_id(EpochIDHex),
                    do_net_server_append(FluName, Sock, CSumHex,
                                         LenHex, ExtraHex, Prefix);
                <<"R ",
                  EpochIDHex:(?EpochIDSpace)/binary,
                  OffsetHex:16/binary, LenHex:8/binary,
                  File:FileLenLF/binary, "\n">> ->
                    EpochID = decode_epoch_id(EpochIDHex),
                    do_net_server_read(Sock, OffsetHex, LenHex, File, DataDir,
                                       EpochID, S);
                <<"L ", EpochIDHex:(?EpochIDSpace)/binary, "\n">> ->
                    _EpochID = decode_epoch_id(EpochIDHex),
                    do_net_server_listing(Sock, DataDir, S);
                <<"C ",
                  EpochIDHex:(?EpochIDSpace)/binary,
                  File:CSumFileLenLF/binary, "\n">> ->
                    _EpochID = decode_epoch_id(EpochIDHex),
                    do_net_server_checksum_listing(Sock, File, DataDir, S);
                <<"QUIT\n">> ->
                    catch gen_tcp:close(Sock),
                    exit(normal);
                <<"QUIT\r\n">> ->
                    catch gen_tcp:close(Sock),
                    exit(normal);
                %% For "internal" replication only.
                <<"W-repl ",
                  EpochIDHex:(?EpochIDSpace)/binary,
                  CSumHex:(?CSumSpace)/binary,
                  OffsetHex:16/binary, LenHex:8/binary,
                  File:WriteFileLenLF/binary, "\n">> ->
                    _EpochID = decode_epoch_id(EpochIDHex),
                    do_net_server_write(Sock, CSumHex, OffsetHex, LenHex,
                                        File, DataDir,
                                        <<"fixme1">>, false, <<"fixme2">>);
                %% For data migration only.
                <<"DEL-migration ",
                  EpochIDHex:(?EpochIDSpace)/binary,
                  File:DelFileLenLF/binary, "\n">> ->
                    _EpochID = decode_epoch_id(EpochIDHex),
                    do_net_server_delete_migration_only(Sock, File, DataDir);
                %% For erasure coding hackityhack
                <<"TRUNC-hack--- ",
                  EpochIDHex:(?EpochIDSpace)/binary,
                  File:DelFileLenLF/binary, "\n">> ->
                    _EpochID = decode_epoch_id(EpochIDHex),
                    do_net_server_truncate_hackityhack(Sock, File, DataDir);
                <<"PROJ ", LenHex:8/binary, "\n">> ->
                    do_projection_command(Sock, LenHex, S);
                <<"WEDGE-STATUS\n">> ->
                    do_wedge_status(FluName, Sock);
                <<"PUT ", _/binary>>=PutLine ->
                    http_server_hack(FluName, PutLine, Sock, S);
                <<"GET ", _/binary>>=PutLine ->
                    http_server_hack(FluName, PutLine, Sock, S);
                <<"PROTOCOL-BUFFERS\n">> ->
                    ok = gen_tcp:send(Sock, <<"OK\n">>),
                    ok = inet:setopts(Sock, [{packet, 4},
                                             {packet_size, 33*1024*1024}]),
                    {ok, Proj} = machi_projection_store:read_latest_projection(
                                   S#state.proj_store, private),
                    Ps = [P_srvr ||
                             {_, P_srvr} <- orddict:to_list(
                                              Proj#projection_v1.members_dict)],
                    machi_pb_server:run_loop(Sock, Ps);
                _ ->
                    machi_util:verb("Else Got: ~p\n", [Line]),
                    io:format(user, "TODO: Else Got: ~p\n", [Line]),
                    gen_tcp:send(Sock, "ERROR SYNTAX\n"),
                    catch gen_tcp:close(Sock),
                    exit(normal)
            end,
            net_server_loop(Sock, S);
        _ ->
            catch gen_tcp:close(Sock),
            exit(normal)
    end.

append_server_dispatch(From, Prefix, Chunk, CSum, Extra, DataDir, LinkPid) ->
    Pid = write_server_get_pid(Prefix, DataDir, LinkPid),
    Pid ! {seq_append, From, Prefix, Chunk, CSum, Extra},
    exit(normal).

do_net_server_append(FluName, Sock, CSumHex, LenHex, ExtraHex, Prefix) ->
    %% TODO: robustify against other invalid path characters such as NUL
    case sanitize_file_string(Prefix) of
        ok ->
            do_net_server_append2(FluName, Sock, CSumHex,
                                  LenHex, ExtraHex, Prefix);
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

do_net_server_append2(FluName, Sock, CSumHex, LenHex, ExtraHex, Prefix) ->
    <<Len:32/big>> = machi_util:hexstr_to_bin(LenHex),
    <<Extra:32/big>> = machi_util:hexstr_to_bin(ExtraHex),
    ClientCSum = machi_util:hexstr_to_bin(CSumHex),
    ok = inet:setopts(Sock, [{packet, raw}]),
    {ok, Chunk} = gen_tcp:recv(Sock, Len, 60*1000),
    try
        CSum = case ClientCSum of
                   <<?CSUM_TAG_NONE:8, _/binary>> ->
                       %% TODO: If the client was foolish enough to use
                       %% this type of non-checksum, then the client gets
                       %% what it deserves wrt data integrity, alas.  In
                       %% the client-side Chain Replication method, each
                       %% server will calculated this independently, which
                       %% isn't exactly what ought to happen for best data
                       %% integrity checking.  In server-side CR, the csum
                       %% should be calculated by the head and passed down
                       %% the chain together with the value.
                       CS = machi_util:checksum_chunk(Chunk),
                       machi_util:make_tagged_csum(server_sha, CS);
                   <<?CSUM_TAG_CLIENT_SHA:8, ClientCS/binary>> ->
                       CS = machi_util:checksum_chunk(Chunk),
                       if CS == ClientCS ->
                               ClientCSum;
                          true ->
                               throw({bad_csum, CS})
                       end;
                   _ ->
                       ClientCSum
               end,
        FluName ! {seq_append, self(), Prefix, Chunk, CSum, Extra}
    catch
        throw:{bad_csum, _CS} ->
            ok = gen_tcp:send(Sock, <<"ERROR BAD-CHECKSUM\n">>),
            exit(normal);
        error:badarg ->
            error_logger:error_msg("Message send to ~p gave badarg, make certain server is running with correct registered name\n", [?MODULE])
    end,
    receive
        {assignment, Offset, File} ->
            OffsetHex = machi_util:bin_to_hexstr(<<Offset:64/big>>),
            Out = io_lib:format("OK ~s ~s ~s\n", [CSumHex, OffsetHex, File]),
            ok = gen_tcp:send(Sock, Out);
        wedged ->
            ok = gen_tcp:send(Sock, <<"ERROR WEDGED\n">>)
    after 10*1000 ->
            ok = gen_tcp:send(Sock, "TIMEOUT\n")
    end.

do_wedge_status(FluName, Sock) ->
    FluName ! {wedge_status, self()},
    Reply = receive
                {wedge_status_reply, Bool, EpochId} ->
                    BoolHex = if Bool == false -> <<"00">>;
                                 Bool == true  -> <<"01">>
                              end,
                    case EpochId of
                        undefined ->
                            EpochHex = machi_util:int_to_hexstr(0, 32),
                            CSumHex = machi_util:bin_to_hexstr(<<0:(20*8)/big>>);
                        {Epoch, EpochCSum} ->
                            EpochHex = machi_util:int_to_hexstr(Epoch, 32),
                            CSumHex = machi_util:bin_to_hexstr(EpochCSum)
                    end,
                    [<<"OK ">>, BoolHex, 32, EpochHex, 32, CSumHex, 10]
            after 30*1000 ->
                    <<"give_it_up\n">>
            end,
    ok = gen_tcp:send(Sock, Reply).

do_net_server_read(Sock, OffsetHex, LenHex, FileBin, DataDir,
                   EpochID, S) ->
    {Wedged_p, CurrentEpochId} = ets:lookup_element(S#state.etstab, epoch, 2),
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
                                   [read, binary, raw], DoItFun,
                                   EpochID, Wedged_p, CurrentEpochId).

do_net_server_readwrite_common(Sock, OffsetHex, LenHex, FileBin, DataDir,
                               FileOpts, DoItFun,
                               EpochID, Wedged_p, CurrentEpochId) ->
    case {Wedged_p, sanitize_file_string(FileBin)} of
        {false, ok} ->
            do_net_server_readwrite_common2(Sock, OffsetHex, LenHex, FileBin,
                                            DataDir, FileOpts, DoItFun,
                                            EpochID, Wedged_p, CurrentEpochId);
        {true, _} ->
            ok = gen_tcp:send(Sock, <<"ERROR WEDGED\n">>);
        {_, __} ->
            ok = gen_tcp:send(Sock, <<"ERROR BAD-ARG\n">>)
    end.

do_net_server_readwrite_common2(Sock, OffsetHex, LenHex, FileBin, DataDir,
                                FileOpts, DoItFun,
                                EpochID, Wedged_p, CurrentEpochId) ->
    NoSuchFileFun = fun(Sck) ->
                            ok = gen_tcp:send(Sck, <<"ERROR NO-SUCH-FILE\n">>)
                    end,
    BadIoFun = fun(Sck) ->
                       ok = gen_tcp:send(Sck, <<"ERROR BAD-IO\n">>)
               end,
    do_net_server_readwrite_common2(Sock, OffsetHex, LenHex, FileBin, DataDir,
                                    FileOpts, DoItFun,
                                    EpochID, Wedged_p, CurrentEpochId,
                                    NoSuchFileFun, BadIoFun).

do_net_server_readwrite_common2(Sock, OffsetHex, LenHex, FileBin, DataDir,
                                FileOpts, DoItFun,
                                EpochID, Wedged_p, CurrentEpochId,
                                NoSuchFileFun, BadIoFun) ->
    <<Offset:64/big>> = machi_util:hexstr_to_bin(OffsetHex),
    <<Len:32/big>> = machi_util:hexstr_to_bin(LenHex),
    {_, Path} = machi_util:make_data_filename(DataDir, FileBin),
    OptsHasWrite = lists:member(write, FileOpts),
    OptsHasRead = lists:member(read, FileOpts),
    case file:open(Path, FileOpts) of
        {ok, FH} ->
            try
                DoItFun(FH, Offset, Len)
            catch
                throw:{bad_csum, _CS} ->
                    ok = gen_tcp:send(Sock, <<"ERROR BAD-CHECKSUM\n">>)
            after
                file:close(FH)
            end;
        {error, enoent} when OptsHasWrite ->
            do_net_server_readwrite_common(
              Sock, OffsetHex, LenHex, FileBin, DataDir,
              FileOpts, DoItFun,
              EpochID, Wedged_p, CurrentEpochId);
        {error, enoent} when OptsHasRead ->
            ok = NoSuchFileFun(Sock);
        _Else ->
            ok = BadIoFun(Sock)
    end.

do_net_server_write(Sock, CSumHex, OffsetHex, LenHex, FileBin, DataDir,
                    EpochID, Wedged_p, CurrentEpochId) ->
    CSumPath = machi_util:make_checksum_filename(DataDir, FileBin),
    case file:open(CSumPath, [append, raw, binary, delayed_write]) of
        {ok, FHc} ->
            do_net_server_write2(Sock, CSumHex, OffsetHex, LenHex, FileBin,
                                 DataDir, FHc, EpochID, Wedged_p,
                                 CurrentEpochId);
        {error, enoent} ->
            ok = filelib:ensure_dir(CSumPath),
            do_net_server_write(Sock, CSumHex, OffsetHex, LenHex, FileBin,
                                DataDir, EpochID, Wedged_p,
                                CurrentEpochId)
    end.

do_net_server_write2(Sock, CSumHex, OffsetHex, LenHex, FileBin, DataDir, FHc,
                     EpochID, Wedged_p, CurrentEpochId) ->
    ClientCSum = machi_util:hexstr_to_bin(CSumHex),
    DoItFun = fun(FHd, Offset, Len) ->
                      ok = inet:setopts(Sock, [{packet, raw}]),
                      {ok, Chunk} = gen_tcp:recv(Sock, Len),
                      CSum = case ClientCSum of
                                 <<?CSUM_TAG_NONE:8, _/binary>> ->
                                     %% TODO: If the client was foolish enough to use
                                     %% this type of non-checksum, then the client gets
                                     %% what it deserves wrt data integrity, alas.  In
                                     %% the client-side Chain Replication method, each
                                     %% server will calculated this independently, which
                                     %% isn't exactly what ought to happen for best data
                                     %% integrity checking.  In server-side CR, the csum
                                     %% should be calculated by the head and passed down
                                     %% the chain together with the value.
                                     CS = machi_util:checksum_chunk(Chunk),
                                     machi_util:make_tagged_csum(server_sha,CS);
                                 <<?CSUM_TAG_CLIENT_SHA:8, ClientCS/binary>> ->
                                     CS = machi_util:checksum_chunk(Chunk),
                                     if CS == ClientCS ->
                                             ClientCSum;
                                        true ->
                                             throw({bad_csum, CS})
                                     end;
                                 _ ->
                                     ClientCSum
                             end,
                      case file:pwrite(FHd, Offset, Chunk) of
                          ok ->
                              CSumHex2 = machi_util:bin_to_hexstr(CSum),
                              CSum_info = [OffsetHex, 32, LenHex, 32,
                                           CSumHex2, 10],
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
                                   [write, read, binary, raw], DoItFun,
                                   EpochID, Wedged_p, CurrentEpochId).

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

do_net_server_listing(Sock, DataDir, S) ->
    {Wedged_p, _CurrentEpochId} = ets:lookup_element(S#state.etstab, epoch, 2),
    if Wedged_p ->
            ok = gen_tcp:send(Sock, <<"ERROR WEDGED\n">>);
       true ->
            do_net_server_listing2(Sock, DataDir)
    end.

do_net_server_listing2(Sock, DataDir) ->
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

do_net_server_checksum_listing(Sock, File, DataDir, S) ->
    {Wedged_p, _CurrentEpochId} = ets:lookup_element(S#state.etstab, epoch, 2),
    case {Wedged_p, sanitize_file_string(File)} of
        {true, _} ->
            ok = gen_tcp:send(Sock, <<"ERROR WEDGED\n">>);
        {false, ok} ->
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
        {seq_append, From, Prefix, Chunk, CSum, Extra} ->
            if Chunk /= <<>> ->
                    ok = file:pwrite(FHd, Offset, Chunk);
               true ->
                    ok
            end,
            From ! {assignment, Offset, File},
            Len = byte_size(Chunk),
            OffsetHex = machi_util:bin_to_hexstr(<<Offset:64/big>>),
            LenHex = machi_util:bin_to_hexstr(<<Len:32/big>>),
            CSumHex = machi_util:bin_to_hexstr(CSum),
            CSum_info = [OffsetHex, 32, LenHex, 32, CSumHex, 10],
            ok = file:write(FHc, CSum_info),
            seq_append_server_loop(DataDir, Prefix, File, FH_,
                                   FileNum, Offset + Len + Extra);
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
        ProjCmdM = machi_pb:decode_mpb_ll_request(ProjCmdBin),
        {ID, ProjCmd} = machi_pb_wrap:unmake_projection_req(ProjCmdM),
        ProjOp = element(1, ProjCmd),
        put(hack, ProjCmd),
        Res = handle_projection_command(ProjCmd, S),
        ResM = machi_pb_wrap:make_projection_resp(ID, ProjOp, Res),
        ResBin = machi_pb:encode_mpb_ll_response(ResM),
        ResLenHex = machi_util:int_to_hexbin(iolist_size(ResBin), 32),
        ok = gen_tcp:send(Sock, [<<"OK ">>, ResLenHex, <<"\n">>, ResBin])
    catch
        What:Why ->
            io:format(user, "OOPS ~p\n", [get(hack)]),
            io:format(user, "OOPS ~p ~p ~p\n", [What, Why, erlang:get_stacktrace()]),
            WHA = list_to_binary(io_lib:format("TODO-YOLO.~w:~w-~w",
                                               [What, Why, erlang:get_stacktrace()])),
            _ = (catch gen_tcp:send(Sock, [<<"ERROR ">>, WHA, <<"\n">>]))
    end.

handle_projection_command({get_latest_epochid, ProjType},
                          #state{proj_store=ProjStore}) ->
    machi_projection_store:get_latest_epochid(ProjStore, ProjType);
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

%% This is the name of the projection store that is spawned by the
%% *flu*, for use primarily in testing scenarios.  In normal use, we
%% ought to be using the OTP style of managing processes, via
%% supervisors, namely via machi_flu_psup.erl, which uses a
%% *different* naming convention for the projection store name that it
%% registers.

make_projection_server_regname(BaseName) ->
    list_to_atom(atom_to_list(BaseName) ++ "_pstore2").

http_server_hack(FluName, Line1, Sock, S) ->
    {ok, {http_request, HttpOp, URI0, _HttpV}, _x} =
        erlang:decode_packet(http_bin, Line1, [{line_length,4095}]),
    MyURI = case URI0 of
              {abs_path, Path} -> <<"/", Rest/binary>> = Path,
                                  Rest;
              _                -> URI0
          end,
    Hdrs = http_harvest_headers(Sock),
    G = digest_header_goop(Hdrs, #http_goop{}),
    case HttpOp of
        'PUT' ->
            http_server_hack_put(Sock, G, FluName, MyURI);
        'GET' ->
            http_server_hack_get(Sock, G, FluName, MyURI, S)
    end,
    ok = gen_tcp:close(Sock),
    exit(normal).

http_server_hack_put(Sock, G, FluName, MyURI) ->
    ok = inet:setopts(Sock, [{packet, raw}]),
    {ok, Chunk} = gen_tcp:recv(Sock, G#http_goop.len, 60*1000),
    CSum0 = machi_util:checksum_chunk(Chunk),
    try
        CSum = case G#http_goop.x_csum of
                   undefined ->
                       machi_util:make_tagged_csum(server_sha, CSum0);
                   XX when is_binary(XX) ->
                       if XX == CSum0 ->
                               machi_util:make_tagged_csum(client_sha,  CSum0);
                          true ->
                               throw({bad_csum, XX})
                       end
               end,
        FluName ! {seq_append, self(), MyURI, Chunk, CSum, 0}
    catch
        throw:{bad_csum, _CS} ->
            Out = "HTTP/1.0 412 Precondition failed\r\n"
                "X-Reason: bad checksum\r\n\r\n",
            ok = gen_tcp:send(Sock, Out),
            ok = gen_tcp:close(Sock),
            exit(normal);
        error:badarg ->
            error_logger:error_msg("Message send to ~p gave badarg, make certain server is running with correct registered name\n", [?MODULE])
    end,
    receive
        {assignment, Offset, File} ->
            Msg = io_lib:format("HTTP/1.0 201 Created\r\nLocation: ~s\r\n"
                                "X-Offset: ~w\r\nX-Size: ~w\r\n\r\n",
                                [File, Offset, byte_size(Chunk)]),
            ok = gen_tcp:send(Sock, Msg);
        wedged ->
            ok = gen_tcp:send(Sock, <<"HTTP/1.0 499 WEDGED\r\n\r\n">>)
    after 10*1000 ->
            ok = gen_tcp:send(Sock, <<"HTTP/1.0 499 TIMEOUT\r\n\r\n">>)
    end.

http_server_hack_get(Sock, _G, _FluName, MyURI, S) ->
    DataDir = S#state.data_dir,
    {Wedged_p, CurrentEpochId} = ets:lookup_element(S#state.etstab, epoch, 2),
    EpochID = <<"unused">>,
    NoSuchFileFun = fun(Sck) ->
                            ok = gen_tcp:send(Sck, "HTTP/1.0 455 NOT-WRITTEN\r\n\r\n")
                    end,
    BadIoFun = fun(Sck) ->
                            ok = gen_tcp:send(Sck, "HTTP/1.0 466 BAD-IO\r\n\r\n")
               end,
    DoItFun = fun(FH, Offset, Len) ->
                      case file:pread(FH, Offset, Len) of
                          {ok, Bytes} when byte_size(Bytes) == Len ->
                              Hdrs = io_lib:format("HTTP/1.0 200 OK\r\nContent-Length: ~w\r\n\r\n", [Len]),
                              gen_tcp:send(Sock, [Hdrs, Bytes]);
                          {ok, Bytes} ->
                              machi_util:verb("ok read but wanted ~p  got ~p: ~p @ offset ~p\n",
                                  [Len, size(Bytes), Bytes, Offset]),
                              ok = gen_tcp:send(Sock, "HTTP/1.0 455 PARTIAL-READ\r\n\r\n");
                          eof ->
                              ok = gen_tcp:send(Sock, "HTTP/1.0 455 NOT-WRITTEN\r\n\r\n");
                          _Else2 ->
                              machi_util:verb("Else2 ~p ~p ~P\n",
                                              [Offset, Len, _Else2, 20]),
                              ok = gen_tcp:send(Sock, "HTTP/1.0 466 ERROR BAD-READ\r\n\r\n")
                      end
              end,
    [File, OptsBin] = binary:split(MyURI, <<"?">>),
    Opts = split_uri_options(OptsBin),
    OffsetHex = machi_util:int_to_hexstr(proplists:get_value(offset, Opts), 64),
    LenHex = machi_util:int_to_hexstr(proplists:get_value(size, Opts), 32),
    do_net_server_readwrite_common2(Sock, OffsetHex, LenHex, File, DataDir,
                                    [read, binary, raw], DoItFun,
                                    EpochID, Wedged_p, CurrentEpochId,
                                    NoSuchFileFun, BadIoFun).

http_harvest_headers(Sock) ->
    ok = inet:setopts(Sock, [{packet, httph}]),
    http_harvest_headers(gen_tcp:recv(Sock, 0, ?SERVER_CMD_READ_TIMEOUT),
                         Sock, []).

http_harvest_headers({ok, http_eoh}, _Sock, Acc) ->
    Acc;
http_harvest_headers({error, _}, _Sock, _Acc) ->
    [];
http_harvest_headers({ok, Hdr}, Sock, Acc) ->
    http_harvest_headers(gen_tcp:recv(Sock, 0, ?SERVER_CMD_READ_TIMEOUT),
                         Sock, [Hdr|Acc]).

digest_header_goop([], G) ->
    G;
digest_header_goop([{http_header, _, 'Content-Length', _, Str}|T], G) ->
    digest_header_goop(T, G#http_goop{len=list_to_integer(Str)});
digest_header_goop([{http_header, _, "X-Checksum", _, Str}|T], G) ->
    SHA = machi_util:hexstr_to_bin(Str),
    CSum = machi_util:make_tagged_csum(client_sha, SHA),
    digest_header_goop(T, G#http_goop{x_csum=CSum});
digest_header_goop([_H|T], G) ->
    digest_header_goop(T, G).

split_uri_options(OpsBin) ->
    L = binary:split(OpsBin, <<"&">>),
    [case binary:split(X, <<"=">>) of
         [<<"offset">>, Bin] ->
             {offset, binary_to_integer(Bin)};
         [<<"size">>, Bin] ->
             {size, binary_to_integer(Bin)}
     end || X <- L].
