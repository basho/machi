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
-include("machi_pb.hrl").
-include("machi_projection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

-define(SERVER_CMD_READ_TIMEOUT, 600*1000).

-export([start_link/1, stop/1,
         update_wedge_state/3]).
-export([make_listener_regname/1, make_projection_server_regname/1]).
-export([encode_csum_file_entry/3, encode_csum_file_entry_bin/3,
         decode_csum_file_entry/1,
         split_checksum_list_blob/1, split_checksum_list_blob_decode/1]).

-record(state, {
          flu_name        :: atom(),
          proj_store      :: pid(),
          append_pid      :: pid(),
          tcp_port        :: non_neg_integer(),
          data_dir        :: string(),
          wedged = true   :: boolean(),
          etstab          :: ets:tid(),
          epoch_id        :: 'undefined' | machi_dt:epoch_id(),
          pb_mode = undefined  :: 'undefined' | 'high' | 'low',
          high_clnt       :: 'undefined' | pid(),
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
    SockOpts = ?PB_PACKET_OPTS ++
        [{reuseaddr, true}, {mode, binary}, {active, false}],
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

net_server_loop(Sock, S) ->
    case gen_tcp:recv(Sock, 0, ?SERVER_CMD_READ_TIMEOUT) of
        {ok, Bin} ->
            {RespBin, S2} = 
                case machi_pb:decode_mpb_ll_request(Bin) of
                    LL_req when LL_req#mpb_ll_request.do_not_alter == 2 ->
                        {R, NewS} = do_pb_ll_request(LL_req, S),
                        {machi_pb:encode_mpb_ll_response(R), mode(low, NewS)};
                    _ ->
                        HL_req = machi_pb:decode_mpb_request(Bin),
                        1 = HL_req#mpb_request.do_not_alter,
                        {R, NewS} = do_pb_hl_request(HL_req, make_high_clnt(S)),
                        {machi_pb:encode_mpb_response(R), mode(high, NewS)}
                end,
            ok = gen_tcp:send(Sock, RespBin),
            net_server_loop(Sock, S2);
        {error, SockError} ->
            Msg = io_lib:format("Socket error ~w", [SockError]),
            R = #mpb_ll_response{req_id= <<>>,
                                 generic=#mpb_errorresp{code=1, msg=Msg}},
            Resp = machi_pb:encode_mpb_ll_response(R),
            _ = (catch gen_tcp:send(Sock, Resp)),
            (catch gen_tcp:close(Sock)),
            exit(normal)
    end.

mode(Mode, #state{pb_mode=undefined}=S) ->
    S#state{pb_mode=Mode};
mode(_, S) ->
    S.

make_high_clnt(#state{high_clnt=undefined}=S) ->
    {ok, Proj} = machi_projection_store:read_latest_projection(
                   S#state.proj_store, private),
    Ps = [P_srvr || {_, P_srvr} <- orddict:to_list(
                                     Proj#projection_v1.members_dict)],
    {ok, Clnt} = machi_cr_client:start_link(Ps),
    S#state{high_clnt=Clnt};
make_high_clnt(S) ->
    S.

do_pb_ll_request(#mpb_ll_request{req_id=ReqID}, #state{pb_mode=high}=S) ->
    Result = {high_error, 41, "Low protocol request while in high mode"},
    {machi_pb_translate:to_pb_response(ReqID, unused, Result), S};
do_pb_ll_request(PB_request, S) ->
    Req = machi_pb_translate:from_pb_request(PB_request),
    {ReqID, Cmd, Result, S2} = 
        case Req of
            {RqID, {low_proj, _}=CMD} ->
                %% Skip wedge check for projection commands!
                {Rs, NewS} = do_pb_ll_request3(CMD, S),
                {RqID, CMD, Rs, NewS};
            {RqID, {low_wedge_status, _}=CMD} ->
                %% Skip wedge check for low_wedge_status!
                {Rs, NewS} = do_pb_ll_request3(CMD, S),
                {RqID, CMD, Rs, NewS};
            {RqID, CMD} ->
                EpochID = element(2, CMD),      % by common convention
                {Rs, NewS} = do_pb_ll_request2(EpochID, CMD, S),
                {RqID, CMD, Rs, NewS}
        end,
    {machi_pb_translate:to_pb_response(ReqID, Cmd, Result), S2}.

do_pb_ll_request2(EpochID, CMD, S) ->
    {Wedged_p, CurrentEpochID} = ets:lookup_element(S#state.etstab, epoch, 2),
    if Wedged_p == true ->
            {{error, wedged}, S};
       not ((not is_tuple(EpochID)) orelse EpochID == ?DUMMY_PV1_EPOCH)
       andalso
       EpochID /= CurrentEpochID ->
            {Epoch, _} = EpochID,
            {CurrentEpoch, _} = CurrentEpochID,
            if Epoch < CurrentEpoch ->
                    ok;
               true ->
                    %% We're at same epoch # but different checksum, or
                    %% we're at a newer/bigger epoch #.
                    io:format(user, "\n\nTODO: wedge myself!\n\n", []),
                    todo_wedge_myself
            end,
            {{error, bad_epoch}, S};
       true ->
            do_pb_ll_request3(CMD, S)
    end.

do_pb_ll_request3({low_echo, _BogusEpochID, Msg}, S) ->
    {Msg, S};
do_pb_ll_request3({low_auth, _BogusEpochID, _User, _Pass}, S) ->
    {-6, S};
do_pb_ll_request3({low_append_chunk, _EpochID, PKey, Prefix, Chunk, CSum_tag,
                CSum, ChunkExtra}, S) ->
    {do_server_append_chunk(PKey, Prefix, Chunk, CSum_tag, CSum,
                            ChunkExtra, S), S};
do_pb_ll_request3({low_write_chunk, _EpochID, File, Offset, Chunk, CSum_tag,
                   CSum}, S) ->
    {do_server_write_chunk(File, Offset, Chunk, CSum_tag, CSum, S), S};
do_pb_ll_request3({low_read_chunk, _EpochID, File, Offset, Size, Opts}, S) ->
    {do_server_read_chunk(File, Offset, Size, Opts, S), S};
do_pb_ll_request3({low_checksum_list, _EpochID, File}, S) ->
    {do_server_checksum_listing(File, S), S};
do_pb_ll_request3({low_list_files, _EpochID}, S) ->
    {do_server_list_files(S), S};
do_pb_ll_request3({low_wedge_status, _EpochID}, S) ->
    {do_server_wedge_status(S), S};
do_pb_ll_request3({low_delete_migration, _EpochID, File}, S) ->
    {do_server_delete_migration(File, S), S};
do_pb_ll_request3({low_trunc_hack, _EpochID, File}, S) ->
    {do_server_trunc_hack(File, S), S};
do_pb_ll_request3({low_proj, PCMD}, S) ->
    {do_server_proj_request(PCMD, S), S}.

do_pb_hl_request(#mpb_request{req_id=ReqID}, #state{pb_mode=low}=S) ->
    Result = {low_error, 41, "High protocol request while in low mode"},
    {machi_pb_translate:to_pb_response(ReqID, unused, Result), S};
do_pb_hl_request(PB_request, S) ->
    {ReqID, Cmd} = machi_pb_translate:from_pb_request(PB_request),
    {Result, S2} = do_pb_hl_request2(Cmd, S),
    {machi_pb_translate:to_pb_response(ReqID, Cmd, Result), S2}.

do_pb_hl_request2({high_echo, Msg}, S) ->
    {Msg, S};
do_pb_hl_request2({high_auth, _User, _Pass}, S) ->
    {-77, S};
do_pb_hl_request2({high_append_chunk, _todoPK, Prefix, ChunkBin, TaggedCSum,
                   ChunkExtra}, #state{high_clnt=Clnt}=S) ->
    Chunk = {TaggedCSum, ChunkBin},
    Res = machi_cr_client:append_chunk_extra(Clnt, Prefix, Chunk,
                                             ChunkExtra),
    {Res, S};
do_pb_hl_request2({high_write_chunk, File, Offset, ChunkBin, TaggedCSum},
                  #state{high_clnt=Clnt}=S) ->
    Chunk = {TaggedCSum, ChunkBin},
    Res = machi_cr_client:write_chunk(Clnt, File, Offset, Chunk),
    {Res, S};
do_pb_hl_request2({high_read_chunk, File, Offset, Size},
                  #state{high_clnt=Clnt}=S) ->
    Res = machi_cr_client:read_chunk(Clnt, File, Offset, Size),
    {Res, S};
do_pb_hl_request2({high_checksum_list, File}, #state{high_clnt=Clnt}=S) ->
    Res = machi_cr_client:checksum_list(Clnt, File),
    {Res, S};
do_pb_hl_request2({high_list_files}, #state{high_clnt=Clnt}=S) ->
    Res = machi_cr_client:list_files(Clnt),
    {Res, S}.

do_server_proj_request({get_latest_epochid, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:get_latest_epochid(ProjStore, ProjType);
do_server_proj_request({read_latest_projection, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:read_latest_projection(ProjStore, ProjType);
do_server_proj_request({read_projection, ProjType, Epoch},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:read(ProjStore, ProjType, Epoch);
do_server_proj_request({write_projection, ProjType, Proj},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:write(ProjStore, ProjType, Proj);
do_server_proj_request({get_all_projections, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:get_all_projections(ProjStore, ProjType);
do_server_proj_request({list_all_projections, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:list_all_projections(ProjStore, ProjType).

do_server_append_chunk(PKey, Prefix, Chunk, CSum_tag, CSum,
                       ChunkExtra, S) ->
    case sanitize_file_string(Prefix) of
        ok ->
            do_server_append_chunk2(PKey, Prefix, Chunk, CSum_tag, CSum,
                                    ChunkExtra, S);
        _ ->
            {error, bad_arg}
    end.

do_server_append_chunk2(_PKey, Prefix, Chunk, CSum_tag, Client_CSum,
                        ChunkExtra, #state{flu_name=FluName}=_S) ->
    %% TODO: Do anything with PKey?
    try
        TaggedCSum = check_or_make_tagged_checksum(CSum_tag, Client_CSum,Chunk),
        FluName ! {seq_append, self(), Prefix, Chunk, TaggedCSum, ChunkExtra},
        receive
            {assignment, Offset, File} ->
                Size = iolist_size(Chunk),
                {ok, {Offset, Size, File}};
            wedged ->
                {error, wedged}
        after 10*1000 ->
                {error, partition}
        end
    catch
        throw:{bad_csum, _CS} ->
            {error, bad_checksum};
        error:badarg ->
            error_logger:error_msg("Message send to ~p gave badarg, make certain server is running with correct registered name\n", [?MODULE]),
            {error, bad_arg}
    end.

do_server_write_chunk(File, Offset, Chunk, CSum_tag, CSum,
                      #state{data_dir=DataDir}=S) ->
    case sanitize_file_string(File) of
        ok ->
            CSumPath = machi_util:make_checksum_filename(DataDir, File),
            case file:open(CSumPath, [append, raw, binary]) of
                {ok, FHc} ->
                    Path = DataDir ++ "/data/" ++
                        machi_util:make_string(File),
                    {ok, FHd} = file:open(Path, [read, write, raw, binary]),
                    try
                        do_server_write_chunk2(
                          File, Offset, Chunk, CSum_tag, CSum, DataDir,
                          FHc, FHd)
                    after
                        (catch file:close(FHc)),
                        (catch file:close(FHd))
                    end;
                {error, enoent} ->
                    ok = filelib:ensure_dir(CSumPath),
                    do_server_write_chunk(File, Offset, Chunk, CSum_tag,
                                          CSum, S)
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_write_chunk2(_File, Offset, Chunk, CSum_tag,
                       Client_CSum, _DataDir, FHc, FHd) ->
    try
        TaggedCSum = check_or_make_tagged_checksum(CSum_tag, Client_CSum,Chunk),
        Size = iolist_size(Chunk),
        case file:pwrite(FHd, Offset, Chunk) of
            ok ->
                CSum_info = encode_csum_file_entry(Offset, Size, TaggedCSum),
                ok = file:write(FHc, CSum_info),
                ok;
            _Else3 ->
                machi_util:verb("Else3 ~p ~p ~p\n",
                                [Offset, Size, _Else3]),
                {error, bad_arg}
        end
    catch
        throw:{bad_csum, _CS} ->
            {error, bad_checksum};
        error:badarg ->
            error_logger:error_msg("Message send to ~p gave badarg, make certain server is running with correct registered name\n", [?MODULE]),
            {error, bad_arg}
    end.

do_server_read_chunk(File, Offset, Size, _Opts, #state{data_dir=DataDir})->
    %% TODO: Look inside Opts someday.
    case sanitize_file_string(File) of
        ok ->
            {_, Path} = machi_util:make_data_filename(DataDir, File),
            case file:open(Path, [read, binary, raw]) of
                {ok, FH} ->
                    try
                        case file:pread(FH, Offset, Size) of
                            {ok, Bytes} when byte_size(Bytes) == Size ->
                                {ok, Bytes};
                            {ok, Bytes} ->
                                machi_util:verb("ok read but wanted ~p got ~p: ~p @ offset ~p\n",
                                                [Size,size(Bytes),File,Offset]),
                                io:format(user, "ok read but wanted ~p got ~p: ~p @ offset ~p\n",
                                          [Size,size(Bytes),File,Offset]),
                                {error, partial_read};
                            eof ->
                                {error, not_written}; %% TODO perhaps_do_net_server_ec_read(Sock, FH);
                            _Else2 ->
                                machi_util:verb("Else2 ~p ~p ~P\n",
                                                [Offset, Size, _Else2, 20]),
                                {error, bad_read}
                        end
                    after
                        file:close(FH)
                    end;
                {error, enoent} ->
                    {error, not_written};
                {error, _Else} ->
                    io:format(user, "Unexpected ~p at ~p ~p\n",
                              [_Else, ?MODULE, ?LINE]),
                    {error, bad_arg}
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_checksum_listing(File, #state{data_dir=DataDir}=_S) ->
    case sanitize_file_string(File) of
        ok ->
            ok = sync_checksum_file(File),
            CSumPath = machi_util:make_checksum_filename(DataDir, File),
            %% TODO: If this file is legitimately bigger than our
            %% {packet_size,N} limit, then we'll have a difficult time, eh?
            case file:read_file(CSumPath) of
                {ok, Bin} ->
                    if byte_size(Bin) > (?PB_MAX_MSG_SIZE - 1024) ->
                            %% TODO: Fix this limitation by streaming the
                            %% binary in multiple smaller PB messages.
                            %% Also, don't read the file all at once. ^_^
                            error_logger:error_msg("~s:~w oversize ~s\n",
                                                   [?MODULE, ?LINE, CSumPath]),
                            {error, bad_arg};
                       true ->
                            {ok, Bin}
                    end;
                {error, enoent} ->
                    {error, no_such_file};
                {error, _} ->
                    {error, bad_arg}
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_list_files(#state{data_dir=DataDir}=_S) ->
    {_, WildPath} = machi_util:make_data_filename(DataDir, ""),
    Files = filelib:wildcard("*", WildPath),
    {ok, [begin
              {ok, FI} = file:read_file_info(WildPath ++ "/" ++ File),
              Size = FI#file_info.size,
              {Size, File}
          end || File <- Files]}.

do_server_wedge_status(S) ->
    {Wedged_p, CurrentEpochID0} = ets:lookup_element(S#state.etstab, epoch, 2),
    CurrentEpochID = if CurrentEpochID0 == undefined ->
                             ?DUMMY_PV1_EPOCH;
                        true ->
                             CurrentEpochID0
                     end,
    {Wedged_p, CurrentEpochID}.

do_server_delete_migration(File, #state{data_dir=DataDir}=_S) ->
    case sanitize_file_string(File) of
        ok ->
            {_, Path} = machi_util:make_data_filename(DataDir, File),
            case file:delete(Path) of
                ok ->
                    ok;
                {error, enoent} ->
                    {error, no_such_file};
                _ ->
                    {error, bad_arg}
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_trunc_hack(File, #state{data_dir=DataDir}=_S) ->
    case sanitize_file_string(File) of
        ok ->
            {_, Path} = machi_util:make_data_filename(DataDir, File),
            case file:open(Path, [read, write, binary, raw]) of
                {ok, FH} ->
                    try
                        {ok, ?MINIMUM_OFFSET} = file:position(FH,
                                                              ?MINIMUM_OFFSET),
                        ok = file:truncate(FH),
                        ok
                    after
                        file:close(FH)
                    end;
                {error, enoent} ->
                    {error, no_such_file};
                _ ->
                    {error, bad_arg}
            end;
        _ ->
            {error, bad_arg}
    end.

append_server_dispatch(From, Prefix, Chunk, CSum, Extra, DataDir, LinkPid) ->
    Pid = write_server_get_pid(Prefix, DataDir, LinkPid),
    Pid ! {seq_append, From, Prefix, Chunk, CSum, Extra},
    exit(normal).

sanitize_file_string(Str) ->
    case re:run(Str, "/") of
        nomatch ->
            ok;
        _ ->
            error
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
                          [read, write, raw, binary]),
    CSumPath = machi_util:make_checksum_filename(
                 DataDir, Prefix, SequencerNameHack, FileNum),
    {ok, FHc} = file:open(CSumPath, [append, raw, binary]),
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
        {seq_append, From, Prefix, Chunk, TaggedCSum, Extra} ->
            if Chunk /= <<>> ->
                    ok = file:pwrite(FHd, Offset, Chunk);
               true ->
                    ok
            end,
            From ! {assignment, Offset, File},
            Size = iolist_size(Chunk),
            CSum_info = encode_csum_file_entry(Offset, Size, TaggedCSum),
            ok = file:write(FHc, CSum_info),
            seq_append_server_loop(DataDir, Prefix, File, FH_,
                                   FileNum, Offset + Size + Extra);
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

http_server_hack_get(Sock, _G, _FluName, _MyURI, _S) ->
    ok = gen_tcp:send(Sock, <<"TODO BROKEN FEATURE see old commits\r\n">>).

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

%% @doc Encode `Offset + Size + TaggedCSum' into an `iolist()' type for
%% internal storage by the FLU.

-spec encode_csum_file_entry(
        machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()) ->
        iolist().
encode_csum_file_entry(Offset, Size, TaggedCSum) ->
    Len = 8 + 4 + byte_size(TaggedCSum),
    [<<Len:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big>>,
     TaggedCSum].

%% @doc Encode `Offset + Size + TaggedCSum' into an `binary()' type for
%% internal storage by the FLU.

-spec encode_csum_file_entry_bin(
        machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()) ->
        binary().
encode_csum_file_entry_bin(Offset, Size, TaggedCSum) ->
    Len = 8 + 4 + byte_size(TaggedCSum),
    <<Len:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big,
      TaggedCSum/binary>>.

%% @doc Decode a single `binary()' blob into an
%%      `{Offset,Size,TaggedCSum}' tuple.
%%
%% The internal encoding (which is currently exposed to the outside world
%% via this function and related ones) is:
%%
%% <ul>
%% <li> 1 byte: record length
%% </li>
%% <li> 8 bytes (unsigned big-endian): byte offset
%% </li>
%% <li> 4 bytes (unsigned big-endian): chunk size
%% </li>
%% <li> all remaining bytes: tagged checksum (1st byte = type tag)
%% </li>
%% </ul>
%%
%% See `machi.hrl' for the tagged checksum types, e.g.,
%% `?CSUM_TAG_NONE'.

-spec decode_csum_file_entry(binary()) ->
        {machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()}.
decode_csum_file_entry(<<_:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big, TaggedCSum/binary>>) ->
    {Offset, Size, TaggedCSum}.

%% @doc Split a `binary()' blob of `checksum_list' data into a list of
%% unparsed `binary()' blobs, one per entry.
%%
%% Decode the unparsed blobs with {@link decode_csum_file_entry/1}, if
%% desired.
%%
%% The return value `TrailingJunk' is unparseable bytes at the end of
%% the checksum list blob.

-spec split_checksum_list_blob(binary()) ->
          {list(binary()), TrailingJunk::binary()}.
split_checksum_list_blob(Bin) ->
    split_checksum_list_blob(Bin, []).

split_checksum_list_blob(<<Len:8/unsigned-big, Part:Len/binary, Rest/binary>>, Acc)->
    case get(hack_length) of
        Len -> ok;
        _   -> put(hack_different, true)
    end,
    split_checksum_list_blob(Rest, [<<Len:8/unsigned-big, Part/binary>>|Acc]);
split_checksum_list_blob(Rest, Acc) ->
    {lists:reverse(Acc), Rest}.

%% @doc Split a `binary()' blob of `checksum_list' data into a list of
%% `{Offset,Size,TaggedCSum}' tuples.

-spec split_checksum_list_blob_decode(binary()) ->
  {list({machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()}),
   TrailingJunk::binary()}.
split_checksum_list_blob_decode(Bin) ->
    split_checksum_list_blob_decode(Bin, []).

split_checksum_list_blob_decode(<<Len:8/unsigned-big, Part:Len/binary, Rest/binary>>, Acc)->
    One = <<Len:8/unsigned-big, Part/binary>>,
    split_checksum_list_blob_decode(Rest, [decode_csum_file_entry(One)|Acc]);
split_checksum_list_blob_decode(Rest, Acc) ->
    {lists:reverse(Acc), Rest}.

check_or_make_tagged_checksum(?CSUM_TAG_NONE, Client_CSum, Chunk) ->
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
check_or_make_tagged_checksum(?CSUM_TAG_CLIENT_SHA, Client_CSum, Chunk) ->
    CS = machi_util:checksum_chunk(Chunk),
    if CS == Client_CSum ->
            machi_util:make_tagged_csum(server_sha,
                                        Client_CSum);
       true ->
            throw({bad_csum, CS})
    end.

-ifdef(TEST).

%% Remove "_COMMENTED" string to run the demo/exploratory code.

timing_demo_test_COMMENTED_() ->
    {timeout, 300, fun() -> timing_demo_test2() end}.

%% Demo/exploratory hackery to check relative speeds of dealing with
%% checksum data in different ways.
%%
%% Summary:
%%
%% * Use compact binary encoding, with 1 byte header for entry length.
%%     * Because the hex-style code is *far* slower just for enc & dec ops.
%%     * For 1M entries of enc+dec: 0.215 sec vs. 15.5 sec.
%% * File sorter when sorting binaries as-is is only 30-40% slower
%%   than an in-memory split (of huge binary emulated by file:read_file()
%%   "big slurp") and sort of the same as-is sortable binaries.
%% * File sorter slows by a factor of about 2.5 if {order, fun compare/2}
%%   function must be used, i.e. because the checksum entry lengths differ.
%% * File sorter + {order, fun compare/2} is still *far* faster than external
%%   sort by OS X's sort(1) of sortable ASCII hex-style:
%%   4.5 sec vs. 21 sec.
%% * File sorter {order, fun compare/2} is faster than in-memory sort
%%   of order-friendly 3-tuple-style: 4.5 sec vs. 15 sec.

timing_demo_test2() ->
    Xs = [random:uniform(1 bsl 32) || _ <- lists:duplicate(1*1000*1000, $x)],
    CSum = <<"123456789abcdef0A">>,
    17 = byte_size(CSum),
    io:format(user, "\n", []),

    %% %% {ok, ZZZ} = file:open("/tmp/foo.hex-style", [write, binary, raw, delayed_write]),
    io:format(user, "Hex-style file entry enc+dec: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {HexUSec, _} =
    timer:tc(fun() ->
                     lists:foldl(fun(X, _) ->
                                         B = encode_csum_file_entry_hex(X, 100, CSum),
                                         %% file:write(ZZZ, [B, 10]),
                                         decode_csum_file_entry_hex(list_to_binary(B))
                                 end, x, Xs)
             end),
    io:format(user, "~.3f sec\n", [HexUSec / 1000000]),
    %% %% file:close(ZZZ),

    io:format(user, "Not-sortable file entry enc+dec: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotSortedUSec, _} =
    timer:tc(fun() ->
                     lists:foldl(fun(X, _) ->
                                         B = encode_csum_file_entry(X, 100, CSum),
                                         decode_csum_file_entry(list_to_binary(B))
                                 end, x, Xs)
             end),
    io:format(user, "~.3f sec\n", [NotSortedUSec / 1000000]),

    NotHexList = lists:foldl(fun(X, Acc) ->
                                 B = encode_csum_file_entry(X, 100, CSum),
                                 [B|Acc]
                         end, [], Xs),
    NotHexBin = iolist_to_binary(NotHexList),

    io:format(user, "Split NotHexBin: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotHexBinUSec, SplitRes} =
    timer:tc(fun() ->
                     put(hack_length, 29),
                     put(hack_different, false),
                     {Sorted, _Leftover} = split_checksum_list_blob(NotHexBin),
                     io:format(user, " Leftover ~p (hack_different ~p) ", [_Leftover, get(hack_different)]),
                     Sorted
             end),
    io:format(user, "~.3f sec\n", [NotHexBinUSec / 1000000]),

    io:format(user, "Sort Split results: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {SortSplitUSec, _} =
    timer:tc(fun() ->
                     lists:sort(SplitRes)
                     %% lists:sort(fun sort_2lines/2, SplitRes)
             end),
    io:format(user, "~.3f sec\n", [SortSplitUSec / 1000000]),

    UnsortedName = "/tmp/foo.unsorted",
    SortedName = "/tmp/foo.sorted",

    ok = file:write_file(UnsortedName, NotHexList),
    io:format(user, "File Sort Split results: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {FileSortUSec, _} =
        timer:tc(fun() ->
                         {ok, FHin} = file:open(UnsortedName, [read, binary]),
                         {ok, FHout} = file:open(SortedName,
                                                [write, binary, delayed_write]),
                         put(hack_sorter_sha_ctx, crypto:hash_init(sha)),
                         ok = file_sorter:sort(sort_input_fun(FHin, <<>>),
                                               sort_output_fun(FHout),
                                               [{format,binary},
                                                {header, 1}
                                                %% , {order, fun sort_2lines/2}
                                               ])
                 end),
    io:format(user, "~.3f sec\n", [FileSortUSec / 1000000]),
    _SHA = crypto:hash_final(get(hack_sorter_sha_ctx)),
    %% io:format(user, "SHA via (hack_sorter_sha_ctx) = ~p\n", [_SHA]),

    io:format(user, "NotHex-Not-sortable tuple list creation: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotHexTupleCreationUSec, NotHexTupleList} =
    timer:tc(fun() ->
                     lists:foldl(fun(X, Acc) ->
                                         B = encode_csum_file_entry_hex(
                                               X, 100, CSum),
                                         [B|Acc]
                                 end, [], Xs)
             end),
    io:format(user, "~.3f sec\n", [NotHexTupleCreationUSec / 1000000]),

    io:format(user, "NotHex-Not-sortable tuple list sort: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotHexTupleSortUSec, _} =
    timer:tc(fun() ->
                     lists:sort(NotHexTupleList)
             end),
    io:format(user, "~.3f sec\n", [NotHexTupleSortUSec / 1000000]),

    ok.

sort_2lines(<<_:1/binary, A/binary>>, <<_:1/binary, B/binary>>) ->
    A < B.

sort_input_fun(FH, PrevStuff) ->
    fun(close) ->
            ok;
       (read) ->
            case file:read(FH, 1024*1024) of
                {ok, NewStuff} ->
                    AllStuff = if PrevStuff == <<>> ->
                                       NewStuff;
                                  true ->
                                       <<PrevStuff/binary, NewStuff/binary>>
                               end,
                    {SplitRes, Leftover} = split_checksum_list_blob(AllStuff),
                    {SplitRes, sort_input_fun(FH, Leftover)};
                eof ->
                    end_of_input
            end
    end.

sort_output_fun(FH) ->
    fun(close) ->
            file:close(FH);
       (Stuff) ->
            Ctx = get(hack_sorter_sha_ctx),
            put(hack_sorter_sha_ctx, crypto:hash_update(Ctx, Stuff)),
            ok = file:write(FH, Stuff),
            sort_output_fun(FH)
    end.

encode_csum_file_entry_hex(Offset, Size, TaggedCSum) ->
    OffsetHex = machi_util:bin_to_hexstr(<<Offset:64/big>>),
    SizeHex = machi_util:bin_to_hexstr(<<Size:32/big>>),
    CSumHex = machi_util:bin_to_hexstr(TaggedCSum),
    [OffsetHex, 32, SizeHex, 32, CSumHex].

decode_csum_file_entry_hex(<<OffsetHex:16/binary, _:1/binary, SizeHex:8/binary, _:1/binary, CSumHex/binary>>) ->
    Offset = machi_util:hexstr_to_bin(OffsetHex),
    Size = machi_util:hexstr_to_bin(SizeHex),
    CSum = machi_util:hexstr_to_bin(CSumHex),
    {Offset, Size, CSum}.

-endif. % TEST
