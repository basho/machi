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
%% Today's FLU fully supports a protocol that is based on Protocol
%% Buffers.  Please see the `src/machi.proto' file for details.
%%
%% === TODO items ===
%%
%% TODO The per-file metadata tuple store is missing from this implementation.
%%
%% TODO Section 4.1 ("The FLU") of the Machi design doc suggests that
%% the FLU keep track of the epoch number of the last file write (and
%% perhaps last metadata write), as an optimization for inter-FLU data
%% replication/chain repair.

-module(machi_flu1).

-include_lib("kernel/include/file.hrl").

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").
-define(V(X,Y), ok).
%% -include("machi_verbose.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

-export([start_link/1, stop/1,
         update_wedge_state/3, wedge_myself/2]).
-export([make_listener_regname/1, make_projection_server_regname/1]).
%% TODO: remove or replace in OTP way after gen_*'ified
-export([main2/4, run_append_server/2, run_listen_server/1,
         current_state/1, format_state/1]).

-record(state, {
          flu_name        :: atom(),
          proj_store      :: pid(),
          witness = false :: boolean(),
          append_pid      :: pid(),
          tcp_port        :: non_neg_integer(),
          data_dir        :: string(),
          wedged = true   :: boolean(),
          etstab          :: ets:tid(),
          epoch_id        :: 'undefined' | machi_dt:epoch_id(),
          pb_mode = undefined  :: 'undefined' | 'high' | 'low',
          high_clnt       :: 'undefined' | pid(),
          trim_table      :: ets:tid(),
          props = []      :: list()  % proplist
         }).

-define(SERVER_CMD_READ_TIMEOUT, 600*1000).
-define(INIT_TIMEOUT, 60*1000).

start_link([{FluName, TcpPort, DataDir}|Rest])
  when is_atom(FluName), is_integer(TcpPort), is_list(DataDir) ->
    proc_lib:start_link(?MODULE, main2, [FluName, TcpPort, DataDir, Rest],
                        ?INIT_TIMEOUT).

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

wedge_myself(PidSpec, EpochId)
  when is_tuple(EpochId) ->
    PidSpec ! {wedge_myself, EpochId}.

current_state(PidSpec) ->
    PidSpec ! {current_state, self()},
    %% TODO: Not so rubust f(^^;)
    receive
        Res -> Res
    after
        60*1000 -> {error, timeout}
    end.

format_state(State) ->
    Fields = record_info(fields, state),
    [_Name | Values] = tuple_to_list(State),
    lists:zip(Fields, Values).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ets_table_name(FluName) when is_atom(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_epoch").

main2(FluName, TcpPort, DataDir, Props) ->
    {SendAppendPidToProj_p, ProjectionPid} =
        case proplists:get_value(projection_store_registered_name, Props) of
            undefined ->
                RN = make_projection_server_regname(FluName),
                {ok, PP} =
                    machi_projection_store:start_link(RN, DataDir, undefined),
                {true, PP};
            RN ->
                {false, whereis(RN)}
        end,
    InitialWedged_p = proplists:get_value(initial_wedged, Props),
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
    Witness_p = proplists:get_value(witness_mode, Props, false),
    
    S0 = #state{flu_name=FluName,
                proj_store=ProjectionPid,
                tcp_port=TcpPort,
                data_dir=DataDir,
                wedged=Wedged_p,
                witness=Witness_p,
                etstab=ets_table_name(FluName),
                epoch_id=EpochId,
                props=Props},
    {ok, AppendPid} = start_append_server(S0, self()),
    if SendAppendPidToProj_p ->
            machi_projection_store:set_wedge_notify_pid(ProjectionPid,
                                                        AppendPid);
       true ->
            ok
    end,
    S1 = S0#state{append_pid=AppendPid},
    {ok, ListenPid} = start_listen_server(S1),

    Config_e = machi_util:make_config_filename(DataDir, "unused"),
    ok = filelib:ensure_dir(Config_e),
    {_, Data_e} = machi_util:make_data_filename(DataDir, "unused"),
    ok = filelib:ensure_dir(Data_e),
    Projection_e = machi_util:make_projection_filename(DataDir, "unused"),
    ok = filelib:ensure_dir(Projection_e),

    put(flu_flu_name, FluName),
    put(flu_append_pid, S1#state.append_pid),
    put(flu_projection_pid, ProjectionPid),
    put(flu_listen_pid, ListenPid),
    proc_lib:init_ack({ok, self()}),

    receive killme -> ok end,
    (catch exit(S1#state.append_pid, kill)),
    (catch exit(ProjectionPid, kill)),
    (catch exit(ListenPid, kill)),
    ok.

start_listen_server(S) ->
    proc_lib:start_link(?MODULE, run_listen_server, [S], ?INIT_TIMEOUT).

start_append_server(S, AckPid) ->
    proc_lib:start_link(?MODULE, run_append_server, [AckPid, S], ?INIT_TIMEOUT).

run_listen_server(#state{flu_name=FluName, tcp_port=TcpPort}=S) ->
    register(make_listener_regname(FluName), self()),
    SockOpts = ?PB_PACKET_OPTS ++
        [{reuseaddr, true}, {mode, binary}, {active, false},
         {backlog,8192}],
    case gen_tcp:listen(TcpPort, SockOpts) of
        {ok, LSock} ->
            proc_lib:init_ack({ok, self()}),
            listen_server_loop(LSock, S);
        Else ->
            error_logger:warning_msg("~s:run_listen_server: "
                                     "listen to TCP port ~w: ~w\n",
                                     [?MODULE, TcpPort, Else]),
            exit({?MODULE, run_listen_server, tcp_port, TcpPort, Else})
    end.

run_append_server(FluPid, #state{flu_name=Name,
                                 wedged=Wedged_p,epoch_id=EpochId}=S) ->
    %% Reminder: Name is the "main" name of the FLU, i.e., no suffix
    register(Name, self()),
    TID = ets:new(ets_table_name(Name),
                  [set, protected, named_table, {read_concurrency, true}]),
    ets:insert(TID, {epoch, {Wedged_p, EpochId}}),
    proc_lib:init_ack({ok, self()}),
    append_server_loop(FluPid, S#state{etstab=TID}).

listen_server_loop(LSock, S) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    spawn_link(fun() -> net_server_loop(Sock, S) end),
    listen_server_loop(LSock, S).

append_server_loop(FluPid, #state{wedged=Wedged_p,
                                  witness=Witness_p,
                                  epoch_id=OldEpochId, flu_name=FluName}=S) ->
    receive
        {seq_append, From, _Prefix, _Chunk, _CSum, _Extra, _EpochID}
          when Witness_p ->
            %% The FLU's net_server_loop() process ought to filter all
            %% witness states, but we'll keep this clause for extra
            %% paranoia.
            From ! witness,
            append_server_loop(FluPid, S);
        {seq_append, From, _Prefix, _Chunk, _CSum, _Extra, _EpochID}
          when Wedged_p ->
            From ! wedged,
            append_server_loop(FluPid, S);
        {seq_append, From, Prefix, Chunk, CSum, Extra, EpochID} ->
            %% Old is the one from our state, plain old 'EpochID' comes
            %% from the client.
            _ = case OldEpochId == EpochID of
                true ->
                    spawn(fun() -> 
                        append_server_dispatch(From, Prefix, Chunk, CSum, Extra, FluName, EpochID) 
                    end);
                false ->
                    From ! {error, bad_epoch}
            end,
            append_server_loop(FluPid, S);
        {wedge_myself, WedgeEpochId} ->
            if not Wedged_p andalso WedgeEpochId == OldEpochId ->
                    true = ets:insert(S#state.etstab,
                                      {epoch, {true, OldEpochId}}),
                    %% Tell my chain manager that it might want to react to
                    %% this new world.
                    Chmgr = machi_chain_manager1:make_chmgr_regname(FluName),
                    spawn(fun() ->
                            catch machi_chain_manager1:trigger_react_to_env(Chmgr)
                          end),
                    append_server_loop(FluPid, S#state{wedged=true});
               true ->
                    append_server_loop(FluPid, S)
            end;
        {wedge_state_change, Boolean, {NewEpoch, _}=NewEpochId} ->
            OldEpoch = case OldEpochId of {OldE, _} -> OldE;
                                          undefined -> -1
                       end,
            if NewEpoch >= OldEpoch ->
                    true = ets:insert(S#state.etstab,
                                      {epoch, {Boolean, NewEpochId}}),
                    append_server_loop(FluPid, S#state{wedged=Boolean,
                                                       epoch_id=NewEpochId});
               true ->
                    append_server_loop(FluPid, S)
            end;
        {wedge_status, FromPid} ->
            #state{wedged=Wedged_p, epoch_id=EpochId} = S,
            FromPid ! {wedge_status_reply, Wedged_p, EpochId},
            append_server_loop(FluPid, S);
        {current_state, FromPid} ->
            FromPid ! S;
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
                        {maybe_encode_response(R), mode(low, NewS)};
                    _ ->
                        HL_req = machi_pb:decode_mpb_request(Bin),
                        1 = HL_req#mpb_request.do_not_alter,
                        {R, NewS} = do_pb_hl_request(HL_req, make_high_clnt(S)),
                        {machi_pb:encode_mpb_response(R), mode(high, NewS)}
                end,
            if RespBin == async_no_response ->
                    net_server_loop(Sock, S2);
               true ->
                    case gen_tcp:send(Sock, RespBin) of
                        ok ->
                            net_server_loop(Sock, S2);
                        {error, _} ->
                            (catch gen_tcp:close(Sock)),
                            exit(normal)
                    end
            end;
        {error, SockError} ->
            Msg = io_lib:format("Socket error ~w", [SockError]),
            R = #mpb_ll_response{req_id= <<>>,
                                 generic=#mpb_errorresp{code=1, msg=Msg}},
            _Resp = machi_pb:encode_mpb_ll_response(R),
            %% TODO: Weird that sometimes neither catch nor try/catch
            %%       can prevent OTP's SASL from logging an error here.
            %%       Error in process <0.545.0> with exit value: {badarg,[{erlang,port_command,.......
            %% TODO: is this what causes the intermittent PULSE deadlock errors?
            %% _ = (catch gen_tcp:send(Sock, _Resp)), timer:sleep(1000),
            (catch gen_tcp:close(Sock)),
            exit(normal)
    end.

maybe_encode_response(async_no_response=X) ->
    X;
maybe_encode_response(R) ->
    machi_pb:encode_mpb_ll_response(R).

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
            {RqID, {LowCmd, _}=CMD}
              when LowCmd == low_proj;
                   LowCmd == low_wedge_status; LowCmd == low_list_files ->
                %% Skip wedge check for projection commands!
                %% Skip wedge check for these unprivileged commands
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
            {{error, wedged}, S#state{epoch_id=CurrentEpochID}};
       is_tuple(EpochID)
       andalso
       EpochID /= CurrentEpochID ->
            {Epoch, _} = EpochID,
            {CurrentEpoch, _} = CurrentEpochID,
            if Epoch < CurrentEpoch ->
                    ok;
               true ->
                    %% We're at same epoch # but different checksum, or
                    %% we're at a newer/bigger epoch #.
                    _ = wedge_myself(S#state.flu_name, CurrentEpochID),
                    ok
            end,
            {{error, bad_epoch}, S#state{epoch_id=CurrentEpochID}};
       true ->
            do_pb_ll_request3(CMD, S#state{epoch_id=CurrentEpochID})
    end.

%% Witness status does not matter below.
do_pb_ll_request3({low_echo, _BogusEpochID, Msg}, S) ->
    {Msg, S};
do_pb_ll_request3({low_auth, _BogusEpochID, _User, _Pass}, S) ->
    {-6, S};
do_pb_ll_request3({low_wedge_status, _EpochID}, S) ->
    {do_server_wedge_status(S), S};
do_pb_ll_request3({low_proj, PCMD}, S) ->
    {do_server_proj_request(PCMD, S), S};
%% Witness status *matters* below
do_pb_ll_request3({low_append_chunk, _EpochID, PKey, Prefix, Chunk, CSum_tag,
                CSum, ChunkExtra},
                  #state{witness=false}=S) ->
    {do_server_append_chunk(PKey, Prefix, Chunk, CSum_tag, CSum,
                            ChunkExtra, S), S};
do_pb_ll_request3({low_write_chunk, _EpochID, File, Offset, Chunk, CSum_tag,
                   CSum},
                  #state{witness=false}=S) ->
    {do_server_write_chunk(File, Offset, Chunk, CSum_tag, CSum, S), S};
do_pb_ll_request3({low_read_chunk, _EpochID, File, Offset, Size, Opts},
                  #state{witness=false} = S) ->
    {do_server_read_chunk(File, Offset, Size, Opts, S), S};
do_pb_ll_request3({low_trim_chunk, _EpochID, File, Offset, Size, TriggerGC},
                  #state{witness=false}=S) ->
    {do_server_trim_chunk(File, Offset, Size, TriggerGC, S), S};
do_pb_ll_request3({low_checksum_list, _EpochID, File},
                  #state{witness=false}=S) ->
    {do_server_checksum_listing(File, S), S};
do_pb_ll_request3({low_list_files, _EpochID},
                  #state{witness=false}=S) ->
    {do_server_list_files(S), S};
do_pb_ll_request3({low_delete_migration, _EpochID, File},
                  #state{witness=false}=S) ->
    {do_server_delete_migration(File, S),
     #state{witness=false}=S};
do_pb_ll_request3({low_trunc_hack, _EpochID, File},
                  #state{witness=false}=S) ->
    {do_server_trunc_hack(File, S), S};
do_pb_ll_request3(_, #state{witness=true}=S) ->
    {{error, bad_arg}, S}.                       % TODO: new status code??

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
do_pb_hl_request2({high_read_chunk, File, Offset, Size, Opts},
                  #state{high_clnt=Clnt}=S) ->
    Res = machi_cr_client:read_chunk(Clnt, File, Offset, Size, Opts),
    {Res, S};
do_pb_hl_request2({high_trim_chunk, File, Offset, Size},
                  #state{high_clnt=Clnt}=S) ->
    Res = machi_cr_client:trim_chunk(Clnt, File, Offset, Size),
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
                       #state{flu_name=FluName, proj_store=ProjStore}) ->
    if Proj#projection_v1.epoch_number == ?SPAM_PROJ_EPOCH ->
            %% io:format(user, "DBG ~s ~w ~P\n", [?MODULE, ?LINE, Proj, 5]),
            Chmgr = machi_flu_psup:make_fitness_regname(FluName),
            [Map] = Proj#projection_v1.dbg,
            catch machi_fitness:send_fitness_update_spam(
                    Chmgr, Proj#projection_v1.author_server, Map);
       true ->
            catch machi_projection_store:write(ProjStore, ProjType, Proj)
    end;
do_server_proj_request({get_all_projections, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:get_all_projections(ProjStore, ProjType);
do_server_proj_request({list_all_projections, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:list_all_projections(ProjStore, ProjType);
do_server_proj_request({kick_projection_reaction},
                       #state{flu_name=FluName}) ->
    %% Tell my chain manager that it might want to react to
    %% this new world.
    Chmgr = machi_chain_manager1:make_chmgr_regname(FluName),
    spawn(fun() ->
                  catch machi_chain_manager1:trigger_react_to_env(Chmgr)
          end),
    async_no_response.

do_server_append_chunk(PKey, Prefix, Chunk, CSum_tag, CSum,
                       ChunkExtra, S) ->
    case sanitize_prefix(Prefix) of
        ok ->
            do_server_append_chunk2(PKey, Prefix, Chunk, CSum_tag, CSum,
                                    ChunkExtra, S);
        _ ->
            {error, bad_arg}
    end.

do_server_append_chunk2(_PKey, Prefix, Chunk, CSum_tag, Client_CSum,
                        ChunkExtra, #state{flu_name=FluName,
                                           epoch_id=EpochID}=_S) ->
    %% TODO: Do anything with PKey?
    try
        TaggedCSum = check_or_make_tagged_checksum(CSum_tag, Client_CSum,Chunk),
        R = {seq_append, self(), Prefix, Chunk, TaggedCSum, ChunkExtra, EpochID},
        FluName ! R,
        receive
            {assignment, Offset, File} ->
                Size = iolist_size(Chunk),
                {ok, {Offset, Size, File}};
            witness ->
                {error, bad_arg};
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

do_server_write_chunk(File, Offset, Chunk, CSum_tag, CSum, #state{flu_name=FluName}) ->
    case sanitize_file_string(File) of
        ok ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, File}) of
                {ok, Pid} ->
                    Meta = [{client_csum_tag, CSum_tag}, {client_csum, CSum}],
                    machi_file_proxy:write(Pid, Offset, Meta, Chunk);
                {error, trimmed} = Error ->
                    Error
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_read_chunk(File, Offset, Size, Opts, #state{flu_name=FluName})->
    case sanitize_file_string(File) of
        ok ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, File}) of
                {ok, Pid} ->
                    case machi_file_proxy:read(Pid, Offset, Size, Opts) of
                        %% XXX FIXME 
                        %% For now we are omiting the checksum data because it blows up
                        %% protobufs.
                        {ok, ChunksAndTrimmed} -> {ok, ChunksAndTrimmed};
                        Other -> Other
                    end;
                {error, trimmed} = Error ->
                    Error
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_trim_chunk(File, Offset, Size, TriggerGC, #state{flu_name=FluName}) ->
    lager:debug("Hi there! I'm trimming this: ~s, (~p, ~p), ~p~n",
                [File, Offset, Size, TriggerGC]),
    case sanitize_file_string(File) of
        ok ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, File}) of
                {ok, Pid} ->
                    machi_file_proxy:trim(Pid, Offset, Size, TriggerGC);
                {error, trimmed} = Trimmed ->
                    %% Should be returned back to (maybe) trigger repair
                    Trimmed
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_checksum_listing(File, #state{flu_name=FluName, data_dir=DataDir}=_S) ->
    case sanitize_file_string(File) of
        ok ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, File}) of
                {ok, Pid} ->
                    {ok, List} = machi_file_proxy:checksum_list(Pid),
                    Bin = erlang:term_to_binary(List),
                    if byte_size(Bin) > (?PB_MAX_MSG_SIZE - 1024) ->
                            %% TODO: Fix this limitation by streaming the
                            %% binary in multiple smaller PB messages.
                            %% Also, don't read the file all at once. ^_^
                            error_logger:error_msg("~s:~w oversize ~s\n",
                                                   [?MODULE, ?LINE, DataDir]),
                            {error, bad_arg};
                       true ->
                            {ok, Bin}
                    end;
                {error, trimmed} ->
                    {error, trimmed}
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

append_server_dispatch(From, Prefix, Chunk, CSum, Extra, FluName, EpochId) ->
    Result = case handle_append(Prefix, Chunk, CSum, Extra, FluName, EpochId) of
        {ok, File, Offset} ->
            {assignment, Offset, File};
        Other ->
            Other
    end,
    From ! Result,
    exit(normal).

handle_append(_Prefix, <<>>, _Csum, _Extra, _FluName, _EpochId) ->
    {error, bad_arg};
handle_append(Prefix, Chunk, Csum, Extra, FluName, EpochId) ->
    Res = machi_flu_filename_mgr:find_or_make_filename_from_prefix(FluName, EpochId, {prefix, Prefix}),
    case Res of
        {file, F} ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, F}) of
                {ok, Pid} ->
                    {Tag, CS} = machi_util:unmake_tagged_csum(Csum),
                    Meta = [{client_csum_tag, Tag}, {client_csum, CS}],
                    machi_file_proxy:append(Pid, Meta, Extra, Chunk);
                {error, trimmed} = E ->
                    E
            end;
        Error ->
            Error
    end.

sanitize_file_string(Str) ->
    case has_no_prohibited_chars(Str) andalso machi_util:is_valid_filename(Str) of
        true -> ok;
        false -> error
    end.

has_no_prohibited_chars(Str) ->
    case re:run(Str, "/") of
        nomatch ->
            true;
        _ ->
            true
    end.

sanitize_prefix(Prefix) ->
    %% We are using '^' as our component delimiter
    case re:run(Prefix, "/|\\^") of
        nomatch ->
            ok;
        _ ->
            error
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
    list_to_atom(atom_to_list(BaseName) ++ "_pstore").

check_or_make_tagged_checksum(?CSUM_TAG_NONE, _Client_CSum, Chunk) ->
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
                                         B = machi_checksums:encode_csum_file_entry_hex(X, 100, CSum),
                                         %% file:write(ZZZ, [B, 10]),
                                         machi_checksums:decode_csum_file_entry_hex(list_to_binary(B))
                                 end, x, Xs)
             end),
    io:format(user, "~.3f sec\n", [HexUSec / 1000000]),
    %% %% file:close(ZZZ),

    io:format(user, "Not-sortable file entry enc+dec: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotSortedUSec, _} =
    timer:tc(fun() ->
                     lists:foldl(fun(X, _) ->
                                         B = machi_checksums:encode_csum_file_entry(X, 100, CSum),
                                         machi_checksums:decode_csum_file_entry(list_to_binary(B))
                                 end, x, Xs)
             end),
    io:format(user, "~.3f sec\n", [NotSortedUSec / 1000000]),

    NotHexList = lists:foldl(fun(X, Acc) ->
                                     B = machi_checksums:encode_csum_file_entry(X, 100, CSum),
                                 [B|Acc]
                         end, [], Xs),
    NotHexBin = iolist_to_binary(NotHexList),

    io:format(user, "Split NotHexBin: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotHexBinUSec, SplitRes} =
    timer:tc(fun() ->
                     put(hack_length, 29),
                     put(hack_different, false),
                     {Sorted, _Leftover} = machi_checksums:split_checksum_list_blob(NotHexBin),
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
                    {SplitRes, Leftover} = machi_checksums:split_checksum_list_blob(AllStuff),
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
