% 1. start file proxy supervisor
% 2. start projection store
% 3. start listener
-module(machi_flu_listener).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").

-record(state, {
    pb_mode,
    high_clnt,
    proj_store,
    etstab,
    epoch_id,
    flu_name
}).

-define(SERVER_CMD_READ_TIMEOUT, 600 * 1000).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = ranch:accept_ack(Ref),
    %% By default, ranch sets sockets to
    %% {active, false}, {packet, raw}, {reuseaddr, true}
    ok = Transport:setopts(Socket, ?PB_PACKET_OPTS),
    loop(Socket, Transport, #state{}).

loop(Socket, Transport, S) ->
    case Transport:recv(Socket, 0, ?SERVER_CMD_READ_TIMEOUT) of
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
                    ok;
               true ->
                    ok = Transport:send(Socket, RespBin)
            end,
            loop(Socket, Transport, S2);
        {error, SockError} ->
            lager:error("Socket error ~w", [SockError]),
            (catch Transport:close(Socket))
    end.

make_high_clnt(#state{high_clnt=undefined}=S) ->
    {ok, Proj} = machi_projection_store:read_latest_projection(
                   S#state.proj_store, private),
    Ps = [P_srvr || {_, P_srvr} <- orddict:to_list(
                                     Proj#projection_v1.members_dict)],
    {ok, Clnt} = machi_cr_client:start_link(Ps),
    S#state{high_clnt=Clnt};
make_high_clnt(S) ->
    S.

maybe_encode_response(async_no_response=X) ->
    X;
maybe_encode_response(R) ->
    machi_pb:encode_mpb_ll_response(R).

mode(Mode, #state{pb_mode=undefined}=S) ->
    S#state{pb_mode=Mode};
mode(_, S) ->
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
                    wedge_myself(S#state.flu_name, CurrentEpochID),
                    ok
            end,
            {{error, bad_epoch}, S#state{epoch_id=CurrentEpochID}};
       true ->
            do_pb_ll_request3(CMD, S#state{epoch_id=CurrentEpochID})
    end.

do_pb_ll_request3({low_echo, _BogusEpochID, Msg}, S) ->
    {Msg, S};
do_pb_ll_request3({low_auth, _BogusEpochID, _User, _Pass}, S) ->
    {-6, S};
do_pb_ll_request3(Cmd, S) ->
    {execute_cmd(Cmd), S}.
%do_pb_ll_request3({low_append_chunk, _EpochID, PKey, Prefix, Chunk, CSum_tag,
%                CSum, ChunkExtra}, S) ->
%    {do_server_append_chunk(PKey, Prefix, Chunk, CSum_tag, CSum,
%                            ChunkExtra, S), S};
%do_pb_ll_request3({low_write_chunk, _EpochID, File, Offset, Chunk, CSum_tag,
%                   CSum}, S) ->
%    {do_server_write_chunk(File, Offset, Chunk, CSum_tag, CSum, S), S};
%do_pb_ll_request3({low_read_chunk, _EpochID, File, Offset, Size, Opts}, S) ->
%    {do_server_read_chunk(File, Offset, Size, Opts, S), S};
%do_pb_ll_request3({low_checksum_list, _EpochID, File}, S) ->
%    {do_server_checksum_listing(File, S), S};
%do_pb_ll_request3({low_list_files, _EpochID}, S) ->
%    {do_server_list_files(S), S};
%do_pb_ll_request3({low_wedge_status, _EpochID}, S) ->
%    {do_server_wedge_status(S), S};
%do_pb_ll_request3({low_delete_migration, _EpochID, File}, S) ->
%    {do_server_delete_migration(File, S), S};
%do_pb_ll_request3({low_trunc_hack, _EpochID, File}, S) ->
%    {do_server_trunc_hack(File, S), S};
%do_pb_ll_request3({low_proj, PCMD}, S) ->
%    {do_server_proj_request(PCMD, S), S}.
execute_cmd(_Cmd) ->
    ok.


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

wedge_myself(_, _) -> ok.

