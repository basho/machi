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

%% @doc High level protocol server-side processing (temporary?)

-module(machi_pb_server).

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").

-define(SERVER_CMD_READ_TIMEOUT, 600*1000).

-export([run_loop/2]).

run_loop(Sock, P_srvr_list) ->
    ok = inet:setopts(Sock, ?PB_PACKET_OPTS),
    {ok, Clnt} = machi_cr_client:start_link(P_srvr_list),
    protocol_buffers_loop(Sock, Clnt).

protocol_buffers_loop(Sock, Clnt) ->
    case gen_tcp:recv(Sock, 0, ?SERVER_CMD_READ_TIMEOUT) of
        {ok, Bin} ->
            R = do_pb_request(catch machi_pb:decode_mpb_request(Bin), Clnt),
            Resp = machi_pb:encode_mpb_response(R),
            ok = gen_tcp:send(Sock, Resp),
            protocol_buffers_loop(Sock, Clnt);
        {error, SockError} ->
            Msg = io_lib:format("Socket error ~w", [SockError]),
            R = #mpb_errorresp{code=1, msg=Msg},
            Resp = machi_pb:encode_mpb_response(R),
            _ = (catch gen_tcp:send(Sock, Resp)),
            (catch gen_tcp:close(Sock)),
            exit(normal)
    end.

do_pb_request(PB_request, Clnt) ->
    {ReqID, Cmd, Result} = 
        case machi_pb_translate:from_pb(PB_request) of
            {RqID, {high_echo, Msg}=CMD} ->
                Rs = Msg,
                {RqID, CMD, Rs};
            {RqID, {high_auth, _User, _Pass}} ->
                {RqID, not_implemented};
            {RqID, {high_append_chunk, _todoPK, Prefix, ChunkBin, TaggedCSum,
                    ChunkExtra}=CMD} ->
                Chunk = {TaggedCSum, ChunkBin},
                Rs = machi_cr_client:append_chunk_extra(Clnt, Prefix, Chunk,
                                                        ChunkExtra),

                {RqID, CMD, Rs};
            {RqID, {high_write_chunk, File, Offset, ChunkBin, TaggedCSum}=CMD} ->
                Chunk = {TaggedCSum, ChunkBin},
                Rs = machi_cr_client:write_chunk(Clnt, File, Offset, Chunk),
                {RqID, CMD, Rs};
            {RqID, {high_read_chunk, File, Offset, Size}=CMD} ->
                Rs = machi_cr_client:read_chunk(Clnt, File, Offset, Size),
                {RqID, CMD, Rs};
            {RqID, {high_checksum_list, File}=CMD} ->
                Rs = machi_cr_client:checksum_list(Clnt, File),
                {RqID, CMD, Rs};
            {RqID, {high_list_files}=CMD} ->
                Rs = machi_cr_client:list_files(Clnt),
                {RqID, CMD, Rs};
            {RqID, {high_error, ErrCode, ErrMsg}=CMD} ->
                Rs = {ErrCode, ErrMsg},
                {RqID, CMD, Rs}
        end,
    machi_pb_translate:to_pb(ReqID, Cmd, Result).
