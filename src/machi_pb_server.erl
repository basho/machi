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

-module(machi_pb_server).

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").

-define(SERVER_CMD_READ_TIMEOUT, 600*1000).

-export([run_loop/2]).

run_loop(Sock, P_srvr_list) ->
    ok = inet:setopts(Sock, [{packet, 4},
                             {packet_size, 33*1024*1024}]),
    {ok, Clnt} = machi_cr_client:start_link(P_srvr_list),
    protocol_buffers_loop(Sock, Clnt).

protocol_buffers_loop(Sock, Clnt) ->
    case gen_tcp:recv(Sock, 0, ?SERVER_CMD_READ_TIMEOUT) of
        {ok, Bin} ->
            R = do_pb_request(catch machi_pb:decode_mpb_request(Bin), Clnt),
            %% R = #mpb_response{req_id= <<"not paying any attention">>,
            %%                   generic=#mpb_errorresp{code=-6,
            %%                                          msg="not implemented"}},
            Resp = machi_pb:encode_mpb_response(R),
            ok = gen_tcp:send(Sock, Resp),
            protocol_buffers_loop(Sock, Clnt);
        {error, _} ->
            (catch gen_tcp:close(Sock)),
            exit(normal)
    end.

do_pb_request(#mpb_request{req_id=ReqID,
                           echo=#mpb_echoreq{message=Msg}}, _Clnt) ->
    #mpb_response{req_id=ReqID,
                  echo=#mpb_echoresp{message=Msg}};
do_pb_request(#mpb_request{req_id=ReqID,
                           auth=#mpb_authreq{}}, _Clnt) ->
    #mpb_response{req_id=ReqID,
                  generic=#mpb_errorresp{code=1,
                                         msg="AUTH not implemented"}};
do_pb_request(#mpb_request{req_id=ReqID,
                           append_chunk=IR=#mpb_appendchunkreq{}}, Clnt) ->
    #mpb_appendchunkreq{placement_key=__todo__PK,
                        prefix=Prefix,
                        chunk=ChunkBin,
                        csum=CSum,
                        chunk_extra=ChunkExtra} = IR,
    TaggedCSum = make_tagged_csum(CSum, ChunkBin),
    Chunk = {TaggedCSum, ChunkBin},
    case (catch machi_cr_client:append_chunk_extra(Clnt, Prefix, Chunk,
                                                   ChunkExtra)) of
        {ok, {Offset, Size, File}} ->
            make_append_resp(ReqID, 'OK',
                             #mpb_chunkpos{offset=Offset,
                                           chunk_size=Size,
                                           file_name=File});
        {error, bad_arg} ->
            make_append_resp(ReqID, 'BAD_ARG');
        {error, wedged} ->
            make_append_resp(ReqID, 'WEDGED');
        {error, bad_checksum} ->
            make_append_resp(ReqID, 'BAD_CHECKSUM');
        {error, partition} ->
            make_append_resp(ReqID, 'PARTITION');
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
do_pb_request(#mpb_request{req_id=ReqID,
                           write_chunk=IR=#mpb_writechunkreq{}}, Clnt) ->
    #mpb_writechunkreq{file=File,
                       offset=Offset,
                       chunk=ChunkBin,
                       csum=CSum} = IR,
    TaggedCSum = make_tagged_csum(CSum, ChunkBin),
    Chunk = {TaggedCSum, ChunkBin},
    case (catch machi_cr_client:write_chunk(Clnt, File, Offset, Chunk)) of
        {ok, {_,_,_}} ->
            %% machi_cr_client returns ok 2-tuple, convert to simple ok.
            make_write_resp(ReqID, 'OK');
        {error, bad_arg} ->
            make_write_resp(ReqID, 'BAD_ARG');
        {error, wedged} ->
            make_write_resp(ReqID, 'WEDGED');
        {error, bad_checksum} ->
            make_write_resp(ReqID, 'BAD_CHECKSUM');
        {error, partition} ->
            make_write_resp(ReqID, 'PARTITION');
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
do_pb_request(#mpb_request{req_id=ReqID,
                           read_chunk=IR=#mpb_readchunkreq{}}, Clnt) ->
    #mpb_readchunkreq{file=File,
                      offset=Offset,
                      size=Size} = IR,
    %% TODO: implement the optional flags in Mpb_ReadChunkReq
    case (catch machi_cr_client:read_chunk(Clnt, File, Offset, Size)) of
        {ok, Chunk} ->
            make_read_resp(ReqID, 'OK', Chunk);
        {error, bad_arg} ->
            make_read_resp(ReqID, 'BAD_ARG', undefined);
        {error, wedged} ->
            make_read_resp(ReqID, 'WEDGED', undefined);
        {error, bad_checksum} ->
            make_read_resp(ReqID, 'BAD_CHECKSUM', undefined);
        {error, partition} ->
            make_read_resp(ReqID, 'PARTITION', undefined);
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
do_pb_request(#mpb_request{req_id=ReqID,
                           checksum_list=IR=#mpb_checksumlistreq{}}, Clnt) ->
    #mpb_checksumlistreq{file=File} = IR,
    %% TODO: implement the optional flags in Mpb_ReadChunkReq
    case (catch machi_cr_client:checksum_list(Clnt, File)) of
        {ok, Chunk} ->
            make_checksum_list_resp(ReqID, 'OK', Chunk);
        {error, bad_arg} ->
            make_checksum_list_resp(ReqID, 'BAD_ARG', undefined);
        {error, wedged} ->
            make_checksum_list_resp(ReqID, 'WEDGED', undefined);
        {error, bad_checksum} ->
            make_checksum_list_resp(ReqID, 'BAD_CHECKSUM', undefined);
        {error, partition} ->
            make_checksum_list_resp(ReqID, 'PARTITION', undefined);
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
do_pb_request(#mpb_request{req_id=ReqID,
                           list_files=IR=#mpb_listfilesreq{}}, Clnt) ->
    #mpb_listfilesreq{} = IR,
    %% TODO: implement the optional flags in Mpb_ReadChunkReq
    case (catch machi_cr_client:list_files(Clnt)) of
        {ok, FileInfo} ->
            make_list_files_resp(ReqID, 'OK', FileInfo);
        {error, bad_arg} ->
            make_list_files_resp(ReqID, 'BAD_ARG', []);
        {error, wedged} ->
            make_list_files_resp(ReqID, 'WEDGED', []);
        {error, bad_checksum} ->
            make_list_files_resp(ReqID, 'BAD_CHECKSUM', []);
        {error, partition} ->
            make_list_files_resp(ReqID, 'PARTITION', []);
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
do_pb_request(#mpb_request{req_id=ReqID}, _Clnt) ->
    #mpb_response{req_id=ReqID,
                  generic=#mpb_errorresp{code=66,
                                         msg="Unknown request"}};
do_pb_request(_Else, _Clnt) ->
    #mpb_response{req_id= <<>>,
                  generic=#mpb_errorresp{code=67,
                                         msg="Unknown PB request"}}.

make_tagged_csum(#mpb_chunkcsum{type='CSUM_TAG_NONE'}, ChunkBin) ->
    C = machi_util:checksum_chunk(ChunkBin),
    machi_util:make_tagged_csum(server_sha, C);
make_tagged_csum(#mpb_chunkcsum{type='CSUM_TAG_CLIENT_SHA', csum=CSum}, _CB) ->
    machi_util:make_tagged_csum(client_sha, CSum).

make_append_resp(ReqID, Status) ->
    make_append_resp(ReqID, Status, undefined).

make_append_resp(ReqID, Status, Where) ->
    #mpb_response{req_id=ReqID,
                  append_chunk=#mpb_appendchunkresp{status=Status,
                                                    chunk_pos=Where}}.

make_write_resp(ReqID, Status) ->
    #mpb_response{req_id=ReqID,
                  write_chunk=#mpb_writechunkresp{status=Status}}.

make_read_resp(ReqID, Status, Chunk) ->
    #mpb_response{req_id=ReqID,
                  read_chunk=#mpb_readchunkresp{status=Status,
                                                chunk=Chunk}}.

make_checksum_list_resp(ReqID, Status, __todo__Chunk) ->
    Chunk = <<"TODO item: refactor the checksum_list op to return simply the text file representation of the checksums?">>,
    #mpb_response{req_id=ReqID,
                  checksum_list=#mpb_checksumlistresp{status=Status,
                                                      chunk=Chunk}}.

make_list_files_resp(ReqID, Status, FileInfo) ->
    Files = [#mpb_fileinfo{file_size=Size, file_name=Name} ||
                {Size, Name} <- FileInfo],
    #mpb_response{req_id=ReqID,
                  list_files=#mpb_listfilesresp{status=Status,
                                                files=Files}}.

make_error_resp(ReqID, Code, Msg) ->
    #mpb_response{req_id=ReqID,
                  generic=#mpb_errorresp{code=Code,
                                         msg=Msg}}.    
