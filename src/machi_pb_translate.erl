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
%% -------------------------------------------------------------------
%%

-module(machi_pb_translate).

%% @doc Adapt impedence mismatches between Erlang and Protocol Buffers.

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-include_lib("pulse_otp/include/pulse_otp.hrl").
-endif.

-export([from_pb_request/1,
         from_pb_response/1,
         to_pb_request/2,
         to_pb_response/3,
         conv_from_append_opts/1,
         conv_to_append_opts/1
        ]).

%% TODO: fixme cleanup
-export([conv_to_csum_tag/1]).

from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   echo=#mpb_echoreq{message=Msg}}) ->
    {ReqID, {low_skip_wedge, {low_echo, Msg}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   auth=#mpb_authreq{user=User, password=Pass}}) ->
    {ReqID, {low_skip_wedge, {low_auth, User, Pass}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   append_chunk=IR=#mpb_ll_appendchunkreq{
                     namespace_version=NSVersion,
                     namespace=NS_str,
                     locator=NSLocator,
                     epoch_id=PB_EpochID,
                     prefix=Prefix,
                     chunk=Chunk,
                     csum=#mpb_chunkcsum{type=CSum_type, csum=CSum}}}) ->
    NS = list_to_binary(NS_str),
    EpochID = conv_to_epoch_id(PB_EpochID),
    CSum_tag = conv_to_csum_tag(CSum_type),
    Opts = conv_to_append_opts(IR),
    %% NOTE: The tuple position of NSLocator is a bit odd, because EpochID
    %%       _must_ be in the 4th position (as NSV & NS must be in 2nd & 3rd).
    {ReqID, {low_append_chunk, NSVersion, NS, EpochID, NSLocator,
             Prefix, Chunk, CSum_tag, CSum, Opts}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   write_chunk=#mpb_ll_writechunkreq{
                     namespace_version=NSVersion,
                     namespace=NS_str,
                     epoch_id=PB_EpochID,
                     chunk=#mpb_chunk{file_name=File,
                                      offset=Offset,
                                      chunk=Chunk,
                                      csum=#mpb_chunkcsum{type=CSum_type, csum=CSum}}}}) ->
    NS = list_to_binary(NS_str),
    EpochID = conv_to_epoch_id(PB_EpochID),
    CSum_tag = conv_to_csum_tag(CSum_type),
    {ReqID, {low_write_chunk, NSVersion, NS, EpochID, File, Offset, Chunk, CSum_tag, CSum}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   read_chunk=#mpb_ll_readchunkreq{
                                 namespace_version=NSVersion,
                                 namespace=NS_str,
                                 epoch_id=PB_EpochID,
                                 chunk_pos=ChunkPos,
                                 flag_no_checksum=PB_GetNoChecksum,
                                 flag_no_chunk=PB_GetNoChunk,
                                 flag_needs_trimmed=PB_NeedsTrimmed}}) ->
    NS = list_to_binary(NS_str),
    EpochID = conv_to_epoch_id(PB_EpochID),
    Opts = #read_opts{no_checksum=PB_GetNoChecksum,
                      no_chunk=PB_GetNoChunk,
                      needs_trimmed=PB_NeedsTrimmed},
    #mpb_chunkpos{file_name=File,
                  offset=Offset,
                  chunk_size=Size} = ChunkPos,
    {ReqID, {low_read_chunk, NSVersion, NS, EpochID, File, Offset, Size, Opts}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   trim_chunk=#mpb_ll_trimchunkreq{
                     namespace_version=NSVersion,
                     namespace=NS_str,
                     epoch_id=PB_EpochID,
                     file=File,
                     offset=Offset,
                     size=Size,
                     trigger_gc=TriggerGC}}) ->
    NS = list_to_binary(NS_str),
    EpochID = conv_to_epoch_id(PB_EpochID),
    {ReqID, {low_trim_chunk, NSVersion, NS, EpochID, File, Offset, Size, TriggerGC}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   checksum_list=#mpb_ll_checksumlistreq{
                     file=File}}) ->
    {ReqID, {low_skip_wedge, {low_checksum_list, File}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   list_files=#mpb_ll_listfilesreq{
                     epoch_id=PB_EpochID}}) ->
    EpochID = conv_to_epoch_id(PB_EpochID),
    {ReqID, {low_skip_wedge, {low_list_files, EpochID}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   wedge_status=#mpb_ll_wedgestatusreq{}}) ->
    {ReqID, {low_skip_wedge, {low_wedge_status}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   delete_migration=#mpb_ll_deletemigrationreq{
                    epoch_id=PB_EpochID,
                    file=File}}) ->
    EpochID = conv_to_epoch_id(PB_EpochID),
    {ReqID, {low_skip_wedge, {low_delete_migration, EpochID, File}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   trunc_hack=#mpb_ll_trunchackreq{
                    epoch_id=PB_EpochID,
                    file=File}}) ->
    EpochID = conv_to_epoch_id(PB_EpochID),
    {ReqID, {low_skip_wedge, {low_trunc_hack, EpochID, File}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   proj_gl=#mpb_ll_getlatestepochidreq{type=ProjType}}) ->
    {ReqID, {low_proj, {get_latest_epochid, conv_to_type(ProjType)}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   proj_rl=#mpb_ll_readlatestprojectionreq{type=ProjType}}) ->
    {ReqID, {low_proj, {read_latest_projection, conv_to_type(ProjType)}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   proj_rp=#mpb_ll_readprojectionreq{type=ProjType,
                                                     epoch_number=Epoch}}) ->
    {ReqID, {low_proj, {read_projection, conv_to_type(ProjType), Epoch}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   proj_wp=#mpb_ll_writeprojectionreq{type=ProjType,
                                                      proj=ProjM}}) ->
    Proj = conv_to_projection_v1(ProjM),
    {ReqID, {low_proj, {write_projection, conv_to_type(ProjType), Proj}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   proj_ga=#mpb_ll_getallprojectionsreq{type=ProjType}}) ->
    {ReqID, {low_proj, {get_all_projections, conv_to_type(ProjType)}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   proj_la=#mpb_ll_listallprojectionsreq{type=ProjType}}) ->
    {ReqID, {low_proj, {list_all_projections, conv_to_type(ProjType)}}};
from_pb_request(#mpb_ll_request{
                   req_id=ReqID,
                   proj_kp=#mpb_ll_kickprojectionreactionreq{}}) ->
    {ReqID, {low_proj, {kick_projection_reaction}}};
%%qqq
from_pb_request(#mpb_request{req_id=ReqID,
                             echo=#mpb_echoreq{message=Msg}}) ->
    {ReqID, {high_echo, Msg}};
from_pb_request(#mpb_request{req_id=ReqID,
                             auth=#mpb_authreq{user=User, password=Pass}}) ->
    {ReqID, {high_auth, User, Pass}};
from_pb_request(#mpb_request{req_id=ReqID,
                             append_chunk=IR=#mpb_appendchunkreq{}}) ->
    #mpb_appendchunkreq{namespace=NS_str,
                        prefix=Prefix,
                        chunk=Chunk,
                        csum=CSum} = IR,
    NS = list_to_binary(NS_str),
    TaggedCSum = make_tagged_csum(CSum, Chunk),
    Opts = conv_to_append_opts(IR),
    {ReqID, {high_append_chunk, NS, Prefix, Chunk, TaggedCSum, Opts}};
from_pb_request(#mpb_request{req_id=ReqID,
                             write_chunk=IR=#mpb_writechunkreq{}}) ->
    #mpb_writechunkreq{chunk=#mpb_chunk{file_name=File,
                                        offset=Offset,
                                        chunk=Chunk,
                                        csum=CSumRec}} = IR,
    CSum = make_tagged_csum(CSumRec, Chunk),
    {ReqID, {high_write_chunk, File, Offset, Chunk, CSum}};
from_pb_request(#mpb_request{req_id=ReqID,
                             read_chunk=IR=#mpb_readchunkreq{}}) ->
    #mpb_readchunkreq{chunk_pos=#mpb_chunkpos{file_name=File,
                                              offset=Offset,
                                              chunk_size=Size},
                     flag_no_checksum=FlagNoChecksum,
                     flag_no_chunk=FlagNoChunk,
                     flag_needs_trimmed=NeedsTrimmed} = IR,
    Opts = #read_opts{no_checksum=FlagNoChecksum,
                      no_chunk=FlagNoChunk,
                      needs_trimmed=NeedsTrimmed},
    {ReqID, {high_read_chunk, File, Offset, Size, Opts}};
from_pb_request(#mpb_request{req_id=ReqID,
                             trim_chunk=IR=#mpb_trimchunkreq{}}) ->
    #mpb_trimchunkreq{chunk_pos=#mpb_chunkpos{file_name=File,
                                              offset=Offset,
                                              chunk_size=Size}} = IR,
    {ReqID, {high_trim_chunk, File, Offset, Size}};
from_pb_request(#mpb_request{req_id=ReqID,
                             checksum_list=IR=#mpb_checksumlistreq{}}) ->
    #mpb_checksumlistreq{file=File} = IR,
    {ReqID, {high_checksum_list, File}};
from_pb_request(#mpb_request{req_id=ReqID,
                             list_files=_IR=#mpb_listfilesreq{}}) ->
    {ReqID, {high_list_files}};
from_pb_request(#mpb_request{req_id=ReqID}) ->
    {ReqID, {high_error, 999966, "Unknown request"}};
from_pb_request(_Else) ->
    io:format(user, "\nRRR from_pb_request(~p)\n", [_Else]), %%timer:sleep(2000),
    {<<>>, {high_error, 999667, "Unknown PB request"}}.

from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    % There is no separate LL error response record
                    generic=#mpb_errorresp{code=Code, msg=Msg}}) ->
    {ReqID, {error, {Code, Msg}}};
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    % There is no separate LL echo response record
                    echo=#mpb_echoresp{message=Msg}}) ->
    {ReqID, Msg};
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    % There is no separate LL auth response record
                    auth=#mpb_authresp{code=Code}}) ->
    {ReqID, Code};
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    append_chunk=#mpb_ll_appendchunkresp{status=Status,
                                                        chunk_pos=ChunkPos}}) ->
    case Status of
        'OK' ->
            #mpb_chunkpos{offset=Offset,
                          chunk_size=Size,
                          file_name=File} = ChunkPos,
            {ReqID, {ok, {Offset, Size, list_to_binary(File)}}};
        _ ->
            {ReqID, machi_pb_high_client:convert_general_status_code(Status)}
    end;
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    write_chunk=#mpb_ll_writechunkresp{status=Status}}) ->
    {ReqID, machi_pb_high_client:convert_general_status_code(Status)};
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    read_chunk=#mpb_ll_readchunkresp{status=Status,
                                                     chunks=PB_Chunks,
                                                     trimmed=PB_Trimmed}}) ->
    case Status of
        'OK' ->
            Chunks = lists:map(fun(#mpb_chunk{file_name=File,
                                          offset=Offset,
                                          chunk=Bytes,
                                          csum=#mpb_chunkcsum{type=T,csum=Ck}}) ->
                                       Csum = <<(conv_to_csum_tag(T)):8, Ck/binary>>,
                                      {list_to_binary(File), Offset, Bytes, Csum}
                              end, PB_Chunks),
            Trimmed = lists:map(fun(#mpb_chunkpos{file_name=File,
                                                  offset=Offset,
                                                  chunk_size=Size}) ->
                                        {list_to_binary(File), Offset, Size}
                                end, PB_Trimmed),
            {ReqID, {ok, {Chunks, Trimmed}}};
        _ ->
            {ReqID, machi_pb_high_client:convert_general_status_code(Status)}
    end;
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    trim_chunk=#mpb_ll_trimchunkresp{status=Status}}) ->
    {ReqID, machi_pb_high_client:convert_general_status_code(Status)};
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    checksum_list=#mpb_ll_checksumlistresp{
                      status=Status, chunk=Chunk}}) ->
    case Status of
        'OK' ->
            {ReqID, {ok, Chunk}};
        _ ->
            {ReqID, machi_pb_high_client:convert_general_status_code(Status)}
    end;
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    list_files=#mpb_ll_listfilesresp{
                      status=Status, files=PB_Files}}) ->
    case Status of
        'OK' ->
            Files = [{Size, list_to_binary(Name)} ||
                        #mpb_fileinfo{file_size=Size,
                                      file_name=Name} <- PB_Files],
            {ReqID, {ok, Files}};
        _ ->
            {ReqID, machi_pb_high_client:convert_general_status_code(Status)}
    end;
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    wedge_status=#mpb_ll_wedgestatusresp{
                      status=Status,
                      epoch_id=PB_EpochID, wedged_flag=Wedged_p,
                      namespace_version=NSVersion, namespace=NS_str}}) ->
    GeneralStatus = case machi_pb_high_client:convert_general_status_code(Status) of
                        ok    -> ok;
                        _Else -> {yukky, _Else}
                    end,
    EpochID = conv_to_epoch_id(PB_EpochID),
    NS = list_to_binary(NS_str),
    {ReqID, {GeneralStatus, {Wedged_p, EpochID, NSVersion, NS}}};
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    delete_migration=#mpb_ll_deletemigrationresp{
                      status=Status}}) ->
    {ReqID, machi_pb_high_client:convert_general_status_code(Status)};
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    trunc_hack=#mpb_ll_trunchackresp{
                      status=Status}}) ->
    {ReqID, machi_pb_high_client:convert_general_status_code(Status)};
%%qqq
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    proj_gl=#mpb_ll_getlatestepochidresp{
                      status=Status, epoch_id=EID}}) ->
    case Status of
        'OK' ->
            #mpb_epochid{epoch_number=Epoch, epoch_csum=CSum} = EID,
            {ReqID, {ok, {Epoch, CSum}}};
        _ ->
            {ReqID, machi_pb_high_client:convert_general_status_code(Status)}
    end;
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    proj_rl=#mpb_ll_readlatestprojectionresp{
                      status=Status, proj=P}}) ->
    case Status of
        'OK' ->
            {ReqID, {ok, conv_to_projection_v1(P)}};
        _ ->
            {ReqID, machi_pb_high_client:convert_general_status_code(Status)}
    end;
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    proj_rp=#mpb_ll_readprojectionresp{
                      status=Status, proj=P}}) ->
    case Status of
        'OK' ->
            {ReqID, {ok, conv_to_projection_v1(P)}};
        _ ->
            {ReqID, machi_pb_high_client:convert_general_status_code(Status)}
    end;
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    proj_wp=#mpb_ll_writeprojectionresp{
                      status=Status}}) ->
    {ReqID, machi_pb_high_client:convert_general_status_code(Status)};
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    proj_ga=#mpb_ll_getallprojectionsresp{
                      status=Status, projs=ProjsM}}) ->
    case Status of
        'OK' ->
            {ReqID, {ok, [conv_to_projection_v1(ProjM) || ProjM <- ProjsM]}};
        _ ->
            {ReqID, machi_pb_high_client:convert_general_status_code(Status)}
    end;
from_pb_response(#mpb_ll_response{
                    req_id=ReqID,
                    proj_la=#mpb_ll_listallprojectionsresp{
                      status=Status, epochs=Epochs}}) ->
    case Status of
        'OK' ->
            {ReqID, {ok, Epochs}};
        _ ->
            {ReqID, machi_pb_high_client:convert_general_status_code(Status)}
    end.
%% No response for proj_kp/kick_projection_reaction

%% TODO: move the #mbp_* record making code from
%%       machi_pb_high_client:do_send_sync() clauses into to_pb_request().

to_pb_request(ReqID, {low_skip_wedge, {low_echo, Msg}}) ->
    #mpb_ll_request{
               req_id=ReqID, do_not_alter=2,
               echo=#mpb_echoreq{message=Msg}};
to_pb_request(ReqID, {low_skip_wedge, {low_auth, User, Pass}}) ->
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    auth=#mpb_authreq{user=User, password=Pass}};
%% NOTE: The tuple position of NSLocator is a bit odd, because EpochID
%%       _must_ be in the 4th position (as NSV & NS must be in 2nd & 3rd).
to_pb_request(ReqID, {low_append_chunk, NSVersion, NS, EpochID, NSLocator,
                      Prefix, Chunk, CSum_tag, CSum, Opts}) ->
    PB_EpochID = conv_from_epoch_id(EpochID),
    CSum_type = conv_from_csum_tag(CSum_tag),
    PB_CSum = #mpb_chunkcsum{type=CSum_type, csum=CSum},
    {ChunkExtra, Pref, FailPref} = conv_from_append_opts(Opts),
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    append_chunk=#mpb_ll_appendchunkreq{
                      namespace_version=NSVersion,
                      namespace=NS,
                      locator=NSLocator,
                      epoch_id=PB_EpochID,
                      prefix=Prefix,
                      chunk=Chunk,
                      csum=PB_CSum,
                      chunk_extra=ChunkExtra,
                      preferred_file_name=Pref,
                      flag_fail_preferred=FailPref}};
to_pb_request(ReqID, {low_write_chunk, NSVersion, NS, EpochID, File, Offset, Chunk, CSum_tag, CSum}) ->
    PB_EpochID = conv_from_epoch_id(EpochID),
    CSum_type = conv_from_csum_tag(CSum_tag),
    PB_CSum = #mpb_chunkcsum{type=CSum_type, csum=CSum},
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    write_chunk=#mpb_ll_writechunkreq{
                      namespace_version=NSVersion,
                      namespace=NS,
                      epoch_id=PB_EpochID,
                                   chunk=#mpb_chunk{file_name=File,
                                                    offset=Offset,
                                                    chunk=Chunk,
                                                    csum=PB_CSum}}};
to_pb_request(ReqID, {low_read_chunk, NSVersion, NS, EpochID, File, Offset, Size, Opts}) ->
    PB_EpochID = conv_from_epoch_id(EpochID),
    #read_opts{no_checksum=FNChecksum,
               no_chunk=FNChunk,
               needs_trimmed=NeedsTrimmed} = Opts,
    #mpb_ll_request{
               req_id=ReqID, do_not_alter=2,
               read_chunk=#mpb_ll_readchunkreq{
                            namespace_version=NSVersion,
                            namespace=NS,
                            epoch_id=PB_EpochID,
                            chunk_pos=#mpb_chunkpos{
                                          file_name=File,
                                          offset=Offset,
                                          chunk_size=Size},
                            flag_no_checksum=FNChecksum,
                            flag_no_chunk=FNChunk,
                            flag_needs_trimmed=NeedsTrimmed}};
to_pb_request(ReqID, {low_trim_chunk, NSVersion, NS, EpochID, File, Offset, Size, TriggerGC}) ->
    PB_EpochID = conv_from_epoch_id(EpochID),
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    trim_chunk=#mpb_ll_trimchunkreq{
                                  namespace_version=NSVersion,
                                  namespace=NS,
                                  epoch_id=PB_EpochID,
                                  file=File,
                                  offset=Offset,
                                  size=Size,
                                  trigger_gc=TriggerGC}};
to_pb_request(ReqID, {low_skip_wedge, {low_checksum_list, File}}) ->
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    checksum_list=#mpb_ll_checksumlistreq{
                      file=File}};
to_pb_request(ReqID, {low_skip_wedge, {low_list_files, EpochID}}) ->
    PB_EpochID = conv_from_epoch_id(EpochID),
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    list_files=#mpb_ll_listfilesreq{epoch_id=PB_EpochID}};
to_pb_request(ReqID, {low_skip_wedge, {low_wedge_status}}) ->
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    wedge_status=#mpb_ll_wedgestatusreq{}};
to_pb_request(ReqID, {low_skip_wedge, {low_delete_migration, EpochID, File}}) ->
    PB_EpochID = conv_from_epoch_id(EpochID),
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    delete_migration=#mpb_ll_deletemigrationreq{
                     epoch_id=PB_EpochID,
                      file=File}};
to_pb_request(ReqID, {low_skip_wedge, {low_trunc_hack, EpochID, File}}) ->
    PB_EpochID = conv_from_epoch_id(EpochID),
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    trunc_hack=#mpb_ll_trunchackreq{
                     epoch_id=PB_EpochID,
                      file=File}};
to_pb_request(ReqID, {low_proj, {get_latest_epochid, ProjType}}) ->
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    proj_gl=#mpb_ll_getlatestepochidreq{type=conv_from_type(ProjType)}};
to_pb_request(ReqID, {low_proj, {read_latest_projection, ProjType}}) ->
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    proj_rl=#mpb_ll_readlatestprojectionreq{type=conv_from_type(ProjType)}};
to_pb_request(ReqID, {low_proj, {read_projection, ProjType, Epoch}}) ->
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    proj_rp=#mpb_ll_readprojectionreq{type=conv_from_type(ProjType),
                                              epoch_number=Epoch}};
to_pb_request(ReqID, {low_proj, {write_projection, ProjType, Proj}}) ->
    ProjM = conv_from_projection_v1(Proj),
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    proj_wp=#mpb_ll_writeprojectionreq{type=conv_from_type(ProjType),
                                                       proj=ProjM}};
to_pb_request(ReqID, {low_proj, {get_all_projections, ProjType}}) ->
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    proj_ga=#mpb_ll_getallprojectionsreq{type=conv_from_type(ProjType)}};
to_pb_request(ReqID, {low_proj, {list_all_projections, ProjType}}) ->
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    proj_la=#mpb_ll_listallprojectionsreq{type=conv_from_type(ProjType)}};
to_pb_request(ReqID, {low_proj, {kick_projection_reaction}}) ->
    #mpb_ll_request{req_id=ReqID, do_not_alter=2,
                    proj_kp=#mpb_ll_kickprojectionreactionreq{}}.
%%qqq

to_pb_response(_ReqID, _, async_no_response=X) ->
    X;
to_pb_response(ReqID, _, {low_error, ErrCode, ErrMsg}) ->
    make_ll_error_resp(ReqID, ErrCode, ErrMsg);
to_pb_response(ReqID, {low_skip_wedge, {low_echo, _Msg}}, Resp) ->
    #mpb_ll_response{
                req_id=ReqID,
                echo=#mpb_echoresp{message=Resp}};
to_pb_response(ReqID, {low_skip_wedge, {low_auth, _, _}}, __TODO_Resp) ->
    #mpb_ll_response{req_id=ReqID,
                     generic=#mpb_errorresp{code=1,
                                            msg="AUTH not implemented"}};
to_pb_response(ReqID, {low_append_chunk, _NSV, _NS, _EID, _NSL, _Pfx, _Ch, _CST, _CS, _O}, Resp)->
    case Resp of
        {ok, {Offset, Size, File}} ->
            Where = #mpb_chunkpos{offset=Offset,
                                  chunk_size=Size,
                                  file_name=File},
            #mpb_ll_response{req_id=ReqID,
                             append_chunk=#mpb_ll_appendchunkresp{status='OK',
                                                              chunk_pos=Where}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                           append_chunk=#mpb_ll_appendchunkresp{status=Status}};
        _Else ->
            make_ll_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {low_write_chunk, _NSV, _NS, _EID, _Fl, _Off, _Ch, _CST, _CS},Resp)->
    Status = conv_from_status(Resp),
    #mpb_ll_response{req_id=ReqID,
                     write_chunk=#mpb_ll_writechunkresp{status=Status}};
to_pb_response(ReqID, {low_read_chunk, _NSV, _NS, _EID, _Fl, _Off, _Sz, _Opts}, Resp)->
    case Resp of
        {ok, {Chunks, Trimmed}} ->
            PB_Chunks = lists:map(fun({File, Offset, Bytes, Csum}) ->
                                          {Tag, Ck} = machi_util:unmake_tagged_csum(Csum),
                                          #mpb_chunk{file_name=File,
                                                     offset=Offset,
                                                     chunk=Bytes,
                                                     csum=#mpb_chunkcsum{type=conv_from_csum_tag(Tag),
                                                                         csum=Ck}}
                                  end, Chunks),
            PB_Trimmed = lists:map(fun({File, Offset, Size}) ->
                                           #mpb_chunkpos{file_name=File,
                                                         offset=Offset,
                                                         chunk_size=Size}
                                   end, Trimmed),
            #mpb_ll_response{req_id=ReqID,
                             read_chunk=#mpb_ll_readchunkresp{status='OK',
                                                              chunks=PB_Chunks,
                                                              trimmed=PB_Trimmed}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                             read_chunk=#mpb_ll_readchunkresp{status=Status}};
        _Else ->
            make_ll_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {low_trim_chunk, _, _, _, _, _, _, _}, Resp) ->
    case Resp of
        ok ->
            #mpb_ll_response{req_id=ReqID,
                             trim_chunk=#mpb_ll_trimchunkresp{status='OK'}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                             trim_chunk=#mpb_ll_trimchunkresp{status=Status}};
        _Else ->
            make_ll_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {low_skip_wedge, {low_checksum_list, _File}}, Resp) ->
    case Resp of
        {ok, Chunk} ->
            #mpb_ll_response{req_id=ReqID,
                           checksum_list=#mpb_ll_checksumlistresp{status='OK',
                                                                  chunk=Chunk}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                         checksum_list=#mpb_ll_checksumlistresp{status=Status}};
        _Else ->
            make_ll_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {low_skip_wedge, {low_list_files, _EpochID}}, Resp) ->
    case Resp of
        {ok, FileInfo} ->
            PB_Files = [#mpb_fileinfo{file_size=Size, file_name=Name} ||
                           {Size, Name} <- FileInfo],
            #mpb_ll_response{req_id=ReqID,
                             list_files=#mpb_ll_listfilesresp{status='OK',
                                                              files=PB_Files}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                         list_files=#mpb_ll_listfilesresp{status=Status}};
        _Else ->
            make_ll_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {low_skip_wedge, {low_wedge_status}}, Resp) ->
    case Resp of
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                           wedge_status=#mpb_ll_wedgestatusresp{status=Status}};
        {Wedged_p, EpochID, NSVersion, NS} ->
            PB_EpochID = conv_from_epoch_id(EpochID),
            #mpb_ll_response{req_id=ReqID,
                             wedge_status=#mpb_ll_wedgestatusresp{
                               status='OK',
                               epoch_id=PB_EpochID,
                               wedged_flag=Wedged_p,
                               namespace_version=NSVersion,
                               namespace=NS
                              }}
    end;
to_pb_response(ReqID, {low_skip_wedge, {low_delete_migration, _EID, _Fl}}, Resp)->
    Status = conv_from_status(Resp),
    #mpb_ll_response{req_id=ReqID,
                   delete_migration=#mpb_ll_deletemigrationresp{status=Status}};
to_pb_response(ReqID, {low_skip_wedge, {low_trunc_hack, _EID, _Fl}}, Resp)->
    Status = conv_from_status(Resp),
    #mpb_ll_response{req_id=ReqID,
                     trunc_hack=#mpb_ll_trunchackresp{status=Status}};
to_pb_response(ReqID, {low_proj, {get_latest_epochid, _ProjType}}, Resp)->
    case Resp of
        {ok, {Epoch, CSum}} ->
            EID = #mpb_epochid{epoch_number=Epoch, epoch_csum=CSum},
            #mpb_ll_response{req_id=ReqID,
                             proj_gl=#mpb_ll_getlatestepochidresp{
                               status='OK', epoch_id=EID}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                            proj_gl=#mpb_ll_getlatestepochidresp{status=Status}}
    end;
to_pb_response(ReqID, {low_proj, {read_latest_projection, _ProjType}}, Resp) ->
    case Resp of
        {ok, Proj} ->
            ProjM = conv_from_projection_v1(Proj),
            #mpb_ll_response{req_id=ReqID,
                             proj_rl=#mpb_ll_readlatestprojectionresp{
                               status='OK', proj=ProjM}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                       proj_rl=#mpb_ll_readlatestprojectionresp{status=Status}}
    end;
to_pb_response(ReqID, {low_proj, {read_projection, _ProjType, _Epoch}}, Resp)->
    case Resp of
        {ok, Proj} ->
            ProjM = conv_from_projection_v1(Proj),
            #mpb_ll_response{req_id=ReqID,
                             proj_rp=#mpb_ll_readprojectionresp{
                               status='OK', proj=ProjM}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                             proj_rp=#mpb_ll_readprojectionresp{status=Status}}
    end;
to_pb_response(ReqID, {low_proj, {write_projection, _ProjType, _Proj}}, Resp) ->
    Status = conv_from_status(Resp),
    #mpb_ll_response{req_id=ReqID,
                     proj_wp=#mpb_ll_writeprojectionresp{status=Status}};
to_pb_response(ReqID, {low_proj, {get_all_projections, _ProjType}}, Resp)->
    case Resp of
        {ok, Projs} ->
            ProjsM = [conv_from_projection_v1(Proj) || Proj <- Projs],
            #mpb_ll_response{req_id=ReqID,
                             proj_ga=#mpb_ll_getallprojectionsresp{
                               status='OK', projs=ProjsM}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                             proj_ga=#mpb_ll_getallprojectionsresp{
                               status=Status}}
    end;
to_pb_response(ReqID, {low_proj, {list_all_projections, _ProjType}}, Resp)->
    case Resp of
        {ok, Epochs} ->
            #mpb_ll_response{req_id=ReqID,
                             proj_la=#mpb_ll_listallprojectionsresp{
                               status='OK', epochs=Epochs}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_ll_response{req_id=ReqID,
                             proj_la=#mpb_ll_listallprojectionsresp{
                               status=Status}}
    end;
%% No response for {kick_projection_reaction}!
%%qqq
to_pb_response(ReqID, _, {high_error, ErrCode, ErrMsg}) ->
    make_error_resp(ReqID, ErrCode, ErrMsg);
to_pb_response(ReqID, {high_echo, _Msg}, Resp) ->
    Msg = Resp,
    #mpb_response{req_id=ReqID,
                  echo=#mpb_echoresp{message=Msg}};
to_pb_response(ReqID, {high_auth, _User, _Pass}, _Resp) ->
    #mpb_response{req_id=ReqID,
                  generic=#mpb_errorresp{code=1,
                                         msg="AUTH not implemented"}};
to_pb_response(ReqID, {high_append_chunk, _NS, _Prefix, _Chunk, _TSum, _O}, Resp)->
    case Resp of
        {ok, {Offset, Size, File}} ->
            Where = #mpb_chunkpos{offset=Offset,
                                  chunk_size=Size,
                                  file_name=File},
            #mpb_response{req_id=ReqID,
                          append_chunk=#mpb_appendchunkresp{status='OK',
                                                            chunk_pos=Where}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_response{req_id=ReqID,
                          append_chunk=#mpb_appendchunkresp{status=Status}};
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {high_write_chunk, _File, _Offset, _Chunk, _CSum}, Resp) ->
    case Resp of
        {ok, {_,_,_}} ->
            %% machi_cr_client returns ok 2-tuple, convert to simple ok.
            #mpb_response{req_id=ReqID,
                          write_chunk=#mpb_writechunkresp{status='OK'}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_response{req_id=ReqID,
                          write_chunk=#mpb_writechunkresp{status=Status}};
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {high_read_chunk, _File, _Offset, _Size, _}, Resp) ->
    case Resp of
        {ok, {Chunks, Trimmed}} ->
            PB_Chunks = lists:map(fun({File, Offset, Bytes, Csum}) ->
                                          {Tag, Ck} = machi_util:unmake_tagged_csum(Csum),
                                          #mpb_chunk{
                                             offset=Offset,
                                             file_name=File,
                                             chunk=Bytes,
                                             csum=#mpb_chunkcsum{type=conv_from_csum_tag(Tag), csum=Ck}}
                                  end, Chunks),
            PB_Trimmed = lists:map(fun({File, Offset, Size}) ->
                                           #mpb_chunkpos{file_name=File,
                                                         offset=Offset,
                                                         chunk_size=Size}
                                   end, Trimmed),
            #mpb_response{req_id=ReqID,
                          read_chunk=#mpb_readchunkresp{status='OK',
                                                        chunks=PB_Chunks,
                                                        trimmed=PB_Trimmed}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_response{req_id=ReqID,
                          read_chunk=#mpb_readchunkresp{status=Status}};
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {high_trim_chunk, _File, _Offset, _Size}, Resp) ->
    case Resp of
        ok ->
            #mpb_response{req_id=ReqID,
                          trim_chunk=#mpb_trimchunkresp{status='OK'}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_response{req_id=ReqID,
                          trim_chunk=#mpb_trimchunkresp{status=Status}};
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {high_checksum_list, _File}, Resp) ->
    case Resp of
        {ok, Chunk} ->
            #mpb_response{req_id=ReqID,
                          checksum_list=#mpb_checksumlistresp{status='OK',
                                                              chunk=Chunk}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_response{req_id=ReqID,
                          checksum_list=#mpb_checksumlistresp{status=Status}};
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end;
to_pb_response(ReqID, {high_list_files}, Resp) ->
    case Resp of
        {ok, FileInfo} ->
            Files = [#mpb_fileinfo{file_size=Size, file_name=Name} ||
                        {Size, Name} <- FileInfo],
            #mpb_response{req_id=ReqID,
                          list_files=#mpb_listfilesresp{status='OK',
                                                        files=Files}};
        {error, _}=Error ->
            Status = conv_from_status(Error),
            #mpb_response{req_id=ReqID,
                          list_files=#mpb_listfilesresp{status=Status}};
        _Else ->
            make_error_resp(ReqID, 66, io_lib:format("err ~p", [_Else]))
    end.

make_tagged_csum(#mpb_chunkcsum{type='CSUM_TAG_NONE'}, Chunk) ->
    C = machi_util:checksum_chunk(Chunk),
    machi_util:make_tagged_csum(server_sha, C);
make_tagged_csum(#mpb_chunkcsum{type='CSUM_TAG_CLIENT_SHA', csum=CSum}, _CB) ->
    machi_util:make_tagged_csum(client_sha, CSum).

make_ll_error_resp(ReqID, Code, Msg) ->
    #mpb_ll_response{req_id=ReqID,
                     generic=#mpb_errorresp{code=Code,
                                            msg=Msg}}.

make_error_resp(ReqID, Code, Msg) ->
    #mpb_response{req_id=ReqID,
                  generic=#mpb_errorresp{code=Code,
                                         msg=Msg}}.

conv_from_epoch_id({Epoch, EpochCSum}) ->
    #mpb_epochid{epoch_number=Epoch,
                 epoch_csum=EpochCSum}.

conv_to_epoch_id(#mpb_epochid{epoch_number=Epoch,
                              epoch_csum=EpochCSum}) ->
    {Epoch, EpochCSum}.

conv_to_projection_v1(#mpb_projectionv1{epoch_number=Epoch,
                                        epoch_csum=CSum,
                                        author_server=Author,
                                        chain_name=ChainName,
                                        all_members=AllMembers,
                                        witnesses=Witnesses,
                                        creation_time=CTime,
                                        mode=Mode,
                                        upi=UPI,
                                        repairing=Repairing,
                                        down=Down,
                                        opaque_dbg=Dbg,
                                        opaque_dbg2=Dbg2,
                                        members_dict=MembersDict}) ->
    #projection_v1{epoch_number=Epoch,
                   epoch_csum=CSum,
                   author_server=to_atom(Author),
                   chain_name=to_atom(ChainName),
                   all_members=[to_atom(X) || X <- AllMembers],
                   witnesses=[to_atom(X) || X <- Witnesses],
                   creation_time=conv_to_now(CTime),
                   mode=conv_to_mode(Mode),
                   upi=[to_atom(X) || X <- UPI],
                   repairing=[to_atom(X) || X <- Repairing],
                   down=[to_atom(X) || X <- Down],
                   dbg=dec_sexp(Dbg),
                   dbg2=dec_sexp(Dbg2),
                   members_dict=conv_to_members_dict(MembersDict)}.

enc_sexp(T) ->
    term_to_binary(T).

dec_sexp(Bin) when is_binary(Bin) ->
    binary_to_term(Bin).

conv_from_members_dict(D) ->
    %% Use list_to_binary() here to "flatten" the serialized #p_srvr{}
    [#mpb_membersdictentry{key=to_list(K), val=conv_from_p_srvr(V)} ||
        {K, V} <- orddict:to_list(D)].

conv_to_members_dict(List) ->
    orddict:from_list([{to_atom(K), conv_to_p_srvr(V)} ||
                          #mpb_membersdictentry{key=K, val=V} <- List]).

conv_from_p_srvr(#p_srvr{name=Name,
                         proto_mod=ProtoMod,
                         address=Address,
                         port=Port,
                         props=Props}) ->
    #mpb_p_srvr{name=to_list(Name),
                proto_mod=to_list(ProtoMod),
                address=to_list(Address),
                port=to_list(Port),
                opaque_props=enc_sexp(Props)}.

conv_to_p_srvr(#mpb_p_srvr{name=Name,
                           proto_mod=ProtoMod,
                           address=Address,
                           port=Port,
                           opaque_props=Props}) ->
    #p_srvr{name=to_atom(Name),
            proto_mod=to_atom(ProtoMod),
            address=to_list(Address),
            port=to_integer(Port),
            props=dec_sexp(Props)}.

to_list(X) when is_atom(X) ->
    atom_to_list(X);
to_list(X) when is_binary(X) ->
    binary_to_list(X);
to_list(X) when is_integer(X) ->
    integer_to_list(X);
to_list(X) when is_list(X) ->
    X.

to_atom(X) when is_list(X) ->
    list_to_atom(X);
to_atom(X) when is_binary(X) ->
    erlang:binary_to_atom(X, latin1);
to_atom(X) when is_atom(X) ->
    X.

to_integer(X) when is_list(X) ->
    list_to_integer(X);
to_integer(X) when is_binary(X) ->
    list_to_binary(binary_to_list(X));
to_integer(X) when is_integer(X) ->
    X.

conv_from_csum_tag(CSum_tag) ->
    case CSum_tag of
        ?CSUM_TAG_NONE -> 'CSUM_TAG_NONE';
        ?CSUM_TAG_CLIENT_SHA -> 'CSUM_TAG_CLIENT_SHA';
        ?CSUM_TAG_SERVER_SHA -> 'CSUM_TAG_SERVER_SHA';
        ?CSUM_TAG_SERVER_REGEN_SHA -> 'CSUM_TAG_SERVER_REGEN_SHA'
    end.

conv_to_csum_tag(Type) ->
    case Type of
        'CSUM_TAG_NONE' -> ?CSUM_TAG_NONE;
        'CSUM_TAG_CLIENT_SHA' -> ?CSUM_TAG_CLIENT_SHA;
        'CSUM_TAG_SERVER_SHA' -> ?CSUM_TAG_SERVER_SHA;
        'CSUM_TAG_SERVER_REGEN_SHA' -> ?CSUM_TAG_SERVER_REGEN_SHA
    end.

conv_from_now({A,B,C}) ->
    #mpb_now{sec=(1000000 * A) + B,
             usec=C}.

conv_to_now(#mpb_now{sec=Sec, usec=USec}) ->
    {Sec div 1000000, Sec rem 1000000, USec}.

conv_from_mode(ap_mode) -> 'AP_MODE';
conv_from_mode(cp_mode) -> 'CP_MODE'.

conv_to_mode('AP_MODE') -> ap_mode;
conv_to_mode('CP_MODE') -> cp_mode.

conv_from_type(private) -> 'PRIVATE';
conv_from_type(public)  -> 'PUBLIC'.

conv_to_type('PRIVATE') -> private;
conv_to_type('PUBLIC')  -> public.

conv_from_status(ok) ->
    'OK';
conv_from_status({error, bad_arg}) ->
    'BAD_ARG';
conv_from_status({error, wedged}) ->
    'WEDGED';
conv_from_status({error, bad_checksum}) ->
    'BAD_CHECKSUM';
conv_from_status({error, partition}) ->
    'PARTITION';
conv_from_status({error, not_written}) ->
    'NOT_WRITTEN';
conv_from_status({error, written}) ->
    'WRITTEN';
conv_from_status({error, trimmed}) ->
    'TRIMMED';
conv_from_status({error, no_such_file}) ->
    'NO_SUCH_FILE';
conv_from_status({error, partial_read}) ->
    'PARTIAL_READ';
conv_from_status({error, bad_epoch}) ->
    'BAD_EPOCH';
conv_from_status(_OOPS) ->
    io:format(user, "HEY, ~s:~w got ~p\n", [?MODULE, ?LINE, _OOPS]),
    'BAD_JOSS'.

conv_from_append_opts(#append_opts{chunk_extra=ChunkExtra,
                                   preferred_file_name=Pref,
                                   flag_fail_preferred=FailPref}) ->
    {ChunkExtra, Pref, FailPref}.


conv_to_append_opts(#mpb_appendchunkreq{
                       chunk_extra=ChunkExtra,
                       preferred_file_name=Pref,
                       flag_fail_preferred=FailPref}) ->
    #append_opts{chunk_extra=ChunkExtra,
                 preferred_file_name=Pref,
                 flag_fail_preferred=FailPref};
conv_to_append_opts(#mpb_ll_appendchunkreq{
                       chunk_extra=ChunkExtra,
                       preferred_file_name=Pref,
                       flag_fail_preferred=FailPref}) ->
    #append_opts{chunk_extra=ChunkExtra,
                 preferred_file_name=Pref,
                 flag_fail_preferred=FailPref}.

conv_from_projection_v1(#projection_v1{epoch_number=Epoch,
                                       epoch_csum=CSum,
                                       author_server=Author,
                                       chain_name=ChainName,
                                       all_members=AllMembers,
                                       witnesses=Witnesses,
                                       creation_time=CTime,
                                       mode=Mode,
                                       upi=UPI,
                                       repairing=Repairing,
                                       down=Down,
                                       dbg=Dbg,
                                       dbg2=Dbg2,
                                       members_dict=MembersDict}) ->
    #mpb_projectionv1{epoch_number=Epoch,
                      epoch_csum=CSum,
                      author_server=to_list(Author),
                      chain_name=to_list(ChainName),
                      all_members=[to_list(X) || X <- AllMembers],
                      witnesses=[to_list(X) || X <- Witnesses],
                      creation_time=conv_from_now(CTime),
                      mode=conv_from_mode(Mode),
                      upi=[to_list(X) || X <- UPI],
                      repairing=[to_list(X) || X <- Repairing],
                      down=[to_list(X) || X <- Down],
                      opaque_dbg=enc_sexp(Dbg),
                      opaque_dbg2=enc_sexp(Dbg2),
                      members_dict=conv_from_members_dict(MembersDict)}.
