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

%% @doc Erlang API for the Machi FLU TCP protocol version 1.
%%
%% This client API handles low-level PDU serialization/deserialization
%% and low-level TCP session management, e.g. open, receive, write,
%% close.  The API for higher-level session management and Machi state
%% management can be found in {@link machi_proxy_flu1_client} and
%% {@link machi_cr_client}.
%%
%% For the moment, this module implements a Protocol Buffers-based
%% protocol as the sole supported access method to the server,
%% sequencer, and projection store.  Conceptually, those three
%% services are independent and ought to have their own protocols.  As
%% a practical matter, there is no need for wire protocol
%% compatibility.  Furthermore, from the perspective of failure
%% detection, it is very convenient that all three FLU-related
%% services are accessed using the same single TCP port.
%%
%% TODO This EDoc was written first, and the EDoc and also `-type' and
%% `-spec' definitions for {@link machi_proxy_flu1_client} and {@link
%% machi_cr_client} must be improved.

-module(machi_flu1_client).

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-include_lib("pulse_otp/include/pulse_otp.hrl").
-endif.

-define(HARD_TIMEOUT, 2500).

-export([
         %% File API
         append_chunk/4, append_chunk/5,
         append_chunk_extra/5, append_chunk_extra/6,
         read_chunk/5, read_chunk/6,
         checksum_list/3, checksum_list/4,
         list_files/2, list_files/3,
         wedge_status/1, wedge_status/2,

         %% Projection API
         get_latest_epochid/2, get_latest_epochid/3,
         read_latest_projection/2, read_latest_projection/3,
         read_projection/3, read_projection/4,
         write_projection/3, write_projection/4,
         get_all_projections/2, get_all_projections/3,
         list_all_projections/2, list_all_projections/3,
         kick_projection_reaction/2, kick_projection_reaction/3,

         %% Common API
         echo/2, echo/3,
         quit/1,

         %% Connection management API
         connected_p/1, connect/1, disconnect/1
        ]).
%% For "internal" replication only.
-export([
         write_chunk/5, write_chunk/6,
         trim_chunk/5, trim_chunk/6,
         delete_migration/3, delete_migration/4,
         trunc_hack/3, trunc_hack/4
        ]).

-type port_wrap()   :: {w,atom(),term()}.

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

-spec append_chunk(port_wrap(), machi_dt:epoch_id(), machi_dt:file_prefix(), machi_dt:chunk()) ->
      {ok, machi_dt:chunk_pos()} | {error, machi_dt:error_general()} | {error, term()}.
append_chunk(Sock, EpochID, Prefix, Chunk) ->
    append_chunk2(Sock, EpochID, Prefix, Chunk, 0).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

-spec append_chunk(machi_dt:inet_host(), machi_dt:inet_port(),
                   machi_dt:epoch_id(), machi_dt:file_prefix(), machi_dt:chunk()) ->
      {ok, machi_dt:chunk_pos()} | {error, machi_dt:error_general()} | {error, term()}.
append_chunk(Host, TcpPort, EpochID, Prefix, Chunk) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        append_chunk2(Sock, EpochID, Prefix, Chunk, 0)
    after
        disconnect(Sock)
    end.

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix' and also request an additional `Extra' bytes.
%%
%% For example, if the `Chunk' size is 1 KByte and `Extra' is 4K Bytes, then
%% the file offsets that follow `Chunk''s position for the following 4K will
%% be reserved by the file sequencer for later write(s) by the
%% `write_chunk()' API.

-spec append_chunk_extra(port_wrap(), machi_dt:epoch_id(), machi_dt:file_prefix(), machi_dt:chunk(), machi_dt:chunk_size()) ->
      {ok, machi_dt:chunk_pos()} | {error, machi_dt:error_general()} | {error, term()}.
append_chunk_extra(Sock, EpochID, Prefix, Chunk, ChunkExtra)
  when is_integer(ChunkExtra), ChunkExtra >= 0 ->
    append_chunk2(Sock, EpochID, Prefix, Chunk, ChunkExtra).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix' and also request an additional `Extra' bytes.
%%
%% For example, if the `Chunk' size is 1 KByte and `Extra' is 4K Bytes, then
%% the file offsets that follow `Chunk''s position for the following 4K will
%% be reserved by the file sequencer for later write(s) by the
%% `write_chunk()' API.

-spec append_chunk_extra(machi_dt:inet_host(), machi_dt:inet_port(),
               machi_dt:epoch_id(), machi_dt:file_prefix(), machi_dt:chunk(), machi_dt:chunk_size()) ->
      {ok, machi_dt:chunk_pos()} | {error, machi_dt:error_general()} | {error, term()}.
append_chunk_extra(Host, TcpPort, EpochID, Prefix, Chunk, ChunkExtra)
  when is_integer(ChunkExtra), ChunkExtra >= 0 ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        append_chunk2(Sock, EpochID, Prefix, Chunk, ChunkExtra)
    after
        disconnect(Sock)
    end.

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

-spec read_chunk(port_wrap(), machi_dt:epoch_id(), machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk_size()) ->
      {ok, machi_dt:chunk_s()} |
      {error, machi_dt:error_general() | 'not_written' | 'partial_read'} |
      {error, term()}.
read_chunk(Sock, EpochID, File, Offset, Size)
  when Offset >= ?MINIMUM_OFFSET, Size >= 0 ->
    read_chunk2(Sock, EpochID, File, Offset, Size).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

-spec read_chunk(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:epoch_id(),
                 machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk_size()) ->
      {ok, machi_dt:chunk_s()} |
      {error, machi_dt:error_general() | 'not_written' | 'partial_read'} |
      {error, term()}.
read_chunk(Host, TcpPort, EpochID, File, Offset, Size)
  when Offset >= ?MINIMUM_OFFSET, Size >= 0 ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        read_chunk2(Sock, EpochID, File, Offset, Size)
    after
        disconnect(Sock)
    end.

%% @doc Fetch the list of chunk checksums for `File'.

-spec checksum_list(port_wrap(), machi_dt:epoch_id(), machi_dt:file_name()) ->
      {ok, binary()} |
      {error, machi_dt:error_general() | 'no_such_file' | 'partial_read'} |
      {error, term()}.
checksum_list(Sock, EpochID, File) ->
    checksum_list2(Sock, EpochID, File).

%% @doc Fetch the list of chunk checksums for `File'.
%%
%% Why return a simple `binary()' type rather than
%% `[machi_dt:chunk_summary()]'?  The two reasons are:
%% <ol>
%% <li> Server overhead: the CPU required to chop up the implementation-
%%      specific store into zillions of very small terms is very high.
%% </li>
%% <li> Protocol encoding and decoding overhead: the cost is non-zero,
%%      and the sum of cost of encoding and decoding a zillion small terms
%%      is substantial.
%% </li>
%% </ol>
%%
%% For both reasons, the server's protocol response is absurdly simple
%% and very fast: send back a `binary()' blob to the client.  Then it
%% is the client's responsibility to spend the CPU time to parse the
%% blob.
%%
%% Details of the encoding used inside the `binary()' blog can be found
%% in the EDoc comments for {@link machi_flu1:decode_csum_file_entry/1}.

-spec checksum_list(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:epoch_id(), machi_dt:file_name()) ->
      {ok, binary()} |
      {error, machi_dt:error_general() | 'no_such_file'} | {error, term()}.
checksum_list(Host, TcpPort, EpochID, File) when is_integer(TcpPort) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        checksum_list2(Sock, EpochID, File)
    after
        disconnect(Sock)
    end.

%% @doc Fetch the list of all files on the remote FLU.

-spec list_files(port_wrap(), machi_dt:epoch_id()) ->
      {ok, [machi_dt:file_info()]} | {error, term()}.
list_files(Sock, EpochID) ->
    list2(Sock, EpochID).

%% @doc Fetch the list of all files on the remote FLU.

-spec list_files(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:epoch_id()) ->
      {ok, [machi_dt:file_info()]} | {error, term()}.
list_files(Host, TcpPort, EpochID) when is_integer(TcpPort) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        list2(Sock, EpochID)
    after
        disconnect(Sock)
    end.

%% @doc Fetch the wedge status from the remote FLU.

-spec wedge_status(port_wrap()) ->
      {ok, {boolean(), machi_dt:epoch_id()}} | {error, term()}.

wedge_status(Sock) ->
    wedge_status2(Sock).

%% @doc Fetch the wedge status from the remote FLU.

-spec wedge_status(machi_dt:inet_host(), machi_dt:inet_port()) ->
      {ok, {boolean(), machi_dt:epoch_id()}} | {error, term()}.
wedge_status(Host, TcpPort) when is_integer(TcpPort) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        wedge_status2(Sock)
    after
        disconnect(Sock)
    end.

%% @doc Get the latest epoch number + checksum from the FLU's projection store.

-spec get_latest_epochid(port_wrap(), machi_dt:projection_type()) ->
      {ok, machi_dt:epoch_id()} | {error, term()}.
get_latest_epochid(Sock, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    get_latest_epochid2(Sock, ProjType).

%% @doc Get the latest epoch number + checksum from the FLU's projection store.

-spec get_latest_epochid(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:projection_type()) ->
      {ok, machi_dt:epoch_id()} | {error, term()}.
get_latest_epochid(Host, TcpPort, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        get_latest_epochid2(Sock, ProjType)
    after
        disconnect(Sock)
    end.

%% @doc Get the latest projection from the FLU's projection store for `ProjType'

-spec read_latest_projection(port_wrap(), machi_dt:projection_type()) ->
      {ok, machi_dt:projection()} | {error, not_written} | {error, term()}.
read_latest_projection(Sock, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    read_latest_projection2(Sock, ProjType).

%% @doc Get the latest projection from the FLU's projection store for `ProjType'

-spec read_latest_projection(machi_dt:inet_host(), machi_dt:inet_port(),
                             machi_dt:projection_type()) ->
      {ok, machi_dt:projection()} | {error, not_written} | {error, term()}.
read_latest_projection(Host, TcpPort, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        read_latest_projection2(Sock, ProjType)
    after
        disconnect(Sock)
    end.

%% @doc Read a projection `Proj' of type `ProjType'.

-spec read_projection(port_wrap(), machi_dt:projection_type(), machi_dt:epoch_num()) ->
      {ok, machi_dt:projection()} | {error, not_written} | {error, term()}.
read_projection(Sock, ProjType, Epoch)
  when ProjType == 'public' orelse ProjType == 'private' ->
    read_projection2(Sock, ProjType, Epoch).

%% @doc Read a projection `Proj' of type `ProjType'.

-spec read_projection(machi_dt:inet_host(), machi_dt:inet_port(),
                      machi_dt:projection_type(), machi_dt:epoch_num()) ->
      {ok, machi_dt:projection()} | {error, not_written} | {error, term()}.
read_projection(Host, TcpPort, ProjType, Epoch)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        read_projection2(Sock, ProjType, Epoch)
    after
        disconnect(Sock)
    end.

%% @doc Write a projection `Proj' of type `ProjType'.

-spec write_projection(port_wrap(), machi_dt:projection_type(), machi_dt:projection()) ->
      'ok' | {error, 'written'} | {error, term()}.
write_projection(Sock, ProjType, Proj)
  when ProjType == 'public' orelse ProjType == 'private',
       is_record(Proj, projection_v1) ->
    write_projection2(Sock, ProjType, Proj).

%% @doc Write a projection `Proj' of type `ProjType'.

-spec write_projection(machi_dt:inet_host(), machi_dt:inet_port(),
                       machi_dt:projection_type(), machi_dt:projection()) ->
      'ok' | {error, 'written'} | {error, term()}.
write_projection(Host, TcpPort, ProjType, Proj)
  when ProjType == 'public' orelse ProjType == 'private',
       is_record(Proj, projection_v1) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        write_projection2(Sock, ProjType, Proj)
    after
        disconnect(Sock)
    end.

%% @doc Get all projections from the FLU's projection store.

-spec get_all_projections(port_wrap(), machi_dt:projection_type()) ->
      {ok, [machi_dt:projection()]} | {error, term()}.
get_all_projections(Sock, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    get_all_projections2(Sock, ProjType).

%% @doc Get all projections from the FLU's projection store.

-spec get_all_projections(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:projection_type()) ->
      {ok, [machi_dt:projection()]} | {error, term()}.
get_all_projections(Host, TcpPort, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        get_all_projections2(Sock, ProjType)
    after
        disconnect(Sock)
    end.

%% @doc Get all epoch numbers from the FLU's projection store.

-spec list_all_projections(port_wrap(), machi_dt:projection_type()) ->
      {ok, [non_neg_integer()]} | {error, term()}.
list_all_projections(Sock, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    list_all_projections2(Sock, ProjType).

%% @doc Get all epoch numbers from the FLU's projection store.

-spec list_all_projections(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:projection_type()) ->
      {ok, [non_neg_integer()]} | {error, term()}.
list_all_projections(Host, TcpPort, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        list_all_projections2(Sock, ProjType)
    after
        disconnect(Sock)
    end.

%% @doc Kick (politely) the remote chain manager to react to a
%% projection change.

-spec kick_projection_reaction(port_wrap(), list()) ->
      ok.
kick_projection_reaction(Sock, Options) ->
    kick_projection_reaction2(Sock, Options).

%% @doc Kick (politely) the remote chain manager to react to a
%% projection change.

-spec kick_projection_reaction(machi_dt:inet_host(), machi_dt:inet_port(), list()) ->
      ok.
kick_projection_reaction(Host, TcpPort, Options) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        kick_projection_reaction2(Sock, Options)
    after
        disconnect(Sock)
    end.

%% @doc Echo -- test protocol round-trip.

-spec echo(port_wrap(), string()) ->
      string() | {error, term()}.
echo(Sock, String) when is_list(String) ->
    echo2(Sock, String).

%% @doc Get all epoch numbers from the FLU's projection store.

-spec echo(machi_dt:inet_host(), machi_dt:inet_port(), string()) ->
      string() | {error, term()}.
echo(Host, TcpPort, String) when is_list(String) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        echo2(Sock, String)
    after
        disconnect(Sock)
    end.

%% @doc Quit &amp; close the connection to remote FLU.

-spec quit(port_wrap()) ->
      ok.
quit(Sock) ->
    catch (_ = w_send(Sock, <<"QUIT\n">>)),
    disconnect(Sock),
    ok.

connected_p({w,tcp,Sock}) ->
    case (catch inet:peername(Sock)) of
        {ok, _} -> true;
        _       -> false
    end;
connected_p(_) ->
    false.

connect(#p_srvr{}=P) ->
    w_connect(P).

disconnect({w,tcp,_Sock}=WS) ->
    w_close(WS),
    ok;
disconnect(_) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

-spec write_chunk(port_wrap(), machi_dt:epoch_id(), machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk()) ->
      ok | {error, machi_dt:error_general()} | {error, term()}.
write_chunk(Sock, EpochID, File, Offset, Chunk)
  when Offset >= ?MINIMUM_OFFSET ->
    write_chunk2(Sock, EpochID, File, Offset, Chunk).

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

-spec write_chunk(machi_dt:inet_host(), machi_dt:inet_port(),
                  machi_dt:epoch_id(), machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk()) ->
      ok | {error, machi_dt:error_general()} | {error, term()}.
write_chunk(Host, TcpPort, EpochID, File, Offset, Chunk)
  when Offset >= ?MINIMUM_OFFSET ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        write_chunk2(Sock, EpochID, File, Offset, Chunk)
    after
        disconnect(Sock)
    end.

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

-spec trim_chunk(port_wrap(), machi_dt:epoch_id(), machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk_size()) ->
      ok | {error, machi_dt:error_general()} | {error, term()}.
trim_chunk(Sock, EpochID, File0, Offset, Size)
  when Offset >= ?MINIMUM_OFFSET ->
    ReqID = <<"id">>,
    File = machi_util:make_binary(File0),
    true = (Offset >= ?MINIMUM_OFFSET),
    Req = machi_pb_translate:to_pb_request(
            ReqID,
            {low_trim_chunk, EpochID, File, Offset, Size, 0}),
    do_pb_request_common(Sock, ReqID, Req).

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

-spec trim_chunk(machi_dt:inet_host(), machi_dt:inet_port(),
                  machi_dt:epoch_id(), machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk_size()) ->
      ok | {error, machi_dt:error_general()} | {error, term()}.
trim_chunk(_Host, _TcpPort, _EpochID, _File, _Offset, _Size) ->
    not_used.


%% @doc Restricted API: Delete a file after it has been successfully
%% migrated.

-spec delete_migration(port_wrap(), machi_dt:epoch_id(), machi_dt:file_name()) ->
      ok | {error, machi_dt:error_general() | 'no_such_file'} | {error, term()}.
delete_migration(Sock, EpochID, File) ->
    delete_migration2(Sock, EpochID, File).

%% @doc Restricted API: Delete a file after it has been successfully
%% migrated.

-spec delete_migration(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:epoch_id(), machi_dt:file_name()) ->
      ok | {error, machi_dt:error_general() | 'no_such_file'} | {error, term()}.
delete_migration(Host, TcpPort, EpochID, File) when is_integer(TcpPort) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        delete_migration2(Sock, EpochID, File)
    after
        disconnect(Sock)
    end.

%% @doc Restricted API: Truncate a file after it has been successfully
%% erasure coded.

-spec trunc_hack(port_wrap(), machi_dt:epoch_id(), machi_dt:file_name()) ->
      ok | {error, machi_dt:error_general() | 'no_such_file'} | {error, term()}.
trunc_hack(Sock, EpochID, File) ->
    trunc_hack2(Sock, EpochID, File).

%% @doc Restricted API: Truncate a file after it has been successfully
%% erasure coded.

-spec trunc_hack(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:epoch_id(), machi_dt:file_name()) ->
      ok | {error, machi_dt:error_general() | 'no_such_file'} | {error, term()}.
trunc_hack(Host, TcpPort, EpochID, File) when is_integer(TcpPort) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        trunc_hack2(Sock, EpochID, File)
    after
        disconnect(Sock)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

read_chunk2(Sock, EpochID, File0, Offset, Size) ->
    ReqID = <<"id">>,
    File = machi_util:make_binary(File0),
    Req = machi_pb_translate:to_pb_request(
            ReqID,
            {low_read_chunk, EpochID, File, Offset, Size, []}),
    do_pb_request_common(Sock, ReqID, Req).

append_chunk2(Sock, EpochID, Prefix0, Chunk0, ChunkExtra) ->
    ReqID = <<"id">>,
    {Chunk, CSum_tag, CSum} =
        case Chunk0 of
            X when is_binary(X) ->
                {Chunk0, ?CSUM_TAG_NONE, <<>>};
            {ChunkCSum, Chk} ->
                {Tag, CS} = machi_util:unmake_tagged_csum(ChunkCSum),
                {Chk, Tag, CS}
        end,
    PKey = <<>>,                                % TODO
    Prefix = machi_util:make_binary(Prefix0),
    Req = machi_pb_translate:to_pb_request(
            ReqID,
            {low_append_chunk, EpochID, PKey, Prefix, Chunk, CSum_tag, CSum,
             ChunkExtra}),
    do_pb_request_common(Sock, ReqID, Req).

write_chunk2(Sock, EpochID, File0, Offset, Chunk0) ->
    ReqID = <<"id">>,
    File = machi_util:make_binary(File0),
    true = (Offset >= ?MINIMUM_OFFSET),
    {Chunk, CSum_tag, CSum} =
        case Chunk0 of
            X when is_binary(X) ->
                {Chunk0, ?CSUM_TAG_NONE, <<>>};
            {ChunkCSum, Chk} ->
                {Tag, CS} = machi_util:unmake_tagged_csum(ChunkCSum),
                {Chk, Tag, CS}
        end,
    Req = machi_pb_translate:to_pb_request(
            ReqID,
            {low_write_chunk, EpochID, File, Offset, Chunk, CSum_tag, CSum}),
    do_pb_request_common(Sock, ReqID, Req).

list2(Sock, EpochID) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_list_files, EpochID}),
    do_pb_request_common(Sock, ReqID, Req).

wedge_status2(Sock) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_wedge_status, undefined}),
    do_pb_request_common(Sock, ReqID, Req).

echo2(Sock, Message) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_echo, undefined, Message}),
    do_pb_request_common(Sock, ReqID, Req).

checksum_list2(Sock, EpochID, File) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_checksum_list, EpochID, File}),
    do_pb_request_common(Sock, ReqID, Req).

delete_migration2(Sock, EpochID, File) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_delete_migration, EpochID, File}),
    do_pb_request_common(Sock, ReqID, Req).

trunc_hack2(Sock, EpochID, File) ->
    ReqID = <<"id-trunc">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_trunc_hack, EpochID, File}),
    do_pb_request_common(Sock, ReqID, Req).

get_latest_epochid2(Sock, ProjType) ->
    ReqID = <<42>>,
    Req = machi_pb_translate:to_pb_request(
                ReqID, {low_proj, {get_latest_epochid, ProjType}}),
    do_pb_request_common(Sock, ReqID, Req).

read_latest_projection2(Sock, ProjType) ->
    ReqID = <<42>>,
    Req = machi_pb_translate:to_pb_request(
                ReqID, {low_proj, {read_latest_projection, ProjType}}),
    do_pb_request_common(Sock, ReqID, Req).

read_projection2(Sock, ProjType, Epoch) ->
    ReqID = <<42>>,
    Req = machi_pb_translate:to_pb_request(
                ReqID, {low_proj, {read_projection, ProjType, Epoch}}),
    do_pb_request_common(Sock, ReqID, Req).

write_projection2(Sock, ProjType, Proj) ->
    ReqID = <<42>>,
    Req = machi_pb_translate:to_pb_request(
                ReqID, {low_proj, {write_projection, ProjType, Proj}}),
    do_pb_request_common(Sock, ReqID, Req).

get_all_projections2(Sock, ProjType) ->
    ReqID = <<42>>,
    Req = machi_pb_translate:to_pb_request(
                ReqID, {low_proj, {get_all_projections, ProjType}}),
    do_pb_request_common(Sock, ReqID, Req).

list_all_projections2(Sock, ProjType) ->
    ReqID = <<42>>,
    Req = machi_pb_translate:to_pb_request(
                ReqID, {low_proj, {list_all_projections, ProjType}}),
    do_pb_request_common(Sock, ReqID, Req).

kick_projection_reaction2(Sock, _Options) ->
    ReqID = <<42>>,
    Req = machi_pb_translate:to_pb_request(
                ReqID, {low_proj, {kick_projection_reaction}}),
    do_pb_request_common(Sock, ReqID, Req, false).

do_pb_request_common(Sock, ReqID, Req) ->
    do_pb_request_common(Sock, ReqID, Req, true).

do_pb_request_common(Sock, ReqID, Req, GetReply_p) ->
    erase(bad_sock),
    try
        ReqBin = list_to_binary(machi_pb:encode_mpb_ll_request(Req)),
        ok = w_send(Sock, ReqBin),
        if GetReply_p ->
                case w_recv(Sock, 0) of
                    {ok, RespBin} ->
                        Resp = machi_pb:decode_mpb_ll_response(RespBin),
                        {ReqID2, Reply} = machi_pb_translate:from_pb_response(Resp),
                        true = (ReqID == ReqID2 orelse ReqID2 == <<>>),
                        Reply;
                    {error, _}=Err ->
                        throw(Err)
                end;
           not GetReply_p ->
                ok
        end
    catch
        throw:Error ->
            put(bad_sock, Sock),
            filter_sock_error_result(Error);
        error:{case_clause,_}=Noo ->
            put(bad_sock, Sock),
            {error, {badmatch, Noo, erlang:get_stacktrace()}};
        error:{badmatch,_}=BadMatch ->
            put(bad_sock, Sock),
            {error, {badmatch, BadMatch, erlang:get_stacktrace()}};
        error:Whoa ->
            put(bad_sock, Sock),
            %% TODO: The machi_chain_manager1_converge_demo:t() test can
            %%       create a large number of these errors when moving from
            %%       no partitions to many partitions:
            %%       Whoa undefined: function_clause
            %% In theory this is harmless, because the client will retry
            %% with a new socket.  But, fix it anyway.
            io:format(user, "DBG Whoa ~w: ~w at ~w ~P\n", [Sock, Whoa, time(), erlang:get_stacktrace(), 25]), timer:sleep(250),
            {error, {whoa, Whoa, erlang:get_stacktrace()}}
    end.

filter_sock_error_result({error, closed}) ->
    {error, partition};
filter_sock_error_result(Error) ->
    Error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

w_connect(#p_srvr{proto_mod=?MODULE, address=Host, port=Port, props=Props}=_P)->
    try
        case proplists:get_value(session_proto, Props, tcp) of
            tcp ->
put(xxx, goofus),
                Sock = machi_util:connect(Host, Port, ?HARD_TIMEOUT),
put(xxx, Sock),
                ok = inet:setopts(Sock, ?PB_PACKET_OPTS),
                {w,tcp,Sock};
            %% sctp ->
            %%     %% TODO: not implemented
            %%     {w,sctp,Sock}
            ssl ->
                %% TODO: veryveryuntested
                SslOptions = proplists:get_value(ssl_options, Props),
                Sock = machi_util:connect(Port, Port),
                {ok, SslSock} = ssl:connect(Sock, SslOptions),
                {w,ssl,SslSock}
        end
    catch
        _X:_Y ->
            %% io:format(user, "DBG Whoa ~w w_connect port ~w sock ~w err ~w ~w\n", [time(), Port, get(xxx), _X, _Y]),
            undefined
    end.

w_close({w,tcp,Sock}) ->
    catch gen_tcp:close(Sock),
    ok.

w_recv({w,tcp,Sock}, Amt) ->
    gen_tcp:recv(Sock, Amt, ?HARD_TIMEOUT).

w_send({w,tcp,Sock}, IoData) ->
    gen_tcp:send(Sock, IoData).

%% w_setopts({w,tcp,Sock}, Opts) ->
%%     inet:setopts(Sock, Opts).

