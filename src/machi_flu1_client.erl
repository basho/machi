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
%% TODO This EDoc was written first, and the EDoc and also `-type' and
%% `-spec' definitions for {@link machi_proxy_flu1_client} and {@link
%% machi_cr_client} must be improved.
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

-module(machi_flu1_client).

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").

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

         %% Common API
         echo/2, echo/3,
         quit/1,

         %% Connection management API
         connected_p/1, connect/1, disconnect/1
        ]).
%% For "internal" replication only.
-export([
         write_chunk/5, write_chunk/6,
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
      {ok, [machi_dt:chunk_summary()]} |
      {error, machi_dt:error_general() | 'no_such_file' | 'partial_read'} |
      {error, term()}.
checksum_list(Sock, EpochID, File) ->
    checksum_list2(Sock, EpochID, File).

%% @doc Fetch the list of chunk checksums for `File'.

-spec checksum_list(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:epoch_id(), machi_dt:file_name()) ->
      {ok, [machi_dt:chunk_summary()]} |
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

%% @doc Echo -- test protocol round-trip.

-spec echo(port_wrap(), string()) ->
      string() | {error, term()}.
echo(Sock, String) when is_list(String) ->
    echo2(Sock, String).

%% @doc Get all epoch numbers from the FLU's projection store.

-spec echo(machi_dt:inet_host(), machi_dt:inet_port(), string()) ->
      {ok, [non_neg_integer()]} | {error, term()}.
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
    erase(bad_sock),
    try
        {EpochNum, EpochCSum} = EpochID,
        EpochIDHex = machi_util:bin_to_hexstr(
                       <<EpochNum:(4*8)/big, EpochCSum/binary>>),
        File = machi_util:make_binary(File0),
        PrefixHex = machi_util:int_to_hexbin(Offset, 64),
        SizeHex = machi_util:int_to_hexbin(Size, 32),
        CmdLF = [$R, 32, EpochIDHex, PrefixHex, SizeHex, File, 10],
        ok = w_send(Sock, CmdLF),
        ok = w_setopts(Sock, [{packet, raw}]),
        case w_recv(Sock, 3) of
            {ok, <<"OK\n">>} ->
                {ok, _Chunk}=Res = w_recv(Sock, Size),
                Res;
            {ok, Else} ->
                ok = w_setopts(Sock, [{packet, line}]),
                {ok, Else2} = w_recv(Sock, 0),
                case Else of
                    <<"ERA">> ->
                        {error, todo_erasure_coded}; %% escript_cc_parse_ec_info(Sock, Line, Else2);
                    <<"ERR">> ->
                        case Else2 of
                            <<"OR NO-SUCH-FILE\n">> ->
                                {error, not_written};
                            <<"OR NOT-ERASURE\n">> ->
                                %% {error, no_such_file};
                                %% Ignore the fact that the file doesn't exist.
                                {error, not_written};
                            <<"OR BAD-ARG\n">> ->
                                {error, bad_arg};
                            <<"OR PARTIAL-READ\n">> ->
                                {error, partial_read};
                            <<"OR WEDGED", _/binary>> ->
                                {error, wedged};
                            _ ->
                                {error, Else2}
                        end;
                    _ ->
                        {error, {whaaa_todo, <<Else/binary, Else2/binary>>}}
                end
        end
    catch
        throw:Error ->
            put(bad_sock, Sock),
            Error;
        error:{case_clause,_}=Noo ->
            put(bad_sock, Sock),
            {error, {badmatch, Noo, erlang:get_stacktrace()}};
        error:{badmatch,_}=BadMatch ->
            put(bad_sock, Sock),
            {error, {badmatch, BadMatch, erlang:get_stacktrace()}}
    end.

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

checksum_list_fast(Sock, 0) ->
    {ok, <<".\n">> = _Line} = w_recv(Sock, 2),
    [];
checksum_list_fast(Sock, Remaining) ->
    Num = erlang:min(Remaining, 1024*1024),
    {ok, Bytes} = w_recv(Sock, Num),
    [Bytes|checksum_list_fast(Sock, Remaining - byte_size(Bytes))].

checksum_list_finish(Chunks) ->
    Bin = case Chunks of
              [X] ->
                  X;
              _ ->
                  iolist_to_binary(Chunks)
          end,
    [begin
         CSumLen = byte_size(Line) - 16 - 1 - 8 - 1,
         <<OffsetHex:16/binary, " ", SizeHex:8/binary, " ",
           CSum:CSumLen/binary>> = Line,
         {machi_util:hexstr_to_int(OffsetHex),
          machi_util:hexstr_to_int(SizeHex),
          machi_util:hexstr_to_bin(CSum)}
     end || Line <- re:split(Bin, "\n", [{return, binary}]),
            Line /= <<>>].

write_chunk2(Sock, EpochID, File0, Offset, Chunk0) ->
    erase(bad_sock),
    try
        {EpochNum, EpochCSum} = EpochID,
        EpochIDHex = machi_util:bin_to_hexstr(
                       <<EpochNum:(4*8)/big, EpochCSum/binary>>),
        %% TODO: add client-side checksum to the server's protocol
        %% _ = machi_util:checksum_chunk(Chunk),
        File = machi_util:make_binary(File0),
        true = (Offset >= ?MINIMUM_OFFSET),
        OffsetHex = machi_util:int_to_hexbin(Offset, 64),
        {CSum, Chunk} = case Chunk0 of
                            {_,_} ->
                                Chunk0;
                            XX when is_binary(XX) ->
                                SHA = machi_util:checksum_chunk(Chunk0),
                                {<<?CSUM_TAG_CLIENT_SHA:8, SHA/binary>>, Chunk0}
                        end,
        CSumHex = machi_util:bin_to_hexstr(CSum),
        Len = iolist_size(Chunk),
        true = (Len =< ?MAX_CHUNK_SIZE),
        LenHex = machi_util:int_to_hexbin(Len, 32),
        Cmd = [<<"W-repl ">>, EpochIDHex, CSumHex, OffsetHex,
               LenHex, File, <<"\n">>],
        ok = w_send(Sock, [Cmd, Chunk]),
        {ok, Line} = w_recv(Sock, 0),
        PathLen = byte_size(Line) - 3 - 16 - 1 - 1,
        case Line of
            <<"OK\n">> ->
                ok;
            <<"ERROR BAD-ARG", _/binary>> ->
                {error, bad_arg};
            <<"ERROR WEDGED", _/binary>> ->
                {error, wedged};
            <<"ERROR BAD-CHECKSUM", _/binary>> ->
                {error, bad_checksum};
            <<"ERROR ", _/binary>>=Else ->
                {error, {server_said, Else}}
        end
    catch
        throw:Error ->
            put(bad_sock, Sock),
            Error;
        error:{badmatch,_}=BadMatch ->
            put(bad_sock, Sock),
            {error, {badmatch, BadMatch, erlang:get_stacktrace()}}
    end.

delete_migration2(Sock, EpochID, File) ->
    erase(bad_sock),
    try
        {EpochNum, EpochCSum} = EpochID,
        EpochIDHex = machi_util:bin_to_hexstr(
                       <<EpochNum:(4*8)/big, EpochCSum/binary>>),
        Cmd = [<<"DEL-migration ">>, EpochIDHex, File, <<"\n">>],
        ok = w_send(Sock, Cmd),
        ok = w_setopts(Sock, [{packet, line}]),
        case w_recv(Sock, 0) of
            {ok, <<"OK\n">>} ->
                ok;
            {ok, <<"ERROR NO-SUCH-FILE", _/binary>>} ->
                {error, no_such_file};
            {ok, <<"ERROR BAD-ARG", _/binary>>} ->
                {error, bad_arg};
            {ok, <<"ERROR WEDGED", _/binary>>} ->
                {error, wedged};
            {ok, Else} ->
                throw({server_protocol_error, Else})
        end
    catch
        throw:Error ->
            put(bad_sock, Sock),
            Error;
        error:{case_clause,_}=Noo ->
            put(bad_sock, Sock),
            {error, {badmatch, Noo, erlang:get_stacktrace()}};
        error:{badmatch,_}=BadMatch ->
            put(bad_sock, Sock),
            {error, {badmatch, BadMatch}}
    end.

trunc_hack2(Sock, EpochID, File) ->
    erase(bad_sock),
    try
        {EpochNum, EpochCSum} = EpochID,
        EpochIDHex = machi_util:bin_to_hexstr(
                       <<EpochNum:(4*8)/big, EpochCSum/binary>>),
        Cmd = [<<"TRUNC-hack--- ">>, EpochIDHex, File, <<"\n">>],
        ok = w_send(Sock, Cmd),
        ok = w_setopts(Sock, [{packet, line}]),
        case w_recv(Sock, 0) of
            {ok, <<"OK\n">>} ->
                ok;
            {ok, <<"ERROR NO-SUCH-FILE", _/binary>>} ->
                {error, no_such_file};
            {ok, <<"ERROR BAD-ARG", _/binary>>} ->
                {error, bad_arg};
            {ok, <<"ERROR WEDGED", _/binary>>} ->
                {error, wedged};
            {ok, Else} ->
                throw({server_protocol_error, Else})
        end
    catch
        throw:Error ->
            put(bad_sock, Sock),
            Error;
        error:{case_clause,_}=Noo ->
            put(bad_sock, Sock),
            {error, {badmatch, Noo, erlang:get_stacktrace()}};
        error:{badmatch,_}=BadMatch ->
            put(bad_sock, Sock),
            {error, {badmatch, BadMatch}}
    end.

get_latest_epochid2(Sock, ProjType) ->
    ReqID = <<42>>,
    Req = machi_pb_wrap:make_projection_req(
                ReqID, {get_latest_epochid, ProjType}),
    do_pb_request_common(Sock, ReqID, Req).

read_latest_projection2(Sock, ProjType) ->
    ReqID = <<42>>,
    Req = machi_pb_wrap:make_projection_req(
                ReqID, {read_latest_projection, ProjType}),
    do_pb_request_common(Sock, ReqID, Req).

read_projection2(Sock, ProjType, Epoch) ->
    ReqID = <<42>>,
    Req = machi_pb_wrap:make_projection_req(
                ReqID, {read_projection, ProjType, Epoch}),
    do_pb_request_common(Sock, ReqID, Req).

write_projection2(Sock, ProjType, Proj) ->
    ReqID = <<42>>,
    Req = machi_pb_wrap:make_projection_req(
                ReqID, {write_projection, ProjType, Proj}),
    do_pb_request_common(Sock, ReqID, Req).

get_all_projections2(Sock, ProjType) ->
    ReqID = <<42>>,
    Req = machi_pb_wrap:make_projection_req(
                ReqID, {get_all_projections, ProjType}),
    do_pb_request_common(Sock, ReqID, Req).

list_all_projections2(Sock, ProjType) ->
    ReqID = <<42>>,
    Req = machi_pb_wrap:make_projection_req(
                ReqID, {list_all_projections, ProjType}),
    do_pb_request_common(Sock, ReqID, Req).

do_pb_request_common(Sock, ReqID, Req) ->
    erase(bad_sock),
    try
        ReqBin = list_to_binary(machi_pb:encode_mpb_ll_request(Req)),
io:format(user, "\nCCC Req ~p\n", [Req]),
        ok = w_send(Sock, ReqBin),
        case w_recv(Sock, 0) of
            {ok, RespBin} ->
                Resp = machi_pb:decode_mpb_ll_response(RespBin),
                io:format(user, "\nCCC Resp ~p\n", [Resp]),
                {ReqID2, Reply} = machi_pb_translate:from_pb_response(Resp),
                io:format(user, "\nCCC ReqID2 ~p Reply ~p\n", [ReqID2, Reply]),
                true = (ReqID == ReqID2 orelse ReqID2 == <<>>),
                Reply;
            Else ->
                {error, Else}
        end
    catch
        throw:Error ->
            put(bad_sock, Sock),
            Error;
        error:{case_clause,_}=Noo ->
            put(bad_sock, Sock),
            {error, {badmatch, Noo, erlang:get_stacktrace()}};
        error:{badmatch,_}=BadMatch ->
            put(bad_sock, Sock),
            {error, {badmatch, BadMatch, erlang:get_stacktrace()}}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

w_connect(#p_srvr{proto_mod=?MODULE, address=Host, port=Port, props=Props})->
    try
        case proplists:get_value(session_proto, Props, tcp) of
            tcp ->
                Sock = machi_util:connect(Host, Port, ?HARD_TIMEOUT),
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
        _:_ ->
            undefined
    end.

w_close({w,tcp,Sock}) ->
    catch gen_tcp:close(Sock),
    ok.

w_recv({w,tcp,Sock}, Amt) ->
    gen_tcp:recv(Sock, Amt, ?HARD_TIMEOUT).

w_send({w,tcp,Sock}, IoData) ->
    gen_tcp:send(Sock, IoData).

w_setopts({w,tcp,Sock}, Opts) ->
    inet:setopts(Sock, Opts).

