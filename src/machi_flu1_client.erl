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
%%
%% == Client API implementation notes ==
%%
%% At the moment, there are several modules that implement various
%% subsets of the Machi API. The table below attempts to show how and
%% why they differ.
%%
%% ```
%% |--------------------------+-------+-----+------+------+-------+----------------|
%% |                          | PB    |     | #    |      | Conn  | Epoch & NS     |
%% | Module name              | Level | CR? | FLUS | Impl | Life? | version aware? |
%% |--------------------------+-------+-----+------+------+-------+----------------|
%% | machi_pb_high_api_client | high  | yes | many | proc | long  | no             |
%% | machi_cr_client          | low   | yes | many | proc | long  | no             |
%% | machi_proxy_flu1_client  | low   | no  | 1    | proc | long  | yes            |
%% | machi_flu1_client        | low   | no  | 1    | lib  | short | yes            |
%% |--------------------------+-------+-----+------+------+-------+----------------|
%% '''
%%
%% In terms of use and API layering, the table rows are in highest`->'lowest
%% order: each level calls the layer immediately below it.
%%
%% <dl>
%% <dt> <b> PB Level</b> </dt>
%% <dd> The Protocol Buffers API is divided logically into two levels,
%% "low" and "high".  The low-level protocol is used for intra-chain
%% communication.  The high-level protocol is used for clients outside
%% of a Machi chain or Machi cluster of chains.
%% </dd>
%% <dt> <b> CR?</b> </dt>
%% <dd> Does this API support (directly or indirectly) Chain
%% Replication?  If `no', then the API has no awareness of multiple
%% replicas of any file or file chunk; unaware clients can only
%% perform operations at a single Machi FLU's file service or
%% projection store service.
%% </dd>
%% <dt> <b> # FLUs</b> </dt>
%% <dd> Now many FLUs does this API layer communicate with
%% simultaneously?  Note that there is a one-to-one correspondence
%% between this value and the "CR?" column's value.
%% </dd>
%% <dt> <b> Impl</b> </dt>
%% <dd> Implementation: library-only or an Erlang process,
%%      e.g., `gen_server'.
%% </dd>
%% <dt> <b> Conn Life?</b> </dt>
%% <dd> Expected TCP session connection life: short or long.  At the
%% lowest level, the {@link machi_flu1_client} API implementation takes
%% no effort to reconnect to a remote FLU when its single TCP session
%% is broken.  For long-lived connection life APIs, the server side will
%% automatically attempt to reconnect to remote FLUs when a TCP session
%% is broken.
%% </dd>
%% <dt> <b> Epoch &amp; NS version aware?</b> </dt>
%% <dd> Are clients of this API responsible for knowing a chain's EpochID
%% and namespace version numbers?  If `no', then the server side of the
%% API will automatically attempt to discover/re-discover the EpochID and
%% namespace version numbers whenever they change.
%% </dd>
%% </dl>
%%
%% The only protocol that we expect to be used by entities outside of
%% a single Machi chain or a multi-chain cluster is the "high"
%% Protocol Buffers API.  The {@link riak_pb_high_api_client} module
%% is an Erlang reference implementation of this PB API.

-module(machi_flu1_client).

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-include_lib("pulse_otp/include/pulse_otp.hrl").
-endif.

-define(SHORT_TIMEOUT, 2500).
-define(LONG_TIMEOUT, (60*1000)).

-export([
         %% File API
         append_chunk/6, append_chunk/7,
         append_chunk/8, append_chunk/9,
         read_chunk/7, read_chunk/8,
         checksum_list/2, checksum_list/3,
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
         write_chunk/7, write_chunk/8,
         trim_chunk/6,
         delete_migration/3, delete_migration/4,
         trunc_hack/3, trunc_hack/4
        ]).

-type port_wrap()   :: {w,atom(),term()}.

-spec append_chunk(port_wrap(),
                   'undefined' | machi_dt:ns_info(), machi_dt:epoch_id(),
                   machi_dt:file_prefix(), machi_dt:chunk(),
                   machi_dt:chunk_csum()) ->
      {ok, machi_dt:chunk_pos()} | {error, machi_dt:error_general()} | {error, term()}.
append_chunk(Sock, NSInfo, EpochID, Prefix, Chunk, CSum) ->
    append_chunk(Sock, NSInfo, EpochID, Prefix, Chunk, CSum,
                 #append_opts{}, ?LONG_TIMEOUT).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix' and also request an additional `Extra' bytes.
%%
%% For example, if the `Chunk' size is 1 KByte and `Extra' is 4K Bytes, then
%% the file offsets that follow `Chunk''s position for the following 4K will
%% be reserved by the file sequencer for later write(s) by the
%% `write_chunk()' API.

-spec append_chunk(machi_dt:inet_host(), machi_dt:inet_port(),
                   'undefined' | machi_dt:ns_info(), machi_dt:epoch_id(),
                   machi_dt:file_prefix(), machi_dt:chunk(),
                   machi_dt:chunk_csum()) ->
      {ok, machi_dt:chunk_pos()} | {error, machi_dt:error_general()} | {error, term()}.
append_chunk(Host, TcpPort, NSInfo, EpochID, Prefix, Chunk, CSum) ->
    append_chunk(Host, TcpPort, NSInfo, EpochID, Prefix, Chunk, CSum,
                 #append_opts{}, ?LONG_TIMEOUT).

-spec append_chunk(port_wrap(),
                   'undefined' | machi_dt:ns_info(), machi_dt:epoch_id(),
                   machi_dt:file_prefix(), machi_dt:chunk(),
                   machi_dt:chunk_csum(), machi_dt:append_opts(), timeout()) ->
      {ok, machi_dt:chunk_pos()} | {error, machi_dt:error_general()} | {error, term()}.
append_chunk(Sock, NSInfo0, EpochID, Prefix, Chunk, CSum, Opts, Timeout) ->
    NSInfo = machi_util:ns_info_default(NSInfo0),
    append_chunk2(Sock, NSInfo, EpochID, Prefix, Chunk, CSum, Opts, Timeout).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix' and also request an additional `Extra' bytes.
%%
%% For example, if the `Chunk' size is 1 KByte and `Extra' is 4K Bytes, then
%% the file offsets that follow `Chunk''s position for the following 4K will
%% be reserved by the file sequencer for later write(s) by the
%% `write_chunk()' API.

-spec append_chunk(machi_dt:inet_host(), machi_dt:inet_port(),
                   'undefined' | machi_dt:ns_info(), machi_dt:epoch_id(),
                   machi_dt:file_prefix(), machi_dt:chunk(),
                   machi_dt:chunk_csum(), machi_dt:append_opts(), timeout()) ->
      {ok, machi_dt:chunk_pos()} | {error, machi_dt:error_general()} | {error, term()}.
append_chunk(Host, TcpPort, NSInfo0, EpochID,
             Prefix, Chunk, CSum, Opts, Timeout) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        NSInfo = machi_util:ns_info_default(NSInfo0),
        append_chunk2(Sock, NSInfo, EpochID,
                      Prefix, Chunk, CSum, Opts, Timeout)
    after
        disconnect(Sock)
    end.

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

-spec read_chunk(port_wrap(), 'undefined' | machi_dt:ns_info(), machi_dt:epoch_id(), machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk_size(),
                 machi_dt:read_opts_x()) ->
      {ok, {[machi_dt:chunk_summary()], [machi_dt:chunk_pos()]}} |
      {error, machi_dt:error_general() | 'not_written' | 'partial_read'} |
      {error, term()}.
read_chunk(Sock, NSInfo0, EpochID, File, Offset, Size, Opts0)
  when Offset >= ?MINIMUM_OFFSET, Size >= 0 ->
    NSInfo = machi_util:ns_info_default(NSInfo0),
    Opts = machi_util:read_opts_default(Opts0),
    read_chunk2(Sock, NSInfo, EpochID, File, Offset, Size, Opts).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

-spec read_chunk(machi_dt:inet_host(), machi_dt:inet_port(), 'undefined' | machi_dt:ns_info(), machi_dt:epoch_id(),
                 machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk_size(),
                 machi_dt:read_opts_x()) ->
      {ok, [machi_dt:chunk_summary()]} |
      {error, machi_dt:error_general() | 'not_written' | 'partial_read'} |
      {error, term()}.
read_chunk(Host, TcpPort, NSInfo0, EpochID, File, Offset, Size, Opts0)
  when Offset >= ?MINIMUM_OFFSET, Size >= 0 ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    NSInfo = machi_util:ns_info_default(NSInfo0),
    Opts = machi_util:read_opts_default(Opts0),
    try
        read_chunk2(Sock, NSInfo, EpochID, File, Offset, Size, Opts)
    after
        disconnect(Sock)
    end.

%% @doc Fetch the list of chunk checksums for `File'.

-spec checksum_list(port_wrap(), machi_dt:file_name()) ->
      {ok, binary()} |
      {error, machi_dt:error_general() | 'no_such_file' | 'partial_read'} |
      {error, term()}.
checksum_list(Sock, File) ->
    checksum_list2(Sock, File).

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

-spec checksum_list(machi_dt:inet_host(), machi_dt:inet_port(), machi_dt:file_name()) ->
      {ok, binary()} |
      {error, machi_dt:error_general() | 'no_such_file'} | {error, term()}.
checksum_list(Host, TcpPort, File) when is_integer(TcpPort) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        checksum_list2(Sock, File)
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
      {ok, {boolean(), machi_dt:epoch_id(), machi_dt:namespace_version(),machi_dt:namespace()}} | {error, term()}.

wedge_status(Sock) ->
    wedge_status2(Sock).

%% @doc Fetch the wedge status from the remote FLU.

-spec wedge_status(machi_dt:inet_host(), machi_dt:inet_port()) ->
      {ok, {boolean(), machi_dt:epoch_id(), machi_dt:namespace_version(),machi_dt:namespace()}} | {error, term()}.
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

-spec write_chunk(port_wrap(), 'undefined' | machi_dt:ns_info(), machi_dt:epoch_id(), machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk(), machi_dt:chunk_csum()) ->
      ok | {error, machi_dt:error_general()} | {error, term()}.
write_chunk(Sock, NSInfo0, EpochID, File, Offset, Chunk, CSum)
  when Offset >= ?MINIMUM_OFFSET ->
    NSInfo = machi_util:ns_info_default(NSInfo0),
    write_chunk2(Sock, NSInfo, EpochID, File, Offset, Chunk, CSum).

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

-spec write_chunk(machi_dt:inet_host(), machi_dt:inet_port(),
                  'undefined' | machi_dt:ns_info(), machi_dt:epoch_id(), machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk(), machi_dt:chunk_csum()) ->
      ok | {error, machi_dt:error_general()} | {error, term()}.
write_chunk(Host, TcpPort, NSInfo0, EpochID, File, Offset, Chunk, CSum)
  when Offset >= ?MINIMUM_OFFSET ->
    Sock = connect(#p_srvr{proto_mod=?MODULE, address=Host, port=TcpPort}),
    try
        NSInfo = machi_util:ns_info_default(NSInfo0),
        write_chunk2(Sock, NSInfo, EpochID, File, Offset, Chunk, CSum)
    after
        disconnect(Sock)
    end.

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

-spec trim_chunk(port_wrap(), 'undefined' | machi_dt:ns_info(), machi_dt:epoch_id(), machi_dt:file_name(), machi_dt:file_offset(), machi_dt:chunk_size()) ->
      ok | {error, machi_dt:error_general()} | {error, term()}.
trim_chunk(Sock, NSInfo0, EpochID, File0, Offset, Size)
  when Offset >= ?MINIMUM_OFFSET ->
    ReqID = <<"id">>,
    NSInfo = machi_util:ns_info_default(NSInfo0),
    #ns_info{version=NSVersion, name=NS} = NSInfo,
    File = machi_util:make_binary(File0),
    true = (Offset >= ?MINIMUM_OFFSET),
    Req = machi_pb_translate:to_pb_request(
            ReqID,
            {low_trim_chunk, NSVersion, NS, EpochID, File, Offset, Size, 0}),
    do_pb_request_common(Sock, ReqID, Req).

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

read_chunk2(Sock, NSInfo, EpochID, File0, Offset, Size, Opts) ->
    ReqID = <<"id">>,
    #ns_info{version=NSVersion, name=NS} = NSInfo,
    File = machi_util:make_binary(File0),
    Req = machi_pb_translate:to_pb_request(
            ReqID,
            {low_read_chunk, NSVersion, NS, EpochID, File, Offset, Size, Opts}),
    do_pb_request_common(Sock, ReqID, Req).

append_chunk2(Sock, NSInfo, EpochID,
              Prefix0, Chunk, CSum0, Opts, Timeout) ->
    ReqID = <<"id">>,
    Prefix = machi_util:make_binary(Prefix0),
    {CSum_tag, CSum} = case CSum0 of
                           <<>> ->
                               {?CSUM_TAG_NONE, <<>>};
                           {_Tag, _CS} ->
                               CSum0;
                           B when is_binary(B) ->
                               machi_util:unmake_tagged_csum(CSum0)
                       end,
    #ns_info{version=NSVersion, name=NS, locator=NSLocator} = NSInfo,
    %% NOTE: The tuple position of NSLocator is a bit odd, because EpochID
    %%       _must_ be in the 4th position (as NSV & NS must be in 2nd & 3rd).
    Req = machi_pb_translate:to_pb_request(
            ReqID,
            {low_append_chunk, NSVersion, NS, EpochID, NSLocator,
             Prefix, Chunk, CSum_tag, CSum, Opts}),
    do_pb_request_common(Sock, ReqID, Req, true, Timeout).

write_chunk2(Sock, NSInfo, EpochID, File0, Offset, Chunk, CSum0) ->
    ReqID = <<"id">>,
    #ns_info{version=NSVersion, name=NS} = NSInfo,
    File = machi_util:make_binary(File0),
    true = (Offset >= ?MINIMUM_OFFSET),
    {CSum_tag, CSum} = case CSum0 of
                           <<>> ->
                               {?CSUM_TAG_NONE, <<>>};
                           {_Tag, _CS} ->
                               CSum0;
                           B when is_binary(B) ->
                               machi_util:unmake_tagged_csum(CSum0)
                       end,
    Req = machi_pb_translate:to_pb_request(
            ReqID,
            {low_write_chunk, NSVersion, NS, EpochID, File, Offset, Chunk, CSum_tag, CSum}),
    do_pb_request_common(Sock, ReqID, Req).

list2(Sock, EpochID) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_skip_wedge, {low_list_files, EpochID}}),
    do_pb_request_common(Sock, ReqID, Req).

wedge_status2(Sock) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_skip_wedge, {low_wedge_status}}),
    do_pb_request_common(Sock, ReqID, Req).

echo2(Sock, Message) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_skip_wedge, {low_echo, Message}}),
    do_pb_request_common(Sock, ReqID, Req).

checksum_list2(Sock, File) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_skip_wedge, {low_checksum_list, File}}),
    do_pb_request_common(Sock, ReqID, Req).

delete_migration2(Sock, EpochID, File) ->
    ReqID = <<"id">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_skip_wedge, {low_delete_migration, EpochID, File}}),
    do_pb_request_common(Sock, ReqID, Req).

trunc_hack2(Sock, EpochID, File) ->
    ReqID = <<"id-trunc">>,
    Req = machi_pb_translate:to_pb_request(
            ReqID, {low_skip_wedge, {low_trunc_hack, EpochID, File}}),
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
    do_pb_request_common(Sock, ReqID, Req, false, ?LONG_TIMEOUT).

do_pb_request_common(Sock, ReqID, Req) ->
    do_pb_request_common(Sock, ReqID, Req, true, ?LONG_TIMEOUT).

do_pb_request_common(Sock, ReqID, Req, GetReply_p, Timeout) ->
    erase(bad_sock),
    try
        ReqBin = list_to_binary(machi_pb:encode_mpb_ll_request(Req)),
        ok = w_send(Sock, ReqBin),
        if GetReply_p ->
                case w_recv(Sock, 0, Timeout) of
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
                Sock = machi_util:connect(Host, Port, ?SHORT_TIMEOUT),
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

w_recv({w,tcp,Sock}, Amt, Timeout) ->
    gen_tcp:recv(Sock, Amt, Timeout).

w_send({w,tcp,Sock}, IoData) ->
    gen_tcp:send(Sock, IoData).

%% w_setopts({w,tcp,Sock}, Opts) ->
%%     inet:setopts(Sock, Opts).

