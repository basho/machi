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

-module(machi_flu1_client).

-include("machi.hrl").
-include("machi_projection.hrl").

-export([
         %% File API
         append_chunk/4, append_chunk/5,
         read_chunk/5, read_chunk/6,
         checksum_list/3, checksum_list/4,
         list_files/2, list_files/3,

         %% Projection API
         get_latest_epoch/2, get_latest_epoch/3,
         read_projection/3, read_projection/4,
         write_projection/3, write_projection/4,

         %% Common API
         quit/1
        ]).
%% For "internal" replication only.
-export([
         write_chunk/5, write_chunk/6,
         delete_migration/3, delete_migration/4,
         trunc_hack/3, trunc_hack/4
        ]).

-type chunk()       :: binary() | iolist().    % client can use either
-type chunk_csum()  :: {file_offset(), chunk_size(), binary()}.
-type chunk_s()     :: binary().               % server always uses binary()
-type chunk_pos()   :: {file_offset(), chunk_size(), file_name_s()}.
-type chunk_size()  :: non_neg_integer().
-type epoch_csum()  :: binary().
-type epoch_num()   :: non_neg_integer().
-type epoch_id()    :: {epoch_num(), epoch_csum()}.
-type file_info()   :: {file_size(), file_name_s()}.
-type file_name()   :: binary() | list().
-type file_name_s() :: binary().                % server reply
-type file_offset() :: non_neg_integer().
-type file_size()   :: non_neg_integer().
-type file_prefix() :: binary() | list().
-type inet_host()   :: inet:ip_address() | inet:hostname().
-type inet_port()   :: inet:port_number().
-type projection()      :: #projection_v1{}.
-type projection_type() :: 'public' | 'private'.

-export_type([epoch_id/0]).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

-spec append_chunk(port(), epoch_id(), file_prefix(), chunk()) ->
      {ok, chunk_pos()} | {error, term()}.
append_chunk(Sock, EpochID, Prefix, Chunk) ->
    append_chunk2(Sock, EpochID, Prefix, Chunk).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

-spec append_chunk(inet_host(), inet_port(),
                   epoch_id(), file_prefix(), chunk()) ->
      {ok, chunk_pos()} | {error, term()}.
append_chunk(Host, TcpPort, EpochID, Prefix, Chunk) ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        append_chunk2(Sock, EpochID, Prefix, Chunk)
    after
        catch gen_tcp:close(Sock)
    end.

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

-spec read_chunk(port(), epoch_id(), file_name(), file_offset(), chunk_size()) ->
      {ok, chunk_s()} | {error, term()}.
read_chunk(Sock, EpochID, File, Offset, Size)
  when Offset >= ?MINIMUM_OFFSET, Size >= 0 ->
    read_chunk2(Sock, EpochID, File, Offset, Size).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

-spec read_chunk(inet_host(), inet_port(), epoch_id(),
                 file_name(), file_offset(), chunk_size()) ->
      {ok, chunk_s()} | {error, term()}.
read_chunk(Host, TcpPort, EpochID, File, Offset, Size)
  when Offset >= ?MINIMUM_OFFSET, Size >= 0 ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        read_chunk2(Sock, EpochID, File, Offset, Size)
    after
        catch gen_tcp:close(Sock)
    end.

%% @doc Fetch the list of chunk checksums for `File'.

-spec checksum_list(port(), epoch_id(), file_name()) ->
      {ok, [chunk_csum()]} | {error, term()}.
checksum_list(Sock, EpochID, File) when is_port(Sock) ->
    checksum_list2(Sock, EpochID, File).

%% @doc Fetch the list of chunk checksums for `File'.

-spec checksum_list(inet_host(), inet_port(), epoch_id(), file_name()) ->
      {ok, [chunk_csum()]} | {error, term()}.
checksum_list(Host, TcpPort, EpochID, File) when is_integer(TcpPort) ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        checksum_list2(Sock, EpochID, File)
    after
        catch gen_tcp:close(Sock)
    end.

%% @doc Fetch the list of all files on the remote FLU.

-spec list_files(port(), epoch_id()) ->
      {ok, [file_info()]} | {error, term()}.
list_files(Sock, EpochID) when is_port(Sock) ->
    list2(Sock, EpochID).

%% @doc Fetch the list of all files on the remote FLU.

-spec list_files(inet_host(), inet_port(), epoch_id()) ->
      {ok, [file_info()]} | {error, term()}.
list_files(Host, TcpPort, EpochID) when is_integer(TcpPort) ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        list2(Sock, EpochID)
    after
        catch gen_tcp:close(Sock)
    end.

%% @doc Get the latest epoch number from the FLU's projection store.

-spec get_latest_epoch(port(), projection_type()) ->
      {ok, -1|non_neg_integer()} | {error, term()}.
get_latest_epoch(Sock, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    get_latest_epoch2(Sock, ProjType).

%% @doc Get the latest epoch number from the FLU's projection store.

-spec get_latest_epoch(inet_host(), inet_port(),
                       projection_type()) ->
      {ok, -1|non_neg_integer()} | {error, term()}.
get_latest_epoch(Host, TcpPort, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        get_latest_epoch2(Sock, ProjType)
    after
        catch gen_tcp:close(Sock)
    end.

%% @doc Read a projection `Proj' of type `ProjType'.

-spec read_projection(port(), projection_type(), epoch_num()) ->
      'ok' | {error, written} | {error, term()}.
read_projection(Sock, ProjType, Epoch)
  when ProjType == 'public' orelse ProjType == 'private' ->
    read_projection2(Sock, ProjType, Epoch).

%% @doc Read a projection `Proj' of type `ProjType'.

-spec read_projection(inet_host(), inet_port(),
                       projection_type(), epoch_num()) ->
      'ok' | {error, written} | {error, term()}.
read_projection(Host, TcpPort, ProjType, Epoch)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        read_projection2(Sock, ProjType, Epoch)
    after
        catch gen_tcp:close(Sock)
    end.

%% @doc Write a projection `Proj' of type `ProjType'.

-spec write_projection(port(), projection_type(), projection()) ->
      'ok' | {error, written} | {error, term()}.
write_projection(Sock, ProjType, Proj)
  when ProjType == 'public' orelse ProjType == 'private',
       is_record(Proj, projection_v1) ->
    write_projection2(Sock, ProjType, Proj).

%% @doc Write a projection `Proj' of type `ProjType'.

-spec write_projection(inet_host(), inet_port(),
                       projection_type(), projection()) ->
      'ok' | {error, written} | {error, term()}.
write_projection(Host, TcpPort, ProjType, Proj)
  when ProjType == 'public' orelse ProjType == 'private',
       is_record(Proj, projection_v1) ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        write_projection2(Sock, ProjType, Proj)
    after
        catch gen_tcp:close(Sock)
    end.

%% @doc Quit &amp; close the connection to remote FLU.

-spec quit(port()) ->
      ok.
quit(Sock) when is_port(Sock) ->
    catch (_ = gen_tcp:send(Sock, <<"QUIT\n">>)),
    catch gen_tcp:close(Sock),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

-spec write_chunk(port(), epoch_id(), file_name(), file_offset(), chunk()) ->
      ok | {error, term()}.
write_chunk(Sock, EpochID, File, Offset, Chunk)
  when Offset >= ?MINIMUM_OFFSET ->
    write_chunk2(Sock, EpochID, File, Offset, Chunk).

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

-spec write_chunk(inet_host(), inet_port(),
                  epoch_id(), file_name(), file_offset(), chunk()) ->
      ok | {error, term()}.
write_chunk(Host, TcpPort, EpochID, File, Offset, Chunk)
  when Offset >= ?MINIMUM_OFFSET ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        write_chunk2(Sock, EpochID, File, Offset, Chunk)
    after
        catch gen_tcp:close(Sock)
    end.

%% @doc Restricted API: Delete a file after it has been successfully
%% migrated.

-spec delete_migration(port(), epoch_id(), file_name()) ->
      ok | {error, term()}.
delete_migration(Sock, EpochID, File) when is_port(Sock) ->
    delete_migration2(Sock, EpochID, File).

%% @doc Restricted API: Delete a file after it has been successfully
%% migrated.

-spec delete_migration(inet_host(), inet_port(), epoch_id(), file_name()) ->
      ok | {error, term()}.
delete_migration(Host, TcpPort, EpochID, File) when is_integer(TcpPort) ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        delete_migration2(Sock, EpochID, File)
    after
        catch gen_tcp:close(Sock)
    end.

%% @doc Restricted API: Truncate a file after it has been successfully
%% erasure coded.

-spec trunc_hack(port(), epoch_id(), file_name()) ->
      ok | {error, term()}.
trunc_hack(Sock, EpochID, File) when is_port(Sock) ->
    trunc_hack2(Sock, EpochID, File).

%% @doc Restricted API: Truncate a file after it has been successfully
%% erasure coded.

-spec trunc_hack(inet_host(), inet_port(), epoch_id(), file_name()) ->
      ok | {error, term()}.
trunc_hack(Host, TcpPort, EpochID, File) when is_integer(TcpPort) ->
    Sock = machi_util:connect(Host, TcpPort),
    try
        trunc_hack2(Sock, EpochID, File)
    after
        catch gen_tcp:close(Sock)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

append_chunk2(Sock, EpochID, Prefix0, Chunk0) ->
    try
        %% TODO: add client-side checksum to the server's protocol
        %% _ = crypto:hash(md5, Chunk),
        Prefix = machi_util:make_binary(Prefix0),
        Chunk = machi_util:make_binary(Chunk0),
        Len = iolist_size(Chunk0),
        true = (Len =< ?MAX_CHUNK_SIZE),
        {EpochNum, EpochCSum} = EpochID,
        EpochIDRaw = <<EpochNum:(4*8)/big, EpochCSum/binary>>,
        LenHex = machi_util:int_to_hexbin(Len, 32),
        Cmd = [<<"A ">>, EpochIDRaw, LenHex, Prefix, 10],
        ok = gen_tcp:send(Sock, [Cmd, Chunk]),
        {ok, Line} = gen_tcp:recv(Sock, 0),
        PathLen = byte_size(Line) - 3 - 16 - 1 - 1,
        case Line of
            <<"OK ", OffsetHex:16/binary, " ",
              Path:PathLen/binary, _:1/binary>> ->
                Offset = machi_util:hexstr_to_int(OffsetHex),
                {ok, {Offset, Len, Path}};
            <<"ERROR BAD-ARG", _/binary>> ->
                {error, bad_arg};
            <<"ERROR ", Rest/binary>> ->
                {error, Rest}
        end
    catch
        throw:Error ->
            Error;
        error:{badmatch,_}=BadMatch ->
            {error, {badmatch, BadMatch, erlang:get_stacktrace()}}
    end.

read_chunk2(Sock, EpochID, File0, Offset, Size) ->
    {EpochNum, EpochCSum} = EpochID,
    EpochIDRaw = <<EpochNum:(4*8)/big, EpochCSum/binary>>,
    File = machi_util:make_binary(File0),
    PrefixHex = machi_util:int_to_hexbin(Offset, 64),
    SizeHex = machi_util:int_to_hexbin(Size, 32),
    CmdLF = [$R, 32, EpochIDRaw, PrefixHex, SizeHex, File, 10],
    ok = gen_tcp:send(Sock, CmdLF),
    case gen_tcp:recv(Sock, 3) of
        {ok, <<"OK\n">>} ->
            {ok, _Chunk}=Res = gen_tcp:recv(Sock, Size),
            Res;
        {ok, Else} ->
            {ok, OldOpts} = inet:getopts(Sock, [packet]),
            ok = inet:setopts(Sock, [{packet, line}]),
            {ok, Else2} = gen_tcp:recv(Sock, 0),
            ok = inet:setopts(Sock, OldOpts),
            case Else of
                <<"ERA">> ->
                    {error, todo_erasure_coded}; %% escript_cc_parse_ec_info(Sock, Line, Else2);
                <<"ERR">> ->
                    case Else2 of
                        <<"OR BAD-IO\n">> ->
                            {error, no_such_file};
                        <<"OR NOT-ERASURE\n">> ->
                            {error, no_such_file};
                        <<"OR BAD-ARG\n">> ->
                            {error, bad_arg};
                        <<"OR PARTIAL-READ\n">> ->
                            {error, partial_read};
                        _ ->
                            {error, Else2}
                    end;
                _ ->
                    {error, {whaaa, <<Else/binary, Else2/binary>>}}
            end
    end.

list2(Sock, EpochID) ->
    try
        {EpochNum, EpochCSum} = EpochID,
        EpochIDRaw = <<EpochNum:(4*8)/big, EpochCSum/binary>>,
        ok = gen_tcp:send(Sock, [<<"L ">>, EpochIDRaw, <<"\n">>]),
        ok = inet:setopts(Sock, [{packet, line}]),
        {ok, <<"OK\n">>} = gen_tcp:recv(Sock, 0),
        Res = list3(gen_tcp:recv(Sock, 0), Sock),
        ok = inet:setopts(Sock, [{packet, raw}]),
        {ok, Res}
    catch
        throw:Error ->
            Error;
        error:{badmatch,_}=BadMatch ->
            {error, {badmatch, BadMatch}}
    end.

list3({ok, <<".\n">>}, _Sock) ->
    [];
list3({ok, Line}, Sock) ->
    FileLen = byte_size(Line) - 16 - 1 - 1,
    <<SizeHex:16/binary, " ", File:FileLen/binary, _/binary>> = Line,
    Size = machi_util:hexstr_to_int(SizeHex),
    [{Size, File}|list3(gen_tcp:recv(Sock, 0), Sock)];
list3(Else, _Sock) ->
    throw({server_protocol_error, Else}).

checksum_list2(Sock, EpochID, File) ->
    try
        {EpochNum, EpochCSum} = EpochID,
        EpochIDRaw = <<EpochNum:(4*8)/big, EpochCSum/binary>>,
        ok = gen_tcp:send(Sock, [<<"C ">>, EpochIDRaw, File, <<"\n">>]),
        ok = inet:setopts(Sock, [{packet, line}]),
        case gen_tcp:recv(Sock, 0) of
            {ok, <<"OK ", Rest/binary>> = Line} ->
                put(status, ok),                    % may be unset later
                RestLen = byte_size(Rest) - 1,
                <<LenHex:RestLen/binary, _:1/binary>> = Rest,
                <<Len:64/big>> = machi_util:hexstr_to_bin(LenHex),
                ok = inet:setopts(Sock, [{packet, raw}]),
                {ok, checksum_list_finish(checksum_list_fast(Sock, Len))};
            {ok, <<"ERROR NO-SUCH-FILE", _/binary>>} ->
                {error, no_such_file};
            {ok, <<"ERROR BAD-ARG", _/binary>>} ->
                {error, bad_arg};
            {ok, Else} ->
                throw({server_protocol_error, Else})
        end
    catch
        throw:Error ->
            Error;
        error:{badmatch,_}=BadMatch ->
            {error, {badmatch, BadMatch}}
    end.

checksum_list_fast(Sock, 0) ->
    {ok, <<".\n">> = _Line} = gen_tcp:recv(Sock, 2),
    [];
checksum_list_fast(Sock, Remaining) ->
    Num = erlang:min(Remaining, 1024*1024),
    {ok, Bytes} = gen_tcp:recv(Sock, Num),
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
    try
        {EpochNum, EpochCSum} = EpochID,
        EpochIDRaw = <<EpochNum:(4*8)/big, EpochCSum/binary>>,
        %% TODO: add client-side checksum to the server's protocol
        %% _ = crypto:hash(md5, Chunk),
        File = machi_util:make_binary(File0),
        true = (Offset >= ?MINIMUM_OFFSET),
        OffsetHex = machi_util:int_to_hexbin(Offset, 64),
        Chunk = machi_util:make_binary(Chunk0),
        Len = iolist_size(Chunk0),
        true = (Len =< ?MAX_CHUNK_SIZE),
        LenHex = machi_util:int_to_hexbin(Len, 32),
        Cmd = [<<"W-repl ">>, EpochIDRaw, OffsetHex,
               LenHex, File, <<"\n">>],
        ok = gen_tcp:send(Sock, [Cmd, Chunk]),
        {ok, Line} = gen_tcp:recv(Sock, 0),
        PathLen = byte_size(Line) - 3 - 16 - 1 - 1,
        case Line of
            <<"OK\n">> ->
                ok;
            <<"ERROR BAD-ARG", _/binary>> ->
                {error, bad_arg};
            <<"ERROR ", _/binary>>=Else ->
                {error, {server_said, Else}}
        end
    catch
        throw:Error ->
            Error;
        error:{badmatch,_}=BadMatch ->
            {error, {badmatch, BadMatch, erlang:get_stacktrace()}}
    end.

delete_migration2(Sock, EpochID, File) ->
    try
        {EpochNum, EpochCSum} = EpochID,
        EpochIDRaw = <<EpochNum:(4*8)/big, EpochCSum/binary>>,
        Cmd = [<<"DEL-migration ">>, EpochIDRaw, File, <<"\n">>],
        ok = gen_tcp:send(Sock, Cmd),
        ok = inet:setopts(Sock, [{packet, line}]),
        case gen_tcp:recv(Sock, 0) of
            {ok, <<"OK\n">>} ->
                ok;
            {ok, <<"ERROR NO-SUCH-FILE", _/binary>>} ->
                {error, no_such_file};
            {ok, <<"ERROR BAD-ARG", _/binary>>} ->
                {error, bad_arg};
            {ok, Else} ->
                throw({server_protocol_error, Else})
        end
    catch
        throw:Error ->
            Error;
        error:{badmatch,_}=BadMatch ->
            {error, {badmatch, BadMatch}}
    end.

trunc_hack2(Sock, EpochID, File) ->
    try
        {EpochNum, EpochCSum} = EpochID,
        EpochIDRaw = <<EpochNum:(4*8)/big, EpochCSum/binary>>,
        Cmd = [<<"TRUNC-hack--- ">>, EpochIDRaw, File, <<"\n">>],
        ok = gen_tcp:send(Sock, Cmd),
        ok = inet:setopts(Sock, [{packet, line}]),
        case gen_tcp:recv(Sock, 0) of
            {ok, <<"OK\n">>} ->
                ok;
            {ok, <<"ERROR NO-SUCH-FILE", _/binary>>} ->
                {error, no_such_file};
            {ok, <<"ERROR BAD-ARG", _/binary>>} ->
                {error, bad_arg};
            {ok, Else} ->
                throw({server_protocol_error, Else})
        end
    catch
        throw:Error ->
            Error;
        error:{badmatch,_}=BadMatch ->
            {error, {badmatch, BadMatch}}
    end.

get_latest_epoch2(Sock, ProjType) ->
    ProjCmd = {get_latest_epoch, ProjType},
    do_projection_common(Sock, ProjCmd).

read_projection2(Sock, ProjType, Epoch) ->
    ProjCmd = {read_projection, ProjType, Epoch},
    do_projection_common(Sock, ProjCmd).

write_projection2(Sock, ProjType, Proj) ->
    ProjCmd = {write_projection, ProjType, Proj},
    do_projection_common(Sock, ProjCmd).

do_projection_common(Sock, ProjCmd) ->
    try
        ProjCmdBin = term_to_binary(ProjCmd),
        Len = iolist_size(ProjCmdBin),
        true = (Len =< ?MAX_CHUNK_SIZE),
        LenHex = machi_util:int_to_hexbin(Len, 32),
        Cmd = [<<"PROJ ">>, LenHex, <<"\n">>],
        ok = gen_tcp:send(Sock, [Cmd, ProjCmdBin]),
        ok = inet:setopts(Sock, [{packet, line}]),
        {ok, Line} = gen_tcp:recv(Sock, 0),
        case Line of
            <<"OK ", ResLenHex:8/binary, "\n">> ->
                ResLen = machi_util:hexstr_to_int(ResLenHex),
                ok = inet:setopts(Sock, [{packet, raw}]),
                {ok, ResBin} = gen_tcp:recv(Sock, ResLen),
                ok = inet:setopts(Sock, [{packet, line}]),
                binary_to_term(ResBin);
            Else ->
                {error, Else}
        end
    catch
        throw:Error ->
            Error;
        error:{badmatch,_}=BadMatch ->
            {error, {badmatch, BadMatch, erlang:get_stacktrace()}}
    end.
