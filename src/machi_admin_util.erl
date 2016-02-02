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

%% @doc Machi chain replication administration utilities.

-module(machi_admin_util).

-export([
         verify_file_checksums_local/3,  verify_file_checksums_local/4,
         verify_file_checksums_remote/3, verify_file_checksums_remote/4
        ]).
-compile(export_all).

-include("machi.hrl").
-include("machi_projection.hrl").

-define(FLU_C, machi_flu1_client).

-spec verify_file_checksums_local(port(), machi_dt:epoch_id(), binary()|list()) ->
      {ok, [tuple()]} | {error, term()}.
verify_file_checksums_local(Sock1, EpochID, Path) when is_port(Sock1) ->
    verify_file_checksums_local2(Sock1, EpochID, Path).

-spec verify_file_checksums_local(machi_dt:inet_host(), machi_dt:inet_port(),
                                  machi_dt:epoch_id(), binary()|list()) ->
      {ok, [tuple()]} | {error, term()}.
verify_file_checksums_local(Host, TcpPort, EpochID, Path) ->
    Sock1 = ?FLU_C:connect(#p_srvr{address=Host, port=TcpPort}),
    try
        verify_file_checksums_local2(Sock1, EpochID, Path)
    after
        catch ?FLU_C:disconnect(Sock1)
    end.

-spec verify_file_checksums_remote(port(), machi_dt:epoch_id(), binary()|list()) ->
      {ok, [tuple()]} | {error, term()}.
verify_file_checksums_remote(Sock1, EpochID, File) when is_port(Sock1) ->
    verify_file_checksums_remote2(Sock1, EpochID, File).

-spec verify_file_checksums_remote(machi_dt:inet_host(), machi_dt:inet_port(),
                                   machi_dt:epoch_id(), binary()|list()) ->
      {ok, [tuple()]} | {error, term()}.
verify_file_checksums_remote(Host, TcpPort, EpochID, File) ->
    Sock1 = ?FLU_C:connect(#p_srvr{address=Host, port=TcpPort}),
    try
        verify_file_checksums_remote2(Sock1, EpochID, File)
    after
        catch ?FLU_C:disconnect(Sock1)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

verify_file_checksums_local2(Sock1, EpochID, Path0) ->
    Path = machi_util:make_string(Path0),
    case file:open(Path, [read, binary, raw]) of
        {ok, FH} ->
            File = re:replace(Path, ".*/", "", [{return, binary}]),
            try
                ReadChunk = fun(F, Offset, Size) ->
                                    case file:pread(FH, Offset, Size) of
                                        {ok, Bin} ->
                                            {ok, {[{F, Offset, Bin, undefined}], []}};
                                        Err ->
                                            Err
                                    end
                            end,
                verify_file_checksums_common(Sock1, EpochID, File, ReadChunk)
            after
                file:close(FH)
            end;
        Else ->
            Else
    end.

verify_file_checksums_remote2(Sock1, EpochID, File) ->
    NSInfo = undefined,
    ReadChunk = fun(File_name, Offset, Size) ->
                        ?FLU_C:read_chunk(Sock1, NSInfo, EpochID,
                                          File_name, Offset, Size, undefined)
                end,
    verify_file_checksums_common(Sock1, EpochID, File, ReadChunk).

verify_file_checksums_common(Sock1, _EpochID, File, ReadChunk) ->
    try
        case ?FLU_C:checksum_list(Sock1, File) of
            {ok, InfoBin} ->
                Info = machi_csum_table:split_checksum_list_blob_decode(InfoBin),
                Res = lists:foldl(verify_chunk_checksum(File, ReadChunk),
                                  [], Info),
                {ok, Res};
            {error, no_such_file}=Nope ->
                Nope;
            {error, _}=Else ->
                Else
        end
    catch
        What:Why ->
            {error, {What, Why, erlang:get_stacktrace()}}
    end.

verify_chunk_checksum(File, ReadChunk) ->
    fun({0, ?MINIMUM_OFFSET, none}, []) ->
            [];
       ({Offset, Size, <<_Tag:1/binary, CSum/binary>>}, Acc) ->
            case ReadChunk(File, Offset, Size) of
                {ok, {[{_, Offset, Chunk, _}], _}} ->
                    CSum2 = machi_util:checksum_chunk(Chunk),
                    if CSum == CSum2 ->
                            Acc;
                       true ->
                            [{Offset, Size, File, CSum, now, CSum2}|Acc]
                    end;
                _Else ->
                    [{Offset, Size, File, CSum, now, read_failure}|Acc]
            end
    end.
