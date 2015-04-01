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

-module(machi_admin_util).

-export([
         verify_file_checksums_remote/2, verify_file_checksums_remote/3
        ]).
-compile(export_all).

-include("machi.hrl").

-define(FLU_C, machi_flu1_client).

verify_file_checksums_remote(Sock1, File) when is_port(Sock1) ->
    verify_file_checksums_remote2(Sock1, File).

verify_file_checksums_remote(Host, TcpPort, File) ->
    Sock1 = machi_util:connect(Host, TcpPort),
    verify_file_checksums_remote2(Sock1, File).

%%%%%%%%%%%%%%%%%%%%%%%%%%%

verify_file_checksums_remote2(Sock1, File) ->
    try
        {ok, Info} = ?FLU_C:checksum_list(Sock1, File),
        Res = lists:foldl(verify_chunk_checksum(Sock1, File), [], Info),
        {ok, Res}
    catch
        What:Why ->
            {error, {What, Why, erlang:get_stacktrace()}}
    end.

verify_chunk_checksum(Sock1, File) ->
    fun({Offset, Size, CSum}, Acc) ->
            case ?FLU_C:read_chunk(Sock1, File, Offset, Size) of
                {ok, Chunk} ->
                    CSum2 = machi_util:checksum(Chunk),
                    if CSum == CSum2 ->
                            Acc;
                       true ->
                            [{Offset, Size, File, CSum, now, CSum2}|Acc]
                    end;
                _Else ->
                    [{Offset, Size, File, CSum, now, read_failure}|Acc]
            end
    end.
