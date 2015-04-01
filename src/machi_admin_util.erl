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

verify_file_checksums_remote(Sock, File) ->
    verify_file_checksums_remote2(Sock, Sock, File).

verify_file_checksums_remote(_Host, _TcpPort, File) ->
    verify_file_checksums_remote2(todo, todo, File).

%%%%%%%%%%%%%%%%%%%%%%%%%%%

verify_file_checksums_remote2(Sock, Sock, File) ->
    todo.
