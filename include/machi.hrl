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

%% @doc Now 4GiBytes, could be up to 64bit due to PB message limit of
%% chunk size
-define(DEFAULT_MAX_FILE_SIZE, ((1 bsl 32) - 1)).
-define(MINIMUM_OFFSET, 1024).

%% 0th draft of checksum typing with 1st byte.
-define(CSUM_TAG_NONE, 0).                   % No csum provided by client
-define(CSUM_TAG_CLIENT_SHA, 1).             % Client-generated SHA1
-define(CSUM_TAG_SERVER_SHA, 2).             % Server-genereated SHA1
-define(CSUM_TAG_SERVER_REGEN_SHA, 3).       % Server-regenerated SHA1

-define(CSUM_TAG_NONE_ATOM, none).
-define(CSUM_TAG_CLIENT_SHA_ATOM, client_sha).
-define(CSUM_TAG_SERVER_SHA_ATOM, server_sha).
-define(CSUM_TAG_SERVER_REGEN_SHA_ATOM, server_regen_sha).

%% Protocol Buffers goop
-define(PB_MAX_MSG_SIZE, (33*1024*1024)).
-define(PB_PACKET_OPTS, [{packet, 4}, {packet_size, ?PB_MAX_MSG_SIZE}]).

%% TODO: it's used in flu_sup and elsewhere, change this to suitable name
-define(TEST_ETS_TABLE, test_ets_table).

-define(DEFAULT_COC_NAMESPACE, "").
-define(DEFAULT_COC_LOCATOR,    0).

-record(ns_info, {
          version = 0  :: machi_dt:namespace_version(),
          name = <<>>  :: machi_dt:namespace(),
          locator = 0  :: machi_dt:locator()
         }).

-record(append_opts, {
          chunk_extra = 0             :: machi_dt:chunk_size(),
          preferred_file_name         :: 'undefined' | machi_dt:file_name_s(),
          flag_fail_preferred = false :: boolean()
         }).

-record(read_opts, {
          no_checksum = false         :: boolean(),
          no_chunk = false            :: boolean(),
          needs_trimmed = false       :: boolean()
         }).
