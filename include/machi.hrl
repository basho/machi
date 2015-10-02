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

-define(MAX_FILE_SIZE, 256*1024*1024).     % 256 MBytes
-define(MAX_CHUNK_SIZE, ((1 bsl 32) - 1)).
%% -define(DATA_DIR, "/Volumes/SAM1/seq-tests/data").
-define(DATA_DIR, "./data").
-define(MINIMUM_OFFSET, 1024).
-define(FN_DELIMITER, "^").

%% 0th draft of checksum typing with 1st byte.
-define(CSUM_TAG_NONE, 0).                   % No csum provided by client
-define(CSUM_TAG_CLIENT_SHA, 1).             % Client-generated SHA1
-define(CSUM_TAG_SERVER_SHA, 2).             % Server-genereated SHA1
-define(CSUM_TAG_SERVER_REGEN_SHA, 3).       % Server-regenerated SHA1

%% Protocol Buffers goop
-define(PB_MAX_MSG_SIZE, (33*1024*1024)).
-define(PB_PACKET_OPTS, [{packet, 4}, {packet_size, ?PB_MAX_MSG_SIZE}]).
