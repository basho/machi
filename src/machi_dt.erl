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

-module(machi_dt).

-include("machi_projection.hrl").

-type chunk()       :: chunk_bin() | {chunk_csum(), chunk_bin()}.
-type chunk_bin()   :: binary() | iolist().    % client can use either
-type chunk_csum()  :: binary().               % 1 byte tag, N-1 bytes checksum
-type chunk_summary() :: {file_offset(), chunk_size(), binary()}.
-type chunk_s()     :: binary().               % server always uses binary()
-type chunk_pos()   :: {file_offset(), chunk_size(), file_name_s()}.
-type chunk_size()  :: non_neg_integer().
-type error_general() :: 'bad_arg' | 'wedged' | 'bad_checksum'.
-type epoch_csum()  :: binary().
-type epoch_num()   :: -1 | non_neg_integer().
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

-export_type([
              chunk/0,
              chunk_bin/0,
              chunk_csum/0,
              chunk_summary/0,
              chunk_s/0,
              chunk_pos/0,
              chunk_size/0,
              error_general/0,
              epoch_csum/0,
              epoch_num/0,
              epoch_id/0,
              file_info/0,
              file_name/0,
              file_name_s/0,
              file_offset/0,
              file_size/0,
              file_prefix/0,
              inet_host/0,
              inet_port/0,
              projection/0,
              projection_type/0
             ]).

