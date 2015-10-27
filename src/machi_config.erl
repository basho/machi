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

%% @doc Configuration consulting utilities. Some conventions:
%% - The function name should match with exact configuration
%%   name in `app.config' or `advanced.config' of `machi' section.
%% - The default value of that configuration is expected to be in
%%   cuttlefish schema file. Otherwise some macro in headers may
%%   be chosen.
%% - Documentation of the configuration is supposed to be written
%%   in cuttlefish schema file, rather than @doc section of the function.
%% - spec of the function should be written.
%% - Returning `undefined' is strongly discouraged. Return some default
%%   value instead.
%% - `application:get_env/3' is recommended. See `max_file_size/0' for
%%   example.

-module(machi_config).

-include("machi.hrl").

-export([max_file_size/0]).

-spec max_file_size() -> pos_integer().
max_file_size() ->
    application:get_env(machi, max_file_size, ?DEFAULT_MAX_FILE_SIZE).
