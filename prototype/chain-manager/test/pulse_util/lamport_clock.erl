%% -------------------------------------------------------------------
%%
%% Machi: a small village of replicated files
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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
-module(lamport_clock).

-export([init/0, reset/0, get/0, update/1, incr/0]).

-define(KEY, ?MODULE).

-ifdef(TEST).

init() ->
    case get(?KEY) of
        undefined ->
            reset();
        N when is_integer(N) ->
            ok
    end.

reset() ->
    FakeTOD = 0,
    put(?KEY, FakeTOD + 1).

get() ->
    init(),
    get(?KEY).

update(Remote) ->
    New = erlang:max(get(?KEY), Remote) + 1,
    put(?KEY, New),
    New.        

incr() ->
    New = get(?KEY) + 1,
    put(?KEY, New),
    New.

-else. % TEST

init() ->
    ok.

reset() ->
    ok.

get() ->
    ok.

update(_) ->
    ok.

incr() ->
    ok.

-endif. % TEST
