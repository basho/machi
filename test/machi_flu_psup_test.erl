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

%% @doc The Machi write-once projection store service.
%%
%% This API is gen_server-style message passing, intended for use
%% within a single Erlang node to glue together the projection store
%% server with the node-local process that implements Machi's TCP
%% client access protocol (on the "server side" of the TCP connection).
%%
%% All Machi client access to the projection store SHOULD NOT use this
%% module's API.
%%
%% The projection store is implemented by an Erlang/OTP `gen_server'
%% process that is associated with each FLU.  Conceptually, the
%% projection store is an array of write-once registers.  For each
%% projection store register, the key is a 2-tuple of an epoch number
%% (`non_neg_integer()' type) and a projection type (`public' or
%% `private' type); the value is a projection data structure
%% (`projection_v1()' type).

-module(machi_flu_psup_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

smoke_test() ->
    {ok, PidA} = machi_flu_sup:start_link(),
    try
        {ok, _} = machi_flu_psup:start_flu_package(a, 5555, "./data.a",
                                                   [{active_mode,false}]),
        {ok, _} = machi_flu_psup:start_flu_package(b, 5556, "./data.b",
                                                   [{active_mode,false}]),
        {ok, _} = machi_flu_psup:start_flu_package(c, 5557, "./data.c",
                                                   [{active_mode,false}]),
        ok
    after
        [ok = machi_flu_psup:stop_flu_package(X) || X <- [a,b,c]]
    end.

-endif. % TEST

        
    
