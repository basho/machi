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

-module(machi_flu_psup_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

smoke_test() ->
    [os:cmd("rm -rf " ++ X) || X <- ["./data.a", "./data.b", "/data.c"] ],
    {ok, SupPid} = machi_flu_sup:start_link(),
    try
        {ok, _} = machi_flu_psup:start_flu_package(a, 5555, "./data.a",
                                                   [{active_mode,false}]),
        {ok, _} = machi_flu_psup:start_flu_package(b, 5556, "./data.b",
                                                   [{active_mode,false}]),
        {ok, _} = machi_flu_psup:start_flu_package(c, 5557, "./data.c",
                                                   [{active_mode,false}]),

        [begin
             _QQ = machi_chain_manager1:test_react_to_env(a_chmgr),
             ok
         end || _ <- lists:seq(1,5)],
        ok
    after
        exit(SupPid, normal),
        machi_util:wait_for_death(SupPid, 100),
        ok
    end.

-endif. % TEST

        
    
