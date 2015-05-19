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

-module(machi_cr_client_test).

-behaviour(gen_server).

-include("machi.hrl").
-include("machi_projection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


smoke_test() ->
    os:cmd("rm -rf ./data.a ./data.b ./data.c"),
    {ok, SupPid} = machi_flu_sup:start_link(),
    try
        Prefix = <<"pre">>,
        Chunk1 = <<"yochunk">>,
        Host = "localhost",
        PortBase = 4444,
        {ok,_}=machi_flu_psup:start_flu_package(a, PortBase+0, "./data.a", []),
        {ok,_}=machi_flu_psup:start_flu_package(b, PortBase+1, "./data.b", []),
        {ok,_}=machi_flu_psup:start_flu_package(c, PortBase+2, "./data.c", []),
        D = orddict:from_list(
              [{a,{p_srvr,a,machi_flu1_client,"localhost",PortBase+0,[]}},
               {b,{p_srvr,b,machi_flu1_client,"localhost",PortBase+1,[]}},
               {c,{p_srvr,c,machi_flu1_client,"localhost",PortBase+2,[]}}]),
        ok = machi_chain_manager1:set_chain_members(a_chmgr, D),
        ok = machi_chain_manager1:set_chain_members(b_chmgr, D),
        ok = machi_chain_manager1:set_chain_members(c_chmgr, D),
        machi_projection_store:read_latest_projection(a_pstore, private),

        {ok, C1} = machi_cr_client:start_link([P || {_,P}<-orddict:to_list(D)]),
        machi_cr_client:append_chunk(C1, Prefix, Chunk1),
        %% {machi_flu_psup:stop_flu_package(c), timer:sleep(50)},
        {ok, {Off1,Size1,File1}} =
            machi_cr_client:append_chunk(C1, Prefix, Chunk1),
        {ok, Chunk1} = machi_cr_client:read_chunk(C1, File1, Off1, Size1),
        {ok, EpochID} = machi_flu1_client:get_latest_epochid(Host, PortBase+0,
                                                             private),
        %% Verify that the client's CR wrote to all of them.
        [{ok, Chunk1} = machi_flu1_client:read_chunk(
                          Host, PortBase+X, EpochID, File1, Off1, Size1) ||
            X <- [0,1,2] ],

        %% Manually write to head, then verify that read-repair fixes all.
        FooOff1 = Off1 + (1024*1024),
        [{error, not_written} = machi_flu1_client:read_chunk(
                                  Host, PortBase+X, EpochID,
                                  File1, FooOff1, Size1) || X <- [0,1,2] ],
        ok = machi_flu1_client:write_chunk(Host, PortBase+0, EpochID,
                                           File1, FooOff1, Chunk1),
        {ok, Chunk1} = machi_cr_client:read_chunk(C1, File1, FooOff1, Size1),
        [{X,{ok, Chunk1}} = {X,machi_flu1_client:read_chunk(
                                 Host, PortBase+X, EpochID,
                                 File1, FooOff1, Size1)} || X <- [0,1,2] ],

        ok
    after
        catch application:stop(machi),
        exit(SupPid, normal)
    end.

-endif. % TEST.
