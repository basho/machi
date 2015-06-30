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

-include("machi.hrl").
-include("machi_projection.hrl").

-ifdef(TEST).
-ifndef(PULSE).

-include_lib("eunit/include/eunit.hrl").

smoke_test_() -> {timeout, 1*60, fun() -> smoke_test2() end}.

smoke_test2() ->
    os:cmd("rm -rf ./data.a ./data.b ./data.c"),
    {ok, SupPid} = machi_flu_sup:start_link(),
    error_logger:tty(false),
    try
        Prefix = <<"pre">>,
        Chunk1 = <<"yochunk">>,
        Host = "localhost",
        PortBase = 64444,
        Os = [{ignore_stability_time, true}, {active_mode, false}],
        {ok,_}=machi_flu_psup:start_flu_package(a, PortBase+0, "./data.a", Os),
        {ok,_}=machi_flu_psup:start_flu_package(b, PortBase+1, "./data.b", Os),
        {ok,_}=machi_flu_psup:start_flu_package(c, PortBase+2, "./data.c", Os),
        D = orddict:from_list(
              [{a,{p_srvr,a,machi_flu1_client,"localhost",PortBase+0,[]}},
               {b,{p_srvr,b,machi_flu1_client,"localhost",PortBase+1,[]}},
               {c,{p_srvr,c,machi_flu1_client,"localhost",PortBase+2,[]}}]),
        %% Force the chain to repair & fully assemble as quickly as possible.
        %% 1. Use set_chain_members() on all 3
        %% 2. Force a to run repair in a tight loop
        %% 3. Stop as soon as we see UPI=[a,b,c] and also author=c.
        %%    Otherwise, we can have a race with later, minor
        %%    projection changes which will change our understanding of
        %%    the epoch id. (C is the author with highest weight.)
        %% 4. Wait until all others are using epoch id from #3.
        %%
        %% Damn, this is a pain to make 100% deterministic, bleh.
        ok = machi_chain_manager1:set_chain_members(a_chmgr, D),
        ok = machi_chain_manager1:set_chain_members(b_chmgr, D),
        ok = machi_chain_manager1:set_chain_members(c_chmgr, D),
        TickAll = fun() -> [begin
                                Pid ! tick_check_environment,
                                timer:sleep(50)
                            end || Pid <- [a_chmgr,b_chmgr,c_chmgr] ]
                  end,
        _ = lists:foldl(
              fun(_, [{c,[a,b,c]}]=Acc) -> Acc;
                 (_, _Acc)  ->
                      TickAll(),                % has some sleep time inside
                      Xs = [begin
                                {ok, Prj} = machi_projection_store:read_latest_projection(PStore, private),
                                {Prj#projection_v1.author_server,
                                 Prj#projection_v1.upi}
                            end || PStore <- [a_pstore,b_pstore,c_pstore] ],
                      lists:usort(Xs)
              end, undefined, lists:seq(1,10000)),
        %% Everyone is settled on the same damn epoch id.
        {ok, EpochID} = machi_flu1_client:get_latest_epochid(Host, PortBase+0,
                                                             private),
        {ok, EpochID} = machi_flu1_client:get_latest_epochid(Host, PortBase+1,
                                                             private),
        {ok, EpochID} = machi_flu1_client:get_latest_epochid(Host, PortBase+2,
                                                             private),

        %% Whew ... ok, now start some damn tests.
        {ok, C1} = machi_cr_client:start_link([P || {_,P}<-orddict:to_list(D)]),
        machi_cr_client:append_chunk(C1, Prefix, Chunk1),
        {ok, {Off1,Size1,File1}} =
            machi_cr_client:append_chunk(C1, Prefix, Chunk1),
        Chunk1_badcs = {<<?CSUM_TAG_CLIENT_SHA:8, 0:(8*20)>>, Chunk1},
        {error, bad_checksum} =
            machi_cr_client:append_chunk(C1, Prefix, Chunk1_badcs),
        {ok, Chunk1} = machi_cr_client:read_chunk(C1, File1, Off1, Size1),
        {ok, PPP} = machi_flu1_client:read_latest_projection(Host, PortBase+0,
                                                             private),
        %% Verify that the client's CR wrote to all of them.
        [{ok, Chunk1} = machi_flu1_client:read_chunk(
                          Host, PortBase+X, EpochID, File1, Off1, Size1) ||
            X <- [0,1,2] ],

        %% Test read repair: Manually write to head, then verify that
        %% read-repair fixes all.
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

        %% Test read repair: Manually write to middle, then same checking.
        FooOff2 = Off1 + (2*1024*1024),
        Chunk2 = <<"Middle repair chunk">>,
        Size2 = size(Chunk2),
        ok = machi_flu1_client:write_chunk(Host, PortBase+1, EpochID,
                                           File1, FooOff2, Chunk2),
        {ok, Chunk2} = machi_cr_client:read_chunk(C1, File1, FooOff2, Size2),
        [{X,{ok, Chunk2}} = {X,machi_flu1_client:read_chunk(
                                 Host, PortBase+X, EpochID,
                                 File1, FooOff2, Size2)} || X <- [0,1,2] ],

        %% Misc API smoke & minor regression checks
        {error, not_written} = machi_cr_client:read_chunk(C1, <<"no">>,
                                                          999999999, 1),
        {error, partial_read} = machi_cr_client:read_chunk(C1, File1,
                                                           Off1, 88888888),
        %% Checksum list return value is a primitive binary().
        {ok, KludgeBin} = machi_cr_client:checksum_list(C1, File1),
        true = is_binary(KludgeBin),

        {error, no_such_file} = machi_cr_client:checksum_list(C1, <<"!!!!">>),
        %% Exactly one file right now
        {ok, [_]} = machi_cr_client:list_files(C1),

        %% Go back and test append_chunk_extra() and write_chunk()
        Chunk10 = <<"It's a different chunk!">>,
        Size10 = byte_size(Chunk10),
        Extra10 = 5,
        {ok, {Off10,Size10,File10}} =
            machi_cr_client:append_chunk_extra(C1, Prefix, Chunk10,
                                               Extra10 * Size10),
        {ok, Chunk10} = machi_cr_client:read_chunk(C1, File10, Off10, Size10),
        [begin
             Offx = Off10 + (Seq * Size10),
             %% TODO: uncomment written/not_written enforcement is available.
             %% {error,not_written} = machi_cr_client:read_chunk(C1, File10,
             %%                                                  Offx, Size10),
             {ok, {Offx,Size10,File10}} =
                 machi_cr_client:write_chunk(C1, File10, Offx, Chunk10),
             {ok, Chunk10} = machi_cr_client:read_chunk(C1, File10, Offx,
                                                        Size10)
         end || Seq <- lists:seq(1, Extra10)],
        {ok, {Off11,Size11,File11}} =
            machi_cr_client:append_chunk(C1, Prefix, Chunk10),
        %% Double-check that our reserved extra bytes were really honored!
        true = (Off11 > (Off10 + (Extra10 * Size10))),

        ok
    after
        error_logger:tty(true),
        catch application:stop(machi),
        exit(SupPid, normal)
    end.

-endif. % !PULSE
-endif. % TEST.
