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

setup_smoke_test(Host, PortBase, Os, Witness_list) ->
    os:cmd("rm -rf ./data.a ./data.b ./data.c"),
    {ok, _} = machi_util:wait_for_life(machi_flu_sup, 100),

    F = fun(X) -> case lists:member(X, Witness_list) of
                      true ->
                          [{witness_mode, true}|Os];
                      false ->
                          Os
                  end
        end,
    {ok,_}=machi_flu_psup:start_flu_package(a, PortBase+0, "./data.a", F(a)),
    {ok,_}=machi_flu_psup:start_flu_package(b, PortBase+1, "./data.b", F(b)),
    {ok,_}=machi_flu_psup:start_flu_package(c, PortBase+2, "./data.c", F(c)),
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
    CMode = if Witness_list == [] -> ap_mode;
               Witness_list /= [] -> cp_mode
            end,
    ok = machi_chain_manager1:set_chain_members(a_chmgr, ch0, 0, CMode,
                                                D, Witness_list),
    ok = machi_chain_manager1:set_chain_members(b_chmgr, ch0, 0, CMode,
                                                D, Witness_list),
    ok = machi_chain_manager1:set_chain_members(c_chmgr, ch0, 0, CMode,
                                                D, Witness_list),
    run_ticks([a_chmgr,b_chmgr,c_chmgr]),
    %% Everyone is settled on the same damn epoch id.
    {ok, EpochID} = machi_flu1_client:get_latest_epochid(Host, PortBase+0,
                                                         private),
    {ok, EpochID} = machi_flu1_client:get_latest_epochid(Host, PortBase+1,
                                                         private),
    {ok, EpochID} = machi_flu1_client:get_latest_epochid(Host, PortBase+2,
                                                         private),

    {D, EpochID}.

run_ticks(MgrList) ->
    TickAll = fun() -> [begin
                            Pid ! tick_check_environment,
                            timer:sleep(50)
                        end || Pid <- MgrList]
              end,
    _ = lists:foldl(
          fun(_, [{_,[a,b,c]}]=Acc) -> Acc;
             (_, [{_,[b,c]}]=Acc)   -> Acc;  %% Fragile, but for witness=[a]
             (_, [{_,[a,c]},{yy,b_pstore}]=Acc) -> Acc;  %% Fragile, but for witness=[a]
             (_, _Acc)  ->
                  TickAll(),                % has some sleep time inside
                  Xs = [try
                            {ok, Prj} = machi_projection_store:read_latest_projection(PStore, private),
                            {Prj#projection_v1.author_server,
                             Prj#projection_v1.upi}
                        catch _:_ ->
                                {yy, PStore}
                        end || PStore <- [a_pstore,b_pstore,c_pstore] ],
                  lists:usort(Xs)
          end, undefined, lists:seq(1,10000)),
    ok.

smoke_test2() ->
    {ok, SupPid} = machi_sup:start_link(),
    error_logger:tty(false),
    try
        Prefix = <<"pre">>,
        Chunk1 = <<"yochunk">>,
        NSInfo = undefined,
        NoCSum = <<>>,
        Host = "localhost",
        PortBase = 64454,
        Os = [{ignore_stability_time, true}, {active_mode, false}],
        {D, EpochID} = setup_smoke_test(Host, PortBase, Os, []),

        %% Whew ... ok, now start some damn tests.
        {ok, C1} = machi_cr_client:start_link([P || {_,P}<-orddict:to_list(D)]),
        machi_cr_client:append_chunk(C1, NSInfo, Prefix, Chunk1, NoCSum),
        {ok, {Off1,Size1,File1}} =
            machi_cr_client:append_chunk(C1, NSInfo, Prefix, Chunk1, NoCSum),
        BadCSum = {?CSUM_TAG_CLIENT_SHA, crypto:hash(sha, "foo")},
        {error, bad_checksum} =
            machi_cr_client:append_chunk(C1, NSInfo, Prefix, Chunk1, BadCSum),
        {ok, {[{_, Off1, Chunk1, _}], []}} =
            machi_cr_client:read_chunk(C1, NSInfo, File1, Off1, Size1, undefined),
        {ok, PPP} = machi_flu1_client:read_latest_projection(Host, PortBase+0,
                                                             private),
        %% Verify that the client's CR wrote to all of them.
        [{ok, {[{_, Off1, Chunk1, _}], []}} =
             machi_flu1_client:read_chunk(
               Host, PortBase+X, NSInfo, EpochID, File1, Off1, Size1, undefined) ||
            X <- [0,1,2] ],

        %% Test read repair: Manually write to head, then verify that
        %% read-repair fixes all.
        FooOff1 = Off1 + (1024*1024),
        [{error, not_written} = machi_flu1_client:read_chunk(
                                  Host, PortBase+X, NSInfo, EpochID,
                                  File1, FooOff1, Size1, undefined) || X <- [0,1,2] ],
        ok = machi_flu1_client:write_chunk(Host, PortBase+0, NSInfo, EpochID,
                                           File1, FooOff1, Chunk1, NoCSum),
        {ok, {[{File1, FooOff1, Chunk1, _}=_YY], []}} =
            machi_flu1_client:read_chunk(Host, PortBase+0, NSInfo, EpochID,
                                         File1, FooOff1, Size1, undefined),
        {ok, {[{File1, FooOff1, Chunk1, _}], []}} =
            machi_cr_client:read_chunk(C1, NSInfo, File1, FooOff1, Size1, undefined),
        [?assertMatch({X,{ok, {[{_, FooOff1, Chunk1, _}], []}}},
                      {X,machi_flu1_client:read_chunk(
                                 Host, PortBase+X, NSInfo, EpochID,
                                 File1, FooOff1, Size1, undefined)})
         || X <- [0,1,2] ],

        %% Test read repair: Manually write to middle, then same checking.
        FooOff2 = Off1 + (2*1024*1024),
        Chunk2 = <<"Middle repair chunk">>,
        Size2 = size(Chunk2),
        ok = machi_flu1_client:write_chunk(Host, PortBase+1, NSInfo, EpochID,
                                           File1, FooOff2, Chunk2, NoCSum),
        {ok, {[{File1, FooOff2, Chunk2, _}], []}} =
            machi_cr_client:read_chunk(C1, NSInfo, File1, FooOff2, Size2, undefined),
        [{X,{ok, {[{File1, FooOff2, Chunk2, _}], []}}} =
             {X,machi_flu1_client:read_chunk(
                  Host, PortBase+X, NSInfo, EpochID,
                  File1, FooOff2, Size2, undefined)} || X <- [0,1,2] ],

        %% Misc API smoke & minor regression checks
        {error, bad_arg} = machi_cr_client:read_chunk(C1, NSInfo, <<"no">>,
                                                      999999999, 1, undefined),
        {ok, {[{File1,Off1,Chunk1,_}, {File1,FooOff1,Chunk1,_}, {File1,FooOff2,Chunk2,_}],
              []}} =
            machi_cr_client:read_chunk(C1, NSInfo, File1, Off1, 88888888, undefined),
        %% Checksum list return value is a primitive binary().
        {ok, KludgeBin} = machi_cr_client:checksum_list(C1, File1),
        true = is_binary(KludgeBin),

        {error, bad_arg} = machi_cr_client:checksum_list(C1, <<"!!!!">>),
        io:format(user, "\nFiles = ~p\n", [machi_cr_client:list_files(C1)]),
        %% Exactly one file right now, e.g.,
        %% {ok,[{2098202,<<"pre^b144ef13-db4d-4c9f-96e7-caff02dc754f^1">>}]}
        {ok, [_]} = machi_cr_client:list_files(C1),

        %% Go back and test append_chunk() + extra and write_chunk()
        Chunk10 = <<"It's a different chunk!">>,
        Size10 = byte_size(Chunk10),
        Extra10 = 5,
        Opts1 = #append_opts{chunk_extra=Extra10*Size10},
        {ok, {Off10,Size10,File10}} =
            machi_cr_client:append_chunk(C1, NSInfo, Prefix, Chunk10,
                                         NoCSum, Opts1),
        {ok, {[{_, Off10, Chunk10, _}], []}} =
            machi_cr_client:read_chunk(C1, NSInfo, File10, Off10, Size10, undefined),
        [begin
             Offx = Off10 + (Seq * Size10),
             %% TODO: uncomment written/not_written enforcement is available.
             %% {error,not_written} = machi_cr_client:read_chunk(C1, NSInfo, File10,
             %%                                                  Offx, Size10),
             {ok, {Offx,Size10,File10}} =
                 machi_cr_client:write_chunk(C1, NSInfo, File10, Offx, Chunk10, NoCSum),
             {ok, {[{_, Offx, Chunk10, _}], []}} =
                 machi_cr_client:read_chunk(C1, NSInfo, File10, Offx, Size10, undefined)
         end || Seq <- lists:seq(1, Extra10)],
        {ok, {Off11,Size11,File11}} =
            machi_cr_client:append_chunk(C1, NSInfo, Prefix, Chunk10, NoCSum),
        %% %% Double-check that our reserved extra bytes were really honored!
        %% true = (Off11 > (Off10 + (Extra10 * Size10))),
io:format(user, "\nFiles = ~p\n", [machi_cr_client:list_files(C1)]),

        ok
    after
        exit(SupPid, normal),
        machi_util:wait_for_death(SupPid, 100),
        error_logger:tty(true),
        catch application:stop(machi)
    end.

witness_smoke_test_() -> {timeout, 1*60, fun() -> witness_smoke_test2() end}.

witness_smoke_test2() ->
    SupPid = case machi_sup:start_link() of
                {ok, P} -> P;
                {error, {already_started, P1}} -> P1;
                Other -> error(Other)
    end,
    %% TODO: I wonder why commenting this out makes this test pass
    %% error_logger:tty(true),
    try
        Prefix = <<"pre">>,
        Chunk1 = <<"yochunk">>,
        NSInfo = undefined,
        NoCSum = <<>>,
        Host = "localhost",
        PortBase = 64444,
        Os = [{ignore_stability_time, true}, {active_mode, false},
              {consistency_mode, cp_mode}],
        OurWitness = a,
        {D, EpochID} = setup_smoke_test(Host, PortBase, Os, [OurWitness]),

        %% Whew ... ok, now start some damn tests.
        {ok, C1} = machi_cr_client:start_link([P || {_,P}<-orddict:to_list(D)]),
        {ok, _} = machi_cr_client:append_chunk(C1, NSInfo, Prefix,
                                               Chunk1, NoCSum),
        {ok, {Off1,Size1,File1}} =
            machi_cr_client:append_chunk(C1, NSInfo, Prefix, Chunk1, NoCSum),
        BadCSum = {?CSUM_TAG_CLIENT_SHA, crypto:hash(sha, "foo")},
        {error, bad_checksum} =
            machi_cr_client:append_chunk(C1, NSInfo, Prefix, Chunk1, BadCSum),
        {ok, {[{_, Off1, Chunk1, _}], []}} =
            machi_cr_client:read_chunk(C1, NSInfo, File1, Off1, Size1, undefined),

        %% Stop 'b' and let the chain reset.
        ok = machi_flu_psup:stop_flu_package(b),
        %% ok = machi_fitness:add_admin_down(a_fitness, admin_down_bogus_flu, [{why,because}]),
        %% ok = machi_fitness:delete_admin_down(a_fitness, admin_down_bogus_flu),
        %% Run ticks enough times to force auto-unwedge of both a & c.
        [run_ticks([a_chmgr,c_chmgr]) || _ <- [1,2,3,4] ],

        %% The chain should now be [a,c].
        %% Let's wedge OurWitness and see what happens: timeout/partition.
        #p_srvr{name=WitName, address=WitA, port=WitP} =
            orddict:fetch(OurWitness, D),
        {ok, {false, EpochID2,_,_}} = machi_flu1_client:wedge_status(WitA, WitP),
        machi_flu1:wedge_myself(WitName, EpochID2),
        case machi_flu1_client:wedge_status(WitA, WitP) of
            {ok, {true,  EpochID2,_,_}} ->
                ok;
            {ok, {false,  EpochID2,_,_}} ->
                %% This is racy.  Work around it by sleeping a while.
                timer:sleep(6*1000),
                {ok, {true,  EpochID2,_,_}} =
                    machi_flu1_client:wedge_status(WitA, WitP)
        end,

        %% Chunk1 is still readable: not affected by wedged witness head.
        {ok, {[{_, Off1, Chunk1, _}], []}} =
            machi_cr_client:read_chunk(C1, NSInfo, File1, Off1, Size1, undefined),
        %% But because the head is wedged, an append will fail.
        {error, partition} =
            machi_cr_client:append_chunk(C1, NSInfo, Prefix, Chunk1, NoCSum,
                                         #append_opts{}, 1*1000),

        %% The witness's wedge status should cause timeout/partition
        %% for write_chunk also.
        Chunk10 = <<"It's a different chunk!">>,
        Size10 = byte_size(Chunk10),
        File10 = File1,
        Offx = Off1 + (1 * Size10),
        {error, partition} =
            machi_cr_client:write_chunk(C1, NSInfo, File10, Offx, Chunk10, NoCSum, 1*1000),

        ok
    after
        error_logger:tty(true),
        catch application:stop(machi),
        exit(SupPid, normal)
    end.

-endif. % !PULSE
-endif. % TEST.
