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
-ifndef(PULSE).

-include_lib("eunit/include/eunit.hrl").

-include("machi_projection.hrl").

%% smoke_test2() will try repeatedly to make a TCP connection to ports
%% on localhost that have no listener.
%% If you use 'sysctl -w net.inet.icmp.icmplim=3' before running this
%% test, you'll get to exercise some timeout handling in
%% machi_chain_manager1:perhaps_call_t().
%% The default for net.inet.icmp.icmplim is 50.

smoke_test_() ->
    {timeout, 5*60, fun() -> smoke_test2() end}.

smoke_test2() ->
    Ps = [{a,#p_srvr{name=a, address="localhost", port=5550, props="./data.a"}},
          {b,#p_srvr{name=b, address="localhost", port=5551, props="./data.b"}},
          {c,#p_srvr{name=c, address="localhost", port=5552, props="./data.c"}}
         ],
    [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
    {ok, SupPid} = machi_sup:start_link(),
    try
        %% Only run a, don't run b & c so we have 100% failures talking to them
        [begin
             #p_srvr{name=Name, port=Port, props=Dir} = P,
             {ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, [])
         end || {_,P} <- [hd(Ps)]],

        [machi_chain_manager1:trigger_react_to_env(a_chmgr) || _ <-lists:seq(1,5)],
        machi_chain_manager1:set_chain_members(a_chmgr, orddict:from_list(Ps)),
        [machi_chain_manager1:trigger_react_to_env(a_chmgr) || _ <-lists:seq(1,5)],
        ok
    after
        exit(SupPid, normal),
        [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
        machi_util:wait_for_death(SupPid, 100),
        ok
    end.

partial_stop_restart_test_() ->
    {timeout, 5*60, fun() -> partial_stop_restart2() end}.

partial_stop_restart2() ->
    Ps = [{a,#p_srvr{name=a, address="localhost", port=5560, props="./data.a"}},
          {b,#p_srvr{name=b, address="localhost", port=5561, props="./data.b"}},
          {c,#p_srvr{name=c, address="localhost", port=5562, props="./data.c"}}
         ],
    ChMgrs = [machi_flu_psup:make_mgr_supname(P#p_srvr.name) || {_,P} <-Ps],
    PStores = [machi_flu_psup:make_proj_supname(P#p_srvr.name) || {_,P} <-Ps],
    Dict = orddict:from_list(Ps),
    [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
    {ok, SupPid} = machi_sup:start_link(),
    DbgProps = [{initial_wedged, true}],
    Start = fun({_,P}) ->
                    #p_srvr{name=Name, port=Port, props=Dir} = P,
                    {ok, _} = machi_flu_psup:start_flu_package(
                                Name, Port, Dir, [{active_mode,false}|DbgProps])
            end,
    WedgeStatus = fun({_,#p_srvr{address=Addr, port=TcpPort}}) ->
                          machi_flu1_client:wedge_status(Addr, TcpPort)
                  end,
    NSInfo = undefined,
    Append = fun({_,#p_srvr{address=Addr, port=TcpPort}}, EpochID) ->
                     NoCSum = <<>>,
                     machi_flu1_client:append_chunk(Addr, TcpPort,
                                                    NSInfo, EpochID,
                                                    <<"prefix">>,
                                                    <<"data">>, NoCSum)
                  end,
    try
        [Start(P) || P <- Ps],
        [{ok, {true, _,_,_}} = WedgeStatus(P) || P <- Ps], % all are wedged
        [{error,wedged} = Append(P, ?DUMMY_PV1_EPOCH) || P <- Ps], % all are wedged

        [machi_chain_manager1:set_chain_members(ChMgr, Dict) ||
            ChMgr <- ChMgrs ],
        {ok, {false, EpochID1,_,_}} = WedgeStatus(hd(Ps)),
        [{ok, {false, EpochID1,_,_}} = WedgeStatus(P) || P <- Ps], % *not* wedged
        [{ok,_} = Append(P, EpochID1) || P <- Ps],             % *not* wedged
        {ok, {_,_,File1}} = Append(hd(Ps), EpochID1),

        {_,_,_} = machi_chain_manager1:trigger_react_to_env(hd(ChMgrs)),
        [begin
             _QQa = machi_chain_manager1:trigger_react_to_env(ChMgr)
         end || _ <- lists:seq(1,125), ChMgr <- ChMgrs],

        %% All chain managers & projection stores should be using the
        %% same projection which is max projection in each store.
        {no_change,_,Epoch_m} = machi_chain_manager1:trigger_react_to_env(
                                  hd(ChMgrs)),
        [{no_change,_,Epoch_m} = machi_chain_manager1:trigger_react_to_env(
                                   ChMgr )|| ChMgr <- ChMgrs],
        {ok, Proj_m} = machi_projection_store:read_latest_projection(
                         hd(PStores), public),
        [begin
             {ok, Proj_m2} = machi_projection_store:read_latest_projection(
                               PStore, ProjType),
             true = (machi_projection:update_dbg2(Proj_m, []) ==
                     machi_projection:update_dbg2(Proj_m2, []))
         end || ProjType <- [public, private], PStore <- PStores ],
        Epoch_m = Proj_m#projection_v1.epoch_number,
        %% Confirm that all FLUs are *not* wedged, with correct proj & epoch
        Proj_mCSum = Proj_m#projection_v1.epoch_csum,
        [{ok, {false, {Epoch_m, Proj_mCSum},_,_}} = WedgeStatus(P) || % *not* wedged
             P <- Ps], 
        {ok, {false, EpochID1,_,_}} = WedgeStatus(hd(Ps)),
        [{ok,_} = Append(P, EpochID1) || P <- Ps],            % *not* wedged

        %% Stop all but 'a'.
        [ok = machi_flu_psup:stop_flu_package(Name) || {Name,_} <- tl(Ps)],

        %% Stop and restart a.
        {FluName_a, _} = hd(Ps),
        ok = machi_flu_psup:stop_flu_package(FluName_a),
        {ok, _} = Start(hd(Ps)),
        timer:sleep(123),      % TODO fix server socket available race condition in better way
        %% Remember: 'a' is not in active mode.
        {ok, Proj_m3} = machi_projection_store:read_latest_projection(
                          hd(PStores), private),
        true = (machi_projection:update_dbg2(Proj_m, []) ==
                    machi_projection:update_dbg2(Proj_m3, [])),
        %% Confirm that 'a' is wedged
        {error, wedged} = Append(hd(Ps), EpochID1),
        {_, #p_srvr{address=Addr_a, port=TcpPort_a}} = hd(Ps),
        {error, wedged} = machi_flu1_client:read_chunk(
                            Addr_a, TcpPort_a, NSInfo, ?DUMMY_PV1_EPOCH,
                            <<>>, 99999999, 1, undefined),
        {error, bad_arg} = machi_flu1_client:checksum_list(
                             Addr_a, TcpPort_a, <<>>),
        %% list_files() is permitted despite wedged status
        {ok, _} = machi_flu1_client:list_files(
                            Addr_a, TcpPort_a, ?DUMMY_PV1_EPOCH),

        %% Iterate through humming consensus once
        {now_using,_,Epoch_n} = machi_chain_manager1:trigger_react_to_env(
                                  hd(ChMgrs)),
        true = (Epoch_n > Epoch_m),
        {ok, {false, EpochID3,_,_}} = WedgeStatus(hd(Ps)),
        %% The file we're assigned should be different with the epoch change.
        {ok, {_,_,File3}} = Append(hd(Ps), EpochID3),
        true = (File1 /= File3),

        %% Confirm that 'a' is *not* wedged
        {ok, _} = Append(hd(Ps), EpochID3),

        ok
    after
        exit(SupPid, normal),
        [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
        machi_util:wait_for_death(SupPid, 100),
        ok
    end.

p_srvr_rec_test() ->
    P = #p_srvr{name=a, address="localhost", port=1024, props=[yo]},
    [P] = machi_flu_sup:sanitize_p_srvr_records([P]),
    [P] = machi_flu_sup:sanitize_p_srvr_records([P,P]),
    [] = machi_flu_sup:sanitize_p_srvr_records([nope]),
    [] = machi_flu_sup:sanitize_p_srvr_records([#p_srvr{proto_mod=does_not_exist}]),
    [] = machi_flu_sup:sanitize_p_srvr_records([#p_srvr{proto_mod="lists"}]),
    [] = machi_flu_sup:sanitize_p_srvr_records([#p_srvr{address=7}]),
    [] = machi_flu_sup:sanitize_p_srvr_records([#p_srvr{port=5}]),
    [] = machi_flu_sup:sanitize_p_srvr_records([#p_srvr{port=foo}]),
    [] = machi_flu_sup:sanitize_p_srvr_records([#p_srvr{props=foo}]),
    ok.

-endif. % !PULSE
-endif. % TEST

        
    
