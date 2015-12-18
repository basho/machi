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

-module(machi_cinfo_test).

-ifdef(TEST).
-ifndef(PULSE).

-include_lib("eunit/include/eunit.hrl").

-include("machi_projection.hrl").

%% smoke_test() will try just dump cluster_info and call each functions

smoke_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      fun() -> machi_cinfo:public_projection(a) end,
      fun() -> machi_cinfo:private_projection(a) end,
      fun() -> machi_cinfo:fitness(a) end,
      fun() -> machi_cinfo:chain_manager(a) end,
      fun() -> machi_cinfo:flu1(a) end,
      fun() -> machi_cinfo:dump() end
     ]}.

setup() ->
    machi_cinfo:register(),
    Ps = [{a,#p_srvr{name=a, address="localhost", port=5555, props="./data.a"}},
          {b,#p_srvr{name=b, address="localhost", port=5556, props="./data.b"}},
          {c,#p_srvr{name=c, address="localhost", port=5557, props="./data.c"}}
         ],
    [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
    {ok, SupPid} = machi_sup:start_link(),
    %% Only run a, don't run b & c so we have 100% failures talking to them
    [begin
         #p_srvr{name=Name, port=Port, props=Dir} = P,
         {ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, [])
     end || {_,P} <- [hd(Ps)]],
    machi_chain_manager1:set_chain_members(a_chmgr, orddict:from_list(Ps)),
    {SupPid, Ps}.

cleanup({SupPid, Ps}) ->
    exit(SupPid, normal),
    [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
    machi_util:wait_for_death(SupPid, 100),
    ok.

-endif. % !PULSE
-endif. % TEST
