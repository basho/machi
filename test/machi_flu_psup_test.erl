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
    Ps = [{a,#p_srvr{name=a, address="localhost", port=5555, props="./data.a"}},
          {b,#p_srvr{name=b, address="localhost", port=5556, props="./data.b"}},
          {c,#p_srvr{name=c, address="localhost", port=5557, props="./data.c"}}
         ],
    [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
    {ok, SupPid} = machi_flu_sup:start_link(),
    try
        [begin
             #p_srvr{name=Name, port=Port, props=Dir} = P,
             {ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, [])
         end || {_,P} <- [hd(Ps)]],
         %% end || {_,P} <- Ps],

        [begin
             _QQ = machi_chain_manager1:test_react_to_env(a_chmgr),
             ok
         end || _ <- lists:seq(1,5)],
        machi_chain_manager1:set_chain_members(a_chmgr, orddict:from_list(Ps)),
        [begin
             _QQ = machi_chain_manager1:test_react_to_env(a_chmgr),
             ok
         end || _ <- lists:seq(1,5)],
        ok
    after
        exit(SupPid, normal),
        [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
        machi_util:wait_for_death(SupPid, 100),
        ok
    end.

-endif. % TEST

        
    
