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

-module(machi_pb_high_client_test).
-compile(export_all).

-ifdef(TEST).
-ifndef(PULSE).

-include("machi_pb.hrl").
-include("machi_projection.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(C, machi_pb_high_client).

smoke_test_() ->
    {timeout, 5*60, fun() -> smoke_test2() end}.

smoke_test2() ->
    Port = 5720,
    Ps = [#p_srvr{name=a, address="localhost", port=Port, props="./data.a"}
         ],
    
    [os:cmd("rm -rf " ++ P#p_srvr.props) || P <- Ps],
    {ok, SupPid} = machi_flu_sup:start_link(),
    try
        [begin
             #p_srvr{name=Name, port=Port, props=Dir} = P,
             {ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, [])
         end || P <- Ps],
        [machi_chain_manager1:test_react_to_env(a_chmgr) || _ <-lists:seq(1,5)],

        {ok, Clnt} = ?C:start_link(Ps),
        try
            true = ?C:connected_p(Clnt),
            String = "yo, dawg",
            String = ?C:echo(Clnt, String),

            ok
        after
            (catch ?C:quit(Clnt))
        end
    after
        exit(SupPid, normal),
        [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
        machi_util:wait_for_death(SupPid, 100),
        ok
    end.

-endif. % !PULSE
-endif. % TEST
