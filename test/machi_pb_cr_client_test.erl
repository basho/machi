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

-module(machi_pb_cr_client_test).
-compile(export_all).

-ifdef(TEST).
-ifndef(PULSE).

-include("machi_pb.hrl").
-include("machi_projection.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(C, machi_pb_cr_client).

smoke_test_() ->
    {timeout, 5*60, fun() -> smoke_test2() end}.

smoke_test2() ->
    Port = 5720,
    Ps = [{a,#p_srvr{name=a, address="localhost", port=Port, props="./data.a"}}
         ],
    
    [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
    {ok, SupPid} = machi_flu_sup:start_link(),
    try
        [begin
             #p_srvr{name=Name, port=Port, props=Dir} = P,
             {ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, [])
         end || {_,P} <- Ps],
        [machi_chain_manager1:test_react_to_env(a_chmgr) || _ <-lists:seq(1,5)],

        {_, P_a} = hd(Ps),
        {ok, Sock} = gen_tcp:connect(P_a#p_srvr.address, P_a#p_srvr.port,
                                     [{packet, line}, binary, {active, false}]),
        try
            Prefix = <<"pre">>,
            Chunk1 = <<"yochunk">>,
            ok = gen_tcp:send(Sock, <<"PROTOCOL-BUFFERS\n">>),
            {ok, <<"OK\n">>} = gen_tcp:recv(Sock, 0),
            ok = inet:setopts(Sock, [{packet,4}]),
            R1a = #mpb_request{req_id= <<0>>,
                               echo=#mpb_echoreq{message = <<"Hello, world!">>}},
            Bin1a = machi_pb:encode_mpb_request(R1a),
            ok = gen_tcp:send(Sock, Bin1a),
            {ok, Bin1B} = gen_tcp:recv(Sock, 0),
            R1b = machi_pb:decode_mpb_response(Bin1B),
            true = is_record(R1b, mpb_response),

            ok
        after
            (catch gen_tcp:close(Sock))
        end
    after
        exit(SupPid, normal),
        [os:cmd("rm -rf " ++ P#p_srvr.props) || {_,P} <- Ps],
        machi_util:wait_for_death(SupPid, 100),
        ok
    end.

-endif. % !PULSE
-endif. % TEST
