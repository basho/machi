%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(tango_oid).

-behaviour(gen_server).

%% API
-export([start_link/2, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(OID_STREAM_NUMBER, 0).

-define(LONG_TIME, 5*1000).
%% -define(LONG_TIME, 30*1000).

-record(state, {
          seq :: pid(),
          proj :: term()
         }).

start_link(SequencerPid, Proj) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [SequencerPid, Proj], []).

stop(Pid) ->
    gen_server:call(Pid, {stop}, ?LONG_TIME).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([SequencerPid, Proj]) ->
    {ok, CurrentLPN} = corfurl_sequencer:get(SequencerPid, 0),
    LastLPN = CurrentLPN - 1,
io:format(user, "LastLPN = ~p\n", [LastLPN]),
    LPNandPages = tango:scan_backward(Proj, ?OID_STREAM_NUMBER, LastLPN, false),
    {_LPNS, Pages} = lists:unzip(LPNandPages),
    io:format("Pages = ~p\n", [Pages]),
    {ok, #state{seq=SequencerPid,
                proj=Proj}}.

handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    Reply = whaaaaaaaaaaaa,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

