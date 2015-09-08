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

-module(machi_fitness).

-behaviour(gen_server).

%% API
-export([start_link/1,
         get_unfit_list/1, update_local_down_list/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          unfit=[] :: list()
         }).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

get_unfit_list(PidSpec) ->
    gen_server:call(PidSpec, {get_unfit_list}, infinity).

update_local_down_list(PidSpec, Down) ->
    gen_server:call(PidSpec, {update_local_down_list, Down}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([{FluName}|_Args]) ->
    RegName = machi_flu_psup:make_fitness_regname(FluName),
    register(RegName, self()),
    {ok, #state{}}.

handle_call({get_unfit_list}, _From, S) ->
    {reply, S#state.unfit, S};
handle_call({update_local_down_list, Down}, _From, S) ->
    {reply, ok, S#state{unfit=Down}};
handle_call(_Request, _From, S) ->
    Reply = whhhhhhhhhhhhhhaaaaaaaaaaaaaaa,
    {reply, Reply, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
