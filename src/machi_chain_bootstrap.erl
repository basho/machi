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

-module(machi_chain_bootstrap).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    self() ! finish_init,
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(finish_init, State) ->
    FLUs = get_local_flu_names(),
    FLU_Epochs = get_latest_public_epochs(FLUs),
    FLUs_at_zero = [FLU || {FLU, 0} <- FLU_Epochs],
    lager:info("FLUs at epoch 0: ~p\n", [FLUs_at_zero]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%

get_local_flu_names() ->
    [Name || {Name,_,_,_} <- supervisor:which_children(machi_flu_sup)].

get_latest_public_epochs(FLUs) ->
    [begin
         PS = machi_flu1:make_projection_server_regname(FLU),
         {ok, {Epoch, _CSum}} = machi_projection_store:get_latest_epochid(
                                  PS, public),
         {FLU, Epoch}
     end || FLU <- FLUs].
