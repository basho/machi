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

-module(machi_flu_manager).

-behaviour(gen_server).

-include("machi_flu.hrl"). %% contains state record

%% Public API
-export([
    start_link/1,
    start/1,
    stop/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% PUBLIC API

start_link(S = #state{flu_name = Name}) ->
    gen_server:start_link({local, Name}, ?MODULE, [S], []).

%% TODO Make this a functional thing
start(_FluName) ->
    ok.

%% TODO Make this functional
stop(_) -> ok.

%% gen_server callbacks
init(S = #state{flu_name = N, epoch_id = EpochId, wedged = W}) ->
    Tid = ets:new(make_name(N, "_epoch"), [set, protected, named_table, {read_concurrency, true}]),
    true = ets:insert(Tid, {epoch, {W, EpochId}}),
    {ok, S#state{etstab=Tid}}.

handle_cast(Req, S) ->
    lager:warning("Unexpected cast ~p", [Req]),
    {noreply, S}.

handle_call(Req, _From, S) ->
    lager:warning("Unexpected call ~p", [Req]),
    {reply, unexpected, S}.

handle_info({wedge_myself, _EpochId}, S = #state{wedged = true}) ->
    lager:debug("Request to wedge myself, but I'm already wedged. Ignoring."),
    {noreply, S};
handle_info({wedge_myself, EpochId}, S = #state{flu_name = N,
                                                wedged = false,
                                                epoch_id = E, 
                                                etstab = Tid}) when EpochId == E ->
    true = ets:insert(Tid, {epoch, {true, E}}),
    kick_chain_manager(N),
    {noreply, S#state{wedged=true}};

handle_info({wedge_state_change, Bool, {NewEpoch, _}}, 
            S = #state{epoch_id = undefined, etstab=Tid}) ->
   true = ets:insert(Tid, {epoch, {Bool, NewEpoch}}),
   {noreply, S#state{wedged = Bool, epoch_id = NewEpoch}};
handle_info({wedge_state_change, Bool, {NewEpoch, _}}, 
            S = #state{epoch_id = E, etstab = Tid}) when NewEpoch >= E ->
   true = ets:insert(Tid, {epoch, {Bool, NewEpoch}}),
   {noreply, S#state{wedged = Bool, epoch_id = NewEpoch}};
handle_info(M = {wedge_state_change, _Bool, {NewEpoch, _}}, 
            S = #state{epoch_id = E}) when NewEpoch < E ->
    lager:debug("Wedge state change message ~p, but my epoch id is higher (~p). Ignoring.", 
                [M, E]),
    {noreply, S};

handle_info({wedge_status, From}, S = #state{wedged = W, epoch_id = E}) ->
    From ! {wedge_status_reply, W, E},
    {noreply, S};

handle_info({seq_append, From, _Prefix, _Chunk, _Csum, _Extra, _EpochId}, 
            S = #state{wedged = true}) ->
    From ! wedged,
    {noreply, S};

handle_info({seq_append, From, Prefix, Chunk, Csum, Extra, EpochId},
            S = #state{epoch_id = EpochId}) ->
    handle_append(From, Prefix, Chunk, Csum, Extra),
    {noreply, S};

handle_info(Info, S) ->
    lager:warning("Unexpected info ~p", [Info]),
    {noreply, S}.

terminate(Reason, _S) ->
    lager:info("Terminating because ~p", [Reason]),
    ok.

code_change(_Old, S, _Extra) ->
    {ok, S}.

%% private
kick_chain_manager(Name) ->
    Chmgr = machi_chain_manager1:make_chmgr_regname(Name),
    spawn(fun() ->
        catch machi_chain_manager1:trigger_react_to_env(Chmgr)
    end).

handle_append(From, Prefix, Chunk, Csum, Extra) ->
    spawn(fun() -> 
        dispatch_append(From, Prefix, Chunk, Csum, Extra) 
    end).

dispatch_append(From, Prefix, Chunk, Csum, Extra) ->
    {ok, Pid} = machi_flu_metadata_mgr:start_proxy_pid(Prefix),
    {Tag, CS} = machi_util:unmake_tagged_csum(Csum),
    try
        {ok, Filename, Offset} = machi_flu_file_proxy:append(Pid, 
                                [{client_csum_tag, Tag}, {client_csum, CS}], 
                                Extra, Chunk),
        From ! {assignment, Offset, Filename},
        exit(normal)
    catch
        _Type:Reason ->
            lager:error("Could not append chunk to prefix ~p because ~p",
                        [Prefix, Reason]),
            exit(Reason)
    end.

make_name(N, Suffix) ->
    atom_to_list(N) ++ Suffix.
