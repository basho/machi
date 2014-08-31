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
-export([start_link/3, stop/1,
         new/2, get/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Tango datatype callbacks (prototype)
-export([fresh/0,
         do_pure_op/2, do_dirty_op/6, play_log_mutate_i_state/3]).

-define(SERVER, ?MODULE).
-define(OID_STREAM_NUMBER, 0).

-define(LONG_TIME, 30*1000).

-define(D(X), io:format(user, "Dbg: ~s =\n  ~p\n", [??X, X])).

-type lpn() :: non_neg_integer().

-record(state, {
          page_size :: non_neg_integer(),       % CORFU page size
          seq :: pid(),                         % sequencer pid
          proj :: term(),                       % projection
          last_read_lpn :: lpn(),               %
          last_write_lpn :: lpn(),
          back_ps :: [lpn()],                   % back pointers (up to 4)
          i_state :: term()                     % internal state thingie
         }).

start_link(PageSize, SequencerPid, Proj) ->
    gen_server:start_link(?MODULE,
                          [PageSize, SequencerPid, Proj], []).

stop(Pid) ->
    gen_server:call(Pid, {stop}, ?LONG_TIME).

new(Pid, Key) ->
    gen_server:call(Pid, {new, Key}, ?LONG_TIME).

get(Pid, Key) ->
    gen_server:call(Pid, {get, Key}, ?LONG_TIME).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([PageSize, SequencerPid, Proj]) ->
    LastLPN = find_last_lpn(SequencerPid),
    {BackPs, Pages} = fetch_unread_pages(Proj, LastLPN, 0),
    I_State = play_log_pages(Pages, fresh(), ?MODULE, false),
    {ok, #state{page_size=PageSize,
                seq=SequencerPid,
                proj=Proj,
                last_read_lpn=LastLPN,
                last_write_lpn=LastLPN,
                back_ps=BackPs,
                i_state=I_State}}.

handle_call({new, Key}, From,
            #state{proj=Proj0,
                   page_size=PageSize, back_ps=BackPs, i_state=I_State}=State) ->
    Op = {new_oid, Key, From},
    {_Res, I_State2, Proj1, LPN, NewBackPs} =
        do_dirty_op(Op, I_State, ?OID_STREAM_NUMBER,
                    Proj0, PageSize, BackPs),
    %% Let's see how much trouble we can get outselves in here.
    %% If we're here, then we've written to the log without error.
    %% So then the cast to roll forward must see that log entry
    %% (if it also operates without error).  So, the side-effect of
    %% the op ought to always send a reply to the client.
    gen_server:cast(self(), {roll_forward}),
    {noreply, State#state{i_state=I_State2,
                          proj=Proj1,
                          last_write_lpn=LPN,
                          back_ps=NewBackPs}};
handle_call({get, _Key}=Op, _From, State) ->
    State2 = #state{i_state=I_State} = roll_log_forward(State),
    Reply = do_pure_op(Op, I_State),
    {reply, Reply, State2};
handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    Reply = whaaaaaaaaaaaa,
    {reply, Reply, State}.

handle_cast({roll_forward}, State) ->
    State2 = roll_log_forward(State),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

find_last_lpn(#state{seq=SequencerPid}) ->
    find_last_lpn(SequencerPid);
find_last_lpn(SequencerPid) ->
    {ok, CurrentLPN} = corfurl_sequencer:get(SequencerPid, 0),
    CurrentLPN - 1.

fetch_unread_pages(#state{seq=___FIXME_SequencerPid, proj=Proj,
                          last_read_lpn=StopAtLPN,
                          last_write_lpn=LastLPN} = State) ->
    %% TODO: fixme: to handle concurrent updates correctly, we should
    %%       query the sequencer for the last LPN.
    {BackPs, Pages} = fetch_unread_pages(Proj, LastLPN, StopAtLPN),
    %% TODO ????
    %% LastReadLPN = if BackPs == [] -> 0;
    %%                  true         -> hd(BackPs)
    %%               end,
    {Pages, State#state{last_read_lpn=LastLPN, back_ps=BackPs}}.

fetch_unread_pages(Proj, LastLPN, StopAtLPN) ->
    %% ?D({fetch_unread_pages, LastLPN, StopAtLPN}),
    LPNandPages = tango:scan_backward(Proj, ?OID_STREAM_NUMBER, LastLPN,
                                      StopAtLPN, true),
    {LPNs, Pages} = lists:unzip(LPNandPages),
    BackPs = lists:foldl(fun(P, BPs) -> tango:add_back_pointer(BPs, P) end,
                         [], LPNs),
    {BackPs, Pages}.

play_log_pages(Pages, SideEffectsP,
               #state{i_state=I_State} = State) ->
    I_State2 = play_log_pages(Pages, I_State, ?MODULE, SideEffectsP),
    State#state{i_state=I_State2}.

play_log_pages(Pages, I_State, CallbackMod, SideEffectsP) ->
    CallbackMod:play_log_mutate_i_state(Pages, SideEffectsP, I_State).

roll_log_forward(State) ->
    {Pages, State2} = fetch_unread_pages(State),
    play_log_pages(Pages, true, State2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(oid_map, {
          next :: non_neg_integer(),
          map :: dict()
         }).

-define(DICTMOD, dict).

fresh() ->
    #oid_map{next=1,
             map=?DICTMOD:new()}.

do_pure_op({get, Key}, #oid_map{map=Dict}) ->
    ?DICTMOD:find(Key, Dict).

do_dirty_op({new_oid, _Key, _From}=Op,
            I_State, StreamNum, Proj0, PageSize, BackPs) ->
    Page = term_to_binary(Op),
    FullPage = tango:pack_v1([{StreamNum, BackPs}], Page, PageSize),
    {{ok, LPN}, Proj1} = corfurl_client:append_page(Proj0, FullPage),
    NewBackPs = tango:add_back_pointer(BackPs, LPN),
    {ok, I_State, Proj1, LPN, NewBackPs}.

play_log_mutate_i_state(Pages, SideEffectsP, I_State) ->
    lists:foldl(fun({new_oid, Key, From}=_Op, #oid_map{map=Dict, next=Next}=O) ->
                        {Res, O2} = 
                            case ?DICTMOD:find(Key, Dict) of
                                error ->
                                    Dict2 = ?DICTMOD:store(Key, Next, Dict),
                                    {{ok, Next},O#oid_map{map=Dict2,
                                                          next=Next + 1}};
                                {ok, _} ->
                                    {already_exists, O}
                            end,
                        if SideEffectsP ->
                                gen_server:reply(From, Res);
                           true ->
                                ok
                        end,
                        O2
                end,
                I_State,
                [binary_to_term(Page) || Page <- Pages]).
    
