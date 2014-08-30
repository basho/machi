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
-export([start_link/4, stop/1,
         put/3, get/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(OID_STREAM_NUMBER, 0).

-define(LONG_TIME, 5*1000).
%% -define(LONG_TIME, 30*1000).

-define(D(X), io:format(user, "Dbg: ~s =\n  ~p\n", [??X, X])).

-type lpn() :: non_neg_integer().

-record(state, {
          page_size :: non_neg_integer(),       % CORFU page size
          seq :: pid(),                         % sequencer pid
          proj :: term(),                       % projection
          cb_mod :: atom(),                     % callback module
          last_read_lpn :: lpn(),               %
          last_write_lpn :: lpn(),
          back_ps :: [lpn()],                   % back pointers (up to 4)
          i_state :: term()                     % internal state thingie
         }).

start_link(PageSize, SequencerPid, Proj, CallbackMod) ->
    gen_server:start_link(?MODULE,
                          [PageSize, SequencerPid, Proj, CallbackMod], []).

stop(Pid) ->
    gen_server:call(Pid, {stop}, ?LONG_TIME).

put(Pid, Key, Val) ->
    gen_server:call(Pid, {put, Key, Val}, ?LONG_TIME).

get(Pid, Key) ->
    gen_server:call(Pid, {get, Key}, ?LONG_TIME).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([PageSize, SequencerPid, Proj, CallbackMod]) ->
    LastLPN = find_last_lpn(SequencerPid),
    {BackPs, Pages} = fetch_unread_pages(Proj, LastLPN, 0),
    I_State = play_log_pages(Pages, CallbackMod:fresh(), CallbackMod),
    {ok, #state{page_size=PageSize,
                seq=SequencerPid,
                proj=Proj,
                cb_mod=CallbackMod,
                last_read_lpn=LastLPN,
                last_write_lpn=LastLPN,
                back_ps=BackPs,
                i_state=I_State}}.

handle_call({put, _Key, _Val}=Op, _From,
            #state{proj=Proj0, cb_mod=CallbackMod,
                   page_size=PageSize, back_ps=BackPs, i_state=I_State}=State) ->
    {Res, I_State2, Proj1, LPN, NewBackPs} =
        CallbackMod:do_dirty_op(Op, I_State, ?OID_STREAM_NUMBER,
                                Proj0, PageSize, BackPs),
    {reply, Res, State#state{i_state=I_State2,
                             proj=Proj1,
                             last_write_lpn=LPN,
                             back_ps=NewBackPs}};
handle_call({get, _Key}=Op, _From, #state{cb_mod=CallbackMod} = State) ->
    {Pages, State2} = fetch_unread_pages(State),
    %% ?D(Pages),
    #state{i_state=I_State} = State3 = play_log_pages(Pages, State2),
    Reply = CallbackMod:do_pure_op(Op, I_State),
    {reply, Reply, State3};
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

play_log_pages(Pages, #state{cb_mod=CallbackMod, i_state=I_State} = State) ->
    I_State2 = play_log_pages(Pages, I_State, CallbackMod),
    State#state{i_state=I_State2}.

play_log_pages(Pages, I_State, CallbackMod) ->
    CallbackMod:play_log_mutate_i_state(Pages, I_State).
