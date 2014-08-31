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

-module(tango_dt).

-behaviour(gen_server).

%% API
-export([start_link/5, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(LONG_TIME, 30*1000).

-define(D(X), io:format(user, "Dbg: ~s =\n  ~p\n", [??X, X])).

-type lpn() :: non_neg_integer().

-record(state, {
          page_size :: non_neg_integer(),       % CORFU page size
          seq :: pid(),                         % sequencer pid
          proj :: term(),                       % projection
          stream_num :: non_neg_integer(),         % this instance's OID number
          cb_mod :: atom(),                     % callback module
          last_read_lpn :: lpn(),               %
          back_ps :: [lpn()],                   % back pointers (up to 4)
          i_state :: term()                     % internal state thingie
         }).

-type callback_i_state() :: term().
-type gen_server_from() :: {pid(), Tag::term()}.

-callback fresh() -> callback_i_state().
-callback do_pure_op(term(), callback_i_state()) -> term().
-callback do_dirty_op(term(), gen_server_from(), callback_i_state(),
                      StreamNum::non_neg_integer(),
                      Proj0::term(), PageSize::non_neg_integer(),
                      BackPs::list()) ->
    {Reply::term(), New_I_State::callback_i_state(),
     Proj::term(), LPN::non_neg_integer(), NewBackPs::list()}.
-callback play_log_mutate_i_state([binary()], boolean(), callback_i_state()) ->
    callback_i_state().

start_link(PageSize, SequencerPid, Proj, CallbackMod, StreamNum) ->
    gen_server:start_link(?MODULE,
                          [PageSize, SequencerPid, Proj, CallbackMod, StreamNum],
                          []).

stop(Pid) ->
    gen_server:call(Pid, {stop}, ?LONG_TIME).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([PageSize, SequencerPid, Proj, CallbackMod, StreamNum]) ->
    LastLPN = find_last_lpn(SequencerPid),
    {BackPs, Pages} = fetch_unread_pages(Proj, LastLPN, 0, StreamNum),
    I_State = play_log_pages(Pages, CallbackMod:fresh(), CallbackMod, false),
    {ok, #state{page_size=PageSize,
                seq=SequencerPid,
                proj=Proj,
                cb_mod=CallbackMod,
                stream_num=StreamNum,
                last_read_lpn=LastLPN,
                back_ps=BackPs,
                i_state=I_State}}.

handle_call({cb_dirty_op, Op}, From,
            #state{proj=Proj0, cb_mod=CallbackMod, stream_num=StreamNum,
                   page_size=PageSize, back_ps=BackPs, i_state=I_State}=State) ->
    {AsyncType, I_State2, Proj1, LPN, NewBackPs} =
        CallbackMod:do_dirty_op(Op, From, I_State, StreamNum,
                                Proj0, PageSize, BackPs),
    State2 = State#state{i_state=I_State2,
                         proj=Proj1,
                         back_ps=NewBackPs},
    if AsyncType == op_t_async ->
            {reply, ok, State2};
       AsyncType == op_t_sync ->
            State3 = roll_log_forward(LPN, State2),
            {noreply, State3}
    end;
handle_call({cb_pure_op, Op}, _From, #state{cb_mod=CallbackMod} = State) ->
    State2 = #state{i_state=I_State} = roll_log_forward(State),
    Reply = CallbackMod:do_pure_op(Op, I_State),
    {reply, Reply, State2};
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

fetch_unread_pages(#state{seq=SequencerPid, stream_num=StreamNum} = State) ->
    {ok, [LastLPN]} = corfurl_sequencer:get_tails(SequencerPid,
                                                  [StreamNum]),
    fetch_unread_pages2(LastLPN, State).

fetch_unread_pages(LastLPN, State) ->
    fetch_unread_pages2(LastLPN, State).

fetch_unread_pages2(LastLPN,
                    #state{proj=Proj, stream_num=StreamNum,
                          last_read_lpn=StopAtLPN} = State) ->
    {BackPs, Pages} = fetch_unread_pages(Proj, LastLPN, StopAtLPN, StreamNum),
    {Pages, State#state{last_read_lpn=LastLPN, back_ps=BackPs}}.

fetch_unread_pages(Proj, LastLPN, StopAtLPN, StreamNum) ->
    LPNandPages = tango:scan_backward(Proj, StreamNum, LastLPN,
                                      StopAtLPN, true),
    {LPNs, Pages} = lists:unzip(LPNandPages),
    BackPs = lists:foldl(fun(P, BPs) -> tango:add_back_pointer(BPs, P) end,
                         [], LPNs),
    {BackPs, Pages}.

play_log_pages(Pages, SideEffectsP,
               #state{cb_mod=CallbackMod, i_state=I_State} = State) ->
    I_State2 = play_log_pages(Pages, I_State, CallbackMod, SideEffectsP),
    State#state{i_state=I_State2}.

play_log_pages(Pages, I_State, CallbackMod, SideEffectsP) ->
    CallbackMod:play_log_mutate_i_state(Pages, SideEffectsP, I_State).

roll_log_forward(State) ->
    {Pages, State2} = fetch_unread_pages(State),
    play_log_pages(Pages, true, State2).

roll_log_forward(MaybeStartingLPN, State) ->
    {Pages, State2} = fetch_unread_pages(MaybeStartingLPN, State),
    play_log_pages(Pages, true, State2).

