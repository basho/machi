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

%% Tango datatype callbacks (prototype)
-export([fresh/0,
         do_pure_op/2, do_dirty_op/6, play_log_mutate_i_state/3]).

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

-callback fresh() -> callback_i_state().
-callback do_pure_op(term(), callback_i_state()) -> term().
-callback do_dirty_op(term(), callback_i_state(),
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

handle_call({cb_dirty_op, Op}, _From,
            #state{proj=Proj0, cb_mod=CallbackMod, stream_num=StreamNum,
                   page_size=PageSize, back_ps=BackPs, i_state=I_State}=State) ->
    {_Res, I_State2, Proj1, _LPN, NewBackPs} =
        CallbackMod:do_dirty_op(Op, I_State, StreamNum,
                                Proj0, PageSize, BackPs),
    {reply, ok, State#state{i_state=I_State2,
                            proj=Proj1,
                            back_ps=NewBackPs}};
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

fetch_unread_pages(#state{seq=SequencerPid, proj=Proj, stream_num=StreamNum,
                          last_read_lpn=StopAtLPN} = State) ->
    {ok, [LastLPN]} = corfurl_sequencer:get_tails(SequencerPid,
                                                  [StreamNum]),
    {BackPs, Pages} = fetch_unread_pages(Proj, LastLPN, StopAtLPN, StreamNum),
    %% TODO ????
    %% LastReadLPN = if BackPs == [] -> 0;
    %%                  true         -> hd(BackPs)
    %%               end,
    {Pages, State#state{last_read_lpn=LastLPN, back_ps=BackPs}}.

fetch_unread_pages(Proj, LastLPN, StopAtLPN, StreamNum) ->
    %% ?D({fetch_unread_pages, LastLPN, StopAtLPN}),
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fresh() ->
    undefined.

do_pure_op({o_get}, Register) ->
    Register.

do_dirty_op({o_set, _Val}=Op,
            I_State, StreamNum, Proj0, PageSize, BackPs) ->
    Page = term_to_binary(Op),
    FullPage = tango:pack_v1([{StreamNum, BackPs}], Page, PageSize),
    {{ok, LPN}, Proj1} = corfurl_client:append_page(Proj0, FullPage,
                                                    [StreamNum]),
    NewBackPs = tango:add_back_pointer(BackPs, LPN),
    {ok, I_State, Proj1, LPN, NewBackPs}.

play_log_mutate_i_state(Pages, _SideEffectsP, I_State) ->
    lists:foldl(fun({o_set, Val}=_Op, _OldVal) ->
                        Val
                end,
                I_State,
                [binary_to_term(Page) || Page <- Pages]).
    
