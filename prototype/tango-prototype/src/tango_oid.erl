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
          page_size :: non_neg_integer(),
          seq :: pid(),
          proj :: term(),
          last_read_lpn :: lpn(),
          last_write_lpn :: lpn(),
          back_ps :: [lpn()],
          i_state :: orddict:orddict()
         }).

start_link(PageSize, SequencerPid, Proj) ->
    gen_server:start_link(?MODULE, [PageSize, SequencerPid, Proj], []).

stop(Pid) ->
    gen_server:call(Pid, {stop}, ?LONG_TIME).

put(Pid, Key, Val) ->
    gen_server:call(Pid, {put, Key, Val}, ?LONG_TIME).

get(Pid, Key) ->
    gen_server:call(Pid, {get, Key}, ?LONG_TIME).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([PageSize, SequencerPid, Proj]) ->
    LastLPN = find_last_lpn(SequencerPid),
    {BackPs, Pages} = fetch_unread_pages(Proj, LastLPN, 0),
    Dict = play_log_pages(Pages, orddict:new()),
    {ok, #state{page_size=PageSize,
                seq=SequencerPid,
                proj=Proj,
                last_read_lpn=LastLPN,
                last_write_lpn=LastLPN,
                back_ps=BackPs,
                i_state=Dict}}.

handle_call({put, Key, Val}, _From,
            #state{proj=Proj0, page_size=PageSize, back_ps=BackPs} = State) ->
    Page = term_to_binary({put, Key, Val}),
    FullPage = tango:pack_v1([{?OID_STREAM_NUMBER, BackPs}], Page, PageSize),
    {{ok, LPN}, Proj1} = corfurl_client:append_page(Proj0, FullPage),
    NewBackPs = tango:add_back_pointer(BackPs, LPN),
    {reply, garrr, State#state{proj=Proj1,
                               last_write_lpn=LPN,
                               back_ps=NewBackPs}};
handle_call({get, Key}, _From, State) ->
    {Pages, State2} = fetch_unread_pages(State),
    %% ?D(Pages),
    #state{i_state=Dict} = State3 = play_log_pages(Pages, State2),
    {reply, orddict:find(Key, Dict), State3};
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

play_log_pages(Pages, #state{i_state=Dict} = State) ->
    Dict2 = play_log_pages(Pages, Dict),
    State#state{i_state=Dict2};
play_log_pages(Pages, Dict) ->
    %% ?D({play_log_pages, Pages}),
    lists:foldl(fun({put, K, V}=_Op, D) ->
                        %% ?D(_Op),
                        orddict:store(K, V, D)
                end,
                Dict,
                [binary_to_term(Page) || Page <- Pages]).
