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

-module(corfurl_sequencer).

-behaviour(gen_server).

-export([start_link/1, stop/1, get/2]).
-ifdef(TEST).
-export([start_link/2]).
-compile(export_all).
-endif.

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.
-endif.

-define(SERVER, ?MODULE).

start_link(FLUs) ->
    start_link(FLUs, standard).

start_link(FLUs, SeqType) ->
    gen_server:start_link(?MODULE, {FLUs, SeqType}, []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

get(Pid, NumPages) ->
    {LPN, LC} = gen_server:call(Pid, {get, NumPages, lclock_get()},
                                infinity),
    lclock_update(LC),
    LPN.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

init({FLUs, TypeOrSeed}) ->
    lclock_init(),
    MLP = get_max_logical_page(FLUs),
    if TypeOrSeed == standard ->
            {ok, MLP + 1};
       true ->
            {Seed, BadPercent, MaxDifference} = TypeOrSeed,
            random:seed(Seed),
            {ok, {MLP+1, BadPercent, MaxDifference}}
    end.

handle_call({get, NumPages, LC}, _From, MLP) when is_integer(MLP) ->
    NewLC = lclock_update(LC),
    {reply, {MLP, NewLC}, MLP + NumPages};
handle_call({get, NumPages, LC}, _From, {MLP, BadPercent, MaxDifference}) ->
    NewLC = lclock_update(LC),
    Fudge = case random:uniform(100) of
                N when N < BadPercent ->
                    random:uniform(MaxDifference * 2) - MaxDifference;
                _ ->
                    0
            end,
    {reply, {erlang:max(1, MLP + Fudge), NewLC},
     {MLP + NumPages, BadPercent, MaxDifference}};
handle_call(stop, _From, MLP) ->
    {stop, normal, ok, MLP};
handle_call(_Request, _From, MLP) ->
    Reply = whaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,
    {reply, Reply, MLP}.

handle_cast(_Msg, MLP) ->
    {noreply, MLP}.

handle_info(_Info, MLP) ->
    {noreply, MLP}.

terminate(_Reason, _MLP) ->
    %% io:format(user, "C=~w,", [lclock_get()]),
    ok.

code_change(_OldVsn, MLP, _Extra) ->
    {ok, MLP}.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

get_max_logical_page(FLUs) ->
    lists:max([proplists:get_value(max_logical_page, Ps, 0) ||
                  FLU <- FLUs,
                  {ok, Ps} <- [corfurl_flu:status(FLU)]]).

-ifdef(PULSE).

lclock_init() ->
    lamport_clock:init().

lclock_get() ->
    lamport_clock:get().

lclock_update(LC) ->
    lamport_clock:update(LC).

-else.  % PULSE

lclock_init() ->
    ok.

lclock_get() ->
    ok.

lclock_update(_LC) ->
    ok.

-endif. % PLUSE
