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

-export([start_link/1, start_link/2, start_link/3,
         stop/1, stop/2,
         get/2, get_tails/3]).
-export([set_tails/2]).
-ifdef(TEST).
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
%% -define(LONG_TIME, 30*1000).
-define(LONG_TIME, 5*1000).

-define(D(X), io:format(user, "Dbg: ~s =\n  ~p\n", [??X, X])).

start_link(FLUs) ->
    start_link(FLUs, standard).

start_link(FLUs, SeqType) ->
    start_link(FLUs, SeqType, ?SERVER).

start_link(FLUs, SeqType, RegName) ->
    case gen_server:start_link({local, RegName}, ?MODULE, {FLUs, SeqType},[]) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        Else ->
            Else
    end.

stop(Pid) ->
    stop(Pid, stop).

stop(Pid, Method) ->
    Res = gen_server:call(Pid, stop, infinity),
    if Method == kill ->
            %% Emulate gen.erl's client-side behavior when the server process
            %% is killed.
            exit(killed);
       true ->
            Res
    end.

get(Pid, NumPages) ->
    {LPN, LC} = gen_server:call(Pid, {get, NumPages, lclock_get()},
                                ?LONG_TIME),
    lclock_update(LC),
    LPN.

get_tails(Pid, NumPages, StreamList) ->
    {Tails, LC} = gen_server:call(Pid,
                                {get_tails, NumPages, StreamList, lclock_get()},
                                ?LONG_TIME),
    lclock_update(LC),
    Tails.

set_tails(Pid, StreamTails) ->
    ok = gen_server:call(Pid, {set_tails, StreamTails}, ?LONG_TIME).

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

init({FLUs, TypeOrSeed}) ->
    lclock_init(),
    MLP = get_max_logical_page(FLUs),
    Tab = ets:new(?MODULE, [set, private, {keypos, 1}]),
    if TypeOrSeed == standard ->
            {ok, {Tab, MLP + 1}};
       true ->
            {Seed, BadPercent, MaxDifference} = TypeOrSeed,
            random:seed(Seed),
            {ok, {Tab, MLP+1, BadPercent, MaxDifference}}
    end.

handle_call({get, NumPages, LC}, _From, {Tab, MLP}) ->
    NewLC = lclock_update(LC),
    {reply, {{ok, MLP}, NewLC}, {Tab, MLP + NumPages}};
handle_call({get, NumPages, LC}, _From,
            {Tab, MLP, BadPercent, MaxDifference}) ->
    NewLC = lclock_update(LC),
    Fudge = case random:uniform(100) of
                N when N < BadPercent ->
                    random:uniform(MaxDifference * 2) - MaxDifference;
                _ ->
                    0
            end,
    {reply, {{ok, erlang:max(1, MLP + Fudge)}, NewLC},
     {Tab, MLP + NumPages, BadPercent, MaxDifference}};
handle_call({get_tails, NumPages, StreamList, LC}, _From, MLP_tuple) ->
    Tab = element(1, MLP_tuple),
    MLP = element(2, MLP_tuple),
    Tails = [case (catch ets:lookup_element(Tab, Stream, 2)) of
                 {'EXIT', _} ->
                     [];
                 Res ->
                     Res
             end || Stream <- StreamList],
    if NumPages > 0 ->
            update_stream_tails(Tab, StreamList, MLP);
       true ->
            ok
    end,
    NewLC = lclock_update(LC),
    {reply, {{ok, MLP, Tails}, NewLC},
     setelement(2, MLP_tuple, MLP + NumPages)};
handle_call({set_tails, StreamTails}, _From, MLP_tuple) ->
    Tab = element(1, MLP_tuple),
    true = ets:delete_all_objects(Tab),
    [ets:insert(Tab, {Stream, Tail}) || {Stream, Tail} <- StreamTails],
    {reply, ok, MLP_tuple};
handle_call(stop, _From, MLP) ->
    {stop, normal, ok, MLP};
handle_call(_Request, _From, MLP) ->
    Reply = idunnoooooooooooooooooooooooooo,
    {reply, Reply, MLP}.

handle_cast(_Msg, MLP) ->
    {noreply, MLP}.

handle_info(_Info, MLP) ->
    {noreply, MLP}.

terminate(_Reason, _MLP) ->
    ok.

code_change(_OldVsn, MLP, _Extra) ->
    {ok, MLP}.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

get_max_logical_page(FLUs) ->
    lists:max([proplists:get_value(max_logical_page, Ps, 0) ||
                  FLU <- FLUs,
                  {ok, Ps} <- [corfurl_flu:status(FLU)]]).

update_stream_tails(Tab, StreamList, LPN) ->
    [begin
         OldBackPs = try   ets:lookup_element(Tab, Stream, 2)
                     catch error:badarg -> []
                     end,
         NewBackPs = add_back_pointer(OldBackPs, LPN),
         ets:insert(Tab, {Stream, NewBackPs})
     end || Stream <- StreamList].

add_back_pointer([D,C,B,_A|_], New) ->
    [New,D,C,B];
add_back_pointer([], New) ->
    [New];
add_back_pointer(BackPs, New) ->
    [New|BackPs].

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
