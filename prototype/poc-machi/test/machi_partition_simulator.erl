%% -------------------------------------------------------------------
%%
%% Machi: a small village of replicated files
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
-module(machi_partition_simulator).

-behaviour(gen_server).

-ifdef(TEST).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-export([start_link/3, stop/0,
         get/1, reset_thresholds/2, no_partitions/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TAB, ?MODULE).

-record(state, {
          seed,
          old_partitions,
          old_threshold,
          no_partition_threshold
         }).

start_link(Seed, OldThreshold, NoPartitionThreshold) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          {Seed, OldThreshold, NoPartitionThreshold}, []).

stop() ->
    gen_server:call(?MODULE, {stop}, infinity).

get(Nodes) ->
    gen_server:call(?MODULE, {get, Nodes}, infinity).

reset_thresholds(OldThreshold, NoPartitionThreshold) ->
    gen_server:call(?MODULE, {reset_thresholds, OldThreshold, NoPartitionThreshold}, infinity).

no_partitions() ->
    reset_thresholds(-999, 999).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Seed, OldThreshold, NoPartitionThreshold}) ->
    {ok, #state{seed=Seed,
                old_partitions={[],[[]]},
                old_threshold=OldThreshold,
                no_partition_threshold=NoPartitionThreshold}}.

handle_call({get, Nodes}, _From, S) ->
    {Seed2, Partitions} =
        calc_network_partitions(Nodes,
                                S#state.seed,
                                S#state.old_partitions,
                                S#state.old_threshold,
                                S#state.no_partition_threshold),
    {reply, Partitions, S#state{seed=Seed2,
                                old_partitions=Partitions}};
handle_call({reset_thresholds, OldThreshold, NoPartitionThreshold}, _From, S) ->
    {reply, ok, S#state{old_threshold=OldThreshold,
                        no_partition_threshold=NoPartitionThreshold}};
handle_call({stop}, _From, S) ->
    {stop, normal, ok, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

calc_network_partitions(Nodes, Seed1, OldPartition,
                        OldThreshold, NoPartitionThreshold) ->
    {Cutoff2, Seed2} = random:uniform_s(100, Seed1),
    if Cutoff2 < OldThreshold ->
            {Seed2, OldPartition};
       true ->
            {Cutoff3, Seed3} = random:uniform_s(100, Seed1),
            if Cutoff3 < NoPartitionThreshold ->
                    {Seed3, {[], [Nodes]}};
               true ->
                    make_network_partition_locations(Nodes, Seed3)
            end
    end.

make_network_partition_locations(Nodes, Seed1) ->
    %% TODO: To simplify debugging a bit, I'm switching to partitions that are
    %%       bi-directional only.
    Num = length(Nodes),
    {Seed2, WeightsNodes} = lists:foldl(
                              fun(Node, {Seeda, Acc}) ->
                                      {Cutoff0, Seedb} =
                                                  random:uniform_s(100, Seeda),
                                      Cutoff = erlang:max(
                                                 2, if Cutoff0 rem 4 == 0 ->
                                                            0;
                                                       true ->
                                                            Cutoff0
                                                    end),
                                      {Seedb, [{Cutoff, Node}|Acc]}
                              end, {Seed1, []}, Nodes),
    IslandSep = 100 div Num,
    Islands = [
               lists:sort([Nd || {Weight, Nd} <- WeightsNodes,
                                 (Max - IslandSep) =< Weight, Weight < Max])
               || Max <- lists:seq(IslandSep + 1, 105, IslandSep)],
    {Seed2, {lists:usort(islands2partitions(Islands)), lists:sort(Islands)}}.

islands2partitions([]) ->
    [];
islands2partitions([Island|Rest]) ->
    [{X,Y} || X <- Island,
              Y <- lists:append(Rest), X /= Y]
    ++
    [{Y,X} || X <- Island,
              Y <- lists:append(Rest), X /= Y]
    ++
    islands2partitions(Rest).



-endif. % TEST
