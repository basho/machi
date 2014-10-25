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
-module(machi_chain_manager_test).

-export([]).

-ifdef(TEST).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-record(s, {
          step = 0 :: non_neg_integer(),
          seed :: {integer(), integer(), integer()},
          comm_weights = [] :: list()
         }).

gen_all_nodes() ->
    [a, b, c].

gen_rand_seed() ->
    noshrink({gen_num(), gen_num(), gen_num()}).

gen_num() ->
    ?LET(I, oneof([int(), largeint()]),
         erlang:abs(I)).

gen_communicating_weights(Nodes) ->
    Pairs = make_all_pairs(Nodes),
    Num = length(Pairs),
    ?LET(Weights, vector(Num, choose(1, 100)),
         lists:zip(Weights, Pairs)).

%% gen_communicating_nodes(Nodes) ->
%%     ?LET(Pairs, make_all_pairs(Nodes),
%%          frequency([{10, Pairs},
%%                     { 8, gen_take_some(Pairs)}])).

%% gen_take_some(L) ->
%%     Num = length(L),
%%     ?LET({Weights, Cutoff}, {vector(Num, choose(1, 100)), choose(1, 100)},
%%          [X || {Weight, X} <- lists:zip(Weights, L),
%%                Weight < Cutoff]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    #s{}.

command(#s{step=0}) ->
    L = gen_all_nodes(),
    {call, ?MODULE, init_run, [gen_rand_seed(), gen_communicating_weights(L)]};
command(_S) ->
    foo.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

prop_m() ->
    ?FORALL({_Cmds, _PulseSeed}, {commands(?MODULE), pulse:seed()},
            begin
                true
            end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_all_pairs(L) ->
    lists:flatten(make_all_pairs2(lists:usort(L))).

make_all_pairs2([]) ->
    [];
make_all_pairs2([_]) ->
    [];
make_all_pairs2([H1|T]) ->
    [[{H1, X}, {X, H1}] || X <- T] ++ make_all_pairs(T).
    %% [{H1, H2}, {H2, H1}|make_all_pairs([H2|T])].
