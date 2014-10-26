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
          seed :: {integer(), integer(), integer()}
         }).

gen_all_nodes() ->
    [a, b, c].

gen_rand_seed() ->
    noshrink({gen_num(), gen_num(), gen_num()}).

gen_num() ->
    ?LET(I, oneof([int(), largeint()]),
         erlang:abs(I)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    #s{}.

command(#s{step=0}) ->
    L = gen_all_nodes(),
    {call, ?MODULE, init_run, [gen_rand_seed(), L]};
command(_S) ->
    foo.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

prop_m() ->
    ?FORALL({_Cmds, _PulseSeed}, {commands(?MODULE), pulse:seed()},
            begin
                true
            end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

