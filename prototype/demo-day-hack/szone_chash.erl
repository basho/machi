%%%-------------------------------------------------------------------
%%% Copyright (c) 2007-2011 Gemini Mobile Technologies, Inc.  All rights reserved.
%%% Copyright (c) 2013-2014 Basho Technologies, Inc.  All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%%-------------------------------------------------------------------

-module(szone_chash).

-define(SMALLEST_SIGNIFICANT_FLOAT_SIZE, 0.1e-12).

%% -compile(export_all).
-export([make_float_map/1, make_float_map/2,
         sum_map_weights/1,
         make_tree/1,
         make_tree/2,
         query_tree/2,
         hash_string/2,
         pretty_with_integers/2,
         pretty_with_integers/3]).
-export([make_demo_map1/0, make_demo_map2/0]).

-type map_name() :: term().
-type float_map() :: [{map_name(), float()}].

-export_type([float_map/0]).

make_float_map(NewChainWeights) ->
    make_float_map([], NewChainWeights).

make_float_map([], NewChainWeights) ->
    Sum = add_all_weights(NewChainWeights),
    DiffMap = [{Ch, Wt/Sum} || {Ch, Wt} <- NewChainWeights],
    make_float_map2([{unused, 1.0}], DiffMap, NewChainWeights);
make_float_map(OldFloatMap, NewChainWeights) ->
    NewSum = add_all_weights(NewChainWeights),
    %% Normalize to unit interval
    %% NewChainWeights2 = [{Ch, Wt / NewSum} || {Ch, Wt} <- NewChainWeights],

    %% Reconstruct old chain weights (will be normalized to unit interval)
    SumOldFloatsDict =
        lists:foldl(fun({Ch, Wt}, OrdDict) ->
                            orddict:update_counter(Ch, Wt, OrdDict)
                    end, orddict:new(), OldFloatMap),
    OldChainWeights = orddict:to_list(SumOldFloatsDict),
    OldSum = add_all_weights(OldChainWeights),

    OldChs = [Ch || {Ch, _} <- OldChainWeights],
    NewChs = [Ch || {Ch, _} <- NewChainWeights],
    OldChsOnly = OldChs -- NewChs,

    %% Mark any space in by a deleted chain as unused.
    OldFloatMap2 = lists:map(
                     fun({Ch, Wt} = ChWt) ->
                                 case lists:member(Ch, OldChsOnly) of
                                     true  ->
                                         {unused, Wt};
                                     false ->
                                         ChWt
                                 end
                     end, OldFloatMap),

    %% Create a diff map of changing chains and added chains
    DiffMap = lists:map(fun({Ch, NewWt}) ->
                                case orddict:find(Ch, SumOldFloatsDict) of
                                    {ok, OldWt} ->
                                        {Ch, (NewWt / NewSum) -
                                             (OldWt / OldSum)};
                                    error ->
                                        {Ch, NewWt / NewSum}
                                end
                        end, NewChainWeights),
    make_float_map2(OldFloatMap2, DiffMap, NewChainWeights).

make_float_map2(OldFloatMap, DiffMap, _NewChainWeights) ->
    FloatMap = apply_diffmap(DiffMap, OldFloatMap),
    XX = combine_neighbors(collapse_unused_in_float_map(FloatMap)),
    XX.

apply_diffmap(DiffMap, FloatMap) ->
    SubtractDiff = [{Ch, abs(Diff)} || {Ch, Diff} <- DiffMap, Diff < 0],
    AddDiff = [D || {_Ch, Diff} = D <- DiffMap, Diff > 0],
    TmpFloatMap = iter_diffmap_subtract(SubtractDiff, FloatMap),
    iter_diffmap_add(AddDiff, TmpFloatMap).

add_all_weights(ChainWeights) ->
    lists:foldl(fun({_Ch, Weight}, Sum) -> Sum + Weight end, 0.0, ChainWeights).

iter_diffmap_subtract([{Ch, Diff}|T], FloatMap) ->
    iter_diffmap_subtract(T, apply_diffmap_subtract(Ch, Diff, FloatMap));
iter_diffmap_subtract([], FloatMap) ->
    FloatMap.

iter_diffmap_add([{Ch, Diff}|T], FloatMap) ->
    iter_diffmap_add(T, apply_diffmap_add(Ch, Diff, FloatMap));
iter_diffmap_add([], FloatMap) ->
    FloatMap.

apply_diffmap_subtract(Ch, Diff, [{Ch, Wt}|T]) ->
    if Wt == Diff ->
            [{unused, Wt}|T];
       Wt > Diff ->
            [{Ch, Wt - Diff}, {unused, Diff}|T];
       Wt < Diff ->
            [{unused, Wt}|apply_diffmap_subtract(Ch, Diff - Wt, T)]
    end;
apply_diffmap_subtract(Ch, Diff, [H|T]) ->
    [H|apply_diffmap_subtract(Ch, Diff, T)];
apply_diffmap_subtract(_Ch, _Diff, []) ->
    [].

apply_diffmap_add(Ch, Diff, [{unused, Wt}|T]) ->
    if Wt == Diff ->
            [{Ch, Wt}|T];
       Wt > Diff ->
            [{Ch, Diff}, {unused, Wt - Diff}|T];
       Wt < Diff ->
            [{Ch, Wt}|apply_diffmap_add(Ch, Diff - Wt, T)]
    end;
apply_diffmap_add(Ch, Diff, [H|T]) ->
    [H|apply_diffmap_add(Ch, Diff, T)];
apply_diffmap_add(_Ch, _Diff, []) ->
    [].

combine_neighbors([{Ch, Wt1}, {Ch, Wt2}|T]) ->
    combine_neighbors([{Ch, Wt1 + Wt2}|T]);
combine_neighbors([H|T]) ->
    [H|combine_neighbors(T)];
combine_neighbors([]) ->
    [].

collapse_unused_in_float_map([{Ch, Wt1}, {unused, Wt2}|T]) ->
    collapse_unused_in_float_map([{Ch, Wt1 + Wt2}|T]);
collapse_unused_in_float_map([{unused, _}] = L) ->
    L;                                          % Degenerate case only
collapse_unused_in_float_map([H|T]) ->
    [H|collapse_unused_in_float_map(T)];
collapse_unused_in_float_map([]) ->
    [].

chash_float_map_to_nextfloat_list(FloatMap) when length(FloatMap) > 0 ->
    %% QuickCheck found a bug ... need to weed out stuff smaller than
    %% ?SMALLEST_SIGNIFICANT_FLOAT_SIZE here.
    FM1 = [P || {_X, Y} = P <- FloatMap, Y > ?SMALLEST_SIGNIFICANT_FLOAT_SIZE],
    {_Sum, NFs0} = lists:foldl(fun({Name, Amount}, {Sum, List}) ->
                                       {Sum+Amount, [{Sum+Amount, Name}|List]}
                               end, {0, []}, FM1),
    lists:reverse(NFs0).

chash_nextfloat_list_to_gb_tree([]) ->
    gb_trees:balance(gb_trees:from_orddict([]));
chash_nextfloat_list_to_gb_tree(NextFloatList) ->
    {_FloatPos, Name} = lists:last(NextFloatList),
    %% QuickCheck found a bug ... it really helps to add a catch-all item
    %% at the far "right" of the list ... 42.0 is much greater than 1.0.
    NFs = NextFloatList ++ [{42.0, Name}],
    gb_trees:balance(gb_trees:from_orddict(orddict:from_list(NFs))).

chash_gb_next(X, {_, GbTree}) ->
    chash_gb_next1(X, GbTree).

chash_gb_next1(X, {Key, Val, Left, _Right}) when X < Key ->
    case chash_gb_next1(X, Left) of
        nil ->
            {Key, Val};
        Res ->
            Res
    end;
chash_gb_next1(X, {Key, _Val, _Left, Right}) when X >= Key ->
    chash_gb_next1(X, Right);
chash_gb_next1(_X, nil) ->
    nil.

%% @type float_map() = list({brick(), float()}).  A float_map is a
%% definition of brick assignments over the unit interval [0.0, 1.0].
%% The sum of all floats must be 1.0.
%% For example, [{{br1, nd1}, 0.25}, {{br2, nd1}, 0.5}, {{br3, nd1}, 0.25}].

%% @type nextfloat_list() = list({float(), brick()}).  A nextfloat_list
%% differs from a float_map in two respects: 1) nextfloat_list contains
%% tuples with the brick name in 2nd position, 2) the float() at each
%% position I_n > I_m, for all n, m such that n > m.
%% For example, a nextfloat_list of the float_map example above,
%% [{0.25, {br1, nd1}}, {0.75, {br2, nd1}}, {1.0, {br3, nd1}].

%% @doc Not used directly, but can give a developer an idea of how well
%% chash_float_map_to_nextfloat_list will do for a given value of Max.
%%
%% For example:
%% <verbatim>
%%     NewFloatMap = make_float_map([{unused, 1.0}],
%%                                        [{a,100}, {b, 100}, {c, 10}]),
%%     ChashMap = chash_scale_to_int_interval(NewFloatMap, 100),
%%     io:format("QQQ: int int = ~p\n", [ChashIntInterval]),
%% -> [{a,1,47},{b,48,94},{c,94,100}]
%% </verbatim>
%%
%% Interpretation: out of the 100 slots:
%% <ul>
%% <li> 'a' uses the slots 1-47 </li>
%% <li> 'b' uses the slots 48-94 </li>
%% <li> 'c' uses the slots 95-100 </li>
%% </ul>

chash_scale_to_int_interval(NewFloatMap, Max) ->
    chash_scale_to_int_interval(NewFloatMap, 0, Max).

chash_scale_to_int_interval([{Ch, _Wt}], Cur, Max) ->
    [{Ch, Cur, Max}];
chash_scale_to_int_interval([{Ch, Wt}|T], Cur, Max) ->
    Int = trunc(Wt * Max),
    [{Ch, Cur + 1, Cur + Int}|chash_scale_to_int_interval(T, Cur + Int, Max)].

%%%%%%%%%%%%%

pretty_with_integers(Map, Scale) ->
    chash_scale_to_int_interval(Map, Scale).

pretty_with_integers(OldWeights, NewWeights, Scale) ->
    chash_scale_to_int_interval(
      make_float_map(make_float_map(OldWeights),
                           NewWeights),
      Scale).

make_tree(Map) ->
    chash_nextfloat_list_to_gb_tree(
      chash_float_map_to_nextfloat_list(Map)).

make_tree(OldWeights, NewWeights) ->
    chash_nextfloat_list_to_gb_tree(
      chash_float_map_to_nextfloat_list(
        make_float_map(make_float_map(OldWeights), NewWeights))).

query_tree(Val, Tree) when is_float(Val), 0.0 =< Val, Val =< 1.0 ->
    chash_gb_next(Val, Tree).

make_demo_map1() ->
    {_, Res} = make_demo_map1_i(),
    Res.

make_demo_map1_i() ->
    Fail1 = {b, 100},
    L1 = [{a, 100}, Fail1, {c, 100}],
    L2 = L1 ++ [{d, 100}, {e, 100}],
    L3 = L2 -- [Fail1],
    L4 = L3 ++ [{giant, 300}],
    {L4, lists:foldl(fun(New, Old) -> make_float_map(Old, New) end,
                     make_float_map(L1), [L2, L3, L4])}.

make_demo_map2() ->
    {L0, _} = make_demo_map1_i(),
    L1 = L0 ++ [{h, 100}],
    L2 = L1 ++ [{i, 100}],
    L3 = L2 ++ [{j, 100}],
    lists:foldl(fun(New, Old) -> make_float_map(Old, New) end,
                make_demo_map1(), [L1, L2, L3]).

sum_map_weights(Map) ->
    L = sum_map_weights(lists:sort(Map), undefined, 0.0) -- [{undefined,0.0}],
    WeightSum = lists:sum([Weight || {_, Weight} <- L]),
    {{per_szone, L}, {weight_sum, WeightSum}}.

sum_map_weights([{SZ, Weight}|T], SZ, SZ_total) ->
    sum_map_weights(T, SZ, SZ_total + Weight);
sum_map_weights([{SZ, Weight}|T], LastSZ, LastSZ_total) ->
    [{LastSZ, LastSZ_total}|sum_map_weights(T, SZ, Weight)];
sum_map_weights([], LastSZ, LastSZ_total) ->
    [{LastSZ, LastSZ_total}].

-define(SHA_MAX, (1 bsl (20*8))).

%% Optimization is certainly possible here.

hash_string(Key, Map) ->
    catch crypto:start(),
    Tree = make_tree(Map),
    <<Int:(20*8)/unsigned>> = crypto:hash(sha, Key),
    Float = Int / ?SHA_MAX,
    query_tree(Float, Tree).

%%%%% Examples

%% %% Make a map.  See the code for make_demo_map1() for the order of
%% %% additions & deletions.  Here's a brief summary of the 4 steps.
%% %%
%% %%   * 'a' through 'e' are weighted @ 100.
%% %%   * 'giant' is weighted @ 300.
%% %%   * 'b' is removed at step #3.

%% 40> M1 = szone_chash:make_demo_map1().
%% [{a,0.09285714285714286},
%%  {giant,0.10714285714285715},
%%  {d,0.026190476190476153},
%%  {giant,0.10714285714285715},
%%  {a,0.04999999999999999},
%%  {giant,0.04999999999999999},
%%  {d,0.04999999999999999},
%%  {giant,0.050000000000000044},
%%  {d,0.06666666666666671},
%%  {e,0.009523809523809434},
%%  {giant,0.05714285714285716},
%%  {c,0.14285714285714285},
%%  {giant,0.05714285714285716},
%%  {e,0.13333333333333341}]


%% %% Map M1 onto the interval of integers 0-10,1000
%% %%
%% %% output = list({SZ_name::term(), Start::integer(), End::integer()})

%% 41> szone_chash:pretty_with_integers(M1, 10*1000).
%% [{a,1,928},
%%  {giant,929,1999},
%%  {d,2000,2260},
%%  {giant,2261,3331},
%%  {a,3332,3830},
%%  {giant,3831,4329},
%%  {d,4330,4828},
%%  {giant,4829,5328},
%%  {d,5329,5994},
%%  {e,5995,6089},
%%  {giant,6090,6660},
%%  {c,6661,8088},
%%  {giant,8089,8659},
%%  {e,8659,10000}]

%% %% Sum up all of the weights, make sure it's what we expect:

%% 55> szone_chash:sum_map_weights(M1).
%% {{per_szone,[{a,0.14285714285714285},
%%              {c,0.14285714285714285},
%%              {d,0.14285714285714285},
%%              {e,0.14285714285714285},
%%              {giant,0.42857142857142866}]},
%%  {weight_sum,1.0}}

%% %% Make a tree, then query it
%% %%     (Hash::float(), tree()) -> {NextLargestBoundary::float(), szone()}

%% 58> T1 = szone_chash:make_tree(M1).
%% 59> szone_chash:query_tree(0.2555, T1).
%% {0.3333333333333333,giant}
%% 60> szone_chash:query_tree(0.3555, T1).
%% {0.3833333333333333,a}
%% 61> szone_chash:query_tree(0.4555, T1).
%% {0.4833333333333333,d}

%% %% How about hashing a bunch of strings and see what happens?

%% 74> Key1 = "Hello, world!".
%% "Hello, world!"
%% 75> [{K, element(2, szone_chash:hash_string(K, M1))} || K <- [lists:sublist(Key1, X) || X <- lists:seq(1, length(Key1))]].
%% [{"H",giant},
%%  {"He",giant},
%%  {"Hel",giant},
%%  {"Hell",e},
%%  {"Hello",e},
%%  {"Hello,",giant},
%%  {"Hello, ",e},
%%  {"Hello, w",e},
%%  {"Hello, wo",giant},
%%  {"Hello, wor",d},
%%  {"Hello, worl",giant},
%%  {"Hello, world",e},
%%  {"Hello, world!",d}]
