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
-module(machi_chain_manager0_test).

-include("machi.hrl").

-define(MGR, machi_chain_manager0).
-define(D(X), io:format(user, "~s ~p\n", [??X, X])).

-export([]).

-ifdef(TEST).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
%% -include_lib("eqc/include/eqc_statem.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

smoke0_test() ->
    S0 = ?MGR:make_initial_state(a, [a,b,c,d], {4,5,6}),
    lists:foldl(fun(_, S) ->
                        {P1, S1} = ?MGR:calc_projection(20, 1, S),
                        %% io:format(user, "~p\n", [?MGR:make_projection_summary(P1)]),
                        S1#ch_mgr{proj=P1}
                end, S0, lists:seq(1,10)).

-ifdef(CRUFT_DELETE_ME_MAYBE).

-record(s, {
          step = 0 :: non_neg_integer(),
          seed :: {integer(), integer(), integer()}
         }).

gen_all_nodes() ->
    [a, b, c].

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

-endif. % CRUFT_DELETE_ME_MAYBE

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_rand_seed() ->
    noshrink({gen_num(), gen_num(), gen_num()}).

gen_num() ->
    choose(1, 50000).

prop_calc_projection() ->
    prop_calc_projection([a,b,c], false).

prop_calc_projection(Nodes, TrackUseTab) ->
    ?FORALL(
       {Seed, OldThreshold, NoPartitionThreshold, Steps, HeadNode},
       {gen_rand_seed(),
        oneof([0,15,35,55,75,85,100]),
        oneof([0,15,35,55,75,85,100]),
        5000,
        oneof(Nodes)},
       begin
           erase(goofus),
           {RandN, _} = random:uniform_s(length(Nodes), Seed),
           NodesShuffle = lists:nth(RandN, perms(Nodes)),
           S0 = ?MGR:make_initial_state(HeadNode, NodesShuffle, Seed),
           F = fun(_, {S, Acc}) ->
                       {P1, S1} = ?MGR:calc_projection(
                                    OldThreshold, NoPartitionThreshold, S),
                       %%io:format(user, "~p\n", [make_projection_summary(P1)]),
                       {S1#ch_mgr{proj=P1}, [P1|Acc]}
               end,
           {_, Projs0} = lists:foldl(F, {S0, []}, lists:seq(1,Steps)),
           Projs = lists:reverse(Projs0),
           if TrackUseTab == false ->
                   ok;
              true ->
                   Transitions = extract_upi_transitions(Projs),
                   %% io:format(user, "\n~P\n", [lists:usort(Transitions), 10]),
                   [ets:update_counter(TrackUseTab, Trans, 1) ||
                       Trans <- Transitions]
           end,
           true = projection_transitions_are_sane(Projs)
       end).

extract_upi_transitions([]) ->
    [];
extract_upi_transitions([_]) ->
    [];
extract_upi_transitions([P1, P2|T]) ->
    Trans = {P1#projection.upi, P2#projection.upi},
    [Trans|extract_upi_transitions([P2|T])].

find_common_prefix([], _) ->
    [];
find_common_prefix(_, []) ->
    [];
find_common_prefix([H|L1], [H|L2]) ->
    [H|find_common_prefix(L1, L2)];
find_common_prefix(_, _) ->
    [].

calc_projection_test_() ->
    Runtime = 2, %% Runtime = 60*60,
    {timeout, Runtime * 500,
     fun() ->
             Nodes = [a,b,c],
             Cs = combinations(Nodes),
             Combos = lists:sort([{UPI1, UPI2} || UPI1 <- Cs, UPI2 <- Cs]),
             put(hack, 0),
             timer:sleep(500),
             io:format(user, "\n", []),
             Rs = [begin
                       Hack = get(hack),if Hack rem 10000 == 0 -> ?D({time(), Hack}); true -> ok end,put(hack, Hack + 1),
                       P1 = ?MGR:make_projection(2, 1, <<>>, HdNd, Nodes,
                                                 [], PsUPI1, Nodes -- PsUPI1, []),
                       P2 = ?MGR:make_projection(3, 2, <<>>, HdNd, Nodes,
                                                 [], PsUPI2, Nodes -- PsUPI2, []),
                       Res = case projection_transition_is_sane(P1, P2) of
                                 true -> true;
                                 _    -> false
                             end,
                       {Res, PsUPI1, PsUPI2}
                   end || HdNd <- Nodes,
                          {PsUPI1,PsUPI2} <- Combos,
                          %% We assume that the author appears in any UPI list
                          PsUPI1 /= [],
                          PsUPI2 /= [],
                          %% HdNd is the author for all of these
                          %% tests, so it must be present in UPI1 & UPI2
                          lists:member(HdNd, PsUPI1),
                          lists:member(HdNd, PsUPI2)],
             OKs = [begin
                        {UPI1,UPI2}
                    end || {true, UPI1, UPI2} <- Rs],
             %% io:format(user, "OKs = ~p\n", [lists:usort(OKs)]),
             Tab = ets:new(count, [public, set, {keypos, 1}]),
             [ets:insert(Tab, {Transition, 0}) || Transition <- OKs],

             true = eqc:quickcheck(
                        eqc:testing_time(Runtime,
                        ?QC_OUT(prop_calc_projection(Nodes, Tab)))),

             NotCounted = lists:sort([Transition || {Transition, 0} <- ets:tab2list(Tab)]),
             Counted = [X || {_, N}=X <- ets:tab2list(Tab),
                             N > 0],
             CountedX = lists:sort(fun(X, Y) ->
                                           element(2, X) < element(2, Y)
                                   end, Counted),
             timer:sleep(100),
             File = io_lib:format("/tmp/manager-test.~w.~w.~w",
                                  tuple_to_list(time())),
             {ok, FH} = file:open(File, [write]),
             io:format(FH, "% NotCounted =\n~p.\n", [NotCounted]),
             file:close(FH),
             %% io:format(user, "\tNotCounted = ~p\n", [NotCounted]),
             io:format(user, "\n\tNotCounted list was written to ~s\n", [File]),
             io:format(user, "\tOKs length = ~p\n", [length(OKs)]),
             io:format(user, "\tTransitions hit = ~p\n",
                       [length(OKs) - length(NotCounted)]),
             %% io:format(user, "\tTransitions = ~p\n", [lists:sort(_Counted)]),
             io:format(user, "\tNotCounted length = ~p\n",
                       [length(NotCounted)]),
             io:format(user, "\tLeast-counted transition = ~p\n",
                       [hd(CountedX)]),
             io:format(user, "\tMost-counted transition  = ~p\n",
                       [lists:last(CountedX)]),
             ok
     end}.

projection_transitions_are_sane([]) ->
    true;
projection_transitions_are_sane([_]) ->
    true;
projection_transitions_are_sane([P1, P2|T]) ->
    case projection_transition_is_sane(P1, P2) of
        true ->
            projection_transitions_are_sane([P2|T]);
        Else ->
            Else
    end.

projection_transition_is_sane(
  #projection{epoch_number=Epoch1,
              epoch_csum=CSum1,
              prev_epoch_num=PrevEpoch1,
              prev_epoch_csum=PrevCSum1,
              creation_time=CreationTime1,
              author_server=AuthorServer1,
              all_members=All_list1,
              down=Down_list1,
              upi=UPI_list1,
              repairing=Repairing_list1,
              dbg=Dbg1} = P1,
  #projection{epoch_number=Epoch2,
              epoch_csum=CSum2,
              prev_epoch_num=PrevEpoch2,
              prev_epoch_csum=PrevCSum2,
              creation_time=CreationTime2,
              author_server=AuthorServer2,
              all_members=All_list2,
              down=Down_list2,
              upi=UPI_list2,
              repairing=Repairing_list2,
              dbg=Dbg2} = P2) ->
 try
    true = is_integer(Epoch1) andalso is_integer(Epoch2),
    true = is_binary(CSum1) andalso is_binary(CSum2),
    true = is_integer(PrevEpoch1) andalso is_integer(PrevEpoch2),
    true = is_binary(PrevCSum1) andalso is_binary(PrevCSum2),
    {_,_,_} = CreationTime1,
    {_,_,_} = CreationTime2,
    true = is_atom(AuthorServer1) andalso is_atom(AuthorServer2), % todo will probably change
    true = is_list(All_list1) andalso is_list(All_list2),
    true = is_list(Down_list1) andalso is_list(Down_list2),
    true = is_list(UPI_list1) andalso is_list(UPI_list2),
    true = is_list(Repairing_list1) andalso is_list(Repairing_list2),
    true = is_list(Dbg1) andalso is_list(Dbg2),

    true = Epoch2 > Epoch1,
    true = PrevEpoch2 > PrevEpoch1,
    All_list1 = All_list2,                 % todo will probably change

    %% No duplicates
    true = lists:sort(Down_list2) == lists:usort(Down_list2),
    true = lists:sort(UPI_list2) == lists:usort(UPI_list2),
    true = lists:sort(Repairing_list2) == lists:usort(Repairing_list2),

    %% Disjoint-ness
    true = lists:sort(All_list2) == lists:sort(Down_list2 ++ UPI_list2 ++
                                                   Repairing_list2),
    [] = [X || X <- Down_list2, not lists:member(X, All_list2)],
    [] = [X || X <- UPI_list2, not lists:member(X, All_list2)],
    [] = [X || X <- Repairing_list2, not lists:member(X, All_list2)],
    DownS2 = sets:from_list(Down_list2),
    UPIS2 = sets:from_list(UPI_list2),
    RepairingS2 = sets:from_list(Repairing_list2),
    true = sets:is_disjoint(DownS2, UPIS2),
    true = sets:is_disjoint(DownS2, RepairingS2),
    true = sets:is_disjoint(UPIS2, RepairingS2),

    %% The author must not be down.
    false = lists:member(AuthorServer1, Down_list1),
    false = lists:member(AuthorServer2, Down_list2),
    %% The author must be in either the UPI or repairing list.
    true = lists:member(AuthorServer1, UPI_list1 ++ Repairing_list1),
    true = lists:member(AuthorServer2, UPI_list2 ++ Repairing_list2),

    %% Additions to the UPI chain may only be at the tail
    UPI_common_prefix = find_common_prefix(UPI_list1, UPI_list2),
    if UPI_common_prefix == [] ->
            if UPI_list1 == [] orelse UPI_list2 == [] ->
                    %% If the common prefix is empty, then one of the
                    %% inputs must be empty.
                    true;
               true ->
                    %% Otherwise, we have a case of UPI changing from
                    %% one of these two situations:
                    %%
                    %% UPI_list1 -> UPI_list2
                    %% [d,c,b,a] -> [c,a]
                    %% [d,c,b,a] -> [c,a,repair_finished_added_to_tail].
                    NotUPI2 = (Down_list2 ++ Repairing_list2),
                    true = lists:prefix(UPI_list1 -- NotUPI2,
                                        UPI_list2)
            end;
       true ->
            true
    end,
    true = lists:prefix(UPI_common_prefix, UPI_list1),
    true = lists:prefix(UPI_common_prefix, UPI_list2),
    UPI_1_suffix = UPI_list1 -- UPI_common_prefix,
    UPI_2_suffix = UPI_list2 -- UPI_common_prefix,

    %% Where did elements in UPI_2_suffix come from?
    %% Only two sources are permitted.
    [true = lists:member(X, Repairing_list1) % X added after repair done
            orelse
            lists:member(X, UPI_list1)       % X in UPI_list1 after common pref
     || X <- UPI_2_suffix],

    %% The UPI_2_suffix must exactly be equal to: ordered items from
    %% UPI_list1 concat'ed with ordered items from Repairing_list1.
    %% Both temp vars below preserve relative order!
    UPI_2_suffix_from_UPI1 = [X || X <- UPI_1_suffix,
                                   lists:member(X, UPI_list2)],
    UPI_2_suffix_from_Repairing1 = [X || X <- UPI_2_suffix,
                                         lists:member(X, Repairing_list1)],
    %% true?
    UPI_2_suffix = UPI_2_suffix_from_UPI1 ++ UPI_2_suffix_from_Repairing1,

    true
 catch
     _Type:_Err ->
         S1 = ?MGR:make_projection_summary(P1),
         S2 = ?MGR:make_projection_summary(P2),
         Trace = erlang:get_stacktrace(),
         {err, from, S1, to, S2, stack, Trace}
 end.

pass1_smoke_test() ->
    P2 = ?MGR:make_projection(2, 1, <<>>, a, [a,b,c,d],
                              [],      [d,c,b,a], [], []),
    P3 = ?MGR:make_projection(3, 2, <<>>, a, [a,b,c,d],
                              [b,c,d], [a],       [], []),
    true = projection_transition_is_sane(P2, P3),

    P2b= ?MGR:make_projection(2, 1, <<>>, a, [a,b,c,d],
                              [],      [d,c,b,a], [], []),
    P3b= ?MGR:make_projection(3, 2, <<>>, a, [a,b,c,d],
                              [b,d],   [c,a],       [], []),
    true = projection_transition_is_sane(P2b, P3b),

    P2c= ?MGR:make_projection(2, 1, <<>>, a, [a,b,c,d,e],
                              [],      [e,d,c,b], [a], []),
    P3c= ?MGR:make_projection(3, 2, <<>>, a, [a,b,c,d,e],
                              [e,c],   [d,b,a],   [], []),
    true = projection_transition_is_sane(P2c, P3c),

    true.

fail1_smoke_test() ->
    P15 = ?MGR:make_projection(15, 14, <<>>, a, [a,b,c,d],
                              [b,d], [a],   [c], []),
    P16 = ?MGR:make_projection(16, 15, <<>>, a, [a,b,c,d],
                              [b,d], [c,a], [],  []),
    true = projection_transition_is_sane(P15, P16) /= true,

    P2d= ?MGR:make_projection(2, 1, <<>>, a, [a,b,c,d,e],
                              [],      [e,d,c,b], [a], []),
    P3d= ?MGR:make_projection(3, 2, <<>>, a, [a,b,c,d,e],
                              [e,c],   [d,a,b],   [], []),
    true = projection_transition_is_sane(P2d, P3d) /= true,

    P2e= ?MGR:make_projection(2, 1, <<>>, a, [a,b,c,d,e],
                              [],      [e,d,c,b], [a], []),
    P3e= ?MGR:make_projection(3, 2, <<>>, a, [a,b,c,d,e],
                              [e,c],   [a,d,b],   [], []),
    true = projection_transition_is_sane(P2e, P3e) /= true,

    true.

fail2_smoke_test() ->
    AB = [a,b],
    BadSets = [{1, [b,a],[a,b]},
               {2, [b],[a,b]},
               %% {3, [a],[b]},  % weird but valid: [a] means b is repairing;
                                 % so if a fails and b finishes repairing,
                                 % swapping both at the same time is "ok".
               {4, [a,b],[b,a]},
               %% {5, [b],[a]},  % weird but valid: see above
               {6, [a],[b,a]}
              ],
    [begin
         P2f= ?MGR:make_projection(2, 1, <<>>, a, AB,
                                   [], Two,   AB -- Two, []),
         P3f= ?MGR:make_projection(3, 2, <<>>, a, AB,
                                   [], Three, AB -- Three, []),
         {Label, true} =
             {Label, (projection_transition_is_sane(P2f, P3f) /= true)}
     end || {Label, Two, Three} <- BadSets],

    true.

fail3_smoke_test() ->
    Nodes = [a,b],
    UPI1 = [a,b],
    UPI2 = [b,a],
    [begin
         PsUPI1 = UPI1,
         PsUPI2 = UPI2,
         P1 = ?MGR:make_projection(2, 1, <<>>, HdNd, Nodes,
                                   [], PsUPI1, Nodes -- PsUPI1, []),
         P2 = ?MGR:make_projection(3, 2, <<>>, HdNd, Nodes,
                                   [], PsUPI2, Nodes -- PsUPI2, []),
         Res = case projection_transition_is_sane(P1, P2) of
                   true -> true;
                   _    -> false
               end,
         {Res, HdNd, PsUPI1, PsUPI2} = {false, HdNd, PsUPI1, PsUPI2}
     end || HdNd <- Nodes],

    true.

fail4_smoke_test() ->
    Nodes = [a,b,c],
    Two = [c,b,a],
    Three = [c,a,b],
    P2a= ?MGR:make_projection(2, 1, <<>>, a, Nodes,
                              [], Two,   Nodes -- Two, []),
    P3a= ?MGR:make_projection(3, 2, <<>>, a, Nodes,
                              [], Three, Nodes -- Three, []),
    true = (projection_transition_is_sane(P2a, P3a) /= true),

    true.


%% aaa_smoke_test() ->
%%     L = [a,b,c,d],
%%     Cs = combinations(L),
%%     Combos = lists:sort([{X, Y} || X <- Cs, Y <- Cs]),
%%     timer:sleep(500),
%%     io:format(user, "\n", []),
%%     Rs = [begin
%%               P1 = ?MGR:make_projection(2, 1, <<>>, a, L,
%%                                         [], X, L -- X, []),
%%               P2 = ?MGR:make_projection(3, 2, <<>>, a, L,
%%                                         [], Y, L -- Y, []),
%%               Res = case projection_transition_is_sane(P1, P2) of
%%                         true -> true;
%%                         _    -> false
%%                     end,
%%               {Res, P1, P2}
%%           end || {X,Y} <- Combos],
%%     OKs = [{X,Y} || {true, X, Y} <- Rs],
%%     io:format(user, "Cs are ~p\n", [length(Cs)]),
%%     io:format(user, "OKs are ~p\n", [length(OKs)]),
%%     Bads = [{X,Y} || {false, X, Y} <- Rs],
%%     io:format(user, "Bads are ~p\n", [length(Bads)]),
%%     %% [io:format(user, "~p -> ~p: ~p\n", [X, Y, Res]);
%%     ok.

combinations(L) ->
    lists:usort(perms(L) ++ lists:append([ combinations(L -- [X]) || X <- L])).

perms([]) -> [[]];
perms(L)  -> [[H|T] || H <- L, T <- perms(L--[H])].

-endif.
