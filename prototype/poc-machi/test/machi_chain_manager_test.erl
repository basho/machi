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

-include("machi.hrl").

-define(MGR, machi_chain_manager).

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
                        io:format(user, "~p\n", [?MGR:make_projection_summary(P1)]),
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
    %% ?LET(I, oneof([int(), largeint()]),
    %%      erlang:abs(I)).

prop_calc_projection() ->
    ?FORALL(
       {Seed, OldThreshold, NoPartitionThreshold, Steps},
       {gen_rand_seed(), choose(0, 101), choose(0, 101), choose(500, 2000)},
       begin
           erase(goofus),
           S0 = ?MGR:make_initial_state(a, [a,b,c,d,e], Seed),
           F = fun(_, {S, Acc}) ->
                       {P1, S1} = ?MGR:calc_projection(
                                    OldThreshold, NoPartitionThreshold, S),
                       %%io:format(user, "~p\n", [make_projection_summary(P1)]),
                       {S1#ch_mgr{proj=P1}, [P1|Acc]}
               end,
           {_, Projs0} = lists:foldl(F, {S0, []}, lists:seq(1,Steps)),
           Projs = lists:reverse(Projs0),
           true = projection_transitions_are_sane(Projs)
       end).

calc_projection_test_() ->
    {timeout, 60,
     fun() ->
           true = eqc:quickcheck(eqc:numtests(5,
                                              ?QC_OUT(prop_calc_projection())))
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

    %% Additions to the UPI chain may only be at the tail
    UPI_common_prefix =
        lists:takewhile(fun(X) -> sets:is_element(X, UPIS2) end, UPI_list1),
    if UPI_common_prefix == [] ->
            if UPI_list1 == [] orelse UPI_list2 == [] ->
                    %% If the common prefix is empty, then one of the inputs
                    %% must be empty.
                    true;
               true ->
                    %% Otherwise, we have a case of UPI changing from one
                    %% of these two situations:
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
    UPI_2_suffix_from_UPI1 = [X || X <- UPI_2_suffix,
                                   lists:member(X, UPI_list1)],
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

-endif.
