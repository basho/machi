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
-module(machi_chain_manager).

-export([]).
-compile(export_all).

-ifdef(TEST).
-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-endif. %TEST

-type m_csum()      :: {none | sha1 | sha1_excl_final_20, binary()}.
%% -type m_epoch()     :: {m_epoch_n(), m_csum()}.
-type m_epoch_n()   :: non_neg_integer().
-type m_server()    :: atom().
-type timestamp()   :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

-record(projection, {
            epoch_number    :: m_epoch_n(),
            epoch_csum      :: m_csum(),
            prev_epoch_num  :: m_epoch_n(),
            prev_epoch_csum :: m_csum(),
            creation_time   :: timestamp(),
            author_server   :: m_server(),
            all_members     :: [m_server()],
            down            :: [m_server()],
            upi             :: [m_server()],
            repairing       :: [m_server()],
            dbg             :: list() %proplist()
        }).

-record(state, {
          name :: m_server(),
          proj :: #projection{},
          %%
          runenv :: list() %proplist()
         }).

make_initial_state(MyName, All_list, Seed) ->
    RunEnv = [{seed, Seed},
              {network_partitions, []}],
    #state{name=MyName,
           proj=make_initial_projection(MyName, All_list, All_list, [], []),
           runenv=RunEnv}.

make_initial_projection(MyName, All_list, UPI_list, Repairing_list, Ps) ->
    make_projection(1, 0, <<>>,
                    MyName, All_list, [], UPI_list, Repairing_list, Ps).

make_projection(EpochNum, PrevEpochNum, PrevEpochCSum,
                MyName, All_list, Down_list, UPI_list, Repairing_list, Ps) ->
    P = #projection{epoch_number=EpochNum,
                    epoch_csum= <<>>,
                    prev_epoch_num=PrevEpochNum,
                    prev_epoch_csum=PrevEpochCSum,
                    creation_time=now(),
                    author_server=MyName,
                    all_members=All_list,
                    down=Down_list,
                    upi=UPI_list,
                    repairing=Repairing_list,
                    dbg=Ps},
    CSum = crypto:hash(sha, term_to_binary(P)),
    P#projection{epoch_csum=CSum}.

%% OldThreshold: Percent chance of using the old/previous network partition list
%% NoPartitionThreshold: If the network partition changes, what are the odds
%%                       that there are no partitions at all?

calc_projection(OldThreshold, NoPartitionThreshold,
                #state{name=MyName, proj=LastProj, runenv=RunEnv1} = S) ->
    #projection{epoch_number=OldEpochNum,
                epoch_csum=OldEpochCsum,
                all_members=All_list,
                upi=OldUPI_list,
                repairing=OldRepairing_list
               } = LastProj,
    LastUp = lists:usort(OldUPI_list ++ OldRepairing_list),
    AllMembers = (S#state.proj)#projection.all_members,
    {Up, RunEnv2} = calc_up_nodes(MyName, OldThreshold, NoPartitionThreshold,
                                  AllMembers, RunEnv1),
    NewUp = Up -- LastUp,
    Down = AllMembers -- Up,

    NewUPI_list = [X || X <- OldUPI_list, lists:member(X, Up)],
    Repairing_list2 = [X || X <- OldRepairing_list, lists:member(X, Up)],
    {NewUPI_list3, Repairing_list3, RunEnv3} =
        case {NewUp, Repairing_list2} of
            {[], []} ->
                {NewUPI_list, [], RunEnv2};
            {[], [H|T]} ->
                {Prob, RunEnvX} = roll_dice(100, RunEnv2),
                if Prob =< 50 ->
                        {NewUPI_list ++ [H], T, RunEnvX};
                   true ->
                        {NewUPI_list, OldRepairing_list, RunEnvX}
                end;
            {_, _} ->
                {NewUPI_list, OldRepairing_list, RunEnv2}
        end,
    Repairing_list4 = case NewUp of
                          []    -> Repairing_list3;
                          NewUp -> Repairing_list3 ++ NewUp
                      end,
    Repairing_list5 = Repairing_list4 -- Down,
    P = make_projection(OldEpochNum + 1, OldEpochNum, OldEpochCsum,
                        MyName, All_list, Down, NewUPI_list3, Repairing_list5,
                        [goo]),
    {P, S#state{runenv=RunEnv3}}.

calc_up_nodes(MyName, OldThreshold, NoPartitionThreshold,
              AllMembers, RunEnv1) ->
    Seed1 = proplists:get_value(seed, RunEnv1),
    Partitions1 = proplists:get_value(network_partitions, RunEnv1),
    {Seed2, Partitions2} =
        calc_network_partitions(AllMembers, Seed1, Partitions1,
                                OldThreshold, NoPartitionThreshold),
    UpNodes = lists:sort(
                [Node || Node <- AllMembers,
                         not lists:member({MyName, Node}, Partitions2),
                         not lists:member({Node, MyName}, Partitions2)]),
    RunEnv2 = replace(RunEnv1,
                      [{seed, Seed2}, {network_partitions, Partitions2}]),
    {UpNodes, RunEnv2}.


calc_network_partitions(Nodes, Seed1, OldPartition,
                        OldThreshold, NoPartitionThreshold) ->
    {Cutoff2, Seed2} = random:uniform_s(100,Seed1),
    if Cutoff2 < OldThreshold ->
            {Seed2, OldPartition};
       true ->
            {F3, Seed3} = random:uniform_s(Seed1),
            Cutoff3 = trunc(F3 * 100),
            if Cutoff3 < NoPartitionThreshold ->
                    {Seed3, []};
               true ->
                    make_network_partition_locations(Nodes, Seed3)
            end
    end.

replace(PropList, Items) ->
    lists:foldl(fun({Key, Val}, Ps) ->
                        lists:keyreplace(Key, 1, Ps, {Key,Val})
                end, PropList, Items).

make_projection_summary(#projection{epoch_number=EpochNum,
                                    all_members=_All_list,
                                    down=Down_list,
                                    upi=UPI_list,
                                    repairing=Repairing_list}) ->
    [{epoch,EpochNum},
     {upi,UPI_list},{repair,Repairing_list},{down,Down_list}].
    %% [{epoch,EpochNum},{all,All_list},{down,Down_list},
    %%  {upi,UPI_list},{repair,Repairing_list}].

roll_dice(N, RunEnv) ->
    Seed1 = proplists:get_value(seed, RunEnv),
    {Val, Seed2} = random:uniform_s(N, Seed1),
    {Val, replace(RunEnv, [{seed, Seed2}])}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_network_partition_locations(Nodes, Seed1) ->
    Pairs = make_all_pairs(Nodes),
    Num = length(Pairs),
    {Seed2, Weights} = lists:foldl(
                         fun(_, {Seeda, Acc}) ->
                                 {F, Seedb} = random:uniform_s(Seeda),
                                 Cutoff = trunc(F * 100),
                                 {Seedb, [Cutoff|Acc]}
                         end, {Seed1, []}, lists:seq(1, Num)),
    {F3, Seed3} = random:uniform_s(Seed2),
    Cutoff3 = trunc(F3 * 100),
    {Seed3, [X || {Weight, X} <- lists:zip(Weights, Pairs),
                  Weight < Cutoff3]}.

make_all_pairs(L) ->
    lists:flatten(make_all_pairs2(lists:usort(L))).

make_all_pairs2([]) ->
    [];
make_all_pairs2([_]) ->
    [];
make_all_pairs2([H1|T]) ->
    [[{H1, X}, {X, H1}] || X <- T] ++ make_all_pairs(T).

-ifdef(TEST).

smoke0_test() ->
    S0 = ?MODULE:make_initial_state(a, [a,b,c,d], {4,5,6}),
    lists:foldl(fun(_, S) ->
                        {P1, S1} = calc_projection(20, 1, S),
                        io:format(user, "~p\n", [make_projection_summary(P1)]),
                        S1#state{proj=P1}
                end, S0, lists:seq(1,10)).

-ifdef(EQC).
gen_rand_seed() ->
    noshrink({gen_num(), gen_num(), gen_num()}).

gen_num() ->
    ?LET(I, oneof([int(), largeint()]),
         erlang:abs(I)).

calc_projection_prop() ->
    ?FORALL(
       {Seed, OldThreshold, NoPartitionThreshold},
       {gen_rand_seed(), choose(0, 101), choose(0,101)},
       begin
           Steps = 200,
           S0 = ?MODULE:make_initial_state(a, [a,b,c,d], Seed),
           F = fun(_, {S, Acc}) ->
                       {P1, S1} = calc_projection(OldThreshold,
                                                  NoPartitionThreshold, S),
                       %%io:format(user, "~p\n", [make_projection_summary(P1)]),
                       {S1#state{proj=P1}, [P1|Acc]}
               end,
           {_, Projs0} = lists:foldl(F, {S0, []}, lists:seq(1,Steps)),
           Projs = lists:reverse(Projs0),
           true = projection_transitions_are_sane(Projs)
       end).

calc_projection_test_() ->
    {timeout, 60,
     fun() ->
           true = eqc:quickcheck(eqc:numtests(1000,
                                              ?QC_OUT(calc_projection_prop())))
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
              dbg=Dbg1} = _P1,
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
              dbg=Dbg2} = _P2) ->
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
    %% If the common prefix is empty, then one of the inputs must be empty.
    if UPI_common_prefix == [] ->
            true = UPI_list1 == [] orelse UPI_list2 == [];
       true ->
            true
    end,
    true = lists:prefix(UPI_common_prefix, UPI_list1), % sanity
    true = lists:prefix(UPI_common_prefix, UPI_list2), % sanity
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

    true.

-endif. % EQC

%% [begin
%%     %%io:format(user, "S0 ~p\n", [S0]),
%%     {P1, _S1} = calc_projection(555, 0, S0),
%%     io:format(user, "P1 ~p\n", [P1]),
%%     ok % io:format(user, "S1 ~p\n", [S1]).
%% end || X <- lists:seq(1, 10)].

-endif. %TEST
