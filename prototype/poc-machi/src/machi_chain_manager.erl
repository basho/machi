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

-ifdef(TEST).
-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
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
            active_upi      :: [m_server()],
            active_repairing:: [m_server()],
            dbg             :: list() %proplist()
        }).

-record(state, {
          name :: m_server(),
          proj :: #projection{},
          last_up :: list(m_server()),
          %%
          runenv :: list() %proplist()
         }).

make_initial_state(MyName, All_list, Seed) ->
    RunEnv = [{seed, Seed},
              {network_partitions, []}],
    #state{name=MyName,
           proj=make_initial_projection(MyName, All_list, All_list, [], []),
           last_up=All_list,
           runenv=RunEnv}.

make_initial_projection(MyName, All_list, UPI_list, Repairing_list, Ps) ->
    make_projection(1, 0, <<>>,
                    MyName, All_list, UPI_list, Repairing_list, Ps).

make_projection(EpochNum, PrevEpochNum, PrevEpochCSum,
                MyName, All_list, UPI_list, Repairing_list, Ps) ->
    P = #projection{epoch_number=EpochNum,
                    epoch_csum= <<>>,
                    prev_epoch_num=PrevEpochNum,
                    prev_epoch_csum=PrevEpochCSum,
                    creation_time=now(),
                    author_server=MyName,
                    all_members=All_list,
                    active_upi=UPI_list,
                    active_repairing=Repairing_list,
                    dbg=Ps},
    CSum = crypto:hash(sha, term_to_binary(P)),
    P#projection{epoch_csum=CSum}.

calc_projection(OldThreshold, NoPartitionThreshold,
                #state{name=MyName, last_up=_LastUp, proj=LastProj,
                       runenv=RunEnv1} = S) ->
    #projection{epoch_number=OldEpochNum,
                epoch_csum=OldEpochCsum,
                all_members=All_list
               } = LastProj,
    AllMembers = (S#state.proj)#projection.all_members,
    {Up, RunEnv2} = calc_up_nodes(MyName, OldThreshold, NoPartitionThreshold,
                                  AllMembers, RunEnv1),
    P = make_projection(OldEpochNum + 1, OldEpochNum, OldEpochCsum,
                        MyName, All_list, Up, [], [{fubar,true}]),
    {P, S#state{runenv=RunEnv2}}.

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
    {F2, Seed2} = random:uniform_s(Seed1),
    Cutoff2 = trunc(F2 * 100),
    if Cutoff2 < OldThreshold ->
            {F3, Seed3} = random:uniform_s(Seed1),
            Cutoff3 = trunc(F3 * 100),
            if Cutoff3 < NoPartitionThreshold ->
                    {Seed3, []};
               true ->
                    make_network_partition_locations(Nodes, Seed3)
            end;
       true ->
            {Seed2, OldPartition}
    end.

replace(PropList, Items) ->
    lists:foldl(fun({Key, Val}, Ps) ->
                        lists:keyreplace(Key, 1, Ps, {Key,Val})
                end, PropList, Items).

make_projection_summary(#projection{epoch_number=EpochNum,
                                    all_members=All_list,
                                    active_upi=UPI_list,
                                    active_repairing=Repairing_list}) ->
    [{epoch,EpochNum},{all,All_list},{upi,UPI_list},{repair,Repairing_list}].

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
    S0 = ?MODULE:make_initial_state(a, [a,b,c], {4,5,6}),
    lists:foldl(fun(_, S) ->
                        {P1, S1} = calc_projection(555, 0, S),
                        io:format(user, "~p\n", [make_projection_summary(P1)]),
                        S1#state{proj=P1}
                end, S0, lists:seq(1,10)).
%% [begin
%%     %%io:format(user, "S0 ~p\n", [S0]),
%%     {P1, _S1} = calc_projection(555, 0, S0),
%%     io:format(user, "P1 ~p\n", [P1]),
%%     ok % io:format(user, "S1 ~p\n", [S1]).
%% end || X <- lists:seq(1, 10)].

-endif. %TEST
