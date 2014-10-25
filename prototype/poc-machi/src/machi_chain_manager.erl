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
            dbg             :: list()%proplist()
        }).

-record(state, {
          name :: m_server(),
          proj :: #projection{},
          seed :: timestamp(),
          last_up :: list(m_server())
         }).

make_initial_state(MyName, All_list, Seed) ->
    #state{name=MyName,
           proj=make_initial_projection(MyName, All_list, All_list, [], []),
           seed=Seed,
           last_up=All_list}.

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

calc_projection(CurrentInfo, #state{name=MyName, proj=OldProj} = S) ->
    {UpNodes, S2} = calc_up_nodes(CurrentInfo, S),
    #projection{epoch_number=OldEpochNum,
                epoch_csum=OldEpochCsum,
                all_members=All_list
                %% active_upi=UPI_list,
                %% active_repairing=Repairing_list,
                %% dbg=Ps
               } = OldProj,
    P = make_projection(OldEpochNum + 1, OldEpochNum, OldEpochCsum,
                        MyName, All_list, UpNodes, [], [{fubar,true}]),
    {P, S2}.

calc_up_nodes(CurrentInfo,
              #state{name=MyName, seed=Seed, last_up=LastUp} = S) ->
    AllMembers = (S#state.proj)#projection.all_members,
    {F, Seed2} = random:uniform_s(Seed),
    Cutoff = trunc(F * 100),
    if LastUp == undefined orelse Cutoff rem 3 == 0 ->
            Up = calc_new_up_nodes(MyName, AllMembers, CurrentInfo, Cutoff),
            {Up, S#state{seed=Seed2, last_up=Up}};
       true ->
            {LastUp, S#state{seed=Seed2}}
    end.

calc_new_up_nodes(MyName, Nodes, CurrentInfo, Cutoff) ->
    C_weights = proplists:get_value(communicating_weights, CurrentInfo),
    lists:usort([MyName] ++
                    [Node || Node <- Nodes,
                             Node /= MyName,
                             Weight_to <- [element(1,
                                                   lists:keyfind({MyName, Node},
                                                                2, C_weights))],
                             Weight_from <- [element(1,
                                                 lists:keyfind({Node, MyName},
                                                               2, C_weights))],
                             Weight_to =< Cutoff,
                             Weight_from =< Cutoff]).
