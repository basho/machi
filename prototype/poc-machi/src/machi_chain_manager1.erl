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
-module(machi_chain_manager1).

-behaviour(gen_server).

-include("machi.hrl").
-define(D(X), io:format(user, "~s ~p\n", [??X, X])).

%% API
-export([start_link/6, stop/1,
         calculate_projection_internal_old/1,
         cl_write_current_projection/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([calc_projection/3, make_projection_summary/1]).

-ifdef(TEST).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif. %TEST

start_link(MyName, All_list, Seed,
           OldThreshold, NoPartitionThreshold,
           MyFLUPid) ->
    gen_server:start_link(?MODULE, {MyName, All_list, Seed,
                                    OldThreshold, NoPartitionThreshold,
                                    MyFLUPid}, []).

stop(Pid) ->
    gen_server:call(Pid, {stop}, infinity).

calculate_projection_internal_old(Pid) ->
    gen_server:call(Pid, {calculate_projection_internal_old}, infinity).

cl_write_current_projection(Pid) ->
    gen_server:call(Pid, {cl_write_current_projection}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({MyName, All_list, Seed, OldThreshold, NoPartitionThreshold, MyFLUPid}) ->
    RunEnv = [{seed, Seed},
              {network_partitions, []},
              {old_threshold, OldThreshold},
              {no_partition_threshold, NoPartitionThreshold}],
    S = #ch_mgr{name=MyName,
                proj=make_initial_projection(MyName, All_list, All_list,
                                             [{author_proc, init}], []),
                myflu=MyFLUPid, % pid or atom local name
                runenv=RunEnv},
    {ok, S}.

handle_call({calculate_projection_internal_old}, _From,
            #ch_mgr{runenv=RunEnv}=S) ->
    OldThreshold = proplists:get_value(old_threshold, RunEnv),
    NoPartitionThreshold = proplists:get_value(no_partition_threshold, RunEnv),
    {Reply, S2} = calc_projection(OldThreshold, NoPartitionThreshold, S),
    {reply, Reply, S2};
handle_call({cl_write_current_projection}, _From, S) ->
    {Res, S2} = do_cl_write_current_proj(S),
    {reply, Res, S2};
handle_call({stop}, _From, S) ->
    {stop, normal, ok, S};
handle_call(_Call, _From, S) ->
    {reply, whaaaaaaaaaa, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_cl_write_current_proj(#ch_mgr{proj=Proj, myflu=MyFLU} = S) ->
    #projection{epoch_number=Epoch} = Proj,
    case cl_write_public_proj(Epoch, Proj, S) of
        {ok, S2} ->
            case cl_read_public_proj(S2) of
                {ok, Proj2, S3} ->
                    ?D(Proj2),
                    ?D(machi_flu0:get_epoch(MyFLU)),
                    Proj2b = update_projection_dbg2(
                               Proj2, [{hooray, {date(), time()}}]),
                    ok = machi_flu0:proj_write(MyFLU, Epoch, private, Proj2b),
                    MyP = make_projection_summary(element(2,
                                 machi_flu0:proj_read_latest(MyFLU, private))),
                    ?D(MyP),
                    {ok, S3#ch_mgr{proj=Proj2}};
                {_Other3, _S3}=Else3 ->
                    Else3
            end;
        {_Other2, _S2}=Else2 ->
            Else2
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cl_write_public_proj(Epoch, Proj, S) ->
    cl_write_public_proj_local(Epoch, Proj, S).

cl_write_public_proj_local(Epoch, Proj, #ch_mgr{myflu=MyFLU}=S) ->
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    Res0 = perhaps_call_t(
             S, Partitions, MyFLU,
             fun() -> machi_flu0:proj_write(MyFLU, Epoch, public, Proj) end),
    case Res0 of
        ok ->
            %% todo
            FLUs = Proj#projection.all_members -- [MyFLU],
            cl_write_public_proj_remote(FLUs, Partitions, Epoch, Proj, S);
        Else when Else == error_written; Else == timeout; Else == t_timeout ->
            {Else, S2}
    end.

cl_write_public_proj_remote(FLUs, Partitions, Epoch, Proj, S) ->
    %% We're going to be very cavalier about this write because we'll rely
    %% on the read side to do any read repair.
    DoIt = fun(X) -> machi_flu0:proj_write(X, Epoch, public, Proj) end,
    Rs = [perhaps_call_t(S, Partitions, FLU, fun() -> DoIt(FLU) end) ||
             FLU <- FLUs],
    case lists:usort(Rs) of
        [ok] ->
            {ok, S};
        _ ->
            {{mixed_bag, lists:zip(FLUs, Rs)}, S}
    end.

cl_read_public_proj(#ch_mgr{proj=Proj0}=S) ->
    #projection{all_members=All_list} = Proj0,
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    %% todo
    ?D({todo, All_list, Partitions}),
    DoIt = fun(X) ->
                   case machi_flu0:proj_read_latest(X, public) of
                       {ok, P} -> P;
                       Else    -> Else
                   end
           end,
    Rs = [perhaps_call_t(S, Partitions, FLU, fun() -> DoIt(FLU) end) ||
             FLU <- All_list],
    case lists:usort(Rs) of
        [P] when is_record(P, projection) ->
            {ok, S#ch_mgr.proj, S2}
    end.

make_initial_projection(MyName, All_list, UPI_list, Repairing_list, Ps) ->
    make_projection(1, 0, <<>>,
                    MyName, All_list, [], UPI_list, Repairing_list, Ps).

make_projection(EpochNum, PrevEpochNum, PrevEpochCSum,
                MyName, All_list, Down_list, UPI_list, Repairing_list,
                Dbg) ->
    make_projection(EpochNum, PrevEpochNum, PrevEpochCSum,
                    MyName, All_list, Down_list, UPI_list, Repairing_list,
                    Dbg, []).

make_projection(EpochNum, PrevEpochNum, PrevEpochCSum,
                MyName, All_list, Down_list, UPI_list, Repairing_list,
                Dbg, Dbg2) ->
    P = #projection{epoch_number=EpochNum,
                    epoch_csum= <<>>,           % always checksums as <<>>
                    prev_epoch_num=PrevEpochNum,
                    prev_epoch_csum=PrevEpochCSum,
                    creation_time=now(),
                    author_server=MyName,
                    all_members=All_list,
                    down=Down_list,
                    upi=UPI_list,
                    repairing=Repairing_list,
                    dbg=Dbg,
                    dbg2=[]                     % always checksums as []
                   },
    CSum = crypto:hash(sha, term_to_binary(P)),
    P#projection{epoch_csum=CSum,
                 dbg2=Dbg2}.

update_projection_dbg2(P, Dbg2) when is_list(Dbg2) ->
    P#projection{dbg2=Dbg2}.

%% OldThreshold: Percent chance of using the old/previous network partition list
%% NoPartitionThreshold: If the network partition changes, what are the odds
%%                       that there are no partitions at all?

calc_projection(OldThreshold, NoPartitionThreshold,
                #ch_mgr{proj=LastProj} = S) ->
    calc_projection(OldThreshold, NoPartitionThreshold, LastProj, S).

calc_projection(OldThreshold, NoPartitionThreshold, LastProj,
                #ch_mgr{name=MyName, runenv=RunEnv1} = S) ->
    #projection{epoch_number=OldEpochNum,
                epoch_csum=OldEpochCsum,
                all_members=All_list,
                upi=OldUPI_list,
                repairing=OldRepairing_list
               } = LastProj,
    LastUp = lists:usort(OldUPI_list ++ OldRepairing_list),
    AllMembers = (S#ch_mgr.proj)#projection.all_members,
    {Up, _, RunEnv2} = calc_up_nodes(MyName, OldThreshold, NoPartitionThreshold,
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
    {P, S#ch_mgr{runenv=RunEnv3}}.

calc_up_nodes(#ch_mgr{name=MyName, proj=Proj, runenv=RunEnv1}=S) ->
    OldThreshold = proplists:get_value(old_threshold, RunEnv1),
    NoPartitionThreshold = proplists:get_value(no_partition_threshold, RunEnv1),
    AllMembers = Proj#projection.all_members,
    {UpNodes, Partitions, RunEnv2} =
        calc_up_nodes(MyName, OldThreshold, NoPartitionThreshold,
                      AllMembers, RunEnv1),
    {UpNodes, Partitions, S#ch_mgr{runenv=RunEnv2}}.

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
    {UpNodes, Partitions2, RunEnv2}.


calc_network_partitions(Nodes, Seed1, OldPartition,
                        OldThreshold, NoPartitionThreshold) ->
    {Cutoff2, Seed2} = random:uniform_s(100, Seed1),
    if Cutoff2 < OldThreshold ->
            {Seed2, OldPartition};
       true ->
            {Cutoff3, Seed3} = random:uniform_s(100, Seed1),
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
                                    repairing=Repairing_list,
                                    dbg=Dbg, dbg2=Dbg2}) ->
    [{epoch,EpochNum},
     {upi,UPI_list},{repair,Repairing_list},{down,Down_list},
     {d,Dbg}, {d2,Dbg2}].

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
                                 {Cutoff, Seedb} = random:uniform_s(100, Seeda),
                                 {Seedb, [Cutoff|Acc]}
                         end, {Seed1, []}, lists:seq(1, Num)),
    {Cutoff3, Seed3} = random:uniform_s(100, Seed2),
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

perhaps_call_t(S, Partitions, FLU, DoIt) ->
    try
        perhaps_call(S, Partitions, FLU, DoIt)
    catch
        exit:timeout ->
            t_timeout
    end.

perhaps_call(#ch_mgr{name=MyName}, Partitions, FLU, DoIt) ->
    case lists:keyfind(FLU, 1, Partitions) of
        false ->
            Res = DoIt(),
            case lists:keyfind(MyName, 2, Partitions) of
                false ->
                    Res;
                _ ->
                    exit(timeout)
            end;
        _ ->
            exit(timeout)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

-define(MGR, machi_chain_manager1).

smoke0_test() ->
    {ok, M0} = ?MGR:start_link(a, [a,b,c], {1,2,3}, 50, 50, a),
    try
        [begin
             Proj = ?MGR:calculate_projection_internal_old(M0),
             io:format(user, "~p\n", [?MGR:make_projection_summary(Proj)])
         end || _ <- lists:seq(1,5)]
    after
        ok = ?MGR:stop(M0)
    end.

smoke1_test() ->
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, FLUb} = machi_flu0:start_link(b),
    {ok, FLUc} = machi_flu0:start_link(c),
    I_represent = I_am = a,
    {ok, M0} = ?MGR:start_link(I_represent, [a,b,c], {1,2,3}, 50, 50, I_am),
    try
        ok = cl_write_current_projection(M0)
    after
        ok = ?MGR:stop(M0),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb),
        ok = machi_flu0:stop(FLUc)        
    end.

-endif.