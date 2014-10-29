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
-define(Dw(X), io:format(user, "~s ~w\n", [??X, X])).

%% API
-export([start_link/6, stop/1, ping/1,
         calculate_projection_internal_old/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([make_projection_summary/1]).

-ifdef(TEST).
-export([test_calc_projection/2,
         test_calc_proposed_projection/1,
         test_write_proposed_projection/1,
         test_read_public_projection/2]).

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

ping(Pid) ->
    gen_server:call(Pid, {ping}, infinity).

calculate_projection_internal_old(Pid) ->
    gen_server:call(Pid, {calculate_projection_internal_old}, infinity).

test_write_proposed_projection(Pid) ->
    gen_server:call(Pid, {test_write_proposed_projection}, infinity).

-ifdef(TEST).

%% Test/debugging code only.

%% Calculate a projection and return it to us.
%% If KeepRunenvP is true, the server will retain its change in its
%% runtime environment, e.g., changes in simulated network partitions.
%% The server's internal proposed projection is not altered.
test_calc_projection(Pid, KeepRunenvP) ->
    gen_server:call(Pid, {test_calc_projection, KeepRunenvP}, infinity).

%% Async!
%% The server's internal proposed projection *is* altered.
test_calc_proposed_projection(Pid) ->
    gen_server:cast(Pid, {test_calc_proposed_projection}).

test_read_public_projection(Pid, ReadRepairP) ->
    gen_server:call(Pid, {test_read_public_projection, ReadRepairP}, infinity).
-endif. % TEST

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({MyName, All_list, Seed, OldThreshold, NoPartitionThreshold, MyFLUPid}) ->
    RunEnv = [{seed, Seed},
              {network_partitions, []},
              {old_threshold, OldThreshold},
              {no_partition_threshold, NoPartitionThreshold}],
    BestProj = make_initial_projection(MyName, All_list, All_list,
                                       [{author_proc, init_best}], []),
    NoneProj = make_initial_projection(MyName, All_list, [],
                                       [{author_proc, init_none}], []),
    S = #ch_mgr{init_finished=false,
                name=MyName,
                proj=NoneProj,
                myflu=MyFLUPid, % pid or atom local name
                runenv=RunEnv},
    self() ! {finish_init, BestProj},
    {ok, S}.

handle_call(_Call, _From, #ch_mgr{init_finished=false} = S) ->
    {reply, not_initialized, S};
handle_call({calculate_projection_internal_old}, _From, S) ->
    {Reply, S2} = calc_projection(S, [{author_proc, call}]),
    {reply, Reply, S2};
handle_call({test_write_proposed_projection}, _From, S) ->
    if S#ch_mgr.proj_proposed == none ->
            {reply, none, S};
       true ->
            {Res, S2} = do_cl_write_proposed_proj(S),
            {reply, Res, S2}
    end;
handle_call({ping}, _From, S) ->
    {reply, pong, S};
handle_call({stop}, _From, S) ->
    {stop, normal, ok, S};
handle_call({test_calc_projection, KeepRunenvP}, _From, S) ->
    {P, S2} = calc_projection(S, [{author_proc, call}]),
    {reply, {ok, P}, if KeepRunenvP -> S2;
                        true        -> S
                     end};
handle_call({test_read_public_projection, ReadRepairP}, _From, S) ->
    {Res, ExtraInfo, _DiscardS2} = do_cl_read_public_projection(ReadRepairP, S),
    {reply, {Res, ExtraInfo}, S};
handle_call(_Call, _From, S) ->
    {reply, whaaaaaaaaaa, S}.

handle_cast(_Cast, #ch_mgr{init_finished=false} = S) ->
    {noreply, S};
handle_cast({test_calc_proposed_projection}, S) ->
    {Proj, S2} = calc_projection(S, [{author_proc, cast}]),
    ?D({make_projection_summary(Proj)}),
    {noreply, S2#ch_mgr{proj_proposed=Proj}};
handle_cast(_Cast, S) ->
    ?D({cast_whaaaaaaaaaaa, _Cast}),
    {noreply, S}.

handle_info({finish_init, BestProj}, S) ->
    S2 = finish_init(BestProj, S),
    {noreply, S2};
handle_info(Msg, S) ->
    exit({bummer, Msg}),
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

finish_init(BestProj, #ch_mgr{init_finished=false, myflu=MyFLU} = S) ->
    case machi_flu0:proj_read_latest(MyFLU, private) of
        error_unwritten ->
            Epoch = BestProj#projection.epoch_number,
            case machi_flu0:proj_write(MyFLU, Epoch, private, BestProj) of
                ok ->
                    S#ch_mgr{init_finished=true, proj=BestProj};
                error_written ->
                    exit({yo_impossible, ?LINE});
                Else ->
                    ?D({retry,Else}),
                    timer:sleep(100),
                    finish_init(BestProj, S)
            end;
        {ok, Proj} ->
            S#ch_mgr{init_finished=true, proj=Proj};
        Else ->
            ?D({todo, fix_up_eventually, Else}),
            exit({yo_weird, Else})
    end.

do_cl_write_proposed_proj(#ch_mgr{proj_proposed=Proj} = S) ->
    #projection{epoch_number=Epoch} = Proj,
    case cl_write_public_proj(Epoch, Proj, S) of
        {ok, _S2}=Res ->
            Res;
        {_Other2, _S2}=Else2 ->
            Else2
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cl_write_public_proj(Epoch, Proj, S) ->
    %% Write to local public projection store first, and if it succeeds,
    %% then write to all remote public projection stores.
    cl_write_public_proj_local(Epoch, Proj, S).

cl_write_public_proj_local(Epoch, Proj, #ch_mgr{myflu=MyFLU}=S) ->
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    Res0 = perhaps_call_t(
             S, Partitions, MyFLU,
             fun() -> machi_flu0:proj_write(MyFLU, Epoch, public, Proj) end),
    case Res0 of
        ok ->
            FLUs = Proj#projection.all_members -- [MyFLU],
            cl_write_public_proj_remote(FLUs, Partitions, Epoch, Proj, S);
        Else when Else == error_written; Else == timeout; Else == t_timeout ->
            {Else, S2}
    end.

cl_write_public_proj_remote(FLUs, Partitions, Epoch, Proj, S) ->
    %% We're going to be very cavalier about this write because we'll rely
    %% on the read side to do any read repair.
    DoIt = fun(X) -> machi_flu0:proj_write(X, Epoch, public, Proj) end,
    Rs = [{FLU, perhaps_call_t(S, Partitions, FLU, fun() -> DoIt(FLU) end)} ||
             FLU <- FLUs],
    {{remote_write_results, Rs}, S}.

do_cl_read_public_projection(ReadRepairP,
                             #ch_mgr{proj=Proj1, myflu=MyFLU} = S) ->
    Epoch1 = Proj1#projection.epoch_number,
    case cl_read_public_projection(S) of
        {unanimous, Proj2, Extra, S3} when Proj2 == Proj1 ->
            {Proj2, Extra, S3};
        {unanimous, #projection{epoch_number=Epoch2}=Proj2, Extra, _S3}
          when Epoch2 < Epoch1 orelse
               (Epoch2 == Epoch1 andalso Proj2 /= Proj1) ->
            exit({invariant_error, mine, Proj1, cl_unanimous, Proj2, extra, Extra});
        {unanimous, #projection{epoch_number=Epoch2}=Proj2, Extra, S3}
          when Epoch2 > Epoch1 ->
            Proj2b = update_projection_dbg2(
                       Proj2, [{hooray, {date(), time()}}|Extra]),
            ok = machi_flu0:proj_write(MyFLU, Epoch2, private, Proj2b),
            {Proj2, Extra, S3#ch_mgr{proj=Proj2b}};
        {needs_work, _FLUsRs, _Extra, _S3}=Else3 ->
            if not ReadRepairP ->
                    Else3;
               true ->
                    %% 1. Do the results contain a projection?
                    %%    perhaps figure that in cl_read_public_projection()?
                    %% 2. Were there any error_unwritten?
                    %% 3. Repair the unwritten FLUs.
                    %% 4. Nothing to do with timeouts, right?  They're
                    %%    hopeless for the moment, need to wait.
                    
                    %% For read-repair, just choose the best and then brute-
                    %% force write, don't care about write status, then
                    %% repeat do_cl_read_public_projection() ??
                    Else3
            end
    end.

cl_read_public_projection(#ch_mgr{proj=Proj}=S) ->
    #projection{all_members=All_list} = Proj,
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
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
            Extra = [{all_replied, length(Rs) == length(All_list)}],
            {unanimous, P, [{unanimous,true}|Extra], S2};
        _ ->
            FLUsRs = lists:zip(All_list, Rs),
            {needs_work, FLUsRs, [flarfus], S2} % todo?
    end.

make_initial_projection(MyName, All_list, UPI_list, Repairing_list, Ps) ->
    make_projection(0, -1, <<>>,
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

calc_projection(#ch_mgr{proj=LastProj, runenv=RunEnv} = S, Dbg) ->
    OldThreshold = proplists:get_value(old_threshold, RunEnv),
    NoPartitionThreshold = proplists:get_value(no_partition_threshold, RunEnv),
    calc_projection(OldThreshold, NoPartitionThreshold, LastProj, Dbg, S).

%% OldThreshold: Percent chance of using the old/previous network partition list
%% NoPartitionThreshold: If the network partition changes, what are the odds
%%                       that there are no partitions at all?

calc_projection(OldThreshold, NoPartitionThreshold, LastProj, Dbg,
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
                        Dbg ++ [{nodes_up, Up}]),
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

rank_and_repair_instructions([_|_] = FLUsRs, Proj) ->
    #projection{all_members=All_list} = Proj,
    MemberRank = orddict:from_list(
                   lists:zip(All_list, lists:seq(1, length(All_list)))),
    N = length(All_list),
    TopRank = lists:max([-1] ++ 
                        [rank_projection(ProjX, MemberRank, N) ||
                            {_FLU, ProjX} <- FLUsRs,
                            is_record(ProjX, projection)]),
    rank_and_repair(FLUsRs, Proj, TopRank, undefined, [], MemberRank, N).

rank_and_repair([], _Proj, TopRank, WinningProj,
                RepairInstr, _MemberRank, _N) ->
    {TopRank, WinningProj, RepairInstr};
rank_and_repair([{FLU, Proj}|T], OldProj, TopRank, WinningProj,
                RepairInstr, MemberRank, N)
  when is_record(Proj, projection) ->
    case rank_projection(Proj, MemberRank, N) of
        Rank when Rank == TopRank ->
            rank_and_repair(T, OldProj, TopRank, Proj,
                            RepairInstr, MemberRank, N);
        _Rank ->
            rank_and_repair(T, OldProj, TopRank, WinningProj,
                            [FLU|RepairInstr], MemberRank, N)
    end;
rank_and_repair([{FLU, _SomeError}|T], OldProj, TopRank, WinningProj,
                RepairInstr, MemberRank, N) ->
    ?D({?LINE, _SomeError}),
    rank_and_repair(T, OldProj, TopRank, WinningProj,
                    [FLU|RepairInstr], MemberRank, N).
    

rank_projection(#projection{author_server=Author,
                            upi=UPI_list,
                            repairing=Repairing_list}, MemberRank, N) ->
    AuthorRank = orddict:fetch(Author, MemberRank),
    AuthorRank +
        (1*N + length(Repairing_list)) +
        (2*N + length(UPI_list)).

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
            ?D({perhaps_call, S#ch_mgr.myflu, FLU, Partitions}),
            t_timeout
    end.

perhaps_call(#ch_mgr{name=MyName, myflu=MyFLU}, Partitions, FLU, DoIt) ->
    RemoteFLU_p = FLU /= MyFLU,
    case RemoteFLU_p andalso lists:keyfind(FLU, 1, Partitions) of
        false ->
            Res = DoIt(),
            case RemoteFLU_p andalso lists:keyfind(MyName, 2, Partitions) of
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
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, M0} = ?MGR:start_link(a, [a,b,c], {1,2,3}, 50, 50, a),
    try
        pong = ping(M0),

        [begin
             Proj = ?MGR:calculate_projection_internal_old(M0),
             io:format(user, "~w\n", [?MGR:make_projection_summary(Proj)])
         end || _ <- lists:seq(1,5)]
    after
        ok = ?MGR:stop(M0),
        ok = machi_flu0:stop(FLUa)
    end.

smoke1_test() ->
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, FLUb} = machi_flu0:start_link(b),
    {ok, FLUc} = machi_flu0:start_link(c),
    I_represent = I_am = a,
    %% {ok, M0} = ?MGR:start_link(I_represent, [a,b,c], {1,2,3}, 50, 50, I_am),
    {ok, M0} = ?MGR:start_link(I_represent, [a,b,c], {1,2,3}, 0, 100, I_am),
    try
        {ok, P1} = test_calc_projection(M0, false),
        %% ?D(make_projection_summary(P1)),
        %% {ok, _Pa} = test_calc_projection(M0, true),
        %% %% ?D(make_projection_summary(_Pa)),
        %% {ok, _Pb} = test_calc_projection(M0, true),
        %% %% ?D(make_projection_summary(_Pb)),

        _ = test_calc_proposed_projection(M0),
        {remote_write_results, [{b,ok},{c,ok}]} =
              test_write_proposed_projection(M0),
        XX = test_read_public_projection(M0, false),
        ?D(XX),

        ok
    after
        ok = ?MGR:stop(M0),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb),
        ok = machi_flu0:stop(FLUc)        
    end.

%% nonunanimous_read_setup_test() ->
%%     {ok, FLUa} = machi_flu0:start_link(a),
%%     {ok, FLUb} = machi_flu0:start_link(b),
%%     I_represent = I_am = a,
%%     {ok, M0} = ?MGR:start_link(I_represent, [a,b], {1,2,3}, 50, 50, I_am),
%%     try
%%         ok = test_write_proposed_projection(M0),
%%         {ok, P1} = test_calc_projection(M0),
%%         P1a = P1#projection{down=[b], upi=[a], dbg=[{hackhack, ?LINE}]},
%%         P1b = P1#projection{down=[a], upi=[b], dbg=[{hackhack, ?LINE}]},
%%         P1Epoch = P1#projection.epoch_number,
%%         ok = machi_flu0:proj_write(FLUa, P1Epoch, public, P1a),
%%         ok = machi_flu0:proj_write(FLUb, P1Epoch, public, P1b),
%%         XX = test_read_public_projection(M0, false),
%%         ?D(XX),
%%         XX2 = test_read_public_projection(M0, true),
%%         ?D(XX2)
%%     after
%%         ok = ?MGR:stop(M0),
%%         ok = machi_flu0:stop(FLUa),
%%         ok = machi_flu0:stop(FLUb)
%%     end.

-endif.
