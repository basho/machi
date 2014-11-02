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
         test_read_latest_public_projection/2,
         test_react_to_env/1,
         test_reset_thresholds/3]).

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

-ifdef(TEST).

%% Test/debugging code only.

test_write_proposed_projection(Pid) ->
    gen_server:call(Pid, {test_write_proposed_projection}, infinity).

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

test_read_latest_public_projection(Pid, ReadRepairP) ->
    gen_server:call(Pid, {test_read_latest_public_projection, ReadRepairP}, infinity).

test_react_to_env(Pid) ->
    gen_server:call(Pid, {test_react_to_env}, infinity).

test_reset_thresholds(Pid, OldThreshold, NoPartitionThreshold) ->
    gen_server:call(Pid, {test_reset_thresholds, OldThreshold, NoPartitionThreshold}, infinity).

-endif. % TEST

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({MyName, All_list, Seed, OldThreshold, NoPartitionThreshold, MyFLUPid}) ->
    RunEnv = [{seed, Seed},
              {network_partitions, []},
              {old_threshold, OldThreshold},
              {no_partition_threshold, NoPartitionThreshold},
              {up_nodes, not_init_yet}],
    BestProj = make_initial_projection(MyName, All_list, All_list,
                                       [], [{author_proc, init_best}]),
    NoneProj = make_initial_projection(MyName, All_list, [],
                                       [], [{author_proc, init_none}]),
    S = #ch_mgr{init_finished=false,
                name=MyName,
                proj=NoneProj,
                myflu=MyFLUPid, % pid or atom local name
                runenv=RunEnv},

    %% TODO: There is a bootstrapping problem there that needs to be
    %% solved eventually: someone/something needs to set the initial
    %% state for the chain.
    %%
    %% The PoC hack here will set the chain to all members.  That may
    %% be fine for testing purposes, but it won't work for real life.
    %% For example, if chain C has been running with [a,b] for a
    %% while, then we start c.  We don't want c to immediately say,
    %% hey, let's do [a,b,c] immediately ... UPI invariant requires
    %% repair, etc. etc.

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
handle_call({test_read_latest_public_projection, ReadRepairP}, _From, S) ->
    {Perhaps, Val, ExtraInfo, S2} =
        do_cl_read_latest_public_projection(ReadRepairP, S),
    Res = {Perhaps, Val, ExtraInfo},
    {reply, Res, S2};
handle_call({test_react_to_env}, _From, S) ->
    {TODOtodo, S2} =  do_react_to_env(S),
    {reply, TODOtodo, S2};
handle_call({test_reset_thresholds, OldThreshold, NoPartitionThreshold}, _From,
            #ch_mgr{runenv=RunEnv} = S) ->
    RunEnv2 = replace(RunEnv, [{old_threshold, OldThreshold},
                               {no_partition_threshold, NoPartitionThreshold}]),

    {reply, ok, S#ch_mgr{runenv=RunEnv2}};
handle_call(_Call, _From, S) ->
    {reply, whaaaaaaaaaa, S}.

handle_cast(_Cast, #ch_mgr{init_finished=false} = S) ->
    {noreply, S};
handle_cast({test_calc_proposed_projection}, S) ->
    {Proj, S2} = calc_projection(S, [{author_proc, cast}]),
    %% ?Dw({?LINE,make_projection_summary(Proj)}),
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
    cl_write_public_proj(Epoch, Proj, false, S).

cl_write_public_proj_skip_local_error(Epoch, Proj, S) ->
    cl_write_public_proj(Epoch, Proj, true, S).

cl_write_public_proj(Epoch, Proj, SkipLocalWriteErrorP, S) ->
    %% Write to local public projection store first, and if it succeeds,
    %% then write to all remote public projection stores.
    cl_write_public_proj_local(Epoch, Proj, SkipLocalWriteErrorP, S).

cl_write_public_proj_local(Epoch, Proj, SkipLocalWriteErrorP,
                           #ch_mgr{myflu=MyFLU}=S) ->
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    Res0 = perhaps_call_t(
             S, Partitions, MyFLU,
             fun() -> machi_flu0:proj_write(MyFLU, Epoch, public, Proj) end),
    Continue = fun() ->
                  FLUs = Proj#projection.all_members -- [MyFLU],
                  cl_write_public_proj_remote(FLUs, Partitions, Epoch, Proj, S)
               end,
    case Res0 of
        ok ->
            Continue();
        _Else when SkipLocalWriteErrorP ->
            Continue();
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

do_cl_read_latest_public_projection(ReadRepairP,
                                    #ch_mgr{proj=Proj1, myflu=_MyFLU} = S) ->
    _Epoch1 = Proj1#projection.epoch_number,
    case cl_read_latest_public_projection(S) of
        {needs_repair, FLUsRs, Extra, S3} ->
            if not ReadRepairP ->
                    {not_unanimous, todoxyz, [{results, FLUsRs}|Extra], S3};
               true ->
                    {_Status, S4} = do_read_repair(FLUsRs, Extra, S3),
                    do_cl_read_latest_public_projection(ReadRepairP, S4)
            end;
        {UnanimousTag, Proj2, Extra, S3}=_Else ->
            {UnanimousTag, Proj2, Extra, S3}
    end.

cl_read_latest_public_projection(#ch_mgr{proj=CurrentProj}=S) ->
    #projection{all_members=All_list} = CurrentProj,
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    DoIt = fun(X) ->
                   case machi_flu0:proj_read_latest(X, public) of
                       {ok, P} -> P;
                       Else    -> Else
                   end
           end,
    Rs = [perhaps_call_t(S, Partitions, FLU, fun() -> DoIt(FLU) end) ||
             FLU <- All_list],
    FLUsRs = lists:zip(All_list, Rs),
    UnwrittenRs = [x || error_unwritten <- Rs],
    Ps = [Proj || {_FLU, Proj} <- FLUsRs, is_record(Proj, projection)],
    if length(UnwrittenRs) == length(Rs) ->
            {error_unwritten, FLUsRs, [todo_fix_caller_perhaps], S2};
       UnwrittenRs /= [] ->
            {needs_repair, FLUsRs, [flarfus], S2};
       true ->
            [{_Rank, BestProj}|_] = rank_and_sort_projections(Ps, CurrentProj),
            NotBestPs = [Proj || Proj <- Ps, Proj /= BestProj],
            UnanimousTag = if NotBestPs == [] -> unanimous;
                              true            -> not_unanimous
                           end,
            Extra = [{all_members_replied, length(Rs) == length(All_list)}],
            Best_FLUs = [FLU || {FLU, Projx} <- FLUsRs, Projx == BestProj],
            {UnanimousTag, BestProj, [{best_flus,Best_FLUs}|Extra], S2}
    end.

%% 1. Do the results contain a projection?
%%    perhaps figure that in cl_read_latest_public_projection()?
%% 2. Were there any error_unwritten?
%% 3. Repair the unwritten FLUs.
%% 4. Nothing to do with timeouts, right?  They're
%%    hopeless for the moment, need to wait.
%%
%% For read-repair, just choose the best and then brute-
%% force write, don't care about write status, then
%% repeat do_cl_read_latest_public_projection() ??

do_read_repair(FLUsRs, _Extra, #ch_mgr{proj=CurrentProj} = S) ->
    Unwrittens = [x || {_FLU, error_unwritten} <- FLUsRs],
    Ps = [Proj || {_FLU, Proj} <- FLUsRs, is_record(Proj, projection)],
    if Unwrittens == [] orelse Ps == [] ->
            {nothing_to_do, S};
       true ->
            %% We have at least one unwritten and also at least one proj.
            %% Pick the best one, then spam it everywhere.

            [{_Rank, BestProj}|_] = rank_and_sort_projections(Ps, CurrentProj),
            Epoch = BestProj#projection.epoch_number,

            %% We're doing repair, so use the flavor that will
            %% continue to all others even if there is an
            %% error_written on the local FLU.
            {_DontCare, _S2}=Res = cl_write_public_proj_skip_local_error(
                                                           Epoch, BestProj, S),
            Res
    end.

make_initial_projection(MyName, All_list, UPI_list, Repairing_list, Ps) ->
    make_projection(0, MyName, All_list, [], UPI_list, Repairing_list, Ps).

make_projection(EpochNum,
                MyName, All_list, Down_list, UPI_list, Repairing_list,
                Dbg) ->
    make_projection(EpochNum,
                    MyName, All_list, Down_list, UPI_list, Repairing_list,
                    Dbg, []).

make_projection(EpochNum,
                MyName, All_list, Down_list, UPI_list, Repairing_list,
                Dbg, Dbg2) ->
    P = #projection{epoch_number=EpochNum,
                    epoch_csum= <<>>,           % always checksums as <<>>
                    creation_time=now(),
                    author_server=MyName,
                    all_members=All_list,
                    down=Down_list,
                    upi=UPI_list,
                    repairing=Repairing_list,
                    dbg=Dbg,
                    dbg2=[]                     % always checksums as []
                   },
    P2 = update_projection_checksum(P),
    P2#projection{dbg2=Dbg2}.

update_projection_checksum(P) ->
    CSum = crypto:hash(sha, term_to_binary(P)),
    P#projection{epoch_csum=CSum}.

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
    P = make_projection(OldEpochNum + 1,
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
                      [{seed, Seed2}, {network_partitions, Partitions2},
                       {up_nodes, UpNodes}]),
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

-ifdef(TEST).
mps(P) ->
    make_projection_summary(P).
-endif. % TEST

make_projection_summary(#projection{epoch_number=EpochNum,
                                    all_members=_All_list,
                                    down=Down_list,
                                    author_server=Author,
                                    upi=UPI_list,
                                    repairing=Repairing_list,
                                    dbg=Dbg, dbg2=Dbg2}) ->
    [{epoch,EpochNum},{author,Author},
     {upi,UPI_list},{repair,Repairing_list},{down,Down_list},
     {d,Dbg}, {d2,Dbg2}].

roll_dice(N, RunEnv) ->
    Seed1 = proplists:get_value(seed, RunEnv),
    {Val, Seed2} = random:uniform_s(N, Seed1),
    {Val, replace(RunEnv, [{seed, Seed2}])}.

rank_and_sort_projections(Ps, CurrentProj) ->
    Epoch = lists:max([Proj#projection.epoch_number || Proj <- Ps]),
    MaxPs = [Proj || Proj <- Ps,
                     Proj#projection.epoch_number == Epoch],
    %% Sort with highest rank first (custom sort)
    lists:sort(fun({RankA,_}, {RankB,_}) -> RankA > RankB end,
               rank_projections(MaxPs, CurrentProj)).

%% Caller must ensure all Projs are of the same epoch number.
%% If the caller gives us projections with different epochs, we assume
%% that the caller is doing an OK thing.

rank_projections(Projs, CurrentProj) ->
    #projection{all_members=All_list} = CurrentProj,
    MemberRank = orddict:from_list(
                   lists:zip(All_list, lists:seq(1, length(All_list)))),
    N = length(All_list),
    [{rank_projection(Proj, MemberRank, N), Proj} || Proj <- Projs].

rank_projection(#projection{author_server=Author,
                            upi=UPI_list,
                            repairing=Repairing_list}, MemberRank, N) ->
    AuthorRank = orddict:fetch(Author, MemberRank),
    AuthorRank +
        (1*N + length(Repairing_list)) +
        (2*N + length(UPI_list)).

do_react_to_env(S) ->
    react_to_env_A10(S).

react_to_env_A10(S) ->
    react_to_env_A20(0, S).

react_to_env_A20(Retries, S) ->
    %% io:format(user, "current:  ~w\n", [make_projection_summary(S#ch_mgr.proj)]),
    {P_newprop, S2} = calc_projection(S, [{author_proc, react}]),
    %% io:format(user, "proposed: ~w\n", [make_projection_summary(Proposed)]),
    react_to_env_A30(Retries, P_newprop, S2).

react_to_env_A30(Retries, P_newprop, S) ->
    {UnanimousTag, P_latest, _Extra, S2} =
        do_cl_read_latest_public_projection(true, S),
    LatestUnanimousP = if UnanimousTag == unanimous     -> true;
                          UnanimousTag == not_unanimous -> false;
                          true -> exit({badbad, UnanimousTag})
                       end,
    react_to_env_A40(Retries, P_newprop, P_latest, LatestUnanimousP, S2).

react_to_env_A40(Retries, P_newprop, P_latest, LatestUnanimousP,
                 #ch_mgr{myflu=MyFLU, proj=P_current}=S) ->
    [{Rank_newprop, _}] = rank_projections([P_newprop], P_current),
    [{Rank_latest, _}] = rank_projections([P_latest], P_current),
    LatestAuthorDownP = lists:member(P_latest#projection.author_server,
                                     P_newprop#projection.down),

    %% Proj = S#ch_mgr.proj, if Proj#projection.epoch_number >= 7 -> ?Dw({Rank_newprop,Rank_latest}); true -> ok end,

    if
        P_latest#projection.epoch_number > P_current#projection.epoch_number
        orelse
        not LatestUnanimousP ->

            %% 1st clause: someone else has written a newer projection
            %% 2nd clause: a network partition has healed, revealing a
            %%             differing opinion.
            react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                             Rank_newprop, Rank_latest, S);

        P_latest#projection.epoch_number < P_current#projection.epoch_number
        orelse
        P_latest /= P_current ->

            %% Both of these cases are rare.  Elsewhere, the code
            %% assumes that the local FLU's projection store is always
            %% available, so reads & writes to it aren't going to fail
            %% willy-nilly.  If that assumption is true, then we can
            %% reason as follows:
            %%
            %% a. If we can always read from the local FLU projection
            %% store, then the 1st clause isn't possible because
            %% P_latest's epoch # must be at least as large as
            %% P_current's epoch #
            %%
            %% b. If P_latest /= P_current, then there can't be a
            %% unanimous reply for P_latest, so the earlier 'if'
            %% clause would be triggered and so we could never reach
            %% this clause.
            %%
            %% I'm keeping this 'if' clause just in case the local FLU
            %% projection store assumption changes.
            react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                             Rank_newprop, Rank_latest, S);

        %% A40a (see flowchart)
        Rank_newprop > Rank_latest ->
            react_to_env_C300(P_newprop, S);

        %% A40b (see flowchart)
        P_latest#projection.author_server == MyFLU
        andalso
        (P_newprop#projection.upi /= P_latest#projection.upi
         orelse
         P_newprop#projection.repairing /= P_latest#projection.repairing) ->
            react_to_env_C300(P_newprop, S);

        %% A40c (see flowchart)
        LatestAuthorDownP ->

            %% TODO: I believe that membership in the
            %% P_newprop#projection.down is not sufficient for long
            %% chains.  Rather, we ought to be using a full broadcast
            %% gossip of server up status.
            %%
            %% Imagine 5 servers in an "Olympic Rings" style
            %% overlapping network paritition, where ring1 = upper
            %% leftmost and ring5 = upper rightmost.  It's both
            %% possible and desirable for ring5's projection to be
            %% seen (public) by ring1.  Ring5's projection's rank is
            %% definitely higher than ring1's proposed projection's
            %% rank ... but we're in a crazy netsplit where:
            %%  * if we accept ring5's proj: only one functioning chain
            %%    ([ring4,ring5] but stable
            %%  * if we accept ring1's proj: two functioning chains
            %%    ([ring1,ring2] and [ring4,ring5] indepependently)
            %%    but unstable: we're probably going to flap back & forth?!

            ?D({{{{{yoyoyo_A40c}}}}}),
            react_to_env_C300(P_newprop, S);

        true ->
            {{no_change, P_latest#projection.epoch_number}, S}
    end.

react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                 Rank_newprop, Rank_latest, #ch_mgr{name=MyName}=S) ->
    if
        LatestUnanimousP ->
            react_to_env_C100(P_newprop, P_latest, S);

        Retries > 2 ->

            %% The author of P_latest is too slow or crashed.
            %% Let's try to write P_newprop and see what happens!
            react_to_env_C300(P_newprop, S);

        Rank_latest >= Rank_newprop
        andalso
        P_latest#projection.author_server /= MyName ->

            %% Give the author of P_latest an opportunite to write a
            %% new projection in a new epoch to resolve this mixed
            %% opinion.
            react_to_env_C200(Retries, P_latest, S);

        true ->

            %% P_newprop is best, so let's write it.
            react_to_env_C300(P_newprop, S)
    end.

react_to_env_C100(P_newprop, P_latest,
                  #ch_mgr{myflu=MyFLU, proj=P_current}=S) ->
    case projection_transition_is_sane(P_current, P_latest, MyFLU) of
        true ->
            react_to_env_C110(P_latest, S);
        _AnyOtherReturnValue ->
            %% P_latest is known to be crap.
            %% By process of elimination, P_newprop is best,
            %% so let's write it.
            react_to_env_C300(P_newprop, S)
    end.

react_to_env_C110(P_latest, #ch_mgr{myflu=MyFLU} = S) ->
    %% TOOD: Should we carry along any extra info that that would be useful
    %%       in the dbg2 list?
    Extra_todo = [],
    RunEnv = S#ch_mgr.runenv,
    UpNodes = proplists:get_value(up_nodes, RunEnv),
    P_latest2 = update_projection_dbg2(
                  P_latest,
                  [{up_nodz, UpNodes},{hooray, {v2, date(), time()}}|Extra_todo]),
    Epoch = P_latest2#projection.epoch_number,
    ok = machi_flu0:proj_write(MyFLU, Epoch, private, P_latest2),
    react_to_env_C120(P_latest, S).

react_to_env_C120(P_latest, S) ->
    {{now_using, P_latest#projection.epoch_number},
     S#ch_mgr{proj=P_latest, proj_proposed=none}}.

react_to_env_C200(Retries, P_latest, S) ->
    try
        yo:tell_author_yo(P_latest#projection.author_server)
    catch Type:Err ->
            io:format(user, "TODO: tell_author_yo is broken: ~p ~p\n",
                      [Type, Err])
    end,
    react_to_env_C210(Retries, S).

react_to_env_C210(Retries, S) ->
    %% TODO: implement the ranked sleep thingie?
    timer:sleep(10),
    react_to_env_C220(Retries, S).

react_to_env_C220(Retries, S) ->
    react_to_env_A20(Retries + 1, S).

react_to_env_C300(#projection{epoch_number=Epoch}=P_newprop, S) ->
    P_newprop2 = P_newprop#projection{epoch_number=Epoch + 1},
    react_to_env_C310(update_projection_checksum(P_newprop2), S).

react_to_env_C310(P_newprop, S) ->
    Epoch = P_newprop#projection.epoch_number,
    {_Res, S2} = cl_write_public_proj_skip_local_error(Epoch, P_newprop, S),
MyFLU=S#ch_mgr.myflu,
    ?D({c310, MyFLU, Epoch, _Res}),
    react_to_env_A10(S2).

projection_transition_is_sane(P1, P2) ->
    projection_transition_is_sane(P1, P2, undefined).

projection_transition_is_sane(
  #projection{epoch_number=Epoch1,
              epoch_csum=CSum1,
              creation_time=CreationTime1,
              author_server=AuthorServer1,
              all_members=All_list1,
              down=Down_list1,
              upi=UPI_list1,
              repairing=Repairing_list1,
              dbg=Dbg1} = P1,
  #projection{epoch_number=Epoch2,
              epoch_csum=CSum2,
              creation_time=CreationTime2,
              author_server=AuthorServer2,
              all_members=All_list2,
              down=Down_list2,
              upi=UPI_list2,
              repairing=Repairing_list2,
              dbg=Dbg2} = P2,
  RelativeToServer) ->
 try
    true = is_integer(Epoch1) andalso is_integer(Epoch2),
    true = is_binary(CSum1) andalso is_binary(CSum2),
    {_,_,_} = CreationTime1,
    {_,_,_} = CreationTime2,
    true = is_atom(AuthorServer1) andalso is_atom(AuthorServer2), % todo will probably change
    true = is_list(All_list1) andalso is_list(All_list2),
    true = is_list(Down_list1) andalso is_list(Down_list2),
    true = is_list(UPI_list1) andalso is_list(UPI_list2),
    true = is_list(Repairing_list1) andalso is_list(Repairing_list2),
    true = is_list(Dbg1) andalso is_list(Dbg2),

    true = Epoch2 > Epoch1,
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
                    %% -------------------------------------------------
                    %% [d,c,b,a] -> [c,a]
                    %% [d,c,b,a] -> [c,a,repair_finished_added_to_tail].
                    NotUPI2 = (Down_list2 ++ Repairing_list2),
                    case lists:prefix(UPI_list1 -- NotUPI2, UPI_list2) of
                        true ->
                            true;
                        false ->
                            %% Here's a possible failure scenario:
                            %% UPI_list1        -> UPI_list2
                            %% Repairing_list1  -> Repairing_list2
                            %% -----------------------------------
                            %% [a,b,c] author=a -> [c,a] author=c
                            %% []                  [b]
                            %% 
                            %% ... where RelativeToServer=b.  In this case, b
                            %% has been partitions for a while and has only
                            %% now just learned of several epoch transitions.
                            %% If the author of both is also in the UPI of
                            %% both, then those authors would not have allowed
                            %% a bad transition, so we will assume this
                            %% transition is OK.
?D(aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa),
                            lists:member(AuthorServer1, UPI_list1)
                            andalso
                            lists:member(AuthorServer2, UPI_list2)
                    end
            end;
       true ->
            true
    end,
    true = lists:prefix(UPI_common_prefix, UPI_list1),
    true = lists:prefix(UPI_common_prefix, UPI_list2),
    UPI_1_suffix = UPI_list1 -- UPI_common_prefix,
    UPI_2_suffix = UPI_list2 -- UPI_common_prefix,

    MoreCheckingP =
        RelativeToServer == undefined
        orelse
        not (lists:member(RelativeToServer, Down_list2) orelse
             lists:member(RelativeToServer, Repairing_list2)),
    
    if not MoreCheckingP ->
            ok;
        MoreCheckingP ->
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
            %% ?D(UPI_2_suffix),
            %% ?D(UPI_2_suffix_from_UPI1),
            %% ?D(UPI_2_suffix_from_Repairing1),
            %% ?D(UPI_2_suffix_from_UPI1 ++ UPI_2_suffix_from_Repairing1),
            UPI_2_suffix = UPI_2_suffix_from_UPI1 ++ UPI_2_suffix_from_Repairing1,
            ok
    end,
    true
 catch
     _Type:_Err ->
         S1 = make_projection_summary(P1),
         S2 = make_projection_summary(P2),
         Trace = erlang:get_stacktrace(),
         {err, _Type, _Err, from, S1, to, S2, relative_to, RelativeToServer,
          stack, Trace}
 end.

find_common_prefix([], _) ->
    [];
find_common_prefix(_, []) ->
    [];
find_common_prefix([H|L1], [H|L2]) ->
    [H|find_common_prefix(L1, L2)];
find_common_prefix(_, _) ->
    [].

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
            %% ?D({perhaps_call, S#ch_mgr.myflu, FLU, Partitions}),
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

        %% If/when calculate_projection_internal_old() disappears, then
        %% get rid of the comprehension below ... start/ping/stop is
        %% good enough for smoke0.
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
        %% ?D(x),
        {ok, _P1} = test_calc_projection(M0, false),

        _ = test_calc_proposed_projection(M0),
        {remote_write_results,
         [{b,ok},{c,ok}]} = test_write_proposed_projection(M0),
        {unanimous, P1, Extra1} = test_read_latest_public_projection(M0, false),
        %% ?Dw(make_projection_summary(P1)),
        %% ?D(Extra1),

        ok
    after
        ok = ?MGR:stop(M0),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb),
        ok = machi_flu0:stop(FLUc)        
    end.

nonunanimous_setup_and_fix_test() ->
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, FLUb} = machi_flu0:start_link(b),
    I_represent = I_am = a,
    {ok, Ma} = ?MGR:start_link(I_represent, [a,b], {1,2,3}, 0, 100, I_am),
    {ok, Mb} = ?MGR:start_link(b, [a,b], {4,5,6}, 0, 100, b),
    try
        {ok, P1} = test_calc_projection(Ma, false),

        P1a = update_projection_checksum(
                P1#projection{down=[b], upi=[a], dbg=[{hackhack, ?LINE}]}),
        P1b = update_projection_checksum(
                P1#projection{author_server=b, creation_time=now(),
                              down=[a], upi=[b], dbg=[{hackhack, ?LINE}]}),
        P1Epoch = P1#projection.epoch_number,
        ok = machi_flu0:proj_write(FLUa, P1Epoch, public, P1a),
        ok = machi_flu0:proj_write(FLUb, P1Epoch, public, P1b),

        ?D(x),
        {not_unanimous,_,_}=_XX = test_read_latest_public_projection(Ma, false),
        ?Dw(_XX),
        {not_unanimous,_,_}=_YY = test_read_latest_public_projection(Ma, true),
        %% The read repair here doesn't automatically trigger the creation of
        %% a new projection (to try to create a unanimous projection).  So
        %% we expect nothing to change when called again.
        {not_unanimous,_,_}=_YY = test_read_latest_public_projection(Ma, true),

        {now_using, _} = test_react_to_env(Ma),
        {unanimous,P2,E2} = test_read_latest_public_projection(Ma, false),
        {ok, P2pa} = machi_flu0:proj_read_latest(FLUa, private),
        P2 = P2pa#projection{dbg2=[]},

        %% FLUb should still be using proj #0 for its private use
        {ok, P0pb} = machi_flu0:proj_read_latest(FLUb, private),
        0 = P0pb#projection.epoch_number,

        %% Poke FLUb to react ... should be using the same private proj
        %% as FLUa.
        {now_using, _} = test_react_to_env(Mb),
        {ok, P2pb} = machi_flu0:proj_read_latest(FLUb, private),
        P2 = P2pb#projection{dbg2=[]},

        ok
    after
        ok = ?MGR:stop(Ma),
        ok = ?MGR:stop(Mb),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb)
    end.

zoof_test() ->
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, FLUb} = machi_flu0:start_link(b),
    {ok, FLUc} = machi_flu0:start_link(c),
    I_represent = I_am = a,
    {ok, Ma} = ?MGR:start_link(I_represent, [a,b,c], {1,2,3}, 50, 90, I_am),
    {ok, Mb} = ?MGR:start_link(b, [a,b,c], {4,5,6}, 50, 90, b),
    {ok, Mc} = ?MGR:start_link(c, [a,b,c], {4,5,6}, 50, 90, c),
?D(x),
    try
        {ok, P1} = test_calc_projection(Ma, false),
        P1Epoch = P1#projection.epoch_number,
        ok = machi_flu0:proj_write(FLUa, P1Epoch, public, P1),
        ok = machi_flu0:proj_write(FLUb, P1Epoch, public, P1),
        ok = machi_flu0:proj_write(FLUc, P1Epoch, public, P1),

        {now_using, XX1} = test_react_to_env(Ma),
        ?D(XX1),
        {QQ,QQP2,QQE2} = test_read_latest_public_projection(Ma, false),
        ?D(QQ),
        ?Dw(make_projection_summary(QQP2)),
        ?D(QQE2),
        %% {unanimous,P2,E2} = test_read_latest_public_projection(Ma, false),

        Parent = self(),
        DoIt = fun() ->
                       Pids = [spawn(fun() ->
                                             [begin
                                                  erlang:yield(),
                                                  Res = test_react_to_env(MMM),
                                                  Res=Res %% ?D({self(), Res})
                                              end || _ <- lists:seq(1,7)],
                                             Parent ! done
                                     end) || MMM <- [Ma, Mb, Mc] ],
                       [receive
                            done ->
                                ok
                        after 5000 ->
                                exit(icky_timeout)
                        end || _ <- Pids]
               end,

        DoIt(),
        [test_reset_thresholds(M, 0, 100) || M <- [Ma, Mb, Mc]],
        DoIt(),

        %% [begin
        %%      La = machi_flu0:proj_list_all(FLU, Type),
        %%      [io:format(user, "~p ~p ~p: ~w\n", [FLUName, Type, Epoch, make_projection_summary(catch element(2,machi_flu0:proj_read(FLU, Epoch, Type)))]) || Epoch <- La]
        %%  end || {FLUName, FLU} <- [{a, FLUa}, {b, FLUb}],
        %%         Type <- [public, private] ],

        %% Dump the public
        [begin
             La = machi_flu0:proj_list_all(FLU, Type),
             [io:format(user, "~p ~p ~p: ~w\n", [FLUName, Type, Epoch, make_projection_summary(catch element(2,machi_flu0:proj_read(FLU, Epoch, Type)))]) || Epoch <- La]
         end || {FLUName, FLU} <- [{a, FLUa}, {b, FLUb}, {c, FLUc}],
                Type <- [public] ],

        Namez = [{a, FLUa}, {b, FLUb}, {c, FLUc}],
        UniquePrivateEs =
            lists:usort(lists:flatten(
                          [machi_flu0:proj_list_all(FLU, private) ||
                              {_FLUName, FLU} <- Namez])),
        DumbFinderBackward =
            fun(FLU) ->
                    fun(E, error_unwritten) ->
                            case machi_flu0:proj_read(FLU, E, private) of
                                {ok, T} -> T;
                                Else    -> Else
                            end;
                       (_E, Acc) ->
                            Acc
                    end
            end,
                             
        [begin
             io:format(user, "~p private: ~w\n",
                       [FLUName,
                        make_projection_summary(
                          lists:foldl(DumbFinderBackward(FLU),
                                      error_unwritten,
                                      lists:seq(Epoch, 0, -1)))]),
             if FLUName == c -> io:format(user, "\n", []); true -> ok end
         end || Epoch <- UniquePrivateEs, {FLUName, FLU} <- Namez],

        ok
    after
        ok = ?MGR:stop(Ma),
        ok = ?MGR:stop(Mb),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb)
    end.

-endif.
