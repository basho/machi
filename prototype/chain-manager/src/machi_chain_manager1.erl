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

%% TODO: I am going to sever the connection between the flowchart and the
%%       code.  That diagram is really valuable, but it also takes a long time
%%       to make any kind of edit; the process is too slow.  This is a todo
%%       item a reminder that the flowchart is important documentation and
%%       must be brought back into sync with the code soon.

-behaviour(gen_server).

-include("machi.hrl").
-define(D(X), io:format(user, "~s ~p\n", [??X, X])).
-define(Dw(X), io:format(user, "~s ~w\n", [??X, X])).

%% Keep a history of our flowchart execution in the process dictionary.
-define(REACT(T), put(react, [T|get(react)])).

%% API
-export([start_link/3, start_link/4, stop/1, ping/1,
         calculate_projection_internal_old/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([make_projection_summary/1, projection_transitions_are_sane/2]).

-ifdef(TEST).

-export([test_calc_projection/2,
         test_calc_proposed_projection/1,
         test_write_proposed_projection/1,
         test_read_latest_public_projection/2,
         test_react_to_env/1]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif. %TEST

start_link(MyName, All_list, MyFLUPid) ->
    start_link(MyName, All_list, MyFLUPid, []).

start_link(MyName, All_list, MyFLUPid, MgrOpts) ->
    gen_server:start_link(?MODULE, {MyName, All_list, MyFLUPid, MgrOpts}, []).

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
    gen_server:call(Pid, {test_read_latest_public_projection, ReadRepairP},
                    infinity).

test_react_to_env(Pid) ->
    gen_server:call(Pid, {test_react_to_env}, infinity).

-endif. % TEST

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({MyName, All_list, MyFLUPid, MgrOpts}) ->
    RunEnv = [%% {seed, Seed},
              {seed, now()},
              {network_partitions, []},
              {network_islands, []},
              {flapping_i, []},
              {up_nodes, not_init_yet}],
    BestProj = make_initial_projection(MyName, All_list, All_list,
                                       [], []),
    NoneProj = make_initial_projection(MyName, All_list, [],
                                       [], []),
    S = #ch_mgr{init_finished=false,
                name=MyName,
                proj=NoneProj,
                proj_history=queue:new(),
                myflu=MyFLUPid, % pid or atom local name
                flap_limit=length(All_list) + 1,
                %% TODO 2015-03-04: revisit, should this constant be bigger?
                %% flap_limit=length(All_list) + 5,
                runenv=RunEnv,
                opts=MgrOpts},

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
handle_call({calculate_projection_internal_old}, _From,
            #ch_mgr{name=MyName}=S) ->
    RelativeToServer = MyName,
    {Reply, S2} = calc_projection(S, RelativeToServer, []),
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
handle_call({test_calc_projection, KeepRunenvP}, _From,
            #ch_mgr{name=MyName}=S) ->
    RelativeToServer = MyName,
    {P, S2} = calc_projection(S, RelativeToServer, []),
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
handle_call(_Call, _From, S) ->
    {reply, whaaaaaaaaaa, S}.

handle_cast(_Cast, #ch_mgr{init_finished=false} = S) ->
    {noreply, S};
handle_cast({test_calc_proposed_projection}, #ch_mgr{name=MyName}=S) ->
    RelativeToServer = MyName,
    {Proj, S2} = calc_projection(S, RelativeToServer, []),
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
    %% We're going to be very care-free about this write because we'll rely
    %% on the read side to do any read repair.
    DoIt = fun(X) -> machi_flu0:proj_write(X, Epoch, public, Proj) end,
    Rs = [{FLU, perhaps_call_t(S, Partitions, FLU, fun() -> DoIt(FLU) end)} ||
             FLU <- FLUs],
    {{remote_write_results, Rs}, S}.

do_cl_read_latest_public_projection(ReadRepairP,
                                    #ch_mgr{proj=Proj1, myflu=_MyFLU} = S) ->
    _Epoch1 = Proj1#projection.epoch_number,
    case cl_read_latest_projection(public, S) of
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

cl_read_latest_projection(ProjectionType, #ch_mgr{proj=CurrentProj}=S) ->
    #projection{all_members=All_list} = CurrentProj,
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    DoIt = fun(X) ->
                   case machi_flu0:proj_read_latest(X, ProjectionType) of
                       {ok, P} -> P;
                       Else    -> Else
                   end
           end,
    Rs = [perhaps_call_t(S, Partitions, FLU, fun() -> DoIt(FLU) end) ||
             FLU <- All_list],
    FLUsRs = lists:zip(All_list, Rs),
    UnwrittenRs = [x || error_unwritten <- Rs],
    Ps = [Proj || {_FLU, Proj} <- FLUsRs, is_record(Proj, projection)],
    BadAnswerFLUs = [FLU || {FLU, Answer} <- FLUsRs,
                            not is_record(Answer, projection)],
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
            Extra2 = [{unanimous_flus,Best_FLUs},
                      {not_unanimous_flus, All_list --
                                                 (Best_FLUs ++ BadAnswerFLUs)},
                      {bad_answer_flus, BadAnswerFLUs},
                      {not_unanimous_answers, NotBestPs}|Extra],
            {UnanimousTag, BestProj, Extra2, S2}
    end.

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

calc_projection(#ch_mgr{proj=LastProj, runenv=RunEnv} = S, RelativeToServer,
                Dbg) ->
    OldThreshold = proplists:get_value(old_threshold, RunEnv),
    NoPartitionThreshold = proplists:get_value(no_partition_threshold, RunEnv),
    calc_projection(OldThreshold, NoPartitionThreshold, LastProj,
                    RelativeToServer, Dbg, S).

%% OldThreshold: Percent chance of using the old/previous network partition list
%% NoPartitionThreshold: If the network partition changes, what percent chance
%%                       that there are no partitions at all?

calc_projection(_OldThreshold, _NoPartitionThreshold, LastProj,
                RelativeToServer, Dbg,
                #ch_mgr{name=MyName, runenv=RunEnv1}=S) ->
    #projection{epoch_number=OldEpochNum,
                all_members=All_list,
                upi=OldUPI_list,
                repairing=OldRepairing_list
               } = LastProj,
    LastUp = lists:usort(OldUPI_list ++ OldRepairing_list),
    AllMembers = (S#ch_mgr.proj)#projection.all_members,
    {Up, Partitions, RunEnv2} = calc_up_nodes(MyName,
                                              AllMembers, RunEnv1),

    NewUp = Up -- LastUp,
    Down = AllMembers -- Up,

    NewUPI_list = [X || X <- OldUPI_list, lists:member(X, Up)],
    Repairing_list2 = [X || X <- OldRepairing_list, lists:member(X, Up)],
    {NewUPI_list3, Repairing_list3, RunEnv3} =
        case {NewUp, Repairing_list2} of
            {[], []} ->
D_foo=[],
                {NewUPI_list, [], RunEnv2};
            {[], [H|T]} when RelativeToServer == hd(NewUPI_list) ->
                %% The author is head of the UPI list.  Let's see if
                %% *everyone* in the UPI+repairing lists are using our
                %% projection.  This is to simulate a requirement that repair
                %% a real repair process cannot take place until the chain is
                %% stable, i.e. everyone is in the same epoch.

                %% TODO create a real API call for fetching this info.
                SameEpoch_p = check_latest_private_projections(
                                tl(NewUPI_list) ++ Repairing_list2,
                                S#ch_mgr.proj, Partitions, S),
                if not SameEpoch_p ->
D_foo=[],
                        {NewUPI_list, OldRepairing_list, RunEnv2};
                   true ->
D_foo=[{repair_airquote_done, {we_agree, (S#ch_mgr.proj)#projection.epoch_number}}],
                        {NewUPI_list ++ [H], T, RunEnv2}
                end;
            {_, _} ->
D_foo=[],
                {NewUPI_list, OldRepairing_list, RunEnv2}
        end,
    Repairing_list4 = case NewUp of
                          []    -> Repairing_list3;
                          NewUp -> Repairing_list3 ++ NewUp
                      end,
    Repairing_list5 = Repairing_list4 -- Down,

    TentativeUPI = NewUPI_list3,
    TentativeRepairing = Repairing_list5,

    {NewUPI, NewRepairing} =
        if TentativeUPI == [] andalso TentativeRepairing /= [] ->
                [FirstRepairing|TailRepairing] = TentativeRepairing,
                {[FirstRepairing], TailRepairing};
           true ->
                {TentativeUPI, TentativeRepairing}
        end,

    P = make_projection(OldEpochNum + 1,
                        MyName, All_list, Down, NewUPI, NewRepairing,
                        D_foo ++
                        Dbg ++ [{ps, Partitions},{nodes_up, Up}]),
    {P, S#ch_mgr{runenv=RunEnv3}}.

check_latest_private_projections(FLUs, MyProj, Partitions, S) ->
    FoldFun = fun(_FLU, false) ->
                      false;
                 (FLU, true) ->
                      F = fun() ->
                                  machi_flu0:proj_read_latest(FLU, private)
                          end,
                      case perhaps_call_t(S, Partitions, FLU, F) of
                          {ok, RemotePrivateProj} ->
                              if MyProj#projection.epoch_number ==
                                 RemotePrivateProj#projection.epoch_number
                                 andalso
                                 MyProj#projection.epoch_csum ==
                                 RemotePrivateProj#projection.epoch_csum ->
                                      true;
                                 true ->
                                      false
                              end;
                          _ ->
                              false
                      end
              end,
    lists:foldl(FoldFun, true, FLUs).

calc_up_nodes(#ch_mgr{name=MyName, proj=Proj, runenv=RunEnv1}=S) ->
    AllMembers = Proj#projection.all_members,
    {UpNodes, Partitions, RunEnv2} =
        calc_up_nodes(MyName, AllMembers, RunEnv1),
    {UpNodes, Partitions, S#ch_mgr{runenv=RunEnv2}}.

calc_up_nodes(MyName, AllMembers, RunEnv1) ->
    {Partitions2, Islands2} = machi_partition_simulator:get(AllMembers),
    catch ?REACT({partitions,Partitions2}),
    catch ?REACT({islands,Islands2}),
    UpNodes = lists:sort(
                [Node || Node <- AllMembers,
                         not lists:member({MyName, Node}, Partitions2),
                         not lists:member({Node, MyName}, Partitions2)]),
    RunEnv2 = replace(RunEnv1,
                      [{network_partitions, Partitions2},
                       {network_islands, Islands2},
                       {up_nodes, UpNodes}]),
    {UpNodes, Partitions2, RunEnv2}.

replace(PropList, Items) ->
    lists:foldl(fun({Key, Val}, Ps) ->
                        lists:keyreplace(Key, 1, Ps, {Key,Val})
                end, PropList, Items).

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
    %% (AuthorRank-AuthorRank) +                   % feels unstable????
    AuthorRank +                                % feels stable
        (  N * length(Repairing_list)) +
        (N*N * length(UPI_list)).

do_react_to_env(S) ->
    put(react, []),
    react_to_env_A10(S).

react_to_env_A10(S) ->
    ?REACT(a10),
    react_to_env_A20(0, S).

react_to_env_A20(Retries, S) ->
    ?REACT(a20),
    {UnanimousTag, P_latest, ReadExtra, S2} =
        do_cl_read_latest_public_projection(true, S),

    %% The UnanimousTag isn't quite sufficient for our needs.  We need
    %% to determine if *all* of the UPI+Repairing FLUs are members of
    %% the unanimous server replies.
    UnanimousFLUs = lists:sort(proplists:get_value(unanimous_flus, ReadExtra)),
    UPI_Repairing_FLUs = lists:sort(P_latest#projection.upi ++
                                    P_latest#projection.repairing),
    All_UPI_Repairing_were_unanimous = UPI_Repairing_FLUs == UnanimousFLUs,
    %% TODO: investigate if the condition below is more correct?
    %% All_UPI_Repairing_were_unanimous = (UPI_Repairing_FLUs -- UnanimousFLUs) == [],
    LatestUnanimousP =
        if UnanimousTag == unanimous
           andalso
           All_UPI_Repairing_were_unanimous ->
                ?REACT({a20,?LINE}),
                true;
           UnanimousTag == unanimous ->
                ?REACT({a20,?LINE,UPI_Repairing_FLUs,UnanimousFLUs}),
                false;
           UnanimousTag == not_unanimous ->
                ?REACT({a20,?LINE}),
                false;
           true ->
                exit({badbad, UnanimousTag})
        end,
    react_to_env_A30(Retries, P_latest,
                     LatestUnanimousP, S2).

react_to_env_A30(Retries, P_latest, LatestUnanimousP,
                 #ch_mgr{name=MyName, flap_limit=FlapLimit} = S) ->
    ?REACT(a30),
    RelativeToServer = MyName,
    {P_newprop1, S2} = calc_projection(S, RelativeToServer,
                                       []),

    {S3, P_newprop2} = calculate_flaps(P_newprop1, FlapLimit, S2),

    P_newprop2_all_hosed =
        proplists:get_value(all_hosed,
                            proplists:get_value(flapping_i, P_newprop2#projection.dbg, [])),
    NewDown = lists:usort(P_newprop2#projection.down ++ P_newprop2_all_hosed),

    %% 2015-03-04: This will definitely stop a self-identified hosed machine
    %%             from participating.  However, it doesn't prevent problems
    %%             like UPI=[x],all_hosed=[x], either.
    %%
    case lists:member(MyName, P_newprop2_all_hosed) of
        true ->
            io:format(user, "{~w hosed}", [MyName]),
            ?REACT({a30, i_am_hosed}),
            react_to_env_A50(P_latest, S);
        false ->
            ?REACT({a30, i_am_normal}),
            react_to_env_A40(Retries, P_newprop2, P_latest,
                             LatestUnanimousP, S3)
    end.

react_to_env_A40(Retries, P_newprop, P_latest, LatestUnanimousP,
                 #ch_mgr{name=MyName, proj=P_current}=S) ->
    ?REACT(a40),
    [{Rank_newprop, _}] = rank_projections([P_newprop], P_current),
    [{Rank_latest, _}] = rank_projections([P_latest], P_current),
    LatestAuthorDownP = lists:member(P_latest#projection.author_server,
                                     P_newprop#projection.down),

    if
        P_latest#projection.epoch_number > P_current#projection.epoch_number
        orelse
        not LatestUnanimousP ->
            ?REACT({a40, ?LINE,P_latest#projection.epoch_number > P_current#projection.epoch_number, not LatestUnanimousP}),

            %% 1st clause: someone else has written a newer projection
            %% 2nd clause: a network partition has healed, revealing a
            %%             differing opinion.
            react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                             Rank_newprop, Rank_latest, S);

        P_latest#projection.epoch_number < P_current#projection.epoch_number
        orelse
        P_latest /= P_current ->
            ?REACT({a40, ?LINE}),

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
            ?REACT({a40, ?LINE}),

            %% TODO: There may be an "improvement" here.  If we're the
            %% highest-ranking FLU in the all_members list, then if we make a
            %% projection where our UPI list is the same as P_latest's, and
            %% our repairing list is the same as P_latest's, then it may not
            %% be necessary to write our projection: it doesn't "improve"
            %% anything UPI-wise or repairing-wise.  But it isn't clear to me
            %% if it's 100% correct to "improve" here and skip writing
            %% P_newprop, yet.
            react_to_env_C300(P_newprop, P_latest, S);

        %% A40b (see flowchart)
        P_latest#projection.author_server == MyName
        andalso
        (P_newprop#projection.upi /= P_latest#projection.upi
         orelse
         P_newprop#projection.repairing /= P_latest#projection.repairing) ->
            ?REACT({a40, ?LINE}),

            react_to_env_C300(P_newprop, P_latest, S);

        %% A40c (see flowchart)
        LatestAuthorDownP ->
            ?REACT({a40, ?LINE}),

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
            react_to_env_C300(P_newprop, P_latest, S);

        true ->
            ?REACT({a40, ?LINE}),

            react_to_env_A50(P_latest, S)
    end.

react_to_env_A50(P_latest, S) ->
    ?REACT({a50, ?LINE}),

    {{no_change, P_latest#projection.epoch_number}, S}.

react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                 Rank_newprop, Rank_latest,
                 #ch_mgr{name=MyName, flap_limit=FlapLimit}=S) ->
    ?REACT(b10),

    P_newprop_flap_count =
        proplists:get_value(flap_count,
                            proplists:get_value(flapping_i, P_newprop#projection.dbg, [])),
    LatestAllFlapCounts =
        proplists:get_value(all_flap_counts,
                            proplists:get_value(flapping_i, P_latest#projection.dbg, []),
                            []),
    P_latest_trans_flap_count = my_find_minmost(LatestAllFlapCounts),
    P_latest_all_hosed =
        proplists:get_value(all_hosed,
                            proplists:get_value(flapping_i, P_latest#projection.dbg, [])),

    if
        LatestUnanimousP ->
            ?REACT({b10, ?LINE}),
            put(b10_hack, false),

            react_to_env_C100(P_newprop, P_latest, S);

        P_newprop_flap_count >= FlapLimit ->
            %% I am flapping ... what else do I do?
            B10Hack = get(b10_hack),
            if B10Hack == false andalso P_newprop_flap_count - FlapLimit - 3 =< 0 -> io:format(user, "{FLAP: ~w flaps ~w}!\n", [S#ch_mgr.name, P_newprop_flap_count]), put(b10_hack, true); true -> ok end,

            if
                P_latest_trans_flap_count >= FlapLimit ->
                    %% Everyone that's flapping together now has flap_count
                    %% that's larger than the limit.  So it's safe and good
                    %% to stop here, so we can break the cycle of flapping.
                    react_to_env_A50(P_latest, S);

                true ->
                    %% It is our moral imperative to write so that the flap
                    %% cycle continues enough times so that everyone notices
                    %% and thus the earlier clause above fires.
                    P_latest_all_hosed=P_latest_all_hosed,
                    react_to_env_C300(P_newprop, P_latest, S)

               %% Rank_latest =< Rank_newprop ->
               %%      {_, _, USec} = os:timestamp(),
               %%      %% If we always go to C200, then we can deadlock sometimes.
               %%      %% So we roll the dice.
               %%      %% TODO: make this PULSE-friendly!
               %%      if USec rem 3 == 0 ->
               %%              react_to_env_A50(P_latest, S);
               %%         true ->
               %%              react_to_env_C200(Retries, P_latest, S)
               %%      end;
                %% true ->
                %%     react_to_env_A50(P_latest, S)
            end;

        Retries > 2 ->
            ?REACT({b10, ?LINE}),
            put(b10_hack, false),

            %% The author of P_latest is too slow or crashed.
            %% Let's try to write P_newprop and see what happens!
            react_to_env_C300(P_newprop, P_latest, S);

        Rank_latest >= Rank_newprop
        andalso
        P_latest#projection.author_server /= MyName ->
            ?REACT({b10, ?LINE}),
            put(b10_hack, false),

            %% Give the author of P_latest an opportunite to write a
            %% new projection in a new epoch to resolve this mixed
            %% opinion.
            react_to_env_C200(Retries, P_latest, S);

        true ->
            ?REACT({b10, ?LINE}),
            put(b10_hack, false),

            %% P_newprop is best, so let's write it.
            react_to_env_C300(P_newprop, P_latest, S)
    end.

react_to_env_C100(P_newprop, P_latest,
                  #ch_mgr{name=MyName, proj=P_current}=S) ->
    ?REACT(c100),
    I_am_UPI_in_newprop_p = lists:member(MyName, P_newprop#projection.upi),
    I_am_Repairing_in_latest_p = lists:member(MyName,
                                              P_latest#projection.repairing),
    ShortCircuit_p =
        P_latest#projection.epoch_number > P_current#projection.epoch_number
        andalso
        I_am_UPI_in_newprop_p
        andalso
        I_am_Repairing_in_latest_p,

    case {ShortCircuit_p, projection_transition_is_sane(P_current, P_latest,
                                                        MyName)} of
        {true, _} ->
            ?REACT({c100, repairing_short_circuit}),
            %% Someone else believes that I am repairing.  We assume
            %% that nobody is being Byzantine, so we'll believe that I
            %% am/should be repairing.  We ignore our proposal and try
            %% to go with the latest.
            react_to_env_C110(P_latest, S);
        {_, true} ->
            ?REACT({c100, sane}),
            react_to_env_C110(P_latest, S);
        {_, _AnyOtherReturnValue} ->
            ?REACT({c100, not_sane}),
            %% P_latest is not sane.
            %% By process of elimination, P_newprop is best,
            %% so let's write it.
            react_to_env_C300(P_newprop, P_latest, S)
    end.

react_to_env_C110(P_latest, #ch_mgr{myflu=MyFLU} = S) ->
    ?REACT(c110),
    %% TOOD: Should we carry along any extra info that that would be useful
    %%       in the dbg2 list?
    Extra_todo = [],
    RunEnv = S#ch_mgr.runenv,
    Islands = proplists:get_value(network_islands, RunEnv),
    P_latest2 = update_projection_dbg2(
                  P_latest,
                  [%% {network_islands, Islands},
                   %% {hooray, {v2, date(), time()}}
                   Islands--Islands
                   |Extra_todo]),
    Epoch = P_latest2#projection.epoch_number,
    ok = machi_flu0:proj_write(MyFLU, Epoch, private, P_latest2),
    case proplists:get_value(private_write_verbose, S#ch_mgr.opts) of
        true ->
            {_,_,C} = os:timestamp(),
            MSec = trunc(C / 1000),
            {HH,MM,SS} = time(),
            io:format(user, "\n~2..0w:~2..0w:~2..0w.~3..0w ~p uses: ~w\n",
                      [HH,MM,SS,MSec, S#ch_mgr.name,
                       make_projection_summary(P_latest2)]);
        _ ->
            ok
    end,
    react_to_env_C120(P_latest, S).

react_to_env_C120(P_latest, #ch_mgr{proj_history=H} = S) ->
    ?REACT(c120),
    H2 = queue:in(P_latest, H),
    H3 = case queue:len(H2) of
             X when X > length(P_latest#projection.all_members) * 2 ->
                 {_V, Hxx} = queue:out(H2),
                 Hxx;
             _ ->
                 H2
         end,
    {{now_using, P_latest#projection.epoch_number},
     S#ch_mgr{proj=P_latest, proj_history=H3, proj_proposed=none}}.

react_to_env_C200(Retries, P_latest, S) ->
    ?REACT(c200),
    try
        yo:tell_author_yo(P_latest#projection.author_server)
    catch _Type:_Err ->
            %% io:format(user, "TODO: tell_author_yo is broken: ~p ~p\n",
            %%           [_Type, _Err]),
            ok
    end,
    react_to_env_C210(Retries, S).

react_to_env_C210(Retries, #ch_mgr{name=MyName, proj=Proj} = S) ->
    ?REACT(c210),
    sleep_ranked_order(10, 100, MyName, Proj#projection.all_members),
    react_to_env_C220(Retries, S).

react_to_env_C220(Retries, S) ->
    ?REACT(c220),
    react_to_env_A20(Retries + 1, S).

react_to_env_C300(#projection{epoch_number=Epoch_newprop}=P_newprop,
                  #projection{epoch_number=Epoch_latest}=_P_latest, S) ->
    ?REACT(c300),
    NewEpoch = erlang:max(Epoch_newprop, Epoch_latest) + 1,
    P_newprop2 = P_newprop#projection{epoch_number=NewEpoch},
    react_to_env_C310(update_projection_checksum(P_newprop2), S).

react_to_env_C310(P_newprop, S) ->
    ?REACT(c310),
    Epoch = P_newprop#projection.epoch_number,
    {_Res, S2} = cl_write_public_proj_skip_local_error(Epoch, P_newprop, S),
    ?REACT({c310,make_projection_summary(P_newprop)}),
    ?REACT({c310,_Res}),
    
    react_to_env_A10(S2).

calculate_flaps(P_newprop, FlapLimit,
                #ch_mgr{name=MyName, proj_history=H, flaps=Flaps, runenv=RunEnv0} = S) ->
    RunEnv1 = replace(RunEnv0, [{flapping_i, []}]),
    HistoryPs = queue:to_list(H),
    Ps = HistoryPs ++ [P_newprop],
    UPI_Repairing_combos =
        lists:usort([{P#projection.upi, P#projection.repairing} || P <- Ps]),
    Down_combos = lists:usort([P#projection.down || P <- Ps]),

    {_WhateverUnanimous, BestP, Props, _S} =
        cl_read_latest_projection(private, S),
    NotBestPs = proplists:get_value(not_unanimous_answers, Props),
    DownUnion = lists:usort(
                  lists:flatten(
                    [P#projection.down ||
                        P <- [BestP|NotBestPs]])),
    HosedTransUnion = lists:usort(
                        lists:flatten(
                          [X || P <- [BestP|NotBestPs],
                                FIs <- [proplists:get_value(flapping_i,
                                                            P#projection.dbg,
                                                            [])],
                                X <- proplists:get_value(all_hosed, FIs, [])])),
    TransFlapCounts0 = lists:usort(
                         lists:flatten(
                           [X || P <- [BestP|NotBestPs],
                                 FIs <- [proplists:get_value(flapping_i,
                                                             P#projection.dbg,
                                                             [])],
                                 X <- proplists:get_value(all_flap_counts, FIs, [])])),

    _Unanimous = proplists:get_value(unanimous_flus, Props),
    _NotUnanimous = proplists:get_value(not_unanimous_flus, Props),
    BadFLUs = proplists:get_value(bad_answer_flus, Props),

    case {queue:len(H), length(UPI_Repairing_combos), length(Down_combos)} of
        {N, _, _} when N < length(P_newprop#projection.all_members) ->
            NewFlaps = 0,
            AllFlapCounts = [],
            AllHosed = [];
        {_, 1=_URs, 1=_Ds} ->
            NewFlaps = Flaps + 1,
            FlapFLUs = lists:usort([FLU || {FLU, _FlapCount} <- TransFlapCounts0]),
            %% We're interested in the *largest* flap count from each
            %% remote participant.
            RemoteTransFlapCounts = [{FLU, my_find_max(FLU, TransFlapCounts0)} ||
                                        FLU <- FlapFLUs,
                                        FLU /= MyName],
            AllFlapCounts = [{MyName, NewFlaps}|RemoteTransFlapCounts],
            AllHosed = lists:usort(DownUnion ++ HosedTransUnion ++ BadFLUs);
        {_N, _URs, _Ds} ->
            NewFlaps = 0,
            AllFlapCounts = [],
            AllHosed = []
    end,

    AllFlapCountsSettled = my_find_minmost(AllFlapCounts) >= FlapLimit,
    FlappingI = {flapping_i, [{flap_count,NewFlaps},
                              {all_hosed, AllHosed},
                              {all_flap_counts, AllFlapCounts},
                              {all_flap_counts_settled, AllFlapCountsSettled},
                              {bad,BadFLUs}]},
    Dbg2 = [FlappingI|P_newprop#projection.dbg],
    RunEnv2 = replace(RunEnv1, [FlappingI]),
    {S#ch_mgr{flaps=NewFlaps, runenv=RunEnv2}, update_projection_checksum(P_newprop#projection{dbg=Dbg2})}.

projection_transitions_are_sane(Ps, RelativeToServer) ->
    projection_transitions_are_sane(Ps, RelativeToServer, false).

-ifdef(TEST).
projection_transitions_are_sane_retrospective(Ps, RelativeToServer) ->
    projection_transitions_are_sane(Ps, RelativeToServer, true).
-endif. % TEST

projection_transitions_are_sane([], _RelativeToServer, _RetrospectiveP) ->
    true;
projection_transitions_are_sane([_], _RelativeToServer, _RetrospectiveP) ->
    true;
projection_transitions_are_sane([P1, P2|T], RelativeToServer, RetrospectiveP) ->
    case projection_transition_is_sane(P1, P2, RelativeToServer,
                                       RetrospectiveP) of
        true ->
            projection_transitions_are_sane([P2|T], RelativeToServer,
                                           RetrospectiveP);
        Else ->
            Else
    end.

projection_transition_is_sane(P1, P2, RelativeToServer) ->
    projection_transition_is_sane(P1, P2, RelativeToServer, false).

-ifdef(TEST).
projection_transition_is_sane_retrospective(P1, P2, RelativeToServer) ->
    projection_transition_is_sane(P1, P2, RelativeToServer, true).
-endif. % TEST

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
  RelativeToServer, RetrospectiveP) ->
 try
    %% General notes:
    %%
    %% I'm making no attempt to be "efficient" here.  All of these data
    %% structures are small, and they're not called zillions of times per
    %% second.
    %%
    %% The chain sequence/order checks at the bottom of this function aren't
    %% as easy-to-read as they ought to be.  However, I'm moderately confident
    %% that it isn't buggy.  TODO: refactor them for clarity.

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
            [lists:member(X, Repairing_list1) % X added after repair done
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
            UPI_2_concat = (UPI_2_suffix_from_UPI1 ++ UPI_2_suffix_from_Repairing1),
            if UPI_2_suffix == UPI_2_concat ->
                    ok;
               true ->
                    if RetrospectiveP ->
                            %% We are in retrospective mode.  But there are
                            %% some transitions that are difficult to find
                            %% when standing outside of all of the FLUs and
                            %% examining their behavior.  (In contrast to
                            %% this same function being called "in the path"
                            %% of a projection transition by a particular FLU
                            %% which knows exactly its prior projection and
                            %% exactly what it intends to do.)  Perhaps this
                            %% exception clause here can go away with
                            %% better/more clever retrospection analysis?
                            %%
                            %% Here's a case that PULSE found:
                            %% FLU B:
                            %%   E=257: UPI=[c,a], REPAIRING=[b]
                            %%   E=284: UPI=[c,a], REPAIRING=[b]
                            %% FLU a:
                            %%   E=251: UPI=[c], REPAIRING=[a,b]
                            %%   E=284: UPI=[c,a], REPAIRING=[b]
                            %% FLU c:
                            %%   E=282: UPI=[c], REPAIRING=[a,b]
                            %%   E=284: UPI=[c,a], REPAIRING=[b]
                            %%
                            %% From the perspective of each individual FLU,
                            %% the unanimous transition at epoch #284 is
                            %% good.  The repair that is done by FLU c -> a
                            %% is likewise good.
                            %%
                            %% From a retrospective point of view (and the
                            %% current implementation), there's a bad-looking
                            %% transition from epoch #269 to #284.  This is
                            %% from the point of view of the last two
                            %% unanimous private projection store epochs:
                            %%
                            %%   E=269: UPI=[c], REPAIRING=[], DOWN=[a,b]
                            %%   E=284: UPI=[c,a], REPAIRING=[b]
                            %%
                            %% The retrospective view by
                            %% machi_chain_manager1_pulse.erl just can't
                            %% reason correctly about this situation.  We
                            %% will instead rely on the non-introspective
                            %% sanity checking that each FLU does before it
                            %% writes to its private projection store and
                            %% then adopts that projection (and unwedges
                            %% itself, etc etc).

                            %% io:format(user, "QQQ: RetrospectiveP ~p\n", [RetrospectiveP]),
                            %% io:format(user, "QQQ: UPI_2_suffix ~p\n", [UPI_2_suffix]),
                            %% io:format(user, "QQQ: UPI_2_suffix_from_UPI1 ~p\n", [UPI_2_suffix_from_UPI1]),
                            %% io:format(user, "QQQ: UPI_2_suffix_from_Repairing1 ~p\n", [UPI_2_suffix_from_Repairing1]),
                            io:format(user, "|~p,~p TODO revisit|",
                                      [?MODULE, ?LINE]),
                            ok;
                        not RetrospectiveP ->
                            exit({upi_2_suffix_error})
                    end
            end
    end,
    true
 catch
     _Type:_Err ->
         S1 = make_projection_summary(P1),
         S2 = make_projection_summary(P2),
         Trace = erlang:get_stacktrace(),
         %% TODO: this history goop is useful sometimes for debugging but
         %% not for any "real" use.  Get rid of it, for the long term.
         H = (catch [{FLUName, Type, P#projection.epoch_number, make_projection_summary(P)} ||
                 FLUName <- P1#projection.all_members,
                 Type <- [public,private],
                 P <- machi_flu0:proj_get_all(FLUName, Type)]),
         {err, _Type, _Err, from, S1, to, S2, relative_to, RelativeToServer,
          history, (catch lists:sort(H)),
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

sleep_ranked_order(MinSleep, MaxSleep, FLU, FLU_list) ->
    Front = lists:takewhile(fun(X) -> X /=FLU end, FLU_list),
    Index = length(Front) + 1,
    NumNodes = length(FLU_list),
    SleepIndex = NumNodes - Index,
    SleepChunk = MaxSleep div NumNodes,
    SleepTime = MinSleep + (SleepChunk * SleepIndex),
    timer:sleep(SleepTime),
    SleepTime.

my_find_max(_FLU, []) ->
    0;
my_find_max(FLU, TransFlapCounts0) ->
    lists:max([FlapCount || {F, FlapCount} <- TransFlapCounts0,
                            F == FLU]).

my_find_minmost([]) ->
    0;
my_find_minmost(TransFlapCounts0) ->
    lists:min([FlapCount || {_F, FlapCount} <- TransFlapCounts0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

perhaps_call_t(S, Partitions, FLU, DoIt) ->
    try
        perhaps_call(S, Partitions, FLU, DoIt)
    catch
        exit:timeout ->
            t_timeout
    end.

perhaps_call(#ch_mgr{name=MyName, myflu=MyFLU}, Partitions, FLU, DoIt) ->
    RemoteFLU_p = FLU /= MyFLU,
    case RemoteFLU_p andalso lists:member({MyName, FLU}, Partitions) of
        false ->
            Res = DoIt(),
            case RemoteFLU_p andalso lists:member({FLU, MyName}, Partitions) of
                false ->
                    Res;
                _ ->
                    (catch put(react, [{timeout2,me,MyFLU,to,FLU,RemoteFLU_p,Partitions}|get(react)])),
                    exit(timeout)
            end;
        _ ->
            (catch put(react, [{timeout1,me,MyFLU,to,FLU,RemoteFLU_p,Partitions}|get(react)])),
            exit(timeout)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

