%% -------------------------------------------------------------------
%%
%% Machi: a small village of replicated files
%%
%% Copyright (c) 2014-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The Machi chain manager, Guardian of all things related to
%% Chain Replication state, status, and data replica safety.
%%
%% The Chain Manager is responsible for managing the state of Machi's
%% "Chain Replication" state.  This role is roughly analogous to the
%% "Riak Core" application inside of Riak, which takes care of
%% coordinating replica placement and replica repair.
%% 
%% For each primitive data server in the cluster, a Machi FLU, there
%% is a Chain Manager process that manages its FLU's role within the
%% Machi cluster's Chain Replication scheme.  Each Chain Manager
%% process executes locally and independently to manage the
%% distributed state of a single Machi Chain Replication chain.
%%
%% Machi's Chain Manager process performs similar tasks as Riak Core's
%% claimant.  However, Machi has several active Chain Manager
%% processes, one per FLU server, instead of a single active process
%% like Core's claimant.  Each Chain Manager process acts
%% independently; each is constrained so that it will reach consensus
%% via independent computation &amp; action.

-module(machi_chain_manager1).

%% TODO: I am going to sever the connection between the flowchart and the
%%       code.  That diagram is really valuable, but it also takes a long time
%%       to make any kind of edit; the process is too slow.  This is a todo
%%       item a reminder that the flowchart is important documentation and
%%       must be brought back into sync with the code soon.

-behaviour(gen_server).

-include("machi_projection.hrl").
-include("machi_chain_manager.hrl").

-define(D(X), io:format(user, "~s ~p\n", [??X, X])).
-define(Dw(X), io:format(user, "~s ~w\n", [??X, X])).

-define(FLU_C,  machi_flu1_client).
-define(FLU_PC, machi_proxy_flu1_client).
-define(TO, (2*1000)).                          % default timeout

%% Keep a history of our flowchart execution in the process dictionary.
-define(REACT(T), put(react, [T|get(react)])).

%% API
-export([start_link/2, start_link/3, stop/1, ping/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([projection_transitions_are_sane/2]).

-ifdef(TEST).

-export([test_calc_projection/2,
         test_write_public_projection/2,
         test_read_latest_public_projection/2,
         test_set_active/2,
         test_react_to_env/1,
         get_all_hosed/1]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif. %TEST

start_link(MyName, MembersDict) ->
    start_link(MyName, MembersDict, []).

start_link(MyName, MembersDict, MgrOpts) ->
    gen_server:start_link(?MODULE, {MyName, MembersDict, MgrOpts}, []).

stop(Pid) ->
    gen_server:call(Pid, {stop}, infinity).

ping(Pid) ->
    gen_server:call(Pid, {ping}, infinity).

-ifdef(TEST).

%% Test/debugging code only.

test_write_public_projection(Pid, Proj) ->
    gen_server:call(Pid, {test_write_public_projection, Proj}, infinity).

%% Calculate a projection and return it to us.
%% If KeepRunenvP is true, the server will retain its change in its
%% runtime environment, e.g., changes in simulated network partitions.
test_calc_projection(Pid, KeepRunenvP) ->
    gen_server:call(Pid, {test_calc_projection, KeepRunenvP}, infinity).

test_read_latest_public_projection(Pid, ReadRepairP) ->
    gen_server:call(Pid, {test_read_latest_public_projection, ReadRepairP},
                    infinity).

test_set_active(Pid, Boolean) when Boolean == true; Boolean == false ->
    gen_server:call(Pid, {test_set_active, Boolean}, infinity).

test_react_to_env(Pid) ->
    gen_server:call(Pid, {test_react_to_env}, infinity).

-endif. % TEST

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({MyName, MembersDict, MgrOpts}) ->
    All_list = [P#p_srvr.name || {_, P} <- orddict:to_list(MembersDict)],
    Opt = fun(Key, Default) -> proplists:get_value(Key, MgrOpts, Default) end,
    RunEnv = [{seed, Opt(seed, now())},
              {network_partitions, Opt(network_partitions, [])},
              {network_islands, Opt(network_islands, [])},
              {flapping_i, Opt(flapping, [])},
              {up_nodes, Opt(up_nodes, not_init_yet)}],
    ActiveP = Opt(active_mode, true),
    Down_list = All_list -- [MyName],
    UPI_list = [MyName],
    NoneProj = machi_projection:new(MyName, MembersDict,
                                    Down_list, UPI_list, [], []),
    Proxies = orddict:fold(
                fun(K, P, Acc) ->
                        {ok, Pid} = ?FLU_PC:start_link(P),
                        [{K, Pid}|Acc]
                end, [], MembersDict),
    S = #ch_mgr{name=MyName,
                %% TODO 2015-03-04: revisit, should this constant be bigger?
                %% Yes, this should be bigger, but it's a hack.  There is
                %% no guarantee that all parties will advance to a minimum
                %% flap awareness in the amount of time that this mgr will.
                flap_limit=length(All_list) + 50,
                proj=NoneProj,
                timer='undefined',
                proj_history=queue:new(),
                runenv=RunEnv,
                opts=MgrOpts,
                members_dict=MembersDict,
                proxies_dict=orddict:from_list(Proxies)},
    S2 = if ActiveP == false ->
                 S;
            ActiveP == true ->
                 set_active_timer(S)
         end,
    {ok, S2}.

handle_call({ping}, _From, S) ->
    {reply, pong, S};
handle_call({stop}, _From, S) ->
    {stop, normal, ok, S};
handle_call({test_calc_projection, KeepRunenvP}, _From,
            #ch_mgr{name=MyName}=S) ->
    RelativeToServer = MyName,
    {P, S2} = calc_projection(S, RelativeToServer),
    {reply, {ok, P}, if KeepRunenvP -> S2;
                        true        -> S
                     end};
handle_call({test_write_public_projection, Proj}, _From, S) ->
    {Res, S2} = do_cl_write_public_proj(Proj, S),
    {reply, Res, S2};
handle_call({test_read_latest_public_projection, ReadRepairP}, _From, S) ->
    {Perhaps, Val, ExtraInfo, S2} =
        do_cl_read_latest_public_projection(ReadRepairP, S),
    Res = {Perhaps, Val, ExtraInfo},
    {reply, Res, S2};
handle_call({test_set_active, Boolean}, _From, #ch_mgr{timer=TRef}=S) ->
    case {Boolean, TRef} of
        {true, undefined} ->
            S2 = set_active_timer(S),
            {reply, ok, S2};
        {false, _} ->
            (catch timer:cancel(TRef)),
            {reply, ok, S#ch_mgr{timer=undefined}};
        _ ->
            {reply, error, S}
    end;
handle_call({test_react_to_env}, _From, S) ->
    {TODOtodo, S2} =  do_react_to_env(S),
    {reply, TODOtodo, S2};
handle_call(_Call, _From, S) ->
    {reply, whaaaaaaaaaa, S}.

handle_cast(_Cast, S) ->
    ?D({cast_whaaaaaaaaaaa, _Cast}),
    {noreply, S}.

handle_info(Msg, S) ->
    exit({bummer, Msg}),
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

set_active_timer(#ch_mgr{name=MyName, members_dict=MembersDict}=S) ->
    FLU_list = [P#p_srvr.name || {_,P} <- orddict:to_list(MembersDict)],
    USec = calc_sleep_ranked_order(1000, 2000, MyName, FLU_list),
    {ok, TRef} = timer:send_interval(USec, yo_yo_yo),
    S#ch_mgr{timer=TRef}.

do_cl_write_public_proj(Proj, S) ->
    #projection_v1{epoch_number=Epoch} = Proj,
    cl_write_public_proj(Epoch, Proj, S).

cl_write_public_proj(Epoch, Proj, S) ->
    cl_write_public_proj(Epoch, Proj, false, S).

cl_write_public_proj_skip_local_error(Epoch, Proj, S) ->
    cl_write_public_proj(Epoch, Proj, true, S).

cl_write_public_proj(Epoch, Proj, SkipLocalWriteErrorP, S) ->
    %% Write to local public projection store first, and if it succeeds,
    %% then write to all remote public projection stores.
    cl_write_public_proj_local(Epoch, Proj, SkipLocalWriteErrorP, S).

cl_write_public_proj_local(Epoch, Proj, SkipLocalWriteErrorP,
                           #ch_mgr{name=MyName}=S) ->
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    Res0 = perhaps_call_t(
             S, Partitions, MyName,
             fun(Pid) -> ?FLU_PC:write_projection(Pid, public, Proj, ?TO) end),
    Continue = fun() ->
                  FLUs = Proj#projection_v1.all_members -- [MyName],
                  cl_write_public_proj_remote(FLUs, Partitions, Epoch, Proj, S)
               end,
    case Res0 of
        ok ->
            {XX, SS} = Continue(),
            {{local_write_result, ok, XX}, SS};
        Else when SkipLocalWriteErrorP ->
            {XX, SS} = Continue(),
            {{local_write_result, Else, XX}, SS};
        Else when Else == error_written; Else == timeout; Else == t_timeout ->
            {Else, S2}
    end.

cl_write_public_proj_remote(FLUs, Partitions, _Epoch, Proj, S) ->
    %% We're going to be very care-free about this write because we'll rely
    %% on the read side to do any read repair.
    DoIt = fun(Pid) -> ?FLU_PC:write_projection(Pid, public, Proj, ?TO) end,
    Rs = [{FLU, perhaps_call_t(S, Partitions, FLU, fun(Pid) -> DoIt(Pid) end)} ||
             FLU <- FLUs],
    {{remote_write_results, Rs}, S}.

do_cl_read_latest_public_projection(ReadRepairP,
                                    #ch_mgr{proj=Proj1} = S) ->
    _Epoch1 = Proj1#projection_v1.epoch_number,
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

read_latest_projection_call_only(ProjectionType, AllHosed,
                                      #ch_mgr{proj=CurrentProj}=S) ->
    #projection_v1{all_members=All_list} = CurrentProj,
    All_queried_list = All_list -- AllHosed,

    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    DoIt = fun(Pid) ->
                   case ?FLU_PC:read_latest_projection(Pid, ProjectionType, ?TO) of
                       {ok, P} -> P;
                       Else    -> Else
                   end
           end,
    Rs = [perhaps_call_t(S, Partitions, FLU, fun(Pid) -> DoIt(Pid) end) ||
             FLU <- All_queried_list],
    FLUsRs = lists:zip(All_queried_list, Rs),
    {All_queried_list, FLUsRs, S2}.

cl_read_latest_projection(ProjectionType, S) ->
    AllHosed = [],
    cl_read_latest_projection(ProjectionType, AllHosed, S).

cl_read_latest_projection(ProjectionType, AllHosed, S) ->
    {All_queried_list, FLUsRs, S2} =
        read_latest_projection_call_only(ProjectionType, AllHosed, S),

    rank_and_sort_projections_with_extra(All_queried_list, FLUsRs, S2).

rank_and_sort_projections_with_extra(All_queried_list, FLUsRs,
                                     #ch_mgr{proj=CurrentProj}=S) ->
    UnwrittenRs = [x || {_, error_unwritten} <- FLUsRs],
    Ps = [Proj || {_FLU, Proj} <- FLUsRs, is_record(Proj, projection_v1)],
    BadAnswerFLUs = [FLU || {FLU, Answer} <- FLUsRs,
                            not is_record(Answer, projection_v1)],

    if All_queried_list == []
       orelse
       length(UnwrittenRs) == length(FLUsRs) ->
            {error_unwritten, FLUsRs, [todo_fix_caller_perhaps], S};
       UnwrittenRs /= [] ->
            {needs_repair, FLUsRs, [flarfus], S};
       true ->
            [{_Rank, BestProj}|_] = rank_and_sort_projections(Ps, CurrentProj),
            NotBestPs = [Proj || Proj <- Ps, Proj /= BestProj],
            UnanimousTag = if NotBestPs == [] -> unanimous;
                              true            -> not_unanimous
                           end,
            Extra = [{all_members_replied, length(FLUsRs) == length(All_queried_list)}],
            Best_FLUs = [FLU || {FLU, Projx} <- FLUsRs, Projx == BestProj],
            TransAllHosed = lists:usort(
                              lists:flatten([get_all_hosed(P) || P <- Ps])),
            AllFlapCounts = merge_flap_counts([get_all_flap_counts(P) ||
                                                  P <- Ps]),
            Extra2 = [{all_queried_list, All_queried_list},
                      {flus_rs, FLUsRs},
                      {unanimous_flus,Best_FLUs},
                      {not_unanimous_flus, All_queried_list --
                                                 (Best_FLUs ++ BadAnswerFLUs)},
                      {bad_answer_flus, BadAnswerFLUs},
                      {not_unanimous_answers, NotBestPs},
                      {trans_all_hosed, TransAllHosed},
                      {trans_all_flap_counts, AllFlapCounts}|Extra],
            {UnanimousTag, BestProj, Extra2, S}
    end.

do_read_repair(FLUsRs, _Extra, #ch_mgr{proj=CurrentProj} = S) ->
    Unwrittens = [x || {_FLU, error_unwritten} <- FLUsRs],
    Ps = [Proj || {_FLU, Proj} <- FLUsRs, is_record(Proj, projection_v1)],
    if Unwrittens == [] orelse Ps == [] ->
            {nothing_to_do, S};
       true ->
            %% We have at least one unwritten and also at least one proj.
            %% Pick the best one, then spam it everywhere.

            [{_Rank, BestProj}|_] = rank_and_sort_projections(Ps, CurrentProj),
            Epoch = BestProj#projection_v1.epoch_number,

            %% We're doing repair, so use the flavor that will
            %% continue to all others even if there is an
            %% error_written on the local FLU.
            {_DontCare, _S2}=Res = cl_write_public_proj_skip_local_error(
                                                           Epoch, BestProj, S),
            Res
    end.

calc_projection(S, RelativeToServer) ->
    calc_projection(S, RelativeToServer, []).

calc_projection(#ch_mgr{proj=LastProj, runenv=RunEnv} = S,
                RelativeToServer, AllHosed) ->
    Dbg = [],
    OldThreshold = proplists:get_value(old_threshold, RunEnv),
    NoPartitionThreshold = proplists:get_value(no_partition_threshold, RunEnv),
    calc_projection(OldThreshold, NoPartitionThreshold, LastProj,
                    RelativeToServer, AllHosed, Dbg, S).

%% OldThreshold: Percent chance of using the old/previous network partition list
%% NoPartitionThreshold: If the network partition changes, what percent chance
%%                       that there are no partitions at all?

calc_projection(_OldThreshold, _NoPartitionThreshold, LastProj,
                RelativeToServer, AllHosed, Dbg,
                #ch_mgr{name=MyName, runenv=RunEnv1}=S) ->
    #projection_v1{epoch_number=OldEpochNum,
                   members_dict=MembersDict,
                   upi=OldUPI_list,
                   repairing=OldRepairing_list
                  } = LastProj,
    LastUp = lists:usort(OldUPI_list ++ OldRepairing_list),
    AllMembers = (S#ch_mgr.proj)#projection_v1.all_members,
    {Up0, Partitions, RunEnv2} = calc_up_nodes(MyName,
                                               AllMembers, RunEnv1),
    Up = Up0 -- AllHosed,

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
D_foo=[{repair_airquote_done, {we_agree, (S#ch_mgr.proj)#projection_v1.epoch_number}}],
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

    P = machi_projection:new(OldEpochNum + 1,
                             MyName, MembersDict, Down, NewUPI, NewRepairing,
                             D_foo ++
                                 Dbg ++ [{ps, Partitions},{nodes_up, Up}]),
    {P, S#ch_mgr{runenv=RunEnv3}}.

check_latest_private_projections(FLUs, MyProj, Partitions, S) ->
    FoldFun = fun(_FLU, false) ->
                      false;
                 (FLU, true) ->
                      F = fun(Pid) ->
                                  ?FLU_PC:read_latest_projection(Pid, private, ?TO)
                          end,
                      case perhaps_call_t(S, Partitions, FLU, F) of
                          {ok, RemotePrivateProj} ->
                              %% TODO: For use inside the simulator, this
                              %% function needs to check if RemotePrivateProj
                              %% contains a nested inner projection and, if
                              %% so, compare epoch# and upi & repairing lists.
                              %% If the nested inner proj is not checked here,
                              %% then a FLU in asymmetric partition flapping
                              %% case will appear in the simulator to be stuck
                              %% in repairing state.
                              if MyProj#projection_v1.epoch_number ==
                                 RemotePrivateProj#projection_v1.epoch_number
                                 andalso
                                 MyProj#projection_v1.epoch_csum ==
                                 RemotePrivateProj#projection_v1.epoch_csum ->
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
    AllMembers = Proj#projection_v1.all_members,
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
    proplists:compact(Items ++ PropList).

rank_and_sort_projections([], CurrentProj) ->
    rank_projections([CurrentProj], CurrentProj);
rank_and_sort_projections(Ps, CurrentProj) ->
    Epoch = lists:max([Proj#projection_v1.epoch_number || Proj <- Ps]),
    MaxPs = [Proj || Proj <- Ps,
                     Proj#projection_v1.epoch_number == Epoch],
    %% Sort with highest rank first (custom sort)
    lists:sort(fun({RankA,_}, {RankB,_}) -> RankA > RankB end,
               rank_projections(MaxPs, CurrentProj)).

%% Caller must ensure all Projs are of the same epoch number.
%% If the caller gives us projections with different epochs, we assume
%% that the caller is doing an OK thing.

rank_projections(Projs, CurrentProj) ->
    #projection_v1{all_members=All_list} = CurrentProj,
    MemberRank = orddict:from_list(
                   lists:zip(All_list, lists:seq(1, length(All_list)))),
    N = length(All_list),
    [{rank_projection(Proj, MemberRank, N), Proj} || Proj <- Projs].

rank_projection(#projection_v1{upi=[]}, _MemberRank, _N) ->
    -100;
rank_projection(#projection_v1{author_server=Author,
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
    UPI_Repairing_FLUs = lists:sort(P_latest#projection_v1.upi ++
                                    P_latest#projection_v1.repairing),
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
                ?REACT({a20,?LINE,[{upi_repairing,UPI_Repairing_FLUs},
                                   {unanimous,UnanimousFLUs}]}),
                false;
           UnanimousTag == not_unanimous ->
                ?REACT({a20,?LINE}),
                false;
           true ->
                exit({badbad, UnanimousTag})
        end,
    react_to_env_A30(Retries, P_latest, LatestUnanimousP, ReadExtra, S2).

react_to_env_A30(Retries, P_latest, LatestUnanimousP, _ReadExtra,
                 #ch_mgr{name=MyName, proj=P_current,
                         flap_limit=FlapLimit} = S) ->
    ?REACT(a30),
    RelativeToServer = MyName,
    {P_newprop1, S2} = calc_projection(S, RelativeToServer),
    ?REACT({a30, ?LINE, [{newprop1, machi_projection:make_summary(P_newprop1)}]}),

    %% Are we flapping yet?
    {P_newprop2, S3} = calculate_flaps(P_newprop1, P_current, FlapLimit, S2),

    %% Move the epoch number up ... originally done in C300.
    #projection_v1{epoch_number=Epoch_newprop2}=P_newprop2,
    #projection_v1{epoch_number=Epoch_latest}=P_latest,
    NewEpoch = erlang:max(Epoch_newprop2, Epoch_latest) + 1,
    P_newprop3 = P_newprop2#projection_v1{epoch_number=NewEpoch},
    ?REACT({a30, ?LINE, [{newprop3, machi_projection:make_summary(P_newprop3)}]}),

    {P_newprop10, S10} =
        case get_flap_count(P_newprop3) of
            %% TODO: refactor to eliminate cut-and-paste code in 'when'
            {_, P_newprop3_flap_count} when P_newprop3_flap_count >= FlapLimit ->
                AllHosed = get_all_hosed(S3),
                {P_i, S_i} = calc_projection(S3, MyName, AllHosed),
                P_inner = case lists:member(MyName, AllHosed) of
                              false ->
                                  P_i;
                              true ->
                                  P_i#projection_v1{upi=[MyName],
                                                 repairing=[],
                                                 down=P_i#projection_v1.all_members
                                                      -- [MyName]}
                          end,

                %% TODO FIXME A naive assignment here will cause epoch #
                %% instability of the inner projection.  We need a stable
                %% epoch number somehow.  ^_^
                %% P_inner2 = P_inner#projection_v1{epoch_number=P_newprop3#projection_v1.epoch_number},

                FinalInnerEpoch =
                    case proplists:get_value(inner_projection,
                                             P_current#projection_v1.dbg) of
                        undefined ->
                            AllFlapCounts_epk =
                                [Epk || {{Epk,_FlTime}, _FlCount} <-
                                            get_all_flap_counts(P_newprop3)],
                            case AllFlapCounts_epk of
                                [] ->
                                    P_newprop3#projection_v1.epoch_number;
                                [_|_] ->
                                    lists:max(AllFlapCounts_epk)
                            end;
                        P_oldinner ->
                            if P_oldinner#projection_v1.upi == P_inner#projection_v1.upi
                               andalso
                               P_oldinner#projection_v1.repairing == P_inner#projection_v1.repairing
                               andalso
                               P_oldinner#projection_v1.down == P_inner#projection_v1.down ->
                                    P_oldinner#projection_v1.epoch_number;
                               true ->
                                    P_oldinner#projection_v1.epoch_number + 1
                            end
                    end,

                P_inner2 = P_inner#projection_v1{epoch_number=FinalInnerEpoch},
                InnerInfo = [{inner_summary, machi_projection:make_summary(P_inner2)},
                             {inner_projection, P_inner2}],
                DbgX = replace(P_newprop3#projection_v1.dbg, InnerInfo),
                ?REACT({a30, ?LINE, [qqqwww|DbgX]}),
                {P_newprop3#projection_v1{dbg=DbgX}, S_i};
            _ ->
                {P_newprop3, S3}
        end,

    react_to_env_A40(Retries, P_newprop10, P_latest,
                     LatestUnanimousP, S10).

react_to_env_A40(Retries, P_newprop, P_latest, LatestUnanimousP,
                 #ch_mgr{name=MyName, proj=P_current}=S) ->
    ?REACT(a40),
    [{Rank_newprop, _}] = rank_projections([P_newprop], P_current),
    [{Rank_latest, _}] = rank_projections([P_latest], P_current),
    LatestAuthorDownP = lists:member(P_latest#projection_v1.author_server,
                                     P_newprop#projection_v1.down),

    if
        (P_current#projection_v1.epoch_number > 0
         andalso
         P_latest#projection_v1.epoch_number > P_current#projection_v1.epoch_number)
        orelse
        not LatestUnanimousP ->
            ?REACT({a40, ?LINE,
                    [{latest_epoch, P_latest#projection_v1.epoch_number},
                     {current_epoch, P_current#projection_v1.epoch_number},
                     {latest_unanimous_p, LatestUnanimousP}]}),

            %% 1st clause: someone else has written a newer projection
            %% 2nd clause: a network partition has healed, revealing a
            %%             differing opinion.
            react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                             Rank_newprop, Rank_latest, S);

        P_latest#projection_v1.epoch_number < P_current#projection_v1.epoch_number
        orelse
        P_latest /= P_current ->
            ?REACT({a40, ?LINE,
                    [{latest_epoch, P_latest#projection_v1.epoch_number},
                     {current_epoch, P_current#projection_v1.epoch_number},
                     {neq, P_latest /= P_current}]}),

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
            ?REACT({b10, ?LINE,
                    [{rank_latest, Rank_latest},
                     {rank_newprop, Rank_newprop},
                     {latest_author, P_latest#projection_v1.author_server}]}),

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
        P_latest#projection_v1.author_server == MyName
        andalso
        (P_newprop#projection_v1.upi /= P_latest#projection_v1.upi
         orelse
         P_newprop#projection_v1.repairing /= P_latest#projection_v1.repairing) ->
            ?REACT({a40, ?LINE,
                   [{latest_author, P_latest#projection_v1.author_server},
                   {newprop_upi, P_newprop#projection_v1.upi},
                   {latest_upi, P_latest#projection_v1.upi},
                   {newprop_repairing, P_newprop#projection_v1.repairing},
                   {latest_repairing, P_latest#projection_v1.repairing}]}),

            react_to_env_C300(P_newprop, P_latest, S);

        %% A40c (see flowchart)
        LatestAuthorDownP ->
            ?REACT({a40, ?LINE,
                   [{latest_author, P_latest#projection_v1.author_server},
                    {author_is_down_p, LatestAuthorDownP}]}),

            %% TODO: I believe that membership in the
            %% P_newprop#projection_v1.down is not sufficient for long
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
            ?REACT({a40, ?LINE, [true]}),

            react_to_env_A50(P_latest, S)
    end.

react_to_env_A50(P_latest, S) ->
    ?REACT(a50),

    HH = get(react),
    io:format(user, "HEE50s ~w ~w ~w\n", [S#ch_mgr.name, self(), lists:reverse([X || X <- HH, is_atom(X)])]),
    %% io:format(user, "HEE50 ~w ~w ~p\n", [S#ch_mgr.name, self(), lists:reverse(HH)]),

    ?REACT({a50, ?LINE, [{latest_epoch, P_latest#projection_v1.epoch_number}]}),
    {{no_change, P_latest#projection_v1.epoch_number}, S}.

react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                 Rank_newprop, Rank_latest,
                 #ch_mgr{name=MyName, flap_limit=FlapLimit}=S)->
    ?REACT(b10),

    {_P_newprop_flap_time, P_newprop_flap_count} = get_flap_count(P_newprop),
    LatestAllFlapCounts = get_all_flap_counts_counts(P_latest),
    P_latest_trans_flap_count = my_find_minmost(LatestAllFlapCounts),

    if
        LatestUnanimousP ->
            ?REACT({b10, ?LINE, [{latest_unanimous_p, LatestUnanimousP}]}),
            put(b10_hack, false),

            react_to_env_C100(P_newprop, P_latest, S);

        P_newprop_flap_count >= FlapLimit ->
            %% I am flapping ... what else do I do?
            ?REACT({b10, ?LINE, [i_am_flapping,
                                 {newprop_flap_count, P_newprop_flap_count},
                                 {latest_trans_flap_count, P_latest_trans_flap_count},
                                 {flap_limit, FlapLimit}]}),
            _B10Hack = get(b10_hack),
            %% if _B10Hack == false andalso P_newprop_flap_count - FlapLimit - 3 =< 0 -> io:format(user, "{FLAP: ~w flaps ~w}!\n", [S#ch_mgr.name, P_newprop_flap_count]), put(b10_hack, true); true -> ok end,
            io:format(user, "{FLAP: ~w flaps ~w}!\n", [S#ch_mgr.name, P_newprop_flap_count]),

            if
                %% So, if we noticed a flap count by some FLU X with a
                %% count below FlapLimit, then X crashes so that X's
                %% flap count remains below FlapLimit, then we could get
                %% stuck forever?  Hrm, except that 'crashes' ought to be
                %% detected by our own failure detector and get us out of
                %% this current flapping situation, right? TODO
                %%
                %% 2015-04-05: If we add 'orelse AllSettled' to this 'if'
                %% clause, then we can end up short-circuiting too
                %% early.  (Where AllSettled comes from the runenv's
                %% flapping_i prop.)  So, I believe that we need to
                %% rely on the failure detector to rescue us.
                %%
                %% TODO About the above ^^ I think that was based on buggy
                %%      calculation of AllSettled.  Recheck!
                %%
                %% TODO Yay, another magic constant below, added to
                %%      FlapLimit, that needs thorough examination and
                %%      hopefully elimination.  I'm adding it to try to
                %%      make it more likely that someone's private proj
                %%      will include all_flap_counts_settled,true 100%
                %%      of the time.  But I'm not sure how important that
                %%      really is.
                %%      That settled flag can lag behind after a change in
                %%      network conditions, so I'm not sure how big its
                %%      value is, if any.
% QQQ TODO
%                P_latest_trans_flap_count >= FlapLimit + 20 ->
%                    %% Everyone that's flapping together now has flap_count
%                    %% that's larger than the limit.  So it's safe and good
%                    %% to stop here, so we can break the cycle of flapping.
%                    ?REACT({b10, ?LINE, [flap_stop]}),
%                    react_to_env_A50(P_latest, S);

                true ->
                    %% It is our moral imperative to write so that the flap
                    %% cycle continues enough times so that everyone notices
                    %% and thus the earlier clause above fires.
                    ?REACT({b10, ?LINE, [flap_continue]}),
                    react_to_env_C300(P_newprop, P_latest, S)
            end;

        Retries > 2 ->
            ?REACT({b10, ?LINE, [{retries, Retries}]}),
            put(b10_hack, false),

            %% The author of P_latest is too slow or crashed.
            %% Let's try to write P_newprop and see what happens!
            react_to_env_C300(P_newprop, P_latest, S);

        Rank_latest >= Rank_newprop
        andalso
        P_latest#projection_v1.author_server /= MyName ->
            ?REACT({b10, ?LINE,
                    [{rank_latest, Rank_latest},
                     {rank_newprop, Rank_newprop},
                     {latest_author, P_latest#projection_v1.author_server}]}),
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
    I_am_UPI_in_newprop_p = lists:member(MyName, P_newprop#projection_v1.upi),
    I_am_Repairing_in_latest_p = lists:member(MyName,
                                              P_latest#projection_v1.repairing),
    ShortCircuit_p =
        P_latest#projection_v1.epoch_number > P_current#projection_v1.epoch_number
        andalso
        I_am_UPI_in_newprop_p
        andalso
        I_am_Repairing_in_latest_p,

    case {ShortCircuit_p, projection_transition_is_sane(P_current, P_latest,
                                                        MyName)} of
        _ when P_current#projection_v1.epoch_number =< 0 ->
            ?REACT({c100, ?LINE, [first_write]}),
            react_to_env_C110(P_latest, S);
        {true, _} ->
            %% Someone else believes that I am repairing.  We assume
            %% that nobody is being Byzantine, so we'll believe that I
            %% am/should be repairing.  We ignore our proposal and try
            %% to go with the latest.
            ?REACT({c100, ?LINE, [repairing_short_circuit]}),
            react_to_env_C110(P_latest, S);
        {_, true} ->
            ?REACT({c100, ?LINE, [sane]}),
            react_to_env_C110(P_latest, S);
        {_, _AnyOtherReturnValue} ->
            %% P_latest is not sane.
            %% By process of elimination, P_newprop is best,
            %% so let's write it.
            ?REACT({c100, ?LINE, [not_sane]}),
            react_to_env_C300(P_newprop, P_latest, S)
    end.

react_to_env_C110(P_latest, #ch_mgr{name=MyName} = S) ->
    ?REACT(c110),
    %% TOOD: Should we carry along any extra info that that would be useful
    %%       in the dbg2 list?
    Extra_todo = [],
    RunEnv = S#ch_mgr.runenv,
    Islands = proplists:get_value(network_islands, RunEnv),
    P_latest2 = machi_projection:update_dbg2(
                  P_latest,
                  [%% {network_islands, Islands},
                   %% {hooray, {v2, date(), time()}}
                   Islands--Islands
                   |Extra_todo]),

    MyNamePid = proxy_pid(MyName, S),
    ok = ?FLU_PC:write_projection(MyNamePid, private, P_latest2, ?TO),
    case proplists:get_value(private_write_verbose, S#ch_mgr.opts) of
        true ->
            {_,_,C} = os:timestamp(),
            MSec = trunc(C / 1000),
            {HH,MM,SS} = time(),
            io:format(user, "\n~2..0w:~2..0w:~2..0w.~3..0w ~p uses: ~w\n",
                      [HH,MM,SS,MSec, S#ch_mgr.name,
                       machi_projection:make_summary(P_latest2)]);
        _ ->
            ok
    end,
    react_to_env_C120(P_latest, S).

react_to_env_C120(P_latest, #ch_mgr{proj_history=H} = S) ->
    ?REACT(c120),
    H2 = queue:in(P_latest, H),
    H3 = case queue:len(H2) of
             %% TODO: revisit this constant?  Is this too long as a base?
             %% My hunch is that it's fine and that the flap_limit needs to
             %% be raised much higher (because it can increase several ticks
             %% without a newer public epoch proposed anywhere).
             X when X > length(P_latest#projection_v1.all_members) * 2 ->
                 {_V, Hxx} = queue:out(H2),
                 Hxx;
             _ ->
                 H2
         end,

    HH = get(react),
    io:format(user, "HEE120s ~w ~w ~w\n", [S#ch_mgr.name, self(), lists:reverse([X || X <- HH, is_atom(X)])]),
    %% io:format(user, "HEE120 ~w ~w ~p\n", [S#ch_mgr.name, self(), lists:reverse(HH)]),

    ?REACT({c120, [{latest, machi_projection:make_summary(P_latest)}]}),
    {{now_using, P_latest#projection_v1.epoch_number},
     S#ch_mgr{proj=P_latest, proj_history=H3}}.

react_to_env_C200(Retries, P_latest, S) ->
    ?REACT(c200),
    try
        %% TODO: This code works "well enough" without actually
        %% telling anybody anything.  Do we want to rip this out?
        %% Actually implement it?  None of the above?
        yo:tell_author_yo(P_latest#projection_v1.author_server)
    catch _Type:_Err ->
            %% io:format(user, "TODO: tell_author_yo is broken: ~p ~p\n",
            %%           [_Type, _Err]),
            ok
    end,
    react_to_env_C210(Retries, S).

react_to_env_C210(Retries, #ch_mgr{name=MyName, proj=Proj} = S) ->
    ?REACT(c210),
    sleep_ranked_order(10, 100, MyName, Proj#projection_v1.all_members),
    react_to_env_C220(Retries, S).

react_to_env_C220(Retries, S) ->
    ?REACT(c220),
    react_to_env_A20(Retries + 1, S).

react_to_env_C300(#projection_v1{epoch_number=_Epoch_newprop}=P_newprop,
                  #projection_v1{epoch_number=_Epoch_latest}=_P_latest, S) ->
    ?REACT(c300),

    %% This logic moved to A30.
    %% NewEpoch = erlang:max(Epoch_newprop, Epoch_latest) + 1,
    %% P_newprop2 = P_newprop#projection_v1{epoch_number=NewEpoch},
    %% react_to_env_C310(update_checksum(P_newprop2), S).

    react_to_env_C310(machi_projection:update_checksum(P_newprop), S).

react_to_env_C310(P_newprop, S) ->
    ?REACT(c310),
    Epoch = P_newprop#projection_v1.epoch_number,
    {WriteRes, S2} = cl_write_public_proj_skip_local_error(Epoch, P_newprop, S),
    ?REACT({c310, ?LINE,
            [{newprop, machi_projection:make_summary(P_newprop)},
            {write_result, WriteRes}]}),
    react_to_env_A10(S2).

calculate_flaps(P_newprop, _P_current, FlapLimit,
                #ch_mgr{name=MyName, proj_history=H, flap_start=FlapStart,
                        flaps=Flaps, runenv=RunEnv0} = S) ->
    RunEnv1 = replace(RunEnv0, [{flapping_i, []}]),
    HistoryPs = queue:to_list(H),
    Ps = HistoryPs ++ [P_newprop],
    UniqueProposalSummaries = lists:usort([{P#projection_v1.upi,
                                            P#projection_v1.repairing,
                                            P#projection_v1.down} || P <- Ps]),

    {_WhateverUnanimous, BestP, Props, _S} =
        cl_read_latest_projection(private, S),
    NotBestPs = proplists:get_value(not_unanimous_answers, Props),
    DownUnion = lists:usort(
                  lists:flatten(
                    [P#projection_v1.down ||
                        P <- [BestP|NotBestPs]])),
    HosedTransUnion = proplists:get_value(trans_all_hosed, Props),
    TransFlapCounts0 = proplists:get_value(trans_all_flap_counts, Props),

    _Unanimous = proplists:get_value(unanimous_flus, Props),
    _NotUnanimous = proplists:get_value(not_unanimous_flus, Props),
    %% NOTE: bad_answer_flus are probably due to timeout or some other network
    %%       glitch, i.e., anything other than {ok, P::projection()}
    %%       response from machi_flu0:proj_read_latest().
    BadFLUs = proplists:get_value(bad_answer_flus, Props),

    RemoteTransFlapCounts1 = lists:keydelete(MyName, 1, TransFlapCounts0),
    RemoteTransFlapCounts =
        [X || {_FLU, {{_FlEpk,FlTime}, _FlapCount}}=X <- RemoteTransFlapCounts1,
              FlTime /= ?NOT_FLAPPING],
    TempNewFlaps = Flaps + 1,
    TempAllFlapCounts = lists:sort([{MyName, {FlapStart, TempNewFlaps}}|
                                    RemoteTransFlapCounts]),
    %% Sanity check.
    true = lists:all(fun({_,{_,_}}) -> true;
                        (_)         -> false end, TempAllFlapCounts),

    %% H is the bounded history of all of this manager's private
    %% projection store writes.  If we've proposed the *same*
    %% {UPI+Repairing, Down} combination for the entire length of our
    %% bounded size of H, then we're flapping.
    %%
    %% If we're flapping, then we use our own flap counter and that of
    %% all of our peer managers to see if we've all got flap counters
    %% that exceed the flap_limit.  If that global condition appears
    %% true, then we "blow the circuit breaker" by stopping our
    %% participation in the flapping store (via the shortcut to A50).
    %%
    %% We reset our flap counter on any of several conditions:
    %%
    %% 1. If our bounded history H contains more than one proposal,
    %%    then by definition we are not flapping.
    %% 2. If a remote manager is flapping and has re-started a new
    %%    flapping episode.
    %% 3. If one of the remote managers that we saw earlier has
    %%    stopped flapping.

    ?REACT({calculate_flaps, queue:len(H), UniqueProposalSummaries}),
    case {queue:len(H), UniqueProposalSummaries} of
        {N, [_]} when N >= length(P_newprop#projection_v1.all_members) ->
            NewFlaps = TempNewFlaps,
            if element(2,FlapStart) == ?NOT_FLAPPING ->
                    NewFlapStart = {{epk,P_newprop#projection_v1.epoch_number},now()};
               true ->
                    NewFlapStart = FlapStart
            end,

            %% Wow, this behavior is almost spooky.
            %%
            %% For an example partition map [{c,a}], on the very first
            %% time this 'if' clause is hit by FLU b, AllHosed=[a,c].
            %% How the heck does B know that??
            %%
            %% If I use:
            %% DownUnionQQQ = [{P#projection_v1.epoch_number, P#projection_v1.author_server, P#projection_v1.down} || P <- [BestP|NotBestPs]],
            %% AllHosed = [x_1] ++ DownUnion ++ [x_2] ++ HosedTransUnion ++ [x_3] ++ BadFLUs ++ [{downunionqqq, DownUnionQQQ}];
            %%
            %% ... then b sees this when proposing epoch 451:
            %%
            %% {all_hosed,
            %%  [x_1,a,c,x_2,x_3,
            %%   {downunionqqq,
            %%    [{450,a,[c]},{449,b,[]},{448,c,[a]},{441,d,[]}]}]},
            %%
            %% So b's working on epoch 451 at the same time that d's latest
            %% public projection is only epoch 441.  But there's enough
            %% lag so that b can "see" that a's bad=[c] (due to t_timeout!)
            %% and c's bad=[a].  So voila, b magically knows about both
            %% problem FLUs.  Weird/cool.

            AllFlapCounts = TempAllFlapCounts,
            AllHosed = lists:usort(DownUnion ++ HosedTransUnion ++ BadFLUs);
        {_N, _} ->
            NewFlaps = 0,
            NewFlapStart = {{epk,-1},?NOT_FLAPPING},
            AllFlapCounts = [],
            AllHosed = []
    end,

    %% If there's at least one count in AllFlapCounts that isn't my
    %% flap count, and if it's over the flap limit, then consider them
    %% settled.
    AllFlapCountsSettled = lists:keydelete(MyName, 1, AllFlapCounts) /= []
                           andalso
                           my_find_minmost(AllFlapCounts) >= FlapLimit,
    FlappingI = {flapping_i, [{flap_count, {NewFlapStart, NewFlaps}},
                              {all_hosed, AllHosed},
                              {all_flap_counts, lists:sort(AllFlapCounts)},
                              {all_flap_counts_settled, AllFlapCountsSettled},
                              {bad,BadFLUs},
                              {da_downu, DownUnion}, % debugging aid
                              {da_hosedtu, HosedTransUnion}, % debugging aid
                              {da_downreports, [{P#projection_v1.epoch_number, P#projection_v1.author_server, P#projection_v1.down} || P <- [BestP|NotBestPs]]} % debugging aid
                             ]},
    Dbg2 = [FlappingI|P_newprop#projection_v1.dbg],
    %% SLF TODO: 2015-03-04: I'm growing increasingly suspicious of
    %% the 'runenv' variable that's threaded through all this code.
    %% It isn't doing what I'd originally intended.  And I think that
    %% the flapping information that we've just constructed here is
    %% going to get lost, and that's a shame.  Fix it.
    RunEnv2 = replace(RunEnv1, [FlappingI]),
    %% NOTE: If we'd increment of flaps here, that doesn't mean that
    %%       someone's public proj store has been updated.  For example,
    %%       if we loop through states C2xx a few times, we would incr
    %%       flaps each time ... but the C2xx path doesn't write a new
    %%       proposal to everyone's public proj stores, and there's no
    %%       guarantee that anyone else as written a new public proj either.
    {machi_projection:update_checksum(P_newprop#projection_v1{dbg=Dbg2}),
     S#ch_mgr{flaps=NewFlaps, flap_start=NewFlapStart, runenv=RunEnv2}}.

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
  #projection_v1{epoch_number=Epoch1,
              epoch_csum=CSum1,
              creation_time=CreationTime1,
              author_server=AuthorServer1,
              all_members=All_list1,
              down=Down_list1,
              upi=UPI_list1,
              repairing=Repairing_list1,
              dbg=Dbg1} = P1,
  #projection_v1{epoch_number=Epoch2,
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
         S1 = machi_projection:make_summary(P1),
         S2 = machi_projection:make_summary(P2),
         Trace = erlang:get_stacktrace(),
         %% %% TODO: this history goop is useful sometimes for debugging but
         %% %% not for any "real" use.  Get rid of it, for the long term.
         %% H = (catch [{FLUName, Type, P#projection_v1.epoch_number, machi_projection:make_summary(P)} ||
         %%         FLUName <- P1#projection_v1.all_members,
         %%         Type <- [public,private],
         %%         P <- ?FLU_PC:proj_get_all(orddict:fetch(FLUName, What?), Type)]),
         {err, _Type, _Err, from, S1, to, S2, relative_to, RelativeToServer,
          history, (catch lists:sort([no_history])),
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
    USec = calc_sleep_ranked_order(MinSleep, MaxSleep, FLU, FLU_list),
    timer:sleep(USec),
    USec.

calc_sleep_ranked_order(MinSleep, MaxSleep, FLU, FLU_list) ->
    Front = lists:takewhile(fun(X) -> X /=FLU end, FLU_list),
    Index = length(Front) + 1,
    NumNodes = length(FLU_list),
    SleepIndex = NumNodes - Index,
    SleepChunk = MaxSleep div NumNodes,
    MinSleep + (SleepChunk * SleepIndex).

my_find_minmost([]) ->
    0;
my_find_minmost([{_,_}|_] = TransFlapCounts0) ->
    lists:min([FlapCount || {_T, {_FlTime, FlapCount}} <- TransFlapCounts0]);
my_find_minmost(TransFlapCounts0) ->
    lists:min(TransFlapCounts0).

get_raw_flapping_i(#projection_v1{dbg=Dbg}) ->
    proplists:get_value(flapping_i, Dbg, []).

get_flap_count(P) ->
    proplists:get_value(flap_count, get_raw_flapping_i(P), 0).

get_all_flap_counts(P) ->
    proplists:get_value(all_flap_counts, get_raw_flapping_i(P), []).

get_all_flap_counts_counts(P) ->
    case get_all_flap_counts(P) of
        [] ->
            [];
        [{_,{_,_}}|_] = Cs ->
            [Count || {_FLU, {_Time, Count}} <- Cs]
    end.

get_all_hosed(P) when is_record(P, projection_v1)->
    proplists:get_value(all_hosed, get_raw_flapping_i(P), []);
get_all_hosed(S) when is_record(S, ch_mgr) ->
    proplists:get_value(all_hosed,
                        proplists:get_value(flapping_i, S#ch_mgr.runenv, []),
                        []).

merge_flap_counts(FlapCounts) ->
    merge_flap_counts(FlapCounts, orddict:new()).

merge_flap_counts([], D) ->
    orddict:to_list(D);
merge_flap_counts([FlapCount|Rest], D1) ->
    %% We know that FlapCount is list({Actor, {{_epk,FlapStartTime},NumFlaps}}).
    D2 = orddict:from_list(FlapCount),
     D2 = orddict:from_list(FlapCount),
    %% If the FlapStartTimes are identical, then pick the bigger flap count.
    %% If the FlapStartTimes differ, then pick the larger start time tuple.
    D3 = orddict:merge(fun(_Key, {{_,T1}, NF1}= V1, {{_,T2}, NF2}=V2)
                             when T1 == T2 ->
                               if NF1 > NF2 ->
                                       V1;
                                  true ->
                                       V2
                               end;
                          (_Key, {{_,T1},_NF1}= V1, {{_,T2},_NF2}=V2) ->
                               if T1 > T2 ->
                                       V1;
                                  true ->
                                       V2
                               end;
                          (_Key, V1, V2) ->
                               exit({bad_merge_2tuples,mod,?MODULE,line,?LINE,
                                     _Key, V1, V2})
                       end, D1, D2),
    merge_flap_counts(Rest, D3).

proxy_pid(Name, #ch_mgr{proxies_dict=ProxiesDict}) ->
    orddict:fetch(Name, ProxiesDict).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

perhaps_call_t(S, Partitions, FLU, DoIt) ->
    try
        perhaps_call(S, Partitions, FLU, DoIt)
    catch
        exit:timeout ->
            t_timeout
    end.

perhaps_call(#ch_mgr{name=MyName}=S, Partitions, FLU, DoIt) ->
    ProxyPid = proxy_pid(FLU, S),
    RemoteFLU_p = FLU /= MyName,
    case RemoteFLU_p andalso lists:member({MyName, FLU}, Partitions) of
        false ->
            Res = DoIt(ProxyPid),
            case RemoteFLU_p andalso lists:member({FLU, MyName}, Partitions) of
                false ->
                    Res;
                _ ->
                    (catch put(react, [{timeout2,me,MyName,to,FLU,RemoteFLU_p,Partitions}|get(react)])),
                    exit(timeout)
            end;
        _ ->
            (catch put(react, [{timeout1,me,MyName,to,FLU,RemoteFLU_p,Partitions}|get(react)])),
            exit(timeout)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

