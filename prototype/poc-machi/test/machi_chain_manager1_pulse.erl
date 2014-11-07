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
-module(machi_chain_manager1_pulse).

%% The while module is ifdef:ed, rebar should set PULSE
-ifdef(PULSE).

-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include("machi.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{application, pulse_application}]}).
%% The following functions contains side_effects but are run outside
%% PULSE, i.e. PULSE needs to leave them alone
-compile({pulse_skip,[{prop_pulse_test_,0}]}).
-compile({pulse_no_side_effect,[{file,'_','_'}, {erlang, now, 0}]}).

%% Used for output within EUnit...
-define(QC_FMT(Fmt, Args),
        io:format(user, Fmt, Args)).

%% And to force EUnit to output QuickCheck output...
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> ?QC_FMT(Str, Args) end, P)).

-define(MGR, machi_chain_manager1).
-define(MGRTEST, machi_chain_manager1_test).

-record(state, {
          step=0,
          num_pids,
          pids,
          dump_state
         }).

initial_state() ->
    #state{}.

gen_num_pids() ->
    choose(2, 5).

gen_seed() ->
    noshrink({choose(1, 10000), choose(1, 10000), choose(1, 10000)}).

gen_old_threshold() ->
    noshrink(choose(1, 100)).

gen_no_partition_threshold() ->
    noshrink(choose(1, 100)).

command(#state{step=0}) ->
    {call, ?MODULE, setup, [gen_num_pids(), gen_seed()]};
command(S) ->
    frequency([
               { 1, {call, ?MODULE, change_partitions,
                     [gen_old_threshold(), gen_no_partition_threshold()]}},
               {50, {call, ?MODULE, do_ticks,
                     [choose(5, 100), S#state.pids,
                      gen_old_threshold(), gen_no_partition_threshold()]}}
              ]).

precondition(_S, _) ->
    true.

next_state(#state{step=Step}=S, Res, Call) ->
    next_state2(S#state{step=Step + 1}, Res, Call).

next_state2(S, Res, {call, _, setup, [NumPids, _Seed]}) ->
    S#state{num_pids=NumPids, pids=Res};
next_state2(S, Res, {call, _, dump_state, _Args}) ->
    S#state{dump_state=Res};
next_state2(S, _Res, {call, _, _Func, _Args}) ->
    S.

postcondition(_S, {call, _, _Func, _Args}, _Res) ->
    true.

all_list() ->
    [a,b,c].
    %% [a,b,c,d,e].

setup(_Num, Seed) ->
    ?QC_FMT("\nsetup,", []),
    All_list = all_list(),
    _ = machi_partition_simulator:start_link(Seed, 0, 100),
    _Partitions = machi_partition_simulator:get(All_list),

    FLU_pids = [begin
                    {ok, FLUPid} = machi_flu0:start_link(Name),
                    _ = machi_flu0:get_epoch(FLUPid),
                    FLUPid
                end || Name <- All_list],
    Namez = lists:zip(All_list, FLU_pids),
    Mgr_pids = [begin
                    {ok, Mgr} = ?MGR:start_link(Name, All_list, FLU_pid),
                    Mgr
                end || {Name, FLU_pid} <- Namez],
    timer:sleep(1),
    {ok, P1} = ?MGR:test_calc_projection(hd(Mgr_pids), false),
    P1Epoch = P1#projection.epoch_number,
    [ok = machi_flu0:proj_write(FLU, P1Epoch, public, P1) || FLU <- FLU_pids],
    [?MGR:test_react_to_env(Mgr) || Mgr <- Mgr_pids],

    Res = {FLU_pids, Mgr_pids},
    put(manager_pids_hack, Res),
    Res.

change_partitions(OldThreshold, NoPartitionThreshold) ->
    machi_partition_simulator:reset_thresholds(OldThreshold,
                                               NoPartitionThreshold).

do_ticks(Num, PidsMaybe, OldThreshold, NoPartitionThreshold) ->
    io:format(user, "~p,~p,~p|", [Num, OldThreshold, NoPartitionThreshold]),
    {_FLU_pids, Mgr_pids} = case PidsMaybe of
                                undefined -> get(manager_pids_hack);
                                _         -> PidsMaybe
                            end,
    if is_integer(OldThreshold) ->
            machi_partition_simulator:reset_thresholds(OldThreshold,
                                                       NoPartitionThreshold);
       true ->
            ?QC_FMT("{e=~w},", [get_biggest_private_epoch_number()]),
            machi_partition_simulator:no_partitions()
    end,
    Res = exec_ticks(Num, Mgr_pids),
    if not is_integer(OldThreshold) ->
            ?QC_FMT("{e=~w},", [get_biggest_private_epoch_number()]);
       true ->
            ok
    end,
    Res.

get_biggest_private_epoch_number() ->
    lists:last(
      lists:usort(
        lists:flatten(
          [machi_flu0:proj_list_all(FLU, private) ||
              FLU <- all_list()]))).

dump_state() ->
  try
    ?QC_FMT("dump_state(", []),
    {FLU_pids, _Mgr_pids} = get(manager_pids_hack),
    Namez = zip(all_list(), FLU_pids),
    Report = ?MGRTEST:unanimous_report(Namez),
    %% ?QC_FMT("Report ~p\n", [Report]),

    Diag1 = [begin
                 Ps = machi_flu0:proj_get_all(FLU, Type),
                 [io_lib:format("~p ~p ~p: ~w\n", [FLUName, Type, P#projection.epoch_number, ?MGR:make_projection_summary(P)]) || P <- Ps]
             end || {FLUName, FLU} <- Namez,
                    Type <- [public] ],

    UniquePrivateEs =
        lists:usort(lists:flatten(
                      [machi_flu0:proj_list_all(FLU, private) ||
                          {_FLUName, FLU} <- Namez])),
    P_lists0 = [{FLUName, Type, machi_flu0:proj_get_all(FLUPid, Type)} ||
                   {FLUName, FLUPid} <- Namez, Type <- [public,private]],
    P_lists = [{FLUName, Type, P} || {FLUName, Type, Ps} <- P_lists0,
                                     P <- Ps],
    AllDict = lists:foldl(fun({FLU, Type, P}, D) ->
                                  K = {FLU, Type, P#projection.epoch_number},
                                  dict:store(K, P, D)
                          end, dict:new(), lists:flatten(P_lists)),
    DumbFinderBackward =
        fun(FLUName) ->
                fun(E, error_unwritten) ->
                        case dict:find({FLUName, private, E}, AllDict) of
                            {ok, T} -> T;
                            error   -> error_unwritten
                        end;
                        %% case machi_flu0:proj_read(FLU, E, private) of
                        %%     {ok, T} -> T;
                        %%     Else    -> Else
                        %% end;
                   (_E, Acc) ->
                        Acc
                end
        end,
    Diag2 = [[
                io_lib:format("~p private: ~w\n",
                              [FLUName,
                               ?MGR:make_projection_summary(
                                  lists:foldl(DumbFinderBackward(FLUName),
                                              error_unwritten,
                                              lists:seq(Epoch, 0, -1)))])
              || {FLUName, _FLU} <- Namez]
             || Epoch <- UniquePrivateEs],

    ?QC_FMT(")", []),
    {Report, lists:flatten([Diag1, Diag2])}
  catch XX:YY ->
        ?QC_FMT("OUCH: ~p ~p @ ~p\n", [XX, YY, erlang:get_stacktrace()])
  end.

prop_pulse() ->
    ?FORALL({Cmds0, Seed}, {non_empty(commands(?MODULE)), pulse:seed()},
    ?IMPLIES(1 < length(Cmds0) andalso length(Cmds0) < 5,
    begin
        ok = shutdown_hard(),
        %% PULSE can be really unfair, of course, including having exec_ticks
        %% run where all of FLU a does its ticks then FLU b.  Such a situation
        %% doesn't always allow unanimous private projection store values:
        %% FLU a might need one more tick to write its private projection, but
        %% it isn't given a chance at the end of the PULSE run.  So we cheat
        LastTriggerTicks = {set,{var,99999997},
                            {call, ?MODULE, do_ticks, [25, undefined, no, no]}},
        Cmds1 = lists:duplicate(2, LastTriggerTicks),
        %% Cmds1 = lists:duplicate(length(all_list())*2, LastTriggerTicks),
        Cmds = Cmds0 ++
               Cmds1 ++ [{set,{var,99999999},
                          {call, ?MODULE, dump_state, []}}],
        {_H2, S2, Res} = pulse:run(
                           fun() ->
                                   {_H, _S, _R} = run_commands(?MODULE, Cmds)
                           end, [{seed, Seed},
                                 {strategy, unfair}]),
        %% {FLU_pids, Mgr_pids} = S2#state.pids,
        %% [ok = machi_flu0:stop(FLU) || FLU <- FLU_pids],
        %% [ok = ?MGR:stop(Mgr) || Mgr <- Mgr_pids],
        ok = shutdown_hard(),

%% ?QC_FMT("Cmds ~p\n", [Cmds]),
%% ?QC_FMT("H2 ~p\n", [_H2]),
%% ?QC_FMT("S2 ~p\n", [S2]),
        {Report, Diag} = S2#state.dump_state,
%% ?QC_FMT("\nLast report = ~p\n", [lists:last(Report)]),

        %% Given the report, we flip it around so that we observe the
        %% sets of chain transitions relative to each FLU.
        R_Chains = [?MGRTEST:extract_chains_relative_to_flu(FLU, Report) ||
                       FLU <- all_list()],
        %% ?D(R_Chains),
        R_Projs = [{FLU, [?MGRTEST:chain_to_projection(
                             FLU, Epoch, UPI, Repairing, all_list()) ||
                             {Epoch, UPI, Repairing} <- E_Chains]} ||
                      {FLU, E_Chains} <- R_Chains],

        %% For each chain transition experienced by a particular FLU,
        %% confirm that each state transition is OK.
        Sane =
          [{FLU,_SaneRes} = {FLU,?MGR:projection_transitions_are_sane(Ps, FLU)} ||
              {FLU, Ps} <- R_Projs],
%% ?QC_FMT("Sane ~p\n", [Sane]),
        SaneP = lists:all(fun({_FLU, SaneRes}) -> SaneRes == true end, Sane),

        %% The final report item should say that all are agreed_membership.
        {_LastEpoch, {ok_disjoint, LastRepXs}} = lists:last(Report),
%% ?QC_FMT("LastEpoch=~p,", [_LastEpoch]),
%% ?QC_FMT("Report ~P\n", [Report, 5000]),
%% ?QC_FMT("Diag ~s\n", [Diag]),
        AgreedOrNot = lists:usort([element(1, X) || X <- LastRepXs]),
%% ?QC_FMT("LastRepXs ~p", [LastRepXs]),
        
        %% TODO: Check that we've converged to a single chain with no repairs.
        SingleChainNoRepair = case LastRepXs of
                                  [{agreed_membership,{_UPI,[]}}] ->
                                      true;
                                  _ ->
                                      LastRepXs
                              end,

        ?WHENFAIL(
        begin
            ?QC_FMT("Res = ~p\n", [Res]),
            ?QC_FMT("Diag = ~s\n", [Diag]),
            ?QC_FMT("Report = ~p\n", [Report]),
            ?QC_FMT("Sane = ~p\n", [Sane]),
            ?QC_FMT("SingleChainNoRepair failure =\n    ~p\n", [SingleChainNoRepair])
        end,
        conjunction([{res, Res == true orelse Res == ok},
                     {all_disjoint, ?MGRTEST:all_reports_are_disjoint(Report)},
                     {sane, SaneP},
                     {all_agreed_at_end, AgreedOrNot == [agreed_membership]},
                     {single_chain_no_repair, SingleChainNoRepair}
                    ]))
    end)).

prop_pulse_test_() ->
    Timeout = case os:getenv("PULSE_TIME") of
                  false -> 60;
                  Val   -> list_to_integer(Val)
              end,
    ExtraTO = case os:getenv("PULSE_SHRINK_TIME") of
                  false -> 0;
                  Val2  -> list_to_integer(Val2)
              end,
    {timeout, (Timeout+ExtraTO+300),     % 300 = a bit more fudge time
     fun() ->
             ?assert(eqc:quickcheck(eqc:testing_time(Timeout,
                                                     ?QC_OUT(prop_pulse()))))
     end}.

shutdown_hard() ->
    (catch machi_partition_simulator:stop()),
    [(catch machi_flu0:stop(X)) || X <- all_list()],
    timer:sleep(1),
    (catch exit(whereis(machi_partition_simulator), kill)),
    [(catch exit(whereis(X), kill)) || X <- all_list()],
    erlang:yield(),
    ok.

exec_ticks(Num, Mgr_pids) ->
    Parent = self(),
    Pids = [spawn(fun() ->
                          [begin
                               erlang:yield(),
                               Max = 10,
                               Elapsed =
                                   ?MGR:sleep_ranked_order(1, Max, M_name, all_list()),
                               Res = ?MGR:test_react_to_env(MMM),
                               timer:sleep(erlang:max(0, Max - Elapsed)),
                               Res=Res %% ?D({self(), Res})
                           end || _ <- lists:seq(1,Num)],
                          Parent ! done
                  end) || {M_name, MMM} <- lists:zip(all_list(), Mgr_pids) ],
    [receive
         done ->
             ok
     after 5000 ->
             exit(icky_timeout)
     end || _ <- Pids],    
    ok.

-endif. % PULSE
