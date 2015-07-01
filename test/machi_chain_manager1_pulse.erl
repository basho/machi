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

-include("machi_projection.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include("machi.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{application, pulse_application}]}).
%% The following functions contains side_effects but are run outside
%% PULSE, i.e. PULSE needs to leave them alone
-compile({pulse_skip,[{prop_pulse_test_,0}, {shutdown_hard,0}]}).
-compile({pulse_no_side_effect,[{file,'_','_'}, {erlang, now, 0}]}).

%% Used for output within EUnit...
-define(QC_FMT(Fmt, Args),
        io:format(user, Fmt, Args)).

%% And to force EUnit to output QuickCheck output...
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> ?QC_FMT(Str, Args) end, P)).

-define(MGR, machi_chain_manager1).
-define(MGRTEST, machi_chain_manager1_test).
-define(FLU_PC, machi_proxy_flu1_client).

-record(state, {
          step=0,
          num_pids,
          pids,
          dump_state
         }).

initial_state() ->
    #state{}.

gen_num_pids() ->
    choose(2, length(all_list_extra())).

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
                     [choose(5, 200), S#state.pids,
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

all_list_extra() ->
    [ %% Genenerators assume that this list is at least 2 items
       {#p_srvr{name=a, address="localhost", port=7400,
                props=[{chmgr, a_chmgr}]}, "./data.pulse.a"}
     , {#p_srvr{name=b, address="localhost", port=7401,
                props=[{chmgr, b_chmgr}]}, "./data.pulse.b"}
     , {#p_srvr{name=c, address="localhost", port=7402,
                props=[{chmgr, c_chmgr}]}, "./data.pulse.c"}
     , {#p_srvr{name=d, address="localhost", port=7403,
                props=[{chmgr, d_chmgr}]}, "./data.pulse.d"}
     , {#p_srvr{name=e, address="localhost", port=7404,
                props=[{chmgr, e_chmgr}]}, "./data.pulse.e"}
    ].

all_list() ->
    [P#p_srvr.name || {P, _Dir} <- all_list_extra()].

setup(Num, Seed) ->
    ?QC_FMT("\nsetup(~w", [Num]),
    error_logger:tty(false),
    All_list = lists:sublist(all_list(), Num),
    All_listE = lists:sublist(all_list_extra(), Num),
    %% shutdown_hard() has taken care of killing all relevant procs.
    [machi_flu1_test:clean_up_data_dir(Dir) || {_P, Dir} <- All_listE],
    ?QC_FMT(",z~w", [?LINE]),

    %% Start partition simulator
    {ok, PSimPid} = machi_partition_simulator:start_link(Seed, 0, 100),
    _Partitions = machi_partition_simulator:get(All_list),
    ?QC_FMT(",z~w", [?LINE]),

    %% Start FLUs and their associated procs
    {ok, SupPid} = machi_flu_sup:start_link(),
    FluOpts = [{use_partition_simulator, true}, {active_mode, false}],
    [begin
         #p_srvr{name=Name, port=Port} = P,
         {ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, FluOpts)
     end || {P, Dir} <- All_listE],
    %% Set up the chain
    Dict = orddict:from_list([{P#p_srvr.name, P} || {P, _Dir} <- All_listE]),
    ?QC_FMT(",z~w", [?LINE]),
    [machi_chain_manager1:set_chain_members(get_chmgr(P), Dict) ||
        {P, _Dir} <- All_listE],
    %% Trigger some environment reactions for humming consensus: first
    %% do all the same server first, then round-robin evenly across
    %% servers.
    [begin
         _QQa = machi_chain_manager1:test_react_to_env(get_chmgr(P))
     end || {P, _Dir} <- All_listE, _I <- lists:seq(1,20), _Repeat <- [1,2]],
    ?QC_FMT(",z~w", [?LINE]),
    [begin
         _QQa = machi_chain_manager1:test_react_to_env(get_chmgr(P))
     end || _I <- lists:seq(1,20), {P, _Dir} <- All_listE, _Repeat <- [1,2]],
    ?QC_FMT(",z~w", [?LINE]),

    ProxiesDict = ?FLU_PC:start_proxies(Dict),

    Res = {PSimPid, SupPid, ProxiesDict, All_listE},
    put(manager_pids_hack, Res),
    ?QC_FMT("),", []),
    Res.

change_partitions(OldThreshold, NoPartitionThreshold) ->
    machi_partition_simulator:reset_thresholds(OldThreshold,
                                               NoPartitionThreshold).

always_last_partitions() ->
    machi_partition_simulator:always_last_partitions().

private_stable_check() ->
    {_PSimPid, _SupPid, ProxiesDict, All_listE} = get(manager_pids_hack),
    Res = private_projections_are_stable_check(ProxiesDict, All_listE),
    if not Res ->
            io:format(user, "BUMMER: private stable check failed!\n", []);
       true ->
            ok
    end,
    Res.

do_ticks(Num, PidsMaybe, OldThreshold, NoPartitionThreshold) ->
    io:format(user, "~p,~p,~p|", [Num, OldThreshold, NoPartitionThreshold]),
    {_PSimPid, _SupPid, ProxiesDict, All_listE} =
        case PidsMaybe of
            undefined -> get(manager_pids_hack);
            _         -> PidsMaybe
        end,
    if is_integer(OldThreshold) ->
            machi_partition_simulator:reset_thresholds(OldThreshold,
                                                       NoPartitionThreshold);
       true ->
            ?QC_FMT("{e=~w},", [get_biggest_private_epoch_number(ProxiesDict)]),
            machi_partition_simulator:no_partitions()
    end,
    Res = exec_ticks(Num, All_listE),
    if not is_integer(OldThreshold) ->
            ?QC_FMT("{e=~w},", [get_biggest_private_epoch_number(ProxiesDict)]);
       true ->
            ok
    end,
    Res.

get_biggest_private_epoch_number(ProxiesDict) ->
    lists:last(
      lists:usort(
        lists:flatten(
          [begin
               {ok, {Epoch, _}} = ?FLU_PC:get_latest_epochid(Proxy, private),
               Epoch
           end || {_Name, Proxy} <- orddict:to_list(ProxiesDict)]))).

dump_state() ->
  try
    ?QC_FMT("dump_state(", []),
    {_PSimPid, _SupPid, ProxiesDict, _AlE} = get(manager_pids_hack),
    Report = ?MGRTEST:unanimous_report(ProxiesDict),
    Namez = ProxiesDict,
    %% ?QC_FMT("Report ~p\n", [Report]),

    %% Diag1 = [begin
    %%              {ok, Ps} = ?FLU_PC:get_all_projections(Proxy, Type),
    %%              [io_lib:format("~p ~p ~p: ~w\n", [FLUName, Type, P#projection_v1.epoch_number, machi_projection:make_summary(P)]) || P <- Ps]
    %%          end || {FLUName, Proxy} <- orddict:to_list(ProxiesDict),
    %%                 Type <- [public] ],

    UniquePrivateEs =
        lists:usort(lists:flatten(
                      [element(2,?FLU_PC:list_all_projections(Proxy,private)) ||
                          {_FLUName, Proxy} <- orddict:to_list(ProxiesDict)])),
    P_lists0 = [{FLUName, Type,
                 element(2,?FLU_PC:get_all_projections(Proxy, Type))} ||
                   {FLUName, Proxy} <- orddict:to_list(ProxiesDict),
                   Type <- [public,private]],
    P_lists = [{FLUName, Type, P} || {FLUName, Type, Ps} <- P_lists0,
                                     P <- Ps],
    AllDict = lists:foldl(fun({FLU, Type, P}, D) ->
                                  K = {FLU, Type, P#projection_v1.epoch_number},
                                  dict:store(K, P, D)
                          end, dict:new(), lists:flatten(P_lists)),
    DumbFinderBackward =
        fun(FLUName) ->
                fun(E, error_unwritten) ->
                        case dict:find({FLUName, private, E}, AllDict) of
                            {ok, T} -> T;
                            error   -> error_unwritten
                        end;
                   (_E, Acc) ->
                        Acc
                end
        end,
    %% Diag2 = [[
    %%             io_lib:format("~p private: ~w\n",
    %%                           [FLUName,
    %%                            machi_projection:make_summary(
    %%                               lists:foldl(DumbFinderBackward(FLUName),
    %%                                           error_unwritten,
    %%                                           lists:seq(Epoch, 0, -1)))])
    %%           || {FLUName, _FLU} <- Namez]
    %%          || Epoch <- UniquePrivateEs],

    PrivProjs = [{Name, begin
                            {ok, Ps} = ?FLU_PC:get_all_projections(Proxy,
                                                                   private),
                            [P || P <- Ps,
                                  P#projection_v1.epoch_number /= 0]
                        end} || {Name, Proxy} <- ProxiesDict],

    ?QC_FMT(")", []),
    Diag1 = Diag2 = "skip_diags",
    {Report, PrivProjs, lists:flatten([Diag1, Diag2])}
  catch XX:YY ->
        ?QC_FMT("OUCH: ~p ~p @ ~p\n", [XX, YY, erlang:get_stacktrace()]),
        ?QC_FMT("Exiting now to move to manual post-mortem....\n", []),
        erlang:halt(0),
        false
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
        Stabilize1 = [{set,{var,99999995},
                      {call, ?MODULE, always_last_partitions, []}}],
        Stabilize2 = [{set,{var,99999996},
                       {call, ?MODULE, private_stable_check, []}}],
        LastTriggerTicks = {set,{var,99999997},
                            {call, ?MODULE, do_ticks, [123, undefined, no, no]}},
        Cmds1 = lists:duplicate(2, LastTriggerTicks),
        %% Cmds1 = lists:duplicate(length(all_list())*2, LastTriggerTicks),
        Cmds = Cmds0 ++
               Stabilize1 ++
               Cmds1 ++
               Stabilize2 ++
            [{set,{var,99999999}, {call, ?MODULE, dump_state, []}}],
        {_H2, S2, Res} = pulse:run(
                           fun() ->
                                   {_H, _S, _R} = run_commands(?MODULE, Cmds)
                           end, [{seed, Seed},
                                 {strategy, unfair}]),
        %% ?QC_FMT("S2 ~p\n", [S2]),
        case S2#state.dump_state of
            undefined ->
                ?QC_FMT("BUMMER Cmds = ~p\n", [Cmds]);
            _ ->
                ok
        end,
        {Report, PrivProjs, Diag} = S2#state.dump_state,

        %% Report is ordered by Epoch.  For each private projection
        %% written during any given epoch, confirm that all chain
        %% members appear in only one unique chain, i.e., the sets of
        %% unique chains are disjoint.
        AllDisjointP = ?MGRTEST:all_reports_are_disjoint(Report),

        %% For each chain transition experienced by a particular FLU,
        %% confirm that each state transition is OK.
        Sane =
          [{FLU,_SaneRes} = {FLU,?MGR:projection_transitions_are_sane_retrospective(
                                    Ps, FLU)} ||
              {FLU, Ps} <- PrivProjs],
        SaneP = lists:all(fun({_FLU, SaneRes}) -> SaneRes == true end, Sane),

        %% The final report item should say that all are agreed_membership.
        {_LastEpoch, {ok_disjoint, LastRepXs}} = lists:last(Report),
        AgreedOrNot = lists:usort([element(1, X) || X <- LastRepXs]),
        
        %% TODO: Check that we've converged to a single chain with no repairs.
        SingleChainNoRepair = case LastRepXs of
                                  [{agreed_membership,{_UPI,[]}}] ->
                                      true;
                                  _ ->
                                      LastRepXs
                              end,

        ok = shutdown_hard(),
        ?WHENFAIL(
        begin
            ?QC_FMT("Cmds = ~p\n", [Cmds]),
            ?QC_FMT("Res = ~p\n", [Res]),
            ?QC_FMT("Diag = ~s\n", [Diag]),
            ?QC_FMT("Report = ~p\n", [Report]),
            ?QC_FMT("PrivProjs = ~p\n", [PrivProjs]),
            ?QC_FMT("Sane = ~p\n", [Sane]),
            ?QC_FMT("SingleChainNoRepair failure =\n    ~p\n", [SingleChainNoRepair])
,erlang:halt(0)
        end,
        conjunction([{res, Res == true orelse Res == ok},
                     {all_disjoint, AllDisjointP},
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
    {timeout, (Timeout+ExtraTO+600),     % 600 = a bit more fudge time
     fun() ->
             ?assert(eqc:quickcheck(eqc:testing_time(Timeout,
                                                     ?QC_OUT(prop_pulse()))))
     end}.

shutdown_hard() ->
    ?QC_FMT("shutdown(", []),
    (catch unlink(whereis(machi_partition_simulator))),
    [begin
         Pid = whereis(X),
         spawn(fun() -> (catch X:stop()) end),
         timer:sleep(50),
         (catch unlink(Pid)),
         timer:sleep(10),
         (catch exit(Pid, shutdown)),
         timer:sleep(1),
         (catch exit(Pid, kill))
     end || X <- [machi_partition_simulator, machi_flu_sup] ],
    timer:sleep(1),
    ?QC_FMT(")", []),
    ok.

exec_ticks(Num, All_listE) ->
    Parent = self(),
    Pids = [spawn_link(fun() ->
                          [begin
                               erlang:yield(),
                               M_name = P#p_srvr.name,
                               Max = 10,
                               Elapsed =
                                   ?MGR:sleep_ranked_order(1, Max, M_name, all_list()),
                               Res = ?MGR:test_react_to_env(get_chmgr(P)),
                               timer:sleep(erlang:max(0, Max - Elapsed)),
                               Res=Res %% ?D({self(), Res})
                           end || _ <- lists:seq(1,Num)],
                          Parent ! done
                  end) || {P, _Dir} <- All_listE],
    [receive
         done ->
             ok
     %% after 500*1000 ->
     %%         exit(icky_timeout)
     end || _Pid <- Pids],    
    ok.

private_projections_are_stable_check(ProxiesDict, All_listE) ->
    %% TODO: extend the check to look not only for latest num, but
    %% also check for flapping, and if yes, to see if all_hosed are
    %% all exactly equal.

    _ = exec_ticks(40, All_listE),
    Private1 = [?FLU_PC:get_latest_epochid(Proxy, private) ||
                   {_FLU, Proxy} <- orddict:to_list(ProxiesDict)],
    _ = exec_ticks(5, All_listE),
    Private2 = [?FLU_PC:get_latest_epochid(Proxy, private) ||
                   {_FLU, Proxy} <- orddict:to_list(ProxiesDict)],

    (Private1 == Private2).

get_chmgr(#p_srvr{props=Ps}) ->
    proplists:get_value(chmgr, Ps).

-endif. % PULSE
