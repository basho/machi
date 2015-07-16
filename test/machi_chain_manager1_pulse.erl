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
-module(machi_chain_manager1_pulse).

%% The while module is ifdef:ed, rebar should set PULSE
-ifdef(PULSE).

-compile(export_all).

-include("machi_projection.hrl").
-include("machi_verbose.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include("machi.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{application, pulse_application}]}).
%% The following functions contains side_effects but are run outside
%% PULSE, i.e. PULSE needs to leave them alone
-compile({pulse_skip,[{prop_pulse_test_,0}, {prop_pulse_regression_test_,0},
                      {prop_pulse,1},
                      {shutdown_hard,0}]}).
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
    noshrink(frequency([
                        {10, {keep}},
                        {10, choose(1, 100)},
                        {10, oneof([{island1}])},
                        {10, oneof([{asymm1}, {asymm2}, {asymm3}])}
                       ])).

gen_no_partition_threshold() ->
    noshrink(choose(1, 100)).

gen_commands(new) ->
    non_empty(commands(?MODULE));
gen_commands(regression) ->
    %% These regression tests include only few, very limited command
    %% sequences that have been helpful in turning up bugs in the past.
    %% For this style test, QuickCheck is basically just choosing random
    %% seeds + PULSE execution to see if one of the oldies-but-goodies can
    %% find another execution/interleaving that still shows a problem.
    Cmd_a = [{set,{var,1},
              {call,machi_chain_manager1_pulse,setup,[3,{846,1222,4424}]}},
             {set,{var,2},
              {call,machi_chain_manager1_pulse,do_ticks,[6,{var,1},13,48]}}],
    Cmd_b = [{set,{var,1},
              {call,machi_chain_manager1_pulse,setup,[4,{354,7401,1237}]}},
             {set,{var,2},
              {call,machi_chain_manager1_pulse,do_ticks,[10,{var,1},15,77]}},
             {set,{var,3},
              {call,machi_chain_manager1_pulse,do_ticks,[7,{var,1},92,39]}}],
    Cmd_c = [{set,{var,1},
              {call,machi_chain_manager1_pulse,setup,[2,{5202,467,3157}]}},
             {set,{var,2},
              {call,machi_chain_manager1_pulse,do_ticks,[8,{var,1},98,3]}},
             {set,{var,3},
              {call,machi_chain_manager1_pulse,do_ticks,[5,{var,1},56,49]}},
             {set,{var,4},
              {call,machi_chain_manager1_pulse,do_ticks,[10,{var,1},33,72]}},
             {set,{var,5},
              {call,machi_chain_manager1_pulse,do_ticks,[10,{var,1},88,20]}},
             {set,{var,6},
              {call,machi_chain_manager1_pulse,do_ticks,[8,{var,1},67,10]}},
             {set,{var,7},
              {call,machi_chain_manager1_pulse,do_ticks,[5,{var,1},86,25]}},
             {set,{var,8},
              {call,machi_chain_manager1_pulse,do_ticks,[6,{var,1},74,88]}},
             {set,{var,9},
              {call,machi_chain_manager1_pulse,do_ticks,[8,{var,1},78,39]}}],
    Cmd_d = [{set,{var,1},
              {call,machi_chain_manager1_pulse,setup,[5,{436,5950,9085}]}},
             {set,{var,2},
              {call,machi_chain_manager1_pulse,do_ticks,[7,{var,1},19,80]}}],
    noshrink(oneof([Cmd_a, Cmd_b, Cmd_c, Cmd_d])).

command(#state{step=0}) ->
    {call, ?MODULE, setup, [gen_num_pids(), gen_seed()]};
command(S) ->
    frequency([
               { 1, {call, ?MODULE, change_partitions,
                     [gen_old_threshold(), gen_no_partition_threshold()]}},
               {50, {call, ?MODULE, do_ticks,
                     [choose(5, 50), S#state.pids,
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
    {PortBase, DirBase} = get_port_dir_base(),
    [ %% Genenerators assume that this list is at least 2 items
       {#p_srvr{name=a, address="localhost", port=PortBase+0,
                props=[{chmgr, a_chmgr}]}, DirBase ++ "/data.pulse.a"}
     , {#p_srvr{name=b, address="localhost", port=PortBase+1,
                props=[{chmgr, b_chmgr}]}, DirBase ++ "/data.pulse.b"}
     , {#p_srvr{name=c, address="localhost", port=PortBase+2,
                props=[{chmgr, c_chmgr}]}, DirBase ++ "//data.pulse.c"}
     , {#p_srvr{name=d, address="localhost", port=PortBase+3,
                props=[{chmgr, d_chmgr}]}, DirBase ++ "/data.pulse.d"}
     , {#p_srvr{name=e, address="localhost", port=PortBase+4,
                props=[{chmgr, e_chmgr}]}, DirBase ++ "/data.pulse.e"}
    ].

all_list() ->
    [P#p_srvr.name || {P, _Dir} <- all_list_extra()].

setup(Num, Seed) ->
    ?V("\nsetup(~w,~w", [self(), Num]),
    All_list = lists:sublist(all_list(), Num),
    All_listE = lists:sublist(all_list_extra(), Num),
    %% shutdown_hard() has taken care of killing all relevant procs.
    [begin
         machi_flu1_test:clean_up_data_dir(Dir),
         filelib:ensure_dir(Dir ++ "/not-used")
     end || {_P, Dir} <- All_listE],
    ?V(",z~w", [?LINE]),

    %% GRRR, not PULSE: {ok, _} = application:ensure_all_started(machi),
    [begin
         ?V(",z~w,~w", [?LINE,App]),
         _QQ = (catch application:start(App)),
         erlang:display({app_start,App,_QQ})
     end || App <- [machi] ],
    ?V(",z~w", [?LINE]),

    SimSpec = {part_sim, {machi_partition_simulator, start_link,
                          [{0,0,0}, 0, 100]},
               permanent, 500, worker, []},
    ?V(",z~w", [?LINE]),
    {ok, PSimPid} = supervisor:start_child(machi_sup, SimSpec),
    ?V(",z~w", [?LINE]),
    ok = machi_partition_simulator:set_seed(Seed),
    _Partitions = machi_partition_simulator:get(All_list),
    ?V(",z~w", [?LINE]),

    %% Start FLUs and their associated procs
    FluOpts = [{use_partition_simulator, true}, {active_mode, false}],
    [begin
         #p_srvr{name=Name, port=Port} = P,
         {ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, FluOpts)
     end || {P, Dir} <- All_listE],
    %% Set up the chain
    Dict = orddict:from_list([{P#p_srvr.name, P} || {P, _Dir} <- All_listE]),
    ?V(",z~w", [?LINE]),
    [machi_chain_manager1:set_chain_members(get_chmgr(P), Dict) ||
        {P, _Dir} <- All_listE],

    %% Trigger some environment reactions for humming consensus: first
    %% do all the same server first, then round-robin evenly across
    %% servers.
    [begin
         _QQa = machi_chain_manager1:test_react_to_env(get_chmgr(P))
     end || {P, _Dir} <- All_listE, _I <- lists:seq(1,20), _Repeat <- [1,2]],
    ?V(",z~w", [?LINE]),
    [begin
         _QQa = machi_chain_manager1:test_react_to_env(get_chmgr(P))
     end || _I <- lists:seq(1,20), {P, _Dir} <- All_listE, _Repeat <- [1,2]],
    ?V(",z~w", [?LINE]),

    ProxiesDict = ?FLU_PC:start_proxies(Dict),

    Res = {PSimPid, 'machi_flu_sup', ProxiesDict, All_listE},
    put(manager_pids_hack, Res),
    ?V("),", []),
    Res.

change_partitions(OldThreshold, NoPartitionThreshold)
  when is_integer(OldThreshold) ->
    machi_partition_simulator:reset_thresholds(OldThreshold,
                                               NoPartitionThreshold);
change_partitions({keep}, _NoPartitionThreshold) ->
    ok;
change_partitions({island1}, _NoPartitionThreshold) ->
    AB = [a,b],
    NotAB = all_list() -- AB,
    Partitions = lists:usort([{X, Y} || X <- AB, Y <- NotAB] ++
                             [{X, Y} || X <- NotAB, Y <- AB]),
    machi_partition_simulator:always_these_partitions(Partitions);
change_partitions({asymm1}, _NoPartitionThreshold) ->
    Partitions = [{a,b}],
    machi_partition_simulator:always_these_partitions(Partitions);
change_partitions({asymm2}, _NoPartitionThreshold) ->
    Partitions = [{a,b},{a,c},{a,d},{a,e},{b,a},{b,c},{b,e},{c,a},{c,b},{c,d},{c,e},{d,a},{d,c},{d,e},{e,a},{e,b},{e,c},{e,d}],
    machi_partition_simulator:always_these_partitions(Partitions);
change_partitions({asymm3}, _NoPartitionThreshold) ->
    Partitions = [{a,b},{a,c},{a,d},{a,e},{b,a},{b,d},{b,e},{c,d},{d,a},{d,c},{d,e},{e,a},{e,b},{e,d}],
    machi_partition_simulator:always_these_partitions(Partitions).

always_last_partitions() ->
    machi_partition_simulator:always_last_partitions().

private_stable_check() ->
    {_PSimPid, _SupPid, ProxiesDict, All_listE} = get(manager_pids_hack),
    Res = private_projections_are_stable_check(ProxiesDict, All_listE),
    if not Res ->
            ?QC_FMT("BUMMER: private stable check failed!\n", []);
       true ->
            ok
    end,
    Res.

do_ticks(Num, PidsMaybe, OldThreshold, NoPartitionThreshold) ->
    ?V("~p,~p,~p|", [Num, OldThreshold, NoPartitionThreshold]),
    {_PSimPid, _SupPid, ProxiesDict, All_listE} =
        case PidsMaybe of
            undefined -> get(manager_pids_hack);
            _         -> PidsMaybe
        end,
    if is_atom(OldThreshold) ->
            ?V("{e=~w},", [get_biggest_private_epoch_number(ProxiesDict)]),
            machi_partition_simulator:no_partitions();
       true ->
            change_partitions(OldThreshold, NoPartitionThreshold)
    end,
    Res = exec_ticks(Num, All_listE),
    if not is_integer(OldThreshold) ->
            ?V("{e=~w},", [get_biggest_private_epoch_number(ProxiesDict)]);
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
    ?V("dump_state(", []),
    {_PSimPid, _SupPid, ProxiesDict, _AlE} = get(manager_pids_hack),
    Report = ?MGRTEST:unanimous_report(ProxiesDict),
    Namez = ProxiesDict,
    PrivProjs = [{Name, begin
                            {ok, Ps} = ?FLU_PC:get_all_projections(Proxy,
                                                                   private),
                            [P || P <- Ps,
                                  P#projection_v1.epoch_number /= 0]
                        end} || {Name, Proxy} <- ProxiesDict],
    ?V("~w,", [ [{X,whereis(X)} || X <- [machi_sup, machi_flu_sup, machi_partition_simulator]] ]),
    ?V("~w,", [catch application:stop(machi)]),
    [?V("~w,", [timer:sleep(10)]) || _ <- lists:seq(1,50)],
    ?V("~w,", [ [{X,whereis(X)} || X <- [machi_sup, machi_flu_sup, machi_partition_simulator]] ]),
    ?V(")", []),
    Diag1 = Diag2 = "skip_diags",
    {Report, PrivProjs, lists:flatten([Diag1, Diag2])}
  catch XX:YY ->
        ?V("OUCH: ~p ~p @ ~p\n", [XX, YY, erlang:get_stacktrace()]),
        ?V("Exiting now to move to manual post-mortem....\n", []),
        erlang:halt(66),
        false
  end.

prop_pulse() ->
    prop_pulse(new).

prop_pulse(Style) when Style == new; Style == regression ->
    _ = application:start(sasl),
    _ = application:start(crypto),
    ?FORALL({Cmds0, Seed}, {gen_commands(Style), pulse:seed()},
    ?IMPLIES(1 < length(Cmds0) andalso length(Cmds0) < 11,
    begin
erlang:display({prop,?MODULE,?LINE,self()}),
        ok = shutdown_hard(),
erlang:display({prop,?MODULE,?LINE,self()}),
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
        Cmds1 = lists:duplicate(4, LastTriggerTicks),
        Cmds = Cmds0 ++
               Stabilize1 ++
               Cmds1 ++
               Stabilize2 ++
            [{set,{var,99999999}, {call, ?MODULE, dump_state, []}}],

        error_logger:tty(false),
        pulse:verbose([format]),
        {_H2, S2, Res} = pulse:run(
                           fun() ->
                                   ?V("PROP-~w,", [self()]),
                                   %% {_H, _S, _R} = run_commands(?MODULE, Cmds)
                                   _QAQA = run_commands(?MODULE, Cmds)
, erlang:display({prop,?MODULE,?LINE,self()}), _QAQA
                                   %% ,?V("pid681=~p", [process_info(list_to_pid("<0.681.0>"))]), _QAQA
                           end, [{seed, Seed},
                                 {strategy, unfair}]),
erlang:display({prop,?MODULE,?LINE,self()}),
        ok = shutdown_hard(),
        {Report, PrivProjs, Diag} = S2#state.dump_state,

        %% Report is ordered by Epoch.  For each private projection
        %% written during any given epoch, confirm that all chain
        %% members appear in only one unique chain, i.e., the sets of
        %% unique chains are disjoint.
        {AllDisjointP, AllDisjointDetail} =
            case ?MGRTEST:all_reports_are_disjoint(Report) of
                true -> {true, true};
                Else -> {false, Else}
            end,

        %% For each chain transition experienced by a particular FLU,
        %% confirm that each state transition is OK.
        Sane =
          [{FLU,_SaneRes} = {FLU,?MGR:projection_transitions_are_sane_retrospective(
                                    Ps, FLU)} ||
              {FLU, Ps} <- PrivProjs],
        SaneP = lists:all(fun({_FLU, SaneRes}) -> SaneRes == true end, Sane),
        %% On a really bad day, this could trigger a badmatch exception....
        {_LastEpoch, {ok_disjoint, LastRepXs}} = lists:last(Report),
        
        %% TODO: Check that we've converged to a single chain with no repairs.
        {SingleChainNoRepair_p, SingleChainNoRepairDetail} =
             case LastRepXs of
                 [LastUPI] when length(LastUPI) == S2#state.num_pids ->
                     {true, true};
                 _ ->
                     {false, LastRepXs}
             end,

        ?WHENFAIL(
        begin
            %% ?QC_FMT("PrivProjs = ~P\n", [PrivProjs, 50]),
            ?QC_FMT("Report = ~p\n", [Report]),
            ?QC_FMT("Cmds = ~p\n", [Cmds]),
            ?QC_FMT("Res = ~p\n", [Res]),
            ?QC_FMT("Diag = ~s\n", [Diag]),
            ?QC_FMT("Sane = ~p\n", [Sane]),
            ?QC_FMT("AllDisjointDetail = ~p\n", [AllDisjointDetail]),
            ?QC_FMT("SingleChainNoRepair failure = ~p\n", [SingleChainNoRepairDetail])
,?QC_FMT("\n\nHalting now!!!!!!!!!!\n\n", []),timer:sleep(500),erlang:halt(1)
        end,
        conjunction([{res, Res == true orelse Res == ok},
                     {all_disjoint, AllDisjointP},
                     {sane, SaneP},
                     {single_chain_no_repair, SingleChainNoRepair_p}
                    ]))
    end)).

-define(FIXTURE(TIMEOUT, EXTRATO, FUN), {timeout, (Timeout+ExtraTO+600), FUN}).

prop_pulse_new_test_() ->
    {Timeout, ExtraTO} = get_timeouts(),
    DoShrink = get_do_shrink(),
    F = fun() ->
             ?assert(do_quickcheck(DoShrink, Timeout, new))
        end,
    case os:getenv("PULSE_SKIP_NEW") of
        false ->
            ?FIXTURE(Timeout, ExtraTO, F);
        _ ->
            {timeout, 5,
             fun() -> timer:sleep(200),
                      io:format(user, " (skip new style) ", []) end}
    end.

%% See gen_commands() for more detail on the regression tests.

prop_pulse_regression_test_() ->
    {Timeout, ExtraTO} = get_timeouts(),
    DoShrink = get_do_shrink(),
    F = fun() ->
             ?assert(do_quickcheck(DoShrink, Timeout, regression))
        end,
    case os:getenv("PULSE_SKIP_REGRESSION") of
        false ->
            ?FIXTURE(Timeout, ExtraTO, F);
        _ ->
            {timeout, 5,
             fun() -> timer:sleep(200),
                      io:format(user, " (skip regression style) ", []) end}
    end.

do_quickcheck(Timeout, Style) ->
    do_quickcheck(true, Timeout, Style).

do_quickcheck(true, Timeout, Style) ->
    eqc:quickcheck(eqc:testing_time(Timeout,
                                    ?QC_OUT(prop_pulse(Style))));
do_quickcheck(false, Timeout, Style) ->
  noshrink(
    eqc:quickcheck(eqc:testing_time(Timeout,
                                    ?QC_OUT(prop_pulse(Style))))).

get_timeouts() ->
    Timeout = case os:getenv("PULSE_TIME") of
                  false -> 60;
                  Val   -> list_to_integer(Val)
              end,
    ExtraTO = case os:getenv("PULSE_SHRINK_TIME") of
                  false -> 0;
                  Val2  -> list_to_integer(Val2)
              end,
    {Timeout, ExtraTO}.

get_do_shrink() ->
    case os:getenv("PULSE_NOSHRINK") of
        false ->
            false;
        _ ->
            true
    end.

shutdown_hard() ->
erlang:display({hard,?MODULE,?LINE,self()}),
    %% HANG: [catch machi_flu_psup:stop_flu_package(FLU) || FLU <- all_list()],
    erlang:display({apps,?LINE,application:which_applications()}),
    %%erlang:display({apps,?LINE,application:which_applications()}),
    [begin
erlang:display({hard,?MODULE,?LINE,self()}),
         _STOP = application:stop(App),
         erlang:display({stop, App, _STOP})
     end || App <- [machi] ],
    timer:sleep(100),

    (catch unlink(whereis(machi_partition_simulator))),
    [begin
         Pid = whereis(X),
erlang:display({hard,?MODULE,?LINE,self(),X,Pid}),
okokokokokokwhaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
         %% %%%%%%DELME deadlock source? spawn(fun() -> ?QC_FMT("shutdown-~w,", [self()]), (catch X:stop()) end),
         %% timer:sleep(50),
         %% timer:sleep(10),
         %% (catch exit(Pid, shutdown)),
         %% timer:sleep(1),
         %% (catch exit(Pid, kill))
     %% end || X <- [machi_partition_simulator] ],
     end || X <- [machi_partition_simulator, machi_flu_sup, machi_sup] ],
    timer:sleep(100),
    ok.

exec_ticks(Num, All_listE) ->
    Parent = self(),
    Pids = [spawn_link(fun() ->
                          ?V("tick-~w,", [self()]),
                          [begin
                               M_name = P#p_srvr.name,
                               %% Max = 10,
                               Max = 25,
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

%%    {PortBase, DirBase} = get_port_dir_base(),
get_port_dir_base() ->
    I = case os:getenv("PULSE_BASE_PORT") of
            false ->
                0;
            II ->
                list_to_integer(II)
        end,
    D = case os:getenv("PULSE_BASE_DIR") of
            false ->
                "/tmp/c/";
            DD ->
                DD
        end,
    {7400 + (I * 100), D ++ "/" ++ integer_to_list(I)}.

-endif. % PULSE
