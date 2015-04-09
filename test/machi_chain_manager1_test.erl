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
-module(machi_chain_manager1_test).

-include("machi.hrl").
-include("machi_projection.hrl").

-define(MGR, machi_chain_manager1).

-define(D(X), io:format(user, "~s ~p\n", [??X, X])).
-define(Dw(X), io:format(user, "~s ~w\n", [??X, X])).
-define(FLU_C,  machi_flu1_client).
-define(FLU_PC, machi_proxy_flu1_client).

-export([]).

-ifdef(TEST).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
%% -include_lib("eqc/include/eqc_statem.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

unanimous_report(Namez) ->
    UniquePrivateEs =
        lists:usort(lists:flatten(
                      [machi_flu0:proj_list_all(FLU, private) ||
                          {_FLUName, FLU} <- Namez])),
    [unanimous_report(Epoch, Namez) || Epoch <- UniquePrivateEs].

unanimous_report(Epoch, Namez) ->
    Projs = [{FLUName, case machi_flu0:proj_read(FLU, Epoch, private) of
                           {ok, T} -> T;
                           _Else   -> not_in_this_epoch
                       end} || {FLUName, FLU} <- Namez],
    UPI_R_Sums = [{Proj#projection_v1.upi, Proj#projection_v1.repairing,
                   Proj#projection_v1.epoch_csum} ||
                     {_FLUname, Proj} <- Projs,
                     is_record(Proj, projection_v1)],
    UniqueUPIs = lists:usort([UPI || {UPI, _Repairing, _CSum} <- UPI_R_Sums]),
    Res =
        [begin
             case lists:usort([CSum || {U, _Repairing, CSum} <- UPI_R_Sums,
                                       U == UPI]) of
                 [_1CSum] ->
                     %% Yay, there's only 1 checksum.  Let's check
                     %% that all FLUs are in agreement.
                     {UPI, Repairing, _CSum} =
                         lists:keyfind(UPI, 1, UPI_R_Sums),
                     %% TODO: make certain that this subtlety doesn't get
                     %%       last in later implementations.

                     %% So, this is a bit of a tricky thing.  If we're at
                     %% upi=[c] and repairing=[a,b], then the transition
                     %% (eventually!) to upi=[c,a] does not currently depend
                     %% on b being an active participant in the repair.
                     %%
                     %% Yes, b's state is very important for making certain
                     %% that all repair operations succeed both to a & b.
                     %% However, in this simulation, we only consider that
                     %% the head(Repairing) is sane.  Therefore, we use only
                     %% the "HeadOfRepairing" in our considerations here.
                     HeadOfRepairing = case Repairing of
                                          [H_Rep|_] ->
                                              [H_Rep];
                                          _ ->
                                              []
                                      end,
                     Tmp = [{FLU, case proplists:get_value(FLU, Projs) of
                                      P when is_record(P, projection_v1) ->
                                          P#projection_v1.epoch_csum;
                                      Else ->
                                          Else
                                  end} || FLU <- UPI ++ HeadOfRepairing],
                     case lists:usort([CSum || {_FLU, CSum} <- Tmp]) of
                         [_] ->
                             {agreed_membership, {UPI, Repairing}};
                         Else2 ->
                             {not_agreed, {UPI, Repairing}, Else2}
                     end;
                 _Else ->
                     {UPI, not_unique, Epoch, _Else}
             end
         end || UPI <- UniqueUPIs],
    AgreedResUPI_Rs = [UPI++Repairing ||
                          {agreed_membership, {UPI, Repairing}} <- Res],
    Tag = case lists:usort(lists:flatten(AgreedResUPI_Rs)) ==
               lists:sort(lists:flatten(AgreedResUPI_Rs)) of
              true ->
                  ok_disjoint;
              false ->
                  bummer_NOT_DISJOINT
          end,
    {Epoch, {Tag, Res}}.

all_reports_are_disjoint(Report) ->
    [] == [X || {_Epoch, Tuple}=X <- Report,
                element(1, Tuple) /= ok_disjoint].

extract_chains_relative_to_flu(FLU, Report) ->
    {FLU, [{Epoch, UPI, Repairing} ||
              {Epoch, {ok_disjoint, Es}} <- Report,
              {agreed_membership, {UPI, Repairing}} <- Es,
              lists:member(FLU, UPI) orelse lists:member(FLU, Repairing)]}.

chain_to_projection(MyName, Epoch, UPI_list, Repairing_list, All_list) ->
    ?MGR:make_projection(Epoch, MyName, All_list,
                         All_list -- (UPI_list ++ Repairing_list),
                         UPI_list, Repairing_list, []).

-ifndef(PULSE).

smoke0_test() ->
    %% TODO attack list:
    %% 0. Add start option to chain manager to be "passive" only, i.e.,
    %%    not immediately go to work on
    %% 1. Start FLUs with full complement of FLU+proj+chmgr.
    %% 2. Put each of them under a supervisor?
    %%    - Sup proc could be a created-specifically-for-test thing, perhaps?
    %%      Rather than relying on a supervisor with reg name + OTP app started
    %%      plus plus more more yaddayadda?
    %% 3. Add projection catalog/orddict of #p_srvr records??
    %% 4. Backport the changes to smoke0_test().
    %% 5. Do it to smoke1 test, yadda...
    {ok, _} = machi_partition_simulator:start_link({1,2,3}, 50, 50),
    Host = "localhost",
    TcpPort = 6623,
    {ok, FLUa} = machi_flu1:start_link([{a,TcpPort,"./data.a"}]),
    Pa = #p_srvr{name=a, proto=ipv4, address=Host, port=TcpPort},
    P_Srvr_Dict = machi_projection:make_members_dict([Pa]),
    %% Egadz, more racing on startup, yay.  TODO fix.
    timer:sleep(1),
    {ok, FLUaP} = ?FLU_PC:start_link(Pa),
    {ok, M0} = ?MGR:start_link(a, [a,b,c], P_Srvr_Dict, [{active_mode, false}]),
    _SockA = machi_util:connect(Host, TcpPort),
    try
        pong = ?MGR:ping(M0)
    after
        ok = ?MGR:stop(M0),
        ok = machi_flu1:stop(FLUa),
        ok = ?FLU_PC:quit(FLUaP),
        ok = machi_partition_simulator:stop()
    end.

smoke1_testTODO() ->
    machi_partition_simulator:start_link({1,2,3}, 100, 0),
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, FLUb} = machi_flu0:start_link(b),
    {ok, FLUc} = machi_flu0:start_link(c),
    I_represent = I_am = a,
    {ok, M0} = ?MGR:start_link(I_represent, [a,b,c], I_am),
    try
        {ok, _P1} = ?MGR:test_calc_projection(M0, false),

        _ = ?MGR:test_calc_proposed_projection(M0),
        {local_write_result, ok,
         {remote_write_results, [{b,ok},{c,ok}]}} =
            ?MGR:test_write_proposed_projection(M0),
        {unanimous, P1, Extra1} = ?MGR:test_read_latest_public_projection(M0, false),

        ok
    after
        ok = ?MGR:stop(M0),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb),
        ok = machi_flu0:stop(FLUc),
        ok = machi_partition_simulator:stop()
    end.

nonunanimous_setup_and_fix_testTODO() ->
    machi_partition_simulator:start_link({1,2,3}, 100, 0),
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, FLUb} = machi_flu0:start_link(b),
    I_represent = I_am = a,
    {ok, Ma} = ?MGR:start_link(I_represent, [a,b], I_am),
    {ok, Mb} = ?MGR:start_link(b, [a,b], b),
    try
        {ok, P1} = ?MGR:test_calc_projection(Ma, false),

        P1a = ?MGR:update_projection_checksum(
                P1#projection_v1{down=[b], upi=[a], dbg=[{hackhack, ?LINE}]}),
        P1b = ?MGR:update_projection_checksum(
                P1#projection_v1{author_server=b, creation_time=now(),
                              down=[a], upi=[b], dbg=[{hackhack, ?LINE}]}),
        P1Epoch = P1#projection_v1.epoch_number,
        ok = machi_flu0:proj_write(FLUa, P1Epoch, public, P1a),
        ok = machi_flu0:proj_write(FLUb, P1Epoch, public, P1b),

        ?D(x),
        {not_unanimous,_,_}=_XX = ?MGR:test_read_latest_public_projection(Ma, false),
        ?Dw(_XX),
        {not_unanimous,_,_}=_YY = ?MGR:test_read_latest_public_projection(Ma, true),
        %% The read repair here doesn't automatically trigger the creation of
        %% a new projection (to try to create a unanimous projection).  So
        %% we expect nothing to change when called again.
        {not_unanimous,_,_}=_YY = ?MGR:test_read_latest_public_projection(Ma, true),

        {now_using, _} = ?MGR:test_react_to_env(Ma),
        {unanimous,P2,E2} = ?MGR:test_read_latest_public_projection(Ma, false),
        {ok, P2pa} = machi_flu0:proj_read_latest(FLUa, private),
        P2 = P2pa#projection_v1{dbg2=[]},

        %% FLUb should still be using proj #0 for its private use
        {ok, P0pb} = machi_flu0:proj_read_latest(FLUb, private),
        0 = P0pb#projection_v1.epoch_number,

        %% Poke FLUb to react ... should be using the same private proj
        %% as FLUa.
        {now_using, _} = ?MGR:test_react_to_env(Mb),
        {ok, P2pb} = machi_flu0:proj_read_latest(FLUb, private),
        P2 = P2pb#projection_v1{dbg2=[]},

        ok
    after
        ok = ?MGR:stop(Ma),
        ok = ?MGR:stop(Mb),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb),
        ok = machi_partition_simulator:stop()
    end.

short_doc() ->
"
A visualization of the convergence behavior of the chain self-management
algorithm for Machi.
  1. Set up 4 FLUs and chain manager pairs.
  2. Create a number of different network partition scenarios, where
     (simulated) partitions may be symmetric or asymmetric.  Then halt changing
     the partitions and keep the simulated network stable and broken.
  3. Run a number of iterations of the algorithm in parallel by poking each
     of the manager processes on a random'ish basis.
  4. Afterward, fetch the chain transition changes made by each FLU and
     verify that no transition was unsafe.

During the iteration periods, the following is a cheatsheet for the output.
See the internal source for interpreting the rest of the output.

    'Let loose the dogs of war!'  Network instability
    'SET partitions = '           Network stability (but broken)
    'x uses:' The FLU x has made an internal state transition.  The rest of
              the line is a dump of internal state.
    '{t}'     This is a tick event which triggers one of the manager processes
              to evaluate its environment and perhaps make a state transition.

A long chain of '{t}{t}{t}{t}' means that the chain state has settled
to a stable configuration, which is the goal of the algorithm.
Press control-c to interrupt....".

long_doc() ->
    "
'Let loose the dogs of war!'

    The simulated network is very unstable for a few seconds.

'x uses'

    After a single iteration, server x has determined that the chain
    should be defined by the upi, repair, and down list in this record.
    If all participants reach the same conclusion at the same epoch
    number (and checksum, see next item below), then the chain is
    stable, fully configured, and can provide full service.

'epoch,E'

    The epoch number for this decision is E.  The checksum of the full
    record is not shown.  For purposes of the protocol, a server will
    'wedge' itself and refuse service (until a new config is chosen)
    whenever: a). it sees a bigger epoch number mentioned somewhere, or
    b). it sees the same epoch number but a different checksum.  In case
    of b), there was a network partition that has healed, and both sides
    had chosen to operate with an identical epoch number but different
    chain configs.

'upi', 'repair', and 'down'

    Members in the chain that are fully in sync and thus preserving the
    Update Propagation Invariant, up but under repair (simulated), and
    down, respectively.

'ps,[some list]'

    The list of asymmetric network partitions.  {a,b} means that a
    cannot send to b, but b can send to a.

    This partition list is recorded for debugging purposes but is *not*
    used by the algorithm.  The algorithm only 'feels' its effects via
    simulated timeout whenever there's a partition in one of the
    messaging directions.

'nodes_up,[list]'

    The best guess right now of which ndoes are up, relative to the
    author node, specified by '{author,X}'

'SET partitions = [some list]'

    All subsequent iterations should have a stable list of partitions,
    i.e. the 'ps' list described should be stable.

'{FLAP: x flaps n}!'

    Server x has detected that it's flapping/oscillating after iteration
    n of a naive/1st draft detection algorithm.
".

convergence_demo_testTODO_() ->
    {timeout, 98*300, fun() -> convergence_demo_testfun() end}.

convergence_demo_testfun() ->
    convergence_demo_testfun(3).

convergence_demo_testfun(NumFLUs) ->
    timer:sleep(100),
    io:format(user, short_doc(), []),
    %% Faster test startup, commented: timer:sleep(3000),

    FLU_biglist = [a,b,c,d,e,f,g],
    All_list = lists:sublist(FLU_biglist, NumFLUs),
    io:format(user, "\nSET # of FLus = ~w members ~w).\n",
              [NumFLUs, All_list]),
    machi_partition_simulator:start_link({111,222,33}, 0, 100),
    _ = machi_partition_simulator:get(All_list),

    Namez =
        [begin
             {ok, Pid} = machi_flu0:start_link(Name),
             {Name, Pid}
         end || Name <- All_list ],

    MgrOpts = [private_write_verbose],
    MgrNamez =
        [begin
             {ok, MPid} = ?MGR:start_link(Name, All_list, FLUPid, MgrOpts),
             {Name, MPid}
         end || {Name, FLUPid} <- Namez],
    try
      [{_, Ma}|_] = MgrNamez,
      {ok, P1} = ?MGR:test_calc_projection(Ma, false),
      P1Epoch = P1#projection_v1.epoch_number,
      [ok = machi_flu0:proj_write(FLUPid, P1Epoch, public, P1) ||
          {_, FLUPid} <- Namez, FLUPid /= Ma],

      machi_partition_simulator:reset_thresholds(10, 50),
      _ = machi_partition_simulator:get(All_list),

      Parent = self(),
      DoIt = fun(Iters, S_min, S_max) ->
                     io:format(user, "\nDoIt: top\n\n", []),
                     Pids = [spawn(fun() ->
                                           random:seed(now()),
                                           [begin
                                                erlang:yield(),
                                                S_max_rand = random:uniform(
                                                               S_max + 1),
                                                io:format(user, "{t}", []),
                                                Elapsed =
                                                    ?MGR:sleep_ranked_order(
                                                       S_min, S_max_rand,
                                                       M_name, All_list),
                                                _ = ?MGR:test_react_to_env(MMM),
                        %% if M_name == d ->
                        %%         [_ = ?MGR:test_react_to_env(MMM) ||
                        %%             _ <- lists:seq(1,3)],
                        %%         superunfair;
                        %%    true ->
                        %%         ok
                        %% end,
                                                %% Be more unfair by not
                                                %% sleeping here.
                                                %% timer:sleep(S_max - Elapsed),
                                                Elapsed
                                            end || _ <- lists:seq(1, Iters)],
                                           Parent ! done
                                   end) || {M_name, MMM} <- MgrNamez ],
                     [receive
                          done ->
                              ok
                      after 995000 ->
                              exit(icky_timeout)
                      end || _ <- Pids]
             end,

      _XandYs1 = [[{X,Y}] || X <- All_list, Y <- All_list, X /= Y],
      _XandYs2 = [[{X,Y}, {A,B}] || X <- All_list, Y <- All_list, X /= Y,
                                   A <- All_list, B <- All_list, A /= B,
                                   X /= A],
      _XandYs3 = [[{X,Y}, {A,B}, {C,D}] || X <- All_list, Y <- All_list, X /= Y,
                                          A <- All_list, B <- All_list, A /= B,
                                          C <- All_list, D <- All_list, C /= D,
                                          X /= A, X /= C, A /= C],
      %% AllPartitionCombinations = _XandYs1 ++ _XandYs2,
      %% AllPartitionCombinations = _XandYs3,
      AllPartitionCombinations = _XandYs1 ++ _XandYs2 ++ _XandYs3,
      ?D({?LINE, length(AllPartitionCombinations)}),

      machi_partition_simulator:reset_thresholds(10, 50),
      io:format(user, "\nLet loose the dogs of war!\n", []),
      DoIt(30, 0, 0),
      [begin
           io:format(user, "\nSET partitions = ~w.\n", [ [] ]),machi_partition_simulator:no_partitions(),
           [DoIt(50, 10, 100) || _ <- [1,2,3]],

           %% machi_partition_simulator:reset_thresholds(10, 50),
           %% io:format(user, "\nLet loose the dogs of war!\n", []),
           %% DoIt(30, 0, 0),

           machi_partition_simulator:always_these_partitions(Partition),
           io:format(user, "\nSET partitions = ~w.\n", [Partition]),
           [DoIt(50, 10, 100) || _ <- [1,2,3,4] ],
           PPP =
               [begin
                    PPPallPubs = machi_flu0:proj_list_all(FLU, public),
                    [begin
                         {ok, Pr} = machi_flu0:proj_read(FLU, PPPepoch, public),
                         {Pr#projection_v1.epoch_number, FLUName, Pr}
                     end || PPPepoch <- PPPallPubs]
                end || {FLUName, FLU} <- Namez],
           io:format(user, "PPP ~p\n", [lists:sort(lists:append(PPP))]),

           %%%%%%%% {stable,true} = {stable,private_projections_are_stable(Namez, DoIt)},
           {hosed_ok,true} = {hosed_ok,all_hosed_lists_are_identical(Namez, Partition)},
           io:format(user, "\nSweet, all_hosed are identical-or-islands-inconclusive.\n", []),
           timer:sleep(1000),
           ok
       end || Partition <- AllPartitionCombinations
       %% end || Partition <- [ [{a,b},{b,d},{c,b}],
       %%                       [{a,b},{b,d},{c,b}, {a,b},{b,a},{a,c},{c,a},{a,d},{d,a}],
       %%                       %% [{a,b},{b,d},{c,b}, {b,a},{a,b},{b,c},{c,b},{b,d},{d,b}],
       %%                       [{a,b},{b,d},{c,b}, {c,a},{a,c},{c,b},{b,c},{c,d},{d,c}],
       %%                       [{a,b},{b,d},{c,b}, {d,a},{a,d},{d,b},{b,d},{d,c},{c,d}] ]
       %% end || Partition <- [ [{a,b}, {b,c}],
       %%                       [{a,b}, {c,b}]  ]
       %% end || Partition <- [ [{a,b}, {b,c}]  ]  %% hosed-not-equal @ 3 FLUs
       %% end || Partition <- [ [{a,b}],
       %%                       [{b,a}] ]
       %% end || Partition <- [ [{a,b}, {c,b}],
       %%                       [{a,b}, {b,c}] ]
       %% end || Partition <- [ [{a,b}, {b,c},       {c,d}],
       %%                       [{a,b}, {b,c},{b,d}, {c,d}],
       %%                       [{b,a}, {b,c},       {c,d}],
       %%                       [{a,b}, {c,b},       {c,d}],
       %%                       [{a,b}, {b,c},       {d,c}] ]
       %% end || Partition <- [ [{a,b}, {b,c}, {c,d}, {d,e}],
       %%                       [{b,a}, {b,c}, {c,d}, {d,e}],
       %%                       [{a,b}, {c,b}, {c,d}, {d,e}],
       %%                       [{a,b}, {b,c}, {d,c}, {d,e}],
       %%                       [{a,b}, {b,c}, {c,d}, {e,d}] ]
       %% end || Partition <- [ [{c,a}] ]
       %% end || Partition <- [ [{c,a}], [{c,b}, {a, b}] ]
       %% end || Partition <- [ [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}],
       %%                       [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}, {b,c}],
       %%                       [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}, {c,d}] ]
       %% end || Partition <- [ [{a,b}],
       %%                       [{a,b}, {a,b},{b,a},{a,c},{c,a},{a,d},{d,a}],
       %%                       [{a,b}, {b,a},{a,b},{b,c},{c,b},{b,d},{d,b}],
       %%                       [{a,b}, {c,a},{a,c},{c,b},{b,c},{c,d},{d,c}],
       %%                       [{a,b}, {d,a},{a,d},{d,b},{b,d},{d,c},{c,d}] ]
      ],
       %% exit(end_experiment),

      io:format(user, "\nSET partitions = []\n", []),
      io:format(user, "We should see convergence to 1 correct chain.\n", []),
      machi_partition_simulator:no_partitions(),
      [DoIt(50, 10, 100) || _ <- [1]],
      io:format(user, "Sweet, finishing early\n", []), exit(yoyoyo_testing_hack),
      %% WARNING: In asymmetric partitions, private_projections_are_stable()
      %%          will never be true; code beyond this point on the -exp3
      %%          branch is bit-rotted, sorry!
      true = private_projections_are_stable(Namez, DoIt),
      io:format(user, "~s\n", [os:cmd("date")]),

      %% We are stable now ... analyze it.

      %% Create a report where at least one FLU has written a
      %% private projection.
      Report = unanimous_report(Namez),
      %% ?D(Report),

      %% Report is ordered by Epoch.  For each private projection
      %% written during any given epoch, confirm that all chain
      %% members appear in only one unique chain, i.e., the sets of
      %% unique chains are disjoint.
      true = all_reports_are_disjoint(Report),

      %% Given the report, we flip it around so that we observe the
      %% sets of chain transitions relative to each FLU.
      R_Chains = [extract_chains_relative_to_flu(FLU, Report) ||
                     FLU <- All_list],
      %% ?D(R_Chains),
      R_Projs = [{FLU, [chain_to_projection(FLU, Epoch, UPI, Repairing,
                                            All_list) ||
                           {Epoch, UPI, Repairing} <- E_Chains]} ||
                    {FLU, E_Chains} <- R_Chains],

      %% For each chain transition experienced by a particular FLU,
      %% confirm that each state transition is OK.
      try
          [{FLU, true} = {FLU, ?MGR:projection_transitions_are_sane(Ps, FLU)} ||
              {FLU, Ps} <- R_Projs],
          io:format(user, "\nAll sanity checks pass, hooray!\n", [])
      catch _Err:_What ->
              io:format(user, "Report ~p\n", [Report]),
              exit({line, ?LINE, _Err, _What})
      end,
      %% ?D(R_Projs),

      ok
    after
        [ok = ?MGR:stop(MgrPid) || {_, MgrPid} <- MgrNamez],
        [ok = machi_flu0:stop(FLUPid) || {_, FLUPid} <- Namez],
        ok = machi_partition_simulator:stop()
    end.

private_projections_are_stable(Namez, PollFunc) ->
    Private1 = [machi_flu0:proj_get_latest_num(FLU, private) ||
                   {_Name, FLU} <- Namez],
    PollFunc(5, 1, 10),
    Private2 = [machi_flu0:proj_get_latest_num(FLU, private) ||
                   {_Name, FLU} <- Namez],
    true = (Private1 == Private2).

all_hosed_lists_are_identical(Namez, Partition0) ->
    Partition = lists:usort(Partition0),
    Ps = [machi_flu0:proj_read_latest(FLU, private) || {_Name, FLU} <- Namez],
    UniqueAllHoseds = lists:usort([machi_chain_manager1:get_all_hosed(P) ||
                                      {ok, P} <- Ps]),
    Members = [M || {M, _Pid} <- Namez],
    Islands = machi_partition_simulator:partitions2num_islands(
                Members, Partition),
    %% io:format(user, "all_hosed_lists_are_identical:\n", []),
    %% io:format(user, "  Uniques = ~p Islands ~p\n  Partition ~p\n",
    %%           [Uniques, Islands, Partition]),
    case length(UniqueAllHoseds) of
        1 ->
            true;
        %% TODO: With the addition of the digraph stuff below, the clause
        %%       below probably isn't necessary anymore, since the
        %%       digraph calculation should catch complete partition islands?
        _ when Islands == 'many' ->
            %% There are at least two partitions, so yes, it's quite
            %% possible that the all_hosed lists may differ.
            %% TODO Fix this up to be smarter about fully-isolated
            %% islands of partition.
            true;
        _ ->
            DG = digraph:new(),
            Connection = machi_partition_simulator:partition2connection(
                           Members, Partition),
            [digraph:add_vertex(DG, X) || X <- Members],
            [digraph:add_edge(DG, X, Y) || {X,Y} <- Connection],
            Any = 
                lists:any(
                  fun(X) ->
                          NotX = Members -- [X],
                          lists:any(
                            fun(Y) ->
                                    %% There must be a shortest path of length
                                    %% two in both directions, otherwise
                                    %% the read projection call will fail.
                                    %% And it's that failure that we're
                                    %% interested in here.
                                    XtoY = digraph:get_short_path(DG, X, Y),
                                    YtoX = digraph:get_short_path(DG, Y, X),
                                    (XtoY == false orelse
                                     length(XtoY) > 2)
                                    orelse
                                    (YtoX == false orelse
                                     length(YtoX) > 2)
                            end, NotX)
                  end, Members),
            digraph:delete(DG),
            if Any == true ->
                    %% There's a missing path of length 2 between some
                    %% two FLUs, so yes, there's going to be
                    %% non-identical all_hosed lists.
                    true;
               true ->
                    false                   % There's no excuse, buddy
            end
    end.

-endif. % not PULSE
-endif. % TEST
