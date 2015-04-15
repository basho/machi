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
-module(machi_chain_manager1_converge_demo).

-include("machi.hrl").
-include("machi_projection.hrl").

-define(MGR, machi_chain_manager1).

-define(D(X), io:format(user, "~s ~p\n", [??X, X])).
-define(Dw(X), io:format(user, "~s ~w\n", [??X, X])).
-define(FLU_C,  machi_flu1_client).
-define(FLU_PC, machi_proxy_flu1_client).

-compile(export_all).

-ifdef(TEST).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
%% -include_lib("eqc/include/eqc_statem.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.

-include_lib("eunit/include/eunit.hrl").

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

%% convergence_demo_test_() ->
%%     {timeout, 98*300, fun() -> convergence_demo_testfun() end}.

%% convergence_demo_testfun() ->
%%     convergence_demo_testfun(3).

t() ->
    t(3).

t(N) ->
    convergence_demo_testfun(N).

convergence_demo_testfun(NumFLUs) ->
    timer:sleep(100),
    %% Faster test startup, commented: io:format(user, short_doc(), []),
    %% Faster test startup, commented: timer:sleep(3000),

    TcpPort = 62877,
    FluInfo = [{a,TcpPort+0,"./data.a"}, {b,TcpPort+1,"./data.b"},
               {c,TcpPort+2,"./data.c"}, {d,TcpPort+3,"./data.d"},
               {e,TcpPort+4,"./data.e"}, {f,TcpPort+5,"./data.f"}],
    FLU_biglist = [X || {X,_,_} <- FluInfo],
    All_list = lists:sublist(FLU_biglist, NumFLUs),
    io:format(user, "\nSET # of FLus = ~w members ~w).\n",
              [NumFLUs, All_list]),
    machi_partition_simulator:start_link({111,222,33}, 0, 100),
    _ = machi_partition_simulator:get(All_list),

    Ps = [#p_srvr{name=Name,address="localhost",port=Port} ||
             {Name,Port,_Dir} <- lists:sublist(FluInfo, NumFLUs)],
    PsDirs = lists:zip(Ps,
                       [Dir || {_,_,Dir} <- lists:sublist(FluInfo, NumFLUs)]),
    FLU_pids = [machi_flu1_test:setup_test_flu(Name, Port, Dir) ||
                   {#p_srvr{name=Name,port=Port}, Dir} <- PsDirs],
    Namez = [begin
                 {ok, PPid} = ?FLU_PC:start_link(P),
                 {Name, PPid}
             end || {#p_srvr{name=Name}=P, _Dir} <- PsDirs],
    MembersDict = machi_projection:make_members_dict(Ps),
    MgrOpts = [private_write_verbose, {active_mode,false}],
    MgrNamez =
        [begin
             {ok, MPid} = ?MGR:start_link(P#p_srvr.name, MembersDict, MgrOpts),
             {P#p_srvr.name, MPid}
         end || P <- Ps],

    try
      [{_, Ma}|_] = MgrNamez,
      {ok, P1} = ?MGR:test_calc_projection(Ma, false),
      [ok = ?FLU_PC:write_projection(FLUPid, public, P1) ||
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
                      after 120*1000 ->
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
           io:format(user, "\nSET partitions = ~w.\n", [ [] ]),
           machi_partition_simulator:no_partitions(),
           [DoIt(50, 10, 100) || _ <- [1,2,3]],
           %% io:format(user, "\nSET partitions = ~w.\n", [ [] ]),
           %% machi_partition_simulator:no_partitions(),
           %% [DoIt(10, 10, 100) || _ <- [1]],

           %% machi_partition_simulator:reset_thresholds(10, 50),
           %% io:format(user, "\nLet loose the dogs of war!\n", []),
           %% DoIt(30, 0, 0),

           machi_partition_simulator:always_these_partitions(Partition),
           io:format(user, "\nSET partitions = ~w.\n", [Partition]),
           [DoIt(50, 10, 300) || _ <- [1,2,3,4] ],
           %% [DoIt(50, 10, 100) || _ <- [1,2,3,4] ],
           _PPP =
               [begin
                    {ok, PPPallPubs} = ?FLU_PC:list_all_projections(FLU,public),
                    [begin
                         {ok, Pr} = todo_why_does_this_crash_sometimes(
                                      FLUName, FLU, PPPepoch),
                         {Pr#projection_v1.epoch_number, FLUName, Pr}
                     end || PPPepoch <- PPPallPubs]
                end || {FLUName, FLU} <- Namez],
           %% io:format(user, "PPP ~p\n", [lists:sort(lists:append(_PPP))]),

           %%%%%%%% {stable,true} = {stable,private_projections_are_stable(Namez, DoIt)},
           {hosed_ok,true} = {hosed_ok,all_hosed_lists_are_identical(Namez, Partition)},
           io:format(user, "\nSweet, all_hosed are identical-or-islands-inconclusive.\n", []),
           timer:sleep(1000),
           ok
       %% end || Partition <- AllPartitionCombinations
       %% end || Partition <- [ [{a,b},{b,d},{c,b}],
       %%                       [{a,b},{b,d},{c,b}, {a,b},{b,a},{a,c},{c,a},{a,d},{d,a}],
       %%                       %% [{a,b},{b,d},{c,b}, {b,a},{a,b},{b,c},{c,b},{b,d},{d,b}],
       %%                       [{a,b},{b,d},{c,b}, {c,a},{a,c},{c,b},{b,c},{c,d},{d,c}],
       %%                       [{a,b},{b,d},{c,b}, {d,a},{a,d},{d,b},{b,d},{d,c},{c,d}] ]
       %% end || Partition <- [ [{a,b}, {b,c}],
       %%                       [{a,b}, {c,b}]  ]
       %% end || Partition <- [ [{a,b}, {b,c}]  ]  %% hosed-not-equal @ 3 FLUs
       %% end || Partition <- [ [{b,d}] ]
       %% end || Partition <- [ [{a,b}, {b,a}] ]
       %% end || Partition <- [ [{a,b}, {b,a}, {a,c},{c,a}] ]
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
       end || Partition <- [ [{c,a},{a,c}, {c,b},{b,c}, {c,d},{d,c}] ]
       %% end || Partition <- [ [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}],
       %%                       [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}],
       %%                       [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}],
       %%                       [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}],
       %%                       [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}]
       %%                       ]
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
      Report = machi_chain_manager1_test:unanimous_report(Namez),
      %% ?D(Report),

      %% Report is ordered by Epoch.  For each private projection
      %% written during any given epoch, confirm that all chain
      %% members appear in only one unique chain, i.e., the sets of
      %% unique chains are disjoint.
      true = machi_chain_manager1_test:all_reports_are_disjoint(Report),

      %% Given the report, we flip it around so that we observe the
      %% sets of chain transitions relative to each FLU.
      R_Chains = [machi_chain_manager1_test:extract_chains_relative_to_flu(
                    FLU, Report) || FLU <- All_list],
      %% ?D(R_Chains),
      R_Projs = [{FLU, [machi_chain_manager1_test:chain_to_projection(
                          FLU, Epoch, UPI, Repairing, All_list) ||
                           {Epoch, UPI, Repairing} <- E_Chains]} ||
                    {FLU, E_Chains} <- R_Chains],

      %% For each chain transition experienced by a particular FLU,
      %% confirm that each state transition is OK.
      try
          [{FLU, true} = {FLU, ?MGR:projection_transitions_are_sane(Psx, FLU)} ||
              {FLU, Psx} <- R_Projs],
          io:format(user, "\nAll sanity checks pass, hooray!\n", [])
      catch _Err:_What ->
              io:format(user, "Report ~p\n", [Report]),
              exit({line, ?LINE, _Err, _What})
      end,
      %% ?D(R_Projs),

      ok
    catch
        XX:YY ->
            io:format(user, "BUMMER ~p ~p @ ~p\n",
                      [XX, YY, erlang:get_stacktrace()]),
            exit({bummer,XX,YY})
    after
        [ok = ?MGR:stop(MgrPid) || {_, MgrPid} <- MgrNamez],
        [ok = ?FLU_PC:quit(PPid) || {_, PPid} <- Namez],
        [ok = machi_flu1:stop(FLUPid) || FLUPid <- FLU_pids],
        ok = machi_partition_simulator:stop()
    end.

todo_why_does_this_crash_sometimes(FLUName, FLU, PPPepoch) ->
    try
        {ok, _}=Res = ?FLU_PC:read_projection(FLU, public, PPPepoch),
        Res
    catch _:_ ->
            io:format(user, "QQQ Whoa, it crashed this time for ~p at epoch ~p\n",
                      [FLUName, PPPepoch]),
            timer:sleep(1000),
            ?FLU_PC:read_projection(FLU, public, PPPepoch)
    end.

private_projections_are_stable(Namez, PollFunc) ->
    Private1 = [?FLU_PC:get_latest_epoch(FLU, private) ||
                   {_Name, FLU} <- Namez],
    PollFunc(5, 1, 10),
    Private2 = [?FLU_PC:get_latest_epoch(FLU, private) ||
                   {_Name, FLU} <- Namez],
    true = (Private1 == Private2).

all_hosed_lists_are_identical(Namez, Partition0) ->
    Partition = lists:usort(Partition0),
    Ps = [element(2,?FLU_PC:read_latest_projection(FLU, private)) ||
             {_Name, FLU} <- Namez],
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
-endif. % TEST
