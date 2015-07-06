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
-ifndef(PULSE).

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

-define(DEFAULT_MGR_OPTS, [{private_write_verbose, false},
                           {active_mode,false},
                           {use_partition_simulator, true}]).

t() ->
    t(3).

t(N) ->
    t(N, ?DEFAULT_MGR_OPTS).

t(N, MgrOpts) ->
    convergence_demo_testfun(N, MgrOpts).

convergence_demo_testfun(NumFLUs, MgrOpts0) ->
    timer:sleep(100),
    %% Faster test startup, commented: io:format(user, short_doc(), []),
    %% Faster test startup, commented: timer:sleep(3000),

    TcpPort = 62877,
    ok = filelib:ensure_dir("/tmp/c/data.a"),
    FluInfo = [{a,TcpPort+0,"/tmp/c/data.a"}, {b,TcpPort+1,"/tmp/c/data.b"},
               {c,TcpPort+2,"/tmp/c/data.c"}, {d,TcpPort+3,"/tmp/c/data.d"},
               {e,TcpPort+4,"/tmp/c/data.e"}, {f,TcpPort+5,"/tmp/c/data.f"}],
    FLU_biglist = [X || {X,_,_} <- FluInfo],
    All_list = lists:sublist(FLU_biglist, NumFLUs),
    io:format(user, "\nSET # of FLUs = ~w members ~w).\n",
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
    MgrOpts = MgrOpts0 ++ ?DEFAULT_MGR_OPTS,
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
                     %% io:format(user, "\nDoIt: top\n\n", []),
                     io:format(user, "DoIt, ", []),
                     Pids = [spawn(fun() ->
                                           random:seed(now()),
                                           [begin
                                                erlang:yield(),
                                                S_max_rand = random:uniform(
                                                               S_max + 1),
                                                %% io:format(user, "{t}", []),
                                                Elapsed =
                                                    ?MGR:sleep_ranked_order(
                                                       S_min, S_max_rand,
                                                       M_name, All_list),
                                                _ = ?MGR:test_react_to_env(MMM),
                                                %% Be more unfair by not
                                                %% sleeping here.
                                                % timer:sleep(S_max - Elapsed),
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

      machi_partition_simulator:reset_thresholds(10, 50),
      io:format(user, "\nLet loose the dogs of war!\n", []),
      [DoIt(30, 0, 0) || _ <- lists:seq(1,2)],
      AllPs = make_partition_list(All_list),
      PartitionCounts = lists:zip(AllPs, lists:seq(1, length(AllPs))),
      FLUFudge = if NumFLUs < 4 ->
                         2;
                    true ->
                         8
                         %% 13
                 end,
      [begin
           machi_partition_simulator:always_these_partitions(Partition),
           io:format(user, "\nSET partitions = ~w (~w of ~w) at ~w\n",
                     [Partition, Count, length(AllPs), time()]),
           [DoIt(40, 10, 50) || _ <- lists:seq(0, trunc(NumFLUs*FLUFudge)) ],

           {stable,true} = {stable,private_projections_are_stable(Namez, DoIt)},
           io:format(user, "\nSweet, private projections are stable\n", []),
io:format(user, "Rolling sanity check ... ", []),
PrivProjs = [{Name, begin
                        {ok, Ps8} = ?FLU_PC:get_all_projections(FLU, private,
                                                                infinity),
                        Ps9 = if length(Ps8) < 5*1000 ->
                                      Ps8;
                                 true ->
                                      io:format(user, "trunc a bit... ", []),
                                      lists:nthtail(3*1000, Ps8)
                              end,
                        [P || P <- Ps9,
                              P#projection_v1.epoch_number /= 0]
                    end} || {Name, FLU} <- Namez],
try
    [{FLU, true} = {FLU, ?MGR:projection_transitions_are_sane_retrospective(Psx, FLU)} ||
        {FLU, Psx} <- PrivProjs]
catch _Err:_What ->
        io:format(user, "PrivProjs ~p\n", [PrivProjs]),
        exit({line, ?LINE, _Err, _What})
end,
io:format(user, "Yay!\n", []),
%%       ReportXXX = machi_chain_manager1_test:unanimous_report(Namez),
%%       true = machi_chain_manager1_test:all_reports_are_disjoint(ReportXXX),
%% io:format(user, "\nLast Reports: ~p\n", [lists:nthtail(length(ReportXXX)-8,ReportXXX)]),
           timer:sleep(1250),
           ok
       end || {Partition, Count} <- PartitionCounts
      ],
       %% exit(end_experiment),

      io:format(user, "\nSET partitions = []\n", []),
      io:format(user, "We should see convergence to 1 correct chain.\n", []),
      machi_partition_simulator:no_partitions(),
      [DoIt(50, 10, 50) || _ <- [1]],
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
%% io:format(user, "\nLast Reports: ~p\n", [lists:nthtail(length(Report)-8,Report)]),

      %% For each chain transition experienced by a particular FLU,
      %% confirm that each state transition is OK.
      PrivProjs = [{Name, begin
                              {ok, Ps9} = ?FLU_PC:get_all_projections(FLU,
                                                                      private),
                              [P || P <- Ps9,
                                    P#projection_v1.epoch_number /= 0]
                          end} || {Name, FLU} <- Namez],
      try
          [{FLU, true} = {FLU, ?MGR:projection_transitions_are_sane_retrospective(Psx, FLU)} ||
              {FLU, Psx} <- PrivProjs],
          io:format(user, "\nAll sanity checks pass, hooray!\n", [])
      catch _Err:_What ->
              io:format(user, "Report ~p\n", [Report]),
              io:format(user, "PrivProjs ~p\n", [PrivProjs]),
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

%% Many of the static partition lists below have been problematic at one
%% time or another.....
%%
%% Uncomment *one* of the following make_partition_list() bodies.

make_partition_list(All_list) ->
    _X_Ys1 = [[{X,Y}] || X <- All_list, Y <- All_list, X /= Y],
    _X_Ys2 = [[{X,Y}, {A,B}] || X <- All_list, Y <- All_list, X /= Y,
                                A <- All_list, B <- All_list, A /= B,
                                X /= A],
    _X_Ys3 = [[{X,Y}, {A,B}, {C,D}] || X <- All_list, Y <- All_list, X /= Y,
                                       A <- All_list, B <- All_list, A /= B,
                                       C <- All_list, D <- All_list, C /= D,
                                       X /= A, X /= C, A /= C],
    %% Concat = _X_Ys1.
    %% Concat = _X_Ys2.
    %% Concat = _X_Ys1 ++ _X_Ys2.
    %% Concat = _X_Ys3.
    %% Concat = _X_Ys1 ++ _X_Ys2 ++ _X_Ys3,
    %% random_sort(lists:usort([lists:sort(L) || L <- Concat])).

    %% [ [{a,b},{b,d},{c,b}],
    %%   [{a,b},{b,d},{c,b}, {a,b},{b,a},{a,c},{c,a},{a,d},{d,a}],
    %%   %% [{a,b},{b,d},{c,b}, {b,a},{a,b},{b,c},{c,b},{b,d},{d,b}],
    %%   [{a,b},{b,d},{c,b}, {c,a},{a,c},{c,b},{b,c},{c,d},{d,c}],
    %%   [{a,b},{b,d},{c,b}, {d,a},{a,d},{d,b},{b,d},{d,c},{c,d}] ].

    %% [ [{a,b}, {b,c}],
    %%   [{a,b}, {c,b}]  ].

    %% [ [{a,b}, {b,c}]  ].  %% hosed-not-equal @ 3 FLUs

    %% [ [{b,d}] ].

    %% [ [{a,b}], [], [{a,b}], [], [{a,b}] ].
    [
     [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
     [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
     [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
     [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
     [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
     [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], []
    ].

    %% [ [{a,b}, {b,a}] ].

    %% [ [{a,b},{b,c},{c,a}],
    %%   [{a,b}, {b,a}, {a,c},{c,a}] ].

    %% [ [{a,b}, {c,b}],
    %%   [{a,b}, {b,c}] ].

    %% [ [{a,b}, {b,c},       {c,d}],
    %%   [{a,b}, {b,c},{b,d}, {c,d}],
    %%   [{b,a}, {b,c},       {c,d}],
    %%   [{a,b}, {c,b},       {c,d}],
    %%   [{a,b}, {b,c},       {d,c}] ].

    %% [ [{a,b}, {b,c}, {c,d}, {d,e}],
    %%   [{b,a}, {b,c}, {c,d}, {d,e}],
    %%   [{a,b}, {c,b}, {c,d}, {d,e}],
    %%   [{a,b}, {b,c}, {d,c}, {d,e}],
    %%   [{a,b}, {b,c}, {c,d}, {e,d}] ].

    %% [ [{c,a}] ].  %% TODO double-check for total repair stability at SET=[]!!

    %% [ [{c,a}],
    %%   [{c,b}, {a, b}] ].

    %% [ [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}],
    %%   [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}, {b,c}],
    %%   [{a,b},{b,a}, {a,c},{c,a}, {a,d},{d,a}, {c,d}] ].

    %% [ [{a,b}, {a,b},{b,a},{a,c},{c,a},{a,d},{d,a}],
    %%   [{a,b}, {b,a},{a,b},{b,c},{c,b},{b,d},{d,b}],
    %%   [{a,b}],
    %%   [{a,b}, {c,a},{a,c},{c,b},{b,c},{c,d},{d,c}],
    %%   [{a,b}, {d,a},{a,d},{d,b},{b,d},{d,c},{c,d}] ].

todo_why_does_this_crash_sometimes(FLUName, FLU, PPPepoch) ->
    try
        {ok, _}=Res = ?FLU_PC:read_projection(FLU, public, PPPepoch),
        Res
    catch _:_ ->
            io:format(user, "QQQ Whoa, it crashed this time for ~p at epoch ~p\n",
                      [FLUName, PPPepoch]),
            timer:sleep(1000),
            exit(still_a_problem),
            ?FLU_PC:read_projection(FLU, public, PPPepoch)
    end.

private_projections_are_stable(Namez, PollFunc) ->
    Private1 = [get_latest_inner_proj_summ(FLU) || {_Name, FLU} <- Namez],
    PollFunc(5, 1, 10),
    Private2 = [get_latest_inner_proj_summ(FLU) || {_Name, FLU} <- Namez],
    if Private1 == Private2 ->
            ok;
       true ->
            io:format(user, "Oops: Private1: ~p\n", [Private1]),
            io:format(user, "Oops: Private2: ~p\n", [Private2])
    end,
    true = (Private1 == Private2).

get_latest_inner_proj_summ(FLU) ->
    {ok, Proj} = ?FLU_PC:read_latest_projection(FLU, private),
    #projection_v1{epoch_number=E, upi=UPI, repairing=Repairing, down=Down} =
        machi_chain_manager1:inner_projection_or_self(Proj),
    {E, UPI, Repairing, Down}.

random_sort(L) ->
    random:seed(now()),
    L1 = [{random:uniform(99999), X} || X <- L],
    [X || {_, X} <- lists:sort(L1)].

-endif. % !PULSE
-endif. % TEST
