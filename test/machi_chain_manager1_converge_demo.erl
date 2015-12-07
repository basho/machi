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

help() ->
    io:format("~s\n", [short_doc()]).

short_doc() ->
"
A visualization of the convergence behavior of the chain self-management
algorithm for Machi.

  1. Set up some server and chain manager pairs.
  2. Create a number of different network partition scenarios, where
     (simulated) partitions may be symmetric or asymmetric.  Then stop changing
     the partitions and keep the simulated network stable (and perhaps broken).
  3. Run a number of iterations of the algorithm in parallel by poking each
     of the manager processes on a random'ish basis.
  4. Afterward, fetch the chain transition changes made by each FLU and
     verify that no transition was unsafe.

During the iteration periods, the following is a cheatsheet for the output.
See the internal source for interpreting the rest of the output.

    'SET partitions = '

        A pair-wise list of actors which cannot send messages.  The
        list is uni-directional.  If there are three servers (a,b,c),
        and if the partitions list is '[{a,b},{b,c}]' then all
        messages from a->b and b->c will be dropped, but any other
        sender->recipient messages will be delivered successfully.

    'x uses:'

        The FLU x has made an internal state transition and is using
        this epoch's projection as operating chain configuration.  The
        rest of the line is a summary of the projection.

    'CONFIRM epoch {N}'

        This message confirms that all of the servers listed in the
        UPI and repairing lists of the projection at epoch {N} have
        agreed to use this projection because they all have written
        this projection to their respective private projection stores.
        The chain is now usable by/available to all clients.

    'Sweet, private projections are stable'

        This report announces that this iteration of the test cycle
        has passed successfully.  The report that follows briefly
        summarizes the latest private projection used by each
        participating server.  For example, when in strong consistency
        mode with 'a' as a witness and 'b' and 'c' as real servers:

        %% Legend:
        %% server name, epoch ID, UPI list, repairing list, down list, ...
        %%                         ... witness list, 'false' (a constant value)

        [{a,{{1116,<<23,143,246,55>>},[a,b],[],[c],[a],false}},
         {b,{{1116,<<23,143,246,55>>},[a,b],[],[c],[a],false}}]

        Both servers 'a' and 'b' agree on epoch 1116 with epoch ID
        {1116,<<23,143,246,55>>} where UPI=[a,b], repairing=[],
        down=[c], and witnesses=[a].

        Server 'c' is not shown because 'c' has wedged itself OOS (out
        of service) by configuring a chain length of zero.

        If no servers are listed in the report (i.e. only '[]' is
        displayed), then all servers have wedged themselves OOS, and
        the chain is unavailable.

    'DoIt,' 

        This marks a group of tick events which trigger the manager
        processes to evaluate their environment and perhaps make a
        state transition.

A long chain of 'DoIt,DoIt,DoIt,' means that the chain state has
(probably) settled to a stable configuration, which is the goal of the
algorithm.

Press control-c to interrupt the test....".

%% ' silly Emacs syntax highlighting....

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
    %% Faster test startup, commented: io:format(user, short_doc(), []),
    %% Faster test startup, commented: timer:sleep(3000),

    application:start(sasl),

    MgrOpts = MgrOpts0 ++ ?DEFAULT_MGR_OPTS,
    TcpPort = proplists:get_value(port_base, MgrOpts, 62877),
    TDir = proplists:get_value(tmp_dir, MgrOpts, "/tmp/c"),
    ok = filelib:ensure_dir(TDir ++ "/not-used"),
    _ = os:cmd("rm -rf " ++ TDir ++ "/*"),
    FluInfo = [
               {a,TcpPort+0,TDir++"/data.a"}, {b,TcpPort+1,TDir++"/data.b"},
               {c,TcpPort+2,TDir++"/data.c"}, {d,TcpPort+3,TDir++"/data.d"},
               {e,TcpPort+4,TDir++"/data.e"}, {f,TcpPort+5,TDir++"/data.f"},
               {g,TcpPort+6,TDir++"/data.g"}, {h,TcpPort+7,TDir++"/data.h"},
               {i,TcpPort+8,TDir++"/data.i"}, {j,TcpPort+9,TDir++"/data.j"},
               {k,TcpPort+10,TDir++"/data.k"}, {l,TcpPort+11,TDir++"/data.l"},
               {m,TcpPort+12,TDir++"/data.m"}, {n,TcpPort+13,TDir++"/data.n"},
               {o,TcpPort+14,TDir++"/data.o"}, {p,TcpPort+15,TDir++"/data.p"},
               {q,TcpPort+16,TDir++"/data.q"}, {r,TcpPort+17,TDir++"/data.r"}
              ],
    FLU_biglist = [X || {X,_,_} <- FluInfo],
    All_list = lists:sublist(FLU_biglist, NumFLUs),
    io:format(user, "\nSET # of FLUs = ~w members ~w).\n",
              [NumFLUs, All_list]),

    machi_partition_simulator:start_link({111,222,33}, 0, 100),
    _ = machi_partition_simulator:get(All_list),

    Ps = [#p_srvr{name=Name,address="localhost",port=Port,
                  props=[{data_dir,Dir}|MgrOpts]} ||
             {Name,Port,Dir} <- lists:sublist(FluInfo, NumFLUs)],
    {ok, SupPid} = machi_flu_sup:start_link(),
    [{ok, _} = machi_flu_psup:start_flu_package(P) || P <- Ps],
    Namez = [begin
                 {ok, PPid} = ?FLU_PC:start_link(P),
                 {Name, PPid}
             end || #p_srvr{name=Name}=P <- Ps],
    MembersDict = machi_projection:make_members_dict(Ps),
    Witnesses = proplists:get_value(witnesses, MgrOpts, []),
    CMode = case {Witnesses, proplists:get_value(consistency_mode, MgrOpts,
                                                 ap_mode)} of
                {[_|_], _}   -> cp_mode;
                {_, cp_mode} -> cp_mode;
                {_, ap_mode} -> ap_mode
            end,
    MgrNamez = [begin
                    MgrName = machi_flu_psup:make_mgr_supname(Name),
                    ok = ?MGR:set_chain_members(MgrName, ch_demo, 0, CMode,
                                                MembersDict,Witnesses),
                    {Name, MgrName}
                end || #p_srvr{name=Name} <- Ps],

    try
      [{_, Ma}|_] = MgrNamez,
      {ok, P1} = ?MGR:test_calc_projection(Ma, false),
      [_ = ?FLU_PC:write_projection(FLUPid, public, P1) ||
          {_, FLUPid} <- Namez, FLUPid /= Ma],

      machi_partition_simulator:reset_thresholds(10, 50),
      _ = machi_partition_simulator:get(All_list),

      Parent = self(),
      DoIt = fun(Iters, S_min, S_max) ->
                     %% io:format(user, "\nDoIt: top\n\n", []),
                     io:format(user, "DoIt, ", []),
                     Pids = [{spawn(fun() ->
                                           random:seed(now()),
                                           [begin
                                                erlang:yield(),
                                                perhaps_adjust_pstore_sleep(),
                                                S_max_rand = random:uniform(
                                                               S_max + 1),
                                                %% io:format(user, "{t}", []),
                                                Elapsed =
                                                    ?MGR:sleep_ranked_order(
                                                       S_min, S_max_rand,
                                                       M_name, All_list),
                                                _ = (catch ?MGR:trigger_react_to_env(MMM)),
                                                %% Be more unfair by not
                                                %% sleeping here.
                                                % timer:sleep(S_max - Elapsed),
                                                Elapsed
                                            end || _ <- lists:seq(1, Iters)],
                                           Parent ! {done, self()}
                                   end), M_name} || {M_name, MMM} <- MgrNamez ],
                     [receive
                          {done, ThePid} ->
                              ok
                      after 120*1000 ->
                              [begin
                                   case whereis(XX) of
                                       undefined -> ok;
                                       XXPid -> {_, XXbin} = process_info(XXPid, backtrace),
                                                {_, XXdict} = process_info(XXPid, dictionary),
                                                TTT = proplists:get_value(ttt, XXdict),
                                                io:format(user, "BACK ~w: ttt=~w\n~s\n", [XX, TTT, XXbin])
                                   end
                               end || XX <- [file_server_2] ++
                                          [a_chmgr,b_chmgr,c_chmgr,d_chmgr,e_chmgr,f_chmgr,g_chmgr,h_chmgr,i_chmgr,j_chmgr] ++
                                          [a_pstore,b_pstore,c_pstore,d_pstore,e_pstore,f_pstore,g_pstore,h_pstore,i_pstore,j_pstore] ++
                                          [a_fitness,b_fitness,c_fitness,d_fitness,e_fitness,f_fitness,g_fitness,h_fitness,i_fitness,j_fitness] ],
                              [begin 
                                   [begin
                                        case whereis(XX) of
                                            undefined -> ok;
                                            XXPid -> {_, XXbin} = process_info(XXPid, backtrace),
                                                     io:format(user, "BACK ~w: ~w\n~s\n", [XX, time(), XXbin])
                                        end
                                    end || XX <- [a_pstore,b_pstore,c_pstore,d_pstore,e_pstore,f_pstore,g_pstore,h_pstore,i_pstore,j_pstore] ],
                                   timer:sleep(20)
                               end || _ <- lists:seq(1,30)],
                              exit({icky_timeout, M_name})
                      end || {ThePid,M_name} <- Pids]
             end,

      %% machi_partition_simulator:reset_thresholds(10, 50),
      %% io:format(user, "\nLet loose the dogs of war!\n", []),
      %% [DoIt(20, 0, 0) || _ <- lists:seq(1,9)],
      %% %% io:format(user, "\nVariations of puppies and dogs of war!\n", []),
      %% %% [begin
      %% %%      machi_partition_simulator:reset_thresholds(90, 90),
      %% %%      DoIt(7, 0, 0),
      %% %%      machi_partition_simulator:always_these_partitions([]),
      %% %%      DoIt(7, 0, 0)
      %% %%  end || _ <- lists:seq(1, 3)],
      machi_partition_simulator:always_these_partitions([]),
      io:format(user, "\nPuppies for everyone!\n", []),
      [DoIt(20, 0, 0) || _ <- lists:seq(1,9)],

      AllPs = make_partition_list(All_list),
      PartitionCounts = lists:zip(AllPs, lists:seq(1, length(AllPs))),
      MaxIters = NumFLUs * (NumFLUs + 1) * 6,
      [begin
           machi_partition_simulator:always_these_partitions(Partition),
           io:format(user, "\n~s SET partitions = ~w (~w of ~w)\n",
                     [machi_util:pretty_time(), Partition, Count, length(AllPs)]),
           true = lists:foldl(
                    fun(_, true) ->
                            true;
                       (_, _) ->
                            %% Run a few iterations
                            [DoIt(10, 10, 50) || _ <- lists:seq(1, 6)],
                            %% If stable, return true to short circuit remaining
                            private_projections_are_stable(Namez, DoIt)
                    end, false, lists:seq(0, MaxIters)),
           io:format(user, "\n~s Sweet, private projections are stable\n", [machi_util:pretty_time()]),
           io:format(user, "\t~P\n", [get(stable), 24]),
           io:format(user, "Rolling sanity check ... ", []),
           PrivProjs = [{Name, begin
                                   {ok, Ps8} = ?FLU_PC:get_all_projections(
                                                  FLU, private, infinity),
                                   [P || P <- Ps8,
                                         P#projection_v1.epoch_number /= 0]
                               end} || {Name, FLU} <- Namez],
           try
               [{FLU, true} = {FLU, ?MGR:projection_transitions_are_sane_retrospective(Psx, FLU)} ||
                   {FLU, Psx} <- PrivProjs]
           catch
               _Err:_What when CMode == cp_mode ->
                   io:format(user, "none proj skip detected, TODO? ", []);
               _Err:_What when CMode == ap_mode ->
                   io:format(user, "PrivProjs ~p\n", [PrivProjs]),
                   exit({line, ?LINE, _Err, _What})
           end,
           io:format(user, "Yay!\n", []),
%% io:format(user, "\n\nEXITING!\n\n", []), timer:sleep(500), erlang:halt(0),
           ReportXX = machi_chain_manager1_test:unanimous_report(Namez),
           true = machi_chain_manager1_test:all_reports_are_disjoint(ReportXX),
           io:format(user, "Yay for ReportXX!\n", []),
timer:sleep(1234),

           %% TODO: static count is not sufficient.  Must not delete the last
           %%       private_proj_is_upi_unanimous projection!
           MaxFiles = 123,
           [begin
                Privs = filelib:wildcard(Dir ++ "/projection/private/*"),
                FilesToDel1 = lists:sublist(Privs,
                                            max(0, length(Privs)-MaxFiles)),
                [_ = file:delete(File) || File <- FilesToDel1],
                Pubs = filelib:wildcard(Dir ++ "/projection/public/*"),
                FilesToDel2 = lists:sublist(Pubs,
                                            max(0, length(Pubs)-MaxFiles)),
                [_ = file:delete(File) || File <- FilesToDel2],
                io:format(user, "Yay, now prune: ~w ~w, ", [length(FilesToDel1), length(FilesToDel2)])
            end || Dir <- filelib:wildcard(TDir ++ "/data*")],
           io:format(user, "\n", []),

           timer:sleep(1250),
           ok
       end || {Partition, Count} <- PartitionCounts
      ],

      io:format(user, "\nSET partitions = []\n", []),
      io:format(user, "We should see convergence to 1 correct chain.\n", []),
      machi_partition_simulator:no_partitions(),
      [DoIt(50, 10, 50) || _ <- [1,2,3]],
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
      catch
          _Err:_What when CMode == cp_mode ->
              io:format(user, "none proj skip detected, TODO? ", []);
          _Err:_What when CMode == ap_mode ->
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
        exit(SupPid, normal),
        ok = machi_partition_simulator:stop(),
        [ok = ?FLU_PC:quit(PPid) || {_, PPid} <- Namez],
        machi_util:wait_for_death(SupPid, 100)
    end.

%% Many of the static partition lists below have been problematic at one
%% time or another.....
%%
%% Uncomment *one* of the following make_partition_list() bodies.

make_partition_list(All_list) ->
    %% [ [{a,c},{b,c}],
    %%   [{a,b},{c,b}] ].

    %% Island1 = [hd(All_list), lists:last(All_list)],
    %% Island2 = All_list -- Island1,
    %% [
    %%  [{X,Y} || X <- Island1, Y <- Island2]
    %%  ++
    %%  [{X,Y} || X <- Island2, Y <- Island1]
    %% ].

    _X_Ys1 = [[{X,Y}] || X <- All_list, Y <- All_list, X /= Y],
    _X_Ys2 = [[{X,Y}, {A,B}] || X <- All_list, Y <- All_list, X /= Y,
                                A <- All_list, B <- All_list, A /= B,
                                X /= A],
    _X_Ys3 = [[{X,Y}, {A,B}, {C,D}] || X <- All_list, Y <- All_list, X /= Y,
                                       A <- All_list, B <- All_list, A /= B,
                                       C <- All_list, D <- All_list, C /= D,
                                       X /= A, X /= C, A /= C],
    %% _X_Ys4 = [[{X,Y}, {A,B}, {C,D}, {E,F}] ||
    %%                                    X <- All_list, Y <- All_list, X /= Y,
    %%                                    A <- All_list, B <- All_list, A /= B,
    %%                                    C <- All_list, D <- All_list, C /= D,
    %%                                    E <- All_list, F <- All_list, E /= F,
    %%                                    X /= A, X /= C, X /= E, A /= C, A /= E,
    %%                                    C /= E],
    %% Concat = _X_Ys1,
    %% Concat = _X_Ys2,
    %% Concat = _X_Ys1 ++ _X_Ys2,
    %% %% Concat = _X_Ys3,
    Concat = _X_Ys1 ++ _X_Ys2 ++ _X_Ys3,
    %% Concat = _X_Ys1 ++ _X_Ys2 ++ _X_Ys3 ++ _X_Ys4,
    NoPartitions = lists:duplicate(trunc(length(Concat) * 0.1), []),
    uniq_reverse(random_sort(lists:usort([lists:sort(L) || L <- Concat])
                             ++ NoPartitions)).

   %% %% for len=5 and 2 witnesses
   %%  [
   %%   [{b,c}],
   %%   [],
   %%   [{b,c}],
   %%   [{a,c},{b,c}],
   %%   [{b,c}],
   %%   [],
   %%   [{c,d}],
   %%   [],
   %%   [{d,e}],
   %%   [],
   %%   [{d,c}],
   %%   [{b,c}],
   %%   [],
   %%   [{c,e}]
   %%  ].
    %% [
    %%  [{b,c}],
    %%  [{a,c},{b,c}]
    %%  %% [{b,c}],
    %%  %% [],
    %%  %% [{c,d}],
    %%  %% [],
    %%  %% [{d,e}],
    %%  %% [],
    %%  %% [{c,e}]
    %% ].

    %% [
    %%  [{b,c}],
    %%  [{b,c},{c,d},{e,a}],
    %%  [{a,c},{a,d},{a,e},{c,a},{d,a},{e,a},{b,c},{b,d},{b,e},{b,c},{b,d},{b,e}, % iof2
    %%   {c,a},{c,b},{c,d},{c,e},{a,c},{b,c},{d,c},{e,c}, % island of 1
    %%   {d,a},{d,b},{d,c},{d,e},{a,d},{b,d},{c,d},{e,d}, % island of 1
    %%   {e,a},{e,b},{e,c},{e,d},{a,e},{b,e},{c,e},{d,e}],% island of 1
    %%  [{a,e},{b,c},{d,e}] % the stinker?
    %%  ,
    %%  [],
    %%  [{b,a},{d,e},{e,a}],
    %%  [{b,c},{c,d}],
    %%  [{a,c},{c,a},{d,b}],
    %%  [{a,e},{c,e},{e,d}],
    %%  [{a,e},{c,d},{d,b}],
    %%  [{b,e},{c,a},{e,d}],
    %%  [{b,c},{c,d},{e,a}],
    %%  [{d,e},{e,c}],
    %%  [{a,e},{b,c},{d,e}] % the stinker?
    %%  ,
    %%  [],
    %%  [{e,a},{g,d}],
    %%  [{b,f},{f,b}],
    %%  [{a,g},{c,d}]
    %% ]. % for 5 in AP, yay, working now.


    %% [ [{a,b},{b,d},{c,b}],
    %%   [{a,b},{b,d},{c,b}, {a,b},{b,a},{a,c},{c,a},{a,d},{d,a}],
    %%   %% [{a,b},{b,d},{c,b}, {b,a},{a,b},{b,c},{c,b},{b,d},{d,b}],
    %%   [{a,b},{b,d},{c,b}, {c,a},{a,c},{c,b},{b,c},{c,d},{d,c}],
    %%   [{a,b},{b,d},{c,b}, {d,a},{a,d},{d,b},{b,d},{d,c},{c,d}] ].

    %% [  [{a,b}, {b,c}],
    %%   [{a,b}, {a,c}]  ].
    %% Q = [ {X,Y} || X <- [a], Y <- [b,c,d,e,f,g,h,i,j,k,l,m,n,o,p] ],
    %% %% [ [{d,e}], Q]. %% len=7 problem: bad inner flip when ps=[] at end!
    %% [ Q, [{a,b},{c,d},{e,f}] ]. %% len=7 problem: WTF, double-check please!

                   %% len=7 problem: insane evil-near-infinite-loop sometimes

    %% [ [{a,b}], Q, [{c,d}], Q, [{d,e}], Q].

    %% [ [{a,b}, {b,c}]  ].  %% hosed-not-equal @ 3 FLUs

    %% [ [{b,d}] ].

    %% [ [{a,b}], [], [{a,b}], [], [{a,b}] ].

    %% [
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  [{b,a},{d,e}], 
    %%  [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], []
    %% ].

    %% [
    %%  [{a,b}],
    %%  [],
    %%  [{a,b}, {b,a}],
    %%  [],
    %%  [{b,c}]
    %% ].

    %% [
    %%  [{a,c},{b,a},{c,b}],
    %%  [{b,a}]
    %% ].

    %% [
    %%   [{c,b}, {c,a}],
    %%   [{b,c}, {b,a}],
    %%   [],
    %%   [{c,b}, {c,a}],
    %%   [{b,c}, {b,a}]
    %% ].

    %% [
    %%   [{a,b}], [],
    %%   [{b,a}, {b,c}], [],
    %%   [{c,b}, {c,a}, {d,c}], [],
    %%   [{c,b}, {c,a}], [],

    %%   [{b,a}, {c,a}], [],
    %%   [{a,b}, {c,b}], [],
    %%   [{b,c}, {a,c}]
    %% ].

    %% [ [{a,b},{b,c},{c,a}],
    %%   [{a,b}, {b,a}, {a,c},{c,a}] ].

    %% [ [{a,b}, {c,b}],
    %%   [{a,b}, {b,c}] ].

    %% [ [{a,b}, {b,c},       {c,d}],
    %%   [{a,b}, {b,c},{b,d}, {c,d}],
    %%   [{b,a}, {b,c},       {c,d}],
    %%   [{a,b}, {c,b},       {c,d}],
    %%   [{a,b}, {b,c},       {d,c}] ].

    %% [
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  [{a,b}], [], [{a,b}], [], [{a,b}]
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [],
    %%  %% [{b,a},{d,e}],
    %%  %% [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], [], [{a,b}], []
    %% ].
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
    FilterNoneProj = fun({_EpochID,[],[],_Dn,_W,InnerP}) -> false;
                        (_)                              -> true
                     end,
    Private1x = [{Name, get_latest_inner_proj_summ(FLU)} || {Name,FLU} <- Namez],
    Private1 = [X || X={_,Proj} <- Private1x, FilterNoneProj(Proj)],
    [PollFunc(15, 1, 10) || _ <- lists:seq(1,6)],
    Private2x = [{Name, get_latest_inner_proj_summ(FLU)} || {Name,FLU} <- Namez],
    Private2 = [X || X={_,Proj} <- Private2x, FilterNoneProj(Proj)],
    %% Is = [Inner_p || {_,_,_,_,Inner_p} <- Private1],
    put(stable, lists:sort(Private1)),
    %% We want either all true or all false (inner or not) ... except
    %% that it isn't quite that simple.  I've now witnessed a case
    %% where the projections are stable but not everyone is
    %% unanimously outer or unanimously inner!
    %% Old partitions: [{a,b},{b,c},{c,a}]
    %%                 result: all 3 had inner proj of [self]
    %% New partitions: [{b,a},{c,b}]
    %%                 Priv1 [{342,[c,a],[],[b],[],false},
    %%                       {326,[b],[],[a,c],[],true},
    %%                       {342,[c,a],[],[b],[],false}]
    %%                 ... and it stays completely stable with these epoch #s.
    %%
    %% So, instead, if inner/outer status isn't unanimous, then we
    %% should check to see if the sets of unique UPIs are disjoint.
    %%
    FLUs = [FLU || {FLU,_Pid} <- Namez],
    U_UPI_Rs = lists:usort([UPI++Rep ||
                           {_Nm,{_EpochID,UPI,Rep,_Dn,_W,InnerP}} <- Private2]),
    FLU_uses = [{Name, EpochID} ||
                   {Name,{EpochID,_UPI,Rep,_Dn,_W,InnerP}} <- Private2],
    Witnesses = hd([Ws ||
                   {_Name,{_EpochID,_UPI,Rep,_Dn,Ws,InnerP}} <- Private2x]),
    HaveWitnesses_p = Witnesses /= [],
    CMode = if HaveWitnesses_p -> cp_mode;
               true            -> ap_mode
            end,
    Unanimous_with_all_peers_p =
        lists:all(fun({FLU, UsesEpochID}) ->
                   WhoInEpochID = [Name ||
                                  {Name,{EpochID,_UPI,_Rep,_Dn,_W,I_}}<-Private2,
                                  EpochID == UsesEpochID],
                   WhoInEpochID_s = ordsets:from_list(WhoInEpochID),
                   UPI_R_versions = [UPI++Rep ||
                               {_Name,{EpochID,UPI,Rep,_Dn,_W,I_}}<-Private2,
                               EpochID == UsesEpochID],
                   UPI_R_vers_s = ordsets:from_list(hd(UPI_R_versions)),
                   UPI_R_versions == [ [] ] % This FLU in minority partition
                   orelse
                   (length(lists:usort(UPI_R_versions)) == 1
                    andalso
                    (ordsets:is_subset(UPI_R_vers_s, WhoInEpochID_s) orelse
                     (CMode == cp_mode andalso
                     ordsets:is_disjoint(UPI_R_vers_s, WhoInEpochID_s))))
                  end, FLU_uses),
    Flat_U_UPI_Rs = lists:flatten(U_UPI_Rs),
    %% Pubs = [begin
    %%             {ok, P} = ?FLU_PC:read_latest_projection(FLU, public),
    %%             {Name, P#projection_v1.epoch_number}
    %%         end || {Name, FLU} <- Namez],

    %% In AP mode, if not disjoint, then a FLU will appear twice in
    %% flattented U_UPIs.
    AP_mode_disjoint_test_p =
        if CMode == cp_mode ->
                true;
           CMode == ap_mode ->
                lists:sort(Flat_U_UPI_Rs) == lists:usort(Flat_U_UPI_Rs)
        end,

    CP_mode_agree_test_p =
        if CMode == cp_mode ->
                FullMajority = (length(Namez) div 2) + 1,
                EpochIDs = lists:sort(
                             [EpochID || {_Name,{EpochID,_UPI,_Rep,_Dn,_W,I_}}<-Private2]),
                case lists:reverse(lists:sort(uniq_c(EpochIDs))) of
                    [{Count,EpochID}|_] when Count >= FullMajority ->
                        [{UPI, Rep}] = lists:usort(
                          [{_UPI,_Rep} || {_Name,{EpochIDx,_UPI,_Rep,_Dn,_W,I_}}<-Private2,
                                          EpochIDx == EpochID]),
                        ExpectedFLUs = lists:sort(UPI ++ Rep),
                        UsingFLUs = lists:sort(
                          [Name || {Name,{EpochIDx,_UPI,_Rep,_Dn,_W,I_}}<-Private2,
                                   EpochIDx == EpochID]),
                        io:format(user, "Priv2: EID ~W e ~w u ~w\n", [EpochID, 7, ExpectedFLUs, UsingFLUs]),
                        ordsets:is_subset(ordsets:from_list(ExpectedFLUs),
                                          ordsets:from_list(UsingFLUs));
                    [{1=_Count,_EpochID}|_] ->
                        %% Our list is sorted & reversed, so 1=_Count
                        %% is biggest.  If a majority is using the none proj,
                        %% then we're OK.
                        Private2None = [X || {_,{_,[],[],_,_,_}}=X <- Private2],
                        length(Private2None) >= FullMajority;
                    [] when Private2 == [], Private2x /= [] ->
                        %% Everyone is using none proj, chain is unavailable.
                        true;
                    Else ->
                        %% This is bad: we have a count that's less than
                        %% FullMajority but greater than 1.
                        false
                end;
           CMode == ap_mode ->
                true
        end,

    %% io:format(user, "\nPriv1 ~p\nPriv2 ~p\n1==2 ~w ap_disjoint ~w u_all_peers ~w cp_mode_agree ~w\n", [lists:sort(Private1), lists:sort(Private2), Private1 == Private2, AP_mode_disjoint_test_p, Unanimous_with_all_peers_p, CP_mode_agree_test_p]),
    Private1 == Private2 andalso
        AP_mode_disjoint_test_p andalso
        (
         %% Another property that we want is that for each participant
         %% X mentioned in a UPI or Repairing list of some epoch E that
         %% X is using the same epoch E.
         %%
         %% It's possible (in theory) for humming consensus to agree on
         %% the membership of UPI+Repairing but arrive those lists at
         %% different epoch numbers.  Machi chain replication won't
         %% work in that case: all participants need to be using the
         %% same epoch (and csum)!
         (CMode == ap_mode andalso Unanimous_with_all_peers_p)
         orelse
         (CMode == cp_mode andalso CP_mode_agree_test_p)
        ).

get_latest_inner_proj_summ(FLU) ->
    {ok, Proj} = ?FLU_PC:read_latest_projection(FLU, private),
    #projection_v1{epoch_number=E, epoch_csum= <<CSum4:4/binary, _/binary>>,
                   upi=UPI, repairing=Repairing,
                   witnesses=Witnesses, down=Down} = Proj,
    Inner_p = false,
    EpochID = {E, CSum4},
    {EpochID, UPI, Repairing, Down, Witnesses, Inner_p}.

uniq_reverse(L) ->
    uniq_reverse(L, []).

uniq_reverse([], Acc) ->
    Acc;
uniq_reverse([H|T], []) ->
    uniq_reverse(T, [H]);
uniq_reverse([Same|T], [Same|_]=Acc) ->
    uniq_reverse(T, Acc);
uniq_reverse([H|T], Acc) ->
    uniq_reverse(T, [H|Acc]).

random_sort(L) ->
    random:seed(now()),
    L1 = [{random:uniform(), X} || X <- L],
    [X || {_, X} <- lists:sort(L1)].

foo(NumFLUs, MgrOpts0) ->
    timer:sleep(100),
    %% Faster test startup, commented: io:format(user, short_doc(), []),
    %% Faster test startup, commented: timer:sleep(3000),

    TcpPort = 62877,
    ok = filelib:ensure_dir("/tmp/c/not-used"),
    FluInfo = [
               {a,TcpPort+0,"/tmp/c/data.a"}, {b,TcpPort+1,"/tmp/c/data.b"},
               {c,TcpPort+2,"/tmp/c/data.c"}, {d,TcpPort+3,"/tmp/c/data.d"},
               {e,TcpPort+4,"/tmp/c/data.e"}, {f,TcpPort+5,"/tmp/c/data.f"},
               {g,TcpPort+6,"/tmp/c/data.g"}, {h,TcpPort+7,"/tmp/c/data.h"},
               {i,TcpPort+8,"/tmp/c/data.i"}, {j,TcpPort+9,"/tmp/c/data.j"},
               {k,TcpPort+10,"/tmp/c/data.k"}, {l,TcpPort+11,"/tmp/c/data.l"},
               {m,TcpPort+12,"/tmp/c/data.m"}, {n,TcpPort+13,"/tmp/c/data.n"},
               {o,TcpPort+14,"/tmp/c/data.o"}, {p,TcpPort+15,"/tmp/c/data.p"},
               {q,TcpPort+16,"/tmp/c/data.q"}, {r,TcpPort+17,"/tmp/c/data.r"}
              ],
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
                                                _ = ?MGR:trigger_react_to_env(MMM),
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

      %% machi_partition_simulator:reset_thresholds(10, 50),
      %% io:format(user, "\nLet loose the dogs of war!\n", []),
      machi_partition_simulator:always_these_partitions([]),
      io:format(user, "\nPuppies for everyone!\n", []),
      [DoIt(30, 0, 0) || _ <- lists:seq(1,5)],

      DoIt
    catch XXX:YYY ->
            {XXX,YYY}
    end.

uniq_c(L) ->
    uniq_c(L, 0, unused).

uniq_c([], 0, _Last) ->
    [];
uniq_c([], Count, Last) ->
    [{Count, Last}];
uniq_c([H|T], 0, _Last) ->
    uniq_c(T, 1, H);
uniq_c([H|T], Count, H) ->
    uniq_c(T, Count+1, H);
uniq_c([H|T], Count, Last) ->
    [{Count, Last}|uniq_c(T, 1, H)].

perhaps_adjust_pstore_sleep() ->
    try
        {ok, Bin} = file:read_file("/tmp/pstore_sleep_msec"),
        {MSec,_} = string:to_integer(binary_to_list(Bin)),
        ets:insert(?TEST_ETS_TABLE, {projection_store_sleep_time, MSec})
    catch _:_ ->
            ok
    end.


%%       MaxIters = NumFLUs * (NumFLUs + 1) * 6,
%%       Stable = fun(S_Namez) ->
%%            true = lists:foldl(
%%                     fun(_, true) ->
%%                             true;
%%                        (_, _) ->
%%                             %% Run a few iterations
%%                             [DoIt(10, 10, 50) || _ <- lists:seq(1, 6)],
%%                             %% If stable, return true to short circuit remaining
%%                             private_projections_are_stable(S_Namez, DoIt)
%%                     end, false, lists:seq(0, MaxIters))
%%                end,

%%       %% Part_b = [{a,b},{c,d}],
%%       Part_b = [{c,d}],
%%       %% Part_b = [{X,Y} || {X,_} <- Namez, {Y,_} <- Namez, X == b orelse Y == b],
%%       %% Part_d = [{X,Y} || {X,_} <- Namez, {Y,_} <- Namez, X == d orelse Y == d],

%%       %% machi_partition_simulator:always_these_partitions(Part_b),
%%       %% io:format(user, "\nSET partitions = ~w at ~w\n", [Part_b, time()]),
%%       %% true = Stable(Namez), io:format(user, "\nSweet, private projections are stable\n", []), io:format(user, "\t~P\n", [get(stable), 14]), (fun() -> ReportXX = machi_chain_manager1_test:unanimous_report(Namez), true = machi_chain_manager1_test:all_reports_are_disjoint(ReportXX), io:format(user, "Yay for ReportXX!\n", []) end)(),

%%       %% Part_bd = Part_b ++ Part_d,
%%       %% machi_partition_simulator:always_these_partitions(Part_bd),
%%       %% io:format(user, "\nSET partitions = ~w at ~w\n", [Part_bd, time()]),
%%       %% true = Stable(Namez), io:format(user, "\nSweet, private projections are stable\n", []), io:format(user, "\t~P\n", [get(stable), 14]), (fun() -> ReportXX = machi_chain_manager1_test:unanimous_report(Namez), true = machi_chain_manager1_test:all_reports_are_disjoint(ReportXX), io:format(user, "Yay for ReportXX!\n", []) end)(),

%%       os:cmd("rm /tmp/signal"),
%%       Part_b_partial_d = Part_b ++ [{e,f}],
%%       %% Part_b_partial_d = [{a,b}, {b,c}, {c,d}, {d,e}],
%%       %% Part_b_partial_d = [{a,b}, {b,d}, {d,e}],
%%       machi_partition_simulator:always_these_partitions(Part_b_partial_d),
%%       io:format(user, "\nSET partitions = ~w at ~w\n", [Part_b_partial_d, time()]),
%%       %% Only_ab_namez = [T || T={Name, _} <- Namez, lists:member(Name, [a,b])],
%%       true = Stable(Namez), io:format(user, "\nSweet, private projections are stable\n", []), io:format(user, "\t~P\n", [get(stable), 14]), (fun() -> ReportXX = machi_chain_manager1_test:unanimous_report(Namez), true = machi_chain_manager1_test:all_reports_are_disjoint(ReportXX), io:format(user, "Yay for ReportXX!\n", []) end)(),

%%       machi_partition_simulator:always_these_partitions([{b,c}]),
%%       io:format(user, "\nSET partitions = ~w at ~w\n", [[{b,c}], time()]),
%%       %% Only_ab_namez = [T || T={Name, _} <- Namez, lists:member(Name, [a,b])],
%%       [true = Stable(Namez) || _ <- [1,2,3] ], io:format(user, "\nSweet, private projections are stable\n", []), io:format(user, "\t~P\n", [get(stable), 14]), (fun() -> ReportXX = machi_chain_manager1_test:unanimous_report(Namez), true = machi_chain_manager1_test:all_reports_are_disjoint(ReportXX), io:format(user, "Yay for ReportXX!\n", []) end)(),

%% %% [begin
%% %%      QQQ = lists:sublist(Part_b_partial_d, NNN),
%% %%      machi_partition_simulator:always_these_partitions(QQQ),
%% %%      io:format(user, "\nSET partitions = ~w at ~w\n", [QQQ, time()]),
%% %%      %% Only_ab_namez = [T || T={Name, _} <- Namez, lists:member(Name, [a,b])],
%% %%      true = Stable(Namez), io:format(user, "\nSweet, private projections are stable\n", []), io:format(user, "\t~P\n", [get(stable), 14]), (fun() -> ReportXX = machi_chain_manager1_test:unanimous_report(Namez), true = machi_chain_manager1_test:all_reports_are_disjoint(ReportXX), io:format(user, "Yay for ReportXX!\n", []) end)()
%% %% end || NNN <- lists:seq(1, length(Part_b_partial_d))],

%%       io:format(user, "\nSET partitions = []\n", []),
%%       io:format(user, "We should see convergence to 1 correct chain.\n", []),
%%       machi_partition_simulator:no_partitions(),
%%       [DoIt(50, 10, 50) || _ <- [1,2,3]],
%%       true = private_projections_are_stable(Namez, DoIt),
%%       io:format(user, "~s\n", [os:cmd("date")]),

%%       %% We are stable now ... analyze it.

%%       %% Create a report where at least one FLU has written a
%%       %% private projection.
%%       Report = machi_chain_manager1_test:unanimous_report(Namez),
%%       %% ?D(Report),

%%       %% Report is ordered by Epoch.  For each private projection
%%       %% written during any given epoch, confirm that all chain
%%       %% members appear in only one unique chain, i.e., the sets of
%%       %% unique chains are disjoint.
%%       true = machi_chain_manager1_test:all_reports_are_disjoint(Report),
%%       %% io:format(user, "\nLast Reports: ~p\n", [lists:nthtail(length(Report)-8,Report)]),

%%       %% For each chain transition experienced by a particular FLU,
%%       %% confirm that each state transition is OK.
%%       PrivProjs = [{Name, begin
%%                               {ok, Ps9} = ?FLU_PC:get_all_projections(FLU,
%%                                                                       private),
%%                               [P || P <- Ps9,
%%                                     P#projection_v1.epoch_number /= 0]
%%                           end} || {Name, FLU} <- Namez],
%%       try
%%           [{FLU, true} = {FLU, ?MGR:projection_transitions_are_sane_retrospective(Psx, FLU)} ||
%%               {FLU, Psx} <- PrivProjs],
%%           io:format(user, "\nAll sanity checks pass, hooray!\n", [])
%%       catch _Err:_What ->
%%               io:format(user, "Report ~p\n", [Report]),
%%               io:format(user, "PrivProjs ~p\n", [PrivProjs]),
%%               exit({line, ?LINE, _Err, _What})
%%       end,
%%       %% ?D(R_Projs),

%%       ok
%%     catch
%%         XX:YY ->
%%             io:format(user, "BUMMER ~p ~p @ ~p\n",
%%                       [XX, YY, erlang:get_stacktrace()]),
%%             exit({bummer,XX,YY})
%%     after
%%         [ok = ?MGR:stop(MgrPid) || {_, MgrPid} <- MgrNamez],
%%         [ok = ?FLU_PC:quit(PPid) || {_, PPid} <- Namez],
%%         [ok = machi_flu1:stop(FLUPid) || FLUPid <- FLU_pids],
%%         ok = machi_partition_simulator:stop()
%%     end.

-endif. % !PULSE
-endif. % TEST

