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

-define(MGR, machi_chain_manager1).

-define(D(X), io:format(user, "~s ~p\n", [??X, X])).
-define(Dw(X), io:format(user, "~s ~w\n", [??X, X])).

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

smoke0_test() ->
    machi_partition_simulator:start_link({1,2,3}, 50, 50),
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, M0} = ?MGR:start_link(a, [a,b,c], a),
    try
        pong = ?MGR:ping(M0),

        %% If/when calculate_projection_internal_old() disappears, then
        %% get rid of the comprehension below ... start/ping/stop is
        %% good enough for smoke0.
        [begin
             Proj = ?MGR:calculate_projection_internal_old(M0),
             io:format(user, "~w\n", [?MGR:make_projection_summary(Proj)])
         end || _ <- lists:seq(1,5)]
    after
        ok = ?MGR:stop(M0),
        ok = machi_flu0:stop(FLUa),
        ok = machi_partition_simulator:stop()
    end.

smoke1_test() ->
    machi_partition_simulator:start_link({1,2,3}, 100, 0),
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, FLUb} = machi_flu0:start_link(b),
    {ok, FLUc} = machi_flu0:start_link(c),
    I_represent = I_am = a,
    {ok, M0} = ?MGR:start_link(I_represent, [a,b,c], I_am),
    try
        {ok, _P1} = ?MGR:test_calc_projection(M0, false),

        _ = ?MGR:test_calc_proposed_projection(M0),
        {remote_write_results,
         [{b,ok},{c,ok}]} = ?MGR:test_write_proposed_projection(M0),
        {unanimous, P1, Extra1} = ?MGR:test_read_latest_public_projection(M0, false),

        ok
    after
        ok = ?MGR:stop(M0),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb),
        ok = machi_flu0:stop(FLUc),
        ok = machi_partition_simulator:stop()
    end.

nonunanimous_setup_and_fix_test() ->
    machi_partition_simulator:start_link({1,2,3}, 100, 0),
    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, FLUb} = machi_flu0:start_link(b),
    I_represent = I_am = a,
    {ok, Ma} = ?MGR:start_link(I_represent, [a,b], I_am),
    {ok, Mb} = ?MGR:start_link(b, [a,b], b),
    try
        {ok, P1} = ?MGR:test_calc_projection(Ma, false),

        P1a = ?MGR:update_projection_checksum(
                P1#projection{down=[b], upi=[a], dbg=[{hackhack, ?LINE}]}),
        P1b = ?MGR:update_projection_checksum(
                P1#projection{author_server=b, creation_time=now(),
                              down=[a], upi=[b], dbg=[{hackhack, ?LINE}]}),
        P1Epoch = P1#projection.epoch_number,
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
        P2 = P2pa#projection{dbg2=[]},

        %% FLUb should still be using proj #0 for its private use
        {ok, P0pb} = machi_flu0:proj_read_latest(FLUb, private),
        0 = P0pb#projection.epoch_number,

        %% Poke FLUb to react ... should be using the same private proj
        %% as FLUa.
        {now_using, _} = ?MGR:test_react_to_env(Mb),
        {ok, P2pb} = machi_flu0:proj_read_latest(FLUb, private),
        P2 = P2pb#projection{dbg2=[]},

        ok
    after
        ok = ?MGR:stop(Ma),
        ok = ?MGR:stop(Mb),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb),
        ok = machi_partition_simulator:stop()
    end.

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
    UPI_R_Sums = [{Proj#projection.upi, Proj#projection.repairing,
                   Proj#projection.epoch_csum} ||
                     {_FLUname, Proj} <- Projs,
                     is_record(Proj, projection)],
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
                                      P when is_record(P, projection) ->
                                          P#projection.epoch_csum;
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

convergence_demo_test_() ->
    {timeout, 300, fun() -> convergence_demo_test(x) end}.

convergence_demo_test(_) ->
    All_list = [a,b,c],
    machi_partition_simulator:start_link({1,22,33}, 0, 100),
    _ = machi_partition_simulator:get(All_list),

    {ok, FLUa} = machi_flu0:start_link(a),
    {ok, FLUb} = machi_flu0:start_link(b),
    {ok, FLUc} = machi_flu0:start_link(c),
    Namez = [{a, FLUa}, {b, FLUb}, {c, FLUc}],
    I_represent = I_am = a,
    MgrOpts = [private_write_verbose],
    {ok, Ma} = ?MGR:start_link(I_represent, All_list, I_am, MgrOpts),
    {ok, Mb} = ?MGR:start_link(b, All_list, b, MgrOpts),
    {ok, Mc} = ?MGR:start_link(c, All_list, c, MgrOpts),
    try
        {ok, P1} = ?MGR:test_calc_projection(Ma, false),
        P1Epoch = P1#projection.epoch_number,
        ok = machi_flu0:proj_write(FLUa, P1Epoch, public, P1),
        ok = machi_flu0:proj_write(FLUb, P1Epoch, public, P1),
        ok = machi_flu0:proj_write(FLUc, P1Epoch, public, P1),

        {now_using, XX1} = ?MGR:test_react_to_env(Ma),
        ?D(XX1),
        {now_using, _} = ?MGR:test_react_to_env(Mb),
        {now_using, _} = ?MGR:test_react_to_env(Mc),
        {QQ,QQP2,QQE2} = ?MGR:test_read_latest_public_projection(Ma, false),
        ?D(QQ),
        ?Dw(?MGR:make_projection_summary(QQP2)),
        ?D(QQE2),
        %% {unanimous,P2,E2} = test_read_latest_public_projection(Ma, false),

        machi_partition_simulator:reset_thresholds(10, 50),
        _ = machi_partition_simulator:get(All_list),

        Parent = self(),
        DoIt = fun(Iters, S_min, S_max) ->
                       Pids = [spawn(fun() ->
                                             [begin
                                                  erlang:yield(),
                                                  Elapsed =
                                                      ?MGR:sleep_ranked_order(S_min, S_max, M_name, All_list),
                                                  Res = ?MGR:test_react_to_env(MMM),
                                                  timer:sleep(S_max - Elapsed),
                                                  Res=Res %% ?D({self(), Res})
                                              end || _ <- lists:seq(1, Iters)],
                                             Parent ! done
                                     end) || {M_name, MMM} <- [{a, Ma},
                                                               {b, Mb},
                                                               {c, Mc}] ],
                       [receive
                            done ->
                                ok
                        after 995000 ->
                                exit(icky_timeout)
                        end || _ <- Pids]
               end,

        DoIt(30, 0, 0),
        io:format(user, "SET always_last_partitions ON ... we should see convergence to correct chains.\n", []),
        machi_partition_simulator:always_last_partitions(),
        [DoIt(20, 40, 400) || _ <- [1]],
        %% TODO: We should be stable now ... analyze it.

        io:format(user, "SET always_last_partitions OFF ... let loose the dogs of war!\n", []),
        machi_partition_simulator:reset_thresholds(10, 50),
        DoIt(30, 0, 0),
        io:format(user, "SET always_last_partitions ON ... we should see convergence to correct chains.\n", []),
        machi_partition_simulator:always_last_partitions(),
        [DoIt(20, 40, 400) || _ <- [1]],
        %% TODO: We should be stable now ... analyze it.

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
                {FLU, Ps} <- R_Projs]
        catch _Err:_What ->
                io:format(user, "Report ~p\n", [Report]),
                exit({line, ?LINE, _Err, _What})
        end,
        %% ?D(R_Projs),

        ok
    after
        ok = ?MGR:stop(Ma),
        ok = ?MGR:stop(Mb),
        ok = machi_flu0:stop(FLUa),
        ok = machi_flu0:stop(FLUb),
        ok = machi_partition_simulator:stop()
    end.

-endif. % not PULSE
-endif. % TEST
