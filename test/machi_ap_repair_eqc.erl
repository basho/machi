%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% EQC single-threaded and concurrent test for file operations and repair
%% under simulated network partition.

%% The main purpose is to confirm no dataloss, i.e. every chunk that
%% has been successfully written (ACK received) by append/write
%% opration will be read after partition heals.
%%
%% All updating -- append, write and trim -- operations are executed
%% through CR client, not directly by flu1 client, in order to be
%% end-to-end test (in single chain point of veiw.) There may be churn
%% for projections by simulated network partition.
%%
%% Test steps
%% 1. Setup single chain.
%% 2. Execute updating operations and simulated partition (by eqc_statem).
%%    Every updating results are recorded in ETS tables.
%% 3. When {error, timeout|partition} happens, trigger management tick for
%%    every chain manager process.
%% 4. After commands are executed, remove patition and wait for the chain
%%    without down nodes nor repairing nodes.
%% 5. Asserting written results so that each record be read from the
%%    chain and data be the same with written one.

%% Improvements to-do's
%% - Use higher concurrency, e.g. 10+
%% - Random length for binary to write
%% - Operations other than append, write, trim
%% - Use checksum instead of binary to save memory
%% - More variety for partitioning pattern: non-constant failure

-module(machi_ap_repair_eqc).

-ifdef(TEST).
-ifdef(EQC).
-compile(export_all).
-include("machi.hrl").
-include("machi_projection.hrl").
-include("machi_verbose.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

%% ETS table names
-define(WRITTEN_TAB,  written).  % Successfully written data
-define(ACCPT_TAB,    accpt).    % Errors with no harm, e.g. timeout
-define(FAILED_TAB,   failed).   % Uncategorized errors, when happenes
                                 % it should be re-categorized to accept or critical
-define(CRITICAL_TAB, critical). % Critical errors, e.g. double write to the same key

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%% EUNIT TEST DEFINITION
prop_repair_test_() ->
    {PropTO, EUnitTO} = eqc_timeout(60),
    Verbose = eqc_verbose(),
    {spawn,
     [{timeout, EUnitTO,
       ?_assertEqual(
          true,
          eqc:quickcheck(eqc:testing_time(
                           PropTO, ?QC_OUT(noshrink(prop_repair(Verbose))))))}]}.

prop_repair_par_test_() ->
    {PropTO, EUnitTO} = eqc_timeout(60),
    Verbose = eqc_verbose(),
    {spawn,
     [{timeout, EUnitTO,
       ?_assertEqual(
          true,
          eqc:quickcheck(eqc:testing_time(
                           PropTO, ?QC_OUT(noshrink(prop_repair_par(Verbose))))))}]}.

%% SHELL HELPERS
test() -> test(100).
test(N) -> test(N, true).
test(N, Verbose) -> quickcheck(numtests(N, noshrink(prop_repair_par(Verbose)))).
check() -> check(prop_repair_par(true), current_counterexample()).

-record(state, {num,             % Number of FLU servers
                seed,            % Seed for partition simulator
                verbose=false,   % Verbose output for debugging
                flu_names,       % List of FLU names
                mgr_names,       % List of chain manager names
                fc_list,         % List of FLU1 proxy clients
                cr_list}).       % List of CR clients

weight(_S, change_partition) ->  20;
weight(_S, _)                -> 100.

%% append

append_args(#state{cr_list=CRList}=S) ->
    [elements(CRList), chunk(), S].

append({_SimSelfName, C}, Bin, #state{verbose=V}=S) ->
    Prefix = <<"pre">>,
    Len = byte_size(Bin),
    Res = (catch machi_cr_client:append_chunk(C, Prefix, Bin, {sec(1), sec(1)})),
    case Res of
        {ok, {_Off, Len, _FileName}=Key} ->
            case ets:insert_new(?WRITTEN_TAB, {Key, Bin}) of
                true ->
                    [?V("<o>", []) || V],
                    ok;
                false ->
                    %% The Key is alread written, WHY!!!????
                    case ets:lookup(?WRITTEN_TAB, Key) of
                        [{Key, Bin}] ->
                            %% TODO: The identical binary is alread inserted in
                            %%  written table. Is this acceptable??? Hmm, maybe NO...
                            [?V("<dws:~w>", [Key]) || V],
                            true = ets:insert_new(?ACCPT_TAB,
                                                  {make_ref(), double_write_same, Key}),
                            {acceptable_error, doublewrite_the_same};
                        [{Key, OtherBin}] ->
                            [?V("<dwd:~w:~w>", [Key, {OtherBin, Bin}]) || V],
                            true = ets:insert_new(?CRITICAL_TAB,
                                                  {make_ref(), double_write_diff, Key}),
                            R = {critical_error,
                                 {doublewrite_diff, Key, {OtherBin, Bin}}},
                            %% TODO: when double write happens, it seems that
                            %% repair process got stack with endless loop. To
                            %% avoit it, return error here.
                            %% If this error/1 will be removed, one can possibly
                            %% know double write frequency/rate.
                            error(R)
                    end
            end;
        {error, partition} -> 
            [?V("<pt>", []) || V],
            true = ets:insert_new(?ACCPT_TAB, {make_ref(), timeout}),
            _ = tick(S),
            {acceptable_error, partition};
        {'EXIT', {timeout, _}} ->
            [?V("<to:~w:~w>", [_SimSelfName, C]) || V],
            true = ets:insert_new(?ACCPT_TAB, {make_ref(), timeout}),
            _ = tick(S),
            {acceptable_error, timeout};
        {ok, {_Off, UnexpectedLen, _FileName}=Key} ->
            [?V("<XX>", []) || V],
            true = ets:insert_new(?CRITICAL_TAB, {make_ref(), unexpected_len, Key}),
            {critical_error, {unexpected_len, Key, Len, UnexpectedLen}};
        {error, _Reason} = Error ->
            [?V("<er>", []) || V],
            true = ets:insert_new(?FAILED_TAB, {make_ref(), Error}),
            {other_error, Error};
        Other ->
            [?V("<er>", []) || V],
            true = ets:insert_new(?FAILED_TAB, {make_ref(), Other}),
            {other_error, Other}
    end.

%% change partition

change_partition_args(#state{flu_names=FLUNames}=S) ->
    %% [partition(FLUNames), S].
    [partition_sym(FLUNames), S].

change_partition(Partition,
                 #state{verbose=Verbose, flu_names=FLUNames}=S) ->
    [case Partition of
         [] -> ?V("## Turn OFF partition: ~w~n", [Partition]);
         _  -> ?V("## Turn ON  partition: ~w~n", [Partition])
     end || Verbose],
    machi_partition_simulator:always_these_partitions(Partition),
    _ = machi_partition_simulator:get(FLUNames),
    %% Don't wait for stable chain, tick will be executed on demand
    %% in append oprations
    _ = tick(S),
    ok.

%% Generators

num() ->
    choose(2, 5).
    %% return(3).

%% Generator for possibly assymmetric partition information
partition(FLUNames) ->
    frequency([{10, return([])},
               {20, non_empty(sublist(flu_ordered_pairs(FLUNames)))}]).

%% Generator for symmetric partition information
partition_sym(FLUNames) ->
    ?LET(Pairs, non_empty(sublist(flu_pairs(FLUNames))),
         lists:flatmap(fun({One, Another}) -> [{One, Another}, {Another, One}] end,
                       Pairs)).

flu_ordered_pairs(FLUNames) ->
    [{From, To} || From <- FLUNames, To <- FLUNames, From =/= To].

flu_pairs(FLUNames) ->
    [{One, Another} || One <- FLUNames, Another <- FLUNames, One > Another].

chunk() ->
    non_empty(binary(10)).

%% Property

prop_repair(Verbose) ->
    error_logger:tty(false),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),

    Seed = {1445,935441,287549},
    ?FORALL(Num, num(),
    ?FORALL(Cmds, commands(?MODULE, initial_state(Num, Seed, Verbose)),
            begin
                SetupState = setup_chain(Num, Seed, Verbose),
                {H, S1, Res} = run_commands(?MODULE, Cmds),
                %% ?V("S1=~w~n", [S1]),
                ?V("==== Start post operations, stabilize and confirm results~n", []),
                {_Res2, S2} = stabilize(commands_len(Cmds), SetupState),
                {Dataloss, Critical} = confirm_result(S2),
                pretty_commands(
                  ?MODULE, Cmds, {H, S1, Res},
                  aggregate(with_title(cmds), command_names(Cmds),
                  collect(with_title(length5), (length(Cmds) div 5) * 5,
                          {Dataloss, Critical} =:= {0, 0})))
            end)).

prop_repair_par(Verbose) ->
    error_logger:tty(false),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),

    Seed = {1445,935441,287549},
    ?FORALL(Num, num(),
    ?FORALL(Cmds,
            %% Now try-and-err'ing, how to control command length and concurrency?
            ?SUCHTHAT(Cmds0, ?SIZED(Size, resize(Size,
                      parallel_commands(?MODULE, initial_state(Num, Seed, Verbose)))),
                      commands_len(Cmds0) > 20
                      andalso
                      concurrency(Cmds0) > 2),
            begin
                CmdsLen= commands_len(Cmds),
                SetupState = setup_chain(Num, Seed, Verbose),
                {Seq, Par, Res} = run_parallel_commands(?MODULE, Cmds),
                %% ?V("Seq=~w~n", [Seq]),
                %% ?V("Par=~w~n", [Par]),
                ?V("==== Start post operations, stabilize and confirm results~n", []),
                {Dataloss, Critical} =
                    case Res of
                        ok ->
                            {_Res2, S2} = stabilize(CmdsLen, SetupState),
                            confirm_result(S2);
                        _ ->
                            ?V("Res=~w~n", [Res]),
                            {undefined, undefined}
                end,
                pretty_commands(
                  ?MODULE, Cmds, {Seq, Par, Res},
                  aggregate(with_title(cmds), command_names(Cmds),
                  collect(with_title(length5), (CmdsLen div 5) * 5,
                  collect(with_title(conc),    concurrency(Cmds),
                          {Dataloss, Critical} =:= {0, 0})))
                 )
            end)).

%% Initilization / setup

%% Fake initialization function for debugging in shell like:
%% > eqc_gen:sample(eqc_statem:commands(machi_ap_repair_eqc)).
%% but not so helpful.
initial_state() ->
    #state{cr_list=[a,b,c]}.

initial_state(Num, Seed, Verbose) ->
    AllListE = all_list_extra(Num),
    FLUNames = [P#p_srvr.name || {P, _Dir} <- AllListE],
    MgrNames = [{Name, machi_flu_psup:make_mgr_supname(Name)} || Name <- FLUNames],
    Dict = orddict:from_list([{P#p_srvr.name, P} || {P, _Dir} <- AllListE]),

    FCList = [begin
                  {ok, PCPid} = machi_proxy_flu1_client:start_link(P),
                  {Name, PCPid}
              end || {_, #p_srvr{name=Name}=P} <- Dict],
    %% CR clients are pooled, each has "name" which is interpreted "From"
    %% side of simulated partition.
    CRListCount = 10, % ad-hoc
    SimSelfNames = lists:append(lists:duplicate(CRListCount div Num +1, FLUNames)),
    CRList = [begin
                  {ok, C} = machi_cr_client:start_link(
                              [P || {_, P} <- Dict],
                              [{use_partition_simulator, true},
                               {simulator_self_name, SimSelfName},
                               {simulator_members, FLUNames}]),
                  {SimSelfName, C}
              end || SimSelfName <- SimSelfNames],
    #state{num=Num, seed=Seed, verbose=Verbose,
           flu_names=FLUNames, mgr_names=MgrNames,
           cr_list=CRList, fc_list=FCList}.

setup_chain(Num, Seed, Verbose) ->
    %% ?V("setup_chain(Num=~w, Seed=~w~nn", [Num, Seed]),
    AllListE = all_list_extra(Num),
    FLUNames = [P#p_srvr.name || {P, _Dir} <- AllListE],
    MgrNames = [{Name, machi_flu_psup:make_mgr_supname(Name)} || Name <- FLUNames],
    Dict = orddict:from_list([{P#p_srvr.name, P} || {P, _Dir} <- AllListE]),

    ok = shutdown_hard(),
    [begin
         machi_flu1_test:clean_up_data_dir(Dir),
         filelib:ensure_dir(Dir ++ "/not-used")
     end || {_P, Dir} <- AllListE],
    [catch ets:delete(T) || T <- tabs()],

    [ets:new(T, [set, public, named_table,
                 {write_concurrency, true}, {read_concurrency, true}]) ||
        T <- tabs()],
    {ok, _} = application:ensure_all_started(machi),

    SimSpec = {part_sim,
               {machi_partition_simulator, start_link, [{0,0,0}, 0, 100]},
               permanent, 500, worker, []},
    {ok, _PSimPid} = supervisor:start_child(machi_sup, SimSpec),
    ok = machi_partition_simulator:set_seed(Seed),
    _Partitions = machi_partition_simulator:get(FLUNames),

    %% Start FLUs and setup the chain
    FLUOpts = [{use_partition_simulator, true},
               %% {private_write_verbose, true},
               {active_mode, false},
               {simulate_repair, false}],
    [{ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, FLUOpts) ||
        {#p_srvr{name=Name, port=Port}, Dir} <- AllListE],
    [machi_chain_manager1:set_chain_members(MgrName, Dict) || {_, MgrName} <- MgrNames],

    State = initial_state(Num, Seed, Verbose),
    %% Don't wait for complete chain. Even partialy completed, the chain
    %% should work fine. Right?
    wait_until_stable(chain_state_all_ok(FLUNames), FLUNames, MgrNames,
                      State#state.fc_list, 20),
    State.

%% Post commands

stabilize(0, S) ->
    {ok, S};
stabilize(_CmdsLen, #state{flu_names=FLUNames, mgr_names=MgrNames, fc_list=FCList}=S) ->
    machi_partition_simulator:no_partitions(),
    wait_until_stable(chain_state_all_ok(FLUNames), FLUNames, MgrNames, FCList, 100),
    {ok, S}.

chain_state_all_ok(FLUNames) ->
    [{FLUName, {FLUNames, [], []}} || FLUName <- FLUNames].

confirm_result(#state{cr_list=[{_, C}|_]}=_S) ->
    [{written, _Written}, {accpt, Accpt},
     {failed, Failed}, {critical, Critical}] = tab_counts(),
    {OK, Dataloss} = confirm_written(C),
    ?V("  Written=~w, DATALOSS=~w, Acceptable=~w~n", [OK, Dataloss, Accpt]),
    ?V("  Failed=~w, Critical=~w~n~n", [Failed, Critical]),
    DirBase = get_dir_base(),
    Suffix = dump_file_suffix(),
    case Failed of
        0 -> ok;
        _ ->
            DumpFailed = filename:join(DirBase, "dump-failed-" ++ Suffix),
            ?V("Dump failed ETS tab to: ~w~n", [DumpFailed]),
            ets:tab2file(?FAILED_TAB, DumpFailed)
    end,
    case Critical of
        0 -> ok;
        _ ->
            DumpCritical = filename:join(DirBase, "dump-critical-" ++ Suffix),
            ?V("Dump critical ETS tab to: ~w~n", [DumpCritical]),
            ets:tab2file(?CRITICAL_TAB, DumpCritical)
    end,
    {Dataloss, Critical}.

confirm_written(C) ->
    ets:foldl(
      fun({Key, Bin}, {OK, NG}) ->
              case assert_chunk(C, Key, Bin) of
                  ok -> {OK+1, NG};
                  {error, _} -> {OK, NG+1}
              end
      end, {0, 0}, ?WRITTEN_TAB).

assert_chunk(C, {Off, Len, FileName}=Key, Bin) ->
    %% TODO: This probably a bug, read_chunk respnds with filename of `string()' type
    FileNameStr = binary_to_list(FileName),
    %% TODO : Use CSum instead of binary (after disuccsion about CSum is calmed down?)
    case (catch machi_cr_client:read_chunk(C, FileName, Off, Len, [], sec(3))) of
        {ok, {[{FileNameStr, Off, Bin, _}], []}} ->
            ok;
        {ok, Got} ->
            ?V("read_chunk got different binary for Key=~p~n", [Key]),
            ?V("    Expected: ~p~n", [{[{FileNameStr, Off, Bin, <<"CSum-NYI">>}], []}]),
            ?V("    Got:      ~p~n", [Got]),
            {error, different_binary};
        {error, Reason} ->
            ?V("read_chunk error for Key=~p: ~p~n", [Key, Reason]),
            {error, Reason};
        Other ->
            ?V("read_chunk other error for Key=~p: ~p~n", [Key, Other]),
            {error, Other}
    end.

%% Internal utilities

eqc_verbose() ->
    os:getenv("EQC_VERBOSE") =:= "true".

eqc_timeout(Default) ->
    PropTimeout = case os:getenv("EQC_TIMEOUT") of
                      false -> Default;
                      V -> list_to_integer(V)
                  end,
    {PropTimeout, PropTimeout * 300}.

%% Returns a list like
%% `[{#p_srvr{name=a, port=7501, ..}, "./eqc/data.eqc.a/"}, ...]'
all_list_extra(Num) ->
    {PortBase, DirBase} = get_port_dir_base(),
    [begin
         FLUNameStr = [$a + I - 1],
         FLUName = list_to_atom(FLUNameStr),
         MgrName = machi_flu_psup:make_mgr_supname(FLUName),
         {#p_srvr{name=FLUName, address="localhost", port=PortBase+I,
                  props=[{chmgr, MgrName}]},
          DirBase ++ "/data.eqc." ++ FLUNameStr}
     end || I <- lists:seq(1, Num)].

get_port_dir_base() ->
    I = case os:getenv("EQC_BASE_PORT") of
            false -> 0;
            V -> list_to_integer(V)
        end,
    D = get_dir_base(),
    {7400 + (I * 100), D ++ "/" ++ integer_to_list(I)}.

get_dir_base() ->
    case os:getenv("EQC_BASE_DIR") of
        false -> "./eqc";
        DD -> DD
    end.

shutdown_hard() ->
    _STOP = application:stop(machi),
    timer:sleep(100).

tick(#state{flu_names=FLUNames, mgr_names=MgrNames,
            fc_list=FCList, verbose=Verbose}) ->
    tick(FLUNames, MgrNames, FCList, Verbose).

tick(FLUNames, MgrNames, FCList, Verbose) ->
    tick(FLUNames, MgrNames, 2, 100, FCList, Verbose).

tick(FLUNames, MgrNames, Iter, SleepMax, FCList, Verbose) ->
    TickFun = tick_fun(FLUNames, MgrNames, self()),
    TickFun(Iter, 0, SleepMax),
    [?V("## Chain state after tick()=~w~n", [chain_state(FCList)]) || Verbose].

tick_fun(FLUNames, MgrNames, Parent) ->
    fun(Iters, SleepMin, SleepMax) ->
            %% ?V("^", []),
            Trigger =
                fun(FLUName, MgrName) ->
                        random:seed(now()),
                        [begin
                             erlang:yield(),
                             SleepMaxRand = random:uniform(SleepMax + 1),
                             %% io:format(user, "{t}", []),
                             Elapsed = machi_chain_manager1:sleep_ranked_order(
                                         SleepMin, SleepMaxRand,
                                         FLUName, FLUNames),
                             MgrName ! tick_check_environment,
                             %% Be more unfair by not sleeping here.
                             timer:sleep(max(SleepMax - Elapsed, 1)),
                             ok
                         end || _ <- lists:seq(1, Iters)],
                        Parent ! {done, self()}
                end,
            Pids = [{spawn(fun() -> Trigger(FLUName, MgrName) end), FLUName} ||
                       {FLUName, MgrName} <- MgrNames ],
            [receive
                 {done, ThePid} ->
                     ok
             after 120*1000 ->
                     exit({icky_timeout, M_name})
             end || {ThePid, M_name} <- Pids]
    end.

wait_until_stable(ExpectedChainState, FLUNames, MgrNames, FCList) ->
    wait_until_stable(ExpectedChainState, FLUNames, MgrNames, FCList, 20).

wait_until_stable(ExpectedChainState, FLUNames, MgrNames, FCList, Retries) ->
    TickFun = tick_fun(FLUNames, MgrNames, self()),
    wait_until_stable1(ExpectedChainState, TickFun, FCList, Retries).

wait_until_stable1(_ExpectedChainState, _TickFun, FCList, 0) ->
    ?V("  [ERROR] wait_until_stable failed.... : ~p~n", [chain_state(FCList)]),
    false;
wait_until_stable1(ExpectedChainState, TickFun, FCList, Reties) ->
    [TickFun(3, 0, 100) || _ <- lists:seq(1, 3)],
    Normalized = normalize_chain_state(chain_state(FCList)),
    case Normalized of
        ExpectedChainState ->
            ?V("  Got stable chain: ~w~n", [chain_state(FCList)]),
            true;
        _ ->
            ?V("  NOT YET stable chain: ~w~n", [chain_state(FCList)]),
            wait_until_stable1(ExpectedChainState, TickFun, FCList, Reties-1)
    end.

normalize_chain_state(ChainState) ->
    lists:usort([{FLUName,
                  {lists:usort(UPI), lists:usort(Repairing), lists:usort(Down)}} ||
                    {FLUName, {_EpochNo, UPI, Repairing, Down}} <- ChainState]).

chain_state(FCList) ->
    lists:usort(
      [case (catch machi_proxy_flu1_client:read_latest_projection(C, private, sec(5))) of
           {ok, #projection_v1{epoch_number=EpochNo, upi=UPI,
                               repairing=Repairing, down=Down}} ->
               {FLUName, {EpochNo, UPI, Repairing, Down}};
           Other ->
               {FLUName, Other}
       end || {FLUName, C} <- FCList]).

tabs() -> [?WRITTEN_TAB, ?ACCPT_TAB, ?FAILED_TAB, ?CRITICAL_TAB].

tab_counts() ->
    [{T, ets:info(T, size)} || T <- tabs()].

sec(Sec) ->
    timer:seconds(Sec).

commands_len({SeqCmds, ParCmdsList} = _Cmds) ->
    lists:sum([length(SeqCmds) | [length(P) || P <- ParCmdsList]]);
commands_len(Cmds) ->
    length(Cmds).

concurrency({_SeqCmds, ParCmdsList} = _Cmds) -> length(ParCmdsList);
concurrency(_) -> 1.

dump_file_suffix() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:local_time(),
    lists:flatten(
      io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B.000Z",
                    [Year, Month, Day, Hour, Min, Sec])).

-endif. % EQC
-endif. % TEST
