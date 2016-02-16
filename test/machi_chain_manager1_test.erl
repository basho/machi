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

%% @doc Create a summary report of all of the *private* projections of
%%      each of the FLUs in the chain, and create a summary for each
%%      epoch number.
%%
%% Report format: list({EpochNumber:non_neg_integer(), Report:rpt()})
%%        rpt(): {'ok_disjoint', unique_upi_repair_lists()} |
%%               {'bummer_NOT_DISJOINT', {flat(), summaries()}
%% unique_upi_repair_lists(): list(upi_and_repair_lists_concatenated())
%% flat(): debugging term; any duplicate in this list is an invalid FLU.
%% summaries(): list({FLU, ProjectionSummary:string() | 'not_in_this_epoch'})
%%
%% Example:
%%
%% [{1,{ok_disjoint,[[a,b,c]]}},
%%  {4,{ok_disjoint,[[a,b,c]]}},
%%  {6,{ok_disjoint,[[a,b,c]]}},
%%  {16,{ok_disjoint,[[a,b,c]]}},
%%  {22,{ok_disjoint,[[b]]}},
%%  {1174,
%%   {bummer_NOT_DISJOINT,{[a,a,b],
%%                         [{a,"[{epoch,1174},{author,a},{upi,[a]},{repair,[]},{down,[b]},{d,[{ps,[{a,b},{b,a}]},{nodes_up,[a]}]},{d2,[]}]"},
%%                          {b,"[{epoch,1174},{author,b},{upi,[b]},{repair,[a]},{down,[]},{d,[{ps,[]},{nodes_up,[a,b]}]},{d2,[]}]"}]}}},
%%  ...]

unanimous_report(Namez) ->
    UniquePrivateEs =
        lists:usort(lists:flatten(
                      [element(2, ?FLU_PC:list_all_projections(FLU, private)) ||
                          {_FLUName, FLU} <- Namez])),
    [{Epoch, unanimous_report(Epoch, Namez)} || Epoch <- UniquePrivateEs,
                                                Epoch /= 0].

unanimous_report(Epoch, Namez) ->
    FLU_Projs = [{FLUName,
                  case ?FLU_PC:read_projection(FLU, private, Epoch) of
                      {ok, T} ->
                          T;
                      _Else ->
                          not_in_this_epoch
                  end} || {FLUName, FLU} <- Namez],
    unanimous_report2(FLU_Projs).

unanimous_report2(FLU_Projs) ->
    ProjsSumms = [{FLU, if is_tuple(P) ->
                                Summ = machi_projection:make_summary(P),
                                lists:flatten(io_lib:format("~w", [Summ]));
                           is_atom(P) ->
                                P
                        end} || {FLU, P} <- FLU_Projs],
    UPI_R_Sums = [{Proj#projection_v1.upi, Proj#projection_v1.repairing,
                   Proj#projection_v1.epoch_csum} ||
                     {_FLUname, Proj} <- FLU_Projs,
                     is_record(Proj, projection_v1)],
    UniqueUPIs = lists:usort([UPI || {UPI, _Repairing, _CSum} <- UPI_R_Sums]),
    if length(UniqueUPIs) =< 1 ->
            {ok_disjoint, UniqueUPIs};
        true ->
            Flat = lists:flatten(UniqueUPIs),
            case lists:usort(Flat) == lists:sort(Flat) of
                true ->
                    {ok_disjoint, UniqueUPIs};
                false ->
                    {bummer_NOT_DISJOINT, {lists:sort(Flat), ProjsSumms}}
            end
    end.

all_reports_are_disjoint(Report) ->
    case [X || {_Epoch, Tuple}=X <- Report,
               element(1, Tuple) /= ok_disjoint] of
        [] ->
            true;
        Else ->
            Else
    end.

-ifndef(PULSE).

simple_chain_state_transition_is_sane_test_() ->
    {timeout, 60, fun() -> simple_chain_state_transition_is_sane_test2() end}.

simple_chain_state_transition_is_sane_test2() ->
    %% All: A list of all FLUS for a particular test
    %% UPI1: some combination of All that represents UPI1
    %% Repair1: Some combination of (All -- UP1) that represents Repairing1
    %% ... then we test check_simple_chain_state_transition_is_sane() with all
    %% possible UPI1 and Repair1.
    [true = check_simple_chain_state_transition_is_sane(UPI1, Repair1) ||
        %% The five elements below runs on my MacBook Pro in about 4.8 seconds
        %% All <- [ [a], [a,b], [a,b,c], [a,b,c,d], [a,b,c,d,e] ],
        %% For elements on the same MBP is about 0.15 seconds.
        All <- [ [a], [a,b], [a,b,c], [a,b,c,d] ],
        UPI1 <- machi_util:combinations(All),
        Repair1 <- machi_util:combinations(All -- UPI1)].

%% Given a UPI1 and Repair1 list, we calculate all possible good UPI2
%% lists.  For all good {UPI1, Repair1} -> UPI2 transitions, then the
%% simple_chain_state_transition_is_sane() function must be true.  For
%% all other UPI2 transitions, simple_chain_state_transition_is_sane()
%% must be false.
%%
%% We include adding an extra possible participant, 'bogus', to the
%% list of all possible UPI2 transitions, just to demonstrate that
%% adding an extra element/participant/thingie is never sane.

check_simple_chain_state_transition_is_sane([], []) ->
    true;
check_simple_chain_state_transition_is_sane(UPI1, Repair1) ->
    Good_UPI2s = [ X ++ Y || X <- machi_util:ordered_combinations(UPI1),
                             Y <- machi_util:ordered_combinations(Repair1)],
    All_UPI2s = machi_util:combinations(lists:usort(UPI1 ++ Repair1) ++
                                            [bogus]),

    [true = ?MGR:simple_chain_state_transition_is_sane(UPI1, Repair1, UPI2) ||
        UPI2 <- Good_UPI2s],
    [false = ?MGR:simple_chain_state_transition_is_sane(UPI1, Repair1, UPI2) ||
        UPI2 <- (All_UPI2s -- Good_UPI2s)],

    true.

-ifdef(EQC).

%% This QuickCheck property is crippled: because the old chain state
%% transition check, chain_mgr_legacy:projection_transition_is_sane(),
%% is so buggy and the new check is (apparently) so much better, I
%% have changed the ?WHENFAIL() criteria to check for either agreement
%% _or_ a case where the legacy check says true but the new check says
%% false.
%%
%% On my MacBook Pro, less than 1000 tests are required to find at
%% least one problem case for the legacy check that the new check is
%% correct for.  Running for two seconds can do about 3,500 test
%% cases.

compare_eqc_setup_test_() ->
    %% Silly QuickCheck can take a long time to start up, check its
    %% license, etcetc.
    %%   machi_chain_manager1_test: compare_eqc_setup_test...[1.788 s] ok
    {timeout, 30,
     fun() -> eqc:quickcheck(eqc:testing_time(0.1, true)) end}.

-define(COMPARE_TIMEOUT, 1.2).
%% -define(COMPARE_TIMEOUT, 4.8).

compare_legacy_with_v2_chain_transition_check1_test() ->
    eqc:quickcheck(
      ?QC_OUT(
        eqc:testing_time(
          ?COMPARE_TIMEOUT,
          prop_compare_legacy_with_v2_chain_transition_check(primitive)))).

compare_legacy_with_v2_chain_transition_check2_test() ->
    eqc:quickcheck(
      ?QC_OUT(
        eqc:testing_time(
          ?COMPARE_TIMEOUT,
          prop_compare_legacy_with_v2_chain_transition_check(primitive)))).

prop_compare_legacy_with_v2_chain_transition_check() ->
    prop_compare_legacy_with_v2_chain_transition_check(primitive).

prop_compare_legacy_with_v2_chain_transition_check(Style) ->
    %% ?FORALL(All, nonempty(list([a,b,c,d,e])),
    ?FORALL(All, non_empty(some([a,b,c,d])),
    ?FORALL({Author1, UPI1, Repair1x, Author2, UPI2, Repair2x},
         {elements(All),some(All),some(All),elements(All),some(All),some(All)},
    ?IMPLIES(length(lists:usort(UPI1 ++ Repair1x)) > 0 andalso
             length(lists:usort(UPI2 ++ Repair2x)) > 0,
    begin
        MembersDict = orddict:from_list([{X, #p_srvr{name=X}} || X <- All]),
        Repair1 = Repair1x -- UPI1,
        Down1 = All -- (UPI1 ++ Repair1),
        Repair2 = Repair2x -- UPI2,
        Down2 = All -- (UPI2 ++ Repair2),
        P1 = machi_projection:new(1, Author1, MembersDict,
                                  Down1, UPI1, Repair1, []),
        P2 = machi_projection:new(2, Author2, MembersDict,
                                  Down2, UPI2, Repair2, []),
        Old_res = chain_mgr_legacy:projection_transition_is_sane(
                       P1, P2, Author1, false),
        Old_p = case Old_res of true -> true;
                                _    -> false
                end,
        case Style of
            primitive ->
                New_res = ?MGR:chain_state_transition_is_sane(
                             Author1, UPI1, Repair1, Author2, UPI2, Author2),
                New_p = case New_res of true -> true;
                                        _    -> false
                        end;
            whole ->
                New_res = machi_chain_manager1:projection_transition_is_sane(
                            P1, P2, Author1, false),
                New_p = case New_res of true -> true;
                                        _    -> false
                        end
        end,
        (catch ets:insert(count,
                    {{Author1, UPI1, Repair1, Author2, UPI2, Repair2}, true})),
        ?WHENFAIL(io:format(user,
                         "Old_res: ~p/~p  (~p)\nNew_res: ~p/~p (why line ~P)\n",
                         [Old_p, Old_res, catch get(why1),
                          New_p, New_res, catch get(why2), 30]),
                  %% Old_p == New_p)
                  Old_p == New_p orelse (Old_p == true andalso New_p == false))
    end))).

some(L) ->
    ?LET(L2, list(oneof(L)),
         dedupe(L2)).

dedupe(L) ->
    dedupe(L, []).

dedupe([H|T], Seen) ->
    case lists:member(H, Seen) of
        false ->
            [H|dedupe(T, [H|Seen])];
        true ->
            dedupe(T, Seen)
    end;
dedupe([], _) ->
    [].

make_prop_ets() ->
    ets:new(count, [named_table, set, public]).

-endif. % EQC

make_advance_fun(FitList, FLUList, MgrList, Num) ->
    fun() ->
            [begin
                 [catch machi_fitness:trigger_early_adjustment(Fit, Tgt) ||
                     Fit <- FitList,
                     Tgt <- FLUList ],
                 [catch ?MGR:trigger_react_to_env(Mgr) || Mgr <- MgrList],
                 ok
             end || _ <- lists:seq(1, Num)]
    end.

smoke0_test() ->
    TcpPort = 6623,
    {[Pa], [M0], _Dirs} = machi_test_util:start_flu_packages(
                            1, TcpPort, "./data.", []),
    {ok, FLUaP} = ?FLU_PC:start_link(Pa),
    try
        pong = ?MGR:ping(M0)
    after
        ok = ?FLU_PC:quit(FLUaP),
        machi_test_util:stop_flu_packages()
    end.

smoke1_test_() ->
    {timeout, 1*60, fun() -> smoke1_test2() end}.

smoke1_test2() ->
    TcpPort = 62777,
    MgrOpts = [{active_mode,false}],
    try
        {Ps, MgrNames, _Dirs} = machi_test_util:start_flu_packages(
                                  3, TcpPort, "./data.", MgrOpts),
        MembersDict = machi_projection:make_members_dict(Ps),
        [machi_chain_manager1:set_chain_members(M, MembersDict) || M <- MgrNames],
        Ma = hd(MgrNames),

        {ok, P1} = ?MGR:test_calc_projection(Ma, false),
        % DERP! Check for race with manager's proxy vs. proj listener
        ok = lists:foldl(
                    fun(_, {_,{true,[{c,ok},{b,ok},{a,ok}]}}) ->
                            ok;             % Short-circuit remaining attempts
                       (_, ok) ->
                            ok;             % Skip remaining!
                       (_, _Else) ->
                            timer:sleep(10),
                            ?MGR:test_write_public_projection(Ma, P1)
                    end, not_ok, lists:seq(1, 1000)),
        %% Writing the exact same projection multiple times returns ok:
        %% no change!
        {_,{true,[{c,ok},{b,ok},{a,ok}]}} =  ?MGR:test_write_public_projection(Ma, P1),
        {unanimous, P1, Extra1} = ?MGR:test_read_latest_public_projection(Ma, false),

        ok
    after
        machi_test_util:stop_flu_packages()
    end.

nonunanimous_setup_and_fix_test_() ->
    os:cmd("rm -f /tmp/moomoo.*"),
    {timeout, 1*60, fun() -> nonunanimous_setup_and_fix_test2() end}.

nonunanimous_setup_and_fix_test2() ->
    TcpPort = 62877,
    MgrOpts = [{active_mode,false}],
    {Ps, [Ma,Mb,Mc], Dirs} = machi_test_util:start_flu_packages(
                             3, TcpPort, "./data.", MgrOpts),
    MembersDict = machi_projection:make_members_dict(Ps),
    ChainName = my_little_chain,
    [machi_chain_manager1:set_chain_members(M, ChainName, 0, ap_mode,
                                            MembersDict, []) || M <- [Ma, Mb]],

    [Proxy_a, Proxy_b, Proxy_c] = Proxies =
        [element(2, ?FLU_PC:start_link(P)) || P <- Ps],

    try
        {ok, P1} = ?MGR:test_calc_projection(Ma, false),

        P1a = machi_projection:update_checksum(
                 P1#projection_v1{down=[b], upi=[a], dbg=[{hackhack, ?LINE}]}),
        P1b = machi_projection:update_checksum(
                 P1#projection_v1{author_server=b, creation_time=now(),
                                  down=[a], upi=[b], dbg=[{hackhack, ?LINE}]}),
        %% Scribble different projections
        ok = ?FLU_PC:write_projection(Proxy_a, public, P1a),
        ok = ?FLU_PC:write_projection(Proxy_b, public, P1b),

        %% ?D(x),
        {not_unanimous,_,_}=_XX = ?MGR:test_read_latest_public_projection(
                                     Ma, false),
        %% ?Dw(_XX),
        {not_unanimous,_,_}=_YY = ?MGR:test_read_latest_public_projection(
                                     Ma, true),
        %% The read repair here doesn't automatically trigger the creation of
        %% a new projection (to try to create a unanimous projection).  So
        %% we expect nothing to change when called again.
        {not_unanimous,_,_}=_YY = ?MGR:test_read_latest_public_projection(
                                     Ma, true),

        {now_using, _, EpochNum_a} = ?MGR:trigger_react_to_env(Ma),
        {no_change, _, EpochNum_a} = ?MGR:trigger_react_to_env(Ma),
        {unanimous,P2,_E2} = ?MGR:test_read_latest_public_projection(Ma, false),
        {ok, P2pa} = ?FLU_PC:read_latest_projection(Proxy_a, private),
        P2 = P2pa#projection_v1{dbg2=[]},

        %% Poke FLUb to react ... should be using the same private proj
        %% as FLUa.
        {now_using, _, EpochNum_a} = ?MGR:trigger_react_to_env(Mb),
        {ok, P2pb} = ?FLU_PC:read_latest_projection(Proxy_b, private),
        P2 = P2pb#projection_v1{dbg2=[]},

        Mgrs = [a_chmgr, b_chmgr, c_chmgr],
        Advance = make_advance_fun([a_fitness,b_fitness,c_fitness],
                                   [a,b,c],
                                   Mgrs,
                                   3),
        Advance(),
        {_, _, TheEpoch_3} = ?MGR:trigger_react_to_env(Ma),
        {_, _, TheEpoch_3} = ?MGR:trigger_react_to_env(Mb),
        {_, _, TheEpoch_3} = ?MGR:trigger_react_to_env(Mc),

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("STEP: Remove 'a' from the chain.\n", []),

        MembersDict4 = machi_projection:make_members_dict(tl(Ps)),
        ok = machi_chain_manager1:set_chain_members(
               Mb, ChainName, TheEpoch_3, ap_mode, MembersDict4, []),

        Advance(),
        {ok, {true, _,_,_}} = ?FLU_PC:wedge_status(Proxy_a),
        {_, _, TheEpoch_4} = ?MGR:trigger_react_to_env(Mb),
        {_, _, TheEpoch_4} = ?MGR:trigger_react_to_env(Mc),
        [{ok, #projection_v1{upi=[b,c], repairing=[]}} =
             ?FLU_PC:read_latest_projection(Pxy, private) || Pxy <- tl(Proxies)],

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("STEP: Add a to the chain again (a is running).\n", []),

        MembersDict5 = machi_projection:make_members_dict(Ps),
        ok = machi_chain_manager1:set_chain_members(
               Mb, ChainName, TheEpoch_4, ap_mode, MembersDict5, []),

        Advance(),
        {_, _, TheEpoch_5} = ?MGR:trigger_react_to_env(Ma),
        {_, _, TheEpoch_5} = ?MGR:trigger_react_to_env(Mb),
        {_, _, TheEpoch_5} = ?MGR:trigger_react_to_env(Mc),
        [{ok, #projection_v1{upi=[b,c], repairing=[a]}} =
             ?FLU_PC:read_latest_projection(Pxy, private) || Pxy <- Proxies],

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("STEP: Stop a while a chain member, advance b&c.\n", []),

        ok = machi_flu_psup:stop_flu_package(a),
        Advance(),
        {_, _, TheEpoch_6} = ?MGR:trigger_react_to_env(Mb),
        {_, _, TheEpoch_6} = ?MGR:trigger_react_to_env(Mc),
        [{ok, #projection_v1{upi=[b,c], repairing=[]}} =
             ?FLU_PC:read_latest_projection(Pxy, private) || Pxy <- tl(Proxies)],

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("STEP: Remove 'a' from the chain.\n", []),

        MembersDict7 = machi_projection:make_members_dict(tl(Ps)),
        ok = machi_chain_manager1:set_chain_members(
               Mb, ChainName, TheEpoch_6, ap_mode, MembersDict7, []),

        Advance(),
        {_, _, TheEpoch_7} = ?MGR:trigger_react_to_env(Mb),
        {_, _, TheEpoch_7} = ?MGR:trigger_react_to_env(Mc),
        [{ok, #projection_v1{upi=[b,c], repairing=[]}} =
             ?FLU_PC:read_latest_projection(Pxy, private) || Pxy <- tl(Proxies)],

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("STEP: Start a, advance.\n", []),

        Opts = [{active_mode, false}, {initial_wedged, true}],
        #p_srvr{name=NameA} = hd(Ps),
        {ok,_}=machi_flu_psup:start_flu_package(NameA, TcpPort+1, hd(Dirs), Opts),
        Advance(),
        {ok, {true, _,_,_}} = ?FLU_PC:wedge_status(Proxy_a),
        {ok, {false, EpochID_8,_,_}} = ?FLU_PC:wedge_status(Proxy_b),
        {ok, {false, EpochID_8,_,_}} = ?FLU_PC:wedge_status(Proxy_c),
        [{ok, #projection_v1{upi=[b,c], repairing=[]}} =
             ?FLU_PC:read_latest_projection(Pxy, private) || Pxy <- tl(Proxies)],

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("STEP: Stop a, delete a's data, leave it stopped\n", []),

        ok = machi_flu_psup:stop_flu_package(a),
        Advance(),
        machi_flu1_test:clean_up_data_dir(hd(Dirs)),
        {ok, {false, _,_,_}} = ?FLU_PC:wedge_status(Proxy_b),
        {ok, {false, _,_,_}} = ?FLU_PC:wedge_status(Proxy_c),

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("STEP: Add a to the chain again (a is stopped).\n", []),

        MembersDict9 = machi_projection:make_members_dict(Ps),
        {_, _, TheEpoch_9} = ?MGR:trigger_react_to_env(Mb),
        ok = machi_chain_manager1:set_chain_members(
               Mb, ChainName, TheEpoch_9, ap_mode, MembersDict9, []),
        Advance(),
        {_, _, TheEpoch_9b} = ?MGR:trigger_react_to_env(Mb),
        true = (TheEpoch_9b > TheEpoch_9),

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("STEP: Start a, and it joins like it ought to\n", []),

        {ok,_}=machi_flu_psup:start_flu_package(NameA, TcpPort+1, hd(Dirs), Opts),
        Advance(),
        {ok, {false, {TheEpoch10,_},_,_}} = ?FLU_PC:wedge_status(Proxy_a),
        {ok, {false, {TheEpoch10,_},_,_}} = ?FLU_PC:wedge_status(Proxy_b),
        {ok, {false, {TheEpoch10,_},_,_}} = ?FLU_PC:wedge_status(Proxy_c),
        [{ok, #projection_v1{upi=[b,c], repairing=[a]}} =
             ?FLU_PC:read_latest_projection(Pxy, private) || Pxy <- Proxies],
        ok
    after
        [ok = ?FLU_PC:quit(X) || X <- Proxies],
        machi_test_util:stop_flu_packages()
    end.

unanimous_report_test() ->
    TcpPort = 63877,
    FluInfo = [{a,TcpPort+0,"./data.a"}, {b,TcpPort+1,"./data.b"}],
    P_s = [#p_srvr{name=Name, address="localhost", port=Port} ||
              {Name,Port,_Dir} <- FluInfo],
    MembersDict = machi_projection:make_members_dict(P_s),

    E5 = 5,
    UPI5 = [a,b],
    Rep5 = [],
    Report5 = [UPI5],
    P5 = machi_projection:new(E5, a, MembersDict, [], UPI5, Rep5, []),
    {ok_disjoint, Report5} =
        unanimous_report2([{a, P5}, {b, P5}]),
    {ok_disjoint, Report5} =
        unanimous_report2([{a, not_in_this_epoch}, {b, P5}]),
    {ok_disjoint, Report5} =
        unanimous_report2([{a, P5}, {b, not_in_this_epoch}]),

    UPI5_b = [a],
    Rep5_b = [],
    P5_b = machi_projection:new(E5, b, MembersDict, [b], UPI5_b, Rep5_b, []),
    {bummer_NOT_DISJOINT, _} = unanimous_report2([{a, P5}, {b, P5_b}]),

    UPI5_c = [b],
    Rep5_c = [a],
    P5_c = machi_projection:new(E5, b, MembersDict, [], UPI5_c, Rep5_c, []),
    {bummer_NOT_DISJOINT, _} =
        unanimous_report2([{a, P5}, {b, P5_c}]),

    P_s3 = [#p_srvr{name=Name, address="localhost", port=Port} ||
              {Name,Port,_Dir} <- FluInfo ++ [{c,TcpPort+0,"./data.c"}]],
    MembersDict3 = machi_projection:make_members_dict(P_s3),

    UPI5_d = [c],
    Rep5_d = [a],
    Report5d = [UPI5, UPI5_d],
    P5_d = machi_projection:new(E5, b, MembersDict3, [b], UPI5_d, Rep5_d, []),
    {ok_disjoint, Report5d} = unanimous_report2([{a, P5}, {b, P5_d}]),

    UPI5_e = [b],
    Rep5_e = [c],
    Report5be = [UPI5_b, UPI5_e],
    P5_e = machi_projection:new(E5, b, MembersDict3, [a], UPI5_e, Rep5_e, []),
    {bummer_NOT_DISJOINT, _} = unanimous_report2([{a, P5},   {b, P5_e}]),
    {ok_disjoint, Report5be} = unanimous_report2([{a, P5_b}, {b, P5_e}]),

    ok.

-endif. % !PULSE
-endif. % TEST
