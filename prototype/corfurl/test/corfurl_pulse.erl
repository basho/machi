%% -------------------------------------------------------------------
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

-module(corfurl_pulse).

-ifdef(TEST).
-ifdef(PULSE).

-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include("corfurl.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, pulse_instrument}).

-compile({pulse_skip,[{prop_pulse_test_,0},{clean_up_runtime,1}]}).
%% -compile({pulse_no_side_effect,[{file,'_','_'}, {erlang, now, 0}]}).

%% Used for output within EUnit...
-define(QC_FMT(Fmt, Args),
        io:format(user, Fmt, Args)).

%% And to force EUnit to output QuickCheck output...
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> ?QC_FMT(Str, Args) end, P)).

-define(MAX_PAGES, 50000).

-record(run, {
          seq,                                  % Sequencer
          proj,                                 % Projection
          flus                                  % List of FLUs
         }).

-record(state, {
          is_setup = false :: boolean(),
          num_chains = 0 :: integer(),
          chain_len = 0 :: integer(),
          page_size = 0 :: integer(),
          run :: #run{}
         }).

initial_state() ->
    #state{}.

gen_page(PageSize) ->
    binary(PageSize).

gen_seed() ->
    noshrink({choose(1, 20000), choose(1, 20000), choose(1, 20000)}).

gen_sequencer_percent() ->
    frequency([{10, choose(1,100)},
               {5, choose(90,100)}]).

gen_sequencer() ->
    frequency([{100, standard},
               {50, {gen_seed(), gen_sequencer_percent(), choose(1, 2)}}]).

gen_approx_page() ->
    %% EQC can't know what pages are perhaps-written, so pick something big.
    ?LET(I, largeint(), abs(I)).

command(#state{run=Run} = S) ->
    ?LET({NumChains, ChainLen, PageSize},
         {parameter(num_chains), parameter(chain_len), parameter(page_size)},
    frequency(
      [{10, {call, ?MODULE, setup, [NumChains, ChainLen, PageSize, gen_sequencer()]}}
       || not S#state.is_setup] ++
      [{10, {call, ?MODULE, append, [Run, gen_page(PageSize)]}}
       || S#state.is_setup] ++
      [{10, {call, ?MODULE, read_approx, [Run, gen_approx_page()]}}
       || S#state.is_setup] ++
      [])).

%% Precondition, checked before a command is added to the command sequence.
precondition(S, {call, _, setup, _}) ->
    not S#state.is_setup;
precondition(S, {call, _, _, _}) ->
    S#state.is_setup.

%% Next state transformation, S is the current state and V is the result of the
%% command.
next_state(S, Res, {call, _, setup, [NumChains, ChainLen, PageSize, _SeqType]}) ->
    S#state{is_setup=true,
            num_chains=NumChains,
            chain_len=ChainLen,
            page_size=PageSize,
            run=Res};
next_state(S, _, {call, _, append, _}) ->
    S;
next_state(S, _, {call, _, read_approx, _}) ->
    S.

eqeq(X, X) -> true;
eqeq(X, Y) -> {X, '/=', Y}.

postcondition(_S, {call, _, setup, _}, #run{} = _V) ->
    true;
postcondition(_S, {call, _, append, _}, {ok, LPN}) when is_integer(LPN) ->
    true;
postcondition(_S, {call, _, append, _}, V) ->
    eqeq(V, todoTODO_fixit);
postcondition(_S, {call, _, read_approx, _}, V) ->
    case V of
        {ok, Pg} when is_binary(Pg) -> true;
        error_unwritten             -> true;
        error_trimmed               -> true;
        _                           -> eqeq(V, todoTODO_fixit)
    end.

run_commands_on_node(_LocalOrSlave, Cmds, Seed) ->
    %% AfterTime = if LocalOrSlave == local -> 50000;
    %%                LocalOrSlave == slave -> 1000000
    %%             end,
    event_logger:start_link(),
    pulse:start(),
    error_logger:tty(false),
    error_logger:add_report_handler(handle_errors),
    event_logger:start_logging(),
    X =
    try
      {H, S, Res, Trace} = pulse:run(fun() ->
          %% application:start(my_test_app),
          %% receive after AfterTime -> ok end,
          {H, S, R} = run_parallel_commands(?MODULE, Cmds),
          %% io:format(user, "Yooo: H = ~p\n", [H]),
          %% io:format(user, "Yooo: S = ~p\n", [S]),
          %% io:format(user, "Yooo: R = ~p\n", [R]),
          %% receive after AfterTime -> ok end,
          Trace = event_logger:get_events(),
          %% receive after AfterTime -> ok end,
          catch exit(pulse_application_controller, shutdown),
          {H, S, R, Trace}
        end, [{seed, Seed},
              {strategy, unfair}]),
      Schedule = pulse:get_schedule(),
      Errors = gen_event:call(error_logger, handle_errors, get_errors, 60*1000),
      [clean_up_runtime(S) || S#state.run /= undefined],
      {H, S, Res, Trace, Schedule, Errors}
    catch
        _:Err ->
            {'EXIT', Err}
    end,
    X.

prop_pulse() ->
  prop_pulse(local).

prop_pulse(LocalOrSlave) ->
  ?FORALL({NumChains, ChainLen, PageSize},
          {choose(1, 3), choose(1, 3), choose(1, 16)},
  begin
    P = ?FORALL({Cmds, Seed},
              {with_parameters([{num_chains, NumChains},
                                {chain_len, ChainLen},
                                {page_size, PageSize}], parallel_commands(?MODULE)),
               pulse:seed()},
    begin
      case run_commands_on_node(LocalOrSlave, Cmds, Seed) of
          {'EXIT', Err} ->
              equals({'EXIT', Err}, ok);
          {_H, S, Res, Trace, Schedule, Errors} ->
              CheckTrace = check_trace(Trace, Cmds, Seed),
              ?WHENFAIL(
                 S = S, % ?QC_FMT("\nState: ~p\n", [S]),
                 measure(schedule, length(Schedule),
                 conjunction(
                   [{simple_result, equals(Res, ok)},
                    {errors, equals(Errors, [])},
                    {events, CheckTrace} ])))
      end
    end),
  P
  end).

prop_pulse_test_() ->
  Timeout = case os:getenv("PULSE_TIME") of
                false -> 60;
                Val   -> list_to_integer(Val)
            end,
  ExtraTO = case os:getenv("PULSE_SHRINK_TIME") of
                false -> 0;
                Val2  -> list_to_integer(Val2)
            end,
  io:format(user, "prop_pulse_test time: ~p + ~p seconds\n",
            [Timeout, ExtraTO]),
  {timeout, (Timeout+ExtraTO) + 60,
   fun() ->
       ?assert(eqc:quickcheck(eqc:testing_time(Timeout,?QC_OUT(prop_pulse()))))
   end}.


%% Example Trace0 (raw event info, from the ?LOG macro)
%%
%% [{32014,{call,<0.467.0>,{append,<<"O">>}}},
%%  {32421,{call,<0.466.0>,{append,<<134>>}}},
%%  {44522,{result,<0.467.0>,{ok,1}}},
%%  {47651,{result,<0.466.0>,{ok,2}}}]

check_trace(Trace0, _Cmds, _Seed) ->
    %% Let's treat this thing like a KV store.  It is, mostly.
    %% Key = LPN, Value = error_unwritten | {ok, Blob} | error_trimmed
    %%
    %% Problem: At {call, Pid, ...} time, we don't know what Key is!
    %%          We find out at {return, Pid, {ok, LSN}} time.
    %%          Also, the append might fail, so the model can ignore those
    %%          failures because they're not mutating any state that and
    %%          external viewer can see.
    Trace = add_LPN_to_append_calls(Trace0),

    Events = eqc_temporal:from_timed_list(Trace),
    %% Example Events, temporal style, 1 usec resolution, same as original trace
    %%
    %% [{0,32014,[]},
    %%  {32014,32015,[{call,<0.467.0>,{append,<<"O">>,will_be,1}}]},
    %%  {32015,32421,[]},
    %%  {32421,32422,[{call,<0.466.0>,{append,<<134>>,will_be,2}}]},
    %%  {32422,44522,[]},
    %%  {44522,44523,[{result,<0.467.0>,{ok,...}}]},
    %%  {44523,47651,[]},
    %%  {47651,47652,[{result,<0.466.0>,{ok,...}}]},
    %%  {47652,infinity,[]}]

    Calls  = eqc_temporal:stateful(
               fun({call, Pid, Call}) -> [{call, Pid, Call}] end,
               fun({call, Pid, _Call}, {result, Pid, _}) -> [] end,
               Events),
    %% Example Calls (temporal map of when a call is in progress)
    %%
    %% [{0,32014,[]},
    %%  {32014,32421,[{call,<0.467.0>,{append,<<"O">>,will_be,1}}]},
    %%  {32421,44522,
    %%   [{call,<0.466.0>,{append,<<134>>,will_be,2}},{call,<0.467.0>,{append,<<"O">>,will_be,1}}]},
    %%  {44522,47651,[{call,<0.466.0>,{append,<<134>>,will_be,2}}]},
    %%  {47651,infinity,[]}]

    %% Remember: Mods contains only successful append ops!
    %% ModsAllFuture is used for calculating which LPNs were written,
    %% but Mods is used for everything else.  The two stateful() calls
    %% at identical except for the "Compare here" difference.
    Mods = eqc_temporal:stateful(
               fun({call, Pid, {append, Pg, will_be, LPN}}) ->
                       {lpn, LPN, Pg, Pid}
               end,
               fun({lpn, LPN, _Pg, Pid}, {result, Pid, {ok, LPN}})->
                       []                       % Compare here
               end,
               Events),
    ModsAllFuture = eqc_temporal:stateful(
               fun({call, Pid, {append, Pg, will_be, LPN}}) ->
                       {lpn, LPN, Pg, Pid}
               end,
               fun({lpn, LPN, Pg, Pid}, {result, Pid, {ok, LPN}})->
                       %% Keep this into the infinite future
                       [{lpn, LPN, Pg}]         % Compare here
               end,
               Events),

    %%QQQ = -5,
    %%if length(Trace) < QQQ -> io:format("Trace ~p\n", [Trace]), io:format("Events ~p\n", [Events]), io:format("Mods ~p\n", [Mods]); true -> ok end,

    %% The last item in the relation tells us what the last/infinite future
    %% state of each LPN is.  We'll use it to identify all successfully
    %% written LPNs and other stuff.
    {_, infinity, FinalStatus} = lists:last(eqc_temporal:all_future(ModsAllFuture)),

    %% StartMod contains {m_start, LPN, V} when a modification finished.
    %% DoneMod contains {m_end, LPN, V} when a modification finished.
    %% This is a clever trick: Mods contains the start & end timestamp
    %% for each modification.  Use shift() by 1 usec to move all timestamps
    %% forward/backward 1 usec, then subtract away the original time range to
    %% leave a 1 usec relation in time.
    DoneMod = eqc_temporal:map(
                fun({lpn, LPN, Pg, _Pid}) -> {m_end, LPN, Pg} end,
                eqc_temporal:subtract(eqc_temporal:shift(1, Mods), Mods)),
    StartMod = eqc_temporal:map(
                fun({lpn, LPN, Pg, _Pid}) -> {m_start, LPN, Pg} end,
                eqc_temporal:subtract(Mods, eqc_temporal:shift(1, Mods))),
    %% if length(Trace) < QQQ -> io:format(user, "StartMod  ~P\n", [StartMod, 100]), io:format(user, "DoneMod  ~P\n", [DoneMod, 100]); true -> ok end,
    StartsDones = eqc_temporal:union(StartMod, DoneMod),
    %%if length(Trace) < QQQ -> io:format(user, "StartsDones ~P\n", [StartsDones, 100]); true -> ok end,

    %% TODO: A brighter mind than mine might figure out how to do this
    %% next step using only eqc_temporal.
    %%
    %% We create a new relation, ValuesR.  This relation contains
    %% {values, OD::orddict()} for each time interval in the relation.
    %% The OD contains all possible values for a particular LPN at
    %% that time in the relation.
    %% The key for OD is LPN, the value is an unordered list of possible values.

    InitialDict = orddict:from_list([{LPN, [error_unwritten]} ||
                                        {lpn, LPN, _} <- FinalStatus]),
    {ValuesR, _} =
        lists:mapfoldl(
          fun({TS1, TS2, StEnds}, Dict1) ->
                  Dict2 = lists:foldl(
                            fun({m_start, LPN, Pg}, D) ->
                                    orddict:append(LPN, Pg, D)
                            end, Dict1, [X || X={m_start,_,_} <- StEnds]),
                  Dict3 = lists:foldl(
                            fun({m_end, LPN, Pg}, D) ->
                                    orddict:store(LPN, [Pg], D)
                            end, Dict2, [X || X={m_end,_,_} <- StEnds]),
                  {{TS1, TS2, [{values, Dict3}]}, Dict3}
          end, InitialDict, StartsDones),
    %%if length(Trace) < QQQ -> io:format(user, "ValuesR ~P\n", [ValuesR, 100]); true -> ok end,

    %% We want to find & fail any two clients that append the exact same page
    %% data to the same LPN.  Unfortunately, the eqc_temporal library will
    %% merge two such facts together into a single fact.  So this method
    %% commented below isn't good enough.
    %% M_Ends = eqc_temporal:at(infinity, eqc_temporal:any_past(DoneMod)),
    %% AppendedLPNs = lists:sort([LPN || {m_end, LPN, _} <- M_Ends]),
    %% {_Last, DuplicateLPNs} = lists:foldl(fun(X, {X, Dups}) -> {X, [X|Dups]};
    %%                                         (X, {_, Dups}) -> {X, Dups}
    %%                                      end, {undefined, []}, AppendedLPNs),
    AppendWillBes = [LPN || {_TS, {call, _, {append, _, will_be, LPN}}} <- Trace],
    DuplicateLPNs = AppendWillBes -- lists:usort(AppendWillBes),

    Reads = eqc_temporal:stateful(
               fun({call, Pid, {read, LPN}}) ->
                       {read, Pid, LPN}
               end,
               fun({read, Pid, LPN}, {result, Pid, {ok, Pg}}) ->
                       [{read_finished, LPN, Pg}];
                  ({read, Pid, LPN}, {result, Pid, Else}) ->
                       [{read_finished, LPN, Else}]
               end,
               Events),
    DoneRead = eqc_temporal:map(
                fun({read_finished, LPN, Pg}) -> {read_end, LPN, Pg} end,
                eqc_temporal:subtract(eqc_temporal:shift(-1, Reads), Reads)),
    StartRead = eqc_temporal:map(
                fun({read, Pid, LPN}) -> {read_start, LPN, Pid} end,
                eqc_temporal:subtract(Reads, eqc_temporal:shift(1, Reads))),
    %%io:format("Reads = ~P\n", [Reads, 30]),
    %%io:format("DoneRead = ~P\n", [DoneRead, 30]),
    %%io:format("UU ~p\n", [eqc_temporal:union(DoneRead, ValuesR)]),
    BadReadR = eqc_temporal:stateful(
                 fun({read_end, _, _} = I) -> I end,
                 fun({read_end, LPN, Pg}, {values, Dict}) ->
                         {ok, PossibleVals} = orddict:find(LPN, Dict),
                         case lists:member(Pg, PossibleVals) of
                             true ->
                                 [];
                             false ->
                                 [{bad, read, LPN, got, Pg,
                                   possible, PossibleVals}]
                         end
                 end, eqc_temporal:union(DoneRead, ValuesR)),
    %%io:format("BadReadR = ~P\n", [BadReadR, 20]),
    BadFilter = fun(bad) -> true;
                   (Bad) when is_tuple(Bad), element(1, Bad) == bad -> true;
                   (_)   -> false end,
    %%io:format("BadReadR = ~P\n", [BadReadR, 40]),
    BadReads = [{TS1, TS2, lists:filter(BadFilter, Facts)} ||
                   {TS1, TS2, Facts} <- BadReadR,
                   Fact <- Facts, BadFilter(Fact)],

    %% Desired properties
    AllCallsFinish = eqc_temporal:is_false(eqc_temporal:all_future(Calls)),
    NoAppendDuplicates = (DuplicateLPNs == []),
    NoBadReads = (BadReads == []),

    ?WHENFAIL(begin
    %% ?QC_FMT("*Events: ~p\n", [Events]),
    ?QC_FMT("*DuplicateLPNs: ~p\n", [DuplicateLPNs]),
    ?QC_FMT("*Mods: ~p\n", [Mods]),
    ?QC_FMT("*readsUmods: ~p\n", [eqc_temporal:union(Reads, Mods)]),
    ?QC_FMT("*DreadUDmod: ~p\n", [eqc_temporal:unions([DoneRead, DoneMod,
                                                       StartRead, StartMod])]),
    ?QC_FMT("*BadReads: ~p\n", [BadReads])
    end,
    conjunction(
      [
       {all_calls_finish, AllCallsFinish},
       {no_append_duplicates, NoAppendDuplicates},
       {no_bad_reads, NoBadReads},
       %% If you want to see PULSE causing crazy scheduling, then
       %% change one of the "true orelse" -> "false orelse" below.
       %% {bogus_no_gaps,
       %%  true orelse
       %%          (AppendLPNs == [] orelse length(range_ify(AppendLPNs)) == 1)},
       %% {bogus_exactly_1_to_N,
       %%  true orelse (AppendLPNs == lists:seq(1, length(AppendLPNs)))},
       {true, true}
      ])).

add_LPN_to_append_calls([{TS, {call, Pid, {append, Page}}}|Rest]) ->
    Res = trace_lookahead_pid(Pid, Rest),
    New = case Res of
              {ok, LPN} ->
                  {TS, {call, Pid, {append, Page, will_be, LPN}}};
              Else ->
                  {TS, {call, Pid, {append, Page, will_fail, Else}}}
          end,
    [New|add_LPN_to_append_calls(Rest)];
add_LPN_to_append_calls([X|Rest]) ->
    [X|add_LPN_to_append_calls(Rest)];
add_LPN_to_append_calls([]) ->
    [].

trace_lookahead_pid(Pid, [{_TS, {result, Pid, Res}}|_]) ->
    Res;
trace_lookahead_pid(Pid, [_H|T]) ->
    trace_lookahead_pid(Pid, T).

%% Presenting command data statistics in a nicer way
command_data({set, _, {call, _, Fun, _}}, {_S, _V}) ->
  Fun.

%% Convenience functions for running tests

test() ->
  test({20, sec}).

test(N) when is_integer(N) ->
  quickcheck(numtests(N, prop_pulse()));
test({Time, sec}) ->
  quickcheck(eqc:testing_time(Time, prop_pulse()));
test({Time, min}) ->
  test({Time * 60, sec});
test({Time, h}) ->
  test({Time * 60, min}).

check() ->
  check(current_counterexample()).

verbose() ->
  verbose(current_counterexample()).

verbose(CE) ->
  erlang:put(verbose, true),
  Ok = check(CE),
  erlang:put(verbose, false),
  Ok.

check(CE) ->
  check(on_output(fun("OK" ++ _, []) -> ok; (Fmt, Args) -> io:format(Fmt, Args) end,
                  prop_pulse(true == erlang:get(verbose))),
        CE).

recheck() ->
  recheck(prop_pulse()).

zipwith(F, [X|Xs], [Y|Ys]) ->
  [F(X, Y)|zipwith(F, Xs, Ys)];
zipwith(_, _, _) -> [].

clean_up_runtime(#state{run=R} = _S) ->
    %% io:format(user, "clean_up_runtime: run = ~p\n", [R]),
    catch corfurl_sequencer:stop(R#run.seq),
    [catch corfurl_flu:stop(F) || F <- R#run.flus],
    corfurl_test:setup_del_all(length(R#run.flus)).

make_chains(ChainLen, FLUs) ->
    make_chains(ChainLen, FLUs, [], []).

make_chains(_ChainLen, [], SmallAcc, BigAcc) ->
    lists:reverse([SmallAcc|BigAcc]);
make_chains(ChainLen, [H|T], SmallAcc, BigAcc) ->
    if length(SmallAcc) == ChainLen ->
            make_chains(ChainLen, T, [H], [SmallAcc|BigAcc]);
       true ->
            make_chains(ChainLen, T, [H|SmallAcc], BigAcc)
    end.

setup(NumChains, ChainLen, PageSize, SeqType) ->
    N = NumChains * ChainLen,
    FLUs = corfurl_test:setup_basic_flus(N, PageSize, ?MAX_PAGES),
    {ok, Seq} = corfurl_sequencer:start_link(FLUs, SeqType),
    Chains = make_chains(ChainLen, FLUs),
    %% io:format(user, "Cs = ~p\n", [Chains]),
    Proj = corfurl:new_simple_projection(1, 1, ?MAX_PAGES, Chains),
    #run{seq=Seq, proj=Proj, flus=FLUs}.

range_ify([]) ->
    [];
range_ify(L) ->
    [H|T] = lists:sort(L),
    range_ify(H, H+1, T).

range_ify(Beginning, Next, [Next|T]) ->
    range_ify(Beginning, Next+1, T);
range_ify(Beginning, Next, [Else|T]) ->
    [{Beginning, to, Next-1}|range_ify(Else, Else+1, T)];
range_ify(Beginning, Next, []) ->
    [{Beginning, to, Next-1}].

-define(LOG(Tag, MkCall),
        event_logger:event({call, self(), Tag}),
        LOG__Result = MkCall,
        event_logger:event({result, self(), LOG__Result}),
        LOG__Result).

-ifndef(TEST_TRIP_no_append_duplicates).

append(#run{seq=Seq, proj=Proj}, Page) ->
    ?LOG({append, Page},
         corfurl:append_page(Seq, Proj, Page)).
-else. % TEST_TRIP_no_append_duplicates

%% If the appended LPN > 3, just lie and say that it was 3.

append(#run{seq=Seq, proj=Proj}, Page) ->
    MaxLPN = 3,
    ?LOG({append, Page},
         begin
             case corfurl:append_page(Seq, Proj, Page) of
                 {ok, LPN} when LPN > MaxLPN ->
                     Bad = {ok, MaxLPN},
                     io:format("BAD: append: ~p -> ~p\n", [Page, Bad]),
                     Bad;
                 Else ->
                     Else
             end
         end).
-endif. % TEST_TRIP_no_append_duplicates

-ifndef(TEST_TRIP_bad_read).

read_approx(#run{seq=Seq, proj=Proj}, SeedInt) ->
    Max = corfurl_sequencer:get(Seq, 0),
    %% The sequencer may be lying to us, shouganai.
    LPN = (SeedInt rem Max) + 1,
    ?LOG({read, LPN},
         corfurl:read_page(Proj, LPN)).
-else. % TEST_TRIP_bad_read

read_approx(#run{seq=Seq, proj=Proj}, SeedInt) ->
    Fake = <<"FAKE!">>,
    Max = corfurl_sequencer:get(Seq, 0),
    LPN = (SeedInt rem Max) + 1,
    ?LOG({read, LPN},
         if LPN > 4 ->
                 io:format("read_approx: ~p -> ~p\n", [LPN, Fake]),
                 {ok, Fake};
            true ->
                 Res = corfurl:read_page(Proj, LPN),
                 %% io:format("read_approx: ~p -> ~P\n", [LPN, Res, 6]),
                 Res
         end).

-endif. % TEST_TRIP_bad_read

-endif. % PULSE
-endif. % TEST

