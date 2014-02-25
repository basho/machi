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
-define(MY_TAB,  i_have_a_name).
-define(MY_KEY, ?MY_TAB).

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

%% Model testing things:
%% Define true to fake bad behavior that model **must** notice & fail!

-ifndef(TRIP_no_append_duplicates).
-define(TRIP_no_append_duplicates, false).
-endif.
-ifndef(TRIP_bad_read).
-define(TRIP_bad_read, false).
-endif.
-ifndef(TRIP_bad_scan_forward).
-define(TRIP_bad_scan_forward, false).
-endif.
-ifndef(TRIP_bad_fill).
-define(TRIP_bad_fill, false).
-endif.
-ifndef(TRIP_bad_trim).
-define(TRIP_bad_trim, false).
-endif.

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
    noshrink(?LET(I, largeint(), abs(I))).

gen_scan_forward_start() ->
    oneof([1, gen_approx_page()]).

command(#state{run=Run} = S) ->
    ?LET({NumChains, ChainLen, PageSize},
         {parameter(num_chains), parameter(chain_len), parameter(page_size)},
    frequency(
      [{10, {call, ?MODULE, setup, [NumChains, ChainLen, PageSize, gen_sequencer()]}}
       || not S#state.is_setup] ++
      [{10, {call, ?MODULE, append, [Run, gen_page(PageSize)]}}
       || S#state.is_setup] ++
      [{3, {call, ?MODULE, read_approx, [Run, gen_approx_page()]}}
       || S#state.is_setup] ++
      [{5, {call, ?MODULE, scan_forward, [Run, gen_scan_forward_start(), nat()]}}
       || S#state.is_setup] ++
      [{4, {call, ?MODULE, fill, [Run, gen_approx_page()]}}
       || S#state.is_setup] ++
      [{4, {call, ?MODULE, trim, [Run, gen_approx_page()]}}
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
    S;
next_state(S, _, {call, _, scan_forward, _}) ->
    S;
next_state(S, _, {call, _, fill, _}) ->
    S;
next_state(S, _, {call, _, trim, _}) ->
    S.

eqeq(X, X) -> true;
eqeq(X, Y) -> {X, '/=', Y}.

postcondition(_S, {call, _, setup, _}, #run{} = _V) ->
    true;
postcondition(_S, {call, _, append, _}, {ok, LPN}) when is_integer(LPN) ->
    true;
postcondition(_S, {call, _, append, _}, {special_trimmed, LPN})
  when is_integer(LPN) ->
    true;
postcondition(_S, {call, _, append, _}, V) ->
    eqeq(V, todoTODO_fixit);
postcondition(_S, {call, _, read_approx, _}, V) ->
    valid_read_result(V);
postcondition(_S, {call, _, scan_forward, _}, V) ->
    case V of
        {ok, LastLSN, MoreP, Pages} ->
            true = is_integer(LastLSN),
            true = LastLSN > 0,
            true = (MoreP == true orelse MoreP == false),
            [] = lists:usort([X || {_LPN, Pg} <- Pages,
                                   X <- [valid_read_result(Pg)], X /= true]),
            true;
        _ ->
            eqeq(V, {todoTODO_fixit,?LINE})
    end;
postcondition(_S, {call, _, FillTrim, _}, V)
  when FillTrim == fill; FillTrim == trim ->
    case V of
        ok                -> true;
        error_trimmed     -> true;
        error_unwritten   -> true;
        error_overwritten -> true;
        _                 -> eqeq(V, {error, FillTrim, V})
    end.

valid_read_result(Pg) when is_binary(Pg) -> true;
valid_read_result(error_unwritten)       -> true;
valid_read_result(error_trimmed)         -> true;
valid_read_result(V)                     -> eqeq(V, {todoTODO_fixit,?LINE}).

run_commands_on_node(LocalOrSlave, Cmds, Seed) ->
    AfterTime = if LocalOrSlave == local -> 50000;
                   LocalOrSlave == slave -> 1000000
                end,
    event_logger:start_link(),
    pulse:start(),
    error_logger:tty(false),
    error_logger:add_report_handler(handle_errors),
    event_logger:start_logging(),
    X =
    try
      {H, S, Res, Trace} = pulse:run(fun() ->
          catch ets:new(?MY_TAB, [public, set, named_table]),
          ets:insert(?MY_TAB, {?MY_KEY, undefined}),
          %% application:start(my_test_app),
          %% receive after AfterTime -> ok end,
          {H, S, R} = run_parallel_commands(?MODULE, Cmds),
          %% io:format(user, "Yooo: H = ~p\n", [H]),
          %% io:format(user, "Yooo: S = ~p\n", [S]),
          %% io:format(user, "Yooo: R = ~p\n", [R]),
          receive after AfterTime -> ok end,
          Trace = event_logger:get_events(),
          %% receive after AfterTime -> ok end,
          [{_, ThisRun}] = ets:lookup(?MY_TAB, ?MY_KEY),
          [clean_up_runtime(ThisRun) || ThisRun /= undefined],
          %% stop pulse controller *after* clean_up_runtime().
          catch exit(pulse_application_controller, shutdown),
          {H, S, R, Trace}
        end, [{seed, Seed},
              {strategy, unfair}]),
      Schedule = pulse:get_schedule(),
      Errors = gen_event:call(error_logger, handle_errors, get_errors, 60*1000),
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
    %% WARNING: Trace0 + lamport_clocks means Trace0 is not strictly sorted!
    Trace = add_LPN_to_append_calls(lists:sort(Trace0)),

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
               fun({call, _Pid, _Call} = I) -> [I] end,
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

    AllLPNsR = eqc_temporal:stateful(
                fun({call, _Pid, {append, _Pg, will_be, LPN}}) -> LPN;
                   ({call, _Pid, {append, _Pg, will_fail, {special_trimmed, LPN}}}) -> LPN;
                   ({call, _Pid, {read, LPN, _, _}}) -> LPN;
                   ({call, _Pid, {fill, LPN, will_be, ok}}) -> LPN;
                   ({call, _Pid, {trim, LPN, will_be, ok}}) -> LPN
                end,
                fun(x) -> [] end,
                Calls),
    %%io:format("Calls ~p\n", [Calls]),
    %%io:format("AllLPNsR ~p\n", [AllLPNsR]),
    %% The last item in the relation tells us what the final facts are in the
    %% relation.  In this case, it's all LPNs ever mentioned in the test run.
    {_, infinity, AllLPNs} = lists:last(eqc_temporal:all_future(AllLPNsR)),

    %% Use the following atoms to denote transitions ("Ttn") by an LPN:
    %% w_0 = not written yet, error_unwritten
    %% w_1 = written successfully, {ok, binary::()}
    %% w_ft = fill trimmed, error_trimmed
    %% w_tt = trim trimmed, error_trimmed

    Mods = eqc_temporal:stateful(
               fun({call, Pid, {append, Pg, will_be, LPN}}) ->
                       {mod_working, w_1, LPN, Pg, Pid};
                  ({call, Pid, {append, Pg, will_fail, {special_trimmed, LPN}}}) ->
                       %% This is a special case for the model.  We know that
                       %% a write raced with a trim and lost (at least some of
                       %% the time inside the chain).  But the transition that
                       %% we model in this case is a special w_ type that is
                       %% is trated specially by the dictionary-making
                       %% creation of the ValuesR relation.
                       {mod_working, w_special_trimmed, LPN, Pg, Pid};
                  ({call, Pid, {fill, LPN, will_be, ok}}) ->
                       {mod_working, w_ft, LPN, fill, Pid};
                  ({call, Pid, {trim, LPN, will_be, ok}}) ->
                       {mod_working, w_tt, LPN, trim, Pid};
                  ({call, Pid, {read, LPN, will_fail, error_trimmed}}) ->
                       {mod_working, w_tt, LPN, read_repair_maybe, Pid}
               end,
               fun({mod_working, _Ttn, _LPN, _Pg, _Pid}, {result, _Pid, _Res})->
                       []
               end,
               Events),

    %% StartMod contains {mod_start, Ttn, LPN, V} when a modification finished.
    %% DoneMod contains {mod_end, Ttn, LPN, V} when a modification finished.
    %% This is a clever trick: Mods contains the start & end timestamp
    %% for each modification.  Use shift() by 1 usec to move all timestamps
    %% forward/backward 1 usec, then subtract away the original time range to
    %% leave a 1 usec relation in time.
    StartMod = eqc_temporal:map(
                fun({mod_working, Ttn, LPN, Pg, _Pid}) ->
                        {mod_start, Ttn, LPN, Pg}
                end,
                eqc_temporal:subtract(Mods, eqc_temporal:shift(1, Mods))),
    DoneMod = eqc_temporal:map(
                fun({mod_working, Ttn, LPN, Pg, _Pid}) ->
                        {mod_end, Ttn, LPN, Pg}
                end,
                eqc_temporal:subtract(eqc_temporal:shift(1, Mods), Mods)),
    StartsDones = eqc_temporal:union(StartMod, DoneMod),

    %% TODO: A brighter mind than mine might figure out how to do this
    %% next step using only eqc_temporal.
    %%
    %% We create a new relation, ValuesR.  This relation contains
    %% {values, OD::orddict()} for each time interval in the relation.
    %% The OD contains all possible values for a particular LPN at
    %% that time in the relation.
    %% The key for OD is LPN, the value is an unordered list of possible values.

    InitialValDict = orddict:from_list([{LPN, [error_unwritten]} ||
                                        LPN <- AllLPNs]),
    {ValuesR, _} =
        lists:mapfoldl(
          fun({TS1, TS2, StEnds}, Dict1) ->
                  Dict2 = lists:foldl(
                            fun({mod_start, w_1, LPN, Pg}, D) ->
                                    orddict:append(LPN, Pg, D);
                               ({mod_start, WType, LPN, _Pg}, D)
                                  when WType == w_ft; WType == w_tt ->
                                    case lists:member(error_trimmed,
                                                      orddict:fetch(LPN, D)) of
                                        true ->
                                            D;
                                        false ->
                                            orddict:append(LPN, error_trimmed,D)
                                    end;
                               ({mod_start, w_special_trimmed, LPN, Pg}, D)->
                                       orddict:append(LPN, Pg, D)
                            end, Dict1, [X || X={mod_start,_,_,_} <- StEnds]),
                  Dict3 = lists:foldl(
                            fun({mod_end, w_1, LPN, Pg}, D) ->
                                    Vs1 = orddict:fetch(LPN, D),
                                    %% We've written a page.  error_unwriten is
                                    %% now impossible; any other binary() is
                                    %% also impossible.  However, there may be
                                    %% a trim operation that's still in flight!
                                    Vs2 = [V || V <- Vs1, V /= error_unwritten,
                                                not is_binary(V)],
                                    orddict:store(LPN, [Pg|Vs2], D);
                               ({mod_end, WType, LPN, _Pg}, D)
                                  when WType == w_ft; WType == w_tt ->
                                    orddict:store(LPN, [error_trimmed], D);
                               ({mod_end, w_special_trimmed, LPN, Pg}, D) ->
                                    orddict:store(LPN, [Pg,error_trimmed], D)
                            end, Dict2, [X || X={mod_end,_,_,_} <- StEnds]),
                  {{TS1, TS2, [{values, Dict3}]}, Dict3}
          end, InitialValDict, StartsDones),

    InitialTtnDict = orddict:from_list([{LPN, [w_0]} || LPN <- AllLPNs]),
    {TransitionsR, _} =
        lists:mapfoldl(
          fun({TS1, TS2, StEnds}, Dict1) ->
                  Dict2 = lists:foldl(
                            fun({mod_end, Ttn, LPN, _Pg}, D) ->
                                    %% orddict does not discard duplicates
                                    orddict:append(LPN, Ttn, D);
                               (_, D) ->
                                    D
                            end, Dict1, [X || X={mod_end,_,_,_} <- StEnds]),
                  {{TS1, TS2, [{transitions, Dict2}]}, Dict2}
          end, InitialTtnDict, StartsDones),

    %% Checking reads is a tricky thing.  My first attempt created a temporal
    %% relation for the 1usec window when the read call was complete, then
    %% union with the ValuesR relation to see what values were valid at that
    %% particular instant.  That approach fails sometimes!
    %%
    %% The reason is honest race conditions with a mutation: the model doesn't
    %% know exactly when the data was written, so a valid value may have been
    %% added/removed from the ValuesR relation that aren't there for the
    %% 1usec window that intersects with ValuesR.
    %%
    %% Instead, we need to merge together all possible values from ValuesR
    %% that appear at any time during the read op's lifetime.

    Reads = eqc_temporal:stateful(
               fun({call, Pid, {read, LPN, _, _}}) ->
                       {read, Pid, LPN, []}
               end,
               fun({read, Pid, LPN, V1s}, {values, Values}) ->
                       {ok, V2s} = orddict:find(LPN, Values),
                       NewVs = lists:umerge(lists:sort(V1s),
                                            lists:sort(V2s)),
                       %% Throw an exception (which is equivalent to a no-op)
                       %% if there are no differences: if we make multiples
                       %% of the exact same thing, stateful() will get confused.
                       false = NewVs == V1s,
                       {read, Pid, LPN, NewVs};
                  ({read, Pid, LPN, Vs}, {result, Pid, Pg}) ->
                       case lists:member(Pg, Vs) of
                           true  -> [];
                           false -> [{bad, read, LPN, Pid, got, Pg,
                                      possible, Vs}]
                       end
               end, eqc_temporal:union(Events, ValuesR)),
    BadFilter = fun(bad) -> true;
                   (Bad) when is_tuple(Bad), element(1, Bad) == bad -> true;
                   (_)   -> false end,
    BadReads = filter_relation_facts(BadFilter, Reads),

    %% Property: For all LPNs, the transition list for K must be one of the
    %% following four (4) acceptable transition orderings.
    {_, _, [{transitions, FinalTtns}]} = lists:last(
                                         eqc_temporal:all_future(TransitionsR)),
    FinaTtns_filtered = filter_transition_trimfill_suffixes(FinalTtns),
    InvalidTransitions = orddict:fold(
                           fun(_LPN, [w_0], Acc) ->
                                   Acc;
                              (_LPN, [w_0,w_1], Acc) ->
                                   Acc;
                              (_LPN, [w_0,'w_t+'], Acc) ->
                                   Acc;
                              (_LPN, [w_0,w_1,'w_t+'], Acc) ->
                                   Acc;
                              (LPN, BadTtns, Acc) ->
                                   [{LPN, BadTtns}|Acc]
                           end, [], FinaTtns_filtered),

    ?WHENFAIL(begin
    ?QC_FMT("*Trace: ~p\n", [Trace]),
    ?QC_FMT("*ModsReads: ~p\n", [eqc_temporal:unions([Mods,Reads])]),
    ?QC_FMT("*InvalidTtns: ~p\n", [InvalidTransitions]),
    ?QC_FMT("*BadReads: ~p\n", [BadReads])
    end,
    conjunction(
      [
       {all_calls_finish,
        eqc_temporal:is_false(eqc_temporal:all_future(Calls))},
       {no_invalidTransitions,
        InvalidTransitions == []},
       {no_bad_reads,
        eqc_temporal:is_false(eqc_temporal:all_future(BadReads))},
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
add_LPN_to_append_calls([{TS, {call, Pid, {OpName, LPN}}}|Rest])
  when OpName == fill; OpName == trim ->
    Res = trace_lookahead_pid(Pid, Rest),
    New = case Res of
              ok ->
                  {TS, {call, Pid, {OpName, LPN, will_be, ok}}};
              Else ->
                  {TS, {call, Pid, {OpName, LPN, will_fail, Else}}}
          end,
    [New|add_LPN_to_append_calls(Rest)];
add_LPN_to_append_calls([{TS, {call, Pid, {read, LPN}}}|Rest]) ->
    Res = trace_lookahead_pid(Pid, Rest),
    New = case Res of
              Page when is_binary(Page) ->
                  {TS, {call, Pid, {read, LPN, will_be, Page}}};
              Else ->
                  {TS, {call, Pid, {read, LPN, will_fail, Else}}}
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

clean_up_runtime(R) ->
    %% io:format(user, "clean_up_runtime: run = ~p\n", [R]),
    catch corfurl_sequencer:stop(R#run.seq),
    [catch corfurl_flu:stop(F) || F <- R#run.flus],
    corfurl_test:setup_del_all(length(R#run.flus)).

make_chains(ChainLen, FLUs) ->
    make_chains(ChainLen, FLUs, [], []).

make_chains(_ChainLen, [], SmallAcc, BigAcc) ->
    [lists:reverse(SmallAcc)|BigAcc];
make_chains(ChainLen, [H|T], SmallAcc, BigAcc) ->
    if length(SmallAcc) == ChainLen ->
            make_chains(ChainLen, T, [H], [lists:reverse(SmallAcc)|BigAcc]);
       true ->
            make_chains(ChainLen, T, [H|SmallAcc], BigAcc)
    end.

setup(NumChains, ChainLen, PageSize, SeqType) ->
    lamport_clock:init(),
    N = NumChains * ChainLen,
    FLUs = corfurl_test:setup_basic_flus(N, PageSize, ?MAX_PAGES),
    {ok, Seq} = corfurl_sequencer:start_link(FLUs, SeqType),
    Chains = make_chains(ChainLen, FLUs),
    %% io:format(user, "Cs = ~p\n", [Chains]),
    Proj = corfurl:new_simple_projection(1, 1, ?MAX_PAGES, Chains),
    Run = #run{seq=Seq, proj=Proj, flus=FLUs},
    ets:insert(?MY_TAB, {?MY_KEY, Run}),
    Run.

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

filter_relation_facts(FilterFun, R) ->
    [{TS1, TS2, lists:filter(FilterFun, Facts)} || {TS1, TS2, Facts} <- R].
        %% {TS1, TS2, Facts} <- Reads, Fact <- Facts, BadFilter(Fact)],

filter_transition_trimfill_suffixes(Ttns) ->
    [{X, filter_1_transition_list(L)} || {X, L} <- Ttns].

filter_1_transition_list([]) ->
    [];
filter_1_transition_list(Old) ->
    %% Strategy: Chop off all of the w_* at the end, then look at **Old** to
    %% see if we chopped off any.  If we did chop off any, then add back a
    %% constant 'w_t+' as a suffix.
    New = lists:reverse(lists:dropwhile(fun(w_tt)              -> true;
                                           (w_ft)              -> true;
                                           (w_special_trimmed) -> true;
                                           (_)    -> false
                                        end, lists:reverse(Old))),
    Suffix = case lists:last(Old) of
                 w_ft ->              ['w_t+'];
                 w_tt ->              ['w_t+'];
                 w_special_trimmed -> ['w_t+'];
                 _ ->                 []
             end,
    New ++ Suffix.

log_make_call(Tag) ->
    log_make_call(self(), Tag).

log_make_call(Pid, Tag) ->
    {call, Pid, Tag}.

log_make_result(Result) ->
    log_make_result(self(), Result).

log_make_result(Pid, Result) ->
    {result, Pid, Result}.

pick_an_LPN(Seq, SeedInt) ->
    Max = corfurl_sequencer:get(Seq, 0),
    %% The sequencer may be lying to us, shouganai.
    if SeedInt > Max -> (SeedInt rem Max) + 1;
       true          -> SeedInt
    end.

-define(LOG(Tag, MkCall),
        event_logger:event(log_make_call(Tag), lamport_clock:get()),
        LOG__Result = MkCall,
        event_logger:event(log_make_result(LOG__Result), lamport_clock:get()),
        LOG__Result).

append(#run{seq=Seq, proj=Proj}, Page) ->
    lamport_clock:init(),
    lamport_clock:incr(),
    ?LOG({append, Page},
         begin
             Res = corfurl:append_page(Seq, Proj, Page),
             perhaps_trip_append_page(?TRIP_no_append_duplicates, Res, Page)
         end).

read_result_mangle({ok, Page}) ->
    Page;
read_result_mangle(Else) ->
    Else.

read_approx(#run{seq=Seq, proj=Proj}, SeedInt) ->
    lamport_clock:init(),
    lamport_clock:incr(),
    LPN = pick_an_LPN(Seq, SeedInt),
    ?LOG({read, LPN},
         begin
             Res = read_result_mangle(corfurl:read_page(Proj, LPN)),
             perhaps_trip_read_approx(?TRIP_bad_read, Res, LPN)
         end).

scan_forward(#run{seq=Seq, proj=Proj}, SeedInt, NumPages) ->
    lamport_clock:init(),
    lamport_clock:incr(),
    StartLPN = if SeedInt == 1 -> 1;
                  true         -> pick_an_LPN(Seq, SeedInt)
               end,
    %% Our job is complicated by the ?LOG() macro, which isn't good enough
    %% for our purpose: we must lie about the starting timestamp, to make
    %% it appear as if each LPN result that scan_forward() gives us came
    %% instead from a single-page read_page() call.
    ?LOG({scan_forward, StartLPN, NumPages},
         begin
             TS1 = lamport_clock:get(),
             case corfurl:scan_forward(Proj, StartLPN, NumPages) of
                 {ok, EndLPN, MoreP, Pages} ->
                     PageIs = lists:zip(Pages, lists:seq(1, length(Pages))),
                     TS2 = lamport_clock:get(),
                     [begin
                          PidI = {self(), s_f, I},
                          event_logger:event(log_make_call(PidI, {read, LPN}),
                                             TS1),
                          Pm = perhaps_trip_scan_forward(
                                 ?TRIP_bad_scan_forward, read_result_mangle(P),
                                 EndLPN),
                          event_logger:event(log_make_result(PidI, Pm), TS2)
                      end || {{LPN, P}, I} <- PageIs],
                     Ps = [{LPN, read_result_mangle(P)} ||
                              {LPN, P} <- Pages],
                     {ok, EndLPN, MoreP, Ps}
             end
         end).

fill(#run{seq=Seq, proj=Proj}, SeedInt) ->
    lamport_clock:init(),
    lamport_clock:incr(),
    LPN = pick_an_LPN(Seq, SeedInt),
    ?LOG({fill, LPN},
         begin
             Res = corfurl:fill_page(Proj, LPN),
             perhaps_trip_fill_page(?TRIP_bad_fill, Res, LPN)
         end).

trim(#run{seq=Seq, proj=Proj}, SeedInt) ->
    lamport_clock:init(),
    lamport_clock:incr(),
    LPN = pick_an_LPN(Seq, SeedInt),
    ?LOG({trim, LPN},
         begin
             Res = corfurl:trim_page(Proj, LPN),
             perhaps_trip_trim_page(?TRIP_bad_trim, Res, LPN)
         end).

perhaps_trip_append_page(false, Res, _Page) ->
    Res;
perhaps_trip_append_page(true, {ok, LPN}, _Page) when LPN > 3 ->
    io:format(user, "TRIP: append_page\n", []),
    {ok, 3};
perhaps_trip_append_page(true, Else, _Page) ->
    Else.

perhaps_trip_read_approx(false, Res, _LPN) ->
    Res;
perhaps_trip_read_approx(true, _Res, 3 = LPN) ->
    io:format(user, "TRIP: read_approx LPN ~p\n", [LPN]),
    <<"FAKE!">>;
perhaps_trip_read_approx(true, Res, _LPN) ->
    Res.

perhaps_trip_scan_forward(false, Res, _EndLPN) ->
    Res;
perhaps_trip_scan_forward(true, _Res, 10) ->
    io:format(user, "TRIP: scan_forward\n", []),
    <<"magic number bingo, you are a winner">>;
perhaps_trip_scan_forward(true, Res, _EndLPN) ->
    Res.

perhaps_trip_fill_page(false, Res, _EndLPN) ->
    Res;
perhaps_trip_fill_page(true, _Res, LPN) when 3 =< LPN, LPN =< 5 ->
    io:format(user, "TRIP: fill_page\n", []),
    ok; % can trigger both invalid ttn and bad read
perhaps_trip_fill_page(true, Res, _EndLPN) ->
    Res.

perhaps_trip_trim_page(false, Res, _EndLPN) ->
    Res;
perhaps_trip_trim_page(true, _Res, LPN) when 3 =< LPN, LPN =< 5 ->
    io:format(user, "TRIP: trim_page\n", []),
    ok;
perhaps_trip_trim_page(true, Res, _EndLPN) ->
    Res.

-endif. % PULSE
-endif. % TEST

