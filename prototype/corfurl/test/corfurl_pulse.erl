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

command(#state{run=Run} = S) ->
    ?LET({NumChains, ChainLen, PageSize},
         {parameter(num_chains), parameter(chain_len), parameter(page_size)},
    frequency(
      [{10, {call, ?MODULE, setup, [NumChains, ChainLen, PageSize]}}
       || not S#state.is_setup] ++
      [{10, {call, ?MODULE, append, [Run, gen_page(PageSize)]}}
       || S#state.is_setup] ++
      [])).

%% Precondition, checked before a command is added to the command sequence.
precondition(S, {call, _, setup, _}) ->
    not S#state.is_setup;
precondition(S, {call, _, _, _}) ->
    S#state.is_setup.

%% Next state transformation, S is the current state and V is the result of the
%% command.
next_state(S, Res, {call, _, setup, [NumChains, ChainLen, PageSize]}) ->
    S#state{is_setup=true,
            num_chains=NumChains,
            chain_len=ChainLen,
            page_size=PageSize,
            run=Res};
next_state(S, _, {call, _, append, _}) ->
    S.

eqeq(X, X) -> true;
eqeq(X, Y) -> {X, '/=', Y}.

postcondition(_S, {call, _, setup, _}, #run{} = _V) ->
    true;
postcondition(_S, {call, _, append, _}, {ok, LPN}) when is_integer(LPN) ->
    true;
postcondition(_S, {call, _, append, _}, V) ->
    eqeq(V, todoTODO_fixit).

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


%% If you want to see PULSE causing crazy scheduling, then
%% use this code instead of the usual stuff.
%% check_trace(Trace, Cmds, _Seed) ->
%%     Results = [X || {_TS, {result, _Pid, X}} <- Trace],
%%     {CmdsSeq, CmdsPars} = Cmds,
%%     NaiveCmds = CmdsSeq ++ lists:flatten(CmdsPars),
%%     NaiveCommands = [{Sym, Args} || {set,_,{call,_,Sym,Args}} <- NaiveCmds],
%%     NaiveAppends = [X || {append, _} = X <- NaiveCommands],
%%     conjunction(
%%       [{identity, equals(NaiveAppends, NaiveAppends)},
%%        {bogus_order_check_do_not_use_me, equals(Results, lists:usort(Results))}]).

%% Example Trace (raw event info, from the ?LOG macro)
%%
%% [{32014,{call,<0.467.0>,{append,<<"O">>}}},
%%  {32421,{call,<0.466.0>,{append,<<134>>}}},
%%  {44522,{result,<0.467.0>,{ok,1}}},
%%  {47651,{result,<0.466.0>,{ok,2}}}]

check_trace(Trace, _Cmds, _Seed) ->
    Events = eqc_temporal:from_timed_list(Trace),
    %% Example Events, temporal style, 1 usec resolution, same as original trace
    %%
    %% [{0,32014,[]},
    %%  {32014,32015,[{call,<0.467.0>,{append,<<"O">>}}]},
    %%  {32015,32421,[]},
    %%  {32421,32422,[{call,<0.466.0>,{append,<<134>>}}]},
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
    %%  {32014,32421,[{call,<0.467.0>,{append,<<"O">>}}]},
    %%  {32421,44522,
    %%   [{call,<0.466.0>,{append,<<134>>}},{call,<0.467.0>,{append,<<"O">>}}]},
    %%  {44522,47651,[{call,<0.466.0>,{append,<<134>>}}]},
    %%  {47651,infinity,[]}]

    AppendResultFilter = fun({ok, LPN}) -> LPN;
                            (Else)      -> Else end,
    AppendResults = eqc_temporal:stateful(
               fun({call, Pid, Call}) -> [{call, Pid, Call}] end,
               fun({call, Pid, {append, _Pg}}, {result, Pid, Res}) ->
                       [AppendResultFilter(Res)] end,
               Events),

    %% Desired properties
    AllCallsFinish = eqc_temporal:is_false(eqc_temporal:all_future(Calls)),
    NoAppendLPNDups = lists:sort(AppendResults) == lists:usort(AppendResults),

    conjunction(
      [
       {all_calls_finish, AllCallsFinish},
       {no_append_duplicates, NoAppendLPNDups}
      ]).

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

setup(NumChains, ChainLen, PageSize) ->
    N = NumChains * ChainLen,
    FLUs = corfurl_test:setup_basic_flus(N, PageSize, 50000),
    {ok, Seq} = corfurl_sequencer:start_link(FLUs),
    Chains = make_chains(ChainLen, FLUs),
    %% io:format(user, "Cs = ~p\n", [Chains]),
    Proj = corfurl:new_simple_projection(1, 1, 50000, Chains),
    #run{seq=Seq, proj=Proj, flus=FLUs}.

-define(LOG(Tag, MkCall),
        event_logger:event({call, self(), Tag}),
        LOG__Result = MkCall,
        event_logger:event({result, self(), LOG__Result}),
        LOG__Result).

append(#run{seq=Seq,proj=Proj}, Page) ->
    ?LOG({append, Page},
         corfurl:append_page(Seq, Proj, Page)).

-endif. % PULSE
-endif. % TEST

