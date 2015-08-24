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

-module(machi_file_proxy_eqc).

-ifdef(TEST).
-ifdef(EQC).
-compile(export_all).
-include("machi.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).


%% EUNIT TEST DEFINITION
eqc_test_() ->
    {timeout, 60,
     {spawn,
      [
        {timeout, 30, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(15, ?QC_OUT(prop_ok()))))}
      ]
     }}.

%% SHELL HELPERS
test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop_ok())).

check() ->
    check(prop_ok(), current_counterexample()).

%% GENERATORS

csum_type() ->
    elements([?CSUM_TAG_NONE, ?CSUM_TAG_CLIENT_SHA, ?CSUM_TAG_SERVER_SHA]).

csum(Type, Binary) ->
    case Type of
        ?CSUM_TAG_NONE -> <<>>;
        _ -> machi_util:checksum_chunk(Binary)
    end.

offset() ->
%    ?SUCHTHAT(X, oneof([largeint(), int()]), X >= 0).
    ?SUCHTHAT(X, int(), X >= 0).

len() ->
%    ?SUCHTHAT(X, oneof([largeint(), int()]), X >= 1).
    ?SUCHTHAT(X, int(), X >= 1).

data_with_csum() ->
    ?LET({B,T},{eqc_gen:largebinary(), csum_type()}, {B,T, csum(T, B)}).

small_data() ->
    ?LET(D, ?SUCHTHAT(S, int(), S >= 1 andalso S < 500), binary(D)).

%% INITIALIZATION

-record(state, {pid, file = 0, written=[]}).

initial_state() -> #state{written=[{0,1023}]}.

%% HELPERS

%% check if an operation is permitted based on whether a write has
%% occurred
check_writes([], _Off, _L) ->
    false;
check_writes([{Pos, Sz}|_T], Off, L) when Off >= Pos
                                           andalso Off < (Pos + Sz)
                                           andalso L < ( Sz - ( Off - Pos ) )->
    true;
check_writes([{Pos, Sz}|_T], Off, _L) when Off > ( Pos + Sz ) ->
    false;
check_writes([_H|T], Off, L) ->
    check_writes(T, Off, L).

is_error({error, _}) -> true;
is_error({error, _, _}) -> true;
is_error(Other) -> {expected_ERROR, Other}.

is_ok({ok, _, _}) -> true;
is_ok(ok) -> true;
is_ok(Other) -> {expected_OK, Other}.


-define(TESTDIR, "./eqc").

cleanup() ->
    [begin
         Fs = filelib:wildcard(?TESTDIR ++ Glob),
         [file:delete(F) || F <- Fs],
         [file:del_dir(F) || F <- Fs]
     end || Glob <- ["*/*/*/*", "*/*/*", "*/*", "*"] ],
    _ = file:del_dir(?TESTDIR),
    ok.

%% start

start_pre(S) ->
    S#state.pid == undefined.

start_command(S) ->
    {call, ?MODULE, start, [S]}.

start(S) ->
    File = test_server:temp_name("eqc_data") ++ "." ++ integer_to_list(S#state.file),
    {ok, Pid} = machi_file_proxy:start_link(File, ?TESTDIR),
    unlink(Pid),
    Pid.

start_next(S, Pid, _Args) ->
    S#state{pid = Pid, file = S#state.file + 1}.

%% read

read_pre(S) ->
    S#state.pid /= undefined.

read_args(S) ->
    [S#state.pid, offset(), len()].

read_ok(S, Off, L) ->
    case S#state.written of
        [] -> false;
        [{0, 1023}] -> false;
        W -> check_writes(W, Off, L)
    end.

read_post(S, [_Pid, Off, L], Res) ->
    case read_ok(S, Off, L) of
        true -> is_ok(Res);
        false -> is_error(Res)
    end.

read_next(S, _Res, _Args) -> S.

read(Pid, Offset, Length) ->
    machi_file_proxy:read(Pid, Offset, Length).

%% write

write_pre(S) ->
    S#state.pid /= undefined.

write_args(S) ->
    [S#state.pid, offset(), data_with_csum()].

write_ok(_S, [_Pid, Off, _Data]) when Off < 1024 -> false;
write_ok(S, [_Pid, Off, {Bin, _Tag, _Csum}]) ->
    Size = iolist_size(Bin),
    %% Check writes checks if a byte range is *written*
    %% So writes are ok IFF they are NOT written, so
    %% we want not check_writes/3 to be true.
    not check_writes(S#state.written, Off, Size).

write_post(S, Args, Res) ->
    case write_ok(S, Args) of
        true -> eq(Res, ok);
        false -> is_error(Res)
    end.

write_next(S, ok, [_Pid, Offset, {Bin, _Tag, _Csum}]) ->
    S#state{written = lists:sort(S#state.written ++ [{Offset, iolist_size(Bin)}])};
write_next(S, _Res, _Args) -> S.

write(Pid, Offset, {Bin, Tag, Csum}) ->
    Meta = [{client_csum_tag, Tag},
            {client_csum, Csum}],
    machi_file_proxy:write(Pid, Offset, Meta, Bin).

%% append
%% TODO - ensure offset is expected offset

append_pre(S) ->
    S#state.pid /= undefined.

append_args(S) ->
    [S#state.pid, default(0, len()), data_with_csum()].

append(Pid, Extra, {Bin, Tag, Csum}) ->
    Meta = [{client_csum_tag, Tag},
            {client_csum, Csum}],
    machi_file_proxy:append(Pid, Meta, Extra, Bin).

append_next(S, {ok, _File, Offset}, [_Pid, _Extra, {Bin, _Tag, _Csum}]) ->
    S#state{written = lists:sort(S#state.written ++ [{Offset, iolist_size(Bin)}])};
append_next(S, _Res, _Args) -> S.

%% appends should always succeed unless the disk is full 
%% or there's a hardware failure.
append_post(_S, _Args, Res) ->
    is_ok(Res).

%% Property

prop_ok() ->
  ?FORALL(Cmds, commands(?MODULE),
  begin
    cleanup(),
    %io:format(user, "Commands: ~p~n", [Cmds]),
    {H, S, Res} = run_commands(?MODULE, Cmds),
    pretty_commands(?MODULE, Cmds, {H, S, Res},
      aggregate(command_names(Cmds), Res == ok))
  end).

-endif. % EQC
-endif. % TEST
