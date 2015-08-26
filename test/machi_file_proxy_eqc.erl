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

position(P) ->
    ?LET(O, offset(), P + O).

offset() ->
    ?SUCHTHAT(X, int(), X >= 0).

offset_base() ->
    elements([4096, 6144, 7168, 8192, 20480, 100000, 1000000]).

big_offset() ->
    ?LET(P, int(), ?LET(X, offset_base(), P+X)).

len() ->
    ?SUCHTHAT(X, int(), X >= 1).

data_with_csum() ->
    ?LET({B,T},{eqc_gen:largebinary(), csum_type()}, {B,T, csum(T, B)}).
    %?LET({B,T},{eqc_gen:binary(), csum_type()}, {B,T, csum(T, B)}).

data_with_csum(Limit) ->
    ?LET({B,T},{?LET(S, Limit, eqc_gen:largebinary(S)), csum_type()}, {B,T, csum(T, B)}).
    %?LET({B,T},{?LET(S, Limit, eqc_gen:binary(S)), csum_type()}, {B,T, csum(T, B)}).

maybe_gen_valid_write([{Off, L}]) ->
    {default(Off+L, offset()), len()};
maybe_gen_valid_write([{O1, L1}, {O2, L2}]) ->
    Pos = O1 + L1, % end of previous write
    case Pos == O2 of
        true ->
            %% The previous write ended where next write begins, so
            %% we'll pick the end of the next write and a random length
            {O2 + L2, len()};
        false ->
            {position(O2-Pos), len()}
    end;
maybe_gen_valid_write(_) ->
    {big_offset(), len()}.
    
    

%% INITIALIZATION

-record(state, {pid, file = 0, written=[]}).

initial_state() -> #state{written=[{0,1024}]}.

%% HELPERS

%% check if an operation is permitted based on whether a write has
%% occurred
check_writes(_Op, [], _Off, _L) ->
    false;
check_writes(_Op, [{Pos, Sz}|_T], Off, L) when Pos == Off 
                                          andalso Sz == L ->
    mostly_true;
check_writes(read, [{Pos, Sz}|_T], Off, L) when Off >= Pos 
                                          andalso Off < (Pos + Sz) 
                                          andalso Sz >= ( L - ( Off - Pos ) ) ->
    true;
check_writes(write, [{Pos, Sz}|_T], Off, L) when ( Off + L ) > Pos 
                                                 andalso Off < (Pos + Sz) ->
    true;
check_writes(Op, [_H|T], Off, L) ->
    check_writes(Op, T, Off, L).

is_error({error, _}) -> true;
is_error({error, _, _}) -> true;
is_error(Other) -> {expected_ERROR, Other}.

probably_error(ok) -> true;
probably_error(V) -> is_error(V).

is_ok({ok, _, _}) -> true;
is_ok(ok) -> true;
is_ok(Other) -> {expected_OK, Other}.

get_offset({ok, _Filename, Offset}) -> Offset;
get_offset(_) -> error(badarg).

offset_valid(Offset, L) ->
    {Pos, Sz} = lists:last(L),
    Offset == Pos + Sz.

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
        [{0, 1024}] -> false;
        W -> check_writes(read, W, Off, L)
    end.

read_post(S, [_Pid, Off, L], Res) ->
    case read_ok(S, Off, L) of
        true -> is_ok(Res);
        mostly_true -> is_ok(Res);
        false -> is_error(Res)
    end.

read_next(S, _Res, _Args) -> S.

read(Pid, Offset, Length) ->
    machi_file_proxy:read(Pid, Offset, Length).

%% write

write_pre(S) ->
    S#state.pid /= undefined.

%% do not allow writes with empty data
write_pre(_S, [_Pid, _Extra, {<<>>, _Tag, _Csum}]) ->
    false;
write_pre(_S, _Args) ->
    true.

write_args(S) ->
    %{Offset, Length} = maybe_gen_valid_write(S#state.written),
    [S#state.pid, big_offset(), data_with_csum()].

write_ok(_S, [_Pid, Off, _Data]) when Off < 1024 -> false;
write_ok(S, [_Pid, Off, {Bin, _Tag, _Csum}]) ->
    Size = iolist_size(Bin),
    %% Check writes checks if a byte range is *written*
    %% So writes are ok IFF they are NOT written, so
    %% we want not check_writes/3 to be true.
    check_writes(write, S#state.written, Off, Size).

write_post(S, Args, Res) ->
    case write_ok(S, Args) of
        %% false means this range has NOT been written before, so
        %% it should succeed
        false -> eq(Res, ok);
        %% mostly true means we've written this range before BUT
        %% as a special case if we get a call to write the EXACT
        %% same data that's already on the disk, we return "ok"
        %% instead of {error, written}.
        mostly_true -> probably_error(Res); 
        %% If we get true, then we've already written this section 
        %% or a portion of this range to disk and should return an 
        %% error.
        true -> is_error(Res)
    end.

write_next(S, Res, [_Pid, Offset, {Bin, _Tag, _Csum}]) ->
    case is_ok(Res) of
        true -> 
            S#state{written = lists:sort(S#state.written ++ [{Offset, iolist_size(Bin)}])};
        _ -> 
            S
    end.

write(Pid, Offset, {Bin, Tag, Csum}) ->
    Meta = [{client_csum_tag, Tag},
            {client_csum, Csum}],
    machi_file_proxy:write(Pid, Offset, Meta, Bin).

%% append

append_pre(S) ->
    S#state.pid /= undefined.

%% do not allow appends with empty binary data
append_pre(_S, [_Pid, _Extra, {<<>>, _Tag, _Csum}]) ->
    false;
append_pre(_S, _Args) ->
    true.

append_args(S) ->
    [S#state.pid, default(0, len()), data_with_csum()].

append(Pid, Extra, {Bin, Tag, Csum}) ->
    Meta = [{client_csum_tag, Tag},
            {client_csum, Csum}],
    machi_file_proxy:append(Pid, Meta, Extra, Bin).

append_next(S, Res, [_Pid, _Extra, {Bin, _Tag, _Csum}]) ->
    case is_ok(Res) of
        true -> 
            Offset = get_offset(Res),
            true == offset_valid(Offset, S#state.written),
            S#state{written = lists:sort(S#state.written ++ [{Offset, iolist_size(Bin)}])};
        _ ->
            S
    end.

%% appends should always succeed unless the disk is full 
%% or there's a hardware failure.
append_post(_S, _Args, Res) ->
    true == is_ok(Res).

%% Property

prop_ok() ->
  cleanup(),
  ?FORALL(Cmds, commands(?MODULE),
  begin
    {H, S, Res} = run_commands(?MODULE, Cmds),
    pretty_commands(?MODULE, Cmds, {H, S, Res},
      aggregate(command_names(Cmds), Res == ok))
  end).

-endif. % EQC
-endif. % TEST
