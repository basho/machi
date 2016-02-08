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

-define(TESTDIR, "./eqc").

%% EUNIT TEST DEFINITION
eqc_test_() ->
    PropTimeout = case os:getenv("EQC_TIME") of
                      false -> 30;
                      V     -> list_to_integer(V)
                  end,
    {timeout, PropTimeout*2 + 30,
     {spawn,
      [
       ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(PropTimeout, ?QC_OUT(prop_ok()))))
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
    %?LET({B,T},{?LET(S, Limit, eqc_gen:largebinary(S)), csum_type()}, {B,T, csum(T, B)}).
    ?LET({B,T},{?LET(S, Limit, eqc_gen:binary(S)), csum_type()}, {B,T, csum(T, B)}).

intervals([]) ->
    [];
intervals([N]) ->
    [{N, choose(1,1)}];
intervals([A,B|T]) ->
    [{A, oneof([choose(1, B-A), B-A])}|intervals([B|T])].

interval_list() ->
    ?LET(L,
         oneof([list(choose(1025, 1033)), list(choose(1024, 4096))]),
         intervals(lists:usort(L))).

shuffle_interval() ->
    ?LET(L, interval_list(), shuffle(L)).

get_written_interval(L) ->
    ?LET({O, Ln}, elements(L), {O+1, Ln-1}).

%% INITIALIZATION

-record(state, {pid, prev_extra = 0,
                filename = undefined,
                planned_writes=[],
                planned_trims=[],
                written=[],
                trimmed=[]}).

initial_state() ->
    {_, _, MS} = os:timestamp(),
    Filename = test_server:temp_name("eqc_data") ++ "." ++ integer_to_list(MS),
    #state{filename=Filename, written=[{0,1024}]}.

initial_state(I, T) ->
    S=initial_state(),
    S#state{written=[{0,1024}],
            planned_writes=I,
            planned_trims=T}.

weight(_S, rewrite) -> 1;
weight(_S, _) -> 2.

%% HELPERS

get_overlaps(_Offset, _Len, [], Acc) -> lists:reverse(Acc);
get_overlaps(Offset, Len, [{Pos, Sz} = Ck|T], Acc0)
%% Overlap judgement differnt from the one in machi_csum_table
%% [a=Offset, b), [x=Pos, y) ...
  when
      %% a =< x && x < b && b =< y
      (Offset =< Pos andalso Pos < Offset + Len andalso Offset + Len =< Pos + Sz) orelse
      %% a =< x          && y < b
      (Offset =< Pos andalso Pos + Sz < Offset + Len) orelse
      %% x < a && a < y  && y =< b
      (Pos < Offset andalso Offset < Pos + Sz andalso Pos + Sz =< Offset + Len) orelse
      %% x < a           && b < y
      (Pos < Offset + Len andalso Offset + Len < Pos + Sz) ->
    get_overlaps(Offset, Len, T, [Ck|Acc0]);
get_overlaps(Offset, Len, [_Ck|T], Acc0) ->
    get_overlaps(Offset, Len, T, Acc0).

%% Inefficient but simple easy code to verify by eyes - returns all
%% bytes that fits in (Offset, Len)
chop(Offset, Len, List) ->
    ChopLeft = fun({Pos, Sz}) when Pos < Offset andalso Offset =< Pos + Sz ->
                       {Offset, Sz + Pos - Offset};
                  ({Pos, Sz}) when Offset =< Pos andalso Pos + Sz < Offset + Len ->
                       {Pos, Sz};
                  ({Pos, _Sz}) when Offset =< Pos ->
                       {Pos, Offset + Len - Pos}
               end,
    ChopRight = fun({Pos, Sz}) when Offset + Len < Pos + Sz ->
                        {Pos, Offset + Len - Pos};
                   ({Pos, Sz}) ->
                        {Pos, Sz}
                end,
    Filter0 = fun({_, 0}) -> false;
                 (Other) -> {true, Other} end,
    lists:filtermap(fun(E) -> Filter0(ChopRight(ChopLeft(E))) end,
                    List).

%% Returns all bytes that are at left side of the Offset
chopped_left(_Offset, []) -> undefined;
chopped_left(Offset, [{Pos,_Sz}|_]) when Pos < Offset ->
    {Pos, Offset - Pos};
chopped_left(_, _) ->
    undefined.

chopped_right(_Offset, []) -> undefined;
chopped_right(Offset, List) ->
    {Pos, Sz} = lists:last(List),
    if Offset < Pos + Sz ->
            {Offset, Pos + Sz - Offset};
       true ->
            undefined
    end.

cleanup_chunk(Offset, Length, ChunkList) ->
    Overlaps = get_overlaps(Offset, Length, ChunkList, []),
    NewCL0 = lists:foldl(fun lists:delete/2,
                         ChunkList, Overlaps),
    NewCL1 = case chopped_left(Offset, Overlaps) of
                 undefined -> NewCL0;
                 LeftRemain -> [LeftRemain|NewCL0]
             end,
    NewCL2 = case chopped_right(Offset+Length, Overlaps) of
                 undefined -> NewCL1;
                 RightRemain -> [RightRemain|NewCL1]
             end,
    lists:sort(NewCL2).

is_error({error, _}) -> true;
is_error({error, _, _}) -> true;
is_error(Other) -> {expected_ERROR, Other}.

is_ok({ok, _, _}) -> true;
is_ok(ok) -> true;
is_ok(Other) -> {expected_OK, Other}.

get_offset({ok, _Filename, Offset}) -> Offset;
get_offset(_) -> error(badarg).

last_byte([]) -> 0;
last_byte(L0) ->
    L1 = lists:map(fun({Pos, Sz}) -> Pos + Sz end, L0),
    lists:last(lists:sort(L1)).

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
    S#state.pid =:= undefined.

start_command(S) ->
    {call, ?MODULE, start, [S]}.

start(#state{filename=File}) ->
    {ok, Pid} = machi_file_proxy:start_link(some_flu, File, ?TESTDIR),
    unlink(Pid),
    Pid.

start_next(S, Pid, _) ->
    S#state{pid = Pid}.

%% read

read_pre(S) ->
    S#state.pid /= undefined.

read_args(S) ->
    [S#state.pid, oneof([offset(), big_offset()]), len()].

read_post(S, [_Pid, Off, L], Res) ->
    Written = get_overlaps(Off, L, S#state.written, []),
    Chopped = chop(Off, L, Written),
    Trimmed = get_overlaps(Off, L, S#state.trimmed, []),
    Eof = lists:max([Pos+Sz||{Pos,Sz}<-S#state.written]),
    case Res of
        {ok, {Written0, Trimmed0}} ->
            Written1 = lists:map(fun({_, Pos, Chunk, _}) ->
                                         {Pos, iolist_size(Chunk)}
                                 end, Written0),
            Trimmed1 = lists:map(fun({_, Pos, Sz}) -> {Pos, Sz} end, Trimmed0),
            Chopped =:= Written1
                andalso Trimmed =:= Trimmed1;
        %% TODO: such response are ugly, rethink the SPEC
        {error, not_written} when Eof < Off + L ->
            true;
        {error, not_written} when Chopped =:= [] andalso Trimmed =:= [] ->
            true;
        _Other ->
            is_error(Res)
    end.

read_next(S, _Res, _Args) -> S.

read(Pid, Offset, Length) ->
    machi_file_proxy:read(Pid, Offset, Length, [{needs_trimmed, true}]).

%% write

write_pre(S) ->
    S#state.pid /= undefined andalso S#state.planned_writes /= [].

%% do not allow writes with empty data
write_pre(_S, [_Pid, _Extra, {<<>>, _Tag, _Csum}]) ->
    ?assert(false),
    false;
write_pre(_S, _Args) ->
    true.

write_args(S) ->
    {Off, Len} = hd(S#state.planned_writes),
    [S#state.pid, Off, data_with_csum(Len)].

write_post(S, [_Pid, Off, {Bin, _Tag, _Csum}] = _Args, Res) ->
    Size = iolist_size(Bin),
    case {get_overlaps(Off, Size, S#state.written, []),
          get_overlaps(Off, Size, S#state.trimmed, [])} of
        {[], []} ->
            %% No overlap neither with written ranges nor trimmed
            %% ranges; OK to write things.
            eq(Res, ok);
        {_, _} ->
            %% overlap found in either or both at written or at
            %% trimmed ranges; can't write.
            is_error(Res)
    end.

write_next(S, Res, [_Pid, Offset, {Bin, _Tag, _Csum}]) ->
    S0 = case is_ok(Res) of
        true ->
            S#state{written = lists:sort(S#state.written ++ [{Offset, iolist_size(Bin)}]) };
        _ ->
            S
    end,
    S0#state{prev_extra = 0, planned_writes=tl(S0#state.planned_writes)}.


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

append_next(S, Res, [_Pid, Extra, {Bin, _Tag, _Csum}]) ->
    case is_ok(Res) of
        true ->
            Offset = get_offset(Res),
            S#state{prev_extra = Extra,
                    written = lists:sort(S#state.written ++ [{Offset, iolist_size(Bin)}])};
        _Other ->
            S
    end.

%% appends should always succeed unless the disk is full
%% or there's a hardware failure.
append_post(S, _Args, Res) ->
    case is_ok(Res) of
        true ->
            Offset = get_offset(Res),
            case erlang:max(last_byte(S#state.written),
                            last_byte(S#state.trimmed)) + S#state.prev_extra of
                Offset ->
                    true;
                UnexpectedByte ->
                    {wrong_offset_after_append,
                     {Offset, UnexpectedByte},
                     {S#state.written, S#state.prev_extra}}
            end;
        Error ->
            Error
    end.

%% rewrite

rewrite_pre(S) ->
    S#state.pid /= undefined andalso
        (S#state.written ++ S#state.trimmed) /= [] .

rewrite_args(S) ->
    ?LET({Off, Len},
         get_written_interval(S#state.written ++ S#state.trimmed),
         [S#state.pid, Off, data_with_csum(Len)]).

rewrite(Pid, Offset, {Bin, Tag, Csum}) ->
    Meta = [{client_csum_tag, Tag},
            {client_csum, Csum}],
    machi_file_proxy:write(Pid, Offset, Meta, Bin).

rewrite_post(_S, _Args, Res) ->
    is_error(Res).

rewrite_next(S, _Res, _Args) ->
    S#state{prev_extra = 0}.

%% trim

trim_pre(S) ->
    S#state.pid /= undefined andalso S#state.planned_trims /= [].

trim_args(S) ->
    {Offset, Length} = hd(S#state.planned_trims),
    [S#state.pid, Offset, Length].

trim(Pid, Offset, Length) ->
    machi_file_proxy:trim(Pid, Offset, Length, false).

trim_post(_S, [_Pid, _Offset, _Length], ok) ->
    true;
trim_post(_S, [_Pid, _Offset, _Length], _Res) ->
    false.

trim_next(S, Res, [_Pid, Offset, Length]) ->
    S1 = case is_ok(Res) of
             true ->
                 NewWritten = cleanup_chunk(Offset, Length, S#state.written),
                 Trimmed1 = cleanup_chunk(Offset, Length, S#state.trimmed),
                 NewTrimmed = lists:sort([{Offset, Length}|Trimmed1]),
                 S#state{trimmed=NewTrimmed,
                         written=NewWritten};
             _Other ->
                 S
         end,
    S1#state{prev_extra=0,
             planned_trims=tl(S#state.planned_trims)}.

stop_pre(S) ->
    S#state.pid /= undefined.

stop_args(S) ->
    [S#state.pid].

stop(Pid) ->
    catch machi_file_proxy:stop(Pid).

stop_post(_, _, _) -> true.

stop_next(S, _, _) ->
    S#state{pid=undefined, prev_extra=0}.

%% Property

prop_ok() ->
    cleanup(),
    ?FORALL({I, T},
            {shuffle_interval(), shuffle_interval()},
            ?FORALL(Cmds, parallel_commands(?MODULE, initial_state(I, T)),
                    begin
                        {H, S, Res} = run_parallel_commands(?MODULE, Cmds),
                        cleanup(),
                        pretty_commands(?MODULE, Cmds, {H, S, Res},
                                        aggregate(command_names(Cmds), Res == ok))
                    end)).

%% Test for tester functions
chopper_test_() ->
    [?_assertEqual([{0, 1024}],
                   get_overlaps(1, 1, [{0, 1024}], [])),
     ?_assertEqual([],
                   get_overlaps(10, 5, [{9, 1}, {15, 1}], [])),
     ?_assertEqual([{9,2},{14,1}],
                   get_overlaps(10, 5, [{9, 2}, {14, 1}], [])),
     ?_assertEqual([],       chop(0, 0, [{0,2}])),
     ?_assertEqual([{0, 1}], chop(0, 1, [{0,2}])),
     ?_assertEqual([],       chop(1, 0, [{0,2}])),
     ?_assertEqual([{1, 1}], chop(1, 1, [{0,2}])),
     ?_assertEqual([{1, 1}], chop(1, 2, [{0,2}])),
     ?_assertEqual([],       chop(2, 1, [{0,2}])),
     ?_assertEqual([],       chop(2, 2, [{0,2}])),
     ?_assertEqual([{1, 1}], chop(1, 3, [{0,2}])),
     ?_assertError(_,  chop(3, 1, [{0,2}])),
     ?_assertEqual([], chop(2, 3, [{0,2}])),
     ?_assertEqual({0, 1}, chopped_left(1, [{0, 1024}])),
     ?_assertEqual([{0, 1}, {2, 1022}], cleanup_chunk(1, 1, [{0, 1024}])),
     ?_assertEqual([{2, 1022}], cleanup_chunk(0, 2, [{0, 1}, {2, 1022}])),
     ?_assert(true)
    ].

-endif. % EQC
-endif. % TEST
