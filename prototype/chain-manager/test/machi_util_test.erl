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
-module(machi_util_test).

-include("machi.hrl").

-export([]).

-ifdef(TEST).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

repair_merge_test_() ->
    {timeout, 60,
     fun() ->
             true = eqc:quickcheck(eqc:numtests(300, ?QC_OUT(prop_repair_merge())))
     end}.

prop_repair_merge() ->
    ?FORALL(S, gen_written_sequences(),
            begin
                Merged = machi_util:repair_merge(S),
                check_repair_merge(S, Merged)
            end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_written_sequences() ->
    ?LET(FLUs, non_empty(list(gen_flu())),
         [gen_sequence_list(FLU) || FLU <- lists:usort(FLUs)]).

gen_sequence_list(FLU) ->
    ?LET(Files, non_empty(list(gen_file())),
    ?LET(Items, [gen_sequence_items(FLU, File) || File <- lists:usort(Files)],
         lists:append(Items))).

gen_flu() ->
    elements([a, b, c]).

gen_file() ->
    elements(["f1", "f2", "f3"]).

gen_sequence_items(FLU, File) ->
    %% Pairs = [{StartingOffset, # of bytes/pages/whatever} ...]
    ?LET(Pairs, non_empty(list({nat(), nat()})),
         begin
             %% Pairs2 = [{StartingOffset, EndingOffset} ...]
             Pairs2 = [{Start, Start + (Num div 2)} || {Start, Num} <- Pairs],
             %% Pairs3 = *sorted*, Pairs2 all overlapping offsets removed
             {_, Pairs3} =
                 lists:foldl(
                   fun({NewStart, NewEnd}, {OldEnd, Acc}) ->
                           if NewStart =< OldEnd ->
                                   {OldEnd, Acc};
                              true ->
                                   {NewEnd, [{File, NewStart, NewEnd, [FLU]}|Acc]}
                           end
                   end, {-1, []}, lists:sort(Pairs2)),
             %% Now combine any adjacent
             combine_adjacent(lists:reverse(Pairs3))
         end).

combine_adjacent([]=L) ->
    L;
combine_adjacent([_]=L) ->
    L;
combine_adjacent([{F1, P1a, P1z, M1s}, {F2, P2a, P2z, M2s}|T])
  when F1 == F2, P1z == P2a - 1 ->
    combine_adjacent([{F1, P1a, P2z, lists:usort(M1s ++ M2s)}|T]);
combine_adjacent([H|T]) ->
    [H|combine_adjacent(T)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_repair_merge(S, Merged) ->
    ?WHENFAIL(begin
                  io:format(user, "Input  = ~p\n", [S]),
                  io:format(user, "Merged = ~p\n", [Merged])
              end,
    conjunction([{piece_wise, compare_piece_wise_ok(S, Merged)},
                 {non_overlapping, check_strictly_non_overlapping(Merged)}])).

compare_piece_wise_ok(S, Merged) ->
    PieceWise1 = lists:sort(make_canonical_form(S)),

    PieceWise2 = make_canonical_form(Merged),
    if PieceWise1 == PieceWise2 ->
            true;
       true ->
            {correct, PieceWise1, wrong, PieceWise2}
    end.

check_strictly_non_overlapping(S) ->
    try
        [First|Rest] = S,
        lists:foldl(fun({F2, _F2a, _F2z, _}=New, {F1, _F1a, _F1z, _})
                          when F2 > F1 ->
                            New;
                       ({_F2, F2a, F2z, _}=New, {_F1, F1a, F1z, _})
                          when F2a > F1a, F2a > F1z,
                               F2z > F1a, F2z > F1z ->
                            New
                    end, First, Rest),
        true
    catch _:_ ->
            false
    end.

%% Given a list of XXX, we create a list of 1-byte/page/thing
%% graunularity items which is equivalent but in a canonical form to
%% make correctness testing easier.

make_canonical_form(ListOfLists) ->
    lists:sort(make_canonical_form2(lists:flatten(ListOfLists))).

make_canonical_form2([]) ->
    [];
make_canonical_form2([{File, Start, End, Members}|T]) ->
    [{File, Pos, Pos, Member} || Pos <- lists:seq(Start, End),
                                 Member <- Members] ++
        make_canonical_form2(T).

-endif. % TEST

