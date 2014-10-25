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
-module(machi_util).

-export([repair_merge/1]).

-ifdef(TEST).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

%% repair_merge(): Given a list of lists of {Filename, StartOff, EndOff, FLU},
%% merge them into a single list of {Filename, StartOff, EndOff, FLU_list}
%% where the FLUs in FLU_list all have a copy of {Filename, StartOff, EndOff}.

repair_merge(ListOfLists) ->
    repair_merge2(lists:sort(lists:append(ListOfLists))).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% repair_merge2() invariant, to be "enforced"/honored by the caller:
%% L __must__ must be sorted.

repair_merge2([]=L) ->
    L;
repair_merge2([_]=L) ->
    L;
repair_merge2([H1, H2|T]) ->
    repair_merge_pair(H1, H2, T).

repair_merge_pair({F1, _, _, _}=H1, {F2, _, _, _}=H2, T)
  when F1 /= F2 ->
    %% No overlap: different files
    [H1|repair_merge2([H2|T])];
repair_merge_pair({_F1, _P1a, P1z, _M1s}=H1, {_F2, P2a, _P2z, _}=H2, T)
  when P1z < P2a ->
    %% No overlap: same file, H1 is strictly earlier than H2
    [H1|repair_merge2([H2|T])];
repair_merge_pair({F1, P1a, P1z, M1s}=_H1, {F2, P2a, P2z, M2s}=_H2, T)
  when F1 == F2, P1a == P2a, P1z == P2z ->
    %% Exact file & range: merge Ms
    NewMs = lists:usort(M1s ++ M2s),
    repair_merge2([{F1, P1a, P1z, NewMs}|T]);
repair_merge_pair(F1, F2, T) ->
    Split = split_overlapping(F1, F2),
    %% If we don't sort *everything* at this step, then we can end up
    %% with an invariant violation for repair_merge2(), which is that
    %% all items in L __must__ be sorted.
    repair_merge2(lists:sort(Split ++ T)).

split_overlapping({F1, _, _, _}=H1, {F2, _, _, _}=H2) when F1 /= F2 ->
    %% These are different files, why were we called?
    throw({whaaa, H1, H2}),
    [H1, H2];
split_overlapping({F, F1a, F1z, M1s}=H1, {F, F2a, F2z, M2s}=H2) when H1 =< H2 ->
    if F1a == F2a, F1z == F2z ->
            %% These are the same, why were we called?
            [{F, F1a, F1z, lists:usort(M1s ++ M2s)}];
       F1a == F2a ->
            %% 100% overlap, starting at the beginning of H1
            [{F, F2a, F1z, lists:usort(M1s ++ M2s)},
             {F, F1z + 1, F2z, M2s}];
       F1z == F2z ->
            %% 100% overlap, ending at the end of H1
            [{F, F1a, F2a - 1, M1s},
             {F, F2a, F1z, lists:usort(M1s ++ M2s)}];
       F2a < F1z, F2z < F1z ->
            %% 100% overlap, H2 is in the middle of H1
            [{F, F1a, F2a - 1, M1s},
             {F, F2a, F2z, lists:usort(M1s ++ M2s)},
             {F, F2z + 1, F1z, M1s}];
       true ->
            %% partial overlap
            [{F, F1a, F2a - 1, M1s},
             {F, F2a, F1z, lists:usort(M1s ++ M2s)},
             {F, F1z + 1, F2z, M2s}]
    end;
split_overlapping(H1, H2) ->
    split_overlapping(H2, H1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

-ifdef(EQC).

repair_merge_test() ->
    true = eqc:quickcheck(prop_repair_merge()).

prop_repair_merge() ->
    ?FORALL(S, gen_written_sequences(),
            begin
                Merged = repair_merge(S),
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

-endif. % EQC
-endif. % TEST
