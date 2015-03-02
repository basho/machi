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
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
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

