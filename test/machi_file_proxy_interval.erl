-module(machi_file_proxy_interval).
-include_lib("eqc/include/eqc.hrl").
-compile(export_all).

%% [{0,1024}] ++ list(intervals())

intervals([]) ->
    [];
intervals([N]) ->
    [{N, choose(1,150)}];
intervals([A,B|T]) ->
    [{A, choose(1, B-A)}|intervals([B|T])].

interval_list() ->
    ?LET(L, list(choose(1024, 4096)), 
                 intervals(lists:usort(L))).

shuffle_interval() ->
    ?LET(L, interval_list(), shuffle(L)).

prop_no_overlap() ->
    ?FORALL(I, [{0,1024}|interval_list()],
            no_overlap(I, 0)).

no_overlap([], _Acc) -> true;
no_overlap([{Off, Len}|Rest], Acc) ->
    Acc =< Off andalso no_overlap(Rest, Off+Len).

