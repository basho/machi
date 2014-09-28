
-module(machi_flu0_test).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-ifdef(TEST).
-ifndef(PULSE).

concuerror1_test() ->
    ok.

concuerror2_test() ->
    {ok, F} = machi_flu0:start_link("one"),
    ok = machi_flu0:stop(F),
    ok.

concuerror3_test() ->
    Me = self(),
    Fun = fun() -> {ok, F1} = machi_flu0:start_link("one"),
                        ok = machi_flu0:stop(F1),
                        Me ! done
               end,
    P1 = spawn(Fun),
    P2 = spawn(Fun),
    [receive done -> ok end || _ <- [P1, P2]],
    
    ok.

concuerror4_test() ->
    {ok, F1} = machi_flu0:start_link("one"),
    Val = <<"val!">>,
    ok = machi_flu0:write(F1, Val),
    Me = self(),
    TrimFun = fun() -> Res = machi_flu0:trim(F1),
                       Me ! {self(), Res}
               end,
    TrimPids = [spawn(TrimFun), spawn(TrimFun),spawn(TrimFun)],
    TrimExpected = [error_trimmed,error_trimmed,ok],

    GetFun = fun() -> Res = machi_flu0:get(F1),
                      Me ! {self(), Res}
               end,
    GetPids = [spawn(GetFun)],
    GetExpected = fun(Results) ->
                          [] = [X || X <- Results, X == unwritten],
                          ok
                  end,

    TrimResults = lists:sort([receive
                                  {TrimPid, Res} -> Res
                              end || TrimPid <- TrimPids]),
    TrimExpected = TrimResults,
    GetResults = lists:sort([receive
                                 {GetPid, Res} -> Res
                             end || GetPid <- GetPids]),
    ok = GetExpected(GetResults),

    ok = machi_flu0:stop(F1),
    ok.
    
proj_store_test() ->
    {ok, F1} = machi_flu0:start_link("one"),

    error_unwritten = machi_flu0:proj_get_latest_num(F1),
    error_unwritten = machi_flu0:proj_read_latest(F1),

    Proj1 = whatever1,
    ok = machi_flu0:proj_write(F1, 1, Proj1),
    error_written = machi_flu0:proj_write(F1, 1, Proj1),
    {ok, Proj1} = machi_flu0:proj_read(F1, 1),
    {ok, 1} = machi_flu0:proj_get_latest_num(F1),
    {ok, Proj1} = machi_flu0:proj_read_latest(F1),

    ok = machi_flu0:stop(F1),
    ok.

-endif.
-endif.
