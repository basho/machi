
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
    event_setup(),
    {ok, F1} = machi_flu0:start_link("one"),
    ProjNum = 1,
    ok = m_proj_write(F1, ProjNum, dontcare),

    Val = <<"val!">>,
    ok = m_write(F1, ProjNum, Val),
    {error_stale_projection, ProjNum} = m_write(F1, ProjNum - 1, Val),

    Me = self(),
    TrimFun = fun() -> Res = m_trim(F1, ProjNum),
                       Me ! {self(), Res}
               end,
    TrimPids = [spawn(TrimFun), spawn(TrimFun), spawn(TrimFun)],
    TrimExpected = [error_trimmed,error_trimmed,ok],

    GetFun = fun() -> Res = m_read(F1, ProjNum),
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

    ok = m_stop(F1),
    ok.
    
proj_store_test() ->
    event_setup(),
    {ok, F1} = machi_flu0:start_link("one"),

    error_unwritten = m_proj_get_latest_num(F1),
    error_unwritten = m_proj_read_latest(F1),

    Proj1 = whatever1,
    ok = m_proj_write(F1, 1, Proj1),
    error_written = m_proj_write(F1, 1, Proj1),
    {ok, Proj1} = m_proj_read(F1, 1),
    {ok, 1} = m_proj_get_latest_num(F1),
    {ok, Proj1} = m_proj_read_latest(F1),

    ok = m_stop(F1),
    ok.

wedge_test() ->
    event_setup(),
    {ok, F1} = machi_flu0:start_link("one"),
    ProjNum1 = 1,
    ok = m_proj_write(F1, ProjNum1, dontcare),

    Val = <<"val!">>,
    ok = m_write(F1, ProjNum1, Val),
    {error_stale_projection, ProjNum1} = m_write(F1, ProjNum1 - 1, Val),
    error_wedged = m_write(F1, ProjNum1 + 1, Val),
    %% Until we write a newer/bigger projection, all ops are error_wedged
    error_wedged = m_read(F1, ProjNum1),
    error_wedged = m_write(F1, ProjNum1, Val),
    error_wedged = m_trim(F1, ProjNum1),

    ProjNum2 = ProjNum1 + 1,
    ok = m_proj_write(F1, ProjNum2, dontcare),
    {ok, Val} = m_read(F1, ProjNum2),
    error_written = m_write(F1, ProjNum2, Val),
    ok = m_trim(F1, ProjNum2),
    error_trimmed = m_trim(F1, ProjNum2),

    ok = m_stop(F1),
    _XX = event_get_all(), io:format(user, "XX ~p\n", [_XX]),
    event_shutdown(),
    ok.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

m_write(Pid, ProjNum1, Val) ->
    Res = machi_flu0:write(Pid, ProjNum1, Val),
    event_add(write, Pid, Res),
    Res.

m_read(Pid, ProjNum) ->
    Res = machi_flu0:read(Pid, ProjNum),
    event_add(get, Pid, Res),
    Res.

m_trim(Pid, ProjNum) ->
    Res = machi_flu0:trim(Pid, ProjNum),
    event_add(trim, Pid, Res),
    Res.

m_stop(Pid) ->
    Res = machi_flu0:stop(Pid),
    event_add(stop, Pid, Res),
    Res.

m_proj_write(Pid, ProjNum, Proj) ->
    Res = machi_flu0:proj_write(Pid, ProjNum, Proj),
    event_add(proj_write, Pid, Res),
    Res.

m_proj_read(Pid, ProjNum) ->
    Res = machi_flu0:proj_read(Pid, ProjNum),
    event_add(proj_read, Pid, Res),
    Res.

m_proj_get_latest_num(Pid) ->
    Res = machi_flu0:proj_get_latest_num(Pid),
    event_add(proj_get_latest_num, Pid, Res),
    Res.

m_proj_read_latest(Pid) ->
    Res = machi_flu0:proj_read_latest(Pid),
    event_add(proj_read_latest, Pid, Res),
    Res.

event_setup() ->
    Tab = ?MODULE,
    ok = event_shutdown(),
    ets:new(Tab, [named_table, ordered_set, public]).

event_shutdown() ->
    Tab = ?MODULE,
    (catch ets:delete(Tab)),
    ok.

event_add(Key, Who, Description) ->
    Tab = ?MODULE,
    ets:insert(Tab, {lamport_clock:get(), Key, Who, Description}).

event_get_all() ->
    Tab = ?MODULE,
    ets:tab2list(Tab).

-endif.
-endif.
