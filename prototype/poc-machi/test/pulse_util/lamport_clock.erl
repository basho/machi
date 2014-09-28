
-module(lamport_clock).

-export([init/0, get/0, update/1, incr/0]).

-define(KEY, ?MODULE).

-ifdef(TEST).

init() ->
    case get(?KEY) of
        undefined ->
            FakeTOD = 0,
            put(?KEY, FakeTOD + 1);
        N when is_integer(N) ->
            ok
    end.

get() ->
    init(),
    get(?KEY).

update(Remote) ->
    New = erlang:max(get(?KEY), Remote) + 1,
    put(?KEY, New),
    New.        

incr() ->
    New = get(?KEY) + 1,
    put(?KEY, New),
    New.

-else. % TEST

init() ->
    ok.

get() ->
    ok.

update(_) ->
    ok.

incr() ->
    ok.

-endif. % TEST
