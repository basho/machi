-module(machi_plist_test).

-include_lib("eunit/include/eunit.hrl").

open_close_test() ->
    FileName = "bark-bark-one",
    file:delete(FileName),
    {ok, PList0} = machi_plist:open(FileName, []),
    {ok, PList1} = machi_plist:add(PList0, "boomar"),
    ?assertEqual(["boomar"], machi_plist:all(PList1)),
    ok = machi_plist:close(PList1),

    {ok, PList2} = machi_plist:open(FileName, []),
    ?assertEqual(["boomar"], machi_plist:all(PList2)),
    ok = machi_plist:close(PList2),
    file:delete(FileName),
    ok.
