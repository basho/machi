-module(machi_csum_table_test).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-define(HDR, {0, 1024, <<0>>}).

smoke_test() ->
    Filename = "./temp-checksum-dumb-file",
    _ = file:delete(Filename),
    {ok, MC} = machi_csum_table:open(Filename, []),
    [{1024, infinity}] = machi_csum_table:calc_unwritten_bytes(MC),
    Entry = {Offset, Size, Checksum} = {1064, 34, <<"deadbeef">>},
    [] = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:write(MC, Offset, Size, Checksum),
    [{1024, 40}, {1098, infinity}] = machi_csum_table:calc_unwritten_bytes(MC),
    [Entry] = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:trim(MC, Offset, Size),
    [{Offset, Size, trimmed}] = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:close(MC),
    ok = machi_csum_table:delete(MC).

close_test() ->
    Filename = "./temp-checksum-dumb-file-2",
    _ = file:delete(Filename),
    {ok, MC} = machi_csum_table:open(Filename, []),
    Entry = {Offset, Size, Checksum} = {1064, 34, <<"deadbeef">>},
    [] = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:write(MC, Offset, Size, Checksum),
    [Entry] = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:close(MC),

    {ok, MC2} = machi_csum_table:open(Filename, []),
    [Entry] = machi_csum_table:find(MC2, Offset, Size),
    ok = machi_csum_table:trim(MC2, Offset, Size),
    [{Offset, Size, trimmed}] = machi_csum_table:find(MC2, Offset, Size),
    ok = machi_csum_table:delete(MC2).

smoke2_test() ->
    Filename = "./temp-checksum-dumb-file-3",
    _ = file:delete(Filename),
    {ok, MC} = machi_csum_table:open(Filename, []),
    Entry = {Offset, Size, Checksum} = {1025, 10, <<"deadbeef">>},
    ok = machi_csum_table:write(MC, Offset, Size, Checksum),
    [] = machi_csum_table:find(MC, 0, 0),
    [?HDR] = machi_csum_table:find(MC, 0, 1),
    [Entry] = machi_csum_table:find(MC, Offset, Size),
    [?HDR] = machi_csum_table:find(MC, 1, 1024),
    [?HDR, Entry] = machi_csum_table:find(MC, 1023, 1024),
    [Entry] = machi_csum_table:find(MC, 1024, 1024),
    [Entry] = machi_csum_table:find(MC, 1025, 1024),

    ok = machi_csum_table:trim(MC, Offset, Size),
    [{Offset, Size, trimmed}] = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:close(MC),
    ok = machi_csum_table:delete(MC).

%% TODO: add quickcheck test here
