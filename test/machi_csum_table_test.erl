-module(machi_csum_table_test).
-compile(export_all).

smoke_test() ->
    Filename = "./temp-checksum-dumb-file",
    _ = file:delete(Filename),
    {ok, MC} = machi_csum_table:open(Filename, []),
    {Offset, Size, Checksum} = {64, 34, <<"deadbeef">>},
    {error, unknown_chunk} = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:write(MC, Offset, Size, Checksum),
    {ok, Checksum} = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:trim(MC, Offset, Size),
    {error, trimmed} = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:close(MC),
    ok = machi_csum_table:delete(MC).

close_test() ->
    Filename = "./temp-checksum-dumb-file-2",
    _ = file:delete(Filename),
    {ok, MC} = machi_csum_table:open(Filename, []),
    {Offset, Size, Checksum} = {64, 34, <<"deadbeef">>},
    {error, unknown_chunk} = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:write(MC, Offset, Size, Checksum),
    {ok, Checksum} = machi_csum_table:find(MC, Offset, Size),
    ok = machi_csum_table:close(MC),

    {ok, MC2} = machi_csum_table:open(Filename, []),
    {ok, Checksum} = machi_csum_table:find(MC2, Offset, Size),
    ok = machi_csum_table:trim(MC2, Offset, Size),
    {error, trimmed} = machi_csum_table:find(MC2, Offset, Size),
    ok = machi_csum_table:delete(MC2).
