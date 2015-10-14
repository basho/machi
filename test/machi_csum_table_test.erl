-module(machi_csum_table_test).
-compile(export_all).

smoke_test() ->
    Filename = "./temp-checksum-dumb-file",
    {ok, MC} = machi_checksums:new(Filename),
    {Offset, Size, Checksum} = {64, 34, <<"deadbeef">>},
    {error, unwritten} = machi_checksums:find(MC, Offset, Size),
    ok = machi_checksums:write(MC, Offset, Size, Checksum),
    {ok, Checksum} = machi_checksums:find(MC, Offset, Size),
    ok = machi_checksums:trim(MC, Offset, Size),
    {error, trimmed} = machi_checksums:find(MC, Offset, Size),
    ok = machi_checksums:destroy(MC),
    ok = machi_checksums:delete_file(MC).

close_test() ->
    Filename = "./temp-checksum-dumb-file-2",
    {ok, MC} = machi_checksums:new(Filename),
    {Offset, Size, Checksum} = {64, 34, <<"deadbeef">>},
    {error, unwritten} = machi_checksums:find(MC, Offset, Size),
    ok = machi_checksums:write(MC, Offset, Size, Checksum),
    {ok, Checksum} = machi_checksums:find(MC, Offset, Size),
    ok = machi_checksums:destroy(MC),

    {ok, MC2} = machi_checksums:new(Filename),
    {ok, Checksum} = machi_checksums:find(MC2, Offset, Size),
    ok = machi_checksums:trim(MC2, Offset, Size),
    {error, trimmed} = machi_checksums:find(MC2, Offset, Size),
    ok = machi_checksums:delete_file(MC2).
