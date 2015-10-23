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


smoke3_test() ->
    Filename = "./temp-checksum-dumb-file-4",
    _ = file:delete(Filename),
    {ok, MC} = machi_csum_table:open(Filename, []),
    Scenario =
        [%% Command, {Offset, Size, Csum}, LeftNeighbor, RightNeibor
         {?LINE, write, {2000, 10, <<"heh">>}, undefined, undefined},
         {?LINE, write, {3000, 10, <<"heh">>}, undefined, undefined},
         {?LINE, write, {4000, 10, <<"heh2">>}, undefined, undefined},
         {?LINE, write, {4000, 10, <<"heh2">>}, undefined, undefined},
         {?LINE, write, {4005, 10, <<"heh3">>}, {4000, 5, <<"heh2">>}, undefined},
         {?LINE, write, {4005, 10, <<"heh3">>}, undefined, undefined},
         {?LINE, trim, {3005, 10, <<>>}, {3000, 5, <<"heh">>}, undefined},
         {?LINE, trim, {2000, 10, <<>>}, undefined, undefined},
         {?LINE, trim, {2005, 5, <<>>}, {2000, 5, trimmed}, undefined},
         {?LINE, trim, {3000, 5, <<>>}, undefined, undefined},
         {?LINE, trim, {4000, 10, <<>>}, undefined, {4010, 5, <<"heh3">>}},
         {?LINE, trim, {4010, 5, <<>>}, undefined, undefined},
         {?LINE, trim, {0, 1024, <<>>}, undefined, undefined}
        ],
    [ begin
          %% ?debugVal({_Line, Chunk}),
          {Offset, Size, Csum} = Chunk,
          ?assertEqual(LeftN0,
                       machi_csum_table:find_leftneighbor(MC, Offset)),
          ?assertEqual(RightN0,
                       machi_csum_table:find_rightneighbor(MC, Offset+Size)),
          LeftN = case LeftN0 of
                      {OffsL, SizeL, trimmed} -> {OffsL, SizeL, trimmed};
                      {OffsL, SizeL, _} -> {OffsL, SizeL, <<"boom">>};
                      OtherL -> OtherL
                  end,
          RightN = case RightN0 of
                       {OffsR, SizeR, _} -> {OffsR, SizeR, <<"boot">>};
                       OtherR -> OtherR
                   end,
          case Cmd of
              write ->
                  ok = machi_csum_table:write(MC, Offset, Size, Csum,
                                              LeftN, RightN);
              trim ->
                  ok = machi_csum_table:trim(MC, Offset, Size,
                                             LeftN, RightN)
          end
      end || {_Line, Cmd, Chunk, LeftN0, RightN0} <- Scenario ],
    ?assert(not machi_csum_table:all_trimmed(MC, 10000)),
    machi_csum_table:trim(MC, 0, 10000, undefined, undefined),
    ?assert(machi_csum_table:all_trimmed(MC, 10000)),

    ok = machi_csum_table:close(MC),
    ok = machi_csum_table:delete(MC).


%% TODO: add quickcheck test here
