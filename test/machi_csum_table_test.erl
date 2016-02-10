-module(machi_csum_table_test).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

cleanup(Dir) ->
    os:cmd("rm -rf " ++ Dir).

smoke_test() ->
    DBFile = "./temp-checksum-dumb-file",
    Filename = <<"/some/puppy/and/cats^^^42">>,
    _ = cleanup(DBFile),
    {ok, MC} = machi_csum_table:open(DBFile, []),
    ?assertEqual([{0, infinity}],
                 machi_csum_table:calc_unwritten_bytes(MC, Filename)),
    Entry = {Offset, Size, Checksum} = {1064, 34, <<"deadbeef">>},
    [] = machi_csum_table:find(MC, Filename, Offset, Size),
    ok = machi_csum_table:write(MC, Filename, Offset, Size, Checksum),
    [{0, 1064}, {1098, infinity}] = machi_csum_table:calc_unwritten_bytes(MC, Filename),
    ?assertEqual([Entry], machi_csum_table:find(MC, Filename, Offset, Size)),
    ok = machi_csum_table:trim(MC, Filename, Offset, Size, undefined, undefined),
    ?assertEqual([{Offset, Size, trimmed}],
                 machi_csum_table:find(MC, Filename, Offset, Size)),
    ok = machi_csum_table:close(MC).

close_test() ->
    DBFile = "./temp-checksum-dumb-file-2",
    Filename = <<"/some/puppy/and/cats^^^43">>,
    _ = cleanup(DBFile),
    {ok, MC} = machi_csum_table:open(DBFile, []),
    Entry = {Offset, Size, Checksum} = {1064, 34, <<"deadbeef">>},
    [] = machi_csum_table:find(MC, Filename, Offset, Size),
    ok = machi_csum_table:write(MC, Filename, Offset, Size, Checksum),
    [Entry] = machi_csum_table:find(MC, Filename, Offset, Size),
    ok = machi_csum_table:close(MC),

    {ok, MC2} = machi_csum_table:open(DBFile, []),
    [Entry] = machi_csum_table:find(MC2, Filename, Offset, Size),
    ok = machi_csum_table:trim(MC2, Filename, Offset, Size, undefined, undefined),
    [{Offset, Size, trimmed}] = machi_csum_table:find(MC2, Filename, Offset, Size),
    ok = machi_csum_table:close(MC2).

smoke2_test() ->
    DBFile = "./temp-checksum-dumb-file-3",
    Filename = <<"/some/puppy/and/cats^^^43">>,
    _ = cleanup(DBFile),
    {ok, MC} = machi_csum_table:open(DBFile, []),
    Entry = {Offset, Size, Checksum} = {1025, 10, <<"deadbeef">>},
    ok = machi_csum_table:write(MC, Filename, Offset, Size, Checksum),
    ?assertEqual([], machi_csum_table:find(MC, Filename, 0, 0)),
    ?assertEqual([], machi_csum_table:find(MC, Filename, 0, 1)),
    [Entry] = machi_csum_table:find(MC, Filename, Offset, Size),
    [] = machi_csum_table:find(MC, Filename, 1, 1024),
    ?assertEqual([Entry],
                 machi_csum_table:find(MC, Filename, 1023, 1024)),
    [Entry] = machi_csum_table:find(MC, Filename, 1024, 1024),
    [Entry] = machi_csum_table:find(MC, Filename, 1025, 1024),

    ok = machi_csum_table:trim(MC, Filename, Offset, Size, undefined, undefined),
    [{Offset, Size, trimmed}] = machi_csum_table:find(MC, Filename, Offset, Size),
    ok = machi_csum_table:close(MC).

smoke3_test() ->
    DBFile = "./temp-checksum-dumb-file-4",
    Filename = <<"/some/puppy/and/cats^^^44">>,
    _ = cleanup(DBFile),
    {ok, MC} = machi_csum_table:open(DBFile, []),
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
                       machi_csum_table:find_leftneighbor(MC, Filename, Offset)),
          ?assertEqual(RightN0,
                       machi_csum_table:find_rightneighbor(MC, Filename, Offset+Size)),
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
                  ok = machi_csum_table:write(MC, Filename, Offset, Size, Csum,
                                              LeftN, RightN);
              trim ->
                  ok = machi_csum_table:trim(MC, Filename, Offset, Size,
                                             LeftN, RightN)
          end
      end || {_Line, Cmd, Chunk, LeftN0, RightN0} <- Scenario ],
    ?assert(not machi_csum_table:all_trimmed(MC, Filename, 0, 10000)),
    machi_csum_table:trim(MC, Filename, 0, 10000, undefined, undefined),
    ?assert(machi_csum_table:all_trimmed(MC, Filename, 0, 10000)),

    ok = machi_csum_table:close(MC).

%% TODO: add quickcheck test here

%% Previous implementation
-spec all_trimmed2(machi_csum_table:table(),
                   non_neg_integer(), non_neg_integer()) -> boolean().
all_trimmed2(CsumT, Left, Right) ->
    Chunks = machi_csum_table:find(CsumT, Left, Right),
    runthru(Chunks, Left, Right).

%% @doc make sure all trimmed chunks are continously chained
%% TODO: test with EQC
runthru([], Pos, Pos) -> true;
runthru([], Pos0, Pos) when Pos0 < Pos -> false;
runthru([{Offset0, Size0, trimmed}|T], Offset, Pos) when Offset0 =< Offset ->
    runthru(T, Offset0+Size0, Pos);
runthru(_L, _O, _P) ->
    false.
