%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(tango_test).

-compile(export_all).

-include("corfurl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.
-endif.

-define(SEQ, corfurl_sequencer).
-define(T, tango).

-define(D(X), io:format(user, "Dbg: ~s =\n  ~p\n", [??X, X])).

-ifdef(TEST).
-ifndef(PULSE).

pack_v1_test() ->
    [begin
         Packed = ?T:pack_v1(StreamList, Options, term_to_binary(Term), Size),
         StreamList = ?T:unpack_v1(Packed, stream_list),
         TermBin = ?T:unpack_v1(Packed, page),
         Term = binary_to_term(TermBin)
     end || StreamList <- [[], [1], [1,2,4]],
            Options <- [[]],
            Term <- [foo, {bar, baz, <<"yo">>}],
            Size <- lists:seq(100, 5000, 500)].

run_test(RootDir, BaseDirName, PageSize, NumPages, NumFLUs, FUN) ->
    {FLUs, Seq, P1, Del} = corfurl:simple_test_setup(
                             RootDir, BaseDirName, PageSize, NumPages, NumFLUs),
    try
        FUN(PageSize, Seq, P1)
    after
        ?SEQ:stop(Seq),
        [ok = corfurl_flu:stop(FLU) || FLU <- FLUs],
        Del()
    end.

smoke_test() ->
    ok = run_test("/tmp", "projection",
                  4096, 5*1024, 1, fun smoke_test_int/3).

smoke_test_int(PageSize, Seq, P1) ->
    ok = ?SEQ:set_tails(Seq, [{42,4242}, {43,4343}]),
    {ok, _, [4242, 4343]} = ?SEQ:get_tails(Seq, 0, [42, 43]),

    LPN_Pgs = [{X, ?T:pad_bin(PageSize, term_to_binary({smoke, X}))} ||
                  X <- lists:seq(1, 5)],
    [begin
         {{ok, LPN}, _} = corfurl_client:append_page(P1, Pg)
     end || {LPN, Pg} <- LPN_Pgs],
    [begin
         {ok, Pg} = corfurl:read_page(P1, LPN)
     end || {LPN, Pg} <- LPN_Pgs],

    ok.

write_forward_test() ->
    ok = run_test("/tmp", "write_forward",
                  4096, 5*1024, 1, fun write_forward_test_int/3).

write_forward_test_int(PageSize, _Seq, P1) ->
    StreamNum = 0,
    NumPages = 10,
    Pages = [term_to_binary({smoke, X}) || X <- lists:seq(1, NumPages)],
    BackPs0 = [{StreamNum, []}],
    {P2, BackPs1} = write_stream_pages(P1, Pages, PageSize, BackPs0, StreamNum),
    {_P3, _BackPs2} = write_stream_pages(P2, Pages, PageSize, BackPs1, StreamNum, 3),

    ok.

write_stream_pages(Proj0, Pages, PageSize, InitialBackPs, StreamNum) ->
    write_stream_pages(Proj0, Pages, PageSize, InitialBackPs, StreamNum, 0).

write_stream_pages(Proj0, Pages, _PageSize, InitialBackPs, StreamNum, Junk) ->
    WriteJunk = fun() -> JP0 = <<"blah">>,
                         {{ok, _}, _} = tango:append_page(Proj0, JP0,
                                                          [StreamNum])
                end,
    F = fun(Page, {Proj1, BackPs}) ->
                if Junk band 1 /= 0 -> WriteJunk();
                   true             -> ok end,
                {{ok, LPN}, Proj2} =
                    tango:append_page(Proj1, Page, [StreamNum]),
                if Junk band 1 /= 0 -> WriteJunk();
                   true             -> ok end,
                {Proj2, tango:add_back_pointer(StreamNum, BackPs, LPN)}
        end,
    {_Px, _BackPs} = Res = lists:foldl(F, {Proj0, InitialBackPs}, Pages),
    %% io:format(user, "BackPs ~w\n", [_BackPs]),
    Res.

scan_backward_test() ->
    ok = run_test("/tmp", "scan_backward",
                  4096, 5*1024, 1, fun scan_backward_test_int/3).

scan_backward_test_int(PageSize, _Seq, P1) ->
    StreamNum = 0,
    NumPages = 10,
    PageSeq = lists:seq(1, NumPages),
    Pages = [term_to_binary({smoke, X}) || X <- PageSeq],
    BackPs0 = [{StreamNum, []}],
    {P2, BackPs1} = write_stream_pages(P1, Pages, PageSize, BackPs0, StreamNum),
    LastLPN = hd(proplists:get_value(StreamNum, BackPs1)),

    LastLPN=LastLPN,
    [begin
         ShouldBe = lists:seq(1, BackwardStartLPN),
         ShouldBePages = lists:zip(ShouldBe, lists:sublist(Pages, BackwardStartLPN)),

         %% If we scan backward, we should get a list of LPNs in
         %% oldest -> newest (i.e. smallest LPN to largest LPN) order.
         ShouldBe = tango:scan_backward(P2, StreamNum, BackwardStartLPN,
                                        false),
         StopAtLimit = NumPages div 2,
         StopAtKicksInAt = StopAtLimit + 2,
         {StopAtLPN, ShouldBeLPNS} =
             if BackwardStartLPN < StopAtKicksInAt ->
                     {0, ShouldBe};
               true ->
                     {StopAtLimit, [LPN || LPN <- ShouldBe, LPN > StopAtLimit]}
            end,
         ShouldBeLPNS =
             tango:scan_backward(P2, StreamNum, BackwardStartLPN, StopAtLPN,
                                 false),

         %% If we scan backward, we should get a list of LPNs in
         %% oldest -> newest (i.e. smallest LPN to largest LPN) order
         %% together with the actual page data.
         ShouldBePages = tango:scan_backward(P2, StreamNum, BackwardStartLPN,
                                             true),
         ok
     end || BackwardStartLPN <- lists:seq(1, NumPages)],

    ok.

tango_dt_register_test() ->
    ok = run_test("/tmp", "tango_dt_register",
                  4096, 5*1024, 1, fun tango_dt_register_int/3).

tango_dt_register_int(PageSize, Seq, Proj) ->
    {ok, OID_Map} = tango_oid:start_link(PageSize, Seq, Proj),

    {ok, Reg1Num} = tango_oid:new(OID_Map, "register1"),
    {ok, Reg1} = tango_dt_register:start_link(PageSize, Seq, Proj,
                                              Reg1Num),
    {ok, Reg2Num} = tango_oid:new(OID_Map, "register2"),
    {ok, Reg2} = tango_dt_register:start_link(PageSize, Seq, Proj,
                                              Reg2Num),

    NumVals = 8,
    Vals = [lists:flatten(io_lib:format("version ~w", [X])) ||
               X <- lists:seq(1, NumVals)],
    [tango_dt_register:set(Reg, Val) || Reg <- [Reg1, Reg2], Val <- Vals],
    LastVal = lists:last(Vals),
    {ok, LastVal} = tango_dt_register:get(Reg1),
    {ok, LastVal} = tango_dt_register:get(Reg2),

    %% If we instantiate a new instance of an existing register, then
    %% a single get should show the most recent modification.
    {ok, Reg2b} = tango_dt_register:start_link(PageSize, Seq, Proj,
                                               Reg2Num),
    {ok, LastVal} = tango_dt_register:get(Reg2b),
    %% If we update the "old" instance of a register, then the "new"
    %% instance should also see the update.
    NewVal = {"Heh", "a new value"},
    ok = tango_dt_register:set(Reg2, NewVal),
    C1 = fun() -> {ok, NewVal} = tango_dt_register:get(Reg2), % sanity check
                  {ok, NewVal} = tango_dt_register:get(Reg2b), ok end,
    ok = C1(),

    ok = tango_dt_register:checkpoint(Reg2),
    ok = C1(),
    {ok, Reg2c} = tango_dt_register:start_link(PageSize, Seq, Proj,
                                               Reg2Num),
    {ok, NewVal} = tango_dt_register:get(Reg2c),

    [ok = tango_dt_register:stop(X) || X <- [Reg1, Reg2, Reg2b, Reg2c]],
    ok.

tango_dt_map_test() ->
    ok = run_test("/tmp", "tango_dt_map",
                  4096, 5*1024, 1, fun tango_dt_map_int/3).

tango_dt_map_int(PageSize, Seq, Proj) ->
    {ok, OID_Map} = tango_oid:start_link(PageSize, Seq, Proj),

    {ok, Reg1Num} = tango_oid:new(OID_Map, "map1"),
    {ok, Reg1} = tango_dt_map:start_link(PageSize, Seq, Proj, Reg1Num),
    {ok, Reg2Num} = tango_oid:new(OID_Map, "map2"),
    {ok, Reg2} = tango_dt_map:start_link(PageSize, Seq, Proj, Reg2Num),

    NumVals = 8,
    Vals = [lists:flatten(io_lib:format("version ~w", [X])) ||
               X <- lists:seq(1, NumVals)],
    Keys = ["key1", "key2"],
    [tango_dt_map:set(Reg, Key, Val) || Reg <- [Reg1, Reg2],
                                        Key <- Keys, Val <- Vals],
    LastVal = lists:last(Vals),
    C1 = fun(R, LV) -> [{ok, LV} = tango_dt_map:get(R, Key) || Key <- Keys],
                       ok end,
    ok = C1(Reg1, LastVal),
    ok = C1(Reg2, LastVal),

    %% If we instantiate a new instance of an existing map, then
    %% a single get should show the most recent modification.
    {ok, Reg2b} = tango_dt_map:start_link(PageSize, Seq, Proj, Reg2Num),
    [{ok, LastVal} = tango_dt_map:get(Reg2b, Key) || Key <- Keys],
    %% If we update the "old" instance of a map, then the "new"
    %% instance should also see the update.
    NewVal = {"Heh", "a new value"},
    [ok = tango_dt_map:set(Reg2, Key, NewVal) || Key <- Keys],
    [ok = C1(R, NewVal) || R <- [Reg2, Reg2b]],
    [ok = C1(R, LastVal) || R <- [Reg1]],

    [ok = tango_dt_map:checkpoint(R) || R <- [Reg1, Reg2, Reg2b]],
    NewVal2 = "after the checkpoint....",
    [ok = tango_dt_map:set(Reg2, Key, NewVal2) || Key <- Keys],
    [ok = C1(R, NewVal2) || R <- [Reg2, Reg2b]],
    [ok = C1(R, LastVal) || R <- [Reg1]],

    ok.

tango_dt_queue_test() ->
    ok = run_test("/tmp", "tango_dt_queue",
                  4096, 5*1024, 1, fun tango_dt_queue_int/3).

tango_dt_queue_int(PageSize, Seq, Proj) ->
    MOD = tango_dt_queue,
    {ok, OID_Map} = tango_oid:start_link(PageSize, Seq, Proj),

    {ok, Q1Num} = tango_oid:new(OID_Map, "queue1"),
    {ok, Q1} = MOD:start_link(PageSize, Seq, Proj, Q1Num),

    {ok, true} = MOD:is_empty(Q1),
    {ok, 0} = MOD:length(Q1),
    Num1 = 15,
    Seq1 = lists:seq(1, Num1),
    RevSeq1 = lists:reverse(Seq1),
    [ok = MOD:in(Q1, X) || X <- Seq1],
    {ok, Num1} = MOD:length(Q1),
    {ok, {value, 1}} = MOD:peek(Q1),
    {ok, Seq1} = MOD:to_list(Q1),
    ok = MOD:reverse(Q1),
    {ok, RevSeq1} = MOD:to_list(Q1),
    ok = MOD:reverse(Q1),

    [{ok, {value, X}} = MOD:out(Q1) || X <- lists:seq(1, Num1)],
    {ok, empty} = MOD:out(Q1),
    {ok, []} = MOD:to_list(Q1),

    [ok = MOD:in(Q1, X) || X <- Seq1],
    {ok, false} = MOD:member(Q1, does_not_exist),
    {ok, true} = MOD:member(Q1, Num1),
    ok = MOD:filter(Q1, fun(X) when X == Num1 -> false;
                           (_)                -> true
                        end),
    Num1Minus1 = Num1 - 1,
    C1 = fun(Q, Expected) -> {ok, false} = MOD:member(Q, Num1),
                             {ok, true} = MOD:member(Q, Num1 - 1),
                             {ok, Expected} = MOD:length(Q), ok end,
    ok = C1(Q1, Num1Minus1),

    {ok, Q2} = MOD:start_link(PageSize, Seq, Proj, Q1Num),
    ok = C1(Q2, Num1Minus1),
    ok = MOD:in(Q2, 88),
    ok = C1(Q2, Num1),
    ok = C1(Q1, Num1),

    [ok = MOD:checkpoint(Q1) || _ <- lists:seq(1, 4)],
    [ok = C1(X, Num1) || X <- [Q1, Q2]],
    {ok, Q3} = MOD:start_link(PageSize, Seq, Proj, Q1Num),
    [ok = C1(X, Num1) || X <- [Q1, Q2, Q3]],
    {ok, Q4} = MOD:start_link(PageSize, Seq, Proj, Q1Num),
    ok = MOD:in(Q4, 89),
    Num1Plus1 = Num1 + 1,
    [ok = C1(X, Num1Plus1) || X <- [Q1, Q2, Q3, Q4]],

    [ok = MOD:stop(X) || X <- [Q1, Q2, Q3, Q4]],
    ok.

-endif. % not PULSE
-endif. % TEST
