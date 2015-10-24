%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(machi_merkle_tree_mgr_test).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-define(TESTFILE, "yza^4c784dc2-19bf-4ac6-91f6-58bbe5aa88e0^1").
-define(GAP_CHANCE, 0.10).

make_csum_file(DataDir, Filename, Offsets) ->
    Path = machi_util:make_checksum_filename(DataDir, Filename),
    filelib:ensure_dir(Path),
    {ok, MC} = machi_csum_table:open(Path, []),
    lists:foreach(fun({Offset, Size, Checksum}) -> 
                    machi_csum_table:write(MC, Offset, Size, Checksum) end,
                  Offsets),
    machi_csum_table:close(MC).

choose_int(Factor) ->
    random:uniform(1024*Factor).

small_int() ->
    choose_int(10).

medium_int() ->
    choose_int(1024).

large_int() ->
    choose_int(4096).

make_offsets(Filename) ->
    {ok, Info} = file:read_file_info(Filename),
    Filesize = Info#file_info.size,
    {ok, FH} = file:open(Filename, [read, raw, binary]),
    Offsets = generate_offsets(FH, Filesize, 1024, []),
    file:close(FH),
    Offsets.

random_from_list(L) ->
    N = random:uniform(length(L)),
    lists:nth(N, L).

choose_size() ->
    F = random_from_list([fun small_int/0, fun medium_int/0, fun large_int/0]),
    F().

maybe_gap(Chance) when Chance < ?GAP_CHANCE ->
    choose_size();
maybe_gap(_) -> 0.

generate_offsets(FH, Filesize, Current, Acc) when Current < Filesize ->
    Length0 = choose_size(),

    Length = case Length0 + Current > Filesize of
                 false -> Length0;
                  true -> Filesize - Current
    end,
    {ok, Data} = file:pread(FH, Current, Length),
    Checksum = machi_util:make_tagged_csum(client_sha, machi_util:checksum_chunk(Data)),
    Gap = maybe_gap(random:uniform()),
    generate_offsets(FH, Filesize, Current + Length + Gap, [ {Current, Length, Checksum} | Acc ]);
generate_offsets(_FH, _Filesize, _Current, Acc) ->
    lists:reverse(Acc).

test() ->
    random:seed(os:timestamp()),
    O = make_offsets("test/" ++ ?TESTFILE),
    ?debugFmt("Offsets: ~p", [O]),
    make_csum_file(".", ?TESTFILE, O),

    _ = machi_merkle_tree_mgr:start_link(test, ".", []),
    machi_merkle_tree_mgr:initialize(test, ?TESTFILE),
    timer:sleep(1000),
    Root = machi_merkle_tree_mgr:fetch(test, ?TESTFILE, root),
    ?debugFmt("Root: ~p~n", [Root]),
    All = machi_merkle_tree_mgr:fetch(test, ?TESTFILE),
    ?debugFmt("All: ~p~n", [All]),
    ok.

