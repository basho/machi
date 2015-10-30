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

%% @doc Creates a Merkle tree per file based on the checksum data for
%% a given data file.
%%
%% Has selectable backend, chosen at open.
%%
%% The default 'merklet' implementation uses the `merklet' library.  Keys are
%% encoded as `<<Offset:64, Size:32>>' values encoded as `<<Tag:8, Csum/binary>>'
%% *or* as `<<0>>' for unwritten bytes, or `<<1>>' for trimmed bytes.
%%
%% The `naive' implementation representation is:
%%
%% `<<Length:64, Offset:32, 0>>' for unwritten bytes
%% `<<Length:64, Offset:32, 1>>' for trimmed bytes
%% `<<Length:64, Offset:32, Csum/binary>>' for written bytes
%%
%% The tree feeds these leaf nodes into hashes representing chunks of a minimum
%% size of at least 1024 KB (1 MB), but if the file size is larger, we will try
%% to get about 100 chunks for called "Level 1." We aim for around 10 hashes at
%% level 2, and then 2 hashes level 3 and finally the root.

-module(machi_merkle_tree).

-include("machi.hrl").

-export([
    open/2,
    open/3,
    tree/1,
    filename/1,
    diff/2
]).


-record(naive, {
          chunk_size = 1048576   :: pos_integer(), %% default 1 MB
          recalc = true          :: boolean(),
          root                   :: 'undefined' | binary(),
          lvl1 = []              :: [ binary() ],
          lvl2 = []              :: [ binary() ],
          lvl3 = []              :: [ binary() ],
          leaves = []            :: [ { Offset :: pos_integer(),
                                        Size :: pos_integer(),
                                        Csum :: binary()} ]
         }).

-record(mt, {
          filename               :: string(),
          tree                   :: #naive{}|merklet:tree(),
          backend = 'merklet'    :: 'naive'|'merklet'
         }).

-define(TRIMMED, <<1>>).
-define(UNWRITTEN, <<0>>).
-define(ENCODE(Offset, Size), <<Offset:64/unsigned-big, Size:32/unsigned-big>>).
-define(NAIVE_ENCODE(Offset, Size, Data), <<Offset:64/unsigned-big, Size:32/unsigned-big, Data/binary>>).

-define(NEW_MERKLET, undefined).
-define(TIMEOUT, (10*1000)).

-define(MINIMUM_CHUNK, 1048576). %% 1024 * 1024
-define(LEVEL_SIZE, 10).
-define(H, sha).

%% public API

open(Filename, DataDir) ->
    open(Filename, DataDir, merklet).

open(Filename, DataDir, Type) ->
    Tree = load_filename(Filename, DataDir, Type),
    {ok, #mt{ filename = Filename, tree = Tree, backend = Type}}.

tree(#mt{ tree = T, backend = merklet }) -> T;
tree(#mt{ tree = T, backend = naive }) ->
    case T#naive.recalc of
         true -> build_tree(T);
        false -> T
    end.

filename(#mt{ filename = F }) -> F.

diff(#mt{backend = naive, tree = T1}, #mt{backend = naive, tree = T2}) ->
    case T1#naive.root == T2#naive.root of
        true -> same;
        false -> naive_diff(T1, T2) 
    end;
diff(#mt{backend = merklet, tree = T1}, #mt{backend = merklet, tree = T2}) ->
    case merklet:diff(T1, T2) of
        [] -> same;
        Diff -> Diff
    end;
diff(_, _) -> error(badarg).

%% private

% @private
load_filename(Filename, DataDir, merklet) ->
    {_Last, M} = do_load(Filename, DataDir, fun insert_csum/2, ?NEW_MERKLET),
    M;
load_filename(Filename, DataDir, naive) ->
    {Last, M} = do_load(Filename, DataDir, fun insert_csum_naive/2, []),
    ChunkSize = max(?MINIMUM_CHUNK, Last div 100),
    T = #naive{ leaves = lists:reverse(M), chunk_size = ChunkSize, recalc = true },
    build_tree(T).

do_load(Filename, DataDir, FoldFun, AccInit) ->
    CsumFile = machi_util:make_checksum_filename(DataDir, Filename),
    {ok, T} = machi_csum_table:open(CsumFile, []),
    Acc = machi_csum_table:foldl_chunks(FoldFun, {0, AccInit}, T),
    ok = machi_csum_table:close(T),
    Acc.

% @private
insert_csum({Last, Size, _Csum}=In, {Last, MT}) ->
    %% no gap here, insert a record
    {Last+Size, update_merkle_tree(In, MT)};
insert_csum({Offset, Size, _Csum}=In, {Last, MT}) ->
    %% gap here, insert unwritten record 
    %% *AND* insert written record
    Hole = Offset - Last,
    MT0 = update_merkle_tree({Last, Hole, unwritten}, MT),
    {Offset+Size, update_merkle_tree(In, MT0)}.

insert_csum_naive({Last, Size, _Csum}=In, {Last, MT}) ->
    %% no gap
    {Last+Size, update_acc(In, MT)};
insert_csum_naive({Offset, Size, _Csum}=In, {Last, MT}) ->
    Hole = Offset - Last,
    MT0 = update_acc({Last, Hole, unwritten}, MT),
    {Offset+Size, update_acc(In, MT0)}.

% @private
update_merkle_tree({Offset, Size, unwritten}, MT) ->
    merklet:insert({?ENCODE(Offset, Size), ?UNWRITTEN}, MT);
update_merkle_tree({Offset, Size, trimmed}, MT) ->
    merklet:insert({?ENCODE(Offset, Size), ?TRIMMED}, MT);
update_merkle_tree({Offset, Size, Csum}, MT) ->
    merklet:insert({?ENCODE(Offset, Size), Csum}, MT).

update_acc({Offset, Size, unwritten}, MT) ->
    [ {Offset, Size, ?NAIVE_ENCODE(Offset, Size, ?UNWRITTEN)} | MT ];
update_acc({Offset, Size, trimmed}, MT) ->
    [ {Offset, Size, ?NAIVE_ENCODE(Offset, Size, ?TRIMMED)} | MT ];
update_acc({Offset, Size, Csum}, MT) ->
    [ {Offset, Size, ?NAIVE_ENCODE(Offset, Size, Csum)} | MT ].

build_tree(MT = #naive{ leaves = L, chunk_size = ChunkSize }) ->
    lager:debug("Leaves: ~p~n", [L]),
    Lvl1s = build_level_1(ChunkSize, L, 1, [ crypto:hash_init(?H) ]),
    lager:debug("Lvl1: ~p~n", [Lvl1s]),
    Mod2 = length(Lvl1s) div ?LEVEL_SIZE,
    Lvl2s = build_int_level(Mod2, Lvl1s, 1, [ crypto:hash_init(?H) ]),
    lager:debug("Lvl2: ~p~n", [Lvl2s]),
    Mod3 = length(Lvl2s) div 2,
    Lvl3s = build_int_level(Mod3, Lvl2s, 1, [ crypto:hash_init(?H) ]),
    lager:debug("Lvl3: ~p~n", [Lvl3s]),
    Root = build_root(Lvl3s, crypto:hash_init(?H)),
    lager:debug("Root: ~p~n", [Root]),
    MT#naive{ root = Root, lvl1 = Lvl1s, lvl2 = Lvl2s, lvl3 = Lvl3s, recalc = false }.

build_root([], Ctx) ->
    crypto:hash_final(Ctx);
build_root([H|T], Ctx) ->
    build_root(T, crypto:hash_update(Ctx, H)).

build_int_level(_Mod, [], _Cnt, [ Ctx | Rest ]) ->
    lists:reverse( [ crypto:hash_final(Ctx) | Rest ] );
build_int_level(Mod, [H|T], Cnt, [ Ctx | Rest ]) when Cnt rem Mod == 0 ->
    NewCtx = crypto:hash_init(?H),
    build_int_level(Mod, T, Cnt + 1, [ crypto:hash_update(NewCtx, H), crypto:hash_final(Ctx) | Rest ]);
build_int_level(Mod, [H|T], Cnt, [ Ctx | Rest ]) ->
    build_int_level(Mod, T, Cnt+1, [ crypto:hash_update(Ctx, H) | Rest ]).

build_level_1(_Size, [], _Multiple, [ Ctx | Rest ]) ->
    lists:reverse([ crypto:hash_final(Ctx) | Rest ]);
build_level_1(Size, [{Pos, Len, Hash}|T], Multiple, [ Ctx | Rest ])
                                    when ( Pos + Len ) > ( Size * Multiple ) ->
    NewCtx = crypto:hash_init(?H),
    build_level_1(Size, T, Multiple+1,
                  [ crypto:hash_update(NewCtx, Hash), crypto:hash_final(Ctx) | Rest ]);
build_level_1(Size, [{Pos, Len, Hash}|T], Multiple, [ Ctx | Rest ])
                                    when ( Pos + Len ) =< ( Size * Multiple ) ->
    build_level_1(Size, T, Multiple, [ crypto:hash_update(Ctx, Hash) | Rest ]).

naive_diff(#naive{lvl1 = L1}, #naive{lvl1=L2, chunk_size=CS2}) ->
    lager:debug("naive diff: Our lvl1: ~p~n", [L1]),
    lager:debug("naive diff: Their chunk size: ~p, lvl1: ~p~n", [CS2, L2]),

    Set1 = gb_sets:from_list(lists:zip(lists:seq(1, length(L1), L1))),
    Set2 = gb_sets:from_list(lists:zip(lists:seq(1, length(L2), L2))),

    %% The byte ranges in list 2 that do not match in list 1
    %%
    %% We have to decide what to do now - should we filter the
    %% leaf nodes using these ranges and find specific divergence
    %% between Tree1 and Tree2? 
    %%
    %% Or should we do something else?
    [ {(X-1)*CS2, CS2, SHA} || {X, SHA} <- gb_sets:to_list(gb_sets:subtract(Set1, Set2)) ].


