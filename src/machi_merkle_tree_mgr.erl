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

%% @doc This manager maintains a Merkle tree per file per FLU. The leaf
%% nodes are stored in the same manner as the checksum data files, except
%% they are represented as 
%%
%% `<<Length:64, Offset:32, 0>>' for unwritten bytes
%% `<<Length:64, Offset:32, 1>>' for trimmed bytes
%% `<<Length:64, Offset:32, Csum/binary>>' for written bytes
%%
%% In this case, the checksum tag is thrown away.  The tree feeds these
%% leaf nodes into hashes representing 10 GB ranges, called Level 1. We aim for 
%% around %% 10 hashes at level 2, and then 2 hashes level 3 and finally the
%% root.

-module(machi_merkle_tree_mgr).
-behaviour(gen_server).

-include("machi.hrl").

-export([
    child_spec/3,
    start_link/3,
    initialize/2,
    update/5,
    fetch/2,
    fetch/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
          fluname :: atom(),
          datadir :: string(),
          tid     :: ets:tid()
         }).

-record(mt, {
          filename               :: string(),
          recalc = true          :: boolean(),
          root                   :: 'undefined' | binary(),
          lvl1 = []              :: [ binary() ],
          lvl2 = []              :: [ binary() ],
          lvl3 = []              :: [ binary() ],
          leaves = orddict:new() :: mt_entry()
         }).

-type mt_entry() :: orddict:orddict().

-define(WRITTEN(Offset, Size, Csum), <<Offset:64/unsigned-big, Size:32/unsigned-big, Csum/binary>>).
-define(TRIMMED(Offset, Size), <<Offset:64/unsigned-big, Size:32/unsigned-big, 1>>).
-define(UNWRITTEN(Offset, Size), <<Offset:64/unsigned-big, Size:32/unsigned-big, 0>>).

-define(CHUNK_SIZE, (10*1024*1024)).
-define(LEVEL_SIZE, 10).
-define(H, sha).

-define(TIMEOUT, (10*1000)).

%% public API

child_spec(FluName, DataDir, Options) ->
    Name = make_merkle_tree_mgr_name(FluName),
    {Name, 
        {?MODULE, start_link, [FluName, DataDir, Options]},
        permanent, 5000, worker, [?MODULE]}.

start_link(FluName, DataDir, Options) ->
    gen_server:start_link({local, make_merkle_tree_mgr_name(FluName)},
                          ?MODULE,
                          {FluName, DataDir, Options},
                          []).

-spec initialize(  FluName :: atom(),
                  Filename :: string() ) -> ok.
%% @doc A heads-up hint to the manager that it ought to compute a merkle
%% tree for the given file (if it hasn't already).
initialize(FluName, Filename) ->
    gen_server:cast(make_merkle_tree_mgr_name(FluName), 
                    {initialize, Filename}).

-spec update(  FluName :: atom(),
              Filename :: string(),
                Offset :: non_neg_integer(),
                Length :: pos_integer(),
                  Csum :: binary() ) -> ok.
%% @doc A new leaf node ought to be added file the given filename,
%% with the particular information.
update(FluName, Filename, Offset, Length, Csum) ->
    gen_server:cast(make_merkle_tree_mgr_name(FluName), 
                    {update, Filename, Offset, Length, Csum}).

-spec fetch ( FluName :: atom(),
             Filename :: string() ) -> {ok, [ Data :: binary() ]}.
%% @doc Returns the entire merkle tree for the given filename.
%% {@link fetch/3}
fetch(FluName, Filename) ->
    fetch(FluName, Filename, all).

-spec fetch(  FluName :: atom(),
             Filename :: string(),
                Level :: 'root' | 'all' 
           ) -> {ok, [ Data :: binary() ]}.
%% @doc Return the current merkle tree for the given filename. 
%% If `root' is specified, returns a list with 1 element, the root
%% checksum of the tree.  If `all' is specified, returns a list
%% with all levels.
fetch(FluName, Filename, Level) ->
    gen_server:call(make_merkle_tree_mgr_name(FluName),
                    {fetch, Filename, Level}, ?TIMEOUT).

%% gen_server callbacks
init({FluName, DataDir, Options}) ->
    Tid = ets:new(make_merkle_tree_mgr_name(FluName), [{keypos, 2}, {read_concurrency, true}]),
    case proplists:get_value(no_load, Options, false) of
        true ->
            ok;
        false ->
            handle_load(Tid, DataDir)
    end,
    {ok, #state{fluname=FluName, datadir=DataDir, tid = Tid}}.

handle_call({fetch, Filename, Level}, _From, S = #state{ tid = Tid }) ->
    Res = handle_fetch(Tid, Filename, Level),
    {reply, {ok, Res}, S};
handle_call(Req, _From, State) ->
    lager:warning("Unknown call: ~p", [Req]),
    {reply, whoaaaaaaaaaaaa, State}.

handle_cast({initialize, Filename}, S = #state{ datadir = D, tid = Tid }) ->
    load_filename(Tid, D, Filename),
    {noreply, S};

handle_cast({update, Filename, Offset, Length, Csum}, S = #state{ tid = Tid }) ->
    %% XXX FIXME: Not sure about the correctness of this
    insert(Tid, Filename, {Offset, Length, Csum}),
    {noreply, S};

handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

handle_info(Req, State) ->
    lager:warning("Unknown info message: ~p", [Req]),
    {noreply, State}.

terminate(Reason, #state{fluname = F}) ->
    lager:debug("Shutting down merkle tree manager for FLU ~p because ~p", 
                [F, Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% private

make_merkle_tree_mgr_name(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_merkle_tree_mgr").

handle_load(Tid, DataDir) ->
    Files = get_files(DataDir),
    lists:foreach(fun(F) -> load_filename(Tid, DataDir, F) end, Files).

get_files(DataDir) ->
    {_, WildPath} = machi_util:make_data_filename(DataDir, ""),
    filelib:wildcard("*", WildPath).

load_filename(Tid, DataDir, Filename) ->
    CsumFile = machi_util:make_checksum_filename(DataDir, Filename),
    case file:read_file(CsumFile) of
        {error, enoent} -> 
            insert(Tid, Filename, {0, ?MINIMUM_OFFSET, unwritten});
        {ok, Bin} ->
            load_bin(Tid, Filename, Bin);
        Error ->
            throw(Error)
    end.

load_bin(Tid, Filename, Bin) ->
    {CsumL, _} = machi_csum_table:split_checksum_list_blob_decode(Bin),
    iter_csum_list(Tid, Filename, CsumL).

iter_csum_list(Tid, Filename, []) ->
    insert(Tid, Filename, {0, ?MINIMUM_OFFSET, unwritten});
iter_csum_list(Tid, Filename, L = [ {Last, _, _} | _ ]) ->
    make_insert(Tid, Filename, Last, L).

make_insert(_Tid, _Filename, _Last, []) ->
    ok;
%% case where Last offset matches Current, just insert current
make_insert(Tid, Filename, Last, [H={Last, Len, _Csum}|T]) ->
    insert(Tid, Filename, H),
    make_insert(Tid, Filename, Last+Len, T);
%% case where we have a hole
make_insert(Tid, Filename, Last, [H={Off, Len, _Csum}|T]) ->
    Hole = Off - Last,
    insert(Tid, Filename, {Last, Hole, unwritten}),
    insert(Tid, Filename, H),
    make_insert(Tid, Filename, Off+Len, T).

insert(Tid, Filename, {Offset, Length, trimmed}) ->
    do_insert(Tid, Filename, Offset, Length, ?TRIMMED(Offset, Length));

insert(Tid, Filename, {Offset, Length, unwritten}) ->
    do_insert(Tid, Filename, Offset, Length, ?UNWRITTEN(Offset, Length));

insert(Tid, Filename, {Offset, Length, <<_Tag:8, Csum/binary>>}) ->
    do_insert(Tid, Filename, Offset, Length, ?WRITTEN(Offset, Length, Csum)).

do_insert(Tid, Filename, Offset, Length, Csum) ->
    MT = case find(Tid, Filename) of
        [] ->
            #mt{ filename = Filename };
        V -> 
            V
    end,
    ok = maybe_update(Tid, Offset, Length, Csum, MT),
    ok.
    
maybe_update(Tid, Offset, Length, Csum, MT) ->
    case orddict:find({Offset, Length}, MT#mt.leaves) of
        error ->
            %% range not found in our orddict, so fill it in
            do_update(Tid, Offset, Length, Csum, MT);
        {ok, Csum} ->
            %% trying to insert a value we already have that
            %% matches
            ok;
        {ok, ?UNWRITTEN(Offset, Length)} ->
            %% old value was unwritten, now we are filling it in
            %% so that's legit
            do_update(Tid, Offset, Length, Csum, MT);
        {ok, ?TRIMMED(Offset, Length)} ->
            %% XXX FIXME 
            %% Scott - range was trimmed - do we fill it in with new stuff?
            ok;
        {ok, Other} ->
            %% we found a checksum that is different
            %% this shouldn't happen because once we write a range, it's written
            %% TODO - file corruption? insanity?
            lager:error("Tried to update merkle tree for file ~p at offset ~p, length ~p, got checksum ~p and tried to insert ~p",
                        [MT#mt.filename, Offset, Length, Other, Csum]),
            throw({weird, Other})
    end.

do_update(Tid, Offset, Length, Csum, MT) ->
    D = orddict:store({Offset, Length}, Csum, MT#mt.leaves),
    true = ets:insert(Tid, MT#mt{ recalc = true, leaves = D }),
    ok.

handle_fetch(Tid, Filename, root) ->
    case find(Tid, Filename) of
        [] -> undefined;
        #mt{ root = undefined } -> undefined;
        #mt{ recalc = true } = MT -> hd(build_tree(Tid, MT));
        #mt{ root = R, recalc = false } -> R
    end;

handle_fetch(Tid, Filename, all) ->
    case find(Tid, Filename) of
        [] -> undefined;
        #mt{ recalc = true } = MT -> build_tree(Tid, MT);
        #mt{ recalc = false, 
             root = R,
             lvl1 = L1,
             lvl2 = L2,
             lvl3 = L3 } -> [ R, L3, L2, L1 ]
    end.

find(Tid, Filename) ->
    case ets:lookup(Tid, Filename) of 
        [] -> [];
        [R] -> R
    end.

build_tree(Tid, MT = #mt{ leaves = D }) ->
    Leaves = lists:map(fun map_dict/1, orddict:to_list(D)),
    io:format(user, "Leaves: ~p~n", [Leaves]),
    Lvl1s = build_level_1(?CHUNK_SIZE, Leaves, 1, [ crypto:hash_init(?H) ]),
    io:format(user, "Lvl1: ~p~n", [Lvl1s]),
    Mod2 = length(Lvl1s) div ?LEVEL_SIZE,
    Lvl2s = build_int_level(Mod2, Lvl1s, 1, [ crypto:hash_init(?H) ]),
    io:format(user, "Lvl2: ~p~n", [Lvl2s]),
    Mod3 = length(Lvl2s) div 2,
    Lvl3s = build_int_level(Mod3, Lvl2s, 1, [ crypto:hash_init(?H) ]),
    io:format(user, "Lvl3: ~p~n", [Lvl3s]),
    Root = build_root(Lvl3s, crypto:hash_init(?H)),
    io:format(user, "Root: ~p~n", [Root]),
    ets:insert(Tid, MT#mt{ root = Root, lvl1 = Lvl1s, lvl2 = Lvl2s, lvl3 = Lvl3s, recalc = false }),
    [Root, Lvl3s, Lvl2s, Lvl1s].

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

map_dict({{Offset, Len}, Hash}) ->
    {Offset + Len, Hash}.

build_level_1(_Size, [], _Multiple, [ Ctx | Rest ]) ->
    lists:reverse([ crypto:hash_final(Ctx) | Rest ]);
build_level_1(Size, [{Pos, Hash}|T], Multiple, [ Ctx | Rest ]) when Pos > ( Size * Multiple ) ->
    NewCtx = crypto:hash_init(?H),
    build_level_1(Size, T, Multiple+1, 
                  [ crypto:hash_update(NewCtx, Hash), crypto:hash_final(Ctx) | Rest ]);
build_level_1(Size, [{Pos, Hash}|T], Multiple, [ Ctx | Rest ]) when Pos =< ( Size * Multiple ) ->
    io:format(user, "Size: ~p, Pos: ~p, Multiple: ~p~n", [Size, Pos, Multiple]),
    build_level_1(Size, T, Multiple, [ crypto:hash_update(Ctx, Hash) | Rest ]).

