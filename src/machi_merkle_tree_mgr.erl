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

%% @doc This manager maintains a Merkle tree per file per FLU as implemented
%% by the `merklet' library.  Keys are encoded as `<<Offset:64, Size:32>>'
%% values encoded as `<<Tag:8, Csum/binary>>' *or* as <<0>> for unwritten
%% bytes, or <<1>> for trimmed bytes.

-module(machi_merkle_tree_mgr).
-behaviour(gen_server).

-include("machi.hrl").

-export([
    child_spec/3,
    start_link/3,
    initialize/2,
    update/5,
    fetch/2
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
          tree                   :: merklet:tree()
         }).

-define(TRIMMED, <<1>>).
-define(UNWRITTEN, <<0>>).
-define(ENCODE(Offset, Size), <<Offset:64/unsigned-big, Size:32/unsigned-big>>).

-define(NEW_MERKLET, undefined).
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
             Filename :: string() ) -> {ok, 'undefined'|merklet:tree()}.
%% @doc Returns the merkle tree for the given filename.
fetch(FluName, Filename) ->
    gen_server:call(make_merkle_tree_mgr_name(FluName),
                    {fetch, Filename}, ?TIMEOUT).

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

handle_call({fetch, Filename}, _From, S = #state{ tid = Tid }) ->
    Res = handle_fetch(Tid, Filename),
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
    {ok, T} = machi_csum_table:open(CsumFile, []),
    %% docs say that the traversal order of ets:foldl is non-determinstic 
    %% but hopefully since csum_table uses an ordered set that's not true...
    {_LastPosition, M} = machi_csum_table:foldl_chunks(fun insert_csum/2,
                                                       {?MINIMUM_OFFSET, ?NEW_MERKLET}, T),
    true = ets:insert_new(Tid, #mt{ filename = Filename, tree = M}),
    ok = machi_csum_table:close(T),
    ok.

insert_csum({Last, Size, _Csum}=In, {Last, MT}) ->
    %% no gap here, insert a record
    {Last+Size, update_merkle_tree(In, MT)};
insert_csum({Offset, Size, _Csum}=In, {Last, MT}) ->
    %% gap here, insert unwritten record 
    %% *AND* insert written record
    Hole = Offset - Last,
    MT0 = update_merkle_tree({Last, Hole, unwritten}, MT),
    {Offset+Size, update_merkle_tree(In, MT0)}.

insert(Tid, Filename, Term) ->
    case ets:lookup(Tid, Filename) of
        [] -> error(not_found); %% TODO: Something better?
        [R] -> 
            NewMT = update_merkle_tree(Term, R#mt.tree),
            %% we choose update_element because it
            %% makes atomic changes so it is concurrent
            %% safe. The regular 'insert' function
            %% does not provide that guarantee.
            true = ets:update_element(Tid, Filename, {#mt.tree, NewMT}),
            ok
    end.

handle_fetch(Tid, Filename) ->
    case ets:lookup(Tid, Filename) of
        [] -> undefined;
        [R] -> R#mt.tree
    end.

update_merkle_tree({Offset, Size, unwritten}, MT) ->
    merklet:insert({?ENCODE(Offset, Size), ?UNWRITTEN}, MT);
update_merkle_tree({Offset, Size, trimmed}, MT) ->
    merklet:insert({?ENCODE(Offset, Size), ?TRIMMED}, MT);
update_merkle_tree({Offset, Size, Csum}, MT) ->
    merklet:insert({?ENCODE(Offset, Size), Csum}, MT).
