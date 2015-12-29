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

%% @doc A simple basho_bench driver for Machi
%%
%% Basho_bench was originally developed to stress test key-value
%% stores (as was YCSB and several other mechmarking tools).  A person
%% can consider the UNIX file system to be a key-value store and thus
%% use basho_bench to measure its performance under a certain
%% workload.  Machi is a bit different than most KV stores in that the
%% client has no direct control over the keys -- Machi servers always
%% assign the keys.  The schemes typically used by basho_bench &amp; YCSB
%% to use/mimic key naming conventions used internally ... are
%% difficult to adapt to Machi.
%%
%% So, we'll try to manage key reading by using a common ETS table
%% that is populated with:
%%
%% 1. Key: `non_neg_integer()`
%% 2. Value: The `{File,Offset,Size}` for a chunk previously written.
%%
%% At startup time, basho_bench can use the `list_files' and
%% `checksum_list' API operations to fetch all of the
%% `{File,Offset,Size}` tuples that currently reside in the cluster.
%% Also, optionally (?), each new `append' operation by the b_b driver
%% could add new entries to this ETS table.
%%
%% Now we can use various integer-centric key generators that are
%% already bundled with basho_bench.
%%
%% TODO: Add CRC checking, when feasible and when supported on the
%% server side.
%%
%% TODO: As an alternate idea, if we know that the chunks written are
%% always the same size, and if we don't care about CRC checking, then
%% all we need to know are the file names &amp; file sizes on the server:
%% we can then pick any valid offset within that file.  That would
%% certainly be more scalable than the zillion-row-ETS-table, which is
%% definitely RAM-hungry.

-module(machi_basho_bench_driver).

-export([new/1, run/4]).

-record(m, {
          conn,
          max_key
         }).

-define(ETS_TAB, machi_keys).
-define(THE_TIMEOUT, 60*1000).

-define(INFO(Str, Args), (_ = lager:info(Str, Args))).
-define(WARN(Str, Args), (_ = lager:warning(Str, Args))).
-define(ERROR(Str, Args), (_ = lager:error(Str, Args))).

new(Id) ->
    Ps = find_server_info(Id),
    {ok, Conn} = machi_cr_client:start_link(Ps),
    if Id == 1 ->
            ?INFO("Key preload: starting", []),
            TabType = basho_bench_config:get(machi_ets_key_tab_type, set),
            ETS = ets:new(?ETS_TAB, [public, named_table, TabType,
                                     {read_concurrency, true}]),
            ets:insert(ETS, {max_key, 0}),
            ets:insert(ETS, {total_bytes, 0}),
            MaxKeys = load_ets_table(Conn, ETS),
            ?INFO("Key preload: finished, ~w keys loaded", [MaxKeys]),
            Bytes = ets:lookup_element(ETS, total_bytes, 2),
            ?INFO("Key preload: finished, chunk list specifies ~s MBytes of chunks",
                  [machi_util:mbytes(Bytes)]),
            ok;
       true ->
            ok
    end,
    {ok, #m{conn=Conn}}.

run(append, KeyGen, ValueGen, #m{conn=Conn}=S) ->
    Prefix = KeyGen(),
    Value = ValueGen(),
    case machi_cr_client:append_chunk(Conn, Prefix, Value, ?THE_TIMEOUT) of
        {ok, Pos} ->
            EtsKey = ets:update_counter(?ETS_TAB, max_key, 1),
            true = ets:insert(?ETS_TAB, {EtsKey, Pos}),
            {ok, S};
        {error, _}=Err ->
            ?ERROR("append ~w bytes to prefix ~w: ~p\n",
                   [iolist_size(Value), Prefix, Err]),
            {error, Err, S}
    end;
run(read, KeyGen, ValueGen, #m{max_key=undefined}=S) ->
    MaxKey = ets:update_counter(?ETS_TAB, max_key, 0),
    run(read, KeyGen, ValueGen, S#m{max_key=MaxKey});
run(read, KeyGen, _ValueGen, #m{conn=Conn, max_key=MaxKey}=S) ->
    Idx = KeyGen() rem MaxKey,
    %% {File, Offset, Size, _CSum} = ets:lookup_element(?ETS_TAB, Idx, 2),
    {File, Offset, Size} = ets:lookup_element(?ETS_TAB, Idx, 2),
    case machi_cr_client:read_chunk(Conn, File, Offset, Size, undefined, ?THE_TIMEOUT) of
        {ok, _Chunk} ->
            {ok, S};
        {error, _}=Err ->
            ?ERROR("read file ~p offset ~w size ~w: ~w\n",
                   [File, Offset, Size, Err]),
            {error, Err, S}
    end.

find_server_info(_Id) ->
    Key = machi_server_info,
    case basho_bench_config:get(Key, undefined) of
        undefined ->
            ?ERROR("Please define '~w' in your basho_bench config.\n", [Key]),
            timer:sleep(500),
            exit(bad_config);
        Ps ->
            Ps
    end.

load_ets_table(Conn, ETS) ->
    {ok, Fs} = machi_cr_client:list_files(Conn),
    [begin
         {ok, InfoBin} = machi_cr_client:checksum_list(Conn, File),
         PosList = machi_csum_table:split_checksum_list_blob_decode(InfoBin),
         StartKey = ets:update_counter(ETS, max_key, 0),
         %% _EndKey = lists:foldl(fun({Off,Sz,CSum}, K) ->
         %%                               V = {File, Off, Sz, CSum},
         {_, Bytes} = lists:foldl(fun({Off,Sz,_CSum}, {K, Bs}) ->
                                          V = {File, Off, Sz},
                                          ets:insert(ETS, {K, V}),
                                          {K + 1, Bs + Sz}
                                  end, {StartKey, 0}, PosList),
         ets:update_counter(ETS, max_key, length(PosList)),
         ets:update_counter(ETS, total_bytes, Bytes)
     end || {_Size, File} <- Fs],
    ets:update_counter(?ETS_TAB, max_key, 0).

