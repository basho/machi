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
%%
%% @doc This process is responsible for managing filenames assigned to
%% prefixes. It's started out of `machi_flu_psup'.
%%
%% Supported operations include finding the "current" filename assigned to
%% a prefix. Incrementing the sequence number and returning a new file name
%% and listing all data files assigned to a given prefix.
%%
%% All prefixes should have the form of `{prefix, P}'. Single filename
%% return values have the form of `{file, F}'.
%%
%% <h2>Finding the current file associated with a sequence</h2>
%% First it looks up the sequence number from the prefix name. If
%% no sequence file is found, it uses 0 as the sequence number and searches
%% for a matching file with the prefix and 0 as the sequence number.
%% If no file is found, the it generates a new filename by incorporating
%% the given prefix, a randomly generated (v4) UUID and 0 as the
%% sequence number.
%%
%% If the sequence number is > 0, then the process scans the filesystem
%% looking for a filename which matches the prefix and given sequence number and
%% returns that.

-module(machi_flu_filename_mgr).
-behavior(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-export([
    child_spec/2,
    start_link/2,
    find_or_make_filename_from_prefix/4,
    increment_prefix_sequence/2,
    list_files_by_prefix/2
    ]).

%% gen_server callbacks
-export([
    init/1,
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    terminate/2,
    code_change/3
    ]).

-define(TIMEOUT, 10 * 1000).
-include("machi_projection.hrl"). %% included for pv1_epoch_n type

-record(state, {fluname :: atom(),
                tid     :: ets:tid(),
                datadir :: string(),
                epoch   :: pv1_epoch_n()
               }).

%% public API

child_spec(FluName, DataDir) ->
    Name = make_filename_mgr_name(FluName),
    {Name,
        {?MODULE, start_link, [FluName, DataDir]},
        permanent, 5000, worker, [?MODULE]}.

start_link(FluName, DataDir) when is_atom(FluName) andalso is_list(DataDir) ->
    N = make_filename_mgr_name(FluName),
    gen_server:start_link({local, N}, ?MODULE, [FluName, DataDir], []).

-spec find_or_make_filename_from_prefix( FluName :: atom(), 
                                         EpochId :: pv1_epoch_n(), 
                                         Prefix :: {prefix, string()},
                                         {coc, riak_dt:coc_namespace(), riak_dt:coc_locator()}) ->
        {file, Filename :: string()} | {error, Reason :: term() } | timeout.
% @doc Find the latest available or make a filename from a prefix. A prefix
% should be in the form of a tagged tuple `{prefix, P}'. Returns a tagged
% tuple in the form of `{file, F}' or an `{error, Reason}'
find_or_make_filename_from_prefix(FluName, EpochId,
                                  {prefix, Prefix},
                                  {coc, _CoC_Ns, _CoC_Loc}=CoC_NL)
  when is_atom(FluName) ->
    N = make_filename_mgr_name(FluName),
    gen_server:call(N, {find_filename, EpochId, CoC_NL, Prefix}, ?TIMEOUT);
find_or_make_filename_from_prefix(_FluName, _EpochId, Other, Other2) ->
    lager:error("~p is not a valid prefix/CoC ~p", [Other, Other2]),
    error(badarg).

-spec increment_prefix_sequence( FluName :: atom(), Prefix :: {prefix, string()} ) ->
        ok | {error, Reason :: term() } | timeout.
% @doc Increment the sequence counter for a given prefix. Prefix should
% be in the form of `{prefix, P}'.
increment_prefix_sequence(FluName, {prefix, Prefix}) when is_atom(FluName) ->
    gen_server:call(make_filename_mgr_name(FluName), {increment_sequence, Prefix}, ?TIMEOUT);
increment_prefix_sequence(_FluName, Other) ->
    lager:error("~p is not a valid prefix.", [Other]),
    error(badarg).

-spec list_files_by_prefix( FluName :: atom(), Prefix :: {prefix, string()} ) ->
    [ file:name() ] | timeout | {error, Reason :: term() }.
% @doc Given a prefix in the form of `{prefix, P}' return
% all the data files associated with that prefix. Returns
% a list.
list_files_by_prefix(FluName, {prefix, Prefix}) when is_atom(FluName) ->
    gen_server:call(make_filename_mgr_name(FluName), {list_files, Prefix}, ?TIMEOUT);
list_files_by_prefix(_FluName, Other) ->
    lager:error("~p is not a valid prefix.", [Other]),
    error(badarg).

%% gen_server API
init([FluName, DataDir]) ->
    Tid = ets:new(make_filename_mgr_name(FluName), [named_table, {read_concurrency, true}]),
    {ok, #state{fluname = FluName,
                epoch = 0,
                datadir = DataDir,
                tid = Tid}}.

handle_cast(Req, State) ->
    lager:warning("Got unknown cast ~p", [Req]),
    {noreply, State}.

%% Important assumption: by the time we reach here the EpochId is kosher.
%% the FLU has already validated that the caller's epoch id and the FLU's epoch id
%% are the same. So we *assume* that remains the case here - that is to say, we
%% are not wedged.
handle_call({find_filename, EpochId, CoC_NL, Prefix}, _From, S = #state{ datadir = DataDir,
                                                                 epoch = EpochId, 
                                                                 tid = Tid }) ->
    %% Our state and the caller's epoch ids are the same. Business as usual.
    File = handle_find_file(Tid, CoC_NL, Prefix, DataDir),
    {reply, {file, File}, S};

handle_call({find_filename, EpochId, CoC_NL, Prefix}, _From, S = #state{ datadir = DataDir, tid = Tid }) ->
    %% If the epoch id in our state and the caller's epoch id were the same, it would've
    %% matched the above clause. Since we're here, we know that they are different.
    %% If epoch ids between our state and the caller's are different, we must increment the
    %% sequence number, generate a filename and then cache it.
    File = increment_and_cache_filename(Tid, DataDir, CoC_NL, Prefix),
    {reply, {file, File}, S#state{epoch = EpochId}};

handle_call({increment_sequence, Prefix}, _From, S = #state{ datadir = DataDir }) ->
    ok = machi_util:increment_max_filenum(DataDir, Prefix),
    {reply, ok, S};
handle_call({list_files, Prefix}, From, S = #state{ datadir = DataDir }) ->
    spawn(fun() ->
        L = list_files(DataDir, Prefix),
        gen_server:reply(From, L)
    end),
    {noreply, S};

handle_call(Req, From, State) ->
    lager:warning("Got unknown call ~p from ~p", [Req, From]),
    {reply, hoge, State}.

handle_info(Info, State) ->
    lager:warning("Got unknown info ~p", [Info]),
    {noreply, State}.

terminate(Reason, _State) ->
    lager:info("Shutting down because ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% private

%% Quoted from https://github.com/afiskon/erlang-uuid-v4/blob/master/src/uuid.erl
%% MIT License
generate_uuid_v4_str() ->
    <<A:32, B:16, C:16, D:16, E:48>> = crypto:strong_rand_bytes(16),
    io_lib:format("~8.16.0b-~4.16.0b-4~3.16.0b-~4.16.0b-~12.16.0b",
                        [A, B, C band 16#0fff, D band 16#3fff bor 16#8000, E]).

find_file(DataDir, Prefix, N) ->
    {_Filename, Path} = machi_util:make_data_filename(DataDir, Prefix, "*", N),
    filelib:wildcard(Path).

list_files(DataDir, Prefix) ->
    {F_bin, Path} = machi_util:make_data_filename(DataDir, Prefix, "*", "*"),
    filelib:wildcard(binary_to_list(F_bin), filename:dirname(Path)).

make_filename_mgr_name(FluName) when is_atom(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_filename_mgr").

handle_find_file(Tid, {coc,CoC_Namespace,CoC_Locator}, Prefix, DataDir) ->
    N = machi_util:read_max_filenum(DataDir, CoC_Namespace, CoC_Locator, Prefix),
    {File, Cleanup} = case find_file(DataDir, Prefix, N) of
        [] ->
            {find_or_make_filename(Tid, DataDir, CoC_Namespace, CoC_Locator, Prefix, N), false};
        [H] -> {H, true};
        [Fn | _ ] = L ->
            lager:debug(
              "Searching for a matching file to prefix ~p and sequence number ~p gave multiples: ~p",
              [Prefix, N, L]),
            {Fn, true}
    end,
    maybe_cleanup(Tid, {Prefix, N}, Cleanup),
    filename:basename(File).

find_or_make_filename(Tid, DataDir, CoC_Namespace, CoC_Locator, Prefix, N) ->
    case ets:lookup(Tid, {CoC_Namespace, CoC_Locator, Prefix, N}) of
         [] ->
            F = generate_filename(DataDir, CoC_Namespace, CoC_Locator, Prefix, N),
            true = ets:insert_new(Tid, {{CoC_Namespace, CoC_Locator, Prefix, N}, F}),
            F;
        [{_Key, File}] ->
            File
    end.

generate_filename(DataDir, CoC_Namespace, CoC_Locator, Prefix, N) ->
    {F, _} = machi_util:make_data_filename(
              DataDir,
              CoC_Namespace, CoC_Locator, Prefix,
              generate_uuid_v4_str(),
              N),
    binary_to_list(F).

maybe_cleanup(_Tid, _Key, false) ->
    ok;
maybe_cleanup(Tid, Key, true) ->
    true = ets:delete(Tid, Key).

increment_and_cache_filename(Tid, DataDir, {coc,CoC_Namespace,CoC_Locator}, Prefix) ->
    ok = machi_util:increment_max_filenum(DataDir, CoC_Namespace, CoC_Locator, Prefix),
    N = machi_util:read_max_filenum(DataDir, CoC_Namespace, CoC_Locator, Prefix),
    F = generate_filename(DataDir, CoC_Namespace, CoC_Locator, Prefix, N),
    true = ets:insert_new(Tid, {{CoC_Namespace, CoC_Locator, Prefix, N}, F}),
    filename:basename(F).



-ifdef(TEST).

-endif.
