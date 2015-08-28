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

%% @doc This is a metadata service for the machi FLU which currently
%% tracks the mappings between prefixes, filenames and file proxies.
%%
%% The service takes a given hash space and spreads it out over a
%% pool of N processes which are responsible for 1/Nth the hash 
%% space. When a user requests an operation on a particular file
%% prefix, the prefix is hashed into the hash space and the request
%% forwarded to a particular manager responsible for that slice
%% of the hash space.
%%
%% The current hash implementation is `erlang:phash2/1' which has
%% a range between 0..2^27-1 or 134,217,727. 

-module(machi_flu_metadata_mgr).
-behaviour(gen_server).

-include("machi.hrl").
-include_lib("kernel/include/file.hrl").

-define(MAX_MGRS, 10). %% number of managers to start by default.
-define(HASH(X), erlang:phash2(X)). %% hash algorithm to use
-define(TIMEOUT, 10 * 1000). %% 10 second timeout

-record(state, {name    :: atom(),
                datadir :: string(),
                tid     :: ets:tid()
               }).

%% This record goes in the ets table where prefix is the key
-record(md, {prefix            :: string(),
             file_proxy_pid    :: undefined|pid(),
             mref              :: undefined|reference(), %% monitor ref for file proxy
             current_file      :: undefined|string(),
             next_file_num = 0 :: non_neg_integer()
            }).

%% public api
-export([
         start_link/2,
         lookup_manager_pid/1,
         lookup_proxy_pid/1,
         start_proxy_pid/1,
         stop_proxy_pid/1,
         lookup_files/1
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

%% Public API

start_link(Name, DataDir) when is_atom(Name) andalso is_list(DataDir) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, DataDir], []).

lookup_manager_pid(Data) ->
    whereis(get_manager_atom(Data)).

lookup_proxy_pid(Data) ->
    gen_server:call(get_manager_atom(Data), {proxy_pid, Data}, ?TIMEOUT).

start_proxy_pid(Data) ->
    gen_server:call(get_manager_atom(Data), {start_proxy_pid, Data}, ?TIMEOUT).

stop_proxy_pid(Data) ->
    gen_server:call(get_manager_atom(Data), {stop_proxy_pid, Data}, ?TIMEOUT).

lookup_files(Data) ->
    gen_server:call(get_manager_atom(Data), {files, Data}, ?TIMEOUT).

%% gen_server callbacks
init([Name, DataDir]) ->
    Tid = ets:new(Name, [{keypos, 2}, {read_concurrency, true}, {write_concurrency, true}]),
    {ok, #state{ name = Name, datadir = DataDir, tid = Tid}}.

handle_cast(Req, State) ->
    lager:warning("Got unknown cast ~p", [Req]),
    {noreply, State}.

handle_call({proxy_pid, Prefix}, _From, State = #state{ tid = Tid }) ->
    Reply = case lookup_md(Tid, Prefix) of
                not_found -> undefined;
                R -> R#md.file_proxy_pid
    end,
    {reply, Reply, State};
handle_call({start_proxy_pid, Prefix}, _From, State = #state{ tid = Tid, datadir = D }) ->
    {Pid, NewR} = case lookup_md(Tid, Prefix) of
        not_found ->
            R0 = start_file_proxy(D, Prefix),
            {R0#md.file_proxy_pid, R0};
        #md{ file_proxy_pid = undefined } = R ->
            R1 = start_file_proxy(D, Prefix, R),
            {R1#md.file_proxy_pid, R1};
        #md{ file_proxy_pid = Pid0 } ->
            {Pid0, false}
    end,
    NewR1 = maybe_monitor_pid(Pid, NewR),
    maybe_update_ets(Tid, NewR1),
    {reply, {ok, Pid}, State};
handle_call({stop_proxy_pid, Prefix}, _From, State = #state{ tid = Tid }) ->
    case lookup_md(Tid, Prefix) of
        not_found ->
            ok;
        #md{ file_proxy_pid = undefined } ->
            ok;
        #md{ file_proxy_pid = Pid, mref = M } = R ->
            demonitor(M, [flush]),
            machi_file_proxy:stop(Pid),
            maybe_update_ets(Tid, R#md{ file_proxy_pid = undefined, mref = undefined })
    end,
    {reply, ok, State};
handle_call({files, Prefix}, _From, State = #state{ datadir = D }) ->
    {reply, list_files(D, Prefix), State};
handle_call(Req, From, State) ->
    lager:warning("Got unknown call ~p from ~p", [Req, From]),
    {reply, hoge, State}.

handle_info({'DOWN', Mref, process, Pid, normal}, State = #state{ tid = Tid }) ->
    lager:debug("file proxy ~p shutdown normally", [Pid]),
    clear_ets(Tid, Mref),
    {noreply, State};
handle_info({'DOWN', Mref, process, Pid, file_rollover}, State = #state{ tid = Tid, datadir = D }) ->
    lager:info("file proxy ~p shutdown because of file rollover", [Pid]),
    R = find_md_record(Tid, Mref),
    NewR = start_file_proxy(D, R#md.prefix, R#md{ file_proxy_pid = undefined, 
                                                  mref = undefined, 
                                                  current_file = undefined }),
    NewR1 = maybe_monitor_pid(NewR#md.file_proxy_pid, NewR),
    maybe_update_ets(Tid, NewR1),
    {noreply, State};
handle_info({'DOWN', Mref, process, Pid, wedged}, State = #state{ tid = Tid }) ->
    lager:error("file proxy ~p shutdown because it's wedged", [Pid]),
    clear_ets(Tid, Mref),
    {noreply, State};
handle_info({'DOWN', Mref, process, Pid, Error}, State = #state{ tid = Tid }) ->
    lager:error("file proxy ~p shutdown because ~p", [Pid, Error]),
    clear_ets(Tid, Mref),
    {noreply, State};


handle_info(Info, State) ->
    lager:warning("Got unknown info ~p", [Info]),
    {noreply, State}.

terminate(Reason, _State) ->
    lager:info("Shutting down because ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private functions

compute_hash(Data) ->
    ?HASH(Data).

compute_worker(Hash) ->
    Hash rem ?MAX_MGRS.

build_metadata_mgr_name(N) when is_integer(N) ->
    list_to_atom("machi_flu_metadata_mgr_" ++ integer_to_list(N)).

get_manager_atom(Prefix) ->
    build_metadata_mgr_name(compute_worker(compute_hash(Prefix))).

lookup_md(Tid, Prefix) ->
    case ets:lookup(Tid, Prefix) of
         [] -> not_found;
        [R] -> R
    end.

find_or_create_filename(D, Prefix) ->
    N = machi_util:read_max_filenum(D, Prefix),
    find_or_create_filename(D, Prefix, #md{ prefix = Prefix, next_file_num = N }).

find_or_create_filename(D, Prefix, R = #md{ current_file = undefined, next_file_num = 0 }) ->
    F = make_filename(Prefix, 0),
    ok = machi_util:increment_max_filenum(D, Prefix),
    find_or_create_filename(D, Prefix, R#md{ current_file = F, next_file_num = 1});
find_or_create_filename(D, Prefix, R = #md{ current_file = undefined, next_file_num = N }) ->
    File = find_file(D, Prefix, N),
    File1 = case File of
                not_found -> make_filename(Prefix, N);
                _ -> File
    end,
    {_, Path} = machi_util:make_data_filename(D, File1),
    F = maybe_make_new_file(File1, Prefix, N, file:read_file_info(Path)),
    R#md{ current_file = F }.

start_file_proxy(D, Prefix) ->
    start_file_proxy(D, Prefix, find_or_create_filename(D, Prefix)).
start_file_proxy(D, Prefix, #md{ current_file = undefined }) ->
    start_file_proxy(D, Prefix, find_or_create_filename(D, Prefix));
start_file_proxy(D, _Prefix, R = #md{ file_proxy_pid = undefined, current_file = F } ) ->
    {ok, Pid} = machi_file_proxy_sup:start_proxy(D, F),
    R#md{ file_proxy_pid = Pid };
start_file_proxy(_D, _Prefix, R = #md{ file_proxy_pid = _Pid }) ->
    R.

find_file(D, Prefix, N) ->
    {_, Path} = machi_util:make_data_filename(D, Prefix, "*", N),
    case filelib:wildcard(Path) of
        [] -> not_found;
        [F] -> F;
        [F|_Fs] -> F %% XXX FIXME: What to do when there's more than one match? 
                     %%            Arbitrarily pick the head for now, I guess.
    end.

maybe_make_new_file(F, Prefix, N, {ok, #file_info{ size = S }}) when S >= ?MAX_FILE_SIZE ->
    lager:info("~p is larger than ~p. Starting new file.", [F, ?MAX_FILE_SIZE]),
    make_filename(Prefix, N);
maybe_make_new_file(F, Prefix, N, Err = {error, _Reason}) ->
    lager:error("When reading file information about ~p, got ~p! Going to use new file",
                [F, Err]),
    make_filename(Prefix, N);
maybe_make_new_file(F, _Prefix, _N, _Info) ->
    F.
    
make_filename(Prefix, N) ->
    {F, _} = machi_util:make_data_filename("", Prefix, something(), N),
    F.

%% XXX FIXME: Might just be time to generate UUIDs
something() ->
    lists:flatten(io_lib:format("~.36B~.36B",                    
                                [element(3,now()),
                                list_to_integer(os:getpid())])).

maybe_monitor_pid(_Pid, false) -> false;
maybe_monitor_pid(Pid, R = #md{ mref = undefined }) ->
    Mref = monitor(process, Pid),
    R#md{ mref = Mref };
maybe_monitor_pid(_Pid, R) -> R.

maybe_update_ets(_Tid, false) -> ok;
maybe_update_ets(Tid, R) ->
    ets:insert(Tid, R).

list_files(D, Prefix) ->
    {F, Path} = machi_util:make_data_filename(D, Prefix, "*", "*"),
    {ok, filelib:wildcard(F, filename:dirname(Path))}.

clear_ets(Tid, Mref) ->
    R = find_md_record(Tid, Mref),
    maybe_update_ets(Tid, R#md{ file_proxy_pid = undefined, mref = undefined }).

find_md_record(Tid, Mref) ->
    [R] = ets:match(Tid, {md, '_', '_', Mref, '_', '_'}),
    R.
