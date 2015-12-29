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
%% tracks the mappings between filenames and file proxies.
%%
%% The service takes a given hash space and spreads it out over a
%% pool of N processes which are responsible for 1/Nth the hash 
%% space. When a user requests an operation on a particular file
%% the filename is hashed into the hash space and the request
%% forwarded to a particular manager responsible for that slice
%% of the hash space.
%%
%% The current hash implementation is `erlang:phash2/1' which has
%% a range between 0..2^27-1 or 134,217,727. 

-module(machi_flu_metadata_mgr).
-behaviour(gen_server).

-include("machi.hrl").

-define(MAX_MGRS, 10). %% number of managers to start by default.
-define(HASH(X), erlang:phash2(X)). %% hash algorithm to use
-define(TIMEOUT, 10 * 1000). %% 10 second timeout

-define(KNOWN_FILES_LIST_PREFIX, "known_files_").

-record(state, {fluname :: atom(),
                datadir :: string(),
                tid     :: ets:tid(),
                cnt     :: non_neg_integer(),
                trimmed_files :: machi_plist:plist()
               }).

%% This record goes in the ets table where filename is the key
-record(md, {filename          :: string(), 
             proxy_pid         :: undefined|pid(),
             mref              :: undefined|reference() %% monitor ref for file proxy
            }).

%% public api
-export([
         child_spec/4,
         start_link/4,
         lookup_manager_pid/2,
         lookup_proxy_pid/2,
         start_proxy_pid/2,
         stop_proxy_pid/2,
         build_metadata_mgr_name/2,
         trim_file/2
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
build_metadata_mgr_name(FluName, N) when is_atom(FluName) andalso is_integer(N) ->
    list_to_atom(atom_to_list(FluName) ++ "_metadata_mgr_" ++ integer_to_list(N)).

child_spec(FluName, C, DataDir, N) ->
    Name = build_metadata_mgr_name(FluName, C),
    {Name,
     {?MODULE, start_link, [FluName, Name, DataDir, N]},
     permanent, 5000, worker, [?MODULE]}.

start_link(FluName, Name, DataDir, Num) when is_atom(Name) andalso is_list(DataDir) ->
    gen_server:start_link({local, Name}, ?MODULE, [FluName, Name, DataDir, Num], []).

lookup_manager_pid(FluName, {file, Filename}) ->
    whereis(get_manager_atom(FluName, Filename)).

lookup_proxy_pid(FluName, {file, Filename}) ->
    gen_server:call(get_manager_atom(FluName, Filename), {proxy_pid, Filename}, ?TIMEOUT).

start_proxy_pid(FluName, {file, Filename}) ->
    gen_server:call(get_manager_atom(FluName, Filename), {start_proxy_pid, Filename}, ?TIMEOUT).

stop_proxy_pid(FluName, {file, Filename}) ->
    gen_server:call(get_manager_atom(FluName, Filename), {stop_proxy_pid, Filename}, ?TIMEOUT).

trim_file(FluName, {file, Filename}) ->
    gen_server:call(get_manager_atom(FluName, Filename), {trim_file, Filename}, ?TIMEOUT).

%% gen_server callbacks
init([FluName, Name, DataDir, Num]) ->
    %% important: we'll need another persistent storage to
    %% remember deleted (trimmed) file, to prevent resurrection after
    %% flu restart and append.
    FileListFileName =
        filename:join([DataDir, ?KNOWN_FILES_LIST_PREFIX ++ atom_to_list(FluName)]),
    {ok, PList} = machi_plist:open(FileListFileName, []),
    %% TODO make sure all files non-existent, if any remaining files
    %% here, just delete it. They're in the list *because* they're all
    %% trimmed.

    Tid = ets:new(Name, [{keypos, 2}, {read_concurrency, true}, {write_concurrency, true}]),
    {ok, #state{fluname = FluName, datadir = DataDir, tid = Tid, cnt = Num,
                trimmed_files=PList}}.

handle_cast(Req, State) ->
    lager:warning("Got unknown cast ~p", [Req]),
    {noreply, State}.

handle_call({proxy_pid, Filename}, _From, State = #state{ tid = Tid }) ->
    Reply = case lookup_md(Tid, Filename) of
                not_found -> undefined;
                R -> R#md.proxy_pid
    end,
    {reply, Reply, State};

handle_call({start_proxy_pid, Filename}, _From,
            State = #state{ fluname = N, tid = Tid, datadir = D,
                            trimmed_files=TrimmedFiles}) ->
    case machi_plist:find(TrimmedFiles, Filename) of
        false ->
            NewR = case lookup_md(Tid, Filename) of
                       not_found ->
                           start_file_proxy(N, D, Filename);
                       #md{ proxy_pid = undefined } = R0 ->
                           start_file_proxy(N, D, R0);
                       #md{ proxy_pid = _Pid } = R1 ->
                           R1
                   end,
            update_ets(Tid, NewR),
            {reply, {ok, NewR#md.proxy_pid}, State};
        true ->
            {reply, {error, trimmed}, State}
    end;

handle_call({stop_proxy_pid, Filename}, _From, State = #state{ tid = Tid }) ->
    case lookup_md(Tid, Filename) of
        not_found ->
            ok;
        #md{ proxy_pid = undefined } ->
            ok;
        #md{ proxy_pid = Pid, mref = M } = R ->
            demonitor(M, [flush]),
            machi_file_proxy:stop(Pid),
            update_ets(Tid, R#md{ proxy_pid = undefined, mref = undefined })
    end,
    {reply, ok, State};

handle_call({trim_file, Filename}, _,
            S = #state{trimmed_files = TrimmedFiles }) ->
    case machi_plist:add(TrimmedFiles, Filename) of
        {ok, TrimmedFiles2} ->
            {reply, ok, S#state{trimmed_files=TrimmedFiles2}};
        Error ->
            {reply, Error, S}
    end;

handle_call(Req, From, State) ->
    lager:warning("Got unknown call ~p from ~p", [Req, From]),
    {reply, hoge, State}.

handle_info({'DOWN', Mref, process, Pid, normal}, State = #state{ tid = Tid }) ->
    lager:debug("file proxy ~p shutdown normally", [Pid]),
    clear_ets(Tid, Mref),
    {noreply, State};

handle_info({'DOWN', Mref, process, Pid, file_rollover}, State = #state{ fluname = FluName, 
                                                                         tid = Tid }) ->
    lager:info("file proxy ~p shutdown because of file rollover", [Pid]),
    R = get_md_record_by_mref(Tid, Mref),
    {Prefix, NS, NSLocator, _, _} =
        machi_util:parse_filename(R#md.filename),

    %% We only increment the counter here. The filename will be generated on the 
    %% next append request to that prefix and since the filename will have a new
    %% sequence number it probably will be associated with a different metadata
    %% manager. That's why we don't want to generate a new file name immediately
    %% and use it to start a new file proxy.
    NSInfo = #ns_info{name=NS, locator=NSLocator},
    ok = machi_flu_filename_mgr:increment_prefix_sequence(FluName, NSInfo, {prefix, Prefix}),

    %% purge our ets table of this entry completely since it is likely the
    %% new filename (whenever it comes) will be in a different manager than
    %% us.
    purge_ets(Tid, R),
    {noreply, State};

handle_info({'DOWN', Mref, process, Pid, wedged}, State = #state{ tid = Tid }) ->
    lager:error("file proxy ~p shutdown because it's wedged", [Pid]),
    clear_ets(Tid, Mref),
    {noreply, State};
handle_info({'DOWN', _Mref, process, Pid, trimmed}, State = #state{ tid = _Tid }) ->
    lager:debug("file proxy ~p shutdown because the file was trimmed", [Pid]),
    {noreply, State};
handle_info({'DOWN', Mref, process, Pid, Error}, State = #state{ tid = Tid }) ->
    lager:error("file proxy ~p shutdown because ~p", [Pid, Error]),
    clear_ets(Tid, Mref),
    {noreply, State};

handle_info(Info, State) ->
    lager:warning("Got unknown info ~p", [Info]),
    {noreply, State}.

terminate(Reason, _State = #state{trimmed_files=TrimmedFiles}) ->
    lager:info("Shutting down because ~p", [Reason]),
    machi_plist:close(TrimmedFiles),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private functions

compute_hash(Data) ->
    ?HASH(Data).

compute_worker(Hash) ->
    MgrCount = get_env(metadata_manager_count, ?MAX_MGRS),
    (Hash rem MgrCount) + 1.
    %% TODO MARK: Hrm, the intermittent failures of both of the tests
    %% in machi_cr_client_test.erl were due to this func returning 0.
    %% But machi_flu_metadata_mgr_sup:init() doesn't make a child with N=0.
    %%
    %% The remaining puzzle for me, which I'm now punting to you, sorry,
    %% is why those two tests would sometimes pass, which implies to me that
    %% sometimes the code path used by those tests never chooses worker 0
    %% and occasionally it does choose worker 0.

get_manager_atom(FluName, Data) ->
    build_metadata_mgr_name(FluName, compute_worker(compute_hash(Data))).

lookup_md(Tid, Data) ->
    case ets:lookup(Tid, Data) of
         [] -> not_found;
        [R] -> R
    end.

start_file_proxy(FluName, D, R = #md{filename = F} ) ->
    {ok, Pid} = machi_file_proxy_sup:start_proxy(FluName, D, F),
    Mref = monitor(process, Pid),
    R#md{ proxy_pid = Pid, mref = Mref };

start_file_proxy(FluName, D, Filename) ->
    start_file_proxy(FluName, D, #md{ filename = Filename }).

update_ets(Tid, R) ->
    ets:insert(Tid, R).

clear_ets(Tid, Mref) ->
    R = get_md_record_by_mref(Tid, Mref),
    update_ets(Tid, R#md{ proxy_pid = undefined, mref = undefined }).

purge_ets(Tid, R) ->
    true = ets:delete_object(Tid, R).

get_md_record_by_mref(Tid, Mref) ->
    [R] = ets:match_object(Tid, {md, '_', '_', Mref}),
    R.

get_env(Setting, Default) ->
    case application:get_env(machi, Setting) of
        undefined -> Default;
        {ok, V} -> V
    end.
