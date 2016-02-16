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

%% @doc Erlang API for the Machi client-implemented Chain Replication
%% (CORFU-style) protocol.
%%
%% Please see {@link machi_flu1_client} the "Client API implemntation notes"
%% section for how this module relates to the rest of the client API
%% implementation.
%%
%% The API here is much simpler than the {@link machi_flu1_client} or
%% {@link machi_proxy_flu1_client} APIs.  This module's API is a
%% proposed simple-but-complete form for clients who are not
%% interested in being an active participant in a Machi cluster and to
%% have the responsibility for Machi internals, i.e., client-side
%% Chain Replication, client-side read repair, client-side tracking of
%% internal Machi epoch &amp; projection changes, etc.
%%
%% This client is implemented as a long-lived Erlang process using
%% `gen_server'-style OTP code practice.  A naive client can expect
%% that this process will manage all transient TCP session
%% disconnections and Machi chain reconfigurations.  This client's
%% efforts are best-effort and can require some time to retry
%% operations in certain failure cases, i.e., up to several seconds
%% during a Machi projection &amp; epoch change when a new server is
%% added to the chain.
%%
%% Doc TODO: Once this API stabilizes, add all relevant data type details
%% to the EDoc here.

-module(machi_cr_client).

-behaviour(gen_server).

-include("machi.hrl").
-include("machi_projection.hrl").
-include("machi_verbose.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST.

-export([start_link/1, start_link/2]).
%% FLU1 API
-export([
         %% File API
         append_chunk/5,
         append_chunk/6, append_chunk/7,
         write_chunk/6, write_chunk/7,
         read_chunk/6, read_chunk/7,
         trim_chunk/5, trim_chunk/6,
         checksum_list/2, checksum_list/3,
         list_files/1, list_files/2,

         %% Common API
         quit/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(FLU_PC, machi_proxy_flu1_client).
-define(TIMEOUT, 2*1000).
-define(DEFAULT_TIMEOUT, 10*1000).
-define(MAX_RUNTIME, 8*1000).
-define(WORST_PROJ, #projection_v1{epoch_number=0,epoch_csum= <<>>,
                                   members_dict=[]}).

-record(state, {
          members_dict    :: p_srvr_dict(),
          proxies_dict    :: orddict:orddict(),
          epoch_id,
          proj,
          bad_proj,
          opts            :: proplists:proplist()
         }).

%% @doc Start a local, long-lived process that will be our steady
%% &amp; reliable communication proxy with the fickle &amp; flaky
%% remote Machi server.

start_link(P_srvr_list) ->
    gen_server:start_link(?MODULE, [P_srvr_list, []], []).

start_link(P_srvr_list, Opts) ->
    gen_server:start_link(?MODULE, [P_srvr_list, Opts], []).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(PidSpec, NSInfo, Prefix, Chunk, CSum) ->
    append_chunk(PidSpec, NSInfo, Prefix, Chunk, CSum, #append_opts{}, ?DEFAULT_TIMEOUT).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(PidSpec, NSInfo, Prefix, Chunk, CSum, #append_opts{}=Opts) ->
    append_chunk(PidSpec, NSInfo, Prefix, Chunk, CSum, Opts, ?DEFAULT_TIMEOUT).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(PidSpec, NSInfo, Prefix, Chunk, CSum, #append_opts{}=Opts, Timeout0) ->
    NSInfo2 = machi_util:ns_info_default(NSInfo),
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {append_chunk,
                                    NSInfo2, Prefix, Chunk, CSum, Opts, TO}},
                    Timeout).

%% @doc Write a chunk of data (that has already been
%% allocated/sequenced by an earlier append_chunk() call) to
%% `File' at `Offset'.

write_chunk(PidSpec, NSInfo, File, Offset, Chunk, CSum) ->
    write_chunk(PidSpec, NSInfo, File, Offset, Chunk, CSum, ?DEFAULT_TIMEOUT).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

write_chunk(PidSpec, NSInfo, File, Offset, Chunk, CSum, Timeout0) ->
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {write_chunk, NSInfo, File, Offset, Chunk, CSum, TO}},
                    Timeout).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, NSInfo, File, Offset, Size, Opts) ->
    read_chunk(PidSpec, NSInfo, File, Offset, Size, Opts, ?DEFAULT_TIMEOUT).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, NSInfo, File, Offset, Size, Opts, Timeout0) ->
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {read_chunk, NSInfo, File, Offset, Size, Opts, TO}},
                    Timeout).

%% @doc Trim a chunk of data of size `Size' from `File' at `Offset'.

trim_chunk(PidSpec, NSInfo, File, Offset, Size) ->
    trim_chunk(PidSpec, NSInfo, File, Offset, Size, ?DEFAULT_TIMEOUT).

%% @doc Trim a chunk of data of size `Size' from `File' at `Offset'.

trim_chunk(PidSpec, NSInfo, File, Offset, Size, Timeout0) ->
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {trim_chunk, NSInfo, File, Offset, Size, TO}},
                    Timeout).

%% @doc Fetch the list of chunk checksums for `File'.

checksum_list(PidSpec, File) ->
    checksum_list(PidSpec, File, ?DEFAULT_TIMEOUT).

%% @doc Fetch the list of chunk checksums for `File'.

checksum_list(PidSpec, File, Timeout0) ->
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {checksum_list, File, TO}},
                    Timeout).

%% @doc Fetch the list of all files on the remote FLU.

list_files(PidSpec) ->
    list_files(PidSpec, ?DEFAULT_TIMEOUT).

%% @doc Fetch the list of all files on the remote FLU.

list_files(PidSpec, Timeout0) ->
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {list_files, TO}},
                    Timeout).

%% @doc Quit &amp; close the connection to remote FLU and stop our
%% proxy process.

quit(PidSpec) ->
    gen_server:call(PidSpec, quit, ?DEFAULT_TIMEOUT).

%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([P_srvr_list, Opts]) ->
    MembersDict = orddict:from_list([{P#p_srvr.name, P} || P <- P_srvr_list]),
    ProxiesDict = ?FLU_PC:start_proxies(MembersDict),
    {ok, #state{members_dict=MembersDict, proxies_dict=ProxiesDict, opts=Opts}}.

handle_call({req, Req}, From, S) ->
    handle_call2(Req, From, update_proj(S));
handle_call(quit, _From, #state{members_dict=MembersDict}=S) ->
    ?FLU_PC:stop_proxies(MembersDict),
    {stop, normal, ok, S};
handle_call(_Request, _From, S) ->
    Reply = whaaaaaaaaaaaaaaaaaaaa,
    {reply, Reply, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    io:format(user, "~s:handle_info: ~p\n", [?MODULE, _Info]),
    {noreply, S}.

terminate(_Reason, #state{proxies_dict=ProxiesDict}=_S) ->
    _ = ?FLU_PC:stop_proxies(ProxiesDict),
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call2({append_chunk, NSInfo,
              Prefix, Chunk, CSum, Opts, TO}, _From, S) ->
    do_append_head(NSInfo, Prefix,
                   Chunk, CSum, Opts, 0, os:timestamp(), TO, S);
handle_call2({write_chunk, NSInfo, File, Offset, Chunk, CSum, TO}, _From, S) ->
    do_write_head(NSInfo, File, Offset, Chunk, CSum, 0, os:timestamp(), TO, S);
handle_call2({read_chunk, NSInfo, File, Offset, Size, Opts, TO}, _From, S) ->
    do_read_chunk(NSInfo, File, Offset, Size, Opts, 0, os:timestamp(), TO, S);
handle_call2({trim_chunk, NSInfo, File, Offset, Size, TO}, _From, S) ->
    do_trim_chunk(NSInfo, File, Offset, Size, 0, os:timestamp(), TO, S);
handle_call2({checksum_list, File, TO}, _From, S) ->
    do_checksum_list(File, 0, os:timestamp(), TO, S);
handle_call2({list_files, TO}, _From, S) ->
    do_list_files(0, os:timestamp(), TO, S).

do_append_head(NSInfo, Prefix,
               Chunk, CSum, Opts, 0=Depth, STime, TO, S) ->
    do_append_head2(NSInfo, Prefix,
                    Chunk, CSum, Opts, Depth + 1, STime, TO, S);
do_append_head(NSInfo, Prefix,
               Chunk, CSum, Opts, Depth, STime, TO, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > TO ->
            {reply, {error, partition}, S};
       true ->
            %% This is suboptimal for performance: there are some paths
            %% through this point where our current projection is good
            %% enough.  But we're going to try to keep the code as simple
            %% as we can for now.
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_append_head(NSInfo, Prefix,
                                   Chunk, CSum, Opts, Depth + 1,
                                   STime, TO, S2);
                _ ->
                    do_append_head2(NSInfo, Prefix,
                                    Chunk, CSum, Opts, Depth + 1,
                                    STime, TO, S2)
            end
    end.

do_append_head2(NSInfo, Prefix,
                Chunk, CSum, Opts, Depth, STime, TO,
                #state{proj=P}=S) ->
    [HeadFLU|_RestFLUs] = mutation_flus(P),
    case is_witness_flu(HeadFLU, P) of
        true ->
            case witnesses_use_our_epoch(S) of
                true ->
                    do_append_head3(NSInfo, Prefix,
                                    Chunk, CSum, Opts, Depth,
                                    STime, TO, S);
                false ->
                    %% Bummer, go back to the beginning and retry.
                    do_append_head(NSInfo, Prefix,
                                   Chunk, CSum, Opts, Depth,
                                   STime, TO, S)
            end;
        false ->
            do_append_head3(NSInfo, Prefix,
                            Chunk, CSum, Opts, Depth, STime, TO, S)
    end.

do_append_head3(NSInfo, Prefix,
                Chunk, CSum, Opts, Depth, STime, TO,
                #state{epoch_id=EpochID, proj=P, proxies_dict=PD}=S) ->
    [HeadFLU|RestFLUs] = non_witness_flus(mutation_flus(P), P),
    Proxy = orddict:fetch(HeadFLU, PD),
    case ?FLU_PC:append_chunk(Proxy, NSInfo, EpochID,
                              Prefix, Chunk, CSum, Opts, ?TIMEOUT) of
        {ok, {Offset, _Size, File}=_X} ->
            do_wr_app_midtail(RestFLUs, NSInfo, Prefix,
                              File, Offset, Chunk, CSum, Opts,
                              [HeadFLU], 0, STime, TO, append, S);
        {error, bad_checksum}=BadCS ->
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_append_head(NSInfo, Prefix,
                           Chunk, CSum, Opts, Depth, STime, TO, S);
        {error, written} ->
            %% Implicit sequencing + this error = we don't know where this
            %% written block is.  But we lost a race.  Repeat, with a new
            %% sequencer assignment.
            do_append_head(NSInfo, Prefix,
                           Chunk, CSum, Opts, Depth, STime, TO, S);
        {error, trimmed} = Err ->
            %% TODO: behaviour
            {reply, Err, S};
        {error, not_written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,
                  Prefix,iolist_size(Chunk)})
    end.

do_wr_app_midtail(RestFLUs, NSInfo, Prefix,
                  File, Offset, Chunk, CSum, Opts,
                  Ws, Depth, STime, TO, MyOp, S)
  when RestFLUs == [] orelse Depth == 0 ->
       do_wr_app_midtail2(RestFLUs, NSInfo, Prefix,
                          File, Offset, Chunk, CSum, Opts,
                          Ws, Depth + 1, STime, TO, MyOp, S);
do_wr_app_midtail(_RestFLUs, NSInfo, Prefix, File,
                  Offset, Chunk, CSum, Opts,
                  Ws, Depth, STime, TO, MyOp, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > TO ->
            {reply, {error, partition}, S};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                undefined ->
                    {reply, {error, partition}, S};
                P2 ->
                    RestFLUs2 = mutation_flus(P2),
                    case RestFLUs2 -- Ws of
                        RestFLUs2 ->
                            if Prefix == undefined -> % atom! not binary()!!
                                    {error, partition};
                               MyOp == append ->
                                    %% None of the writes that we have done so
                                    %% far are to FLUs that are in the
                                    %% RestFLUs2 list.  We are pessimistic
                                    %% here and assume that those FLUs are
                                    %% permanently dead.  Start over with a
                                    %% new sequencer assignment, at the 2nd
                                    %% have of the impl (we have already slept
                                    %% & refreshed the projection).
                                    do_append_head2(NSInfo,
                                                    Prefix, Chunk, CSum, Opts,
                                                    Depth, STime, TO, S2);
                               MyOp == write ->
                                    do_wr_app_midtail2(RestFLUs2,
                                                       NSInfo,
                                                       Prefix, File, Offset,
                                                       Chunk, CSum, Opts,
                                                       Ws, Depth + 1, STime, TO,
                                                       MyOp, S2)
                            end;
                        RestFLUs3 ->
                            do_wr_app_midtail2(RestFLUs3,
                                               NSInfo,
                                               Prefix, File, Offset,
                                               Chunk, CSum, Opts,
                                               Ws, Depth + 1, STime, TO,
                                               MyOp, S2)
                    end
            end
    end.

do_wr_app_midtail2([], _NSInfo,
                   _Prefix, File, Offset, Chunk,
                   _CSum, _Opts, _Ws, _Depth, _STime, _TO, _MyOp, S) ->
    {reply, {ok, {Offset, chunk_wrapper_size(Chunk), File}}, S};
do_wr_app_midtail2([FLU|RestFLUs]=FLUs, NSInfo,
                   Prefix, File, Offset, Chunk,
                   CSum, Opts, Ws, Depth, STime, TO, MyOp, 
                   #state{epoch_id=EpochID, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(FLU, PD),
    case ?FLU_PC:write_chunk(Proxy, NSInfo, EpochID, File, Offset, Chunk, CSum, ?TIMEOUT) of
        ok ->
            do_wr_app_midtail2(RestFLUs, NSInfo, Prefix,
                               File, Offset, Chunk,
                               CSum, Opts, [FLU|Ws], Depth, STime, TO, MyOp, S);
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_wr_app_midtail(FLUs, NSInfo, Prefix,
                              File, Offset, Chunk,
                              CSum, Opts, Ws, Depth, STime, TO, MyOp, S);
        {error, written} ->
            %% We know what the chunk ought to be, so jump to the
            %% middle of read-repair.
            Resume = {append, Offset, iolist_size(Chunk), File},
            do_repair_chunk(FLUs, Resume, Chunk, CSum, [], NSInfo, File, Offset,
                            iolist_size(Chunk), Depth, STime, S);
        {error, trimmed} = Err ->
            %% TODO: nothing can be done
            {reply, Err, S};
        {error, not_written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset})
    end.

witnesses_use_our_epoch(#state{proj=P}=S) ->
    Witnesses = witness_flus(P#projection_v1.upi, P),
    witnesses_use_our_epoch(Witnesses, S).

witnesses_use_our_epoch([], _S) ->
    true;
witnesses_use_our_epoch([FLU|RestFLUs],
                        #state{epoch_id=EpochID, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(FLU, PD),
    %% Check both that the EpochID is the same *and* not wedged!
    case ?FLU_PC:wedge_status(Proxy, ?TIMEOUT) of
        {ok, {false, EID,_,_}} when EID == EpochID ->
            witnesses_use_our_epoch(RestFLUs, S);
        _Else ->
            false
    end.

do_write_head(NSInfo, File, Offset, Chunk, CSum, 0=Depth, STime, TO, S) ->
    do_write_head2(NSInfo, File, Offset, Chunk, CSum, Depth + 1, STime, TO, S);
do_write_head(NSInfo, File, Offset, Chunk, CSum, Depth, STime, TO, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > TO ->
            {reply, {error, partition}, S};
       true ->
            %% This is suboptimal for performance: there are some paths
            %% through this point where our current projection is good
            %% enough.  But we're going to try to keep the code as simple
            %% as we can for now.
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_write_head(NSInfo, File, Offset, Chunk, CSum, Depth + 1,
                                  STime, TO, S2);
                _ ->
                    do_write_head2(NSInfo, File, Offset, Chunk, CSum, Depth + 1,
                                   STime, TO, S2)
            end
    end.

do_write_head2(NSInfo, File, Offset, Chunk, CSum, Depth, STime, TO,
               #state{epoch_id=EpochID, proj=P, proxies_dict=PD}=S) ->
    [HeadFLU|RestFLUs] = mutation_flus(P),
    Proxy = orddict:fetch(HeadFLU, PD),
    case ?FLU_PC:write_chunk(Proxy, NSInfo, EpochID, File, Offset, Chunk, CSum, ?TIMEOUT) of
        ok ->
            %% From this point onward, we use the same code & logic path as
            %% append does.
            Prefix=unused_write_path,
            Opts=unused_write_path,
            do_wr_app_midtail(RestFLUs, NSInfo, Prefix,
                              File, Offset, Chunk,
                              CSum, Opts, [HeadFLU], 0, STime, TO, write, S);
        {error, bad_checksum}=BadCS ->
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_write_head(NSInfo, File, Offset, Chunk, CSum, Depth, STime, TO, S);
        {error, written}=Err ->
            {reply, Err, S};
        {error, trimmed}=Err ->
            {reply, Err, S};
        {error, not_written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,
                  iolist_size(Chunk)})
    end.

do_read_chunk(NSInfo, File, Offset, Size, Opts, 0=Depth, STime, TO,
              #state{proj=#projection_v1{upi=[_|_]}}=S) -> % UPI is non-empty
    do_read_chunk2(NSInfo, File, Offset, Size, Opts, Depth + 1, STime, TO, S);
do_read_chunk(NSInfo, File, Offset, Size, Opts, Depth, STime, TO, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > TO ->
            {reply, {error, partition}, S};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_read_chunk(NSInfo, File, Offset, Size, Opts, Depth + 1, STime, TO, S2);
                _ ->
                    do_read_chunk2(NSInfo, File, Offset, Size, Opts, Depth + 1, STime, TO, S2)
            end
    end.

do_read_chunk2(NSInfo, File, Offset, Size, Opts, Depth, STime, TO,
               #state{proj=P, epoch_id=EpochID, proxies_dict=PD}=S) ->
    UPI = readonly_flus(P),
    Tail = lists:last(UPI),
    ConsistencyMode = P#projection_v1.mode,
    case ?FLU_PC:read_chunk(orddict:fetch(Tail, PD), NSInfo, EpochID,
                            File, Offset, Size, Opts, ?TIMEOUT) of
        {ok, {Chunks, Trimmed}} when is_list(Chunks), is_list(Trimmed) ->
            %% After partition heal, there could happen that heads may
            %% have chunk trimmed but tails may have chunk written -
            %% such repair couldn't be triggered in read time (because
            %% there's data!). In this case, repair should happen by
            %% partition heal event or some background
            %% hashtree-n-repair service. TODO. FIXME.
            {reply, {ok, {Chunks, Trimmed}}, S};
        %% {ok, BadChunk} ->
        %%     %% TODO cleaner handling of bad chunks
        %%     exit({todo, bad_chunk_size, ?MODULE, ?LINE, File, Offset, Size,
        %%           got, byte_size(BadChunk)});
        {error, bad_arg} = BadArg ->
            {reply, BadArg, S};
        {error, partial_read}=Err ->
            %% TODO: maybe this case we might need another repair?
            {reply, Err, S};
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            %% Maybe we need read repair here, too?
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_read_chunk(NSInfo, File, Offset, Size, Opts, Depth, STime, TO, S);
        {error, not_written} ->
            read_repair(ConsistencyMode, read, NSInfo, File, Offset, Size, Depth, STime, S);
            %% {reply, {error, not_written}, S};
        {error, written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size});
        {error, trimmed}=Err ->
            {reply, Err, S}
    end.

do_trim_chunk(NSInfo, File, Offset, Size, 0=Depth, STime, TO, S) ->
    do_trim_chunk(NSInfo, File, Offset, Size, Depth+1, STime, TO, S);

do_trim_chunk(NSInfo, File, Offset, Size, Depth, STime, TO, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > TO ->
            {reply, {error, partition}, S};
       true ->
            %% This is suboptimal for performance: there are some paths
            %% through this point where our current projection is good
            %% enough.  But we're going to try to keep the code as simple
            %% as we can for now.
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_trim_chunk(NSInfo, File, Offset, Size, Depth + 1,
                                  STime, TO, S2);
                _ ->
                    do_trim_chunk2(NSInfo, File, Offset, Size, Depth + 1,
                                   STime, TO, S2)
            end
    end.

do_trim_chunk2(NSInfo, File, Offset, Size, Depth, STime, TO,
               #state{epoch_id=EpochID, proj=P, proxies_dict=PD}=S) ->
    [HeadFLU|RestFLUs] = mutation_flus(P),
    Proxy = orddict:fetch(HeadFLU, PD),
    case ?FLU_PC:trim_chunk(Proxy, NSInfo, EpochID, File, Offset, Size, ?TIMEOUT) of
        ok ->
            do_trim_midtail(RestFLUs, undefined, NSInfo, File, Offset, Size,
                            [HeadFLU], 0, STime, TO, S);
        {error, trimmed} ->
            %% Maybe the trim had failed in the middle of the tail so re-run
            %% trim accross the whole chain.
            do_trim_midtail(RestFLUs, undefined, NSInfo, File, Offset, Size,
                            [HeadFLU], 0, STime, TO, S);
        {error, bad_checksum}=BadCS ->
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_trim_chunk(NSInfo, File, Offset, Size, Depth, STime, TO, S)
    end.

do_trim_midtail(RestFLUs, Prefix, NSInfo, File, Offset, Size,
                Ws, Depth, STime, TO, S)
  when RestFLUs == [] orelse Depth == 0 ->
       do_trim_midtail2(RestFLUs, Prefix, NSInfo, File, Offset, Size,
                        Ws, Depth + 1, STime, TO, S);
do_trim_midtail(_RestFLUs, Prefix, NSInfo, File, Offset, Size,
                Ws, Depth, STime, TO, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > TO ->
            {reply, {error, partition}, S};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                undefined ->
                    {reply, {error, partition}, S};
                P2 ->
                    RestFLUs2 = mutation_flus(P2),
                    case RestFLUs2 -- Ws of
                        RestFLUs2 ->
                            %% None of the writes that we have done so far
                            %% are to FLUs that are in the RestFLUs2 list.
                            %% We are pessimistic here and assume that
                            %% those FLUs are permanently dead.  Start
                            %% over with a new sequencer assignment, at
                            %% the 2nd have of the impl (we have already
                            %% slept & refreshed the projection).

                            if Prefix == undefined -> % atom! not binary()!!
                                    {error, partition};
                               true ->
                                    do_trim_chunk(NSInfo, Prefix, Offset, Size,
                                                  Depth, STime, TO, S2)
                            end;
                        RestFLUs3 ->
                            do_trim_midtail2(RestFLUs3, Prefix, NSInfo, File, Offset, Size,
                                             Ws, Depth + 1, STime, TO, S2)
                    end
            end
    end.

do_trim_midtail2([], _Prefix, _NSInfo, _File, _Offset, _Size,
                   _Ws, _Depth, _STime, _TO, S) ->
    {reply, ok, S};
do_trim_midtail2([FLU|RestFLUs]=FLUs, Prefix, NSInfo, File, Offset, Size,
                   Ws, Depth, STime, TO,
                   #state{epoch_id=EpochID, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(FLU, PD),
    case ?FLU_PC:trim_chunk(Proxy, NSInfo, EpochID, File, Offset, Size, ?TIMEOUT) of
        ok ->
            do_trim_midtail2(RestFLUs, Prefix, NSInfo, File, Offset, Size,
                             [FLU|Ws], Depth, STime, TO, S);
        {error, trimmed} ->
            do_trim_midtail2(RestFLUs, Prefix, NSInfo, File, Offset, Size,
                             [FLU|Ws], Depth, STime, TO, S);
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_trim_midtail(FLUs, Prefix, NSInfo, File, Offset, Size,
                            Ws, Depth, STime, TO, S)
    end.


%% Read repair: depends on the consistency mode that we're in:
%%
%% CP mode: If the head is written, then use it to repair UPI++Repairing.
%%          If head is not_written, then do nothing.
%% AP mode: If any FLU in UPI++Repairing is written, then use it to repair
%%          UPI+repairing.
%%          If all FLUs in UPI++Repairing are not_written, then do nothing.

%% Never matches because Depth is always incremented beyond 0 prior to
%% getting here.
%%
%% read_repair(ConsistencyMode, ReturnMode, NSInfo, File, Offset, Size, 0=Depth,
%%           STime, #state{proj=#projection_v1{upi=[_|_]}}=S) -> % UPI is non-empty
%%     read_repair2(ConsistencyMode, ReturnMode, NSInfo, File, Offset, Size, Depth + 1,
%%                  STime, S);
read_repair(ConsistencyMode, ReturnMode, NSInfo, File, Offset, Size, Depth,
            STime, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > ?MAX_RUNTIME ->
            {reply, {error, partition}, S};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    read_repair(ConsistencyMode, ReturnMode, NSInfo, File, Offset,
                                Size, Depth + 1, STime, S2);
                _ ->
                    read_repair2(ConsistencyMode, ReturnMode, NSInfo, File, Offset,
                                 Size, Depth + 1, STime, S2)
            end
    end.

read_repair2(cp_mode=ConsistencyMode,
             ReturnMode, NSInfo, File, Offset, Size, Depth, STime,
             #state{proj=P, epoch_id=EpochID, proxies_dict=PD}=S) ->
    %% TODO WTF was I thinking here??....
    Tail = lists:last(readonly_flus(P)),
    case ?FLU_PC:read_chunk(orddict:fetch(Tail, PD), NSInfo, EpochID,
                            File, Offset, Size, undefined, ?TIMEOUT) of
        {ok, Chunks} when is_list(Chunks) ->
            %% TODO: change to {Chunks, Trimmed} and have them repaired
            ToRepair = mutation_flus(P) -- [Tail],
            {Reply, S1} = do_repair_chunks(Chunks, ToRepair, ReturnMode,
                                           [Tail], NSInfo, File, Depth, STime, S, {ok, Chunks}),
            {reply, Reply, S1};
        %% {ok, BadChunk} ->
        %%     exit({todo, bad_chunk_size, ?MODULE, ?LINE, File, Offset,
        %%           Size, got, byte_size(BadChunk)});
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            read_repair(ConsistencyMode, ReturnMode, NSInfo, File, Offset,
                        Size, Depth, STime, S);
        {error, not_written} ->
            {reply, {error, not_written}, S};
        {error, written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size});
        {error, trimmed} ->
            %% TODO: Again, whole file was trimmed. Needs repair. How
            %% do we repair trimmed file (which was already unlinked)
            %% across the flu servers?
            exit({todo_should_repair_unlinked_files, ?MODULE, ?LINE, File})
    end;
read_repair2(ap_mode=ConsistencyMode,
             ReturnMode, NSInfo, File, Offset, Size, Depth, STime,
             #state{proj=P}=S) ->
    Eligible = mutation_flus(P),
    case try_to_find_chunk(Eligible, NSInfo, File, Offset, Size, S) of
        {ok, {Chunks, _Trimmed}, GotItFrom} when is_list(Chunks) ->
            %% TODO: Repair trimmed chunks
            ToRepair = mutation_flus(P) -- [GotItFrom],
            Reply = {ok, {Chunks, []}},
            {Reply, S1} = do_repair_chunks(Chunks, ToRepair, ReturnMode, [GotItFrom],
                                            NSInfo, File, Depth, STime, S, Reply),
            {reply, Reply, S1};
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            read_repair(ConsistencyMode, ReturnMode, NSInfo, File,
                        Offset, Size, Depth, STime, S);
        {error, not_written} ->
            {reply, {error, not_written}, S};
        {error, written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size});
        {error, trimmed} ->
            %% TODO: Again, whole file was trimmed. Needs repair. How
            %% do we repair trimmed file across the flu servers?
            exit({todo_should_repair_unlinked_files, ?MODULE, ?LINE, File})
    end.

do_repair_chunks([], _, _, _, _, _, _, _, S, Reply) ->
    {Reply, S};
do_repair_chunks([{_, Offset, Chunk, CSum}|T],
                 ToRepair, ReturnMode, [GotItFrom], NSInfo, File, Depth, STime, S, Reply) ->
    true = not is_atom(CSum),
    Size = iolist_size(Chunk),
    case do_repair_chunk(ToRepair, ReturnMode, Chunk, CSum, [GotItFrom], NSInfo, File, Offset,
                         Size, Depth, STime, S) of
        {reply, {ok, _}, S1} ->
            do_repair_chunks(T, ToRepair, ReturnMode, [GotItFrom], NSInfo, File, Depth, STime, S1, Reply);
        Error ->
            Error
    end.

do_repair_chunk(ToRepair, ReturnMode, Chunk, CSum, Repaired, NSInfo, File, Offset,
                Size, Depth, STime, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > ?MAX_RUNTIME ->
            {reply, {error, partition}, S};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_repair_chunk(ToRepair, ReturnMode, Chunk, CSum, Repaired, NSInfo, File,
                                    Offset, Size, Depth + 1, STime, S2);
                P2 ->
                    ToRepair2 = mutation_flus(P2) -- Repaired,
                    do_repair_chunk2(ToRepair2, ReturnMode, Chunk, CSum, Repaired, NSInfo, File,
                                     Offset, Size, Depth + 1, STime, S2)
            end
    end.

do_repair_chunk2([], ReturnMode, Chunk, CSum, _Repaired, _NSInfo, File, Offset,
                 _IgnoreSize, _Depth, _STime, S) ->
    %% TODO: add stats for # of repairs, length(_Repaired)-1, etc etc?
    case ReturnMode of
        read ->
            {reply, {ok, {[{File, Offset, Chunk, CSum}], []}}, S};
        {append, Offset, Size, File} ->
            {reply, {ok, {[{Offset, Size, File}], []}}, S}
    end;
do_repair_chunk2([First|Rest]=ToRepair, ReturnMode, Chunk, CSum, Repaired, NSInfo, File, Offset,
                 Size, Depth, STime, #state{epoch_id=EpochID, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(First, PD),
    case ?FLU_PC:write_chunk(Proxy, NSInfo, EpochID, File, Offset, Chunk, CSum, ?TIMEOUT) of
        ok ->
            do_repair_chunk2(Rest, ReturnMode, Chunk, CSum, [First|Repaired], NSInfo, File,
                             Offset, Size, Depth, STime, S);
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_repair_chunk(ToRepair, ReturnMode, Chunk, CSum, Repaired, NSInfo, File,
                            Offset, Size, Depth, STime, S);
        {error, written} ->
            %% TODO: To be very paranoid, read the chunk here to verify
            %% that it is exactly our Chunk.
            do_repair_chunk2(Rest, ReturnMode, Chunk, CSum, Repaired, NSInfo, File,
                             Offset, Size, Depth, STime, S);
        {error, trimmed} = _Error ->
            %% TODO
            exit(todo_should_repair_trimmed);
        {error, not_written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size})
    end.

do_checksum_list(File, 0=Depth, STime, TO, S) ->
    do_checksum_list2(File, Depth + 1, STime, TO, S);
do_checksum_list(File, Depth, STime, TO, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > TO ->
            {reply, {error, partition}, S};
       true ->
            %% This is suboptimal for performance: there are some paths
            %% through this point where our current projection is good
            %% enough.  But we're going to try to keep the code as simple
            %% as we can for now.
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_checksum_list(File, Depth + 1, STime, TO, S2);
                _ ->
                    do_checksum_list2(File, Depth + 1, STime, TO, S2)
            end
    end.

do_checksum_list2(File, Depth, STime, TO,
                  #state{proj=P, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(lists:last(readonly_flus(P)), PD),
    case ?FLU_PC:checksum_list(Proxy, File, ?TIMEOUT) of
        {ok, _}=OK ->
            {reply, OK, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_checksum_list(File, Depth, STime, TO, S);
        {error, _}=Error ->
            {reply, Error, S}
    end.

do_list_files(0=Depth, STime, TO, S) ->
    do_list_files2(Depth + 1, STime, TO, S);
do_list_files(Depth, STime, TO, #state{proj=P}=S) ->
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > TO ->
            {reply, {error, partition}, S};
       true ->
            %% This is suboptimal for performance: there are some paths
            %% through this point where our current projection is good
            %% enough.  But we're going to try to keep the code as simple
            %% as we can for now.
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_list_files(Depth + 1, STime, TO, S2);
                _ ->
                    do_list_files2(Depth + 1, STime, TO, S2)
            end
    end.

do_list_files2(Depth, STime, TO,
                  #state{epoch_id=EpochID, proj=P, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(lists:last(readonly_flus(P)), PD),
    case ?FLU_PC:list_files(Proxy, EpochID, ?TIMEOUT) of
        {ok, _}=OK ->
            {reply, OK, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_list_files(Depth, STime, TO, S);
        {error, _}=Error ->
            {reply, Error, S}
    end.

update_proj(#state{proj=undefined}=S) ->
    update_proj2(1, S);
update_proj(S) ->
    S.

update_proj2(Count, #state{bad_proj=BadProj, proxies_dict=ProxiesDict,
                           opts=Opts}=S) ->
    Timeout = 2*1000,
    WTimeout = 2*Timeout,
    SimName = proplists:get_value(simulator_self_name, Opts, cr_client),
    ExcludedFLUs =
        case proplists:get_value(use_partition_simulator, Opts, false) of
            true ->
                Members = proplists:get_value(simulator_members, Opts, []),
                {Partitions, _Islands} = machi_partition_simulator:get(Members),
                lists:filtermap(fun({A, B}) when A =:= SimName -> {true, B};
                                   ({A, B}) when B =:= SimName -> {true, A};
                                   (_) -> false
                                end, Partitions);
            false -> []
        end,
    Proxies = lists:foldl(fun(Name, Dict) ->
                                  orddict:erase(Name, Dict)
                          end, ProxiesDict, ExcludedFLUs),
    Work = fun({_K, Proxy}) ->
                   ?FLU_PC:read_latest_projection(Proxy, private, Timeout)
           end,
    Rs = run_middleworker_job(Work, Proxies, WTimeout),
    %% TODO: There's a possible bug here when running multiple independent
    %% Machi clusters/chains.  If our chain used to be [a,b], but our
    %% sysadmin has changed our cluster to be [a] and a completely seprate
    %% cluster with [b], and if b is reusing the address & port number,
    %% then it is possible that choose_best_projs() can incorrectly choose
    %% b's projection.
    case choose_best_proj(Rs) of
        P when P == ?WORST_PROJ ->
            io:format(user, "TODO: Using ?WORST_PROJ, chain is not available ~w\n", [self()]),
            sleep_a_while(Count),
            update_proj2(Count + 1, S);
        P when P >= BadProj ->
            #projection_v1{epoch_number=Epoch, epoch_csum=CSum,
                           members_dict=NewMembersDict, dbg2=Dbg2} = P,
            EpochID = {Epoch, CSum},
            ?FLU_PC:stop_proxies(ProxiesDict),
            NewProxiesDict = ?FLU_PC:start_proxies(NewMembersDict),
            %% Make crash reports shorter by getting rid of 'react' history.
            P2 = P#projection_v1{dbg2=lists:keydelete(react, 1, Dbg2)},
            S#state{bad_proj=undefined, proj=P2, epoch_id=EpochID,
                    members_dict=NewMembersDict, proxies_dict=NewProxiesDict};
        _P ->
            sleep_a_while(Count),
            update_proj2(Count + 1, S)
    end.

run_middleworker_job(Fun, ArgList, WTimeout) ->
    Parent = self(),
    MiddleWorker =
        spawn_link(fun() ->
                  PidsMons =
                      [spawn_monitor(fun() ->
                                             Res = (catch Fun(Arg)),
                                             exit(Res)
                                     end) || Arg <- ArgList],
                      Rs = gather_worker_statuses(PidsMons, WTimeout),
                      Parent ! {res, self(), Rs},
                      exit(normal)
              end),
    receive
        {res, MiddleWorker, Results} ->
            Results
    after WTimeout+100 ->
            []
    end.

gather_worker_statuses([], _Timeout) ->
    [];
gather_worker_statuses([{Pid,Ref}|Rest], Timeout) ->
    receive
        {'DOWN', R, process, P, Status} when R == Ref, P == Pid ->
            [Status|gather_worker_statuses(Rest, Timeout)]
    after Timeout ->
            gather_worker_statuses(Rest, 0)
    end.

choose_best_proj(Rs) ->
    lists:foldl(fun({ok, NewProj}, BestProj)
                      when NewProj > BestProj ->
                        NewProj;
                   (_, BestProj) ->
                        BestProj
                end, ?WORST_PROJ, Rs).

try_to_find_chunk(Eligible, NSInfo, File, Offset, Size,
                  #state{epoch_id=EpochID, proxies_dict=PD}) ->
    Timeout = 2*1000,
    Work = fun(FLU) ->
                   Proxy = orddict:fetch(FLU, PD),
                   case ?FLU_PC:read_chunk(Proxy, NSInfo, EpochID,
                                           %% TODO Trimmed is required here
                                           File, Offset, Size, undefined) of
                       {ok, {_Chunks, _} = ChunksAndTrimmed} ->
                           {FLU, {ok, ChunksAndTrimmed}};
                       Else ->
                           {FLU, Else}
                   end
           end,
    Rs = run_middleworker_job(Work, Eligible, Timeout),

    case [X || {_Fluname, {ok, {[{_,_,B,_}], _}}}=X <- Rs, is_binary(B)] of
        [{FoundFLU, {ok, ChunkAndTrimmed}}|_] ->
            {ok, ChunkAndTrimmed, FoundFLU};
        [] ->
            RetryErrs = [partition, bad_epoch, wedged, trimmed],
            %% Adding 'trimmed' to return so as to trigger repair,
            %% once all other retry errors fixed
            case [Err || {error, Err} <- Rs, lists:member(Err, RetryErrs)] of
                [SomeErr|_] ->
                    {error, SomeErr};
                [] ->
                    %% TODO does this really work 100% of the time?
                    {error, not_written}
            end
    end.

mutation_flus(#projection_v1{upi=UPI, repairing=Repairing}) ->
    UPI ++ Repairing;
mutation_flus(#state{proj=P}) ->
    mutation_flus(P).

readonly_flus(#projection_v1{upi=UPI}) ->
    UPI;
readonly_flus(#state{proj=P}) ->
    readonly_flus(P).

is_witness_flu(F, #projection_v1{witnesses=Witness_list}) ->
    lists:member(F, Witness_list).

non_witness_flus(FLUs, P) ->
    lists:dropwhile(fun(F) -> is_witness_flu(F, P) end, FLUs).

witness_flus(FLUs, P) ->
    lists:takewhile(fun(F) -> is_witness_flu(F, P) end, FLUs).

sleep_a_while(0) ->
    ok;
sleep_a_while(1) ->
    ok;
sleep_a_while(Depth) ->
    timer:sleep(30 + trunc(math:pow(1.9, Depth))).

chunk_wrapper_size({_TaggedCSum, Chunk}) ->
    iolist_size(Chunk);
chunk_wrapper_size(Chunk) ->
    iolist_size(Chunk).

%% The intra-gen_server-timeout is approximate.  We'll add some 30
%% seconds of fudge to the user-specified time to accomodate the
%% imprecision.  Using 'infinity' would definitely handle the case of
%% horrible time consumption by the gen_server proc, but I'm slightly
%% leery of really waiting forever: better to have an
%% {'EXIT',{timeout,_}} to raise awareness of a serious problem.

timeout(infinity) ->
    timeout(15*60*1000);                        % close enough to infinity
timeout({_, _}=Timeout) ->
    Timeout;
timeout(Timeout0) ->
    {Timeout0, Timeout0 + 30*1000}.
