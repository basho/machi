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
%% See also the docs for {@link machi_flu1_client} for additional
%% details on data types and operation descriptions.
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
%%
%%
%% === Missing API features ===
%%
%% So far, there is one missing client API feature that ought to be
%% added to Machi in the near future: more flexible checksum
%% management.
%%
%% Add a `source' annotation to all checksums to indicate where the
%% checksum was calculated.  For example,
%%
%% <ul>
%%
%% <li> Calculated by client that performed the original chunk append,
%% </li>
%%
%% <li> Calculated by the 1st Machi server to receive an
%%      un-checksummed append request
%% </li>
%%
%% <li> Re-calculated by Machi to manage fewer checksums of blocks of
%%      data larger than the original client-specified chunks.
%% </li>
%% </ul>
%%
%% Client-side checksums would be the "strongest" type of
%% checksum, meaning that any data corruption (of the original
%% data and/or of the checksum itself) can be detected after the
%% client-side calculation.  There are too many horror stories on
%% The Net about IP PDUs that are corrupted but unnoticed due to
%% weak TCP checksums, buggy hardware, buggy OS drivers, etc.
%% Checksum versioning is also desirable if/when the current checksum
%% implementation changes from SHA-1 to something else.
%%
%%
%% === Implementation notes ===
%%
%% The major operation processing is implemented in a state machine-like
%% manner.  Before attempting an operation `X', there's an initial
%% operation `pre-X' that takes care of updating the epoch id,
%% restarting client protocol proxies, and if there's any server
%% instability (e.g. some server is wedged), then insert some sleep
%% time.  When the chain appears to have stabilized, then we try the `X'
%% operation again.
%%
%% Function name for the `pre-X' stuff is usually `X()', and the
%% function name for the `X' stuff is usually `X2()'.  (I.e., the `X'
%% stuff follows after `pre-X' and therefore has a `2' suffix on the
%% function name.)
%%
%% In the case of read repair, there are two stages: find the value to
%% perform the repair, then perform the repair writes.  In the case of
%% the repair writes, the `pre-X' function is named `read_repair3()',
%% and the `X' function is named `read_repair4()'.
%%
%% TODO: It would be nifty to lift the very-nearly-but-not-quite-boilerplate
%% of the `pre-X' functions into a single common function ... but I'm not
%% sure yet on how to do it without making the code uglier.

-module(machi_cr_client).

-behaviour(gen_server).

-include("machi.hrl").
-include("machi_projection.hrl").
-include("machi_verbose.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([trim_both_side/3]).
-endif. % TEST.

-export([start_link/1]).
%% FLU1 API
-export([
         %% File API
         append_chunk/3, append_chunk/4,
         append_chunk_extra/4, append_chunk_extra/5,
         write_chunk/4, write_chunk/5,
         read_chunk/4, read_chunk/5,
         trim_chunk/4, trim_chunk/5,
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
          bad_proj
         }).

%% @doc Start a local, long-lived process that will be our steady
%% &amp; reliable communication proxy with the fickle &amp; flaky
%% remote Machi server.

start_link(P_srvr_list) ->
    gen_server:start_link(?MODULE, [P_srvr_list], []).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(PidSpec, Prefix, Chunk) ->
    append_chunk(PidSpec, Prefix, Chunk, ?DEFAULT_TIMEOUT).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(PidSpec, Prefix, Chunk, Timeout) ->
    append_chunk_extra(PidSpec, Prefix, Chunk, 0, Timeout).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk_extra(PidSpec, Prefix, Chunk, ChunkExtra)
  when is_integer(ChunkExtra), ChunkExtra >= 0 ->
    append_chunk_extra(PidSpec, Prefix, Chunk, ChunkExtra, ?DEFAULT_TIMEOUT).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk_extra(PidSpec, Prefix, Chunk, ChunkExtra, Timeout0) ->
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {append_chunk_extra, Prefix,
                                    Chunk, ChunkExtra, TO}},
                    Timeout).

%% @doc Write a chunk of data (that has already been
%% allocated/sequenced by an earlier append_chunk_extra() call) to
%% `File' at `Offset'.

write_chunk(PidSpec, File, Offset, Chunk) ->
    write_chunk(PidSpec, File, Offset, Chunk, ?DEFAULT_TIMEOUT).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

write_chunk(PidSpec, File, Offset, Chunk, Timeout0) ->
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {write_chunk, File, Offset, Chunk, TO}},
                    Timeout).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, File, Offset, Size) ->
    read_chunk(PidSpec, File, Offset, Size, ?DEFAULT_TIMEOUT).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, File, Offset, Size, Timeout0) ->
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {read_chunk, File, Offset, Size, TO}},
                    Timeout).

%% @doc Trim a chunk of data of size `Size' from `File' at `Offset'.

trim_chunk(PidSpec, File, Offset, Size) ->
    trim_chunk(PidSpec, File, Offset, Size, ?DEFAULT_TIMEOUT).

%% @doc Trim a chunk of data of size `Size' from `File' at `Offset'.

trim_chunk(PidSpec, File, Offset, Size, Timeout0) ->
    {TO, Timeout} = timeout(Timeout0),
    gen_server:call(PidSpec, {req, {trim_chunk, File, Offset, Size, TO}},
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

init([P_srvr_list]) ->
    MembersDict = orddict:from_list([{P#p_srvr.name, P} || P <- P_srvr_list]),
    ProxiesDict = ?FLU_PC:start_proxies(MembersDict),
    {ok, #state{members_dict=MembersDict, proxies_dict=ProxiesDict}}.

handle_call({req, Req}, From, S) ->
    handle_call2(Req, From, update_proj(S));
handle_call(quit, _From, S) ->
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

handle_call2({append_chunk_extra, Prefix, Chunk, ChunkExtra, TO}, _From, S) ->
    do_append_head(Prefix, Chunk, ChunkExtra, 0, os:timestamp(), TO, S);
handle_call2({write_chunk, File, Offset, Chunk, TO}, _From, S) ->
    do_write_head(File, Offset, Chunk, 0, os:timestamp(), TO, S);
handle_call2({read_chunk, File, Offset, Size, TO}, _From, S) ->
    do_read_chunk(File, Offset, Size, 0, os:timestamp(), TO, S);
handle_call2({trim_chunk, File, Offset, Size, TO}, _From, S) ->
    do_trim_chunk(File, Offset, Size, 0, os:timestamp(), TO, S);
handle_call2({checksum_list, File, TO}, _From, S) ->
    do_checksum_list(File, 0, os:timestamp(), TO, S);
handle_call2({list_files, TO}, _From, S) ->
    do_list_files(0, os:timestamp(), TO, S).

do_append_head(Prefix, Chunk, ChunkExtra, 0=Depth, STime, TO, S) ->
    do_append_head2(Prefix, Chunk, ChunkExtra, Depth + 1, STime, TO, S);
do_append_head(Prefix, Chunk, ChunkExtra, Depth, STime, TO, #state{proj=P}=S) ->
    %% io:format(user, "head sleep1,", []),
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
                    do_append_head(Prefix, Chunk, ChunkExtra, Depth + 1,
                                   STime, TO, S2);
                _ ->
                    do_append_head2(Prefix, Chunk, ChunkExtra, Depth + 1,
                                    STime, TO, S2)
            end
    end.

do_append_head2(Prefix, Chunk, ChunkExtra, Depth, STime, TO,
                #state{proj=P}=S) ->
    [HeadFLU|_RestFLUs] = mutation_flus(P),
    case is_witness_flu(HeadFLU, P) of
        true ->
            case witnesses_use_our_epoch(S) of
                true ->
                    do_append_head3(Prefix, Chunk, ChunkExtra, Depth,
                                    STime, TO, S);
                false ->
                    %% Bummer, go back to the beginning and retry.
                    do_append_head(Prefix, Chunk, ChunkExtra, Depth,
                                   STime, TO, S)
            end;
        false ->
            do_append_head3(Prefix, Chunk, ChunkExtra, Depth, STime, TO, S)
    end.

do_append_head3(Prefix, Chunk, ChunkExtra, Depth, STime, TO,
                #state{epoch_id=EpochID, proj=P, proxies_dict=PD}=S) ->
    [HeadFLU|RestFLUs] = non_witness_flus(mutation_flus(P), P),
    Proxy = orddict:fetch(HeadFLU, PD),
    case ?FLU_PC:append_chunk_extra(Proxy,
                                    EpochID, Prefix, Chunk, ChunkExtra,
                                    ?TIMEOUT) of
        {ok, {Offset, _Size, File}=_X} ->
            %% io:format(user, "append ~w,", [HeadFLU]),
            do_append_midtail(RestFLUs, Prefix, File, Offset, Chunk, ChunkExtra,
                              [HeadFLU], 0, STime, TO, S);
        {error, bad_checksum}=BadCS ->
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_append_head(Prefix, Chunk, ChunkExtra, Depth, STime, TO, S);
        {error, written} ->
            %% Implicit sequencing + this error = we don't know where this
            %% written block is.  But we lost a race.  Repeat, with a new
            %% sequencer assignment.
            do_append_head(Prefix, Chunk, ChunkExtra, Depth, STime, TO, S);
        {error, not_written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,
                  Prefix,iolist_size(Chunk)})
    end.

do_append_midtail(RestFLUs, Prefix, File, Offset, Chunk, ChunkExtra,
                  Ws, Depth, STime, TO, S)
  when RestFLUs == [] orelse Depth == 0 ->
       do_append_midtail2(RestFLUs, Prefix, File, Offset, Chunk, ChunkExtra,
                          Ws, Depth + 1, STime, TO, S);
do_append_midtail(_RestFLUs, Prefix, File, Offset, Chunk, ChunkExtra,
                  Ws, Depth, STime, TO, #state{proj=P}=S) ->
    %% io:format(user, "midtail sleep2,", []),
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
                                    do_append_head2(Prefix, Chunk, ChunkExtra,
                                                    Depth, STime, TO, S2)
                            end;
                        RestFLUs3 ->
                            do_append_midtail2(RestFLUs3, Prefix, File, Offset,
                                               Chunk, ChunkExtra,
                                               Ws, Depth + 1, STime, TO, S2)
                    end
            end
    end.            

do_append_midtail2([], _Prefix, File, Offset, Chunk,
                   _ChunkExtra, _Ws, _Depth, _STime, _TO, S) ->
    %% io:format(user, "ok!\n", []),
    {reply, {ok, {Offset, chunk_wrapper_size(Chunk), File}}, S};
do_append_midtail2([FLU|RestFLUs]=FLUs, Prefix, File, Offset, Chunk,
                   ChunkExtra, Ws, Depth, STime, TO,
                   #state{epoch_id=EpochID, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(FLU, PD),
    case ?FLU_PC:write_chunk(Proxy, EpochID, File, Offset, Chunk, ?TIMEOUT) of
        ok ->
            %% io:format(user, "write ~w,", [FLU]),
            do_append_midtail2(RestFLUs, Prefix, File, Offset, Chunk,
                               ChunkExtra, [FLU|Ws], Depth, STime, TO, S);
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_append_midtail(FLUs, Prefix, File, Offset, Chunk,
                              ChunkExtra, Ws, Depth, STime, TO, S);
        {error, written} ->
            %% We know what the chunk ought to be, so jump to the
            %% middle of read-repair.
            Resume = {append, Offset, iolist_size(Chunk), File},
            do_repair_chunk(FLUs, Resume, Chunk, [], File, Offset,
                            iolist_size(Chunk), Depth, STime, S);
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
        {ok, {false, EID}} when EID == EpochID ->
            witnesses_use_our_epoch(RestFLUs, S);
        _Else ->
            false
    end.

do_write_head(File, Offset, Chunk, 0=Depth, STime, TO, S) ->
    do_write_head2(File, Offset, Chunk, Depth + 1, STime, TO, S);
do_write_head(File, Offset, Chunk, Depth, STime, TO, #state{proj=P}=S) ->
    %% io:format(user, "head sleep1,", []),
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
                    do_write_head(File, Offset, Chunk, Depth + 1,
                                  STime, TO, S2);
                _ ->
                    do_write_head2(File, Offset, Chunk, Depth + 1,
                                   STime, TO, S2)
            end
    end.

do_write_head2(File, Offset, Chunk, Depth, STime, TO,
               #state{epoch_id=EpochID, proj=P, proxies_dict=PD}=S) ->
    [HeadFLU|RestFLUs] = mutation_flus(P),
    Proxy = orddict:fetch(HeadFLU, PD),
    case ?FLU_PC:write_chunk(Proxy, EpochID, File, Offset, Chunk, ?TIMEOUT) of
        ok ->
            %% From this point onward, we use the same code & logic path as
            %% append does.
            do_append_midtail(RestFLUs, undefined, File, Offset, Chunk,
                              undefined, [HeadFLU], 0, STime, TO, S);
        {error, bad_checksum}=BadCS ->
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_write_head(File, Offset, Chunk, Depth, STime, TO, S);
        {error, written}=Err ->
            {reply, Err, S};
        {error, not_written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,
                  iolist_size(Chunk)})
    end.

do_read_chunk(File, Offset, Size, 0=Depth, STime, TO,
              #state{proj=#projection_v1{upi=[_|_]}}=S) -> % UPI is non-empty
    do_read_chunk2(File, Offset, Size, Depth + 1, STime, TO, S);
do_read_chunk(File, Offset, Size, Depth, STime, TO, #state{proj=P}=S) ->
    %% io:format(user, "read sleep1,", []),
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > TO ->
            {reply, {error, partition}, S};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_read_chunk(File, Offset, Size, Depth + 1, STime, TO, S2);
                _ ->
                    do_read_chunk2(File, Offset, Size, Depth + 1, STime, TO, S2)
            end
    end.

do_read_chunk2(File, Offset, Size, Depth, STime, TO,
               #state{proj=P, epoch_id=EpochID, proxies_dict=PD}=S) ->
    UPI = readonly_flus(P),
    Tail = lists:last(UPI),
    ConsistencyMode = P#projection_v1.mode,
    case ?FLU_PC:read_chunk(orddict:fetch(Tail, PD), EpochID,
                            File, Offset, Size, ?TIMEOUT) of
        {ok, Chunks0} when is_list(Chunks0) ->
            Chunks = trim_both_side(Chunks0, Offset, Size),
            {reply, {ok, Chunks}, S};
        %% {ok, BadChunk} ->
        %%     %% TODO cleaner handling of bad chunks
        %%     exit({todo, bad_chunk_size, ?MODULE, ?LINE, File, Offset, Size,
        %%           got, byte_size(BadChunk)});
        {error, bad_arg} = BadArg -> 
            {reply, BadArg, S};
        {error, partial_read}=Err ->
            {reply, Err, S};
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_read_chunk(File, Offset, Size, Depth, STime, TO, S);
        {error, not_written} ->
            read_repair(ConsistencyMode, read, File, Offset, Size, Depth, STime, S);
            %% {reply, {error, not_written}, S};
        {error, written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size})
    end.

do_trim_chunk(_File, _Offset, _Size, _Depth, _STime, _TO, S) ->
    %% This is just a stub to reach CR client from high level client
    {reply, {error, bad_joss}, S}.

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
%% read_repair(ConsistencyMode, ReturnMode, File, Offset, Size, 0=Depth,
%%           STime, #state{proj=#projection_v1{upi=[_|_]}}=S) -> % UPI is non-empty
%%     read_repair2(ConsistencyMode, ReturnMode, File, Offset, Size, Depth + 1,
%%                  STime, S);
read_repair(ConsistencyMode, ReturnMode, File, Offset, Size, Depth,
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
                    read_repair(ConsistencyMode, ReturnMode, File, Offset,
                                Size, Depth + 1, STime, S2);
                _ ->
                    read_repair2(ConsistencyMode, ReturnMode, File, Offset,
                                 Size, Depth + 1, STime, S2)
            end
    end.

read_repair2(cp_mode=ConsistencyMode,
             ReturnMode, File, Offset, Size, Depth, STime,
             #state{proj=P, epoch_id=EpochID, proxies_dict=PD}=S) ->
    %% TODO WTF was I thinking here??....
    Tail = lists:last(readonly_flus(P)),
    case ?FLU_PC:read_chunk(orddict:fetch(Tail, PD), EpochID,
                            File, Offset, Size, ?TIMEOUT) of
        {ok, Chunks} when is_list(Chunks) ->
            ToRepair = mutation_flus(P) -- [Tail],
            {Reply, S1} = do_repair_chunks(Chunks, ToRepair, ReturnMode,
                                           [Tail], File, Depth, STime, S, {ok, Chunks}),
            {reply, Reply, S1};
        %% {ok, BadChunk} ->
        %%     exit({todo, bad_chunk_size, ?MODULE, ?LINE, File, Offset,
        %%           Size, got, byte_size(BadChunk)});
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            read_repair(ConsistencyMode, ReturnMode, File, Offset,
                        Size, Depth, STime, S);
        {error, not_written} ->
            {reply, {error, not_written}, S};
        {error, written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size})
    end;
read_repair2(ap_mode=ConsistencyMode,
             ReturnMode, File, Offset, Size, Depth, STime,
             #state{proj=P}=S) ->
    Eligible = mutation_flus(P),
    case try_to_find_chunk(Eligible, File, Offset, Size, S) of
        {ok, Chunks, GotItFrom} when is_list(Chunks) ->
            ToRepair = mutation_flus(P) -- [GotItFrom],
            {Reply, S1} = do_repair_chunks(Chunks, ToRepair, ReturnMode, [GotItFrom],
                                           File, Depth, STime, S, {ok, Chunks}),
            {reply, Reply, S1};
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {reply, BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            read_repair(ConsistencyMode, ReturnMode, File,
                        Offset, Size, Depth, STime, S);
        {error, not_written} ->
            {reply, {error, not_written}, S};
        {error, written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size})
    end.

do_repair_chunks([], _, _, _, _, _, _, S, Reply) ->
    {Reply, S};
do_repair_chunks([{_, Offset, Chunk, _Csum}|T],
                 ToRepair, ReturnMode, [GotItFrom], File, Depth, STime, S, Reply) ->
    Size = iolist_size(Chunk),
    case do_repair_chunk(ToRepair, ReturnMode, Chunk, [GotItFrom], File, Offset,
                         Size, Depth, STime, S) of
        {ok, Chunk, S1} ->
            do_repair_chunks(T, ToRepair, ReturnMode, [GotItFrom], File, Depth, STime, S1, Reply);
        Error ->
            Error
    end.

do_repair_chunk(ToRepair, ReturnMode, Chunk, Repaired, File, Offset,
                Size, Depth, STime, #state{proj=P}=S) ->
    %% io:format(user, "read_repair3 sleep1,", []),
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > ?MAX_RUNTIME ->
            {reply, {error, partition}, S};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_repair_chunk(ToRepair, ReturnMode, Chunk, Repaired, File,
                                    Offset, Size, Depth + 1, STime, S2);
                P2 ->
                    ToRepair2 = mutation_flus(P2) -- Repaired,
                    do_repair_chunk2(ToRepair2, ReturnMode, Chunk, Repaired, File,
                                     Offset, Size, Depth + 1, STime, S2)
            end
    end.

do_repair_chunk2([], ReturnMode, Chunk, _Repaired, File, Offset,
                 _IgnoreSize, _Depth, _STime, S) ->
    %% TODO: add stats for # of repairs, length(_Repaired)-1, etc etc?
    case ReturnMode of
        read ->
            {ok, Chunk, S};
        {append, Offset, Size, File} ->
            {ok, {Offset, Size, File}, S}
    end;
do_repair_chunk2([First|Rest]=ToRepair, ReturnMode, Chunk, Repaired, File, Offset,
                 Size, Depth, STime, #state{epoch_id=EpochID, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(First, PD),
    case ?FLU_PC:write_chunk(Proxy, EpochID, File, Offset, Chunk, ?TIMEOUT) of
        ok ->
            do_repair_chunk2(Rest, ReturnMode, Chunk, [First|Repaired], File,
                             Offset, Size, Depth, STime, S);
        {error, bad_checksum}=BadCS ->
            %% TODO: alternate strategy?
            {BadCS, S};
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_repair_chunk(ToRepair, ReturnMode, Chunk, Repaired, File,
                            Offset, Size, Depth, STime, S);
        {error, written} ->
            %% TODO: To be very paranoid, read the chunk here to verify
            %% that it is exactly our Chunk.
            do_repair_chunk2(Rest, ReturnMode, Chunk, Repaired, File,
                             Offset, Size, Depth, STime, S);
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
                  #state{epoch_id=EpochID, proj=P, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(lists:last(readonly_flus(P)), PD),
    case ?FLU_PC:checksum_list(Proxy, EpochID, File, ?TIMEOUT) of
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

update_proj2(Count, #state{bad_proj=BadProj, proxies_dict=ProxiesDict}=S) ->
    Timeout = 2*1000,
    WTimeout = 2*Timeout,
    Proxies = orddict:to_list(ProxiesDict),
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
            io:format(user, "TODO: Using ?WORST_PROJ, chain is not available\n", []),
            sleep_a_while(Count),
            update_proj2(Count + 1, S);
        P when P >= BadProj ->
            #projection_v1{epoch_number=Epoch, epoch_csum=CSum,
                           members_dict=NewMembersDict} = P,
            EpochID = {Epoch, CSum},
            ?FLU_PC:stop_proxies(ProxiesDict),
            NewProxiesDict = ?FLU_PC:start_proxies(NewMembersDict),
            S#state{bad_proj=undefined, proj=P, epoch_id=EpochID,
                    members_dict=NewMembersDict, proxies_dict=NewProxiesDict};
        _ ->
            sleep_a_while(Count),
            update_proj2(Count + 1, S)
    end.

run_middleworker_job(Fun, ArgList, WTimeout) ->
    Parent = self(),
    MiddleWorker =
        spawn(fun() ->
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

try_to_find_chunk(Eligible, File, Offset, Size,
                  #state{epoch_id=EpochID, proxies_dict=PD}) ->
    Timeout = 2*1000,
    Work = fun(FLU) ->
                   Proxy = orddict:fetch(FLU, PD),
                   case ?FLU_PC:read_chunk(Proxy, EpochID,
                                           File, Offset, Size) of
                       {ok, Chunks} when is_list(Chunks) ->
                           {FLU, {ok, Chunks}};
                       Else ->
                           {FLU, Else}
                   end
           end,
    Rs = run_middleworker_job(Work, Eligible, Timeout),
    case [X || {_, {ok, [{_,_,B,_}]}}=X <- Rs, is_binary(B)] of
        [{FoundFLU, {ok, Chunk}}|_] ->
            {ok, Chunk, FoundFLU};
        [] ->
            RetryErrs = [partition, bad_epoch, wedged],
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
timeout(Timeout0) ->
    {Timeout0, Timeout0 + 30*1000}.

trim_both_side([], _Offset, _Size) -> [];
trim_both_side([{F, Offset0, Chunk, _Csum}|L], Offset, Size)
  when Offset0 < Offset ->
    TrashLen = 8 * (Offset - Offset0),
    <<_:TrashLen/binary, NewChunk/binary>> = Chunk,
    NewH = {F, Offset, NewChunk, <<>>},
    trim_both_side([NewH|L], Offset, Size);
trim_both_side(Chunks, Offset, Size) ->
    %% TODO: optimize
    [{F, Offset1, Chunk1, _Csum1}|L] = lists:reverse(Chunks),
    Size1 = iolist_size(Chunk1),
    if Offset + Size < Offset1 + Size1 ->
            Size2 = Offset + Size - Offset1,
            <<NewChunk1:Size2/binary, _/binary>> = Chunk1,
            lists:reverse([{F, Offset1, NewChunk1, <<>>}|L]);
       true ->
            Chunks
    end.
