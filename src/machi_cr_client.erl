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

-module(machi_cr_client).

-behaviour(gen_server).

-include("machi.hrl").
-include("machi_projection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST.

-export([start_link/1]).
%% FLU1 API
-export([
         %% File API
         append_chunk/3, append_chunk/4,
         append_chunk_extra/4, append_chunk_extra/5,
         read_chunk/4, read_chunk/5,
%%          checksum_list/3, checksum_list/4,
%%          list_files/2, list_files/3,
%%          wedge_status/1, wedge_status/2,

%%          %% %% Projection API
%%          get_epoch_id/1, get_epoch_id/2,
%%          get_latest_epoch/2, get_latest_epoch/3,
%%          read_latest_projection/2, read_latest_projection/3,
%%          read_projection/3, read_projection/4,
%%          write_projection/3, write_projection/4,
%%          get_all_projections/2, get_all_projections/3,
%%          list_all_projections/2, list_all_projections/3,

%%          %% Common API
%%          quit/1,

%%          %% Internal API
%%          write_chunk/5, write_chunk/6
         noop/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(FLU_PC, machi_proxy_flu1_client).
-define(TIMEOUT, 2*1000).
-define(MAX_RUNTIME, 8*1000).

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
    append_chunk(PidSpec, Prefix, Chunk, infinity).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(PidSpec, Prefix, Chunk, Timeout) ->
    append_chunk_extra(PidSpec, Prefix, Chunk, 0, Timeout).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk_extra(PidSpec, Prefix, Chunk, ChunkExtra)
  when is_integer(ChunkExtra), ChunkExtra >= 0 ->
    append_chunk_extra(PidSpec, Prefix, Chunk, ChunkExtra, infinity).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk_extra(PidSpec, Prefix, Chunk, ChunkExtra, Timeout) ->
    gen_server:call(PidSpec, {req, {append_chunk_extra, Prefix,
                                    Chunk, ChunkExtra}},
                    Timeout).

%% %% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, File, Offset, Size) ->
    read_chunk(PidSpec, File, Offset, Size, infinity).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, File, Offset, Size, Timeout) ->
    gen_server:call(PidSpec, {req, {read_chunk, File, Offset, Size}},
                    Timeout).

%% %% @doc Fetch the list of chunk checksums for `File'.

%% checksum_list(PidSpec, EpochID, File) ->
%%     checksum_list(PidSpec, EpochID, File, infinity).

%% %% @doc Fetch the list of chunk checksums for `File'.

%% checksum_list(PidSpec, EpochID, File, Timeout) ->
%%     gen_server:call(PidSpec, {req, {checksum_list, EpochID, File}},
%%                     Timeout).

%% %% @doc Fetch the list of all files on the remote FLU.

%% list_files(PidSpec, EpochID) ->
%%     list_files(PidSpec, EpochID, infinity).

%% %% @doc Fetch the list of all files on the remote FLU.

%% list_files(PidSpec, EpochID, Timeout) ->
%%     gen_server:call(PidSpec, {req, {list_files, EpochID}},
%%                     Timeout).

%% %% @doc Fetch the wedge status from the remote FLU.

%% wedge_status(PidSpec) ->
%%     wedge_status(PidSpec, infinity).

%% %% @doc Fetch the wedge status from the remote FLU.

%% wedge_status(PidSpec, Timeout) ->
%%     gen_server:call(PidSpec, {req, {wedge_status}},
%%                     Timeout).

%% %% @doc Get the `epoch_id()' of the FLU's current/latest projection.

%% get_epoch_id(PidSpec) ->
%%     get_epoch_id(PidSpec, infinity).

%% %% @doc Get the `epoch_id()' of the FLU's current/latest projection.

%% get_epoch_id(PidSpec, Timeout) ->
%%     gen_server:call(PidSpec, {req, {get_epoch_id}}, Timeout).

%% %% @doc Get the latest epoch number + checksum from the FLU's projection store.

%% get_latest_epoch(PidSpec, ProjType) ->
%%     get_latest_epoch(PidSpec, ProjType, infinity).

%% %% @doc Get the latest epoch number + checksum from the FLU's projection store.

%% get_latest_epoch(PidSpec, ProjType, Timeout) ->
%%     gen_server:call(PidSpec, {req, {get_latest_epoch, ProjType}},
%%                     Timeout).

%% %% @doc Get the latest projection from the FLU's projection store for `ProjType'

%% read_latest_projection(PidSpec, ProjType) ->
%%     read_latest_projection(PidSpec, ProjType, infinity).

%% %% @doc Get the latest projection from the FLU's projection store for `ProjType'

%% read_latest_projection(PidSpec, ProjType, Timeout) ->
%%     gen_server:call(PidSpec, {req, {read_latest_projection, ProjType}},
%%                     Timeout).

%% %% @doc Read a projection `Proj' of type `ProjType'.

%% read_projection(PidSpec, ProjType, Epoch) ->
%%     read_projection(PidSpec, ProjType, Epoch, infinity).

%% %% @doc Read a projection `Proj' of type `ProjType'.

%% read_projection(PidSpec, ProjType, Epoch, Timeout) ->
%%     gen_server:call(PidSpec, {req, {read_projection, ProjType, Epoch}},
%%                     Timeout).

%% %% @doc Write a projection `Proj' of type `ProjType'.

%% write_projection(PidSpec, ProjType, Proj) ->
%%     write_projection(PidSpec, ProjType, Proj, infinity).

%% %% @doc Write a projection `Proj' of type `ProjType'.

%% write_projection(PidSpec, ProjType, Proj, Timeout) ->
%%     gen_server:call(PidSpec, {req, {write_projection, ProjType, Proj}},
%%                     Timeout).

%% %% @doc Get all projections from the FLU's projection store.

%% get_all_projections(PidSpec, ProjType) ->
%%     get_all_projections(PidSpec, ProjType, infinity).

%% %% @doc Get all projections from the FLU's projection store.

%% get_all_projections(PidSpec, ProjType, Timeout) ->
%%     gen_server:call(PidSpec, {req, {get_all_projections, ProjType}},
%%                     Timeout).

%% %% @doc Get all epoch numbers from the FLU's projection store.

%% list_all_projections(PidSpec, ProjType) ->
%%     list_all_projections(PidSpec, ProjType, infinity).

%% %% @doc Get all epoch numbers from the FLU's projection store.

%% list_all_projections(PidSpec, ProjType, Timeout) ->
%%     gen_server:call(PidSpec, {req, {list_all_projections, ProjType}},
%%                     Timeout).

%% %% @doc Quit &amp; close the connection to remote FLU and stop our
%% %% proxy process.

%% quit(PidSpec) ->
%%     gen_server:call(PidSpec, quit, infinity).

%% %% @doc Write a chunk (binary- or iolist-style) of data to a file
%% %% with `Prefix' at `Offset'.

%% write_chunk(PidSpec, EpochID, File, Offset, Chunk) ->
%%     write_chunk(PidSpec, EpochID, File, Offset, Chunk, infinity).

%% %% @doc Write a chunk (binary- or iolist-style) of data to a file
%% %% with `Prefix' at `Offset'.

%% write_chunk(PidSpec, EpochID, File, Offset, Chunk, Timeout) ->
%%     gen_server:call(PidSpec, {req, {write_chunk, EpochID, File, Offset, Chunk}},
%%                     Timeout).

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
    Reply = ok,
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

handle_call2({append_chunk_extra, Prefix, Chunk, ChunkExtra}, _From, S) ->
    do_append_head(Prefix, Chunk, ChunkExtra, 0, os:timestamp(), S);
handle_call2({read_chunk, File, Offset, Size}, _From, S) ->
    do_read_chunk(File, Offset, Size, 0, os:timestamp(), S).

do_append_head(Prefix, Chunk, ChunkExtra, 0=Depth, STime, S) ->
    do_append_head2(Prefix, Chunk, ChunkExtra, Depth + 1, STime, S);
do_append_head(Prefix, Chunk, ChunkExtra, Depth, STime, #state{proj=P}=S) ->
    %% io:format(user, "head sleep1,", []),
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > ?MAX_RUNTIME ->
            {error, partition};
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
                                   STime, S2);
                _ ->
                    do_append_head2(Prefix, Chunk, ChunkExtra, Depth + 1,
                                    STime, S2)
            end
    end.

do_append_head2(Prefix, Chunk, ChunkExtra, Depth, STime,
                #state{epoch_id=EpochID, proj=P, proxies_dict=PD}=S) ->
    [HeadFLU|RestFLUs] = mutation_flus(P),
    Proxy = orddict:fetch(HeadFLU, PD),
    case ?FLU_PC:append_chunk_extra(Proxy,
                                    EpochID, Prefix, Chunk, ChunkExtra,
                                    ?TIMEOUT) of
        {ok, {Offset, _Size, File}=_X} ->
            %% io:format(user, "append ~w,", [HeadFLU]),
            do_append_midtail(RestFLUs, Prefix, File, Offset, Chunk, ChunkExtra,
                              [HeadFLU], 0, STime, S);
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_append_head(Prefix, Chunk, ChunkExtra, Depth, STime, S);
        {error, written} ->
            %% Implicit sequencing + this error = we don't know where this
            %% written block is.  But we lost a race.  Repeat, with a new
            %% sequencer assignment.
            do_append_head(Prefix, Chunk, ChunkExtra, Depth, STime, S);
        {error, not_written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,
                  Prefix,iolist_size(Chunk)})
    end.

do_append_midtail(RestFLUs, Prefix, File, Offset, Chunk, ChunkExtra,
                  Ws, Depth, STime, S)
  when RestFLUs == [] orelse Depth == 0 ->
       do_append_midtail2(RestFLUs, Prefix, File, Offset, Chunk, ChunkExtra,
                          Ws, Depth + 1, STime, S);
do_append_midtail(_RestFLUs, Prefix, File, Offset, Chunk, ChunkExtra,
                  Ws, Depth, STime, #state{proj=P}=S) ->
    %% io:format(user, "midtail sleep2,", []),
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > ?MAX_RUNTIME ->
            {error, partition};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                undefined ->
                    {error, partition};
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
                            do_append_head2(Prefix, Chunk, ChunkExtra, Depth,
                                            STime, S2);
                        RestFLUs3 ->
                            do_append_midtail2(RestFLUs3, Prefix, File, Offset,
                                               Chunk, ChunkExtra,
                                               Ws, Depth + 1, STime, S2)
                    end
            end
    end.            

do_append_midtail2([], _Prefix, File, Offset, Chunk,
                   _ChunkExtra, _Ws, _Depth, _STime, S) ->
    %% io:format(user, "ok!\n", []),
    {reply, {ok, {Offset, iolist_size(Chunk), File}}, S};
do_append_midtail2([FLU|RestFLUs]=FLUs, Prefix, File, Offset, Chunk,
                   ChunkExtra, Ws, Depth, STime,
                   #state{epoch_id=EpochID, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(FLU, PD),
    case ?FLU_PC:write_chunk(Proxy, EpochID, File, Offset, Chunk, ?TIMEOUT) of
        ok ->
            %% io:format(user, "write ~w,", [FLU]),
            do_append_midtail2(RestFLUs, Prefix, File, Offset, Chunk,
                               ChunkExtra, [FLU|Ws], Depth, STime, S);
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_append_midtail(FLUs, Prefix, File, Offset, Chunk,
                              ChunkExtra, Ws, Depth, STime, S);
        {error, written} ->
            exit({todo,read_repair,?MODULE,?LINE,File,Offset,iolist_size(Chunk)})
        %% TODO return values here
    end.

do_read_chunk(File, Offset, Size, 0=Depth, STime,
              #state{proj=#projection_v1{upi=[_|_]}}=S) -> % UPI is non-empty
    do_read_chunk2(File, Offset, Size, Depth, STime, S);
do_read_chunk(File, Offset, Size, Depth, STime, #state{proj=P}=S) ->
    %% io:format(user, "read sleep1,", []),
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > ?MAX_RUNTIME ->
            {error, partition};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    do_read_chunk(File, Offset, Size, Depth + 1, STime, S2);
                _ ->
                    do_read_chunk2(File, Offset, Size, Depth + 1, STime, S2)
            end
    end.

do_read_chunk2(File, Offset, Size, Depth, STime,
               #state{proj=P, epoch_id=EpochID, proxies_dict=PD}=S) ->
    UPI = readonly_flus(P),
    Head = hd(UPI),
    Tail = lists:last(UPI),
    case ?FLU_PC:read_chunk(orddict:fetch(Tail, PD), EpochID,
                            File, Offset, Size, ?TIMEOUT) of
        {ok, Chunk} when byte_size(Chunk) == Size ->
            {{ok, Chunk}, S};
        {ok, BadChunk} ->
            exit({todo, bad_chunk_size, ?MODULE, ?LINE, File, Offset, Size,
                  got, byte_size(BadChunk)});
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            do_read_chunk(File, Offset, Size, Depth, STime, S);
        {error, not_written} when Tail == Head ->
            {{error, not_written}, S};
        {error, not_written} when Tail /= Head ->
            read_repair(read, File, Offset, Size, Depth, STime, S);
        {error, written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size})
    end.

read_repair(ReturnMode, File, Offset, Size, 0=Depth, STime,
            #state{proj=#projection_v1{upi=[_|_]}}=S) -> % UPI is non-empty
    read_repair2(ReturnMode, File, Offset, Size, Depth, STime, S);
read_repair(ReturnMode, File, Offset, Size, Depth, STime, #state{proj=P}=S) ->
    %% io:format(user, "read_repair sleep1,", []),
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > ?MAX_RUNTIME ->
            {error, partition};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    read_repair(ReturnMode, File, Offset, Size,
                                Depth + 1, STime, S2);
                _ ->
                    read_repair2(ReturnMode, File, Offset, Size,
                                 Depth + 1, STime, S2)
            end
    end.

read_repair2(ReturnMode, File, Offset, Size, Depth, STime,
             #state{proj=P, epoch_id=EpochID, proxies_dict=PD}=S) ->
    [Head|MidsTails] = readonly_flus(P),
    case ?FLU_PC:read_chunk(orddict:fetch(Head, PD), EpochID,
                            File, Offset, Size, ?TIMEOUT) of
        {ok, Chunk} when byte_size(Chunk) == Size ->
            read_repair3(MidsTails, ReturnMode, Chunk, [Head], File, Offset,
                         Size, Depth, STime, S);
        {ok, BadChunk} ->
            exit({todo, bad_chunk_size, ?MODULE, ?LINE, File, Offset, Size,
                  got, byte_size(BadChunk)});
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            read_repair(ReturnMode, File, Offset, Size, Depth, STime, S);
        {error, not_written} ->
            {{error, not_written}, S};
        {error, written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size})
    end.

read_repair3([], ReturnMode, Chunk, Repaired, File, Offset,
             Size, Depth, STime, S) ->
    read_repair4([], ReturnMode, Chunk, Repaired, File, Offset,
                 Size, Depth, STime, S);
read_repair3(MidsTails, ReturnMode, Chunk, Repaired, File, Offset,
             Size, 0=Depth, STime, S) ->
    read_repair4(MidsTails, ReturnMode, Chunk, Repaired, File, Offset,
                  Size, Depth, STime, S);
read_repair3(MidsTails, ReturnMode, Chunk, File, Repaired, Offset,
             Size, Depth, STime, #state{proj=P}=S) ->
    %% io:format(user, "read_repair3 sleep1,", []),
    sleep_a_while(Depth),
    DiffMs = timer:now_diff(os:timestamp(), STime) div 1000,
    if DiffMs > ?MAX_RUNTIME ->
            {error, partition};
       true ->
            S2 = update_proj(S#state{proj=undefined, bad_proj=P}),
            case S2#state.proj of
                P2 when P2 == undefined orelse
                        P2#projection_v1.upi == [] ->
                    read_repair3(MidsTails, ReturnMode, Chunk, Repaired, File,
                                 Offset, Size, Depth + 1, STime, S2);
                P2 ->
                    MidsTails2 = P2#projection_v1.upi -- Repaired,
                    read_repair4(MidsTails2, ReturnMode, Chunk, Repaired, File,
                                 Offset, Size, Depth + 1, STime, S2)
            end
    end.

read_repair4([], ReturnMode, Chunk, Repaired, File, Offset,
             Size, Depth, STime, S) ->
    case ReturnMode of
        read ->
            {reply, {ok, Chunk}, S}
    end;
read_repair4([First|Rest]=MidsTails, ReturnMode, Chunk, Repaired, File, Offset,
             Size, Depth, STime, #state{epoch_id=EpochID, proxies_dict=PD}=S) ->
    Proxy = orddict:fetch(First, PD),
    case ?FLU_PC:write_chunk(Proxy, EpochID, File, Offset, Chunk, ?TIMEOUT) of
        ok ->
            read_repair4(Rest, ReturnMode, Chunk, [First|Repaired], File,
                         Offset, Size, Depth, STime, S);
        {error, Retry}
          when Retry == partition; Retry == bad_epoch; Retry == wedged ->
            read_repair3(MidsTails, ReturnMode, Chunk, Repaired, File,
                         Offset, Size, Depth, STime, S);
        {error, written} ->
            %% TODO: To be very paranoid, read the chunk here to verify
            %% that it is exactly our Chunk.
            read_repair4(Rest, ReturnMode, Chunk, Repaired, File,
                         Offset, Size, Depth, STime, S);
        {error, not_written} ->
            exit({todo_should_never_happen,?MODULE,?LINE,File,Offset,Size})
    end.

update_proj(#state{proj=undefined}=S) ->
    update_proj2(1, S);
update_proj(S) ->
    S.

update_proj2(Count, #state{bad_proj=BadProj, proxies_dict=ProxiesDict}=S) ->
    Timeout = 2*1000,
    Parent = self(),
    Proxies = orddict:to_list(ProxiesDict),
    MiddleWorker = spawn(
               fun() ->
                  PidsMons =
                      [spawn_monitor(fun() ->
                                      exit(catch ?FLU_PC:read_latest_projection(
                                                       Proxy, private, Timeout))
                                      end) || {_K, Proxy} <- Proxies],
                  Rs = gather_worker_statuses(PidsMons, Timeout*2),
                  Parent ! {res, self(), Rs},
                  exit(normal)
               end),
    Rs = receive {res, MiddleWorker, Results} -> Results
         after Timeout*2 -> []
         end,
    %% TODO: There's a possible bug here when running multiple independent
    %% Machi clusters/chains.  If our chain used to be [a,b], but our
    %% sysadmin has changed our cluster to be [a] and a completely seprate
    %% cluster with [b], and if b is reusing the address & port number,
    %% then it is possible that choose_best_projs() can incorrectly choose
    %% b's projection.
    case choose_best_proj(Rs) of
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
    WorstEpoch = #projection_v1{epoch_number=-1,epoch_csum= <<>>},
    lists:foldl(fun({ok, NewEpoch}, BestEpoch)
                      when NewEpoch > BestEpoch ->
                        NewEpoch;
                   (_, BestEpoch) ->
                        BestEpoch
                end, WorstEpoch, Rs).

mutation_flus(#projection_v1{upi=UPI, repairing=Repairing}) ->
    UPI ++ Repairing;
mutation_flus(#state{proj=P}) ->
    mutation_flus(P).

readonly_flus(#projection_v1{upi=UPI}) ->
    UPI;
readonly_flus(#state{proj=P}) ->
    readonly_flus(P).

sleep_a_while(0) ->
    ok;
sleep_a_while(1) ->
    ok;
sleep_a_while(Depth) ->
    timer:sleep(30 + trunc(math:pow(1.9, Depth))).

noop() ->
    ok.
