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
%%          read_chunk/5, read_chunk/6,
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

%% read_chunk(PidSpec, EpochID, File, Offset, Size) ->
%%     read_chunk(PidSpec, EpochID, File, Offset, Size, infinity).

%% %% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

%% read_chunk(PidSpec, EpochID, File, Offset, Size, Timeout) ->
%%     gen_server:call(PidSpec, {req, {read_chunk, EpochID, File, Offset, Size}},
%%                     Timeout).

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
    {noreply, S}.

terminate(_Reason, #state{proxies_dict=ProxiesDict}=_S) ->
    _ = ?FLU_PC:stop_proxies(ProxiesDict),
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call2({append_chunk_extra, Prefix, Chunk, ChunkExtra}, _From, S) ->
    {reply, sorry_dude, S}.

update_proj(#state{proj=undefined}=S) ->
    update_proj2(1, S);
update_proj(S) ->
    S.

update_proj2(Count, #state{bad_proj=BadProj, proxies_dict=ProxiesDict}=S) ->
    Timeout = 2*1000,
    PidsMons =
        [spawn_monitor(fun() ->
                               exit(catch ?FLU_PC:read_latest_projection(
                                             Proxy, private, Timeout))
                       end) || {_K, Proxy} <- orddict:to_list(ProxiesDict)],
    Rs = gather_worker_statuses(PidsMons, Timeout*2),
    case choose_best_proj(Rs) of
        P when P >= BadProj ->
            io:format(user, "~s: proj ~P\n", [?MODULE, P, 10]),
            EpochID = {P#projection_v1.epoch_number,
                       P#projection_v1.epoch_csum},
            S#state{bad_proj=undefined, proj=EpochID, epoch_id=EpochID};
        _ ->
            timer:sleep(10 + (Count * 20)),
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
    WorstEpoch = #projection_v1{epoch_number=-1},
    lists:foldl(fun({ok, NewEpoch}, BestEpoch)
                      when NewEpoch > BestEpoch ->
                        NewEpoch;
                   (_, BestEpoch) ->
                        BestEpoch
                end, WorstEpoch, Rs).

noop() ->
    ok.
