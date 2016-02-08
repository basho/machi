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

%% @doc Erlang API for the Machi FLU TCP protocol version 1, with a
%% proxy-process style API for hiding messy details such as TCP
%% connection/disconnection with the remote Machi server.
%%
%% Please see {@link machi_flu1_client} the "Client API implemntation notes"
%% section for how this module relates to the rest of the client API
%% implementation.
%%
%% Machi is intentionally avoiding using distributed Erlang for
%% Machi's communication.  This design decision makes Erlang-side code
%% more difficult &amp; complex, but it's the price to pay for some
%% language independence.  Later in Machi's life cycle, we may (?) need to
%% (re-)implement some components in a non-Erlang/BEAM-based language.
%%
%% This module implements a "man in the middle" proxy between the
%% Erlang client and Machi server (which is on the "far side" of a TCP
%% connection to somewhere).  This proxy process will always execute
%% on the same Erlang node as the Erlang client that uses it.  The
%% proxy is intended to be a stable, long-lived process that survives
%% TCP communication problems with the remote server.
%%
%% For a higher level interface, see {@link machi_cr_client}.
%% For a lower level interface, see {@link machi_flu1_client}.

-module(machi_proxy_flu1_client).

-behaviour(gen_server).

-include("machi.hrl").
-include("machi_projection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-include_lib("pulse_otp/include/pulse_otp.hrl").
-endif.
-endif. % TEST.

-export([start_link/1]).
%% FLU1 API
-export([
         %% File API
         append_chunk/6, append_chunk/8,
         read_chunk/7, read_chunk/8,
         checksum_list/2, checksum_list/3,
         list_files/2, list_files/3,
         wedge_status/1, wedge_status/2,

         %% %% Projection API
         get_epoch_id/1, get_epoch_id/2,
         get_latest_epochid/2, get_latest_epochid/3,
         read_latest_projection/2, read_latest_projection/3,
         read_projection/3, read_projection/4,
         write_projection/3, write_projection/4,
         get_all_projections/2, get_all_projections/3,
         list_all_projections/2, list_all_projections/3,
         kick_projection_reaction/2, kick_projection_reaction/3,

         %% Common API
         quit/1,

         %% Internal API
         write_chunk/7, write_chunk/8,
         trim_chunk/6, trim_chunk/7,

         %% Helpers
         stop_proxies/1, start_proxies/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          i    :: #p_srvr{},
          sock :: 'undefined' | port()
         }).

%% @doc Start a local, long-lived process that will be our steady
%% &amp; reliable communication proxy with the fickle &amp; flaky
%% remote Machi server.

start_link(#p_srvr{}=I) ->
    gen_server:start_link(?MODULE, [I], []).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(PidSpec, NSInfo, EpochID, Prefix, Chunk, CSum) ->
    append_chunk(PidSpec, NSInfo, EpochID, Prefix, Chunk, CSum,
                 #append_opts{}, infinity).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(PidSpec, NSInfo, EpochID, Prefix, Chunk, CSum, Opts,
             Timeout) ->
    gen_server:call(PidSpec, {req, {append_chunk, NSInfo, EpochID,
                                    Prefix, Chunk, CSum, Opts, Timeout}},
                    Timeout).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, NSInfo, EpochID, File, Offset, Size, Opts) ->
    read_chunk(PidSpec, NSInfo, EpochID, File, Offset, Size, Opts, infinity).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, NSInfo, EpochID, File, Offset, Size, Opts, Timeout) ->
    gen_server:call(PidSpec, {req, {read_chunk, NSInfo, EpochID, File, Offset, Size, Opts}},
                    Timeout).

%% @doc Fetch the list of chunk checksums for `File'.

checksum_list(PidSpec, File) ->
    checksum_list(PidSpec, File, infinity).

%% @doc Fetch the list of chunk checksums for `File'.

checksum_list(PidSpec, File, Timeout) ->
    gen_server:call(PidSpec, {req, {checksum_list, File}},
                    Timeout).

%% @doc Fetch the list of all files on the remote FLU.

list_files(PidSpec, EpochID) ->
    list_files(PidSpec, EpochID, infinity).

%% @doc Fetch the list of all files on the remote FLU.

list_files(PidSpec, EpochID, Timeout) ->
    gen_server:call(PidSpec, {req, {list_files, EpochID}},
                    Timeout).

%% @doc Fetch the wedge status from the remote FLU.

wedge_status(PidSpec) ->
    wedge_status(PidSpec, infinity).

%% @doc Fetch the wedge status from the remote FLU.

wedge_status(PidSpec, Timeout) ->
    gen_server:call(PidSpec, {req, {wedge_status}},
                    Timeout).

%% @doc Get the `epoch_id()' of the FLU's current/latest projection.

get_epoch_id(PidSpec) ->
    get_epoch_id(PidSpec, infinity).

%% @doc Get the `epoch_id()' of the FLU's current/latest projection.

get_epoch_id(PidSpec, Timeout) ->
    gen_server:call(PidSpec, {req, {get_epoch_id}}, Timeout).

%% @doc Get the latest epoch number + checksum from the FLU's projection store.

get_latest_epochid(PidSpec, ProjType) ->
    get_latest_epochid(PidSpec, ProjType, infinity).

%% @doc Get the latest epoch number + checksum from the FLU's projection store.

get_latest_epochid(PidSpec, ProjType, Timeout) ->
    gen_server:call(PidSpec, {req, {get_latest_epochid, ProjType}},
                    Timeout).

%% @doc Get the latest projection from the FLU's projection store for `ProjType'

read_latest_projection(PidSpec, ProjType) ->
    read_latest_projection(PidSpec, ProjType, infinity).

%% @doc Get the latest projection from the FLU's projection store for `ProjType'

read_latest_projection(PidSpec, ProjType, Timeout) ->
    gen_server:call(PidSpec, {req, {read_latest_projection, ProjType}},
                    Timeout).

%% @doc Read a projection `Proj' of type `ProjType'.

read_projection(PidSpec, ProjType, Epoch) ->
    read_projection(PidSpec, ProjType, Epoch, infinity).

%% @doc Read a projection `Proj' of type `ProjType'.

read_projection(PidSpec, ProjType, Epoch, Timeout) ->
    gen_server:call(PidSpec, {req, {read_projection, ProjType, Epoch}},
                    Timeout).

%% @doc Write a projection `Proj' of type `ProjType'.

write_projection(PidSpec, ProjType, Proj) ->
    write_projection(PidSpec, ProjType, Proj, infinity).

%% @doc Write a projection `Proj' of type `ProjType'.

write_projection(PidSpec, ProjType, Proj, Timeout) ->
    case gen_server:call(PidSpec, {req, {write_projection, ProjType, Proj}},
                         Timeout) of
        {error, written}=Err ->
            Epoch = Proj#projection_v1.epoch_number,
            case read_projection(PidSpec, ProjType, Epoch, Timeout) of
                {ok, Proj2} when Proj2 == Proj ->
                    %% The proxy made (at least) two attempts to write
                    %% this projection.  An earlier one appeared to
                    %% have failed, so the proxy retried.  The later
                    %% attempt returned to us {error,written} because
                    %% the earlier attempt was actually received &
                    %% processed by the server.  So, we consider this
                    %% a successful write.
                    ok;
                _ ->
                    Err
            end;
        Else ->
            Else
    end.

%% @doc Get all projections from the FLU's projection store.

get_all_projections(PidSpec, ProjType) ->
    get_all_projections(PidSpec, ProjType, infinity).

%% @doc Get all projections from the FLU's projection store.

get_all_projections(PidSpec, ProjType, Timeout) ->
    gen_server:call(PidSpec, {req, {get_all_projections, ProjType}},
                    Timeout).

%% @doc Get all epoch numbers from the FLU's projection store.

list_all_projections(PidSpec, ProjType) ->
    list_all_projections(PidSpec, ProjType, infinity).

%% @doc Get all epoch numbers from the FLU's projection store.

list_all_projections(PidSpec, ProjType, Timeout) ->
    gen_server:call(PidSpec, {req, {list_all_projections, ProjType}},
                    Timeout).

%% @doc Kick (politely) the remote chain manager to react to a
%% projection change.

kick_projection_reaction(PidSpec, Options) ->
    kick_projection_reaction(PidSpec, Options, infinity).

%% @doc Kick (politely) the remote chain manager to react to a
%% projection change.

kick_projection_reaction(PidSpec, Options, Timeout) ->
    gen_server:call(PidSpec, {req, {kick_projection_reaction, Options}},
                    Timeout).

%% @doc Quit &amp; close the connection to remote FLU and stop our
%% proxy process.

quit(PidSpec) ->
    gen_server:call(PidSpec, quit, infinity).

%% @doc Write a chunk (binary- or iolist-style) of data to a file
%% with `Prefix' at `Offset'.

write_chunk(PidSpec, NSInfo, EpochID, File, Offset, Chunk, CSum) ->
    write_chunk(PidSpec, NSInfo, EpochID, File, Offset, Chunk, CSum, infinity).

%% @doc Write a chunk (binary- or iolist-style) of data to a file
%% with `Prefix' at `Offset'.

write_chunk(PidSpec, NSInfo, EpochID, File, Offset, Chunk, CSum, Timeout) ->
    case gen_server:call(PidSpec, {req, {write_chunk, NSInfo, EpochID, File, Offset, Chunk, CSum}},
                         Timeout) of
        {error, written}=Err ->
            Size = byte_size(Chunk),
            case read_chunk(PidSpec, NSInfo, EpochID, File, Offset, Size, undefined, Timeout) of
                {ok, {[{File, Offset, Chunk2, _}], []}} when Chunk2 == Chunk ->
                    %% See equivalent comment inside write_projection().
                    ok;
                _ ->
                    Err
            end;
        Else ->
            Else
    end.


trim_chunk(PidSpec, NSInfo, EpochID, File, Offset, Size) ->
    trim_chunk(PidSpec, NSInfo, EpochID, File, Offset, Size, infinity).

%% @doc Write a chunk (binary- or iolist-style) of data to a file
%% with `Prefix' at `Offset'.

trim_chunk(PidSpec, NSInfo, EpochID, File, Offset, Chunk, Timeout) ->
    gen_server:call(PidSpec,
                    {req, {trim_chunk, NSInfo, EpochID, File, Offset, Chunk}},
                    Timeout).

%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([I]) ->
    S0 = #state{i=I},
    S1 = try_connect(S0),
    {ok, S1}.

handle_call({req, Req}, _From, S) ->
    {Reply, NewS} = do_req(Req, S),
    {reply, Reply, NewS};
handle_call(quit, _From, S) ->
    {stop, normal, ok, disconnect(S)};
handle_call(_Request, _From, S) ->
    Reply = ok,
    {reply, Reply, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    io:format(user, "~s:handle_info: ~p\n", [?MODULE, _Info]),
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_req(Req, S) ->
    do_req(Req, 1, S).

do_req(Req, Depth, S) ->
    S2 = try_connect(S),
    Fun = make_req_fun(Req, S2),
    case connected_p(S2) of
        true ->
            case Fun() of
                ok ->
                    {ok, S2};
                T when element(1, T) == ok ->
                    {T, S2};
                TheErr ->
                    case get(bad_sock) of
                        Bad when Bad == S2#state.sock ->
                            do_req_retry(Req, Depth, TheErr, S2);
                        _ ->
                            {TheErr, S2}
                    end
            end;
        false ->
            {{error, partition}, S2}
    end.

do_req_retry(_Req, 2, Err, S) ->
    {Err, disconnect(S)};
do_req_retry(Req, Depth, _Err, S) ->
    do_req(Req, Depth + 1, try_connect(disconnect(S))).

make_req_fun({append_chunk, NSInfo, EpochID,
              Prefix, Chunk, CSum, Opts, Timeout},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:append_chunk(Sock, NSInfo, EpochID,
                              Prefix, Chunk, CSum, Opts, Timeout)
    end;
make_req_fun({read_chunk, NSInfo, EpochID, File, Offset, Size, Opts},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:read_chunk(Sock, NSInfo, EpochID, File, Offset, Size, Opts) end;
make_req_fun({write_chunk, NSInfo, EpochID, File, Offset, Chunk, CSum},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:write_chunk(Sock, NSInfo, EpochID, File, Offset, Chunk, CSum) end;
make_req_fun({trim_chunk, NSInfo, EpochID, File, Offset, Size},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:trim_chunk(Sock, NSInfo, EpochID, File, Offset, Size) end;
make_req_fun({checksum_list, File},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:checksum_list(Sock, File) end;
make_req_fun({list_files, EpochID},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:list_files(Sock, EpochID) end;
make_req_fun({wedge_status},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:wedge_status(Sock) end;
make_req_fun({get_epoch_id},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> case Mod:read_latest_projection(Sock, private) of
                 {ok, P} ->
                     #projection_v1{epoch_number=Epoch,
                                    epoch_csum=CSum} = P,
                     {ok, {Epoch, CSum}};
                 Error ->
                     Error
             end
    end;
make_req_fun({get_latest_epochid, ProjType},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:get_latest_epochid(Sock, ProjType) end;
make_req_fun({read_latest_projection, ProjType},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:read_latest_projection(Sock, ProjType) end;
make_req_fun({read_projection, ProjType, Epoch},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:read_projection(Sock, ProjType, Epoch) end;
make_req_fun({write_projection, ProjType, Proj},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:write_projection(Sock, ProjType, Proj) end;
make_req_fun({get_all_projections, ProjType},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:get_all_projections(Sock, ProjType) end;
make_req_fun({list_all_projections, ProjType},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:list_all_projections(Sock, ProjType) end;
make_req_fun({kick_projection_reaction, Options},
             #state{sock=Sock,i=#p_srvr{proto_mod=Mod}}) ->
    fun() -> Mod:kick_projection_reaction(Sock, Options) end.

connected_p(#state{sock=SockMaybe,
                   i=#p_srvr{proto_mod=Mod}=_I}=_S) ->
    Mod:connected_p(SockMaybe).

try_connect(#state{sock=undefined,
                   i=#p_srvr{proto_mod=Mod}=P}=S) ->
    Sock = Mod:connect(P),
    S#state{sock=Sock};
try_connect(S) ->
    %% If we're connection-based, we're already connected.
    %% If we're not connection-based, then there's nothing to do.
    S.

disconnect(#state{sock=undefined}=S) ->
    S;
disconnect(#state{sock=Sock,
                  i=#p_srvr{proto_mod=Mod}=_I}=S) ->
    Mod:disconnect(Sock),
    S#state{sock=undefined}.


stop_proxies(undefined) ->
    [];
stop_proxies(ProxiesDict) ->
    orddict:fold(
      fun(_K, Pid, _Acc) ->
              _ = (catch machi_proxy_flu1_client:quit(Pid))
      end, [], ProxiesDict).

start_proxies(MembersDict) ->
    Proxies = orddict:fold(
                fun(K, P, Acc) ->
                        {ok, Pid} = machi_proxy_flu1_client:start_link(P),
                        [{K, Pid}|Acc]
                end, [], orddict:to_list(MembersDict)),
    orddict:from_list(Proxies).


