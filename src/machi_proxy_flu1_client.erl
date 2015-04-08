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
%% Machi is intentionally avoiding using distributed Erlang for
%% Machi's communication.  This design decision makes Erlang-side code
%% more difficult &amp; complex, but it's the price to pay for some
%% language independence.  Later in Machi's life cycle, we need to
%% (re-)implement some components in a non-Erlang/BEAM-based language.
%%
%% This module implements a "man in the middle" proxy between the
%% Erlang client and Machi server (which is on the "far side" of a TCP
%% connection to somewhere).  This proxy process will always execute
%% on the same Erlang node as the Erlang client that uses it.  The
%% proxy is intended to be a stable, long-lived process that survives
%% TCP communication problems with the remote server.

-module(machi_proxy_flu1_client).

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
         append_chunk/4, append_chunk/5,
         read_chunk/5, read_chunk/6,
         checksum_list/3, checksum_list/4,
         list_files/2, list_files/3,

         %% %% Projection API
         get_latest_epoch/2, get_latest_epoch/3,
         read_latest_projection/2, read_latest_projection/3,
         read_projection/3, read_projection/4,
         write_projection/3, write_projection/4,
         get_all_projections/2, get_all_projections/3,
         list_all_projections/2, list_all_projections/3,

         %% Common API
         quit/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(FLU_C, machi_flu1_client).

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

append_chunk(PidSpec, EpochID, Prefix, Chunk) ->
    append_chunk(PidSpec, EpochID, Prefix, Chunk, infinity).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(PidSpec, EpochID, Prefix, Chunk, Timeout) ->
    gen_server:call(PidSpec, {req, {append_chunk, EpochID, Prefix, Chunk}},
                    Timeout).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, EpochID, File, Offset, Size) ->
    read_chunk(PidSpec, EpochID, File, Offset, Size, infinity).

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(PidSpec, EpochID, File, Offset, Size, Timeout) ->
    gen_server:call(PidSpec, {req, {read_chunk, EpochID, File, Offset, Size}},
                    Timeout).

%% @doc Fetch the list of chunk checksums for `File'.

checksum_list(PidSpec, EpochID, File) ->
    checksum_list(PidSpec, EpochID, File, infinity).

%% @doc Fetch the list of chunk checksums for `File'.

checksum_list(PidSpec, EpochID, File, Timeout) ->
    gen_server:call(PidSpec, {req, {checksum_list, EpochID, File}},
                    Timeout).

%% @doc Fetch the list of all files on the remote FLU.

list_files(PidSpec, EpochID) ->
    list_files(PidSpec, EpochID, infinity).

%% @doc Fetch the list of all files on the remote FLU.

list_files(PidSpec, EpochID, Timeout) ->
    gen_server:call(PidSpec, {req, {list_files, EpochID}},
                    Timeout).

%% @doc Get the latest epoch number + checksum from the FLU's projection store.

get_latest_epoch(PidSpec, ProjType) ->
    get_latest_epoch(PidSpec, ProjType, infinity).

%% @doc Get the latest epoch number + checksum from the FLU's projection store.

get_latest_epoch(PidSpec, ProjType, Timeout) ->
    gen_server:call(PidSpec, {req, {get_latest_epoch, ProjType}},
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
    gen_server:call(PidSpec, {req, {write_projection, ProjType, Proj}},
                    Timeout).

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

%% @doc Quit &amp; close the connection to remote FLU and stop our
%% proxy process.

quit(PidSpec) ->
    gen_server:call(PidSpec, quit, infinity).

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
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_req(Req, S) ->
    S2 = try_connect(S),
    Fun = make_req_fun(Req, S2),
    case connected_p(S2) of
        true ->
            case Fun() of
                T when element(1, T) == ok ->
                    {T, S2};
                Else ->
                    case get(bad_sock) of
                        Bad when Bad == S2#state.sock ->
                            {Else, disconnect(S2)};
                        _ ->
                            {Else, S2}
                    end
            end;
        false ->
            {{error, not_connected}, S2}
    end.

make_req_fun({append_chunk, EpochID, Prefix, Chunk}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:append_chunk(Sock, EpochID, Prefix, Chunk) end;
make_req_fun({read_chunk, EpochID, File, Offset, Size}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:read_chunk(Sock, EpochID, File, Offset, Size) end;
make_req_fun({checksum_list, EpochID, File}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:checksum_list(Sock, EpochID, File) end;
make_req_fun({list_files, EpochID}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:list_files(Sock, EpochID) end;
make_req_fun({get_latest_epoch, ProjType}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:get_latest_epoch(Sock, ProjType) end;
make_req_fun({read_latest_projection, ProjType}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:read_latest_projection(Sock, ProjType) end;
make_req_fun({read_projection, ProjType, Epoch}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:read_projection(Sock, ProjType, Epoch) end;
make_req_fun({write_projection, ProjType, Proj}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:write_projection(Sock, ProjType, Proj) end;
make_req_fun({get_all_projections, ProjType}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:get_all_projections(Sock, ProjType) end;
make_req_fun({list_all_projections, ProjType}, #state{sock=Sock}) ->
    fun() -> ?FLU_C:list_all_projections(Sock, ProjType) end.

connected_p(#state{sock=SockMaybe,
                   i=#p_srvr{proto=ipv4}=_I}=_S) ->
    is_port(SockMaybe);
connected_p(#state{i=#p_srvr{proto=disterl,
                             name=_NodeName}=_I}=_S) ->
    true.
    %% case net_adm:ping(NodeName) of
    %%     ping ->
    %%         true;
    %%     _ ->
    %%         false
    %% end.

try_connect(#state{sock=undefined,
                   i=#p_srvr{proto=ipv4, address=Host, port=TcpPort}=_I}=S) ->
    try
        Sock = machi_util:connect(Host, TcpPort),
        S#state{sock=Sock}
    catch
        _:_ ->
            S
    end;
try_connect(S) ->
    %% If we're connection-based, we're already connected.
    %% If we're not connection-based, then there's nothing to do.
    S.

disconnect(#state{sock=Sock,
                  i=#p_srvr{proto=ipv4}=_I}=S) ->
    (catch gen_tcp:close(Sock)),
    S#state{sock=undefined};
disconnect(S) ->
    S.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

dummy_server(Parent, TcpPort) ->
    spawn_link(fun() ->
                       {ok, LSock} = gen_tcp:listen(TcpPort,
                                                    [{reuseaddr,true},
                                                     {packet, line},
                                                     {mode, binary},
                                                     {active, false}]),
                       dummy_ack(Parent),
                       {ok, Sock} = gen_tcp:accept(LSock),
                       ok = inet:setopts(Sock, [{packet, line}]),
                       {ok, _Line} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock, "ERROR BADARG\n"),
                       (catch gen_tcp:close(Sock)),
                       unlink(Parent),
                       exit(normal)
               end).

dummy_ack(Parent) ->
    Parent ! go.

dummy_wait_for_ack() ->
    receive go -> ok end.    

smoke_test() ->
    TcpPort = 57123,
    Me = self(),
    _ServerPid = dummy_server(Me, TcpPort),
    dummy_wait_for_ack(),

    I = #p_srvr{name=smoke, proto=ipv4, address="localhost", port=TcpPort},
    S0 = #state{i=I},
    false = connected_p(S0),
    S1 = try_connect(S0),
    true = connected_p(S1),
    gen_tcp:send(S1#state.sock, "yo dawg\n"),
    {ok, _Answer} = gen_tcp:recv(S1#state.sock, 0),
    _S2 = disconnect(S1),
    
    ok.

api_smoke_test() ->
    RegName = api_smoke_flu,
    Host = "localhost",
    TcpPort = 57124,
    DataDir = "./data.api_smoke_flu",
    FLU1 = machi_flu1_test:setup_test_flu(RegName, TcpPort, DataDir),
    erase(flu_pid),

    try
        I = #p_srvr{name=RegName, proto=ipv4, address=Host, port=TcpPort},
        {ok, Prox1} = start_link(I),
        try
            FakeEpoch = {-1, <<0:(20*8)/big>>},
            [{ok, {_,_,_}} = append_chunk(Prox1,
                                          FakeEpoch, <<"prefix">>, <<"data">>,
                                          infinity) || _ <- lists:seq(1,5)],
            %% Stop the FLU, what happens?
            machi_flu1:stop(FLU1),
            {error,_} = append_chunk(Prox1,
                                FakeEpoch, <<"prefix">>, <<"data">>,
                                infinity),
            {error,not_connected} = append_chunk(Prox1,
                                FakeEpoch, <<"prefix">>, <<"data">>,
                                infinity),
            %% Start the FLU again, we should be able to do stuff immediately
            FLU1b = machi_flu1_test:setup_test_flu(RegName, TcpPort, DataDir,
                                                   [save_data_dir]),
            put(flu_pid, FLU1b),
            MyChunk = <<"my chunk data">>,
            {ok, {MyOff,MySize,MyFile}} =
                append_chunk(Prox1, FakeEpoch, <<"prefix">>, MyChunk,
                             infinity),
            {ok, MyChunk} = read_chunk(Prox1, FakeEpoch, MyFile, MyOff, MySize),

            %% Alright, now for the rest of the API, whee
            BadFile = <<"no-such-file">>,
            {error, no_such_file} = checksum_list(Prox1, FakeEpoch, BadFile),
            {ok, [_]} = list_files(Prox1, FakeEpoch),
            {ok, FakeEpoch} = get_latest_epoch(Prox1, public),
            {error, not_written} = read_latest_projection(Prox1, public),
            {error, not_written} = read_projection(Prox1, public, 44),
            P1 = machi_projection:new(1, a, [a], [], [a], [], []),
            ok = write_projection(Prox1, public, P1),
            {ok, P1} = read_projection(Prox1, public, 1),
            {ok, [P1]} = get_all_projections(Prox1, public),
            {ok, [1]} = list_all_projections(Prox1, public),
            ok
        after
            _ = (catch quit(Prox1))
        end
    after
        (catch machi_flu1:stop(FLU1)),
        (catch machi_flu1:stop(get(flu_pid)))
    end.
    
-endif. % TEST
