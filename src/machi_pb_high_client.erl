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

%% @doc Machi PB (Protocol Buffers) high-level client (prototype, API TBD)
%%
%% At the moment, this is prototype-quality code: the API is not yet
%% fully specified, there is very little error handling with respect
%% to a single socket connection, and there is no code to deal with
%% multiple connections/load balancing/error handling to several/all
%% Machi cluster servers.

-module(machi_pb_high_client).

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").

-define(DEFAULT_TIMEOUT, 10*1000).

-export([start_link/1, quit/1,
         connected_p/1,
         echo/2, echo/3,
         auth/3, auth/4,
         append_chunk/6, append_chunk/7
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          server_list :: p_srvr_dict(),
          sock        :: 'undefined' | port(),
          sock_id     :: integer(),
          count=0     :: non_neg_integer()
         }).

start_link(P_srvr_list) ->
    gen_server:start_link(?MODULE, [P_srvr_list], []).

quit(PidSpec) ->
    gen_server:call(PidSpec, quit, infinity).

connected_p(PidSpec) ->
    gen_server:call(PidSpec, connected_p, infinity).

echo(PidSpec, String) ->
    echo(PidSpec, String, ?DEFAULT_TIMEOUT).

echo(PidSpec, String, Timeout) ->
    send_sync(PidSpec, {echo, String}, Timeout).

%% TODO: auth() is not implemented.  Auth requires SSL, and this client
%% doesn't support SSL yet.  This is just a placeholder & reminder.

auth(PidSpec, User, Pass) ->
    auth(PidSpec, User, Pass, ?DEFAULT_TIMEOUT).

auth(PidSpec, User, Pass, Timeout) ->
    send_sync(PidSpec, {auth, User, Pass}, Timeout).

append_chunk(PidSpec, PlacementKey, Prefix, Chunk, CSum, ChunkExtra) ->
    append_chunk(PidSpec, PlacementKey, Prefix, Chunk, CSum, ChunkExtra, ?DEFAULT_TIMEOUT).

append_chunk(PidSpec, PlacementKey, Prefix, Chunk, CSum, ChunkExtra, Timeout) ->
    send_sync(PidSpec, {append_chunk, PlacementKey, Prefix, Chunk, CSum, ChunkExtra}, Timeout).

send_sync(PidSpec, Cmd, Timeout) ->
    gen_server:call(PidSpec, {send_sync, Cmd}, Timeout).

%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([P_srvr_list]) ->
    {ok, #state{server_list=P_srvr_list}}.

handle_call(quit, _From, S) ->
    {stop, normal, ok, S};
handle_call(connected_p, _From, #state{sock=Port}=S)
  when is_port(Port) ->
    {reply, true, S};
handle_call(connected_p, _From, #state{sock=undefined}=S) ->
    S2 = try_connect(S),
    {reply, is_port(S2#state.sock), S2};
handle_call({send_sync, _Cmd}, _From, #state{sock=undefined}=S) ->
    {reply, not_connected, S};
handle_call({send_sync, Cmd}, _From, S) ->
    {Reply, S2} = do_send_sync(Cmd, S),
    {reply, Reply, S2};
handle_call(_Request, _From, S) ->
    Reply = whaaaaaaaaaaaaaaaaaaaa,
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

try_connect(#state{sock=Sock}=S) when is_port(Sock) ->
    S;
try_connect(#state{server_list=Ps}=S) ->
    case lists:foldl(fun(_, Sock) when is_port(Sock) ->
                             Sock;
                        (P, _) ->
                             do_connect_to_pb_listener(P)
                     end, unused, Ps) of
        Sock when is_port(Sock) ->
            {id, Index} = erlang:port_info(Sock, id),
            S#state{sock=Sock, sock_id=Index, count=0};
        _Else ->
            S
    end.

do_connect_to_pb_listener(P) ->
    try
        {ok, Sock} = gen_tcp:connect(P#p_srvr.address, P#p_srvr.port,
                                     [{packet, line}, binary, {active, false}]),
        ok = gen_tcp:send(Sock, <<"PROTOCOL-BUFFERS\n">>),
        {ok, <<"OK\n">>} = gen_tcp:recv(Sock, 0),
        ok = inet:setopts(Sock, [{packet,4}]),
        Sock
    catch _X:_Y ->
            io:format(user, "\n~p ~p @ ~p\n", [_X, _Y, erlang:get_stacktrace()]),
            bummer
    end.

do_send_sync({echo, String}, #state{sock=Sock}=S) ->
    try
        ReqID = <<0>>,
        R1a = #mpb_request{req_id=ReqID,
                           echo=#mpb_echoreq{message=String}},
        Bin1a = machi_pb:encode_mpb_request(R1a),
        ok = gen_tcp:send(Sock, Bin1a),
        {ok, Bin1B} = gen_tcp:recv(Sock, 0),
        case (catch machi_pb:decode_mpb_response(Bin1B)) of
            #mpb_response{req_id=ReqID, echo=Echo} when Echo /= undefined ->
                {Echo#mpb_echoresp.message, S};
            #mpb_response{req_id=ReqID, generic=G} when G /= undefined ->
                #mpb_errorresp{code=Code, msg=Msg, extra=Extra} = G,
                {{error, {Code, Msg, Extra}}, S}
        end
    catch X:Y ->
            Res = {bummer, {X, Y, erlang:get_stacktrace()}},
            {Res, S}
    end;
do_send_sync({auth, User, Pass}, #state{sock=Sock}=S) ->
    try
        ReqID = <<0>>,
        R1a = #mpb_request{req_id=ReqID,
                           auth=#mpb_authreq{user=User, password=Pass}},
        Bin1a = machi_pb:encode_mpb_request(R1a),
        ok = gen_tcp:send(Sock, Bin1a),
        {ok, Bin1B} = gen_tcp:recv(Sock, 0),
        case (catch machi_pb:decode_mpb_response(Bin1B)) of
            #mpb_response{req_id=ReqID, auth=Auth} when Auth /= undefined ->
                {Auth#mpb_authresp.code, S};
            #mpb_response{req_id=ReqID, generic=G} when G /= undefined ->
                #mpb_errorresp{code=Code, msg=Msg, extra=Extra} = G,
                {{error, {Code, Msg, Extra}}, S}
        end
    catch X:Y ->
            Res = {bummer, {X, Y, erlang:get_stacktrace()}},
            {Res, S}
    end;
do_send_sync({append_chunk, PlacementKey, Prefix, Chunk, CSum, ChunkExtra},
             #state{sock=Sock, sock_id=Index, count=Count}=S) ->
    try
        ReqID = <<Index:64/big, Count:64/big>>,
        PK = if PlacementKey == <<>> -> undefined;
                true                 -> PlacementKey
             end,
        CSumT = case CSum of
                    none ->
                        #mpb_chunkcsum{type='CSUM_TAG_NONE',
                                       csum=undefined};
                    {client_sha, CSumBin} ->
                        #mpb_chunkcsum{type='CSUM_TAG_CLIENT_SHA',
                                       csum=CSumBin}
                end,
        Req = #mpb_appendchunkreq{placement_key=PK,
                                  prefix=Prefix,
                                  chunk=Chunk,
                                  csum=CSumT,
                                  chunk_extra=ChunkExtra},
        R1a = #mpb_request{req_id=ReqID,
                           append_chunk=Req},
        Bin1a = machi_pb:encode_mpb_request(R1a),
        ok = gen_tcp:send(Sock, Bin1a),
        {ok, Bin1B} = gen_tcp:recv(Sock, 0),
        case (catch machi_pb:decode_mpb_response(Bin1B)) of
            #mpb_response{req_id=ReqID, append_chunk=R} when R /= undefined ->
                Result = convert_append_chunk_resp(R),
                {Result, S#state{count=Count+1}};
            #mpb_response{req_id=ReqID, generic=G} when G /= undefined ->
                #mpb_errorresp{code=Code, msg=Msg, extra=Extra} = G,
                {{error, {Code, Msg, Extra}}, S#state{count=Count+1}}
        end
    catch X:Y ->
            Res = {bummer, {X, Y, erlang:get_stacktrace()}},
            {Res, S#state{count=Count+1}}
    end.

convert_append_chunk_resp(#mpb_appendchunkresp{status='OK', chunk_pos=CP}) ->
    #mpb_chunkpos{offset=Offset, chunk_size=Size, file_name=File} = CP,
    {ok, {Offset, Size, File}};
convert_append_chunk_resp(#mpb_appendchunkresp{status='BAD_ARG'}) ->
    {error, bad_arg};
convert_append_chunk_resp(#mpb_appendchunkresp{status='WEDGED'}) ->
    {error, wedged};
convert_append_chunk_resp(#mpb_appendchunkresp{status='BAD_CHECKSUM'}) ->
    {error, bad_checksum};
convert_append_chunk_resp(#mpb_appendchunkresp{status='PARTITION'}) ->
    {error, partition};
convert_append_chunk_resp(#mpb_appendchunkresp{status='BAD_JOSS'}) ->
    throw({error, bad_joss_taipan_fixme}).


