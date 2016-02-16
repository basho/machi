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

%% @doc Ranch protocol callback module to handle PB protocol over
%% transport, including both high and low modes.

%% TODO
%% - Two modes, high and low should be separated at listener level?

-module(machi_flu1_net_server).

-behaviour(gen_server).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/file.hrl").

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

-record(state, {
          %% Ranch's transport management stuff
          ref         :: ranch:ref(),
          socket      :: socket(),
          transport   :: module(),

          %% Machi FLU configurations, common for low and high
          data_dir    :: string(),
          witness     :: boolean(),
          pb_mode     :: undefined | high | low,
          %% - Used in projection related requests in low mode
          %% - Used in spawning CR client in high mode
          proj_store  :: pid(),

          %% Low mode only items
          %% Current best knowledge, used for wedge_self / bad_epoch check
          epoch_id    :: undefined | machi_dt:epoch_id(),
          %% Used in dispatching append_chunk* reqs to the
          %% append serializing process
          flu_name    :: pv1_server(),
          %% Used in server_wedge_status to lookup the table
          epoch_tab   :: ets:tab(),
          %% Clustering: cluster map version number
          namespace_version = 0 :: machi_dt:namespace_version(),
          %% Clustering: my (and my chain's) assignment to a specific namespace
          namespace = <<>> :: machi_dt:namespace(),

          %% High mode only
          high_clnt   :: pid(),

          %% anything you want
          props = []  :: proplists:proplist()
         }).

-type socket() :: any().
-type state()  :: #state{}.

-spec start_link(ranch:ref(), socket(), module(), [term()]) -> {ok, pid()}.
start_link(Ref, Socket, Transport, [FluName, Witness, DataDir, EpochTab, ProjStore, Props]) ->
    NS = proplists:get_value(namespace, Props, <<>>),
    true = is_binary(NS),
    proc_lib:start_link(?MODULE, init, [#state{ref=Ref,
                                               socket=Socket,
                                               transport=Transport,
                                               flu_name=FluName,
                                               witness=Witness,
                                               data_dir=DataDir,
                                               epoch_tab=EpochTab,
                                               proj_store=ProjStore,
                                               namespace=NS,
                                               props=Props}]).

-spec init(state()) -> no_return().
init(#state{ref=Ref, socket=Socket, transport=Transport}=State) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    {_Wedged_p, CurrentEpochID} = lookup_epoch(State),
    ok = Transport:setopts(Socket, [{active, once}|?PB_PACKET_OPTS]),
    gen_server:enter_loop(?MODULE, [], State#state{epoch_id=CurrentEpochID}).

handle_call(Request, _From, S) ->
    lager:warning("~s:handle_call UNKNOWN message: ~w", [?MODULE, Request]),
    Reply = {error, {unknown_message, Request}},
    {reply, Reply, S}.

handle_cast(_Msg, S) ->
    lager:warning("~s:handle_cast UNKNOWN message: ~w", [?MODULE, _Msg]),
    {noreply, S}.

%% TODO: Other transport support needed?? TLS/SSL, SCTP
handle_info({tcp, Socket, Data}=_Info, #state{socket=Socket}=S) ->
    lager:debug("~s:handle_info: ~w", [?MODULE, _Info]),
    transport_received(Socket, Data, S);
handle_info({tcp_closed, Socket}=_Info, #state{socket=Socket}=S) ->
    lager:debug("~s:handle_info: ~w", [?MODULE, _Info]),
    transport_closed(Socket, S);
handle_info({tcp_error, Socket, Reason}=_Info, #state{socket=Socket}=S) ->
    lager:warning("~s:handle_info (socket=~w) tcp_error: ~w", [?MODULE, Socket, Reason]),
    transport_error(Socket, Reason, S);
handle_info(_Info, S) ->
    lager:warning("~s:handle_info UNKNOWN message: ~w", [?MODULE, _Info]),
    {noreply, S}.

terminate(normal, #state{socket=undefined}=_S) ->
    ok;
terminate(Reason, #state{socket=undefined}=_S) ->
    lager:warning("~s:terminate (socket=undefined): ~w", [?MODULE, Reason]),
    ok;
terminate(normal, #state{socket=Socket}=_S) ->
    (catch gen_tcp:close(Socket)),
    ok;
terminate(Reason, #state{socket=Socket}=_S) ->
    lager:warning("~s:terminate (socket=Socket): ~w", [?MODULE, Reason]),
    (catch gen_tcp:close(Socket)),
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%% -- private

%%%% Common transport handling

-spec transport_received(socket(), machi_dt:chunk(), state()) ->
                                {noreply, state()}.
transport_received(Socket, <<"QUIT\n">>, #state{socket=Socket}=S) ->
    {stop, normal, S};
transport_received(Socket, Bin, #state{transport=Transport}=S) ->
    {RespBin, S2} =
        case machi_pb:decode_mpb_ll_request(Bin) of
            LL_req when LL_req#mpb_ll_request.do_not_alter == 2 ->
                {R, NewS} = do_pb_ll_request(LL_req, S),
                {maybe_encode_response(R), set_mode(low, NewS)};
            _ ->
                HL_req = machi_pb:decode_mpb_request(Bin),
                1 = HL_req#mpb_request.do_not_alter,
                {R, NewS} = do_pb_hl_request(HL_req, make_high_clnt(S)),
                {machi_pb:encode_mpb_response(R), set_mode(high, NewS)}
        end,
    case RespBin of
        async_no_response ->
            Transport:setopts(Socket, [{active, once}]),
            {noreply, S2};
        _ ->
            case Transport:send(Socket, RespBin) of
                ok ->
                    Transport:setopts(Socket, [{active, once}]),
                    {noreply, S2};
                {error, Reason} ->
                    transport_error(Socket, Reason, S2)
            end
    end.

-spec transport_closed(socket(), state()) -> {stop, term(), state()}.
transport_closed(_Socket, S) ->
    {stop, normal, S}.

-spec transport_error(socket(), term(), state()) -> no_return().
transport_error(Socket, Reason, #state{transport=Transport}=_S) ->
    Msg = io_lib:format("Socket error ~w", [Reason]),
    R = #mpb_ll_response{req_id= <<>>,
                         generic=#mpb_errorresp{code=1, msg=Msg}},
    _Resp = machi_pb:encode_mpb_ll_response(R),
    %% TODO for TODO comments: comments below with four %s are copy-n-paste'd,
    %% then it should be considered they are still open and should be addressed.
    %%%% TODO: Weird that sometimes neither catch nor try/catch
    %%%%       can prevent OTP's SASL from logging an error here.
    %%%%       Error in process <0.545.0> with exit value: {badarg,[{erlang,port_command,.......
    %%%% TODO: is this what causes the intermittent PULSE deadlock errors?
    %%%% _ = (catch gen_tcp:send(Sock, _Resp)), timer:sleep(1000),
    (catch Transport:close(Socket)),
    _ = lager:warning("Socket error (~w -> ~w): ~w",
                      [Transport:sockname(Socket), Transport:peername(Socket), Reason]),
    %% TODO: better to exit with `Reason' without logging?
    exit(normal).

maybe_encode_response(async_no_response=R) ->
    R;
maybe_encode_response(R) ->
    machi_pb:encode_mpb_ll_response(R).

set_mode(Mode, #state{pb_mode=undefined}=S) ->
    S#state{pb_mode=Mode};
set_mode(_, S) ->
    S.

%%%% Low PB mode %%%%

do_pb_ll_request(#mpb_ll_request{req_id=ReqID}, #state{pb_mode=high}=S) ->
    Result = {high_error, 41, "Low protocol request while in high mode"},
    {machi_pb_translate:to_pb_response(ReqID, unused, Result), S};
do_pb_ll_request(PB_request, S) ->
    Req = machi_pb_translate:from_pb_request(PB_request),
    {ReqID, Cmd, Result, S2} = 
        case Req of
            {RqID, {low_skip_wedge, LowSubCmd}=Cmd0} ->
                %% Skip wedge check for these unprivileged commands
                {Rs, NewS} = do_pb_ll_request3(LowSubCmd, S),
                {RqID, Cmd0, Rs, NewS};
            {RqID, {low_proj, _LowSubCmd}=Cmd0} ->
                {Rs, NewS} = do_pb_ll_request3(Cmd0, S),
                {RqID, Cmd0, Rs, NewS};
            {RqID, Cmd0} ->
                %% All remaining must have NSVersion, NS, & EpochID at next pos
                NSVersion = element(2, Cmd0),
                NS = element(3, Cmd0),
                EpochID = element(4, Cmd0),
                {Rs, NewS} = do_pb_ll_request2(NSVersion, NS, EpochID, Cmd0, S),
                {RqID, Cmd0, Rs, NewS}
        end,
    {machi_pb_translate:to_pb_response(ReqID, Cmd, Result), S2}.

%% do_pb_ll_request2(): Verification of epoch details & namespace details.

do_pb_ll_request2(NSVersion, NS, EpochID, CMD, S) ->
    {Wedged_p, CurrentEpochID} = lookup_epoch(S),
    if not is_tuple(EpochID) orelse tuple_size(EpochID) /= 2 ->
            exit({bad_epoch_id, EpochID, for, CMD});
       Wedged_p == true ->
            {{error, wedged}, S#state{epoch_id=CurrentEpochID}};
       EpochID /= CurrentEpochID ->
            {Epoch, _} = EpochID,
            {CurrentEpoch, _} = CurrentEpochID,
            if Epoch < CurrentEpoch ->
                    {{error, bad_epoch}, S};
               true ->
                    _ = machi_flu1:wedge_myself(S#state.flu_name, CurrentEpochID),
                    {{error, wedged}, S#state{epoch_id=CurrentEpochID}}
            end;
       true ->
            #state{namespace_version=MyNSVersion, namespace=MyNS} = S,
            if NSVersion /= MyNSVersion ->
                    {{error, bad_epoch}, S};
               NS /= MyNS ->
                    {{error, bad_arg}, S};
               true ->
                    do_pb_ll_request3(CMD, S)
            end
    end.

lookup_epoch(#state{epoch_tab=T}) ->
    %% TODO: race in shutdown to access ets table after owner dies
    ets:lookup_element(T, epoch, 2).

%% Witness status does not matter below.
do_pb_ll_request3({low_echo, Msg}, S) ->
    {Msg, S};
do_pb_ll_request3({low_auth, _User, _Pass}, S) ->
    {-6, S};
do_pb_ll_request3({low_wedge_status}, S) ->
    {do_server_wedge_status(S), S};
do_pb_ll_request3({low_proj, PCMD}, S) ->
    {do_server_proj_request(PCMD, S), S};

%% Witness status *matters* below
do_pb_ll_request3({low_append_chunk, NSVersion, NS, EpochID, NSLocator,
                   Prefix, Chunk, CSum_tag,
                   CSum, Opts},
                  #state{witness=false}=S) ->
    NSInfo = #ns_info{version=NSVersion, name=NS, locator=NSLocator},
    {do_server_append_chunk(NSInfo, EpochID,
                            Prefix, Chunk, CSum_tag, CSum,
                            Opts, S), S};
do_pb_ll_request3({low_write_chunk, _NSVersion, _NS, _EpochID, File, Offset, Chunk, CSum_tag,
                   CSum},
                  #state{witness=false}=S) ->
    {do_server_write_chunk(File, Offset, Chunk, CSum_tag, CSum, S), S};
do_pb_ll_request3({low_read_chunk, _NSVersion, _NS, _EpochID, File, Offset, Size, Opts},
                  #state{witness=false} = S) ->
    {do_server_read_chunk(File, Offset, Size, Opts, S), S};
do_pb_ll_request3({low_trim_chunk, _NSVersion, _NS, _EpochID, File, Offset, Size, TriggerGC},
                  #state{witness=false}=S) ->
    {do_server_trim_chunk(File, Offset, Size, TriggerGC, S), S};
do_pb_ll_request3({low_checksum_list, File},
                  #state{witness=false}=S) ->
    {do_server_checksum_listing(File, S), S};
do_pb_ll_request3({low_list_files, _EpochID},
                  #state{witness=false}=S) ->
    {do_server_list_files(S), S};
do_pb_ll_request3({low_delete_migration, _EpochID, File},
                  #state{witness=false}=S) ->
    {do_server_delete_migration(File, S),
     #state{witness=false}=S};
do_pb_ll_request3({low_trunc_hack, _EpochID, File},
                  #state{witness=false}=S) ->
    {do_server_trunc_hack(File, S), S};

do_pb_ll_request3(_, #state{witness=true}=S) ->
    {{error, bad_arg}, S}.                       % TODO: new status code??

do_server_proj_request({get_latest_epochid, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:get_latest_epochid(ProjStore, ProjType);
do_server_proj_request({read_latest_projection, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:read_latest_projection(ProjStore, ProjType);
do_server_proj_request({read_projection, ProjType, Epoch},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:read(ProjStore, ProjType, Epoch);
do_server_proj_request({write_projection, ProjType, Proj},
                       #state{flu_name=FluName, proj_store=ProjStore}) ->
    if Proj#projection_v1.epoch_number == ?SPAM_PROJ_EPOCH ->
            %% io:format(user, "DBG ~s ~w ~P\n", [?MODULE, ?LINE, Proj, 5]),
            Chmgr = machi_flu_psup:make_fitness_regname(FluName),
            [Map] = Proj#projection_v1.dbg,
            catch machi_fitness:send_fitness_update_spam(
                    Chmgr, Proj#projection_v1.author_server, Map);
       true ->
            catch machi_projection_store:write(ProjStore, ProjType, Proj)
    end;
do_server_proj_request({get_all_projections, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:get_all_projections(ProjStore, ProjType);
do_server_proj_request({list_all_projections, ProjType},
                       #state{proj_store=ProjStore}) ->
    machi_projection_store:list_all_projections(ProjStore, ProjType);
do_server_proj_request({kick_projection_reaction},
                       #state{flu_name=FluName}) ->
    %% Tell my chain manager that it might want to react to
    %% this new world.
    Chmgr = machi_chain_manager1:make_chmgr_regname(FluName),
    spawn(fun() ->
                  catch machi_chain_manager1:trigger_react_to_env(Chmgr)
          end),
    async_no_response.

do_server_append_chunk(NSInfo, EpochID,
                       Prefix, Chunk, CSum_tag, CSum,
                       Opts, S) ->
    case sanitize_prefix(Prefix) of
        ok ->
            do_server_append_chunk2(NSInfo, EpochID,
                                    Prefix, Chunk, CSum_tag, CSum,
                                    Opts, S);
        _ ->
            {error, bad_arg}
    end.

do_server_append_chunk2(NSInfo, EpochID,
                        Prefix, Chunk, CSum_tag, Client_CSum,
                        Opts, #state{flu_name=FluName,
                                     epoch_id=EpochID}=_S) ->
    %% TODO: Do anything with PKey?
    try
        TaggedCSum = check_or_make_tagged_checksum(CSum_tag, Client_CSum,Chunk),
        R = {seq_append, self(), NSInfo, EpochID,
             Prefix, Chunk, TaggedCSum, Opts},
        case gen_server:call(FluName, R, 10*1000) of
            {assignment, Offset, File} ->
                Size = iolist_size(Chunk),
                {ok, {Offset, Size, File}};
            witness ->
                {error, bad_arg};
            wedged ->
                {error, wedged};
            {error, timeout} ->
                {error, partition}
        end
    catch
        throw:{bad_csum, _CS} ->
            {error, bad_checksum};
        error:badarg ->
            lager:error("badarg at ~w:do_server_append_chunk2:~w ~w",
                        [?MODULE, ?LINE, erlang:get_stacktrace()]),
            {error, bad_arg}
    end.

do_server_write_chunk(File, Offset, Chunk, CSum_tag, CSum, #state{flu_name=FluName}) ->
    case sanitize_file_string(File) of
        ok ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, File}) of
                {ok, Pid} ->
                    Meta = [{client_csum_tag, CSum_tag}, {client_csum, CSum}],
                    machi_file_proxy:write(Pid, Offset, Meta, Chunk);
                {error, trimmed} = Error ->
                    Error
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_read_chunk(File, Offset, Size, Opts, #state{flu_name=FluName})->
    case sanitize_file_string(File) of
        ok ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, File}) of
                {ok, Pid} ->
                    case machi_file_proxy:read(Pid, Offset, Size, Opts) of
                        %% XXX FIXME 
                        %% For now we are omiting the checksum data because it blows up
                        %% protobufs.
                        {ok, ChunksAndTrimmed} -> {ok, ChunksAndTrimmed};
                        Other -> Other
                    end;
                {error, trimmed} = Error ->
                    Error
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_trim_chunk(File, Offset, Size, TriggerGC, #state{flu_name=FluName}) ->
    lager:debug("Hi there! I'm trimming this: ~s, (~p, ~p), ~p~n",
                [File, Offset, Size, TriggerGC]),
    case sanitize_file_string(File) of
        ok ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, File}) of
                {ok, Pid} ->
                    machi_file_proxy:trim(Pid, Offset, Size, TriggerGC);
                {error, trimmed} = Trimmed ->
                    %% Should be returned back to (maybe) trigger repair
                    Trimmed
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_checksum_listing(File, #state{flu_name=FluName, data_dir=DataDir}=_S) ->
    case sanitize_file_string(File) of
        ok ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, File}) of
                {ok, Pid} ->
                    {ok, List} = machi_file_proxy:checksum_list(Pid),
                    Bin = erlang:term_to_binary(List),
                    if byte_size(Bin) > (?PB_MAX_MSG_SIZE - 1024) ->
                            %% TODO: Fix this limitation by streaming the
                            %% binary in multiple smaller PB messages.
                            %% Also, don't read the file all at once. ^_^
                            error_logger:error_msg("~s:~w oversize ~s\n",
                                                   [?MODULE, ?LINE, DataDir]),
                            {error, bad_arg};
                       true ->
                            {ok, Bin}
                    end;
                {error, trimmed} ->
                    {error, trimmed}
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_list_files(#state{data_dir=DataDir}=_S) ->
    {_, WildPath} = machi_util:make_data_filename(DataDir, ""),
    Files = filelib:wildcard("*", WildPath),
    {ok, [begin
              {ok, FI} = file:read_file_info(WildPath ++ "/" ++ File),
              Size = FI#file_info.size,
              {Size, File}
          end || File <- Files]}.

do_server_wedge_status(#state{namespace_version=NSVersion, namespace=NS}=S) ->
    {Wedged_p, CurrentEpochID0} = lookup_epoch(S),
    CurrentEpochID = if CurrentEpochID0 == undefined ->
                             ?DUMMY_PV1_EPOCH;
                        true ->
                             CurrentEpochID0
                     end,
    {Wedged_p, CurrentEpochID, NSVersion, NS}.

do_server_delete_migration(File, #state{data_dir=DataDir}=_S) ->
    case sanitize_file_string(File) of
        ok ->
            {_, Path} = machi_util:make_data_filename(DataDir, File),
            case file:delete(Path) of
                ok ->
                    ok;
                {error, enoent} ->
                    {error, no_such_file};
                _ ->
                    {error, bad_arg}
            end;
        _ ->
            {error, bad_arg}
    end.

do_server_trunc_hack(File, #state{data_dir=DataDir}=_S) ->
    case sanitize_file_string(File) of
        ok ->
            {_, Path} = machi_util:make_data_filename(DataDir, File),
            case file:open(Path, [read, write, binary, raw]) of
                {ok, FH} ->
                    try
                        {ok, ?MINIMUM_OFFSET} = file:position(FH,
                                                              ?MINIMUM_OFFSET),
                        ok = file:truncate(FH),
                        ok
                    after
                        file:close(FH)
                    end;
                {error, enoent} ->
                    {error, no_such_file};
                _ ->
                    {error, bad_arg}
            end;
        _ ->
            {error, bad_arg}
    end.

sanitize_file_string(Str) ->
    case has_no_prohibited_chars(Str) andalso machi_util:is_valid_filename(Str) of
        true -> ok;
        false -> error
    end.

has_no_prohibited_chars(Str) ->
    case re:run(Str, "/") of
        nomatch ->
            true;
        _ ->
            true
    end.

sanitize_prefix(Prefix) ->
    %% We are using '^' as our component delimiter
    case re:run(Prefix, "/|\\^") of
        nomatch ->
            ok;
        _ ->
            error
    end.

check_or_make_tagged_checksum(?CSUM_TAG_NONE, _Client_CSum, Chunk) ->
    %% TODO: If the client was foolish enough to use
    %% this type of non-checksum, then the client gets
    %% what it deserves wrt data integrity, alas.  In
    %% the client-side Chain Replication method, each
    %% server will calculated this independently, which
    %% isn't exactly what ought to happen for best data
    %% integrity checking.  In server-side CR, the csum
    %% should be calculated by the head and passed down
    %% the chain together with the value.
    CS = machi_util:checksum_chunk(Chunk),
    machi_util:make_tagged_csum(server_sha, CS);
check_or_make_tagged_checksum(?CSUM_TAG_CLIENT_SHA, Client_CSum, Chunk) ->
    CS = machi_util:checksum_chunk(Chunk),
    if CS == Client_CSum ->
            machi_util:make_tagged_csum(server_sha,
                                        Client_CSum);
       true ->
            throw({bad_csum, CS})
    end.

%%%% High PB mode %%%%

do_pb_hl_request(#mpb_request{req_id=ReqID}, #state{pb_mode=low}=S) ->
    Result = {low_error, 41, "High protocol request while in low mode"},
    {machi_pb_translate:to_pb_response(ReqID, unused, Result), S};
do_pb_hl_request(PB_request, S) ->
    {ReqID, Cmd} = machi_pb_translate:from_pb_request(PB_request),
    {Result, S2} = do_pb_hl_request2(Cmd, S),
    {machi_pb_translate:to_pb_response(ReqID, Cmd, Result), S2}.

do_pb_hl_request2({high_echo, Msg}, S) ->
    {Msg, S};
do_pb_hl_request2({high_auth, _User, _Pass}, S) ->
    {-77, S};
do_pb_hl_request2({high_append_chunk=Op, NS, Prefix, Chunk, TaggedCSum, Opts},
                  #state{high_clnt=Clnt}=S) ->
    NSInfo = #ns_info{name=NS},                 % TODO populate other fields
    todo_perhaps_remind_ns_locator_not_chosen(Op),
    Res = machi_cr_client:append_chunk(Clnt, NSInfo,
                                       Prefix, Chunk, TaggedCSum, Opts),
    {Res, S};
do_pb_hl_request2({high_write_chunk=Op, File, Offset, Chunk, CSum},
                  #state{high_clnt=Clnt}=S) ->
    NSInfo = undefined,
    todo_perhaps_remind_ns_locator_not_chosen(Op),
    Res = machi_cr_client:write_chunk(Clnt, NSInfo, File, Offset, Chunk, CSum),
    {Res, S};
do_pb_hl_request2({high_read_chunk=Op, File, Offset, Size, Opts},
                  #state{high_clnt=Clnt}=S) ->
    NSInfo = undefined,
    todo_perhaps_remind_ns_locator_not_chosen(Op),
    Res = machi_cr_client:read_chunk(Clnt, NSInfo, File, Offset, Size, Opts),
    {Res, S};
do_pb_hl_request2({high_trim_chunk=Op, File, Offset, Size},
                  #state{high_clnt=Clnt}=S) ->
    NSInfo = undefined,
    todo_perhaps_remind_ns_locator_not_chosen(Op),
    Res = machi_cr_client:trim_chunk(Clnt, NSInfo, File, Offset, Size),
    {Res, S};
do_pb_hl_request2({high_checksum_list, File}, #state{high_clnt=Clnt}=S) ->
    Res = machi_cr_client:checksum_list(Clnt, File),
    {Res, S};
do_pb_hl_request2({high_list_files}, #state{high_clnt=Clnt}=S) ->
    Res = machi_cr_client:list_files(Clnt),
    {Res, S}.

make_high_clnt(#state{high_clnt=undefined}=S) ->
    {ok, Proj} = machi_projection_store:read_latest_projection(
                   S#state.proj_store, private),
    Ps = [P_srvr || {_, P_srvr} <- orddict:to_list(
                                     Proj#projection_v1.members_dict)],
    {ok, Clnt} = machi_cr_client:start_link(Ps),
    S#state{high_clnt=Clnt};
make_high_clnt(S) ->
    S.

todo_perhaps_remind_ns_locator_not_chosen(Op) ->
    Key = {?MODULE, Op},
    case get(Key) of
        undefined ->
            io:format(user, "TODO op ~w is using default locator value\n",
                      [Op]),
            put(Key, true);
        _ ->
            ok
    end.

