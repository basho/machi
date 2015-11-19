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

%% @doc The Machi FLU file server + file location sequencer.
%%
%% This module implements only the Machi FLU file server and its
%% implicit sequencer.  
%% Please see the EDoc "Overview" for details about the FLU as a
%% primitive file server process vs. the larger Machi design of a FLU
%% as a sequencer + file server + chain manager group of processes.
%%
%% The FLU is named after the CORFU server "FLU" or "FLash Unit" server.
%%
%% === Protocol origins ===
%%
%% Today's FLU fully supports a protocol that is based on Protocol
%% Buffers.  Please see the `src/machi.proto' file for details.
%%
%% === TODO items ===
%%
%% TODO The per-file metadata tuple store is missing from this implementation.
%%
%% TODO Section 4.1 ("The FLU") of the Machi design doc suggests that
%% the FLU keep track of the epoch number of the last file write (and
%% perhaps last metadata write), as an optimization for inter-FLU data
%% replication/chain repair.

-module(machi_flu1).

-include_lib("kernel/include/file.hrl").

-include("machi.hrl").
-include("machi_pb.hrl").
-include("machi_projection.hrl").
-define(V(X,Y), ok).
%% -include("machi_verbose.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

-export([start_link/1, stop/1,
         update_wedge_state/3, wedge_myself/2]).
-export([make_projection_server_regname/1]).
%% TODO: remove or replace in OTP way after gen_*'ified
-export([main2/4, run_append_server/2,
         current_state/1, format_state/1]).

-record(state, {
          flu_name        :: atom(),
          proj_store      :: pid(),
          witness = false :: boolean(),
          append_pid      :: pid(),
          wedged = true   :: boolean(),
          etstab          :: ets:tid(),
          epoch_id        :: 'undefined' | machi_dt:epoch_id(),
          props = []      :: list()  % proplist
         }).

-define(SERVER_CMD_READ_TIMEOUT, 600*1000).
-define(INIT_TIMEOUT, 60*1000).

start_link([{FluName, TcpPort, DataDir}|Rest])
  when is_atom(FluName), is_integer(TcpPort), is_list(DataDir) ->
    proc_lib:start_link(?MODULE, main2, [FluName, TcpPort, DataDir, Rest],
                        ?INIT_TIMEOUT).

stop(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! killme,
            ok;
        false ->
            error
    end.

update_wedge_state(PidSpec, Boolean, EpochId)
  when (Boolean == true orelse Boolean == false), is_tuple(EpochId) ->
    PidSpec ! {wedge_state_change, Boolean, EpochId}.

wedge_myself(PidSpec, EpochId)
  when is_tuple(EpochId) ->
    PidSpec ! {wedge_myself, EpochId}.

current_state(PidSpec) ->
    PidSpec ! {current_state, self()},
    %% TODO: Not so rubust f(^^;)
    receive
        Res -> Res
    after
        60*1000 -> {error, timeout}
    end.

format_state(State) ->
    Fields = record_info(fields, state),
    [_Name | Values] = tuple_to_list(State),
    lists:zip(Fields, Values).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ets_table_name(FluName) when is_atom(FluName) ->
    list_to_atom(atom_to_list(FluName) ++ "_epoch").

main2(FluName, TcpPort, DataDir, Props) ->
    {SendAppendPidToProj_p, ProjectionPid} =
        case proplists:get_value(projection_store_registered_name, Props) of
            undefined ->
                RN = make_projection_server_regname(FluName),
                {ok, PP} =
                    machi_projection_store:start_link(RN, DataDir, undefined),
                {true, PP};
            RN ->
                {false, whereis(RN)}
        end,
    InitialWedged_p = proplists:get_value(initial_wedged, Props),
    ProjRes = machi_projection_store:read_latest_projection(ProjectionPid,
                                                            private),
    {Wedged_p, EpochId} =
        if InitialWedged_p == undefined,
           is_tuple(ProjRes), element(1, ProjRes) == ok ->
                {ok, Proj} = ProjRes,
                {false, {Proj#projection_v1.epoch_number,
                         Proj#projection_v1.epoch_csum}};
           InitialWedged_p == false ->
                {false, ?DUMMY_PV1_EPOCH};
           true ->
                {true, undefined}
        end,
    Witness_p = proplists:get_value(witness_mode, Props, false),
    
    S0 = #state{flu_name=FluName,
                proj_store=ProjectionPid,
                wedged=Wedged_p,
                witness=Witness_p,
                etstab=ets_table_name(FluName),
                epoch_id=EpochId,
                props=Props},
    {ok, AppendPid} = start_append_server(S0, self()),
    if SendAppendPidToProj_p ->
            machi_projection_store:set_wedge_notify_pid(ProjectionPid,
                                                        AppendPid);
       true ->
            ok
    end,
    S1 = S0#state{append_pid=AppendPid},
    {ok, ListenerPid} = start_listen_server(TcpPort, DataDir, S1),
    %% io:format(user, "Listener started: ~w~n", [{FluName, ListenerPid}]),

    Config_e = machi_util:make_config_filename(DataDir, "unused"),
    ok = filelib:ensure_dir(Config_e),
    {_, Data_e} = machi_util:make_data_filename(DataDir, "unused"),
    ok = filelib:ensure_dir(Data_e),
    Projection_e = machi_util:make_projection_filename(DataDir, "unused"),
    ok = filelib:ensure_dir(Projection_e),

    put(flu_flu_name, FluName),
    put(flu_append_pid, S1#state.append_pid),
    put(flu_projection_pid, ProjectionPid),
    put(flu_listen_pid, ListenerPid),
    proc_lib:init_ack({ok, self()}),

    receive killme -> ok end,
    (catch exit(S1#state.append_pid, kill)),
    (catch exit(ProjectionPid, kill)),
    (catch exit(ListenerPid, kill)),
    ok.

start_append_server(S, AckPid) ->
    proc_lib:start_link(?MODULE, run_append_server, [AckPid, S], ?INIT_TIMEOUT).

start_listen_server(TcpPort, DataDir,
                    #state{flu_name=FluName, witness=Witness, etstab=EtsTab,
                           proj_store=ProjStore}=_S) ->
    machi_listener_sup:start_listener(FluName, TcpPort, Witness, DataDir,
                                      EtsTab, ProjStore).

run_append_server(FluPid, #state{flu_name=Name,
                                 wedged=Wedged_p,epoch_id=EpochId}=S) ->
    %% Reminder: Name is the "main" name of the FLU, i.e., no suffix
    register(Name, self()),
    TID = ets:new(ets_table_name(Name),
                  [set, protected, named_table, {read_concurrency, true}]),
    ets:insert(TID, {epoch, {Wedged_p, EpochId}}),
    proc_lib:init_ack({ok, self()}),
    append_server_loop(FluPid, S#state{etstab=TID}).

append_server_loop(FluPid, #state{wedged=Wedged_p,
                                  witness=Witness_p,
                                  epoch_id=OldEpochId, flu_name=FluName}=S) ->
    receive
        {seq_append, From, _N, _L, _Prefix, _Chunk, _CSum, _Extra, _EpochID}
          when Witness_p ->
            %% The FLU's net_server_loop() process ought to filter all
            %% witness states, but we'll keep this clause for extra
            %% paranoia.
            From ! witness,
            append_server_loop(FluPid, S);
        {seq_append, From, _N, _L, _Prefix, _Chunk, _CSum, _Extra, _EpochID}
          when Wedged_p ->
            From ! wedged,
            append_server_loop(FluPid, S);
        {seq_append, From, CoC_Namespace, CoC_Locator,
         Prefix, Chunk, CSum, Extra, EpochID} ->
            %% Old is the one from our state, plain old 'EpochID' comes
            %% from the client.
            _ = case OldEpochId == EpochID of
                true ->
                    spawn(fun() -> 
                        append_server_dispatch(From, CoC_Namespace, CoC_Locator,
                                               Prefix, Chunk, CSum, Extra,
                                               FluName, EpochID) 
                    end);
                false ->
                    From ! {error, bad_epoch}
            end,
            append_server_loop(FluPid, S);
        {wedge_myself, WedgeEpochId} ->
            if not Wedged_p andalso WedgeEpochId == OldEpochId ->
                    true = ets:insert(S#state.etstab,
                                      {epoch, {true, OldEpochId}}),
                    %% Tell my chain manager that it might want to react to
                    %% this new world.
                    Chmgr = machi_chain_manager1:make_chmgr_regname(FluName),
                    spawn(fun() ->
                            catch machi_chain_manager1:trigger_react_to_env(Chmgr)
                          end),
                    append_server_loop(FluPid, S#state{wedged=true});
               true ->
                    append_server_loop(FluPid, S)
            end;
        {wedge_state_change, Boolean, {NewEpoch, _}=NewEpochId} ->
            OldEpoch = case OldEpochId of {OldE, _} -> OldE;
                                          undefined -> -1
                       end,
            if NewEpoch >= OldEpoch ->
                    true = ets:insert(S#state.etstab,
                                      {epoch, {Boolean, NewEpochId}}),
                    append_server_loop(FluPid, S#state{wedged=Boolean,
                                                       epoch_id=NewEpochId});
               true ->
                    append_server_loop(FluPid, S)
            end;
        {wedge_status, FromPid} ->
            #state{wedged=Wedged_p, epoch_id=EpochId} = S,
            FromPid ! {wedge_status_reply, Wedged_p, EpochId},
            append_server_loop(FluPid, S);
        {current_state, FromPid} ->
            FromPid ! S;
        Else ->
            io:format(user, "append_server_loop: WHA? ~p\n", [Else]),
            append_server_loop(FluPid, S)
    end.

append_server_dispatch(From, CoC_Namespace, CoC_Locator,
                       Prefix, Chunk, CSum, Extra, FluName, EpochId) ->
    Result = case handle_append(CoC_Namespace, CoC_Locator,
                                Prefix, Chunk, CSum, Extra, FluName, EpochId) of
        {ok, File, Offset} ->
            {assignment, Offset, File};
        Other ->
            Other
    end,
    From ! Result,
    exit(normal).

handle_append(_N, _L, _Prefix, <<>>, _Csum, _Extra, _FluName, _EpochId) ->
    {error, bad_arg};
handle_append(CoC_Namespace, CoC_Locator,
              Prefix, Chunk, Csum, Extra, FluName, EpochId) ->
    CoC = {coc, CoC_Namespace, CoC_Locator},
    Res = machi_flu_filename_mgr:find_or_make_filename_from_prefix(
            FluName, EpochId, {prefix, Prefix}, CoC),
    case Res of
        {file, F} ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, F}) of
                {ok, Pid} ->
                    {Tag, CS} = machi_util:unmake_tagged_csum(Csum),
                    Meta = [{client_csum_tag, Tag}, {client_csum, CS}],
                    machi_file_proxy:append(Pid, Meta, Extra, Chunk);
                {error, trimmed} = E ->
                    E
            end;
        Error ->
            Error
    end.

%% This is the name of the projection store that is spawned by the
%% *flu*, for use primarily in testing scenarios.  In normal use, we
%% ought to be using the OTP style of managing processes, via
%% supervisors, namely via machi_flu_psup.erl, which uses a
%% *different* naming convention for the projection store name that it
%% registers.

make_projection_server_regname(BaseName) ->
    list_to_atom(atom_to_list(BaseName) ++ "_pstore").


-ifdef(TEST).

%% Remove "_COMMENTED" string to run the demo/exploratory code.

timing_demo_test_COMMENTED_() ->
    {timeout, 300, fun() -> timing_demo_test2() end}.

%% Demo/exploratory hackery to check relative speeds of dealing with
%% checksum data in different ways.
%%
%% Summary:
%%
%% * Use compact binary encoding, with 1 byte header for entry length.
%%     * Because the hex-style code is *far* slower just for enc & dec ops.
%%     * For 1M entries of enc+dec: 0.215 sec vs. 15.5 sec.
%% * File sorter when sorting binaries as-is is only 30-40% slower
%%   than an in-memory split (of huge binary emulated by file:read_file()
%%   "big slurp") and sort of the same as-is sortable binaries.
%% * File sorter slows by a factor of about 2.5 if {order, fun compare/2}
%%   function must be used, i.e. because the checksum entry lengths differ.
%% * File sorter + {order, fun compare/2} is still *far* faster than external
%%   sort by OS X's sort(1) of sortable ASCII hex-style:
%%   4.5 sec vs. 21 sec.
%% * File sorter {order, fun compare/2} is faster than in-memory sort
%%   of order-friendly 3-tuple-style: 4.5 sec vs. 15 sec.

timing_demo_test2() ->
    Xs = [random:uniform(1 bsl 32) || _ <- lists:duplicate(1*1000*1000, $x)],
    CSum = <<"123456789abcdef0A">>,
    17 = byte_size(CSum),
    io:format(user, "\n", []),

    %% %% {ok, ZZZ} = file:open("/tmp/foo.hex-style", [write, binary, raw, delayed_write]),
    io:format(user, "Hex-style file entry enc+dec: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {HexUSec, _} =
    timer:tc(fun() ->
                     lists:foldl(fun(X, _) ->
                                         B = machi_checksums:encode_csum_file_entry_hex(X, 100, CSum),
                                         %% file:write(ZZZ, [B, 10]),
                                         machi_checksums:decode_csum_file_entry_hex(list_to_binary(B))
                                 end, x, Xs)
             end),
    io:format(user, "~.3f sec\n", [HexUSec / 1000000]),
    %% %% file:close(ZZZ),

    io:format(user, "Not-sortable file entry enc+dec: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotSortedUSec, _} =
    timer:tc(fun() ->
                     lists:foldl(fun(X, _) ->
                                         B = machi_checksums:encode_csum_file_entry(X, 100, CSum),
                                         machi_checksums:decode_csum_file_entry(list_to_binary(B))
                                 end, x, Xs)
             end),
    io:format(user, "~.3f sec\n", [NotSortedUSec / 1000000]),

    NotHexList = lists:foldl(fun(X, Acc) ->
                                     B = machi_checksums:encode_csum_file_entry(X, 100, CSum),
                                 [B|Acc]
                         end, [], Xs),
    NotHexBin = iolist_to_binary(NotHexList),

    io:format(user, "Split NotHexBin: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotHexBinUSec, SplitRes} =
    timer:tc(fun() ->
                     put(hack_length, 29),
                     put(hack_different, false),
                     {Sorted, _Leftover} = machi_checksums:split_checksum_list_blob(NotHexBin),
                     io:format(user, " Leftover ~p (hack_different ~p) ", [_Leftover, get(hack_different)]),
                     Sorted
             end),
    io:format(user, "~.3f sec\n", [NotHexBinUSec / 1000000]),

    io:format(user, "Sort Split results: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {SortSplitUSec, _} =
    timer:tc(fun() ->
                     lists:sort(SplitRes)
                     %% lists:sort(fun sort_2lines/2, SplitRes)
             end),
    io:format(user, "~.3f sec\n", [SortSplitUSec / 1000000]),

    UnsortedName = "/tmp/foo.unsorted",
    SortedName = "/tmp/foo.sorted",

    ok = file:write_file(UnsortedName, NotHexList),
    io:format(user, "File Sort Split results: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {FileSortUSec, _} =
        timer:tc(fun() ->
                         {ok, FHin} = file:open(UnsortedName, [read, binary]),
                         {ok, FHout} = file:open(SortedName,
                                                [write, binary, delayed_write]),
                         put(hack_sorter_sha_ctx, crypto:hash_init(sha)),
                         ok = file_sorter:sort(sort_input_fun(FHin, <<>>),
                                               sort_output_fun(FHout),
                                               [{format,binary},
                                                {header, 1}
                                                %% , {order, fun sort_2lines/2}
                                               ])
                 end),
    io:format(user, "~.3f sec\n", [FileSortUSec / 1000000]),
    _SHA = crypto:hash_final(get(hack_sorter_sha_ctx)),
    %% io:format(user, "SHA via (hack_sorter_sha_ctx) = ~p\n", [_SHA]),

    io:format(user, "NotHex-Not-sortable tuple list creation: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotHexTupleCreationUSec, NotHexTupleList} =
    timer:tc(fun() ->
                     lists:foldl(fun(X, Acc) ->
                                         B = encode_csum_file_entry_hex(
                                               X, 100, CSum),
                                         [B|Acc]
                                 end, [], Xs)
             end),
    io:format(user, "~.3f sec\n", [NotHexTupleCreationUSec / 1000000]),

    io:format(user, "NotHex-Not-sortable tuple list sort: ", []),
    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
    {NotHexTupleSortUSec, _} =
    timer:tc(fun() ->
                     lists:sort(NotHexTupleList)
             end),
    io:format(user, "~.3f sec\n", [NotHexTupleSortUSec / 1000000]),

    ok.

sort_2lines(<<_:1/binary, A/binary>>, <<_:1/binary, B/binary>>) ->
    A < B.

sort_input_fun(FH, PrevStuff) ->
    fun(close) ->
            ok;
       (read) ->
            case file:read(FH, 1024*1024) of
                {ok, NewStuff} ->
                    AllStuff = if PrevStuff == <<>> ->
                                       NewStuff;
                                  true ->
                                       <<PrevStuff/binary, NewStuff/binary>>
                               end,
                    {SplitRes, Leftover} = machi_checksums:split_checksum_list_blob(AllStuff),
                    {SplitRes, sort_input_fun(FH, Leftover)};
                eof ->
                    end_of_input
            end
    end.

sort_output_fun(FH) ->
    fun(close) ->
            file:close(FH);
       (Stuff) ->
            Ctx = get(hack_sorter_sha_ctx),
            put(hack_sorter_sha_ctx, crypto:hash_update(Ctx, Stuff)),
            ok = file:write(FH, Stuff),
            sort_output_fun(FH)
    end.

encode_csum_file_entry_hex(Offset, Size, TaggedCSum) ->
    OffsetHex = machi_util:bin_to_hexstr(<<Offset:64/big>>),
    SizeHex = machi_util:bin_to_hexstr(<<Size:32/big>>),
    CSumHex = machi_util:bin_to_hexstr(TaggedCSum),
    [OffsetHex, 32, SizeHex, 32, CSumHex].

decode_csum_file_entry_hex(<<OffsetHex:16/binary, _:1/binary, SizeHex:8/binary, _:1/binary, CSumHex/binary>>) ->
    Offset = machi_util:hexstr_to_bin(OffsetHex),
    Size = machi_util:hexstr_to_bin(SizeHex),
    CSum = machi_util:hexstr_to_bin(CSumHex),
    {Offset, Size, CSum}.

-endif. % TEST
