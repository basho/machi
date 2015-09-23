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
%% The protocol implemented here is an artisanal, hand-crafted, silly
%% thing that was very quick to put together for a "demo day" proof of
%% concept.  It will almost certainly be replaced with something else,
%% both in terms of wire format and better code separation of
%% serialization/deserialization vs. network transport management,
%% etc.
%%
%% For the moment, this module implements a rudimentary TCP-based
%% protocol as the sole supported access method to the server,
%% sequencer, and projection store.  Conceptually, those three
%% services are independent and ought to have their own protocols.  As
%% a practical matter, there is no need for wire protocol
%% compatibility.  Furthermore, from the perspective of failure
%% detection, it is very convenient that all three FLU-related
%% services are accessed using the same single TCP port.
%%
%% === TODO items ===
%%
%% TODO There is a major missing feature in this FLU implementation:
%% there is no "write-once" enforcement for any position in a Machi
%% file.  At the moment, we rely on correct behavior of the client
%% &amp; the sequencer to avoid overwriting data.  In the Real World,
%% however, all Machi file data is supposed to be exactly write-once
%% to avoid problems with bugs, wire protocol corruption, malicious
%% clients, etc.
%%
%% TODO The per-file metadata tuple store is missing from this implementation.
%%
%% TODO Section 4.1 ("The FLU") of the Machi design doc suggests that
%% the FLU keep track of the epoch number of the last file write (and
%% perhaps last metadata write), as an optimization for inter-FLU data
%% replication/chain repair.

-module(machi_flu1).
%
%-include_lib("kernel/include/file.hrl").
%
%-include("machi.hrl").
%-include("machi_pb.hrl").
%-include("machi_projection.hrl").
%-define(V(X,Y), ok).
%%% -include("machi_verbose.hrl").
%
%-ifdef(TEST).
%-include_lib("eunit/include/eunit.hrl").
%-endif. % TEST
%
%
%-export([start_link/1, stop/1,
%         update_wedge_state/3, wedge_myself/2]).
%-export([make_listener_regname/1, make_projection_server_regname/1]).
%-export([encode_csum_file_entry/3, encode_csum_file_entry_bin/3,
%         decode_csum_file_entry/1,
%         split_checksum_list_blob/1, split_checksum_list_blob_decode/1]).
%
%-record(state, {
%          flu_name        :: atom(),
%          proj_store      :: pid(),
%          append_pid      :: pid(),
%          tcp_port        :: non_neg_integer(),
%          data_dir        :: string(),
%          wedged = true   :: boolean(),
%          etstab          :: ets:tid(),
%          epoch_id        :: 'undefined' | machi_dt:epoch_id(),
%          pb_mode = undefined  :: 'undefined' | 'high' | 'low',
%          high_clnt       :: 'undefined' | pid(),
%          dbg_props = []  :: list(), % proplist
%          props = []      :: list()  % proplist
%         }).
%
%start_link([{FluName, TcpPort, DataDir}|Rest])
%  when is_atom(FluName), is_integer(TcpPort), is_list(DataDir) ->
%    {ok, spawn_link(fun() -> main2(FluName, TcpPort, DataDir, Rest) end)}.
%
%stop(Pid) ->
%    case erlang:is_process_alive(Pid) of
%        true ->
%            Pid ! killme,
%            ok;
%        false ->
%            error
%    end.
%
%update_wedge_state(PidSpec, Boolean, EpochId)
%  when (Boolean == true orelse Boolean == false), is_tuple(EpochId) ->
%    PidSpec ! {wedge_state_change, Boolean, EpochId}.
%
%wedge_myself(PidSpec, EpochId)
%  when is_tuple(EpochId) ->
%    PidSpec ! {wedge_myself, EpochId}.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%ets_table_name(FluName) when is_atom(FluName) ->
%    list_to_atom(atom_to_list(FluName) ++ "_epoch").
%%% ets_table_name(FluName) when is_binary(FluName) ->
%%%     list_to_atom(binary_to_list(FluName) ++ "_epoch").
%
%main2(FluName, TcpPort, DataDir, Rest) ->
%    {Props, DbgProps} =  case proplists:get_value(dbg, Rest) of
%                             undefined ->
%                                 {Rest, []};
%                             DPs ->
%                                 {lists:keydelete(dbg, 1, Rest), DPs}
%                         end,
%    {SendAppendPidToProj_p, ProjectionPid} =
%        case proplists:get_value(projection_store_registered_name, Rest) of
%            undefined ->
%                RN = make_projection_server_regname(FluName),
%                {ok, PP} =
%                    machi_projection_store:start_link(RN, DataDir, undefined),
%                {true, PP};
%            RN ->
%                {false, whereis(RN)}
%        end,
%    InitialWedged_p = proplists:get_value(initial_wedged, DbgProps),
%    ProjRes = machi_projection_store:read_latest_projection(ProjectionPid,
%                                                            private),
%    {Wedged_p, EpochId} =
%        if InitialWedged_p == undefined,
%           is_tuple(ProjRes), element(1, ProjRes) == ok ->
%                {ok, Proj} = ProjRes,
%                {false, {Proj#projection_v1.epoch_number,
%                         Proj#projection_v1.epoch_csum}};
%           InitialWedged_p == false ->
%                {false, ?DUMMY_PV1_EPOCH};
%           true ->
%                {true, undefined}
%        end,
%    S0 = #state{flu_name=FluName,
%                proj_store=ProjectionPid,
%                tcp_port=TcpPort,
%                data_dir=DataDir,
%                wedged=Wedged_p,
%                etstab=ets_table_name(FluName),
%                epoch_id=EpochId,
%                dbg_props=DbgProps,
%                props=Props},
%    AppendPid = start_append_server(S0, self()),
%    receive
%        append_server_ack -> ok
%    end,
%    if SendAppendPidToProj_p ->
%            machi_projection_store:set_wedge_notify_pid(ProjectionPid,
%                                                        AppendPid);
%       true ->
%            ok
%    end,
%    S1 = S0#state{append_pid=AppendPid},
%    ListenPid = start_listen_server(S1),
%
%    Config_e = machi_util:make_config_filename(DataDir, "unused"),
%    ok = filelib:ensure_dir(Config_e),
%    {_, Data_e} = machi_util:make_data_filename(DataDir, "unused"),
%    ok = filelib:ensure_dir(Data_e),
%    Projection_e = machi_util:make_projection_filename(DataDir, "unused"),
%    ok = filelib:ensure_dir(Projection_e),
%
%    put(flu_flu_name, FluName),
%    put(flu_append_pid, AppendPid),
%    put(flu_projection_pid, ProjectionPid),
%    put(flu_listen_pid, ListenPid),
%    receive killme -> ok end,
%    (catch exit(AppendPid, kill)),
%    (catch exit(ProjectionPid, kill)),
%    (catch exit(ListenPid, kill)),
%    ok.
%
%
%
%
%
%do_server_proj_request({get_latest_epochid, ProjType},
%                       #state{proj_store=ProjStore}) ->
%    machi_projection_store:get_latest_epochid(ProjStore, ProjType);
%do_server_proj_request({read_latest_projection, ProjType},
%                       #state{proj_store=ProjStore}) ->
%    machi_projection_store:read_latest_projection(ProjStore, ProjType);
%do_server_proj_request({read_projection, ProjType, Epoch},
%                       #state{proj_store=ProjStore}) ->
%    machi_projection_store:read(ProjStore, ProjType, Epoch);
%do_server_proj_request({write_projection, ProjType, Proj},
%                       #state{proj_store=ProjStore}) ->
%    machi_projection_store:write(ProjStore, ProjType, Proj);
%do_server_proj_request({get_all_projections, ProjType},
%                       #state{proj_store=ProjStore}) ->
%    machi_projection_store:get_all_projections(ProjStore, ProjType);
%do_server_proj_request({list_all_projections, ProjType},
%                       #state{proj_store=ProjStore}) ->
%    machi_projection_store:list_all_projections(ProjStore, ProjType);
%do_server_proj_request({kick_projection_reaction},
%                       #state{flu_name=FluName}) ->
%    %% Tell my chain manager that it might want to react to
%    %% this new world.
%    Chmgr = machi_chain_manager1:make_chmgr_regname(FluName),
%    spawn(fun() ->
%                  catch machi_chain_manager1:trigger_react_to_env(Chmgr)
%          end),
%    async_no_response.
%
%start_seq_append_server(Prefix, EpochID, DataDir, AppendServerPid) ->
%    proc_lib:spawn_link(fun() ->
%                                %% The following is only necessary to
%                                %% make nice process relationships in
%                                %% 'appmon' and related tools.
%                                put('$ancestors', [AppendServerPid]),
%                                put('$initial_call', {x,y,3}),
%                                link(AppendServerPid),
%                                run_seq_append_server(Prefix, EpochID, DataDir)
%                        end).
%
%run_seq_append_server(Prefix, EpochID, DataDir) ->
%    true = register(machi_util:make_regname(Prefix), self()),
%    run_seq_append_server2(Prefix, EpochID, DataDir).
%
%run_seq_append_server2(Prefix, EpochID, DataDir) ->
%    FileNum = machi_util:read_max_filenum(DataDir, Prefix) + 1,
%    case machi_util:increment_max_filenum(DataDir, Prefix) of
%        ok ->
%            machi_util:info_msg("start: ~p server at file ~w\n",
%                                [Prefix, FileNum]),
%            seq_append_server_loop(DataDir, Prefix, EpochID, FileNum);
%        Else ->
%            error_logger:error_msg("start: ~p server at file ~w: ~p\n",
%                                   [Prefix, FileNum, Else]),
%            exit(Else)
%
%    end.
%
%-spec seq_name_hack() -> string().
%seq_name_hack() ->
%    lists:flatten(io_lib:format("~.36B~.36B",
%                                [element(3,now()),
%                                 list_to_integer(os:getpid())])).
%
%seq_append_server_loop(DataDir, Prefix, EpochID, FileNum) ->
%    SequencerNameHack = seq_name_hack(),
%    {File, FullPath} = machi_util:make_data_filename(
%                         DataDir, Prefix, SequencerNameHack, FileNum),
%    {ok, FHd} = file:open(FullPath,
%                          [read, write, raw, binary]),
%    CSumPath = machi_util:make_checksum_filename(
%                 DataDir, Prefix, SequencerNameHack, FileNum),
%    {ok, FHc} = file:open(CSumPath, [append, raw, binary]),
%    seq_append_server_loop(DataDir, Prefix, File, {FHd,FHc}, EpochID, FileNum,
%                           ?MINIMUM_OFFSET).
%
%seq_append_server_loop(DataDir, Prefix, _File, {FHd,FHc}, EpochID,
%                       FileNum, Offset)
%  when Offset > ?MAX_FILE_SIZE ->
%    ok = file:close(FHd),
%    ok = file:close(FHc),
%    machi_util:info_msg("rollover: ~p server at file ~w offset ~w\n",
%                        [Prefix, FileNum, Offset]),
%    run_seq_append_server2(Prefix, EpochID, DataDir);    
%seq_append_server_loop(DataDir, Prefix, File, {FHd,FHc}=FH_, EpochID,
%                       FileNum, Offset) ->
%    receive
%        {seq_append, From, Prefix, Chunk, TaggedCSum, Extra, R_EpochID}
%          when R_EpochID == EpochID ->
%            if Chunk /= <<>> ->
%                 %% Do we want better error handling here than just a bad match crash?
%                 %% Does the error tuple need to propagate to somewhere?
%                    ok = try_write_position(FHd, Offset, Chunk);
%               true ->
%                    ok
%            end,
%            From ! {assignment, Offset, File},
%            Size = iolist_size(Chunk),
%            CSum_info = encode_csum_file_entry(Offset, Size, TaggedCSum),
%            ok = file:write(FHc, CSum_info),
%            seq_append_server_loop(DataDir, Prefix, File, FH_, EpochID,
%                                   FileNum, Offset + Size + Extra);
%        {seq_append, _From, _Prefix, _Chunk, _TCSum, _Extra, R_EpochID}=MSG ->
%            %% Rare'ish event: send MSG to myself so it doesn't get lost
%            %% while we recurse around to pick up a new FileNum.
%            self() ! MSG,
%            machi_util:info_msg("rollover: ~p server at file ~w offset ~w "
%                                "by new epoch_id ~W\n",
%                                [Prefix, FileNum, Offset, R_EpochID, 8]),
%            run_seq_append_server2(Prefix, R_EpochID, DataDir);    
%        {sync_stuff, FromPid, Ref} ->
%            file:sync(FHc),
%            FromPid ! {sync_finished, Ref},
%            seq_append_server_loop(DataDir, Prefix, File, FH_, EpochID,
%                                   FileNum, Offset)
%    after 30*1000 ->
%            ok = file:close(FHd),
%            ok = file:close(FHc),
%            machi_util:info_msg("stop: ~p server ~p at file ~w offset ~w\n",
%                                [Prefix, self(), FileNum, Offset]),
%            exit(normal)
%    end.
%
%try_write_position(FHd, Offset, Chunk) ->
%    ok = case file:pread(FHd, Offset, 1) of  %% one byte should be enough right?
%        eof ->
%            ok;
%        {ok, _} ->
%             {error, error_written};
%        {error, Reason} ->
%             {error, Reason}
%    end,
%    ok = file:pwrite(FHd, Offset, Chunk),
%    ok.
%
%make_listener_regname(BaseName) ->
%    list_to_atom(atom_to_list(BaseName) ++ "_listener").
%
%start_append_server(_,_) -> ok.
%start_listen_server(_,_) -> ok.
%
%%% This is the name of the projection store that is spawned by the
%%% *flu*, for use primarily in testing scenarios.  In normal use, we
%%% ought to be using the OTP style of managing processes, via
%%% supervisors, namely via machi_flu_psup.erl, which uses a
%%% *different* naming convention for the projection store name that it
%%% registers.
%
%make_projection_server_regname(BaseName) ->
%    list_to_atom(atom_to_list(BaseName) ++ "_pstore2").
%
%
%%% @doc Encode `Offset + Size + TaggedCSum' into an `iolist()' type for
%%% internal storage by the FLU.
%
%-spec encode_csum_file_entry(
%        machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()) ->
%        iolist().
%encode_csum_file_entry(Offset, Size, TaggedCSum) ->
%    Len = 8 + 4 + byte_size(TaggedCSum),
%    [<<Len:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big>>,
%     TaggedCSum].
%
%%% @doc Encode `Offset + Size + TaggedCSum' into an `binary()' type for
%%% internal storage by the FLU.
%
%-spec encode_csum_file_entry_bin(
%        machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()) ->
%        binary().
%encode_csum_file_entry_bin(Offset, Size, TaggedCSum) ->
%    Len = 8 + 4 + byte_size(TaggedCSum),
%    <<Len:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big,
%      TaggedCSum/binary>>.
%
%%% @doc Decode a single `binary()' blob into an
%%%      `{Offset,Size,TaggedCSum}' tuple.
%%%
%%% The internal encoding (which is currently exposed to the outside world
%%% via this function and related ones) is:
%%%
%%% <ul>
%%% <li> 1 byte: record length
%%% </li>
%%% <li> 8 bytes (unsigned big-endian): byte offset
%%% </li>
%%% <li> 4 bytes (unsigned big-endian): chunk size
%%% </li>
%%% <li> all remaining bytes: tagged checksum (1st byte = type tag)
%%% </li>
%%% </ul>
%%%
%%% See `machi.hrl' for the tagged checksum types, e.g.,
%%% `?CSUM_TAG_NONE'.
%
%-spec decode_csum_file_entry(binary()) ->
%        error |
%        {machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()}.
%decode_csum_file_entry(<<_:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big, TaggedCSum/binary>>) ->
%    {Offset, Size, TaggedCSum};
%decode_csum_file_entry(_Else) ->
%    error.
%
%%% @doc Split a `binary()' blob of `checksum_list' data into a list of
%%% unparsed `binary()' blobs, one per entry.
%%%
%%% Decode the unparsed blobs with {@link decode_csum_file_entry/1}, if
%%% desired.
%%%
%%% The return value `TrailingJunk' is unparseable bytes at the end of
%%% the checksum list blob.
%
%-spec split_checksum_list_blob(binary()) ->
%          {list(binary()), TrailingJunk::binary()}.
%split_checksum_list_blob(Bin) ->
%    split_checksum_list_blob(Bin, []).
%
%split_checksum_list_blob(<<Len:8/unsigned-big, Part:Len/binary, Rest/binary>>, Acc)->
%    case get(hack_length) of
%        Len -> ok;
%        _   -> put(hack_different, true)
%    end,
%    split_checksum_list_blob(Rest, [<<Len:8/unsigned-big, Part/binary>>|Acc]);
%split_checksum_list_blob(Rest, Acc) ->
%    {lists:reverse(Acc), Rest}.
%
%%% @doc Split a `binary()' blob of `checksum_list' data into a list of
%%% `{Offset,Size,TaggedCSum}' tuples.
%
%-spec split_checksum_list_blob_decode(binary()) ->
%  {list({machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()}),
%   TrailingJunk::binary()}.
%split_checksum_list_blob_decode(Bin) ->
%    split_checksum_list_blob_decode(Bin, []).
%
%split_checksum_list_blob_decode(<<Len:8/unsigned-big, Part:Len/binary, Rest/binary>>, Acc)->
%    One = <<Len:8/unsigned-big, Part/binary>>,
%    case decode_csum_file_entry(One) of
%        error ->
%            split_checksum_list_blob_decode(Rest, Acc);
%        DecOne ->
%            split_checksum_list_blob_decode(Rest, [DecOne|Acc])
%    end;
%split_checksum_list_blob_decode(Rest, Acc) ->
%    {lists:reverse(Acc), Rest}.
%
%check_or_make_tagged_checksum(?CSUM_TAG_NONE, _Client_CSum, Chunk) ->
%    %% TODO: If the client was foolish enough to use
%    %% this type of non-checksum, then the client gets
%    %% what it deserves wrt data integrity, alas.  In
%    %% the client-side Chain Replication method, each
%    %% server will calculated this independently, which
%    %% isn't exactly what ought to happen for best data
%    %% integrity checking.  In server-side CR, the csum
%    %% should be calculated by the head and passed down
%    %% the chain together with the value.
%    CS = machi_util:checksum_chunk(Chunk),
%    machi_util:make_tagged_csum(server_sha, CS);
%check_or_make_tagged_checksum(?CSUM_TAG_CLIENT_SHA, Client_CSum, Chunk) ->
%    CS = machi_util:checksum_chunk(Chunk),
%    if CS == Client_CSum ->
%            machi_util:make_tagged_csum(server_sha,
%                                        Client_CSum);
%       true ->
%            throw({bad_csum, CS})
%    end.
%
%-ifdef(TEST).
%
%%% Remove "_COMMENTED" string to run the demo/exploratory code.
%
%timing_demo_test_COMMENTED_() ->
%    {timeout, 300, fun() -> timing_demo_test2() end}.
%
%%% Demo/exploratory hackery to check relative speeds of dealing with
%%% checksum data in different ways.
%%%
%%% Summary:
%%%
%%% * Use compact binary encoding, with 1 byte header for entry length.
%%%     * Because the hex-style code is *far* slower just for enc & dec ops.
%%%     * For 1M entries of enc+dec: 0.215 sec vs. 15.5 sec.
%%% * File sorter when sorting binaries as-is is only 30-40% slower
%%%   than an in-memory split (of huge binary emulated by file:read_file()
%%%   "big slurp") and sort of the same as-is sortable binaries.
%%% * File sorter slows by a factor of about 2.5 if {order, fun compare/2}
%%%   function must be used, i.e. because the checksum entry lengths differ.
%%% * File sorter + {order, fun compare/2} is still *far* faster than external
%%%   sort by OS X's sort(1) of sortable ASCII hex-style:
%%%   4.5 sec vs. 21 sec.
%%% * File sorter {order, fun compare/2} is faster than in-memory sort
%%%   of order-friendly 3-tuple-style: 4.5 sec vs. 15 sec.
%
%timing_demo_test2() ->
%    Xs = [random:uniform(1 bsl 32) || _ <- lists:duplicate(1*1000*1000, $x)],
%    CSum = <<"123456789abcdef0A">>,
%    17 = byte_size(CSum),
%    io:format(user, "\n", []),
%
%    %% %% {ok, ZZZ} = file:open("/tmp/foo.hex-style", [write, binary, raw, delayed_write]),
%    io:format(user, "Hex-style file entry enc+dec: ", []),
%    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
%    {HexUSec, _} =
%    timer:tc(fun() ->
%                     lists:foldl(fun(X, _) ->
%                                         B = encode_csum_file_entry_hex(X, 100, CSum),
%                                         %% file:write(ZZZ, [B, 10]),
%                                         decode_csum_file_entry_hex(list_to_binary(B))
%                                 end, x, Xs)
%             end),
%    io:format(user, "~.3f sec\n", [HexUSec / 1000000]),
%    %% %% file:close(ZZZ),
%
%    io:format(user, "Not-sortable file entry enc+dec: ", []),
%    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
%    {NotSortedUSec, _} =
%    timer:tc(fun() ->
%                     lists:foldl(fun(X, _) ->
%                                         B = encode_csum_file_entry(X, 100, CSum),
%                                         decode_csum_file_entry(list_to_binary(B))
%                                 end, x, Xs)
%             end),
%    io:format(user, "~.3f sec\n", [NotSortedUSec / 1000000]),
%
%    NotHexList = lists:foldl(fun(X, Acc) ->
%                                 B = encode_csum_file_entry(X, 100, CSum),
%                                 [B|Acc]
%                         end, [], Xs),
%    NotHexBin = iolist_to_binary(NotHexList),
%
%    io:format(user, "Split NotHexBin: ", []),
%    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
%    {NotHexBinUSec, SplitRes} =
%    timer:tc(fun() ->
%                     put(hack_length, 29),
%                     put(hack_different, false),
%                     {Sorted, _Leftover} = split_checksum_list_blob(NotHexBin),
%                     io:format(user, " Leftover ~p (hack_different ~p) ", [_Leftover, get(hack_different)]),
%                     Sorted
%             end),
%    io:format(user, "~.3f sec\n", [NotHexBinUSec / 1000000]),
%
%    io:format(user, "Sort Split results: ", []),
%    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
%    {SortSplitUSec, _} =
%    timer:tc(fun() ->
%                     lists:sort(SplitRes)
%                     %% lists:sort(fun sort_2lines/2, SplitRes)
%             end),
%    io:format(user, "~.3f sec\n", [SortSplitUSec / 1000000]),
%
%    UnsortedName = "/tmp/foo.unsorted",
%    SortedName = "/tmp/foo.sorted",
%
%    ok = file:write_file(UnsortedName, NotHexList),
%    io:format(user, "File Sort Split results: ", []),
%    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
%    {FileSortUSec, _} =
%        timer:tc(fun() ->
%                         {ok, FHin} = file:open(UnsortedName, [read, binary]),
%                         {ok, FHout} = file:open(SortedName,
%                                                [write, binary, delayed_write]),
%                         put(hack_sorter_sha_ctx, crypto:hash_init(sha)),
%                         ok = file_sorter:sort(sort_input_fun(FHin, <<>>),
%                                               sort_output_fun(FHout),
%                                               [{format,binary},
%                                                {header, 1}
%                                                %% , {order, fun sort_2lines/2}
%                                               ])
%                 end),
%    io:format(user, "~.3f sec\n", [FileSortUSec / 1000000]),
%    _SHA = crypto:hash_final(get(hack_sorter_sha_ctx)),
%    %% io:format(user, "SHA via (hack_sorter_sha_ctx) = ~p\n", [_SHA]),
%
%    io:format(user, "NotHex-Not-sortable tuple list creation: ", []),
%    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
%    {NotHexTupleCreationUSec, NotHexTupleList} =
%    timer:tc(fun() ->
%                     lists:foldl(fun(X, Acc) ->
%                                         B = encode_csum_file_entry_hex(
%                                               X, 100, CSum),
%                                         [B|Acc]
%                                 end, [], Xs)
%             end),
%    io:format(user, "~.3f sec\n", [NotHexTupleCreationUSec / 1000000]),
%
%    io:format(user, "NotHex-Not-sortable tuple list sort: ", []),
%    [erlang:garbage_collect(self()) || _ <- lists:seq(1, 4)],
%    {NotHexTupleSortUSec, _} =
%    timer:tc(fun() ->
%                     lists:sort(NotHexTupleList)
%             end),
%    io:format(user, "~.3f sec\n", [NotHexTupleSortUSec / 1000000]),
%
%    ok.
%
%sort_2lines(<<_:1/binary, A/binary>>, <<_:1/binary, B/binary>>) ->
%    A < B.
%
%sort_input_fun(FH, PrevStuff) ->
%    fun(close) ->
%            ok;
%       (read) ->
%            case file:read(FH, 1024*1024) of
%                {ok, NewStuff} ->
%                    AllStuff = if PrevStuff == <<>> ->
%                                       NewStuff;
%                                  true ->
%                                       <<PrevStuff/binary, NewStuff/binary>>
%                               end,
%                    {SplitRes, Leftover} = split_checksum_list_blob(AllStuff),
%                    {SplitRes, sort_input_fun(FH, Leftover)};
%                eof ->
%                    end_of_input
%            end
%    end.
%
%sort_output_fun(FH) ->
%    fun(close) ->
%            file:close(FH);
%       (Stuff) ->
%            Ctx = get(hack_sorter_sha_ctx),
%            put(hack_sorter_sha_ctx, crypto:hash_update(Ctx, Stuff)),
%            ok = file:write(FH, Stuff),
%            sort_output_fun(FH)
%    end.
%
%encode_csum_file_entry_hex(Offset, Size, TaggedCSum) ->
%    OffsetHex = machi_util:bin_to_hexstr(<<Offset:64/big>>),
%    SizeHex = machi_util:bin_to_hexstr(<<Size:32/big>>),
%    CSumHex = machi_util:bin_to_hexstr(TaggedCSum),
%    [OffsetHex, 32, SizeHex, 32, CSumHex].
%
%decode_csum_file_entry_hex(<<OffsetHex:16/binary, _:1/binary, SizeHex:8/binary, _:1/binary, CSumHex/binary>>) ->
%    Offset = machi_util:hexstr_to_bin(OffsetHex),
%    Size = machi_util:hexstr_to_bin(SizeHex),
%    CSum = machi_util:hexstr_to_bin(CSumHex),
%    {Offset, Size, CSum}.
%
%-endif. % TEST
