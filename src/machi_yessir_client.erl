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

%% @doc "Yes, sir!" style dummy/mock client facade.

-module(machi_yessir_client).

-ifdef(TODO_refactoring_deferred).

-include("machi.hrl").
-include("machi_projection.hrl").

-export([
         %% File API
         append_chunk/4, append_chunk/5,
         append_chunk_extra/5, append_chunk_extra/6,
         read_chunk/5, read_chunk/6,
         checksum_list/2, checksum_list/3,
         list_files/2, list_files/3,
         wedge_status/1, wedge_status/2,

         %% Projection API
         get_latest_epoch/2, get_latest_epoch/3,
         read_latest_projection/2, read_latest_projection/3,
         read_projection/3, read_projection/4,
         write_projection/3, write_projection/4,
         get_all_projections/2, get_all_projections/3,
         list_all_projections/2, list_all_projections/3,

         %% Common API
         quit/1,

         %% Connection management API
         connected_p/1, connect/1, disconnect/1
        ]).
%% For "internal" replication only.
-export([
         write_chunk/5, write_chunk/6,
         delete_migration/3, delete_migration/4,
         trunc_hack/3, trunc_hack/4
        ]).

-record(yessir, {
          name,
          start,
          start_bin,
          num_files,
          file_size,
          chunk_size
         }).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(Sock, EpochID, Prefix, Chunk) ->
    append_chunk_extra(Sock, EpochID, Prefix, Chunk, 0).

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix'.

append_chunk(_Host, _TcpPort, EpochID, Prefix, Chunk) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        append_chunk_extra(Sock, EpochID, Prefix, Chunk, 0)
    after
        disconnect(Sock)
    end.

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix' and also request an additional `Extra' bytes.
%%
%% For example, if the `Chunk' size is 1 KByte and `Extra' is 4K Bytes, then
%% the file offsets that follow `Chunk''s position for the following 4K will
%% be reserved by the file sequencer for later write(s) by the
%% `write_chunk()' API.

append_chunk_extra(#yessir{name=Name,start_bin=StartBin},
                   _EpochID, Prefix, Chunk, ChunkExtra)
  when is_integer(ChunkExtra), ChunkExtra >= 0 ->
    File = list_to_binary([Prefix, $/, StartBin]),
    Pos = case get({Name,offset,File}) of
              undefined -> ?MINIMUM_OFFSET;
              N         -> N
          end,
    put({Name,offset,File}, Pos + size(Chunk) + ChunkExtra),
    {ok, {Pos, iolist_size(Chunk), Prefix}}.

%% @doc Append a chunk (binary- or iolist-style) of data to a file
%% with `Prefix' and also request an additional `Extra' bytes.
%%
%% For example, if the `Chunk' size is 1 KByte and `Extra' is 4K Bytes, then
%% the file offsets that follow `Chunk''s position for the following 4K will
%% be reserved by the file sequencer for later write(s) by the
%% `write_chunk()' API.

append_chunk_extra(_Host, _TcpPort, EpochID, Prefix, Chunk, ChunkExtra)
  when is_integer(ChunkExtra), ChunkExtra >= 0 ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        append_chunk_extra(Sock, EpochID, Prefix, Chunk, ChunkExtra)
    after
        disconnect(Sock)
    end.

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(#yessir{name=Name}, _EpochID, File, Offset, Size)
  when Offset >= ?MINIMUM_OFFSET, Size >= 0 ->
    case get({Name,offset,File}) of
        undefined ->
            {error, no_such_file};
        MaxOffset ->
            if Offset > MaxOffset ->
                    {error, not_written};
               %% To be more accurate, we ought to include this clause.
               %% But checksum_list() is a bit dumb and can list a byte
               %% range that overlaps the end of the file.
               %% Offset + Size > MaxOffset ->
               %%      {error, partial_read};
               true ->
                    Chunk = make_chunk(Name, Size),
                    {ok, Chunk}
            end
    end.

make_chunk(Name, Size) ->
    case get({Name,chunk,Size}) of
        undefined ->
            Byte = Size rem 253,
            C = list_to_binary(
                  lists:duplicate(Size, Byte)),
            put({Name,chunk,Size}, C),
            C;
        C ->
            C
    end.    

make_csum(Name,Size) ->
    case get({Name,csum,Size}) of
        undefined ->
            C = crypto:hash(sha, make_chunk(Name, Size)),
            put({Name,csum,Size}, C),
            C;
        C ->
            C
    end.    

%% @doc Read a chunk of data of size `Size' from `File' at `Offset'.

read_chunk(_Host, _TcpPort, EpochID, File, Offset, Size)
  when Offset >= ?MINIMUM_OFFSET, Size >= 0 ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        read_chunk(Sock, EpochID, File, Offset, Size)
    after
        disconnect(Sock)
    end.

%% @doc Fetch the list of chunk checksums for `File'.

checksum_list(#yessir{name=Name,chunk_size=ChunkSize}, File) ->
    case get({Name,offset,File}) of
        undefined ->
            {error, no_such_file};
        MaxOffset ->
            C = machi_util:make_tagged_csum(client_sha,
                                            make_csum(Name, ChunkSize)),
            Cs = [{Offset, ChunkSize, C} ||
                     Offset <- lists:seq(?MINIMUM_OFFSET, MaxOffset, ChunkSize)],
            {ok, term_to_binary(Cs)}
    end.

%% @doc Fetch the list of chunk checksums for `File'.

checksum_list(_Host, _TcpPort, File) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        checksum_list(Sock, File)
    after
        disconnect(Sock)
    end.

%% @doc Fetch the list of all files on the remote FLU.

list_files(#yessir{name=Name}, _EpochID) ->
    Files = [{Offset, File} || {{N,offset,File}, Offset} <- get(), N == Name],
    {ok, Files}.

%% @doc Fetch the list of all files on the remote FLU.

list_files(_Host, _TcpPort, EpochID) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        list_files(Sock, EpochID)
    after
        disconnect(Sock)
    end.

%% @doc Fetch the wedge status from the remote FLU.

wedge_status(_Sock) ->
    {ok, {false, ?DUMMY_PV1_EPOCH}}.

%% @doc Fetch the wedge status from the remote FLU.

wedge_status(_Host, _TcpPort) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        wedge_status(Sock)
    after
        disconnect(Sock)
    end.

%% @doc Get the latest epoch number + checksum from the FLU's projection store.

get_latest_epoch(Sock, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    case read_latest_projection(Sock, ProjType) of
        {ok, P} ->
            {ok, {P#projection_v1.epoch_number, P#projection_v1.epoch_csum}}
    end.

%% @doc Get the latest epoch number + checksum from the FLU's projection store.

get_latest_epoch(_Host, _TcpPort, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        get_latest_epoch(Sock, ProjType)
    after
        disconnect(Sock)
    end.

%% @doc Get the latest projection from the FLU's projection store for `ProjType'

read_latest_projection(#yessir{name=Name}, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Ps = [P || {{N,proj,PT,_Epoch}, P} <- get(), N == Name, PT == ProjType],
    case (catch lists:last(lists:sort(Ps))) of
        P when is_record(P, projection_v1) ->
            {ok, P};
        _ ->
            {ok, #projection_v1{epoch_number=0,epoch_csum= <<"yo">>,
                                author_server=zzya,
                                all_members=[],creation_time=now(),upi=[],
                                repairing=[],down=[],dbg=[],dbg2=[],
                                members_dict=[]}}
    end.

%% @doc Get the latest projection from the FLU's projection store for `ProjType'

read_latest_projection(_Host, _TcpPort, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        read_latest_projection(Sock, ProjType)
    after
        disconnect(Sock)
    end.

%% @doc Read a projection `Proj' of type `ProjType'.

read_projection(#yessir{name=Name}, ProjType, Epoch)
  when ProjType == 'public' orelse ProjType == 'private' ->
    case get({Name,proj,ProjType,Epoch}) of
        undefined ->
            {error, not_written};
        P when is_record(P, projection_v1) ->
            {ok, P}
    end.

%% @doc Read a projection `Proj' of type `ProjType'.

read_projection(_Host, _TcpPort, ProjType, Epoch)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        read_projection(Sock, ProjType, Epoch)
    after
        disconnect(Sock)
    end.

%% @doc Write a projection `Proj' of type `ProjType'.

write_projection(#yessir{name=Name}=Sock, ProjType, Proj)
  when ProjType == 'public' orelse ProjType == 'private',
       is_record(Proj, projection_v1) ->
    Epoch = Proj#projection_v1.epoch_number,
    case read_projection(Sock, ProjType, Epoch) of
        {error, not_written} ->
            put({Name,proj,ProjType,Epoch}, Proj),
            ok;
        {ok, _} ->
            {error, written}
    end.

%% @doc Write a projection `Proj' of type `ProjType'.

write_projection(_Host, _TcpPort, ProjType, Proj)
  when ProjType == 'public' orelse ProjType == 'private',
       is_record(Proj, projection_v1) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        write_projection(Sock, ProjType, Proj)
    after
        disconnect(Sock)
    end.

%% @doc Get all projections from the FLU's projection store.

get_all_projections(#yessir{name=Name}, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Ps = [Proj || {{N,proj,PT,_}, Proj} <- get(), N == Name, PT == ProjType],
    {ok, lists:sort(Ps)}.

%% @doc Get all projections from the FLU's projection store.

get_all_projections(_Host, _TcpPort, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        get_all_projections(Sock, ProjType)
    after
        disconnect(Sock)
    end.

%% @doc Get all epoch numbers from the FLU's projection store.

list_all_projections(Sock, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    case get_all_projections(Sock, ProjType) of
        {ok, Ps} ->
            {ok, [P#projection_v1.epoch_number || P <- Ps]}
    end.

%% @doc Get all epoch numbers from the FLU's projection store.

list_all_projections(_Host, _TcpPort, ProjType)
  when ProjType == 'public' orelse ProjType == 'private' ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        list_all_projections(Sock, ProjType)
    after
        disconnect(Sock)
    end.

%% @doc Quit &amp; close the connection to remote FLU.

quit(_) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

write_chunk(#yessir{name=Name}, _EpochID, File, Offset, Chunk)
  when Offset >= ?MINIMUM_OFFSET ->
    Pos = case get({Name,offset,File}) of
              undefined -> Offset;
              N         -> erlang:max(N + size(Chunk), Offset)
          end,
    put({Name,offset,File}, Pos),
    ok.

%% @doc Restricted API: Write a chunk of already-sequenced data to
%% `File' at `Offset'.

write_chunk(_Host, _TcpPort, EpochID, File, Offset, Chunk)
  when Offset >= ?MINIMUM_OFFSET ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        write_chunk(Sock, EpochID, File, Offset, Chunk)
    after
        disconnect(Sock)
    end.

%% @doc Restricted API: Delete a file after it has been successfully
%% migrated.

delete_migration(#yessir{name=Name}, _EpochID, File) ->
    case get({Name,offset,File}) of
        undefined ->
            {error, no_such_file};
        _N ->
            erase({name,offset,File}),
            ok
    end.

%% @doc Restricted API: Delete a file after it has been successfully
%% migrated.

delete_migration(_Host, _TcpPort, EpochID, File) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        delete_migration(Sock, EpochID, File)
    after
        disconnect(Sock)
    end.

%% @doc Restricted API: Truncate a file after it has been successfully
%% erasure coded.

trunc_hack(#yessir{name=Name}, _EpochID, File) ->
    put({Name,offset,File}, ?MINIMUM_OFFSET).

%% @doc Restricted API: Truncate a file after it has been successfully
%% erasure coded.

trunc_hack(_Host, _TcpPort, EpochID, File) ->
    Sock = connect(#p_srvr{proto_mod=?MODULE}),
    try
        trunc_hack(Sock, EpochID, File)
    after
        disconnect(Sock)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

connected_p(_) ->
    true.

connect(#p_srvr{name=Name, props=Props})->
    Now = os:timestamp(),
    StartBin = proplists:get_value(file_suffix, Props,
                                    list_to_binary(io_lib:format("~w", [Now]))),
    NumFiles=proplists:get_value(num_files, Props, 12),
    FileSize=proplists:get_value(file_size, Props, 50*1024*1024),
    ChunkSize=proplists:get_value(chunk_size, Props, 64*1024),
    Sock = #yessir{name=Name,
                   start=Now,
                   start_bin=StartBin,
                   num_files=NumFiles,
                   file_size=FileSize,
                   chunk_size=ChunkSize
                  },
    %% Add fake dict entries for these files
    _ = [begin
             Prefix = list_to_binary(io_lib:format("fake~w", [X])),
             {ok, _} = append_chunk_extra(Sock, {1,<<"unused">>}, Prefix, <<>>, FileSize)
         end || X <- lists:seq(1, NumFiles)],

    Sock.

disconnect(#yessir{name=Name}) ->
    _ = [erase(K) || {{N,offset,_}=K, _V} <- get(), N == Name],
    _ = [erase(K) || {{N,chunk,_}=K, _V} <- get(), N == Name],
    _ = [erase(K) || {{N,csum,_}=K, _V} <- get(), N == Name],
    _ = [erase(K) || {{N,proj,_,_}=K, _V} <- get(), N == Name],
    ok.

%% Example use:

%% application:ensure_all_started(machi).
%% machi_flu_psup:start_flu_package(a, 4444, "./data.a", []).
%% D = [{a,{p_srvr,a,machi_yessir_client,x,x,
%%          [{file_suffix,<<"yo">>},{num_files,7}]}},
%%      {b,{p_srvr,b,machi_yessir_client,x,x,
%%          [{file_suffix,<<"yo">>},{num_files,20}]}}].
%% machi_chain_manager1:set_chain_members(a_chmgr, D).


%% =INFO REPORT==== 17-May-2015::18:57:47 ===
%% Chain manager a found latest public projection 0 has author zzya not a member of our members list [a,b].  Please check chain membership on this rogue chain manager zzya.
%% ok
%% 5> 
%% =INFO REPORT==== 17-May-2015::18:57:51 ===
%% Repair start: tail a of [a] -> [b], ap_mode ID {a,{1431,856671,140404}}
%% MissingFileSummary [{<<"fake1/yo">>,{52429824,[]}},
%%                     {<<"fake10/yo">>,{52429824,[a]}},
%%                     {<<"fake11/yo">>,{52429824,[a]}},
%%                     {<<"fake12/yo">>,{52429824,[a]}},
%%                     {<<"fake13/yo">>,{52429824,[a]}},
%%                     {<<"fake14/yo">>,{52429824,[a]}},
%%                     {<<"fake15/yo">>,{52429824,[a]}},
%%                     {<<"fake16/yo">>,{52429824,[a]}},
%%                     {<<"fake17/yo">>,{52429824,[a]}},
%%                     {<<"fake18/yo">>,{52429824,[a]}},
%%                     {<<"fake19/yo">>,{52429824,[a]}},
%%                     {<<"fake2/yo">>,{52429824,[]}},
%%                     {<<"fake20/yo">>,{52429824,[a]}},
%%                     {<<"fake3/yo">>,{52429824,[]}},
%%                     {<<"fake4/yo">>,{52429824,[]}},
%%                     {<<"fake5/yo">>,{52429824,[]}},
%%                     {<<"fake6/yo">>,{52429824,[]}},
%%                     {<<"fake7/yo">>,{52429824,[]}},
%%                     {<<"fake8/yo">>,{52429824,[a]}},
%%                     {<<"fake9/yo">>,{52429824,[a]}}]
%% Make repair directives: .................... done
%% Out-of-sync data for FLU a: 650.8 MBytes
%% Out-of-sync data for FLU b: 0.0 MBytes
%% Execute repair directives: ..................................................................................................................... done

%% =INFO REPORT==== 17-May-2015::18:57:52 ===
%% Repair success: tail a of [a] finished ap_mode repair ID {a,{1431,856671,140404}}: ok
%% Stats [{t_in_files,0},{t_in_chunks,10413},{t_in_bytes,682426368},{t_out_files,0},{t_out_chunks,10413},{t_out_bytes,682426368},{t_bad_chunks,0},{t_elapsed_seconds,1.591}]

-endif. % TODO_refactoring_deferred
