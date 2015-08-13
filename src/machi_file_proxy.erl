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

%% @doc This is a proxy process which mediates access to Machi FLU
%% controlled files.  In particular, it manages the "write-once register"
%% conceit at the heart of Machi's design.
%%
%% Read, write and append requests for a single file will be managed 
%% through this proxy.  Clients can also request syncs for specific
%% types of filehandles.
%%
%% As operations are requested, the proxy keeps track of how many
%% operations it has performed (and how many errors were generated.)
%% After a sufficient number of inactivity, the server terminates 
%% itself.
%%
%% TODO:
%% 1. Some way to transition the proxy into/out of a wedged state that 
%% doesn't rely on message delivery.

-module(machi_file_proxy).
-behaviour(gen_server).

-include("machi.hrl").

%% public API
-export([
    start_link/2,
    sync/1,
    sync/2,
    read/3,
    write/3,
    write/4,
    append/2,
    append/4
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(TICK, 5*1000).
-define(TICK_THRESHOLD, 5). %% After this + 1 more quiescent ticks, shutdown
-define(TIMEOUT, 10*1000).
-define(TOO_MANY_ERRORS_RATIO, 50).

-type op_stats() :: { Total :: non_neg_integer(), Errors :: non_neg_integer() }.
    
-record(state, {
    data_dir :: string() | undefined,
    filename :: string() | undefined,
    data_path :: string() | undefined,
    wedged = false :: boolean(), 
    csum_file :: string()|undefined,
    csum_path :: string()|undefined,
    last_write_offset = 0 :: non_neg_integer(),
    data_filehandle :: file:filehandle(),
    csum_filehandle :: file:filehandle(),
    tref :: reference(), %% timer ref
    ticks = 0 :: non_neg_integer(), %% ticks elapsed with no new operations
    ops = 0 :: non_neg_integer(), %% sum of all ops
    reads = {0, 0} :: op_stats(),
    writes = {0, 0} :: op_stats(),
    appends = {0, 0} :: op_stats()
}).

%% Public API

start_link(Filename, DataDir) ->
    gen_server:start_link(?MODULE, {Filename, DataDir}, []).

% @doc Force a sync of all filehandles
-spec sync(Pid :: pid()) -> ok|{error, term()}.
sync(Pid) ->
    sync(Pid, all).

% @doc Force a sync of a specific filehandle type. Valid types are `all', `csum' and `data'.
-spec sync(Pid :: pid(), Type :: all|data|csum) -> ok|{error, term()}.
sync(Pid, Type) ->
    gen_server:call(Pid, {sync, Type}, ?TIMEOUT).

% @doc Read file at offset for length
-spec read(Pid :: pid(), Offset :: non_neg_integer(), 
           Length :: non_neg_integer()) -> {ok, Data :: binary(), Checksum :: binary()}|{error, term()}.
read(Pid, Offset, Length) ->
    gen_server:call(Pid, {read, Offset, Length}, ?TIMEOUT).

% @doc Write data at offset
-spec write(Pid :: pid(), Offset :: non_neg_integer(), Data :: binary()) -> ok.
write(Pid, Offset, Data) ->
    write(Pid, Offset, [], Data).

% @doc Write data at offset, including the client metadata. ClientMeta is a proplist
% that expects the following keys and values: 
% <ul>
%       <li>`client_csum_tag' - the type of checksum from the client as defined in the machi.hrl file
%       <li>`client_csum' - the checksum value from the client
% </ul>
-spec write(Pid :: pid(), Offset :: non_neg_integer(), ClientMeta :: proplists:proplist(), 
            Data :: binary()) -> ok|{error, term()}.
write(Pid, Offset, ClientMeta, Data) ->
    gen_server:call(Pid, {write, Offset, ClientMeta, Data}, ?TIMEOUT).

% @doc Append data
-spec append(Pid :: pid(), Data :: binary()) -> ok|{error, term()}.
append(Pid, Data) ->
    append(Pid, [], 0, Data).

% @doc Append data to file, supplying client metadata and (if desired) a
% reservation for additional space.  ClientMeta is a proplist and expects the
% same keys as write/4.
-spec append(Pid :: pid(), ClientMeta :: proplists:proplist(), 
             Extra :: non_neg_integer(), Data :: binary()) -> ok|{error, term()}.
append(Pid, ClientMeta, Extra, Data) ->
    gen_server:call(Pid, {append, ClientMeta, Extra, Data}, ?TIMEOUT).

%% TODO
%% read_repair(Filename, Offset, Data) ???
%% makes sense to me, but we could make the write path serve both purposes
%% I suppose...

%% gen_server callbacks

% @private
init({Filename, DataDir}) ->
    CsumFile = machi_util:make_csum_filename(DataDir, Filename),
    {_, DPath} = machi_util:make_data_filename(DataDir, Filename),
    LastWriteOffset = case parse_csum_file(CsumFile) of 
        0 -> ?MINIMUM_OFFSET;
        V -> V
    end,
    {ok, FHd} = file:open(DPath, [read, write, binary, raw]),
    {ok, FHc} = file:open(CsumFile, [append, binary, raw]),
    Tref = schedule_tick(),
    {ok, #state{
        filename = Filename,
        data_dir = DataDir,
        data_path = DPath,
        csum_file = CsumFile,
        data_filehandle = FHd,
        csum_filehandle = FHc,
        tref = Tref,
        last_write_offset = LastWriteOffset}}.

handle_call({sync, data}, _From, State = #state{ data_filehandle = FHd }) ->
    R = file:sync(FHd),
    {reply, R, State};

handle_call({sync, csum}, _From, State = #state{ csum_filehandle = FHc }) ->
    R = file:sync(FHc),
    {reply, R, State};

handle_call({sync, all}, _From, State = #state{filename = F,
                                               data_filehandle = FHd,
                                               csum_filehandle = FHc
                                              }) ->
    R = file:sync(FHc),
    R1 = file:sync(FHd),
    Resp = case {R, R1} of
        {ok, ok} -> ok;
        {ok, O1} -> 
                   lager:error("Got ~p during a data file sync on file ~p", [O1, F]), 
                   O1;
        {O2, ok} -> 
                   lager:error("Got ~p during a csum file sync on file ~p", [O2, F]), 
                   O2;
        {O3, O4} -> 
                   lager:error("Got ~p ~p syncing all files for file ~p", [O3, O4, F]),
                   {O3, O4}
    end,
    {reply, Resp, State};

%%% READS

handle_call({read, _Offset, _Length}, _From, 
            State = #state{wedged = true,
                           reads = {T, Err}
                          }) ->
    {reply, {error, wedged}, State#state{writes = {T + 1, Err + 1}}};

handle_call({read, Offset, Length}, _From, 
                State = #state{last_write_offset = Last,
                               reads = {T, Err}
                              }) when Offset + Length > Last ->
    lager:error("Read request at offset ~p for ~p bytes is past the last write offset of ~p", 
                [Offset, Length, Last]),
    {reply, {error, not_written}, State = #state{reads = {T + 1, Err + 1}}};

handle_call({read, Offset, Length}, _From, 
            State = #state{filename = F,
                           data_filehandle = FH,
                           reads = {T, Err}
                          }) ->

    Checksum = get({Offset, Length}), %% N.B. Maybe be 'undefined'!

    {Resp, NewErr} = case do_read(FH, F, Checksum, Offset, Length) of
        {ok, Bytes, Csum} ->
            {{ok, Bytes, Csum}, Err};
        eof ->
            {{error, not_written}, Err + 1};
        Error ->
            {Error, Err + 1}
    end,
    {reply, Resp, State#state{reads = {T+1, NewErr}}};

%%% WRITES

handle_call({write, _Offset, _ClientMeta, _Data}, _From, 
            State = #state{wedged = true,
                           writes = {T, Err}
                          }) ->
    {reply, {error, wedged}, State#state{writes = {T + 1, Err + 1}}};

handle_call({write, Offset, ClientMeta, Data}, _From, 
            State = #state{last_write_offset = Last,
                           filename = F,
                           writes = {T, Err},
                           data_filehandle = FHd,
                           csum_filehandle = FHc
                          }) ->

    ClientCsumTag = proplists:get_value(client_csum_tag, ClientMeta, ?CSUM_TAG_NONE), 
    ClientCsum = proplists:get_value(client_csum, ClientMeta, <<>>), 

    {Resp, NewErr, NewLast} = 
    case check_or_make_tagged_csum(ClientCsumTag, ClientCsum, Data) of
        {error, {bad_csum, Bad}} ->
            lager:error("Bad checksum on write; client sent ~p, we computed ~p", 
                        [ClientCsum, Bad]),
            {{error, bad_csum}, Err + 1, Last};
        TaggedCsum ->
            case handle_write(FHd, FHc, F, TaggedCsum, Offset, Data) of
                ok ->
                    {ok, Err, Last + Offset};
                Error ->
                    {Error, Err + 1, Last}
            end
    end,
    {reply, Resp, State#state{writes = {T+1, NewErr}, last_write_offset = NewLast}};

%% APPENDS

handle_call({append, _ClientMeta, _Extra, _Data}, _From, 
            State = #state{wedged = true,
                           appends = {T, Err}
                          }) ->
    {reply, {error, wedged}, State#state{appends = {T+1, Err+1}}};

handle_call({append, ClientMeta, Extra, Data}, _From, 
            State = #state{last_write_offset = Last,
                           filename = F,
                           appends = {T, Err},
                           data_filehandle = FHd,
                           csum_filehandle = FHc
                          }) ->

    ClientCsumTag = proplists:get_value(client_csum_tag, ClientMeta, ?CSUM_TAG_NONE), 
    ClientCsum = proplists:get_value(client_csum, ClientMeta, <<>>), 
    Size = iolist_size(Data),

    {Resp, NewErr, NewLast} = 
    case check_or_make_tagged_csum(ClientCsumTag, ClientCsum, Data) of
        {error, {bad_csum, Bad}} ->
            lager:error("Bad checksum; client sent ~p, we computed ~p",
                        [ClientCsum, Bad]),
            {{error, bad_csum}, Err + 1, Last};
        TaggedCsum ->
            case handle_write(FHd, FHc, F, TaggedCsum, Last, Data) of
                ok ->
                    {{ok, F, Last}, Err, Last + Size + Extra};
                Error ->
                    {Error, Err + 1, Last}
            end
    end,
    {reply, Resp, State#state{appends = {T+1, NewErr}, last_write_offset = NewLast}};

handle_call(Req, _From, State) ->
    lager:warning("Unknown call: ~p", [Req]),
    {reply, whoaaaaaaaaaaaa, State}.

handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

%% I dunno. This may not be a good idea, but it seems like if we're throwing lots of
%% errors, we ought to shut down and give up our file descriptors.
handle_info(tick, State = #state{
                             ops = Ops,
                             reads = {RT, RE},
                             writes = {WT, WE},
                             appends = {AT, AE}
                            }) when Ops > 100 andalso 
                               trunc(((RE+WE+AE) / RT+WT+AT) * 100) > ?TOO_MANY_ERRORS_RATIO ->
    Errors = RE + WE + AE,
    lager:notice("Got ~p errors. Shutting down.", [Errors]),
    {stop, too_many_errors, State};

handle_info(tick, State = #state{
                             ticks = Ticks,
                             ops = Ops,
                             reads = {RT, _RE},
                             writes = {WT, _WE},
                             appends = {AT, _AE}}) when Ops == RT + WT + AT, Ticks == ?TICK_THRESHOLD ->
    lager:debug("Got 5 ticks with no new activity. Shutting down."),
    {stop, normal, State};

handle_info(tick, State = #state{
                             ticks = Ticks,
                             ops = Ops,
                             reads = {RT, _RE},
                             writes = {WT, _WE},
                             appends = {AT, _AE}}) when Ops == RT + WT + AT ->
    lager:debug("No new activity since last tick. Incrementing tick counter."),
    Tref = schedule_tick(),
    {noreply, State#state{tref = Tref, ticks = Ticks + 1}};

handle_info(tick, State = #state{
                                 reads = {RT, _RE},
                                 writes = {WT, _WE},
                                 appends = {AT, _AE}
                                }) ->
    Ops = RT + WT + AT,
    lager:debug("Setting ops counter to ~p", [Ops]),
    Tref = schedule_tick(),
    {noreply, State#state{tref = Tref, ops = Ops}};

%handle_info({wedged, EpochId} State = #state{epoch = E}) when E /= EpochId ->
%    lager:notice("Wedge epoch ~p but ignoring because our epoch id is ~p", [EpochId, E]),
%    {noreply, State};

%handle_info({wedged, EpochId}, State = #state{epoch = E}) when E == EpochId ->
%    lager:notice("Wedge epoch ~p same as our epoch id ~p; we are wedged. Bummer.", [EpochId, E]),
%    {noreply, State#state{wedged = true}};

% flu1.erl:
% ProxyPid = get_proxy_pid(Filename),
% Are we wedged? if not
% machi_file_proxy:read(Pid, Offset, Length)
% otherwise -> error,wedged
%
% get_proxy_pid(Filename) ->
%   Pid = lookup_pid(Filename)
%   is_pid_alive(Pid)
%   Pid
%   if not alive then start one

handle_info(Req, State) ->
    lager:warning("Unknown info message: ~p", [Req]),
    {noreply, State}.

terminate(Reason, #state{
                         filename = F,
                         data_filehandle = FHd, 
                         csum_filehandle = FHc,
                         reads = {RT, RE},
                         writes = {WT, WE},
                         appends = {AT, AE}
                        }) ->
    lager:info("Shutting down proxy for file ~p because ~p", [F, Reason]),
    lager:info("   Op    Tot/Error", []),
    lager:info("  Reads:  ~p/~p", [RT, RE]),
    lager:info(" Writes:  ~p/~p", [WT, WE]),
    lager:info("Appends:  ~p/~p", [AT, AE]),
    ok = file:close(FHd),
    ok = file:close(FHc),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private functions

-spec schedule_tick() -> reference().
schedule_tick() ->
    erlang:send_after(?TICK, self(), tick).

-spec check_or_make_tagged_csum(Type     :: binary(), 
                                Checksum :: binary(),
                                Data     :: binary() ) -> binary() |
                                                          {error, {bad_csum, Bad :: binary()}}.
check_or_make_tagged_csum(?CSUM_TAG_NONE, _Csum, Data) ->
    %% We are making a checksum here
    Csum = machi_util:checksum_chunk(Data),
    machi_util:make_tagged_csum(server_sha, Csum);
check_or_make_tagged_csum(Tag, InCsum, Data) when Tag == ?CSUM_TAG_CLIENT_SHA; 
                                                  Tag == ?CSUM_TAG_SERVER_SHA ->
    Csum = machi_util:checksum_chunk(Data),
    case Csum =:= InCsum of
        true ->
            machi_util:make_tagged_csum(server_sha, Csum);
        false ->
            {error, {bad_csum, Csum}}
    end;
check_or_make_tagged_csum(OtherTag, _ClientCsum, _Data) ->
    lager:warning("Unknown checksum tag ~p", [OtherTag]),
    {error, bad_csum}.

encode_csum_file_entry(Offset, Size, TaggedCSum) ->
    Len = 8 + 4 + byte_size(TaggedCSum),
    [<<Len:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big>>,
     TaggedCSum].

map_offsets_to_csums(CsumList) ->
    lists:foreach(fun insert_offsets/1, CsumList).

insert_offsets({Offset, Length, Checksum}) ->
    put({Offset, Length}, Checksum).

parse_csum_file(Filename) ->
    {ok, CsumData} = file:read_file(Filename),
    {DecodedCsums, _Junk} = machi_flu1:split_checksum_list_blob_decode(CsumData),
    case DecodedCsums of
        [] -> 0;
        _ ->
            map_offsets_to_csums(DecodedCsums),
            {Offset, Size, _Csum} = lists:last(DecodedCsums),
            Offset + Size
    end.

-spec do_read(FHd        :: file:filehandle(),
              Filename   :: string(),
              TaggedCsum :: undefined|binary(),
              Offset     :: non_neg_integer(),
              Size       :: non_neg_integer()) -> eof | 
                                            {ok, Bytes :: binary(), Csum :: binary()} |
                                            {error, bad_csum} |
                                            {error, partial_read} |
                                            {error, Other :: term() }.
do_read(FHd, Filename, undefined, Offset, Size) ->
    do_read(FHd, Filename, machi_util:make_tagged_csum(none), Offset, Size);

do_read(FHd, Filename, TaggedCsum, Offset, Size) ->
    case file:pread(FHd, Offset, Size) of
        eof -> 
            eof;

        {ok, Bytes} when byte_size(Bytes) == Size ->
            {Type, Ck} = machi_util:unmake_tagged_csum(TaggedCsum),
            case check_or_make_tagged_csum(Type, Ck, Bytes) of
                {error, Bad} ->
                    lager:error("Bad checksum; got ~p, expected ~p",
                                [Bad, Ck]),
                    {error, bad_csum};
                TaggedCsum ->
                    {ok, Bytes, TaggedCsum}
            end;

        {ok, Partial} ->
            lager:error("In file ~p, offset ~p, wanted to read ~p bytes, but got ~p", 
                        [Filename, Offset, Size, byte_size(Partial)]),
            {error, partial_read};
                    
        Other ->
            lager:error("While reading file ~p, offset ~p, length ~p, got ~p", 
                        [Filename, Offset, Size, Other]),
            {error, Other}
    end.

-spec handle_write( FHd        :: file:filehandle(),
                    FHc        :: file:filehandle(),
                    Filename   :: string(),
                    TaggedCsum :: binary(),
                    Offset     :: non_neg_integer(),
                    Data       :: binary() ) -> ok |
                                                {error, written} |
                                                {error, Reason :: term()}.
handle_write(FHd, FHc, Filename, TaggedCsum, Offset, Data) ->
    Size = iolist_size(Data),
    case do_read(FHd, Filename, TaggedCsum, Offset, Size) of
        eof ->
            try
                do_write(FHd, FHc, Filename, TaggedCsum, Offset, Size, Data)
            catch
                %%% XXX FIXME: be more specific on badmatch that might
                %%% occur around line 520 when we write the checksum
                %%% file entry for the data blob we just put on the disk
                error:Reason ->
                    {error, Reason}
            end;
        {ok, _, _} ->
            % yep, we did that write! Honest.
            ok;
        {error, Error} ->
            lager:error("During write to ~p, offset ~p, got error ~p; returning {error, written}",
                        [Filename, Offset, Error]),
            {error, written}
    end.

-spec do_write( FHd        :: file:descriptor(),
                FHc        :: file:descriptor(),
                Filename   :: string(),
                TaggedCsum :: binary(),
                Offset     :: non_neg_integer(),
                Size       :: non_neg_integer(),
                Data       :: binary() ) -> ok|term().
do_write(FHd, FHc, Filename, TaggedCsum, Offset, Size, Data) ->
    case file:pwrite(FHd, Offset, Data) of
        ok ->
            lager:debug("Successful write in file ~p at offset ~p, length ~p",
                        [Filename, Offset, Size]),
            EncodedCsum = encode_csum_file_entry(Offset, Size, TaggedCsum),
            ok = file:write(FHc, EncodedCsum),
            lager:debug("Successful write to checksum file for ~p.", [Filename]),
            ok;
        Other ->
            lager:error("Got ~p during write to file ~p at offset ~p, length ~p",
            [Other, Filename, Offset, Size]),
            {error, Other}
    end.
