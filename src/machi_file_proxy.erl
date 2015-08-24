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
%%
%% 2. We might need a read repair command which does an unconditional write
%% on the data block?

-module(machi_file_proxy).
-behaviour(gen_server).

-include("machi.hrl").

%% public API
-export([
    start_link/2,
    stop/1,
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

-define(TICK, 30*1000). %% XXX FIXME Should be something like 5 seconds
-define(TICK_THRESHOLD, 5). %% After this + 1 more quiescent ticks, shutdown
-define(TIMEOUT, 10*1000).
-define(TOO_MANY_ERRORS_RATIO, 50).

-type op_stats()      :: { Total  :: non_neg_integer(), 
                           Errors :: non_neg_integer() }.

-type byte_sequence() :: { Offset :: non_neg_integer(),
                           Size   :: pos_integer()|infinity }.

-record(state, {
    data_dir              :: string() | undefined,
    filename              :: string() | undefined,
    data_path             :: string() | undefined,
    wedged = false        :: boolean(),
    csum_file             :: string()|undefined,
    csum_path             :: string()|undefined,
    eof_position = 0      :: non_neg_integer(),
    unwritten_bytes = []  :: [byte_sequence()],
    data_filehandle       :: file:filehandle(),
    csum_filehandle       :: file:filehandle(),
    tref                  :: reference(), %% timer ref
    ticks = 0             :: non_neg_integer(), %% ticks elapsed with no new operations
    ops = 0               :: non_neg_integer(), %% sum of all ops
    reads = {0, 0}        :: op_stats(),
    writes = {0, 0}       :: op_stats(),
    appends = {0, 0}      :: op_stats()
}).

%% Public API

% @doc Start a new instance of the file proxy service. Takes the filename 
% and data directory as arguments. This function is typically called by the
% `machi_file_proxy_sup:start_proxy/2' function.
-spec start_link(Filename :: string(), DataDir :: string()) -> any().
start_link(Filename, DataDir) ->
    gen_server:start_link(?MODULE, {Filename, DataDir}, []).

% @doc Request to stop an instance of the file proxy service.
-spec stop(Pid :: pid()) -> ok.
stop(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, {stop}, ?TIMEOUT).

% @doc Force a sync of all filehandles
-spec sync(Pid :: pid()) -> ok|{error, term()}.
sync(Pid) when is_pid(Pid) ->
    sync(Pid, all);
sync(_Pid) ->
    lager:warning("Bad pid to sync"),
    {error, bad_arg}.

% @doc Force a sync of a specific filehandle type. Valid types are `all', `csum' and `data'.
-spec sync(Pid :: pid(), Type :: all|data|csum) -> ok|{error, term()}.
sync(Pid, Type) when is_pid(Pid) andalso 
                     ( Type =:= all orelse Type =:= csum orelse Type =:= data ) ->
    gen_server:call(Pid, {sync, Type}, ?TIMEOUT);
sync(_Pid, Type) ->
    lager:warning("Bad arg to sync: Type ~p", [Type]),
    {error, bad_arg}.

% @doc Read file at offset for length
-spec read(Pid :: pid(),
           Offset :: non_neg_integer(),
           Length :: non_neg_integer()) -> {ok, Data :: binary(), Checksum :: binary()} |
                                           {error, Reason :: term()}.
read(Pid, Offset, Length) when is_pid(Pid) andalso is_integer(Offset) andalso Offset >= 0 
                               andalso is_integer(Length) andalso Length > 0 ->
    gen_server:call(Pid, {read, Offset, Length}, ?TIMEOUT);
read(_Pid, Offset, Length) ->
    lager:warning("Bad args to read: Offset ~p, Length ~p", [Offset, Length]),
    {error, bad_arg}.

% @doc Write data at offset
-spec write(Pid :: pid(), Offset :: non_neg_integer(), Data :: binary()) -> ok|{error, term()}.
write(Pid, Offset, Data) when is_pid(Pid) andalso is_integer(Offset) andalso Offset >= 0
                              andalso is_binary(Data) ->
    write(Pid, Offset, [], Data);
write(_Pid, Offset, _Data) ->
    lager:warning("Bad arg to write: Offset ~p", [Offset]),
    {error, bad_arg}.

% @doc Write data at offset, including the client metadata. ClientMeta is a proplist
% that expects the following keys and values:
% <ul>
%       <li>`client_csum_tag' - the type of checksum from the client as defined in the machi.hrl file</li>
%       <li>`client_csum' - the checksum value from the client</li>
% </ul>
-spec write(Pid :: pid(), Offset :: non_neg_integer(), ClientMeta :: proplists:proplist(),
            Data :: binary()) -> ok|{error, term()}.
write(Pid, Offset, ClientMeta, Data) when is_pid(Pid) andalso is_integer(Offset) andalso Offset >= 0
                                          andalso is_list(ClientMeta) andalso is_binary(Data) ->
    gen_server:call(Pid, {write, Offset, ClientMeta, Data}, ?TIMEOUT);
write(_Pid, Offset, ClientMeta, _Data) ->
    lager:warning("Bad arg to write: Offset ~p, ClientMeta: ~p", [Offset, ClientMeta]),
    {error, bad_arg}.

% @doc Append data
-spec append(Pid :: pid(), Data :: binary()) -> {ok, File :: string(), Offset :: non_neg_integer()}
                                                |{error, term()}.
append(Pid, Data) when is_pid(Pid) andalso is_binary(Data) ->
    append(Pid, [], 0, Data);
append(_Pid, _Data) ->
    lager:warning("Bad arguments to append/2"),
    {error, bad_arg}.

% @doc Append data to file, supplying client metadata and (if desired) a
% reservation for additional space.  ClientMeta is a proplist and expects the
% same keys as write/4.
-spec append(Pid :: pid(), ClientMeta :: proplists:proplist(),
             Extra :: non_neg_integer(), Data :: binary()) -> {ok, File :: string(), Offset :: non_neg_integer()}
                                                              |{error, term()}.
append(Pid, ClientMeta, Extra, Data) when is_pid(Pid) andalso is_list(ClientMeta) 
                                          andalso is_integer(Extra) andalso Extra >= 0 
                                          andalso is_binary(Data) ->
    gen_server:call(Pid, {append, ClientMeta, Extra, Data}, ?TIMEOUT);
append(_Pid, ClientMeta, Extra, _Data) ->
    lager:warning("Bad arg to append: ClientMeta ~p, Extra ~p", [ClientMeta, Extra]),
    {error, bad_arg}.

%% gen_server callbacks

% @private
init({Filename, DataDir}) ->
    CsumFile = machi_util:make_checksum_filename(DataDir, Filename),
    {_, DPath} = machi_util:make_data_filename(DataDir, Filename),
    ok = filelib:ensure_dir(CsumFile),
    ok = filelib:ensure_dir(DPath),
    UnwrittenBytes = parse_csum_file(CsumFile),
    {Eof, infinity} = lists:last(UnwrittenBytes),
    {ok, FHd} = file:open(DPath, [read, write, binary, raw]),
    {ok, FHc} = file:open(CsumFile, [append, binary, raw]),
    Tref = schedule_tick(),
    St = #state{
        filename        = Filename,
        data_dir        = DataDir,
        data_path       = DPath,
        csum_file       = CsumFile,
        data_filehandle = FHd,
        csum_filehandle = FHc,
        tref            = Tref,
        unwritten_bytes = UnwrittenBytes,
        eof_position    = Eof},
    lager:debug("Starting file proxy ~p for filename ~p, state = ~p",
                [self(), Filename, St]),
    {ok, St}.

% @private
handle_call({stop}, _From, State) ->
    lager:debug("Requested to stop."),
    {stop, normal, State};

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
                State = #state{eof_position = Eof,
                               reads = {T, Err}
                              }) when Offset + Length > Eof ->
    lager:error("Read request at offset ~p for ~p bytes is past the last write offset of ~p",
                [Offset, Length, Eof]),
    {reply, {error, not_written}, State#state{reads = {T + 1, Err + 1}}};

handle_call({read, Offset, Length}, _From,
            State = #state{filename = F,
                           data_filehandle = FH,
                           unwritten_bytes = U,
                           reads = {T, Err}
                          }) ->

    Checksum = get({Offset, Length}), %% N.B. Maybe be 'undefined'!

    {Resp, NewErr} = case handle_read(FH, F, Checksum, Offset, Length, U) of
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
            State = #state{unwritten_bytes = U,
                           filename = F,
                           writes = {T, Err},
                           data_filehandle = FHd,
                           csum_filehandle = FHc
                          }) ->

    ClientCsumTag = proplists:get_value(client_csum_tag, ClientMeta, ?CSUM_TAG_NONE),
    ClientCsum = proplists:get_value(client_csum, ClientMeta, <<>>),

    {Resp, NewErr, NewU} =
    case check_or_make_tagged_csum(ClientCsumTag, ClientCsum, Data) of
        {error, {bad_csum, Bad}} ->
            lager:error("Bad checksum on write; client sent ~p, we computed ~p",
                        [ClientCsum, Bad]),
            {{error, bad_csum}, Err + 1, U};
        TaggedCsum ->
            case handle_write(FHd, FHc, F, TaggedCsum, Offset, Data, U) of
                {ok, NewU1} ->
                    {ok, Err, NewU1};
                Error ->
                    {Error, Err + 1, U}
            end
    end,
    {NewEof, infinity} = lists:last(NewU),
    {reply, Resp, State#state{writes = {T+1, NewErr},
                              eof_position = NewEof,
                              unwritten_bytes = NewU
                             }};

%% APPENDS

handle_call({append, _ClientMeta, _Extra, _Data}, _From,
            State = #state{wedged = true,
                           appends = {T, Err}
                          }) ->
    {reply, {error, wedged}, State#state{appends = {T+1, Err+1}}};

handle_call({append, ClientMeta, Extra, Data}, _From,
            State = #state{eof_position = EofP,
                           unwritten_bytes = U,
                           filename = F,
                           appends = {T, Err},
                           data_filehandle = FHd,
                           csum_filehandle = FHc
                          }) ->

    ClientCsumTag = proplists:get_value(client_csum_tag, ClientMeta, ?CSUM_TAG_NONE),
    ClientCsum = proplists:get_value(client_csum, ClientMeta, <<>>),

    {Resp, NewErr, NewU} =
    case check_or_make_tagged_csum(ClientCsumTag, ClientCsum, Data) of
        {error, {bad_csum, Bad}} ->
            lager:error("Bad checksum; client sent ~p, we computed ~p",
                        [ClientCsum, Bad]),
            {{error, bad_csum}, Err + 1, U};
        TaggedCsum ->
            case handle_write(FHd, FHc, F, TaggedCsum, EofP, Data, U) of
                {ok, NewU1} ->
                    {{ok, F, EofP}, Err, NewU1};
                Error ->
                    {Error, Err + 1, EofP, U}
            end
    end,
    {NewEof, infinity} = lists:last(NewU),
    {reply, Resp, State#state{appends = {T+1, NewErr},
                              eof_position = NewEof + Extra,
                              unwritten_bytes = NewU
                             }};

handle_call(Req, _From, State) ->
    lager:warning("Unknown call: ~p", [Req]),
    {reply, whoaaaaaaaaaaaa, State}.

% @private
handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

% @private
handle_info(tick, State = #state{eof_position = Eof}) when Eof >= ?MAX_FILE_SIZE ->
    lager:notice("Eof position ~p >= max file size ~p. Shutting down.",
                 [Eof, ?MAX_FILE_SIZE]),
    {stop, file_rollover, State};

%% XXX Is this a good idea? Need to think this through a bit.
handle_info(tick, State = #state{wedged = true}) ->
    {stop, wedged, State};

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

% @private
terminate(Reason, #state{filename = F,
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

% @private
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

-spec parse_csum_file( Filename :: string() ) -> [byte_sequence()].
parse_csum_file(Filename) ->
    %% using file:read_file works as long as the files are "small"
    try
        {ok, CsumData} = file:read_file(Filename),
        {DecodedCsums, _Junk} = machi_flu1:split_checksum_list_blob_decode(CsumData),
        Sort = lists:sort(DecodedCsums),
        case Sort of
        [] -> [{?MINIMUM_OFFSET, infinity}];
        _ ->
            map_offsets_to_csums(DecodedCsums),
            {First, _, _} = hd(Sort),
            build_unwritten_bytes_list(Sort, First, [])
        end
    catch
        _:{badmatch, {error, enoent}} ->
            [{?MINIMUM_OFFSET, infinity}]
    end.
   
-spec handle_read(FHd        :: file:filehandle(),
                  Filename   :: string(),
                  TaggedCsum :: undefined|binary(),
                  Offset     :: non_neg_integer(),
                  Size       :: non_neg_integer(),
                  Unwritten  :: [byte_sequence()]
             ) -> {ok, Bytes :: binary(), Csum :: binary()} |
                  eof | 
                  {error, bad_csum} |
                  {error, partial_read} |
                  {error, not_written} |
                  {error, Other :: term() }.
% @private Attempt a read operation on the given offset and length.
% <li>
%   <ul> If the byte range is not yet written, `{error, not_written}' is
%        returned.</ul>
%   <ul> If the checksum given does not match what comes off the disk,
%        `{error, bad_csum}' is returned.</ul>
%   <ul> If the number of bytes that comes off the disk is not the requested length,
%        `{error, partial_read}' is returned.</ul>
%   <ul> If the offset is at or beyond the current file boundary, `eof' is returned.</ul>
%   <ul> If some kind of POSIX error occurs, the OTP version of that POSIX error
%        tuple is returned.</ul>
% </li>
%
% On success, `{ok, Bytes, Checksum}' is returned.
handle_read(FHd, Filename, undefined, Offset, Size, U) ->
    handle_read(FHd, Filename, machi_util:make_tagged_csum(none), Offset, Size, U);

handle_read(FHd, Filename, TaggedCsum, Offset, Size, U) ->
    case is_byte_range_unwritten(Offset, Size, U) of
        true ->
            {error, not_written};
        false ->
            do_read(FHd, Filename, TaggedCsum, Offset, Size)
    end.

do_read(FHd, Filename, TaggedCsum, Offset, Size) ->
    case file:pread(FHd, Offset, Size) of
        eof -> 
            eof;
        {ok, Bytes} when byte_size(Bytes) == Size ->
            {Tag, Ck} = machi_util:unmake_tagged_csum(TaggedCsum),
            case check_or_make_tagged_csum(Tag, Ck, Bytes) of
                {error, Bad} ->
                    lager:error("Bad checksum; got ~p, expected ~p",
                                [Bad, Ck]),
                    {error, bad_csum};
                TaggedCsum ->
                    {ok, Bytes, TaggedCsum};
                %% XXX FIXME: Should we return something other than
                %% {ok, ....} in this case?
                OtherCsum when Tag =:= ?CSUM_TAG_NONE ->
                    {ok, Bytes, OtherCsum}
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
                    Data       :: binary(),
                    Unwritten  :: [byte_sequence()]
                  ) -> {ok, NewU :: [byte_sequence()]} |
                       {error, written} |
                       {error, Reason :: term()}.
% @private Implements the write and append operation. The first task is to
% determine if the offset and data size has been written. If not, the write
% is allowed proceed.  A special case is made when an offset and data size
% match a checksum. In that case we read the data off the disk, validate the
% checksum and return a "fake" ok response as if the write had been performed
% when it hasn't really.
%
% If a write proceeds, the offset, size and checksum are written to a metadata
% file, and the internal list of unwritten bytes is modified to reflect the
% just-performed write.  This is then returned to the caller as 
% `{ok, NewUnwritten}' where NewUnwritten is the revised unwritten byte list.
handle_write(FHd, FHc, Filename, TaggedCsum, Offset, Data, U) ->
    Size = iolist_size(Data),

    case is_byte_range_unwritten(Offset, Size, U) of
        false ->
            case get({Offset, Size}) of
                undefined ->
                    {error, written};
                TaggedCsum ->
                    case do_read(FHd, Filename, TaggedCsum, Offset, Size) of
                        eof ->
                            lager:warning("This should never happen: got eof while reading at offset ~p in file ~p that's supposedly written",
                                          [Offset, Filename]),
                            {error, server_insanity};
                        {ok, _, _} ->
                            {ok, U};
                        _ ->
                            {error, written}
                    end;
                OtherCsum ->
                    %% Got a checksum, but it doesn't match the data block's
                    lager:error("During a potential write at offset ~p in file ~p, a check for unwritten bytes gave us checksum ~p but the data we were trying to trying to write has checksum ~p",
                                [Offset, Filename, OtherCsum, TaggedCsum]),
                    {error, written}
            end;
        true ->
            try
                do_write(FHd, FHc, Filename, TaggedCsum, Offset, Size, Data, U)
            catch
                %%% XXX FIXME: be more specific on badmatch that might
                %%% occur around line 593 when we write the checksum
                %%% file entry for the data blob we just put on the disk
                error:Reason ->
                    {error, Reason}
            end
    end.

% @private Implements the disk writes for both the write and append
% operation.
-spec do_write( FHd        :: file:descriptor(),
                FHc        :: file:descriptor(),
                Filename   :: string(),
                TaggedCsum :: binary(),
                Offset     :: non_neg_integer(),
                Size       :: non_neg_integer(),
                Data       :: binary(),
                Unwritten  :: [byte_sequence()]
              ) -> {ok, NewUnwritten :: [byte_sequence()]} |
                   {error, Reason :: term()}.
do_write(FHd, FHc, Filename, TaggedCsum, Offset, Size, Data, U) ->
    case file:pwrite(FHd, Offset, Data) of
        ok ->
            lager:debug("Successful write in file ~p at offset ~p, length ~p",
                        [Filename, Offset, Size]),
            EncodedCsum = encode_csum_file_entry(Offset, Size, TaggedCsum),
            ok = file:write(FHc, EncodedCsum),
            put({Offset, Size}, TaggedCsum),
            NewU = update_unwritten(Offset, Size, U),
            lager:debug("Successful write to checksum file for ~p; unwritten bytes are now: ~p",
                        [Filename, NewU]),
            {ok, NewU};
        Other ->
            lager:error("Got ~p during write to file ~p at offset ~p, length ~p",
            [Other, Filename, Offset, Size]),
            {error, Other}
    end.

-spec is_byte_range_unwritten( Offset    :: non_neg_integer(),
                            Size      :: pos_integer(),
                            Unwritten :: [byte_sequence()] ) -> boolean().
% @private Given an offset and a size, return `true' if a byte range has
% <b>not</b> been written. Otherwise, return `false'.
is_byte_range_unwritten(Offset, Size, Unwritten) ->
    case length(Unwritten) of
        0 ->
            lager:critical("Unwritten byte list has 0 entries! This should never happen."),
            false;
        1 ->
            {Eof, infinity} = hd(Unwritten),
            Offset >= Eof;
        _ ->
            case lookup_unwritten(Offset, Size, Unwritten) of
                {ok, _}   -> true;
                not_found -> false
            end
    end.

-spec lookup_unwritten( Offset    :: non_neg_integer(),
                        Size      :: pos_integer(),
                        Unwritten :: [byte_sequence()]
                      ) -> {ok, byte_sequence()} | not_found.
% @private Given an offset and a size, scan the list of unwritten bytes and
% look for a "hole" where a write might be allowed if any exist. If a
% suitable byte sequence is found, the function returns a tuple of {ok,
% {Position, Space}} is returned. `not_found' is returned if no suitable
% space is located.
lookup_unwritten(_Offset, _Size, []) ->
    not_found;
lookup_unwritten(Offset, _Size, [H={Pos, infinity}|_Rest]) when Offset >= Pos ->
    {ok, H};
lookup_unwritten(Offset, Size, [H={Pos, Space}|_Rest]) 
                when Offset >= Pos andalso Offset < Pos+Space 
                     andalso Size =< (Space - (Offset - Pos)) ->
    {ok, H};
lookup_unwritten(Offset, Size, [_H|Rest]) ->
    %% These are not the droids you're looking for.
    lookup_unwritten(Offset, Size, Rest).

%%% if the pos is greater than offset + size then we're done. End early.

-spec update_unwritten( Offset    :: non_neg_integer(),
                        Size      :: pos_integer(),
                        Unwritten :: [byte_sequence()] ) -> NewUnwritten :: [byte_sequence()].
% @private Given an offset, a size and the unwritten byte list, return an updated
% and sorted unwritten byte list accounting for any completed write operation.
update_unwritten(Offset, Size, Unwritten) ->
    case lookup_unwritten(Offset, Size, Unwritten) of
        not_found ->
            lager:error("Couldn't find byte sequence tuple for a write which earlier found a valid spot to write!!! This should never happen!"),
            Unwritten;
        {ok, {Offset, Size}} ->
            %% we neatly filled in our hole...
            lists:keydelete(Offset, 1, Unwritten);
        {ok, S={Pos, _}} ->
            lists:sort(lists:keydelete(Pos, 1, Unwritten) ++
              update_byte_range(Offset, Size, S))
    end.

-spec update_byte_range( Offset   :: non_neg_integer(),
                         Size     :: pos_integer(),
                         Sequence :: byte_sequence() ) -> Updates :: [byte_sequence()].
% @private Given an offset and size and a byte sequence tuple where a
% write took place, return a list of updates to the list of unwritten bytes
% accounting for the space occupied by the just completed write.
update_byte_range(Offset, Size, {Eof, infinity}) when Offset == Eof ->
    [{Offset + Size, infinity}];
update_byte_range(Offset, Size, {Eof, infinity}) when Offset > Eof ->
    [{Eof, (Offset - Eof)}, {Offset+Size, infinity}];
update_byte_range(Offset, Size, {Pos, Space}) when Offset == Pos andalso Size < Space ->
    [{Offset + Size, Space - Size}];
update_byte_range(Offset, Size, {Pos, Space}) when Offset > Pos ->
    [{Pos, Offset - Pos}, {Offset+Size, ( (Pos+Space) - (Offset + Size) )}].


-spec build_unwritten_bytes_list( CsumData   :: [{ Offset   :: non_neg_integer(),
                                                   Size     :: pos_integer(),
                                                   Checksum :: binary() }],
                                  LastOffset :: non_neg_integer(),
                                  Acc        :: list() ) -> [byte_sequence()].
% @private Given a <b>sorted</b> list of checksum data tuples, return a sorted
% list of unwritten byte ranges. The output list <b>always</b> has at least one
% entry: the last tuple in the list is guaranteed to be the current end of
% bytes written to a particular file with the special space moniker
% `infinity'.
build_unwritten_bytes_list([], Last, Acc) ->
    NewAcc = [ {Last, infinity} | Acc ],
    lists:reverse(NewAcc);
build_unwritten_bytes_list([{CurrentOffset, CurrentSize, _Csum}|Rest], LastOffset, Acc) when
      CurrentOffset /= LastOffset ->
    Hole = CurrentOffset - LastOffset,
    build_unwritten_bytes_list(Rest, (CurrentOffset+CurrentSize), [{LastOffset, Hole}|Acc]);
build_unwritten_bytes_list([{CO, CS, _Ck}|Rest], _LastOffset, Acc) ->
    build_unwritten_bytes_list(Rest, CO + CS, Acc).
