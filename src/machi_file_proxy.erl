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
%% 1. Some way to transition the proxy into a wedged state that
%% doesn't rely on message delivery.
%%
%% 2. Check max file size on appends. Writes we take on faith we can
%% and should handle.
%%
%% 3. Async checksum reads on startup.

-module(machi_file_proxy).
-behaviour(gen_server).

-include("machi.hrl").

%% public API
-export([
    start_link/3,
    stop/1,
    sync/1,
    sync/2,
    read/3,
    read/4,
    write/3,
    write/4,
    trim/4,
    append/2,
    append/4,
    checksum_list/1
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

-record(state, {
    fluname               :: atom(),
    data_dir              :: string() | undefined,
    filename              :: string() | undefined,
    data_path             :: string() | undefined,
    wedged = false        :: boolean(),
    csum_file             :: string()|undefined,
    csum_path             :: string()|undefined,
    data_filehandle       :: file:io_device(),
    csum_table            :: machi_csum_table:table(),
    eof_position = 0      :: non_neg_integer(),
    max_file_size = ?DEFAULT_MAX_FILE_SIZE :: pos_integer(),
    tref                  :: reference(), %% timer ref
    ticks = 0             :: non_neg_integer(), %% ticks elapsed with no new operations
    ops = 0               :: non_neg_integer(), %% sum of all ops
    reads = {0, 0}        :: op_stats(),
    writes = {0, 0}       :: op_stats(),
    appends = {0, 0}      :: op_stats(),
    trims = {0, 0}        :: op_stats()
}).

%% Public API

% @doc Start a new instance of the file proxy service. Takes the filename
% and data directory as arguments. This function is typically called by the
% `machi_file_proxy_sup:start_proxy/2' function.
-spec start_link(FluName :: atom(), Filename :: string(), DataDir :: string()) -> any().
start_link(FluName, Filename, DataDir) ->
    gen_server:start_link(?MODULE, {FluName, Filename, DataDir}, []).

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

% @doc Read file at offset for length. This returns a sequence of all
% written and trimmed (optional) bytes that overlaps with requested
% offset and length. Borders are not aligned.
-spec read(Pid :: pid(),
           Offset :: non_neg_integer(),
           Length :: non_neg_integer()) ->
                  {ok, [{Filename::string(), Offset :: non_neg_integer(),
                         Data :: binary(), Checksum :: binary()}]} |
                  {error, Reason :: term()}.
read(Pid, Offset, Length) ->
    read(Pid, Offset, Length, #read_opts{}).

-spec read(Pid :: pid(),
           Offset :: non_neg_integer(),
           Length :: non_neg_integer(),
           machi_dt:read_opts_x()) ->
                  {ok, [{Filename::string(), Offset :: non_neg_integer(),
                         Data :: binary(), Checksum :: binary()}]} |
                  {error, Reason :: term()}.
read(Pid, Offset, Length, #read_opts{}=Opts)
  when is_pid(Pid) andalso is_integer(Offset) andalso Offset >= 0
       andalso is_integer(Length) andalso Length > 0 ->
    gen_server:call(Pid, {read, Offset, Length, Opts}, ?TIMEOUT);
read(_Pid, Offset, Length, Opts) ->
    lager:warning("Bad args to read: Offset ~p, Length ~p, Options ~p", [Offset, Length, Opts]),
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

trim(Pid, Offset, Size, TriggerGC) when is_pid(Pid),
                                        is_integer(Offset) andalso Offset >= 0,
                                        is_integer(Size) andalso Size > 0,
                                        is_boolean(TriggerGC) ->
    gen_server:call(Pid, {trim ,Offset, Size, TriggerGC}, ?TIMEOUT).

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

-spec checksum_list(pid()) -> {ok, list()}.
checksum_list(Pid) ->
    gen_server:call(Pid, {checksum_list}, ?TIMEOUT).

%% gen_server callbacks

% @private
init({FluName, Filename, DataDir}) ->
    CsumFile = machi_util:make_checksum_filename(DataDir, Filename),
    {_, DPath} = machi_util:make_data_filename(DataDir, Filename),
    ok = filelib:ensure_dir(CsumFile),
    ok = filelib:ensure_dir(DPath),
    {ok, CsumTable} = machi_csum_table:open(CsumFile, []),
    UnwrittenBytes = machi_csum_table:calc_unwritten_bytes(CsumTable),
    {Eof, infinity} = lists:last(UnwrittenBytes),
    {ok, FHd} = file:open(DPath, [read, write, binary, raw]),
    %% Reserve for EC and stuff, to prevent eof when read
    ok = file:pwrite(FHd, 0, binary:copy(<<"so what?">>, ?MINIMUM_OFFSET div 8)),
    Tref = schedule_tick(),
    St = #state{
        fluname         = FluName,
        filename        = Filename,
        data_dir        = DataDir,
        data_path       = DPath,
        csum_file       = CsumFile,
        data_filehandle = FHd,
        csum_table      = CsumTable,
        tref            = Tref,
        eof_position    = Eof,
        max_file_size   = machi_config:max_file_size()},
    lager:debug("Starting file proxy ~p for filename ~p, state = ~p, Eof = ~p",
                [self(), Filename, St, Eof]),
    {ok, St}.

% @private
handle_call({stop}, _From, State) ->
    lager:debug("Requested to stop."),
    {stop, normal, State};

handle_call({sync, data}, _From, State = #state{ data_filehandle = FHd }) ->
    R = file:sync(FHd),
    {reply, R, State};

handle_call({sync, csum}, _From, State) ->
    %% machi_csum_table always writes in {sync, true} option, so here
    %% explicit sync isn't actually needed.
    {reply, ok, State};

handle_call({sync, all}, _From, State = #state{filename = F,
                                               data_filehandle = FHd,
                                               csum_table = _T
                                              }) ->
    Resp = case file:sync(FHd) of
               ok ->
                   ok;
               Error ->
                   lager:error("Got ~p syncing all files for file ~p",
                               [Error, F]),
                   Error
    end,
    {reply, Resp, State};

%%% READS

handle_call({read, _Offset, _Length, _}, _From,
            State = #state{wedged = true,
                           reads = {T, Err}
                          }) ->
    {reply, {error, wedged}, State#state{writes = {T + 1, Err + 1}}};

handle_call({read, Offset, Length, _Opts}, _From,
                State = #state{eof_position = Eof,
                               reads = {T, Err}
                              }) when Offset > Eof ->
    %% make sure [Offset, Offset+Length) has an overlap with file range
    lager:error("Read request at offset ~p for ~p bytes is past the last write offset of ~p",
                [Offset, Length, Eof]),
    {reply, {error, not_written}, State#state{reads = {T + 1, Err + 1}}};

handle_call({read, Offset, Length, Opts}, _From,
            State = #state{filename = F,
                           data_filehandle = FH,
                           csum_table = CsumTable,
                           reads = {T, Err}
                          }) ->
    %% TODO: use these options - NoChunk prevents reading from disks
    %% NoChecksum doesn't check checksums
    #read_opts{no_checksum=NoChecksum, no_chunk=NoChunk,
               needs_trimmed=NeedsTrimmed} = Opts,
    {Resp, NewErr} =
        case do_read(FH, F, CsumTable, Offset, Length, NoChunk, NoChecksum) of
            {ok, {[], []}} ->
                {{error, not_written}, Err + 1};
            {ok, {Chunks0, Trimmed0}} ->
                Chunks = slice_both_side(Chunks0, Offset, Offset+Length),
                Trimmed = case NeedsTrimmed of
                              true -> Trimmed0;
                              false -> []
                          end,
                {{ok, {Chunks, Trimmed}}, Err};
            Error ->
                lager:error("Can't read ~p, ~p at File ~p", [Offset, Length, F]),
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
            State = #state{filename = F,
                           writes = {T, Err},
                           data_filehandle = FHd,
                           csum_table = CsumTable}) ->

    ClientCsumTag = proplists:get_value(client_csum_tag, ClientMeta, ?CSUM_TAG_NONE),
    ClientCsum = proplists:get_value(client_csum, ClientMeta, <<>>),

    {Resp, NewErr} =
    case check_or_make_tagged_csum(ClientCsumTag, ClientCsum, Data) of
        {error, {bad_csum, Bad}} ->
            lager:error("Bad checksum on write; client sent ~p, we computed ~p",
                        [ClientCsum, Bad]),
            {{error, bad_checksum}, Err + 1};
        TaggedCsum ->
            case handle_write(FHd, CsumTable, F, TaggedCsum, Offset, Data) of
                ok ->
                    {ok, Err};
                Error ->
                    {Error, Err + 1}
            end
    end,
    {NewEof, infinity} = lists:last(machi_csum_table:calc_unwritten_bytes(CsumTable)),
    lager:debug("Wrote ~p bytes at ~p of file ~p, NewEOF = ~p~n",
                [iolist_size(Data), Offset, F, NewEof]),
    {reply, Resp, State#state{writes = {T+1, NewErr},
                              eof_position = NewEof}};


%%% TRIMS

handle_call({trim, _Offset, _ClientMeta, _Data}, _From,
            State = #state{wedged = true,
                           writes = {T, Err}
                          }) ->
    {reply, {error, wedged}, State#state{writes = {T + 1, Err + 1}}};

handle_call({trim, Offset, Size, _TriggerGC}, _From,
            State = #state{data_filehandle=FHd,
                           ops = Ops,
                           trims = {T, Err},
                           csum_table = CsumTable}) ->

    case machi_csum_table:all_trimmed(CsumTable, Offset, Offset+Size) of
        true ->
            NewState = State#state{ops=Ops+1, trims={T, Err+1}},
            %% All bytes of that range was already trimmed returns ok
            %% here, not {error, trimmed}, which means the whole file
            %% was trimmed
            maybe_gc(ok, NewState);
        false ->
            LUpdate = maybe_regenerate_checksum(
                        FHd,
                        machi_csum_table:find_leftneighbor(CsumTable, Offset)),
            RUpdate = maybe_regenerate_checksum(
                        FHd,
                        machi_csum_table:find_rightneighbor(CsumTable, Offset+Size)),

            case machi_csum_table:trim(CsumTable, Offset, Size, LUpdate, RUpdate) of
                ok ->
                    {NewEof, infinity} = lists:last(machi_csum_table:calc_unwritten_bytes(CsumTable)),
                    NewState = State#state{ops=Ops+1,
                                           trims={T+1, Err},
                                           eof_position=NewEof},
                    maybe_gc(ok, NewState);
                Error ->
                    {reply, Error, State#state{ops=Ops+1, trims={T, Err+1}}}
            end
    end;

%% APPENDS

handle_call({append, _ClientMeta, _Extra, _Data}, _From,
            State = #state{wedged = true,
                           appends = {T, Err}
                          }) ->
    {reply, {error, wedged}, State#state{appends = {T+1, Err+1}}};

handle_call({append, ClientMeta, Extra, Data}, _From,
            State = #state{eof_position = EofP,
                           filename = F,
                           appends = {T, Err},
                           data_filehandle = FHd,
                           csum_table = CsumTable
                          }) ->

    ClientCsumTag = proplists:get_value(client_csum_tag, ClientMeta, ?CSUM_TAG_NONE),
    ClientCsum = proplists:get_value(client_csum, ClientMeta, <<>>),

    {Resp, NewErr} =
    case check_or_make_tagged_csum(ClientCsumTag, ClientCsum, Data) of
        {error, {bad_csum, Bad}} ->
            lager:error("Bad checksum; client sent ~p, we computed ~p",
                        [ClientCsum, Bad]),
            {{error, bad_checksum}, Err + 1};
        TaggedCsum ->
            case handle_write(FHd, CsumTable, F, TaggedCsum, EofP, Data) of
                ok ->
                    {{ok, F, EofP}, Err};
                Error ->
                    {Error, Err + 1}
            end
    end,
    NewEof = EofP + byte_size(Data) + Extra,
    lager:debug("appended ~p bytes at ~p file ~p. NewEofP = ~p",
                [iolist_size(Data), EofP, F, NewEof]),
    {reply, Resp, State#state{appends = {T+1, NewErr},
                              eof_position = NewEof}};

handle_call({checksum_list}, _FRom, State = #state{csum_table=T}) ->
    All = machi_csum_table:all(T),
    {reply, {ok, All}, State};

handle_call(Req, _From, State) ->
    lager:warning("Unknown call: ~p", [Req]),
    {reply, whoaaaaaaaaaaaa, State}.

% @private
handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

% @private
handle_info(tick, State = #state{eof_position = Eof,
                                 max_file_size = MaxFileSize}) when Eof >= MaxFileSize ->
    lager:notice("Eof position ~p >= max file size ~p. Shutting down.",
                 [Eof, MaxFileSize]),
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
                         csum_table = T,
                         reads = {RT, RE},
                         writes = {WT, WE},
                         appends = {AT, AE}
                        }) ->
    lager:info("Shutting down proxy for file ~p because ~p", [F, Reason]),
    lager:info("   Op    Tot/Error", []),
    lager:info("  Reads:  ~p/~p", [RT, RE]),
    lager:info(" Writes:  ~p/~p", [WT, WE]),
    lager:info("Appends:  ~p/~p", [AT, AE]),
    case FHd of
        undefined ->
            noop; %% file deleted
        _ ->
            ok = file:sync(FHd),
            ok = file:close(FHd)
    end,
    case T of
        undefined ->
            noop; %% file deleted
        _ ->
            ok = machi_csum_table:close(T)
    end,
    ok.

% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private functions

-spec schedule_tick() -> reference().
schedule_tick() ->
    erlang:send_after(?TICK, self(), tick).

-spec check_or_make_tagged_csum(Type     :: non_neg_integer(),
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
check_or_make_tagged_csum(?CSUM_TAG_SERVER_REGEN_SHA,
                          InCsum, Data) ->
    Csum = machi_util:checksum_chunk(Data),
    case Csum =:= InCsum of
        true ->
            machi_util:make_tagged_csum(server_regen_sha, Csum);
        false ->
            {error, {bad_csum, Csum}}
    end;
check_or_make_tagged_csum(OtherTag, _ClientCsum, _Data) ->
    lager:warning("Unknown checksum tag ~p", [OtherTag]),
    {error, bad_checksum}.

-spec do_read(FHd        :: file:io_device(),
              Filename   :: string(),
              CsumTable  :: machi_csum_table:table(),
              Offset     :: non_neg_integer(),
              Size       :: non_neg_integer(),
              NoChunk    :: boolean(),
              NoChecksum :: boolean()
             ) -> {ok, {Chunks :: [{string(), Offset::non_neg_integer(), binary(), Csum :: binary()}],
                        Trimmed :: [{string(), Offset::non_neg_integer(), Size::non_neg_integer()}]}} |
                  {error, bad_checksum} |
                  {error, partial_read} |
                  {error, file:posix()} |
                  {error, Other :: term() }.
% @private Attempt a read operation on the given offset and length.
% <li>
%   <ul> If the byte range is not yet written, `{error, not_written}' is
%        returned.</ul>
%   <ul> If the checksum given does not match what comes off the disk,
%        `{error, bad_checksum}' is returned.</ul>
%   <ul> If the number of bytes that comes off the disk is not the requested length,
%        `{error, partial_read}' is returned.</ul>
%   <ul> If the offset is at or beyond the current file boundary, `eof' is returned.</ul>
%   <ul> If some kind of POSIX error occurs, the OTP version of that POSIX error
%        tuple is returned.</ul>
% </li>
%
do_read(FHd, Filename, CsumTable, Offset, Size, _, _) ->
    %% Note that find/3 only returns overlapping chunks, both borders
    %% are not aligned to original Offset and Size.
    ChunkCsums = machi_csum_table:find(CsumTable, Offset, Size),
    read_all_ranges(FHd, Filename, ChunkCsums, [], []).

-spec read_all_ranges(file:io_device(), string(),
                      [{non_neg_integer(),non_neg_integer(),trimmed|binary()}],
                      Chunks :: [{string(), Offset::non_neg_integer(), binary(), Csum::binary()}],
                      Trimmed :: [{string(), Offset::non_neg_integer(), Size::non_neg_integer()}]) ->
                             {ok, {
                                Chunks :: [{string(), Offset::non_neg_integer(), binary(), Csum::binary()}],
                                Trimmed :: [{string(), Offset::non_neg_integer(), Size::non_neg_integer()}]}} |
                             {erorr, term()|partial_read}.
read_all_ranges(_, _, [], ReadChunks, TrimmedChunks) ->
    %% TODO: currently returns empty list of trimmed chunks
    {ok, {lists:reverse(ReadChunks), lists:reverse(TrimmedChunks)}};

read_all_ranges(FHd, Filename, [{Offset, Size, trimmed}|T], ReadChunks, TrimmedChunks) ->
    read_all_ranges(FHd, Filename, T, ReadChunks, [{Filename, Offset, Size}|TrimmedChunks]);

read_all_ranges(FHd, Filename, [{Offset, Size, TaggedCsum}|T], ReadChunks, TrimmedChunks) ->
    case file:pread(FHd, Offset, Size) of
        eof ->
            read_all_ranges(FHd, Filename, T, ReadChunks, TrimmedChunks);
        {ok, Bytes} when byte_size(Bytes) == Size, TaggedCsum =:= none ->
            read_all_ranges(FHd, Filename, T,
                            [{Filename, Offset, Bytes,
                              machi_util:make_tagged_csum(none, <<>>)}|ReadChunks],
                            TrimmedChunks);
        {ok, Bytes} when byte_size(Bytes) == Size ->
            {Tag, Ck} = machi_util:unmake_tagged_csum(TaggedCsum),
            case check_or_make_tagged_csum(Tag, Ck, Bytes) of
                {error, Bad} ->
                    lager:error("Bad checksum; got ~p, expected ~p",
                                [Bad, Ck]),
                    {error, bad_checksum};
                TaggedCsum ->
                    read_all_ranges(FHd, Filename, T,
                                    [{Filename, Offset, Bytes, TaggedCsum}|ReadChunks],
                                    TrimmedChunks);
                OtherCsum when Tag =:= ?CSUM_TAG_NONE ->
                    %% XXX FIXME: Should we return something other than
                    %% {ok, ....} in this case?
                    read_all_ranges(FHd, Filename, T,
                                    [{Filename, Offset, Bytes, OtherCsum}|ReadChunks],
                                    TrimmedChunks)
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

-spec handle_write( FHd        :: file:io_device(),
                    CsumTable  :: machi_csum_table:table(),
                    Filename   :: string(),
                    TaggedCsum :: binary(),
                    Offset     :: non_neg_integer(),
                    Data       :: binary()
                   ) -> ok |
                       {error, written} |
                       {error, Reason :: term()}.
% @private Implements the write and append operation. The first task is to
% determine if the offset and data size has been written. If not, the write
% is allowed proceed.  A special case is made when an offset and data size
% match a checksum. In that case we read the data off the disk, validate the
% checksum and return a "fake" ok response as if the write had been performed
% when it hasn't really.
%
% If a write proceeds, the offset, size and checksum are written to a
% metadata file, and the internal list of unwritten bytes is modified
% to reflect the just-performed write.  This is then returned to the
% caller as `ok'
handle_write(FHd, CsumTable, Filename, TaggedCsum, Offset, Data) ->
    Size = iolist_size(Data),
 
    case machi_csum_table:find(CsumTable, Offset, Size) of
        [] -> %% Nothing should be there
            try
                do_write(FHd, CsumTable, Filename, TaggedCsum, Offset, Size, Data)
            catch
                %% XXX FIXME: be more specific on badmatch that might
                %% occur around line 593 when we write the checksum
                %% file entry for the data blob we just put on the disk
                error:Reason ->
                    {error, Reason}
            end;
        [{Offset, Size, TaggedCsum}] ->
            case do_read(FHd, Filename, CsumTable, Offset, Size, false, false) of
                {error, _} = E ->
                    lager:warning("This should never happen: got ~p while reading"
                                  " at offset ~p in file ~p that's supposedly written",
                                  [E, Offset, Filename]),
                    {error, server_insanity};
                {ok, {[{_, Offset, Data, TaggedCsum}], _}} ->
                    %% TODO: what if different checksum got from do_read()?
                    ok;
                {ok, _Other} ->
                    %% TODO: leave some debug/warning message here?
                    {error, written}
            end;
        [{Offset, Size, OtherCsum}] ->
            %% Got a checksum, but it doesn't match the data block's
            lager:error("During a potential write at offset ~p in file ~p,"
                        " a check for unwritten bytes gave us checksum ~p"
                        " but the data we were trying to write has checksum ~p",
                        [Offset, Filename, OtherCsum, TaggedCsum]),
            {error, written};
        _Chunks ->
            %% TODO: Do we try to read all continuous chunks to see
            %% wether its total checksum matches client-provided checksum?
            case machi_csum_table:any_trimmed(CsumTable, Offset, Size) of
                true ->
                    %% More than a byte is trimmed, besides, do we
                    %% have to return exact written bytes? No. Clients
                    %% must issue read_chunk() with needs_trimmed
                    %% option as true
                    {error, trimmed};
                false ->
                    %% No byte is trimmed, but at least one byte is written
                    {error, written}
            end
    end.

% @private Implements the disk writes for both the write and append
% operation.
-spec do_write( FHd        :: file:io_device(),
                CsumTable  :: machi_csum_table:table(),
                Filename   :: string(),
                TaggedCsum :: binary(),
                Offset     :: non_neg_integer(),
                Size       :: non_neg_integer(),
                Data       :: binary()
              ) -> ok | {error, Reason :: term()}.
do_write(FHd, CsumTable, Filename, TaggedCsum, Offset, Size, Data) ->
    case file:pwrite(FHd, Offset, Data) of
        ok ->
            lager:debug("Successful write in file ~p at offset ~p, length ~p",
                        [Filename, Offset, Size]),

            %% Overlapping chunk; calculate checksum
            %% read {LOffset, Offset - LOffset} and make csum
            %% as server_sha
            LUpdate = maybe_regenerate_checksum(
                        FHd,
                        machi_csum_table:find_leftneighbor(CsumTable, Offset)),
            RUpdate = maybe_regenerate_checksum(
                        FHd,
                        machi_csum_table:find_rightneighbor(CsumTable, Offset+Size)),
            ok = machi_csum_table:write(CsumTable, Offset, Size,
                                        TaggedCsum, LUpdate, RUpdate),
            lager:debug("Successful write to checksum file for ~p",
                        [Filename]),
            ok;
        Other ->
            lager:error("Got ~p during write to file ~p at offset ~p, length ~p",
            [Other, Filename, Offset, Size]),
            {error, Other}
    end.

%% @doc Trim both right and left border of chunks to fit in to given
%% range [LeftPos, RightPos]. TODO: write unit tests for this function.

%% Dialyzer 'can never match': slice_both_side([], _, _) ->
    %% [];
slice_both_side([], _, _) ->
    [];
slice_both_side([{F, Offset, Chunk, _Csum}|L], LeftPos, RightPos)
  when Offset < LeftPos andalso LeftPos < RightPos ->
    TrashLen = (LeftPos - Offset),
    <<_:TrashLen/binary, NewChunk/binary>> = Chunk,
    NewChecksum = machi_util:make_tagged_csum(?CSUM_TAG_SERVER_REGEN_SHA_ATOM, Chunk),
    NewH = {F, LeftPos, NewChunk, NewChecksum},
    slice_both_side([NewH|L], LeftPos, RightPos);
slice_both_side(Chunks, LeftPos, RightPos) when LeftPos =< RightPos ->
    %% TODO: optimize
    [{F, Offset, Chunk, _Csum}|L] = lists:reverse(Chunks),
    Size = iolist_size(Chunk),
    if RightPos < Offset + Size ->
            NewSize = RightPos - Offset,
            <<NewChunk:NewSize/binary, _/binary>> = Chunk,
            NewChecksum = machi_util:make_tagged_csum(?CSUM_TAG_SERVER_REGEN_SHA_ATOM, Chunk),
            lists:reverse([{F, Offset, NewChunk, NewChecksum}|L]);
       true ->
            Chunks
    end.

maybe_regenerate_checksum(_, undefined) ->
    undefined;
maybe_regenerate_checksum(_, {_, _, trimmed} = Change) ->
    Change;
maybe_regenerate_checksum(FHd, {Offset, Size, _Csum}) ->
    case file:pread(FHd, Offset, Size) of
        eof ->
            error({eof, Offset, Size});
        {ok, Bytes} when byte_size(Bytes) =:= Size ->

            TaggedCsum = machi_util:make_tagged_csum(server_regen_sha,
                                                     machi_util:checksum_chunk(Bytes)),
            {Offset, Size, TaggedCsum};
        Error ->
            throw(Error)
    end.

%% GC: make sure unwritten bytes = [{Eof, infinity}] and Eof is > max
%% file size walk through the checksum table and make sure all chunks
%% trimmed Then unlink the file
-spec maybe_gc(term(), #state{}) ->
                      {reply, term(), #state{}} | {stop, normal, term(), #state{}}.
maybe_gc(Reply, S = #state{eof_position = Eof,
                           max_file_size = MaxFileSize}) when Eof < MaxFileSize ->
    lager:debug("The file is still small; not trying GC (Eof, MaxFileSize) = (~p, ~p)~n",
                [Eof, MaxFileSize]),
    {reply, Reply, S};
maybe_gc(Reply, S = #state{fluname=FluName,
                           data_filehandle = FHd,
                           data_dir = DataDir,
                           filename = Filename,
                           eof_position = Eof,
                           csum_table=CsumTable}) ->
    case machi_csum_table:all_trimmed(CsumTable, ?MINIMUM_OFFSET, Eof) of
        true ->
            lager:debug("GC? Let's do it: ~p.~n", [Filename]),
            %% Before unlinking a file, it should inform
            %% machi_flu_filename_mgr that this file is
            %% deleted and mark it as "trimmed" to avoid
            %% filename reuse and resurrection. Maybe garbage
            %% will remain if a process crashed but it also
            %% should be recovered at filename_mgr startup.

            %% Also, this should be informed *before* file proxy
            %% deletes files.
            ok = machi_flu_metadata_mgr:trim_file(FluName, {file, Filename}),
            ok = file:close(FHd),
            {_, DPath} = machi_util:make_data_filename(DataDir, Filename),
            ok = file:delete(DPath),
            machi_csum_table:delete(CsumTable),
            {stop, normal, Reply,
             S#state{data_filehandle=undefined,
                     csum_table=undefined}};
        false ->
            {reply, Reply, S}
    end.
