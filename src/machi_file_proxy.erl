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
%% through this proxy.

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
    append/3,
    append/5
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

-type op_stats() :: { Total :: non_neg_integer(), Errors :: non_neg_integer()}.
    
-record(state, {
    data_dir :: string() | undefined,
    filename :: string() | undefined,
    data_path :: string() | undefined,
    sealed = false :: true|false, %% XXX sealed means this file is closed to new writes; not sure if useful
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
    gen_server:start_link({local, to_atom(Filename)}, ?MODULE, {Filename, DataDir}, []).

% @doc Force a sync of all filehandles
-spec sync(Filename :: string()) -> ok|{error, term()}.
sync(Filename) ->
    sync(Filename, all).

% @doc Force a sync of a specific filehandle type. Valid types are `all', `csum' and `data'.
-spec sync(Filename :: string(), Type :: all|data|csum) -> ok|{error, term()}.
sync(Filename, Type) ->
    gen_server:call(to_atom(Filename), {sync, Type}, ?TIMEOUT).

% @doc Read file at offset for length
-spec read(Filename :: string(), Offset :: non_neg_integer(), Length :: non_neg_integer()) -> {ok, binary()}|{error, term()}.
read(Filename, Offset, Length) ->
    gen_server:call(to_atom(Filename), {read, Offset, Length}, ?TIMEOUT).

% @doc Write data at offset
-spec write(Filename :: string(), Offset :: non_neg_integer(), Data :: binary()) -> ok.
write(Filename, Offset, Data) ->
    write(Filename, Offset, [], Data).

% @doc Write data at offset, including the client metadata. ClientMeta is a proplist
% that expects the following keys and values: 
% <ul>
%       <li>`client_csum_tag' - the type of checksum from the client as defined in the machi.hrl file
%       <li>`client_csum' - the checksum value from the client
% </ul>
-spec write(Filename :: string(), Offset :: non_neg_integer(), ClientMeta :: proplists:proplist(), 
            Data :: binary()) -> ok|{error, term()}.
write(Filename, Offset, ClientMeta, Data) ->
    gen_server:call(to_atom(Filename), {write, Offset, ClientMeta, Data}, ?TIMEOUT).

% @doc Append data at offset
-spec append(Filename :: string(), Offset :: non_neg_integer(), Data :: binary()) -> ok|{error, term()}.
append(Filename, Offset, Data) ->
    append(Filename, Offset, [], 0, Data).

% @doc Append data at offset, supplying client metadata and (if desired) a reservation for
% additional space.  ClientMeta is a proplist and expects the same keys as write/4.
-spec append(Filename :: string(), Offset :: non_neg_integer(), ClientMeta :: proplists:proplist(), 
             Extra :: non_neg_integer(), Data :: binary()) -> ok|{error, term()}.
append(Filename, Offset, ClientMeta, Extra, Data) ->
    gen_server:call(to_atom(Filename), {append, Offset, ClientMeta, Extra, Data}, ?TIMEOUT).

%% TODO
%% read_repair(Filename, Offset, Data) ???
%% makes sense to me, but we could make the write path serve both purposes
%% I suppose...

%% gen_server callbacks

% @private
init({Filename, DataDir}) ->
    CsumFile = machi_util:make_csum_filename(DataDir, Filename),
    {_, DPath} = machi_util:make_data_filename(DataDir, Filename),
    LastWriteOffset = get_last_offset_from_csum_file(CsumFile),
    %% The paranoid might do a file info request to validate that the 
    %% calculated offset is the same as the on-disk file's length
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

handle_call({read, Offset, _Length}, _From, 
                State = #state{last_write_offset = Last,
                               reads = {T, Err}
                              }) when Offset > Last ->
    lager:error("Read request at offset ~p is past the last write offset of ~p", 
                [Offset, Last]),
    {reply, {error, not_written}, State#state{reads = {T + 1, Err + 1}}};

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
    {Resp, NewErr} = case file:pread(FH, Offset, Length) of
        {ok, Bytes} when byte_size(Bytes) == Length ->
            lager:debug("successful read at ~p of ~p bytes", [Offset, Length]),
            {{ok, Bytes}, Err};
        {ok, Partial} ->
            lager:error("read ~p bytes, wanted ~p at offset ~p in file ~p", 
                [byte_size(Partial), Length, Offset, F]),
            {{error, partial_read}, Err + 1};
        eof ->
            lager:debug("Got eof on read operation", []),
            {{error, not_written}, Err + 1};
        Other ->
            lager:warning("Got ~p during file read operation on ~p", [Other, F]),
            {{error, Other}, Err + 1}
    end,
    {reply, Resp, State#state{reads = {T+1, NewErr}}};

%%% WRITES

handle_call({write, _Offset, _ClientMeta, _Data}, _From, 
            State = #state{sealed = true,
                           writes = {T, Err}
                          }) ->
    {reply, {error, sealed}, State#state{writes = {T + 1, Err + 1}}};

handle_call({write, Offset, _ClientMeta, _Data}, _From, 
            State = #state{last_write_offset = Last,
                           writes = {T, Err}
                          }) when Offset =< Last ->
    {reply, {error, written}, State#state{writes = {T + 1, Err + 1}}};

%% XXX: What if the chunk is larger than the max file size??
%% XXX: What if the chunk is larger than the physical disk we have??

handle_call({write, Offset, ClientMeta, Data}, _From, 
            State = #state{last_write_offset = Last,
                           filename = F,
                           writes = {T, Err},
                           data_filehandle = FHd,
                           csum_filehandle = FHc
                          }) when Offset > Last ->

    ClientCsumTag = proplists:get_value(client_csum_tag, ClientMeta), %% gets 'undefined' if not found
    ClientCsum = proplists:get_value(client_csum, ClientMeta), %% also potentially 'undefined'
    Size = iolist_size(Data),

    {Resp, NewErr, NewLast} = 
    case check_or_make_tagged_csum(ClientCsumTag, ClientCsum, Data) of
        {error, Error} ->
                {{error, Error}, Err + 1, Last};
        TaggedCsum ->
                %% Is additional paranoia warranted here? Should we attempt a pread
                %% at this position
                case file:pwrite(FHd, Offset, Data) of
                    ok ->
                        EncodedCsum = encode_csum_file_entry(Offset, Size, TaggedCsum),
                        ok = file:write(FHc, EncodedCsum),
                        {ok, Err, Last + Size};
                    Other ->
                        lager:error("Got ~p during write on file ~p at offset ~p, length ~p",
                            [Other, F, Offset, Size]),
                        {Other, Err + 1, Last} %% How do we detect partial writes? Pretend they don't exist? :)
                end
    end,
    {reply, Resp, State#state{writes = {T+1, NewErr}, last_write_offset = NewLast}};

%% APPENDS

handle_call({append, _Offset, _ClientMeta, _Extra, _Data}, _From, 
            State = #state{sealed = true,
                           appends = {T, Err}
                          }) ->
    {reply, {error, sealed}, State#state{appends = {T+1, Err+1}}};

handle_call({append, Offset, _ClientMeta, _Extra, _Data}, _From, 
            State = #state{last_write_offset = Last,
                           appends = {T, Err}
                          }) when Offset =< Last ->
    {reply, {error, written}, State#state{appends = {T+1, Err+1}}};

handle_call({append, Offset, ClientMeta, Extra, Data}, _From, 
            State = #state{last_write_offset = Last,
                           filename = F,
                           appends = {T, Err},
                           data_filehandle = FHd,
                           csum_filehandle = FHc
                          }) when Offset > Last ->

    ClientCsumTag = proplists:get_value(client_csum_tag, ClientMeta), %% gets 'undefined' if not found
    ClientCsum = proplists:get_value(client_csum, ClientMeta), %% also potentially 'undefined'
    Size = iolist_size(Data),

    {Resp, NewErr, NewLast} = 
    case check_or_make_tagged_csum(ClientCsumTag, ClientCsum, Data) of
        {error, Error} ->
                {{error, Error}, Err + 1, Last};
        TaggedCsum ->
                %% Is additional paranoia warranted here? 
                %% Should we attempt a pread at offset?
                case file:pwrite(FHd, Offset, Data) of
                    ok ->
                        EncodedCsum = encode_csum_file_entry(Offset, Size, TaggedCsum),
                        ok = file:write(FHc, EncodedCsum),
                        {ok, Err, Last + Size + Extra};
                    Other ->
                        lager:error("Got ~p during append on file ~p at offset ~p, length ~p",
                            [Other, F, Offset, Size]),
                        {Other, Err + 1, Last} %% How do we detect partial writes? Pretend they don't exist? :)
                end
    end,
    {reply, Resp, State#state{appends = {T+1, NewErr}, last_write_offset = NewLast}};

handle_call(Req, _From, State) ->
    lager:warning("Unknown call: ~p", [Req]),
    {reply, whaaaaaaaaaa, State}.

handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

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

schedule_tick() ->
    erlang:send_after(?TICK, self(), tick).

check_or_make_tagged_csum(undefined, undefined, Data) ->
    check_or_make_tagged_csum(?CSUM_TAG_NONE, undefined, Data);
check_or_make_tagged_csum(?CSUM_TAG_NONE, _Csum, Data) ->
    %% We are making a checksum here
    Csum = machi_util:checksum_chunk(Data),
    machi_util:make_tagged_csum(server_sha, Csum);
check_or_make_tagged_csum(?CSUM_TAG_CLIENT_SHA, ClientCsum, Data) ->
    Csum = machi_util:checksum_chunk(Data),
    case Csum =:= ClientCsum of
        true ->
            machi_util:make_tagged_csum(server_sha, Csum);
        false ->
            {error, bad_csum}
    end;
check_or_make_tagged_csum(OtherTag, _ClientCsum, _Data) ->
    lager:warning("Unknown checksum tag ~p", [OtherTag]),
    {error, bad_csum}.

encode_csum_file_entry(Offset, Size, TaggedCSum) ->
    Len = 8 + 4 + byte_size(TaggedCSum),
    [<<Len:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big>>,
     TaggedCSum].

get_last_offset_from_csum_file(Filename) ->
    {ok, CsumData} = file:read_file(Filename),
    {DecodedCsums, _Junk} = machi_flu1:split_checksum_list_blob_decode(CsumData),
    case DecodedCsums of
        [] -> 0;
        _ ->
            {Offset, Size, _Csum} = lists:last(DecodedCsums),
            Offset + Size
    end.

to_atom(String) when is_list(String) ->
    %% XXX FIXME: leaks atoms, yo.
    list_to_atom(String).

