%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(corfurl_flu).

-behaviour(gen_server).

-type flu_error() :: 'error_badepoch' | 'error_trimmed' |
                     'error_overwritten' | 'error_unwritten'.
-export_type([flu_error/0]).

%% API
-export([start_link/1, start_link/3, status/1, stop/1]).
-export([write/4, read/3, seal/2, trim/3, fill/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("corfurl.hrl").

-ifdef(TEST).
-export([get__mlp/1, get__min_epoch/1, get__trim_watermark/1]).
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.
-endif.

-include_lib("kernel/include/file.hrl").

-record(state, {
          dir :: string(),
          mem_fh :: term(),
          min_epoch :: non_neg_integer(),
          page_size :: non_neg_integer(),
          max_mem :: non_neg_integer(),
          max_logical_page :: 'unknown' | non_neg_integer(),
          %% TODO: Trim watermark handling is *INCOMPLETE*.  The
          %%       current code is broken but is occasionally correct,
          %%       like a broken analog watch is correct 2x per day.
          trim_watermark :: non_neg_integer(),
          trim_count :: non_neg_integer()
         }).

start_link(Dir) ->
    start_link(Dir, 8, 64*1024*1024).

start_link(Dir, PageSize, MaxMem) ->
    gen_server:start_link(?MODULE, {Dir, PageSize, MaxMem}, []).

status(Pid) ->
    gen_server:call(Pid, status, infinity).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

write(Pid, Epoch, LogicalPN, PageBin)
  when is_integer(LogicalPN), LogicalPN > 0, is_binary(PageBin) ->
    gen_server:call(Pid, {write, Epoch, LogicalPN, PageBin}, infinity).

read(Pid, Epoch, LogicalPN)
  when is_integer(Epoch), Epoch > 0, is_integer(LogicalPN), LogicalPN > 0 ->
    gen_server:call(Pid, {read, Epoch, LogicalPN}, infinity).

seal(Pid, Epoch) when is_integer(Epoch), Epoch > 0 ->
    gen_server:call(Pid, {seal, Epoch}, infinity).

trim(Pid, Epoch, LogicalPN)
  when is_integer(Epoch), Epoch > 0, is_integer(LogicalPN), LogicalPN > 0 ->
    gen_server:call(Pid, {trim, Epoch, LogicalPN}, infinity).

fill(Pid, Epoch, LogicalPN)
  when is_integer(Epoch), Epoch > 0, is_integer(LogicalPN), LogicalPN > 0 ->
    gen_server:call(Pid, {fill, Epoch, LogicalPN}, infinity).

-ifdef(TEST).

get__mlp(Pid) ->
    gen_server:call(Pid, get__mlp, infinity).

get__min_epoch(Pid) ->
    gen_server:call(Pid, get__min_epoch, infinity).

get__trim_watermark(Pid) ->
    gen_server:call(Pid, get__trim_watermark, infinity).

-endif. % TEST

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

init({Dir, ExpPageSize, ExpMaxMem}) ->
    MemFile = memfile_path(Dir),
    filelib:ensure_dir(MemFile),
    {ok, FH} = file:open(MemFile, [read, write, raw, binary]),
    
    {_Version, MinEpoch, PageSize, MaxMem, TrimWatermark} =
        try
            Res = read_hard_state(Dir),
            case Res of
                {_V, _LE, PS, MM, TW}
                  when PS =:= ExpPageSize, MM =:= ExpMaxMem ->
                    Res
            end
        catch
            X:Y ->
                if X == error,
                   Y == {case_clause,{error,enoent}} ->
                        ok;
                   true ->
                        %% TODO: log-ify this
                        io:format("init: caught ~p ~p @ ~p\n",
                                  [X, Y, erlang:get_stacktrace()])
                end,
                {no_version_number, 0, ExpPageSize, ExpMaxMem, 0}
        end,
    State = #state{dir=Dir, mem_fh=FH, min_epoch=MinEpoch, page_size=PageSize,
                   max_mem=MaxMem, max_logical_page=unknown,
                   trim_watermark=TrimWatermark, trim_count=0},
    self() ! finish_init,                       % TODO
    {ok, State}.

handle_call({write, ClientEpoch, _LogicalPN, _PageBin}, _From,
            #state{min_epoch=MinEpoch} = State)
  when ClientEpoch < MinEpoch ->
    {reply, error_badepoch, State};
handle_call({write, _ClientEpoch, LogicalPN, PageBin}, _From,
            #state{max_logical_page=MLPN} = State) ->
    case check_write(LogicalPN, PageBin, State) of
        {ok, Offset} ->
            ok = write_page(Offset, LogicalPN, PageBin, State),
            NewMLPN = erlang:max(LogicalPN, MLPN),
            {reply, ok, State#state{max_logical_page=NewMLPN}};
        Else ->
            {reply, Else, State}
    end;

handle_call({read, ClientEpoch, _LogicalPN}, _From,
            #state{min_epoch=MinEpoch} = State)
  when ClientEpoch < MinEpoch ->
    {reply, error_badepoch, State};
handle_call({read, _ClientEpoch, LogicalPN}, _From, State) ->
    {reply, read_page(LogicalPN, State), State};

handle_call({seal, ClientEpoch}, _From, #state{min_epoch=MinEpoch} = State)
  when ClientEpoch =< MinEpoch ->
    {reply, error_badepoch, State};
handle_call({seal, ClientEpoch}, _From, #state{max_logical_page=MLPN}=State) ->
    NewState = State#state{min_epoch=ClientEpoch},
    ok = write_hard_state(NewState),
    {reply, {ok, MLPN}, NewState};

handle_call({trim, ClientEpoch, _LogicalPN}, _From,
            #state{min_epoch=MinEpoch} = State)
  when ClientEpoch < MinEpoch ->
    {reply, error_badepoch, State};
handle_call({trim, _ClientEpoch, LogicalPN}, _From, State) ->
    do_trim_or_fill(trim, LogicalPN, State);

handle_call({fill, ClientEpoch, _LogicalPN}, _From,
            #state{min_epoch=MinEpoch} = State)
  when ClientEpoch < MinEpoch ->
    {reply, error_badepoch, State};
handle_call({fill, _ClientEpoch, LogicalPN}, _From, State) ->
    do_trim_or_fill(fill, LogicalPN, State);

handle_call(get__mlp, _From, State) ->
    {reply, State#state.max_logical_page, State};
handle_call(get__min_epoch, _From, State) ->
    {reply, State#state.min_epoch, State};
handle_call(get__trim_watermark, _From, State) ->
    {reply, State#state.trim_watermark, State};
handle_call(status, _From, State) ->
    L = [{min_epoch, State#state.min_epoch},
         {page_size, State#state.page_size},
         {max_mem, State#state.max_mem},
         {max_logical_page, State#state.max_logical_page},
         {trim_watermark, State#state.trim_watermark}],
    {reply, {ok, L}, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    Reply = {whaaaaaaaaaaaaaaaaaa, Request},
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(finish_init, State) ->
    MLP = find_max_logical_page(State),
    State2 = State#state{max_logical_page=MLP},
    ok = write_hard_state(State2),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ok = write_hard_state(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

read_hard_state(Dir) ->
    File = hard_state_path(Dir),
    case file:read_file(File) of
        {ok, Bin} ->
            case binary_to_term(Bin) of
                T when element(1, T) == v1 ->
                    T
            end;
        Else ->
            Else
    end.            

write_hard_state(#state{min_epoch=MinEpoch, page_size=PageSize, max_mem=MaxMem,
                        trim_watermark=TrimWatermark} = S) ->
    NewPath = hard_state_path(S#state.dir),
    TmpPath = NewPath ++ ".tmp",
    {ok, FH} = file:open(TmpPath, [write, binary, raw]),
    HS = {v1, MinEpoch, PageSize, MaxMem, TrimWatermark},
    ok = file:write(FH, term_to_binary(HS)),
    %% ok = file:sync(FH), % TODO uncomment when the training wheels come off
    ok = file:close(FH),
    ok = file:rename(TmpPath, NewPath).

memfile_path(Dir) ->
    Dir ++ "/memfile".

hard_state_path(Dir) ->
    Dir ++ "/hard-state".

calc_page_offset(PhysicalPN, #state{page_size=PageSize}) ->
    TotalSize = ?PAGE_OVERHEAD + PageSize,
    PhysicalPN * TotalSize.

%% find_max_logical_page(): This is a kludge, based on our naive
%% implementation of not keeping the maximum logical page in hard
%% state.

find_max_logical_page(S) ->
    {ok, FI} = file:read_file_info(memfile_path(S#state.dir)),
    find_max_logical_page(0, 0, FI#file_info.size, S).

find_max_logical_page(MLP, PhysicalPN, FSize,
                      #state{mem_fh=FH, max_mem=MaxMem}=S) ->
    Offset = calc_page_offset(PhysicalPN, S),
    if Offset < MaxMem, Offset < FSize ->
            case file:pread(FH, Offset, 9) of
                {ok, <<1:8/big, LP:64/big>>} ->
                    find_max_logical_page(erlang:max(MLP, LP), PhysicalPN + 1,
                                          FSize, S);
                _ ->
                    find_max_logical_page(MLP, PhysicalPN + 1, FSize, S)
            end;
       true ->
            MLP
    end.    

check_write(LogicalPN, PageBin,
            #state{max_mem=MaxMem, page_size=PageSize} = S) ->
    Offset = calc_page_offset(LogicalPN, S),
    if Offset < MaxMem, byte_size(PageBin) =:= PageSize ->
            case check_is_written(Offset, LogicalPN, S) of
                false ->
                    {ok, Offset};
                true ->
                    error_overwritten
            end;
       true ->
            {bummer, ?MODULE, ?LINE, lpn, LogicalPN, offset, Offset, max_mem, MaxMem, page_size, PageSize}
    end.

check_is_written(Offset, _PhysicalPN, #state{mem_fh=FH}) ->
    case file:pread(FH, Offset, 1) of
        {ok, <<1:8>>} ->
            true;
        {ok, <<0:8>>} ->
            false;
        eof ->
            %% We assume that Offset has been bounds-checked
            false
    end.

write_page(Offset, LogicalPN, PageBin, #state{mem_fh=FH}) ->
    IOList = [<<1:8>>, <<LogicalPN:64/big>>, PageBin, <<1:8>>],
    ok = file:pwrite(FH, Offset, IOList).

read_page(LogicalPN, #state{max_mem=MaxMem, mem_fh=FH,
                            page_size=PageSize} = S) ->
    Offset = calc_page_offset(LogicalPN, S),
    if Offset < MaxMem ->
            case file:pread(FH, Offset, PageSize + ?PAGE_OVERHEAD) of
                {ok, <<1:8, LogicalPN:64/big, Page:PageSize/binary, 1:8>>} ->
                    {ok, Page};
                {ok, <<1:8, _LogicalPN:64/big, _:PageSize/binary, 0:8>>} ->
                    io:format("BUMMER: ~s line ~w: incomplete write at ~p\n",
                              [?MODULE, ?LINE, LogicalPN]),
                    error_unwritten;
                {ok, <<2:8>>} ->
                    error_trimmed;
                {ok, _} ->
                    error_unwritten;
                eof ->
                    error_unwritten;
                Else ->
                    io:format("BUMMER: ~s line ~w: ~p\n",
                              [?MODULE, ?LINE, Else]),
                    badarg               % TODO: better idea
            end;
       true ->
            badarg
    end.                    

do_trim_or_fill(Op, LogicalPN,
                #state{trim_watermark=TrimWatermark, trim_count=TrimCount} = S) ->
    case trim_page(Op, LogicalPN, S) of
        ok ->
            NewS = S#state{trim_watermark=erlang:max(
                                            TrimWatermark, LogicalPN),
                           trim_count=TrimCount + 1},
            if TrimCount rem 1000 == 0 ->
                    ok = write_hard_state(NewS);
               true ->
                    ok
            end,
            {reply, ok, NewS};
        Else ->
            {reply, Else, S}
    end.

trim_page(Op, LogicalPN, #state{max_mem=MaxMem, mem_fh=FH} = S) ->
    Offset = calc_page_offset(LogicalPN, S),
    if Offset < MaxMem ->
            Status = case file:pread(FH, Offset, 1) of
                         {ok, <<0:8>>} ->
                             error_unwritten;
                         {ok, <<1:8>>} ->
                             error_overwritten;
                         {ok, <<2:8>>} ->
                             error_trimmed;
                         eof ->
                             error_unwritten;
                         Else ->
                             io:format("BUMMER: ~s line ~w: ~p\n",
                                       [?MODULE, ?LINE, Else]),
                             error_trimmed % TODO
                     end,
            if Status == error_overwritten andalso Op == trim ->
                    ok = file:pwrite(FH, Offset, <<2:8>>),
                    ok;
               Status == error_unwritten andalso Op == fill ->
                    ok = file:pwrite(FH, Offset, <<2:8>>),
                    ok;
               true ->
                    Status
            end;
       true ->
            badarg
    end.
