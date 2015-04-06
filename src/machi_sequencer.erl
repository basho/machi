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

-module(machi_sequencer).

-compile(export_all).

-include_lib("kernel/include/file.hrl").

-define(CONFIG_DIR, "./config").
-define(DATA_DIR, "./data").

seq(Server, Prefix, Size) when is_binary(Prefix), is_integer(Size), Size > -1 ->
    Server ! {seq, self(), Prefix, Size},
    receive
        {assignment, File, Offset} ->
            {File, Offset}
    after 1*1000 ->
            bummer
    end.

seq_direct(Prefix, Size) when is_binary(Prefix), is_integer(Size), Size > -1 ->
    RegName = make_regname(Prefix),
    seq(RegName, Prefix, Size).

start_server() ->
    start_server(?MODULE).

start_server(Name) ->
    spawn_link(fun() -> run_server(Name) end).

run_server(Name) ->
    register(Name, self()),
    ets:new(?MODULE, [named_table, public, {write_concurrency, true}]),
    server_loop().

server_loop() ->
    receive
        {seq, From, Prefix, Size} ->
            spawn(fun() -> server_dispatch(From, Prefix, Size) end),
            server_loop()
    end.

server_dispatch(From, Prefix, Size) ->
    RegName = make_regname(Prefix),
    case whereis(RegName) of
        undefined ->
            start_prefix_server(Prefix),
            timer:sleep(1),
            server_dispatch(From, Prefix, Size);
        Pid ->
            Pid ! {seq, From, Prefix, Size}
    end,
    exit(normal).

start_prefix_server(Prefix) ->
    spawn(fun() -> run_prefix_server(Prefix) end).

run_prefix_server(Prefix) ->
    true = register(make_regname(Prefix), self()),
    ok = filelib:ensure_dir(?CONFIG_DIR ++ "/unused"),
    ok = filelib:ensure_dir(?DATA_DIR ++ "/unused"),
    FileNum = read_max_filenum(Prefix) + 1,
    ok = increment_max_filenum(Prefix),
    prefix_server_loop(Prefix, FileNum).

prefix_server_loop(Prefix, FileNum) ->
    File = make_data_filename(Prefix, FileNum),
    prefix_server_loop(Prefix, File, FileNum, 0).

prefix_server_loop(Prefix, File, FileNum, Offset) ->
    receive
        {seq, From, Prefix, Size} ->
            From ! {assignment, File, Offset},
            prefix_server_loop(Prefix, File, FileNum, Offset + Size)
    after 30*1000 ->
            io:format("timeout: ~p server stopping\n", [Prefix]),
            exit(normal)
    end.

make_regname(Prefix) ->
    erlang:binary_to_atom(Prefix, latin1).

make_config_filename(Prefix) ->
    lists:flatten(io_lib:format("~s/~s", [?CONFIG_DIR, Prefix])).

make_data_filename(Prefix, FileNum) ->
    erlang:iolist_to_binary(io_lib:format("~s/~s.~w",
                                          [?DATA_DIR, Prefix, FileNum])).

read_max_filenum(Prefix) ->
    case file:read_file_info(make_config_filename(Prefix)) of
        {error, enoent} ->
            0;
        {ok, FI} ->
            FI#file_info.size
    end.

increment_max_filenum(Prefix) ->
    {ok, FH} = file:open(make_config_filename(Prefix), [append]),
    ok = file:write(FH, "x"),
    %% ok = file:sync(FH),
    ok = file:close(FH).

%%%%%%%%%%%%%%%%%

%% basho_bench callbacks

-define(SEQ, ?MODULE).

new(1) ->
    start_server(),
    timer:sleep(100),
    {ok, unused};
new(_Id) ->
    {ok, unused}.

run(null, _KeyGen, _ValgueGen, State) ->
    {ok, State};
run(keygen_then_null, KeyGen, _ValgueGen, State) ->
    _Prefix = KeyGen(),
    {ok, State};
run(seq, KeyGen, _ValgueGen, State) ->
    Prefix = KeyGen(),
    {_, _} = ?SEQ:seq(?SEQ, Prefix, 1),
    {ok, State};
run(seq_direct, KeyGen, _ValgueGen, State) ->
    Prefix = KeyGen(),
    Name = ?SEQ:make_regname(Prefix),
    case get(Name) of
        undefined ->
            case whereis(Name) of
                undefined ->
                    {_, _} = ?SEQ:seq(?SEQ, Prefix, 1);
                Pid ->
                    put(Name, Pid),
                    {_, _} = ?SEQ:seq(Pid, Prefix, 1)
            end;
        Pid ->
            {_, _} = ?SEQ:seq(Pid, Prefix, 1)
    end,
    {ok, State};
run(seq_ets, KeyGen, _ValgueGen, State) ->
    Tab = ?MODULE,
    Prefix = KeyGen(),
    Res = try
              BigNum = ets:update_counter(Tab, Prefix, 1),
              BigBin = <<BigNum:80/big>>,
              <<FileNum:32/big, Offset:48/big>> = BigBin,
              %% if Offset rem 1000 == 0 ->
              %%         io:format("~p,~p ", [FileNum, Offset]);
              %%    true ->
              %%         ok
              %% end,
              {fakefake, FileNum, Offset}
          catch error:badarg ->
                  FileNum2 = 1, Offset2 = 0,
                  FileBin = <<FileNum2:32/big>>,
                  OffsetBin = <<Offset2:48/big>>,
                  Glop = <<FileBin/binary, OffsetBin/binary>>,
                  <<Base:80/big>> = Glop,
                  %% if Prefix == <<"42">> -> io:format("base:~w\n", [Base]); true -> ok end,
                  %% Base = 0,
                  case ets:insert_new(Tab, {Prefix, Base}) of
                      true ->
                          {<<"fakefakefake">>, Base};
                      false ->
                          Result2 = ets:update_counter(Tab, Prefix, 1),
                          {<<"fakefakefake">>, Result2}
                  end
          end,
    Res = Res,
    {ok, State}.

