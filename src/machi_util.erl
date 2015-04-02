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

-module(machi_util).

-export([
         checksum/1,
         hexstr_to_bin/1, bin_to_hexstr/1,
         hexstr_to_int/1, int_to_hexstr/2, int_to_hexbin/2,
         make_binary/1, make_string/1,
         make_regname/1,
         make_checksum_filename/2, make_data_filename/2,
         read_max_filenum/2, increment_max_filenum/2,
         info_msg/2, verb/1, verb/2,
         %% TCP protocol helpers
         connect/2
        ]).
-compile(export_all).

-include("machi.hrl").
-include("machi_projection.hrl").
-include_lib("kernel/include/file.hrl").

append(Server, Prefix, Chunk) when is_binary(Prefix), is_binary(Chunk) ->
    CSum = checksum(Chunk),
    Server ! {seq_append, self(), Prefix, Chunk, CSum},
    receive
        {assignment, Offset, File} ->
            {Offset, File}
    after 10*1000 ->
            bummer
    end.

make_regname(Prefix) when is_binary(Prefix) ->
    erlang:binary_to_atom(Prefix, latin1);
make_regname(Prefix) when is_list(Prefix) ->
    erlang:list_to_atom(Prefix).

make_config_filename(DataDir, Prefix) ->
    lists:flatten(io_lib:format("~s/config/~s", [DataDir, Prefix])).

make_checksum_filename(DataDir, Prefix, SequencerName, FileNum) ->
    lists:flatten(io_lib:format("~s/config/~s.~s.~w.csum",
                                [DataDir, Prefix, SequencerName, FileNum])).

make_checksum_filename(DataDir, FileName) ->
    lists:flatten(io_lib:format("~s/config/~s.csum", [DataDir, FileName])).

make_data_filename(DataDir, File) ->
    FullPath = lists:flatten(io_lib:format("~s/~s",  [DataDir, File])),
    {File, FullPath}.

make_data_filename(DataDir, Prefix, SequencerName, FileNum) ->
    File = erlang:iolist_to_binary(io_lib:format("~s.~s.~w",
                                                 [Prefix, SequencerName, FileNum])),
    FullPath = lists:flatten(io_lib:format("~s/~s",  [DataDir, File])),
    {File, FullPath}.

read_max_filenum(DataDir, Prefix) ->
    case file:read_file_info(make_config_filename(DataDir, Prefix)) of
        {error, enoent} ->
            0;
        {ok, FI} ->
            FI#file_info.size
    end.

increment_max_filenum(DataDir, Prefix) ->
    try
        {ok, FH} = file:open(make_config_filename(DataDir, Prefix), [append]),
        ok = file:write(FH, "x"),
        %% ok = file:sync(FH),
        ok = file:close(FH)
    catch
        error:{badmatch,_}=Error ->
            {error, Error, erlang:get_stacktrace()}
    end.

hexstr_to_bin(S) when is_list(S) ->
  hexstr_to_bin(S, []);
hexstr_to_bin(B) when is_binary(B) ->
  hexstr_to_bin(binary_to_list(B), []).

hexstr_to_bin([], Acc) ->
  list_to_binary(lists:reverse(Acc));
hexstr_to_bin([X,Y|T], Acc) ->
  {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
  hexstr_to_bin(T, [V | Acc]).

bin_to_hexstr(<<>>) ->
    [];
bin_to_hexstr(<<X:4, Y:4, Rest/binary>>) ->
    [hex_digit(X), hex_digit(Y)|bin_to_hexstr(Rest)].

hex_digit(X) when X < 10 ->
    X + $0;
hex_digit(X) ->
    X - 10 + $a.

make_binary(X) when is_binary(X) ->
    X;
make_binary(X) when is_list(X) ->
    iolist_to_binary(X).

make_string(X) when is_list(X) ->
    lists:flatten(X);
make_string(X) when is_binary(X) ->
    binary_to_list(X).

hexstr_to_int(X) ->
    B = hexstr_to_bin(X),
    B_size = byte_size(B) * 8,
    <<I:B_size/big>> = B,
    I.

int_to_hexstr(I, I_size) ->
    bin_to_hexstr(<<I:I_size/big>>).

int_to_hexbin(I, I_size) ->
    list_to_binary(int_to_hexstr(I, I_size)).

checksum(Bin) when is_binary(Bin) ->
    crypto:hash(md5, Bin).

verb(Fmt) ->
    verb(Fmt, []).

verb(Fmt, Args) ->
    case application:get_env(kernel, verbose) of
        {ok, true} -> io:format(Fmt, Args);
        _          -> ok
    end.

info_msg(Fmt, Args) ->
    case application:get_env(kernel, verbose) of {ok, false} -> ok;
                                                 _     -> error_logger:info_msg(Fmt, Args)
    end.

%%%%%%%%%%%%%%%%%

-spec connect(inet:ip_address() | inet:hostname(), inet:port_number()) ->
      port().
connect(Host, Port) ->
    escript_connect(Host, Port).

escript_connect(Host, PortStr) when is_list(PortStr) ->
    Port = list_to_integer(PortStr),
    escript_connect(Host, Port);
escript_connect(Host, Port) when is_integer(Port) ->
    {ok, Sock} = gen_tcp:connect(Host, Port, [{active,false}, {mode,binary},
                                              {packet, raw}]),
    Sock.

