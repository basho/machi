#!/usr/bin/env escript
%% -*- erlang -*-
%%! +A 0 -smp disable -noinput -noshell

%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(file0_cc_migrate_files).
-compile(export_all).
-mode(compile).

-define(NO_MODULE, true).
-include("./file0.erl").

main([]) ->
    io:format("Use:  Migrate files from old chain to new chain via projection.\n"),
    io:format("Args: ProjectionPath Host Port [ verbose check repair delete-source noverbose nodelete-source ]\n"),
    erlang:halt(1);
main([ProjectionPathOrDir, Host, PortStr|Args]) ->
    Ps = make_repair_props(Args),
    P = read_projection_file(ProjectionPathOrDir),
    ChainMap = read_chain_map_file(ProjectionPathOrDir),

    SrcS = escript_connect(Host, PortStr),
    SrcS2 = escript_connect(Host, PortStr),
    Items = escript_list2(SrcS, fun(_) -> ok end),
    process_files(Items, SrcS, SrcS2, P, ChainMap, Ps).

process_files([], _SrcS, _SrcS2, _P, _ChainMap, _Ps) ->
    verb("Done\n"),
    ok;
process_files([Line|Items], SrcS, SrcS2, P, ChainMap, Ps) ->
    FileLen = byte_size(Line) - 16 - 1 - 1,
    <<SizeHex:16/binary, " ", File:FileLen/binary, _/binary>> = Line,
    Size = binary_to_integer(SizeHex, 16),
    Prefix = re:replace(File, "\\..*", "", [{return, binary}]),
    %% verb(Ps, "File ~s, prefix ~s\n", [File, Prefix]),
    verb("File ~s\n", [File]),
    verb("    ~s MBytes, ", [mbytes(Size)]),
    case calc_chain(read, P, ChainMap, Prefix) of
        {[OldChain], _} ->
            verb("remain in ~s\n", [OldChain]);
        {[NewChain,OldChain|_], _} ->
            verb("move ~s -> ~s\n", [OldChain, NewChain]),
            case proplists:get_value(mode, Ps) of
                repair ->
                    DestRawHPs = orddict:fetch(NewChain, ChainMap),
                    ok = migrate_a_file(SrcS, SrcS2, File, DestRawHPs, Ps),
                    case proplists:get_value(delete_source, Ps) of
                        true ->
                            verb("    delete source ~s\n", [File]),
                            ok = escript_delete(SrcS, File),
                            ok;
                        _ ->
                            verb("    skipping delete of source ~s\n", [File])
                    end;
                _ ->
                    ok
            end
    end,
    process_files(Items, SrcS, SrcS2, P, ChainMap, Ps).

migrate_a_file(SrcS, SrcS2, File, DestRawHPs, Ps) ->
    SrcName = "hack-src",
    DstName = "hack-dst",
    [begin
         [HostStr, PortStr] = convert_raw_hps([RawHP]),
         DstS = get_cached_sock(HostStr, PortStr),

         X = escript_compare_servers(SrcS, DstS,
                                     SrcName, DstName,
                                     fun(FName) when FName == File -> true;
                                        (_)                        -> false end,
                                     [null]),

         CheckV = proplists:get_value(mode, Ps, check),
         VerboseV = proplists:get_value(verbose, Ps, t),
         [ok = repair(File, Size, MissingList,
                      CheckV, VerboseV,
                      SrcS, SrcS2, DstS, dst2_unused, SrcName) ||
             {_File, {Size, MissingList}} <- X]
     end || RawHP <- DestRawHPs],
    ok.
