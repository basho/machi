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

%% @doc Erlang API for the Machi FLU TCP protocol version 1, with a
%% proxy-process style API for hiding messy details such as TCP
%% connection/disconnection with the remote Machi server.
%%
%% Machi is intentionally avoiding using distributed Erlang for
%% Machi's communication.  This design decision makes Erlang-side code
%% more difficult &amp; complex, but it's the price to pay for some
%% language independence.  Later in Machi's life cycle, we need to
%% (re-)implement some components in a non-Erlang/BEAM-based language.
%%
%% This module implements a "man in the middle" proxy between the
%% Erlang client and Machi server (which is on the "far side" of a TCP
%% connection to somewhere).  This proxy process will always execute
%% on the same Erlang node as the Erlang client that uses it.  The
%% proxy is intended to be a stable, long-lived process that survives
%% TCP communication problems with the remote server.

-module(machi_chain_repair).

-include("machi_projection.hrl").

-define(SHORT_TIMEOUT, 5*1000).
-define(LONG_TIMEOUT, 60*1000).

%% These macros assume there's a bound variable called Verb.
-define(VERB(Fmt),       if Verb -> io:format(Fmt      ); true -> ok end).
-define(VERB(Fmt, Args), if Verb -> io:format(Fmt, Args); true -> ok end).

-record(stat, {
           i_files = 0,
           i_chs = 0,
           i_bytes = 0,
           o_files = 0,
           o_chs = 0,
           o_bytes = 0}
        ).

-export([repair_cp/4, repair_ap/5]).

repair_cp(Src, Dst, MembersDict, Opts) ->
    %% TODO: add missing function: wipe away any trace of chunks
    %% are present on Dst but missing on Src.
    exit(todo_cp_mode).

repair_ap(Src, Repairing, UPI, MembersDict, Opts) ->
    %% Use process dict so that 'after' clause can always quit all
    %% proxy pids.
    put(proxies_dict, orddict:new()),
    Add = fun(Name, Pid) -> put(proxies_dict, orddict:store(Name, Pid, get(proxies_dict))) end,
    OurFLUs = lists:usort([Src] ++ Repairing ++ UPI),
    Res = try
              [begin
                   {ok, Proxy} = machi_proxy_flu1_client:start_link(P),
                   Add(FLU, Proxy)
               end || {FLU,P} <- MembersDict, lists:member(FLU, OurFLUs)],
              ProxiesDict = get(proxies_dict),

              D = dict:new(),
              D2 = lists:foldl(fun({FLU, Proxy}, Dict) ->
                                       append_file_dict(Proxy, FLU, Dict)
                               end, D, ProxiesDict),
              MissingFileSummary = make_missing_file_summary(D2, OurFLUs),
              io:format(user, "MissingFileSummary ~p\n", [MissingFileSummary]),

              %% Repair files from perspective of Src, i.e. tail(UPI).
              RepairMode = proplists:get_value(repair_mode, Opts, repair),
              [ok = repair_file(ap_mode, RepairMode, File, Size, MissingList,
                                proplists:get_value(verbose, Opts, true),
                                Src, ProxiesDict) ||
                  {File, {Size, MissingList}} <- MissingFileSummary],
              {ok, [yo_no_error, todo_stats_here]}
          catch
              What:Why ->
                  io:format(user, "What Why ~p ~p @\n\t~p\n",
                            [What, Why, erlang:get_stacktrace()]),
                  {error, yo_error}
          after
              [(catch machi_proxy_flu1_client:quit(Pid)) ||
                  Pid <- orddict:to_list(get(proxies_dict))]
          end,
    Res.

make_missing_file_summary(Dict, AllFLUs) ->
    %% FileFilterFun = fun(_) -> true end,
    FoldRes = lists:sort(dict:to_list(Dict)),
    %% NOTE: MissingFileSummary = [{File, {FileSize, ServersMissingFrom}}]
    MissingFileSummary =
        [begin
             {GotIt, Sizes} = lists:unzip(GotSizes),
             Size = lists:max(Sizes),
             Missing = {File, {Size, AllFLUs -- GotIt}},
             Missing
         end || {File, GotSizes} <- FoldRes %% , FileFilterFun(File)
        ],
    MissingFileSummary.

append_file_dict(Proxy, FLU_name, D) ->
    {ok, Res} = machi_proxy_flu1_client:list_files(Proxy, ?DUMMY_PV1_EPOCH),
    lists:foldl(fun({Size, File}, Dict) ->
                           dict:append(File, {FLU_name, Size}, Dict)
                end, D, Res).

%% TODO: There's no reason why repair can't be done 1).in parallel
%% across multiple repairees, and/or 2). with multiple byte ranges in
%% the same file, and/or 3). with bigger chunks.
%%
%% 1. Optimization
%% 2. Optimization
%% 3. Optimization, but it would be the easiest to implement, e.g. use
%%    constant-sized 4MB chunks.  Unfortuntely, it would also destroy
%%    the ability to verify here that the chunk checksums are correct
%%    *and* also propagate the correct checksum metadata to the
%%    destination FLU.
%%    As an additional optimization, add a bit of #2 to start the next
%%    read while the current write is still in progress.

repair_file(ap_mode, RepairMode,
            File, Size, [], Verb, Src, ProxiesDict) ->
    ?VERB("~p: ~s: present on both: ", [Src, File]),
    ?VERB("TODO!\n"), ok;
    %%TODO: repair_both_present(File, Size, RepairMode, V, SrcS, SrcS2, DstS, DstS2);
repair_file(ap_mode, RepairMode,
            File, Size, MissingList, Verb, Src, ProxiesDict) ->
    case lists:member(Src, MissingList) of
        true ->
            ?VERB("~p: ~s -> ~p, skipping: not on source server\n",
                  [Src, File, MissingList]);
        false when RepairMode == check ->
            ?VERB("~p: ~s -> ~p, copy ~s MB (skipped)\n",
                  [Src, File, MissingList, mbytes(Size)]);
        false ->
            ?VERB("~p: ~s -> ~p, copy ~s MB, ",
                  [Src, File, MissingList, mbytes(Size)]),
            MissingProxiesDict =
                orddict:filter(fun(K, _V) -> lists:member(K, MissingList) end,
                               ProxiesDict),
            SrcProxy = orddict:fetch(Src, ProxiesDict),
            ok = copy_file(File, SrcProxy, MissingProxiesDict, Verb),
            ?VERB("done\n", [])
    end.

copy_file(File, SrcProxy, MissingProxiesDict, Verb) ->
    %% Use the first source socket to enumerate the chunks & checksums.
    %% Use the second source socket to copy each chunk.
    N = length(orddict:to_list(MissingProxiesDict)),
    {ok, EpochID} = machi_proxy_flu1_client:get_epoch_id(SrcProxy,
                                                         ?SHORT_TIMEOUT),
    {ok, CheckSums} = machi_proxy_flu1_client:checksum_list(
                        SrcProxy, EpochID, File, ?LONG_TIMEOUT),
    CopyChunks =
        fun({Offset, Size, CSum}, #stat{i_chs=In_Cs, i_bytes=In_Bs,
                                        o_chs=Out_Cs, o_bytes=Out_Bs}=Acc) ->
                if In_Cs rem 100 == 0 -> ?VERB(".", []);
                   true               -> ok end,
                {ok, Chunk} = machi_proxy_flu1_client:read_chunk(
                                SrcProxy, EpochID, File, Offset, Size),
                case machi_util:checksum_chunk(Chunk) of
                    CSum_now when CSum_now == CSum ->
                        [begin
                             ok = machi_proxy_flu1_client:write_chunk(
                                    DstProxy, EpochID, File, Offset, Chunk)
                         end || {_FLU, DstProxy} <- MissingProxiesDict],
                        Acc#stat{i_chs=In_Cs + 1,  i_bytes=In_Bs + Size,
                                 o_chs=Out_Cs + N, o_bytes=Out_Bs+(N*Size)};
                    CSum_now ->
                        error_logger:error_msg(
                          "TODO: Checksum failure: "
                          "file ~p offset ~p size ~p: "
                          "expected ~p got ~p\n",
                          [File, Offset, Size, CSum, CSum_now]),
                        exit({todo_csum_error,
                              {File, Offset, Size, CSum, CSum_now}})
                end
        end,
    Stats = lists:foldl(CopyChunks, #stat{i_files=1,o_files=N}, CheckSums),
    #stat{i_chs=In_Cs, o_chs=Out_Cs} = Stats,
    ?VERB("copied ~w chunks to ~w replicas, ", [In_Cs, Out_Cs]),
    ok.

%% copy_file_proc_checksum_fun(File, SrcS, DstS, _Verbose) ->
%%     fun(<<OffsetHex:16/binary, " ", LenHex:8/binary, " ",
%%           CSumHex:32/binary, "\n">>) ->
%%             <<Len:32/big>> = hexstr_to_bin(LenHex),
%%             DownloadChunkBin = <<OffsetHex/binary, " ", LenHex/binary, " ",
%%                                  File/binary, "\n">>,
%%             [Chunk] = escript_download_chunks(SrcS, {{{DownloadChunkBin}}},
%%                                               fun(_) -> ok end),
%%             CSum = hexstr_to_bin(CSumHex),
%%             CSum2 = checksum(Chunk),
%%             if Len == byte_size(Chunk), CSum == CSum2 ->
%%                     {_,_,_} = upload_chunk_write(DstS, OffsetHex, File, Chunk),
%%                     ok;
%%                true ->
%%                     io:format("ERROR: ~s ~s ~s csum/size error\n",
%%                               [File, OffsetHex, LenHex]),
%%                     error
%%             end;
%%        (_Else) ->
%%             ok
%%     end.

repair_both_present(File, Size, RepairMode, V, SrcS, _SrcS2, DstS, _DstS2) ->
    verb("repair_both_present TODO\n"),
    ok.
    %% io:format("repair_both_present: ~p ~p mode ~p\n", [File, Size, RepairMode]).

%% repair_both_present(File, Size, RepairMode, V, SrcS, _SrcS2, DstS, _DstS2) ->
%%     Tmp1 = lists:flatten(io_lib:format("/tmp/sort.1.~w.~w.~w", tuple_to_list(now()))),
%%     Tmp2 = lists:flatten(io_lib:format("/tmp/sort.2.~w.~w.~w", tuple_to_list(now()))),
%%     J_Both = lists:flatten(io_lib:format("/tmp/join.3-both.~w.~w.~w", tuple_to_list(now()))),
%%     J_SrcOnly = lists:flatten(io_lib:format("/tmp/join.4-src-only.~w.~w.~w", tuple_to_list(now()))),
%%     J_DstOnly = lists:flatten(io_lib:format("/tmp/join.5-dst-only.~w.~w.~w", tuple_to_list(now()))),
%%     S_Identical = lists:flatten(io_lib:format("/tmp/join.6-sort-identical.~w.~w.~w", tuple_to_list(now()))),
%%     {ok, FH1} = file:open(Tmp1, [write, raw, binary]),
%%     {ok, FH2} = file:open(Tmp2, [write, raw, binary]),
%%     try
%%         K = md5_ctx,
%%         MD5_it = fun(Bin) ->
%%                          {FH, MD5ctx1} = get(K),
%%                          file:write(FH, Bin),
%%                          MD5ctx2 = crypto:hash_update(MD5ctx1, Bin),
%%                          put(K, {FH, MD5ctx2})
%%                  end,
%%         put(K, {FH1, crypto:hash_init(md5)}),
%%         ok = escript_checksum_list(SrcS, File, fast, MD5_it),
%%         {_, MD5_1} = get(K),
%%         SrcMD5 = crypto:hash_final(MD5_1),
%%         put(K, {FH2, crypto:hash_init(md5)}),
%%         ok = escript_checksum_list(DstS, File, fast, MD5_it),
%%         {_, MD5_2} = get(K),
%%         DstMD5 = crypto:hash_final(MD5_2),
%%         if SrcMD5 == DstMD5 ->
%%                 verb("identical\n", []);
%%            true ->
%%                 ok = file:close(FH1),
%%                 ok = file:close(FH2),
%%                 _Q1 = os:cmd("./REPAIR-SORT-JOIN.sh " ++ Tmp1 ++ " " ++ Tmp2 ++ " " ++ J_Both ++ " " ++ J_SrcOnly ++ " " ++ J_DstOnly ++ " " ++ S_Identical),
%%                 case file:read_file_info(S_Identical) of
%%                     {ok, _} ->
%%                         verb("identical (secondary sort)\n", []);
%%                     {error, enoent} ->
%%                       io:format("differences found:"),
%%                       repair_both(File, Size, V, RepairMode,
%%                                   J_Both, J_SrcOnly, J_DstOnly,
%%                                   SrcS, DstS)
%%                 end
%%         end
%%     after
%%         catch file:close(FH1),
%%         catch file:close(FH2),
%%         [(catch file:delete(FF)) || FF <- [Tmp1,Tmp2,J_Both,J_SrcOnly,J_DstOnly,
%%                                            S_Identical]]
%%     end.

%% repair_both(File, _Size, V, RepairMode, J_Both, J_SrcOnly, J_DstOnly, SrcS, DstS) ->
%%     AccFun = if RepairMode == check ->
%%                      fun(_X, List) ->    List  end;
%%                 RepairMode == repair ->
%%                      fun( X, List) -> [X|List] end
%%              end,
%%     BothFun = fun(<<_OffsetSrcHex:16/binary, " ",
%%                     LenSrcHex:8/binary, " ", CSumSrcHex:32/binary, " ",
%%                     LenDstHex:8/binary, " ", CSumDstHex:32/binary, "\n">> =Line,
%%                   {SameB, SameC, DiffB, DiffC, Ds}) ->
%%                       <<Len:32/big>> = hexstr_to_bin(LenSrcHex),
%%                       if LenSrcHex == LenDstHex,
%%                          CSumSrcHex == CSumDstHex ->
%%                               {SameB + Len, SameC + 1, DiffB, DiffC, Ds};
%%                          true ->
%%                               %% D = {OffsetSrcHex, LenSrcHex, ........
%%                               {SameB, SameC, DiffB + Len, DiffC + 1,
%%                                AccFun(Line, Ds)}
%%                       end;
%%                  (_Else, Acc) ->
%%                       Acc
%%               end,
%%     OnlyFun = fun(<<_OffsetSrcHex:16/binary, " ", LenSrcHex:8/binary, " ",
%%                     _CSumHex:32/binary, "\n">> = Line,
%%                   {DiffB, DiffC, Ds}) ->
%%                       <<Len:32/big>> = hexstr_to_bin(LenSrcHex),
%%                       {DiffB + Len, DiffC + 1, AccFun(Line, Ds)};
%%                  (_Else, Acc) ->
%%                       Acc
%%               end,
%%     {SameBx, SameCx, DiffBy, DiffCy, BothDiffs} =
%%         file_folder(BothFun, {0,0,0,0,[]}, J_Both),
%%     {DiffB_src, DiffC_src, Ds_src} = file_folder(OnlyFun, {0,0,[]}, J_SrcOnly),
%%     {DiffB_dst, DiffC_dst, Ds_dst} = file_folder(OnlyFun, {0,0,[]}, J_DstOnly),
%%     if RepairMode == check orelse V == true ->
%%             io:format("\n\t"),
%%             io:format("BothR ~p, ", [{SameBx, SameCx, DiffBy, DiffCy}]),
%%             io:format("SrcR ~p, ", [{DiffB_src, DiffC_src}]),
%%             io:format("DstR ~p", [{DiffB_dst, DiffC_dst}]),
%%             io:format("\n");
%%        true -> ok
%%     end,
%%     if RepairMode == repair ->
%%             ok = repair_both_both(File, V, BothDiffs, SrcS, DstS),
%%             ok = repair_copy_chunks(File, V, Ds_src, DiffB_src, DiffC_src,
%%                                     SrcS, DstS),
%%             ok = repair_copy_chunks(File, V, Ds_dst, DiffB_dst, DiffC_dst,
%%                                     DstS, SrcS);
%%        true ->
%%             ok
%%     end.

%% repair_both_both(_File, _V, [_|_], _SrcS, _DstS) ->
%%     %% TODO: fetch both, check checksums, hopefully only exactly one
%%     %%       is correct, then use that one to repair the other.  And if the
%%     %%       sizes are different, hrm, there may be an extra corner case(s)
%%     %%       hiding there.
%%     io:format("WHOA! We have differing checksums or sizes here, TODO not implemented, but there's trouble in the little village on the river....\n"),
%%     timer:sleep(3*1000),
%%     ok;
%% repair_both_both(_File, _V, [], _SrcS, _DstS) ->
%%     ok.

%% repair_copy_chunks(_File, _V, [], _DiffBytes, _DiffCount, _SrcS, _DstS) ->
%%     ok;
%% repair_copy_chunks(File, V, ToBeCopied, DiffBytes, DiffCount, SrcS, DstS) ->
%%     verb("\n", []),
%%     verb("Starting copy of ~p chunks/~s MBytes to \n    ~s: ",
%%            [DiffCount, mbytes(DiffBytes), File]),
%%     InnerCopyFun = copy_file_proc_checksum_fun(File, SrcS, DstS, V),
%%     FoldFun = fun(Line, ok) ->
%%                       ok = InnerCopyFun(Line) % Strong sanity check
%%               end,
%%     ok = lists:foldl(FoldFun, ok, ToBeCopied),
%%     verb(" done\n", []),
%%     ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

file_folder(Fun, Acc, Path) ->
    {ok, FH} = file:open(Path, [read, raw, binary]),
    try
        file_folder2(Fun, Acc, FH)
    after
        file:close(FH)
    end.

file_folder2(Fun, Acc, FH) ->
    file_folder2(file:read_line(FH), Fun, Acc, FH).

file_folder2({ok, Line}, Fun, Acc, FH) ->
    Acc2 = Fun(Line, Acc),
    file_folder2(Fun, Acc2, FH);
file_folder2(eof, _Fun, Acc, _FH) ->
    Acc.

verb(Fmt) ->
    verb(Fmt, []).

verb(Fmt, Args) ->
    case {ok, true} of % application:get_env(kernel, verbose) of
        {ok, true} -> io:format(Fmt, Args);
        _          -> ok
    end.

mbytes(0) ->
    "0.0";
mbytes(Size) ->
    lists:flatten(io_lib:format("~.1.0f", [max(0.1, Size / (1024*1024))])).

