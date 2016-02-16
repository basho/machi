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

%% @doc Perform "chain repair", i.e., resynchronization of Machi file
%% contents and metadata as servers are (re-)added to the chain.
%%
%% The implementation here is a very basic one, and is probably a bit
%% slower than the original "demo day" implementation at
%% [https://github.com/basho/machi/blob/master/prototype/demo-day-hack/file0_repair_server.escript]
%%
%% It's so easy to bikeshed this into a 1 year programming exercise.
%%
%% General TODO note: There are a lot of areas for exploiting parallelism here.
%% I've set the bikeshed aside for now, but "make repair faster" has a
%% lot of room for exploiting concurrency, overlapping reads &amp; writes,
%% etc etc.  There are also lots of different trade-offs to make with
%% regard to RAM use vs. disk use.
%%
%% There's no reason why repair can't be done:
%%
%% <ol>
%% <li> Repair in parallel across multiple repairees ... Optimization.
%% </li>
%% <li> Repair multiple byte ranges concurrently ... Optimization.
%% </li>
%% <li> Use bigger chunks than the client originally used to write the file
%%    ... Optimization ... but it would be the easiest to implement, e.g. use
%%    constant-sized 4MB chunks.  Unfortuntely, it would also destroy
%%    the ability to verify here that the chunk checksums are correct
%%    *and* also propagate the correct checksum metadata to the
%%    destination FLU.
%%
%%    As an additional optimization, add a bit of #2 to start the next
%%    read while the current write is still in progress.
%% </li>
%% <li> The current method centralizes the "smarts" required to compare
%%    checksum differences ... move some computation to each FLU, then use
%%    a Merkle- or other-compression-style scheme to reduce the amount of
%%    data sent across a network.
%% </li>
%% </ol>
%%
%% Most/all of this could be executed in parallel on each FLU relative to
%% its own files.  Then, in another TODO option, perhaps build a Merkle tree
%% or other summary of the local files and send that data structure to the
%% repair coordinator.
%%
%% Also, as another TODO note, repair_both_present() in the
%% prototype/demo-day code uses an optimization of calculating the MD5
%% checksum of the chunk checksum data as it arrives, and if the two MD5s
%% match, then we consider the two files in sync.  If there isn't a match,
%% then we sort the lines and try another MD5, and if they match, then we're
%% in sync.  In theory, that's lower overhead than the procedure used here.
%%
%% NOTE that one reason I chose the "directives list" method is to have an
%% option, later, of choosing to repair a subset of repairee FLUs if there
%% is a big discrepency between out of sync files: e.g., if FLU x has N
%% bytes out of sync but FLU y has 50N bytes out of sync, then it's likely
%% better to repair x only so that x can return to the UPI list quickly.
%% Also, in the event that all repairees are roughly comparably out of sync,
%% then the repair network traffic can be minimized by reading each chunk
%% only once.

-module(machi_chain_repair).

-include("machi_projection.hrl").

-define(SHORT_TIMEOUT, 5*1000).
-define(LONG_TIMEOUT, 60*1000).
-define(MAX_OFFSET, 999*1024*1024*1024*1024*1024*1024*1024).

%% These macros assume there's a bound variable called Verb.
-define(VERB(Fmt),       if Verb -> io:format(Fmt      ); true -> ok end).
-define(VERB(Fmt, Args), if Verb -> io:format(Fmt, Args); true -> ok end).

-ifdef(TEST).
-compile(export_all).
-endif. % TEST

-export([repair/7]).

repair(ap_mode=ConsistencyMode, Src, Repairing, UPI, MembersDict, ETS, Opts) ->
    %% Use process dict so that 'after' clause can always quit all
    %% proxy pids.
    put(proxies_dict, orddict:new()),
    Add = fun(Name, Pid) -> put(proxies_dict, orddict:store(Name, Pid, get(proxies_dict))) end,
    OurFLUs = lists:usort([Src] ++ Repairing ++ UPI), % AP assumption!
    RepairMode = proplists:get_value(repair_mode, Opts, repair),
    Verb = proplists:get_value(verbose, Opts, false),
    RepairId = proplists:get_value(repair_id, Opts, id1),
    Res = try
              _ = [begin
                       {ok, Proxy} = machi_proxy_flu1_client:start_link(P),
                       Add(FLU, Proxy)
                   end || {FLU,P} <- MembersDict, lists:member(FLU, OurFLUs)],
              ProxiesDict = get(proxies_dict),

              D = dict:new(),
              D2 = lists:foldl(fun({FLU, Proxy}, Dict) ->
                                       get_file_lists(Proxy, FLU, Dict)
                               end, D, ProxiesDict),
              MissingFileSummary = make_missing_file_summary(D2, OurFLUs),
              %% ?VERB("~w MissingFileSummary ~p\n",[RepairId,MissingFileSummary]),
              lager:info("Repair ~w MissingFileSummary ~p\n",
                         [RepairId, MissingFileSummary]),

              [ets:insert(ETS, {{directive_bytes, FLU}, 0}) || FLU <- OurFLUs],
              %% Repair files from perspective of Src, i.e. tail(UPI).
              SrcProxy = orddict:fetch(Src, ProxiesDict),
              {ok, EpochID} = machi_proxy_flu1_client:get_epoch_id(
                                SrcProxy, ?SHORT_TIMEOUT),
              %% ?VERB("Make repair directives: "),
              Ds =
                  [{File, make_repair_directives(
                            ConsistencyMode, RepairMode, File, Size, EpochID,
                            Verb,
                            Src, OurFLUs, ProxiesDict, ETS)} ||
                      {File, {Size, _MissingList}} <- MissingFileSummary],
              %% ?VERB(" done\n"),
              lager:info("Repair ~w repair directives finished\n", [RepairId]),
              [begin
                   [{_, Bytes}] = ets:lookup(ETS, {directive_bytes, FLU}),
                   %% ?VERB("Out-of-sync data for FLU ~p: ~s MBytes\n",
                   %%       [FLU, mbytes(Bytes)]),
                   lager:info("Repair ~w "
                              "Out-of-sync data for FLU ~p: ~s MBytes\n",
                         [RepairId, FLU, mbytes(Bytes)]),
                   ok
               end || FLU <- OurFLUs],

              %% ?VERB("Execute repair directives: "),
              ok = execute_repair_directives(ConsistencyMode, Ds, Src, EpochID,
                                             Verb, OurFLUs, ProxiesDict, ETS),
              %% ?VERB(" done\n"),
              lager:info("Repair ~w repair directives finished\n", [RepairId]),
              ok
          catch
              What:Why ->
                  Stack = erlang:get_stacktrace(),
                  {error, {What, Why, Stack}}
          after
              [(catch machi_proxy_flu1_client:quit(Pid)) ||
                  Pid <- orddict:to_list(get(proxies_dict))]
          end,
    Res;
repair(cp_mode=_ConsistencyMode, Src, Repairing, UPI, MembersDict, ETS, Opts) ->
    io:format(user, "\n\nTODO! cp_mode repair is not fully implemented!\n\n", []),
    repair(ap_mode, Src, Repairing, UPI, MembersDict, ETS, Opts).

%% Create a list of servers where the file is completely missing.
%% In the "demo day" implementation and in an early integration WIP,
%% this was a useful thing.  TODO: Can this be removed?

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

get_file_lists(Proxy, FLU_name, D) ->
    {ok, Res} = machi_proxy_flu1_client:list_files(Proxy, ?DUMMY_PV1_EPOCH,
                                                   ?SHORT_TIMEOUT),
    lists:foldl(fun({Size, File}, Dict) ->
                           dict:append(File, {FLU_name, Size}, Dict)
                end, D, Res).

make_repair_compare_fun(SrcFLU) ->
    fun({{Offset_X, _Sz_a, _Cs_a, FLU_a}, _N_a},
        {{Offset_X, _Sz_b, _CS_b, FLU_b}, _N_b}) ->
       %% The repair source FLU always sorts less/earlier than anything else.
       if FLU_a == SrcFLU ->
               true;
          FLU_b == SrcFLU ->
               false;
          true ->
               %% Implicitly, smallest offset first.
               %% Secondarily (and implicitly), sort smallest chunk size first
               FLU_a < FLU_b
       end;
       (T_a, T_b) ->
            %% See implicitly comments above
            T_a =< T_b
    end.

make_repair_directives(ConsistencyMode, RepairMode, File, Size, _EpochID,
                       Verb, Src, FLUs0, ProxiesDict, ETS) ->
    true = (Size < ?MAX_OFFSET),
    FLUs = lists:usort(FLUs0),
    C0 = [begin
              %% erlang:garbage_collect(),
              Proxy = orddict:fetch(FLU, ProxiesDict),
              OffSzCs =
                  case machi_proxy_flu1_client:checksum_list(
                         Proxy, File, ?LONG_TIMEOUT) of
                      {ok, InfoBin} ->
                          machi_csum_table:split_checksum_list_blob_decode(InfoBin);
                      {error, no_such_file} ->
                          []
                  end,
              [{?MAX_OFFSET, 0, <<>>, FLU}]       % our end-of-file marker
              ++
              [{Off, Sz, Cs, FLU} || {Off, Sz, Cs} <- OffSzCs]
          end || FLU <- FLUs],
    C1 = lists:append(C0),
    %% erlang:garbage_collect(),
    C2 = lists:sort(make_repair_compare_fun(Src), C1),
    %% erlang:garbage_collect(),
    Ds = make_repair_directives2(C2, ConsistencyMode, RepairMode,
                                 File, Verb, Src, FLUs, ProxiesDict, ETS),
    Ds.

make_repair_directives2(C2, ConsistencyMode, RepairMode,
                       File, Verb, Src, FLUs, ProxiesDict, ETS) ->
    make_repair_directives3(C2, ConsistencyMode, RepairMode,
                            File, Verb, Src, FLUs, ProxiesDict, ETS, []).

make_repair_directives3([{?MAX_OFFSET, 0, <<>>, _FLU}|_Rest],
                       _ConsistencyMode, _RepairMode,
                       _File, _Verb, _Src, _FLUs, _ProxiesDict, _ETS, Acc) ->
    lists:reverse(Acc);
make_repair_directives3([{Offset, Size, CSum, _FLU}=A|Rest0],
                       ConsistencyMode, RepairMode,
                       File, Verb, Src, FLUs, ProxiesDict, ETS, Acc) ->
    {As0, Rest1} = take_same_offset_size(Rest0, Offset, Size),
    As = [A|As0],
    %% Sanity checking time
    case lists:all(fun({_, _, Cs, _}) when Cs == CSum -> true;
                      (_)                             -> false
                   end, As) of
        true ->
            ok;
        false ->
            %% TODO: Pathology: someone has the wrong checksum.
            %% 1. Fetch Src's chunk.  If checksum is valid, use this chunk
            %%    to repair any invalid value.
            %% 2. If Src's chunk is invalid, then check for other copies
            %%    in the UPI.  If there is a valid chunk there, use it to
            %%    repair any invalid value.
            %% 3a. If there is no valid UPI chunk, then delete this
            %%     byte range from all FLUs
            %% 3b. Log big warning about data loss.
            %% 4. Log any other checksum discrepencies as they are found.
            QQ = [begin
                      Pxy = orddict:fetch(FLU, ProxiesDict),
                      {ok, EpochID} = machi_proxy_flu1_client:get_epoch_id(
                                        Pxy, ?SHORT_TIMEOUT),
                      NSInfo = undefined,
                      XX = machi_proxy_flu1_client:read_chunk(
                             Pxy, NSInfo, EpochID, File, Offset, Size, undefined,
                             ?SHORT_TIMEOUT),
                      {FLU, XX}
                  end || {__Offset, __Size, __CSum, FLU} <- As],

            exit({todo_repair_sanity_check, ?LINE, File, Offset, {as,As}, {qq,QQ}})
    end,
    %% List construction guarantees us that there's at least one ?MAX_OFFSET
    %% item remains.  Sort order + our "taking" of all exact Offset+Size
    %% tuples guarantees that if there's a disagreement about chunk size at
    %% this offset, we can look ahead exactly one to see if there is sanity
    %% or not.
    [{Offset_next, _Size_next, _, _}=A_next|_] = Rest1,
    if Offset + Size =< Offset_next ->
            ok;
       true ->
            exit({todo_repair_sanity_check, ?LINE, File, Offset, Size,
                  next_is, A_next})
    end,
    Do = if ConsistencyMode == ap_mode ->
                 Gots = [FLU || {_Off, _Sz, _Cs, FLU} <- As],
                 Missing = FLUs -- Gots,
                 _ThisSrc = case lists:member(Src, Gots) of
                               true  -> Src;
                               false -> hd(Gots)
                           end,
                 _ = [ets:update_counter(ETS, {directive_bytes, FLU_m}, Size) ||
                         FLU_m <- Missing],
                 if Missing == [] ->
                         noop;
                    true ->
                         {copy, A, Missing}
                 end
            %%      end;
            %% ConsistencyMode == cp_mode ->
            %%      exit({todo_cp_mode, ?MODULE, ?LINE})
         end,
    Acc2 = if Do == noop -> Acc;
              true       -> [Do|Acc]
           end,
    make_repair_directives3(Rest1,
                            ConsistencyMode, RepairMode,
                            File, Verb, Src, FLUs, ProxiesDict, ETS, Acc2).

take_same_offset_size(L, Offset, Size) ->
    take_same_offset_size(L, Offset, Size, []).

take_same_offset_size([{Offset, Size, _CSum, _FLU}=A|Rest], Offset, Size, Acc) ->
    take_same_offset_size(Rest, Offset, Size, [A|Acc]);
take_same_offset_size(Rest, _Offset, _Size, Acc) ->
    {Acc, Rest}.

execute_repair_directives(ap_mode=_ConsistencyMode, Ds, _Src, EpochID, Verb,
                          _OurFLUs, ProxiesDict, ETS) ->
    {_,_,_,_} = lists:foldl(fun execute_repair_directive/2,
                            {ProxiesDict, EpochID, Verb, ETS}, Ds),
    ok.

execute_repair_directive({File, Cmds}, {ProxiesDict, EpochID, _Verb, ETS}=Acc) ->
    EtsKeys = [{in_files, t_in_files}, {in_chunks, t_in_chunks},
               {in_bytes, t_in_bytes}, {out_files, t_out_files},
               {out_chunks, t_out_chunks}, {out_bytes, t_out_bytes}],
    [ets:insert(ETS, {L_K, 0}) || {L_K, _T_K} <- EtsKeys],
    F = fun({copy, {Offset, Size, TaggedCSum, MySrc}, MyDsts}, Acc2) ->
                SrcP = orddict:fetch(MySrc, ProxiesDict),
                %% case ets:lookup_element(ETS, in_chunks, 2) rem 100 of
                %%     0 -> ?VERB(".2", []);
                %%     _ -> ok
                %% end,
                _T1 = os:timestamp(),
                %% TODO: support case multiple written or trimmed chunks returned
                NSInfo = undefined,
                {ok, {[{_, Offset, Chunk, _ReadCSum}|OtherChunks], []=_TrimmedList}} =
                    machi_proxy_flu1_client:read_chunk(
                      SrcP, NSInfo, EpochID, File, Offset, Size, undefined,
                      ?SHORT_TIMEOUT),
                [] = OtherChunks,
                _T2 = os:timestamp(),
                <<_Tag:1/binary, CSum/binary>> = TaggedCSum,
                case machi_util:checksum_chunk(Chunk) of
                    CSum_now when CSum_now == CSum ->
                        _ = [begin
                                 DstP = orddict:fetch(DstFLU, ProxiesDict),
                                 _T3 = os:timestamp(),
                                 ok = machi_proxy_flu1_client:write_chunk(
                                        DstP, NSInfo, EpochID, File, Offset, Chunk, TaggedCSum,
                                        ?SHORT_TIMEOUT),
                                 _T4 = os:timestamp()
                             end || DstFLU <- MyDsts],
                        _ = ets:update_counter(ETS, in_chunks, 1),
                        _ = ets:update_counter(ETS, in_bytes, Size),
                        N = length(MyDsts),
                        _ = ets:update_counter(ETS, out_chunks, N),
                        _ = ets:update_counter(ETS, out_bytes, N*Size),
                        Acc2;
                    CSum_now ->
                        error_logger:error_msg(
                          "TODO: Checksum failure: "
                          "file ~p offset ~p size ~p: "
                          "expected ~p got ~p\n",
                          [File, Offset, Size, CSum, CSum_now]),
                        case ets:update_counter(ETS, t_bad_chunks, 1) of
                            N when N > 100 ->
                                throw(todo_wow_so_many_errors_so_verbose);
                            _ ->
                                ok
                        end,
                        Acc2
                end
        end,
    ok = lists:foldl(F, ok, Cmds),
    %% Copy this file's stats to the total counts.
    _ = [ets:update_counter(ETS, T_K, ets:lookup_element(ETS, L_K, 2)) ||
            {L_K, T_K} <- EtsKeys],
    Acc.

mbytes(N) ->
    machi_util:mbytes(N).

-ifdef(TEST).

repair_compare_fun_test() ->
    F = make_repair_compare_fun(b),
    List = [{{1,10,x,b},y},{{50,10,x,a},y},{{50,10,x,b},y},{{50,10,x,c},y},{{90,10,x,d},y}],
    Input = lists:reverse(lists:sort(List)),
    %% Although the merge func should never have two of the same FLU
    %% represented, it doesn't matter for the purposes of this test.
    %% 1. Smaller offset (element #1) wins, else...
    %% 2. The FLU (element #2) that's the repair source always wins, else...
    %% 3. The FLU with smallest name wins.
    Expect = [{{1,10,x,b},y},{{50,10,x,b},y},{{50,10,x,a},y},{{50,10,x,c},y},{{90,10,x,d},y}],
    Expect = lists:sort(F, Input).

-endif. % TEST
