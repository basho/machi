-module(machi_csum_table).

-export([open/2,
         find/3,
         write/6, write/4, trim/5,
         find_leftneighbor/2, find_rightneighbor/2,
         all_trimmed/3, any_trimmed/3,
         all_trimmed/2,
         calc_unwritten_bytes/1,
         split_checksum_list_blob_decode/1,
         all/1,
         close/1, delete/1,
         foldl_chunks/3]).

-include("machi.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(machi_csum_table,
        {file :: string(),
         table :: eleveldb:db_ref()}).

-type table() :: #machi_csum_table{}.
-type byte_sequence() :: { Offset :: non_neg_integer(),
                           Size   :: pos_integer()|infinity }.
-type chunk() :: {Offset :: machi_dt:file_offset(),
                  Size :: machi_dt:chunk_size(),
                  machi_dt:chunk_csum() | trimmed | none}.

-export_type([table/0]).

-spec open(string(), proplists:proplist()) ->
                  {ok, table()} | {error, file:posix()}.

open(CSumFilename, _Opts) ->
    LevelDBOptions = [{create_if_missing, true},
                      %% Keep this table small so as not to interfere
                      %% operating system's file cache, which is for
                      %% Machi's main read efficiency
                      {total_leveldb_mem_percent, 10}],
    {ok, T} = eleveldb:open(CSumFilename, LevelDBOptions),
    %% Dummy entry for reserved headers
    ok = eleveldb:put(T,
                      sext:encode({0, ?MINIMUM_OFFSET}),
                      sext:encode(?CSUM_TAG_NONE_ATOM),
                      [{sync, true}]),
    C0 = #machi_csum_table{
            file=CSumFilename,
            table=T},
    {ok, C0}.

-spec split_checksum_list_blob_decode(binary())-> [chunk()].
split_checksum_list_blob_decode(Bin) ->
    erlang:binary_to_term(Bin).


-define(has_overlap(LeftOffset, LeftSize, RightOffset, RightSize),
        ((LeftOffset - (RightOffset+RightSize)) * (LeftOffset+LeftSize - RightOffset) < 0)).

-spec find(table(), machi_dt:file_offset(), machi_dt:chunk_size())
          -> [chunk()].
find(#machi_csum_table{table=T}, Offset, Size) ->
    {ok, I} = eleveldb:iterator(T, [], keys_only),
    EndKey = sext:encode({Offset+Size, 0}),
    StartKey = sext:encode({Offset, Size}),

    {ok, FirstKey} = case eleveldb:iterator_move(I, StartKey) of
                         {error, invalid_iterator} ->
                             eleveldb:iterator_move(I, first);
                         {ok, _} = R0 ->
                             case eleveldb:iterator_move(I, prev) of
                                 {error, invalid_iterator} ->
                                     R0;
                                 {ok, _} = R1 ->
                                     R1
                             end
                     end,
    _ = eleveldb:iterator_close(I),
    FoldFun = fun({K, V}, Acc) ->
                      {TargetOffset, TargetSize} = sext:decode(K),
                      case ?has_overlap(TargetOffset, TargetSize, Offset, Size) of
                          true ->
                              [{TargetOffset, TargetSize, sext:decode(V)}|Acc];
                          false ->
                              Acc
                      end;
                 (_K, Acc) ->
                      lager:error("~p wrong option", [_K]),
                      Acc
              end,
    lists:reverse(eleveldb_fold(T, FirstKey, EndKey, FoldFun, [])).


%% @doc Updates all chunk info, by deleting existing entries if exists
%% and putting new chunk info
-spec write(table(),
            machi_dt:file_offset(), machi_dt:chunk_size(),
            machi_dt:chunk_csum()|'none'|'trimmed',
            undefined|chunk(), undefined|chunk()) ->
                   ok | {error, term()}.
write(#machi_csum_table{table=T} = CsumT, Offset, Size, CSum,
      LeftUpdate, RightUpdate) ->
    PutOps =
        [{put,
          sext:encode({Offset, Size}),
          sext:encode(CSum)}]
        ++ case LeftUpdate of
               {LO, LS, LCsum} when LO + LS =:= Offset ->
                   [{put,
                     sext:encode({LO, LS}),
                     sext:encode(LCsum)}];
               undefined ->
                   []
           end
        ++ case RightUpdate of
               {RO, RS, RCsum} when RO =:= Offset + Size ->
                   [{put,
                     sext:encode({RO, RS}),
                     sext:encode(RCsum)}];
               undefined ->
                   []
           end,
    Chunks = find(CsumT, Offset, Size),
    DeleteOps = lists:map(fun({O, L, _}) ->
                                  {delete, sext:encode({O, L})}
                          end, Chunks),
    eleveldb:write(T, DeleteOps ++ PutOps, [{sync, true}]).

-spec find_leftneighbor(table(), non_neg_integer()) ->
                               undefined | chunk().
find_leftneighbor(CsumT, Offset) ->
    case find(CsumT, Offset, 1) of
        [] -> undefined;
        [{Offset, _, _}] -> undefined;
        [{LOffset, _, CsumOrTrimmed}] -> {LOffset, Offset - LOffset, CsumOrTrimmed}
    end.

-spec find_rightneighbor(table(), non_neg_integer()) ->
                                undefined | chunk().
find_rightneighbor(CsumT, Offset) ->
    case find(CsumT, Offset, 1) of
        [] -> undefined;
        [{Offset, _, _}] -> undefined;
        [{ROffset, RSize, CsumOrTrimmed}] ->
            {Offset, ROffset + RSize - Offset, CsumOrTrimmed}
    end.

-spec write(table(), machi_dt:file_offset(), machi_dt:file_size(),
            machi_dt:chunk_csum()|none|trimmed) ->
                   ok | {error, trimmed|file:posix()}.
write(CsumT, Offset, Size, CSum) ->
    write(CsumT, Offset, Size, CSum, undefined, undefined).

trim(CsumT, Offset, Size, LeftUpdate, RightUpdate) ->
    write(CsumT, Offset, Size,
          trimmed, %% Should this be much smaller like $t or just 't'
          LeftUpdate, RightUpdate).

%% @doc returns whether all bytes in a specific window is continously
%% trimmed or not
-spec all_trimmed(table(), non_neg_integer(), non_neg_integer()) -> boolean().
all_trimmed(#machi_csum_table{table=T}, Left, Right) ->
    FoldFun = fun({_, _}, false) ->
                      false;
                 ({K, V}, Pos) when is_integer(Pos) andalso Pos =< Right ->
                      case {sext:decode(K), sext:decode(V)} of
                          {{Pos, Size}, trimmed} ->
                              Pos + Size;
                          {{Offset, Size}, _}
                            when Offset + Size =< Left ->
                              Left;
                          _Eh ->
                              false
                      end
              end,
    case eleveldb:fold(T, FoldFun, Left, [{verify_checksums, true}]) of
        false -> false;
        Right -> true;
        LastTrimmed when LastTrimmed < Right -> false;
        _ -> %% LastTrimmed > Pos0, which is a irregular case but ok
            true
    end.

%% @doc returns whether all bytes 0-Pos0 is continously trimmed or
%% not, including header.
-spec all_trimmed(table(), non_neg_integer()) -> boolean().
all_trimmed(CsumT, Pos0) ->
    all_trimmed(CsumT, 0, Pos0).

-spec any_trimmed(table(),
                  pos_integer(),
                  machi_dt:chunk_size()) -> boolean().
any_trimmed(CsumT, Offset, Size) ->
    Chunks = find(CsumT, Offset, Size),
    lists:any(fun({_, _, State}) -> State =:= trimmed end, Chunks).

-spec calc_unwritten_bytes(table()) -> [byte_sequence()].
calc_unwritten_bytes(#machi_csum_table{table=_} = CsumT) ->
    case lists:sort(all(CsumT)) of
        [] ->
            [{?MINIMUM_OFFSET, infinity}];
        Sorted ->
            {LastOffset, _, _} = hd(Sorted),
            build_unwritten_bytes_list(Sorted, LastOffset, [])
    end.

all(CsumT) ->
    FoldFun = fun(E, Acc) -> [E|Acc] end,
    lists:reverse(foldl_chunks(FoldFun, [], CsumT)).

-spec close(table()) -> ok.
close(#machi_csum_table{table=T}) ->
    ok = eleveldb:close(T).

-spec delete(table()) -> ok.
delete(#machi_csum_table{table=T, file=F}) ->
    catch eleveldb:close(T),
    %% TODO change this to directory walk
    case os:cmd("rm -rf " ++ F) of
        "" -> ok;
        E -> E
    end.

-spec foldl_chunks(fun((chunk(),  Acc0 :: term()) -> Acc :: term()),
                   Acc0 :: term(), table()) -> Acc :: term().
foldl_chunks(Fun, Acc0, #machi_csum_table{table=T}) ->
    FoldFun = fun({K, V}, Acc) ->
                      {Offset, Len} = sext:decode(K),
                      Fun({Offset, Len, sext:decode(V)}, Acc);
                 (_K, Acc) ->
                      _ = lager:error("~p: wrong option?", [_K]),
                      Acc
              end,
    eleveldb:fold(T, FoldFun, Acc0, [{verify_checksums, true}]).

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

%% @doc If you want to find an overlap among two areas [x, y] and [a,
%% b] where x &lt; y and a &lt; b; if (a-y)*(b-x) &lt; 0 then there's a
%% overlap, else, > 0 then there're no overlap. border condition = 0
%% is not overlap in this offset-size case.
%% inclusion_match_spec(Offset, Size) ->
%%     {'>', 0,
%%      {'*',
%%       {'-', Offset + Size, '$1'},
%%       {'-', Offset, {'+', '$1', '$2'}}}}.

-spec eleveldb_fold(eleveldb:db_ref(), binary(), binary(),
                    fun(({binary(), binary()}, AccType::term()) -> AccType::term()),
                    AccType0::term()) ->
                           AccType::term().
eleveldb_fold(Ref, Start, End, FoldFun, InitAcc) ->
    {ok, Iterator} = eleveldb:iterator(Ref, []),
    try
        eleveldb_do_fold(eleveldb:iterator_move(Iterator, Start),
                         Iterator, End, FoldFun, InitAcc)
    catch throw:IteratorClosed ->
            {error, IteratorClosed}
    after
        eleveldb:iterator_close(Iterator)
    end.

-spec eleveldb_do_fold({ok, binary(), binary()}|{error, iterator_closed|invalid_iterator}|{ok,binary()},
                       eleveldb:itr_ref(), binary(),
                       fun(({binary(), binary()}, AccType::term()) -> AccType::term()),
                       AccType::term()) ->
                              AccType::term().
eleveldb_do_fold({ok, Key, Value}, _, End, FoldFun, Acc)
  when End < Key ->
    FoldFun({Key, Value}, Acc);
eleveldb_do_fold({ok, Key, Value}, Iterator, End, FoldFun, Acc) ->
    eleveldb_do_fold(eleveldb:iterator_move(Iterator, next),
                     Iterator, End, FoldFun,
                     FoldFun({Key, Value}, Acc));
eleveldb_do_fold({error, iterator_closed}, _, _, _, Acc) ->
            %% It's really an error which is not expected
    throw({iterator_closed, Acc});
eleveldb_do_fold({error, invalid_iterator}, _, _, _, Acc) ->
    %% Probably reached to end
    Acc.
