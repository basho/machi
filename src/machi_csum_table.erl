-module(machi_csum_table).

-export([open/2,
         find/3,
         write/6, write/4, trim/5, trim/3,
         find_leftneighbor/2, find_rightneighbor/2,
         all_trimmed/3, any_trimmed/3,
         all_trimmed/2,
         sync/1,
         calc_unwritten_bytes/1,
         split_checksum_list_blob_decode/1,
         close/1, delete/1,
         foldl_chunks/3]).

-export([encode_csum_file_entry/3, encode_csum_file_entry_bin/3,
         decode_csum_file_entry/1]).

-include("machi.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([all/1]).
-endif.

-record(machi_csum_table,
        {file :: string(),
         fd   :: file:io_device(),
         table :: ets:tid()}).

-type table() :: #machi_csum_table{}.
-type byte_sequence() :: { Offset :: non_neg_integer(),
                           Size   :: pos_integer()|infinity }.

-export_type([table/0]).

-spec open(string(), proplists:proplist()) ->
                  {ok, table()} | {error, file:posix()}.

open(CSumFilename, _Opts) ->
    T = ets:new(?MODULE, [private, ordered_set]),
    CSum = machi_util:make_tagged_csum(none),
    %% Dummy entry for headers
    true = ets:insert_new(T, {0, ?MINIMUM_OFFSET, CSum}),
    C0 = #machi_csum_table{
            file=CSumFilename,
            table=T},
    case file:read_file(CSumFilename) of
        {ok, Bin} ->
            List = case split_checksum_list_blob_decode(Bin) of
                       {List0, <<>>} ->
                           List0;
                       {List0, _Junk} ->
                           %% Partially written, needs repair TODO
                           List0
                   end,
            %% assuming all entries are strictly ordered by offset,
            %% trim command should always come after checksum entry.
            %% *if* by any chance that order cound not be kept, we
            %% still can do ordering check and monotonic merge here.
            %% TODO: make some random injection tests?
            [begin %% Replay all file contents, Same logic as in write/6
                 Chunks = find(C0, Offset, Size),
                 lists:foreach(fun({O, _, _}) ->
                                       ets:delete(T, O)
                               end, Chunks),
                 true = ets:insert(T, {Offset, Size, CsumOrTrimmed})
             end
             || {Offset, Size, CsumOrTrimmed} <- List],
            ok;
        {error, enoent} ->
            ok;
        Error ->
            throw(Error)
    end,
    {ok, Fd} = file:open(CSumFilename, [raw, binary, append]),
    {ok, C0#machi_csum_table{fd=Fd}}.

-spec find(table(), machi_dt:file_offset(), machi_dt:file_size()) ->
                  list({machi_dt:file_offset(),
                        machi_dt:file_size(),
                        machi_dt:chunk_csum()|trimmed}).
find(#machi_csum_table{table=T}, Offset, Size) ->
    ets:select(T, [{{'$1', '$2', '$3'},
                    [inclusion_match_spec(Offset, Size)],
                    ['$_']}]).

-ifdef(TEST).
all(#machi_csum_table{table=T}) ->
    ets:tab2list(T).
-endif.

write(#machi_csum_table{fd=Fd, table=T} = CsumT,
      Offset, Size, CSum,
      LeftUpdate, RightUpdate) ->
    Binary =
        [encode_csum_file_entry_bin(Offset, Size, CSum),
         case LeftUpdate of
             {LO, LS, LCsum} when LO + LS =:= Offset ->
                 encode_csum_file_entry_bin(LO, LS, LCsum);
             undefined ->
                 <<>>
         end,
         case RightUpdate of
             {RO, RS, RCsum} when RO =:= Offset + Size ->
                 encode_csum_file_entry_bin(RO, RS, RCsum);
             undefined ->
                 <<>>
         end],
    case file:write(Fd, Binary) of
        ok ->
            Chunks = find(CsumT, Offset, Size),
            lists:foreach(fun({O, _, _}) ->
                                  ets:delete(T, O)
                          end, Chunks),
            case LeftUpdate of
                {LO1, LS1, _} when LO1 + LS1 =:= Offset ->
                    ets:insert(T, LeftUpdate);
                undefined -> noop
            end,
            case RightUpdate of
                {RO1, _, _} when RO1 =:= Offset + Size ->
                    ets:insert(T, RightUpdate);
                undefined -> noop
            end,
            true = ets:insert(T, {Offset, Size, CSum}),
            ok;
        Error ->
            io:format(user, "boob *********************", []),
            Error
    end.

-spec find_leftneighbor(table(), non_neg_integer()) ->
                               undefined |
                               {non_neg_integer(), machi_dt:chunk_size(), trimmed|machi_dt:chunk_csum()}.
find_leftneighbor(CsumT, Offset) ->
    case find(CsumT, Offset, 1) of
        [] -> undefined;
        [{Offset, _, _}] -> undefined;
        [{LOffset, _, CsumOrTrimmed}] -> {LOffset, Offset - LOffset, CsumOrTrimmed}
    end.

-spec find_rightneighbor(table(), non_neg_integer()) ->
                                undefined |
                                {non_neg_integer(), machi_dt:chunk_size(), trimmed|machi_dt:chunk_csum()}.
find_rightneighbor(CsumT, Offset) ->
    case find(CsumT, Offset, 1) of
        [] -> undefined;
        [{Offset, _, _}] -> undefined;
        [{ROffset, RSize, CsumOrTrimmed}] ->
            {Offset, ROffset + RSize - Offset, CsumOrTrimmed}
    end.

-spec write(table(), machi_dt:file_offset(), machi_dt:file_size(),
            machi_dt:chunk_csum()|trimmed) ->
                   ok | {error, trimmed|file:posix()}.
write(CsumT, Offset, Size, CSum) ->
    write(CsumT, Offset, Size, CSum, undefined, undefined).

trim(CsumT, Offset, Size, LeftUpdate, RightUpdate) ->
    write(CsumT, Offset, Size, trimmed, LeftUpdate, RightUpdate).

-spec trim(table(), machi_dt:file_offset(), machi_dt:file_size()) ->
                  ok | {error, file:posix()}.
trim(#machi_csum_table{fd=Fd, table=T}, Offset, Size) ->
    Binary = encode_csum_file_entry_bin(Offset, Size, trimmed),
    case file:write(Fd, Binary) of
        ok ->
            true = ets:insert(T, {Offset, Size, trimmed}),
            ok;
        Error ->
            Error
    end.

-spec all_trimmed(table(), non_neg_integer(), non_neg_integer()) -> boolean().
all_trimmed(#machi_csum_table{table=T}, Left, Right) ->
    runthru(ets:tab2list(T), Left, Right).

-spec all_trimmed(table(), non_neg_integer()) -> boolean().
all_trimmed(#machi_csum_table{table=T}, Pos) ->
    case ets:tab2list(T) of
        [{0, ?MINIMUM_OFFSET, _}|L] ->
            %% tl/1 to remove header space {0, 1024, <<0>>}
            runthru(L, ?MINIMUM_OFFSET, Pos);
        List ->
            %% In case a header is removed;
            runthru(List, 0, Pos)
    end.

-spec any_trimmed(table(),
                  pos_integer(),
                  machi_dt:chunk_size()) -> boolean().
any_trimmed(CsumT, Offset, Size) ->
    Chunks = find(CsumT, Offset, Size),
    lists:any(fun({_, _, State}) -> State =:= trimmed end, Chunks).

-spec sync(table()) -> ok | {error, file:posix()}.
sync(#machi_csum_table{fd=Fd}) ->
    file:sync(Fd).

-spec calc_unwritten_bytes(table()) -> [byte_sequence()].
calc_unwritten_bytes(#machi_csum_table{table=T}) ->
    case lists:sort(ets:tab2list(T)) of
        [] ->
            [{?MINIMUM_OFFSET, infinity}];
        Sorted ->
            {LastOffset, _, _} = hd(Sorted),
            build_unwritten_bytes_list(Sorted, LastOffset, [])
    end.

-spec close(table()) -> ok.
close(#machi_csum_table{table=T, fd=Fd}) ->
    true = ets:delete(T),
    ok = file:close(Fd).

-spec delete(table()) -> ok.
delete(#machi_csum_table{file=F} = C) ->
    catch close(C),
    case file:delete(F) of
        ok -> ok;
        {error, enoent} -> ok;
        E -> E
    end.

-spec foldl_chunks(fun(({non_neg_integer(), non_neg_integer(), term()},
                        Acc0 :: term())
                       -> Acc :: term()),
                   Acc0 :: term(), table()) -> Acc :: term().
foldl_chunks(Fun, Acc0, #machi_csum_table{table=T}) ->
    ets:foldl(Fun, Acc0, T).

%% @doc Encode `Offset + Size + TaggedCSum' into an `iolist()' type for
%% internal storage by the FLU.

-spec encode_csum_file_entry(
        machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()) ->
        iolist().
encode_csum_file_entry(Offset, Size, TaggedCSum) ->
    Len = 8 + 4 + byte_size(TaggedCSum),
    [<<$w, Len:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big>>,
     TaggedCSum].

%% @doc Encode `Offset + Size + TaggedCSum' into an `binary()' type for
%% internal storage by the FLU.

-spec encode_csum_file_entry_bin(
        machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()) ->
        binary().
encode_csum_file_entry_bin(Offset, Size, trimmed) ->
    <<$t, Offset:64/unsigned-big, Size:32/unsigned-big>>;
encode_csum_file_entry_bin(Offset, Size, TaggedCSum) ->
    Len = 8 + 4 + byte_size(TaggedCSum),
    <<$w, Len:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big,
      TaggedCSum/binary>>.

%% @doc Decode a single `binary()' blob into an
%%      `{Offset,Size,TaggedCSum}' tuple.
%%
%% The internal encoding (which is currently exposed to the outside world
%% via this function and related ones) is:
%%
%% <ul>
%% <li> 1 byte: record length
%% </li>
%% <li> 8 bytes (unsigned big-endian): byte offset
%% </li>
%% <li> 4 bytes (unsigned big-endian): chunk size
%% </li>
%% <li> all remaining bytes: tagged checksum (1st byte = type tag)
%% </li>
%% </ul>
%%
%% See `machi.hrl' for the tagged checksum types, e.g.,
%% `?CSUM_TAG_NONE'.

-spec decode_csum_file_entry(binary()) ->
        error |
        {machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()}.
decode_csum_file_entry(<<_:8/unsigned-big, Offset:64/unsigned-big, Size:32/unsigned-big, TaggedCSum/binary>>) ->
    {Offset, Size, TaggedCSum};
decode_csum_file_entry(_Else) ->
    error.

%% @doc Split a `binary()' blob of `checksum_list' data into a list of
%% `{Offset,Size,TaggedCSum}' tuples.

-spec split_checksum_list_blob_decode(binary()) ->
  {list({machi_dt:file_offset(), machi_dt:chunk_size(), machi_dt:chunk_s()}),
   TrailingJunk::binary()}.
split_checksum_list_blob_decode(Bin) ->
    split_checksum_list_blob_decode(Bin, []).

split_checksum_list_blob_decode(<<$w, Len:8/unsigned-big, Part:Len/binary, Rest/binary>>, Acc)->
    One = <<Len:8/unsigned-big, Part/binary>>,
    case decode_csum_file_entry(One) of
        error ->
            split_checksum_list_blob_decode(Rest, Acc);
        DecOne ->
            split_checksum_list_blob_decode(Rest, [DecOne|Acc])
    end;
split_checksum_list_blob_decode(<<$t, Offset:64/unsigned-big, Size:32/unsigned-big, Rest/binary>>, Acc) ->
    %% trimmed offset
    split_checksum_list_blob_decode(Rest, [{Offset, Size, trimmed}|Acc]);
split_checksum_list_blob_decode(Rest, Acc) ->
    {lists:reverse(Acc), Rest}.

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

%% @doc make sure all trimmed chunks are continously chained
%% TODO: test with EQC
runthru([], Pos, Pos) -> true;
runthru([], Pos0, Pos) when Pos0 < Pos -> false;
runthru([{Offset0, Size0, trimmed}|T], Offset, Pos) when Offset0 =< Offset ->
    runthru(T, Offset0+Size0, Pos);
runthru(_L, _O, _P) ->
    false.

%% @doc If you want to find an overlap among two areas [x, y] and [a,
%% b] where x < y and a < b; if (a-y)*(b-x) < 0 then there's a
%% overlap, else, > 0 then there're no overlap. border condition = 0
%% is not overlap in this offset-size case.
inclusion_match_spec(Offset, Size) ->
    {'>', 0,
     {'*',
      {'-', Offset + Size, '$1'},
      {'-', Offset, {'+', '$1', '$2'}}}}.
