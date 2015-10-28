-module(machi_plist).

%%% @doc persistent list of binaries that support mutual exclusion

-export([open/2, close/1, find/2, add/2]).

-ifdef(TEST).
-export([all/1]).
-endif.

-record(machi_plist,
        {filename :: file:filename_all(),
         fd :: file:io_device(),
         list = [] :: list(string)}).

-type plist() :: #machi_plist{}.
-export_type([plist/0]).

-spec open(file:filename_all(), proplists:proplist()) ->
                  {ok, plist()} | {error, file:posix()}.
open(Filename, _Opt) ->
    List = case file:read_file(Filename) of
               {ok, <<>>} -> [];
               {ok, Bin} -> binary_to_term(Bin);
               {error, enoent} -> []
           end,
    case file:open(Filename, [read, write, raw, binary, sync]) of
        {ok, Fd} ->
            {ok, #machi_plist{filename=Filename,
                              fd=Fd,
                              list=List}};
        Error ->
            Error
    end.

-spec close(plist()) -> ok.
close(#machi_plist{fd=Fd}) ->
    _ = file:close(Fd).

-spec find(plist(), string()) -> boolean().
find(#machi_plist{list=List}, Name) ->
    lists:member(Name, List).

-spec add(plist(), string()) -> {ok, plist()} | {error, file:posix()}.
add(Plist = #machi_plist{list=List0, fd=Fd}, Name) ->
    case find(Plist, Name) of
        true ->
            {ok, Plist};
        false ->
            List = lists:append(List0, [Name]),
            case file:pwrite(Fd, 0, term_to_binary(List)) of
                ok ->
                    {ok, Plist#machi_plist{list=List}};
                Error ->
                    Error
            end
    end.

-ifdef(TEST).
-spec all(plist()) -> [file:filename()].
all(#machi_plist{list=List}) ->
    List.
-endif.
