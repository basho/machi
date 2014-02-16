
-module(corfurl_util).

-export([delete_dir/1]).

-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

delete_dir(Dir) ->
    %% We don't recursively delete directories, the ok pattern match will fail.
    [ok = file:delete(X) || X <- filelib:wildcard(Dir ++ "/*")],
    case file:del_dir(Dir) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        Else ->
            Else
    end.

