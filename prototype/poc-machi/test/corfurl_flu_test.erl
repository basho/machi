
-module(corfurl_flu_test).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-include("corfurl.hrl").

-define(M, corfurl_flu).

-ifdef(TEST).
-ifndef(PULSE).

startstop_test() ->
    Dir = "/tmp/flu." ++ os:getpid(),
    {ok, P1} = ?M:start_link(Dir),
    try
        {ok, _} = ?M:status(P1),
        ok = ?M:stop(P1),
        {'EXIT', _} = (catch ?M:stop(P1)),

        {ok, P2} = ?M:start_link(Dir),
        0 = ?M:get__mlp(P2),
        0 = ?M:get__min_epoch(P2),
        ok = ?M:stop(P2),

        ok
    after
        ok = corfurl_util:delete_dir(Dir)
    end.

basic_test() ->
    Dir = "/tmp/flu." ++ os:getpid(),
    {ok, P1} = ?M:start_link(Dir),
    try
        Epoch1 = 1,
        Epoch2 = 2,
        Epoch3 = 3,
        LPN = 1,
        Bin1 = <<42:64>>,
        Bin2 = <<42042:64>>,

        error_unwritten = ?M:read(P1, Epoch1, LPN),
        error_unwritten = ?M:trim(P1, Epoch1, LPN),
        error_unwritten = ?M:trim(P1, Epoch1, LPN+77),

        ok = ?M:write(P1, Epoch1, LPN, Bin1),
        error_overwritten = ?M:write(P1, Epoch1, LPN, Bin1),
        error_overwritten = ?M:fill(P1, Epoch1, LPN),
        LPN = ?M:get__mlp(P1),
        0 = ?M:get__min_epoch(P1),
        0 = ?M:get__trim_watermark(P1),
        {ok, LPN} = ?M:seal(P1, Epoch1),
        2 = ?M:get__min_epoch(P1),

        error_overwritten = ?M:write(P1, Epoch2, LPN, Bin1),
        ok = ?M:write(P1, Epoch2, LPN+1, Bin2),
        Epoch2 = ?M:get__min_epoch(P1),

        error_badepoch = ?M:read(P1, Epoch1, LPN),
        {ok, Bin2} = ?M:read(P1, Epoch2, LPN+1),
        error_unwritten = ?M:read(P1, Epoch2, LPN+2),
        badarg = ?M:read(P1, Epoch2, 1 bsl 2982),

        error_badepoch = ?M:seal(P1, Epoch1),
        {ok, _} = ?M:seal(P1, Epoch2),
        error_badepoch = ?M:seal(P1, Epoch2),

        error_badepoch = ?M:read(P1, Epoch1, LPN),
        error_badepoch = ?M:read(P1, Epoch1, LPN+1),
        {ok, Bin1} = ?M:read(P1, Epoch3, LPN),
        {ok, Bin2} = ?M:read(P1, Epoch3, LPN+1),

        error_badepoch = ?M:trim(P1, Epoch1, LPN+1),
        ok = ?M:trim(P1, Epoch3, LPN+1),
        error_trimmed = ?M:trim(P1, Epoch3, LPN+1),
        %% Current watermark processing is broken.  But we'll test what's
        %% there now.
        ExpectedWaterFixMe = LPN+1,
        ExpectedWaterFixMe = ?M:get__trim_watermark(P1),

        ok = ?M:fill(P1, Epoch3, LPN+3),
        error_trimmed = ?M:read(P1, Epoch3, LPN+3),
        error_trimmed = ?M:fill(P1, Epoch3, LPN+3),
        error_trimmed = ?M:trim(P1, Epoch3, LPN+3),

        Epoch3 = ?M:get__min_epoch(P1),
        ok = ?M:stop(P1),
        ok
    after
        ok = corfurl_util:delete_dir(Dir)
    end.

seal_persistence_test() ->
    Dir = "/tmp/flu." ++ os:getpid(),
    {ok, P1} = ?M:start_link(Dir),
    try
        0 = ?M:get__min_epoch(P1),
        Epoch = 665,
        {ok, LPN} = ?M:seal(P1, Epoch-1),
        Epoch = ?M:get__min_epoch(P1),
        ok = ?M:stop(P1),

        {ok, P2} = ?M:start_link(Dir),
        Epoch = ?M:get__min_epoch(P2),

        ok = ?M:stop(P2),
        ok
    after
        ok = corfurl_util:delete_dir(Dir)
    end.

-endif. % not PULSE
-endif. % TEST
