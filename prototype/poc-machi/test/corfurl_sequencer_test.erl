
-module(corfurl_sequencer_test).

-compile(export_all).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.
-endif.

-define(M, corfurl_sequencer).

-ifdef(TEST).
-ifndef(PULSE).

smoke_test() ->
    BaseDir = "/tmp/" ++ atom_to_list(?MODULE) ++ ".",
    PageSize = 8,
    NumPages = 500,
    NumFLUs = 4,
    MyDir = fun(X) -> BaseDir ++ integer_to_list(X) end,
    Del = fun() -> [ok = corfurl_util:delete_dir(MyDir(X)) ||
                       X <- lists:seq(1, NumFLUs)] end,

    Del(),
    FLUs = [begin
                element(2, corfurl_flu:start_link(MyDir(X),
                                                  PageSize, NumPages*PageSize))
            end || X <- lists:seq(1, NumFLUs)],
    FLUsNums = lists:zip(FLUs, lists:seq(1, NumFLUs)),
    
    try
        [ok = corfurl_flu:write(FLU, 1, PageNum, <<42:(8*8)>>) ||
            {FLU, PageNum} <- FLUsNums],
        MLP0 = NumFLUs,
        NumFLUs = ?M:get_max_logical_page(FLUs),

        %% Excellent.  Now let's start the sequencer and see if it gets
        %% the same answer.  If yes, then the first get will return MLP1,
        %% yadda yadda.
        MLP1 = MLP0 + 1,
        MLP3 = MLP0 + 3,
        MLP4 = MLP0 + 4,
        {ok, Sequencer} = ?M:start_link(FLUs),
        try
            {ok, MLP1} = ?M:get(Sequencer, 2),
            {ok, MLP3} = ?M:get(Sequencer, 1),
            {ok, MLP4} = ?M:get(Sequencer, 1)
        after
            ?M:stop(Sequencer)
        end
    after
        [ok = corfurl_flu:stop(FLU) || FLU <- FLUs],
        Del()
    end.

-endif. % not PULSE
-endif. % TEST
