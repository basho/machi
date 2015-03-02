%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

        {ok, Sequencer} = ?M:start_link(FLUs),
        try
            {ok, _} = ?M:get(Sequencer, 5000),
            [{Stream9, Tails9}] = StreamTails = [{9, [1125, 1124, 1123]}],
            ok = ?M:set_tails(Sequencer, StreamTails),
            {ok, _, [Tails9]} = ?M:get_tails(Sequencer, 0, [Stream9]),

            {ok, LPN0a} = ?M:get(Sequencer, 2),
            {ok, LPN0b} = ?M:get(Sequencer, 0),
            LPN0a = LPN0b - 2,
            
            {ok, LPN2a, _} = ?M:get_tails(Sequencer, 1, [2]),
            {ok, LPN1a, _} = ?M:get_tails(Sequencer, 1, [1]),
            {ok, _, [[LPN1a], [LPN2a]]} = ?M:get_tails(Sequencer,
                                                       0, [1,2]),
            {ok, LPN2b, _} = ?M:get_tails(Sequencer, 1, [2]),
            {ok, LPN2c, _} = ?M:get_tails(Sequencer, 1, [2]),
            {ok, _, [[LPN1a], [LPN2c, LPN2b, LPN2a]]} =
                ?M:get_tails(Sequencer, 0, [1,2]),
            {ok, LPN2d, _} = ?M:get_tails(Sequencer, 1, [2]),
            {ok, LPN2e, _} = ?M:get_tails(Sequencer, 1, [2]),

            {ok, LPNX, [[LP1a], [LPN2e, LPN2d, LPN2c, LPN2b]]} =
                ?M:get_tails(Sequencer, 0, [1,2]),
            {ok, LPNX, [[LP1a], [LPN2e, LPN2d, LPN2c, LPN2b]]} =
                ?M:get_tails(Sequencer, 0, [1,2]), % same results
            LPNX = LPN2e + 1,                      % no change with 0 request

            ok
        after
            ?M:stop(Sequencer)
        end
    after
        [ok = corfurl_flu:stop(FLU) || FLU <- FLUs],
        Del()
    end.

-endif. % not PULSE
-endif. % TEST
