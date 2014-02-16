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

-module(corfurl_sequencer).

-behaviour(gen_server).

-export([start_link/1, stop/1, get/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.
-endif.

-define(SERVER, ?MODULE).

start_link(FLUs) ->
    %% gen_server:start_link({local, ?SERVER}, ?MODULE, {FLUs}, []).
    gen_server:start_link(?MODULE, {FLUs}, []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

get(Pid, NumPages) ->
    gen_server:call(Pid, {get, NumPages}, infinity).

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

init({FLUs}) ->
    MLP = get_max_logical_page(FLUs),
    io:format(user, "~s:init: MLP = ~p\n", [?MODULE, MLP]),
    {ok, MLP + 1}.

handle_call({get, NumPages}, _From, MLP) ->
    {reply, MLP, MLP + NumPages};
handle_call(stop, _From, MLP) ->
    {stop, normal, ok, MLP};
handle_call(_Request, _From, MLP) ->
    Reply = whaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,
    {reply, Reply, MLP}.

handle_cast(_Msg, MLP) ->
    {noreply, MLP}.

handle_info(_Info, MLP) ->
    {noreply, MLP}.

terminate(_Reason, _MLP) ->
    ok.

code_change(_OldVsn, MLP, _Extra) ->
    {ok, MLP}.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

get_max_logical_page(FLUs) ->
    lists:max([proplists:get_value(max_logical_page, Ps, 0) ||
                  FLU <- FLUs,
                  {ok, Ps} <- [corfurl_flu:status(FLU)]]).

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

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
        NumFLUs = get_max_logical_page(FLUs),

        %% Excellent.  Now let's start the sequencer and see if it gets
        %% the same answer.  If yes, then the first get will return MLP1,
        %% yadda yadda.
        MLP1 = MLP0 + 1,
        MLP3 = MLP0 + 3,
        MLP4 = MLP0 + 4,
        {ok, Sequencer} = start_link(FLUs),
        try
            MLP1 = get(Sequencer, 2),
            MLP3 = get(Sequencer, 1),
            MLP4 = get(Sequencer, 1)
        after
            stop(Sequencer)
        end
    after
        [ok = corfurl_flu:stop(FLU) || FLU <- FLUs],
        Del()
    end.

-endif. % not PULSE
-endif. % TEST
