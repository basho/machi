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

-module(machi_fitness).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

-define(LWWREG, riak_dt_lwwreg).
-define(MAP, riak_dt_map).

%% API
-export([start_link/1,
         get_unfit_list/1, update_local_down_list/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          name                   :: atom() | binary(),
          local_down=[]          :: list(),
          active_map=?MAP:new()  :: ?MAP:riak_dt_map(),
          pending_map=?MAP:new() :: ?MAP:riak_dt_map()
         }).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

get_unfit_list(PidSpec) ->
    gen_server:call(PidSpec, {get_unfit_list}, infinity).

update_local_down_list(PidSpec, Down) ->
    gen_server:call(PidSpec, {update_local_down_list, Down}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([{FluName}|_Args]) ->
    RegName = machi_flu_psup:make_fitness_regname(FluName),
    register(RegName, self()),
    {ok, #state{name=RegName}}.

handle_call({get_unfit_list}, _From, S) ->
    Unfit = make_unfit_list(S),
    io:format(user, "LINE ~p val ~p\n", [?LINE, Unfit]),
    {reply, Unfit, S};
handle_call({update_local_down_list, Down}, _From,
            #state{name=Name, active_map=OldMap, local_down=OldDown}=S) ->
    NewMap = store_in_map(OldMap, Name, erlang:now(), Down, [props_yo]),
    if Down == OldDown ->
            ok;
       true ->
            io:format(user, "fitness: ~w: ~w -> ~w\n", [Name, OldDown, Down]),
            io:format(user, "TODO: spam\n", []),
            io:format(user, "TODO: sched ticks, others...?\n", [])
    end,
    {reply, ok, S#state{local_down=Down, pending_map=NewMap}};
handle_call(_Request, _From, S) ->
    Reply = whhhhhhhhhhhhhhaaaaaaaaaaaaaaa,
    {reply, Reply, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_unfit_list(S) ->
    Now = erlang:now(),
    F = fun({Server, {UpdateTime, DownList, _Props}}, Acc) ->
                case timer:now_diff(Now, UpdateTime) div (1000*1000) of
                    N when N > 900 ->
                        Acc;
                    _ ->
                        Probs = [{Server,problem_with,D} || D <- DownList],
                        [Probs|Acc]
                end
        end,
    QQ1 = (catch map_fold(F, [], S#state.active_map)),
    QQ2 = (catch map_fold(F, [], S#state.pending_map)),
    io:format(user, "QQ1 ~p\n", [QQ1]),
    io:format(user, "QQ2 ~p\n", [QQ2]),
    S#state.local_down.

store_in_map(Map, Name, Now, Down, Props) ->
    Val = {Now, Down, Props},
    map_set(Name, Map, Name, Val).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

map_set(Actor, Map, Key, ValTerm) ->
    Field = {Key, ?LWWREG},
    Val = term_to_binary(ValTerm),
    {ok, Map2} = ?MAP:update({update, [{update, Field, {assign, Val}}]},
                             Actor, Map),
    Map2.

map_get(Map, Key) ->
    Field = {Key, ?LWWREG},
    case lists:keyfind(Field, 1, ?MAP:value(Map)) of
        false ->
            error;
        {Field, ValBin} ->
            {ok, binary_to_term(ValBin)}
    end.

map_fold(Fun, Acc, Map) ->
    Vs = [{K, binary_to_term(V)} || {{K, _Type}, V} <- ?MAP:value(Map)],
    lists:foldl(Fun, Acc, lists:sort(Vs)).

-ifdef(TEST).


dt_understanding_test() ->
    F1 = {'X', riak_dt_lwwreg},
    F2 = {'Y', riak_dt_lwwreg},
    {ok, Map1} = ?MAP:update({update, [{update, F1, {assign, <<"A">>}}]}, a, ?MAP:new()),
    {ok, Map2} = ?MAP:update({update, [{update, F2, {assign, <<"B2">>}}]}, b, ?MAP:new()),

    %% io:format(user, "\n", []),
    %% io:format(user, "Merge comparison: ~p\n", [?MAP:merge(Map1, Map2) == ?MAP:merge(Map2, Map1)]),
    %% io:format(user, "M12 Val: ~p\n", [?MAP:value(?MAP:merge(Map1, Map2))]),
    %% io:format(user, "M21 Val: ~p\n", [?MAP:value(?MAP:merge(Map2, Map1))]),
    ?MAP:merge(Map1, Map2) == ?MAP:merge(Map2, Map1).

smoke_test() ->
    Map1 = map_set(a, ?MAP:new(), k1, val1),
    Map2 = map_set(a, Map1, k2, val2),
    {ok, val1} = map_get(Map2, k1),
    {ok, val2} = map_get(Map2, k2),
    error = map_get(Map2, does_not_exist),
    Map3 = map_set(a, Map2, k3, val3),

    [{k3,1},{k2,1},{k1,1}] = map_fold(fun({K,_}, Acc) -> [{K,1}|Acc] end,
                                      [], Map3),
    ok.

-endif. % TEST
