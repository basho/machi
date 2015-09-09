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

-include("machi_projection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

-define(LWWREG, riak_dt_lwwreg).
-define(MAP, riak_dt_map).

%% API
-export([start_link/1,
         get_unfit_list/1, update_local_down_list/3,
         send_fitness_update_spam/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          my_flu_name                :: atom() | binary(),
          reg_name                   :: atom(),
          local_down=[]              :: list(),
          members_dict=orddict:new() :: orddict:orddict(),
          active_unfit=[]            :: list(),
          pending_map=?MAP:new()     :: ?MAP:riak_dt_map()
         }).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

get_unfit_list(PidSpec) ->
    gen_server:call(PidSpec, {get_unfit_list}, infinity).

update_local_down_list(PidSpec, Down, MembersDict) ->
    gen_server:call(PidSpec, {update_local_down_list, Down, MembersDict},
                    infinity).

send_fitness_update_spam(Pid, FromName, Dict) ->
    gen_server:call(Pid, {incoming_spam, FromName, Dict}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([{MyFluName}|_Args]) ->
    RegName = machi_flu_psup:make_fitness_regname(MyFluName),
    register(RegName, self()),
timer:send_interval(1000, dump),
    {ok, #state{my_flu_name=MyFluName, reg_name=RegName}}.

handle_call({get_unfit_list}, _From, #state{active_unfit=ActiveUnfit}=S) ->
    Reply = ActiveUnfit,
    {reply, Reply, S};
handle_call({update_local_down_list, Down, MembersDict}, _From,
            #state{my_flu_name=MyFluName, pending_map=OldMap,
                   local_down=OldDown, members_dict=OldMembersDict}=S) ->
    NewMap = store_in_map(OldMap, MyFluName, erlang:now(), Down, [props_yo]),
    S2 = if Down == OldDown, MembersDict == OldMembersDict ->
                 %% Do nothing only if both are equal.  If members_dict is
                 %% changing, that's sufficient reason to spam.
                 ok;
            true ->
                 do_map_change(NewMap, [MyFluName], MembersDict, S)
         end,
    {reply, ok, S2#state{local_down=Down}};
handle_call({incoming_spam, Author, Dict}, _From, S) ->
    {Res, S2} = do_incoming_spam(Author, Dict, S),
    {reply, Res, S2};
handle_call(_Request, _From, S) ->
    Reply = whhhhhhhhhhhhhhaaaaaaaaaaaaaaa,
    {reply, Reply, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({adjust_down_list, FLU}, #state{active_unfit=ActiveUnfit}=S) ->
    NewUnfit = make_unfit_list(S),
    Added_to_new     = NewUnfit -- ActiveUnfit,
    Dropped_from_new = ActiveUnfit -- NewUnfit,
    io:format(user, "adjust_down_list: ~w: adjust ~w: add ~p drop ~p\n", [S#state.my_flu_name, FLU, Added_to_new, Dropped_from_new]),
    case {lists:member(FLU,Added_to_new), lists:member(FLU,Dropped_from_new)} of
        {true, true} ->
            error({bad, ?MODULE, ?LINE, FLU, ActiveUnfit, NewUnfit});
        {true, false} ->
            {noreply, S#state{active_unfit=lists:usort(ActiveUnfit ++ [FLU])}};
        {false, true} ->
            {noreply, S#state{active_unfit=ActiveUnfit -- [FLU]}};
        {false, false} ->
            {noreply, S}
    end;
handle_info(dump, #state{my_flu_name=MyFluName,active_unfit=ActiveUnfit,
                         pending_map=Map}=S) ->
    io:format(user, "DUMP: ~w: ~p ~w\n", [MyFluName, ActiveUnfit, map_value(Map)]),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_unfit_list(#state{members_dict=MembersDict}=S) ->
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
    Problems = (catch lists:flatten(map_fold(F, [], S#state.pending_map))),
    All_list = [K || {K,_V} <- orddict:to_list(MembersDict)],
    Unfit = calc_unfit(All_list, Problems),
    Unfit.

store_in_map(Map, Name, Now, Down, Props) ->
    Val = {Now, Down, Props},
    map_set(Name, Map, Name, Val).

send_spam(NewMap, DontSendList, MembersDict, #state{my_flu_name=MyFluName}) ->
    Send = fun(FLU, #p_srvr{address=Host, port=TcpPort}) ->
                   SpamProj = machi_projection:update_checksum(
                                #projection_v1{epoch_number=?SPAM_PROJ_EPOCH,
                                               author_server=MyFluName,
                                               dbg=[NewMap],
                                               %% stuff only to make PB happy
                                               all_members=[],
                                               witnesses=[],
                                               creation_time={1,2,3},
                                               mode=ap_mode,
                                               upi=[], repairing=[], down=[],
                                               dbg2=[],
                                               members_dict=[]                                           }),
                   %% Best effort, don't care about failure.
                   spawn(fun() ->
                                 machi_flu1_client:write_projection(
                                   Host, TcpPort, public, SpamProj)
                         end)
           end,
    F = fun(FLU, P_srvr, Acc) ->
                case lists:member(FLU, DontSendList) of
                    true ->
                        Acc;
                    false ->
                        Send(FLU, P_srvr),
                        [FLU|Acc]
                end
        end,
    Sent = orddict:fold(F, [], MembersDict),
    ok.

calc_unfit(All_list, HosedAnnotations) ->
    G = digraph:new(),
    [digraph:add_vertex(G, V) || V <- All_list],
    [digraph:add_edge(G, V1, V2) || {V1, problem_with, V2} <- HosedAnnotations],
    calc_unfit2(lists:sort(digraph:vertices(G)), G).

calc_unfit2([], G) ->
    digraph:delete(G),
    [];
calc_unfit2([H|T], G) ->
    case digraph:in_degree(G, H) of
        0 ->
            calc_unfit2(T, G);
        1 ->
            Neighbors = digraph:in_neighbours(G, H),
            case [V || V <- Neighbors, digraph:in_degree(G, V) == 1] of
                [AlsoOne|_] ->
                    %% TODO: be smarter here about the choice of which is down.
                    [H|calc_unfit2(T -- [AlsoOne], G)];
                [] ->
                    %% H is "on the end", e.g. 1-2-1, so it's OK.
                    calc_unfit2(T, G)
            end;
        N when N > 1 ->
            [H|calc_unfit2(T, G)]
    end.

do_incoming_spam(Author, Map,
                 #state{my_flu_name=MyFluName,pending_map=OldMap,
                        members_dict=MembersDict}=S) ->
    OldMapV = map_value(OldMap),
    MapV = map_value(Map),
    if MapV == OldMapV ->
            {ok, S};
       true ->
            %% io:format(user, "YY1 ~p\n", [OldMapV]),
            %% io:format(user, "YY2 ~p\n", [MapV]),
            NewMap = map_merge(OldMap, Map),
            %% NewMapV = map_value(NewMap),
            %% io:format(user, "YY3 ~p\n", [NewMapV]),
            S2 = do_map_change(NewMap, [MyFluName, Author], MembersDict, S),
            {ok, S2}
    end.

do_map_change(NewMap, DontSendList, MembersDict,
              #state{my_flu_name=_MyFluName, pending_map=OldMap}=S) ->
    send_spam(NewMap, DontSendList, MembersDict, S),
    ChangedServers = find_changed_servers(OldMap, NewMap),
    DelayTimeMS = 300,                          % TODO make configurable!
    [erlang:send_after(DelayTimeMS, self(), {adjust_down_list, FLU}) ||
        FLU <- ChangedServers],
    %% _OldMapV = map_value(OldMap),
    %% _MapV = map_value(NewMap),
    %% io:format(user, "TODO: ~w async tick trigger/scheduling... ~w for:\n"
    %%           "    ~p\n    ~p\n",[_MyFluName,ChangedServers,_OldMapV,_MapV]),
    S#state{pending_map=NewMap, members_dict=MembersDict}.

find_changed_servers(OldMap, NewMap) ->
    AddBad = fun({_Who, {_Time, BadList, _Props}}, Acc) -> BadList ++ Acc end,
    OldBad = map_fold(AddBad, [], OldMap),
    NewBad = map_fold(AddBad, [], NewMap),
    lists:usort((OldBad -- NewBad) ++ (NewBad -- OldBad)).

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
    Vs = map_value(Map),
    lists:foldl(Fun, Acc, lists:sort(Vs)).

map_value(Map) ->
    lists:sort([{K, binary_to_term(V)} || {{K, _Type}, V} <- ?MAP:value(Map)]).

map_merge(Map1, Map2) ->
    ?MAP:merge(Map1, Map2).

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
