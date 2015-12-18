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

-include("machi.hrl").
-include("machi_projection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

-define(LWWREG, riak_dt_lwwreg).
-define(MAP, riak_dt_map).

-define(DELAY_TIME_MS, 300).                 % TODO make configurable!

%% API
-export([start_link/1,
         get_unfit_list/1, update_local_down_list/3,
         add_admin_down/3, delete_admin_down/2,
         send_fitness_update_spam/3,
         send_spam_to_everyone/1,
         trigger_early_adjustment/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-record(state, {
          my_flu_name                :: atom() | binary(),
          reg_name                   :: atom(),
          local_down=[]              :: list(),
          admin_down=[]              :: list({term(),term()}),
          members_dict=orddict:new() :: orddict:orddict(),
          proxies_dict=orddict:new() :: orddict:orddict(),
          active_unfit=[]            :: list(),
          pending_map=?MAP:new()     :: ?MAP:riak_dt_map(),
          partition_simulator_p      :: boolean()
         }).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

get_unfit_list(PidSpec) ->
    gen_server:call(PidSpec, {get_unfit_list}, infinity).

update_local_down_list(PidSpec, Down, MembersDict) ->
    gen_server:call(PidSpec, {update_local_down_list, Down, MembersDict},
                    infinity).

add_admin_down(PidSpec, DownFLU, DownProps) ->
    gen_server:call(PidSpec, {add_admin_down, DownFLU, DownProps},
                    infinity).

delete_admin_down(PidSpec, DownFLU) ->
    gen_server:call(PidSpec, {delete_admin_down, DownFLU},
                    infinity).

send_fitness_update_spam(Pid, FromName, Dict) ->
    gen_server:call(Pid, {incoming_spam, FromName, Dict}, infinity).

send_spam_to_everyone(Pid) ->
    gen_server:call(Pid, {send_spam_to_everyone}, infinity).

%% @doc For testing purposes, we don't want a test to wait for
%%      wall-clock time to elapse before the fitness server makes a
%%      down->up status decision.

trigger_early_adjustment(Pid, FLU) ->
    Pid ! {adjust_down_list, FLU}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([{MyFluName}|Args]) ->
    RegName = machi_flu_psup:make_fitness_regname(MyFluName),
    register(RegName, self()),
    {ok, _} = timer:send_interval(5000, debug_dump),
    UseSimulatorP = proplists:get_value(use_partition_simulator, Args, false),
    {ok, #state{my_flu_name=MyFluName, reg_name=RegName,
                partition_simulator_p=UseSimulatorP,
                local_down=[complete_bogus_here_to_trigger_initial_spam]
               }}.

handle_call({get_unfit_list}, _From, #state{active_unfit=ActiveUnfit}=S) ->
    Reply = ActiveUnfit,
    {reply, Reply, S};
handle_call({update_local_down_list, Down, MembersDict}, _From,
            #state{my_flu_name=MyFluName, pending_map=OldMap,
                   local_down=OldDown, members_dict=OldMembersDict,
                   admin_down=AdminDown}=S) ->
    NewMap = store_in_map(OldMap, MyFluName, erlang:now(), Down,
                          AdminDown, [props_yo]),
    S2 = if Down == OldDown, MembersDict == OldMembersDict ->
                 %% Do nothing only if both are equal.  If members_dict is
                 %% changing, that's sufficient reason to spam.
                 S;
            true ->
                 do_map_change(NewMap, [MyFluName], MembersDict, S)
         end,
    {reply, ok, S2#state{local_down=Down}};
handle_call({add_admin_down, DownFLU, DownProps}, _From,
            #state{local_down=OldDown, admin_down=AdminDown}=S) ->
    NewAdminDown = [{DownFLU,DownProps}|lists:keydelete(DownFLU, 1, AdminDown)],
    S3 = finish_admin_down(erlang:now(), OldDown, NewAdminDown,
                           [props_yo], S),
    {reply, ok, S3};
handle_call({delete_admin_down, DownFLU}, _From,
            #state{local_down=OldDown, admin_down=AdminDown}=S) ->
    NewAdminDown = lists:keydelete(DownFLU, 1, AdminDown),
    S3 = finish_admin_down(erlang:now(), OldDown, NewAdminDown,
                           [props_yo], S),
    {reply, ok, S3};
handle_call({incoming_spam, Author, Dict}, _From, S) ->
    {Res, S2} = do_incoming_spam(Author, Dict, S),
    {reply, Res, S2};
handle_call({send_spam_to_everyone}, _From, S) ->
    {Res, S2} = do_send_spam_to_everyone(S),
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
    %% io:format(user, "adjust_down_list: ~w: adjust ~w: add ~p drop ~p\n", [S#state.my_flu_name, FLU, Added_to_new, Dropped_from_new]),
    %% We need to schedule a new round of adjustment messages.  They might
    %% be redundant, or they might not.  Here's a case where the current
    %% code needs the extra:
    %%
    %% SET partitions = [{a,c},{b,c},{c,b}] (11 of 26) at {23,37,44}
    %% We are stable spam/gossip at:
    %%  [{a,problem_with,b},{b,problem_with,c},
    %%   {c,problem_with,a},{c,problem_with,b}]
    %% So everyone agrees unfit=[c].
    %%
    %% SET partitions = [{c,a}] (12 of 26) at {23,37,48}
    %% We are stable spam/gossip at:
    %%  [{a,problem_with,c},{c,problem_with,a}]
    %% So everyone *ought* to agree that unfit=[a].
    %%
    %% In this case, when the partition list changes to [{c,a}],
    %% then we will discover via spam gossip that reports by B & C will
    %% change.  However, our calc_unfit() via
    %% make_unfit_list() algorithm will decide that *a* is the bad guy
    %% and needs to go into our active_unfit list!  And the only way
    %% to get added is via an {adjust_down_list,...} message. The
    %% usual place for generating them isn't wise enough because it
    %% doesn't call make_unfit_list().
    %%
    %% The cost is that there will (at least) a 2x delay to the
    %% ?DELAY_TIME_MS waiting period to detect all partitions.
    %%
    %% Aside: for all I know right now, there may be a corner case
    %% hiding where we need this extra round of messages to *remove* a
    %% FLU from the active_unfit list?

    _ = schedule_adjust_messages(lists:usort(Added_to_new ++ Dropped_from_new)),
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
handle_info(debug_dump, #state{my_flu_name=_MyFluName,active_unfit=_ActiveUnfit,
                               pending_map=_Map}=S) ->
    %% io:format(user, "DUMP: ~w/~w: ~p ~W\n", [_MyFluName, self(), _ActiveUnfit, map_value(_Map), 13]),
    %% io:format(user, "DUMP ~w: ~w, ", [MyFluName, ActiveUnfit]),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

format_status(_Opt, [_PDict, Status]) ->
    Fields = record_info(fields, state),
    [_Name | Values] = tuple_to_list(Status),
    lists:zip(Fields, Values).

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_unfit_list(#state{members_dict=MembersDict}=S) ->
    Now = erlang:now(),
    F = fun({Server, {UpdateTime, DownList, AdminDown, _Props}},
            {ProblemAcc, AdminAcc}) ->
                case timer:now_diff(Now, UpdateTime) div (1000*1000) of
                    N when N > 900 ->           % TODO make configurable
                        {ProblemAcc, AdminAcc};
                    _ ->
                        Probs = [{Server,problem_with,D} || D <- DownList],
                        {[Probs|ProblemAcc], AdminDown++AdminAcc}
                end
        end,
    {Problems0, AdminDown} = map_fold(F, {[], []}, S#state.pending_map),
    Problems = lists:flatten(Problems0),
    All_list = [K || {K,_V} <- orddict:to_list(MembersDict)],
    Unfit = calc_unfit(All_list, Problems),
    lists:usort(Unfit ++ AdminDown).

store_in_map(Map, Name, Now, Down, AdminDown, Props) ->
    {AdminDownServers, AdminDownProps0} = lists:unzip(AdminDown),
    AdminDownProps = lists:append(AdminDownProps0), % flatten one level
    Val = {Now, Down, AdminDownServers, Props ++ AdminDownProps},
    map_set(Name, Map, Name, Val).

send_spam(NewMap, DontSendList, MembersDict, #state{my_flu_name=MyFluName}=S) ->
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
                                send_projection(FLU, Host, TcpPort, SpamProj, S)
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
    _Sent = orddict:fold(F, [], MembersDict),
    ok.

send_projection(FLU, _Host, _TcpPort, SpamProj,
                #state{my_flu_name=MyFluName, members_dict=MembersDict,
                       partition_simulator_p=SimulatorP}=S) ->
    %% At the moment, we're using utterly-temporary-hack method of tunneling
    %% our messages through the write_projection API.  Eventually the PB
    %% API should be expanded to accomodate this new fitness service.
    %% This is "best effort" only, use catch to ignore failures.
    ProxyPid = (catch proxy_pid(FLU, S)),
    DoIt = fun(_ArgIgnored) ->
                   machi_proxy_flu1_client:write_projection(ProxyPid,
                                                            public, SpamProj)
           end,
    ProxyPidPlaceholder = proxy_pid_unused,
    if SimulatorP ->
            AllMembers = [K || {K,_V} <- orddict:to_list(MembersDict)],
            {Partitions, _Islands} = machi_partition_simulator:get(AllMembers),
            machi_chain_manager1:init_remember_down_list(),
            Res = (catch machi_chain_manager1:perhaps_call(ProxyPidPlaceholder,
                                                        MyFluName,
                                                        Partitions, FLU, DoIt)),
            %% case machi_chain_manager1:get_remember_down_list() of
            %%     [] ->
            %%         ok;
            %%     _ ->
            %%         io:format(user, "fitness error ~w -> ~w\n",
            %%                   [MyFluName, FLU])
            %% end,
            Res;
       true ->
            (catch DoIt(ProxyPidPlaceholder))
    end.

proxy_pid(Name, #state{proxies_dict=ProxiesDict}) ->
    orddict:fetch(Name, ProxiesDict).

calc_unfit(All_list, HosedAnnotations) ->
    G = digraph:new(),
    _ = [digraph:add_vertex(G, V) || V <- All_list],
    _ = [digraph:add_edge(G, V1, V2) || {V1, problem_with, V2} <- HosedAnnotations],
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

do_incoming_spam(_Author, Map,
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

            %% Hrm, we may have changes that are interesting to the
            %% Author of this update, so perhaps we shouldn't exclude
            %% Author from our update, right?
            S2 = do_map_change(NewMap, [MyFluName], MembersDict, S),
            {ok, S2}
    end.

do_send_spam_to_everyone(#state{my_flu_name=MyFluName,
                                pending_map=Map,members_dict=MembersDict}=S) ->
    _ = send_spam(Map, [MyFluName], MembersDict, S),
    {ok, S}.

do_map_change(NewMap, DontSendList, MembersDict,
              #state{my_flu_name=_MyFluName, pending_map=OldMap}=S) ->
    send_spam(NewMap, DontSendList, MembersDict, S),
    ChangedServers = find_changed_servers(OldMap, NewMap, _MyFluName),
    _ = schedule_adjust_messages(ChangedServers),
    %% _OldMapV = map_value(OldMap),
    %% _MapV = map_value(NewMap),
    %% io:format(user, "TODO: ~w async tick trigger/scheduling... ~w for:\n"
    %%           "    ~p\n    ~p\n",[_MyFluName,ChangedServers,_OldMapV,_MapV]),
    S2 = perhaps_adjust_members_proxies_dicts(MembersDict, S),
    S2#state{pending_map=NewMap}.

perhaps_adjust_members_proxies_dicts(SameMembersDict,
                                     #state{members_dict=SameMembersDict}=S) ->
    S;
perhaps_adjust_members_proxies_dicts(MembersDict,
                                     #state{proxies_dict=OldProxiesDict}=S) ->
    _ = machi_proxy_flu1_client:stop_proxies(OldProxiesDict),
    ProxiesDict = machi_proxy_flu1_client:start_proxies(MembersDict),
    S#state{members_dict=MembersDict, proxies_dict=ProxiesDict}.

find_changed_servers(OldMap, NewMap, _MyFluName) ->
    AddBad = fun({_Who, {_Time, BadList, AdminDown, _Props}}, Acc) ->
                     BadList ++ AdminDown ++ Acc
             end,
    OldBad = map_fold(AddBad, [], OldMap),
    NewBad = map_fold(AddBad, [], NewMap),
    lists:usort((OldBad -- NewBad) ++ (NewBad -- OldBad)).

schedule_adjust_messages(FLU_list) ->
    [erlang:send_after(?DELAY_TIME_MS, self(), {adjust_down_list, FLU}) ||
        FLU <- FLU_list].

finish_admin_down(Time, Down, NewAdminDown, Props,
                  #state{my_flu_name=MyFluName, local_down=Down,
                         pending_map=OldMap, members_dict=MembersDict}=S) ->
    NewMap = store_in_map(OldMap, MyFluName, Time, Down, NewAdminDown, Props),
    S2 = S#state{admin_down=NewAdminDown},
    do_map_change(NewMap, [MyFluName], MembersDict, S2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

map_set(Actor, Map, Key, ValTerm) ->
    Field = {Key, ?LWWREG},
    Val = term_to_binary(ValTerm),
    {ok, Map2} = ?MAP:update({update, [{update, Field, {assign, Val}}]},
                             Actor, Map),
    Map2.

-ifdef(TEST).
map_get(Map, Key) ->
    Field = {Key, ?LWWREG},
    case lists:keyfind(Field, 1, ?MAP:value(Map)) of
        false ->
            error;
        {Field, ValBin} ->
            {ok, binary_to_term(ValBin)}
    end.
-endif. % TEST

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
