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

%% @doc API for manipulating Machi projection data structures (i.e., records).

-module(machi_projection_browser).

-include("machi_projection.hrl").

-export([]). % TODO
-compile(export_all). % TODO

-define(FLU_C, machi_flu1_client).

visualize(ProjType, Epoch, MembersDictOrDirList, _DotOut) ->
    PsAtEpoch = 
        lists:append([try
                          {ok, P} = read_projection(ProjType, Epoch, X),
                          [P]
                      catch _:_ ->
                              []
                      end || X <- MembersDictOrDirList]),
    Thingies_not_at_Epoch =
        [try
             {error, _} = read_projection(ProjType, Epoch, X),
             X
         catch _:_ ->
                 []
         end || X <- MembersDictOrDirList, X /= []],
    PsPriorToEpoch =
        lists:append([try
                          Es = list_unique_projections(ProjType, [X]),
                          Ep = lists:max([E || E <- Es, E < Epoch]),
                          {ok, P} = read_projection(ProjType, Ep, X),
                          [P]
                      catch _:_ ->
                              []
                      end || X <- Thingies_not_at_Epoch, X /= []]),
    _Graph = graph_projections(PsAtEpoch ++ PsPriorToEpoch).

graph_projections(Ps) ->
    graph_projections(Ps, orddict:new()).

graph_projections(Ps, D0) ->
    D1 = graph_unique_flus(Ps, D0),
    _D2 = graph_heads_tails(Ps, D1).

graph_unique_flus(Ps, D) ->
    Unique = unique_flus(Ps),
    add_facts([{{virt, X}, true} || X <- Unique] ++
              %% Add default color
              [{{virt_color, X}, "red"} || X <- Unique], D).

unique_flus(Ps) ->
    lists:usort(lists:flatten([ [P#projection_v1.all_members,
                                 P#projection_v1.upi,
                                 P#projection_v1.repairing,
                                 P#projection_v1.down] || P <- Ps])).

graph_heads_tails(Ps, D0) ->
    2 = #projection_v1.epoch_number, % sanity check for sorting order.
    [MaxP|_] = lists:reverse(lists:sort(Ps)),
    MaxEpoch = MaxP#projection_v1.epoch_number,
io:format(user, "line ~w MaxEpoch ~w\n", [?LINE, MaxEpoch]),
    %% Update the facts dictionary, via sort from smallest epoch to largest
    lists:foldl(fun(P, Dict) -> graph_heads_tails(P, MaxEpoch, MaxP, Dict) end,
                D0, lists:sort(Ps)).

graph_heads_tails(#projection_v1{upi=UPI, repairing=Repairing}=P,
                  MaxEpoch, MaxP, D0) ->
io:format(user, "line ~w MaxEpoch ~w\n", [?LINE, MaxEpoch]),
    {Head, MidsTails, Repairs} =
        if UPI == [] ->
                {[], [], []};
           true ->
                [Hd|MsT] = UPI,
                {Hd, MsT, Repairing}
        end,
    D10 = add_fact({virt_role, Head}, head, D0),
    %% D20 = add_facts([{{virt_role, X}, mid_tail} || X <- MidsTails ++ Repairs], D10),
    D20 = add_facts([{{virt_role, X}, middle} ||
                        X <- MidsTails,
                        X /= lists:last(MidsTails)], D10),
    D21 = add_facts([{{virt_role, X}, tail} ||
                        X <- MidsTails,
                        X == lists:last(MidsTails)], D20),
    D22 = add_facts([{{virt_role, X}, repairing} || X <- Repairs], D21),
    DownFs = lists:flatten(
               [{{virt_label_down, X}, str("down=~w", [P#projection_v1.down])}||
                   X <- UPI ++ Repairing]),
    D30 = add_facts(DownFs, D22),
    EpochFs = lists:flatten(
                [{{virt_epoch,X}, E} ||
                    E <- [P#projection_v1.epoch_number],
                    X <- P#projection_v1.upi ++
                         P#projection_v1.repairing]),
    D40 = add_facts(EpochFs, D30),
    EpochLabelFs = [{{virt_label_epoch, X}, ""} ||
                        {{virt_epoch,X}, E} <- EpochFs, E == MaxEpoch]
                   ++
                   [{{virt_label_epoch, X}, str("Epoch=~w", [E])} ||
                        {{virt_epoch,X}, E} <- EpochFs, E /= MaxEpoch],
    D50 = add_facts(EpochLabelFs, D40),
    D60 = graph_edges(P, MaxEpoch, MaxP, D50),
    D60.

graph_edges(#projection_v1{
               epoch_number=Epoch, upi=UPI, repairing=Repairing}=_P,
            MaxEpoch, _MaxP, D0) ->
    %% TODO: This edge pointing to self is technically wrong: there is no
    %% chain replication happening.  But it seems to be a nice visual thing?
    D1 = case UPI of
             [X] ->
                 add_fact({edge, {Epoch, {X, X}}}, Epoch == MaxEpoch, D0);
             _ ->
                 D0
         end,
    UPI_Hops = make_hops(UPI),
    D10 = add_facts([{{edge, {Epoch, HopPair}}, Epoch == MaxEpoch} ||
                        HopPair <- UPI_Hops], D1),
    UPI_lastL = if UPI == [] -> [];
                   true      -> [lists:last(UPI)]
                end,
    Repair_Hops = make_hops(UPI_lastL ++ Repairing),
    D20 = add_facts([{{edge, {Epoch, HopPair}}, Epoch == MaxEpoch} ||
                        HopPair <- Repair_Hops], D10),
    io:format(user, "Epoch ~w hops 1 ~p 2 ~p\n", [Epoch, UPI_Hops, Repair_Hops]),
    D20.

make_hops([X, Y|Rest]) ->
    [{X,Y}|make_hops([Y|Rest])];
make_hops(_) ->
    [].    

add_fact(K, V, D) ->
    add_facts([{K,V}], D).

add_facts(KsVs, D) ->
    lists:foldl(fun({K, V}, Acc) -> orddict:store(K, V, Acc) end, D, KsVs).

str(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%

list_unique_projections(ProjType, MembersDictOrDirList) ->
    list_unique_projections(ProjType, MembersDictOrDirList, 0).

list_unique_projections(ProjType, MembersDictOrDirList, MinEpoch) ->
    F = fun({_K, #p_srvr{proto='ipv4', address=Address, port=TcpPort}}, Acc) ->
                {ok, Es} = ?FLU_C:list_all_projections(Address, TcpPort,
                                                       ProjType),
                [Es|Acc];
           (Dir, Acc) when is_list(Dir) ->
                ProjDir = machi_util:make_projection_dirname(Dir, ProjType),
                Es = machi_projection_store:find_all(ProjDir),
                [Es|Acc]
        end,
    %% Abuse the fact that we know the internal structure of MembersDict
    Res0 = lists:foldl(F, [], MembersDictOrDirList),
    [Epoch || Epoch <- lists:usort(lists:flatten(Res0)),
              Epoch >= MinEpoch].
            
%% get_all_projections(ProjType, MembersDictOrDirList) ->
%%     get_all_projections(ProjType, MembersDictOrDirList, 0).

%% get_all_projections(ProjType, MembersDictOrDirList, MinEpoch) ->
%%     F = fun({_K, #p_srvr{proto='ipv4', address=Address, port=TcpPort}}, Acc) ->
%%                 {ok, Ps} = ?FLU_C:get_all_projections(Address, TcpPort,
%%                                                       ProjType),
%%                 [Ps|Acc];
%%            (Dir, Acc) when is_list(Dir) ->
%%                 Es = list_all_projections(ProjType, MembersDictOrDirList,
%%                                           MinEpoch),
%%                 Ps = [begin
%%                           File = machi_util:make_projection_filename(
%%                                    Dir, ProjType, Epoch),
%%                           {ok, P} = machi_projection_store:read_proj_file(File),
%%                           P
%%                       end || Epoch <- Es],
%%                 [Ps|Acc]
%%         end,
%%     %% Abuse the fact that we know the internal structure of MembersDict
%%     Res0 = lists:foldl(F, [], MembersDictOrDirList),
%%     lists:usort(lists:flatten(Res0)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% get_latest_epoch(ProjType, #p_srvr{proto='ipv4',
%%                                    address=Address, port=TcpPort}) ->
%%     ?FLU_C:get_latest_epoch(Address, TcpPort, ProjType);
%% get_latest_epoch(ProjType, Dir) when is_list(Dir) ->
%%     {ok, lists:last(list_all_projections(ProjType, [Dir]))}.

%% read_latest_projection(ProjType, #p_srvr{proto='ipv4',
%%                                    address=Address, port=TcpPort}) ->
%%     ?FLU_C:read_latest_projection(Address, TcpPort, ProjType);
%% read_latest_projection(ProjType, Dir) when is_list(Dir) ->
%%     {ok, Epoch} = get_latest_epoch(ProjType, Dir),
%%     File = machi_util:make_projection_filename(Dir, ProjType, Epoch),
%%     machi_projection_store:read_proj_file(File).

read_projection(ProjType, Epoch, #p_srvr{proto='ipv4',
                                         address=Address, port=TcpPort}) ->
    ?FLU_C:read_projection(Address, TcpPort, ProjType, Epoch);
read_projection(ProjType, Epoch, Dir) when is_list(Dir) ->
    File = machi_util:make_projection_filename(Dir, ProjType, Epoch),
    machi_projection_store:read_proj_file(File).

