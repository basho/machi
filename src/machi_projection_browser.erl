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

visualize(ProjType, Epoch, MembersDictOrDirList, DotOut) ->
    PsAtEpoch0 = [try
                      {ok, P} = read_projection(ProjType, Epoch, X),
                      P
                  catch _:_ ->
                      []
                  end || X <- MembersDictOrDirList],
    Graph = graph_projections(PsAtEpoch0).

graph_projections(Ps) ->
    graph_projections(Ps, orddict:new()).

graph_projections(Ps, D0) ->
    D1 = graph_unique_flus(Ps, D0),
    D2 = graph_heads_tails(Ps, D1).

graph_unique_flus(Ps, D) ->
    Unique = lists:usort(
               lists:flatten(
                 [ [P#projection_v1.all_members,
                    P#projection_v1.upi,
                    P#projection_v1.repairing,
                    P#projection_v1.down] || P <- Ps])),
    add_facts([{{virt, X}, true} || X <- Unique] ++
              %% Add default color
              [{{virt_color, X}, "cyan"} || X <- Unique], D).

graph_heads_tails(Ps, D0) ->
    UPIs = [P#projection_v1.upi || P <- Ps],
    Heads = lists:usort(lists:flatten([hd(UPI) || UPI <- UPIs])),
    MidsTails = lists:usort(lists:flatten([tl(UPI) || UPI <- UPIs])),
    Repairs = lists:usort(lists:flatten([P#projection_v1.repairing|| P <- Ps])),
    D1 = add_facts([{{virt_shape, X},
                    "shape=polygon,sides=7"} || X <- Heads], D0),
    D2 = add_facts([{{virt_shape, X},
                    "shape=ellipse"} || X <- MidsTails ++ Repairs], D1),
    DownFs = lists:flatten(
               [{{virt_label_down, X}, str("down=~w", [P#projection_v1.down])}||
                   P <- Ps, X <- P#projection_v1.upi ++
                                 P#projection_v1.repairing]),
    D3 = add_facts(DownFs, D2),
    MaxEpoch = lists:max([P#projection_v1.epoch_number || P <- Ps]),
    EpochFs = lists:flatten(
                [{{virt_epoch,X}, Epoch} ||
                    P <- Ps,
                    Epoch <- [P#projection_v1.epoch_number],
                    X <- P#projection_v1.upi ++
                         P#projection_v1.repairing]),
    D4 = add_facts(EpochFs, D3),
    EpochLabelFs = [{{virt_label_epoch, X}, ""} ||
                        {{virt_epoch,X}, Epoch} <- EpochFs, Epoch == MaxEpoch]
                   ++
                   [{{virt_label_epoch, str("Epoch=~w", [Epoch])}, ""} ||
                        {{virt_epoch,X}, Epoch} <- EpochFs, Epoch /= MaxEpoch],
    D5 = add_facts(EpochLabelFs, D4),
    D5.

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

