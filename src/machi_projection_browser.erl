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
    Ps = get_viz_projections(ProjType, Epoch, MembersDictOrDirList),
    Facts = compile_projections_to_facts(ProjType, Ps),
    %% io:format(user, "Facts ~p\n", [Facts]),
    make_dot_file(Facts, DotOut).

get_viz_projections(ProjType, Epoch, MembersDictOrDirList) ->
    get_viz_projections(ProjType, Epoch, MembersDictOrDirList, true).

get_viz_projections(ProjType, Epoch, MembersDictOrDirList, GetOlderEpochs_p) ->
    PsAtEpoch = 
        lists:append([try
                          {ok, P} = read_projection(ProjType, Epoch, X),
                          [P]
                      catch _:_ ->
                              []
                      end || X <- MembersDictOrDirList]),
io:format("PsAtEpoch ~P\n", [PsAtEpoch, 10]),
    if not GetOlderEpochs_p ->
            PsAtEpoch;
       PsAtEpoch == [] ->
            {bummer_no_projections_at_epoch, ProjType, Epoch};
       true ->
            [#projection_v1{upi=UPI, repairing=Repairing}|_] = PsAtEpoch,
            UPI_R = UPI ++ Repairing,
            Thingies_not_at_Epoch =
                [try
                     {error, _} = read_projection(ProjType, Epoch, X),
                     X
                 catch _:_ ->
                         []
                 end || X <- MembersDictOrDirList,
                        X /= [],
                        not lists:member(thingie_to_name(X), UPI_R)],
            PsPriorToEpoch =
                lists:append([try
                                  Es = list_unique_projections(ProjType, [X]),
                                  Ep = lists:max([E || E <- Es, E < Epoch]),
                                  {ok, P} = read_projection(ProjType, Ep, X),
                                  [P]
                              catch _:_ ->
                                      []
                              end || X <- Thingies_not_at_Epoch, X /= []]),
            PsAtEpoch ++ PsPriorToEpoch
    end.

thingie_to_name(#p_srvr{name=Name}) ->
    Name;
thingie_to_name(Str) when is_list(Str) ->
    list_to_atom(re:replace(Str, ".*/?data\\.", "", [{return, list}])).

compile_projections_to_facts(ProjType, Ps) ->
    compile_projections_to_facts(ProjType, Ps, orddict:new()).

compile_projections_to_facts(ProjType, Ps, D0) ->
    D1 = make_facts_unique_flus(Ps, D0),
    _D2 = make_facts_heads_tails(ProjType, Ps, D1).

make_facts_unique_flus(Ps, D) ->
    Unique = unique_flus(Ps),
    add_facts([{{virt, X}, true} || X <- Unique] ++
              %% Add default color
              [{{virt_color, X}, "red"} || X <- Unique], D).

unique_flus(Ps) ->
    lists:usort(lists:flatten([ [P#projection_v1.all_members,
                                 P#projection_v1.upi,
                                 P#projection_v1.repairing,
                                 P#projection_v1.down] || P <- Ps])).

make_facts_heads_tails(ProjType, Ps, D0) ->
    2 = #projection_v1.epoch_number, % sanity check for sorting order.
    [MaxP|_] = lists:reverse(lists:sort(Ps)),
    MaxEpoch = MaxP#projection_v1.epoch_number,
    AllEpochs = [P#projection_v1.epoch_number || P <- Ps],
    MaxEpochAuthor = MaxP#projection_v1.author_server,
    Partitions = proplists:get_value(ps, MaxP#projection_v1.dbg, []),
    D1 = add_fact(max_epoch, MaxEpoch,
         add_fact(max_epoch_author, MaxEpochAuthor,
         add_fact(projection_type, ProjType,
         add_fact(creation_time, MaxP#projection_v1.creation_time,
         add_fact(partition_list, Partitions,
                  D0))))),

    %% Update the facts dictionary, via sort from smallest epoch to largest
    lists:foldl(fun(P, Dict) ->
                        make_facts_heads_tails(P, MaxEpoch, AllEpochs, MaxP,
                                               Dict)
                end, D1, lists:sort(Ps)).

make_facts_heads_tails(#projection_v1{upi=UPI, repairing=Repairing}=P,
                       MaxEpoch, AllEpochs, MaxP, D0) ->
    {Head, MidsTails, Repairs} =
        if UPI == [] ->
                {[], [], []};
           true ->
                [Hd|MsT] = UPI,
                {Hd, MsT, Repairing}
        end,
    D10 = add_fact({virt_role, Head}, head, D0),
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
                [{{virt_epoch, X}, E} ||
                    E <- [P#projection_v1.epoch_number],
                    X <- P#projection_v1.upi ++
                         P#projection_v1.repairing]),
    D40 = add_facts(EpochFs, D30),
    EpochLabelFs1 = [{{virt_label_epoch, X}, "Epoch=current"} ||
                        {{virt_epoch, X}, E} <- EpochFs,
                        E == MaxEpoch],
    Es_not_max = AllEpochs -- [MaxEpoch],
    EpochLabelFs2 = [{{virt_label_epoch, X}, str("Epoch=~w", [Es_not_max])} ||
                        {{virt_epoch, X}, E} <- EpochFs,
                        E /= MaxEpoch,
                        get_fact_maybe({virt_epoch, X}, D30, yup) == yup],
    D50 = add_facts(EpochLabelFs1, D40),
    D51 = add_facts(EpochLabelFs2, D50),
    D60 = make_facts_edges(P, MaxEpoch, MaxP, D51),
    D60.

make_facts_edges(#projection_v1{
                    epoch_number=Epoch, upi=UPI, repairing=Repairing}=_P,
                 _MaxEpoch, _MaxP, D0) ->
    %% TODO: This edge pointing to self is technically wrong: there is no
    %% chain replication happening.  But it seems to be a nice visual thing?
    D1 = case {UPI, Repairing} of
             {[X], []} ->
                 add_fact({edge, {Epoch, X, {X, X}}}, true_1, D0);
             _ ->
                 D0
         end,
    UPI_Hops = make_hops(UPI),

    UPI_R = UPI ++ Repairing,
    %%{{edge, {Epoch, In_max_epoch_p, {From_vertex, To_vertex}}}, true}
    D10 = add_facts([{{edge, {Epoch, Who, HopPair}}, true_2} ||
                        HopPair <- UPI_Hops, Who <- UPI_R], D1),
    UPI_lastL = if UPI == [] -> [];
                   true      -> [lists:last(UPI)]
                end,
    Repair_Hops = make_hops(UPI_lastL ++ Repairing),
    D20 = add_facts([{{edge, {Epoch, Who, HopPair}}, true_3} ||
                        HopPair <- Repair_Hops, Who <- UPI_R], D10),
    D20.

make_hops([X, Y|Rest]) ->
    [{X,Y}|make_hops([Y|Rest])];
make_hops(_) ->
    [].    

add_fact(K, V, D) ->
    add_facts([{K,V}], D).

add_facts(KsVs, D) ->
    lists:foldl(fun({K, V}, Acc) -> orddict:store(K, V, Acc) end, D, KsVs).

get_fact(K, D) ->
    orddict:fetch(K, D).

get_fact_maybe(K, D, Default) ->
    case orddict:find(K, D) of error   -> Default;
                               {ok, V} -> V
    end.

str(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).

make_dot_file(Facts, _OutFile) ->
    {ok, FH} = file:open(_OutFile, [write]),
    try
        %% make_dot_file2(Facts, user), % debugging only
        make_dot_file2(Facts, FH)
    after
        (catch file:close(FH))
    end.

-define(ON(FH, FMT),       io:format(FH, FMT ++ "\n", []  )).
-define(ON(FH, FMT, ARGS), io:format(FH, FMT ++ "\n", ARGS)).
-define( O(FH, FMT),       io:format(FH, FMT,         []  )).
-define( O(FH, FMT, ARGS), io:format(FH, FMT,         ARGS)).

make_dot_file2(Facts, FH) ->
    ?ON(FH, "digraph G {"),

    %% Make our label string
    MaxEpoch = get_fact(max_epoch, Facts),
    MaxEpochAuthor = get_fact(max_epoch_author, Facts),
    ProjType = get_fact(projection_type, Facts),
    CreationTime = get_fact(creation_time, Facts),
    Partitions = get_fact(partition_list, Facts),
    Label1 = string:join(
               [str("~w epoch=~w author=~w",
                    [ProjType, MaxEpoch, MaxEpochAuthor]),
                str("~s", [date_str(CreationTime)]),
                str("Partitions=~w", [Partitions])
               ], "\\n"),
    ?ON(FH, "  label=\"~s\"", [Label1]),
    ?ON(FH, ""),

    %% Create rank spec
    Vs = lists:sort([V || {{virt, V}, true} <- Facts]),
    ?O(FH, "  { rank=same; ", []),
    [?O(FH, "~w; ", [V]) || V <- Vs],
    ?ON(FH, "};"),
    ?ON(FH, ""),

    %% Create vertex specs
    [begin
         DownStr = get_fact({virt_label_down, V}, Facts),
         EpochStr = get_fact({virt_label_epoch, V}, Facts),
         V_Epoch = get_fact({virt_epoch, V}, Facts),
         VertColor = dot_vertex_color(V, V_Epoch, MaxEpoch, Facts),
         VertFontColor = if VertColor == "grey" -> "grey";
                            true                -> "black"
                         end,
         Label2 = string:join(
                    [
                     str("label=\"~w\\n~s\\n~s\"", [V, DownStr, EpochStr]),
                     str("shape=~s", [dot_vertex_shape(V, Facts)]),
                     str("color=~s", [VertColor]),
                     str("fontcolor=~s", [VertFontColor])
                    ], ","),
         ?ON(FH, "  ~w [~s]", [V, Label2])
     end || V <- Vs],
    ?ON(FH, ""),

    ?ON(FH, "  /* Add invisible edges so that the diagram is ordered */"),
    ?ON(FH, "  /* left-to-right as we wish. */"),
    [?ON(FH, "  ~w -> ~w [style=invis]", [X, Y]) || {X,Y} <- make_hops(Vs)],
    ?ON(FH, ""),

    %% Create edge specs
    [begin
         %% Color by the role of the destination of the edge.
         VertColor = dot_vertex_color(Y, Epoch, MaxEpoch, Facts),
         VertFontColor = VertColor,
         Label3 = string:join(
                    [
                     str("taillabel=\"~w\"", [Who]),
                     str("color=~s", [VertColor]),
                     str("fontcolor=~s", [VertFontColor])
                    ], ","),
         ?ON(FH, " ~w -> ~w [~s]", [X, Y, Label3])
     end || {{edge, {Epoch, Who, {X, Y}}}, _} <- Facts],
    ?ON(FH, ""),

    %% Done!
    ?ON(FH, "}").

dot_vertex_shape(V, Facts) ->
    case get_fact({virt_role, V}, Facts) of head -> "polygon,sides=7";
                                            _    -> "ellipse"
    end.

dot_vertex_color(V, V_Epoch, MaxEpoch, Facts)
  when V_Epoch == MaxEpoch ->
    case get_fact({virt_role, V}, Facts) of head      -> "green";
                                            middle    -> "green";
                                            tail      -> "green";
                                            repairing -> "purple";
                                            _         -> "red"
    end;
dot_vertex_color(_V, _V_Epoch, _MaxEpoch, _Facts) ->
    "grey".

date_str({_A,_B,MSec}=Now) ->
    {{YYYY,Mo,DD},{HH,MM,SS}} = calendar:now_to_local_time(Now),
    str("~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w.~3..0w",
        [YYYY,Mo,DD,HH,MM,SS,MSec div 1000]).

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

