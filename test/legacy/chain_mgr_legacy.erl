%% -------------------------------------------------------------------
%%
%% Machi: a small village of replicated files
%%
%% Copyright (c) 2014-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(chain_mgr_legacy).
-compile(export_all).

-include("machi_projection.hrl").
-include("machi_chain_manager.hrl").

-define(RETURN1(X), begin put(why1, [?LINE|get(why1)]), X end).

%% This is old code, and it's broken.  We keep it around in case
%% someone wants to execute the QuickCheck property
%% machi_chain_manager1_test:prop_compare_legacy_with_v2_chain_transition_check().
%% In all counterexamples, this legacy code returns 'true' (i.e., the
%% state transition is OK) where the v2 new code correcly returns
%% 'false' (i.e. the state transition is BAD).  Fun, good times.
%% Hooray about more systematic/mathematical reasoning, code
%% structure, and property-based testing.

projection_transition_is_sane(
  #projection_v1{epoch_number=Epoch1,
              epoch_csum=CSum1,
              creation_time=CreationTime1,
              author_server=AuthorServer1,
              all_members=All_list1,
              down=Down_list1,
              upi=UPI_list1,
              repairing=Repairing_list1,
              dbg=Dbg1} = P1,
  #projection_v1{epoch_number=Epoch2,
              epoch_csum=CSum2,
              creation_time=CreationTime2,
              author_server=AuthorServer2,
              all_members=All_list2,
              down=Down_list2,
              upi=UPI_list2,
              repairing=Repairing_list2,
              dbg=Dbg2} = P2,
  RelativeToServer, RetrospectiveP) ->
 try
    put(why1, []),
    %% General notes:
    %%
    %% I'm making no attempt to be "efficient" here.  All of these data
    %% structures are small, and they're not called zillions of times per
    %% second.
    %%
    %% The chain sequence/order checks at the bottom of this function aren't
    %% as easy-to-read as they ought to be.  However, I'm moderately confident
    %% that it isn't buggy.  TODO: refactor them for clarity.

    true = is_integer(Epoch1) andalso is_integer(Epoch2),
    true = is_binary(CSum1) andalso is_binary(CSum2),
    {_,_,_} = CreationTime1,
    {_,_,_} = CreationTime2,
    true = is_atom(AuthorServer1) andalso is_atom(AuthorServer2), % todo type may change?
    true = is_list(All_list1) andalso is_list(All_list2),
    true = is_list(Down_list1) andalso is_list(Down_list2),
    true = is_list(UPI_list1) andalso is_list(UPI_list2),
    true = is_list(Repairing_list1) andalso is_list(Repairing_list2),
    true = is_list(Dbg1) andalso is_list(Dbg2),

    true = Epoch2 > Epoch1,
    All_list1 = All_list2,                 % todo will probably change

    %% No duplicates
    true = lists:sort(Down_list2) == lists:usort(Down_list2),
    true = lists:sort(UPI_list2) == lists:usort(UPI_list2),
    true = lists:sort(Repairing_list2) == lists:usort(Repairing_list2),

    %% Disjoint-ness
    true = lists:sort(All_list2) == lists:sort(Down_list2 ++ UPI_list2 ++
                                                   Repairing_list2),
    [] = [X || X <- Down_list2, not lists:member(X, All_list2)],
    [] = [X || X <- UPI_list2, not lists:member(X, All_list2)],
    [] = [X || X <- Repairing_list2, not lists:member(X, All_list2)],
    DownS2 = sets:from_list(Down_list2),
    UPIS2 = sets:from_list(UPI_list2),
    RepairingS2 = sets:from_list(Repairing_list2),
    true = sets:is_disjoint(DownS2, UPIS2),
    true = sets:is_disjoint(DownS2, RepairingS2),
    true = sets:is_disjoint(UPIS2, RepairingS2),

    %% Additions to the UPI chain may only be at the tail
    UPI_common_prefix = find_common_prefix(UPI_list1, UPI_list2),
    true =
     if UPI_common_prefix == [] ->
            if UPI_list1 == [] orelse UPI_list2 == [] ->
                    %% If the common prefix is empty, then one of the
                    %% inputs must be empty.
                    ?RETURN1(true);
               true ->
                    %% Otherwise, we have a case of UPI changing from
                    %% one of these two situations:
                    %%
                    %% UPI_list1 -> UPI_list2
                    %% -------------------------------------------------
                    %% [d,c,b,a] -> [c,a]
                    %% [d,c,b,a] -> [c,a,repair_finished_added_to_tail].
                    NotUPI2 = (Down_list2 ++ Repairing_list2),
                    case lists:prefix(UPI_list1 -- NotUPI2, UPI_list2) of
                        true ->
                            ?RETURN1(true);
                        false ->
                            %% Here's a possible failure scenario:
                            %% UPI_list1        -> UPI_list2
                            %% Repairing_list1  -> Repairing_list2
                            %% -----------------------------------
                            %% [a,b,c] author=a -> [c,a] author=c
                            %% []                  [b]
                            %% 
                            %% ... where RelativeToServer=b.  In this case, b
                            %% has been partitioned for a while and has only
                            %% now just learned of several epoch transitions.
                            %% If the author of both is also in the UPI of
                            %% both, then those authors would not have allowed
                            %% a bad transition, so we will assume this
                            %% transition is OK.
                            ?RETURN1(
                            lists:member(AuthorServer1, UPI_list1)
                            andalso
                            lists:member(AuthorServer2, UPI_list2)
                            )
                    end
            end;
       true ->
            ?RETURN1(true)
    end,
    true = lists:prefix(UPI_common_prefix, UPI_list1),
    true = lists:prefix(UPI_common_prefix, UPI_list2),
    UPI_1_suffix = UPI_list1 -- UPI_common_prefix,
    UPI_2_suffix = UPI_list2 -- UPI_common_prefix,
    _ = ?RETURN1(yo),

    MoreCheckingP =
        RelativeToServer == undefined
        orelse
        not (lists:member(RelativeToServer, Down_list2) orelse
             lists:member(RelativeToServer, Repairing_list2)),
    _ = ?RETURN1(yo),
    
    UPIs_are_disjointP = ordsets:is_disjoint(ordsets:from_list(UPI_list1),
                                             ordsets:from_list(UPI_list2)),
    case UPI_2_suffix -- UPI_list1 of
        [] ->
            ?RETURN1(true);
        [_|_] = _Added_by_2 ->
            if RetrospectiveP ->
                    %% Any servers added to the UPI must be added from the
                    %% repairing list ... but in retrospective mode (where
                    %% we're checking only the transitions where all
                    %% UPI+repairing participants have unanimous private
                    %% projections!), and if we're under asymmetric
                    %% partition/churn, then we may not see the repairing
                    %% list.  So we will not check that condition here.
                    ?RETURN1(true);
               not RetrospectiveP ->
                    %% We're not retrospective.  So, if some server was
                    %% added by to the UPI, then that means that it was
                    %% added by repair.  And repair is coordinated by the
                    %% UPI tail/last.
%io:format(user, "g: UPI_list1=~w, UPI_list2=~w, UPI_2_suffix=~w, ",
%          [UPI_list1, UPI_list2, UPI_2_suffix]),
%io:format(user, "g", []),
                    ?RETURN1(true = UPI_list1 == [] orelse
                             UPIs_are_disjointP orelse
                             (lists:last(UPI_list1) == AuthorServer2) )
            end
    end,

    if not MoreCheckingP ->
            ?RETURN1(ok);
        MoreCheckingP ->
            %% Where did elements in UPI_2_suffix come from?
            %% Only two sources are permitted.
            Oops_check_UPI_2_suffix =
                [lists:member(X, Repairing_list1) % X added after repair done
                 orelse
                 lists:member(X, UPI_list1)  % X in UPI_list1 after common pref
                 || X <- UPI_2_suffix],
            %% Grrrrr, ok, so this check isn't good, at least at bootstrap time.
            %% TODO: false = lists:member(false, Oops_check_UPI_2_suffix),

            %% The UPI_2_suffix must exactly be equal to: ordered items from
            %% UPI_list1 concat'ed with ordered items from Repairing_list1.
            %% Both temp vars below preserve relative order!
            UPI_2_suffix_from_UPI1 = [X || X <- UPI_1_suffix,
                                           lists:member(X, UPI_list2)],
            UPI_2_suffix_from_Repairing1 = [X || X <- UPI_2_suffix,
                                                 lists:member(X, Repairing_list1)],
            %% true?
            UPI_2_concat = (UPI_2_suffix_from_UPI1 ++ UPI_2_suffix_from_Repairing1),
            if UPI_2_suffix == UPI_2_concat ->
                    ?RETURN1(ok);
               true ->
                    %% 'make dialyzer' will believe that this can never succeed.
                    %% 'make dialyzer-test' will not complain, however.
                    if RetrospectiveP ->
                            %% We are in retrospective mode.  But there are
                            %% some transitions that are difficult to find
                            %% when standing outside of all of the FLUs and
                            %% examining their behavior.  (In contrast to
                            %% this same function being called "in the path"
                            %% of a projection transition by a particular FLU
                            %% which knows exactly its prior projection and
                            %% exactly what it intends to do.)  Perhaps this
                            %% exception clause here can go away with
                            %% better/more clever retrospection analysis?
                            %%
                            %% Here's a case that PULSE found:
                            %% FLU B:
                            %%   E=257: UPI=[c,a], REPAIRING=[b]
                            %%   E=284: UPI=[c,a], REPAIRING=[b]
                            %% FLU a:
                            %%   E=251: UPI=[c], REPAIRING=[a,b]
                            %%   E=284: UPI=[c,a], REPAIRING=[b]
                            %% FLU c:
                            %%   E=282: UPI=[c], REPAIRING=[a,b]
                            %%   E=284: UPI=[c,a], REPAIRING=[b]
                            %%
                            %% From the perspective of each individual FLU,
                            %% the unanimous transition at epoch #284 is
                            %% good.  The repair that is done by FLU c -> a
                            %% is likewise good.
                            %%
                            %% From a retrospective point of view (and the
                            %% current implementation), there's a bad-looking
                            %% transition from epoch #269 to #284.  This is
                            %% from the point of view of the last two
                            %% unanimous private projection store epochs:
                            %%
                            %%   E=269: UPI=[c], REPAIRING=[], DOWN=[a,b]
                            %%   E=284: UPI=[c,a], REPAIRING=[b]
                            %%
                            %% The retrospective view by
                            %% machi_chain_manager1_pulse.erl just can't
                            %% reason correctly about this situation.  We
                            %% will instead rely on the non-retrospective
                            %% sanity checking that each FLU does before it
                            %% writes to its private projection store and
                            %% then adopts that projection (and unwedges
                            %% itself, etc etc).

                            if UPIs_are_disjointP ->
                                    ?RETURN1(true);
                               true ->
                                    ?RETURN1(todo),
                                    exit({todo, revisit, ?MODULE, ?LINE,
                                          [
                                           {oops_check_UPI_2_suffix, Oops_check_UPI_2_suffix},
                                           {upi_2_suffix, UPI_2_suffix},
                                           {upi_2_concat, UPI_2_concat},
                                           {retrospectivep, RetrospectiveP}
                                          ]}),
                                    io:format(user, "|~p,~p TODO revisit|",
                                              [?MODULE, ?LINE]),
                                    ok
                            end;
                       true ->
                            %% The following is OK: We're shifting from a
                            %% normal projection to an inner one.  The old
                            %% normal has a UPI that has nothing to do with
                            %% RelativeToServer a.k.a. me.
                            %% Or else the UPI_list1 is empty, and I'm
                            %% the only member of UPI_list2
                            %% But the new/suffix is definitely me.
                            %% from:
                            %% {epoch,847},{author,c},{upi,[c]},{repair,[]},
                            %%                                   {down,[a,b,d]}
                            %% to:
                            %% {epoch,848},{author,a},{upi,[a]},{repair,[]},
                            %%                                   {down,[b,c,d]}
                            FirstCase_p = (UPI_2_suffix == [AuthorServer2])
                                andalso
                                ((inner_projection_exists(P1) == false
                                  andalso
                                  inner_projection_exists(P2) == true)
                                 orelse UPI_list1 == []),

                            %% Here's another case that's alright:
                            %%
                            %% {a,{err,exit,
                            %%     {upi_2_suffix_error,[c]}, ....
                            %%
                            %% from:
                            %% {epoch,937},{author,a},{upi,[a,b]},{repair,[]},
                            %%                                       {down,[c]}
                            %% to:
                            %% {epoch,943},{author,a},{upi,{a,b,c},{repair,[]},
                            %%                                        {down,[]}

                            %% The author server doesn't matter.  However,
                            %% there were two other epochs in between, 939
                            %% and 941, where there wasn't universal agreement
                            %% of private projections.  The repair controller
                            %% at the tail, 'b', had decided that the repair
                            %% of 'c' was finished @ epoch 941.
                            SecondCase_p = ((UPI_2_suffix -- Repairing_list1)
                                            == []),
                            if FirstCase_p ->
                                    ?RETURN1(true);
                               SecondCase_p ->
                                    ?RETURN1(true);
                               UPIs_are_disjointP ->
                                    %% If there's no overlap at all between
                                    %% UPI_list1 & UPI_list2, then we're OK
                                    %% here.
                                    ?RETURN1(true);
                               true ->
                                    exit({upi_2_suffix_error, UPI_2_suffix})
                            end
                    end
            end
    end,
    ?RETURN1(true)
 catch
     _Type:_Err ->
         ?RETURN1(oops),
         S1 = machi_projection:make_summary(P1),
         S2 = machi_projection:make_summary(P2),
         Trace = erlang:get_stacktrace(),
         {err, _Type, _Err, from, S1, to, S2, relative_to, RelativeToServer,
          history, (catch lists:sort([no_history])),
          stack, Trace}
 end.

find_common_prefix([], _) ->
    [];
find_common_prefix(_, []) ->
    [];
find_common_prefix([H|L1], [H|L2]) ->
    [H|find_common_prefix(L1, L2)];
find_common_prefix(_, _) ->
    [].

inner_projection_exists(#projection_v1{inner=undefined}) ->
    false;
inner_projection_exists(#projection_v1{inner=_}) ->
    true.
