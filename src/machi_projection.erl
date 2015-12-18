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

-module(machi_projection).

-include("machi_projection.hrl").

-export([
         new/6, new/7, new/8,
         update_checksum/1,
         update_dbg2/2,
         compare/2,
         get_epoch_id/1,
         make_summary/1,
         make_members_dict/1,
         make_epoch_id/1
        ]).

%% @doc Create a new projection record.

new(MyName, MemberDict, Down_list, UPI_list, Repairing_list, Ps) ->
    new(0, MyName, MemberDict, Down_list, UPI_list, Repairing_list, Ps).

%% @doc Create a new projection record.

new(EpochNum, MyName, MemberDict, Down_list, UPI_list, Repairing_list, Dbg) ->
    new(EpochNum, MyName, MemberDict, Down_list, UPI_list, Repairing_list,
        Dbg, []).

%% @doc Create a new projection record.
%%
%% The `MemberDict0' argument may be a true `p_srvr_dict()' (i.e, it
%% is a well-formed `orddict' with the correct 2-tuple key-value form)
%% or it may be simply `list(p_srvr())', in which case we'll convert it
%% to a `p_srvr_dict()'.

new(EpochNum, MyName, [_|_] = MembersDict0, Down_list, UPI_list, Repairing_list,
    Dbg, Dbg2)
  when is_integer(EpochNum), EpochNum >= 0,
       is_atom(MyName) orelse is_binary(MyName),
       is_list(MembersDict0), is_list(Down_list), is_list(UPI_list),
       is_list(Repairing_list), is_list(Dbg), is_list(Dbg2) ->
    MembersDict = make_members_dict(MembersDict0),
    All_list = [Name || {Name, _P} <- MembersDict],
    if length(All_list) =< ?MAX_CHAIN_LENGTH ->
            ok;
       true ->
            exit(max_chain_length_error)
    end,
    true = lists:all(fun(X) when is_atom(X) orelse is_binary(X) -> true;
                        (_)                                     -> false
                     end, All_list),
    [true = lists:sort(SomeList) == lists:usort(SomeList) ||
        SomeList <- [All_list, Down_list, UPI_list, Repairing_list] ],
    AllSet = ordsets:from_list(All_list),
    DownSet = ordsets:from_list(Down_list),
    UPISet = ordsets:from_list(UPI_list),
    RepairingSet = ordsets:from_list(Repairing_list),

    true = ordsets:is_element(MyName, AllSet),
    true = ordsets:is_disjoint(DownSet, UPISet),
    true = ordsets:is_disjoint(DownSet, RepairingSet),
    true = ordsets:is_disjoint(UPISet, RepairingSet),

    P = #projection_v1{epoch_number=EpochNum,
                       creation_time=now(),
                       author_server=MyName,
                       all_members=All_list,
                       members_dict=MembersDict,
                       down=Down_list,
                       upi=UPI_list,
                       repairing=Repairing_list,
                       dbg=Dbg
                      },
    update_dbg2(update_checksum(P), Dbg2);
new(EpochNum, MyName, [] = _MembersDict0, _Down_list, _UPI_list,_Repairing_list,
    Dbg, Dbg2)
  when is_integer(EpochNum), EpochNum >= 0,
       is_atom(MyName) orelse is_binary(MyName) ->
    P = #projection_v1{epoch_number=EpochNum,
                       creation_time=now(),
                       author_server=MyName,
                       all_members=[],
                       members_dict=[],
                       down=[],
                       upi=[],
                       repairing=[],
                       dbg=Dbg
                      },
    update_dbg2(update_checksum(P), Dbg2).

%% @doc Update the checksum element of a projection record.

update_checksum(P) ->
    %% Fields that we ignore when calculating checksum:
    %% * epoch_csum
    %% * dbg2: humming consensus participants may modify this at will without
    %%         voiding the identity of the projection as a whole.
    %% * flap: In some cases in CP mode, coode upstream of C120 may have
    %%         updated the flapping information.  That's OK enough: we aren't
    %%         going to violate chain replication safety rules (or
    %%         accidentally encourage someone else sometime later) by
    %%         replacing flapping information with our own local view at
    %%         this instant in time.
    %% * creation_time: With CP mode & inner projections, it's damn annoying
    %%                  to have to copy this around 100% correctly.  {sigh}
    %%                  That's a negative state of the code.  However, there
    %%                  isn't a safety violation if the creation_time is
    %%                  altered for any reason: it's there only for human
    %%                  benefit for debugging.
    CSum = crypto:hash(sha,
                       term_to_binary(P#projection_v1{epoch_csum= <<>>,
                                                      creation_time=undefined,
                                                      dbg2=[]})),
    P#projection_v1{epoch_csum=CSum}.

%% @doc Update the `dbg2' element of a projection record.

update_dbg2(P, Dbg2) when is_list(Dbg2) ->
    P#projection_v1{dbg2=Dbg2}.

%% @doc Compare two projection records for equality (assuming that the
%% checksum element has been correctly calculated).
%%
%% The name "compare" is probably too close to "rank"?  This
%% comparison has nothing to do with projection ranking.
%% TODO: change the name of this function?

-spec compare(#projection_v1{}, #projection_v1{}) ->
      integer().
compare(#projection_v1{epoch_number=E1, epoch_csum=C1},
        #projection_v1{epoch_number=E1, epoch_csum=C1}) ->
    0;
compare(#projection_v1{epoch_number=E1},
        #projection_v1{epoch_number=E2}) ->
    if E1 =< E2 -> -1;
       E1 >  E2 ->  1
    end.

%% @doc Return the epoch_id of the projection.

get_epoch_id(#projection_v1{epoch_number=Epoch, epoch_csum=CSum}) ->
    {Epoch, CSum}.

%% @doc Create a proplist-style summary of a projection record.

make_summary(#projection_v1{epoch_number=EpochNum,
                            epoch_csum= <<_CSum4:4/binary, _/binary>>,
                            all_members=_All_list,
                            mode=CMode,
                            witnesses=Witness_list,
                            down=Down_list,
                            author_server=Author,
                            upi=UPI_list,
                            repairing=Repairing_list,
                            dbg=Dbg, dbg2=Dbg2}) ->
    [{epoch,EpochNum}, {csum,_CSum4},
{all, _All_list},
     {author,Author}, {mode,CMode},{witnesses, Witness_list},
     {upi,UPI_list},{repair,Repairing_list},{down,Down_list}] ++
        [{d,Dbg}, {d2,Dbg2}].

%% @doc Make a `p_srvr_dict()' out of a list of `p_srvr()' or out of a
%% `p_srvr_dict()'.
%%
%% If `Ps' is a `p_srvr_dict()', then this function is usually a
%% no-op.  However, if someone has tampered with the list and screwed
%% up its order, then we should fix it so `orddict' can work
%% correctly.
%%
%% If `Ps' is simply `list(p_srvr())', in which case we'll convert it
%% to a `p_srvr_dict()'.

-spec make_members_dict(list(p_srvr()) | p_srvr_dict()) ->
      p_srvr_dict().
make_members_dict(Ps) ->
    F_rec = fun(P) when is_record(P, p_srvr) -> true;
                      (_)                    -> false
               end,
    F_tup = fun({_K, P}) when is_record(P, p_srvr) -> true;
               (_)                                 -> false
            end,
    case lists:all(F_rec, Ps) of
        true ->
            orddict:from_list([{P#p_srvr.name, P} || P <- Ps]);
        false ->
            case lists:all(F_tup, Ps) of
                true ->
                    orddict:from_list(Ps);
                false ->
                    F_neither = fun(X) -> not (F_rec(X) or F_tup(X)) end,
                    exit({badarg, {make_members_dict, lists:filter(F_neither, Ps)}})
            end
    end.

make_epoch_id(#projection_v1{epoch_number=Epoch, epoch_csum=CSum}) ->
    {Epoch, CSum}.
