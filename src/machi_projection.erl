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

-module(machi_projection).

-include("machi_projection.hrl").

-export([
         new/6, new/7, new/8,
         update_projection_checksum/1,
         update_projection_dbg2/2,
         compare/2,
         make_projection_summary/1
        ]).

new(MyName, All_list, UPI_list, Down_list, Repairing_list, Ps) ->
    new(0, MyName, All_list, Down_list, UPI_list, Repairing_list, Ps).

new(EpochNum, MyName, All_list, Down_list, UPI_list, Repairing_list, Dbg) ->
    new(EpochNum, MyName, All_list, Down_list, UPI_list, Repairing_list,
        Dbg, []).

new(EpochNum, MyName, All_list0, Down_list, UPI_list, Repairing_list,
    Dbg, Dbg2)
  when is_integer(EpochNum), EpochNum >= 0,
       is_atom(MyName) orelse is_binary(MyName),
       is_list(All_list0), is_list(Down_list), is_list(UPI_list),
       is_list(Repairing_list), is_list(Dbg), is_list(Dbg2) ->
    {All_list, MemberDict} =
        case lists:all(fun(P) when is_record(P, p_srvr) -> true;
                          (_)                           -> false
                       end, All_list0) of
            true ->
                All = [S#p_srvr.name || S <- All_list0],
                TmpL = [{S#p_srvr.name, S} || S <- All_list0],
                {All, orddict:from_list(TmpL)};
            false ->
                All_list1 = lists:zip(All_list0,lists:seq(0,length(All_list0)-1)),
                All_list2 = [#p_srvr{name=S, address="localhost",
                                     port=?MACHI_DEFAULT_TCP_PORT+I} ||
                                {S, I} <- All_list1],
                TmpL = [{S#p_srvr.name, S} || S <- All_list2],
                {All_list0, orddict:from_list(TmpL)}
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
    true = (AllSet == ordsets:union([DownSet, UPISet, RepairingSet])),
    true = ordsets:is_disjoint(DownSet, UPISet),
    true = ordsets:is_disjoint(DownSet, RepairingSet),
    true = ordsets:is_disjoint(UPISet, RepairingSet),

    P = #projection_v1{epoch_number=EpochNum,
                       creation_time=now(),
                       author_server=MyName,
                       all_members=All_list,
                       member_dict=MemberDict,
                       down=Down_list,
                       upi=UPI_list,
                       repairing=Repairing_list,
                       dbg=Dbg
                      },
    update_projection_dbg2(update_projection_checksum(P), Dbg2).

update_projection_checksum(P) ->
    CSum = crypto:hash(sha,
                       term_to_binary(P#projection_v1{epoch_csum= <<>>,
                                                      dbg2=[]})),
    P#projection_v1{epoch_csum=CSum}.

update_projection_dbg2(P, Dbg2) when is_list(Dbg2) ->
    P#projection_v1{dbg2=Dbg2}.

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

make_projection_summary(#projection_v1{epoch_number=EpochNum,
                                       all_members=_All_list,
                                       down=Down_list,
                                       author_server=Author,
                                       upi=UPI_list,
                                       repairing=Repairing_list,
                                       dbg=Dbg, dbg2=Dbg2}) ->
    [{epoch,EpochNum},{author,Author},
     {upi,UPI_list},{repair,Repairing_list},{down,Down_list},
     {d,Dbg}, {d2,Dbg2}].
