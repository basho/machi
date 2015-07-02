%% -------------------------------------------------------------------
%%
%% Machi: a small village of replicated files
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
-module(machi_chain_manager1_test).

-include("machi.hrl").
-include("machi_projection.hrl").

-define(MGR, machi_chain_manager1).

-define(D(X), io:format(user, "~s ~p\n", [??X, X])).
-define(Dw(X), io:format(user, "~s ~w\n", [??X, X])).
-define(FLU_C,  machi_flu1_client).
-define(FLU_PC, machi_proxy_flu1_client).

-export([]).

-ifdef(TEST).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
%% -include_lib("eqc/include/eqc_statem.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

%% Example:
%%   [{1,{ok_disjoint,[{agreed_membership,{[a],[b,c]}}]}},
%%    {3,{ok_disjoint,[{agreed_membership,{[a],[b,c]}}]}},
%%    {8,
%%     {ok_disjoint,[{not_agreed,{[a],[b,c]},
%%                               [{b,not_in_this_epoch},
%%                                <<65,159,66,113,232,15,156,244,197,
%%                                  210,39,82,229,84,192,19,27,45,161,38>>]}]}},
%%    {10,{ok_disjoint,[{agreed_membership,{[c],[]}}]}},
%%    ...]

unanimous_report(Namez) ->
    UniquePrivateEs =
        lists:usort(lists:flatten(
                      [element(2, ?FLU_PC:list_all_projections(FLU, private)) ||
                          {_FLUName, FLU} <- Namez])),
    [unanimous_report(Epoch, Namez) || Epoch <- UniquePrivateEs,
                                       Epoch /= 0].

unanimous_report(Epoch, Namez) ->
    Projs = [{FLUName,
              case ?FLU_PC:read_projection(FLU, private, Epoch) of
                  {ok, T} ->
                      machi_chain_manager1:inner_projection_or_self(T);
                  _Else ->
                      {FLUName, not_in_this_epoch}
              end} || {FLUName, FLU} <- Namez],
    UPI_R_Sums = [{Proj#projection_v1.upi, Proj#projection_v1.repairing,
                   Proj#projection_v1.epoch_csum} ||
                     {_FLUname, Proj} <- Projs,
                     is_record(Proj, projection_v1)],
    UniqueUPIs = lists:usort([UPI || {UPI, _Repairing, _CSum} <- UPI_R_Sums]),
    Res =
        [begin
             case lists:usort([CSum || {U, _Repairing, CSum} <- UPI_R_Sums,
                                       U == UPI]) of
                 [_1CSum] ->
                     %% Yay, there's only 1 checksum.  Let's check
                     %% that all FLUs are in agreement.
                     {UPI, Repairing, _CSum} =
                         lists:keyfind(UPI, 1, UPI_R_Sums),
                     Tmp = [{FLU, case proplists:get_value(FLU, Projs) of
                                      P when is_record(P, projection_v1) ->
                                          P#projection_v1.epoch_csum;
                                      Else ->
                                          Else
                                  end} || FLU <- UPI ++ Repairing],
                     case lists:usort([CSum || {_FLU, CSum} <- Tmp]) of
                         [_] ->
                             {agreed_membership, {UPI, Repairing}};
                         Else2 ->
                             {not_agreed, {UPI, Repairing}, Else2}
                     end;
                 _Else ->
                     {not_agreed, {undefined, undefined}, Projs}
             end
         end || UPI <- UniqueUPIs],
    AgreedResUPI_Rs = [UPI++Repairing ||
                          {agreed_membership, {UPI, Repairing}} <- Res],
    Tag = case lists:usort(lists:flatten(AgreedResUPI_Rs)) ==
               lists:sort(lists:flatten(AgreedResUPI_Rs)) of
              true ->
                  ok_disjoint;
              false ->
                  bummer_NOT_DISJOINT
          end,
    {Epoch, {Tag, Res}}.

all_reports_are_disjoint(Report) ->
    [] == [X || {_Epoch, Tuple}=X <- Report,
                element(1, Tuple) /= ok_disjoint].

extract_chains_relative_to_flu(FLU, Report) ->
    {FLU, [{Epoch, UPI, Repairing} ||
              {Epoch, {ok_disjoint, Es}} <- Report,
              {agreed_membership, {UPI, Repairing}} <- Es,
              lists:member(FLU, UPI) orelse lists:member(FLU, Repairing)]}.

chain_to_projection(MyName, Epoch, UPI_list, Repairing_list, All_list) ->
    MemberDict = orddict:from_list([{FLU, #p_srvr{name=FLU}} ||
                                       FLU <- All_list]),
    machi_projection:new(Epoch, MyName, MemberDict,
                         All_list -- (UPI_list ++ Repairing_list),
                         UPI_list, Repairing_list, [{artificial_by, ?MODULE}]).

-ifndef(PULSE).

smoke0_test() ->
    {ok, _} = machi_partition_simulator:start_link({1,2,3}, 50, 50),
    Host = "localhost",
    TcpPort = 6623,
    {ok, FLUa} = machi_flu1:start_link([{a,TcpPort,"./data.a"}]),
    Pa = #p_srvr{name=a, address=Host, port=TcpPort},
    Members_Dict = machi_projection:make_members_dict([Pa]),
    %% Egadz, more racing on startup, yay.  TODO fix.
    timer:sleep(1),
    {ok, FLUaP} = ?FLU_PC:start_link(Pa),
    {ok, M0} = ?MGR:start_link(a, Members_Dict, [{active_mode, false}]),
    _SockA = machi_util:connect(Host, TcpPort),
    try
        pong = ?MGR:ping(M0)
    after
        ok = ?MGR:stop(M0),
        ok = machi_flu1:stop(FLUa),
        ok = ?FLU_PC:quit(FLUaP),
        ok = machi_partition_simulator:stop()
    end.

smoke1_test() ->
    machi_partition_simulator:start_link({1,2,3}, 100, 0),
    TcpPort = 62777,
    FluInfo = [{a,TcpPort+0,"./data.a"}, {b,TcpPort+1,"./data.b"}, {c,TcpPort+2,"./data.c"}],
    P_s = [#p_srvr{name=Name, address="localhost", port=Port} ||
              {Name,Port,_Dir} <- FluInfo],

    [machi_flu1_test:clean_up_data_dir(Dir) || {_,_,Dir} <- FluInfo],
    FLUs = [element(2, machi_flu1:start_link([{Name,Port,Dir}])) ||
               {Name,Port,Dir} <- FluInfo],
    MembersDict = machi_projection:make_members_dict(P_s),
    {ok, M0} = ?MGR:start_link(a, MembersDict, [{active_mode,false}]),
    try
        {ok, P1} = ?MGR:test_calc_projection(M0, false),
        % DERP! Check for race with manager's proxy vs. proj listener
        case ?MGR:test_read_latest_public_projection(M0, false) of
            {error, partition} -> timer:sleep(500);
            _                  -> ok
        end,
        {local_write_result, ok,
         {remote_write_results, [{b,ok},{c,ok}]}} =
            ?MGR:test_write_public_projection(M0, P1),
        {unanimous, P1, Extra1} = ?MGR:test_read_latest_public_projection(M0, false),

        ok
    after
        ok = ?MGR:stop(M0),
        [ok = machi_flu1:stop(X) || X <- FLUs],
        ok = machi_partition_simulator:stop()
    end.

nonunanimous_setup_and_fix_test() ->
    %% TODO attack list:
    %% __ Add start option to chain manager to be "passive" only, i.e.,
    %%    not immediately go to work on
    %% 1. Start FLUs with full complement of FLU+proj+chmgr.
    %% 2. Put each of them under a supervisor?
    %%    - Sup proc could be a created-specifically-for-test thing, perhaps?
    %%      Rather than relying on a supervisor with reg name + OTP app started
    %%      plus plus more more yaddayadda?
    %% 3. Add projection catalog/orddict of #p_srvr records??
    %% 4. Fix this test, etc etc.
    machi_partition_simulator:start_link({1,2,3}, 100, 0),
    TcpPort = 62877,
    FluInfo = [{a,TcpPort+0,"./data.a"}, {b,TcpPort+1,"./data.b"}],
    P_s = [#p_srvr{name=Name, address="localhost", port=Port} ||
              {Name,Port,_Dir} <- FluInfo],
    
    [machi_flu1_test:clean_up_data_dir(Dir) || {_,_,Dir} <- FluInfo],
    FLUs = [element(2, machi_flu1:start_link([{Name,Port,Dir}])) ||
               {Name,Port,Dir} <- FluInfo],
    [Proxy_a, Proxy_b] = Proxies =
        [element(2,?FLU_PC:start_link(P)) || P <- P_s],
    MembersDict = machi_projection:make_members_dict(P_s),
    XX = [],
    %% XX = [{private_write_verbose,true}],
    {ok, Ma} = ?MGR:start_link(a, MembersDict, [{active_mode, false}]++XX),
    {ok, Mb} = ?MGR:start_link(b, MembersDict, [{active_mode, false}]++XX),
    try
        {ok, P1} = ?MGR:test_calc_projection(Ma, false),

        P1a = machi_projection:update_checksum(
                 P1#projection_v1{down=[b], upi=[a], dbg=[{hackhack, ?LINE}]}),
        P1b = machi_projection:update_checksum(
                 P1#projection_v1{author_server=b, creation_time=now(),
                                  down=[a], upi=[b], dbg=[{hackhack, ?LINE}]}),
        %% Scribble different projections
        ok = ?FLU_PC:write_projection(Proxy_a, public, P1a),
        ok = ?FLU_PC:write_projection(Proxy_b, public, P1b),

        %% ?D(x),
        {not_unanimous,_,_}=_XX = ?MGR:test_read_latest_public_projection(Ma, false),
        %% ?Dw(_XX),
        {not_unanimous,_,_}=_YY = ?MGR:test_read_latest_public_projection(Ma, true),
        %% The read repair here doesn't automatically trigger the creation of
        %% a new projection (to try to create a unanimous projection).  So
        %% we expect nothing to change when called again.
        {not_unanimous,_,_}=_YY = ?MGR:test_read_latest_public_projection(Ma, true),

        {now_using, _, EpochNum_a} = ?MGR:test_react_to_env(Ma),
        {no_change, _, EpochNum_a} = ?MGR:test_react_to_env(Ma),
        {unanimous,P2,_E2} = ?MGR:test_read_latest_public_projection(Ma, false),
        {ok, P2pa} = ?FLU_PC:read_latest_projection(Proxy_a, private),
        P2 = P2pa#projection_v1{dbg2=[]},

        %% %% FLUb should have nothing written to private because it hasn't
        %% %% reacted yet.
        %% {error, not_written} = ?FLU_PC:read_latest_projection(Proxy_b, private),

        %% %% Poke FLUb to react ... should be using the same private proj
        %% %% as FLUa.
        %% {now_using, _, EpochNum_a} = ?MGR:test_react_to_env(Mb),
        {ok, P2pb} = ?FLU_PC:read_latest_projection(Proxy_b, private),
        P2 = P2pb#projection_v1{dbg2=[]},

timer:sleep(3000),
        ok
    after
        ok = ?MGR:stop(Ma),
        ok = ?MGR:stop(Mb),
        [ok = ?FLU_PC:quit(X) || X <- Proxies],
        [ok = machi_flu1:stop(X) || X <- FLUs],
        ok = machi_partition_simulator:stop()
    end.

-endif. % !PULSE
-endif. % TEST
