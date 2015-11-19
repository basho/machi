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

-module(machi_psup_test_util).
-compile(export_all).

-ifdef(TEST).
-ifndef(PULSE).

-include_lib("eunit/include/eunit.hrl").

-include("machi.hrl").
-include("machi_projection.hrl").

-define(FLU, machi_flu1).
-define(FLU_C, machi_flu1_client).

-spec start_flu_packages(pos_integer(), string(), inet:port(), list()) ->
                                {MachiSup::pid(), Ps::[#p_srvr{}], MgrNames::[atom()]}.
start_flu_packages(FluCount, DirPrefix, BaseTcpPort, MgrOpts) ->
    FluInfo = flu_info(FluCount, DirPrefix, BaseTcpPort),
    _ = stop_machi_sup(),
    _ = [machi_flu1_test:clean_up_data_dir(Dir) || {_, Dir, _} <- FluInfo],
    {ok, SupPid} = machi_sup:start_link(),
    [{ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, MgrOpts) ||
        {#p_srvr{name=Name, port=Port}, Dir, _} <- FluInfo],
    {Ps, _Dirs, MgrNames} = lists:unzip3(FluInfo),
    {SupPid, Ps, MgrNames}.

stop_flu_packages() ->
    stop_machi_sup().

flu_info(FluCount, DirPrefix, BaseTcpPort) ->
    [begin
         FLUNameStr = [$a + I - 1],
         FLUName = list_to_atom(FLUNameStr),
         MgrName = machi_flu_psup:make_mgr_supname(FLUName),
         {#p_srvr{name=FLUName, address="localhost", port=BaseTcpPort + I,
                  props=[{chmgr, MgrName}]},
          DirPrefix ++ "/data.eqc." ++ FLUNameStr,
          MgrName}
     end || I <- lists:seq(1, FluCount)].

stop_machi_sup() ->
    case whereis(machi_sup) of
        undefined -> ok;
        Pid ->
            catch exit(whereis(machi_sup), normal),
            machi_util:wait_for_death(Pid, 30)
    end.

-endif. % !PULSE
-endif. % TEST

