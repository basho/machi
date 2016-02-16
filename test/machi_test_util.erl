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

-module(machi_test_util).
-compile(export_all).

-ifdef(TEST).
-ifndef(PULSE).

-include_lib("eunit/include/eunit.hrl").

-include("machi.hrl").
-include("machi_projection.hrl").

-define(FLU, machi_flu1).
-define(FLU_C, machi_flu1_client).

-spec start_flu_package(atom(), inet:port_number(), string()) ->
                               {Ps::[#p_srvr{}], MgrNames::[atom()], Dirs::[string()]}.
start_flu_package(FluName, TcpPort, DataDir) ->
    start_flu_package(FluName, TcpPort, DataDir, []).

-spec start_flu_package(atom(), inet:port_number(), string(), list()) ->
                               {Ps::[#p_srvr{}], MgrNames::[atom()], Dirs::[string()]}.
start_flu_package(FluName, TcpPort, DataDir, Props) ->
    MgrName = machi_flu_psup:make_mgr_supname(FluName),
    FluInfo = [{#p_srvr{name=FluName, address="localhost", port=TcpPort,
                        props=[{chmgr, MgrName}, {data_dir, DataDir} | Props]},
                DataDir, MgrName}],
    start_flu_packages(FluInfo).

-spec start_flu_packages(pos_integer(), inet:port_number(), string(), list()) ->
                                {Ps::[#p_srvr{}], MgrNames::[atom()], Dirs::[string()]}.
start_flu_packages(FluCount, BaseTcpPort, DirPrefix, Props) ->
    FluInfo = flu_info(FluCount, BaseTcpPort, DirPrefix, Props),
    start_flu_packages(FluInfo).

start_flu_packages(FluInfo) ->
    _ = stop_machi_sup(),
    clean_up(FluInfo),
    {ok, _SupPid} = machi_sup:start_link(),
    [{ok, _} = machi_flu_psup:start_flu_package(Name, Port, Dir, Props) ||
        {#p_srvr{name=Name, port=Port, props=Props}, Dir, _} <- FluInfo],
    {Ps, Dirs, MgrNames} = lists:unzip3(FluInfo),
    {Ps, MgrNames, Dirs}.

stop_flu_package() ->
    stop_flu_packages().

stop_flu_packages() ->
    stop_machi_sup().

flu_info(FluCount, BaseTcpPort, DirPrefix, Props) ->
    [begin
         FLUNameStr = [$a + I - 1],
         FLUName = list_to_atom(FLUNameStr),
         MgrName = machi_flu_psup:make_mgr_supname(FLUName),
         DataDir = DirPrefix ++ "/data.eqc." ++ FLUNameStr,
         {#p_srvr{name=FLUName, address="localhost", port=BaseTcpPort + I,
                  props=[{chmgr, MgrName}, {data_dir, DataDir} | Props]},
          DataDir, MgrName}
     end || I <- lists:seq(1, FluCount)].

stop_machi_sup() ->
    case whereis(machi_sup) of
        undefined -> ok;
        Pid ->
            catch exit(whereis(machi_sup), normal),
            machi_util:wait_for_death(Pid, 100)
    end.

clean_up(FluInfo) ->
    _ = [begin
             case proplists:get_value(no_cleanup, Props) of
                 true -> ok;
                 _ ->
                     _ = machi_flu1:stop(FLUName),
                     clean_up_dir(Dir)
             end
         end || {#p_srvr{name=FLUName, props=Props}, Dir, _} <- FluInfo],
    ok.

clean_up_dir(Dir) ->
    [begin
         Fs = filelib:wildcard(Dir ++ Glob),
         [file:delete(F) || F <- Fs],
         [file:del_dir(F) || F <- Fs]
     end || Glob <- ["*/*/*/*", "*/*/*", "*/*", "*"] ],
    _ = file:del_dir(Dir),
    ok.

-endif. % !PULSE
-endif. % TEST

