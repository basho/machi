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

-module(machi_projection_store_test).

-ifdef(TEST).
-ifndef(PULSE).

-compile(export_all).
-define(PS, machi_projection_store).

-include("machi_projection.hrl").

smoke_test() ->
    PortBase = 64820,
    Dir = "./data.a",
    Os = [{ignore_stability_time, true}, {active_mode, false}],
    os:cmd("rm -rf " ++ Dir),
    machi_test_util:start_flu_package(a, PortBase, "./data.a", Os),

    try
        P1 = machi_projection:new(1, a, [], [], [], [], []),
        ok = ?PS:write(a_pstore, public, P1),
        {error, written} = ?PS:write(a_pstore, public, P1),

        Pbad = P1#projection_v1{epoch_number=99238}, % break checksum
        {error, bad_arg} = ?PS:write(a_pstore, public, Pbad),

        ok = ?PS:write(a_pstore, private, P1),
        P1a = machi_projection:update_checksum(P1#projection_v1{dbg=[diff_yo]}),
        {error, written} = ?PS:write(a_pstore, private, P1a),

        P1b = P1#projection_v1{dbg2=[version_b]},
        ok = ?PS:write(a_pstore, private, P1b),
        P1c = P1#projection_v1{dbg2=[version_c]},
        ok = ?PS:write(a_pstore, private, P1c),
        {error, written} = ?PS:write(a_pstore, private, P1a),

        ok = ?PS:set_consistency_mode(a_pstore, ap_mode),
        ok = ?PS:set_consistency_mode(a_pstore, cp_mode),

        ok
    after
        machi_test_util:stop_flu_package()
    end.

-endif. % !PULSE
-endif. % TEST
