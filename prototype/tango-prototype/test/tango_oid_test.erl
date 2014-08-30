%% -------------------------------------------------------------------
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

-module(tango_oid_test).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.
-endif.

tango_oid_smoke_test() ->
    ok = tango_test:run_test("/tmp", "tango_oid_smoke", 4096, 5*1024, 1,
                             fun tango_oid_smoke_test_int/3).

tango_oid_smoke_test_int(_PageSize, Seq, Proj) ->
    {ok, OID_Map} = tango_oid:start_link(Seq, Proj),
    ok = tango_oid:stop(OID_Map),

    ok.
