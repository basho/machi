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

-module(machi_projection_test).

-ifdef(TEST).
-compile(export_all).

-include("machi_projection.hrl").

new_test() ->
    %% Bleh, hey QuickCheck ... except that any model probably equals
    %% code under test, bleh.
    true = try_it(a, [a,b,c], [a,b], [], [c], []),
    true = try_it(<<"a">>, [<<"a">>,b,c], [<<"a">>,b], [], [c], []),
    Servers =      [#p_srvr{name=a}, #p_srvr{name=b}, #p_srvr{name=c}],
    Servers_bad1 = [#p_srvr{name= <<"a">>}, #p_srvr{name=b}, #p_srvr{name=c}],
    Servers_bad2 = [#p_srvr{name=z},       #p_srvr{name=b}, #p_srvr{name=c}],
    true = try_it(a, Servers, [a,b], [], [c], []),

    false = try_it(a, not_list, [a,b], [], [c], []),
    false = try_it(a, [a,b,c], not_list, [], [c], []),
    false = try_it(a, [a,b,c], [a,b], not_list, [c], []),
    false = try_it(a, [a,b,c], [a,b], [], not_list, []),
    false = try_it(a, [a,b,c], [a,b], [], [c], not_list),

    false = try_it(<<"x">>, [a,b,c], [a,b], [], [c], []),
    false = try_it(a, [a,b,c], [a,b,c], [], [c], []),
    false = try_it(a, [a,b,c], [a,b], [c], [c], []),
    false = try_it(a, [a,b,c], [a,b], [], [c,c], []),
    false = try_it(a, Servers_bad1, [a,b], [], [c], []),
    false = try_it(a, Servers_bad2, [a,b], [], [c], []),

    ok.

compare_test() ->
    P0  = machi_projection:new(0, a, [a,b,c], [a,b], [], [c], []),
    P1a = machi_projection:new(1, a, [a,b,c], [a,b], [], [c], []),
    P1b = machi_projection:new(1, b, [a,b,c], [a,b], [], [c], []),
    P2  = machi_projection:new(2, a, [a,b,c], [a,b], [], [c], []),

    0  = machi_projection:compare(P0,  P0),
    -1 = machi_projection:compare(P0,  P1a),
    -1 = machi_projection:compare(P1a, P1b),
    -1 = machi_projection:compare(P1b, P1a),
     1 = machi_projection:compare(P2,  P1a),
     1 = machi_projection:compare(P2,  P1b),
     1 = machi_projection:compare(P2,  P0),
    ok.

try_it(MyName, All_list, UPI_list, Down_list, Repairing_list, Ps) ->
    try
        P = machi_projection:new(MyName, All_list, UPI_list, Down_list,
                                 Repairing_list, Ps),
        is_record(P, projection_v1)
    catch _:_ ->
            false
    end.

-endif. % TEST
