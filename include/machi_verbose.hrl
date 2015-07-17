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

-ifdef(PULSE).
-define(V(Fmt, Args), pulse:format(Fmt, Args)).
-else.  % PULSE
-define(V(Fmt, Args), io:format(user, Fmt, Args)).
-endif. % PULSE

-define(D(X), ?V("~s ~p\n", [??X, X])).
-define(Dw(X), ?V("~s ~w\n", [??X, X])).

