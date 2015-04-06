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

-define(NOT_FLAPPING, {0,0,0}).

-type projection() :: #projection_v1{}.

-record(ch_mgr, {
          init_finished   :: boolean(),
          name            :: pv1_server(),
          proj            :: projection(),
          proj_history    :: queue(),
          myflu           :: pid() | atom(),
          flap_limit      :: non_neg_integer(),
          %%
          runenv          :: list(), %proplist()
          opts            :: list(),  %proplist()
          flaps=0         :: integer(),
          flap_start = ?NOT_FLAPPING
                          :: erlang:now(),

          %% Deprecated ... TODO: remove when old test unit test code is removed
          proj_proposed   :: 'none' | projection()
         }).
