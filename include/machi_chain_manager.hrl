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

-include("machi_projection.hrl").

-define(NOT_FLAPPING, {0,0,0}).

-type projection() :: #projection_v1{}.

-record(ch_mgr, {
          name            :: pv1_server(),
          flap_limit      :: non_neg_integer(),
          proj            :: projection(),
          %%
          timer           :: 'undefined' | reference(),
          proj_history    :: queue(),
          flaps=0         :: integer(),
          flap_start = ?NOT_FLAPPING
                          :: erlang:now(),
          runenv          :: list(), %proplist()
          opts            :: list(),  %proplist()
          members_dict    :: p_srvr_dict(),
          proxies_dict    :: orddict:orddict(pv1_server(), pid())
         }).
