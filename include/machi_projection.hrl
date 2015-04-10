%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-ifndef(MACHI_PROJECTION_HRL).
-define(MACHI_PROJECTION_HRL, true).

-type pv1_csum()      :: binary().
-type pv1_epoch()     :: {pv1_epoch_n(), pv1_csum()}.
-type pv1_epoch_n()   :: non_neg_integer().
-type pv1_server()    :: atom() | binary().
-type pv1_timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

-record(p_srvr, {
          name            :: pv1_server(),
          proto = 'ipv4'  :: 'ipv4' | 'disterl', % disterl? Hrm.
          address         :: term(), % Protocol-specific
          port            :: term(), % Protocol-specific
          props = []      :: list()  % proplist for other related info
         }).

-type p_srvr() :: #p_srvr{}.
-type p_srvr_dict() :: orddict:orddict().

-define(DUMMY_PV1_EPOCH, {0,<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>}).

-record(projection_v1, {
          epoch_number    :: pv1_epoch_n(),
          epoch_csum      :: pv1_csum(),
          author_server   :: pv1_server(),
          creation_time   :: pv1_timestamp(),
          all_members     :: [pv1_server()],
          down            :: [pv1_server()],
          upi             :: [pv1_server()],
          repairing       :: [pv1_server()],
          dbg             :: list(), %proplist(), is checksummed
          dbg2            :: list(), %proplist(), is not checksummed
          members_dict    :: p_srvr_dict()
         }).

-define(MACHI_DEFAULT_TCP_PORT, 50000).

-define(SHA_MAX, (1 bsl (20*8))).

-endif. % !MACHI_PROJECTION_HRL
