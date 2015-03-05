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

-record(proj, {                                 % Projection (OLD!)
          epoch :: non_neg_integer(),
          all :: list(pid()),
          active :: list(pid())
         }).

-type m_csum()      :: {none | sha1 | sha1_excl_final_20, binary()}.
%% -type m_epoch()     :: {m_epoch_n(), m_csum()}.
-type m_epoch_n()   :: non_neg_integer().
-type m_server()    :: atom().
-type timestamp()   :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

-record(projection, {
            epoch_number    :: m_epoch_n(),
            epoch_csum      :: m_csum(),
            all_members     :: [m_server()],
            down            :: [m_server()],
            creation_time   :: timestamp(),
            author_server   :: m_server(),
            upi             :: [m_server()],
            repairing       :: [m_server()],
            dbg             :: list(), %proplist(), is checksummed
            dbg2            :: list()  %proplist(), is not checksummed
        }).

-record(ch_mgr, {
          init_finished   :: boolean(),
          name            :: m_server(),
          proj            :: #projection{},
          proj_history    :: queue(),
          myflu           :: pid() | atom(),
          flap_limit      :: non_neg_integer(),
          %%
          runenv          :: list(), %proplist()
          opts            :: list(),  %proplist()
          flaps=0         :: integer(),
          %% Each manager keeps track of the starting time that it
          %% first observed itself flapping.  This time is stored in
          %% the 'flapping_i' proplist in #projection.dbg to help
          %% other nodes tell when some other manager is having a
          %% flapping episode at time T+x that is different from the
          %% flapping episode at an earlier time T.
          flap_start      :: erlang:now(),

          %% Deprecated ... TODO: remove when old test unit test code is removed
          proj_proposed   :: 'none' | #projection{}
         }).

