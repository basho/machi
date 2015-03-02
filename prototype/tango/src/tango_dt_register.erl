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

-module(tango_dt_register).

-behaviour(tango_dt).

-export([start_link/4, stop/1,
         set/2, get/1,
         checkpoint/1]).

%% Tango datatype callbacks
-export([fresh/0,
         do_pure_op/2, do_dirty_op/6, do_checkpoint/1,
         play_log_mutate_i_state/3]).

-define(LONG_TIME, 30*1000).

start_link(PageSize, SequencerPid, Proj, StreamNum) ->
    gen_server:start_link(tango_dt,
                          [PageSize, SequencerPid, Proj, ?MODULE, StreamNum],
                          []).

stop(Pid) ->
    tango_dt:stop(Pid).

set(Pid, Val) ->
    gen_server:call(Pid, {cb_dirty_op, {o_set, Val}}, ?LONG_TIME).

get(Pid) ->
    gen_server:call(Pid, {cb_pure_op, {o_get}}, ?LONG_TIME).

checkpoint(Pid) ->
    tango_dt:checkpoint(Pid).


fresh() ->
    undefined.

do_pure_op({o_get}, Register) ->
    {ok, Register}.

do_dirty_op(Op0, _From,
            I_State, StreamNum, Proj0, ___TODO_delme_PageSize) ->
    Op = if is_list(Op0) -> Op0;
            true         -> [Op0]               % always make a list
         end,
    Page = term_to_binary(Op),
    {{ok, LPN}, Proj1} = tango:append_page(Proj0, Page, [StreamNum]),
    {op_t_async, I_State, Proj1, LPN}.

do_checkpoint(Register=_I_State) ->
    [{o_start_checkpoint},{o_set, Register}].

play_log_mutate_i_state(Pages, _SideEffectsP, OldRegister=_I_State) ->
    lists:foldl(fun({o_set, Val}=_Op, _OldVal) ->
                        Val;
                   ({o_start_checkpoint}, _OldVal) ->
                        fresh()
                end,
                OldRegister,
                lists:append([binary_to_term(Page) || Page <- Pages])).
    
