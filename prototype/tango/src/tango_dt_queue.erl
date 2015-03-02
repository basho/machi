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

-module(tango_dt_queue).

-behaviour(tango_dt).

-export([start_link/4, stop/1,
         is_empty/1, length/1, peek/1, to_list/1, member/2,
         in/2, out/1, reverse/1, filter/2,
         checkpoint/1]).

%% Tango datatype callbacks
-export([fresh/0,
         do_pure_op/2, do_dirty_op/6, do_checkpoint/1,
         play_log_mutate_i_state/3]).

-define(LONG_TIME, 30*1000).

-define(D(X), io:format(user, "Dbg: ~s =\n  ~p\n", [??X, X])).

start_link(PageSize, SequencerPid, Proj, StreamNum) ->
    gen_server:start_link(tango_dt,
                          [PageSize, SequencerPid, Proj, ?MODULE, StreamNum],
                          []).

stop(Pid) ->
    tango_dt:stop(Pid).

is_empty(Pid) ->
    gen_server:call(Pid, {cb_pure_op, {o_is_empty}}, ?LONG_TIME).

length(Pid) ->
    gen_server:call(Pid, {cb_pure_op, {o_length}}, ?LONG_TIME).

peek(Pid) ->
    gen_server:call(Pid, {cb_pure_op, {o_peek}}, ?LONG_TIME).

to_list(Pid) ->
    gen_server:call(Pid, {cb_pure_op, {o_to_list}}, ?LONG_TIME).

member(Pid, X) ->
    gen_server:call(Pid, {cb_pure_op, {o_member, X}}, ?LONG_TIME).

in(Pid, Val) ->
    gen_server:call(Pid, {cb_dirty_op, {o_in, Val}}, ?LONG_TIME).

out(Pid) ->
    gen_server:call(Pid, {cb_dirty_op, {o_out}}, ?LONG_TIME).

reverse(Pid) ->
    gen_server:call(Pid, {cb_dirty_op, {o_reverse}}, ?LONG_TIME).

filter(Pid, Fun) ->
    gen_server:call(Pid, {cb_dirty_op, {o_filter, Fun}}, ?LONG_TIME).

checkpoint(Pid) ->
    tango_dt:checkpoint(Pid).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fresh() ->
    queue:new().

do_pure_op({o_is_empty}, Q) ->
    {ok, queue:is_empty(Q)};
do_pure_op({o_length}, Q) ->
    {ok, queue:len(Q)};
do_pure_op({o_peek}, Q) ->
    {ok, queue:peek(Q)};
do_pure_op({o_to_list}, Q) ->
    {ok, queue:to_list(Q)};
do_pure_op({o_member, X}, Q) ->
    {ok, queue:member(X, Q)}.

do_dirty_op(Op0, From,
            I_State, StreamNum, Proj0, ___TODO_delme_PageSize) ->
    {AsyncType, Op} = transform_dirty_op(Op0, From),
    Page = term_to_binary(Op),
    {{ok, LPN}, Proj1} = tango:append_page(Proj0, Page, [StreamNum]),
    {AsyncType, I_State, Proj1, LPN}.

do_checkpoint(Q=_I_State) ->
    [{o_start_checkpoint}|[{o_in, X} || X <- queue:to_list(Q)]].

play_log_mutate_i_state(Pages, _SideEffectsP, I_State) ->
    lists:foldl(fun({o_in, Val}=_Op, Q) ->
                        queue:in(Val, Q);
                   ({o_out, From, Node, WritingPid}, Q) ->
                        {Reply, NewQ} = queue:out(Q),
                        if Node == node(), WritingPid == self() ->
                                gen_server:reply(From, {ok, Reply});
                           true ->
                                ok
                        end,
                        NewQ;
                   ({o_reverse}, Q) ->
                        queue:reverse(Q);
                   ({o_filter, Fun}, Q) ->
                        queue:filter(Fun, Q);
                   ({o_start_checkpoint}, _Q) ->
                        fresh()
                end,
                I_State,
                lists:append([binary_to_term(Page) || Page <- Pages])).
    
transform_dirty_op({o_out}, From) ->
    %% This func will be executed on the server side prior to writing
    %% to the log.
    {op_t_sync, [{o_out, From, node(), self()}]};
transform_dirty_op(OpList, _From) when is_list(OpList) ->
    {op_t_async, OpList};
transform_dirty_op(Op, _From) ->
    {op_t_async, [Op]}.
