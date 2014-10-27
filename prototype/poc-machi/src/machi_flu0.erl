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
-module(machi_flu0).

-behaviour(gen_server).

-include("machi.hrl").

-export([start_link/1, stop/1,
         write/3, read/2, trim/2,
         proj_write/4, proj_read/3, proj_get_latest_num/2, proj_read_latest/2]).
-export([set_fake_repairing_status/2, get_fake_repairing_status/1]).
-export([make_proj/1, make_proj/2]).

-ifdef(TEST).
-compile(export_all).
-endif.

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.
-endif.

-define(SERVER, ?MODULE).
-define(LONG_TIME, infinity).
%% -define(LONG_TIME, 30*1000).
%% -define(LONG_TIME, 5*1000).

-type register() :: 'unwritten' | binary() | 'trimmed'.

-record(state, {
          name :: atom(),
          wedged = false :: boolean(),
          register = 'unwritten' :: register(),
          fake_repairing = false :: boolean(),
          proj_epoch :: non_neg_integer(),
          proj_store_pub :: dict(),
          proj_store_priv :: dict()
         }).

start_link(Name) when is_atom(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

stop(Pid) ->
    g_call(Pid, stop, infinity).

read(Pid, Epoch) ->
    g_call(Pid, {reg_op, Epoch, read}, ?LONG_TIME).

write(Pid, Epoch, Bin) ->
    g_call(Pid, {reg_op, Epoch, {write, Bin}}, ?LONG_TIME).

trim(Pid, Epoch) ->
    g_call(Pid, {reg_op, Epoch, trim}, ?LONG_TIME).

proj_write(Pid, Epoch, StoreType, Proj)
  when StoreType == public; StoreType == private ->
    g_call(Pid, {proj_write, Epoch, StoreType, Proj}, ?LONG_TIME).

proj_read(Pid, Epoch, StoreType)
  when StoreType == public; StoreType == private ->
    g_call(Pid, {proj_read, Epoch, StoreType}, ?LONG_TIME).

proj_get_latest_num(Pid, StoreType)
  when StoreType == public; StoreType == private ->
    g_call(Pid, {proj_get_latest_num, StoreType}, ?LONG_TIME).

proj_read_latest(Pid, StoreType)
  when StoreType == public; StoreType == private ->
    g_call(Pid, {proj_read_latest, StoreType}, ?LONG_TIME).

set_fake_repairing_status(Pid, Status) ->
    gen_server:call(Pid, {set_fake_repairing_status, Status}, ?LONG_TIME).

get_fake_repairing_status(Pid) ->
    gen_server:call(Pid, {get_fake_repairing_status}, ?LONG_TIME).

g_call(Pid, Arg, Timeout) ->
    LC1 = lclock_get(),
    {Res, LC2} = gen_server:call(Pid, {Arg, LC1}, Timeout),
    lclock_update(LC2),
    Res.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

make_proj(FLUs) ->
    make_proj(1, FLUs).

make_proj(Epoch, FLUs) ->
    #proj{epoch=Epoch, all=FLUs, active=FLUs}.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

init([Name]) ->
    lclock_init(),
    [(catch exit(whereis(Name), kill)) || _ <- lists:seq(1,2)],
    erlang:yield(),
    register(Name, self()),
    {ok, #state{name=Name,
                proj_epoch=-42,
                proj_store_pub=orddict:new(),
                proj_store_priv=orddict:new()}}.

handle_call({{reg_op, _Epoch, _}, LC1}, _From, #state{wedged=true} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {error_wedged, LC2}, S};
handle_call({{reg_op, Epoch, _}, LC1}, _From, #state{proj_epoch=MyEpoch} = S)
  when Epoch < MyEpoch ->
    LC2 = lclock_update(LC1),
    {reply, {{error_stale_projection, MyEpoch}, LC2}, S};
handle_call({{reg_op, Epoch, _}, LC1}, _From, #state{proj_epoch=MyEpoch} = S)
  when Epoch > MyEpoch ->
    LC2 = lclock_update(LC1),
    {reply, {error_wedged, LC2}, S#state{wedged=true}};

handle_call({{reg_op, _Epoch, {write, Bin}}, LC1}, _From,
             #state{register=unwritten} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {ok, LC2}, S#state{register=Bin}};
handle_call({{reg_op, _Epoch, {write, _Bin}}, LC1}, _From,
            #state{register=B} = S) when is_binary(B) ->
    LC2 = lclock_update(LC1),
    {reply, {error_written, LC2}, S};
handle_call({{reg_op, _Epoch, {write, _Bin}}, LC1}, _From,
            #state{register=trimmed} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {error_trimmed, LC2}, S};

handle_call({{reg_op, Epoch, read}, LC1}, _From, #state{proj_epoch=MyEpoch} = S)
  when Epoch /= MyEpoch ->
    LC2 = lclock_update(LC1),
    {reply, {{error_stale_projection, MyEpoch}, LC2}, S};
handle_call({{reg_op, _Epoch, read}, LC1}, _From, #state{register=Reg} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {{ok, Reg}, LC2}, S};

handle_call({{reg_op, _Epoch, trim}, LC1}, _From, #state{register=unwritten} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {ok, LC2}, S#state{register=trimmed}};
handle_call({{reg_op, _Epoch, trim}, LC1}, _From, #state{register=B} = S) when is_binary(B) ->
    LC2 = lclock_update(LC1),
    {reply, {ok, LC2}, S#state{register=trimmed}};
handle_call({{reg_op, _Epoch, trim}, LC1}, _From, #state{register=trimmed} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {error_trimmed, LC2}, S};

handle_call({{proj_write, Epoch, StoreType, Proj}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_write(Epoch, StoreType, Proj, S),
    {reply, {Reply, LC2}, NewS};
handle_call({{proj_read, Epoch, StoreType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_read(Epoch, StoreType, S),
    {reply, {Reply, LC2}, NewS};
handle_call({{proj_get_latest_num, StoreType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_get_latest_num(StoreType, S),
    {reply, {Reply, LC2}, NewS};
handle_call({{proj_read_latest, StoreType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    case do_proj_get_latest_num(StoreType, S) of
        {error_unwritten, _S} ->
            {reply, {error_unwritten, LC2}, S};
        {{ok, Epoch}, _S} ->
            Proj = orddict:fetch(Epoch, get_store_dict(StoreType, S)),
            {reply, {{ok, Proj}, LC2}, S}
    end;
handle_call({stop, LC1}, _From, MLP) ->
    LC2 = lclock_update(LC1),
    {stop, normal, {ok, LC2}, MLP};
handle_call({set_fake_repairing_status, Status}, _From, S) ->
    {reply, ok, S#state{fake_repairing=Status}};
handle_call({get_fake_repairing_status}, _From, S) ->
    {reply, S#state.fake_repairing, S};
handle_call(_Request, _From, MLP) ->
    Reply = whaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,
    {reply, Reply, MLP}.

handle_cast(_Msg, MLP) ->
    {noreply, MLP}.

handle_info(_Info, MLP) ->
    {noreply, MLP}.

terminate(_Reason, _MLP) ->
    ok.

code_change(_OldVsn, MLP, _Extra) ->
    {ok, MLP}.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

do_proj_write(Epoch, StoreType, Proj, #state{proj_epoch=MyEpoch,
                                             wedged=MyWedged} = S) ->
    D = get_store_dict(StoreType, S),
    case orddict:find(Epoch, D) of
        error ->
            D2 = orddict:store(Epoch, Proj, D),
            {NewEpoch, NewWedged} = if StoreType == public ->
                                            {MyEpoch, MyWedged};
                                       Epoch > MyEpoch ->
                                            {Epoch, false};
                                       true ->
                                            {MyEpoch, MyWedged}
                                    end,
            {ok, set_store_dict(StoreType, D2, S#state{wedged=NewWedged,
                                                       proj_epoch=NewEpoch})};
        {ok, _} ->
            {error_written, S}
    end.

do_proj_read(Epoch, StoreType, S) ->
    D = get_store_dict(StoreType, S),
    case orddict:find(Epoch, D) of
        error ->
            {error_unwritten, S};
        {ok, Proj} ->
            {{ok, Proj}, S}
    end.

do_proj_get_latest_num(StoreType, S) ->
    D = get_store_dict(StoreType, S),
    case lists:sort(orddict:to_list(D)) of
        [] ->
            {error_unwritten, S};
        L ->
            {Epoch, _Proj} = lists:last(L),
            {{ok, Epoch}, S}
    end.

get_store_dict(public, #state{proj_store_pub=D}) ->
    D;
get_store_dict(private, #state{proj_store_priv=D}) ->
    D.

set_store_dict(public, D, S) ->
    S#state{proj_store_pub=D};
set_store_dict(private, D, S) ->
    S#state{proj_store_priv=D}.

-ifdef(TEST).

lclock_init() ->
    lamport_clock:init().

lclock_get() ->
    lamport_clock:get().

lclock_update(LC) ->
    lamport_clock:update(LC).

-else.  % PULSE

lclock_init() ->
    ok.

lclock_get() ->
    ok.

lclock_update(_LC) ->
    ok.

-endif. % TEST
