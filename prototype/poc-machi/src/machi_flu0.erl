-module(machi_flu0).

-behaviour(gen_server).

-export([start_link/1, stop/1,
         write/3, read/2, trim/2,
         proj_write/3, proj_read/2, proj_get_latest_num/1, proj_read_latest/1]).
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
          name :: list(),
          wedged = false :: boolean(),
          register = 'unwritten' :: register(),
          proj_num :: non_neg_integer(),
          proj_store :: dict()
         }).

start_link(Name) when is_list(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

stop(Pid) ->
    g_call(Pid, stop, infinity).

read(Pid, ProjNum) ->
    g_call(Pid, {reg_op, ProjNum, read}, ?LONG_TIME).

write(Pid, ProjNum, Bin) when is_binary(Bin) ->
    g_call(Pid, {reg_op, ProjNum, {write, Bin}}, ?LONG_TIME).

trim(Pid, ProjNum) ->
    g_call(Pid, {reg_op, ProjNum, trim}, ?LONG_TIME).

proj_write(Pid, ProjNum, Proj) ->
    g_call(Pid, {proj_write, ProjNum, Proj}, ?LONG_TIME).

proj_read(Pid, ProjNum) ->
    g_call(Pid, {proj_read, ProjNum}, ?LONG_TIME).

proj_get_latest_num(Pid) ->
    g_call(Pid, {proj_get_latest_num}, ?LONG_TIME).

proj_read_latest(Pid) ->
    g_call(Pid, {proj_read_latest}, ?LONG_TIME).

g_call(Pid, Arg, Timeout) ->
    LC1 = lclock_get(),
    {Res, LC2} = gen_server:call(Pid, {Arg, LC1}, Timeout),
    lclock_update(LC2),
    Res.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

init([Name]) ->
    lclock_init(),
    {ok, #state{name=Name,
                proj_num=-42,
                proj_store=orddict:new()}}.

handle_call({{reg_op, _ProjNum, _}, LC1}, _From, #state{wedged=true} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {error_wedged, LC2}, S};
handle_call({{reg_op, ProjNum, _}, LC1}, _From, #state{proj_num=MyProjNum} = S)
  when ProjNum < MyProjNum ->
    LC2 = lclock_update(LC1),
    {reply, {{error_stale_projection, MyProjNum}, LC2}, S};
handle_call({{reg_op, ProjNum, _}, LC1}, _From, #state{proj_num=MyProjNum} = S)
  when ProjNum > MyProjNum ->
    LC2 = lclock_update(LC1),
    {reply, {error_wedged, LC2}, S#state{wedged=true}};

handle_call({{reg_op, _ProjNum, {write, Bin}}, LC1}, _From,
             #state{register=unwritten} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {ok, LC2}, S#state{register=Bin}};
handle_call({{reg_op, _ProjNum, {write, _Bin}}, LC1}, _From,
            #state{register=B} = S) when is_binary(B) ->
    LC2 = lclock_update(LC1),
    {reply, {error_written, LC2}, S};
handle_call({{reg_op, _ProjNum, {write, _Bin}}, LC1}, _From,
            #state{register=trimmed} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {error_trimmed, LC2}, S};

handle_call({{reg_op, ProjNum, read}, LC1}, _From, #state{proj_num=MyProjNum} = S)
  when ProjNum /= MyProjNum ->
    LC2 = lclock_update(LC1),
    {reply, {{error_stale_projection, MyProjNum}, LC2}, S};
handle_call({{reg_op, _ProjNum, read}, LC1}, _From, #state{register=Reg} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {{ok, Reg}, LC2}, S};

handle_call({{reg_op, _ProjNum, trim}, LC1}, _From, #state{register=unwritten} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {ok, LC2}, S#state{register=trimmed}};
handle_call({{reg_op, _ProjNum, trim}, LC1}, _From, #state{register=B} = S) when is_binary(B) ->
    LC2 = lclock_update(LC1),
    {reply, {ok, LC2}, S#state{register=trimmed}};
handle_call({{reg_op, _ProjNum, trim}, LC1}, _From, #state{register=trimmed} = S) ->
    LC2 = lclock_update(LC1),
    {reply, {error_trimmed, LC2}, S};

handle_call({{proj_write, ProjNum, Proj}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_write(ProjNum, Proj, S),
    {reply, {Reply, LC2}, NewS};
handle_call({{proj_read, ProjNum}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_read(ProjNum, S),
    {reply, {Reply, LC2}, NewS};
handle_call({{proj_get_latest_num}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_get_latest_num(S),
    {reply, {Reply, LC2}, NewS};
handle_call({{proj_read_latest}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    case do_proj_get_latest_num(S) of
        {error_unwritten, _S} ->
            {reply, {error_unwritten, LC2}, S};
        {{ok, ProjNum}, _S} ->
            Proj = orddict:fetch(ProjNum, S#state.proj_store),
            {reply, {{ok, Proj}, LC2}, S}
    end;
handle_call({stop, LC1}, _From, MLP) ->
    LC2 = lclock_update(LC1),
    {stop, normal, {ok, LC2}, MLP};
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

do_proj_write(ProjNum, Proj, #state{proj_num=MyProjNum, proj_store=D,
                                    wedged=MyWedged} = S) ->
    case orddict:find(ProjNum, D) of
        error ->
            D2 = orddict:store(ProjNum, Proj, D),
            {NewProjNum, NewWedged} = if ProjNum > MyProjNum ->
                                              {ProjNum, false};
                                         true ->
                                              {MyProjNum, MyWedged}
                                      end,
            {ok, S#state{wedged=NewWedged,
                         proj_num=NewProjNum,
                         proj_store=D2}};
        {ok, _} ->
            {error_written, S}
    end.

do_proj_read(ProjNum, #state{proj_store=D} = S) ->
    case orddict:find(ProjNum, D) of
        error ->
            {error_unwritten, S};
        {ok, Proj} ->
            {{ok, Proj}, S}
    end.

do_proj_get_latest_num(#state{proj_store=D} = S) ->
    case lists:sort(orddict:to_list(D)) of
        [] ->
            {error_unwritten, S};
        L ->
            {ProjNum, _Proj} = lists:last(L),
            {{ok, ProjNum}, S}
    end.

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
