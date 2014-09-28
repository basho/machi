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
    gen_server:call(Pid, stop, infinity).

read(Pid, ProjNum) ->
    gen_server:call(Pid, {reg_op, ProjNum, read}, ?LONG_TIME).

write(Pid, ProjNum, Bin) when is_binary(Bin) ->
    gen_server:call(Pid, {reg_op, ProjNum, {write, Bin}}, ?LONG_TIME).

trim(Pid, ProjNum) ->
    gen_server:call(Pid, {reg_op, ProjNum, trim}, ?LONG_TIME).

proj_write(Pid, ProjNum, Proj) ->
    gen_server:call(Pid, {proj_write, ProjNum, Proj}, ?LONG_TIME).

proj_read(Pid, ProjNum) ->
    gen_server:call(Pid, {proj_read, ProjNum}, ?LONG_TIME).

proj_get_latest_num(Pid) ->
    gen_server:call(Pid, {proj_get_latest_num}, ?LONG_TIME).

proj_read_latest(Pid) ->
    gen_server:call(Pid, {proj_read_latest}, ?LONG_TIME).

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

init([Name]) ->
    {ok, #state{name=Name,
                proj_num=-42,
                proj_store=orddict:new()}}.

handle_call({reg_op, _ProjNum, _}, _From, #state{wedged=true} = S) ->
    {reply, error_wedged, S};
handle_call({reg_op, ProjNum, _}, _From, #state{proj_num=MyProjNum} = S)
  when ProjNum < MyProjNum ->
    {reply, {error_stale_projection, MyProjNum}, S};
handle_call({reg_op, ProjNum, _}, _From, #state{proj_num=MyProjNum} = S)
  when ProjNum > MyProjNum ->
    {reply, error_wedged, S#state{wedged=true}};

handle_call({reg_op, _ProjNum, {write, Bin}}, _From,
             #state{register=unwritten} = S) ->
    {reply, ok, S#state{register=Bin}};
handle_call({reg_op, _ProjNum, {write, _Bin}}, _From,
            #state{register=B} = S) when is_binary(B) ->
    {reply, error_written, S};
handle_call({reg_op, _ProjNum, {write, _Bin}}, _From,
            #state{register=trimmed} = S) ->
    {reply, error_trimmed, S};

handle_call({reg_op, ProjNum, read}, _From, #state{proj_num=MyProjNum} = S)
  when ProjNum /= MyProjNum ->
    {reply, {error_stale_projection, MyProjNum}, S};
handle_call({reg_op, _ProjNum, read}, _From, #state{register=Reg} = S) ->
    {reply, {ok, Reg}, S};

handle_call({reg_op, _ProjNum, trim}, _From, #state{register=unwritten} = S) ->
    {reply, ok, S#state{register=trimmed}};
handle_call({reg_op, _ProjNum, trim}, _From, #state{register=B} = S) when is_binary(B) ->
    {reply, ok, S#state{register=trimmed}};
handle_call({reg_op, _ProjNum, trim}, _From, #state{register=trimmed} = S) ->
    {reply, error_trimmed, S};

handle_call({proj_write, ProjNum, Proj}, _From, S) ->
    {Reply, NewS} = do_proj_write(ProjNum, Proj, S),
    {reply, Reply, NewS};
handle_call({proj_read, ProjNum}, _From, S) ->
    {Reply, NewS} = do_proj_read(ProjNum, S),
    {reply, Reply, NewS};
handle_call({proj_get_latest_num}, _From, S) ->
    {Reply, NewS} = do_proj_get_latest_num(S),
    {reply, Reply, NewS};
handle_call({proj_read_latest}, _From, S) ->
    case do_proj_get_latest_num(S) of
        {error_unwritten, _S} ->
            {reply, error_unwritten, S};
        {{ok, ProjNum}, _S} ->
            Proj = orddict:fetch(ProjNum, S#state.proj_store),
            {reply, {ok, Proj}, S}
    end;
handle_call(stop, _From, MLP) ->
    {stop, normal, ok, MLP};
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
