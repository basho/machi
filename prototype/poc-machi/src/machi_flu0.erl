-module(machi_flu0).

-behaviour(gen_server).

-export([start_link/1, stop/1,
         write/2, get/1, trim/1,
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
          register = 'unwritten' :: register(),
          proj_store :: dict()
         }).

start_link(Name) when is_list(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

get(Pid) ->
    gen_server:call(Pid, {get}, ?LONG_TIME).

write(Pid, Bin) when is_binary(Bin) ->
    gen_server:call(Pid, {write, Bin}, ?LONG_TIME).

trim(Pid) ->
    gen_server:call(Pid, {trim}, ?LONG_TIME).

proj_write(Pid, Num, Proj) ->
    gen_server:call(Pid, {proj_write, Num, Proj}, ?LONG_TIME).

proj_read(Pid, Num) ->
    gen_server:call(Pid, {proj_read, Num}, ?LONG_TIME).

proj_get_latest_num(Pid) ->
    gen_server:call(Pid, {proj_get_latest_num}, ?LONG_TIME).

proj_read_latest(Pid) ->
    gen_server:call(Pid, {proj_read_latest}, ?LONG_TIME).

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

init([Name]) ->
    {ok, #state{name=Name, proj_store=orddict:new()}}.

handle_call({write, Bin}, _From, #state{register=unwritten} = S) ->
    {reply, ok, S#state{register=Bin}};
handle_call({write, _Bin}, _From, #state{register=B} = S) when is_binary(B) ->
    {reply, error_written, S};
handle_call({write, _Bin}, _From, #state{register=trimmed} = S) ->
    {reply, error_trimmed, S};
handle_call({get}, _From, #state{register=Reg} = S) ->
    {reply, {ok, Reg}, S};
handle_call({trim}, _From, #state{register=unwritten} = S) ->
    {reply, ok, S#state{register=trimmed}};
handle_call({trim}, _From, #state{register=B} = S) when is_binary(B) ->
    {reply, ok, S#state{register=trimmed}};
handle_call({trim}, _From, #state{register=trimmed} = S) ->
    {reply, error_trimmed, S};
handle_call({proj_write, Num, Proj}, _From, S) ->
    {Reply, NewS} = do_proj_write(Num, Proj, S),
    {reply, Reply, NewS};
handle_call({proj_read, Num}, _From, S) ->
    {Reply, NewS} = do_proj_read(Num, S),
    {reply, Reply, NewS};
handle_call({proj_get_latest_num}, _From, S) ->
    {Reply, NewS} = do_proj_get_latest_num(S),
    {reply, Reply, NewS};
handle_call({proj_read_latest}, _From, S) ->
    case do_proj_get_latest_num(S) of
        {error_unwritten, _S} ->
            {reply, error_unwritten, S};
        {{ok, Num}, _S} ->
            Proj = orddict:fetch(Num, S#state.proj_store),
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

do_proj_write(Num, Proj, #state{proj_store=D} = S) ->
    case orddict:find(Num, D) of
        error ->
            D2 = orddict:store(Num, Proj, D),
            {ok, S#state{proj_store=D2}};
        {ok, _} ->
            {error_written, S}
    end.

do_proj_read(Num, #state{proj_store=D} = S) ->
    case orddict:find(Num, D) of
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
            {Num, _Proj} = lists:last(L),
            {{ok, Num}, S}
    end.
