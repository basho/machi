-module(machi_flu0).

-behaviour(gen_server).

-export([start_link/0, stop/1,
         write/2, get/1, trim/1]).
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
          register = 'unwritten' :: register()
         }).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

get(Pid) ->
    gen_server:call(Pid, {get}, ?LONG_TIME).

write(Pid, Bin) when is_binary(Bin) ->
    gen_server:call(Pid, {write, Bin}, ?LONG_TIME).

trim(Pid) ->
    gen_server:call(Pid, {trim}, ?LONG_TIME).

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

init([]) ->
    {ok, #state{}}.

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

