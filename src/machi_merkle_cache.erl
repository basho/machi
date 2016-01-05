-module(machi_merkle_cache).
-behaviour(gen_server).

-export([
         child_spec/1,
         start_link/1,
         get/2,
         put/2
        ]).

%% gen server callbacks
-export([
         init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-define(EXPIRE_TIMER, 60*1000).

child_spec(_FLU) ->
    {}.

start_link(FLU) ->
    gen_server:start_link({local, make_cache_name(FLU)}, ?MODULE, [FLU], []).

get(_FLU, _File) ->
    %% case ets:lookup(make_cache_name_ets(FLU), File) of
    %%     undefined -> undefined;
    %%     [MT] -> {ok, MT}
    %% end.
    undefined.

put(_FLU, _MT) ->
    %% gen_server:cast(make_cache_name(FLU), {put, MT}).
    ok.

%% gen server callbacks
init([FLU]) ->
    Tid = ets:new(make_cache_name_ets(FLU), [named_table, {keypos,2}, 
                                             {read_concurrency, true}, 
                                             {write_concurrency, true}]),
    schedule_expire_tick(),
    {ok, Tid}.

handle_cast({put, MT}, Tid) ->
    ets:insert(Tid, MT),
    {noreply, Tid};
handle_cast(Msg, S) ->
    lager:warning("unknown cast ~p", [Msg]),
    {noreply, S}.

handle_call(Msg, From, S) ->
    lager:warning("unknown call ~p from ~p", [Msg, From]),
    {reply, whaaaaa, S}.

handle_info(merkle_expire_tick, Tid) ->
    do_expire(Tid),
    schedule_expire_tick(),
    {noreply, Tid};
handle_info(Msg, S) ->
    lager:warning("unknown info message ~p", [Msg]),
    {noreply, S}.

terminate(Reason, Tid) ->
    lager:debug("Terminating merkle cache ETS table ~p because ~p",
                [Tid, Reason]),
    ok.

code_change(_, _, S) ->
    {ok, S}.

%% private

schedule_expire_tick() ->
    erlang:send_after(?EXPIRE_TIMER, self(), merkle_expire_tick).

make_cache_name_ets(_FLU) ->
    merkle_ets_cache.

make_cache_name(_FLU) ->
    merkle_cache.

do_expire(_Tid) -> ok.
