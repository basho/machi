%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(machi_chain_bootstrap).

-behaviour(gen_server).

-include("machi_projection.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    self() ! finish_init,
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(finish_init, State) ->
    FLUs = get_local_flu_names(),
    FLU_Epochs = get_latest_public_epochs(FLUs),
    FLUs_at_zero = [FLU || {FLU, 0} <- FLU_Epochs],
    lager:info("FLUs at epoch 0: ~p\n", [FLUs_at_zero]),
    ChainDefs = get_initial_chains(),
    perhaps_bootstrap_chains(ChainDefs, FLUs),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%

get_local_flu_names() ->
    [Name || {Name,_,_,_} <- supervisor:which_children(machi_flu_sup)].

get_latest_public_epochs(FLUs) ->
    [begin
         PS = machi_flu1:make_projection_server_regname(FLU),
         {ok, {Epoch, _CSum}} = machi_projection_store:get_latest_epochid(
                                  PS, public),
         {FLU, Epoch}
     end || FLU <- FLUs].

get_initial_chains() ->
    DoesNotExist = "/tmp/does/not/exist",
    ConfigDir = case application:get_env(machi, chain_config_dir, DoesNotExist) of
                    DoesNotExist ->
                        DoesNotExist;
                    Dir ->
                        ok = filelib:ensure_dir(Dir ++ "/unused"),
                        Dir
                end,
    sanitize_chain_def_records(machi_flu_sup:load_rc_d_files_from_dir(ConfigDir)).

sanitize_chain_def_records(Ps) ->
    {Sane, _} = lists:foldl(fun sanitize_chain_def_rec/2, {[], dict:new()}, Ps),
    Sane.

sanitize_chain_def_rec(Whole, {Acc, D}) ->
    try
        #chain_def_v1{name=Name,
                      mode=Mode,
                      upi=UPI,
                      witnesses=Witnesses} = Whole,
        true = is_atom(Name),
        NameK = {name, Name},
        error = dict:find(NameK, D),
        true = (Mode == ap_mode orelse Mode == cp_mode),
        IsPSrvr = fun(X) when is_record(X, p_srvr) -> true;
                     (_)                           -> false
                  end,
        true = lists:all(IsPSrvr, UPI),
        true = lists:all(IsPSrvr, Witnesses),

        %% All is sane enough.
        D2 = dict:store(NameK, Name, D),
        {[Whole|Acc], D2}
    catch _:_ ->
            _ = lager:log(error, self(),
                          "~s: Bad chain_def record, skipping: ~P\n",
                          [?MODULE, Whole, 15]),
            {Acc, D}
    end.

perhaps_bootstrap_chains([], _FLUs) ->
    ok;
perhaps_bootstrap_chains([CD|ChainDefs], FLUs) ->
    #chain_def_v1{upi=UPI, witnesses=Witnesses} = CD,
    AllNames = [Name || #p_srvr{name=Name} <- UPI ++ Witnesses],
    case ordsets:intersection(ordsets:from_list(AllNames),
                              ordsets:from_list(FLUs)) of
        [] ->
            io:format(user, "TODO: no local flus in ~P\n", [CD, 10]),
            ok;
        [FLU1|_] ->
            bootstrap_chain(CD, FLU1)
    end,
    perhaps_bootstrap_chains(ChainDefs, FLUs).

bootstrap_chain(CD, FLU) ->
    io:format(user, "TODO: config ~p as bootstrap member of ~p\n", [FLU, CD]),
    todo.
