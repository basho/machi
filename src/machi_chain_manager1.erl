%% -------------------------------------------------------------------
%%
%% Machi: a small village of replicated files
%%
%% Copyright (c) 2014-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The Machi chain manager, Guardian of all things related to
%% Chain Replication state, status, and data replica safety.
%%
%% The Chain Manager is responsible for managing the state of Machi's
%% "Chain Replication" state.  This role is roughly analogous to the
%% "Riak Core" application inside of Riak, which takes care of
%% coordinating replica placement and replica repair.
%% 
%% For each primitive data server in the cluster, a Machi FLU, there
%% is a Chain Manager process that manages its FLU's role within the
%% Machi cluster's Chain Replication scheme.  Each Chain Manager
%% process executes locally and independently to manage the
%% distributed state of a single Machi Chain Replication chain.
%%
%% Machi's Chain Manager process performs similar tasks as Riak Core's
%% claimant.  However, Machi has several active Chain Manager
%% processes, one per FLU server, instead of a single active process
%% like Core's claimant.  Each Chain Manager process acts
%% independently; each is constrained so that it will reach consensus
%% via independent computation &amp; action.

-module(machi_chain_manager1).

%% TODO: I am going to sever the connection between the flowchart and the
%%       code.  That diagram is really valuable, but it also takes a long time
%%       to make any kind of edit; the process is too slow.  This is a todo
%%       item a reminder that the flowchart is important documentation and
%%       must be brought back into sync with the code soon.

-behaviour(gen_server).

-include("machi_projection.hrl").
-include("machi_chain_manager.hrl").
-include("machi_verbose.hrl").

-record(ch_mgr, {
          name            :: pv1_server(),
          proj            :: projection(),
          proj_unanimous  :: 'false' | erlang:timestamp(),
          %%
          timer           :: 'undefined' | timer:tref(),
          ignore_timer    :: boolean(),
          proj_history    :: queue:queue(),
          not_sanes       :: orddict:orddict(),
          sane_transitions = 0 :: non_neg_integer(),
          consistency_mode:: 'ap_mode' | 'cp_mode',
          repair_worker   :: 'undefined' | pid(),
          repair_start    :: 'undefined' | erlang:timestamp(),
          repair_final_status :: 'undefined' | term(),
          runenv          :: list(), %proplist()
          opts            :: list(),  %proplist()
          members_dict    :: p_srvr_dict(),
          proxies_dict    :: orddict:orddict(),
          last_down       :: list(),
          fitness_svr     :: atom()
         }).

-define(FLU_PC, machi_proxy_flu1_client).
-define(TO, (2*1000)).                          % default timeout

%% Keep a history of our flowchart execution in the process dictionary.
-define(REACT(T), begin put(ttt, [?LINE|get(ttt)]), put(react, [T|get(react)]) end).
-define(TTT(), begin put(ttt, [?LINE|get(ttt)]) end).

%% Define the period of private projection stability before we'll
%% start repair.
-ifdef(TEST).
-define(REPAIR_START_STABILITY_TIME, 3).
-else. % TEST
-define(REPAIR_START_STABILITY_TIME, 10).
-endif. % TEST

%% Magic constant for looping "too frequently" breaker.  TODO revisit & revise.
-define(TOO_FREQUENT_BREAKER, 10).

-define(RETURN2(X), begin (catch put(why2, [?LINE|get(why2)])), X end).

%% This rank is used if a majority quorum is not available.
-define(RANK_CP_MINORITY_QUORUM, -99).

%% Amount of epoch number skip-ahead for set_chain_members call
-define(SET_CHAIN_MEMBERS_EPOCH_SKIP, 1111).

%% Maximum length of the history of adopted projections (via C120).
-define(MAX_HISTORY_LENGTH, 30).

%% API
-export([start_link/2, start_link/3, stop/1, ping/1,
         set_chain_members/2, set_chain_members/6, set_active/2,
         trigger_react_to_env/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, format_status/2, code_change/3]).

-export([make_chmgr_regname/1, projection_transitions_are_sane/2,
         simple_chain_state_transition_is_sane/3,
         simple_chain_state_transition_is_sane/5,
         chain_state_transition_is_sane/6]).
-export([perhaps_call/5, % for partition simulator use w/machi_fitness
         init_remember_down_list/0]).
%% Exports so that EDoc docs generated for these internal funcs.
-export([mk/3]).

%% Exports for developer/debugging
-export([scan_dir/4, strip_dbg2/1,
         get_ps/2, has_not_sane/2]).

-ifdef(TEST).

-export([test_calc_projection/2,
         test_write_public_projection/2,
         test_read_latest_public_projection/2]).
-export([update_remember_down_list/1,
         get_remember_down_list/0]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-include_lib("pulse_otp/include/pulse_otp.hrl").
-endif.

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif. %TEST

start_link(MyName, MembersDict) ->
    start_link(MyName, MembersDict, []).

start_link(MyName, MembersDict, MgrOpts) ->
    gen_server:start_link({local, make_chmgr_regname(MyName)}, ?MODULE,
                          {MyName, MembersDict, MgrOpts}, []).

stop(Pid) ->
    gen_server:call(Pid, {stop}, infinity).

ping(Pid) ->
    gen_server:call(Pid, {ping}, infinity).

%% @doc Set chain members list.
%%
%% NOTE: This implementation is a bit brittle, in that an author with
%% higher rank may try to re-suggest the old membership list if it
%% races with an author of lower rank.  For now, we suggest calling
%% set_chain_members() first on the author of highest rank and finish
%% with lowest rank, i.e. name z* first, name a* last.

set_chain_members(Pid, MembersDict) ->
    set_chain_members(Pid, ch0_name, 0, ap_mode, MembersDict, []).

set_chain_members(Pid, ChainName, OldEpoch, CMode, MembersDict, Witness_list)
  when is_atom(ChainName) andalso
       is_integer(OldEpoch) andalso OldEpoch >= 0 andalso
       (CMode == ap_mode orelse CMode == cp_mode) andalso
       is_list(MembersDict) andalso
       is_list(Witness_list) ->
    case lists:all(fun({X, #p_srvr{name=X}}) -> true;
                      (_)                    -> false
                   end, MembersDict)
         andalso
         lists:all(fun(Witness) -> orddict:is_key(Witness, MembersDict) end,
                    Witness_list) of
        true ->
            Cmd = {set_chain_members, ChainName, OldEpoch, CMode, MembersDict, Witness_list},
            gen_server:call(Pid, Cmd, infinity);
        false ->
            {error, bad_arg}
    end.

set_active(Pid, Boolean) when Boolean == true; Boolean == false ->
    gen_server:call(Pid, {set_active, Boolean}, infinity).

trigger_react_to_env(Pid) ->
    gen_server:call(Pid, {trigger_react_to_env}, infinity).

-ifdef(TEST).

%% Test/debugging code only.

test_write_public_projection(Pid, Proj) ->
    gen_server:call(Pid, {test_write_public_projection, Proj}, infinity).

%% Calculate a projection and return it to us.
%% If KeepRunenvP is true, the server will retain its change in its
%% runtime environment, e.g., changes in simulated network partitions.
test_calc_projection(Pid, KeepRunenvP) ->
    gen_server:call(Pid, {test_calc_projection, KeepRunenvP}, infinity).

test_read_latest_public_projection(Pid, ReadRepairP) ->
    gen_server:call(Pid, {test_read_latest_public_projection, ReadRepairP},
                    infinity).

-endif. % TEST

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Bootstrapping is a hassle ... when when isn't it?
%%
%% If InitMembersDict == [], then we don't know anything about the chain
%% that we'll be participating in.  We'll have to wait for directions from
%% our sysadmin later.
%%
%% If InitMembersDict /= [], then we do know what chain we're
%% participating in.  It's probably test code, since that's about the
%% only time that we know so much at init() time.
%%
%% In either case, we'll try to create & store an epoch 0 projection
%% and store it to both projections stores.  This is tricky if
%% InitMembersDict == [] because InitMembersDict usually contains the
%% #p_svrv records that we need to *write* to the projection store,
%% even our own private store!  For test code, we get the store
%% manager's pid in MgrOpts and use direct gen_server calls to the
%% local projection store.

init({MyName, InitMembersDict, MgrOpts}) ->
    put(ttt, [?LINE]),
    _ = random:seed(now()),
    init_remember_down_list(),
    Opt = fun(Key, Default) -> proplists:get_value(Key, MgrOpts, Default) end,
    InitWitness_list = Opt(witnesses, []),
    ZeroAll_list = [P#p_srvr.name || {_,P} <- orddict:to_list(InitMembersDict)],
    ZeroProj = make_none_projection(0, MyName, ZeroAll_list,
                                    InitWitness_list, InitMembersDict),
    ok = store_zeroth_projection_maybe(ZeroProj, MgrOpts),
    CMode = Opt(consistency_mode, ap_mode),
    case get_projection_store_regname(MgrOpts) of
        undefined ->
            ok;
        PS ->
            ok = set_consistency_mode(PS, CMode)
    end,

    %% Using whatever is the largest epoch number in our local private
    %% store, this manager starts out using the "none" projection.  If
    %% other members of the chain are running, then we'll simply adopt
    %% whatever they're using as a basis for our next suggested
    %% projection.
    %%
    %% If we're in CP mode, we have to be very careful about who we
    %% choose to be UPI members when we (or anyone else) restarts.
    %% However, that choice is *not* made here: it is made later
    %% during our first humming consensus iteration.  When we start
    %% with the none projection, we're make a safe choice before
    %% wading into the deep waters.
    {MembersDict, Proj0} =
        get_my_private_proj_boot_info(MgrOpts, InitMembersDict, ZeroProj),
    #projection_v1{epoch_number=CurrentEpoch,
                   all_members=All_list, witnesses=Witness_list} = Proj0,
    Proj = make_none_projection(CurrentEpoch,
                                MyName, All_list, Witness_list, MembersDict),

    RunEnv = [{seed, Opt(seed, now())},
              {use_partition_simulator, Opt(use_partition_simulator, false)},
              {simulate_repair, Opt(simulate_repair, true)},
              {network_partitions, Opt(network_partitions, [])},
              {network_islands, Opt(network_islands, [])},
              {last_up_nodes, []},
              {last_up_nodes_time, now()},
              {up_nodes, Opt(up_nodes, [])}],
    ActiveP = Opt(active_mode, true),
    S = set_proj(#ch_mgr{name=MyName,
                         timer='undefined',
                         proj_history=queue:new(),
                         not_sanes=orddict:new(),
                         consistency_mode=CMode,
                         runenv=RunEnv,
                         opts=MgrOpts,
                         last_down=[no_such_server_initial_value_only],
                         fitness_svr=machi_flu_psup:make_fitness_regname(MyName)
                        }, Proj),
    S2 = do_set_chain_members_dict(MembersDict, S),
    S3 = if ActiveP == false ->
                 S2;
            ActiveP == true ->
                 set_active_timer(S2)
         end,
    {ok, S3}.

handle_call({ping}, _From, S) ->
    {reply, pong, S};
handle_call({set_chain_members, SetChainName, SetOldEpoch, CMode,
             MembersDict, Witness_list}, _From,
            #ch_mgr{name=MyName,
                    proj=#projection_v1{all_members=OldAll_list,
                                        epoch_number=OldEpoch,
                                        chain_name=ChainName,
                                        upi=OldUPI}=OldProj}=S) ->
    true = (OldEpoch == 0) % in this case we want unconditional set of ch name
           orelse
           (SetOldEpoch == OldEpoch andalso SetChainName == ChainName),
    S2 = do_set_chain_members_dict(MembersDict, S),
    %% TODO: should there be any additional sanity checks?  Right now,
    %% if someone does something bad, then do_react_to_env() will
    %% crash, which will crash us, and we'll restart in a sane & old
    %% config.
    All_list = [P#p_srvr.name || {_, P} <- orddict:to_list(MembersDict)],
    MissingInNew = OldAll_list -- All_list,
    {NewUPI, NewDown} = if OldEpoch == 0 ->
                                {All_list, []};
                           true ->
                                NUPI = OldUPI -- MissingInNew,
                                {NUPI, All_list -- NUPI}
                        end,
    NewEpoch = OldEpoch + ?SET_CHAIN_MEMBERS_EPOCH_SKIP,
    ok = set_consistency_mode(machi_flu_psup:make_proj_supname(MyName), CMode),
    NewProj = machi_projection:update_checksum(
                OldProj#projection_v1{author_server=MyName,
                                      chain_name=SetChainName,
                                      creation_time=now(),
                                      mode=CMode,
                                      epoch_number=NewEpoch,
                                      all_members=All_list,
                                      witnesses=Witness_list,
                                      upi=NewUPI,
                                      repairing=[],
                                      down=NewDown,
                                      members_dict=MembersDict}),
    S3 = set_proj(S2#ch_mgr{proj_history=queue:new(),
                            consistency_mode=CMode}, NewProj),
    {Res, S4} = do_react_to_env(S3),
    Reply = case Res of
                {_,_,_} -> ok
                % Dialyzer says that all vals match with the 3-tuple pattern
            end,
    {reply, Reply, S4};
handle_call({set_active, Boolean}, _From, #ch_mgr{timer=TRef}=S) ->
    case {Boolean, TRef} of
        {true, undefined} ->
            S2 = set_active_timer(S),
            {reply, ok, S2};
        {false, _} ->
            (catch timer:cancel(TRef)),
            {reply, ok, S#ch_mgr{timer=undefined}};
        _ ->
            {reply, error, S}
    end;
handle_call({stop}, _From, S) ->
    {stop, normal, ok, S};
handle_call({test_calc_projection, KeepRunenvP}, _From,
            #ch_mgr{name=MyName}=S) ->
    RelativeToServer = MyName,
    {P, S2, _Up} = calc_projection(S, RelativeToServer),
    {reply, {ok, P}, if KeepRunenvP -> S2;
                        true        -> S
                     end};
handle_call({test_write_public_projection, Proj}, _From, S) ->
    {Res, S2} = do_cl_write_public_proj(Proj, S),
    {reply, Res, S2};
handle_call({test_read_latest_public_projection, ReadRepairP}, _From, S) ->
    {Perhaps, Val, ExtraInfo, S2} =
        do_cl_read_latest_public_projection(ReadRepairP, S),
    Res = {Perhaps, Val, ExtraInfo},
    {reply, Res, S2};
handle_call({trigger_react_to_env}=Call, _From, S) ->
    gobble_calls(Call),
    {Res, S2} = do_react_to_env(S),
    {reply, Res, S2};
handle_call(_Call, _From, S) ->
    io:format(user, "\nBad call to ~p: ~p\n", [S#ch_mgr.name, _Call]),
    {reply, whaaaaaaaaaa, S}.

handle_cast(_Cast, S) ->
    ?D({cast_whaaaaaaaaaaa, _Cast}),
    {noreply, S}.

handle_info(tick_check_environment, #ch_mgr{ignore_timer=true}=S) ->
    {noreply, S};
handle_info(tick_check_environment, S) ->
    {{_Delta, Props, _Epoch}, S1} = do_react_to_env(S),
    S2 = sanitize_repair_state(S1),
    S3 = perhaps_start_repair(S2),
    case proplists:get_value(throttle_seconds, Props) of
        N when is_integer(N), N > 0 ->
            %% We are flapping.  Set ignore_timer=true and schedule a
            %% reminder to stop ignoring.  This slows down the rate of
            %% flapping.
            erlang:send_after(N*1000, self(), stop_ignoring_timer),
            {noreply, S3#ch_mgr{ignore_timer=true}};
        _ ->
            {noreply, S3}
    end;
handle_info(stop_ignoring_timer, S) ->
    {noreply, S#ch_mgr{ignore_timer=false}};
handle_info({'DOWN',_Ref,process,Worker,Res},
            #ch_mgr{repair_worker=Worker}=S)->
    {noreply, S#ch_mgr{ignore_timer=false,
                       repair_worker=undefined,
                       repair_final_status=Res}};
handle_info(Msg, #ch_mgr{name=MyName}=S) ->
    case get(todo_bummer) of undefined -> io:format("TODO: ~w got ~p\n",
                                                    [MyName, Msg]);
                             _         -> ok
    end,
    put(todo_bummer, true),
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

format_status(_Opt, [_PDict, Status]) ->
    Fields = record_info(fields, ch_mgr),
    [_Name | Values] = tuple_to_list(Status),
    lists:zip(Fields, Values).

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_none_projection(Epoch, MyName, All_list, Witness_list, MembersDict) ->
    Down_list = All_list,
    UPI_list = [],
    P = machi_projection:new(MyName, MembersDict, Down_list, UPI_list, [], []),
    CMode = if Witness_list == [] ->
                    ap_mode;
               Witness_list /= [] ->
                    cp_mode
            end,
    machi_projection:update_checksum(P#projection_v1{epoch_number=Epoch,
                                                     mode=CMode,
                                                     witnesses=Witness_list}).

make_all_projection(MyName, All_list, Witness_list, MembersDict) ->
    Down_list = [],
    UPI_list = All_list,
    P = machi_projection:new(MyName, MembersDict, Down_list, UPI_list, [], []),
    machi_projection:update_checksum(P#projection_v1{witnesses=Witness_list}).

get_my_private_proj_boot_info(MgrOpts, DefaultDict, DefaultProj) ->
    get_my_proj_boot_info(MgrOpts, DefaultDict, DefaultProj, private).

get_my_public_proj_boot_info(MgrOpts, DefaultDict, DefaultProj) ->
    get_my_proj_boot_info(MgrOpts, DefaultDict, DefaultProj, public).

get_my_proj_boot_info(MgrOpts, DefaultDict, DefaultProj, ProjType) ->
    case proplists:get_value(projection_store_registered_name, MgrOpts) of
        undefined ->
            {DefaultDict, DefaultProj};
        Store ->
            {ok, P} = machi_projection_store:read_latest_projection(Store,
                                                                    ProjType),
            {P#projection_v1.members_dict, P}
    end.

%% Write the epoch 0 projection store, to assist bootstrapping.  If the
%% 0th epoch is already written, there's no problem.

store_zeroth_projection_maybe(ZeroProj, MgrOpts) ->
    case get_projection_store_regname(MgrOpts) of
        undefined ->
            ok;
        Store ->
            _ = machi_projection_store:write(Store, public, ZeroProj),
            _ = machi_projection_store:write(Store, private, ZeroProj),
            ok
    end.

get_projection_store_regname(MgrOpts) ->
    proplists:get_value(projection_store_registered_name, MgrOpts).

set_consistency_mode(undefined, _CMode) ->
    ok;
set_consistency_mode(ProjStore, CMode) ->
    machi_projection_store:set_consistency_mode(ProjStore, CMode).

set_active_timer(#ch_mgr{name=MyName, members_dict=MembersDict}=S) ->
    FLU_list = [P#p_srvr.name || {_,P} <- orddict:to_list(MembersDict)],
    %% Perturb the order a little bit, to avoid near-lock-step
    %% operations every few ticks.
    MSec = calc_sleep_ranked_order(400, 1500, MyName, FLU_list) +
        random:uniform(100),
    {ok, TRef} = timer:send_interval(MSec, tick_check_environment),
    S#ch_mgr{timer=TRef}.

do_cl_write_public_proj(Proj, S) ->
    #projection_v1{epoch_number=Epoch} = Proj,
    cl_write_public_proj(Epoch, Proj, S).

cl_write_public_proj(Epoch, Proj, S) ->
    cl_write_public_proj(Epoch, Proj, false, S).

cl_write_public_proj_ignore_written_error(Epoch, Proj, S) ->
    cl_write_public_proj(Epoch, Proj, true, S).

cl_write_public_proj(Epoch, Proj, IgnoreWrittenErrorP, S) ->
    %% OLD: Write to local public projection store first, and if it succeeds,
    %% then write to all remote public projection stores.
    %% NEW: Hypothesis: The OLD idea is a bad idea and causes needless retries
    %% via racing with other wrier
    %% NEW: Let's see what kind of trouble we can get ourselves into if
    %%      we abort our writing efforts if we encounter an
    %%      {error,written} status.
    %%      Heh, that doesn't work too well: if we have random uniform write
    %%      delays of 0-1,500 msec, then we end up abandoning this epoch
    %%      number at the first sign of trouble and then re-iterate to suggest
    %%      a new epoch number ... which causes *more* thrash, not less.
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    FLUs = Proj#projection_v1.all_members,
    cl_write_public_proj2(FLUs, Partitions, Epoch, Proj,
                          IgnoreWrittenErrorP, S2).

cl_write_public_proj2(FLUs, Partitions, Epoch, Proj, IgnoreWrittenErrorP, S) ->
    %% We're going to be very care-free about this write because we'll rely
    %% on the read side to do any read repair.
    DoIt = fun(Pid) -> ?FLU_PC:write_projection(Pid, public, Proj, ?TO) end,
    %% Rs = [{FLU, perhaps_call_t(S, Partitions, FLU, fun(Pid) -> DoIt(Pid) end)} ||
    %%          FLU <- FLUs],
    Rs = lists:foldl(
           fun(FLU, {false=_KeepGoingP, Acc}) ->
                   {false, [{FLU,skipped}|Acc]};
              (FLU, {true=_KeepGoingP, Acc}) ->
                   case perhaps_call_t(S, Partitions, FLU, DoIt) of
                       {error,written}=Written when IgnoreWrittenErrorP ->
                           %% io:format(user, "\nTried but written and ignoring written\n", []),
                           {true, [{FLU,Written}|Acc]};
                       {error,written}=Written when not IgnoreWrittenErrorP ->
                           %% Brute-force read-repair-like good enough.
                           DoRead = fun(Pid) -> ?FLU_PC:read_projection(
                                                   Pid, public, Epoch, ?TO)
                                    end,
                           {ok, Proj2} = perhaps_call_t(S, Partitions, FLU,
                                                        DoRead),
                           DoIt2 = fun(Pid) -> ?FLU_PC:write_projection(Pid, public, Proj2, ?TO) end,
                           _Rs=[_ = perhaps_call_t(S, Partitions, Fl, DoIt2)||
                                   Fl <- FLUs],
                           %% io:format(user, "\nTried ~w ~W but repairing with ~w ~W: ~w\n", [Epoch, Proj#projection_v1.epoch_csum, 4, Epoch, Proj2#projection_v1.epoch_csum, 4, _Rs]),
                           {false, [{FLU,Written}|Acc]};
                       Else ->
                           %% io:format(user, "\nTried but got an Else ~p\n", [Else]),
                           {true, [{FLU,Else}|Acc]}
                   end
           end, {true, []}, FLUs),
    %% io:format(user, "\nWrite public ~w by ~w: ~w\n", [Epoch, S#ch_mgr.name, Rs]),
    %% io:format(user, "mgr ~w epoch ~w Rs ~p\n", [S#ch_mgr.name, Epoch, Rs]),
    {{remote_write_results, Rs}, S}.

do_cl_read_latest_public_projection(ReadRepairP,
                                    #ch_mgr{proj=Proj1} = S) ->
    _Epoch1 = Proj1#projection_v1.epoch_number,
    case cl_read_latest_projection(public, S) of
        {needs_repair, FLUsRs, Extra, S3} ->
            if not ReadRepairP ->
                    {not_unanimous, todoxyz, [{unanimous_flus, []},
                                              {results, FLUsRs}|Extra], S3};
               true ->
                    {_Status, S4} = do_read_repair(FLUsRs, Extra, S3),
                    do_cl_read_latest_public_projection(ReadRepairP, S4)
            end;
        {_UnanimousTag, _Proj2, _Extra, _S3}=Else ->
            Else
    end.

read_latest_projection_call_only(ProjectionType, AllHosed,
                                 #ch_mgr{proj=CurrentProj}=S) ->
    #projection_v1{all_members=All_list} = CurrentProj,
    All_queried_list = lists:sort(All_list -- AllHosed),
    read_latest_projection_call_only1(ProjectionType, AllHosed,
                                      All_queried_list, S).

read_latest_projection_call_only1(ProjectionType, AllHosed,
                                  All_queried_list, S) ->
    {Rs_tmp, S2} = read_latest_projection_call_only2(ProjectionType,
                                                     All_queried_list, S),
    New_all_maybe =
        lists:usort(
          lists:flatten(
            [A_l || #projection_v1{all_members=A_l} <- Rs_tmp])) -- AllHosed,
    case New_all_maybe -- All_queried_list of
        [] ->
            FLUsRs = lists:zip(All_queried_list, Rs_tmp),
            {All_queried_list, FLUsRs, S2};
        [AnotherFLU|_] ->
            %% Stop AnotherFLU proxy, in unexpected case where it's open
            try
                Proxy = proxy_pid(AnotherFLU, S2),
                ?FLU_PC:stop_proxies([Proxy])
            catch _:_ -> ok
            end,
            MD = orddict:from_list(
                  lists:usort(
                   lists:flatten(
                    [orddict:to_list(D) || #projection_v1{members_dict=D} <- Rs_tmp]))),
            Another_P_srvr = orddict:fetch(AnotherFLU, MD),
            {ok, Proxy2} = ?FLU_PC:start_link(Another_P_srvr),
            S3 = S2#ch_mgr{proxies_dict=orddict:store(AnotherFLU, Proxy2,
                                                      S2#ch_mgr.proxies_dict)},
            read_latest_projection_call_only1(
              ProjectionType, AllHosed,
              lists:usort([AnotherFLU|All_queried_list]), S3)
    end.

read_latest_projection_call_only2(ProjectionType, All_queried_list, S) ->
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    DoIt = fun(Pid) ->
                   case (?FLU_PC:read_latest_projection(Pid, ProjectionType, ?TO)) of
                       {ok, P} -> P;
                       Else    -> Else
                   end
           end,
    Rs = [(catch perhaps_call_t(S, Partitions, FLU, fun(Pid) -> DoIt(Pid) end)) ||
             FLU <- All_queried_list],
    {Rs, S2}.

cl_read_latest_projection(ProjectionType, S) ->
    AllHosed = [],
    cl_read_latest_projection(ProjectionType, AllHosed, S).

cl_read_latest_projection(ProjectionType, AllHosed, S) ->
    {All_queried_list, FLUsRs, S2} =
        read_latest_projection_call_only(ProjectionType, AllHosed, S),

    rank_and_sort_projections_with_extra(All_queried_list, FLUsRs,
                                         ProjectionType, S2).

rank_and_sort_projections_with_extra(All_queried_list, FLUsRs, ProjectionType,
                                     #ch_mgr{name=MyName,proj=CurrentProj}=S) ->
    UnwrittenRs = [x || {_, {error, not_written}} <- FLUsRs],
    Ps = [Proj || {_FLU, Proj} <- FLUsRs, is_record(Proj, projection_v1)],
    BadAnswerFLUs = [FLU || {FLU, Answer} <- FLUsRs,
                            not is_record(Answer, projection_v1)],
    BadAnswers = [Answer || {_FLU, Answer} <- FLUsRs,
                            not is_record(Answer, projection_v1)],

    if All_queried_list == []
       orelse
       length(UnwrittenRs) == length(FLUsRs) ->
            Witness_list = CurrentProj#projection_v1.witnesses,
            NoneProj = make_none_projection(0, MyName, [], Witness_list,
                                            orddict:new()),
            ChainName = CurrentProj#projection_v1.chain_name,
            NoneProj2 = NoneProj#projection_v1{chain_name=ChainName},
            Extra2 = [{all_members_replied, true},
                      {all_queried_list, All_queried_list},
                      {flus_rs, FLUsRs},
                      {unanimous_flus,[]},
                      {not_unanimous_flus, []},
                      {bad_answer_flus, BadAnswerFLUs},
                      {bad_answers, BadAnswers},
                      {not_unanimous_answers, []}],
            {not_unanimous, NoneProj2, Extra2, S};
       ProjectionType == public, UnwrittenRs /= [] ->
            {needs_repair, FLUsRs, [flarfus], S};
       true ->
            [{_Rank, BestProj}|_] = rank_and_sort_projections(Ps, CurrentProj),
            BestEpoch = BestProj#projection_v1.epoch_number,
            NotBestPs = [Proj || Proj <- Ps, Proj /= BestProj],
            NotBestPsEpochFilt =
                [Proj || Proj <- Ps, Proj /= BestProj,
                         Proj#projection_v1.epoch_number == BestEpoch],
            BadAnswers2 = [Answer || {_FLU, Answer} <- FLUsRs,
                                     not is_record(Answer, projection_v1)
                                         orelse Answer /= BestProj],
            %% Wow, I'm not sure how long this bug has been here, but it's
            %% likely 5 months old (April 2015).  I just now noticed a problem
            %% where BestProj was epoch 1194, but NotBestPs contained a
            %% projection at smaller epoch 1192.  The test for nonempty
            %% NotBestPs list caused the 1194 BestProj to be marked
            %% not_unanimous incorrectly.  (This can happen in asymmetric
            %% partition cases, hooray for crazy corner cases.)
            %%
            %% We correct the bug by filtering NotBestPs further to include
            %% only not-equal projections that also share BestProj's epoch.
            %% We'll get the correct answer we seek using this list == []
            %% method, as long as rank_and_sort_projections() will always pick
            %% a proj with the highest visible epoch.
            UnanimousTag = if NotBestPsEpochFilt == [] -> unanimous;
                              true                     -> not_unanimous
                           end,
            Extra = [{all_members_replied, length(FLUsRs) == length(All_queried_list)}],
            Best_FLUs = [FLU || {FLU, Projx} <- FLUsRs, Projx == BestProj],
            Extra2 = [{all_queried_list, All_queried_list},
                      {flus_rs, FLUsRs},
                      {unanimous_flus,Best_FLUs},
                      {not_unanimous_flus, All_queried_list --
                                                 (Best_FLUs ++ BadAnswerFLUs)},
                      {bad_answer_flus, BadAnswerFLUs},
                      {bad_answers, BadAnswers2},
                      {not_best_ps, NotBestPs},
                      {not_best_ps_epoch_filt, NotBestPsEpochFilt}|Extra],
            {UnanimousTag, BestProj, Extra2, S}
    end.

do_read_repair(FLUsRs, _Extra, #ch_mgr{proj=CurrentProj} = S) ->
    Unwrittens = [x || {_FLU, {error, not_written}} <- FLUsRs],
    Ps = [Proj || {_FLU, Proj} <- FLUsRs, is_record(Proj, projection_v1)],
    if Unwrittens == [] orelse Ps == [] ->
            {nothing_to_do, S};
       true ->
            %% We have at least one unwritten and also at least one proj.
            %% Pick the best one, then spam it everywhere.

            [{_Rank, BestProj}|_] = rank_and_sort_projections(Ps, CurrentProj),
            Epoch = BestProj#projection_v1.epoch_number,

            %% We're doing repair, so use the flavor that will
            %% continue to all others even if there is an
            %% error_written on the local FLU.
            {_DontCare, _S2}=Res = cl_write_public_proj_ignore_written_error(
                                                           Epoch, BestProj, S),
            Res
    end.

calc_projection(S, RelativeToServer) ->
    calc_projection(S, RelativeToServer, []).

calc_projection(#ch_mgr{proj=P_current}=S, RelativeToServer, AllHosed) ->
    calc_projection(S, RelativeToServer, AllHosed, P_current).

calc_projection(#ch_mgr{name=_MyName, consistency_mode=CMode,
                        runenv=_RunEnv}=S,
                RelativeToServer, AllHosed, P_current) ->
    Dbg = [],
    %% OldThreshold = proplists:get_value(old_threshold, _RunEnv),
    %% NoPartitionThreshold = proplists:get_value(no_partition_threshold, _RunEnv),
    if CMode == ap_mode ->
            calc_projection2(P_current, RelativeToServer, AllHosed, Dbg, S);
       CMode == cp_mode ->
            calc_projection2(P_current, RelativeToServer, AllHosed, Dbg, S)
            %% TODO EXPERIMENT 2015-09-21 DELETE-ME???
            %% #projection_v1{epoch_number=OldEpochNum,
            %%                all_members=AllMembers,
            %%                upi=OldUPI_list,
            %%                upi=OldRepairing_list
            %%               } = P_current,
            %% OldUPI_and_Repairing = OldUPI_list ++ OldRepairing_list,
            %% UPI_and_Repairing_length_ok_p =
            %%     length(OldUPI_and_Repairing) >= full_majority_size(AllMembers),
            %% case {OldEpochNum, UPI_length_ok_p} of
            %%     {0, _} ->
            %%         calc_projection2(P_current, RelativeToServer, AllHosed,
            %%                          Dbg, S);
            %%     {_, true} ->
            %%         calc_projection2(P_current, RelativeToServer, AllHosed,
            %%                          Dbg, S);
            %%     {_, false} ->
            %%         {Up, _Partitions, RunEnv2} = calc_up_nodes(
            %%                                       _MyName, AllMembers, _RunEnv),
            %%         %% We can't improve on the current projection.
            %%         {P_current, S#ch_mgr{runenv=RunEnv2}, Up}
            %% end
    end.

%% AllHosed: FLUs that we must treat as if they are down, e.g., we are
%%           in a flapping situation and wish to ignore FLUs that we
%%           believe are bad-behaving causes of our flapping.

calc_projection2(LastProj, RelativeToServer, AllHosed, Dbg,
                 #ch_mgr{name=MyName,
                         proj=CurrentProj,
                         consistency_mode=CMode,
                         runenv=RunEnv1,
                         repair_final_status=RepairFS}=S) ->
    #projection_v1{epoch_number=OldEpochNum,
                   chain_name=ChainName,
                   members_dict=MembersDict,
                   witnesses=OldWitness_list,
                   upi=OldUPI_list,
                   repairing=OldRepairing_list
                  } = LastProj,
    LastUp = lists:usort(OldUPI_list ++ OldRepairing_list),
    AllMembers = CurrentProj#projection_v1.all_members,
    {Up0, Partitions, RunEnv2} = calc_up_nodes(MyName,
                                               AllMembers, RunEnv1),
    Up = Up0 -- AllHosed,

    NewUp = Up -- LastUp,
    Down = AllMembers -- Up,
    ?REACT({calc,?LINE,[{old_epoch,OldEpochNum},
                        {old_upi, OldUPI_list},
                        {old_repairing,OldRepairing_list},
                        {last_up, LastUp}, {up0, Up0}, {all_hosed, AllHosed},
                        {up, Up}, {new_up, NewUp}, {down, Down}]}),

    NewUPI_list =
        [X || X <- OldUPI_list, lists:member(X, Up) andalso
                                not lists:member(X, OldWitness_list)],
    RepChk_Proj =  if AllHosed == [] ->
                           CurrentProj;
                      true ->
                           CurrentProj
                   end,
    RepChk_LastInUPI = case RepChk_Proj#projection_v1.upi of
                           []    -> does_not_exist_because_upi_is_empty;
                           [_|_] -> lists:last(RepChk_Proj#projection_v1.upi)
                       end,
    Repairing_list2 = [X || X <- OldRepairing_list,
                            lists:member(X, Up),
                            not lists:member(X, OldWitness_list)],
    Simulator_p = proplists:get_value(use_partition_simulator, RunEnv2, false),
    SimRepair_p = proplists:get_value(simulate_repair, RunEnv2, true),
    {NewUPI_list3, Repairing_list3, RunEnv3} =
        case {NewUp -- OldWitness_list, Repairing_list2} of
            {[], []} ->
                D_foo=[d_foo1],
                {NewUPI_list, [], RunEnv2};
            {[], [H|T]} when RelativeToServer == RepChk_LastInUPI ->
                %% The author is tail of the UPI list.  Let's see if
                %% *everyone* in the UPI+repairing lists are using our
                %% projection.  This is to simulate a requirement that repair
                %% a real repair process cannot take place until the chain is
                %% stable, i.e. everyone is in the same epoch.

                %% TODO create a real API call for fetching this info?
                SameEpoch_p = check_latest_private_projections_same_epoch(
                                NewUPI_list ++ Repairing_list2,
                                RepChk_Proj, Partitions, S),
                if Simulator_p andalso SimRepair_p andalso
                   SameEpoch_p andalso RelativeToServer == RepChk_LastInUPI ->
                        D_foo=[{repair_airquote_done, {we_agree, (S#ch_mgr.proj)#projection_v1.epoch_number}}],
                        if CMode == cp_mode -> timer:sleep(567); true -> ok end,
                        {NewUPI_list ++ [H], T, RunEnv2};
                   not (Simulator_p andalso SimRepair_p)
                   andalso
                   RepairFS == {repair_final_status, ok} ->
                        D_foo=[{repair_done, {repair_final_status, ok, (S#ch_mgr.proj)#projection_v1.epoch_number}}],
                        {NewUPI_list ++ Repairing_list2, [], RunEnv2};
                   true ->
                        D_foo=[d_foo2],
                        {NewUPI_list, OldRepairing_list, RunEnv2}
                end;
            {_ABC, _XYZ} ->
                D_foo=[d_foo3, {new_upi_list, NewUPI_list}, {new_up, NewUp}, {repairing_list3, OldRepairing_list}],
                {NewUPI_list, OldRepairing_list, RunEnv2}
        end,
    ?REACT({calc,?LINE,
            [{newupi_list3, NewUPI_list3},{repairing_list3, Repairing_list3}]}),
    Repairing_list4 = case NewUp of
                          []    -> Repairing_list3;
                          NewUp -> Repairing_list3 ++ NewUp
                      end,
    Repairing_list5 = (Repairing_list4 -- Down) -- OldWitness_list,

    TentativeUPI = NewUPI_list3,
    TentativeRepairing = Repairing_list5,
    ?REACT({calc,?LINE,[{tent, TentativeUPI},{tent_rep, TentativeRepairing}]}),

    {NewUPI, NewRepairing} =
        if (CMode == ap_mode
            orelse
            (CMode == cp_mode andalso OldEpochNum == 0))
           andalso
           TentativeUPI == [] andalso TentativeRepairing /= [] ->
                %% UPI is empty (not including witnesses), so grab
                %% the first from the repairing list and make it the
                %% only non-witness in the UPI.
                [FirstRepairing|TailRepairing] = TentativeRepairing,
                {[FirstRepairing], TailRepairing};
           true ->
                {TentativeUPI, TentativeRepairing}
        end,
    ?REACT({calc,?LINE,[{new_upi, NewUPI},{new_rep, NewRepairing}]}),

    P0 = machi_projection:new(OldEpochNum + 1,
                              MyName, MembersDict, Down, NewUPI, NewRepairing,
                              D_foo ++
                                  Dbg ++ [{ps, Partitions},{nodes_up, Up}]),
    P1 = P0#projection_v1{chain_name=ChainName},
    P2 = if CMode == cp_mode ->
                 UpWitnesses = [W || W <- Up, lists:member(W, OldWitness_list)],
                 Majority = full_majority_size(AllMembers),
                 %% A repairing node can also contribute to the quorum
                 %% majority required to attest to the history of the UPI.
                 SoFar = length(NewUPI ++ NewRepairing),
                 if SoFar >= Majority ->
                         ?REACT({calc,?LINE,[]}),
                         P1;
                    true ->
                         Need = Majority - SoFar,
                         UpWitnesses = [W || W <- Up,
                                             lists:member(W, OldWitness_list)],
                         if length(UpWitnesses) >= Need ->
                                 Ws = lists:sublist(UpWitnesses, Need),
                                 ?REACT({calc,?LINE,[{ws, Ws}]}),
                                 machi_projection:update_checksum(
                                   P1#projection_v1{upi=Ws++NewUPI});
                            true ->
                                 ?REACT({calc,?LINE,[]}),
                                 P_none0 = make_none_projection(
                                            OldEpochNum + 1,
                                            MyName, AllMembers, OldWitness_list,
                                            MembersDict),
                                 Why = if NewUPI == [] ->
                                               "No real servers in old upi are available now";
                                          true ->
                                               "Not enough witnesses are available now"
                                       end,
                                 P_none1 = P_none0#projection_v1{
                                             chain_name=ChainName,
                                             %% Stable creation time!
                                             creation_time={1,2,3},
                                             dbg=[{none_projection,true},
                                                  {up0, Up0},
                                                  {up, Up},
                                                  {all_hosed, AllHosed},
                                                  {oldupi, OldUPI_list},
                                                  {newupi, NewUPI_list},
                                                  {newupi3, NewUPI_list3},
                                                  {tent_upi, TentativeUPI},
                                                  {new_upi, NewUPI},
                                                  {up_witnesses, UpWitnesses},
                                                  {why_none, Why}],
                                             dbg2=[
                                               {creation_time,os:timestamp()}]},
                                 machi_projection:update_checksum(P_none1)
                         end
                 end;
            CMode == ap_mode ->
                 ?REACT({calc,?LINE,[]}),
                 P1
         end,
    P3 = machi_projection:update_checksum(
           P2#projection_v1{mode=CMode, witnesses=OldWitness_list}),
    ?REACT({calc,?LINE,[machi_projection:make_summary(P3)]}),
    {P3, S#ch_mgr{runenv=RunEnv3}, Up}.

check_latest_private_projections_same_epoch(FLUs, MyProj, Partitions, S) ->
    #projection_v1{epoch_number=MyEpoch, epoch_csum=MyCSum} = MyProj,
    %% NOTE: The caller must provide us with the FLUs list for all
    %%       FLUs that must be up & available right now.  So any
    %%       failure of perhaps_call_t() means that we must return
    %%       false.
    FoldFun = fun(_FLU, false) ->
                      false;
                 (FLU, true) ->
                      F = fun(Pid) ->
                                  ?FLU_PC:read_latest_projection(Pid, private, ?TO)
                          end,
                      case perhaps_call_t(S, Partitions, FLU, F) of
                          {ok, RPJ} ->
                              #projection_v1{epoch_number=RemoteEpoch,
                                             epoch_csum=RemoteCSum} = RPJ,
                              if MyEpoch == RemoteEpoch,
                                 MyCSum  == RemoteCSum ->
                                      true;
                                 true ->
                                      false
                              end;
                          _Else ->
                              false
                      end
              end,
    lists:foldl(FoldFun, true, FLUs).

calc_up_nodes(#ch_mgr{name=MyName, proj=Proj, runenv=RunEnv1}=S) ->
    AllMembers = Proj#projection_v1.all_members,
    {UpNodes, Partitions, RunEnv2} =
        calc_up_nodes(MyName, AllMembers, RunEnv1),
    {UpNodes, Partitions, S#ch_mgr{runenv=RunEnv2}}.

calc_up_nodes(MyName, AllMembers, RunEnv1) ->
    put(myname_hack, MyName),
    case proplists:get_value(use_partition_simulator, RunEnv1) of
        true ->
            calc_up_nodes_sim(MyName, AllMembers, RunEnv1);
        false ->
            UpNodesNew = (AllMembers -- get_remember_down_list()),
            RunEnv2 = update_runenv_with_up_nodes(UpNodesNew, RunEnv1),
            {UpNodesNew, [], RunEnv2}
    end.

update_runenv_with_up_nodes(UpNodesNew, RunEnv1) ->
    LastUpNodes0 = proplists:get_value(last_up_nodes, RunEnv1),
    if UpNodesNew /= LastUpNodes0 ->
            replace(RunEnv1,
                    [{last_up_nodes, UpNodesNew},
                     {last_up_nodes_time, now()}]);
       true ->
            RunEnv1
    end.

calc_up_nodes_sim(MyName, AllMembers, RunEnv1) ->
    {Partitions2, Islands2} = machi_partition_simulator:get(AllMembers),
    catch ?REACT({calc_up_nodes,?LINE,[{partitions,Partitions2},
                                       {islands,Islands2}]}),
    UpNodes = lists:sort(
                [Node || Node <- AllMembers,
                         not lists:member({MyName, Node}, Partitions2),
                         not lists:member({Node, MyName}, Partitions2)]),
    RunEnv2 = replace(RunEnv1,
                      [{network_partitions, Partitions2},
                       {network_islands, Islands2},
                       {up_nodes, UpNodes}]),
    catch ?REACT({calc_up_nodes,?LINE,[{partitions,Partitions2},
                                       {islands,Islands2},
                                       {up_nodes, UpNodes}]}),
    RunEnv3 = update_runenv_with_up_nodes(UpNodes, RunEnv2),
    {UpNodes, Partitions2, RunEnv3}.

replace(PropList, Items) ->
    Tmp = Items ++ PropList,
    [{K, proplists:get_value(K, Tmp)} || K <- proplists:get_keys(Tmp)].

rank_and_sort_projections([], CurrentProj) ->
    rank_projections([CurrentProj], CurrentProj);
rank_and_sort_projections(Ps, CurrentProj) ->
    Epoch = lists:max([Proj#projection_v1.epoch_number || Proj <- Ps]),
    MaxPs = [Proj || Proj <- Ps,
                     Proj#projection_v1.epoch_number == Epoch],
    %% Sort with highest rank first (custom sort)
    lists:sort(fun({RankA,_}, {RankB,_}) -> RankA > RankB end,
               rank_projections(MaxPs, CurrentProj)).

%% Caller must ensure all Projs are of the same epoch number.
%% If the caller gives us projections with different epochs, we assume
%% that the caller is doing an OK thing.
%%
%% TODO: This implementation currently gives higher rank to the last
%%       member of All_list, which is typically/always/TODO-CLARIFY
%%       sorted.  That's fine, but there's a source of unnecessary
%%       churn: during repair, we assume that the head of the chain is
%%       the coordinator of the repair.  So any time that the head
%%       makes a repair-related transition, that projection may get
%%       quickly replaced by an identical projection that merely has
%%       higher rank because it's authored by a higher-ranked member.
%%       Worst case, for chain len=4:
%%          E+0: author=a, upi=[a], repairing=[b,c,d]
%%          E+1: author=b, upi=[a], repairing=[b,c,d] (**)
%%          E+2: author=c, upi=[a], repairing=[b,c,d] (**)
%%          E+3: author=d, upi=[a], repairing=[b,c,d] (**)
%%          E+4: author=a, upi=[a,b], repairing=[c,d]
%%          E+5: author=b, upi=[a,b], repairing=[c,d] (**)
%%          E+6: author=c, upi=[a,b], repairing=[c,d] (**)
%%          E+7: author=d, upi=[a,b], repairing=[c,d] (**)
%%          E+... 6 more (**) epochs when c &amp; d finish their repairs.
%%       Ideally, the "(**)" epochs are avoidable churn.
%%       Perhaps this means that we should change the responsibility
%%       for repair management to the highest ranking member of the
%%       UPI_list?
%%       TODO Hrrrmmmmm ... what about the TODO comment in A40's A40a clause?
%%       That could perhaps resolve this same problem in a better way?

rank_projections(Projs, CurrentProj) ->
    #projection_v1{all_members=All_list} = CurrentProj,
    MemberRank = orddict:from_list(
                   lists:zip(All_list, lists:seq(1, length(All_list)))),
    N = ?MAX_CHAIN_LENGTH + 1,
    [{rank_projection(Proj, MemberRank, N), Proj} || Proj <- Projs].

rank_projection(#projection_v1{upi=[]}, _MemberRank, _N) ->
    ?RANK_CP_MINORITY_QUORUM;
rank_projection(#projection_v1{author_server=_Author,
                               witnesses=Witness_list,
                               upi=UPI_list,
                               repairing=Repairing_list}, _MemberRank, N) ->
    AuthorRank = 0,
    UPI_witn = [X || X <- UPI_list,     lists:member(X, Witness_list)],
    UPI_full = [X || X <- UPI_list, not lists:member(X, Witness_list)],
    case UPI_list -- Witness_list of
        [] ->
            ?RANK_CP_MINORITY_QUORUM;
        _ ->
            AuthorRank +
                (    N * length(Repairing_list)) +
                (  N*N * length(UPI_witn)) +
                (N*N*N * length(UPI_full))
    end.

do_set_chain_members_dict(MembersDict, #ch_mgr{proxies_dict=OldProxiesDict}=S)->
    _ = ?FLU_PC:stop_proxies(OldProxiesDict),
    ProxiesDict = ?FLU_PC:start_proxies(MembersDict),
    S#ch_mgr{members_dict=MembersDict,
             proxies_dict=ProxiesDict}.

do_react_to_env(#ch_mgr{name=MyName,
                        proj=#projection_v1{epoch_number=Epoch,
                                            members_dict=[]=OldDict}=OldProj,
                        opts=Opts}=S) ->
    put(ttt, [?LINE]),
    %% Read from our local *public* projection store.  If some other
    %% chain member has written something there, and if we are a
    %% member of that chain, then we'll adopt that projection and then
    %% start actively humming in that chain.
    {NewMD, NewProj} =
        get_my_public_proj_boot_info(Opts, OldDict, OldProj),
    case orddict:is_key(MyName, NewMD) of
        false ->
            {{empty_members_dict1, [], Epoch}, S};
        true ->
            CMode = NewProj#projection_v1.mode,
            S2 = do_set_chain_members_dict(NewMD, S),
            {Reply, S3} = react_to_env_C110(NewProj,
                                            S2#ch_mgr{members_dict=NewMD,
                                                      consistency_mode=CMode}),
            {Reply, S3}
    end;
do_react_to_env(S) ->
    put(ttt, [?LINE]),
    %% The not_sanes manager counting dictionary is not strictly
    %% limited to flapping scenarios.  (Though the mechanism first
    %% started as a way to deal with rare flapping scenarios.)
    %%
    %% I believe that the problem cannot happen in real life, but it can
    %% happen in simulated environments, especially if the simulation for
    %% repair can be approximately infinitely fast.
    %%
    %% For example:
    %%   P_current: epoch=1135, UPI=[b,e,a], Repairing=[c,d], author=e
    %%
    %%   Now a partition happens, a & b are on an island, c & d & e on
    %%   the other island.
    %%
    %%   P_newprop: epoch=1136, UPI=[e,c], Repairing=[d], author=e
    %%
    %% Why does e think that this is feasible?  Well, the old UPI was
    %% [b,e,a], and we know that a & b are partitioned away from e.
    %% Therefore e chooses the best UPI, [e].  However, the simulator
    %% now also says, hey, there are nodes in the repairing list, so
    %% let's simulate a repair ... and the repair goes infinitely
    %% quickly ...and the epoch is stable during the repair period
    %% (i.e., both e/repairer and c/repairee remained in the same
    %% epoch 1135) ... so e decides that the simulated repair is
    %% "finished" and it's time to add the repairee to the tail of the
    %% UPI ... so that's why 1136's UPI=[e,c].
    %%
    %% I'll try to add a condition to the simulated repair to try to
    %% make slightly fewer assumptions in a row.  However, I believe
    %% it's a good idea to keep this too-many-not_sane-transition-
    %% attempts counter very generic (i.e., not specific for flapping
    %% as it once was).
    %%
    %% The not_sanes counter dict should be reset when we have had at
    %% least 3 state transitions that did not have a not_sane
    %% suggested projection transition or whenever we fall back to the
    %% none_projection.
    %%
    %% We'll probably implement a very simple counter that may/will be
    %% *inaccurate* by at most one -- so any reset test should ignore
    %% counter values of 0 & 1.
    %%
    put(react, []),
    try
        S2 = if S#ch_mgr.sane_transitions > 3 -> % TODO review this constant
                S#ch_mgr{not_sanes=orddict:new()};
                true ->
                     S
             end,
        %% Perhaps tell the fitness server to spam everyone.
?TTT(),
        case random:uniform(100) of
            N when N < 5 ->
                machi_fitness:send_spam_to_everyone(S#ch_mgr.fitness_svr),?TTT();
            _ ->
                ok
        end,
?TTT(),
        %% NOTE: If we use the fitness server's unfit list at the start, then
        %% we would need to add some kind of poll/check for down members to
        %% check if they are now up.  Instead, our lazy attempt to read from
        %% all servers in A20 will give us the info we need to remove a down
        %% server from our last_down list (and also inform our fitness server
        %% of the down->up change).
        %% 
        %% TODO? We may need to change this behavior to make our latency
        %% jitter smoother by only talking to servers that we believe are fit.
        %% But we will defer such work because it may never be necessary.
        {Res, S3} = react_to_env_A10(S2),
?TTT(),
        S4 = manage_last_down_list(S3),
        %% When in CP mode, we call the poll function twice: once at the start
        %% of reacting (in state A10) & once after.  This call is the 2nd.
?TTT(),
        Sx = poll_private_proj_is_upi_unanimous(S4),
?TTT(),
        {Res, Sx}
    catch
        throw:{zerf,_}=_Throw ->
            Proj = S#ch_mgr.proj,
io:format(user, "zerf ~p caught ~p\n", [S#ch_mgr.name, _Throw]),
            {{no_change, [], Proj#projection_v1.epoch_number}, S}
    end.

manage_last_down_list(#ch_mgr{last_down=LastDown,fitness_svr=FitnessSvr,
                              members_dict=MembersDict}=S) ->
    case get_remember_down_list() of
        Down when Down == LastDown ->
            S;
        Down ->
            machi_fitness:update_local_down_list(FitnessSvr, Down, MembersDict),
            S#ch_mgr{last_down=Down}
    end.

react_to_env_A10(S) ->
    ?REACT(a10),
    react_to_env_A20(0, poll_private_proj_is_upi_unanimous(S)).

react_to_env_A20(Retries, #ch_mgr{name=MyName, proj=P_current}=S) ->
    ?REACT(a20),
    init_remember_down_list(),
    {UnanimousTag, P_latest, ReadExtra, S2} =
        do_cl_read_latest_public_projection(true, S),
    LastComplaint = get(rogue_server_epoch),
    case orddict:is_key(P_latest#projection_v1.author_server,
                        S#ch_mgr.members_dict) of
        false when P_latest#projection_v1.epoch_number /= LastComplaint ->
            put(rogue_server_epoch, P_latest#projection_v1.epoch_number),
            Rogue = P_latest#projection_v1.author_server,
            error_logger:info_msg("Chain manager ~w found latest public "
                                  "projection ~w has author ~w not a member "
                                  "of our members list ~w.  Please check "
                                  "chain membership on this "
                                  "rogue chain manager ~w.\n",
                                  [S#ch_mgr.name,
                                   P_latest#projection_v1.epoch_number,
                                   Rogue,
                                   [K || {K,_} <- orddict:to_list(S#ch_mgr.members_dict)],
                                   Rogue]);
        _ ->
            ok
    end,
    case lists:member(MyName, P_latest#projection_v1.all_members) of
        false when P_latest#projection_v1.epoch_number /= LastComplaint,
                   P_latest#projection_v1.all_members /= [] ->
            put(rogue_server_epoch, P_latest#projection_v1.epoch_number),
            error_logger:info_msg("Chain manager ~w found latest public "
                                  "projection ~w with author ~w has a "
                                  "members list ~w that does not include me.  "
                                  "We assume this is a result of administrator "
                                  "action and will thus wedge ourselves until "
                                  "we are re-added to the chain or shutdown.\n",
                                  [S#ch_mgr.name,
                                   P_latest#projection_v1.epoch_number,
                                   P_latest#projection_v1.author_server,
                                   P_latest#projection_v1.all_members]),
            EpochID = machi_projection:make_epoch_id(P_current),
            ProjStore = get_projection_store_pid_or_regname(S),
            {ok, NotifyPid} = machi_projection_store:get_wedge_notify_pid(ProjStore),
            _QQ = machi_flu1:update_wedge_state(NotifyPid, true, EpochID),
            #projection_v1{epoch_number=Epoch,
                           chain_name=ChainName,
                           all_members=All_list,
                           witnesses=Witness_list,
                           members_dict=MembersDict} = P_current,
            P_none0 = make_none_projection(Epoch,
                                           MyName, All_list, Witness_list, MembersDict),
            P_none = P_none0#projection_v1{chain_name=ChainName},
            {{now_using,[],Epoch}, set_proj(S2, P_none)};
        _ ->
            react_to_env_A21(Retries, UnanimousTag, P_latest, ReadExtra, S2)
    end.

react_to_env_A21(Retries, UnanimousTag, P_latest, ReadExtra, S) ->
    %% The UnanimousTag isn't quite sufficient for our needs.  We need
    %% to determine if *all* of the UPI+Repairing FLUs are members of
    %% the unanimous server replies.  All Repairing FLUs should be up
    %% now (because if they aren't then they cannot be repairing), so 
    %% all Repairing FLUs have no non-race excuse not to be in UnanimousFLUs.
    UnanimousFLUs = lists:sort(proplists:get_value(unanimous_flus, ReadExtra)),
    UPI_Repairing_FLUs = lists:sort(P_latest#projection_v1.upi ++
                                    P_latest#projection_v1.repairing),
    All_UPI_Repairing_were_unanimous =
        ordsets:is_subset(ordsets:from_list(UPI_Repairing_FLUs),
                          ordsets:from_list(UnanimousFLUs)),
    NotUnanimousFLUs = lists:sort(proplists:get_value(not_unanimous_flus,
                                                      ReadExtra, [xxx])),
    NotUnanimousPs = lists:sort(proplists:get_value(not_unanimous_answers,
                                                    ReadExtra, [xxx])),
    NotUnanimousSumms = [machi_projection:make_summary(
                       P#projection_v1{dbg2=[omitted]}) ||
                            P <- NotUnanimousPs,
                            is_record(P, projection_v1)],
    BadAnswerFLUs = lists:sort(proplists:get_value(bad_answer_flus, ReadExtra)),
    BadAnswers = lists:sort(proplists:get_value(bad_answers, ReadExtra)),
    ?REACT({a20,?LINE,[{upi_repairing,UPI_Repairing_FLUs},
                       {unanimous_flus,UnanimousFLUs},
                       {all_upi_repairing_were_unanimous,All_UPI_Repairing_were_unanimous},
                       {not_unanimous_flus, NotUnanimousFLUs},
                       {not_unanimous_answers, NotUnanimousSumms},
                       {bad_answer_flus, BadAnswerFLUs},
                       {bad_answers, BadAnswers}
                      ]}),
    LatestUnanimousP =
        if UnanimousTag == unanimous
           andalso
           All_UPI_Repairing_were_unanimous ->
                ?REACT({a20,?LINE}),
                true;
           UnanimousTag == unanimous ->
                ?REACT({a20,?LINE}),
                false;
           UnanimousTag == not_unanimous ->
                ?REACT({a20,?LINE}),
                false;
           true ->
                exit({badbad, UnanimousTag})
        end,
    react_to_env_A29(Retries, P_latest, LatestUnanimousP, ReadExtra, S).

react_to_env_A29(Retries, P_latest, LatestUnanimousP, _ReadExtra,
                 #ch_mgr{consistency_mode=CMode,
                         proj=P_current} = S) ->
    {Epoch_current,_} = EpochID_current =
        machi_projection:get_epoch_id(P_current),
    {Epoch_latest,_} = EpochID_latest = machi_projection:get_epoch_id(P_latest),
    Trigger = if CMode == cp_mode, EpochID_latest /= EpochID_current ->
                      true;
                 true ->
                      false
              end,
    if Trigger ->
            ?REACT({a29, ?LINE,
                    [{epoch_id_latest,EpochID_latest},
                     {epoch_id_current,EpochID_current},
                     {old_current, machi_projection:make_summary(P_current)}]}),
            if Epoch_latest >= Epoch_current orelse Epoch_latest == 0 orelse
               P_current#projection_v1.upi == [] ->
                    ok;                                 % sanity check
               true ->
                    exit({?MODULE,?LINE,
                          {epoch_latest,Epoch_latest},
                          {epoch_current,Epoch_current},
                          {latest,machi_projection:make_summary(P_latest)},
                          {current,machi_projection:make_summary(P_current)}})
            end,
            put(yyy_hack, []),
            case make_zerf(P_current, S) of
                Zerf when is_record(Zerf, projection_v1) ->
                    ?REACT({a29, ?LINE,
                            [{zerf_backstop, true},
                             {zerf_in, machi_projection:make_summary(Zerf)}]}),
                    #projection_v1{dbg=ZerfDbg} = Zerf,
                    Backstop = if Zerf#projection_v1.upi == [] ->
                                       [];
                                  true ->
                                       [{zerf_backstop,true}]
                               end,
                    P_current_calc = Zerf#projection_v1{
                                       dbg=Backstop ++ ZerfDbg},
                    react_to_env_A30(Retries, P_latest, LatestUnanimousP,
                                     P_current_calc, S);
                Zerf ->
                    {{{yo_todo_incomplete_fix_me_cp_mode, line, ?LINE, Zerf}}}
            end;
       true ->
            ?REACT({a29, ?LINE, []}),
            react_to_env_A30(Retries, P_latest, LatestUnanimousP, P_current, S)
    end.

react_to_env_A30(Retries, P_latest, LatestUnanimousP, P_current_calc,
                 #ch_mgr{name=MyName, proj=P_current,
                         consistency_mode=CMode} = S) ->
    V = case file:read_file("/tmp/moomoo."++atom_to_list(S#ch_mgr.name)) of {ok,_} -> true; _ -> false end,
    if V -> io:format(user, "A30: ~w: ~p\n", [S#ch_mgr.name, get(react)]); true -> ok end,
    ?REACT(a30),
    AllHosed = get_unfit_list(S#ch_mgr.fitness_svr),
    ?REACT({a30, ?LINE,
            [{current, machi_projection:make_summary(S#ch_mgr.proj)},
             {current_calc, machi_projection:make_summary(P_current_calc)},
             {latest, machi_projection:make_summary(P_latest)},
             {all_hosed, AllHosed}
            ]}),
    ?REACT({a30, ?LINE, []}),
    case lists:member(MyName, AllHosed) of
        true ->
            #projection_v1{epoch_number=Epoch, all_members=All_list,
                           witnesses=Witness_list,
                           members_dict=MembersDict} = P_latest,
            P = #projection_v1{down=Down} =
                make_none_projection(Epoch + 1, MyName, All_list,
                                     Witness_list, MembersDict),
            ChainName = P_current#projection_v1.chain_name,
            P1 = P#projection_v1{chain_name=ChainName},
            P_newprop = if CMode == ap_mode ->
                                %% Not really none proj: just myself, AP style
                                machi_projection:update_checksum(
                                  P1#projection_v1{upi=[MyName],
                                                  down=Down -- [MyName],
                                                  dbg=[{hosed_list,AllHosed}]});
                           CMode == cp_mode ->
                                machi_projection:update_checksum(
                                  P1#projection_v1{dbg=[{hosed_list,AllHosed}]})
                        end,
            react_to_env_A40(Retries, P_newprop, P_latest, LatestUnanimousP,
                             P_current_calc, true, S);
        false ->
            react_to_env_A31(Retries, P_latest, LatestUnanimousP,
                             P_current_calc, AllHosed, S)
    end.

react_to_env_A31(Retries, P_latest, LatestUnanimousP, P_current_calc,
                 AllHosed, #ch_mgr{name=MyName} = S) ->
    {P_newprop1, S2,_Up} = calc_projection(S, MyName, AllHosed, P_current_calc),
    ?REACT({a31, ?LINE,
            [{newprop1, machi_projection:make_summary(P_newprop1)}]}),

    {P_newprop2, S3} = {P_newprop1, S2},

    %% Move the epoch number up ... originally done in C300.
    #projection_v1{epoch_number=Epoch_newprop2}=P_newprop2,
    #projection_v1{epoch_number=Epoch_latest}=P_latest,
    NewEpoch = erlang:max(Epoch_newprop2, Epoch_latest) + 1,
    P_newprop3 = P_newprop2#projection_v1{epoch_number=NewEpoch},
    ?REACT({a31, ?LINE, [{newprop3, machi_projection:make_summary(P_newprop3)}]}),

    {P_newprop10, S10} = {P_newprop3, S3},
    P_newprop11 = machi_projection:update_checksum(P_newprop10),
    ?REACT({a31, ?LINE, [{newprop11, machi_projection:make_summary(P_newprop11)}]}),

    react_to_env_A40(Retries, P_newprop11, P_latest, LatestUnanimousP,
                     P_current_calc, false, S10).

a40_latest_author_down(#projection_v1{author_server=LatestAuthor}=_P_latest,
                       #projection_v1{upi=[], repairing=[],
                                      all_members=AllMembers}=_P_newprop,
                       #ch_mgr{name=MyName, runenv=RunEnv}) ->
    %% P_newprop is the none projection.  P_newprop's down list is
    %% bogus, we cannot use it here.
    {Up, _Partitions, _RunEnv2} = calc_up_nodes(MyName, AllMembers, RunEnv),
    ?REACT({a40,?LINE,[{latest_author,LatestAuthor}, {up,Up}]}),
    not lists:member(LatestAuthor, Up);
a40_latest_author_down(#projection_v1{author_server=LatestAuthor}=_P_latest,
                       #projection_v1{down=NewPropDown}=_P_newprop, _S) ->
    lists:member(LatestAuthor, NewPropDown).

react_to_env_A40(Retries, P_newprop, P_latest, LatestUnanimousP,
                 P_current_calc, AmHosedP, #ch_mgr{name=MyName, proj=P_current,
                                                   consistency_mode=CMode}=S) ->
    ?REACT(a40),
    [{Rank_newprop, _}] = rank_projections([P_newprop], P_current),
    [{Rank_latest, _}] = rank_projections([P_latest], P_current),
    LatestAuthorDownP = a40_latest_author_down(P_latest, P_newprop, S)
                        andalso
                        P_latest#projection_v1.author_server /= MyName,
    P_latestStable = make_basic_comparison_stable(P_latest),
    P_currentStable = make_basic_comparison_stable(P_current),
    %% 2015-09-15: Experiment time.  For CP mode, if the P_latest UPI includes
    %% me, then this clause is possible.  However, if P_latest has a server in
    %% UPI or repairing that I believe is down, then this projection is not
    %% relevant to me.  E.g. in CP mode, if P_newprop is none proj (not
    %% because AmHosedP is true but because I've decided that a majority
    %% quorum is not possible now), then P_newprop will have a very low rank!
    Latest_vs_newprop_down_p =
        if CMode == ap_mode ->
                %% TODO: Apply 2015-09-15 comment above to AP mode also?
                false;
           CMode == cp_mode ->
                P_latest_s = ordsets:from_list(P_latest#projection_v1.upi ++
                                              P_latest#projection_v1.repairing),
                Down_s = ordsets:from_list(P_newprop#projection_v1.down),
                %% If any of P_latest's servers are in P_newprop's down, then
                %% we have a disagreement.
                not ordsets:is_disjoint(P_latest_s, Down_s)
        end,
    AmExcludedFromLatestAll_p =
        P_latest#projection_v1.epoch_number /= 0
        andalso
        (not lists:member(MyName, P_latest#projection_v1.all_members)),
    ?REACT({a40, ?LINE,
            [{latest_author, P_latest#projection_v1.author_server},
             {am_excluded_from_latest_all_p, AmExcludedFromLatestAll_p},
             {author_is_down_p, LatestAuthorDownP},
             {rank_latest, Rank_latest},
             {rank_newprop, Rank_newprop}]}),

    if
        AmExcludedFromLatestAll_p ->
            ?REACT({a40, ?LINE, [{latest,machi_projection:make_summary(P_latest)}]}),
            react_to_env_A50(P_latest, [], S);

        AmHosedP ->
            ExpectedUPI = if CMode == cp_mode -> [];
                             CMode == ap_mode -> [MyName]
                          end,
            if (P_current#projection_v1.upi /= ExpectedUPI orelse
                P_current#projection_v1.repairing /= [])
               andalso
               (P_newprop#projection_v1.upi == ExpectedUPI andalso
                P_newprop#projection_v1.repairing == []) ->
                    %% I am hosed.  I need to shut up and quit disturbing my
                    %% peers.  If P_latest is the none projection that I wrote
                    %% on a previous iteration and it's also unanimous, then
                    %% go to B10 so that I can adopt it.  Otherwise, tell the
                    %% world my intention via C300.
                    if P_latest#projection_v1.author_server == MyName andalso
                       P_latest#projection_v1.upi == ExpectedUPI andalso
                       LatestUnanimousP ->
                            ?REACT({a40, ?LINE, []}),
                            react_to_env_B10(Retries, P_newprop, P_latest,
                                             LatestUnanimousP, P_current_calc,
                                             AmHosedP, Rank_newprop, Rank_latest, S);
                       true ->
                            ?REACT({a40, ?LINE, [{q1,P_current#projection_v1.upi},
                                                 {q2,P_current#projection_v1.repairing},
                                                 {q3,P_newprop#projection_v1.upi},
                                                 {q4,P_newprop#projection_v1.repairing},
                                                 {q5, ExpectedUPI}]}),
                            %% Ha, there's a "fun" sync problem with the
                            %% machi_chain_manager1_converge_demo simulator:
                            %% two servers could get caught in a mutual lock-
                            %% step that we would end up in this branch 100%
                            %% of the time because each would only ever see
                            %% P_newprop authored by the other server.
                            timer:sleep(random:uniform(100)),
                            react_to_env_C300(P_newprop, P_latest, S)
                    end;
               true ->
                    ?REACT({a40, ?LINE, []}),
                    react_to_env_A50(P_latest, [], S)
            end;

        %% Epoch == 0 is reserved for first-time, just booting conditions.
        (Rank_newprop > 0 orelse (Rank_newprop == ?RANK_CP_MINORITY_QUORUM))
        andalso
        ((P_current#projection_v1.epoch_number > 0
          andalso
          P_latest#projection_v1.epoch_number > P_current#projection_v1.epoch_number)
         orelse
         not LatestUnanimousP) ->
            ?REACT({a40, ?LINE,
                    [{latest_epoch, P_latest#projection_v1.epoch_number},
                     {current_epoch, P_current#projection_v1.epoch_number},
                     {latest_unanimous_p, LatestUnanimousP}]}),

            %% TODO 2015-09-14: Should rank be factored in here?  If P_latest
            %% rank is definitely lower than current rank (or perhaps lower
            %% than P_newprop rank?), then don't accept it.  Hrm, I'm not sure
            %% how that would ripple through the rest of this state machine &
            %% interactions with the other state machines, hrmmmm.  For a real
            %% example, if current upi/rep = [c,b,g,a],[f], going to P_latest
            %% of [b,g,f],[a,c] doesn't make sense (even if we ignore the UPI
            %% sanity problem in this example).
            react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                             P_current_calc, AmHosedP,
                             Rank_newprop, Rank_latest, S);

        Rank_newprop > 0
        andalso
        (P_latest#projection_v1.epoch_number < P_current#projection_v1.epoch_number
         orelse
         P_latestStable /= P_currentStable) ->
            ?REACT({a40, ?LINE,
                    [{latest, P_latestStable},
                     {current, P_currentStable},
                     {neq, P_latestStable /= P_currentStable}]}),

            %% Both of these cases are rare.  Elsewhere, the code
            %% assumes that the local FLU's projection store is always
            %% available, so reads & writes to it aren't going to fail
            %% willy-nilly.  If that assumption is true, then we can
            %% reason as follows:
            %%
            %% a. If we can always read from the local FLU projection
            %% store, then the 1st clause isn't possible because
            %% P_latest's epoch # must be at least as large as
            %% P_current's epoch #
            %%
            %% b. If P_latest /= P_current, then there can't be a
            %% unanimous reply for P_latest, so the earlier 'if'
            %% clause would be triggered and so we could never reach
            %% this clause.
            %%
            %% I'm keeping this 'if' clause just in case the local FLU
            %% projection store assumption changes.
            react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                             P_current_calc, AmHosedP,
                             Rank_newprop, Rank_latest, S);

        %% A40a (see flowchart)
        Rank_newprop > Rank_latest ->
            ?REACT({a40, ?LINE,
                    [{rank_latest, Rank_latest},
                     {rank_newprop, Rank_newprop},
                     {latest_author, P_latest#projection_v1.author_server}]}),

            %% TODO: There may be an "improvement" here.  If we're the
            %% highest-ranking FLU in the all_members list, then if we make a
            %% projection where our UPI list is the same as P_latest's, and
            %% our repairing list is the same as P_latest's, then it may not
            %% be necessary to write our projection: it doesn't "improve"
            %% anything UPI-wise or repairing-wise.  But it isn't clear to me
            %% if it's 100% correct to "improve" here and skip writing
            %% P_newprop, yet.
            react_to_env_C300(P_newprop, P_latest, S);

        %% A40b (see flowchart)
        Rank_newprop > 0
        andalso
        P_latest#projection_v1.author_server == MyName
        andalso
        (P_newprop#projection_v1.upi /= P_latest#projection_v1.upi
         orelse
         P_newprop#projection_v1.repairing /= P_latest#projection_v1.repairing) ->
            ?REACT({a40, ?LINE,
                   [{latest_author, P_latest#projection_v1.author_server},
                   {newprop_upi, P_newprop#projection_v1.upi},
                   {latest_upi, P_latest#projection_v1.upi},
                   {newprop_repairing, P_newprop#projection_v1.repairing},
                   {latest_repairing, P_latest#projection_v1.repairing}]}),

            react_to_env_C300(P_newprop, P_latest, S);

        %% A40c (see flowchart)
        LatestAuthorDownP ->
            ?REACT({a40, ?LINE,
                   [{latest_author, P_latest#projection_v1.author_server},
                    {author_is_down_p, LatestAuthorDownP}]}),

            react_to_env_C300(P_newprop, P_latest, S);

    Latest_vs_newprop_down_p ->
            ?REACT({a40, ?LINE, []}),
            %% P_latest isn't relevant: it has at least one member of UPI
            %% and/or repairing that we believe is down.  Write P_newprop.
            react_to_env_C300(P_newprop, P_latest, S);

        true ->
            ?REACT({a40, ?LINE, [true]}),
            CurrentZerfInStatus_p = has_make_zerf_annotation(P_current),
            ?REACT({a40,?LINE,[{currentzerfinstatus_p,CurrentZerfInStatus_p}]}),
            GoTo50_p = if CurrentZerfInStatus_p andalso
                          P_newprop#projection_v1.upi /= [] ->
                               %% One scenario here: we are waking up after
                               %% a slumber with the none proj and need to
                               %% send P_newprop (which has non/empty UPI)
                               %% through the process to continue chain
                               %% recovery.
                               ?REACT({a40, ?LINE, []}),
                               false;
                          true ->
                               ?REACT({a40, ?LINE, []}),
                               true
                       end,
            if GoTo50_p ->
                    ?REACT({a40, ?LINE, []}),
                    FinalProps = [{throttle_seconds, 0}],
                    react_to_env_A50(P_latest, FinalProps, S);
               true ->
                    ?REACT({a40, ?LINE, []}),
                    react_to_env_C300(P_newprop, P_latest, S)
            end
    end.

react_to_env_A50(P_latest, FinalProps, #ch_mgr{proj=P_current}=S) ->
    ?REACT(a50),
    ?REACT({a50, ?LINE, [{current_epoch, P_current#projection_v1.epoch_number},
                         {latest_epoch, P_latest#projection_v1.epoch_number},
                         {final_props, FinalProps}]}),
    V = case file:read_file("/tmp/moomoo."++atom_to_list(S#ch_mgr.name)) of {ok,_} -> true; _ -> false end,
    if V -> io:format(user, "A50: ~w: ~p\n", [S#ch_mgr.name, get(react)]); true -> ok end,
    {{no_change, FinalProps, P_current#projection_v1.epoch_number}, S}.

react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP, P_current_calc,
                _AmHosedP, Rank_newprop, Rank_latest,
                 #ch_mgr{name=MyName, proj=P_current,
                         proj_history=History}=S) ->
    ?REACT(b10),

    P_current_upi = if is_record(P_current, projection_v1) ->
                            P_current#projection_v1.upi;
                       true ->
                            []
                    end,
    #projection_v1{author_server=P_latest_author, witnesses=P_latest_witnesses,
                   upi=P_latest_upi, repairing=P_latest_repairing} = P_latest,
    I_am_in_P_latest_upi = lists:member(MyName, P_latest_upi),
    I_am_in_P_latest_repairing = lists:member(MyName, P_latest_repairing),
    IsRelevantToMe_p = if P_latest_author == MyName ->
                               true;
                          P_latest_upi == [] ->
                               %% If we're here, P_latest's author is not me.
                               %% If someone else suggests upi=[], it's not
                               %% relevant to me.
                               false;
                          not (I_am_in_P_latest_upi
                               orelse I_am_in_P_latest_repairing) ->
                               %% There is no sense for me to leave whatever
                               %% chain I'm in and go join some other chain
                               %% that doesn't include me at all in either
                               %% UPI or repairing.  E.g., someone else
                               %% fell back to none proj ... that proj is
                               %% now P_latest and it's unanimous.  But that
                               %% doesn't make it a good idea.  ^_^
                               case lists:member(MyName, P_latest_witnesses) of
                                   true ->
                                       %% CP mode: Commentary above doesn't
                                       %% apply to me.  For example, I am a
                                       %% witness, and P_current
                                       %% upi=[Me,NonWit1].  Now P_latest is
                                       %% upi=[NonWit1,NonWit2]. Yes, this
                                       %% projection is definitely relevant.
                                       true;
                                   false ->
                                       %% Commentary above does apply.
                                       false
                               end;
                          I_am_in_P_latest_repairing ->
                               %% If I'm already in the current UPI, and the
                               %% current UPI is longer than P_latest's UPI,
                               %% then it makes no sense to leave the UPI to
                               %% go to someone else's suggestion of
                               %% repairing.  If I'm the only member of
                               %% P_current UPI, then sure, then having me
                               %% join a repairing list is relevant.
                               not (lists:member(MyName, P_current_upi) andalso
                                  length(P_current_upi) > length(P_latest_upi));
                          true ->
                               true
                       end,
    HistoryList = queue:to_list(History),
    UniqueHistories = lists:usort(HistoryList),
    UniqueHistoryTrigger_p = length(HistoryList) > (?MAX_HISTORY_LENGTH-1)
                             andalso case UniqueHistories of
                                 [ {[],[]} ]       -> false;
                                 [ {[MyName],[]} ] -> false;
                                 [ _  ]            -> true;
                                 _                 -> false
                             end,
    ?REACT({b10,?LINE,[{newprop_epoch,P_newprop#projection_v1.epoch_number},
                       {is_relevant_to_me_p,IsRelevantToMe_p},
                       {unique_histories,UniqueHistories},
                       {unique_history_trigger_p,UniqueHistoryTrigger_p}]}),
    if
        UniqueHistoryTrigger_p ->
            ?REACT({b10, ?LINE, []}),
            %% We need to do something drastic: we're repeating ourselves.  In
            %% a former version of this code, we called this "flapping" and
            %% would enter an alternative projection calculation mode in order
            %% to dampen the flapping.  In today's version of the code, we use
            %% the machi_fitness service to help us figure out who is causing
            %% the flapping so that we can exclude them from our projection
            %% calculations.
            %%
            %% In this hypothetical example (i.e., I haven't witnessed this
            %% actually happening with this code, but it's a valid scenario, I
            %% believe), with a chain length of 7, during an asymmetric
            %% partition scenario, we could have:
            %%     f suggests       : upi=[b,g,f] repairing=[c]
            %%     c,b,g all suggest: upi=[c,b,g], repairing=[f]
            %%
            %% The projection ranking and UPI lengths are the same, but the
            %% UPIs are incompatible.  In the Git history, very recently, we
            %% hit a case very similar to this one.  The fix, after more
            %% thought, wasn't sufficient.  A more comprehensive fix, I
            %% believe, is using the older "flapping" detection mechanism for
            %% this worst-case condition: go to C103, fall back to shortest &
            %% safe projection, tell fitness server we're administratively
            %% down for a short while (to signal to other state machines that
            %% they need to adapt to our bad situation), and then resume.

            io:format(user, "\nCONFIRM dbg *************************** ~w UniqueHistoryTrigger_p\n", [MyName]),
            react_to_env_C103(P_newprop, P_latest, P_current_calc, S);

        LatestUnanimousP andalso IsRelevantToMe_p ->
            ?REACT({b10, ?LINE,
                    [{latest_unanimous_p, LatestUnanimousP},
                     {latest_epoch,P_latest#projection_v1.epoch_number},
                     {latest_author,P_latest#projection_v1.author_server},
                     {newprop_epoch,P_newprop#projection_v1.epoch_number},
                     {newprop_author,P_newprop#projection_v1.author_server}
                    ]}),

            react_to_env_C100(P_newprop, P_latest, P_current_calc, S);

        Retries > 2 ->
            ?REACT({b10, ?LINE, [{retries, Retries}]}),

            %% The author of P_latest is too slow or crashed.
            %% Let's try to write P_newprop and see what happens!
            react_to_env_C300(P_newprop, P_latest, S);

        Rank_latest < 0 andalso Rank_newprop < 0 ->
            ?REACT({b10, ?LINE,
                    [{rank_latest, Rank_latest},
                     {rank_newprop, Rank_newprop}]}),
            %% The latest projection is none proj, so is my newprop.  In CP
            %% mode and asymmetric partitions, we might write a lot of new
            %% none projs, but this action also helps trigger others to change
            %% their projections in a healthy way.  TODO: perhaps use a
            %% counter here to silence ourselves for good after a certain time
            %% and/or number of retries?
            case random:uniform(100) of
                N when N < 4 ->
                    ?REACT({b10, ?LINE}),
                    react_to_env_C300(P_newprop, P_latest, S);
                _ ->
                    ?REACT({b10, ?LINE}),
                    react_to_env_A50(P_latest, [], S)
            end;

        Rank_latest >= Rank_newprop
        andalso
        P_latest#projection_v1.author_server /= MyName ->
            ?REACT({b10, ?LINE,
                    [{rank_latest, Rank_latest},
                     {rank_newprop, Rank_newprop},
                     {latest_author, P_latest#projection_v1.author_server}]}),

            %% Give the author of P_latest an opportunity to write a
            %% new projection in a new epoch to resolve this mixed
            %% opinion.
            react_to_env_C200(Retries, P_latest, S);

        true ->
            ?REACT({b10, ?LINE}),
            ?REACT({b10, ?LINE, [{retries,Retries},{rank_latest, Rank_latest},                     {rank_newprop, Rank_newprop},                     {latest_author, P_latest#projection_v1.author_server}]}), % TODO debug delete me!

            %% P_newprop is best, so let's write it.
            react_to_env_C300(P_newprop, P_latest, S)
    end.

react_to_env_C100(P_newprop,
                  #projection_v1{author_server=Author_latest}=P_latest,
                  P_current_calc,
                  #ch_mgr{name=MyName, proj=P_current, consistency_mode=CMode,
                          not_sanes=NotSanesDict0}=S) ->
    ?REACT(c100),

    P_cur_for_sanity = if CMode == cp_mode ->
                               %% Assume E = P_latest's epoch #.  P_current
                               %% may be at epoch E-delta but P_current_calc
                               %% is at exactly E because E-delta is stale,
                               %% and the CP world has changed while we were
                               %% napping.  But the "exactly epoch E" will
                               %% cause problems for the strictly monotonic
                               %% epoch check in our sanity checking.  So we
                               %% fake the epoch number here to work around
                               %% the strictly-SI check.
                               %%
                               %% If we don't fake the epoch here, then (I
                               %% have observed today) that we go through
                               %% several insane projection attempts and then
                               %% reset and eventually come to the right
                               %% answer ... but this churn is avoidable, and
                               %% for CP mode this is an ok safeguard to bend
                               %% expressly because of the make_zerf() aspect
                               %% of CP's chain processing.
                               E_c = erlang:min(
                                     P_current#projection_v1.epoch_number,
                                     P_current_calc#projection_v1.epoch_number),
                               P_current_calc#projection_v1{epoch_number=E_c};
                          CMode == ap_mode ->
                               P_current
                       end,
    ?REACT({c100, ?LINE,
            [{current_epoch, P_cur_for_sanity#projection_v1.epoch_number},
             {current_author, P_cur_for_sanity#projection_v1.author_server},
             {current_upi, P_cur_for_sanity#projection_v1.upi},
             {current_repairing, P_cur_for_sanity#projection_v1.repairing},
             {latest_epoch, P_latest#projection_v1.epoch_number},
             {latest_author, P_latest#projection_v1.author_server},
             {latest_upi, P_latest#projection_v1.upi},
             {latest_repairing, P_latest#projection_v1.repairing}]}),
    Sane = projection_transition_is_sane(P_cur_for_sanity, P_latest, MyName),
    if Sane == true ->
            ok;
       true ->
            %% QQ_current = lists:flatten(io_lib:format("cur=~w:~w,~w/calc=~w:~w,~w", [P_current#projection_v1.epoch_number, P_current#projection_v1.upi, P_current#projection_v1.repairing, P_current_calc#projection_v1.epoch_number, P_current_calc#projection_v1.upi, P_current_calc#projection_v1.repairing])),
            %% QQ_latest = lists:flatten(io_lib:format("~w:~w,~w", [P_latest#projection_v1.epoch_number, P_latest#projection_v1.upi, P_latest#projection_v1.repairing])),
            %% ?V("\n~w-insane-~w-auth=~w ~s -> ~s ~w\n    ~p\n    ~p\n", [?LINE, MyName, P_newprop#projection_v1.author_server, QQ_current, QQ_latest, Sane, get(why2), get(react)]),
            ok
    end,
    ?REACT({c100, ?LINE, [zoo, {me,MyName},
                          {author_latest,Author_latest},
                          {why2, get(why2)}]}),

    %% Note: The value of `Sane' may be `true', `false', or `term() /= true'.
    %%       The error value `false' is reserved for chain order violations.
    %%       Any other non-true value can be used for projection structure
    %%       construction errors, checksum error, etc.
    case Sane of
        _ when P_current#projection_v1.epoch_number == 0 ->
            %% Epoch == 0 is reserved for first-time, just booting conditions
            %% or for when we got stuck in an insane projection transition
            %% and were forced to the none projection to recover.
            ?REACT({c100, ?LINE, [first_write]}),
            %% if Sane == true -> ok;  true -> ?V("~w-insane-~w-~w:~w:~w,", [?LINE, MyName, P_newprop#projection_v1.epoch_number, P_newprop#projection_v1.upi, P_newprop#projection_v1.repairing]) end, %%% DELME!!!
            react_to_env_C110(P_latest, S);
        true ->
            ?REACT({c100, ?LINE, []}),
            %% if Sane == true -> ok;  true -> ?V("~w-insane-~w-~w:~w:~w@~w,", [?LINE, MyName, P_newprop#projection_v1.epoch_number, P_newprop#projection_v1.upi, P_newprop#projection_v1.repairing, ?LINE]) end, %%% DELME!!!

    V = case file:read_file("/tmp/bugbug."++atom_to_list(S#ch_mgr.name)) of {ok,_} -> true; _ -> false end,
    if V -> 
            react_to_env_C103(P_newprop, P_latest, P_current_calc, S);
       true ->
            react_to_env_C110(P_latest, S)
    end;
            %% ORIGINAL react_to_env_C110(P_latest, S);
        NotSaneBummer ->
            ?REACT({c100, ?LINE, [{not_sane, NotSaneBummer}]}),
            react_to_env_C100_inner(Author_latest, NotSanesDict0, MyName,
                                    P_newprop, P_latest, P_current_calc, S)
    end.

react_to_env_C100_inner(Author_latest, NotSanesDict0, _MyName,
                        P_newprop, P_latest, P_current_calc, S) ->
    NotSanesDict = orddict:update_counter(Author_latest, 1, NotSanesDict0),
    S2 = S#ch_mgr{not_sanes=NotSanesDict, sane_transitions=0},
    case orddict:fetch(Author_latest, NotSanesDict) of
        N when N > ?TOO_FREQUENT_BREAKER ->
            ?V("\n\nYOYO ~w breaking the cycle insane-freq=~w by-author=~w of:\n  current: ~w\n  new    : ~w\n", [_MyName, N, Author_latest, machi_projection:make_summary(S#ch_mgr.proj), machi_projection:make_summary(P_latest)]),
            ?REACT({c100, ?LINE, [{not_sanes_author_count, N}]}),
            react_to_env_C103(P_newprop, P_latest, P_current_calc, S2);
        N ->
           %% ?V("YOYO,~w,~w,~w,",[_MyName, P_latest#projection_v1.epoch_number,N]),
            ?REACT({c100, ?LINE, [{not_sanes_author_count, N}]}),
            %% P_latest is not sane.
            %% By process of elimination, P_newprop is best,
            %% so let's write it.
            react_to_env_C300(P_newprop, P_latest, S2)
    end.

react_to_env_C103(#projection_v1{epoch_number=_Epoch_newprop} = _P_newprop,
                  #projection_v1{epoch_number=Epoch_latest,
                                 all_members=All_list} = _P_latest,
                  P_current_calc,
                  #ch_mgr{name=MyName, proj=P_current}=S) ->
    #projection_v1{witnesses=Witness_list,
                   members_dict=MembersDict} = P_current,
    P_none0 = make_none_projection(Epoch_latest,
                                   MyName, All_list, Witness_list, MembersDict),
    ChainName = P_current#projection_v1.chain_name,
    P_none1 = P_none0#projection_v1{chain_name=ChainName,
                                    dbg=[{none_projection,true}]},
    P_none = machi_projection:update_checksum(P_none1),
    ?REACT({c103, ?LINE,
            [{current_epoch, P_current#projection_v1.epoch_number},
             {none_projection_epoch, P_none#projection_v1.epoch_number}]}),
    io:format(user, "SET add_admin_down(~w) at ~w current_epoch ~w none_proj_epoch ~w =====================================\n", [MyName, time(), P_current#projection_v1.epoch_number, P_none#projection_v1.epoch_number]),
    machi_fitness:add_admin_down(S#ch_mgr.fitness_svr, MyName, []),
    timer:sleep(5*1000),
    io:format(user, "SET delete_admin_down(~w) at ~w =====================================\n", [MyName, time()]),
    machi_fitness:delete_admin_down(S#ch_mgr.fitness_svr, MyName),
    react_to_env_C100(P_none, P_none, P_current_calc, S).

react_to_env_C110(P_latest, #ch_mgr{name=MyName} = S) ->
    ?REACT(c110),
    ?REACT({c110, ?LINE, [{latest_epoch,P_latest#projection_v1.epoch_number}]}),
    Extra1 = [],
    Extra2 = [{react,get(react)}],
    P_latest2 = machi_projection:update_dbg2(P_latest, Extra1 ++ Extra2),

    MyStorePid = proxy_pid(MyName, S),
    Goo = P_latest2#projection_v1.epoch_number,
    %% This is the local projection store.  Use a larger timeout, so
    %% that things locally are pretty horrible if we're killed by a
    %% timeout exception.
    Goo = P_latest2#projection_v1.epoch_number,

    %% Ha, yet another example of why distributed systems are so much fun and
    %% hassle and frustration.
    %%
    %% The following write will be to our local private projection store.  But
    %% there are several reasons why the write might fail: client library bug,
    %% slow server (which triggers timeout), kernel decides it doesn't like
    %% this TCP connection, projection server crashed (but our mutual
    %% supervisor parent proc hasn't killed *us* quite yet), etc etc.  Even if
    %% we didn't use TCP and made a gen_server:call() directly, we'd still
    %% have a problem of not knowing 100% for sure if the write was successful
    %% to the projection store ... to know for certain, we need to try to read
    %% it back.
    %%
    %% In contrast to the public projection store writes, Humming Consensus
    %% doesn't care about the status of writes to the public store: it's
    %% always relying only on successful reads of the public store.
    case {?FLU_PC:write_projection(MyStorePid, private, P_latest2,?TO*30),Goo} of
        {ok, Goo} ->
            ?REACT({c110, [{write, ok}]}),
            react_to_env_C111(P_latest, P_latest2, Extra1, MyStorePid, S);
        {{error, bad_arg}, _Goo} ->
            ?REACT({c120, [{write, bad_arg}]}),

            %% This bad_arg error is the result of an implicit pre-condition
            %% failure that is now built-in to the projection store: when
            %% writing a private projection, return {error, bad_arg} if there
            %% the store contains a *public* projection with a higher epoch
            %% number.
            %%
            %% In the context of AP mode, this is harmless: we avoid a bit of
            %% extra work by adopting P_latest now.
            %%
            %% In the context of CP mode, this pre-condition failure is very
            %% important: it signals to us that the world is changing (or
            %% trying to change), and it is vital that we avoid doing
            %% something based on stale info.
            %%
            %% Worst case: our humming consensus round was executing very
            %% quickly until the point immediately before writing our private
            %% projection above: immediately before the private proj write,
            %% we go to sleep for 10 days.  When we wake up after such a long
            %% sleep, we would definitely notice the last projections made by
            %% everyone, but we would miss the intermediate *history* of
            %% chain changes over those 10 days.  In CP mode it's vital that
            %% we don't miss any of that history while we're running (i.e.,
            %% here in this func) or when we're restarting after a
            %% shutdown/crash.
            %%
            %% React to newer public write by restarting the iteration.
            react_to_env_A20(0, S);
        {{error, TO_or_part}=Else, _Goo} when TO_or_part == timeout;
                                              TO_or_part == partition ->
            ?REACT({c110, [{write, Else}]}),
            react_to_env_C111(P_latest, P_latest2, Extra1, MyStorePid, S);
        Else ->
            Summ = machi_projection:make_summary(P_latest2),
            io:format(user, "C110 error by ~w: ~w, ~w\n~p\n",
                      [MyName, Else, Summ, get(react)]),
            error_logger:error_msg("C110 error by ~w: ~w, ~w, ~w\n",
                                   [MyName, Else, Summ, get(react)]),
            exit({c110_failure, MyName, Else, Summ})
    end.

react_to_env_C111(#projection_v1{epoch_number=Epoch}=P_latest, P_latest2,
                  Extra1, MyStorePid, #ch_mgr{name=MyName}=S) ->
    case ?FLU_PC:read_projection(MyStorePid, private, Epoch) of
        {ok, P_latest2} ->
            ?REACT({c111, [{read, ok}]}),
            %% We very intentionally do *not* pass P_latest2 forward:
            %% we must avoid bloating the dbg2 list!
            P_latest2_perhaps_annotated =
                machi_projection:update_dbg2(P_latest, Extra1),
            perhaps_verbose_c111(P_latest2_perhaps_annotated, S),
            react_to_env_C120(P_latest2_perhaps_annotated, [], S);
        {ok, P_wtf} ->
            exit({c111_failure, MyName, Epoch, P_wtf});
        {error, not_written} ->
            ?REACT({c111, [{read, not_written}]}),
            %% Well, we'd wanted to write a new private projection.  But an
            %% timeout/partition/bug prevented the write at C110.  Let's
            %% retry now, since we probably need to make a change.
            react_to_env_A20(0, S);
        {error, TO_or_part}=Else when TO_or_part == timeout;
                                      TO_or_part == partition ->
            ?REACT({c111, [{read, Else}]}),
            %% Drat.  Well, let's retry in a while.
            timer:sleep(100),
            react_to_env_C111(P_latest, P_latest2, Extra1, MyStorePid, S);
        Else ->
            Summ = machi_projection:make_summary(P_latest),
            io:format(user, "C111 error by ~w: ~w, ~w\n~p\n",
                      [MyName, Else, Summ, get(react)]),
            error_logger:error_msg("C111 error by ~w: ~w, ~w, ~w\n",
                                   [MyName, Else, Summ, get(react)]),
            exit({c111_failure, MyName, Else, Summ})
    end.

react_to_env_C120(P_latest, FinalProps, #ch_mgr{proj_history=H,
                                                sane_transitions=Xtns}=S) ->
    ?REACT(c120),
    H2   = add_and_trunc_history(P_latest, H, ?MAX_HISTORY_LENGTH),

    %% diversion_c120_verbose_goop(P_latest, S),
    ?REACT({c120, [{latest, machi_projection:make_summary(P_latest)}]}),
    S2 = set_proj(S#ch_mgr{proj_history=H2,
                           sane_transitions=Xtns + 1}, P_latest),
    S3 = case is_annotated(P_latest) of
             false ->
                 S2;
             {{_ConfEpoch, _ConfCSum}, ConfTime} ->
                 io:format(user, "\nCONFIRM debug C120 ~w was annotated ~w\n", [S#ch_mgr.name, P_latest#projection_v1.epoch_number]),
                 S2#ch_mgr{proj_unanimous=ConfTime}
         end,
    V = case file:read_file("/tmp/moomoo."++atom_to_list(S#ch_mgr.name)) of {ok,_} -> true; _ -> false end,
    if V -> io:format("C120: ~w: ~p\n", [S#ch_mgr.name, get(react)]); true -> ok end,
    {{now_using, FinalProps, P_latest#projection_v1.epoch_number}, S3}.

add_and_trunc_history(P_latest, H, MaxLength) ->
    Latest_U_R = {P_latest#projection_v1.upi, P_latest#projection_v1.repairing},
    H2 = if P_latest#projection_v1.epoch_number > 0 ->
                 queue:in(Latest_U_R, H);
            true ->
                 H
         end,
    case queue:len(H2) of
        X when X > MaxLength ->
            {_V, Hxx} = queue:out(H2),
            Hxx;
        _ ->
            H2
    end.

react_to_env_C200(Retries, P_latest, S) ->
    ?REACT(c200),
    try
        AuthorProxyPid = proxy_pid(P_latest#projection_v1.author_server, S),
        ?FLU_PC:kick_projection_reaction(AuthorProxyPid, [])
    catch _Type:_Err ->
            %% ?V("TODO: tell_author_yo is broken: ~p ~p\n",
            %%           [_Type, _Err]),
            ok
    end,
    react_to_env_C210(Retries, S).

react_to_env_C210(Retries, #ch_mgr{name=MyName, proj=Proj} = S) ->
    ?REACT(c210),
    sleep_ranked_order(250, 500, MyName, Proj#projection_v1.all_members),
    react_to_env_C220(Retries, S).

react_to_env_C220(Retries, S) ->
    ?REACT(c220),
    react_to_env_A20(Retries + 1, manage_last_down_list(S)).

react_to_env_C300(#projection_v1{epoch_number=_Epoch_newprop}=P_newprop,
                  #projection_v1{epoch_number=_Epoch_latest}=_P_latest, S) ->
    ?REACT(c300),

    react_to_env_C310(machi_projection:update_checksum(P_newprop), S).

react_to_env_C310(P_newprop, S) ->
    ?REACT(c310),
    Epoch = P_newprop#projection_v1.epoch_number,
    {WriteRes, S2} = cl_write_public_proj(Epoch, P_newprop, S),
    ?REACT({c310, ?LINE,
            [{newprop, machi_projection:make_summary(P_newprop)},
            {write_result, WriteRes}]}),
    react_to_env_A10(manage_last_down_list(S2)).

projection_transitions_are_sane(Ps, RelativeToServer) ->
    projection_transitions_are_sane(Ps, RelativeToServer, false).

-ifdef(TEST).
projection_transitions_are_sane_retrospective(Ps, RelativeToServer) ->
    projection_transitions_are_sane(Ps, RelativeToServer, true).
-endif. % TEST

projection_transitions_are_sane([], _RelativeToServer, _RetrospectiveP) ->
    true;
projection_transitions_are_sane([_], _RelativeToServer, _RetrospectiveP) ->
    true;
projection_transitions_are_sane([P1, P2|T], RelativeToServer, RetrospectiveP) ->
    case projection_transition_is_sane(P1, P2, RelativeToServer,
                                       RetrospectiveP) of
        true ->
            projection_transitions_are_sane([P2|T], RelativeToServer,
                                           RetrospectiveP);
        Else ->
            {Else, from, P1#projection_v1.epoch_number, to, P2#projection_v1.epoch_number}
    end.

-ifdef(TEST).
projection_transition_is_sane_retrospective(P1, P2, RelativeToServer) ->
    projection_transition_is_sane(P1, P2, RelativeToServer, true).
-endif. % TEST

projection_transition_is_sane(P1, P2, RelativeToServer) ->
    projection_transition_is_sane(P1, P2, RelativeToServer, false).

%% @doc Check if a projection transition is sane &amp; safe.
%%
%% NOTE: The return value convention is `true' for sane/safe and
%% `term() /= true' for any unsafe/insane value.

projection_transition_is_sane(P1, P2, RelativeToServer, RetrospectiveP) ->
    put(myname, RelativeToServer),
    put(why2, []),
    case projection_transition_is_sane_with_si_epoch(
           P1, P2, RelativeToServer, RetrospectiveP) of
        true ->
            projection_transition_is_sane_final_review(P1, P2,
                                                       ?RETURN2(true));
        {epoch_not_si,SameEpoch,not_gt,SameEpoch}=Reason ->
            if P1#projection_v1.upi == [],
               P2#projection_v1.upi == [] ->
                    %% None proj -> none proj is ok
                    ?RETURN2(true);
               true ->
                    ?RETURN2(Reason)
            end;
        Else ->
            ?RETURN2(Else)
    end.

projection_transition_is_sane_final_review(
  _P1, P2, {expected_author2,UPI1_tail,_}=Else) ->
    if UPI1_tail == P2#projection_v1.author_server ->
            ?RETURN2(true);
       true ->
            ?RETURN2({gazzuknkgazzuknk, Else, gazzuknk})
    end;
projection_transition_is_sane_final_review(
  #projection_v1{mode=CMode1}=_P1,
  #projection_v1{mode=CMode2}=_P2,
  _) when CMode1 /= CMode2 ->
    {wtf, cmode1, CMode1, cmode2, CMode2};
projection_transition_is_sane_final_review(
  #projection_v1{mode=cp_mode, upi=UPI1, dbg=P1_dbg}=_P1,
  #projection_v1{mode=cp_mode, upi=UPI2, witnesses=Witness_list}=_P2,
  true) ->
    %% All earlier sanity checks has said that this transition is sane, but
    %% we also need to make certain that any CP mode transition preserves at 
    %% least one non-witness server in the UPI list.  Earlier checks have
    %% verified that the ordering of the FLUs within the UPI list is ok.
    UPI1_s = ordsets:from_list(UPI1 -- Witness_list),
    UPI2_s = ordsets:from_list(UPI2 -- Witness_list),
    catch ?REACT({projection_transition_is_sane_final_review, ?LINE,
                  [{upi1,UPI1}, {upi2,UPI2}, {witnesses,Witness_list},
                   {zerf_backstop, proplists:get_value(zerf_backstop, P1_dbg)},
                   {upi1_s,UPI1}, {upi2_s,UPI2}]}),
    case proplists:get_value(zerf_backstop, P1_dbg) of
        true when UPI1 == [] ->
            %% CAUTION, this is a dangerous case.  If the old projection, P1,
            %% has a 'zerf_backstop' annotation, then when this function
            %% returns true, we are (in effect) saying, "We trust you."  What
            %% if we called make_zerf() a year ago because we took a 1 year
            %% nap??  How can we trust this?
            %%
            %% The answer is: this is not our last safety enforcement for CP
            %% mode, fortunately.  We are going from the none projection to a
            %% quorum majority projection, *and* we will not unwedge ourself
            %% until we can verify that all UPI members of the chain are
            %% unanimous for this epoch.  So if we took a 1 year nap already,
            %% or if we take a one year right now and delay writing our
            %% private projection for 1 year, then if we disagree with the
            %% quorum majority, we simply won't be able to unwedge.
            ?RETURN2(true);
        _ when UPI2 == [] ->
            %% We're down to the none projection to wedge ourself.  That's ok.
            ?RETURN2(true);
        _ ->
            ?RETURN2(not ordsets:is_disjoint(UPI1_s, UPI2_s))
    end;
projection_transition_is_sane_final_review(
  #projection_v1{mode=cp_mode, upi=UPI1, repairing=Repairing1}=_P1,
  #projection_v1{mode=cp_mode, upi=UPI2, repairing=Repairing2}=_P2,
  {epoch_not_si,EpochX,not_gt,EpochY}=Else) ->
    if EpochX == EpochY, UPI1 == UPI2, Repairing1 == Repairing2 ->
            %% TODO: So, technically speaking, I think, probably, that this is
            %% cheating.  But it's also safe cheating: the UPI & repairing
            %% lists are identical.  If we end up in this case, where the
            %% epoch isn't strictly increasing, I'm *guessing*, is because
            %% there's a flaw in the adoption of the null projection (as a
            %% safety fall-back) and not always (?) writing that none proj
            %% through the usual C300-and-reiterate cycle?  In terms of chain
            %% state transition, equal UPI and equal repairing is 100% safe.
            %% However, in acceptin this transition, we run a risk of creating
            %% an infinite-loop-via-always-same-epoch-number problem.
            ?RETURN2(true);
       true ->
            ?RETURN2(Else)
    end;
projection_transition_is_sane_final_review(_P1, _P2, Else) ->
    ?RETURN2(Else).

%% @doc Check if a projection transition is sane &amp; safe with a
%%      strictly increasing epoch number.
%%
%% NOTE: The return value convention is `true' for sane/safe and
%% `term() /= true' for any unsafe/insane value.

projection_transition_is_sane_with_si_epoch(
  #projection_v1{epoch_number=Epoch1} = P1,
  #projection_v1{epoch_number=Epoch2} = P2,
  RelativeToServer, RetrospectiveP) ->
    case projection_transition_is_sane_except_si_epoch(
           P1, P2, RelativeToServer, RetrospectiveP) of
        true ->
            %% Must be a strictly increasing epoch.
            case Epoch2 > Epoch1 of
                true ->
                    ?RETURN2(true);
                false ->
                    ?RETURN2({epoch_not_si, Epoch2, 'not_gt', Epoch1})
            end;
        Else ->
            ?RETURN2(Else)
    end.

%% @doc Check if a projection transition is sane &amp; safe with the
%%      exception of a strictly increasing epoch number (equality is ok).
%%
%% NOTE: The return value convention is `true' for sane/safe and
%% `term() /= true' for any unsafe/insane value.

projection_transition_is_sane_except_si_epoch(
  #projection_v1{epoch_number=Epoch1,
                 epoch_csum=CSum1,
                 creation_time=CreationTime1,
                 mode=CMode1,
                 author_server=AuthorServer1,
                 chain_name=ChainName1,
                 all_members=All_list1,
                 witnesses=Witness_list1,
                 down=Down_list1,
                 upi=UPI_list1,
                 repairing=Repairing_list1,
                 dbg=Dbg1} = P1,
  #projection_v1{epoch_number=Epoch2,
                 epoch_csum=CSum2,
                 creation_time=CreationTime2,
                 mode=CMode2,
                 author_server=AuthorServer2,
                 chain_name=ChainName2,
                 all_members=All_list2,
                 witnesses=Witness_list2,
                 down=Down_list2,
                 upi=UPI_list2,
                 repairing=Repairing_list2,
                 dbg=Dbg2} = P2,
  RelativeToServer, __TODO_RetrospectiveP) ->
 ?RETURN2(undefined),
 try
    %% General notes:
    %%
    %% I'm making no attempt to be "efficient" here.  All of these data
    %% structures are small, and the funcs aren't called zillions of times per
    %% second.

    CMode1 = CMode2,
    true = is_integer(Epoch1) andalso is_integer(Epoch2),
    true = is_binary(CSum1) andalso is_binary(CSum2),
    {_,_,_} = CreationTime1,
    {_,_,_} = CreationTime2,
    true = is_atom(AuthorServer1) andalso is_atom(AuthorServer2),
    true = is_atom(ChainName1) andalso is_atom(ChainName2),
    true = is_list(All_list1) andalso is_list(All_list2),
    true = is_list(Witness_list1) andalso is_list(Witness_list2),
    true = is_list(Down_list1) andalso is_list(Down_list2),
    true = is_list(UPI_list1) andalso is_list(UPI_list2),
    true = is_list(Repairing_list1) andalso is_list(Repairing_list2),
    true = is_list(Dbg1) andalso is_list(Dbg2),

    %% Don't check for strictly increasing epoch here: that's the job of
    %% projection_transition_is_sane_with_si_epoch().
    true = Epoch2 >= Epoch1,

    %% Don't change chain names in the middle of the stream.
    true = (ChainName1 == ChainName2),

    %% No duplicates
    true = lists:sort(Witness_list2) == lists:usort(Witness_list2),
    true = lists:sort(Down_list2) == lists:usort(Down_list2),
    true = lists:sort(UPI_list2) == lists:usort(UPI_list2),
    true = lists:sort(Repairing_list2) == lists:usort(Repairing_list2),

    %% Disjoint-ness
    %% %% %% %% %% %% %% %% All_list1 = All_list2,                 % todo will probably change
    %% true = lists:sort(All_list2) == lists:sort(Down_list2 ++ UPI_list2 ++
    %%                                                Repairing_list2),
    [] = [X || X <- Witness_list2, not lists:member(X, All_list2)],
    [] = [X || X <- Down_list2, not lists:member(X, All_list2)],
    [] = [X || X <- UPI_list2, not lists:member(X, All_list2)],
    [] = [X || X <- Repairing_list2, not lists:member(X, All_list2)],
    DownS2 = sets:from_list(Down_list2),
    UPIS2 = sets:from_list(UPI_list2),
    RepairingS2 = sets:from_list(Repairing_list2),
    true = sets:is_disjoint(DownS2, UPIS2),
    true = sets:is_disjoint(DownS2, RepairingS2),
    true = sets:is_disjoint(UPIS2, RepairingS2),

    %% We won't check the checksum of P1, but we will of P2.
    P2 = machi_projection:update_checksum(P2),

    %% CP mode extra sanity checks
    if CMode1 == cp_mode ->
            Majority = full_majority_size(All_list2),
            UPI2_and_Repairing2 = UPI_list2 ++ Repairing_list2,
            if length(UPI_list2) == 0 ->
                    ok;                         % none projection
               length(UPI2_and_Repairing2) >= Majority ->
                    %% We are assuming here that the client side is smart
                    %% enough to do the *safe* thing when the
                    %% length(UPI_list2) < Majority ... the client must use
                    %% the repairing nodes both as witnesses to check the
                    %% current epoch.
                    ok;
               true ->
                    error({majority_not_met, UPI_list2})
            end;
       CMode1 == ap_mode ->
            ok
    end,

    %% Hooray, all basic properties of the projection's elements are
    %% not obviously bad.  Now let's check if the UPI+Repairing->UPI
    %% transition is good.
    %%
    %% NOTE: chain_state_transition_is_sane() only cares about strong
    %% consistency violations and (because witness servers don't store
    %% any data) doesn't care about witness servers.  So we remove all
    %% witnesses from the UPI lists before calling
    %% chain_state_transition_is_sane()
    UPI_list1w = UPI_list1 -- Witness_list1,
    UPI_list2w = UPI_list2 -- Witness_list2,
    ?RETURN2(
       chain_state_transition_is_sane(AuthorServer1, UPI_list1w,Repairing_list1,
                                      AuthorServer2, UPI_list2w,
                                      RelativeToServer))
 catch
     _Type:_Err ->
         ?RETURN2(oops),
         S1 = machi_projection:make_summary(P1),
         S2 = machi_projection:make_summary(P2),
         Trace = erlang:get_stacktrace(),
         %% There are basic data structure checks only, do not return `false'
         %% here.
         {err, _Type, _Err, from, S1, to, S2, relative_to, RelativeToServer,
          react, get(react),
          stack, Trace}
 end.

poll_private_proj_is_upi_unanimous(#ch_mgr{proj_unanimous={_,_,_}} = S) ->
    S;
poll_private_proj_is_upi_unanimous(#ch_mgr{consistency_mode=ap_mode,
                                           proj=Proj} = S) ->
    if Proj#projection_v1.upi == [] % Nobody to poll?
       orelse
       Proj#projection_v1.epoch_number == 0 -> % Skip polling for epoch 0?
            S;
       true ->
            %% Try it just a single time, no other waiting
            poll_private_proj_is_upi_unanimous3(S)
    end;
poll_private_proj_is_upi_unanimous(#ch_mgr{consistency_mode=cp_mode,
                                           proj_unanimous=false,
                                           proj=Proj} = S) ->
    if Proj#projection_v1.upi == [] % Nobody to poll?
       orelse
       Proj#projection_v1.epoch_number == 0 -> % Skip polling for epoch 0?
            S;
       true ->
            poll_private_proj_is_upi_unanimous_sleep(0, S)
    end.

poll_private_proj_is_upi_unanimous_sleep(Count, S) when Count > 2 ->
    S;
poll_private_proj_is_upi_unanimous_sleep(Count, #ch_mgr{runenv=RunEnv}=S) ->
    Denom = case proplists:get_value(use_partition_simulator, RunEnv, false) of
                true ->
                    20;
                _ ->
                    1
            end,
    timer:sleep(((Count * Count) * 50) div Denom),
    case poll_private_proj_is_upi_unanimous3(S) of
        #ch_mgr{proj_unanimous=false} = S2 ->
            poll_private_proj_is_upi_unanimous_sleep(Count + 1, S2);
        S2 ->
            S2
    end.

poll_private_proj_is_upi_unanimous3(#ch_mgr{name=MyName, proj=P_current} = S) ->
    UPI = P_current#projection_v1.upi,
    EpochID = machi_projection:make_epoch_id(P_current),
    {Rs, S2} = read_latest_projection_call_only2(private, UPI, S),
    Rs2 = [if is_record(R, projection_v1) ->
                   machi_projection:make_epoch_id(R);
              true ->
                   R                            % probably {error, unwritten}
           end || R <- Rs],
    case lists:usort(Rs2) of
        [EID] when EID == EpochID ->
            %% We have a debugging problem, alas.  It would be really great
            %% if we could preserve the dbg2 info that's in the current
            %% projection that's on disk.  However, the full dbg2 list
            %% with 'react' trace data isn't in the #ch_mgr.proj copy of
            %% the projection.  So, go read it from the store.
            %%
            %% But of course there's another small problem.  P_current could
            %% be the result of make_zerf(), which helps us "fast forward" to
            %% a newer CP mode projection.  And so what we just read in the
            %% 'Rs' at the top of this function may be for a new epoch that
            %% we've never seen before and therefore doesn't exist in our
            %% local private projection store.  But if it came from
            %% make_zerf(), by definition it must be annotated, so don't try
            %% to proceed any further.
            ProxyPid = proxy_pid(MyName, S),
            Epoch = P_current#projection_v1.epoch_number,
            case ?FLU_PC:read_projection(ProxyPid, private, Epoch) of
                {ok, P_currentFull} ->
                    Now = os:timestamp(),
                    Annotation = make_annotation(EpochID, Now),
                    NewDbg2 = [Annotation|P_currentFull#projection_v1.dbg2],
                    NewProj = P_currentFull#projection_v1{dbg2=NewDbg2},
                    ProjStore = get_projection_store_pid_or_regname(S),
                    #projection_v1{epoch_number=_EpochRep,
                                   epoch_csum= <<_CSumRep:4/binary,_/binary>>,
                                   upi=_UPIRep,
                                   repairing=_RepairingRep} = NewProj,
                    ok = machi_projection_store:write(ProjStore, private, NewProj),
                    case proplists:get_value(private_write_verbose, S#ch_mgr.opts) of
                        true ->
                            io:format(user, "\n~s CONFIRM epoch ~w ~w upi ~w rep ~w by ~w\n", [machi_util:pretty_time(), _EpochRep, _CSumRep, _UPIRep, _RepairingRep, MyName]);
                        _ ->
                            ok
                    end,
                    %% Unwedge our FLU.
                    {ok, NotifyPid} = machi_projection_store:get_wedge_notify_pid(ProjStore),
                    _ = machi_flu1:update_wedge_state(NotifyPid, false, EpochID),
                    S2#ch_mgr{proj_unanimous=Now};
                _ ->
                    S2
            end;
        _Else ->
            S2
    end.

sleep_ranked_order(MinSleep, MaxSleep, FLU, FLU_list) ->
    USec = calc_sleep_ranked_order(MinSleep, MaxSleep, FLU, FLU_list),
    timer:sleep(USec),
    USec.

calc_sleep_ranked_order(MinSleep, MaxSleep, FLU, FLU_list) ->
    Front = lists:takewhile(fun(X) -> X /= FLU end,
                            lists:reverse(lists:sort(FLU_list))),
    Index = length(Front),
    NumNodes = length(FLU_list),
    SleepChunk = if NumNodes == 0 -> 0;
                    true          -> (MaxSleep - MinSleep) div NumNodes
                 end,
    MinSleep + (SleepChunk * Index).

proxy_pid(Name, #ch_mgr{proxies_dict=ProxiesDict}) ->
    orddict:fetch(Name, ProxiesDict).

make_chmgr_regname(A) when is_atom(A) ->
    list_to_atom(atom_to_list(A) ++ "_chmgr");
make_chmgr_regname(B) when is_binary(B) ->
    list_to_atom(binary_to_list(B) ++ "_chmgr").

gobble_calls(StaticCall) ->
    receive
        {'$gen_call',From,{trigger_react_to_env}} ->
            gen_server:reply(From, todo_overload),
            gobble_calls(StaticCall)
    after 1 ->                                  % after 0 angers pulse.
            ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

perhaps_start_repair(#ch_mgr{name=MyName,
                             consistency_mode=CMode,
                             repair_worker=undefined,
                             proj=P_current}=S) ->
    case P_current of
        #projection_v1{creation_time=Start,
                       upi=[_|_]=UPI,
                       repairing=[_|_]} ->
            RepairId = {MyName, os:timestamp()},
            RepairOpts = [{repair_mode,repair}, verbose, {repair_id,RepairId}],
            %% RepairOpts = [{repair_mode, check}, verbose],
            RepairFun = fun() -> do_repair(S, RepairOpts, CMode) end,
            LastUPI = lists:last(UPI),
            IgnoreStabilityTime_p = proplists:get_value(ignore_stability_time,
                                                        S#ch_mgr.opts, false),
            case timer:now_diff(os:timestamp(), Start) div 1000000 of
                N when MyName == LastUPI andalso
                       (IgnoreStabilityTime_p orelse
                        N >= ?REPAIR_START_STABILITY_TIME) ->
                    {WorkerPid, _Ref} = spawn_monitor(RepairFun),
                    S#ch_mgr{repair_worker=WorkerPid,
                             repair_start=os:timestamp(),
                             repair_final_status=undefined};
                _ ->
                    S
            end;
        _ ->
            S
    end;
perhaps_start_repair(S) ->
    S.

do_repair(#ch_mgr{name=MyName,
                  proj=#projection_v1{witnesses=Witness_list,
                                      upi=UPI0,
                                      repairing=[_|_]=Repairing,
                                      members_dict=MembersDict}}=S,
          Opts, RepairMode) ->
    ETS = ets:new(repair_stats, [private, set]),
    ETS_T_Keys = [t_in_files, t_in_chunks, t_in_bytes,
                  t_out_files, t_out_chunks, t_out_bytes,
                  t_bad_chunks, t_elapsed_seconds],
    [ets:insert(ETS, {K, 0}) || K <- ETS_T_Keys],

    {ok, MyProj} = ?FLU_PC:read_latest_projection(proxy_pid(MyName, S),
                                                  private),
    MyEpochID = machi_projection:get_epoch_id(MyProj),
    RepairEpochIDs = [case ?FLU_PC:read_latest_projection(proxy_pid(Rep, S),
                                                          private) of
                          {ok, Proj} ->
                              machi_projection:get_epoch_id(Proj);
                          _ ->
                              unknown
                      end || Rep <- Repairing],
    case lists:usort(RepairEpochIDs) of
        [MyEpochID] ->
            T1 = os:timestamp(),
            RepairId = proplists:get_value(repair_id, Opts, id1),
            error_logger:info_msg(
              "Repair ~w start: tail ~p of ~p -> ~p, ~p\n",
              [RepairId, MyName, UPI0, Repairing, RepairMode]),

            UPI = UPI0 -- Witness_list,
            Res = machi_chain_repair:repair(RepairMode, MyName, Repairing, UPI,
                                            MembersDict, ETS, Opts),
            T2 = os:timestamp(),
            Elapsed = (timer:now_diff(T2, T1) div 1000) / 1000,
            ets:insert(ETS, {t_elapsed_seconds, Elapsed}),
            Summary = case Res of ok -> "success";
                          _  -> "FAILURE"
                      end,
            Stats = [{K, ets:lookup_element(ETS, K, 2)} || K <- ETS_T_Keys],
            error_logger:info_msg(
              "Repair ~w ~s: tail ~p of ~p finished ~p: "
              "~p Stats: ~p\n",
              [RepairId, Summary, MyName, UPI0, RepairMode, Res, Stats]),
            ets:delete(ETS),
            exit({repair_final_status, Res});
        _ ->
            exit(not_all_in_same_epoch)
    end.

sanitize_repair_state(#ch_mgr{repair_final_status=Res,
                              proj=#projection_v1{upi=[_|_]}}=S)
  when Res /= undefined ->
    S#ch_mgr{repair_worker=undefined, repair_start=undefined,
             repair_final_status=undefined};
sanitize_repair_state(S) ->
    S.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

perhaps_call_t(#ch_mgr{name=MyName}=S, Partitions, FLU, DoIt) ->
    ProxyPid = proxy_pid(FLU, S),
    perhaps_call(ProxyPid, MyName, Partitions, FLU, DoIt).

perhaps_call(ProxyPid, MyName, Partitions, FLU, DoIt) ->
    try
        perhaps_call2(ProxyPid, MyName, Partitions, FLU, DoIt)
    catch
        exit:timeout ->
            update_remember_down_list(FLU),
            {error, partition};
        exit:{timeout,_} ->
            update_remember_down_list(FLU),
            {error, partition}
    end.

perhaps_call2(ProxyPid, MyName, Partitions, FLU, DoIt) ->
    RemoteFLU_p = FLU /= MyName,
    erase(bad_sock),
    case RemoteFLU_p andalso lists:member({MyName, FLU}, Partitions) of
        false ->
            Res = DoIt(ProxyPid),
            if Res == {error, partition} ->
                    update_remember_down_list(FLU);
               true ->
                    ok
            end,
            case RemoteFLU_p andalso lists:member({FLU, MyName}, Partitions) of
                false ->
                    Res;
                _ ->
                    (catch put(react, [{timeout2,me,MyName,to,FLU,RemoteFLU_p,Partitions}|get(react)])),
                    exit(timeout)
            end;
        true ->
            (catch put(react, [{timeout1,me,MyName,to,FLU,RemoteFLU_p,Partitions}|get(react)])),
            exit(timeout)
    end.

%% Why are we using the process dictionary for this?  In part because
%% we're lazy and in part because we don't want to clutter up the
%% return value of perhaps_call_t() in order to make perhaps_call_t()
%% a 100% pure function.

init_remember_down_list() ->
    put(remember_down_list, []).

update_remember_down_list(FLU) ->
    catch put(remember_down_list,
              lists:usort([FLU|get_remember_down_list()])).

get_remember_down_list() ->
    get(remember_down_list).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc A simple technique for checking chain state transition safety.
%%
%% Math tells us that any change state `UPI1' plus `Repair1' to state
%% `UPI2' is OK as long as `UPI2' is a concatenation of some
%% order-preserving combination from `UPI1' with some order-preserving
%% combination from `Repair1'.
%%
%% ```
%%   Good_UPI2s = [ X ++ Y || X <- machi_util:ordered_combinations(UPI1),
%%                            Y <- machi_util:ordered_combinations(Repair1)]'''
%%
%% Rather than creating that list and then checking if `UPI2' is in
%% it, we try a `diff'-like technique to check for basic state
%% transition safety.  See docs for {@link mk/3} for more detail.
%%
%% ```
%% 2> machi_chain_manager1:mk([a,b], [], [a]).
%% {[keep,del],[]}        %% good transition
%% 3> machi_chain_manager1:mk([a,b], [], [b,a]).
%% {[del,keep],[]}        %% bad transition: too few 'keep' for UPI2's length 2
%% 4> machi_chain_manager1:mk([a,b], [c,d,e], [a,d]).
%% {[keep,del],[2]}       %% good transition
%% 5> machi_chain_manager1:mk([a,b], [c,d,e], [a,bogus]).
%% {[keep,del],[error]}   %% bad transition: 'bogus' not in Repair1'''

simple_chain_state_transition_is_sane(UPI1, Repair1, UPI2) ->
    ?RETURN2(simple_chain_state_transition_is_sane(undefined, UPI1, Repair1,
                                                   undefined, UPI2)).

%% @doc Simple check if a projection transition is sane &amp; safe: we assume
%% that the caller has checked basic projection data structure contents.
%%
%% NOTE: The return value convention is `true' for sane/safe and
%% `term() /= true' for any unsafe/insane value.

simple_chain_state_transition_is_sane(_Author1, UPI1, Repair1, Author2, UPI2) ->
    {KeepsDels, Orders} = mk(UPI1, Repair1, UPI2),
    NumKeeps = length([x || keep <- KeepsDels]),
    NumOrders = length(Orders),
    NoErrorInOrders = (false == lists:member(error, Orders)),
    OrdersOK = (Orders == lists:sort(Orders)),
    UPI2LengthOK = (length(UPI2) == NumKeeps + NumOrders),
    Answer1 = NoErrorInOrders andalso OrdersOK andalso UPI2LengthOK,
    catch ?REACT({simple, ?LINE,
                  [{sane, answer1,Answer1,
                    author1,_Author1, upi1,UPI1, repair1,Repair1,
                    author2,Author2, upi2,UPI2,
                    keepsdels,KeepsDels, orders,Orders, numKeeps,NumKeeps,
                    numOrders,NumOrders, answer1,Answer1},
                   {why2, get(why2)}]}),
    if not Answer1 ->
            ?RETURN2(Answer1);
       true ->
            if Orders == [] ->
                    %% No repairing have joined UPI2. Keep original answer.
                    ?RETURN2(Answer1);
               Author2 == undefined ->
                    %% At least one Repairing1 element is now in UPI2.
                    %% We need Author2 to make better decision.  Go
                    %% with what we know, silly caller for not giving
                    %% us what we need.
                    ?RETURN2(Answer1);
               Author2 /= undefined ->
                    %% At least one Repairing1 element is now in UPI2.
                    %% We permit only the tail to author such a UPI2.
                    case catch(lists:last(UPI1)) of
                        UPI1_tail when UPI1_tail == Author2 ->
                            ?RETURN2(true);
                        UPI1_tail ->
                            ?RETURN2({expected_author2,UPI1_tail,
                                      [{upi1,UPI1},
                                       {repair1,Repair1},
                                       {author2,Author2},
                                       {upi2,UPI2}]})
                    end
            end
    end.

%% @doc Check if a projection transition is sane &amp; safe: we assume
%% that the caller has checked basic projection data structure contents.
%%
%% NOTE: The return value convention is `true' for sane/safe and `term() /=
%% true' for any unsafe/insane value.  This function (and its callee
%% functions) are the only functions (throughout all of the chain state
%% transition sanity checking functions) that is allowed to return `false'.

chain_state_transition_is_sane(Author1, UPI1, Repair1, Author2, UPI2,
                               RelativeToServer) ->
    ToSelfOnly_p = if UPI2 == [Author2] -> true;
                      true              -> false
                   end,
    Disjoint_UPIs = ordsets:is_disjoint(ordsets:from_list(UPI1),
                                        ordsets:from_list(UPI2)),
    %% This if statement contains the only exceptions that we make to
    %% the judgement of simple_chain_state_transition_is_sane().
    if ToSelfOnly_p ->
            %% The transition is to UPI2=[Author2].
            %% For AP mode, this transition is always safe (though not
            %% always optimal for highest availability).
            ?RETURN2(true);
       Disjoint_UPIs ->
            %% The transition from UPI1 -> UPI2 where the two are
            %% disjoint/no FLUs in common.
            %% For AP mode, this transition is safe (though not
            %% always optimal for highest availability) if I am not
            %% a member of the new UPI.
            ?RETURN2(not lists:member(RelativeToServer, UPI2));
       true ->
            ?RETURN2(
               simple_chain_state_transition_is_sane(Author1, UPI1, Repair1,
                                                     Author2, UPI2))
    end.

%% @doc Create a 2-tuple that describes how `UPI1' + `Repair1' are
%%      transformed into `UPI2' in a chain state change.
%%
%% The 1st part of the 2-tuple is a list of `keep' and `del' instructions,
%% relative to the items in UPI1 and whether they are present (`keep') or
%% absent (`del') in `UPI2'.
%%
%% The 2nd part of the 2-tuple is `list(non_neg_integer()|error)' that
%% describes the relative order of items in `Repair1' that appear in
%% `UPI2'.  The `error' atom is used to denote items not present in
%% `Repair1'.

mk(UPI1, Repair1, UPI2) ->
    mk(UPI1, Repair1, UPI2, []).

mk([X|UPI1], Repair1, [X|UPI2], Acc) ->
    mk(UPI1, Repair1, UPI2, [keep|Acc]);
mk([X|UPI1], Repair1, UPI2, Acc) ->
    mk(UPI1, Repair1, UPI2 -- [X], [del|Acc]);
mk([], [], [], Acc) ->
    {lists:reverse(Acc), []};
mk([], Repair1, UPI2, Acc) ->
    {lists:reverse(Acc), machi_util:mk_order(UPI2, Repair1)}.

scan_dir(Dir, FileFilterFun, FoldEachFun, FoldEachAcc) ->
    Files = filelib:wildcard(Dir ++ "/*"),
    Xs = [binary_to_term(element(2, file:read_file(File))) || File <- Files],
    Xs2 = FileFilterFun(Xs),
    lists:foldl(FoldEachFun, FoldEachAcc, Xs2).

get_ps(#projection_v1{epoch_number=Epoch, dbg=Dbg}, Acc) ->
    [{Epoch, proplists:get_value(ps, Dbg, [])}|Acc].

strip_dbg2(P) ->
    P#projection_v1{dbg2=[stripped]}.

has_not_sane(#projection_v1{epoch_number=Epoch, dbg2=Dbg2}, Acc) ->
    Reacts = proplists:get_value(react, Dbg2, []),
    case [X || {_State,_Line, [not_sane|_]}=X <- Reacts] of
        [] ->
            Acc;
        Xs->
            [{Epoch, Xs}|Acc]
    end.

full_majority_size(N) when is_integer(N) ->
    (N div 2) + 1;
full_majority_size(L) when is_list(L) ->
    full_majority_size(length(L)).

make_zerf(#projection_v1{epoch_number=OldEpochNum,
                         chain_name=ChainName,
                         all_members=AllMembers,
                         members_dict=MembersDict,
                         witnesses=OldWitness_list
                        } = _LastProj,
          #ch_mgr{name=MyName,
                  consistency_mode=cp_mode,
                  runenv=RunEnv1} = S) ->
    {Up, _Partitions, _RunEnv2} = calc_up_nodes(MyName,
                                                AllMembers, RunEnv1),
    (catch put(yyy_hack, [{up,Up}|get(yyy_hack)])),
    MajoritySize = full_majority_size(AllMembers),
    case length(Up) >= MajoritySize of
        false ->
            %% Make it appear like nobody is up now: we'll have to
            %% wait until the Up list changes so that
            %% zerf_find_last_common() can confirm a common stable
            %% last stable epoch.

            P = make_none_projection(OldEpochNum,
                                     MyName, AllMembers, OldWitness_list,
                                     MembersDict),
            machi_projection:update_checksum(
              P#projection_v1{chain_name=ChainName,
                              mode=cp_mode,
                              dbg2=[zerf_none,{up,Up},{maj,MajoritySize}]});
        true ->
            make_zerf2(OldEpochNum, Up, MajoritySize, MyName,
                       AllMembers, OldWitness_list, MembersDict, S)
    end.

make_zerf2(OldEpochNum, Up, MajoritySize, MyName, AllMembers, OldWitness_list,
           MembersDict, S) ->
    try
        #projection_v1{epoch_number=Epoch} = Proj =
            zerf_find_last_common(MajoritySize, Up, S),
        Proj2 = Proj#projection_v1{dbg2=[{make_zerf,Epoch},
                                         {yyy_hack, get(yyy_hack)},
                                         {up,Up},{maj,MajoritySize}]},
        Proj2
    catch
        throw:{zerf,no_common} ->
            %% Epoch 0 special case: make the "all" projection.
            %% calc_projection2() will then filter out any FLUs that
            %% aren't currently up to create the first chain.  If not
            %% enough are up now, then it will fail to create a first
            %% chain.
            %%
            %% If epoch 0 isn't the only epoch that we've looked at,
            %% but we still couldn't find a common projection, then
            %% we still need to default to the "all" projection and let
            %% subsequent chain calculations do their calculations....
            P = make_all_projection(MyName, AllMembers, OldWitness_list,
                                    MembersDict),
            P2 = machi_projection:update_checksum(
                   P#projection_v1{epoch_number=OldEpochNum,
                                   mode=cp_mode, dbg2=[zerf_all]}),
            P2;
        _X:_Y ->
            throw({zerf, {damn_exception, Up, _X, _Y, erlang:get_stacktrace()}})
    end.

zerf_find_last_common(MajoritySize, Up, S) ->
    case lists:reverse(
           lists:sort(
             lists:flatten(
               [zerf_find_last_annotated(FLU,MajoritySize,S) || FLU <- Up]))) of
        [] ->
            throw({zerf,no_common});
        [P|_]=_TheList ->
            %% TODO is this simple sort really good enough?
            P
    end.

zerf_find_last_annotated(FLU, MajoritySize, S) ->
    Proxy = proxy_pid(FLU, S),
    {ok, Epochs} = ?FLU_PC:list_all_projections(Proxy, private, 60*1000),
    P = lists:foldl(
          fun(_Epoch, #projection_v1{}=Proj) ->
                  Proj;
             (Epoch, Acc) ->
                  {ok, Proj} = ?FLU_PC:read_projection(Proxy, private,
                                                       Epoch, ?TO*10),
                  case is_annotated(Proj) of
                      false ->
                          (catch put(yyy_hack, [{FLU, Epoch, not_annotated}|get(yyy_hack)])),
                          Acc;
                      {{ConfEpoch, ConfCSum}, _ConfTime} ->
                          Px = if ConfEpoch == Epoch ->
                                       (catch put(yyy_hack, [{FLU, Epoch, ok1}|get(yyy_hack)])),
                                       Proj;
                                  true ->
                                       %% Sanity checking
                                       ConfEpoch = Proj#projection_v1.epoch_number,
                                       ConfCSum  = Proj#projection_v1.epoch_csum,
                                       (catch put(yyy_hack, [{FLU, Epoch, ok2}|get(yyy_hack)])),
                                       Proj
                               end,
                          UPI_and_Repairing = Px#projection_v1.upi ++
                                              Px#projection_v1.repairing,
                          if length(UPI_and_Repairing) >= MajoritySize ->
                                  (catch put(yyy_hack, [{FLU, Epoch, yay}|get(yyy_hack)])),
                                  Px;
                             true ->
                                  (catch put(yyy_hack, [{FLU, Epoch, skip}|get(yyy_hack)])),
                                  Acc
                          end
                  end
          end, first_accumulator, lists:reverse(Epochs)),
    if is_record(P, projection_v1) ->
            P;
       true ->
            []                                  % lists:flatten() will destroy
    end.

perhaps_verbose_c111(P_latest2, S) ->
    case proplists:get_value(private_write_verbose, S#ch_mgr.opts) of
        true ->
            Dbg2X = lists:keydelete(react, 1,
                                    P_latest2#projection_v1.dbg2) ++
                [{is_annotated,is_annotated(P_latest2)}],
            P_latest2x = P_latest2#projection_v1{dbg2=Dbg2X}, % limit verbose len.
            Last2 = get(last_verbose),
            Summ2 = machi_projection:make_summary(P_latest2x),
            if P_latest2#projection_v1.upi == [],
               (S#ch_mgr.proj)#projection_v1.upi /= [] ->
                    <<CSumRep:4/binary,_/binary>> =
                                          P_latest2#projection_v1.epoch_csum,
                    io:format(user, "~s CONFIRM epoch ~w ~w upi ~w rep ~w by ~w\n", [machi_util:pretty_time(), (S#ch_mgr.proj)#projection_v1.epoch_number, CSumRep, P_latest2#projection_v1.upi, P_latest2#projection_v1.repairing, S#ch_mgr.name]);
               true ->
                    ok
            end,
            case proplists:get_value(private_write_verbose,
                                     S#ch_mgr.opts) of
            %% case true of
                true when Summ2 /= Last2 ->
                    put(last_verbose, Summ2),
                    ?V("\n~s ~p uses plain: ~w \n",
                       [machi_util:pretty_time(), S#ch_mgr.name, Summ2]);
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

set_proj(S, Proj) ->
    S#ch_mgr{proj=Proj, proj_unanimous=false}.

make_annotation(EpochID, Time) ->
    {private_proj_is_upi_unanimous, {EpochID, Time}}.

is_annotated(#projection_v1{dbg2=Dbg2}) ->
    proplists:get_value(private_proj_is_upi_unanimous, Dbg2, false).

make_basic_comparison_stable(P) ->
    P#projection_v1{creation_time=undefined,
                    dbg=[],
                    dbg2=[],
                    members_dict=[]}.

has_make_zerf_annotation(P) ->
    case proplists:get_value(make_zerf, P#projection_v1.dbg2) of
        Z_epoch when Z_epoch == P#projection_v1.epoch_number ->
            true;
        _ ->
            false
    end.

get_unfit_list(FitnessServer) ->
    try
        machi_fitness:get_unfit_list(FitnessServer)
    catch exit:{noproc,_} ->
            %% We are probably operating in an eunit test that hasn't used
            %% the machi_flu_psup supervisor for startup.
            []
    end.

get_projection_store_pid_or_regname(#ch_mgr{name=MyName, opts=MgrOpts}) ->
    case get_projection_store_regname(MgrOpts) of
        undefined ->
            machi_flu_psup:make_proj_supname(MyName);
        PStr ->
            PStr
    end.
