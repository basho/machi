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

-define(NOT_FLAPPING_START, {{epk,-1},?NOT_FLAPPING}).

-record(ch_mgr, {
          name            :: pv1_server(),
          flap_limit      :: non_neg_integer(),
          proj            :: projection(),
          proj_unanimous  :: 'false' | erlang:timestamp(),
          %%
          timer           :: 'undefined' | timer:tref(),
          ignore_timer    :: boolean(),
          proj_history    :: queue:queue(),
          flap_count=0    :: non_neg_integer(), % I am flapping if > 0.
          flap_start=?NOT_FLAPPING_START
                          :: {{'epk', integer()}, erlang:timestamp()},
          flap_last_up=[] :: list(),
          flap_last_up_change=now() :: erlang:timestamp(),
          flap_counts_last=[] :: list(),
          not_sanes       :: orddict:orddict(),
          sane_transitions = 0 :: non_neg_integer(),
          consistency_mode:: 'ap_mode' | 'cp_mode',
          repair_worker   :: 'undefined' | pid(),
          repair_start    :: 'undefined' | erlang:timestamp(),
          repair_final_status :: 'undefined' | term(),
          runenv          :: list(), %proplist()
          opts            :: list(),  %proplist()
          members_dict    :: p_srvr_dict(),
          proxies_dict    :: orddict:orddict()
         }).

-define(FLU_PC, machi_proxy_flu1_client).
-define(TO, (2*1000)).                          % default timeout

%% Keep a history of our flowchart execution in the process dictionary.
-define(REACT(T), put(react, [T|get(react)])).

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

%% Minimum guideline for considering a remote to be flapping
-define(MINIMUM_ALL_FLAP_LIMIT, 10).

%% API
-export([start_link/2, start_link/3, stop/1, ping/1,
         set_chain_members/2, set_chain_members/3, set_active/2,
         trigger_react_to_env/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([make_chmgr_regname/1, projection_transitions_are_sane/2,
         inner_projection_exists/1, inner_projection_or_self/1,
         simple_chain_state_transition_is_sane/3,
         simple_chain_state_transition_is_sane/5,
         chain_state_transition_is_sane/5]).
%% Exports so that EDoc docs generated for these internal funcs.
-export([mk/3]).

%% Exports for developer/debugging
-export([scan_dir/4, strip_dbg2/1,
         get_ps/2, has_not_sane/2, all_hosed_history/2]).

-ifdef(TEST).

-export([test_calc_projection/2,
         test_write_public_projection/2,
         test_read_latest_public_projection/2,
         get_all_hosed/1]).

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
    set_chain_members(Pid, MembersDict, []).

set_chain_members(Pid, MembersDict, Witness_list) ->
    case lists:all(fun(Witness) -> orddict:is_key(Witness, MembersDict) end,
                   Witness_list) of
        true ->
            Cmd = {set_chain_members, MembersDict, Witness_list},
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
    random:seed(now()),
    init_remember_partition_hack(),
    Opt = fun(Key, Default) -> proplists:get_value(Key, MgrOpts, Default) end,
    InitWitness_list = Opt(witnesses, []),
    ZeroAll_list = [P#p_srvr.name || {_,P} <- orddict:to_list(InitMembersDict)],
    ZeroProj = make_none_projection(MyName, ZeroAll_list,
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
    Proj1 = make_none_projection(MyName, All_list, Witness_list, MembersDict),
    Proj = machi_projection:update_checksum(
             Proj1#projection_v1{epoch_number=CurrentEpoch}),

    RunEnv = [{seed, Opt(seed, now())},
              {use_partition_simulator, Opt(use_partition_simulator, false)},
              {simulate_repair, Opt(simulate_repair, true)},
              {network_partitions, Opt(network_partitions, [])},
              {network_islands, Opt(network_islands, [])},
              {up_nodes, Opt(up_nodes, not_init_yet)}],
    ActiveP = Opt(active_mode, true),
    S = set_proj(#ch_mgr{name=MyName,
                %% TODO 2015-03-04: revisit, should this constant be bigger?
                %% Yes, this should be bigger, but it's a hack.  There is
                %% no guarantee that all parties will advance to a minimum
                %% flap awareness in the amount of time that this mgr will.
                flap_limit=length(All_list) + 3,
                timer='undefined',
                proj_history=queue:new(),
                not_sanes=orddict:new(),
                consistency_mode=CMode,
                runenv=RunEnv,
                opts=MgrOpts}, Proj),
    {_, S2} = do_set_chain_members_dict(MembersDict, S),
    S3 = if ActiveP == false ->
                 S2;
            ActiveP == true ->
                 set_active_timer(S2)
         end,
    {ok, S3}.

handle_call({ping}, _From, S) ->
    {reply, pong, S};
handle_call({set_chain_members, MembersDict, Witness_list}, _From,
            #ch_mgr{name=MyName,
                    proj=#projection_v1{all_members=OldAll_list,
                                        epoch_number=OldEpoch,
                                        upi=OldUPI}=OldProj}=S) ->
    {Reply, S2} = do_set_chain_members_dict(MembersDict, S),
    %% TODO: should there be any additional sanity checks?  Right now,
    %% if someone does something bad, then do_react_to_env() will
    %% crash, which will crash us, and we'll restart in a sane & old
    %% config.
    All_list = [P#p_srvr.name || {_, P} <- orddict:to_list(MembersDict)],
    MissingInNew = OldAll_list -- All_list,
    NewUPI = OldUPI -- MissingInNew,
    NewDown = All_list -- NewUPI,
    NewEpoch = OldEpoch + ?SET_CHAIN_MEMBERS_EPOCH_SKIP,
    CMode = calc_consistency_mode(Witness_list),
    ok = set_consistency_mode(machi_flu_psup:make_proj_supname(MyName), CMode),
    NewProj = machi_projection:update_checksum(
                OldProj#projection_v1{author_server=MyName,
                                      creation_time=now(),
                                      mode=CMode,
                                      epoch_number=NewEpoch,
                                      all_members=All_list,
                                      witnesses=Witness_list,
                                      upi=NewUPI,
                                      repairing=[],
                                      down=NewDown,
                                      members_dict=MembersDict}),
    %% Reset all flapping state.
    NewProj2 = NewProj#projection_v1{flap=make_flapping_i()},
    S3 = clear_flapping_state(set_proj(S2#ch_mgr{proj_history=queue:new()},
                                       NewProj2)),
    {_QQ, S4} = do_react_to_env(S3),
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
    {TODOtodo, S2} = do_react_to_env(S),
    {reply, TODOtodo, S2};
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
handle_info(Msg, S) ->
    case get(todo_bummer) of undefined -> io:format("TODO: got ~p\n", [Msg]);
                             _         -> ok
    end,
    put(todo_bummer, true),
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_none_projection(MyName, All_list, Witness_list, MembersDict) ->
    Down_list = All_list,
    UPI_list = [],
    P = machi_projection:new(MyName, MembersDict, Down_list, UPI_list, [], []),
    CMode = if Witness_list == [] ->
                    ap_mode;
               Witness_list /= [] ->
                    cp_mode
            end,
    machi_projection:update_checksum(P#projection_v1{mode=CMode,
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

cl_write_public_proj_skip_local_error(Epoch, Proj, S) ->
    cl_write_public_proj(Epoch, Proj, true, S).

cl_write_public_proj(Epoch, Proj, SkipLocalWriteErrorP, S) ->
    %% Write to local public projection store first, and if it succeeds,
    %% then write to all remote public projection stores.
    cl_write_public_proj_local(Epoch, Proj, SkipLocalWriteErrorP, S).

cl_write_public_proj_local(Epoch, Proj, SkipLocalWriteErrorP,
                           #ch_mgr{name=MyName}=S) ->
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    Res0 = perhaps_call_t(
             S, Partitions, MyName,
             fun(Pid) -> ?FLU_PC:write_projection(Pid, public, Proj, ?TO) end),
    Continue = fun() ->
                  FLUs = Proj#projection_v1.all_members -- [MyName],
                  cl_write_public_proj_remote(FLUs, Partitions, Epoch, Proj, S)
               end,
    case Res0 of
        ok ->
            {XX, SS} = Continue(),
            {{local_write_result, ok, XX}, SS};
        Else when SkipLocalWriteErrorP ->
            {XX, SS} = Continue(),
            {{local_write_result, Else, XX}, SS};
        Else ->
            {Else, S2}
    end.

cl_write_public_proj_remote(FLUs, Partitions, _Epoch, Proj, S) ->
    %% We're going to be very care-free about this write because we'll rely
    %% on the read side to do any read repair.
    DoIt = fun(Pid) -> ?FLU_PC:write_projection(Pid, public, Proj, ?TO) end,
    Rs = [{FLU, perhaps_call_t(S, Partitions, FLU, fun(Pid) -> DoIt(Pid) end)} ||
             FLU <- FLUs],
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
    All_queried_list = All_list -- AllHosed,

    {Rs, S2} = read_latest_projection_call_only2(ProjectionType,
                                                 All_queried_list, S),
    FLUsRs = lists:zip(All_queried_list, Rs),
    {All_queried_list, FLUsRs, S2}.

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

read_projection_call_only2(ProjectionType, Epoch, All_queried_list, S) ->
    {_UpNodes, Partitions, S2} = calc_up_nodes(S),
    DoIt = fun(Pid) ->
                   case (?FLU_PC:read_projection(Pid, ProjectionType, Epoch, ?TO)) of
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

    if All_queried_list == []
       orelse
       length(UnwrittenRs) == length(FLUsRs) ->
            Witness_list = CurrentProj#projection_v1.witnesses,
            NoneProj = make_none_projection(MyName, [], Witness_list,
                                            orddict:new()),
            Extra2 = [{all_members_replied, true},
                      {all_queried_list, All_queried_list},
                      {flus_rs, FLUsRs},
                      {unanimous_flus,[]},
                      {not_unanimous_flus, []},
                      {bad_answer_flus, BadAnswerFLUs},
                      {not_unanimous_answers, []},
                      {trans_all_hosed, []},
                      {trans_all_flap_counts, []}],
            {not_unanimous, NoneProj, Extra2, S};
       ProjectionType == public, UnwrittenRs /= [] ->
            {needs_repair, FLUsRs, [flarfus], S};
       true ->
            [{_Rank, BestProj}|_] = rank_and_sort_projections(Ps, CurrentProj),
            NotBestPs = [Proj || Proj <- Ps, Proj /= BestProj],
            UnanimousTag = if NotBestPs == [] -> unanimous;
                              true            -> not_unanimous
                           end,
            Extra = [{all_members_replied, length(FLUsRs) == length(All_queried_list)}],
            Best_FLUs = [FLU || {FLU, Projx} <- FLUsRs, Projx == BestProj],
            TransAllHosed = lists:usort(
                              lists:flatten([get_all_hosed(P) || P <- Ps])),
            AllFlapCounts = merge_flap_counts([get_all_flap_counts(P) ||
                                                  P <- Ps]),
            Extra2 = [{all_queried_list, All_queried_list},
                      {flus_rs, FLUsRs},
                      {unanimous_flus,Best_FLUs},
                      {not_unanimous_flus, All_queried_list --
                                                 (Best_FLUs ++ BadAnswerFLUs)},
                      {bad_answer_flus, BadAnswerFLUs},
                      {not_unanimous_answers, NotBestPs},
                      {trans_all_hosed, TransAllHosed},
                      {trans_all_flap_counts, AllFlapCounts}|Extra],
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
            {_DontCare, _S2}=Res = cl_write_public_proj_skip_local_error(
                                                           Epoch, BestProj, S),
            Res
    end.

calc_projection(S, RelativeToServer) ->
    calc_projection(S, RelativeToServer, []).

calc_projection(#ch_mgr{proj=LastProj, consistency_mode=CMode} = S,
                RelativeToServer, AllHosed) ->
    Dbg = [],
    %% OldThreshold = proplists:get_value(old_threshold, RunEnv),
    %% NoPartitionThreshold = proplists:get_value(no_partition_threshold, RunEnv),
    if CMode == ap_mode ->
            calc_projection2(LastProj, RelativeToServer, AllHosed, Dbg, S);
       CMode == cp_mode ->
            #projection_v1{epoch_number=OldEpochNum,
                           all_members=AllMembers,
                           upi=OldUPI_list
                          } = LastProj,
            UPI_length_ok_p =
                length(OldUPI_list) >= full_majority_size(AllMembers),
            case {OldEpochNum, UPI_length_ok_p} of
                {0, _} ->
                    calc_projection2(LastProj, RelativeToServer, AllHosed,
                                     Dbg, S);
                {_, true} ->
                    calc_projection2(LastProj, RelativeToServer, AllHosed,
                                     Dbg, S);
                {_, false} ->
                    case make_zerf(LastProj, S) of
                        Zerf when is_record(Zerf, projection_v1) ->
                            ?REACT({calc,?LINE,
                                    [{zerf_backstop, true},
                                     {zerf_in, machi_projection:make_summary(Zerf)}]}),
                            %% io:format(user, "zerf_in: ~p: ~w\n", [S#ch_mgr.name,  machi_projection:make_summary(Zerf)]),
                            calc_projection2(Zerf, RelativeToServer, AllHosed,
                                             [{zerf_backstop, true}]++Dbg, S);
                        Zerf ->
                            {{{yo_todo_incomplete_fix_me_cp_mode, OldEpochNum, OldUPI_list, Zerf}}}
                    end
            end
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
                   members_dict=MembersDict,
                   witnesses=OldWitness_list,
                   upi=OldUPI_list,
                   repairing=OldRepairing_list
                  } = LastProj,
    LastUp = lists:usort(OldUPI_list ++ OldRepairing_list),
    AllMembers = (S#ch_mgr.proj)#projection_v1.all_members,
    {Up0, Partitions, RunEnv2} = calc_up_nodes(MyName,
                                               AllMembers, RunEnv1),
    Up = Up0 -- AllHosed,

    NewUp = Up -- LastUp,
    Down = AllMembers -- Up,
    ?REACT({calc,?LINE,[{old_upi, OldUPI_list},
                        {old_repairing,OldRepairing_list},
                        {last_up, LastUp}, {up0, Up0}, {all_hosed, AllHosed},
                        {up, Up}, {new_up, NewUp}, {down, Down}]}),

    NewUPI_list =
        [X || X <- OldUPI_list, lists:member(X, Up) andalso
                                not lists:member(X, OldWitness_list)],
    %% If we are not flapping (AllHosed /= [], which is a good enough proxy),
    %% then we do our repair checks based on the inner projection only.  There
    %% is no value in doing repairs during flapping.
    RepChk_Proj =  if AllHosed == [] ->
                           CurrentProj;
                      true ->
                           inner_projection_or_self(CurrentProj)
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

    P = machi_projection:new(OldEpochNum + 1,
                             MyName, MembersDict, Down, NewUPI, NewRepairing,
                             D_foo ++
                                 Dbg ++ [{ps, Partitions},{nodes_up, Up}]),
    P2 = if CMode == cp_mode ->
                 UpWitnesses = [W || W <- Up, lists:member(W, OldWitness_list)],
                 Majority = full_majority_size(AllMembers),
                 SoFar = length(NewUPI),
                 if SoFar >= Majority ->
                         ?REACT({calc,?LINE,[]}),
                         P;
                    true ->
                         Need = Majority - SoFar,
                         UpWitnesses = [W || W <- Up,
                                             lists:member(W, OldWitness_list)],
                         if length(UpWitnesses) >= Need ->
                                 Ws = lists:sublist(UpWitnesses, Need),
                                 ?REACT({calc,?LINE,[{ws, Ws}]}),
                                 machi_projection:update_checksum(
                                   P#projection_v1{upi=Ws++NewUPI});
                            true ->
                                 ?REACT({calc,?LINE,[]}),
                                 P_none0 = make_none_projection(
                                            MyName, AllMembers, OldWitness_list,
                                            MembersDict),
                                 Why = if NewUPI == [] ->
                                               no_real_servers;
                                          true ->
                                               not_enough_witnesses
                                       end,
                                 P_none1 = P_none0#projection_v1{
                                             epoch_number=OldEpochNum + 1,
                                             dbg=[{none_projection,true},
                                                  {up0, Up0},
                                                  {up, Up},
                                                  {all_hosed, AllHosed},
                                                  {oldepoch, OldEpochNum},
                                                  {oldupi, OldUPI_list},
                                                  {newupi, NewUPI_list},
                                                  {newupi3, NewUPI_list3},
                                                  {tent_upi, TentativeUPI},
                                                  {new_upi, NewUPI},
                                                  {up_witnesses, UpWitnesses},
                                                  {why_none, Why}]},
                                 machi_projection:update_checksum(P_none1)
                         end
                 end;
            CMode == ap_mode ->
                 ?REACT({calc,?LINE,[]}),
                 P
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
                                             epoch_csum=RemoteCSum} =
                                  inner_projection_or_self(RPJ),
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
    case proplists:get_value(use_partition_simulator, RunEnv1) of
        true ->
            calc_up_nodes_sim(MyName, AllMembers, RunEnv1);
        false ->
            {AllMembers -- get(remember_partition_hack), [], RunEnv1}
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
    {UpNodes, Partitions2, RunEnv2}.

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
    {ok, S#ch_mgr{members_dict=MembersDict,
                  proxies_dict=ProxiesDict}}.

do_react_to_env(#ch_mgr{name=MyName,
                        proj=#projection_v1{epoch_number=Epoch,
                                            members_dict=[]=OldDict}=OldProj,
                        opts=Opts}=S) ->
    %% Read from our local *public* projection store.  If some other
    %% chain member has written something there, and if we are a
    %% member of that chain, then we'll adopt that projection and then
    %% start actively humming in that chain.
    {NewMembersDict, NewProj} =
        get_my_public_proj_boot_info(Opts, OldDict, OldProj),
    case orddict:is_key(MyName, NewMembersDict) of
        false ->
            {{empty_members_dict, [], Epoch}, S};
        true ->
            {_, S2} = do_set_chain_members_dict(NewMembersDict, S),
            CMode = calc_consistency_mode(NewProj#projection_v1.witnesses),
            {{empty_members_dict, [], Epoch},
             set_proj(S2#ch_mgr{members_dict=NewMembersDict,
                                consistency_mode=CMode}, NewProj)}
    end;
do_react_to_env(S) ->
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
        %% When in CP mode, we call the poll function twice: once before
        %% reacting & once after.  This call is the 2nd.
        {Res, S3} = react_to_env_A10(S2),
        {Res, poll_private_proj_is_upi_unanimous(S3)}
    catch
        throw:{zerf,_}=_Throw ->
            Proj = S#ch_mgr.proj,
io:format(user, "zerf ~p caught ~p\n", [S#ch_mgr.name, _Throw]),
            {{no_change, [], Proj#projection_v1.epoch_number}, S}
    end.

react_to_env_A10(S) ->
    ?REACT(a10),
    react_to_env_A20(0, poll_private_proj_is_upi_unanimous(S)).

react_to_env_A20(Retries, #ch_mgr{name=MyName}=S) ->
    ?REACT(a20),
    init_remember_partition_hack(),
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
            error_logger:info_msg("Chain manager ~p found latest public "
                                  "projection ~p has author ~p has a "
                                  "members list ~p that does not include me.\n",
                                  [S#ch_mgr.name,
                                   P_latest#projection_v1.epoch_number,
                                   P_latest#projection_v1.author_server,
                                   P_latest#projection_v1.all_members]);
        _ ->
            ok
    end,

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
    ?REACT({a20,?LINE,[{upi_repairing,UPI_Repairing_FLUs},
                       {unanimous_flus,UnanimousFLUs},
                       {all_upi_repairing_were_unanimous,All_UPI_Repairing_were_unanimous},
                       {not_unanimous_flus, NotUnanimousFLUs},
                       {not_unanimous_answers, NotUnanimousSumms},
                       {bad_answer_flus, BadAnswerFLUs}
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
    react_to_env_A29(Retries, P_latest, LatestUnanimousP, ReadExtra, S2).

react_to_env_A29(Retries, P_latest, LatestUnanimousP, ReadExtra,
                 #ch_mgr{name=MyName, consistency_mode=CMode,
                         proj=P_current} = S) ->
    #projection_v1{epoch_number=Epoch_latest,
                   author_server=Author_latest} = P_latest,
    if CMode == cp_mode,
       Epoch_latest > P_current#projection_v1.epoch_number,
       Author_latest /= MyName ->
            put(yyy_hack, []),
            case make_zerf(P_current, S) of
                Zerf when is_record(Zerf, projection_v1) ->
                    ?REACT({a29, ?LINE,
                            [{zerf_filler, true},
                             {zerf_in, machi_projection:make_summary(Zerf)}]}),
                    io:format(user, "zerf_in: A29: ~p: ~w\n\t~p\n", [MyName,  machi_projection:make_summary(Zerf), get(yyy_hack)]),
                    P_current2 = Zerf#projection_v1{
                                             flap=P_current#projection_v1.flap},
                    S2 = set_proj(S, P_current2),
                    react_to_env_A30(Retries, P_latest, LatestUnanimousP,
                                     ReadExtra, S2);
                Zerf ->
                    {{{yo_todo_incomplete_fix_me_cp_mode, line, ?LINE, Zerf}}}
            end;
       true ->
            react_to_env_A30(Retries, P_latest, LatestUnanimousP, ReadExtra, S)
    end.

react_to_env_A30(Retries, P_latest, LatestUnanimousP, _ReadExtra,
                 #ch_mgr{name=MyName, proj=P_current,
                         consistency_mode=CMode, flap_limit=FlapLimit} = S) ->
    ?REACT(a30),
    %% case length(get(react)) of XX when XX > 500 -> io:format(user, "A30 ~w! ~w: ~P\n", [MyName, XX, get(react), 300]), timer:sleep(500); _ -> ok end,
    {P_newprop1, S2, Up} = calc_projection(S, MyName),
    ?REACT({a30, ?LINE, [{current, machi_projection:make_summary(S#ch_mgr.proj)}]}),
    ?REACT({a30, ?LINE, [{newprop1, machi_projection:make_summary(P_newprop1)}]}),
    ?REACT({a30, ?LINE, [{latest, machi_projection:make_summary(P_latest)}]}),

    %% Are we flapping yet?
    {P_newprop2, S3} = calculate_flaps(P_newprop1, P_latest, P_current, Up,
                                       FlapLimit, S2),

    %% Move the epoch number up ... originally done in C300.
    #projection_v1{epoch_number=Epoch_newprop2}=P_newprop2,
    #projection_v1{epoch_number=Epoch_latest,
                   author_server=Author_latest}=P_latest,
    NewEpoch = erlang:max(Epoch_newprop2, Epoch_latest) + 1,
    P_newprop3 = P_newprop2#projection_v1{epoch_number=NewEpoch},
    ?REACT({a30, ?LINE, [{newprop3, machi_projection:make_summary(P_newprop3)}]}),

    {P_newprop10, S10} =
        case get_flap_count(P_newprop3) of
            {_,P_newprop3_flap_count} when P_newprop3_flap_count >= FlapLimit ->
                %% I'm flapping.  Perhaps make an inner projection?
                ?REACT({a30, ?LINE,[{newprop3_flap_count,P_newprop3_flap_count},
                                    {flap_limit, FlapLimit}]}),
                AFPs = get_all_flap_counts(P_newprop3),
                case lists:sort([FlapCount ||
                                    {_FLU, {_EpkTime, FlapCount}} <- AFPs]) of
                    [SmallestFC|_] when SmallestFC > ?MINIMUM_ALL_FLAP_LIMIT ->
                        a30_make_inner_projection(
                          P_current, P_newprop3, P_latest, Up, S3);
                    _ ->
                        %% Not everyone is flapping enough.  Or perhaps
                        %% everyone was but we finally saw some new server X
                        %% where X's flap count isn't big enough.
                        {P_newprop3, S3}
                end;
            {_, P_newprop3_flap_count} ->
                ?REACT({a30, ?LINE,[{newprop3_flap_count,P_newprop3_flap_count},
                                    {flap_limit, FlapLimit}]}),
                {P_newprop3, S3}
        end,
    ?REACT({a30, ?LINE, [{newprop10, machi_projection:make_summary(P_newprop10)}]}),

    %% Here's a more common reason for moving from inner projection to
    %% a normal projection: the old proj has an inner but the newprop
    %% does not.
    MoveFromInnerToNorm_p =
        case {inner_projection_exists(P_current),
              inner_projection_exists(P_newprop10)} of
            {true, false} -> true;
            {_, _}        -> false
        end,

    %% If P_current says that we believe that we're currently flapping,
    %% and if P_newprop10 says that we're no longer flapping, then we
    %% really ought to stop flapping, right.
    %%
    %% Not quite so simple....
    %%
    %% AAAAH, right.  The case I'm dealing with right now is an asymmetric
    %% partition in a 4 member chain that affects all_hosed=[a,b,c] but
    %% member D is *NOT* noticing anything different in the current scheme:
    %% {inner_projection_exists(current), inner_projection_exists(new)}
    %% is {true, true}.
    %% Yes, that hypothesis is confirmed by time-honored io:format() tracing.
    %%
    %% So, we need something to kick a silly member like 'd' out of its
    %% rut of am-still-flapping.  So, let's try this:
    %%   If we see a P_latest from author != MyName, and if P_latest's 
    %%   author's flap count is now 0 (latest!), but that same member's
    %%   flap count in P_current is non-zero, then we assume that author
    %%   has moved out of flapping state and that therefore we ought to do
    %%   the same.

    %% Remember! P_current is this manager's private in-use projection.
    %% It is always less than or equal to P_latest's epoch!
    Current_flap_counts = get_all_flap_counts(P_current),
    Latest_authors_flap_count_current = proplists:get_value(
                                         Author_latest, Current_flap_counts),
    Latest_flap_counts = get_all_flap_counts(P_latest),
    Latest_authors_flap_count_latest = proplists:get_value(
                                         Author_latest, Latest_flap_counts),
    Kicker_p = case {Latest_authors_flap_count_current,
                     Latest_authors_flap_count_latest} of
                   {NotUndef, undefined} when NotUndef /= undefined ->
                       %% OK, someone else has switched from non-zero flap
                       %% count to zero flap count.  But ... do not kick out
                       %% of our flapping mode locally if we do not have an
                       %% inner projection.
                       inner_projection_exists(P_current);
                   {_, _} ->
                       false
               end,

    ClauseInfo = [{inner_kicker, Kicker_p},
                  {inner_kicker2, {Latest_authors_flap_count_current,
                                   Latest_authors_flap_count_latest}},
                  {move_from_inner, MoveFromInnerToNorm_p}],
    ?REACT({a30, ?LINE, ClauseInfo}),
    MoveToNorm_p = MoveFromInnerToNorm_p orelse Kicker_p,
    if MoveToNorm_p, CMode == cp_mode ->
            %% Too much weird stuff may have hapened while we were suffering
            %% the flapping/asymmetric partition.  Fall back to the none
            %% projection as if we're restarting.
            ?REACT({a30, ?LINE, [{move_to_norm, MoveToNorm_p}]}),
            react_to_env_A49(P_latest, [], S10);
       MoveToNorm_p, CMode == ap_mode ->
            %% Move from inner projection to outer.
            P_inner2A = inner_projection_or_self(P_current),
            ResetEpoch = P_newprop10#projection_v1.epoch_number,
            ResetAuthor = case P_current#projection_v1.upi of
                              [] ->
                                  %% Drat, fall back to current's author.
                                  P_current#projection_v1.author_server;
                              _ ->
                                  lists:last(P_current#projection_v1.upi)
                          end,
            ClauseInfo2 = [{move_from_inner_to_outer, true},
                           {old_author, P_inner2A#projection_v1.author_server},
                           {reset_author, ResetAuthor},
                           {reset_epoch, ResetEpoch}],
            P_inner2B =
                machi_projection:update_checksum(
                  P_inner2A#projection_v1{epoch_number=ResetEpoch,
                                          author_server=ResetAuthor,
                                          dbg=ClauseInfo++ClauseInfo2}),
            ReactI = [{inner2b,machi_projection:make_summary(P_inner2B)}],
            ?REACT({a30, ?LINE, ReactI}),
            %% In the past, we've tried:
            %%     react_to_env_C100(P_inner2B, P_latest, S);
            %%
            %% But we *know* that direct transition is racy/buggy: if
            %% P_latest UPIs are not unanimous, then we run the risk of
            %% non-disjoint UPIs; state B10 exists for a reason!
            %%
            %% So, we're going to use P_inner2B as our new proposal and run
            %% it through the regular system, as we did prior to 2015-04-14.
            %%
            %% OK, but we need to avoid a possible infinite loop by trying to
            %% use the inner projection as-is.  Because we're moving from
            %% inner to outer projections, the partition situation has
            %% altered significantly.  Use calc_projection() to find out what
            %% nodes are down *now* (as best as we can tell right now).
            {P_o, S_o, _Up2} = calc_projection2(P_inner2B, MyName, [], [], S10),
            %% NOTE: We are intentionally clearing flap info by not
            %% carrying it forwarding in the new projection.
            react_to_env_A40(Retries, P_o, P_latest, LatestUnanimousP, S_o);
       true ->
            ?REACT({a30, ?LINE, []}),
            react_to_env_A40(Retries, P_newprop10, P_latest,
                             LatestUnanimousP, S10)
    end.

a30_make_inner_projection(P_current, P_newprop3, P_latest, Up,
                          #ch_mgr{name=MyName, consistency_mode=CMode} = S) ->
    AllHosed = get_all_hosed(P_newprop3),
    P_current_has_inner_p = inner_projection_exists(P_current),
    P_current_ios = inner_projection_or_self(P_current),
    {P_i1, S_i, _Up} = calc_projection2(P_current_ios,
                                        MyName, AllHosed, [], S),
    ?REACT({a30, ?LINE, [{raw_all_hosed,get_all_hosed(P_newprop3)},
                         {up, Up},
                         {all_hosed, AllHosed},
                         {p_c_i, machi_projection:make_summary(P_current_ios)},
                         {p_i1, machi_projection:make_summary(P_i1)}]}),
    %% The inner projection will have a fake author, which
    %% everyone will agree is the largest UPI member's
    %% name.
    BiggestUPIMember =
        if P_i1#projection_v1.upi == [] ->
                %% Oops, ok, fall back to author
                P_i1#projection_v1.author_server;
           true ->
                lists:last(lists:sort(P_i1#projection_v1.upi))
        end,
    P_i2 = P_i1#projection_v1{author_server=BiggestUPIMember},
    P_i3 = case lists:member(MyName, AllHosed)
               andalso
               CMode == ap_mode
           of
               false ->
                   P_i2;
               true ->
                   %% Fall back to a safe AP mode chain: just me.
                   P_i2#projection_v1{
                     upi=[MyName],
                     repairing=[],
                     down=P_i2#projection_v1.all_members
                     -- [MyName]}
           end,
    HasCompatibleInner =
        case inner_projection_exists(P_latest) of
            true ->
                P_latest_i = inner_projection_or_self(P_latest),
                #projection_v1{epoch_number=___Epoch_current_x,
                               upi=UPI_current_x,
                               repairing=Repairing_current_x} = P_current_ios,
                #projection_v1{epoch_number=Epoch_latest_i,
                               upi=UPI_latest_i,
                               repairing=Repairing_latest_i} = P_latest_i,
                ?REACT({a30, ?LINE, [{epoch_latest_i, Epoch_latest_i},
                                     {upi_latest_i, UPI_latest_i},
                                     {repairing_latest_i,Repairing_latest_i}]}),
                LatestSameEnough_p =
                    (UPI_latest_i ++ Repairing_latest_i) ==
                        (UPI_current_x ++ Repairing_current_x)
                    andalso
                    Epoch_latest_i >= P_current_ios#projection_v1.epoch_number,
                CurrentHasInner_and_LatestIsDisjoint_p =
                    P_current_has_inner_p
                    andalso
                    ordsets:is_disjoint(
                      ordsets:from_list(UPI_current_x ++ Repairing_current_x),
                      ordsets:from_list(UPI_latest_i ++ Repairing_latest_i)),
                if LatestSameEnough_p ->
                        ?REACT({a30, ?LINE, []}),
                        case P_current_has_inner_p andalso
                            (UPI_current_x /= P_i3#projection_v1.upi orelse
                            Repairing_current_x /= P_i3#projection_v1.repairing)
                        of
                            true ->
                                %% Current proj is inner *and* our new
                                %% proposed inner proj differs substantially
                                %% from the current.  Don't use latest or
                                %% current.
                                false;
                            false ->
                                P_latest_i
                            end;
                   CurrentHasInner_and_LatestIsDisjoint_p ->
                        ?REACT({a30, ?LINE, []}),
                        P_current_ios;
                    true ->
                        ?REACT({a30, ?LINE, []}),
                        false
                end;
            false ->
                #projection_v1{upi=UPI_i3,
                               repairing=Repairing_i3} = P_i3,
                if P_current_has_inner_p,
                   UPI_i3 == P_current_ios#projection_v1.upi,
                   Repairing_i3 == P_current_ios#projection_v1.repairing ->
                        ?REACT({a30, ?LINE, []}),
                        P_current_ios;
                    true ->
                        ?REACT({a30, ?LINE, []}),
                        false
                end
        end,
    if HasCompatibleInner /= false ->
            ?REACT({a30, ?LINE,
                    [{inner_summary,
                      machi_projection:make_summary(HasCompatibleInner)}]}),
            P_newprop4 = machi_projection:update_checksum(
                           P_newprop3#projection_v1{inner=HasCompatibleInner}),
            {P_newprop4, S_i};
       true ->
            FinalInnerEpoch =
                case inner_projection_exists(P_current) of
                    false ->
                        ?REACT({a30xyzxyz, ?LINE, [P_newprop3#projection_v1.epoch_number]}),
                        FinalCreation = P_newprop3#projection_v1.creation_time,
                        P_newprop3#projection_v1.epoch_number;
                    true ->
                        P_oldinner = inner_projection_or_self(P_current),
                        ?REACT({a30xyzxyz, ?LINE, [P_oldinner#projection_v1.epoch_number + 1]}),
                        FinalCreation = P_newprop3#projection_v1.creation_time,
                        P_oldinner#projection_v1.epoch_number + 1
                end,
            %% TODO: When we implement the real chain repair function, we
            %%       need to keep in mind that an inner projection with
            %%       up nodes > 1, repair is required there!  In the
            %%       current simulator, repair is not simulated and
            %%       finished (and then growing the UPI list).  Fix.
            P_i4 = machi_projection:update_checksum(
                     P_i3#projection_v1{epoch_number=FinalInnerEpoch,
                                        creation_time=FinalCreation}),
            ?REACT({a30, ?LINE, [{inner_summary,
                                  machi_projection:make_summary(P_i4)}]}),
            %% Put it all together.
            P_newprop4 = machi_projection:update_checksum(
                           P_newprop3#projection_v1{inner=P_i4}),
            {P_newprop4, S_i}
    end.

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
                 #ch_mgr{name=MyName, proj=P_current}=S) ->
    ?REACT(a40),
    [{Rank_newprop, _}] = rank_projections([P_newprop], P_current),
    [{Rank_latest, _}] = rank_projections([P_latest], P_current),
    LatestAuthorDownP = a40_latest_author_down(P_latest, P_newprop, S)
                        andalso
                        P_latest#projection_v1.author_server /= MyName,
    ?REACT({a40, ?LINE,
            [{latest_author, P_latest#projection_v1.author_server},
             {author_is_down_p, LatestAuthorDownP},
             {latest_flap, P_latest#projection_v1.flap},
             {newprop_flap, P_newprop#projection_v1.flap}]}),

    if
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

            react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                             Rank_newprop, Rank_latest, S);

        Rank_newprop > 0
        andalso
        (P_latest#projection_v1.epoch_number < P_current#projection_v1.epoch_number
         orelse
         P_latest /= P_current) ->
            ?REACT({a40, ?LINE,
                    [{latest_epoch, P_latest#projection_v1.epoch_number},
                     {current_epoch, P_current#projection_v1.epoch_number},
                     {neq, P_latest /= P_current}]}),

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

            %% TODO: I believe that membership in the
            %% P_newprop#projection_v1.down is not sufficient for long
            %% chains.  Rather, we ought to be using a full broadcast
            %% gossip of server up status.
            %%
            %% Imagine 5 servers in an "Olympic Rings" style
            %% overlapping network paritition, where ring1 = upper
            %% leftmost and ring5 = upper rightmost.  It's both
            %% possible and desirable for ring5's projection to be
            %% seen (public) by ring1.  Ring5's projection's rank is
            %% definitely higher than ring1's proposed projection's
            %% rank ... but we're in a crazy netsplit where:
            %%  * if we accept ring5's proj: only one functioning chain
            %%    ([ring4,ring5] but stable
            %%  * if we accept ring1's proj: two functioning chains
            %%    ([ring1,ring2] and [ring4,ring5] indepependently)
            %%    but unstable: we're probably going to flap back & forth?!
            react_to_env_C300(P_newprop, P_latest, S);

        true ->
            ?REACT({a40, ?LINE, [true]}),
            GoTo50_p =
                case inner_projection_exists(P_current) andalso
                     inner_projection_exists(P_newprop) andalso
                     inner_projection_exists(P_latest) of
                    true ->
                        %% All three projections are flapping ... do we have a
                        %% new projection (probably due to repair) that is
                        %% worth suggesting via C300?
                        #projection_v1{epoch_number=Epoch_currenti} =
                            inner_projection_or_self(P_current),
                        #projection_v1{epoch_number=Epoch_newpropi} =
                            inner_projection_or_self(P_newprop),
                        ?REACT({a40, ?LINE, [{epoch_currenti,Epoch_currenti},
                                             {epoch_newpropi,Epoch_newpropi}]}),
                        if Epoch_currenti > Epoch_newpropi ->
                                %% Inner has a newer epoch, don't go to A50.
                                ?REACT({a40, ?LINE, []}),
                                false;
                           true ->
                                ?REACT({a40, ?LINE, []}),
                                true
                        end;
                    false ->
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

react_to_env_A49(_P_latest, FinalProps, #ch_mgr{name=MyName,
                                                proj=P_current} = S) ->
    ?REACT(a49),
    #projection_v1{all_members=All_list,
                   witnesses=Witness_list,
                   members_dict=MembersDict} = P_current,
    P_none = make_none_projection(MyName, All_list, Witness_list,
                                  MembersDict),
io:format(user, "Debug A49: ~w forced to none\n\n    ~P", [MyName, get(react), 120]),
    react_to_env_A50(P_none, FinalProps, set_proj(S, P_none)).

react_to_env_A50(P_latest, FinalProps, #ch_mgr{proj=P_current}=S) ->
    ?REACT(a50),
    ?REACT({a50, ?LINE, [{current_epoch, P_current#projection_v1.epoch_number},
                         {latest_epoch, P_latest#projection_v1.epoch_number},
                         {final_props, FinalProps}]}),
    %% if S#ch_mgr.name == b; S#ch_mgr.name == c -> io:format(user, "A50: ~p: ~p\n", [S#ch_mgr.name, get(react)]); true -> ok end,
%% io:format(user, "Debug A50: ~w P_current outer ~w ~w ~w\n", [S#ch_mgr.name, P_current#projection_v1.epoch_number,P_current#projection_v1.upi,P_current#projection_v1.repairing]),
    {{no_change, FinalProps, P_current#projection_v1.epoch_number}, S}.

react_to_env_B10(Retries, P_newprop, P_latest, LatestUnanimousP,
                 Rank_newprop, Rank_latest,
                 #ch_mgr{name=MyName, consistency_mode=CMode,
                         flap_limit=FlapLimit, proj=P_current}=S)->
    ?REACT(b10),

    {P_newprop_flap_time, P_newprop_flap_count} = get_flap_count(P_newprop),
    ?REACT({b10, ?LINE, [{newprop_epoch, P_newprop#projection_v1.epoch_number},
                         {newprop_flap_time, P_newprop_flap_time},
                         {newprop_flap_count, P_newprop_flap_count}]}),
    UnanimousLatestInnerNotRelevant_p =
        case inner_projection_exists(P_latest) of
            true when P_latest#projection_v1.author_server /= MyName ->
                #projection_v1{down=Down_inner,
                        epoch_number=EpochLatest_i} = inner_projection_or_self(
                                                         P_latest),
                case lists:member(MyName, Down_inner) of
                    true ->
                        %% Some foreign author's inner projection thinks that
                        %% I'm down.  Silly!  We ought to ignore this one.
                        ?REACT({b10, ?LINE, [{down_inner, Down_inner}]}),
                        true;
                    false ->
                        #projection_v1{epoch_number=Epoch_current} =
                            inner_projection_or_self(P_current),
                        Latest_GTE_Epoch_p = EpochLatest_i >= Epoch_current,
                        ?REACT({b10, ?LINE, [{down_inner, Down_inner},
                                      {latest_GTE_epoch, Latest_GTE_Epoch_p}]}),
                        not Latest_GTE_Epoch_p
                end;
            _Else_u ->
                false
        end,
    #flap_i{all_hosed=P_newprop_AllHosed,
            my_unique_prop_count=MyUniquePropCount} =
        case P_newprop#projection_v1.flap of undefined -> make_flapping_i();
                                             Flap      -> Flap
        end,
    P_newprop_AllHosedPlus =
        lists:flatten([[X,Y] || {X,problem_with,Y} <- P_newprop_AllHosed]),
    %% Commit 66cafe06 added UnanimousLatestInnerNotRelevant_p to the
    %% compound predicate below.  I'm yanking it out now. TODO re-study?
    #projection_v1{upi=P_newprop_upi_ooi, repairing=P_newprop_repairing_ooi} =
        inner_projection_or_self(P_newprop),
    EnoughAreFlapping_and_IamBad_p =
        %% Ignore inner_projection_exists(P_current): We might need to
        %% shut up quickly (adopting a new P_current can take a long
        %% time).
        (inner_projection_exists(P_latest) orelse
         inner_projection_exists(P_newprop)) andalso
        %% I have been flapping for a while
        S#ch_mgr.flap_count > 100 andalso
        %% I'm suspected of being bad
        lists:member(MyName, P_newprop_AllHosedPlus) andalso
        %% I'm not in the critical UPI or repairing lists
        (not lists:member(MyName, P_newprop_upi_ooi++P_newprop_repairing_ooi))
        andalso
        %% My down lists are the same, i.e., no state change to announce
        P_current#projection_v1.down == P_newprop#projection_v1.down,
    ?REACT({b10, ?LINE, [{0,EnoughAreFlapping_and_IamBad_p},
                         {1,inner_projection_exists(P_current)},
                         {2,inner_projection_exists(P_latest)},
                         {3,inner_projection_exists(P_newprop)},
                         {4,MyUniquePropCount},
                         {5,{MyName, P_newprop_AllHosedPlus}},
                         {6,UnanimousLatestInnerNotRelevant_p}]}),
    if
        EnoughAreFlapping_and_IamBad_p ->
            ?REACT({b10, ?LINE, []}),
            %% There's outer flapping happening *and* we ourselves are
            %% definitely flapping (flapping manifesto, starting clause 1)
            %% ... and also we are a member of the all_hosed club.  So, we
            %% should shut up and let someone else do the proposing.
            FinalProps = [{muting_myself, true},
                          {all_hosed, P_newprop_AllHosed}],
%% io:format(user, "B10: ~w shut up, latest e=~w/Inner=~w, ", [MyName, P_latest#projection_v1.epoch_number, (inner_projection_or_self(P_latest))#projection_v1.epoch_number]),
            if CMode == ap_mode ->
                    react_to_env_A50(P_latest, FinalProps, S);
               CMode == cp_mode ->
                    %% Be more harsh, stop iterating by A49 so that when we
                    %% resume we will have a much small opinion about the
                    %% world.
                    %% WHOOPS, doesn't allow convergence in simple cases,
                    %% needs more work!!!!!!!!!!!!!!!! Monday evening!!!!
                    %% react_to_env_A49(P_latest, FinalProps, S)
                    react_to_env_A50(P_latest, FinalProps, S)
            end;

        LatestUnanimousP
        andalso
        UnanimousLatestInnerNotRelevant_p ->
            ?REACT({b10, ?LINE, []}),

            %% Do not go to C100, because we want to ignore this latest
            %% proposal.  Write ours instead via C300.
            react_to_env_C300(P_newprop, P_latest, S);

        LatestUnanimousP ->
            ?REACT({b10, ?LINE,
                    [{latest_unanimous_p, LatestUnanimousP},
                     {latest_epoch,P_latest#projection_v1.epoch_number},
                     {latest_author,P_latest#projection_v1.author_server},
                     {newprop_epoch,P_newprop#projection_v1.epoch_number},
                     {newprop_author,P_newprop#projection_v1.author_server}
                    ]}),

            react_to_env_C100(P_newprop, P_latest, S);

        P_newprop_flap_count >= FlapLimit ->
            %% I am flapping ... what else do I do?
            ?REACT({b10, ?LINE, [i_am_flapping,
                                 {newprop_flap_count, P_newprop_flap_count},
                                 {flap_limit, FlapLimit}]}),
            case proplists:get_value(private_write_verbose, S#ch_mgr.opts) of
                true ->
                    ok; %% ?V("{FLAP: ~w flaps ~w}!  ", [S#ch_mgr.name, P_newprop_flap_count]);
                _ ->
                    ok
            end,
            %% MEANWHILE, we have learned some things about this
            %% algorithm in the past many months.  With the introduction
            %% of the "inner projection" concept, we know that the inner
            %% projection may be stable but the "outer" projection will
            %% continue to be flappy for as long as there's an
            %% asymmetric network partition somewhere.  We now know that
            %% that flappiness is OK and that the only problem with it
            %% is that it needs to be slowed down so that we don't have
            %% zillions of public projection proposals written every
            %% second.
            %%
            %% It doesn't matter if the FlapLimit count mechanism
            %% doesn't give an accurate sense of global flapping state.
            %% FlapLimit is enough to be able to tell us to slow down.

            %% We already know that I'm flapping.  We need to
            %% signal to the rest of the world that I'm writing
            %% and flapping and churning, so we cannot always
            %% go to A50 from here.
            %%
            %% If we do go to A50, then recommend that we poll less
            %% frequently.
            {X, S2} = gimme_random_uniform(100, S),
            if X < 80 ->
                    ?REACT({b10, ?LINE, [flap_stop]}),
                    ThrottleTime = if P_newprop_flap_count <  500 -> 1;
                                      P_newprop_flap_count < 1000 -> 5;
                                      P_newprop_flap_count < 5000 -> 10;
                                      true                        -> 30
                                   end,
                    FinalProps = [{my_flap_limit, FlapLimit},
                                  {throttle_seconds, ThrottleTime}],
                    react_to_env_A50(P_latest, FinalProps, S2);
               true ->
                    %% It is our moral imperative to write so that
                    %% the flap cycle continues enough times so that
                    %% everyone notices then eventually falls into
                    %% consensus.
                    react_to_env_C300(P_newprop, P_latest, S2)
            end;

        Retries > 2 ->
            ?REACT({b10, ?LINE, [{retries, Retries}]}),

            %% The author of P_latest is too slow or crashed.
            %% Let's try to write P_newprop and see what happens!
            react_to_env_C300(P_newprop, P_latest, S);

        Rank_latest >= Rank_newprop
        andalso
        P_latest#projection_v1.author_server /= MyName ->
            ?REACT({b10, ?LINE,
                    [{rank_latest, Rank_latest},
                     {rank_newprop, Rank_newprop},
                     {latest_author, P_latest#projection_v1.author_server}]}),

            %% TODO: Is a UnanimousLatestInnerNotRelevant_p test needed in this clause???

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

react_to_env_C100(P_newprop, #projection_v1{author_server=Author_latest,
                                            flap=Flap_latest0}=P_latest,
                  #ch_mgr{name=MyName, proj=P_current,
                          not_sanes=NotSanesDict0}=S) ->
    ?REACT(c100),

    Sane = projection_transition_is_sane(P_current, P_latest, MyName),
    QQ_current = lists:flatten(io_lib:format("~w:~w,~w/~w:~w,~w", [P_current#projection_v1.epoch_number, P_current#projection_v1.upi, P_current#projection_v1.repairing, (inner_projection_or_self(P_current))#projection_v1.epoch_number, (inner_projection_or_self(P_current))#projection_v1.upi, (inner_projection_or_self(P_current))#projection_v1.repairing])),
    QQ_latest = lists:flatten(io_lib:format("~w:~w,~w/~w:~w,~w", [P_latest#projection_v1.epoch_number, P_latest#projection_v1.upi, P_latest#projection_v1.repairing, (inner_projection_or_self(P_latest))#projection_v1.epoch_number, (inner_projection_or_self(P_latest))#projection_v1.upi, (inner_projection_or_self(P_latest))#projection_v1.repairing])),
    if Sane == true -> ok;  true -> ?V("\n~w-insane-~w-auth=~w ~s -> ~s ~w\n    ~p\n", [?LINE, MyName, P_newprop#projection_v1.author_server, QQ_current, QQ_latest, Sane, get(react)]) end,
    Flap_latest = if is_record(Flap_latest0, flap_i) ->
                          Flap_latest0;
                     true ->
                          not_a_flap_i_record
                  end,
    ?REACT({c100, ?LINE, [zoo, {me,MyName}, {author_latest,Author_latest},
                          {flap_latest,Flap_latest}]}),

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
            if Sane == true -> ok;  true -> ?V("~w-insane-~w-~w:~w:~w,", [?LINE, MyName, P_newprop#projection_v1.epoch_number, P_newprop#projection_v1.upi, P_newprop#projection_v1.repairing]) end, %%% DELME!!!
            react_to_env_C110(P_latest, S);
        true ->
            ?REACT({c100, ?LINE, [sane]}),
            if Sane == true -> ok;  true -> ?V("~w-insane-~w-~w:~w:~w@~w,", [?LINE, MyName, P_newprop#projection_v1.epoch_number, P_newprop#projection_v1.upi, P_newprop#projection_v1.repairing, ?LINE]) end, %%% DELME!!!
            react_to_env_C110(P_latest, S);
        DoctorSays ->
            ?REACT({c100, ?LINE, [{not_sane, DoctorSays}]}),
            %% This is a fun case.  We had just enough asymmetric partition
            %% to cause the chain to fragment into two *incompatible* and
            %% *overlapping membership* chains, but the chain fragmentation
            %% happened "quickly" enough so that by the time everyone's flap
            %% counters hit the flap_limit, the asymmetric partition has
            %% disappeared ... we'd be stuck in a flapping state forever (or
            %% until the partition situation changes again, which might be a
            %% very long time).
            %%
            %% Alas, this case took a long time to find in model checking
            %% zillions of asymmetric partitions.  Our solution is a bit
            %% harsh: we fall back to the "none projection" and let the chain
            %% reassemble from there.  Hopefully this case is quite rare,
            %% since asymmetric partitions (we assume) are pretty rare?
            %%
            %% Examples of overlapping membership insanity (at same instant):
            %% Key: {author, suggested UPI, suggested Reparing}
            %%
            %%     {a,[a,b],[c,d,e]},
            %%     {b,[a,b],[c,d,e]},
            %%     {c,[e,b],[a,c,d]},
            %%     {d,[a,b],[c,d,e]},
            %%     {e,[e,b],[a,c,d]},
            %% OR
            %%     [{a,[c,e],[a,b,d]},
            %%      {b,[e,a,b,c,d],[]},
            %%      {c,[c,e],[a,b,d]},
            %%      {d,[c,e],[a,b,d]},
            %%      {e,[c,e],[a,b,d]}]
            %%
            %% So, I'd tried this kind of "if everyone is doing it, then we
            %% 'agree' and we can do something different" strategy before,
            %% and it didn't work then.  Silly me.  Distributed systems
            %% lesson #823: do not forget the past.  In a situation created
            %% by PULSE, of all=[a,b,c,d,e], b & d & e were scheduled
            %% completely unfairly.  So a & c were the only authors ever to
            %% suceessfully write a suggested projection to a public store.
            %% Oops.
            %%
            %% So, we're going to keep track in #ch_mgr state for the number
            %% of times that this insane judgement has happened.
            %%
            %% See also: comment in do_react_to_env() about
            %% non-flapping-scenario that can also cause us to want to
            %% collapse to the none_projection to break a
            %% livelock/infinite loop.
            react_to_env_C100_inner(Author_latest, NotSanesDict0, MyName,
                                    P_newprop, P_latest, S)
    end.

react_to_env_C100_inner(Author_latest, NotSanesDict0, MyName,
                        P_newprop, P_latest,
                        #ch_mgr{consistency_mode=CMode} = S) ->
    NotSanesDict = orddict:update_counter(Author_latest, 1, NotSanesDict0),
    S2 = S#ch_mgr{not_sanes=NotSanesDict, sane_transitions=0},
    case orddict:fetch(Author_latest, NotSanesDict) of
        N when CMode == cp_mode ->
           ?V("YOYO-cp-mode,~w,~w,~w,",[MyName, P_latest#projection_v1.epoch_number,N]),
            ?REACT({c100, ?LINE, [{cmode,CMode},
                                  {not_sanes_author_count, N}]}),
            case get({zzz_quiet, P_latest#projection_v1.epoch_number}) of undefined -> ?V("YOYO-cp-mode,~w,current=~w,",[MyName, machi_projection:make_summary((S#ch_mgr.proj))]); _ -> ok end,
            put({zzz_quiet, P_latest#projection_v1.epoch_number}, true),
            react_to_env_A49(P_latest, [], S2);
        N when N > ?TOO_FREQUENT_BREAKER ->
            ?V("\n\nYOYO ~w breaking the cycle of:\n  current: ~w\n  new    : ~w\n", [MyName, machi_projection:make_summary(S#ch_mgr.proj), machi_projection:make_summary(P_latest)]),
            ?REACT({c100, ?LINE, [{not_sanes_author_count, N}]}),
            react_to_env_C103(P_newprop, P_latest, S2);
        N ->
           ?V("YOYO,~w,~w,~w,",[MyName, P_latest#projection_v1.epoch_number,N]),
            ?REACT({c100, ?LINE, [{not_sanes_author_count, N}]}),
            %% P_latest is not sane.
            %% By process of elimination, P_newprop is best,
            %% so let's write it.
            react_to_env_C300(P_newprop, P_latest, S2)
    end.

react_to_env_C103(#projection_v1{epoch_number=_Epoch_newprop} = _P_newprop,
                  #projection_v1{epoch_number=Epoch_latest,
                                 all_members=All_list,
                                 flap=Flap} = _P_latest,
                  #ch_mgr{name=MyName, proj=P_current}=S) ->
    #projection_v1{witnesses=Witness_list,
                   members_dict=MembersDict} = P_current,
    P_none0 = make_none_projection(MyName, All_list, Witness_list, MembersDict),
    %% P_none1 = P_none0#projection_v1{epoch_number=erlang:max(Epoch_newprop,
    %%                                                         Epoch_latest),
    P_none1 = P_none0#projection_v1{epoch_number=Epoch_latest,
                                    flap=Flap,
                                    dbg=[{none_projection,true}]},
    P_none = machi_projection:update_checksum(P_none1),
    %% Use it, darn it, because it's 100% safe.  And exit flapping state.
    ?REACT({c103, ?LINE,
            [{current_epoch, P_current#projection_v1.epoch_number},
             {none_projection_epoch, P_none#projection_v1.epoch_number}]}),
    %% Reset the not_sanes count dictionary here, or else an already
    %% ?TOO_FREQUENT_BREAKER count for an author might prevent a
    %% transition from C100_inner()->C300, which can lead to infinite
    %% looping C100->C103->C100.
    react_to_env_C100(P_none, P_none, clear_flapping_state(S)).

react_to_env_C110(P_latest, #ch_mgr{name=MyName, proj=P_current,
                                    proj_unanimous=ProjUnanimous} = S) ->
    ?REACT(c110),
    ?REACT({c110, ?LINE, [{latest_epoch,P_latest#projection_v1.epoch_number}]}),
    Extra1 = case inner_projection_exists(P_current) andalso
                  inner_projection_exists(P_latest) andalso
                  (machi_projection:get_epoch_id(
                     inner_projection_or_self(P_current)) ==
                   machi_projection:get_epoch_id(
                     inner_projection_or_self(P_latest)))
                 andalso ProjUnanimous /= false of
                 true ->
                     EpochID = machi_projection:get_epoch_id(
                                 inner_projection_or_self(P_latest)),
                     UnanimousTime = ProjUnanimous,
                     A = make_annotation(EpochID, UnanimousTime),
                     io:format(user, "\nCONFIRM debug C110 ~w annotates ~W outer ~w\n", [MyName, EpochID, 5, P_latest#projection_v1.epoch_number]),
                     [A, {annotated_by,c110}];
                 false ->
                     []
             end,
    Extra2 = [{react,get(react)}],
    P_latest2 = machi_projection:update_dbg2(P_latest, Extra1 ++ Extra2),

    MyNamePid = proxy_pid(MyName, S),
    Goo = P_latest2#projection_v1.epoch_number,
    %% This is the local projection store.  Use a larger timeout, so
    %% that things locally are pretty horrible if we're killed by a
    %% timeout exception.
    Goo = P_latest2#projection_v1.epoch_number,
    %% ?V("HEE110 ~w ~w ~w\n", [S#ch_mgr.name, self(), lists:reverse(get(react))]),

    case {?FLU_PC:write_projection(MyNamePid, private, P_latest2,?TO*30),Goo} of
        {ok, Goo} ->
            ?REACT({c120, [{write, ok}]}),
            %% We very intentionally do *not* pass P_latest2 forward:
            %% we must avoid bloating the dbg2 list!
            P_latest2_perhaps_annotated =
                machi_projection:update_dbg2(P_latest, Extra1),
            perhaps_verbose_c110(P_latest2_perhaps_annotated, S),
            react_to_env_C120(P_latest2_perhaps_annotated, [], S);
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
        Else ->
            Summ = machi_projection:make_summary(P_latest2),
            io:format(user, "C110 error by ~w: ~w, ~w\n~p\n",
                      [MyName, Else, Summ, get(react)]),
            error_logger:error_msg("C110 error by ~w: ~w, ~w, ~w\n",
                                   [MyName, Else, Summ, get(react)]),
            exit({c110_failure, MyName, Else, Summ})
    end.

react_to_env_C120(P_latest, FinalProps, #ch_mgr{proj_history=H,
                                                sane_transitions=Xtns}=S) ->
    ?REACT(c120),
    %% TODO: revisit this constant?
    MaxLength = length(P_latest#projection_v1.all_members) * 1.5,
    H2   = add_and_trunc_history(P_latest, H, MaxLength),

    %% diversion_c120_verbose_goop(P_latest, S),
    ?REACT({c120, [{latest, machi_projection:make_summary(P_latest)}]}),
    S2 = set_proj(S#ch_mgr{proj_history=H2,
                           sane_transitions=Xtns + 1}, P_latest),
    S3 = case is_annotated(P_latest) of
             false ->
                 S2;
             {{_ConfEpoch, _ConfCSum}, ConfTime} ->
io:format(user, "\nCONFIRM debug C120 ~w was annotated ~W outer ~w\n", [S#ch_mgr.name, (inner_projection_or_self(P_latest))#projection_v1.epoch_number, 5, P_latest#projection_v1.epoch_number]),
                 S2#ch_mgr{proj_unanimous=ConfTime}
         end,
    {{now_using, FinalProps, P_latest#projection_v1.epoch_number}, S3}.

add_and_trunc_history(P_latest, H, MaxLength) ->
    H2 = if P_latest#projection_v1.epoch_number > 0 ->
                 queue:in(P_latest, H);
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
    sleep_ranked_order(10, 100, MyName, Proj#projection_v1.all_members),
    react_to_env_C220(Retries, S).

react_to_env_C220(Retries, S) ->
    ?REACT(c220),
    react_to_env_A20(Retries + 1, S).

react_to_env_C300(#projection_v1{epoch_number=_Epoch_newprop}=P_newprop,
                  #projection_v1{epoch_number=_Epoch_latest}=_P_latest, S) ->
    ?REACT(c300),

    react_to_env_C310(machi_projection:update_checksum(P_newprop), S).

react_to_env_C310(P_newprop, S) ->
    ?REACT(c310),
    Epoch = P_newprop#projection_v1.epoch_number,
    {WriteRes, S2} = cl_write_public_proj_skip_local_error(Epoch, P_newprop, S),
    ?REACT({c310, ?LINE,
            [{newprop, machi_projection:make_summary(P_newprop)},
            {write_result, WriteRes}]}),
    react_to_env_A10(S2).

%% calculate_flaps() ... Create the background/internal state regarding our
%% own flapping state and what we know about the flapping state of our peers.
%% Other functions will consume this data and alter our suggested projection
%% changes if/when needed later.
%%
%% The flapping manifesto:
%%
%% I will enter flapping mode if:
%%
%% 1. My last F adopted private projections+P_newprop have the same
%%    UPI+Repairing list.  (If I am a direct victim of asymmetric
%%    partition, this is my only behavior.)
%%
%% 2. I observe a latest public projection by a flapping author.
%%
%% I will leave flapping mode if:
%%
%% 1. My view of up FLUs changes.  (As a direct observer, this is a
%%    will (indirectly) trigger a change of UPI+Repairing that I will
%%    suggest.)  Alas, there is no guarantee that I will win enough
%%    races to have my single new public projection truly observed by
%%    anyone else.  We will rely on fate and retrying @ new epochs to
%%    get our state changed noticed by someone later.
%%
%% 2. I observe a latest public projection E by author X such that author
%%    X is marked as not flapping *and* I believe that X is flapping at
%%    some earlier epoch E-delta.

calculate_flaps(P_newprop, P_latest, _P_current, CurrentUp, _FlapLimit,
                #ch_mgr{name=MyName, proj_history=H,
                        flap_start=FlapStart,
                        flap_count=FlapCount, flap_last_up=FlapLastUp,
                        flap_last_up_change=LastUpChange0,
                        flap_counts_last=FlapCountsLast,
                        runenv=RunEnv1}=S) ->
    UniqueProposalSummaries = make_unique_proposal_summaries(H, P_newprop),
    MyUniquePropCount = length(UniqueProposalSummaries),
    LastUpChange = if CurrentUp /= FlapLastUp ->
                           now();
                      true ->
                           LastUpChange0
                   end,
    LastUpChange_diff = timer:now_diff(now(), LastUpChange) / 1000000,
    ?REACT({calculate_flaps,?LINE,[{flap_start,FlapStart},
                                   {flap_count,FlapCount},
                                   {flap_last_up,FlapLastUp},
                                   {flap_counts_last,FlapCountsLast},
                                   {my_unique_prop_count,MyUniquePropCount},
                                   {current_up,CurrentUp},
                                   {last_up_change,LastUpChange},
                                   {last_up_change_diff,LastUpChange_diff}]}),

    %% TODO: Do we want to try to use BestP below to short-circuit
    %% calculation if we notice that the best private epoch # from
    %% somewhere has advanced?
    {_WhateverUnanimous, _BestP, Props, _S} =
        cl_read_latest_projection(private, S),
    HosedTransUnion = proplists:get_value(trans_all_hosed, Props),
    TransFlapCounts0 = proplists:get_value(trans_all_flap_counts, Props),

    %% NOTE: bad_answer_flus are due to timeout or some other network
    %%       glitch, i.e., anything other than {ok, P::projection()}
    %%       response from machi_flu0:proj_read_latest().
    BadFLUs = proplists:get_value(bad_answer_flus, Props),

    RemoteTransFlapCounts1 = lists:keydelete(MyName, 1, TransFlapCounts0),
    RemoteTransFlapCounts =
        [X || {_FLU, {{_FlEpk,FlTime}, _FlapCount}}=X <- RemoteTransFlapCounts1,
              FlTime /= ?NOT_FLAPPING],
    TempNewFlapCount = FlapCount + 1,
    TempAllFlapCounts = lists:sort([{MyName, {FlapStart, TempNewFlapCount}}|
                                    RemoteTransFlapCounts]),
    %% Sanity check.
    true = lists:all(fun({_FLU,{_EpkTime,_Count}}) -> true;
                        (_)                        -> false
                     end, TempAllFlapCounts),

    %% H is the bounded history of all of this manager's private
    %% projection store writes.  If we've proposed the *same*
    %% UPI+Repairing combo for the entire length of our
    %% bounded size of H, then we're flapping.
    %%
    %% If we're flapping, then we use our own flap counter and that of
    %% all of our peer managers to see if we've all got flap counters
    %% that exceed the flap_limit.  If that global condition appears
    %% true, then we "blow the circuit breaker" by stopping our
    %% participation in the flapping store (via the shortcut to A50).
    %%
    %% We reset our flap counter on any of several conditions:
    %%
    %% 1. If our bounded history H contains more than one proposal,
    %%    then by definition we are not flapping.
    %% 2. If a remote manager is flapping and has re-started a new
    %%    flapping episode.
    %% 3. If one of the remote managers that we saw earlier has
    %%    stopped flapping.

    ?REACT({calculate_flaps, ?LINE, [{queue_len, queue:len(H)},
                                     {uniques, UniqueProposalSummaries}]}),
    P_latest_Flap = get_raw_flapping_i(P_latest),
    AmFlappingNow_p = not (FlapStart == ?NOT_FLAPPING_START),
                      %% TODO: revisit why I added this extra length()
                      %%       condition back on commit 3dfe5c2.
                      %% andalso
                      %% length(UniqueProposalSummaries) == 1,
    P_latest_flap_start = case P_latest_Flap of
                              undefined ->
                                  ?NOT_FLAPPING_START;
                              _ ->
                                  element(1, P_latest_Flap#flap_i.flap_count)
                          end,
    MinQueueLen = 3,
    StartFlapping_p =
        case {queue:len(H), UniqueProposalSummaries} of
            _ when AmFlappingNow_p ->
                ?REACT({calculate_flaps,?LINE,[{flap_count,FlapCount},
                                               {flap_start,FlapStart}]}),
                %% I'm already flapping, therefore don't start again.
                false;
            {N, _} when N >= MinQueueLen,
                        P_latest_flap_start /= ?NOT_FLAPPING_START ->
                ?REACT({calculate_flaps,?LINE,
                        [{manifesto_clause,{start,2}},
                         {latest_epoch, P_latest#projection_v1.epoch_number},
                         {latest_flap_count,P_latest_Flap#flap_i.flap_count}]}),
                true;
            {N, [_]} when N >= MinQueueLen ->
                ?REACT({calculate_flaps,?LINE,[{manifesto_clause,{start,1}}]}),
                true;
            {_N, _} ->
                ?REACT({calculate_flaps,?LINE,[]}),
                false
        end,
    LeaveFlapping_p =
        if 
            LastUpChange_diff < 3.0 ->
                %% If the last time we saw a hard change in up/down status
                %% was less than this time ago, then we do not flap.  Give
                %% the status change some time to propagate.
                ?REACT({calculate_flaps,?LINE,[{time_diff,LastUpChange_diff}]}),
                true;
            StartFlapping_p ->
                %% If we're starting flapping on this iteration, don't ignore
                %% that intent.
                ?REACT({calculate_flaps,?LINE,[]}),
                false;
            AmFlappingNow_p andalso
            CurrentUp /= FlapLastUp ->
                ?REACT({calculate_flaps,?LINE,[{manifesto_clause,{leave,1}}]}),
                true;
            AmFlappingNow_p ->
                P_latest_LastStartTime =
                    search_last_flap_counts(
                      P_latest#projection_v1.author_server, FlapCountsLast),
                case get_flap_count(P_latest) of
                    {?NOT_FLAPPING_START, _Count}
                      when P_latest_LastStartTime == undefined ->
                        %% latest proj not flapping & not flapping last time
                        ?REACT({calculate_flaps,?LINE,[]}),
                        false;
                    {Curtime, _Count} when Curtime == P_latest_LastStartTime ->
                        %% latest proj flapping & flapping last time
                        ?REACT({calculate_flaps,?LINE,[]}),
                        false;
                    {0=Curtime, 0} when P_latest_LastStartTime /= undefined,
                                        P_latest_LastStartTime /= ?NOT_FLAPPING_START ->

                        ?REACT({calculate_flaps,?LINE,
                                [{manifesto_clause,{leave,2}},
                                 {p_latest, machi_projection:make_summary(P_latest)},
                                 {curtime, Curtime},
                                 {flap_counts_last, FlapCountsLast},
                                 {laststart_time, P_latest_LastStartTime}]}),
                        %% latest proj not flapping & flapping last time:
                        %% this author
                        true;
                    _ ->
                        ?REACT({calculate_flaps,?LINE,[]}),
                        false
                end;
            true ->
                ?REACT({calculate_flaps,?LINE,[]}),
                false
        end,
%% if true -> io:format(user, "CALC_FLAP: ~w: flapping_now ~w start ~w leave ~w latest-epoch ~w: ~w\n", [MyName, AmFlappingNow_p, StartFlapping_p, LeaveFlapping_p, P_latest#projection_v1.epoch_number, [X || X={calculate_flaps,_,_} <- lists:sublist(get(react), 3)]]); true -> ok end,
%% if LeaveFlapping_p andalso (AmFlappingNow_p orelse StartFlapping_p) -> io:format(user, "CALC_FLAP: ~w: flapping_now ~w start ~w leave ~w latest-epoch ~w: ~w\n", [MyName, AmFlappingNow_p, StartFlapping_p, LeaveFlapping_p, P_latest#projection_v1.epoch_number, [X || X={calculate_flaps,_,_} <- lists:sublist(get(react), 3)]]); true -> ok end,
    AmFlapping_p = if LeaveFlapping_p -> false;
                      true            -> AmFlappingNow_p orelse StartFlapping_p
                   end,

    if AmFlapping_p andalso not LeaveFlapping_p ->
            NewFlapCount = TempNewFlapCount,
            if element(2,FlapStart) == ?NOT_FLAPPING ->
                    NewFlapStart = {{epk,P_newprop#projection_v1.epoch_number},now()};
               true ->
                    NewFlapStart = FlapStart
            end,

            AllFlapCounts = TempAllFlapCounts,
            HosedTransUnionTs = [T || T <- HosedTransUnion, is_tuple(T)],
            AnnotatedBadFLUs = [{MyName, problem_with, FLU} || FLU <- BadFLUs],
            HosedAnnotations = lists:usort(HosedTransUnionTs ++ AnnotatedBadFLUs),
            Magic = lists:sort(
                      digraph_magic(P_newprop#projection_v1.all_members,
                                    HosedAnnotations)),
            AllHosed = lists:usort(HosedAnnotations ++ Magic),
            %%io:format(user, "ALLHOSED ~p: ~p ~w\n", [MyName, Magic, HosedAnnotations]),
            AllHosed;
       not AmFlapping_p ->
            NewFlapCount = 0,
            NewFlapStart = ?NOT_FLAPPING_START,
            AllFlapCounts = [],
            AllHosed = []
    end,

    AllFlapCounts_with_my_new =
        [{MyName, NewFlapStart}|lists:keydelete(MyName, 1, AllFlapCounts)],
    FlappingI = make_flapping_i(NewFlapStart, NewFlapCount, AllHosed,
                                AllFlapCounts_with_my_new, MyUniquePropCount),
    %% NOTE: Just because we increment flaps here, there's no correlation
    %%       to successful public proj store writes!  For example,
    %%       if we loop through states C2xx a few times, we would incr
    %%       flaps each time ... but the C2xx path doesn't write a new
    %%       proposal to everyone's public proj stores.  Similarly,
    %%       if we go through to C300, we will *try* to write to all public
    %%       stores, but the C3xx path doesn't care if all of those write
    %%       attempts *fail*.  Our flap count is a rough heuristic only, and
    %%       a large local flaps count gives no concrete guarantee that any
    %%       communication has been successful with any other part of the
    %%       cluster.
    %% TODO: 2015-03-04: I'm growing increasingly suspicious of
    %% the 'runenv' variable that's threaded through all this code.
    %% It isn't doing what I'd originally intended.  Fix it.
    S2 = S#ch_mgr{flap_count=NewFlapCount, flap_start=NewFlapStart,
                  flap_last_up=CurrentUp, flap_last_up_change=LastUpChange,
                  flap_counts_last=AllFlapCounts,
                  runenv=RunEnv1},
    {machi_projection:update_checksum(P_newprop#projection_v1{
                                                         flap=FlappingI}),
     if AmFlapping_p ->
             S2;
        true ->
             clear_most_flapping_state(S2)
     end}.

make_unique_proposal_summaries(H, P_newprop) ->
    HistoryPs = queue:to_list(H),
    Ps = HistoryPs ++ [P_newprop],
    lists:usort([{P#projection_v1.upi,
                  P#projection_v1.repairing} ||
                    P <- Ps]).

make_flapping_i() ->
    make_flapping_i(?NOT_FLAPPING_START, 0, [], [], 0).

make_flapping_i(NewFlapStart, NewFlapCount, AllHosed, AllFlapCounts,
                MyUniquePropCount) ->
    #flap_i{flap_count={NewFlapStart, NewFlapCount},
            all_hosed=AllHosed,
            all_flap_counts=lists:sort(AllFlapCounts),
            my_unique_prop_count=MyUniquePropCount}.

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
            Else
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
            HasInner1 = inner_projection_exists(P1),
            HasInner2 = inner_projection_exists(P2),
            if HasInner1 orelse HasInner2 ->
                    Inner1 = inner_projection_or_self(P1),
                    Inner2 = inner_projection_or_self(P2),
                    if HasInner1 orelse HasInner2 ->
                       %% In case of transition with inner projections, we
                       %% must allow the epoch number to remain constant.
                       %% Thus, we call the function that does not check for
                       %% a strictly-increasing epoch.
                       ?RETURN2(
                         projection_transition_is_sane_final_review(P1, P2,
                           projection_transition_is_sane_except_si_epoch(
                            Inner1, Inner2, RelativeToServer, RetrospectiveP)));
                   true ->
                       ?RETURN2(
                         projection_transition_is_sane_final_review(P1, P2,
                           projection_transition_is_sane_with_si_epoch(
                            Inner1, Inner2, RelativeToServer, RetrospectiveP)))
                    end;
               true ->
                    projection_transition_is_sane_final_review(P1, P2,
                                                               ?RETURN2(true))
            end;
        Else ->
            ?RETURN2(Else)
    end.

projection_transition_is_sane_final_review(
  P1, P2, {expected_author2,UPI1_tail,_}=Else) ->
    %% Reminder: P1 & P2 are outer projections
    %%
    %% We have a small problem for state transition sanity checking in the
    %% case where we are flapping *and* a repair has finished.  One of the
    %% sanity checks in simple_chain_state_transition_is_sane(() is that
    %% the author of P2 in this case must be the tail of P1's UPI: i.e.,
    %% it's the tail's responsibility to perform repair, therefore the tail
    %% must damn well be the author of any transition that says a repair
    %% finished successfully.
    %%
    %% The problem is that author_server of the inner projection does not
    %% reflect the actual author!  See the comment with the text
    %% "The inner projection will have a fake author" in react_to_env_A30().
    %%
    %% So, there's a special return value that tells us to try to check for
    %% the correct authorship here.
    P1HasInner_p = inner_projection_exists(P1),
    P2HasInner_p = inner_projection_exists(P2),
    P1_LastInnerUPI = case (inner_projection_or_self(P1))#projection_v1.upi of
                          P1InnerUPI=[_|_] when P1HasInner_p ->
                              lists:last(P1InnerUPI);
                          _ ->
                              no_such_author
                      end,
    if P1HasInner_p, P2HasInner_p ->
            if UPI1_tail == P1_LastInnerUPI ->
                    ?RETURN2(true);
               true ->
                    ?RETURN2(Else)
               end;
       UPI1_tail == P2#projection_v1.author_server ->
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
  #projection_v1{mode=cp_mode, upi=UPI1}=_P1,
  #projection_v1{mode=cp_mode, upi=UPI2, witnesses=Witness_list, dbg=Dbg2}=_P2,
  true) ->
    %% All earlier sanity checks has said that this transition is sane, but
    %% we also need to make certain that any CP mode transition preserves at
    %% least one non-witness server in the UPI list.  Earlier checks have
    %% verified that the ordering of the FLUs within the UPI list is ok.
    UPI1_s = ordsets:from_list(UPI1 -- Witness_list),
    UPI2_s = ordsets:from_list(UPI2 -- Witness_list),
    case proplists:get_value(zerf_backstop, Dbg2) of
        true when UPI1 == [] ->
            ?RETURN2(true);
        _ when UPI2 == [] ->
            %% We're down to the none projection to wedge ourself.  That's ok.
            ?RETURN2(true);
        _ ->
            ?RETURN2(not ordsets:is_disjoint(UPI1_s, UPI2_s))
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
    true = is_atom(AuthorServer1) andalso is_atom(AuthorServer2), % todo type may change?
    true = is_list(All_list1) andalso is_list(All_list2),
    true = is_list(Witness_list1) andalso is_list(Witness_list2),
    true = is_list(Down_list1) andalso is_list(Down_list2),
    true = is_list(UPI_list1) andalso is_list(UPI_list2),
    true = is_list(Repairing_list1) andalso is_list(Repairing_list2),
    true = is_list(Dbg1) andalso is_list(Dbg2),

    %% Don't check for strictly increasing epoch here: that's the job of
    %% projection_transition_is_sane_with_si_epoch().
    true = Epoch2 >= Epoch1,

    %% No duplicates
    true = lists:sort(Witness_list2) == lists:usort(Witness_list2),
    true = lists:sort(Down_list2) == lists:usort(Down_list2),
    true = lists:sort(UPI_list2) == lists:usort(UPI_list2),
    true = lists:sort(Repairing_list2) == lists:usort(Repairing_list2),

    %% Disjoint-ness
    All_list1 = All_list2,                 % todo will probably change
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
                                      AuthorServer2, UPI_list2w))
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

poll_private_proj_is_upi_unanimous(#ch_mgr{consistency_mode=ap_mode} = S) ->
    S;
poll_private_proj_is_upi_unanimous(#ch_mgr{consistency_mode=cp_mode,
                                           proj_unanimous={_,_,_}} = S) ->
    %% #ch_mgr{name=MyName, proj=Proj} = S,
    %% io:format(user, "\nCONFIRM debug ~w skip poll for inner ~w outer ~w\n",
    %%           [MyName, (inner_projection_or_self(Proj))#projection_v1.epoch_number, Proj#projection_v1.epoch_number]),
    S;
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
poll_private_proj_is_upi_unanimous_sleep(Count, S) ->
    timer:sleep((Count * Count) * 50),
    case poll_private_proj_is_upi_unanimous3(S) of
        #ch_mgr{proj_unanimous=false} = S2 ->
            poll_private_proj_is_upi_unanimous_sleep(Count + 1, S2);
        S2 ->
            S2
    end.

poll_private_proj_is_upi_unanimous3(#ch_mgr{name=MyName, proj=P_current,
                                            opts=MgrOpts} = S) ->
    Proj_ios = inner_projection_or_self(P_current),
    UPI = Proj_ios#projection_v1.upi,
    EpochID = machi_projection:make_epoch_id(Proj_ios),
    {Rs, S2} = read_latest_projection_call_only2(private, UPI, S),
    Rs2 = [if is_record(R, projection_v1) ->
                   machi_projection:make_epoch_id(inner_projection_or_self(R));
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
            OuterEpoch = P_current#projection_v1.epoch_number,
            case ?FLU_PC:read_projection(ProxyPid, private, OuterEpoch) of
                {ok, P_currentFull} ->
                    Now = os:timestamp(),
                    Annotation = make_annotation(EpochID, Now),
                    NewDbg2 = [Annotation|P_currentFull#projection_v1.dbg2],
                    NewProj = P_currentFull#projection_v1{dbg2=NewDbg2},
                    ProjStore = case get_projection_store_regname(MgrOpts) of
                                   undefined ->
                                       machi_flu_psup:make_proj_supname(MyName);
                                   PStr ->
                                       PStr
                                end,
                    #projection_v1{epoch_number=_EpochRep,
                                   epoch_csum= <<_CSumRep:4/binary,_/binary>>,
                                   upi=_UPIRep,
                                   repairing=_RepairingRep} =
                        inner_projection_or_self(NewProj),
                    io:format(user, "\nCONFIRM epoch ~w ~w upi ~w rep ~w by ~w ~w\n", [_EpochRep, _CSumRep, _UPIRep, _RepairingRep, MyName, if P_current#projection_v1.inner == undefined -> outer; true -> {inner,{outer,P_current#projection_v1.epoch_number}} end]),
                    ok = machi_projection_store:write(ProjStore, private, NewProj),
                    %% Unwedge our FLU.
                    {ok, NotifyPid} = machi_projection_store:get_wedge_notify_pid(ProjStore),
                    _ = machi_flu1:update_wedge_state(NotifyPid, false, EpochID),
                    S2#ch_mgr{proj_unanimous=Now};
                _ ->
                    S2
            end;
        _Else ->
            %% io:format(user, "poll by ~w: want ~W got ~W\n",
            %%           [MyName, EpochID, 6, _Else, 8]),
            S2
    end.

poll_read_private_projections(#projection_v1{inner=undefined,
                                             epoch_number=Epoch,
                                             upi=UPI}=_P_current, S) ->
    read_projection_call_only2(private, Epoch, UPI, S);
poll_read_private_projections(#projection_v1{inner=_not_undefined,
                                             upi=UPI}=_P_current, S) ->
    %% For inner projections, we are (by definition) flapping, and the
    %% outer epoch numbers are (by definition) unstable.  However, any
    %% observed use of the the inner proj epoch # is what we need.
    read_latest_projection_call_only2(private, UPI, S).

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

get_raw_flapping_i(#projection_v1{flap=F}) ->
    F.

get_flap_count(P) ->
    case get_raw_flapping_i(P) of undefined -> {0, 0};
                                  F ->         F#flap_i.flap_count
    end.

get_all_flap_counts(P) ->
    case get_raw_flapping_i(P) of undefined -> [];
                                  F ->         F#flap_i.all_flap_counts
    end.

get_all_hosed(P) when is_record(P, projection_v1)->
    case get_raw_flapping_i(P) of undefined -> [];
                                  F ->         F#flap_i.all_hosed
    end.

merge_flap_counts(FlapCounts) ->
    merge_flap_counts(FlapCounts, orddict:new()).

merge_flap_counts([], D) ->
    orddict:to_list(D);
merge_flap_counts([FlapCount|Rest], D1) ->
    %% We know that FlapCount is list({Actor, {{_epk,FlapStartTime},NumFlapCount}}).
    D2 = orddict:from_list(FlapCount),
    D2 = orddict:from_list(FlapCount),
    %% If the FlapStartTimes are identical, then pick the bigger flap count.
    %% If the FlapStartTimes differ, then pick the larger start time tuple.
    D3 = orddict:merge(fun(_Key, {{_,T1}, NF1}= V1, {{_,T2}, NF2}=V2)
                             when T1 == T2 ->
                               if NF1 > NF2 ->
                                       V1;
                                  true ->
                                       V2
                               end;
                          (_Key, {{_,T1},_NF1}= V1, {{_,T2},_NF2}=V2) ->
                               if T1 > T2 ->
                                       V1;
                                  true ->
                                       V2
                               end;
                          (_Key, V1, V2) ->
                               exit({bad_merge_2tuples,mod,?MODULE,line,?LINE,
                                     _Key, V1, V2})
                       end, D1, D2),
    merge_flap_counts(Rest, D3).

proxy_pid(Name, #ch_mgr{proxies_dict=ProxiesDict}) ->
    orddict:fetch(Name, ProxiesDict).

gimme_random_uniform(N, S) ->
    RunEnv1 = S#ch_mgr.runenv,
    Seed1 = proplists:get_value(seed, RunEnv1),
    {X, Seed2} = random:uniform_s(N, Seed1),
    RunEnv2 = [{seed, Seed2}|lists:keydelete(seed, 1, RunEnv1)],
    {X, S#ch_mgr{runenv=RunEnv2}}.

inner_projection_exists(#projection_v1{inner=undefined}) ->
    false;
inner_projection_exists(#projection_v1{inner=_}) ->
    true.

inner_projection_or_self(P) ->
    case inner_projection_exists(P) of
        false ->
            P;
        true ->
            P#projection_v1.inner
    end.

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
    case inner_projection_or_self(P_current) of
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
              "Repair start: tail ~p of ~p -> ~p, ~p ID ~w\n",
              [MyName, UPI0, Repairing, RepairMode, RepairId]),

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
              "Repair ~s: tail ~p of ~p finished ~p repair ID ~w: "
              "~p\nStats ~p\n",
              [Summary, MyName, UPI0, RepairMode, RepairId,
               Res, Stats]),
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

perhaps_call_t(S, Partitions, FLU, DoIt) ->
    try
        perhaps_call(S, Partitions, FLU, DoIt)
    catch
        exit:timeout ->
            remember_partition_hack(FLU),
            {error, partition};
        exit:{timeout,_} ->
            remember_partition_hack(FLU),
            {error, partition}
    end.

perhaps_call(#ch_mgr{name=MyName}=S, Partitions, FLU, DoIt) ->
    ProxyPid = proxy_pid(FLU, S),
    RemoteFLU_p = FLU /= MyName,
    erase(bad_sock),
    case RemoteFLU_p andalso lists:member({MyName, FLU}, Partitions) of
        false ->
            Res = DoIt(ProxyPid),
            if Res == {error, partition} ->
                    remember_partition_hack(FLU);
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
        _ ->
            (catch put(react, [{timeout1,me,MyName,to,FLU,RemoteFLU_p,Partitions}|get(react)])),
            exit(timeout)
    end.

init_remember_partition_hack() ->
    put(remember_partition_hack, []).

remember_partition_hack(FLU) ->
    put(remember_partition_hack, [FLU|get(remember_partition_hack)]).

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
                    numOrders,NumOrders, answer1,Answer1}]}),
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

chain_state_transition_is_sane(Author1, UPI1, Repair1, Author2, UPI2) ->
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
            %% For AP mode, this transition is always safe (though not
            %% always optimal for highest availability).
            ?RETURN2(true);
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

all_hosed_history(#projection_v1{epoch_number=_Epoch, flap=Flap},
                  {OldAllHosed,Acc}) ->
    AllHosed = if Flap == undefined ->
                       [];
                  true ->
                       Flap#flap_i.all_hosed
               end,
    if AllHosed == OldAllHosed ->
            {OldAllHosed, Acc};
       true ->
            {AllHosed, [AllHosed|Acc]}
    end.

clear_flapping_state(S) ->
    S2 = clear_most_flapping_state(S),
    S2#ch_mgr{not_sanes=orddict:new()}.

clear_most_flapping_state(S) ->
    S#ch_mgr{flap_count=0,
             flap_start=?NOT_FLAPPING_START,
             %% Do not clear flap_last_up.
             flap_counts_last=[]}.

full_majority_size(N) when is_integer(N) ->
    (N div 2) + 1;
full_majority_size(L) when is_list(L) ->
    full_majority_size(length(L)).

make_zerf(#projection_v1{epoch_number=OldEpochNum,
                         all_members=AllMembers,
                         members_dict=MembersDict,
                         witnesses=OldWitness_list,
                         flap=OldFlap
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
            throw({zerf, {not_enough_up, Up, AllMembers}});
        true ->
            make_zerf2(OldEpochNum, Up, MajoritySize, MyName,
                       AllMembers, OldWitness_list, MembersDict, OldFlap, S)
    end.

make_zerf2(OldEpochNum, Up, MajoritySize, MyName, AllMembers, OldWitness_list,
           MembersDict, OldFlap, S) ->
    try
        Proj = zerf_find_last_common(MajoritySize, Up, S),
        Proj2 = Proj#projection_v1{flap=OldFlap, dbg2=[]},
        %% io:format(user, "ZERF ~w\n",[machi_projection:make_summary(Proj2)]),
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
            P2 =
            machi_projection:update_checksum(
              P#projection_v1{epoch_number=OldEpochNum,
                              mode=cp_mode,
                              dbg2=[zerf_all]}),
            %% io:format(user, "ZERF ~w\n",[machi_projection:make_summary(P2)]),
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
                          (catch put(yyy_hack, [{FLU, Epoch, outer_ok}|get(yyy_hack)])),
                                       Proj;
                                  true ->
                                       %% We only use Proj2 for sanity checking
                                       %% here, do not return an inner!
                                       Proj2 = inner_projection_or_self(Proj),
                                       %% Sanity checking
                                       ConfEpoch = Proj2#projection_v1.epoch_number,
                                       ConfCSum  = Proj2#projection_v1.epoch_csum,
                          (catch put(yyy_hack, [{FLU, Epoch, inner_ok_return_original_outerplusinner}|get(yyy_hack)])),
                                       Proj
                               end,
                          if length(Px#projection_v1.upi) >= MajoritySize ->
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

my_lists_split(N, L) ->
    try
        lists:split(N, L)
    catch
        error:badarg ->
            {L, []}
    end.

diversion_c120_verbose_goop(#projection_v1{upi=[], repairing=[]}, _S) ->
    ok;
diversion_c120_verbose_goop(Proj, S) ->
    case proplists:get_value(private_write_verbose, S#ch_mgr.opts) of
        true ->
            diversion_c120_verbose_goop2(Proj, S);
        _ ->
            ok
    end.

diversion_c120_verbose_goop2(P_latest0, S) ->
    P_latest = machi_projection:update_checksum(P_latest0#projection_v1{dbg2=[]}),
    Type = case inner_projection_exists(P_latest) of true -> "inner";
                                                     _    -> "outer"
           end,
    #projection_v1{epoch_number=Epoch, epoch_csum=CSum, upi=UPI,
                   repairing=Repairing}= inner_projection_or_self(P_latest),
    UPI_Rs = UPI ++ Repairing,
    R = [try
             true = (UPI_Rs /= []),
             Proxy = proxy_pid(FLU, S),
             {ok, P} = ?FLU_PC:read_projection(Proxy, private, Epoch),
             case machi_projection:update_checksum(P#projection_v1{dbg2=[]}) of
                 X when X == P_latest ->
                     FLU;
                 _ ->
                     nope
             end
         catch _:_ ->
                 definitely_not
         end || FLU <- UPI_Rs],
    if R == UPI_Rs ->
            io:format(user, "\nCONFIRM by epoch ~s ~p ~W at ~p ~p\n",
                      [Type, Epoch, CSum, 4, UPI, Repairing]);
       true ->
            ok
    end.

perhaps_verbose_c110(P_latest2, S) ->
    case proplists:get_value(private_write_verbose, S#ch_mgr.opts) of
        true ->
            {_,_,C} = os:timestamp(),
            MSec = trunc(C / 1000),
            {HH,MM,SS} = time(),
            Dbg2X = lists:keydelete(react, 1,
                                    P_latest2#projection_v1.dbg2) ++
                [{is_annotated,is_annotated(P_latest2)}],
            P_latest2x = P_latest2#projection_v1{dbg2=Dbg2X}, % limit verbose len.
            case inner_projection_exists(P_latest2) of
                false ->
                    Last2 = get(last_verbose),
                    Summ2 = machi_projection:make_summary(P_latest2x),
                    case proplists:get_value(private_write_verbose,
                                             S#ch_mgr.opts) of
                        true ->
                        %% true when Summ2 /= Last2 ->
                            put(last_verbose, Summ2),
                            ?V("\n~2..0w:~2..0w:~2..0w.~3..0w ~p uses plain: ~w \n",
                              [HH,MM,SS,MSec, S#ch_mgr.name, Summ2]);
                        _ ->
                            ok
                    end;
                true ->
                    Last2 = get(last_verbose),
                    P_inner = inner_projection_or_self(P_latest2),
                    P_innerx = P_inner#projection_v1{dbg2=Dbg2X}, % limit verbose len.
                    Summ2 = machi_projection:make_summary(P_innerx),
                    case proplists:get_value(private_write_verbose,
                                             S#ch_mgr.opts) of
                        true ->
                        %% true when Summ2 /= Last2 ->
                            put(last_verbose, Summ2),
                            ?V("\n~2..0w:~2..0w:~2..0w.~3..0w ~p uses inner: ~w (outer ~w auth ~w flap ~w)\n",
                              [HH,MM,SS,MSec, S#ch_mgr.name, Summ2, P_latest2#projection_v1.epoch_number, P_latest2#projection_v1.author_server, P_latest2#projection_v1.flap]);
                        _ ->
                            ok
                    end
            end;
        _ ->
            ok
    end.

digraph_magic(All_list, HosedAnnotations) ->
    G = digraph:new(),
    [digraph:add_vertex(G, V) || V <- All_list],
    [digraph:add_edge(G, V1, V2) || {V1, problem_with, V2} <- HosedAnnotations],
    calc_magic_down(lists:sort(digraph:vertices(G)), G).

calc_magic_down([], G) ->
    digraph:delete(G),
    [];
calc_magic_down([H|T], G) ->
    case digraph:in_degree(G, H) of
        0 ->
            calc_magic_down(T, G);
        1 ->
            Neighbors = digraph:in_neighbours(G, H),
            case [V || V <- Neighbors, digraph:in_degree(G, V) == 1] of
                [AlsoOne|_] ->
                    %% TODO: be smarter here about the choice of which is down.
                    [H|calc_magic_down(T -- [AlsoOne], G)];
                [] ->
                    %% H is "on the end", e.g. 1-2-1, so it's OK.
                    calc_magic_down(T, G)
            end;
        N when N > 1 ->
            [H|calc_magic_down(T, G)]
    end.

search_last_flap_counts(FLU, FlapCountsLast) ->
    proplists:get_value(FLU, FlapCountsLast, undefined).

calc_consistency_mode(_Witness_list = []) ->
    ap_mode;
calc_consistency_mode(_Witness_list) ->
    cp_mode.

set_proj(S, Proj) ->
    S#ch_mgr{proj=Proj, proj_unanimous=false}.

make_annotation(EpochID, Time) ->
    {private_proj_is_upi_unanimous, {EpochID, Time}}.

is_annotated(#projection_v1{dbg2=Dbg2}) ->
    proplists:get_value(private_proj_is_upi_unanimous, Dbg2, false).
