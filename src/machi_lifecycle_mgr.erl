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

%% @doc Lifecycle manager for Machi FLUs and chains.
%%
%% Over the lifetime of a Machi cluster, both the number and types of
%% FLUs and chains may change.  The lifecycle manager is responsible
%% for implementing the lifecycle changes as expressed by "policy".
%% In our case, "policy" is created by an external administrative
%% entity that creates and deletes configuration files that define
%% FLUs and chains relative to this local machine.  (This "policy"
%% may be a human administrator or (as the Machi project matures)
%% partial or full automatic implementation of policy.)
%%
%% The "master configuration" for deciding which FLUs should be
%% running on this machine was inspired by BSD UNIX's `init(8)' and the
%% "rc.d" scheme.  FLU definitions are found in a single directory,
%% with one file per FLU.  Chains are defined similarly, with one
%% definition file per chain.
%%
%% If a definition file for a FLU (or chain) exists, then that
%% FLU/chain ought to be configured into being and running.  If a
%% definition file for a FLU/chain is removed, then that FLU/chain
%% should be stopped gracefully.  However, deleting of a file destroys
%% information that is stored inside of that file.  Therefore, we will
%% <b>not allow arbitrary unlinking of lifecycle config files</b>.  If
%% the administrator deletes these config files using `unlink(8)'
%% directly, then "the warranty has been broken".
%%
%% We will rely on using an administrative command to inform the
%% running system to stop and/or delete lifecycle resources.  If the
%% Machi application is not running, sorry, please start Machi first.
%%
%% == Wheel reinvention ==
%%
%% There's a whole mess of configuration management research &amp;
%% libraries out there.  I hope to ignore them all by doing something
%% quick &amp; dirty &amp; good enough here.  If I fail, then I'll go
%% pay attention to That Other Stuff.
%%
%% == A note about policy ==
%%
%% It is outside of the scope of this local lifecycle manager to make
%% decisions about policy or to distribute policy info/files/whatever
%% to other machines.  This is our machine.  There are many like it,
%% but this one is ours.
%%
%% == Machi Application Variables ==
%%
%% All OTP application environment variables below are defined in the
%% `machi' application.
%%
%% <ul>
%%  <li> <tt>flu_config_dir</tt>: Stores the `rc.d-'like config files for
%%       FLU runtime policy.
%% </li>
%%  <li> <tt>flu_data_dir</tt>: Stores the file data and metadata for
%%       all FLUs.
%% </li>
%%  <li> <tt>chain_config_dir</tt>: Stores the `rc.d'-like config files for
%%       chain runtime policy.
%% </li>
%% </ul>
%%
%% == The FLU Lifecycle ==
%%
%% See also: [https://github.com/basho/machi/tree/master/doc/flu-and-chain-lifecycle.org]
%%
%% FLUs on the local machine may be started and stopped, as defined by
%% administrative policy.  In order to do any useful work, however, a
%% running FLU must also be configured to be a member of a replication
%% chain.  Thus, as a practical matter, both a FLU and the chain that
%% the FLU participates in must both be managed by this manager.
%%
%% The FLU will be started with the file's specified parameters.  A
%% FLU should be defined and started before configuring its chain
%% membership.
%%
%% Usually a FLU is removed implicitly by removing that FLU from the a
%% newer definition of its chain (by omitting the FLU's name).
%% If a FLU has been started but never been a chain
%% member, then the FLU can be stopped and removed explicitly.
%%
%% A FLU may be created or removed (via implicit or explicit policy).
%% An existing FLU may not be reconfigured.
%%
%% == The Chain Lifecycle ==
%%
%% See also: [https://github.com/basho/machi/tree/master/doc/flu-and-chain-lifecycle.org]
%%
%% If a FLU on the local machine is expected to participate in a
%% replication chain, then an `rc.d'-style chain definition file must
%% also be present on each machine that runs a FLU in the chain.
%%
%% FLUs in a new chain should have definition files created on each
%% FLU's respective machine prior to defining their chain.  Similarly,
%% on each machine that hosts a chain member, a chain definition file
%% created.  External policy is responsible for creating each of these
%% files.
%%
%% A chain may be created or modified.
%%
%% A modification request writes a `#chain_def_v1{}' record with the
%% same name but different `full' and/or `witnesses' list to each
%% machine that hosts a FLU in the chain (in both the old and new
%% versions of the chain).
%%
%% == Conflicts with TCP ports, FLU &amp; chain names, etc ==
%%
%% This manager is not responsible for managing conflicts in resource
%% namespaces, e.g., TCP port numbers, FLU names, chain names, etc.
%% Managing these namespaces is external policy's responsibility.

-module(machi_lifecycle_mgr).

-behaviour(gen_server).

-include("machi_projection.hrl").

%% API
-export([start_link/0,
         process_pending/0]).
-export([get_pending_dir/0, get_rejected_dir/0, get_flu_config_dir/0,
         get_flu_data_dir/0, get_chain_config_dir/0, get_data_dir/0]).
-export([quick_admin_sanity_check/1, quick_admin_apply/2]).
-export([make_pending_config/1, run_ast/1, diff_env/2]).
-ifdef(TEST).
-compile(export_all).
-endif. % TEST

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {
          flus = []   :: [atom()],
          chains = [] :: list()
         }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

process_pending() ->
    gen_server:call(?SERVER, {process_pending}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    self() ! finish_init,
    {ok, #state{}}.

handle_call({process_pending}, _From, State) ->
    {Reply, NewState} = do_process_pending(State),
    {reply, Reply, NewState};
handle_call(_Request, _From, State) ->
    Reply = 'whatwatwha????????????????????',
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(finish_init, State) ->
    {noreply, finish_init(State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

finish_init(S) ->
    %% machi_flu_sup will start all FLUs that have a valid definition
    %% file.  That supervisor's structure + OTP supervisor behavior
    %% guarantees that those FLUs should now be running.
    %% (TODO: Unless they absolutely cannot keep running and the
    %% supervisor has given up and terminated them.)
    RunningFLUs = get_local_running_flus(),
    RunningFLU_Epochs = get_latest_public_epochs(RunningFLUs),
    RunningFLUs_at_zero = [FLU || {FLU, 0} <- RunningFLU_Epochs],
    lager:info("Running FLUs: ~p\n", [RunningFLUs]),
    lager:info("Running FLUs at epoch 0: ~p\n", [RunningFLUs_at_zero]),
    ChainDefs = get_initial_chains(),
    perhaps_bootstrap_chains(ChainDefs, RunningFLUs_at_zero, RunningFLUs),
    S#state{flus=RunningFLUs, chains=ChainDefs}.

get_local_running_flus() ->
    [Name || {Name,_,_,_} <- supervisor:which_children(machi_flu_sup)].

get_latest_public_epochs(FLUs) ->
    [begin
         PS = machi_flu1:make_projection_server_regname(FLU),
         {ok, {Epoch, _CSum}} = machi_projection_store:get_latest_epochid(
                                  PS, public),
         {FLU, Epoch}
     end || FLU <- FLUs].

get_initial_chains() ->
    ConfigDir = get_chain_config_dir(),
    CDefs = [CDef || {_File, CDef} <- machi_flu_sup:load_rc_d_files_from_dir(
                                        ConfigDir)],
    sanitize_chain_def_records(CDefs).

sanitize_chain_def_records(Ps) ->
    {Sane, _} = lists:foldl(fun sanitize_chain_def_rec/2, {[], gb_trees:empty()}, Ps),
    Sane.

sanitize_chain_def_rec(Whole, {Acc, D}) ->
    try
        #chain_def_v1{name=Name,
                      mode=Mode,
                      full=Full,
                      witnesses=Witnesses} = Whole,
        {true, ?LINE} = {is_atom(Name), ?LINE},
        NameK = {name, Name},
        {none, ?LINE} = {gb_trees:lookup(NameK, D), ?LINE},
        {true, ?LINE} = {(Mode == ap_mode orelse Mode == cp_mode), ?LINE},
        IsPSrvr = fun(X) when is_record(X, p_srvr) -> true;
                     (_)                           -> false
                  end,
        {true, ?LINE} = {lists:all(IsPSrvr, Full), ?LINE},
        {true, ?LINE} = {lists:all(IsPSrvr, Witnesses), ?LINE},

        %% All is sane enough.
        D2 = gb_trees:enter(NameK, Name, D),
        {[Whole|Acc], D2}
    catch X:Y ->                                % badmatch will include ?LINE
            lager:error("~s: Bad chain_def record (~w ~w), skipping: ~P\n",
                        [?MODULE, X, Y, Whole, 15]),
            {Acc, D}
    end.

perhaps_bootstrap_chains([], LocalFLUs_at_zero, LocalFLUs) ->
    if LocalFLUs == [] ->
            ok;
       LocalFLUs_at_zero == [] ->
            ok;
       true ->
            lager:warning("The following FLUs are defined but are not also "
                          "members of a defined chain: ~w\n",
                          [LocalFLUs_at_zero])
    end,
    ok;
perhaps_bootstrap_chains([CD|ChainDefs], LocalFLUs_at_zero, LocalFLUs) ->
    #chain_def_v1{full=Full, witnesses=Witnesses} = CD,
    AllNames = [Name || #p_srvr{name=Name} <- Full ++ Witnesses],
    case ordsets:intersection(ordsets:from_list(AllNames),
                              ordsets:from_list(LocalFLUs_at_zero)) of
        [] ->
            perhaps_bootstrap_chains(ChainDefs, LocalFLUs_at_zero, LocalFLUs);
        [FLU1|_]=FLUs ->
            %% One FLU is enough: Humming Consensus will config the remaining
            _ = bootstrap_chain(CD, FLU1),
            perhaps_bootstrap_chains(ChainDefs, LocalFLUs_at_zero -- FLUs,
                                    LocalFLUs -- FLUs)
    end.

bootstrap_chain(CD, FLU) ->
    bootstrap_chain2(CD, FLU, 20).

bootstrap_chain2(CD, FLU, 0) ->
    lager:warning("Failed all attempts to bootstrap chain ~w via FLU ~w ",
                  [CD,FLU]),
    failed;
bootstrap_chain2(#chain_def_v1{name=NewChainName, mode=NewCMode,
                               full=Full, witnesses=Witnesses,
                               old_full=ReqOldFull,
                               old_witnesses=ReqOldWitnesses,
                               props=_Props}=CD,
                 FLU, N) ->
    All_p_srvrs = Witnesses ++ Full,
    L = [{Name, P_srvr} || #p_srvr{name=Name}=P_srvr <- All_p_srvrs],
    MembersDict = orddict:from_list(L),
    NewAll_list = [Name || #p_srvr{name=Name} <- All_p_srvrs],
    NewWitnesses_list = [Name || #p_srvr{name=Name} <- Witnesses],

    Mgr = machi_chain_manager1:make_chmgr_regname(FLU),
    PStore = machi_flu1:make_projection_server_regname(FLU),
    {ok, #projection_v1{epoch_number=OldEpoch, chain_name=OldChainName,
                        mode=OldCMode,
                        all_members=OldAll_list, witnesses=OldWitnesses}} =
        machi_projection_store:read_latest_projection(PStore, private),
    case set_chain_members(OldChainName, NewChainName, OldCMode,
                           ReqOldFull, ReqOldWitnesses,
                           OldAll_list, OldWitnesses,
                           NewAll_list, Witnesses,
                           Mgr, NewChainName, OldEpoch,
                           NewCMode, MembersDict, NewWitnesses_list) of
        ok ->
            lager:info("Configured chain ~w via FLU ~w to "
                       "mode=~w all=~w witnesses=~w\n",
                       [NewChainName, FLU, NewCMode,
                        NewAll_list, NewWitnesses_list]),
            ok;
        chain_bad_state=Else ->
            lager:error("Attempt to bootstrap chain ~w via FLU ~w "
                        "failed (no retries): ~w (defn ~w)\n",
                        [NewChainName, FLU, Else, CD]),
            Else;
        Else ->
            lager:error("Attempt ~w to bootstrap chain ~w via FLU ~w "
                        "failed: ~w (may retry with defn ~w)\n",
                        [N, NewChainName, FLU, Else, CD]),
            bootstrap_chain2(CD, FLU, N-1)
    end.

set_chain_members(OldChainName, NewChainName, OldCMode,
                  ReqOldFull, ReqOldWitnesses,
                  OldFull_list, OldWitnesses, NewAll_list, NewWitnesses,
                  Mgr, ChainName, OldEpoch, NewCMode, MembersDict, _Props) ->
    if OldChainName == NewChainName, OldCMode == NewCMode,
       OldFull_list == NewAll_list, OldWitnesses == NewWitnesses ->
            %% The chain's current definition at this FLU is already what we
            %% want.  Let's pretend that we sent the command and that it was
            %% successful.
            ok;
       OldEpoch == 0 orelse (OldChainName == NewChainName andalso
                             OldCMode == NewCMode andalso
                             ReqOldFull == OldFull_list andalso
                             ReqOldWitnesses == OldWitnesses) ->
            %% The old epoch is 0 (no chain defined) or else the prerequisites
            %% for our chain change request are indeed matched by the FLU's
            %% current private projection.
            machi_chain_manager1:set_chain_members(Mgr, ChainName, OldEpoch,
                                                   NewCMode,
                                                   MembersDict, NewWitnesses);
       true ->
            chain_bad_state
    end.

do_process_pending(S) ->
    PendingDir = get_pending_dir(),
    PendingParsed = machi_flu_sup:load_rc_d_files_from_dir(PendingDir),
    %% A pending file has exactly one record (#p_srvr{} or #chain_def_v1{}).
    P_FLUs = [X || {_File, #p_srvr{}}=X <- PendingParsed],
    P_Chains = [X || {_File, #chain_def_v1{}}=X <- PendingParsed],
    BadFiles = [File || {File, []} <- PendingParsed],
    S2 = process_pending_flus(P_FLUs, S),
    S3 = process_pending_chains(P_Chains, S2),
    S4 = process_bad_files(BadFiles, S3),
    {{P_FLUs, P_Chains}, S4}.

flu_config_exists(FLU) ->
    ConfigDir = get_flu_config_dir(),
    case file:read_file_info(ConfigDir ++ "/" ++ atom_to_list(FLU)) of
        {ok, _} ->
            true;
        _ ->
            false
    end.

get_pending_dir() ->
    {ok, EtcDir} = application:get_env(machi, platform_etc_dir),
    EtcDir ++ "/pending".

get_rejected_dir() ->
    {ok, EtcDir} = application:get_env(machi, platform_etc_dir),
    EtcDir ++ "/rejected".

get_flu_config_dir() ->
    {ok, Dir} = application:get_env(machi, flu_config_dir),
    Dir.

get_flu_data_dir() ->
    {ok, Dir} = application:get_env(machi, flu_data_dir),
    Dir.

get_chain_config_dir() ->
    application:get_env(machi, chain_config_dir, "/does/not/exist").

get_data_dir() ->
    {ok, Dir} = application:get_env(machi, platform_data_dir),
    Dir.

get_preserve_dir() ->
    get_data_dir() ++ "/^PRESERVE".

get_quick_admin_dir() ->
    {ok, EtcDir} = application:get_env(machi, platform_etc_dir),
    EtcDir ++ "/quick-admin-archive".

process_pending_flus(P_FLUs, S) ->
    lists:foldl(fun process_pending_flu/2, S, P_FLUs).

process_pending_flu({File, P}, S) ->
    #p_srvr{name=FLU} = P,
    CurrentPs = machi_flu_sup:get_initial_flus(),
    Valid_Ps = machi_flu_sup:sanitize_p_srvr_records(CurrentPs ++ [P]),
    case lists:member(P, Valid_Ps)
         andalso
         (not lists:keymember(FLU, #p_srvr.name, CurrentPs)) of
        false ->
            lager:error("Pending FLU config file ~s has been rejected\n",
                        [File]),
            _ = move_to_rejected(File, S),
            S;
        true ->
            try
                {ok, SupPid} = machi_flu_psup:start_flu_package(P),
                lager:info("Started FLU ~w with supervisor pid ~p\n",
                           [FLU, SupPid]),
                _ = move_to_flu_config(FLU, File, S),
                S
            catch error:Else ->
                    lager:error("Start FLU ~w failed: ~p\n", [FLU, Else]),
                    _ = move_to_rejected(File, S),
                    S
            end
    end.

process_pending_chains(P_Chains, S) ->
    lists:foldl(fun process_pending_chain/2, S, P_Chains).

process_pending_chain({File, CD}, S) ->
    #chain_def_v1{name=Name,
                  local_stop=LocalStopFLUs, local_run=LocalRunFLUs} = CD,
    case sanitize_chain_def_records([CD]) of
        [CD] ->
            case LocalRunFLUs of
                [] ->
                    case LocalStopFLUs of
                        [] ->
                            lager:error("Pending chain config file ~s has no "
                                        "FLUs on this machine, rejected\n",
                                        [File]),
                            _ = move_to_rejected(File, S),
                            S;
                        [_|_] ->
                            lager:info("Pending chain config file ~s stops "
                                       "all local members of chain ~w: ~w\n",
                                       [File, Name, LocalStopFLUs]),
                            process_pending_chain2(File, CD, LocalStopFLUs,
                                                   delete, S)
                    end;
                [FLU|_] ->
                    %% TODO: Between the successful chain change inside of
                    %% bootstrap_chain() (and before it returns to us!) and
                    %% the return of process_pending_chain2(), we have a race
                    %% window if this process crashes. (Review again!)
                    case bootstrap_chain(CD, FLU) of
                        ok ->
                            process_pending_chain2(File, CD, LocalStopFLUs,
                                                   move, S);
                        Else ->
                            lager:error("Pending chain config file ~s "
                                        "has failed (~w), rejected\n",
                                        [Else, File]),
                            _ = move_to_rejected(File, S),
                            S
                    end
            end;
        [] ->
            lager:error("Pending chain config file ~s has been rejected\n",
                        [File]),
            _ = move_to_rejected(File, S),
            S
    end.

process_pending_chain2(File, CD, RemovedFLUs, ChainConfigAction, S) ->
    LocalRemovedFLUs = [FLU || FLU <- RemovedFLUs,
                               flu_config_exists(FLU)],
    case LocalRemovedFLUs of
        [] ->
            ok;
        [_|_] ->
            %% %% Sleep for a little bit to allow HC to settle.
            %% timer:sleep(1000),
            [begin
                 %% We may be retrying this, so be liberal with any pattern
                 %% matching on return values.
                 _ = machi_flu_psup:stop_flu_package(FLU),
                 ConfigDir = get_flu_config_dir(),
                 FluDataDir = get_flu_data_dir(),
                 PreserveDir = get_preserve_dir(),
                 Suffix = make_ts_suffix(),
                 FLU_str = atom_to_list(FLU),
                 MyPreserveDir = PreserveDir ++ "/" ++ FLU_str ++ "." ++ Suffix,
                 ok = filelib:ensure_dir(MyPreserveDir ++ "/unused"),
                 _ = file:make_dir(MyPreserveDir),
                 Src1 = ConfigDir ++ "/" ++ FLU_str,
                 Dst1 = MyPreserveDir ++ "/" ++ FLU_str ++ ".config",
                 lager:info("Stopped FLU ~w: rename ~s ~s\n",
                            [FLU, Src1, Dst1]),
                 _ = file:rename(Src1, Dst1),
                 Src2 = FluDataDir ++ "/" ++ FLU_str,
                 Dst2 = MyPreserveDir ++ "/" ++ FLU_str ++ ".data",
                 lager:info("Stopped FLU ~w: rename ~s ~s\n",
                            [FLU, Src2, Dst2]),
                 %% TODO: If EXDEV, then we should rename to
                 %%       another dir on same device, but ... where?
                 _ = file:rename(Src2, Dst2),
                 ok
             end || FLU <- LocalRemovedFLUs],
            ok
    end,
    #chain_def_v1{name=Name} = CD,
    if ChainConfigAction == move ->
            _ = move_to_chain_config(Name, File, S);
       ChainConfigAction == delete ->
            _ = delete_chain_config(Name, File, S)
    end,
    S.

process_bad_files(Files, S) ->
    lists:foldl(fun move_to_rejected/2, S, Files).

move_to_rejected(File, S) ->
    lager:error("Pending unknown config file ~s has been rejected\n", [File]),
    Dst = get_rejected_dir(),
    Suffix = make_ts_suffix(),
    ok = file:rename(File, Dst ++ "/" ++ filename:basename(File) ++ Suffix),
    S.

make_ts_suffix() ->
    str("~w,~w,~w", tuple_to_list(os:timestamp())).

move_to_flu_config(FLU, File, S) ->
    lager:info("Creating FLU config file ~w\n", [FLU]),
    Dst = get_flu_config_dir(),
    ok = file:rename(File, Dst ++ "/" ++ atom_to_list(FLU)),
    S.

move_to_chain_config(Name, File, S) ->
    lager:info("Creating chain config file ~w\n", [Name]),
    Dst = get_chain_config_dir(),
    ok = file:rename(File, Dst ++ "/" ++ atom_to_list(Name)),
    S.

delete_chain_config(Name, File, S) ->
    lager:info("Deleting chain config file ~s for chain ~w\n", [File, Name]),
    Dst = get_chain_config_dir(),
    ok = file:delete(Dst ++ "/" ++ atom_to_list(Name)),
    S.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_ast_tuple_syntax(Ts) ->
    lists:partition(fun check_an_ast_tuple/1, Ts).

check_an_ast_tuple({host, Name, Props}) ->
    is_stringy(Name) andalso is_proplisty(Props) andalso
    lists:all(fun({admin_interface, X})  -> is_stringy(X);
                 ({client_interface, X}) -> is_stringy(X);
                 (_)                     -> false
              end, Props);
check_an_ast_tuple({host, Name, AdminI, ClientI, Props}) ->
    is_stringy(Name) andalso
    is_stringy(AdminI) andalso is_stringy(ClientI) andalso
    is_proplisty(Props) andalso
    lists:all(fun({admin_interface, X})  -> is_stringy(X);
                 ({client_interface, X}) -> is_stringy(X);
                 (_)                     -> false
              end, Props);
check_an_ast_tuple({flu, Name, HostName, Port, Props}) ->
    is_atom(Name) andalso is_stringy(HostName) andalso
        is_porty(Port) andalso is_proplisty(Props);
check_an_ast_tuple({chain, Name, FullList, Props}) ->
    is_atom(Name) andalso
    lists:all(fun erlang:is_atom/1, FullList) andalso
    is_proplisty(Props);
check_an_ast_tuple({chain, Name, CMode, FullList, Witnesses, Props}) ->
    is_atom(Name) andalso
    (CMode == ap_mode orelse CMode == cp_mode) andalso
    lists:all(fun erlang:is_atom/1, FullList) andalso
    lists:all(fun erlang:is_atom/1, Witnesses) andalso
    is_proplisty(Props);
check_an_ast_tuple(switch_old_and_new) ->
    true;
check_an_ast_tuple(_) ->
    false.

%% Prerequisite: all tuples are approved by check_ast_tuple_syntax().

normalize_ast_tuple_syntax(Ts) ->
    lists:map(fun normalize_an_ast_tuple/1, Ts).

normalize_an_ast_tuple({host, Name, Props}) ->
    AdminI  = proplists:get_value(admin_interface, Props, Name),
    ClientI = proplists:get_value(client_interface, Props, Name),
    Props2 = lists:keydelete(admin_interface, 1,
             lists:keydelete(client_interface, 1, Props)),
    {host, Name, AdminI, ClientI, n(Props2)};
normalize_an_ast_tuple({host, Name, AdminI, ClientI, Props}) ->
    Props2 = lists:keydelete(admin_interface, 1,
             lists:keydelete(client_interface, 1, Props)),
    {host, Name, AdminI, ClientI, n(Props2)};
normalize_an_ast_tuple({flu, Name, HostName, Port, Props}) ->
    {flu, Name, HostName, Port, n(Props)};
normalize_an_ast_tuple({chain, Name, FullList, Props}) ->
    {chain, Name, ap_mode, n(FullList), [], n(Props)};
normalize_an_ast_tuple({chain, Name, CMode, FullList, Witnesses, Props}) ->
    {chain, Name, CMode, n(FullList), n(Witnesses), n(Props)};
normalize_an_ast_tuple(A=switch_old_and_new) ->
    A.

run_ast(Ts) ->
    case check_ast_tuple_syntax(Ts) of
        {_, []} ->
            Ts2 = normalize_ast_tuple_syntax(Ts),
            Env1 = make_ast_run_env(),
            try
                Env2 = lists:foldl(fun run_ast_cmd/2, Env1, Ts2),
                {ok, Env2}
            catch throw:DbgStuff ->
                    {error, DbgStuff}
            end;
        {_, Else} ->
            {error, {bad_syntax, Else}}
    end.

%% Legend for env key naming scheme
%%
%% {kv, X}
%% Mutable: no.
%% Reference KV store for X.  Variations of X are:
%%     {host, Name} | {flu, Name} | {chain, Name}
%%     Value is a {host,...} or {flu,...}, or {chain,...} AST tuple.
%%
%%     {p_srvr, Name}
%%     #p_srvr{} record for FLU Name, for cache/convenience purposes.
%%     If a FLU has been defined via {kv,_}, this key must also exist.
%%
%%
%% {tmp, X}
%% Mutable: yes.
%% Tmp scratch for X.  Variations of X are:
%%     {flu_assigned_to, ChainName}
%%     If a FLU is currently assigned to a chain, map to ChainName.
%%     If a FLU is not currently assigned to a chain, key does not exist.

run_ast_cmd({host, Name, _AdminI, _ClientI, _Props}=T, E) ->
    Key = {kv,{host,Name}},
    case d_find(Key, E) of
        error ->
            d_store(Key, T, E);
        {ok, _} ->
            err("Duplicate host definition ~p", [Name], T)
    end;
run_ast_cmd({flu, Name, HostName, Port, Props}=T, E) ->
    Key   = {kv,{flu,Name}},
    Key_p = {kv,{p_srvr,Name}},
    HostExists_p = env_host_exists(HostName, E),
    case d_find(Key, E) of
        error when HostExists_p ->
            case host_port_is_assigned(HostName, Port, E) of
                false ->
                    {ok, ClientI} = get_host_client_interface(HostName, E),
                    Mod = proplists:get_value(
                            proto_mod, Props, 'machi_flu1_client'),
                    Val_p = #p_srvr{name=Name, proto_mod=Mod,
                                    address=ClientI, port=Port, props=Props},
                    d_store(Key, T,
                    d_store(Key_p, Val_p, E));
                {true, UsedBy} ->
                    err("Host ~p port ~p already in use by FLU ~p",
                        [HostName, Port, UsedBy], T)
            end;
        error ->
            err("Unknown host ~p", [HostName], T);
        {ok, _} ->
            err("Duplicate flu ~p", [Name], T)
    end;
run_ast_cmd({chain, Name, CMode, FullList, Witnesses, _Props}=T, E) ->
    Key = {kv,{chain,Name}},
    AllFLUs = FullList ++ Witnesses,

    %% All FLUs must exist.
    case lists:sort(AllFLUs) == lists:usort(AllFLUs) of
        true  -> ok;
        false -> err("Duplicate FLU(s) specified", [], T)
    end,
    MissingFLUs = [FLU || FLU <- AllFLUs, not env_flu_exists(FLU, E)],
    case MissingFLUs of
        []    -> ok;
        [_|_] -> err("Undefined FLU(s) ~p", [MissingFLUs], T)
    end,

    %% All FLUs must not be assigned to another chain.
    AssignedFLUs =
        [case d_find({tmp,{flu_assigned_to,FLU}}, E) of
             error ->
                 [];
             {ok, Ch} when Ch == Name ->
                 [];                            % It's assigned to me already
             {ok, Ch} ->
                 [{flu, FLU, assigned_to_chain, Ch}]
         end || FLU <- AllFLUs],
    case lists:append(AssignedFLUs) of
        [] -> ok;
        As -> err("Some FLUs are assigned to other chains: ~p\n", [As], T)
    end,

    %% If chain already exists, then the consistency mode cannot change.
    case d_find(Key, E) of
        error ->
            ok;
        {ok, C_old} ->
            {chain, _, OldCMode, _, _, _} = C_old,
            if CMode == OldCMode ->
                    ok;
               true ->
                    err("Consistency mode change ~w -> ~w is not permitted\n",
                        [OldCMode, CMode], T)
            end
    end,

    E2 = lists:foldl(fun(FLU, Env) ->
                             d_erase({tmp,{flu_assigned_to,FLU}}, Env)
                     end, E, AllFLUs),
    E3 = lists:foldl(fun(FLU, Env) ->
                             d_store({tmp,{flu_assigned_to,FLU}}, Name, Env)
                     end, E2, AllFLUs),

    %% It's good, let's roll.
    d_store(Key, T, E3);
run_ast_cmd(switch_old_and_new, E) ->
    switch_env_dict(E);
run_ast_cmd(Unknown, _E) ->
    err("Unknown AST thingie", [], Unknown).

make_ast_run_env() ->
    {_KV_old=gb_trees:empty(), _KV_new=gb_trees:empty(), _IsNew=false}.

env_host_exists(Name, E) ->
    Key = {kv,{host,Name}},
    case d_find(Key, E) of
        error ->
            false;
        {ok, _} ->
            true
    end.

env_flu_exists(Name, E) ->
    Key = {kv,{flu,Name}},
    case d_find(Key, E) of
        error ->
            false;
        {ok, _} ->
            true
    end.

get_host_client_interface(HostName, E) ->
    Key = {kv,{host,HostName}},
    case d_find(Key, E) of
        error ->
            false;
        {ok, {host, _Name, _AdminI, ClientI, _Props}} ->
            {ok, ClientI}
    end.

host_port_is_assigned(HostName, Port, {KV_old, KV_new, _}) ->
    L = gb_trees:to_list(KV_old) ++ gb_trees:to_list(KV_new),
    FLU_Ts = [V || {{kv,{flu,_}}, V} <- L],
    case [V || {flu, _Nm, Host_, Port_, _Ps}=V <- FLU_Ts,
               Host_ == HostName, Port_ == Port] of
        [{flu, Name, _Host, _Port, _Ps}] ->
            {true, Name};
        [] ->
            false
    end.

d_find(Key, {KV_old, KV_new, IsNew}) ->
    %% Bah, use 'dict' return value convention.
    case gb_trees:lookup(Key, KV_new) of
        {value, Val} when IsNew ->
            {ok, Val};
        _ ->
            case gb_trees:lookup(Key, KV_old) of
                {value, Val} ->
                    {ok, Val};
                _ ->
                    error
            end
    end.

d_get(Key, {KV_old, KV_new, IsNew}) ->
    %% Bah, use 'dict' return value convention.
    %% Assume key exists, fail if not found.
    case gb_trees:lookup(Key, KV_new) of
        {value, Val} when IsNew ->
            Val;
        _ ->
            case gb_trees:lookup(Key, KV_old) of
                {value, Val} ->
                    Val
            end
    end.

d_store(Key, Val, {KV_old, KV_new, false}) ->
    {gb_trees:enter(Key, Val, KV_old), KV_new, false};
d_store(Key, Val, {KV_old, KV_new, true}) ->
    {KV_old, gb_trees:enter(Key, Val, KV_new), true}.

d_erase({tmp,_}=Key, {KV_old, KV_new, IsNew}) ->
    {gb_trees:delete_any(Key, KV_old), gb_trees:delete_any(Key, KV_new), IsNew}.

switch_env_dict({KV_old, KV_new, false}) ->
    {KV_old, KV_new, true};
switch_env_dict({_, _, true}) ->
    A = switch_old_and_new,
    err("Duplicate ~p", [A], A).

n(L) ->
    lists:sort(L).

err(Fmt, Args, AST) ->
    throw({str(Fmt, Args), AST}).

str(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).

%% We won't allow 'atom' style proplist members: too difficult to normalize.
%% Also, no duplicates, again because normalizing useful for checksums but
%% bad for order-based traditional proplists (first key wins).

is_proplisty(Props) ->
    is_list(Props) andalso
    lists:all(fun({_,_})             -> true;
                 %% nope: (X) when is_atom(X) -> true;
                 (_)                 -> false
              end, Props) andalso
    begin
        Ks = [K || {K,_V} <- Props],
        lists:sort(Ks) == lists:usort(Ks)
    end.

is_stringy(L) ->
    is_list(L) andalso
    lists:all(fun(C) when 33 =< C, C =< 126 -> true;
                 (_)                        -> false
              end, L).

is_porty(Port) ->
    is_integer(Port) andalso 1024 =< Port andalso Port =< 65535.

diff_env({KV_old, KV_new, _IsNew}=E, RelativeHost) ->
    New_list = gb_trees:to_list(KV_new),
    put(final, []),
    Add = fun(X) -> put(final, [X|get(final)]) end,

    %% Find all new FLUs and define them.
    [begin
         {flu, Name, Host, _Port, _Ps} = V,
         if Host == RelativeHost orelse RelativeHost == all ->
                 {ok, P_srvr} = d_find({kv,{p_srvr,Name}}, E),
                 Add(P_srvr),
                 ok;
            true ->
                 ok
         end
     end || {{kv,{flu,Name}}, V} <- New_list],

    %% Find new chains on this host and define them.
    %% Find modified chains on this host and re-define them.
    [begin
         {chain, Name, CMode, FullList, Witnesses, Props} = V,
         FLUsF = [d_get({kv,{flu,FLU}}, E) || FLU <- FullList],
         FLUsW = [d_get({kv,{flu,FLU}}, E) || FLU <- Witnesses],
         TheFLU_Hosts =
             [{FLU, Host} || {flu, FLU, Host, _Port, _Ps} <- FLUsF ++ FLUsW],
         case (lists:keymember(RelativeHost, 2, TheFLU_Hosts)
               orelse RelativeHost == all) of
             true ->
                 Ps_F = [d_get({kv,{p_srvr,FLU}}, E) || FLU <- FullList],
                 Ps_W = [d_get({kv,{p_srvr,FLU}}, E) || FLU <- Witnesses],

                 case gb_trees:lookup({kv,{chain,Name}}, KV_old) of
                     {value, OldT} ->
                         {chain, _, _, OldFull_ss, OldWitnesses_ss, _} = OldT,
                         OldFull = [Str || Str <- OldFull_ss],
                         OldWitnesses = [Str || Str <- OldWitnesses_ss];
                     none ->
                         OldFull = [],
                         OldWitnesses = []
                 end,
                 Run = [FLU || {FLU, Hst} <- TheFLU_Hosts,
                               Hst == RelativeHost
                                   orelse RelativeHost == all,
                               not lists:member(FLU,
                                                OldFull++OldWitnesses)],
                 %% Gaaah, need to find the host for FLUs not present
                 %% in FLUsF ++ FLUsW.
                 OldFLUsF = [d_get({kv,{flu,FLU}}, E) || FLU <- OldFull],
                 OldFLUsW = [d_get({kv,{flu,FLU}}, E) || FLU <- OldWitnesses],
                 OldTheFLU_Hosts =
                     [{FLU, Host} || {flu, FLU, Host, _Port, _Ps} <- OldFLUsF ++ OldFLUsW],
                 %% Yay, now we have the info we need for local FLU Stop list.
                 Stop = [FLU || FLU <- OldFull++OldWitnesses,
                                not (lists:member(FLU, FullList)
                                     orelse
                                     lists:member(FLU, Witnesses)),
                                lists:member({FLU, RelativeHost}, OldTheFLU_Hosts)
                                    orelse RelativeHost == all],
                 PropsExtra = [],
                 %% PropsExtra = [{auto_gen,true}],
                 Add(#chain_def_v1{name=Name,
                                   mode=CMode, full=Ps_F, witnesses=Ps_W,
                                   old_full=OldFull, old_witnesses=OldWitnesses,
                                   local_run=Run, local_stop=Stop,
                                   props=Props ++ PropsExtra}),
                 ok;
             false ->
                 ok
         end
     end || {{kv,{chain,Name}}, V} <- New_list],

    {x, lists:reverse(get(final))}.

make_pending_config(Term) ->
    Dir = get_pending_dir(),
    Blob = io_lib:format("~p.\n", [Term]),
    {A,B,C} = os:timestamp(),
    Path = str("~s/~w.~6..0w",[Dir, A*1000000+B, C]),
    ok = file:write_file(Path, Blob).

%% @doc Check a "quick admin" directory's for sanity
%%
%% This is a quick admin tool, though perhaps "tool" is too big of a word.
%% The meaning of "quick" is closer to "quick &amp; dirty hack".  Any
%% violation of the assumptions of these quick admin functions will result in
%% unspecified behavior, bugs, plagues, and perhaps lost data.
%%
%% Add files in this directory are assumed to have names of the same length
%% (e.g. 4 bytes), ASCII formatted numbers, greater than 0, left-padded with
%% "0".  Each file is assumed to have zero or more AST tuples within, parsable
%% using {ok, ListOfAST_tuples} = file:consult(QuickAdminFile).
%%
%% The largest numbered file is assumed to be all of the AST changes that we
%% want to apply in a single batch.  The AST tuples of all files with smaller
%% numbers will be concatenated together to create the prior history of
%% the cluster.  We assume that all transitions inside these earlier
%% files were actually safe &amp; sane, therefore any sanity problem can only
%% be caused by the contents of the largest numbered file.

quick_admin_sanity_check(File) ->
    try
        {ok, Env} = quick_admin_run_ast(File),
        ok
    catch X:Y ->
            {error, {X,Y, erlang:get_stacktrace()}}
    end.

quick_admin_apply(File, HostName) ->
    try
        {ok, Env} = quick_admin_run_ast(File),
        {_, Cs} = diff_env(Env, HostName),
        [ok = make_pending_config(C) || C <- Cs],
        {PassFLUs, PassChains} = machi_lifecycle_mgr:process_pending(),
        case length(PassFLUs) + length(PassChains) of
            N when N == length(Cs) ->
                ok = quick_admin_add_archive_file(File),
                ok;
            _ ->
                {error, {expected, length(Cs), Cs, got, PassFLUs, PassChains}}
        end
    catch X:Y ->
            {error, {X,Y, erlang:get_stacktrace()}}
    end.


quick_admin_parse_quick(F) ->
    {ok, Terms} = file:consult(F),
    Terms.

quick_admin_run_ast(File) ->
    Prevs = quick_admin_list_archive_files(),
    PrevASTs = lists:append([quick_admin_parse_quick(F) || F <- Prevs]),
    LatestASTs = quick_admin_parse_quick(File),
    {ok, _Env} = run_ast(PrevASTs ++ [switch_old_and_new] ++ LatestASTs).

quick_admin_list_archive_files() ->
    Prevs0 = filelib:wildcard(get_quick_admin_dir() ++ "/*"),
    [Fn || Fn <- Prevs0, base_fn_all_digits(Fn)].

base_fn_all_digits(Path) ->
    Base = filename:basename(Path),
    lists:all(fun is_ascii_digit/1, Base).

is_ascii_digit(X) when $0 =< X, X =< $9 ->
    true;
is_ascii_digit(_) ->
    false.

quick_admin_add_archive_file(File) ->
    Prevs = quick_admin_list_archive_files(),
    N = case [list_to_integer(filename:basename(Fn)) || Fn <- Prevs] of
            [] -> 0;
            Ns -> lists:max(Ns)
        end,
    Dir = get_quick_admin_dir(),
    NewName = str("~s/~6..0w", [Dir, N + 1]),
    {ok, Contents} = file:read_file(File),
    ok = file:write_file(NewName, Contents).
