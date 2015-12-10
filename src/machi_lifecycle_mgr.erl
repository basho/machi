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
%% FLUs on the local machine may be started and stopped, as defined by
%% administrative policy.  In order to do any useful work, however, a
%% running FLU must also be configured to be a member of a replication
%% chain.  Thus, as a practical matter, both a FLU and the chain that
%% the FLU participates in must both be managed by this manager.
%%
%% When a new `rc.d'-style config file is written to the FLU
%% definition directory, a Machi server process will discover the file
%% within a certain period of time, e.g. 15 seconds.  The FLU will be
%% started with the file's specified parameters.  A FLU should be
%% defined and started before configuring its chain membership.
%%
%% Usually a FLU is removed implicitly by removing that FLU from the a
%% newer definition file for the chain, or by deleting the entire
%% chain definition.  If a FLU has been started but never been a chain
%% member, then the FLU can be stopped &amp; removed explicitly.
%%
%% When a FLU has been removed by policy, the FLU's data files are set
%% aside into a temporary area.  An additional policy command may be
%% used to permanently delete such FLUs' data files, i.e. to reclaim
%% disk space.
%%
%% Resources for the FLU are defined in `machi_projection.hrl'
%% in the `p_srvr{}' record.  The major elements of this record are:
%%
%% <ul>
%%
%%  <li> <tt>name :: atom()</tt>: The name of the FLU.  This name
%%       should be unique over the lifetime of the administrative
%%       domain and thus managed by external policy.  This name must be
%%       the same as the name of the `rc.d'-style config file that
%%       defines the FLU.
%% </li>
%%  <li> <tt>address :: string()</tt>: The DNS hostname or IP address
%%       used by other servers to communicate with this FLU.
%% </li>
%%  <li> <tt>port :: non_neg_integer() </tt>: The TCP port number that
%%       the FLU listens to for incoming Protocol Buffers-serialized
%%       communication.
%% </li>
%%  <li> <tt>props :: property_list()</tt>: A general-purpose property
%%       list.  Its use is currently fluid &amp; not well-defined yet.
%% </li>
%% </ul>
%%
%% A FLU may be created or removed (via implicit or explicit policy).
%% An existing FLU may not be reconfigured.
%%
%% == The Chain Lifecycle ==
%%
%% If a FLU on the local machine is expected to participate in a
%% replication chain, then an `rc.d'-style chain definition file must
%% also be present on each machine that runs a FLU in the chain.
%%
%% Machi's chains are self-managing, via Humming Consensus; see the
%% [https://github.com/basho/machi/tree/master/doc/] directory for
%% much more detail about Humming Consensus.  After FLUs have received
%% their initial chain configuration for Humming Consensus, the FLUs
%% will manage each other (and the chain) themselves.
%%
%% However, Humming Consensus does not handle three chain management
%% problems: 1. specifying the very first chain configuration,
%% 2. altering the membership of the chain (adding/removing FLUs from
%% the chain), or 3. stopping the chain permanently.
%%
%% FLUs in a new chain should have definition files created on each
%% FLU's respective machine prior to defining their chain.  Similarly,
%% on each machine that hosts a chain member, a chain definition file
%% created.  External policy is responsible for creating each of these
%% files.
%%
%% Resources for the chain are defined in `machi_projection.hrl'
%% in the `chain_def_v1{}' record.  The major elements of this record are:
%%
%% <ul>
%%
%%  <li> <tt>name :: atom()</tt>: The name of the chain.  This name
%%       should be unique over the lifetime of the administrative
%%       domain and thus managed by external policy.  This name must be
%%       the same as the name of the `rc.d'-style config file that
%%       defines the chain.
%% </li>
%%  <li> <tt>mode :: 'ap_mode' | 'cp_mode'</tt>: This is the consistency
%%       to be used for managing the chain's replicated data: eventual
%%       consistency and strong consistency, respectively.
%% </li>
%%  <li> <tt>full :: [#p_srvr{}] </tt>: A list of `#p_srvr{}' records
%%       to define the full-service members of the chain.
%% </li>
%%  <li> <tt>witnesses :: [#p_srvr{}] </tt>: A list of `#p_srvr{}' records
%%       to define the witness-only members of the chain.  Witness servers
%%       may only be used with strong consistency mode.
%% </li>
%%  <li> <tt>props :: property_list()</tt>: A general-purpose property
%%       list.  Its use is currently fluid &amp; not well-defined yet.
%% </li>
%% </ul>
%%
%% A chain may be created, removed, or modified.
%%
%% A modification request writes a `#chain_def_v1{}' record with the
%% same name but different `full' and/or `witnesses' list to each
%% machine that hosts a FLU in the chain (in both the old and new
%% versions of the chain).
%%
%% A deletion request writes a `#chain_def_v1{}' record with the same
%% name but empty lists for `full' and `witnesses' to each machine
%% that hosts a FLU in the chain (in the old version of the chain).
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
    DoesNotExist = "/tmp/does/not/exist",
    ConfigDir = case application:get_env(machi, chain_config_dir, DoesNotExist) of
                    DoesNotExist ->
                        DoesNotExist;
                    Dir ->
                        ok = filelib:ensure_dir(Dir ++ "/unused"),
                        Dir
                end,
    CDefs = [CDef || {_File, CDef} <- machi_flu_sup:load_rc_d_files_from_dir(
                                        ConfigDir)],
    sanitize_chain_def_records(CDefs).

sanitize_chain_def_records(Ps) ->
    {Sane, _} = lists:foldl(fun sanitize_chain_def_rec/2, {[], dict:new()}, Ps),
    Sane.

sanitize_chain_def_rec(Whole, {Acc, D}) ->
    try
        #chain_def_v1{name=Name,
                      mode=Mode,
                      full=Full,
                      witnesses=Witnesses} = Whole,
        {true, 10} = {is_atom(Name), 10},
        NameK = {name, Name},
        {error, 20} = {dict:find(NameK, D), 20},
        {true, 30} = {(Mode == ap_mode orelse Mode == cp_mode), 30},
        IsPSrvr = fun(X) when is_record(X, p_srvr) -> true;
                     (_)                           -> false
                  end,
        {true, 40} = {lists:all(IsPSrvr, Full), 40},
        {true, 50} = {lists:all(IsPSrvr, Witnesses), 50},

        %% All is sane enough.
        D2 = dict:store(NameK, Name, D),
        {[Whole|Acc], D2}
    catch X:Y ->
            lager:error("~s: Bad chain_def record (~w ~w), skipping: ~P\n",
                        [?MODULE, X, Y, Whole, 15]),
            {Acc, D}
    end.

perhaps_bootstrap_chains([], LocalFLUs_at_zero, LocalFLUs) ->
    if LocalFLUs == [] ->
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
                               old_all=ReqOldAll, old_witnesses=ReqOldWitnesses,
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
                           ReqOldAll, ReqOldWitnesses,
                           OldAll_list, OldWitnesses,
                           NewAll_list, Witnesses,
                           Mgr, NewChainName, OldEpoch,
                           NewCMode, MembersDict, NewWitnesses_list) of
        ok ->
            lager:info("Bootstrapped chain ~w via FLU ~w to "
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
            lager:error("Attempt to bootstrap chain ~w via FLU ~w "
                        "failed: ~w (defn ~w)\n",
                        [NewChainName, FLU, Else, CD]),
            timer:sleep(555),
            bootstrap_chain2(CD, FLU, N-1)
    end.

set_chain_members(OldChainName, NewChainName, OldCMode,
                  ReqOldAll, ReqOldWitnesses,
                  OldAll_list, OldWitnesses, NewAll_list, NewWitnesses,
                  Mgr, ChainName, OldEpoch, NewCMode, MembersDict, _Props) ->
    if OldChainName == NewChainName, OldCMode == NewCMode,
       OldAll_list == NewAll_list, OldWitnesses == NewWitnesses ->
            %% The chain's current definition at this FLU is already what we
            %% want.  Let's pretend that we sent the command and that it was
            %% successful.
            ok;
       OldEpoch == 0 orelse (OldChainName == NewChainName andalso
                             OldCMode == NewCMode andalso
                             ReqOldAll == OldAll_list andalso
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
    PendingDir = get_pending_dir(S),
    %% PendingFiles = get_pending_files(PendingDir, S),
    PendingParsed = machi_flu_sup:load_rc_d_files_from_dir(PendingDir),
    P_FLUs = [X || {_File, #p_srvr{}}=X <- PendingParsed],
    P_Chains = [X || {_File, #chain_def_v1{}}=X <- PendingParsed],
    BadFiles = [File || {File, []} <- PendingParsed],
    S2 = process_pending_flus(P_FLUs, S),
    S3 = process_pending_chains(P_Chains, S2),
    S4 = process_bad_files(BadFiles, S3),
    {{P_FLUs, P_Chains}, S4}.

flu_config_exists(FLU, S) ->
    ConfigDir = get_flu_config_dir(S),
    case file:read_file_info(ConfigDir ++ "/" ++ atom_to_list(FLU)) of
        {ok, _} ->
            true;
        _ ->
            false
    end.

get_pending_dir(_S) ->
    {ok, EtcDir} = application:get_env(machi, platform_etc_dir),
    EtcDir ++ "/pending".

get_rejected_dir(_S) ->
    {ok, EtcDir} = application:get_env(machi, platform_etc_dir),
    EtcDir ++ "/rejected".

get_flu_config_dir(_S) ->
    {ok, Dir} = application:get_env(machi, flu_config_dir),
    Dir.

get_flu_data_dir(_S) ->
    {ok, Dir} = application:get_env(machi, flu_data_dir),
    Dir.

get_chain_config_dir(_S) ->
    {ok, Dir} = application:get_env(machi, chain_config_dir),
    Dir.

get_data_dir(_S) ->
    {ok, Dir} = application:get_env(machi, platform_data_dir),
    Dir.

get_preserve_dir(S) ->
    get_data_dir(S) ++ "/^PRESERVE".

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
            case machi_flu_psup:start_flu_package(P) of
                {ok, SupPid} ->
                    lager:info("Started FLU ~w with supervisor pid ~p\n",
                               [FLU, SupPid]),
                    _ = move_to_flu_config(FLU, File, S),
                    S;
                Else ->
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
                               flu_config_exists(FLU, S)],
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
                 ConfigDir = get_flu_config_dir(S),
                 FluDataDir = get_flu_data_dir(S),
                 PreserveDir = get_preserve_dir(S),
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
                 _ = file:rename(Src2, Dst2),
                 ok
             end || FLU <- LocalRemovedFLUs]
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
    Dst = get_rejected_dir(S),
    Suffix = make_ts_suffix(),
    ok = file:rename(File, Dst ++ "/" ++ filename:basename(File) ++ Suffix),
    S.

make_ts_suffix() ->
    lists:flatten(io_lib:format("~w,~w,~w", tuple_to_list(os:timestamp()))).

move_to_flu_config(FLU, File, S) ->
    lager:info("Creating FLU config file ~w\n", [FLU]),
    Dst = get_flu_config_dir(S),
    ok = file:rename(File, Dst ++ "/" ++ atom_to_list(FLU)),
    S.

move_to_chain_config(Name, File, S) ->
    lager:info("Creating chain config file ~w\n", [Name]),
    Dst = get_chain_config_dir(S),
    ok = file:rename(File, Dst ++ "/" ++ atom_to_list(Name)),
    S.

delete_chain_config(Name, File, S) ->
    lager:info("Deleting chain config file ~s for chain ~w\n", [File, Name]),
    Dst = get_chain_config_dir(S),
    ok = file:delete(Dst ++ "/" ++ atom_to_list(Name)),
    S.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_ast_tuple_syntax(Ts) ->
    lists:partition(fun check_a_ast_tuple/1, Ts).

check_a_ast_tuple({host, Name, Props}) ->
    is_stringy(Name) andalso is_proplisty(Props) andalso
    lists:all(fun({admin_interface, X})  -> is_stringy(X);
                 ({client_interface, X}) -> is_stringy(X);
                 (_)                     -> false
              end, Props);
check_a_ast_tuple({flu, Name, HostName, Port, Props}) ->
    is_stringy(Name) andalso is_stringy(HostName) andalso
        is_porty(Port) andalso is_proplisty(Props);
check_a_ast_tuple({chain, Name, AddList, RemoveList, Props}) ->
    is_stringy(Name) andalso
    lists:all(fun is_stringy/1, AddList) andalso
    lists:all(fun is_stringy/1, RemoveList) andalso
        is_proplisty(Props);
check_a_ast_tuple(_) ->
    false.

is_proplisty(Props) ->
    is_list(Props) andalso
    lists:all(fun({_,_})             -> true;
                 (X) when is_atom(X) -> true;
                 (_)                 -> false
              end, Props).

is_stringy(L) ->
    is_list(L) andalso
    lists:all(fun(C) when 33 =< C, C =< 126 -> true;
                 (_)                        -> false
              end, L).

is_porty(Port) ->
    is_integer(Port) andalso 1024 =< Port andalso Port =< 65535.
