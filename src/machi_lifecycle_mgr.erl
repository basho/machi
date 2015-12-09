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
%% FLUs and chains relative to this local machine.
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
%%       domain and thus managed by outside policy.  This name must be
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
%%       domain and thus managed by outside policy.  This name must be
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
%% == Conflicts with TCP ports, FLU &amp; chain names, etc ==
%%
%% This manager is not responsible for managing conflicts in resource
%% namespaces, e.g., TCP port numbers, FLU names, chain names, etc.
%% Managing these namespaces is external policy's responsibility.

-module(machi_lifecycle_mgr).

-behaviour(gen_server).

-include("machi_projection.hrl").

%% API
-export([start_link/0]).

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

init([]) ->
    self() ! finish_init,
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
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

%%%%%

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
    sanitize_chain_def_records(machi_flu_sup:load_rc_d_files_from_dir(ConfigDir)).

sanitize_chain_def_records(Ps) ->
    {Sane, _} = lists:foldl(fun sanitize_chain_def_rec/2, {[], dict:new()}, Ps),
    Sane.

sanitize_chain_def_rec(Whole, {Acc, D}) ->
    try
        #chain_def_v1{name=Name,
                      mode=Mode,
                      full=Full,
                      witnesses=Witnesses} = Whole,
        true = is_atom(Name),
        NameK = {name, Name},
        error = dict:find(NameK, D),
        true = (Mode == ap_mode orelse Mode == cp_mode),
        IsPSrvr = fun(X) when is_record(X, p_srvr) -> true;
                     (_)                           -> false
                  end,
        true = lists:all(IsPSrvr, Full),
        true = lists:all(IsPSrvr, Witnesses),

        %% All is sane enough.
        D2 = dict:store(NameK, Name, D),
        {[Whole|Acc], D2}
    catch _X:_Y ->
            _ = lager:log(error, self(),
                          "~s: Bad chain_def record (~w ~w), skipping: ~P\n",
                          [?MODULE, _X, _Y, Whole, 15]),
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
            bootstrap_chain(CD, FLU1),
            perhaps_bootstrap_chains(ChainDefs, LocalFLUs_at_zero -- FLUs,
                                    LocalFLUs -- FLUs)
    end.

bootstrap_chain(#chain_def_v1{name=ChainName, mode=CMode, full=Full,
                              witnesses=Witnesses, props=Props}=CD, FLU) ->
    All_p_srvrs = Full ++ Witnesses,
    L = [{Name, P_srvr} || #p_srvr{name=Name}=P_srvr <- All_p_srvrs],
    MembersDict = orddict:from_list(L),
    Mgr = machi_chain_manager1:make_chmgr_regname(FLU),
    case machi_chain_manager1:set_chain_members(Mgr, ChainName, 0, CMode,
                                                MembersDict, Props) of
        ok ->
            ok;
        Else ->
            error_logger:warning("Attempt to bootstrap chain ~w via FLU ~w "
                                 "failed: ~w (defn ~w)\n", [Else, CD]),
            ok
    end.

%% 1. Don't worry about partial writes to config dir: that's a Policy
%%    implementation worry: create a temp file, fsync(2), then rename(2)
%%    and possibly fsync(2) on the dir.
%%
%% 2. Add periodic re-scan check FLUs & chains (in that order).
%%
%% 3. Add force re-rescan of FLUs *or* chains.  (separate is better?)
%%
%% 4. For chain modification: add "change" directory?  Scan looks in
%%    "change"?
%%
%% 5. Easy comparison for current vs. new chain config.
%%    - identify new FLUs: no action required, HC will deal with it?
%%    - identify removed FLUs: set_chain_members() to change HC
%%                           : stop FLU package (after short pause)
%%                           : move config file -> just-in-case
%%                           : move data files -> just-in-case
%%                             - identify j-i-c dir in case resuming from interruption??? to avoid problem of putting config & data files in different places?
%%                             - move data files itself may be interrupted?
%%    - move chain def'n from change -> chain_config_dir

