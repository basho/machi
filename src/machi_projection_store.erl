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

%% @doc The Machi write-once projection store service.
%%
%% This API is gen_server-style message passing, intended for use
%% within a single Erlang node to glue together the projection store
%% server with the node-local process that implements Machi's FLU
%% client access protocol (on the "server side" of the TCP connection).
%%
%% All Machi client access to the projection store SHOULD NOT use this
%% module's API.  Instead, clients should access indirectly via {@link
%% machi_cr_client}, {@link machi_proxy_flu1_client}, or {@link
%% machi_flu1_client}.
%%
%% The projection store is implemented by an Erlang/OTP `gen_server'
%% process that is associated with each FLU.  Conceptually, the
%% projection store is an array of write-once registers.  For each
%% projection store register, the key is a 2-tuple of an epoch number
%% (`non_neg_integer()' type) and a projection type (`public' or
%% `private' type); the value is a projection data structure
%% (`projection_v1()' type).

-module(machi_projection_store).

-include("machi.hrl").
-include("machi_projection.hrl").
-define(V(X,Y), ok).
%% -include("machi_verbose.hrl").

%% -ifdef(PULSE).
%% -compile({parse_transform, pulse_instrument}).
%% -include_lib("pulse_otp/include/pulse_otp.hrl").
%% -endif.

%% API
-export([
         start_link/3,
         get_latest_epochid/2, get_latest_epochid/3,
         read_latest_projection/2, read_latest_projection/3,
         read/3, read/4,
         write/3, write/4,
         get_all_projections/2, get_all_projections/3,
         list_all_projections/2, list_all_projections/3
        ]).
-export([set_wedge_notify_pid/2, get_wedge_notify_pid/1,
         set_consistency_mode/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(NO_EPOCH, ?DUMMY_PV1_EPOCH).

-record(state, {
          public_dir = ""        :: string(),
          private_dir = ""       :: string(),
          wedge_notify_pid       :: pid() | atom(),
          max_public_epochid =  ?NO_EPOCH :: {-1 | non_neg_integer(), binary()},
          max_private_epochid = ?NO_EPOCH :: {-1 | non_neg_integer(), binary()},
          consistency_mode=ap_mode :: 'ap_mode' | 'cp_mode'
         }).

%% @doc Start a new projection store server.
%%
%% The `DataDir' argument should be the same directory as specified
%% for use by our companion FLU data server -- all file system paths
%% used by this server are intended to be stored underneath a common
%% file system parent directory as the FLU data server &amp; sequencer
%% servers.

start_link(RegName, DataDir, NotifyWedgeStateChanges) ->
    gen_server:start_link({local, RegName},
                          ?MODULE, [DataDir, NotifyWedgeStateChanges], []).

%% @doc Fetch the latest epoch number + checksum for type `ProjType'.

get_latest_epochid(PidSpec, ProjType) ->
    get_latest_epochid(PidSpec, ProjType, infinity).

%% @doc Fetch the latest epoch number + checksum for type `ProjType'.
%% projection.

get_latest_epochid(PidSpec, ProjType, Timeout)
  when ProjType == 'public' orelse ProjType == 'private' ->
    g_call(PidSpec, {get_latest_epochid, ProjType}, Timeout).

%% @doc Fetch the latest projection record for type `ProjType'.

read_latest_projection(PidSpec, ProjType) ->
    read_latest_projection(PidSpec, ProjType, infinity).

%% @doc Fetch the latest projection record for type `ProjType'.

read_latest_projection(PidSpec, ProjType, Timeout)
  when ProjType == 'public' orelse ProjType == 'private' ->
    g_call(PidSpec, {read_latest_projection, ProjType}, Timeout).

%% @doc Fetch the projection record type `ProjType' for epoch number `Epoch' .

read(PidSpec, ProjType, Epoch) ->
    read(PidSpec, ProjType, Epoch, infinity).

%% @doc Fetch the projection record type `ProjType' for epoch number `Epoch' .

read(PidSpec, ProjType, Epoch, Timeout)
  when ProjType == 'public' orelse ProjType == 'private',
       is_integer(Epoch), Epoch >= 0 ->
    g_call(PidSpec, {read, ProjType, Epoch}, Timeout).

%% @doc Write the projection record type `ProjType' for epoch number `Epoch' .

write(PidSpec, ProjType, Proj) ->
    write(PidSpec, ProjType, Proj, infinity).

%% @doc Write the projection record type `ProjType' for epoch number `Epoch' .

write(PidSpec, ProjType, Proj, Timeout)
  when ProjType == 'public' orelse ProjType == 'private',
       is_record(Proj, projection_v1),
       is_integer(Proj#projection_v1.epoch_number),
       Proj#projection_v1.epoch_number >= 0 ->
    testing_sleep_perhaps(),
    g_call(PidSpec, {write, ProjType, Proj}, Timeout).

%% @doc Fetch all projection records of type `ProjType'.

get_all_projections(PidSpec, ProjType) ->
    get_all_projections(PidSpec, ProjType, infinity).

%% @doc Fetch all projection records of type `ProjType'.

get_all_projections(PidSpec, ProjType, Timeout)
  when ProjType == 'public' orelse ProjType == 'private' ->
    g_call(PidSpec, {get_all_projections, ProjType}, Timeout).

%% @doc Fetch all projection epoch numbers of type `ProjType'.

list_all_projections(PidSpec, ProjType) ->
    list_all_projections(PidSpec, ProjType, infinity).

%% @doc Fetch all projection epoch numbers of type `ProjType'.

list_all_projections(PidSpec, ProjType, Timeout)
  when ProjType == 'public' orelse ProjType == 'private' ->
    g_call(PidSpec, {list_all_projections, ProjType}, Timeout).

set_wedge_notify_pid(PidSpec, NotifyWedgeStateChanges) ->
    gen_server:call(PidSpec, {set_wedge_notify_pid, NotifyWedgeStateChanges},
                    infinity).

get_wedge_notify_pid(PidSpec) ->
    gen_server:call(PidSpec, {get_wedge_notify_pid},
                    infinity).

set_consistency_mode(PidSpec, CMode)
  when CMode == ap_mode; CMode == cp_mode ->
    gen_server:call(PidSpec, {set_consistency_mode, CMode}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%

g_call(PidSpec, Arg, Timeout) ->
    LC1 = lclock_get(),
    {Res, LC2} = gen_server:call(PidSpec, {Arg, LC1}, Timeout),
    lclock_update(LC2),
    Res.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([DataDir, NotifyWedgeStateChanges]) ->
    lclock_init(),
    PublicDir = machi_util:make_projection_filename(DataDir, "public"),
    PrivateDir = machi_util:make_projection_filename(DataDir, "private"),
    ok = filelib:ensure_dir(PublicDir ++ "/ignored"),
    ok = filelib:ensure_dir(PrivateDir ++ "/ignored"),
    MbEpoch = find_max_epochid(PublicDir),
    %% MbEpoch = {Mb#projection_v1.epoch_number, Mb#projection_v1.epoch_csum},
    MvEpoch = find_max_epochid(PrivateDir),
    %% MvEpoch = {Mv#projection_v1.epoch_number, Mv#projection_v1.epoch_csum},

    {ok, #state{public_dir=PublicDir,
                private_dir=PrivateDir,
                wedge_notify_pid=NotifyWedgeStateChanges,
                max_public_epochid=MbEpoch,
                max_private_epochid=MvEpoch}}.

handle_call({{get_latest_epochid, ProjType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    EpochId = if ProjType == public  -> S#state.max_public_epochid;
                 ProjType == private -> S#state.max_private_epochid
             end,
    {reply, {{ok, EpochId}, LC2}, S};
handle_call({{read_latest_projection, ProjType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {EpochNum, _CSum} = if ProjType == public  -> S#state.max_public_epochid;
                           ProjType == private -> S#state.max_private_epochid
            end,
    {Reply, NewS} = do_proj_read(ProjType, EpochNum, S),
    {reply, {Reply, LC2}, NewS};
handle_call({{read, ProjType, Epoch}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_read(ProjType, Epoch, S),
    {reply, {Reply, LC2}, NewS};
handle_call({{write, ProjType, Proj}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_write(ProjType, Proj, S),
    {reply, {Reply, LC2}, NewS};
handle_call({{get_all_projections, ProjType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    Dir = pick_path(ProjType, S),
    Epochs = find_all(Dir),
    All = [begin
               {{ok, Proj}, _} = do_proj_read(ProjType, Epoch, S),
               Proj
           end || Epoch <- Epochs],
    {reply, {{ok, All}, LC2}, S};
handle_call({{list_all_projections, ProjType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    Dir = pick_path(ProjType, S),
    {reply, {{ok, find_all(Dir)}, LC2}, S};
handle_call({set_wedge_notify_pid, NotifyWedgeStateChanges}, _From, S) ->
    {reply, ok, S#state{wedge_notify_pid=NotifyWedgeStateChanges}};
handle_call({get_wedge_notify_pid}, _From, S) ->
    {reply, {ok, S#state.wedge_notify_pid}, S};
handle_call({set_consistency_mode, CMode}, _From, S) ->
    {reply, ok, S#state{consistency_mode=CMode}};
handle_call(_Request, _From, S) ->
    Reply = {whaaaaaaaaaaaaazz, _Request},
    {reply, Reply, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_proj_read(_ProjType, Epoch, S) when Epoch < 0 ->
    {{error, not_written}, S};
do_proj_read(ProjType, Epoch, S_or_Dir) ->
    Dir = if is_record(S_or_Dir, state) ->
                  pick_path(ProjType, S_or_Dir);
             is_list(S_or_Dir) ->
                  S_or_Dir
          end,
    Path = filename:join(Dir, epoch2name(Epoch)),
    case file:read_file(Path) of
        {ok, Bin} ->
            %% TODO and if Bin is corrupt? (even if binary_to_term() succeeds)
            {{ok, binary_to_term(Bin)}, S_or_Dir};
        {error, enoent} ->
            {{error, not_written}, S_or_Dir};
        {error, Else} ->
            {{error, Else}, S_or_Dir}
    end.

do_proj_write(ProjType, Proj, S) ->
    do_proj_write2(ProjType, Proj, S).

do_proj_write2(ProjType, #projection_v1{epoch_csum=CSum}=Proj, S) ->
    case (machi_projection:update_checksum(Proj))#projection_v1.epoch_csum of
        CSum2 when CSum2 == CSum ->
            do_proj_write3(ProjType, Proj, S);
        _Else ->
            {{error, bad_arg}, S}
    end.

do_proj_write3(ProjType, #projection_v1{epoch_number=Epoch,
                                        epoch_csum=CSum}=Proj, S) ->
    %% TODO: We probably ought to check the projection checksum for sanity, eh?
    Dir = pick_path(ProjType, S),
    Path = filename:join(Dir, epoch2name(Epoch)),
    case file:read_file(Path) of
        {ok, _Bin} when ProjType == public ->
            {{error, written}, S};
        {ok, Bin} when ProjType == private ->
            #projection_v1{epoch_number=CurEpoch,
                           epoch_csum=CurCSum} = _CurProj = binary_to_term(Bin),
            %% We've already checked that CSum is correct matches the
            %% contents of this new projection version.  If the epoch_csum
            %% values match, and if we trust the value on disk (TODO paranoid
            %% check that, also), then the only difference must be the dbg2
            %% list, which is ok.
            if CurEpoch == Epoch, CurCSum == CSum ->
                    do_proj_write4(ProjType, Proj, Path, Epoch, S);
               true ->
                    %% io:format(user, "OUCH: on disk: ~w\n", [machi_projection:make_summary(binary_to_term(Bin))]),
                    %% io:format(user, "OUCH: clobber: ~w\n", [machi_projection:make_summary(Proj)]),
                    %% io:format(user, "OUCH: clobber: ~p\n", [Proj#projection_v1.dbg2]),
                    %% {{error, written, CurEpoch, Epoch, CurCSum, CSum}, S}
                    {{error, written}, S}
            end;
        {error, enoent} ->
            do_proj_write4(ProjType, Proj, Path, Epoch, S);
        {error, Else} ->
            {{error, Else}, S}
    end.

do_proj_write4(ProjType, Proj, Path, Epoch, #state{consistency_mode=CMode}=S) ->
    {{ok, FH}, Epoch, Path} = {file:open(Path, [write, raw, binary]), Epoch, Path},
    ok = file:write(FH, term_to_binary(Proj)),
    ok = file:sync(FH),
    ok = file:close(FH),
    EffectiveProj = Proj,
    EffectiveEpoch = EffectiveProj#projection_v1.epoch_number,
    EpochId = machi_projection:get_epoch_id(Proj),
    EffectiveEpochId = machi_projection:get_epoch_id(EffectiveProj),
    %%
    NewS = if ProjType == public,
              Epoch > element(1, S#state.max_public_epochid) ->
                   if Epoch == EffectiveEpoch ->
                           %% This is a regular projection, i.e.,
                           %% does not have an inner proj.
                           update_wedge_state(
                             S#state.wedge_notify_pid, true,
                             EffectiveEpochId);
                      Epoch /= EffectiveEpoch ->
                           %% This projection has an inner proj.
                           %% The outer proj is flapping, so we do
                           %% not bother wedging.
                           ok
                   end,
                   S#state{max_public_epochid=EpochId};
              ProjType == private,
              Epoch > element(1, S#state.max_private_epochid) ->
                   if CMode == ap_mode ->
                           update_wedge_state(
                             S#state.wedge_notify_pid, false,
                             EffectiveEpochId);
                      true ->
                           %% If ProjType == private and CMode == cp_mode, then
                           %% the unwedge action is not performed here!
                           ok
                   end,
                   S#state{max_private_epochid=EpochId};
              true ->
                   S
           end,
    {ok, NewS}.

update_wedge_state(PidSpec, Boolean, {0,_}=EpochId) ->
    %% Epoch #0 is a special case: no projection has been written yet.
    %% However, given the way that machi_flu_psup starts the
    %% processes, we are roughly 100% certain that the FLU for PidSpec
    %% is not yet running.
    catch machi_flu1:update_wedge_state(PidSpec, Boolean, EpochId);
update_wedge_state(PidSpec, Boolean, EpochId) ->
    %% We have a race problem with the startup order by machi_flu_psup:
    %% the order is projection store (me!), projection manager, FLU.
    %% PidSpec is the FLU.  It's almost certainly a registered name.
    %% Wait for it to exist before sending a message to it.  Racing with
    %% supervisor startup/shutdown/restart is ok.
    ok = wait_for_liveness(PidSpec, 10*1000),
    machi_flu1:update_wedge_state(PidSpec, Boolean, EpochId).

wait_for_liveness(Pid, _WaitTime) when is_pid(Pid) ->
    ok;
wait_for_liveness(PidSpec, WaitTime) ->
    wait_for_liveness(PidSpec, os:timestamp(), WaitTime).

wait_for_liveness(PidSpec, StartTime, WaitTime) ->
    case whereis(PidSpec) of
        undefined ->
            case timer:now_diff(os:timestamp(), StartTime) div 1000 of
                X when X < WaitTime ->
                    timer:sleep(1),
                    wait_for_liveness(PidSpec, StartTime, WaitTime)
            end;
        _SomePid ->
            ok
    end.

pick_path(public, S) ->
    S#state.public_dir;
pick_path(private, S) ->
    S#state.private_dir.

epoch2name(Epoch) ->
    machi_util:int_to_hexstr(Epoch, 32).

name2epoch(Name) ->
    machi_util:hexstr_to_int(Name).

find_all(Dir) ->
    Fs = filelib:wildcard("*", Dir),
    lists:sort([name2epoch(F) || F <- Fs]).

find_max_epochid(Dir) ->
    Fs = lists:sort(filelib:wildcard("*", Dir)),
    if Fs == [] ->
            ?NO_EPOCH;
       true ->
            EpochNum = name2epoch(lists:last(Fs)),
            {{ok, Proj}, _} = do_proj_read(proj_type_ignored, EpochNum, Dir),
            {EpochNum, Proj#projection_v1.epoch_csum}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

lclock_init() ->
    lamport_clock:init().

lclock_get() ->
    lamport_clock:get().

lclock_update(LC) ->
    lamport_clock:update(LC).

testing_sleep_perhaps() ->
    try
        [{_,Max}] = ets:lookup(?TEST_ETS_TABLE, projection_store_sleep_time),
        MSec = random:uniform(Max),
        io:format(user, "{", []),
        timer:sleep(MSec),
        io:format(user, "}", []),
        ok
    catch _X:_Y ->
            ok
    end.

-else.  % TEST

lclock_init() ->
    ok.

lclock_get() ->
    ok.

lclock_update(_LC) ->
    ok.

testing_sleep_perhaps() ->
    ok.

-endif. % TEST
