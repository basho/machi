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
%% server with the node-local process that implements Machi's TCP
%% client access protocol (on the "server side" of the TCP connection).
%%
%% All Machi client access to the projection store SHOULD NOT use this
%% module's API.
%%
%% The projection store is implemented by an Erlang/OTP `gen_server'
%% process that is associated with each FLU.  Conceptually, the
%% projection store is an array of write-once registers.  For each
%% projection store register, the key is a 2-tuple of an epoch number
%% (`non_neg_integer()' type) and a projection type (`public' or
%% `private' type); the value is a projection data structure
%% (`projection_v1()' type).


-module(machi_projection_store).

-include("machi_projection.hrl").

%% API
-export([
         start_link/3,
         get_latest_epoch/2, get_latest_epoch/3,
         read_latest_projection/2, read_latest_projection/3,
         read/3, read/4,
         write/3, write/4,
         get_all_projections/2, get_all_projections/3,
         list_all_projections/2, list_all_projections/3
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(NO_EPOCH, {-1,<<0:(20*8)/big>>}).

-record(state, {
          public_dir = ""        :: string(),
          private_dir = ""       :: string(),
          wedged = true          :: boolean(),
          wedge_notify_pid       :: pid() | atom(),
          max_public_epoch =  ?NO_EPOCH :: {-1 | non_neg_integer(), binary()},
          max_private_epoch = ?NO_EPOCH :: {-1 | non_neg_integer(), binary()}
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

get_latest_epoch(PidSpec, ProjType) ->
    get_latest_epoch(PidSpec, ProjType, infinity).

%% @doc Fetch the latest epoch number + checksum for type `ProjType'.
%% projection.

get_latest_epoch(PidSpec, ProjType, Timeout)
  when ProjType == 'public' orelse ProjType == 'private' ->
    g_call(PidSpec, {get_latest_epoch, ProjType}, Timeout).

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
    MaxPublicEpoch = find_max_epoch(PublicDir),
    MaxPrivateEpoch = find_max_epoch(PrivateDir),

    {ok, #state{public_dir=PublicDir,
                private_dir=PrivateDir,
                wedged=true,
                wedge_notify_pid=NotifyWedgeStateChanges,
                max_public_epoch=MaxPublicEpoch,
                max_private_epoch=MaxPrivateEpoch}}.

handle_call({{get_latest_epoch, ProjType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    EpochT = if ProjType == public  -> S#state.max_public_epoch;
                ProjType == private -> S#state.max_private_epoch
             end,
    {reply, {{ok, EpochT}, LC2}, S};
handle_call({{read_latest_projection, ProjType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {EpochNum, _CSum} = if ProjType == public  -> S#state.max_public_epoch;
                           ProjType == private -> S#state.max_private_epoch
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
handle_call(_Request, _From, S) ->
    Reply = whaaaaaaaaaaaaa,
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

do_proj_write(ProjType, #projection_v1{epoch_number=Epoch}=Proj, S) ->
    %% TODO: We probably ought to check the projection checksum for sanity, eh?
    Dir = pick_path(ProjType, S),
    Path = filename:join(Dir, epoch2name(Epoch)),
    case file:read_file_info(Path) of
        {ok, _FI} ->
            {{error, written}, S};
        {error, enoent} ->
            {ok, FH} = file:open(Path, [write, raw, binary]),
            ok = file:write(FH, term_to_binary(Proj)),
            ok = file:sync(FH),
            ok = file:close(FH),
            EpochT = {Epoch, Proj#projection_v1.epoch_csum},
            NewS = if ProjType == public,
                      Epoch > element(1, S#state.max_public_epoch) ->
                           %io:format(user, "TODO: tell ~p we are wedged by epoch ~p\n", [S#state.wedge_notify_pid, Epoch]),
                           S#state{max_public_epoch=EpochT, wedged=true};
                      ProjType == private,
                      Epoch > element(1, S#state.max_private_epoch) ->
                           %io:format(user, "TODO: tell ~p we are unwedged by epoch ~p\n", [S#state.wedge_notify_pid, Epoch]),
                           S#state{max_private_epoch=EpochT, wedged=false};
                      true ->
                           S
                   end,
            {ok, NewS};
        {error, Else} ->
            {{error, Else}, S}
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

find_max_epoch(Dir) ->
    Fs = lists:sort(filelib:wildcard("*", Dir)),
    if Fs == [] ->
            ?NO_EPOCH;
       true ->
            EpochNum = name2epoch(lists:last(Fs)),
            {{ok, Proj}, _} = do_proj_read(proj_type_ignored, EpochNum, Dir),
            {EpochNum, Proj}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

lclock_init() ->
    lamport_clock:init().

lclock_get() ->
    lamport_clock:get().

lclock_update(LC) ->
    lamport_clock:update(LC).

-else.  % TEST

lclock_init() ->
    ok.

lclock_get() ->
    ok.

lclock_update(_LC) ->
    ok.

-endif. % TEST
