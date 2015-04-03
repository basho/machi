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

-module(machi_projection_store).

-include("machi_projection.hrl").

%% API
-export([
         start_link/3,
         get_latest_epoch/2, get_latest_epoch/3,
         read/3, read/4,
         write/3, write/4
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          public_dir = ""        :: string(),
          private_dir = ""       :: string(),
          wedged = true          :: boolean(),
          wedge_notify_pid       :: pid() | atom(),
          max_public_epoch = -1  :: -1 | non_neg_integer(),
          max_private_epoch = -1 :: -1 | non_neg_integer()
         }).

start_link(RegName, DataDir, NotifyWedgeStateChanges) ->
    gen_server:start_link({local, RegName},
                          ?MODULE, [DataDir, NotifyWedgeStateChanges], []).

get_latest_epoch(PidSpec, ProjType) ->
    get_latest_epoch(PidSpec, ProjType, infinity).

get_latest_epoch(PidSpec, ProjType, Timeout)
  when ProjType == 'public' orelse ProjType == 'private' ->
    g_call(PidSpec, {get_latest_epoch, ProjType}, Timeout).

read(PidSpec, ProjType, Epoch) ->
    read(PidSpec, ProjType, Epoch, infinity).

read(PidSpec, ProjType, Epoch, Timeout)
  when ProjType == 'public' orelse ProjType == 'private' ->
    g_call(PidSpec, {read, ProjType, Epoch}, Timeout).

write(PidSpec, ProjType, Proj) ->
    write(PidSpec, ProjType, Proj, infinity).

write(PidSpec, ProjType, Proj, Timeout)
  when ProjType == 'public' orelse ProjType == 'private',
       is_record(Proj, projection_v1) ->
    g_call(PidSpec, {write, ProjType, Proj}, Timeout).

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%

g_call(PidSpec, Arg, Timeout) ->
    LC1 = lclock_get(),
    {Res, LC2} = gen_server:call(PidSpec, {Arg, LC1}, Timeout),
    lclock_update(LC2),
    Res.

%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call({{get_latest_epoch, ProjType}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    Epoch = if ProjType == public  -> S#state.max_public_epoch;
               ProjType == private -> S#state.max_private_epoch
            end,
    {reply, {{ok, Epoch}, LC2}, S};
handle_call({{read, ProjType, Epoch}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_read(ProjType, Epoch, S),
    {reply, {Reply, LC2}, NewS};
handle_call({{write, ProjType, Proj}, LC1}, _From, S) ->
    LC2 = lclock_update(LC1),
    {Reply, NewS} = do_proj_write(ProjType, Proj, S),
    {reply, {Reply, LC2}, NewS};
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

do_proj_read(ProjType, Epoch, S) ->
    Dir = pick_path(ProjType, S),
    Path = filename:join(Dir, epoch2name(Epoch)),
    case file:read_file(Path) of
        {ok, Bin} ->
            %% TODO and if Bin is corrupt? (even if binary_to_term() succeeds)
            {{ok, binary_to_term(Bin)}, S};
        {error, enoent} ->
            {{error, not_written}, S};
        {error, Else} ->
            {{error, Else}, S}
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
            {ok, S};
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

find_max_epoch(Dir) ->
    Fs = lists:sort(filelib:wildcard("*", Dir)),
    if Fs == [] ->
            -1;
       true ->
            name2epoch(lists:last(Fs))
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
