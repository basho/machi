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

%% @doc Machi FLU1 append serialization server process

-module(machi_flu1_append_server).

-behavior(gen_server).

-include("machi.hrl").
-include("machi_projection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

-export([start_link/4]).

-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([int_update_wedge_state/3, int_wedge_myself/2]).
-export([current_state/1, format_state/1]).

-record(state, {
          flu_name        :: atom(),
          witness = false :: boolean(),
          wedged = true   :: boolean(),
          etstab          :: ets:tid(),
          epoch_id        :: 'undefined' | machi_dt:epoch_id()
         }).

-define(INIT_TIMEOUT, 60*1000).
-define(CALL_TIMEOUT, 60*1000).

-spec start_link(pv1_server(), boolean(), boolean(),
                 undefined | machi_dt:epoch_id()) -> {ok, pid()}.
start_link(Fluname, Witness_p, Wedged_p, EpochId) ->
    %% Reminder: Name is the "main" name of the FLU, i.e., no suffix
    gen_server:start_link({local, Fluname},
                          ?MODULE, [Fluname, Witness_p, Wedged_p, EpochId],
                          [{timeout, ?INIT_TIMEOUT}]).

-spec current_state(atom() | pid()) -> term().
current_state(PidSpec) ->
    gen_server:call(PidSpec, current_state, ?CALL_TIMEOUT).

format_state(State) ->
    Fields = record_info(fields, state),
    [_Name | Values] = tuple_to_list(State),
    lists:zip(Fields, Values).

int_update_wedge_state(PidSpec, Boolean, EpochId)
  when is_boolean(Boolean), is_tuple(EpochId) ->
    gen_server:cast(PidSpec, {wedge_state_change, Boolean, EpochId}).

int_wedge_myself(PidSpec, EpochId)
  when is_tuple(EpochId) ->
    gen_server:cast(PidSpec, {wedge_myself, EpochId}).

init([Fluname, Witness_p, Wedged_p, EpochId]) ->
    TID = ets:new(machi_flu1:ets_table_name(Fluname),
                  [set, protected, named_table, {read_concurrency, true}]),
    ets:insert(TID, {epoch, {Wedged_p, EpochId}}),
    {ok, #state{flu_name=Fluname, witness=Witness_p, wedged=Wedged_p,
                etstab=TID, epoch_id=EpochId}}.

handle_call({seq_append, _From2, _NSInfo, _EpochID, _Prefix, _Chunk, _TCSum, _Opts},
            _From, #state{witness=true}=S) ->
    %% The FLU's machi_flu1_net_server process ought to filter all
    %% witness states, but we'll keep this clause for extra
    %% paranoia.
    {reply, witness, S};
handle_call({seq_append, _From2, _NSInfo, _EpochID, _Prefix, _Chunk, _TCSum, _Opts},
            _From, #state{wedged=true}=S) ->
    {reply, wedged, S};
handle_call({seq_append, _From2, NSInfo, EpochID,
             Prefix, Chunk, TCSum, Opts},
            From, #state{flu_name=FluName, epoch_id=OldEpochId}=S) ->
    %% Old is the one from our state, plain old 'EpochID' comes
    %% from the client.
    _ = case OldEpochId of
            EpochID ->
                spawn(fun() ->
                              append_server_dispatch(From, NSInfo,
                                                     Prefix, Chunk, TCSum, Opts,
                                                     FluName, EpochID)
                      end),
                {noreply, S};
            _ ->
                {reply, {error, bad_epoch}, S}
        end;
%% TODO: Who sends this message?
handle_call(wedge_status, _From,
            #state{wedged=Wedged_p, epoch_id=EpochId} = S) ->
    {reply, {wedge_status_reply, Wedged_p, EpochId}, S};
handle_call(current_state, _From, S) ->
    {reply, S, S};
handle_call(Else, From, S) ->
    io:format(user, "~s:handle_call: WHA? from=~w ~w\n", [?MODULE, From, Else]),
    {noreply, S}.

handle_cast({wedge_myself, WedgeEpochId},
            #state{flu_name=FluName, wedged=Wedged_p, epoch_id=OldEpochId}=S) ->
    if not Wedged_p andalso WedgeEpochId == OldEpochId ->
            true = ets:insert(S#state.etstab,
                              {epoch, {true, OldEpochId}}),
            %% Tell my chain manager that it might want to react to
            %% this new world.
            Chmgr = machi_chain_manager1:make_chmgr_regname(FluName),
            spawn(fun() ->
                          catch machi_chain_manager1:trigger_react_to_env(Chmgr)
                  end),
            {noreply, S#state{wedged=true}};
       true ->
            {noreply, S}
    end;
handle_cast({wedge_state_change, Boolean, {NewEpoch, _}=NewEpochId},
            #state{epoch_id=OldEpochId}=S) ->
    OldEpoch = case OldEpochId of {OldE, _} -> OldE;
                   undefined -> -1
               end,
    if NewEpoch >= OldEpoch ->
            true = ets:insert(S#state.etstab,
                              {epoch, {Boolean, NewEpochId}}),
            {noreply, S#state{wedged=Boolean, epoch_id=NewEpochId}};
       true ->
            {noreply, S}
    end;
handle_cast(Else, S) ->
    io:format(user, "~s:handle_cast: WHA? ~p\n", [?MODULE, Else]),
    {noreply, S}.

handle_info(Else, S) ->
    io:format(user, "~s:handle_info: WHA? ~p\n", [?MODULE, Else]),
    {noreply, S}.

terminate(normal, _S) ->
    ok;
terminate(Reason, _S) ->
    lager:warning("~s:terminate: ~w", [?MODULE, Reason]),
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

append_server_dispatch(From, NSInfo,
                       Prefix, Chunk, TCSum, Opts, FluName, EpochId) ->
    Result = case handle_append(NSInfo,
                                Prefix, Chunk, TCSum, Opts, FluName, EpochId) of
        {ok, File, Offset} ->
            {assignment, Offset, File};
        Other ->
            Other
    end,
    _ = gen_server:reply(From, Result),
    ok.

handle_append(NSInfo,
              Prefix, Chunk, TCSum, Opts, FluName, EpochId) ->
    Res = machi_flu_filename_mgr:find_or_make_filename_from_prefix(
            FluName, EpochId, {prefix, Prefix}, NSInfo),
    case Res of
        {file, F} ->
            case machi_flu_metadata_mgr:start_proxy_pid(FluName, {file, F}) of
                {ok, Pid} ->
                    {Tag, CS} = machi_util:unmake_tagged_csum(TCSum),
                    Meta = [{client_csum_tag, Tag}, {client_csum, CS}],
                    Extra = Opts#append_opts.chunk_extra,
                    machi_file_proxy:append(Pid, Meta, Extra, Chunk);
                {error, trimmed} = E ->
                    E
            end;
        Error ->
            Error
    end.
