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

%% @doc Supervisor for Machi FLU servers and their related support
%% servers.
%%
%% Responsibility for managing FLU and chain lifecycle after the initial
%% application startup is delegated to {@link machi_lifecycle_mgr}.
%%
%% See {@link machi_flu_psup} for an illustration of the entire Machi
%% application process structure.

-module(machi_flu_sup).

-behaviour(supervisor).

-include("machi.hrl").
-include("machi_projection.hrl").
-include("machi_verbose.hrl").

-ifdef(TEST).
-compile(export_all).
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-include_lib("pulse_otp/include/pulse_otp.hrl").
-define(SHUTDOWN, infinity).
-else.
-define(SHUTDOWN, 5000).
-endif.
-endif. %TEST

%% API
-export([start_link/0,
         get_initial_flus/0, load_rc_d_files_from_dir/1,
         sanitize_p_srvr_records/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    _Tab = ets:new(?TEST_ETS_TABLE, [named_table, public, ordered_set,
                                     {read_concurrency,true}]),

    Ps = get_initial_flus(),
    FLU_specs = [machi_flu_psup:make_package_spec(P) || P <- Ps],

    {ok, {SupFlags, FLU_specs}}.

-ifdef(PULSE).
get_initial_flus() ->
    [].
-else.  % PULSE
get_initial_flus() ->
    DoesNotExist = "/tmp/does/not/exist",
    ConfigDir = case application:get_env(machi, flu_config_dir, DoesNotExist) of
                    DoesNotExist ->
                        DoesNotExist;
                    Dir ->
                        Dir
                end,
    Ps = [P || {_File, P} <- load_rc_d_files_from_dir(ConfigDir)],
    sanitize_p_srvr_records(Ps).
-endif. % PULSE

load_rc_d_files_from_dir(Dir) ->
    Files = filelib:wildcard(Dir ++ "/*"),
    [case file:consult(File) of
         {ok, [X]} ->
             {File, X};
         _ ->
             lager:warning("Error parsing file '~s', ignoring",
                           [File]),
             {File, []}
     end || File <- Files].

sanitize_p_srvr_records(Ps) ->
    {Sane, _} = lists:foldl(fun sanitize_p_srvr_rec/2, {[], dict:new()}, Ps),
    Sane.

sanitize_p_srvr_rec(Whole, {Acc, D}) ->
    try
        #p_srvr{name=Name,
                proto_mod=PMod,
                address=Address,
                port=Port,
                props=Props} = Whole,
        true = is_atom(Name),
        NameK = {name, Name},
        error = dict:find(NameK, D),
        true = is_atom(PMod),
        case code:is_loaded(PMod) of
            {file, _} ->
                ok;
            _ ->
                {module, _} = code:load_file(PMod),
                ok
        end,
        if is_list(Address)  -> ok;
           is_tuple(Address) -> ok              % Erlang-style IPv4 or IPv6
        end,
        true = is_integer(Port) andalso Port >= 1024 andalso Port =< 65534,
        PortK = {port, Port},
        error = dict:find(PortK, D),
        true = is_list(Props),

        %% All is sane enough.
        D2 = dict:store(NameK, Name,
                        dict:store(PortK, Port, D)),
        {[Whole|Acc], D2}
    catch _:_ ->
            _ = lager:log(error, self(),
                          "~s: Bad (or duplicate name/port) p_srvr record, "
                          "skipping: ~P\n",
                          [?MODULE, Whole, 15]),
            {Acc, D}
    end.
