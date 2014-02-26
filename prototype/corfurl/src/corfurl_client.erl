%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(corfurl_client).

-export([append_page/2]).
-export([restart_sequencer/1]).

-include("corfurl.hrl").

-define(LONG_TIME, 5*1000).
%% -define(LONG_TIME, 30*1000).

append_page(P, Page) ->
    append_page(P, Page, 1).

append_page(#proj{seq={Sequencer,_,_}} = P, Page, Retries)
  when Retries < 50 ->
    try
        case corfurl_sequencer:get(Sequencer, 1) of
            {ok, LPN} ->
                case append_page2(P, LPN, Page) of
                    lost_race ->
                        append_page(P, Page, Retries - 1);
                    Else ->
                        {Else, P}
                end
        end
    catch
        exit:{Reason,{_gen_server_or_pulse_gen_server,call,[Sequencer|_]}}
          when Reason == noproc; Reason == normal ->
            append_page(restart_sequencer(P), Page, Retries);
        exit:Exit ->
            {failed, incomplete_code, Exit}
    end.

append_page2(P, LPN, Page) ->
    case corfurl:write_page(P, LPN, Page) of
        ok ->
            {ok, LPN};
        X when X == error_overwritten; X == error_trimmed ->
            report_lost_race(LPN, X),
            lost_race;
        {special_trimmed, LPN}=XX ->
            XX
            %% Let it crash: error_unwritten
    end.

restart_sequencer(#proj{seq={OldSequencer, _SeqHost, SeqName},
                        epoch=Epoch, r=Ranges} = P) ->
    spawn(fun() ->
                  (catch corfurl_sequencer:stop(OldSequencer))
          end),
    TODO_type = standard,                       % TODO: fix this hard-coding
    FLUs = lists:usort(
             [FLU || R <- Ranges,
                     C <- tuple_to_list(R#range.chains), FLU <- C]),
    case corfurl_sequencer:start_link(FLUs, TODO_type, SeqName) of
        {ok, Pid} ->
            NewP = P#proj{seq={Pid, node(), SeqName}, epoch=Epoch+1},
            save_projection_or_get_latest(NewP)
    end.

save_projection_or_get_latest(#proj{dir=Dir} = P) ->
    case corfurl:save_projection(Dir, P) of
        ok ->
            P;
        error_overwritten ->
            NewEpoch = corfurl:latest_projection_epoch_number(Dir),
            {ok, NewP} = corfurl:read_projection(Dir, NewEpoch),
            NewP
    end.

-ifdef(TEST).
-ifdef(PULSE).
report_lost_race(_LPN, _Reason) ->
    %% It's interesting (sometime?) to know if a page was overwritten
    %% because the sequencer was configured by QuickCheck to hand out
    %% duplicate LPNs.  If this gets too annoying, this can be a no-op
    %% function.
    io:format(user, "o", []).
-else.  % PULSE
report_lost_race(LPN, Reason) ->
    io:format(user, "LPN ~p race lost: ~p\n", [LPN, Reason]).
-endif. % PULSE
-else.  % TEST

report_lost_race(LPN, Reason) ->
    %% Perhaps it's an interesting event, but the rest of the system
    %% should react correctly whenever this happens, so it shouldn't
    %% ever cause an external consistency problem.
    error_logger:debug_msg("LPN ~p race lost: ~p\n", [LPN, Reason]).

-endif. % TEST
