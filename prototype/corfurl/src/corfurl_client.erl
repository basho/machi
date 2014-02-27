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
                    error_badepoch ->
                        case poll_for_new_epoch_projection(P) of
                            {ok, NewP} ->
                                append_page(NewP, Page, Retries-1);
                            Else ->
                                {Else, P}
                        end;
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
    end;
append_page(P, _Page, _Retries) ->
    {error_badepoch, P}.

append_page2(P, LPN, Page) ->
    case corfurl:write_page(P, LPN, Page) of
        ok ->
            {ok, LPN};
        X when X == error_overwritten; X == error_trimmed ->
            report_lost_race(LPN, X),
            lost_race;
        {special_trimmed, LPN}=XX ->
            XX;
        error_badepoch=XX->
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
    %% TODO: We can proceed if we can seal at least one FLU in
    %%       each chain.  Robustify and sanity check.
    [begin
         _Res = corfurl_flu:seal(FLU, Epoch)
     end || FLU <- lists:reverse(FLUs)],
    case get(goo) of undefined -> put(goo, 0); _Q -> ok end,
    case corfurl_sequencer:start_link(FLUs, TODO_type, SeqName) of
        {ok, Pid} ->
            NewP = P#proj{seq={Pid, node(), SeqName}, epoch=Epoch+1},
            save_projection_or_get_latest(NewP)
            %% case put(goo, get(goo) + 1) of
            %%     N when N < 2 ->
            %%         io:format(user, "hiiiiiiiiiiiiiiiiiiiiiiiiiiiii", []),
            %%         P#proj{seq={Pid, node(), SeqName}, epoch=Epoch};
            %%     _ ->
            %%         save_projection_or_get_latest(NewP)
            %% end
    end.

poll_for_new_epoch_projection(P) ->
    put(silly_poll_counter, 0),
    poll_for_new_epoch_projection(P, get_poll_retries()).

poll_for_new_epoch_projection(P, 0) ->
    %% TODO: The client that caused the seal may have crashed before
    %%       writing a new projection.  We should try to pick up here,
    %%       write a new projection, and bully forward.
    case corfurl:latest_projection_epoch_number(P#proj.dir) of
        Neg when Neg < 0 ->
            error_badepoch;
        Other ->
            exit({bummer, ?MODULE, ?LINE, latest_epoch, Other})
    end;
poll_for_new_epoch_projection(#proj{dir=Dir, epoch=Epoch} = P, Tries) ->
    case corfurl:latest_projection_epoch_number(Dir) of
        NewEpoch when NewEpoch > Epoch ->
            corfurl:read_projection(Dir, NewEpoch);
        _ ->
            timer:sleep(get_poll_sleep_time()),
            case put(silly_poll_counter, get(silly_poll_counter) + 1) div 10*1000 of
                0 -> io:format(user, "P", []);
                _ -> ok
            end,
            poll_for_new_epoch_projection(P, Tries - 1)
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

-ifdef(PULSE).
get_poll_retries() ->
    9999*1000.

get_poll_sleep_time() ->
    1.

-else.
get_poll_retries() ->
    25.

get_poll_sleep_time() ->
    50.

-endif.
