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

-export([append_page/2, read_page/2, fill_page/2, trim_page/2, scan_forward/3]).
-export([restart_sequencer/1]).

-include("corfurl.hrl").

-define(LONG_TIME, 5*1000).
%% -define(LONG_TIME, 30*1000).

append_page(Proj, Page) ->
    append_page(Proj, Page, 5).

append_page(Proj, _Page, 0) ->
    {{error_failed, ?MODULE, ?LINE}, Proj};
append_page(#proj{seq={Sequencer,_,_}} = Proj, Page, Retries) ->
    try
        case corfurl_sequencer:get(Sequencer, 1) of
            {ok, LPN} ->
                case append_page2(Proj, LPN, Page) of
                    lost_race ->
                        append_page(Proj, Page, Retries - 1);
                    error_badepoch ->
                        case poll_for_new_epoch_projection(Proj) of
                            {ok, NewProj} ->
                                append_page(NewProj, Page, Retries - 1);
                            Else ->
                                {Else, Proj}
                        end;
                    Else ->
                        {Else, Proj}
                end
        end
    catch
        exit:{Reason,{_gen_server_or_pulse_gen_server,call,[Sequencer|_]}}
          when Reason == noproc; Reason == normal ->
            append_page(restart_sequencer(Proj), Page, Retries);
        exit:Exit ->
            {{error_failed, ?MODULE, ?LINE}, incomplete_code, Exit}
    end.

append_page2(Proj, LPN, Page) ->
    case corfurl:write_page(Proj, LPN, Page) of
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

read_page(Proj, LPN) ->
    retry_loop(Proj, fun(P) -> corfurl:read_page(P, LPN) end, 10).

fill_page(Proj, LPN) ->
    retry_loop(Proj, fun(P) -> corfurl:fill_page(P, LPN) end, 10).

trim_page(Proj, LPN) ->
    retry_loop(Proj, fun(P) -> corfurl:trim_page(P, LPN) end, 10).

scan_forward(Proj, LPN, MaxPages) ->
    %% This is fiddly stuff that I'll get 0.7% wrong if I try to be clever.
    %% So, do something simple and (I hope) obviously correct.
    %% TODO: do something "smarter".
    case corfurl:scan_forward(Proj, LPN, MaxPages) of
        {error_badepoch, _LPN2, _MoreP, _Pages} = Res ->
            case poll_for_new_epoch_projection(Proj) of
                {ok, NewProj} ->
                    {Res, NewProj};
                _Else ->
                    %% TODO: What is the risk of getting caught in a situation
                    %% where we can never make any forward progress when pages
                    %% really are being written?
                    {Res, Proj}
            end;
        Res ->
            {Res, Proj}
    end.

%%%%% %%%%% %%%%% %%%%% %%%%% %%%%% %%%%% %%%%% %%%%% 

retry_loop(Proj, _Fun, 0) ->
    {{error_failed, ?MODULE, ?LINE}, Proj};
retry_loop(Proj, Fun, Retries) ->
    case Fun(Proj) of
        error_badepoch ->
            case poll_for_new_epoch_projection(Proj) of
                {ok, NewProj} ->
                    retry_loop(NewProj, Fun, Retries - 1);
                _Else ->
                    {{error_failed, ?MODULE, ?LINE}, Proj}
            end;
        Else ->
            {Else, Proj}
    end.

restart_sequencer(#proj{epoch=Epoch, dir=Dir} = P) ->
    case corfurl:latest_projection_epoch_number(Dir) of
        N when N > Epoch ->
            %% Yay, someone else has intervened.  Perhaps they've solved
            %% our sequencer problem for us?
            read_latest_projection(P);
        _ ->
            restart_sequencer2(P)
    end.

restart_sequencer2(#proj{seq={OldSequencer, _SeqHost, SeqName},
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
    case corfurl_sequencer:start_link(FLUs, TODO_type, SeqName) of
        {ok, Pid} ->
            NewP = P#proj{seq={Pid, node(), SeqName}, epoch=Epoch+1},
            save_projection_or_get_latest(NewP)
    end.

poll_for_new_epoch_projection(P) ->
    put(silly_poll_counter, 0),
    poll_for_new_epoch_projection(P, get_poll_retries()).

poll_for_new_epoch_projection(P, 0) ->
    %% TODO: The client that caused the seal may have crashed before
    %%       writing a new projection.  We should try to pick up here,
    %%       write a new projection, and bully forward.
    %%       NOTE: When that new logic is added, the huge polling interval
    %%       that PULSE uses should be reduced to something tiny.
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
            read_latest_projection(P)
    end.

read_latest_projection(#proj{dir=Dir}) ->
    NewEpoch = corfurl:latest_projection_epoch_number(Dir),
    {ok, NewP} = corfurl:read_projection(Dir, NewEpoch),
    NewP.

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
