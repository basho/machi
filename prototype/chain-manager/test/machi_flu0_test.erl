%% -------------------------------------------------------------------
%%
%% Machi: a small village of replicated files
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
-module(machi_flu0_test).

-include("machi.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-ifdef(TEST).
-ifndef(PULSE).

repair_status_test() ->
    {ok, F} = machi_flu0:start_link(one),
    try
        ok = machi_flu0:set_fake_repairing_status(F, true),
        true = machi_flu0:get_fake_repairing_status(F),
        ok = machi_flu0:set_fake_repairing_status(F, false),
        false = machi_flu0:get_fake_repairing_status(F)
    after
        ok = machi_flu0:stop(F)
    end.


concuerror1_test() ->
    ok.

concuerror2_test() ->
    {ok, F} = machi_flu0:start_link(one),
    ok = machi_flu0:stop(F),
    ok.

concuerror3_test() ->
    Me = self(),
    Inner = fun(Name) -> {ok, F1} = machi_flu0:start_link(Name),
                         ok = machi_flu0:stop(F1),
                         Me ! done
            end,
    P1 = spawn(fun() -> Inner(one) end),
    P2 = spawn(fun() -> Inner(two) end),
    [receive done -> ok end || _ <- [P1, P2]],
    
    ok.

concuerror4_test() ->
    event_setup(),
    {ok, F1} = machi_flu0:start_link(one),
    Epoch = 1,
    ok = m_proj_write(F1, Epoch, dontcare),

    Val = <<"val!">>,
    ok = m_write(F1, Epoch, Val),
    {error_stale_projection, Epoch} = m_write(F1, Epoch - 1, Val),

    Me = self(),
    TrimFun = fun() -> Res = m_trim(F1, Epoch),
                       Me ! {self(), Res}
               end,
    TrimPids = [spawn(TrimFun), spawn(TrimFun), spawn(TrimFun)],
    TrimExpected = [error_trimmed,error_trimmed,ok],

    GetFun = fun() -> Res = m_read(F1, Epoch),
                      Me ! {self(), Res}
               end,
    GetPids = [spawn(GetFun)],
    GetExpected = fun(Results) ->
                          [] = [X || X <- Results, X == unwritten],
                          ok
                  end,

    TrimResults = lists:sort([receive
                                  {TrimPid, Res} -> Res
                              end || TrimPid <- TrimPids]),
    TrimExpected = TrimResults,
    GetResults = lists:sort([receive
                                 {GetPid, Res} -> Res
                             end || GetPid <- GetPids]),
    ok = GetExpected(GetResults),

    ok = m_stop(F1),
    ok.
    
proj_store_test() ->
    event_setup(),
    {ok, F1} = machi_flu0:start_link(one),

    error_unwritten = m_proj_get_latest_num(F1),
    error_unwritten = m_proj_read_latest(F1),

    Proj1 = whatever1,
    ok = m_proj_write(F1, 1, Proj1),
    error_written = m_proj_write(F1, 1, Proj1),
    {ok, Proj1} = m_proj_read(F1, 1),
    {ok, 1} = m_proj_get_latest_num(F1),
    {ok, Proj1} = m_proj_read_latest(F1),

    ok = m_stop(F1),
    ok.

wedge_test() ->
    event_setup(),
    {ok, F1} = machi_flu0:start_link(one),
    Epoch1 = 1,
    ok = m_proj_write(F1, Epoch1, dontcare),

    Val = <<"val!">>,
    ok = m_write(F1, Epoch1, Val),
    {error_stale_projection, Epoch1} = m_write(F1, Epoch1 - 1, Val),
    error_wedged = m_write(F1, Epoch1 + 1, Val),
    %% Until we write a newer/bigger projection, all ops are error_wedged
    error_wedged = m_read(F1, Epoch1),
    error_wedged = m_write(F1, Epoch1, Val),
    error_wedged = m_trim(F1, Epoch1),

    Epoch2 = Epoch1 + 1,
    ok = m_proj_write(F1, Epoch2, dontcare),
    {ok, Val} = m_read(F1, Epoch2),
    error_written = m_write(F1, Epoch2, Val),
    ok = m_trim(F1, Epoch2),
    error_trimmed = m_trim(F1, Epoch2),

    ok = m_stop(F1),
    _XX = event_get_all(), %% io:format(user, "XX ~p\n", [_XX]),
    event_shutdown(),
    ok.

proj0_test() ->
    Me = self(),
    event_setup(),
    {ok, F1} = machi_flu0:start_link(one),
    {ok, F2} = machi_flu0:start_link(two),
    FLUs = [F1, F2],
    FirstProj = machi_flu0:make_proj(1, FLUs),
    Epoch1 = FirstProj#proj.epoch,
    [ok = m_proj_write(F, Epoch1, FirstProj) || F <- FLUs],

    Proj0 = machi_flu0:make_proj(-42, FLUs),
    Val = <<"val!">>,
    Pid1 = spawn(fun() ->
                         {ok, _Proj1} = m_append_page(Proj0, Val),
                         Me ! {self(), done}
                 end),
    %% Pids = [Pid1],

    SecondProj = machi_flu0:make_proj(2, FLUs),
    Epoch2 = SecondProj#proj.epoch,
    Pid2 = spawn(fun() ->
                         %% [ok = m_proj_write_with_check(F, Epoch2, SecondProj) ||
                         [case m_proj_write(F, Epoch2, SecondProj) of
                              ok ->
                                  ok;
                              error_written ->
                                  ok
                          end || F <- FLUs],
                         Me ! {self(), done}
                 end),
    Pids = [Pid1, Pid2],

    [receive {Pid, _} -> ok end || Pid <- Pids],

    [ok = m_stop(F) || F <- FLUs],
    _XX = event_get_all(), %%io:format(user, "XX ~p\n", [_XX]),
    event_shutdown(),
    ok.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

m_write(Pid, Epoch1, Val) ->
    Res = machi_flu0:write(Pid, Epoch1, Val),
    event_add(write, Pid, Res),
    Res.

m_read(Pid, Epoch) ->
    Res = machi_flu0:read(Pid, Epoch),
    event_add(get, Pid, Res),
    Res.

m_trim(Pid, Epoch) ->
    Res = machi_flu0:trim(Pid, Epoch),
    event_add(trim, Pid, Res),
    Res.

m_stop(Pid) ->
    Res = machi_flu0:stop(Pid),
    event_add(stop, Pid, Res),
    Res.

m_proj_write(Pid, Epoch, Proj) ->
    Res = machi_flu0:proj_write(Pid, Epoch, private, Proj),
    event_add(proj_write, Pid, Res),
    Res.

m_proj_write_with_check(Pid, Epoch, Proj) ->
    case m_proj_write(Pid, Epoch, Proj) of
        ok ->
            ok;
        error_written ->
            case m_proj_read(Pid, Epoch) of
                {ok, Proj} ->
                    ok;
                {ok, OtherProj} ->
                    {bummer, other_proj, OtherProj};
                Else ->
                    Else
            end
    end.

m_proj_read(Pid, Epoch) ->
    Res = machi_flu0:proj_read(Pid, Epoch, private),
    event_add(proj_read, Pid, Res),
    Res.

m_proj_get_latest_num(Pid) ->
    Res = machi_flu0:proj_get_latest_num(Pid, private),
    event_add(proj_get_latest_num, Pid, Res),
    Res.

m_proj_read_latest(Pid) ->
    Res = machi_flu0:proj_read_latest(Pid, private),
    event_add(proj_read_latest, Pid, Res),
    Res.

m_append_page(Proj, Bytes) ->
    m_append_page(Proj, Bytes, 5).

m_append_page(Proj, _Bytes, 0) ->
    {{error_failed, ?MODULE, ?LINE}, Proj};
m_append_page(Proj, Bytes, Retries) ->
    Retry = fun() ->
                    case poll_for_new_epoch_projection(Proj) of
                        {ok, NewProj} ->
                            m_append_page(NewProj, Bytes, Retries - 1);
                        Else ->
                            {Else, Proj}
                    end
            end,

    case m_append_page2(Proj, Bytes) of
        %% lost_race ->
        %%     m_append_pageQQ(Proj, Bytes, Retries - 1);
        {error_stale_projection, _} ->
            Retry();
        error_wedged ->
            youbetcha = m_repair_projection_store(Proj),
            Retry();
        Else ->
            {Else, Proj}
    end.

m_append_page2(#proj{epoch=Epoch, active=Active}, Bytes) ->
    m_append_page3(Active, Epoch, Bytes).

m_append_page3([], _Epoch, _Bytes) ->
    ok;
m_append_page3([H|T], Epoch, Bytes) ->
    Res =  (catch m_write(H, Epoch, Bytes)),
    case Res of
        ok ->
            m_append_page3(T, Epoch, Bytes);
        error_unwritten ->
            exit({gack, line, ?LINE});
        error_written ->
            case m_read(H, Epoch) of
                {ok, Present} when Present == Bytes ->
                    m_append_page3(T, Epoch, Bytes);
                {error_stale_projection, _}=ESP ->
                    ESP;
                Else ->
                    Else
            end;
        Else ->
            Else
    end.
    %%     L ->
    %%         case [x || {error_stale_projection, _} <- L] of
    %%             [] ->
    %%                 UnwrittenP = lists:member(error_unwritten, L),
    %%                 WrittenP = lists:member(error_written, L),
    %%                 TrimmedP = lists:member(error_trimmed, L),
    %%                 WedgedP = lists:member(error_wedged, L),
    %%                 if UnwrittenP ->
    %%                         error_unwritten;
    %%                    WrittenP ->
    %%                         error_written;
    %%                    TrimmedP ->
    %%                         error_trimmed;
    %%                    WedgedP ->
    %%                         error_wedged;
    %%                    true ->
    %%                         exit({gack, L})
    %%                 end;
    %%             _ ->
    %%                 {error_stale_projection, caller_not_looking_here}
    %%         end
    %% end.

get_poll_retries() ->
    25.

get_poll_sleep_time() ->
    50.

poll_for_new_epoch_projection(P) ->
    poll_for_new_epoch_projection(P, get_poll_retries()).

poll_for_new_epoch_projection(_P, 0) ->
    exit({ouch, ?MODULE, ?LINE});
poll_for_new_epoch_projection(#proj{all=All} = P, Tries) ->
    case multi_call(All, ?MODULE, m_proj_read_latest, []) of
        [] ->
            timer:sleep(get_poll_sleep_time()),
            poll_for_new_epoch_projection(P, Tries - 1);
        L ->
            Answer = lists:last(lists:sort(lists:flatten(L))),
            {ok, Answer}
    end.

multi_call([], _Mod, _Fun, _ArgSuffix) ->
    [];
multi_call([H|T], Mod, Fun, ArgSuffix) ->
    case erlang:apply(Mod,Fun, [H|ArgSuffix]) of
        {ok, X} ->
            [X|multi_call(T, Mod, Fun, ArgSuffix)];
        _ ->
            multi_call(T, Mod, Fun, ArgSuffix)
    end.

m_repair_projection_store(Proj) ->
    [begin
         catch m_proj_write(FLU, Proj#proj.epoch, Proj)
     end || FLU <- Proj#proj.all],
    youbetcha.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

event_setup() ->
    lamport_clock:reset(),
    Tab = ?MODULE,
    ok = event_shutdown(),
    ets:new(Tab, [named_table, ordered_set, public]).

event_shutdown() ->
    Tab = ?MODULE,
    (catch ets:delete(Tab)),
    ok.

event_add(Key, Who, Description) ->
    Tab = ?MODULE,
    E = {lamport_clock:get(), Key, Who, Description},
    %%io:format(user, "E = ~p\n", [E]),
    ets:insert(Tab, E).

event_get_all() ->
    Tab = ?MODULE,
    ets:tab2list(Tab).

-endif. % ! PULSE
-endif.
