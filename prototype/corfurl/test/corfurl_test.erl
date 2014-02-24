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

-module(corfurl_test).

-include("corfurl.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(M, corfurl).

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%


setup_flu_basedir() ->
    "./tmp." ++
        atom_to_list(?MODULE) ++ "." ++ os:getpid() ++ ".".

setup_flu_dir(N) ->
    setup_flu_basedir() ++ integer_to_list(N).

setup_del_all(NumFLUs) ->
    [ok = corfurl_util:delete_dir(setup_flu_dir(N)) ||
        N <- lists:seq(1, NumFLUs)].

setup_basic_flus(NumFLUs, PageSize, NumPages) ->
    setup_del_all(NumFLUs),
    [begin
         element(2, corfurl_flu:start_link(setup_flu_dir(X),
                                   PageSize, NumPages * (PageSize * ?PAGE_OVERHEAD)))
     end || X <- lists:seq(1, NumFLUs)].

-ifndef(PULSE).

save_read_test() ->
    Dir = "/tmp/" ++ atom_to_list(?MODULE) ++".save-read",
    Chain = [a,b],
    P1 = ?M:new_simple_projection(1, 1, 1*100, [Chain]),

    try
        filelib:ensure_dir(Dir ++ "/ignored"),
        ok = ?M:save_projection(Dir, P1),
        error_overwritten = ?M:save_projection(Dir, P1),

        {ok, P1} = ?M:read_projection(Dir, 1),
        error_unwritten = ?M:read_projection(Dir, 2),

        ok
    after
        ok = corfurl_util:delete_dir(Dir)
    end.

smoke1_test() ->
    NumFLUs = 6,
    PageSize = 8,
    NumPages = 10,
    FLUs = [F1, F2, F3, F4, F5, F6] =
        setup_basic_flus(NumFLUs, PageSize, NumPages),
    {ok, Seq} = corfurl_sequencer:start_link(FLUs),

    %% We know that the first LPN will be 1.
    LPN_Pgs = [{X, list_to_binary(
                     lists:flatten(io_lib:format("~8..0w", [X])))} ||
                  X <- lists:seq(1, 5)],
    try
        P1 = ?M:new_simple_projection(1, 1, 1*100, [[F1, F2, F3], [F4, F5, F6]]),
        [begin {ok, LPN} = ?M:append_page(Seq, P1, Pg) end || {LPN, Pg} <- LPN_Pgs],

        [begin {ok, Pg} = ?M:read_page(P1, LPN) end || {LPN, Pg} <- LPN_Pgs],

        [begin
             LPNplus = LPN + 1,
             {ok, LPNplus, true, [{LPN, Pg}]} = ?M:scan_forward(P1, LPN, 1)
         end || {LPN, Pg} <- LPN_Pgs],
        {ok, 6, false, []} = ?M:scan_forward(P1, 6, 1),
        {ok, 6, false, []} = ?M:scan_forward(P1, 6, 10),
        [{LPN1,Pg1}, {LPN2,Pg2}, {LPN3,Pg3}, {LPN4,Pg4}, {LPN5,Pg5}] = LPN_Pgs,
        {ok, 4, true, [{LPN2,Pg2}, {LPN3,Pg3}]} = ?M:scan_forward(P1, 2, 2),
        {ok, 6, false, [{LPN3,Pg3}, {LPN4,Pg4}, {LPN5,Pg5}]} =
            ?M:scan_forward(P1, 3, 10),

        %% Let's smoke read-repair: regular write failure
        Epoch = P1#proj.epoch,
        Pg6 = <<424242:(PageSize*8)>>,

        %% Simulate a failed write to the chain.
        [F6a, F6b, F6c] = Chain6 = ?M:project_to_chain(6, P1),
        NotHead6 = [F6b, F6c],
        ok = ?M:write_single_page_to_chain([F6a], Epoch, 6, Pg6, 1),

        %% Does the chain look as expected?
        {ok, Pg6} = corfurl_flu:read(?M:flu_pid(F6a), Epoch, 6),
        [error_unwritten = corfurl_flu:read(?M:flu_pid(X), Epoch, 6) ||
            X <- NotHead6],

        %% Read repair should fix it.
        {ok, Pg6} = ?M:read_page(P1, 6),
        [{ok, Pg6} = corfurl_flu:read(?M:flu_pid(X), Epoch, 6) || X <- Chain6],

        %% Let's smoke read-repair: failed fill
        [F7a, F7b, F7c] = Chain7 = ?M:project_to_chain(7, P1),
        NotHead7 = [F7b, F7c],
        ok = corfurl_flu:fill(?M:flu_pid(F7a), Epoch, 7),

        %% Does the chain look as expected?
        error_trimmed = corfurl_flu:read(?M:flu_pid(F7a), Epoch, 7),
        [error_unwritten = corfurl_flu:read(?M:flu_pid(X), Epoch, 7) ||
            X <- NotHead7],

        %% Read repair should fix it.
        error_trimmed = ?M:read_page(P1, 7),
        [error_trimmed = corfurl_flu:read(?M:flu_pid(X), Epoch, 7) || X <- Chain7],
        %% scan_forward shouldn't see it either
        {ok, 8, false, [{6,Pg6}]} = ?M:scan_forward(P1, 6, 10),

        [F8a|_] = Chain8 = ?M:project_to_chain(8, P1),
        ok = corfurl_flu:fill(?M:flu_pid(F8a), Epoch, 8),
        %% No read before scan, scan_forward shouldn't see 8 either,
        %% but the next seq should be 9
        {ok, 9, false, [{6,Pg6}]} = ?M:scan_forward(P1, 6, 10),

        ok
    after
        corfurl_sequencer:stop(Seq),
        [corfurl_flu:stop(F) || F <- FLUs],
        setup_del_all(NumFLUs)
    end.

-ifdef(TIMING_TEST).

forfun_test_() ->
    {timeout, 99999, fun() ->
                             [forfun(Procs) || Procs <- [10,100,1000,5000]]
                     end}.

forfun_append(0, _Seq, _P, _Page) ->
    ok;
forfun_append(N, Seq, P, Page) ->
    {ok, _} = ?M:append_page(Seq, P, Page),
    forfun_append(N - 1, Seq, P, Page).

%%% My MBP, SSD
%%% The 1K and 5K procs shows full-mailbox-scan ickiness
%%% when getting replies from prim_file.  :-(

%%% forfun: 10 procs writing 200000 pages of 8 bytes/page to 2 chains of 4 total FLUs in 10.016815 sec
%%% forfun: 100 procs writing 200000 pages of 8 bytes/page to 2 chains of 4 total FLUs in 10.547976 sec
%%% forfun: 1000 procs writing 200000 pages of 8 bytes/page to 2 chains of 4 total FLUs in 13.706686 sec
%%% forfun: 5000 procs writing 200000 pages of 8 bytes/page to 2 chains of 4 total FLUs in 33.516312 sec

%%% forfun: 10 procs writing 200000 pages of 8 bytes/page to 4 chains of 4 total FLUs in 5.350147 sec
%%% forfun: 100 procs writing 200000 pages of 8 bytes/page to 4 chains of 4 total FLUs in 5.429485 sec
%%% forfun: 1000 procs writing 200000 pages of 8 bytes/page to 4 chains of 4 total FLUs in 5.643233 sec
%%% forfun: 5000 procs writing 200000 pages of 8 bytes/page to 4 chains of 4 total FLUs in 15.686058 sec

%%%% forfun: 10 procs writing 200000 pages of 4096 bytes/page to 2 chains of 4 total FLUs in 13.479458 sec
%%%% forfun: 100 procs writing 200000 pages of 4096 bytes/page to 2 chains of 4 total FLUs in 14.752565 sec
%%%% forfun: 1000 procs writing 200000 pages of 4096 bytes/page to 2 chains of 4 total FLUs in 25.012306 sec
%%%% forfun: 5000 procs writing 200000 pages of 4096 bytes/page to 2 chains of 4 total FLUs in 38.972076 sec

forfun(NumProcs) ->
    io:format(user, "\n", []),
    NumFLUs = 4,
    PageSize = 8,
    %%PageSize = 4096,
    NumPages = 200*1000,
    PagesPerProc = NumPages div NumProcs,
    FLUs = [F1, F2, F3, F4] = setup_basic_flus(NumFLUs, PageSize, NumPages),
    {ok, Seq} = corfurl_sequencer:start_link(FLUs),

    try
        Chains = [[F1, F2], [F3, F4]],
        %%Chains = [[F1], [F2], [F3], [F4]],
        P = ?M:new_simple_projection(1, 1, NumPages*2, Chains),
        Me = self(),
        Start = now(),
        Ws = [begin
                  Page = <<X:(PageSize*8)>>,
                  spawn_link(fun() ->
                                     forfun_append(PagesPerProc, Seq, P, Page),
                                     Me ! {done, self()}
                             end)
              end || X <- lists:seq(1, NumProcs)],
        [receive {done, W} -> ok end || W <- Ws],
        End = now(),
        io:format(user, "forfun: ~p procs writing ~p pages of ~p bytes/page to ~p chains of ~p total FLUs in ~p sec\n",
                  [NumProcs, NumPages, PageSize, length(Chains), length(lists:flatten(Chains)), timer:now_diff(End, Start) / 1000000]),
        ok
    after
        corfurl_sequencer:stop(Seq),
        [corfurl_flu:stop(F) || F <- FLUs],
        setup_del_all(NumFLUs)
    end.

-endif. % TIMING_TEST
-endif. % not PULSE
-endif. % TEST
