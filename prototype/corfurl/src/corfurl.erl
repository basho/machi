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

-module(corfurl).

-export([new_simple_projection/4,
         new_range/3,
         read_projection/2,
         save_projection/2]).
-export([append_page/3, read_page/2, scan_forward/3]).

-include("corfurl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-type flu_name() :: atom().
-type flu() :: pid() | flu_name().
-type flu_chain() :: [flu()].

-record(range, {
          pn_start :: non_neg_integer(),            % start page number
          pn_end :: non_neg_integer(),              % end page number
          chains :: [flu_chain()]
         }).

-record(proj, {                                 % Projection
          epoch :: non_neg_integer(),
          r :: [#range{}]
         }).

append_page(Sequencer, P, Page) ->
    append_page(Sequencer, P, Page, 1).

append_page(Sequencer, P, Page, Retries) when Retries < 50 ->
    case corfurl_sequencer:get(Sequencer, 1) of
        LPN when is_integer(LPN) ->
            case write_single_page(P, LPN, Page) of
                ok ->
                    {ok, LPN};
                X when X == error_overwritten; X == error_trimmed ->
                    io:format(user, "LPN ~p race lost: ~p\n", [LPN, X]),
                    append_page(Sequencer, P, Page);
                Else ->
                    exit({todo, ?MODULE, line, ?LINE, Else})
            end;
        _ ->
            timer:sleep(Retries),               % TODO naive
            append_page(Sequencer, P, Page, Retries * 2)
    end.

write_single_page(#proj{epoch=Epoch} = P, LPN, Page) ->
    Chain = project_to_chain(LPN, P),
    write_single_page_to_chain(Chain, Epoch, LPN, Page, 1).

write_single_page_to_chain([], _Epoch, _LPN, _Page, _Nth) ->
    ok;
write_single_page_to_chain([FLU|Rest], Epoch, LPN, Page, Nth) ->
    case corfurl_flu:write(flu_pid(FLU), Epoch, LPN, Page) of
        ok ->
            write_single_page_to_chain(Rest, Epoch, LPN, Page, Nth+1);
        error_badepoch ->
            %% TODO: Interesting case: there may be cases where retrying with
            %%       a new epoch & that epoch's projection is just fine (and
            %%       we'll succeed) and cases where retrying will fail.
            %%       Figure out what those cases are, then for the
            %%       destined-to-fail case, try to clean up (via trim?)?
            error_badepoch;
        error_trimmed ->
            %% Whoa, partner, you're movin' kinda fast for a trim.
            %% This might've been due to us being too slow and someone
            %% else junked us.
            %% TODO We should go trim our previously successful writes?
            error_trimmed;
        error_overwritten when Nth == 1 ->
            %% The sequencer lied, or we didn't use the sequencer and
            %% guessed and guessed poorly, or someone is accidentally
            %% trying to take our page.  Shouganai, these things happen.
            error_overwritten;
        error_overwritten when Nth > 1 ->
            %% The likely cause is that another reader has noticed that
            %% we haven't finished writing this page in this chain and
            %% has repaired the remainder of the chain while we were
            %% drinking coffee.  Let's double-check.
            case corfurl_flu:read(flu_pid(FLU), Epoch, LPN) of
                {ok, AlreadyThere} when AlreadyThere =:= Page ->
                    %% Alright, well, let's go continue the repair/writing,
                    %% since we agree on the page's value.
                    write_single_page_to_chain(Rest, Epoch, LPN, Page, Nth+1);
                error_badepoch ->
                    %% TODO: same TODO as the above error_badepoch case.
                    error_badepoch;
                Else ->
                    error({left_off_here, ?MODULE, ?LINE, Else})
            end
    end.

read_page(#proj{epoch=Epoch} = P, LPN) ->
    Chain = project_to_chain(LPN, P),
    Tail = lists:last(Chain),
    case corfurl_flu:read(flu_pid(Tail), Epoch, LPN) of
        {ok, _} = OK ->
            OK;
        error_badepoch ->
            error_badepoch;
        error_trimmed ->
            %% TODO: A sanity/should-never-happen check would be to
            %%       see if everyone else in the chain are also trimmed.
            error_trimmed;
        error_unwritten ->
            %% TODO: During scan_forward(), this pestering of the upstream
            %%       nodes in the chain is possibly-excessive-work.
            %%       For now, we'll assume that we always want to repair.
            read_repair_chain(Epoch, LPN, Chain)
        %% Let it crash: error_overwritten
    end.

read_repair_chain(Epoch, LPN, [Head|Rest] = Chain) ->
    case corfurl_flu:read(flu_pid(Head), Epoch, LPN) of
        {ok, Page} ->
            read_repair_chain2(Rest, Epoch, LPN, Page, Chain);
        error_badepoch ->
            error_badepoch;
        error_trimmed ->
            %% TODO: robustify
            [ok = case corfurl_flu:fill(flu_pid(X), Epoch, LPN) of
                      ok ->            ok;
                      error_trimmed -> ok;
                      Else          -> Else
                  end || X <- Rest],
            error_trimmed;
        error_unwritten ->
            error_unwritten
        %% Let it crash: error_overwritten
    end.

read_repair_chain2([] = _Repairees, _Epoch, _LPN, Page, _OriginalChain) ->
    {ok, Page};
read_repair_chain2([RepairFLU|Rest], Epoch, LPN, Page, OriginalChain) ->
    case corfurl_flu:write(flu_pid(RepairFLU), Epoch, LPN, Page) of
        ok ->
            read_repair_chain2(Rest, Epoch, LPN, Page, OriginalChain);
        error_badepoch ->
            error_badepoch;
        error_trimmed ->
            error_trimmed;
        error_overwritten ->
            %% We're going to do an optional sanity check here.
            %% TODO: make the sanity check configurable?
            case corfurl_flu:read(flu_pid(RepairFLU), Epoch, LPN) of
                {ok, Page2} when Page2 =:= Page ->
                    %% TODO: is there a need to continue working upstream
                    %%       to fix problems?
                    {ok, Page2};
                {ok, _Page2} ->
                    error({bummerbummer, ?MODULE, ?LINE, sanity_check_failure,
                           lpn, LPN, epoch, Epoch});
                error_badepoch ->
                    error_badepoch;
                error_trimmed ->
                    %% Start repair at the beginning to handle this case
                    read_repair_chain(Epoch, LPN, OriginalChain)
                %% Let it crash: error_overwritten, error_unwritten
            end
        %% Let it crash: error_unwritten
    end.

read_repair_trim(RepairFLU, LPN) ->
    case corfurl_flu:trim(flu_pid(RepairFLU), LPN) of
        ok ->
            error_trimmed;
        Else ->
            Else
    end.

scan_forward(P, LPN, MaxPages) ->
    scan_forward(P, LPN, MaxPages, ok, true, []).

scan_forward(_P, LPN, 0, Status, MoreP, Acc) ->
    {Status, LPN, MoreP, lists:reverse(Acc)};
scan_forward(P, LPN, MaxPages, _Status, _MoreP, Acc) ->
    case read_page(P, LPN) of
        {ok, Page} ->
            Res = {LPN, Page},
            scan_forward(P, LPN + 1, MaxPages - 1, ok, true, [Res|Acc]);
        error_badepoch ->
            %% Halt, allow recursion to create our return value.
            scan_forward(P, LPN, 0, error_badepoch, false, Acc);
        error_trimmed ->
            %% TODO: API question, do we add a 'trimmed' indicator
            %%       in the Acc?  Or should the client assume that if
            %%       scan_forward() doesn't mention a page that
            scan_forward(P, LPN + 1, MaxPages - 1, ok, true, Acc);
        error_unwritten ->
            %% Halt, allow recursion to create our return value.
            %% TODO: It's possible that we're stuck here because a client
            %%       crashed and that we see an unwritten page at LPN.
            %%       We ought to ask the sequencer always/sometime?? what
            %%       tail LPN is, and if there's a hole, start a timer to
            %%       allow us to fill the hole.
            scan_forward(P, LPN, 0, ok, false, Acc)
        %% Let it crash: error_overwritten
    end.

flu_pid(X) when is_pid(X) ->
    X;
flu_pid(X) when is_atom(X) ->
    ets:lookup_element(flu_pid_tab, X, 1).

%%%% %%%% %%%%    projection utilities    %%%% %%%% %%%%

new_range(Start, End, ChainList) ->
    %% TODO: sanity checking of ChainList, Start < End, yadda
    #range{pn_start=Start, pn_end=End, chains=list_to_tuple(ChainList)}.

new_simple_projection(Epoch, Start, End, ChainList) ->
    #proj{epoch=Epoch, r=[new_range(Start, End, ChainList)]}.

make_projection_path(Dir, Epoch) ->
    lists:flatten(io_lib:format("~s/~12..0w.proj", [Dir, Epoch])).

read_projection(Dir, Epoch) ->
    case file:read_file(make_projection_path(Dir, Epoch)) of
        {ok, Bin} ->
            {ok, binary_to_term(Bin)};          % TODO if corrupted?
        {error, enoent} ->
            error_unwritten;
        Else ->
            Else                                % TODO API corner case
    end.

save_projection(Dir, #proj{epoch=Epoch} = P) ->
    Path = make_projection_path(Dir, Epoch),
    ok = filelib:ensure_dir(Dir ++ "/ignored"),
    {_, B, C} = now(),
    TmpPath = Path ++ lists:flatten(io_lib:format(".~w.~w.~w", [B, C, node()])),
    %% TODO: don't be lazy, do a flush before link when training wheels come off
    ok = file:write_file(TmpPath, term_to_binary(P)),
    case file:make_link(TmpPath, Path) of
        ok ->
            file:delete(TmpPath),
            ok;
        {error, eexist} ->
            error_overwritten;
        Else ->
            Else                                % TODO API corner case
    end.

project_to_chain(LPN, P) ->
    %% TODO fixme
    %% TODO something other than round-robin?
    [#range{pn_start=Start, pn_end=End, chains=Chains}] = P#proj.r,
    if Start =< LPN, LPN =< End ->
            I = ((LPN - Start) rem tuple_size(Chains)) + 1,
            element(I, Chains)
    end.

%%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%% %%%%

-ifdef(TEST).

save_read_test() ->
    Dir = "/tmp/" ++ atom_to_list(?MODULE) ++".save-read",
    Chain = [a,b],
    P1 = new_simple_projection(1, 1, 1*100, [Chain]),

    try
        filelib:ensure_dir(Dir ++ "/ignored"),
        ok = save_projection(Dir, P1),
        error_overwritten = save_projection(Dir, P1),

        {ok, P1} = read_projection(Dir, 1),
        error_unwritten = read_projection(Dir, 2),

        ok
    after
        ok = corfurl_util:delete_dir(Dir)
    end.

setup_flu_basedir() ->
    "/tmp/" ++ atom_to_list(?MODULE) ++ ".".    

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
        P1 = new_simple_projection(1, 1, 1*100, [[F1, F2, F3], [F4, F5, F6]]),
        [begin {ok, LPN} = append_page(Seq, P1, Pg) end || {LPN, Pg} <- LPN_Pgs],

        [begin {ok, Pg} = read_page(P1, LPN) end || {LPN, Pg} <- LPN_Pgs],

        [begin
             LPNplus = LPN + 1,
             {ok, LPNplus, true, [{LPN, Pg}]} = scan_forward(P1, LPN, 1)
         end || {LPN, Pg} <- LPN_Pgs],
        {ok, 6, false, []} = scan_forward(P1, 6, 1),
        {ok, 6, false, []} = scan_forward(P1, 6, 10),
        [{LPN1,Pg1}, {LPN2,Pg2}, {LPN3,Pg3}, {LPN4,Pg4}, {LPN5,Pg5}] = LPN_Pgs,
        {ok, 4, true, [{LPN2,Pg2}, {LPN3,Pg3}]} = scan_forward(P1, 2, 2),
        {ok, 6, false, [{LPN3,Pg3}, {LPN4,Pg4}, {LPN5,Pg5}]} = scan_forward(P1, 3, 10),

        %% Let's smoke read-repair: regular write failure
        Epoch = P1#proj.epoch,
        Pg6 = <<424242:(PageSize*8)>>,

        %% Simulate a failed write to the chain.
        [F6a, F6b, F6c] = Chain6 = project_to_chain(6, P1),
        NotHead6 = [F6b, F6c],
        ok = write_single_page_to_chain([F6a], Epoch, 6, Pg6, 1),

        %% Does the chain look as expected?
        {ok, Pg6} = corfurl_flu:read(flu_pid(F6a), Epoch, 6),
        [error_unwritten = corfurl_flu:read(flu_pid(X), Epoch, 6) ||
            X <- NotHead6],

        %% Read repair should fix it.
        {ok, Pg6} = read_page(P1, 6),
        [{ok, Pg6} = corfurl_flu:read(flu_pid(X), Epoch, 6) || X <- Chain6],

        %% Let's smoke read-repair: failed fill
        [F7a, F7b, F7c] = Chain7 = project_to_chain(7, P1),
        NotHead7 = [F7b, F7c],
        ok = corfurl_flu:fill(flu_pid(F7a), Epoch, 7),

        %% Does the chain look as expected?
        error_trimmed = corfurl_flu:read(flu_pid(F7a), Epoch, 7),
        [error_unwritten = corfurl_flu:read(flu_pid(X), Epoch, 7) ||
            X <- NotHead7],

        %% Read repair should fix it.
        error_trimmed = read_page(P1, 7),
        [error_trimmed = corfurl_flu:read(flu_pid(X), Epoch, 7) || X <- Chain7],
        %% scan_forward shouldn't see it either
        {ok, 8, false, [{6,Pg6}]} = scan_forward(P1, 6, 10),

        [F8a|_] = Chain8 = project_to_chain(8, P1),
        ok = corfurl_flu:fill(flu_pid(F8a), Epoch, 8),
        %% No read before scan, scan_forward shouldn't see 8 either,
        %% but the next seq should be 9
        {ok, 9, false, [{6,Pg6}]} = scan_forward(P1, 6, 10),

        ok
    after
        corfurl_sequencer:stop(Seq),
        [corfurl_flu:stop(F) || F <- FLUs],
        setup_del_all(NumFLUs)
    end.

forfun_append(0, _Seq, _P, _Page) ->
    ok;
forfun_append(N, Seq, P, Page) ->
    ok = append_page(Seq, P, Page),
    forfun_append(N - 1, Seq, P, Page).

-ifdef(TIMING_TEST).

forfun_test_() ->
    {timeout, 99999, fun() ->
                             [forfun(Procs) || Procs <- [10,100,1000,5000]]
                     end}.

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
        P = new_simple_projection(1, 1, NumPages*2, Chains),
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

-endif. % TEST
