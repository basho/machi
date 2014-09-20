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

-export([new_simple_projection/5,
         new_range/3,
         read_projection/2,
         save_projection/2,
         latest_projection_epoch_number/1]).
-export([write_page/3, read_page/2, scan_forward/3,
         fill_page/2, trim_page/2]).
-export([simple_test_setup/5]).

-include("corfurl.hrl").

-ifdef(TEST).
-compile(export_all).
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.
-endif.

%%% Debugging: for extra events in the PULSE event log, use the 2nd statement.
-define(EVENT_LOG(X), ok).
%%% -define(EVENT_LOG(X), event_logger:event(X)).

write_page(#proj{epoch=Epoch} = P, LPN, Page) ->
    Chain = project_to_chain(LPN, P),
    write_page_to_chain(Chain, Chain, Epoch, LPN, Page, 1).

write_page_to_chain(Chain, Chain, Epoch, LPN, Page, Nth) ->
    write_page_to_chain(Chain, Chain, Epoch, LPN, Page, Nth, ok).

write_page_to_chain([], _Chain, _Epoch, _LPN, _Page, _Nth, Reply) ->
    Reply;
write_page_to_chain([FLU|Rest], Chain, Epoch, LPN, Page, Nth, Reply) ->
    case corfurl_flu:write(flu_pid(FLU), Epoch, LPN, Page) of
        ok ->
            write_page_to_chain(Rest, Chain, Epoch, LPN, Page, Nth+1, Reply);
        error_badepoch ->
            %% TODO: Interesting case: there may be cases where retrying with
            %%       a new epoch & that epoch's projection is just fine (and
            %%       we'll succeed) and cases where retrying will fail.
            %%       Figure out what those cases are, then for the
            %%       destined-to-fail case, try to clean up (via trim?)?
            error_badepoch;
        error_trimmed when Nth == 1 ->
            %% Whoa, partner, you're movin' kinda fast for a trim.
            %% This might've been due to us being too slow and someone
            %% else junked us.
            error_trimmed;
        error_trimmed when Nth > 1 ->
            %% We're racing with a trimmer.  We won the race at head,
            %% but here in the middle or tail (Nth > 1), we lost.
            %% Our strategy is keep racing down to the tail.
            %% If we continue to lose the exact same race for the rest
            %% of the chain, the 1st clause of this func will return 'ok'.
            %% That is *exactly* our intent and purpose!
            write_page_to_chain(Rest, Chain, Epoch, LPN, Page, Nth+1, {special_trimmed, LPN});
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
                    write_page_to_chain(Rest, Chain, Epoch, LPN, Page, Nth+1, Reply);
                error_badepoch ->
                    %% TODO: same TODO as the above error_badepoch case.
                    error_badepoch;
                error_trimmed ->
                    %% This is the same as 'error_trimmed when Nth > 1' above.
                    %% Do the same thing.
                    write_page_to_chain(Rest, Chain, Epoch, LPN, Page, Nth+1, {special_trimmed, LPN});
                Else ->
                    %% Can PULSE can drive us to this case?
                    giant_error({left_off_here, ?MODULE, ?LINE, Else, nth, Nth})
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

ok_or_trim(ok) ->
    ok;
ok_or_trim(error_trimmed) ->
    ok;
ok_or_trim(Else) ->
    Else.

read_repair_chain(Epoch, LPN, Chain) ->
    try
        read_repair_chain1(Epoch, LPN, Chain)
    catch
        throw:{i_give_up,Res} ->
            Res
    end.

read_repair_chain1(Epoch, LPN, [Head|Rest] = Chain) ->
    ?EVENT_LOG({read_repair, LPN, Chain, i_am, self()}),
    case corfurl_flu:read(flu_pid(Head), Epoch, LPN) of
        {ok, Page} ->
            ?EVENT_LOG({read_repair, LPN, Head, ok}),
            read_repair_chain2(Rest, Epoch, LPN, Page, Chain);
        error_badepoch ->
            ?EVENT_LOG({read_repair, LPN, Head, badepoch}),
            error_badepoch;
        error_trimmed ->
            ?EVENT_LOG({read_repair, LPN, Head, trimmed}),
            %% TODO: robustify
            [begin
                 ?EVENT_LOG({read_repair, LPN, fill, flu_pid(X)}),
                 ok = case ok_or_trim(corfurl_flu:fill(flu_pid(X), Epoch,
                                                       LPN)) of
                          ok ->
                              ?EVENT_LOG({read_repair, LPN, fill, flu_pid(X), ok}),
                              ok;
                          error_overwritten ->
                              ?EVENT_LOG({read_repair, LPN, fill, flu_pid(X), overwritten, try_to_trim}),
                              Res2 = ok_or_trim(corfurl_flu:trim(
                                                  flu_pid(X), Epoch, LPN)),
                              ?EVENT_LOG({read_repair, LPN, fill, flu_pid(X), trim, Res2}),
                              case Res2 of ok -> ok;
                                           _  -> throw({i_give_up,Res2})
                              end;
                          Else ->
                              %% We're too deeply nested for the current code
                              %% to deal with, and we're racing.  Fine, let
                              %% our opponent continue.  We'll give up, and if
                              %% the client wants to try again, we can try
                              %% again from the top.
                              ?EVENT_LOG({read_repair, LPN, fill, flu_pid(X), Else}),
                              throw({i_give_up,Else})
                      end
             end || X <- Rest],
            error_trimmed;
        error_unwritten ->
            ?EVENT_LOG({read_repair, LPN, read, Head, unwritten}),
            error_unwritten
        %% Let it crash: error_overwritten
    end.

read_repair_chain2([] = _Repairees, _Epoch, _LPN, Page, _OriginalChain) ->
    ?EVENT_LOG({read_repair2, _LPN, finished, {ok, Page}}),
    {ok, Page};
read_repair_chain2([RepairFLU|Rest], Epoch, LPN, Page, OriginalChain) ->
    case corfurl_flu:write(flu_pid(RepairFLU), Epoch, LPN, Page) of
        ok ->
            ?EVENT_LOG({read_repair2, LPN, write, flu_pid(RepairFLU), ok}),
            read_repair_chain2(Rest, Epoch, LPN, Page, OriginalChain);
        error_badepoch ->
            ?EVENT_LOG({read_repair2, LPN, write, flu_pid(RepairFLU), badepoch}),
            error_badepoch;
        error_trimmed ->
            ?EVENT_LOG({read_repair2, LPN, write, flu_pid(RepairFLU), trimmed}),
            error_trimmed;
        error_overwritten ->
            ?EVENT_LOG({read_repair2, LPN, write, flu_pid(RepairFLU), overwritten}),
            %% We're going to do an optional sanity check here.
            %% TODO: make the sanity check configurable?
            case corfurl_flu:read(flu_pid(RepairFLU), Epoch, LPN) of
                {ok, Page2} when Page2 =:= Page ->
                    ?EVENT_LOG({read_repair2, LPN, read, flu_pid(RepairFLU), exact_page}),
                    %% We're probably going to be racing against someone else
                    %% that's also doing repair, but so be it.
                    read_repair_chain2(Rest, Epoch, LPN, Page, OriginalChain);
                {ok, _Page2} ->
                    ?EVENT_LOG({read_repair2, LPN, read, flu_pid(RepairFLU), bad_page, _Page2}),
                    giant_error({bummerbummer, ?MODULE, ?LINE,
                                 sanity_check_failure, lpn, LPN, epoch, Epoch});
                error_badepoch ->
                    ?EVENT_LOG({read_repair2, LPN, read, flu_pid(RepairFLU), badepoch}),
                    error_badepoch;
                error_trimmed ->
                    ?EVENT_LOG({read_repair2, LPN, read, flu_pid(RepairFLU), trimmed}),
                    %% Start repair at the beginning to handle this case
                    read_repair_chain(Epoch, LPN, OriginalChain)
                %% Let it crash: error_overwritten, error_unwritten
            end
        %% Let it crash: error_unwritten
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

fill_page(#proj{epoch=Epoch} = P, LPN) ->
    Chain = project_to_chain(LPN, P),
    fill_or_trim_page(Chain, Epoch, LPN, fill).

trim_page(#proj{epoch=Epoch} = P, LPN) ->
    Chain = project_to_chain(LPN, P),
    fill_or_trim_page(Chain, Epoch, LPN, trim).

fill_or_trim_page([], _Epoch, _LPN, _Func) ->
    ok;
fill_or_trim_page([H|T], Epoch, LPN, Func) ->
    case corfurl_flu:Func(flu_pid(H), Epoch, LPN) of
        Res when Res == ok; Res == error_trimmed ->
            %% Detecting a race here between fills and trims is too crazy,
            %% and I don't believe that it *matters*.  The ickiest one
            %% is a race between Proc A = trim and Proc B = read,
            %% chain length of 2 or more:
            %% Proc A: trim head -> ok
            %% Proc B: read tail -> error_unwritten
            %% Proc B: read head -> error_trimmed
            %% Proc B: trim tail -> ok
            %% Proc A: trim tail -> ??
            %%
            %% The result that we want that both A & B & any later
            %% readers agree that the LPN is trimmed.  If the chain is
            %% >2, then the procs can win some/all/none of the races
            %% to fix up the chain, that's no problem.  But don't tell
            %% the caller that there was an error during those races.
            fill_or_trim_page(T, Epoch, LPN, Func);
        Else ->
            %% TODO: worth doing anything here, if we're in the middle of chain?
            %% TODO: is that ^^ anything different for fill vs. trim?
            Else
    end.

flu_pid(X) when is_pid(X) ->
    X;
flu_pid(X) when is_atom(X) ->
    ets:lookup_element(flu_pid_tab, X, 1).

giant_error(Err) ->
    io:format(user, "GIANT ERROR: ~p\n", [Err]),
    exit(Err).

%%%% %%%% %%%%    projection utilities    %%%% %%%% %%%%

new_range(Start, End, ChainList) ->
    %% TODO: sanity checking of ChainList, Start < End, yadda
    #range{pn_start=Start, pn_end=End, chains=list_to_tuple(ChainList)}.

new_simple_projection(Dir, Epoch, Start, End, ChainList) ->
    ok = filelib:ensure_dir(Dir ++ "/unused"),
    #proj{dir=Dir, epoch=Epoch, r=[new_range(Start, End, ChainList)]}.

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

latest_projection_epoch_number(Dir) ->
    case filelib:wildcard("*.proj", Dir) of
        [] ->
            -1;
        Files ->
            {Epoch, _} = string:to_integer(lists:last(Files)),
            Epoch
    end.

project_to_chain(LPN, P) ->
    %% TODO fixme
    %% TODO something other than round-robin?
    [#range{pn_start=Start, pn_end=End, chains=Chains}] = P#proj.r,
    if Start =< LPN, LPN =< End ->
            I = ((LPN - Start) rem tuple_size(Chains)) + 1,
            element(I, Chains);
       true ->
            exit({?MODULE, project_to_chain, [{start, Start},
                                              {lpn, LPN},
                                              {'end', End}]})
    end.

simple_test_setup(RootDir, BaseDirName, PageSize, NumPages, NumFLUs) ->
    PDir = RootDir ++ "/" ++ BaseDirName ++ ".projection",
    filelib:ensure_dir(PDir),
    BaseDir = RootDir ++ "/flu." ++ BaseDirName ++ ".",
    MyDir = fun(X) -> BaseDir ++ integer_to_list(X) end,
    DeleteFLUData = fun() -> [ok = corfurl_util:delete_dir(MyDir(X)) ||
                                 X <- lists:seq(1, NumFLUs)] end,
    DeleteFLUData(),
    FLUs = [begin
                element(2, corfurl_flu:start_link(MyDir(X),
                                                  PageSize, NumPages*PageSize))
            end || X <- lists:seq(1, NumFLUs)],

    {ok, Seq} = corfurl_sequencer:start_link(FLUs),
    P0 = corfurl:new_simple_projection(PDir, 1, 1, 1*100, [FLUs]),
    P1 = P0#proj{seq={Seq, unused, unused}, page_size=PageSize},
    {FLUs, Seq, P1, DeleteFLUData}.
