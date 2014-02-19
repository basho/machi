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
-export([append_page/3, read_page/2, scan_forward/3,
         fill_page/2]).

-include("corfurl.hrl").

-ifdef(TEST).
-compile(export_all).
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.
-endif.

append_page(Sequencer, P, Page) ->
    append_page(Sequencer, P, Page, 1).

append_page(Sequencer, P, Page, Retries) when Retries < 50 ->
    case corfurl_sequencer:get(Sequencer, 1) of
        LPN when is_integer(LPN) ->
            case write_single_page(P, LPN, Page) of
                ok ->
                    {ok, LPN};
                X when X == error_overwritten; X == error_trimmed ->
                    report_lost_race(LPN, X),
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
    fill_page2(Chain, Epoch, LPN).

fill_page2([], _Epoch, _LPN) ->
    ok;
fill_page2([H|T], Epoch, LPN) ->
    case corfurl_flu:fill(flu_pid(H), Epoch, LPN) of
        ok ->
            fill_page2(T, Epoch, LPN);
        Else ->
            %% TODO: worth doing anything here, if we're in the middle of chain?
            Else
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
