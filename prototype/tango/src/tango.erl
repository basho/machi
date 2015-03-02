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

%% A prototype implementation of Tango over Corfurl.

-module(tango).

-include("corfurl.hrl").

-export([pack_v1/4, unpack_v1/2,
         add_back_pointer/2,
         add_back_pointer/3,
         scan_backward/4,
         scan_backward/5,
         pad_bin/2,
         append_page/3,
         back_ps2last_lpn/1,
         append_lpns/2]).

-define(MAGIC_NUMBER_V1, 16#88990011).

-define(D(X), io:format(user, "Dbg: ~s =\n  ~p\n", [??X, X])).

%% TODO: for version 2: add strong checksum

pack_v1(Stream_BackPs, Options, Page, PageSize)
  when is_list(Stream_BackPs), is_list(Options), is_binary(Page),
       is_integer(PageSize), PageSize > 0 ->
    Stream_BackPsBin = term_to_binary(Stream_BackPs),
    Stream_BackPsSize = byte_size(Stream_BackPsBin),
    OptionsInt = convert_options_list2int(Options),
    PageActualSize = byte_size(Page),
    pad_bin(PageSize,
            list_to_binary([<<?MAGIC_NUMBER_V1:32/big>>,
                            <<OptionsInt:8/big>>,
                            <<Stream_BackPsSize:16/big>>,
                            Stream_BackPsBin,
                            <<PageActualSize:16/big>>,
                            Page])).

unpack_v1(<<?MAGIC_NUMBER_V1:32/big,
            _Options:8/big,
            Stream_BackPsSize:16/big, Stream_BackPsBin:Stream_BackPsSize/binary,
            PageActualSize:16/big, Page:PageActualSize/binary,
            _/binary>>, Part) ->
    if Part == stream_list ->
            binary_to_term(Stream_BackPsBin);
       Part == page ->
            Page
    end.

pad_bin(Size, Bin) when byte_size(Bin) >= Size ->
    Bin;
pad_bin(Size, Bin) ->
    PadSize = Size - byte_size(Bin),
    <<Bin/binary, 0:(PadSize*8)>>.

add_back_pointer(StreamNum, BackPs, NewBackP) ->
    case proplists:get_value(StreamNum, BackPs) of
        undefined ->
            [{StreamNum, [NewBackP]}];
        IndividualBackPs ->
            [{StreamNum, add_back_pointer(IndividualBackPs, NewBackP)}
             |lists:keydelete(StreamNum, 1, BackPs)]
    end.        

add_back_pointer([A,B,C,_D|_], New) ->
    [New,A,B,C];
add_back_pointer([], New) ->
    [New];
add_back_pointer(BackPs, New) ->
    [New|BackPs].

convert_options_list2int(Options) ->
    lists:foldl(fun(t_final_page, Int) -> Int + 1;
                   (_,            Int) -> Int
                end, 0, Options).

scan_backward(Proj, Stream, LastLPN, WithPagesP) ->
    scan_backward(Proj, Stream, LastLPN, 0, WithPagesP).

scan_backward(Proj, Stream, LastLPN, StopAtLPN, WithPagesP) ->
    lists:reverse(scan_backward2(Proj, Stream, LastLPN, StopAtLPN,
                                 0, WithPagesP)).

scan_backward2(_Proj, _Stream, LastLPN, StopAtLPN, _NumPages, _WithPagesP)
  when LastLPN =< StopAtLPN; LastLPN =< 0 ->
    [];
scan_backward2(Proj, Stream, LastLPN, StopAtLPN, NumPages, WithPagesP) ->
    case corfurl:read_page(Proj, LastLPN) of
        {ok, FullPage} ->
            case proplists:get_value(Stream, unpack_v1(FullPage, stream_list)) of
                undefined ->
                    if NumPages == 0 ->
                            %% We were told to start scanning backward at some
                            %% LPN, but that LPN doesn't have a stream for us.
                            %% So we'll go backward a page and see if we get
                            %% lucky there.
                            scan_backward2(Proj, Stream, LastLPN-1, StopAtLPN,
                                           NumPages, WithPagesP);
                       true ->
                            %% Oops, we pointed into a hole.  That's bad.
                            %% TODO: fixme
                            {gah_fixme, lpn, LastLPN, unpack_v1(FullPage, stream_list)}
                    end;
                [] ->
                    if WithPagesP ->
                            [{LastLPN, unpack_v1(FullPage, page)}];
                       true ->
                            [LastLPN]
                    end;
                BackPs ->
                    if WithPagesP ->
                            %% ?D({bummer, BackPs}),
                            [{LastLPN, unpack_v1(FullPage, page)}|
                             scan_backward2(Proj, Stream,
                                            hd(BackPs), StopAtLPN, NumPages + 1,
                                            WithPagesP)];
                       true ->
                            SkipLPN = lists:last(BackPs),
                            AddLPNs = [LPN || LPN <- BackPs,
                                              LPN /= SkipLPN,
                                              LPN > StopAtLPN],
                            [LastLPN] ++ AddLPNs ++
                                scan_backward2(Proj, Stream,
                                               SkipLPN, StopAtLPN, NumPages + 1,
                                               WithPagesP)
                    end
            end;
        Err ->
            %% ?D({scan, LastLPN, Err}),
            Err
    end.

%% Hrm, this looks pretty similar to corfurl_client:append_page.

append_page(Proj, Page, StreamList) ->
    append_page(Proj, Page, StreamList, 5).

append_page(Proj, _Page, _StreamList, 0) ->
    {{error_failed, ?MODULE, ?LINE}, Proj};
append_page(#proj{seq={Sequencer,_,_}, page_size=PageSize} = Proj,
            OrigPage, StreamList, Retries) ->
    try
        {ok, LPN, BackPsList} = corfurl_sequencer:get_tails(Sequencer, 1,
                                                            StreamList),
        %% pulse_tracing_add(write, LPN),
        StreamBackPs = lists:zip(StreamList, BackPsList),
        Page = tango:pack_v1(StreamBackPs, [t_final_page],
                             OrigPage, PageSize),
        append_page1(Proj, LPN, Page, StreamList, 5, OrigPage)
    catch
        exit:{Reason,{_gen_server_or_pulse_gen_server,call,[Sequencer|_]}}
          when Reason == noproc; Reason == normal ->
            NewSeq = corfurl_client:restart_sequencer(Proj),
            append_page(Proj#proj{seq=NewSeq}, OrigPage, StreamList, Retries);
        exit:Exit ->
            {{error_failed, ?MODULE, ?LINE}, incomplete_code, Exit}
    end.

append_page1(Proj, _LPN, _Page, _StreamList, 0, _OrigPage) ->
    {{error_failed, ?MODULE, ?LINE}, Proj};
append_page1(Proj, LPN, Page, StreamList, Retries, OrigPage) ->
    case append_page2(Proj, LPN, Page) of
        lost_race ->
            append_page(Proj, OrigPage, StreamList, Retries - 1);
        error_badepoch ->
            case corfurl_sequencer:poll_for_new_epoch_projection(Proj) of
                {ok, NewProj} ->
                    append_page1(NewProj, LPN, Page, StreamList, Retries - 1,
                                 OrigPage);
                Else ->
                    {Else, Proj}
            end;
        Else ->
            {Else, Proj}
    end.

append_page2(Proj, LPN, Page) ->
    case corfurl:write_page(Proj, LPN, Page) of
        ok ->
            {ok, LPN};
        X when X == error_overwritten; X == error_trimmed ->
            %% report_lost_race(LPN, X),
            lost_race;
        {special_trimmed, LPN}=XX ->
            XX;
        error_badepoch=XX->
            XX
            %% Let it crash: error_unwritten
    end.

back_ps2last_lpn([]) ->
    0;
back_ps2last_lpn([H|_]) ->
    H.

append_lpns([], BPs) ->
    BPs;
append_lpns(LPNs, BPs) ->
    lists:reverse(LPNs) ++ BPs.

