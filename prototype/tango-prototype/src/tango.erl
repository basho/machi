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

%% A prototype implementation of Tango over CORFU.

-module(tango).

-export([pack_v1/3, unpack_v1/2,
         add_back_pointer/3,
         scan_backward/4,
         scan_backward/5,
         pad_bin/2]).

-define(MAGIC_NUMBER_V1, 16#88990011).

%% TODO: for version 2: add strong checksum

pack_v1(StreamList, Page, PageSize) when is_list(StreamList), is_binary(Page) ->
    StreamListBin = term_to_binary(StreamList),
    StreamListSize = byte_size(StreamListBin),
    PageActualSize = byte_size(Page),
    pad_bin(PageSize,
            list_to_binary([<<?MAGIC_NUMBER_V1:32/big>>,
                            <<StreamListSize:16/big>>,
                            StreamListBin,
                            <<PageActualSize:16/big>>,
                            Page])).

unpack_v1(<<?MAGIC_NUMBER_V1:32/big,
            StreamListSize:16/big, StreamListBin:StreamListSize/binary,
            PageActualSize:16/big, Page:PageActualSize/binary,
            _/binary>>, Part) ->
    if Part == stream_list ->
            binary_to_term(StreamListBin);
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

scan_backward(Proj, Stream, LastLPN, WithPagesP) ->
    scan_backward(Proj, Stream, LastLPN, 0, WithPagesP).

scan_backward(Proj, Stream, LastLPN, StopAtLPN, WithPagesP) ->
    lists:reverse(scan_backward2(Proj, Stream, LastLPN, StopAtLPN, WithPagesP)).

scan_backward2(_Proj, _Stream, LastLPN, StopAtLPN, _WithPagesP)
  when LastLPN =< StopAtLPN ->
    [];
scan_backward2(Proj, Stream, LastLPN, StopAtLPN, WithPagesP) ->
    case corfurl:read_page(Proj, LastLPN) of
        {ok, FullPage} ->
            case proplists:get_value(Stream, unpack_v1(FullPage, stream_list)) of
                undefined ->
                    {gah_fixme, lpn, LastLPN, unpack_v1(FullPage, stream_list)};
                [] ->
                    if WithPagesP ->
                            [{LastLPN, unpack_v1(FullPage, page)}];
                       true ->
                            [LastLPN]
                    end;
                BackPs ->
                    if WithPagesP ->
                            [{LastLPN, unpack_v1(FullPage, page)}|
                             scan_backward2(Proj, Stream,
                                            hd(BackPs), StopAtLPN,
                                            WithPagesP)];
                       true ->
                            SkipLPN = lists:last(BackPs),
                            AddLPNs = [LPN || LPN <- BackPs,
                                              LPN /= SkipLPN,
                                              LPN > StopAtLPN],
                            [LastLPN] ++ AddLPNs ++
                                scan_backward2(Proj, Stream,
                                               SkipLPN, StopAtLPN,
                                               WithPagesP)
                    end
            end;
        Err ->
            Err
    end.

