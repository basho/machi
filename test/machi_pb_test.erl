%% -------------------------------------------------------------------
%%
%% Machi: a small village of replicated files
%%
%% Copyright (c) 2014-2015 Basho Technologies, Inc.  All Rights Reserved.
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
-module(machi_pb_test).

-include("machi_pb.hrl").

-compile(export_all).

-ifdef(TEST).
-ifndef(PULSE).

-include_lib("eunit/include/eunit.hrl").

%% We don't expect any problems with these functions: nearly all
%% code executed is compiled from the PB definition file(s) and so
%% ought to be bug-free.  Errors are likely to be caused by
%% changes in the spec but without corresponding changes to
%% message types/names here.

smoke_requests_test() ->
    Echo0 = #mpb_request{req_id= <<"x">>,
                         echo=#mpb_echoreq{}},
    Echo0 = encdec_request(Echo0),
    Echo1 = #mpb_request{req_id= <<"x">>,
                         echo=#mpb_echoreq{message="Yo!"}},
    Echo1 = encdec_request(Echo1),

    ok.

smoke_responses_test() ->
    R1 = #mpb_response{req_id= <<"x">>,
                       generic=#mpb_errorresp{code=7,
                                              msg="foo",
                                              extra= <<"bar">>}},
    R1 = encdec_response(R1),

    ok.

encdec_request(M) ->
    machi_pb:decode_mpb_request(
      list_to_binary(machi_pb:encode_mpb_request(M))).

encdec_response(M) ->
    machi_pb:decode_mpb_response(
      list_to_binary(machi_pb:encode_mpb_response(M))).

-endif. % !PULSE
-endif. % TEST
