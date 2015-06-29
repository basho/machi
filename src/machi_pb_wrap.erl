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

%% @doc Wrappers for Protocol Buffers encoding, including hacks to fix
%%      impedance mismatches between Erlang terms and PB encodings.
%%
%% TODO: Any use of enc_sexp() and dec_sexp() should be eliminated,
%%       except for the possibility of items where we are 100% sure
%%       that a non-Erlang software component can get away with always
%%       treating that item as an opaque thing.

-module(machi_pb_wrap).

-include("machi_pb.hrl").
-include("machi_projection.hrl").

-ifdef(COMMENT_DELME).

-export([enc_p_srvr/1, dec_p_srvr/1,
         enc_projection_v1/1, dec_projection_v1/1,
         make_projection_req/2, unmake_projection_req/1,
         make_projection_resp/3]).
-ifdef(TEST).
-compile(export_all).
-endif. % TEST

enc_p_srvr(P) ->
    machi_pb:encode_mpb_p_srvr(conv_from_p_srvr(P)).

dec_p_srvr(Bin) ->
    conv_to_p_srvr(machi_pb:decode_mpb_p_srvr(Bin)).

conv_from_p_srvr(#p_srvr{name=Name,
                   proto_mod=ProtoMod,
                   address=Address,
                   port=Port,
                   props=Props}) ->
    #mpb_p_srvr{name=to_list(Name),
                proto_mod=to_list(ProtoMod),
                address=to_list(Address),
                port=to_list(Port),
                opaque_props=enc_sexp(Props)}.

conv_to_p_srvr(#mpb_p_srvr{name=Name,
                           proto_mod=ProtoMod,
                           address=Address,
                           port=Port,
                           opaque_props=Props}) ->
    #p_srvr{name=to_atom(Name),
            proto_mod=to_atom(ProtoMod),
            address=to_list(Address),
            port=to_integer(Port),
            props=dec_sexp(Props)}.

enc_projection_v1(P) ->
    %% Awww, flatten it here
    list_to_binary(
      machi_pb:encode_mpb_projectionv1(conv_from_projection_v1(P))).

dec_projection_v1(Bin) ->
    delme.
    %% conv_to_projection_v1(machi_pb:decode_mpb_projectionv1(Bin)).





%%%%%%%%%%%%%%%%%%%

enc_sexp(T) ->
    term_to_binary(T).

dec_sexp(Bin) when is_binary(Bin) ->
    binary_to_term(Bin).

enc_optional_sexp(undefined) ->
    undefined;
enc_optional_sexp(T) ->
    enc_sexp(T).

dec_optional_sexp(undefined) ->
    undefined;
dec_optional_sexp(T) ->
    dec_sexp(T).

conv_from_members_dict(D) ->
    %% Use list_to_binary() here to "flatten" the serialized #p_srvr{}
    [#mpb_membersdictentry{key=to_list(K), val=conv_from_p_srvr(V)} ||
        {K, V} <- orddict:to_list(D)].

conv_to_members_dict(List) ->
    orddict:from_list([{to_atom(K), conv_to_p_srvr(V)} ||
                          #mpb_membersdictentry{key=K, val=V} <- List]).

to_list(X) when is_atom(X) ->
    atom_to_list(X);
to_list(X) when is_binary(X) ->
    binary_to_list(X);
to_list(X) when is_integer(X) ->
    integer_to_list(X);
to_list(X) when is_list(X) ->
    X.

to_atom(X) when is_list(X) ->
    list_to_atom(X);
to_atom(X) when is_binary(X) ->
    erlang:binary_to_atom(X, latin1);
to_atom(X) when is_atom(X) ->
    X.

to_integer(X) when is_list(X) ->
    list_to_integer(X);
to_integer(X) when is_binary(X) ->
    list_to_binary(binary_to_list(X));
to_integer(X) when is_integer(X) ->
    X.

-endif. % COMMENT_DELME
