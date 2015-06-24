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
-module(machi_pb_wrap).

-include("machi_pb.hrl").
-include("machi_projection.hrl").

-export([enc_p_srvr/1, dec_p_srvr/1,
        enc_projection_v1/1, dec_projection_v1/1]).
-ifdef(TEST).
-compile(export_all).
-endif. % TEST

enc_p_srvr(#p_srvr{name=Name,
                   proto_mod=ProtoMod,
                   address=Address,
                   port=Port,
                   props=Props}) ->
    machi_pb:encode_mpb_p_srvr(#mpb_p_srvr{name=to_list(Name),
                                           proto_mod=to_list(ProtoMod),
                                           address=to_list(Address),
                                           port=to_list(Port),
                                           props=todo_enc_opaque(Props)}).

dec_p_srvr(Bin) ->
    #mpb_p_srvr{name=Name,
                proto_mod=ProtoMod,
                address=Address,
                port=Port,
                props=Props} = machi_pb:decode_mpb_p_srvr(Bin),
    #p_srvr{name=to_atom(Name),
            proto_mod=to_atom(ProtoMod),
            address=to_list(Address),
            port=to_integer(Port),
            props=todo_dec_opaque(Props)}.

enc_projection_v1(#projection_v1{dbg=Dbg,
                                 dbg2=Dbg2,
                                 members_dict=MembersDict} = P) ->
    term_to_binary(P#projection_v1{dbg=enc_sexp(Dbg),
                                   dbg2=enc_sexp(Dbg2),
                                   members_dict=enc_members_dict(MembersDict)}).

dec_projection_v1(Bin) ->
    P = #projection_v1{dbg=Dbg,
                       dbg2=Dbg2,
                       members_dict=MembersDict} = binary_to_term(Bin),
    P#projection_v1{dbg=dec_sexp(Dbg),
                    dbg2=dec_sexp(Dbg2),
                    members_dict=dec_members_dict(MembersDict)}.

%%%%%%%%%%%%%%%%%%%

enc_sexp(T) ->
    lists:flatten(io_lib:format("~w.", [T])).

dec_sexp(String) ->
    {ok,Tks,_} = erl_scan:string(String),
    {ok,E} = erl_parse:parse_exprs(Tks),
    {value,Funs,_} = erl_eval:exprs(E,[]),
    Funs.

enc_members_dict(D) ->
    %% Use list_to_binary() here to "flatten" the serialized #p_srvr{}
    [{K, list_to_binary(enc_p_srvr(V))} || {K, V} <- orddict:to_list(D)].

dec_members_dict(List) ->
    orddict:from_list([{K, dec_p_srvr(V)} || {K, V} <- List]).

to_binary(X) when is_list(X) ->
    list_to_binary(X);
to_binary(X) when is_integer(X) ->
    list_to_binary(integer_to_list(X));
to_binary(X) when is_atom(X) ->
    erlang:atom_to_binary(X, latin1);
to_binary(X) when is_binary(X) ->
    X.

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

todo_enc_opaque(X) ->
    erlang:term_to_binary(X).

todo_dec_opaque(X) ->
    erlang:binary_to_term(X).
