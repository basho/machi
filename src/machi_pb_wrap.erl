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
                props=enc_sexp(Props)}.

conv_to_p_srvr(#mpb_p_srvr{name=Name,
                           proto_mod=ProtoMod,
                           address=Address,
                           port=Port,
                           props=Props}) ->
    #p_srvr{name=to_atom(Name),
            proto_mod=to_atom(ProtoMod),
            address=to_list(Address),
            port=to_integer(Port),
            props=dec_sexp(to_list(Props))}.

enc_projection_v1(P) ->
    %% Awww, flatten it here
    list_to_binary(
      machi_pb:encode_mpb_projectionv1(conv_from_projection_v1(P))).

dec_projection_v1(Bin) ->
    conv_to_projection_v1(machi_pb:decode_mpb_projectionv1(Bin)).

conv_from_projection_v1(#projection_v1{epoch_number=Epoch,
                                       epoch_csum=CSum,
                                       author_server=Author,
                                       all_members=AllMembers,
                                       creation_time=CTime,
                                       mode=Mode,
                                       upi=UPI,
                                       repairing=Repairing,
                                       down=Down,
                                       flap=Flap,
                                       inner=Inner,
                                       dbg=Dbg,
                                       dbg2=Dbg2,
                                       members_dict=MembersDict}) ->
    #mpb_projectionv1{epoch_number=Epoch,
                      epoch_csum=CSum,
                      author_server=to_list(Author),
                      all_members=[to_list(X) || X <- AllMembers],
                      creation_time=conv_from_now(CTime),
                      mode=conv_from_mode(Mode),
                      upi=[to_list(X) || X <- UPI],
                      repairing=[to_list(X) || X <- Repairing],
                      down=[to_list(X) || X <- Down],
                      opaque_flap=enc_optional_sexp(Flap),
                      opaque_inner=enc_optional_sexp(Inner),
                      opaque_dbg=enc_sexp(Dbg),
                      opaque_dbg2=enc_sexp(Dbg2),
                      members_dict=conv_from_members_dict(MembersDict)}.

conv_to_projection_v1(#mpb_projectionv1{epoch_number=Epoch,
                                        epoch_csum=CSum,
                                        author_server=Author,
                                        all_members=AllMembers,
                                        creation_time=CTime,
                                        mode=Mode,
                                        upi=UPI,
                                        repairing=Repairing,
                                        down=Down,
                                        opaque_flap=Flap,
                                        opaque_inner=Inner,
                                        opaque_dbg=Dbg,
                                        opaque_dbg2=Dbg2,
                                        members_dict=MembersDict}) ->
    #projection_v1{epoch_number=Epoch,
                   epoch_csum=CSum,
                   author_server=to_atom(Author),
                   all_members=[to_atom(X) || X <- AllMembers],
                   creation_time=conv_to_now(CTime),
                   mode=conv_to_mode(Mode),
                   upi=[to_atom(X) || X <- UPI],
                   repairing=[to_atom(X) || X <- Repairing],
                   down=[to_atom(X) || X <- Down],
                   flap=dec_optional_sexp(Flap),
                   inner=dec_optional_sexp(Inner),
                   dbg=dec_sexp(Dbg),
                   dbg2=dec_sexp(Dbg2),
                   members_dict=conv_to_members_dict(MembersDict)}.

%%%%%%%%%%%%%%%%%%%

enc_sexp(T) ->
    lists:flatten(io_lib:format("~w.", [T])).

dec_sexp(Bin) when is_binary(Bin) ->
    dec_sexp(binary_to_list(Bin));
dec_sexp(String) when is_list(String) ->
    {ok,Tks,_} = erl_scan:string(String),
    {ok,E} = erl_parse:parse_exprs(Tks),
    {value,Funs,_} = erl_eval:exprs(E,[]),
    Funs.

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

conv_from_now({A,B,C}) ->
    #mpb_now{sec=(1000000 * A) + B,
             usec=C}.

conv_to_now(#mpb_now{sec=Sec, usec=USec}) ->
    {Sec div 1000000, Sec rem 1000000, USec}.

conv_from_mode(ap_mode) -> 'AP_MODE';
conv_from_mode(cp_mode) -> 'CP_MODE'.

conv_to_mode('AP_MODE') -> ap_mode;
conv_to_mode('CP_MODE') -> cp_mode.

