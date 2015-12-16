%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(machi_lifecycle_mgr_test).
-compile(export_all).

-ifdef(TEST).
-ifndef(PULSE).

-include_lib("eunit/include/eunit.hrl").

-include("machi.hrl").
-include("machi_projection.hrl").

-define(MGR, machi_chain_manager1).

setup() ->
    catch application:stop(machi),
    {ok, SupPid} = machi_sup:start_link(),
    error_logger:tty(false),
    Dir = "./" ++ atom_to_list(?MODULE) ++ ".datadir",
    machi_flu1_test:clean_up_data_dir(Dir ++ "/*/*"),
    machi_flu1_test:clean_up_data_dir(Dir),
    Envs = [{flu_data_dir, Dir ++ "/data/flu"},
            {flu_config_dir, Dir ++ "/etc/flu-config"},
            {chain_config_dir, Dir ++ "/etc/chain-config"},
            {platform_data_dir, Dir ++ "/data"},
            {platform_etc_dir, Dir ++ "/etc"},
            {not_used_pending, Dir ++ "/etc/pending"}
           ],
    EnvKeys = [K || {K,_V} <- Envs],
    undefined = application:get_env(machi, yo),
    Cleanup = machi_flu1_test:get_env_vars(machi, EnvKeys ++ [yo]),
    [begin
         filelib:ensure_dir(V ++ "/unused"),
         application:set_env(machi, K, V)
     end || {K, V} <- Envs],
    {SupPid, Dir, Cleanup}.

cleanup({SupPid, Dir, Cleanup}) ->
    exit(SupPid, normal),
    machi_util:wait_for_death(SupPid, 100),
    error_logger:tty(true),
    catch application:stop(machi),
    machi_flu1_test:clean_up_data_dir(Dir ++ "/*/*"),
    machi_flu1_test:clean_up_data_dir(Dir),
    machi_flu1_test:clean_up_env_vars(Cleanup),
    undefined = application:get_env(machi, yo),
    ok.

smoke_test_() ->
    {timeout, 60, fun() -> smoke_test2() end}.

smoke_test2() ->
    YoCleanup = setup(),
    try
        Prefix = <<"pre">>,
        Chunk1 = <<"yochunk">>,
        Host = "localhost",
        PortBase = 60120,

        Pa = #p_srvr{name=a,address="localhost",port=PortBase+0},
        Pb = #p_srvr{name=b,address="localhost",port=PortBase+1},
        Pc = #p_srvr{name=c,address="localhost",port=PortBase+2},
        %% Pstore_a = machi_flu1:make_projection_server_regname(a),
        %% Pstore_b = machi_flu1:make_projection_server_regname(b),
        %% Pstore_c = machi_flu1:make_projection_server_regname(c),
        Pstores = [Pstore_a, Pstore_b, Pstore_c] =
            [machi_flu1:make_projection_server_regname(a),
             machi_flu1:make_projection_server_regname(b),
             machi_flu1:make_projection_server_regname(c)],
        ChMgrs = [ChMgr_a, ChMgr_b, ChMgr_c] =
            [machi_chain_manager1:make_chmgr_regname(a),
             machi_chain_manager1:make_chmgr_regname(b),
             machi_chain_manager1:make_chmgr_regname(c)],
        Fits = [Fit_a, Fit_b, Fit_c] =
            [machi_flu_psup:make_fitness_regname(a),
             machi_flu_psup:make_fitness_regname(b),
             machi_flu_psup:make_fitness_regname(c)],
        Advance = machi_chain_manager1_test:make_advance_fun(
                    Fits, [a,b,c], ChMgrs, 3),

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("\nSTEP: Start 3 FLUs, no chain.\n", []),

        [machi_lifecycle_mgr:make_pending_config(P) || P <- [Pa,Pb,Pc] ],
        {[_,_,_],[]} = machi_lifecycle_mgr:process_pending(),
        [{ok, #projection_v1{epoch_number=0}} =
             machi_projection_store:read_latest_projection(PSTORE, private)
         || PSTORE <- Pstores],

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("\nSTEP: Start chain = [a,b,c]\n", []),

        C1 = #chain_def_v1{name=cx, mode=ap_mode, full=[Pa,Pb,Pc],
                           local_run=[a,b,c]},
        machi_lifecycle_mgr:make_pending_config(C1),
        {[],[_]} = machi_lifecycle_mgr:process_pending(),
        Advance(),
        [{ok, #projection_v1{all_members=[a,b,c]}} =
             machi_projection_store:read_latest_projection(PSTORE, private)
         || PSTORE <- Pstores],

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("\nSTEP: Reset chain = [b,c]\n", []),

        C2 = #chain_def_v1{name=cx, mode=ap_mode, full=[Pb,Pc],
                           old_full=[a,b,c], old_witnesses=[],
                           local_stop=[a], local_run=[b,c]},
        machi_lifecycle_mgr:make_pending_config(C2),
        {[],[_]} = machi_lifecycle_mgr:process_pending(),
        Advance(),
        %% a should be down
        {'EXIT', _} = (catch machi_projection_store:read_latest_projection(
                               hd(Pstores), private)),
        [{ok, #projection_v1{all_members=[b,c]}} =
             machi_projection_store:read_latest_projection(PSTORE, private)
         || PSTORE <- tl(Pstores)],

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        io:format("\nSTEP: Reset chain = []\n", []),

        C3 = #chain_def_v1{name=cx, mode=ap_mode, full=[],
                           old_full=[b,c], old_witnesses=[],
                           local_stop=[b,c], local_run=[]},
        machi_lifecycle_mgr:make_pending_config(C3),
        {[],[_]} = machi_lifecycle_mgr:process_pending(),
        Advance(),
        %% a,b,c should be down
        [{'EXIT', _} = (catch machi_projection_store:read_latest_projection(
                                PSTORE, private))
         || PSTORE <- Pstores],

        ok
    after
        cleanup(YoCleanup)
    end.

ast_tuple_syntax_test() ->
    T = fun(L) -> machi_lifecycle_mgr:check_ast_tuple_syntax(L) end,
    Canon1 = [ {host, "localhost", []},
               {host, "localhost", [{client_interface, "1.2.3.4"},
                                    {admin_interface, "5.6.7.8"}]},
               {flu, 'fx', "foohost", 4000, []},
               switch_old_and_new,
               {chain, 'cy', ['fx', 'fy'], [{foo,"yay"},{bar,baz}]} ],

    {_Good,[]=_Bad} = T(Canon1),
    Canon1_norm = machi_lifecycle_mgr:normalize_ast_tuple_syntax(Canon1),
    true = (length(Canon1) == length(Canon1_norm)),
    {Canon1_norm_b, []} = T(Canon1_norm),
    true = (length(Canon1_norm) == length(Canon1_norm_b)),

    {[],[_,_,_,_]} =
        T([ {host, 'localhost', []},
            {host, 'localhost', yo},
            {host, "localhost", [{client_interface, 77.88293829832}]},
            {host, "localhost", [{client_interface, "1.2.3.4"},
                                 {bummer, "5.6.7.8"}]} ]),
    {[],[_,_,_,_,_,_]} =
        T([ {flu, 'fx', 'foohost', 4000, []},
            {flu, 'fx', <<"foohost">>, 4000, []},
            {flu, 'fx', "foohost", -4000, []},
            {flu, 'fx', "foohost", 40009999, []},
            {flu, 'fx', "foohost", 4000, gack},
            {flu, 'fx', "foohost", 4000, [22]} ]),
    {[],[_,_,_]} =
        T([ {chain, 'cy', ["fx", "fy"], [foo,{bar,baz}]},
            yoloyolo,
            {chain, "cy", ["fx", 27], oops,arity,way,way,way,too,big,x}
          ]).

ast_run_test() ->
    PortBase = 20300,
    R1 = [
          {host, "localhost", "localhost", "localhost", []},
          {flu, 'f0', "localhost", PortBase+0, []},
          {flu, 'f1', "localhost", PortBase+1, []},
          {chain, 'ca', ['f0'], []},
          {chain, 'cb', ['f1'], []},
          switch_old_and_new,
          {flu, 'f2', "localhost", PortBase+2, []},
          {flu, 'f3', "localhost", PortBase+3, []},
          {flu, 'f4', "localhost", PortBase+4, []},
          {chain, 'ca', ['f0', 'f2'], []},
          {chain, 'cc', ['f3', 'f4'], []}
         ],

    {ok, Env1} = machi_lifecycle_mgr:run_ast(R1),
    %% Uncomment to examine the Env trees.
    %% Y1 = {lists:sort(gb_trees:to_list(element(1, Env1))),
    %%       lists:sort(gb_trees:to_list(element(2, Env1))),
    %%       element(3, Env1)},
    %% io:format(user, "\nY1 ~p\n", [Y1]),

    Negative_after_R1 =
        [
          {host, "localhost", "foo", "foo", []}, % dupe host
          {flu, 'f1', "other", PortBase+9999999, []}, % bogus port # (syntax)
          {flu, 'f1', "other", PortBase+888, []}, % dupe flu name
          {flu, 'f7', "localhost", PortBase+1, []}, % dupe host+port
          {chain, 'ca', ['f7'], []}, % unknown flu
          {chain, 'cc', ['f0'], []}, % flu previously assigned
          {chain, 'ca', cp_mode, ['f0', 'f1', 'f2'], [], []} % mode change
        ],
    [begin
         %% io:format(user, "dbg: Neg ~p\n", [Neg]),
         {error, _} = machi_lifecycle_mgr:run_ast(R1 ++ [Neg])
     end || Neg <- Negative_after_R1],

    %% The 'run' phase doesn't blow smoke.  What about 'diff'?
    {X1a, X1b} = machi_lifecycle_mgr:diff_env(Env1, "localhost"),
    %% There's only one host, "localhost", so 'all' should be exactly equal.
    {X1a, X1b} = machi_lifecycle_mgr:diff_env(Env1, all),
    %% io:format(user, "X1b: ~p\n", [X1b]),

    %% Append to the R1 scenario: for chain cc: add f5, remove f4
    %% Expect: see pattern matching below on X2b.
    R2 = (R1 -- [switch_old_and_new]) ++
        [switch_old_and_new,
         {flu, 'f5', "localhost", PortBase+5, []},
         {chain, 'cc', ['f3','f5'], []}],
    {ok, Env2} = machi_lifecycle_mgr:run_ast(R2),
    {_X2a, X2b} = machi_lifecycle_mgr:diff_env(Env2, "localhost"),
    %% io:format(user, "X2b: ~p\n", [X2b]),
    F5_port = PortBase+5,
    [#p_srvr{name='f5',address="localhost",port=F5_port},
     #chain_def_v1{name='cc',
                   full=[#p_srvr{name='f3'},#p_srvr{name='f5'}], witnesses=[],
                   old_full=[f3,f4], old_witnesses=[],
                   local_run=[f5], local_stop=[f4]}] = X2b,

    ok.

ast_then_apply_test_() ->
    {timeout, 60, fun() -> ast_then_apply_test2() end}.

ast_then_apply_test2() ->
    YoCleanup = setup(),
    try
        PortBase = 20400,
        NumChains = 4,
        ChainLen = 3,
        FLU_num = NumChains * ChainLen,
        FLU_defs = [{flu, list_to_atom("f"++integer_to_list(X)),
                     "localhost", PortBase+X, []} || X <- lists:seq(1,FLU_num)],
        FLU_names = [FLU || {flu,FLU,_,_,_} <- FLU_defs],
        Ch_defs = [{chain, list_to_atom("c"++integer_to_list(X)),
                    lists:sublist(FLU_names, X, 3),
                    []} || X <- lists:seq(1, FLU_num, 3)],

        R1 = [switch_old_and_new,
              {host, "localhost", "localhost", "localhost", []}]
             ++ FLU_defs ++ Ch_defs,
        {ok, Env1} = machi_lifecycle_mgr:run_ast(R1),
        {_X1a, X1b} = machi_lifecycle_mgr:diff_env(Env1, "localhost"),
        %% io:format(user, "X1b ~p\n", [X1b]),
        [machi_lifecycle_mgr:make_pending_config(X) || X <- X1b],
        {PassFLUs, PassChains} = machi_lifecycle_mgr:process_pending(),
        true = (length(PassFLUs) == length(FLU_defs)),
        true = (length(PassChains) == length(Ch_defs)),
             
        %% Kick the chain managers into doing something useful right now.
        Pstores = [list_to_atom(atom_to_list(X) ++ "_pstore") || X <- FLU_names],
        Fits = [list_to_atom(atom_to_list(X) ++ "_fitness") || X <- FLU_names],
        ChMgrs = [list_to_atom(atom_to_list(X) ++ "_chmgr") || X <- FLU_names],
        Advance = machi_chain_manager1_test:make_advance_fun(
                    Fits, FLU_names, ChMgrs, 3),
        Advance(),

        %% Sanity check: everyone is configured properly.
        [begin
             {ok, #projection_v1{epoch_number=Epoch, all_members=All,
                                 chain_name=ChainName, upi=UPI}} =
                 machi_projection_store:read_latest_projection(PStore, private),
             %% io:format(user, "~p: epoch ~p all ~p\n", [PStore, Epoch, All]),
             true = Epoch > 0,
             ChainLen = length(All),
             true = (length(UPI) > 0),
             {chain, _, Full, []} = lists:keyfind(ChainName, 2, Ch_defs),
             true = lists:sort(Full) == lists:sort(All)
         end || PStore <- Pstores],
        
        ok
    after
        cleanup(YoCleanup)
    end.

-endif. % !PULSE
-endif. % TEST
