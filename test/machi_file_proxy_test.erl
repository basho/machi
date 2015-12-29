%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(machi_file_proxy_test).

-ifdef(TEST).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include("machi.hrl").

clean_up_data_dir(DataDir) ->
    [begin
         Fs = filelib:wildcard(DataDir ++ Glob),
         [file:delete(F) || F <- Fs],
         [file:del_dir(F) || F <- Fs]
     end || Glob <- ["*/*/*/*", "*/*/*", "*/*", "*"] ],
    _ = file:del_dir(DataDir),
    ok.

-ifndef(PULSE).

-define(TESTDIR, "./t").
-define(HYOOGE, 1 * 1024 * 1024 * 1024). % 1 long GB

random_binary_single() ->
    %% OK, I guess it's not that random...
    <<"Four score and seven years ago our fathers brought forth on this
    continent a new nation, conceived in liberty, and dedicated to the
    proposition that all men are created equal.

    Now we are engaged in a great civil war, testing whether that nation, or any
    nation so conceived and so dedicated, can long endure. We are met on a great
    battlefield of that war. We have come to dedicate a portion of that field, as a
    final resting place for those who here gave their lives that that nation
    might live. It is altogether fitting and proper that we should do this.

    But, in a larger sense, we can not dedicate, we can not consecrate, we can not 
    hallow this ground. The brave men, living and dead, who struggled here, have
    consecrated it, far above our poor power to add or detract. The world will
    little note, nor long remember what we say here, but it can never forget what 
    they did here. It is for us the living, rather, to be dedicated here to the 
    unfinished work which they who fought here have thus far so nobly advanced. It
    is rather for us to be here dedicated to the great task remaining before us— 
    that from these honored dead we take increased devotion to that cause for which 
    they gave the last full measure of devotion— that we here highly resolve that 
    these dead shall not have died in vain— that this nation, under God, shall have 
    a new birth of freedom— and that government of the people, by the people, for 
    the people, shall not perish from the earth.">>.

random_binary(Start, End) ->
    Size = byte_size(random_binary_single()) - 1,
    case End > Size of
        true ->
            Copies = ( End div Size ) + 1,
            D0 = binary:copy(random_binary_single(), Copies),
            binary:part(<<D0/binary>>, Start, End);
        false ->
            binary:part(random_binary_single(), Start, End)
    end.

setup() ->
    {ok, Pid} = machi_file_proxy:start_link(fluname, "test", ?TESTDIR),
    Pid.

teardown(Pid) ->
    catch machi_file_proxy:stop(Pid).

machi_file_proxy_test_() ->
    clean_up_data_dir(?TESTDIR),
    {setup,
     fun setup/0,
     fun teardown/1,
     fun(Pid) ->
             [
              ?_assertEqual({error, bad_arg}, machi_file_proxy:read(Pid, -1, -1)),
              ?_assertEqual({error, bad_arg}, machi_file_proxy:write(Pid, -1, <<"yo">>)),
              ?_assertEqual({error, bad_arg}, machi_file_proxy:append(Pid, [], -1, <<"krep">>)),
              ?_assertMatch({ok, {_, []}}, machi_file_proxy:read(Pid, 1, 1)),
              ?_assertEqual({error, not_written}, machi_file_proxy:read(Pid, 1024, 1)),
              ?_assertMatch({ok, {_, []}}, machi_file_proxy:read(Pid, 1, 1024)),
              ?_assertEqual({error, not_written}, machi_file_proxy:read(Pid, 1024, ?HYOOGE)),
              ?_assertEqual({error, not_written}, machi_file_proxy:read(Pid, ?HYOOGE, 1)),
              {timeout, 10,
               ?_assertEqual({error, written}, machi_file_proxy:write(Pid, 1, random_binary(0, ?HYOOGE)))},
              ?_assertMatch({ok, "test", _}, machi_file_proxy:append(Pid, random_binary(0, 1024))),
              ?_assertEqual({error, written}, machi_file_proxy:write(Pid, 1024, <<"fail">>)),
              ?_assertEqual({error, written}, machi_file_proxy:write(Pid, 1, <<"fail">>)),
              ?_assertMatch({ok, {[{_, _, _, _}], []}}, machi_file_proxy:read(Pid, 1025, 1000)),
              ?_assertMatch({ok, "test", _}, machi_file_proxy:append(Pid, [], 1024, <<"mind the gap">>)),
              ?_assertEqual(ok, machi_file_proxy:write(Pid, 2060, [], random_binary(0, 1024)))
             ]
     end}.

multiple_chunks_read_test_() ->
    clean_up_data_dir(?TESTDIR),
    {setup,
     fun setup/0,
     fun teardown/1,
     fun(Pid) ->
             [
              ?_assertEqual(ok, machi_file_proxy:trim(Pid, 0, 1, false)),
              ?_assertMatch({ok, {[], [{"test", 0, 1}]}},
                            machi_file_proxy:read(Pid, 0, 1,
                                                  #read_opts{needs_trimmed=true})),
              ?_assertMatch({ok, "test", _}, machi_file_proxy:append(Pid, random_binary(0, 1024))),
              ?_assertEqual(ok, machi_file_proxy:write(Pid, 10000, <<"fail">>)),
              ?_assertEqual(ok, machi_file_proxy:write(Pid, 20000, <<"fail">>)),
              ?_assertEqual(ok, machi_file_proxy:write(Pid, 30000, <<"fail">>)),
              %% Freeza
              ?_assertEqual(ok, machi_file_proxy:write(Pid, 530000, <<"fail">>)),
              ?_assertMatch({ok, {[{"test", 1024, _, _},
                                   {"test", 10000, <<"fail">>, _},
                                   {"test", 20000, <<"fail">>, _},
                                   {"test", 30000, <<"fail">>, _},
                                   {"test", 530000, <<"fail">>, _}], []}},
                            machi_file_proxy:read(Pid, 1024, 530000)),
              ?_assertMatch({ok, {[{"test", 1, _, _}], [{"test", 0, 1}]}},
                            machi_file_proxy:read(Pid, 0, 1024,
                                                  #read_opts{needs_trimmed=true}))
             ]
     end}.

-endif. % !PULSE
-endif. % TEST.
