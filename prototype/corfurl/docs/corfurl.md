
## Fiddling with PULSE

Do the following:

    make clean
    make
    make pulse

... then watch the dots go across the screen for 60 seconds.  If you
wish, you can press `Control-c` to interrupt the test.  We're really
interested in the build artifacts.

    erl -pz .eunit deps/*/ebin
    eqc:quickcheck(eqc:testing_time(5, corfurl_pulse:prop_pulse())).

This will run the PULSE test for 5 seconds.  Feel free to adjust for
as many seconds as you wish.

    Erlang R16B02-basho4 (erts-5.10.3) [source] [64-bit] [smp:8:8] [async-threads:10] [hipe] [kernel-poll:false] [dtrace]
    
    Eshell V5.10.3  (abort with ^G)
    1> eqc:quickcheck(eqc:testing_time(5, corfurl_pulse:prop_pulse())).
    Starting Quviq QuickCheck version 1.30.4
       (compiled at {{2014,2,7},{9,19,50}})
    Licence for Basho reserved until {{2014,2,17},{1,41,39}}
    ......................................................................................
    OK, passed 86 tests
    schedule:    Count: 86   Min: 2   Max: 1974   Avg: 3.2e+2   Total: 27260
    true
    2> 

REPL interactive work can be done via:

1. Edit code, e.g. `corfurl_pulse.erl`.
2. Run `env BITCASK_PULSE=1 ./rebar skip_deps=true -D PULSE eunit suites=SKIP`
to compile.
3. Reload any recompiled modules, e.g. `l(corfurl_pulse).`
4. Resume QuickCheck activities.

## Seeing an PULSE scheduler interleaving failure in action

1. Edit `corfurl_pulse:check_trace()` to uncomment the
   use of `conjunction()` that mentions `bogus_order_check_do_not_use_me`
   and comment out the real `conjunction()` call below it.
2. Recompile & reload.
3. Check.

For example:

    9> eqc:quickcheck(eqc:testing_time(5, corfurl_pulse:prop_pulse())).
    .........Failed! After 9 tests.

Sweet!  The first tuple below are the first `?FORALL()` values,
and the 2nd is the list of commands,
`{SequentialCommands, ListofParallelCommandLists}`.  The 3rd is the
seed used to perturb the PULSE scheduler.

In this case, `SequentialCommands` has two calls (to `setup()` then
`append()`) and there are two parallel procs: one makes 1 call
call to `append()` and the other makes 2 calls to `append()`.

    {2,2,9}
    {{[{set,{var,1},{call,corfurl_pulse,setup,[2,2,9]}}],
      [[{set,{var,3},
             {call,corfurl_pulse,append,
                   [{var,1},<<231,149,226,203,10,105,54,223,147>>]}}],
       [{set,{var,2},
             {call,corfurl_pulse,append,
                   [{var,1},<<7,206,146,75,249,13,154,238,110>>]}},
        {set,{var,4},
             {call,corfurl_pulse,append,
                   [{var,1},<<224,121,129,78,207,23,79,216,36>>]}}]]},
     {27492,46961,4884}}

Here are our results:

    simple_result: passed
    errors: passed
    events: failed
    identity: passed
    bogus_order_check_do_not_use_me: failed
    [{ok,1},{ok,3},{ok,2}] /= [{ok,1},{ok,2},{ok,3}]

Our (bogus!) order expectation was violated.  Shrinking!

    simple_result: passed
    errors: passed
    events: failed
    identity: passed
    bogus_order_check_do_not_use_me: failed
    [{ok,1},{ok,3},{ok,2}] /= [{ok,1},{ok,2},{ok,3}]

Shrinking was able to remove two `append()` calls and to shrink the
size of the pages down from 9 bytes down to 1 byte.

    Shrinking........(8 times)
    {1,1,1}
    {{[{set,{var,1},{call,corfurl_pulse,setup,[1,1,1]}}],
      [[{set,{var,3},{call,corfurl_pulse,append,[{var,1},<<0>>]}}],
       [{set,{var,4},{call,corfurl_pulse,append,[{var,1},<<0>>]}}]]},
     {27492,46961,4884}}
    events: failed
    bogus_order_check_do_not_use_me: failed
    [{ok,2},{ok,1}] /= [{ok,1},{ok,2}]
    false
