
# Fiddling with PULSE

## About the PULSE test

This test is based on `eqc_statem` QuickCheck model, i.e., a
stateful/state machine style test.  Roughly speaking, it does the
following things:

1. Chooses a random number of chains, chain length, and simulated flash
   page size.
2. Generates a random set of stateful commands to run.
3. During the test case run, an event trace log is generated.
4. If there are any `postcondition()` checks that fail, of course,
   QuickCheck will stop the test and start shrinking.
5. If all of the postcondition checks (and the rest of QuickCheck's
   sanity checking) are OK, the event trace log is checked for
   sanity.

### The eqc_statem commands used.

See the `corfurl_pulse:command/1` function for full details.  In
summary:

* `'setup'`, for configuring the # of chains, chain length, simulated
   page size, and whether or not the sequencer is faulty (i.e.,
   gives faulty sequencer assignments, including duplicate page numbers and
   gaps of unused pages).
* `'append'`, for `corfurl_client:append_page/2`
* '`read_approx'`, for `corfurl_client:read_page/2`
* `'scan_forward'`, for `corfurl_client:scan_forward/3`
* `'fill'`, for `corfurl_client:fill_page/2`
* `'trim'`, for `corfurl_client:trim_page/2'
* `'stop_sequencer'`, for `corfurl_sequencer:stop/2'

### Sanity checks for the event trace log

Checking the event trace log for errors is a bit tricky.  The model is
similar to checking a key-value store.  In a simple key-value store
model, we know (in advance) the full key.  However, in CORFU, the
sequencer tells us the key, i.e., the flash page number that an
"append page" operation will use.  So the model must be able to infer
the flash page number from the event trace, then use that page number
as the key for the rest of the key-value-store-like model checks.

This test also uses the `eqc_temporal` library for temporal logic.  I
don't claim to be a master of using temporal logic in general or that
library specifically ... so I hope that I haven't introduced a subtle
bug into the model.  <tt>^_^</tt>.

Summary of the sanity checks of the event trace:

* Do all calls finish?
* Are there any invalid page transitions?  E.g., `written ->
  unwritten` is forbidden.
* Are there any bad reads?  E.g., reading an `error_unwritten` result
  when the page has **definitely** been written/filled/trimmed.
    * Note that temporal logic is used to calculate when we definitely
      know a page's value vs. when we know that a page's value is
      definitely going to change
      **but we don't know exactly when the change has taken place**.

### Manual fault injection

TODO: Automate the fault injection testing, via "erl -D" compilation.

There are (at least) five different types of fault injection that can
be implemented by defining certain Erlang preprocessor symbols at
compilation time of `corfurl_pulse.erl`.

    TRIP_no_append_duplicates
        Will falsely report the LPN (page number) of an append, if the
        actual LPN is 3, as page #3.
    TRIP_bad_read
        Will falsely report the value of a read operation of LPN #3.
    TRIP_bad_scan_forward
        Will falsely report written/filled pages if the # of requested
        pages is equal to 10.
    TRIP_bad_fill
        Will falsely report the return value of a fill operation if the
        requested LPN is between 3 & 5.
    TRIP_bad_trim
        Will falsely report the return value of a trim operation if the
        requested LPN is between 3 & 5.

## Compiling and executing batch-style

Do the following:

    make clean ; make ; make pulse

... then watch the dots go across the screen for 60 seconds.  If you
wish, you can press `Control-c` to interrupt the test.  We're really
interested in the build artifacts.

## Executing interactively at the REPL shell

After running `make pulse`, use the following two commands to start an
Erlang REPL shell and run a test for 5 seconds.

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
2. Run `env USE_PULSE=1 rebar skip_deps=true -D PULSE eunit suites=SKIP`
   to compile.
3. Reload any recompiled modules, e.g. `l(corfurl_pulse).`
4. Resume QuickCheck activities.

## Seeing an PULSE scheduler interleaving failure in action

1. Edit `corfurl_pulse:check_trace()` to uncomment the
   use of `conjunction()` that mentions the `{bogus_no_gaps, ...}` tuple.
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
