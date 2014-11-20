## CORFU papers

I recommend the "5 pages" paper below first, to give a flavor of
what the CORFU is about.  When Scott first read the CORFU paper
back in 2011 (and the Hyder paper), he thought it was insanity.
He recommends waiting before judging quite so hastily.  :-)

After that, then perhaps take a step back are skim over the
Hyder paper.  Hyder started before CORFU, but since CORFU, the
Hyder folks at Microsoft have rewritten Hyder to use CORFU as
the shared log underneath it.  But the Hyder paper has lots of
interesting bits about how you'd go about creating a distributed
DB where the transaction log *is* the DB.

### "CORFU: A Distributed Shared Log"

MAHESH BALAKRISHNAN, DAHLIA MALKHI, JOHN D. DAVIS, and VIJAYAN
PRABHAKARAN, Microsoft Research Silicon Valley, MICHAEL WEI,
University of California, San Diego, TED WOBBER, Microsoft Research
Silicon Valley

Long version of introduction to CORFU (~30 pages)
http://www.snookles.com/scottmp/corfu/corfu.a10-balakrishnan.pdf

### "CORFU: A Shared Log Design for Flash Clusters"

Same authors as above

Short version of introduction to CORFU paper above (~12 pages)

http://www.snookles.com/scottmp/corfu/corfu-shared-log-design.nsdi12-final30.pdf

### "From Paxos to CORFU: A Flash-Speed Shared Log"

Same authors as above

5 pages, a short summary of CORFU basics and some trial applications
that have been implemented on top of it.

http://www.snookles.com/scottmp/corfu/paxos-to-corfu.malki-acmstyle.pdf

### "Beyond Block I/O: Implementing a Distributed Shared Log in Hardware"

Wei, Davis, Wobber, Balakrishnan, Malkhi

Summary report of implmementing the CORFU server-side in
FPGA-style hardware. (~11 pages)

http://www.snookles.com/scottmp/corfu/beyond-block-io.CameraReady.pdf

### "Tango: Distributed Data Structures over a Shared Log"

Balakrishnan, Malkhi, Wobber, Wu, Brabhakaran, Wei, Davis, Rao, Zou, Zuck

Describes a framework for developing data structures that reside
persistently within a CORFU log: the log *is* the database/data
structure store.

http://www.snookles.com/scottmp/corfu/Tango.pdf

### "Dynamically Scalable, Fault-Tolerant Coordination on a Shared Logging Service"

Wei, Balakrishnan, Davis, Malkhi, Prabhakaran, Wobber

The ZooKeeper inter-server communication is replaced with CORFU.
Faster, fewer lines of code than ZK, and more features than the
original ZK code base.

http://www.snookles.com/scottmp/corfu/zookeeper-techreport.pdf

### "Hyder – A Transactional Record Manager for Shared Flash"

Bernstein, Reid, Das

Describes a distributed log-based DB system where the txn log is
treated quite oddly: a "txn intent" record is written to a
shared common log All participants read the shared log in
parallel and make commit/abort decisions in parallel, based on
what conflicts (or not) that they see in the log.  Scott's first
reading was "No way, wacky" ... and has since changed his mind.

http://www.snookles.com/scottmp/corfu/CIDR11Proceedings.pdf
pages 9-20



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
