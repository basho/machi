# The chain manager prototype

This is a very early experiment to try to create a distributed "rough
consensus" algorithm that is sufficient & safe for managing the order
of a Chain Replication chain, its members, and its chain order.  A
name hasn't been chosen yet, though the following are contenders:

* chain self-management
* rough consensus
* humming consensus
* foggy consensus

## Code status: active!

Unlike the other code projects in this repository's `prototype`
directory, the chain management code is still under active
development.  It is quite likely (as of early March 2015) that this
code will be robust enough to move to the "real" Machi code base soon.

The most up-to-date documentation for this prototype will **not** be
found in this subdirectory.  Rather, please see the `doc` directory at
the top of the Machi source repository.
 
## Testing, testing, testing

It's important to implement any Chain Replication chain manager as
close to 100% bug-free as possible.  Any bug can introduce the
possibility of data loss, which is something we must avoid.
Therefore, we will spend a large amount of effort to use as many
robust testing tools and methods as feasible to test this code.

* [Concuerror](http://concuerror.com), a DPOR-based full state space
  exploration tool.  Some preliminary Concuerror tests can be found in the
  `test/machi_flu0_test.erl` module.
* [QuickCheck](http://www.quviq.com/products/erlang-quickcheck/), a
  property-based testing tool for Erlang.  QuickCheck doesn't provide
  the reassurance of 100% state exploration, but it proven quite
  effective at Basho for finding numerous subtle bugs.
* Automatic simulation of arbitrary network partition failures.  This
  code is already in progress and is used, for example, by the
  `test/machi_chain_manager1_test.erl` module.
* TLA+ (future work), to try to create a rigorous model of the
  algorithm and its behavior

If you'd like to work on additional testing of this component, please
[open a new GitHub Issue ticket](https://github.com/basho/machi) with
any questions you have.  Or just open a GitHub pull request.  <tt>^_^</tt>

## Compilation & unit testing

Use `make` and `make test`.  Note that the Makefile assumes that the
`rebar` utility is available somewhere in your path.

Tested using Erlang/OTP R16B and Erlang/OTP 17, both on OS X.

If you wish to run the PULSE test in
`test/machi_chain_manager1_pulse.erl` module, you must use Erlang
R16B and Quviq QuickCheck 1.30.2 -- there is a known problem with
QuickCheck 1.33.2, sorry!  Also, please note that a single iteration
of a PULSE test case in this model can run for 10s of seconds!

Otherwise, it ought to "just work" on other versions of Erlang and on other OS
platforms, but sorry, I haven't tested it.

### Testing with simulated network partitions

See the `doc/chain-self-management-sketch.org` file for details of how
the simulator works.

In summary, the simulator tries to emulate the effect of arbitrary
asymmetric network partitions.  For example, for two simulated nodes A
and B, it's possible to have node A send messages to B, but B cannot
send messages to A.

This kind of one-way message passing is nearly impossible do with
distributed Erlang, because disterl uses TCP.  If a network partition
happens at ISO Layer 2 (for example, due to a bad Ethernet cable that
has a faulty receive wire), the entire TCP connection will hang rather
than deliver disterl messages in only one direction.

### Testing simulated data "repair"

In the Machi documentation, "repair" is a re-syncronization of data
between the UPI members of the chain (see below) and members which
have been down/partitioned/gone-to-Hawaii-for-vacation for some period
of time and may have state which is out-of-sync with the rest of the
active-and-running-and-fully-in-sync chain members.

A rough-and-inaccurate-but-useful summary of state transitions are:

    down -> repair eligible -> repairing started -> repairing finished -> upi
    
        * Any state can transition back to 'down'
        * Repair interruptions might trigger a transition to
          'repair eligible instead of 'down'.
        * UPI = Update Propagation Invariant (per the original
                Chain Replication paper) preserving members.
                
                I.e., The state stored by any UPI member is fully
                in sync with all other UPI chain members, except
                for new updates which are being processed by Chain
                Replication at a particular instant in time.

In both the PULSE and `convergence_demo*()` tests, there is a
simulated time when a FLU's repair state goes from "repair started" to
"repair finished", which means that the FLU-under-repair is now
eligible to join the UPI portion of the chain as a fully-sync'ed
member of the chain.  The simulation is based on a simple "coin
flip"-style random choice.

The simulator framework is simulating repair failures when a network
partition is detected with the repair destination FLU.  In the "real
world", other kinds of failure could also interrupt the repair
process.

### The PULSE test in machi_chain_manager1_test.erl

As mentioned above, this test is quite slow: it can take many dozens
of seconds to execute a single test case.  However, the test really is using
PULSE to play strange games with Erlang process scheduling.

Unfortnately, the PULSE framework is very slow for this test.  We'd
like something better, so I wrote the
`machi_chain_manager1_test:convergence_demo_test()` test to use most
of the network partition simulator to try to run many more partition
scenarios in the same amount of time.

### machi_chain_manager1_test:convergence_demo1()

This function is intended both as a demo and as a possible
fully-automated sanity checking function (it will throw an exception
when a model failure happens).  It's purpose is to "go faster" than
the PULSE test describe above.  It meets this purpose handily.
However, it doesn't give quite as much confidence as PULSE does that
Erlang process scheduling cannot somehow break algorithm running
inside the simulator.

To execute:

    make test
    erl -pz ./.eunit deps/*/ebin
    ok = machi_chain_manager1_test:convergence_demo1().

In summary:

* Set up four FLUs, `[a,b,c,d]`, to be used for the test
* Set up a set of random asymmetric network partitions, based on a
  'seed' for a pseudo-random number generator.  Each call to the
  partition simulator may yield a different partition scenario ... so
  the simulated environment is very unstable.
* Run the algorithm for a while so that it has witnessed the partition
  instability for a long time.
* Set the partitions definition to a fixed `[{a,b}]`, meaning that FLU `a`
  cannot send messages to FLU `b`, but all other communication
  (including messages from `b -> a`) works correctly.
* Run the algorithm, wait for everyone to settle on rough consensus.
* Set the partition definition to wildly random again.
* Run the algorithm for a while so that it has witnessed the partition
  instability for a long time.
* Set the partitions definition to a fixed `[{a,c}]`.
* Run the algorithm, wait for everyone to settle on rough consensus.
* Set the partitions definition to a fixed `[]`, i.e., there are no
  network partitions at all.
* Run the algorithm, wait for everyone to settle on a **unanimous value**
  of some ordering of all four FLUs.

To try to understand the simulator's output, let's look at some examples:

    20:12:59.120 c uses: [{epoch,1023},{author,d},{upi,[c,b,d,a]},{repair,[]},{down,[]},{d,[{author_proc,react},{ps,[]},{nodes_up,[a,b,c,d]}]},{d2,[{network_islands,[[a,b,c,d]]},{hooray,{v2,{2015,3,3},{20,12,59}}}]}]

So, server C has decided the following, as far as it can tell:

* Epoch 1023 is the latest epoch
* There's already a projection written to the "public" projection stores by author server D.
* C has decided that D's proposal is the best out of all that C can see in the "public" projection stores plus its own calculation
* The UPI/active chain order is: C (head), B, D, A (tail).
* No servers are under repair
* No servers are down.
* Then there's some other debugging/status info in the 'd' and 'd2' data attributes
    * The 'react' to outside stimulus triggered the author's action
    * The 'ps' says that there are no network partitions *inside the simulator* (yes, that's cheating, but sooo useful for debugging)
    * All 4 nodes are believed up
    * (aside) The 'ps' partition list describes nodes that cannot talk to each other.
    * For easier debugging/visualization, the 'network_islands' converts 'ps' into lists of "islands" where nodes can talk to each other.
    * So 'network_islands' says that A&B&C&D can all message each other, as far as author D understands at the moment.
    * Hooray, the decision was made at 20:12:59 on 2015-03-03.

So, let's see a tiny bit of what happens when there's an asymmetric
network partition.  Note that no consensus has yet been reached:
participants are still churning/uncertain.

    20:12:48.420 a uses: [{epoch,1011},{author,a},{upi,[a,b]},{repair,[d]},{down,[c]},{d,[{author_proc,react},{ps,[{a,c}]},{nodes_up,[a,b,d]}]},{d2,[{network_islands,[na_reset_by_always]},{hooray,{v2,{2015,3,3},{20,12,48}}}]}]
    20:12:48.811 d uses: [{epoch,1012},{author,d},{upi,[a]},{repair,[b,c,d]},{down,[]},{d,[{author_proc,react},{ps,[{a,c}]},{nodes_up,[a,b,c,d]}]},{d2,[{network_islands,[na_reset_by_always]},{hooray,{v2,{2015,3,3},{20,12,48}}}]}]
    {FLAP: a flaps 5}!

* The simulator says that the one-way partition definition is `{ps,[{a,c}]}`. This is authoritative info from the simulator.  The algorithm *does not* use this source of info, however!
* Server A believes that `{nodes_up,[a,b,d]}`.  A is a victim of the simulator's partitioning, so this belief is correct relative to A.
* Server D believes that `{nodes_up,[a,b,c,d]}`.  D doesn't have any simulator partition, so this belief is also correct relative to D.
* A participant has now noticed that server A has "flapped": it has
  proposed the same proposal at least 5 times in a row.  This kind of
  pattern is indicative of an asymmetric partition ... which is indeed
  what is happening at this moment.
