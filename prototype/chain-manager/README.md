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

One of the unit tests spits out **a tremendous amount** of verbose
logging information to the console.  This test, the
`machi_chain_manager1_test:convergence_demo_test()`, isn't the typical
small unit test.  Rather, it (ab)uses the EUnit framework to
automatically run this quite large test together with all of the other
tiny unit tests.

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
