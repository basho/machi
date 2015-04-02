# Prototype directory

The contents of the `prototype` directory is the result of
consolidating several small & independent repos.  Originally, each
small was a separate prototype/quick hack for experimentation
purposes.  The code is preserved here for use as:

* Examples of what not to do ... the code **is** a bit ugly, after
  all.  <tt>^_^</tt>
* Some examples of what to do when prototyping in Erlang.  For
  example, "Let it crash" style coding is so nice to hack on quickly.
* Some code might actually be reusable, as-is or after some
  refactoring.

The prototype code here is not meant for long-term use or
maintenance.  We are unlikely to accept changes/pull requests for adding
large new features or to build full Erlang/OTP applications using this
code only.

However, pull requests for small changes, such as support for
newer Erlang versions (e.g., Erlang 17), will be gladly accepted.
We will also accept fixes for bugs in the test code.

## The corfurl prototype

The `corfurl` code is a mostly-complete complete implementation of the
CORFU server & client specification.  More details on the papers about
CORFU are mentioned in the `corfurl/docs/corfurl.md` file.

This code contains a QuickCheck + PULSE test.  If you wish to use it,
please note the usage instructions and restrictions mentioned in the
`README.md` file.

## The demo-day-hack prototype

This code in the `demo-day-hack` is expected to remain static,
as an archive of past "Demo Day" work.

See the top-level README.md file for details on work to move
much of this code out of the `prototype` directory and into real
use elsewhere in the repo.

## The tango prototype

A quick & dirty prototype of Tango on top of the `prototype/corfurl`
CORFU implementation.  The implementation is powerful enough (barely)
to run concurrently on multiple Erlang nodes.  See its `README.md`
file for limitations, TODO items, etc.

## The chain-manager prototype

This is a very early experiment to try to create a distributed "rough
consensus" algorithm that is sufficient & safe for managing the order
of a Chain Replication chain, its members, and its chain order.

* Code status: **active**!

Unlike the other code projects in this repository's `prototype`
directory, the chain management code is still under active
development.  It is quite likely (as of early March 2015) that this
code will be robust enough to move to the "real" Machi code base soon.


