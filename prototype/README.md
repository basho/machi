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

## The chain-manager prototype

TODO

## The corfurl prototype

The `corfurl` code is a mostly-complete complete implementation of the
CORFU server & client specification.  More details on the papers about
CORFU are mentioned in the `corfurl/docs/corfurl.md` file.

This code contains a QuickCheck + PULSE test.  If you wish to use it,
please note the usage instructions and restrictions mentioned in the
`README.md` file.

## The demo-day-hack prototype

TODO

## The tango prototype

A quick & dirty prototype of Tango on top of the `prototype/corfurl`
CORFU implementation.  The implementation is powerful enough (barely)
to run concurrently on multiple Erlang nodes.  See its `README.md`
file for limitations, TODO items, etc.
