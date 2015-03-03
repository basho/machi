# CORFU in Erlang, a prototype

This is a mostly-complete complete prototype implementation of the
CORFU server & client specification.  More details on the papers about
CORFU are mentioned in the `docs/corfurl.md` file.

## Compilation & unit testing

Use `make` and `make test`.  Note that the Makefile assumes that the
`rebar` utility is available somewhere in your path.

## Testing with QuickCheck + PULSE

This model is a bit exciting because it includes all of the following:

* It uses PULSE
* It uses temporal logic to help verify the model's properties
* It also includes a (manual!) fault injection method to help verify
that the model can catch many bugs.  The `eqc_temporal` library uses
a lot of `try/catch` internally, and if your callback code causes an
exception in the "wrong" places, the library will pursue a default
action rather than triggering an error!  The fault injection is an
additional sanity check to verify that the model isn't (obviously)
flawed or broken.
* Uses Lamport clocks to help order happens-before and concurrent events.
* Includes stopping the sequencer (either nicely or brutal kill) to verify
that the logic still works without any active sequencer.
* Includes logic to allow the sequencer to give
**faulty sequencer assignments**, including duplicate page numbers and
gaps of unused pages.

If you have a Quviq QuickCheck license, then you can also use the
`make pulse` target.
Please note the following prerequisites:

* Erlang R16B.  Perhaps R15B might also work, but it has not been
  tested yet.
* Quviq QuickCheck version 1.30.2.  There appears to be an
  `eqc_statem` change in Quviq EQC 1.33.2 that has broken the
  test.  We'll try to fix the test to be able to use 1.33.x or later,
  but it is a lower priority work item for the team right now.

For more information about the PULSE test and how to use it, see the
`docs/using-pulse.md` file.
