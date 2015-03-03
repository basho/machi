
# Tango prototype

This is a quick hack, just to see how quick & easy it might be to
build Tango on top of corfurl.  It turned out to be pretty quick and
easy.

This prototype does not include any datatype-specific APIs, such as an
HTTP REST interface for manipulating a queue.  The current API is
native Erlang only.  However, because the Tango client communicates to
the underlying CORFU log via the `corfurl` interface, this
implementation is powerful enough to run concurrently on multiple
Erlang nodes.

This implementation does not follow the same structure as described in
the Tango paper.  I made some changes, based on some guesses/partial
understanding of the paper.  If I were to start over again, I'd try to
use the exact same naming scheme & structure suggested by the paper.

## Testing environment

Tested using Erlang/OTP R16B and Erlang/OTP 17, both on OS X.

It ought to "just work" on other versions of Erlang and on other OS
platforms, but sorry, I haven't tested it.

Use `make` and `make test` to compile and run unit tests.

## Data types implemented

* OID mapper
* Simple single-value register
* Map (i.e., multi-value register or basic key-value store)
* Queue
    * Used the Erlang/OTP `queue.erl` library for rough inspiration
    * Operations: is_empty, length, peek, to_list, member, in, out,
      reverse, filter.
    * Queue mutation operations are not idempotent with respect to
      multiple writes in the underlying CORFU log, e.g., due to CORFU
      log reconfiguration or partial write error/timeouts.

## Experimental idea: built-in OID checkpointing

I was toying with the idea of adding a Tango "history splicing"
operation that could make the implementation per-OID checkpoint &
garbage collection (and CORFU-level trimming) operations much easier.
I think that this might be a very good idea and that it deserves more
research & work.

The implementation of the checkpointing & splicing as it is today is
flawed.  See the TODO list below for more details.

## Information about the Tango paper

"Tango: Distributed Data Structures over a Shared Log"

Balakrishnan, Malkhi, Wobber, Wu, Brabhakaran, Wei, Davis, Rao, Zou, Zuck

Describes a framework for developing data structures that reside
persistently within a CORFU log: the log *is* the database/data
structure store.

http://www.snookles.com/scottmp/corfu/Tango.pdf

See also, `../corfu/docs/corfurl.md` for more information on CORFU
research papers.

## TODO list

__ The src/corfu* files in this sub-repo differ from the original
   prototype source files in the ../corfu sub-repo, sorry!

__ The current checkpoint implementation is fundamentally broken and
   needs a rewrite, or else.
   This issue is not mentioned at all in the Tango paper.

   option 1: fix checkpoint to be 100% correct
   option 2: checkpointing is for the weak and the memory-constrained, so
             don't bother.  Instead, rip out the current checkpoint code,
             period. 
   option 3: other

xx Checkpoint fix option #1: history splicing within the same OID?

xx Checkpoint fix option #2: checkpoint to a new OID, history writes to both
                             OIDs during the CP, then a marker in the old OID
                             to switch over to the new OID?

History splicing has a flaw that I belive just won't work.  The switch to a
new OID has problems with updates written to the old OID before and before the
new checkpoint has finished.

I believe that a checkpoint where:
    * all Tango writes, checkpoint and non-checkpoint alike, are noted with
      a checkpoint number.
    * that checkpoint number is strictly increasing
    * a new checkpoint has a new checkpoint number
    * scans ignore blocks with checkpoint numbers larger than the current
      active checkpoint #, until the checkpoint is complete.

... ought to work correctly.  
