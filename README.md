# Machi: a distributed, decentralized blob/large file store

  [Travis-CI](http://travis-ci.org/basho/machi) :: ![Travis-CI](https://secure.travis-ci.org/basho/machi.png)

Outline

1. [Why another blob/file store?](#sec1)
2. [Where to learn more about Machi](#sec2)
3. [Development status summary](#sec3)
4. [Contributing to Machi's development](#sec4)

<a name="sec1">
## 1. Why another blob/file store?

Our goal is a robust & reliable, distributed, highly available, large
file and blob store.  Such stores already exist, both in the open source world
and in the commercial world.  Why reinvent the wheel?  We believe
there are three reasons, ordered by decreasing rarity.

1. We want end-to-end checksums for all file data, from the initial
   file writer to every file reader, anywhere, all the time.
2. We need flexibility to trade consistency for availability:
   e.g. weak consistency in exchange for being available in cases
   of partial system failure.
3. We want to manage file replicas in a way that's provably correct
   and also easy to test.

Criteria #3 is difficult to find in the open source world but perhaps
not impossible.

If we have app use cases where availability is more important than
consistency, then systems that meet criteria #2 are also rare.
Most file stores provide only strong consistency and therefore
have unavoidable, unavailable behavior when parts of the system
fail.
What if we want a file store that is always available to write new
file data and attempts best-effort file reads?

If we really do care about data loss and/or data corruption, then we
really want both #3 and #1.  Unfortunately, systems that meet
criteria #1 are _very rare_.  (Nonexistant?)
Why?  This is 2015.  We have decades of research that shows
that computer hardware can (and
indeed does) corrupt data at nearly every level of the modern
client/server application stack.  Systems with end-to-end data
corruption detection should be ubiquitous today.  Alas, they are not.

Machi is an effort to change the deplorable state of the world, one
Erlang function at a time.

<a name="sec2">
## 2. Where to learn more about Machi

The two major design documents for Machi are now mostly stable.
Please see the [doc](./doc) directory's [README](./doc) for details.

We also have a
[Frequently Asked Questions (FAQ) list](./FAQ.md).

Scott recently (November 2015) gave a presentation at the
[RICON 2015 conference](http://ricon.io) about one of the techniques
used by Machi; "Managing Chain Replication Metadata with
Humming Consensus" is available online now.
* [slides (PDF format)](http://ricon.io/speakers/slides/Scott_Fritchie_Ricon_2015.pdf)
* [video](https://www.youtube.com/watch?v=yR5kHL1bu1Q)

See later in this document for how to run the Humming Consensus demos,
including the network partition simulator.

<a name="sec3">
## 3. Development status summary

Mid-March 2016: The Machi development team has been downsized in
recent months, and the pace of development has slowed.  Here is a
summary of the status of Machi's major components.

* Humming Consensus and the chain manager
  * No new safety bugs have been found by model-checking tests.
  * A new document,
    (Hand-on experiments with Machi and Humming Consensus)[doc/humming-consensus-demo.md]
    is now available.  It is a tutorial for setting up a 3 virtual
    machine Machi cluster and how to demonstrate the chain manager's
    reactions to server stops & starts, crashes & restarts, and pauses
    (simulated by `SIGSTOP` and `SIGCONT`).
  * The chain manager can still make suboptimal-but-safe choices for
    chain transitions when a server hangs/pauses temporarily.
    * Recent chain manager changes have made the instability window
      much shorter when the slow/paused server resumes execution.
    * Scott believes that a modest change to the chain manager's
      calculation of a new projection can reduce flapping in this (and
      many other cases) less likely.  Currently, the new local
      projection is calculated using only local state (i.e., the chain
      manager's internal state + the fitness server's state).
      However, if the "latest" projection read from the public
      projection stores were also input to the new projection
      calculation function, then many obviously bad projections can be
      avoided without needing rounds of Humming Consensus to
      demonstrate that a bad projection is bad.

* FLU/data server process
  * All known correctness bugs have been fixed.
  * Performance has not yet been measured.  Performance measurement
    and enhancements are scheduled to start in the middle of March 2016.
    (This will include a much-needed update to the `basho_bench` driver.)

* Access protocols and client libraries
  * The protocol used by both external clients and internally (instead
    of using Erlang's native message passing mechanisms) is based on
    Protocol Buffers.
    * (Machi PB protocol specification: ./src/machi.proto)[./src/machi.proto]
    * At the moment, the PB specification contains two protocols.
      Sometime in the near future, the spec will be split to separate
      the external client API (the "high" protocol) from the internal
      communication API (the "low" protocol).

* Recent conference talks about Machi
  * Erlang Factory San Francisco 2016
    (the slides and video recording)[http://www.erlang-factory.com/sfbay2016/scott-lystig-fritchie]
    will be available a few weeks after the conference ends on March
    11, 2016.
  * Ricon 2015
    * (The slides)[http://ricon.io/archive/2015/slides/Scott_Fritchie_Ricon_2015.pdf]
    * and the (video recording)[https://www.youtube.com/watch?v=yR5kHL1bu1Q&index=13&list=PL9Jh2HsAWHxIc7Tt2M6xez_TOP21GBH6M]
    are now available.
    * If you would like to run the Humming Consensus code (with or without
    the network partition simulator) as described in the RICON 2015
    presentation, please see the
    [Humming Consensus demo doc](./doc/humming_consensus_demo.md).

<a name="sec4">
## 4. Contributing to Machi's development

### 4.1 License

Basho Technologies, Inc. as committed to licensing all work for Machi
under the
[Apache Public License version 2](./LICENSE).  All authors of source code
and documentation who agree with these licensing terms are welcome to
contribute their ideas in any form: suggested design or features,
documentation, and source code.

Machi is still a very young project within Basho, with a small team of
developers; please bear with us as we grow out of "toddler" stage into
a more mature open source software project.
We invite all contributors to review the
[CONTRIBUTING.md](./CONTRIBUTING.md) document for guidelines for
working with the Basho development team.

### 4.2 Development environment requirements

All development to date has been done with Erlang/OTP version 17 on OS
X.  The only known limitations for using R16 are minor type
specification difference between R16 and 17, but we strongly suggest
continuing development using version 17.

We also assume that you have the standard UNIX/Linux developer
tool chain for C and C++ applications.  Also, we assume
that Git and GNU Make are available.
The utility used to compile the Machi source code,
`rebar`, is pre-compiled and included in the repo.
For more details, please see the
[Machi development environment prerequisites doc](./doc/dev-prerequisites.md).

Machi has a dependency on the
[ELevelDB](https://github.com/basho/eleveldb) library.  ELevelDB only
supports UNIX/Linux OSes and 64-bit versions of Erlang/OTP only; we
apologize to Windows-based and 32-bit-based Erlang developers for this
restriction.

### 4.3 New protocols and features

If you'd like to work on a protocol such as Thrift, UBF,
msgpack over UDP, or some other protocol, let us know by
[opening an issue to discuss it](./issues/new).
