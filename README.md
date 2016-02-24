# Machi: a robust & reliable, distributed, highly available, large file store

  [Travis-CI](http://travis-ci.org/basho/machi) :: ![Travis-CI](https://secure.travis-ci.org/basho/machi.png)

Outline

1. [Why another file store?](#sec1)
2. [Where to learn more about Machi](#sec2)
3. [Development status summary](#sec3)
4. [Contributing to Machi's development](#sec4)

<a name="sec1">
## 1. Why another file store?

Our goal is a robust & reliable, distributed, highly available, large
file store.  Such stores already exist, both in the open source world
and in the commercial world.  Why reinvent the wheel?  We believe
there are three reasons, ordered by decreasing rarity.

1. We want end-to-end checksums for all file data, from the initial
   file writer to every file reader, anywhere, all the time.
2. We need flexibility to trade consistency for availability:
   e.g. weak consistency in exchange for being available in cases
   of partial system failure.
3. We want to manage file replicas in a way that's provably correct
   and also easy to test.

Of all the file stores in the open source & commercial worlds, only
criteria #3 is a viable option.  Or so we hope.  Or we just don't
care, and if data gets lost or corrupted, then ... so be it.

If we have app use cases where availability is more important than
consistency, then systems that meet criteria #2 are also rare.
Most file stores provide only strong consistency and therefore
have unavoidable, unavailable behavior when parts of the system
fail.
What if we want a file store that is always available to write new
file data and attempts best-effort file reads?

If we really do care about data loss and/or data corruption, then we
really want both #3 and #1.  Unfortunately, systems that meet
criteria #1 are _very rare_.
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

Mid-December 2015: work is underway.

* In progress:
    * Code refactoring: metadata management using
      [ELevelDB](https://github.com/basho/eleveldb)
    * File repair using file-centric, Merkle-style hash tree.
    * Server-side socket handling is now performed by
      [ranch](https://github.com/ninenines/ranch)
    * QuickCheck tests for file repair correctness
        * 2015-12-15: The EUnit test `machi_ap_repair_eqc` is
          currently failing occasionally because it (correctly) detects
          double-write errors.  Double-write errors will be eliminated
          when the ELevelDB integration work is complete.
    * The `make stage` and `make release` commands can be used to
      create a primitive "package".  Use `./rel/machi/bin/machi console`
      to start the Machi app in interactive mode.  Substitute the word
      `start` instead of console to start Machi in background/daemon
      mode.  The `./rel/machi/bin/machi` command without any arguments
      will give a short usage summary.
    * Chain Replication management using the Humming Consensus
      algorithm to manage chain state is stable.
        * ... with the caveat that it runs very well in a very harsh
          and unforgiving network partition simulator but has not run
          much yet in the real world.
    * All Machi client/server protocols are based on
      [Protocol Buffers](https://developers.google.com/protocol-buffers/docs/overview).
        * The current specification for Machi's protocols can be found at
          [https://github.com/basho/machi/blob/master/src/machi.proto](https://github.com/basho/machi/blob/master/src/machi.proto).
        * The Machi PB protocol is not yet stable.  Expect change!
        * The Erlang language client implementation of the high-level
          protocol flavor is brittle (e.g., little error handling yet).

If you would like to run the Humming Consensus code (with or without
the network partition simulator) as described in the RICON 2015
presentation, please see the
[Humming Consensus demo doc.](./doc/humming_consensus_demo.md).

If you'd like to work on a protocol such as Thrift, UBF,
msgpack over UDP, or some other protocol, let us know by
[opening an issue to discuss it](./issues/new).

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
