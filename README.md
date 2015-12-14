# Machi: a robust & reliable, distributed, highly available, large file store

  [Travis-CI](http://travis-ci.org/basho/machi) :: ![Travis-CI](https://secure.travis-ci.org/basho/machi.png)

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
really want both #3 and #1.  Unfortunately, systems that meet criteria
#1 are *very*
rare.  Why?  This is 2015.  We have decades of research that shows
that computer hardware can (and
indeed does) corrupt data at nearly every level of the modern
client/server application stack.  Systems with end-to-end data
corruption detection should be ubiquitous today.  Alas, they are not.
Machi is an effort to change the deplorable state of the world, one
Erlang function at a time.

## Status: mid-December 2015: work is underway

TODO: status update here.

* The chain manager is ready for both eventual consistency use ("available
  mode") and strong consistency use ("consistent mode").  Both modes use a new
  consensus technique, Humming Consensus.
    * Scott will be
      [speaking about Humming Consensus](http://ricon.io/agenda/#managing-chain-replication-metadata-with-humming-consensus)
      at the [Ricon 2015 conference] (http://ricon.io) in San Francisco,
      CA, USA on Thursday, November 5th, 2015.
    * If you would like to run the network partition simulator
      mentioned in that Ricon presentation, please see the
      [partition simulator convergence test doc.](./doc/machi_chain_manager1_converge_demo.md)
    * Implementation of the file repair process for strong consistency
      is still in progress.

* All Machi client/server protocols are based on
  [Protocol Buffers](https://developers.google.com/protocol-buffers/docs/overview).
    * The current specification for Machi's protocols can be found at
      [https://github.com/basho/machi/blob/master/src/machi.proto](https://github.com/basho/machi/blob/master/src/machi.proto).
    * The Machi PB protocol is not yet stable.  Expect change!
    * The Erlang language client implementation of the high-level
      protocol flavor is brittle (e.g., little error handling yet).

If you'd like to work on a protocol such as Thrift, UBF,
msgpack over UDP, or some other protocol, let us know by
[opening an issue to discuss it](./issues/new).

## Where to learn more about Machi

The two major design documents for Machi are now mostly stable.
Please see the [doc](./doc) directory's [README](./doc) for details.

Scott recently (November 2015) gave a presentation at the
[RICON 2015 conference](http://ricon.io) about one of the techniques
used by Machi; "Managing Chain Replication Metadata with
Humming Consensus" is available online now.
* [slides (PDF format)](http://ricon.io/speakers/slides/Scott_Fritchie_Ricon_2015.pdf)
* [video](https://www.youtube.com/watch?v=yR5kHL1bu1Q)

## Contributing to Machi: source code, documentation, etc.

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

## A brief survey of this directories in this repository

* A list of Frequently Asked Questions, a.k.a.
  [the Machi FAQ](./FAQ.md).

* The [doc](./doc/) directory: home for major documents about Machi:
  high level design documents as well as exploration of features still
  under design & review within Basho.

* The `ebin` directory: used for compiled application code

* The `include`, `src`, and `test` directories: contain the header
  files, source files, and test code for Machi, respectively.

* The [prototype](./prototype/) directory: contains proof of concept
  code, scaffolding libraries, and other exploratory code.  Curious
  readers should see the [prototype/README.md](./prototype/README.md)
  file for more explanation of the small sub-projects found here.

## Development environment requirements

All development to date has been done with Erlang/OTP version 17 on OS
X.  The only known limitations for using R16 are minor type
specification difference between R16 and 17, but we strongly suggest
continuing development using version 17.

We also assume that you have the standard UNIX/Linux developers
tool chain for C and C++ applications.  Specifically, we assume `make`
is available.  The utility used to compile the Machi source code,
`rebar`, is pre-compiled and included in the repo.

There are no known OS limits at this time: any platform that supports
Erlang/OTP should be sufficient for Machi.  This may change over time
(e.g., adding NIFs which can make full portability to Windows OTP
environments difficult), but it hasn't happened yet.

## Contributions 

Basho encourages contributions to Riak from the community. Here’s how
to get started.

* Fork the appropriate sub-projects that are affected by your change. 
* Create a topic branch for your change and checkout that branch.
     git checkout -b some-topic-branch
* Make your changes and run the test suite if one is provided. (see below)
* Commit your changes and push them to your fork.
* Open pull-requests for the appropriate projects.
* Contributors will review your pull request, suggest changes, and merge it when it’s ready and/or offer feedback.
* To report a bug or issue, please open a new issue against this repository.

-The Machi team at Basho, 
[Scott Lystig Fritchie](mailto:scott@basho.com), technical lead, and
[Matt Brender](mailto:mbrender@basho.com), your developer advocate.

