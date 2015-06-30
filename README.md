# Machi

Our goal is a robust & reliable, distributed, highly available(*),
large file store based upon write-once registers, append-only files,
Chain Replication, and client-server style architecture.  All members
of the cluster store all of the files.  Distributed load
balancing/sharding of files is __outside__ of the scope of this
system.  However, it is a high priority that this system be able to
integrate easily into systems that do provide distributed load
balancing, e.g., Riak Core.  Although strong consistency is a major
feature of Chain Replication, first use cases will focus mainly on
eventual consistency features --- strong consistency design will be
discussed in a separate design document (read more below).

The ability for Machi to maintain strong consistency will make it
attractive as a toolkit for building things like CORFU and Tango as
well as better-known open source software such as Kafka's file
replication.  (See the bibliography of the [Machi high level design
doc](./doc/high-level-machi.pdf) for further references.)

(*) Capable of operating in "AP mode" or "CP mode" relative to the
  CAP Theorem.

## Status: mid-June 2015: work is underway

The two major design documents for Machi are now ready or nearly ready
for internal Basho and external party review.  Please see the
[doc](./doc) directory's [README](./doc) for details

* Machi high level design
* Machi chain self-management design

The work of implementing first draft of Machi is now underway.  The
code from the [prototype/demo-day-hack](prototype/demo-day-hack/) directory is
being used as the initial scaffolding.

* The chain manager is ready for "AP mode" use in eventual
  consistency use cases.

* All Machi client/server protocols are based on
  [Protocol Buffers](https://developers.google.com/protocol-buffers/docs/overview).
    * The current specification for Machi's protocols can be found at
      [https://github.com/basho/machi/blob/master/src/machi.proto](https://github.com/basho/machi/blob/master/src/machi.proto).
    * The Machi PB protocol is not yet stable.  Expect change!
    * The Erlang language client implementation of the high-level
      protocol flavor is very brittle (e.g., very little error
      handling yet).
    * The Erlang language client implementation of the low-level
      protocol flavor are still a work-in-progress ... but they are
      more robust than the high-level library's implementation.

If you'd like to work on a protocol such as Thrift, UBF,
msgpack over UDP, or some other protocol, let us know by
[opening an issue](./issues/new) to discuss it.

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

