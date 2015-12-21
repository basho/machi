## Machi Documentation Overview

For a Web-browsable version of a snapshot of the source doc "EDoc"
Erlang documentation, please use this link:
[Machi EDoc snapshot](https://basho.github.io/machi/edoc/).

## Documents in this directory

### high-level-machi.pdf

[high-level-machi.pdf](high-level-machi.pdf)
is an overview of the high level design for
Machi.  Its abstract:

> Our goal is a robust & reliable, distributed, highly available large
> file store based upon write-once registers, append-only files, Chain
> Replication, and client-server style architecture.  All members of
> the cluster store all of the files.  Distributed load
> balancing/sharding of files is outside of the scope of this system.
> However, it is a high priority that this system be able to integrate
> easily into systems that do provide distributed load balancing,
> e.g., Riak Core.  Although strong consistency is a major feature of
> Chain Replication, this document will focus mainly on eventual
> consistency features --- strong consistency design will be discussed
> in a separate document.

### high-level-chain-mgr.pdf

[high-level-chain-mgr.pdf](high-level-chain-mgr.pdf)
is an overview of the techniques used by
Machi to manage Chain Replication metadata state.  It also provides an
introduction to the Humming Consensus algorithm.  Its abstract:

> Machi is an immutable file store, now in active development by Basho
> Japan KK.  Machi uses Chain Replication to maintain strong consistency
> of file updates to all replica servers in a Machi cluster.  Chain
> Replication is a variation of primary/backup replication where the
> order of updates between the primary server and each of the backup
> servers is strictly ordered into a single "chain".  Management of
> Chain Replication's metadata, e.g., "What is the current order of
> servers in the chain?", remains an open research problem.  The
> current state of the art for Chain Replication metadata management
> relies on an external oracle (e.g., ZooKeeper) or the Elastic
> Replication algorithm.
> 
> This document describes the Machi chain manager, the component
> responsible for managing Chain Replication metadata state.  The chain
> manager uses a new technique, based on a variation of CORFU, called
> "humming consensus".
> Humming consensus does not require active participation by all or even
> a majority of participants to make decisions.  Machi's chain manager
> bases its logic on humming consensus to make decisions about how to
> react to changes in its environment, e.g. server crashes, network
> partitions, and changes by Machi cluster admnistrators.  Once a
> decision is made during a virtual time epoch, humming consensus will
> eventually discover if other participants have made a different
> decision during that epoch.  When a differing decision is discovered,
> new time epochs are proposed in which a new consensus is reached and
> disseminated to all available participants.

### chain-self-management-sketch.org

[chain-self-management-sketch.org](chain-self-management-sketch.org)
is a mostly-deprecated draft of
an introduction to the
self-management algorithm proposed for Machi.  Most material has been
moved to the [high-level-chain-mgr.pdf](high-level-chain-mgr.pdf) document.

### cluster (directory)

This directory contains the sketch of the cluster design
strawman for partitioning/distributing/sharding files across a large
number of independent Machi chains.

