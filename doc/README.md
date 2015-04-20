## Machi Documentation Overview

For a Web-browsable version of a snapshot of the source doc "EDoc"
Erlang documentation, please use this link:
[Machi EDoc snapshot](https://basho.github.io/machi/edoc/).

## Documents in this directory

### chain-self-management-sketch.org

__chain-self-management-sketch.org__ is an introduction to the
self-management algorithm proposed for Machi.  This algorithm is
(hoped to be) sufficient for managing the Chain Replication state of a
Machi cluster.

### high-level-machi.pdf

__high-level-machi.pdf__ is an overview of the high level design for
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

