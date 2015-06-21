# Frequently Asked Questions (FAQ)

<!-- Formatting: -->
<!-- All headings omitted from outline are H1 -->
<!-- All other headings must be on a single line -->
<!-- Run: ./priv/make-faq.pl ./FAQ.md > ./tmpfoo; mv ./tmpfoo ./FAQ.md -->

# Outline

<!-- OUTLINE -->

+ [1 Questions about Machi in general](#n1)
    + [1.1 What is Machi?](#n1.1)
    + [1.2 What does Machi's API look like?](#n1.2)
+ [2 Questions about Machi relative to something else](#n2)
    + [2.1 How is Machi better than Hadoop?](#n2.1)
    + [2.2 How does Machi differ from HadoopFS?](#n2.2)
    + [2.3 How does Machi differ from Kafka?](#n2.3)
    + [2.4 How does Machi differ from Bookkeeper?](#n2.4)
    + [2.5 How does Machi differ from CORFU and Tango?](#n2.5)

<!-- ENDOUTLINE -->

<a name="n1">
## 1.  Questions about Machi in general

<a name="n1.1">
### 1.1.  What is Machi?

TODO: expand this topic.

Very briefly, Machi is a very simple append-only file store; it is
"dumber" than many other file stores (i.e., lacking many features
found in other file stores) such as HadoopFS or simple NFS or CIFS file
server.
However, Machi is a distributed file store, which makes it different
(and, in some ways, more complicated) than a simple NFS or CIFS file
server.

As a distributed system, Machi can be configured to operate with
either eventually consistent mode or strongly consistent mode.  (See
the high level design document for definitions and details.)

For a much longer answer, please see the
[Machi high level design doc](./doc/high-level-machi.pdf).

<a name="n1.2">
### 1.2.  What does Machi's API look like?

The Machi API only contains a handful of API operations.  The function
arguments shown below use Erlang-style type annotations.

    append_chunk(Prefix:binary(), Chunk:binary()).
    append_chunk_extra(Prefix:binary(), Chunk:binary(), ExtraSpace:non_neg_integer()).
    read_chunk(File:binary(), Offset:non_neg_integer(), Size:non_neg_integer()).
    
    checksum_list(File:binary()).
    list_files().

Machi allows the client to choose the prefix of the file name to
append data to, but the Machi server will always choose the final file
name and byte offset for each `append_chunk()` operation.  This
restriction on file naming makes it easy to operate in "eventually
consistent" mode: files may be written to any server during network
partitions and can be easily merged together after the partition is
healed.

Internally, there is a more complex protocol used by individual
cluster members to manage file contents and to repair damaged/missing
files.  See Figure 3 in
[Machi high level design doc](./doc/high-level-machi.pdf)
for more details.

<a name="n2">
## 2.  Questions about Machi relative to something else

<a name="better-than-hadoop">
<a name="n2.1">
### 2.1.  How is Machi better than Hadoop?

This question is frequently asked by trolls.  If this is a troll
question, the answer is either, "Nothing is better than Hadoop," or
else "Everything is better than Hadoop."

The real answer is that Machi is not a distributed data processing
framework like Hadoop is.
See [Hadoop's entry in Wikipedia](https://en.wikipedia.org/wiki/Apache_Hadoop)
and focus on the description of Hadoop's MapReduce and YARN; Machi
contains neither.

<a name="n2.2">
### 2.2.  How does Machi differ from HadoopFS?

This is a much better question than the
[How is Machi better than Hadoop?](#better-than-hadoop)
question.

[HadoopFS's entry in Wikipedia](https://en.wikipedia.org/wiki/Apache_Hadoop#HDFS)

One way to look at Machi is to consider Machi as a distributed file
store. HadoopFS is also a distributed file store.  Let's compare and
contrast.

<table>

<tr>
<td> <b>Machi</b>
<td> <b>Hadoop</b>

<tr>
<td> Not POSIX compliant
<td> Not POSIX compliant

<tr>
<td> Immutable file store with append-only semantics (simplifying
things a little bit).
<td> Immutable file store with append-only semantics

<tr>
<td> File data may be read concurrently while file is being actively
appended to.
<td> File must be closed before a client can read it.

<tr>
<td> No concept (yet) of users, directories, or ACLs
<td> Has concepts of users, directories, and ACLs.

<tr>
<td> Machi oes not allow clients to name their own files or to specify data
placement/offset within a file.
<td> While not POSIX compliant, HDFS allows a fairly flexible API for
managing file names and file writing position within a file (during a
file's writable phase). 

<tr>
<td> Does not have any file distribution/partitioning/sharding across
Machi clusters: in a single Machi cluster, all files are replicated by
all servers in the cluster.  The "cluster of clusters" concept is used
to distribute/partition/shard files across multiple Machi clusters.
<td> File distribution/partitioning/sharding is performed
automatically by the HDFS "name node".

<tr>
<td> Machi requires no central "name node" for single cluster use.
Machi requires no central "name node" for "cluster of clusters" use
<td> Requires a single "namenode" server to maintain file system contents
and file content mapping.  (May be deployed with a "secondary
namenode" to reduce unavailability when the primary namenode fails.)

<tr>
<td> Machi uses Chain Replication to manage all file replicas.
<td> The HDFS name node uses an ad hoc mechanism for replicating file
contents.  The HDFS file system metadata (file names, file block(s)
locations, ACLs, etc.) is stored by the name node in the local file
system and is replicated to any secondary namenode using snapshots.

<tr>
<td> Machi replicates files *N* ways where *N* is the length of the
Chain Replication chain. Typically, *N=2*, but this is configurable.
<td> HDFS typical replicates file contents *N=3* ways, but this is
configurable. 

<tr>
<td>
<td>

</table>

<a name="n2.3">
### 2.3.  How does Machi differ from Kafka?

Machi is rather close to Kafka in spirit, though its implementation is
quite different.

<table>

<tr>
<td> <b>Machi</b>
<td> <b>Kafka</b>

<tr>
<td> Append-only, strongly consistent file store only
<td> Append-only, strongly consistent log file store + additional
services: for example, producer topics & sharding, consumer groups &
failover, etc.

<tr>
<td> Not yet code complete nor "battle tested" in large production
environments.
<td> "Battle tested" in large production environments.

</table>

In theory, it should be "quite straightforward" to remove these parts
of Kafka's code base:

* local file system I/O for all topic/partition/log files
* leader/follower file replication, ISR ("In Sync Replica") state
  management, and related log file replication logic

... and replace those parts with Machi client API calls.  Those parts
of Kafka are what Machi has been designed to do from the very
beginning.

See also:
<a href="#corfu-and-tango">How does Machi differ from CORFU and Tango?</a>

<a name="n2.4">
### 2.4.  How does Machi differ from Bookkeeper?

Sorry, we haven't studied Bookkeeper very deeply or used Bookkeeper
for any non-trivial project.

One notable limitation of the Bookkeeper API is that a ledger cannot
be read by other clients until it has been closed.  Any byte in a
Machi file that has been written successfully may
be read immedately by any other Machi client.

The name "Machi" does not have three consecutive pairs of repeating
letters.  The name "Bookkeeper" does.

<a name="corfu-and-tango">
<a name="n2.5">
### 2.5.  How does Machi differ from CORFU and Tango?

Machi's design borrows very heavily from CORFU.  We acknowledge a deep
debt to the original Microsoft Research papers that describe CORFU's
original design and implementation.

See also: the "Recommended reading & related work" and "References"
sections of the
[Machi high level design doc](./doc/high-level-machi.pdf)
for pointers to the MSR papers related to CORFU.

Machi does not implement Tango directly.  (Not yet, at least.)
However, there is a prototype implementation included in the Machi
source tree.  See
[the prototype/tango source code directory](https://github.com/basho/machi/tree/master/prototype/tango)
for details.
