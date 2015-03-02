## Basic glossary/summary/review for the impatient

* **Machi**: An immutable, append-only distributed file storage service.
    * For more detail, see: [https://github.com/basho/internal_wiki/wiki/RFCs#riak-cs-rfcs](https://github.com/basho/internal_wiki/wiki/RFCs#riak-cs-rfcs)
* **Machi cluster**: A small cluster of machines, e.g. 2 or 3, which form a single Machi cluster.
    * All cluster data is replicated via Chain Replication.
    * In nominal/full-repair state, all servers store exactly the same copies of all files.
* **Strong Consistency** and **CP-style operation**: Not here, please look elsewhere.  This is an eventual consistency joint.
* **Sequencer**: Logical component that specifies where any new data will be stored.
    * For each chunk of data written to a Machi server, the sequencer is solely responsible for assigning a file name and byte offset within that file.
    *  [http://martinfowler.com/bliki/TwoHardThings.html](http://martinfowler.com/bliki/TwoHardThings.html)
* **Server**: A mostly-dumb file server, with fewer total functions than an NFS version 2 file server.
    * Compare with  [https://tools.ietf.org/html/rfc1094#section-2.2](https://tools.ietf.org/html/rfc1094#section-2.2)
* **Chunk**: The basic storage unit of Machi: an ordered sequence of bytes.  Each chunk is stored together with its MD5 checksum.
    * Think of a single Erlang binary term...
* **File**: A strictly-ordered collection of chunks.
* **Chain Replication**: A variation of master/slave replication where writes and reads are strictly ordered in a "chain-like" manner
* **Chain Repair**: Identify chunks that are missing between servers in a chain and then fix them.
* **Cluster of clusters**: A desirable rainbow unicorn with purple fur and files automagically distributed across many Machi clusters, scalable to infinite size within a single data center.
* **Projection**: A data structure which specifies file distribution and file migration across a cluster of clusters.
    * Also used by a single Machi cluster to determine the current membership & status of the cluster & its chain.
* **Erasure Coding**: A family of techniques of encoding data with redundant data. The redundant data is smaller than the original data, yet can be used to reconstruct the original data if some original parts are lost/destroyed.

## Present in this POC

* Basic sequencer and file server
    * Sequencer and file server are combined for efficiency
    * Basic ops: append chunk to file (with implicit sequencer assignment), read chunk, list files, fetch chunk checksums (by file)
    * Chain replication ops: write chunk (location already determined by sequencer), delete file (cluster-of-clusters file migration use only), truncate file (cluster-of-clusters + erasure coding use only)

* Basic file clients
    * Write chunks to a single server (via sequencer) and read from a single server.
    * Write and read chunks to multiple servers, using both projection and chain replication
    * All chain replication and projection features are implemented on the client side.

* Basic cluster-of-cluster functions
    * Hibari style consistent hashing for file uploads & downloads
        * There is no ring.
        * Arbitrary weighted distribution of files across multiple clusters
    * Append/write/read ops are automatically routed to the correct chain/cluster.
    * File migration/rebalancing

* Administrative features
    * Verify chunk checksums in a file.
    * Find and fix missing chunks between any two servers in a single cluster.
        * Normal cluster operations are uninterrupted.
    * Create new projection definitions
    * Demonstrate file -> cluster mapping via projection
    * Identify files in current projection that must be moved in the new projection
        * ... and move them to the new projection
        * Normal cluster operations are uninterrupted.

* Erasure coding implementation
    * Inefficient & just barely enough to work
        * No NIF or Erlang port integration
    * Reed-Solomon 10,4 encoding only
        * 10 independent sub-chunks of original data
        * 4 independent sub-chunks of parity data
        * Use any 10 sub-chunks to reconstruct all original and parity data
    * Encode an entire file and write sub-chunks to a secondary projection of 14 servers
    * Download chunks automatically via regular projection (for replicated chunks) or a secondary projection (for EC-coded sub-chunks).

## Missing from this POC

* Chunk immutability is not enforced
* Read-repair ... no good reason, I just ran out of 2014 to implement it
* Per-chunk metadata
* Repair of erasure coded missing blocks: all major pieces are there, but I ran out of 2014 here, also.
* Automation: all chain management functions are manual, no health/status monitoring, etc.

## Simple examples

Run a server:

    ./file0_server.escript file0_server 7071 /tmp/path-to-data-dir
    
Upload chunks of a local file in 1 million byte chunks:

    ./file0_write_client.escript localhost 7071 1000000 someprefix /path/to/some/local/file
    
To fetch the chunks, we need a list of filenames, offsets, and chunk
sizes.  The easiest way to get such a list is to save the output from
`./file0_write_client.escript` when uploading the file in the first
place.  For example, save the following command's output into some
file e.g. `/tmp/input`.  Then:

    ./file0_read_client.escript localhost 7071 /tmp/input

## More details

### Server protocol

All negative server responses start with `ERROR` unless otherwise noted.

* No distributed Erlang message passing ... "just ASCII on a socket".

* `A LenHex Prefix` ... Append a chunk of data to a file with name prefix `Prefix`. File name & offset will be assigned by server-side sequencer.
    * `OK OffsetHex Filename`

* `R OffsetHex LenHex Filename` ... Read a chunk from `OffsetHex` for length `Lenhex` from file `FileName`.
    * `OK [chunk follows]`

* `L` ... List all files stored on the server, one per line
    * `OK [list of files & file sizes, ending with .]`

* `C Filename` ... Fetch chunk offset & checksums
    * `OK [list of chunk offset, lengths, and checksums, ending with .]`

* `QUIT` ... Close connection
    * `OK`

* `W-repl OffsetHex LenHex Filename` ... Write/replicate a chunk of data to a file with an `OffsetHex` that has already been assigned by a sequencer.
    * `OK`

* `TRUNC-hack--- Filename` ... Truncate a file (after erasure encoding is successful).
    * `OK`

### Start a server, upload & download chunks to a single server

Mentioned above.

### Upload chunks to a cluster (members specified manually)

    ./file0_1file_write_redundant.escript 1000000 otherprefix /path/to/local/file localhost 7071 localhost 7072

### List files (and file sizes) on a server

    ./file0_list.escript localhost 7071

### List all chunk sizes & checksums for a single file

    setenv M_FILE otherprefix.KCL2CC9.1
    ./file0_checksum_list.escript localhost 7071 $M_FILE

### Verify checksums in a file

    ./file0_verify_checksums.escript localhost 7071 $M_FILE

### Compare two servers and file all missing files

TODO script is broken, but is just a proof-of-concept for early repair work.

    $ ./file0_compare_filelists.escript localhost 7071 localhost 7072
    {legend, {file, list_of_servers_without_file}}.
    {all, [{"localhost","7071"},{"localhost","7072"}]}.

### Repair server A -> server B, replicating all missing data

    ./file0_repair_server.escript localhost 7071 localhost 7072 verbose check
    ./file0_repair_server.escript localhost 7071 localhost 7072 verbose repair
    ./file0_repair_server.escript localhost 7071 localhost 7072 verbose repair

And then repair in the reverse direction:

    ./file0_repair_server.escript localhost 7072 localhost 7071 verbose check
    ./file0_repair_server.escript localhost 7072 localhost 7071 verbose repair
    ./file0_repair_server.escript localhost 7072 localhost 7071 verbose repair

### Projections: how to create & use them

* Easiest use: all projection data is stored in a single directory; use path to the directory for all PoC scripts
* Files in the projection directory:
    * `chains.map`
        * Define all members of all chains at the current time
    * `*.weight`
        * Define chain capacity weights
    * `*.proj`
        * Define a projection (based on previous projection file + weight file)

#### Sample chains.map file

    %% Chain names (key): please use binary
    %% Chain members (value): two-tuples of binary hostname and integer TCP port
    
    {<<"chain1">>, [{<<"localhost">>, 7071}]}.
    {<<"chain10">>, [{<<"localhost">>, 7071}, {<<"localhost">>, 7072}]}.

#### Sample weight file

    %% Please use binaries for chain names
    
    [
     {<<"chain1">>, 1000}
    ].

#### Sample projection file

    %% Created by:
    %    ./file0_cc_make_projection.escript new examples/weight_map1.weight examples/1.proj
    
    {projection,1,0,[{<<"chain1">>,1.0}],undefined,undefined,undefined,undefined}.

### Create the `examples/1.proj` file

    ./file0_cc_make_projection.escript new examples/weight_map_ec1.weight examples/ec1.proj

### Demo consistent hashing of file prefixes: what chain do we map to?

    ./file0_cc_map_prefix.escript examples/1.proj foo-prefix
    ./file0_cc_map_prefix.escript examples/3.proj foo-prefix
    ./file0_cc_map_prefix.escript examples/56.proj foo-prefix

### Write replica chunks to a chain via projection

    ./file0_cc_1file_write_redundant.escript 4444 some-prefix /path/to/local/file ./examples/1.proj

### Read replica chunks to a chain via projection

    ./file0_cc_read_client.escript examples/1.proj /tmp/input console 

### Change of projection: migrate 1 server's files from old to new projection

    ./file0_cc_migrate_files.escript examples/2.proj localhost 7071 verbose check
    ./file0_cc_migrate_files.escript examples/2.proj localhost 7071 verbose repair delete-source

### Erasure encode a file, then delete the original replicas, then fetch via EC sub-chunks

The current prototype fetches the file directly from the file system, rather than fetching the file over the network via the `RegularProjection` projection definition.

WARNING: For this part of demo to work correctly, `$M_PATH` must be a
file that exists on `localhost 7071` and not on `localhost 7072` or
any other server.

    setenv M_PATH /tmp/SAM1/seq-tests/data1/otherprefix.77Z34TQ.1
    ./file0_cc_ec_encode.escript examples/1.proj $M_PATH /tmp/zooenc examples/ec1.proj verbose check nodelete-tmp

    less $M_PATH
    ./file0_cc_ec_encode.escript examples/1.proj $M_PATH /tmp/zooenc examples/ec1.proj verbose repair progress delete-source
    less $M_PATH
    ./file0_read_client.escript localhost 7071 /tmp/input
    ./file0_read_client.escript localhost 7072 /tmp/input
    ./file0_cc_read_client.escript examples/1.proj /tmp/input console examples/55.proj

