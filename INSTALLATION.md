
# Installation instructions for Machi

Machi is still a young enough project that there is no "installation".
All development is still done using the Erlang/OTP interactive shell
for experimentation, using `make` to compile, and the shell's
`l(ModuleName).` command to reload any recompiled modules.

In the coming months (mid-2015), there are plans to create OS packages
for common operating systems and OS distributions, such as FreeBSD and
Linux.  If building RPM, DEB, PKG, or other OS package managers is
your specialty, we could use your help to speed up the process!  <b>:-)</b>

## Development toolchain dependencies

Machi's dependencies on the developer's toolchain are quite small.

* Erlang/OTP version 17.0 or later, 32-bit or 64-bit
* The `make` utility.
    * The GNU version of make is not required.
    * Machi is bundled with a `rebar` package and should be usable on
      any Erlang/OTP 17.x platform.

Machi does not use any Erlang NIF or port drivers.

## Development OS 

At this time, Machi is 100% Erlang.  Although we have not tested it,
there should be no good reason why Machi cannot run on Erlang/OTP on
Windows platforms.  Machi has been developed on OS X and FreeBSD and
is expected to work on any UNIX-ish platform supported by Erlang/OTP.

## Compiling the Machi source

First, clone the Machi source code, then compile it.  You will
need Erlang/OTP version 17.x to compile.

    cd /some/nice/dev/place
    git clone https://github.com/basho/machi.git
    cd machi
    make
    make test

The unit test suite is based on the EUnit framework (bundled with
Erlang/OTP 17).  The `make test` suite runs on my MacBook in 10
seconds or less.

## Setting up a Machi cluster

As noted above, everything is done manually at the moment.  Here is a
rough sketch of day-to-day development workflow.

### 1. Run the server

    cd /some/nice/dev/place/machi
    make
    erl -pz ebin deps/*/ebin +A 253 +K true

This will start an Erlang shell, plus a few extras.

* Tell the OTP code loader where to find dependent BEAM files.
* Set a large pool (253) of file I/O worker threads
* Use a more efficient kernel polling mechanism for network sockets.
    * If your Erlang/OTP package does not support `+K true`, do not
      worry.  It is an optional flag.

The following commands will start three Machi FLU server processes and
then tell them to form a single chain.  Internally, each FLU will have
Erlang registered processes with the names `a`, `b`, and `c`, and
listen on TCP ports 4444, 4445, and 4446, respectively.  Each will use
a data directory located in the current directory, e.g. `./data.a`.

Cut-and-paste the following commands into the CLI at the prompt:

    application:ensure_all_started(machi).        
    machi_flu_psup:start_flu_package(a, 4444, "./data.a", []).
    machi_flu_psup:start_flu_package(b, 4445, "./data.b", []).
    D = orddict:from_list([{a,{p_srvr,a,machi_flu1_client,"localhost",4444,[]}},{b,{p_srvr,b,machi_flu1_client,"localhost",4445,[]}}]).
    machi_chain_manager1:set_chain_members(a_chmgr, D).
    machi_chain_manager1:set_chain_members(b_chmgr, D).

If you change the TCP ports of any of the processes, you must make the
same change both in the `machi_flu_psup:start_flu_package()` arguments
and also in the `D` dictionary.

The Erlang processes that will be started are arranged in the
following hierarchy.  See the
[machi_flu_psup.erl](http://basho.github.io/machi/edoc/machi_flu_psup.html)
EDoc documentation for a description of each of these processes.

![](https://basho.github.io/machi/images/supervisor-2flus.png)

### 2. Check the status of the server processes.

Each Machi FLU is an independent file server.  All replication between
Machi servers is currently implemented by code on the *client* side.
(This will change a bit later in 2015.)

Use the `read_latest_projection` command on the server CLI, e.g.:

    rr("include/machi_projection.hrl").
    machi_projection_store:read_latest_projection(a_pstore, private).

... to query the projection store of the local FLU named `a`.

If you haven't looked at the server-side description of the various
Machi server-side processes, please take a couple minutes to read
[machi_flu_psup.erl](http://basho.github.io/machi/edoc/machi_flu_psup.html).

### 3. Use the machi_cr_client.erl client

For development work, I run the client & server on the same Erlang
VM.  It's just easier that way ... but the Machi client & server use
TCP to communicate with each other.

If you are using a separate machine for the client, then compile the
Machi source on the client machine.  Then run:

    cd /some/nice/dev/place/machi
    make
    erl -pz ebin deps/*/ebin

(You can add `+K true` if you wish ... but for light development work,
it doesn't make a big difference.)

At the CLI, define the dictionary that describes the host & TCP port
location for each of the Machi servers.  (If you changed the host
and/or TCP port values when starting the servers, then place make the
same changes here.

    D = orddict:from_list([{a,{p_srvr,a,machi_flu1_client,"localhost",4444,[]}},{b,{p_srvr,b,machi_flu1_client,"localhost",4445,[]}}]).

Then start a `machi_cr_client` client process.

    {ok, C1} = machi_cr_client:start_link([P || {_,P} <- orddict:to_list(D)]).

Please keep in mind that this process is **linked** to your CLI
process.  If you run a CLI command the throws an exception/exits, then
this `C1` process will also die!  You can start a new one, using a
different name, e.g. `C2`.  Or you can start a new one by first
"forgetting" the CLI's binding for `C1`.

    f(C1).
    {ok, C1} = machi_cr_client:start_link([P || {_,P} <- orddict:to_list(D)]).

Now, append a small chunk of data to a file with the prefix
`<<"pre">>`.

    12> {ok, C1} = machi_cr_client:start_link([P || {_,P} <- orddict:to_list(D)]).
    {ok,<0.112.0>}
    
    13> machi_cr_client:append_chunk(C1, <<"pre">>, <<"Hello, world">>).
    {ok,{1024,12,<<"pre.G6C116EA.3">>}}


This chunk was written successfully to a file called
`<<"pre.5BBL16EA.1">>` at byte offset 1024.  Let's fetch it now.  And
let's see what happens in a couple of error conditions: fetching
bytes that "straddle" the end of file, bytes that are after the known
end of file, and bytes from a file that has never been written.

    26> machi_cr_client:read_chunk(C1, <<"pre.G6C116EA.3">>, 1024, 12).
    {ok,<<"Hello, world">>}
    
    27> machi_cr_client:read_chunk(C1, <<"pre.G6C116EA.3">>, 1024, 777).
    {error,partial_read}
    
    28> machi_cr_client:read_chunk(C1, <<"pre.G6C116EA.3">>, 889323, 12).
    {error,not_written}
    
    29> machi_cr_client:read_chunk(C1, <<"no-such-file">>, 1024, 12).
    {error,not_written}

### 4. Use the `machi_proxy_flu1_client.erl` client

The `machi_proxy_flu1_client` module implements a simpler client that
only uses a single Machi FLU file server.  This client is **not**
aware of chain replication in any way.

Let's use this client to verify that the `<<"Hello, world!">> data
that we wrote in step #3 was truly written to both FLU servers by the
`machi_cr_client` library.  We start proxy processes for each of the
FLUs, then we'll query each ... but first we also need to ask (at
least one of) the servers for the current Machi cluster's Epoch ID.

    {ok, Pa} = machi_proxy_flu1_client:start_link(orddict:fetch(a, D)).
    {ok, Pb} = machi_proxy_flu1_client:start_link(orddict:fetch(b, D)).
    {ok, EpochID0} = machi_proxy_flu1_client:get_epoch_id(Pa).
    machi_proxy_flu1_client:read_chunk(Pa, EpochID0, <<"pre.G6C116EA.3">>, 1024, 12).
    machi_proxy_flu1_client:read_chunk(Pb, EpochID0, <<"pre.G6C116EA.3">>, 1024, 12).

### 5. Checking how Chain Replication "read repair" works

Now, let's cause some trouble: we will write some data only to the
head of the chain.  By default, all read operations go to the tail of
the chain.  But, if a value is not written at the tail, then "read
repair" ought to verify:

* Perhaps the value truly is not written at any server in the chain.
* Perhaps the value was partially written, i.e. by a buggy or
  crashed-in-the-middle-of-the-writing-procedure client.

So, first, let's double-check that the chain is in the order that we
expect it to be.

    rr("include/machi_projection.hrl").   % In case you didn't do this earlier.
    machi_proxy_flu1_client:read_latest_projection(Pa, private).

The part of the `#projection_v1` record that we're interested in is
the `upi`.  This is the list of servers that preserve the Update
Propagation Invariant property of the Chain Replication algorithm.
The output should look something like:

    {ok,#projection_v1{
            epoch_number = 1119,
            [...]
            author_server = b,
            all_members = [a,b],
            creation_time = {1432,189599,85392},
            mode = ap_mode,
            upi = [a,b],
            repairing = [],down = [],
            [...]
    }

So, we see `upi=[a,b]`, which means that FLU `a` is the head of the
chain and that `b` is the tail.

Let's append to `a` using the `machi_proxy_flu1_client` to the head
and then read from both the head and tail.  (If your chain order is
different, then please exchange `Pa` and `Pb` in all of the commands
below.)

    16> {ok, {Off1,Size1,File1}} = machi_proxy_flu1_client:append_chunk(Pa, EpochID0, <<"foo">>, <<"Hi, again">>).
    {ok,{1024,9,<<"foo.K63D16M4.1">>}}
    
    17> machi_proxy_flu1_client:read_chunk(Pa, EpochID0, File1, Off1, Size1).       {ok,<<"Hi, again">>}          
    
    18> machi_proxy_flu1_client:read_chunk(Pb, EpochID0, File1, Off1, Size1).
    {error,not_written}

That is correct!  Now, let's read the same file & offset using the
client that understands chain replication.  Then we will try reading
directly from FLU `b` again ... we should see something different.

    19> {ok, C2} = machi_cr_client:start_link([P || {_,P} <- orddict:to_list(D)]).
    {ok,<0.113.0>}                
    
    20> machi_cr_client:read_chunk(C2, File1, Off1, Size1).
    {ok,<<"Hi, again">>}
    
    21> machi_proxy_flu1_client:read_chunk(Pb, EpochID0, File1, Off1, Size1).
    {ok,<<"Hi, again">>}

That is correct! The command at prompt #20 automatically performed
"read repair" on FLU `b`.

### 6. Exploring the rest of the client APIs

Please see the EDoc documentation for the client APIs.  Feel free to
explore!

* [Erlang type definitions for the client APIs](http://basho.github.io/machi/edoc/machi_flu1_client.html)
* [EDoc for machi_cr_client.erl](http://basho.github.io/machi/edoc/machi_cr_client.html)
* [EDoc for machi_proxy_flu1_client.erl](http://basho.github.io/machi/edoc/machi_proxy_flu1_client.html)
