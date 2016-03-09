
# Table of contents

* [Hand-on experiments with Machi and Humming Consensus](#hands-on)
* [Using the network partition simulator and convergence demo test code](#partition-simulator)

<a name="hands-on">
# Hand-on experiments with Machi and Humming Consensus

## Prerequisites

Please refer to the
[Machi development environment prerequisites doc](./dev-prerequisites.md)
for Machi developer environment prerequisites.

If you do not have an Erlang/OTP runtime system available, but you do
have [the Vagrant virtual machine](https://www.vagrantup.com/) manager
available, then please refer to the instructions in the prerequisites
doc for using Vagrant.

<a name="clone-compile">
## Clone and compile the code

Please see the
[Machi 'clone and compile' doc](./dev-clone-compile.md)
for the short list of steps required to fetch the Machi source code
from GitHub and to compile &amp; test Machi.

## Running three Machi instances on a single machine

All of the commands that should be run at your login shell (e.g. Bash,
c-shell) can be cut-and-pasted from this document directly to your
login shell prompt.

Run the following command:

    make stagedevrel

This will create a directory structure like this:
          
          |-dev1-|... stand-alone Machi app + subdirectories
    |-dev-|-dev2-|... stand-alone Machi app + directories
          |-dev3-|... stand-alone Machi app + directories
    
Each of the `dev/dev1`, `dev/dev2`, and `dev/dev3` are stand-alone
application instances of Machi and can be run independently of each
other on the same machine.  This demo will use all three.

The lifecycle management utilities for Machi are a bit immature,
currently.  They assume that each Machi server runs on a host with a
unique hostname -- there is no flexibility built-in yet to easily run
multiple Machi instances on the same machine.  To continue with the
demo, we need to use `sudo` or `su` to obtain superuser privileges to
edit the `/etc/hosts` file.

Please add the following line to `/etc/hosts`, using this command:

    sudo sh -c 'echo "127.0.0.1 machi1 machi2 machi3" >> /etc/hosts'

Next, we will use a shell script to finish setting up our cluster.  It
will do the following for us:

* Verify that the new line that was added to `/etc/hosts` is correct.
* Modify the `etc/app.config` files to configure the Humming Consensus
  chain manager's actions logged to the `log/console.log` file.
* Start the three application instances.
* Verify that the three instances are running correctly.
* Configure a single chain, with one FLU server per application
  instance.

Please run this script using this command:

    ./priv/humming-consensus-demo.setup.sh

If the output looks like this (and exits with status zero), then the
script was successful.

    Step: Verify that the required entries in /etc/hosts are present
    Step: add a verbose logging option to app.config
    Step: start three three Machi application instances
    pong
    pong
    pong
    Step: configure one chain to start a Humming Consensus group with three members
    Result: ok
    Result: ok
    Result: ok

We have now created a single replica chain, called `c1`, that has
three file servers participating in the chain.  Thanks to the
hostnames that we added to `/etc/hosts`, all are using the localhost
network interface.

    | App instance | Pseudo   | FLU name | TCP port |
    | directory    | Hostname |          |   number |
    |--------------+----------+----------+----------|
    | dev1         | machi1   | flu1     |    20401 |
    | dev2         | machi2   | flu2     |    20402 |
    | dev3         | machi3   | flu3     |    20403 |

The log files for each application instance can be found in the
`./dev/devN/log/console.log` file, where the `N` is the instance
number: 1, 2, or 3.

## Understanding the chain manager's log file output

After running the `./priv/humming-consensus-demo.setup.sh` script,
let's look at the last few lines of the `./dev/dev1/log/console.log`
log file for Erlang VM process #1.

    2016-03-09 10:16:35.676 [info] <0.105.0>@machi_lifecycle_mgr:process_pending_flu:422 Started FLU f1 with supervisor pid <0.128.0>
    2016-03-09 10:16:35.676 [info] <0.105.0>@machi_lifecycle_mgr:move_to_flu_config:540 Creating FLU config file f1
    2016-03-09 10:16:35.790 [info] <0.105.0>@machi_lifecycle_mgr:bootstrap_chain2:312 Configured chain c1 via FLU f1 to mode=ap_mode all=[f1,f2,f3] witnesses=[]
    2016-03-09 10:16:35.790 [info] <0.105.0>@machi_lifecycle_mgr:move_to_chain_config:546 Creating chain config file c1
    2016-03-09 10:16:44.139 [info] <0.132.0> CONFIRM epoch 1141 <<155,42,7,221>> upi [] rep [] auth f1 by f1
    2016-03-09 10:16:44.271 [info] <0.132.0> CONFIRM epoch 1148 <<57,213,154,16>> upi [f1] rep [] auth f1 by f1
    2016-03-09 10:16:44.864 [info] <0.132.0> CONFIRM epoch 1151 <<239,29,39,70>> upi [f1] rep [f3] auth f1 by f1
    2016-03-09 10:16:45.235 [info] <0.132.0> CONFIRM epoch 1152 <<173,17,66,225>> upi [f2] rep [f1,f3] auth f2 by f1
    2016-03-09 10:16:47.343 [info] <0.132.0> CONFIRM epoch 1154 <<154,231,224,149>> upi [f2,f1,f3] rep [] auth f2 by f1

Let's pick apart some of these lines.  We have started all three
servers at about the same time.  We see some race conditions happen,
and some jostling and readjustment happens pretty quickly in the first
few seconds.

* `Started FLU f1 with supervisor pid <0.128.0>`
  * This VM, #1,
  started a FLU (Machi data server) with the name `f1`.  In the Erlang
  process supervisor hierarchy, the process ID of the top supervisor
  is `<0.128.0>`.
* `Configured chain c1 via FLU f1 to mode=ap_mode all=[f1,f2,f3] witnesses=[]`
  * A bootstrap configuration for a chain named `c1` has been created.
  * The FLUs/data servers that are eligible for participation in the
    chain have names `f1`, `f2`, and `f3`.
  * The chain will operate in eventual consistency mode (`ap_mode`)
  * The witness server list is empty.  Witness servers are never used
    in eventual consistency mode.
* `CONFIRM epoch 1141 <<155,42,7,221>> upi [] rep [] auth f1 by f1`
  * All participants in epoch 1141 are unanimous in adopting epoch
    1141's projection.  All active membership lists are empty, so
    there is no functional chain replication yet, at least as far as
    server `f1` knows
  * The epoch's abbreviated checksum is `<<155,42,7,221>>`.
  * The UPI list, i.e. the replicas whose data is 100% in sync is
    `[]`, the empty list.  (UPI = Update Propagation Invariant)
  * The list of servers that are under data repair (`rep`) is also
    empty, `[]`.
  * This projection was authored by server `f1`.
  * The log message was generated by server `f1`.
* `CONFIRM epoch 1148 <<57,213,154,16>> upi [f1] rep [] auth f1 by f1`
  * Now the server `f1` has created a chain of length 1, `[f1]`.
  * Chain repair/file re-sync is not required when the UPI server list
    changes from length 0 -> 1.
* `CONFIRM epoch 1151 <<239,29,39,70>> upi [f1] rep [f3] auth f1 by f1`
  * Server `f1` has noticed that server `f3` is alive.  Apparently it
    has not yet noticed that server `f2` is also running.
  * Server `f3` is in the repair list.
* `CONFIRM epoch 1152 <<173,17,66,225>> upi [f2] rep [f1,f3] auth f2 by f1`
  * Server `f2` is apparently now aware that all three servers are running.
  * The previous configuration used by `f2` was `upi [f2]`, i.e., `f2`
    was running in a chain of one.  `f2` noticed that `f1` and `f3`
    were now available and has started adding them to the chain.
  * All new servers are always added to the tail of the chain in the
    repair list.
  * In eventual consistency mode, a UPI change like this is OK.
    * When performing a read, a client must read from both tail of the
      UPI list and also from all repairing servers.
    * When performing a write, the client writes to both the UPI
      server list and also the repairing list, in that order.
      * I.e., the client concatenates both lists,
      `UPI ++ Repairing`, for its chain configuration for the write.
  * Server `f2` will trigger file repair/re-sync shortly.
    * The waiting time for starting repair has been configured to be
      extremely short, 1 second.  The default waiting time is 10
      seconds, in case Humming Consensus remains unstable.
* `CONFIRM epoch 1154 <<154,231,224,149>> upi [f2,f1,f3] rep [] auth f2 by f1`
  * File repair/re-sync has finished.  All file data on all servers
    are now in sync.
  * The UPI/in-sync part of the chain is now `[f2,f1,f3]`, and there
    are no servers under repair.

## Let's create some failures

Here are some suggestions for creating failures.

* Use the `./dev/devN/bin/machi stop` and `./dev/devN/bin/machi start`
  commands to stop & start VM #`N`.
* Stop a VM abnormally by using `kill`.  The OS process name to look
  for is `beam.smp`.
* Suspend and resume a VM, using the `SIGSTOP` and `SIGCONT` signals.
  * E.g. `kill -STOP 9823` and `kill -CONT 9823`

The network partition simulator is not (yet) available when running
Machi in this mode.  Please see the next section for instructions on
how to use partition simulator.


<a name="partition-simulator">
# Using the network partition simulator and convergence demo test code

This is the demo code mentioned in the presentation that Scott Lystig
Fritchie gave at the
[RICON 2015 conference](http://ricon.io).
* [slides (PDF format)](http://ricon.io/speakers/slides/Scott_Fritchie_Ricon_2015.pdf)
* [video](https://www.youtube.com/watch?v=yR5kHL1bu1Q)

## A complete example of all input and output

If you don't have an Erlang/OTP 17 runtime environment available,
please see this file for full input and output of a strong consistency
length=3 chain test:
https://gist.github.com/slfritchie/8352efc88cc18e62c72c
This file contains all commands input and all simulator output from a
sample run of the simulator.

To help interpret the output of the test, please skip ahead to the
"The test output is very verbose" section.

## Prerequisites

If you don't have `git` and/or the Erlang 17 runtime system available
on your OS X, FreeBSD, Linux, or Solaris machine, please take a look
at the [Prerequisites section](#prerequisites) first.  When you have
installed the prerequisite software, please return back here.

## Clone and compile the code

Please briefly visit the [Clone and compile the code](#clone-compile)
section.  When finished, please return back here.

## Run an interactive Erlang CLI shell

Run the following command at your login shell:

    erl -pz .eunit ebin deps/*/ebin

If you are using Erlang/OTP version 17, you should see some CLI output
that looks like this:

    Erlang/OTP 17 [erts-6.4] [source] [64-bit] [smp:8:8] [async-threads:10] [hipe] [kernel-poll:false] [dtrace]
    
    Eshell V6.4  (abort with ^G)
    1>

## The test output is very verbose ... what are the important parts?

The output of the Erlang command
`machi_chain_manager1_converge_demo:help()` will display the following
guide to the output of the tests.

    A visualization of the convergence behavior of the chain self-management
    algorithm for Machi.
    
      1. Set up some server and chain manager pairs.
      2. Create a number of different network partition scenarios, where
         (simulated) partitions may be symmetric or asymmetric.  Then stop changing
         the partitions and keep the simulated network stable (and perhaps broken).
      3. Run a number of iterations of the algorithm in parallel by poking each
         of the manager processes on a random'ish basis.
      4. Afterward, fetch the chain transition changes made by each FLU and
         verify that no transition was unsafe.
    
    During the iteration periods, the following is a cheatsheet for the output.
    See the internal source for interpreting the rest of the output.
    
        'SET partitions = '
    
            A pair-wise list of actors which cannot send messages.  The
            list is uni-directional.  If there are three servers (a,b,c),
            and if the partitions list is '[{a,b},{b,c}]' then all
            messages from a->b and b->c will be dropped, but any other
            sender->recipient messages will be delivered successfully.
    
        'x uses:'
    
            The FLU x has made an internal state transition and is using
            this epoch's projection as operating chain configuration.  The
            rest of the line is a summary of the projection.
    
        'CONFIRM epoch {N}'
    
            This message confirms that all of the servers listed in the
            UPI and repairing lists of the projection at epoch {N} have
            agreed to use this projection because they all have written
            this projection to their respective private projection stores.
            The chain is now usable by/available to all clients.
    
        'Sweet, private projections are stable'
    
            This report announces that this iteration of the test cycle
            has passed successfully.  The report that follows briefly
            summarizes the latest private projection used by each
            participating server.  For example, when in strong consistency
            mode with 'a' as a witness and 'b' and 'c' as real servers:
    
            %% Legend:
            %% server name, epoch ID, UPI list, repairing list, down list, ...
            %%                         ... witness list, 'false' (a constant value)
    
            [{a,{{1116,<<23,143,246,55>>},[a,b],[],[c],[a],false}},
             {b,{{1116,<<23,143,246,55>>},[a,b],[],[c],[a],false}}]
    
            Both servers 'a' and 'b' agree on epoch 1116 with epoch ID
            {1116,<<23,143,246,55>>} where UPI=[a,b], repairing=[],
            down=[c], and witnesses=[a].
    
            Server 'c' is not shown because 'c' has wedged itself OOS (out
            of service) by configuring a chain length of zero.
    
            If no servers are listed in the report (i.e. only '[]' is
            displayed), then all servers have wedged themselves OOS, and
            the chain is unavailable.
    
        'DoIt,' 
    
            This marks a group of tick events which trigger the manager
            processes to evaluate their environment and perhaps make a
            state transition.
    
    A long chain of 'DoIt,DoIt,DoIt,' means that the chain state has
    (probably) settled to a stable configuration, which is the goal of the
    algorithm.
    
    Press control-c to interrupt the test....".

## Run a test in eventual consistency mode

Run the following command at the Erlang CLI prompt:

    machi_chain_manager1_converge_demo:t(3, [{private_write_verbose,true}]).

The first argument, `3`, is the number of servers to participate in
the chain.  Please note:

* Chain lengths as short as 1 or 2 are valid, but the results are a
  bit boring.
* Chain lengths as long as 7 or 9 can be used, but they may
  suffer from longer periods of churn/instability before all chain
  managers reach agreement via humming consensus.  (It is future work
  to shorten the worst of the unstable churn latencies.)
* In eventual consistency mode, chain lengths may be even numbers,
  e.g. 2, 4, or 6.
* The simulator will choose partition events from the permutations of
  all 1, 2, and 3 node partition pairs.  The total runtime will
  increase *dramatically* with chain length.
    * Chain length 2: about 3 partition cases
    * Chain length 3: about 35 partition cases
    * Chain length 4: about 230 partition cases
    * Chain length 5: about 1100 partition cases

## Run a test in strong consistency mode (with witnesses):

*NOTE:* Due to a bug in the test code, please do not try to run the
 convergence test in strong consistency mode and also without the
 correct minority number of witness servers!  If in doubt, please run
 the commands shown below exactly.

Run the following command at the Erlang CLI prompt:

    machi_chain_manager1_converge_demo:t(3, [{private_write_verbose,true}, {consistency_mode, cp_mode}, {witnesses, [a]}]).

The first argument, `3`, is the number of servers to participate in
the chain.  Chain lengths as long as 7 or 9 can be used, but they may
suffer from longer periods of churn/instability before all chain
managers reach agreement via humming consensus.

Due to the bug mentioned above, please use the following
commands when running with chain lengths of 5 or 7, respectively.

    machi_chain_manager1_converge_demo:t(5, [{private_write_verbose,true}, {consistency_mode, cp_mode}, {witnesses, [a,b]}]).
    machi_chain_manager1_converge_demo:t(7, [{private_write_verbose,true}, {consistency_mode, cp_mode}, {witnesses, [a,b,c]}]).

