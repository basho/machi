
# Using the network partition simulator and convergence demo test code

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

1. You'll need the `git` source management 
2. You'll need the Erlang/OTP 17 runtime environment.  Please don't
   use earlier or later versions until we have a chance to fix the
   compilation warnings that versions R16B and 18 will trigger.

All of the commands that should be run at your login shell (e.g. Bash,
c-shell) can be cut-and-pasted from this document directly to your
login shell prompt.

## Clone and compile the code

Clone the Machi source repo and compile the source and test code.  Run
the following commands at your login shell:

    cd /tmp
    git clone https://github.com/basho/machi.git
    cd machi
    git checkout master
    make

Then run the unit test suite.  This may take up to two minutes or so
to finish.  Most of the tests will be silent; please be patient until
the tests finish.

    make test

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

