
# Using basho_bench to twiddle with Machi

"Twiddle"?  Really, is that a word?  (Yes, it is a real English word.)

## Benchmarking Machi's performance ... no, don't do it.

Machi isn't ready for benchmark testing.  Its public-facing API isn't
finished yet.  Its internal APIs aren't quite finished yet either.  So
any results of "benchmarking" effort is something that has even less
value **N** months from now than the usual benchmarking effort.

However, there are uses for a benchmark tool.  For example, one of my
favorites is to put **stress** on a system.  I don't care about
average or 99th-percentile latencies, but I **might** care very much
about behavior.

* What happens if a Machi system is under moderate load, and then I
  stop one of the servers?  What happens?
    * How quickly do the chain managers react?
    * How quickly do the client libraries within Machi react?
    * How quickly do the external client API libraries react?

* What happens if a Machi system is under heavy load, for example,
  100% CPU load.  Not all 100% might be the Machi services.  Some CPU
  consumption might be from the load generator, like `basho_bench`
  itself that is running on the same machine as a Machi server.  Or
  perhaps it's a tiny C program that I wrote:

    main()
    { while (1) { ; } }

## An example of how adding moderate stress can find weird bugs

The driver/plug-in module for `basho_bench` is only a few hours old.
(I'm writing on Wednesday, 2015-05-20.)  But just now, I configured my
basho_bench config file to try to contact a Machi cluster of three
nodes ... but really, only one was running.  The client library,
`machi_cr_client.erl`, has **an extremely simple** method for dealing
with failed servers.  I know it's simple and dumb, but that's OK in
many cases.

However, `basho_bench` and the `machi_cr_client.erl` were acting very,
very badly.  I couldn't figure it out until I took a peek at my OS's
`dmesg` output, namely: `dmesg | tail`.  It said things like this:

    Limiting closed port RST response from 690 to 50 packets per second
    Limiting closed port RST response from 367 to 50 packets per second
    Limiting closed port RST response from 101 to 50 packets per second
    Limiting closed port RST response from 682 to 50 packets per second
    Limiting closed port RST response from 467 to 50 packets per second

Well, isn't that interesting?

This system was running on a single OS X machine: my MacBook Pro
laptop, running OS X 10.10 (Yosemite).  I have seen that error
before.  And I know how to fix it.

* **Option 1**: Change the client library config to ignore the Machi
    servers that I know will always be down during my experiment.
* ** Option 2**: Use the following to change my OS's TCP stack RST
    behavior.  (If a TCP port is not being listened to, the OS will
    send a RST packet to signal "connection refused".)

On OS X, the limit for RST packets is 50/second.  The
`machi_cr_client.erl` client can generate far more than 50/second, as
the `Limiting closed port RST response...` messages above show.  So, I
used some brute-force to change the environment:

    sudo sysctl -w net.inet.icmp.icmplim=20000

... and the problem disappeared.

## Starting with basho_bench: a step-by-step tutorial

First, clone the `basho_bench` source code, then compile it.  You will
need Erlang/OTP version R16B or later to compile.  I recommend using
Erlang/OTP 17.x, because I've been doing my Machi development using
17.x.

    cd /some/nice/dev/place
    git clone https://github.com/basho/basho_bench.git
    cd basho_bench
    make

In order to create graphs of `basho_bench` output, you'll need
installed one of the following:

* R (the statistics package)
* gnuplot

If you don't have either available on the machine(s) you're testing,
but you do have R (or gnuplot) on some other machine **Y**, then you can
copy the output files to machine **Y** and generate the graphs there.

## Compiling the Machi source

First, clone the `basho_bench` source code, then compile it.  You will
need Erlang/OTP version 17.x to compile.

    cd /some/nice/dev/place
    git clone https://github.com/basho/machi.git
    cd machi
    make

## Creating a basho_bench test configuration file.

There are a couple of example `basho_bench` configuration files in the
Machi `priv` directory.

* [basho_bench.append-example.config](priv/basho_bench.append-example.config),
  an example for writing Machi files.
* [basho_bench.read-example.config](priv/basho_bench.read-example.config),
  an example for reading Machi files.

If you want a test to do both reading & writing ... well, the
driver/plug-in is not mature enough to do it **well**.  If you really
want to, refer to the `basho_bench` docs for how to use the
`operations` config option.

The `basho_bench` config file is configured in Erlang term format.
Each configuration item is a 2-tuple followed by a period.  Comments
begin with a `%` character and continue to the end-of-line.

    %% Mandatory: adjust this code path to top of your compiled Machi source distro
    {code_paths, ["/Users/fritchie/b/src/machi"]}.
    {driver, machi_basho_bench_driver}.
    
    %% Chose your maximum rate (per worker proc, see 'concurrent' below)
    {mode, {rate, 25}}.
    
    %% Runtime & reporting interval 
    {duration, 10}.         % minutes
    {report_interval, 1}.   % seconds
    
    %% Choose your number of worker procs
    {concurrent, 5}.
    
    %% Here's a chain of (up to) length 3, all on localhost
    {machi_server_info,
     [
      {p_srvr,a,machi_flu1_client,"localhost",4444,[]},
      {p_srvr,b,machi_flu1_client,"localhost",4445,[]},
      {p_srvr,c,machi_flu1_client,"localhost",4446,[]}
     ]}.
    {machi_ets_key_tab_type, set}.   % 'set' or 'ordered_set'
    
    %% Workload-specific definitions follow....
    
    %% 10 parts 'append' operation + 0 parts anything else = 100% 'append' ops
    {operations, [{append, 10}]}.
    
    %% For append, key = Machi file prefix name
    {key_generator, {concat_binary, <<"prefix">>,
                                    {to_binstr, "~w", {uniform_int, 30}}}}.
    
    %% Increase size of value_generator_source_size if value_generator is big!!
    {value_generator_source_size, 2111000}.
    {value_generator, {fixed_bin, 32768}}.   %  32 KB

In summary:

* Yes, you really need to change `code_paths` to be the same as your
  `/some/nice/dev/place/basho_bench` directory ... and that directory
  must be on the same machine(s) that you intend to run `basho_bench`.
* Each worker process will have a rate limit of 25 ops/sec.
* The test will run for 10 minutes and report stats every 1 second.
* There are 5 concurrent worker processes.  Each worker will
  concurrently issue commands from the `operations` list, within the
  workload throttle limit.
* The Machi cluster is a collection of three servers, all on
  "localhost", and using TCP ports 4444-4446.
* Don't change the `machi_ets_key_tab_type`
* Our workload operation mix is 100% `append` operations.
* The key generator for the `append` operation specifies the file
  prefix that will be chosen (at pseudo-random).  In this case, we'll
  choose uniformly randomly between file prefix `prefix0` and
  `prefix29`.
* The values that we append will be fixed 32KB length, but they will
  be chosen from a random byte string of 2,111,000 bytes.

There are many other options for `basho_bench`, especially for the
`key_generator` and `value_generator` options.  Please see the
`basho_bench` docs for further information.

## Running basho_bench

You can run `basho_bench` using the command:

    /some/nice/dev/place/basho_bench/basho_bench /path/to/config/file

... where `/path/to/config/file` is the path to your config file. (If
you use an example from the `priv` dir, we recommend that you make a
copy elsewhere, edit the copy, and then use the copy to run
`basho_bench`.)

You'll create a stats output directory, called `tests`, in the current
working directory. (Add `{results_dir, "/some/output/dir"}.` to change
the default!)

Each time `basho_bench` is run, a new output stats directory is
created in the `tests` directory.  The symbolic link `tests/current`
will always point to the last `basho_bench` run's output.  But all
prior results are always accessible!  Take a look in this directory
for all of the output.

## Generating some pretty graphs

If you are using R, then the following command will create a graph:

    Rscript --vanilla /some/nice/dev/place/basho_bench/basho_bench/priv/summary.r -i $CWD/tests/current

If the `tests` directory is not in your current working dir (i.e. not
in `$CWD`), then please alter the command accordingly.

R will create the final results graph in `$CWD/tests/current/summary.png`.

If you are using gnuplot, please look at
`/some/nice/dev/place/basho_bench/basho_bench/Makefile` to see how to
use gnuplot to create the final results graph.

## An example graph

So, without a lot of context about the **Machi system** or about the
**basho_bench system** or about the ops being performed, here is an
example graph that was created by R:

![](https://basho.github.io/machi/images/basho_bench.example0.png)

**Without context??*  How do I remember the context?

My recommendation is: always keep the `.config` file together with the
graph file.  In the `tests` directory, `basho_bench` will always make
a copy of the config file used to generate the test data.

This config tells you very little about the environment of the load
generator machine or the Machi cluster, but ... you need to maintain
that documentation yourself, please!  You'll thank me for that advice,
someday, 11 months from now when you can't remember the details of
that important test that you ran so very long ago.

## Conclusion

Really, we don't recommend using `basho_bench` for any serious
performance measurement of Machi yet: Machi needs more maturity before
it's reasonable to measure & judge its performance.  But stress
testing is indeed useful for reasons other than measuring
Nth-percentile latency of operation `flarfbnitz`.  We hope that this
tutorial has been helpful!

If you encounter any difficulty with this tutorial or with Machi,
please open an issue/ticket at [GH Issues for
Machi](https://github.com/basho/machi/issues) ... use the green "New
issue" button.  There are bugs and misfeatures in the `basho_bench`
plugin, sorry, but please help us fix them.

> -Scott Lystig Fritchie,
> Machi Team @ Basho
