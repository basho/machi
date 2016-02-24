## Machi developer environment prerequisites

1. Machi requires an OS X, FreeBSD, Linux, or Solaris machine is a
   standard developer environment for C and C++ applications.
2. You'll need the `git` source management utility.
3. You'll need the Erlang/OTP 17 runtime environment.  Please don't
   use earlier or later versions until we have a chance to fix the
   compilation warnings that versions R16B and 18 will trigger.

For `git` and the Erlang runtime, please use your OS-specific
package manager to install these.  If your package manager doesn't
have Erlang/OTP version 17 available, then we recommend using the
[precompiled packages available at Erlang Solutions](https://www.erlang-solutions.com/resources/download.html).

Also, please verify that you have enough file descriptors available to
your user processes.  The output of `ulimit -n` should report at least
4,000 file descriptors available.  If your limit is lower (a frequent
problem for OS X users), please increase it to at least 4,000.
