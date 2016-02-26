## Machi developer environment prerequisites

1. Machi requires an 64-bit variant of UNIX: OS X, FreeBSD, Linux, or
   Solaris machine is a standard developer environment for C and C++
   applications (64-bit versions).
2. You'll need the `git` source management utility.
3. You'll need the 64-bit Erlang/OTP 17 runtime environment.  Please
   don't use earlier or later versions until we have a chance to fix
   the compilation warnings that versions R16B and 18 will trigger.
   Also, please verify that you are not using a 32-bit Erlang/OTP
   runtime package.

For `git` and the Erlang runtime, please use your OS-specific
package manager to install these.  If your package manager doesn't
have 64-bit Erlang/OTP version 17 available, then we recommend using the
[precompiled packages available at Erlang Solutions](https://www.erlang-solutions.com/resources/download.html).

Also, please verify that you have enough file descriptors available to
your user processes.  The output of `ulimit -n` should report at least
4,000 file descriptors available.  If your limit is lower (a frequent
problem for OS X users), please increase it to at least 4,000.

# Using Vagrant to set up a developer environment for Machi

The Machi source directory contains a `Vagrantfile` for creating an
Ubuntu Linux-based virtual machine for compiling and running Machi.
This file is in the
[$SRC_TOP/priv/humming-consensus-demo.vagrant](../priv/humming-consensus-demo.vagrant)
directory.

If used as-is, the virtual machine specification is modest.

* 1 virtual CPU
* 512MB virtual memory
* 768MB swap space
* 79GB sparse virtual disk image.  After installing prerequisites and
  compiling Machi, the root file system uses approximately 2.7 GBytes.

