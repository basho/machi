# The chain-manager prototype

This is a very early experiment to try to create a distributed "rough
consensus" algorithm that is sufficient & safe for managing the order
of a Chain Replication chain, its members, and its chain order.
 
## Compilation & unit testing

Use `make` and `make test`.  Note that the Makefile assumes that the
`rebar` utility is available somewhere in your path.

Tested using Erlang/OTP R16B and Erlang/OTP 17, both on OS X.

It ought to "just work" on other versions of Erlang and on other OS
platforms, but sorry, I haven't tested it.
