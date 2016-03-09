# Clone and compile Machi

Clone the Machi source repo and compile the source and test code.  Run
the following commands at your login shell:

    cd /tmp
    git clone https://github.com/basho/machi.git
    cd machi
    git checkout master
    make          # or 'gmake' if GNU make uses an alternate name

Then run the unit test suite.  This may take up to two minutes or so
to finish.

    make test

At the end, the test suite should report that all tests passed.  The
actual number of tests shown in the "All `X` tests passed" line may be
different than the example below.

    [... many lines omitted ...]
    module 'event_logger'
    module 'chain_mgr_legacy'
    =======================================================
      All 90 tests passed.

If you had a test failure, a likely cause may be a limit on the number
of file descriptors available to your user process.  (Recent releases
of OS X have a limit of 1024 file descriptors, which may be too slow.)
The output of the `limit -n` will tell you your file descriptor limit.
