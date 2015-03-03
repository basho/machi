## Notes on developing & debugging this CORFU prototype

I've recorded some notes while developing & debugging this CORFU
prototype.  See the `corfurl/notes` subdirectory.

Most of the cases mentioned involve race conditions that were notable
during the development cycle.  There is one case that IIRC is not
mentioned in any of the CORFU papers and is probably a case that
cannot be fixed/solved by CORFU itself.

Each of the scenario notes includes an MSC diagram specification file
to help illustrate the race.  The diagrams are annotated by hand, both
with text and color, to point out critical points of timing.

## CORFU papers

I recommend the "5 pages" paper below first, to give a flavor of
what the CORFU is about.  When Scott first read the CORFU paper
back in 2011 (and the Hyder paper), he thought it was insanity.
He recommends waiting before judging quite so hastily.  :-)

After that, then perhaps take a step back are skim over the
Hyder paper.  Hyder started before CORFU, but since CORFU, the
Hyder folks at Microsoft have rewritten Hyder to use CORFU as
the shared log underneath it.  But the Hyder paper has lots of
interesting bits about how you'd go about creating a distributed
DB where the transaction log *is* the DB.

### "CORFU: A Distributed Shared Log"

MAHESH BALAKRISHNAN, DAHLIA MALKHI, JOHN D. DAVIS, and VIJAYAN
PRABHAKARAN, Microsoft Research Silicon Valley, MICHAEL WEI,
University of California, San Diego, TED WOBBER, Microsoft Research
Silicon Valley

Long version of introduction to CORFU (~30 pages)
http://www.snookles.com/scottmp/corfu/corfu.a10-balakrishnan.pdf

### "CORFU: A Shared Log Design for Flash Clusters"

Same authors as above

Short version of introduction to CORFU paper above (~12 pages)

http://www.snookles.com/scottmp/corfu/corfu-shared-log-design.nsdi12-final30.pdf

### "From Paxos to CORFU: A Flash-Speed Shared Log"

Same authors as above

5 pages, a short summary of CORFU basics and some trial applications
that have been implemented on top of it.

http://www.snookles.com/scottmp/corfu/paxos-to-corfu.malki-acmstyle.pdf

### "Beyond Block I/O: Implementing a Distributed Shared Log in Hardware"

Wei, Davis, Wobber, Balakrishnan, Malkhi

Summary report of implmementing the CORFU server-side in
FPGA-style hardware. (~11 pages)

http://www.snookles.com/scottmp/corfu/beyond-block-io.CameraReady.pdf

### "Tango: Distributed Data Structures over a Shared Log"

Balakrishnan, Malkhi, Wobber, Wu, Brabhakaran, Wei, Davis, Rao, Zou, Zuck

Describes a framework for developing data structures that reside
persistently within a CORFU log: the log *is* the database/data
structure store.

http://www.snookles.com/scottmp/corfu/Tango.pdf

### "Dynamically Scalable, Fault-Tolerant Coordination on a Shared Logging Service"

Wei, Balakrishnan, Davis, Malkhi, Prabhakaran, Wobber

The ZooKeeper inter-server communication is replaced with CORFU.
Faster, fewer lines of code than ZK, and more features than the
original ZK code base.

http://www.snookles.com/scottmp/corfu/zookeeper-techreport.pdf

### "Hyder – A Transactional Record Manager for Shared Flash"

Bernstein, Reid, Das

Describes a distributed log-based DB system where the txn log is
treated quite oddly: a "txn intent" record is written to a
shared common log All participants read the shared log in
parallel and make commit/abort decisions in parallel, based on
what conflicts (or not) that they see in the log.  Scott's first
reading was "No way, wacky" ... and has since changed his mind.

http://www.snookles.com/scottmp/corfu/CIDR11Proceedings.pdf
pages 9-20

