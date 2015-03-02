
Tango prototype TODO list
=========================

__ The current checkpoint implementation is fundamentally broken and
   needs a rewrite, or else.
   This issue is not mentioned at all in the Tango paper.

   option 1: fix checkpoint to be 100% correct
   option 2: checkpointing is for the weak and the memory-constrained, so
             don't bother.  Instead, rip out the current checkpoint code,
             period. 
   option 3: other

xx Checkpoint fix option #1: history splicing within the same OID?

xx Checkpoint fix option #2: checkpoint to a new OID, history writes to both
                             OIDs during the CP, then a marker in the old OID
                             to switch over to the new OID?

History splicing has a flaw that I belive just won't work.  The switch to a
new OID has problems with updates written to the old OID before and before the
new checkpoint has finished.

I believe that a checkpoint where:
    * all Tango writes, checkpoint and non-checkpoint alike, are noted with
      a checkpoint number.
    * that checkpoint number is strictly increasing
    * a new checkpoint has a new checkpoint number
    * scans ignore blocks with checkpoint numbers larger than the current
      active checkpoint #, until the checkpoint is complete.

... ought to work correctly.  
