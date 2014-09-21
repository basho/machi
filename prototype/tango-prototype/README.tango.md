
Tango prototype TODO list
=========================

__ The current checkpoint implementation is fundamentally broken and
   needs a rewrite, or else.
   option 1: fix checkpoint to be 100% correct
   option 2: checkpointing is for the weak and the memory-constrained, so
             don't bother.  Instead, rip out the current checkpoint code,
             period. 

__ Checkpoint fix option #1: history splicing within the same OID?

__ Checkpoint fix option #2: checkpoint to a new OID, history writes to both
                             OIDs during the CP, then a marker in the old OID
                             to switch over to the new OID?


