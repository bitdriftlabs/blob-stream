--------------------------- MODULE StaleWriterWitness ---------------------------
EXTENDS BlobStreamPartition

(*******************************************************************************
  This module constrains the base model to one documented stale-writer trace.
  It does not alter BlobStreamPartition's general transitions. Instead, each
  witness action combines one base-model action with a scenario phase update.
  TLC can therefore show the exact causal path without a configuration
  constraint silently pruning unrelated transitions.

  The configuration assigns these names to members of the base-model broker and
  batch sets. FormerBroker accepts the lower range. SuccessorBroker later owns
  the lease and publishes a higher range before the former broker resumes its
  metadata write.
*******************************************************************************)
CONSTANTS FormerBroker, SuccessorBroker, LowerBatch, HigherBatch

VARIABLE scenarioPhase

witnessVars == <<vars, scenarioPhase>>

(*******************************************************************************
  The string values make TLC's trace readable for a first-time TLA+ reader.
  They are all finite values, so this additional control state preserves a
  finite state space.
*******************************************************************************)
ScenarioPhases == {
  "AcquireFormer",
  "ReserveLower",
  "AcceptLower",
  "UploadLower",
  "ExpireFormerFirstTick",
  "ExpireFormerSecondTick",
  "AcquireSuccessor",
  "ReserveHigher",
  "AcceptHigher",
  "UploadHigher",
  "PublishHigher",
  "DeliverHigher",
  "PublishLower",
  "AcknowledgeLower",
  "SkipLower",
  "Complete"
}

WitnessInit ==
  /\ Init
  /\ scenarioPhase = "AcquireFormer"

(*******************************************************************************
  Every action below calls a base-model action and assigns scenarioPhase'. The
  base action defines the primed values of all base variables; this wrapper
  defines the one additional witness variable.
*******************************************************************************)
AcquireFormer ==
  /\ scenarioPhase = "AcquireFormer"
  /\ AcquireOrRenewLease(FormerBroker)
  /\ scenarioPhase' = "ReserveLower"

ReserveLower ==
  /\ scenarioPhase = "ReserveLower"
  /\ ReserveRange(FormerBroker, LowerBatch)
  /\ scenarioPhase' = "AcceptLower"

AcceptLower ==
  /\ scenarioPhase = "AcceptLower"
  /\ AcceptBatch(FormerBroker, LowerBatch)
  /\ scenarioPhase' = "UploadLower"

UploadLower ==
  /\ scenarioPhase = "UploadLower"
  /\ UploadBlob(FormerBroker, LowerBatch)
  /\ scenarioPhase' = "ExpireFormerFirstTick"

(*******************************************************************************
  LeaseDuration is two in this small witness configuration. Advancing time
  twice makes FormerBroker's lease invalid without crashing its process: this
  represents the long-pause or partitioned former holder from the design.
*******************************************************************************)
ExpireFormerFirstTick ==
  /\ scenarioPhase = "ExpireFormerFirstTick"
  /\ AdvanceTime
  /\ scenarioPhase' = "ExpireFormerSecondTick"

ExpireFormerSecondTick ==
  /\ scenarioPhase = "ExpireFormerSecondTick"
  /\ AdvanceTime
  /\ scenarioPhase' = "AcquireSuccessor"

AcquireSuccessor ==
  /\ scenarioPhase = "AcquireSuccessor"
  /\ AcquireOrRenewLease(SuccessorBroker)
  /\ scenarioPhase' = "ReserveHigher"

ReserveHigher ==
  /\ scenarioPhase = "ReserveHigher"
  /\ ReserveRange(SuccessorBroker, HigherBatch)
  /\ scenarioPhase' = "AcceptHigher"

AcceptHigher ==
  /\ scenarioPhase = "AcceptHigher"
  /\ AcceptBatch(SuccessorBroker, HigherBatch)
  /\ scenarioPhase' = "UploadHigher"

UploadHigher ==
  /\ scenarioPhase = "UploadHigher"
  /\ UploadBlob(SuccessorBroker, HigherBatch)
  /\ scenarioPhase' = "PublishHigher"

PublishHigher ==
  /\ scenarioPhase = "PublishHigher"
  /\ PublishMetadata(SuccessorBroker, HigherBatch)
  /\ scenarioPhase' = "DeliverHigher"

DeliverHigher ==
  /\ scenarioPhase = "DeliverHigher"
  /\ DeliverPublishedBatch(HigherBatch)
  /\ scenarioPhase' = "PublishLower"

(*******************************************************************************
  This is the essential stale-writer transition. FormerBroker remains alive but
  no longer owns a valid lease. PublishMetadata intentionally permits this
  write because the production path has no durable cross-process publication
  fence. The reader has already advanced through HigherBatch at this point.
*******************************************************************************)
PublishLower ==
  /\ scenarioPhase = "PublishLower"
  /\ PublishMetadata(FormerBroker, LowerBatch)
  /\ scenarioPhase' = "AcknowledgeLower"

AcknowledgeLower ==
  /\ scenarioPhase = "AcknowledgeLower"
  /\ AcknowledgeProducer(FormerBroker, LowerBatch)
  /\ scenarioPhase' = "SkipLower"

SkipLower ==
  /\ scenarioPhase = "SkipLower"
  /\ SkipCoveredBatch(LowerBatch)
  /\ scenarioPhase' = "Complete"

(*******************************************************************************
  The witness completes only after the lower acknowledged batch is skipped.
  Keeping stuttering guarded retains TLC's deadlock detection before completion.
*******************************************************************************)
WitnessQuiescent ==
  /\ scenarioPhase = "Complete"
  /\ UNCHANGED witnessVars

WitnessNext ==
  \/ AcquireFormer
  \/ ReserveLower
  \/ AcceptLower
  \/ UploadLower
  \/ ExpireFormerFirstTick
  \/ ExpireFormerSecondTick
  \/ AcquireSuccessor
  \/ ReserveHigher
  \/ AcceptHigher
  \/ UploadHigher
  \/ PublishHigher
  \/ DeliverHigher
  \/ PublishLower
  \/ AcknowledgeLower
  \/ SkipLower
  \/ WitnessQuiescent

WitnessSpec == WitnessInit /\ [][WitnessNext]_witnessVars

(*******************************************************************************
  These invariants protect the fixture itself. They ensure the configuration
  really represents two distinct brokers/batches inside the base-model sets.
*******************************************************************************)
WitnessConstantsOK ==
  /\ FormerBroker \in Brokers
  /\ SuccessorBroker \in Brokers
  /\ FormerBroker # SuccessorBroker
  /\ LowerBatch \in Batches
  /\ HigherBatch \in Batches
  /\ LowerBatch # HigherBatch

WitnessTypeOK ==
  /\ TypeOK
  /\ scenarioPhase \in ScenarioPhases

(*******************************************************************************
  StaleWriterPublicationLoss is a derived state predicate, not a new mutable
  flag. It expresses the accepted limitation with concrete evidence:

    - a former holder's lower batch was published and acknowledged;
    - a successor's higher batch was delivered first;
    - the former holder no longer has a valid lease; and
    - cursor filtering skipped the lower acknowledged batch.

  The safety configuration checks the base invariants while allowing this
  predicate. The expected-failure configuration negates it so TLC prints this
  exact witness trace.
*******************************************************************************)
StaleWriterPublicationLoss ==
  /\ scenarioPhase = "Complete"
  /\ metadataPublishedBy[LowerBatch] = FormerBroker
  /\ acknowledgementPhase[LowerBatch] = "Acknowledged"
  /\ readerResult[HigherBatch] = "Delivered"
  /\ readerResult[LowerBatch] = "Skipped"
  /\ acceptedLeaseTerm[LowerBatch] < acceptedLeaseTerm[HigherBatch]
  /\ ~ValidLease(FormerBroker)
  /\ reservedRange[LowerBatch][2] < readerCursor

NoStaleWriterPublicationLoss == ~StaleWriterPublicationLoss

(*******************************************************************************
  A skipped batch in this tightly constrained scenario must be the documented
  stale-writer loss. This passing invariant will catch an accidental new skip
  path if a future edit expands the witness transitions.
*******************************************************************************)
AllSkippedBatchesHaveKnownCause ==
  \A batch \in Batches :
    readerResult[batch] = "Skipped" =>
      /\ batch = LowerBatch
      /\ StaleWriterPublicationLoss

=============================================================================
