------------------------- MODULE EventualMetadataWitness -------------------------
EXTENDS BlobStreamPartition

(*******************************************************************************
  This module models the second documented accepted loss: an eventually
  consistent metadata replica returns a later row while omitting an earlier row,
  and the earlier row becomes visible only after it leaves Fast's bounded scan
  horizon.

  It is a phase-gated witness like StaleWriterWitness. The base model continues
  to provide leasing, sequence allocation, publication order, and cursor
  filtering. This module adds only the state needed to describe what a Fast
  metadata scan can observe; it does not pretend that durable metadata vanished.
*******************************************************************************)
CONSTANTS WriterBroker, LowerBatch, HigherBatch, VisibilityDelay, FastHorizon

VARIABLES
  scenarioPhase,
  metadataPublishedAt,
  metadataOrder,
  replicaVisibility,
  scanObservation,
  fastFrontier,
  lateLowerReturned

witnessVars == <<
  vars,
  scenarioPhase,
  metadataPublishedAt,
  metadataOrder,
  replicaVisibility,
  scanObservation,
  fastFrontier,
  lateLowerReturned
>>

(*******************************************************************************
  Publication order is an abstract stand-in for the metadata snowflake ID. It
  is distinct from the batch sequence range: Fast's frontier filters metadata
  rows by this publication order, while the reader cursor filters batches by
  sequence end.
*******************************************************************************)
MetadataOrders == 0 .. MaxSequence

(*******************************************************************************
  Replica visibility describes the reader's eventually consistent replica, not
  durable metadata. A published row starts NotVisible and can later become
  Visible. This models delayed observation without inventing a delete operation.
*******************************************************************************)
ReplicaVisibilityStates == {"NotVisible", "Visible"}
ScanObservationStates == {"NotScanned", "Omitted", "Returned", "ExcludedByHorizon"}

ScenarioPhases == {
  "AcquireWriter",
  "ReserveLower",
  "AcceptLower",
  "UploadLower",
  "PublishLower",
  "AcknowledgeLower",
  "AdvanceAfterLower",
  "ReserveHigher",
  "AcceptHigher",
  "UploadHigher",
  "PublishHigher",
  "AcknowledgeHigher",
  "AdvanceUntilHigherEligible",
  "ReplicaMakesHigherVisible",
  "ScanOmitsLowerReturnsHigher",
  "DeliverHigher",
  "AdvancePastLowerHorizon",
  "ReplicaMakesLowerVisible",
  "FastExcludesLateLower",
  "LateScanReturnsLower",
  "SkipLateLower",
  "Complete"
}

WitnessInit ==
  /\ Init
  /\ scenarioPhase = "AcquireWriter"
  /\ metadataPublishedAt = [batch \in Batches |-> Null]
  /\ metadataOrder = [batch \in Batches |-> 0]
  /\ replicaVisibility = [batch \in Batches |-> "NotVisible"]
  /\ scanObservation = [batch \in Batches |-> "NotScanned"]
  /\ fastFrontier = 0
  /\ lateLowerReturned = FALSE

(*******************************************************************************
  A row is visibility-eligible only after the configured delay. The delay is a
  best-effort margin, not a replica completeness guarantee: the witness makes
  the later row visible while the earlier row remains absent.
*******************************************************************************)
VisibilityEligible(batch) ==
  /\ metadataPublishedAt[batch] # Null
  /\ now >= metadataPublishedAt[batch] + VisibilityDelay

(*******************************************************************************
  Fast scans retain only rows published within the bounded availability horizon.
  The strict comparison matches the design wording: once a row's time is older
  than the horizon, Fast no longer selects its source window.
*******************************************************************************)
WithinFastHorizon(batch) ==
  /\ metadataPublishedAt[batch] # Null
  /\ now - metadataPublishedAt[batch] <= FastHorizon

(*******************************************************************************
  Wrapper actions combine one base-model action with a scenario phase update.
  Unless an action explicitly changes witness-local state, it leaves that state
  unchanged. The base action remains responsible for every variable in vars.
*******************************************************************************)
AcquireWriter ==
  /\ scenarioPhase = "AcquireWriter"
  /\ AcquireOrRenewLease(WriterBroker)
  /\ scenarioPhase' = "ReserveLower"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

ReserveLower ==
  /\ scenarioPhase = "ReserveLower"
  /\ ReserveRange(WriterBroker, LowerBatch)
  /\ scenarioPhase' = "AcceptLower"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

AcceptLower ==
  /\ scenarioPhase = "AcceptLower"
  /\ AcceptBatch(WriterBroker, LowerBatch)
  /\ scenarioPhase' = "UploadLower"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

UploadLower ==
  /\ scenarioPhase = "UploadLower"
  /\ UploadBlob(WriterBroker, LowerBatch)
  /\ scenarioPhase' = "PublishLower"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

PublishLower ==
  /\ scenarioPhase = "PublishLower"
  /\ PublishMetadata(WriterBroker, LowerBatch)
  /\ metadataPublishedAt' = [metadataPublishedAt EXCEPT ![LowerBatch] = now]
  /\ metadataOrder' = [metadataOrder EXCEPT ![LowerBatch] = 1]
  /\ scenarioPhase' = "AcknowledgeLower"
  /\ UNCHANGED <<replicaVisibility, scanObservation, fastFrontier, lateLowerReturned>>

AcknowledgeLower ==
  /\ scenarioPhase = "AcknowledgeLower"
  /\ AcknowledgeProducer(WriterBroker, LowerBatch)
  /\ scenarioPhase' = "AdvanceAfterLower"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

AdvanceAfterLower ==
  /\ scenarioPhase = "AdvanceAfterLower"
  /\ AdvanceTime
  /\ scenarioPhase' = "ReserveHigher"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

ReserveHigher ==
  /\ scenarioPhase = "ReserveHigher"
  /\ ReserveRange(WriterBroker, HigherBatch)
  /\ scenarioPhase' = "AcceptHigher"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

AcceptHigher ==
  /\ scenarioPhase = "AcceptHigher"
  /\ AcceptBatch(WriterBroker, HigherBatch)
  /\ scenarioPhase' = "UploadHigher"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

UploadHigher ==
  /\ scenarioPhase = "UploadHigher"
  /\ UploadBlob(WriterBroker, HigherBatch)
  /\ scenarioPhase' = "PublishHigher"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

PublishHigher ==
  /\ scenarioPhase = "PublishHigher"
  /\ PublishMetadata(WriterBroker, HigherBatch)
  /\ metadataPublishedAt' = [metadataPublishedAt EXCEPT ![HigherBatch] = now]
  /\ metadataOrder' = [metadataOrder EXCEPT ![HigherBatch] = 2]
  /\ scenarioPhase' = "AcknowledgeHigher"
  /\ UNCHANGED <<replicaVisibility, scanObservation, fastFrontier, lateLowerReturned>>

AcknowledgeHigher ==
  /\ scenarioPhase = "AcknowledgeHigher"
  /\ AcknowledgeProducer(WriterBroker, HigherBatch)
  /\ scenarioPhase' = "AdvanceUntilHigherEligible"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

(*******************************************************************************
  The configuration gives VisibilityDelay the value one. The reader waits one
  logical tick after higher metadata is published, so accepting that row is not
  simply bypassing the delay. The earlier row remains inside Fast's horizon but
  is still absent from the replica, which is the no-bounded-staleness case.
*******************************************************************************)
AdvanceUntilHigherEligible ==
  /\ scenarioPhase = "AdvanceUntilHigherEligible"
  /\ AdvanceTime
  /\ scenarioPhase' = "ReplicaMakesHigherVisible"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

ReplicaMakesHigherVisible ==
  /\ scenarioPhase = "ReplicaMakesHigherVisible"
  /\ VisibilityEligible(HigherBatch)
  /\ WithinFastHorizon(LowerBatch)
  /\ replicaVisibility[HigherBatch] = "NotVisible"
  /\ replicaVisibility' = [replicaVisibility EXCEPT ![HigherBatch] = "Visible"]
  /\ scenarioPhase' = "ScanOmitsLowerReturnsHigher"
  /\ UNCHANGED <<vars, metadataPublishedAt, metadataOrder, scanObservation, fastFrontier,
                 lateLowerReturned>>

(*******************************************************************************
  The scan sees higher metadata but omits lower metadata despite both rows being
  durably published and lower still being within the current Fast horizon. The
  omission is an allowed eventually consistent replica result, not a claim that
  the reader queried a range that excluded lower at this point.
*******************************************************************************)
ScanOmitsLowerReturnsHigher ==
  /\ scenarioPhase = "ScanOmitsLowerReturnsHigher"
  /\ replicaVisibility[LowerBatch] = "NotVisible"
  /\ replicaVisibility[HigherBatch] = "Visible"
  /\ WithinFastHorizon(LowerBatch)
  /\ scanObservation' = [scanObservation EXCEPT
       ![LowerBatch] = "Omitted",
       ![HigherBatch] = "Returned"]
  /\ scenarioPhase' = "DeliverHigher"
  /\ UNCHANGED <<vars, metadataPublishedAt, metadataOrder, replicaVisibility, fastFrontier,
                 lateLowerReturned>>

(*******************************************************************************
  The base complete-view delivery action cannot be used here because it rightly
  sees the durable lower row. This witness action instead consumes the explicit
  replica scan result. It advances both the sequence cursor and the observed
  metadata frontier from higher's returned row, exactly where the design warns
  that a successful scan is not proof lower metadata was returned.
*******************************************************************************)
DeliverHigher ==
  /\ scenarioPhase = "DeliverHigher"
  /\ scanObservation[HigherBatch] = "Returned"
  /\ readerResult[HigherBatch] = "Unseen"
  /\ readerCursor < reservedRange[HigherBatch][1]
  /\ readerCursor' = reservedRange[HigherBatch][2]
  /\ previousReaderCursor' = readerCursor
  /\ readerResult' = [readerResult EXCEPT ![HigherBatch] = "Delivered"]
  /\ fastFrontier' = metadataOrder[HigherBatch]
  /\ scenarioPhase' = "AdvancePastLowerHorizon"
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, previousHighWater,
                 brokerAlive, brokerIncarnation, batchPhase, reservedBy, reservedRange,
                 acceptedLeaseTerm, blobPhase, metadataPhase, metadataPublishedBy,
                 acceptedIncarnation, acknowledgementPhase, metadataPublishedAt, metadataOrder, replicaVisibility,
                 scanObservation, lateLowerReturned>>

AdvancePastLowerHorizon ==
  /\ scenarioPhase = "AdvancePastLowerHorizon"
  /\ AdvanceTime
  /\ now + 1 - metadataPublishedAt[LowerBatch] > FastHorizon
  /\ scenarioPhase' = "ReplicaMakesLowerVisible"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

ReplicaMakesLowerVisible ==
  /\ scenarioPhase = "ReplicaMakesLowerVisible"
  /\ replicaVisibility[LowerBatch] = "NotVisible"
  /\ ~WithinFastHorizon(LowerBatch)
  /\ replicaVisibility' = [replicaVisibility EXCEPT ![LowerBatch] = "Visible"]
  /\ scenarioPhase' = "FastExcludesLateLower"
  /\ UNCHANGED <<vars, metadataPublishedAt, metadataOrder, scanObservation, fastFrontier,
                 lateLowerReturned>>

(*******************************************************************************
  Lower metadata is now visible at the replica, but Fast does not scan its old
  source window any more. The existing frontier is already at higher's metadata
  order. This records exclusion by query bounds separately from cursor filtering.
*******************************************************************************)
FastExcludesLateLower ==
  /\ scenarioPhase = "FastExcludesLateLower"
  /\ replicaVisibility[LowerBatch] = "Visible"
  /\ ~WithinFastHorizon(LowerBatch)
  /\ metadataOrder[LowerBatch] < fastFrontier
  /\ scanObservation' = [scanObservation EXCEPT ![LowerBatch] = "ExcludedByHorizon"]
  /\ scenarioPhase' = "LateScanReturnsLower"
  /\ UNCHANGED <<vars, metadataPublishedAt, metadataOrder, replicaVisibility, fastFrontier,
                 lateLowerReturned>>

(*******************************************************************************
  Fast does not return lower from its old window, but a later explicit
  rediscovery does return the durable row. This transition separates the query
  bound that first excluded lower from the normal cursor filter that later
  prevents delivery once higher has already advanced the cursor.
*******************************************************************************)
LateScanReturnsLower ==
  /\ scenarioPhase = "LateScanReturnsLower"
  /\ replicaVisibility[LowerBatch] = "Visible"
  /\ scanObservation[LowerBatch] = "ExcludedByHorizon"
  /\ lateLowerReturned' = TRUE
  /\ scenarioPhase' = "SkipLateLower"
  /\ UNCHANGED <<vars, metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier>>

(*******************************************************************************
  Even if a later query did rediscover the row, normal cursor filtering cannot
  deliver it: lower's sequence end is already at or below the cursor advanced by
  higher. Reusing the base action preserves the same cursor rule as production.
*******************************************************************************)
SkipLateLower ==
  /\ scenarioPhase = "SkipLateLower"
  /\ lateLowerReturned
  /\ SkipCoveredBatch(LowerBatch)
  /\ scenarioPhase' = "Complete"
  /\ UNCHANGED <<metadataPublishedAt, metadataOrder, replicaVisibility, scanObservation,
                 fastFrontier, lateLowerReturned>>

WitnessQuiescent ==
  /\ scenarioPhase = "Complete"
  /\ UNCHANGED witnessVars

WitnessNext ==
  \/ AcquireWriter
  \/ ReserveLower
  \/ AcceptLower
  \/ UploadLower
  \/ PublishLower
  \/ AcknowledgeLower
  \/ AdvanceAfterLower
  \/ ReserveHigher
  \/ AcceptHigher
  \/ UploadHigher
  \/ PublishHigher
  \/ AcknowledgeHigher
  \/ AdvanceUntilHigherEligible
  \/ ReplicaMakesHigherVisible
  \/ ScanOmitsLowerReturnsHigher
  \/ DeliverHigher
  \/ AdvancePastLowerHorizon
  \/ ReplicaMakesLowerVisible
  \/ FastExcludesLateLower
  \/ LateScanReturnsLower
  \/ SkipLateLower
  \/ WitnessQuiescent

WitnessSpec == WitnessInit /\ [][WitnessNext]_witnessVars

WitnessConstantsOK ==
  /\ WriterBroker \in Brokers
  /\ LowerBatch \in Batches
  /\ HigherBatch \in Batches
  /\ LowerBatch # HigherBatch
  /\ VisibilityDelay \in Nat
  /\ FastHorizon \in Nat

WitnessTypeOK ==
  /\ TypeOK
  /\ scenarioPhase \in ScenarioPhases
  /\ metadataPublishedAt \in [Batches -> ((0 .. MaxTime) \cup {Null})]
  /\ metadataOrder \in [Batches -> MetadataOrders]
  /\ replicaVisibility \in [Batches -> ReplicaVisibilityStates]
  /\ scanObservation \in [Batches -> ScanObservationStates]
  /\ fastFrontier \in MetadataOrders
  /\ lateLowerReturned \in BOOLEAN

(*******************************************************************************
  EventualMetadataLoss is a derived predicate. It requires evidence of every
  causal link: lower durable publication, a scan that omitted it while it was
  inside Fast's horizon, delivery of higher, later replica visibility outside
  the horizon, exclusion by query bounds, and final cursor filtering.
*******************************************************************************)
EventualMetadataLoss ==
  /\ scenarioPhase = "Complete"
  /\ acknowledgementPhase[LowerBatch] = "Acknowledged"
  /\ acknowledgementPhase[HigherBatch] = "Acknowledged"
  /\ metadataOrder[LowerBatch] < metadataOrder[HigherBatch]
  /\ scanObservation[HigherBatch] = "Returned"
  /\ scanObservation[LowerBatch] = "ExcludedByHorizon"
  /\ replicaVisibility[LowerBatch] = "Visible"
  /\ lateLowerReturned
  /\ readerResult[HigherBatch] = "Delivered"
  /\ readerResult[LowerBatch] = "Skipped"
  /\ fastFrontier = metadataOrder[HigherBatch]
  /\ reservedRange[LowerBatch][2] < readerCursor

NoEventualMetadataLoss == ~EventualMetadataLoss

(*******************************************************************************
  In this tightly prescribed trace, a skipped batch must be lower and must have
  the documented eventual-consistency cause. This is the passing regression
  guard paired with the intentionally failing no-loss assertion.
*******************************************************************************)
AllSkippedBatchesHaveKnownCause ==
  \A batch \in Batches :
    readerResult[batch] = "Skipped" =>
      /\ batch = LowerBatch
      /\ EventualMetadataLoss

=============================================================================
