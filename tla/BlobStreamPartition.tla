--------------------------- MODULE BlobStreamPartition ---------------------------
EXTENDS Naturals, TLC

(*******************************************************************************
  BlobStreamPartition is a deliberately small model of one blob-stream virtual
  partition. It is a learning model, not a line-for-line implementation of the
  Rust services.

  Stage 3 models the producer-side safety boundary:

    acquire producer lease -> reserve a Hi-Lo block -> accept one batch
      -> persist blob -> publish metadata -> acknowledge producer
      -> reader delivers or safely skips covered metadata

  The reader currently has a complete metadata view. A later stage will make
  metadata observation incomplete to model the Fast reader's bounded horizon.

  A TLA+ module describes all permitted states and transitions. TLC starts at
  Init and explores every possible Next transition within the finite constants
  supplied by BlobStreamPartition.cfg.
*******************************************************************************)

(*******************************************************************************
  Constants are fixed for one TLC run. They are intentionally tiny: exhaustive
  exploration of a small model teaches us more than a large model that cannot
  finish.

  Brokers          process identities, such as BrokerA and BrokerB.
  Batches          symbolic accepted batches, not individual application records.
  LeaseDuration    logical clock ticks for which a producer lease is valid.
  ReservationSize  sequence values in one abstract Hi-Lo reservation.
  MaxSequence      prevents unbounded reservation and keeps TLC's state space finite.
  MaxTime          prevents logical time from growing without bound.
  MaxLeaseTerm     bounds release/reacquire cycles in this finite teaching model.
  MaxIncarnation   bounds crash/restart cycles in this finite teaching model.
  Null             a finite sentinel for absent durable/process-local state.
*******************************************************************************)
CONSTANTS Brokers, Batches, LeaseDuration, ReservationSize, MaxSequence, MaxTime, MaxLeaseTerm,
          MaxIncarnation, Null

(*******************************************************************************
  Null is assigned in the TLC configuration as a symbolic model value such as
  NoValue. Unlike the previous unbounded CHOOSE expression, this gives TLC a
  finite state domain it can enumerate. The configuration must keep Null distinct
  from configured brokers and batches.
*******************************************************************************)

(*******************************************************************************
  A sequence range is represented as <<first, last>>, inclusive at both ends.
  This matches blob-stream's SeqRange contract. A range can have gaps from a
  previous range; the model checks only that ranges never overlap.
*******************************************************************************)
SequenceRange == {range \in Nat \X Nat : range[1] <= range[2]}

RangeValues(range) == range[1] .. range[2]

(*******************************************************************************
  The model state. Each VARIABLES name is mutable: every action below specifies
  its next value with a prime, for example highWater'.

  now                 a monotonic logical clock, not wall time.
  leaseHolder         durable producer lease owner, or Null when unowned.
  leaseExpiresAt      logical time at which the durable lease stops being valid.
  leaseTerm           increases whenever an expired/unowned lease is acquired.
  highWater           highest sequence value durably reserved by Hi-Lo.
  previousHighWater   records the prior state's high-water for an easy invariant.
  brokerAlive         whether a broker process can initiate new work.
  brokerIncarnation   increments on restart, making crash/restart state visible.
  batchPhase          New, Reserved, or Accepted for each symbolic batch.
  reservedBy          broker that durably reserved this batch's sequence range.
  reservedRange       the durable sequence range, or Null before reservation.
  acceptedLeaseTerm   lease term observed when the batch was accepted.
  blobPhase           NotUploaded or Uploaded for each accepted batch's blob.
  metadataPhase       NotPublished or Published for each batch's metadata row.
  metadataPublishedBy broker that wrote the metadata row, or Null before it.
  acknowledgementPhase NotAcknowledged or Acknowledged for the producer reply.
  readerCursor        greatest sequence end the reader has processed.
  previousReaderCursor readerCursor from the preceding transition.
  readerResult        Unseen, Delivered, or Skipped for each published batch.

  A real broker can allocate many batches from one large reservation. This
  introductory model assigns one symbolic batch per reservation so the first
  safety proof focuses on non-overlap. A later refinement can split a reserved
  block into multiple accepted subranges.
*******************************************************************************)
VARIABLES
  now,
  leaseHolder,
  leaseExpiresAt,
  leaseTerm,
  highWater,
  previousHighWater,
  brokerAlive,
  brokerIncarnation,
  batchPhase,
  reservedBy,
  reservedRange,
  acceptedLeaseTerm,
  blobPhase,
  metadataPhase,
  metadataPublishedBy,
  acknowledgementPhase,
  readerCursor,
  previousReaderCursor,
  readerResult

vars == <<
  now,
  leaseHolder,
  leaseExpiresAt,
  leaseTerm,
  highWater,
  previousHighWater,
  brokerAlive,
  brokerIncarnation,
  batchPhase,
  reservedBy,
  reservedRange,
  acceptedLeaseTerm,
  blobPhase,
  metadataPhase,
  metadataPublishedBy,
  acknowledgementPhase,
  readerCursor,
  previousReaderCursor,
  readerResult
>>

(*******************************************************************************
  These helpers refer to mutable state variables, so TLA+ requires them to be
  declared after VARIABLES. They describe whether a broker currently owns the
  durable producer lease and how an acquisition affects its fencing term.
*******************************************************************************)
ValidLease(broker) ==
  leaseHolder = broker /\ now < leaseExpiresAt

RenewingLease(broker) ==
  leaseHolder = broker /\ now < leaseExpiresAt

NextLeaseTerm(broker) ==
  IF RenewingLease(broker) THEN leaseTerm ELSE leaseTerm + 1

(*******************************************************************************
  Init defines exactly one initial state. Every broker starts alive, no durable
  producer lease exists, no sequence has been reserved, and every batch is New.
*******************************************************************************)
Init ==
  /\ now = 0
  /\ leaseHolder = Null
  /\ leaseExpiresAt = 0
  /\ leaseTerm = 0
  /\ highWater = 0
  /\ previousHighWater = 0
  /\ brokerAlive = [broker \in Brokers |-> TRUE]
  /\ brokerIncarnation = [broker \in Brokers |-> 0]
  /\ batchPhase = [batch \in Batches |-> "New"]
  /\ reservedBy = [batch \in Batches |-> Null]
  /\ reservedRange = [batch \in Batches |-> Null]
  /\ acceptedLeaseTerm = [batch \in Batches |-> Null]
  /\ blobPhase = [batch \in Batches |-> "NotUploaded"]
  /\ metadataPhase = [batch \in Batches |-> "NotPublished"]
  /\ metadataPublishedBy = [batch \in Batches |-> Null]
  /\ acknowledgementPhase = [batch \in Batches |-> "NotAcknowledged"]
  /\ readerCursor = 0
  /\ previousReaderCursor = 0
  /\ readerResult = [batch \in Batches |-> "Unseen"]

(*******************************************************************************
  AcquireOrRenewLease models the conditional durable lease mutation.

  Before: broker is alive and either already holds an unexpired lease, or the
          durable lease is absent/expired.
  After:  broker holds a lease through now + LeaseDuration. Acquiring after an
          expiry increments leaseTerm; renewing an existing lease preserves it.

  The term is not needed for every Stage 1 invariant. It is recorded now because
  later consumer and stale-writer stages need an explicit fencing identity.
*******************************************************************************)
AcquireOrRenewLease(broker) ==
  /\ broker \in Brokers
  /\ brokerAlive[broker]
  /\ leaseHolder = broker \/ now >= leaseExpiresAt
  /\ RenewingLease(broker) \/ leaseTerm < MaxLeaseTerm
  /\ leaseHolder' = broker
  /\ leaseExpiresAt' = now + LeaseDuration
  /\ leaseTerm' = NextLeaseTerm(broker)
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<now, highWater, previousHighWater, brokerAlive, brokerIncarnation, batchPhase,
                 reservedBy, reservedRange, acceptedLeaseTerm, blobPhase, metadataPhase,
                 metadataPublishedBy, acknowledgementPhase, readerCursor, readerResult>>

(*******************************************************************************
  ReserveRange models the atomic lease-store operation that advances the durable
  Hi-Lo high-water mark and returns the newly reserved inclusive block.

  Before: the broker is the valid lease holder and the chosen batch is New.
  After:  highWater advances by ReservationSize and the batch owns exactly that
          new range. Since the next range starts at old highWater + 1, two
          successful reservations cannot overlap.
*******************************************************************************)
ReserveRange(broker, batch) ==
  /\ broker \in Brokers
  /\ batch \in Batches
  /\ brokerAlive[broker]
  /\ ValidLease(broker)
  /\ batchPhase[batch] = "New"
  /\ highWater + ReservationSize <= MaxSequence
  /\ highWater' = highWater + ReservationSize
  /\ previousHighWater' = highWater
  /\ batchPhase' = [batchPhase EXCEPT ![batch] = "Reserved"]
  /\ reservedBy' = [reservedBy EXCEPT ![batch] = broker]
  /\ reservedRange' = [reservedRange EXCEPT ![batch] = <<highWater + 1, highWater + ReservationSize>>]
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, brokerAlive, brokerIncarnation,
                 acceptedLeaseTerm, blobPhase, metadataPhase, metadataPublishedBy,
                 acknowledgementPhase, readerCursor, readerResult>>

(*******************************************************************************
  AcceptBatch models the broker's in-memory transition from an allocated range
  to accepted producer work. It is deliberately earlier than blob persistence:
  publication will be a separate stage of the model.

  Before: the reserving broker is still the valid lease holder.
  After:  the batch is Accepted and remembers the lease term that authorized it.
*******************************************************************************)
AcceptBatch(broker, batch) ==
  /\ broker \in Brokers
  /\ batch \in Batches
  /\ brokerAlive[broker]
  /\ ValidLease(broker)
  /\ batchPhase[batch] = "Reserved"
  /\ reservedBy[batch] = broker
  /\ batchPhase' = [batchPhase EXCEPT ![batch] = "Accepted"]
  /\ acceptedLeaseTerm' = [acceptedLeaseTerm EXCEPT ![batch] = leaseTerm]
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, previousHighWater,
                 brokerAlive, brokerIncarnation, reservedBy, reservedRange, blobPhase,
                 metadataPhase, metadataPublishedBy, acknowledgementPhase, readerCursor,
                 readerResult>>

(*******************************************************************************
  UploadBlob is the first durable publication step. A broker may persist work
  only after it accepted the work while holding a valid lease. Requiring the
  original accepting broker keeps this model's initial publication path narrow;
  a future retry/handoff refinement can introduce explicit transfer semantics.
*******************************************************************************)
UploadBlob(broker, batch) ==
  /\ broker \in Brokers
  /\ batch \in Batches
  /\ brokerAlive[broker]
  /\ batchPhase[batch] = "Accepted"
  /\ reservedBy[batch] = broker
  /\ blobPhase[batch] = "NotUploaded"
  /\ blobPhase' = [blobPhase EXCEPT ![batch] = "Uploaded"]
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, previousHighWater,
                 brokerAlive, brokerIncarnation, batchPhase, reservedBy, reservedRange,
                 acceptedLeaseTerm, metadataPhase, metadataPublishedBy, acknowledgementPhase,
                 readerCursor, readerResult>>

(*******************************************************************************
  PublishMetadata records the second durable publication step. Its intentionally
  absent ValidLease guard models the documented limitation: an alive broker that
  accepted and uploaded work before losing its lease may later write metadata.
  The stale-writer witness will make this permitted ordering visible to a reader.
*******************************************************************************)
PublishMetadata(broker, batch) ==
  /\ broker \in Brokers
  /\ batch \in Batches
  /\ brokerAlive[broker]
  /\ batchPhase[batch] = "Accepted"
  /\ reservedBy[batch] = broker
  /\ blobPhase[batch] = "Uploaded"
  /\ metadataPhase[batch] = "NotPublished"
  /\ metadataPhase' = [metadataPhase EXCEPT ![batch] = "Published"]
  /\ metadataPublishedBy' = [metadataPublishedBy EXCEPT ![batch] = broker]
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, previousHighWater,
                 brokerAlive, brokerIncarnation, batchPhase, reservedBy, reservedRange,
                 acceptedLeaseTerm, blobPhase, acknowledgementPhase, readerCursor,
                 readerResult>>

(*******************************************************************************
  AcknowledgeProducer models successful completion of the broker's flush path.
  It happens only after the metadata row is durable. The acknowledgement is
  tracked separately because a future reader can observe metadata before the
  producer receives its RPC response.
*******************************************************************************)
AcknowledgeProducer(broker, batch) ==
  /\ broker \in Brokers
  /\ batch \in Batches
  /\ brokerAlive[broker]
  /\ metadataPhase[batch] = "Published"
  /\ metadataPublishedBy[batch] = broker
  /\ acknowledgementPhase[batch] = "NotAcknowledged"
  /\ acknowledgementPhase' = [acknowledgementPhase EXCEPT ![batch] = "Acknowledged"]
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, previousHighWater,
                 brokerAlive, brokerIncarnation, batchPhase, reservedBy, reservedRange,
                 acceptedLeaseTerm, blobPhase, metadataPhase, metadataPublishedBy, readerCursor,
                 readerResult>>

(*******************************************************************************
  DeliverPublishedBatch is the minimal reader transition for this stage. The
  reader has a complete view of all published metadata, so it may select any
  unseen batch above its cursor. A later stage will replace that assumption with
  an explicit eventually consistent metadata scan and Fast frontier.

  The guard uses the range start rather than requiring contiguity: Hi-Lo gaps
  are valid, and a cursor can advance from one delivered range to a later one.
*******************************************************************************)
DeliverPublishedBatch(batch) ==
  /\ batch \in Batches
  /\ metadataPhase[batch] = "Published"
  /\ blobPhase[batch] = "Uploaded"
  /\ readerResult[batch] = "Unseen"
  /\ reservedRange[batch] \in SequenceRange
  /\ readerCursor < reservedRange[batch][1]
  /\ readerCursor' = reservedRange[batch][2]
  /\ previousReaderCursor' = readerCursor
  /\ readerResult' = [readerResult EXCEPT ![batch] = "Delivered"]
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, previousHighWater,
                 brokerAlive, brokerIncarnation, batchPhase, reservedBy, reservedRange,
                 acceptedLeaseTerm, blobPhase, metadataPhase, metadataPublishedBy,
                 acknowledgementPhase>>

(*******************************************************************************
  SkipCoveredBatch models cursor filtering during a later metadata observation.
  A metadata row whose entire range is at or below the cursor is not delivered
  again. In the normal complete-view model this is harmless replay handling; in
  the later stale-writer witness, the same transition will reveal an accepted
  loss when late lower metadata is first published after higher delivery.
*******************************************************************************)
SkipCoveredBatch(batch) ==
  /\ batch \in Batches
  /\ metadataPhase[batch] = "Published"
  /\ readerResult[batch] = "Unseen"
  /\ reservedRange[batch] \in SequenceRange
  /\ reservedRange[batch][2] <= readerCursor
  /\ previousReaderCursor' = readerCursor
  /\ readerResult' = [readerResult EXCEPT ![batch] = "Skipped"]
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, previousHighWater,
                 brokerAlive, brokerIncarnation, batchPhase, reservedBy, reservedRange,
                 acceptedLeaseTerm, blobPhase, metadataPhase, metadataPublishedBy,
                 acknowledgementPhase, readerCursor>>

(*******************************************************************************
  AdvanceTime is the only action that changes logical time. It never changes a
  lease record itself: a lease becomes invalid because the guard in ValidLease
  compares now with leaseExpiresAt. This avoids modeling expiry as a destructive
  background operation and mirrors lease-store expiry checks.
*******************************************************************************)
AdvanceTime ==
  /\ now < MaxTime
  /\ now' = now + 1
  /\ previousHighWater' = highWater
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<leaseHolder, leaseExpiresAt, leaseTerm, highWater, brokerAlive,
                 brokerIncarnation, batchPhase, reservedBy, reservedRange, acceptedLeaseTerm,
                 blobPhase, metadataPhase, metadataPublishedBy, acknowledgementPhase,
                 readerCursor, readerResult>>

(*******************************************************************************
  CrashBroker and RestartBroker are process-local events. A crash does not erase
  durable lease or reservation state. Restart increments the local incarnation
  so later publication stages can distinguish an old process from a replacement.
*******************************************************************************)
CrashBroker(broker) ==
  /\ broker \in Brokers
  /\ brokerAlive[broker]
  /\ brokerAlive' = [brokerAlive EXCEPT ![broker] = FALSE]
  /\ previousHighWater' = highWater
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, brokerIncarnation,
                 batchPhase, reservedBy, reservedRange, acceptedLeaseTerm, blobPhase,
                 metadataPhase, metadataPublishedBy, acknowledgementPhase, readerCursor,
                 readerResult>>

RestartBroker(broker) ==
  /\ broker \in Brokers
  /\ ~brokerAlive[broker]
  /\ brokerIncarnation[broker] < MaxIncarnation
  /\ brokerAlive' = [brokerAlive EXCEPT ![broker] = TRUE]
  /\ brokerIncarnation' = [brokerIncarnation EXCEPT ![broker] = @ + 1]
  /\ previousHighWater' = highWater
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, batchPhase, reservedBy,
                 reservedRange, acceptedLeaseTerm, blobPhase, metadataPhase, metadataPublishedBy,
                 acknowledgementPhase, readerCursor, readerResult>>

(*******************************************************************************
  ReleaseLease represents a graceful producer handoff. It can only be performed
  by the current valid holder. It is optional for correctness because expiration
  also permits a successor to acquire the lease.
*******************************************************************************)
ReleaseLease(broker) ==
  /\ broker \in Brokers
  /\ brokerAlive[broker]
  /\ ValidLease(broker)
  /\ leaseHolder' = Null
  /\ leaseExpiresAt' = now
  /\ previousHighWater' = highWater
  /\ previousReaderCursor' = readerCursor
  /\ UNCHANGED <<now, leaseTerm, highWater, brokerAlive, brokerIncarnation, batchPhase,
                 reservedBy, reservedRange, acceptedLeaseTerm, blobPhase, metadataPhase,
                 metadataPublishedBy, acknowledgementPhase, readerCursor, readerResult>>

(*******************************************************************************
  BoundedScenarioIsComplete is a test-model condition, not a blob-stream
  protocol condition. This Stage 3 configuration treats MaxTime as its intended
  horizon. Reaching that horizon permits the scenario to remain idle, while a
  state that gets stuck before it remains a TLC-reported deadlock.
*******************************************************************************)
BoundedScenarioIsComplete == now = MaxTime

(*******************************************************************************
  Quiescent is the only explicit stuttering action in Next. It leaves every
  model variable unchanged, but is enabled only once the bounded scenario has
  reached its configured horizon. This prevents an unconditional stutter from
  masking accidental deadlocks in earlier states.
*******************************************************************************)
Quiescent ==
  /\ BoundedScenarioIsComplete
  /\ UNCHANGED vars

(*******************************************************************************
  Next is the nondeterministic choice of one enabled action. The \/ operator is
  logical OR, and \E means "there exists". TLC explores every broker/batch choice
  satisfying an action's guards, rather than executing these branches in order.
*******************************************************************************)
Next ==
  \/ \E broker \in Brokers : AcquireOrRenewLease(broker)
  \/ \E broker \in Brokers : \E batch \in Batches : ReserveRange(broker, batch)
  \/ \E broker \in Brokers : \E batch \in Batches : AcceptBatch(broker, batch)
  \/ \E broker \in Brokers : \E batch \in Batches : UploadBlob(broker, batch)
  \/ \E broker \in Brokers : \E batch \in Batches : PublishMetadata(broker, batch)
  \/ \E broker \in Brokers : \E batch \in Batches : AcknowledgeProducer(broker, batch)
  \/ \E batch \in Batches : DeliverPublishedBatch(batch)
  \/ \E batch \in Batches : SkipCoveredBatch(batch)
  \/ AdvanceTime
  \/ \E broker \in Brokers : CrashBroker(broker)
  \/ \E broker \in Brokers : RestartBroker(broker)
  \/ \E broker \in Brokers : ReleaseLease(broker)
  \/ Quiescent

(*******************************************************************************
  The final Next alternative is a guarded quiescent stutter step. It is enabled
  only after the model reaches its configured time horizon. This tells TLC that
  intentional completion is legal while preserving deadlock detection for a
  state that becomes stuck before that horizon.

  Spec means: begin in Init, then repeatedly take a Next step or stutter. The
  [] operator means "always" and the underscore allows a harmless step that
  changes none of vars. Stuttering is part of standard TLA+ behavior.
*******************************************************************************)
Spec == Init /\ [][Next]_vars

(*******************************************************************************
  Invariants are predicates that must hold in every state TLC reaches. A passing
  invariant check is exhaustive only for the finite constants in the .cfg file;
  it is strong evidence about the abstraction, not a proof of all production
  executions or all possible configuration sizes.
*******************************************************************************)
TypeOK ==
  /\ now \in 0 .. MaxTime
  /\ leaseHolder \in Brokers \cup {Null}
  /\ leaseExpiresAt \in Nat
  /\ leaseTerm \in 0 .. MaxLeaseTerm
  /\ highWater \in 0 .. MaxSequence
  /\ previousHighWater \in 0 .. MaxSequence
  /\ brokerAlive \in [Brokers -> BOOLEAN]
  /\ brokerIncarnation \in [Brokers -> (0 .. MaxIncarnation)]
  /\ batchPhase \in [Batches -> {"New", "Reserved", "Accepted"}]
  /\ reservedBy \in [Batches -> (Brokers \cup {Null})]
  /\ reservedRange \in [Batches -> (SequenceRange \cup {Null})]
  /\ acceptedLeaseTerm \in [Batches -> (Nat \cup {Null})]
  /\ blobPhase \in [Batches -> {"NotUploaded", "Uploaded"}]
  /\ metadataPhase \in [Batches -> {"NotPublished", "Published"}]
  /\ metadataPublishedBy \in [Batches -> (Brokers \cup {Null})]
  /\ acknowledgementPhase \in [Batches -> {"NotAcknowledged", "Acknowledged"}]
  /\ readerCursor \in 0 .. MaxSequence
  /\ previousReaderCursor \in 0 .. MaxSequence
  /\ readerResult \in [Batches -> {"Unseen", "Delivered", "Skipped"}]

(*******************************************************************************
  LeaseFencing states the key producer-side rule: every Reserved or Accepted
  batch has a recorded reserver and range. The action guards establish that this
  reservation was possible only while its broker held the valid durable lease.
*******************************************************************************)
LeaseFencing ==
  \A batch \in Batches :
    batchPhase[batch] \in {"Reserved", "Accepted"} =>
      /\ reservedBy[batch] \in Brokers
      /\ reservedRange[batch] \in SequenceRange

(*******************************************************************************
  HighWaterNeverRegresses compares the current state with a copy of the previous
  state's high-water. All actions either preserve highWater or advance it, so a
  future accidental decrement produces a TLC invariant violation immediately.
*******************************************************************************)
HighWaterNeverRegresses == highWater >= previousHighWater

(*******************************************************************************
  ReservationsDoNotOverlap allows gaps but rejects reused sequence values. It is
  intentionally phrased over durable reserved ranges, which is stronger than
  checking only accepted batches.
*******************************************************************************)
ReservationsDoNotOverlap ==
  \A left \in Batches :
    \A right \in Batches :
      left # right /\ reservedRange[left] # Null /\ reservedRange[right] # Null =>
        RangeValues(reservedRange[left]) \cap RangeValues(reservedRange[right]) = {}

(*******************************************************************************
  AcceptedBatchesWereReserved captures the causal boundary for the next stage:
  no batch can be accepted until it has a durable sequence allocation, and the
  batch records the nonzero producer lease term that authorized acceptance.
*******************************************************************************)
AcceptedBatchesWereReserved ==
  \A batch \in Batches :
    batchPhase[batch] = "Accepted" =>
      /\ reservedRange[batch] \in SequenceRange
      /\ acceptedLeaseTerm[batch] \in Nat \ {0}

(*******************************************************************************
  BlobBeforeMetadata captures the first production ordering edge: a metadata
  row cannot name a segment until the segment blob is durable. It is independent
  of whether the producer reply has been sent.
*******************************************************************************)
BlobBeforeMetadata ==
  \A batch \in Batches :
    metadataPhase[batch] = "Published" =>
      blobPhase[batch] = "Uploaded"

(*******************************************************************************
  MetadataBeforeAcknowledgement captures the second production ordering edge.
  A successful producer acknowledgement means the metadata row already exists.
*******************************************************************************)
MetadataBeforeAcknowledgement ==
  \A batch \in Batches :
    acknowledgementPhase[batch] = "Acknowledged" =>
      metadataPhase[batch] = "Published"

(*******************************************************************************
  PublishedMetadataHasAcceptanceProvenance makes the publisher and original
  accepted range explicit. This does not require the publisher to still own the
  lease: that missing publication fence is intentionally modeled for the later
  stale-writer witness.
*******************************************************************************)
PublishedMetadataHasAcceptanceProvenance ==
  \A batch \in Batches :
    metadataPhase[batch] = "Published" =>
      /\ batchPhase[batch] = "Accepted"
      /\ metadataPublishedBy[batch] = reservedBy[batch]
      /\ acceptedLeaseTerm[batch] \in Nat \ {0}

(*******************************************************************************
  ReaderCursorNeverRegresses is the reader counterpart to high-water safety.
  The history variable records the cursor before every real transition, so this
  invariant detects a future action that accidentally moves the cursor backward.
*******************************************************************************)
ReaderCursorNeverRegresses == readerCursor >= previousReaderCursor

(*******************************************************************************
  DeliveredBatchesWerePublished says the reader cannot fabricate a batch from a
  metadata result: delivery requires a published metadata row and durable blob.
*******************************************************************************)
DeliveredBatchesWerePublished ==
  \A batch \in Batches :
    readerResult[batch] = "Delivered" =>
      /\ metadataPhase[batch] = "Published"
      /\ blobPhase[batch] = "Uploaded"
      /\ reservedRange[batch] \in SequenceRange

(*******************************************************************************
  SkippedBatchesAreCovered captures normal cursor-based replay filtering. A
  batch is skipped only after the reader's monotonic cursor covers its complete
  sequence range.
*******************************************************************************)
SkippedBatchesAreCovered ==
  \A batch \in Batches :
    readerResult[batch] = "Skipped" =>
      /\ metadataPhase[batch] = "Published"
      /\ reservedRange[batch] \in SequenceRange
      /\ reservedRange[batch][2] <= readerCursor

=============================================================================
