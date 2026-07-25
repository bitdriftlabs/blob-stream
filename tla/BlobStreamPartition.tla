--------------------------- MODULE BlobStreamPartition ---------------------------
EXTENDS Naturals, TLC

(*******************************************************************************
  BlobStreamPartition is a deliberately small model of one blob-stream virtual
  partition. It is a learning model, not a line-for-line implementation of the
  Rust services.

  Stage 1 models the producer-side safety boundary:

    acquire producer lease -> reserve a Hi-Lo block -> accept one batch

  It leaves out blob/metadata publication and readers for now. Those stages
  will be added only after TLC exhaustively checks this smaller state machine.

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
  acceptedLeaseTerm

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
  acceptedLeaseTerm
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
  /\ UNCHANGED <<now, highWater, previousHighWater, brokerAlive, brokerIncarnation, batchPhase,
                 reservedBy, reservedRange, acceptedLeaseTerm>>

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
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, brokerAlive, brokerIncarnation,
                 acceptedLeaseTerm>>

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
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, previousHighWater,
                 brokerAlive, brokerIncarnation, reservedBy, reservedRange>>

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
  /\ UNCHANGED <<leaseHolder, leaseExpiresAt, leaseTerm, highWater, brokerAlive,
                 brokerIncarnation, batchPhase, reservedBy, reservedRange, acceptedLeaseTerm>>

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
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, brokerIncarnation,
                 batchPhase, reservedBy, reservedRange, acceptedLeaseTerm>>

RestartBroker(broker) ==
  /\ broker \in Brokers
  /\ ~brokerAlive[broker]
  /\ brokerIncarnation[broker] < MaxIncarnation
  /\ brokerAlive' = [brokerAlive EXCEPT ![broker] = TRUE]
  /\ brokerIncarnation' = [brokerIncarnation EXCEPT ![broker] = @ + 1]
  /\ previousHighWater' = highWater
  /\ UNCHANGED <<now, leaseHolder, leaseExpiresAt, leaseTerm, highWater, batchPhase, reservedBy,
                 reservedRange, acceptedLeaseTerm>>

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
  /\ UNCHANGED <<now, leaseTerm, highWater, brokerAlive, brokerIncarnation, batchPhase,
                 reservedBy, reservedRange, acceptedLeaseTerm>>

(*******************************************************************************
  BoundedScenarioIsComplete is a test-model condition, not a blob-stream
  protocol condition. This Stage 1 configuration treats MaxTime as its intended
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

=============================================================================
