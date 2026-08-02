(Human note to reader: Everything in this directory was generated and explored with 5.6 Terra. I
have no experience with TLA+ and am using this as a learning exercise. I make no claims anything in
here is correct and appreciate help and feedback.)

# TLA+ Model: Blob-Stream Virtual Partition

This directory contains a small, commented TLA+ model of blob-stream's
correctness-critical state transitions. It is intended to be read while learning
TLA+, not merely run in CI.

The first model is deliberately narrower than the service:

- One topic and one virtual partition.
- Up to two broker identities.
- A small number of symbolic batches.
- Producer lease acquisition, Hi-Lo reservation, batch acceptance, crash,
   restart, release, blob persistence, metadata publication, producer
   acknowledgement, a monotonic reader cursor, and logical time.

It does **not** yet model eventually consistent metadata observation, Fast
frontiers, producer batching, compression, S3 range reads, consumer
coordination, partition assignment, retries, or multiple partitions. Each
omitted area can be added later only if it changes an invariant we want to
check.

The configuration also bounds logical time, lease terms, and process
incarnations. Those bounds are not production limits. They make the state space
finite, allowing TLC to check every behavior in this teaching-sized model. A
model variable that can grow forever, even a diagnostic counter, prevents a
finite exhaustive run.

The bounded model can also reach its configured time horizon. `Next` therefore
includes a guarded `Quiescent` action that stutters only at that horizon. This
says that a deliberately completed bounded scenario may remain in the same
state, while TLC still reports a model that gets stuck unexpectedly before the
horizon.

## Install And Run

Install the TLA+ Toolbox, which includes the TLC model checker:

```sh
brew install --cask tla+-toolbox
```

The Toolbox is also a useful editor and trace viewer when learning. The
Makefile automatically finds its checker JAR in the standard macOS location, so
run the bounded Stage 3 model with:

```sh
cd tla
make check
```

If the Toolbox is installed elsewhere, override the auto-detected JAR:

```sh
make check TLA_TOOLS_JAR="/absolute/path/to/tla2tools.jar"
```

TLC should report that it checked every reachable state for the small constants
in [BlobStreamPartition.cfg](BlobStreamPartition.cfg). A passing result means
the listed invariants held for this abstract model and those bounds. It does not
prove the Rust implementation correct or cover production-scale values.

## TLA+ Vocabulary

TLA+ describes a system as a **state machine**. A state is one complete snapshot
of variables such as `leaseHolder` and `highWater`. An **action** describes one
allowed transition between an old state and a new state. TLC starts in `Init` and
tries every enabled action repeatedly.

| Term | TLA+ meaning | Blob-stream example |
| --- | --- | --- |
| Constant | Fixed for one model-checker run. | The two configured broker identities and `Null = NoValue`. |
| Variable | Changes from one state to the next. | `highWater` after a Hi-Lo reservation. |
| Action | Predicate over old and new state. | `ReserveRange(BrokerA, BatchA)`. |
| Prime (`x'`) | The value of `x` in the next state. | `highWater' = highWater + 1`. |
| Invariant | Predicate that must be true in every reachable state. | Reserved sequence ranges do not overlap. |
| Specification | Initial state plus permitted next steps. | `Spec == Init /\ [][Next]_vars`. |
| Counterexample | A TLC-produced path to an invariant violation. | A trace showing two ranges overlap. |

TLA+ actions are not an imperative program. `Next` uses logical OR to say that
any enabled action may happen next. `\E broker \in Brokers` means TLC explores
the action for every possible broker, rather than executing a loop in a chosen
order.

## Stage 1: Lease And Hi-Lo Safety

[BlobStreamPartition.tla](BlobStreamPartition.tla) models the same abstract
contract as the producer lease store and broker allocation path:

- A live broker can acquire or renew the durable producer lease.
- Only a broker holding an unexpired lease can reserve a sequence block.
- A reservation advances a durable high-water mark.
- A broker can accept work only from a range it reserved while it held the lease.
- A crash does not erase durable state; restart changes only process-local state.
- Sequence gaps are legal. Sequence reuse is not.

## Stage 2: Producer Publication Ordering

Stage 2 extends each accepted batch through the broker's durable flush order:

```text
Accepted -> blob uploaded -> metadata published -> producer acknowledged
```

This reflects the order in
[blob-stream-broker/src/write/flush.rs](../blob-stream-broker/src/write/flush.rs):
the broker stores the segment blob, writes the metadata row naming that blob,
then completes the successful flush that permits a producer response.

The model uses four separate per-batch state values instead of replacing
`Accepted` with one larger phase enum. Keeping acceptance and each publication
step separate makes their distinct causal roles visible in an invariant and will
let the next reader stage observe metadata before a producer acknowledgement.

`PublishMetadata` intentionally does **not** require `ValidLease(broker)`. The
broker did hold a valid lease when it accepted the batch, but the production
design does not transactionally fence the later metadata write. An alive former
holder may therefore publish its already uploaded batch after its lease expires.
This is the precisely scoped behavior needed for the documented stale-writer
witness; it is not a claim that accepting new work without a lease is allowed.

## Stage 3: Minimal Cursor Reader

Stage 3 adds one reader with a monotonic `readerCursor`, the greatest sequence
end it has processed. This first reader abstraction assumes a complete view of
published metadata: whenever metadata has been published, the reader can select
it. That deliberately postpones DynamoDB replica staleness and Fast-frontier
logic to the next stage.

For each published batch, the reader records one result:

```text
Unseen -> Delivered  when seq_start is above readerCursor
Unseen -> Skipped    when seq_end is already covered by readerCursor
```

The `Delivered` transition moves the cursor to the batch's inclusive `seq_end`.
It permits gaps because a new producer lease can leave unused values in a Hi-Lo
reservation. When several rows are visible, delivery chooses the lowest unseen
sequence range first. `Skipped` models normal replay filtering: a metadata row
seen more than once must not be delivered again when its whole range is already
covered.

At this stage a skipped row is not automatically considered data loss. With a
complete metadata view, it normally represents a safe duplicate observation.
The stale-writer witness below constrains the order so an older row is first
published only after a newer row advanced the cursor; that is the accepted loss
case described in the design.

## Stale-Writer Loss Witness

[StaleWriterWitness.tla](StaleWriterWitness.tla) is a separate phase-gated
module that imports the general model and prescribes the documented stale-writer
trace. It adds `scenarioPhase` only to control the witness; it does not weaken
the production-model actions or use a TLC `CONSTRAINT` to prune transitions.

Run its two checks from this directory:

```sh
make check-stale-writer-safety
make witness-stale-writer
```

The first command passes 16 distinct states at depth 16. It checks every normal
Stage 1-3 invariant and `AllSkippedBatchesHaveKnownCause`, while deliberately
allowing the known loss predicate. The second command runs that safety check
first, then asks TLC to check the false invariant
`NoStaleWriterPublicationLoss`. The Make target succeeds only when TLC fails at
that exact invariant and prints its counterexample trace.

The trace is deliberately small:

1. Broker A acquires the lease, accepts and uploads lower batch A with `[1, 1]`.
2. Logical time reaches A's lease expiry; broker B acquires a new lease term,
   accepts and uploads higher batch B with `[2, 2]`.
3. B publishes B and the reader delivers it, advancing its cursor to `2`.
4. Still-alive former holder A publishes and acknowledges A despite no longer
   holding a valid lease.
5. The reader observes A and skips `[1, 1]` because its cursor already covers
   that range.

This is an accepted product limitation, not a passing no-loss claim. The
residual invariants remain true: the allocations do not overlap, each metadata
row has a blob, acknowledgements follow metadata, and the cursor never moves
backward. A transactional publication fence should intentionally make this
witness unreachable and turn the no-loss assertion into a normal passing check.

The model maps most directly to
[blob-stream-metadata-store/src/lib.rs](../blob-stream-metadata-store/src/lib.rs)
and [blob-stream-broker/src/write/engine.rs](../blob-stream-broker/src/write/engine.rs).

### Action Guide

| Action | Before | After | Production concept |
| --- | --- | --- | --- |
| `AcquireOrRenewLease` | Broker is live and owns the valid lease, or lease is absent/expired. | Lease owner and expiry are updated; a new acquisition increments the term. | Conditional producer lease mutation. |
| `ReserveRange` | Valid holder chooses a `New` batch. | Durable high-water advances and the batch owns the resulting range. | Atomic lease renewal/reservation. |
| `AcceptBatch` | Valid holder owns a reserved batch range. | Batch becomes accepted and records the authorizing lease term. | Broker accepts producer data. |
| `AdvanceTime` | Logical time is below `MaxTime`. | Time increases by one. | Lease expiry becoming observable. |
| `CrashBroker` | Broker is alive. | Broker stops initiating work; durable state remains. | Process crash or long pause. |
| `RestartBroker` | Broker is stopped. | Broker becomes alive with a new process incarnation. | Restart after crash. |
| `ReleaseLease` | Current valid holder releases gracefully. | Lease becomes unowned immediately. | Drained handoff/shutdown. |
| `UploadBlob` | Accepting broker is alive and its batch is not uploaded. | The batch's blob becomes durable. | Segment blob-store write. |
| `PublishMetadata` | Accepting broker is alive and its blob is uploaded. | Durable metadata is written with publisher provenance. | Metadata-store write; deliberately not publication-fenced. |
| `AcknowledgeProducer` | Publishing broker is alive and metadata exists. | Batch receives successful acknowledgement. | Successful flush permits the producer RPC response. |
| `DeliverPublishedBatch` | Published blob-backed range begins above the cursor. | Batch is delivered and cursor advances to its sequence end. | Reader decodes newly observed metadata. |
| `SkipCoveredBatch` | Published range is fully at or below the cursor. | Batch is marked skipped without changing the cursor. | Cursor filtering for duplicate metadata observation. |
| `Quiescent` | Logical time reached the bounded model horizon. | No model variable changes. | Completed test scenario, not a production operation. |

### Invariants

- `TypeOK`: every variable has the intended shape. It catches many modeling
  mistakes before domain-specific invariants become meaningful.
- `LeaseFencing`: a reserved or accepted batch has a broker and durable range.
- `HighWaterNeverRegresses`: a future action cannot silently decrease high-water.
- `ReservationsDoNotOverlap`: no two durable allocations reuse a sequence value.
- `AcceptedBatchesWereReserved`: accepted work has a durable range and lease term.
- `BlobBeforeMetadata`: published metadata always names a durable blob.
- `MetadataBeforeAcknowledgement`: an acknowledged batch already has metadata.
- `PublishedMetadataHasAcceptanceProvenance`: published metadata belongs to an
   accepted batch and retains its accepting broker and lease-term evidence.
- `ReaderCursorNeverRegresses`: a reader transition cannot move its cursor back.
- `DeliveredBatchesWerePublished`: a delivered batch has durable metadata and
   blob state; the reader cannot invent data.
- `SkippedBatchesAreCovered`: cursor filtering skips only a range fully covered
   by the current monotonic cursor.

## What Comes Next

The next refinement adds the Fast reader's bounded metadata-observation horizon
and reproduces the second accepted loss condition from the design:

1. A Fast reader can observe later metadata while an eventually-consistent
   metadata scan omits an earlier row, then cursor/frontier advancement prevents
   recovery of that earlier row.

That path will use the same paired configuration pattern: first pass residual
invariants, then intentionally fail one named no-loss invariant so TLC prints a
trace. Ordinary configurations will continue to check residual guarantees and
reject any permanent loss that is not explicitly classified as an accepted
cause.

## Deadlock And Stuttering

The temporal specification permits stuttering, which is a transition that does
not change any modeled variable. TLC separately reports a deadlock when none of
the actions in `Next` is enabled. The model uses a guarded `Quiescent` action
only when `now = MaxTime`, its deliberately configured test horizon. This keeps
intentional finite-scenario completion legal without hiding a state that gets
stuck unexpectedly before the horizon.

## Reading TLC Output

For a successful run, focus first on:

- the number of distinct states and the search depth;
- whether every invariant was checked;
- the elapsed time and memory use.

For an invariant failure, TLC prints a numbered state trace. Read it as a
timeline: each state is the result of one action from the previous state. Start
at the first changed variable, find the action whose comment describes that
change, and then decide whether the model action or the intended product rule is
wrong. Counterexamples are the primary learning tool here; a small incorrect
model is useful when TLC makes its mistake concrete.
