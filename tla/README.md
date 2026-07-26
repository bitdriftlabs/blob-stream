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
   acknowledgement, and logical time.

It does **not** yet model reader observation, producer batching, compression,
S3 range reads, consumer coordination, partition assignment, retries, or
multiple partitions. Each omitted area can be added later only if it changes an
invariant we want to check.

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
run the bounded Stage 1 model with:

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

## What Comes Next

The next refinement adds a minimal reader cursor. It will first make the
stale-writer publication limitation concrete: a former holder publishes a lower
range after the reader already advances through a higher range.

After that, the model will add the Fast reader's bounded metadata-observation
horizon and deliberately reproduce the two accepted loss conditions from the
design:

1. A broker can publish metadata for already accepted work after its producer
   lease has expired and a successor has published higher sequences.
2. A Fast reader can observe later metadata while an eventually-consistent
   metadata scan omits an earlier row, then cursor/frontier advancement prevents
   recovery of that earlier row.

Those paths will use separate TLC witness configurations. Each witness will
first pass residual invariants, then intentionally fail one named no-loss
invariant so TLC prints a trace. Ordinary configurations will continue to check
residual guarantees and reject any permanent loss that is not explicitly
classified as one of these two accepted causes.

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
