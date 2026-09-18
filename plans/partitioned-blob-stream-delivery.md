# Partitioned Blob Stream Delivery Plan

## Goal

Allow one Blob Stream consumer-group member to deliver records from multiple assigned virtual
partitions concurrently. The consumer must retain one coordinator for group membership, leases,
assignment, cursor staging, commits, seeks, and shutdown.

This is a delivery-parallelism change only. It must not change the producer protocol, metadata
window model, visibility behavior, recovery algorithm, durable cursor format, or cache/flush
behavior in loop-api.

## Problem Statement

`ConsumerIteratorImpl` is `Send + Sync`, but its public `ConsumerIterator::next(&mut self)`
requires exclusive access. It drains a single `DeliveryState` containing one global batch queue
and one current batch. The `bd-kafka` adapter preserves that constraint through its borrowed
`KafkaIterator::next(&mut self)` result.

Blob Stream already has useful read-side parallelism:

- `PrefetchWorker` runs independently of application delivery.
- `ConsumerReaderImpl` groups batch range reads and bounds concurrent fetch/decode work with
  `max_in_flight_batch_reads` (default: 32).
- The prefetch byte limit is shared and bounded by `prefetch_max_bytes` (default: 64 MiB).

The first experiment should therefore remove only the serialized application-delivery boundary.
It should preserve the single `PrefetchWorker` and `ConsumerReaderImpl` until telemetry proves
that shared reader planning is the next bottleneck.

## Selected Design

Introduce an exclusive partitioned-consumption mode in `bd-kafka`, implemented by Blob Stream.
The mode has two planes:

1. A coordinator/control plane delivers partition assignment and revocation events.
2. A data plane returns one independently pollable handle for each assigned virtual partition.

Each handle yields owned messages. This avoids extending the lifetime of the existing borrowed
`PartitionItem` across a spawned task. Handles do not own a lease and must not expose commit,
seek, or revocation-complete methods.

The mode conversion consumes the iterator, so one source cannot be used through both serial
`next()` and partition handles at once.

### Proposed Generic API

Add a capability adjacent to `KafkaIterator` in `internal-core/bd-kafka/src/iterator.rs`.
Names are illustrative and should be finalized with the implementation.

```rust
pub struct OwnedPartitionItem {
  pub partition: i32,
  pub offset: i64,
  pub payload: Bytes,
  pub seek_metadata: SeekMetadata,
}

#[async_trait]
pub trait PartitionConsumer: Send + Sync {
  fn partition(&self) -> i32;
  async fn next(&mut self) -> anyhow::Result<Option<OwnedPartitionItem>>;
}

pub enum PartitionConsumerEvent {
  Assigned(Box<dyn PartitionConsumer>),
  Revoked(Box<dyn RevokedWrapper>),
}

#[async_trait]
pub trait PartitionedKafkaIterator: Send + Sync {
  async fn next_event(&mut self) -> anyhow::Result<PartitionConsumerEvent>;
  fn store_offset(&mut self, partition: i32, offset: i64) -> anyhow::Result<()>;
  async fn commit(&mut self) -> anyhow::Result<()>;
  async fn seek(
    &mut self,
    partition: i32,
    target: SeekTarget,
    timeout: Duration,
  ) -> anyhow::Result<()>;
  async fn shutdown(self: Box<Self>) -> anyhow::Result<()>;
}

#[async_trait]
pub trait KafkaIterator: Send + Sync {
  // Existing serial methods remain unchanged.

  async fn into_partitioned(
    self: Box<Self>,
  ) -> anyhow::Result<Box<dyn PartitionedKafkaIterator>> {
    anyhow::bail!("partitioned consumption is unsupported by this iterator")
  }
}
```

`OwnedPartitionItem` should use the existing `Bytes` dependency rather than copying payloads.
The Blob Stream adapter continues to encode versioned source-checkpoint metadata exactly as its
serial `next()` path does today.

### Blob Stream API

Add equivalent types in `blob-stream-consumer/src/iterator/api.rs` and re-export them from
`iterator/mod.rs`:

- `PartitionedConsumerIterator`, owned by a single group coordinator.
- `PartitionConsumerHandle`, bound to one `VirtualPartitionId` and assignment generation.
- `PartitionConsumerEvent::{Assigned, Revoked}`.
- An owned Blob Stream record type containing `VirtualPartitionId`, offset, source checkpoint,
  and `Record`.

`ConsumerIteratorImpl` should gain a consuming conversion into the partitioned implementation.
The existing `ConsumerIterator` API remains unchanged and is the default for existing callers.

## Required Invariants

### Delivery

- A handle only delivers records for its own virtual partition.
- Records are FIFO by offset within one partition.
- A stalled or slow handle must not prevent a ready record from another active partition from
  being delivered.
- Cancelling a pending handle `next()` must not consume or discard a record.
- A closed handle returns `Ok(None)` and cannot become live again. A later assignment creates a
  new handle, even for the same partition ID.
- The total queued/current/pending payload bytes across all slots remain subject to the current
  shared soft prefetch budget. This change must not create a per-partition multiplicative memory
  budget.

### Offsets And Seeks

- The coordinator remains the only component permitted to call `store_offset`, `commit`, or
  `seek`.
- `store_offset` retains current validation against delivered source ranges and monotonically
  stages the partition's durable source checkpoint.
- `seek` fences and clears unread data only for its target partition, then uses the existing
  prefetch-worker command boundary before delivery resumes.
- The `BlobStreamIterator` adapter retains exact duplicate store-offset suppression per
  partition. Lower or higher offsets continue to reach Blob Stream for invariant validation.

### Assignment And Revocation

- A handle is published only after cursor hydration and reader assignment have succeeded.
- Added partitions may publish handles without disrupting existing handles.
- A revocation fences and wakes only the revoked handles, discards their queued/current batches,
  and prevents them from emitting another record.
- The existing one group-level `RevokedPartitions` wrapper remains the sole completion token.
  The owner of the control plane must drain in-flight application work for all listed partitions
  and call `complete()` once.
- No replacement handles become visible until that completion has been observed and the
  coordinator releases the old leases and applies the replacement reader assignment.
- Commits during the revocation drain retain current behavior: final staged cursors for revoked
  partitions may still be committed before completion.

### Concurrency

- No state lock may be held across metadata, blob, channel, notification, or task awaits.
- The shared reader remains exclusively owned by one `PrefetchWorker`.
- The partition-handle change must not create concurrent `ConsumerReaderImpl::read_available`
  calls.
- Shared delivery accounting and `ActivePartitionState` mutations use short critical sections.
- The data plane must not invent independently leased consumers or consumer groups.

## Implementation Steps

### 1. Add Generic Capability Types

Files:

- `internal-core/bd-kafka/src/iterator.rs`
- `internal-core/bd-kafka/src/iterator_blob_stream.rs`
- `internal-core/bd-kafka/src/iterator_blob_stream_test.rs`
- `internal-core/bd-kafka/src/test.rs`

Add owned partition message, partition handle, control event, and partitioned iterator traits.
Provide a default unsupported conversion on `KafkaIterator`. `PartitionIterator` (librdkafka) and
test mocks can keep the default until an explicit non-Blob Stream implementation is justified.

Implement the conversion for `BlobStreamIterator`. Move or share the existing payload metrics,
message-age update, source-checkpoint metadata encoding, duplicate offset suppression, and seek
metadata validation so serial and partitioned paths cannot diverge.

### 2. Make Delivery State Partition Local

Files:

- `blob-stream-consumer/src/iterator/delivery.rs`
- `blob-stream-consumer/src/iterator/shared.rs`
- `blob-stream-consumer/src/iterator/facade.rs`

Replace global `DeliveryState.batches` and `DeliveryState.current_batch` with partition-local
slots, keyed by `VirtualPartitionId`. A slot contains the queue, current batch, retained-byte
accounting, active/fenced state, and a per-slot notification mechanism.

Keep aggregate accounting in `DeliveryState` so existing prefetch gauges and the shared byte
budget remain accurate. Adapt delivery-gap detection and delivered-source-range tracking to run
for the slot's partition under the existing `ActivePartitionState` map.

The serial facade should either keep a small compatibility scheduler across active slots or remain
backed by a parallel-safe any-ready delivery helper. This compatibility path must not change its
observable record/revocation semantics.

### 3. Introduce Blob Stream Partition Handles

Files:

- `blob-stream-consumer/src/iterator/api.rs`
- `blob-stream-consumer/src/iterator/mod.rs`
- `blob-stream-consumer/src/iterator/facade.rs`
- `blob-stream-consumer/src/iterator/shared.rs`

Create a handle with stable shared slot ownership. Its `next()` should:

1. Check whether the slot is active and has an available record under a short lock.
2. Return an owned record after recording per-partition delivery provenance.
3. Return `None` when the slot is closed/fenced.
4. Await only the slot's notification when empty and active, with notification setup that cannot
   lose a wakeup between the state check and the wait.

The partitioned coordinator owns driver startup, control-event delivery, `store_offset`, `commit`,
`seek`, and shutdown. It should not allow a caller to obtain a second handle for the same active
partition.

### 4. Preserve One Prefetch Reader

Files:

- `blob-stream-consumer/src/iterator/prefetch.rs`
- `blob-stream-consumer/src/iterator/driver/reader.rs`

Keep the current single prefetch task and command-safe mutation boundary. Change only admission:
completed `ConsumerBatch` values go to their active partition slot rather than one global queue.

Retain:

- One global pending queue when admission is blocked by the aggregate budget.
- Current `ReadCapacity` calculation across pending and all delivery slots.
- Current `max_in_flight_batch_reads` behavior inside `ConsumerReaderImpl`.
- Current assignment and seek command processing between reader scan calls.

Do not add per-partition budgets, one reader per partition, or one fetch task per handle.

### 5. Publish Control Events And Fence Handles

Files:

- `blob-stream-consumer/src/iterator/driver/mod.rs`
- `blob-stream-consumer/src/iterator/driver/assignment.rs`
- `blob-stream-consumer/src/iterator/driver/runtime.rs`
- `blob-stream-consumer/src/iterator/driver/reader.rs`

On successful assignment, create slots and enqueue `Assigned` events after the reader accepts the
assignment. On cooperative revocation or heartbeat fencing, mark affected slots closed, discard
their prefetched data, wake their waiters, and enqueue the existing group-level revocation event.

Maintain the current delivery fence until the caller completes the revocation wrapper. The driver
must still keep active partition state available long enough for final cursor commits during the
drain. Remove it only when lease release and replacement assignment reach their existing success
path.

### 6. Integrate Loop-API Merge Worker

Files:

- `loop-api/loop-api-insights/src/merger/worker.rs`
- `loop-api/loop-api-insights/src/merger/worker_test.rs`

Attempt the consuming partitioned conversion for the primary Blob Stream iterator and the
optional alternate iterator. When unavailable, retain the existing serial `KafkaIterator::next()`
loop for librdkafka and test sources.

For a partitioned source:

1. Receive `Assigned` events and spawn one short-lived poll task per partition handle.
2. Have each task forward owned items through a bounded ingress channel to the central merge
   worker loop.
3. Keep cache mutation, cache routing, partition offset state, successful-offset recording,
   rewind decisions, and full flush behavior centralized in that loop.
4. On a revocation event, stop intake for listed partitions, join only their poll tasks, run the
   existing parallel cache full flush, record/store/commit eligible offsets, remove the revoked
   partition state, then call `complete()`.
5. On shutdown, stop and join every poll task before the existing full flush and iterator
   shutdown sequence.

The bounded ingress channel is mandatory. A task must wait for capacity rather than retaining an
unbounded stream of owned records. Choose its initial capacity conservatively and instrument it;
avoid tying it implicitly to the cache count without a measured reason.

Keep primary and alternate source state separate. The current alternate-discard behavior still
stages its offset in the central receiver without parsing or writing the payload.

## Tests

### Blob Stream Unit Tests

Extend `blob-stream-consumer/src/iterator/iterator_test.rs` with deterministic tests using
existing lifecycle hooks, manual clocks, blocking stores, semaphores, or notifications. Do not
make a test pass through wall-clock delay.

- Two handles can wait concurrently; releasing partition B first delivers B while A remains
  blocked.
- Each handle delivers monotonically increasing offsets for its own partition.
- Cancelling one pending handle poll preserves a ready record for its next poll.
- Aggregate buffered/current/pending bytes remain accurate across multiple slots, and one full
  slot/partition cannot permit aggregate prefetch budget overrun.
- A target-partition seek discards only that slot's unread data and resumes after the target
  source checkpoint.
- Revocation closes and wakes a blocked target handle, discards its buffered records, leaves an
  unrelated handle deliverable, and prevents replacement-handle publication before group-level
  completion.
- A staged cursor from a revoked partition can be committed during the drain before `complete()`.
- Shutdown closes all handles and leaves no pending worker task or stale delivery state.
- Serial `ConsumerIterator::next()` keeps existing behavior over the slot-based implementation.

### Adapter Tests

Extend `internal-core/bd-kafka/src/iterator_blob_stream_test.rs` with fakes that exercise:

- Assigned-event translation and virtual partition `u32` to Kafka `i32` validation.
- Owned payload and source-checkpoint metadata round trips without payload copies.
- Duplicate offset suppression in partitioned coordinator mode.
- Revocation wrapper forwarding and exactly one completion path.
- Unsupported conversion for the existing librdkafka `PartitionIterator`.

### Merge Worker Tests

Extend `loop-api/loop-api-insights/src/merger/worker_test.rs` to verify:

- Two assigned partition poll tasks can feed the central worker concurrently.
- Bounded ingress backpressure stalls a poll task without dropping/reordering its partition.
- Cache routing and centralized offset tracking stay correct with interleaved partition arrivals.
- Revoking one partition waits for only that partition's task while another partition remains
  runnable until the source's group-level revocation contract requires a broader pause.
- Full flush, offset storage, commit, and completion preserve current failure/rewind behavior.
- Alternate-discard sources retain their offset behavior.
- Unsupported/legacy iterators take the existing serial path.

## Observability And Rollout

Before enabling the merge worker integration broadly, add or derive the following measurements:

- Active partition-handle count and handle closure/fencing count.
- Handle wait duration and records delivered per partition.
- Merge ingress queue depth, saturation time, and active poll-task count.
- Time from Blob Stream delivery to central cache admission.
- Existing iterator records/sec, bytes/sec, aggregate prefetch bytes, and blob range-read
  concurrency.
- Existing merge cache size, flush latency, ClickHouse failures, and rewind activity.

Run the old serial path and the partitioned path in staging under comparable partition count and
traffic. The change is successful only if iterator lag and/or delivery-to-cache latency improve
without increased memory pressure, duplicate loss, failed commits, or revocations that exceed the
lease timing budget.

## Deferred Reader Parallelism

Do not implement reader striping as part of this plan. `ConsumerReaderImpl::read_available_impl`
currently treats a scan pass as a transaction: it clones and restores the complete partition-state
and Fast-frontier maps on failure, combines partitions into shared metadata-window requests, and
uses shared capacity. Splitting it into independent reader workers requires a separate design for:

- Partition-stripe ownership and reassignment.
- Shared metadata-window query coalescing without duplicated scans.
- Global byte and in-flight-read limits.
- Per-partition cursor/frontier transactions and recovery ordering.
- Seek/revocation cancellation at scan boundaries.
- Error aggregation and rollback semantics.

Revisit that design only if the partitioned delivery telemetry shows the prefetch reader, rather
than serial application delivery or merge cache admission, remains the limiting stage.

## Validation Commands

Run from the monorepo root after implementation:

```sh
just rustfmt \
  internal-core/bd-kafka/src/iterator.rs \
  internal-core/bd-kafka/src/iterator_blob_stream.rs \
  internal-core/bd-kafka/src/iterator_blob_stream_test.rs \
  blob-stream/blob-stream-consumer/src/iterator/api.rs \
  blob-stream/blob-stream-consumer/src/iterator/delivery.rs \
  blob-stream/blob-stream-consumer/src/iterator/shared.rs \
  blob-stream/blob-stream-consumer/src/iterator/facade.rs \
  blob-stream/blob-stream-consumer/src/iterator/prefetch.rs \
  blob-stream/blob-stream-consumer/src/iterator/driver/mod.rs \
  blob-stream/blob-stream-consumer/src/iterator/driver/assignment.rs \
  blob-stream/blob-stream-consumer/src/iterator/driver/reader.rs \
  loop-api/loop-api-insights/src/merger/worker.rs \
  loop-api/loop-api-insights/src/merger/worker_test.rs

./bazelw test //internal-core/bd-kafka:test
./bazelw test //blob-stream/blob-stream-consumer:test
./bazelw test //loop-api/loop-api-insights:test

./bazelw test --config=clippy \
  //internal-core/bd-kafka/... \
  //blob-stream/blob-stream-consumer/... \
  //loop-api/loop-api-insights/...

git diff --check
```

Use the normal Bazel Nextest wrappers for focused and full tests. For new service-backed Blob
Stream integration tests, use `--nocache_test_results`, named lifecycle boundaries, and the
deterministic test guidance in `blob-stream/plans/TEST_AUDIT.md`.
