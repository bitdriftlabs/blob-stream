# blob-stream System Design

`blob-stream` is a brokered streaming system for high-volume telemetry workloads. It prioritizes
throughput and storage cost over ultra-low latency. Producers write opaque records through a
broker, brokers persist compressed segment blobs and metadata indexes, and consumer groups read
directly from blob storage and metadata storage.

This document describes the current implementation and its behavioral contracts. See
[README.md](README.md) for deployment, configuration, storage provisioning, RBAC, and
observability. See [DEVELOPMENT.md](DEVELOPMENT.md) for contributor workflows.

## Goals and Boundaries

- Brokered writes aggregate records before durable storage.
- Payloads are opaque. The system does not validate an application schema.
- Delivery is at least once. Producer retries, interrupted consumer commits, and rebalances can
  produce duplicates. Consumers must tolerate or deduplicate them as required by their domain.
- Records have no global ordering and no cross-partition ordering guarantee. Sequence numbers
  provide monotonic progress only within a virtual partition.
- Consumers use cursors, not timestamp seeks. A new consumer group starts at its current aligned
  metadata window; a resumed group recovers from its committed metadata source through retention.
- There is no broker read path, compaction, built-in authorization, or idempotent producer
  protocol.
- DynamoDB TTL is written by the service. S3 lifecycle expiration is configured by operators and
  must remain aligned with the chosen retention period.

## Core Model

### Topics and Partitions

A topic has a fixed `partition_count` and `num_writers`. A producer hashes each record key to a
logical partition:

```
logical_partition_id = hash(record_key) % partition_count
```

Each producer deployment has a static `writer_id` in the range `[0, num_writers)`. It maps a
logical partition to a virtual partition:

```
virtual_partition_id = logical_partition_id + (writer_id * partition_count)
```

Virtual partitions let independent writer deployments write the same topic without sharing a
single producer identity. A consumer handles all virtual partitions; grouping them by
`virtual_partition_id % partition_count` reconstructs the logical-partition fan-in. The producer
validates `writer_id` against each configured topic when it starts. `writer_id` is not sent over
the wire; the broker receives only the resulting virtual partition ID and verifies that it is in
the topic's configured range.

A broker deployment also has an explicit `writer_id`. A deployment represents one local
writer/AZ isolation domain and discovers only brokers from that same domain. Its writer ID must
be within every configured topic's writer range. Other writer IDs/AZs are intentionally outside
local discovery, routing, and admin-state scope; they are not failover targets.

### Routing and Broker Ownership

Producers and brokers share the same deterministic local assignment plan. For the configured
writer ID, the planner considers only that writer's virtual partitions and the locally discovered
broker pool. It processes partitions in canonical order, selects from the currently least-loaded
brokers, and uses rendezvous score as a deterministic tie-breaker over:

```
(topic, virtual_partition_id, node_id)
```

For $P$ local writer-scoped partitions and $N$ live local brokers, each broker receives either
$floor(P / N)$ or $ceil(P / N)$ partitions. Membership comes from either a static list or
Kubernetes Endpoints. Brokers watch local membership and self-assign the partitions planned for
their own node ID. A stale producer membership view can send a request to a previous local owner;
that broker returns `NOT_LEASE_HOLDER`, and the producer retries with exponential backoff using
its current membership view. An empty local membership has no producer route; it never falls back
to a different writer/AZ.

Each broker fences writes through a producer-partition lease. A lease key contains topic, the
virtual partition ID. The virtual partition calculation already includes writer identity, so the
broker writer ID is not duplicated in the key. Only the active lease holder can reserve sequences
and accept writes for that lease key.

Broker admin state reports its configured writer ID, local membership as `{node_id, address}`,
and one ownership row per local writer-scoped virtual partition. Each row distinguishes the
deterministic planned owner from the observed lease holder. Producer state reports the same local
membership shape and one route per logical partition for its configured writer ID. Neither state
surface claims knowledge of another writer/AZ. State snapshots use RFC 3339 UTC strings for
absolute timestamps; elapsed durations remain numeric milliseconds.

### Sequence Numbers and Cursors

The broker assigns an inclusive sequence range to each accepted producer batch. Sequence numbers
are monotonic within a virtual partition; they have no meaning across virtual partitions. The
range `[S, E]` uses $S$ for the first (start) sequence number and $E$ for the last (end) sequence
number, with both endpoints included. If a batch contains $R$ records, then $E = S + R - 1$: its
first record has sequence $S$, its last record has sequence $E$, and each intervening record has
the next sequence number. The batch metadata persists that range once; the records themselves do
not each carry a separate durable sequence-allocation record.

Sequence allocation uses a Hi-Lo allocator to avoid a metadata-store operation for every record
or every producer batch. The durable producer-partition lease stores a high-water mark. While it
holds that lease, a broker atomically advances the high-water mark by the configured reservation
size and receives the resulting inclusive block, for example `[10,000, 10,999]`. This durable
block is the "high" portion. The broker keeps the next unused value and the block end in memory,
which is the "low" portion, and hands out contiguous subranges to accepted batches until the
block is exhausted. A batch with 250 records can therefore consume `[10,000, 10,249]` entirely
from memory; only the next exhausted-block refill requires another lease-store update.

Broker metrics expose aggregate refill behavior without topic or partition labels:
`sequence_reservations_total` counts successful durable block refills,
`sequence_reservation_records_total` counts values included in those blocks,
`sequence_reservation_failures_total` counts failed or fenced refill attempts, and
`sequence_reservation_latency_seconds` measures lease-store refill latency. For steady-state
traffic, the ratio of reservation rate to record rate should be close to one divided by the
reservation size. A materially higher ratio indicates allocation waste from ownership churn,
restarts, or unexpectedly small batches.

The broker may combine several accepted producer batches from the same virtual partition into
one flushed segment, but it preserves a distinct sequence range and metadata index entry for
each original batch. Consumers use those batch ranges to skip data already covered by a cursor,
then expose records in range order. A crash or ownership transfer can leave unused values from a
reserved block, creating gaps, but a new holder reserves only above the durable high-water mark
and therefore cannot reuse a successfully reserved value.

The broker serializes lease/refill/allocation transitions and durable flush plans for each virtual
partition within one live broker. Later accepted batches buffer while an earlier plan uploads its
blob and writes metadata, and therefore cannot become visible first from that broker. During a
graceful membership handoff or orderly shutdown, the broker stops accepting new batches for the
partition, drains its already accepted work, and only then voluntarily releases the producer
lease. Flushes for different virtual partitions remain concurrent.

This is not a durable cross-process publication fence. A broker crash, long pause, network
partition, or stale process resuming after its lease expires can still result in a former holder
attempting its unconditional metadata write after a successor has published higher sequences. A
consumer cursor advances past a later `seq_end` and cannot recover an earlier range that appears
afterward, so this remains a known limitation until metadata publication is transactionally
conditioned on a unique producer lease session or epoch.

A consumer cursor is the greatest processed `seq_end` for one virtual partition. During scans,
the consumer skips a batch when `batch.seq_end <= cursor` and advances its cursor only forward.
This makes normal re-scans and late metadata discovery safe under the monotonic sequence
invariant.

## Write Path

1. A producer buffers records independently for each `(topic, virtual_partition_id)`.
2. It flushes a producer batch when record count, payload-byte, or time thresholds are reached.
   Producer dispatches batches concurrently up to its configured request limit and does not
   preserve producer submission order, including within one virtual partition. The broker assigns
   sequence ranges in the order that it accepts requests and preserves that durable order.
3. It routes `ProduceBatch(topic, virtual_partition_id, records)` to the locally balanced broker
  selected for its configured writer ID.
4. The broker validates the topic, validates the virtual partition range, acquires or renews the
   producer-partition lease, and reserves sequence space as necessary.
5. The broker buffers accepted batches in memory. It flushes a virtual-partition buffer when its
   raw payload bytes reach `flush_max_bytes` or its oldest batch reaches `flush_max_delay_ms`. A
   partition with a durable plan in progress continues buffering its next epoch until that prior
   plan completes. A single bounded flush scheduler wakes for eligible writes, timer ticks, and
   durable-plan completions; a completion immediately promotes an eligible successor epoch.
6. A flush serializes each batch as `StoredRecordBatch`, compresses each serialized batch
   independently, concatenates the stored bytes into a segment blob, uploads the blob, and then
   writes the segment metadata row. Plans may run concurrently for different virtual partitions,
   but each virtual partition persists plans in sequence order.
7. Only after both blob upload and metadata write succeed does the broker complete the waiting
   write and return `OK`. A producer acknowledgement therefore represents durable segment
   metadata, not merely in-memory buffering.

The gRPC response has only `status` and `error_message`; it does not return sequence ranges.
Sequence ranges are internal durable metadata used by consumers. The protocol statuses are:

- `OK`: Blob and segment metadata were persisted for the accepted batch.
- `NOT_LEASE_HOLDER`: The broker cannot currently accept that virtual partition.
- `UNKNOWN_TOPIC`: The topic is absent from broker configuration.
- `OVERLOADED`: The broker rejected the request for an invalid partition, empty batch, exhausted
  sequence reservation, or another internal write-path failure.

The producer treats `NOT_LEASE_HOLDER`, `OVERLOADED`, and transport errors as retryable until its
configured retry budget is exhausted. Retrying after an ambiguous failure can produce a duplicate
batch, which is part of the at-least-once contract.

## Persistent Data Layout

### Segment Blobs

Segments are stored through the blob-store abstraction, backed by S3 in production and an
in-memory implementation in tests. A key has this form:

```
<optional-prefix>/<topic>/<window_start_unix_seconds>/<snowflake_id>.<zst|bin>
```

The extension records whether stored batches use zstd or no compression. A segment is not
compressed as one monolithic payload. It is a concatenation of individually serialized and
individually compressed `StoredRecordBatch` values, allowing consumers to fetch only the byte
range for a required batch.

### Segment Metadata

`blob_segments` stores one metadata row per segment. Its DynamoDB partition key is:

```
pk = "<topic>#<window_start_unix_seconds>"
```

Its sort key is a fixed-width, lexicographically sortable snowflake ID. The row includes the
blob key, segment compression, aggregate record and event-time statistics, creation time,
metadata publication time, and a per-virtual-partition index. Each index entry identifies a byte
range, sequence range, batch summary, and compression setting. Publication time is captured after
the blob upload and immediately before the metadata write.

Metadata is written only after the blob upload succeeds. Segment metadata TTL is derived from the
topic retention setting plus the configured DynamoDB TTL buffer. S3 lifecycle expiration is not
managed by the service; operators must configure it so blobs remain available at least as long as
the metadata that references them.

### Lease and Membership Domains

The production metadata configuration names four DynamoDB tables:

| Domain | Key | Responsibility |
| --- | --- | --- |
| `blob_segments` | topic-window and snowflake ID | Segment index used by consumers. |
| `producer_partition_leases` | topic and virtual partition | Broker write fencing and Hi-Lo sequence reservation. |
| `consumer_group_leases` | topic-group and virtual partition | Consumer ownership, generation fencing, and committed cursor. |
| `consumer_group_membership` | topic-group and member ID | Consumer member liveness used for dynamic rebalancing. |

Lease and membership rows receive TTL values based on their expiration plus the configured lease
TTL buffer. DynamoDB TTL cleanup is asynchronous, so expiry checks in the store also use the
stored lease timestamp.

## Read Path

Consumers read DynamoDB metadata and blob storage directly. The broker is not in the read path.
For every `read_available()` call, a consumer plans work independently for each owned partition:

1. A fresh group scans only its current aligned metadata window.
2. A resumed partition reads its committed cursor and source checkpoint from the lease. It scans
  full metadata windows from that source through a fixed current-window cutover, clamped to the
  configured finite topic retention. Legacy rows without a source checkpoint fall back to their
  committed timestamp; if that timestamp is absent, recovery starts at the retention floor.
3. Recovery proceeds in chronological bounded slices. A recovering partition does not use the
  live fast path until it reaches its captured cutover. Recovery scans do not use the checkpoint
  snowflake as a DynamoDB lower bound, so late lower-snowflake metadata remains discoverable.
4. Fast partitions scan a trailing horizon derived from the topic's enforced metadata-publication
  deadline plus `metadata_visibility_delay_ms`. Each aggregate window query starts at the lowest
  established inclusive frontier across assigned partitions; frontiers remain per partition and
  window because snowflake ordering is not shared across concurrently flushed partitions.
5. The reader sorts segments by snowflake ID, filters their indexes to eligible assigned
  partitions, sorts batches by `seq_start`, skips ranges covered by the in-memory cursor, and
  decodes only the required blob byte ranges.

Every committed cursor includes the source window and snowflake of the record batch that produced
it. The iterator carries that provenance from decoded batches through `store_offset()` to the
lease heartbeat, so a replacement owner can resume the correct finite-retention recovery range.

The reader advances an in-memory `(virtual_partition_id, window)` snowflake frontier only after
the source is visibility-eligible and its batches are successfully processed. It retains the bound
inclusively, so the boundary row is replayed and removed by cursor deduplication. Assignment loss
clears only the affected partition's frontiers, which are pruned once their windows leave the
derived horizon.

Consumer metrics distinguish fast and recovery query volume. `metadata_recovery_scan_hits`
counts successful recovery passes that emit one or more new batches, and
`metadata_recovery_scan_batches_read` counts those emitted batches. These counters exclude
recovery results discarded by cursor deduplication; `metadata_recovery_scan_segments` instead
measures the raw recovery scan volume.

The iterator adds a background prefetch buffer with a configurable soft byte budget. It pauses
prefetch after crossing the budget and resumes when callers drain buffered records. A partition
revocation removes buffered data for that partition before the new assignment becomes active.

### Delayed Metadata Bound

delays, retries, outages, and clock skew.
guarantee. DynamoDB does not provide a maximum convergence delay for eventually consistent
The fast path considers metadata only after the configured visibility delay. Its candidate horizon
covers the broker's metadata-publication deadline plus that delay, so a metadata row that meets the
publication contract remains in a scanned window when it becomes eligible. Event timestamps do not
affect metadata selection; the bound is based on segment windows and snowflakes.

The default visibility delay is two seconds. It is a best-effort staleness margin, not a DynamoDB
replication-delay guarantee: eventually consistent reads have no bounded convergence time. A row
that becomes visible after its derived candidate window leaves the horizon is not automatically
rediscovered by the fast path; a resumed consumer instead performs retention-bounded recovery.

### Read Consistency and Delivery Tradeoffs

The implementation uses eventually consistent DynamoDB metadata queries. A broker returns `OK`
only after it uploads the segment blob and writes its metadata row, but an eventually consistent
reader replica may not observe that row immediately. `metadata_visibility_delay_ms` can defer
accepting metadata whose publication timestamp is too recent. It defaults to two seconds and is a
best-effort staleness margin, not a correctness guarantee: DynamoDB supplies no bounded
replication-delay contract, and the delay does not solve stale-writer publication.

The reader's cursor filtering relies on metadata becoming visible in compatible sequence order.
Graceful broker handoff drains locally accepted work before lease release, but the expiry/stale
writer limitation described above remains. Applications must tolerate duplicates, and deployments
that require a stronger at-least-once contract across all writer failures need a transactional
producer publication fence in addition to any reader-consistency choice.

Future reader modes may offer the following cost/correctness tradeoffs:

- **Strongly consistent metadata queries:** Querying every metadata page with DynamoDB strong
  consistency removes read-replica staleness at approximately twice the metadata-query RRU
  component. It does not by itself prevent a stale former producer from publishing metadata after
  lease expiry, so it must be paired with a publication fence for a complete ordering guarantee.
Use `cost_analysis.py` with real page sizes, poll rates, consumer counts, and regional pricing
before selecting strong reads: they approximately double metadata scan RRUs, while shorter
recovery intervals or delayed-visibility horizons add rescans.

## Consumer Group Coordination

Consumer instances with the same topic and group ID register membership liveness. They use the
observed active-member list to calculate a deterministic cooperative sticky assignment of virtual
partitions. Existing placements are preserved where possible, then the minimum required moves
balance overloaded and underloaded members.

An assignment is only intent. A consumer becomes an active owner after the lease store grants its
lease for the current generation. The generation increments when the desired assignment changes;
heartbeat and commit operations include the generation so stale owners cannot renew or advance the
cursor after replacement. Consumers include their latest cursors in lease heartbeats to avoid a
separate steady-state commit write. On shutdown they release owned leases best-effort to shorten
handoff time.

During rebalance, an iterator stops delivering revoked partitions, discards their prefetched
records, invokes the configured revocation callback, and waits for callback completion before
activating the replacement assignment. A replacement owner hydrates its reader from the committed
cursor, source checkpoint, and legacy commit timestamp. It recovers through the fixed cutover
within retention before enabling that partition's bounded fast path.

## Correctness and Failure Behavior

The design relies on these invariants:

- A broker must hold the producer-partition lease before it allocates sequences or accepts a
  batch for that lease key.
- Broker assignment is balanced only within one local writer/AZ domain; a deployment never
  assigns or routes another writer ID's virtual partitions.
- Hi-Lo reservations never overlap for a lease key. Unused values may create gaps.
- Blob upload precedes segment metadata persistence. Producer acknowledgement follows both.
- Durable metadata visibility follows sequence order within a virtual partition, even when flush
  plans for other virtual partitions run concurrently.
- Valid later batches for a virtual partition have `seq_end` greater than already processed
  batches, so cursors never regress.
- A consumer lease generation fences stale ownership and stale cursor commits.
- Resumed-partition recovery scans find late lower-snowflake metadata throughout the topic
  retention horizon; fast-path rediscovery covers the derived publication and visibility horizon.

These invariants prevent a consumer from treating an already committed cursor as unprocessed
work, but they do not provide exactly-once delivery. Applications needing exactly-once effects
must use idempotent sinks, application record IDs, or another deduplication strategy.

## Configuration Defaults

The protobuf configuration schema is the authoritative definition of runtime settings. Important
defaults are:

| Setting | Default |
| --- | --- |
| Broker flush bytes | 64 MiB raw buffered payload per virtual partition |
| Broker flush delay | 1 second |
| Broker sequence reservation | 10,000 sequence values per virtual partition |
| Broker segment compression | zstd, level 3 |
| Producer batch records | 1,000 |
| Producer batch payload bytes | 1 MiB |
| Producer flush delay | 200 ms |
| Producer retries | 5 |
| Consumer metadata window | 300 seconds |
| Consumer lookback | 2 windows |
| Consumer metadata recovery scan | 60 seconds |
| Consumer metadata fast scan | enabled |
| Consumer prefetch target | 64 MiB |
| Consumer lease duration | 30 seconds |
| Consumer heartbeat and rebalance intervals | 10 seconds |

## Implementation Map

- Protocol and runtime configuration:
  [blob-stream-proto/proto/blobstream/v1](blob-stream-proto/proto/blobstream/v1)
- Partition helpers, sequence ranges, cursors, windows, and snowflake IDs:
  [blob-stream-types/src/lib.rs](blob-stream-types/src/lib.rs)
- Shared broker membership and rendezvous owner selection:
  [blob-stream-broker-discovery/src/lib.rs](blob-stream-broker-discovery/src/lib.rs)
- Broker write engine, lease self-assignment, and segment flush:
  [blob-stream-broker/src/write](blob-stream-broker/src/write)
- S3/in-memory blob stores and DynamoDB/in-memory metadata and lease stores:
  [blob-stream-blob-store](blob-stream-blob-store) and
  [blob-stream-metadata-store](blob-stream-metadata-store)
- Producer batching, routing, retries, and acknowledgement:
  [blob-stream-producer/src/producer.rs](blob-stream-producer/src/producer.rs)
- Consumer scans, coordination, iterator behavior, and prefetch:
  [blob-stream-consumer/src](blob-stream-consumer/src)
- End-to-end and deterministic fault-injection coverage:
  [blob-stream-integration-tests](blob-stream-integration-tests)
