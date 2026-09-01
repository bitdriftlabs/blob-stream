# blob-stream System Design

`blob-stream` is a brokered streaming system for high-volume telemetry workloads. It prioritizes
throughput and storage cost over ultra-low latency. Producers write opaque records through a
broker, brokers persist compressed segment blobs and metadata indexes, and consumer groups read
through broker cache services with direct storage fallback.

This document describes the current implementation and its behavioral contracts. See the
[repository overview](../README.md), [Infrastructure setup](infrastructure.md), and
[Operations guide](operations.md) for deployment and runtime guidance. See
[DEVELOPMENT.md](../DEVELOPMENT.md) for contributor workflows.

## Goals and Boundaries

- Brokered writes aggregate records before durable storage.
- Payloads are opaque. The system does not validate an application schema.
- Delivery is at least once. Producer retries, interrupted consumer commits, and rebalances can
  produce duplicates. Consumers must tolerate or deduplicate them as required by their domain.
- Records have no global ordering and no cross-partition ordering guarantee. Sequence numbers
  provide monotonic progress only within a virtual partition.
- Consumers use cursors, not timestamp seeks. A new consumer group starts at its current aligned
  metadata window; a resumed group recovers from its committed metadata source through retention.
- Brokers serve bounded cached metadata-window reads and a bounded raw blob-range RPC. Blob-range
  requests are limited to 16 MiB, while responses may contain up to the effective
  `max_segment_bytes` setting of requested compressed bytes. The blob-range service inspects an
  object's advertised content length and admits its complete immutable body only when the Linux
  cgroup-aware memory monitor has enough headroom, before streaming that body from storage. It
  then returns exact requested byte slices. Consumers group the current pass's segment ranges by
  immutable blob key. A complete validated broker response supplies raw bytes to the existing
  consumer decoder; any unavailable, malformed, or retryable response reissues the whole group
  through direct blob storage. An authoritative broker `NOT_FOUND` follows the existing
  missing-batch path without a direct retry. There is no compaction, built-in authorization, or
  idempotent producer protocol.
- DynamoDB TTL is written by the service. S3 lifecycle expiration is configured by operators and
  must remain aligned with the chosen retention period.

## Core Model

### Topics and Partitions

A topic has a fixed `partition_count` and `num_writers`. A producer hashes each record key to a
logical partition:

```
logical_partition_id = hash(record_key) % partition_count
```

`hash(record_key)` is SipHash-1-3 with both 64-bit keys set to zero, a little-endian 64-bit
byte-slice length prefix, and the raw record-key bytes. This preserves the Rust 1.98
`DefaultHasher::new()` mapping used by existing 64-bit little-endian producer deployments while
making the partitioning contract independent of the target architecture. The implementation writes
those bytes explicitly rather than delegating to the standard-library `Hash` trait, so partition
assignment remains stable across Rust upgrades and deployment architectures.

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
that broker returns `NOT_LEASE_HOLDER`. Producers treat this differently from transport and
overload failures: they use a slower backoff and wait for either the next membership update or
lease convergence before retrying. A membership update receives a short settle delay because
Endpoints visibility can lead the new broker's lease acquisition. An empty local membership has no
producer route; it never falls back to a different writer/AZ.

The producer keeps gRPC clients keyed by broker address so normal batches reuse an HTTP/2
connection. Whenever discovery removes an address, it drops that address's cached client
reference immediately. In-flight requests retain their own reference and can finish normally;
the producer does not force-close an active connection.

Broker discovery distinguishes a pending initial watch value from an initialized membership
snapshot. A producer and a broker-collapsed consumer metadata client wait up to 10 seconds for an
initialized snapshot before creating routes; otherwise construction fails. A broker starts its
listener while discovery is pending so Kubernetes can mark the pod ready and include it in
Endpoints, but it performs no lease acquisition until an initialized membership contains its own
node ID. Every membership snapshot, including pending, empty, or foreign membership, replaces the
broker's local write-admission set. The broker accepts a virtual-partition write only when the
current deterministic plan contains that partition; rejection occurs before it creates local state,
acquires a lease, or reserves sequences. An initialized membership without the local node after
activation is an ownership loss, and an initialized empty membership owns no partitions and has no
producer route. This prevents a pod from temporarily claiming every partition while Kubernetes
publishes its initial Endpoint set.

Each broker fences writes through a producer-partition lease. A lease key contains topic, the
virtual partition ID. The virtual partition calculation already includes writer identity, so the
broker writer ID is not duplicated in the key. Only the active lease holder can reserve sequences
and accept writes for that lease key. Lease drains, releases, and acquisition maintenance run
independently from membership consumption. A later membership snapshot therefore updates write
admission immediately even while an earlier ownership loss drains accepted work or an earlier
lease acquisition is still in flight. If a newer snapshot restores local ownership of a partition
being released, that partition remains outside effective write admission until the old release
reaches a terminal outcome; assignment maintenance then reacquires it under the latest membership
plan. Release work is never cancelled because a conditional remote release may complete after its
caller stops waiting.

Broker configuration sets both the lease duration and the producer-lease heartbeat interval. The
heartbeat must be shorter than the lease duration; it renews every producer-partition lease owned
by the broker. A failed acquisition because another holder is still live retries after 250 ms with
exponential backoff capped at 2 seconds. Increasing both reduces DynamoDB renewal writes but
extends recovery after an ungraceful broker loss. During graceful membership changes and shutdown,
the broker first removes moved partitions from local write admission, drains already accepted work
while renewing the lease before its existing expiry, and then releases moved leases explicitly.
Reconciliation also drains and releases any locally tracked lease outside the current plan, covering
state left by an older inline-acquisition path rather than waiting for its TTL to expire. Lease
acquisition logs record whether acquisition was requested inline by `ProduceBatch` or by assignment
maintenance, plus the assignment-set generation that authorized it.

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
holds that lease, a broker atomically advances the high-water mark and receives the resulting
inclusive block, for example `[10,000, 10,999]`. This durable block is the "high" portion. The
broker keeps the next unused value and the block end in memory, which is the "low" portion, and
hands out contiguous subranges to accepted batches until the block is exhausted. A batch with 250
records can therefore consume `[10,000, 10,249]` entirely from memory.

`sequence_reservation_size` is the per-partition base reservation target rather than a permanent
block size. A local target starts at this base after ownership acquisition or process start. If a
foreground write exhausts its current range before the next lease-maintenance cycle, the broker
doubles that partition's target, saturating at the maximum configuration value. At lease
maintenance, the broker compares its remaining sequence capacity with the records allocated in
the previous cycle. If that consumption would exhaust the current range before the next cycle,
it reserves another range inline with the lease renewal and appends it to the local range. When
the previous cycle allocated at least 75% of the current target, the new range uses a doubled
target; otherwise it uses the current target. This gives the next cycle headroom for ordinary
traffic variation without requiring a foreground exhaustion to land before maintenance. This
normally makes a renewal plus proactive refill one conditional lease-store mutation rather than
two writes. The adaptive target is process-local and has no decay policy: low traffic simply
stops refills until the current range is consumed.

Broker metrics expose aggregate refill behavior without topic or partition labels:
`sequence_reservations_total` counts successful durable block refills,
`sequence_reservation_records_total` counts values included in those blocks,
`sequence_reservation_failures_total` counts unexpected durable refill failures, and
`sequence_reservation_latency_seconds` measures lease-store refill latency. For steady-state
traffic, the ratio of reservation rate to record rate should be close to one divided by the
reservation size. A materially higher ratio indicates allocation waste from ownership churn,
restarts, or unexpectedly small batches.

Write metrics distinguish attempts from definitive outcomes. `produce_requests_total` counts
write-engine attempts. `produce_records_total` and `produce_payload_bytes_total` count only
records and payload bytes whose batch completed with a durable `OK`. The corresponding
`produce_rejected_records_total` and `produce_rejected_payload_bytes_total` counters count
definitive write-engine errors. A gRPC timeout is intentionally excluded from both pairs because
the cancelled caller may have already handed the batch to a flush that later persists; operators
use `grpc:request_timeouts_total` for those ambiguous outcomes.

The broker coalesces all accepted producer batches for one virtual partition in a flush plan into
one stored batch and one metadata index entry. The stored sequence range spans the contiguous
accepted ranges, and records retain broker acceptance order. Consumers use that range to skip
data already covered by a cursor, then expose records in range order. A crash or ownership
transfer can leave unused values from a reserved block, creating gaps, but a new holder reserves
only above the durable high-water mark and therefore cannot reuse a successfully reserved value.

The broker serializes lease/refill/allocation transitions for each virtual partition within one live
broker. Later accepted batches buffer in a successor flush epoch while an earlier epoch persists.
The broker assigns segment identities in epoch order, then successive epochs may encode and upload
blobs concurrently, subject to a broker-wide upload bound. A plan starts metadata publication only
after every blob upload in that plan has reached a terminal result. The broker publishes metadata
for each partition in epoch order, so a later sequence range cannot become consumer-visible first.
During a graceful membership handoff or orderly shutdown, the broker stops accepting new batches for
the partition, drains its already accepted work, and only then voluntarily releases the producer
lease.

The optional `fenced_metadata_writes` mode supplies a durable cross-process publication fence. Each
write engine creates a unique process session ID; acquisition by a new or expired session increments
the durable lease epoch, while a live renewal by the same session preserves it. Metadata publication
uses one DynamoDB transaction containing the metadata `Put` and a condition check for every
partition's holder ID, epoch, session ID, and unexpired lease. A former process cannot publish after
a successor takes the lease, even if both processes use the same node ID.

The mode defaults off. Producer lease rows always carry a holder ID, lease epoch, and session ID;
the DynamoDB lease decoder rejects rows without this durable fence identity. A transaction has room
for one metadata write and at most 99 lease checks, so the scheduler splits larger flushes. A lost
fence after blob upload can leave an orphaned blob, but the transaction does not publish metadata and
the producer receives a retryable failure. When the mode is disabled, the stale-writer limitation
remains.

A consumer cursor is the greatest processed `seq_end` for one virtual partition. During scans,
the consumer skips a batch when `batch.seq_end <= cursor` and advances its cursor only forward.
This makes normal re-scans and late metadata discovery safe under the monotonic sequence
invariant.

## Write Path

1. A producer buffers records independently for each `(topic, virtual_partition_id)`.
2. A producer seals all currently buffered partition batches on the `flush_max_delay_ms` cadence or
  when a batch reaches its record-count or payload-byte threshold. It samples live batch limits
  when admitting a `produce` call and samples a live flush delay only when scheduling its next
  cadence interval; existing buffers and an already scheduled interval retain their prior values.
  New records enter a subsequent generation. The producer maintains equally sized dispatch-task
  and request permit pools. It acquires both permits before removing one byte-bounded broker group
  from sealed state; unadmitted work remains sealed until both are available. The dispatch-task
  permit remains held through terminal waiter notification, bounding retained group work. The
  initial RPC and each retry attempt hold a request permit only while calling the broker, releasing
  it before response handling or backoff. Retryable logical batches in one admitted group run as
  concurrent futures within that bounded dispatch task, and each independently acquires request
  capacity while sharing the original retry deadline. Membership updates refresh the cached route
  map but do not force an early seal. This does not preserve producer submission order, including
  within one virtual partition. The broker starts all logical batches in a grouped request
  concurrently and returns their results in request order. It assigns sequence ranges in the order
  that it accepts requests and preserves that durable order.
3. `ProduceBatches` returns one ordered result per submitted partition batch. The producer
   resolves successful entries independently and retries only entries that were rejected or whose
   request outcome is ambiguous. Each `ProduceBatches` message has a 16 MiB decoded protobuf cap;
   the broker also permits the fixed five-byte gRPC envelope. It retains the legacy `ProduceBatch`
   RPC only for a staged broker-first deployment; new producers use `ProduceBatches` exclusively.
4. The broker validates each topic and virtual partition, acquires or renews the
   producer-partition lease, and reserves sequence space as necessary.
5. The broker samples jemalloc allocation against its Linux cgroup memory limit and rejects new
   batches with `OVERLOADED` when utilization exceeds its admission threshold. It flushes a
   virtual-partition buffer when its raw payload bytes reach `flush_max_bytes` or its oldest batch
   reaches `flush_max_delay_ms`. `blob_stream_broker_flush_max_bytes` and
   `blob_stream_broker_flush_max_delay_ms` are positive live integer overrides. The scheduler
   snapshots them when it starts its next timer cycle; an existing cycle and already selected plans
   retain their prior values. A time-due partition establishes a flush cadence for the local broker
   and includes every available buffered, non-draining virtual partition across its local topics in
   the same plan. This can flush younger peer buffers before their individual delay to produce
   larger blobs and fewer S3 PUTs. Byte-threshold flushes remain local unless a time-due shared
   flush pulls them forward, and lease-drain flushes remain partition-local. `max_segment_bytes`
   separately caps each serialized compressed object; it defaults to 64 MiB and can be changed live
   with `blob_stream_broker_max_segment_bytes`. A compressed partition batch that cannot be split
   is emitted alone when it exceeds that cap. A partition with a durable plan in progress continues
   buffering its next epoch until its normal byte or delay trigger. Successive epochs can then build
   and upload blobs concurrently, while their metadata publication remains ordered. A single bounded
   flush scheduler wakes for eligible writes, timer ticks, and durable-plan completions. It runs at
   most four durable flush plans concurrently; `write:active_flush_plans` reports its current
   occupancy.
6. A flush coalesces each virtual partition's accepted batches into one `StoredRecordBatch`,
  compresses each serialized partition batch independently, and concatenates the stored bytes into
  bounded segment objects. One time-triggered object can contain contiguous sections for several
  topics. The broker uploads each object once, then writes one ordinary topic-local metadata row
  for every section. A successful row acknowledges only its own topic's partitions; a failed row
  remains retryable and can produce an at-least-once duplicate. With fenced metadata writes
  enabled, each row is a transaction conditioned on the snapshot lease fences for that row's topic.
  Plans may overlap for the same or different virtual partitions. Blob uploads may proceed in
  parallel, but each virtual partition publishes plan metadata in sequence order.
7. Only after both blob upload and metadata write succeed does the broker complete the waiting
   write and return `OK`. A producer acknowledgement therefore represents durable segment
   metadata, not merely in-memory buffering.

Segment IDs use Sonyflake's default machine-ID provider in production. The local in-process test
cluster supplies stable, distinct machine IDs because every broker runs on the same host; this is
test-harness wiring only and does not change production identity selection.

The gRPC response has only `status` and `error_message`; it does not return sequence ranges.
Sequence ranges are internal durable metadata used by consumers. The protocol statuses are:

- `OK`: Blob and segment metadata were persisted for the accepted batch.
- `NOT_LEASE_HOLDER`: The broker cannot currently accept that virtual partition.
- `UNKNOWN_TOPIC`: The topic is absent from broker configuration.
- `BAD_REQUEST`: The batch is invalid, such as one containing no records.
- `OVERLOADED`: The broker rejected the request for an invalid partition, exhausted sequence
  reservation, or another transient internal write-path failure.

The producer treats `NOT_LEASE_HOLDER`, `OVERLOADED`, and transport errors as retryable until its
total retry deadline is exhausted. `UNKNOWN_TOPIC` and `BAD_REQUEST` are terminal. Transport and
overload failures use capped exponential backoff; `NOT_LEASE_HOLDER` uses a slower,
membership-aware backoff. Each RPC and delay is clipped to the remaining deadline. Retrying after
an ambiguous failure can produce a duplicate batch, which is part of the at-least-once contract.

## Persistent Data Layout

### Segment Blobs

Segments are stored through the blob-store abstraction, backed by S3 in production and an
in-memory implementation in tests.

All segment objects use a shared key:

```
<optional-prefix>/shared/<window_start_unix_seconds>/<snowflake_id>.<zst|bin>
```

Objects can contain sections for topics with different retention periods. Configure the S3
lifecycle rule for the `shared/` prefix to retain objects for at least the maximum retention of all
topics in the deployment, plus the metadata TTL buffer.

The extension records whether stored batches use zstd or no compression. A segment is not
compressed as one monolithic payload. It is a concatenation of individually serialized and
individually compressed `StoredRecordBatch` values, allowing consumers to fetch only the byte
range for a required batch.

### Segment Metadata

`blob_segments` stores one metadata row per segment. Its DynamoDB partition key is:

```
pk = "<topic>#<window_start_unix_seconds>"
```

Its sort key is a fixed-width, lexicographically sortable snowflake ID. The top-level row stores
only `pk`, `sk`, the optional TTL, and `segment_metadata_v1`, a binary protobuf payload. That
payload contains the blob key, creation and publication times, segment-level compression, and a
per-virtual-partition index. Each flushed virtual partition has one index entry identifying its
byte range, sequence range, and stored payload size. Publication time is captured after the blob
upload and immediately before the metadata write.

The consumer requires every metadata row to use this durable layout. During the testing-only
cutover, missing, malformed, or legacy rows are skipped with a rate-limited warning rather than
being decoded through a compatibility path.

Metadata is written only after the blob upload succeeds. Rows can reference either key form during a
flag rollout; the broker-mediated range reader routes by the complete immutable key rather than
inferring its topic from the key shape. Segment metadata TTL is derived from the topic retention
setting plus the configured DynamoDB TTL buffer. S3 lifecycle expiration is not managed by the
service; operators must configure it so blobs remain available at least as long as the metadata that
references them.

### Lease and Membership Domains

The production metadata configuration names four DynamoDB tables:

| Domain | Key | Responsibility |
| --- | --- | --- |
| `blob_segments` | topic-window and snowflake ID | Segment index used by consumers. |
| `producer_partition_leases` | topic and virtual partition | Broker write fencing and Hi-Lo sequence reservation. |
| `consumer_group_leases` | topic-group and virtual partition | Consumer ownership, generation fencing, and committed cursor. |
| `consumer_group_membership` | topic-group and member ID | Consumer member liveness plus shared assignment-plan/planner records. |

Consumer-group lease rows retain their committed cursor only until their TTL, derived from lease
expiration plus the configured lease TTL buffer. Producer leases and consumer membership rows use
the same expiry-plus-buffer model. DynamoDB TTL cleanup is asynchronous, so expiry checks in the
store also use the stored lease timestamp.

The assignment plan and planner lease use the existing `consumer_group_membership` table under the
separate reserved partition key
`__blob_stream_assignment_control_v1__#<topic>#<group_id>`, with reserved sort keys
`__blob_stream_assignment_plan_v1__` and `__blob_stream_assignment_planner_v1__`. Member rows
remain under `<topic>#<group_id>`, so rolling deployments with legacy membership queries neither
interpret the planner lease as a member nor read the full plan item. This retains the four-table
deployment model.

## Read Path

Consumers range-read segment bytes directly from blob storage by default. Metadata reads use
DynamoDB directly by default; an enabled consumer may route a bounded metadata-window request to
its deterministically selected local broker. The broker uses a byte-bounded Moka cache for eventual
metadata, collapses concurrent requests, and falls back to its authoritative DynamoDB store on a
miss. Strong reads collapse only concurrent broker requests and are never retained after
completion. A broker failure, invalid response, unavailable membership, or disabled capability
causes the consumer to use its original direct DynamoDB scan.

The independently enabled raw blob path groups currently planned batch ranges by immutable blob
key and sends each group to its deterministic local broker. The broker caches a bounded complete
compressed object, shares one in-flight whole-object read for concurrent same-key requests, then
returns only the requested ranges. A request is limited to 16 MiB of serialized range data and a
response to the effective `max_segment_bytes` value of requested compressed bytes plus bounded
protobuf range metadata. This covers normal objects; a larger unsplittable singleton retains the
direct-storage fallback. Consumers retain decompression, validation, ordering, cursor progression,
and direct blob-storage authority. `NOT_FOUND` is authoritative for an immutable object and
preserves the existing missing-batch behavior; every other broker, protocol, admission, or timeout
failure retries every range in the group directly from blob storage. The broker cache is best-effort:
idle expiry or memory pressure removes retained objects without changing delivery semantics.

Tail and Full Recovery metadata use independent byte-weighted Moka caches with a five-minute idle
expiry. A retained entry is invalidated when it is stale or cannot cover the request. The broker
limits request partitions, cache-entry items, response items and bytes, waiters per key and
globally, and concurrent refills. Its coalescing delay is bounded to at most half the cache RPC
timeout, so an admission, size, refill, or timeout limit returns a typed overload instead of a
partial response; the consumer then repeats the original direct scan. Full Recovery remains
unpaged: each retained entry is one complete immutable snapshot, installed atomically after the
authoritative query succeeds. In this model a Full Recovery baseline and final seal occur together
when that snapshot is installed.

For broker-routed recovery, the checkpoint-overlap window uses Tail coverage with its inclusive
Sonyflake floor. Later unbounded recovery windows, along with Fresh windows, use Full Recovery
coverage for the required partitions; a Full Recovery request that overlaps Fast also includes the
Fast partitions, which retain their own frontier filtering after the response. The protocol has no
pagination: a response that exceeds its byte budget fails as a whole and the consumer repeats its
original direct scan, so no partial recovery response can advance a cursor.
The reader has two different progress markers for each virtual partition:

- The **cursor** is the greatest delivered-and-committed sequence end. It decides whether a batch
  is new: `batch.seq_end > cursor` is required for delivery.
- The **source checkpoint** is the `(metadata window, segment snowflake)` of the batch that
  produced that cursor. It tells a replacement owner where retained recovery begins and, when it
  remains within retention, supplies the first-window query floor. It is not a correctness or
  metadata-ordering watermark.

Each `read_available()` pass first plans work per owned partition, then merges that work into at
most one DynamoDB query for each metadata window. A merged query has no snowflake lower bound when
any Fresh or unbounded Recovering partition needs a full-window scan. Fast partitions sharing that
query still apply their own frontier filters after the result is returned.

### Reader Modes and Progress

1. **Fresh:** A partition with no durable cursor or source checkpoint scans only the current
  aligned window, with no snowflake lower bound. It becomes Fast only after that window is fully
  scanned without a visibility deferral or prefetch-capacity exhaustion. This is the intended
  start position for a new group, but it also applies to a partition that has never delivered a
  record for an existing group: without a delivered record, that partition has no durable
  recovery origin.
2. **Recovering:** A resumed partition starts at the checkpoint's window, clamped to the topic
  retention floor, and captures the current aligned window as its cutover. A legacy cursor without
  a checkpoint starts from its committed timestamp's window; if that is also absent, it starts at
  the retention floor. Recovery scans up to 32 consecutive windows per pass, from oldest to newest,
  and becomes Fast only after its cutover window is fully scanned. When the checkpoint window is
  the cutover window, recovery is a single-window pass.
3. **Fast:** A partition that has completed Fresh or Recovery scans only the bounded recent horizon
   where metadata can still be unpublished or invisible. Fast is an optimization; it is never used
   to replace retained recovery for a resumed partition.

For a usable source checkpoint that was not clamped to retention, recovery uses an inclusive lower
bound only for its first window. Let $D$ be the broker's publication deadline plus the consumer's
configured `max_clock_skew` bound and the reader's effective visibility delay. The lower bound is
the minimum Sonyflake at $max(checkpoint\_window\_start, checkpoint\_snowflake\_time - D)$,
evaluated with millisecond precision and converted to its inclusive 10 ms Sonyflake bucket. The
cursor remains the correctness watermark: the inclusive boundary row is replayed and skipped if its
sequence end is already committed. Every later recovery window is queried without a Sonyflake lower
bound.

This deliberately adopts Fast's bounded-availability tradeoff for replacement startup. A metadata
row older than the overlap that becomes visible only after the reader leaves the first source window
is not automatically rediscovered. Legacy checkpoints, retention-clamped checkpoints, and malformed
checkpoint IDs retain the conservative full-window behavior. Neither path uses the source checkpoint
as a correctness ordering certificate.

Explicit seek requires a caller-provided source location: the requested sequence offset, metadata
window, and an optional source segment identifier. The consumer discards buffered records, starts
recovery at that window, and uses the supplied segment identifier to derive the first-window overlap
bound. It rejects no source location because an offset alone cannot identify a bounded metadata
recovery origin. Each delivered record has an offset strictly greater than the requested offset,
including when the target falls within a batch.

Recovery cost is proportional to the gap from the durable source checkpoint to the captured
cutover, not to current partition activity. A sparse partition with an old committed cursor still
recovers from that cursor's source window through retention in chronological slices of up to 32
windows per pass. Historical passes select one Recovering partition in round-robin partition order,
so a dense historical partition cannot starve a later, independent recovery. When every recovering
partition needs only its active cutover window, they share that one query to avoid serializing a
normal group takeover. The first source window uses the bounded overlap; every later recovery
window is a full-window query. This intentionally finds retained records written while the group
was down, at the cost of scanning a long downtime interval. It is distinct from a cursorless
partition, which has no retained recovery origin and starts Fresh in its current window.

When a recovery-only window has ended before the publication and visibility safety horizon, its
successful metadata response is immutable for the reader's correctness model. The reader retains
that response locally until every batch in the window is cursor-covered or delivered, so draining
prefetch capacity or retrying a blob read never issues another metadata query for that window.
Visibility-deferred, Fresh, Fast, shared, and active-horizon responses are not cached. The cache is
intentionally reader-local and is discarded on restart, revocation, seek, or replacement recovery
hydration.

The effective visibility delay applies only to eventual reads. In the default eventual-read mode it
is `metadata_visibility_delay_ms`; when an eligible metadata row is newer than
`now - effective_visibility_delay`, the reader defers it. Strong reads accept every validated row
returned by storage. Clock skew is accounted for by the Fast and checkpoint availability horizon,
not added to the eventual-read per-row maturity test. In Recovery, deferring a window blocks later
recovery windows for that partition in the same pass, so the cursor cannot advance past a missing
earlier sequence range. The recovery pointer remains at the deferred window for the next pass when
that window is outside Fast's bounded scan horizon. A deferred window inside that horizon instead
completes recovery and is handed to Fast: Fast retains the same visibility check, blocks later
snowflakes in that partition/window during the pass, and retries from its inclusive time floor and
frontier. This prevents recovery from tail-chasing a busy active window while preserving the
historical recovery barrier that protects cursor order. A failed metadata or blob read, except a
classified missing blob, restores the pass's cursor and frontier state so an undelivered batch is
retried.

A classified blob `NotFound` is a retention or storage durability violation: metadata is published
only after its blob upload, and S3 retention must outlive the referencing metadata. The reader
counts the missing batch's records in `lost_records`, advances its in-memory cursor through the
batch's sequence range, and does not deliver its records. This lets later retained data continue
without treating timeouts, permissions, corruption, or other storage failures as loss. The reader
does not automatically commit this skip; the normal caller-controlled commit path remains
responsible for persisting later acknowledged progress. A restart before that commit can classify
the same missing range again rather than silently acknowledging previously delivered records.

Prefetch-capacity exhaustion is also a recovery barrier. The reader advances the recovery pointer
past only fully processed windows and leaves it at the earliest window containing a batch whose
capacity reservation failed. After callers drain prefetched records, the next pass resumes at that
window from its mature cached metadata when available, rather than querying either that window or
the completed prefix of the slice again. This preserves the same cursor ordering guarantee as
visibility deferral while avoiding repeated metadata scans during a long backlog.

After a successful pass that produces no batches solely because eligible metadata was deferred,
the prefetch worker waits until the earliest deferred row reaches its precise eligibility instant.
This replaces only the normal empty-result idle backoff for that pass; a pass that produces batches
continues immediately, and reader commands interrupt the wait so assignment, hydration, and seek
changes remain prompt.

### Fast Query Bounds

Let $D$ be the broker's enforced maximum metadata-publication lag plus the consumer's configured
`max_clock_skew` bound and the effective visibility delay. The Fast safe timestamp is
$T_{safe} = now - D$. The reader considers the current window plus enough preceding windows to
cover $D$, then omits every window whose end is at or before $T_{safe}$.

For a configured skew bound $S$, eventual reads use $D = 15s + S + 2s$ by default, and strong
reads use $D = 15s + S$. The unset `max_clock_skew` default is $S = 10ms$, yielding 17.010s and
15.010s respectively. This is Fast's metadata-query lower-bound horizon, not a wait of that
duration applied to every returned row: the publication deadline and clock-skew bound account for
metadata that might not yet be safely queryable, while the visibility delay separately decides
whether an existing returned row is old enough to use. A higher observed frontier can narrow an
individual steady-state query beyond the time floor, but it does not reduce the required horizon
before that frontier exists.

For each remaining window, the time floor is the smallest Sonyflake for
$max(window_start, T_{safe})$. A Fast partition's effective lower bound is the greater of this
time floor and its observed inclusive `(virtual_partition_id, window)` frontier. If several Fast
partitions share a window, the DynamoDB query uses the lowest effective bound, then each partition
filters rows below its own frontier. This protects a sparse partition whose last observed
snowflake is lower than another partition's. Frontiers are updated only after visibility-eligible
metadata is selected, are inclusive so the boundary row is safely replayed, and are cleared on
assignment loss or explicit seek and pruned when their window leaves the Fast horizon.

After the metadata query, the reader sorts segment rows by snowflake ID, filters each row to
eligible assigned partitions, and sorts batches within that row's partition index by `seq_start`.
It then skips batches covered by the cursor, reserves prefetch capacity, and plans blob reads. The
reader relies on the producer's per-partition durable publication order for ordering across segment
rows; local sorting does not create a global sequence merge.

Every decoded batch carries its source checkpoint. Delivery retains this provenance for each
delivered offset, so `store_offset()` can persist the checkpoint paired with the committed cursor
on the next heartbeat. A replacement owner can then start retained recovery from the correct
window.

Reader diagnostics expose the inputs and result of Fast planning separately: `fast_scan_bounds`
shows each Fast partition/window's time floor, observed frontier, effective partition bound, and
the merged query bound; `fast_frontiers` contains only real retained frontiers. Recovery metrics
are pass-level: if a pass includes Fresh or Recovering work, its query and emitted-batch counters
are classified as recovery even when the merged query also serves Fast work.

The iterator adds a background prefetch buffer with a configurable byte budget. It reserves batch
payload capacity before blob reads, retains decoded batches only within that budget, and resumes
prefetch when callers drain records. One oversized batch may proceed only when no payload is
otherwise retained. For every segment with selected batches, the reader fetches one range spanning
the lowest selected start offset through the highest selected end offset, then decodes only the
selected batch slices. This lowers S3 request count when an owner holds several partitions packed
into one segment, at the cost of downloading intervening unowned batches. Segment range reads and
decodes overlap up to `max_in_flight_batch_reads` (default 32), while buffered completion preserves
planned output order. Both limits can be overridden between scan passes through runtime feature
flags. A partition revocation removes buffered data for that partition before the new assignment
becomes active.

### Worked Read Scenarios

**Fresh assignment.** Assume topic `telemetry` uses 300-second windows and `now = 950`, so the
current window is `[900, 1200)`. Partition 7 has no committed cursor and is therefore Fresh. It
queries `pk = telemetry#900` with no snowflake lower bound. If the query returns an already
visibility-eligible segment containing sequence range `[1, 2]`, the reader delivers that range,
advances its in-memory cursor to 2, and enters Fast after it completes this one window.

**Same-window checkpoint recovery.** Assume topic `telemetry` uses 300-second windows, the broker
publication deadline is 15 seconds, and the consumer visibility delay is explicitly zero. At
`now = 1,050`, the current and cutover window is `[900, 1200)`. A replacement owner restores
partition 7 with cursor 10 and a usable source checkpoint at timestamp 1,020 in that same window.
Here $D = 15.010s$, so its one recovery query is `pk = telemetry#900` with the Sonyflake minimum
for timestamp $1,020 - 15.010 = 1,004.990$. The checkpoint row is replayed and skipped because
its sequence end is 10; a later row ending at 11 is delivered. Because the source and cutover are
the same window, recovery completes in one scan and the partition enters Fast. This is covered by
`recovery_scans_single_checkpoint_window_with_overlap_bound` and is the expected shape during a
consumer recovery or reassignment that overlaps a broker rolling restart.

**Later recovery window.** Assume topic `telemetry` uses 300-second windows, the default
15-second publication deadline, default `max_clock_skew = 10ms`, and default 2-second
visibility delay, making $D = 17.010s$. At
`now = 1,350`, the cutover window is `[1,200, 1,500)`. Partition 7 restores a usable source
checkpoint from timestamp 1,020 in the earlier `[900, 1,200)` window. Its source-window query is
`pk = telemetry#900` with a Sonyflake lower bound for $1,020 - 17.010 = 1,002.990$. Its later
cutover-window query is `pk = telemetry#1200` with no snowflake lower bound. Only the source window uses the
checkpoint overlap; every later recovery window remains a full-window query. Partition 7 enters
Fast only after it has fully scanned the `[1,200, 1,500)` cutover window in a recovery pass. If a
visibility deferral or prefetch-capacity limit blocks that window, it remains Recovering and retries
that window on a later pass.

**Sparse partition after downtime.** Assume topic `telemetry` has seven-day retention and
300-second windows. Partition 7 last delivered sequence 10 from window `W0`, then remains idle
while the consumer group is down for six days. Its replacement owner restores cursor 10 and the
source checkpoint in `W0`, then scans every retained window from `W0` through its captured current
cutover in chronological slices of at most 32 windows. This can require many scan passes, but it
discovers records written to partition 7 while the group was down. In contrast, a partition that
has never delivered a record has no durable cursor or source checkpoint. On reassignment it is
Fresh and scans only the new current window, just as a newly created group does; it has no durable
origin from which to recover earlier retained windows.

**Recovery waits for a visibility gap.** Assume topic `telemetry` uses 300-second windows and a
2-second visibility delay. At `now = 1,230`, a partition with cursor 1 performs legacy recovery
from window `[900, 1,200)` through its cutover window `[1,200, 1,500)`; legacy recovery has no
checkpoint lower bound. The `[2, 2]` row in the first window has
`metadata_published_ts_ms = 1,229,000`, so it is deferred because it is newer than
`1,230,000 - 2,000`. The `[3, 3]` row in the cutover window has
`metadata_published_ts_ms = 1,200,000` and is already visible. The pass delivers neither range
and retains cursor 1, because the deferred earlier window blocks recovery progress. At
`now = 1,232`, the `[2, 2]` row becomes eligible; the next pass delivers `[2, 2]` followed by
`[3, 3]`. This prevents cursor 3 from hiding sequence 2 and is covered by
`recovery_does_not_advance_cursor_past_visibility_deferred_window`.

**Active-window visibility handoff.** Assume a graceful replacement starts in the same 300-second
window as its committed source checkpoint. A newer row in that window is still inside the reader's
visibility delay. Recovery leaves the durable cursor at the checkpoint and enters Fast because the
deferred window is in Fast's bounded availability horizon. After the row becomes visible, Fast
rescans it from its inclusive lower bound and delivers it. This avoids waiting for the active window
to close while preserving the normal visibility and cursor protections; it is covered by
`consumer_restart_hands_active_window_visibility_deferral_to_fast`.

**Constantly producing Fast partition.** Assume one Fast partition in topic `telemetry` produces
segments continuously in the `[900, 1,200)` window. With the default 15-second publication
deadline, default `max_clock_skew = 10ms`, and 2-second visibility delay, at `now = 1,020`
$D = 17.010s$ and $T_{safe} = 1,002.990$.
Suppose an earlier scan already observed a visibility-eligible segment at Sonyflake timestamp
1,015, so its inclusive frontier is 1,015. The next query uses
$max(1,002.990, 1,015) = 1,015$: it replays the boundary row and does not rescan the interval from
1,002.990 through 1,014. If that query returns a later segment whose metadata was published at
1,019, Fast defers that segment at `now = 1,020` because it is newer than the 2-second visibility
cutoff of 1,018. Its frontier remains 1,015 and the segment becomes eligible at `now = 1,021`.
The 17.010-second floor still matters before a higher frontier exists: a segment with Sonyflake
timestamp 1,004 can be selected by that floor but, if its metadata was published at 1,019, is
separately deferred until 1,021. Thus 17.010 seconds controls how far back Fast queries; 2 seconds
controls whether a returned metadata row is accepted on that pass.

**Fast time floor and frontiers.** Assume topic `telemetry` uses 300-second windows, the default
15-second publication deadline, default `max_clock_skew = 10ms`, and default 2-second
visibility delay. At `now = 1,020`, the current window is `[900, 1,200)` and $D = 17.010s$, so
$T_{safe} = 1,002.990$. The preceding window
`[600, 900)` ended before $T_{safe}$ and is omitted. In the current window, partition 7 has an
inclusive frontier at Sonyflake timestamp 1,005, while partition 8 has an inclusive frontier at
timestamp 1,001. Their shared query uses the lower effective bound, timestamp 1,001; partition 7
filters rows below its own 1,005 frontier locally, while partition 8 can still receive rows at or
after 1,001.

**One range read for several selected batches.** Assume a selected segment from topic `telemetry`
contains three independently compressed batches: batches for owned partitions 7 and 9 occupy
`[0, 100)` and `[150, 240)`, while an unowned partition 8 batch occupies `[100, 150)`. Window
size, publication deadline, and visibility delay no longer affect this stage: metadata selection
has already chosen the two owned batches. The reader issues one blob range request for `[0, 240)`,
then decodes only `[0, 100)` and `[150, 240)`. The middle 50 bytes are intentional overfetch that
trades data transfer for one fewer object-store request.

### Publication and Visibility Bound

The broker starts `max_metadata_publication_lag_ms` before segment construction and requires both
blob upload and metadata persistence to finish within the remaining budget. The unset topic default
is 15 seconds. Consumers add their configured `max_clock_skew` bound (10 ms only when unset)
and their effective visibility delay to form $D$. Eventual reads use
`metadata_visibility_delay_ms` (two seconds by default); strong reads use zero delay even when
that setting is nonzero.

The Fast safe timestamp is $T_{safe} = now - D$; it omits older windows and uses the Sonyflake
minimum for $max(window\_start, T_{safe})$ in the remaining windows. Checkpoint recovery uses the
same $D$ only for its first source window as described above. Sonyflake timestamps have a 10 ms
quantum, so `minimum_for_timestamp` converts each millisecond-precision floor to its inclusive
bucket. This conversion does not add to $D$. Event timestamps do not affect metadata selection.

Fifteen seconds is a configurable operational deadline, not a DynamoDB replication-delay guarantee
or an empirically proven universal value. It is intentionally short enough to limit Fast and
checkpoint-recovery scan work, while still budgeting for segment construction, blob upload, and
metadata persistence. A deadline exhaustion fails the flush, so the setting is also a producer
availability limit, not only a reader-cost control.

Broker metrics expose whole-publication latency and whether the deadline expired before persistence
or while persisting. Operators should set a topic-specific deadline with headroom above sustained
high-percentile publication latency, include the visibility delay when evaluating reader work, and
investigate deadline exhaustion and flush failures rather than silently widening the scan horizon.

### Read Consistency and Delivery Tradeoffs

Metadata queries are eventually consistent by default. A broker returns `OK` only after it uploads
the segment blob and writes its metadata row, but an eventually consistent reader replica may not
observe that row immediately. `metadata_visibility_delay_ms` can defer accepting metadata whose
publication timestamp is too recent. It defaults to two seconds and is a best-effort staleness
margin, not a correctness guarantee: DynamoDB supplies no bounded replication-delay contract, and
the delay does not solve stale-writer publication.

Set `ConsumerReadConfig.strongly_consistent_metadata_reads` to enable DynamoDB `ConsistentRead` for
every metadata-query page. It defaults to false. The runtime feature flag
`blob_stream_consumer_strong_metadata_reads` overrides that setting, using the configuration value
as its fallback. The reader snapshots the resolved value at the beginning of each scan pass; a flag
change therefore affects the next pass without invalidating metadata caches, clearing Fast
frontiers, replaying cursors, or retroactively changing a hydrated recovery source-window overlap.
Operational flag changes are expected to be infrequent: a strong-to-eventual change after hydration
retains the shorter strong-mode overlap for that recovery. Strong mode ignores a configured nonzero
`metadata_visibility_delay_ms` and accepts a row at its publication timestamp. The broker
publication deadline and configured `max_clock_skew` remain part of Fast and checkpoint-recovery
overlap calculations in both modes.

The delay is needed even though Fast retains a per-partition observed snowflake frontier. That
frontier records only metadata returned by a prior query; it is not evidence that the query returned
every lower snowflake. For example, one partition can publish row `A` at snowflake 100 with sequence
range `[1, 1]`, then row `B` at snowflake 101 with range `[2, 2]`, including when both rows come
from the same broker lease in quick succession. An eventually consistent query can omit `A` while
returning `B`. With no delay, accepting `B` advances the cursor to 2 and the observed frontier to
101. Later Fast queries use the inclusive frontier and time floor as their lower bound, so they do
not select `A`; even if another query later found it, its sequence end is already behind cursor 2
and it is skipped. Deferring recent `B` leaves both values unchanged until a later retry can
normally observe `A` and `B` together and deliver them in sequence order.

Fast does not continuously scan backward from the last observed snowflake. Its frontier is an
observed lower bound, retained per partition and metadata window, and is pruned once that window
leaves the bounded availability horizon. The visibility delay reduces the chance that a query
advances this bound during ordinary replica lag; it does not turn the frontier into a completeness
watermark or establish a DynamoDB visibility guarantee.

A Fast or checkpoint-overlap recovery scan can miss a row that becomes visible outside its bounded
availability horizon. Strong metadata reads remove the read-replica component, but do not make a
paginated query an atomic multi-page snapshot. Enabling fenced broker metadata writes additionally
prevents a stale former producer from publishing after lease expiry; with it disabled, the former
stale-writer limitation remains.

The reader's cursor filtering relies on metadata becoming visible in compatible sequence order.
Graceful broker handoff drains locally accepted work before lease release. Applications must tolerate
duplicates; deployments that require the stronger cross-process publication property must enable
fenced metadata writes in addition to choosing an appropriate reader-consistency mode.

Strong metadata queries remove read-replica staleness at approximately twice the metadata-query RRU
component. They do not by themselves prevent stale publication; fenced broker metadata writes
provide that publication fence when enabled.

Use `cost-analysis/cost_analysis.py` with real page sizes, poll rates, consumer counts, and
regional pricing before enabling strong reads. Its `--strong-metadata-reads` mode models the
resolved `strongly_consistent_metadata_reads` configuration or runtime-flag value; it doubles
metadata scan RRUs but leaves already strongly consistent coordination reads unchanged. Its
`--transactional-metadata-writes` mode separately models fenced metadata publication.

## Consumer Group Coordination

Consumer instances with the same topic and group ID register membership liveness. They use one
shared, versioned assignment plan per topic-group rather than independently retaining local sticky
maps. A planner lease elects the member that refreshes or replaces this plan. On a membership or
partition-inventory change, the planner derives a complete cooperative sticky assignment from the
previous persisted plan, preserving existing placements where possible before moving the minimum
required partitions to balance member load. It conditionally publishes the result while it owns the
planner lease.

Consumers can optionally register a stable physical `pod_id` alongside their stable member ID.
When every active member has supplied this metadata, the planner automatically changes from the
legacy flat member policy to a hierarchical policy: it first preserves and balances aggregate
partition load across pods, then balances the partitions selected for each pod across that pod's
members. Pod loads and per-pod member loads therefore differ by at most one partition. This keeps
existing worker concurrency unchanged. If any active member lacks a `pod_id`, the planner retains
the legacy flat policy, which balances all members globally. Rolling upgrades need no migration:
the next plan published after all legacy membership rows have expired or been heartbeated with a
`pod_id` uses the hierarchical policy and moves only the partitions necessary to meet pod balance.

Every accepted plan must name its planner as a member, cover each configured virtual partition
exactly once, and assign every partition to a plan member. Flat plans keep global member loads
within one partition. Pod-aware plans include a complete member-to-pod snapshot and keep both pod
loads and member loads within each pod within one partition.
A consumer makes no local desired-ownership decision while no structurally valid persisted plan is
available. If it observes a stale local membership view while the elected planner remains live, it
continues using the last valid shared plan rather than creating an incompatible local map. The
planner lease is fenced by a unique coordinator session, preventing a stale process that reused a
member ID from publishing or releasing a successor's lease. Once it expires, any live member can
acquire it and publish a successor plan.

Assignment-plan publication transactionally condition-checks the planner lease and updates the
plan. Concurrent acquisition, renewal, or release of that same lease can therefore receive
`TransactionConflictException`; these planner-item mutations retry the short-lived conflict with
a bounded exponential delay. The standard DynamoDB SDK retry policy does not classify this error
as retryable. Conditional failures remain normal fencing outcomes and are not retried.

TODO: A single DynamoDB assignment-plan item is limited to 400 KB. If group plans can approach
that limit, replace it with sharded or S3-backed plan storage; an S3-backed design will require
the corresponding consumer IAM read permissions.

An assignment is only intent. A consumer becomes an active owner after the lease store grants its
lease for the plan version, which is also its per-partition lease generation. Partition leases
continue to fence actual ownership and preserve committed cursors; they do not determine desired
coverage. A stable accepted plan generation does not re-claim leases that this member already owns;
the coordinator claims only newly assigned partitions or partitions whose plan generation changed.
This prevents periodic rebalances from generating an assignment write for every owned partition.

Lease heartbeats and cursor commits include the generation so stale owners cannot renew or advance
the cursor after replacement. Scheduled maintenance renews every owned partition lease and flushes
any staged cursors. An explicit `commit()` writes only the staged cursor partitions through the
conditional cursor operation: it does not renew unrelated partition leases, extend lease expiry, or
heartbeat membership. This keeps frequent application checkpointing from amplifying steady-state
lease traffic while retaining generation fencing on every durable cursor advance.

An orderly release writes a durable `graceful_release_ts` marker while expiring the lease. The next
successful claim removes that marker and classifies its predecessor as either a graceful handoff or
an expiry takeover. Initial claims and same-owner generation advances are classified separately.
These transition counts, scheduled renewals, cursor commits, heartbeat failure domain, and retry
count are emitted without topic, group, member, or partition labels so they can be correlated with
aggregate DynamoDB capacity without unbounded cardinality.

Scheduled heartbeat and rebalance failures use independent infinite exponential backoffs with a
500 ms initial delay, $2.0$ multiplier, 50% jitter, and 30-second maximum. A successful operation
resets its own backoff. A failed scheduled heartbeat still defers a due rebalance for that driver
iteration, preventing a membership or lease-store outage from causing concurrent retry storms.
On shutdown consumers release owned leases best-effort, deregister membership, and conditionally
release their planner lease so a remaining member can elect immediately without discarding the
sticky assignment plan.

During rebalance, an iterator stops delivering revoked partitions, discards their prefetched
records, invokes the configured revocation callback, and waits for callback completion before
activating the replacement assignment. A replacement owner hydrates its reader from the committed
cursor, source checkpoint, and legacy commit timestamp. It recovers through the fixed cutover
within retention before enabling that partition's bounded fast path.

### Consumer State Snapshot

The consumer keeps its most recently published immutable local snapshot in memory. Reading this
snapshot does not wait for metadata, blob, lease, or membership-store operations, so the driver and
local diagnostics remain available while a reader scan is stalled on an external dependency.
Snapshots are republished after completed local state transitions; their timestamp therefore
reflects publication time and fields may lag an in-flight read or rebalance.

Assignment-plan snapshots report their active policy, registered member-to-pod topology, aggregate
pod loads, and the optional `pod_id` beside every partition's member assignment. Legacy plans
continue to report member-only assignments without pod fields.

The always-available `/state` endpoint returns that local state plus a fresh, strongly consistent
lease-table query for the consumer group. Its `local.partitions` reports only current or pending
local assignments; revocation discards the prior local cursor snapshot. The response joins
retained lease rows with the last structurally valid assignment plan accepted by the local
coordinator, so it includes planned but currently unleased partitions as well as other consumers'
owner, generation, heartbeat, and committed cursor information. The inline query never enters the
driver control flow and is bounded to one second. A query error or timeout is represented as
`lookup_failed`; the local state remains available in that response.

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
- Legacy and retention-clamped recovery scans traverse full retained windows; usable checkpoints
  instead apply the bounded publication-and-visibility overlap to only their first window.

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
| Broker sequence reservation | 10,000 base sequence values per virtual partition |
| Broker segment compression | zstd, level 3 |
| Producer batch records | 10,000 |
| Producer batch payload bytes | 1 MiB |
| Producer flush delay | 200 ms |
| Producer retry deadline | 30 seconds |
| Topic metadata window | 300 seconds; fixed broker/consumer metadata-key layout contract |
| Broker metadata publication deadline | 15 seconds |
| Consumer `max_clock_skew` | 10 ms when unset |
| Consumer metadata visibility delay | 2 seconds |
| Consumer strong metadata reads | Disabled |
| Consumer recovery slice | Up to 32 metadata windows per scan pass |
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

## Operational Assumptions and Accepted Risks

This system makes the following operational assumptions. They reduce the probability of known
failure modes but are not correctness guarantees. Deployments that cannot accept the corresponding
risks must use the stronger mechanisms described below.

### Clock Synchronization

Fast scans and checkpoint-overlap recovery require a bounded pairwise clock offset between brokers
and consumers. Configure `ConsumerReadConfig.max_clock_skew` to a proven, monitored deployment
bound; it defaults to 10 ms only when unset and is not a generic NTP guarantee. A consumer that leads a broker beyond
this bound can floor out a segment that is still publishing, while a broker that leads is
conservative but adds latency. Managed time services are appropriate only when their documented or
measured uncertainty, including alerting and propagation allowance, fits the configured bound.
Event timestamps do not participate in this calculation.

### Eventually Consistent Metadata Reads

Consumers use eventually consistent DynamoDB metadata queries. The visibility delay is a
best-effort operational margin intended for ordinary replica lag; DynamoDB does not provide a
bounded replication-delay guarantee. Fast and checkpoint-overlap scans can miss metadata that
becomes visible outside their bounded availability horizon.

This tradeoff is appropriate only when occasional delayed or missed discovery under an extreme
replica-lag event is acceptable. Increasing the visibility delay reduces the risk at the cost of
more scan work and latency. Strongly consistent metadata reads can be configured and will remove the
read-replica component (at the cost of approximately doubling the metadata-query RRU component), but
do not by themselves fence stale producer publication.

### Unfenced Stale-Writer Publication

Producer leases fence sequence reservation and write acceptance, but segment metadata publication
is not transactionally conditioned on a unique producer lease session or epoch. A broker that
crashes, pauses for an extended period, loses network connectivity, or resumes after lease expiry
can publish metadata after a successor has already published later sequences. A consumer can then
advance its cursor past the late range and cannot recover it.

Kubernetes liveness probes, broker deadlines, and graceful handoff reduce the likelihood and
duration of some stalled-process failures, but they do not provide a publication fence: a paused or
partitioned former holder can still resume and publish late. This tradeoff is appropriate only
when that rare loss mode is acceptable.

Deployments requiring a stronger at-least-once guarantee across writer failures can configure
transactionally fenced metadata publication, paired with strongly consistent reads where replica
staleness must also be eliminated. Transaction write fencing significantly increase metadata WRU
costs because every segment write must condition check all lease keys in a single transaction.
