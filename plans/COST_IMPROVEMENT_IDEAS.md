# Blob-Stream Cost Improvement Ideas

## Purpose

This document describes three potential cost improvements for blob-stream:

1. Store all topic data flushed by one broker in one shared segment object rather than one object
   per topic.
2. Serve immutable blob ranges through a consistently routed broker cache to collapse overlapping S3
   reads.
3. Serve segment metadata through a consistently routed broker cache to collapse repeated DynamoDB
   metadata scans.

The proposals are independent. Measure and roll them out separately so their cost, latency, and
reliability effects are attributable.

Recommended sequence:

1. Establish a production-calibrated baseline.
2. Implement and measure shared cross-topic segment objects for compatible topics.
3. Prototype a stateless broker batch cache with direct-S3 fallback.
4. Implement broker-collapsed metadata scans only when measured DynamoDB read amplification
   justifies the additional read-path service.

The formulae below identify the workload measurements needed to decide whether each idea is
worthwhile. They are not production forecasts.

## Current Architecture And Cost Multipliers

### Write path

Producers send batches to a broker selected for a topic and virtual partition. Brokers lease
producer partitions, allocate sequence ranges, buffer accepted batches, then persist segments. A
producer receives `OK` only after the segment object and metadata have both persisted.

The broker already coalesces within a topic:

- A time-due partition causes the scheduler to pull all available buffered partitions for that topic
  into one flush plan.
- The plan serializes one compressed `StoredRecordBatch` per virtual partition and concatenates
  those batches into one object.
- One `blob_segments` DynamoDB row indexes that object for the topic and window.
- Byte-threshold and lease-drain flushes remain partition-local so they can persist promptly.

The current coalescing boundary is `topic`, not `virtual_partition`. Idea 1 extends that boundary to
all topics with data in a broker scheduler pass.

### Persistent layout

An object key currently contains the topic and aligned time window:

```
<prefix>/<topic>/<window_start>/<snowflake_id>.<zst|bin>
```

Each metadata row is keyed by:

```
pk = <topic>#<window_start>
sk = <lexicographically sortable snowflake_id>
```

The metadata payload contains the blob key, compression, publication time, and a virtual-partition
index with byte and sequence ranges. Consumers range-read only the compressed batches selected from
that index.

### Read path

Consumers query DynamoDB directly for each required topic/window, then filter results by assignment,
cursor, recovery state, and metadata visibility delay. They plan S3 range reads for eligible
batches, decode locally, and commit cursors through consumer-group lease records.

The broker is not currently on the read path. Direct reads multiply with consumers:

- Every consumer process scans the same topic/window metadata even when it owns only a subset of
  virtual partitions.
- Every consumer group independently range-reads the same immutable S3 bytes when it tails the same
  topic.

### Existing baseline signals

| Concern | Existing metric or source |
| --- | --- |
| Flush-plan count | `write:flush_plans_total` |
| Partitions included in flush plans | `write:flush_partitions_total` |
| Trigger mix | `write:flush_batches_max_delay_total`, `write:flush_batches_max_bytes_total`, `write:flush_batches_lease_drain_total` |
| Uploaded object size | `write:flush_uploaded_object_bytes`, `write:flush_uploaded_object_bytes_total` |
| Consumer metadata scans | `reader:metadata_scan_requests`, fast and recovery variants |
| Consumer range reads | `reader:blob_range_requests`, `reader:blob_range_bytes`, batch-range metrics |
| Metadata DynamoDB capacity | `dynamo:read_request_units_total`, `dynamo:write_request_units_total` |
| Storage charges | S3 and DynamoDB CloudWatch/billing data for the same interval |

Existing metrics do not expose distinct topics per broker scheduler pass. Add one low-cardinality
histogram or counter for cross-topic fan-in before forecasting Idea 1. Do not add per-topic metric
labels.

## Baseline And Costing Method

Collect at least one representative steady interval and one peak interval. Record:

- $F_{time}$: broker scheduler passes with at least one time-triggered topic flush.
- $T$: mean active topics included by a pass, weighted by current object writes.
- $P_{local}$: object writes caused by byte-threshold and lease-drain work that must remain prompt.
- $C_{local}$: active consumer processes in one AZ/cluster reading a topic across all consumer
  groups.
- $G_{local}$: concurrent consumer groups in that AZ/cluster reading the same topic and partition
  data.
- $W$: active/recovery windows scanned by a consumer during a polling pass.
- $R$: scan refreshes or consumer polling passes per hour.
- $Q$: observed S3 range reads per topic or consumer group per hour.
- $B$: observed S3 range bytes for the same workload.
- DynamoDB RRU/WRU, S3 PUT/GET request counts, transfer cost, and broker CPU, memory, and network
  headroom.

Use actual regional prices and include the network path. Read caches are AZ/cluster-local, so the
broker-to-consumer leg must remain local. Price the S3-to-broker and local broker-to-consumer paths,
and reject any configuration that would route a cache response across AZs.

`cost-analysis/cost_analysis.py` is useful for broad request-cost sanity checking, but its fallback
estimates are not authoritative here. The implementation already coalesces timer flushes by topic
and consumer range reads by segment. Set direct segment and range-read overrides from observed
metrics before using the script for a decision.

## Idea 1: Shared Cross-Topic Segment Objects

### Proposal

During one broker scheduler pass, serialize buffered partitions from every included topic into one
S3 object. Publish a normal, separate metadata row for each topic. Each row points to the shared
blob key but indexes only that topic's byte ranges.

This does not change:

- Flush cadence or maximum-publication-lag contract.
- Per-partition sequence allocation or ordering.
- The metadata table's topic/window key structure.
- Consumer metadata scans, cursor semantics, or range-read selection.
- The requirement that a producer receives `OK` only after blob and metadata persistence.

### Object And Metadata Shape

Use a globally unique key that is no longer topic-scoped, for example:

```
<prefix>/shared/<window_start>/<broker_id>/<snowflake_id>.<zst|bin>
```

The exact format is an implementation choice, but it must retain current collision and retry
properties. A deterministic flush identity is preferable when a failed publication may retry after
the blob already exists.

Arrange payloads as contiguous topic sections, then contiguous virtual-partition batches within each
section:

```
[topic A partition batches][topic B partition batches][topic C partition batches]
```

This preserves efficient topic-specific range reads. A consumer reading topic A should not download
topic B bytes merely because both topics share an object.

For every topic in the object, write one ordinary `SegmentMetadata` row:

- Its `TopicWindowKey` remains the topic plus aligned window.
- Its partition index contains only that topic's virtual partitions and ranges in the shared object.
- It uses the shared blob key and existing compression format.
- It records the metadata publication timestamp immediately before its row is written.

One Snowflake ID can be used for all rows because their DynamoDB partition keys differ. The sort key
only orders rows within one topic/window. Verify existing retry and ID-generation assumptions during
implementation.

### Expected Cost Effect

For time-triggered work, ignoring prompt local flushes:

$$ P_{current,time} = F_{time} \times T $$

$$ P_{shared,time} = F_{time} $$

$$ reduction_{PUT,time} = 1 - \frac{1}{T} $$

Including byte-threshold and lease-drain writes that remain independent:

$$ P_{current} = F_{time} \times T + P_{local} $$

$$ P_{shared} = F_{time} + P_{local} $$

Monthly S3 PUT saving is:

$$ (P_{current} - P_{shared}) \times price_{PUT} \times hours_{month} $$

| Mean active topics per pass ($T$) | Maximum time-triggered PUT reduction |
| --- | --- |
| 1 | 0% |
| 2 | 50% |
| 4 | 75% |
| 10 | 90% |

This does not directly reduce DynamoDB segment-metadata writes, consumer metadata scans, stored S3
bytes, or consumer range-read count. Compression savings are likely small because partition batches
remain independently encoded and compressed; do not count compression as a benefit without
measurement.

### Implementation Outline

1. Replace independent topic-scoped `FlushPlan` values with one broker-level aggregate containing
   ordered topic subplans.
2. For time-due work, aggregate all buffered topics selected by that scheduler pass. Preserve prompt
   byte-threshold and lease-drain behavior unless a separate latency decision permits pulling peer
   topics forward.
3. Build one payload and a topic-local index for every included topic.
4. Upload the shared object once.
5. Write every topic metadata row with bounded concurrency and retries.
6. Complete a topic's producer acknowledgements only after that topic's metadata row is durable.

### Failure, Retry, And Durability Semantics

The shared object creates partial-publication states:

| State | Required behavior |
| --- | --- |
| Blob upload fails | No metadata row is written; all affected batches retry or fail together as today. |
| Blob succeeds, no metadata succeeds | Object is orphaned; retry metadata publication without changing byte ranges or object identity. This is already possible for one topic today. |
| Some metadata succeeds | Succeeded topics can become visible and acknowledge. Retry only unpublished topic rows; metadata writes must be idempotent. |
| Process crashes after partial metadata | Restart/retry must not corrupt already-published rows or violate per-partition publication order. Decide whether callers retry a stable identity or a durable outbox records pending topic rows. |

The final row is the important non-mechanical part of this change. A whole-aggregate retry after
partial success must not duplicate or corrupt metadata and must preserve each partition's
publication order.

### Operational Constraints

Blob-stream currently uses one S3 bucket with a lifecycle TTL already set to the longest supported
topic retention. Shared cross-topic objects retain the same bucket and lifecycle behavior, so no new
retention or multiple-bucket policy is required.

The relevant constraints are operational:

- **Object size and deadline:** Larger aggregate objects can increase upload latency and memory
  pressure. Enforce an aggregate payload cap and retain maximum publication-lag enforcement.
- **Partial publication:** Per-topic metadata rows can succeed independently after the shared blob
  upload. Retries and acknowledgements must retain the topic-level behavior described above.
- **Object access:** The existing bucket access model continues to apply because the implementation
  already uses one bucket. Re-evaluate only if future deployment changes introduce per-topic buckets
  or object-prefix isolation.

### Measurement, Tests, And Rollout

Implement only when $T$ and trigger mix predict material savings after excluding prompt local
flushes.

Required deterministic tests:

- One/multiple topics and disjoint virtual-partition sets.
- Timer aggregation plus unchanged byte-trigger and lease-drain behavior.
- Exact topic-local metadata ranges and consumer reads from every topic.
- Per-partition sequence order and topic-level acknowledgement behavior.
- Blob failure, metadata failure before any row, metadata failure after one row, retry, and
  crash/restart behavior.
- Object-size cap and publication-deadline enforcement.
- Existing bucket lifecycle and access configuration remains valid for shared object keys.

Roll out behind a broker feature flag to a small compatible topic set on one broker deployment.
Compare S3 object counts and producer latency with baseline, then expand. Rollback must leave
consumers able to read both old topic-scoped and new shared keys; the metadata-driven reader should
support this if blob keys remain opaque.

## Idea 2: Stateless Broker Batch Cache

### Proposal

Keep consumer control-plane behavior in the consumer library, but route immutable blob batch fetches
through a consistently selected broker. The broker caches compressed batch bytes, coalesces
concurrent misses, reads S3 once for overlapping demand, and returns bytes for the consumer's
existing local decode and validation path.

The consumer continues to own:

- Consumer-group membership and partition assignment.
- Cursor tracking and source checkpoints.
- `seek`, delivery ordering, visibility delay, and recovery planning.
- `store_offset` and durable cursor commits.

This is a storage-read optimization, not a new consumer protocol. It deliberately avoids broker APIs
for seek, commit, rebalance, or consumer-group ownership.

### Routing And Minimal API

Use a dedicated read-broker membership pool in each AZ/cluster. Discovery and routing should follow
the producer pattern: consumers discover only local brokers and rendezvous-hash within that local
membership view. A cache request must never cross an AZ boundary.

Every AZ/cluster consequently has an independent cache. Consumers in different AZs can populate the
same immutable range more than once; that is an intentional tradeoff to avoid cross-AZ transfer.

Rendezvous-hash at least `(topic, virtual_partition_id)` to select a cache owner. Including the blob
key is optional: partition routing improves temporal locality for tails, while blob-key routing
spreads load for a hot partition across object generations. Choose deliberately and expose routing
in diagnostics.

A bounded unary or streaming RPC can accept a batch of requests. Each request needs immutable
identity:

```
topic
virtual_partition_id
blob_key
compression
byte_range
expected_seq_range
```

Each response echoes the identity and returns compressed bytes. The consumer retains decompression,
protobuf parsing, virtual-partition validation, and sequence-range handling. This makes a direct-S3
shadow comparison exact.

Cap request bytes, response bytes, item count, and range size. Leave ranges too large for a bounded
RPC on the direct-S3 path initially.

### Broker Cache Behavior

The broker maintains:

- Bounded LRU or TinyLFU-style cache of immutable compressed batch bytes.
- TTL bounded by object retention, primarily for memory management rather than correctness.
- Per-cache-key in-flight futures so concurrent requests wait on one S3 read.
- Adjacent-range grouping when requests share an object and overfetch is within a configured limit.
- Per-topic and global memory limits, admission control, and eviction metrics.

No invalidation protocol is required because published blobs and byte ranges are immutable. On
discovery failure, overload, transport failure, or malformed broker response, the consumer falls
back to direct `BlobStore::get_range()`.

### Expected Cost Effect

The cache helps only when consumers in one AZ/cluster request the same immutable ranges while they
remain cached. With $G_{local}$ fully overlapping local consumer groups tailing every batch:

$$ reduction_{GET,local} = 1 - \frac{1}{G_{local}} $$

One group has no guaranteed fanout saving, although rebalance, replay, or duplicate readers can
still hit. Low temporal overlap or insufficient cache capacity can make realized savings approach
zero.

Account for both data paths:

$$ net\ transfer\ cost = avoided\ S3\ transfer - local\ broker\ egress\ cost $$

This proposal does not reduce DynamoDB metadata scans. It is independently deployable from Idea 3.

### Measurement, Tests, And Rollout

Add metrics only where current counters cannot answer the decision:

- Cache hit/miss/eviction counts and bytes.
- In-flight miss coalescing waiters.
- S3 requests/bytes issued by the cache.
- Direct-S3 fallback count and reason.
- RPC latency, response bytes, broker memory, and overload rejections.

Use shadow mode before changing delivery: fetch through broker and direct S3, compare identities and
decoded records, but deliver the direct result. Then enable broker results for a small consumer
group while retaining direct-S3 fallback.

Deterministic tests must cover cache hit, concurrent miss coalescing, eviction, range grouping,
routing change, malformed response rejection, broker-failure fallback, and direct-versus-broker
decoded equality.
