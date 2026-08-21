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
