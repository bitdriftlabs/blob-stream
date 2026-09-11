# Consumption

This page defines consumer recovery, metadata and blob reads, cursor progression, and consistency
tradeoffs. See [storage](storage.md) for the durable data layout and [consumer
coordination](consumer-coordination.md) for group ownership and commits.

## Read Sources

Consumer bootstrap requires broker discovery. Every planned metadata-window request and every
grouped blob-range request route to the deterministically selected local broker. An eventual
metadata request may use or refill a retained broker cache entry: `Tail` and `FullRecovery`
coverage use separate caches. Strong metadata requests bypass retained coverage but still route
through the broker to DynamoDB. A Fresh-only scan uses `FullRecovery` coverage for the current
window because it has no checkpoint bound. An unavailable membership, broker failure, invalid
response, overload, or timeout makes the consumer fail over to its original direct DynamoDB read.

The broker returns `NOT_FOUND` authoritatively for an immutable blob object. Consumers follow the
normal missing-batch path in that case; every other blob-service failure falls back to direct blob
storage. Consumers retain authority for decompression, validation, ordering, and cursor
progression.

## Progress Markers

Each virtual partition has two durable progress markers:

- The **cursor** is the greatest individual sequence offset delivered and committed. A batch is
   fully consumed when `seq_end <= cursor`. When its inclusive range `[seq_start, seq_end]` crosses
   the cursor, the reader discards offsets through the cursor and delivers the remaining suffix.
- The **source checkpoint** identifies the metadata window and segment that produced the cursor.
  A replacement owner uses it to bound its first recovery query. It is not an ordering watermark.

`read_available()` plans work per owned partition and merges compatible work into at most one
metadata query per window. Results are filtered for each partition, sorted by Sonyflake ID, and
their partition batches are ordered by `seq_start`. Sorting does not create a global sequence
order; it relies on [production](production.md)'s per-partition publication ordering.

Every delivered record retains its source checkpoint. `store_offset()` persists the checkpoint
paired with the cursor, allowing a replacement owner to start retained recovery at the correct
window.

## Reader Modes

A partition moves through these modes:

1. **Fresh:** No committed cursor or source checkpoint exists. The reader scans the current aligned
   window and becomes Fast after completing it without a visibility or capacity barrier.
2. **Recovering:** A resumed partition starts at the checkpoint window, clamped to retention, and
   scans chronologically to a captured current-window cutover. A usable checkpoint applies an
   inclusive overlap bound only to its first window; later windows are full scans. Recovery scans
   at most 32 consecutive windows per pass.
3. **Fast:** After Fresh or Recovery reaches its cutover, the reader scans the bounded recent
   horizon where metadata may still be unpublished or invisible. Fast is an optimization and
   never replaces retained recovery for a resumed partition.

A visibility deferral, a sequence discontinuity whose predecessor may still appear, or exhausted
prefetch capacity blocks cursor progress at the earliest affected batch/window. A later batch
cannot advance a partition cursor past that barrier. Failed metadata or blob reads restore the
pass state so an undelivered batch is retried.

## Sonyflake Time Bounds And Clock Synchronization

A Sonyflake ID is the time-ordered 64-bit identifier assigned to each segment; its timestamp has
10 ms resolution. Fast scans and the first recovering window that overlaps a source checkpoint
start from a time-based floor, then convert that floor to the lowest Sonyflake ID that could have
been generated at or after it. The floor is inclusive: replayed boundary metadata is made safe by
the per-partition cursor.

This conversion depends on broker and consumer clocks. `max_clock_skew` must bound the monitored
pairwise clock offset for the deployment; its 10 ms default is not a generic NTP guarantee. If the
actual offset exceeds the configured bound, a consumer can derive a Sonyflake floor that is too
new and omit metadata that it still needs to scan. Producer event timestamps do not affect segment
selection.

## Visibility And Fast Bounds

Let $D$ be the broker's maximum metadata-publication lag plus the configured consumer clock-skew
bound and effective eventual-read visibility delay. The Fast safe timestamp is:

$$
T_{safe} = now - D
$$

Fast considers the current window and enough preceding windows to cover $D$, omitting a window
whose end is at or before $T_{safe}$. Its lower bound is the lowest Sonyflake ID for the later of
the window start and $T_{safe}$, further limited by the partition's inclusive observed frontier.
The frontier is a lower bound from a prior response, not evidence that all earlier metadata was
visible. It is cleared when ownership is lost or a caller seeks.

For strong reads, the default is $D = 15s + S$, where $S$ is `max_clock_skew` and defaults to
10 ms when unset. Explicit eventual reads add their visibility delay, which defaults to two
seconds when unset. The publication deadline is an availability budget as well as a reader-cost
bound: expiration fails the flush rather than silently widening the read horizon.

The effective visibility delay is used only for explicit eventual metadata reads. It defers a
recently published row as a best-effort replica-staleness margin; it does not establish a
DynamoDB replication-delay guarantee. Strong reads accept every valid row returned by storage.

## Delivery, Loss, And Consistency

The consumer prefetches decoded batches within a byte budget and preserves planned output order.
It may range-read one enclosing span for several selected batches from the same segment, accepting
intentional overfetch to reduce object-store requests. Assignment revocation discards buffered
records for the revoked partition before the replacement assignment becomes active.

A valid metadata row whose blob is `NotFound` violates the [storage](storage.md) retention and
durability contract. The reader records the loss, advances its in-memory cursor through that
range, and does not deliver it. The caller must still commit later acknowledged progress; a restart
before commit can classify the range again.

The system is at least once, not exactly once. Cursor filtering makes ordinary replay safe, but
applications requiring exactly-once effects need idempotent sinks, application record IDs, or
their own deduplication.

Strong metadata reads remove read-replica staleness but do not fence stale producer publication.
Eventual reads can miss a row outside Fast or checkpoint recovery's bounded horizon. Deployments
that need the stronger cross-process publication property must enable fenced metadata writes and
choose strong reads (the default) when replica staleness is unacceptable.

## Appendix: Diagnostic Details

The consumer reports `delivery_gap_events` when the first post-recovery delivery exceeds its
recovered committed cursor plus one. Its rate-limited evidence includes the final admitted metadata
scan and a supplemental live partition snapshot, but does not infer a root cause. Reader
diagnostics expose retained Fast frontiers and the time/frontier bounds used for each Fast scan.
Detailed examples are in [read-path scenarios](appendix-read-scenarios.md).

[Design overview](README.md) | [Storage](storage.md) | [Consumer coordination](consumer-coordination.md)
