# Consumption

This page defines consumer recovery, metadata and blob reads, cursor progression, and consistency
tradeoffs. See [storage](storage.md) for the durable data layout and [consumer
coordination](consumer-coordination.md) for group ownership and commits.

## Read Sources

Consumer bootstrap requires broker discovery. Every planned metadata-window request and every
grouped blob-range request route to the deterministically selected local broker. An eventual
metadata request may use or refill a retained broker cache entry: `Tail` and `FullRecovery` coverage
use separate caches. Strong metadata requests cannot reuse eventual coverage; they may reuse a
complete sealed window or a sealed prefix from a prior strong observation. In an open window the
broker reads the suffix from the inclusive seal boundary on every request (concurrent readers of the
same prefix generation and suffix floor share an in-flight query). A Fresh-only scan uses
`FullRecovery` coverage for the current window because it has no checkpoint bound. An unavailable
membership, broker failure, invalid response, non-storage overload, or timeout makes the consumer
fail over to its original direct DynamoDB read. An exhausted DynamoDB Query throttle, reported by
the broker as `STORAGE_THROTTLED`, does not fall back to another Query. The same rule applies to a
direct metadata scan that exhausts its throttle budget. The live
`blob_stream_consumer_broker_metadata_direct_fallback` flag defaults to true; disabling it returns
broker metadata errors instead of retrying those requests directly. Direct-only scans are
unaffected. Prefetch backs off with a positive, capped full-jitter delay after every failed read;
failed scans restore cursor, frontier, and immutable traversal state. Unknown or unspecified broker
overload reasons retain the direct fallback when the flag is enabled.

Each metadata Query page, whether read by the broker or directly by the consumer, disables SDK
retries and operation timeouts for that operation. A live, three-second page deadline bounds
application retries for confirmed throttling and transient connection, timeout, or server failures.
Non-retryable errors fail immediately. A retry repeats only the failed page; completed pages remain
in the scan. Other DynamoDB operations retain their SDK retry and timeout settings. The broker
bounds a metadata request to five seconds including coalescing and refill work; the consumer's
broker RPC timeout defaults to seven seconds. Both request deadlines can change via live feature
flags. A broker request timeout is not assumed to mean storage throttling.

The broker returns `NOT_FOUND` authoritatively for an immutable blob object. Consumers follow the
normal missing-batch path in that case; every other blob-service failure falls back to direct blob
storage. `OVERLOADED` blob responses identify request timeout, memory pressure, fetch concurrency,
or whole-object cache-admission rejection with an additive typed reason. Unknown or unspecified
reasons have the same direct-storage fallback as other non-`NOT_FOUND` failures. Consumers retain
authority for decompression, validation, ordering, and cursor progression.

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
   at most 32 consecutive windows per pass, issuing up to eight independent uncached window
   metadata queries concurrently while finalizing results in chronological order.
3. **Fast:** After Fresh or Recovery reaches its cutover, the reader scans the bounded recent
   horizon where metadata may still be unpublished or invisible. Fast is an optimization and
   never replaces retained recovery for a resumed partition.

The reader retains complete mature windows and per-partition sealed Recovery and Fast prefixes. A
prefix `[window lower bound, sealed_before)` is installed only after a complete strong scan whose
original observation proves the metadata publication deadline has passed; neither an eventual
response nor elapsed time alone establishes that proof. The broker caps a requested seal using its
publication-lag and clock-skew budget measured before the storage scan begins, even when the
refill's first caller did not request a seal, and returns that original observation. Direct strong
scans use the same pre-scan proof time. The reader also caps the seal at its pass-start Fast safety
floor, validates broker proofs against its own horizon, and falls back to direct strong storage on
an unusable broker response. Later passes process cached batches before asking for the inclusive
`[sealed_before, window end)` suffix; capacity admission can defer that query altogether. Shared
requests require coverage of every participating partition's lower interval and use the least
advanced valid seal, including mixed Recovery and Fast requests. An uncovered interval forces the
original query. An unbounded Recovery request becomes bounded only after its complete lower interval
has been observed. Contiguous strong observations can extend a prefix without changing its earlier
rows; elapsed time does not extend coverage.

Recovery retains a next-unexamined position in each partition/window's immutable snapshot, validated
against the accepted cursor, snapshot identity, and the position's initial query floor. Relaxing that
floor reconsiders earlier rows even when the snapshot is unchanged. Partition-projected traversal
avoids visiting other partitions' copies. Only cursor-proved consumed rows, ordered accepted batches,
and the normal missing-blob policy advance that position. Partial, deferred, and held batches remain
reachable. Failed reads restore staged positions without copying retained rows; gap retries invalidate
the affected positions. Prefetched responses are admitted once, including their newly installed prefix.
An open prefix alone cannot complete Recovery: the suffix and all barriers must complete before
transition to Fast, whose initial coverage floor preserves the validated Recovery seal.

Reader-local metadata retention has a 16 MiB conservative decoded-byte budget and a 512-entry limit.
Each partition/window retains one canonical, ordered segment snapshot. Its mature completeness
qualification and its optional strong sealed-prefix qualification each count as one logical entry,
even when both describe the same backing; narrower query floors do not create additional entries.
Snapshots use a persistent vector of shared immutable row objects. Appending a newly sealed suffix
shares the retained prefix with the previous view instead of cloning and sorting every retained row.
Empty or overlapping coverage upgrades preserve snapshot identity when they add no rows. Partition
projection copies only the selected batch index, not the original multi-partition index.
Accounting includes partition indexes and traversal overhead, counts the backing once, and caches
its allocation cost incrementally rather than walking its rows on every refill. Oldest-window
eviction discards coverage and its position together; subsequent reads safely requery from the durable
checkpoint floor. Ownership loss, seeks, hydration, consistency or availability-horizon changes,
completed Recovery windows, and retention expiry invalidate the applicable state. Recovery prefixes
remain available until their windows complete even outside the Fast horizon; Fast prefixes follow
eligible Fast windows. The broker's separate strong cache has its own byte budget and idle expiry.

`ReaderMetadataCache` owns these retained observations, traversal positions, policy invalidation,
and footprint accounting. Observations are grouped by partition/window, with a complete mature lower
bound and an optional strong seal describing one shared snapshot. Coverage lookup accepts a request
only when the retained proof covers its actual floor; Fast requests record their bounded floor
rather than an unbounded cache identity. Overlapping immutable rows are deduplicated, and contiguous
prefix and mature coverage can widen the complete range without inventing coverage across a gap.
Fast retains a rollover tail's inclusive frontier until its coverage completes, so later capacity
refills do not widen their query below the cached mature floor. The two qualifications retain
independent lifetimes within their container. Appending rows preserves traversal positions;
inserting earlier rows requires recomputing them. Traversal positions remain separately
checkpointed. The reader supplies checkpoint floors, accepted cursors, availability horizons, and
lifecycle retention decisions through the cache interface. Progress checkpoints restore only
traversal state after a failed pass; validated observations remain available for retry.

Cached partition projections of the same immutable segment are coalesced into one enclosing byte
range when several owned batches fit in a pass. Broker and direct fallback reads use that same plan;
partition-local cache ownership does not multiply blob-range requests for a shared segment.

A visibility deferral, a sequence discontinuity whose predecessor may still appear, or exhausted
prefetch capacity blocks cursor progress at the earliest affected batch/window. If concurrent
Recovery scans expose a later sequence while an earlier window can still publish its predecessor,
Recovery holds the later batch and promptly re-queries the oldest still-open window, clamped to the
Recovery start, before advancing. Later batches cannot advance a partition cursor past that barrier.
Failed metadata or blob reads restore the pass state so an undelivered batch is retried.

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
