# Blob-Stream Rebalance Investigation

## Conclusion

Both concerns are supported by the supplied event. A single observed shutdown releases six
partitions, while fourteen surviving pods cooperatively release another 57 distinct partitions.
Recovery repeatedly examines already-consumed metadata: 98.95% of metadata batch observations are
cursor skips. The current code exposes three amplification mechanisms: non-sticky co-location
planning, full-worker flushing on any revocation, and prefetch that remains assigned to revoked
partitions until application acknowledgement.

The operator additionally reports heavy DynamoDB read request unit (RRU) spikes during these
rebalance events. This is operational evidence of real storage-read pressure, not merely inflated
local scan counters. It strengthens the repeated-storage-read hypothesis; the RRU series has not
been independently retrieved or attributed to individual queries in this investigation.

The evidence does not establish that all recovery payload reads are duplicates, that metadata
rescanning dominates wall time, or that every survivor move was unnecessary under the complete
production assignment policy. No production source code or configuration was changed.

## Dataset And Verification

- Source: production traces, `69e134032e628e9e2c5f1bd5`.
- Query window: `2026-10-02T21:40:54Z` through `2026-10-02T21:44:54Z`.
- Filter: `loop-api` on merge-worker pods, with the five span names from the supplied URL.
- 412 spans, 412 unique `(TraceId, SpanId)` pairs, 42 complete MCP responses.
- Counts: 300 partition handoffs, 63 recoveries, 34 assignments, 14 revocation handoffs, one shutdown.
- No response notes or trimming markers; nested diagnostic JSON was parsed successfully.
- Nanosecond timestamps and durations are preserved as strings. Raw attributes, events, and links
  are retained in [spans.jsonl](spans.jsonl); response hashes are in [provenance.json](provenance.json).

The MCP's search limit is nominally 200, but large responses are also trimmed to ten rows or have
large values removed. Array and JSON aggregate exports were not complete. Disjoint hash buckets
containing at most ten spans avoided this. [manifest.json](manifest.json) records the exact query
and bucket plan; [verification.json](verification.json) records the integrity checks.

The [archive guide](README.md#span-export-options) records upstream MCP limits and simpler UI/direct
ClickHouse capture options for future investigations. This event retains the verified MCP capture.

## Partition Movement

All 63 released partitions have exactly one recovery and change owner. There is no repeated
ownership move for a partition in this selection. The generation changes from 4195 to 4196.

| Release Reason | Moves | Recovery Batches Accepted | Recovery Records Accepted |
|---|---:|---:|---:|
| Departing pod shutdown | 6 | 604 | 7,880,611 |
| Surviving-owner revocation | 57 | 2,795 | 30,879,840 |
| Total | 63 | 3,399 | 38,760,451 |

The 300 handoff spans are snapshots, not 300 moves. They break down into 57 revocation-pre-release,
57 revocation-result, six shutdown-pre-release, six shutdown-result, 141 rebalance-assigned, and
33 startup-assigned snapshots. Assigned snapshots include retained partitions and intermediate
partial assignments while incoming leases become available.

The placement reconstruction covers 136 observed partitions on 21 old owners and 20 new owners.
This is not a complete group-membership or inventory snapshot. Among these observed placements,
all old and new owner loads are six or seven. The number of observed logical groups split between
pods increases from three to five. These facts do not demonstrate a co-location improvement.

### Likely Planner Cause

The checked-in production configuration enables `blob_stream_consumer_colocate_logical_partitions`.
The actual versioned plan and deployed flag snapshot are absent from these spans, so this is
supporting evidence, not confirmation of the planner inputs used for generation 4196.

In [assign_colocated_groups](../../blob-stream-consumer/src/coordination/assignment.rs#L122),
whole-group selection ranks remaining-capacity fit before retained ownership. Groups are processed
in logical-ID order. An early orphaned group can therefore consume a surviving owner's capacity
and displace that owner's later group.

[policy-probe.json](policy-probe.json) contains a small reproduction of that exact selection order
with uniform cluster preference: eight partitions, four two-partition logical groups, and four
balanced owners. Removing a two-partition owner causes four moves. An orphan-only reassignment
causes two moves with the same final loads and the same three whole logical groups. Thus the
selection rule can cause unnecessary churn even without a balance or co-location benefit.

This is a Python reproduction of the current Rust selection rule, not a Rust test execution or an
exact replay of this production event. A production replay needs both complete plans and membership.

## Recovery Work

| Measure | Value |
|---|---:|
| Metadata batch observations | 423,158 |
| Cursor-skipped observations | 418,711 |
| Cursor-skip fraction | 98.95% |
| Accepted batches | 3,399 |
| Observations per accepted batch | 124.49 |
| Capacity deferrals | 1,048 |
| Visibility deferrals or blocks | 0 |
| Recovery outcomes | 63 `fast_path_active` |
| Recovery span median / p95 / max | 11.88 / 36.54 / 36.54 seconds |

The accounting closes exactly: `423158 = 418711 + 3399 + 1048`. These are repeated observations,
not unique metadata batches. Every recovery has both its initial recovery window and cutover
window set to `1790977200`, or `21:40:00 UTC`. There is no multi-window retention catch-up in the
recorded recovery start bounds. No visibility delay is reported in the recovery summaries.

The selected partition 181 on `merge-worker-848db95476-kfwws` takes 31.66 seconds and 245 passes:
16,860 metadata observations, 16,738 cursor skips, 86 accepted batches, and 36 capacity deferrals.
Its seven co-recovering partitions finish together after 245 shared worker passes; together they
report 117,962 observations and 117,113 cursor skips. Pass counts are not independent partition
workers or necessarily metadata RPC counts.

[recovery_scan_min_snowflake](../../blob-stream-consumer/src/consumer/scan/planning.rs#L392)
keeps the first-window bound anchored to the durable resume checkpoint. Capacity-limited passes
remain in the incomplete window. The [cursor filter](../../blob-stream-consumer/src/consumer/scan/execution.rs#L1656)
then examines and skips already-consumed batches again, before constructing blob read candidates.
[Recovery metadata caching](../../blob-stream-consumer/src/consumer/scan/planning.rs#L37)
requires mature windows; these recovery start windows are also the current cutover window.

This explains a repeated-enumeration path consistent with the data. The counters alone cannot
separate local CPU enumeration, broker cache hits, metadata RPCs, or DynamoDB cost. Cursor-skipped
batches do not themselves establish redundant blob downloads. Accepted recovery records include
backlog and new ingestion as well as possible redelivery; they are not all duplicates.

### DynamoDB Cost Evidence And Follow-Up

The operator's correlated RRU spikes make storage cost a primary optimization target, alongside
local traversal. A client cache or faster cursor filter is insufficient if the same storage-query
work remains. The spikes alone do not separate redundant overlap from necessary backlog reads,
more recovering partitions, or membership/lease/plan reads.

Align the RRU series with rebalance, assignment, and active-recovery intervals. Identify the
table/index, region, and workload scope; separate metadata queries from coordination reads.
Measure storage pages, consumed read capacity, consistency, retries, broker cache/refill and
coalescing, and consumer direct fallback. Count actual storage queries rather than converting
consumer RPCs or partition observations into assumed DynamoDB calls. Compare excess event RRU
above a comparable steady-state baseline and normalize by recovered partitions/useful progress.

Acceptance must include reduced attributed metadata read cost, not only fewer local visits.
The existing uncertainty concerns query attribution, amplification magnitude, and latency
contribution, not whether the operator observes a real RRU spike during these events.

## Revocation And Timing

All fourteen revocation handoffs succeed, taking 6.18 to 14.97 seconds. The
[merge-worker revocation handler](https://github.com/bitdriftlabs/loop-api/blob/7019808f8b2b699d26845d542adc428254503711/loop-api-insights/src/merger/worker.rs#L550)
calls `do_full_flush()` before acknowledging, even for a single revoked partition. The
[full flush](https://github.com/bitdriftlabs/loop-api/blob/7019808f8b2b699d26845d542adc428254503711/loop-api-insights/src/merger/worker.rs#L724) flushes every cache and commits
the primary and optional alternate iterator. Delivery is globally fenced while acknowledgement
is pending, not just for the revoked partition.

The [cooperative-revocation path](../../blob-stream-consumer/src/iterator/driver/assignment.rs#L487)
drops queued revoked batches, but keeps the reader's old assignment until acknowledgement and
lease release. [Prefetch admission](../../blob-stream-consumer/src/iterator/prefetch.rs#L670)
checks active membership, not pending cooperative revocation, so it can refill revoked work during
the drain, up to the byte budget. Its amount is not measurable from the supplied snapshots.

The shutdown snapshots retain 15 buffered batches containing 259,284 records. Cooperative
pre-release snapshots are taken after their initial queue drop, and release-result snapshots
reuse pre-ack state, so these snapshots cannot measure the total discarded work during revocation.
For 27 partitions, the new owner's claimed cursor is behind the old reader's pre-ack read cursor;
read progress is not the same as application-committed progress.

There is a tracing limitation: [partition read states](../../blob-stream-consumer/src/consumer/reader/diagnostics.rs#L10)
include pending recovery, and [recovery tracing](../../blob-stream-consumer/src/iterator/prefetch.rs#L350)
starts before active assignment. For all six shutdown partitions, 7.51 seconds of the longest
36.54-second recovery span precedes active assignment. Their activation-to-Fast time is about
29.03 seconds. Across the selection, the longest snapshot-based activation-to-Fast time is about
31.67 seconds. Do not sum overlapping revocation and recovery spans.

For partition zero, release occurs at `21:43:20.567966282`, recovery tracing begins at
`21:43:37.517004206`, assignment becomes active around `21:43:45.029341126`, and Fast is reached
around `21:44:14.054648118`. The interval from shutdown-pre-release to Fast is 53.51 seconds.
Fast completion is not necessarily the first delivered record: recovery itself delivers records.

ClickStack's child-operation breakdown reports only 114 handoff snapshot children under the
fourteen revocation roots, totaling 0.519 milliseconds, and no children under the long recovery
roots. Those traces do not identify a slow storage or network dependency.

## Recommended Work

1. Add the eight-partition counterexample to the Rust planner tests. Preserve survivor placements
   before packing orphaned groups, or optimize movement subject to balance and co-location goals.
   Replay complete plans 4195 and 4196 before assigning a precise avoidable-move count to this event.
2. Remove revoked partitions from reader eligibility at a safe worker boundary before waiting for
   application acknowledgement, while retaining delivery/commit bookkeeping needed for final flush.
   Test that existing application work can commit while revoked prefetch stops.
3. Make incomplete recovery traversal resumable instead of enumerating consumed prefixes on every
   capacity refill. Preserve inclusive checkpoint overlap, late visibility, sequence-gap handling,
   and rollback correctness. Measure metadata requests, cache reuse, and enumeration time before
   attributing the 31-second recovery entirely to overscan.
4. Separate pending-assignment wait from active recovery tracing and capture post-drain committed
   state at release. Add flush/commit spans linked to the revocation so its delay can be attributed.

## Local Artifacts

- [spans.jsonl](spans.jsonl): preserved spans with full attributes.
- [spans.csv](spans.csv): decoded diagnostic fields for inspection.
- [transitions.csv](transitions.csv): one row per confirmed move, including activation timing.
- [summary.json](summary.json): recovery totals and per-pod statistics.
- [movement.json](movement.json): observed placement maps and transition details.
- [policy-probe.json](policy-probe.json): synthetic planner counterexample.
- [event-shaped-probe.json](event-shaped-probe.json): complete synthetic 21-to-20-pod fixture;
   64 greedy moves versus six feasible moves at equal co-location quality, not a production replay.
- [analyze.py](analyze.py): standard-library analysis, no external dependencies.

From the Blob Stream checkout root, reproduce the analysis and counterexample with:

```sh
python3 plans/rebalance-2026-10-02/analyze.py plans/rebalance-2026-10-02/raw/batch-*.json
python3 plans/rebalance-2026-10-02/analyze.py --policy-probe
python3 plans/rebalance-2026-10-02/analyze.py --event-shaped-probe
```
