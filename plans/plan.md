# Consumer Rebalance And Recovery Performance Plan

## Scope

Reduce unnecessary partition movement, stop prefetch for partitions whose ownership is being
revoked, and eliminate repeated recovery traversal of already-consumed metadata where it is safe.
Keep merge-worker's full-cache flush and iterator commit behavior unchanged.

Best-effort logical co-location becomes the sole placement policy. Remove the placement feature
flag and alternate non-colocated policy as part of implementation; do not add a replacement policy
flag or retain flag-based planner rollback.

This is an implementation plan, not an assertion that the production event has been reproduced
with complete planner inputs or that all recovery time is spent scanning metadata.

## Evidence And Working Diagnosis

The 2026-10-02 production merge-worker rebalance contains 412 verified spans. Six partitions leave
the shutdown pod; another 57 distinct partitions move between surviving owners. All 63 recoveries
complete successfully. The event's placement snapshots cover only 136 partitions and are not the
complete assignment inventory.

The [preserved investigation](rebalance-2026-10-02/report.md) includes the analysis script, raw
responses, verified full-span export, provenance, and derived placement/recovery data. Keep future
replays and additional event captures alongside their reports, rather than relying on scratch paths.

Recovery reports 423,158 metadata observations: 418,711 cursor skips, 3,399 accepted batches, and
1,048 capacity deferrals. The cursor-skip fraction is 98.95%. Every recovery's initial window and
cutover are `2026-10-02T21:40:00Z`; recorded visibility-deferral counters are zero. These counts are
per-pass observations, not unique batches or metadata RPCs. Accepted records are not necessarily
duplicates.

The operator also reports heavy DynamoDB read request unit (RRU) spikes during these events.
Treat this as supplied operational evidence of real storage-read pressure, not a local-counter
artifact. The metric series has not been independently retrieved here. It strengthens the
repeated-storage-read hypothesis without identifying the responsible table/index, exact query
path, or fraction of cost due to redundant overlap rather than necessary catch-up and coordination.

The current [recovery bound](../blob-stream-consumer/src/consumer/scan/planning.rs#L392) remains
anchored to the first-window durable checkpoint. The [capacity-stop path](../blob-stream-consumer/src/consumer/scan/execution.rs#L1656)
holds an incomplete recovery window while the next pass re-enumerates its consumed prefix. This
is a falsifiable explanation for the counts: a static-window fixture with a small byte budget
should reproduce growing cursor skips as refills increase, without requiring visibility delay.

### What Further Code Review Establishes

1. Successful assigned-reader passes construct zeroed partition scan snapshots and replace
   `last_scan`; [recovery tracing](../blob-stream-consumer/src/iterator/prefetch.rs#L475) adds those
   observations. The assigned path is not simply adding a lifetime cumulative counter repeatedly.
   Pending recovery and cancellation still need explicit scan identities for reliable attribution.
2. [Planning](../blob-stream-consumer/src/consumer/scan/planning.rs#L502) merges all partitions that
   need only their cutover window into one window request. Otherwise historical recovery rotates
   partitions. The selected seven-partition pod reports 245 passes for each recovery, 605 accepted
   batches, and 244 capacity deferrals in aggregate. This fits shared budget-limited passes, not
   seven independent workers making 245 RPCs each.
3. [Mature-window reuse](../blob-stream-consumer/src/consumer/scan/execution.rs#L213) avoids repeated
   queries but rebuilds results for the usual filtering/admission loop. It does not maintain a
   consumed-prefix traversal position. Caching alone therefore does not eliminate local overscan.
4. Strong broker responses and direct strong scans already supply an observation-based seal.
   However, [prefix installation](../blob-stream-consumer/src/consumer/scan/execution.rs#L406) and
   [prefix reuse](../blob-stream-consumer/src/consumer/scan/execution.rs#L163) exclude requests with
   Recovery bounds. The open recovery window is neither mature nor eligible for this Fast-only
   prefix path. Each refill can request and revisit the same safe prefix again.
5. [Broker request construction](../blob-stream-consumer/src/consumer/scan/execution.rs#L1928)
   uses `Tail` when every recovering partition has a checkpoint bound, and `FullRecovery` when
   any participating Recovery bound is absent. Do not describe every recovery query as a full
   window read: a fixed, inclusive Tail bound can still produce severe repeated prefix work.
6. Cursor filtering happens before blob-read candidate construction. A cursor skip is metadata
   work, not proof that that batch's payload was downloaded again. An admitted enclosing range
   may overfetch other bytes; measure actual requested and returned bytes separately.

For a static partition with `C` already-consumed overlapping batches, `N` unread batches, and `P`
capacity-limited passes, the current path can revisit the `C` batches every pass and an increasing
consumed portion of `N`. With one accepted batch per pass, this is roughly `P*C + N*(N-1)/2`
cursor skips, plus accepted and deferred candidates. Query caching can remove remote work without
removing this local growth. A resumed immutable traversal should instead scale with `C + N + P`.
This is a fixture-level complexity prediction, not an estimate of this event's unique batch count.

### What Is Not Yet Established

- The exact avoidable production movement count: plans 4195/4196, full membership, partition
  inventory, and deployed planner mode are missing. Checked-in configuration enables co-location,
  but is not a deployed flag snapshot. The eight-partition probe is not an actual Rust execution.
- The deployed binary's revision and consistency/runtime settings. Match the investigated source
  to the deployment before treating the seal-path diagnosis as a production implementation fact.
- How much of the 31.66-second selected recovery is query, network transfer, enumeration, blob
  fetch/decode, budget wait, or application consumption. The long roots have no useful child
  operations for attribution. Do not claim metadata enumeration dominates that wall time.
- Unique skipped rows, broker cache reuse versus storage queries, actual bytes, and speculative
  revoked work. Neither accepted-record counts nor stale release snapshots measure duplicates.

All six shutdown recoveries include approximately 7.51 seconds before assignment activation in
their 36.54-second spans. Active recovery is about 29.03 seconds for those partitions; the largest
observed assignment-to-Fast interval is 31.67 seconds. Fast is not first delivery: Recovery itself
can return records. Preserve these distinctions in benchmarks and dashboards.

**Conclusion:** repeated checkpoint-prefix traversal under capacity is understandable from code
and consistent with the observations. Reported RRU spikes corroborate a storage-read cost problem;
exact query attribution, amplification magnitude, and runtime contribution remain unresolved.
Prioritize eliminating redundant storage queries as well as local visits. Extend the existing
strong seal mechanism and measure both kinds of work; do not start with a new wire protocol or an
unproven advancing checkpoint floor.

## Non-Negotiable Invariants

- Preserve at-least-once delivery, per-partition ordering, generation fencing, and durable cursors.
- A revoked partition cannot deliver another record after revocation begins.
- Final application work can still stage and commit offsets while its revocation callback runs.
- Lease release follows application acknowledgement; do not release early to reduce latency.
- Reader mutation remains serialized at the prefetch worker's safe read boundary.
- A failed read must not leave traversal progress ahead of accepted data.
- Capacity, visibility, and sequence-gap deferrals remain reachable on a later scan.
- Prefetch and retained metadata remain bounded; do not trade overscan for unbounded memory.
- Merge-worker's full flush remains untouched.

## Workstream 1: Placement

The [co-location planner](../blob-stream-consumer/src/coordination/assignment.rs#L122) currently
ranks tight capacity fit before previous ownership and processes groups in logical-ID order.
An early orphaned group can consume a survivor's capacity and displace that survivor's later group.
An eight-partition local reproduction moves four partitions where two achieve identical balance
and the same number of whole logical groups. Port this fixture to the actual Rust planner tests.

### Objective And Compatibility

Hard constraints remain: canonical complete partition coverage, one eligible owner per partition,
floor/ceiling load balance at the pod layer and then within each pod, and deterministic output.
Keep the existing cluster residual-capacity preference and partial-topology fallback. A group with
incomplete pod metadata still uses logical co-location over flat members, not a different placement
policy. Count unique canonical inventory partitions, not stale previous assignments.

### Remove The Placement Flag

Remove `blob_stream_consumer_colocate_logical_partitions` from runtime definitions, watched settings,
planner branching, configuration overrides, diagnostics that present it as a selectable mode, and
current-state documentation. Remove obsolete plain-sticky policy code and flag-toggle tests where
they have no remaining callers. Always invoke the improved best-effort co-location algorithm.

Update generated flag bindings through their owning generation workflow. Clean up deployment
configuration references in the same implementation series; neither true nor false should remain
a supported configuration choice. Replace toggle tests with unconditional-policy regressions.

Check versioned assignment-plan compatibility before removing any serialized policy field. A
stored field may temporarily remain a fixed compatibility marker for older readers; it is not a
runtime option. Preserve accepted-plan validation, planner authority, and cooperative handoff
during a rolling binary upgrade. Do not reinterpret an old accepted plan in place or let members
select different algorithms independently. Remove transitional serialization only when its reader
compatibility is proven separately.

Preserve the current contract that whole logical groups take priority over previous ownership.
Do not buy stickiness by degrading co-location. Score feasible candidates lexicographically by:

1. Number of logical groups split at the current placement layer.
2. Number of surviving partitions moved at that layer.
3. Extra owner fragments beyond the first split for already-split groups.
4. Canonical owner/partition ordering for deterministic ties.

Apply this at the pod layer first, then at the worker layer with pod placement fixed. Track
cross-pod moves separately from intra-pod member moves. Mandatory orphan placements are reported
but do not count as avoidable survivor moves. Unknown departed-pod topology must not be invented.

### Algorithm

- Keep the current greedy result as a valid comparison candidate and fallback. The selected
   candidate cannot be worse in split-group count or survivor moves at equal split-group count.
- Construct a second candidate by reserving existing whole groups and retained fragments on
   eligible owners before placing orphaned/new partitions. Repair only capacity violations; retain
   as much previous ownership as possible, and use canonical ordering to break ties.
- Pack remaining whole groups into free capacity when possible. Split only when the remaining
   balanced capacity requires it, including the case where preserving a later survivor group is
   better than packing an early orphan group whole.
- Improve both candidates with capacity-preserving partition exchanges and whole-group swaps.
   Accept only strict improvements to the documented score. Search small interacting groups for
   multi-exchange repairs; bound search by a deterministic count of examined states, not wall time.
   Keep the best feasible candidate on search exhaustion. This is a bounded heuristic, not a claim
   of globally optimal bin packing.
- Specify the state-count limit from the large fixture's planner lease budget before enabling the
   new policy. A limit must never yield an incomplete assignment. Expose budget exhaustion in the
   existing bounded planner summary, rather than creating an unbounded tracing stream.

Use [coordination_test.rs](../blob-stream-consumer/src/coordination_test.rs#L263) for regressions.
The paired-group fixture must move only departed partitions `0` and `4`, retain every survivor,
produce loads `3/3/2`, and keep three whole groups. Existing whole-group-priority and
smaller-cluster-residual tests must remain valid.

Test unchanged feasible membership/idempotence, shuffled and duplicate inputs, empty inventory,
join/leave, uneven groups, previously fragmented groups, worker departure inside a surviving pod,
multiple workers, partial pod/cluster metadata, and rolling accepted-plan compatibility. Use exhaustive small
assignment enumeration as an oracle: require the counterexample's optimum and record gaps for
other cases without pretending the bounded heuristic is universally optimal. Replanning the
result with unchanged inputs must be stable, not move to a different equivalent packing.

### Event-Shaped Regression And Expanded Coverage

Add a deterministic complete synthetic inventory, not a reconstruction that treats the observed
136 partitions as the full production group:

- Use 64 logical IDs, 45 triples `[logical_id, logical_id + 64, logical_id + 128]` for IDs 0 through
   44, plus singleton partition 63: 136 explicit fixture partitions on 21 pods, one worker per pod.
- Pod `i` initially owns whole groups `2*i` and `2*i + 1`, for pods 0 through 20. Split the three
   remaining groups 42 through 44 over pods 0 through 8, one additional partition per pod. Put the
   singleton on pod 9. This yields ten loads of seven, eleven loads of six, and three split groups.
- Remove pod 20, which owns the six partitions of groups 40 and 41. A feasible assignment keeps
   every survivor and puts one orphan on each of pods 10 through 15. It has sixteen loads of seven,
   four loads of six, and five split groups, without any survivor move.
- Require exactly six changed partition owners, zero survivor moves, complete unique coverage,
   the expected balanced loads, five split groups, and stable replanning. Under capacity seven,
   each remaining pod can hold at most two complete triples, so at most 40 of 45 triples can stay
   whole; five splits are necessary. This makes the no-survivor-move baseline a real optimum for
   this fixture, not a claim that the production event needed only six moves.

Replay the current and improved algorithms against the fixture and retain both movement counts
as evidence. The [preserved Python probe](rebalance-2026-10-02/event-shaped-probe.json) reports
64 greedy moves, including 58 survivor moves, versus six feasible moves with zero survivor moves,
at the same five split groups. This validates the fixture against the Python reproduction, not an
actual Rust test or production replay. Do not hard-code the production count of 57 survivor moves
into synthetic output.
Repeat with permuted inputs, pod IDs, departing pod position, multiple workers per pod, uneven
cluster membership, and successive departure/join cycles. Build a second full 192-partition/
64-triple fixture to cover a conventional complete virtual-partition layout as well.

Add seeded property-style cases for inventory/owner uniqueness, floor/ceiling balance, deterministic
ordering, idempotence, no worse co-location than the comparison candidate, and no extra movement at
equal co-location quality. Use small exhaustive oracles and large bounded-work cases in addition to
the event-shaped regression. Avoid a few hand-picked comparator tests as the sole coverage.

Complete a live-group event-shaped departure scenario with causal gates, exact changed-owner
counts, per-record delivery counts, durable cursor/checkpoint checks, and convergence. This must
exercise the unconditional policy through normal coordination, not only the assignment helper.

For production replay, acquire the full accepted old/new plans and membership from the owning
plan store or a reproducible diagnostic capture. Run both policies offline. Attribute necessary
and avoidable moves only after checking the deployed mode and topology inputs; the 136 observed
partitions are not a valid full-inventory replay fixture.

## Workstream 2: Revoked Prefetch

Cooperative revocation currently drops delivery batches but retains reader assignment until
application acknowledgement. [Prefetch admission](../blob-stream-consumer/src/iterator/prefetch.rs#L670)
checks active membership, so revoked work can refill during the existing full-flush wait.

### State And Command Design

Add a read-fenced partition set to [shared iterator state](../blob-stream-consumer/src/iterator/shared.rs#L163),
separate from `active_partitions`, which controls commit eligibility. Identify changes with a local
assignment/revocation epoch so a delayed command cannot fence a later reacquisition.

At revocation detection, under the existing shared-state lock, publish the read fence before
exposing `NextResult::Revoked` and dropping revoked delivery work. Queue a dedicated revoked-only
reader command, conceptually `SuspendReadPartitions { partitions, epoch }`, and notify both the
reader-command and prefetch-space wait paths. Never wait under the shared lock.

Do not implement suspension as `SetAssignment(retained_only)`. The current
[assignment replacement](../blob-stream-consumer/src/consumer/reader/lifecycle.rs#L190) deletes all
state outside the replacement, including newly hydrated `PendingRecovering` additions that are
not allowed to scan yet. Introduce a selective reader operation that removes only revoked state,
frontiers, recovery traversal, and cached metadata. Preserve retained and incoming pending state.

At the next serialized worker boundary, apply suspension before another read. Close only revoked
recovery/seek traces, purge revoked worker-pending batches, repair exact byte/record counts, and
refresh diagnostics. Do not emit an ordinary active-assignment snapshot or clear the global
delivery fence for this read-only operation.

Both read-result-to-pending admission and pending-to-delivery admission must test the shared read
fence, not only active ownership. Work from a pass already in flight may finish, but revoked output
must not be retained or delivered. The first version permits at most that bounded pass of residual
work; it does not abort a mixed-partition future with retained output or invent cancellation safety.

This is cooperative suspension, not generation fencing. Leave `active_partitions`, staged offsets,
source checkpoints, and valid lease heartbeats intact while the application flushes. Preserve the
existing global delivery pause, callback completion, lease release, terminal handling of ambiguous
release failures, and post-ack assignment activation.

Clear old read fences only with the successful corresponding assignment transition. Reacquisition
hydrates from the durable claimed cursor; never resurrect the old reader's speculative cursor.
Handle callback completion before command processing: command order/epoch checks must not suspend
a just-reactivated partition. A failed final release must not silently resume revoked reads.

### Tests

Extend existing iterator tests, particularly
[commit_during_revocation_persists_revoked_partition_cursor](../blob-stream-consumer/src/iterator/iterator_test.rs#L2527).
Hold the callback with named gates and prove:

- A buffered revoked record and an in-flight revoked result cannot pass admission or delivery.
- After the suspension-applied gate, no new revoked-only metadata/blob read is started. A shared
   window request may still serve retained partitions; inspect requested partition coverage rather
   than interpreting any request to the same window as revoked work.
- Retained data stays intact and incoming hydrated partitions do not become Fresh or scan early.
- Final revoked offsets/checkpoints can still commit, and lease renewal/release ordering is intact.
- Capacity-blocked, idle, and retry-backoff workers wake for suspension; released bytes are correct.
- Immediate acknowledgement, command races, duplicate suspension, release failure, seek overlap,
   shutdown, and later reacquisition do not leave a stale fence or reader cursor.

Use a live group integration test to complete the handoff: the new owner acquires a newer lease,
skips committed work, redelivers deliberately uncommitted work with explicit per-ID counts, commits
its source checkpoint, and fences stale old-owner operations. No merge-worker flush changes are
required to achieve these guarantees.

## Workstream 3: Recovery Overscan

### Step A: Reproduce And Attribute

Extend [consumer_test.rs](../blob-stream-consumer/src/consumer/consumer_test.rs#L4213), reusing
`RecordingMetadataStore`, broker request recording, blob fault helpers, and metrics `Helper`.
The existing mature-window test proves query reuse, not bounded local cursor-skip visits.

Create a static current-window fixture with 1,000 committed overlapping batches and 100 unread
batches, all safely below a strong observation's seal but inside a window whose end is not mature.
Force one accepted batch per refill, then drive the final empty-suffix scan to Fast. Repeat with
seven recovering partitions sharing the cutover and with mixed Fast/Recovery partitions.
Use actual Snowflake time bounds and checkpoint overlap, not artificial bounds that skip the
committed prefix by construction.

Measure consumer RPCs, direct scans, returned/projected metadata entries, per-partition visits,
cursor skips, cache reuse, capacity stops, and admitted payload ranges. Demonstrate baseline repeated
visits and fixed request bounds. Distinguish an open window from its already-immutable prefix.
Also use a complete mature window to demonstrate repeated local visits despite one storage query.

Before selecting the production latency target, collect stage timing and coverage provenance in
one controlled drain/rebalance. Keep data rate, partition topology, payload size, prefetch budget,
read consistency, application consumption, and binary revision comparable.

### Step B: Reuse Strong Recovery Prefixes

Generalize the existing validated strong-prefix installation and phased cached-prefix/suffix
execution to Recovery. Do not merely widen the Fast predicate: include Recovery's per-partition
checkpoint bounds, window-completion barriers, and pending/active lifecycle in the proof.

- A complete mature window is reusable as a whole observation. An open window is reusable only
   below the existing validated `sealed_before`, using the original pre-scan `observed_at`.
- Store a per-partition coverage interval with its observation and the checkpoint/recovery epoch.
   Share immutable backing storage where possible; do not repeatedly rebuild projected responses.
- Every participating partition must have valid complete coverage from its required lower bound.
   For merged requests use the least covering seal; never advance all partitions to the greatest
   individual seal. A partial cache hit cannot certify the uncovered partitions.
- Process cached eligible batches before fetching the suffix, and recheck capacity between phases.
   Query the still-open suffix inclusively from the seal. Update both request-level and per-partition
   Recovery bounds. A previously unbounded Recovery request can become bounded only after its
   lower interval has complete coverage; retain Tail/FullRecovery wire compatibility.
- A prefetched response that installs a prefix is processed once, not also re-added from the newly
   installed cache in the same pass. Preserve chronological finalization of concurrent windows.
- Complete the window or enter Fast only after all required partitions/sources have been inspected
   without a capacity, visibility, or sequence barrier. An exhausted cached prefix is not an
   exhausted window; the open suffix still requires the existing scan semantics.

This step reduces metadata RPC/response amplification but is not sufficient if cached batches are
still visited repeatedly. Measure it separately from the next step. No broker/protobuf change is
expected: [response validation](../blob-stream-consumer/src/consumer/metadata_query.rs#L164) already
validates the required seal proof.

### Step C: Resume Immutable Traversal

For each retained immutable coverage entry, keep the next unexamined segment/batch position and
the accepted reader cursor that validated the consumed prefix. Initialize by finding the consumed
prefix once. Partition-specific traversal must not repeatedly visit other partitions' projected
copies in merged results.

Only discard or advance past an entry when its existing cursor proves it consumed, its selected
payload is successfully decoded and accepted in order, or the existing missing-blob path explicitly
classifies the loss and advances through it. Capacity-deferred and gap/visibility-held candidates
stay reachable. A partially consumed batch remains at the inclusive cursor boundary until its
remaining records have been accepted. Preserve the current oversized-single-batch escape path.

Stage traversal changes for the pass and commit them with successful reader acceptance. Extend
the [read rollback](../blob-stream-consumer/src/consumer/scan/execution.rs#L1113) to the new progress
state. Cache population may survive a failed read if its coverage proof remains valid, but its
consumed position must not. Recovery predecessor retry must rewind/invalidate affected traversal
along with the existing held window; never move past a gap because a later batch was fetched.

When coverage grows, extend or merge it without losing unread candidates or making a saved index
refer to different metadata. Identity includes partition, window, original lower bound, recovery
epoch, and proof; an arbitrary new RPC response is not the same immutable traversal snapshot.
Avoid cloning entire retained metadata on every pass solely to support rollback.

Bound new retained recovery coverage and indexing, initially to a 16 MiB per-reader retained-state
budget with decoded-entry accounting and a bounded entry count. Count shared backing once and
include per-partition indexing. This is a proposed initial tuning value, not the existing RPC
response-size limit or an assertion about current memory. Measure it against the seven-partition
fixture before rollout. Eviction must discard only an optimization: a later pass can safely
requery from its durable/recovery floor. Existing in-flight response memory is measured separately.

Invalidate affected entries on suspension/revocation, seek, cursor hydration/reset, ownership epoch
change, retention expiry, incompatible consistency/horizon changes, and recovery completion.
Do not prune a still-needed historical Recovery entry using Fast's live-horizon rule.

### Open Suffix And Eventual Reads

Do not advance an unsealed network query floor to the latest accepted Snowflake, cache an eventual
observation as complete strong coverage, or declare a window sealed merely because time elapsed.
Late publication, cross-window predecessor arrival, replica visibility, and configuration changes
retain their current safe requery paths.

Initially leave unsealed/eventual suffix scanning unchanged. Instrument its residual work. If it
dominates after sealed-prefix resume, evaluate a bounded observation-local pending queue whose
expiry/refresh does not certify missing metadata. This requires dedicated late-row and gap tests.
If existing proofs cannot make it safe, the next deliverable is a separate consumption/protocol
design backed by telemetry, not an unsafe floor optimization hidden in this implementation.

### Test Matrix And Numerical Gates

- Static sealed-prefix fixture: visits are bounded by `C + N + 2P`, rather than increasing with
   `P*C` and the growing consumed suffix. Allow explicitly counted inclusive boundary replay.
   For the 1,000/100 fixture require at least 90% fewer visits versus its measured baseline.
- While unread immutable prefix work fills capacity, no repeat full-prefix RPC is required.
   The static open-window fixture needs at most the initial observation and final suffix query;
   separate failures, evictions, and genuinely new observations from this deterministic bound.
- Mature-window fixture: keep the existing single-query guarantee and add the same visit bound.
- Merged unequal floors/seals, partial cache coverage, duplicate segment projection, mixed modes,
   and shared capacity do not omit partitions or count one RPC per participating partition.
- Inject late earlier metadata inside the unsealed suffix and across a window boundary. Verify
   gap retry, inclusive seal overlap, retention clamp, eventual visibility deferral, and no loss.
- Inject a late blob/read/decode failure after an earlier candidate succeeds. Retry returns the
   required batches; traversal/cursor rollback, `NotFound` policy, and no-data retries are correct.
- Cover oversized and zero-byte batches, budget changes, seek/revoke/reacquire, prefix eviction,
   concurrency-prefetched windows, chronological finalization, and recovery-to-Fast rollover.
- Assert per-record sequence/count outcomes, final durable cursors/checkpoints through live group
   consumers, and peak retained-state accounting. Preserve existing recovery fairness; a dense
   immutable prefix must not monopolize historical partition rotation or the shared byte budget.

## Telemetry And Validation

### Bounded Attribution Contract

Reuse existing scan/broker-offload/refill/budget counters and histograms where available. Add their
per-recovery or per-read deltas to bounded `recovery.summary_json`, rather than duplicating global
metrics just to produce a trace summary. Keep exported spans within 16 attributes; avoid spans per
batch, high-cardinality partition metric labels, or unbounded sets of seen Snowflakes.

| Area | Required Evidence | Ownership And Interpretation |
| --- | --- | --- |
| Scan identity | monotonic reader pass ID, recovery epoch, active/pending state | aggregate each partition's snapshot once; empty/pending loops are not scans |
| Request work | consumer RPC/direct-scan counts, response bytes/entries, Tail/FullRecovery, bound/seal provenance | count once per window request, not once per partition |
| DynamoDB cost | table/index RRU series, storage Query pages and consumed capacity, consistency, retries, broker refill/coalescing, direct fallback | separate metadata from coordination; consumer RPCs and cursor skips are not storage-query counts |
| Cache work | mature/sealed reuse, retained bytes, evictions, cursor-skipped/visited entries | distinguish remote response work from local traversal |
| Payload work | admitted/requested/returned bytes, accepted batches/records, missing blobs | distinguish useful payload from overlap and range overfetch |
| Stage timing | planning, metadata await, local enumeration, blob await, decode/accept, capacity/idle/retry wait | wall-time buckets must not double-count concurrent futures as critical-path time |
| Revocation | fence publication/application, pending/queued/in-flight discarded bytes, callback wait, commit and release timing | commit eligibility survives suspension; one bounded in-flight pass is permitted |
| Planner | mode/revision, membership/inventory hash and counts, co-location score, orphan/survivor/cross-pod/member moves, search exhaustion | full replay inputs live in an explicit diagnostic artifact, not trace attributes |

Use existing segment-without-partition counters to reveal work not represented by batch cursor
skips. Exact unique observation sets belong in bounded test fixtures or diagnostic captures, not
long-lived production deduplication sets. New metrics require operational value beyond existing
signals and tests through `bd_server_stats::test::util::stats::Helper`.

Correlate the reported RRU spikes with exact event intervals and table/index/region scope. Reuse
existing storage-capacity telemetry where available; otherwise capture consumed capacity at
actual paginated metadata Query execution in both broker and direct paths. Preserve operation
semantics, count capacity once per completed page/attempt, and do not treat unavailable capacity
on a failed attempt as zero. Distinguish strong/eventual read cost and useful catch-up volume from
repeated overlap. Table-wide RRU can include unrelated traffic and coordination reads; it is not
automatically per-recovery cost. Put storage-side attribution in bounded correlated diagnostics,
not new high-cardinality per-partition metrics or a new wire contract.

Add first-active-scan time, first accepted batch, and active-to-Fast timing alongside the existing
recovery root; do not silently reinterpret its historical duration. Pending-assignment wait and
actual scanning must be separately observable. Reader pass identity also prevents accidentally
recounting an old snapshot after an assignment/seek transition.

Capture a refreshed post-ack snapshot before releasing the lease, then attach that state to
release-result spans; retain the pre-ack snapshot as a separately named observation. Test a final
commit that advances the source checkpoint while the callback is held. Include cancellation,
release failure, and closed-callback behavior without changing their existing semantics.

Trace iterator commit/release stages and callback wait with bounded correlation. Callback wait is
not synonymous with flush duration. Only add observational timing around the application's
existing full-flush call if these stages still leave the delay unattributed; do not alter cache
selection, flush scheduling, commit frequency, or callback ordering.

### Telemetry Decision Gate

Collect one controlled seven-partition recovery and one live-group departure with the new fields.
Match their reader counters with request recordings/metrics and confirm accounting under a failed
read, pending assignment, and cancellation. Reconcile actual metadata Query pages/capacity with
the table/index RRU spike; distinguish broker refill, direct fallback, coordination reads, and
necessary catch-up. Storage-cost reduction is required even if metadata scans do not dominate
wall time. Then select the next performance action:

- Repeated sealed responses or storage refills dominate: enable Recovery seal reuse and confirm
   actual storage-query/capacity reduction as well as RPC/bytes reduction. A broker cache hit may
   avoid a storage prefix read already; attribute its suffix/refill cost rather than assuming
   each repeated consumer response rereads the entire prefix from DynamoDB.
- Cached cursor visits dominate: enable traversal resume and confirm local visits/CPU reduction.
- Open suffix/eventual visibility dominates: keep the correct path and investigate bounded
   observation reuse with the measured coverage proof; do not assume the sealed fix covers it.
- Blob/decode or capacity/application wait dominates: report that result and keep it out of the
   metadata-scan latency claim. Full-flush policy remains outside this plan's behavior changes.

Follow [TEST_AUDIT.md](TEST_AUDIT.md): use named lifecycle gates and logical sleeps, not wall-clock
delays. Reader tests may isolate scanning, but ownership and duplicate-policy outcomes require
live group consumers and per-record delivery counts.

Update [consumer coordination](../docs/design/consumer-coordination.md) and
[consumption](../docs/design/consumption.md) with each corresponding behavior change. Keep delivery
and durable storage contracts unchanged unless a separately justified design requires otherwise.

## Delivery Order And Acceptance

Implement as reviewable changes; prioritize measured recovery work because it is the main concern.
Placement and suspension fixtures can be developed independently of the recovery optimization.

1. **Evidence and instrumentation:** port the planner counterexample, add baseline open/mature
   recovery visit tests, add pass/request/timing provenance, and fix post-ack diagnostic snapshots.
   Exit: repeatable work amplification and reliable phase accounting; obtain deployment revision.
2. **Recovery proof reuse:** extend the existing strong-prefix path without new wire fields.
   Exit: no repeated full-prefix RPC while cached unread work fills capacity, all coverage tests pass.
3. **Recovery traversal:** add bounded immutable progress, rollback, invalidation, and memory tests.
   Exit: numerical visit gates pass; timing improvements are attributed, not guessed from skip ratio.
4. **Revoked read suspension:** introduce the selective command and admission fence, preserving
   incoming hydration and final application commits. Exit: deterministic worker and group handoff
   tests; no revoked reads after the safe boundary and no late retained-output loss.
5. **Placement:** remove the placement flag/alternate policy and implement scored retention/repair
   with deterministic bounded improvement. Exit: two-move counterexample, six-move event-shaped
   fixture with zero survivor moves, substantially expanded property/oracle/live-group coverage,
   rolling-plan compatibility, existing co-location/topology contracts, idempotence, bounded
   runtime, and offline production replay when full inputs are available.
6. **Production comparison:** exercise a controlled departure at comparable workload and inspect
   movement, active recovery, attributed metadata RRU, remote/local work, peak memory, duplicates,
   and final checkpoints.
   Keep residual open-suffix and application-wait costs visible, not relabeled as fixed overscan.

### Rollout And Rollback

Recovery optimization may use the existing runtime-feature infrastructure for reversible rollout.
Placement has no policy flag: best-effort logical co-location is the only behavior. Roll out the
planner fix through coordinated binary releases and versioned plans under the existing planner
authority. Rollback uses a known-good binary that also enforces co-location, not a toggle to plain
sticky placement. Any required replan still follows normal cooperative handoff; never reassign
leases in place or reinterpret an already accepted plan.

Roll out a coordinated test group before a production cohort. If adding a suspension kill switch,
evaluate it at an assignment epoch and make disabling it preserve current drain/fencing state;
never resume a revoked partition mid-callback. Do not add speculative controls that have no safe
runtime transition or documented owner.

Promotion requires all correctness gates, no load/co-location regression, bounded retained state,
and reduced eligible prefix request/visit work. Compare attributed excess metadata RRU above a
matched steady-state baseline, RRU per recovered partition/useful batch, normalized visits/bytes
per accepted batch, and active-to-Fast time at the same offered load and consistency. Keep total
event cost separate from per-partition efficiency: fewer moves should reduce the former, while
safe recovery reuse should improve the latter. A CPU-only improvement is not sufficient to close
the reported storage-cost issue. A high initial skip fraction alone is not a
failure: committed checkpoint overlap is intentional. Do not promise an arbitrary latency target
before the attribution capture identifies the dominant stage.

Rollback on new delivery gaps/loss, cursor or lease-fencing regressions, nonconverging plans,
unbounded retained metadata, or an increase in comparable recovery work. Preserve diagnostics and
the failing replay fixture. Do not weaken publication/visibility safety to meet a performance gate.

## Implementation Ownership And Required Checks

| Change | Owning Files | Required Design Update |
| --- | --- | --- |
| Unconditional placement/repair and coverage | `coordination/assignment.rs`, coordinator settings/plan validation, `coordination_test.rs`, flag definitions/generated bindings and deployment overrides in their owning repositories | consumer coordination and current configuration reference |
| Read fence/command/admission | `iterator/shared.rs`, `iterator/driver/assignment.rs`, `iterator/prefetch.rs`, `iterator/iterator_test.rs`, selective reader lifecycle | consumer coordination and consumption |
| Recovery coverage/progress | `consumer/scan/planning.rs`, `consumer/scan/execution.rs`, `consumer/state.rs`, reader lifecycle, `consumer/consumer_test.rs` | consumption |
| Attribution/snapshots | prefetch, scan execution, driver assignment, `diagnostics.rs`, existing metrics modules | metrics reference and consumption/coordination diagnostic contracts |
| End-to-end handoff | existing integration tests and lifecycle gates | test audit if a named scenario changes |

Paths in this table are relative to `blob-stream-consumer/src`, except the integration-test crate.
Shared flag definitions/generated bindings and deployment overrides live in their owning checkouts;
inspect and validate those separately instead of assuming all flag cleanup is consumer-local.
Reuse existing modules/test helpers. No merger behavior change, durable format migration, storage
schema change, new dependency, or broker protocol change is expected. Update
[metrics](../docs/metrics.md) and [operations](../docs/operations.md) only for shipped signals and
supported controls; keep current-state contracts out of migration narratives.

These are future implementation gates, not tests executed by writing this plan. Run from the
monorepo root, not from the physical Blob Stream symlink target:

```sh
just rustfmt
./bazelw test --config=clippy //blob-stream/blob-stream-consumer/...
./bazelw test --nocache_test_results \
  --test_arg=-E \
  --test_arg='test(colocated_assignment_) | test(recovery_) | test(mature_recovery_) | test(commit_during_revocation_) | test(revocation_)' \
  //blob-stream/blob-stream-consumer:test
./bazelw test --nocache_test_results //blob-stream/blob-stream-consumer:test
./bazelw test --nocache_test_results \
  --test_arg=-E \
  --test_arg='test(prefetch_rebalance_) | test(consumer_restart_resume_from_committed_offsets) | test(lease_expiry_takeover_preserves_progress)' \
  //blob-stream/blob-stream-integration-tests:end-to-end-test
```

Add the new narrowly named regression filters to the focused invocation as they are introduced;
the full consumer target also covers compatibility cases outside these patterns. Include the
owning fault-injection wrapper for new failure cases. Lint every touched Rust package; if the
integration-test crate changes, also run its package-pattern Clippy check. Run broader affected
existing integration wrappers before rollout. Their `BUILD.bazel` declares DynamoDB/LocalStack
service dependencies; do not use `__libtest` or start unrelated external services.

For changed asynchronous lifecycle tests, prove uncached serial repetitions:

```sh
./bazelw test --nocache_test_results --runs_per_test=25 --local_test_jobs=1 \
  --test_arg=-E --test_arg='test(the_changed_lifecycle_test)' \
  //blob-stream/blob-stream-integration-tests:end-to-end-test
```

Substitute the actual new/changed test name and owning wrapper. Gate manual-time advances on a
registered logical sleep or causal lifecycle event. Run Bazel invocations sequentially and do not
edit their inputs while they run. After Clippy-driven edits, rerun formatting and focused Clippy.
When BUILD/Starlark/TOML changes are needed, run `just format` and `just check-format` too. Finish
each implementation with editor diagnostics and `git diff --check` in every touched checkout.
