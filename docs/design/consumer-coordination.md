# Consumer Coordination

This page describes how a consumer group assigns virtual partitions, fences ownership, and
persists progress. See [consumption](consumption.md) for the reader protocol and [storage](storage.md)
for the durable tables.

## Membership And Assignment Plans

Consumers with the same topic and group ID register membership liveness. They share one versioned
assignment plan per topic-group rather than deriving incompatible local maps. A planner lease elects
the member that refreshes or replaces the plan when membership or partition inventory changes.

The planner uses balanced sticky placement with best-effort logical co-location and an optional
membership-change consolidation allowance. A valid plan names its planner as a member, covers every
configured virtual partition exactly once, and assigns each partition to a plan member. Until a
consumer sees a structurally valid plan, it makes no local desired-ownership decision.

Virtual partitions are grouped by `virtual_partition_id % TopicConfig.partition_count`; the logical
count must be positive. Inventory and eligible owners are canonicalized and deduplicated. Stale
previous partitions do not affect balanced capacity. Each placement layer has a floor/ceiling
capacity allocation, so its owner loads differ by at most one.

Consumers can register an optional stable `pod_id`. When every active member supplies one, the
planner balances aggregate partition load across pods first, then balances each pod's partitions
among its members. Otherwise it keeps logical co-location over flat members, balancing them
globally. The sticky baseline reserves surviving ownership at each layer before placing orphaned,
new, or capacity-excess partitions, keeping whole groups where the remaining slots permit it.
Optional repair tries pods first, then workers within each pod. Balance and baseline retention take
precedence over co-location; a group may remain split even when a different packing could fit it.

Consumers may also register `cluster_id`. When every pod-aware member has a cluster ID and all
members on a pod agree on it, the planner uses the number of distinct active pods per cluster as a
final tie-breaker. It gives a residual assignment, such as 8 instead of 7, to a pod in a cluster
with fewer pods when that preserves the existing at-most-one partition pod-load difference. It is
not cluster-load balancing: it never changes primary pod or member balance to equalize aggregate
cluster load. During a partial or inconsistent cluster-ID rollout, the planner retains pod-aware
assignment and simply omits this preference.

The planner lease uses a unique coordinator session. Its publication transaction condition-checks
the lease, preventing a stale process that reused a member ID from publishing or releasing a
successor's plan.

## Sticky Baseline And Optional Repair

The planner first completes one balanced sticky assignment, including both pod and worker layers. It
reserves eligible previous ownership within fixed floor/ceiling capacities before placing mandatory
moves. Previous accepted topology preserves the known pod of a departed worker; missing departed-pod
information is never inferred from member IDs. The cluster residual-slot preference still applies
when fixing pod capacities.

Only a membership difference from the accepted valid plan enables optional repair. Initial plans,
unchanged membership, topology-only changes, planner/session takeover, and runtime-setting updates
do not enable it. Once the new membership is published, heartbeat replans and cooperative handoff
retries retain the result rather than spending another allowance.

The planner snapshots `blob_stream_consumer_colocation_repair_percent` once for a membership
transition. It defaults to 10 and accepts 0-100. For canonical inventory size N, the plan-wide
allowance is the percentage rounded down, with a positive minimum of two and a maximum of N. Zero
disables optional repair. Invalid values use the default with a rate-limited warning. Mandatory
departure, new-partition, and baseline-capacity moves do not consume this allowance. Movement counts
unique final worker-owner differences from the complete sticky baseline, not exchanges or separate
pod/worker moves. All layers and pods share that same baseline and allowance.

At each repair layer, groups are visited once by descending size and then logical ID. A split group
gets one proposal: consolidate on its most occupied domain (canonical ID breaks ties), exchange
non-group partitions back to the source workers, and evaluate the complete result atomically. Return
partitions prefer already-fragmented groups and groups already represented on the source domain,
then partition ID. These fixed construction hints may miss a better return combination. Accept only
a strict lexicographic reduction in split-group count and extra owner fragments beyond the first
split, within the movement allowance. Equal locality never justifies canonical-only churn.

Every exchange preserves the exact worker load histogram, and therefore pod loads. Pod repair runs
on the complete worker map; subsequent worker repair exchanges only within a pod and cannot worsen
pod locality. An infeasible, over-budget, or non-improving proposal leaves the accepted assignment
untouched. There is no recursive search, alternate plan comparison, or return-permutation
enumeration. Candidate count is bounded by one proposal per split group per layer; full copying and
scoring still scale with inventory. Each pass caches complete occupancy and locality for the
accepted assignment; already-whole groups do not trigger inventory rescans. A feasible,
within-budget proposal is scored once, and its occupancy replaces the cache only when accepted. This
is neither globally optimal nor guaranteed to converge to optimal co-location across deployments.
The planner independently validates coverage and balance before publication and verifies its lease
again when publishing. A slow or fenced planner cannot publish after its authority expires. Debug
summaries report attempted and consolidated groups, final optional movement, the shared allowance,
and complete occupancy scans.

Stored `colocate_logical_partitions` is not configuration; plans set it to true. Consumers follow a
structurally valid plan from an active planner without reinterpreting its assignments. Only an
authoritative planner can replace that plan with a higher version. Cooperative revocation and lease
fencing remain unchanged.

## Lease Ownership And Commits

An assignment plan is intent, not active ownership. A consumer becomes owner only after it acquires
the partition's consumer-group lease for the plan version. That version is the lease generation.
Lease heartbeats and cursor commits include the generation, so a stale owner cannot renew a lease
or advance a cursor after replacement.

Scheduled maintenance renews owned leases and flushes staged cursors. An explicit `commit()` writes
only staged cursor partitions through the conditional cursor operation; it does not renew unrelated
leases, extend lease expiry, or heartbeat membership. This keeps application checkpointing from
amplifying steady-state lease traffic while preserving generation fencing.

## Rebalances And Shutdown

During rebalance, the iterator stops delivering revoked partitions, discards their buffered records,
invokes the revocation callback, and waits for that callback before activating replacement work. A
replacement owner hydrates its reader with the committed cursor and source checkpoint, then recovers
through retention before entering the bounded Fast path.

A batch already returned by `next_batch()` remains bounded in-flight application work if ownership
is subsequently fenced. The fence removes unread queued records, not those already returned. Before
acknowledging revocation, the application finishes or abandons that work and stages only processed
offsets with their delivered source checkpoints. Pending revocation callbacks are returned before
any additional batch, and no new revoked output is admitted.

Read suspension is separate from commit eligibility. Before publishing a revocation, the iterator
fences admission of revoked output while retaining final staged offsets and valid lease heartbeats
through application acknowledgement. At the next serialized prefetch boundary, a revoked-only
command removes that partition's reader state, metadata cache, traversal, traces, and pending
batches. Retained partitions and hydrated incoming partitions are not replaced by this operation. An
already in-flight mixed-partition read may finish, but its revoked batches are discarded at both
admission boundaries. Idle, capacity-blocked, and retrying workers are notified to apply suspension
without waiting for their ordinary poll deadline. Both capacity waits listen for persistent
reader-command notification permits, including commands queued before a waiter registers; the
periodic capacity retry remains only a fallback for delivery-space notifications. Seeks targeting a
read-fenced partition are rejected at both the direct-reader and worker boundaries, so they cannot
recreate suspended reader state while final commits remain eligible. Shared queued records, staged
offsets, and source provenance reset only when the serialized reader accepts the seek. A rejected
seek, including one fenced after it was queued, leaves final progress unchanged.

Local read epochs keep delayed commands and assignment completion from clearing a newer fence or
suspending a later reacquisition; they do not replace durable lease-generation fencing. Reader
assignment succeeds before its applicable fences are retired. An assignment unrelated to a held
revocation cannot retire that revocation's read fences or reactivate its reader. A heartbeat that
reports another lost lease selectively suspends only that partition and removes it from the saved
replacement assignment, preserving retained reader state and incoming durable hydration. Removal
also clears unread delivery and worker-pending output so a rapid same-member reacquisition cannot
replay speculative batches ahead of recovery. A failed final release leaves delivery and reads
fenced. Application acknowledgement still precedes lease release, and a reacquired partition starts
from durable claimed progress, never the predecessor's speculative cursor.

Orderly release stores a `graceful_release_ts` marker while expiring the lease. The next claimant
can distinguish a graceful handoff from an expiry takeover. On shutdown, a consumer best-effort
releases owned leases, deregisters membership, and conditionally releases its planner lease so a
remaining member can elect promptly without discarding the accepted plan.

## State Endpoint

The consumer keeps an immutable local state snapshot in memory. Reading it does not wait for
metadata, blobs, leases, or membership operations. When an embedding application mounts
`ConsumerDiagnostics::admin_router`, its `/state` route returns that snapshot and a fresh, strongly
consistent lease-table query bounded to one second. A lookup failure is reported without hiding
local state.

The response joins retained leases with the last valid assignment plan, so it includes planned but
unleased partitions and other members' ownership, generation, heartbeat, and committed cursor. Plan
and local/lease partition rows include the logical partition ID. The plan reports its topology and
groups virtual partition IDs and distinct planned member IDs under `logical_partitions`; `colocated`
is false when a group spans multiple planned members. These groupings describe desired ownership,
not a guarantee that leases have converged.

## Invariants

- A valid assignment plan covers each configured virtual partition exactly once.
- Consumer lease generation fences stale ownership, renewals, and cursor commits.
- A revoked partition cannot deliver buffered records after revocation begins.
- A replacement recovers from durable progress before relying on the Fast path.

## Appendix: Operational Limits

One DynamoDB assignment-plan item is limited to 400 KB. Groups approaching that size need sharded
or S3-backed plan storage; an S3-backed design also needs consumer IAM read permission. Scheduled
heartbeat and rebalance failures back off independently so a store outage does not produce
concurrent retry storms.

[Design overview](README.md) | [Consumption](consumption.md) | [Reference](reference.md)
