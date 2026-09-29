# Consumer Coordination

This page describes how a consumer group assigns virtual partitions, fences ownership, and
persists progress. See [consumption](consumption.md) for the reader protocol and [storage](storage.md)
for the durable tables.

## Membership And Assignment Plans

Consumers with the same topic and group ID register membership liveness. They share one versioned
assignment plan per topic-group rather than deriving incompatible local sticky maps. A planner
lease elects the member that refreshes or replaces the plan when membership or partition inventory
changes.

The planner preserves existing placements where possible, then moves the minimum required
partitions to balance load. A valid plan names its planner as a member, covers every configured
virtual partition exactly once, and assigns each partition to a plan member. Until a consumer sees
a structurally valid plan, it makes no local desired-ownership decision.

When the `blob_stream_consumer_colocate_logical_partitions` flag is enabled, the planner instead
groups virtual partitions by `virtual_partition_id % TopicConfig.partition_count` and tries to
assign each logical group to one member. It retains the same at-most-one virtual-partition load
difference, preferring the tightest whole-group fit before prior ownership and splitting a group
when no remaining owner can fit it. This mode favors co-location over prior virtual-partition
ownership. Without the flag, the existing cooperative sticky policy remains in effect. The active
planner reads the flag on each rebalance; changing it publishes a new versioned assignment plan even
if membership is unchanged. The plan records the chosen mode so members with different flag
snapshots still follow one shared plan.

Consumers can register an optional stable `pod_id`. When every active member supplies one, the
planner balances aggregate partition load across pods first, then balances each pod's partitions
among its members. Otherwise it uses the flat policy, which balances all members globally. This
supports rolling adoption without a migration. In co-location mode, the planner first tries to keep
whole logical groups on a pod, then on a worker within that pod. Pod balance takes precedence over
worker co-location; a logical group may span pods or workers when their respective virtual-partition
balance requires it.

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

Orderly release stores a `graceful_release_ts` marker while expiring the lease. The next claimant
can distinguish a graceful handoff from an expiry takeover. On shutdown, a consumer best-effort
releases owned leases, deregisters membership, and conditionally releases its planner lease so a
remaining member can elect promptly without discarding the sticky plan.

## State Endpoint

The consumer keeps an immutable local state snapshot in memory. Reading it does not wait for
metadata, blobs, leases, or membership operations. When an embedding application mounts
`ConsumerDiagnostics::admin_router`, its `/state` route returns that snapshot and a fresh, strongly
consistent lease-table query bounded to one second. A lookup failure is reported without hiding
local state.

The response joins retained leases with the last valid assignment plan, so it includes planned but
unleased partitions and other members' ownership, generation, heartbeat, and committed cursor. Plan
and local/lease partition rows include the logical partition ID. The plan reports its
`colocate_logical_partitions` mode and groups virtual partition IDs and distinct planned member IDs
under `logical_partitions`; `colocated` is false when a group spans multiple planned members. These
groupings describe desired ownership, not a guarantee that leases have converged.

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
