# Offset Allocation

This page defines how records are partitioned, routed, and assigned monotonic sequence numbers.
See [production](production.md) for durable writes and [consumer coordination](consumer-coordination.md)
for consumer ownership.

## Partitions

A topic has a fixed `partition_count` and `num_writers`. A producer maps each record key to a
logical partition:

```
logical_partition_id = hash(record_key) % partition_count
```

`hash(record_key)` is SipHash-1-3 with zero 64-bit keys, a little-endian 64-bit key-length
prefix, and the raw key bytes. This is a stable, architecture-independent partitioning contract.

Each producer deployment has a static `writer_id` in `[0, num_writers)`. It maps a logical
partition to a virtual partition:

```
virtual_partition_id = logical_partition_id + (writer_id * partition_count)
```

Independent writer deployments therefore write separate virtual partitions for the same logical
topic partition. A consumer group covers every virtual partition while each member owns an assigned
subset; grouping by `virtual_partition_id % partition_count` restores logical-partition fan-in. The
producer validates its writer ID against every configured topic at startup. The broker receives
only the resulting virtual partition ID and validates it against the topic configuration.

## Routing And Write Ownership

Broker and producer deployments use an explicit writer ID for their local writer/AZ domain. They
discover only brokers in that domain and never route or fail over to another writer ID.

Producers and brokers derive the same assignment plan from the current local membership. The plan
considers the writer's virtual partitions in canonical order, balances them across live brokers,
and uses rendezvous hashing over `(topic, virtual_partition_id, node_id)` to break ties. With $P$
partitions and $N$ brokers, each broker owns either $floor(P / N)$ or $ceil(P / N)$ partitions.

A stale producer can send to a previous owner. That broker returns `NOT_LEASE_HOLDER`; producers
retry it with membership-aware backoff. An empty local membership has no route and never falls
back to another writer/AZ.

On membership change, a broker removes moved partitions from write admission, drains accepted work,
then releases their leases. It does not reacquire a returning partition until an earlier release
reaches a terminal outcome.

## Producer Leases

A producer-partition lease is keyed by topic and virtual partition. Only its active holder can
accept writes or reserve sequences. Brokers renew leases before expiry and drain accepted work
before gracefully releasing a moved or shutting-down partition.

Lease duration and heartbeat interval are broker configuration. The heartbeat interval must be
shorter than the lease duration. Longer settings reduce DynamoDB renewal traffic but extend
recovery after an ungraceful broker loss.

## Sequence Ranges And Cursors

The broker assigns every accepted batch an inclusive sequence range `[S, E]` within its virtual
partition. A batch with $R$ records uses $E = S + R - 1$. Sequences are monotonic only within one
virtual partition and do not provide a global order.

Sequence allocation uses a Hi-Lo reservation. The durable lease stores a high-water mark; the
broker conditionally advances it to reserve a block, then serves contiguous batch ranges from that
block in memory. A later holder reserves only above the durable high-water mark. Unused values
from a crash or ownership transfer can create gaps, but reservations never overlap.

A consumer cursor is the greatest individual sequence offset processed for one virtual partition;
it can therefore fall within a batch's inclusive range `[S, E]`. A batch is fully consumed and is
skipped when $E \le cursor$. When $S \le cursor < E$, the consumer discards offsets through `cursor`
and delivers the remaining suffix `[cursor + 1, E]`; after processing a batch, it advances the
cursor monotonically to $E$. This makes replay and normal rescans safe under the
monotonic-sequence invariant.

## Invariants

- A broker holds the producer-partition lease before accepting a batch or reserving sequences.
- Hi-Lo reservations do not overlap for a lease key.
- For any two accepted batches in one virtual partition, if `[S_1, E_1]` precedes `[S_2, E_2]`,
  then $S_2 > E_1$: ranges are non-overlapping and monotonically increasing.
- Sequence gaps are possible; sequence reuse is not.

## Appendix: Allocation Diagnostics

`sequence_reservation_size` is the base reservation target. A broker can grow its process-local
target when traffic consumes reservations faster than lease maintenance can refill them. This
reduces foreground lease-store writes but does not change the sequence contract. The broker exposes
aggregate reservation counts, records, failures, and latency; a sustained reservation-to-record
ratio above roughly one divided by the reservation size usually indicates allocation waste from
ownership churn, restarts, or small batches.

[Design overview](README.md) | [Production](production.md) | [Storage](storage.md)
