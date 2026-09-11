# blob-stream System Design

`blob-stream` is a brokered streaming system for high-volume workloads. It favors
total cost of ownership over ultra-low latency: brokers collect producer batches, persist
compressed segment blobs, publish metadata indexes, and serve consumer metadata and blob reads.

## Guarantees And Boundaries

- Delivery is at least once. Producers can retry ambiguous requests, and consumers can replay
  records after an interrupted commit or rebalance. Applications must tolerate duplicates.
- Ordering and sequence progress are per virtual partition. There is no global or cross-partition
  ordering guarantee.
- Brokers have no local durable storage. DynamoDB stores metadata and leases; blob storage holds
  immutable segment payloads. Consumer bootstrap requires broker discovery. All metadata queries
  and blob-range reads route through local brokers; eventual `Tail` and `FullRecovery` metadata
  requests can use retained cache coverage. Direct storage is the fallback for an unusable broker
  response.
- New consumer groups begin in the current metadata window. Resumed partitions recover from their
  committed source checkpoint while it remains within retention.
- Consumers and brokers require a shared, accurate clock. See the [FAQ](../faq.md#what-time-source-is-required-to-operate-blob-stream-correctly).
- The service does not provide compaction, built-in authorization, or an idempotent producer
  protocol.

## Design Topics

- [Offset allocation](offset-allocation.md): partitions, routing, producer leases, sequences, and
  cursors.
- [Production](production.md): batching, admission, durable publication, acknowledgements, and
  retries.
- [Storage](storage.md): segment objects, metadata indexes, DynamoDB tables, and retention.
- [Consumption](consumption.md): recovery, metadata and blob reads, visibility, and consistency.
- [Consumer coordination](consumer-coordination.md): membership, plans, leases, commits, and
  rebalances.
- [Reference](reference.md): important defaults, implementation map, and operational risks.
- [Read-path scenarios](appendix-read-scenarios.md): detailed explanatory examples.

## Related Guides

- [Repository overview](../../README.md)
- [Infrastructure setup](../infrastructure.md)
- [Operations guide](../operations.md)
- [Producer and consumer integration](../integration.md)
- [Metrics reference](../metrics.md)
- [TLA+ model](../../tla/README.md)
