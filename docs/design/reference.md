# Design Reference

The [protobuf configuration schema](../../blob-stream-proto/proto/blobstream/v1/config.proto) is
the authoritative field-level reference. This page collects important defaults, source-code entry
points, and operational assumptions.

## Important Defaults

| Setting | Default |
| --- | --- |
| Broker flush bytes | 64 MiB raw buffered payload per virtual partition |
| Broker flush delay | 1 second |
| Broker sequence reservation | 10,000 base sequence values per virtual partition |
| Broker segment compression | zstd, level 3 |
| Broker metadata cache | 64 MiB; 250 ms coalescing window; 5-second request deadline |
| Broker blob cache | 30-second request deadline; 10-second idle retention when no override is configured |
| Producer batch records | 10,000 |
| Producer batch payload bytes | 1 MiB |
| Producer flush delay | 200 ms |
| Producer retry deadline | 30 seconds |
| Topic metadata window | 300 seconds |
| Broker metadata publication deadline | 15 seconds |
| Consumer `max_clock_skew` | 10 ms when unset |
| Consumer metadata reads | Strongly consistent |
| Consumer eventual-read visibility delay | 2 seconds when eventual reads are configured and no delay is set |
| Consumer recovery slice | Up to 32 metadata windows per pass |
| Consumer prefetch target | 64 MiB |
| Consumer lease duration | 30 seconds |
| Consumer heartbeat and rebalance intervals | 10 seconds |

## Implementation Map

- Protocol and configuration: [blob-stream-proto](../../blob-stream-proto/proto/blobstream/v1)
- Partitions, sequence ranges, cursors, windows, and Sonyflake IDs:
  [blob-stream-types](../../blob-stream-types/src/lib.rs)
- Membership and rendezvous owner selection:
  [blob-stream-broker-discovery](../../blob-stream-broker-discovery/src/lib.rs)
- Broker write engine and flushing: [blob-stream-broker](../../blob-stream-broker/src/write)
- Blob and metadata stores: [blob-stream-blob-store](../../blob-stream-blob-store) and
  [blob-stream-metadata-store](../../blob-stream-metadata-store)
- Producer batching and retries: [blob-stream-producer](../../blob-stream-producer/src/producer.rs)
- Consumer scans, coordination, and iteration: [blob-stream-consumer](../../blob-stream-consumer/src)
- End-to-end fault coverage: [blob-stream-integration-tests](../../blob-stream-integration-tests)

## Operational Assumptions And Accepted Risks

Consumer bootstrap requires broker discovery. Every metadata and blob-range read uses the selected
local broker. Eventual `Tail` and `FullRecovery` metadata requests use separate retained caches;
strong metadata reads bypass retained coverage. Direct storage is the fallback for an unavailable or
unusable broker response. See [Consumption](consumption.md) for the read protocol.

### Clock Synchronization

Fast scans and first-window checkpoint recovery require a proven, monitored bound on pairwise broker
and consumer clock offset. Configure `ConsumerReadConfig.max_clock_skew` to that deployment bound;
its 10 ms default is not a generic NTP guarantee. See
[Consumption](consumption.md#sonyflake-time-bounds-and-clock-synchronization) for the time-to-ID
bound and its consequences.

### Eventually Consistent Metadata Reads

Metadata reads are strongly consistent unless `eventual_metadata_reads` is configured. Eventual
mode's visibility delay is a best-effort margin for ordinary replica lag, not a bounded DynamoDB
replication guarantee. Fast and checkpoint-overlap reads can miss metadata that becomes visible
outside their bounded horizon. Use eventual mode only when that risk is acceptable.

### Unfenced Stale-Writer Publication

Producer leases protect write admission and sequence reservations. Without `fenced_metadata_writes`,
a paused or partitioned former holder can publish metadata after a successor has published later
sequences; a consumer can advance past that late range and never recover it. Liveness probes,
deadlines, and graceful handoff reduce the likelihood but do not provide a publication fence.

Deployments that require a stronger at-least-once guarantee across writer failures must enable
transactional metadata fencing. Pair it with strong metadata reads when replica staleness is also
unacceptable. Fencing increases DynamoDB write cost because each metadata transaction condition
checks its producing lease keys.

[Design overview](README.md) | [Operations guide](../operations.md) | [FAQ](../faq.md)
