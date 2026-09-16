# Production

This page describes the path from a producer call to a durable acknowledgement. See
[offset allocation](offset-allocation.md) for partition routing, leases, and sequences, and
[storage](storage.md) for the durable layout.

## Producer Batching

The producer buffers records independently for each `(topic, virtual_partition_id)`. It seals a
batch when the configured record-count or payload-byte limit is reached, or on its flush cadence.
Sealed batches are grouped by their selected broker and sent with `ProduceBatches`.

`ProduceBatches` returns an ordered result for every submitted partition batch. A message has a
16 MiB decoded-protobuf limit, with the fixed five-byte gRPC envelope also accepted. The producer
uses this RPC for both initial sends and retries; a retry contains one `ProduceBatchRequest`.

Producer submission order is not a delivery-order guarantee, including within one virtual
partition. The broker assigns sequences in the order it accepts batches, and preserves that order
in durable metadata.

## Broker Admission And Flush

The broker validates the topic and virtual partition, confirms ownership through its
producer-partition lease, and reserves sequence space as needed. It rejects new batches with
`OVERLOADED` when memory admission exceeds the configured threshold.

Accepted batches are coalesced by virtual partition into a flush plan. A plan flushes on the
configured byte or delay threshold; time-triggered plans can include available peer partitions to
create larger segment objects. Each partition batch is serialized and compressed independently.
The broker concatenates these batches into bounded immutable segment objects, uploads every
object, and then writes one metadata row for each topic section.

An object can contain sections for multiple topics. A virtual partition can start a later flush
epoch while an earlier plan is uploading, but metadata for that partition is published in sequence
order. A plan starts metadata publication only after all its blob uploads have reached a terminal
result.

## Durability And Acknowledgement

The broker writes segment metadata only after its blob upload succeeds. It returns `OK` only after
both the blob and the relevant metadata row are durable. An acknowledgement therefore represents
durable segment metadata, not buffered memory.

A failed metadata row remains retryable. Because the payload blob may already exist, retrying an
ambiguous producer request can deliver a duplicate batch. This is part of the at-least-once
contract.

With `fenced_metadata_writes` enabled, each metadata row is transactionally conditioned on the
current producer-lease holder, epoch, session ID, and expiry. A former process cannot publish
after a successor takes the lease. The DynamoDB transaction has room for one metadata write and
at most 99 lease checks, so the broker splits larger publication sets. A lost fence after upload
can leave an unreferenced blob but cannot publish its metadata.

Without fenced metadata writes, a stalled former holder can publish metadata after a successor has
published later sequences. See [reference](reference.md#operational-assumptions-and-accepted-risks)
for this accepted risk.

## Statuses And Retries

| Status | Meaning | Producer handling |
| --- | --- | --- |
| `OK` | Blob and metadata were durable. | Complete the batch. |
| `NOT_LEASE_HOLDER` | The broker does not currently own the virtual partition. | Retry with membership-aware backoff. |
| `UNKNOWN_TOPIC` | The topic is not configured. | Fail permanently. |
| `BAD_REQUEST` | The batch or virtual partition is invalid. | Fail permanently. |
| `OVERLOADED` | The broker rejected admission or encountered a transient write-path failure. | Retry with capped exponential backoff. |

Transport failures are also retried until the producer retry deadline. Each RPC and retry delay is
limited by the remaining deadline. A timeout is ambiguous: the broker can still persist its
already accepted batch, so the producer may retry and applications must tolerate duplicates.

## Invariants

- Blob upload precedes metadata persistence.
- A producer acknowledgement follows both operations.
- Metadata for one virtual partition becomes visible in sequence order.
- Concurrent plans for different partitions do not create a cross-partition ordering guarantee.

## Appendix: Flush Control Details

Live feature flags can change the broker's byte threshold, maximum flush delay, adaptive-delay
enablement, adaptive-delay floor, and maximum segment size. Watched updates immediately rearm
deadlines for buffered, unselected partitions; selected plans retain their prior values. The
adaptive controller is broker-wide: three matching outcomes can shorten the next delay after split
segments or recover it after unsplit plans. It changes batching efficiency, not acknowledgement or
ordering semantics.

[Design overview](README.md) | [Offset allocation](offset-allocation.md) | [Storage](storage.md)
