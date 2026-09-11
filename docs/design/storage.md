# Storage

`blob-stream` separates immutable record payloads from their mutable metadata and lease state.
See [production](production.md) for publication ordering and [infrastructure setup](../infrastructure.md)
for provisioning.

## Segment Blobs

Segments use the blob-store abstraction, backed by S3 in production and an in-memory store in
tests. Every object uses this key shape:

```
<optional-prefix>/shared/<window_start_unix_seconds>/<snowflake_id>.<zst|bin>
```

The extension identifies zstd or uncompressed stored batches. An object is a concatenation of
individually serialized and compressed `StoredRecordBatch` values, so consumers can range-read
only the selected batch bytes.

One object can contain sections for topics with different retention periods. Operators must keep
the S3 lifecycle rule for `shared/` at least as long as the maximum topic retention plus the
metadata TTL buffer. The service does not manage S3 lifecycle expiration.

## Segment IDs

Each segment receives a **Sonyflake ID**, a 64-bit identifier allocated by the broker while
building the segment for persistence. The ID encodes its generation time in 10 ms buckets plus
machine and sequence components for uniqueness. Numeric order, and the fixed-width decimal form
used in storage, follow the ID's time component; IDs from the same time bucket have no
cross-partition record-order meaning.

The Sonyflake ID identifies a segment object and orders metadata rows within a topic window. It
does not replace per-virtual-partition sequence ranges, which determine record ordering and
consumer cursor progress. Consumers also convert time-based scan floors into the lowest eligible
Sonyflake ID; see [consumption](consumption.md#sonyflake-time-bounds-and-clock-synchronization).

## Segment Metadata

The segment-metadata table, typically named `blob_segments`, stores one row for each topic section
in an uploaded segment object. Its DynamoDB key is:

```
pk = "<topic>#<window_start_unix_seconds>"
sk = <fixed-width, lexicographically sortable Sonyflake ID>
```

The row contains its key, optional TTL, and a binary `segment_metadata_v1` protobuf. The protobuf
contains the immutable blob key, creation and publication times, compression, and one index entry
per virtual partition. An index entry records the byte range, sequence range, and stored payload
size for that partition batch. A segment contains exactly one stored batch per virtual partition.
One blob can therefore have one metadata row per topic section.

Metadata is written after blob upload. Metadata TTL is derived from the topic retention setting
and configured DynamoDB TTL buffer. A missing blob referenced by valid metadata is therefore a
retention or storage-durability violation, not an ordinary eventual-consistency outcome.

## Metadata And Lease Tables

Production metadata uses four DynamoDB tables:

| Domain | Key | Responsibility |
| --- | --- | --- |
| `blob_segments` | topic-window and Sonyflake ID | Segment indexes used by consumers. |
| `producer_partition_leases` | topic and virtual partition | Broker write ownership and Hi-Lo reservations. |
| `consumer_group_leases` | topic-group and virtual partition | Consumer ownership, generation fencing, and committed cursors. |
| `consumer_group_membership` | topic-group and member ID | Member liveness and assignment-plan state. |

Lease rows and membership rows use expiry plus a TTL buffer. DynamoDB TTL deletion is
asynchronous, so reads also compare the recorded lease expiry. Consumer-group lease rows retain
their committed cursor only until their derived TTL.

Assignment plans and planner leases share the membership table under reserved control keys.

## Storage Invariants

- Blobs are immutable and metadata points to a complete uploaded blob.
- Segment metadata represents byte and sequence ranges for the indexed virtual partitions.
- Service-managed DynamoDB TTL and operator-managed S3 lifecycle must remain aligned.
- Consumers treat valid metadata with a missing blob as data loss.

[Design overview](README.md) | [Production](production.md) | [Consumption](consumption.md)
