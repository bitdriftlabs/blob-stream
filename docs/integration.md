# Producer And Consumer Integration

This guide describes the Rust library entry points and the configuration contracts around them.
For the field-level schema, defaults, and validation rules, use
[`blobstream/v1/config.proto`](../blob-stream-proto/proto/blobstream/v1/config.proto).

## Shared Topic Contract

The broker, every producer, and every consumer for a topic must agree on its `TopicConfig`:
`name`, `partition_count`, `num_writers`, `retention_days`, and
`max_metadata_publication_lag_ms`. Treat a change to partition count or writer count as a
coordinated deployment change, not an independent client setting.

A producer's `writer_id` identifies its writer/AZ domain. It must be less than `num_writers`, and
its broker discovery view must contain only brokers serving that writer ID. Consumers do not choose
a writer ID: their bootstrap `TopicConfig` describes the complete topic partition space.

## Producer Library

Use `ProducerClientImpl::from_runtime_config` for the standard gRPC producer. It validates the
runtime configuration, starts broker discovery, and starts the background batch-flush task. Supply
a `bd_server_stats::stats::Scope` from the embedding application's collector so producer metrics
are exported with the rest of the application telemetry.

```rust
use bd_server_stats::stats::Collector;
use blob_stream_producer::{
  ProducerClient,
  ProducerClientImpl,
  ProducerRecord,
  ProducerRuntimeConfig,
};

let metrics_scope = Collector::default().scope("telemetry_ingester");
let producer = ProducerClientImpl::from_runtime_config(runtime, metrics_scope).await?;

let acknowledgement = producer
  .produce(ProducerRecord::new(
    "telemetry",
    record_key,
    payload,
    event_timestamp_ms,
  ))
  .await?;
producer.flush().await?;
```

`produce` batches records by topic and virtual partition, then resolves when the broker acknowledges
that batch. Retrying an ambiguous request can deliver a record more than once, so downstream
processing must remain idempotent. Call `flush` before a controlled shutdown to wait for buffered
records; the producer client itself owns a background flush task.

`ProducerRuntimeConfig` contains:

| Surface | Responsibility |
| --- | --- |
| `producer` | Writer ID, batch record and byte limits, flush delay, request compression, request concurrency, and retry timing. |
| `discovery` | Static broker nodes or Kubernetes Service endpoint discovery. |
| `topics` | The shared topic contract used to validate and route records. |

The producer returns an acknowledgement with the selected virtual partition and attempt count.
Persistent `NOT_LEASE_HOLDER`, overload, or transport errors are retried until
`retry_deadline_ms` expires. See [Metrics](metrics.md) and the broker ownership runbook in
[Operations](operations.md) before changing retry or batch settings to hide an availability issue.

The optional `ProducerClient::diagnostics()` handle exposes the selected broker route, known
membership, per-partition buffers, and bounded retry samples. Mount
`diagnostics.admin_router()` under an application-owned authenticated prefix to expose
`GET <prefix>/state`.

## Consumer Library

Use `ConsumerConfigFactory::build_iterator_from_proto_config` for the standard production
bootstrap. It validates the complete bootstrap configuration and constructs the blob store,
metadata store, consumer coordinator, reader, and iterator. The caller owns the iterator
lifecycle:

```rust
use bd_server_stats::stats::Collector;
use blob_stream_consumer::ConsumerConfigFactory;
use blob_stream_consumer::iterator::{ConsumerIterator, NextResult};

let metrics_scope = Collector::default().scope("telemetry_worker");
let mut iterator = ConsumerConfigFactory::build_iterator_from_proto_config(
  bootstrap,
  metrics_scope,
  feature_flags,
)
.await?;
iterator.start()?;

match iterator.next().await? {
  NextResult::Record(record) => {
    process(record.record).await?;
    iterator.store_offset(record.virtual_partition_id, record.offset)?;
    iterator.commit().await?;
  },
  NextResult::Revoked(revoked) => {
    drain_in_flight_work(revoked.partitions()).await?;
    revoked.complete().await;
  },
}
```

After successful application processing, stage the record offset with `store_offset` and persist
it with `commit`. When `next` returns `Revoked`, finish work for those partitions before calling
`complete`; otherwise a new owner can replay records that were still in flight. Call `shutdown` to
perform the final commit and release membership and leases during controlled termination.

`ConsumerIteratorBootstrapConfig` contains:

| Surface | Responsibility |
| --- | --- |
| `runtime.read` | Idle polling, visibility delay, clock-skew horizon, prefetch memory, batch-read concurrency, and strong-read setting. |
| `runtime.group` | Topic, consumer-group ID, stable member ID, optional pod ID, lease duration, heartbeat interval, and rebalance interval. |
| `topic` | The shared partition and durable metadata-window contract used to derive the complete virtual partition space. |
| `blob_store` and `metadata_store` | S3/DynamoDB production stores or in-memory local/test stores. |

Feature flags are optional at construction time. Strong metadata reads, prefetch capacity, and
batch-read concurrency update live. Idle polling, lease duration, heartbeat interval, and rebalance
interval are sampled at construction and require rebuilding the iterator to adopt a new value. See
[Infrastructure setup](infrastructure.md) and [Operations](operations.md) for deployment and
consistency effects.

## Consumer Diagnostics Endpoint

`ConsumerIterator::diagnostics()` returns a `ConsumerDiagnostics` handle for the standard
iterator implementation. Mount its router in the embedding application's Axum router; Blob Stream
does not start an HTTP listener for a consumer library.

```rust
use axum::Router;
use blob_stream_consumer::iterator::ConsumerIterator;

let diagnostics = iterator
  .diagnostics()
  .expect("the standard consumer iterator provides diagnostics");
let app = Router::new().nest("/admin/consumer", diagnostics.admin_router());
// GET /admin/consumer/state now returns the consumer state response as JSON.
```

`GET <mounted-prefix>/state` returns local state immediately and independently attempts a
one-second group-lease lookup. The response includes topic and group identity, iterator start
state, accepted assignment plan, owned/active/pending partitions, cursors and committed source
checkpoints, reader mode and last scan details, prefetch occupancy, heartbeat schedule, and
revocation state. `group_lease_observation` is either a fresh group-wide lease view or
`lookup_failed` with the metadata-store error; a lookup failure does not erase the useful local
snapshot.

The response can reveal topic names, member IDs, assignment topology, and offsets. Protect this
route with the same network and authentication policy as other application admin endpoints.
