# blob-stream

`blob-stream` is a Kafka-like streaming system optimized for high-throughput telemetry with lower
cost priority over ultra-low latency.

The system uses:
- Brokered writes with lease fencing
- Blob storage for payload segments (S3 in production)
- Key-value metadata/indexes (DynamoDB in production)
- Pull-based consumers with cursor progression per virtual partition

Delivery semantics are at-least-once. Duplicate records are possible on retries.

## Project layout

- `blob-stream-broker/` (`blob-stream` crate): broker binary and write path
- `blob-stream-producer/`: producer client library
- `blob-stream-consumer/`: consumer iterator/coordinator library
- `blob-stream-broker-discovery/`: static and Kubernetes service discovery
- `blob-stream-blob-store/`: blob store trait + in-memory/S3 backends
- `blob-stream-metadata-store/`: metadata + lease store traits + in-memory/Dynamo backends
- `blob-stream-proto/`: protobuf API and config schemas
- `blob-stream-types/`: shared wire/storage types
- `blob-stream-integration-tests/`: end-to-end and deterministic fault-injection tests

## System model at a glance

- Producer hashes each record key into a logical partition.
- Producer maps logical partition to a virtual partition via `writer_id`.
- Producer sends `ProduceBatch` requests to the broker selected by discovery + rendezvous hashing.
- Broker acquires/heartbeats producer-partition leases and reserves monotonic seq ranges (Hi-Lo).
- Broker flushes compressed segment blobs and writes segment metadata indexes.
- Consumer scans metadata windows, fetches blob byte ranges, decodes records, and commits cursors.

## Prerequisites

- Rust toolchain (edition 2024)
- Docker + Docker Compose (for local dependencies)
- Optional for local cloud emulation commands:
  - AWS CLI

## Build and test

From repo root:

```bash
cargo build --workspace
cargo clippy --workspace --bins --examples --tests -- --no-deps
cargo nextest run
```

Integration tests for this project specifically:

```bash
RUST_LOG=off cargo nextest run -p blob-stream-integration-tests
```

## Local infrastructure (S3 + Dynamo emulation)

Start local dependencies:

```bash
docker compose up -d
```

This starts:
- DynamoDB Local on `http://localhost:8000`
- LocalStack S3 on `http://localhost:4566`

## Broker service setup

The broker binary is in `blob-stream-broker` (crate name `blob-stream`).

### 1) Create runtime config

Create a YAML config file, for example `configs/broker.local.yaml`:

```yaml
broker:
  bind_addr: "0.0.0.0:8080"
  flush_max_bytes: 67108864
  flush_max_delay_ms: 1000
  node_identity:
    hostname: {}
  discovery:
    static:
      nodes:
        - node_id: "broker-a"
          address: "127.0.0.1:8080"

topics:
  - name: "telemetry"
    partition_count: 128
    num_writers: 1
    retention_days: 7

blob_store:
  s3:
    bucket: "blob-stream-local"
    prefix: "blob-stream/"
    region: "us-east-1"
    endpoint: "http://localhost:4566"

metadata_store:
  dynamo:
    segment_metadata_table_name: "blob_segments"
    producer_partition_lease_table_name: "producer_partition_leases"
    consumer_group_lease_table_name: "consumer_group_leases"
    consumer_group_membership_table_name: "consumer_group_membership"
    region: "us-east-1"
    endpoint: "http://localhost:8000"
```

Notes:
- The broker config loader accepts `.json`, `.yaml`, and `.yml` files.
- `bind_addr`, blob-store backend, metadata-store backend, and topic list are required.
- `/metrics` is exposed on the same HTTP listener.

### 2) Run broker

```bash
cargo run -p blob-stream -- --config configs/broker.local.yaml
```

Or with environment variable:

```bash
BLOB_STREAM_CONFIG=configs/broker.local.yaml cargo run -p blob-stream -- --config "$BLOB_STREAM_CONFIG"
```

## Producer library usage

The producer API is in `blob-stream-producer`.

### Runtime config fields

`ProducerRuntimeConfig` includes:
- `producer`:
  - `writer_id`
  - `max_batch_records`
  - `max_batch_bytes`
  - `flush_max_delay_ms`
  - `max_retries`
  - `retry_base_delay_ms`
  - `retry_max_delay_ms`
  - `connect_timeout_ms`
  - `request_timeout_ms`
  - `max_request_concurrency`
  - `compression` (`PRODUCER_COMPRESSION_NONE` or `PRODUCER_COMPRESSION_SNAPPY`)
- `discovery`: static nodes or `k8s_service`
- `topics`: repeated `TopicConfig`
- `stats_scope` (optional)

### Example

```rust
use anyhow::Result;
use blob_stream_producer::{ProducerClient, ProducerClientImpl, ProducerRecord, ProducerRuntimeConfig};

#[tokio::main]
async fn main() -> Result<()> {
  let mut runtime = ProducerRuntimeConfig::new();

  let mut producer = blob_stream_producer::ProducerConfig::new();
  producer.writer_id = Some(0);
  producer.max_batch_records = Some(1000);
  producer.max_batch_bytes = Some(1_048_576);
  producer.flush_max_delay_ms = Some(200);
  producer.max_retries = Some(5);
  producer.retry_base_delay_ms = Some(25);
  producer.retry_max_delay_ms = Some(1000);
  producer.connect_timeout_ms = Some(2000);
  producer.request_timeout_ms = Some(5000);
  producer.max_request_concurrency = Some(64);
  producer.compression = Some(
    blob_stream_producer::ProducerCompression::PRODUCER_COMPRESSION_SNAPPY.into(),
  );

  let mut discovery = blob_stream_producer::ProducerDiscoveryConfig::new();
  {
    let mut static_cfg = blob_stream_proto::protos::blobstream::v1::config::StaticBrokerDiscoveryConfig::new();
    let mut node = blob_stream_producer::ProducerNodeConfig::new();
    node.node_id = "broker-a".into();
    node.address = "127.0.0.1:8080".into();
    static_cfg.nodes.push(node);
    discovery.static_ = Some(static_cfg).into();
  }

  let mut topic = blob_stream_producer::ProducerTopicConfig::new();
  topic.name = "telemetry".into();
  topic.partition_count = 128;
  topic.num_writers = 1;
  topic.retention_days = 7;

  runtime.producer = Some(producer).into();
  runtime.discovery = Some(discovery).into();
  runtime.topics.push(topic);

  let producer = ProducerClientImpl::from_runtime_config(runtime).await?;

  let ack = producer
    .produce(ProducerRecord::new(
      "telemetry",
      b"device-123".to_vec(),
      b"payload".to_vec(),
      1_700_000_000_000,
    ))
    .await?;

  println!("acked partition={}, attempts={}", ack.virtual_partition_id, ack.attempts);
  producer.flush().await?;
  Ok(())
}
```

## Consumer library usage

The consumer API is in `blob-stream-consumer`.

The primary production path is a single-proto bootstrap
(`ConsumerIteratorBootstrapConfig`), which builds the required stores and iterator for you. For
advanced deployments, you can still inject storage and coordination dependencies manually.

Consumer group membership for this bootstrap path is derived dynamically from the consumer-group
membership store; there is no static member list in production config.

### Runtime config fields

- `read`:
  - `topic`
  - `window_size_seconds`
  - `lookback_windows`
- `group`:
  - `topic`
  - `group_id`
  - `member_id`
  - `lease_duration_ms`
  - `heartbeat_interval_ms`
  - `rebalance_interval_ms`
- `stats_scope` (optional)

### Example (single-proto bootstrap)

```rust
use anyhow::Result;
use blob_stream_consumer::{
  ConsumerConfigFactory,
  ConsumerIterator,
  NextResult,
};
use blob_stream_proto::protos::blobstream::v1::config::{
  BlobStoreConfig,
  ConsumerGroupConfig,
  ConsumerIteratorBootstrapConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
  DynamoMetadataStoreConfig,
  MetadataStoreConfig,
  S3BlobStoreConfig,
  TopicConfig,
  blob_store_config,
  metadata_store_config,
};

#[tokio::main]
async fn main() -> Result<()> {
  let mut runtime = ConsumerRuntimeConfig::new();
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".into();
  read.window_size_seconds = Some(300);
  read.lookback_windows = Some(3);

  let mut group = ConsumerGroupConfig::new();
  group.topic = "telemetry".into();
  group.group_id = "group-a".into();
  group.member_id = "member-a".into();
  group.lease_duration_ms = Some(30_000);
  group.heartbeat_interval_ms = Some(10_000);
  group.rebalance_interval_ms = Some(10_000);

  runtime.read = Some(read).into();
  runtime.group = Some(group).into();

  let mut topic = TopicConfig::new();
  topic.name = "telemetry".into();
  topic.partition_count = 128;
  topic.num_writers = 1;
  topic.retention_days = 7;

  let mut s3 = S3BlobStoreConfig::new();
  s3.bucket = "blob-stream-prod".into();
  s3.prefix = "blob-stream/".into();
  s3.region = "us-east-1".into();

  let mut blob_store = BlobStoreConfig::new();
  blob_store.backend = Some(blob_store_config::Backend::S3(s3));

  let mut dynamo = DynamoMetadataStoreConfig::new();
  dynamo.region = "us-east-1".into();
  dynamo.segment_metadata_table_name = "blob_segments".into();
  dynamo.consumer_group_lease_table_name = "consumer_group_leases".into();
  dynamo.consumer_group_membership_table_name = "consumer_group_membership".into();

  let mut metadata_store = MetadataStoreConfig::new();
  metadata_store.backend = Some(metadata_store_config::Backend::Dynamo(dynamo));

  let mut bootstrap = ConsumerIteratorBootstrapConfig::new();
  bootstrap.runtime = Some(runtime).into();
  bootstrap.topic = Some(topic).into();
  bootstrap.blob_store = Some(blob_store).into();
  bootstrap.metadata_store = Some(metadata_store).into();

  let mut iter = ConsumerConfigFactory::build_iterator_from_proto_config(bootstrap).await?;

  iter.start()?;

  loop {
    match iter.next().await? {
      NextResult::Batch(batch) => {
        // Process records, then store and commit offsets.
        iter.store_offset(batch.virtual_partition_id, batch.seq_range.end)?;
        let _report = iter.commit().await?;
      },
      NextResult::Revoked(revoked) => {
        // Stop work on revoked partitions and acknowledge completion.
        revoked.complete().await;
      },
    }
  }
}
```

For advanced/custom deployments (custom discovery or custom stores), use
`ConsumerConfigFactory::build_iterator(...)` (typed bootstrap config) or
`ConsumerIteratorImpl::from_runtime_config(...)` (full manual wiring).

## DynamoDB tables required

Production deployments should provision the following logical tables.

### 1) `blob_segments`

Purpose:
- Segment metadata index used by consumers for window scans.

Keys:
- Partition key: `pk` = `"<topic>#<window_start_unix_seconds>"`
- Sort key: `sk` = lexicographic snowflake id string

Representative attributes:
- `topic`, `window_start_ts`, `blob_key`, `segment_index`, `compression`, `record_count`,
  `min_event_ts_ms`, `max_event_ts_ms`, `checksum`, `created_ts_ms`

### 2) `producer_partition_leases`

Purpose:
- Lease fencing and sequence reservation for broker writes.

Keys:
- Partition key: `pk` = `"<topic>#<writer_id>#<virtual_partition_id>"`

Representative attributes:
- `holder_id`, `lease_expiration_ts_ms`, `max_allocated_seq`, `topic`, `writer_id`,
  `virtual_partition_id`

### 3) `consumer_group_leases`

Purpose:
- Consumer ownership, heartbeats, generation fencing, and committed cursors.

Keys:
- Partition key: `pk` = `"<topic>#<group_id>"`
- Sort key: `sk` = `"<virtual_partition_id>"`

Representative attributes:
- `owner_id`, `generation`, `lease_expiry_ts`, `last_heartbeat_ts`, `committed_cursor`,
  `committed_ts`, `topic`, `group_id`, `virtual_partition_id`

### Provisioning note

Current implementation uses the three logical Dynamo data domains listed above (`blob_segments`,
`producer_partition_leases`, and `consumer_group_leases`). Consumer-group membership is stored in
an additional `consumer_group_membership` table. Configure explicit table names in
`metadata_store.dynamo`:
- `segment_metadata_table_name`
- `producer_partition_lease_table_name`
- `consumer_group_lease_table_name`
- `consumer_group_membership_table_name`

Context usage:
- Broker context uses `segment_metadata_table_name` +
  `producer_partition_lease_table_name`.
- Consumer context uses `segment_metadata_table_name` +
  `consumer_group_lease_table_name` + `consumer_group_membership_table_name`.

### 4) `consumer_group_membership`

Purpose:
- Consumer member liveness used to compute dynamic rebalance membership.

Keys:
- Partition key: `pk` = `"<topic>#<group_id>"`
- Sort key: `sk` = `"<member_id>"`

Representative attributes:
- `member_id`, `lease_expiry_ts`, `last_heartbeat_ts`, `topic`, `group_id`

## S3 bucket requirements

At minimum, provide:
- A bucket writable/readable by brokers and readable by consumers
- Region matching `blob_store.s3.region`
- Optional prefix from `blob_store.s3.prefix`

Recommended:
- Lifecycle expiration matching retention target (for example 3-7 days)
- Server-side encryption (SSE-S3 or SSE-KMS)
- Access logging for auditability

Object key format produced by broker:
- `<topic>/<window_ts>/<snowflake_id>.zst`

## Kubernetes RBAC for discovery

If using `k8s_service` discovery backend (brokers and/or producers), the workload needs to
watch `Endpoints` for the broker Service in its namespace.

Example namespace-scoped RBAC:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: blob-stream
  namespace: telemetry
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: blob-stream-endpoints-read
  namespace: telemetry
rules:
  - apiGroups: [""]
    resources: ["endpoints"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: blob-stream-endpoints-read
  namespace: telemetry
subjects:
  - kind: ServiceAccount
    name: blob-stream
    namespace: telemetry
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: blob-stream-endpoints-read
```

Notes:
- Apply this to broker pods when broker discovery is `k8s_service`.
- Apply this to producer apps when producer discovery is `k8s_service`.
- Consumers do not require this discovery RBAC unless your application adds k8s-based coordination.

## Observability

Broker exposes Prometheus metrics at:
- `GET /metrics`

Producer and consumer libraries also emit metrics through `bd-server-stats` using their configured
`stats_scope`.

## Development and verification

- Format: `cargo +nightly fmt`
- Lint: `cargo clippy --workspace --bins --examples --tests -- --no-deps`
- Test: `cargo nextest run`
- Integration tests: `cargo nextest run -p blob-stream-integration-tests`

Fault-injection tests are in:
- `blob-stream-integration-tests/tests/fault_injection_test.rs`
