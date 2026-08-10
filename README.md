<img width="2926" height="1606" alt="blob-stream" src="https://github.com/user-attachments/assets/18979bb4-3c66-4222-a359-9d98f741a021" />

# blob-stream

`blob-stream` is a Kafka-like streaming system optimized for high-throughput telemetry with lower
cost priority over ultra-low latency.

The system uses:
- Brokered writes with lease fencing
- Blob storage for payload segments (S3 in production)
- Key-value metadata/indexes (DynamoDB in production)
- Pull-based consumers with cursor progression per virtual partition
- Writers allow for no cross AZ data transfer using virtual partitions. Consumers read all topic
  data by consuming all virtual partitions.

Delivery semantics are at-least-once. Duplicate records are possible on retries.

## Project layout

- `blob-stream-broker/` (`blob-stream-broker` crate): broker binary and write path
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

For the implementation-backed architecture, correctness boundaries, and data-flow details, see
[`DESIGN.md`](DESIGN.md). This README focuses on deployment and operator responsibilities.

For build, test, local docs preview, and developer workflows, see `DEVELOPMENT.md`.

For a local S3/DynamoDB-backed correctness stress run with configurable broker, producer, and
consumer counts, see the **Local stress runner** section in `DEVELOPMENT.md`.

## Broker service setup

The broker binary is in `blob-stream-broker` (crate name `blob-stream-broker`).

### 1) Create runtime config

Create a YAML config file, for example `configs/broker.local.yaml`:

```yaml
broker:
  bind_addr: "0.0.0.0:8080"
  flush_max_bytes: 67108864
  flush_max_delay_ms: 1000
  # Base reservation size. The broker adapts upward per active partition after exhaustion.
  sequence_reservation_size: 10000
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
    segment_ttl_buffer_seconds: 3600
    lease_ttl_buffer_seconds: 3600
    region: "us-east-1"
    endpoint: "http://localhost:8000"
```

Notes:
- The broker config loader accepts `.json`, `.yaml`, and `.yml` files.
- `bind_addr`, blob-store backend, metadata-store backend, and topic list are required.
- `/metrics` is exposed on the same HTTP listener.

### 2) Run broker

```bash
cargo run -p blob-stream-broker -- --config configs/broker.local.yaml
```

Or with environment variable:

```bash
BLOB_STREAM_CONFIG=configs/broker.local.yaml cargo run -p blob-stream-broker -- --config "$BLOB_STREAM_CONFIG"
```

## Producer and consumer configuration

Applications use the producer and consumer libraries rather than connecting to storage directly.
Their configuration is defined by
[`blobstream/v1/config.proto`](blob-stream-proto/proto/blobstream/v1/config.proto). Keep the
`TopicConfig` for a topic identical across the broker, every producer, and every consumer:
`name`, `partition_count`, `num_writers`, and retention are a shared contract. A producer's
`writer_id` must be in `[0, num_writers)`; deployments with one writer use `writer_id: 0`.
A broker deployment must also explicitly configure the same local `writer_id`. Its discovery
configuration lists only brokers in that local writer/AZ domain; other writer IDs are not routing
or failover targets.

### Producers

Configure producers with `ProducerRuntimeConfig`:

- `producer` controls batching, retries, request concurrency, compression, and `writer_id`.
- `discovery` selects either a static broker list or `k8s_service` discovery.
- `topics` contains the topic definitions the process is allowed to publish.

Records are keyed. The producer uses the key to select a logical partition, so choose a stable key
when ordering for an entity matters. Producers require network access to brokers. With
`k8s_service` discovery, their ServiceAccount also needs `get`, `list`, and `watch` on core
`Endpoints` in the broker namespace.

### Consumers

Configure consumers with `ConsumerIteratorBootstrapConfig`:

- `runtime.read` selects the topic and metadata scan/prefetch behavior.
- `runtime.group` sets the topic, stable `group_id`, and a unique `member_id` for each running
  instance.
- `topic` repeats the shared `TopicConfig`.
- `blob_store` and `metadata_store` select the S3 and DynamoDB resources used for reads and
  coordination.

Instances with the same `(topic, group_id)` cooperatively share virtual partitions. Use a stable
group ID for one logical consumer application and a unique process or pod identity for
`member_id`; reusing a member ID across concurrently running instances prevents correct group
membership. Consumer offsets are committed to the consumer lease table, not Kafka.

Delivery is at least once. Consumers must tolerate duplicate records, including records replayed
after an interrupted commit or a partition rebalance. Consumers need S3 object read access,
metadata-table query access, and read/write/delete access to the consumer lease and membership
tables. Brokers need S3 object write access, segment-metadata writes, and producer-lease access.

### Consumer IAM permissions

For DynamoDB, a consumer role needs `dynamodb:Query` on the segment metadata and consumer
membership tables, `dynamodb:GetItem`, `dynamodb:UpdateItem`, and `dynamodb:DeleteItem` on the
consumer lease and membership tables, and `dynamodb:TransactWriteItems` plus
`dynamodb:ConditionCheckItem` on the consumer membership table.

Assignment-plan publication atomically verifies the separate, session-fenced planner-lease item
and writes the plan item. DynamoDB therefore authorizes the conditional transaction as both
`TransactWriteItems` and `ConditionCheckItem`; omitting the latter causes plan publication to
fail with `AccessDeniedException`. With this two-item layout, the transaction is required to
prevent a planner whose lease has expired or been replaced from publishing a plan.

The following policy fragment grants the DynamoDB permissions used by a consumer. Substitute the
configured table names, region, and account ID. S3 `GetObject` access for the configured blob
prefix is also required, but is omitted here because its bucket and prefix are deployment-specific.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:Query"],
      "Resource": [
        "arn:aws:dynamodb:<region>:<account-id>:table/<segment-metadata-table>",
        "arn:aws:dynamodb:<region>:<account-id>:table/<consumer-membership-table>"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:<region>:<account-id>:table/<consumer-lease-table>",
        "arn:aws:dynamodb:<region>:<account-id>:table/<consumer-membership-table>"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:TransactWriteItems",
        "dynamodb:ConditionCheckItem"
      ],
      "Resource": "arn:aws:dynamodb:<region>:<account-id>:table/<consumer-membership-table>"
    }
  ]
}
```

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
  `min_event_ts_ms`, `max_event_ts_ms`, `checksum`, `created_ts_ms`, `ttl_epoch_seconds`

### 2) `producer_partition_leases`

Purpose:
- Lease fencing and sequence reservation for broker writes.

Keys:
- Partition key: `pk` = `"<topic>#<virtual_partition_id>"`

Representative attributes:
- `holder_id`, `lease_expiration_ts_ms`, `max_allocated_seq`, `topic`, `virtual_partition_id`,
  `ttl_epoch_seconds`

### 3) `consumer_group_leases`

Purpose:
- Consumer ownership, heartbeats, generation fencing, and committed cursors.

Keys:
- Partition key: `pk` = `"<topic>#<group_id>"`
- Sort key: `sk` = `"<virtual_partition_id>"`

Representative attributes:
- `owner_id`, `generation`, `lease_expiry_ts`, `last_heartbeat_ts`, `committed_cursor`,
  `committed_ts`, `topic`, `group_id`, `virtual_partition_id`, `ttl_epoch_seconds`

### 4) `consumer_group_membership`

Purpose:
- Consumer member liveness and the authoritative dynamic consumer-group assignment plan.

Keys:
- Partition key: `pk` = `"<topic>#<group_id>"`
- Sort key: `sk` = `"<member_id>"`

Representative attributes:
- `member_id`, `lease_expiry_ts`, `last_heartbeat_ts`, `topic`, `group_id`, `ttl_epoch_seconds`

Assignment control records use a distinct reserved partition key,
`"__blob_stream_assignment_control_v1__#<topic>#<group_id>"`, in the same table. This keeps
the legacy member query partition limited to member rows during rolling upgrades and avoids
reading the complete plan when listing active members. No additional DynamoDB table is required:
- `sk = "__blob_stream_assignment_plan_v1__"`: a complete, versioned virtual-partition to member
  assignment map and the member set used to calculate it.
- `sk = "__blob_stream_assignment_planner_v1__"`: the short-lived session-fenced planner lease
  that authorizes plan publication.

Member IDs beginning with `__blob_stream_` are reserved. Member rows carry `record_type = member`
so active-member reads exclude coordination system rows; legacy untyped member rows remain
compatible.

### Provisioning note

Current implementation uses the 4 logical Dynamo data domains listed above (`blob_segments`,
`producer_partition_leases`, `consumer_group_leases`, `consumer_group_membership`). Configure
explicit table names in `metadata_store.dynamo`:
- `segment_metadata_table_name`
- `producer_partition_lease_table_name`
- `consumer_group_lease_table_name`
- `consumer_group_membership_table_name`

Context usage:
- Broker context uses `segment_metadata_table_name` +
  `producer_partition_lease_table_name`.
- Consumer context uses `segment_metadata_table_name` +
  `consumer_group_lease_table_name` + `consumer_group_membership_table_name`.

### Enable DynamoDB TTL

All Dynamo tables should enable TTL on the same attribute name:
- TTL attribute name: `ttl_epoch_seconds` (Unix epoch seconds)

Example AWS CLI commands:

```bash
aws dynamodb update-time-to-live \
  --table-name blob_segments \
  --time-to-live-specification "Enabled=true,AttributeName=ttl_epoch_seconds"

aws dynamodb update-time-to-live \
  --table-name producer_partition_leases \
  --time-to-live-specification "Enabled=true,AttributeName=ttl_epoch_seconds"

aws dynamodb update-time-to-live \
  --table-name consumer_group_leases \
  --time-to-live-specification "Enabled=true,AttributeName=ttl_epoch_seconds"

aws dynamodb update-time-to-live \
  --table-name consumer_group_membership \
  --time-to-live-specification "Enabled=true,AttributeName=ttl_epoch_seconds"
```

TTL behavior by table:
- `blob_segments`: broker writes `ttl_epoch_seconds` using `topic.retention_days` plus
  `metadata_store.dynamo.segment_ttl_buffer_seconds`.
- `consumer_group_leases`: consumer writes `ttl_epoch_seconds` from lease expiry plus the topic
  `retention_days`, preserving committed cursors through the readable-data retention period.
- `producer_partition_leases`, `consumer_group_membership`: lease rows write `ttl_epoch_seconds`
  from their lease expiration plus `metadata_store.dynamo.lease_ttl_buffer_seconds`.

Notes:
- DynamoDB TTL is asynchronous; expired items are typically deleted within hours, not immediately.
- Keep segment TTL aligned with S3 lifecycle to avoid metadata pointing at already-deleted blobs.

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
- Consumers do not require this discovery RBAC unless your application adds k8s-based
  coordination.

## Observability

Broker exposes Prometheus metrics at:
- `GET /metrics`

Producer and consumer libraries also emit metrics through `bd-server-stats`.
For consumer metadata scans, monitor `metadata_recovery_scan_hits` and
`metadata_recovery_scan_batches_read`: they count successful unbounded recovery passes that
returned new batches. Compare them with `metadata_recovery_scan_requests`,
`metadata_recovery_scan_segments`, and
`metadata_recovery_scan_failures` when tuning the recovery interval and scan horizon.

Consumer lifecycle handoffs emit structured `blob_stream.consumer.partition_handoff` OTEL spans
for each virtual partition. Query by `consumer.topic`, `consumer.group_id`,
`messaging.partition`, `handoff.cursor_key`, `handoff.phase`, and `handoff.outcome` to connect an
outgoing shutdown or revocation span with the incoming startup or rebalance assignment. Individual
attributes include the committed offset, reader mode, and recovery bounds. The complete
per-partition state, including source checkpoint, pending cursor state, scan counters, and retained
fast frontiers, is captured in `handoff.snapshot_json`. The handoff phases are
`startup_assigned`, `rebalance_assigned`, `revocation_pre_release`,
`revocation_release_result`, `shutdown_pre_release`, and `shutdown_release_result`. Release
outcomes are `pending_release`, `awaiting_application_ack`, `released`, `not_released`, and
`failed`; assignment outcomes are `assigned` or `not_applicable`.

To bound allocation and attribute size, each scan-detail collection in `handoff.snapshot_json` is
limited to 64 entries. The corresponding `*_truncated` field is true when additional scan windows,
fast-scan bounds, or fast frontiers were omitted; scalar scan counters always retain their complete
values.

Each partition that enters bounded recovery also emits a
`blob_stream.consumer.partition_recovery` span from assignment until it enters the Fast path or
loses assignment. Its queryable attributes include starting and committed cursors, recovery bounds,
duration, scan-pass count, and outcome. `recovery.summary_json` contains aggregate cursor-skip,
visibility-deferral, frontier-skip, capacity-deferral, accepted-batch, and accepted-record counters
that explain both recovery time and why a handoff reread more metadata than expected. Its
`recovery_segments_handed_to_fast_by_visibility` and
`recovery_segments_blocked_by_visibility` counters distinguish active Fast-horizon deferrals from
historical recovery barriers. This is intentional: active-horizon deferrals hand off to Fast for a
later bounded rescan, while older deferred windows keep recovery blocked to preserve cursor order.
Recovery detail remains in this JSON attribute because exported spans are limited to 16 attributes.

Graceful shutdown emits a `blob_stream.consumer.shutdown` root span covering the final commit,
lease release, membership deregistration, planner release, and prefetch termination. Its per-step
outcome attributes show which operation failed; its OTEL span status is `OK` only when every
fallible shutdown operation succeeds. Failed shutdown and revocation-handoff spans include
`error.message` with the failed operation and error text. Revocation roots also expose
`handoff.lease_release_outcome` and `handoff.assignment_outcome`, and report success only after
the replacement assignment is active.

