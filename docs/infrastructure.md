# Infrastructure Setup

This guide covers the resources and configuration needed to deploy `blob-stream`. For system
behavior, see [Design](design.md). For runtime diagnostics and consistency controls, see
[Operations](operations.md).

The protobuf schema in
[`blobstream/v1/config.proto`](../blob-stream-proto/proto/blobstream/v1/config.proto) is the
canonical field-level configuration reference.

## Runtime Configuration

The broker loads JSON, YAML, or YML runtime configuration through `--config` or
`BLOB_STREAM_CONFIG`. A minimal local S3/DynamoDB configuration is:

```yaml
broker:
  bind_addr: "0.0.0.0:8080"
  node_identity:
    hostname: {}
  writer_id: 0
  discovery:
    static:
      nodes:
        - node_id: "broker-a"
          address: "127.0.0.1:8080"

topics:
  - name: "telemetry"
    partition_count: 128
    num_writers: 1
    retention: "604800s"

blob_store:
  s3:
    bucket: "blob-stream"
    prefix: "telemetry/"
    region: "us-east-1"

metadata_store:
  dynamo:
    region: "us-east-1"
    segment_metadata_table_name: "blob_segments"
    producer_partition_lease_table_name: "producer_partition_leases"
    consumer_group_lease_table_name: "consumer_group_leases"
    consumer_group_membership_table_name: "consumer_group_membership"
```

`bind_addr`, a blob-store backend, a metadata-store backend, and at least one topic are required.
Every broker, producer, and consumer using a topic must use the same `name`, `partition_count`,
`num_writers`, and retention. A deployment's `writer_id` identifies one local writer/AZ domain; it
must be in range for every configured topic, and its broker discovery must contain only brokers in
that domain.

Important broker defaults are 64 MiB `flush_max_bytes`, 1 second `flush_max_delay`, 10,000
`sequence_reservation_size`, zstd segment compression, and disabled `fenced_metadata_writes`.
Topics default `max_metadata_publication_lag` to 15 seconds when unset. Consumers default
`max_clock_skew` to 10 ms when unset. Configure the skew bound to the proven, monitored pairwise
broker-to-consumer clock offset for the deployment; it is a consumer read setting.
Producer and consumer defaults, including batching, retry, read-window, and prefetch values, are
documented in the protobuf schema.

### Feature-Flag Mounts

A broker can watch a filesystem-backed feature-flag file, typically a Kubernetes ConfigMap mount:

```yaml
broker:
  feature_flags:
    dir: "/etc/blob-stream/feature_flags"
    file: "/etc/blob-stream/feature_flags/feature_flags.yaml"
```

The feature flags `blob_stream_broker_fenced_metadata_writes`,
`blob_stream_consumer_strong_metadata_reads`, `blob_stream_consumer_prefetch_max_bytes`, and
`blob_stream_consumer_max_in_flight_batch_reads` override their configured fallback values. See
[Operations](operations.md) for their runtime effects.

## S3

Production brokers write segment objects and consumers range-read them. Configure an S3 bucket in
the same region as the deployment, grant brokers write access and consumers read access, and choose
an optional prefix for isolation. Segment keys use:

```text
<prefix>/<topic>/<window_start_unix_seconds>/<snowflake_id>.<zst|bin>
```

Configure the S3 lifecycle so objects remain available for at least the topic retention period plus
the metadata TTL buffer. DynamoDB TTL deletion is asynchronous; expiring S3 objects first can leave
metadata pointing to a missing blob. Server-side encryption and bucket access logging are recommended
according to the deployment's security requirements.

## DynamoDB

Provision four tables with string `pk` partition keys. The segment, consumer-lease, and
consumer-membership tables also use a string `sk` sort key; the producer-lease table is partition-key
only.

| Table | Key layout | Purpose |
| --- | --- | --- |
| Segment metadata | `pk = <topic>#<window_start>`, `sk = <snowflake_id>` | Consumer metadata scans |
| Producer leases | `pk = <topic>#<virtual_partition_id>` | Broker ownership and sequence reservations |
| Consumer leases | `pk = <topic>#<group_id>`, `sk = <virtual_partition_id>` | Consumer ownership and committed cursors |
| Consumer membership | `pk = <topic>#<group_id>`, `sk = <member_id>` | Member liveness and assignment coordination |

Enable TTL on `ttl_epoch_seconds` for every table. Segment metadata TTL is topic retention plus
`segment_ttl_buffer` (default 3,600 seconds). Producer leases and consumer membership use
lease expiration plus `lease_ttl_buffer` (default 3,600 seconds). Consumer lease rows retain
committed cursors through the readable-data retention period.

The membership table also holds reserved assignment-control rows under
`__blob_stream_assignment_control_v1__#<topic>#<group_id>`. They store the versioned assignment plan
and session-fenced planner lease; no fifth table is needed. Producer lease rows always include a
holder ID, lease epoch, and lease session ID, even when transactional fenced metadata publication is
disabled.

Enable TTL on each configured table name, for example:

```bash
aws dynamodb update-time-to-live \
  --table-name blob_segments \
  --time-to-live-specification "Enabled=true,AttributeName=ttl_epoch_seconds"
```

Repeat this for the producer lease, consumer lease, and consumer membership tables.

## IAM

Scope resource ARNs to the configured table names and bucket prefix. The following list covers the
AWS API calls made by Blob Stream; a bucket using SSE-KMS also needs the KMS key permissions
required by its encryption policy.

### Broker

- S3 bucket/prefix: `s3:PutObject`.
- Segment-metadata table: `dynamodb:PutItem` for ordinary and fenced metadata publication.
- Producer-lease table: `dynamodb:GetItem` and `dynamodb:UpdateItem` for lease observation,
  acquisition, heartbeat, sequence reservation, and release.
- Fenced publication: `dynamodb:ConditionCheckItem` on the producer-lease table. The transaction
  puts segment metadata and condition-checks each current producer lease; it does not
  condition-check a segment-metadata item.

### Consumer

- S3 bucket/prefix: `s3:GetObject`.
- Segment-metadata table: `dynamodb:Query`.
- Consumer-lease table: `dynamodb:GetItem`, `dynamodb:Query`, and `dynamodb:UpdateItem`.
- Consumer-membership table: `dynamodb:GetItem`, `dynamodb:Query`, `dynamodb:UpdateItem`, and
  `dynamodb:DeleteItem`. Assignment-plan publication additionally needs
  `dynamodb:ConditionCheckItem` on this table.

Do not grant a fenced broker only `PutItem` and `ConditionCheckItem`: its normal producer-lease
lifecycle still uses `GetItem` and `UpdateItem`.

## Kubernetes Discovery RBAC

When broker or producer discovery uses `k8s_service`, the workload ServiceAccount needs `get`,
`list`, and `watch` on core `Endpoints` in the broker namespace:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: blob-stream-endpoints-read
  namespace: telemetry
rules:
  - apiGroups: [""]
    resources: ["endpoints"]
    verbs: ["get", "list", "watch"]
```

Bind this Role to broker pods using Kubernetes discovery and to producer applications using the same
discovery backend. Consumers do not need this permission unless their application adds its own
Kubernetes discovery behavior.
