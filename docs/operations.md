# Operations Guide

This guide covers live `blob-stream` operation, observability, and consistency controls. See
[Infrastructure setup](infrastructure.md) for configuration, access, and resource provisioning, and
[Design](design.md) for the underlying delivery guarantees.

## Consistency Controls

Consumer metadata reads are eventually consistent by default. The controls below address different
failure modes and can be enabled independently.

| Control | Default | Protects | Cost and limits |
| --- | --- | --- | --- |
| `strongly_consistent_metadata_reads` or `blob_stream_consumer_strong_metadata_reads` | `false` | DynamoDB read-replica staleness | Strong metadata reads approximately double query RRUs. They set the effective metadata visibility delay to zero but do not make paginated scans atomic or stop stale producer publication. |
| `fenced_metadata_writes` or `blob_stream_broker_fenced_metadata_writes` | `false` | A former producer-lease holder publishing after replacement | Each publication becomes a DynamoDB transaction. It conditions metadata publication on every current lease, requires additional IAM actions, and is limited to 99 producer-lease checks per plan. |

Strong reads remove the two-second default eventual-read visibility margin; the broker's metadata
publication deadline still applies. Fenced writes retain a durable holder ID, lease epoch, and
session ID in every producer lease row regardless of whether the mode is enabled.

A lost publication fence after blob upload leaves an unreferenced blob but does not publish its
metadata. The broker returns a retryable failure. There is no unfenced fallback for that plan.

## HTTP Endpoints

The broker listener serves gRPC plus these HTTP endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /metrics` | Prometheus text format (`text/plain; version=0.0.4`) |
| `GET /admin/state` | JSON snapshot of broker configuration, discovery, ownership, and local buffers |
| `POST /admin/log?rust_log=<filter>` | Replaces the active Rust log filter without restarting the broker |

Protect the admin listener according to the deployment's network policy. `/admin/log` accepts a log
filter such as `blob_stream=debug,bd=debug`; use trace only for targeted investigations.

## Reading Broker State

`/admin/state` includes the snapshot timestamp, local holder ID and writer ID, active flush limits,
and discovered membership. Its ownership list covers writer-scoped partitions and reports the planned
broker, whether the assignment is local, the observed producer lease, and one of these statuses:

- `local_active`: this broker holds the active lease.
- `remote_active`: another broker holds the active lease.
- `assigned_local_pending` or `assigned_remote_pending`: discovery assignment and lease state are
  converging.
- `unleased_or_expired`: no active lease is visible.
- `lookup_failed`: the lease lookup failed; investigate metadata-store availability before acting on
  ownership assumptions.

The per-topic local partition list exposes lease expiry, allocation status, queued batches, buffered
records and bytes, the oldest buffered timestamp, and the current sequence reservation plus next
sequence. Persistent growth in buffered bytes or an old `first_buffered_at` with active allocation
usually points to blob-store, DynamoDB, or publication-latency problems. An unexpected local
assignment without `local_active` is expected briefly during discovery or lease convergence; pair it
with lease status and error metrics before treating it as an incident.

## Metrics

Broker metric names use the `blob_stream_broker` namespace and component scopes such as `grpc` and
`write`. The precise Prometheus exposition is available at `/metrics`.

### Request Outcomes

Monitor `grpc:requests_total`, `grpc:request_latency_seconds`, and
`grpc:request_timeouts_total`, along with the `write:produce_*` counters. `write:produce_requests_total`
counts attempts entering the write engine. `write:produce_records_total` and
`write:produce_payload_bytes_total` count durable `OK` outcomes, while their `rejected_*`
counterparts count definitive failures. A gRPC timeout is deliberately neither success nor rejection:
the broker may still complete the flush, and a producer retry can duplicate the batch.

Rising `produce_not_lease_holder_total` indicates routing or ownership churn. Rising
`produce_overloaded_total` or `admission_rejections_total` indicates admission pressure or an
internal transient failure. Correlate both with `/admin/state` before changing capacity.

### Flush And Publication

Use `flush_plans_total`, `flush_partitions_total`, and `flush_batches_total` to understand write
coalescing. The `flush_batches_max_bytes_total`, `flush_batches_max_delay_total`, and
`flush_batches_lease_drain_total` counters identify the trigger. Investigate increases in
`flush_failures_total`, `flush_latency_seconds`,
`metadata_publication_latency_seconds`, and either
`metadata_publication_deadline_exhausted_before_persistence_total` or
`metadata_publication_deadline_exhausted_while_persisting_total`.

Publication deadline exhaustion fails the flush; it is not a warning that permits delayed
publication. Check blob-store latency, DynamoDB throttling, object size
(`flush_uploaded_object_bytes` and `flush_uploaded_object_bytes_total`), and configured
`max_metadata_publication_lag_ms` together.

### Leases And Sequence Reservations

`sequence_reservations_total`, `sequence_reservation_records_total`,
`sequence_reservation_failures_total`, and `sequence_reservation_latency_seconds` describe durable
Hi-Lo range allocation. A large reservation rate relative to record rate can indicate ownership
churn, restart churn, or undersized reservation targets. `lease_drain_starts_total` and
`lease_drain_completions_total` expose graceful ownership handoffs; sustained divergence indicates
partitions are failing to drain or release.

## Diagnostic Runbooks

### Producers Receive `NOT_LEASE_HOLDER`

1. Check `/admin/state` on the planned local broker and its peers.
2. Confirm the writer ID and discovered membership are scoped to the same local writer/AZ domain.
3. Distinguish a short `assigned_local_pending` convergence period from a persistent `remote_active`
   lease or `lookup_failed` metadata-store failure.
4. Check producer discovery and retry metrics before forcing a broker restart.

### Metadata Is Late Or Missing

1. Check publication deadline-exhaustion and flush-failure counters.
2. Inspect partition buffer age and queued bytes in `/admin/state`.
3. Verify S3 and DynamoDB latency, throttling, IAM, and lifecycle policy.
4. For eventual-read consumers, decide whether the configured visibility margin is adequate; enable
   strong metadata reads only when replica staleness is the relevant risk and the RRU increase is
   acceptable.

### Stale Producer Publication Must Be Rejected

1. Enable `fenced_metadata_writes` through static configuration or its watched runtime flag.
2. Confirm broker IAM includes `TransactWriteItems` and `ConditionCheckItem` for both affected
   DynamoDB tables.
3. Monitor publication failures and retry behavior during rollout.
4. Remember that strong reads do not add publication fencing; they solve a separate read-side issue.

### Consumer Recovery Is Expensive

1. Inspect consumer metadata scan and blob-range metrics in the embedding application's
   `bd-server-stats` registry.
2. Compare recovery work with topic retention, publication lag, and eventual-read visibility delay.
3. Use [Cost analysis](cost-analysis.md) with observed page and range-read rates before changing
   strong-read or transactional-publication modes.
