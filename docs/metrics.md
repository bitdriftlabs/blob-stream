# Metrics Reference

This reference inventories Blob Stream-owned metrics for brokers, producers, and consumers. Use
[Integration](integration.md) for how libraries receive a metrics scope, and [Operations](operations.md)
for the incident runbooks that use these signals.

## Names And Types

Metric names use colon-delimited `bd-server-stats` scopes. The names below omit a caller-provided
root scope. For example, a producer built with `scope("telemetry_ingester")` exports
`telemetry_ingester:producer:records_enqueued`; a consumer built with
`scope("telemetry_worker")` exports
`telemetry_worker:consumer:reader:read_available_calls`. The broker root is fixed as
`blob_stream_broker` and serves its registry at `GET /metrics`.

Counters accumulate from process start and reset when the process restarts. Gauges report an
instantaneous value. Histograms export the usual Prometheus bucket, sum, and count series. All
names in a row share the heading's scope prefix.

Shared dependencies can add their own metrics to a supplied registry. This reference lists the
stable Blob Stream metrics and the DynamoDB capacity totals that Blob Stream wires into broker and
consumer bootstraps.

## Producer Metrics

Producer metrics use `<application-scope>:producer`.

| Metric | Type | Meaning |
| --- | --- | --- |
| `records_enqueued` | Counter | Records accepted into a producer-side partition buffer. |
| `batches_sent`, `records_sent` | Counters | Batches and records acknowledged by the broker after retry handling. |
| `flushes_max_size`, `flushes_max_delay` | Counters | Producer buffer flushes triggered by the configured record/byte limit or flush delay. |
| `active_requests` | Gauge | gRPC produce requests currently in flight. |
| `send_latency_seconds` | Histogram | End-to-end time spent attempting one producer batch, including retries. |
| `retries` | Counter | Retry attempts scheduled for producer batches. |
| `not_lease_holder_retry_timers`, `not_lease_holder_retry_membership_updates` | Counters | `NOT_LEASE_HOLDER` retries resumed by backoff timing or a broker-membership change. |
| `not_lease_holder_retry_same_owner`, `not_lease_holder_retry_changed_owner` | Counters | `NOT_LEASE_HOLDER` responses where the selected broker route remained the same or changed before retry. |
| `failures` | Counter | Producer batches that ended in a terminal send failure. |
| `no_brokers` | Counter | Send attempts made while discovery had no eligible broker. |

## Consumer Metrics

Consumer bootstrap adds the `consumer` scope, then the reader and iterator add their own scope:
`<application-scope>:consumer:reader` and `<application-scope>:consumer:iterator`.

### Reader

| Metric | Type | Meaning |
| --- | --- | --- |
| `read_available_calls`, `read_available_empty`, `read_available_latency_seconds` | Counter, counter, histogram | Reader polls, polls that yielded no batches, and total scan/read planning time. |
| `metadata_scan_requests`, `metadata_scan_segments`, `metadata_scan_latency_seconds` | Counters, histogram | All metadata scans, segment rows examined, and metadata scan latency. |
| `metadata_fast_scan_requests`, `metadata_fast_scan_segments` | Counters | Metadata work on the steady-state fast path. |
| `metadata_recovery_scan_requests`, `metadata_recovery_scan_segments`, `metadata_recovery_scan_failures` | Counters | Metadata work and failures while replaying retained history. |
| `metadata_recovery_scan_hits`, `metadata_recovery_scan_batches_read` | Counters | Recovery scans that returned any batch and the batches returned by them. |
| `recovery_metadata_cache_hits`, `recovery_metadata_cache_misses`, `recovery_metadata_cache_inserts`, `recovery_metadata_cache_invalidations` | Counters | Recovery metadata-cache effectiveness and maintenance. |
| `recovery_metadata_cache_entries`, `recovery_metadata_cache_retained_bytes` | Gauges | Current retained recovery cache entry count and bytes. |
| `metadata_fast_scan_without_lower_bound` | Counter | Fast scans that could not use a derived metadata lower bound. |
| `metadata_segments_deferred_by_visibility_delay` | Counter | Segment rows deferred by the resolved visibility maturity delay. Strong reads resolve that delay to zero, so only rows timestamped after the reader's current time are deferred. |
| `metadata_batches_scanned`, `metadata_batches_skipped_by_cursor` | Counters | Batches decoded from metadata and batches skipped because the committed cursor had already passed them. |
| `blob_range_requests`, `blob_range_bytes`, `blob_range_latency_seconds` | Counters, histogram | S3/object-store byte-range reads, bytes read, and range-read latency. |
| `blob_batch_ranges`, `blob_batch_range_bytes` | Counters | Byte ranges planned for individual encoded batches and their total bytes. |
| `batches_read`, `records_read`, `record_payload_bytes` | Counters | Decoded batches, records, and uncompressed record payload bytes accepted by the reader. |
| `lost_records` | Counter | Records dropped because a requested historical position cannot be recovered from retained data. |

### Iterator And Coordination

| Metric | Type | Meaning |
| --- | --- | --- |
| `batches_delivered`, `records_delivered` | Counters | Batches and records yielded to application code. |
| `next_latency_seconds`, `commit_latency_seconds` | Histograms | Application-visible `next()` and explicit `commit()` operation latency. |
| `retries`, `failures`, `seeks`, `revocations` | Counters | Iterator retry attempts, terminal failures, explicit cursor seeks, and revocation events surfaced to the application. |
| `rebalances_total`, `rebalance_failures_total` | Counters | Rebalance attempts and attempts that failed before an assignment applied. |
| `assignment_plans_applied_total`, `assignment_plan_rejections_total`, `assignment_applications_total` | Counters | Accepted assignment plans, rejected plan versions, and local assignment changes that reached the reader. |
| `lease_claims_initial`, `lease_claims_retained`, `lease_claims_graceful_handoff`, `lease_claims_expiry_takeover` | Counters | Partition claims classified by initial ownership, retention, cooperative handoff, or takeover after owner expiry. |
| `desired_partitions`, `owned_partitions`, `active_partitions` | Gauges | Partitions assigned by the plan, currently leased by this member, and currently active for reading. |
| `prefetch_buffered_batches`, `prefetch_buffered_bytes` | Gauges | Fully prefetched batches and bytes ready for delivery. |
| `prefetch_pending_batches`, `prefetch_pending_bytes` | Gauges | Batches and bytes being read or decoded but not ready for delivery. |
| `prefetch_total_bytes` | Gauge | Total buffered plus pending payload bytes counted against the consumer prefetch budget. |
| `prefetch_paused_budget`, `prefetch_refill_cycles` | Counters | Prefetch pauses caused by the memory budget and attempts to refill available prefetch capacity. |
| `heartbeat_calls`, `heartbeat_scheduled_calls`, `heartbeat_commit_calls` | Counters | All coordination heartbeats, scheduled renewals, and heartbeats caused by an explicit commit. |
| `heartbeat_failures`, `membership_heartbeat_failures`, `lease_heartbeat_failures` | Counters | Heartbeat failures overall, failures updating member liveness, and failures renewing partition leases. |
| `heartbeat_retry_attempts`, `rebalance_retry_attempts` | Counters | Retries scheduled after heartbeat or rebalance failures. |
| `heartbeat_committed_offsets`, `heartbeat_renewed_partitions`, `lease_renewed_partitions`, `cursor_commit_partitions` | Counters | Partitions with staged cursors successfully submitted by any heartbeat, partitions renewed by any heartbeat, partitions renewed by scheduled heartbeats, and partitions included in explicit cursor commits. |
| `heartbeat_fenced_partitions` | Counter | Partitions lost because the consumer was fenced during heartbeat processing. |
| `heartbeat_latency_seconds` | Histogram | End-to-end heartbeat latency. |

## Broker Metrics

Broker metrics use `blob_stream_broker` with the component scopes below.

### gRPC: `blob_stream_broker:grpc`

| Metric | Type | Meaning |
| --- | --- | --- |
| `requests_total`, `batches_total`, `records_total` | Counters | Produce RPCs, logical batches, and records accepted at the gRPC boundary. |
| `responses_ok_total`, `responses_not_lease_holder_total`, `responses_unknown_topic_total`, `responses_overloaded_total`, `responses_bad_request_total` | Counters | Produce response statuses returned by the broker. |
| `request_timeouts_total` | Counter | gRPC produce batches that exceeded the broker request timeout. A timeout is ambiguous: background work may still complete and a producer retry can duplicate it. |
| `active_batches` | Gauge | Logical produce batches currently being handled. |
| `request_latency_seconds` | Histogram | gRPC batch handler latency, including write-engine work. |

### Write Path: `blob_stream_broker:write`

| Metric | Type | Meaning |
| --- | --- | --- |
| `produce_requests_total` | Counter | Logical produce batches that entered the write engine. |
| `produce_records_total`, `produce_payload_bytes_total`, `produce_ok_total` | Counters | Records, raw payload bytes, and batches with a durable `OK` outcome. |
| `produce_rejected_records_total`, `produce_rejected_payload_bytes_total` | Counters | Records and payload bytes in definitively rejected batches. |
| `produce_not_lease_holder_total`, `produce_overloaded_total`, `produce_unknown_topic_total` | Counters | Rejections classified as ownership/fence loss, overload or internal transient failure, and unknown topic. |
| `produce_latency_seconds` | Histogram | Write-engine attempt latency before the gRPC response is formed. |
| `admission_rejections_total` | Counter | Batches rejected by broker admission control before normal write processing. |
| `sequence_reservations_total`, `sequence_reservation_records_total`, `sequence_reservation_failures_total`, `sequence_reservation_latency_seconds` | Counters, histogram | Durable Hi-Lo range reservations, sequence capacity obtained, reservation failures, and reservation latency. |
| `flush_plans_total`, `flush_partitions_total`, `flush_batches_total` | Counters | Constructed flush plans, partition plans, and producer batches included in flushes. |
| `flush_batches_max_bytes_total`, `flush_batches_max_delay_total`, `flush_batches_lease_drain_total` | Counters | Flushed batches classified by size, delay, or lease-drain trigger. |
| `flush_failures_total`, `flush_latency_seconds` | Counter, histogram | Failed flushes and complete flush latency. |
| `metadata_publication_latency_seconds` | Histogram | Time spent persisting segment metadata after blob upload. |
| `metadata_publication_deadline_exhausted_before_persistence_total`, `metadata_publication_deadline_exhausted_while_persisting_total` | Counters | Publication deadlines exhausted before metadata persistence begins or while it is in progress. Both fail the flush. |
| `flush_uploaded_object_bytes_total`, `flush_uploaded_object_bytes` | Counter, histogram | Total bytes uploaded in flush objects and per-object upload size. |
| `lease_drain_starts_total`, `lease_drain_completions_total` | Counters | Graceful producer-lease drain starts and completed releases. |

### Memory Admission: `blob_stream_broker:memory_pressure`

These metrics are populated on Linux when the cgroup/jemalloc sampler can run; memory-pressure
admission is disabled when that setup is unavailable.

| Metric | Type | Meaning |
| --- | --- | --- |
| `utilization_percent` | Gauge | jemalloc allocation as a percentage of the cgroup memory limit. |
| `overloaded` | Gauge | `1` while the memory controller is rejecting new writes and `0` otherwise. |
| `transitions_total` | Counter | Changes into or out of memory-overloaded state. |
| `sampling_failures_total` | Counter | Failed cgroup or allocator utilization samples. |

### DynamoDB Capacity: `*:dynamo`

Both broker and consumer DynamoDB bootstraps create this scope. Its full name is
`blob_stream_broker:dynamo` for a broker and `<application-scope>:dynamo` for a consumer.

| Metric | Type | Meaning |
| --- | --- | --- |
| `read_request_units_total` | Counter | DynamoDB read capacity units returned by requests, including transactional read capacity. |
| `write_request_units_total` | Counter | DynamoDB write capacity units returned by requests, including transactional write capacity. |
