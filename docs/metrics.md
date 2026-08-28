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
| `broker_metadata_offload_requests`, `broker_metadata_offload_deliveries`, `broker_metadata_offload_fallbacks` | Counters | Broker metadata RPCs attempted, validated broker responses delivered, and original direct queries retried after an unusable broker response. Requests equal deliveries plus fallbacks. |
| `recovery_metadata_cache_hits`, `recovery_metadata_cache_misses`, `recovery_metadata_cache_inserts`, `recovery_metadata_cache_invalidations` | Counters | Recovery metadata-cache effectiveness and maintenance. |
| `recovery_metadata_cache_entries`, `recovery_metadata_cache_retained_bytes` | Gauges | Current retained recovery cache entry count and bytes. |
| `metadata_fast_scan_without_lower_bound` | Counter | Fast scans that could not use a derived metadata lower bound. |
| `metadata_segments_deferred_by_visibility_delay` | Counter | Segment rows deferred by the eventual-read visibility maturity delay. Strong reads accept every validated row and do not increment this counter. |
| `metadata_batches_scanned`, `metadata_batches_skipped_by_cursor` | Counters | Batches decoded from metadata and batches skipped because the committed cursor had already passed them. |
| `broker_blob_range_requests`, `broker_blob_range_successes`, `broker_blob_range_bytes`, `broker_blob_range_latency_seconds` | Counters, counter, counter, histogram | Broker raw-blob group requests, validated successes, delivered bytes, and successful broker delivery latency. |
| `broker_blob_range_fallbacks`, `broker_blob_range_not_found_groups` | Counters | Broker groups retried through direct object-store reads after an unusable response, and authoritative immutable-object absences accepted without a direct retry. Together with successes, these are the terminal outcomes of broker requests. |
| `fallback_blob_range_requests`, `fallback_blob_range_bytes`, `fallback_blob_range_latency_seconds` | Counters, counter, histogram | Direct object-store range reads, bytes, and latency after a broker group falls back. One fallback group can produce multiple direct range reads. |
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

### Metadata Cache: `blob_stream_broker:metadata_cache`

| Metric | Type | Meaning |
| --- | --- | --- |
| `requests_total`, `tail_hits_total`, `recovery_hits_total` | Counters | Metadata-cache RPCs and retained Tail or Full Recovery hits. |
| `storage_queries_total` | Counter | Authoritative metadata-store queries started after coalescing, for both eventual and strong reads. |
| `tail_refills_total`, `recovery_baselines_total`, `recovery_seals_total` | Counters | Tail refills and complete Full Recovery snapshots installed. Full Recovery is unpaged, so its baseline and final seal are recorded together. |
| `invalidations_total`, `evictions_total` | Counters | Retained entries removed because they no longer satisfy a request and entries evicted by cache policy. |
| `failures_total`, `overloads_total` | Counters | Rejected or failed metadata-cache reads and the subset caused by admission, size, or timeout overload. |
| `response_items_total`, `response_bytes_total` | Counters | Metadata segments and encoded metadata bytes returned in successful broker responses. |
| `coalescing_window_requests_total` | Counter | Requests still admitted when a coalescing window starts its authoritative query, including the request that created the query group. |
| `observation_age_seconds` | Histogram | Age of a retained hit. |
| `active_waiters`, `active_refills` | Gauges | Requests currently waiting on refill work and refills holding concurrency permits. |
| `tail_entries`, `recovery_entries`, `tail_retained_bytes`, `recovery_retained_bytes` | Gauges | Current retained entry count and weighted bytes for each cache. |

These metrics have no topic, metadata-window, partition, or consumer-group labels. Inspect
aggregate capacity and current admission state through `/admin/metadata-cache`.

`coalescing_window_requests_total / storage_queries_total` is the average request fan-in per
authoritative query. Subtracting `storage_queries_total` from
`coalescing_window_requests_total` gives the number of requests collapsed into an existing query
group. Both calculations apply to eventual and strong reads; retained-cache hits are separate.

### Blob Cache: `blob_stream_broker:blob_cache`

| Metric | Type | Meaning |
| --- | --- | --- |
| `requests_total`, `response_items_total`, `response_bytes_total`, `request_latency_seconds` | Counters, counters, counter, histogram | Raw blob-range requests, successful requested-range items and bytes, and complete broker cache request latency. |
| `hits_total`, `fetches_total`, `fetch_bytes_total` | Counters | Retained complete-blob hits, bounded whole-object storage reads, and bytes fetched into the cache. |
| `evictions_total`, `pressure_flushes_total` | Counters | Idle/underlying-cache removals and cache invalidations caused by a memory-overload transition. |
| `not_found_total`, `overloads_total`, `failures_total` | Counters | Authoritative absent objects, retryable overload outcomes, and all failed cache requests. |
| `entries`, `retained_bytes`, `active_fetches` | Gauges | Current retained complete-object count and bytes, and in-flight whole-object reads. |

These metrics have no object-key, topic, partition, or consumer labels. Use `/admin/blob-cache`
for the matching aggregate cache headroom and reservation state. A useful aggregate collapse signal
is:

$$
collapse\ rate = 1 - \frac{fetches\_total}{response\_items\_total}
$$

Interpret it together with direct consumer range reads and bytes: a higher collapse rate is useful
only when whole-object overfetch and broker resource use remain acceptable.

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
| `flush_partitions_total`, `flush_batches_total` | Counters | Partition plans and producer batches included in flushes. |
| `flush_batches_max_bytes_total`, `flush_batches_max_delay_total`, `flush_batches_lease_drain_total` | Counters | Flushed batches classified by size, delay, or lease-drain trigger. |
| `flush_failures_total`, `flush_latency_seconds` | Counter, histogram | Failed flushes and complete flush latency. |
| `metadata_publication_deadline_exhausted_before_persistence_total`, `metadata_publication_deadline_exhausted_while_persisting_total` | Counters | Publication deadlines exhausted before metadata persistence begins or while it is in progress. Both fail the flush. |
| `flush_uploaded_objects_total`, `flush_oversized_single_partition_objects_total` | Counters | Uploaded immutable objects and objects containing one partition that exceeded `max_segment_bytes` and could not be split. |
| `flush_uploaded_object_bytes_total`, `flush_uploaded_object_bytes` | Counter, histogram | Total bytes uploaded and per-object upload size. |
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
