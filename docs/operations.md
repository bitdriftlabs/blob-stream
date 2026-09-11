# Operations Guide

This guide covers live `blob-stream` operation, observability, and consistency controls. See
[Infrastructure setup](infrastructure.md) for configuration, access, and resource provisioning, and
[Consumption](design/consumption.md) for the underlying delivery guarantees.

## Investigating A Consumer Delivery Gap

Use `blob-stream-metadata-inspector` to strongly read the current DynamoDB metadata rows around a
gap. It uses standard AWS credential resolution; supply `AWS_REGION` and, when inspecting a local
table, `DYNAMODB_ENDPOINT`. The default 300-second metadata window must be changed with
`--metadata-window-seconds` for a topic configured with a different fixed window size.

```sh
AWS_REGION=us-east-1 ./bazelw run //blob-stream/blob-stream-metadata-inspector:blob-stream-metadata-inspector-bin -- \
   --segment-dynamo-table <table> \
   --topic <topic> \
   --target-time 2026-09-05T13:16:17.556509Z \
   --suspect-cursor 11480895592 \
   --partition 1
```

The report includes the target window and one adjacent window on each side. Its table output shows
three source rows before and after the cursor boundary by default, including blob keys, timestamps,
byte ranges, and payload sizes; it summarizes the omitted rows. Use `--context-rows <count>` to
widen that focused view, or `--format json` to attach every queried row to an incident. It flags
uncovered sequence intervals, overlaps, and when source order differs from sequence order.

Strong reads show the table's current state, not what an earlier eventual consumer query returned.
For a delivery gap, its warning's `admission_scan_json` is the exact finalized reader scan that
admitted the gapped batch; compare its rows and partition batch ranges with the inspector output,
then inspect the listed S3 blob keys. `partition_state_json` is a later live snapshot and is
supplemental only. A row present now but absent from `admission_scan_json` supports an
eventual-consistency or query-response hypothesis only when both
`metadata_sources_truncated` and `metadata_sources_incomplete_by_capacity` are false. A row
observed but skipped at or below the cursor supports consumer accounting investigation; no row
spanning the interval points to producer allocation or publication.

## Consistency Controls

Consumer metadata reads are strongly consistent by default. The controls below address different
failure modes and can be selected independently.

| Control | Default | Protects | Cost and limits |
| --- | --- | --- | --- |
| `eventual_metadata_reads` | Unset (strong reads) | Reduce DynamoDB metadata-query RRUs | Opts into eventual metadata reads. Its `visibility_delay` defaults to two seconds when unset; this best-effort margin does not make paginated scans atomic or stop stale producer publication. |
| `fenced_metadata_writes` or `blob_stream_broker_fenced_metadata_writes` | `false` | A former producer-lease holder publishing after replacement | Each publication becomes a DynamoDB transaction. It conditions metadata publication on every current lease, requires additional IAM actions, and is limited to 99 producer-lease checks per plan. |
| `blob_stream_broker_max_segment_bytes` | `max_segment_bytes` config, 64 MiB by default | Oversized time-triggered objects | Positive integer override for newly selected objects. A partition batch larger after serialization and compression is emitted alone. |

Strong reads have no eventual-read visibility margin; the broker's metadata publication deadline
and the consumer's configured `max_clock_skew` still apply to the Fast and checkpoint horizon.
Fenced writes retain a durable holder ID, lease epoch, and session ID in every producer lease row
regardless of whether the mode is enabled.

A lost publication fence after blob upload leaves an unreferenced blob but does not publish its
metadata. The broker returns a retryable failure. There is no unfenced fallback for that plan.

## Runtime Feature Flags

Feature flags are local process controls. `TopicConfig.metadata_window_size` defines the durable
metadata-key layout used by broker publication and consumer scans.
`ConsumerIteratorBootstrapConfig.broker_discovery` is required. Consumers read metadata and raw
blob ranges through brokers. Eventual `Tail` and `FullRecovery` metadata requests can use retained
cache coverage, while strong reads are coalesced without retained cache. An unusable broker
metadata response retries the original direct DynamoDB query; any non-`NOT_FOUND` broker blob
response or rejected payload retries the affected blob group directly from storage.

| Scope | Flags | Adoption | Operational effect |
| --- | --- | --- | --- |
| Consumer reader | `blob_stream_consumer_prefetch_max_bytes`, `blob_stream_consumer_max_in_flight_batch_reads` | Live | Configures prefetch capacity and range-read concurrency. Metadata consistency is static `runtime.read` configuration. Non-`NOT_FOUND` broker blob failures retry the full affected group directly. |
| Broker metadata-cache startup | `blob_stream_broker_metadata_recovery_cache_max_bytes`, `blob_stream_broker_metadata_cache_max_waiters_per_key`, `blob_stream_broker_metadata_cache_max_waiters`, `blob_stream_broker_metadata_cache_max_refills`, `blob_stream_broker_metadata_cache_max_request_partitions`, `blob_stream_broker_metadata_cache_max_response_items`, `blob_stream_broker_metadata_cache_max_response_bytes`, `blob_stream_broker_metadata_cache_max_entry_items` | Restart the broker | Overrides the internal cache defaults when a feature-flag loader is configured. Values must be positive; the per-key waiter limit cannot exceed the global waiter limit, and the response-byte limit is capped at the gRPC request maximum. |
| Broker blob-cache startup | `blob_stream_broker_blob_cache_idle_ttl_ms` | Restart the broker | Sets positive idle retention for complete immutable blobs; absent means 10 seconds. Blob requests remain limited to 16 MiB; broker responses allow the effective `max_segment_bytes` amount of requested compressed bytes, covering normal segment objects. The broker admits each object from its reported content length and current cgroup headroom before reading its body. Cgroup-aware memory admission can flush entries or disable cache admission without disabling direct consumer reads. |
| Broker write path | `blob_stream_broker_flush_max_bytes`, `blob_stream_broker_flush_max_delay_ms`, `blob_stream_broker_max_segment_bytes`, `blob_stream_broker_adaptive_flush_max_delay_enabled`, `blob_stream_broker_adaptive_flush_max_delay_floor_ms` | Live | A time-due local partition always pulls every available buffered non-draining local topic section into bounded shared objects. Positive byte overrides and positive delay overrides no greater than `flush_max_delay` apply at the next scheduler timer cycle; invalid values retain static configuration. Adaptive delay defaults on and may be disabled statically with `adaptive_flush_max_delay_enabled: false` or live with its watched flag. Its static or watched floor defaults to half the live maximum delay and must be positive and no greater than that maximum. Three consecutive split plans derive a proportional target from their extra objects, then reduce the delay by one quarter of the distance to it; three consecutive successful unsplit plans recover half the remaining headroom. Every adjustment requires a new three-plan streak, and failures or opposite outcomes reset the streak. Positive segment-cap overrides apply to future flush plans. Independently byte-triggered and lease-drain work remain local. |
| Consumer startup | `blob_stream_consumer_idle_poll_delay_ms`, `blob_stream_consumer_max_idle_poll_delay_ms`, `blob_stream_consumer_lease_duration_ms`, `blob_stream_consumer_heartbeat_interval_ms`, `blob_stream_consumer_rebalance_interval_ms` | Rebuild or restart the iterator | Changes local polling and consumer-group scheduling. Persistent lease state stores absolute expiry timestamps, so members may use different local durations. |
| Producer batching and dispatch | `blob_stream_producer_max_batch_records`, `blob_stream_producer_max_batch_bytes`, `blob_stream_producer_flush_max_delay_ms`, `blob_stream_producer_retry_base_delay_ms`, `blob_stream_producer_retry_max_delay_ms`, `blob_stream_producer_request_timeout_ms`, `blob_stream_producer_compression` | Live | Batch limits apply to the next `produce` admission, flush delay to the next timer cycle, and retry/request/compression settings to the next broker group dispatch. A group and all of its retries retain one snapshot. |
| Producer startup | `blob_stream_producer_connect_timeout_ms`, `blob_stream_producer_max_request_concurrency` | Recreate or restart the producer | These settings construct cached gRPC clients and producer-wide dispatch/request permit pools. |

Validate feature-flag rollouts against the configured fallback values. Missing or nonpositive broker
write-path overrides, broker delay overrides above `flush_max_delay`, and adaptive floors above the
live delay ceiling retain the configured value. An explicit static adaptive floor above
`flush_max_delay` fails broker construction. Invalid producer overrides fail construction; a running
producer retains its previous effective values when a runtime snapshot is invalid. Invalid consumer
startup duration overrides fail iterator construction. A running consumer continues with its
already-applied startup settings.

## HTTP Endpoints

The broker listener serves gRPC plus these HTTP endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /metrics` | Prometheus text format (`text/plain; version=0.0.4`) |
| `GET /admin/state` | JSON snapshot of broker configuration, discovery, local buffers, and durable lease state |
| `GET /admin/metadata-cache` | Aggregate bounded metadata-cache capacity, age, refill, waiter, and failure state |
| `GET /admin/blob-cache` | Aggregate raw blob-cache retention, active fetch, memory headroom, and failure state; never exposes object keys or bytes |
| `POST /admin/log?rust_log=<filter>` | Replaces the active Rust log filter without restarting the broker |

Protect the admin listener according to the deployment's network policy. `/admin/log` accepts a log
filter such as `blob_stream=debug,bd=debug`; use trace only for targeted investigations.

Consumer state responses include `suspected_lagging_partitions` when the group-lease lookup
succeeds. Each entry is the complete observed lease snapshot for a partition whose committed source
checkpoint is not in the current topic-configured metadata window, including entries with no source
checkpoint. This is a quick signal for stalled, high-write partitions rather than a delivery
guarantee; correlate it with the owner, heartbeat, and committed checkpoint before acting.

## Reading Broker State

`/admin/state` includes the snapshot timestamp, local holder ID and writer ID, configured and
effective flush limits, configured and effective segment-byte caps, and discovered membership. Its
ownership list covers writer-scoped partitions and reports the planned
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

`durable_topics` is a read-only, deployment-wide complement to the local state: it lists every
configured virtual partition, its producer lease fence, expiry, and allocation high-water mark, and
the active leases for every consumer group. `durable_consumer_lease_scan` describes the single
consumer-table scan. A `missing` producer lease or an empty consumer list is meaningful only when
the corresponding lookup completed; `unavailable`, `lookup_failed`, and `timed_out` mean the view
is partial and should not be used to infer inactivity.

Producer lease diagnostics include `lease_sequence_start`, `max_allocated_seq`,
`last_handed_out_seq`, and `sequence_progress_updated_at`. The first two bound the sequences
reserved by the active lease session; `last_handed_out_seq` is null until the current broker
process has allocated a sequence. These informational fields are refreshed with successful lease
mutations, never used as inputs to fencing, allocation, replay, or recovery.

### Metadata Cache State

`/admin/metadata-cache` reports only aggregate cache state: Tail and Full Recovery entry counts,
retained bytes and byte budgets, oldest retained-entry ages, in-flight refills, active waiters,
available refill permits, and total failures. It intentionally contains no topic, window, or
partition identifiers. Use sustained full byte budgets, eviction growth, exhausted refill permits,
or overload failures to decide whether the configured cache limits need capacity or workload
changes. A broker cache overload is safe for delivery because consumers retry the original direct
metadata scan.

## Metrics

The broker serves its Prometheus registry at `GET /metrics`. Producer and consumer libraries add
metrics to the `bd-server-stats` scope supplied by their embedding application. See the complete
[Metrics reference](metrics.md) for names, scopes, types, and meanings.

For broker alerts, focus on gRPC timeouts and status failures, metadata-cache overloads and
evictions, blob-cache overloads/failures/pressure flushes, write admission rejections and flush
failures, metadata-publication deadline exhaustion, sequence-reservation failures, adaptive delay,
and memory admission state. Pair an unexpectedly low `adaptive_flush_max_delay_ms` with
`flush_max_segment_size_splits_total` before reducing the segment cap or changing flush limits.
Pair ownership or write-admission failures with `/admin/state`, metadata-cache pressure with
`/admin/metadata-cache`, and raw blob-cache pressure with `/admin/blob-cache`, before changing
capacity or routing.

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
4. For eventual-read consumers, decide whether the configured visibility margin is adequate;
   remove `eventual_metadata_reads` to return to strong reads when replica staleness is the
   relevant risk and the RRU increase is acceptable.
5. Verify that broker and consumer clocks remain within the consumer's `max_clock_skew` bound;
   investigate time-source or clock-health alerts before widening the availability horizon.

### Stale Producer Publication Must Be Rejected

1. Enable `fenced_metadata_writes` through static configuration or its watched runtime flag.
2. Confirm broker IAM includes `TransactWriteItems` and `ConditionCheckItem` for both affected
   DynamoDB tables.
3. Monitor publication failures and retry behavior during rollout.
4. Remember that strong reads do not add publication fencing; they solve a separate read-side issue.

### Consumer Recovery Is Expensive

1. Inspect consumer metadata scan and blob-range metrics in the embedding application's
   `bd-server-stats` registry.
2. Compare recovery work with topic retention and the availability horizon: publication lag,
   the consumer's `max_clock_skew`, and the effective visibility delay.
3. Use [Cost analysis](cost-analysis.md) with observed page and range-read rates before selecting
   eventual-read or transactional-publication modes.

### Broker Blob Delivery Falls Back

1. Compare `broker_blob_range_requests` with the sum of `broker_blob_range_successes`,
   `broker_blob_range_fallbacks`, and `broker_blob_range_not_found_groups`. Compare
   `broker_blob_range_bytes` with direct `fallback_blob_range_bytes`; one fallback group can
   result in multiple `fallback_blob_range_requests`.
2. Inspect broker `blob_cache` overloads, storage failures, pressure flushes, retained bytes, and
   whole-object fetch volume. Check `/admin/blob-cache` for headroom and active fetches.
3. Verify broker discovery returns only healthy local brokers and that the broker has
   `s3:GetObject` on the configured prefix.
