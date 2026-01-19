# Plan

## Glossary
- **topic**: A named stream of records (e.g., "telemetry").
- **partition_count**: Fixed number of logical partitions per topic (default 128).
- **logical_partition_id**: Partition derived from `hash(record_key) % partition_count`.
- **writer_id**: Integer `0..num_writers-1` assigned per producer cluster via static config.
- **num_writers**: Number of producer clusters writing to a topic.
- **virtual_partition_id**: `logical_partition_id + (writer_id * partition_count)`. Unique
  per (logical partition, writer cluster) pair.
- **window**: Time bucket (e.g., 5-minute intervals) used for partitioning metadata in Dynamo.
- **seq**: Monotonically increasing per-record sequence number scoped to a virtual
  partition. The broker assigns a seq to each record as it ingests it, then emits
  seq ranges (seq_start/seq_end) for each flushed batch/segment. Cursors track the
  highest seq_end consumed.
- **segment**: A blob containing batched records, stored in S3.

## Goals
- Create a Kafka-like system tailored to this use case with the following properties:
  - Within a topic partition, order does not matter
  - All storage will use blob store (initially S3)
  - Metadata will be stored in a KV store (initially Dynamo)
  - Hashing of inserted items to partitions is required
- System can trivially be auto scaled
- Low cost is more important than latency
- Rust is the only initial target for libraries. There will be a producer library
  as well as a consumer library. This repo will contain the libraries as well as
  the broker binary.

## Requirements (refined)
- Workload: high-volume telemetry data. Target: maximize throughput and minimize cost.
- Delivery: at-least-once semantics.
- Consumer model: pull-based (Kafka-like).
- Latency: 1-2s from produce to consume in common case is acceptable.
- Partitions: ~128 per topic, primarily for consumer auto scaling.
- Consumer groups: single group per topic today, but support multiple groups if low cost.
- Metrics/observability: required for throughput, lag, error rates, etc.
- Acking: producer acks after metadata write succeeds.
- Retention: 3-7 days using Dynamo TTL and S3 lifecycle policies (no compaction).
- Deployment: stateless broker service on Kubernetes behind a load balancer.
- Payloads: opaque (no schema validation in v1).
- Consumer API: cursor-based only (no timestamp-based reads in v1).
- Access control: out of scope for v1.
- Idempotency: out of scope for v1. Duplicates may occur on producer retry.
  Consumer applications are responsible for deduplication if needed (e.g., via
  application-level record IDs). See "Idempotency considerations" section.

## Implementation guidelines
- Protos must live in this repo; generated code is committed. Follow the
  shared-core/bd-proto patterns.
- Configuration must be defined in proto schemas. Load config from YAML/JSON by
  decoding into the proto types using a well-defined decoder.
- gRPC should use shared-core/bd-grpc.
- Metrics should use shared-core/bd-stats.
- Tokio is the async runtime for all components.
- Each component is defined behind an async trait to allow mocking and isolated
  implementation. This includes all data stores.
- Data-store-specific logic is confined to the implementation layer (e.g., S3 details
  stay inside the S3 backend), enabling easier testing and swapping providers.
- Use debug/trace logging liberally. Info and above must not be spammy in steady
  state. Use bd-log’s warn_every for noisy warnings.

## Proposed defaults (configurable)
- Blob segment rollovers: size-based with a target 64 MiB (min 1 MiB, max 128 MiB)
  and time-based flush with max delay 1s to balance latency vs S3 PUT cost.
- Compression: zstd per partition batch inside a shared blob segment. Compression is
  optional at producer; the broker may recompress at segment assembly time.
- Retention: S3 lifecycle (days) with Dynamo TTL matching lifecycle minus a buffer.
- Configuration: broker config file/env layer for rollover size, flush delay,
  compression, retention, and datastore settings (blob store: in-memory/S3;
  metadata store: in-memory/Dynamo).
- Cursor model: per-virtual-partition seq cursor to reduce coordination cost.
- Consumer leases: 10s heartbeat, 30s lease duration (configurable).
- S3 layout: single bucket with per-topic prefixes.
- S3 key format: `<topic>/<window_ts>/<snowflake_id>.zst` where window_ts is
  Unix epoch seconds (rounded to window boundary).

## Architecture sketch
- Producer library:
  - Hash key -> logical partition id -> virtual partition id.
  - Batches records per virtual partition and sends to broker.
  - Retries on transient errors; may produce duplicates (at-least-once).
- Broker:
  - Receives batches, appends to in-memory partition buffers.
  - Periodically flushes buffers into shared blob segments.
  - Writes metadata entries to Dynamo after blob upload completes.
- Consumer library:
  - Pulls assigned virtual partitions.
  - Reads Dynamo time windows for new segments and byte ranges.
  - Uses S3 byte range downloads and decompresses batches.
- Metadata (Dynamo):
  - Partitioned by topic and time window.
  - Items include blob key, per-virtual-partition byte ranges, compression info,
    record count, time window, batch-level min/max event timestamps, writer_id,
    virtual_partition_id, and seq_start/seq_end.
- Consumer group coordination (Dynamo):
  - Cooperative sticky balancing with periodic heartbeats and lease ownership.
  - Rebalancing triggered by membership changes or lease expirations.

### Wire protocol (gRPC)
- Producer -> Broker:
  - `ProduceBatch(topic, writer_id, virtual_partition_id, records[]) -> ProduceAck`
  - `ProduceAck` contains: `seq_start`, `seq_end`, or error code.
- Broker error codes:
  - `OK`: Batch accepted and metadata written.
  - `NOT_LEASE_HOLDER`: Broker does not hold lease; producer should refresh broker
    list and retry.
  - `UNKNOWN_TOPIC`: Topic not found in `topics` table.
  - `INVALID_WRITER_ID`: writer_id out of range for this topic.
  - `OVERLOADED`: Broker is at capacity; producer should backoff and retry.

## Broker discovery and partition routing
### Goals
- Producers can route partitions to brokers without a centralized stateful router.
- Keep brokers stateless for storage, with lightweight lease fencing for routing.

### Routing model (stateless)
- Producers use a stable broker list from service discovery (K8s service or DNS).
- Partition -> broker mapping uses consistent hashing on (topic, virtual_partition_id).
- Each broker accepts any partition, but consistent hashing minimizes movement when
  broker membership changes.
- When broker membership changes, producers refresh the broker list and re-hash.

### Partition assignment
- Partitions are logical and fixed per topic (default 128). Producers derive
  partition_id via hash(record_key) % partition_count.
- Brokers do not own partitions durably; they only acquire short-lived leases for
  virtual partitions to prevent split-writes during routing changes.

### Virtual partitions (multi-cluster safe)
- Goal: keep single-writer semantics per virtual partition while allowing many
  producer clusters/brokers to write to the same topic.
- Formula:
  - logical_partition_id = hash(record_key) % partition_count
  - virtual_partition_id = logical_partition_id + (writer_id * partition_count)
  - where writer_id is 0..num_writers-1, assigned per cluster via static config.
- This guarantees all virtual partitions for a given key map to the same logical
  partition, preserving key co-location for consumers.
- Each writer_id is stable per producer cluster; brokers within the cluster enforce
  single-writer semantics via leases.
- writer_id assignment: Each producer cluster is configured with a unique writer_id
  (integer). This is static configuration, not dynamically discovered.
- Consumers merge virtual partitions that map to the same logical partition by
  iterating: for each logical partition, consume virtual partitions at offsets
  logical_partition_id + (i * partition_count) for i in 0..num_writers-1.

### Virtual partition ordering guarantee
- Ordering of events is not required, but metadata for a given virtual partition
  must be monotonic to support cursor-based reads.
- The broker assigns a monotonic sequence number per virtual partition and
  includes (seq_start, seq_end) in metadata.
- To avoid expensive transactions on every flush, brokers use a "Sequence Reservation"
  (Hi-Lo) pattern. The broker reserves a large block of sequences (e.g., +1000)
  in `producer_partition_leases` and increments in-memory.
- Ideally, `max_allocated_seq` is stored in `producer_partition_leases` and only updated
  when the broker exhausts its reservation.
- Producers serialize sends per virtual partition (single in-flight) so seq
  ranges never overlap or arrive out of order.
- Brokers enforce fencing via leases and reject writes from non-lease-holders,
  forcing producer retry/re-route.

### Autoscaling with virtual partitions
- Brokers are stateless for storage; routing changes only affect which broker
  receives a batch, not data correctness.
- Producers maintain a broker list from service discovery and use consistent hashing
  on (topic, virtual_partition_id) to select a broker.
- When brokers scale up/down, producers re-hash to new brokers. During refresh delay,
  some batches may still go to old brokers; brokers use leases to prevent
  concurrent writers per virtual partition.
- To minimize churn, use rendezvous (highest-random-weight) hashing or similar.
- Optional: brokers can advertise a broker-list version to trigger producer refresh.
- Split-write prevention: brokers require a lease for (writer_id, virtual_partition_id)
  before accepting a batch (stored in producer_partition_leases). If a producer hits
  a broker without the lease, it gets a retry/refresh signal and re-routes.

### Consumer model with virtual partitions
- Cursor is per virtual partition; committed_cursor is stored per virtual partition
  in consumer_group_leases.
- Consumers assigned a logical partition will own the set of its virtual partitions.
- Rebalance moves virtual partitions as a unit, preserving per-virtual-partition
  monotonic progress.
- Cursor is based on (seq_end) for each virtual partition; re-scan windows catch
  delayed metadata without replaying earlier seq ranges.

### Virtual partition discovery
- `partition_count` and `num_writers` are fixed configuration per topic.
- These values are stored in a `topics` table in DynamoDB (see schema).
- Consumers fetch this configuration on startup to self-configure.
- Consumers enumerate virtual partitions for a logical partition using:
  virtual_partition_id = logical_partition_id + (i * partition_count) for i in 0..num_writers-1.
- No dynamic discovery needed; consumers are configured with partition_count and
  num_writers.

## Complex flow examples (multi-cluster)
Assumptions:
- At least two Kubernetes clusters (A and B) produce into the same topic.
- Each cluster uses its own broker deployment with k8s service discovery.
- Virtual partitions are enabled; each cluster has a stable writer_id.
- Brokers use producer_partition_leases to fence virtual partition writers.

### Flow 1: Broker autoscaling in cluster A
1. Cluster A brokers scale from 3 to 5.
2. Producers in cluster A refresh broker list and re-hash virtual partitions.
3. During refresh delay, some virtual partitions still route to old brokers.
4. Old brokers accept writes only if they still hold the lease for the virtual
  partition; otherwise they reject and producers retry.
5. Consumers continue scanning by window and process new ranges.
Outcome:
- No writes are lost; lease fencing prevents concurrent writers per virtual partition.
- No replay beyond normal at-least-once; cursors advance per virtual partition.

### Flow 2: Broker autoscaling in cluster B during low throughput
1. Cluster B scales down brokers; producers still use stale broker list briefly.
2. Some writes fail or go to a broker that is terminating; producers retry.
3. Retries are re-hashed with the updated broker list and succeed after lease check.
4. Metadata appears later than usual; consumers re-scan trailing windows.
Outcome:
- Delayed metadata is picked up by re-scan; at-least-once is preserved.
- Possible duplicates only if producer retries after a partial failure.

### Flow 3: Consumer autoscaling (scale up)
1. Consumer group scales from 4 to 8 instances.
2. Coordinator recomputes assignment and moves virtual partitions.
3. New owners acquire leases, start at committed_cursor, and re-scan trailing windows.
4. Old owners stop fetching and commit final cursor on heartbeat.
Outcome:
- No missed data: re-scan catches late metadata.
- Minimal replay: skip ranges that end at or before committed_cursor.

### Flow 4: Consumer autoscaling (scale down) with delayed metadata
1. Consumer group scales down; some partitions are revoked.
2. Old owners commit cursor; new owners acquire leases.
3. A metadata write for a prior segment appears after the rebalance.
4. New owners re-scan trailing windows and process the late range.
Outcome:
- Late data is processed without reprocessing already committed ranges.
- No loss because lag window overlaps with metadata delay.

### Worked examples: Hi-Lo reservations + delayed read look-back
These examples show why the Hi-Lo sequence reservation cannot cause missed reads and
how the look-back window closes the gap created by delayed metadata writes.

#### Example A: Normal reservation, no delay
Assume virtual_partition_id=17, seq is per-virtual-partition, window size 5 min.
1. Broker acquires lease and reserves seq block [1000..1999].
2. It flushes a segment with seq_start=1000, seq_end=1039 at time T, writes metadata
   into window W.
3. Consumer scans W, sees seq_end=1039, advances committed_cursor to 1039.
Proof of no loss:
- Every record has a seq in the reserved range; metadata includes seq_start/seq_end.
- Consumer advances cursor based on seq_end; no gaps inside a segment are possible
  because the broker assigns seqs monotonically within its reservation.

#### Example B: Reservation survives broker crash (gap is OK)
1. Broker A reserves [2000..2999], assigns up to 2050, then crashes.
2. Lease expires; Broker B acquires lease and reserves [3000..3999].
3. Next segment is seq_start=3000, seq_end=3075.
Observation:
- Seq range [2051..2999] is a gap with no data. Gaps are allowed; regression is not.
- Consumer sees seq_end jump from 2050 to 3075 and advances cursor. There is no
  missing data because there were no records assigned seqs in the gap.
Proof of no loss:
- The only way to miss data would be if a segment with seq_end <= committed_cursor
  arrives later. That is prevented by monotonic assignment per lease + fencing.

#### Example C: Delayed metadata write + look-back
Assume consumer look-back scans the last 3 windows.
1. Broker flushes segment S with seq_start=4000, seq_end=4099 at time T in window W.
2. S3 upload completes at T+4s, but metadata write to Dynamo is delayed until T+90s
   (now in window W+1 due to clock rounding).
3. Consumer scanned W at T+20s and did not see S.
4. At T+2m, consumer scans look-back windows {W-1, W, W+1}. It now sees S in W+1.
5. Consumer compares S.seq_end (4099) with committed_cursor (e.g., 3950) and processes
   S because seq_end > committed_cursor, then advances to 4099.
Proof of no loss:
- Look-back guarantees that any metadata that arrives late but still within the
  lag window will be re-discovered.
- Cursor-based dedupe ensures we do not reprocess older ranges while still
  accepting late-arriving ranges.

#### Example D: Delayed metadata + re-reservation (fencing + monotonicity)
1. Broker A holds lease and reserves [5000..5999]. It flushes seq_end=5050 but the
   metadata write is delayed.
2. Broker A loses lease; Broker B acquires and reserves [6000..6999]. It flushes
   seq_end=6025 quickly and metadata appears immediately.
3. Consumer sees seq_end=6025 first, advances cursor to 6025.
4. Later, delayed metadata for seq_end=5050 arrives in a look-back window.
5. Consumer skips it because seq_end (5050) <= committed_cursor (6025).
Proof of no loss:
- No records are missed: the late metadata corresponds to seqs that are strictly
  lower than the committed cursor, meaning all higher seqs were already processed.
- This is safe because the lease fencing prevents two brokers from assigning
  overlapping seq ranges for the same virtual partition.

#### Key invariants (why we can’t miss writes)
- Per virtual partition, seqs are strictly monotonic across leases (Hi-Lo blocks
  never overlap; gaps are allowed). This prevents out-of-order or duplicate seqs.
- Consumers only advance committed_cursor forward based on seq_end.
- Look-back re-scans recent windows so delayed metadata will be discovered.
- Any late metadata with seq_end <= committed_cursor is safely ignored.

## Balancing and rebalancing behavior
### Goals
- Avoid duplicate processing during rebalances beyond at-least-once guarantees.
- Ensure new owners resume from a committed cursor and do not skip delayed writes.

### Lease and fencing model
- Each virtual partition has a lease row in consumer_group_leases with owner_id,
  generation, and lease_expiry_ts.
- Ownership transfer requires a new generation; old owners are fenced once the
  generation increments.
- Commits include (partition_id, generation, cursor) and are rejected if the
  generation is stale.
- Coordination uses Dynamo conditional writes (no separate leader election). A
  designated member attempts a lightweight "coordinator" role by updating a
  group-level heartbeat row stored in consumer_group_leases; if it fails, another
  member can take over.

### Rebalance flow (cooperative sticky)
1. Members heartbeat; coordinator detects membership change or lease expiry.
2. Compute new assignment with sticky preference to minimize movement.
3. For virtual partitions to revoke, current owners stop fetching and commit final
  cursor.
4. New owners acquire leases with a higher generation and start reading at the
  committed cursor (or earliest if none).

### Commit on heartbeat
- Consumers always include committed_cursor in the heartbeat write to avoid extra
  Dynamo writes. Conditional update ensures only the current lease owner can
  advance the cursor.

### Delayed write handling during rebalance
- Consumers always re-scan trailing windows using the lag window, even on first
  assignment, so delayed metadata is not missed.
- New owners start from committed_cursor and re-scan the trailing window. During
  re-scan, the consumer skips any seq range that ends at or before committed_cursor
  for that virtual partition, which avoids reprocessing while still catching late writes.
- If a commit arrives after a lease is revoked, it is rejected due to generation
  mismatch, preventing old owners from moving the cursor forward incorrectly.

## Consumer discovery & delayed write handling
### Goals
- Consumers must discover new data with low cost and tolerate delayed metadata writes.
- Avoid lost writes when metadata arrives after the consumer has scanned past a window.
- Retention TTL on blob_segments must not delete data before all consumers are done.

### Discovery path
- Primary mode: consumers read Dynamo directly (time-window scans) using eventually
  consistent reads to minimize cost.
- Broker is not used in the read path (used only for coordination or future cache).
- Each consumer keeps a per-virtual-partition cursor and a per-topic scan watermark.

### Scan algorithm (time windows)
Notes:
- Window scans are unordered for lowest cost; callers sort client-side only if needed.
1. Determine current scan window based on wall clock and cursor.
2. Scan window items from blob_segments (topic#window).
3. Filter the segment_index for assigned virtual partitions only.
4. For each virtual partition, read byte ranges from S3 and advance cursor.
5. Repeat until caught up, then sleep/poll on a short interval.

### Recommended defaults (configurable)
- Re-scan window: last 2-3 windows (10-15 minutes with 5-minute windows) or
  last 2-5 minutes if we make window size smaller. This protects against late
  metadata writes without large extra read cost.
- Poll interval: 500ms to 1s when caught up; backoff to 2-5s during low throughput.
- Optional optimization: DynamoDB Streams on blob_segments can be used to notify
  consumers of new items, but this adds operational complexity and still requires
  periodic scans for catch-up and fault tolerance.

### Handling delayed metadata writes
- The broker writes metadata after blob upload completes. Metadata can lag by seconds.
- Consumers must re-scan a small trailing window to catch late writes.
- Maintain a configurable lag window (e.g., re-scan last N windows or last M minutes).
- Consumers dedupe by cursor (seq_end) to avoid reprocessing.

### Expected metadata delay + look-back efficiency
- Typical delay should be seconds (S3 upload + Dynamo write). Longer delays (tens of
  seconds to a few minutes) can happen during transient retries, throttling, or
  broker failover. Look-back should cover the worst-case delay + clock skew.
- The partition key is the window (topic#window), so efficient queries are scoped to
  a single window. The snowflake sort key can be used to bound reads within a window
  (e.g., SK >= lower_bound) to avoid scanning the entire window.
- We cannot efficiently query “last 1 minute” across multiple windows without
  issuing separate queries per window (or adding an index). So a 5-minute window
  does not force a full 5-minute scan, but it does define the PK granularity.
- Recommendation: keep 5-minute windows and, on look-back boundaries, issue up to
  two window queries (previous + current) with a snowflake lower-bound in each.
  This keeps reads bounded without extra indexes; only reduce window size if scan
  cost remains too high in practice.
- If we want a true 1-minute look-back with minimal reads, options are:
  1) reduce the window size to 1 minute (more PKs, smaller per-query scans), or
  2) add a secondary index keyed by time (higher write cost/complexity).

### TTL and safety
- blob_segments TTL must exceed maximum consumer lag + re-scan window + retention buffer.
- Example: retention 7 days, TTL 7 days + 1 hour buffer.
- Consumers should not assume windows are complete until TTL horizon passes.

### Broker vs direct read decision
- Default: direct Dynamo + S3 reads for lowest cost.
- Broker read path could be added later for caching, throttling, or auth control.

### Local vs shared scan watermark
- Local-only watermark (per consumer): simplest and cheapest, but each consumer
  does its own scanning and may duplicate read work.
- Shared watermark (per group/topic): reduces duplicate scans, but requires extra
  coordination writes and risk of stalling if the shared watermark advances too
  aggressively on delayed metadata. Given cost focus, start with local-only.

### Brokerless write path (producer -> S3/Dynamo)
- Pros: fewer services, lower infra cost, potentially lower latency.
- Cons: more complex producer library (S3 multipart upload, retries, metadata
  consistency), harder to enforce schemas/auth, and harder to batch/compress
  across producers. Also complicates coordination if producers must assemble
  shared blob segments.
- Recommendation: keep broker for now as write aggregator and metadata authority,
  revisit brokerless writes only if cost is dominated by broker compute.

### Window size clarification
- Dynamo write rate is driven by flush frequency, not window size.
- Window size primarily affects read scan size, hot-partition risk, and how much
  history consumers re-scan for late metadata.

### Consumer "earliest" behavior
- When a consumer takes ownership of a virtual partition with no committed_cursor
  (first-time assignment or cursor reset), it starts from the oldest available
  window in blob_segments that has not yet exceeded TTL.
- This ensures no data is missed on fresh consumer groups.
- For "latest" semantics (skip old data), the consumer can initialize committed_cursor
  to the current max seq before starting.

### Idempotency considerations
- At-least-once delivery means producers may retry failed batches, causing duplicates.
- The system does not enforce exactly-once semantics in v1.
- If consumers require deduplication, they have two options:
  1. Application-level record IDs: Producers include a unique ID per record (e.g.,
     UUID or hash of record content). Consumers track seen IDs.
  2. Stateless dedup: If records are idempotent by nature (e.g., full state snapshots),
     no dedup is needed.
- Future: The broker could support producer-side dedup via (producer_id, sequence)
  pairs, similar to Kafka's idempotent producer, but this adds significant complexity.

## Milestones
- [x] Milestone 0: Repo scaffolding + proto baseline
  - [x] Define proto files in this repo following shared-core/bd-proto patterns.
  - [x] Commit generated code for Rust (and any other planned languages).
  - [x] Add minimal gRPC service wiring using shared-core/bd-grpc.
  - [x] Define configuration protos (topic config, runtime config) and implement
        YAML/JSON decoding into proto types via a well-defined decoder.

- [x] Milestone 1: Core data model + shared types
  - [x] Define record/batch types (record format, batch metadata, compression metadata).
  - [x] Define cursor types (seq_start/seq_end, committed_cursor).
  - [x] Define DynamoDB schema constants and key builders (topic/window, snowflake id).
  - [x] Add unit tests for serialization/deserialization and key building.

- [x] Milestone 2: Blob storage (trait + implementation)
  - [x] Define async trait for blob storage (put/get range/delete).
  - [x] Implement in-memory blob storage for tests.
  - [x] Implement blob storage using S3 (encapsulate S3 details inside the impl).
  - [x] Add unit tests for blob storage trait behavior.

- [x] Milestone 3: Segment metadata store (trait + implementation)
  - [x] Define async trait for metadata store (write segment metadata, scan windows).
  - [x] Document unordered scan behavior (callers sort client-side if needed).
  - [x] Implement in-memory metadata store for tests.
  - [x] Implement metadata store using DynamoDB (window scans, segment_index writes).
  - [x] Add unit tests for metadata store scans and writes.
  - [x] Add datastore config selection (in-memory/S3/Dynamo) to config protos.

- [x] Milestone 4: Producer partition leases (trait + implementation)
  - [x] Define async trait for producer partition leases (acquire/heartbeat/reserve seq).
  - [x] Implement in-memory lease store with Hi-Lo reservation semantics.
  - [x] Implement producer partition leases with Hi-Lo reservation (DynamoDB).
  - [x] Add unit tests for lease fencing and sequence reservation.

- [ ] Milestone 5: Consumer group leases (trait + implementation)
  - [ ] Define async trait for consumer group leases (heartbeat/commit/assignment).
  - [ ] Implement in-memory consumer group leases.
  - [ ] Implement consumer group leases (DynamoDB).
  - [ ] Add unit tests for heartbeat, commit, and assignment updates.

- [ ] Milestone 6: Broker write path (trait-first)
  - [ ] Define async trait for broker write engine (ingest -> buffer -> flush).
  - [ ] Implement in-memory buffering + rollover logic (size/time).
  - [ ] Integrate compression (zstd) and segment assembly.
  - [ ] Write unit tests for buffering, rollover, and seq assignment.
  - [ ] Implement broker gRPC handler using bd-grpc and the write trait.

- [ ] Milestone 7: Consumer read path (trait-first)
  - [ ] Define async trait for consumer reader (scan -> fetch -> decode).
  - [ ] Implement window scan + byte-range fetch + decode pipeline.
  - [ ] Implement cursor tracking and re-scan window logic.
  - [ ] Write unit tests for cursor advancement and late metadata handling.

- [ ] Milestone 8: Consumer group coordination
  - [ ] Implement cooperative sticky assignment logic.
  - [ ] Implement lease heartbeat + commit on heartbeat.
  - [ ] Implement rebalance flow with generation fencing.
  - [ ] Add tests for assignment stability and lease fencing behavior.

- [ ] Milestone 9: Observability + logging
  - [ ] Add bd-stats metrics for broker/producer/consumer throughput and lag.
  - [ ] Add structured logging with debug/trace for hot paths.
  - [ ] Apply warn_every for noisy warnings.

- [ ] Milestone 10: End-to-end integration
  - [ ] Compose broker + producer + consumer in docker compose (local S3/Dynamo).
  - [ ] Verify autoscaling behaviors (simulated broker/consumer membership changes).
  - [ ] Validate duplicate handling and cursor monotonicity under retries.

- [ ] Milestone 11: Load + cost validation
  - [ ] Run throughput and latency tests under representative load.
  - [ ] Measure DynamoDB/S3 costs vs targets.
  - [ ] Adjust rollover/window/scan defaults based on cost/latency tradeoffs.

## Open Questions
- None currently; iterate as implementation progresses.

## DynamoDB schema proposal
### Table: blob_segments
- PK: topic#window (window = 5-minute rounded time, configurable)
- SK: snowflake_id
- Attributes:
  - topic
  - window_start_ts
  - blob_key
  - segment_index (map from virtual_partition_id -> list of byte ranges + metadata)
    - entry fields: seq_start, seq_end, byte_start, byte_end, record_count
    - Note: writer_id can be derived from virtual_partition_id but may be stored
      denormalized for debugging/observability.
  - compression (codec + level)
  - record_count
  - min_event_ts
  - max_event_ts
  - checksum
  - created_ts
- Query model: time-window scans only (no GSI) to reduce write cost.
- Reads can be eventually consistent to cut RCU cost.

### Table: consumer_group_leases
- PK: topic#group_id
- SK: virtual_partition_id
- Attributes:
  - owner_id
  - lease_expiry_ts
  - generation
  - last_heartbeat_ts
  - committed_cursor (see cursor model below)
  - committed_ts

### Group coordination row (in consumer_group_leases)
- PK: topic#group_id
- SK: __group_state__
- Attributes:
  - coordinator_id
  - coordinator_lease_expiry_ts
  - generation
  - last_heartbeat_ts

### Table: topics
- PK: topic_name
- SK: __config__ (single row per topic; SK exists for future extensibility)
- Attributes:
  - partition_count
  - num_writers
  - created_ts
  - retention_days (optional, defaults to global config)

### Table: producer_partition_leases
- PK: topic#virtual_partition_id (distributed to prevent hot partitions)
- SK: lease
- Attributes:
  - broker_id
  - lease_expiry_ts (default 30s, configurable)
  - generation
  - last_heartbeat_ts
  - max_allocated_seq (Hi-Lo reservation upper bound)

### Broker lease lifecycle
- Broker acquires lease via Dynamo conditional write (if not held or expired).
- Broker heartbeats every 10s to extend lease_expiry_ts.
- On broker crash, lease expires after 30s and another broker can acquire.
- Sequence Reservation (Hi-Lo):
  - On lease acquisition/refresh, broker increments `max_allocated_seq` by a block
    (e.g. 1000).
  - Broker issues in-memory sequences up to `max_allocated_seq` without writing to Dynamo.
  - When exhausted, broker performs another Dynamo update to reserve the next block.
  - This prevents sequence regression on crash (gaps are allowed, regression is not).

### Cursor model
- committed_cursor stores (seq_end) per virtual partition.
