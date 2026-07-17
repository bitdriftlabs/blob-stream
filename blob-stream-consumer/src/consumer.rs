#[cfg(test)]
#[path = "./consumer_test.rs"]
mod tests;

use crate::config::{
  ConsumerReadConfig,
  consumer_candidate_window_count,
  consumer_metadata_visibility_delay_ms,
  consumer_window_size_seconds,
  validate_read_config,
};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use bd_server_stats::stats::Scope;
use blob_stream_blob_store::{BlobStore, ByteRange};
use blob_stream_metadata_store::MetadataStore;
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  CommittedCursor,
  CommittedSourceCheckpoint,
  CompressionCodec,
  Record,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
  Window,
  format_unix_timestamp_ms,
};
use futures::future::try_join_all;
use log::{info, trace};
use prometheus::{Histogram, IntCounter, IntGauge};
use protobuf::Message;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

const MAX_RECOVERY_WINDOWS_PER_SCAN: usize = 32;

fn format_unix_timestamp_seconds(timestamp_seconds: i64) -> String {
  format_unix_timestamp_ms(timestamp_seconds.saturating_mul(1_000))
}

//
// Scanning algorithm overview
//
// This reader uses a deterministic per-partition state machine:
//
// 1) Recover retained history
//    - A partition with a persisted cursor starts at its source checkpoint, clamped to the topic
//      retention floor. Legacy cursors without a checkpoint start at their commit time or the
//      retention floor.
//    - Recovery scans consecutive windows in chronological, bounded slices. A partition enters the
//      fast path only after the slice containing its captured cutover window is complete.
//    - Metadata deferred by the visibility delay prevents recovery from advancing beyond its
//      window, so a stale eventual read cannot skip it.
//
// 2) Scan the active publication horizon
//    - Fast partitions scan the current window and enough trailing windows to cover the configured
//      metadata publication bound plus the visibility delay.
//    - Each partition/window pair has an inclusive snowflake frontier. The query uses the lowest
//      frontier among eligible fast partitions, then each partition filters independently. This
//      finds late metadata without requiring a broad metadata projection.
//
// 3) Filter, decode, and advance cursors
//    - Metadata scans are unordered, so segments are sorted by snowflake id and each partition's
//      batches are sorted by sequence start.
//    - Only batches for assigned, eligible partitions are read. A batch whose seq_end is at or
//      below the current cursor is skipped; otherwise its referenced blob byte range is decoded.
//    - After decoding, the cursor advances monotonically and the batch carries the metadata source
//      checkpoint needed to persist an accurate recovery starting point.
//
// Metadata visibility and ordering assumptions
//
// This logic relies on the following producer and broker invariants for each virtual partition:
//
// - Single active writer via lease fencing: Brokers must hold the producer-partition lease to
//   accept writes for a virtual partition. A broker without the lease rejects writes, preventing
//   concurrent seq assignment.
//
// - Monotonic sequence assignment: The sequence allocator (Hi-Lo reservation) hands out strictly
//   increasing seq values per virtual partition. Reservations may introduce gaps, but
//   overlapping/reused seq ranges are not allowed.
//
// - Retry semantics preserve monotonic progress: If routing is stale or lease ownership changes,
//   producers retry to the current lease holder. The accepted batch still receives seq ranges from
//   the active monotonic allocator.
//
// Given these invariants, cursor-based filtering by seq_end is correct: once cursor reaches X,
// any later valid batch for the same virtual partition must have seq_end > X (or be a duplicate
// replay of already processed data that safely satisfies seq_end <= X).
//
// The fast path is bounded by the configured metadata publication deadline and visibility delay.
// Recovery covers the full configured retention duration.

//
// ConsumerBatch
//

#[derive(Clone, Debug, PartialEq)]
/// A decoded batch returned by the consumer read path.
pub struct ConsumerBatch {
  /// Virtual partition that owns this batch.
  pub virtual_partition_id: VirtualPartitionId,
  /// Inclusive sequence range for this batch.
  pub seq_range: SeqRange,
  /// Metadata source used to recover this batch after a consumer restart.
  pub source_checkpoint: CommittedSourceCheckpoint,
  /// Decoded records for the batch.
  pub records: Vec<Record>,
}

//
// ConsumerReaderMetrics
//

#[derive(Clone)]
struct ConsumerReaderMetrics {
  read_available_calls: IntCounter,
  read_available_empty: IntCounter,
  read_available_latency_seconds: Histogram,
  metadata_scan_requests: IntCounter,
  metadata_scan_segments: IntCounter,
  metadata_scan_latency_seconds: Histogram,
  metadata_fast_scan_requests: IntCounter,
  metadata_fast_scan_segments: IntCounter,
  metadata_recovery_scan_requests: IntCounter,
  metadata_recovery_scan_segments: IntCounter,
  metadata_recovery_scan_failures: IntCounter,
  metadata_recovery_scan_hits: IntCounter,
  metadata_recovery_scan_batches_read: IntCounter,
  metadata_fast_scan_frontiers: IntGauge,
  metadata_fast_scan_without_lower_bound: IntCounter,
  metadata_fast_scan_segments_below_partition_frontier: IntCounter,
  metadata_fast_scan_segments_without_assigned_batches: IntCounter,
  metadata_segments_deferred_by_visibility_delay: IntCounter,
  metadata_batches_scanned: IntCounter,
  metadata_batches_skipped_by_cursor: IntCounter,
  blob_range_requests: IntCounter,
  blob_range_bytes: IntCounter,
  blob_range_latency_seconds: Histogram,
  decompression_latency_seconds: Histogram,
  protobuf_decode_latency_seconds: Histogram,
  batches_read: IntCounter,
  records_read: IntCounter,
  record_payload_bytes: IntCounter,
}

impl ConsumerReaderMetrics {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("reader");
    Self {
      read_available_calls: scope.counter("read_available_calls"),
      read_available_empty: scope.counter("read_available_empty"),
      read_available_latency_seconds: scope.histogram("read_available_latency_seconds"),
      metadata_scan_requests: scope.counter("metadata_scan_requests"),
      metadata_scan_segments: scope.counter("metadata_scan_segments"),
      metadata_scan_latency_seconds: scope.histogram("metadata_scan_latency_seconds"),
      metadata_fast_scan_requests: scope.counter("metadata_fast_scan_requests"),
      metadata_fast_scan_segments: scope.counter("metadata_fast_scan_segments"),
      metadata_recovery_scan_requests: scope.counter("metadata_recovery_scan_requests"),
      metadata_recovery_scan_segments: scope.counter("metadata_recovery_scan_segments"),
      metadata_recovery_scan_failures: scope.counter("metadata_recovery_scan_failures"),
      metadata_recovery_scan_hits: scope.counter("metadata_recovery_scan_hits"),
      metadata_recovery_scan_batches_read: scope.counter("metadata_recovery_scan_batches_read"),
      metadata_fast_scan_frontiers: scope.gauge("metadata_fast_scan_frontiers"),
      metadata_fast_scan_without_lower_bound: scope
        .counter("metadata_fast_scan_without_lower_bound"),
      metadata_fast_scan_segments_below_partition_frontier: scope
        .counter("metadata_fast_scan_segments_below_partition_frontier"),
      metadata_fast_scan_segments_without_assigned_batches: scope
        .counter("metadata_fast_scan_segments_without_assigned_batches"),
      metadata_segments_deferred_by_visibility_delay: scope
        .counter("metadata_segments_deferred_by_visibility_delay"),
      metadata_batches_scanned: scope.counter("metadata_batches_scanned"),
      metadata_batches_skipped_by_cursor: scope.counter("metadata_batches_skipped_by_cursor"),
      blob_range_requests: scope.counter("blob_range_requests"),
      blob_range_bytes: scope.counter("blob_range_bytes"),
      blob_range_latency_seconds: scope.histogram("blob_range_latency_seconds"),
      decompression_latency_seconds: scope.histogram("decompression_latency_seconds"),
      protobuf_decode_latency_seconds: scope.histogram("protobuf_decode_latency_seconds"),
      batches_read: scope.counter("batches_read"),
      records_read: scope.counter("records_read"),
      record_payload_bytes: scope.counter("record_payload_bytes"),
    }
  }

  fn record_metadata_scan(&self, started_at: Instant, segment_count: usize, recovery_scan: bool) {
    self.metadata_scan_requests.inc();
    self
      .metadata_scan_segments
      .inc_by(u64::try_from(segment_count).unwrap_or(u64::MAX));
    let (mode_requests, mode_segments) = if recovery_scan {
      (
        &self.metadata_recovery_scan_requests,
        &self.metadata_recovery_scan_segments,
      )
    } else {
      (
        &self.metadata_fast_scan_requests,
        &self.metadata_fast_scan_segments,
      )
    };
    mode_requests.inc();
    mode_segments.inc_by(u64::try_from(segment_count).unwrap_or(u64::MAX));
    self
      .metadata_scan_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }

  fn record_fast_frontiers(&self, frontier_count: usize) {
    self
      .metadata_fast_scan_frontiers
      .set(i64::try_from(frontier_count).unwrap_or(i64::MAX));
  }

  fn record_blob_range(&self, started_at: Instant, bytes: usize) {
    self.blob_range_requests.inc();
    self
      .blob_range_bytes
      .inc_by(u64::try_from(bytes).unwrap_or(u64::MAX));
    self
      .blob_range_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }

  fn record_batch(&self, record_count: usize, payload_bytes: usize) {
    self.batches_read.inc();
    self
      .records_read
      .inc_by(u64::try_from(record_count).unwrap_or(u64::MAX));
    self
      .record_payload_bytes
      .inc_by(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
  }

  fn record_read_available(
    &self,
    started_at: Instant,
    batch_count: usize,
    metadata_batches_scanned: usize,
    metadata_batches_skipped_by_cursor: usize,
    recovery_scan: bool,
  ) {
    self.read_available_calls.inc();
    if batch_count == 0 {
      self.read_available_empty.inc();
    }
    if recovery_scan && batch_count > 0 {
      self.metadata_recovery_scan_hits.inc();
      self
        .metadata_recovery_scan_batches_read
        .inc_by(u64::try_from(batch_count).unwrap_or(u64::MAX));
    }
    self
      .metadata_batches_scanned
      .inc_by(u64::try_from(metadata_batches_scanned).unwrap_or(u64::MAX));
    self
      .metadata_batches_skipped_by_cursor
      .inc_by(u64::try_from(metadata_batches_skipped_by_cursor).unwrap_or(u64::MAX));
    self
      .read_available_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }
}

//
// ConsumerReader
//

#[async_trait]
/// Low-level batch reader over blob + metadata stores.
pub trait ConsumerReader: Send {
  /// Scan available windows and return newly available batches.
  async fn read_available(&mut self, now_unix_seconds: i64) -> Result<Vec<ConsumerBatch>>;
  /// Return committed cursor for a virtual partition, if known.
  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64>;
  /// Return all tracked cursors.
  fn cursors(&self) -> HashMap<VirtualPartitionId, u64>;
}

#[derive(Clone, Debug)]
enum PartitionReadState {
  Fresh {
    initial_window_start_unix_seconds: i64,
  },
  Recovering(RecoveryState),
  Fast,
}

#[derive(Clone, Debug)]
struct RecoveryState {
  next_window_start_unix_seconds: i64,
  cutover_window_start_unix_seconds: i64,
}

//
// ConsumerReaderPartitionMode
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerReaderPartitionMode {
  Fresh {
    initial_window_start_unix_seconds: i64,
  },
  Recovering {
    next_window_start_unix_seconds: i64,
    cutover_window_start_unix_seconds: i64,
  },
  Fast,
}

//
// ConsumerReaderPartitionState
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerReaderPartitionState {
  pub(super) virtual_partition_id: VirtualPartitionId,
  pub(super) mode: ConsumerReaderPartitionMode,
}

#[derive(Clone)]
struct ScanRequest {
  window: TopicWindowKey,
  min_snowflake: Option<SnowflakeId>,
  recovery_scan: bool,
  eligibility: ScanEligibility,
}

#[derive(Clone, Copy)]
struct ScanEligibility {
  recovering: bool,
  fast: bool,
  fresh: bool,
}

//
// ConsumerReaderImpl
//

/// Default `ConsumerReader` implementation used by `ConsumerIteratorImpl`.
pub struct ConsumerReaderImpl {
  config: ConsumerReadConfig,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  assigned_virtual_partitions: Vec<VirtualPartitionId>,
  cursors: HashMap<VirtualPartitionId, u64>,
  partition_read_states: HashMap<VirtualPartitionId, PartitionReadState>,
  retention_days: u32,
  maximum_metadata_publication_lag_ms: u64,
  fast_frontiers: HashMap<(VirtualPartitionId, i64), SnowflakeId>,
  metrics: ConsumerReaderMetrics,
}

impl ConsumerReaderImpl {
  /// Create a reader with finite retention recovery and an explicit metadata publication bound.
  pub fn new_with_retention_and_publication_lag(
    config: ConsumerReadConfig,
    assigned_virtual_partitions: Vec<VirtualPartitionId>,
    initial_cursors: HashMap<VirtualPartitionId, u64>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    metrics_scope: &Scope,
    retention_days: u32,
    maximum_metadata_publication_lag_ms: u64,
  ) -> Result<Self> {
    validate_read_config(&config)?;
    ensure!(
      retention_days > 0,
      "consumer retention recovery requires topic retention_days greater than zero"
    );
    info!(
      "consumer reader initialized: topic={}, retention_days={}, \
       maximum_metadata_publication_lag_ms={}, assigned_partitions={}, initial_cursors={}",
      config.topic,
      retention_days,
      maximum_metadata_publication_lag_ms,
      assigned_virtual_partitions.len(),
      initial_cursors.len()
    );

    let partition_read_states = assigned_virtual_partitions
      .iter()
      .copied()
      .map(|partition_id| (partition_id, PartitionReadState::Fast))
      .collect();

    // Seed runtime cursors from the caller-provided committed state.
    // Missing partitions default to cursor 0 during read processing.
    Ok(Self {
      assigned_virtual_partitions,
      cursors: initial_cursors,
      partition_read_states,
      retention_days,
      maximum_metadata_publication_lag_ms,
      fast_frontiers: HashMap::new(),
      config,
      blob_store,
      metadata_store,
      metrics: ConsumerReaderMetrics::new(metrics_scope),
    })
  }

  fn scan_windows(&self, now_unix_seconds: i64) -> Result<Vec<TopicWindowKey>> {
    // Anchor scans to the current window and cover the enforced publication deadline plus the
    // eventual-read visibility delay. Oldest -> newest ordering keeps traversal deterministic.
    let current_window =
      Window::for_timestamp(now_unix_seconds, consumer_window_size_seconds(&self.config))
        .start_unix_seconds;

    let candidate_windows =
      consumer_candidate_window_count(&self.config, self.maximum_metadata_publication_lag_ms)?;
    let window_size_seconds = consumer_window_size_seconds(&self.config);
    let mut windows = Vec::with_capacity(candidate_windows);
    for offset in (0 .. candidate_windows).rev() {
      let offset = i64::try_from(offset).unwrap_or(i64::MAX);
      let window_start = current_window.saturating_sub(offset.saturating_mul(window_size_seconds));
      windows.push(TopicWindowKey {
        topic: self.config.topic.to_string(),
        window_start_unix_seconds: window_start,
      });
    }

    trace!(
      "consumer scan windows computed: topic={}, count={}, now={}",
      self.config.topic,
      windows.len(),
      format_unix_timestamp_seconds(now_unix_seconds)
    );

    Ok(windows)
  }

  fn window_start(&self, unix_seconds: i64) -> i64 {
    Window::for_timestamp(unix_seconds, consumer_window_size_seconds(&self.config))
      .start_unix_seconds
  }

  fn retention_floor_window_start(&self, cutover_window_start_unix_seconds: i64) -> i64 {
    let retention_seconds = i64::from(self.retention_days).saturating_mul(86_400);
    self.window_start(cutover_window_start_unix_seconds.saturating_sub(retention_seconds))
  }

  fn insert_scan_request(
    scan_requests: &mut BTreeMap<i64, ScanRequest>,
    topic: &str,
    window_start_unix_seconds: i64,
    min_snowflake: Option<SnowflakeId>,
    recovery_scan: bool,
    eligibility: ScanEligibility,
  ) {
    scan_requests
      .entry(window_start_unix_seconds)
      .and_modify(|request| {
        request.min_snowflake = request.min_snowflake.min(min_snowflake);
        request.recovery_scan |= recovery_scan;
        request.eligibility.recovering |= eligibility.recovering;
        request.eligibility.fast |= eligibility.fast;
        request.eligibility.fresh |= eligibility.fresh;
      })
      .or_insert_with(|| ScanRequest {
        window: TopicWindowKey {
          topic: topic.to_string(),
          window_start_unix_seconds,
        },
        min_snowflake,
        recovery_scan,
        eligibility,
      });
  }

  fn fast_scan_min_snowflake(&self, window_start_unix_seconds: i64) -> Option<SnowflakeId> {
    let mut minimum = None;
    for partition_id in &self.assigned_virtual_partitions {
      if !matches!(
        self.partition_read_states.get(partition_id),
        Some(PartitionReadState::Fast)
      ) {
        continue;
      }
      let frontier = self
        .fast_frontiers
        .get(&(*partition_id, window_start_unix_seconds))
        .copied()?;
      minimum = Some(minimum.map_or(frontier, |current: SnowflakeId| current.min(frontier)));
    }
    minimum
  }

  fn scan_requests(&self, now_unix_seconds: i64) -> Result<(Vec<ScanRequest>, bool)> {
    let mut scan_requests = BTreeMap::new();
    let mut recovery_scan = false;

    // Scan a bounded chronological slice of retention recovery before allowing this partition's
    // fast path. A failure leaves the state untouched, so the same slice is retried.
    let oldest_recovery_window = self
      .partition_read_states
      .values()
      .filter_map(|state| match state {
        PartitionReadState::Recovering(state) => Some(state.next_window_start_unix_seconds),
        PartitionReadState::Fresh { .. } | PartitionReadState::Fast => None,
      })
      .min();
    if let Some(oldest_recovery_window) = oldest_recovery_window {
      let window_size_seconds = consumer_window_size_seconds(&self.config);
      let latest_recovery_window = self
        .partition_read_states
        .values()
        .filter_map(|state| match state {
          PartitionReadState::Recovering(state) => Some(state.cutover_window_start_unix_seconds),
          PartitionReadState::Fresh { .. } | PartitionReadState::Fast => None,
        })
        .max()
        .unwrap_or(oldest_recovery_window);
      for offset in 0 .. MAX_RECOVERY_WINDOWS_PER_SCAN {
        let offset = i64::try_from(offset).unwrap_or(i64::MAX);
        let window_start =
          oldest_recovery_window.saturating_add(offset.saturating_mul(window_size_seconds));
        if window_start > latest_recovery_window {
          break;
        }
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          window_start,
          None,
          true,
          ScanEligibility {
            recovering: true,
            fast: false,
            fresh: false,
          },
        );
      }
      recovery_scan = true;
    }

    for state in self.partition_read_states.values() {
      if let PartitionReadState::Fresh {
        initial_window_start_unix_seconds,
      } = state
      {
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          *initial_window_start_unix_seconds,
          None,
          true,
          ScanEligibility {
            recovering: false,
            fast: false,
            fresh: true,
          },
        );
        recovery_scan = true;
      }
    }

    if self
      .partition_read_states
      .values()
      .any(|state| matches!(state, PartitionReadState::Fast))
    {
      for window in self.scan_windows(now_unix_seconds)? {
        Self::insert_scan_request(
          &mut scan_requests,
          self.config.topic.as_str(),
          window.window_start_unix_seconds,
          self.fast_scan_min_snowflake(window.window_start_unix_seconds),
          false,
          ScanEligibility {
            recovering: false,
            fast: true,
            fresh: false,
          },
        );
      }
    }

    Ok((scan_requests.into_values().collect(), recovery_scan))
  }

  fn prune_fast_frontiers(&mut self, now_unix_seconds: i64) -> Result<()> {
    let windows = self.scan_windows(now_unix_seconds)?;
    if let Some(oldest_window) = windows.first() {
      self
        .fast_frontiers
        .retain(|(_, window_start), _| *window_start >= oldest_window.window_start_unix_seconds);
    }
    self
      .metrics
      .record_fast_frontiers(self.fast_frontiers.len());
    Ok(())
  }

  async fn read_batch(
    &self,
    metadata: &blob_stream_metadata_store::SegmentMetadata,
    batch_metadata: &BatchMetadata,
    virtual_partition_id: VirtualPartitionId,
  ) -> Result<ConsumerBatch> {
    trace!(
      "consumer read batch start: topic={}, partition={}, blob_key={}, seq_start={}, seq_end={}",
      self.config.topic,
      virtual_partition_id,
      metadata.blob_key.as_str(),
      batch_metadata.seq_range.start,
      batch_metadata.seq_range.end
    );
    // Pull only the referenced byte range for this batch to avoid downloading full segments.
    let blob_read_started_at = Instant::now();
    let payload = self
      .blob_store
      .get_range(
        &metadata.blob_key,
        ByteRange {
          start: batch_metadata.byte_range.start,
          end: batch_metadata.byte_range.end,
        },
      )
      .await?;
    self
      .metrics
      .record_blob_range(blob_read_started_at, payload.len());

    // Decode in two stages: transport/storage compression first, then logical RecordBatch format.
    let decompression_started_at = Instant::now();
    let decoded: Cow<'_, [u8]> = match batch_metadata.compression.codec {
      CompressionCodec::None => Cow::Borrowed(payload.as_ref()),
      CompressionCodec::Zstd => zstd::stream::decode_all(Cursor::new(payload.as_ref()))
        .map(Cow::Owned)
        .map_err(|error| anyhow!("failed to decode zstd batch: {error}"))?,
    };
    if matches!(batch_metadata.compression.codec, CompressionCodec::Zstd) {
      self
        .metrics
        .decompression_latency_seconds
        .observe(decompression_started_at.elapsed().as_secs_f64());
    }

    let protobuf_decode_started_at = Instant::now();
    let record_batch = StoredRecordBatch::parse_from_bytes(decoded.as_ref())
      .map_err(|error| anyhow!("failed to decode record batch protobuf: {error}"))?;
    self
      .metrics
      .protobuf_decode_latency_seconds
      .observe(protobuf_decode_started_at.elapsed().as_secs_f64());

    // Defensive integrity check: segment index entry and decoded payload must agree on partition.
    ensure!(
      record_batch.virtual_partition_id == virtual_partition_id,
      "decoded batch partition {} does not match expected {}",
      record_batch.virtual_partition_id,
      virtual_partition_id
    );

    let payload_bytes = record_batch.records.iter().fold(0_usize, |total, record| {
      total.saturating_add(record.payload.len())
    });
    self
      .metrics
      .record_batch(record_batch.records.len(), payload_bytes);

    Ok(ConsumerBatch {
      virtual_partition_id,
      seq_range: batch_metadata.seq_range.clone(),
      source_checkpoint: CommittedSourceCheckpoint {
        window_start_unix_seconds: metadata.window.window_start_unix_seconds,
        snowflake_id: metadata.snowflake_id.as_u64(),
      },
      records: record_batch.records,
    })
  }

  /// Replace the current assignment set.
  pub fn set_assigned_virtual_partitions(
    &mut self,
    assigned_virtual_partitions: Vec<VirtualPartitionId>,
    now_unix_seconds: i64,
  ) -> Result<()> {
    let previous_assignment = self.assigned_virtual_partitions.clone();
    if self.assigned_virtual_partitions != assigned_virtual_partitions {
      let assigned = assigned_virtual_partitions
        .iter()
        .copied()
        .collect::<HashSet<_>>();
      self
        .fast_frontiers
        .retain(|(partition_id, _), _| assigned.contains(partition_id));
    }
    let assigned = assigned_virtual_partitions
      .iter()
      .copied()
      .collect::<std::collections::HashSet<_>>();
    self
      .partition_read_states
      .retain(|partition_id, _| assigned.contains(partition_id));
    let initial_window_start_unix_seconds = self.window_start(now_unix_seconds);
    for partition_id in &assigned_virtual_partitions {
      self
        .partition_read_states
        .entry(*partition_id)
        .or_insert(PartitionReadState::Fresh {
          initial_window_start_unix_seconds,
        });
    }
    self.assigned_virtual_partitions = assigned_virtual_partitions;
    if self.assigned_virtual_partitions != previous_assignment {
      let previous = previous_assignment.into_iter().collect::<HashSet<_>>();
      let current = self
        .assigned_virtual_partitions
        .iter()
        .copied()
        .collect::<HashSet<_>>();
      let mut added = current.difference(&previous).copied().collect::<Vec<_>>();
      let mut removed = previous.difference(&current).copied().collect::<Vec<_>>();
      added.sort_unstable();
      removed.sort_unstable();
      let mut assigned = self.assigned_virtual_partitions.clone();
      assigned.sort_unstable();
      let (fresh, recovering, fast) = self.partition_read_mode_counts();
      info!(
        "consumer reader assignment updated: topic={}, assigned={assigned:?}, added={added:?}, \
         removed={removed:?}, fresh_partitions={fresh}, recovering_partitions={recovering}, \
         fast_partitions={fast}",
        self.config.topic
      );
    }
    Ok(())
  }

  /// Set the in-memory cursor for a virtual partition.
  pub fn set_cursor(&mut self, virtual_partition_id: VirtualPartitionId, seq_end: u64) {
    self.cursors.insert(virtual_partition_id, seq_end);
  }

  /// Update cursor and plan finite-retention recovery from externally committed state.
  pub fn hydrate_cursor_with_source(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    committed_cursor: &CommittedCursor,
    committed_ts_ms: Option<i64>,
    now_unix_seconds: i64,
  ) {
    let current = self
      .cursors
      .get(&virtual_partition_id)
      .copied()
      .unwrap_or(0);
    self
      .cursors
      .insert(virtual_partition_id, current.max(committed_cursor.seq_end));
    if self
      .partition_read_states
      .contains_key(&virtual_partition_id)
    {
      return;
    }

    let cutover_window_start_unix_seconds = self.window_start(now_unix_seconds);
    let retention_floor = self.retention_floor_window_start(cutover_window_start_unix_seconds);
    let source_window_start_unix_seconds = committed_cursor
      .source_checkpoint
      .as_ref()
      .map(|checkpoint| checkpoint.window_start_unix_seconds)
      .or_else(|| committed_ts_ms.map(|timestamp_ms| self.window_start(timestamp_ms / 1_000)))
      .unwrap_or(retention_floor)
      .max(retention_floor);
    if source_window_start_unix_seconds <= cutover_window_start_unix_seconds {
      self.partition_read_states.insert(
        virtual_partition_id,
        PartitionReadState::Recovering(RecoveryState {
          next_window_start_unix_seconds: source_window_start_unix_seconds,
          cutover_window_start_unix_seconds,
        }),
      );
      info!(
        "consumer partition recovery started: topic={}, partition={}, committed_offset={}, \
         recovery_start_window={}, cutover_window={}",
        self.config.topic,
        virtual_partition_id,
        committed_cursor.seq_end,
        format_unix_timestamp_seconds(source_window_start_unix_seconds),
        format_unix_timestamp_seconds(cutover_window_start_unix_seconds)
      );
    } else {
      self
        .partition_read_states
        .insert(virtual_partition_id, PartitionReadState::Fast);
      info!(
        "consumer partition resumed on fast path: topic={}, partition={}, committed_offset={}, \
         source_window={}, cutover_window={}",
        self.config.topic,
        virtual_partition_id,
        committed_cursor.seq_end,
        format_unix_timestamp_seconds(source_window_start_unix_seconds),
        format_unix_timestamp_seconds(cutover_window_start_unix_seconds)
      );
    }
  }

  pub(super) fn partition_read_states(&self) -> Vec<ConsumerReaderPartitionState> {
    let mut states = self
      .partition_read_states
      .iter()
      .map(
        |(virtual_partition_id, state)| ConsumerReaderPartitionState {
          virtual_partition_id: *virtual_partition_id,
          mode: match state {
            PartitionReadState::Fresh {
              initial_window_start_unix_seconds,
            } => ConsumerReaderPartitionMode::Fresh {
              initial_window_start_unix_seconds: *initial_window_start_unix_seconds,
            },
            PartitionReadState::Recovering(recovery_state) => {
              ConsumerReaderPartitionMode::Recovering {
                next_window_start_unix_seconds: recovery_state.next_window_start_unix_seconds,
                cutover_window_start_unix_seconds: recovery_state.cutover_window_start_unix_seconds,
              }
            },
            PartitionReadState::Fast => ConsumerReaderPartitionMode::Fast,
          },
        },
      )
      .collect::<Vec<_>>();
    states.sort_by_key(|state| state.virtual_partition_id);
    states
  }

  fn partition_read_mode_counts(&self) -> (usize, usize, usize) {
    self.partition_read_states.values().fold(
      (0_usize, 0_usize, 0_usize),
      |(fresh, recovering, fast), state| match state {
        PartitionReadState::Fresh { .. } => (fresh.saturating_add(1), recovering, fast),
        PartitionReadState::Recovering(_) => (fresh, recovering.saturating_add(1), fast),
        PartitionReadState::Fast => (fresh, recovering, fast.saturating_add(1)),
      },
    )
  }
}

#[async_trait]
impl ConsumerReader for ConsumerReaderImpl {
  async fn read_available(&mut self, now_unix_seconds: i64) -> Result<Vec<ConsumerBatch>> {
    let read_started_at = Instant::now();
    trace!(
      "consumer read_available start: topic={}, assigned_partitions={}",
      self.config.topic,
      self.assigned_virtual_partitions.len()
    );
    // Output contains only newly consumable batches according to per-partition cursor state.
    let mut output = Vec::new();
    let mut metadata_batches_scanned = 0_usize;
    let mut metadata_batches_skipped_by_cursor = 0_usize;
    let visibility_cutoff_ts_ms = now_unix_seconds.saturating_mul(1_000).saturating_sub(
      i64::try_from(consumer_metadata_visibility_delay_ms(&self.config)).unwrap_or(i64::MAX),
    );

    // Recovery scans catch late lower-snowflake rows; fast scans avoid rereading old metadata.
    let (scan_requests, recovery_scan) = self.scan_requests(now_unix_seconds)?;
    let recovery_window_end = scan_requests
      .iter()
      .filter(|request| request.eligibility.recovering)
      .map(|request| request.window.window_start_unix_seconds)
      .max();
    let scanned_fresh_window_starts = scan_requests
      .iter()
      .filter(|request| request.eligibility.fresh)
      .map(|request| request.window.window_start_unix_seconds)
      .collect::<Vec<_>>();
    let scan_futures = scan_requests.iter().cloned().map(|request| {
      let metadata_store = Arc::clone(&self.metadata_store);
      let metrics = self.metrics.clone();
      async move {
        if !request.recovery_scan && request.eligibility.fast && request.min_snowflake.is_none() {
          metrics.metadata_fast_scan_without_lower_bound.inc();
        }
        let scan_started_at = Instant::now();
        let segments = metadata_store
          .scan_window_from_snowflake(&request.window, request.min_snowflake)
          .await?;
        metrics.record_metadata_scan(scan_started_at, segments.len(), request.recovery_scan);
        Ok::<_, anyhow::Error>((request, segments))
      }
    });

    // Run independent per-window metadata queries concurrently while preserving input order.
    let window_results = match try_join_all(scan_futures).await {
      Ok(window_results) => window_results,
      Err(error) => {
        if recovery_scan {
          self.metrics.metadata_recovery_scan_failures.inc();
        }
        return Err(error);
      },
    };

    let mut next_fast_frontiers = self.fast_frontiers.clone();
    let mut blocked_fast_sources = HashSet::new();
    let mut deferred_recovery_windows = HashSet::new();
    let mut deferred_fresh_partitions = HashSet::new();
    for (request, mut segments) in window_results {
      let window = request.window;
      trace!(
        "consumer scanned window: topic={}, window_start={}, segments={}",
        window.topic,
        format_unix_timestamp_seconds(window.window_start_unix_seconds),
        segments.len()
      );

      // Metadata scans are unordered by contract; sorting provides deterministic processing.
      segments.sort_by_key(|metadata| metadata.snowflake_id);
      for segment in segments {
        // Restrict work to currently assigned virtual partitions only.
        let mut segment_has_assigned_batches = false;
        for partition_id in &self.assigned_virtual_partitions {
          let partition_state = self.partition_read_states.get(partition_id);
          let partition_is_eligible = match partition_state {
            Some(PartitionReadState::Fresh {
              initial_window_start_unix_seconds,
            }) => {
              request.eligibility.fresh
                && *initial_window_start_unix_seconds == window.window_start_unix_seconds
            },
            Some(PartitionReadState::Recovering(state)) => {
              request.eligibility.recovering
                && state.next_window_start_unix_seconds <= window.window_start_unix_seconds
                && window.window_start_unix_seconds <= state.cutover_window_start_unix_seconds
            },
            Some(PartitionReadState::Fast) => request.eligibility.fast,
            None => false,
          };
          if !partition_is_eligible {
            continue;
          }
          let Some(partition_batches) = segment.segment_index.get(partition_id) else {
            continue;
          };
          segment_has_assigned_batches = true;

          let frontier_key = (*partition_id, window.window_start_unix_seconds);
          let fast_partition = matches!(partition_state, Some(PartitionReadState::Fast));
          if fast_partition {
            if blocked_fast_sources.contains(&frontier_key) {
              continue;
            }
            if next_fast_frontiers
              .get(&frontier_key)
              .is_some_and(|frontier| segment.snowflake_id < *frontier)
            {
              self
                .metrics
                .metadata_fast_scan_segments_below_partition_frontier
                .inc();
              continue;
            }
          }

          if segment.metadata_published_ts_ms > visibility_cutoff_ts_ms {
            self
              .metrics
              .metadata_segments_deferred_by_visibility_delay
              .inc();
            trace!(
              "consumer deferred metadata by visibility delay: topic={}, partition={}, \
               window_start={}, snowflake_id={}, published_at={}, visibility_cutoff={}",
              self.config.topic,
              partition_id,
              format_unix_timestamp_seconds(window.window_start_unix_seconds),
              segment.snowflake_id.as_u64(),
              format_unix_timestamp_ms(segment.metadata_published_ts_ms),
              format_unix_timestamp_ms(visibility_cutoff_ts_ms)
            );
            if fast_partition {
              blocked_fast_sources.insert(frontier_key);
            } else if matches!(partition_state, Some(PartitionReadState::Fresh { .. })) {
              deferred_fresh_partitions.insert(*partition_id);
            } else {
              deferred_recovery_windows.insert(window.window_start_unix_seconds);
            }
            continue;
          }

          metadata_batches_scanned =
            metadata_batches_scanned.saturating_add(partition_batches.len());
          let current_cursor = self.cursors.get(partition_id).copied();

          // Avoid sorting historical batches when this partition has already consumed all of
          // them. Any late batch beyond the cursor still uses the normal sorted path below.
          if current_cursor.is_some_and(|cursor| {
            partition_batches
              .iter()
              .all(|batch| batch.seq_range.end <= cursor)
          }) {
            metadata_batches_skipped_by_cursor =
              metadata_batches_skipped_by_cursor.saturating_add(partition_batches.len());
            continue;
          }

          // Metadata scans are unordered and can arrive late. Sorting by seq_start keeps
          // processing deterministic while cursor checks prevent replay.
          let mut sorted_batches = partition_batches.iter().collect::<Vec<_>>();
          sorted_batches.sort_by_key(|batch| batch.seq_range.start);

          for batch_metadata in sorted_batches {
            // Cursor semantics: seq_end <= cursor was already consumed and can be skipped.
            let current_cursor = self.cursors.get(partition_id).copied();
            if current_cursor.is_some_and(|cursor| batch_metadata.seq_range.end <= cursor) {
              trace!(
                "consumer skipped batch by cursor: topic={}, partition={}, seq_end={}, cursor={}",
                self.config.topic,
                partition_id,
                batch_metadata.seq_range.end,
                current_cursor.unwrap_or(0)
              );
              metadata_batches_skipped_by_cursor =
                metadata_batches_skipped_by_cursor.saturating_add(1);
              continue;
            }

            // Decode batch payload only after passing cursor filter to avoid unnecessary I/O.
            let batch = self
              .read_batch(&segment, batch_metadata, *partition_id)
              .await?;

            // Cursor always moves forward. max() keeps monotonicity if metadata ordering is odd.
            let next_cursor = batch.seq_range.end.max(current_cursor.unwrap_or(0));
            self.cursors.insert(*partition_id, next_cursor);
            trace!(
              "consumer accepted batch: topic={}, partition={}, seq_start={}, seq_end={}, \
               records={}, new_cursor={}",
              self.config.topic,
              partition_id,
              batch.seq_range.start,
              batch.seq_range.end,
              batch.records.len(),
              next_cursor
            );
            output.push(batch);
          }

          if fast_partition {
            next_fast_frontiers
              .entry(frontier_key)
              .and_modify(|frontier| *frontier = (*frontier).max(segment.snowflake_id))
              .or_insert(segment.snowflake_id);
          }
        }
        if !segment_has_assigned_batches {
          self
            .metrics
            .metadata_fast_scan_segments_without_assigned_batches
            .inc();
        }
      }
    }

    let window_size_seconds = consumer_window_size_seconds(&self.config);
    let mut initial_scans_completed = Vec::new();
    let mut recoveries_completed = Vec::new();
    for (partition_id, state) in &mut self.partition_read_states {
      match state {
        PartitionReadState::Fresh {
          initial_window_start_unix_seconds,
        } if scanned_fresh_window_starts.contains(initial_window_start_unix_seconds)
          && !deferred_fresh_partitions.contains(partition_id) =>
        {
          initial_scans_completed.push((*partition_id, *initial_window_start_unix_seconds));
          *state = PartitionReadState::Fast;
        },
        PartitionReadState::Recovering(recovery_state) => {
          let Some(recovery_window_end) = recovery_window_end else {
            continue;
          };
          let recovery_window_end =
            deferred_recovery_windows
              .iter()
              .min()
              .map_or(recovery_window_end, |deferred_window| {
                recovery_window_end
                  .min(deferred_window.saturating_sub(consumer_window_size_seconds(&self.config)))
              });
          if recovery_state.next_window_start_unix_seconds > recovery_window_end {
            continue;
          }
          recovery_state.next_window_start_unix_seconds =
            recovery_window_end.saturating_add(window_size_seconds);
          if recovery_state.next_window_start_unix_seconds
            > recovery_state.cutover_window_start_unix_seconds
          {
            recoveries_completed.push((
              *partition_id,
              recovery_state.cutover_window_start_unix_seconds,
            ));
            *state = PartitionReadState::Fast;
          }
        },
        PartitionReadState::Fresh { .. } | PartitionReadState::Fast => {},
      }
    }
    for (partition_id, initial_window_start_unix_seconds) in initial_scans_completed {
      info!(
        "consumer partition initial scan completed; fast path active: topic={}, partition={}, \
         initial_window={}",
        self.config.topic,
        partition_id,
        format_unix_timestamp_seconds(initial_window_start_unix_seconds)
      );
    }
    for (partition_id, cutover_window_start_unix_seconds) in recoveries_completed {
      info!(
        "consumer partition recovery completed; fast path active: topic={}, partition={}, \
         cutover_window={}",
        self.config.topic,
        partition_id,
        format_unix_timestamp_seconds(cutover_window_start_unix_seconds)
      );
    }

    trace!(
      "consumer read_available complete: topic={}, output_batches={}, \
       metadata_batches_scanned={}, metadata_batches_skipped_by_cursor={}",
      self.config.topic,
      output.len(),
      metadata_batches_scanned,
      metadata_batches_skipped_by_cursor
    );

    self.fast_frontiers = next_fast_frontiers;
    self.prune_fast_frontiers(now_unix_seconds)?;

    self.metrics.record_read_available(
      read_started_at,
      output.len(),
      metadata_batches_scanned,
      metadata_batches_skipped_by_cursor,
      recovery_scan,
    );

    Ok(output)
  }

  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64> {
    self.cursors.get(&virtual_partition_id).copied()
  }

  fn cursors(&self) -> HashMap<VirtualPartitionId, u64> {
    self.cursors.clone()
  }
}
