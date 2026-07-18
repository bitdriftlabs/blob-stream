#[cfg(test)]
#[path = "./consumer_test.rs"]
mod tests;

mod scan;
mod state;

use crate::config::{
  ConsumerReadConfig,
  ConsumerReadRuntimeSettings,
  consumer_read_runtime_settings,
  consumer_window_size_seconds,
  validate_read_config,
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use bd_server_stats::stats::Scope;
use blob_stream_blob_store::BlobStore;
use blob_stream_metadata_store::MetadataStore;
use blob_stream_types::{
  CommittedCursor,
  CommittedSourceCheckpoint,
  Record,
  SeqRange,
  SnowflakeId,
  VirtualPartitionId,
  Window,
  format_unix_timestamp_ms,
};
use log::info;
use prometheus::{Histogram, IntCounter, IntGauge};
pub use scan::ReadCapacity;
pub use state::{ConsumerReaderPartitionMode, ConsumerReaderPartitionState};
use state::{RecoveryState, VirtualPartitionState};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

const MAX_RECOVERY_WINDOWS_PER_SCAN: usize = 32;
const HISTORICAL_SEEK_RECOVERY_SECONDS: i64 = 10 * 60;

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
  /// Scan available windows and return batches that fit within the supplied payload capacity.
  async fn read_available(
    &mut self,
    now_unix_seconds: i64,
    capacity: ReadCapacity,
  ) -> Result<Vec<ConsumerBatch>>;
  /// Return committed cursor for a virtual partition, if known.
  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64>;
  /// Return all tracked cursors.
  fn cursors(&self) -> HashMap<VirtualPartitionId, u64>;
}

//
// ConsumerReaderImpl
//

/// Default `ConsumerReader` implementation used by `ConsumerIteratorImpl`.
pub struct ConsumerReaderImpl {
  config: ConsumerReadConfig,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  virtual_partition_states: HashMap<VirtualPartitionId, VirtualPartitionState>,
  retention_days: u32,
  maximum_metadata_publication_lag_ms: u64,
  fast_frontiers: HashMap<(VirtualPartitionId, i64), SnowflakeId>,
  feature_flags: Option<FeatureFlagsWatch>,
  metrics: ConsumerReaderMetrics,
}

impl ConsumerReaderImpl {
  /// Create a reader with finite retention recovery and an explicit metadata publication bound.
  pub fn new(
    config: ConsumerReadConfig,
    assigned_virtual_partitions: Vec<VirtualPartitionId>,
    initial_cursors: HashMap<VirtualPartitionId, u64>,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    metrics_scope: &Scope,
    retention_days: u32,
    maximum_metadata_publication_lag_ms: u64,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Result<Self> {
    validate_read_config(&config)?;
    ensure!(
      retention_days > 0,
      "consumer retention recovery requires topic retention_days greater than zero"
    );
    info!(
      "consumer reader initialized: topic={}, retention_days={}, \
       maximum_metadata_publication_lag_ms={}, max_in_flight_batch_reads={}, \
       assigned_partitions={}, initial_cursors={}",
      config.topic,
      retention_days,
      maximum_metadata_publication_lag_ms,
      consumer_read_runtime_settings(&config, feature_flags.as_ref()).max_in_flight_batch_reads,
      assigned_virtual_partitions.len(),
      initial_cursors.len()
    );

    let mut virtual_partition_states = initial_cursors
      .into_iter()
      .map(|(partition_id, cursor)| {
        (
          partition_id,
          VirtualPartitionState::PendingCursor { cursor },
        )
      })
      .collect::<HashMap<_, _>>();
    for partition_id in assigned_virtual_partitions {
      let cursor = virtual_partition_states
        .remove(&partition_id)
        .and_then(|state| state.cursor());
      virtual_partition_states.insert(partition_id, VirtualPartitionState::Fast { cursor });
    }

    Ok(Self {
      virtual_partition_states,
      retention_days,
      maximum_metadata_publication_lag_ms,
      fast_frontiers: HashMap::new(),
      config,
      blob_store,
      metadata_store,
      feature_flags,
      metrics: ConsumerReaderMetrics::new(metrics_scope),
    })
  }

  fn window_start(&self, unix_seconds: i64) -> i64 {
    Window::for_timestamp(unix_seconds, consumer_window_size_seconds(&self.config))
      .start_unix_seconds
  }

  pub(crate) fn runtime_settings(&self) -> ConsumerReadRuntimeSettings {
    consumer_read_runtime_settings(&self.config, self.feature_flags.as_ref())
  }

  fn retention_floor_window_start(&self, cutover_window_start_unix_seconds: i64) -> i64 {
    let retention_seconds = i64::from(self.retention_days).saturating_mul(86_400);
    self.window_start(cutover_window_start_unix_seconds.saturating_sub(retention_seconds))
  }

  /// Replace the current assignment set.
  pub fn set_assigned_virtual_partitions(
    &mut self,
    assigned_virtual_partitions: &[VirtualPartitionId],
    now_unix_seconds: i64,
  ) -> Result<()> {
    let previous_assignment = self.assigned_virtual_partition_ids();
    let assigned = assigned_virtual_partitions
      .iter()
      .copied()
      .collect::<HashSet<_>>();
    if previous_assignment != assigned_virtual_partitions {
      self
        .fast_frontiers
        .retain(|(partition_id, _), _| assigned.contains(partition_id));
    }
    // A revoked partition has no reader-local work left after the iterator drains it. Its durable
    // cursor belongs in the consumer-group lease and is hydrated again if this reader reacquires
    // the partition.
    self
      .virtual_partition_states
      .retain(|partition_id, _| assigned.contains(partition_id));
    let initial_window_start_unix_seconds = self.window_start(now_unix_seconds);
    for partition_id in assigned_virtual_partitions {
      let state = self.virtual_partition_states.remove(partition_id);
      let state = state.map_or_else(
        || VirtualPartitionState::fresh(initial_window_start_unix_seconds),
        |state| state.into_assigned(initial_window_start_unix_seconds),
      );
      self.virtual_partition_states.insert(*partition_id, state);
    }
    let current_assignment = self.assigned_virtual_partition_ids();
    if current_assignment != previous_assignment {
      let previous = previous_assignment.into_iter().collect::<HashSet<_>>();
      let current = current_assignment.iter().copied().collect::<HashSet<_>>();
      let mut added = current.difference(&previous).copied().collect::<Vec<_>>();
      let mut removed = previous.difference(&current).copied().collect::<Vec<_>>();
      added.sort_unstable();
      removed.sort_unstable();
      let (fresh, recovering, fast) = self.partition_read_mode_counts();
      info!(
        "consumer reader assignment updated: topic={}, assigned={current_assignment:?}, \
         added={added:?}, removed={removed:?}, fresh_partitions={fresh}, \
         recovering_partitions={recovering}, fast_partitions={fast}",
        self.config.topic
      );
    }
    Ok(())
  }

  /// Set the in-memory cursor for a virtual partition and reset its fast scan frontier.
  pub fn set_cursor(&mut self, virtual_partition_id: VirtualPartitionId, seq_end: u64) {
    self
      .virtual_partition_states
      .entry(virtual_partition_id)
      .and_modify(|state| state.set_cursor(seq_end))
      .or_insert(VirtualPartitionState::PendingFast { cursor: seq_end });
    self
      .fast_frontiers
      .retain(|(partition_id, _), _| *partition_id != virtual_partition_id);
  }

  /// Reposition a partition cursor and recover recent windows before returning to the fast path.
  pub fn seek(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    seq_end: u64,
    now_unix_seconds: i64,
  ) {
    self.set_cursor(virtual_partition_id, seq_end);
    let cutover_window_start_unix_seconds = self.window_start(now_unix_seconds);
    let recovery_start_window_unix_seconds = self
      .window_start(now_unix_seconds.saturating_sub(HISTORICAL_SEEK_RECOVERY_SECONDS))
      .max(self.retention_floor_window_start(cutover_window_start_unix_seconds));
    let recovery_state = RecoveryState {
      next_window_start_unix_seconds: recovery_start_window_unix_seconds,
      cutover_window_start_unix_seconds,
    };
    if let Some(state) = self.virtual_partition_states.get_mut(&virtual_partition_id) {
      state.start_recovery(recovery_state);
    } else {
      self.virtual_partition_states.insert(
        virtual_partition_id,
        VirtualPartitionState::PendingRecovering {
          cursor: seq_end,
          recovery_state,
        },
      );
    }
    info!(
      "consumer historical seek recovery started: topic={}, partition={}, offset={}, \
       recovery_start_window={}, cutover_window={}",
      self.config.topic,
      virtual_partition_id,
      seq_end,
      format_unix_timestamp_seconds(recovery_start_window_unix_seconds),
      format_unix_timestamp_seconds(cutover_window_start_unix_seconds)
    );
  }

  /// Update cursor and plan finite-retention recovery from externally committed state.
  pub fn hydrate_cursor_with_source(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    committed_cursor: &CommittedCursor,
    committed_ts_ms: Option<i64>,
    now_unix_seconds: i64,
  ) {
    // Hydration can arrive before assignment during a rebalance. The default Pending lifecycle
    // state retains the recovery plan without allowing the reader to scan this partition.
    let cutover_window_start_unix_seconds = self.window_start(now_unix_seconds);
    let retention_floor = self.retention_floor_window_start(cutover_window_start_unix_seconds);
    let source_window_start_unix_seconds = committed_cursor
      .source_checkpoint
      .as_ref()
      .map(|checkpoint| checkpoint.window_start_unix_seconds)
      .or_else(|| committed_ts_ms.map(|timestamp_ms| self.window_start(timestamp_ms / 1_000)))
      .unwrap_or(retention_floor)
      .max(retention_floor);
    let hydrated_cursor = self
      .virtual_partition_states
      .get(&virtual_partition_id)
      .and_then(VirtualPartitionState::cursor)
      .map_or(committed_cursor.seq_end, |cursor| {
        cursor.max(committed_cursor.seq_end)
      });
    if let Some(state) = self.virtual_partition_states.get_mut(&virtual_partition_id)
      && !matches!(state, VirtualPartitionState::PendingCursor { .. })
    {
      state.advance_cursor(committed_cursor.seq_end);
      return;
    }
    if source_window_start_unix_seconds <= cutover_window_start_unix_seconds {
      self.virtual_partition_states.insert(
        virtual_partition_id,
        VirtualPartitionState::PendingRecovering {
          cursor: hydrated_cursor,
          recovery_state: RecoveryState {
            next_window_start_unix_seconds: source_window_start_unix_seconds,
            cutover_window_start_unix_seconds,
          },
        },
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
      self.virtual_partition_states.insert(
        virtual_partition_id,
        VirtualPartitionState::PendingFast {
          cursor: hydrated_cursor,
        },
      );
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
      .virtual_partition_states
      .iter()
      .filter_map(|(virtual_partition_id, state)| {
        state
          .reader_mode()
          .map(|mode| ConsumerReaderPartitionState {
            virtual_partition_id: *virtual_partition_id,
            mode,
          })
      })
      .collect::<Vec<_>>();
    states.sort_by_key(|state| state.virtual_partition_id);
    states
  }

  fn partition_read_mode_counts(&self) -> (usize, usize, usize) {
    self.virtual_partition_states.values().fold(
      (0_usize, 0_usize, 0_usize),
      |(fresh, recovering, fast), state| match state {
        VirtualPartitionState::PendingCursor { .. } => (fresh, recovering, fast),
        VirtualPartitionState::Fresh { .. } => (fresh.saturating_add(1), recovering, fast),
        VirtualPartitionState::PendingRecovering { .. }
        | VirtualPartitionState::Recovering { .. } => (fresh, recovering.saturating_add(1), fast),
        VirtualPartitionState::PendingFast { .. } | VirtualPartitionState::Fast { .. } => {
          (fresh, recovering, fast.saturating_add(1))
        },
      },
    )
  }

  fn assigned_virtual_partition_ids(&self) -> Vec<VirtualPartitionId> {
    // Keep the scan order deterministic while excluding recovered state that is still pending a
    // coordinator assignment.
    let mut partition_ids = self
      .virtual_partition_states
      .iter()
      .filter_map(|(partition_id, state)| state.is_assigned().then_some(*partition_id))
      .collect::<Vec<_>>();
    partition_ids.sort_unstable();
    partition_ids
  }

  pub(crate) async fn read_available_with_capacity_and_settings(
    &mut self,
    now_unix_seconds: i64,
    capacity: ReadCapacity,
    runtime_settings: ConsumerReadRuntimeSettings,
  ) -> Result<Vec<ConsumerBatch>> {
    self
      .read_available_impl(now_unix_seconds, capacity, runtime_settings)
      .await
  }
}

#[async_trait]
impl ConsumerReader for ConsumerReaderImpl {
  async fn read_available(
    &mut self,
    now_unix_seconds: i64,
    capacity: ReadCapacity,
  ) -> Result<Vec<ConsumerBatch>> {
    let runtime_settings = self.runtime_settings();
    self
      .read_available_impl(now_unix_seconds, capacity, runtime_settings)
      .await
  }

  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64> {
    self
      .virtual_partition_states
      .get(&virtual_partition_id)
      .and_then(VirtualPartitionState::cursor)
  }

  fn cursors(&self) -> HashMap<VirtualPartitionId, u64> {
    self
      .virtual_partition_states
      .iter()
      .filter_map(|(partition_id, state)| state.cursor().map(|cursor| (*partition_id, cursor)))
      .collect()
  }
}
