//! Background reader and prefetch worker for the consumer iterator.
//!
//! `PrefetchWorker` exclusively owns `ConsumerReaderImpl` after the driver starts. Driver
//! requests that change reader state travel through `ConsumerReaderCommand` and are applied only
//! between `read_available()` calls, which prevents assignment, hydration, or seek mutations from
//! racing an asynchronous metadata or blob read.
//!
//! The worker uses the shared-state mutex only to transfer completed batches and publish a
//! diagnostic snapshot. It never holds that lock across metadata, blob, sleep, or notification
//! awaits. The local pending queue holds reader output that cannot yet enter the delivery queue
//! because doing so would exceed the soft byte budget.

use super::{
  ConsumerIteratorMetrics,
  ConsumerLifecycleHooks,
  ConsumerSharedState,
  prefetched_batch_bytes,
  update_total_prefetch_bytes,
  update_worker_prefetch_metrics,
};
use crate::consumer::{
  ConsumerBatch,
  ConsumerReader,
  ConsumerReaderFastFrontierState,
  ConsumerReaderFastScanBoundState,
  ConsumerReaderImpl,
  ConsumerReaderPartitionMode,
  ConsumerReaderPartitionScanState,
  ReadCapacity,
};
use crate::coordination::RecoveredCursor;
use crate::diagnostics::{
  ConsumerDiagnostics,
  ConsumerPartitionReadMode,
  ConsumerReaderFastFrontierSnapshot,
  ConsumerReaderFastScanBoundSnapshot,
  ConsumerReaderPartitionSnapshot,
  ConsumerReaderScanSnapshot,
  emit_partition_handoff_snapshots,
  handoff_cursor_key,
  offsets_from_map,
};
use anyhow::Result;
use bd_backoff::{ExponentialBackoff, ExponentialBackoffBuilder, InfiniteBackoff as _};
use bd_log::warn_every;
use bd_time::TimeProvider;
use blob_stream_types::{SnowflakeId, VirtualPartitionId, format_unix_timestamp_ms};
use log::debug;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use time::ext::NumericalDuration;
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::{Span, field};

const MAX_SCAN_DETAIL_ENTRIES: usize = 64;

//
// IdlePollBackoff
//

/// Exponential idle delay used only when a complete reader pass produces no batches.
pub(super) struct IdlePollBackoff {
  inner: ExponentialBackoff,
}

impl IdlePollBackoff {
  pub(super) fn new(base_delay_ms: u64, max_delay_ms: Option<u64>) -> Self {
    Self {
      inner: ExponentialBackoffBuilder::new_infinite()
        .with_initial_interval(
          time::Duration::try_from(std::time::Duration::from_millis(base_delay_ms))
            .unwrap_or(time::Duration::MAX),
        )
        .with_randomization_factor(0.0)
        .with_multiplier(2.0)
        .with_max_interval(
          time::Duration::try_from(std::time::Duration::from_millis(
            max_delay_ms.unwrap_or(base_delay_ms),
          ))
          .unwrap_or(time::Duration::MAX),
        )
        .build(),
    }
  }

  /// Return the current delay before increasing it for the next consecutive empty read.
  pub(super) fn next_delay_ms(&mut self) -> u64 {
    u64::try_from(self.inner.next_backoff().whole_milliseconds()).unwrap_or(u64::MAX)
  }

  /// A nonempty reader result restores the configured base poll delay.
  pub(super) fn reset(&mut self) {
    self.inner.reset();
  }
}

//
// ConsumerReaderCommand
//

/// Reader mutations queued by the driver for the worker's next safe boundary.
pub(super) enum ConsumerReaderCommand {
  HydrateCursors {
    recovered_cursors: HashMap<VirtualPartitionId, RecoveredCursor>,
    now_unix_seconds: i64,
  },
  SetAssignment {
    assignment: Vec<VirtualPartitionId>,
    now_unix_seconds: i64,
    handoff_phase: Option<&'static str>,
    release_delivery_fence: bool,
  },
  Seek {
    virtual_partition_id: VirtualPartitionId,
    offset: u64,
    now_unix_seconds: i64,
    response: oneshot::Sender<Result<()>>,
  },
}

struct RecoveryTrace {
  span: Span,
  started_at: Instant,
  scan_passes: usize,
  metadata_batches_seen: usize,
  batches_skipped_by_cursor: usize,
  segments_skipped_by_frontier: usize,
  segments_deferred_by_visibility: usize,
  batches_accepted: usize,
  records_accepted: usize,
}

//
// PrefetchWorker
//

/// Single-owner task that scans the reader and admits batches into caller-visible delivery.
pub(super) struct PrefetchWorker {
  reader: ConsumerReaderImpl,
  reader_command_rx: mpsc::UnboundedReceiver<ConsumerReaderCommand>,
  shared_state: Arc<Mutex<ConsumerSharedState>>,
  diagnostics: ConsumerDiagnostics,
  delivery_notify: Arc<Notify>,
  prefetch_space_notify: Arc<Notify>,
  prefetch_shutdown: Arc<AtomicBool>,
  metrics: ConsumerIteratorMetrics,
  base_idle_delay_ms: u64,
  max_idle_delay_ms: Option<u64>,
  recovery_traces: HashMap<VirtualPartitionId, RecoveryTrace>,
  time_provider: Arc<dyn TimeProvider>,
  lifecycle_hooks: Arc<dyn ConsumerLifecycleHooks>,
  member_id: String,
}

impl PrefetchWorker {
  #[allow(clippy::too_many_arguments)]
  pub(super) fn new(
    reader: ConsumerReaderImpl,
    reader_command_rx: mpsc::UnboundedReceiver<ConsumerReaderCommand>,
    shared_state: Arc<Mutex<ConsumerSharedState>>,
    diagnostics: ConsumerDiagnostics,
    delivery_notify: Arc<Notify>,
    prefetch_space_notify: Arc<Notify>,
    prefetch_shutdown: Arc<AtomicBool>,
    metrics: ConsumerIteratorMetrics,
    base_idle_delay_ms: u64,
    max_idle_delay_ms: Option<u64>,
    time_provider: Arc<dyn TimeProvider>,
    lifecycle_hooks: Arc<dyn ConsumerLifecycleHooks>,
    member_id: String,
  ) -> Self {
    Self {
      reader,
      reader_command_rx,
      shared_state,
      diagnostics,
      delivery_notify,
      prefetch_space_notify,
      prefetch_shutdown,
      metrics,
      base_idle_delay_ms,
      max_idle_delay_ms,
      recovery_traces: HashMap::new(),
      time_provider,
      lifecycle_hooks,
      member_id,
    }
  }

  /// Run until shutdown, a reader command failure, or the driver drops the command channel.
  pub(super) async fn run(mut self) {
    let mut idle_poll_backoff =
      IdlePollBackoff::new(self.base_idle_delay_ms, self.max_idle_delay_ms);
    let mut pending = VecDeque::<ConsumerBatch>::new();
    let mut pending_record_count = 0_usize;
    let mut pending_bytes = 0_u64;

    loop {
      if self.prefetch_shutdown.load(Ordering::Acquire) {
        self.finish_recovery_traces("worker_stopped");
        return;
      }
      if !process_reader_commands(
        &mut self.reader,
        &mut self.reader_command_rx,
        &self.shared_state,
        &self.diagnostics,
        &self.delivery_notify,
        &mut pending,
        &mut pending_record_count,
        &mut pending_bytes,
      ) {
        self.finish_recovery_traces("worker_stopped");
        return;
      }
      self.start_recovery_traces();

      let runtime_settings = self.reader.runtime_settings();

      if self
        .admit_pending_batches(
          &mut pending,
          &mut pending_record_count,
          &mut pending_bytes,
          runtime_settings.prefetch_max_bytes,
        )
        .await
      {
        continue;
      }

      let Some(capacity) = self.read_capacity(pending_bytes, runtime_settings.prefetch_max_bytes)
      else {
        self.metrics.prefetch_paused_budget.inc();
        let _ = tokio::time::timeout(
          std::time::Duration::from_millis(250),
          self.prefetch_space_notify.notified(),
        )
        .await;
        continue;
      };

      let read_started_at = Instant::now();
      let batches = self
        .read_available_with_retry(capacity, runtime_settings)
        .await;
      record_reader_diagnostics(&self.reader, &self.shared_state);
      self.record_recovery_progress();

      self
        .metrics
        .next_latency_seconds
        .observe(read_started_at.elapsed().as_secs_f64());

      if batches.is_empty() {
        let idle_delay_ms = idle_poll_backoff.next_delay_ms();
        self
          .time_provider
          .sleep(time::Duration::milliseconds(
            i64::try_from(idle_delay_ms).unwrap_or(i64::MAX),
          ))
          .await;
        continue;
      }

      idle_poll_backoff.reset();
      self.metrics.prefetch_refill_cycles.inc();
      pending_record_count = pending_record_count.saturating_add(
        batches
          .iter()
          .map(|batch| batch.records.len())
          .sum::<usize>(),
      );
      pending_bytes =
        pending_bytes.saturating_add(batches.iter().map(prefetched_batch_bytes).sum::<u64>());
      pending.extend(batches);
      self.record_pending_diagnostics(&pending, pending_record_count, pending_bytes);
    }
  }

  fn start_recovery_traces(&mut self) {
    let new_recoveries = self
      .reader
      .partition_read_states()
      .into_iter()
      .filter_map(|state| {
        let ConsumerReaderPartitionMode::Recovering {
          next_window_start_unix_seconds,
          cutover_window_start_unix_seconds,
        } = state.mode
        else {
          return None;
        };
        (!self
          .recovery_traces
          .contains_key(&state.virtual_partition_id))
        .then_some((
          state.virtual_partition_id,
          next_window_start_unix_seconds,
          cutover_window_start_unix_seconds,
        ))
      })
      .collect::<Vec<_>>();
    if new_recoveries.is_empty() {
      return;
    }

    let snapshot = self.diagnostics.state_snapshot();
    for (partition_id, next_window_start_unix_seconds, cutover_window_start_unix_seconds) in
      new_recoveries
    {
      let partition = snapshot
        .local
        .partitions
        .iter()
        .find(|partition| partition.virtual_partition_id == partition_id);
      let cursor_key = partition.map_or_else(
        || {
          format!(
            "{}:{}:{}:none:none",
            snapshot.topic, snapshot.group_id, partition_id
          )
        },
        |partition| handoff_cursor_key(&snapshot, partition),
      );
      let recovery_start_cursor = partition.and_then(|partition| partition.cursor);
      let recovery_committed_cursor =
        partition.and_then(|partition| partition.last_committed_offset);
      let span = bd_log::otel_info_span!(
        "blob_stream.consumer.partition_recovery",
        otel.kind = "consumer",
        consumer.topic = %snapshot.topic,
        consumer.group_id = %snapshot.group_id,
        consumer.generation = snapshot.accepted_assignment_plan_version,
        messaging.partition = partition_id,
        handoff.cursor_key = %cursor_key,
        recovery.start_cursor = ?recovery_start_cursor,
        recovery.committed_cursor = ?recovery_committed_cursor,
        recovery.next_window_start = next_window_start_unix_seconds,
        recovery.cutover_window_start = cutover_window_start_unix_seconds,
        recovery.scan_passes = field::Empty,
        recovery.duration_ms = field::Empty,
        recovery.outcome = field::Empty,
        recovery.summary_json = field::Empty,
        otel.status_code = field::Empty,
      );
      self.recovery_traces.insert(
        partition_id,
        RecoveryTrace {
          span,
          started_at: Instant::now(),
          scan_passes: 0,
          metadata_batches_seen: 0,
          batches_skipped_by_cursor: 0,
          segments_skipped_by_frontier: 0,
          segments_deferred_by_visibility: 0,
          batches_accepted: 0,
          records_accepted: 0,
        },
      );
    }
  }

  fn record_recovery_progress(&mut self) {
    if self.recovery_traces.is_empty() {
      return;
    }

    let modes = self
      .reader
      .partition_read_states()
      .into_iter()
      .map(|state| (state.virtual_partition_id, state.mode))
      .collect::<HashMap<_, _>>();
    let scans = self
      .reader
      .partition_scan_states()
      .into_iter()
      .map(|state| (state.virtual_partition_id, state))
      .collect::<HashMap<_, _>>();
    let mut completed = Vec::new();
    for (partition_id, recovery) in &mut self.recovery_traces {
      recovery.scan_passes = recovery.scan_passes.saturating_add(1);
      if let Some(scan) = scans.get(partition_id) {
        recovery.metadata_batches_seen = recovery
          .metadata_batches_seen
          .saturating_add(scan.metadata_batches_seen);
        recovery.batches_skipped_by_cursor = recovery
          .batches_skipped_by_cursor
          .saturating_add(scan.metadata_batches_skipped_by_cursor);
        recovery.segments_skipped_by_frontier = recovery
          .segments_skipped_by_frontier
          .saturating_add(scan.metadata_segments_skipped_by_frontier);
        recovery.segments_deferred_by_visibility = recovery
          .segments_deferred_by_visibility
          .saturating_add(scan.metadata_segments_deferred_by_visibility);
        recovery.batches_accepted = recovery
          .batches_accepted
          .saturating_add(scan.batches_accepted);
        recovery.records_accepted = recovery
          .records_accepted
          .saturating_add(scan.records_accepted);
      }
      if matches!(
        modes.get(partition_id),
        Some(ConsumerReaderPartitionMode::Recovering { .. })
      ) {
        continue;
      }
      let completed_normally = matches!(
        modes.get(partition_id),
        Some(ConsumerReaderPartitionMode::Fast)
      );
      Self::finish_recovery_trace(
        recovery,
        if completed_normally {
          "fast_path_active"
        } else {
          "cancelled"
        },
      );
      completed.push(*partition_id);
    }
    for partition_id in completed {
      self.recovery_traces.remove(&partition_id);
    }
  }

  fn finish_recovery_traces(&mut self, outcome: &str) {
    for recovery in self.recovery_traces.values() {
      Self::finish_recovery_trace(recovery, outcome);
    }
    self.recovery_traces.clear();
  }

  fn finish_recovery_trace(recovery: &RecoveryTrace, outcome: &str) {
    let summary_json = serde_json::json!({
      "metadata_batches_seen": recovery.metadata_batches_seen,
      "batches_skipped_by_cursor": recovery.batches_skipped_by_cursor,
      "segments_skipped_by_frontier": recovery.segments_skipped_by_frontier,
      "segments_deferred_by_visibility": recovery.segments_deferred_by_visibility,
      "batches_accepted": recovery.batches_accepted,
      "records_accepted": recovery.records_accepted,
    })
    .to_string();
    recovery
      .span
      .record("recovery.scan_passes", recovery.scan_passes);
    recovery.span.record(
      "recovery.duration_ms",
      u64::try_from(recovery.started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
    );
    recovery.span.record("recovery.outcome", outcome);
    recovery.span.record("recovery.summary_json", summary_json);
    recovery.span.record(
      "otel.status_code",
      if outcome == "fast_path_active" {
        "OK"
      } else {
        "UNSET"
      },
    );
  }

  /// Return capacity remaining after queued, pending, and partially delivered records.
  fn read_capacity(&self, pending_bytes: u64, prefetch_max_bytes: u64) -> Option<ReadCapacity> {
    let retained_bytes = self
      .shared_state
      .lock()
      .delivery_state
      .retained_bytes()
      .saturating_add(pending_bytes);
    (retained_bytes < prefetch_max_bytes).then(|| {
      ReadCapacity::with_oversized_batch(
        prefetch_max_bytes.saturating_sub(retained_bytes),
        retained_bytes == 0,
      )
    })
  }

  fn record_pending_diagnostics(
    &self,
    pending: &VecDeque<ConsumerBatch>,
    pending_record_count: usize,
    pending_bytes: u64,
  ) {
    let mut shared_state = self.shared_state.lock();
    shared_state.diagnostics.prefetch_pending_batch_count = pending.len();
    shared_state.diagnostics.prefetch_pending_record_count = pending_record_count;
    shared_state.diagnostics.prefetch_pending_bytes = pending_bytes;
    self
      .metrics
      .prefetch_pending_batches
      .set(i64::try_from(pending.len()).unwrap_or(i64::MAX));
    self
      .metrics
      .prefetch_pending_bytes
      .set(i64::try_from(pending_bytes).unwrap_or(i64::MAX));
    update_total_prefetch_bytes(&self.metrics, &shared_state.delivery_state, pending_bytes);
  }

  /// Move pending batches into the shared delivery queue, returning true after a backpressure wait.
  async fn admit_pending_batches(
    &self,
    pending: &mut VecDeque<ConsumerBatch>,
    pending_record_count: &mut usize,
    pending_bytes: &mut u64,
    prefetch_max_bytes: u64,
  ) -> bool {
    let (pending_remains, buffered_partitions) = {
      let mut shared_state = self.shared_state.lock();
      let mut buffered_partitions = Vec::new();

      while let Some(batch) = pending.front() {
        let batch_record_count = batch.records.len();
        let batch_bytes = prefetched_batch_bytes(batch);
        // Assignment can change while the reader is blocked. Drop obsolete pending work before it
        // consumes shared buffer capacity or becomes visible to the caller.
        if !shared_state
          .active_partitions
          .contains_key(&batch.virtual_partition_id)
        {
          pending.pop_front();
          *pending_record_count = pending_record_count.saturating_sub(batch_record_count);
          *pending_bytes = pending_bytes.saturating_sub(batch_bytes);
          continue;
        }
        let buffer = &mut shared_state.delivery_state;
        let retained_bytes = buffer.retained_bytes();
        let would_cross = retained_bytes
          .saturating_add(batch_bytes)
          .gt(&prefetch_max_bytes);

        // The configured budget is a soft target: one batch may cross it so a single oversized
        // batch remains deliverable. Once delivery retains payload, pause before adding another.
        if would_cross && retained_bytes > 0 {
          self.metrics.prefetch_paused_budget.inc();
          break;
        }

        let Some(batch) = pending.pop_front() else {
          break;
        };
        *pending_record_count = pending_record_count.saturating_sub(batch_record_count);
        *pending_bytes = pending_bytes.saturating_sub(batch_bytes);
        buffer.buffered_bytes = buffer.buffered_bytes.saturating_add(batch_bytes);
        buffered_partitions.push(batch.virtual_partition_id);
        buffer.batches.push_back(batch);
      }

      {
        let buffer = &mut shared_state.delivery_state;
        if !buffer.batches.is_empty() {
          update_worker_prefetch_metrics(&self.metrics, buffer);
          self.delivery_notify.notify_waiters();
        }
      }
      // These counts describe worker-owned pending work, not the shared delivery queue.
      shared_state.diagnostics.prefetch_pending_batch_count = pending.len();
      shared_state.diagnostics.prefetch_pending_record_count = *pending_record_count;
      shared_state.diagnostics.prefetch_pending_bytes = *pending_bytes;
      self
        .metrics
        .prefetch_pending_batches
        .set(i64::try_from(pending.len()).unwrap_or(i64::MAX));
      self
        .metrics
        .prefetch_pending_bytes
        .set(i64::try_from(*pending_bytes).unwrap_or(i64::MAX));
      update_total_prefetch_bytes(&self.metrics, &shared_state.delivery_state, *pending_bytes);

      (!pending.is_empty(), buffered_partitions)
    };
    for virtual_partition_id in buffered_partitions {
      self
        .lifecycle_hooks
        .prefetch_batch_buffered(&self.member_id, virtual_partition_id)
        .await;
    }
    if pending_remains {
      // Wake quickly when callers drain the delivery queue, but periodically retry so a lost
      // notification cannot stall prefetch permanently.
      let _ = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        self.prefetch_space_notify.notified(),
      )
      .await;
    }
    pending_remains
  }

  /// Retry one failed read immediately, then wait before subsequent retry attempts.
  async fn read_available_with_retry(
    &mut self,
    capacity: ReadCapacity,
    runtime_settings: crate::config::ConsumerReadRuntimeSettings,
  ) -> Vec<ConsumerBatch> {
    let mut read_attempt: u8 = 0;
    loop {
      let now_s = self.time_provider.now().unix_timestamp();
      match self
        .reader
        .read_available_with_capacity_and_settings(now_s, capacity, runtime_settings)
        .await
      {
        Ok(batches) => return batches,
        Err(read_error) => {
          if read_attempt == 0 {
            read_attempt = 1;
            self.metrics.retries.inc();
            warn_every!(
              15.seconds(),
              "consumer prefetch read retrying after error: error={read_error:#}"
            );
            continue;
          }

          self.metrics.failures.inc();
          warn_every!(
            15.seconds(),
            "consumer prefetch read failed after retry: error={read_error:#}"
          );
          self
            .time_provider
            .sleep(time::Duration::milliseconds(
              i64::try_from(self.base_idle_delay_ms).unwrap_or(i64::MAX),
            ))
            .await;
        },
      }
    }
  }
}

/// Convert reader-owned lifecycle state into the diagnostic representation after each mutation.
pub(super) fn record_reader_diagnostics(
  reader: &ConsumerReaderImpl,
  shared_state: &Arc<Mutex<ConsumerSharedState>>,
) {
  let mut shared_state = shared_state.lock();
  shared_state.diagnostics.cursors = offsets_from_map(&reader.cursors());
  shared_state.diagnostics.reader_partitions = reader
    .partition_read_states()
    .into_iter()
    .map(|state| reader_partition_snapshot(state.virtual_partition_id, state.mode))
    .collect();
  shared_state.diagnostics.reader_partition_scans = reader
    .partition_scan_states()
    .into_iter()
    .map(|state| {
      (
        state.virtual_partition_id,
        reader_partition_scan_snapshot(state),
      )
    })
    .collect();
}

fn reader_partition_scan_snapshot(
  state: &ConsumerReaderPartitionScanState,
) -> ConsumerReaderScanSnapshot {
  ConsumerReaderScanSnapshot {
    completed_at: format_unix_timestamp_ms(state.completed_at_unix_seconds.saturating_mul(1_000)),
    scanned_window_starts: state
      .scanned_window_starts
      .iter()
      .take(MAX_SCAN_DETAIL_ENTRIES)
      .map(|window_start| format_unix_timestamp_ms(window_start.saturating_mul(1_000)))
      .collect(),
    scanned_window_starts_truncated: state.scanned_window_starts.len() > MAX_SCAN_DETAIL_ENTRIES,
    fast_scan_bounds: state
      .fast_scan_bounds
      .iter()
      .take(MAX_SCAN_DETAIL_ENTRIES)
      .map(reader_fast_scan_bound_snapshot)
      .collect(),
    fast_scan_bounds_truncated: state.fast_scan_bounds.len() > MAX_SCAN_DETAIL_ENTRIES,
    fast_frontiers: state
      .fast_frontiers
      .iter()
      .take(MAX_SCAN_DETAIL_ENTRIES)
      .map(reader_fast_frontier_snapshot)
      .collect(),
    fast_frontiers_truncated: state.fast_frontiers.len() > MAX_SCAN_DETAIL_ENTRIES,
    cursor_before: state.cursor_before,
    cursor_after: state.cursor_after,
    metadata_segments_seen: state.metadata_segments_seen,
    metadata_segments_without_partition_batches: state.metadata_segments_without_partition_batches,
    metadata_batches_seen: state.metadata_batches_seen,
    metadata_batches_skipped_by_cursor: state.metadata_batches_skipped_by_cursor,
    metadata_segments_skipped_by_frontier: state.metadata_segments_skipped_by_frontier,
    metadata_segments_deferred_by_visibility: state.metadata_segments_deferred_by_visibility,
    metadata_segments_blocked_by_visibility: state.metadata_segments_blocked_by_visibility,
    metadata_batches_deferred_by_capacity: state.metadata_batches_deferred_by_capacity,
    batches_accepted: state.batches_accepted,
    records_accepted: state.records_accepted,
  }
}

fn reader_fast_scan_bound_snapshot(
  state: &ConsumerReaderFastScanBoundState,
) -> ConsumerReaderFastScanBoundSnapshot {
  ConsumerReaderFastScanBoundSnapshot {
    window_start: format_unix_timestamp_ms(state.window_start_unix_seconds.saturating_mul(1_000)),
    floor_timestamp: format_unix_timestamp_ms(
      state.floor_timestamp_unix_seconds.saturating_mul(1_000),
    ),
    time_floor_snowflake_id: state.time_floor.as_u64(),
    observed_frontier_snowflake_id: state.observed_frontier.map(SnowflakeId::as_u64),
    partition_lower_bound_snowflake_id: state.partition_lower_bound.as_u64(),
    query_lower_bound_snowflake_id: state.query_lower_bound.map(SnowflakeId::as_u64),
  }
}

fn reader_fast_frontier_snapshot(
  state: &ConsumerReaderFastFrontierState,
) -> ConsumerReaderFastFrontierSnapshot {
  ConsumerReaderFastFrontierSnapshot {
    window_start: format_unix_timestamp_ms(state.window_start_unix_seconds.saturating_mul(1_000)),
    snowflake_id: state.snowflake_id.as_u64(),
  }
}

/// Apply every queued reader mutation before starting another asynchronous reader scan.
fn process_reader_commands(
  reader: &mut ConsumerReaderImpl,
  reader_command_rx: &mut mpsc::UnboundedReceiver<ConsumerReaderCommand>,
  shared_state: &Arc<Mutex<ConsumerSharedState>>,
  diagnostics: &ConsumerDiagnostics,
  delivery_notify: &Notify,
  pending: &mut VecDeque<ConsumerBatch>,
  pending_record_count: &mut usize,
  pending_bytes: &mut u64,
) -> bool {
  loop {
    let command = match reader_command_rx.try_recv() {
      Ok(command) => command,
      Err(mpsc::error::TryRecvError::Empty) => return true,
      Err(mpsc::error::TryRecvError::Disconnected) => return false,
    };
    let mut handoff_assignment = None;
    let seek_response = match command {
      ConsumerReaderCommand::HydrateCursors {
        recovered_cursors,
        now_unix_seconds,
      } => {
        for (partition_id, recovered_cursor) in recovered_cursors {
          reader.hydrate_cursor_with_source(
            partition_id,
            &recovered_cursor.committed_cursor,
            recovered_cursor.committed_ts_ms,
            now_unix_seconds,
          );
        }
        None
      },
      ConsumerReaderCommand::SetAssignment {
        assignment,
        now_unix_seconds,
        handoff_phase,
        release_delivery_fence,
      } => {
        if let Err(error) = reader.set_assigned_virtual_partitions(&assignment, now_unix_seconds) {
          shared_state.lock().terminal_error = Some(format!("{error:#}"));
          return false;
        }
        if release_delivery_fence {
          let mut shared_state = shared_state.lock();
          if shared_state.delivery_state.revocation_in_progress {
            shared_state.delivery_state.revocation_in_progress = false;
            debug!("consumer delivery fence released after reader assignment update");
            delivery_notify.notify_waiters();
          }
        }
        handoff_assignment = handoff_phase.map(|phase| (assignment, phase));
        None
      },
      ConsumerReaderCommand::Seek {
        virtual_partition_id,
        offset,
        now_unix_seconds,
        response,
      } => {
        reader.seek(virtual_partition_id, offset, now_unix_seconds);
        // A seek invalidates all unread reader output for that partition, including batches that
        // have not crossed the shared byte-budget boundary yet.
        pending.retain(|batch| {
          if batch.virtual_partition_id != virtual_partition_id {
            return true;
          }
          *pending_record_count = pending_record_count.saturating_sub(batch.records.len());
          *pending_bytes = pending_bytes.saturating_sub(prefetched_batch_bytes(batch));
          false
        });
        Some(response)
      },
    };
    record_reader_diagnostics(reader, shared_state);
    if let Some((assignment, handoff_phase)) = handoff_assignment {
      {
        let mut shared_state = shared_state.lock();
        shared_state
          .diagnostics
          .active_assignment
          .clone_from(&assignment);
        shared_state
          .diagnostics
          .owned_partitions
          .clone_from(&assignment);
      }
      let handoff_snapshot = diagnostics.state_snapshot();
      let assignment_span = bd_log::otel_info_span!(
        "blob_stream.consumer.assignment",
        otel.kind = "internal",
        consumer.topic = %handoff_snapshot.topic,
        consumer.group_id = %handoff_snapshot.group_id,
        consumer.member_id = %handoff_snapshot.member_id,
        consumer.generation = handoff_snapshot.accepted_assignment_plan_version,
        assignment.phase = handoff_phase,
        assignment.partition_count = assignment.len(),
        otel.status_code = field::Empty,
      );
      emit_partition_handoff_snapshots(
        &handoff_snapshot,
        &assignment,
        handoff_phase,
        "assigned",
        "not_applicable",
        &assignment_span,
      );
      assignment_span.record("otel.status_code", "OK");
    }
    if let Some(response) = seek_response {
      let _ = response.send(Ok(()));
    }
  }
}

/// Translate reader mode data into stable, readable state-response fields.
fn reader_partition_snapshot(
  virtual_partition_id: VirtualPartitionId,
  mode: ConsumerReaderPartitionMode,
) -> ConsumerReaderPartitionSnapshot {
  match mode {
    ConsumerReaderPartitionMode::Fresh {
      initial_window_start_unix_seconds,
    } => ConsumerReaderPartitionSnapshot {
      virtual_partition_id,
      mode: ConsumerPartitionReadMode::Fresh,
      recovery_next_window_start: Some(format_unix_timestamp_ms(
        initial_window_start_unix_seconds.saturating_mul(1_000),
      )),
      recovery_cutover_window_start: None,
    },
    ConsumerReaderPartitionMode::Recovering {
      next_window_start_unix_seconds,
      cutover_window_start_unix_seconds,
    } => ConsumerReaderPartitionSnapshot {
      virtual_partition_id,
      mode: ConsumerPartitionReadMode::Recovering,
      recovery_next_window_start: Some(format_unix_timestamp_ms(
        next_window_start_unix_seconds.saturating_mul(1_000),
      )),
      recovery_cutover_window_start: Some(format_unix_timestamp_ms(
        cutover_window_start_unix_seconds.saturating_mul(1_000),
      )),
    },
    ConsumerReaderPartitionMode::Fast => ConsumerReaderPartitionSnapshot {
      virtual_partition_id,
      mode: ConsumerPartitionReadMode::Fast,
      recovery_next_window_start: None,
      recovery_cutover_window_start: None,
    },
  }
}
