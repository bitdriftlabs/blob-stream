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
  ConsumerSharedState,
  prefetched_batch_bytes,
  update_total_prefetch_bytes,
  update_worker_prefetch_metrics,
};
use crate::consumer::{
  ConsumerBatch,
  ConsumerReader,
  ConsumerReaderFastFrontierState,
  ConsumerReaderImpl,
  ConsumerReaderPartitionMode,
  ConsumerReaderPartitionScanState,
  ReadCapacity,
};
use crate::coordination::RecoveredCursor;
use crate::diagnostics::{
  ConsumerPartitionReadMode,
  ConsumerReaderFastFrontierSnapshot,
  ConsumerReaderPartitionSnapshot,
  ConsumerReaderScanSnapshot,
  offsets_from_map,
};
use anyhow::Result;
use bd_log::warn_every;
use blob_stream_types::{VirtualPartitionId, format_unix_timestamp_ms, now_unix_seconds};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use time::ext::NumericalDuration;
use tokio::sync::{Notify, mpsc, oneshot};

//
// IdlePollBackoff
//

/// Exponential idle delay used only when a complete reader pass produces no batches.
pub(super) struct IdlePollBackoff {
  base_delay_ms: u64,
  max_delay_ms: Option<u64>,
  current_delay_ms: u64,
}

impl IdlePollBackoff {
  pub(super) fn new(base_delay_ms: u64, max_delay_ms: Option<u64>) -> Self {
    Self {
      base_delay_ms,
      max_delay_ms,
      current_delay_ms: base_delay_ms,
    }
  }

  /// Return the current delay before increasing it for the next consecutive empty read.
  pub(super) fn next_delay_ms(&mut self) -> u64 {
    let delay_ms = self.current_delay_ms;
    if let Some(max_delay_ms) = self.max_delay_ms {
      self.current_delay_ms = self.current_delay_ms.saturating_mul(2).min(max_delay_ms);
    }
    delay_ms
  }

  /// A nonempty reader result restores the configured base poll delay.
  pub(super) fn reset(&mut self) {
    self.current_delay_ms = self.base_delay_ms;
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
  },
  Seek {
    virtual_partition_id: VirtualPartitionId,
    offset: u64,
    now_unix_seconds: i64,
    response: oneshot::Sender<Result<()>>,
  },
}

//
// PrefetchWorker
//

/// Single-owner task that scans the reader and admits batches into caller-visible delivery.
pub(super) struct PrefetchWorker {
  reader: ConsumerReaderImpl,
  reader_command_rx: mpsc::UnboundedReceiver<ConsumerReaderCommand>,
  shared_state: Arc<Mutex<ConsumerSharedState>>,
  delivery_notify: Arc<Notify>,
  prefetch_space_notify: Arc<Notify>,
  prefetch_shutdown: Arc<AtomicBool>,
  metrics: ConsumerIteratorMetrics,
  base_idle_delay_ms: u64,
  max_idle_delay_ms: Option<u64>,
}

impl PrefetchWorker {
  #[allow(clippy::too_many_arguments)]
  pub(super) fn new(
    reader: ConsumerReaderImpl,
    reader_command_rx: mpsc::UnboundedReceiver<ConsumerReaderCommand>,
    shared_state: Arc<Mutex<ConsumerSharedState>>,
    delivery_notify: Arc<Notify>,
    prefetch_space_notify: Arc<Notify>,
    prefetch_shutdown: Arc<AtomicBool>,
    metrics: ConsumerIteratorMetrics,
    base_idle_delay_ms: u64,
    max_idle_delay_ms: Option<u64>,
  ) -> Self {
    Self {
      reader,
      reader_command_rx,
      shared_state,
      delivery_notify,
      prefetch_space_notify,
      prefetch_shutdown,
      metrics,
      base_idle_delay_ms,
      max_idle_delay_ms,
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
        return;
      }
      if !process_reader_commands(
        &mut self.reader,
        &mut self.reader_command_rx,
        &self.shared_state,
        &mut pending,
        &mut pending_record_count,
        &mut pending_bytes,
      ) {
        return;
      }

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

      self
        .metrics
        .next_latency_seconds
        .observe(read_started_at.elapsed().as_secs_f64());

      if batches.is_empty() {
        let idle_delay_ms = idle_poll_backoff.next_delay_ms();
        tokio::time::sleep(std::time::Duration::from_millis(idle_delay_ms)).await;
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
    let pending_remains = {
      let mut shared_state = self.shared_state.lock();

      while let Some(batch) = pending.front() {
        let batch_record_count = batch.records.len();
        let batch_bytes = prefetched_batch_bytes(batch);
        // Assignment can change while the reader is blocked. Drop obsolete pending work before it
        // consumes shared buffer capacity or becomes visible to the caller.
        if !shared_state
          .active_assignment
          .contains(&batch.virtual_partition_id)
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

      !pending.is_empty()
    };
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
      let now_s = now_unix_seconds();
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
              "consumer prefetch read retrying after error: error={read_error}"
            );
            continue;
          }

          self.metrics.failures.inc();
          warn_every!(
            15.seconds(),
            "consumer prefetch read failed after retry: error={read_error}"
          );
          tokio::time::sleep(std::time::Duration::from_millis(self.base_idle_delay_ms)).await;
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
      .map(|window_start| format_unix_timestamp_ms(window_start.saturating_mul(1_000)))
      .collect(),
    fast_frontiers: state
      .fast_frontiers
      .iter()
      .map(reader_fast_frontier_snapshot)
      .collect(),
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
      } => {
        if let Err(error) = reader.set_assigned_virtual_partitions(&assignment, now_unix_seconds) {
          shared_state.lock().terminal_error = Some(error.to_string());
          return false;
        }
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
