//! Delivery queue mechanics for the consumer iterator.
//!
//! The prefetch worker owns queued `ConsumerBatch` values until it moves them into
//! `DeliveryState`. The public iterator then owns record-by-record delivery from the current
//! batch. These paths synchronize through `ConsumerSharedState`; this module never awaits while
//! it mutates the delivery queue.
//!
//! A revocation removes both queued and current batches before the replacement assignment can
//! activate. This avoids delivering records from a partition after its consumer-group lease has
//! been fenced, while leaving durable cursor recovery to the next owner.

use super::shared::{ActivePartitionState, ConsumerIteratorMetrics};
use super::{ConsumerRecord, NextResult};
use crate::consumer::ConsumerBatch;
use bd_log_util::warn_every;
use blob_stream_types::{CommittedSourceCheckpoint, Record, VirtualPartitionId};
use log::debug;
use std::collections::{HashMap, HashSet, VecDeque};
use time::ext::NumericalDuration;

//
// BufferedBatch
//

/// A batch currently being expanded into individual records for the caller.
pub struct BufferedBatch {
  pub(crate) virtual_partition_id: VirtualPartitionId,
  pub(super) next_offset: u64,
  pub(super) source_checkpoint: CommittedSourceCheckpoint,
  pub(super) remaining_payload_bytes: u64,
  pub records: std::vec::IntoIter<Record>,
}

//
// DeliveredSourceRange
//

/// Consecutive offsets that share the source checkpoint needed by a later commit.
#[derive(Clone)]
pub struct DeliveredSourceRange {
  pub(super) start_offset: u64,
  pub(super) end_offset: u64,
  pub(super) source_checkpoint: CommittedSourceCheckpoint,
}

//
// DeliveryState
//

/// Batches made visible to callers and the single batch currently being delivered.
#[derive(Default)]
pub struct DeliveryState {
  pub batches: VecDeque<ConsumerBatch>,
  pub buffered_bytes: u64,
  pub current_batch: Option<BufferedBatch>,
  pub pending_revocation: Option<NextResult>,
  pub revocation_in_progress: bool,
}

impl DeliveryState {
  /// Return payload bytes retained in both queued and currently delivered batches.
  pub(super) fn retained_bytes(&self) -> u64 {
    self.buffered_bytes.saturating_add(
      self
        .current_batch
        .as_ref()
        .map_or(0, |batch| batch.remaining_payload_bytes),
    )
  }

  /// Return the next revocation or record that remains valid for active iterator partitions.
  pub(super) fn try_take_next(
    &mut self,
    active_partitions: &mut HashMap<VirtualPartitionId, ActivePartitionState>,
    metrics: &ConsumerIteratorMetrics,
  ) -> Option<NextResult> {
    // A revocation takes precedence over records so callers cannot observe a replacement
    // assignment before acknowledging the ownership loss.
    if let Some(revocation) = self.pending_revocation.take() {
      debug!("consumer delivery surfaced revocation callback");
      return Some(revocation);
    }
    if self.revocation_in_progress {
      debug!("consumer delivery is fenced while revocation completion is pending");
      return None;
    }

    loop {
      // Fencing can race with caller polling. Drop an in-flight batch before producing another
      // record when its partition is no longer locally active.
      if self
        .current_batch
        .as_ref()
        .is_some_and(|batch| !active_partitions.contains_key(&batch.virtual_partition_id))
      {
        self.current_batch = None;
        continue;
      }

      if let Some(current_batch) = self.current_batch.as_mut() {
        if let Some(record) = current_batch.records.next() {
          current_batch.remaining_payload_bytes = current_batch
            .remaining_payload_bytes
            .saturating_sub(u64::try_from(record.payload.len()).unwrap_or(u64::MAX));
          let offset = current_batch.next_offset;
          current_batch.next_offset = current_batch.next_offset.saturating_add(1);
          record_delivery_offset(
            active_partitions,
            current_batch.virtual_partition_id,
            offset,
            metrics,
          );
          record_delivered_source(
            active_partitions,
            current_batch.virtual_partition_id,
            offset,
            current_batch.source_checkpoint.clone(),
          );
          metrics.records_delivered.inc();
          debug!(
            "consumer delivery surfaced record: partition={}, offset={offset}",
            current_batch.virtual_partition_id,
          );
          return Some(NextResult::Record(ConsumerRecord {
            virtual_partition_id: current_batch.virtual_partition_id,
            offset,
            source_checkpoint: current_batch.source_checkpoint.clone(),
            record,
          }));
        }
        self.current_batch = None;
      }

      let batch = self.batches.pop_front()?;
      self.buffered_bytes = self
        .buffered_bytes
        .saturating_sub(prefetched_batch_bytes(&batch));
      if !active_partitions.contains_key(&batch.virtual_partition_id) {
        continue;
      }

      metrics.batches_delivered.inc();
      let remaining_payload_bytes = prefetched_batch_bytes(&batch);
      self.current_batch = Some(BufferedBatch {
        virtual_partition_id: batch.virtual_partition_id,
        next_offset: batch.seq_range.start,
        source_checkpoint: batch.source_checkpoint,
        remaining_payload_bytes,
        records: batch.records.into_iter(),
      });
    }
  }

  /// Discard all local delivery state for revoked partitions before assignment changes.
  pub(super) fn drop_partitions(&mut self, partitions: &HashSet<VirtualPartitionId>) {
    debug!("consumer delivery dropping revoked partitions: {partitions:?}");
    if self
      .current_batch
      .as_ref()
      .is_some_and(|batch| partitions.contains(&batch.virtual_partition_id))
    {
      self.current_batch = None;
    }

    self
      .batches
      .retain(|batch| !partitions.contains(&batch.virtual_partition_id));
    self.buffered_bytes = self.batches.iter().map(prefetched_batch_bytes).sum();
  }
}

/// Record the application-visible sequence boundary for one partition.
fn record_delivery_offset(
  active_partitions: &mut HashMap<VirtualPartitionId, ActivePartitionState>,
  virtual_partition_id: VirtualPartitionId,
  offset: u64,
  metrics: &ConsumerIteratorMetrics,
) {
  let Some(partition_state) = active_partitions.get_mut(&virtual_partition_id) else {
    return;
  };
  if let Some(expected_offset) = partition_state
    .delivery_gap_baseline
    .and_then(|baseline| baseline.checked_add(1))
    && offset > expected_offset
  {
    let missing_sequences = offset - expected_offset;
    metrics.delivery_gap_events.inc();
    warn_every!(
      15.seconds(),
      "consumer delivery gap: partition={virtual_partition_id}, \
       expected_offset={expected_offset}, received_offset={offset}, \
       missing_sequences={missing_sequences}"
    );
  }
  partition_state.delivery_gap_baseline = Some(offset);
}

/// Record source provenance in compact consecutive ranges for `store_offset` validation.
pub(super) fn record_delivered_source(
  active_partitions: &mut HashMap<VirtualPartitionId, ActivePartitionState>,
  virtual_partition_id: VirtualPartitionId,
  offset: u64,
  source_checkpoint: CommittedSourceCheckpoint,
) {
  let Some(partition_state) = active_partitions.get_mut(&virtual_partition_id) else {
    return;
  };
  if let Some(last) = partition_state.delivered_source_ranges.last_mut()
    && last.end_offset.saturating_add(1) == offset
    && last.source_checkpoint == source_checkpoint
  {
    last.end_offset = offset;
    return;
  }
  partition_state
    .delivered_source_ranges
    .push(DeliveredSourceRange {
      start_offset: offset,
      end_offset: offset,
      source_checkpoint,
    });
}

/// Return the payload bytes that count against the configured prefetch budget.
pub(super) fn prefetched_batch_bytes(batch: &ConsumerBatch) -> u64 {
  batch.records.iter().fold(0_u64, |acc, record| {
    acc.saturating_add(record.payload.len() as u64)
  })
}

/// Keep delivery-owned occupancy metrics aligned with queued and current caller-visible records.
pub(super) fn update_worker_prefetch_metrics(
  metrics: &ConsumerIteratorMetrics,
  buffer: &DeliveryState,
) {
  metrics
    .prefetch_buffered_batches
    .set(i64::try_from(buffer.batches.len()).unwrap_or(i64::MAX));
  metrics
    .prefetch_buffered_bytes
    .set(i64::try_from(buffer.retained_bytes()).unwrap_or(i64::MAX));
}

/// Publish total decoded payload occupancy across delivery-owned and worker-pending batches.
pub(super) fn update_total_prefetch_bytes(
  metrics: &ConsumerIteratorMetrics,
  buffer: &DeliveryState,
  pending_bytes: u64,
) {
  metrics
    .prefetch_total_bytes
    .set(i64::try_from(buffer.retained_bytes().saturating_add(pending_bytes)).unwrap_or(i64::MAX));
}
