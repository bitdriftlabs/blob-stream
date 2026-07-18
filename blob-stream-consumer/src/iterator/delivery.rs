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

use super::{ConsumerIteratorMetrics, ConsumerRecord, NextResult};
use crate::consumer::ConsumerBatch;
use blob_stream_types::{CommittedSourceCheckpoint, Record, VirtualPartitionId};
use std::collections::{HashMap, HashSet, VecDeque};

//
// BufferedBatch
//

/// A batch currently being expanded into individual records for the caller.
pub struct BufferedBatch {
  pub(crate) virtual_partition_id: VirtualPartitionId,
  pub(super) next_offset: u64,
  pub(super) source_checkpoint: CommittedSourceCheckpoint,
  pub records: std::vec::IntoIter<Record>,
}

//
// DeliveredSourceRange
//

/// Consecutive offsets that share the source checkpoint needed by a later commit.
#[derive(Clone)]
pub(super) struct DeliveredSourceRange {
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
}

impl DeliveryState {
  /// Return payload bytes retained in both queued and currently delivered batches.
  pub(super) fn retained_bytes(&self) -> u64 {
    self
      .buffered_bytes
      .saturating_add(self.current_batch.as_ref().map_or(0, |batch| {
        batch
          .records
          .as_slice()
          .iter()
          .fold(0_u64, |total, record| {
            total.saturating_add(record.payload.len() as u64)
          })
      }))
  }

  /// Return the next revocation or record that remains valid for the active assignment.
  pub(super) fn try_take_next(
    &mut self,
    active_assignment: &HashSet<VirtualPartitionId>,
    delivered_source_ranges: &mut HashMap<VirtualPartitionId, Vec<DeliveredSourceRange>>,
    metrics: &ConsumerIteratorMetrics,
  ) -> Option<NextResult> {
    // A revocation takes precedence over records so callers cannot observe a replacement
    // assignment before acknowledging the ownership loss.
    if let Some(revocation) = self.pending_revocation.take() {
      return Some(revocation);
    }

    loop {
      // Fencing can race with caller polling. Drop an in-flight batch before producing another
      // record when its partition is no longer locally active.
      if self
        .current_batch
        .as_ref()
        .is_some_and(|batch| !active_assignment.contains(&batch.virtual_partition_id))
      {
        self.current_batch = None;
        continue;
      }

      if let Some(current_batch) = self.current_batch.as_mut() {
        if let Some(record) = current_batch.records.next() {
          let offset = current_batch.next_offset;
          current_batch.next_offset = current_batch.next_offset.saturating_add(1);
          record_delivered_source(
            delivered_source_ranges,
            current_batch.virtual_partition_id,
            offset,
            current_batch.source_checkpoint.clone(),
          );
          metrics.records_delivered.inc();
          return Some(NextResult::Record(ConsumerRecord {
            virtual_partition_id: current_batch.virtual_partition_id,
            offset,
            record,
          }));
        }
        self.current_batch = None;
      }

      let batch = self.batches.pop_front()?;
      self.buffered_bytes = self
        .buffered_bytes
        .saturating_sub(prefetched_batch_bytes(&batch));
      if !active_assignment.contains(&batch.virtual_partition_id) {
        continue;
      }

      metrics.batches_delivered.inc();
      self.current_batch = Some(BufferedBatch {
        virtual_partition_id: batch.virtual_partition_id,
        next_offset: batch.seq_range.start,
        source_checkpoint: batch.source_checkpoint,
        records: batch.records.into_iter(),
      });
    }
  }

  /// Discard all local delivery state for revoked partitions before assignment changes.
  pub(super) fn drop_partitions(&mut self, partitions: &HashSet<VirtualPartitionId>) {
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

/// Record source provenance in compact consecutive ranges for `store_offset` validation.
pub(super) fn record_delivered_source(
  delivered_source_ranges: &mut HashMap<VirtualPartitionId, Vec<DeliveredSourceRange>>,
  virtual_partition_id: VirtualPartitionId,
  offset: u64,
  source_checkpoint: CommittedSourceCheckpoint,
) {
  let ranges = delivered_source_ranges
    .entry(virtual_partition_id)
    .or_default();
  if let Some(last) = ranges.last_mut()
    && last.end_offset.saturating_add(1) == offset
    && last.source_checkpoint == source_checkpoint
  {
    last.end_offset = offset;
    return;
  }
  ranges.push(DeliveredSourceRange {
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
