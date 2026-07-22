use super::WriteConfig;
use anyhow::Result;
use blob_stream_types::{BatchSummary, Record, SeqRange, VirtualPartitionId};
use tokio::sync::oneshot;

pub(super) type FlushCompletion = oneshot::Sender<Result<(), String>>;

//
// BufferState
//

#[derive(Debug, Default)]
pub(super) struct BufferState {
  pub(super) batches: Vec<BufferedBatch>,
  pub(super) buffered_bytes: u64,
  pub(super) first_buffered_ts_ms: Option<i64>,
}

impl BufferState {
  pub(super) fn push(&mut self, batch: BufferedBatch, now_ts_ms: i64) {
    if self.first_buffered_ts_ms.is_none() {
      self.first_buffered_ts_ms = Some(now_ts_ms);
    }
    self.buffered_bytes = self
      .buffered_bytes
      .saturating_add(batch.summary.payload_bytes);
    self.batches.push(batch);
  }

  pub(super) fn flush_trigger(&self, now_ts_ms: i64, config: &WriteConfig) -> Option<FlushTrigger> {
    if self.batches.is_empty() {
      return None;
    }
    if self.buffered_bytes >= config.flush_max_bytes {
      return Some(FlushTrigger::MaxBytes);
    }

    let first_ts = self.first_buffered_ts_ms?;
    (now_ts_ms.saturating_sub(first_ts) >= config.flush_max_delay_ms)
      .then_some(FlushTrigger::MaxDelay)
  }

  pub(super) fn is_time_due(&self, now_ts_ms: i64, config: &WriteConfig) -> bool {
    self
      .first_buffered_ts_ms
      .is_some_and(|first_ts| now_ts_ms.saturating_sub(first_ts) >= config.flush_max_delay_ms)
  }

  pub(super) fn reset(&mut self) {
    self.batches.clear();
    self.buffered_bytes = 0;
    self.first_buffered_ts_ms = None;
  }
}

//
// BufferedBatch
//

#[derive(Debug)]
pub(super) struct BufferedBatch {
  pub(super) records: Vec<Record>,
  pub(super) summary: BatchSummary,
  pub(super) seq_range: SeqRange,
  pub(super) completion: Option<FlushCompletion>,
}

//
// FlushPlan
//

#[derive(Debug)]
pub(super) struct FlushPlan {
  pub(super) topic: String,
  pub(super) partitions: Vec<FlushPartition>,
  pub(super) max_metadata_publication_lag_ms: u64,
}

//
// FlushPartition
//

#[derive(Debug)]
pub(super) struct FlushPartition {
  pub(super) virtual_partition_id: VirtualPartitionId,
  pub(super) batches: Vec<BufferedBatch>,
  pub(super) trigger: FlushTrigger,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum FlushTrigger {
  MaxBytes,
  MaxDelay,
  LeaseDrain,
}
