use super::config::EffectiveFlushConfig;
use anyhow::Result;
use blob_stream_metadata_store::ProducerLeaseFence;
use blob_stream_types::{BatchSummary, Record, SeqRange, VirtualPartitionId};
use protobuf::Chars;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::sync::{oneshot, watch};

pub(super) type FlushCompletion = oneshot::Sender<Result<(), FlushCompletionError>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FlushCompletionError {
  LeaseFenceLost,
  Internal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FlushPublicationResult {
  Succeeded,
  Failed(FlushCompletionError),
}

#[derive(Clone, Debug)]
pub(super) struct FlushPublicationDependency {
  pub(super) result_rx: watch::Receiver<Option<FlushPublicationResult>>,
}

#[derive(Debug)]
pub(super) struct FlushPublicationCompletion {
  pub(super) topic: Chars,
  pub(super) virtual_partition_id: VirtualPartitionId,
  pub(super) result_tx: watch::Sender<Option<FlushPublicationResult>>,
}

#[derive(Clone, Debug)]
pub(super) struct FlushPartitionResult {
  pub(super) topic: Chars,
  pub(super) virtual_partition_id: VirtualPartitionId,
  pub(super) error: Option<FlushCompletionError>,
}

//
// BufferState
//

#[derive(Debug, Default)]
pub(super) struct BufferState {
  pub(super) batches: Vec<BufferedBatch>,
  pub(super) buffered_bytes: u64,
  pub(super) first_buffered_at: Option<OffsetDateTime>,
}

impl BufferState {
  pub(super) fn push(&mut self, batch: BufferedBatch, now: OffsetDateTime) {
    if self.first_buffered_at.is_none() {
      self.first_buffered_at = Some(now);
    }
    self.buffered_bytes = self
      .buffered_bytes
      .saturating_add(batch.summary.payload_bytes);
    self.batches.push(batch);
  }

  pub(super) fn flush_trigger(
    &self,
    now: OffsetDateTime,
    config: &EffectiveFlushConfig,
  ) -> Option<FlushTrigger> {
    if self.batches.is_empty() {
      return None;
    }
    if self.buffered_bytes >= config.max_bytes {
      return Some(FlushTrigger::MaxBytes);
    }

    let first_buffered_at = self.first_buffered_at?;
    (now - first_buffered_at >= config.max_delay).then_some(FlushTrigger::MaxDelay)
  }

  pub(super) fn is_time_due(&self, now: OffsetDateTime, config: &EffectiveFlushConfig) -> bool {
    self
      .first_buffered_at
      .is_some_and(|first_buffered_at| now - first_buffered_at >= config.max_delay)
  }

  pub(super) fn reset(&mut self) {
    self.batches.clear();
    self.buffered_bytes = 0;
    self.first_buffered_at = None;
  }

  pub(super) fn discard(&mut self) -> Vec<FlushCompletion> {
    self.buffered_bytes = 0;
    self.first_buffered_at = None;
    std::mem::take(&mut self.batches)
      .into_iter()
      .filter_map(|mut batch| batch.completion.take())
      .collect()
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
  pub(super) acceptance_fence: Option<Arc<ProducerLeaseFence>>,
  pub(super) completion: Option<FlushCompletion>,
}

//
// TopicFlushPlan
//

#[derive(Debug)]
pub(super) struct TopicFlushPlan {
  pub(super) topic: Chars,
  pub(super) partitions: Vec<FlushPartition>,
  pub(super) max_metadata_publication_lag: Duration,
  pub(super) metadata_window_size: Duration,
  pub(super) fenced_metadata_writes: bool,
}

//
// FlushPlan
//

/// One or more topic sections selected for one or more bounded immutable objects.
#[derive(Debug)]
pub(super) struct FlushPlan {
  pub(super) topics: Vec<TopicFlushPlan>,
  pub(super) max_segment_bytes: u64,
  pub(super) shared_blob: bool,
  pub(super) publication_completions: Vec<FlushPublicationCompletion>,
}

//
// FlushPartition
//

#[derive(Debug)]
pub(super) struct FlushPartition {
  pub(super) virtual_partition_id: VirtualPartitionId,
  pub(super) lease_fence: Option<Arc<ProducerLeaseFence>>,
  pub(super) batches: Vec<BufferedBatch>,
  pub(super) trigger: FlushTrigger,
  pub(super) publication_predecessor: Option<FlushPublicationDependency>,
  pub(super) publication_result_tx: Option<watch::Sender<Option<FlushPublicationResult>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FlushTrigger {
  MaxBytes,
  MaxDelay,
  LeaseDrain,
}
