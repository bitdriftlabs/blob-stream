use anyhow::Result;
use async_trait::async_trait;
use blob_stream_types::{CommittedSourceCheckpoint, Record, SeqRange, VirtualPartitionId};
use std::collections::HashMap;
use time::OffsetDateTime;

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
// ConsumerReadOutcome
//

/// Internal reader outcome used to schedule a visibility-aware empty-result retry.
pub struct ConsumerReadOutcome {
  /// Batches ready for the prefetch worker to admit to delivery.
  pub batches: Vec<ConsumerBatch>,
  /// Earliest instant at which a deferred metadata row can be safely retried.
  ///
  /// This is present only when the pass has no ready batches and was not stopped by capacity.
  pub next_visibility_eligible_at: Option<OffsetDateTime>,
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
    now: OffsetDateTime,
    capacity: ReadCapacity,
  ) -> Result<Vec<ConsumerBatch>>;
  /// Return committed cursor for a virtual partition, if known.
  fn cursor(&self, virtual_partition_id: VirtualPartitionId) -> Option<u64>;
  /// Return all tracked cursors.
  fn cursors(&self) -> HashMap<VirtualPartitionId, u64>;
}

//
// ReadCapacity
//

/// Payload-byte capacity available for one reader pass.
#[derive(Clone, Copy)]
pub struct ReadCapacity {
  remaining_payload_bytes: u64,
  oversized_batch_allowed: bool,
}

impl ReadCapacity {
  /// Create a capacity that admits decoded payloads up to `remaining_payload_bytes`.
  #[must_use]
  pub fn new(remaining_payload_bytes: u64) -> Self {
    Self {
      remaining_payload_bytes,
      oversized_batch_allowed: false,
    }
  }

  pub(crate) fn with_oversized_batch(
    remaining_payload_bytes: u64,
    oversized_batch_allowed: bool,
  ) -> Self {
    Self {
      remaining_payload_bytes,
      oversized_batch_allowed,
    }
  }

  /// Reserve one decoded batch. A single oversized batch may make progress from an empty buffer.
  pub(in crate::consumer) fn reserve(&mut self, payload_bytes: u64) -> bool {
    if payload_bytes <= self.remaining_payload_bytes {
      self.remaining_payload_bytes = self.remaining_payload_bytes.saturating_sub(payload_bytes);
      self.oversized_batch_allowed = false;
      return true;
    }
    if self.oversized_batch_allowed {
      self.remaining_payload_bytes = 0;
      self.oversized_batch_allowed = false;
      return true;
    }
    false
  }
}
