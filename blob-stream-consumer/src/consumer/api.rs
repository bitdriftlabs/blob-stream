use crate::diagnostics::ConsumerReaderScanSnapshot;
use anyhow::Result;
use async_trait::async_trait;
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{CommittedSourceCheckpoint, Record, SeqRange, VirtualPartitionId};
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;

//
// ConsumerBatchSource
//

/// Immutable metadata and blob provenance for a decoded batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerBatchSource {
  /// Blob object containing the batch payload.
  pub blob_key: BlobKey,
  /// Instant immediately before the metadata index row was written.
  pub metadata_published_at: OffsetDateTime,
}

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
  /// Immutable source details for delivery-gap investigation.
  pub source: ConsumerBatchSource,
  /// Finalized scan evidence that admitted this batch, shared by every batch in the scan pass.
  pub admission_scan: Option<Arc<ConsumerReaderScanSnapshot>>,
  /// Decoded records for the batch.
  pub records: Vec<Record>,
}

impl ConsumerBatch {
  /// Drop records already covered by a cursor while preserving the remaining sequence offsets.
  pub(in crate::consumer) fn discard_through(&mut self, cursor: u64) {
    if cursor < self.seq_range.start {
      return;
    }
    let discarded_records = usize::try_from(
      cursor
        .saturating_sub(self.seq_range.start)
        .saturating_add(1),
    )
    .unwrap_or(usize::MAX);
    self.records.drain(.. discarded_records);
    self.seq_range.start = cursor.saturating_add(1);
  }
}

//
// ConsumerReadOutcome
//

/// Internal reader outcome used to schedule a metadata-maturity-aware empty-result retry.
pub struct ConsumerReadOutcome {
  /// Batches ready for the prefetch worker to admit to delivery.
  pub batches: Vec<ConsumerBatch>,
  /// Earliest instant at which deferred metadata can be safely retried.
  ///
  /// This covers eventual-consistency visibility delays and Fast cross-window sequence holds.
  ///
  /// This is present only when the pass has no ready batches and was not stopped by capacity.
  pub next_metadata_eligible_at: Option<OffsetDateTime>,
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
