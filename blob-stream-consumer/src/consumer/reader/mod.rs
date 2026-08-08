use super::{
  ConsumerBatch,
  ConsumerReader,
  ConsumerReaderMetrics,
  ConsumerReaderPartitionScanState,
  ConsumerReaderPartitionState,
  ReadCapacity,
  RecoveryState,
  VirtualPartitionState,
  format_unix_timestamp_seconds,
  metadata_availability_delay_seconds,
};
use crate::config::{
  ConsumerReadConfig,
  ConsumerReadRuntimeSettings,
  consumer_read_runtime_settings,
  consumer_window_size_seconds,
  validate_read_config,
};
use anyhow::{Result, ensure};
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use bd_server_stats::stats::Scope;
use blob_stream_blob_store::BlobStore;
use blob_stream_metadata_store::{MetadataStore, SegmentMetadata};
use blob_stream_types::{BatchMetadata, CommittedCursor, SnowflakeId, VirtualPartitionId, Window};
use log::info;
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;

mod diagnostics;
mod lifecycle;
mod runtime;

const HISTORICAL_SEEK_RECOVERY_SECONDS: i64 = 10 * 60;

/// Identity of one immutable recovery metadata response retained by this reader instance.
pub(in crate::consumer) type RecoveryMetadataCacheKey = (VirtualPartitionId, i64, Option<u64>);

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
//    - Fast partitions scan only windows that can still contain unpublished or invisible metadata.
//      The shared Sonyflake time floor derived from the publication and visibility bounds narrows
//      each query, including when a sparse partition has no observed segment frontier.
//    - Each partition/window pair retains an inclusive observed frontier. The query uses the lowest
//      effective lower bound among eligible fast partitions, then each partition filters
//      independently. This finds late metadata without rereading a completed sparse window.
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
// ConsumerReaderImpl
//

/// Default `ConsumerReader` implementation used by `ConsumerIteratorImpl`.
pub struct ConsumerReaderImpl {
  pub(in crate::consumer) config: ConsumerReadConfig,
  pub(in crate::consumer) blob_store: Arc<dyn BlobStore>,
  pub(in crate::consumer) metadata_store: Arc<dyn MetadataStore>,
  pub(in crate::consumer) virtual_partition_states:
    HashMap<VirtualPartitionId, VirtualPartitionState>,
  pub(in crate::consumer) retention_days: u32,
  pub(in crate::consumer) maximum_metadata_publication_lag_ms: u64,
  pub(in crate::consumer) fast_frontiers: HashMap<(VirtualPartitionId, i64), SnowflakeId>,
  pub(in crate::consumer) recovery_scan_last_partition: Option<VirtualPartitionId>,
  pub(in crate::consumer) recovery_metadata_cache:
    HashMap<RecoveryMetadataCacheKey, Arc<[SegmentMetadata]>>,
  pub(in crate::consumer) feature_flags: Option<FeatureFlagsWatch>,
  pub(in crate::consumer) metrics: ConsumerReaderMetrics,
}

impl ConsumerReaderImpl {
  /// Record the current reader-local cache footprint after any mutation.
  pub(in crate::consumer) fn record_recovery_metadata_cache_state(&self) {
    self
      .metrics
      .record_recovery_metadata_cache_entries(self.recovery_metadata_cache.len());
    self.metrics.record_recovery_metadata_cache_retained_bytes(
      self
        .recovery_metadata_cache
        .values()
        .flat_map(|segments| segments.iter())
        .map(Self::recovery_metadata_cache_segment_bytes)
        .fold(0_u64, u64::saturating_add),
    );
  }

  fn recovery_metadata_cache_segment_bytes(segment: &SegmentMetadata) -> u64 {
    let mut bytes = u64::try_from(size_of::<SegmentMetadata>()).unwrap_or(u64::MAX);
    bytes = bytes.saturating_add(u64::try_from(segment.window.topic.len()).unwrap_or(u64::MAX));
    bytes =
      bytes.saturating_add(u64::try_from(segment.blob_key.as_str().len()).unwrap_or(u64::MAX));
    for batches in segment.segment_index.values() {
      bytes = bytes.saturating_add(
        u64::try_from(size_of::<BatchMetadata>().saturating_mul(batches.capacity()))
          .unwrap_or(u64::MAX),
      );
    }
    bytes
  }
}
