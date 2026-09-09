use blob_stream_types::{SeqRange, SnowflakeId, VirtualPartitionId};
use std::collections::VecDeque;
use std::sync::Arc;
use time::OffsetDateTime;

pub const MAX_METADATA_SOURCE_DETAILS: usize = 64;

//
// ConsumerReaderPartitionScanState
//

/// Most recent successful metadata-scan outcome for one assigned virtual partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerReaderPartitionScanState {
  pub(crate) virtual_partition_id: VirtualPartitionId,
  pub(crate) scanned_window_starts: Arc<[i64]>,
  pub(crate) fast_scan_bounds: Vec<ConsumerReaderFastScanBoundState>,
  pub(crate) fast_frontiers: Vec<ConsumerReaderFastFrontierState>,
  pub(crate) completed_at_unix_seconds: i64,
  pub(crate) cursor_before: Option<u64>,
  pub(crate) cursor_after: Option<u64>,
  pub(crate) metadata_segments_seen: usize,
  pub(crate) metadata_segments_without_partition_batches: usize,
  pub(crate) metadata_sources: VecDeque<ConsumerReaderMetadataSource>,
  pub(crate) metadata_sources_truncated: bool,
  pub(crate) metadata_sources_incomplete_by_capacity: bool,
  pub(crate) metadata_batches_seen: usize,
  pub(crate) metadata_batches_skipped_by_cursor: usize,
  pub(crate) metadata_segments_skipped_by_frontier: usize,
  pub(crate) metadata_segments_deferred_by_visibility: usize,
  pub(crate) metadata_segments_blocked_by_visibility: usize,
  pub(crate) recovery_segments_handed_to_fast_by_visibility: usize,
  pub(crate) recovery_segments_blocked_by_visibility: usize,
  pub(crate) mature_metadata_cache_reuses: usize,
  pub(crate) metadata_batches_deferred_by_capacity: usize,
  pub(crate) batches_accepted: usize,
  pub(crate) records_accepted: usize,
}

//
// ConsumerReaderFastScanBoundState
//

/// Fast-path lower-bound inputs used to plan the shared metadata query for one window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerReaderFastScanBoundState {
  pub(crate) window_start_unix_seconds: i64,
  pub(crate) floor_timestamp: OffsetDateTime,
  pub(crate) time_floor: SnowflakeId,
  pub(crate) observed_frontier: Option<SnowflakeId>,
  pub(crate) partition_lower_bound: SnowflakeId,
  pub(crate) query_lower_bound: Option<SnowflakeId>,
}

//
// ConsumerReaderFastFrontierState
//

/// Observed inclusive metadata frontier retained after a successful scan of one eligible Fast
/// window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerReaderFastFrontierState {
  pub(crate) window_start_unix_seconds: i64,
  pub(crate) snowflake_id: SnowflakeId,
}

impl ConsumerReaderPartitionScanState {
  pub(in crate::consumer) fn new(
    virtual_partition_id: VirtualPartitionId,
    scanned_window_starts: Arc<[i64]>,
    completed_at_unix_seconds: i64,
    cursor_before: Option<u64>,
  ) -> Self {
    Self {
      virtual_partition_id,
      scanned_window_starts,
      fast_scan_bounds: Vec::new(),
      fast_frontiers: Vec::new(),
      completed_at_unix_seconds,
      cursor_before,
      cursor_after: cursor_before,
      metadata_segments_seen: 0,
      metadata_segments_without_partition_batches: 0,
      metadata_sources: VecDeque::with_capacity(MAX_METADATA_SOURCE_DETAILS),
      metadata_sources_truncated: false,
      metadata_sources_incomplete_by_capacity: false,
      metadata_batches_seen: 0,
      metadata_batches_skipped_by_cursor: 0,
      metadata_segments_skipped_by_frontier: 0,
      metadata_segments_deferred_by_visibility: 0,
      metadata_segments_blocked_by_visibility: 0,
      recovery_segments_handed_to_fast_by_visibility: 0,
      recovery_segments_blocked_by_visibility: 0,
      mature_metadata_cache_reuses: 0,
      metadata_batches_deferred_by_capacity: 0,
      batches_accepted: 0,
      records_accepted: 0,
    }
  }
}

//
// ConsumerReaderMetadataSource
//

/// A metadata row observed for one partition during a reader scan, retained in Snowflake order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerReaderMetadataSource {
  pub(crate) window_start_unix_seconds: i64,
  pub(crate) snowflake_id: u64,
  pub(crate) blob_key: String,
  pub(crate) metadata_published_at: OffsetDateTime,
  pub(crate) batch_ranges: Vec<SeqRange>,
}
