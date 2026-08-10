//! Metadata scan planning and blob batch decoding for `ConsumerReaderImpl`.
//!
//! This module owns the mechanics that turn reader lifecycle state into metadata requests. It
//! deliberately keeps recovery chronological, scopes fast scans to the publication horizon, and
//! leaves lifecycle transitions to the caller after an entire scan pass succeeds.

use super::{
  ConsumerBatch,
  ConsumerReaderFastFrontierState,
  ConsumerReaderFastScanBoundState,
  ConsumerReaderImpl,
  ConsumerReaderPartitionScanState,
  ReadCapacity,
  VirtualPartitionState,
  format_unix_timestamp_seconds,
  metadata_availability_delay_seconds,
};
use crate::config::{
  ConsumerReadRuntimeSettings,
  consumer_candidate_window_count,
  consumer_metadata_visibility_delay_ms,
  consumer_window_size_seconds,
};
use anyhow::{Context, Error, Result, ensure};
use blob_stream_blob_store::{BlobKey, ByteRange};
use blob_stream_metadata_store::SegmentMetadata;
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  CommittedSourceCheckpoint,
  CompressionCodec,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
  Window,
  format_unix_timestamp_ms,
  now_unix_seconds as system_now_unix_seconds,
};
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt, stream};
use log::{debug, info, trace};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

mod decode;
mod execution;
mod planning;

const MAX_RECOVERY_WINDOWS_PER_SCAN: usize = 32;

//
// ScanRequest
//

/// One metadata-window request, with all reader modes that require its results.
///
/// A single window can simultaneously serve fresh partitions, retained-history recovery, and the
/// live fast path. `insert_scan_request` merges those independent demands into one metadata query,
/// so this uses fixed flags rather than a single mode or an allocating mode set.
#[derive(Clone)]
pub(in crate::consumer) struct ScanRequest {
  pub(in crate::consumer) window: TopicWindowKey,
  pub(in crate::consumer) min_snowflake: Option<SnowflakeId>,
  pub(in crate::consumer) recovery_scan: bool,
  pub(in crate::consumer) eligibility: ScanEligibility,
}

//
// ScanEligibility
//

/// Reader modes eligible to consume a shared metadata-window request.
///
/// These flags are intentionally not mutually exclusive: recovery can overlap the fast horizon,
/// and a newly assigned partition can share a window with either. They avoid duplicate metadata
/// queries while each partition still applies its own cursor and mode filtering.
#[derive(Clone)]
pub(in crate::consumer) struct ScanEligibility {
  pub(in crate::consumer) recovering_partitions: Vec<VirtualPartitionId>,
  pub(in crate::consumer) fast: bool,
  pub(in crate::consumer) fresh: bool,
}

//
// BatchReadCandidate
//

/// One ordered, capacity-reserved batch contained in a segment read plan.
pub(in crate::consumer) struct BatchReadCandidate {
  pub(in crate::consumer) batch_metadata: BatchMetadata,
  pub(in crate::consumer) virtual_partition_id: VirtualPartitionId,
}

//
// BatchReadResult
//

/// Result of reading one selected batch from a segment.
pub(in crate::consumer) enum BatchReadResult {
  /// A selected batch was fetched and decoded successfully.
  Decoded {
    candidate: BatchReadCandidate,
    batch: ConsumerBatch,
  },
  /// The segment object was conclusively missing and its batch is lost.
  Missing {
    candidate: BatchReadCandidate,
    blob_key: BlobKey,
  },
}

//
// SegmentReadPlan
//

/// One consolidated segment range read and the ordered batches it supplies.
pub(in crate::consumer) struct SegmentReadPlan {
  pub(in crate::consumer) metadata: SegmentMetadata,
  pub(in crate::consumer) candidates: Vec<BatchReadCandidate>,
  pub(in crate::consumer) byte_range: ByteRange,
}

impl SegmentReadPlan {
  /// Create the smallest range containing every selected batch in this segment.
  fn new(metadata: SegmentMetadata, candidates: Vec<BatchReadCandidate>) -> Result<Self> {
    ensure!(
      !candidates.is_empty(),
      "segment read plan for {} has no batch candidates",
      metadata.blob_key.as_str()
    );

    let mut start = u64::MAX;
    let mut end = 0;
    for candidate in &candidates {
      let range = &candidate.batch_metadata.byte_range;
      ensure!(
        !range.is_empty(),
        "segment {} has an empty batch range for partition {}",
        metadata.blob_key.as_str(),
        candidate.virtual_partition_id
      );
      start = start.min(range.start);
      end = end.max(range.end);
    }

    Ok(Self {
      metadata,
      candidates,
      byte_range: ByteRange { start, end },
    })
  }
}
