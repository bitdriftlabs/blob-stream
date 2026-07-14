//! Shared domain and wire/storage helper types for `blob-stream`.

#[cfg(test)]
#[path = "./types_test.rs"]
mod tests;

use bd_time::{OffsetDateTimeExt, SystemTimeProvider, TimeProvider};
pub use blob_stream_blob_store::ByteRange;
pub use blob_stream_proto::protos::blobstream::v1::broker::Record;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// Identifier of a virtual partition (`logical_partition + writer_offset`).
pub type VirtualPartitionId = u32;

#[must_use]
/// Return current Unix time in milliseconds.
pub fn now_unix_millis() -> i64 {
  SystemTimeProvider.now().unix_timestamp_ms()
}

#[must_use]
/// Return current Unix time in seconds.
pub fn now_unix_seconds() -> i64 {
  SystemTimeProvider.now().unix_timestamp()
}

#[must_use]
/// Format a Unix timestamp in milliseconds as an RFC 3339 UTC timestamp for diagnostics.
pub fn format_unix_timestamp_ms(timestamp_ms: i64) -> String {
  let Some(timestamp_ns) = i128::from(timestamp_ms).checked_mul(1_000_000) else {
    return format!("invalid Unix timestamp: {timestamp_ms} ms");
  };
  let Ok(timestamp) = OffsetDateTime::from_unix_timestamp_nanos(timestamp_ns) else {
    return format!("invalid Unix timestamp: {timestamp_ms} ms");
  };
  timestamp
    .format(&Rfc3339)
    .unwrap_or_else(|_| format!("invalid Unix timestamp: {timestamp_ms} ms"))
}

#[must_use]
/// Compute logical partition from a record key and partition count.
pub fn logical_partition_for_key(record_key: &[u8], partition_count: u32) -> u32 {
  let mut hasher = DefaultHasher::new();
  record_key.hash(&mut hasher);
  let logical_partition = hasher.finish() % u64::from(partition_count);
  u32::try_from(logical_partition).map_or(0, |partition_id| partition_id)
}

#[must_use]
/// Map a logical partition id to virtual partition id using writer id.
pub fn virtual_partition_for_logical(
  logical_partition_id: u32,
  partition_count: u32,
  writer_id: u32,
) -> VirtualPartitionId {
  logical_partition_id.saturating_add(writer_id.saturating_mul(partition_count))
}

#[must_use]
/// Compute virtual partition directly from record key.
pub fn virtual_partition_for_key(
  record_key: &[u8],
  partition_count: u32,
  writer_id: u32,
) -> VirtualPartitionId {
  let logical_partition_id = logical_partition_for_key(record_key, partition_count);
  virtual_partition_for_logical(logical_partition_id, partition_count, writer_id)
}

#[must_use]
/// Build a protobuf-backed `Record` from owned payload bytes.
pub fn new_record(payload: Vec<u8>, event_ts_ms: i64) -> Record {
  Record {
    payload: payload.into(),
    event_ts_ms,
    ..Default::default()
  }
}

//
// RecordBatch
//

#[derive(Clone, Debug, PartialEq)]
/// Batch of records for a single virtual partition.
pub struct RecordBatch {
  /// Owning virtual partition id.
  pub virtual_partition_id: VirtualPartitionId,
  /// Batch records.
  pub records: Vec<Record>,
}

impl RecordBatch {
  /// Build a `RecordBatch`.
  #[must_use]
  pub fn new(virtual_partition_id: VirtualPartitionId, records: Vec<Record>) -> Self {
    Self {
      virtual_partition_id,
      records,
    }
  }

  #[must_use]
  /// Summarize record count, payload bytes, and event-time bounds for this batch.
  pub fn summary(&self) -> Option<BatchSummary> {
    Self::summary_from_records(&self.records)
  }

  #[must_use]
  /// Summarize record count, payload bytes, and event-time bounds for a record slice.
  pub fn summary_from_records(records: &[Record]) -> Option<BatchSummary> {
    let mut iter = records.iter();
    let first = iter.next()?;
    let mut min_event_ts_ms = first.event_ts_ms;
    let mut max_event_ts_ms = first.event_ts_ms;
    let mut payload_bytes = first.payload.len() as u64;

    for record in iter {
      min_event_ts_ms = min_event_ts_ms.min(record.event_ts_ms);
      max_event_ts_ms = max_event_ts_ms.max(record.event_ts_ms);
      payload_bytes += record.payload.len() as u64;
    }

    let record_count = u32::try_from(records.len()).ok()?;

    Some(BatchSummary {
      record_count,
      payload_bytes,
      min_event_ts_ms,
      max_event_ts_ms,
    })
  }
}

//
// BatchSummary
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Aggregate statistics for a `RecordBatch`.
pub struct BatchSummary {
  /// Number of records in the batch.
  pub record_count: u32,
  /// Total payload bytes across all records.
  pub payload_bytes: u64,
  /// Minimum event timestamp in milliseconds.
  pub min_event_ts_ms: i64,
  /// Maximum event timestamp in milliseconds.
  pub max_event_ts_ms: i64,
}

//
// SeqRange
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Inclusive sequence range.
pub struct SeqRange {
  /// Inclusive start sequence.
  pub start: u64,
  /// Inclusive end sequence.
  pub end: u64,
}

impl SeqRange {
  #[must_use]
  /// Number of sequence values in the range.
  pub fn len(&self) -> u64 {
    self.end.saturating_sub(self.start).saturating_add(1)
  }

  #[must_use]
  /// Whether the range is empty (`end < start`).
  pub fn is_empty(&self) -> bool {
    self.end < self.start
  }
}

//
// CommittedCursor
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Committed cursor for one virtual partition.
pub struct CommittedCursor {
  /// Virtual partition id.
  pub virtual_partition_id: VirtualPartitionId,
  /// Highest fully processed sequence.
  pub seq_end: u64,
}

//
// CompressionCodec
//

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Compression codec enum for stored batches.
pub enum CompressionCodec {
  None,
  Zstd,
}

//
// Compression
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Compression settings for a stored batch.
pub struct Compression {
  /// Compression codec.
  pub codec: CompressionCodec,
  /// Optional codec level.
  pub level: Option<i32>,
}

impl Compression {
  #[must_use]
  /// Compression disabled.
  pub fn none() -> Self {
    Self {
      codec: CompressionCodec::None,
      level: None,
    }
  }

  #[must_use]
  /// Zstd compression with explicit level.
  pub fn zstd(level: i32) -> Self {
    Self {
      codec: CompressionCodec::Zstd,
      level: Some(level),
    }
  }
}

//
// BatchMetadata
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Metadata describing a batch location inside a segment blob.
pub struct BatchMetadata {
  /// Sequence interval covered by the batch.
  pub seq_range: SeqRange,
  /// Byte range in the segment blob.
  pub byte_range: ByteRange,
  /// Batch-level summary fields.
  pub summary: BatchSummary,
  /// Stored compression settings.
  pub compression: Compression,
}

//
// Window
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Fixed-size time window.
pub struct Window {
  /// Window start Unix timestamp in seconds.
  pub start_unix_seconds: i64,
  /// Window size in seconds.
  pub size_seconds: i64,
}

impl Window {
  #[must_use]
  /// Compute the aligned window for a timestamp.
  pub fn for_timestamp(unix_seconds: i64, size_seconds: i64) -> Self {
    let start_unix_seconds = unix_seconds.div_euclid(size_seconds) * size_seconds;
    Self {
      start_unix_seconds,
      size_seconds,
    }
  }

  #[must_use]
  /// Build a topic-window key using this window start.
  pub fn key(self, topic: impl Into<String>) -> TopicWindowKey {
    TopicWindowKey {
      topic: topic.into(),
      window_start_unix_seconds: self.start_unix_seconds,
    }
  }
}

//
// TopicWindowKey
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Compound key for topic + window start.
pub struct TopicWindowKey {
  /// Topic name.
  pub topic: String,
  /// Window start Unix timestamp in seconds.
  pub window_start_unix_seconds: i64,
}

impl TopicWindowKey {
  #[must_use]
  /// Format as `"{topic}#{window_start_unix_seconds}"`.
  pub fn format(&self) -> String {
    format!("{}#{}", self.topic, self.window_start_unix_seconds)
  }
}

//
// SnowflakeId
//

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// Monotonic sortable id used for segment ordering.
pub struct SnowflakeId(pub u64);

impl SnowflakeId {
  #[must_use]
  /// Access raw numeric id.
  pub fn as_u64(self) -> u64 {
    self.0
  }

  #[must_use]
  /// Lexicographically sortable fixed-width decimal representation.
  pub fn format_lex(self) -> String {
    format!("{:020}", self.0)
  }
}
