//! Shared domain and wire/storage helper types for `blob-stream`.

#[cfg(test)]
#[path = "./types_test.rs"]
mod tests;

use bd_time::{OffsetDateTimeExt, SystemTimeProvider, TimeProvider};
pub use bd_time::{ProtoDurationExt, ToProtoDuration};
pub use blob_stream_blob_store::ByteRange;
pub use blob_stream_proto::protos::blobstream::v1::broker::Record;
use blob_stream_proto::protos::blobstream::v1::config::TopicConfig;
use bytes::Bytes;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::TryFromIntError;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

/// Identifier of a virtual partition (`logical_partition + writer_offset`).
pub type VirtualPartitionId = u32;

/// Default duration of a metadata window used for segment keys and consumer scans.
pub const DEFAULT_METADATA_WINDOW_SIZE: Duration = Duration::minutes(5);

/// Error returned when a topic cannot produce stable metadata window keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicMetadataWindowError {
  NonPositive,
  SubSecond,
}

impl std::fmt::Display for TopicMetadataWindowError {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::NonPositive => formatter.write_str("topic metadata_window_size must be positive"),
      Self::SubSecond => {
        formatter.write_str("topic metadata_window_size must be a whole-second duration")
      },
    }
  }
}

impl std::error::Error for TopicMetadataWindowError {}

/// Resolve the fixed metadata-key window for a topic.
pub fn topic_metadata_window_size(
  config: &TopicConfig,
) -> Result<Duration, TopicMetadataWindowError> {
  let metadata_window_size = config.metadata_window_size.as_ref().map_or(
    DEFAULT_METADATA_WINDOW_SIZE,
    ProtoDurationExt::to_time_duration,
  );
  if !metadata_window_size.is_positive() {
    return Err(TopicMetadataWindowError::NonPositive);
  }
  if metadata_window_size.subsec_nanoseconds() != 0 {
    return Err(TopicMetadataWindowError::SubSecond);
  }
  Ok(metadata_window_size)
}

/// Default maximum elapsed time for segment construction and durable metadata publication.
///
/// Brokers enforce this value for topic configurations that leave the deadline unset. Consumers
/// use the same value when deriving their metadata availability horizon.
pub const DEFAULT_MAX_METADATA_PUBLICATION_LAG: Duration = Duration::seconds(15);

/// Maximum decoded bytes accepted for one broker produce RPC.
pub const MAX_PRODUCE_BATCHES_REQUEST_BYTES: usize = 16 * 1024 * 1024;

//
// Serde Helpers
//

/// Serialize any displayable string-like value as a JSON string without requiring `Serialize`.
pub fn serialize_as_string<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
  S: Serializer,
  T: std::fmt::Display + ?Sized,
{
  serializer.collect_str(value)
}

/// Serialize an optional displayable string-like value as a JSON string or null.
pub fn serialize_optional_as_string<S, T>(
  value: &Option<T>,
  serializer: S,
) -> Result<S::Ok, S::Error>
where
  S: Serializer,
  T: std::fmt::Display,
{
  match value {
    Some(value) => serializer.collect_str(value),
    None => serializer.serialize_none(),
  }
}

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
/// Convert a persisted Unix millisecond timestamp into an instant.
pub fn offset_datetime_from_unix_millis(timestamp_ms: i64) -> OffsetDateTime {
  OffsetDateTime::UNIX_EPOCH.saturating_add(Duration::milliseconds(timestamp_ms))
}

/// Strictly convert a persisted Unix millisecond timestamp into an instant.
pub fn offset_datetime_from_unix_millis_checked(
  timestamp_ms: i64,
) -> Result<OffsetDateTime, time::error::ComponentRange> {
  OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000)
}

/// Strictly convert an instant to a Unix millisecond timestamp for persistence.
pub fn unix_millis_from_offset_datetime(timestamp: OffsetDateTime) -> Result<i64, TryFromIntError> {
  i64::try_from(timestamp.unix_timestamp_nanos().div_euclid(1_000_000))
}

#[must_use]
/// Convert a Unix second timestamp into an instant.
pub fn offset_datetime_from_unix_seconds(timestamp_seconds: i64) -> OffsetDateTime {
  OffsetDateTime::UNIX_EPOCH.saturating_add(Duration::seconds(timestamp_seconds))
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
pub fn new_record(payload: impl Into<Bytes>, event_ts_ms: i64) -> Record {
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
  /// Summarize record count and payload bytes for this batch.
  pub fn summary(&self) -> Option<BatchSummary> {
    Self::summary_from_records(&self.records)
  }

  #[must_use]
  /// Summarize record count and payload bytes for a record slice.
  pub fn summary_from_records(records: &[Record]) -> Option<BatchSummary> {
    let mut iter = records.iter();
    let first = iter.next()?;
    let mut payload_bytes = first.payload.len() as u64;

    for record in iter {
      payload_bytes += record.payload.len() as u64;
    }

    let record_count = u32::try_from(records.len()).ok()?;

    Some(BatchSummary {
      record_count,
      payload_bytes,
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
// CommittedSourceCheckpoint
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Metadata source that produced a committed consumer cursor.
pub struct CommittedSourceCheckpoint {
  /// Window containing the segment metadata row.
  pub window_start_unix_seconds: i64,
  /// Segment metadata row identifier within the window.
  pub snowflake_id: u64,
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
  /// Metadata source checkpoint for retention-wide recovery.
  #[serde(default)]
  pub source_checkpoint: Option<CommittedSourceCheckpoint>,
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
  /// Total payload bytes across all records in this batch.
  pub payload_bytes: u64,
}

//
// Window
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Fixed-size time window.
pub struct Window {
  /// Window start instant.
  pub start: OffsetDateTime,
  /// Window duration.
  pub size: Duration,
}

impl Window {
  #[must_use]
  /// Compute the aligned window for a timestamp.
  pub fn for_timestamp(timestamp: OffsetDateTime, size: Duration) -> Self {
    debug_assert!(
      size.is_positive() && size.subsec_nanoseconds() == 0,
      "durable topic window keys require positive whole-second sizes"
    );
    let size_nanoseconds = size.whole_nanoseconds();
    let start_nanoseconds = timestamp
      .unix_timestamp_nanos()
      .div_euclid(size_nanoseconds)
      * size_nanoseconds;
    Self {
      start: OffsetDateTime::from_unix_timestamp_nanos(start_nanoseconds)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH),
      size,
    }
  }

  #[must_use]
  /// Build a topic-window key using this window start.
  pub fn key(self, topic: impl Into<String>) -> TopicWindowKey {
    TopicWindowKey {
      topic: topic.into(),
      window_start_unix_seconds: self.start.unix_timestamp(),
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
  /// Return the lowest Sonyflake ID that can be generated at or after `timestamp`.
  pub fn minimum_for_timestamp(timestamp: OffsetDateTime) -> Self {
    Self(sonyflake::minimum_for_timestamp(timestamp))
  }

  #[must_use]
  /// Return the default-epoch Sonyflake timestamp encoded in this ID.
  pub fn timestamp(self) -> Option<OffsetDateTime> {
    let elapsed_nanoseconds = sonyflake::decompose(self.0).nanos_time();
    datetime!(2014-09-01 00:00 UTC).checked_add(Duration::nanoseconds(elapsed_nanoseconds))
  }

  #[must_use]
  /// Lexicographically sortable fixed-width decimal representation.
  pub fn format_lex(self) -> String {
    format!("{:020}", self.0)
  }
}
