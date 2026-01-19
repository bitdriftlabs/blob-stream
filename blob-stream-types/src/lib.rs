// blob-stream - shared data model and types
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./types_test.rs"]
mod tests;

use serde::{Deserialize, Serialize};

pub use blob_stream_blob_store::ByteRange;

pub type VirtualPartitionId = u32;

//
// Record
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
  pub payload: Vec<u8>,
  pub event_ts_ms: i64,
}

impl Record {
  #[must_use]
  pub fn new(payload: Vec<u8>, event_ts_ms: i64) -> Self {
    Self {
      payload,
      event_ts_ms,
    }
  }
}

//
// RecordBatch
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordBatch {
  pub virtual_partition_id: VirtualPartitionId,
  pub records: Vec<Record>,
}

impl RecordBatch {
  #[must_use]
  pub fn new(virtual_partition_id: VirtualPartitionId, records: Vec<Record>) -> Self {
    Self {
      virtual_partition_id,
      records,
    }
  }

  #[must_use]
  pub fn summary(&self) -> Option<BatchSummary> {
    let mut iter = self.records.iter();
    let first = iter.next()?;
    let mut min_event_ts_ms = first.event_ts_ms;
    let mut max_event_ts_ms = first.event_ts_ms;
    let mut payload_bytes = first.payload.len() as u64;

    for record in iter {
      min_event_ts_ms = min_event_ts_ms.min(record.event_ts_ms);
      max_event_ts_ms = max_event_ts_ms.max(record.event_ts_ms);
      payload_bytes += record.payload.len() as u64;
    }

    let record_count = u32::try_from(self.records.len()).ok()?;

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
pub struct BatchSummary {
  pub record_count: u32,
  pub payload_bytes: u64,
  pub min_event_ts_ms: i64,
  pub max_event_ts_ms: i64,
}

//
// SeqRange
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeqRange {
  pub start: u64,
  pub end: u64,
}

impl SeqRange {
  #[must_use]
  pub fn len(&self) -> u64 {
    self.end.saturating_sub(self.start).saturating_add(1)
  }

  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.end < self.start
  }
}

//
// CommittedCursor
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedCursor {
  pub virtual_partition_id: VirtualPartitionId,
  pub seq_end: u64,
}

//
// CompressionCodec
//

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionCodec {
  None,
  Zstd,
}

//
// Compression
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Compression {
  pub codec: CompressionCodec,
  pub level: Option<i32>,
}

impl Compression {
  #[must_use]
  pub fn none() -> Self {
    Self {
      codec: CompressionCodec::None,
      level: None,
    }
  }

  #[must_use]
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
pub struct BatchMetadata {
  pub seq_range: SeqRange,
  pub byte_range: ByteRange,
  pub summary: BatchSummary,
  pub compression: Compression,
}

//
// Window
//

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
  pub start_unix_seconds: i64,
  pub size_seconds: i64,
}

impl Window {
  #[must_use]
  pub fn for_timestamp(unix_seconds: i64, size_seconds: i64) -> Self {
    let start_unix_seconds = unix_seconds.div_euclid(size_seconds) * size_seconds;
    Self {
      start_unix_seconds,
      size_seconds,
    }
  }

  #[must_use]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicWindowKey {
  pub topic: String,
  pub window_start_unix_seconds: i64,
}

impl TopicWindowKey {
  #[must_use]
  pub fn format(&self) -> String {
    format!("{}#{}", self.topic, self.window_start_unix_seconds)
  }
}

//
// SnowflakeId
//

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SnowflakeId(pub u64);

impl SnowflakeId {
  #[must_use]
  pub fn as_u64(self) -> u64 {
    self.0
  }

  #[must_use]
  pub fn format_lex(self) -> String {
    format!("{:020}", self.0)
  }
}
