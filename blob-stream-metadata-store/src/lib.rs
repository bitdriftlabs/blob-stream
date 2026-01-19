// blob-stream - segment metadata storage abstraction
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./metadata_store_test.rs"]
mod tests;

use anyhow::Result;
use async_trait::async_trait;
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{
  BatchMetadata,
  Compression,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
use std::collections::HashMap;

mod dynamo;
mod memory;

pub use dynamo::DynamoMetadataStore;
pub use memory::InMemoryMetadataStore;

//
// SegmentMetadata
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentMetadata {
  pub window: TopicWindowKey,
  pub snowflake_id: SnowflakeId,
  pub blob_key: BlobKey,
  pub segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
  pub compression: Compression,
  pub record_count: u64,
  pub min_event_ts_ms: i64,
  pub max_event_ts_ms: i64,
  pub checksum: Option<String>,
  pub created_ts_ms: i64,
}

impl SegmentMetadata {
  #[must_use]
  pub fn new(
    window: TopicWindowKey,
    snowflake_id: SnowflakeId,
    blob_key: BlobKey,
    segment_index: HashMap<VirtualPartitionId, Vec<BatchMetadata>>,
    compression: Compression,
    record_count: u64,
    min_event_ts_ms: i64,
    max_event_ts_ms: i64,
    checksum: Option<String>,
    created_ts_ms: i64,
  ) -> Self {
    Self {
      window,
      snowflake_id,
      blob_key,
      segment_index,
      compression,
      record_count,
      min_event_ts_ms,
      max_event_ts_ms,
      checksum,
      created_ts_ms,
    }
  }

  #[must_use]
  pub fn partition_key(&self) -> String {
    self.window.format()
  }

  #[must_use]
  pub fn snowflake_key(&self) -> String {
    self.snowflake_id.format_lex()
  }
}

//
// MetadataStore
//

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait MetadataStore: Send + Sync {
  /// Persist metadata for a flushed segment.
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()>;

  /// Scan a single window for segments, optionally starting at a snowflake lower bound.
  ///
  /// Results are unordered for cost and performance.
  async fn scan_window(
    &self,
    window: &TopicWindowKey,
    min_snowflake_id: Option<SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>>;
}
