// blob-stream - in-memory metadata store
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./memory_test.rs"]
mod tests;

use crate::{MetadataStore, SegmentMetadata};
use anyhow::Result;
use async_trait::async_trait;
use blob_stream_types::{SnowflakeId, TopicWindowKey};
use std::collections::HashMap;
use tokio::sync::RwLock;

//
// InMemoryMetadataStore
//

#[derive(Debug, Default)]
pub struct InMemoryMetadataStore {
  windows: RwLock<HashMap<String, Vec<SegmentMetadata>>>,
}

impl InMemoryMetadataStore {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }
}

#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()> {
    let mut guard = self.windows.write().await;
    let key = metadata.partition_key();
    guard.entry(key).or_default().push(metadata);
    Ok(())
  }

  async fn scan_window(
    &self,
    window: &TopicWindowKey,
    min_snowflake_id: Option<SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>> {
    let guard = self.windows.read().await;
    let Some(segments) = guard.get(&window.format()) else {
      return Ok(Vec::new());
    };

    let filtered = segments
      .iter()
      .filter(|segment| min_snowflake_id.is_none_or(|min| segment.snowflake_id >= min))
      .cloned()
      .collect();

    Ok(filtered)
  }
}
