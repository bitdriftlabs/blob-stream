#[cfg(test)]
#[path = "./memory_test.rs"]
mod tests;

use crate::{MetadataStore, SegmentMetadata};
use anyhow::Result;
use async_trait::async_trait;
use blob_stream_types::{SnowflakeId, TopicWindowKey};
use log::trace;
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
    trace!(
      "metadata(memory) write_segment: topic={}, window_start={}, snowflake_id={}",
      metadata.window.topic,
      metadata.window.window_start_unix_seconds,
      metadata.snowflake_id.as_u64()
    );
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
    trace!(
      "metadata(memory) scan_window: topic={}, window_start={}, min_snowflake={:?}",
      window.topic,
      window.window_start_unix_seconds,
      min_snowflake_id.map(SnowflakeId::as_u64)
    );
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
