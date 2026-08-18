#[cfg(test)]
#[path = "./memory_test.rs"]
mod tests;

use crate::codec::EncodedSegmentMetadata;
use crate::{
  MetadataReadConsistency,
  MetadataStore,
  MetadataWriteResult,
  ProducerPartitionFence,
  SegmentMetadata,
};
use anyhow::Result;
use async_trait::async_trait;
use bd_log_util::warn_every;
use blob_stream_types::{SnowflakeId, TopicWindowKey, offset_datetime_from_unix_seconds};
use log::trace;
use parking_lot::RwLock;
use std::collections::HashMap;
use time::ext::NumericalDuration;

//
// InMemoryMetadataStore
//

#[derive(Debug, Default)]
pub struct InMemoryMetadataStore {
  windows: RwLock<HashMap<String, Vec<EncodedSegmentMetadata>>>,
}

impl InMemoryMetadataStore {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }
}

#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    _now_ts_ms: i64,
  ) -> MetadataWriteResult {
    if fences.is_some() {
      return Err(
        anyhow::anyhow!("fenced metadata writes require the DynamoDB metadata store").into(),
      );
    }
    trace!(
      "metadata(memory) write_segment: topic={}, window_start={}, snowflake_id={}",
      metadata.window.topic,
      offset_datetime_from_unix_seconds(metadata.window.window_start_unix_seconds),
      metadata.snowflake_id.as_u64()
    );
    let encoded = crate::codec::encode(&metadata)?;
    let mut guard = self.windows.write();
    guard
      .entry(encoded.partition_key.clone())
      .or_default()
      .push(encoded);
    Ok(())
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    _consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    trace!(
      "metadata(memory) scan_window: topic={}, window_start={}, min_snowflake={:?}",
      window.topic,
      offset_datetime_from_unix_seconds(window.window_start_unix_seconds),
      min_snowflake.map(SnowflakeId::as_u64)
    );
    let guard = self.windows.read();
    let Some(segments) = guard.get(&window.format()) else {
      return Ok(Vec::new());
    };

    let min_sort_key = min_snowflake.map(SnowflakeId::format_lex);
    Ok(
      segments
        .iter()
        .filter(|segment| {
          min_sort_key
            .as_ref()
            .is_none_or(|min_sort_key| segment.sort_key >= *min_sort_key)
        })
        .filter_map(|segment| {
          match crate::codec::decode(&segment.partition_key, &segment.sort_key, &segment.payload) {
            Ok(metadata) => Some(metadata),
            Err(error) => {
              warn_every!(
                15.seconds(),
                "metadata(memory) skipped noncompliant segment: {error}"
              );
              None
            },
          }
        })
        .collect(),
    )
  }
}
