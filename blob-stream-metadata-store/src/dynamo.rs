use crate::{MetadataStore, SegmentMetadata};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{
  BatchMetadata,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
  format_unix_timestamp_ms,
};
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;

#[cfg(test)]
#[path = "./dynamo_test.rs"]
mod tests;

const ATTR_PK: &str = "pk";
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;
const DEFAULT_SEGMENT_TTL_BUFFER_SECONDS: u32 = 3_600;

//
// DynamoMetadataStore
//

#[derive(Clone, Debug)]
pub struct DynamoMetadataStore {
  client: Client,
  table_name: String,
  topic_retention_days: HashMap<String, u32>,
  ttl_buffer_seconds: i64,
}

impl DynamoMetadataStore {
  #[must_use]
  pub fn new(client: Client, table_name: impl Into<String>) -> Self {
    Self::with_segment_ttl(
      client,
      table_name,
      HashMap::new(),
      DEFAULT_SEGMENT_TTL_BUFFER_SECONDS,
    )
  }

  #[must_use]
  pub fn with_segment_ttl(
    client: Client,
    table_name: impl Into<String>,
    topic_retention_days: HashMap<String, u32>,
    ttl_buffer_seconds: u32,
  ) -> Self {
    Self {
      client,
      table_name: table_name.into(),
      topic_retention_days,
      ttl_buffer_seconds: i64::from(ttl_buffer_seconds),
    }
  }

  fn metadata_ttl_epoch_seconds(&self, metadata: &SegmentMetadata) -> Option<i64> {
    let retention_days = self.topic_retention_days.get(&metadata.window.topic)?;
    if *retention_days == 0 {
      return None;
    }

    let retention_seconds = i64::from(*retention_days).checked_mul(SECONDS_PER_DAY)?;
    let created_seconds = metadata.created_ts_ms.checked_div(1_000)?;

    created_seconds
      .checked_add(retention_seconds)?
      .checked_add(self.ttl_buffer_seconds)
  }
}

#[async_trait]
impl MetadataStore for DynamoMetadataStore {
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()> {
    trace!(
      "metadata(dynamo) write_segment start: table={}, topic={}, window_start={}, snowflake_id={}",
      self.table_name,
      metadata.window.topic,
      format_unix_timestamp_ms(
        metadata
          .window
          .window_start_unix_seconds
          .saturating_mul(1_000)
      ),
      metadata.snowflake_id.as_u64()
    );
    let ttl_epoch_seconds = self.metadata_ttl_epoch_seconds(&metadata);
    let item = DynamoSegmentItem::from_metadata(metadata, ttl_epoch_seconds);
    let item = serde_dynamo::to_item(item)?;

    self
      .client
      .put_item()
      .table_name(&self.table_name)
      .set_item(Some(item))
      .send()
      .await?;

    debug!(
      "metadata(dynamo) write_segment complete: table={}",
      self.table_name
    );

    Ok(())
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>> {
    trace!(
      "metadata(dynamo) scan_window start: table={}, topic={}, window_start={}, min_snowflake={:?}",
      self.table_name,
      window.topic,
      format_unix_timestamp_ms(window.window_start_unix_seconds.saturating_mul(1_000)),
      min_snowflake.map(SnowflakeId::as_u64)
    );

    let mut segments = Vec::new();
    let mut start_key = None;
    loop {
      let mut values = HashMap::new();
      values.insert(":pk".to_string(), AttributeValue::S(window.format()));
      if let Some(min_snowflake) = min_snowflake {
        values.insert(
          ":min_snowflake".to_string(),
          AttributeValue::S(min_snowflake.format_lex()),
        );
      }

      let key_condition = if min_snowflake.is_some() {
        format!("{ATTR_PK} = :pk AND sk >= :min_snowflake")
      } else {
        format!("{ATTR_PK} = :pk")
      };
      let mut query = self
        .client
        .query()
        .table_name(&self.table_name)
        .key_condition_expression(key_condition)
        .set_expression_attribute_values(Some(values))
        .consistent_read(false);
      if let Some(key) = start_key.take() {
        query = query.set_exclusive_start_key(Some(key));
      }

      let response = query.send().await?;
      segments.extend(
        response
          .items
          .unwrap_or_default()
          .into_iter()
          .map(|item| {
            let entry: DynamoSegmentItem = serde_dynamo::from_item(item)?;
            SegmentMetadata::try_from(entry)
          })
          .collect::<Result<Vec<_>>>()?,
      );

      let Some(key) = response.last_evaluated_key else {
        break;
      };
      start_key = Some(key);
    }

    debug!(
      "metadata(dynamo) scan_window complete: table={}, segments={}",
      self.table_name,
      segments.len()
    );
    Ok(segments)
  }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DynamoSegmentItem {
  #[serde(rename = "pk")]
  partition_key: String,
  #[serde(rename = "sk")]
  sort_key: String,
  blob_key: String,
  segment_index: HashMap<String, Vec<BatchMetadata>>,
  created_ts_ms: i64,
  #[serde(skip_serializing_if = "Option::is_none")]
  metadata_published_ts_ms: Option<i64>,
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(rename = "ttl_epoch_seconds")]
  ttl_epoch_seconds: Option<i64>,
}

impl DynamoSegmentItem {
  fn from_metadata(metadata: SegmentMetadata, ttl_epoch_seconds: Option<i64>) -> Self {
    let partition_key = metadata.partition_key();
    let sort_key = metadata.snowflake_key();
    let blob_key = metadata.blob_key.as_str().to_string();
    let created_ts_ms = metadata.created_ts_ms;
    let metadata_published_ts_ms = Some(metadata.metadata_published_ts_ms);

    let segment_index = metadata
      .segment_index
      .into_iter()
      .map(|(virtual_partition_id, batches)| (virtual_partition_id.to_string(), batches))
      .collect();

    Self {
      partition_key,
      sort_key,
      blob_key,
      segment_index,
      created_ts_ms,
      metadata_published_ts_ms,
      ttl_epoch_seconds,
    }
  }
}

impl TryFrom<DynamoSegmentItem> for SegmentMetadata {
  type Error = anyhow::Error;

  fn try_from(item: DynamoSegmentItem) -> Result<Self> {
    let segment_index = item
      .segment_index
      .into_iter()
      .map(|(virtual_partition_id, batches)| {
        let virtual_partition_id =
          virtual_partition_id
            .parse::<VirtualPartitionId>()
            .map_err(|error| {
              anyhow!("invalid virtual_partition_id {virtual_partition_id}: {error}")
            })?;
        Ok((virtual_partition_id, batches))
      })
      .collect::<Result<HashMap<_, _>>>()?;

    let (topic, window_start) = item
      .partition_key
      .rsplit_once('#')
      .ok_or_else(|| anyhow!("invalid metadata partition key {}", item.partition_key))?;
    let window_start_unix_seconds = window_start
      .parse::<i64>()
      .map_err(|error| anyhow!("invalid metadata window start {window_start}: {error}"))?;
    let sort_key = item.sort_key;

    Ok(Self {
      window: TopicWindowKey {
        topic: topic.to_string(),
        window_start_unix_seconds,
      },
      snowflake_id: SnowflakeId(
        sort_key
          .parse::<u64>()
          .map_err(|error| anyhow!("invalid snowflake id {sort_key}: {error}"))?,
      ),
      blob_key: BlobKey::from(item.blob_key),
      segment_index,
      created_ts_ms: item.created_ts_ms,
      metadata_published_ts_ms: item.metadata_published_ts_ms.unwrap_or(item.created_ts_ms),
    })
  }
}
