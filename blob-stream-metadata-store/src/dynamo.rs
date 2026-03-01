// blob-stream - DynamoDB metadata store
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{MetadataStore, SegmentMetadata};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{
  BatchMetadata,
  Compression,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Write;

#[cfg(test)]
#[path = "./dynamo_test.rs"]
mod tests;

const ATTR_PK: &str = "pk";
const ATTR_SK: &str = "sk";

//
// DynamoMetadataStore
//

#[derive(Clone, Debug)]
pub struct DynamoMetadataStore {
  client: Client,
  table_name: String,
}

impl DynamoMetadataStore {
  #[must_use]
  pub fn new(client: Client, table_name: impl Into<String>) -> Self {
    Self {
      client,
      table_name: table_name.into(),
    }
  }
}

#[async_trait]
impl MetadataStore for DynamoMetadataStore {
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()> {
    trace!(
      "metadata(dynamo) write_segment start: table={}, topic={}, window_start={}, snowflake_id={}",
      self.table_name,
      metadata.window.topic,
      metadata.window.window_start_unix_seconds,
      metadata.snowflake_id.as_u64()
    );
    let item = DynamoSegmentItem::from_metadata(metadata);
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

  async fn scan_window(
    &self,
    window: &TopicWindowKey,
    min_snowflake_id: Option<SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>> {
    trace!(
      "metadata(dynamo) scan_window start: table={}, topic={}, window_start={}, min_snowflake={:?}",
      self.table_name,
      window.topic,
      window.window_start_unix_seconds,
      min_snowflake_id.map(SnowflakeId::as_u64)
    );
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S(window.format()));

    let mut condition = format!("{ATTR_PK} = :pk");
    if let Some(min) = min_snowflake_id {
      write!(&mut condition, " and {ATTR_SK} >= :sk").expect("format condition");
      values.insert(":sk".to_string(), AttributeValue::S(min.format_lex()));
    }

    let response = self
      .client
      .query()
      .table_name(&self.table_name)
      .key_condition_expression(condition)
      .set_expression_attribute_values(Some(values))
      .consistent_read(false)
      .send()
      .await?;

    let output: Result<Vec<SegmentMetadata>> = response
      .items
      .unwrap_or_default()
      .into_iter()
      .map(|item| {
        let entry: DynamoSegmentItem = serde_dynamo::from_item(item)?;
        SegmentMetadata::try_from(entry)
      })
      .collect();

    if let Ok(ref segments) = output {
      debug!(
        "metadata(dynamo) scan_window complete: table={}, segments={}",
        self.table_name,
        segments.len()
      );
    }

    output
  }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DynamoSegmentItem {
  #[serde(rename = "pk")]
  partition_key: String,
  #[serde(rename = "sk")]
  sort_key: String,
  topic: String,
  window_start_ts: i64,
  blob_key: String,
  segment_index: HashMap<String, Vec<BatchMetadata>>,
  compression: Compression,
  record_count: u64,
  min_event_ts_ms: i64,
  max_event_ts_ms: i64,
  checksum: Option<String>,
  created_ts_ms: i64,
}

impl DynamoSegmentItem {
  fn from_metadata(metadata: SegmentMetadata) -> Self {
    let partition_key = metadata.partition_key();
    let sort_key = metadata.snowflake_key();
    let topic = metadata.window.topic;
    let window_start_ts = metadata.window.window_start_unix_seconds;
    let blob_key = metadata.blob_key.as_str().to_string();
    let compression = metadata.compression;
    let record_count = metadata.record_count;
    let min_event_ts_ms = metadata.min_event_ts_ms;
    let max_event_ts_ms = metadata.max_event_ts_ms;
    let checksum = metadata.checksum;
    let created_ts_ms = metadata.created_ts_ms;

    let segment_index = metadata
      .segment_index
      .into_iter()
      .map(|(virtual_partition_id, batches)| (virtual_partition_id.to_string(), batches))
      .collect();

    Self {
      partition_key,
      sort_key,
      topic,
      window_start_ts,
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

    let sort_key = item.sort_key;

    Ok(Self {
      window: TopicWindowKey {
        topic: item.topic,
        window_start_unix_seconds: item.window_start_ts,
      },
      snowflake_id: SnowflakeId(
        sort_key
          .parse::<u64>()
          .map_err(|error| anyhow!("invalid snowflake id {sort_key}: {error}"))?,
      ),
      blob_key: BlobKey::from(item.blob_key),
      segment_index,
      compression: item.compression,
      record_count: item.record_count,
      min_event_ts_ms: item.min_event_ts_ms,
      max_event_ts_ms: item.max_event_ts_ms,
      checksum: item.checksum,
      created_ts_ms: item.created_ts_ms,
    })
  }
}
