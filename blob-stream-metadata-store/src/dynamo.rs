use crate::{
  DynamoCapacityMetrics,
  MetadataReadConsistency,
  MetadataStore,
  SegmentMetadata,
  codec,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnConsumedCapacity};
use bd_log_util::warn_every;
use blob_stream_types::{SnowflakeId, TopicWindowKey, format_unix_timestamp_ms};
use bytes::Bytes;
use log::{debug, trace};
use protobuf::Chars;
use std::collections::HashMap;
use time::ext::NumericalDuration;

#[cfg(test)]
#[path = "./dynamo_test.rs"]
mod tests;

const ATTR_PK: &str = "pk";
const ATTR_SK: &str = "sk";
const ATTR_SEGMENT_METADATA_V1: &str = "segment_metadata_v1";
const ATTR_TTL_EPOCH_SECONDS: &str = "ttl_epoch_seconds";
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

//
// DynamoMetadataStore
//

#[derive(Clone, Debug)]
pub struct DynamoMetadataStore {
  client: Client,
  table_name: String,
  topic_retention_days: HashMap<Chars, u32>,
  ttl_buffer_seconds: i64,
  capacity_metrics: Option<DynamoCapacityMetrics>,
}

impl DynamoMetadataStore {
  #[must_use]
  pub fn new(
    client: Client,
    table_name: impl Into<String>,
    topic_retention_days: HashMap<Chars, u32>,
    ttl_buffer_seconds: u32,
    capacity_metrics: Option<DynamoCapacityMetrics>,
  ) -> Self {
    Self {
      client,
      table_name: table_name.into(),
      topic_retention_days,
      ttl_buffer_seconds: i64::from(ttl_buffer_seconds),
      capacity_metrics,
    }
  }

  fn record_read_capacity(
    &self,
    consumed_capacity: Option<&aws_sdk_dynamodb::types::ConsumedCapacity>,
  ) {
    if let Some(capacity_metrics) = &self.capacity_metrics {
      capacity_metrics.record_read(consumed_capacity);
    }
  }

  fn record_write_capacity(
    &self,
    consumed_capacity: Option<&aws_sdk_dynamodb::types::ConsumedCapacity>,
  ) {
    if let Some(capacity_metrics) = &self.capacity_metrics {
      capacity_metrics.record_write(consumed_capacity);
    }
  }

  fn metadata_ttl_epoch_seconds(&self, metadata: &SegmentMetadata) -> Option<i64> {
    let retention_days = self
      .topic_retention_days
      .get(metadata.window.topic.as_str())?;
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
    let encoded = codec::encode(metadata)?;
    let mut item = HashMap::from([
      (
        ATTR_PK.to_string(),
        AttributeValue::S(encoded.partition_key),
      ),
      (ATTR_SK.to_string(), AttributeValue::S(encoded.sort_key)),
      (
        ATTR_SEGMENT_METADATA_V1.to_string(),
        AttributeValue::B(Blob::new(encoded.payload)),
      ),
    ]);
    if let Some(ttl_epoch_seconds) = ttl_epoch_seconds {
      item.insert(
        ATTR_TTL_EPOCH_SECONDS.to_string(),
        AttributeValue::N(ttl_epoch_seconds.to_string()),
      );
    }

    let response = self
      .client
      .put_item()
      .table_name(&self.table_name)
      .set_item(Some(item))
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await?;
    self.record_write_capacity(response.consumed_capacity.as_ref());

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
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    trace!(
      "metadata(dynamo) scan_window start: table={}, topic={}, window_start={}, \
       min_snowflake={:?}, consistency={consistency:?}",
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
        .projection_expression(format!("{ATTR_PK}, {ATTR_SK}, {ATTR_SEGMENT_METADATA_V1}"))
        .set_expression_attribute_values(Some(values))
        .consistent_read(matches!(consistency, MetadataReadConsistency::Strong));
      if let Some(key) = start_key.take() {
        query = query.set_exclusive_start_key(Some(key));
      }

      let response = query
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await?;
      self.record_read_capacity(response.consumed_capacity.as_ref());
      for item in response.items.unwrap_or_default() {
        match Self::decode_item(item) {
          Ok(metadata) => segments.push(metadata),
          Err(error) => {
            warn_every!(
              15.seconds(),
              "metadata(dynamo) skipped noncompliant segment: {error}"
            );
          },
        }
      }

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

impl DynamoMetadataStore {
  fn decode_item(mut item: HashMap<String, AttributeValue>) -> Result<SegmentMetadata> {
    let partition_key = Self::take_string_attribute(&mut item, ATTR_PK)?;
    let sort_key = Self::take_string_attribute(&mut item, ATTR_SK)?;
    let payload = item
      .remove(ATTR_SEGMENT_METADATA_V1)
      .ok_or_else(|| anyhow!("metadata row {partition_key}/{sort_key} is missing v1 payload"))?;
    let payload = match payload {
      AttributeValue::B(payload) => Bytes::from(payload.into_inner()),
      _ => {
        return Err(anyhow!(
          "metadata row {partition_key}/{sort_key} has a non-binary v1 payload"
        ));
      },
    };
    codec::decode(&partition_key, &sort_key, &payload)
  }

  fn take_string_attribute(
    item: &mut HashMap<String, AttributeValue>,
    attribute_name: &str,
  ) -> Result<String> {
    match item.remove(attribute_name) {
      Some(AttributeValue::S(value)) => Ok(value),
      Some(_) => Err(anyhow!("metadata row has a non-string {attribute_name}")),
      None => Err(anyhow!("metadata row is missing {attribute_name}")),
    }
  }
}
