use crate::aws::retry_dynamo_transaction_conflicts;
use crate::dynamo_attributes::{
  ATTR_EPOCH,
  ATTR_EXPIRES,
  ATTR_HOLDER,
  ATTR_PK,
  ATTR_SESSION,
  ATTR_SK,
  ATTR_TTL,
};
use crate::{
  DynamoCapacityMetrics,
  MetadataReadConsistency,
  MetadataStore,
  ProducerLeaseFenceLost,
  ProducerPartitionFence,
  SegmentMetadata,
  codec,
};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
  AttributeValue,
  ConditionCheck,
  Put,
  ReturnConsumedCapacity,
  TransactWriteItem,
};
use bd_log_util::warn_every;
use blob_stream_types::{SnowflakeId, TopicWindowKey, format_unix_timestamp_ms};
use bytes::Bytes;
use log::{debug, trace};
use protobuf::Chars;
use std::collections::{HashMap, HashSet};
use time::ext::NumericalDuration;
use uuid::Uuid;

#[cfg(test)]
#[path = "./dynamo_test.rs"]
mod tests;

const ATTR_SEGMENT_METADATA_V1: &str = "segment_metadata_v1";
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;
pub const MAX_FENCED_METADATA_PARTITIONS: usize = 99;

//
// DynamoMetadataStore
//

#[derive(Clone, Debug)]
pub struct DynamoMetadataStore {
  client: Client,
  table_name: String,
  producer_partition_lease_table_name: String,
  topic_retention_days: HashMap<Chars, u32>,
  ttl_buffer_seconds: i64,
  capacity_metrics: Option<DynamoCapacityMetrics>,
}

impl DynamoMetadataStore {
  #[must_use]
  pub fn new(
    client: Client,
    table_name: impl Into<String>,
    producer_partition_lease_table_name: impl Into<String>,
    topic_retention_days: HashMap<Chars, u32>,
    ttl_buffer_seconds: u32,
    capacity_metrics: Option<DynamoCapacityMetrics>,
  ) -> Self {
    Self {
      client,
      table_name: table_name.into(),
      producer_partition_lease_table_name: producer_partition_lease_table_name.into(),
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
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> Result<()> {
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
    if let Some(fences) = fences {
      ensure!(
        !fences.is_empty(),
        "fenced metadata publication requires at least one producer partition fence"
      );
      ensure!(
        fences.len() <= MAX_FENCED_METADATA_PARTITIONS,
        "fenced metadata publication supports at most {MAX_FENCED_METADATA_PARTITIONS} partitions"
      );
      let mut lease_keys = HashSet::with_capacity(fences.len());
      for fence in fences {
        ensure!(
          lease_keys.insert(fence.key.format()),
          "fenced metadata publication has duplicate producer lease key"
        );
      }
      ensure!(
        fences.len() == metadata.segment_index.len()
          && fences.iter().all(|fence| {
            fence.key.topic.as_str() == metadata.window.topic
              && metadata
                .segment_index
                .contains_key(&fence.key.virtual_partition_id)
          }),
        "fenced metadata publication requires producer lease fences matching segment partitions"
      );
    }

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
        ATTR_TTL.to_string(),
        AttributeValue::N(ttl_epoch_seconds.to_string()),
      );
    }

    let Some(fences) = fences else {
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
      return Ok(());
    };

    let mut lease_keys = HashSet::with_capacity(fences.len());
    for fence in fences {
      let key_is_new = lease_keys.insert(fence.key.format());
      debug_assert!(key_is_new, "fence keys were validated before encoding");
    }

    let metadata_put = Put::builder()
      .table_name(&self.table_name)
      .set_item(Some(item))
      .build()?;
    let mut transaction_items = Vec::with_capacity(fences.len() + 1);
    transaction_items.push(TransactWriteItem::builder().put(metadata_put).build());
    for producer_fence in fences {
      let mut values = HashMap::new();
      values.insert(
        ":holder".to_string(),
        AttributeValue::S(producer_fence.fence.holder_id.clone()),
      );
      values.insert(
        ":epoch".to_string(),
        AttributeValue::N(producer_fence.fence.lease_epoch.to_string()),
      );
      values.insert(
        ":session".to_string(),
        AttributeValue::S(producer_fence.fence.lease_session_id.clone()),
      );
      values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
      let lease_check = ConditionCheck::builder()
        .table_name(&self.producer_partition_lease_table_name)
        .key(ATTR_PK, AttributeValue::S(producer_fence.key.format()))
        .condition_expression(format!(
          "{ATTR_HOLDER} = :holder AND {ATTR_EPOCH} = :epoch AND {ATTR_SESSION} = :session AND \
           {ATTR_EXPIRES} > :now"
        ))
        .set_expression_attribute_values(Some(values))
        .build()?;
      transaction_items.push(
        TransactWriteItem::builder()
          .condition_check(lease_check)
          .build(),
      );
    }

    let client_request_token = Uuid::new_v4().to_string();
    let result = retry_dynamo_transaction_conflicts(
      "fenced_metadata_write",
      || {
        self
          .client
          .transact_write_items()
          .client_request_token(client_request_token.clone())
          .set_transact_items(Some(transaction_items.clone()))
          .return_consumed_capacity(ReturnConsumedCapacity::Total)
          .send()
      },
      |error| {
        matches!(
          error,
          SdkError::ServiceError(service_error)
            if transaction_cancellation_has_code(service_error.err(), "TransactionConflict")
        )
      },
    )
    .await;
    match result {
      Ok(output) => {
        for capacity in output.consumed_capacity.as_deref().unwrap_or_default() {
          self.record_write_capacity(Some(capacity));
        }
        debug!(
          "metadata(dynamo) fenced write complete: table={}, partitions={}",
          self.table_name,
          lease_keys.len()
        );
        Ok(())
      },
      Err(SdkError::ServiceError(service_error))
        if transaction_cancellation_has_code(service_error.err(), "ConditionalCheckFailed") =>
      {
        Err(ProducerLeaseFenceLost.into())
      },
      Err(error) => Err(error.into()),
    }
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

fn transaction_cancellation_has_code(error: &TransactWriteItemsError, expected_code: &str) -> bool {
  matches!(
    error,
    TransactWriteItemsError::TransactionCanceledException(cancellation)
      if cancellation
        .cancellation_reasons()
        .iter()
        .any(|reason| reason.code() == Some(expected_code))
  )
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
