#[cfg(test)]
#[path = "./consumer_group_leases_dynamo_test.rs"]
mod tests;

use crate::dynamo::duration_seconds_ceil;
use crate::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeasePredecessor,
  ConsumerGroupLeaseStore,
  ConsumerGroupReleaseOutcome,
  DynamoCapacityMetrics,
  consumer_group_lease_transition,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnConsumedCapacity, ReturnValue};
use blob_stream_types::{CommittedCursor, unix_millis_from_offset_datetime};
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};

const ATTR_PK: &str = "pk";
const ATTR_SK: &str = "sk";
const ATTR_OWNER: &str = "owner_id";
const ATTR_LEASE_EXPIRES: &str = "lease_expiry_ts";
const ATTR_GENERATION: &str = "generation";
const ATTR_LAST_HEARTBEAT: &str = "last_heartbeat_ts";
const ATTR_COMMITTED_CURSOR: &str = "committed_cursor";
const ATTR_COMMITTED_TS: &str = "committed_ts";
const ATTR_GRACEFUL_RELEASE_TS: &str = "graceful_release_ts";
const ATTR_TTL: &str = "ttl_epoch_seconds";

//
// DynamoConsumerGroupLeaseStore
//

#[derive(Clone, Debug)]
pub struct DynamoConsumerGroupLeaseStore {
  client: Client,
  table_name: String,
  ttl_buffer: Duration,
  capacity_metrics: Option<DynamoCapacityMetrics>,
}

impl DynamoConsumerGroupLeaseStore {
  #[must_use]
  pub fn new(
    client: Client,
    table_name: impl Into<String>,
    ttl_buffer: Duration,
    capacity_metrics: Option<DynamoCapacityMetrics>,
  ) -> Self {
    Self {
      client,
      table_name: table_name.into(),
      ttl_buffer,
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

  async fn get_lease(&self, key: &ConsumerGroupLeaseKey) -> Result<Option<ConsumerGroupLease>> {
    let response = self
      .client
      .get_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(key.partition_key()))
      .key(ATTR_SK, AttributeValue::S(key.sort_key()))
      .consistent_read(false)
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await?;
    self.record_read_capacity(response.consumed_capacity.as_ref());

    let Some(item) = response.item else {
      return Ok(None);
    };

    let entry: DynamoLeaseItem = serde_dynamo::from_item(item)?;
    Ok(Some(entry.into_lease(key.clone())))
  }

  fn lease_from_item(
    attributes: HashMap<String, AttributeValue>,
    key: ConsumerGroupLeaseKey,
  ) -> Result<ConsumerGroupLease> {
    let entry: DynamoLeaseItem = serde_dynamo::from_item(attributes)?;
    Ok(entry.into_lease(key))
  }

  fn cursor_value(cursor: &CommittedCursor) -> Result<AttributeValue> {
    let item = serde_dynamo::to_item(cursor)?;
    Ok(AttributeValue::M(item))
  }
}

#[async_trait]
impl ConsumerGroupLeaseStore for DynamoConsumerGroupLeaseStore {
  async fn list_group_leases(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Vec<ConsumerGroupLease>> {
    trace!(
      "consumer lease(dynamo) list group: table={}, topic={}, group_id={}",
      self.table_name, topic, group_id
    );
    let partition_key = format!("{topic}#{group_id}");
    let mut leases = Vec::new();
    let mut start_key: Option<HashMap<String, AttributeValue>> = None;

    loop {
      let mut values = HashMap::new();
      values.insert(":pk".to_string(), AttributeValue::S(partition_key.clone()));
      let mut query = self
        .client
        .query()
        .table_name(&self.table_name)
        .key_condition_expression(format!("{ATTR_PK} = :pk"))
        .consistent_read(true)
        .set_expression_attribute_values(Some(values));
      if let Some(key) = start_key.take() {
        query = query.set_exclusive_start_key(Some(key));
      }

      let response = query
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await?;
      self.record_read_capacity(response.consumed_capacity.as_ref());
      for item in response.items() {
        let partition_id = item
          .get(ATTR_SK)
          .ok_or_else(|| anyhow!("consumer lease query returned row without string sort key"))?
          .as_s()
          .map_err(|_| anyhow!("consumer lease query returned row without string sort key"))?
          .parse()
          .map_err(|error| {
            anyhow!("consumer lease query returned invalid partition id: {error}")
          })?;
        let key = ConsumerGroupLeaseKey {
          topic: topic.to_string(),
          group_id: group_id.to_string(),
          virtual_partition_id: partition_id,
        };
        leases.push(Self::lease_from_item(item.clone(), key)?);
      }

      if let Some(key) = response.last_evaluated_key {
        start_key = Some(key);
      } else {
        break;
      }
    }

    leases.sort_by_key(|lease| lease.key.virtual_partition_id);
    Ok(leases)
  }

  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: Duration,
  ) -> Result<ConsumerGroupAssignmentOutcome> {
    trace!(
      "consumer lease(dynamo) assign: table={}, topic={}, group_id={}, partition={}, owner_id={}, \
       generation={}",
      self.table_name, key.topic, key.group_id, key.virtual_partition_id, owner_id, generation
    );
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let lease_duration_ms = i64::try_from(lease_duration.whole_milliseconds())
      .map_err(|_| anyhow!("lease duration exceeds Dynamo millisecond range"))?;
    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at, self.ttl_buffer)?;

    let mut values = HashMap::new();
    values.insert(":owner".to_string(), AttributeValue::S(owner_id.clone()));
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at.to_string()),
    );
    values.insert(
      ":ttl".to_string(),
      AttributeValue::N(ttl_epoch_seconds.to_string()),
    );
    values.insert(
      ":generation".to_string(),
      AttributeValue::N(generation.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    let update = format!(
      "SET {ATTR_OWNER} = :owner, {ATTR_LEASE_EXPIRES} = :expires, {ATTR_GENERATION} = \
       :generation, {ATTR_LAST_HEARTBEAT} = :now, {ATTR_TTL} = :ttl REMOVE \
       {ATTR_GRACEFUL_RELEASE_TS}"
    );
    let condition = format!(
      "attribute_not_exists({ATTR_PK}) OR {ATTR_LEASE_EXPIRES} <= :now OR {ATTR_OWNER} = :owner"
    );

    let response = self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(key.partition_key()))
      .key(ATTR_SK, AttributeValue::S(key.sort_key()))
      .update_expression(update)
      .condition_expression(condition)
      .set_expression_attribute_values(Some(values))
      .return_values(ReturnValue::AllOld)
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await;

    match response {
      Ok(output) => {
        self.record_write_capacity(output.consumed_capacity.as_ref());
        let previous_entry = output.attributes.map(serde_dynamo::from_item).transpose()?;
        let previous_lease = previous_entry
          .as_ref()
          .map(|entry: &DynamoLeaseItem| Box::new(entry.clone().into_lease(key.clone())));
        let lease = ConsumerGroupLease {
          key,
          owner_id,
          generation,
          lease_expiration_ts_ms: expires_at,
          last_heartbeat_ts_ms: now_ts_ms,
          committed_cursor: previous_lease
            .as_ref()
            .and_then(|previous| previous.committed_cursor.clone()),
          committed_ts_ms: previous_lease
            .as_ref()
            .and_then(|previous| previous.committed_ts_ms),
        };
        if let Some(previous) = previous_entry.as_ref()
          && previous.owner_id != lease.owner_id
          && previous.graceful_release_ts.is_none()
        {
          debug_assert!(previous.lease_expiration_ts_ms <= now_ts_ms);
        }
        let transition = consumer_group_lease_transition(
          previous_entry
            .as_ref()
            .map(|previous| ConsumerGroupLeasePredecessor {
              owner_id: &previous.owner_id,
              generation: previous.generation,
              last_heartbeat_ts_ms: previous.last_heartbeat_ts_ms,
              graceful_release_ts_ms: previous.graceful_release_ts,
            }),
          &lease.owner_id,
        );
        debug!("consumer lease(dynamo) assign result: assigned, transition={transition:?}");
        Ok(ConsumerGroupAssignmentOutcome::Assigned {
          lease,
          previous_lease,
          transition,
        })
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        debug!("consumer lease(dynamo) assign result: held_by_other");
        let lease = self
          .get_lease(&key)
          .await?
          .ok_or_else(|| anyhow!("lease missing after conditional failure"))?;
        Ok(ConsumerGroupAssignmentOutcome::HeldByOther(lease))
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: Duration,
    committed_cursor: Option<CommittedCursor>,
  ) -> Result<ConsumerGroupHeartbeatOutcome> {
    trace!(
      "consumer lease(dynamo) heartbeat: table={}, topic={}, group_id={}, partition={}, \
       owner_id={}, generation={}",
      self.table_name, key.topic, key.group_id, key.virtual_partition_id, owner_id, generation
    );
    if let Some(cursor) = committed_cursor.as_ref() {
      validate_cursor(key, cursor)?;
    }

    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let lease_duration_ms = i64::try_from(lease_duration.whole_milliseconds())
      .map_err(|_| anyhow!("lease duration exceeds Dynamo millisecond range"))?;
    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at, self.ttl_buffer)?;

    let mut values = HashMap::new();
    values.insert(
      ":owner".to_string(),
      AttributeValue::S(owner_id.to_string()),
    );
    values.insert(
      ":generation".to_string(),
      AttributeValue::N(generation.to_string()),
    );
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at.to_string()),
    );
    values.insert(
      ":ttl".to_string(),
      AttributeValue::N(ttl_epoch_seconds.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));

    let update = if let Some(cursor) = committed_cursor {
      values.insert(":cursor".to_string(), Self::cursor_value(&cursor)?);
      format!(
        "SET {ATTR_LEASE_EXPIRES} = :expires, {ATTR_LAST_HEARTBEAT} = :now, {ATTR_TTL} = :ttl, \
         {ATTR_COMMITTED_CURSOR} = :cursor, {ATTR_COMMITTED_TS} = :now"
      )
    } else {
      format!(
        "SET {ATTR_LEASE_EXPIRES} = :expires, {ATTR_LAST_HEARTBEAT} = :now, {ATTR_TTL} = :ttl"
      )
    };
    let condition = format!(
      "{ATTR_OWNER} = :owner AND {ATTR_GENERATION} = :generation AND {ATTR_LEASE_EXPIRES} > :now"
    );

    let response = self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(key.partition_key()))
      .key(ATTR_SK, AttributeValue::S(key.sort_key()))
      .update_expression(update)
      .condition_expression(condition)
      .set_expression_attribute_values(Some(values))
      .return_values(ReturnValue::AllNew)
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await;

    match response {
      Ok(output) => {
        self.record_write_capacity(output.consumed_capacity.as_ref());
        let attributes = output
          .attributes
          .ok_or_else(|| anyhow!("lease attributes missing"))?;
        let lease = Self::lease_from_item(attributes, key.clone())?;
        debug!("consumer lease(dynamo) heartbeat result: renewed");
        Ok(ConsumerGroupHeartbeatOutcome::Renewed(lease))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.get_lease(key).await? else {
          debug!("consumer lease(dynamo) heartbeat result: expired");
          return Ok(ConsumerGroupHeartbeatOutcome::Expired);
        };
        if lease.lease_expiration_ts_ms <= now_ts_ms {
          debug!("consumer lease(dynamo) heartbeat result: expired");
          Ok(ConsumerGroupHeartbeatOutcome::Expired)
        } else {
          debug!("consumer lease(dynamo) heartbeat result: held_by_other");
          Ok(ConsumerGroupHeartbeatOutcome::HeldByOther(lease))
        }
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    committed_cursor: CommittedCursor,
  ) -> Result<ConsumerGroupCommitOutcome> {
    trace!(
      "consumer lease(dynamo) commit: table={}, topic={}, group_id={}, partition={}, owner_id={}, \
       generation={}, seq_end={}",
      self.table_name,
      key.topic,
      key.group_id,
      key.virtual_partition_id,
      owner_id,
      generation,
      committed_cursor.seq_end
    );
    validate_cursor(key, &committed_cursor)?;

    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let mut values = HashMap::new();
    values.insert(
      ":owner".to_string(),
      AttributeValue::S(owner_id.to_string()),
    );
    values.insert(
      ":generation".to_string(),
      AttributeValue::N(generation.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    values.insert(
      ":cursor".to_string(),
      Self::cursor_value(&committed_cursor)?,
    );

    let update = format!("SET {ATTR_COMMITTED_CURSOR} = :cursor, {ATTR_COMMITTED_TS} = :now");
    let condition = format!(
      "{ATTR_OWNER} = :owner AND {ATTR_GENERATION} = :generation AND {ATTR_LEASE_EXPIRES} > :now"
    );

    let response = self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(key.partition_key()))
      .key(ATTR_SK, AttributeValue::S(key.sort_key()))
      .update_expression(update)
      .condition_expression(condition)
      .set_expression_attribute_values(Some(values))
      .return_values(ReturnValue::AllNew)
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await;

    match response {
      Ok(output) => {
        self.record_write_capacity(output.consumed_capacity.as_ref());
        let attributes = output
          .attributes
          .ok_or_else(|| anyhow!("lease attributes missing"))?;
        let lease = Self::lease_from_item(attributes, key.clone())?;
        debug!("consumer lease(dynamo) commit result: committed");
        Ok(ConsumerGroupCommitOutcome::Committed(lease))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.get_lease(key).await? else {
          debug!("consumer lease(dynamo) commit result: expired");
          return Ok(ConsumerGroupCommitOutcome::Expired);
        };
        if lease.lease_expiration_ts_ms <= now_ts_ms {
          debug!("consumer lease(dynamo) commit result: expired");
          Ok(ConsumerGroupCommitOutcome::Expired)
        } else {
          debug!("consumer lease(dynamo) commit result: held_by_other");
          Ok(ConsumerGroupCommitOutcome::HeldByOther(lease))
        }
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
  ) -> Result<ConsumerGroupReleaseOutcome> {
    trace!(
      "consumer lease(dynamo) release: table={}, topic={}, group_id={}, partition={}, \
       owner_id={}, generation={}",
      self.table_name, key.topic, key.group_id, key.virtual_partition_id, owner_id, generation
    );

    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let mut values = HashMap::new();
    values.insert(
      ":owner".to_string(),
      AttributeValue::S(owner_id.to_string()),
    );
    values.insert(
      ":generation".to_string(),
      AttributeValue::N(generation.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    values.insert(
      ":ttl".to_string(),
      AttributeValue::N(ttl_epoch_seconds(now_ts_ms, self.ttl_buffer)?.to_string()),
    );
    let update = format!(
      "SET {ATTR_LEASE_EXPIRES} = :now, {ATTR_LAST_HEARTBEAT} = :now, {ATTR_GRACEFUL_RELEASE_TS} \
       = :now, {ATTR_TTL} = :ttl"
    );
    let condition = format!(
      "{ATTR_OWNER} = :owner AND {ATTR_GENERATION} = :generation AND {ATTR_LEASE_EXPIRES} > :now"
    );

    let response = self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(key.partition_key()))
      .key(ATTR_SK, AttributeValue::S(key.sort_key()))
      .update_expression(update)
      .condition_expression(condition)
      .set_expression_attribute_values(Some(values))
      .return_values(ReturnValue::AllNew)
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await;

    match response {
      Ok(output) => {
        self.record_write_capacity(output.consumed_capacity.as_ref());
        debug!("consumer lease(dynamo) release result: released");
        Ok(ConsumerGroupReleaseOutcome::Released)
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.get_lease(key).await? else {
          debug!("consumer lease(dynamo) release result: expired");
          return Ok(ConsumerGroupReleaseOutcome::Expired);
        };

        if lease.lease_expiration_ts_ms <= now_ts_ms {
          debug!("consumer lease(dynamo) release result: expired");
          Ok(ConsumerGroupReleaseOutcome::Expired)
        } else {
          debug!("consumer lease(dynamo) release result: held_by_other");
          Ok(ConsumerGroupReleaseOutcome::HeldByOther(lease))
        }
      },
      Err(error) => Err(error.into()),
    }
  }
}

//
// DynamoLeaseItem
//

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DynamoLeaseItem {
  owner_id: String,
  generation: u64,
  #[serde(rename = "lease_expiry_ts")]
  lease_expiration_ts_ms: i64,
  #[serde(rename = "last_heartbeat_ts")]
  last_heartbeat_ts_ms: i64,
  committed_cursor: Option<CommittedCursor>,
  committed_ts: Option<i64>,
  graceful_release_ts: Option<i64>,
}

impl DynamoLeaseItem {
  fn into_lease(self, key: ConsumerGroupLeaseKey) -> ConsumerGroupLease {
    ConsumerGroupLease {
      key,
      owner_id: self.owner_id,
      generation: self.generation,
      lease_expiration_ts_ms: self.lease_expiration_ts_ms,
      last_heartbeat_ts_ms: self.last_heartbeat_ts_ms,
      committed_cursor: self.committed_cursor,
      committed_ts_ms: self.committed_ts,
    }
  }
}

fn validate_cursor(key: &ConsumerGroupLeaseKey, cursor: &CommittedCursor) -> Result<()> {
  if key.virtual_partition_id != cursor.virtual_partition_id {
    return Err(anyhow!(
      "committed cursor partition mismatch: {} != {}",
      key.virtual_partition_id,
      cursor.virtual_partition_id
    ));
  }

  Ok(())
}

fn expires_at(now_ts_ms: i64, lease_duration_ms: i64) -> Result<i64> {
  now_ts_ms
    .checked_add(lease_duration_ms)
    .ok_or_else(|| anyhow!("lease expiration overflow"))
}

fn ttl_epoch_seconds(expires_at_ms: i64, ttl_buffer: Duration) -> Result<i64> {
  let expires_at_seconds = expires_at_ms
    .checked_div(1_000)
    .ok_or_else(|| anyhow!("lease ttl conversion overflow"))?;
  let ttl_buffer_seconds = duration_seconds_ceil(ttl_buffer)
    .ok_or_else(|| anyhow!("lease ttl buffer conversion overflow"))?;

  expires_at_seconds
    .checked_add(ttl_buffer_seconds)
    .ok_or_else(|| anyhow!("lease ttl overflow"))
}
