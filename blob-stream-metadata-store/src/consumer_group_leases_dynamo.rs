// blob-stream - DynamoDB consumer group leases
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./consumer_group_leases_dynamo_test.rs"]
mod tests;

use crate::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use blob_stream_types::CommittedCursor;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const ATTR_PK: &str = "pk";
const ATTR_SK: &str = "sk";
const ATTR_OWNER: &str = "owner_id";
const ATTR_LEASE_EXPIRES: &str = "lease_expiry_ts";
const ATTR_GENERATION: &str = "generation";
const ATTR_LAST_HEARTBEAT: &str = "last_heartbeat_ts";
const ATTR_COMMITTED_CURSOR: &str = "committed_cursor";
const ATTR_COMMITTED_TS: &str = "committed_ts";
const ATTR_TOPIC: &str = "topic";
const ATTR_GROUP_ID: &str = "group_id";
const ATTR_VIRTUAL_PARTITION_ID: &str = "virtual_partition_id";

//
// DynamoConsumerGroupLeaseStore
//

#[derive(Clone, Debug)]
pub struct DynamoConsumerGroupLeaseStore {
  client: Client,
  table_name: String,
}

impl DynamoConsumerGroupLeaseStore {
  #[must_use]
  pub fn new(client: Client, table_name: impl Into<String>) -> Self {
    Self {
      client,
      table_name: table_name.into(),
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
      .send()
      .await?;

    let Some(item) = response.item else {
      return Ok(None);
    };

    let entry: DynamoLeaseItem = serde_dynamo::from_item(item)?;
    Ok(Some(entry.into_lease()))
  }

  fn lease_from_item(attributes: HashMap<String, AttributeValue>) -> Result<ConsumerGroupLease> {
    let entry: DynamoLeaseItem = serde_dynamo::from_item(attributes)?;
    Ok(entry.into_lease())
  }

  fn cursor_value(cursor: &CommittedCursor) -> Result<AttributeValue> {
    let item = serde_dynamo::to_item(cursor)?;
    Ok(AttributeValue::M(item))
  }
}

#[async_trait]
impl ConsumerGroupLeaseStore for DynamoConsumerGroupLeaseStore {
  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<ConsumerGroupAssignmentOutcome> {
    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;

    let mut values = HashMap::new();
    values.insert(":owner".to_string(), AttributeValue::S(owner_id.clone()));
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at.to_string()),
    );
    values.insert(
      ":generation".to_string(),
      AttributeValue::N(generation.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    values.insert(":topic".to_string(), AttributeValue::S(key.topic.clone()));
    values.insert(
      ":group_id".to_string(),
      AttributeValue::S(key.group_id.clone()),
    );
    values.insert(
      ":virtual_partition_id".to_string(),
      AttributeValue::N(key.virtual_partition_id.to_string()),
    );

    let update = format!(
      "SET {ATTR_OWNER} = :owner, {ATTR_LEASE_EXPIRES} = :expires, {ATTR_GENERATION} = \
       :generation, {ATTR_LAST_HEARTBEAT} = :now, {ATTR_TOPIC} = if_not_exists({ATTR_TOPIC}, \
       :topic), {ATTR_GROUP_ID} = if_not_exists({ATTR_GROUP_ID}, :group_id), \
       {ATTR_VIRTUAL_PARTITION_ID} = if_not_exists({ATTR_VIRTUAL_PARTITION_ID}, \
       :virtual_partition_id)"
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
      .return_values(ReturnValue::AllNew)
      .send()
      .await;

    match response {
      Ok(output) => {
        let attributes = output
          .attributes
          .ok_or_else(|| anyhow!("lease attributes missing"))?;
        let lease = Self::lease_from_item(attributes)?;
        Ok(ConsumerGroupAssignmentOutcome::Assigned(lease))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
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
    now_ts_ms: i64,
    lease_duration_ms: i64,
    committed_cursor: Option<CommittedCursor>,
  ) -> Result<ConsumerGroupHeartbeatOutcome> {
    if let Some(cursor) = committed_cursor.as_ref() {
      validate_cursor(key, cursor)?;
    }

    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;

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
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));

    let update = if let Some(cursor) = committed_cursor {
      values.insert(":cursor".to_string(), Self::cursor_value(&cursor)?);
      format!(
        "SET {ATTR_LEASE_EXPIRES} = :expires, {ATTR_LAST_HEARTBEAT} = :now, \
         {ATTR_COMMITTED_CURSOR} = :cursor, {ATTR_COMMITTED_TS} = :now"
      )
    } else {
      format!("SET {ATTR_LEASE_EXPIRES} = :expires, {ATTR_LAST_HEARTBEAT} = :now")
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
      .send()
      .await;

    match response {
      Ok(output) => {
        let attributes = output
          .attributes
          .ok_or_else(|| anyhow!("lease attributes missing"))?;
        let lease = Self::lease_from_item(attributes)?;
        Ok(ConsumerGroupHeartbeatOutcome::Renewed(lease))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.get_lease(key).await? else {
          return Ok(ConsumerGroupHeartbeatOutcome::Expired);
        };
        if lease.lease_expiration_ts_ms <= now_ts_ms {
          Ok(ConsumerGroupHeartbeatOutcome::Expired)
        } else {
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
    now_ts_ms: i64,
    committed_cursor: CommittedCursor,
  ) -> Result<ConsumerGroupCommitOutcome> {
    validate_cursor(key, &committed_cursor)?;

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
      .send()
      .await;

    match response {
      Ok(output) => {
        let attributes = output
          .attributes
          .ok_or_else(|| anyhow!("lease attributes missing"))?;
        let lease = Self::lease_from_item(attributes)?;
        Ok(ConsumerGroupCommitOutcome::Committed(lease))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.get_lease(key).await? else {
          return Ok(ConsumerGroupCommitOutcome::Expired);
        };
        if lease.lease_expiration_ts_ms <= now_ts_ms {
          Ok(ConsumerGroupCommitOutcome::Expired)
        } else {
          Ok(ConsumerGroupCommitOutcome::HeldByOther(lease))
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
  #[serde(rename = "pk")]
  partition_key: String,
  #[serde(rename = "sk")]
  sort_key: String,
  topic: String,
  group_id: String,
  virtual_partition_id: u32,
  owner_id: String,
  generation: u64,
  #[serde(rename = "lease_expiry_ts")]
  lease_expiration_ts_ms: i64,
  #[serde(rename = "last_heartbeat_ts")]
  last_heartbeat_ts_ms: i64,
  committed_cursor: Option<CommittedCursor>,
  committed_ts: Option<i64>,
}

impl DynamoLeaseItem {
  fn into_lease(self) -> ConsumerGroupLease {
    ConsumerGroupLease {
      key: ConsumerGroupLeaseKey {
        topic: self.topic,
        group_id: self.group_id,
        virtual_partition_id: self.virtual_partition_id,
      },
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
