#[cfg(test)]
#[path = "./consumer_group_membership_dynamo_test.rs"]
mod tests;

use crate::ConsumerGroupMembershipStore;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use log::trace;
use std::collections::{HashMap, HashSet};

const ATTR_PK: &str = "pk";
const ATTR_SK: &str = "sk";
const ATTR_LEASE_EXPIRES: &str = "lease_expiry_ts";
const ATTR_LAST_HEARTBEAT: &str = "last_heartbeat_ts";
const ATTR_TTL: &str = "ttl_epoch_seconds";
const DEFAULT_MEMBERSHIP_TTL_BUFFER_SECONDS: u32 = 3_600;

//
// DynamoConsumerGroupMembershipStore
//

#[derive(Clone, Debug)]
pub struct DynamoConsumerGroupMembershipStore {
  client: Client,
  table_name: String,
  ttl_buffer_seconds: i64,
}

impl DynamoConsumerGroupMembershipStore {
  #[must_use]
  pub fn new(client: Client, table_name: impl Into<String>) -> Self {
    Self::with_ttl_buffer_seconds(client, table_name, DEFAULT_MEMBERSHIP_TTL_BUFFER_SECONDS)
  }

  #[must_use]
  pub fn with_ttl_buffer_seconds(
    client: Client,
    table_name: impl Into<String>,
    ttl_buffer_seconds: u32,
  ) -> Self {
    Self {
      client,
      table_name: table_name.into(),
      ttl_buffer_seconds: i64::from(ttl_buffer_seconds),
    }
  }

  fn pk(topic: &str, group_id: &str) -> String {
    format!("{topic}#{group_id}")
  }

  fn sk(member_id: &str) -> String {
    member_id.to_string()
  }
}

#[async_trait]
impl ConsumerGroupMembershipStore for DynamoConsumerGroupMembershipStore {
  async fn register_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> Result<()> {
    trace!(
      "consumer membership(dynamo) register: table={}, topic={}, group_id={}, member_id={}",
      self.table_name, topic, group_id, member_id
    );
    let expires_at = expires_at(now_ts_ms, ttl_ms)?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at, self.ttl_buffer_seconds)?;

    let mut values = HashMap::new();
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at.to_string()),
    );
    values.insert(
      ":ttl".to_string(),
      AttributeValue::N(ttl_epoch_seconds.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));

    self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(Self::pk(topic, group_id)))
      .key(ATTR_SK, AttributeValue::S(Self::sk(member_id)))
      .update_expression(format!(
        "SET {ATTR_LEASE_EXPIRES} = :expires, {ATTR_TTL} = :ttl, {ATTR_LAST_HEARTBEAT} = :now"
      ))
      .set_expression_attribute_values(Some(values))
      .send()
      .await?;

    Ok(())
  }

  async fn heartbeat_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> Result<()> {
    trace!(
      "consumer membership(dynamo) heartbeat: table={}, topic={}, group_id={}, member_id={}",
      self.table_name, topic, group_id, member_id
    );
    self
      .register_member(topic, group_id, member_id, now_ts_ms, ttl_ms)
      .await
  }

  async fn deregister_member(&self, topic: &str, group_id: &str, member_id: &str) -> Result<()> {
    trace!(
      "consumer membership(dynamo) deregister: table={}, topic={}, group_id={}, member_id={}",
      self.table_name, topic, group_id, member_id
    );

    self
      .client
      .delete_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(Self::pk(topic, group_id)))
      .key(ATTR_SK, AttributeValue::S(Self::sk(member_id)))
      .send()
      .await?;

    Ok(())
  }

  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now_ts_ms: i64,
  ) -> Result<Vec<String>> {
    trace!(
      "consumer membership(dynamo) list_active_members: table={}, topic={}, group_id={}",
      self.table_name, topic, group_id
    );

    let mut members = HashSet::new();
    let mut start_key: Option<HashMap<String, AttributeValue>> = None;

    loop {
      let mut values = HashMap::new();
      values.insert(
        ":pk".to_string(),
        AttributeValue::S(Self::pk(topic, group_id)),
      );
      values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));

      let mut query = self
        .client
        .query()
        .table_name(&self.table_name)
        .key_condition_expression(format!("{ATTR_PK} = :pk"))
        .filter_expression(format!("{ATTR_LEASE_EXPIRES} > :now"))
        .projection_expression(ATTR_SK)
        .set_expression_attribute_values(Some(values));

      if let Some(key) = start_key.take() {
        query = query.set_exclusive_start_key(Some(key));
      }

      let response = query.send().await?;
      for item in response.items() {
        if let Some(AttributeValue::S(member_id)) = item.get(ATTR_SK) {
          members.insert(member_id.clone());
        }
      }

      if let Some(key) = response.last_evaluated_key {
        start_key = Some(key);
      } else {
        break;
      }
    }

    let mut members = members.into_iter().collect::<Vec<_>>();
    members.sort();
    Ok(members)
  }
}

fn expires_at(now_ts_ms: i64, ttl_ms: i64) -> Result<i64> {
  if ttl_ms <= 0 {
    return Err(anyhow!("membership ttl must be greater than zero"));
  }

  now_ts_ms
    .checked_add(ttl_ms)
    .ok_or_else(|| anyhow!("membership expiration overflow"))
}

fn ttl_epoch_seconds(expires_at_ms: i64, ttl_buffer_seconds: i64) -> Result<i64> {
  let expires_at_seconds = expires_at_ms
    .checked_div(1_000)
    .ok_or_else(|| anyhow!("membership ttl conversion overflow"))?;

  expires_at_seconds
    .checked_add(ttl_buffer_seconds)
    .ok_or_else(|| anyhow!("membership ttl overflow"))
}
