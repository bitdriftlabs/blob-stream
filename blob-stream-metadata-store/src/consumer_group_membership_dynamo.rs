#[cfg(test)]
#[path = "./consumer_group_membership_dynamo_test.rs"]
mod tests;

use crate::{
  ConsumerGroupAssignment,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLease,
  ConsumerGroupPlannerLeaseOutcome,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::types::{AttributeValue, ConditionCheck, TransactWriteItem, Update};
use log::trace;
use std::collections::{HashMap, HashSet};

const ATTR_PK: &str = "pk";
const ATTR_SK: &str = "sk";
const ATTR_LEASE_EXPIRES: &str = "lease_expiry_ts";
const ATTR_LAST_HEARTBEAT: &str = "last_heartbeat_ts";
const ATTR_TTL: &str = "ttl_epoch_seconds";
const ATTR_RECORD_TYPE: &str = "record_type";
const ATTR_OWNER: &str = "owner_id";
const ATTR_PLAN_VERSION: &str = "plan_version";
const ATTR_PLAN_PLANNER: &str = "plan_planner_member_id";
const ATTR_PLAN_MEMBERS: &str = "plan_members";
const ATTR_PLAN_ASSIGNMENTS: &str = "plan_assignments";
const ATTR_PLAN_PUBLISHED: &str = "plan_published_ts";
const ATTR_ASSIGNMENT_PARTITION: &str = "virtual_partition_id";
const ATTR_ASSIGNMENT_MEMBER: &str = "member_id";
const RECORD_TYPE_MEMBER: &str = "member";
const RECORD_TYPE_ASSIGNMENT_PLAN: &str = "assignment_plan";
const RECORD_TYPE_PLANNER_LEASE: &str = "assignment_planner_lease";
const ASSIGNMENT_PLAN_SORT_KEY: &str = "__blob_stream_assignment_plan_v1__";
const PLANNER_LEASE_SORT_KEY: &str = "__blob_stream_assignment_planner_v1__";
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

  fn planner_key() -> String {
    PLANNER_LEASE_SORT_KEY.to_string()
  }

  fn plan_key() -> String {
    ASSIGNMENT_PLAN_SORT_KEY.to_string()
  }

  fn plan_from_item(item: &HashMap<String, AttributeValue>) -> Result<ConsumerGroupAssignmentPlan> {
    let version = item
      .get(ATTR_PLAN_VERSION)
      .and_then(|value| value.as_n().ok())
      .ok_or_else(|| anyhow!("assignment plan version missing"))?
      .parse()?;
    let members = item
      .get(ATTR_PLAN_MEMBERS)
      .and_then(|value| value.as_l().ok())
      .ok_or_else(|| anyhow!("assignment plan members missing"))?
      .iter()
      .map(|value| {
        value
          .as_s()
          .map(ToString::to_string)
          .map_err(|_| anyhow!("assignment plan member must be a string"))
      })
      .collect::<Result<Vec<_>>>()?;
    let planner_member_id = item
      .get(ATTR_PLAN_PLANNER)
      .and_then(|value| value.as_s().ok())
      .ok_or_else(|| anyhow!("assignment plan planner missing"))?
      .clone();
    let assignments = item
      .get(ATTR_PLAN_ASSIGNMENTS)
      .and_then(|value| value.as_l().ok())
      .ok_or_else(|| anyhow!("assignment plan assignments missing"))?
      .iter()
      .map(|value| {
        let assignment = value
          .as_m()
          .map_err(|_| anyhow!("assignment plan entry must be a map"))?;
        let virtual_partition_id = assignment
          .get(ATTR_ASSIGNMENT_PARTITION)
          .and_then(|value| value.as_n().ok())
          .ok_or_else(|| anyhow!("assignment plan partition missing"))?
          .parse()?;
        let member_id = assignment
          .get(ATTR_ASSIGNMENT_MEMBER)
          .and_then(|value| value.as_s().ok())
          .ok_or_else(|| anyhow!("assignment plan owner missing"))?
          .clone();
        Ok(ConsumerGroupAssignment {
          virtual_partition_id,
          member_id,
        })
      })
      .collect::<Result<Vec<_>>>()?;
    let published_ts_ms = item
      .get(ATTR_PLAN_PUBLISHED)
      .and_then(|value| value.as_n().ok())
      .ok_or_else(|| anyhow!("assignment plan publication timestamp missing"))?
      .parse()?;

    Ok(ConsumerGroupAssignmentPlan {
      version,
      planner_member_id,
      members,
      assignments,
      published_ts_ms,
    })
  }

  fn plan_values(plan: &ConsumerGroupAssignmentPlan) -> HashMap<String, AttributeValue> {
    let mut values = HashMap::new();
    values.insert(
      ":version".to_string(),
      AttributeValue::N(plan.version.to_string()),
    );
    values.insert(
      ":planner".to_string(),
      AttributeValue::S(plan.planner_member_id.clone()),
    );
    values.insert(
      ":members".to_string(),
      AttributeValue::L(
        plan
          .members
          .iter()
          .cloned()
          .map(AttributeValue::S)
          .collect(),
      ),
    );
    values.insert(
      ":assignments".to_string(),
      AttributeValue::L(
        plan
          .assignments
          .iter()
          .map(|assignment| {
            let mut entry = HashMap::new();
            entry.insert(
              ATTR_ASSIGNMENT_PARTITION.to_string(),
              AttributeValue::N(assignment.virtual_partition_id.to_string()),
            );
            entry.insert(
              ATTR_ASSIGNMENT_MEMBER.to_string(),
              AttributeValue::S(assignment.member_id.clone()),
            );
            AttributeValue::M(entry)
          })
          .collect(),
      ),
    );
    values.insert(
      ":published".to_string(),
      AttributeValue::N(plan.published_ts_ms.to_string()),
    );
    values.insert(
      ":plan_type".to_string(),
      AttributeValue::S(RECORD_TYPE_ASSIGNMENT_PLAN.to_string()),
    );
    values
  }

  fn planner_lease_from_item(
    item: &HashMap<String, AttributeValue>,
  ) -> Result<ConsumerGroupPlannerLease> {
    let member_id = item
      .get(ATTR_OWNER)
      .and_then(|value| value.as_s().ok())
      .ok_or_else(|| anyhow!("assignment planner owner missing"))?
      .clone();
    let lease_expiration_ts_ms = item
      .get(ATTR_LEASE_EXPIRES)
      .and_then(|value| value.as_n().ok())
      .ok_or_else(|| anyhow!("assignment planner expiration missing"))?
      .parse()?;
    Ok(ConsumerGroupPlannerLease {
      member_id,
      lease_expiration_ts_ms,
    })
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
    values.insert(
      ":member_type".to_string(),
      AttributeValue::S(RECORD_TYPE_MEMBER.to_string()),
    );

    self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(Self::pk(topic, group_id)))
      .key(ATTR_SK, AttributeValue::S(Self::sk(member_id)))
      .update_expression(format!(
        "SET {ATTR_LEASE_EXPIRES} = :expires, {ATTR_TTL} = :ttl, {ATTR_LAST_HEARTBEAT} = :now, \
         {ATTR_RECORD_TYPE} = :member_type"
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
      values.insert(
        ":member_type".to_string(),
        AttributeValue::S(RECORD_TYPE_MEMBER.to_string()),
      );

      let mut query = self
        .client
        .query()
        .table_name(&self.table_name)
        .key_condition_expression(format!("{ATTR_PK} = :pk"))
        .consistent_read(true)
        .filter_expression(format!(
          "{ATTR_LEASE_EXPIRES} > :now AND (attribute_not_exists({ATTR_RECORD_TYPE}) OR \
           {ATTR_RECORD_TYPE} = :member_type)"
        ))
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

  async fn get_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Option<ConsumerGroupAssignmentPlan>> {
    let response = self
      .client
      .get_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(Self::pk(topic, group_id)))
      .key(ATTR_SK, AttributeValue::S(Self::plan_key()))
      .consistent_read(true)
      .send()
      .await?;
    let Some(item) = response.item else {
      return Ok(None);
    };

    if item
      .get(ATTR_RECORD_TYPE)
      .and_then(|value| value.as_s().ok().map(String::as_str))
      != Some(RECORD_TYPE_ASSIGNMENT_PLAN)
    {
      return Err(anyhow!("unexpected assignment plan record type"));
    }

    Self::plan_from_item(&item).map(Some)
  }

  async fn get_planner_lease(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Option<ConsumerGroupPlannerLease>> {
    let response = self
      .client
      .get_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(Self::pk(topic, group_id)))
      .key(ATTR_SK, AttributeValue::S(Self::planner_key()))
      .consistent_read(true)
      .send()
      .await?;
    let Some(item) = response.item else {
      return Ok(None);
    };
    if item
      .get(ATTR_RECORD_TYPE)
      .and_then(|value| value.as_s().ok().map(String::as_str))
      != Some(RECORD_TYPE_PLANNER_LEASE)
    {
      return Err(anyhow!("unexpected assignment planner record type"));
    }
    Self::planner_lease_from_item(&item).map(Some)
  }

  async fn acquire_or_renew_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> Result<ConsumerGroupPlannerLeaseOutcome> {
    let expires_at = expires_at(now_ts_ms, ttl_ms)?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at, self.ttl_buffer_seconds)?;
    let mut values = HashMap::new();
    values.insert(
      ":owner".to_string(),
      AttributeValue::S(member_id.to_string()),
    );
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    values.insert(
      ":ttl".to_string(),
      AttributeValue::N(ttl_epoch_seconds.to_string()),
    );
    values.insert(
      ":planner_type".to_string(),
      AttributeValue::S(RECORD_TYPE_PLANNER_LEASE.to_string()),
    );

    let response = self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(Self::pk(topic, group_id)))
      .key(ATTR_SK, AttributeValue::S(Self::planner_key()))
      .update_expression(format!(
        "SET {ATTR_RECORD_TYPE} = :planner_type, {ATTR_OWNER} = :owner, {ATTR_LEASE_EXPIRES} = \
         :expires, {ATTR_LAST_HEARTBEAT} = :now, {ATTR_TTL} = :ttl"
      ))
      .condition_expression(format!(
        "attribute_not_exists({ATTR_PK}) OR {ATTR_LEASE_EXPIRES} <= :now OR {ATTR_OWNER} = :owner"
      ))
      .set_expression_attribute_values(Some(values))
      .send()
      .await;

    match response {
      Ok(_) => Ok(ConsumerGroupPlannerLeaseOutcome::Acquired),
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        Ok(ConsumerGroupPlannerLeaseOutcome::HeldByOther)
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn release_planner(&self, topic: &str, group_id: &str, member_id: &str) -> Result<bool> {
    let mut values = HashMap::new();
    values.insert(
      ":owner".to_string(),
      AttributeValue::S(member_id.to_string()),
    );
    let response = self
      .client
      .delete_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(Self::pk(topic, group_id)))
      .key(ATTR_SK, AttributeValue::S(Self::planner_key()))
      .condition_expression(format!("{ATTR_OWNER} = :owner"))
      .set_expression_attribute_values(Some(values))
      .send()
      .await;

    match response {
      Ok(_) => Ok(true),
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        Ok(false)
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn publish_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    plan: ConsumerGroupAssignmentPlan,
  ) -> Result<bool> {
    let mut planner_values = HashMap::new();
    planner_values.insert(
      ":owner".to_string(),
      AttributeValue::S(member_id.to_string()),
    );
    planner_values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    let plan_values = Self::plan_values(&plan);
    let group_key = Self::pk(topic, group_id);

    let planner_check = ConditionCheck::builder()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(group_key.clone()))
      .key(ATTR_SK, AttributeValue::S(Self::planner_key()))
      .condition_expression(format!(
        "{ATTR_OWNER} = :owner AND {ATTR_LEASE_EXPIRES} > :now"
      ))
      .set_expression_attribute_values(Some(planner_values))
      .build()?;
    let plan_update = Update::builder()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(group_key))
      .key(ATTR_SK, AttributeValue::S(Self::plan_key()))
      .update_expression(format!(
        "SET {ATTR_RECORD_TYPE} = :plan_type, {ATTR_PLAN_VERSION} = :version, {ATTR_PLAN_PLANNER} \
         = :planner, {ATTR_PLAN_MEMBERS} = :members, {ATTR_PLAN_ASSIGNMENTS} = :assignments, \
         {ATTR_PLAN_PUBLISHED} = :published"
      ))
      .set_expression_attribute_values(Some(plan_values))
      .build()?;
    let response = self
      .client
      .transact_write_items()
      .transact_items(
        TransactWriteItem::builder()
          .condition_check(planner_check)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().update(plan_update).build())
      .send()
      .await;

    match response {
      Ok(_) => Ok(true),
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_transaction_canceled_exception() =>
      {
        Ok(false)
      },
      Err(error) => Err(error.into()),
    }
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
