#[cfg(test)]
#[path = "./consumer_group_membership_dynamo_test.rs"]
mod tests;

use crate::aws::{is_dynamo_transaction_conflict, retry_dynamo_transaction_conflicts};
use crate::dynamo::duration_seconds_ceil;
use crate::{
  ConsumerGroupAssignment,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupMember,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLease,
  ConsumerGroupPlannerLeaseOutcome,
  DynamoCapacityMetrics,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::types::{
  AttributeValue,
  ConditionCheck,
  ReturnConsumedCapacity,
  TransactWriteItem,
  Update,
};
use blob_stream_types::unix_millis_from_offset_datetime;
use log::trace;
use std::collections::{HashMap, HashSet};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

const ATTR_PK: &str = "pk";
const ATTR_SK: &str = "sk";
const ATTR_LEASE_EXPIRES: &str = "lease_expiry_ts";
const ATTR_LAST_HEARTBEAT: &str = "last_heartbeat_ts";
const ATTR_POD_ID: &str = "pod_id";
const ATTR_TTL: &str = "ttl_epoch_seconds";
const ATTR_RECORD_TYPE: &str = "record_type";
const ATTR_OWNER: &str = "owner_id";
const ATTR_PLANNER_SESSION: &str = "planner_session_id";
const ATTR_PLAN_VERSION: &str = "plan_version";
const ATTR_PLAN_PLANNER: &str = "plan_planner_member_id";
const ATTR_PLAN_MEMBERS: &str = "plan_members";
const ATTR_PLAN_MEMBER_TOPOLOGY: &str = "plan_member_topology";
const ATTR_PLAN_ASSIGNMENTS: &str = "plan_assignments";
const ATTR_PLAN_PUBLISHED: &str = "plan_published_ts";
const ATTR_ASSIGNMENT_PARTITION: &str = "virtual_partition_id";
const ATTR_ASSIGNMENT_MEMBER: &str = "member_id";
const RECORD_TYPE_MEMBER: &str = "member";
const RECORD_TYPE_ASSIGNMENT_PLAN: &str = "assignment_plan";
const RECORD_TYPE_PLANNER_LEASE: &str = "assignment_planner_lease";
const ASSIGNMENT_CONTROL_PARTITION_PREFIX: &str = "__blob_stream_assignment_control_v1__";
const ASSIGNMENT_PLAN_SORT_KEY: &str = "__blob_stream_assignment_plan_v1__";
const PLANNER_LEASE_SORT_KEY: &str = "__blob_stream_assignment_planner_v1__";
//
// DynamoConsumerGroupMembershipStore
//

#[derive(Clone, Debug)]
pub struct DynamoConsumerGroupMembershipStore {
  client: Client,
  table_name: String,
  ttl_buffer: Duration,
  capacity_metrics: Option<DynamoCapacityMetrics>,
}

impl DynamoConsumerGroupMembershipStore {
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

  fn record_write_capacities(
    &self,
    consumed_capacities: &[aws_sdk_dynamodb::types::ConsumedCapacity],
  ) {
    if let Some(capacity_metrics) = &self.capacity_metrics {
      capacity_metrics.record_writes(consumed_capacities);
    }
  }

  fn pk(topic: &str, group_id: &str) -> String {
    format!("{topic}#{group_id}")
  }

  fn control_pk(topic: &str, group_id: &str) -> String {
    format!("{ASSIGNMENT_CONTROL_PARTITION_PREFIX}#{topic}#{group_id}")
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
    let member_topology = item
      .get(ATTR_PLAN_MEMBER_TOPOLOGY)
      .map(|value| {
        value
          .as_l()
          .map_err(|_| anyhow!("assignment plan member topology must be a list"))?
          .iter()
          .map(|value| {
            let member = value
              .as_m()
              .map_err(|_| anyhow!("assignment plan topology entry must be a map"))?;
            let member_id = member
              .get(ATTR_ASSIGNMENT_MEMBER)
              .and_then(|value| value.as_s().ok())
              .ok_or_else(|| anyhow!("assignment plan topology member missing"))?
              .clone();
            let pod_id = member
              .get(ATTR_POD_ID)
              .and_then(|value| value.as_s().ok())
              .ok_or_else(|| anyhow!("assignment plan topology pod missing"))?
              .clone();
            Ok(ConsumerGroupMember {
              member_id,
              pod_id: Some(pod_id),
            })
          })
          .collect::<Result<Vec<_>>>()
      })
      .transpose()?;

    Ok(ConsumerGroupAssignmentPlan {
      version,
      planner_member_id,
      members,
      member_topology,
      assignments,
      published_ts_ms,
    })
  }

  fn plan_values(plan: &ConsumerGroupAssignmentPlan) -> Result<HashMap<String, AttributeValue>> {
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
    if let Some(member_topology) = &plan.member_topology {
      let member_topology = member_topology
        .iter()
        .map(|member| {
          let pod_id = member.pod_id.as_ref().ok_or_else(|| {
            anyhow!(
              "pod-aware assignment topology missing pod ID for member {}",
              member.member_id
            )
          })?;
          let mut entry = HashMap::new();
          entry.insert(
            ATTR_ASSIGNMENT_MEMBER.to_string(),
            AttributeValue::S(member.member_id.clone()),
          );
          entry.insert(ATTR_POD_ID.to_string(), AttributeValue::S(pod_id.clone()));
          Ok(AttributeValue::M(entry))
        })
        .collect::<Result<Vec<_>>>()?;
      values.insert(
        ":member_topology".to_string(),
        AttributeValue::L(member_topology),
      );
    }
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
    Ok(values)
  }

  fn planner_lease_from_item(
    item: &HashMap<String, AttributeValue>,
  ) -> Result<ConsumerGroupPlannerLease> {
    let member_id = item
      .get(ATTR_OWNER)
      .and_then(|value| value.as_s().ok())
      .ok_or_else(|| anyhow!("assignment planner owner missing"))?
      .clone();
    let planner_session_id = item
      .get(ATTR_PLANNER_SESSION)
      .and_then(|value| value.as_s().ok())
      .ok_or_else(|| anyhow!("assignment planner session missing"))?
      .clone();
    let lease_expiration_ts_ms = item
      .get(ATTR_LEASE_EXPIRES)
      .and_then(|value| value.as_n().ok())
      .ok_or_else(|| anyhow!("assignment planner expiration missing"))?
      .parse()?;
    Ok(ConsumerGroupPlannerLease {
      member_id,
      planner_session_id,
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
    pod_id: Option<String>,
    now: OffsetDateTime,
    ttl: Duration,
  ) -> Result<()> {
    trace!(
      "consumer membership(dynamo) register: table={}, topic={}, group_id={}, member_id={}",
      self.table_name, topic, group_id, member_id
    );
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let ttl_ms = i64::try_from(ttl.whole_milliseconds())
      .map_err(|_| anyhow!("membership ttl exceeds Dynamo millisecond range"))?;
    let expires_at = expires_at(now_ts_ms, ttl_ms)?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at, self.ttl_buffer)?;

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
    if let Some(pod_id) = &pod_id {
      values.insert(":pod_id".to_string(), AttributeValue::S(pod_id.clone()));
    }
    let pod_id_expression = if pod_id.is_some() {
      format!(", {ATTR_POD_ID} = :pod_id")
    } else {
      String::new()
    };
    let pod_id_removal = if pod_id.is_some() {
      String::new()
    } else {
      format!(" REMOVE {ATTR_POD_ID}")
    };

    let response = self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(Self::pk(topic, group_id)))
      .key(ATTR_SK, AttributeValue::S(Self::sk(member_id)))
      .update_expression(format!(
        "SET {ATTR_LEASE_EXPIRES} = :expires, {ATTR_TTL} = :ttl, {ATTR_LAST_HEARTBEAT} = :now, \
         {ATTR_RECORD_TYPE} = :member_type{pod_id_expression}{pod_id_removal}"
      ))
      .set_expression_attribute_values(Some(values))
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await?;
    self.record_write_capacity(response.consumed_capacity.as_ref());

    Ok(())
  }

  async fn heartbeat_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    pod_id: Option<String>,
    now: OffsetDateTime,
    ttl: Duration,
  ) -> Result<()> {
    trace!(
      "consumer membership(dynamo) heartbeat: table={}, topic={}, group_id={}, member_id={}",
      self.table_name, topic, group_id, member_id
    );
    self
      .register_member(topic, group_id, member_id, pod_id, now, ttl)
      .await
  }

  async fn deregister_member(&self, topic: &str, group_id: &str, member_id: &str) -> Result<()> {
    trace!(
      "consumer membership(dynamo) deregister: table={}, topic={}, group_id={}, member_id={}",
      self.table_name, topic, group_id, member_id
    );

    let response = self
      .client
      .delete_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(Self::pk(topic, group_id)))
      .key(ATTR_SK, AttributeValue::S(Self::sk(member_id)))
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await?;
    self.record_write_capacity(response.consumed_capacity.as_ref());

    Ok(())
  }

  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now: OffsetDateTime,
  ) -> Result<Vec<ConsumerGroupMember>> {
    trace!(
      "consumer membership(dynamo) list_active_members: table={}, topic={}, group_id={}",
      self.table_name, topic, group_id
    );

    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
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
        .projection_expression(format!("{ATTR_SK}, {ATTR_POD_ID}"))
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
        if let Some(AttributeValue::S(member_id)) = item.get(ATTR_SK) {
          members.insert(ConsumerGroupMember {
            member_id: member_id.clone(),
            pod_id: item
              .get(ATTR_POD_ID)
              .and_then(|value| value.as_s().ok())
              .cloned(),
          });
        }
      }

      if let Some(key) = response.last_evaluated_key {
        start_key = Some(key);
      } else {
        break;
      }
    }

    let mut members = members.into_iter().collect::<Vec<_>>();
    members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
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
      .key(
        ATTR_PK,
        AttributeValue::S(Self::control_pk(topic, group_id)),
      )
      .key(ATTR_SK, AttributeValue::S(Self::plan_key()))
      .consistent_read(true)
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await?;
    self.record_read_capacity(response.consumed_capacity.as_ref());
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
      .key(
        ATTR_PK,
        AttributeValue::S(Self::control_pk(topic, group_id)),
      )
      .key(ATTR_SK, AttributeValue::S(Self::planner_key()))
      .consistent_read(true)
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await?;
    self.record_read_capacity(response.consumed_capacity.as_ref());
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
    planner_session_id: &str,
    now: OffsetDateTime,
    ttl: Duration,
  ) -> Result<ConsumerGroupPlannerLeaseOutcome> {
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let ttl_ms = i64::try_from(ttl.whole_milliseconds())
      .map_err(|_| anyhow!("planner ttl exceeds Dynamo millisecond range"))?;
    let expires_at = expires_at(now_ts_ms, ttl_ms)?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at, self.ttl_buffer)?;
    let mut values = HashMap::new();
    values.insert(
      ":owner".to_string(),
      AttributeValue::S(member_id.to_string()),
    );
    values.insert(
      ":session".to_string(),
      AttributeValue::S(planner_session_id.to_string()),
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

    let control_pk = Self::control_pk(topic, group_id);
    let response = retry_dynamo_transaction_conflicts(
      "acquire_or_renew",
      || {
        self
          .client
          .update_item()
          .table_name(&self.table_name)
          .key(ATTR_PK, AttributeValue::S(control_pk.clone()))
          .key(ATTR_SK, AttributeValue::S(Self::planner_key()))
          .update_expression(format!(
            "SET {ATTR_RECORD_TYPE} = :planner_type, {ATTR_OWNER} = :owner, \
             {ATTR_PLANNER_SESSION} = :session, {ATTR_LEASE_EXPIRES} = :expires, \
             {ATTR_LAST_HEARTBEAT} = :now, {ATTR_TTL} = :ttl"
          ))
          .condition_expression(format!(
            "attribute_not_exists({ATTR_PK}) OR {ATTR_LEASE_EXPIRES} <= :now OR ({ATTR_OWNER} = \
             :owner AND {ATTR_PLANNER_SESSION} = :session)"
          ))
          .set_expression_attribute_values(Some(values.clone()))
          .return_consumed_capacity(ReturnConsumedCapacity::Total)
          .send()
      },
      is_dynamo_transaction_conflict,
    )
    .await;

    match response {
      Ok(output) => {
        self.record_write_capacity(output.consumed_capacity.as_ref());
        Ok(ConsumerGroupPlannerLeaseOutcome::Acquired)
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        Ok(ConsumerGroupPlannerLeaseOutcome::HeldByOther)
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn release_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
  ) -> Result<bool> {
    let mut values = HashMap::new();
    values.insert(
      ":owner".to_string(),
      AttributeValue::S(member_id.to_string()),
    );
    values.insert(
      ":session".to_string(),
      AttributeValue::S(planner_session_id.to_string()),
    );
    let control_pk = Self::control_pk(topic, group_id);
    let response = retry_dynamo_transaction_conflicts(
      "release",
      || {
        self
          .client
          .delete_item()
          .table_name(&self.table_name)
          .key(ATTR_PK, AttributeValue::S(control_pk.clone()))
          .key(ATTR_SK, AttributeValue::S(Self::planner_key()))
          .condition_expression(format!(
            "{ATTR_OWNER} = :owner AND {ATTR_PLANNER_SESSION} = :session"
          ))
          .set_expression_attribute_values(Some(values.clone()))
          .return_consumed_capacity(ReturnConsumedCapacity::Total)
          .send()
      },
      is_dynamo_transaction_conflict,
    )
    .await;

    match response {
      Ok(output) => {
        self.record_write_capacity(output.consumed_capacity.as_ref());
        Ok(true)
      },
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
    planner_session_id: &str,
    now: OffsetDateTime,
    plan: ConsumerGroupAssignmentPlan,
  ) -> Result<bool> {
    if plan.planner_member_id != member_id {
      return Ok(false);
    }

    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let mut planner_values = HashMap::new();
    planner_values.insert(
      ":owner".to_string(),
      AttributeValue::S(member_id.to_string()),
    );
    planner_values.insert(
      ":session".to_string(),
      AttributeValue::S(planner_session_id.to_string()),
    );
    planner_values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    let plan_values = Self::plan_values(&plan)?;
    let group_key = Self::control_pk(topic, group_id);

    let planner_check = ConditionCheck::builder()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(group_key.clone()))
      .key(ATTR_SK, AttributeValue::S(Self::planner_key()))
      .condition_expression(format!(
        "{ATTR_OWNER} = :owner AND {ATTR_PLANNER_SESSION} = :session AND {ATTR_LEASE_EXPIRES} > \
         :now"
      ))
      .set_expression_attribute_values(Some(planner_values))
      .build()?;
    let topology_update = if plan.member_topology.is_some() {
      format!(", {ATTR_PLAN_MEMBER_TOPOLOGY} = :member_topology")
    } else {
      String::new()
    };
    let topology_removal = if plan.member_topology.is_some() {
      String::new()
    } else {
      format!(" REMOVE {ATTR_PLAN_MEMBER_TOPOLOGY}")
    };
    let plan_update = Update::builder()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(group_key))
      .key(ATTR_SK, AttributeValue::S(Self::plan_key()))
      .update_expression(format!(
        "SET {ATTR_RECORD_TYPE} = :plan_type, {ATTR_PLAN_VERSION} = :version, {ATTR_PLAN_PLANNER} \
         = :planner, {ATTR_PLAN_MEMBERS} = :members, {ATTR_PLAN_ASSIGNMENTS} = :assignments, \
         {ATTR_PLAN_PUBLISHED} = :published{topology_update}{topology_removal}"
      ))
      .set_expression_attribute_values(Some(plan_values))
      .build()?;
    let client_request_token = Uuid::new_v4().to_string();
    let response = self
      .client
      .transact_write_items()
      .client_request_token(client_request_token)
      .transact_items(
        TransactWriteItem::builder()
          .condition_check(planner_check)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().update(plan_update).build())
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await;

    match response {
      Ok(output) => {
        self.record_write_capacities(output.consumed_capacity.as_deref().unwrap_or_default());
        Ok(true)
      },
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

fn ttl_epoch_seconds(expires_at_ms: i64, ttl_buffer: Duration) -> Result<i64> {
  let expires_at_seconds = expires_at_ms
    .checked_div(1_000)
    .ok_or_else(|| anyhow!("membership ttl conversion overflow"))?;
  let ttl_buffer_seconds = duration_seconds_ceil(ttl_buffer)
    .ok_or_else(|| anyhow!("membership ttl buffer conversion overflow"))?;

  expires_at_seconds
    .checked_add(ttl_buffer_seconds)
    .ok_or_else(|| anyhow!("membership ttl overflow"))
}
