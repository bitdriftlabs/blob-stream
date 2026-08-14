#[cfg(test)]
#[path = "./consumer_group_membership_memory_test.rs"]
mod tests;

use crate::{
  ConsumerGroupAssignmentPlan,
  ConsumerGroupMember,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLease,
  ConsumerGroupPlannerLeaseOutcome,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_types::unix_millis_from_offset_datetime;
use log::trace;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use time::{Duration, OffsetDateTime};

//
// InMemoryConsumerGroupMembershipStore
//

#[derive(Default)]
pub struct InMemoryConsumerGroupMembershipStore {
  state: RwLock<MembershipState>,
}

impl InMemoryConsumerGroupMembershipStore {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }
}

#[async_trait]
impl ConsumerGroupMembershipStore for InMemoryConsumerGroupMembershipStore {
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
      "consumer membership(memory) register: topic={topic}, group_id={group_id}, \
       member_id={member_id}"
    );
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds in-memory millisecond range"))?;
    let ttl_ms = i64::try_from(ttl.whole_milliseconds())
      .map_err(|_| anyhow!("membership ttl exceeds in-memory millisecond range"))?;
    let expires_at = expires_at(now_ts_ms, ttl_ms)?;
    let key = MemberKey::new(topic, group_id, member_id);
    let state = MemberState {
      lease_expiration_ts_ms: expires_at,
      pod_id,
    };
    self.state.write().members.insert(key, state);
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
      "consumer membership(memory) heartbeat: topic={topic}, group_id={group_id}, \
       member_id={member_id}"
    );
    self
      .register_member(topic, group_id, member_id, pod_id, now, ttl)
      .await
  }

  async fn deregister_member(&self, topic: &str, group_id: &str, member_id: &str) -> Result<()> {
    trace!(
      "consumer membership(memory) deregister: topic={topic}, group_id={group_id}, \
       member_id={member_id}"
    );
    let key = MemberKey::new(topic, group_id, member_id);
    self.state.write().members.remove(&key);
    Ok(())
  }

  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now: OffsetDateTime,
  ) -> Result<Vec<ConsumerGroupMember>> {
    trace!("consumer membership(memory) list_active_members: topic={topic}, group_id={group_id}");
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds in-memory millisecond range"))?;
    let guard = self.state.read();
    let mut members = HashSet::new();
    for (key, state) in &guard.members {
      if key.topic == topic && key.group_id == group_id && state.lease_expiration_ts_ms > now_ts_ms
      {
        members.insert(ConsumerGroupMember {
          member_id: key.member_id.clone(),
          pod_id: state.pod_id.clone(),
        });
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
    Ok(
      self
        .state
        .read()
        .plans
        .get(&GroupKey::new(topic, group_id))
        .cloned(),
    )
  }

  async fn get_planner_lease(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Option<ConsumerGroupPlannerLease>> {
    Ok(
      self
        .state
        .read()
        .planners
        .get(&GroupKey::new(topic, group_id))
        .map(|planner| ConsumerGroupPlannerLease {
          member_id: planner.member_id.clone(),
          planner_session_id: planner.planner_session_id.clone(),
          lease_expiration_ts_ms: planner.lease_expiration_ts_ms,
        }),
    )
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
      .map_err(|_| anyhow!("current time exceeds in-memory millisecond range"))?;
    let ttl_ms = i64::try_from(ttl.whole_milliseconds())
      .map_err(|_| anyhow!("planner ttl exceeds in-memory millisecond range"))?;
    let expires_at = expires_at(now_ts_ms, ttl_ms)?;
    let group_key = GroupKey::new(topic, group_id);
    let mut state = self.state.write();

    match state.planners.get(&group_key) {
      Some(planner)
        if planner.lease_expiration_ts_ms > now_ts_ms
          && (planner.member_id != member_id
            || planner.planner_session_id != planner_session_id) =>
      {
        Ok(ConsumerGroupPlannerLeaseOutcome::HeldByOther)
      },
      _ => {
        state.planners.insert(
          group_key,
          PlannerState {
            member_id: member_id.to_string(),
            planner_session_id: planner_session_id.to_string(),
            lease_expiration_ts_ms: expires_at,
          },
        );
        Ok(ConsumerGroupPlannerLeaseOutcome::Acquired)
      },
    }
  }

  async fn release_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
  ) -> Result<bool> {
    let group_key = GroupKey::new(topic, group_id);
    let mut state = self.state.write();
    if state.planners.get(&group_key).is_none_or(|planner| {
      planner.member_id != member_id || planner.planner_session_id != planner_session_id
    }) {
      return Ok(false);
    }

    state.planners.remove(&group_key);
    Ok(true)
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
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds in-memory millisecond range"))?;
    let group_key = GroupKey::new(topic, group_id);
    let mut state = self.state.write();
    let Some(planner) = state.planners.get(&group_key) else {
      return Ok(false);
    };

    if planner.member_id != member_id
      || planner.planner_session_id != planner_session_id
      || planner.lease_expiration_ts_ms <= now_ts_ms
      || plan.planner_member_id != member_id
    {
      return Ok(false);
    }

    state.plans.insert(group_key, plan);
    Ok(true)
  }
}

//
// MembershipState
//

#[derive(Default)]
struct MembershipState {
  members: HashMap<MemberKey, MemberState>,
  planners: HashMap<GroupKey, PlannerState>,
  plans: HashMap<GroupKey, ConsumerGroupAssignmentPlan>,
}

//
// GroupKey
//

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct GroupKey {
  topic: String,
  group_id: String,
}

impl GroupKey {
  fn new(topic: &str, group_id: &str) -> Self {
    Self {
      topic: topic.to_string(),
      group_id: group_id.to_string(),
    }
  }
}

//
// MemberKey
//

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct MemberKey {
  topic: String,
  group_id: String,
  member_id: String,
}

impl MemberKey {
  fn new(topic: &str, group_id: &str, member_id: &str) -> Self {
    Self {
      topic: topic.to_string(),
      group_id: group_id.to_string(),
      member_id: member_id.to_string(),
    }
  }
}

//
// MemberState
//

#[derive(Clone, Debug)]
struct MemberState {
  lease_expiration_ts_ms: i64,
  pod_id: Option<String>,
}

//
// PlannerState
//

#[derive(Clone, Debug)]
struct PlannerState {
  member_id: String,
  planner_session_id: String,
  lease_expiration_ts_ms: i64,
}

fn expires_at(now_ts_ms: i64, ttl_ms: i64) -> Result<i64> {
  if ttl_ms <= 0 {
    return Err(anyhow!("membership ttl must be greater than zero"));
  }

  now_ts_ms
    .checked_add(ttl_ms)
    .ok_or_else(|| anyhow!("membership expiration overflow"))
}
