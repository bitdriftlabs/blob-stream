#[cfg(test)]
#[path = "./consumer_group_membership_memory_test.rs"]
mod tests;

use crate::{
  ConsumerGroupAssignmentPlan,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLease,
  ConsumerGroupPlannerLeaseOutcome,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use log::trace;
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;

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
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> Result<()> {
    trace!(
      "consumer membership(memory) register: topic={topic}, group_id={group_id}, \
       member_id={member_id}"
    );
    let expires_at = expires_at(now_ts_ms, ttl_ms)?;
    let key = MemberKey::new(topic, group_id, member_id);
    let state = MemberState {
      lease_expiration_ts_ms: expires_at,
    };
    self.state.write().await.members.insert(key, state);
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
      "consumer membership(memory) heartbeat: topic={topic}, group_id={group_id}, \
       member_id={member_id}"
    );
    self
      .register_member(topic, group_id, member_id, now_ts_ms, ttl_ms)
      .await
  }

  async fn deregister_member(&self, topic: &str, group_id: &str, member_id: &str) -> Result<()> {
    trace!(
      "consumer membership(memory) deregister: topic={topic}, group_id={group_id}, \
       member_id={member_id}"
    );
    let key = MemberKey::new(topic, group_id, member_id);
    self.state.write().await.members.remove(&key);
    Ok(())
  }

  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now_ts_ms: i64,
  ) -> Result<Vec<String>> {
    trace!("consumer membership(memory) list_active_members: topic={topic}, group_id={group_id}");
    let guard = self.state.read().await;
    let mut members = HashSet::new();
    for (key, state) in &guard.members {
      if key.topic == topic && key.group_id == group_id && state.lease_expiration_ts_ms > now_ts_ms
      {
        members.insert(key.member_id.clone());
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
    Ok(
      self
        .state
        .read()
        .await
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
        .await
        .planners
        .get(&GroupKey::new(topic, group_id))
        .map(|planner| ConsumerGroupPlannerLease {
          member_id: planner.member_id.clone(),
          lease_expiration_ts_ms: planner.lease_expiration_ts_ms,
        }),
    )
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
    let group_key = GroupKey::new(topic, group_id);
    let mut state = self.state.write().await;

    match state.planners.get(&group_key) {
      Some(planner)
        if planner.lease_expiration_ts_ms > now_ts_ms && planner.member_id != member_id =>
      {
        Ok(ConsumerGroupPlannerLeaseOutcome::HeldByOther)
      },
      _ => {
        state.planners.insert(
          group_key,
          PlannerState {
            member_id: member_id.to_string(),
            lease_expiration_ts_ms: expires_at,
          },
        );
        Ok(ConsumerGroupPlannerLeaseOutcome::Acquired)
      },
    }
  }

  async fn release_planner(&self, topic: &str, group_id: &str, member_id: &str) -> Result<bool> {
    let group_key = GroupKey::new(topic, group_id);
    let mut state = self.state.write().await;
    if state
      .planners
      .get(&group_key)
      .is_none_or(|planner| planner.member_id != member_id)
    {
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
    now_ts_ms: i64,
    plan: ConsumerGroupAssignmentPlan,
  ) -> Result<bool> {
    let group_key = GroupKey::new(topic, group_id);
    let mut state = self.state.write().await;
    let Some(planner) = state.planners.get(&group_key) else {
      return Ok(false);
    };

    if planner.member_id != member_id || planner.lease_expiration_ts_ms <= now_ts_ms {
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

#[derive(Clone, Copy, Debug)]
struct MemberState {
  lease_expiration_ts_ms: i64,
}

//
// PlannerState
//

#[derive(Clone, Debug)]
struct PlannerState {
  member_id: String,
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
