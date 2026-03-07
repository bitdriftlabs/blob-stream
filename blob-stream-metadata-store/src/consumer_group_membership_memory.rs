#[cfg(test)]
#[path = "./consumer_group_membership_memory_test.rs"]
mod tests;

use crate::ConsumerGroupMembershipStore;
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
  members: RwLock<HashMap<MemberKey, MemberState>>,
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
    self.members.write().await.insert(key, state);
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
    self.members.write().await.remove(&key);
    Ok(())
  }

  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now_ts_ms: i64,
  ) -> Result<Vec<String>> {
    trace!("consumer membership(memory) list_active_members: topic={topic}, group_id={group_id}");
    let guard = self.members.read().await;
    let mut members = HashSet::new();
    for (key, state) in guard.iter() {
      if key.topic == topic && key.group_id == group_id && state.lease_expiration_ts_ms > now_ts_ms
      {
        members.insert(key.member_id.clone());
      }
    }

    let mut members = members.into_iter().collect::<Vec<_>>();
    members.sort();
    Ok(members)
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

fn expires_at(now_ts_ms: i64, ttl_ms: i64) -> Result<i64> {
  if ttl_ms <= 0 {
    return Err(anyhow!("membership ttl must be greater than zero"));
  }

  now_ts_ms
    .checked_add(ttl_ms)
    .ok_or_else(|| anyhow!("membership expiration overflow"))
}
