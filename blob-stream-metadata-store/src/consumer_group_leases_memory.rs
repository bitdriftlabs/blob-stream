#[cfg(test)]
#[path = "./consumer_group_leases_memory_test.rs"]
mod tests;

use crate::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupReleaseOutcome,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_types::CommittedCursor;
use log::trace;
use std::collections::HashMap;
use tokio::sync::RwLock;

//
// InMemoryConsumerGroupLeaseStore
//

#[derive(Debug, Default)]
pub struct InMemoryConsumerGroupLeaseStore {
  leases: RwLock<HashMap<ConsumerGroupLeaseKey, LeaseState>>,
}

impl InMemoryConsumerGroupLeaseStore {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }
}

#[async_trait]
impl ConsumerGroupLeaseStore for InMemoryConsumerGroupLeaseStore {
  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<ConsumerGroupAssignmentOutcome> {
    trace!(
      "consumer lease(memory) assign: topic={}, group_id={}, partition={}, owner_id={}, \
       generation={}",
      key.topic, key.group_id, key.virtual_partition_id, owner_id, generation
    );
    let mut guard = self.leases.write().await;
    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;

    match guard.get_mut(&key) {
      None => {
        let lease = LeaseState {
          owner_id,
          generation,
          lease_expiration_ts_ms: expires_at,
          last_heartbeat_ts_ms: now_ts_ms,
          committed_cursor: None,
          committed_ts_ms: None,
        };
        guard.insert(key.clone(), lease.clone());
        Ok(ConsumerGroupAssignmentOutcome::Assigned(
          lease.to_lease(key),
        ))
      },
      Some(state) => {
        if state.is_expired(now_ts_ms) {
          state.owner_id = owner_id;
          state.generation = generation;
          state.lease_expiration_ts_ms = expires_at;
          state.last_heartbeat_ts_ms = now_ts_ms;
          return Ok(ConsumerGroupAssignmentOutcome::Assigned(
            state.to_lease(key),
          ));
        }

        if state.owner_id == owner_id {
          if generation < state.generation {
            return Ok(ConsumerGroupAssignmentOutcome::HeldByOther(
              state.to_lease(key),
            ));
          }

          state.generation = generation;
          state.lease_expiration_ts_ms = expires_at;
          state.last_heartbeat_ts_ms = now_ts_ms;
          return Ok(ConsumerGroupAssignmentOutcome::Assigned(
            state.to_lease(key),
          ));
        }

        Ok(ConsumerGroupAssignmentOutcome::HeldByOther(
          state.to_lease(key),
        ))
      },
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
    trace!(
      "consumer lease(memory) heartbeat: topic={}, group_id={}, partition={}, owner_id={}, \
       generation={}",
      key.topic, key.group_id, key.virtual_partition_id, owner_id, generation
    );
    if let Some(cursor) = committed_cursor.as_ref() {
      validate_cursor(key, cursor)?;
    }

    let mut guard = self.leases.write().await;
    let Some(state) = guard.get_mut(key) else {
      return Ok(ConsumerGroupHeartbeatOutcome::Expired);
    };

    if state.is_expired(now_ts_ms) {
      return Ok(ConsumerGroupHeartbeatOutcome::Expired);
    }

    if state.owner_id != owner_id || state.generation != generation {
      return Ok(ConsumerGroupHeartbeatOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    state.lease_expiration_ts_ms = expires_at(now_ts_ms, lease_duration_ms)?;
    state.last_heartbeat_ts_ms = now_ts_ms;

    if let Some(cursor) = committed_cursor {
      state.committed_cursor = Some(cursor);
      state.committed_ts_ms = Some(now_ts_ms);
    }

    Ok(ConsumerGroupHeartbeatOutcome::Renewed(
      state.to_lease(key.clone()),
    ))
  }

  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    committed_cursor: CommittedCursor,
  ) -> Result<ConsumerGroupCommitOutcome> {
    trace!(
      "consumer lease(memory) commit: topic={}, group_id={}, partition={}, owner_id={}, \
       generation={}, seq_end={}",
      key.topic,
      key.group_id,
      key.virtual_partition_id,
      owner_id,
      generation,
      committed_cursor.seq_end
    );
    validate_cursor(key, &committed_cursor)?;

    let mut guard = self.leases.write().await;
    let Some(state) = guard.get_mut(key) else {
      return Ok(ConsumerGroupCommitOutcome::Expired);
    };

    if state.is_expired(now_ts_ms) {
      return Ok(ConsumerGroupCommitOutcome::Expired);
    }

    if state.owner_id != owner_id || state.generation != generation {
      return Ok(ConsumerGroupCommitOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    state.committed_cursor = Some(committed_cursor);
    state.committed_ts_ms = Some(now_ts_ms);

    Ok(ConsumerGroupCommitOutcome::Committed(
      state.to_lease(key.clone()),
    ))
  }

  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
  ) -> Result<ConsumerGroupReleaseOutcome> {
    trace!(
      "consumer lease(memory) release: topic={}, group_id={}, partition={}, owner_id={}, \
       generation={}",
      key.topic, key.group_id, key.virtual_partition_id, owner_id, generation
    );

    let mut guard = self.leases.write().await;
    let Some(state) = guard.get_mut(key) else {
      return Ok(ConsumerGroupReleaseOutcome::Expired);
    };

    if state.is_expired(now_ts_ms) {
      return Ok(ConsumerGroupReleaseOutcome::Expired);
    }

    if state.owner_id != owner_id || state.generation != generation {
      return Ok(ConsumerGroupReleaseOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    // Expire in place so the committed cursor remains available for the next owner.
    state.lease_expiration_ts_ms = now_ts_ms;
    state.last_heartbeat_ts_ms = now_ts_ms;
    Ok(ConsumerGroupReleaseOutcome::Released)
  }
}

//
// LeaseState
//

#[derive(Clone, Debug)]
struct LeaseState {
  owner_id: String,
  generation: u64,
  lease_expiration_ts_ms: i64,
  last_heartbeat_ts_ms: i64,
  committed_cursor: Option<CommittedCursor>,
  committed_ts_ms: Option<i64>,
}

impl LeaseState {
  fn is_expired(&self, now_ts_ms: i64) -> bool {
    now_ts_ms >= self.lease_expiration_ts_ms
  }

  fn to_lease(&self, key: ConsumerGroupLeaseKey) -> ConsumerGroupLease {
    ConsumerGroupLease {
      key,
      owner_id: self.owner_id.clone(),
      generation: self.generation,
      lease_expiration_ts_ms: self.lease_expiration_ts_ms,
      last_heartbeat_ts_ms: self.last_heartbeat_ts_ms,
      committed_cursor: self.committed_cursor.clone(),
      committed_ts_ms: self.committed_ts_ms,
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
