#[cfg(test)]
#[path = "./consumer_group_leases_memory_test.rs"]
mod tests;

use crate::{
  ConsumerGroupArmFreshStartOutcome,
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeasePredecessor,
  ConsumerGroupLeaseStore,
  ConsumerGroupLeaseTransition,
  ConsumerGroupReleaseOutcome,
  FreshStartMarker,
  consumer_group_lease_transition,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_types::{CommittedCursor, unix_millis_from_offset_datetime};
use log::trace;
use parking_lot::RwLock;
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};

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
  async fn list_group_leases(
    &self,
    topic: &str,
    group_id: &str,
  ) -> Result<Vec<ConsumerGroupLease>> {
    let guard = self.leases.read();
    let mut leases = guard
      .iter()
      .filter(|(key, _)| key.topic == topic && key.group_id == group_id)
      .map(|(key, state)| state.to_lease(key.clone()))
      .collect::<Vec<_>>();
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
      "consumer lease(memory) assign: topic={}, group_id={}, partition={}, owner_id={}, \
       generation={}",
      key.topic, key.group_id, key.virtual_partition_id, owner_id, generation
    );
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds in-memory millisecond range"))?;
    let lease_duration_ms = i64::try_from(lease_duration.whole_milliseconds())
      .map_err(|_| anyhow!("lease duration exceeds in-memory millisecond range"))?;
    let mut guard = self.leases.write();
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
          fresh_start_marker: None,
          graceful_release_ts_ms: None,
        };
        guard.insert(key.clone(), lease.clone());
        Ok(ConsumerGroupAssignmentOutcome::Assigned {
          lease: lease.to_lease(key),
          previous_lease: None,
          transition: ConsumerGroupLeaseTransition::Initial,
        })
      },
      Some(state) => {
        if state.is_expired(now_ts_ms) {
          let previous = state.clone();
          let transition = consumer_group_lease_transition(
            Some(ConsumerGroupLeasePredecessor {
              owner_id: &previous.owner_id,
              generation: previous.generation,
              last_heartbeat_ts_ms: previous.last_heartbeat_ts_ms,
              graceful_release_ts_ms: previous.graceful_release_ts_ms,
            }),
            &owner_id,
          );
          state.owner_id = owner_id;
          state.generation = generation;
          state.lease_expiration_ts_ms = expires_at;
          state.last_heartbeat_ts_ms = now_ts_ms;
          state.graceful_release_ts_ms = None;
          return Ok(ConsumerGroupAssignmentOutcome::Assigned {
            lease: state.to_lease(key.clone()),
            previous_lease: Some(Box::new(previous.to_lease(key))),
            transition,
          });
        }

        if state.owner_id == owner_id {
          if generation < state.generation {
            return Ok(ConsumerGroupAssignmentOutcome::HeldByOther(
              state.to_lease(key),
            ));
          }

          let previous = state.clone();
          state.generation = generation;
          state.lease_expiration_ts_ms = expires_at;
          state.last_heartbeat_ts_ms = now_ts_ms;
          state.graceful_release_ts_ms = None;
          return Ok(ConsumerGroupAssignmentOutcome::Assigned {
            lease: state.to_lease(key.clone()),
            previous_lease: Some(Box::new(previous.to_lease(key))),
            transition: ConsumerGroupLeaseTransition::Retained,
          });
        }

        Ok(ConsumerGroupAssignmentOutcome::HeldByOther(
          state.to_lease(key),
        ))
      },
    }
  }

  async fn arm_next_window_fresh_start(
    &self,
    key: &ConsumerGroupLeaseKey,
    metadata_window_size: Duration,
    marker_id: String,
    now: OffsetDateTime,
  ) -> Result<ConsumerGroupArmFreshStartOutcome> {
    let metadata_window_seconds = metadata_window_size.whole_seconds();
    if metadata_window_seconds <= 0 {
      return Err(anyhow!("metadata window size must be positive"));
    }
    let armed_at_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds in-memory millisecond range"))?;
    let mut guard = self.leases.write();
    let Some(state) = guard.get_mut(key) else {
      return Ok(ConsumerGroupArmFreshStartOutcome::MissingLease);
    };
    if state.fresh_start_marker.is_some() {
      return Ok(ConsumerGroupArmFreshStartOutcome::AlreadyArmed(
        state.to_lease(key.clone()),
      ));
    }
    let Some(source_checkpoint) = state
      .committed_cursor
      .as_ref()
      .and_then(|cursor| cursor.source_checkpoint.clone())
    else {
      return Ok(ConsumerGroupArmFreshStartOutcome::MissingSourceCheckpoint(
        state.to_lease(key.clone()),
      ));
    };
    let target_window_start_unix_seconds = source_checkpoint
      .window_start_unix_seconds
      .checked_add(metadata_window_seconds)
      .ok_or_else(|| anyhow!("fresh start target window overflow"))?;
    state.fresh_start_marker = Some(FreshStartMarker {
      marker_id,
      source_checkpoint,
      target_window_start_unix_seconds,
      armed_at_ts_ms,
    });
    Ok(ConsumerGroupArmFreshStartOutcome::Armed(
      state.to_lease(key.clone()),
    ))
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
    self
      .heartbeat_partition_consuming_fresh_start_marker(
        key,
        owner_id,
        generation,
        now,
        lease_duration,
        committed_cursor,
        None,
      )
      .await
  }

  async fn heartbeat_partition_consuming_fresh_start_marker(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: Duration,
    committed_cursor: Option<CommittedCursor>,
    consumed_fresh_start_marker_id: Option<String>,
  ) -> Result<ConsumerGroupHeartbeatOutcome> {
    trace!(
      "consumer lease(memory) heartbeat: topic={}, group_id={}, partition={}, owner_id={}, \
       generation={}",
      key.topic, key.group_id, key.virtual_partition_id, owner_id, generation
    );
    if let Some(cursor) = committed_cursor.as_ref() {
      validate_cursor(key, cursor)?;
    }

    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds in-memory millisecond range"))?;
    let lease_duration_ms = i64::try_from(lease_duration.whole_milliseconds())
      .map_err(|_| anyhow!("lease duration exceeds in-memory millisecond range"))?;
    let mut guard = self.leases.write();
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

    let marker_matches = consumed_fresh_start_marker_id
      .as_ref()
      .is_none_or(|marker_id| {
        committed_cursor.is_some()
          && state
            .fresh_start_marker
            .as_ref()
            .is_some_and(|marker| marker.marker_id == *marker_id)
      });
    if !marker_matches {
      return Err(anyhow!("fresh start marker changed before cursor commit"));
    }

    state.lease_expiration_ts_ms = expires_at(now_ts_ms, lease_duration_ms)?;
    state.last_heartbeat_ts_ms = now_ts_ms;

    if let Some(cursor) = committed_cursor {
      if consumed_fresh_start_marker_id.is_some() {
        state.fresh_start_marker = None;
      }
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
    now: OffsetDateTime,
    committed_cursor: CommittedCursor,
  ) -> Result<ConsumerGroupCommitOutcome> {
    self
      .commit_cursor_consuming_fresh_start_marker(
        key,
        owner_id,
        generation,
        now,
        committed_cursor,
        None,
      )
      .await
  }

  async fn commit_cursor_consuming_fresh_start_marker(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    committed_cursor: CommittedCursor,
    consumed_fresh_start_marker_id: Option<String>,
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

    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds in-memory millisecond range"))?;
    let mut guard = self.leases.write();
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

    if let Some(marker_id) = consumed_fresh_start_marker_id {
      let marker_matches = state
        .fresh_start_marker
        .as_ref()
        .is_some_and(|marker| marker.marker_id == marker_id);
      if !marker_matches {
        return Err(anyhow!("fresh start marker changed before cursor commit"));
      }
      state.fresh_start_marker = None;
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
    now: OffsetDateTime,
  ) -> Result<ConsumerGroupReleaseOutcome> {
    trace!(
      "consumer lease(memory) release: topic={}, group_id={}, partition={}, owner_id={}, \
       generation={}",
      key.topic, key.group_id, key.virtual_partition_id, owner_id, generation
    );

    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds in-memory millisecond range"))?;
    let mut guard = self.leases.write();
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
    state.graceful_release_ts_ms = Some(now_ts_ms);
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
  fresh_start_marker: Option<FreshStartMarker>,
  graceful_release_ts_ms: Option<i64>,
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
      fresh_start_marker: self.fresh_start_marker.clone(),
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
