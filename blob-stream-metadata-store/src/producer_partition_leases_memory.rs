#[cfg(test)]
#[path = "./producer_partition_leases_memory_test.rs"]
mod tests;

use crate::{
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  ProducerPartitionLease,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SequenceReservation,
  SequenceReservationOutcome,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_types::SeqRange;
use log::trace;
use std::collections::HashMap;
use tokio::sync::RwLock;

//
// InMemoryProducerPartitionLeaseStore
//

#[derive(Debug, Default)]
pub struct InMemoryProducerPartitionLeaseStore {
  leases: RwLock<HashMap<ProducerPartitionLeaseKey, LeaseState>>,
}

impl InMemoryProducerPartitionLeaseStore {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }
}

#[async_trait]
impl ProducerPartitionLeaseStore for InMemoryProducerPartitionLeaseStore {
  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseAcquireOutcome> {
    trace!(
      "producer lease(memory) acquire: topic={}, writer_id={}, partition={}, holder_id={}",
      key.topic, key.writer_id, key.virtual_partition_id, holder_id
    );
    let mut guard = self.leases.write().await;
    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;

    match guard.get_mut(&key) {
      None => {
        let lease = LeaseState {
          holder_id,
          lease_expiration_ts_ms: expires_at,
          max_allocated_seq: None,
        };
        guard.insert(key.clone(), lease.clone());
        Ok(LeaseAcquireOutcome::Acquired(lease.to_lease(key)))
      },
      Some(state) => {
        if state.is_expired(now_ts_ms) {
          state.holder_id = holder_id;
          state.lease_expiration_ts_ms = expires_at;
          Ok(LeaseAcquireOutcome::Acquired(state.to_lease(key)))
        } else if state.holder_id == holder_id {
          state.lease_expiration_ts_ms = expires_at;
          Ok(LeaseAcquireOutcome::Acquired(state.to_lease(key)))
        } else {
          Ok(LeaseAcquireOutcome::HeldByOther(state.to_lease(key)))
        }
      },
    }
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseHeartbeatOutcome> {
    trace!(
      "producer lease(memory) heartbeat: topic={}, writer_id={}, partition={}, holder_id={}",
      key.topic, key.writer_id, key.virtual_partition_id, holder_id
    );
    let mut guard = self.leases.write().await;
    let Some(state) = guard.get_mut(key) else {
      return Ok(LeaseHeartbeatOutcome::Expired);
    };

    if state.is_expired(now_ts_ms) {
      return Ok(LeaseHeartbeatOutcome::Expired);
    }

    if state.holder_id != holder_id {
      return Ok(LeaseHeartbeatOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    state.lease_expiration_ts_ms = expires_at(now_ts_ms, lease_duration_ms)?;
    Ok(LeaseHeartbeatOutcome::Renewed(state.to_lease(key.clone())))
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome> {
    trace!(
      "producer lease(memory) reserve: topic={}, writer_id={}, partition={}, holder_id={}, size={}",
      key.topic, key.writer_id, key.virtual_partition_id, holder_id, reservation_size
    );
    if reservation_size == 0 {
      return Err(anyhow!("reservation_size must be greater than zero"));
    }

    let mut guard = self.leases.write().await;
    let Some(state) = guard.get_mut(key) else {
      return Ok(SequenceReservationOutcome::Expired);
    };

    if state.is_expired(now_ts_ms) {
      return Ok(SequenceReservationOutcome::Expired);
    }

    if state.holder_id != holder_id {
      return Ok(SequenceReservationOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    let (range, updated) = reserve_range(state.max_allocated_seq, reservation_size)?;
    state.max_allocated_seq = Some(updated);

    Ok(SequenceReservationOutcome::Reserved(SequenceReservation {
      range,
      lease: state.to_lease(key.clone()),
    }))
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
  ) -> Result<LeaseReleaseOutcome> {
    trace!(
      "producer lease(memory) release: topic={}, writer_id={}, partition={}, holder_id={}",
      key.topic, key.writer_id, key.virtual_partition_id, holder_id
    );
    let mut guard = self.leases.write().await;
    let Some(state) = guard.get(key).cloned() else {
      return Ok(LeaseReleaseOutcome::Expired);
    };

    if state.is_expired(now_ts_ms) {
      guard.remove(key);
      return Ok(LeaseReleaseOutcome::Expired);
    }

    if state.holder_id != holder_id {
      return Ok(LeaseReleaseOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    guard.remove(key);
    Ok(LeaseReleaseOutcome::Released)
  }
}

//
// LeaseState
//

#[derive(Clone, Debug)]
struct LeaseState {
  holder_id: String,
  lease_expiration_ts_ms: i64,
  max_allocated_seq: Option<u64>,
}

impl LeaseState {
  fn is_expired(&self, now_ts_ms: i64) -> bool {
    now_ts_ms >= self.lease_expiration_ts_ms
  }

  fn to_lease(&self, key: ProducerPartitionLeaseKey) -> ProducerPartitionLease {
    ProducerPartitionLease {
      key,
      holder_id: self.holder_id.clone(),
      lease_expiration_ts_ms: self.lease_expiration_ts_ms,
      max_allocated_seq: self.max_allocated_seq,
    }
  }
}

fn reserve_range(current: Option<u64>, reservation_size: u64) -> Result<(SeqRange, u64)> {
  let start = match current {
    Some(value) => value
      .checked_add(1)
      .ok_or_else(|| anyhow!("sequence range overflow"))?,
    None => 0,
  };
  let end = start
    .checked_add(reservation_size.saturating_sub(1))
    .ok_or_else(|| anyhow!("sequence range overflow"))?;

  Ok((SeqRange { start, end }, end))
}

fn expires_at(now_ts_ms: i64, lease_duration_ms: i64) -> Result<i64> {
  now_ts_ms
    .checked_add(lease_duration_ms)
    .ok_or_else(|| anyhow!("lease expiration overflow"))
}
