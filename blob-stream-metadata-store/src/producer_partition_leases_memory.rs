#[cfg(test)]
#[path = "./producer_partition_leases_memory_test.rs"]
mod tests;

use crate::{
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  ProducerLeaseFence,
  ProducerPartitionLease,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SequenceReservation,
  SequenceReservationOutcome,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_types::SeqRange;
use log::{debug, trace};
use parking_lot::RwLock;
use std::collections::HashMap;

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
  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>> {
    let guard = self.leases.read();
    Ok(guard.get(key).map(|state| state.to_lease(key.clone())))
  }

  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseAcquireOutcome> {
    match self
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now_ts_ms,
        lease_duration_ms,
        None,
      )
      .await?
    {
      LeaseAcquireAndReserveOutcome::Acquired {
        lease,
        reservation: None,
      } => Ok(LeaseAcquireOutcome::Acquired(lease)),
      LeaseAcquireAndReserveOutcome::Acquired {
        reservation: Some(_),
        ..
      } => {
        unreachable!("lease acquisition did not request a sequence reservation")
      },
      LeaseAcquireAndReserveOutcome::HeldByOther(lease) => {
        Ok(LeaseAcquireOutcome::HeldByOther(lease))
      },
    }
  }

  async fn acquire_lease_and_reserve_sequences(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now_ts_ms: i64,
    lease_duration_ms: i64,
    reservation_size: Option<u64>,
  ) -> Result<LeaseAcquireAndReserveOutcome> {
    trace!(
      "producer lease(memory) acquire/reserve: topic={}, partition={}, holder_id={}, \
       reservation_size={reservation_size:?}",
      key.topic, key.virtual_partition_id, holder_id
    );
    if reservation_size == Some(0) {
      return Err(anyhow!("reservation_size must be greater than zero"));
    }

    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;
    let mut guard = self.leases.write();
    let state = guard.entry(key.clone()).or_insert_with(|| LeaseState {
      holder_id: holder_id.clone(),
      lease_epoch: 1,
      lease_session_id: lease_session_id.clone(),
      lease_expiration_ts_ms: expires_at,
      max_allocated_seq: None,
    });
    if !state.is_expired(now_ts_ms)
      && (state.holder_id != holder_id || state.lease_session_id != lease_session_id)
    {
      return Ok(LeaseAcquireAndReserveOutcome::HeldByOther(
        state.to_lease(key),
      ));
    }

    let takeover = state.is_expired(now_ts_ms) || state.lease_session_id != lease_session_id;
    let lease_epoch = if takeover {
      state
        .lease_epoch
        .checked_add(1)
        .ok_or_else(|| anyhow!("lease epoch overflow"))?
    } else {
      state.lease_epoch
    };
    let reservation = reservation_size
      .map(|size| reserve_range(state.max_allocated_seq, size))
      .transpose()?;

    // Compute all fallible takeover state first so an overflow leaves the active lease unchanged.
    if takeover {
      state.lease_epoch = lease_epoch;
      state.lease_session_id = lease_session_id;
    }
    state.holder_id = holder_id;
    state.lease_expiration_ts_ms = expires_at;
    let reservation = if let Some((range, updated)) = reservation {
      state.max_allocated_seq = Some(updated);
      Some(range)
    } else {
      None
    };
    let lease = state.to_lease(key);
    debug!("producer lease(memory) acquire/reserve result: acquired, reservation={reservation:?}");
    Ok(LeaseAcquireAndReserveOutcome::Acquired { lease, reservation })
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseHeartbeatOutcome> {
    trace!(
      "producer lease(memory) heartbeat: topic={}, partition={}, holder_id={}",
      key.topic, key.virtual_partition_id, holder_id
    );
    let mut guard = self.leases.write();
    let Some(state) = guard.get_mut(key) else {
      return Ok(LeaseHeartbeatOutcome::Expired);
    };

    if state.is_expired(now_ts_ms) {
      return Ok(LeaseHeartbeatOutcome::Expired);
    }

    if state.holder_id != holder_id || state.lease_session_id != lease_session_id {
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
    lease_session_id: &str,
    now_ts_ms: i64,
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome> {
    trace!(
      "producer lease(memory) reserve: topic={}, partition={}, holder_id={}, size={}",
      key.topic, key.virtual_partition_id, holder_id, reservation_size
    );
    if reservation_size == 0 {
      return Err(anyhow!("reservation_size must be greater than zero"));
    }

    let mut guard = self.leases.write();
    let Some(state) = guard.get_mut(key) else {
      return Ok(SequenceReservationOutcome::Expired);
    };

    if state.is_expired(now_ts_ms) {
      return Ok(SequenceReservationOutcome::Expired);
    }

    if state.holder_id != holder_id || state.lease_session_id != lease_session_id {
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
    lease_session_id: &str,
    now_ts_ms: i64,
  ) -> Result<LeaseReleaseOutcome> {
    trace!(
      "producer lease(memory) release: topic={}, partition={}, holder_id={}",
      key.topic, key.virtual_partition_id, holder_id
    );
    let mut guard = self.leases.write();
    let Some(mut state) = guard.get(key).cloned() else {
      return Ok(LeaseReleaseOutcome::Expired);
    };

    if state.is_expired(now_ts_ms) {
      return Ok(LeaseReleaseOutcome::Expired);
    }

    if state.holder_id != holder_id || state.lease_session_id != lease_session_id {
      return Ok(LeaseReleaseOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    state.lease_expiration_ts_ms = now_ts_ms;
    guard.insert(key.clone(), state);
    Ok(LeaseReleaseOutcome::Released)
  }
}

//
// LeaseState
//

#[derive(Clone, Debug)]
struct LeaseState {
  holder_id: String,
  lease_epoch: u64,
  lease_session_id: String,
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
      fence: Some(ProducerLeaseFence {
        holder_id: self.holder_id.clone(),
        lease_epoch: self.lease_epoch,
        lease_session_id: self.lease_session_id.clone(),
      }),
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
