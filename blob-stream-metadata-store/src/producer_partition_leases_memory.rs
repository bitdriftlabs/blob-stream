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
  ProducerSequenceProgress,
  SequenceReservation,
  SequenceReservationOutcome,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_types::SeqRange;
use log::{debug, trace};
use parking_lot::RwLock;
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};

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
    now: OffsetDateTime,
    lease_duration: Duration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireOutcome> {
    match self
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        None,
        sequence_progress,
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
    now: OffsetDateTime,
    lease_duration: Duration,
    reservation_size: Option<u64>,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireAndReserveOutcome> {
    trace!(
      "producer lease(memory) acquire/reserve: topic={}, partition={}, holder_id={}, \
       reservation_size={reservation_size:?}",
      key.topic, key.virtual_partition_id, holder_id
    );
    if reservation_size == Some(0) {
      return Err(anyhow!("reservation_size must be greater than zero"));
    }

    let expires_at = expires_at(now, lease_duration)?;
    let mut guard = self.leases.write();
    let state = guard.entry(key.clone()).or_insert_with(|| LeaseState {
      holder_id: holder_id.clone(),
      lease_epoch: 1,
      lease_session_id: lease_session_id.clone(),
      lease_expiration_at: expires_at,
      max_allocated_seq: None,
      reservation_start: None,
      last_handed_out_seq: None,
      sequence_progress_updated_at: None,
    });
    if !state.is_expired(now)
      && (state.holder_id != holder_id || state.lease_session_id != lease_session_id)
    {
      return Ok(LeaseAcquireAndReserveOutcome::HeldByOther(
        state.to_lease(key),
      ));
    }

    let takeover = state.is_expired(now) || state.lease_session_id != lease_session_id;
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
    state.lease_expiration_at = expires_at;
    let reservation = if let Some((range, updated)) = reservation {
      state.max_allocated_seq = Some(updated);
      Some(range)
    } else {
      None
    };
    state.reservation_start = sequence_progress
      .reservation_start
      .or_else(|| reservation.as_ref().map(|range| range.start));
    state.last_handed_out_seq = sequence_progress.last_handed_out_seq;
    state.sequence_progress_updated_at = Some(now);
    let lease = state.to_lease(key);
    debug!("producer lease(memory) acquire/reserve result: acquired, reservation={reservation:?}");
    Ok(LeaseAcquireAndReserveOutcome::Acquired { lease, reservation })
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    lease_duration: Duration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseHeartbeatOutcome> {
    trace!(
      "producer lease(memory) heartbeat: topic={}, partition={}, holder_id={}",
      key.topic, key.virtual_partition_id, holder_id
    );
    let mut guard = self.leases.write();
    let Some(state) = guard.get_mut(key) else {
      return Ok(LeaseHeartbeatOutcome::Expired);
    };

    if state.is_expired(now) {
      return Ok(LeaseHeartbeatOutcome::Expired);
    }

    if state.holder_id != holder_id || state.lease_session_id != lease_session_id {
      return Ok(LeaseHeartbeatOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    state.lease_expiration_at = expires_at(now, lease_duration)?;
    state.reservation_start = sequence_progress.reservation_start;
    state.last_handed_out_seq = sequence_progress.last_handed_out_seq;
    state.sequence_progress_updated_at = Some(now);
    Ok(LeaseHeartbeatOutcome::Renewed(state.to_lease(key.clone())))
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    reservation_size: u64,
    sequence_progress: ProducerSequenceProgress,
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

    if state.is_expired(now) {
      return Ok(SequenceReservationOutcome::Expired);
    }

    if state.holder_id != holder_id || state.lease_session_id != lease_session_id {
      return Ok(SequenceReservationOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    let (range, updated) = reserve_range(state.max_allocated_seq, reservation_size)?;
    state.max_allocated_seq = Some(updated);
    state.reservation_start = sequence_progress.reservation_start.or(Some(range.start));
    state.last_handed_out_seq = sequence_progress.last_handed_out_seq;
    state.sequence_progress_updated_at = Some(now);

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
    now: OffsetDateTime,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseReleaseOutcome> {
    trace!(
      "producer lease(memory) release: topic={}, partition={}, holder_id={}",
      key.topic, key.virtual_partition_id, holder_id
    );
    let mut guard = self.leases.write();
    let Some(mut state) = guard.get(key).cloned() else {
      return Ok(LeaseReleaseOutcome::Expired);
    };

    if state.is_expired(now) {
      return Ok(LeaseReleaseOutcome::Expired);
    }

    if state.holder_id != holder_id || state.lease_session_id != lease_session_id {
      return Ok(LeaseReleaseOutcome::HeldByOther(
        state.to_lease(key.clone()),
      ));
    }

    state.lease_expiration_at = now;
    state.reservation_start = sequence_progress.reservation_start;
    state.last_handed_out_seq = sequence_progress.last_handed_out_seq;
    state.sequence_progress_updated_at = Some(now);
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
  lease_expiration_at: OffsetDateTime,
  max_allocated_seq: Option<u64>,
  reservation_start: Option<u64>,
  last_handed_out_seq: Option<u64>,
  sequence_progress_updated_at: Option<OffsetDateTime>,
}

impl LeaseState {
  fn is_expired(&self, now: OffsetDateTime) -> bool {
    now >= self.lease_expiration_at
  }

  fn to_lease(&self, key: ProducerPartitionLeaseKey) -> ProducerPartitionLease {
    ProducerPartitionLease {
      key,
      fence: ProducerLeaseFence {
        holder_id: self.holder_id.clone(),
        lease_epoch: self.lease_epoch,
        lease_session_id: self.lease_session_id.clone(),
      },
      lease_expiration_at: self.lease_expiration_at,
      max_allocated_seq: self.max_allocated_seq,
      reservation_start: self.reservation_start,
      last_handed_out_seq: self.last_handed_out_seq,
      sequence_progress_updated_at: self.sequence_progress_updated_at,
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

fn expires_at(now: OffsetDateTime, lease_duration: Duration) -> Result<OffsetDateTime> {
  now
    .checked_add(lease_duration)
    .ok_or_else(|| anyhow!("lease expiration overflow"))
}
