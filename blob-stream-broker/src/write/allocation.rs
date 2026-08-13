#[cfg(test)]
#[path = "./allocation_test.rs"]
mod tests;

use super::state::WriteState;
use crate::write::buffer::FlushCompletionError;
use blob_stream_metadata_store::ProducerPartitionLease;
use blob_stream_types::{SeqRange, VirtualPartitionId};
use log::trace;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::futures::OwnedNotified;

const MAINTENANCE_TARGET_HIGH_UTILIZATION_PERCENT: u64 = 75;

//
// AllocationTransition
//

pub(super) struct AllocationTransition {
  state: Arc<Mutex<WriteState>>,
  topic: String,
  virtual_partition_id: VirtualPartitionId,
  reset_sequence_allocation_on_finish: bool,
  finished: bool,
}

impl AllocationTransition {
  pub(super) fn finish(
    mut self,
    lease_expiration_update: LeaseExpirationUpdate,
    lease: Option<ProducerPartitionLease>,
    reservation: Option<SeqRange>,
  ) {
    self.finish_inner(lease_expiration_update, lease, reservation, None);
  }

  pub(super) fn finish_lease_maintenance(
    mut self,
    lease_expiration_update: LeaseExpirationUpdate,
    lease: Option<ProducerPartitionLease>,
    reservation: Option<SeqRange>,
    records_allocated_since_last_maintenance: u64,
  ) {
    self.finish_inner(
      lease_expiration_update,
      lease,
      reservation,
      Some(records_allocated_since_last_maintenance),
    );
  }

  fn finish_inner(
    &mut self,
    lease_expiration_update: LeaseExpirationUpdate,
    lease: Option<ProducerPartitionLease>,
    reservation: Option<SeqRange>,
    records_allocated_since_last_maintenance: Option<u64>,
  ) {
    if self.finished {
      return;
    }

    trace!(
      "broker allocation transition finishing: topic={}, virtual_partition_id={}, \
       lease_expiration_update={}, reservation={}, reset_sequence_allocation={}",
      self.topic,
      self.virtual_partition_id,
      match lease_expiration_update {
        LeaseExpirationUpdate::Preserve => "preserve",
        LeaseExpirationUpdate::Set(None) => "clear",
        LeaseExpirationUpdate::Set(Some(_)) => "set",
      },
      reservation.is_some(),
      self.reset_sequence_allocation_on_finish,
    );

    let (allocation_notify, drain_notify, stale_completions) = {
      let mut state = self.state.lock();
      let partition_state = state.partition_state_mut(&self.topic, self.virtual_partition_id);
      let mut stale_completions = Vec::new();
      if let LeaseExpirationUpdate::Set(lease_expiration_ts_ms) = lease_expiration_update {
        partition_state.lease_expiration_ts_ms = lease_expiration_ts_ms;
      }
      if let Some(lease) = lease {
        let lease_fence = Some(Arc::new(lease.fence));
        if partition_state.lease_fence != lease_fence {
          stale_completions = partition_state.buffer.discard();
        }
        partition_state.lease_fence = lease_fence;
      } else if matches!(lease_expiration_update, LeaseExpirationUpdate::Set(None)) {
        stale_completions = partition_state.buffer.discard();
        partition_state.lease_fence = None;
      }
      if matches!(lease_expiration_update, LeaseExpirationUpdate::Set(None))
        || (self.reset_sequence_allocation_on_finish
          && matches!(lease_expiration_update, LeaseExpirationUpdate::Set(Some(_))))
      {
        partition_state.reset_sequence_allocation();
      }
      if let Some(reservation) = reservation {
        partition_state
          .seq_allocator
          .install_or_extend_reservation(reservation);
      }
      if let Some(records_allocated_since_last_maintenance) =
        records_allocated_since_last_maintenance
        && matches!(lease_expiration_update, LeaseExpirationUpdate::Set(Some(_)))
      {
        partition_state.records_allocated_since_lease_maintenance = partition_state
          .records_allocated_since_lease_maintenance
          .saturating_sub(records_allocated_since_last_maintenance);
      }
      partition_state.allocation_in_flight = false;
      partition_state.allocation_started_ts_ms = None;
      (
        Arc::clone(&partition_state.allocation_notify),
        Arc::clone(&partition_state.drain_notify),
        stale_completions,
      )
    };
    self.finished = true;
    trace!(
      "broker allocation transition notifying partition drain waiter: topic={}, \
       virtual_partition_id={}",
      self.topic, self.virtual_partition_id,
    );
    allocation_notify.notify_waiters();
    drain_notify.notify_waiters();
    for completion in stale_completions {
      let _ignored = completion.send(Err(FlushCompletionError::LeaseFenceLost));
    }
  }
}

impl Drop for AllocationTransition {
  fn drop(&mut self) {
    self.finish_inner(LeaseExpirationUpdate::Preserve, None, None, None);
  }
}

//
// LeaseExpirationUpdate
//

#[derive(Clone, Copy)]
pub(super) enum LeaseExpirationUpdate {
  Preserve,
  Set(Option<i64>),
}

//
// AllocationTransitionWork
//

pub(super) struct AllocationTransitionWork {
  pub(super) transition: AllocationTransition,
  pub(super) needs_lease: bool,
  pub(super) reservation: Option<ReservationRequest>,
  pub(super) records_allocated_since_last_maintenance: Option<u64>,
}

//
// ReservationRequest
//

#[derive(Clone, Copy, Debug)]
pub(super) struct ReservationRequest {
  pub(super) size: u64,
  pub(super) reason: ReservationReason,
}

//
// ReservationReason
//

#[derive(Clone, Copy, Debug)]
pub(super) enum ReservationReason {
  Initial,
  ForegroundExhaustion,
  MaintenanceTopUp,
  MaintenanceHighUtilization,
}

fn maintenance_high_utilization_threshold(target_size: u64) -> u64 {
  target_size
    .saturating_mul(MAINTENANCE_TARGET_HIGH_UTILIZATION_PERCENT)
    .saturating_add(99)
    / 100
}

//
// AllocationTransitionDecision
//

pub(super) enum AllocationTransitionDecision {
  Draining,
  Ready,
  Waiting(OwnedNotified),
  Claimed(AllocationTransitionWork),
}

pub(super) fn begin_allocation_transition(
  state: &Arc<Mutex<WriteState>>,
  topic: &str,
  virtual_partition_id: VirtualPartitionId,
  record_count: u64,
  now_ts_ms: i64,
  renew_lease: bool,
  base_reservation_size: u64,
) -> AllocationTransitionDecision {
  let mut state_guard = state.lock();
  let partition_state = state_guard.partition_state_mut(topic, virtual_partition_id);
  if partition_state.draining && !renew_lease {
    return AllocationTransitionDecision::Draining;
  }
  if partition_state.allocation_in_flight {
    return AllocationTransitionDecision::Waiting(
      Arc::clone(&partition_state.allocation_notify).notified_owned(),
    );
  }

  let lease_was_expired = partition_state.needs_lease(now_ts_ms);
  let needs_lease = renew_lease || lease_was_expired;
  let remaining_capacity = partition_state.seq_allocator.remaining_capacity();
  let records_allocated_since_last_maintenance =
    partition_state.records_allocated_since_lease_maintenance;
  let maintenance_high_utilization = renew_lease
    && partition_state.seq_allocator.has_reservation()
    && records_allocated_since_last_maintenance
      >= maintenance_high_utilization_threshold(
        partition_state.reservation_target(base_reservation_size),
      );
  let reservation = if !partition_state.seq_allocator.can_allocate(record_count) {
    let reason = if !partition_state.seq_allocator.has_reservation() {
      ReservationReason::Initial
    } else if maintenance_high_utilization {
      ReservationReason::MaintenanceHighUtilization
    } else if renew_lease {
      ReservationReason::MaintenanceTopUp
    } else {
      ReservationReason::ForegroundExhaustion
    };
    let size = match reason {
      ReservationReason::ForegroundExhaustion | ReservationReason::MaintenanceHighUtilization => {
        partition_state.double_reservation_target(base_reservation_size)
      },
      ReservationReason::Initial | ReservationReason::MaintenanceTopUp => {
        partition_state.reservation_target(base_reservation_size)
      },
    };
    Some(ReservationRequest { size, reason })
  } else if renew_lease && remaining_capacity < records_allocated_since_last_maintenance {
    // Grow only when a new window is needed. High utilization then doubles the next window so
    // normal burst timing relative to maintenance cannot delay convergence indefinitely.
    let (size, reason) = if maintenance_high_utilization {
      (
        partition_state.double_reservation_target(base_reservation_size),
        ReservationReason::MaintenanceHighUtilization,
      )
    } else {
      (
        partition_state.reservation_target(base_reservation_size),
        ReservationReason::MaintenanceTopUp,
      )
    };
    Some(ReservationRequest { size, reason })
  } else {
    None
  };
  trace!(
    "broker sequence allocation decision: topic={topic}, \
     virtual_partition_id={virtual_partition_id}, record_count={record_count}, \
     renew_lease={renew_lease}, needs_lease={needs_lease}, \
     remaining_capacity={remaining_capacity}, \
     records_allocated_since_last_maintenance={records_allocated_since_last_maintenance}, \
     target_size={}, reservation={reservation:?}",
    partition_state
      .adaptive_reservation_size
      .unwrap_or(base_reservation_size),
  );
  if !needs_lease && reservation.is_none() {
    return AllocationTransitionDecision::Ready;
  }

  partition_state.allocation_in_flight = true;
  partition_state.allocation_started_ts_ms = Some(now_ts_ms);
  trace!(
    "broker allocation transition claimed: topic={topic}, \
     virtual_partition_id={virtual_partition_id}, renew_lease={renew_lease}, \
     needs_lease={needs_lease}, reservation={reservation:?}"
  );
  AllocationTransitionDecision::Claimed(AllocationTransitionWork {
    transition: AllocationTransition {
      state: Arc::clone(state),
      topic: topic.to_string(),
      virtual_partition_id,
      reset_sequence_allocation_on_finish: lease_was_expired,
      finished: false,
    },
    needs_lease,
    reservation,
    records_allocated_since_last_maintenance: renew_lease
      .then_some(records_allocated_since_last_maintenance),
  })
}
