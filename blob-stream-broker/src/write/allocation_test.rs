use super::{
  AllocationTransition,
  AllocationTransitionDecision,
  AllocationTransitionFinish,
  LeaseExpirationUpdate,
  begin_allocation_transition,
};
use crate::write::buffer::{BufferedBatch, FlushCompletionError};
use crate::write::state::{SeqAllocator, WriteState};
use blob_stream_metadata_store::{
  ProducerLeaseFence,
  ProducerPartitionLease,
  ProducerPartitionLeaseKey,
};
use blob_stream_types::{BatchSummary, SeqRange, new_record, offset_datetime_from_unix_millis};
use parking_lot::Mutex;
use std::sync::Arc;

fn fence(epoch: u64) -> ProducerLeaseFence {
  ProducerLeaseFence {
    holder_id: "broker-a".to_string(),
    lease_epoch: epoch,
    lease_session_id: format!("session-{epoch}"),
  }
}

fn state_with_local_telemetry_assignment() -> Arc<Mutex<WriteState>> {
  let mut state = WriteState::default();
  // Direct allocation tests model the post-discovery state explicitly. Production only publishes
  // this authorization after membership has produced an initialized snapshot containing itself.
  state.publish_assignment(&[("telemetry".into(), 0)]);
  Arc::new(Mutex::new(state))
}

#[test]
fn fence_change_discards_buffered_batches_and_completes_them() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  let (completion, mut completion_rx) = tokio::sync::oneshot::channel();
  {
    let mut state = state.lock();
    state.publish_assignment(&[("telemetry".into(), 0)]);
    let partition = state.partition_state_mut("telemetry", 0);
    partition.lease_fence = Some(Arc::new(fence(1)));
    partition.buffer.push(
      BufferedBatch {
        records: vec![new_record(vec![1], 0)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 1,
        },
        seq_range: SeqRange { start: 0, end: 0 },
        acceptance_fence: Some(Arc::new(fence(1))),
        completion: Some(completion),
      },
      offset_datetime_from_unix_millis(0),
    );
  }

  AllocationTransition {
    state: Arc::clone(&state),
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    assignment_generation: 1,
    reset_sequence_allocation_on_finish: true,
    finished: false,
  }
  .finish(
    LeaseExpirationUpdate::Set(Some(offset_datetime_from_unix_millis(1_100))),
    Some(ProducerPartitionLease {
      key: ProducerPartitionLeaseKey {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
      },
      fence: fence(2),
      lease_expiration_at: offset_datetime_from_unix_millis(1_100),
      max_allocated_seq: Some(0),
      reservation_start: None,
      last_handed_out_seq: None,
      sequence_progress_updated_at: None,
    }),
    None,
  );

  assert_eq!(
    completion_rx
      .try_recv()
      .expect("superseded batch must complete"),
    Err(FlushCompletionError::LeaseFenceLost)
  );
  let state = state.lock();
  let partition = state
    .partition_state("telemetry", 0)
    .expect("partition exists");
  assert!(partition.buffer.batches.is_empty());
  assert_eq!(
    partition.lease_expiration_at,
    Some(offset_datetime_from_unix_millis(1_100))
  );
  assert_eq!(partition.lease_fence.as_deref(), Some(&fence(2)));
}

#[test]
fn lease_maintenance_distinguishes_acquisition_from_renewal() {
  let state = state_with_local_telemetry_assignment();
  let acquired_at = offset_datetime_from_unix_millis(1_000);
  let lease_expires_at = offset_datetime_from_unix_millis(1_100);

  let AllocationTransitionDecision::Claimed(initial) =
    begin_allocation_transition(&state, "telemetry", 0, 1, acquired_at, true, 10)
  else {
    panic!("initial lease maintenance must claim allocation");
  };
  assert!(initial.lease_was_expired);
  initial.transition.finish(
    LeaseExpirationUpdate::Set(Some(lease_expires_at)),
    None,
    None,
  );

  let AllocationTransitionDecision::Claimed(renewal) = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_050),
    true,
    10,
  ) else {
    panic!("lease renewal must claim allocation");
  };
  assert!(!renewal.lease_was_expired);
}

#[test]
fn allocation_rejects_unassigned_partition_without_creating_state() {
  let state = Arc::new(Mutex::new(WriteState::default()));

  let decision = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_000),
    false,
    10,
  );

  assert!(matches!(
    decision,
    AllocationTransitionDecision::NotAssigned
  ));
  assert!(state.lock().partition_keys().is_empty());
}

#[test]
fn allocator_sequence_progress_tracks_last_handout() {
  let mut allocator = SeqAllocator::default();
  assert_eq!(allocator.sequence_progress().reservation_start, None);
  assert_eq!(allocator.sequence_progress().last_handed_out_seq, None);

  allocator.install_or_extend_reservation(SeqRange { start: 10, end: 19 });
  assert_eq!(allocator.sequence_progress().reservation_start, Some(10));
  assert_eq!(allocator.sequence_progress().last_handed_out_seq, None);
  assert_eq!(allocator.allocate(3), Some(SeqRange { start: 10, end: 12 }));
  assert_eq!(allocator.sequence_progress().last_handed_out_seq, Some(12));

  allocator.install_or_extend_reservation(SeqRange { start: 20, end: 29 });
  assert_eq!(allocator.sequence_progress().reservation_start, Some(10));
  assert_eq!(
    allocator
      .sequence_progress_for_new_reservation()
      .reservation_start,
    None
  );
  assert_eq!(
    allocator
      .sequence_progress_for_new_reservation()
      .last_handed_out_seq,
    Some(12)
  );
}

#[test]
fn stale_assignment_completion_discards_lease_and_reservation() {
  let state = state_with_local_telemetry_assignment();
  let now = offset_datetime_from_unix_millis(1_000);
  let AllocationTransitionDecision::Claimed(transition) =
    begin_allocation_transition(&state, "telemetry", 0, 1, now, false, 10)
  else {
    panic!("assigned partition must claim its initial transition");
  };

  // This is the same atomic assignment publication performed by the membership loop while the
  // lease-store request is in flight. Completion must only clear the claim, never revive state.
  state.lock().publish_assignment(&[]);
  let finish = transition.transition.finish(
    LeaseExpirationUpdate::Set(Some(now + time::Duration::seconds(60))),
    Some(ProducerPartitionLease {
      key: ProducerPartitionLeaseKey {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
      },
      fence: fence(1),
      lease_expiration_at: now + time::Duration::seconds(60),
      max_allocated_seq: Some(9),
      reservation_start: None,
      last_handed_out_seq: None,
      sequence_progress_updated_at: None,
    }),
    Some(SeqRange { start: 0, end: 9 }),
  );

  assert_eq!(finish, AllocationTransitionFinish::StaleAssignment);
  let state = state.lock();
  let partition_state = state
    .partition_state("telemetry", 0)
    .expect("the draining release still owns state retirement");
  assert!(partition_state.lease_expiration_at.is_none());
  assert!(partition_state.lease_fence.is_none());
  assert!(partition_state.seq_allocator.reservation.is_none());
  assert!(!partition_state.allocation_in_flight);
}

#[test]
fn terminal_release_preserves_a_reassigned_partition_state() {
  let mut state = WriteState::default();
  state.publish_assignment(&[("telemetry".into(), 0)]);
  let partition_state = state.partition_state_mut("telemetry", 0);
  partition_state.draining = true;
  partition_state.lease_expiration_at = Some(offset_datetime_from_unix_millis(1_100));
  partition_state.lease_fence = Some(Arc::new(fence(1)));
  partition_state
    .seq_allocator
    .install_or_extend_reservation(SeqRange { start: 0, end: 9 });

  // A membership update can restore this partition while an earlier asynchronous release is
  // completing. That release must fence its old lease but leave state for the new maintenance
  // pass instead of deleting the fresh assignment's partition entry.
  state.clear_terminally_released_partition("telemetry", 0);

  let partition_state = state
    .partition_state("telemetry", 0)
    .expect("reassigned partition state must remain available for maintenance");
  assert!(partition_state.lease_expiration_at.is_none());
  assert!(partition_state.lease_fence.is_none());
  assert!(partition_state.seq_allocator.reservation.is_none());
  assert!(partition_state.draining);
}
