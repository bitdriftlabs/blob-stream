use super::{
  AllocationTransition,
  AllocationTransitionDecision,
  LeaseExpirationUpdate,
  begin_allocation_transition,
};
use crate::write::buffer::{BufferedBatch, FlushCompletionError};
use crate::write::state::WriteState;
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

#[test]
fn fence_change_discards_buffered_batches_and_completes_them() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  let (completion, mut completion_rx) = tokio::sync::oneshot::channel();
  {
    let mut state = state.lock();
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
  let state = Arc::new(Mutex::new(WriteState::default()));
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
