use super::collect_flush_plans;
use crate::write::buffer::{BufferedBatch, FlushCompletionError};
use crate::write::state::WriteState;
use crate::write::{TopicInfo, WriteConfig};
use blob_stream_metadata_store::ProducerLeaseFence;
use blob_stream_types::{BatchSummary, SeqRange, new_record, offset_datetime_from_unix_millis};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use time::Duration;

fn fence() -> ProducerLeaseFence {
  ProducerLeaseFence {
    holder_id: "broker-a".to_string(),
    lease_epoch: 1,
    lease_session_id: "session-a".to_string(),
  }
}

fn topics() -> HashMap<protobuf::Chars, TopicInfo> {
  HashMap::from([(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count: 100,
      num_writers: 1,
      retention: Duration::days(7),
      max_metadata_publication_lag: Duration::milliseconds(15_000),
    },
  )])
}

#[test]
fn fenced_flushes_split_at_ninety_nine_flushable_partitions() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    for virtual_partition_id in 0 .. 100 {
      let partition = state.partition_state_mut("telemetry", virtual_partition_id);
      partition.lease_fence = Some(Arc::new(fence()));
      partition.buffer.push(
        BufferedBatch {
          records: vec![new_record(vec![1], 0)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 1,
          },
          seq_range: SeqRange {
            start: virtual_partition_id.into(),
            end: virtual_partition_id.into(),
          },
          acceptance_fence: Some(Arc::new(fence())),
          completion: None,
        },
        offset_datetime_from_unix_millis(0),
      );
    }
  }

  let mut config = WriteConfig::with_defaults();
  config.fenced_metadata_writes = true;
  let plans = collect_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &config,
    None,
    &topics(),
    4,
  );

  assert_eq!(plans.len(), 2);
  assert_eq!(plans[0].partitions.len(), 99);
  assert_eq!(plans[1].partitions.len(), 1);
  assert!(plans.iter().all(|plan| plan.fenced_metadata_writes));
}

#[test]
fn fenced_flush_uses_the_batches_acceptance_fence() {
  let acceptance_fence = fence();
  let current_fence = ProducerLeaseFence {
    lease_epoch: 2,
    ..acceptance_fence.clone()
  };
  let state = Arc::new(Mutex::new(WriteState::default()));
  {
    let mut state = state.lock();
    let partition = state.partition_state_mut("telemetry", 0);
    partition.lease_fence = Some(Arc::new(current_fence));
    partition.buffer.push(
      BufferedBatch {
        records: vec![new_record(vec![1], 0)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 1,
        },
        seq_range: SeqRange { start: 0, end: 0 },
        acceptance_fence: Some(Arc::new(acceptance_fence.clone())),
        completion: None,
      },
      offset_datetime_from_unix_millis(0),
    );
  }

  let mut config = WriteConfig::with_defaults();
  config.fenced_metadata_writes = true;
  let plans = collect_flush_plans(
    &state,
    offset_datetime_from_unix_millis(1_000),
    &config,
    None,
    &topics(),
    1,
  );

  assert_eq!(plans.len(), 1);
  assert_eq!(
    plans[0].partitions[0].lease_fence.as_deref(),
    Some(&acceptance_fence)
  );
}

#[test]
fn fenced_flush_drops_batches_without_an_acceptance_fence() {
  let state = Arc::new(Mutex::new(WriteState::default()));
  let (completion, mut completion_rx) = tokio::sync::oneshot::channel();
  {
    let mut state = state.lock();
    let partition = state.partition_state_mut("telemetry", 0);
    partition.lease_fence = Some(Arc::new(fence()));
    partition.buffer.push(
      BufferedBatch {
        records: vec![new_record(vec![1], 0)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 1,
        },
        seq_range: SeqRange { start: 0, end: 0 },
        acceptance_fence: None,
        completion: Some(completion),
      },
      offset_datetime_from_unix_millis(0),
    );
  }

  let mut config = WriteConfig::with_defaults();
  config.fenced_metadata_writes = true;
  assert!(
    collect_flush_plans(
      &state,
      offset_datetime_from_unix_millis(1_000),
      &config,
      None,
      &topics(),
      1,
    )
    .is_empty()
  );
  assert_eq!(
    completion_rx
      .try_recv()
      .expect("discarded batch must complete"),
    Err(FlushCompletionError::LeaseFenceLost)
  );
  let state = state.lock();
  let partition = state
    .partition_state("telemetry", 0)
    .expect("partition exists");
  assert!(partition.buffer.batches.is_empty());
  assert!(!partition.flush_in_flight);
}
