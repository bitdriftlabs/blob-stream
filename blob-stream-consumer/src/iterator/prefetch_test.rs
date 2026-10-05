#![allow(clippy::unwrap_used)]

use super::{ConsumerReaderCommand, PrefetchWorker, SeekTrace, process_reader_commands};
use crate::consumer::{
  ConsumerBatch,
  ConsumerBatchSource,
  ConsumerReader,
  ConsumerReaderPartitionMode,
  ReadCapacity,
};
use crate::coordination::RecoveredCursor;
use crate::iterator::shared::PendingCommit;
use crate::iterator::tests::{
  MutableCoordinationSource,
  rejecting_broker_blob_range_query,
  rejecting_broker_metadata_query,
  runtime_config_with_prefetch_max_bytes,
};
use crate::iterator::{
  ConsumerIteratorBuilder,
  ConsumerLifecycleHooks,
  ConsumerSeekTarget,
  ConsumerSharedState,
  CoordinationSnapshot,
  TopicPartitionLayout,
};
use bd_server_stats::stats::Collector;
use blob_stream_blob_store::{BlobKey, InMemoryBlobStore};
use blob_stream_metadata_store::{
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
  InMemoryMetadataStore,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{
  CommittedCursor,
  CommittedSourceCheckpoint,
  SeqRange,
  VirtualPartitionId,
  new_record,
};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use time::{Duration, OffsetDateTime};
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::Span;

async fn worker_fixture() -> (PrefetchWorker, mpsc::UnboundedSender<ConsumerReaderCommand>) {
  worker_fixture_with_budget(None).await
}

async fn worker_fixture_with_budget(
  prefetch_max_bytes: Option<u64>,
) -> (PrefetchWorker, mpsc::UnboundedSender<ConsumerReaderCommand>) {
  let clock = Arc::new(ManualTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let iterator = ConsumerIteratorBuilder::new(
    &runtime_config_with_prefetch_max_bytes(prefetch_max_bytes),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    Arc::new(InMemoryConsumerGroupLeaseStore::new()),
    Arc::new(InMemoryConsumerGroupMembershipStore::new()),
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![7, 8],
    })),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    Collector::default().scope("prefetch_test"),
    Duration::days(1),
    Duration::seconds(5),
    None,
    TopicPartitionLayout::new(64).unwrap(),
  )
  .time_provider(clock.clone())
  .build()
  .await
  .unwrap();
  let mut driver = iterator.driver.unwrap();
  let (tx, rx) = mpsc::unbounded_channel();
  let worker = PrefetchWorker::new(
    driver.reader.take().unwrap(),
    rx,
    driver.shared_state,
    driver.diagnostics,
    driver.delivery_notify,
    driver.prefetch_space_notify,
    driver.reader_command_notify,
    driver.prefetch_shutdown,
    driver.metrics,
    Duration::milliseconds(10),
    None,
    clock,
    None,
    "member-a".to_string(),
  );
  (worker, tx)
}

fn batch(partition_id: VirtualPartitionId, bytes: usize) -> ConsumerBatch {
  ConsumerBatch {
    virtual_partition_id: partition_id,
    seq_range: SeqRange { start: 1, end: 1 },
    source_checkpoint: CommittedSourceCheckpoint {
      window_start_unix_seconds: 0,
      snowflake_id: 1,
    },
    source: ConsumerBatchSource {
      blob_key: BlobKey::new("telemetry/0/1.bin"),
      metadata_published_at: OffsetDateTime::UNIX_EPOCH,
    },
    admission_scan: None,
    records: vec![new_record(vec![1; bytes], 0)],
  }
}

fn recovered(partition_id: VirtualPartitionId) -> RecoveredCursor {
  RecoveredCursor {
    committed_cursor: CommittedCursor {
      virtual_partition_id: partition_id,
      seq_end: 42,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 1,
      }),
    },
    committed_ts_ms: Some(0),
  }
}

async fn apply_commands(worker: &mut PrefetchWorker, pending: &mut VecDeque<ConsumerBatch>) {
  let mut records = pending.iter().map(|batch| batch.records.len()).sum();
  let mut bytes = pending.iter().map(super::prefetched_batch_bytes).sum();
  assert!(
    process_reader_commands(
      &mut worker.reader,
      &mut worker.reader_command_rx,
      &worker.shared_state,
      &worker.diagnostics,
      &worker.delivery_notify,
      pending,
      &mut records,
      &mut bytes,
      &mut worker.recovery_traces,
      &mut worker.pending_seek_traces,
      worker.lifecycle_hooks.as_deref(),
      &worker.member_id,
      &worker.metrics,
    )
    .await
  );
  assert_eq!(
    records,
    pending
      .iter()
      .map(|batch| batch.records.len())
      .sum::<usize>()
  );
  assert_eq!(
    bytes,
    pending
      .iter()
      .map(super::prefetched_batch_bytes)
      .sum::<u64>()
  );
  let state = worker.shared_state.lock();
  assert_eq!(state.diagnostics.prefetch_pending_record_count, records);
  assert_eq!(state.diagnostics.prefetch_pending_bytes, bytes);
}

#[tokio::test]
async fn suspension_preserves_retained_and_incoming_hydrated_partitions() {
  let (mut worker, tx) = worker_fixture().await;
  tx.send(ConsumerReaderCommand::HydrateCursors {
    recovered_cursors: HashMap::from([(9, recovered(9))]),
    now: OffsetDateTime::UNIX_EPOCH,
  })
  .unwrap();
  let epoch = worker.shared_state.lock().fence_reads(&HashSet::from([7]));
  tx.send(ConsumerReaderCommand::SuspendReadPartitions {
    partitions: vec![7],
    read_epoch: epoch,
  })
  .unwrap();
  let mut pending = VecDeque::from([batch(7, 3), batch(8, 5)]);
  apply_commands(&mut worker, &mut pending).await;
  assert_eq!(pending.len(), 1);
  assert_eq!(pending[0].virtual_partition_id, 8);
  assert!(
    worker
      .shared_state
      .lock()
      .active_partitions
      .contains_key(&7)
  );
  let states = worker.reader.partition_read_states();
  assert_eq!(states.len(), 2);
  assert_eq!(states[0].virtual_partition_id, 8);
  assert!(matches!(
    states[0].mode,
    ConsumerReaderPartitionMode::Fresh { .. }
  ));
  assert_eq!(states[1].virtual_partition_id, 9);
  assert!(matches!(
    states[1].mode,
    ConsumerReaderPartitionMode::Recovering { .. }
  ));
  assert!(
    !worker
      .shared_state
      .lock()
      .active_partitions
      .contains_key(&9)
  );
  assert_eq!(
    worker
      .reader
      .read_available(OffsetDateTime::UNIX_EPOCH, ReadCapacity::new(100))
      .await
      .unwrap(),
    Vec::<ConsumerBatch>::new()
  );
  assert!(
    worker
      .reader
      .partition_scan_states()
      .iter()
      .all(|scan| scan.virtual_partition_id != 9)
  );

  tx.send(ConsumerReaderCommand::SetAssignment {
    assignment: vec![8, 9],
    now: OffsetDateTime::UNIX_EPOCH,
    handoff_phase: None,
    release_delivery_fence: false,
    read_epoch: epoch,
  })
  .unwrap();
  apply_commands(&mut worker, &mut pending).await;
  assert!(worker.reader.partition_read_states().iter().any(|state| {
    state.virtual_partition_id == 9
      && matches!(state.mode, ConsumerReaderPartitionMode::Recovering { .. })
  }));
}

#[tokio::test]
async fn delayed_duplicate_suspension_does_not_remove_reacquired_partition() {
  let (mut worker, tx) = worker_fixture().await;
  let epoch = worker.shared_state.lock().fence_reads(&HashSet::from([7]));
  tx.send(ConsumerReaderCommand::SuspendReadPartitions {
    partitions: vec![7],
    read_epoch: epoch,
  })
  .unwrap();
  tx.send(ConsumerReaderCommand::SetAssignment {
    assignment: vec![8],
    now: OffsetDateTime::UNIX_EPOCH,
    handoff_phase: None,
    release_delivery_fence: false,
    read_epoch: epoch,
  })
  .unwrap();
  tx.send(ConsumerReaderCommand::HydrateCursors {
    recovered_cursors: HashMap::from([(7, recovered(7))]),
    now: OffsetDateTime::UNIX_EPOCH,
  })
  .unwrap();
  tx.send(ConsumerReaderCommand::SetAssignment {
    assignment: vec![7, 8],
    now: OffsetDateTime::UNIX_EPOCH,
    handoff_phase: None,
    release_delivery_fence: false,
    read_epoch: epoch,
  })
  .unwrap();
  tx.send(ConsumerReaderCommand::SuspendReadPartitions {
    partitions: vec![7],
    read_epoch: epoch,
  })
  .unwrap();
  let mut pending = VecDeque::from([batch(7, 3), batch(8, 5)]);
  apply_commands(&mut worker, &mut pending).await;
  assert!(worker.reader.partition_read_states().iter().any(|state| {
    state.virtual_partition_id == 7
      && matches!(state.mode, ConsumerReaderPartitionMode::Recovering { .. })
  }));
  assert!(
    !worker
      .shared_state
      .lock()
      .read_fenced_partitions
      .contains_key(&7)
  );
  assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn old_assignment_cannot_resume_a_newer_revocation() {
  let (mut worker, tx) = worker_fixture().await;
  let old = worker.shared_state.lock().fence_reads(&HashSet::from([7]));
  let new = {
    let mut state = worker.shared_state.lock();
    let epoch = state.fence_reads(&HashSet::from([7]));
    state.delivery_fence_epoch = Some(epoch);
    state.delivery_state.revocation_in_progress = true;
    epoch
  };
  tx.send(ConsumerReaderCommand::SetAssignment {
    assignment: vec![7, 8],
    now: OffsetDateTime::UNIX_EPOCH,
    handoff_phase: None,
    release_delivery_fence: true,
    read_epoch: old,
  })
  .unwrap();
  tx.send(ConsumerReaderCommand::SuspendReadPartitions {
    partitions: vec![7],
    read_epoch: new,
  })
  .unwrap();
  apply_commands(&mut worker, &mut VecDeque::new()).await;
  let state = worker.shared_state.lock();
  assert!(state.delivery_state.revocation_in_progress);
  assert_eq!(state.read_fenced_partitions.get(&7), Some(&new));
  assert!(
    worker
      .reader
      .partition_read_states()
      .iter()
      .all(|state| state.virtual_partition_id != 7)
  );
}

#[tokio::test]
async fn unrelated_read_fence_does_not_hold_completed_delivery_revocation() {
  let (worker, _tx) = worker_fixture().await;
  let mut state = worker.shared_state.lock();
  let delivery_epoch = state.fence_reads(&HashSet::from([7]));
  state.delivery_fence_epoch = Some(delivery_epoch);
  state.delivery_state.revocation_in_progress = true;
  let newer = state.fence_reads(&HashSet::from([8]));
  assert!(state.complete_read_assignment(delivery_epoch, true));
  assert!(!state.delivery_state.revocation_in_progress);
  assert_eq!(state.read_fenced_partitions.get(&8), Some(&newer));
}

#[tokio::test]
async fn suspension_and_seek_overlap_cannot_leave_revoked_read_work() {
  for seek_first in [true, false] {
    let (mut worker, tx) = worker_fixture().await;
    worker
      .shared_state
      .lock()
      .active_partitions
      .get_mut(&7)
      .unwrap()
      .pending_commit = Some(PendingCommit {
      offset: 42,
      source_checkpoint: recovered(7).committed_cursor.source_checkpoint.unwrap(),
    });
    let (response_tx, response_rx) = oneshot::channel();
    let seek = ConsumerReaderCommand::Seek {
      virtual_partition_id: 7,
      target: ConsumerSeekTarget {
        offset: 42,
        window_start_unix_seconds: 0,
        snowflake_id: None,
      },
      now: OffsetDateTime::UNIX_EPOCH,
      seek_trace: SeekTrace::new(Span::none()),
      response: response_tx,
    };
    if seek_first {
      tx.send(seek).unwrap();
      let epoch = worker.shared_state.lock().fence_reads(&HashSet::from([7]));
      tx.send(ConsumerReaderCommand::SuspendReadPartitions {
        partitions: vec![7],
        read_epoch: epoch,
      })
      .unwrap();
    } else {
      let epoch = worker.shared_state.lock().fence_reads(&HashSet::from([7]));
      tx.send(ConsumerReaderCommand::SuspendReadPartitions {
        partitions: vec![7],
        read_epoch: epoch,
      })
      .unwrap();
      tx.send(seek).unwrap();
    }
    let mut pending = VecDeque::from([batch(7, 3), batch(8, 5)]);
    apply_commands(&mut worker, &mut pending).await;
    assert!(
      response_rx
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("read-fenced")
    );
    assert!(
      worker
        .reader
        .partition_read_states()
        .iter()
        .all(|partition| partition.virtual_partition_id != 7)
    );
    assert!(!worker.pending_seek_traces.contains_key(&7));
    assert!(!worker.recovery_traces.contains_key(&7));
    assert_eq!(
      worker.shared_state.lock().active_partitions[&7]
        .pending_commit
        .as_ref()
        .unwrap()
        .offset,
      42
    );
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].virtual_partition_id, 8);
  }
}

#[tokio::test]
async fn read_fence_rejects_pending_output_without_removing_commit_eligibility() {
  let (worker, _tx) = worker_fixture().await;
  worker.shared_state.lock().fence_reads(&HashSet::from([7]));
  let mut pending = VecDeque::from([batch(7, 3), batch(8, 5)]);
  let mut records = 2;
  let mut bytes = 8;
  assert!(
    !worker
      .admit_pending_batches(&mut pending, &mut records, &mut bytes, 100)
      .await
  );
  assert!(pending.is_empty());
  assert_eq!((records, bytes), (0, 0));
  let state = worker.shared_state.lock();
  assert!(state.active_partitions.contains_key(&7));
  assert_eq!(state.delivery_state.batches.len(), 1);
  assert_eq!(state.delivery_state.batches[0].virtual_partition_id, 8);
  assert_eq!(state.delivery_state.retained_bytes(), 5);
}

#[tokio::test]
async fn unrelated_lease_loss_cannot_resume_pending_revocation() {
  let (mut worker, tx) = worker_fixture().await;
  let epoch = {
    let mut state = worker.shared_state.lock();
    let epoch = state.fence_reads(&HashSet::from([7]));
    state.delivery_fence_epoch = Some(epoch);
    state.delivery_state.revocation_in_progress = true;
    epoch
  };
  tx.send(ConsumerReaderCommand::SuspendReadPartitions {
    partitions: vec![7],
    read_epoch: epoch,
  })
  .unwrap();
  apply_commands(&mut worker, &mut VecDeque::new()).await;
  let epoch = {
    let mut state = worker.shared_state.lock();
    state.remove_fenced_partitions(&[8]);
    state.read_epoch
  };
  tx.send(ConsumerReaderCommand::SetAssignment {
    assignment: vec![7],
    now: OffsetDateTime::UNIX_EPOCH,
    handoff_phase: None,
    release_delivery_fence: false,
    read_epoch: epoch,
  })
  .unwrap();
  apply_commands(&mut worker, &mut VecDeque::new()).await;
  {
    let state = worker.shared_state.lock();
    assert!(state.delivery_state.revocation_in_progress);
    assert!(!state.read_allowed(7));
    assert!(state.read_fenced_partitions.contains_key(&7));
  }
  assert_eq!(worker.reader.partition_read_states().len(), 0);
  tx.send(ConsumerReaderCommand::SetAssignment {
    assignment: vec![],
    now: OffsetDateTime::UNIX_EPOCH,
    handoff_phase: None,
    release_delivery_fence: true,
    read_epoch: epoch,
  })
  .unwrap();
  apply_commands(&mut worker, &mut VecDeque::new()).await;
  let state = worker.shared_state.lock();
  assert!(!state.delivery_state.revocation_in_progress);
  assert!(state.read_fenced_partitions.is_empty());
}

//
// CommandBeforeCapacityWaitHooks
//

struct CommandBeforeCapacityWaitHooks {
  shared_state: Arc<Mutex<ConsumerSharedState>>,
  commands: mpsc::UnboundedSender<ConsumerReaderCommand>,
  command_notify: Arc<Notify>,
  space_notify: Arc<Notify>,
  requested: AtomicBool,
  applied: AtomicBool,
}

impl CommandBeforeCapacityWaitHooks {
  fn new(
    worker: &PrefetchWorker,
    commands: mpsc::UnboundedSender<ConsumerReaderCommand>,
  ) -> Arc<Self> {
    Arc::new(Self {
      shared_state: worker.shared_state.clone(),
      commands,
      command_notify: worker.reader_command_notify.clone(),
      space_notify: worker.prefetch_space_notify.clone(),
      requested: AtomicBool::new(false),
      applied: AtomicBool::new(false),
    })
  }

  fn request_suspension(&self) {
    if self.requested.swap(true, Ordering::SeqCst) {
      return;
    }
    let epoch = self.shared_state.lock().fence_reads(&HashSet::from([7]));
    self
      .commands
      .send(ConsumerReaderCommand::SuspendReadPartitions {
        partitions: vec![7],
        read_epoch: epoch,
      })
      .unwrap();
    self.command_notify.notify_one();
    self.space_notify.notify_waiters();
  }
}

#[async_trait::async_trait]
impl ConsumerLifecycleHooks for CommandBeforeCapacityWaitHooks {
  async fn prefetch_capacity_exhausted(&self, _: &str, _: &[VirtualPartitionId]) {
    self.request_suspension();
  }

  async fn prefetch_batch_buffered(&self, _: &str, partition_id: VirtualPartitionId) {
    if partition_id == 7 {
      self.request_suspension();
    }
  }

  async fn read_suspension_applied(&self, _: &str, _: &[VirtualPartitionId]) {
    self.applied.store(true, Ordering::SeqCst);
  }
}

#[tokio::test]
async fn capacity_command_before_waiter_registration_does_not_wait_for_fallback() {
  let (mut worker, tx) = worker_fixture_with_budget(Some(3)).await;
  {
    let mut state = worker.shared_state.lock();
    state.delivery_state.batches.push_back(batch(7, 3));
    state.delivery_state.buffered_bytes = 3;
  }
  let hooks = CommandBeforeCapacityWaitHooks::new(&worker, tx);
  worker.lifecycle_hooks = Some(hooks.clone());
  let mut run = Box::pin(worker.run());
  let mut context = Context::from_waker(Waker::noop());
  assert!(matches!(run.as_mut().poll(&mut context), Poll::Pending));
  assert!(hooks.applied.load(Ordering::SeqCst));
}

#[tokio::test]
async fn pending_capacity_command_before_waiter_registration_does_not_wait_for_fallback() {
  let (mut worker, tx) = worker_fixture_with_budget(Some(8)).await;
  {
    let mut state = worker.shared_state.lock();
    state.delivery_state.batches.push_back(batch(8, 3));
    state.delivery_state.buffered_bytes = 3;
  }
  let hooks = CommandBeforeCapacityWaitHooks::new(&worker, tx);
  worker.lifecycle_hooks = Some(hooks.clone());
  let mut pending = VecDeque::from([batch(7, 3), batch(8, 3), batch(7, 3)]);
  let mut records = 3;
  let mut bytes = 9;
  {
    let mut admission =
      Box::pin(worker.admit_pending_batches(&mut pending, &mut records, &mut bytes, 8));
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(admission.as_mut().poll(&mut context), Poll::Ready(true));
  }
  apply_commands(&mut worker, &mut pending).await;
  assert!(hooks.applied.load(Ordering::SeqCst));
  assert_eq!(pending.len(), 1);
  assert_eq!(pending[0].virtual_partition_id, 8);
  assert_eq!(
    worker.shared_state.lock().delivery_state.retained_bytes(),
    3
  );
}
