#![allow(clippy::unwrap_used)]

use crate::write::{TopicInfo, WriteConfig, WriteEngineBuilder, WriteEngineImpl};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use bd_shutdown::ComponentShutdownTrigger;
use bd_time::TimeProvider;
use blob_stream_blob_store::InMemoryBlobStore;
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  ProducerLeaseFence,
  ProducerPartitionLease,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  ProducerSequenceProgress,
  SequenceReservationOutcome,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{
  VirtualPartitionId,
  offset_datetime_from_unix_millis,
  virtual_partition_for_logical,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use protobuf::Chars;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration as StdDuration;
use time::{Duration, OffsetDateTime};
use tokio::sync::{Semaphore, mpsc, watch};

#[test]
fn acquisition_retry_ignores_in_flight_partitions() {
  let initial_time = offset_datetime_from_unix_millis(1_000);
  let due_partition = (Chars::from("due"), 0);
  let later_partition = (Chars::from("later"), 1);
  let pending_acquisitions = HashMap::from([
    (
      due_partition.clone(),
      super::LeaseRetrySchedule::new(initial_time),
    ),
    (
      later_partition.clone(),
      super::LeaseRetrySchedule::new(initial_time + Duration::milliseconds(250)),
    ),
  ]);

  assert_eq!(
    super::next_acquisition_retry_at(&pending_acquisitions, &HashSet::new()),
    Some(initial_time)
  );
  assert_eq!(
    super::next_acquisition_retry_at(&pending_acquisitions, &HashSet::from([due_partition]),),
    Some(initial_time + Duration::milliseconds(250))
  );
  assert_eq!(
    super::next_acquisition_retry_at(&pending_acquisitions, &HashSet::from([later_partition]),),
    Some(initial_time)
  );
}

struct BlockingReleaseLeaseStore {
  inner: InMemoryProducerPartitionLeaseStore,
  started_tx: mpsc::UnboundedSender<VirtualPartitionId>,
  release: Arc<Semaphore>,
}

struct TransientHeartbeatLeaseStore {
  inner: InMemoryProducerPartitionLeaseStore,
  heartbeat_failures_remaining: AtomicUsize,
}

//
// TerminalReleaseLeaseStore
//

/// Returns a selected terminal outcome while recording attempted lease releases.
struct TerminalReleaseLeaseStore {
  release_calls: AtomicUsize,
  outcome: LeaseReleaseOutcome,
}

#[async_trait]
impl ProducerPartitionLeaseStore for TerminalReleaseLeaseStore {
  async fn get_lease(
    &self,
    _key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>> {
    Ok(None)
  }

  async fn acquire_lease(
    &self,
    _key: ProducerPartitionLeaseKey,
    _holder_id: String,
    _lease_session_id: String,
    _now: OffsetDateTime,
    _lease_duration: Duration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireOutcome> {
    let _ = sequence_progress;
    panic!("terminal release test does not acquire leases")
  }

  async fn acquire_lease_and_reserve_sequences(
    &self,
    _key: ProducerPartitionLeaseKey,
    _holder_id: String,
    _lease_session_id: String,
    _now: OffsetDateTime,
    _lease_duration: Duration,
    _reservation_size: Option<u64>,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireAndReserveOutcome> {
    let _ = sequence_progress;
    panic!("terminal release test does not acquire leases")
  }

  async fn heartbeat_lease(
    &self,
    _key: &ProducerPartitionLeaseKey,
    _holder_id: &str,
    _lease_session_id: &str,
    _now: OffsetDateTime,
    _lease_duration: Duration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseHeartbeatOutcome> {
    let _ = sequence_progress;
    panic!("terminal release test does not heartbeat leases")
  }

  async fn reserve_sequences(
    &self,
    _key: &ProducerPartitionLeaseKey,
    _holder_id: &str,
    _lease_session_id: &str,
    _now: OffsetDateTime,
    _reservation_size: u64,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<SequenceReservationOutcome> {
    let _ = sequence_progress;
    panic!("terminal release test does not reserve sequences")
  }

  async fn release_lease(
    &self,
    _key: &ProducerPartitionLeaseKey,
    _holder_id: &str,
    _lease_session_id: &str,
    _now: OffsetDateTime,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseReleaseOutcome> {
    let _ = sequence_progress;
    self.release_calls.fetch_add(1, Ordering::AcqRel);
    Ok(self.outcome.clone())
  }
}

#[async_trait]
impl ProducerPartitionLeaseStore for TransientHeartbeatLeaseStore {
  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>> {
    self.inner.get_lease(key).await
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
    self
      .inner
      .acquire_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
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
    self
      .inner
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        reservation_size,
        sequence_progress,
      )
      .await
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
    if self
      .heartbeat_failures_remaining
      .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
        remaining.checked_sub(1)
      })
      .is_ok()
    {
      return Err(anyhow!("injected transient heartbeat failure"));
    }
    self
      .inner
      .heartbeat_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
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
    self
      .inner
      .reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        reservation_size,
        sequence_progress,
      )
      .await
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<blob_stream_metadata_store::LeaseReleaseOutcome> {
    self
      .inner
      .release_lease(key, holder_id, lease_session_id, now, sequence_progress)
      .await
  }
}

#[async_trait]
impl ProducerPartitionLeaseStore for BlockingReleaseLeaseStore {
  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>> {
    self.inner.get_lease(key).await
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
    self
      .inner
      .acquire_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
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
    self
      .inner
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        reservation_size,
        sequence_progress,
      )
      .await
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
    self
      .inner
      .heartbeat_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
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
    self
      .inner
      .reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        reservation_size,
        sequence_progress,
      )
      .await
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<blob_stream_metadata_store::LeaseReleaseOutcome> {
    self
      .started_tx
      .send(key.virtual_partition_id)
      .expect("parallel release test receiver remains open");
    self
      .release
      .acquire()
      .await
      .expect("parallel release test gate remains open")
      .forget();
    self
      .inner
      .release_lease(key, holder_id, lease_session_id, now, sequence_progress)
      .await
  }
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_broker_test")
}

#[test]
fn ownership_changes_with_membership() {
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count: 8,
      num_writers: 1,
      retention: Duration::days(7),
      max_metadata_publication_lag: Duration::seconds(30),
      metadata_window_size: Duration::minutes(5),
    },
  );

  let solo_a = BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]);
  let owned_solo = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-a", &solo_a);
  assert_eq!(owned_solo.len(), 8);

  let split = BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".into(),
      address: "10.0.0.1:8080".into(),
    },
    BrokerNode {
      node_id: "node-b".into(),
      address: "10.0.0.2:8080".into(),
    },
  ]);

  let owned_a = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-a", &split)
    .into_iter()
    .collect::<HashSet<_>>();
  let owned_b = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-b", &split)
    .into_iter()
    .collect::<HashSet<_>>();

  assert!(!owned_a.is_empty());
  assert!(!owned_b.is_empty());
  assert!(owned_a.is_disjoint(&owned_b));
  assert_eq!(owned_a.union(&owned_b).count(), 8);

  let solo_b = BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]);
  let owned_after_move = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-a", &solo_b);
  assert!(owned_after_move.is_empty());

  let owned_empty = WriteEngineImpl::owned_virtual_partitions(
    &topics,
    0,
    "node-a",
    &BrokerMembership::new(Vec::new()),
  );
  assert!(owned_empty.is_empty());
}

#[test]
fn ownership_includes_only_local_producer_writer_virtual_partitions() {
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count: 2,
      num_writers: 2,
      retention: Duration::days(7),
      max_metadata_publication_lag: Duration::seconds(30),
      metadata_window_size: Duration::minutes(5),
    },
  );
  let membership = BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".into(),
      address: "10.0.0.1:8080".into(),
    },
    BrokerNode {
      node_id: "node-b".into(),
      address: "10.0.0.2:8080".into(),
    },
  ]);

  let owned_a = WriteEngineImpl::owned_virtual_partitions(&topics, 1, "node-a", &membership)
    .into_iter()
    .collect::<HashSet<_>>();
  let owned_b = WriteEngineImpl::owned_virtual_partitions(&topics, 1, "node-b", &membership)
    .into_iter()
    .collect::<HashSet<_>>();

  assert_eq!(owned_a.len(), 1);
  assert_eq!(owned_b.len(), 1);
  assert!(owned_a.is_disjoint(&owned_b));
  assert_eq!(
    owned_a.union(&owned_b).cloned().collect::<HashSet<_>>(),
    HashSet::from([("telemetry".into(), 2), ("telemetry".into(), 3)])
  );
}

fn make_topic(partition_count: u32) -> HashMap<Chars, TopicInfo> {
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count,
      num_writers: 1,
      retention: Duration::days(7),
      max_metadata_publication_lag: Duration::seconds(30),
      metadata_window_size: Duration::minutes(5),
    },
  );
  topics
}

async fn all_partitions_acquired(
  store: &InMemoryProducerPartitionLeaseStore,
  holder_id: &str,
  partition_count: u32,
) -> bool {
  for logical_partition_id in 0 .. partition_count {
    let virtual_partition_id =
      virtual_partition_for_logical(logical_partition_id, partition_count, 0);
    let key = ProducerPartitionLeaseKey {
      topic: "telemetry".into(),
      virtual_partition_id,
    };
    let outcome = store
      .acquire_lease(
        key,
        holder_id.to_string(),
        holder_id.to_string(),
        offset_datetime_from_unix_millis(1_000),
        Duration::seconds(60),
        ProducerSequenceProgress::default(),
      )
      .await
      .expect("acquire lease");
    if !matches!(outcome, LeaseAcquireOutcome::Acquired(_)) {
      return false;
    }
  }

  true
}

async fn all_partitions_held_by(
  store: &InMemoryProducerPartitionLeaseStore,
  holder_id: &str,
  partition_count: u32,
  now: OffsetDateTime,
) -> bool {
  for logical_partition_id in 0 .. partition_count {
    let virtual_partition_id =
      virtual_partition_for_logical(logical_partition_id, partition_count, 0);
    let key = ProducerPartitionLeaseKey {
      topic: "telemetry".into(),
      virtual_partition_id,
    };
    let Ok(Some(lease)) = store.get_lease(&key).await else {
      return false;
    };
    if lease.fence.holder_id != holder_id || lease.lease_expiration_at <= now {
      return false;
    }
  }

  true
}

async fn all_partitions_expired(
  store: &InMemoryProducerPartitionLeaseStore,
  partition_count: u32,
  now: OffsetDateTime,
) -> bool {
  for logical_partition_id in 0 .. partition_count {
    let virtual_partition_id =
      virtual_partition_for_logical(logical_partition_id, partition_count, 0);
    let key = ProducerPartitionLeaseKey {
      topic: "telemetry".into(),
      virtual_partition_id,
    };
    let Ok(Some(lease)) = store.get_lease(&key).await else {
      return false;
    };
    if lease.lease_expiration_at > now {
      return false;
    }
  }

  true
}

async fn release_partition_leases(
  lease_store: &Arc<dyn ProducerPartitionLeaseStore>,
  state: &Arc<parking_lot::Mutex<super::super::super::state::WriteState>>,
  flush_notifier: &Arc<tokio::sync::Notify>,
  metrics: &super::super::super::metrics::WriteMetrics,
  holder_id: &str,
  lease_session_id: &str,
  partitions: Vec<(Chars, VirtualPartitionId, super::LeaseReleaseReason)>,
  time_provider: &dyn TimeProvider,
  lease_duration: Duration,
  heartbeat_interval: Duration,
  lifecycle_hooks: Option<&Arc<dyn super::super::super::BrokerLifecycleHooks>>,
) {
  let mut releases = FuturesUnordered::new();
  for (topic, virtual_partition_id, release_reason) in partitions {
    releases.push(async move {
      WriteEngineImpl::release_partition_lease(
        lease_store,
        state,
        flush_notifier,
        metrics,
        holder_id,
        lease_session_id,
        &topic,
        virtual_partition_id,
        release_reason,
        time_provider,
        lease_duration,
        heartbeat_interval,
        lifecycle_hooks,
      )
      .await;
    });
  }

  while releases.next().await.is_some() {}
}

#[tokio::test]
async fn releases_partitions_in_parallel_after_their_drains_complete() {
  let (started_tx, mut started_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let lease_store: Arc<dyn ProducerPartitionLeaseStore> = Arc::new(BlockingReleaseLeaseStore {
    inner: InMemoryProducerPartitionLeaseStore::new(),
    started_tx,
    release: Arc::clone(&release),
  });
  let state = Arc::new(parking_lot::Mutex::new(
    super::super::super::state::WriteState::default(),
  ));
  {
    let mut state = state.lock();
    state.partition_state_mut("telemetry", 0);
    state.partition_state_mut("telemetry", 1);
  }
  let flush_notifier = Arc::new(tokio::sync::Notify::new());
  let metrics = super::super::super::metrics::WriteMetrics::new(&metrics_scope());
  let time_provider = ManualTimeProvider::new(offset_datetime_from_unix_millis(1_000));
  let lifecycle_hooks: Option<Arc<dyn super::super::super::BrokerLifecycleHooks>> =
    Some(Arc::new(super::super::super::NoopBrokerLifecycleHooks));
  let releases = release_partition_leases(
    &lease_store,
    &state,
    &flush_notifier,
    &metrics,
    "node-a",
    "session-a",
    vec![
      (
        "telemetry".into(),
        0,
        super::LeaseReleaseReason::AssignmentLoss,
      ),
      (
        "telemetry".into(),
        1,
        super::LeaseReleaseReason::AssignmentLoss,
      ),
    ],
    &time_provider,
    Duration::seconds(60),
    Duration::seconds(10),
    lifecycle_hooks.as_ref(),
  );
  tokio::pin!(releases);

  let first = tokio::select! {
    () = &mut releases => panic!("releases completed before reaching the release gates"),
    partition = started_rx.recv() => partition.expect("first release started"),
  };
  let second = tokio::select! {
    () = &mut releases => panic!("releases completed before reaching the release gates"),
    partition = started_rx.recv() => partition.expect("second release started"),
  };
  assert_eq!(HashSet::from([first, second]), HashSet::from([0, 1]));
  release.add_permits(2);
  releases.await;
}

#[tokio::test]
async fn terminal_release_outcomes_retire_unassigned_partition_state() {
  let now = offset_datetime_from_unix_millis(1_000);
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };
  let held_by_other = ProducerPartitionLease {
    key: key.clone(),
    fence: ProducerLeaseFence {
      holder_id: "node-b".to_string(),
      lease_epoch: 2,
      lease_session_id: "session-b".to_string(),
    },
    lease_expiration_at: now + Duration::seconds(60),
    max_allocated_seq: None,
    reservation_start: None,
    last_handed_out_seq: None,
    sequence_progress_updated_at: None,
  };

  for outcome in [
    LeaseReleaseOutcome::Released,
    LeaseReleaseOutcome::Expired,
    LeaseReleaseOutcome::HeldByOther(held_by_other),
  ] {
    let lease_store_impl = Arc::new(TerminalReleaseLeaseStore {
      release_calls: AtomicUsize::new(0),
      outcome,
    });
    let lease_store: Arc<dyn ProducerPartitionLeaseStore> = lease_store_impl.clone();
    let state = Arc::new(parking_lot::Mutex::new(
      super::super::super::state::WriteState::default(),
    ));
    state.lock().partition_state_mut("telemetry", 0);
    let flush_notifier = Arc::new(tokio::sync::Notify::new());
    let metrics = super::super::super::metrics::WriteMetrics::new(&metrics_scope());
    let time_provider = ManualTimeProvider::new(now);

    // Each terminal result retires state that no longer belongs to this broker. Calling release
    // again models the next reconciliation pass and must not issue a second store operation.
    for _ in 0 .. 2 {
      WriteEngineImpl::release_partition_lease(
        &lease_store,
        &state,
        &flush_notifier,
        &metrics,
        "node-a",
        "session-a",
        &key.topic,
        key.virtual_partition_id,
        super::LeaseReleaseReason::DefensiveReconciliation,
        &time_provider,
        Duration::seconds(60),
        Duration::seconds(10),
        None,
      )
      .await;
    }
    assert!(state.lock().partition_keys().is_empty());
    assert_eq!(lease_store_impl.release_calls.load(Ordering::Acquire), 1);
  }
}

#[tokio::test]
async fn transient_drain_heartbeat_failure_retries_before_lease_expiry() -> Result<()> {
  let initial_time = offset_datetime_from_unix_millis(1_000);
  let time_provider = ManualTimeProvider::new(initial_time);
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };
  let lease_store: Arc<dyn ProducerPartitionLeaseStore> = Arc::new(TransientHeartbeatLeaseStore {
    inner: InMemoryProducerPartitionLeaseStore::new(),
    heartbeat_failures_remaining: AtomicUsize::new(1),
  });
  lease_store
    .acquire_lease(
      key.clone(),
      "node-a".to_string(),
      "session-a".to_string(),
      initial_time,
      Duration::seconds(10),
      ProducerSequenceProgress::default(),
    )
    .await?;
  let state = Arc::new(parking_lot::Mutex::new(
    super::super::super::state::WriteState::default(),
  ));
  {
    let mut state = state.lock();
    let partition_state = state.partition_state_mut("telemetry", 0);
    partition_state.outstanding_flushes = 1;
    partition_state.lease_expiration_at = Some(initial_time + Duration::seconds(10));
  }

  let wait_for_drain = WriteEngineImpl::wait_for_partition_drain(
    &lease_store,
    &state,
    &key,
    "node-a",
    "session-a",
    &time_provider,
    initial_time + Duration::seconds(5),
    Duration::seconds(10),
    Duration::seconds(5),
  );
  tokio::pin!(wait_for_drain);

  let initial_sleep = tokio::select! {
    () = &mut wait_for_drain => panic!("drain completed before the initial heartbeat"),
    registration = time_provider.wait_for_sleep_registration_after(0) => registration,
  };
  time_provider.advance(Duration::seconds(5));
  let retry_sleep = tokio::select! {
    () = &mut wait_for_drain => panic!("drain completed before retrying the failed heartbeat"),
    registration = time_provider.wait_for_sleep_registration_after(initial_sleep) => registration,
  };
  time_provider.advance(Duration::milliseconds(250));
  tokio::select! {
    () = &mut wait_for_drain => panic!("drain completed before retrying the failed heartbeat"),
    registration = time_provider.wait_for_sleep_registration_after(retry_sleep) => {
      let _ = registration;
    },
  }
  time_provider.advance(Duration::seconds(5));

  let takeover = lease_store
    .acquire_lease(
      key.clone(),
      "node-b".to_string(),
      "session-b".to_string(),
      time_provider.now(),
      Duration::seconds(10),
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(takeover, LeaseAcquireOutcome::HeldByOther(_)));

  {
    let mut state = state.lock();
    let partition_state = state.partition_state_mut("telemetry", 0);
    partition_state.outstanding_flushes = 0;
    partition_state.drain_notify.notify_waiters();
  }
  wait_for_drain.await;
  Ok(())
}

async fn wait_for_all_partitions(mut predicate: impl AsyncFnMut() -> bool) -> bool {
  for _ in 0 .. 300 {
    if predicate().await {
      return true;
    }
    tokio::time::sleep(StdDuration::from_millis(20)).await;
  }

  false
}

#[tokio::test]
async fn lease_assignment_waits_for_initialized_self_membership() -> Result<()> {
  let partition_count = 4;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let now_ts_ms = 1_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ts_ms,
  )));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration = Duration::seconds(60);
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::default());
  let shutdown_trigger = ComponentShutdownTrigger::default();

  let _engine = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "node-a".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .membership_rx(membership_rx)
  .time_provider(time_provider)
  .build()?;

  tokio::time::sleep(StdDuration::from_millis(50)).await;
  assert!(
    !all_partitions_held_by(
      &lease_store,
      "node-a",
      partition_count,
      offset_datetime_from_unix_millis(now_ts_ms)
    )
    .await
  );

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  tokio::time::sleep(StdDuration::from_millis(50)).await;
  assert!(
    !all_partitions_held_by(
      &lease_store,
      "node-a",
      partition_count,
      offset_datetime_from_unix_millis(now_ts_ms)
    )
    .await
  );

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]))?;
  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(
        &lease_store,
        "node-a",
        partition_count,
        offset_datetime_from_unix_millis(now_ts_ms),
      )
    })
    .await,
    "leases were not acquired after node-a appeared in membership"
  );

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn lease_assignment_reacquires_partitions_after_membership_flap() -> Result<()> {
  let partition_count = 4;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let now_ts_ms = 1_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ts_ms,
  )));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration = Duration::seconds(60);
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let shutdown_trigger = ComponentShutdownTrigger::default();

  let _engine = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "node-a".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .membership_rx(membership_rx)
  .time_provider(time_provider)
  .build()?;

  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(
        &lease_store,
        "node-a",
        partition_count,
        offset_datetime_from_unix_millis(now_ts_ms),
      )
    })
    .await,
    "node-a did not acquire its initial leases"
  );

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  assert!(
    wait_for_all_partitions(|| all_partitions_expired(
      &lease_store,
      partition_count,
      offset_datetime_from_unix_millis(now_ts_ms)
    ))
    .await,
    "node-a did not release leases after losing membership"
  );

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]))?;
  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(
        &lease_store,
        "node-a",
        partition_count,
        offset_datetime_from_unix_millis(now_ts_ms),
      )
    })
    .await,
    "node-a did not reacquire leases after rejoining membership"
  );

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn lease_assignment_retries_held_partition_before_heartbeat() -> Result<()> {
  let partition_count = 1;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let initial_time = offset_datetime_from_unix_millis(1_000);
  let time_provider = Arc::new(ManualTimeProvider::new(initial_time));
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: virtual_partition_for_logical(0, partition_count, 0),
  };
  lease_store
    .acquire_lease(
      key.clone(),
      "old-node".to_string(),
      "old-session".to_string(),
      initial_time,
      Duration::seconds(60),
      ProducerSequenceProgress::default(),
    )
    .await?;

  let mut config = WriteConfig::with_defaults();
  config.lease_duration = Duration::seconds(60);
  config.heartbeat_interval = Duration::seconds(40);
  let (_membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "new-node".into(),
    address: "10.0.0.2:8080".into(),
  }]));
  let shutdown_trigger = ComponentShutdownTrigger::default();

  let _engine = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "new-node".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .membership_rx(membership_rx)
  .lease_session_id("new-session".to_string())
  .time_provider(time_provider.clone())
  .build()?;

  time_provider.wait_until_sleeping(2).await;
  lease_store
    .release_lease(
      &key,
      "old-node",
      "old-session",
      initial_time,
      ProducerSequenceProgress::default(),
    )
    .await?;
  time_provider.advance(Duration::milliseconds(250));

  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(
        &lease_store,
        "new-node",
        partition_count,
        initial_time + Duration::milliseconds(250),
      )
    })
    .await,
    "new node did not retry lease acquisition before its heartbeat interval"
  );

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn lease_assignment_does_not_retry_held_partition_on_early_heartbeat() -> Result<()> {
  let partition_count = 1;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let initial_time = offset_datetime_from_unix_millis(1_000);
  let time_provider = Arc::new(ManualTimeProvider::new(initial_time));
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: virtual_partition_for_logical(0, partition_count, 0),
  };
  lease_store
    .acquire_lease(
      key.clone(),
      "old-node".to_string(),
      "old-session".to_string(),
      initial_time,
      Duration::seconds(60),
      ProducerSequenceProgress::default(),
    )
    .await?;

  let mut config = WriteConfig::with_defaults();
  config.lease_duration = Duration::seconds(60);
  config.heartbeat_interval = Duration::milliseconds(10);
  let (_membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "new-node".into(),
    address: "10.0.0.2:8080".into(),
  }]));
  let shutdown_trigger = ComponentShutdownTrigger::default();

  let _engine = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "new-node".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .membership_rx(membership_rx)
  .lease_session_id("new-session".to_string())
  .time_provider(time_provider.clone())
  .build()?;

  time_provider.wait_until_sleeping(2).await;
  lease_store
    .release_lease(
      &key,
      "old-node",
      "old-session",
      initial_time,
      ProducerSequenceProgress::default(),
    )
    .await?;

  tokio::time::advance(StdDuration::from_millis(10)).await;
  tokio::task::yield_now().await;
  assert!(
    !all_partitions_held_by(&lease_store, "new-node", partition_count, initial_time).await,
    "heartbeat retried lease acquisition before the scheduled retry"
  );

  time_provider.advance(Duration::milliseconds(250));
  tokio::task::yield_now().await;
  assert!(
    all_partitions_held_by(
      &lease_store,
      "new-node",
      partition_count,
      initial_time + Duration::milliseconds(250)
    )
    .await,
    "scheduled retry did not acquire the released lease"
  );

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn scale_down_releases_previously_owned_leases() -> Result<()> {
  let partition_count = 4;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_000,
  )));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration = Duration::seconds(60);

  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let shutdown_trigger = ComponentShutdownTrigger::default();

  let _engine = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "node-a".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .membership_rx(membership_rx)
  .lease_session_id("session-a".to_string())
  .time_provider(time_provider)
  .build()?;

  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(
        &lease_store,
        "node-a",
        partition_count,
        offset_datetime_from_unix_millis(1_000),
      )
    })
    .await,
    "node-a did not acquire its initial leases"
  );

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;

  let mut converged = false;
  for _ in 0 .. 300 {
    if all_partitions_acquired(&lease_store, "node-b", partition_count).await {
      converged = true;
      break;
    }
    tokio::time::sleep(StdDuration::from_millis(20)).await;
  }
  assert!(converged, "leases did not converge to node-b");

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn shutdown_releases_currently_owned_leases() -> Result<()> {
  let partition_count = 4;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_000,
  )));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration = Duration::seconds(60);

  let (_membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let shutdown_trigger = ComponentShutdownTrigger::default();

  let _engine = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "node-a".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .membership_rx(membership_rx)
  .lease_session_id("session-a".to_string())
  .time_provider(time_provider)
  .build()?;

  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(
        &lease_store,
        "node-a",
        partition_count,
        offset_datetime_from_unix_millis(1_000),
      )
    })
    .await,
    "node-a did not acquire its initial leases"
  );

  shutdown_trigger.shutdown().await;

  let mut released = false;
  for _ in 0 .. 300 {
    if all_partitions_acquired(&lease_store, "node-b", partition_count).await {
      released = true;
      break;
    }
    tokio::time::sleep(StdDuration::from_millis(20)).await;
  }
  assert!(released, "shutdown did not release leases promptly");

  Ok(())
}

#[tokio::test]
async fn reconciliation_releases_a_tracked_legacy_lease_outside_the_assignment() -> Result<()> {
  let now = offset_datetime_from_unix_millis(1_000);
  let time_provider = Arc::new(ManualTimeProvider::new(now));
  let membership = BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".into(),
      address: "10.0.0.1:8080".into(),
    },
    BrokerNode {
      node_id: "node-b".into(),
      address: "10.0.0.2:8080".into(),
    },
  ]);
  let topics = make_topic(2);
  let assigned_to_a = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-a", &membership)
    .into_iter()
    .collect::<HashSet<_>>();
  let unassigned_partition = (0 .. 2)
    .find(|partition| !assigned_to_a.contains(&(Chars::from("telemetry"), *partition)))
    .expect("two-node assignment leaves one partition for node-b");
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: unassigned_partition,
  };
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::default());
  let shutdown_trigger = ComponentShutdownTrigger::default();

  let engine = WriteEngineBuilder::new(
    WriteConfig::with_defaults(),
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "node-a".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .membership_rx(membership_rx)
  .lease_session_id("session-a".to_string())
  .time_provider(time_provider.clone())
  .build()?;

  // Seed the state produced by the old inline path before publishing membership. The partition is
  // not in node-a's plan, so a delta-only reconciler would never attempt this prompt release.
  lease_store
    .acquire_lease(
      key.clone(),
      "node-a".to_string(),
      "session-a".to_string(),
      now,
      Duration::seconds(60),
      ProducerSequenceProgress::default(),
    )
    .await?;
  {
    let mut state = engine.state.lock();
    let partition_state = state.partition_state_mut("telemetry", unassigned_partition);
    partition_state.lease_expiration_at = Some(now + Duration::seconds(60));
  }

  membership_tx.send(membership)?;
  assert!(
    wait_for_all_partitions(|| async {
      matches!(
        lease_store
          .acquire_lease(
            key.clone(),
            "node-b".to_string(),
            "session-b".to_string(),
            now,
            Duration::seconds(60),
            ProducerSequenceProgress::default(),
          )
          .await,
        Ok(LeaseAcquireOutcome::Acquired(_))
      )
    })
    .await,
    "legacy lease was not released when membership became authoritative"
  );
  assert!(
    engine
      .state
      .lock()
      .partition_state("telemetry", unassigned_partition)
      .is_none(),
    "terminal reconciliation must retire state so later heartbeats do not release it again"
  );

  Ok(())
}
