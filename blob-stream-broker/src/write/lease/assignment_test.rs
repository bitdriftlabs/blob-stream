#![allow(clippy::unwrap_used)]

use crate::write::{TopicInfo, WriteConfig, WriteEngineBuilder, WriteEngineImpl};
use anyhow::Result;
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use bd_shutdown::ComponentShutdownTrigger;
use blob_stream_blob_store::InMemoryBlobStore;
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  ProducerPartitionLease,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SequenceReservationOutcome,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{VirtualPartitionId, virtual_partition_for_logical};
use protobuf::Chars;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::OffsetDateTime;
use tokio::sync::{Semaphore, mpsc, watch};

struct BlockingReleaseLeaseStore {
  inner: InMemoryProducerPartitionLeaseStore,
  started_tx: mpsc::UnboundedSender<VirtualPartitionId>,
  release: Arc<Semaphore>,
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
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseAcquireOutcome> {
    self
      .inner
      .acquire_lease(
        key,
        holder_id,
        lease_session_id,
        now_ts_ms,
        lease_duration_ms,
      )
      .await
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
    self
      .inner
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now_ts_ms,
        lease_duration_ms,
        reservation_size,
      )
      .await
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseHeartbeatOutcome> {
    self
      .inner
      .heartbeat_lease(
        key,
        holder_id,
        lease_session_id,
        now_ts_ms,
        lease_duration_ms,
      )
      .await
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now_ts_ms: i64,
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome> {
    self
      .inner
      .reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now_ts_ms,
        reservation_size,
      )
      .await
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now_ts_ms: i64,
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
      .release_lease(key, holder_id, lease_session_id, now_ts_ms)
      .await
  }
}

fn time_from_ms(ms: i64) -> OffsetDateTime {
  OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
    .unwrap_or(OffsetDateTime::UNIX_EPOCH)
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
      retention_days: 7,
      max_metadata_publication_lag_ms: 30_000,
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
      retention_days: 7,
      max_metadata_publication_lag_ms: 30_000,
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
      retention_days: 7,
      max_metadata_publication_lag_ms: 30_000,
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
        1_000,
        60_000,
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
  now_ts_ms: i64,
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
    if lease.holder_id != holder_id || lease.lease_expiration_ts_ms <= now_ts_ms {
      return false;
    }
  }

  true
}

async fn all_partitions_expired(
  store: &InMemoryProducerPartitionLeaseStore,
  partition_count: u32,
  now_ts_ms: i64,
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
    if lease.lease_expiration_ts_ms > now_ts_ms {
      return false;
    }
  }

  true
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
  let flush_notifier = Arc::new(tokio::sync::Notify::new());
  let metrics = super::super::super::metrics::WriteMetrics::new(&metrics_scope());
  let lifecycle_hooks: Option<Arc<dyn super::super::super::BrokerLifecycleHooks>> =
    Some(Arc::new(super::super::super::NoopBrokerLifecycleHooks));
  let releases = WriteEngineImpl::release_partition_leases(
    &lease_store,
    &state,
    &flush_notifier,
    &metrics,
    "node-a",
    "session-a",
    vec![("telemetry".into(), 0), ("telemetry".into(), 1)],
    1_000,
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
  let time_provider = Arc::new(ManualTimeProvider::new(time_from_ms(now_ts_ms)));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration_ms = 60_000;
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
  assert!(!all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms).await);

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  tokio::time::sleep(StdDuration::from_millis(50)).await;
  assert!(!all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms).await);

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]))?;
  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms)
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
  let time_provider = Arc::new(ManualTimeProvider::new(time_from_ms(now_ts_ms)));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration_ms = 60_000;
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
      all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms)
    })
    .await,
    "node-a did not acquire its initial leases"
  );

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  assert!(
    wait_for_all_partitions(|| all_partitions_expired(&lease_store, partition_count, now_ts_ms))
      .await,
    "node-a did not release leases after losing membership"
  );

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]))?;
  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms)
    })
    .await,
    "node-a did not reacquire leases after rejoining membership"
  );

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn scale_down_releases_previously_owned_leases() -> Result<()> {
  let partition_count = 4;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let time_provider = Arc::new(ManualTimeProvider::new(time_from_ms(1_000)));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration_ms = 60_000;

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
      all_partitions_held_by(&lease_store, "node-a", partition_count, 1_000)
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
  let time_provider = Arc::new(ManualTimeProvider::new(time_from_ms(1_000)));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration_ms = 60_000;

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
      all_partitions_held_by(&lease_store, "node-a", partition_count, 1_000)
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
