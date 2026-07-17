#![allow(clippy::unwrap_used)]

use crate::write::{TopicInfo, WriteConfig, WriteEngineImpl};
use anyhow::Result;
use bd_server_stats::stats::Collector;
use bd_time::TestTimeProvider;
use blob_stream_blob_store::InMemoryBlobStore;
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  LeaseAcquireOutcome,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
};
use blob_stream_types::virtual_partition_for_logical;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::OffsetDateTime;
use tokio::sync::watch;

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
    "telemetry".to_string(),
    TopicInfo {
      name: "telemetry".to_string(),
      partition_count: 8,
      num_writers: 1,
      retention_days: 7,
      max_metadata_publication_lag_ms: 30_000,
    },
  );

  let solo_a = BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".to_string(),
    address: "10.0.0.1:8080".to_string(),
  }]);
  let owned_solo = WriteEngineImpl::owned_virtual_partitions(&topics, 0, "node-a", &solo_a);
  assert_eq!(owned_solo.len(), 8);

  let split = BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".to_string(),
      address: "10.0.0.1:8080".to_string(),
    },
    BrokerNode {
      node_id: "node-b".to_string(),
      address: "10.0.0.2:8080".to_string(),
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
    node_id: "node-b".to_string(),
    address: "10.0.0.2:8080".to_string(),
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
    "telemetry".to_string(),
    TopicInfo {
      name: "telemetry".to_string(),
      partition_count: 2,
      num_writers: 2,
      retention_days: 7,
      max_metadata_publication_lag_ms: 30_000,
    },
  );
  let membership = BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".to_string(),
      address: "10.0.0.1:8080".to_string(),
    },
    BrokerNode {
      node_id: "node-b".to_string(),
      address: "10.0.0.2:8080".to_string(),
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
    HashSet::from([("telemetry".to_string(), 2), ("telemetry".to_string(), 3)])
  );
}

fn make_topic(partition_count: u32) -> HashMap<String, TopicInfo> {
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".to_string(),
    TopicInfo {
      name: "telemetry".to_string(),
      partition_count,
      num_writers: 1,
      retention_days: 7,
      max_metadata_publication_lag_ms: 30_000,
    },
  );
  topics
}

async fn acquire_all_partitions(
  store: &InMemoryProducerPartitionLeaseStore,
  holder_id: &str,
  partition_count: u32,
) {
  for logical_partition_id in 0 .. partition_count {
    let virtual_partition_id =
      virtual_partition_for_logical(logical_partition_id, partition_count, 0);
    let key = ProducerPartitionLeaseKey {
      topic: "telemetry".to_string(),
      virtual_partition_id,
    };
    let _outcome = store
      .acquire_lease(key, holder_id.to_string(), 1_000, 60_000)
      .await
      .expect("acquire lease");
  }
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
      topic: "telemetry".to_string(),
      virtual_partition_id,
    };
    let outcome = store
      .acquire_lease(key, holder_id.to_string(), 1_000, 60_000)
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
      topic: "telemetry".to_string(),
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
      topic: "telemetry".to_string(),
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
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ts_ms)));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration_ms = 60_000;
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::default());

  let engine = WriteEngineImpl::new_with_time_provider(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "node-a".to_string(),
    Some(membership_rx),
    time_provider,
    &metrics_scope(),
  )?;

  tokio::time::sleep(StdDuration::from_millis(50)).await;
  assert!(!all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms).await);

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".to_string(),
    address: "10.0.0.2:8080".to_string(),
  }]))?;
  tokio::time::sleep(StdDuration::from_millis(50)).await;
  assert!(!all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms).await);

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".to_string(),
    address: "10.0.0.1:8080".to_string(),
  }]))?;
  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms)
    })
    .await,
    "leases were not acquired after node-a appeared in membership"
  );

  drop(engine);
  Ok(())
}

#[tokio::test]
async fn lease_assignment_reacquires_partitions_after_membership_flap() -> Result<()> {
  let partition_count = 4;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let now_ts_ms = 1_000;
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ts_ms)));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration_ms = 60_000;
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".to_string(),
    address: "10.0.0.1:8080".to_string(),
  }]));

  let engine = WriteEngineImpl::new_with_time_provider(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "node-a".to_string(),
    Some(membership_rx),
    time_provider,
    &metrics_scope(),
  )?;

  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms)
    })
    .await,
    "node-a did not acquire its initial leases"
  );

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".to_string(),
    address: "10.0.0.2:8080".to_string(),
  }]))?;
  assert!(
    wait_for_all_partitions(|| all_partitions_expired(&lease_store, partition_count, now_ts_ms))
      .await,
    "node-a did not release leases after losing membership"
  );

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".to_string(),
    address: "10.0.0.1:8080".to_string(),
  }]))?;
  assert!(
    wait_for_all_partitions(|| {
      all_partitions_held_by(&lease_store, "node-a", partition_count, now_ts_ms)
    })
    .await,
    "node-a did not reacquire leases after rejoining membership"
  );

  drop(engine);
  Ok(())
}

#[tokio::test]
async fn scale_down_releases_previously_owned_leases() -> Result<()> {
  let partition_count = 4;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_000)));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration_ms = 60_000;

  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".to_string(),
    address: "10.0.0.1:8080".to_string(),
  }]));

  let engine = WriteEngineImpl::new_with_time_provider(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "node-a".to_string(),
    Some(membership_rx),
    time_provider,
    &metrics_scope(),
  )?;

  acquire_all_partitions(&lease_store, "node-a", partition_count).await;
  tokio::time::sleep(StdDuration::from_millis(50)).await;

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".to_string(),
    address: "10.0.0.2:8080".to_string(),
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

  drop(engine);
  Ok(())
}

#[tokio::test]
async fn shutdown_releases_currently_owned_leases() -> Result<()> {
  let partition_count = 4;
  let topics = make_topic(partition_count);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_000)));
  let mut config = WriteConfig::with_defaults();
  config.lease_duration_ms = 60_000;

  let (_membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".to_string(),
    address: "10.0.0.1:8080".to_string(),
  }]));

  let engine = WriteEngineImpl::new_with_time_provider(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store.clone(),
    "node-a".to_string(),
    Some(membership_rx),
    time_provider,
    &metrics_scope(),
  )?;

  acquire_all_partitions(&lease_store, "node-a", partition_count).await;

  drop(engine);

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
