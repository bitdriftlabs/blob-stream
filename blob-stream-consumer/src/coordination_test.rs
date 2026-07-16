#![allow(clippy::unwrap_used)]

use crate::config::ConsumerGroupConfig;
use crate::coordination::{
  ConsumerGroupCoordinator,
  ConsumerGroupCoordinatorImpl,
  cooperative_sticky_assignment,
};
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  InMemoryConsumerGroupLeaseStore,
};
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn sticky_assignment_stable_for_same_membership() {
  let members = vec!["member-a".to_string(), "member-b".to_string()];
  let partitions = vec![0, 1, 2, 3, 4, 5];

  let first = cooperative_sticky_assignment(&members, &partitions, &HashMap::new(), "member-a");
  let second = cooperative_sticky_assignment(&members, &partitions, &first, "member-a");

  assert_eq!(first, second);
}

#[test]
fn sticky_assignment_moves_minimal_partitions_on_scale_out() {
  let initial_members = vec!["member-a".to_string(), "member-b".to_string()];
  let scaled_members = vec![
    "member-a".to_string(),
    "member-b".to_string(),
    "member-c".to_string(),
  ];
  let partitions = vec![0, 1, 2, 3, 4, 5];

  let before =
    cooperative_sticky_assignment(&initial_members, &partitions, &HashMap::new(), "member-a");
  let after = cooperative_sticky_assignment(&scaled_members, &partitions, &before, "member-a");

  let moved = partitions
    .iter()
    .filter(|partition_id| before.get(partition_id) != after.get(partition_id))
    .count();

  assert_eq!(moved, 2);
}

#[tokio::test]
async fn oversubscribed_consumers_keep_stable_assignments_without_fencing() {
  let store: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let members = (0 .. 4)
    .map(|member_index| format!("member-{member_index}"))
    .collect::<Vec<_>>();
  let partitions = vec![0, 1];
  let mut coordinators = members
    .iter()
    .map(|member_id| {
      ConsumerGroupCoordinatorImpl::new(
        ConsumerGroupConfig {
          topic: "topic-a".to_string().into(),
          group_id: "group-a".to_string().into(),
          member_id: member_id.clone().into(),
          lease_duration_ms: Some(1_000),
          heartbeat_interval_ms: Some(50),
          rebalance_interval_ms: Some(50),
          ..Default::default()
        },
        Arc::clone(&store),
      )
      .unwrap()
    })
    .collect::<Vec<_>>();

  let mut expected_ownership = Vec::with_capacity(coordinators.len());
  for coordinator in &mut coordinators {
    let report = coordinator
      .rebalance(members.clone(), partitions.clone(), 1_000)
      .await
      .unwrap();
    expected_ownership.push(report.owned_partitions);
  }

  assert_eq!(
    expected_ownership
      .iter()
      .flatten()
      .copied()
      .collect::<std::collections::HashSet<_>>()
      .len(),
    partitions.len()
  );
  assert_eq!(
    expected_ownership
      .iter()
      .filter(|ownership| ownership.is_empty())
      .count(),
    members.len() - partitions.len()
  );

  for coordinator in &mut coordinators {
    let report = coordinator
      .heartbeat_and_commit(1_010, &HashMap::new())
      .await
      .unwrap();
    assert!(report.fenced_partitions.is_empty());
  }

  for (coordinator, ownership) in coordinators.iter_mut().zip(&expected_ownership) {
    let report = coordinator
      .rebalance(members.clone(), partitions.clone(), 1_020)
      .await
      .unwrap();
    assert_eq!(report.owned_partitions, *ownership);
    assert_eq!(coordinator.generation(), 1);

    let heartbeat = coordinator
      .heartbeat_and_commit(1_030, &HashMap::new())
      .await
      .unwrap();
    assert_eq!(heartbeat.renewed_partitions, *ownership);
    assert!(heartbeat.fenced_partitions.is_empty());
  }
}

#[tokio::test]
async fn heartbeat_commit_renews_and_commits_cursor() {
  let store: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let mut coordinator = ConsumerGroupCoordinatorImpl::new(
    ConsumerGroupConfig {
      topic: "topic-a".to_string().into(),
      group_id: "group-a".to_string().into(),
      member_id: "member-a".to_string().into(),
      lease_duration_ms: Some(100),
      heartbeat_interval_ms: Some(50),
      rebalance_interval_ms: Some(50),
      ..Default::default()
    },
    Arc::clone(&store),
  )
  .unwrap();

  let owned = coordinator
    .rebalance(vec!["member-a".to_string()], vec![7], 1_000)
    .await
    .unwrap();
  assert_eq!(owned.owned_partitions, vec![7]);
  assert!(owned.committed_cursors.is_empty());

  let report = coordinator
    .heartbeat_and_commit(1_010, &HashMap::from([(7_u32, 10_u64)]))
    .await
    .unwrap();
  assert_eq!(report.renewed_partitions, vec![7]);
  assert!(report.fenced_partitions.is_empty());
}

#[tokio::test]
async fn stable_rebalance_preserves_owned_partitions_and_committed_cursor() {
  let store: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let mut coordinator = ConsumerGroupCoordinatorImpl::new(
    ConsumerGroupConfig {
      topic: "topic-a".to_string().into(),
      group_id: "group-a".to_string().into(),
      member_id: "member-a".to_string().into(),
      lease_duration_ms: Some(100),
      heartbeat_interval_ms: Some(50),
      rebalance_interval_ms: Some(50),
      ..Default::default()
    },
    Arc::clone(&store),
  )
  .unwrap();

  coordinator
    .rebalance(vec!["member-a".to_string()], vec![7], 1_000)
    .await
    .unwrap();
  coordinator
    .heartbeat_and_commit(1_010, &HashMap::from([(7_u32, 10_u64)]))
    .await
    .unwrap();

  let report = coordinator
    .rebalance(vec!["member-a".to_string()], vec![7], 1_020)
    .await
    .unwrap();

  assert_eq!(report.owned_partitions, vec![7]);
  assert_eq!(report.committed_cursors, HashMap::from([(7_u32, 10_u64)]));
}

#[tokio::test]
async fn heartbeat_detects_fencing_by_new_generation() {
  let concrete_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let store: Arc<dyn ConsumerGroupLeaseStore> = concrete_store.clone();
  let mut coordinator = ConsumerGroupCoordinatorImpl::new(
    ConsumerGroupConfig {
      topic: "topic-a".to_string().into(),
      group_id: "group-a".to_string().into(),
      member_id: "member-a".to_string().into(),
      lease_duration_ms: Some(100),
      heartbeat_interval_ms: Some(50),
      rebalance_interval_ms: Some(50),
      ..Default::default()
    },
    Arc::clone(&store),
  )
  .unwrap();

  coordinator
    .rebalance(
      vec!["member-a".to_string(), "member-b".to_string()],
      vec![7],
      1_000,
    )
    .await
    .unwrap();
  assert_eq!(coordinator.owned_partitions(), vec![7]);

  let key = ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 7,
  };

  concrete_store
    .assign_partition(key, "member-b".to_string(), 2, 1_120, 100)
    .await
    .unwrap();

  let report = coordinator
    .heartbeat_and_commit(1_130, &HashMap::from([(7_u32, 12_u64)]))
    .await
    .unwrap();

  assert!(report.renewed_partitions.is_empty());
  assert_eq!(report.fenced_partitions, vec![7]);
  assert!(coordinator.owned_partitions().is_empty());
}

#[tokio::test]
async fn release_owned_releases_partitions_for_fast_takeover() {
  let concrete_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let store: Arc<dyn ConsumerGroupLeaseStore> = concrete_store.clone();
  let mut coordinator = ConsumerGroupCoordinatorImpl::new(
    ConsumerGroupConfig {
      topic: "topic-a".to_string().into(),
      group_id: "group-a".to_string().into(),
      member_id: "member-a".to_string().into(),
      lease_duration_ms: Some(1_000),
      heartbeat_interval_ms: Some(50),
      rebalance_interval_ms: Some(50),
      ..Default::default()
    },
    Arc::clone(&store),
  )
  .unwrap();

  coordinator
    .rebalance(vec!["member-a".to_string()], vec![7], 1_000)
    .await
    .unwrap();
  assert_eq!(coordinator.owned_partitions(), vec![7]);

  let released = coordinator.release_owned(1_010).await.unwrap();
  assert_eq!(released, vec![7]);
  assert!(coordinator.owned_partitions().is_empty());

  let key = ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 7,
  };
  let reassigned = concrete_store
    .assign_partition(key, "member-b".to_string(), 2, 1_010, 1_000)
    .await
    .unwrap();
  assert!(matches!(
    reassigned,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));
}
