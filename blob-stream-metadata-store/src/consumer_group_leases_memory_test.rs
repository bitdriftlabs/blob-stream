use crate::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupReleaseOutcome,
  InMemoryConsumerGroupLeaseStore,
};
use blob_stream_types::CommittedCursor;

fn lease_key() -> ConsumerGroupLeaseKey {
  ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 7,
  }
}

fn cursor(virtual_partition_id: u32, seq_end: u64) -> CommittedCursor {
  CommittedCursor {
    virtual_partition_id,
    seq_end,
    source_checkpoint: None,
  }
}

fn lease_key_for(topic: &str, group_id: &str, virtual_partition_id: u32) -> ConsumerGroupLeaseKey {
  ConsumerGroupLeaseKey {
    topic: topic.to_string(),
    group_id: group_id.to_string(),
    virtual_partition_id,
  }
}

#[tokio::test]
async fn list_group_leases_returns_retained_rows_in_partition_order() {
  let store = InMemoryConsumerGroupLeaseStore::new();
  let key_two = lease_key_for("topic-a", "group-a", 2);
  let key_ten = lease_key_for("topic-a", "group-a", 10);
  let other_group_key = lease_key_for("topic-a", "group-b", 4);

  store
    .assign_partition(key_ten.clone(), "member-b".to_string(), 3, 1_000, 100)
    .await
    .unwrap();
  store
    .heartbeat_partition(&key_ten, "member-b", 3, 1_010, 100, Some(cursor(10, 42)))
    .await
    .unwrap();
  store
    .assign_partition(key_two.clone(), "member-a".to_string(), 2, 1_000, 100)
    .await
    .unwrap();
  store
    .release_partition(&key_two, "member-a", 2, 1_020)
    .await
    .unwrap();
  store
    .assign_partition(other_group_key, "member-c".to_string(), 1, 1_000, 100)
    .await
    .unwrap();

  let leases = store.list_group_leases("topic-a", "group-a").await.unwrap();
  assert_eq!(
    leases
      .iter()
      .map(|lease| lease.key.virtual_partition_id)
      .collect::<Vec<_>>(),
    vec![2, 10]
  );
  assert_eq!(leases[0].owner_id, "member-a");
  assert_eq!(leases[0].lease_expiration_ts_ms, 1_020);
  assert_eq!(leases[1].owner_id, "member-b");
  assert_eq!(leases[1].committed_cursor, Some(cursor(10, 42)));
  assert_eq!(leases[1].committed_ts_ms, Some(1_010));
}

#[tokio::test]
async fn fences_assignment() {
  let store = InMemoryConsumerGroupLeaseStore::new();
  let key = lease_key();

  let outcome = store
    .assign_partition(key.clone(), "member-a".to_string(), 1, 1000, 100)
    .await
    .expect("assign lease");

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));

  let outcome = store
    .assign_partition(key.clone(), "member-b".to_string(), 1, 1000, 100)
    .await
    .expect("assign lease");

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::HeldByOther(_)
  ));

  let outcome = store
    .assign_partition(key.clone(), "member-b".to_string(), 2, 1100, 100)
    .await
    .expect("assign lease after expiration");

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));
}

#[tokio::test]
async fn heartbeats_and_commits() {
  let store = InMemoryConsumerGroupLeaseStore::new();
  let key = lease_key();

  store
    .assign_partition(key.clone(), "member-a".to_string(), 1, 1000, 100)
    .await
    .expect("assign lease");

  let outcome = store
    .heartbeat_partition(
      &key,
      "member-a",
      1,
      1010,
      100,
      Some(cursor(key.virtual_partition_id, 10)),
    )
    .await
    .expect("heartbeat lease");

  let ConsumerGroupHeartbeatOutcome::Renewed(lease) = outcome else {
    panic!("expected renewed heartbeat");
  };

  assert_eq!(
    lease.committed_cursor,
    Some(cursor(key.virtual_partition_id, 10))
  );

  let outcome = store
    .commit_cursor(
      &key,
      "member-a",
      1,
      1020,
      cursor(key.virtual_partition_id, 12),
    )
    .await
    .expect("commit cursor");

  let ConsumerGroupCommitOutcome::Committed(lease) = outcome else {
    panic!("expected committed cursor");
  };

  assert_eq!(
    lease.committed_cursor,
    Some(cursor(key.virtual_partition_id, 12))
  );
}

#[tokio::test]
async fn heartbeat_fences_other_members() {
  let store = InMemoryConsumerGroupLeaseStore::new();
  let key = lease_key();

  store
    .assign_partition(key.clone(), "member-a".to_string(), 1, 1000, 100)
    .await
    .expect("assign lease");

  let outcome = store
    .heartbeat_partition(&key, "member-b", 1, 1010, 100, None)
    .await
    .expect("heartbeat lease");

  assert!(matches!(
    outcome,
    ConsumerGroupHeartbeatOutcome::HeldByOther(_)
  ));
}

#[tokio::test]
async fn release_partition_allows_immediate_takeover() {
  let store = InMemoryConsumerGroupLeaseStore::new();

  let key = ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 1,
  };

  store
    .assign_partition(key.clone(), "member-a".to_string(), 2, 1000, 100)
    .await
    .expect("assign lease");

  let release = store
    .release_partition(&key, "member-a", 2, 1010)
    .await
    .expect("release lease");
  assert_eq!(release, ConsumerGroupReleaseOutcome::Released);

  let reassigned = store
    .assign_partition(key, "member-b".to_string(), 3, 1010, 100)
    .await
    .expect("assign lease after release");
  assert!(matches!(
    reassigned,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));
}

#[tokio::test]
async fn release_partition_rejects_stale_owner_or_generation() {
  let store = InMemoryConsumerGroupLeaseStore::new();

  let key = ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 1,
  };

  store
    .assign_partition(key.clone(), "member-a".to_string(), 2, 1000, 100)
    .await
    .expect("assign lease");

  let release = store
    .release_partition(&key, "member-b", 2, 1010)
    .await
    .expect("release lease");
  assert!(matches!(
    release,
    ConsumerGroupReleaseOutcome::HeldByOther(_)
  ));
}
