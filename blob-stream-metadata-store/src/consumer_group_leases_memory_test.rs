use crate::{
  ConsumerGroupArmFreshStartOutcome,
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupLeaseTransition,
  ConsumerGroupReleaseOutcome,
  InMemoryConsumerGroupLeaseStore,
};
use blob_stream_types::{
  CommittedCursor,
  CommittedSourceCheckpoint,
  offset_datetime_from_unix_millis,
};
use time::Duration;

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
    .assign_partition(
      key_ten.clone(),
      "member-b".to_string(),
      3,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .unwrap();
  store
    .heartbeat_partition(
      &key_ten,
      "member-b",
      3,
      offset_datetime_from_unix_millis(1_010),
      Duration::milliseconds(100),
      Some(cursor(10, 42)),
    )
    .await
    .unwrap();
  store
    .assign_partition(
      key_two.clone(),
      "member-a".to_string(),
      2,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .unwrap();
  store
    .release_partition(
      &key_two,
      "member-a",
      2,
      offset_datetime_from_unix_millis(1_020),
    )
    .await
    .unwrap();
  store
    .assign_partition(
      other_group_key,
      "member-c".to_string(),
      1,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
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
    .assign_partition(
      key.clone(),
      "member-a".to_string(),
      1,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease");

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::Assigned {
      transition: ConsumerGroupLeaseTransition::Initial,
      ..
    }
  ));

  let outcome = store
    .assign_partition(
      key.clone(),
      "member-b".to_string(),
      1,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease");

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::HeldByOther(_)
  ));

  let outcome = store
    .assign_partition(
      key.clone(),
      "member-b".to_string(),
      2,
      offset_datetime_from_unix_millis(1_100),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease after expiration");

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::Assigned {
      transition: ConsumerGroupLeaseTransition::ExpiryTakeover {
        previous_owner_id,
        previous_generation: 1,
        ..
      },
      ..
    } if previous_owner_id == "member-a"
  ));
}

#[tokio::test]
async fn heartbeats_and_commits() {
  let store = InMemoryConsumerGroupLeaseStore::new();
  let key = lease_key();

  store
    .assign_partition(
      key.clone(),
      "member-a".to_string(),
      1,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease");

  let outcome = store
    .heartbeat_partition(
      &key,
      "member-a",
      1,
      offset_datetime_from_unix_millis(1_010),
      Duration::milliseconds(100),
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
      offset_datetime_from_unix_millis(1_020),
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
async fn fresh_start_marker_survives_normal_commit_and_requires_matching_consumption() {
  let store = InMemoryConsumerGroupLeaseStore::new();
  let key = lease_key();
  let committed_cursor = CommittedCursor {
    virtual_partition_id: key.virtual_partition_id,
    seq_end: 10,
    source_checkpoint: Some(CommittedSourceCheckpoint {
      window_start_unix_seconds: 1_200,
      snowflake_id: 42,
    }),
  };

  store
    .assign_partition(
      key.clone(),
      "member-a".to_string(),
      1,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease");
  store
    .commit_cursor(
      &key,
      "member-a",
      1,
      offset_datetime_from_unix_millis(1_010),
      committed_cursor,
    )
    .await
    .expect("commit source checkpoint");

  let outcome = store
    .arm_next_window_fresh_start(
      &key,
      Duration::seconds(300),
      "marker-a".to_string(),
      offset_datetime_from_unix_millis(1_020),
    )
    .await
    .expect("arm fresh start");
  let ConsumerGroupArmFreshStartOutcome::Armed(lease) = outcome else {
    panic!("expected armed marker");
  };
  assert!(matches!(
    lease.fresh_start_marker,
    Some(ref marker) if marker.marker_id == "marker-a"
      && marker.target_window_start_unix_seconds == 1_500
  ));

  let normal_commit = store
    .commit_cursor(
      &key,
      "member-a",
      1,
      offset_datetime_from_unix_millis(1_030),
      cursor(key.virtual_partition_id, 11),
    )
    .await
    .expect("normal owner commit");
  let ConsumerGroupCommitOutcome::Committed(lease) = normal_commit else {
    panic!("expected committed cursor");
  };
  assert!(lease.fresh_start_marker.is_some());

  let reset_commit = store
    .commit_cursor_consuming_fresh_start_marker(
      &key,
      "member-a",
      1,
      offset_datetime_from_unix_millis(1_040),
      cursor(key.virtual_partition_id, 1),
      Some("marker-a".to_string()),
    )
    .await
    .expect("consume fresh-start marker");
  let ConsumerGroupCommitOutcome::Committed(lease) = reset_commit else {
    panic!("expected committed cursor");
  };
  assert_eq!(
    lease.committed_cursor,
    Some(cursor(key.virtual_partition_id, 1))
  );
  assert!(lease.fresh_start_marker.is_none());
}

#[tokio::test]
async fn retained_assignment_preserves_committed_cursor() {
  let store = InMemoryConsumerGroupLeaseStore::new();
  let key = lease_key();
  let committed_cursor = cursor(key.virtual_partition_id, 10);

  store
    .assign_partition(
      key.clone(),
      "member-a".to_string(),
      1,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease");
  store
    .heartbeat_partition(
      &key,
      "member-a",
      1,
      offset_datetime_from_unix_millis(1_010),
      Duration::milliseconds(100),
      Some(committed_cursor.clone()),
    )
    .await
    .expect("commit cursor");

  let outcome = store
    .assign_partition(
      key,
      "member-a".to_string(),
      2,
      offset_datetime_from_unix_millis(1_020),
      Duration::milliseconds(100),
    )
    .await
    .expect("retain lease");

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::Assigned {
      lease,
      previous_lease: Some(previous_lease),
      transition: ConsumerGroupLeaseTransition::Retained,
    } if lease.committed_cursor == Some(committed_cursor.clone())
      && previous_lease.committed_cursor == Some(committed_cursor)
  ));
}

#[tokio::test]
async fn heartbeat_fences_other_members() {
  let store = InMemoryConsumerGroupLeaseStore::new();
  let key = lease_key();

  store
    .assign_partition(
      key.clone(),
      "member-a".to_string(),
      1,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease");

  let outcome = store
    .heartbeat_partition(
      &key,
      "member-b",
      1,
      offset_datetime_from_unix_millis(1_010),
      Duration::milliseconds(100),
      None,
    )
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
    .assign_partition(
      key.clone(),
      "member-a".to_string(),
      2,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease");

  let release = store
    .release_partition(&key, "member-a", 2, offset_datetime_from_unix_millis(1_010))
    .await
    .expect("release lease");
  assert_eq!(release, ConsumerGroupReleaseOutcome::Released);

  let reassigned = store
    .assign_partition(
      key,
      "member-b".to_string(),
      3,
      offset_datetime_from_unix_millis(1_010),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease after release");
  assert!(matches!(
    reassigned,
    ConsumerGroupAssignmentOutcome::Assigned {
      transition: ConsumerGroupLeaseTransition::GracefulHandoff {
        previous_owner_id,
        previous_generation: 2,
        graceful_release_ts_ms: 1_010,
        ..
      },
      ..
    } if previous_owner_id == "member-a"
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
    .assign_partition(
      key.clone(),
      "member-a".to_string(),
      2,
      offset_datetime_from_unix_millis(1_000),
      Duration::milliseconds(100),
    )
    .await
    .expect("assign lease");

  let release = store
    .release_partition(&key, "member-b", 2, offset_datetime_from_unix_millis(1_010))
    .await
    .expect("release lease");
  assert!(matches!(
    release,
    ConsumerGroupReleaseOutcome::HeldByOther(_)
  ));
}
