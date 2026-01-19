// blob-stream - in-memory consumer group leases tests
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
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
  }
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
