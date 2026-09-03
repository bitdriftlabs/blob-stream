#![allow(clippy::unwrap_used)]

use super::{ConsumerArmFreshStartResultOutcome, ConsumerDiagnostics};
use crate::config::ConsumerGroupConfig;
use crate::iterator::ConsumerSharedState;
use async_trait::async_trait;
use blob_stream_metadata_store::{
  ConsumerGroupArmFreshStartOutcome,
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupReleaseOutcome,
  InMemoryConsumerGroupLeaseStore,
};
use blob_stream_types::{CommittedCursor, CommittedSourceCheckpoint};
use parking_lot::Mutex;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

struct PartialArmFailureLeaseStore {
  inner: InMemoryConsumerGroupLeaseStore,
  failing_partition: u32,
}

#[async_trait]
impl ConsumerGroupLeaseStore for PartialArmFailureLeaseStore {
  async fn list_group_leases(
    &self,
    topic: &str,
    group_id: &str,
  ) -> anyhow::Result<Vec<ConsumerGroupLease>> {
    self.inner.list_group_leases(topic, group_id).await
  }

  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: Duration,
  ) -> anyhow::Result<ConsumerGroupAssignmentOutcome> {
    self
      .inner
      .assign_partition(key, owner_id, generation, now, lease_duration)
      .await
  }

  async fn arm_next_window_fresh_start(
    &self,
    key: &ConsumerGroupLeaseKey,
    metadata_window_size: Duration,
    marker_id: String,
    now: OffsetDateTime,
  ) -> anyhow::Result<ConsumerGroupArmFreshStartOutcome> {
    if key.virtual_partition_id == self.failing_partition {
      return Err(anyhow::anyhow!("injected fresh-start arming failure"));
    }
    self
      .inner
      .arm_next_window_fresh_start(key, metadata_window_size, marker_id, now)
      .await
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: Duration,
    committed_cursor: Option<CommittedCursor>,
  ) -> anyhow::Result<ConsumerGroupHeartbeatOutcome> {
    self
      .inner
      .heartbeat_partition(
        key,
        owner_id,
        generation,
        now,
        lease_duration,
        committed_cursor,
      )
      .await
  }

  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    committed_cursor: CommittedCursor,
  ) -> anyhow::Result<ConsumerGroupCommitOutcome> {
    self
      .inner
      .commit_cursor(key, owner_id, generation, now, committed_cursor)
      .await
  }

  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
  ) -> anyhow::Result<ConsumerGroupReleaseOutcome> {
    self
      .inner
      .release_partition(key, owner_id, generation, now)
      .await
  }
}

fn key(virtual_partition_id: u32) -> ConsumerGroupLeaseKey {
  ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id,
  }
}

#[tokio::test]
async fn fresh_start_arm_reports_each_partition_when_one_arm_fails() {
  let store = Arc::new(PartialArmFailureLeaseStore {
    inner: InMemoryConsumerGroupLeaseStore::new(),
    failing_partition: 1,
  });
  let now = OffsetDateTime::now_utc();
  for partition_id in [0, 1] {
    let key = key(partition_id);
    store
      .assign_partition(
        key.clone(),
        "member-a".to_string(),
        1,
        now,
        Duration::minutes(1),
      )
      .await
      .unwrap();
    store
      .commit_cursor(
        &key,
        "member-a",
        1,
        now,
        CommittedCursor {
          virtual_partition_id: partition_id,
          seq_end: 10,
          source_checkpoint: Some(CommittedSourceCheckpoint {
            window_start_unix_seconds: 1_200,
            snowflake_id: 42,
          }),
        },
      )
      .await
      .unwrap();
  }
  let diagnostics = ConsumerDiagnostics::new(
    ConsumerGroupConfig {
      topic: "topic-a".into(),
      group_id: "group-a".into(),
      member_id: "member-a".into(),
      ..Default::default()
    },
    Arc::new(Mutex::new(ConsumerSharedState::default())),
    0,
    store.clone(),
    Duration::seconds(300),
  );

  let results = diagnostics
    .arm_next_window_fresh_start(vec![0, 1], false)
    .await
    .unwrap();

  assert_eq!(results.len(), 2);
  assert!(matches!(
    results[0].outcome,
    ConsumerArmFreshStartResultOutcome::Armed
  ));
  assert!(results[0].error.is_none());
  assert!(matches!(
    results[1].outcome,
    ConsumerArmFreshStartResultOutcome::Failed
  ));
  assert!(
    results[1]
      .error
      .as_deref()
      .is_some_and(|error| error.contains("injected fresh-start arming failure"))
  );
  let leases = store.list_group_leases("topic-a", "group-a").await.unwrap();
  assert!(leases[0].fresh_start_marker.is_some());
  assert!(leases[1].fresh_start_marker.is_none());
}
