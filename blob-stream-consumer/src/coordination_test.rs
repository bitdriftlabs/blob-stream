#![allow(clippy::unwrap_used)]

use crate::config::ConsumerGroupConfig;
use crate::coordination::{
  AssignmentPlanValidationError,
  ConsumerGroupCoordinator,
  ConsumerGroupCoordinatorImpl,
  RecoveredCursor,
  assignment_plan_validation_error,
  cooperative_sticky_assignment,
};
use blob_stream_metadata_store::{
  ConsumerGroupAssignment,
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  ConsumerGroupReleaseOutcome,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
};
use blob_stream_types::CommittedCursor;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

fn membership_store() -> Arc<dyn ConsumerGroupMembershipStore> {
  Arc::new(InMemoryConsumerGroupMembershipStore::new())
}

fn committed_cursor(virtual_partition_id: u32, seq_end: u64) -> CommittedCursor {
  CommittedCursor {
    virtual_partition_id,
    seq_end,
    source_checkpoint: None,
  }
}

struct PartialHeartbeatFailureLeaseStore {
  inner: InMemoryConsumerGroupLeaseStore,
  failing_partition: u32,
}

struct BlockingAssignmentLeaseStore {
  inner: InMemoryConsumerGroupLeaseStore,
  assignment_started: AtomicUsize,
  assignment_started_notify: Notify,
  assignment_released: AtomicBool,
  assignment_release_notify: Notify,
}

#[async_trait::async_trait]
impl ConsumerGroupLeaseStore for PartialHeartbeatFailureLeaseStore {
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
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> anyhow::Result<ConsumerGroupAssignmentOutcome> {
    self
      .inner
      .assign_partition(key, owner_id, generation, now_ts_ms, lease_duration_ms)
      .await
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
    committed_cursor: Option<CommittedCursor>,
  ) -> anyhow::Result<ConsumerGroupHeartbeatOutcome> {
    if key.virtual_partition_id == self.failing_partition {
      return Err(anyhow::anyhow!("injected heartbeat failure"));
    }
    self
      .inner
      .heartbeat_partition(
        key,
        owner_id,
        generation,
        now_ts_ms,
        lease_duration_ms,
        committed_cursor,
      )
      .await
  }

  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    committed_cursor: CommittedCursor,
  ) -> anyhow::Result<ConsumerGroupCommitOutcome> {
    self
      .inner
      .commit_cursor(key, owner_id, generation, now_ts_ms, committed_cursor)
      .await
  }

  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
  ) -> anyhow::Result<ConsumerGroupReleaseOutcome> {
    self
      .inner
      .release_partition(key, owner_id, generation, now_ts_ms)
      .await
  }
}

#[async_trait::async_trait]
impl ConsumerGroupLeaseStore for BlockingAssignmentLeaseStore {
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
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> anyhow::Result<ConsumerGroupAssignmentOutcome> {
    self.assignment_started.fetch_add(1, Ordering::SeqCst);
    self.assignment_started_notify.notify_waiters();
    while !self.assignment_released.load(Ordering::SeqCst) {
      let release_notified = self.assignment_release_notify.notified();
      if !self.assignment_released.load(Ordering::SeqCst) {
        release_notified.await;
      }
    }
    self
      .inner
      .assign_partition(key, owner_id, generation, now_ts_ms, lease_duration_ms)
      .await
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
    committed_cursor: Option<CommittedCursor>,
  ) -> anyhow::Result<ConsumerGroupHeartbeatOutcome> {
    self
      .inner
      .heartbeat_partition(
        key,
        owner_id,
        generation,
        now_ts_ms,
        lease_duration_ms,
        committed_cursor,
      )
      .await
  }

  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    committed_cursor: CommittedCursor,
  ) -> anyhow::Result<ConsumerGroupCommitOutcome> {
    self
      .inner
      .commit_cursor(key, owner_id, generation, now_ts_ms, committed_cursor)
      .await
  }

  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
  ) -> anyhow::Result<ConsumerGroupReleaseOutcome> {
    self
      .inner
      .release_partition(key, owner_id, generation, now_ts_ms)
      .await
  }
}

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

#[test]
fn sticky_assignment_repairs_zero_owner_when_other_members_are_at_ceiling() {
  let members = vec![
    "member-a".to_string(),
    "member-b".to_string(),
    "member-c".to_string(),
    "member-d".to_string(),
  ];
  let partitions = vec![0, 1, 2, 3, 4, 5];
  let previous = HashMap::from([
    (0, "member-a".to_string()),
    (1, "member-a".to_string()),
    (2, "member-b".to_string()),
    (3, "member-b".to_string()),
    (4, "member-c".to_string()),
    (5, "member-c".to_string()),
  ]);

  let assignment = cooperative_sticky_assignment(&members, &partitions, &previous, "member-a");
  let loads = members
    .iter()
    .map(|member| assignment.values().filter(|owner| *owner == member).count())
    .collect::<Vec<_>>();
  let moved = partitions
    .iter()
    .filter(|partition_id| assignment.get(partition_id) != previous.get(partition_id))
    .count();

  assert_eq!(loads.iter().min(), Some(&1));
  assert_eq!(loads.iter().max(), Some(&2));
  assert_eq!(loads[3], 1);
  assert_eq!(moved, 1);
}

#[test]
fn assignment_plan_validation_reports_imbalanced_load() {
  let members = vec![
    "member-a".to_string(),
    "member-b".to_string(),
    "member-c".to_string(),
    "member-d".to_string(),
  ];
  let plan = ConsumerGroupAssignmentPlan {
    version: 73,
    planner_member_id: "member-a".to_string(),
    members,
    assignments: (0 .. 6)
      .map(|virtual_partition_id| ConsumerGroupAssignment {
        virtual_partition_id,
        member_id: match virtual_partition_id {
          0 | 1 => "member-a",
          2 | 3 => "member-b",
          _ => "member-c",
        }
        .to_string(),
      })
      .collect(),
    published_ts_ms: 1_000,
  };

  assert_eq!(
    assignment_plan_validation_error(&plan, &[0, 1, 2, 3, 4, 5]),
    Some(AssignmentPlanValidationError::ImbalancedLoad {
      min_load: 0,
      max_load: 2,
    })
  );
}

#[tokio::test]
async fn oversubscribed_consumers_keep_stable_assignments_without_fencing() {
  let store: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store = membership_store();
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
        Arc::clone(&membership_store),
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
async fn shared_plan_covers_every_partition_despite_divergent_member_snapshots() {
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let members = (0 .. 4)
    .map(|member_index| format!("member-{member_index}"))
    .collect::<Vec<_>>();
  let partitions = (0 .. 32).collect::<Vec<_>>();
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
        Arc::clone(&lease_store),
        Arc::clone(&membership_store),
      )
      .unwrap()
    })
    .collect::<Vec<_>>();

  // Member 0 publishes the initial complete plan. The remaining coordinators deliberately see
  // different incomplete snapshots, matching the stale local-view failure that left partitions
  // uncovered before plans were shared.
  let mut owned = coordinators[0]
    .rebalance(members.clone(), partitions.clone(), 1_000)
    .await
    .unwrap()
    .owned_partitions;
  for (coordinator, snapshot) in coordinators.iter_mut().skip(1).zip([
    vec!["member-1".to_string(), "member-2".to_string()],
    vec!["member-0".to_string(), "member-2".to_string()],
    vec!["member-3".to_string()],
  ]) {
    owned.extend(
      coordinator
        .rebalance(snapshot, partitions.clone(), 1_010)
        .await
        .unwrap()
        .owned_partitions,
    );
  }

  owned.sort_unstable();
  owned.dedup();
  assert_eq!(owned, partitions);
  assert!(
    coordinators
      .iter()
      .all(|coordinator| coordinator.generation() == 1)
  );
}

#[tokio::test]
async fn coordinator_does_not_create_local_assignment_without_shared_plan() {
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store = membership_store();
  assert_eq!(
    membership_store
      .acquire_or_renew_planner(
        "topic-a",
        "group-a",
        "member-b",
        "member-b-session",
        1_000,
        1_000,
      )
      .await
      .unwrap(),
    blob_stream_metadata_store::ConsumerGroupPlannerLeaseOutcome::Acquired
  );
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
    lease_store,
    membership_store,
  )
  .unwrap();

  let report = coordinator
    .rebalance(vec!["member-a".to_string()], vec![0, 1], 1_001)
    .await
    .unwrap();

  assert!(report.owned_partitions.is_empty());
  assert_eq!(coordinator.generation(), 0);
}

#[tokio::test]
async fn heartbeat_commit_renews_and_commits_cursor() {
  let store: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store = membership_store();
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
    membership_store,
  )
  .unwrap();

  let owned = coordinator
    .rebalance(vec!["member-a".to_string()], vec![7], 1_000)
    .await
    .unwrap();
  assert_eq!(owned.owned_partitions, vec![7]);
  assert!(owned.recovered_cursors.is_empty());

  let report = coordinator
    .heartbeat_and_commit(1_010, &HashMap::from([(7_u32, committed_cursor(7, 10))]))
    .await
    .unwrap();
  assert_eq!(report.renewed_partitions, vec![7]);
  assert!(report.fenced_partitions.is_empty());
}

#[tokio::test]
async fn rebalance_acquires_partition_leases_concurrently() {
  let concrete_store = Arc::new(BlockingAssignmentLeaseStore {
    inner: InMemoryConsumerGroupLeaseStore::new(),
    assignment_started: AtomicUsize::new(0),
    assignment_started_notify: Notify::new(),
    assignment_released: AtomicBool::new(false),
    assignment_release_notify: Notify::new(),
  });
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
    store,
    membership_store(),
  )
  .unwrap();

  let rebalance = tokio::spawn(async move {
    coordinator
      .rebalance(vec!["member-a".to_string()], vec![0, 1, 2], 1_000)
      .await
  });

  timeout(Duration::from_secs(1), async {
    while concrete_store.assignment_started.load(Ordering::SeqCst) < 2 {
      concrete_store.assignment_started_notify.notified().await;
    }
  })
  .await
  .expect("rebalance should start multiple assignments before one completes");
  concrete_store
    .assignment_released
    .store(true, Ordering::SeqCst);
  concrete_store.assignment_release_notify.notify_waiters();

  let report = rebalance.await.unwrap().unwrap();
  assert_eq!(report.owned_partitions, vec![0, 1, 2]);
}

#[tokio::test]
async fn stable_rebalance_preserves_owned_partitions_and_committed_cursor() {
  let store: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store = membership_store();
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
    membership_store,
  )
  .unwrap();

  coordinator
    .rebalance(vec!["member-a".to_string()], vec![7], 1_000)
    .await
    .unwrap();
  coordinator
    .heartbeat_and_commit(1_010, &HashMap::from([(7_u32, committed_cursor(7, 10))]))
    .await
    .unwrap();

  let report = coordinator
    .rebalance(vec!["member-a".to_string()], vec![7], 1_020)
    .await
    .unwrap();

  assert_eq!(report.owned_partitions, vec![7]);
  assert_eq!(
    report.recovered_cursors,
    HashMap::from([(
      7_u32,
      RecoveredCursor {
        committed_cursor: committed_cursor(7, 10),
        committed_ts_ms: Some(1_010),
      },
    )])
  );
}

#[tokio::test]
async fn heartbeat_detects_fencing_by_new_generation() {
  let concrete_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let store: Arc<dyn ConsumerGroupLeaseStore> = concrete_store.clone();
  let membership_store = membership_store();
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
    membership_store,
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
    .heartbeat_and_commit(1_130, &HashMap::from([(7_u32, committed_cursor(7, 12))]))
    .await
    .unwrap();

  assert!(report.renewed_partitions.is_empty());
  assert_eq!(report.fenced_partitions, vec![7]);
  assert!(coordinator.owned_partitions().is_empty());
}

#[tokio::test]
async fn heartbeat_reconciles_fencing_when_another_partition_errors() {
  let concrete_store = Arc::new(PartialHeartbeatFailureLeaseStore {
    inner: InMemoryConsumerGroupLeaseStore::new(),
    failing_partition: 8,
  });
  let store: Arc<dyn ConsumerGroupLeaseStore> = concrete_store.clone();
  let membership_store = membership_store();
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
    store,
    membership_store,
  )
  .unwrap();

  coordinator
    .rebalance(vec!["member-a".to_string()], vec![7, 8], 1_000)
    .await
    .unwrap();
  assert_eq!(coordinator.owned_partitions(), vec![7, 8]);

  concrete_store
    .inner
    .assign_partition(
      ConsumerGroupLeaseKey {
        topic: "topic-a".to_string(),
        group_id: "group-a".to_string(),
        virtual_partition_id: 7,
      },
      "member-b".to_string(),
      2,
      1_120,
      100,
    )
    .await
    .unwrap();

  assert!(
    coordinator
      .heartbeat_and_commit(1_130, &HashMap::new())
      .await
      .is_err()
  );
  assert_eq!(coordinator.owned_partitions(), vec![8]);
}

#[tokio::test]
async fn release_owned_releases_partitions_for_fast_takeover() {
  let concrete_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let store: Arc<dyn ConsumerGroupLeaseStore> = concrete_store.clone();
  let membership_store = membership_store();
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
    membership_store,
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
