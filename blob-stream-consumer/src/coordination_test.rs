#![allow(clippy::unwrap_used)]

use crate::config::ConsumerGroupConfig;
use crate::coordination::{
  AssignmentPlanValidationError,
  ConsumerGroupCoordinator,
  ConsumerGroupCoordinatorImpl,
  LeaseClaimCounts,
  assignment_plan_validation_error,
  cooperative_sticky_assignment,
  cooperative_sticky_assignment_with_pods,
};
use crate::diagnostics::{
  ConsumerAssignmentPolicy,
  ConsumerMemberTopologySnapshot,
  ConsumerPodLoadSnapshot,
  assignment_plan_snapshot,
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
  ConsumerGroupMember,
  ConsumerGroupMembershipStore,
  ConsumerGroupReleaseOutcome,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
};
use blob_stream_types::{CommittedCursor, ToProtoDuration, offset_datetime_from_unix_millis};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use time::{Duration as TimeDuration, OffsetDateTime};
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
  failing_assignment_partition: Option<u32>,
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

  async fn list_active_leases(
    &self,
    topics: &[String],
    now: OffsetDateTime,
  ) -> anyhow::Result<Vec<ConsumerGroupLease>> {
    self.inner.list_active_leases(topics, now).await
  }

  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
  ) -> anyhow::Result<ConsumerGroupAssignmentOutcome> {
    if self.failing_assignment_partition == Some(key.virtual_partition_id) {
      return Err(anyhow::anyhow!("injected assignment failure"));
    }
    self
      .inner
      .assign_partition(key, owner_id, generation, now, lease_duration)
      .await
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
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

#[async_trait::async_trait]
impl ConsumerGroupLeaseStore for BlockingAssignmentLeaseStore {
  async fn list_group_leases(
    &self,
    topic: &str,
    group_id: &str,
  ) -> anyhow::Result<Vec<ConsumerGroupLease>> {
    self.inner.list_group_leases(topic, group_id).await
  }

  async fn list_active_leases(
    &self,
    topics: &[String],
    now: OffsetDateTime,
  ) -> anyhow::Result<Vec<ConsumerGroupLease>> {
    self.inner.list_active_leases(topics, now).await
  }

  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
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
      .assign_partition(key, owner_id, generation, now, lease_duration)
      .await
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
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
fn pod_aware_assignment_balances_pods_with_minimal_movement() {
  let members = (0 .. 7)
    .flat_map(|pod_index| {
      (0 .. 2).map(move |worker_index| ConsumerGroupMember {
        member_id: format!("pod-{pod_index}:worker-{worker_index}"),
        pod_id: Some(format!("pod-{pod_index}")),
      })
    })
    .collect::<Vec<_>>();
  let partitions = (0 .. 32).collect::<Vec<_>>();
  let mut previous = HashMap::new();
  for partition_id in 0 .. 32 {
    let pod_index = match partition_id {
      0 .. 6 => 0,
      6 .. 12 => 1,
      _ => 2 + ((partition_id - 12) / 4),
    };
    let worker_index = match partition_id {
      0 .. 12 => (partition_id % 6) / 3,
      _ => (partition_id % 4) / 2,
    };
    previous.insert(
      partition_id,
      format!("pod-{pod_index}:worker-{worker_index}"),
    );
  }

  let assignment = cooperative_sticky_assignment_with_pods(&members, &partitions, &previous);
  let pod_loads = (0 .. 7)
    .map(|pod_index| {
      assignment
        .values()
        .filter(|member_id| member_id.starts_with(&format!("pod-{pod_index}:")))
        .count()
    })
    .collect::<Vec<_>>();
  let member_loads = members
    .iter()
    .map(|member| {
      assignment
        .values()
        .filter(|member_id| *member_id == &member.member_id)
        .count()
    })
    .collect::<Vec<_>>();
  let moved = partitions
    .iter()
    .filter(|partition_id| assignment.get(partition_id) != previous.get(partition_id))
    .count();

  assert_eq!(pod_loads, vec![5, 5, 5, 5, 4, 4, 4]);
  assert!(member_loads.iter().all(|load| (2 ..= 3).contains(load)));
  assert_eq!(moved, 2);
}

#[test]
fn pod_aware_assignment_plan_snapshot_includes_member_and_pod_identity() {
  let plan = ConsumerGroupAssignmentPlan {
    version: 7,
    planner_member_id: "pod-a:worker-0".to_string(),
    members: vec!["pod-a:worker-0".to_string(), "pod-b:worker-0".to_string()],
    member_topology: Some(vec![
      ConsumerGroupMember {
        member_id: "pod-a:worker-0".to_string(),
        pod_id: Some("pod-a".to_string()),
      },
      ConsumerGroupMember {
        member_id: "pod-b:worker-0".to_string(),
        pod_id: Some("pod-b".to_string()),
      },
      ConsumerGroupMember {
        member_id: "pod-c:worker-0".to_string(),
        pod_id: Some("pod-c".to_string()),
      },
    ]),
    assignments: vec![
      ConsumerGroupAssignment {
        virtual_partition_id: 0,
        member_id: "pod-a:worker-0".to_string(),
      },
      ConsumerGroupAssignment {
        virtual_partition_id: 1,
        member_id: "pod-b:worker-0".to_string(),
      },
    ],
    published_ts_ms: 1_000,
  };

  let snapshot = assignment_plan_snapshot(plan);

  assert_eq!(snapshot.policy, ConsumerAssignmentPolicy::PodAware);
  assert_eq!(
    snapshot.member_topology,
    vec![
      ConsumerMemberTopologySnapshot {
        member_id: "pod-a:worker-0".to_string(),
        pod_id: "pod-a".to_string(),
      },
      ConsumerMemberTopologySnapshot {
        member_id: "pod-b:worker-0".to_string(),
        pod_id: "pod-b".to_string(),
      },
      ConsumerMemberTopologySnapshot {
        member_id: "pod-c:worker-0".to_string(),
        pod_id: "pod-c".to_string(),
      },
    ]
  );
  assert_eq!(
    snapshot.pod_loads,
    vec![
      ConsumerPodLoadSnapshot {
        pod_id: "pod-a".to_string(),
        partition_count: 1,
      },
      ConsumerPodLoadSnapshot {
        pod_id: "pod-b".to_string(),
        partition_count: 1,
      },
      ConsumerPodLoadSnapshot {
        pod_id: "pod-c".to_string(),
        partition_count: 0,
      },
    ]
  );
  assert_eq!(snapshot.assignments[0].pod_id.as_deref(), Some("pod-a"));
  assert_eq!(snapshot.assignments[1].pod_id.as_deref(), Some("pod-b"));
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
    member_topology: None,
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
          lease_duration: TimeDuration::milliseconds(1_000).into_proto(),
          heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
          rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
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
      .rebalance(
        members.clone(),
        partitions.clone(),
        offset_datetime_from_unix_millis(1_000),
      )
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
      .heartbeat_and_commit(offset_datetime_from_unix_millis(1_010), &HashMap::new())
      .await
      .unwrap();
    assert!(report.fenced_partitions.is_empty());
  }

  for (coordinator, ownership) in coordinators.iter_mut().zip(&expected_ownership) {
    let report = coordinator
      .rebalance(
        members.clone(),
        partitions.clone(),
        offset_datetime_from_unix_millis(1_020),
      )
      .await
      .unwrap();
    assert_eq!(report.owned_partitions, *ownership);
    assert_eq!(coordinator.generation(), 1);

    let heartbeat = coordinator
      .heartbeat_and_commit(offset_datetime_from_unix_millis(1_030), &HashMap::new())
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
          lease_duration: TimeDuration::milliseconds(1_000).into_proto(),
          heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
          rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
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
    .rebalance(
      members.clone(),
      partitions.clone(),
      offset_datetime_from_unix_millis(1_000),
    )
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
        .rebalance(
          snapshot,
          partitions.clone(),
          offset_datetime_from_unix_millis(1_010),
        )
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
        offset_datetime_from_unix_millis(1_000),
        TimeDuration::milliseconds(1_000),
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
      lease_duration: TimeDuration::milliseconds(1_000).into_proto(),
      heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
      rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
      ..Default::default()
    },
    lease_store,
    membership_store,
  )
  .unwrap();

  let report = coordinator
    .rebalance(
      vec!["member-a".to_string()],
      vec![0, 1],
      offset_datetime_from_unix_millis(1_001),
    )
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
      lease_duration: TimeDuration::milliseconds(100).into_proto(),
      heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
      rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
      ..Default::default()
    },
    Arc::clone(&store),
    membership_store,
  )
  .unwrap();

  let owned = coordinator
    .rebalance(
      vec!["member-a".to_string()],
      vec![7],
      offset_datetime_from_unix_millis(1_000),
    )
    .await
    .unwrap();
  assert_eq!(owned.owned_partitions, vec![7]);
  assert!(owned.recovered_cursors.is_empty());

  let report = coordinator
    .heartbeat_and_commit(
      offset_datetime_from_unix_millis(1_010),
      &HashMap::from([(7_u32, committed_cursor(7, 10))]),
    )
    .await
    .unwrap();
  assert_eq!(report.renewed_partitions, vec![7]);
  assert!(report.fenced_partitions.is_empty());
}

#[tokio::test]
async fn commit_cursors_does_not_renew_partition_leases() {
  let concrete_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let store: Arc<dyn ConsumerGroupLeaseStore> = concrete_store.clone();
  let mut coordinator = ConsumerGroupCoordinatorImpl::new(
    ConsumerGroupConfig {
      topic: "topic-a".to_string().into(),
      group_id: "group-a".to_string().into(),
      member_id: "member-a".to_string().into(),
      lease_duration: TimeDuration::milliseconds(100).into_proto(),
      heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
      rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
      ..Default::default()
    },
    store,
    membership_store(),
  )
  .unwrap();

  coordinator
    .rebalance(
      vec!["member-a".to_string()],
      vec![7, 8],
      offset_datetime_from_unix_millis(1_000),
    )
    .await
    .unwrap();

  let report = coordinator
    .commit_cursors(
      offset_datetime_from_unix_millis(1_010),
      &HashMap::from([(7_u32, committed_cursor(7, 10))]),
    )
    .await
    .unwrap();
  assert_eq!(report.renewed_partitions, vec![7]);
  assert!(report.fenced_partitions.is_empty());

  let leases = concrete_store
    .list_group_leases("topic-a", "group-a")
    .await
    .unwrap();
  let committed_lease = leases
    .iter()
    .find(|lease| lease.key.virtual_partition_id == 7)
    .unwrap();
  let untouched_lease = leases
    .iter()
    .find(|lease| lease.key.virtual_partition_id == 8)
    .unwrap();
  assert_eq!(
    committed_lease
      .committed_cursor
      .as_ref()
      .map(|cursor| cursor.seq_end),
    Some(10)
  );
  assert_eq!(committed_lease.lease_expiration_ts_ms, 1_100);
  assert_eq!(untouched_lease.lease_expiration_ts_ms, 1_100);
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
      lease_duration: TimeDuration::milliseconds(1_000).into_proto(),
      heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
      rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
      ..Default::default()
    },
    store,
    membership_store(),
  )
  .unwrap();

  let rebalance = tokio::spawn(async move {
    coordinator
      .rebalance(
        vec!["member-a".to_string()],
        vec![0, 1, 2],
        offset_datetime_from_unix_millis(1_000),
      )
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
      lease_duration: TimeDuration::milliseconds(100).into_proto(),
      heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
      rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
      ..Default::default()
    },
    Arc::clone(&store),
    membership_store,
  )
  .unwrap();

  coordinator
    .rebalance(
      vec!["member-a".to_string()],
      vec![7],
      offset_datetime_from_unix_millis(1_000),
    )
    .await
    .unwrap();
  coordinator
    .heartbeat_and_commit(
      offset_datetime_from_unix_millis(1_010),
      &HashMap::from([(7_u32, committed_cursor(7, 10))]),
    )
    .await
    .unwrap();

  let report = coordinator
    .rebalance(
      vec!["member-a".to_string()],
      vec![7],
      offset_datetime_from_unix_millis(1_020),
    )
    .await
    .unwrap();

  assert_eq!(report.owned_partitions, vec![7]);
  assert!(report.recovered_cursors.is_empty());
  assert_eq!(report.lease_claim_counts, LeaseClaimCounts::default());
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
      lease_duration: TimeDuration::milliseconds(100).into_proto(),
      heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
      rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
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
      offset_datetime_from_unix_millis(1_000),
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
    .assign_partition(
      key,
      "member-b".to_string(),
      2,
      offset_datetime_from_unix_millis(1_120),
      TimeDuration::milliseconds(100),
    )
    .await
    .unwrap();

  let report = coordinator
    .heartbeat_and_commit(
      offset_datetime_from_unix_millis(1_130),
      &HashMap::from([(7_u32, committed_cursor(7, 12))]),
    )
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
    failing_assignment_partition: None,
  });
  let store: Arc<dyn ConsumerGroupLeaseStore> = concrete_store.clone();
  let membership_store = membership_store();
  let mut coordinator = ConsumerGroupCoordinatorImpl::new(
    ConsumerGroupConfig {
      topic: "topic-a".to_string().into(),
      group_id: "group-a".to_string().into(),
      member_id: "member-a".to_string().into(),
      lease_duration: TimeDuration::milliseconds(100).into_proto(),
      heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
      rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
      ..Default::default()
    },
    store,
    membership_store,
  )
  .unwrap();

  coordinator
    .rebalance(
      vec!["member-a".to_string()],
      vec![7, 8],
      offset_datetime_from_unix_millis(1_000),
    )
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
      offset_datetime_from_unix_millis(1_120),
      TimeDuration::milliseconds(100),
    )
    .await
    .unwrap();

  assert!(
    coordinator
      .heartbeat_and_commit(offset_datetime_from_unix_millis(1_130), &HashMap::new())
      .await
      .is_err()
  );
  assert_eq!(coordinator.owned_partitions(), vec![8]);
}

#[tokio::test]
async fn rebalance_preserves_successful_claims_when_a_sibling_claim_fails() {
  let concrete_store = Arc::new(PartialHeartbeatFailureLeaseStore {
    inner: InMemoryConsumerGroupLeaseStore::new(),
    failing_partition: u32::MAX,
    failing_assignment_partition: Some(8),
  });
  let key = ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 7,
  };
  concrete_store
    .inner
    .assign_partition(
      key.clone(),
      "member-b".to_string(),
      1,
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
    )
    .await
    .unwrap();
  concrete_store
    .inner
    .heartbeat_partition(
      &key,
      "member-b",
      1,
      offset_datetime_from_unix_millis(1_010),
      TimeDuration::milliseconds(100),
      Some(committed_cursor(7, 42)),
    )
    .await
    .unwrap();

  let store: Arc<dyn ConsumerGroupLeaseStore> = concrete_store;
  let mut coordinator = ConsumerGroupCoordinatorImpl::new(
    ConsumerGroupConfig {
      topic: "topic-a".to_string().into(),
      group_id: "group-a".to_string().into(),
      member_id: "member-a".to_string().into(),
      lease_duration: TimeDuration::milliseconds(100).into_proto(),
      heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
      rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
      ..Default::default()
    },
    store,
    membership_store(),
  )
  .unwrap();

  let report = coordinator
    .rebalance(
      vec!["member-a".to_string()],
      vec![7, 8],
      offset_datetime_from_unix_millis(1_120),
    )
    .await
    .unwrap();

  assert_eq!(report.owned_partitions, vec![7]);
  assert_eq!(report.lease_claim_counts.expiry_takeovers, 1);
  assert_eq!(
    report.recovered_cursors.get(&7),
    Some(&crate::coordination::RecoveredCursor {
      committed_cursor: committed_cursor(7, 42),
      committed_ts_ms: Some(1_010),
    })
  );
  assert_eq!(
    report.active_partition_lease_expiration_deadline_ms,
    Some(1_220)
  );
  assert!(report.retry_error.is_some());
  assert_eq!(coordinator.owned_partitions(), vec![7]);
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
      lease_duration: TimeDuration::milliseconds(1_000).into_proto(),
      heartbeat_interval: TimeDuration::milliseconds(50).into_proto(),
      rebalance_interval: TimeDuration::milliseconds(50).into_proto(),
      ..Default::default()
    },
    Arc::clone(&store),
    membership_store,
  )
  .unwrap();

  coordinator
    .rebalance(
      vec!["member-a".to_string()],
      vec![7],
      offset_datetime_from_unix_millis(1_000),
    )
    .await
    .unwrap();
  assert_eq!(coordinator.owned_partitions(), vec![7]);

  let released = coordinator
    .release_owned(offset_datetime_from_unix_millis(1_010))
    .await
    .unwrap();
  assert_eq!(released, vec![7]);
  assert!(coordinator.owned_partitions().is_empty());

  let key = ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 7,
  };
  let reassigned = concrete_store
    .assign_partition(
      key,
      "member-b".to_string(),
      2,
      offset_datetime_from_unix_millis(1_010),
      TimeDuration::milliseconds(1_000),
    )
    .await
    .unwrap();
  assert!(matches!(
    reassigned,
    ConsumerGroupAssignmentOutcome::Assigned { .. }
  ));
}
