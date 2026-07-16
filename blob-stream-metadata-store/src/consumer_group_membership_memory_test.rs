use crate::{
  ConsumerGroupAssignment,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLeaseOutcome,
  InMemoryConsumerGroupMembershipStore,
};

#[tokio::test]
async fn register_heartbeat_list_and_deregister() {
  let store = InMemoryConsumerGroupMembershipStore::new();

  store
    .register_member("topic-a", "group-a", "member-a", 1_000, 100)
    .await
    .expect("register member-a");
  store
    .register_member("topic-a", "group-a", "member-b", 1_000, 100)
    .await
    .expect("register member-b");
  store
    .register_member("topic-a", "group-b", "member-c", 1_000, 100)
    .await
    .expect("register member-c");

  let members = store
    .list_active_members("topic-a", "group-a", 1_050)
    .await
    .expect("list members");
  assert_eq!(
    members,
    vec!["member-a".to_string(), "member-b".to_string()]
  );

  store
    .heartbeat_member("topic-a", "group-a", "member-a", 1_120, 100)
    .await
    .expect("heartbeat member-a");

  let members = store
    .list_active_members("topic-a", "group-a", 1_150)
    .await
    .expect("list members");
  assert_eq!(members, vec!["member-a".to_string()]);

  store
    .deregister_member("topic-a", "group-a", "member-a")
    .await
    .expect("deregister member-a");

  let members = store
    .list_active_members("topic-a", "group-a", 1_151)
    .await
    .expect("list members");
  assert!(members.is_empty());
}

#[tokio::test]
async fn register_rejects_invalid_ttl() {
  let store = InMemoryConsumerGroupMembershipStore::new();
  let err = store
    .register_member("topic-a", "group-a", "member-a", 1_000, 0)
    .await
    .expect_err("register should reject zero ttl");
  assert!(
    err
      .to_string()
      .contains("membership ttl must be greater than zero")
  );
}

#[tokio::test]
async fn planner_lease_fences_plan_publication() {
  let store = InMemoryConsumerGroupMembershipStore::new();
  let acquired = store
    .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-a", 1_000, 100)
    .await
    .expect("acquire planner");
  assert_eq!(acquired, ConsumerGroupPlannerLeaseOutcome::Acquired);

  let held = store
    .acquire_or_renew_planner("topic-a", "group-a", "member-b", "session-b", 1_050, 100)
    .await
    .expect("planner held by member-a");
  assert_eq!(held, ConsumerGroupPlannerLeaseOutcome::HeldByOther);

  let plan = ConsumerGroupAssignmentPlan {
    version: 1,
    planner_member_id: "member-a".to_string(),
    members: vec!["member-a".to_string(), "member-b".to_string()],
    assignments: vec![
      ConsumerGroupAssignment {
        virtual_partition_id: 0,
        member_id: "member-a".to_string(),
      },
      ConsumerGroupAssignment {
        virtual_partition_id: 1,
        member_id: "member-b".to_string(),
      },
    ],
    published_ts_ms: 1_000,
  };
  assert!(
    store
      .publish_assignment_plan(
        "topic-a",
        "group-a",
        "member-a",
        "session-a",
        1_001,
        plan.clone(),
      )
      .await
      .expect("publish plan")
  );
  assert!(
    !store
      .publish_assignment_plan(
        "topic-a",
        "group-a",
        "member-b",
        "session-b",
        1_001,
        plan.clone(),
      )
      .await
      .expect("reject non-planner publication")
  );
  assert_eq!(
    store
      .get_assignment_plan("topic-a", "group-a")
      .await
      .expect("read plan"),
    Some(plan)
  );

  let replacement = store
    .acquire_or_renew_planner("topic-a", "group-a", "member-b", "session-b", 1_100, 100)
    .await
    .expect("acquire expired planner");
  assert_eq!(replacement, ConsumerGroupPlannerLeaseOutcome::Acquired);
}

#[tokio::test]
async fn planner_release_allows_immediate_takeover_and_preserves_successor() {
  let store = InMemoryConsumerGroupMembershipStore::new();
  let plan = ConsumerGroupAssignmentPlan {
    version: 1,
    planner_member_id: "member-a".to_string(),
    members: vec!["member-a".to_string()],
    assignments: vec![ConsumerGroupAssignment {
      virtual_partition_id: 0,
      member_id: "member-a".to_string(),
    }],
    published_ts_ms: 1_000,
  };
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-a", 1_000, 1_000)
      .await
      .expect("acquire member-a planner"),
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );
  assert!(
    store
      .publish_assignment_plan(
        "topic-a",
        "group-a",
        "member-a",
        "session-a",
        1_000,
        plan.clone(),
      )
      .await
      .expect("publish member-a plan")
  );
  assert!(
    store
      .release_planner("topic-a", "group-a", "member-a", "session-a")
      .await
      .expect("release member-a planner")
  );
  assert_eq!(
    store
      .get_assignment_plan("topic-a", "group-a")
      .await
      .expect("read retained plan"),
    Some(plan)
  );
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-b", "session-b", 1_001, 1_000)
      .await
      .expect("acquire member-b planner"),
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );
  assert!(
    !store
      .release_planner("topic-a", "group-a", "member-a", "session-a")
      .await
      .expect("reject stale planner release")
  );
  assert_eq!(
    store
      .get_planner_lease("topic-a", "group-a")
      .await
      .expect("read member-b planner")
      .map(|lease| lease.member_id),
    Some("member-b".to_string())
  );
}

#[tokio::test]
async fn planner_session_fences_stale_same_member_process() {
  let store = InMemoryConsumerGroupMembershipStore::new();
  let plan = ConsumerGroupAssignmentPlan {
    version: 1,
    planner_member_id: "member-a".to_string(),
    members: vec!["member-a".to_string()],
    assignments: vec![ConsumerGroupAssignment {
      virtual_partition_id: 0,
      member_id: "member-a".to_string(),
    }],
    published_ts_ms: 1_100,
  };

  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-old", 1_000, 100)
      .await
      .expect("acquire initial session"),
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-new", 1_100, 100)
      .await
      .expect("acquire replacement session"),
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );
  assert!(
    !store
      .publish_assignment_plan(
        "topic-a",
        "group-a",
        "member-a",
        "session-old",
        1_101,
        plan.clone(),
      )
      .await
      .expect("reject stale publication")
  );
  assert!(
    !store
      .release_planner("topic-a", "group-a", "member-a", "session-old")
      .await
      .expect("reject stale release")
  );
  assert!(
    store
      .publish_assignment_plan("topic-a", "group-a", "member-a", "session-new", 1_101, plan)
      .await
      .expect("accept active session publication")
  );
}

#[tokio::test]
async fn planner_rejects_plan_declared_for_a_different_member() {
  let store = InMemoryConsumerGroupMembershipStore::new();
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-a", 1_000, 100)
      .await
      .expect("acquire planner"),
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );
  let mismatched_plan = ConsumerGroupAssignmentPlan {
    version: 1,
    planner_member_id: "member-b".to_string(),
    members: vec!["member-a".to_string()],
    assignments: vec![ConsumerGroupAssignment {
      virtual_partition_id: 0,
      member_id: "member-a".to_string(),
    }],
    published_ts_ms: 1_000,
  };
  assert!(
    !store
      .publish_assignment_plan(
        "topic-a",
        "group-a",
        "member-a",
        "session-a",
        1_001,
        mismatched_plan,
      )
      .await
      .expect("reject mismatched planner member")
  );
}
