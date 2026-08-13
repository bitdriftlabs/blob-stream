use crate::aws::retry_dynamo_transaction_conflicts;
use crate::{
  ConsumerGroupAssignment,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupMember,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLeaseOutcome,
  DynamoConsumerGroupMembershipStore,
};
use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
  AttributeDefinition,
  AttributeValue,
  BillingMode,
  KeySchemaElement,
  KeyType,
  ScalarAttributeType,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

const LOCAL_ENDPOINT: &str = "http://localhost:8000";
const REGION: &str = "us-east-1";
const TTL_ATTRIBUTE_NAME: &str = "ttl_epoch_seconds";
const RECORD_TYPE_ATTRIBUTE_NAME: &str = "record_type";
const POD_ID_ATTRIBUTE_NAME: &str = "pod_id";

#[tokio::test]
async fn transaction_conflict_retries_until_success() {
  let attempts = Arc::new(AtomicUsize::new(0));
  let operation_attempts = attempts.clone();

  let result = retry_dynamo_transaction_conflicts(
    "test",
    move || {
      let operation_attempts = operation_attempts.clone();
      async move {
        let attempt = operation_attempts.fetch_add(1, Ordering::Relaxed);
        if attempt == 0 {
          Err("transaction conflict")
        } else {
          Ok(())
        }
      }
    },
    |error| *error == "transaction conflict",
  )
  .await;

  assert_eq!(result, Ok(()));
  assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

async fn dynamo_client() -> Result<Client> {
  unsafe {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", REGION);
  }

  let config = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url(LOCAL_ENDPOINT)
    .load()
    .await;
  Ok(Client::new(&config))
}

async fn create_membership_table(client: &Client, table_name: &str) -> Result<()> {
  client
    .create_table()
    .table_name(table_name)
    .attribute_definitions(
      AttributeDefinition::builder()
        .attribute_name("pk")
        .attribute_type(ScalarAttributeType::S)
        .build()?,
    )
    .attribute_definitions(
      AttributeDefinition::builder()
        .attribute_name("sk")
        .attribute_type(ScalarAttributeType::S)
        .build()?,
    )
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("pk")
        .key_type(KeyType::Hash)
        .build()?,
    )
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("sk")
        .key_type(KeyType::Range)
        .build()?,
    )
    .billing_mode(BillingMode::PayPerRequest)
    .send()
    .await
    .with_context(|| format!("create table {table_name}"))?;

  wait_for_table_active(client, table_name).await
}

async fn wait_for_table_active(client: &Client, table_name: &str) -> Result<()> {
  for _ in 0 .. 20 {
    let response = client.describe_table().table_name(table_name).send().await;
    if let Ok(response) = response
      && let Some(status) = response.table().and_then(|table| table.table_status())
      && status.as_str() == "ACTIVE"
    {
      return Ok(());
    }

    sleep(Duration::from_millis(100)).await;
  }

  Err(anyhow!("table {table_name} did not become active"))
}

#[tokio::test]
async fn register_heartbeat_list_and_deregister() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_membership_test_{}", Uuid::new_v4());
  create_membership_table(&client, &table_name).await?;

  let store =
    DynamoConsumerGroupMembershipStore::new(client.clone(), table_name.clone(), 3_600, None);

  store
    .register_member(
      "topic-a",
      "group-a",
      "member-a",
      Some("pod-a".to_string()),
      1_000,
      100,
    )
    .await?;
  store
    .register_member("topic-a", "group-a", "member-b", None, 1_000, 100)
    .await?;
  store
    .register_member("topic-a", "group-b", "member-c", None, 1_000, 100)
    .await?;

  let members = store
    .list_active_members("topic-a", "group-a", 1_050)
    .await?;
  assert_eq!(
    members,
    vec![
      ConsumerGroupMember {
        member_id: "member-a".to_string(),
        pod_id: Some("pod-a".to_string()),
      },
      ConsumerGroupMember {
        member_id: "member-b".to_string(),
        pod_id: None,
      },
    ]
  );

  store
    .heartbeat_member(
      "topic-a",
      "group-a",
      "member-a",
      Some("pod-a".to_string()),
      1_120,
      100,
    )
    .await?;

  let members = store
    .list_active_members("topic-a", "group-a", 1_150)
    .await?;
  assert_eq!(
    members,
    vec![ConsumerGroupMember {
      member_id: "member-a".to_string(),
      pod_id: Some("pod-a".to_string()),
    }]
  );

  store
    .deregister_member("topic-a", "group-a", "member-a")
    .await?;

  let members = store
    .list_active_members("topic-a", "group-a", 1_151)
    .await?;
  assert!(members.is_empty());

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn register_rejects_invalid_ttl() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_membership_test_{}", Uuid::new_v4());
  create_membership_table(&client, &table_name).await?;

  let store =
    DynamoConsumerGroupMembershipStore::new(client.clone(), table_name.clone(), 3_600, None);
  let err = store
    .register_member("topic-a", "group-a", "member-a", None, 1_000, 0)
    .await
    .expect_err("expected invalid ttl error");
  assert!(
    err
      .to_string()
      .contains("membership ttl must be greater than zero")
  );

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn writes_ttl_attribute_for_membership_rows() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_membership_test_{}", Uuid::new_v4());
  create_membership_table(&client, &table_name).await?;

  let store =
    DynamoConsumerGroupMembershipStore::new(client.clone(), table_name.clone(), 120, None);

  store
    .register_member(
      "topic-a",
      "group-a",
      "member-a",
      Some("pod-a".to_string()),
      1_000,
      1_000,
    )
    .await?;

  let item = client
    .get_item()
    .table_name(&table_name)
    .key("pk", AttributeValue::S("topic-a#group-a".to_string()))
    .key("sk", AttributeValue::S("member-a".to_string()))
    .send()
    .await?
    .item
    .ok_or_else(|| anyhow!("expected membership item"))?;

  let ttl = item
    .get(TTL_ATTRIBUTE_NAME)
    .and_then(|value| value.as_n().ok())
    .ok_or_else(|| anyhow!("missing ttl attribute"))?
    .parse::<i64>()?;

  assert_eq!(ttl, 122);
  assert_eq!(
    item
      .get(RECORD_TYPE_ATTRIBUTE_NAME)
      .and_then(|value| value.as_s().ok().map(String::as_str)),
    Some("member")
  );
  assert_eq!(
    item
      .get(POD_ID_ATTRIBUTE_NAME)
      .and_then(|value| value.as_s().ok().map(String::as_str)),
    Some("pod-a")
  );
  for attribute in ["topic", "group_id", "member_id"] {
    assert!(
      !item.contains_key(attribute),
      "membership item unexpectedly contains {attribute}"
    );
  }

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn planner_release_allows_immediate_takeover_and_fences_stale_owner() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_membership_test_{}", Uuid::new_v4());
  create_membership_table(&client, &table_name).await?;

  let store =
    DynamoConsumerGroupMembershipStore::new(client.clone(), table_name.clone(), 3_600, None);
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-a", 1_000, 1_000)
      .await?,
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );
  assert!(
    store
      .release_planner("topic-a", "group-a", "member-a", "session-a")
      .await?
  );
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-b", "session-b", 1_001, 1_000)
      .await?,
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );
  assert!(
    !store
      .release_planner("topic-a", "group-a", "member-a", "session-a")
      .await?
  );
  assert_eq!(
    store
      .get_planner_lease("topic-a", "group-a")
      .await?
      .map(|lease| lease.member_id),
    Some("member-b".to_string())
  );

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn planner_records_do_not_appear_in_legacy_member_partition() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_membership_test_{}", Uuid::new_v4());
  create_membership_table(&client, &table_name).await?;

  let store =
    DynamoConsumerGroupMembershipStore::new(client.clone(), table_name.clone(), 3_600, None);
  store
    .register_member("topic-a", "group-a", "member-a", None, 1_000, 1_000)
    .await?;
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-a", 1_000, 1_000)
      .await?,
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
        ConsumerGroupAssignmentPlan {
          version: 1,
          planner_member_id: "member-a".to_string(),
          members: vec!["member-a".to_string()],
          member_topology: None,
          assignments: vec![ConsumerGroupAssignment {
            virtual_partition_id: 0,
            member_id: "member-a".to_string(),
          }],
          published_ts_ms: 1_000,
        },
      )
      .await?
  );

  let legacy_members = client
    .query()
    .table_name(&table_name)
    .key_condition_expression("pk = :pk")
    .expression_attribute_values(":pk", AttributeValue::S("topic-a#group-a".to_string()))
    .send()
    .await?;
  assert_eq!(legacy_members.count(), 1);
  assert_eq!(
    legacy_members
      .items()
      .first()
      .and_then(|item| item.get("sk"))
      .and_then(|value| value.as_s().ok().map(String::as_str)),
    Some("member-a")
  );

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn assignment_plan_topology_round_trips_and_is_removed_for_flat_plan() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_membership_test_{}", Uuid::new_v4());
  create_membership_table(&client, &table_name).await?;

  let store =
    DynamoConsumerGroupMembershipStore::new(client.clone(), table_name.clone(), 3_600, None);
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-a", 1_000, 1_000)
      .await?,
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );
  let assignments = vec![
    ConsumerGroupAssignment {
      virtual_partition_id: 0,
      member_id: "member-a".to_string(),
    },
    ConsumerGroupAssignment {
      virtual_partition_id: 1,
      member_id: "member-b".to_string(),
    },
  ];
  let member_topology = vec![
    ConsumerGroupMember {
      member_id: "member-a".to_string(),
      pod_id: Some("pod-a".to_string()),
    },
    ConsumerGroupMember {
      member_id: "member-b".to_string(),
      pod_id: Some("pod-b".to_string()),
    },
  ];
  assert!(
    store
      .publish_assignment_plan(
        "topic-a",
        "group-a",
        "member-a",
        "session-a",
        1_000,
        ConsumerGroupAssignmentPlan {
          version: 1,
          planner_member_id: "member-a".to_string(),
          members: vec!["member-a".to_string(), "member-b".to_string()],
          member_topology: Some(member_topology.clone()),
          assignments: assignments.clone(),
          published_ts_ms: 1_000,
        },
      )
      .await?
  );
  assert_eq!(
    store
      .get_assignment_plan("topic-a", "group-a")
      .await?
      .and_then(|plan| plan.member_topology),
    Some(member_topology)
  );

  assert!(
    store
      .publish_assignment_plan(
        "topic-a",
        "group-a",
        "member-a",
        "session-a",
        1_001,
        ConsumerGroupAssignmentPlan {
          version: 2,
          planner_member_id: "member-a".to_string(),
          members: vec!["member-a".to_string(), "member-b".to_string()],
          member_topology: None,
          assignments,
          published_ts_ms: 1_001,
        },
      )
      .await?
  );
  assert_eq!(
    store
      .get_assignment_plan("topic-a", "group-a")
      .await?
      .and_then(|plan| plan.member_topology),
    None
  );

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[test]
fn assignment_plan_topology_missing_pod_id_returns_error() {
  let plan = ConsumerGroupAssignmentPlan {
    version: 1,
    planner_member_id: "member-a".to_string(),
    members: vec!["member-a".to_string()],
    member_topology: Some(vec![ConsumerGroupMember {
      member_id: "member-a".to_string(),
      pod_id: None,
    }]),
    assignments: vec![ConsumerGroupAssignment {
      virtual_partition_id: 0,
      member_id: "member-a".to_string(),
    }],
    published_ts_ms: 1_000,
  };

  assert_eq!(
    DynamoConsumerGroupMembershipStore::plan_values(&plan)
      .err()
      .map(|error| error.to_string()),
    Some("pod-aware assignment topology missing pod ID for member member-a".to_string())
  );
}

#[tokio::test]
async fn planner_session_fences_stale_process_and_mismatched_plan_publisher() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_membership_test_{}", Uuid::new_v4());
  create_membership_table(&client, &table_name).await?;

  let store =
    DynamoConsumerGroupMembershipStore::new(client.clone(), table_name.clone(), 3_600, None);
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-old", 1_000, 100)
      .await?,
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );
  assert_eq!(
    store
      .acquire_or_renew_planner("topic-a", "group-a", "member-a", "session-new", 1_100, 100)
      .await?,
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );

  let plan = ConsumerGroupAssignmentPlan {
    version: 1,
    planner_member_id: "member-a".to_string(),
    members: vec!["member-a".to_string()],
    member_topology: None,
    assignments: vec![ConsumerGroupAssignment {
      virtual_partition_id: 0,
      member_id: "member-a".to_string(),
    }],
    published_ts_ms: 1_100,
  };
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
      .await?
  );
  assert!(
    !store
      .release_planner("topic-a", "group-a", "member-a", "session-old")
      .await?
  );

  let mut mismatched_plan = plan.clone();
  mismatched_plan.planner_member_id = "member-b".to_string();
  assert!(
    !store
      .publish_assignment_plan(
        "topic-a",
        "group-a",
        "member-a",
        "session-new",
        1_101,
        mismatched_plan,
      )
      .await?
  );
  assert!(
    store
      .publish_assignment_plan("topic-a", "group-a", "member-a", "session-new", 1_101, plan)
      .await?
  );

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}
