use crate::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupReleaseOutcome,
  DynamoConsumerGroupLeaseStore,
};
use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
  AttributeDefinition,
  BillingMode,
  KeySchemaElement,
  KeyType,
  ScalarAttributeType,
};
use blob_stream_types::CommittedCursor;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

const LOCAL_ENDPOINT: &str = "http://localhost:8000";
const REGION: &str = "us-east-1";

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

async fn create_leases_table(client: &Client, table_name: &str) -> Result<()> {
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
async fn fences_assignment() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone());
  let key = lease_key();

  let outcome = store
    .assign_partition(key.clone(), "member-a".to_string(), 1, 1000, 100)
    .await?;

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));

  let outcome = store
    .assign_partition(key.clone(), "member-b".to_string(), 1, 1000, 100)
    .await?;

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::HeldByOther(_)
  ));

  let outcome = store
    .assign_partition(key.clone(), "member-b".to_string(), 2, 1100, 100)
    .await?;

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}

#[tokio::test]
async fn heartbeats_and_commits() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone());
  let key = lease_key();

  store
    .assign_partition(key.clone(), "member-a".to_string(), 1, 1000, 100)
    .await?;

  let outcome = store
    .heartbeat_partition(
      &key,
      "member-a",
      1,
      1010,
      100,
      Some(cursor(key.virtual_partition_id, 10)),
    )
    .await?;

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
    .await?;

  let ConsumerGroupCommitOutcome::Committed(lease) = outcome else {
    panic!("expected committed cursor");
  };

  assert_eq!(
    lease.committed_cursor,
    Some(cursor(key.virtual_partition_id, 12))
  );

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}

#[tokio::test]
async fn heartbeat_fences_other_members() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone());
  let key = lease_key();

  store
    .assign_partition(key.clone(), "member-a".to_string(), 1, 1000, 100)
    .await?;

  let outcome = store
    .heartbeat_partition(&key, "member-b", 1, 1010, 100, None)
    .await?;

  assert!(matches!(
    outcome,
    ConsumerGroupHeartbeatOutcome::HeldByOther(_)
  ));

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}

#[tokio::test]
async fn release_partition_allows_immediate_takeover() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone());

  let key = ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 1,
  };

  store
    .assign_partition(key.clone(), "member-a".to_string(), 2, 1000, 100)
    .await?;

  let release = store.release_partition(&key, "member-a", 2, 1010).await?;
  assert_eq!(release, ConsumerGroupReleaseOutcome::Released);

  let reassigned = store
    .assign_partition(key, "member-b".to_string(), 3, 1010, 100)
    .await?;
  assert!(matches!(
    reassigned,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn release_partition_rejects_stale_owner_or_generation() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone());
  let key = ConsumerGroupLeaseKey {
    topic: "topic-a".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 1,
  };

  store
    .assign_partition(key.clone(), "member-a".to_string(), 2, 1000, 100)
    .await?;

  let release = store.release_partition(&key, "member-b", 2, 1010).await?;
  assert!(matches!(
    release,
    ConsumerGroupReleaseOutcome::HeldByOther(_)
  ));

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}
