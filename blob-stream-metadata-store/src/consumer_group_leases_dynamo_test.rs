use crate::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupLeaseTransition,
  ConsumerGroupReleaseOutcome,
  DynamoConsumerGroupLeaseStore,
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
use blob_stream_types::{CommittedCursor, CommittedSourceCheckpoint};
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

const LOCAL_ENDPOINT: &str = "http://localhost:8000";
const REGION: &str = "us-east-1";
const TTL_ATTRIBUTE_NAME: &str = "ttl_epoch_seconds";

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

fn lease_key_for(topic: &str, group_id: &str, virtual_partition_id: u32) -> ConsumerGroupLeaseKey {
  ConsumerGroupLeaseKey {
    topic: topic.to_string(),
    group_id: group_id.to_string(),
    virtual_partition_id,
  }
}

fn cursor_with_source(virtual_partition_id: u32, seq_end: u64) -> CommittedCursor {
  CommittedCursor {
    virtual_partition_id,
    seq_end,
    source_checkpoint: Some(CommittedSourceCheckpoint {
      window_start_unix_seconds: 1_200,
      snowflake_id: 42,
    }),
  }
}

#[tokio::test]
async fn list_group_leases_returns_retained_rows_in_partition_order() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone(), 3_600, None);
  let key_two = lease_key_for("topic-a", "group-a", 2);
  let key_ten = lease_key_for("topic-a", "group-a", 10);
  let other_group_key = lease_key_for("topic-a", "group-b", 4);
  store
    .assign_partition(key_ten.clone(), "member-b".to_string(), 3, 1_000, 100)
    .await?;
  store
    .heartbeat_partition(
      &key_ten,
      "member-b",
      3,
      1_010,
      100,
      Some(cursor_with_source(10, 42)),
    )
    .await?;
  store
    .assign_partition(key_two.clone(), "member-a".to_string(), 2, 1_000, 100)
    .await?;
  assert_eq!(
    store
      .release_partition(&key_two, "member-a", 2, 1_020)
      .await?,
    ConsumerGroupReleaseOutcome::Released
  );
  store
    .assign_partition(other_group_key, "member-c".to_string(), 1, 1_000, 100)
    .await?;

  let leases = store.list_group_leases("topic-a", "group-a").await?;
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
  assert_eq!(leases[1].committed_cursor, Some(cursor_with_source(10, 42)));
  assert_eq!(leases[1].committed_ts_ms, Some(1_010));

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn fences_assignment() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone(), 3_600, None);
  let key = lease_key();

  let outcome = store
    .assign_partition(key.clone(), "member-a".to_string(), 1, 1000, 100)
    .await?;

  assert!(matches!(
    outcome,
    ConsumerGroupAssignmentOutcome::Assigned {
      transition: ConsumerGroupLeaseTransition::Initial,
      ..
    }
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
    ConsumerGroupAssignmentOutcome::Assigned {
      transition: ConsumerGroupLeaseTransition::ExpiryTakeover {
        previous_owner_id,
        previous_generation: 1,
        ..
      },
      ..
    } if previous_owner_id == "member-a"
  ));

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}

#[tokio::test]
async fn heartbeats_and_commits() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone(), 3_600, None);
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
      Some(cursor_with_source(key.virtual_partition_id, 10)),
    )
    .await?;

  let ConsumerGroupHeartbeatOutcome::Renewed(lease) = outcome else {
    panic!("expected renewed heartbeat");
  };

  assert_eq!(
    lease.committed_cursor,
    Some(cursor_with_source(key.virtual_partition_id, 10))
  );

  let outcome = store
    .commit_cursor(
      &key,
      "member-a",
      1,
      1020,
      cursor_with_source(key.virtual_partition_id, 12),
    )
    .await?;

  let ConsumerGroupCommitOutcome::Committed(lease) = outcome else {
    panic!("expected committed cursor");
  };

  assert_eq!(
    lease.committed_cursor,
    Some(cursor_with_source(key.virtual_partition_id, 12))
  );

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}

#[tokio::test]
async fn heartbeat_fences_other_members() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone(), 3_600, None);
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

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone(), 3_600, None);

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

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn release_partition_rejects_stale_owner_or_generation() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone(), 3_600, None);
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

#[tokio::test]
async fn writes_ttl_attribute_for_consumer_leases() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("consumer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoConsumerGroupLeaseStore::new(client.clone(), table_name.clone(), 120, None);
  let key = lease_key();

  store
    .assign_partition(key.clone(), "member-a".to_string(), 1, 1_000, 1_000)
    .await?;

  let item = client
    .get_item()
    .table_name(&table_name)
    .key("pk", AttributeValue::S(key.partition_key()))
    .key("sk", AttributeValue::S(key.sort_key()))
    .send()
    .await?
    .item
    .ok_or_else(|| anyhow!("expected lease item"))?;

  let ttl = item
    .get(TTL_ATTRIBUTE_NAME)
    .and_then(|value| value.as_n().ok())
    .ok_or_else(|| anyhow!("missing ttl attribute"))?
    .parse::<i64>()?;

  assert_eq!(ttl, 122);
  for attribute in ["topic", "group_id", "virtual_partition_id"] {
    assert!(
      !item.contains_key(attribute),
      "consumer lease item unexpectedly contains {attribute}"
    );
  }

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}
