use crate::{ConsumerGroupMembershipStore, DynamoConsumerGroupMembershipStore};
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

  let store = DynamoConsumerGroupMembershipStore::new(client.clone(), table_name.clone());

  store
    .register_member("topic-a", "group-a", "member-a", 1_000, 100)
    .await?;
  store
    .register_member("topic-a", "group-a", "member-b", 1_000, 100)
    .await?;
  store
    .register_member("topic-a", "group-b", "member-c", 1_000, 100)
    .await?;

  let members = store
    .list_active_members("topic-a", "group-a", 1_050)
    .await?;
  assert_eq!(
    members,
    vec!["member-a".to_string(), "member-b".to_string()]
  );

  store
    .heartbeat_member("topic-a", "group-a", "member-a", 1_120, 100)
    .await?;

  let members = store
    .list_active_members("topic-a", "group-a", 1_150)
    .await?;
  assert_eq!(members, vec!["member-a".to_string()]);

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

  let store = DynamoConsumerGroupMembershipStore::new(client.clone(), table_name.clone());
  let err = store
    .register_member("topic-a", "group-a", "member-a", 1_000, 0)
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

  let store = DynamoConsumerGroupMembershipStore::with_ttl_buffer_seconds(
    client.clone(),
    table_name.clone(),
    120,
  );

  store
    .register_member("topic-a", "group-a", "member-a", 1_000, 1_000)
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
  for attribute in ["topic", "group_id", "member_id"] {
    assert!(
      !item.contains_key(attribute),
      "membership item unexpectedly contains {attribute}"
    );
  }

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}
