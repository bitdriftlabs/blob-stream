// blob-stream - DynamoDB producer partition leases tests
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{
  DynamoProducerPartitionLeaseStore,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SequenceReservationOutcome,
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
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("pk")
        .key_type(KeyType::Hash)
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

fn lease_key() -> ProducerPartitionLeaseKey {
  ProducerPartitionLeaseKey {
    topic: "topic-a".to_string(),
    writer_id: 1,
    virtual_partition_id: 42,
  }
}

#[tokio::test]
async fn fences_lease_holders() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("producer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoProducerPartitionLeaseStore::new(client.clone(), table_name.clone());
  let key = lease_key();

  let outcome = store
    .acquire_lease(key.clone(), "broker-a".to_string(), 1000, 100)
    .await?;

  assert!(matches!(outcome, LeaseAcquireOutcome::Acquired(_)));

  let outcome = store
    .acquire_lease(key.clone(), "broker-b".to_string(), 1000, 100)
    .await?;

  assert!(matches!(outcome, LeaseAcquireOutcome::HeldByOther(_)));

  let outcome = store.heartbeat_lease(&key, "broker-b", 1000, 100).await?;

  assert!(matches!(outcome, LeaseHeartbeatOutcome::HeldByOther(_)));

  let outcome = store
    .acquire_lease(key.clone(), "broker-b".to_string(), 1100, 100)
    .await?;

  assert!(matches!(outcome, LeaseAcquireOutcome::Acquired(_)));

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}

#[tokio::test]
async fn reserves_sequences_in_order() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("producer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoProducerPartitionLeaseStore::new(client.clone(), table_name.clone());
  let key = lease_key();

  store
    .acquire_lease(key.clone(), "broker-a".to_string(), 1000, 100)
    .await?;

  let first = store
    .reserve_sequences(&key, "broker-a", 1000, 100, 5)
    .await?;

  let SequenceReservationOutcome::Reserved(first) = first else {
    panic!("expected reservation");
  };

  assert_eq!(first.range.start, 0);
  assert_eq!(first.range.end, 4);

  let second = store
    .reserve_sequences(&key, "broker-a", 1000, 100, 3)
    .await?;

  let SequenceReservationOutcome::Reserved(second) = second else {
    panic!("expected reservation");
  };

  assert_eq!(second.range.start, 5);
  assert_eq!(second.range.end, 7);

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}
