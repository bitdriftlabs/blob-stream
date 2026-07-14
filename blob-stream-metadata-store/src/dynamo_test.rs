use crate::{DynamoMetadataStore, MetadataStore, SegmentMetadata};
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
use blob_stream_blob_store::BlobKey;
use blob_stream_types::{
  BatchMetadata,
  BatchSummary,
  Compression,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
use std::collections::HashMap;
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

async fn create_segments_table(client: &Client, table_name: &str) -> Result<()> {
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

fn build_segment(
  topic: &str,
  window_start_unix_seconds: i64,
  snowflake_id: u64,
) -> SegmentMetadata {
  let mut segment_index = HashMap::new();
  let batch = BatchMetadata {
    seq_range: SeqRange { start: 10, end: 19 },
    byte_range: blob_stream_types::ByteRange { start: 0, end: 512 },
    summary: BatchSummary {
      record_count: 10,
      payload_bytes: 512,
      min_event_ts_ms: 1000,
      max_event_ts_ms: 2000,
    },
    compression: Compression::none(),
  };
  segment_index.insert(0 as VirtualPartitionId, vec![batch]);

  SegmentMetadata::new(
    TopicWindowKey {
      topic: topic.to_string(),
      window_start_unix_seconds,
    },
    SnowflakeId(snowflake_id),
    BlobKey::from("topic/1/segment"),
    segment_index,
    Compression::none(),
    10,
    1000,
    2000,
    None,
    3000,
  )
}

#[tokio::test]
async fn writes_and_scans_window() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("blob_segments_test_{}", Uuid::new_v4());
  create_segments_table(&client, &table_name).await?;

  let store = DynamoMetadataStore::new(client.clone(), table_name.clone());
  let first = build_segment("topic-a", 100, 1);
  let second = build_segment("topic-a", 100, 2);
  let other = build_segment("topic-b", 200, 3);

  store.write_segment(first.clone()).await?;
  store.write_segment(second.clone()).await?;
  store.write_segment(other).await?;

  let window = TopicWindowKey {
    topic: "topic-a".to_string(),
    window_start_unix_seconds: 100,
  };
  let segments = store.scan_window(&window).await?;

  assert_eq!(segments.len(), 2);
  assert!(segments.contains(&first));
  assert!(segments.contains(&second));

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}

#[tokio::test]
async fn writes_segment_ttl_attribute() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("blob_segments_test_{}", Uuid::new_v4());
  create_segments_table(&client, &table_name).await?;

  let mut retention_days = HashMap::new();
  retention_days.insert("topic-a".to_string(), 7);
  let store = DynamoMetadataStore::with_segment_ttl(
    client.clone(),
    table_name.clone(),
    retention_days,
    3_600,
  );
  let segment = build_segment("topic-a", 100, 1);

  store.write_segment(segment.clone()).await?;

  let item = client
    .get_item()
    .table_name(&table_name)
    .key("pk", AttributeValue::S(segment.partition_key()))
    .key("sk", AttributeValue::S(segment.snowflake_key()))
    .send()
    .await?
    .item
    .ok_or_else(|| anyhow!("expected item"))?;

  let ttl = item
    .get(TTL_ATTRIBUTE_NAME)
    .and_then(|value| value.as_n().ok())
    .ok_or_else(|| anyhow!("missing ttl attribute"))?
    .parse::<i64>()?;
  let expected = (segment.created_ts_ms / 1_000) + (7 * 24 * 60 * 60) + 3_600;

  assert_eq!(ttl, expected);

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}
