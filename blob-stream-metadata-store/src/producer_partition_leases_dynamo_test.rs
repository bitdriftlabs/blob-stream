use super::DynamoLeaseItem;
use crate::{
  DynamoProducerPartitionLeaseStore,
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  ProducerSequenceProgress,
  SequenceReservationOutcome,
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
use blob_stream_types::offset_datetime_from_unix_millis;
use std::collections::HashMap;
use std::time::Duration;
use time::Duration as TimeDuration;
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
    topic: "topic-a".into(),
    virtual_partition_id: 42,
  }
}

fn default_lease_store(client: Client, table_name: String) -> DynamoProducerPartitionLeaseStore {
  DynamoProducerPartitionLeaseStore::new(client, table_name, TimeDuration::hours(1), None)
}

#[test]
fn rejects_legacy_lease_rows_without_fence_identity() {
  let decoded: std::result::Result<DynamoLeaseItem, _> = serde_dynamo::from_item(HashMap::from([
    (
      "holder_id".to_string(),
      AttributeValue::S("broker-a".to_string()),
    ),
    (
      "lease_expiration_ts_ms".to_string(),
      AttributeValue::N("1100".to_string()),
    ),
  ]));
  let error = decoded.expect_err("legacy lease rows must not deserialize");

  assert!(error.to_string().contains("lease_epoch"));
}

#[tokio::test]
async fn fences_lease_holders() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("producer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = default_lease_store(client.clone(), table_name.clone());
  let key = lease_key();

  let outcome = store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;

  assert!(matches!(outcome, LeaseAcquireOutcome::Acquired(_)));

  let outcome = store
    .acquire_lease(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;

  assert!(matches!(outcome, LeaseAcquireOutcome::HeldByOther(_)));

  let outcome = store
    .heartbeat_lease(
      &key,
      "broker-b",
      "session-b",
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;

  assert!(matches!(outcome, LeaseHeartbeatOutcome::HeldByOther(_)));

  let outcome = store
    .acquire_lease(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_100),
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
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

  let store = default_lease_store(client.clone(), table_name.clone());
  let key = lease_key();

  store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;

  let first = store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_000),
      5,
      ProducerSequenceProgress::default(),
    )
    .await?;

  let SequenceReservationOutcome::Reserved(first) = first else {
    panic!("expected reservation");
  };

  assert_eq!(first.range.start, 0);
  assert_eq!(first.range.end, 4);

  let second = store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_000),
      3,
      ProducerSequenceProgress::default(),
    )
    .await?;

  let SequenceReservationOutcome::Reserved(second) = second else {
    panic!("expected reservation");
  };

  assert_eq!(second.range.start, 5);
  assert_eq!(second.range.end, 7);

  client.delete_table().table_name(table_name).send().await?;

  Ok(())
}

#[tokio::test]
async fn acquires_and_reserves_sequences_in_one_operation() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("producer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = default_lease_store(client.clone(), table_name.clone());
  let key = lease_key();
  let first = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      Some(5),
      ProducerSequenceProgress::default(),
    )
    .await?;
  let LeaseAcquireAndReserveOutcome::Acquired { lease, reservation } = first else {
    panic!("expected acquired lease");
  };
  assert_eq!(
    lease.lease_expiration_at,
    offset_datetime_from_unix_millis(1_100)
  );
  assert_eq!(lease.max_allocated_seq, Some(4));
  assert_eq!(
    reservation,
    Some(blob_stream_types::SeqRange { start: 0, end: 4 })
  );

  let second = store
    .acquire_lease_and_reserve_sequences(
      key,
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_050),
      TimeDuration::milliseconds(100),
      Some(3),
      ProducerSequenceProgress::default(),
    )
    .await?;
  let LeaseAcquireAndReserveOutcome::Acquired { lease, reservation } = second else {
    panic!("expected renewed lease");
  };
  assert_eq!(
    lease.lease_expiration_at,
    offset_datetime_from_unix_millis(1_150)
  );
  assert_eq!(lease.max_allocated_seq, Some(7));
  assert_eq!(
    reservation,
    Some(blob_stream_types::SeqRange { start: 5, end: 7 })
  );

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn persists_sequence_progress_with_each_successful_mutation() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("producer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = default_lease_store(client.clone(), table_name.clone());
  let key = lease_key();
  let now = offset_datetime_from_unix_millis(1_000);
  store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      now,
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;
  store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-a",
      now,
      5,
      ProducerSequenceProgress::default(),
    )
    .await?;
  let progress = ProducerSequenceProgress {
    lease_sequence_start: Some(0),
    last_handed_out_seq: Some(2),
  };
  store
    .heartbeat_lease(
      &key,
      "broker-a",
      "session-a",
      now,
      TimeDuration::milliseconds(100),
      progress,
    )
    .await?;
  store
    .reserve_sequences(&key, "broker-a", "session-a", now, 3, progress)
    .await?;

  let lease = store
    .get_lease(&key)
    .await?
    .ok_or_else(|| anyhow!("expected active lease"))?;
  assert_eq!(lease.lease_sequence_start, Some(0));
  assert_eq!(lease.max_allocated_seq, Some(7));
  assert_eq!(lease.last_handed_out_seq, Some(2));
  assert_eq!(lease.sequence_progress_updated_at, Some(now));

  let stale = store
    .heartbeat_lease(
      &key,
      "broker-a",
      "session-b",
      now,
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(stale, LeaseHeartbeatOutcome::HeldByOther(_)));
  let lease = store
    .get_lease(&key)
    .await?
    .ok_or_else(|| anyhow!("expected active lease"))?;
  assert_eq!(lease.lease_sequence_start, Some(0));
  assert_eq!(lease.last_handed_out_seq, Some(2));

  let cleared_at = now + TimeDuration::milliseconds(1);
  store
    .heartbeat_lease(
      &key,
      "broker-a",
      "session-a",
      cleared_at,
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress {
        lease_sequence_start: Some(0),
        last_handed_out_seq: None,
      },
    )
    .await?;
  let lease = store
    .get_lease(&key)
    .await?
    .ok_or_else(|| anyhow!("expected active lease"))?;
  assert_eq!(lease.lease_sequence_start, Some(0));
  assert_eq!(lease.last_handed_out_seq, None);
  assert_eq!(lease.sequence_progress_updated_at, Some(cleared_at));

  let released_at = cleared_at + TimeDuration::milliseconds(1);
  store
    .release_lease(&key, "broker-a", "session-a", released_at, progress)
    .await?;
  let lease = store
    .get_lease(&key)
    .await?
    .ok_or_else(|| anyhow!("expected released lease row"))?;
  assert_eq!(lease.lease_sequence_start, Some(0));
  assert_eq!(lease.last_handed_out_seq, Some(2));
  assert_eq!(lease.sequence_progress_updated_at, Some(released_at));

  let atomic_key = ProducerPartitionLeaseKey {
    topic: "topic-a".into(),
    virtual_partition_id: 43,
  };
  store
    .acquire_lease_and_reserve_sequences(
      atomic_key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      now,
      TimeDuration::milliseconds(100),
      Some(5),
      ProducerSequenceProgress::default(),
    )
    .await?;
  let lease = store
    .get_lease(&atomic_key)
    .await?
    .ok_or_else(|| anyhow!("expected atomic lease"))?;
  assert_eq!(lease.lease_sequence_start, Some(0));
  assert_eq!(lease.last_handed_out_seq, None);
  assert_eq!(lease.sequence_progress_updated_at, Some(now));

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn rejects_overflowing_atomic_sequence_reservation() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("producer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = default_lease_store(client.clone(), table_name.clone());
  let key = lease_key();
  store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      Some(u64::MAX),
      ProducerSequenceProgress::default(),
    )
    .await?;

  let error = store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_050),
      TimeDuration::milliseconds(100),
      Some(2),
      ProducerSequenceProgress::default(),
    )
    .await
    .expect_err("overflowing reservation must fail");
  assert!(error.to_string().contains("sequence range overflow"));

  let error = store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_050),
      2,
      ProducerSequenceProgress::default(),
    )
    .await
    .expect_err("overflowing standalone reservation must fail");
  assert!(error.to_string().contains("sequence range overflow"));

  let lease = store
    .get_lease(&key)
    .await?
    .ok_or_else(|| anyhow!("lease should remain readable after rejected reservation"))?;
  assert_eq!(lease.max_allocated_seq, Some(u64::MAX - 1));

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn releases_lease_for_current_holder() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("producer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = default_lease_store(client.clone(), table_name.clone());
  let key = lease_key();

  store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;

  let reservation = store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_000),
      5,
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(
    reservation,
    SequenceReservationOutcome::Reserved(_)
  ));

  let release = store
    .release_lease(
      &key,
      "broker-a",
      "session-a",
      offset_datetime_from_unix_millis(1_000),
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(release, LeaseReleaseOutcome::Released));

  let released_lease = store
    .get_lease(&key)
    .await?
    .ok_or_else(|| anyhow!("released lease row should remain available"))?;
  assert_eq!(
    released_lease.lease_expiration_at,
    offset_datetime_from_unix_millis(1_000)
  );
  assert_eq!(released_lease.max_allocated_seq, Some(4));

  let reacquire = store
    .acquire_lease(
      key.clone(),
      "broker-b".to_string(),
      "session-b".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(reacquire, LeaseAcquireOutcome::Acquired(_)));

  let reservation = store
    .reserve_sequences(
      &key,
      "broker-b",
      "session-b",
      offset_datetime_from_unix_millis(1_000),
      2,
      ProducerSequenceProgress::default(),
    )
    .await?;
  let SequenceReservationOutcome::Reserved(reservation) = reservation else {
    panic!("expected reservation");
  };
  assert_eq!(reservation.range.start, 5);
  assert_eq!(reservation.range.end, 6);

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn lookup_reports_absent_and_active_leases() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("producer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = default_lease_store(client.clone(), table_name.clone());
  let key = lease_key();
  assert!(store.get_lease(&key).await?.is_none());

  store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;

  let lease = store
    .get_lease(&key)
    .await?
    .ok_or_else(|| anyhow!("active lease should exist"))?;
  assert_eq!(lease.fence.holder_id, "broker-a");
  assert_eq!(
    lease.lease_expiration_at,
    offset_datetime_from_unix_millis(1_100)
  );

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}

#[tokio::test]
async fn writes_ttl_attribute_for_lease_rows() -> Result<()> {
  let client = dynamo_client().await?;
  let table_name = format!("producer_leases_test_{}", Uuid::new_v4());
  create_leases_table(&client, &table_name).await?;

  let store = DynamoProducerPartitionLeaseStore::new(
    client.clone(),
    table_name.clone(),
    TimeDuration::seconds(120),
    None,
  );
  let key = lease_key();

  store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-a".to_string(),
      offset_datetime_from_unix_millis(2_000),
      TimeDuration::milliseconds(1_000),
      ProducerSequenceProgress::default(),
    )
    .await?;

  let item = client
    .get_item()
    .table_name(&table_name)
    .key("pk", AttributeValue::S(key.format()))
    .send()
    .await?
    .item
    .ok_or_else(|| anyhow!("expected lease item"))?;

  let ttl = item
    .get(TTL_ATTRIBUTE_NAME)
    .and_then(|value| value.as_n().ok())
    .ok_or_else(|| anyhow!("missing ttl attribute"))?
    .parse::<i64>()?;

  assert_eq!(ttl, 123);
  for attribute in ["topic", "virtual_partition_id"] {
    assert!(
      !item.contains_key(attribute),
      "producer lease item unexpectedly contains {attribute}"
    );
  }

  client.delete_table().table_name(table_name).send().await?;
  Ok(())
}
