use crate::test_framework::runtime::runtime_sleep;
use crate::test_framework::store_faults::{
  FaultInjectedBlobStore,
  FaultInjectedConsumerGroupLeaseStore,
  FaultInjectedConsumerGroupMembershipStore,
  FaultInjectedMetadataStore,
  FaultInjectedProducerPartitionLeaseStore,
  StoreFaultController,
};
use anyhow::{Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::types::{
  AttributeDefinition,
  BillingMode,
  KeySchemaElement,
  KeyType,
  ScalarAttributeType,
  TimeToLiveSpecification,
};
use aws_sdk_s3::Client as S3Client;
use blob_stream_blob_store::{BlobStore, InMemoryBlobStore, S3BlobStore};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  DynamoConsumerGroupLeaseStore,
  DynamoConsumerGroupMembershipStore,
  DynamoMetadataStore,
  DynamoProducerPartitionLeaseStore,
  MetadataStore,
  ProducerPartitionLeaseStore,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

const DEFAULT_DYNAMO_ENDPOINT: &str = "http://localhost:8000";
const DEFAULT_S3_ENDPOINT: &str = "http://localhost:4566";
const AWS_REGION: &str = "us-east-1";
const TTL_ATTRIBUTE_NAME: &str = "ttl_epoch_seconds";

#[ctor::ctor(unsafe)]
fn global_init() {
  bd_test_helpers_core::test_global_init();
}

//
// IntegrationResources
//

pub struct IntegrationResources {
  dynamo: DynamoClient,
  s3: S3Client,
  blob_store: Arc<dyn BlobStore>,
  store_fault_controller: StoreFaultController,
  bucket: String,
  metadata_table: String,
  producer_lease_table: String,
  consumer_lease_table: String,
  consumer_membership_table: String,
}

impl IntegrationResources {
  pub async fn create() -> Result<Self> {
    configure_aws_env();

    let dynamo_config = aws_config::defaults(BehaviorVersion::latest())
      .endpoint_url(dynamo_endpoint())
      .load()
      .await;
    let dynamo = DynamoClient::new(&dynamo_config);

    let shared_s3 = aws_config::defaults(BehaviorVersion::latest())
      .endpoint_url(s3_endpoint())
      .load()
      .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&shared_s3)
      .force_path_style(true)
      .build();
    let s3 = S3Client::from_conf(s3_config);

    // Ensure backing services are ready before creating per-test tables and buckets.
    wait_for_dependencies(&dynamo, &s3).await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let metadata_table = format!("blob_segments_it_{suffix}");
    let producer_lease_table = format!("producer_leases_it_{suffix}");
    let consumer_lease_table = format!("consumer_leases_it_{suffix}");
    let consumer_membership_table = format!("consumer_membership_it_{suffix}");
    let bucket = format!("blob-stream-it-{}", Uuid::new_v4().simple());

    create_table_pk_sk(&dynamo, &metadata_table).await?;
    create_table_pk_only(&dynamo, &producer_lease_table).await?;
    create_table_pk_sk(&dynamo, &consumer_lease_table).await?;
    create_table_pk_sk(&dynamo, &consumer_membership_table).await?;
    enable_table_ttl(&dynamo, &metadata_table).await?;
    enable_table_ttl(&dynamo, &producer_lease_table).await?;
    enable_table_ttl(&dynamo, &consumer_lease_table).await?;
    enable_table_ttl(&dynamo, &consumer_membership_table).await?;
    create_bucket(&s3, &bucket).await?;
    verify_s3_roundtrip(&s3, &bucket).await?;

    let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
    let store_fault_controller = StoreFaultController::default();

    Ok(Self {
      dynamo,
      s3,
      blob_store,
      store_fault_controller,
      bucket,
      metadata_table,
      producer_lease_table,
      consumer_lease_table,
      consumer_membership_table,
    })
  }

  pub fn blob_store(&self) -> Arc<dyn BlobStore> {
    Arc::new(FaultInjectedBlobStore::new(
      Arc::clone(&self.blob_store),
      self.store_fault_controller.clone(),
    ))
  }

  pub fn s3_blob_store(&self) -> Arc<dyn BlobStore> {
    Arc::new(FaultInjectedBlobStore::new(
      Arc::new(S3BlobStore::new(self.s3.clone(), self.bucket.clone())),
      self.store_fault_controller.clone(),
    ))
  }

  pub fn metadata_store(&self) -> Arc<dyn MetadataStore> {
    let inner: Arc<dyn MetadataStore> = Arc::new(DynamoMetadataStore::new(
      self.dynamo.clone(),
      self.metadata_table.clone(),
      HashMap::new(),
      3_600,
      None,
    ));
    Arc::new(FaultInjectedMetadataStore::new(
      inner,
      self.store_fault_controller.clone(),
    ))
  }

  pub fn producer_lease_store(&self) -> Arc<dyn ProducerPartitionLeaseStore> {
    let inner: Arc<dyn ProducerPartitionLeaseStore> =
      Arc::new(DynamoProducerPartitionLeaseStore::new(
        self.dynamo.clone(),
        self.producer_lease_table.clone(),
        3_600,
        None,
      ));
    Arc::new(FaultInjectedProducerPartitionLeaseStore::new(
      inner,
      self.store_fault_controller.clone(),
    ))
  }

  pub fn consumer_lease_store(&self) -> Arc<dyn ConsumerGroupLeaseStore> {
    let inner: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(DynamoConsumerGroupLeaseStore::new(
      self.dynamo.clone(),
      self.consumer_lease_table.clone(),
      3_600,
      None,
    ));
    Arc::new(FaultInjectedConsumerGroupLeaseStore::new(
      inner,
      self.store_fault_controller.clone(),
    ))
  }

  pub fn consumer_membership_store(&self) -> Arc<dyn ConsumerGroupMembershipStore> {
    let inner: Arc<dyn ConsumerGroupMembershipStore> =
      Arc::new(DynamoConsumerGroupMembershipStore::new(
        self.dynamo.clone(),
        self.consumer_membership_table.clone(),
        3_600,
        None,
      ));
    Arc::new(FaultInjectedConsumerGroupMembershipStore::new(
      inner,
      self.store_fault_controller.clone(),
    ))
  }

  pub fn store_fault_controller(&self) -> StoreFaultController {
    self.store_fault_controller.clone()
  }

  #[must_use]
  pub fn aws_region(&self) -> &'static str {
    AWS_REGION
  }

  #[must_use]
  pub fn dynamo_endpoint(&self) -> String {
    dynamo_endpoint()
  }

  #[must_use]
  pub fn s3_endpoint(&self) -> String {
    s3_endpoint()
  }

  #[must_use]
  pub fn bucket_name(&self) -> &str {
    &self.bucket
  }

  #[must_use]
  pub fn segment_metadata_table_name(&self) -> &str {
    &self.metadata_table
  }

  #[must_use]
  pub fn producer_lease_table_name(&self) -> &str {
    &self.producer_lease_table
  }

  #[must_use]
  pub fn consumer_lease_table_name(&self) -> &str {
    &self.consumer_lease_table
  }

  #[must_use]
  pub fn consumer_membership_table_name(&self) -> &str {
    &self.consumer_membership_table
  }

  pub async fn cleanup(&self) {
    // Cleanup is best-effort so one failure does not block deleting remaining resources.
    let _ = self
      .dynamo
      .delete_table()
      .table_name(&self.metadata_table)
      .send()
      .await;
    let _ = self
      .dynamo
      .delete_table()
      .table_name(&self.producer_lease_table)
      .send()
      .await;
    let _ = self
      .dynamo
      .delete_table()
      .table_name(&self.consumer_lease_table)
      .send()
      .await;
    let _ = self
      .dynamo
      .delete_table()
      .table_name(&self.consumer_membership_table)
      .send()
      .await;

    if let Ok(objects) = self.s3.list_objects_v2().bucket(&self.bucket).send().await
      && let Some(contents) = objects.contents
    {
      for object in contents {
        if let Some(key) = object.key {
          let _ = self
            .s3
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        }
      }
    }

    let _ = self.s3.delete_bucket().bucket(&self.bucket).send().await;
  }
}

fn configure_aws_env() {
  unsafe {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", AWS_REGION);
  }
}

async fn wait_for_dependencies(dynamo: &DynamoClient, s3: &S3Client) -> Result<()> {
  let started = Instant::now();
  let timeout_at = Duration::from_secs(30);

  loop {
    let dynamo_ready = dynamo.list_tables().send().await.is_ok();
    let s3_ready = s3.list_buckets().send().await.is_ok();

    if dynamo_ready && s3_ready {
      return Ok(());
    }

    if started.elapsed() >= timeout_at {
      return Err(anyhow!(
        "integration-test dependencies not ready: dynamo_ready={dynamo_ready}, s3_ready={s3_ready}"
      ));
    }

    runtime_sleep(Duration::from_millis(300)).await;
  }
}

fn dynamo_endpoint() -> String {
  std::env::var("BD_ITEST_DYNAMODB_ENDPOINT")
    .unwrap_or_else(|_| DEFAULT_DYNAMO_ENDPOINT.to_string())
}

fn s3_endpoint() -> String {
  std::env::var("BD_ITEST_S3_ENDPOINT").unwrap_or_else(|_| DEFAULT_S3_ENDPOINT.to_string())
}

async fn create_table_pk_sk(client: &DynamoClient, table_name: &str) -> Result<()> {
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
    .await?;

  wait_for_table_active(client, table_name).await
}

async fn create_table_pk_only(client: &DynamoClient, table_name: &str) -> Result<()> {
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
    .await?;

  wait_for_table_active(client, table_name).await
}

async fn wait_for_table_active(client: &DynamoClient, table_name: &str) -> Result<()> {
  for _ in 0 .. 40 {
    let response = client.describe_table().table_name(table_name).send().await;
    if let Ok(response) = response
      && let Some(status) = response.table().and_then(|table| table.table_status())
      && status.as_str() == "ACTIVE"
    {
      return Ok(());
    }

    runtime_sleep(Duration::from_millis(150)).await;
  }

  Err(anyhow!("table {table_name} did not become active"))
}

async fn enable_table_ttl(client: &DynamoClient, table_name: &str) -> Result<()> {
  let specification = TimeToLiveSpecification::builder()
    .attribute_name(TTL_ATTRIBUTE_NAME)
    .enabled(true)
    .build()?;

  // DynamoDB Local/LocalStack may not support TTL APIs uniformly across versions. Treat this as
  // best-effort in integration setup because tests assert persisted ttl attributes directly.
  let result = client
    .update_time_to_live()
    .table_name(table_name)
    .time_to_live_specification(specification)
    .send()
    .await;

  match result {
    Ok(_) => Ok(()),
    Err(error) => {
      let text = error.to_string();
      if text.contains("Time to Live") || text.contains("TimeToLive") {
        return Ok(());
      }
      Err(error.into())
    },
  }
}

async fn create_bucket(client: &S3Client, bucket: &str) -> Result<()> {
  client.create_bucket().bucket(bucket).send().await?;
  Ok(())
}

async fn verify_s3_roundtrip(client: &S3Client, bucket: &str) -> Result<()> {
  let key = format!("integration-healthcheck-{}", Uuid::new_v4().simple());
  let payload = b"ok".to_vec();

  client
    .put_object()
    .bucket(bucket)
    .key(&key)
    .body(payload.clone().into())
    .send()
    .await?;

  let response = client.get_object().bucket(bucket).key(&key).send().await?;
  let body = response.body.collect().await?;
  if body.into_bytes().to_vec() != payload {
    return Err(anyhow!("s3 healthcheck payload mismatch"));
  }

  client
    .delete_object()
    .bucket(bucket)
    .key(&key)
    .send()
    .await?;

  Ok(())
}
