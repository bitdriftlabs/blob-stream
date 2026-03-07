use crate::test_framework::runtime::runtime_sleep;
use crate::test_framework::store_faults::{
  FaultInjectedBlobStore,
  FaultInjectedConsumerGroupLeaseStore,
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
};
use aws_sdk_s3::Client as S3Client;
use blob_stream_blob_store::{BlobStore, InMemoryBlobStore};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseStore,
  DynamoConsumerGroupLeaseStore,
  DynamoMetadataStore,
  DynamoProducerPartitionLeaseStore,
  MetadataStore,
  ProducerPartitionLeaseStore,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

const DYNAMO_ENDPOINT: &str = "http://localhost:8000";
const S3_ENDPOINT: &str = "http://localhost:4566";
const AWS_REGION: &str = "us-east-1";

#[cfg(test)]
#[ctor::ctor]
fn global_init() {
  bd_test_helpers::test_global_init();
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
}

impl IntegrationResources {
  pub async fn create() -> Result<Self> {
    configure_aws_env();

    let dynamo_config = aws_config::defaults(BehaviorVersion::latest())
      .endpoint_url(DYNAMO_ENDPOINT)
      .load()
      .await;
    let dynamo = DynamoClient::new(&dynamo_config);

    let shared_s3 = aws_config::defaults(BehaviorVersion::latest())
      .endpoint_url(S3_ENDPOINT)
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
    let bucket = format!("blob-stream-it-{}", Uuid::new_v4().simple());

    create_table_pk_sk(&dynamo, &metadata_table).await?;
    create_table_pk_only(&dynamo, &producer_lease_table).await?;
    create_table_pk_sk(&dynamo, &consumer_lease_table).await?;
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
    })
  }

  pub fn blob_store(&self) -> Arc<dyn BlobStore> {
    Arc::new(FaultInjectedBlobStore::new(
      Arc::clone(&self.blob_store),
      self.store_fault_controller.clone(),
    ))
  }

  pub fn metadata_store(&self) -> Arc<dyn MetadataStore> {
    let inner: Arc<dyn MetadataStore> = Arc::new(DynamoMetadataStore::new(
      self.dynamo.clone(),
      self.metadata_table.clone(),
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
    ));
    Arc::new(FaultInjectedConsumerGroupLeaseStore::new(
      inner,
      self.store_fault_controller.clone(),
    ))
  }

  pub fn store_fault_controller(&self) -> StoreFaultController {
    self.store_fault_controller.clone()
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
        "compose dependencies not ready: dynamo_ready={dynamo_ready}, s3_ready={s3_ready}"
      ));
    }

    runtime_sleep(Duration::from_millis(300)).await;
  }
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
