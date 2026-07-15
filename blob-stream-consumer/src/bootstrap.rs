#[cfg(test)]
#[path = "./bootstrap_test.rs"]
mod tests;

use crate::config::{ConsumerRuntimeConfig, validate_runtime_config};
use crate::iterator::{ConsumerCoordinationSource, ConsumerIteratorImpl, CoordinationSnapshot};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_types::region::Region;
use bd_pgv::proto_validate;
use bd_server_stats::stats::Scope;
use blob_stream_blob_store::{BlobStore, InMemoryBlobStore, S3BlobStore};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  DynamoConsumerGroupLeaseStore,
  DynamoConsumerGroupMembershipStore,
  DynamoMetadataStore,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
  InMemoryMetadataStore,
  MetadataStore,
};
use blob_stream_proto::protos::blobstream::v1::config::{
  BlobStoreConfig,
  ConsumerIteratorBootstrapConfig,
  MetadataStoreConfig,
  TopicConfig,
};
use blob_stream_types::{VirtualPartitionId, now_unix_millis};
use std::sync::Arc;

const DEFAULT_LEASE_TTL_BUFFER_SECONDS: u32 = 3_600;

//
// ConsumerBootstrapConfig
//

#[derive(Clone, Debug)]
/// Strongly typed bootstrap configuration for building a consumer iterator.
pub struct ConsumerBootstrapConfig {
  /// Runtime consumer read and group settings.
  pub runtime: ConsumerRuntimeConfig,
  /// Topic metadata used to derive partition space.
  pub topic: TopicConfig,
  /// Blob storage backend configuration.
  pub blob_store: BlobStoreConfig,
  /// Metadata/coordination backend configuration.
  pub metadata_store: MetadataStoreConfig,
}

//
// ConsumerConfigFactory
//

/// Factory for constructing ready-to-run consumer iterators.
pub struct ConsumerConfigFactory;

impl ConsumerConfigFactory {
  /// Build an iterator from typed bootstrap configuration.
  pub async fn build_iterator(
    config: ConsumerBootstrapConfig,
    metrics_scope: Scope,
  ) -> Result<ConsumerIteratorImpl> {
    ConsumerIteratorImpl::from_bootstrap_config(config, metrics_scope).await
  }

  /// Build an iterator directly from protobuf bootstrap configuration.
  pub async fn build_iterator_from_proto_config(
    config: ConsumerIteratorBootstrapConfig,
    metrics_scope: Scope,
  ) -> Result<ConsumerIteratorImpl> {
    let bootstrap = ConsumerBootstrapConfig::from_proto_config(&config)?;
    Self::build_iterator(bootstrap, metrics_scope).await
  }
}

impl ConsumerBootstrapConfig {
  /// Build a typed bootstrap configuration.
  #[must_use]
  pub fn new(
    runtime: ConsumerRuntimeConfig,
    topic: TopicConfig,
    blob_store: BlobStoreConfig,
    metadata_store: MetadataStoreConfig,
  ) -> Self {
    Self {
      runtime,
      topic,
      blob_store,
      metadata_store,
    }
  }

  /// Convert protobuf bootstrap configuration into typed configuration.
  pub fn from_proto_config(config: &ConsumerIteratorBootstrapConfig) -> Result<Self> {
    proto_validate::validate(config)?;

    let runtime = config
      .runtime
      .as_ref()
      .ok_or_else(|| anyhow!("consumer bootstrap runtime is required"))?
      .clone();
    let topic = config
      .topic
      .as_ref()
      .ok_or_else(|| anyhow!("consumer bootstrap topic is required"))?
      .clone();
    let blob_store = config
      .blob_store
      .as_ref()
      .ok_or_else(|| anyhow!("consumer bootstrap blob_store is required"))?
      .clone();
    let metadata_store = config
      .metadata_store
      .as_ref()
      .ok_or_else(|| anyhow!("consumer bootstrap metadata_store is required"))?
      .clone();

    Ok(Self {
      runtime,
      topic,
      blob_store,
      metadata_store,
    })
  }
}

impl ConsumerIteratorImpl {
  pub async fn from_bootstrap_config(
    config: ConsumerBootstrapConfig,
    metrics_scope: Scope,
  ) -> Result<Self> {
    validate_runtime_config(&config.runtime)?;
    proto_validate::validate(&config.topic)?;
    proto_validate::validate(&config.blob_store)?;
    proto_validate::validate(&config.metadata_store)?;

    let group = config
      .runtime
      .group
      .as_ref()
      .ok_or_else(|| anyhow!("consumer group config is required"))?;

    ensure!(
      group.topic == config.topic.name,
      "consumer runtime topic and bootstrap topic must match"
    );
    ensure!(
      config.topic.retention_days > 0,
      "consumer retention recovery requires topic retention_days greater than zero"
    );

    let virtual_partitions = virtual_partitions_for_topic(&config.topic)?;

    let blob_store = build_blob_store(&config.blob_store).await?;
    let (metadata_store, lease_store, membership_store) =
      build_metadata_and_coordination_stores(&config.metadata_store).await?;

    let coordination = Arc::new(MembershipCoordinationSource::new(
      group.topic.to_string(),
      group.group_id.to_string(),
      group.member_id.to_string(),
      virtual_partitions,
      membership_store.clone(),
    ));

    Self::from_runtime_config_with_retention_and_publication_lag(
      &config.runtime,
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      coordination,
      metrics_scope,
      config.topic.retention_days,
      config
        .topic
        .max_metadata_publication_lag_ms
        .unwrap_or(crate::config::DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS),
    )
    .await
  }
}

fn virtual_partitions_for_topic(topic: &TopicConfig) -> Result<Vec<VirtualPartitionId>> {
  let total = topic.partition_count.saturating_mul(topic.num_writers);
  ensure!(
    total > 0,
    "topic partition_count * num_writers must be greater than zero"
  );
  Ok((0 .. total).collect())
}

async fn build_blob_store(config: &BlobStoreConfig) -> Result<Arc<dyn BlobStore>> {
  if config.has_in_memory() {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
    return Ok(store);
  }

  if config.has_s3() {
    let s3 = config.s3();
    let bucket = s3.bucket.to_string();
    let region = s3.region.to_string();

    let region_provider = RegionProviderChain::first_try(Some(Region::new(region)));
    let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region_provider);
    if !s3.endpoint.trim().is_empty() {
      loader = loader.endpoint_url(s3.endpoint.to_string());
    }
    let shared = loader.load().await;
    let client = if s3.endpoint.trim().is_empty() {
      aws_sdk_s3::Client::new(&shared)
    } else {
      // Local S3-compatible endpoints (e.g. LocalStack) require path-style requests.
      let conf = aws_sdk_s3::config::Builder::from(&shared)
        .force_path_style(true)
        .build();
      aws_sdk_s3::Client::from_conf(conf)
    };

    let store: Arc<dyn BlobStore> = Arc::new(S3BlobStore::new(client, bucket));
    return Ok(store);
  }

  Err(anyhow!("blob_store backend not configured"))
}

async fn build_metadata_and_coordination_stores(
  config: &MetadataStoreConfig,
) -> Result<(
  Arc<dyn MetadataStore>,
  Arc<dyn ConsumerGroupLeaseStore>,
  Arc<dyn ConsumerGroupMembershipStore>,
)> {
  if config.has_in_memory() {
    let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
    let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
      Arc::new(InMemoryConsumerGroupLeaseStore::new());
    let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
      Arc::new(InMemoryConsumerGroupMembershipStore::new());
    return Ok((metadata_store, lease_store, membership_store));
  }

  if config.has_dynamo() {
    let dynamo = config.dynamo();
    let region = dynamo.region.to_string();
    let metadata_table = dynamo.segment_metadata_table_name.to_string();
    let consumer_lease_table = dynamo.consumer_group_lease_table_name.to_string();
    let consumer_membership_table = dynamo.consumer_group_membership_table_name.to_string();

    let region_provider = RegionProviderChain::first_try(Some(Region::new(region)));
    let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region_provider);
    if !dynamo.endpoint.trim().is_empty() {
      loader = loader.endpoint_url(dynamo.endpoint.to_string());
    }
    let shared = loader.load().await;
    let client = aws_sdk_dynamodb::Client::new(&shared);

    let metadata_store: Arc<dyn MetadataStore> =
      Arc::new(DynamoMetadataStore::new(client.clone(), metadata_table));
    let lease_ttl_buffer_seconds = dynamo
      .lease_ttl_buffer_seconds
      .unwrap_or(DEFAULT_LEASE_TTL_BUFFER_SECONDS);
    let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
      Arc::new(DynamoConsumerGroupLeaseStore::with_ttl_buffer_seconds(
        client.clone(),
        consumer_lease_table,
        lease_ttl_buffer_seconds,
      ));
    let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
      Arc::new(DynamoConsumerGroupMembershipStore::with_ttl_buffer_seconds(
        client,
        consumer_membership_table,
        lease_ttl_buffer_seconds,
      ));
    return Ok((metadata_store, lease_store, membership_store));
  }

  Err(anyhow!("metadata_store backend not configured"))
}

/// Dynamic coordination source backed by membership-store liveness entries.
pub struct MembershipCoordinationSource {
  topic: String,
  group_id: String,
  local_member_id: String,
  virtual_partitions: Vec<VirtualPartitionId>,
  membership_store: Arc<dyn ConsumerGroupMembershipStore>,
}

impl MembershipCoordinationSource {
  /// Create a membership-backed coordination source.
  #[must_use]
  pub fn new(
    topic: String,
    group_id: String,
    local_member_id: String,
    virtual_partitions: Vec<VirtualPartitionId>,
    membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  ) -> Self {
    Self {
      topic,
      group_id,
      local_member_id,
      virtual_partitions,
      membership_store,
    }
  }
}

#[async_trait]
impl ConsumerCoordinationSource for MembershipCoordinationSource {
  async fn snapshot(&self) -> Result<CoordinationSnapshot> {
    let now_ts_ms = now_unix_millis();
    let mut members = self
      .membership_store
      .list_active_members(&self.topic, &self.group_id, now_ts_ms)
      .await?;

    if !members.iter().any(|member| member == &self.local_member_id) {
      members.push(self.local_member_id.clone());
    }

    members.sort();
    members.dedup();

    Ok(CoordinationSnapshot {
      members,
      virtual_partitions: self.virtual_partitions.clone(),
    })
  }
}
