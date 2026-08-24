#[cfg(test)]
#[path = "./bootstrap_test.rs"]
mod tests;

use crate::config::{
  ConsumerRuntimeConfig,
  apply_consumer_startup_overrides,
  consumer_max_clock_skew,
  topic_max_metadata_publication_lag,
  topic_metadata_cache_max_age,
  validate_runtime_config,
};
use crate::consumer::{BrokerClientPool, GrpcBrokerBlobRangeQuery, GrpcBrokerMetadataQuery};
use crate::iterator::{
  ConsumerCoordinationSource,
  ConsumerIteratorBuilder,
  ConsumerIteratorImpl,
  ConsumerLifecycleHooks,
  CoordinationSnapshot,
};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_types::region::Region;
use bd_pgv::proto_validate;
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use bd_server_stats::stats::Scope;
use bd_time::{SystemTimeProvider, TimeProvider};
use blob_stream_blob_store::{BlobStore, InMemoryBlobStore, S3BlobStore};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseStore,
  ConsumerGroupMember,
  ConsumerGroupMembershipStore,
  DynamoCapacityMetrics,
  DynamoConsumerGroupLeaseStore,
  DynamoConsumerGroupMembershipStore,
  DynamoMetadataStore,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
  InMemoryMetadataStore,
  MetadataStore,
  aws_retry_config,
  aws_timeout_config,
};
use blob_stream_proto::protos::blobstream::v1::config::{
  BlobStoreConfig,
  BrokerDiscoveryConfig,
  ConsumerIteratorBootstrapConfig,
  MetadataStoreConfig,
  TopicConfig,
};
use blob_stream_types::{ProtoDurationExt, VirtualPartitionId, topic_metadata_window_size};
use std::collections::HashMap;
use std::sync::Arc;
use time::Duration;

const DEFAULT_MEMBERSHIP_TTL_BUFFER: Duration = Duration::hours(1);

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
  /// Broker discovery for optional broker-collapsed metadata queries.
  pub broker_discovery: BrokerDiscoveryConfig,
}

//
// ConsumerBootstrapIteratorBuilder
//

/// Builder for a consumer iterator initialized from bootstrap configuration.
pub struct ConsumerBootstrapIteratorBuilder {
  config: ConsumerBootstrapConfig,
  metrics_scope: Scope,
  feature_flags: Option<FeatureFlagsWatch>,
  time_provider: Arc<dyn TimeProvider>,
  lifecycle_hooks: Option<Arc<dyn ConsumerLifecycleHooks>>,
}

impl ConsumerBootstrapIteratorBuilder {
  #[must_use]
  pub fn new(
    config: ConsumerBootstrapConfig,
    metrics_scope: Scope,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Self {
    Self {
      config,
      metrics_scope,
      feature_flags,
      time_provider: Arc::new(SystemTimeProvider),
      lifecycle_hooks: None,
    }
  }

  /// Use an explicit clock for coordination and driver scheduling.
  #[must_use]
  pub fn time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
    self.time_provider = time_provider;
    self
  }

  /// Use explicit lifecycle hooks for observing iterator transitions.
  #[must_use]
  pub fn lifecycle_hooks(mut self, lifecycle_hooks: Arc<dyn ConsumerLifecycleHooks>) -> Self {
    self.lifecycle_hooks = Some(lifecycle_hooks);
    self
  }

  /// Build the configured consumer iterator.
  pub async fn build(self) -> Result<ConsumerIteratorImpl> {
    ConsumerIteratorImpl::build_from_bootstrap(
      self.config,
      self.metrics_scope,
      self.feature_flags,
      self.time_provider,
      self.lifecycle_hooks,
    )
    .await
  }
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
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Result<ConsumerIteratorImpl> {
    ConsumerIteratorImpl::from_bootstrap_config(config, metrics_scope, feature_flags).await
  }

  /// Build an iterator directly from protobuf bootstrap configuration.
  pub async fn build_iterator_from_proto_config(
    config: ConsumerIteratorBootstrapConfig,
    metrics_scope: Scope,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Result<ConsumerIteratorImpl> {
    let bootstrap = ConsumerBootstrapConfig::from_proto_config(&config)?;
    Self::build_iterator(bootstrap, metrics_scope, feature_flags).await
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
    broker_discovery: BrokerDiscoveryConfig,
  ) -> Self {
    Self {
      runtime,
      topic,
      blob_store,
      metadata_store,
      broker_discovery,
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
    let broker_discovery = config
      .broker_discovery
      .as_ref()
      .ok_or_else(|| anyhow!("consumer bootstrap broker_discovery is required"))?
      .clone();

    Ok(Self {
      runtime,
      topic,
      blob_store,
      metadata_store,
      broker_discovery,
    })
  }
}

impl ConsumerIteratorImpl {
  pub async fn from_bootstrap_config(
    config: ConsumerBootstrapConfig,
    metrics_scope: Scope,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Result<Self> {
    ConsumerBootstrapIteratorBuilder::new(config, metrics_scope, feature_flags)
      .build()
      .await
  }

  async fn build_from_bootstrap(
    mut config: ConsumerBootstrapConfig,
    metrics_scope: Scope,
    feature_flags: Option<FeatureFlagsWatch>,
    time_provider: Arc<dyn TimeProvider>,
    lifecycle_hooks: Option<Arc<dyn ConsumerLifecycleHooks>>,
  ) -> Result<Self> {
    if let Some(feature_flags) = feature_flags.as_ref() {
      apply_consumer_startup_overrides(feature_flags, &mut config.runtime)?;
    }
    validate_runtime_config(&config.runtime)?;
    proto_validate::validate(&config.topic)?;
    proto_validate::validate(&config.blob_store)?;
    proto_validate::validate(&config.metadata_store)?;
    proto_validate::validate(&config.broker_discovery)?;

    let retention = config
      .topic
      .retention
      .as_ref()
      .ok_or_else(|| anyhow!("consumer topic config is missing retention"))?
      .to_time_duration();
    let metadata_window_size = topic_metadata_window_size(&config.topic)?;

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
      retention.is_positive(),
      "consumer retention recovery requires topic retention greater than zero"
    );

    let virtual_partitions = virtual_partitions_for_topic(&config.topic)?;

    let blob_store = build_blob_store(&config.blob_store).await?;
    let dynamo_capacity_metrics = DynamoCapacityMetrics::new(&metrics_scope.scope("dynamo"));
    let (metadata_store, lease_store, membership_store) = build_metadata_and_coordination_stores(
      &config.metadata_store,
      retention,
      dynamo_capacity_metrics,
    )
    .await?;

    let coordination = Arc::new(
      MembershipCoordinationSource::new(
        group.topic.to_string(),
        group.group_id.to_string(),
        group.member_id.to_string(),
        virtual_partitions,
        membership_store.clone(),
      )
      .time_provider(Arc::clone(&time_provider)),
    );
    let broker_client_pool =
      Arc::new(BrokerClientPool::from_config(&config.broker_discovery).await?);
    let broker_metadata_query = Arc::new(GrpcBrokerMetadataQuery::from_client_pool(Arc::clone(
      &broker_client_pool,
    )));
    let broker_blob_range_query = Arc::new(GrpcBrokerBlobRangeQuery::from_client_pool(
      broker_client_pool,
    ));

    let builder = ConsumerIteratorBuilder::new(
      &config.runtime,
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      coordination,
      broker_metadata_query,
      broker_blob_range_query,
      metrics_scope,
      retention,
      topic_max_metadata_publication_lag(&config.topic),
      feature_flags,
    )
    .metadata_window_size(metadata_window_size)
    .metadata_cache_max_age(topic_metadata_cache_max_age(&config.topic))
    .maximum_clock_skew(consumer_max_clock_skew(
      config
        .runtime
        .read
        .as_ref()
        .ok_or_else(|| anyhow!("consumer read config is required"))?,
    ))
    .time_provider(time_provider);
    let builder = if let Some(lifecycle_hooks) = lifecycle_hooks {
      builder.lifecycle_hooks(lifecycle_hooks)
    } else {
      builder
    };
    builder.build().await
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
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
      .region(region_provider)
      .retry_config(aws_retry_config())
      .timeout_config(aws_timeout_config());
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
  retention: Duration,
  capacity_metrics: DynamoCapacityMetrics,
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
    let producer_partition_lease_table = dynamo.producer_partition_lease_table_name.to_string();
    let consumer_lease_table = dynamo.consumer_group_lease_table_name.to_string();
    let consumer_membership_table = dynamo.consumer_group_membership_table_name.to_string();

    let region_provider = RegionProviderChain::first_try(Some(Region::new(region)));
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
      .region(region_provider)
      .retry_config(aws_retry_config())
      .timeout_config(aws_timeout_config());
    if !dynamo.endpoint.trim().is_empty() {
      loader = loader.endpoint_url(dynamo.endpoint.to_string());
    }
    let shared = loader.load().await;
    let client = aws_sdk_dynamodb::Client::new(&shared);

    let metadata_store: Arc<dyn MetadataStore> = Arc::new(DynamoMetadataStore::new(
      client.clone(),
      metadata_table,
      producer_partition_lease_table,
      HashMap::new(),
      DEFAULT_MEMBERSHIP_TTL_BUFFER,
      Some(capacity_metrics.clone()),
    ));
    let membership_ttl_buffer = dynamo.lease_ttl_buffer.as_ref().map_or(
      DEFAULT_MEMBERSHIP_TTL_BUFFER,
      ProtoDurationExt::to_time_duration,
    );
    let consumer_lease_ttl_buffer = consumer_group_lease_ttl_buffer(retention)?;
    let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
      Arc::new(DynamoConsumerGroupLeaseStore::new(
        client.clone(),
        consumer_lease_table,
        consumer_lease_ttl_buffer,
        Some(capacity_metrics.clone()),
      ));
    let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
      Arc::new(DynamoConsumerGroupMembershipStore::new(
        client,
        consumer_membership_table,
        membership_ttl_buffer,
        Some(capacity_metrics),
      ));
    return Ok((metadata_store, lease_store, membership_store));
  }

  Err(anyhow!("metadata_store backend not configured"))
}

fn consumer_group_lease_ttl_buffer(retention: Duration) -> Result<Duration> {
  ensure!(
    retention.is_positive(),
    "topic retention must be greater than zero for consumer lease TTL"
  );
  Ok(retention)
}

/// Dynamic coordination source backed by membership-store liveness entries.
pub struct MembershipCoordinationSource {
  topic: String,
  group_id: String,
  local_member_id: String,
  virtual_partitions: Vec<VirtualPartitionId>,
  membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  time_provider: Arc<dyn TimeProvider>,
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
      time_provider: Arc::new(SystemTimeProvider),
    }
  }

  /// Use a shared clock for membership liveness snapshots.
  #[must_use]
  pub fn time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
    self.time_provider = time_provider;
    self
  }
}

#[async_trait]
impl ConsumerCoordinationSource for MembershipCoordinationSource {
  async fn snapshot(&self) -> Result<CoordinationSnapshot> {
    let now = self.time_provider.now();
    let mut members = self
      .membership_store
      .list_active_members(&self.topic, &self.group_id, now)
      .await?;

    if !members
      .iter()
      .any(|member| member.member_id == self.local_member_id)
    {
      members.push(ConsumerGroupMember {
        member_id: self.local_member_id.clone(),
        pod_id: None,
      });
    }

    members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
    members.dedup_by(|left, right| left.member_id == right.member_id);

    Ok(CoordinationSnapshot {
      members: members.into_iter().map(|member| member.member_id).collect(),
      virtual_partitions: self.virtual_partitions.clone(),
    })
  }
}
