use crate::write::{AdmissionController, DEFAULT_ZSTD_LEVEL, WriteEngine, WriteEngineBuilder};
use anyhow::{Context, Result, anyhow, ensure};
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_types::region::Region;
use bd_log_util::warn_every;
use bd_pgv::proto_validate;
use bd_runtime_config::feature_flags::{FeatureFlags, FeatureFlagsWatch};
use bd_server_stats::stats::Scope;
use bd_shutdown::ComponentShutdownTriggerHandle;
use blob_stream_blob_store::BlobStore;
use blob_stream_broker_discovery::k8s::K8sServiceBrokerDiscovery;
use blob_stream_broker_discovery::r#static::StaticBrokerDiscovery;
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{
  DynamoCapacityMetrics,
  DynamoProducerPartitionLeaseStore,
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  MetadataStore,
  ProducerPartitionLeaseStore,
  aws_retry_config,
  aws_timeout_config,
};
use blob_stream_proto::protos::blobstream::v1::config::{
  BrokerConfig,
  DynamoMetadataStoreConfig,
  MetadataStoreConfig,
  RuntimeConfig,
  SegmentCompression,
  TopicConfig,
};
use blob_stream_types::{
  Compression,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG,
  ProtoDurationExt,
  VirtualPartitionId,
  topic_metadata_window_size,
};
use hostname::get as get_hostname;
use log::{debug, trace};
use protobuf::{Chars, EnumOrUnknown};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::Duration;
use time::ext::NumericalDuration;
use tokio::sync::watch;

const DEFAULT_FLUSH_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_FLUSH_MAX_DELAY: Duration = Duration::seconds(1);
const DEFAULT_LEASE_DURATION: Duration = Duration::seconds(30);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::seconds(10);
const DEFAULT_RESERVATION_SIZE: u64 = 10_000;
const DEFAULT_SEGMENT_TTL_BUFFER: Duration = Duration::hours(1);
const DEFAULT_LEASE_TTL_BUFFER: Duration = Duration::hours(1);
const PRODUCE_REQUEST_TIMEOUT_FLUSH_DELAY_MULTIPLIER: i32 = 10;
pub const FENCED_METADATA_WRITES_FEATURE_FLAG: &str = "blob_stream_broker_fenced_metadata_writes";
pub const MAX_SEGMENT_BYTES_FEATURE_FLAG: &str = "blob_stream_broker_max_segment_bytes";
pub const SHARED_CROSS_TOPIC_BLOBS_FEATURE_FLAG: &str =
  "blob_stream_broker_shared_cross_topic_blobs";

#[cfg(test)]
#[path = "./config_test.rs"]
mod tests;

#[derive(Clone, Copy, Debug)]
enum DynamoTablePurpose {
  SegmentMetadata,
  ProducerPartitionLeases,
}

//
// WriteConfig
//

#[derive(Clone, Debug)]
pub struct WriteConfig {
  pub flush_max_bytes: u64,
  pub max_segment_bytes: u64,
  pub flush_max_delay: Duration,
  pub lease_duration: Duration,
  pub heartbeat_interval: Duration,
  pub reservation_size: u64,
  pub writer_id: u32,
  pub compression: blob_stream_types::Compression,
  pub blob_prefix: Option<String>,
  pub fenced_metadata_writes: bool,
}

impl WriteConfig {
  #[must_use]
  pub fn with_defaults() -> Self {
    Self {
      flush_max_bytes: DEFAULT_FLUSH_MAX_BYTES,
      max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
      flush_max_delay: DEFAULT_FLUSH_MAX_DELAY,
      lease_duration: DEFAULT_LEASE_DURATION,
      heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
      reservation_size: DEFAULT_RESERVATION_SIZE,
      writer_id: 0,
      compression: Compression::zstd(DEFAULT_ZSTD_LEVEL),
      blob_prefix: None,
      fenced_metadata_writes: false,
    }
  }

  pub fn from_broker_config(broker: &BrokerConfig) -> Result<Self> {
    let mut config = Self::with_defaults();

    config.writer_id = broker
      .writer_id
      .ok_or_else(|| anyhow!("broker writer_id must be explicitly configured"))?;

    if broker.flush_max_bytes > 0 {
      config.flush_max_bytes = u64::from(broker.flush_max_bytes);
    }
    if let Some(max_segment_bytes) = broker.max_segment_bytes {
      config.max_segment_bytes = max_segment_bytes;
    }

    if let Some(flush_max_delay) = broker.flush_max_delay.as_ref() {
      config.flush_max_delay = flush_max_delay.to_time_duration();
    }

    config.lease_duration = broker
      .lease_duration
      .as_ref()
      .map_or(DEFAULT_LEASE_DURATION, ProtoDurationExt::to_time_duration);
    config.heartbeat_interval = broker.heartbeat_interval.as_ref().map_or(
      DEFAULT_HEARTBEAT_INTERVAL,
      ProtoDurationExt::to_time_duration,
    );
    ensure!(
      config.heartbeat_interval < config.lease_duration,
      "broker heartbeat_interval must be less than lease_duration"
    );

    if let Some(sequence_reservation_size) = broker.sequence_reservation_size {
      ensure!(
        sequence_reservation_size > 0,
        "broker sequence_reservation_size must be positive"
      );
      config.reservation_size = u64::from(sequence_reservation_size);
    }

    config.fenced_metadata_writes = broker.fenced_metadata_writes;

    let compression = broker.segment_compression.as_ref().map_or(
      SegmentCompression::SEGMENT_COMPRESSION_ZSTD,
      EnumOrUnknown::enum_value_or_default,
    );
    if compression == SegmentCompression::SEGMENT_COMPRESSION_NONE {
      config.compression = Compression::none();
    }

    Ok(config)
  }

  #[must_use]
  pub fn produce_request_timeout(&self) -> StdDuration {
    let flush_delay = self.flush_max_delay.max(Duration::milliseconds(1));
    let timeout = flush_delay
      .checked_mul(PRODUCE_REQUEST_TIMEOUT_FLUSH_DELAY_MULTIPLIER)
      .unwrap_or(Duration::MAX);
    StdDuration::try_from(timeout).unwrap_or(StdDuration::MAX)
  }

  #[must_use]
  pub(crate) fn fenced_metadata_writes(&self, feature_flags: Option<&FeatureFlagsWatch>) -> bool {
    feature_flags.map_or(self.fenced_metadata_writes, |feature_flags| {
      feature_flags.get_bool(
        FENCED_METADATA_WRITES_FEATURE_FLAG,
        self.fenced_metadata_writes,
      )
    })
  }

  #[must_use]
  pub(crate) fn max_segment_bytes(&self, feature_flags: Option<&FeatureFlagsWatch>) -> u64 {
    let value = feature_flags.map_or(self.max_segment_bytes, |feature_flags| {
      feature_flags.get_integer(MAX_SEGMENT_BYTES_FEATURE_FLAG, self.max_segment_bytes)
    });
    if value > 0 {
      return value;
    }

    warn_every!(
      15.seconds(),
      "broker max segment bytes override must be greater than zero; using configured value {}",
      self.max_segment_bytes
    );
    self.max_segment_bytes
  }

  #[must_use]
  pub(crate) fn shared_cross_topic_blobs(feature_flags: Option<&FeatureFlagsWatch>) -> bool {
    feature_flags.is_some_and(|feature_flags| {
      feature_flags.get_bool(SHARED_CROSS_TOPIC_BLOBS_FEATURE_FLAG, false)
    })
  }
}

//
// TopicInfo
//

#[derive(Clone, Debug)]
pub struct TopicInfo {
  pub name: Chars,
  pub partition_count: u32,
  pub num_writers: u32,
  pub retention: Duration,
  pub max_metadata_publication_lag: Duration,
  pub metadata_window_size: Duration,
}

impl TopicInfo {
  pub fn from_proto(proto: &TopicConfig) -> Result<Self> {
    let name = proto.name.clone();

    Ok(Self {
      name,
      partition_count: proto.partition_count,
      num_writers: proto.num_writers,
      retention: proto
        .retention
        .as_ref()
        .ok_or_else(|| anyhow!("topic retention is required"))?
        .to_time_duration(),
      max_metadata_publication_lag: proto.max_metadata_publication_lag.as_ref().map_or(
        DEFAULT_MAX_METADATA_PUBLICATION_LAG,
        ProtoDurationExt::to_time_duration,
      ),
      metadata_window_size: topic_metadata_window_size(proto)?,
    })
  }

  #[must_use]
  pub fn is_valid_partition(&self, virtual_partition_id: VirtualPartitionId) -> bool {
    let max = u64::from(self.partition_count).saturating_mul(u64::from(self.num_writers));
    u64::from(virtual_partition_id) < max
  }
}

//
// RuntimeWriteEngineBuilder
//

/// Builds a write engine from broker runtime configuration and startup-owned dependencies.
pub struct RuntimeWriteEngineBuilder<'a> {
  config: &'a RuntimeConfig,
  metadata_store: Arc<dyn MetadataStore>,
  dynamo_capacity_metrics: DynamoCapacityMetrics,
  blob_store: Option<Arc<dyn BlobStore>>,
  blob_prefix: Option<String>,
  shutdown_trigger_handle: ComponentShutdownTriggerHandle,
  metrics_scope: &'a Scope,
  feature_flags: Option<FeatureFlagsWatch>,
  admission: Option<Arc<dyn AdmissionController>>,
}

impl<'a> RuntimeWriteEngineBuilder<'a> {
  #[must_use]
  pub fn new(
    config: &'a RuntimeConfig,
    metadata_store: Arc<dyn MetadataStore>,
    dynamo_capacity_metrics: DynamoCapacityMetrics,
    shutdown_trigger_handle: ComponentShutdownTriggerHandle,
    metrics_scope: &'a Scope,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Self {
    Self {
      config,
      metadata_store,
      dynamo_capacity_metrics,
      blob_store: None,
      blob_prefix: None,
      shutdown_trigger_handle,
      metrics_scope,
      feature_flags,
      admission: None,
    }
  }

  #[must_use]
  pub fn blob_store(mut self, blob_store: Arc<dyn BlobStore>, blob_prefix: Option<String>) -> Self {
    self.blob_store = Some(blob_store);
    self.blob_prefix = blob_prefix;
    self
  }

  #[must_use]
  pub fn admission(mut self, admission: Arc<dyn AdmissionController>) -> Self {
    self.admission = Some(admission);
    self
  }

  pub async fn build(self) -> Result<Arc<dyn WriteEngine>> {
    let Self {
      config,
      metadata_store,
      dynamo_capacity_metrics,
      blob_store,
      blob_prefix,
      shutdown_trigger_handle,
      metrics_scope,
      feature_flags,
      admission,
    } = self;
    trace!("building broker write engine from runtime config");
    proto_validate::validate(config)?;
    let blob_store = blob_store.ok_or_else(|| anyhow!("broker blob store is required"))?;

    let broker = config
      .broker
      .as_ref()
      .context("runtime config missing broker config")?;
    let mut write_config = WriteConfig::from_broker_config(broker)?;
    let holder_id = resolve_node_id(broker)?;
    let membership_rx = build_membership_watch(broker, &holder_id).await?;

    let topics = build_topics(&config.topics)?;
    validate_writer_id(write_config.writer_id, &topics)?;

    write_config.blob_prefix = blob_prefix;

    let metadata_store_config = config
      .metadata_store
      .as_ref()
      .context("runtime config missing metadata_store config")?;

    let lease_store =
      build_producer_partition_lease_store(metadata_store_config, dynamo_capacity_metrics).await?;
    let topics_count = topics.len();
    let holder_id_for_log = holder_id.clone();
    let writer_id = write_config.writer_id;
    let mut builder = WriteEngineBuilder::new(
      write_config,
      topics,
      blob_store,
      metadata_store,
      lease_store,
      holder_id,
      shutdown_trigger_handle,
      metrics_scope,
    )
    .membership_rx(membership_rx)
    .feature_flags(feature_flags);
    if let Some(admission) = admission {
      builder = builder.admission(admission);
    }
    let engine = builder.build()?;

    debug!(
      "broker write engine built: holder_id={holder_id_for_log}, topics={topics_count}, \
       writer_id={writer_id}"
    );

    Ok(Arc::new(engine))
  }
}

/// Build the metadata store once so broker write and read paths share backend clients and state.
pub async fn build_runtime_metadata_store(
  config: &RuntimeConfig,
  capacity_metrics: DynamoCapacityMetrics,
) -> Result<Arc<dyn MetadataStore>> {
  proto_validate::validate(config)?;
  let metadata_store_config = config
    .metadata_store
    .as_ref()
    .context("runtime config missing metadata_store config")?;
  let topics = build_topics(&config.topics)?;
  build_metadata_store(metadata_store_config, &topics, capacity_metrics).await
}

async fn build_producer_partition_lease_store(
  config: &MetadataStoreConfig,
  capacity_metrics: DynamoCapacityMetrics,
) -> Result<Arc<dyn ProducerPartitionLeaseStore>> {
  if config.has_in_memory() {
    debug!("using in-memory producer partition lease store backend");
    let store: Arc<dyn ProducerPartitionLeaseStore> =
      Arc::new(InMemoryProducerPartitionLeaseStore::new());
    return Ok(store);
  }

  if config.has_dynamo() {
    debug!("using dynamo producer partition lease store backend");
    let dynamo = config.dynamo();
    let table_name =
      dynamo_table_name(dynamo, DynamoTablePurpose::ProducerPartitionLeases).to_string();
    let region = dynamo.region.to_string();

    let region_provider = RegionProviderChain::first_try(Some(Region::new(region)));
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
      .region(region_provider)
      .retry_config(aws_retry_config())
      .timeout_config(aws_timeout_config());
    if !dynamo.endpoint.is_empty() {
      loader = loader.endpoint_url(dynamo.endpoint.to_string());
    }

    let shared = loader.load().await;
    let client = aws_sdk_dynamodb::Client::new(&shared);
    let ttl_buffer = dynamo
      .lease_ttl_buffer
      .as_ref()
      .map_or(DEFAULT_LEASE_TTL_BUFFER, ProtoDurationExt::to_time_duration);
    let store: Arc<dyn ProducerPartitionLeaseStore> =
      Arc::new(DynamoProducerPartitionLeaseStore::new(
        client,
        table_name,
        ttl_buffer,
        Some(capacity_metrics),
      ));
    return Ok(store);
  }

  Err(anyhow!("metadata_store backend not configured"))
}

async fn build_membership_watch(
  broker: &BrokerConfig,
  holder_id: &str,
) -> Result<watch::Receiver<BrokerMembership>> {
  if let Some(discovery) = broker.discovery.as_ref() {
    if discovery.has_static() {
      debug!("using static broker discovery backend");
      let nodes = discovery
        .static_()
        .nodes
        .iter()
        .map(|node| BrokerNode {
          node_id: node.node_id.clone(),
          address: node.address.clone(),
        })
        .collect();
      let discovery = StaticBrokerDiscovery::new(nodes);
      return discovery.watch_membership().await;
    }

    if discovery.has_k8s_service() {
      debug!("using k8s service broker discovery backend");
      let k8s = discovery.k8s_service();
      let discovery =
        K8sServiceBrokerDiscovery::new(k8s.namespace.to_string(), k8s.service_name.to_string());
      return discovery.watch_membership().await;
    }
  }

  let fallback = BrokerMembership::new(vec![BrokerNode {
    node_id: holder_id.to_string().into(),
    address: holder_id.to_string().into(),
  }]);
  debug!("using fallback single-node broker discovery for holder_id={holder_id}");
  let (tx, rx) = watch::channel(fallback);
  drop(tx);
  Ok(rx)
}

fn build_topics(topics: &[TopicConfig]) -> Result<HashMap<Chars, TopicInfo>> {
  trace!(
    "building topic map from {} configured topic(s)",
    topics.len()
  );
  let mut map = HashMap::new();

  for topic in topics {
    let topic_info = TopicInfo::from_proto(topic)?;
    if map.contains_key(&topic_info.name) {
      return Err(anyhow!("duplicate topic: {}", topic_info.name));
    }
    map.insert(topic_info.name.clone(), topic_info);
  }

  Ok(map)
}

fn validate_writer_id(writer_id: u32, topics: &HashMap<Chars, TopicInfo>) -> Result<()> {
  for topic in topics.values() {
    ensure!(
      writer_id < topic.num_writers,
      "broker writer_id {writer_id} must be less than num_writers {} for topic {}",
      topic.num_writers,
      topic.name
    );
  }

  Ok(())
}

fn resolve_node_id(broker: &BrokerConfig) -> Result<String> {
  let identity = broker.node_identity.as_ref();

  if let Some(identity) = identity {
    if identity.has_static_id() {
      let value = identity.static_id().to_string();
      return Ok(value);
    }

    if identity.has_hostname() {
      return hostname_identity();
    }
  }

  hostname_identity()
}

fn hostname_identity() -> Result<String> {
  let hostname = get_hostname().context("resolve hostname")?;
  let hostname = hostname.to_string_lossy().to_string();
  ensure!(!hostname.trim().is_empty(), "hostname is empty");
  Ok(hostname)
}

async fn build_metadata_store(
  config: &MetadataStoreConfig,
  topics: &HashMap<Chars, TopicInfo>,
  capacity_metrics: DynamoCapacityMetrics,
) -> Result<Arc<dyn MetadataStore>> {
  if config.has_in_memory() {
    debug!("using in-memory metadata store backend");
    let store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
    return Ok(store);
  }

  if config.has_dynamo() {
    debug!("using dynamo metadata store backend");
    let dynamo = config.dynamo();
    let table_name = dynamo_table_name(dynamo, DynamoTablePurpose::SegmentMetadata).to_string();
    let producer_partition_lease_table_name =
      dynamo_table_name(dynamo, DynamoTablePurpose::ProducerPartitionLeases).to_string();
    let region = dynamo.region.to_string();

    let region_provider = RegionProviderChain::first_try(Some(Region::new(region)));
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
      .region(region_provider)
      .retry_config(aws_retry_config())
      .timeout_config(aws_timeout_config());
    if !dynamo.endpoint.is_empty() {
      loader = loader.endpoint_url(dynamo.endpoint.to_string());
    }

    let shared = loader.load().await;
    let client = aws_sdk_dynamodb::Client::new(&shared);

    let retention_by_topic = topics
      .iter()
      .map(|(topic, info)| (topic.clone(), info.retention))
      .collect();
    let ttl_buffer = dynamo.segment_ttl_buffer.as_ref().map_or(
      DEFAULT_SEGMENT_TTL_BUFFER,
      ProtoDurationExt::to_time_duration,
    );

    let store: Arc<dyn MetadataStore> =
      Arc::new(blob_stream_metadata_store::DynamoMetadataStore::new(
        client,
        table_name,
        producer_partition_lease_table_name,
        retention_by_topic,
        ttl_buffer,
        Some(capacity_metrics),
      ));
    return Ok(store);
  }

  Err(anyhow!("metadata_store backend not configured"))
}

fn dynamo_table_name(config: &DynamoMetadataStoreConfig, purpose: DynamoTablePurpose) -> &str {
  match purpose {
    DynamoTablePurpose::SegmentMetadata => config.segment_metadata_table_name.as_str(),
    DynamoTablePurpose::ProducerPartitionLeases => {
      config.producer_partition_lease_table_name.as_str()
    },
  }
}
