use crate::write::{WriteEngine, WriteEngineImpl};
use anyhow::{Context, Result, anyhow, ensure};
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_types::region::Region;
use bd_pgv::proto_validate;
use bd_server_stats::stats::Scope;
use blob_stream_blob_store::{BlobStore, InMemoryBlobStore, S3BlobStore};
use blob_stream_broker_discovery::k8s::K8sServiceBrokerDiscovery;
use blob_stream_broker_discovery::r#static::StaticBrokerDiscovery;
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{
  DynamoProducerPartitionLeaseStore,
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  MetadataStore,
  ProducerPartitionLeaseStore,
};
use blob_stream_proto::protos::blobstream::v1::config::{
  BlobStoreConfig,
  BrokerConfig,
  DynamoMetadataStoreConfig,
  MetadataStoreConfig,
  RuntimeConfig,
  SegmentCompression,
  TopicConfig,
};
use blob_stream_types::{Compression, VirtualPartitionId};
use hostname::get as get_hostname;
use log::{debug, trace};
use protobuf::EnumOrUnknown;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

const DEFAULT_FLUSH_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_FLUSH_MAX_DELAY_MS: i64 = 1_000;
const DEFAULT_LEASE_DURATION_MS: i64 = 30_000;
const DEFAULT_RESERVATION_SIZE: u64 = 10_000;
const DEFAULT_WINDOW_SIZE_SECONDS: i64 = 300;
const DEFAULT_SEGMENT_TTL_BUFFER_SECONDS: u32 = 3_600;
const DEFAULT_LEASE_TTL_BUFFER_SECONDS: u32 = 3_600;
const DEFAULT_ZSTD_LEVEL: i32 = 3;

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
  pub flush_max_delay_ms: i64,
  pub lease_duration_ms: i64,
  pub reservation_size: u64,
  pub window_size_seconds: i64,
  pub writer_id: u32,
  pub compression: blob_stream_types::Compression,
  pub blob_prefix: Option<String>,
}

impl WriteConfig {
  #[must_use]
  pub fn with_defaults() -> Self {
    Self {
      flush_max_bytes: DEFAULT_FLUSH_MAX_BYTES,
      flush_max_delay_ms: DEFAULT_FLUSH_MAX_DELAY_MS,
      lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
      reservation_size: DEFAULT_RESERVATION_SIZE,
      window_size_seconds: DEFAULT_WINDOW_SIZE_SECONDS,
      writer_id: 0,
      compression: Compression::zstd(DEFAULT_ZSTD_LEVEL),
      blob_prefix: None,
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

    if broker.flush_max_delay_ms > 0 {
      config.flush_max_delay_ms = i64::from(broker.flush_max_delay_ms);
    }

    if let Some(sequence_reservation_size) = broker.sequence_reservation_size {
      config.reservation_size = u64::from(sequence_reservation_size);
    }

    let compression = broker.segment_compression.as_ref().map_or(
      SegmentCompression::SEGMENT_COMPRESSION_ZSTD,
      EnumOrUnknown::enum_value_or_default,
    );
    if compression == SegmentCompression::SEGMENT_COMPRESSION_NONE {
      config.compression = Compression::none();
    }

    Ok(config)
  }
}

//
// TopicInfo
//

#[derive(Clone, Debug)]
pub struct TopicInfo {
  pub name: String,
  pub partition_count: u32,
  pub num_writers: u32,
  pub retention_days: u32,
}

impl TopicInfo {
  pub fn from_proto(proto: &TopicConfig) -> Result<Self> {
    let name = proto.name.to_string();

    Ok(Self {
      name,
      partition_count: proto.partition_count,
      num_writers: proto.num_writers,
      retention_days: proto.retention_days,
    })
  }

  #[must_use]
  pub fn is_valid_partition(&self, virtual_partition_id: VirtualPartitionId) -> bool {
    let max = u64::from(self.partition_count).saturating_mul(u64::from(self.num_writers));
    u64::from(virtual_partition_id) < max
  }
}

//
// build_write_engine
//

pub async fn build_write_engine(
  config: &RuntimeConfig,
  metrics_scope: &Scope,
) -> Result<Arc<dyn WriteEngine>> {
  trace!("building broker write engine from runtime config");
  proto_validate::validate(config)?;

  let broker = config
    .broker
    .as_ref()
    .context("runtime config missing broker config")?;
  let mut write_config = WriteConfig::from_broker_config(broker)?;
  let holder_id = resolve_node_id(broker)?;
  let membership_rx = build_membership_watch(broker, &holder_id).await?;

  let topics = build_topics(&config.topics)?;
  validate_writer_id(write_config.writer_id, &topics)?;

  let blob_store_config = config
    .blob_store
    .as_ref()
    .context("runtime config missing blob_store config")?;
  let (blob_store, prefix) = build_blob_store(blob_store_config).await?;
  write_config.blob_prefix = prefix;

  let metadata_store_config = config
    .metadata_store
    .as_ref()
    .context("runtime config missing metadata_store config")?;
  let metadata_store = build_metadata_store(metadata_store_config, &topics).await?;

  let lease_store = build_producer_partition_lease_store(metadata_store_config).await?;
  let topics_count = topics.len();
  let holder_id_for_log = holder_id.clone();
  let writer_id = write_config.writer_id;

  let engine = WriteEngineImpl::new(
    write_config,
    topics,
    blob_store,
    metadata_store,
    lease_store,
    holder_id,
    Some(membership_rx),
    metrics_scope,
  )?;

  debug!(
    "broker write engine built: holder_id={holder_id_for_log}, topics={topics_count}, \
     writer_id={writer_id}"
  );

  Ok(Arc::new(engine))
}

async fn build_producer_partition_lease_store(
  config: &MetadataStoreConfig,
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
    let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region_provider);
    if !dynamo.endpoint.is_empty() {
      loader = loader.endpoint_url(dynamo.endpoint.to_string());
    }

    let shared = loader.load().await;
    let client = aws_sdk_dynamodb::Client::new(&shared);
    let ttl_buffer_seconds = dynamo
      .lease_ttl_buffer_seconds
      .unwrap_or(DEFAULT_LEASE_TTL_BUFFER_SECONDS);
    let store: Arc<dyn ProducerPartitionLeaseStore> =
      Arc::new(DynamoProducerPartitionLeaseStore::with_ttl_buffer_seconds(
        client,
        table_name,
        ttl_buffer_seconds,
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
          node_id: node.node_id.to_string(),
          address: node.address.to_string(),
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
    node_id: holder_id.to_string(),
    address: holder_id.to_string(),
  }]);
  debug!("using fallback single-node broker discovery for holder_id={holder_id}");
  let (tx, rx) = watch::channel(fallback);
  drop(tx);
  Ok(rx)
}

fn build_topics(topics: &[TopicConfig]) -> Result<HashMap<String, TopicInfo>> {
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

fn validate_writer_id(writer_id: u32, topics: &HashMap<String, TopicInfo>) -> Result<()> {
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

async fn build_blob_store(
  config: &BlobStoreConfig,
) -> Result<(Arc<dyn BlobStore>, Option<String>)> {
  if config.has_in_memory() {
    debug!("using in-memory blob store backend");
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
    return Ok((store, None));
  }

  if config.has_s3() {
    debug!("using s3 blob store backend");
    let s3 = config.s3();
    let bucket = s3.bucket.to_string();
    let region = s3.region.to_string();

    let region_provider = RegionProviderChain::first_try(Some(Region::new(region)));
    let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region_provider);
    if !s3.endpoint.is_empty() {
      loader = loader.endpoint_url(s3.endpoint.to_string());
    }

    let shared = loader.load().await;
    let client = aws_sdk_s3::Client::new(&shared);
    let store: Arc<dyn BlobStore> = Arc::new(S3BlobStore::new(client, bucket));
    let prefix = if s3.prefix.is_empty() {
      None
    } else {
      Some(s3.prefix.to_string())
    };

    return Ok((store, prefix));
  }

  Err(anyhow!("blob_store backend not configured"))
}

async fn build_metadata_store(
  config: &MetadataStoreConfig,
  topics: &HashMap<String, TopicInfo>,
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
    let region = dynamo.region.to_string();

    let region_provider = RegionProviderChain::first_try(Some(Region::new(region)));
    let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region_provider);
    if !dynamo.endpoint.is_empty() {
      loader = loader.endpoint_url(dynamo.endpoint.to_string());
    }

    let shared = loader.load().await;
    let client = aws_sdk_dynamodb::Client::new(&shared);

    let retention_days_by_topic = topics
      .iter()
      .map(|(topic, info)| (topic.clone(), info.retention_days))
      .collect();
    let ttl_buffer_seconds = dynamo
      .segment_ttl_buffer_seconds
      .unwrap_or(DEFAULT_SEGMENT_TTL_BUFFER_SECONDS);

    let store: Arc<dyn MetadataStore> = Arc::new(
      blob_stream_metadata_store::DynamoMetadataStore::with_segment_ttl(
        client,
        table_name,
        retention_days_by_topic,
        ttl_buffer_seconds,
      ),
    );
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
