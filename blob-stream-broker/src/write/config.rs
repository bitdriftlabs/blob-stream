// blob-stream - broker write path config
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::write::{WriteEngine, WriteEngineImpl};
use anyhow::{Context, Result, anyhow, ensure};
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_types::region::Region;
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
  MetadataStoreConfig,
  RuntimeConfig,
  TopicConfig,
};
use blob_stream_types::VirtualPartitionId;
use hostname::get as get_hostname;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

const DEFAULT_FLUSH_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_FLUSH_MAX_DELAY_MS: i64 = 1_000;
const DEFAULT_LEASE_DURATION_MS: i64 = 30_000;
const DEFAULT_RESERVATION_SIZE: u64 = 1_000;
const DEFAULT_WINDOW_SIZE_SECONDS: i64 = 300;

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
      compression: blob_stream_types::Compression::none(),
      blob_prefix: None,
    }
  }

  pub fn from_broker_config(broker: &BrokerConfig) -> Result<Self> {
    let mut config = Self::with_defaults();

    if broker.flush_max_bytes > 0 {
      config.flush_max_bytes = u64::from(broker.flush_max_bytes);
    }

    if broker.flush_max_delay_ms > 0 {
      config.flush_max_delay_ms = i64::from(broker.flush_max_delay_ms);
    }

    ensure!(
      config.flush_max_bytes > 0,
      "broker.flush_max_bytes must be greater than zero"
    );
    ensure!(
      config.flush_max_delay_ms > 0,
      "broker.flush_max_delay_ms must be greater than zero"
    );

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
}

impl TopicInfo {
  pub fn from_proto(proto: &TopicConfig) -> Result<Self> {
    let name = proto.name.to_string();
    ensure!(!name.trim().is_empty(), "topic name is required");
    ensure!(
      proto.partition_count > 0,
      "partition_count must be greater than zero"
    );
    ensure!(
      proto.num_writers > 0,
      "num_writers must be greater than zero"
    );

    Ok(Self {
      name,
      partition_count: proto.partition_count,
      num_writers: proto.num_writers,
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

pub async fn build_write_engine(config: &RuntimeConfig) -> Result<Arc<dyn WriteEngine>> {
  let broker = config
    .broker
    .as_ref()
    .context("runtime config missing broker config")?;
  let mut write_config = WriteConfig::from_broker_config(broker)?;
  let holder_id = resolve_node_id(broker)?;
  let membership_rx = build_membership_watch(broker, &holder_id).await?;

  let topics = build_topics(&config.topics)?;

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
  let metadata_store = build_metadata_store(metadata_store_config).await?;

  let lease_store = build_producer_partition_lease_store(metadata_store_config).await?;

  let engine = WriteEngineImpl::new(
    write_config,
    topics,
    blob_store,
    metadata_store,
    lease_store,
    holder_id,
    Some(membership_rx),
  )?;

  Ok(Arc::new(engine))
}

async fn build_producer_partition_lease_store(
  config: &MetadataStoreConfig,
) -> Result<Arc<dyn ProducerPartitionLeaseStore>> {
  if config.has_in_memory() {
    let store: Arc<dyn ProducerPartitionLeaseStore> =
      Arc::new(InMemoryProducerPartitionLeaseStore::new());
    return Ok(store);
  }

  if config.has_dynamo() {
    let dynamo = config.dynamo();
    let table_name = dynamo.table_name.to_string();
    let region = dynamo.region.to_string();
    ensure!(
      !table_name.is_empty(),
      "metadata_store.dynamo.table_name is required"
    );
    ensure!(
      !region.is_empty(),
      "metadata_store.dynamo.region is required"
    );

    let region_provider = RegionProviderChain::first_try(Some(Region::new(region)));
    let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region_provider);
    if !dynamo.endpoint.is_empty() {
      loader = loader.endpoint_url(dynamo.endpoint.to_string());
    }

    let shared = loader.load().await;
    let client = aws_sdk_dynamodb::Client::new(&shared);
    let store: Arc<dyn ProducerPartitionLeaseStore> =
      Arc::new(DynamoProducerPartitionLeaseStore::new(client, table_name));
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
      let k8s = discovery.k8s_service();
      ensure!(
        !k8s.namespace.is_empty(),
        "broker.discovery.k8s_service.namespace is required"
      );
      ensure!(
        !k8s.service_name.is_empty(),
        "broker.discovery.k8s_service.service_name is required"
      );
      let discovery =
        K8sServiceBrokerDiscovery::new(k8s.namespace.to_string(), k8s.service_name.to_string());
      return discovery.watch_membership().await;
    }
  }

  let fallback = BrokerMembership::new(vec![BrokerNode {
    node_id: holder_id.to_string(),
    address: holder_id.to_string(),
  }]);
  let (tx, rx) = watch::channel(fallback);
  drop(tx);
  Ok(rx)
}

fn build_topics(topics: &[TopicConfig]) -> Result<HashMap<String, TopicInfo>> {
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

fn resolve_node_id(broker: &BrokerConfig) -> Result<String> {
  let identity = broker.node_identity.as_ref();

  if let Some(identity) = identity {
    if identity.has_static_id() {
      let value = identity.static_id().to_string();
      ensure!(
        !value.trim().is_empty(),
        "broker.node_identity.static_id is empty"
      );
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
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
    return Ok((store, None));
  }

  if config.has_s3() {
    let s3 = config.s3();
    let bucket = s3.bucket.to_string();
    let region = s3.region.to_string();
    ensure!(!bucket.is_empty(), "blob_store.s3.bucket is required");
    ensure!(!region.is_empty(), "blob_store.s3.region is required");

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

async fn build_metadata_store(config: &MetadataStoreConfig) -> Result<Arc<dyn MetadataStore>> {
  if config.has_in_memory() {
    let store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
    return Ok(store);
  }

  if config.has_dynamo() {
    let dynamo = config.dynamo();
    let table_name = dynamo.table_name.to_string();
    let region = dynamo.region.to_string();
    ensure!(
      !table_name.is_empty(),
      "metadata_store.dynamo.table_name is required"
    );
    ensure!(
      !region.is_empty(),
      "metadata_store.dynamo.region is required"
    );

    let region_provider = RegionProviderChain::first_try(Some(Region::new(region)));
    let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region_provider);
    if !dynamo.endpoint.is_empty() {
      loader = loader.endpoint_url(dynamo.endpoint.to_string());
    }

    let shared = loader.load().await;
    let client = aws_sdk_dynamodb::Client::new(&shared);

    let store: Arc<dyn MetadataStore> = Arc::new(
      blob_stream_metadata_store::DynamoMetadataStore::new(client, table_name),
    );
    return Ok(store);
  }

  Err(anyhow!("metadata_store backend not configured"))
}
