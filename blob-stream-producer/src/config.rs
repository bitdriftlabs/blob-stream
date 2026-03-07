use anyhow::{Result, anyhow, ensure};
use bd_grpc::compression::Compression;
use bd_pgv::proto_validate;
use blob_stream_broker_discovery::k8s::K8sServiceBrokerDiscovery;
use blob_stream_broker_discovery::r#static::StaticBrokerDiscovery;
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerNode};
pub use blob_stream_proto::protos::blobstream::v1::config::{
  BrokerDiscoveryConfig,
  BrokerNode as ProducerNodeConfig,
  ProducerCompression,
  ProducerConfig,
  ProducerRuntimeConfig,
  TopicConfig,
};
use log::{debug, trace};
use std::collections::HashSet;
use std::sync::Arc;

const DEFAULT_MAX_BATCH_RECORDS: u32 = 1_000;
const DEFAULT_MAX_BATCH_BYTES: u32 = 1_048_576;
const DEFAULT_FLUSH_MAX_DELAY_MS: u64 = 200;
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_RETRY_BASE_DELAY_MS: u64 = 25;
const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 1_000;
const DEFAULT_CONNECT_TIMEOUT_MS: i64 = 2_000;
const DEFAULT_REQUEST_TIMEOUT_MS: i64 = 5_000;
const DEFAULT_MAX_REQUEST_CONCURRENCY: u64 = 64;

/// Topic configuration message used by the producer runtime config.
pub type ProducerTopicConfig = TopicConfig;
/// Broker discovery configuration used by the producer runtime config.
pub type ProducerDiscoveryConfig = BrokerDiscoveryConfig;

#[must_use]
#[cfg(test)]
pub fn producer_config_with_defaults() -> ProducerConfig {
  let mut config = ProducerConfig::new();
  config.writer_id = Some(0);
  config.max_batch_records = Some(DEFAULT_MAX_BATCH_RECORDS);
  config.max_batch_bytes = Some(DEFAULT_MAX_BATCH_BYTES);
  config.flush_max_delay_ms = Some(DEFAULT_FLUSH_MAX_DELAY_MS);
  config.max_retries = Some(DEFAULT_MAX_RETRIES);
  config.retry_base_delay_ms = Some(DEFAULT_RETRY_BASE_DELAY_MS);
  config.retry_max_delay_ms = Some(DEFAULT_RETRY_MAX_DELAY_MS);
  config.connect_timeout_ms = Some(DEFAULT_CONNECT_TIMEOUT_MS);
  config.request_timeout_ms = Some(DEFAULT_REQUEST_TIMEOUT_MS);
  config.max_request_concurrency = Some(DEFAULT_MAX_REQUEST_CONCURRENCY);
  config.compression = Some(ProducerCompression::PRODUCER_COMPRESSION_NONE.into());
  config
}

#[must_use]
pub fn producer_writer_id(config: &ProducerConfig) -> u32 {
  config.writer_id.unwrap_or(0)
}

#[must_use]
pub fn producer_max_batch_records(config: &ProducerConfig) -> u32 {
  config
    .max_batch_records
    .unwrap_or(DEFAULT_MAX_BATCH_RECORDS)
}

#[must_use]
pub fn producer_max_batch_bytes(config: &ProducerConfig) -> u32 {
  config.max_batch_bytes.unwrap_or(DEFAULT_MAX_BATCH_BYTES)
}

#[must_use]
pub fn producer_flush_max_delay_ms(config: &ProducerConfig) -> u64 {
  config
    .flush_max_delay_ms
    .unwrap_or(DEFAULT_FLUSH_MAX_DELAY_MS)
}

#[must_use]
pub fn producer_max_retries(config: &ProducerConfig) -> u32 {
  config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
}

#[must_use]
pub fn producer_retry_base_delay_ms(config: &ProducerConfig) -> u64 {
  config
    .retry_base_delay_ms
    .unwrap_or(DEFAULT_RETRY_BASE_DELAY_MS)
}

#[must_use]
pub fn producer_retry_max_delay_ms(config: &ProducerConfig) -> u64 {
  config
    .retry_max_delay_ms
    .unwrap_or(DEFAULT_RETRY_MAX_DELAY_MS)
}

#[must_use]
pub fn producer_connect_timeout_ms(config: &ProducerConfig) -> i64 {
  config
    .connect_timeout_ms
    .unwrap_or(DEFAULT_CONNECT_TIMEOUT_MS)
}

#[must_use]
pub fn producer_request_timeout_ms(config: &ProducerConfig) -> i64 {
  config
    .request_timeout_ms
    .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS)
}

#[must_use]
pub fn producer_max_request_concurrency(config: &ProducerConfig) -> u64 {
  config
    .max_request_concurrency
    .unwrap_or(DEFAULT_MAX_REQUEST_CONCURRENCY)
}

#[must_use]
#[allow(clippy::redundant_closure_for_method_calls)]
pub fn producer_compression(config: &ProducerConfig) -> ProducerCompression {
  config.compression.as_ref().map_or(
    ProducerCompression::PRODUCER_COMPRESSION_NONE,
    |compression| compression.enum_value_or_default(),
  )
}

pub fn compression_as_grpc(compression: ProducerCompression) -> Compression {
  match compression {
    ProducerCompression::PRODUCER_COMPRESSION_NONE => Compression::None,
    ProducerCompression::PRODUCER_COMPRESSION_SNAPPY => Compression::Snappy,
  }
}

pub fn validate_producer_config(config: &ProducerConfig) -> Result<()> {
  trace!("validating producer config");
  proto_validate::validate(config)?;
  Ok(())
}

pub fn validate_topic_config(topic: &ProducerTopicConfig) -> Result<()> {
  trace!("validating producer topic config: topic={}", topic.name);
  proto_validate::validate(topic)?;
  Ok(())
}

pub fn validate_runtime_config(runtime: &ProducerRuntimeConfig) -> Result<()> {
  debug!(
    "validating producer runtime config: topics={}",
    runtime.topics.len()
  );
  proto_validate::validate(runtime)?;

  let producer = runtime
    .producer
    .as_ref()
    .ok_or_else(|| anyhow!("producer config is required"))?;

  let mut names = HashSet::new();
  let writer_id = producer_writer_id(producer);
  for topic in &runtime.topics {
    validate_topic_config(topic)?;
    ensure!(
      writer_id < topic.num_writers,
      "writer_id {} must be less than num_writers {} for topic {}",
      writer_id,
      topic.num_writers,
      topic.name
    );
    ensure!(
      names.insert(topic.name.to_string()),
      "duplicate topic config: {}",
      topic.name
    );
  }

  let discovery = runtime
    .discovery
    .as_ref()
    .ok_or_else(|| anyhow!("producer discovery config is required"))?;
  validate_discovery_config(discovery)
}

pub fn validate_discovery_config(discovery: &ProducerDiscoveryConfig) -> Result<()> {
  trace!("validating producer discovery config");
  proto_validate::validate(discovery)?;
  Ok(())
}

pub fn into_discovery(discovery: &ProducerDiscoveryConfig) -> Result<Arc<dyn BrokerDiscovery>> {
  if discovery.has_static() {
    debug!("constructing static producer discovery backend");
    let nodes = discovery
      .static_()
      .nodes
      .iter()
      .map(|node| BrokerNode {
        node_id: node.node_id.to_string(),
        address: node.address.to_string(),
      })
      .collect();
    return Ok(Arc::new(StaticBrokerDiscovery::new(nodes)));
  }

  if discovery.has_k8s_service() {
    debug!("constructing k8s producer discovery backend");
    let k8s = discovery.k8s_service();
    return Ok(Arc::new(K8sServiceBrokerDiscovery::new(
      k8s.namespace.to_string(),
      k8s.service_name.to_string(),
    )));
  }

  Err(anyhow!(
    "producer discovery backend is required (static or k8s_service)"
  ))
}
