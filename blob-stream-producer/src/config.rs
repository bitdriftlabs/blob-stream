#[cfg(test)]
#[path = "./config_test.rs"]
mod tests;

use anyhow::{Result, anyhow, ensure};
use bd_grpc::compression::Compression;
use bd_pgv::proto_validate;
use bd_runtime_config::feature_flags::{FeatureFlags, FeatureFlagsWatch};
use blob_stream_broker_discovery::{BrokerDiscovery, discovery_from_config};
pub use blob_stream_proto::protos::blobstream::v1::config::{
  BrokerDiscoveryConfig,
  BrokerNode as ProducerNodeConfig,
  ProducerCompression,
  ProducerConfig,
  ProducerRuntimeConfig,
  TopicConfig,
};
use blob_stream_runtime_config::feature_flag_duration_milliseconds;
#[cfg(test)]
use blob_stream_types::ToProtoDuration;
use blob_stream_types::{ProtoDurationExt, topic_metadata_window_size, virtual_partition_count};
use log::{debug, info, trace};
use std::collections::HashSet;
use std::sync::Arc;
use time::Duration;

const DEFAULT_MAX_BATCH_RECORDS: u32 = 10_000;
const DEFAULT_MAX_BATCH_BYTES: u32 = 1_048_576;
const DEFAULT_FLUSH_MAX_DELAY: Duration = Duration::milliseconds(200);
const DEFAULT_RETRY_BASE_DELAY: Duration = Duration::milliseconds(25);
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::seconds(1);
const DEFAULT_RETRY_DEADLINE: Duration = Duration::seconds(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::seconds(2);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::seconds(5);
const DEFAULT_MAX_REQUEST_CONCURRENCY: u64 = 64;
const MAX_BATCH_RECORDS_FEATURE_FLAG: &str = "blob_stream_producer_max_batch_records";
const MAX_BATCH_BYTES_FEATURE_FLAG: &str = "blob_stream_producer_max_batch_bytes";
const FLUSH_MAX_DELAY_FEATURE_FLAG: &str = "blob_stream_producer_flush_max_delay_ms";
const RETRY_BASE_DELAY_FEATURE_FLAG: &str = "blob_stream_producer_retry_base_delay_ms";
const RETRY_MAX_DELAY_FEATURE_FLAG: &str = "blob_stream_producer_retry_max_delay_ms";
const CONNECT_TIMEOUT_FEATURE_FLAG: &str = "blob_stream_producer_connect_timeout_ms";
const REQUEST_TIMEOUT_FEATURE_FLAG: &str = "blob_stream_producer_request_timeout_ms";
const MAX_REQUEST_CONCURRENCY_FEATURE_FLAG: &str = "blob_stream_producer_max_request_concurrency";
const COMPRESSION_FEATURE_FLAG: &str = "blob_stream_producer_compression";

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
  config.flush_max_delay = DEFAULT_FLUSH_MAX_DELAY.into_proto();
  config.retry_base_delay = DEFAULT_RETRY_BASE_DELAY.into_proto();
  config.retry_max_delay = DEFAULT_RETRY_MAX_DELAY.into_proto();
  config.retry_deadline = DEFAULT_RETRY_DEADLINE.into_proto();
  config.connect_timeout = DEFAULT_CONNECT_TIMEOUT.into_proto();
  config.request_timeout = DEFAULT_REQUEST_TIMEOUT.into_proto();
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
pub fn producer_flush_max_delay(config: &ProducerConfig) -> Duration {
  config
    .flush_max_delay
    .as_ref()
    .map_or(DEFAULT_FLUSH_MAX_DELAY, ProtoDurationExt::to_time_duration)
}

#[must_use]
pub fn producer_retry_base_delay(config: &ProducerConfig) -> Duration {
  config
    .retry_base_delay
    .as_ref()
    .map_or(DEFAULT_RETRY_BASE_DELAY, ProtoDurationExt::to_time_duration)
}

#[must_use]
pub fn producer_retry_max_delay(config: &ProducerConfig) -> Duration {
  config
    .retry_max_delay
    .as_ref()
    .map_or(DEFAULT_RETRY_MAX_DELAY, ProtoDurationExt::to_time_duration)
}

#[must_use]
pub fn producer_retry_deadline(config: &ProducerConfig) -> Duration {
  config
    .retry_deadline
    .as_ref()
    .map_or(DEFAULT_RETRY_DEADLINE, ProtoDurationExt::to_time_duration)
}

#[must_use]
pub fn producer_connect_timeout(config: &ProducerConfig) -> Duration {
  config
    .connect_timeout
    .as_ref()
    .map_or(DEFAULT_CONNECT_TIMEOUT, ProtoDurationExt::to_time_duration)
}

#[must_use]
pub fn producer_request_timeout(config: &ProducerConfig) -> Duration {
  config
    .request_timeout
    .as_ref()
    .map_or(DEFAULT_REQUEST_TIMEOUT, ProtoDurationExt::to_time_duration)
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

/// Apply startup-only feature flags that affect this producer process's local batching and retry.
pub fn apply_producer_startup_overrides(
  feature_flags: &FeatureFlagsWatch,
  runtime: &mut ProducerRuntimeConfig,
) -> Result<()> {
  let producer = runtime
    .producer
    .as_mut()
    .ok_or_else(|| anyhow!("producer config is required"))?;

  producer.max_batch_records = Some(flag_u32(
    feature_flags,
    MAX_BATCH_RECORDS_FEATURE_FLAG,
    producer_max_batch_records(producer),
  )?);
  producer.max_batch_bytes = Some(flag_u32(
    feature_flags,
    MAX_BATCH_BYTES_FEATURE_FLAG,
    producer_max_batch_bytes(producer),
  )?);
  producer.flush_max_delay = feature_flag_duration_milliseconds(
    feature_flags,
    FLUSH_MAX_DELAY_FEATURE_FLAG,
    producer_flush_max_delay(producer),
  )?;
  producer.retry_base_delay = feature_flag_duration_milliseconds(
    feature_flags,
    RETRY_BASE_DELAY_FEATURE_FLAG,
    producer_retry_base_delay(producer),
  )?;
  producer.retry_max_delay = feature_flag_duration_milliseconds(
    feature_flags,
    RETRY_MAX_DELAY_FEATURE_FLAG,
    producer_retry_max_delay(producer),
  )?;
  producer.connect_timeout = feature_flag_duration_milliseconds(
    feature_flags,
    CONNECT_TIMEOUT_FEATURE_FLAG,
    producer_connect_timeout(producer),
  )?;
  producer.request_timeout = feature_flag_duration_milliseconds(
    feature_flags,
    REQUEST_TIMEOUT_FEATURE_FLAG,
    producer_request_timeout(producer),
  )?;
  producer.max_request_concurrency = Some(flag_u64(
    feature_flags,
    MAX_REQUEST_CONCURRENCY_FEATURE_FLAG,
    producer_max_request_concurrency(producer),
  ));
  let compression_name = feature_flags.get_string(
    COMPRESSION_FEATURE_FLAG,
    &Arc::new(current_compression_name(producer_compression(producer))),
  );
  producer.compression = Some(parse_compression(compression_name.as_ref())?);
  info!("blob-stream producer runtime config after applying feature flag overrides: {runtime}");
  Ok(())
}

fn current_compression_name(compression: ProducerCompression) -> String {
  match compression {
    ProducerCompression::PRODUCER_COMPRESSION_NONE => "none".to_string(),
    ProducerCompression::PRODUCER_COMPRESSION_SNAPPY => "snappy".to_string(),
  }
}

fn parse_compression(value: &str) -> Result<protobuf::EnumOrUnknown<ProducerCompression>> {
  match value.to_ascii_lowercase().as_str() {
    "none" => Ok(ProducerCompression::PRODUCER_COMPRESSION_NONE.into()),
    "snappy" => Ok(ProducerCompression::PRODUCER_COMPRESSION_SNAPPY.into()),
    other => Err(anyhow!("invalid blob-stream compression override: {other}")),
  }
}

fn flag_u64(feature_flags: &FeatureFlagsWatch, name: &str, default: u64) -> u64 {
  feature_flags.get_integer(name, default)
}

fn flag_u32(feature_flags: &FeatureFlagsWatch, name: &str, default: u32) -> Result<u32> {
  u32::try_from(feature_flags.get_integer(name, u64::from(default)))
    .map_err(|_| anyhow!("feature flag {name} exceeds u32"))
}

pub fn validate_producer_config(config: &ProducerConfig) -> Result<()> {
  trace!("validating producer config");
  proto_validate::validate(config)?;
  ensure!(
    producer_retry_base_delay(config) <= producer_retry_max_delay(config),
    "producer retry_base_delay must not exceed retry_max_delay"
  );
  Ok(())
}

pub fn validate_topic_config(topic: &ProducerTopicConfig) -> Result<()> {
  trace!(
    "validating producer topic config: topic={name}",
    name = topic.name
  );
  proto_validate::validate(topic)?;
  topic_metadata_window_size(topic)?;
  virtual_partition_count(topic.partition_count, topic.num_writers)
    .map_err(|error| anyhow!("topic {}: {error}", topic.name))?;
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
  debug!("constructing producer broker discovery backend");
  discovery_from_config(discovery)
}
