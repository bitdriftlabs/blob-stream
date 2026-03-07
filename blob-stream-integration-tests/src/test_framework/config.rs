use crate::test_framework::{PARTITION_COUNT, TOPIC, WINDOW_SIZE_SECONDS};
use blob_stream_consumer::{ConsumerGroupConfig, ConsumerReadConfig, ConsumerRuntimeConfig};
use blob_stream_producer::{ProducerCompression, ProducerConfig, ProducerTopicConfig};

pub fn producer_config(max_retries: u32) -> ProducerConfig {
  producer_config_with_writer_id(max_retries, 0)
}

pub fn producer_config_with_writer_id(max_retries: u32, writer_id: u32) -> ProducerConfig {
  // Use tight batching/retry defaults so integration tests converge quickly.
  let mut config = ProducerConfig::new();
  config.writer_id = Some(writer_id);
  config.max_batch_records = Some(1);
  config.max_batch_bytes = Some(1_024);
  config.flush_max_delay_ms = Some(5);
  config.max_retries = Some(max_retries);
  config.retry_base_delay_ms = Some(10);
  config.retry_max_delay_ms = Some(50);
  config.connect_timeout_ms = Some(100);
  config.request_timeout_ms = Some(2_000);
  config.max_request_concurrency = Some(32);
  config.compression = Some(ProducerCompression::PRODUCER_COMPRESSION_NONE.into());
  config
}

pub fn producer_topic() -> ProducerTopicConfig {
  producer_topic_named_with_writers(TOPIC, 1)
}

pub fn producer_topic_named(topic: &str) -> ProducerTopicConfig {
  producer_topic_named_with_writers(topic, 1)
}

pub fn producer_topic_named_with_writers(topic: &str, num_writers: u32) -> ProducerTopicConfig {
  ProducerTopicConfig {
    name: topic.to_string().into(),
    partition_count: PARTITION_COUNT,
    num_writers,
    retention_days: 0,
    ..Default::default()
  }
}

pub fn consumer_runtime_config(member_id: &str) -> ConsumerRuntimeConfig {
  // Keep lease and rebalance intervals short to make ownership transitions observable in tests.
  let mut read = ConsumerReadConfig::new();
  read.topic = TOPIC.to_string().into();
  read.window_size_seconds = Some(WINDOW_SIZE_SECONDS);
  read.lookback_windows = Some(10);

  let mut group = ConsumerGroupConfig::new();
  group.topic = TOPIC.to_string().into();
  group.group_id = "integration-group".to_string().into();
  group.member_id = member_id.to_string().into();
  group.lease_duration_ms = Some(2_000);
  group.heartbeat_interval_ms = Some(200);
  group.rebalance_interval_ms = Some(200);

  let mut runtime = ConsumerRuntimeConfig::new();
  runtime.read = Some(read).into();
  runtime.group = Some(group).into();
  runtime
}
