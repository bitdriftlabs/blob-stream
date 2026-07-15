use super::resources::IntegrationResources;
use crate::test_framework::{PARTITION_COUNT, TOPIC, WINDOW_SIZE_SECONDS};
use blob_stream_consumer::{ConsumerGroupConfig, ConsumerReadConfig, ConsumerRuntimeConfig};
use blob_stream_producer::{ProducerCompression, ProducerConfig, ProducerTopicConfig};
use blob_stream_proto::protos::blobstream::v1::config::{
  BlobStoreConfig,
  ConsumerIteratorBootstrapConfig,
  DynamoMetadataStoreConfig,
  MetadataStoreConfig,
  S3BlobStoreConfig,
  TopicConfig,
};

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

pub fn consumer_bootstrap_config(
  member_id: &str,
  resources: &IntegrationResources,
) -> ConsumerIteratorBootstrapConfig {
  let runtime = consumer_runtime_config(member_id);

  let mut topic = TopicConfig::new();
  topic.name = TOPIC.to_string().into();
  topic.partition_count = PARTITION_COUNT;
  topic.num_writers = 1;
  topic.retention_days = 1;

  let mut s3 = S3BlobStoreConfig::new();
  s3.bucket = resources.bucket_name().to_string().into();
  s3.region = resources.aws_region().to_string().into();
  s3.endpoint = resources.s3_endpoint().to_string().into();
  let mut blob_store = BlobStoreConfig::new();
  blob_store.set_s3(s3);

  let mut dynamo = DynamoMetadataStoreConfig::new();
  dynamo.region = resources.aws_region().to_string().into();
  dynamo.endpoint = resources.dynamo_endpoint().to_string().into();
  dynamo.segment_metadata_table_name = resources.segment_metadata_table_name().to_string().into();
  dynamo.producer_partition_lease_table_name =
    resources.producer_lease_table_name().to_string().into();
  dynamo.consumer_group_lease_table_name = resources.consumer_lease_table_name().to_string().into();
  dynamo.consumer_group_membership_table_name = resources
    .consumer_membership_table_name()
    .to_string()
    .into();

  let mut metadata_store = MetadataStoreConfig::new();
  metadata_store.set_dynamo(dynamo);

  ConsumerIteratorBootstrapConfig {
    runtime: Some(runtime).into(),
    topic: Some(topic).into(),
    blob_store: Some(blob_store).into(),
    metadata_store: Some(metadata_store).into(),
    ..Default::default()
  }
}
