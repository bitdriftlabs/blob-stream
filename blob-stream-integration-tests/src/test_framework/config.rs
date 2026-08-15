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
use blob_stream_types::ToProtoDuration;
use time::Duration;

pub fn producer_config() -> ProducerConfig {
  producer_config_with_writer_id(0)
}

pub fn producer_config_with_writer_id(writer_id: u32) -> ProducerConfig {
  // Use tight batching/retry defaults so integration tests converge quickly.
  let mut config = ProducerConfig::new();
  config.writer_id = Some(writer_id);
  config.max_batch_records = Some(1);
  config.max_batch_bytes = Some(1_024);
  config.flush_max_delay = Duration::milliseconds(5).into_proto();
  config.retry_base_delay = Duration::milliseconds(10).into_proto();
  config.retry_max_delay = Duration::milliseconds(50).into_proto();
  config.retry_deadline = Duration::seconds(2).into_proto();
  config.connect_timeout = Duration::milliseconds(100).into_proto();
  config.request_timeout = Duration::seconds(2).into_proto();
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
  producer_topic_named_with_partition_count(topic, PARTITION_COUNT, num_writers)
}

pub fn producer_topic_named_with_partition_count(
  topic: &str,
  partition_count: u32,
  num_writers: u32,
) -> ProducerTopicConfig {
  ProducerTopicConfig {
    name: topic.to_string().into(),
    partition_count,
    num_writers,
    retention: Duration::days(1).into_proto(),
    ..Default::default()
  }
}

pub fn consumer_runtime_config(member_id: &str) -> ConsumerRuntimeConfig {
  // Keep lease and rebalance intervals short to make ownership transitions observable in tests.
  let mut read = ConsumerReadConfig::new();
  read.topic = TOPIC.to_string().into();
  read.window_size = Duration::seconds(WINDOW_SIZE_SECONDS).into_proto();

  let mut group = ConsumerGroupConfig::new();
  group.topic = TOPIC.to_string().into();
  group.group_id = "integration-group".to_string().into();
  group.member_id = member_id.to_string().into();
  group.lease_duration = Duration::seconds(2).into_proto();
  group.heartbeat_interval = Duration::milliseconds(200).into_proto();
  group.rebalance_interval = Duration::milliseconds(200).into_proto();

  let mut runtime = ConsumerRuntimeConfig::new();
  runtime.read = Some(read).into();
  runtime.group = Some(group).into();
  runtime
}

pub fn consumer_bootstrap_config(
  member_id: &str,
  resources: &IntegrationResources,
) -> ConsumerIteratorBootstrapConfig {
  consumer_bootstrap_config_for(
    TOPIC,
    PARTITION_COUNT,
    "integration-group",
    member_id,
    resources,
  )
}

pub fn consumer_bootstrap_config_for(
  topic_name: &str,
  partition_count: u32,
  group_id: &str,
  member_id: &str,
  resources: &IntegrationResources,
) -> ConsumerIteratorBootstrapConfig {
  // Keep lease and rebalance intervals short so local runs converge quickly.
  let mut read = ConsumerReadConfig::new();
  read.topic = topic_name.to_string().into();
  read.window_size = Duration::seconds(WINDOW_SIZE_SECONDS).into_proto();

  let mut group = ConsumerGroupConfig::new();
  group.topic = topic_name.to_string().into();
  group.group_id = group_id.to_string().into();
  group.member_id = member_id.to_string().into();
  group.lease_duration = Duration::seconds(2).into_proto();
  group.heartbeat_interval = Duration::milliseconds(200).into_proto();
  group.rebalance_interval = Duration::milliseconds(200).into_proto();

  let mut runtime = ConsumerRuntimeConfig::new();
  runtime.read = Some(read).into();
  runtime.group = Some(group).into();

  let mut topic = TopicConfig::new();
  topic.name = topic_name.to_string().into();
  topic.partition_count = partition_count;
  topic.num_writers = 1;
  topic.retention = Duration::days(1).into_proto();

  let mut s3 = S3BlobStoreConfig::new();
  s3.bucket = resources.bucket_name().to_string().into();
  s3.region = resources.aws_region().to_string().into();
  s3.endpoint = resources.s3_endpoint().into();
  let mut blob_store = BlobStoreConfig::new();
  blob_store.set_s3(s3);

  let mut dynamo = DynamoMetadataStoreConfig::new();
  dynamo.region = resources.aws_region().to_string().into();
  dynamo.endpoint = resources.dynamo_endpoint().into();
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
