#![allow(clippy::unwrap_used)]

use crate::bootstrap::ConsumerBootstrapConfig;
use crate::{
  ConsumerConfigFactory,
  ConsumerGroupConfig,
  ConsumerIterator,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
};
use blob_stream_proto::protos::blobstream::v1::config::{
  BlobStoreConfig,
  ConsumerIteratorBootstrapConfig,
  InMemoryBlobStoreConfig,
  InMemoryMetadataStoreConfig,
  MetadataStoreConfig,
  TopicConfig,
  blob_store_config,
  metadata_store_config,
};

fn runtime(member_id: &str) -> ConsumerRuntimeConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".into();
  read.window_size_seconds = Some(300);
  read.lookback_windows = Some(3);

  let mut group = ConsumerGroupConfig::new();
  group.topic = "telemetry".into();
  group.group_id = "group-a".into();
  group.member_id = member_id.to_string().into();
  group.lease_duration_ms = Some(30_000);
  group.heartbeat_interval_ms = Some(10_000);
  group.rebalance_interval_ms = Some(10_000);

  let mut runtime = ConsumerRuntimeConfig::new();
  runtime.read = Some(read).into();
  runtime.group = Some(group).into();
  runtime
}

fn topic() -> TopicConfig {
  let mut topic = TopicConfig::new();
  topic.name = "telemetry".into();
  topic.partition_count = 8;
  topic.num_writers = 2;
  topic.retention_days = 7;
  topic
}

fn in_memory_blob_store() -> BlobStoreConfig {
  let mut config = BlobStoreConfig::new();
  config.backend = Some(blob_store_config::Backend::InMemory(
    InMemoryBlobStoreConfig::new(),
  ));
  config
}

fn in_memory_metadata_store() -> MetadataStoreConfig {
  let mut config = MetadataStoreConfig::new();
  config.backend = Some(metadata_store_config::Backend::InMemory(
    InMemoryMetadataStoreConfig::new(),
  ));
  config
}

fn proto_bootstrap_config(member_id: &str) -> ConsumerIteratorBootstrapConfig {
  let mut config = ConsumerIteratorBootstrapConfig::new();
  config.runtime = Some(runtime(member_id)).into();
  config.topic = Some(topic()).into();
  config.blob_store = Some(in_memory_blob_store()).into();
  config.metadata_store = Some(in_memory_metadata_store()).into();
  config
}

#[tokio::test]
async fn bootstrap_builds_iterator_for_in_memory_backends() {
  let config = ConsumerBootstrapConfig::new(
    runtime("member-a"),
    topic(),
    in_memory_blob_store(),
    in_memory_metadata_store(),
  );

  let mut iterator = ConsumerConfigFactory::build_iterator(config).await.unwrap();
  iterator.start().unwrap();
}

#[tokio::test]
async fn bootstrap_builds_iterator_without_static_members() {
  let config = ConsumerBootstrapConfig::new(
    runtime("member-a"),
    topic(),
    in_memory_blob_store(),
    in_memory_metadata_store(),
  );

  let mut iterator = ConsumerConfigFactory::build_iterator(config).await.unwrap();
  iterator.start().unwrap();
}

#[tokio::test]
async fn proto_bootstrap_builds_iterator_for_in_memory_backends() {
  let config = proto_bootstrap_config("member-a");

  let mut iterator = ConsumerConfigFactory::build_iterator_from_proto_config(config)
    .await
    .unwrap();
  iterator.start().unwrap();
}

#[tokio::test]
async fn proto_bootstrap_rejects_missing_required_message_fields() {
  let config = ConsumerIteratorBootstrapConfig::new();

  let error = ConsumerConfigFactory::build_iterator_from_proto_config(config)
    .await
    .err()
    .unwrap();
  assert!(
    error
      .to_string()
      .contains("consumer bootstrap runtime is required")
  );
}
