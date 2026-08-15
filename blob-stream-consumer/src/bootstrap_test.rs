#![allow(clippy::unwrap_used)]

use crate::bootstrap::{ConsumerBootstrapConfig, consumer_group_lease_ttl_buffer};
use crate::iterator::ConsumerIterator;
use crate::{
  ConsumerConfigFactory,
  ConsumerGroupConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
};
use bd_server_stats::stats::Collector;
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
use blob_stream_types::ToProtoDuration;
use time::Duration;

fn runtime(member_id: &str) -> ConsumerRuntimeConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".into();
  read.window_size = Duration::seconds(300).into_proto();

  let mut group = ConsumerGroupConfig::new();
  group.topic = "telemetry".into();
  group.group_id = "group-a".into();
  group.member_id = member_id.to_string().into();
  group.lease_duration = Duration::seconds(30).into_proto();
  group.heartbeat_interval = Duration::seconds(10).into_proto();
  group.rebalance_interval = Duration::seconds(10).into_proto();

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
  topic.retention = Duration::days(7).into_proto();
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

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_consumer_test")
}

#[test]
fn consumer_group_lease_ttl_matches_topic_retention() {
  assert_eq!(
    consumer_group_lease_ttl_buffer(Duration::days(7)).unwrap(),
    Duration::days(7)
  );
}

#[tokio::test]
async fn bootstrap_builds_iterator_for_in_memory_backends() {
  let config = ConsumerBootstrapConfig::new(
    runtime("member-a"),
    topic(),
    in_memory_blob_store(),
    in_memory_metadata_store(),
  );

  let mut iterator = ConsumerConfigFactory::build_iterator(config, metrics_scope(), None)
    .await
    .unwrap();
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

  let mut iterator = ConsumerConfigFactory::build_iterator(config, metrics_scope(), None)
    .await
    .unwrap();
  iterator.start().unwrap();
}

#[tokio::test]
async fn proto_bootstrap_builds_iterator_for_in_memory_backends() {
  let config = proto_bootstrap_config("member-a");

  let mut iterator =
    ConsumerConfigFactory::build_iterator_from_proto_config(config, metrics_scope(), None)
      .await
      .unwrap();
  iterator.start().unwrap();
}

#[tokio::test]
async fn proto_bootstrap_rejects_missing_required_message_fields() {
  let config = ConsumerIteratorBootstrapConfig::new();

  let error =
    ConsumerConfigFactory::build_iterator_from_proto_config(config, metrics_scope(), None)
      .await
      .err()
      .unwrap();
  assert!(
    error
      .to_string()
      .contains("ConsumerIteratorBootstrapConfig.runtime")
  );
}

#[tokio::test]
async fn bootstrap_rejects_missing_retention_for_recovery() {
  let mut topic = topic();
  topic.retention.clear();
  let config = ConsumerBootstrapConfig::new(
    runtime("member-a"),
    topic,
    in_memory_blob_store(),
    in_memory_metadata_store(),
  );

  assert!(
    ConsumerConfigFactory::build_iterator(config, metrics_scope(), None)
      .await
      .is_err()
  );
}
