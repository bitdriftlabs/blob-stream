#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

use blob_stream_proto::protos::blobstream::v1::config::{
  BlobStoreConfig,
  BrokerDiscoveryConfig,
  BrokerNode,
  ConsumerGroupConfig,
  ConsumerIteratorBootstrapConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
  DynamoMetadataStoreConfig,
  MetadataStoreConfig,
  ProducerConfig,
  ProducerRuntimeConfig,
  S3BlobStoreConfig,
  StaticBrokerDiscoveryConfig,
  TopicConfig,
  blob_store_config,
  broker_discovery_config,
  metadata_store_config,
};
use blob_stream_types::ToProtoDuration;
use time::Duration;

pub const TOPIC: &str = "local-text";
pub const DEFAULT_GROUP_ID: &str = "local-demo";
const AWS_REGION: &str = "us-east-1";
const DYNAMODB_ENDPOINT: &str = "http://127.0.0.1:8000";
const S3_ENDPOINT: &str = "http://127.0.0.1:4566";
const S3_BUCKET: &str = "blob-stream-local-e2e";
const S3_PREFIX: &str = "local-text/";
const SEGMENT_METADATA_TABLE: &str = "blob_stream_local_e2e_segments";
const PRODUCER_LEASE_TABLE: &str = "blob_stream_local_e2e_producer_leases";
const CONSUMER_LEASE_TABLE: &str = "blob_stream_local_e2e_consumer_leases";
const CONSUMER_MEMBERSHIP_TABLE: &str = "blob_stream_local_e2e_consumer_membership";

/// Create the topic configuration shared by every process in this walkthrough.
#[must_use]
pub fn topic_config() -> TopicConfig {
  TopicConfig {
    name: TOPIC.to_string().into(),
    partition_count: 4,
    num_writers: 1,
    retention: Duration::hours(1).into_proto(),
    metadata_window_size: Duration::minutes(1).into_proto(),
    max_metadata_publication_lag: Duration::seconds(1).into_proto(),
    ..Default::default()
  }
}

/// Create the fixed two-broker membership used by producers and consumer broker reads.
#[must_use]
pub fn discovery_config() -> BrokerDiscoveryConfig {
  let nodes = [
    ("local-broker-1", "127.0.0.1:8080"),
    ("local-broker-2", "127.0.0.1:8081"),
  ]
  .into_iter()
  .map(|(node_id, address)| BrokerNode {
    node_id: node_id.to_string().into(),
    address: address.to_string().into(),
    ..Default::default()
  })
  .collect();

  BrokerDiscoveryConfig {
    backend: Some(broker_discovery_config::Backend::Static(
      StaticBrokerDiscoveryConfig {
        nodes,
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

/// Create producer settings that flush a single entered line promptly.
#[must_use]
pub fn producer_runtime_config() -> ProducerRuntimeConfig {
  ProducerRuntimeConfig {
    producer: Some(ProducerConfig {
      writer_id: Some(0),
      max_batch_records: Some(1),
      flush_max_delay: Duration::milliseconds(50).into_proto(),
      ..Default::default()
    })
    .into(),
    discovery: Some(discovery_config()).into(),
    topics: vec![topic_config()],
    ..Default::default()
  }
}

/// Create durable LocalStack/DynamoDB storage configuration shared by all consumer instances.
#[must_use]
pub fn consumer_bootstrap_config(
  group_id: &str,
  member_id: &str,
) -> ConsumerIteratorBootstrapConfig {
  ConsumerIteratorBootstrapConfig {
    runtime: Some(ConsumerRuntimeConfig {
      read: Some(ConsumerReadConfig {
        topic: TOPIC.to_string().into(),
        idle_poll_delay: Duration::milliseconds(100).into_proto(),
        max_idle_poll_delay: Duration::milliseconds(500).into_proto(),
        ..Default::default()
      })
      .into(),
      group: Some(ConsumerGroupConfig {
        topic: TOPIC.to_string().into(),
        group_id: group_id.to_string().into(),
        member_id: member_id.to_string().into(),
        lease_duration: Duration::seconds(10).into_proto(),
        heartbeat_interval: Duration::seconds(2).into_proto(),
        rebalance_interval: Duration::seconds(2).into_proto(),
        ..Default::default()
      })
      .into(),
      ..Default::default()
    })
    .into(),
    topic: Some(topic_config()).into(),
    blob_store: Some(BlobStoreConfig {
      backend: Some(blob_store_config::Backend::S3(S3BlobStoreConfig {
        bucket: S3_BUCKET.to_string().into(),
        prefix: S3_PREFIX.to_string().into(),
        region: AWS_REGION.to_string().into(),
        endpoint: S3_ENDPOINT.to_string().into(),
        ..Default::default()
      })),
      ..Default::default()
    })
    .into(),
    metadata_store: Some(MetadataStoreConfig {
      backend: Some(metadata_store_config::Backend::Dynamo(
        DynamoMetadataStoreConfig {
          region: AWS_REGION.to_string().into(),
          endpoint: DYNAMODB_ENDPOINT.to_string().into(),
          segment_metadata_table_name: SEGMENT_METADATA_TABLE.to_string().into(),
          producer_partition_lease_table_name: PRODUCER_LEASE_TABLE.to_string().into(),
          consumer_group_lease_table_name: CONSUMER_LEASE_TABLE.to_string().into(),
          consumer_group_membership_table_name: CONSUMER_MEMBERSHIP_TABLE.to_string().into(),
          ..Default::default()
        },
      )),
      ..Default::default()
    })
    .into(),
    broker_discovery: Some(discovery_config()).into(),
    ..Default::default()
  }
}
