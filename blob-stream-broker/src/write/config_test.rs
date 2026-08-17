use super::{DynamoTablePurpose, TopicInfo, WriteConfig, dynamo_table_name, validate_writer_id};
use blob_stream_proto::protos::blobstream::v1::config::{
  BrokerConfig,
  DynamoMetadataStoreConfig,
  SegmentCompression,
  TopicConfig,
};
use blob_stream_types::{CompressionCodec, DEFAULT_MAX_METADATA_PUBLICATION_LAG, ToProtoDuration};
use std::collections::HashMap;
use time::Duration;

fn dynamo_config() -> DynamoMetadataStoreConfig {
  let mut config = DynamoMetadataStoreConfig::new();
  config.segment_metadata_table_name = "blob-segments".into();
  config.producer_partition_lease_table_name = "producer-leases".into();
  config.consumer_group_lease_table_name = "consumer-leases".into();
  config
}

#[test]
fn prefers_explicit_segment_metadata_table_name() {
  let config = dynamo_config();
  let table_name = dynamo_table_name(&config, DynamoTablePurpose::SegmentMetadata);
  assert_eq!(table_name, "blob-segments");
}

#[test]
fn prefers_explicit_producer_lease_table_name() {
  let config = dynamo_config();
  let table_name = dynamo_table_name(&config, DynamoTablePurpose::ProducerPartitionLeases);
  assert_eq!(table_name, "producer-leases");
}

#[test]
fn returns_empty_when_explicit_fields_are_unset() {
  let config = DynamoMetadataStoreConfig::new();

  let metadata_table = dynamo_table_name(&config, DynamoTablePurpose::SegmentMetadata);
  let producer_lease_table =
    dynamo_table_name(&config, DynamoTablePurpose::ProducerPartitionLeases);

  assert_eq!(metadata_table, "");
  assert_eq!(producer_lease_table, "");
}

#[test]
fn defaults_segment_compression_to_zstd() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);

  let config = WriteConfig::from_broker_config(&broker_config).unwrap();

  assert_eq!(config.compression.codec, CompressionCodec::Zstd);
  assert_eq!(config.compression.level, Some(3));
  assert_eq!(config.writer_id, 0);
  assert_eq!(config.lease_duration, Duration::seconds(30));
  assert_eq!(config.heartbeat_interval, Duration::seconds(10));
  assert_eq!(config.reservation_size, 10_000);
  assert!(!config.fenced_metadata_writes);
}

#[test]
fn respects_explicit_lease_timing_configuration() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);
  broker_config.lease_duration = Duration::seconds(120).into_proto();
  broker_config.heartbeat_interval = Duration::seconds(40).into_proto();

  let config = WriteConfig::from_broker_config(&broker_config).unwrap();

  assert_eq!(config.lease_duration, Duration::seconds(120));
  assert_eq!(config.heartbeat_interval, Duration::seconds(40));
}

#[test]
fn rejects_heartbeat_interval_at_or_above_lease_duration() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);
  broker_config.lease_duration = Duration::seconds(30).into_proto();
  broker_config.heartbeat_interval = Duration::seconds(30).into_proto();

  let error = WriteConfig::from_broker_config(&broker_config).unwrap_err();

  assert_eq!(
    error.to_string(),
    "broker heartbeat_interval must be less than lease_duration"
  );
}

#[test]
fn topic_defaults_metadata_publication_lag_to_fifteen_seconds() {
  let mut topic_config = TopicConfig::new();
  topic_config.retention = Duration::days(1).into_proto();
  let topic = TopicInfo::from_proto(&topic_config).unwrap();

  assert_eq!(
    topic.max_metadata_publication_lag,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG
  );
}

#[test]
fn respects_explicit_sequence_reservation_size() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);
  broker_config.sequence_reservation_size = Some(25_000);

  let config = WriteConfig::from_broker_config(&broker_config).unwrap();

  assert_eq!(config.reservation_size, 25_000);
}

#[test]
fn respects_explicit_fenced_metadata_writes() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);
  broker_config.fenced_metadata_writes = true;

  let config = WriteConfig::from_broker_config(&broker_config).unwrap();

  assert!(config.fenced_metadata_writes);
}

#[test]
fn fenced_metadata_writes_defaults_to_static_config_without_feature_flags() {
  let mut config = WriteConfig::with_defaults();
  config.fenced_metadata_writes = true;

  assert!(config.fenced_metadata_writes(None));
}

#[test]
fn derives_produce_request_timeout_from_flush_delay() {
  let mut config = WriteConfig::with_defaults();
  config.flush_max_delay = Duration::milliseconds(250);

  assert_eq!(config.produce_request_timeout().as_millis(), 2_500);
}

#[test]
fn rejects_zero_sequence_reservation_size() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);
  broker_config.sequence_reservation_size = Some(0);

  let error = WriteConfig::from_broker_config(&broker_config).unwrap_err();

  assert_eq!(
    error.to_string(),
    "broker sequence_reservation_size must be positive"
  );
}

#[test]
fn respects_uncompressed_segment_configuration() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);
  broker_config.segment_compression = Some(SegmentCompression::SEGMENT_COMPRESSION_NONE.into());

  let config = WriteConfig::from_broker_config(&broker_config).unwrap();

  assert_eq!(config.compression.codec, CompressionCodec::None);
  assert_eq!(config.compression.level, None);
}

#[test]
fn rejects_broker_configuration_without_writer_id() {
  let error = WriteConfig::from_broker_config(&BrokerConfig::new()).unwrap_err();

  assert_eq!(
    error.to_string(),
    "broker writer_id must be explicitly configured"
  );
}

#[test]
fn rejects_writer_id_outside_a_topic_range() {
  let topics = HashMap::from([(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count: 2,
      num_writers: 1,
      retention: Duration::days(7),
      max_metadata_publication_lag: Duration::seconds(30),
      metadata_window_size: Duration::minutes(5),
    },
  )]);

  let error = validate_writer_id(1, &topics).unwrap_err();

  assert_eq!(
    error.to_string(),
    "broker writer_id 1 must be less than num_writers 1 for topic telemetry"
  );
}
