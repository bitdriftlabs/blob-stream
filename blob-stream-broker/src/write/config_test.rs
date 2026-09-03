use super::{
  ADAPTIVE_FLUSH_MAX_DELAY_ENABLED_FEATURE_FLAG,
  ADAPTIVE_FLUSH_MAX_DELAY_FLOOR_FEATURE_FLAG,
  DynamoTablePurpose,
  FLUSH_MAX_BYTES_FEATURE_FLAG,
  FLUSH_MAX_DELAY_FEATURE_FLAG,
  MAX_SEGMENT_BYTES_FEATURE_FLAG,
  TopicInfo,
  WriteConfig,
  dynamo_table_name,
  validate_writer_id,
};
use bd_runtime_config::loader::Loader;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_proto::protos::blobstream::v1::config::{
  BrokerConfig,
  DynamoMetadataStoreConfig,
  SegmentCompression,
  TopicConfig,
};
use blob_stream_types::{CompressionCodec, DEFAULT_MAX_METADATA_PUBLICATION_LAG, ToProtoDuration};
use std::collections::HashMap;
use std::sync::Arc;
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
  assert_eq!(config.max_segment_bytes, 64 * 1024 * 1024);
  assert!(!config.fenced_metadata_writes);
  assert!(config.adaptive_flush_max_delay_enabled);
  assert_eq!(config.adaptive_flush_max_delay_floor, None);
}

#[test]
fn respects_explicit_max_segment_bytes() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);
  broker_config.max_segment_bytes = Some(8 * 1024 * 1024);

  let config = WriteConfig::from_broker_config(&broker_config).unwrap();

  assert_eq!(config.max_segment_bytes, 8 * 1024 * 1024);
}

#[test]
fn respects_explicit_adaptive_flush_delay_disablement() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);
  broker_config.flush_max_delay = Duration::milliseconds(250).into_proto();
  broker_config.adaptive_flush_max_delay_enabled = Some(false);
  broker_config.adaptive_flush_max_delay_floor = Duration::milliseconds(100).into_proto();

  let config = WriteConfig::from_broker_config(&broker_config).unwrap();

  assert!(!config.adaptive_flush_max_delay_enabled);
  assert_eq!(
    config.adaptive_flush_max_delay_floor,
    Some(Duration::milliseconds(100))
  );
}

#[test]
fn rejects_adaptive_flush_delay_floor_above_static_maximum() {
  let mut broker_config = BrokerConfig::new();
  broker_config.writer_id = Some(0);
  broker_config.flush_max_delay = Duration::milliseconds(250).into_proto();
  broker_config.adaptive_flush_max_delay_floor = Duration::milliseconds(251).into_proto();

  let error = WriteConfig::from_broker_config(&broker_config).unwrap_err();

  assert_eq!(
    error.to_string(),
    "broker adaptive_flush_max_delay_floor must not exceed flush_max_delay"
  );
}

#[test]
fn max_segment_bytes_runtime_override_defaults_to_configured_value() {
  let mut config = WriteConfig::with_defaults();
  config.max_segment_bytes = 8 * 1024 * 1024;
  let defaults = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let override_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag(MAX_SEGMENT_BYTES_FEATURE_FLAG, 4 * 1024 * 1024),
  ));

  assert_eq!(
    config.max_segment_bytes(Some(&defaults.snapshot_watch())),
    config.max_segment_bytes
  );
  assert_eq!(
    config.max_segment_bytes(Some(&override_flags.snapshot_watch())),
    4 * 1024 * 1024
  );
}

#[test]
fn max_segment_bytes_runtime_zero_uses_configured_value() {
  let mut config = WriteConfig::with_defaults();
  config.max_segment_bytes = 8 * 1024 * 1024;
  let override_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(MAX_SEGMENT_BYTES_FEATURE_FLAG, 0),
  ));

  assert_eq!(
    config.max_segment_bytes(Some(&override_flags.snapshot_watch())),
    config.max_segment_bytes
  );
}

#[test]
fn flush_runtime_overrides_default_to_configured_values() {
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 8 * 1024 * 1024;
  config.flush_max_delay = Duration::milliseconds(250);
  let defaults = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let overrides = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag(FLUSH_MAX_BYTES_FEATURE_FLAG, 4 * 1024 * 1024)
      .with_integer_flag(FLUSH_MAX_DELAY_FEATURE_FLAG, 100),
  ));

  let default_flush_config = config.effective_flush_config(Some(&defaults.snapshot_watch()));
  assert_eq!(default_flush_config.max_bytes, config.flush_max_bytes);
  assert_eq!(default_flush_config.max_delay, config.flush_max_delay);

  let overridden_flush_config = config.effective_flush_config(Some(&overrides.snapshot_watch()));
  assert_eq!(overridden_flush_config.max_bytes, 4 * 1024 * 1024);
  assert_eq!(
    overridden_flush_config.max_delay,
    Duration::milliseconds(100)
  );
}

#[test]
fn invalid_flush_runtime_overrides_use_configured_values() {
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 8 * 1024 * 1024;
  config.flush_max_delay = Duration::milliseconds(250);
  let zero_overrides = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag(FLUSH_MAX_BYTES_FEATURE_FLAG, 0)
      .with_integer_flag(FLUSH_MAX_DELAY_FEATURE_FLAG, 0),
  ));
  let over_limit_delay = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(FLUSH_MAX_DELAY_FEATURE_FLAG, 500),
  ));

  let flush_config = config.effective_flush_config(Some(&zero_overrides.snapshot_watch()));
  assert_eq!(flush_config.max_bytes, config.flush_max_bytes);
  assert_eq!(flush_config.max_delay, config.flush_max_delay);

  let flush_config = config.effective_flush_config(Some(&over_limit_delay.snapshot_watch()));
  assert_eq!(flush_config.max_delay, config.flush_max_delay);
}

#[test]
fn adaptive_flush_delay_defaults_to_enabled_with_half_the_effective_maximum_as_its_floor() {
  let mut config = WriteConfig::with_defaults();
  config.flush_max_delay = Duration::milliseconds(250);
  let defaults = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));

  let flush_config = config.effective_flush_config(None);

  assert!(flush_config.adaptive_flush_delay.enabled);
  assert_eq!(
    flush_config.adaptive_flush_delay.floor,
    Duration::milliseconds(125)
  );
  assert_eq!(
    flush_config.adaptive_flush_delay.max_delay,
    Duration::milliseconds(250)
  );

  let flush_config = config.effective_flush_config(Some(&defaults.snapshot_watch()));
  assert!(flush_config.adaptive_flush_delay.enabled);
}

#[test]
fn adaptive_flush_delay_runtime_overrides_use_the_live_delay_ceiling() {
  let mut config = WriteConfig::with_defaults();
  config.flush_max_delay = Duration::milliseconds(1_000);
  config.adaptive_flush_max_delay_floor = Some(Duration::milliseconds(600));
  let overrides = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag(FLUSH_MAX_DELAY_FEATURE_FLAG, 400)
      .with_bool_flag(ADAPTIVE_FLUSH_MAX_DELAY_ENABLED_FEATURE_FLAG, false),
  ));

  let flush_config = config.effective_flush_config(Some(&overrides.snapshot_watch()));

  assert!(!flush_config.adaptive_flush_delay.enabled);
  assert_eq!(
    flush_config.adaptive_flush_delay.max_delay,
    Duration::milliseconds(400)
  );
  assert_eq!(
    flush_config.adaptive_flush_delay.floor,
    Duration::milliseconds(400)
  );
}

#[test]
fn invalid_adaptive_flush_delay_floor_runtime_override_uses_the_configured_floor() {
  let mut config = WriteConfig::with_defaults();
  config.flush_max_delay = Duration::milliseconds(1_000);
  config.adaptive_flush_max_delay_floor = Some(Duration::milliseconds(300));
  let overrides = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag(ADAPTIVE_FLUSH_MAX_DELAY_FLOOR_FEATURE_FLAG, 1_001),
  ));

  let flush_config = config.effective_flush_config(Some(&overrides.snapshot_watch()));

  assert_eq!(
    flush_config.adaptive_flush_delay.floor,
    Duration::milliseconds(300)
  );
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
fn topic_rejects_virtual_partition_count_overflow() {
  let mut topic_config = TopicConfig::new();
  topic_config.name = "telemetry".into();
  topic_config.partition_count = u32::MAX;
  topic_config.num_writers = 2;

  let error = TopicInfo::from_proto(&topic_config).unwrap_err();

  assert_eq!(
    error.to_string(),
    "topic telemetry: partition_count * num_writers must fit within u32"
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
