#![allow(clippy::unwrap_used)]

use super::{
  ProducerCompression,
  ProducerConfig,
  ProducerRuntimeConfig,
  ProducerTopicConfig,
  apply_producer_startup_overrides,
  producer_config_with_runtime_overrides,
  producer_flush_max_delay,
  producer_max_batch_bytes,
  producer_max_batch_records,
  producer_max_request_concurrency,
  producer_retry_max_delay,
  validate_topic_config,
};
use bd_runtime_config::loader::Loader;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_types::ToProtoDuration;
use std::sync::Arc;
use time::Duration;

fn runtime_config() -> ProducerRuntimeConfig {
  let mut producer = ProducerConfig::new();
  producer.max_batch_bytes = Some(42);
  producer.flush_max_delay = Duration::milliseconds(123).into_proto();
  producer.retry_max_delay = Duration::milliseconds(456).into_proto();
  producer.max_request_concurrency = Some(7);
  producer.compression = Some(ProducerCompression::PRODUCER_COMPRESSION_SNAPPY.into());

  let mut runtime = ProducerRuntimeConfig::new();
  runtime.producer = Some(producer).into();
  runtime
}

#[test]
fn startup_overrides_preserve_configured_values_without_flags() {
  let flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut runtime = runtime_config();

  apply_producer_startup_overrides(&flags.snapshot_watch(), &mut runtime).unwrap();

  let producer = runtime.producer.as_ref().unwrap();
  assert_eq!(producer_max_batch_records(producer), 10_000);
  assert_eq!(producer_max_batch_bytes(producer), 42);
  assert_eq!(
    producer_flush_max_delay(producer),
    Duration::milliseconds(123)
  );
  assert_eq!(
    producer_retry_max_delay(producer),
    Duration::milliseconds(456)
  );
  assert_eq!(producer_max_request_concurrency(producer), 7);
  assert_eq!(
    producer
      .compression
      .as_ref()
      .unwrap()
      .enum_value_or_default(),
    ProducerCompression::PRODUCER_COMPRESSION_SNAPPY
  );
}

#[test]
fn startup_overrides_replace_configured_values() {
  let flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag("blob_stream_producer_max_batch_records", 321)
      .with_integer_flag("blob_stream_producer_max_batch_bytes", 2_048)
      .with_integer_flag("blob_stream_producer_flush_max_delay_ms", 10)
      .with_integer_flag("blob_stream_producer_retry_max_delay_ms", 2_222)
      .with_integer_flag("blob_stream_producer_max_request_concurrency", 9)
      .with_string_flag("blob_stream_producer_compression", "none"),
  ));
  let mut runtime = runtime_config();

  apply_producer_startup_overrides(&flags.snapshot_watch(), &mut runtime).unwrap();

  let producer = runtime.producer.as_ref().unwrap();
  assert_eq!(producer_max_batch_records(producer), 321);
  assert_eq!(producer_max_batch_bytes(producer), 2_048);
  assert_eq!(
    producer_flush_max_delay(producer),
    Duration::milliseconds(10)
  );
  assert_eq!(
    producer_retry_max_delay(producer),
    Duration::milliseconds(2_222)
  );
  assert_eq!(producer_max_request_concurrency(producer), 9);
  assert_eq!(
    producer
      .compression
      .as_ref()
      .unwrap()
      .enum_value_or_default(),
    ProducerCompression::PRODUCER_COMPRESSION_NONE
  );
}

#[test]
fn startup_overrides_reject_batch_record_overflow() {
  let flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default().with_integer_flag(
    "blob_stream_producer_max_batch_records",
    u64::from(u32::MAX) + 1,
  )));
  let mut runtime = runtime_config();

  let error = apply_producer_startup_overrides(&flags.snapshot_watch(), &mut runtime).unwrap_err();

  assert!(
    error
      .to_string()
      .contains("feature flag blob_stream_producer_max_batch_records exceeds u32")
  );
}

#[test]
fn runtime_overrides_adopt_live_values_and_revert_to_static_config() {
  let flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag("blob_stream_producer_max_batch_records", 321)
      .with_integer_flag("blob_stream_producer_max_batch_bytes", 2_048)
      .with_integer_flag("blob_stream_producer_flush_max_delay_ms", 10)
      .with_integer_flag("blob_stream_producer_retry_base_delay_ms", 20)
      .with_integer_flag("blob_stream_producer_retry_max_delay_ms", 30)
      .with_integer_flag("blob_stream_producer_request_timeout_ms", 40)
      .with_string_flag("blob_stream_producer_compression", "none"),
  ));
  let runtime = runtime_config();
  let producer = runtime.producer.as_ref().unwrap();
  let feature_flags = flags.snapshot_watch();

  let settings = producer_config_with_runtime_overrides(producer, Some(&feature_flags)).unwrap();
  assert_eq!(producer_max_batch_records(&settings), 321);
  assert_eq!(producer_max_batch_bytes(&settings), 2_048);
  assert_eq!(
    producer_flush_max_delay(&settings),
    Duration::milliseconds(10)
  );
  assert_eq!(
    super::producer_retry_base_delay(&settings),
    Duration::milliseconds(20)
  );
  assert_eq!(
    producer_retry_max_delay(&settings),
    Duration::milliseconds(30)
  );
  assert_eq!(
    super::producer_request_timeout(&settings),
    Duration::milliseconds(40)
  );
  assert_eq!(
    super::producer_compression(&settings),
    ProducerCompression::PRODUCER_COMPRESSION_NONE
  );

  flags.update(Arc::new(DefaultFeatureFlags::default()));

  let settings = producer_config_with_runtime_overrides(producer, Some(&feature_flags)).unwrap();
  assert_eq!(producer_max_batch_records(&settings), 10_000);
  assert_eq!(producer_max_batch_bytes(&settings), 42);
  assert_eq!(
    producer_flush_max_delay(&settings),
    Duration::milliseconds(123)
  );
  assert_eq!(
    super::producer_retry_base_delay(&settings),
    Duration::milliseconds(25)
  );
  assert_eq!(
    producer_retry_max_delay(&settings),
    Duration::milliseconds(456)
  );
  assert_eq!(
    super::producer_request_timeout(&settings),
    Duration::seconds(5)
  );
  assert_eq!(
    super::producer_compression(&settings),
    ProducerCompression::PRODUCER_COMPRESSION_SNAPPY
  );
}

#[test]
fn runtime_overrides_reject_invalid_live_snapshot() {
  let flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag("blob_stream_producer_max_batch_records", 0)
      .with_integer_flag("blob_stream_producer_retry_base_delay_ms", 100)
      .with_integer_flag("blob_stream_producer_retry_max_delay_ms", 10)
      .with_string_flag("blob_stream_producer_compression", "unsupported"),
  ));
  let runtime = runtime_config();
  let producer = runtime.producer.as_ref().unwrap();
  let feature_flags = flags.snapshot_watch();

  let error = producer_config_with_runtime_overrides(producer, Some(&feature_flags)).unwrap_err();
  assert_eq!(
    error.to_string(),
    "invalid blob-stream compression override: unsupported"
  );
}

#[test]
fn topic_validation_rejects_virtual_partition_count_overflow() {
  let mut topic = ProducerTopicConfig::new();
  topic.name = "telemetry".into();
  topic.partition_count = u32::MAX;
  topic.num_writers = 2;
  topic.retention = Duration::days(7).into_proto();

  let error = validate_topic_config(&topic).unwrap_err();

  assert_eq!(
    error.to_string(),
    "topic telemetry: partition_count * num_writers must fit within u32"
  );
}
