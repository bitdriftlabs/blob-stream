#![allow(clippy::unwrap_used)]

use super::{
  ProducerCompression,
  ProducerConfig,
  ProducerRuntimeConfig,
  apply_producer_startup_overrides,
  producer_flush_max_delay,
  producer_max_batch_bytes,
  producer_max_batch_records,
  producer_max_request_concurrency,
  producer_retry_max_delay,
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
  assert_eq!(producer_max_batch_records(producer), 1_000);
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
