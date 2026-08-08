use super::{
  ConsumerReadConfig,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  consumer_candidate_window_count,
  consumer_idle_poll_delay_ms,
  consumer_max_idle_poll_delay_ms,
  consumer_max_in_flight_batch_reads,
  consumer_metadata_visibility_delay_ms,
  consumer_prefetch_max_bytes,
  consumer_read_runtime_settings,
  validate_group_config,
  validate_read_config,
};
use crate::config::ConsumerGroupConfig;
use bd_runtime_config::loader::Loader;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_types::DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS as SHARED_PUBLICATION_LAG_MS;
use std::sync::Arc;

fn read_config() -> ConsumerReadConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".to_string().into();
  read
}

#[test]
fn read_defaults_derive_two_candidate_windows_and_two_second_idle_cap() {
  let read = read_config();
  assert_eq!(
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    SHARED_PUBLICATION_LAG_MS
  );
  assert_eq!(
    consumer_candidate_window_count(&read, DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS).unwrap(),
    2
  );
  assert_eq!(consumer_idle_poll_delay_ms(&read), 250);
  assert_eq!(consumer_max_idle_poll_delay_ms(&read), 2_000);
  assert_eq!(consumer_prefetch_max_bytes(&read), 64 * 1024 * 1024);
  assert_eq!(consumer_metadata_visibility_delay_ms(&read), 2_000);
  assert_eq!(consumer_max_in_flight_batch_reads(&read), 32);
}

#[test]
fn candidate_windows_cover_publication_and_visibility_delay() {
  let mut read = read_config();
  read.window_size_seconds = Some(300);
  read.metadata_visibility_delay_ms = Some(300_000);

  assert_eq!(consumer_candidate_window_count(&read, 30_000).unwrap(), 3);
}

#[test]
fn candidate_windows_rejects_unbounded_scan_horizon() {
  let mut read = read_config();
  read.window_size_seconds = Some(300);
  read.metadata_visibility_delay_ms = Some(9_600_000);

  let error = consumer_candidate_window_count(&read, 300_000).unwrap_err();
  assert!(
    error
      .to_string()
      .contains("metadata availability horizon exceeds")
  );
}

#[test]
fn metadata_visibility_delay_uses_explicit_value() {
  let mut read = read_config();
  read.metadata_visibility_delay_ms = Some(1_500);

  assert_eq!(consumer_metadata_visibility_delay_ms(&read), 1_500);
}

#[test]
fn prefetch_max_bytes_uses_explicit_value() {
  let mut read = read_config();
  read.prefetch_max_bytes = Some(8 * 1024 * 1024);
  assert_eq!(consumer_prefetch_max_bytes(&read), 8 * 1024 * 1024);
}

#[test]
fn prefetch_max_bytes_zero_uses_default() {
  let mut read = read_config();
  read.prefetch_max_bytes = Some(0);

  assert_eq!(consumer_prefetch_max_bytes(&read), 64 * 1024 * 1024);
  assert_eq!(
    consumer_read_runtime_settings(&read, None).prefetch_max_bytes,
    64 * 1024 * 1024
  );
}

#[test]
fn max_in_flight_batch_reads_uses_explicit_value() {
  let mut read = read_config();
  read.max_in_flight_batch_reads = Some(64);
  assert_eq!(consumer_max_in_flight_batch_reads(&read), 64);
}

#[test]
fn runtime_feature_flags_override_configured_reader_settings() {
  let mut read = read_config();
  read.prefetch_max_bytes = Some(16);
  read.max_in_flight_batch_reads = Some(2);
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag("blob_stream_consumer_prefetch_max_bytes", 32)
      .with_integer_flag("blob_stream_consumer_max_in_flight_batch_reads", 4),
  ));

  assert_eq!(
    consumer_read_runtime_settings(&read, Some(&feature_flags.snapshot_watch())),
    super::ConsumerReadRuntimeSettings {
      prefetch_max_bytes: 32,
      max_in_flight_batch_reads: 4,
    }
  );
}

#[test]
fn validate_read_config_rejects_max_idle_backoff_less_than_base() {
  let mut read = read_config();
  read.idle_poll_delay_ms = Some(500);
  read.max_idle_poll_delay_ms = Some(400);

  let error = validate_read_config(&read).unwrap_err();
  assert!(
    error
      .to_string()
      .contains("consumer.read.max_idle_poll_delay_ms must be greater than or equal to")
  );
}

#[test]
fn validate_read_config_accepts_idle_backoff_range() {
  let mut read = read_config();
  read.idle_poll_delay_ms = Some(250);
  read.max_idle_poll_delay_ms = Some(2_000);

  validate_read_config(&read).unwrap();
}

#[test]
fn validate_group_config_rejects_reserved_member_id_prefix() {
  let mut group = ConsumerGroupConfig::new();
  group.topic = "telemetry".to_string().into();
  group.group_id = "group-a".to_string().into();
  group.member_id = "__blob_stream_assignment_plan_v1__".to_string().into();

  let error = validate_group_config(&group).unwrap_err();
  assert!(error.to_string().contains("uses reserved prefix"));
}

#[test]
fn validate_group_config_rejects_empty_pod_id() {
  let mut group = ConsumerGroupConfig::new();
  group.topic = "telemetry".to_string().into();
  group.group_id = "group-a".to_string().into();
  group.member_id = "member-a".to_string().into();
  group.pod_id = Some(String::new().into());

  assert!(validate_group_config(&group).is_err());
}
