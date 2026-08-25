use super::{
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG,
  apply_consumer_startup_overrides,
  consumer_candidate_window_count,
  consumer_idle_poll_delay,
  consumer_max_clock_skew,
  consumer_max_idle_poll_delay,
  consumer_max_in_flight_batch_reads,
  consumer_metadata_visibility_delay,
  consumer_prefetch_max_bytes,
  consumer_read_runtime_settings,
  consumer_strongly_consistent_metadata_reads,
  validate_group_config,
  validate_read_config,
};
use crate::config::ConsumerGroupConfig;
use bd_runtime_config::loader::Loader;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_metadata_store::MetadataReadConsistency;
use blob_stream_types::{
  DEFAULT_MAX_METADATA_PUBLICATION_LAG as SHARED_PUBLICATION_LAG,
  DEFAULT_METADATA_WINDOW_SIZE,
  ProtoDurationExt,
  ToProtoDuration,
};
use std::sync::Arc;
use time::Duration;

fn read_config() -> ConsumerReadConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".to_string().into();
  read
}

fn runtime_config() -> ConsumerRuntimeConfig {
  let mut read = read_config();
  read.idle_poll_delay = Duration::milliseconds(123).into_proto();
  read.max_idle_poll_delay = Duration::milliseconds(456).into_proto();

  let mut group = ConsumerGroupConfig::new();
  group.topic = "telemetry".into();
  group.group_id = "group-a".into();
  group.member_id = "member-a".into();
  group.lease_duration = Duration::milliseconds(1_000).into_proto();
  group.heartbeat_interval = Duration::milliseconds(200).into_proto();
  group.rebalance_interval = Duration::milliseconds(300).into_proto();

  let mut runtime = ConsumerRuntimeConfig::new();
  runtime.read = Some(read).into();
  runtime.group = Some(group).into();
  runtime
}

#[test]
fn startup_overrides_preserve_configured_values_without_flags() {
  let flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut runtime = runtime_config();

  apply_consumer_startup_overrides(&flags.snapshot_watch(), &mut runtime).unwrap();

  let read = runtime.read.as_ref().unwrap();
  let group = runtime.group.as_ref().unwrap();
  assert_eq!(consumer_idle_poll_delay(read), Duration::milliseconds(123));
  assert_eq!(
    consumer_max_idle_poll_delay(read),
    Duration::milliseconds(456)
  );
  assert_eq!(
    group.lease_duration.as_ref().unwrap().to_time_duration(),
    Duration::milliseconds(1_000)
  );
  assert_eq!(
    group
      .heartbeat_interval
      .as_ref()
      .unwrap()
      .to_time_duration(),
    Duration::milliseconds(200)
  );
  assert_eq!(
    group
      .rebalance_interval
      .as_ref()
      .unwrap()
      .to_time_duration(),
    Duration::milliseconds(300)
  );
}

#[test]
fn startup_overrides_replace_configured_values() {
  let flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag("blob_stream_consumer_idle_poll_delay_ms", 10)
      .with_integer_flag("blob_stream_consumer_max_idle_poll_delay_ms", 20)
      .with_integer_flag("blob_stream_consumer_lease_duration_ms", 30)
      .with_integer_flag("blob_stream_consumer_heartbeat_interval_ms", 40)
      .with_integer_flag("blob_stream_consumer_rebalance_interval_ms", 50),
  ));
  let mut runtime = runtime_config();

  apply_consumer_startup_overrides(&flags.snapshot_watch(), &mut runtime).unwrap();

  let read = runtime.read.as_ref().unwrap();
  let group = runtime.group.as_ref().unwrap();
  assert_eq!(consumer_idle_poll_delay(read), Duration::milliseconds(10));
  assert_eq!(
    consumer_max_idle_poll_delay(read),
    Duration::milliseconds(20)
  );
  assert_eq!(
    group.lease_duration.as_ref().unwrap().to_time_duration(),
    Duration::milliseconds(30)
  );
  assert_eq!(
    group
      .heartbeat_interval
      .as_ref()
      .unwrap()
      .to_time_duration(),
    Duration::milliseconds(40)
  );
  assert_eq!(
    group
      .rebalance_interval
      .as_ref()
      .unwrap()
      .to_time_duration(),
    Duration::milliseconds(50)
  );
}

#[test]
fn startup_overrides_reject_duration_overflow() {
  let flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default().with_integer_flag(
    "blob_stream_consumer_idle_poll_delay_ms",
    u64::try_from(i64::MAX).unwrap() + 1,
  )));
  let mut runtime = runtime_config();

  let error = apply_consumer_startup_overrides(&flags.snapshot_watch(), &mut runtime).unwrap_err();

  assert!(
    error
      .to_string()
      .contains("feature flag blob_stream_consumer_idle_poll_delay_ms exceeds i64")
  );
}

#[test]
fn read_defaults_derive_two_candidate_windows_and_two_second_idle_cap() {
  let read = read_config();
  assert_eq!(DEFAULT_MAX_METADATA_PUBLICATION_LAG, SHARED_PUBLICATION_LAG);
  assert_eq!(DEFAULT_MAX_METADATA_PUBLICATION_LAG, Duration::seconds(15));
  assert_eq!(
    consumer_candidate_window_count(
      &read,
      DEFAULT_MAX_METADATA_PUBLICATION_LAG,
      DEFAULT_METADATA_WINDOW_SIZE,
    )
    .unwrap(),
    2
  );
  assert_eq!(consumer_idle_poll_delay(&read), Duration::milliseconds(250));
  assert_eq!(consumer_max_idle_poll_delay(&read), Duration::seconds(2));
  assert_eq!(consumer_prefetch_max_bytes(&read), 64 * 1024 * 1024);
  assert_eq!(
    consumer_metadata_visibility_delay(&read),
    Duration::milliseconds(2_000)
  );
  assert!(!consumer_strongly_consistent_metadata_reads(&read));
  assert_eq!(consumer_max_in_flight_batch_reads(&read), 32);
}

#[test]
fn candidate_windows_cover_publication_and_visibility_delay() {
  let mut read = read_config();
  read.metadata_visibility_delay = Duration::seconds(300).into_proto();

  assert_eq!(
    consumer_candidate_window_count(&read, Duration::seconds(30), DEFAULT_METADATA_WINDOW_SIZE,)
      .unwrap(),
    3
  );
}

#[test]
fn candidate_windows_round_sub_millisecond_horizons_up() {
  let mut read = read_config();
  read.metadata_visibility_delay = (Duration::seconds(300) + Duration::nanoseconds(1)).into_proto();

  assert_eq!(
    consumer_candidate_window_count(&read, Duration::ZERO, DEFAULT_METADATA_WINDOW_SIZE).unwrap(),
    3
  );
}

#[test]
fn candidate_windows_rejects_unbounded_scan_horizon() {
  let mut read = read_config();
  read.metadata_visibility_delay = Duration::minutes(160).into_proto();

  let error =
    consumer_candidate_window_count(&read, Duration::seconds(300), DEFAULT_METADATA_WINDOW_SIZE)
      .unwrap_err();
  assert!(
    error
      .to_string()
      .contains("metadata availability horizon exceeds")
  );
}

#[test]
fn metadata_visibility_delay_uses_explicit_value() {
  let mut read = read_config();
  read.metadata_visibility_delay = Duration::milliseconds(1_500).into_proto();

  assert_eq!(
    consumer_metadata_visibility_delay(&read),
    Duration::milliseconds(1_500)
  );
}

#[test]
fn validate_read_config_accepts_zero_metadata_visibility_delay() {
  let mut read = read_config();
  read.metadata_visibility_delay = Duration::ZERO.into_proto();

  validate_read_config(&read).unwrap();
}

#[test]
fn consumer_clock_skew_uses_default_and_explicit_values() {
  let mut read = read_config();
  assert_eq!(consumer_max_clock_skew(&read), Duration::milliseconds(10));

  read.max_clock_skew = Duration::milliseconds(25).into_proto();
  assert_eq!(consumer_max_clock_skew(&read), Duration::milliseconds(25));
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
      metadata_read_consistency: MetadataReadConsistency::Eventual,
      metadata_visibility_delay: Duration::milliseconds(2_000),
    }
  );
}

#[test]
fn strong_metadata_reads_ignore_the_configured_visibility_delay() {
  let mut read = read_config();
  read.metadata_visibility_delay = Duration::milliseconds(1_500).into_proto();
  read.strongly_consistent_metadata_reads = Some(true);

  let runtime_settings = consumer_read_runtime_settings(&read, None);

  assert!(consumer_strongly_consistent_metadata_reads(&read));
  assert_eq!(
    runtime_settings.metadata_read_consistency,
    MetadataReadConsistency::Strong
  );
  assert_eq!(runtime_settings.metadata_visibility_delay, Duration::ZERO);
}

#[test]
fn runtime_feature_flag_overrides_configured_metadata_read_consistency() {
  let mut read = read_config();
  read.strongly_consistent_metadata_reads = Some(true);
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_bool_flag("blob_stream_consumer_strong_metadata_reads", false),
  ));

  let runtime_settings =
    consumer_read_runtime_settings(&read, Some(&feature_flags.snapshot_watch()));

  assert_eq!(
    runtime_settings.metadata_read_consistency,
    MetadataReadConsistency::Eventual
  );
  assert_eq!(
    runtime_settings.metadata_visibility_delay,
    Duration::milliseconds(2_000)
  );
}

#[test]
fn validate_read_config_rejects_max_idle_backoff_less_than_base() {
  let mut read = read_config();
  read.idle_poll_delay = Duration::milliseconds(500).into_proto();
  read.max_idle_poll_delay = Duration::milliseconds(400).into_proto();

  let error = validate_read_config(&read).unwrap_err();
  assert!(
    error
      .to_string()
      .contains("consumer.read.max_idle_poll_delay must be greater than or equal to")
  );
}

#[test]
fn validate_read_config_accepts_idle_backoff_range() {
  let mut read = read_config();
  read.idle_poll_delay = Duration::milliseconds(250).into_proto();
  read.max_idle_poll_delay = Duration::seconds(2).into_proto();

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
