use super::{
  ConsumerReadConfig,
  consumer_idle_poll_delay_ms,
  consumer_lookback_windows,
  consumer_max_idle_poll_delay_ms,
  consumer_metadata_fast_scan_enabled,
  consumer_metadata_recovery_scan_interval_seconds,
  consumer_prefetch_max_bytes,
  validate_read_config,
};

fn read_config() -> ConsumerReadConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".to_string().into();
  read
}

#[test]
fn read_defaults_use_two_windows_and_two_second_idle_cap() {
  let read = read_config();
  assert_eq!(consumer_lookback_windows(&read), 2);
  assert_eq!(consumer_idle_poll_delay_ms(&read), 250);
  assert_eq!(consumer_max_idle_poll_delay_ms(&read), 2_000);
  assert_eq!(consumer_prefetch_max_bytes(&read), 64 * 1024 * 1024);
  assert_eq!(consumer_metadata_recovery_scan_interval_seconds(&read), 60);
  assert!(consumer_metadata_fast_scan_enabled(&read));
}

#[test]
fn metadata_fast_scan_can_be_disabled() {
  let mut read = read_config();
  read.metadata_fast_scan_enabled = Some(false);

  assert!(!consumer_metadata_fast_scan_enabled(&read));
}

#[test]
fn metadata_recovery_scan_interval_uses_explicit_value() {
  let mut read = read_config();
  read.metadata_recovery_scan_interval_seconds = Some(30);

  assert_eq!(consumer_metadata_recovery_scan_interval_seconds(&read), 30);
}

#[test]
fn prefetch_max_bytes_uses_explicit_value() {
  let mut read = read_config();
  read.prefetch_max_bytes = Some(8 * 1024 * 1024);
  assert_eq!(consumer_prefetch_max_bytes(&read), 8 * 1024 * 1024);
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
