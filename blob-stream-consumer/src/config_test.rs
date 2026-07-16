use super::{
  ConsumerReadConfig,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  consumer_candidate_window_count,
  consumer_idle_poll_delay_ms,
  consumer_max_idle_poll_delay_ms,
  consumer_metadata_visibility_delay_ms,
  consumer_prefetch_max_bytes,
  validate_read_config,
};

fn read_config() -> ConsumerReadConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".to_string().into();
  read
}

#[test]
fn read_defaults_derive_two_candidate_windows_and_two_second_idle_cap() {
  let read = read_config();
  assert_eq!(
    consumer_candidate_window_count(&read, DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS).unwrap(),
    2
  );
  assert_eq!(consumer_idle_poll_delay_ms(&read), 250);
  assert_eq!(consumer_max_idle_poll_delay_ms(&read), 2_000);
  assert_eq!(consumer_prefetch_max_bytes(&read), 64 * 1024 * 1024);
  assert_eq!(consumer_metadata_visibility_delay_ms(&read), 2_000);
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
