use super::{
  ConsumerReadConfig,
  consumer_idle_poll_delay_ms,
  consumer_max_idle_poll_delay_ms,
  validate_read_config,
};

fn read_config() -> ConsumerReadConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".to_string().into();
  read
}

#[test]
fn idle_poll_delay_defaults_to_250ms() {
  let read = read_config();
  assert_eq!(consumer_idle_poll_delay_ms(&read), 250);
  assert_eq!(consumer_max_idle_poll_delay_ms(&read), None);
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
