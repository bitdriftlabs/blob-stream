use super::*;
use bytes::Bytes;

#[test]
fn default_metadata_publication_lag_is_fifteen_seconds() {
  assert_eq!(DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS, 15_000);
}

#[test]
fn default_metadata_window_is_five_minutes() {
  assert_eq!(DEFAULT_METADATA_WINDOW_SIZE_SECONDS, 300);
}

#[test]
fn record_batch_summary() {
  let batch = RecordBatch::new(
    7,
    vec![
      new_record(vec![1, 2, 3, 4], 1_700_000_000_123),
      new_record(vec![9], 1_700_000_000_999),
      new_record(vec![5, 6], 1_699_999_999_999),
    ],
  );

  let summary = batch.summary().expect("summary exists");

  assert_eq!(summary.record_count, 3);
  assert_eq!(summary.payload_bytes, 7);
}

#[test]
fn record_retains_bytes_payload_allocation() {
  let payload = Bytes::from_static(b"payload");
  let payload_pointer = payload.as_ptr();
  let record = new_record(payload, 1_700_000_000_000);

  assert_eq!(record.payload.as_ptr(), payload_pointer);
}

#[test]
fn window_key_formatting() {
  let window = Window::for_timestamp(1_700_000_000, 300);
  let key = window.key("telemetry");

  assert_eq!(key.window_start_unix_seconds, 1_699_999_800);
  assert_eq!(key.format(), "telemetry#1699999800");
}

#[test]
fn unix_timestamp_milliseconds_format_as_rfc3339() {
  assert_eq!(
    format_unix_timestamp_ms(1_700_000_000_000),
    "2023-11-14T22:13:20Z"
  );
}

#[test]
fn invalid_unix_timestamp_milliseconds_are_reported() {
  assert_eq!(
    format_unix_timestamp_ms(i64::MAX),
    format!("invalid Unix timestamp: {} ms", i64::MAX)
  );
}

#[test]
fn snowflake_formatting_is_lex_ordered() {
  let low = SnowflakeId(12);
  let high = SnowflakeId(1234);

  assert!(low.format_lex() < high.format_lex());
  assert_eq!(low.format_lex(), "00000000000000000012");
}

#[test]
fn snowflake_timestamp_decodes_default_epoch_time() {
  let timestamp = time::OffsetDateTime::from_unix_timestamp(1_700_000_000)
    .unwrap()
    .saturating_add(time::Duration::milliseconds(120));
  let snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp);

  assert_eq!(snowflake_id.timestamp(), Some(timestamp));
}

#[test]
fn seq_range_len() {
  let range = SeqRange {
    start: 100,
    end: 105,
  };

  assert_eq!(range.len(), 6);
}
