use super::*;

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
  assert_eq!(summary.min_event_ts_ms, 1_699_999_999_999);
  assert_eq!(summary.max_event_ts_ms, 1_700_000_000_999);
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
fn snowflake_formatting_is_lex_ordered() {
  let low = SnowflakeId(12);
  let high = SnowflakeId(1234);

  assert!(low.format_lex() < high.format_lex());
  assert_eq!(low.format_lex(), "00000000000000000012");
}

#[test]
fn seq_range_len() {
  let range = SeqRange {
    start: 100,
    end: 105,
  };

  assert_eq!(range.len(), 6);
}
