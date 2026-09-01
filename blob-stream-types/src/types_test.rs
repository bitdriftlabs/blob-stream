use super::*;
use bytes::Bytes;
use siphasher::sip::SipHasher13;
use std::hash::Hasher;

#[test]
fn virtual_partition_count_accepts_the_u32_limit() {
  assert_eq!(virtual_partition_count(u32::MAX, 1), Ok(u32::MAX));
}

#[test]
fn virtual_partition_count_rejects_overflow() {
  assert_eq!(
    virtual_partition_count(u32::MAX, 2),
    Err(VirtualPartitionCountError)
  );
}

#[test]
fn default_metadata_publication_lag_is_fifteen_seconds() {
  assert_eq!(DEFAULT_MAX_METADATA_PUBLICATION_LAG, Duration::seconds(15));
}

#[test]
fn default_metadata_window_is_five_minutes() {
  assert_eq!(DEFAULT_METADATA_WINDOW_SIZE, Duration::minutes(5));
}

#[test]
fn partition_hash_matches_legacy_64_bit_little_endian_default_hasher() {
  // These hashes are `DefaultHasher::new()` outputs from Rust 1.98 on 64-bit little-endian
  // targets. They lock the compatible legacy mappings, zero keys, and SipHash-1-3 rounds.
  for (record_key, expected_hash) in [
    (b"".as_slice(), 0xbd60_acb6_58c7_9e45),
    (b"a".as_slice(), 0xbeb9_a6bb_f61b_58b4),
    (b"telemetry".as_slice(), 0x082f_9fc9_2c2b_b490),
    (b"partition-key-177".as_slice(), 0x2555_fd88_a87d_0004),
    (&[0, 1, 2, 3, 255], 0xcd61_9664_a138_6abe),
  ] {
    assert_eq!(partition_hash(record_key), expected_hash);
  }
}

#[test]
fn partition_hash_explicitly_encodes_a_little_endian_u64_slice_length() {
  for record_key in [b"".as_slice(), b"telemetry".as_slice(), &[0, 1, 2, 3, 255]] {
    let mut portable_hasher = SipHasher13::new_with_keys(0, 0);
    portable_hasher.write(&(record_key.len() as u64).to_le_bytes());
    portable_hasher.write(record_key);

    assert_eq!(partition_hash(record_key), portable_hasher.finish());
  }
}

#[test]
fn logical_partition_uses_the_portable_fixed_hash() {
  assert_eq!(logical_partition_for_key(b"partition-key-177", 17), 9);
  assert_eq!(logical_partition_for_key(&[0, 1, 2, 3, 255], 17), 11);
}

#[test]
fn topic_metadata_window_uses_default_when_unset() {
  assert_eq!(
    topic_metadata_window_size(&TopicConfig::default()).unwrap(),
    DEFAULT_METADATA_WINDOW_SIZE
  );
}

#[test]
fn topic_metadata_window_uses_explicit_whole_second_value() {
  let topic = TopicConfig {
    metadata_window_size: Duration::seconds(60).into_proto(),
    ..Default::default()
  };

  assert_eq!(
    topic_metadata_window_size(&topic).unwrap(),
    Duration::seconds(60)
  );
}

#[test]
fn topic_metadata_window_rejects_subsecond_value() {
  let topic = TopicConfig {
    metadata_window_size: Duration::milliseconds(250).into_proto(),
    ..Default::default()
  };

  assert!(topic_metadata_window_size(&topic).is_err());
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
  let window = Window::for_timestamp(
    offset_datetime_from_unix_seconds(1_700_000_000),
    Duration::seconds(300),
  );
  let key = window.key("telemetry");

  assert_eq!(
    window.start,
    offset_datetime_from_unix_seconds(1_699_999_800)
  );
  assert_eq!(window.size, Duration::seconds(300));
  assert_eq!(key.window_start_unix_seconds, 1_699_999_800);
  assert_eq!(key.format(), "telemetry#1699999800");
}

#[test]
#[should_panic(expected = "durable topic window keys require positive whole-second sizes")]
fn window_rejects_subsecond_sizes() {
  let _ = Window::for_timestamp(
    offset_datetime_from_unix_millis(1_700_000_000_375),
    Duration::milliseconds(250),
  );
}

#[test]
fn unix_timestamp_boundaries_convert_to_instants() {
  assert_eq!(
    offset_datetime_from_unix_millis(1_700_000_000_123),
    OffsetDateTime::from_unix_timestamp(1_700_000_000)
      .unwrap()
      .saturating_add(Duration::milliseconds(123))
  );
  assert_eq!(
    offset_datetime_from_unix_seconds(1_700_000_000),
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
  );
}

#[test]
fn checked_unix_millisecond_conversions_preserve_persistence_boundaries() {
  let timestamp =
    offset_datetime_from_unix_millis_checked(1_700_000_000_123).expect("valid persisted timestamp");

  assert_eq!(
    timestamp,
    offset_datetime_from_unix_millis(1_700_000_000_123)
  );
  assert_eq!(
    unix_millis_from_offset_datetime(timestamp).expect("timestamp fits persisted milliseconds"),
    1_700_000_000_123
  );
  let before_epoch = OffsetDateTime::UNIX_EPOCH - Duration::nanoseconds(1);
  assert_eq!(
    unix_millis_from_offset_datetime(before_epoch).expect("timestamp fits persisted milliseconds"),
    -1
  );
  assert!(offset_datetime_from_unix_millis_checked(i64::MAX).is_err());
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
