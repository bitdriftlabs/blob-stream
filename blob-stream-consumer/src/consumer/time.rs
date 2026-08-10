use blob_stream_types::format_unix_timestamp_ms;

pub(in crate::consumer) fn metadata_availability_delay_seconds(
  metadata_visibility_delay_ms: u64,
  maximum_metadata_publication_lag_ms: u64,
) -> i64 {
  let delay_ms = maximum_metadata_publication_lag_ms.saturating_add(metadata_visibility_delay_ms);
  let delay_seconds = delay_ms.saturating_add(999) / 1_000;
  i64::try_from(delay_seconds).unwrap_or(i64::MAX)
}

pub(in crate::consumer) fn format_unix_timestamp_seconds(timestamp_seconds: i64) -> String {
  format_unix_timestamp_ms(timestamp_seconds.saturating_mul(1_000))
}
