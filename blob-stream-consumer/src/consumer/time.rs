use blob_stream_types::format_unix_timestamp_ms;

/// Return Fast's rounded-up metadata-availability horizon in seconds.
///
/// The horizon includes the broker publication deadline, while metadata may not exist yet, and
/// the reader visibility delay, while a returned row may not be replica-visible. Fast and
/// checkpoint-overlap scans must account for both before choosing their lower bound.
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
