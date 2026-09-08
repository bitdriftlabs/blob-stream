pub(in crate::consumer) use blob_stream_types::offset_datetime_from_unix_seconds;
use time::Duration;

//
// AvailabilityHorizon
//

/// Bound used to retain metadata that may still be unpublished or not yet safely visible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::consumer) struct AvailabilityHorizon(Duration);

impl AvailabilityHorizon {
  #[must_use]
  /// Combine publication, clock-skew, visibility, and retained-cache-age bounds.
  pub(in crate::consumer) fn new(
    maximum_metadata_publication_lag: Duration,
    maximum_clock_skew: Duration,
    visibility_delay: Duration,
    metadata_cache_max_age: Duration,
  ) -> Self {
    Self(
      maximum_metadata_publication_lag
        .saturating_add(maximum_clock_skew)
        .saturating_add(visibility_delay)
        .saturating_add(metadata_cache_max_age),
    )
  }

  #[must_use]
  /// Return the underlying typed duration for time arithmetic.
  pub(in crate::consumer) fn duration(self) -> Duration {
    self.0
  }
}
