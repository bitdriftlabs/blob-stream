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
  /// Combine publication, clock-skew, and visibility bounds into one audited duration.
  pub(in crate::consumer) fn new(
    maximum_metadata_publication_lag: Duration,
    maximum_clock_skew: Duration,
    visibility_delay: Duration,
  ) -> Self {
    Self(
      maximum_metadata_publication_lag
        .saturating_add(maximum_clock_skew)
        .saturating_add(visibility_delay),
    )
  }

  #[must_use]
  /// Return the underlying typed duration for time arithmetic.
  pub(in crate::consumer) fn duration(self) -> Duration {
    self.0
  }
}

#[must_use]
/// Return the effective metadata maturity duration for the resolved consistency mode.
pub fn metadata_visibility_delay(
  configured_visibility_delay: Duration,
  strongly_consistent: bool,
) -> Duration {
  if strongly_consistent {
    Duration::ZERO
  } else {
    configured_visibility_delay
  }
}
