//! Shared helpers for applying Blob Stream's local runtime feature flags.

#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

pub mod aws_tracing;

use anyhow::{Result, anyhow};
use bd_runtime_config::feature_flags::{FeatureFlags, FeatureFlagsWatch};
use bd_time::ToProtoDuration;
use protobuf::MessageField;
use protobuf::well_known_types::duration::Duration as ProtoDuration;
use time::Duration;

pub const AWS_S3_TRACE_SAMPLE_RATE: &str = "blob_stream_aws_s3_trace_sample_rate";
pub const AWS_DYNAMODB_TRACE_SAMPLE_RATE: &str = "blob_stream_aws_dynamodb_trace_sample_rate";

//
// AwsTraceSampler
//

/// Live completion sampling. Rates use the feature flag convention of parts per 10,000.
#[derive(Clone, Debug, Default)]
pub struct AwsTraceSampler {
  feature_flags: Option<FeatureFlagsWatch>,
}

impl AwsTraceSampler {
  #[must_use]
  pub fn new(feature_flags: Option<FeatureFlagsWatch>) -> Self {
    Self { feature_flags }
  }

  #[must_use]
  pub fn sample(&self, name: &str) -> bool {
    self.feature_flags.as_ref().is_some_and(|watch| {
      watch
        .borrow()
        .as_deref()
        .is_some_and(|flags| flags.feature_enabled(name, false))
    })
  }
}

/// Read a nonnegative millisecond duration from a feature flag.
pub fn feature_flag_duration_milliseconds(
  feature_flags: &FeatureFlagsWatch,
  name: &str,
  default: Duration,
) -> Result<MessageField<ProtoDuration>> {
  let default = i64::try_from(default.whole_milliseconds())
    .map_err(|_| anyhow!("configured duration for {name} does not fit milliseconds"))?;
  let default = u64::try_from(default)
    .map_err(|_| anyhow!("configured duration for {name} must be nonnegative"))?;
  let value = i64::try_from(feature_flags.get_integer(name, default))
    .map_err(|_| anyhow!("feature flag {name} exceeds i64"))?;
  Ok(Duration::milliseconds(value).into_proto())
}
