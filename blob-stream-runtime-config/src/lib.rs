//! Shared helpers for applying Blob Stream's local runtime feature flags.

use anyhow::{Result, anyhow};
use bd_runtime_config::feature_flags::{FeatureFlags, FeatureFlagsWatch};
use bd_time::ToProtoDuration;
use protobuf::MessageField;
use protobuf::well_known_types::duration::Duration as ProtoDuration;
use time::Duration;

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
