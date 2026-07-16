#[cfg(test)]
#[path = "./config_test.rs"]
mod tests;

use anyhow::{Result, anyhow, ensure};
use bd_pgv::proto_validate;
pub use blob_stream_proto::protos::blobstream::v1::config::{
  ConsumerGroupConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
};
use log::{debug, trace};

const DEFAULT_WINDOW_SIZE_SECONDS: i64 = 300;
const DEFAULT_IDLE_POLL_DELAY_MS: u64 = 250;
const DEFAULT_MAX_IDLE_POLL_DELAY_MS: u64 = 2_000;
const DEFAULT_PREFETCH_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_METADATA_VISIBILITY_DELAY_MS: u64 = 2_000;
pub const DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS: u64 = 30_000;
const MAX_CANDIDATE_WINDOWS: usize = 32;
const DEFAULT_LEASE_DURATION_MS: i64 = 30_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS: i64 = 10_000;
const DEFAULT_REBALANCE_INTERVAL_MS: i64 = 10_000;
const RESERVED_MEMBER_ID_PREFIX: &str = "__blob_stream_";

//
// ConsumerReadConfig
//

#[must_use]
/// Return the configured read window size in seconds, applying defaults when omitted.
pub fn consumer_window_size_seconds(config: &ConsumerReadConfig) -> i64 {
  config
    .window_size_seconds
    .unwrap_or(DEFAULT_WINDOW_SIZE_SECONDS)
}

#[must_use]
/// Return the base idle poll delay in milliseconds, applying defaults.
pub fn consumer_idle_poll_delay_ms(config: &ConsumerReadConfig) -> u64 {
  config
    .idle_poll_delay_ms
    .unwrap_or(DEFAULT_IDLE_POLL_DELAY_MS)
}

#[must_use]
/// Return the max idle poll delay used by exponential backoff, applying defaults when omitted.
pub fn consumer_max_idle_poll_delay_ms(config: &ConsumerReadConfig) -> u64 {
  config
    .max_idle_poll_delay_ms
    .unwrap_or(DEFAULT_MAX_IDLE_POLL_DELAY_MS)
}

#[must_use]
/// Return the soft-target prefetch RAM budget in bytes.
pub fn consumer_prefetch_max_bytes(config: &ConsumerReadConfig) -> u64 {
  config
    .prefetch_max_bytes
    .unwrap_or(DEFAULT_PREFETCH_MAX_BYTES)
}

#[must_use]
/// Return the delay before newly published metadata ranges become eligible for consumption.
pub fn consumer_metadata_visibility_delay_ms(config: &ConsumerReadConfig) -> u64 {
  config
    .metadata_visibility_delay_ms
    .unwrap_or(DEFAULT_METADATA_VISIBILITY_DELAY_MS)
}

/// Derive the candidate windows needed to cover publication and visibility delay.
pub fn consumer_candidate_window_count(
  config: &ConsumerReadConfig,
  maximum_metadata_publication_lag_ms: u64,
) -> Result<usize> {
  let window_size_ms = u64::try_from(consumer_window_size_seconds(config))
    .map_err(|_| anyhow!("consumer.read.window_size_seconds must be positive"))?
    .checked_mul(1_000)
    .ok_or_else(|| anyhow!("consumer.read.window_size_seconds is too large"))?;
  let coverage_ms = maximum_metadata_publication_lag_ms
    .checked_add(consumer_metadata_visibility_delay_ms(config))
    .ok_or_else(|| anyhow!("metadata availability horizon is too large"))?;
  let trailing_windows = coverage_ms
    .checked_add(window_size_ms.saturating_sub(1))
    .ok_or_else(|| anyhow!("metadata availability horizon is too large"))?
    / window_size_ms;
  let candidate_windows = usize::try_from(trailing_windows.saturating_add(1))
    .map_err(|_| anyhow!("metadata availability horizon has too many windows"))?;
  ensure!(
    candidate_windows <= MAX_CANDIDATE_WINDOWS,
    "metadata availability horizon exceeds {MAX_CANDIDATE_WINDOWS} windows"
  );
  Ok(candidate_windows)
}

#[must_use]
/// Return consumer-group lease duration in milliseconds, applying defaults.
pub fn consumer_lease_duration_ms(config: &ConsumerGroupConfig) -> i64 {
  config
    .lease_duration_ms
    .unwrap_or(DEFAULT_LEASE_DURATION_MS)
}

#[must_use]
/// Return consumer-group heartbeat interval in milliseconds, applying defaults.
pub fn consumer_heartbeat_interval_ms(config: &ConsumerGroupConfig) -> i64 {
  config
    .heartbeat_interval_ms
    .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_MS)
}

#[must_use]
/// Return consumer-group rebalance interval in milliseconds, applying defaults.
pub fn consumer_rebalance_interval_ms(config: &ConsumerGroupConfig) -> i64 {
  config
    .rebalance_interval_ms
    .unwrap_or(DEFAULT_REBALANCE_INTERVAL_MS)
}

/// Validate consumer read configuration.
pub fn validate_read_config(config: &ConsumerReadConfig) -> Result<()> {
  trace!(
    "validating consumer read config: topic={topic}",
    topic = config.topic
  );
  proto_validate::validate(config)?;
  ensure!(
    consumer_max_idle_poll_delay_ms(config) >= consumer_idle_poll_delay_ms(config),
    "consumer.read.max_idle_poll_delay_ms must be greater than or equal to \
     consumer.read.idle_poll_delay_ms"
  );

  Ok(())
}

/// Validate consumer group membership/lease configuration.
pub fn validate_group_config(config: &ConsumerGroupConfig) -> Result<()> {
  trace!(
    "validating consumer group config: topic={}, group_id={}, member_id={}",
    config.topic, config.group_id, config.member_id
  );
  proto_validate::validate(config)?;
  ensure!(
    !config.member_id.starts_with(RESERVED_MEMBER_ID_PREFIX),
    "consumer group member_id uses reserved prefix {RESERVED_MEMBER_ID_PREFIX}"
  );
  Ok(())
}

/// Validate full consumer runtime configuration.
pub fn validate_runtime_config(runtime: &ConsumerRuntimeConfig) -> Result<()> {
  debug!("validating consumer runtime config");
  proto_validate::validate(runtime)?;

  let read = runtime
    .read
    .as_ref()
    .ok_or_else(|| anyhow!("consumer runtime validation failed: missing read config"))?;
  let group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("consumer runtime validation failed: missing group config"))?;

  ensure!(
    read.topic == group.topic,
    "consumer read.topic and group.topic must match"
  );

  Ok(())
}
