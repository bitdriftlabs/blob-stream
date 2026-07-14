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
const DEFAULT_LOOKBACK_WINDOWS: u32 = 2;
const DEFAULT_IDLE_POLL_DELAY_MS: u64 = 250;
const DEFAULT_MAX_IDLE_POLL_DELAY_MS: u64 = 2_000;
const DEFAULT_PREFETCH_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_METADATA_RECOVERY_SCAN_INTERVAL_SECONDS: u64 = 60;
const DEFAULT_LEASE_DURATION_MS: i64 = 30_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS: i64 = 10_000;
const DEFAULT_REBALANCE_INTERVAL_MS: i64 = 10_000;

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
/// Return the number of trailing windows scanned on each read, applying defaults.
pub fn consumer_lookback_windows(config: &ConsumerReadConfig) -> u32 {
  config.lookback_windows.unwrap_or(DEFAULT_LOOKBACK_WINDOWS)
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
/// Return the interval between unbounded metadata recovery scans in seconds.
pub fn consumer_metadata_recovery_scan_interval_seconds(config: &ConsumerReadConfig) -> u64 {
  config
    .metadata_recovery_scan_interval_seconds
    .unwrap_or(DEFAULT_METADATA_RECOVERY_SCAN_INTERVAL_SECONDS)
}

#[must_use]
/// Return whether bounded metadata scans are enabled between recovery scans.
pub fn consumer_metadata_fast_scan_enabled(config: &ConsumerReadConfig) -> bool {
  config.metadata_fast_scan_enabled.unwrap_or(true)
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
