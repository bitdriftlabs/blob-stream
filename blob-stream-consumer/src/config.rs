#[cfg(test)]
#[path = "./config_test.rs"]
mod tests;

use anyhow::{Result, anyhow, ensure};
pub use blob_stream_proto::protos::blobstream::v1::config::{
  ConsumerGroupConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
};
use log::{debug, trace};

const DEFAULT_WINDOW_SIZE_SECONDS: i64 = 300;
const DEFAULT_LOOKBACK_WINDOWS: u32 = 3;
const DEFAULT_IDLE_POLL_DELAY_MS: u64 = 250;
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
/// Return the optional max idle poll delay used by exponential backoff.
pub fn consumer_max_idle_poll_delay_ms(config: &ConsumerReadConfig) -> Option<u64> {
  config.max_idle_poll_delay_ms
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
  trace!("validating consumer read config: topic={}", config.topic);
  ensure!(
    !config.topic.trim().is_empty(),
    "consumer topic is required"
  );
  ensure!(
    consumer_window_size_seconds(config) > 0,
    "consumer.read.window_size_seconds must be greater than zero"
  );
  ensure!(
    consumer_lookback_windows(config) > 0,
    "consumer.read.lookback_windows must be greater than zero"
  );
  ensure!(
    consumer_idle_poll_delay_ms(config) > 0,
    "consumer.read.idle_poll_delay_ms must be greater than zero"
  );
  if let Some(max_idle_poll_delay_ms) = consumer_max_idle_poll_delay_ms(config) {
    ensure!(
      max_idle_poll_delay_ms >= consumer_idle_poll_delay_ms(config),
      "consumer.read.max_idle_poll_delay_ms must be greater than or equal to \
       consumer.read.idle_poll_delay_ms"
    );
  }

  Ok(())
}

/// Validate consumer group membership/lease configuration.
pub fn validate_group_config(config: &ConsumerGroupConfig) -> Result<()> {
  trace!(
    "validating consumer group config: topic={}, group_id={}, member_id={}",
    config.topic, config.group_id, config.member_id
  );
  ensure!(
    !config.topic.trim().is_empty(),
    "consumer group topic is required"
  );
  ensure!(
    !config.group_id.trim().is_empty(),
    "consumer group id is required"
  );
  ensure!(
    !config.member_id.trim().is_empty(),
    "consumer member id is required"
  );
  ensure!(
    consumer_lease_duration_ms(config) > 0,
    "consumer.group.lease_duration_ms must be greater than zero"
  );
  ensure!(
    consumer_heartbeat_interval_ms(config) > 0,
    "consumer.group.heartbeat_interval_ms must be greater than zero"
  );
  ensure!(
    consumer_rebalance_interval_ms(config) > 0,
    "consumer.group.rebalance_interval_ms must be greater than zero"
  );
  Ok(())
}

/// Validate full consumer runtime configuration.
pub fn validate_runtime_config(runtime: &ConsumerRuntimeConfig) -> Result<()> {
  debug!("validating consumer runtime config");
  let read = runtime
    .read
    .as_ref()
    .ok_or_else(|| anyhow!("consumer read config is required"))?;
  validate_read_config(read)?;

  let group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("consumer group config is required"))?;
  validate_group_config(group)?;

  ensure!(
    read.topic == group.topic,
    "consumer read.topic and group.topic must match"
  );

  Ok(())
}
