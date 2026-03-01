// blob-stream - consumer configuration
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::{Result, anyhow, ensure};
pub use blob_stream_proto::protos::blobstream::v1::config::{
  ConsumerGroupConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
};

const DEFAULT_WINDOW_SIZE_SECONDS: i64 = 300;
const DEFAULT_LOOKBACK_WINDOWS: u32 = 3;
const DEFAULT_LEASE_DURATION_MS: i64 = 30_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS: i64 = 10_000;
const DEFAULT_REBALANCE_INTERVAL_MS: i64 = 10_000;

//
// ConsumerReadConfig
//

#[must_use]
pub fn consumer_window_size_seconds(config: &ConsumerReadConfig) -> i64 {
  config
    .window_size_seconds
    .unwrap_or(DEFAULT_WINDOW_SIZE_SECONDS)
}

#[must_use]
pub fn consumer_lookback_windows(config: &ConsumerReadConfig) -> u32 {
  config.lookback_windows.unwrap_or(DEFAULT_LOOKBACK_WINDOWS)
}

#[must_use]
pub fn consumer_lease_duration_ms(config: &ConsumerGroupConfig) -> i64 {
  config
    .lease_duration_ms
    .unwrap_or(DEFAULT_LEASE_DURATION_MS)
}

#[must_use]
pub fn consumer_heartbeat_interval_ms(config: &ConsumerGroupConfig) -> i64 {
  config
    .heartbeat_interval_ms
    .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_MS)
}

#[must_use]
pub fn consumer_rebalance_interval_ms(config: &ConsumerGroupConfig) -> i64 {
  config
    .rebalance_interval_ms
    .unwrap_or(DEFAULT_REBALANCE_INTERVAL_MS)
}

pub fn stats_scope(runtime: &ConsumerRuntimeConfig) -> &str {
  if runtime.stats_scope.is_empty() {
    "blob_stream_consumer"
  } else {
    &runtime.stats_scope
  }
}

pub fn validate_read_config(config: &ConsumerReadConfig) -> Result<()> {
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

  Ok(())
}

pub fn validate_group_config(config: &ConsumerGroupConfig) -> Result<()> {
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

pub fn validate_runtime_config(runtime: &ConsumerRuntimeConfig) -> Result<()> {
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
