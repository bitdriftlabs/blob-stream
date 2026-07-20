use anyhow::{Result, ensure};
use std::time::Duration;

const IDENTITY_HEADER_SIZE: usize = 32;

//
// StressConfig
//

#[derive(Clone, Debug)]
pub struct StressConfig {
  pub broker_count: usize,
  pub producer_count: usize,
  pub consumer_count: usize,
  pub partition_count: u32,
  pub total_records: u64,
  pub payload_size: usize,
  pub producer_max_batch_records: Option<u32>,
  pub producer_max_batch_bytes: Option<u32>,
  pub producer_flush_max_delay: Option<Duration>,
  pub producer_max_request_concurrency: Option<u64>,
  pub producer_submit_concurrency: usize,
  pub broker_flush_max_delay: Duration,
  pub consumer_commit_interval_records: u64,
  pub consumer_lease_duration: Duration,
  pub consumer_heartbeat_interval: Duration,
  pub consumer_rebalance_interval: Duration,
  pub overall_timeout: Duration,
  pub startup_timeout: Duration,
  pub producer_timeout: Duration,
  pub drain_timeout: Duration,
  pub verification_timeout: Duration,
  pub consumer_shutdown_timeout: Duration,
  pub progress_interval: Duration,
  pub max_discrepancy_samples: usize,
}

impl Default for StressConfig {
  fn default() -> Self {
    Self {
      broker_count: 1,
      producer_count: 1,
      consumer_count: 1,
      partition_count: 16,
      total_records: 1_000,
      payload_size: 256,
      producer_max_batch_records: None,
      producer_max_batch_bytes: None,
      producer_flush_max_delay: None,
      producer_max_request_concurrency: None,
      producer_submit_concurrency: 64,
      broker_flush_max_delay: Duration::from_secs(1),
      consumer_commit_interval_records: 100,
      consumer_lease_duration: Duration::from_secs(30),
      consumer_heartbeat_interval: Duration::from_secs(10),
      consumer_rebalance_interval: Duration::from_secs(10),
      overall_timeout: Duration::from_secs(10),
      startup_timeout: Duration::from_secs(30),
      producer_timeout: Duration::from_mins(2),
      drain_timeout: Duration::from_secs(30),
      verification_timeout: Duration::from_mins(2),
      consumer_shutdown_timeout: Duration::from_secs(30),
      progress_interval: Duration::from_secs(1),
      max_discrepancy_samples: 20,
    }
  }
}

impl StressConfig {
  pub fn validate(&self) -> Result<()> {
    ensure!(
      self.broker_count > 0,
      "broker_count must be greater than zero"
    );
    ensure!(
      self.producer_count > 0,
      "producer_count must be greater than zero"
    );
    ensure!(
      self.consumer_count > 0,
      "consumer_count must be greater than zero"
    );
    ensure!(
      self.partition_count > 0,
      "partition_count must be greater than zero"
    );
    ensure!(
      self.payload_size >= IDENTITY_HEADER_SIZE,
      "payload_size must be at least {IDENTITY_HEADER_SIZE} bytes"
    );
    ensure!(
      self
        .producer_max_batch_records
        .is_none_or(|max_batch_records| max_batch_records > 0),
      "producer_max_batch_records must be greater than zero when set"
    );
    ensure!(
      self
        .producer_max_batch_bytes
        .is_none_or(|max_batch_bytes| max_batch_bytes > 0),
      "producer_max_batch_bytes must be greater than zero when set"
    );
    ensure!(
      self
        .producer_flush_max_delay
        .is_none_or(|flush_max_delay| !flush_max_delay.is_zero()),
      "producer_flush_max_delay must be greater than zero when set"
    );
    ensure!(
      self
        .producer_max_request_concurrency
        .is_none_or(|max_request_concurrency| max_request_concurrency > 0),
      "producer_max_request_concurrency must be greater than zero when set"
    );
    ensure!(
      self.producer_submit_concurrency > 0,
      "producer_submit_concurrency must be greater than zero"
    );
    ensure!(
      !self.broker_flush_max_delay.is_zero(),
      "broker_flush_max_delay must be greater than zero"
    );
    ensure!(
      self.consumer_commit_interval_records > 0,
      "consumer_commit_interval_records must be greater than zero"
    );
    ensure!(
      !self.consumer_lease_duration.is_zero(),
      "consumer_lease_duration must be greater than zero"
    );
    ensure!(
      !self.consumer_heartbeat_interval.is_zero(),
      "consumer_heartbeat_interval must be greater than zero"
    );
    ensure!(
      !self.consumer_rebalance_interval.is_zero(),
      "consumer_rebalance_interval must be greater than zero"
    );
    ensure!(
      !self.overall_timeout.is_zero(),
      "overall_timeout must be greater than zero"
    );
    ensure!(
      !self.startup_timeout.is_zero(),
      "startup_timeout must be greater than zero"
    );
    ensure!(
      !self.producer_timeout.is_zero(),
      "producer_timeout must be greater than zero"
    );
    ensure!(
      !self.drain_timeout.is_zero(),
      "drain_timeout must be greater than zero"
    );
    ensure!(
      !self.verification_timeout.is_zero(),
      "verification_timeout must be greater than zero"
    );
    ensure!(
      !self.consumer_shutdown_timeout.is_zero(),
      "consumer_shutdown_timeout must be greater than zero"
    );
    ensure!(
      !self.progress_interval.is_zero(),
      "progress_interval must be greater than zero"
    );

    ensure!(
      self.total_records > 0,
      "total_records must be greater than zero"
    );
    Ok(())
  }
}
