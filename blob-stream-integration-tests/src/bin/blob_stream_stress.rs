use anyhow::Result;
use blob_stream_integration_tests::stress::{StressConfig, run};
use clap::Parser;
use std::time::Duration;

//
// Cli
//

#[derive(Debug, Parser)]
#[command(
  name = "blob-stream-stress",
  about = "Run a local blob-stream S3/Dynamo stress test"
)]
struct Cli {
  #[arg(long, default_value_t = 1)]
  brokers: usize,
  #[arg(long, default_value_t = 1)]
  producers: usize,
  #[arg(long, default_value_t = 1)]
  consumers: usize,
  #[arg(long, default_value_t = 16)]
  partitions: u32,
  #[arg(long, default_value_t = 1_000)]
  records: u64,
  #[arg(long, default_value_t = 256)]
  payload_bytes: usize,
  #[arg(long)]
  producer_max_batch_records: Option<u32>,
  #[arg(long)]
  producer_max_batch_bytes: Option<u32>,
  #[arg(long)]
  producer_flush_max_delay_ms: Option<u64>,
  #[arg(long)]
  producer_max_request_concurrency: Option<u64>,
  #[arg(long, default_value_t = 64)]
  producer_submit_concurrency: usize,
  #[arg(long, default_value_t = 1_000)]
  broker_flush_max_delay_ms: u64,
  #[arg(long, default_value_t = 100)]
  consumer_commit_interval_records: u64,
  #[arg(long, default_value_t = 30_000)]
  consumer_lease_duration_ms: u64,
  #[arg(long, default_value_t = 10_000)]
  consumer_heartbeat_interval_ms: u64,
  #[arg(long, default_value_t = 10_000)]
  consumer_rebalance_interval_ms: u64,
  #[arg(long, default_value_t = 10)]
  overall_timeout_seconds: u64,
  #[arg(long, default_value_t = 30)]
  startup_timeout_seconds: u64,
  #[arg(long, default_value_t = 120)]
  producer_timeout_seconds: u64,
  #[arg(long, default_value_t = 30)]
  drain_timeout_seconds: u64,
  #[arg(long, default_value_t = 120)]
  verification_timeout_seconds: u64,
  #[arg(long, default_value_t = 30)]
  consumer_shutdown_timeout_seconds: u64,
  #[arg(long, default_value_t = 5)]
  progress_interval_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
  let cli = Cli::parse();
  let config = StressConfig {
    broker_count: cli.brokers,
    producer_count: cli.producers,
    consumer_count: cli.consumers,
    partition_count: cli.partitions,
    total_records: cli.records,
    payload_size: cli.payload_bytes,
    producer_max_batch_records: cli.producer_max_batch_records,
    producer_max_batch_bytes: cli.producer_max_batch_bytes,
    producer_flush_max_delay: cli.producer_flush_max_delay_ms.map(Duration::from_millis),
    producer_max_request_concurrency: cli.producer_max_request_concurrency,
    producer_submit_concurrency: cli.producer_submit_concurrency,
    broker_flush_max_delay: Duration::from_millis(cli.broker_flush_max_delay_ms),
    consumer_commit_interval_records: cli.consumer_commit_interval_records,
    consumer_lease_duration: Duration::from_millis(cli.consumer_lease_duration_ms),
    consumer_heartbeat_interval: Duration::from_millis(cli.consumer_heartbeat_interval_ms),
    consumer_rebalance_interval: Duration::from_millis(cli.consumer_rebalance_interval_ms),
    overall_timeout: Duration::from_secs(cli.overall_timeout_seconds),
    startup_timeout: Duration::from_secs(cli.startup_timeout_seconds),
    producer_timeout: Duration::from_secs(cli.producer_timeout_seconds),
    drain_timeout: Duration::from_secs(cli.drain_timeout_seconds),
    verification_timeout: Duration::from_secs(cli.verification_timeout_seconds),
    consumer_shutdown_timeout: Duration::from_secs(cli.consumer_shutdown_timeout_seconds),
    progress_interval: Duration::from_secs(cli.progress_interval_seconds),
    ..Default::default()
  };
  let summary = run(config).await?;

  println!(
    "run_id={} acknowledged={} unique={} duplicates={} missing={} retries={} retry_reasons={:?} \
     producer_errors={} consumer_errors={} inline_unique={} inline_duplicates={} \
     inline_missing={} inline_misrouted={} inline_owned_partitions={} \
     inline_expected_data_partitions={} inline_observed_partitions={}",
    summary.run_id,
    summary.acknowledged_records,
    summary.validation.unique_records,
    summary.validation.duplicate_records,
    summary.validation.missing_records,
    summary.retry_attempts,
    summary.retry_reasons,
    summary.producer_errors.len(),
    summary.consumer_errors.len(),
    summary.inline_validation.validation.unique_records,
    summary.inline_validation.validation.duplicate_records,
    summary.inline_validation.validation.missing_records,
    summary.inline_validation.misrouted_records,
    summary.inline_validation.owned_partitions.len(),
    summary.inline_validation.expected_data_partitions.len(),
    summary.inline_validation.observed_partition_records.len(),
  );
  if !summary.is_success() {
    return Err(anyhow::anyhow!(
      "blob-stream stress run failed: {summary:#?}"
    ));
  }

  Ok(())
}
