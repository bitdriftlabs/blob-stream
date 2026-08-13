#[cfg(test)]
#[path = "./config_test.rs"]
mod tests;

use anyhow::{Result, anyhow, ensure};
use bd_log_util::warn_every;
use bd_pgv::proto_validate;
use bd_runtime_config::feature_flags::{FeatureFlags, FeatureFlagsWatch};
use blob_stream_metadata_store::MetadataReadConsistency;
pub use blob_stream_proto::protos::blobstream::v1::config::{
  ConsumerGroupConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
};
pub use blob_stream_types::DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS;
use blob_stream_types::DEFAULT_METADATA_WINDOW_SIZE_SECONDS;
use log::{debug, trace};
use time::ext::NumericalDuration;

const DEFAULT_IDLE_POLL_DELAY_MS: u64 = 250;
const DEFAULT_MAX_IDLE_POLL_DELAY_MS: u64 = 2_000;
const DEFAULT_PREFETCH_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_METADATA_VISIBILITY_DELAY_MS: u64 = 2_000;
const DEFAULT_MAX_IN_FLIGHT_BATCH_READS: u64 = 32;
const MAX_CANDIDATE_WINDOWS: usize = 32;
const DEFAULT_LEASE_DURATION_MS: i64 = 30_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS: i64 = 10_000;
const DEFAULT_REBALANCE_INTERVAL_MS: i64 = 10_000;
const RESERVED_MEMBER_ID_PREFIX: &str = "__blob_stream_";
const PREFETCH_MAX_BYTES_FEATURE_FLAG: &str = "blob_stream_consumer_prefetch_max_bytes";
const MAX_IN_FLIGHT_BATCH_READS_FEATURE_FLAG: &str =
  "blob_stream_consumer_max_in_flight_batch_reads";
const STRONG_METADATA_READS_FEATURE_FLAG: &str = "blob_stream_consumer_strong_metadata_reads";

//
// ConsumerReadConfig
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerReadRuntimeSettings {
  pub(crate) prefetch_max_bytes: u64,
  pub(crate) max_in_flight_batch_reads: usize,
  pub(crate) metadata_read_consistency: MetadataReadConsistency,
  pub(crate) metadata_visibility_delay_ms: u64,
}

#[must_use]
/// Return the configured read window size in seconds, applying defaults when omitted.
pub fn consumer_window_size_seconds(config: &ConsumerReadConfig) -> i64 {
  config
    .window_size_seconds
    .unwrap_or(DEFAULT_METADATA_WINDOW_SIZE_SECONDS)
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
/// Return the consumer prefetch RAM budget in bytes.
pub fn consumer_prefetch_max_bytes(config: &ConsumerReadConfig) -> u64 {
  config
    .prefetch_max_bytes
    .filter(|prefetch_max_bytes| *prefetch_max_bytes > 0)
    .unwrap_or(DEFAULT_PREFETCH_MAX_BYTES)
}

#[must_use]
/// Return the delay before newly published metadata ranges become eligible for consumption.
pub fn consumer_metadata_visibility_delay_ms(config: &ConsumerReadConfig) -> u64 {
  config
    .metadata_visibility_delay_ms
    .unwrap_or(DEFAULT_METADATA_VISIBILITY_DELAY_MS)
}

#[must_use]
/// Return whether metadata queries are strongly consistent, applying the eventual default.
pub fn consumer_strongly_consistent_metadata_reads(config: &ConsumerReadConfig) -> bool {
  config.strongly_consistent_metadata_reads.unwrap_or(false)
}

#[must_use]
/// Return the maximum concurrent blob range reads and decodes, applying the default when omitted.
pub fn consumer_max_in_flight_batch_reads(config: &ConsumerReadConfig) -> u64 {
  config
    .max_in_flight_batch_reads
    .unwrap_or(DEFAULT_MAX_IN_FLIGHT_BATCH_READS)
}

/// Resolve immutable settings for one consumer scan pass.
pub fn consumer_read_runtime_settings(
  config: &ConsumerReadConfig,
  feature_flags: Option<&FeatureFlagsWatch>,
) -> ConsumerReadRuntimeSettings {
  let configured_prefetch_max_bytes = consumer_prefetch_max_bytes(config);
  let prefetch_max_bytes = feature_flags.map_or(configured_prefetch_max_bytes, |feature_flags| {
    feature_flags.get_integer(
      PREFETCH_MAX_BYTES_FEATURE_FLAG,
      configured_prefetch_max_bytes,
    )
  });
  let prefetch_max_bytes = if prefetch_max_bytes == 0 {
    warn_every!(
      15.seconds(),
      "consumer feature flag {PREFETCH_MAX_BYTES_FEATURE_FLAG} was zero; using configured \
       prefetch_max_bytes={configured_prefetch_max_bytes}"
    );
    configured_prefetch_max_bytes
  } else {
    prefetch_max_bytes
  };

  let configured_max_in_flight_batch_reads = consumer_max_in_flight_batch_reads(config);
  let max_in_flight_batch_reads =
    feature_flags.map_or(configured_max_in_flight_batch_reads, |feature_flags| {
      feature_flags.get_integer(
        MAX_IN_FLIGHT_BATCH_READS_FEATURE_FLAG,
        configured_max_in_flight_batch_reads,
      )
    });
  let max_in_flight_batch_reads = match usize::try_from(max_in_flight_batch_reads) {
    Ok(max_in_flight_batch_reads) if max_in_flight_batch_reads > 0 => max_in_flight_batch_reads,
    Ok(_) | Err(_) => {
      warn_every!(
        15.seconds(),
        "consumer feature flag {MAX_IN_FLIGHT_BATCH_READS_FEATURE_FLAG} was invalid; using \
         configured max_in_flight_batch_reads={configured_max_in_flight_batch_reads}"
      );
      usize::try_from(configured_max_in_flight_batch_reads).unwrap_or(usize::MAX)
    },
  };

  let configured_strong_metadata_reads = consumer_strongly_consistent_metadata_reads(config);
  let strong_metadata_reads = feature_flags.map_or(configured_strong_metadata_reads, |flags| {
    flags.get_bool(
      STRONG_METADATA_READS_FEATURE_FLAG,
      configured_strong_metadata_reads,
    )
  });
  let metadata_read_consistency = if strong_metadata_reads {
    MetadataReadConsistency::Strong
  } else {
    MetadataReadConsistency::Eventual
  };
  let metadata_visibility_delay_ms = if strong_metadata_reads {
    0
  } else {
    consumer_metadata_visibility_delay_ms(config)
  };

  ConsumerReadRuntimeSettings {
    prefetch_max_bytes,
    max_in_flight_batch_reads,
    metadata_read_consistency,
    metadata_visibility_delay_ms,
  }
}

/// Derive the candidate windows needed to cover publication and visibility delay.
#[cfg(test)]
pub fn consumer_candidate_window_count(
  config: &ConsumerReadConfig,
  maximum_metadata_publication_lag_ms: u64,
) -> Result<usize> {
  let metadata_visibility_delay_ms = if consumer_strongly_consistent_metadata_reads(config) {
    0
  } else {
    consumer_metadata_visibility_delay_ms(config)
  };
  consumer_candidate_window_count_with_visibility_delay(
    config,
    maximum_metadata_publication_lag_ms,
    metadata_visibility_delay_ms,
  )
}

/// Derive candidate windows for an explicit per-pass effective visibility delay.
pub fn consumer_candidate_window_count_with_visibility_delay(
  config: &ConsumerReadConfig,
  maximum_metadata_publication_lag_ms: u64,
  metadata_visibility_delay_ms: u64,
) -> Result<usize> {
  let window_size_ms = u64::try_from(consumer_window_size_seconds(config))
    .map_err(|_| anyhow!("consumer.read.window_size_seconds must be positive"))?
    .checked_mul(1_000)
    .ok_or_else(|| anyhow!("consumer.read.window_size_seconds is too large"))?;
  let coverage_ms = maximum_metadata_publication_lag_ms
    .checked_add(metadata_visibility_delay_ms)
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
