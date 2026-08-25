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
  TopicConfig,
};
use blob_stream_runtime_config::feature_flag_duration_milliseconds;
pub use blob_stream_types::DEFAULT_MAX_METADATA_PUBLICATION_LAG;
use blob_stream_types::ProtoDurationExt;
use log::{debug, info, trace};
use time::Duration;
use time::ext::NumericalDuration;

const DEFAULT_IDLE_POLL_DELAY: Duration = Duration::milliseconds(250);
const DEFAULT_MAX_IDLE_POLL_DELAY: Duration = Duration::seconds(2);
const DEFAULT_PREFETCH_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_METADATA_VISIBILITY_DELAY: Duration = Duration::milliseconds(2_000);
const DEFAULT_METADATA_CACHE_MAX_AGE: Duration = Duration::milliseconds(250);
const DEFAULT_MAX_IN_FLIGHT_BATCH_READS: u64 = 32;
pub const DEFAULT_MAX_CLOCK_SKEW: Duration = Duration::milliseconds(10);
const MAX_CANDIDATE_WINDOWS: usize = 32;
const DEFAULT_LEASE_DURATION: Duration = Duration::seconds(30);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::seconds(10);
const DEFAULT_REBALANCE_INTERVAL: Duration = Duration::seconds(10);
const RESERVED_MEMBER_ID_PREFIX: &str = "__blob_stream_";
const PREFETCH_MAX_BYTES_FEATURE_FLAG: &str = "blob_stream_consumer_prefetch_max_bytes";
const MAX_IN_FLIGHT_BATCH_READS_FEATURE_FLAG: &str =
  "blob_stream_consumer_max_in_flight_batch_reads";
const STRONG_METADATA_READS_FEATURE_FLAG: &str = "blob_stream_consumer_strong_metadata_reads";
const IDLE_POLL_DELAY_FEATURE_FLAG: &str = "blob_stream_consumer_idle_poll_delay_ms";
const MAX_IDLE_POLL_DELAY_FEATURE_FLAG: &str = "blob_stream_consumer_max_idle_poll_delay_ms";
const LEASE_DURATION_FEATURE_FLAG: &str = "blob_stream_consumer_lease_duration_ms";
const HEARTBEAT_INTERVAL_FEATURE_FLAG: &str = "blob_stream_consumer_heartbeat_interval_ms";
const REBALANCE_INTERVAL_FEATURE_FLAG: &str = "blob_stream_consumer_rebalance_interval_ms";

//
// ConsumerReadConfig
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerReadRuntimeSettings {
  pub(crate) prefetch_max_bytes: u64,
  pub(crate) max_in_flight_batch_reads: usize,
  pub(crate) metadata_read_consistency: MetadataReadConsistency,
  pub(crate) metadata_visibility_delay: Duration,
}

/// Apply startup-only feature flags that affect this consumer process's local scheduling.
pub fn apply_consumer_startup_overrides(
  feature_flags: &FeatureFlagsWatch,
  runtime: &mut ConsumerRuntimeConfig,
) -> Result<()> {
  let read = runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("consumer runtime config is missing read config"))?;
  let group = runtime
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("consumer runtime config is missing group config"))?;

  read.idle_poll_delay = feature_flag_duration_milliseconds(
    feature_flags,
    IDLE_POLL_DELAY_FEATURE_FLAG,
    consumer_idle_poll_delay(read),
  )?;
  read.max_idle_poll_delay = feature_flag_duration_milliseconds(
    feature_flags,
    MAX_IDLE_POLL_DELAY_FEATURE_FLAG,
    consumer_max_idle_poll_delay(read),
  )?;
  group.lease_duration = feature_flag_duration_milliseconds(
    feature_flags,
    LEASE_DURATION_FEATURE_FLAG,
    consumer_lease_duration(group),
  )?;
  group.heartbeat_interval = feature_flag_duration_milliseconds(
    feature_flags,
    HEARTBEAT_INTERVAL_FEATURE_FLAG,
    consumer_heartbeat_interval(group),
  )?;
  group.rebalance_interval = feature_flag_duration_milliseconds(
    feature_flags,
    REBALANCE_INTERVAL_FEATURE_FLAG,
    consumer_rebalance_interval(group),
  )?;
  info!("blob-stream consumer runtime config after applying feature flag overrides: {runtime}");
  Ok(())
}

#[must_use]
/// Return the base idle poll delay, applying defaults.
pub fn consumer_idle_poll_delay(config: &ConsumerReadConfig) -> Duration {
  config
    .idle_poll_delay
    .as_ref()
    .map_or(DEFAULT_IDLE_POLL_DELAY, ProtoDurationExt::to_time_duration)
}

#[must_use]
/// Return the max idle poll delay used by exponential backoff, applying defaults when omitted.
pub fn consumer_max_idle_poll_delay(config: &ConsumerReadConfig) -> Duration {
  config.max_idle_poll_delay.as_ref().map_or(
    DEFAULT_MAX_IDLE_POLL_DELAY,
    ProtoDurationExt::to_time_duration,
  )
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
pub fn consumer_metadata_visibility_delay(config: &ConsumerReadConfig) -> Duration {
  config.metadata_visibility_delay.as_ref().map_or(
    DEFAULT_METADATA_VISIBILITY_DELAY,
    ProtoDurationExt::to_time_duration,
  )
}

#[must_use]
/// Return the topic publication-lag budget as a typed duration, applying its default when unset.
pub fn topic_max_metadata_publication_lag(config: &TopicConfig) -> Duration {
  config.max_metadata_publication_lag.as_ref().map_or(
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    ProtoDurationExt::to_time_duration,
  )
}

#[must_use]
/// Return the shared maximum age for retained eventual broker metadata.
pub fn topic_metadata_cache_max_age(config: &TopicConfig) -> Duration {
  config.metadata_cache_max_age.as_ref().map_or(
    DEFAULT_METADATA_CACHE_MAX_AGE,
    ProtoDurationExt::to_time_duration,
  )
}

#[must_use]
/// Return the consumer clock-skew budget as a typed duration, applying the 10 ms default.
pub fn consumer_max_clock_skew(config: &ConsumerReadConfig) -> Duration {
  config
    .max_clock_skew
    .as_ref()
    .map_or(DEFAULT_MAX_CLOCK_SKEW, ProtoDurationExt::to_time_duration)
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
  let metadata_visibility_delay = crate::consumer::metadata_visibility_delay(
    consumer_metadata_visibility_delay(config),
    strong_metadata_reads,
  );

  ConsumerReadRuntimeSettings {
    prefetch_max_bytes,
    max_in_flight_batch_reads,
    metadata_read_consistency,
    metadata_visibility_delay,
  }
}

/// Derive the candidate windows needed to cover publication and visibility delay.
#[cfg(test)]
pub fn consumer_candidate_window_count(
  config: &ConsumerReadConfig,
  maximum_metadata_publication_lag: Duration,
  metadata_window_size: Duration,
) -> Result<usize> {
  let metadata_visibility_delay = crate::consumer::metadata_visibility_delay(
    consumer_metadata_visibility_delay(config),
    consumer_strongly_consistent_metadata_reads(config),
  );
  consumer_candidate_window_count_with_availability_horizon(
    metadata_window_size,
    maximum_metadata_publication_lag.saturating_add(metadata_visibility_delay),
  )
}

/// Derive candidate windows for an already combined, typed availability horizon.
pub fn consumer_candidate_window_count_with_availability_horizon(
  metadata_window_size: Duration,
  availability_horizon: Duration,
) -> Result<usize> {
  let window_size_ns = u128::try_from(metadata_window_size.whole_nanoseconds())
    .map_err(|_| anyhow!("topic metadata_window_size must be positive"))?;
  let coverage_ns = u128::try_from(availability_horizon.whole_nanoseconds())
    .map_err(|_| anyhow!("metadata availability horizon must not be negative"))?;
  let trailing_windows = coverage_ns
    .checked_add(window_size_ns.saturating_sub(1))
    .ok_or_else(|| anyhow!("metadata availability horizon is too large"))?
    / window_size_ns;
  let candidate_windows = usize::try_from(trailing_windows.saturating_add(1))
    .map_err(|_| anyhow!("metadata availability horizon has too many windows"))?;
  ensure!(
    candidate_windows <= MAX_CANDIDATE_WINDOWS,
    "metadata availability horizon exceeds {MAX_CANDIDATE_WINDOWS} windows"
  );
  Ok(candidate_windows)
}

#[must_use]
/// Return consumer-group lease duration, applying defaults.
pub fn consumer_lease_duration(config: &ConsumerGroupConfig) -> Duration {
  config
    .lease_duration
    .as_ref()
    .map_or(DEFAULT_LEASE_DURATION, ProtoDurationExt::to_time_duration)
}

#[must_use]
/// Return consumer-group heartbeat interval, applying defaults.
pub fn consumer_heartbeat_interval(config: &ConsumerGroupConfig) -> Duration {
  config.heartbeat_interval.as_ref().map_or(
    DEFAULT_HEARTBEAT_INTERVAL,
    ProtoDurationExt::to_time_duration,
  )
}

#[must_use]
/// Return consumer-group rebalance interval, applying defaults.
pub fn consumer_rebalance_interval(config: &ConsumerGroupConfig) -> Duration {
  config.rebalance_interval.as_ref().map_or(
    DEFAULT_REBALANCE_INTERVAL,
    ProtoDurationExt::to_time_duration,
  )
}

/// Validate consumer read configuration.
pub fn validate_read_config(config: &ConsumerReadConfig) -> Result<()> {
  trace!(
    "validating consumer read config: topic={topic}",
    topic = config.topic
  );
  proto_validate::validate(config)?;
  ensure!(
    consumer_max_idle_poll_delay(config) >= consumer_idle_poll_delay(config),
    "consumer.read.max_idle_poll_delay must be greater than or equal to \
     consumer.read.idle_poll_delay"
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
  validate_read_config(read)?;
  validate_group_config(group)?;

  Ok(())
}
