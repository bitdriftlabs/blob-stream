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
pub use blob_stream_types::DEFAULT_MAX_METADATA_PUBLICATION_LAG;
use blob_stream_types::DEFAULT_METADATA_WINDOW_SIZE;
use log::{debug, trace};
use time::Duration;
use time::ext::NumericalDuration;

const DEFAULT_IDLE_POLL_DELAY: Duration = Duration::milliseconds(250);
const DEFAULT_MAX_IDLE_POLL_DELAY: Duration = Duration::seconds(2);
const DEFAULT_PREFETCH_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_METADATA_VISIBILITY_DELAY: Duration = Duration::milliseconds(2_000);
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

#[must_use]
/// Return the configured read window size, applying defaults when omitted.
pub fn consumer_window_size(config: &ConsumerReadConfig) -> Duration {
  config
    .window_size_seconds
    .map_or(DEFAULT_METADATA_WINDOW_SIZE, Duration::seconds)
}

#[must_use]
/// Return the base idle poll delay, applying defaults.
pub fn consumer_idle_poll_delay(config: &ConsumerReadConfig) -> Duration {
  config
    .idle_poll_delay_ms
    .map_or(DEFAULT_IDLE_POLL_DELAY, |milliseconds| {
      Duration::milliseconds(i64::try_from(milliseconds).unwrap_or(i64::MAX))
    })
}

#[must_use]
/// Return the max idle poll delay used by exponential backoff, applying defaults when omitted.
pub fn consumer_max_idle_poll_delay(config: &ConsumerReadConfig) -> Duration {
  config
    .max_idle_poll_delay_ms
    .map_or(DEFAULT_MAX_IDLE_POLL_DELAY, |milliseconds| {
      Duration::milliseconds(i64::try_from(milliseconds).unwrap_or(i64::MAX))
    })
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
  Duration::milliseconds(
    i64::try_from(config.metadata_visibility_delay_ms.unwrap_or_else(|| {
      u64::try_from(DEFAULT_METADATA_VISIBILITY_DELAY.whole_milliseconds()).unwrap_or(u64::MAX)
    }))
    .unwrap_or(i64::MAX),
  )
}

#[must_use]
/// Return the topic publication-lag budget as a typed duration, applying its default when unset.
pub fn topic_max_metadata_publication_lag(config: &TopicConfig) -> Duration {
  config.max_metadata_publication_lag_ms.map_or(
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    |maximum_metadata_publication_lag_ms| {
      Duration::milliseconds(i64::try_from(maximum_metadata_publication_lag_ms).unwrap_or(i64::MAX))
    },
  )
}

#[must_use]
/// Return the consumer clock-skew budget as a typed duration, applying the 10 ms default.
pub fn consumer_max_clock_skew(config: &ConsumerReadConfig) -> Duration {
  Duration::milliseconds(i64::try_from(config.max_clock_skew_ms.unwrap_or(10)).unwrap_or(i64::MAX))
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
) -> Result<usize> {
  let metadata_visibility_delay = crate::consumer::metadata_visibility_delay(
    consumer_metadata_visibility_delay(config),
    consumer_strongly_consistent_metadata_reads(config),
  );
  consumer_candidate_window_count_with_availability_horizon(
    config,
    maximum_metadata_publication_lag.saturating_add(metadata_visibility_delay),
  )
}

/// Derive candidate windows for an already combined, typed availability horizon.
pub fn consumer_candidate_window_count_with_availability_horizon(
  config: &ConsumerReadConfig,
  availability_horizon: Duration,
) -> Result<usize> {
  let window_size_ms = u64::try_from(consumer_window_size(config).whole_milliseconds())
    .map_err(|_| anyhow!("consumer.read.window_size_seconds must be positive"))?;
  let coverage_ms = u64::try_from(availability_horizon.whole_milliseconds()).unwrap_or(u64::MAX);
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
/// Return consumer-group lease duration, applying defaults.
pub fn consumer_lease_duration(config: &ConsumerGroupConfig) -> Duration {
  config
    .lease_duration_ms
    .map_or(DEFAULT_LEASE_DURATION, Duration::milliseconds)
}

#[must_use]
/// Return consumer-group heartbeat interval, applying defaults.
pub fn consumer_heartbeat_interval(config: &ConsumerGroupConfig) -> Duration {
  config
    .heartbeat_interval_ms
    .map_or(DEFAULT_HEARTBEAT_INTERVAL, Duration::milliseconds)
}

#[must_use]
/// Return consumer-group rebalance interval, applying defaults.
pub fn consumer_rebalance_interval(config: &ConsumerGroupConfig) -> Duration {
  config
    .rebalance_interval_ms
    .map_or(DEFAULT_REBALANCE_INTERVAL, Duration::milliseconds)
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
