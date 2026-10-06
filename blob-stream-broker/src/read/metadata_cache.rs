#[cfg(test)]
#[path = "./metadata_cache_test.rs"]
mod tests;

use anyhow::{Result, anyhow, ensure};
use bd_log_util::warn_every;
use bd_runtime_config::feature_flags::{FeatureFlags, FeatureFlagsWatch, WatchedFeatureFlags};
use bd_shutdown::ComponentShutdownTriggerHandle;
use bd_time::{SystemTimeProvider, TimeProvider};
use blob_stream_metadata_store::{
  MetadataQueryThrottled,
  MetadataReadConsistency as StoreConsistency,
  MetadataStore,
  SegmentMetadata,
  encode_segment_metadata_v1,
};
use blob_stream_proto::protos::blobstream::v1::broker::{
  BrokerSegmentMetadata,
  FullRecoveryMetadataCoverage,
  MetadataReadConsistency,
  MetadataReadFailure,
  MetadataReadFailureStatus,
  MetadataReadOverloadReason,
  MetadataReadSuccess,
  ReadMetadataWindowRequest,
  ReadMetadataWindowResponse,
  TailMetadataCoverage,
  read_metadata_window_request,
  read_metadata_window_response,
};
use blob_stream_proto::protos::blobstream::v1::config::{RuntimeConfig, TopicConfig};
use blob_stream_types::{
  DEFAULT_MAX_METADATA_PUBLICATION_LAG,
  ProtoDurationExt,
  SnowflakeId,
  TopicWindowKey,
  topic_metadata_window_size,
};
use im::Vector;
use log::debug;
use moka::future::Cache;
use moka::ops::compute::{CompResult, Op};
use parking_lot::Mutex;
use protobuf::Message;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::future::ready;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration as StdDuration;
use time::ext::NumericalDuration;
use time::{Duration, OffsetDateTime};
use tokio::sync::Notify;

const DEFAULT_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_COALESCING_WINDOW: Duration = Duration::milliseconds(250);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::seconds(5);
const DEFAULT_METADATA_CACHE_MAX_AGE: Duration = Duration::milliseconds(250);
const DEFAULT_SEAL_MAX_CLOCK_SKEW: Duration = Duration::milliseconds(10);
const DEFAULT_RECOVERY_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_WAITERS_PER_KEY: usize = 128;
const DEFAULT_MAX_WAITERS: usize = 4096;
const DEFAULT_MAX_REFILLS: usize = 32;
const DEFAULT_MAX_REQUEST_PARTITIONS: usize = 1024;
const DEFAULT_MAX_RESPONSE_ITEMS: usize = 10_000;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_ENTRY_ITEMS: usize = 100_000;
const CACHE_IDLE_TTL: StdDuration = StdDuration::from_mins(5);
const CACHE_MAINTENANCE_INTERVAL: StdDuration = StdDuration::from_mins(1);
pub const MAX_METADATA_READ_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const RECOVERY_CACHE_MAX_BYTES_FEATURE_FLAG: &str =
  "blob_stream_broker_metadata_recovery_cache_max_bytes";
const MAX_WAITERS_PER_KEY_FEATURE_FLAG: &str =
  "blob_stream_broker_metadata_cache_max_waiters_per_key";
const MAX_WAITERS_FEATURE_FLAG: &str = "blob_stream_broker_metadata_cache_max_waiters";
const MAX_REFILLS_FEATURE_FLAG: &str = "blob_stream_broker_metadata_cache_max_refills";
const MAX_REQUEST_PARTITIONS_FEATURE_FLAG: &str =
  "blob_stream_broker_metadata_cache_max_request_partitions";
const MAX_RESPONSE_ITEMS_FEATURE_FLAG: &str =
  "blob_stream_broker_metadata_cache_max_response_items";
const MAX_RESPONSE_BYTES_FEATURE_FLAG: &str =
  "blob_stream_broker_metadata_cache_max_response_bytes";
const MAX_ENTRY_ITEMS_FEATURE_FLAG: &str = "blob_stream_broker_metadata_cache_max_entry_items";
const REQUEST_TIMEOUT_FEATURE_FLAG: &str = "blob_stream_broker_metadata_cache_request_timeout_ms";

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct MetadataCacheOverload {
  reason: MetadataReadOverloadReason,
  message: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MetadataCacheLimits {
  request_timeout: StdDuration,
  max_waiters_per_key: usize,
  max_waiters: usize,
  max_refills: usize,
  max_request_partitions: usize,
  max_response_items: usize,
  max_response_bytes: u64,
  max_entry_items: usize,
}

impl MetadataCacheLimits {
  fn from_flags(
    flags: &dyn FeatureFlags,
    defaults: Self,
    coalescing_window: StdDuration,
  ) -> Result<Self> {
    let default_timeout_ms =
      u64::try_from(defaults.request_timeout.as_millis()).unwrap_or(u64::MAX);
    let timeout_ms = flags.get_integer(REQUEST_TIMEOUT_FEATURE_FLAG, default_timeout_ms);
    ensure!(
      timeout_ms == default_timeout_ms || (1 ..= 60_000).contains(&timeout_ms),
      "invalid metadata request timeout"
    );
    let limits = Self {
      request_timeout: if timeout_ms == default_timeout_ms {
        defaults.request_timeout
      } else {
        StdDuration::from_millis(timeout_ms)
      },
      max_waiters_per_key: feature_flag_limit(
        Some(flags),
        MAX_WAITERS_PER_KEY_FEATURE_FLAG,
        defaults.max_waiters_per_key,
      )?,
      max_waiters: feature_flag_limit(Some(flags), MAX_WAITERS_FEATURE_FLAG, defaults.max_waiters)?,
      max_refills: feature_flag_limit(Some(flags), MAX_REFILLS_FEATURE_FLAG, defaults.max_refills)?,
      max_request_partitions: feature_flag_limit(
        Some(flags),
        MAX_REQUEST_PARTITIONS_FEATURE_FLAG,
        defaults.max_request_partitions,
      )?,
      max_response_items: feature_flag_limit(
        Some(flags),
        MAX_RESPONSE_ITEMS_FEATURE_FLAG,
        defaults.max_response_items,
      )?,
      max_response_bytes: feature_flag_bytes(
        Some(flags),
        MAX_RESPONSE_BYTES_FEATURE_FLAG,
        defaults.max_response_bytes,
      )?,
      max_entry_items: feature_flag_limit(
        Some(flags),
        MAX_ENTRY_ITEMS_FEATURE_FLAG,
        defaults.max_entry_items,
      )?,
    };
    ensure!(
      coalescing_window <= limits.request_timeout / 2,
      "metadata coalescing window exceeds half the request timeout"
    );
    ensure!(
      limits.max_waiters_per_key <= limits.max_waiters,
      "per-key waiters exceed global cap"
    );
    ensure!(
      limits.max_refills <= 1024,
      "metadata refill concurrency exceeds 1024"
    );
    ensure!(
      limits.max_response_bytes <= MAX_METADATA_READ_REQUEST_BYTES as u64,
      "metadata response limit exceeds transport cap"
    );
    Ok(limits)
  }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadCoverage {
  Tail,
  FullRecovery,
}

//
// MetadataCacheConfig
//

#[derive(Clone)]
pub struct MetadataCacheConfig {
  tail_max_bytes: u64,
  recovery_max_bytes: u64,
  seal_clock_skew: Duration,
  coalescing_window: StdDuration,
  limits: MetadataCacheLimits,
  topics: HashMap<String, TopicCacheContract>,
}

#[derive(Clone)]
struct TopicCacheContract {
  partition_count: u32,
  num_writers: u32,
  metadata_window_size: Duration,
  metadata_cache_max_age: Duration,
  max_metadata_publication_lag: Duration,
}

impl MetadataCacheConfig {
  pub fn from_runtime_config(
    config: &RuntimeConfig,
    feature_flags: Option<&FeatureFlagsWatch>,
  ) -> Result<Self> {
    let feature_flags = feature_flags.map(|flags| flags as &dyn FeatureFlags);
    let broker = config
      .broker
      .as_ref()
      .ok_or_else(|| anyhow!("runtime config missing broker config"))?;
    let mut topics = HashMap::new();
    for topic in &config.topics {
      let contract = TopicCacheContract::from_proto(topic)?;
      if topics.insert(topic.name.to_string(), contract).is_some() {
        return Err(anyhow!("duplicate topic: {}", topic.name));
      }
    }

    let request_timeout = broker
      .metadata_cache_request_timeout
      .as_ref()
      .map_or(DEFAULT_REQUEST_TIMEOUT, ProtoDurationExt::to_time_duration);
    let coalescing_window = broker.metadata_cache_coalescing_window.as_ref().map_or(
      DEFAULT_COALESCING_WINDOW,
      ProtoDurationExt::to_time_duration,
    );
    ensure!(
      !coalescing_window.is_negative(),
      "broker metadata_cache_coalescing_window must not be negative"
    );
    ensure!(
      coalescing_window < request_timeout,
      "broker metadata_cache_coalescing_window must be less than metadata_cache_request_timeout"
    );
    let request_timeout = StdDuration::try_from(request_timeout)
      .map_err(|_| anyhow!("metadata_cache_request_timeout exceeds supported range"))?;
    let coalescing_window = StdDuration::try_from(coalescing_window)
      .map_err(|_| anyhow!("metadata_cache_coalescing_window exceeds supported range"))?;
    ensure!(
      coalescing_window <= request_timeout / 2,
      // Reserve at least half the request budget for refilling storage and encoding the response
      // after compatible requests have had time to coalesce.
      "broker metadata_cache_coalescing_window must leave half the request timeout for refill and \
       response"
    );
    let limits = MetadataCacheLimits {
      request_timeout,
      max_waiters_per_key: DEFAULT_MAX_WAITERS_PER_KEY,
      max_waiters: DEFAULT_MAX_WAITERS,
      max_refills: DEFAULT_MAX_REFILLS,
      max_request_partitions: DEFAULT_MAX_REQUEST_PARTITIONS,
      max_response_items: DEFAULT_MAX_RESPONSE_ITEMS,
      max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
      max_entry_items: DEFAULT_MAX_ENTRY_ITEMS,
    };
    let seal_clock_skew = broker.metadata_seal_max_clock_skew.as_ref().map_or(
      DEFAULT_SEAL_MAX_CLOCK_SKEW,
      ProtoDurationExt::to_time_duration,
    );
    ensure!(
      seal_clock_skew.is_positive(),
      "metadata seal clock skew must be positive"
    );
    if let Some(flags) = feature_flags {
      MetadataCacheLimits::from_flags(flags, limits, coalescing_window)?;
    }
    Ok(Self {
      tail_max_bytes: broker
        .metadata_cache_max_bytes
        .unwrap_or(DEFAULT_CACHE_MAX_BYTES),
      recovery_max_bytes: feature_flag_bytes(
        feature_flags,
        RECOVERY_CACHE_MAX_BYTES_FEATURE_FLAG,
        DEFAULT_RECOVERY_CACHE_MAX_BYTES,
      )?,
      seal_clock_skew,
      coalescing_window,
      limits,
      topics,
    })
  }
}

fn feature_flag_limit(
  feature_flags: Option<&dyn FeatureFlags>,
  name: &str,
  default: usize,
) -> Result<usize> {
  let default = u64::try_from(default).unwrap_or(u64::MAX);
  let value = feature_flags.map_or(default, |feature_flags| {
    feature_flags.get_integer(name, default)
  });
  let value = usize::try_from(value).map_err(|_| anyhow!("feature flag {name} exceeds usize"))?;
  ensure!(value > 0, "feature flag {name} must be greater than zero");
  Ok(value)
}

fn feature_flag_bytes(
  feature_flags: Option<&dyn FeatureFlags>,
  name: &str,
  default: u64,
) -> Result<u64> {
  let value = feature_flags.map_or(default, |feature_flags| {
    feature_flags.get_integer(name, default)
  });
  ensure!(value > 0, "feature flag {name} must be greater than zero");
  Ok(value)
}

impl TopicCacheContract {
  fn from_proto(topic: &TopicConfig) -> Result<Self> {
    let metadata_cache_max_age = topic.metadata_cache_max_age.as_ref().map_or(
      DEFAULT_METADATA_CACHE_MAX_AGE,
      ProtoDurationExt::to_time_duration,
    );
    ensure!(
      !metadata_cache_max_age.is_negative(),
      "topic {} metadata_cache_max_age must not be negative",
      topic.name
    );
    Ok(Self {
      partition_count: topic.partition_count,
      num_writers: topic.num_writers,
      metadata_window_size: topic_metadata_window_size(topic)?,
      metadata_cache_max_age,
      max_metadata_publication_lag: topic.max_metadata_publication_lag.as_ref().map_or(
        DEFAULT_MAX_METADATA_PUBLICATION_LAG,
        ProtoDurationExt::to_time_duration,
      ),
    })
  }

  fn is_valid_partition(&self, partition_id: u32) -> bool {
    u64::from(partition_id) < u64::from(self.partition_count) * u64::from(self.num_writers)
  }
}

//
// MetadataCache
//

/// Shares eventual metadata scans across compatible requests while keeping strong reads uncached.
///
/// Tail bounds and requested partitions are intentionally absent from the cache key. A refill
/// scans from the lowest pending Tail bound, then each caller receives only its own projection.
pub struct MetadataCache {
  metadata_store: Arc<dyn MetadataStore>,
  time_provider: Arc<dyn TimeProvider>,
  config: MetadataCacheConfig,
  last_valid_limits: Mutex<WatchedFeatureFlags<MetadataCacheLimits>>,
  eventual_tail_entries: Cache<CacheKey, Arc<CacheEntry>>,
  eventual_recovery_entries: Cache<CacheKey, Arc<CacheEntry>>,
  strong_sealed_entries: Cache<CacheKey, Arc<CacheEntry>>,
  // Registration is synchronous and short-lived; refills run outside this lock.
  in_flight: Mutex<HashMap<CacheKey, Arc<PendingRefill>>>,
  strong_suffix_in_flight: Mutex<HashMap<StrongSuffixKey, Arc<PendingRefill>>>,
  active_waiters: Arc<AtomicUsize>,
  active_refills: Arc<AtomicUsize>,
  tail_retained_metrics: RetainedCacheMetrics,
  recovery_retained_metrics: RetainedCacheMetrics,
  generation: AtomicU64,
  failures: AtomicU64,
  metrics: MetadataCacheMetrics,
}

//
// MetadataCacheMetrics
//

#[derive(Clone)]
struct MetadataCacheMetrics {
  requests: prometheus::IntCounter,
  storage_queries: prometheus::IntCounter,
  tail_hits: prometheus::IntCounter,
  recovery_hits: prometheus::IntCounter,
  tail_refills: prometheus::IntCounter,
  recovery_baselines: prometheus::IntCounter,
  recovery_seals: prometheus::IntCounter,
  strong_sealed_installs: prometheus::IntCounter,
  strong_sealed_reuses: prometheus::IntCounter,
  strong_suffix_queries: prometheus::IntCounter,
  invalidations: prometheus::IntCounter,
  evictions: prometheus::IntCounter,
  failures: prometheus::IntCounter,
  overloads: prometheus::IntCounter,
  response_items: prometheus::IntCounter,
  response_bytes: prometheus::IntCounter,
  observation_age_seconds: prometheus::Histogram,
  coalescing_window_requests: prometheus::IntCounter,
  active_waiters: prometheus::IntGauge,
  active_refills: prometheus::IntGauge,
  tail_entries: prometheus::IntGauge,
  recovery_entries: prometheus::IntGauge,
  tail_retained_bytes: prometheus::IntGauge,
  recovery_retained_bytes: prometheus::IntGauge,
  strong_sealed_entries: prometheus::IntGauge,
  strong_sealed_retained_bytes: prometheus::IntGauge,
}

impl MetadataCacheMetrics {
  fn new(scope: &bd_server_stats::stats::Scope) -> Self {
    let scope = scope.scope("metadata_cache");
    Self {
      requests: scope.counter("requests_total"),
      storage_queries: scope.counter("storage_queries_total"),
      tail_hits: scope.counter("tail_hits_total"),
      recovery_hits: scope.counter("recovery_hits_total"),
      tail_refills: scope.counter("tail_refills_total"),
      recovery_baselines: scope.counter("recovery_baselines_total"),
      recovery_seals: scope.counter("recovery_seals_total"),
      strong_sealed_installs: scope.counter("strong_sealed_installs_total"),
      strong_sealed_reuses: scope.counter("strong_sealed_reuses_total"),
      strong_suffix_queries: scope.counter("strong_suffix_queries_total"),
      invalidations: scope.counter("invalidations_total"),
      evictions: scope.counter("evictions_total"),
      failures: scope.counter("failures_total"),
      overloads: scope.counter("overloads_total"),
      response_items: scope.counter("response_items_total"),
      response_bytes: scope.counter("response_bytes_total"),
      observation_age_seconds: scope.histogram("observation_age_seconds"),
      coalescing_window_requests: scope.counter("coalescing_window_requests_total"),
      active_waiters: scope.gauge("active_waiters"),
      active_refills: scope.gauge("active_refills"),
      tail_entries: scope.gauge("tail_entries"),
      recovery_entries: scope.gauge("recovery_entries"),
      tail_retained_bytes: scope.gauge("tail_retained_bytes"),
      recovery_retained_bytes: scope.gauge("recovery_retained_bytes"),
      strong_sealed_entries: scope.gauge("strong_sealed_entries"),
      strong_sealed_retained_bytes: scope.gauge("strong_sealed_retained_bytes"),
    }
  }
}

//
// RetainedCacheMetrics
//

/// Cheap cache-capacity accounting for Prometheus scrapes.
///
/// Moka applies policy maintenance asynchronously, so exact cache introspection belongs to the
/// admin snapshot. Insertions and eviction callbacks keep these gauges current enough for normal
/// metrics collection without forcing maintenance in the metadata read hot path.
#[derive(Clone)]
struct RetainedCacheMetrics {
  entries: Arc<AtomicUsize>,
  retained_bytes: Arc<AtomicU64>,
  entries_gauge: prometheus::IntGauge,
  retained_bytes_gauge: prometheus::IntGauge,
}

impl RetainedCacheMetrics {
  fn new(entries_gauge: prometheus::IntGauge, retained_bytes_gauge: prometheus::IntGauge) -> Self {
    Self {
      entries: Arc::new(AtomicUsize::new(0)),
      retained_bytes: Arc::new(AtomicU64::new(0)),
      entries_gauge,
      retained_bytes_gauge,
    }
  }

  fn record_insert(&self, retained_bytes: u32) {
    self.entries.fetch_add(1, Ordering::Relaxed);
    self
      .retained_bytes
      .fetch_add(u64::from(retained_bytes), Ordering::Relaxed);
    self.record_gauges();
  }

  fn record_eviction(&self, retained_bytes: u32) {
    let _ = self
      .entries
      .try_update(Ordering::Relaxed, Ordering::Relaxed, |entries| {
        Some(entries.saturating_sub(1))
      });
    let _ = self
      .retained_bytes
      .try_update(Ordering::Relaxed, Ordering::Relaxed, |bytes| {
        Some(bytes.saturating_sub(u64::from(retained_bytes)))
      });
    self.record_gauges();
  }

  fn record_exact(&self, entries: u64, retained_bytes: u64) {
    self.entries.store(
      usize::try_from(entries).unwrap_or(usize::MAX),
      Ordering::Relaxed,
    );
    self.retained_bytes.store(retained_bytes, Ordering::Relaxed);
    self.record_gauges();
  }

  fn record_gauges(&self) {
    self
      .entries_gauge
      .set(i64::try_from(self.entries.load(Ordering::Relaxed)).unwrap_or(i64::MAX));
    self
      .retained_bytes_gauge
      .set(i64::try_from(self.retained_bytes.load(Ordering::Relaxed)).unwrap_or(i64::MAX));
  }
}

//
// MetadataCacheSnapshot
//

/// Aggregate metadata-cache state suitable for the bounded broker admin endpoint.
#[derive(Debug, Serialize)]
pub struct MetadataCacheSnapshot {
  pub tail_entry_count: u64,
  pub tail_retained_bytes: u64,
  pub tail_byte_budget: u64,
  pub recovery_entry_count: u64,
  pub recovery_retained_bytes: u64,
  pub recovery_byte_budget: u64,
  pub strong_sealed_entry_count: u64,
  pub strong_sealed_retained_bytes: u64,
  pub strong_sealed_byte_budget: u64,
  pub tail_oldest_entry_age_seconds: Option<u64>,
  pub recovery_oldest_entry_age_seconds: Option<u64>,
  pub in_flight_refills: usize,
  pub active_waiters: usize,
  pub available_refill_permits: usize,
  pub failure_total: u64,
}

impl MetadataCache {
  #[must_use]
  pub fn new(
    metadata_store: Arc<dyn MetadataStore>,
    config: MetadataCacheConfig,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Self {
    let collector = bd_server_stats::stats::Collector::default();
    Self::new_inner(
      metadata_store,
      config,
      feature_flags,
      &collector.scope("blob_stream_broker"),
    )
  }

  #[must_use]
  /// Override the clock used for cache entry age and coalescing waits.
  pub fn time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
    self.time_provider = time_provider;
    self
  }

  #[must_use]
  pub fn new_with_metrics(
    metadata_store: Arc<dyn MetadataStore>,
    config: MetadataCacheConfig,
    feature_flags: Option<FeatureFlagsWatch>,
    shutdown_trigger_handle: &ComponentShutdownTriggerHandle,
    metrics_scope: &bd_server_stats::stats::Scope,
  ) -> Arc<Self> {
    let cache = Arc::new(Self::new_inner(
      metadata_store,
      config,
      feature_flags,
      metrics_scope,
    ));
    cache.spawn_maintenance(shutdown_trigger_handle);
    cache
  }

  fn new_inner(
    metadata_store: Arc<dyn MetadataStore>,
    config: MetadataCacheConfig,
    feature_flags: Option<FeatureFlagsWatch>,
    metrics_scope: &bd_server_stats::stats::Scope,
  ) -> Self {
    let initial_limits = WatchedFeatureFlags::new(feature_flags, Arc::new(config.limits));
    let metrics = MetadataCacheMetrics::new(metrics_scope);
    let strong_sealed_byte_budget = config
      .tail_max_bytes
      .saturating_div(2)
      .saturating_add(config.recovery_max_bytes.saturating_div(2));
    let tail_retained_metrics = RetainedCacheMetrics::new(
      metrics.tail_entries.clone(),
      metrics.tail_retained_bytes.clone(),
    );
    let tail_evictions = metrics.evictions.clone();
    let tail_eviction_metrics = tail_retained_metrics.clone();
    let eventual_tail_entries = Cache::builder()
      .max_capacity(
        config
          .tail_max_bytes
          .saturating_sub(config.tail_max_bytes / 2),
      )
      .time_to_idle(CACHE_IDLE_TTL)
      .weigher(|_key: &CacheKey, entry: &Arc<CacheEntry>| entry.retained_bytes)
      .eviction_listener(move |_key, entry, cause| {
        if cause.was_evicted() {
          tail_evictions.inc();
        }
        tail_eviction_metrics.record_eviction(entry.retained_bytes);
      })
      .build();
    let recovery_retained_metrics = RetainedCacheMetrics::new(
      metrics.recovery_entries.clone(),
      metrics.recovery_retained_bytes.clone(),
    );
    let recovery_evictions = metrics.evictions.clone();
    let recovery_eviction_metrics = recovery_retained_metrics.clone();
    let eventual_recovery_entries = Cache::builder()
      .max_capacity(
        config
          .recovery_max_bytes
          .saturating_sub(config.recovery_max_bytes / 2),
      )
      .time_to_idle(CACHE_IDLE_TTL)
      .weigher(|_key: &CacheKey, entry: &Arc<CacheEntry>| entry.retained_bytes)
      .eviction_listener(move |_key, entry, cause| {
        if cause.was_evicted() {
          recovery_evictions.inc();
        }
        recovery_eviction_metrics.record_eviction(entry.retained_bytes);
      })
      .build();
    let strong_evictions = metrics.evictions.clone();
    let strong_sealed_entries = Cache::builder()
      .max_capacity(strong_sealed_byte_budget)
      .time_to_idle(CACHE_IDLE_TTL)
      .weigher(|_key: &CacheKey, entry: &Arc<CacheEntry>| entry.retained_bytes)
      .eviction_listener(move |_key, _entry, cause| {
        if cause.was_evicted() {
          strong_evictions.inc();
        }
      })
      .build();
    Self {
      metadata_store,
      time_provider: Arc::new(SystemTimeProvider),
      last_valid_limits: Mutex::new(initial_limits),
      config,
      eventual_tail_entries,
      eventual_recovery_entries,
      strong_sealed_entries,
      in_flight: Mutex::new(HashMap::new()),
      strong_suffix_in_flight: Mutex::new(HashMap::new()),
      active_waiters: Arc::new(AtomicUsize::new(0)),
      active_refills: Arc::new(AtomicUsize::new(0)),
      tail_retained_metrics,
      recovery_retained_metrics,
      generation: AtomicU64::new(0),
      failures: AtomicU64::new(0),
      metrics,
    }
  }

  fn limits(&self) -> MetadataCacheLimits {
    let mut watched = self.last_valid_limits.lock();
    let (limits, error) = watched.current(|flags, defaults| {
      MetadataCacheLimits::from_flags(flags, *defaults, self.config.coalescing_window)
    });
    if let Some(error) = error {
      warn_every!(
        15.seconds(),
        "broker metadata cache ignoring invalid limits: {error}"
      );
    }
    *limits
  }

  fn try_admit_refill(&self, limit: usize) -> Result<RefillAdmission> {
    self
      .active_refills
      .try_update(Ordering::AcqRel, Ordering::Relaxed, |active| {
        (active < limit).then_some(active + 1)
      })
      .map_err(|_| {
        anyhow!(MetadataCacheOverload {
          reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_REFILL_CONCURRENCY,
          message: "metadata cache overloaded: refill concurrency limit reached",
        })
      })?;
    Ok(RefillAdmission(Arc::clone(&self.active_refills)))
  }

  /// Runs cache eviction work independently of read traffic.
  fn spawn_maintenance(self: &Arc<Self>, shutdown_trigger_handle: &ComponentShutdownTriggerHandle) {
    let cache = Arc::clone(self);
    let mut shutdown = shutdown_trigger_handle.clone().make_shutdown();
    tokio::spawn(async move {
      loop {
        tokio::select! {
          () = shutdown.cancelled() => {
            debug!("broker metadata cache maintenance shutdown");
            return;
          },
          () = tokio::time::sleep(CACHE_MAINTENANCE_INTERVAL) => cache.run_maintenance().await,
        }
      }
    });
  }

  #[must_use]
  pub async fn snapshot(&self) -> MetadataCacheSnapshot {
    self.run_maintenance().await;
    let snapshot = MetadataCacheSnapshot {
      tail_entry_count: self.eventual_tail_entries.entry_count(),
      tail_retained_bytes: self.eventual_tail_entries.weighted_size(),
      tail_byte_budget: self
        .config
        .tail_max_bytes
        .saturating_sub(self.config.tail_max_bytes / 2),
      recovery_entry_count: self.eventual_recovery_entries.entry_count(),
      recovery_retained_bytes: self.eventual_recovery_entries.weighted_size(),
      recovery_byte_budget: self
        .config
        .recovery_max_bytes
        .saturating_sub(self.config.recovery_max_bytes / 2),
      strong_sealed_entry_count: self.strong_sealed_entries.entry_count(),
      strong_sealed_retained_bytes: self.strong_sealed_entries.weighted_size(),
      strong_sealed_byte_budget: self
        .config
        .tail_max_bytes
        .saturating_div(2)
        .saturating_add(self.config.recovery_max_bytes.saturating_div(2)),
      tail_oldest_entry_age_seconds: oldest_entry_age_seconds(
        &self.eventual_tail_entries,
        self.time_provider.now(),
      ),
      recovery_oldest_entry_age_seconds: oldest_entry_age_seconds(
        &self.eventual_recovery_entries,
        self.time_provider.now(),
      ),
      in_flight_refills: self
        .in_flight
        .lock()
        .len()
        .saturating_add(self.strong_suffix_in_flight.lock().len()),
      active_waiters: self.active_waiters.load(Ordering::Relaxed),
      available_refill_permits: self
        .limits()
        .max_refills
        .saturating_sub(self.active_refills.load(Ordering::Relaxed)),
      failure_total: self.failures.load(Ordering::Relaxed),
    };
    self.record_cache_state(&snapshot);
    snapshot
  }

  async fn run_maintenance(&self) {
    // Moka applies expiration and capacity eviction asynchronously. A quiet cache therefore
    // needs a maintenance driver so its retained-entry metrics converge without an admin read.
    self.eventual_tail_entries.run_pending_tasks().await;
    self.eventual_recovery_entries.run_pending_tasks().await;
    self.strong_sealed_entries.run_pending_tasks().await;
    self
      .metrics
      .strong_sealed_entries
      .set(i64::try_from(self.strong_sealed_entries.entry_count()).unwrap_or(i64::MAX));
    self
      .metrics
      .strong_sealed_retained_bytes
      .set(i64::try_from(self.strong_sealed_entries.weighted_size()).unwrap_or(i64::MAX));
    self.tail_retained_metrics.record_exact(
      self.eventual_tail_entries.entry_count(),
      self.eventual_tail_entries.weighted_size(),
    );
    self.recovery_retained_metrics.record_exact(
      self.eventual_recovery_entries.entry_count(),
      self.eventual_recovery_entries.weighted_size(),
    );
  }

  pub async fn read(
    self: &Arc<Self>,
    request: ReadMetadataWindowRequest,
  ) -> ReadMetadataWindowResponse {
    self.metrics.requests.inc();
    let limits = self.limits();
    if request_partition_count(&request) > limits.max_request_partitions {
      self.record_failure(MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED);
      return response_error(
        &request,
        MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED,
        &anyhow!(MetadataCacheOverload {
          reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_REQUEST_PARTITIONS,
          message: "metadata request exceeds the configured partition limit",
        }),
      );
    }
    let specification = match self.validate_request(&request) {
      Ok(specification) => specification,
      Err(error) => {
        self.record_failure(MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_BAD_REQUEST);
        return response_error(
          &request,
          MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_BAD_REQUEST,
          &error,
        );
      },
    };
    let response_limit = response_limit(&request, limits.max_response_bytes);
    // The request budget covers retained-entry lookup, coalescing, refill, and response shaping.
    let response = match tokio::time::timeout(
      limits.request_timeout,
      self.entry_for(specification.clone()),
    )
    .await
    {
      Ok(Ok(entry)) => match response_from_entry(
        &specification,
        &entry.entry,
        response_limit,
        limits.max_response_items,
        entry.retained_coverage,
        entry.retained_strong_coverage,
      ) {
        Ok(response) => response,
        Err(error) => {
          let status = failure_status(&error);
          self.record_failure(status);
          response_error(&request, status, &error)
        },
      },
      Ok(Err(error)) => {
        let status = failure_status(&error);
        self.record_failure(status);
        response_error(&request, status, &error)
      },
      Err(_) => {
        self.record_failure(MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED);
        response_error(
          &request,
          MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED,
          &anyhow!(MetadataCacheOverload {
            reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_REQUEST_TIMEOUT,
            message: "metadata cache request timed out",
          }),
        )
      },
    };
    if let Some(read_metadata_window_response::Result::Success(success)) = response.result.as_ref()
    {
      self
        .metrics
        .response_items
        .inc_by(u64::try_from(success.segments.len()).unwrap_or(u64::MAX));
      self.metrics.response_bytes.inc_by(
        success
          .segments
          .iter()
          .map(|segment| segment.metadata.compute_size())
          .sum(),
      );
    }
    response
  }

  async fn entry_for(
    self: &Arc<Self>,
    specification: ReadSpecification,
  ) -> Result<LoadedCacheEntry> {
    let horizon = specification.seal_horizon(self.config.seal_clock_skew);
    loop {
      if specification.consistency == StoreConsistency::Strong
        && specification.requested_seal_before.is_some()
        && let Some(entry) = self.strong_sealed_entries.get(&specification.key).await
        && entry.covers(&specification)
        && entry
          .sealed_horizon
          .is_none_or(|certified| horizon <= certified)
        && let (Some(stored_bound), Some(sealed_at)) = (entry.sealed_before, entry.sealed_at)
      {
        let safe_at_observation =
          SnowflakeId::minimum_for_timestamp(sealed_at.saturating_sub(horizon));
        let bound = stored_bound
          .min(specification.requested_seal_before.expect("seal requested"))
          .min(safe_at_observation);
        if specification
          .min_snowflake()
          .is_none_or(|floor| floor < bound)
        {
          self.metrics.strong_sealed_reuses.inc();
          if specification
            .window_end_snowflake()
            .is_some_and(|window_end| bound < window_end)
          {
            let scanned = self
              .coalesced_strong_suffix(&specification, entry, bound)
              .await?;
            let proof_at = scanned.sealed_at.unwrap_or(sealed_at);
            let seal_bound = scanned
              .sealed_before
              .unwrap_or(bound)
              .min(specification.requested_seal_before.expect("seal requested"))
              .min(SnowflakeId::minimum_for_timestamp(
                proof_at.saturating_sub(horizon),
              ));
            let entry = Arc::new(CacheEntry {
              refill_floor: scanned.refill_floor,
              observed_at: scanned.observed_at,
              sealed_before: Some(seal_bound),
              sealed_at: Some(proof_at),
              sealed_horizon: (proof_at > sealed_at)
                .then_some(horizon)
                .or(scanned.sealed_horizon),
              retained_bytes: scanned.retained_bytes,
              generation: scanned.generation,
              segments: scanned.segments.clone(),
            });
            return Ok(LoadedCacheEntry {
              entry,
              retained_coverage: false,
              retained_strong_coverage: true,
            });
          }
          return Ok(LoadedCacheEntry {
            entry,
            retained_coverage: false,
            retained_strong_coverage: true,
          });
        }
      }
      // Strong reads intentionally bypass retained data. Eventual entries are usable only when
      // their refill floor and observation age cover this caller's request.
      let entries = self.eventual_entries(specification.coverage);
      if specification.consistency == StoreConsistency::Eventual
        && let Some(entry) = entries.get(&specification.key).await
      {
        let covers_request = entry.covers(&specification);
        let now = self.time_provider.now();
        let fresh = entry.is_fresh(specification.topic.metadata_cache_max_age, now);
        if covers_request && fresh {
          debug!(
            "broker metadata cache retained hit: topic={}, window_start={}, coverage={:?}, \
             generation={}, partitions={}",
            specification.window.topic,
            specification.window.window_start_unix_seconds,
            specification.coverage,
            entry.generation,
            specification.partitions.len(),
          );
          match specification.coverage {
            ReadCoverage::Tail => self.metrics.tail_hits.inc(),
            ReadCoverage::FullRecovery => self.metrics.recovery_hits.inc(),
          }
          self
            .metrics
            .observation_age_seconds
            .observe((now - entry.observed_at).as_seconds_f64().max(0.0));
          return Ok(LoadedCacheEntry {
            entry,
            retained_coverage: true,
            retained_strong_coverage: false,
          });
        }
        debug!(
          "broker metadata cache invalidating retained entry: topic={}, window_start={}, \
           coverage={:?}, generation={}, covers_request={}, fresh={}",
          specification.window.topic,
          specification.window.window_start_unix_seconds,
          specification.coverage,
          entry.generation,
          covers_request,
          fresh,
        );
        entries.invalidate(&specification.key).await;
        self.metrics.invalidations.inc();
      }

      let (pending, _waiter) = self.join_or_start_refill(&specification)?;
      let mut entry = pending.wait().await?;
      if entry.covers(&specification) {
        if specification.consistency == StoreConsistency::Strong
          && let Some(requested) = specification.requested_seal_before
        {
          let sealed_at = entry
            .sealed_at
            .ok_or_else(|| anyhow!("strong refill missing pre-scan seal proof time"))?;
          let safe_at_observation =
            SnowflakeId::minimum_for_timestamp(sealed_at.saturating_sub(horizon));
          let bound = entry
            .sealed_before
            .unwrap_or(requested)
            .min(requested)
            .min(safe_at_observation);
          let seal = specification
            .min_snowflake()
            .is_none_or(|floor| floor < bound);
          entry = Arc::new(CacheEntry {
            refill_floor: entry.refill_floor,
            observed_at: entry.observed_at,
            sealed_before: seal.then_some(bound),
            sealed_at: seal.then_some(sealed_at),
            sealed_horizon: entry.sealed_horizon,
            retained_bytes: entry.retained_bytes,
            generation: entry.generation,
            segments: entry.segments.clone(),
          });
        }
        return Ok(LoadedCacheEntry {
          entry,
          retained_coverage: false,
          retained_strong_coverage: false,
        });
      }
      // A worker may have captured its request before this waiter joined. Retry so a narrower
      // Tail bound starts a refill that covers the caller instead of returning partial metadata.
    }
  }

  async fn coalesced_strong_suffix(
    self: &Arc<Self>,
    specification: &ReadSpecification,
    prefix: Arc<CacheEntry>,
    seal_before: SnowflakeId,
  ) -> Result<Arc<CacheEntry>> {
    let floor = specification
      .min_snowflake()
      .map_or(seal_before, |floor| floor.max(seal_before));
    let key = (
      specification.key.clone(),
      prefix.generation,
      seal_before.as_u64(),
      floor.as_u64(),
    );
    let limits = self.limits();
    let (pending, _waiter) = {
      let mut in_flight = self.strong_suffix_in_flight.lock();
      if let Some(pending) = in_flight.get(&key)
        && !pending.is_complete()
      {
        let waiter = pending.try_add_waiter(
          Arc::clone(&self.active_waiters),
          self.metrics.active_waiters.clone(),
          limits.max_waiters_per_key,
          limits.max_waiters,
        )?;
        (Arc::clone(pending), waiter)
      } else {
        let pending = Arc::new(PendingRefill::new(specification));
        let waiter = pending.try_add_waiter(
          Arc::clone(&self.active_waiters),
          self.metrics.active_waiters.clone(),
          limits.max_waiters_per_key,
          limits.max_waiters,
        )?;
        in_flight.insert(key.clone(), Arc::clone(&pending));
        let cache = Arc::clone(self);
        let request = specification.clone();
        let worker = Arc::clone(&pending);
        tokio::spawn(async move {
          let result = match cache.try_admit_refill(cache.limits().max_refills) {
            Ok(permit) => {
              cache.metrics.active_refills.inc();
              let result = tokio::time::timeout(
                cache.limits().request_timeout,
                cache.refill_strong_suffix(&request, prefix, seal_before),
              )
              .await
              .unwrap_or_else(|_| {
                Err(anyhow!(MetadataCacheOverload {
                  reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_REQUEST_TIMEOUT,
                  message: "metadata cache suffix refill timed out",
                }))
              });
              cache.metrics.active_refills.dec();
              drop(permit);
              result
            },
            Err(error) => Err(error),
          };
          worker.complete(result);
          let mut in_flight = cache.strong_suffix_in_flight.lock();
          if in_flight
            .get(&key)
            .is_some_and(|pending| Arc::ptr_eq(pending, &worker))
          {
            in_flight.remove(&key);
          }
        });
        (pending, waiter)
      }
    };
    pending.wait().await
  }

  async fn refill_strong_suffix(
    &self,
    specification: &ReadSpecification,
    prefix: Arc<CacheEntry>,
    seal_before: SnowflakeId,
  ) -> Result<Arc<CacheEntry>> {
    let scan_started_at = self.time_provider.now();
    let min_snowflake = Some(
      specification
        .min_snowflake()
        .map_or(seal_before, |floor| floor.max(seal_before)),
    );
    self.metrics.strong_suffix_queries.inc();
    self.metrics.storage_queries.inc();
    let mut suffix = self
      .metadata_store
      .scan_window_from_snowflake(
        &specification.window,
        min_snowflake,
        StoreConsistency::Strong,
      )
      .await?;
    suffix.retain(|segment| segment.snowflake_id >= seal_before);
    suffix.sort_by_key(|segment| segment.snowflake_id);
    let observed_at = self.time_provider.now();
    let prefix_len = prefix.prefix_len(seal_before);
    ensure!(
      prefix_len.saturating_add(suffix.len()) <= self.limits().max_entry_items,
      MetadataCacheOverload {
        reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_ENTRY_ITEMS,
        message: "metadata cache overloaded: cache generation exceeds the configured item limit",
      }
    );
    let retained_bytes = prefix
      .retained_bytes
      .saturating_add(estimate_retained_bytes(&suffix));
    let suffix: Vector<_> = suffix.into_iter().map(Arc::new).collect();
    let mut sealed_before = Some(seal_before);
    let mut sealed_at = prefix.sealed_at;
    let mut sealed_horizon = prefix.sealed_horizon;
    let stored_bound = prefix
      .sealed_before
      .ok_or_else(|| anyhow!("strong suffix refill missing retained seal"))?;
    let original_seal_at = prefix
      .sealed_at
      .ok_or_else(|| anyhow!("strong suffix refill missing retained proof time"))?;
    if min_snowflake.is_some_and(|floor| floor <= stored_bound)
      && scan_started_at >= original_seal_at
      && observed_at >= scan_started_at
      && let (Some(requested), Some(window_end)) = (
        specification.requested_seal_before,
        specification.window_end_snowflake(),
      )
    {
      let horizon = specification.seal_horizon(self.config.seal_clock_skew);
      let safe_at_scan =
        SnowflakeId::minimum_for_timestamp(scan_started_at.saturating_sub(horizon));
      let promoted_bound = requested.min(window_end).min(safe_at_scan);
      if promoted_bound > stored_bound {
        sealed_before = Some(promoted_bound);
        sealed_at = Some(scan_started_at);
        sealed_horizon = Some(horizon);
        let newly_sealed: Vector<_> = suffix
          .iter()
          .filter(|segment| {
            segment.snowflake_id >= stored_bound && segment.snowflake_id < promoted_bound
          })
          .cloned()
          .collect();
        let prefix_bytes = if prefix.segments.is_empty() {
          0
        } else {
          prefix.retained_bytes
        };
        let newly_sealed_bytes = if newly_sealed.is_empty() {
          0
        } else {
          estimate_retained_bytes(newly_sealed.iter().map(Arc::as_ref))
        };
        let promoted_bytes = prefix_bytes.saturating_add(newly_sealed_bytes).max(1);
        // Moka serializes this decision per key; the computation must not await or re-enter the
        // cache.
        let installed = self
          .strong_sealed_entries
          .entry(specification.key.clone())
          .and_compute_with(|current| {
            ready(
              if current.is_some_and(|current| Arc::ptr_eq(current.value(), &prefix)) {
                let mut promoted_segments = prefix.segments.clone();
                promoted_segments.append(newly_sealed);
                Op::Put(Arc::new(CacheEntry {
                  refill_floor: prefix.refill_floor,
                  observed_at,
                  sealed_before,
                  sealed_at,
                  sealed_horizon,
                  retained_bytes: promoted_bytes,
                  generation: prefix.generation,
                  segments: promoted_segments,
                }))
              } else {
                Op::Nop
              },
            )
          })
          .await;
        if matches!(installed, CompResult::ReplacedWith(_)) {
          self.metrics.strong_sealed_installs.inc();
        }
      }
    }
    let mut segments = prefix.segments.take(prefix_len);
    segments.append(suffix);
    Ok(Arc::new(CacheEntry {
      refill_floor: prefix.refill_floor,
      observed_at,
      sealed_before,
      sealed_at,
      sealed_horizon,
      retained_bytes,
      generation: self
        .generation
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1),
      segments,
    }))
  }

  fn join_or_start_refill(
    self: &Arc<Self>,
    specification: &ReadSpecification,
  ) -> Result<(Arc<PendingRefill>, PendingWaiter)> {
    let limits = self.limits();
    let mut in_flight = self.in_flight.lock();
    if let Some(pending) = in_flight.get(&specification.key).cloned()
      && !pending.is_complete()
    {
      let waiter = pending.try_add_waiter(
        Arc::clone(&self.active_waiters),
        self.metrics.active_waiters.clone(),
        limits.max_waiters_per_key,
        limits.max_waiters,
      )?;
      // Tail requests arriving during the coalescing window lower per-partition bounds before the
      // worker snapshots them, allowing one scan to cover all admitted waiters.
      pending.admit(specification);
      debug!(
        "broker metadata cache joined pending refill: topic={}, window_start={}, \
         consistency={:?}, coverage={:?}, partitions={}",
        specification.window.topic,
        specification.window.window_start_unix_seconds,
        specification.consistency,
        specification.coverage,
        specification.partitions.len(),
      );
      return Ok((pending, waiter));
    }

    // A completed worker can notify a narrower late waiter before it removes itself from the
    // registry. Replace that stale entry here so the waiter starts a new refill instead of
    // repeatedly observing the same uncovered strong-read result.
    in_flight.remove(&specification.key);

    let pending = Arc::new(PendingRefill::new(specification));
    // Reserve capacity before publishing or spawning a refill. A rejected unique key must not
    // create background storage work while the cache is already overloaded.
    let waiter = pending.try_add_waiter(
      Arc::clone(&self.active_waiters),
      self.metrics.active_waiters.clone(),
      limits.max_waiters_per_key,
      limits.max_waiters,
    )?;
    in_flight.insert(specification.key.clone(), Arc::clone(&pending));
    debug!(
      "broker metadata cache started pending refill: topic={}, window_start={}, consistency={:?}, \
       coverage={:?}, partitions={}",
      specification.window.topic,
      specification.window.window_start_unix_seconds,
      specification.consistency,
      specification.coverage,
      specification.partitions.len(),
    );
    let cache = Arc::clone(self);
    let key = specification.key.clone();
    let window = specification.window.clone();
    let worker = Arc::clone(&pending);
    tokio::spawn(async move {
      // Delay the storage read so compatible requests can widen the shared Tail scan.
      let Ok(coalescing_window) = Duration::try_from(cache.config.coalescing_window) else {
        worker.complete(Err(anyhow::Error::msg(
          "metadata cache coalescing window exceeds supported range",
        )));
        cache.remove_pending_refill(&key, &worker);
        return;
      };
      debug!(
        "broker metadata cache coalescing wait started: topic={}, window_start={}, \
         duration_ms={}, now={}",
        window.topic,
        window.window_start_unix_seconds,
        coalescing_window.whole_milliseconds(),
        cache.time_provider.now(),
      );
      cache.time_provider.sleep(coalescing_window).await;
      debug!(
        "broker metadata cache coalescing wait completed: topic={}, window_start={}, now={}",
        window.topic,
        window.window_start_unix_seconds,
        cache.time_provider.now(),
      );
      let (request, coalescing_window_requests) = worker.begin();
      let result = match cache.try_admit_refill(cache.limits().max_refills) {
        Ok(permit) => {
          cache
            .metrics
            .coalescing_window_requests
            .inc_by(u64::try_from(coalescing_window_requests).unwrap_or(u64::MAX));
          cache.metrics.active_refills.inc();
          let remaining = cache
            .limits()
            .request_timeout
            .saturating_sub(cache.config.coalescing_window);
          let result = tokio::time::timeout(remaining, cache.load_entry(request))
            .await
            .unwrap_or_else(|_| {
              Err(anyhow!(MetadataCacheOverload {
                reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_REQUEST_TIMEOUT,
                message: "metadata cache refill timed out",
              }))
            });
          drop(permit);
          cache.metrics.active_refills.dec();
          result
        },
        Err(error) => Err(error),
      };
      worker.complete(result);
      cache.remove_pending_refill(&key, &worker);
    });
    Ok((pending, waiter))
  }

  fn remove_pending_refill(&self, key: &CacheKey, completed: &Arc<PendingRefill>) {
    let mut in_flight = self.in_flight.lock();
    if in_flight
      .get(key)
      .is_some_and(|pending| Arc::ptr_eq(pending, completed))
    {
      in_flight.remove(key);
    }
  }

  async fn load_entry(&self, specification: ReadSpecification) -> Result<Arc<CacheEntry>> {
    let min_snowflake = specification.min_snowflake();
    debug!(
      "broker metadata cache refill: topic={}, window_start={}, consistency={:?}, coverage={:?}, \
       min_snowflake={:?}, max_metadata_publication_lag={}",
      specification.window.topic,
      specification.window.window_start_unix_seconds,
      specification.consistency,
      specification.coverage,
      min_snowflake.map(SnowflakeId::as_u64),
      specification.topic.max_metadata_publication_lag,
    );
    self.metrics.storage_queries.inc();
    let scan_started_at = self.time_provider.now();
    let mut segments = self
      .metadata_store
      .scan_window_from_snowflake(
        &specification.window,
        min_snowflake,
        specification.consistency,
      )
      .await?;
    segments.sort_by_key(|segment| segment.snowflake_id);
    ensure!(
      segments.len() <= self.limits().max_entry_items,
      MetadataCacheOverload {
        reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_ENTRY_ITEMS,
        message: "metadata cache overloaded: cache generation exceeds the configured item limit",
      }
    );
    let observed_at = self.time_provider.now();
    let sealed_before = if specification.consistency == StoreConsistency::Strong {
      specification.requested_seal_before.map(|requested| {
        requested.min(SnowflakeId::minimum_for_timestamp(
          scan_started_at.saturating_sub(specification.seal_horizon(self.config.seal_clock_skew)),
        ))
      })
    } else {
      None
    };
    let retained_bytes = estimate_retained_bytes(&segments);
    let entry = Arc::new(CacheEntry {
      refill_floor: min_snowflake,
      observed_at,
      sealed_before,
      sealed_at: (specification.consistency == StoreConsistency::Strong).then_some(scan_started_at),
      sealed_horizon: None,
      retained_bytes,
      generation: self
        .generation
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1),
      segments: segments.into_iter().map(Arc::new).collect(),
    });
    // A strong read establishes no reusable eventual observation. Both eventual coverage modes
    // retain the full scan so later callers can be filtered to their own partitions and bounds.
    if specification.consistency == StoreConsistency::Eventual {
      match specification.coverage {
        ReadCoverage::Tail => {
          self.metrics.tail_refills.inc();
          self
            .eventual_tail_entries
            .insert(specification.key, Arc::clone(&entry))
            .await;
          self
            .tail_retained_metrics
            .record_insert(entry.retained_bytes);
        },
        ReadCoverage::FullRecovery => {
          self.metrics.recovery_baselines.inc();
          self.metrics.recovery_seals.inc();
          self
            .eventual_recovery_entries
            .insert(specification.key, Arc::clone(&entry))
            .await;
          self
            .recovery_retained_metrics
            .record_insert(entry.retained_bytes);
        },
      }
    } else if let Some(bound) = sealed_before
      && specification
        .min_snowflake()
        .is_none_or(|floor| floor < bound)
      && specification.window_end_snowflake().is_some()
    {
      let sealed_segments = entry.segments.take(entry.prefix_len(bound));
      let sealed_entry = Arc::new(CacheEntry {
        refill_floor: entry.refill_floor,
        observed_at,
        sealed_before,
        sealed_at: entry.sealed_at,
        sealed_horizon: None,
        retained_bytes: estimate_retained_bytes(sealed_segments.iter().map(Arc::as_ref)),
        generation: entry.generation,
        segments: sealed_segments,
      });
      // Compare and replace together so a late scan cannot overwrite a newer compatible proof.
      let installed = self
        .strong_sealed_entries
        .entry(specification.key.clone())
        .and_compute_with(|current| {
          ready(
            if current.is_some_and(|current| {
              let current = current.value();
              current.covers(&specification)
                && current.sealed_before >= sealed_before
                && current.sealed_horizon.is_none_or(|certified| {
                  specification.seal_horizon(self.config.seal_clock_skew) <= certified
                })
            }) {
              Op::Nop
            } else {
              Op::Put(sealed_entry)
            },
          )
        })
        .await;
      if matches!(
        installed,
        CompResult::Inserted(_) | CompResult::ReplacedWith(_)
      ) {
        self.metrics.strong_sealed_installs.inc();
      }
    }
    Ok(entry)
  }

  fn eventual_entries(&self, coverage: ReadCoverage) -> &Cache<CacheKey, Arc<CacheEntry>> {
    match coverage {
      ReadCoverage::Tail => &self.eventual_tail_entries,
      ReadCoverage::FullRecovery => &self.eventual_recovery_entries,
    }
  }

  fn record_failure(&self, status: MetadataReadFailureStatus) {
    self.failures.fetch_add(1, Ordering::Relaxed);
    self.metrics.failures.inc();
    if status == MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED {
      self.metrics.overloads.inc();
    }
  }

  fn record_cache_state(&self, snapshot: &MetadataCacheSnapshot) {
    self
      .tail_retained_metrics
      .record_exact(snapshot.tail_entry_count, snapshot.tail_retained_bytes);
    self.recovery_retained_metrics.record_exact(
      snapshot.recovery_entry_count,
      snapshot.recovery_retained_bytes,
    );
    self
      .metrics
      .active_waiters
      .set(i64::try_from(snapshot.active_waiters).unwrap_or(i64::MAX));
  }

  fn validate_request(&self, request: &ReadMetadataWindowRequest) -> Result<ReadSpecification> {
    ensure!(!request.topic.is_empty(), "metadata request topic is empty");
    let topic = self
      .config
      .topics
      .get(request.topic.as_str())
      .ok_or_else(|| anyhow!("unknown topic {}", request.topic))?
      .clone();
    ensure!(
      request
        .window_start_unix_seconds
        .rem_euclid(topic.metadata_window_size.whole_seconds())
        == 0,
      "metadata window start is not aligned"
    );
    let consistency = match request.consistency.enum_value() {
      Ok(MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL) => StoreConsistency::Eventual,
      Ok(MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG) => StoreConsistency::Strong,
      Err(_) => return Err(anyhow!("metadata request has an unsupported consistency")),
    };
    let (coverage, partition_ids, min_by_partition) = match request.coverage.as_ref() {
      Some(read_metadata_window_request::Coverage::Tail(TailMetadataCoverage {
        partition_bounds,
        ..
      })) => {
        let bounds = partition_bounds
          .iter()
          .map(|bound| (bound.virtual_partition_id, SnowflakeId(bound.min_snowflake)))
          .collect::<HashMap<_, _>>();
        ensure!(
          !bounds.is_empty() && bounds.len() == partition_bounds.len(),
          "tail metadata request has no bounds or duplicate virtual partitions"
        );
        (
          ReadCoverage::Tail,
          bounds.keys().copied().collect::<BTreeSet<_>>(),
          Some(bounds),
        )
      },
      Some(read_metadata_window_request::Coverage::FullRecovery(
        FullRecoveryMetadataCoverage {
          virtual_partition_ids,
          ..
        },
      )) => {
        let partitions = virtual_partition_ids
          .iter()
          .copied()
          .collect::<BTreeSet<_>>();
        ensure!(
          !partitions.is_empty() && partitions.len() == virtual_partition_ids.len(),
          "full recovery metadata request has no virtual partitions or duplicate partitions"
        );
        (ReadCoverage::FullRecovery, partitions, None)
      },
      None => {
        return Err(anyhow!(
          "metadata request must specify tail or full recovery coverage"
        ));
      },
    };
    ensure!(
      partition_ids
        .iter()
        .all(|partition| topic.is_valid_partition(*partition)),
      "metadata request has an invalid virtual partition"
    );
    // Bounds and partitions are request projections, not cache identity. Keeping them out of the
    // key lets one refill serve compatible callers after the response is filtered below.
    let key = CacheKey {
      topic: request.topic.to_string(),
      window_start_unix_seconds: request.window_start_unix_seconds,
      consistency,
      coverage,
    };
    let requested_seal_before = if let Some(requested) = request.requested_seal_before {
      let window_end = OffsetDateTime::from_unix_timestamp(
        request
          .window_start_unix_seconds
          .saturating_add(topic.metadata_window_size.whole_seconds()),
      )
      .map_err(|_| anyhow!("metadata window end is out of range"))?;
      Some(SnowflakeId(requested).min(SnowflakeId::minimum_for_timestamp(window_end)))
    } else {
      None
    };
    Ok(ReadSpecification {
      key,
      window: TopicWindowKey {
        topic: request.topic.to_string(),
        window_start_unix_seconds: request.window_start_unix_seconds,
      },
      topic,
      consistency,
      coverage,
      partitions: partition_ids,
      min_by_partition,
      requested_seal_before,
      requested_seal_horizon: request.requested_seal_horizon_ms.map_or(
        Ok(Duration::ZERO),
        |milliseconds| {
          i64::try_from(milliseconds)
            .map(Duration::milliseconds)
            .map_err(|_| anyhow!("requested metadata seal horizon is too large"))
        },
      )?,
    })
  }
}

//
// CacheKey
//

type StrongSuffixKey = (CacheKey, u64, u64, u64);

/// Identifies data that can share a storage scan; caller-specific bounds stay in
/// `ReadSpecification`.
#[derive(Clone, Eq)]
struct CacheKey {
  topic: String,
  window_start_unix_seconds: i64,
  consistency: StoreConsistency,
  coverage: ReadCoverage,
}

impl PartialEq for CacheKey {
  fn eq(&self, other: &Self) -> bool {
    self.topic == other.topic
      && self.window_start_unix_seconds == other.window_start_unix_seconds
      && self.consistency == other.consistency
      && self.coverage == other.coverage
  }
}

impl Hash for CacheKey {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.topic.hash(state);
    self.window_start_unix_seconds.hash(state);
    match self.consistency {
      StoreConsistency::Eventual => 0_u8,
      StoreConsistency::Strong => 1_u8,
    }
    .hash(state);
    match self.coverage {
      ReadCoverage::Tail => 0_u8,
      ReadCoverage::FullRecovery => 1_u8,
    }
    .hash(state);
  }
}

/// Validated request state, including the projection applied after loading a shared cache entry.
#[derive(Clone)]
struct ReadSpecification {
  key: CacheKey,
  window: TopicWindowKey,
  topic: TopicCacheContract,
  consistency: StoreConsistency,
  coverage: ReadCoverage,
  partitions: BTreeSet<u32>,
  min_by_partition: Option<HashMap<u32, SnowflakeId>>,
  requested_seal_before: Option<SnowflakeId>,
  requested_seal_horizon: Duration,
}

impl ReadSpecification {
  fn min_snowflake(&self) -> Option<SnowflakeId> {
    self
      .min_by_partition
      .as_ref()
      .and_then(|bounds| bounds.values().copied().min())
  }

  fn seal_horizon(&self, clock_skew: Duration) -> Duration {
    self.requested_seal_horizon.max(
      self
        .topic
        .max_metadata_publication_lag
        .saturating_add(clock_skew),
    )
  }

  fn window_end_snowflake(&self) -> Option<SnowflakeId> {
    OffsetDateTime::from_unix_timestamp(
      self
        .window
        .window_start_unix_seconds
        .saturating_add(self.topic.metadata_window_size.whole_seconds()),
    )
    .ok()
    .map(SnowflakeId::minimum_for_timestamp)
  }
}

struct CacheEntry {
  // The global storage scan floor; a Tail request is covered only when this is no newer than its
  // lowest requested partition bound.
  refill_floor: Option<SnowflakeId>,
  observed_at: OffsetDateTime,
  sealed_before: Option<SnowflakeId>,
  sealed_at: Option<OffsetDateTime>,
  sealed_horizon: Option<Duration>,
  retained_bytes: u32,
  generation: u64,
  segments: Vector<Arc<SegmentMetadata>>,
}

fn oldest_entry_age_seconds(
  entries: &Cache<CacheKey, Arc<CacheEntry>>,
  now: OffsetDateTime,
) -> Option<u64> {
  entries
    .iter()
    .map(|(_, entry)| {
      (now - entry.observed_at)
        .whole_seconds()
        .max(0)
        .cast_unsigned()
    })
    .max()
}

//
// LoadedCacheEntry
//

struct LoadedCacheEntry {
  entry: Arc<CacheEntry>,
  retained_coverage: bool,
  retained_strong_coverage: bool,
}

impl CacheEntry {
  fn prefix_len(&self, bound: SnowflakeId) -> usize {
    self
      .segments
      .binary_search_by_key(&bound, |segment| segment.snowflake_id)
      .unwrap_or_else(|index| index)
  }

  fn covers(&self, specification: &ReadSpecification) -> bool {
    match (self.refill_floor, specification.min_snowflake()) {
      (Some(refill_floor), Some(request_floor)) => refill_floor <= request_floor,
      (None, None) => true,
      _ => false,
    }
  }

  fn is_fresh(&self, maximum_age: Duration, now: OffsetDateTime) -> bool {
    maximum_age > Duration::ZERO && now - self.observed_at <= maximum_age
  }
}

//
// PendingRefill
//

struct PendingRefill {
  state: Mutex<PendingRefillState>,
  ready: Notify,
}

struct PendingRefillState {
  // Tail bounds may be widened while coalescing. Once the worker calls `begin`, this snapshot is
  // immutable and late callers retry if it does not cover them.
  specification: ReadSpecification,
  completed: Option<Result<Arc<CacheEntry>, SharedRefillError>>,
  waiter_count: usize,
}

#[derive(Clone, Debug)]
struct SharedRefillError {
  source: Arc<anyhow::Error>,
  reason: Option<MetadataReadOverloadReason>,
}

impl std::fmt::Display for SharedRefillError {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.source.fmt(formatter)
  }
}

impl std::error::Error for SharedRefillError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    Some(self.source.as_ref().as_ref())
  }
}

impl PendingRefill {
  fn new(specification: &ReadSpecification) -> Self {
    Self {
      state: Mutex::new(PendingRefillState {
        specification: specification.clone(),
        completed: None,
        waiter_count: 0,
      }),
      ready: Notify::new(),
    }
  }

  fn admit(&self, specification: &ReadSpecification) {
    let mut state = self.state.lock();
    if state.completed.is_some() || state.specification.coverage != ReadCoverage::Tail {
      return;
    }
    let Some(bounds) = specification.min_by_partition.as_ref() else {
      return;
    };
    let pending_bounds = state
      .specification
      .min_by_partition
      .as_mut()
      .expect("tail pending refill retains tail bounds");
    for (&partition_id, &bound) in bounds {
      pending_bounds
        .entry(partition_id)
        .and_modify(|pending_bound| *pending_bound = (*pending_bound).min(bound))
        .or_insert(bound);
    }
  }

  fn is_complete(&self) -> bool {
    self.state.lock().completed.is_some()
  }

  fn try_add_waiter(
    self: &Arc<Self>,
    active_waiters: Arc<AtomicUsize>,
    active_waiters_gauge: prometheus::IntGauge,
    max_waiters_per_key: usize,
    max_waiters: usize,
  ) -> Result<PendingWaiter> {
    let active_waiter_count = active_waiters.fetch_add(1, Ordering::Relaxed);
    if active_waiter_count >= max_waiters {
      active_waiters.fetch_sub(1, Ordering::Relaxed);
      return Err(anyhow!(MetadataCacheOverload {
        reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_GLOBAL_WAITERS,
        message: "metadata cache overloaded: global waiter limit reached",
      }));
    }
    let mut state = self.state.lock();
    if state.waiter_count >= max_waiters_per_key {
      active_waiters.fetch_sub(1, Ordering::Relaxed);
      return Err(anyhow!(MetadataCacheOverload {
        reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_PER_KEY_WAITERS,
        message: "metadata cache overloaded: per-key waiter limit reached",
      }));
    }
    state.waiter_count = state.waiter_count.saturating_add(1);
    active_waiters_gauge
      .set(i64::try_from(active_waiters.load(Ordering::Relaxed)).unwrap_or(i64::MAX));
    Ok(PendingWaiter {
      pending: Arc::clone(self),
      active_waiters,
      active_waiters_gauge,
    })
  }

  fn begin(&self) -> (ReadSpecification, usize) {
    let state = self.state.lock();
    (state.specification.clone(), state.waiter_count)
  }

  fn complete(&self, result: Result<Arc<CacheEntry>>) {
    let result = result.map_err(|error| SharedRefillError {
      reason: overload_reason(&error),
      source: Arc::new(error),
    });
    self.state.lock().completed = Some(result);
    self.ready.notify_waiters();
  }

  async fn wait(&self) -> Result<Arc<CacheEntry>> {
    loop {
      // Register the notification before inspecting completion so a worker cannot signal between
      // the state check and `await`.
      let notified = self.ready.notified();
      if let Some(result) = self.state.lock().completed.clone() {
        return result.map_err(anyhow::Error::from);
      }
      notified.await;
    }
  }
}

//
// PendingWaiter
//

/// Releases bounded pending-refill admission after a waiter receives a result or is cancelled.
struct PendingWaiter {
  pending: Arc<PendingRefill>,
  active_waiters: Arc<AtomicUsize>,
  active_waiters_gauge: prometheus::IntGauge,
}

struct RefillAdmission(Arc<AtomicUsize>);

impl Drop for RefillAdmission {
  fn drop(&mut self) {
    self.0.fetch_sub(1, Ordering::AcqRel);
  }
}

impl Drop for PendingWaiter {
  fn drop(&mut self) {
    let mut state = self.pending.state.lock();
    state.waiter_count = state.waiter_count.saturating_sub(1);
    self.active_waiters.fetch_sub(1, Ordering::Relaxed);
    self
      .active_waiters_gauge
      .set(i64::try_from(self.active_waiters.load(Ordering::Relaxed)).unwrap_or(i64::MAX));
  }
}

fn request_partition_count(request: &ReadMetadataWindowRequest) -> usize {
  match request.coverage.as_ref() {
    Some(read_metadata_window_request::Coverage::Tail(coverage)) => coverage.partition_bounds.len(),
    Some(read_metadata_window_request::Coverage::FullRecovery(coverage)) => {
      coverage.virtual_partition_ids.len()
    },
    None => 0,
  }
}

fn response_limit(request: &ReadMetadataWindowRequest, max_response_bytes: u64) -> u64 {
  // A caller can request less, but never more than the broker's configured response budget.
  request.max_response_bytes.min(max_response_bytes).max(1)
}

fn failure_status(error: &anyhow::Error) -> MetadataReadFailureStatus {
  if overload_reason(error).is_some() {
    MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED
  } else {
    MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_FAILED
  }
}

fn overload_reason(error: &anyhow::Error) -> Option<MetadataReadOverloadReason> {
  error.chain().find_map(|cause| {
    cause
      .downcast_ref::<MetadataCacheOverload>()
      .map(|overload| overload.reason)
      .or_else(|| {
        cause
          .downcast_ref::<SharedRefillError>()
          .and_then(|shared| shared.reason)
      })
      .or_else(|| {
        cause
          .is::<MetadataQueryThrottled>()
          .then_some(MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_STORAGE_THROTTLED)
      })
  })
}

fn response_from_entry(
  specification: &ReadSpecification,
  entry: &CacheEntry,
  response_limit: u64,
  max_response_items: usize,
  retained_coverage: bool,
  retained_strong_coverage: bool,
) -> Result<ReadMetadataWindowResponse> {
  // Shared entries contain complete segments. Project them here so one caller cannot observe
  // partitions or Tail rows outside the coverage it requested.
  let mut segments = Vec::new();
  let mut response_bytes = 0_u64;
  for segment in &entry.segments {
    if let Some(min_snowflake) = specification.min_snowflake()
      && segment.snowflake_id < min_snowflake
    {
      continue;
    }
    let mut metadata = encode_segment_metadata_v1(segment)?;
    metadata.partitions.retain(|partition| {
      specification
        .partitions
        .contains(&partition.virtual_partition_id)
        && specification
          .min_by_partition
          .as_ref()
          .is_none_or(|bounds| {
            bounds
              .get(&partition.virtual_partition_id)
              .is_some_and(|bound| segment.snowflake_id >= *bound)
          })
    });
    if metadata.partitions.is_empty() {
      continue;
    }
    let metadata_bytes = metadata.compute_size();
    response_bytes = response_bytes.saturating_add(metadata_bytes);
    ensure!(
      response_bytes <= response_limit,
      MetadataCacheOverload {
        reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_RESPONSE_BYTES,
        message: "metadata cache overloaded: response exceeds configured byte limit",
      }
    );
    ensure!(
      segments.len() < max_response_items,
      MetadataCacheOverload {
        reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_RESPONSE_ITEMS,
        message: "metadata cache overloaded: response exceeds configured item limit",
      }
    );
    segments.push(BrokerSegmentMetadata {
      snowflake_id: segment.snowflake_id.as_u64(),
      metadata: Some(metadata).into(),
      ..Default::default()
    });
  }
  Ok(ReadMetadataWindowResponse {
    result: Some(read_metadata_window_response::Result::Success(
      MetadataReadSuccess {
        observed_at_unix_ms: i64::try_from(
          entry
            .observed_at
            .unix_timestamp_nanos()
            .div_euclid(1_000_000),
        )
        .map_err(|_| anyhow!("broker observation timestamp does not fit in Unix milliseconds"))?,
        refill_floor: entry.refill_floor.map(SnowflakeId::as_u64),
        generation: entry.generation,
        retained_coverage,
        sealed_before: entry.sealed_before.map(|bound| {
          specification
            .requested_seal_before
            .map_or(bound, |requested| bound.min(requested))
            .as_u64()
        }),
        sealed_at_unix_ms: entry
          .sealed_before
          .and(entry.sealed_at)
          .and_then(|observed_at| {
            i64::try_from(observed_at.unix_timestamp_nanos().div_euclid(1_000_000)).ok()
          }),
        retained_strong_coverage,
        segments,
        ..Default::default()
      },
    )),
    ..Default::default()
  })
}

fn response_error(
  request: &ReadMetadataWindowRequest,
  status: MetadataReadFailureStatus,
  error: &anyhow::Error,
) -> ReadMetadataWindowResponse {
  let reason = overload_reason(error);
  warn_every!(
    5.seconds(),
    "broker metadata cache request failed: topic={}, window_start={}, status={status:?}, \
     reason={reason:?}, partitions={}, max_response_bytes={}, error={error:#}",
    request.topic,
    request.window_start_unix_seconds,
    request_partition_count(request),
    request.max_response_bytes
  );
  ReadMetadataWindowResponse {
    result: Some(read_metadata_window_response::Result::Failure(
      MetadataReadFailure {
        status: status.into(),
        overload_reason: reason.unwrap_or_default().into(),
        error_message: if status == MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_FAILED {
          "metadata cache request failed".into()
        } else {
          error.to_string().into()
        },
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

fn estimate_retained_bytes<'segment>(
  segments: impl IntoIterator<Item = &'segment SegmentMetadata>,
) -> u32 {
  let bytes = segments.into_iter().fold(0_u64, |total, segment| {
    let index_bytes = u64::try_from(segment.segment_index.len()).unwrap_or(u64::MAX) * 40;
    total
      .saturating_add(u64::try_from(segment.blob_key.as_str().len()).unwrap_or(u64::MAX))
      .saturating_add(u64::try_from(segment.window.topic.len()).unwrap_or(u64::MAX))
      .saturating_add(index_bytes)
      .saturating_add(128)
  });
  u32::try_from(bytes).unwrap_or(u32::MAX).max(1)
}
