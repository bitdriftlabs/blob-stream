#[cfg(test)]
#[path = "./blob_cache_test.rs"]
mod tests;

use crate::write::memory_pressure::MemoryPressureController;
use anyhow::{Result, anyhow, ensure};
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use bd_time::TimeProvider;
use blob_stream_blob_store::{BlobKey, BlobStore, BlobStoreError};
use blob_stream_proto::protos::blobstream::v1::broker::{
  BlobRangeResult,
  BlobReadFailure,
  BlobReadFailureStatus,
  BlobReadSuccess,
  ReadBlobRangesRequest,
  ReadBlobRangesResponse,
  read_blob_ranges_response,
};
use blob_stream_proto::protos::blobstream::v1::config::BrokerConfig;
use blob_stream_runtime_config::feature_flag_duration_milliseconds;
use blob_stream_types::ProtoDurationExt;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use moka::future::Cache;
use parking_lot::Mutex;
use protobuf::Message;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::Semaphore;

const DEFAULT_REQUEST_TIMEOUT: time::Duration = time::Duration::seconds(30);
const DEFAULT_IDLE_TTL: time::Duration = time::Duration::seconds(10);
const DEFAULT_MAX_FETCHES: usize = 32;
pub const BLOB_CACHE_IDLE_TTL_FEATURE_FLAG: &str = "blob_stream_broker_blob_cache_idle_ttl_ms";
pub const MAX_BLOB_READ_REQUEST_BYTES: usize = 16 * 1024 * 1024;

type SharedFetch = Shared<BoxFuture<'static, std::result::Result<Bytes, BlobCacheError>>>;

//
// BlobCacheConfig
//

#[derive(Clone, Copy)]
pub struct BlobCacheConfig {
  request_timeout: Duration,
  idle_ttl: Duration,
  max_fetches: usize,
}

impl BlobCacheConfig {
  pub fn from_broker_config(
    broker: &BrokerConfig,
    feature_flags: Option<&FeatureFlagsWatch>,
  ) -> Result<Self> {
    let request_timeout = broker
      .blob_cache_request_timeout
      .as_ref()
      .map_or(DEFAULT_REQUEST_TIMEOUT, ProtoDurationExt::to_time_duration);
    let idle_ttl =
      feature_flags.map_or(Ok::<_, anyhow::Error>(DEFAULT_IDLE_TTL), |feature_flags| {
        let idle_ttl = feature_flag_duration_milliseconds(
          feature_flags,
          BLOB_CACHE_IDLE_TTL_FEATURE_FLAG,
          DEFAULT_IDLE_TTL,
        )?;
        Ok(
          idle_ttl
            .as_ref()
            .map_or(DEFAULT_IDLE_TTL, ProtoDurationExt::to_time_duration),
        )
      })?;
    ensure!(
      request_timeout.is_positive(),
      "broker blob_cache_request_timeout must be positive"
    );
    ensure!(
      idle_ttl.is_positive(),
      "feature flag {BLOB_CACHE_IDLE_TTL_FEATURE_FLAG} must be positive"
    );
    Ok(Self {
      request_timeout: Duration::try_from(request_timeout)
        .map_err(|_| anyhow!("blob_cache_request_timeout exceeds supported range"))?,
      idle_ttl: Duration::try_from(idle_ttl).map_err(|_| {
        anyhow!("feature flag {BLOB_CACHE_IDLE_TTL_FEATURE_FLAG} exceeds supported range")
      })?,
      max_fetches: DEFAULT_MAX_FETCHES,
    })
  }
}

//
// BlobCacheSnapshot
//

/// Aggregate blob-cache state suitable for the bounded broker admin endpoint.
#[derive(Debug, Serialize)]
pub struct BlobCacheSnapshot {
  pub enabled: bool,
  pub idle_ttl_seconds: u64,
  pub request_timeout_seconds: u64,
  pub entry_count: u64,
  pub retained_bytes: u64,
  pub active_fetches: usize,
  pub cache_reservations: u64,
  pub cache_headroom_bytes: Option<u64>,
  pub failure_total: u64,
}

//
// BlobCache
//

/// Whole-object cache serving exact byte slices for immutable blobs.
pub struct BlobCache {
  blob_store: Arc<dyn BlobStore>,
  config: BlobCacheConfig,
  pressure: Arc<MemoryPressureController>,
  entries: Cache<String, Bytes>,
  // Production uses Moka's native idle expiry. Tests opt into a logical clock for deterministic
  // expiration without adding a hash table or mutex acquisition to production cache hits.
  logical_idle_expiry: Option<LogicalIdleExpiry>,
  in_flight: Mutex<HashMap<String, Arc<SharedFetch>>>,
  fetch_permits: Arc<Semaphore>,
  cache_generation: Arc<AtomicU64>,
  failures: AtomicU64,
  metrics: BlobCacheMetrics,
}

#[derive(Clone)]
struct LogicalIdleExpiry {
  last_access: Arc<Mutex<HashMap<String, OffsetDateTime>>>,
  time_provider: Arc<dyn TimeProvider>,
}

#[derive(Clone)]
struct BlobCacheMetrics {
  requests: prometheus::IntCounter,
  response_items: prometheus::IntCounter,
  response_bytes: prometheus::IntCounter,
  hits: prometheus::IntCounter,
  fetches: prometheus::IntCounter,
  fetch_bytes: prometheus::IntCounter,
  evictions: prometheus::IntCounter,
  pressure_flushes: prometheus::IntCounter,
  not_found: prometheus::IntCounter,
  overloaded: prometheus::IntCounter,
  failures: prometheus::IntCounter,
  active_fetches: prometheus::IntGauge,
  entries: prometheus::IntGauge,
  retained_bytes: prometheus::IntGauge,
  request_latency_seconds: prometheus::Histogram,
}

impl BlobCacheMetrics {
  fn new(scope: &bd_server_stats::stats::Scope) -> Self {
    let scope = scope.scope("blob_cache");
    Self {
      requests: scope.counter("requests_total"),
      response_items: scope.counter("response_items_total"),
      response_bytes: scope.counter("response_bytes_total"),
      hits: scope.counter("hits_total"),
      fetches: scope.counter("fetches_total"),
      fetch_bytes: scope.counter("fetch_bytes_total"),
      evictions: scope.counter("evictions_total"),
      pressure_flushes: scope.counter("pressure_flushes_total"),
      not_found: scope.counter("not_found_total"),
      overloaded: scope.counter("overloads_total"),
      failures: scope.counter("failures_total"),
      active_fetches: scope.gauge("active_fetches"),
      entries: scope.gauge("entries"),
      retained_bytes: scope.gauge("retained_bytes"),
      request_latency_seconds: scope.histogram("request_latency_seconds"),
    }
  }
}

#[derive(Clone, Debug)]
enum BlobCacheError {
  BadRequest(String),
  Overloaded,
  NotFound,
  Storage,
}

impl BlobCache {
  #[must_use]
  pub fn new(
    blob_store: Arc<dyn BlobStore>,
    config: BlobCacheConfig,
    pressure: Arc<MemoryPressureController>,
    metrics_scope: &bd_server_stats::stats::Scope,
  ) -> Self {
    let metrics = BlobCacheMetrics::new(metrics_scope);
    let evictions = metrics.evictions.clone();
    let cache = Self {
      blob_store,
      config,
      pressure,
      entries: Cache::builder()
        .time_to_idle(config.idle_ttl)
        .weigher(|_key: &String, value: &Bytes| u32::try_from(value.len()).unwrap_or(u32::MAX))
        .eviction_listener(move |_key, _value, cause| {
          if cause.was_evicted() {
            evictions.inc();
          }
        })
        .build(),
      logical_idle_expiry: None,
      in_flight: Mutex::new(HashMap::new()),
      fetch_permits: Arc::new(Semaphore::new(config.max_fetches)),
      cache_generation: Arc::new(AtomicU64::new(0)),
      failures: AtomicU64::new(0),
      metrics,
    };
    cache.register_pressure_flush();
    cache
  }

  #[cfg(test)]
  fn new_with_time_provider(
    blob_store: Arc<dyn BlobStore>,
    config: BlobCacheConfig,
    pressure: Arc<MemoryPressureController>,
    time_provider: Arc<dyn TimeProvider>,
    metrics_scope: &bd_server_stats::stats::Scope,
  ) -> Self {
    let metrics = BlobCacheMetrics::new(metrics_scope);
    let evictions = metrics.evictions.clone();
    let last_access = Arc::new(Mutex::new(HashMap::new()));
    let eviction_last_access = Arc::clone(&last_access);
    let cache = Self {
      blob_store,
      config,
      pressure,
      entries: Cache::builder()
        .weigher(|_key: &String, value: &Bytes| u32::try_from(value.len()).unwrap_or(u32::MAX))
        .eviction_listener(move |key, _value, cause| {
          eviction_last_access.lock().remove(key.as_ref());
          if cause.was_evicted() {
            evictions.inc();
          }
        })
        .build(),
      logical_idle_expiry: Some(LogicalIdleExpiry {
        last_access,
        time_provider,
      }),
      in_flight: Mutex::new(HashMap::new()),
      fetch_permits: Arc::new(Semaphore::new(config.max_fetches)),
      cache_generation: Arc::new(AtomicU64::new(0)),
      failures: AtomicU64::new(0),
      metrics,
    };
    cache.register_pressure_flush();
    cache
  }

  #[must_use]
  pub fn request_timeout(&self) -> Duration {
    self.config.request_timeout
  }

  pub async fn read(self: &Arc<Self>, request: ReadBlobRangesRequest) -> ReadBlobRangesResponse {
    let started = std::time::Instant::now();
    self.metrics.requests.inc();
    let response = match Self::validate_request(&request) {
      Ok(()) => {
        match tokio::time::timeout(self.config.request_timeout, self.read_ranges(&request)).await {
          Ok(Ok(payloads)) => {
            let response = ReadBlobRangesResponse {
              result: Some(read_blob_ranges_response::Result::Success(
                BlobReadSuccess {
                  ranges: payloads
                    .into_iter()
                    .map(|payload| BlobRangeResult {
                      payload,
                      ..Default::default()
                    })
                    .collect(),
                  ..Default::default()
                },
              )),
              ..Default::default()
            };
            if response.compute_size()
              > u64::try_from(MAX_BLOB_READ_REQUEST_BYTES).unwrap_or(u64::MAX)
            {
              self.response_error(BlobCacheError::Overloaded)
            } else {
              response
            }
          },
          Ok(Err(error)) => self.response_error(error),
          Err(_) => self.response_error(BlobCacheError::Overloaded),
        }
      },
      Err(error) => self.response_error(BlobCacheError::BadRequest(error.to_string())),
    };
    self
      .metrics
      .request_latency_seconds
      .observe(started.elapsed().as_secs_f64());
    if let Some(read_blob_ranges_response::Result::Success(success)) = response.result.as_ref() {
      self
        .metrics
        .response_items
        .inc_by(u64::try_from(success.ranges.len()).unwrap_or(u64::MAX));
      self.metrics.response_bytes.inc_by(
        success
          .ranges
          .iter()
          .map(|range| u64::try_from(range.payload.len()).unwrap_or(u64::MAX))
          .sum(),
      );
    }
    self.record_cache_state();
    response
  }

  pub async fn snapshot(&self) -> BlobCacheSnapshot {
    self.expire_idle_entries().await;
    self.entries.run_pending_tasks().await;
    let snapshot = BlobCacheSnapshot {
      enabled: self.pressure.cache_headroom_bytes().is_some(),
      idle_ttl_seconds: self.config.idle_ttl.as_secs(),
      request_timeout_seconds: self.config.request_timeout.as_secs(),
      entry_count: self.entries.entry_count(),
      retained_bytes: self.entries.weighted_size(),
      active_fetches: self.in_flight.lock().len(),
      cache_reservations: self.pressure.cache_reservations(),
      cache_headroom_bytes: self.pressure.cache_headroom_bytes(),
      failure_total: self.failures.load(Ordering::Relaxed),
    };
    self.record_cache_state();
    snapshot
  }

  fn validate_request(request: &ReadBlobRangesRequest) -> Result<()> {
    ensure!(!request.blob_key.is_empty(), "blob request has no blob key");
    ensure!(!request.ranges.is_empty(), "blob request has no ranges");
    let mut response_bytes = 0_u64;
    for range in &request.ranges {
      ensure!(
        range.end > range.start,
        "blob request has an empty or invalid range"
      );
      response_bytes = response_bytes
        .checked_add(range.end - range.start)
        .ok_or_else(|| anyhow!("blob response bytes overflow"))?;
    }
    ensure!(
      response_bytes <= u64::try_from(MAX_BLOB_READ_REQUEST_BYTES).unwrap_or(u64::MAX),
      "blob response exceeds the unary body limit"
    );
    Ok(())
  }

  async fn read_ranges(
    self: &Arc<Self>,
    request: &ReadBlobRangesRequest,
  ) -> std::result::Result<Vec<Bytes>, BlobCacheError> {
    if self.pressure.is_overloaded() {
      return Err(BlobCacheError::Overloaded);
    }
    let key = request.blob_key.to_string();
    self.expire_idle_entry(&key).await;
    let blob = match self.entries.get(&key).await {
      Some(blob) => {
        self.record_logical_access(&key);
        self.metrics.hits.inc();
        blob
      },
      None => self.fetch_blob(key).await?,
    };
    request
      .ranges
      .iter()
      .map(|range| {
        let end = usize::try_from(range.end).map_err(|_| {
          BlobCacheError::BadRequest("range end does not fit in memory".to_string())
        })?;
        let start = usize::try_from(range.start).map_err(|_| {
          BlobCacheError::BadRequest("range start does not fit in memory".to_string())
        })?;
        if end > blob.len() {
          return Err(BlobCacheError::BadRequest(
            "requested range exceeds blob length".to_string(),
          ));
        }
        Ok(blob.slice(start .. end))
      })
      .collect()
  }

  async fn fetch_blob(self: &Arc<Self>, key: String) -> std::result::Result<Bytes, BlobCacheError> {
    let (fetch, started) = {
      let mut in_flight = self.in_flight.lock();
      if let Some(fetch) = in_flight.get(&key).cloned() {
        (fetch, false)
      } else {
        // Refuse new keys when storage fetch capacity is saturated; existing same-key waiters
        // still join their shared worker without consuming another permit.
        let permit = self
          .fetch_permits
          .clone()
          .try_acquire_owned()
          .map_err(|_| BlobCacheError::Overloaded)?;
        let cache = self.entries.clone();
        let blob_store = Arc::clone(&self.blob_store);
        let pressure = self.pressure.clone();
        let metrics = self.metrics.clone();
        let cache_generation = Arc::clone(&self.cache_generation);
        let fetch_generation = cache_generation.load(Ordering::Relaxed);
        let logical_idle_expiry = self.logical_idle_expiry.clone();
        let fetch_key = key.clone();
        let fetch = Arc::new(
          async move {
            let _permit = permit;
            metrics.fetches.inc();
            let reservation = Arc::new(Mutex::new(None));
            let admission = {
              let pressure = Arc::clone(&pressure);
              let reservation = Arc::clone(&reservation);
              move |content_length| {
                let Some(reservation_guard) = pressure.try_reserve_cache_bytes(content_length)
                else {
                  return false;
                };
                *reservation.lock() = Some(reservation_guard);
                true
              }
            };
            let blob = blob_store
              .get_with_cache_admission(&BlobKey::from(fetch_key.clone()), &admission)
              .await
              .map_err(|error| blob_store_error(&error))?;
            metrics
              .fetch_bytes
              .inc_by(u64::try_from(blob.len()).unwrap_or(u64::MAX));
            // A pressure flush invalidates this generation. Do not reinsert an object fetched
            // before that transition, even if pressure recovers before the read completes.
            if cache_generation.load(Ordering::Relaxed) == fetch_generation
              && !pressure.is_overloaded()
            {
              cache.insert(fetch_key.clone(), blob.clone()).await;
              if let Some(logical_idle_expiry) = logical_idle_expiry {
                logical_idle_expiry
                  .last_access
                  .lock()
                  .insert(fetch_key, logical_idle_expiry.time_provider.now());
              }
            }
            Ok(blob)
          }
          .boxed()
          .shared(),
        );
        in_flight.insert(key.clone(), Arc::clone(&fetch));
        (fetch, true)
      }
    };
    if started {
      let cache = Arc::clone(self);
      let fetch_key = key.clone();
      let worker = Arc::clone(&fetch);
      // The worker owns the shared future so a timed-out RPC cannot strand an admission
      // reservation or in-flight entry when no later caller polls the fetch.
      tokio::spawn(async move {
        let _ = worker.as_ref().clone().await;
        cache.remove_in_flight_fetch(&fetch_key, &worker);
      });
    }
    self
      .metrics
      .active_fetches
      .set(i64::try_from(self.in_flight.lock().len()).unwrap_or(i64::MAX));
    let result = fetch.as_ref().clone().await;
    self.remove_in_flight_fetch(&key, &fetch);
    result
  }

  fn remove_in_flight_fetch(&self, key: &str, completed: &Arc<SharedFetch>) {
    let mut in_flight = self.in_flight.lock();
    if in_flight
      .get(key)
      .is_some_and(|fetch| Arc::ptr_eq(fetch, completed))
    {
      in_flight.remove(key);
    }
    self
      .metrics
      .active_fetches
      .set(i64::try_from(in_flight.len()).unwrap_or(i64::MAX));
  }

  fn register_pressure_flush(&self) {
    let entries = self.entries.clone();
    let cache_generation = Arc::clone(&self.cache_generation);
    let metrics = self.metrics.clone();
    let logical_idle_expiry = self.logical_idle_expiry.clone();
    let overload_handler: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
      cache_generation.fetch_add(1, Ordering::Relaxed);
      entries.invalidate_all();
      if let Some(logical_idle_expiry) = logical_idle_expiry.as_ref() {
        logical_idle_expiry.last_access.lock().clear();
      }
      metrics.pressure_flushes.inc();
    });
    self.pressure.register_overload_handler(&overload_handler);
  }

  fn response_error(&self, error: BlobCacheError) -> ReadBlobRangesResponse {
    let (status, error_message) = match error {
      BlobCacheError::BadRequest(message) => (
        BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_BAD_REQUEST,
        message,
      ),
      BlobCacheError::Overloaded => {
        self.metrics.overloaded.inc();
        (
          BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_OVERLOADED,
          "blob cache overloaded".to_string(),
        )
      },
      BlobCacheError::NotFound => {
        self.metrics.not_found.inc();
        (
          BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_NOT_FOUND,
          "blob not found".to_string(),
        )
      },
      BlobCacheError::Storage => (
        BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_STORAGE,
        "blob storage failure".to_string(),
      ),
    };
    self.failures.fetch_add(1, Ordering::Relaxed);
    self.metrics.failures.inc();
    ReadBlobRangesResponse {
      result: Some(read_blob_ranges_response::Result::Failure(
        BlobReadFailure {
          status: status.into(),
          error_message: error_message.into(),
          ..Default::default()
        },
      )),
      ..Default::default()
    }
  }

  fn record_cache_state(&self) {
    self
      .metrics
      .entries
      .set(i64::try_from(self.entries.entry_count()).unwrap_or(i64::MAX));
    self
      .metrics
      .retained_bytes
      .set(i64::try_from(self.entries.weighted_size()).unwrap_or(i64::MAX));
    self
      .metrics
      .active_fetches
      .set(i64::try_from(self.in_flight.lock().len()).unwrap_or(i64::MAX));
  }

  async fn expire_idle_entries(&self) {
    let Some(logical_idle_expiry) = self.logical_idle_expiry.as_ref() else {
      return;
    };
    let now = logical_idle_expiry.time_provider.now();
    let idle_ttl = time::Duration::try_from(self.config.idle_ttl)
      .expect("validated blob cache idle TTL fits time duration");
    let expired: Vec<_> = logical_idle_expiry
      .last_access
      .lock()
      .iter()
      .filter(|(_, accessed)| now >= **accessed + idle_ttl)
      .map(|(key, _)| key.clone())
      .collect();
    for key in expired {
      self.entries.invalidate(&key).await;
      logical_idle_expiry.last_access.lock().remove(&key);
    }
  }

  async fn expire_idle_entry(&self, key: &str) {
    let Some(logical_idle_expiry) = self.logical_idle_expiry.as_ref() else {
      return;
    };
    let now = logical_idle_expiry.time_provider.now();
    let idle_ttl = time::Duration::try_from(self.config.idle_ttl)
      .expect("validated blob cache idle TTL fits time duration");
    let expired = logical_idle_expiry
      .last_access
      .lock()
      .get(key)
      .is_some_and(|accessed| now >= *accessed + idle_ttl);
    if expired {
      self.entries.invalidate(key).await;
      logical_idle_expiry.last_access.lock().remove(key);
    }
  }

  fn record_logical_access(&self, key: &str) {
    if let Some(logical_idle_expiry) = self.logical_idle_expiry.as_ref() {
      logical_idle_expiry
        .last_access
        .lock()
        .insert(key.to_string(), logical_idle_expiry.time_provider.now());
    }
  }
}

fn blob_store_error(error: &BlobStoreError) -> BlobCacheError {
  match error {
    BlobStoreError::NotFound { .. } => BlobCacheError::NotFound,
    BlobStoreError::AdmissionRejected { .. } => BlobCacheError::Overloaded,
    BlobStoreError::InvalidRange { .. } | BlobStoreError::Read { .. } => BlobCacheError::Storage,
  }
}
