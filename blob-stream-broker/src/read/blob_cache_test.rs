#![allow(clippy::unwrap_used)]

use super::*;
use crate::write::memory_pressure::{MemoryPressureSample, MemoryPressureSampler};
use anyhow::Result;
use async_trait::async_trait;
use bd_runtime_config::loader::Loader;
use bd_server_stats::stats::Collector;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_blob_store::{
  BlobCacheAdmission,
  BlobStoreError,
  BlobStoreResult,
  ByteRange,
  InMemoryBlobStore,
};
use blob_stream_proto::protos::blobstream::v1::broker::{
  BlobRangeRequest,
  BlobReadFailureStatus,
  read_blob_ranges_response,
};
use blob_stream_test_utils::ManualTimeProvider;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use time::OffsetDateTime;
use tokio::sync::Notify;

struct GatedBlobStore {
  store: InMemoryBlobStore,
  get_calls: AtomicU64,
  entered: Notify,
  release: Notify,
}

impl GatedBlobStore {
  fn new() -> Self {
    Self {
      store: InMemoryBlobStore::new(),
      get_calls: AtomicU64::new(0),
      entered: Notify::new(),
      release: Notify::new(),
    }
  }
}

#[async_trait]
impl BlobStore for GatedBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    self.store.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    self.store.get_range(key, range).await
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    let blob = self.store.get_with_cache_admission(key, admission).await?;
    self.get_calls.fetch_add(1, Ordering::Relaxed);
    self.entered.notify_waiters();
    self.release.notified().await;
    Ok(blob)
  }
}

struct GatedFailingBlobStore {
  get_calls: AtomicU64,
  entered: Notify,
  released: AtomicBool,
  release: Notify,
}

impl GatedFailingBlobStore {
  fn new() -> Self {
    Self {
      get_calls: AtomicU64::new(0),
      entered: Notify::new(),
      released: AtomicBool::new(false),
      release: Notify::new(),
    }
  }

  fn release(&self) {
    self.released.store(true, Ordering::Relaxed);
    self.release.notify_waiters();
  }
}

#[async_trait]
impl BlobStore for GatedFailingBlobStore {
  async fn put(&self, _key: &BlobKey, _payload: Bytes) -> Result<()> {
    Ok(())
  }

  async fn get_range(&self, _key: &BlobKey, _range: ByteRange) -> BlobStoreResult<Bytes> {
    unreachable!("blob cache uses full-object reads")
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    let _ = admission;
    self.get_calls.fetch_add(1, Ordering::Relaxed);
    self.entered.notify_waiters();
    while !self.released.load(Ordering::Relaxed) {
      let notified = self.release.notified();
      if self.released.load(Ordering::Relaxed) {
        break;
      }
      notified.await;
    }
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("injected storage failure"),
    })
  }
}

struct TestPressureSource {
  samples: Mutex<VecDeque<Result<MemoryPressureSample>>>,
  fallback: MemoryPressureSample,
}

impl TestPressureSource {
  fn new(fallback: MemoryPressureSample) -> Self {
    Self {
      samples: Mutex::new(VecDeque::new()),
      fallback,
    }
  }

  fn push_sample(&self, sample: Result<MemoryPressureSample>) {
    self.samples.lock().push_back(sample);
  }
}

impl MemoryPressureSampler for TestPressureSource {
  fn sample(&self) -> Result<MemoryPressureSample> {
    self.samples.lock().pop_front().unwrap_or(Ok(self.fallback))
  }
}

fn pressure(allocated_bytes: u64, limit_bytes: u64) -> Arc<MemoryPressureController> {
  MemoryPressureController::new_for_test(
    Arc::new(TestPressureSource::new(MemoryPressureSample {
      allocated_bytes,
      limit_bytes,
    })),
    &Collector::default().scope("blob_cache_test"),
  )
}

fn cache(store: Arc<dyn BlobStore>, pressure: Arc<MemoryPressureController>) -> Arc<BlobCache> {
  cache_with_idle_ttl(store, pressure, Duration::from_secs(30))
}

fn cache_with_idle_ttl(
  store: Arc<dyn BlobStore>,
  pressure: Arc<MemoryPressureController>,
  idle_ttl: Duration,
) -> Arc<BlobCache> {
  Arc::new(BlobCache::new(
    store,
    BlobCacheConfig {
      request_timeout: Duration::from_secs(1),
      idle_ttl,
    },
    pressure,
    &Collector::default().scope("blob_cache_test"),
  ))
}

fn cache_with_idle_ttl_and_clock(
  store: Arc<dyn BlobStore>,
  pressure: Arc<MemoryPressureController>,
  idle_ttl: Duration,
  time_provider: Arc<dyn bd_time::TimeProvider>,
) -> Arc<BlobCache> {
  Arc::new(BlobCache::new_with_time_provider(
    store,
    BlobCacheConfig {
      request_timeout: Duration::from_secs(1),
      idle_ttl,
    },
    pressure,
    time_provider,
    &Collector::default().scope("blob_cache_test"),
  ))
}

struct CountingBlobStore {
  store: InMemoryBlobStore,
  get_calls: AtomicU64,
}

impl CountingBlobStore {
  fn new() -> Self {
    Self {
      store: InMemoryBlobStore::new(),
      get_calls: AtomicU64::new(0),
    }
  }
}

#[async_trait]
impl BlobStore for CountingBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    self.store.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    self.store.get_range(key, range).await
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    self.get_calls.fetch_add(1, Ordering::Relaxed);
    self.store.get_with_cache_admission(key, admission).await
  }
}

struct FailingBlobStore;

#[async_trait]
impl BlobStore for FailingBlobStore {
  async fn put(&self, _key: &BlobKey, _payload: Bytes) -> Result<()> {
    Ok(())
  }

  async fn get_range(&self, _key: &BlobKey, _range: ByteRange) -> BlobStoreResult<Bytes> {
    unreachable!("blob cache uses full-object reads")
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    let _ = admission;
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("private bucket credentials failed"),
    })
  }
}

fn request(ranges: &[(u64, u64)]) -> ReadBlobRangesRequest {
  ReadBlobRangesRequest {
    blob_key: "topic/blob".into(),
    ranges: ranges
      .iter()
      .map(|&(start, end)| BlobRangeRequest {
        start,
        end,
        ..Default::default()
      })
      .collect(),
    ..Default::default()
  }
}

#[test]
fn config_reads_idle_ttl_from_feature_flags() {
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(BLOB_CACHE_IDLE_TTL_FEATURE_FLAG, 1_234),
  ));
  let feature_flags = feature_flags.snapshot_watch();

  let config =
    BlobCacheConfig::from_broker_config(&BrokerConfig::new(), Some(&feature_flags)).unwrap();

  assert_eq!(config.idle_ttl, Duration::from_millis(1_234));
}

#[test]
fn config_defaults_idle_ttl_to_ten_seconds() {
  let config = BlobCacheConfig::from_broker_config(&BrokerConfig::new(), None).unwrap();

  assert_eq!(config.idle_ttl, Duration::from_secs(10));
}

#[test]
fn config_rejects_nonpositive_idle_ttl_feature_flags() {
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(BLOB_CACHE_IDLE_TTL_FEATURE_FLAG, 0),
  ));
  let feature_flags = feature_flags.snapshot_watch();

  assert!(BlobCacheConfig::from_broker_config(&BrokerConfig::new(), Some(&feature_flags)).is_err());
}

#[tokio::test]
async fn reads_exact_ranges_and_retains_the_full_blob() {
  let store = Arc::new(InMemoryBlobStore::new());
  store
    .put(
      &BlobKey::from("topic/blob"),
      Bytes::from_static(b"abcdefgh"),
    )
    .await
    .unwrap();
  let cache = cache(store, pressure(0, 1_000));

  let response = cache.read(request(&[(1, 3), (5, 8)])).await;

  let Some(read_blob_ranges_response::Result::Success(success)) = response.result else {
    panic!("expected success");
  };
  assert_eq!(success.ranges[0].payload, Bytes::from_static(b"bc"));
  assert_eq!(success.ranges[1].payload, Bytes::from_static(b"fgh"));
  assert_eq!(cache.snapshot().await.entry_count, 1);
}

#[tokio::test]
async fn cache_hit_reuses_one_full_blob_fetch() {
  let store = Arc::new(CountingBlobStore::new());
  store
    .put(
      &BlobKey::from("topic/blob"),
      Bytes::from_static(b"abcdefgh"),
    )
    .await
    .unwrap();
  let cache = cache(Arc::clone(&store) as Arc<dyn BlobStore>, pressure(0, 1_000));

  assert!(matches!(
    cache.read(request(&[(0, 2)])).await.result,
    Some(read_blob_ranges_response::Result::Success(_))
  ));
  assert!(matches!(
    cache.read(request(&[(5, 8)])).await.result,
    Some(read_blob_ranges_response::Result::Success(_))
  ));

  let snapshot = cache.snapshot().await;
  assert_eq!(store.get_calls.load(Ordering::Relaxed), 1);
  assert_eq!(snapshot.entry_count, 1);
  assert_eq!(snapshot.retained_bytes, 8);
}

#[tokio::test]
async fn idle_expiry_reloads_the_full_blob_on_the_next_read() {
  let store = Arc::new(CountingBlobStore::new());
  store
    .put(
      &BlobKey::from("topic/blob"),
      Bytes::from_static(b"abcdefgh"),
    )
    .await
    .unwrap();
  let time_provider = Arc::new(ManualTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let cache = cache_with_idle_ttl_and_clock(
    Arc::clone(&store) as Arc<dyn BlobStore>,
    pressure(0, 1_000),
    Duration::from_secs(1),
    time_provider.clone(),
  );

  assert!(matches!(
    cache.read(request(&[(0, 2)])).await.result,
    Some(read_blob_ranges_response::Result::Success(_))
  ));
  time_provider.advance(time::Duration::seconds(2));
  assert_eq!(cache.snapshot().await.entry_count, 0);

  assert!(matches!(
    cache.read(request(&[(5, 8)])).await.result,
    Some(read_blob_ranges_response::Result::Success(_))
  ));
  assert_eq!(store.get_calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn missing_blob_is_an_authoritative_failure() {
  let cache = cache(Arc::new(InMemoryBlobStore::new()), pressure(0, 1_000));

  let response = cache.read(request(&[(0, 1)])).await;

  let Some(read_blob_ranges_response::Result::Failure(failure)) = response.result else {
    panic!("expected failure");
  };
  assert_eq!(
    failure.status,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_NOT_FOUND.into()
  );
  assert_eq!(cache.snapshot().await.entry_count, 0);
}

#[tokio::test]
async fn missing_blob_is_not_negatively_cached() {
  let store = Arc::new(CountingBlobStore::new());
  let cache = cache(Arc::clone(&store) as Arc<dyn BlobStore>, pressure(0, 1_000));

  for _ in 0 .. 2 {
    let response = cache.read(request(&[(0, 1)])).await;
    let Some(read_blob_ranges_response::Result::Failure(failure)) = response.result else {
      panic!("expected failure");
    };
    assert_eq!(
      failure.status,
      BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_NOT_FOUND.into()
    );
  }

  assert_eq!(store.get_calls.load(Ordering::Relaxed), 2);
  assert_eq!(cache.snapshot().await.entry_count, 0);
}

#[tokio::test]
async fn invalid_range_does_not_read_or_cache() {
  let cache = cache(Arc::new(InMemoryBlobStore::new()), pressure(0, 1_000));

  let response = cache.read(request(&[(4, 4)])).await;

  let Some(read_blob_ranges_response::Result::Failure(failure)) = response.result else {
    panic!("expected failure");
  };
  assert_eq!(
    failure.status,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_BAD_REQUEST.into()
  );
  assert_eq!(cache.snapshot().await.entry_count, 0);
}

#[tokio::test]
async fn request_over_the_wire_response_limit_does_not_read_or_cache() {
  let store = Arc::new(CountingBlobStore::new());
  let cache = cache(Arc::clone(&store) as Arc<dyn BlobStore>, pressure(0, 1_000));
  let response = cache
    .read(request(&[(
      0,
      u64::try_from(MAX_BLOB_READ_REQUEST_BYTES).unwrap() + 1,
    )]))
    .await;

  let Some(read_blob_ranges_response::Result::Failure(failure)) = response.result else {
    panic!("expected failure");
  };
  assert_eq!(
    failure.status,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_BAD_REQUEST.into()
  );
  assert_eq!(store.get_calls.load(Ordering::Relaxed), 0);
  assert_eq!(cache.snapshot().await.entry_count, 0);
}

#[tokio::test]
async fn insufficient_pressure_headroom_rejects_cache_admission() {
  let store = Arc::new(InMemoryBlobStore::new());
  store
    .put(&BlobKey::from("topic/blob"), Bytes::from(vec![0; 64]))
    .await
    .unwrap();
  let cache = cache(store, pressure(790, 1_000));

  let response = cache.read(request(&[(0, 1)])).await;

  let Some(read_blob_ranges_response::Result::Failure(failure)) = response.result else {
    panic!("expected failure");
  };
  assert_eq!(
    failure.status,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_OVERLOADED.into()
  );
}

#[tokio::test]
async fn overload_flushes_retained_entries_before_rejecting_new_reads() {
  let store = Arc::new(CountingBlobStore::new());
  store
    .put(
      &BlobKey::from("topic/blob"),
      Bytes::from_static(b"abcdefgh"),
    )
    .await
    .unwrap();
  let source = Arc::new(TestPressureSource::new(MemoryPressureSample {
    allocated_bytes: 0,
    limit_bytes: 1_000,
  }));
  let pressure = MemoryPressureController::new_for_test(
    source.clone(),
    &Collector::default().scope("blob_cache_test"),
  );
  let cache = cache(
    Arc::clone(&store) as Arc<dyn BlobStore>,
    Arc::clone(&pressure),
  );

  assert!(matches!(
    cache.read(request(&[(0, 1)])).await.result,
    Some(read_blob_ranges_response::Result::Success(_))
  ));
  assert_eq!(cache.snapshot().await.entry_count, 1);
  source.push_sample(Ok(MemoryPressureSample {
    allocated_bytes: 800,
    limit_bytes: 1_000,
  }));
  assert!(pressure.try_reserve_cache_bytes(0).is_none());
  assert_eq!(cache.snapshot().await.entry_count, 0);

  let response = cache.read(request(&[(0, 1)])).await;

  let Some(read_blob_ranges_response::Result::Failure(failure)) = response.result else {
    panic!("expected failure");
  };
  assert_eq!(
    failure.status,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_OVERLOADED.into()
  );
  assert_eq!(cache.snapshot().await.entry_count, 0);
}

#[tokio::test]
async fn overload_flush_does_not_admit_a_fetch_started_before_the_transition() {
  let store = Arc::new(GatedBlobStore::new());
  store
    .put(
      &BlobKey::from("topic/blob"),
      Bytes::from_static(b"abcdefgh"),
    )
    .await
    .unwrap();
  let source = Arc::new(TestPressureSource::new(MemoryPressureSample {
    allocated_bytes: 0,
    limit_bytes: 1_000,
  }));
  let pressure = MemoryPressureController::new_for_test(
    source.clone(),
    &Collector::default().scope("blob_cache_test"),
  );
  let cache = cache(
    Arc::clone(&store) as Arc<dyn BlobStore>,
    Arc::clone(&pressure),
  );

  let fetching_cache = Arc::clone(&cache);
  let fetching = tokio::spawn(async move { fetching_cache.read(request(&[(0, 1)])).await });
  store.entered.notified().await;
  source.push_sample(Ok(MemoryPressureSample {
    allocated_bytes: 800,
    limit_bytes: 1_000,
  }));
  assert!(pressure.try_reserve_cache_bytes(0).is_none());

  let overloaded = cache.read(request(&[(0, 1)])).await;
  assert!(matches!(
    overloaded.result,
    Some(read_blob_ranges_response::Result::Failure(_))
  ));
  store.release.notify_waiters();
  assert!(matches!(
    fetching.await.unwrap().result,
    Some(read_blob_ranges_response::Result::Success(_))
  ));
  assert_eq!(cache.snapshot().await.entry_count, 0);
}

#[tokio::test]
async fn shares_one_full_blob_fetch_for_concurrent_requests() {
  let store = Arc::new(GatedBlobStore::new());
  store
    .put(
      &BlobKey::from("topic/blob"),
      Bytes::from_static(b"abcdefgh"),
    )
    .await
    .unwrap();
  let cache = cache(Arc::clone(&store) as Arc<dyn BlobStore>, pressure(0, 1_000));

  let first_cache = Arc::clone(&cache);
  let first = tokio::spawn(async move { first_cache.read(request(&[(0, 2)])).await });
  store.entered.notified().await;
  let second_cache = Arc::clone(&cache);
  let second = tokio::spawn(async move { second_cache.read(request(&[(4, 8)])).await });
  tokio::task::yield_now().await;
  assert_eq!(store.get_calls.load(Ordering::Relaxed), 1);

  store.release.notify_waiters();
  let first = first.await.unwrap();
  let second = second.await.unwrap();
  assert!(matches!(
    first.result,
    Some(read_blob_ranges_response::Result::Success(_))
  ));
  assert!(matches!(
    second.result,
    Some(read_blob_ranges_response::Result::Success(_))
  ));
  assert_eq!(store.get_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn concurrent_failed_fetches_share_one_request_and_release_admission() {
  let store = Arc::new(GatedFailingBlobStore::new());
  let pressure = pressure(0, 1_000);
  let cache = cache(
    Arc::clone(&store) as Arc<dyn BlobStore>,
    Arc::clone(&pressure),
  );

  let first_cache = Arc::clone(&cache);
  let first = tokio::spawn(async move { first_cache.read(request(&[(0, 1)])).await });
  store.entered.notified().await;
  let second_cache = Arc::clone(&cache);
  let second = tokio::spawn(async move { second_cache.read(request(&[(1, 2)])).await });
  tokio::task::yield_now().await;
  assert_eq!(store.get_calls.load(Ordering::Relaxed), 1);

  store.release();
  for response in [first.await.unwrap(), second.await.unwrap()] {
    let Some(read_blob_ranges_response::Result::Failure(failure)) = response.result else {
      panic!("expected storage failure");
    };
    assert_eq!(
      failure.status,
      BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_STORAGE.into()
    );
  }
  assert_eq!(cache.snapshot().await.active_fetches, 0);
  assert_eq!(pressure.cache_reservations(), 0);

  let retry = cache.read(request(&[(0, 1)])).await;
  assert!(matches!(
    retry.result,
    Some(read_blob_ranges_response::Result::Failure(_))
  ));
  assert_eq!(store.get_calls.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn timed_out_request_keeps_fetch_cleanup_running() {
  let store = Arc::new(GatedBlobStore::new());
  store
    .put(
      &BlobKey::from("topic/blob"),
      Bytes::from_static(b"abcdefgh"),
    )
    .await
    .unwrap();
  let pressure = pressure(0, 1_000);
  let cache = Arc::new(BlobCache::new(
    Arc::clone(&store) as Arc<dyn BlobStore>,
    BlobCacheConfig {
      request_timeout: Duration::from_millis(1),
      idle_ttl: Duration::from_secs(30),
    },
    Arc::clone(&pressure),
    &Collector::default().scope("blob_cache_test"),
  ));

  let cache_for_request = Arc::clone(&cache);
  let request = tokio::spawn(async move { cache_for_request.read(request(&[(0, 1)])).await });
  store.entered.notified().await;
  tokio::time::advance(Duration::from_millis(1)).await;
  assert!(matches!(
    request.await.unwrap().result,
    Some(read_blob_ranges_response::Result::Failure(_))
  ));
  assert_eq!(cache.snapshot().await.active_fetches, 1);
  assert_eq!(pressure.cache_reservations(), 8);

  store.release.notify_waiters();
  for _ in 0 .. 3 {
    tokio::task::yield_now().await;
  }
  assert_eq!(cache.snapshot().await.active_fetches, 0);
  assert_eq!(pressure.cache_reservations(), 0);
}

#[tokio::test]
async fn content_length_admission_rejects_a_blob_that_exceeds_headroom() {
  let store = Arc::new(InMemoryBlobStore::new());
  store
    .put(&BlobKey::from("topic/blob"), Bytes::from(vec![0; 65]))
    .await
    .unwrap();
  let cache = cache(store, pressure(750, 1_000));

  let response = cache.read(request(&[(0, 1)])).await;

  let Some(read_blob_ranges_response::Result::Failure(failure)) = response.result else {
    panic!("expected failure");
  };
  assert_eq!(
    failure.status,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_OVERLOADED.into()
  );
  assert_eq!(cache.snapshot().await.entry_count, 0);
}

#[tokio::test]
async fn storage_failures_have_a_sanitized_public_message() {
  let pressure = pressure(0, 1_000);
  let cache = cache(Arc::new(FailingBlobStore), Arc::clone(&pressure));

  let response = cache.read(request(&[(0, 1)])).await;

  let Some(read_blob_ranges_response::Result::Failure(failure)) = response.result else {
    panic!("expected failure");
  };
  assert_eq!(
    failure.status,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_STORAGE.into()
  );
  assert_eq!(failure.error_message.as_str(), "blob storage failure");
  assert!(!failure.error_message.contains("private bucket"));
  assert_eq!(pressure.cache_reservations(), 0);
}

#[tokio::test]
async fn snapshot_contains_only_aggregate_cache_state() {
  let store = Arc::new(InMemoryBlobStore::new());
  store
    .put(&BlobKey::from("topic/blob"), Bytes::from_static(b"abcd"))
    .await
    .expect("test blob writes successfully");
  let cache = cache(store, pressure(0, 1_000));
  assert!(matches!(
    cache.read(request(&[(0, 1)])).await.result,
    Some(read_blob_ranges_response::Result::Success(_))
  ));

  let snapshot = serde_json::to_string(&cache.snapshot().await).expect("snapshot serializes");

  assert!(snapshot.contains("entry_count"));
  assert!(!snapshot.contains("topic/blob"));
  assert!(!snapshot.contains("abcd"));
}
