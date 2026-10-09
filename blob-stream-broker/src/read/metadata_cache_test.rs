#![allow(clippy::unwrap_used)]

use super::*;
use async_trait::async_trait;
use bd_runtime_config::loader::Loader;
use bd_server_stats::stats::Collector;
use bd_server_stats::test::util::stats::Helper;
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_blob_store::BlobKey;
use blob_stream_metadata_store::{MetadataWriteResult, SegmentMetadata};
use blob_stream_proto::protos::blobstream::v1::broker::{
  FullRecoveryMetadataCoverage,
  MetadataPartitionBound,
  TailMetadataCoverage,
  read_metadata_window_request,
};
use blob_stream_proto::protos::blobstream::v1::config::BrokerConfig;
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{BatchMetadata, ByteRange, Compression, SeqRange};
use prometheus::labels;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;
use tokio::time::timeout;

struct CountingMetadataStore {
  scans: AtomicUsize,
  observed_bounds: Mutex<Vec<Option<SnowflakeId>>>,
  segments: Vec<SegmentMetadata>,
}

struct GatedMetadataStore {
  scans: AtomicUsize,
  observed_bounds: Mutex<Vec<Option<SnowflakeId>>>,
  segments: Vec<SegmentMetadata>,
  scan_started: Semaphore,
  release_scan: Semaphore,
}

struct FailingMetadataStore;

#[derive(Debug)]
struct CountingFeatureFlags {
  flags: DefaultFeatureFlags,
  reads: Arc<AtomicUsize>,
}

impl FeatureFlags for CountingFeatureFlags {
  fn feature_enabled(&self, name: &str, default: bool) -> bool {
    self.flags.feature_enabled(name, default)
  }

  fn get_bool(&self, name: &str, default: bool) -> bool {
    self.flags.get_bool(name, default)
  }

  fn get_integer(&self, name: &str, default: u64) -> u64 {
    self.reads.fetch_add(1, Ordering::Relaxed);
    self.flags.get_integer(name, default)
  }

  fn get_string(&self, name: &str, default: &Arc<String>) -> Arc<String> {
    self.flags.get_string(name, default)
  }
}

#[test]
fn shared_refill_error_retains_overload_reason_and_source() {
  let original = anyhow!(MetadataCacheOverload {
    reason: MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_ENTRY_ITEMS,
    message: "metadata cache overloaded: entry limit",
  });
  let shared = SharedRefillError {
    reason: overload_reason(&original),
    source: Arc::new(original),
  };
  let error = anyhow!(shared.clone());
  assert_eq!(
    overload_reason(&error),
    Some(MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_ENTRY_ITEMS)
  );
  assert!(
    error
      .chain()
      .any(<dyn std::error::Error>::is::<MetadataCacheOverload>)
  );
  assert_eq!(shared.to_string(), "metadata cache overloaded: entry limit");
}

#[async_trait]
impl MetadataStore for CountingMetadataStore {
  async fn write_segment(
    &self,
    _metadata: SegmentMetadata,
    _fences: Option<&[blob_stream_metadata_store::ProducerPartitionFence]>,
    _now_ts_ms: i64,
  ) -> MetadataWriteResult {
    Ok(())
  }

  async fn scan_window_from_snowflake(
    &self,
    _window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    _consistency: StoreConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    self.scans.fetch_add(1, Ordering::Relaxed);
    self.observed_bounds.lock().push(min_snowflake);
    Ok(self.segments.clone())
  }
}

#[async_trait]
impl MetadataStore for GatedMetadataStore {
  async fn write_segment(
    &self,
    _metadata: SegmentMetadata,
    _fences: Option<&[blob_stream_metadata_store::ProducerPartitionFence]>,
    _now_ts_ms: i64,
  ) -> MetadataWriteResult {
    Ok(())
  }

  async fn scan_window_from_snowflake(
    &self,
    _window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    _consistency: StoreConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    self.scans.fetch_add(1, Ordering::Relaxed);
    self.observed_bounds.lock().push(min_snowflake);
    self.scan_started.add_permits(1);
    self.release_scan.acquire().await.unwrap().forget();
    Ok(self.segments.clone())
  }
}

#[async_trait]
impl MetadataStore for FailingMetadataStore {
  async fn write_segment(
    &self,
    _metadata: SegmentMetadata,
    _fences: Option<&[blob_stream_metadata_store::ProducerPartitionFence]>,
    _now_ts_ms: i64,
  ) -> MetadataWriteResult {
    Ok(())
  }

  async fn scan_window_from_snowflake(
    &self,
    _window: &TopicWindowKey,
    _min_snowflake: Option<SnowflakeId>,
    _consistency: StoreConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    Err(anyhow!(
      "DynamoDB table private-segment-metadata unavailable"
    ))
  }
}

fn cache_config(maximum_age: Duration, coalescing_window: StdDuration) -> MetadataCacheConfig {
  MetadataCacheConfig {
    tail_max_bytes: 1024 * 1024,
    recovery_max_bytes: 1024 * 1024,
    seal_clock_skew: DEFAULT_SEAL_MAX_CLOCK_SKEW,
    coalescing_window,
    limits: MetadataCacheLimits {
      request_timeout: StdDuration::from_secs(1),
      max_waiters_per_key: DEFAULT_MAX_WAITERS_PER_KEY,
      max_waiters: DEFAULT_MAX_WAITERS,
      max_refills: DEFAULT_MAX_REFILLS,
      max_request_partitions: DEFAULT_MAX_REQUEST_PARTITIONS,
      max_response_items: DEFAULT_MAX_RESPONSE_ITEMS,
      max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
      max_entry_items: DEFAULT_MAX_ENTRY_ITEMS,
    },
    topics: HashMap::from([(
      "topic".to_string(),
      TopicCacheContract {
        partition_count: 1,
        num_writers: 1,
        metadata_window_size: Duration::minutes(5),
        metadata_cache_max_age: maximum_age,
        max_metadata_publication_lag: Duration::seconds(15),
      },
    )]),
  }
}

#[test]
fn topic_contract_seal_follows_publication_lag() {
  let mut contract = cache_config(Duration::ZERO, StdDuration::ZERO)
    .topics
    .remove("topic")
    .unwrap();
  assert_eq!(contract.max_metadata_publication_lag, Duration::seconds(15));
  contract.max_metadata_publication_lag = Duration::seconds(30);
  assert_eq!(contract.max_metadata_publication_lag, Duration::seconds(30));
  assert_eq!(
    TopicCacheContract::from_proto(&TopicConfig::new())
      .unwrap()
      .max_metadata_publication_lag,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG
  );
}

#[test]
fn runtime_feature_flags_override_metadata_cache_defaults() {
  let mut runtime = RuntimeConfig::new();
  runtime.broker = Some(BrokerConfig::new()).into();
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag(RECOVERY_CACHE_MAX_BYTES_FEATURE_FLAG, 1_024)
      .with_integer_flag(MAX_WAITERS_PER_KEY_FEATURE_FLAG, 2)
      .with_integer_flag(MAX_WAITERS_FEATURE_FLAG, 3)
      .with_integer_flag(MAX_REFILLS_FEATURE_FLAG, 4)
      .with_integer_flag(MAX_REQUEST_PARTITIONS_FEATURE_FLAG, 5)
      .with_integer_flag(MAX_RESPONSE_ITEMS_FEATURE_FLAG, 6)
      .with_integer_flag(MAX_RESPONSE_BYTES_FEATURE_FLAG, 7)
      .with_integer_flag(MAX_ENTRY_ITEMS_FEATURE_FLAG, 8),
  ));
  let feature_flags = feature_flags.snapshot_watch();

  let config = MetadataCacheConfig::from_runtime_config(&runtime, Some(&feature_flags)).unwrap();

  assert_eq!(config.recovery_max_bytes, 1_024);
  assert_eq!(config.limits.max_refills, DEFAULT_MAX_REFILLS);
  let cache = MetadataCache::new(
    Arc::new(CountingMetadataStore {
      scans: AtomicUsize::new(0),
      observed_bounds: Mutex::new(Vec::new()),
      segments: Vec::new(),
    }),
    config,
    Some(feature_flags),
  );
  let limits = cache.limits();
  assert_eq!(limits.max_waiters_per_key, 2);
  assert_eq!(limits.max_waiters, 3);
  assert_eq!(limits.max_refills, 4);
  assert_eq!(limits.max_request_partitions, 5);
  assert_eq!(limits.max_response_items, 6);
  assert_eq!(limits.max_response_bytes, 7);
  assert_eq!(limits.max_entry_items, 8);
}

#[test]
fn invalid_startup_limit_flags_are_rejected() {
  let mut runtime = RuntimeConfig::new();
  runtime.broker = Some(BrokerConfig::new()).into();
  let loader = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(MAX_WAITERS_PER_KEY_FEATURE_FLAG, 5_000),
  ));
  let error = MetadataCacheConfig::from_runtime_config(&runtime, Some(&loader.snapshot_watch()))
    .err()
    .unwrap();
  assert!(
    error
      .to_string()
      .contains("per-key waiters exceed global cap")
  );
}

#[test]
fn removing_startup_limit_flags_restores_static_defaults() {
  let mut runtime = RuntimeConfig::new();
  runtime.broker = Some(BrokerConfig::new()).into();
  let loader = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag(MAX_REFILLS_FEATURE_FLAG, 2)
      .with_integer_flag(MAX_RESPONSE_BYTES_FEATURE_FLAG, 1_024),
  ));
  let config =
    MetadataCacheConfig::from_runtime_config(&runtime, Some(&loader.snapshot_watch())).unwrap();
  let cache = MetadataCache::new(
    Arc::new(CountingMetadataStore {
      scans: AtomicUsize::new(0),
      observed_bounds: Mutex::new(Vec::new()),
      segments: Vec::new(),
    }),
    config,
    Some(loader.snapshot_watch()),
  );
  assert_eq!(cache.limits().max_refills, 2);
  assert_eq!(cache.limits().max_response_bytes, 1_024);

  loader.update(Arc::new(DefaultFeatureFlags::default()));
  assert_eq!(cache.limits().max_refills, DEFAULT_MAX_REFILLS);
  assert_eq!(
    cache.limits().max_response_bytes,
    DEFAULT_MAX_RESPONSE_BYTES
  );
}

#[test]
fn live_refill_limit_downshifts_and_upshifts_without_losing_inflight_work() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
  });
  let loader = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(MAX_REFILLS_FEATURE_FLAG, 2),
  ));
  let cache = MetadataCache::new(
    store,
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    Some(loader.snapshot_watch()),
  );
  let first = cache.try_admit_refill(cache.limits().max_refills).unwrap();

  loader.update(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(MAX_REFILLS_FEATURE_FLAG, 1),
  ));
  assert!(cache.try_admit_refill(cache.limits().max_refills).is_err());

  loader.update(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(MAX_REFILLS_FEATURE_FLAG, 3),
  ));
  let second = cache.try_admit_refill(cache.limits().max_refills).unwrap();
  assert_eq!(cache.active_refills.load(Ordering::Relaxed), 2);
  drop(first);
  drop(second);
  assert_eq!(cache.active_refills.load(Ordering::Relaxed), 0);

  loader.update(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag(MAX_RESPONSE_BYTES_FEATURE_FLAG, 32 * 1024 * 1024),
  ));
  assert_eq!(cache.limits().max_refills, 3);
}

#[test]
fn live_metadata_request_timeout_keeps_last_valid_value() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
  });
  let loader = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(REQUEST_TIMEOUT_FEATURE_FLAG, 800),
  ));
  let cache = MetadataCache::new(
    store,
    cache_config(Duration::seconds(1), StdDuration::from_millis(250)),
    Some(loader.snapshot_watch()),
  );
  assert_eq!(
    cache.limits().request_timeout,
    StdDuration::from_millis(800)
  );

  loader.update(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(REQUEST_TIMEOUT_FEATURE_FLAG, 400),
  ));
  assert_eq!(
    cache.limits().request_timeout,
    StdDuration::from_millis(800)
  );

  loader.update(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag(REQUEST_TIMEOUT_FEATURE_FLAG, 1200),
  ));
  assert_eq!(
    cache.limits().request_timeout,
    StdDuration::from_millis(1200)
  );
}

#[test]
fn metadata_request_timeout_keeps_static_submillisecond_precision() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::from_millis(250));
  config.limits.request_timeout = StdDuration::from_micros(800_500);
  let loader = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let cache = MetadataCache::new(store, config, Some(loader.snapshot_watch()));
  assert_eq!(
    cache.limits().request_timeout,
    StdDuration::from_micros(800_500)
  );
}

#[test]
fn metadata_cache_limits_parse_only_after_flag_updates() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
  });
  let reads = Arc::new(AtomicUsize::new(0));
  let flags = |max_refills| {
    Arc::new(CountingFeatureFlags {
      flags: DefaultFeatureFlags::default()
        .with_integer_flag(MAX_REFILLS_FEATURE_FLAG, max_refills),
      reads: Arc::clone(&reads),
    })
  };
  let loader = FakeLoader::new(flags(2));
  let cache = MetadataCache::new(
    store,
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    Some(loader.snapshot_watch()),
  );
  assert_eq!(cache.limits().max_refills, 2);
  let initial_reads = reads.load(Ordering::Relaxed);
  assert!(initial_reads > 0);
  assert_eq!(cache.limits().max_refills, 2);
  assert_eq!(reads.load(Ordering::Relaxed), initial_reads);

  loader.update(flags(3));
  assert_eq!(cache.limits().max_refills, 3);
  let updated_reads = reads.load(Ordering::Relaxed);
  assert_eq!(updated_reads, initial_reads * 2);
  assert_eq!(cache.limits().max_refills, 3);
  assert_eq!(reads.load(Ordering::Relaxed), updated_reads);
}

fn segment() -> SegmentMetadata {
  SegmentMetadata::new(
    TopicWindowKey {
      topic: "topic".to_string(),
      window_start_unix_seconds: 0,
    },
    SnowflakeId(100),
    BlobKey::from("topic/segment"),
    Compression::none(),
    HashMap::from([(
      0,
      BatchMetadata {
        seq_range: SeqRange { start: 0, end: 1 },
        byte_range: ByteRange { start: 0, end: 1 },
        payload_bytes: 1,
      },
    )]),
    OffsetDateTime::UNIX_EPOCH,
    OffsetDateTime::UNIX_EPOCH,
  )
}

fn tail_request(
  min_snowflake: u64,
  consistency: MetadataReadConsistency,
) -> ReadMetadataWindowRequest {
  tail_request_with_bounds(vec![(0, min_snowflake)], consistency)
}

fn tail_request_with_bounds(
  bounds: Vec<(u32, u64)>,
  consistency: MetadataReadConsistency,
) -> ReadMetadataWindowRequest {
  ReadMetadataWindowRequest {
    topic: "topic".into(),
    window_start_unix_seconds: 0,
    consistency: consistency.into(),
    coverage: Some(read_metadata_window_request::Coverage::Tail(
      TailMetadataCoverage {
        partition_bounds: bounds
          .into_iter()
          .map(
            |(virtual_partition_id, min_snowflake)| MetadataPartitionBound {
              virtual_partition_id,
              min_snowflake,
              ..Default::default()
            },
          )
          .collect(),
        ..Default::default()
      },
    )),
    max_response_bytes: 1024 * 1024,
    ..Default::default()
  }
}

fn full_recovery_request() -> ReadMetadataWindowRequest {
  ReadMetadataWindowRequest {
    topic: "topic".into(),
    window_start_unix_seconds: 0,
    consistency: MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL.into(),
    coverage: Some(read_metadata_window_request::Coverage::FullRecovery(
      FullRecoveryMetadataCoverage {
        virtual_partition_ids: vec![0],
        ..Default::default()
      },
    )),
    max_response_bytes: 1024 * 1024,
    ..Default::default()
  }
}

#[tokio::test]
async fn returns_failure_branch_for_invalid_request() {
  let cache = Arc::new(MetadataCache::new(
    Arc::new(CountingMetadataStore {
      scans: AtomicUsize::new(0),
      observed_bounds: Mutex::new(Vec::new()),
      segments: Vec::new(),
    }),
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    None,
  ));

  let response = cache.read(ReadMetadataWindowRequest::new()).await;
  let Some(read_metadata_window_response::Result::Failure(failure)) = response.result else {
    panic!("invalid metadata request must return the failure result branch");
  };
  assert_eq!(
    failure.status,
    MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_BAD_REQUEST.into()
  );
  assert!(failure.error_message.contains("topic is empty"));
}

#[tokio::test]
async fn coalesces_tail_requests_at_the_lowest_bound() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let cache = Arc::new(MetadataCache::new(
    store.clone(),
    cache_config(Duration::seconds(1), StdDuration::from_millis(20)),
    None,
  ));

  let (lower, higher) = tokio::join!(
    cache.read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    )),
    cache.read(tail_request(
      200,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    )),
  );

  assert!(matches!(
    lower.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  assert!(matches!(
    higher.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  assert_eq!(store.scans.load(Ordering::Relaxed), 1);
  assert_eq!(
    store.observed_bounds.lock().as_slice(),
    [Some(SnowflakeId(100))]
  );
}

#[tokio::test]
async fn records_coalescing_metrics_for_strong_queries() {
  let collector = Collector::default();
  let metrics = Helper::new_with_collector(collector.clone());
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let cache = MetadataCache::new_with_metrics(
    store.clone(),
    cache_config(Duration::seconds(1), StdDuration::from_millis(20)),
    None,
    &shutdown_trigger.make_handle(),
    &collector.scope("blob_stream_broker_test"),
  );

  let (lower, higher) = tokio::join!(
    cache.read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG,
    )),
    cache.read(tail_request(
      200,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG,
    )),
  );

  assert!(matches!(
    lower.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  assert!(matches!(
    higher.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  assert_eq!(store.scans.load(Ordering::Relaxed), 1);
  metrics.assert_counter_eq(
    1,
    "blob_stream_broker_test:metadata_cache:storage_queries_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    2,
    "blob_stream_broker_test:metadata_cache:coalescing_window_requests_total",
    &labels!(),
  );
  shutdown_trigger.shutdown().await;
}

#[tokio::test]
async fn narrower_strong_request_after_refill_start_runs_a_follow_up_refill() {
  let store = Arc::new(GatedMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
    scan_started: Semaphore::new(0),
    release_scan: Semaphore::new(0),
  });
  let cache = Arc::new(MetadataCache::new(
    store.clone(),
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    None,
  ));

  let first_cache = cache.clone();
  let first = tokio::spawn(async move {
    first_cache
      .read(tail_request(
        200,
        MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG,
      ))
      .await
  });
  let _first_scan = timeout(StdDuration::from_secs(1), store.scan_started.acquire())
    .await
    .unwrap()
    .unwrap();

  let second_cache = cache.clone();
  let second = tokio::spawn(async move {
    second_cache
      .read(tail_request(
        100,
        MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG,
      ))
      .await
  });
  timeout(StdDuration::from_secs(1), async {
    while cache.snapshot().await.active_waiters < 2 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();

  store.release_scan.add_permits(1);
  let _second_scan = timeout(StdDuration::from_secs(1), store.scan_started.acquire())
    .await
    .unwrap()
    .unwrap();
  store.release_scan.add_permits(1);

  assert!(matches!(
    first.await.unwrap().result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  assert!(matches!(
    second.await.unwrap().result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
  assert_eq!(
    store.observed_bounds.lock().as_slice(),
    [Some(SnowflakeId(200)), Some(SnowflakeId(100))]
  );
}

#[tokio::test]
async fn late_strong_waiter_receives_its_own_seal_horizon() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let store = Arc::new(GatedMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
    scan_started: Semaphore::new(0),
    release_scan: Semaphore::new(0),
  });
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(Arc::new(ManualTimeProvider::new(timestamp(100)))),
  );
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(85)).as_u64());
  let first_cache = cache.clone();
  let first = tokio::spawn(async move { first_cache.read(request.clone()).await });
  let _first_scan = timeout(StdDuration::from_secs(1), store.scan_started.acquire())
    .await
    .unwrap()
    .unwrap();

  let mut late_request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  late_request.window_start_unix_seconds = window_start;
  late_request.requested_seal_before =
    Some(SnowflakeId::minimum_for_timestamp(timestamp(65)).as_u64());
  late_request.requested_seal_horizon_ms = Some(45_000);
  let second_cache = cache.clone();
  let second = tokio::spawn(async move { second_cache.read(late_request).await });
  timeout(StdDuration::from_secs(1), async {
    while cache.snapshot().await.active_waiters < 2 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  store.release_scan.add_permits(1);
  let Some(read_metadata_window_response::Result::Success(first)) = first.await.unwrap().result
  else {
    panic!("initial strong read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(second)) = second.await.unwrap().result
  else {
    panic!("coalesced strong read must succeed");
  };
  assert_eq!(store.scans.load(Ordering::Relaxed), 1);
  assert!(first.sealed_before.unwrap() > second.sealed_before.unwrap());
  assert_eq!(
    second.sealed_before,
    Some(SnowflakeId::minimum_for_timestamp(timestamp(55)).as_u64())
  );
  assert_eq!(second.sealed_at_unix_ms, first.sealed_at_unix_ms);
}

#[tokio::test]
async fn late_seal_waiter_uses_pre_scan_time_for_unsealed_strong_refill() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let store = Arc::new(GatedMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
    scan_started: Semaphore::new(0),
    release_scan: Semaphore::new(0),
  });
  let clock = Arc::new(ManualTimeProvider::new(timestamp(100)));
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(clock.clone()),
  );
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  let first_cache = cache.clone();
  let first = tokio::spawn(async move { first_cache.read(request).await });
  let _scan = timeout(StdDuration::from_secs(1), store.scan_started.acquire())
    .await
    .unwrap()
    .unwrap();

  let mut late_request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  late_request.window_start_unix_seconds = window_start;
  late_request.requested_seal_before =
    Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  let second_cache = cache.clone();
  let second = tokio::spawn(async move { second_cache.read(late_request).await });
  timeout(StdDuration::from_secs(1), async {
    while cache.snapshot().await.active_waiters < 2 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  clock.advance(Duration::seconds(30));
  store.release_scan.add_permits(1);

  let Some(read_metadata_window_response::Result::Success(first)) = first.await.unwrap().result
  else {
    panic!("unsealed strong read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(second)) = second.await.unwrap().result
  else {
    panic!("coalesced sealing read must succeed");
  };
  let expected_seal = SnowflakeId::minimum_for_timestamp(
    timestamp(100).saturating_sub(Duration::seconds(15) + DEFAULT_SEAL_MAX_CLOCK_SKEW),
  );
  assert_eq!(store.scans.load(Ordering::Relaxed), 1);
  assert_eq!(first.sealed_before, None);
  assert_eq!(first.sealed_at_unix_ms, None);
  assert_eq!(second.sealed_before, Some(expected_seal.as_u64()));
  assert_eq!(
    second.sealed_at_unix_ms,
    Some(timestamp(100).unix_timestamp() * 1_000)
  );
  assert_eq!(
    second.observed_at_unix_ms,
    timestamp(130).unix_timestamp() * 1_000
  );
}

#[tokio::test]
async fn rejected_waiter_does_not_start_an_unowned_refill() {
  let store = Arc::new(GatedMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
    scan_started: Semaphore::new(0),
    release_scan: Semaphore::new(0),
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.limits.max_waiters = 1;
  let cache = Arc::new(MetadataCache::new(store.clone(), config, None));

  let first_cache = cache.clone();
  let first = tokio::spawn(async move {
    first_cache
      .read(tail_request(
        100,
        MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
      ))
      .await
  });
  let _first_scan = timeout(StdDuration::from_secs(1), store.scan_started.acquire())
    .await
    .unwrap()
    .unwrap();

  let mut rejected_request = tail_request(
    100,
    MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
  );
  rejected_request.window_start_unix_seconds = 300;
  let rejected = cache.read(rejected_request).await;
  let Some(read_metadata_window_response::Result::Failure(failure)) = rejected.result else {
    panic!("overloaded waiter must receive a failure response");
  };
  assert_eq!(
    failure.status,
    MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED.into()
  );
  assert_eq!(store.scans.load(Ordering::Relaxed), 1);

  store.release_scan.add_permits(1);
  assert!(matches!(
    first.await.unwrap().result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
}

#[tokio::test]
async fn timed_out_strong_suffix_releases_waiter_and_refill_admission() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let mut segment = segment();
  segment.window.window_start_unix_seconds = window_start;
  segment.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(30));
  let store = Arc::new(GatedMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment],
    scan_started: Semaphore::new(0),
    release_scan: Semaphore::new(1),
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.limits.request_timeout = StdDuration::from_millis(500);
  let cache = Arc::new(
    MetadataCache::new(store.clone(), config.clone(), None)
      .time_provider(Arc::new(ManualTimeProvider::new(timestamp(100)))),
  );
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(85)).as_u64());
  assert!(matches!(
    cache.read(request.clone()).await.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  let _initial_scan = store.scan_started.acquire().await.unwrap();
  let mut readers = Vec::new();
  for _ in 0 .. 3 {
    let reader_cache = cache.clone();
    let reader_request = request.clone();
    readers.push(tokio::spawn(async move {
      reader_cache.read(reader_request).await
    }));
  }
  let _suffix_scan = timeout(StdDuration::from_secs(1), store.scan_started.acquire())
    .await
    .unwrap()
    .unwrap();
  timeout(StdDuration::from_millis(200), async {
    while cache.snapshot().await.active_waiters < 3 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  for reader in readers {
    let Some(read_metadata_window_response::Result::Failure(failure)) =
      reader.await.unwrap().result
    else {
      panic!("timed out suffix must fail for every waiter");
    };
    assert_eq!(
      failure.overload_reason.enum_value().unwrap(),
      MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_REQUEST_TIMEOUT
    );
  }
  timeout(StdDuration::from_secs(1), async {
    while cache.snapshot().await.in_flight_refills != 0 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  let snapshot = cache.snapshot().await;
  assert_eq!(snapshot.active_waiters, 0);
  assert_eq!(snapshot.available_refill_permits, config.limits.max_refills);
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn tail_response_filters_each_partition_at_its_requested_bound() {
  let segments = vec![SegmentMetadata::new(
    TopicWindowKey {
      topic: "topic".to_string(),
      window_start_unix_seconds: 0,
    },
    SnowflakeId(100),
    BlobKey::from("topic/segment"),
    Compression::none(),
    HashMap::from([
      (
        0,
        BatchMetadata {
          seq_range: SeqRange { start: 0, end: 1 },
          byte_range: ByteRange { start: 0, end: 1 },
          payload_bytes: 1,
        },
      ),
      (
        1,
        BatchMetadata {
          seq_range: SeqRange { start: 0, end: 1 },
          byte_range: ByteRange { start: 1, end: 2 },
          payload_bytes: 1,
        },
      ),
    ]),
    OffsetDateTime::UNIX_EPOCH,
    OffsetDateTime::UNIX_EPOCH,
  )];
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments,
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.topics.get_mut("topic").unwrap().partition_count = 2;
  let cache = Arc::new(MetadataCache::new(store, config, None));

  let response = cache
    .read(tail_request_with_bounds(
      vec![(0, 100), (1, 200)],
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;
  let Some(read_metadata_window_response::Result::Success(success)) = response.result else {
    panic!("tail metadata read must succeed");
  };
  assert_eq!(success.segments.len(), 1);
  assert_eq!(
    success.segments[0]
      .metadata
      .as_ref()
      .expect("broker segment metadata is present")
      .partitions
      .iter()
      .map(|partition| partition.virtual_partition_id)
      .collect::<Vec<_>>(),
    vec![0]
  );
}

#[tokio::test]
async fn does_not_retain_completed_strong_reads() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let cache = Arc::new(MetadataCache::new(
    store.clone(),
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    None,
  ));

  let first = cache
    .read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG,
    ))
    .await;
  let second = cache
    .read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG,
    ))
    .await;

  assert!(matches!(
    first.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  assert!(matches!(
    second.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn retains_strong_reads_of_sealed_windows() {
  let window_start = 1_735_689_600;
  let mut metadata = segment();
  metadata.window.window_start_unix_seconds = window_start;
  metadata.snowflake_id = SnowflakeId::minimum_for_timestamp(
    OffsetDateTime::from_unix_timestamp(window_start + 100).unwrap(),
  );
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![metadata],
  });
  let cache = Arc::new(MetadataCache::new(
    store.clone(),
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    None,
  ));
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  let window_end = SnowflakeId::minimum_for_timestamp(
    OffsetDateTime::from_unix_timestamp(window_start + 300).unwrap(),
  );
  request.requested_seal_before = Some(window_end.as_u64());

  let first = cache.read(request.clone()).await;
  let second = cache.read(request).await;
  let Some(read_metadata_window_response::Result::Success(first)) = first.result else {
    panic!("initial strong read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(second)) = second.result else {
    panic!("retained strong read must succeed");
  };
  assert_eq!(store.scans.load(Ordering::Relaxed), 1);
  assert!(!first.retained_strong_coverage);
  assert!(second.retained_strong_coverage);
  assert!(!second.retained_coverage);
  assert_eq!(second.sealed_before, Some(window_end.as_u64()));
  assert_eq!(first.sealed_at_unix_ms, second.sealed_at_unix_ms);
  assert_eq!(second.segments.len(), 1);
}

#[tokio::test]
async fn strong_seals_stop_at_window_end_on_refill_and_reuse() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let window_end = SnowflakeId::minimum_for_timestamp(timestamp(300));
  for full_recovery in [false, true] {
    let store = Arc::new(CountingMetadataStore {
      scans: AtomicUsize::new(0),
      observed_bounds: Mutex::new(Vec::new()),
      segments: Vec::new(),
    });
    let cache = Arc::new(
      MetadataCache::new(
        store.clone(),
        cache_config(Duration::seconds(1), StdDuration::ZERO),
        None,
      )
      .time_provider(Arc::new(ManualTimeProvider::new(timestamp(500)))),
    );
    let mut request = if full_recovery {
      full_recovery_request()
    } else {
      tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG)
    };
    request.window_start_unix_seconds = window_start;
    request.consistency = MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG.into();
    request.requested_seal_before =
      Some(SnowflakeId::minimum_for_timestamp(timestamp(303)).as_u64());
    for retained in [false, true] {
      let Some(read_metadata_window_response::Result::Success(success)) =
        cache.read(request.clone()).await.result
      else {
        panic!("strong metadata read must succeed");
      };
      assert_eq!(success.sealed_before, Some(window_end.as_u64()));
      assert_eq!(success.retained_strong_coverage, retained);
      assert!(!success.retained_coverage);
    }
    assert_eq!(store.scans.load(Ordering::Relaxed), 1);
    let specification = cache.validate_request(&request).unwrap();
    assert_eq!(specification.requested_seal_before, Some(window_end));
    assert_eq!(
      cache
        .strong_sealed_entries
        .get(&specification.key)
        .await
        .unwrap()
        .sealed_before,
      Some(window_end)
    );
  }
}

#[tokio::test]
async fn retained_oversized_seal_is_bounded_by_the_current_window_request() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
  });
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(Arc::new(ManualTimeProvider::new(timestamp(500)))),
  );
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(303)).as_u64());
  let specification = cache.validate_request(&request).unwrap();
  cache
    .strong_sealed_entries
    .insert(
      specification.key,
      Arc::new(CacheEntry {
        refill_floor: Some(SnowflakeId(0)),
        observed_at: timestamp(500),
        sealed_before: Some(SnowflakeId::minimum_for_timestamp(timestamp(400))),
        sealed_at: Some(timestamp(500)),
        sealed_horizon: None,
        retained_bytes: 0,
        generation: 1,
        segments: Vector::new(),
      }),
    )
    .await;
  let Some(read_metadata_window_response::Result::Success(success)) =
    cache.read(request).await.result
  else {
    panic!("retained metadata read must succeed");
  };
  assert_eq!(
    success.sealed_before,
    Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64())
  );
  assert!(success.retained_strong_coverage);
  assert_eq!(store.scans.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn slow_strong_scan_seals_only_the_pre_scan_safe_prefix() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let store = Arc::new(GatedMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
    scan_started: Semaphore::new(0),
    release_scan: Semaphore::new(0),
  });
  let clock = Arc::new(ManualTimeProvider::new(timestamp(100)));
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(clock.clone()),
  );
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  let first_cache = cache.clone();
  let first_request = request.clone();
  let first = tokio::spawn(async move { first_cache.read(first_request).await });
  let _scan = timeout(StdDuration::from_secs(1), store.scan_started.acquire())
    .await
    .unwrap()
    .unwrap();
  clock.advance(Duration::seconds(30));
  store.release_scan.add_permits(1);
  let Some(read_metadata_window_response::Result::Success(first)) = first.await.unwrap().result
  else {
    panic!("first strong scan must succeed");
  };
  let expected_seal = SnowflakeId::minimum_for_timestamp(
    timestamp(100).saturating_sub(Duration::seconds(15) + DEFAULT_SEAL_MAX_CLOCK_SKEW),
  );
  assert_eq!(first.sealed_before, Some(expected_seal.as_u64()));
  assert_eq!(
    first.sealed_at_unix_ms,
    Some(timestamp(100).unix_timestamp() * 1_000)
  );
  assert_eq!(
    first.observed_at_unix_ms,
    timestamp(130).unix_timestamp() * 1_000
  );

  store.release_scan.add_permits(1);
  let Some(read_metadata_window_response::Result::Success(second)) =
    cache.read(request).await.result
  else {
    panic!("open suffix scan must succeed");
  };
  assert_eq!(store.observed_bounds.lock()[1], Some(expected_seal));
  assert!(second.sealed_before > first.sealed_before);
  assert_eq!(
    second.sealed_at_unix_ms,
    Some(timestamp(130).unix_timestamp() * 1_000)
  );
}

#[tokio::test]
async fn strong_full_recovery_queries_open_suffix_but_reuses_mature_window() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let mut prefix = segment();
  prefix.window.window_start_unix_seconds = window_start;
  prefix.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(30));
  let mut suffix = prefix.clone();
  suffix.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(90));
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![prefix, suffix],
  });
  let mut request = full_recovery_request();
  request.window_start_unix_seconds = window_start;
  request.consistency = MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG.into();
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  let open_cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(Arc::new(ManualTimeProvider::new(timestamp(100)))),
  );
  let first = open_cache.read(request.clone()).await;
  let second = open_cache.read(request.clone()).await;
  let Some(read_metadata_window_response::Result::Success(first)) = first.result else {
    panic!("initial strong recovery read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(second)) = second.result else {
    panic!("open strong recovery read must succeed");
  };
  assert!(!first.retained_strong_coverage);
  assert!(second.retained_strong_coverage);
  assert_eq!(second.segments.len(), 2);
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
  assert_eq!(
    store.observed_bounds.lock()[1],
    second.sealed_before.map(SnowflakeId)
  );

  let mature_cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(Arc::new(ManualTimeProvider::new(timestamp(500)))),
  );
  let mature_first = mature_cache.read(request.clone()).await;
  let mature_second = mature_cache.read(request.clone()).await;
  let Some(read_metadata_window_response::Result::Success(mature_first)) = mature_first.result
  else {
    panic!("first mature recovery read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(mature_second)) = mature_second.result
  else {
    panic!("retained mature recovery read must succeed");
  };
  assert!(!mature_first.retained_strong_coverage);
  assert!(mature_second.retained_strong_coverage);
  assert_eq!(mature_second.sealed_before, request.requested_seal_before);
  assert_eq!(store.scans.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn tail_floor_at_the_seal_boundary_does_not_reuse_a_prefix() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
  });
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(Arc::new(ManualTimeProvider::new(timestamp(100)))),
  );
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(85)).as_u64());
  let Some(read_metadata_window_response::Result::Success(initial)) =
    cache.read(request.clone()).await.result
  else {
    panic!("initial read must succeed");
  };
  let seal = initial.sealed_before.unwrap();
  request = tail_request(
    seal,
    MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG,
  );
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(85)).as_u64());
  for _ in 0 .. 2 {
    let Some(read_metadata_window_response::Result::Success(response)) =
      cache.read(request.clone()).await.result
    else {
      panic!("boundary read must succeed");
    };
    assert!(!response.retained_strong_coverage);
    assert_eq!(response.sealed_before, None);
  }
  assert_eq!(store.scans.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn refills_open_tail_after_reusing_strong_sealed_prefix() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let mut prefix = segment();
  prefix.window.window_start_unix_seconds = window_start;
  prefix.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(30));
  let mut suffix = prefix.clone();
  suffix.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(90));
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![prefix.clone(), suffix.clone()],
  });
  let observed_at = timestamp(100);
  let clock = Arc::new(ManualTimeProvider::new(observed_at));
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(clock),
  );
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(85)).as_u64());

  let first = cache.read(request.clone()).await;
  let second = cache.read(request).await;
  let Some(read_metadata_window_response::Result::Success(first)) = first.result else {
    panic!("first open Tail read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(second)) = second.result else {
    panic!("refilled open Tail read must succeed");
  };
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
  assert_eq!(
    store.observed_bounds.lock()[1],
    first.sealed_before.map(SnowflakeId)
  );
  assert_eq!(first.segments.len(), 2);
  assert_eq!(second.segments.len(), 2);
  assert!(second.retained_strong_coverage);
  assert_eq!(second.sealed_at_unix_ms, first.sealed_at_unix_ms);
  assert!(prefix.snowflake_id.as_u64() < second.sealed_before.unwrap());
  assert!(suffix.snowflake_id.as_u64() >= second.sealed_before.unwrap());
}

#[tokio::test]
async fn empty_strong_suffix_still_advances_the_seal() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let mut segment = segment();
  segment.window.window_start_unix_seconds = window_start;
  segment.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(30));
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment],
  });
  let clock = Arc::new(ManualTimeProvider::new(timestamp(100)));
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(clock.clone()),
  );
  let mut request = full_recovery_request();
  request.window_start_unix_seconds = window_start;
  request.consistency = MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG.into();
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  let Some(read_metadata_window_response::Result::Success(first)) =
    cache.read(request.clone()).await.result
  else {
    panic!("initial strong read must succeed");
  };
  clock.advance(Duration::seconds(80));
  let Some(read_metadata_window_response::Result::Success(second)) =
    cache.read(request.clone()).await.result
  else {
    panic!("empty suffix read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(third)) =
    cache.read(request).await.result
  else {
    panic!("promoted empty suffix read must succeed");
  };
  assert!(second.sealed_before > first.sealed_before);
  assert_eq!(second.sealed_before, third.sealed_before);
  assert_eq!(third.segments.len(), 1);
  assert_eq!(
    store.observed_bounds.lock().as_slice(),
    &[
      None,
      first.sealed_before.map(SnowflakeId),
      second.sealed_before.map(SnowflakeId),
    ]
  );
}

#[tokio::test]
async fn strong_suffix_promotions_share_retained_rows() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: [30, 90, 130]
      .map(|offset| {
        let mut metadata = segment();
        metadata.window.window_start_unix_seconds = window_start;
        metadata.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(offset));
        metadata
      })
      .to_vec(),
  });
  let clock = Arc::new(ManualTimeProvider::new(timestamp(100)));
  let cache = Arc::new(
    MetadataCache::new(
      store,
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(clock.clone()),
  );
  let mut request = full_recovery_request();
  request.window_start_unix_seconds = window_start;
  request.consistency = MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG.into();
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  let specification = cache.validate_request(&request).unwrap();
  let loaded = cache.load_entry(specification.clone()).await.unwrap();
  let mut previous = cache
    .strong_sealed_entries
    .get(&specification.key)
    .await
    .unwrap();
  assert_eq!(previous.segments.len(), 1);
  assert!(Arc::ptr_eq(&loaded.segments[0], &previous.segments[0]));
  for (advance, expected_len) in [(20, 2), (60, 3), (40, 3)] {
    clock.advance(Duration::seconds(advance));
    let scanned = cache
      .refill_strong_suffix(
        &specification,
        previous.clone(),
        previous.sealed_before.unwrap(),
      )
      .await
      .unwrap();
    let promoted = cache
      .strong_sealed_entries
      .get(&specification.key)
      .await
      .unwrap();
    assert!(promoted.sealed_before > previous.sealed_before);
    assert!(promoted.generation > previous.generation);
    assert_eq!(promoted.generation, scanned.generation);
    assert_eq!(promoted.segments.len(), expected_len);
    for (old, retained) in previous.segments.iter().zip(&promoted.segments) {
      assert!(Arc::ptr_eq(old, retained));
    }
    for (retained, returned) in promoted.segments.iter().zip(&scanned.segments) {
      assert!(Arc::ptr_eq(retained, returned));
    }
    assert_eq!(
      promoted.retained_bytes,
      estimate_retained_bytes(promoted.segments.iter().map(Arc::as_ref))
    );
    previous = promoted;
  }
}

#[tokio::test]
async fn stricter_horizon_rescans_a_promoted_prefix() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let segments = [30, 90, 130]
    .map(|offset| {
      let mut metadata = segment();
      metadata.window.window_start_unix_seconds = window_start;
      metadata.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(offset));
      metadata
    })
    .to_vec();
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: segments.clone(),
  });
  let clock = Arc::new(ManualTimeProvider::new(timestamp(100)));
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(clock.clone()),
  );
  let mut request = full_recovery_request();
  request.window_start_unix_seconds = window_start;
  request.consistency = MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG.into();
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  let Some(read_metadata_window_response::Result::Success(first)) =
    cache.read(request.clone()).await.result
  else {
    panic!("initial strong read must succeed");
  };
  clock.advance(Duration::seconds(80));
  let Some(read_metadata_window_response::Result::Success(second)) =
    cache.read(request.clone()).await.result
  else {
    panic!("promoted strong read must succeed");
  };
  assert!(second.sealed_before > first.sealed_before);

  request.requested_seal_horizon_ms = Some(120_000);
  let Some(read_metadata_window_response::Result::Success(stricter)) =
    cache.read(request).await.result
  else {
    panic!("stricter strong read must succeed");
  };
  assert!(!stricter.retained_strong_coverage);
  assert_eq!(
    stricter.sealed_at_unix_ms,
    Some(timestamp(180).unix_timestamp() * 1_000)
  );
  assert_eq!(
    stricter
      .segments
      .iter()
      .map(|segment| segment.snowflake_id)
      .collect::<Vec<_>>(),
    segments
      .iter()
      .map(|segment| segment.snowflake_id.as_u64())
      .collect::<Vec<_>>()
  );
  assert_eq!(
    store.observed_bounds.lock().as_slice(),
    &[None, first.sealed_before.map(SnowflakeId), None]
  );
}

#[tokio::test]
async fn stale_strong_refills_preserve_a_newer_prefix() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  for suffix_refill in [false, true] {
    let store = Arc::new(GatedMetadataStore {
      scans: AtomicUsize::new(0),
      observed_bounds: Mutex::new(Vec::new()),
      segments: Vec::new(),
      scan_started: Semaphore::new(0),
      release_scan: Semaphore::new(1),
    });
    let clock = Arc::new(ManualTimeProvider::new(timestamp(100)));
    let cache = Arc::new(
      MetadataCache::new(
        store.clone(),
        cache_config(Duration::seconds(1), StdDuration::ZERO),
        None,
      )
      .time_provider(clock.clone()),
    );
    let mut request = full_recovery_request();
    request.window_start_unix_seconds = window_start;
    request.consistency = MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG.into();
    request.requested_seal_before =
      Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
    let specification = cache.validate_request(&request).unwrap();
    cache.load_entry(specification.clone()).await.unwrap();
    store.scan_started.acquire().await.unwrap().forget();
    let prefix = cache
      .strong_sealed_entries
      .get(&specification.key)
      .await
      .unwrap();
    clock.advance(Duration::seconds(80));
    let refill_cache = cache.clone();
    let refill_specification = specification.clone();
    let refill_prefix = prefix.clone();
    let refill = tokio::spawn(async move {
      if suffix_refill {
        let bound = refill_prefix.sealed_before.unwrap();
        refill_cache
          .refill_strong_suffix(&refill_specification, refill_prefix, bound)
          .await
      } else {
        refill_cache.load_entry(refill_specification).await
      }
    });
    timeout(StdDuration::from_secs(1), store.scan_started.acquire())
      .await
      .unwrap()
      .unwrap()
      .forget();
    clock.advance(Duration::seconds(80));
    let newer = Arc::new(CacheEntry {
      refill_floor: prefix.refill_floor,
      observed_at: clock.now(),
      sealed_before: Some(SnowflakeId::minimum_for_timestamp(timestamp(230))),
      sealed_at: Some(clock.now()),
      sealed_horizon: Some(specification.seal_horizon(cache.config.seal_clock_skew)),
      retained_bytes: prefix.retained_bytes,
      generation: prefix.generation + 1,
      segments: prefix.segments.clone(),
    });
    cache
      .strong_sealed_entries
      .insert(specification.key.clone(), newer.clone())
      .await;
    store.release_scan.add_permits(1);
    let scanned = timeout(StdDuration::from_secs(1), refill)
      .await
      .unwrap()
      .unwrap()
      .unwrap();
    assert!(scanned.sealed_before < newer.sealed_before);
    let retained = cache
      .strong_sealed_entries
      .get(&specification.key)
      .await
      .unwrap();
    assert!(Arc::ptr_eq(&retained, &newer));
    assert_eq!(store.scans.load(Ordering::Relaxed), 2);
  }
}

#[tokio::test]
async fn coalesced_suffix_caps_each_caller_after_promotion() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let segments = [5, 90, 170]
    .map(|offset| {
      let mut metadata = segment();
      metadata.window.window_start_unix_seconds = window_start;
      metadata.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(offset));
      metadata
    })
    .to_vec();
  let store = Arc::new(GatedMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: segments.clone(),
    scan_started: Semaphore::new(0),
    release_scan: Semaphore::new(1),
  });
  let clock = Arc::new(ManualTimeProvider::new(timestamp(100)));
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(clock.clone()),
  );
  let mut request = full_recovery_request();
  request.window_start_unix_seconds = window_start;
  request.consistency = MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG.into();
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  request.requested_seal_horizon_ms = Some(70_000);
  let Some(read_metadata_window_response::Result::Success(initial)) =
    cache.read(request.clone()).await.result
  else {
    panic!("initial strong read must succeed");
  };
  let _initial_scan = store.scan_started.acquire().await.unwrap();
  clock.advance(Duration::seconds(80));

  let mut shorter_horizon = request.clone();
  shorter_horizon.requested_seal_horizon_ms = Some(20_000);
  let first_cache = cache.clone();
  let first = tokio::spawn(async move { first_cache.read(shorter_horizon).await });
  let _suffix_scan = timeout(StdDuration::from_secs(1), store.scan_started.acquire())
    .await
    .unwrap()
    .unwrap();
  let second_cache = cache.clone();
  let second = tokio::spawn(async move { second_cache.read(request).await });
  timeout(StdDuration::from_secs(1), async {
    while cache.snapshot().await.active_waiters < 2 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  store.release_scan.add_permits(1);
  let Some(read_metadata_window_response::Result::Success(first)) = first.await.unwrap().result
  else {
    panic!("first suffix read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(second)) = second.await.unwrap().result
  else {
    panic!("coalesced suffix read must succeed");
  };
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
  assert_eq!(
    store.observed_bounds.lock()[1],
    initial.sealed_before.map(SnowflakeId)
  );
  assert!(first.sealed_before > second.sealed_before);
  assert!(second.sealed_before > initial.sealed_before);
  assert_eq!(
    first.sealed_at_unix_ms,
    Some(timestamp(180).unix_timestamp() * 1_000)
  );
  assert_eq!(second.sealed_at_unix_ms, first.sealed_at_unix_ms);
  for response in [first, second] {
    assert_eq!(
      response
        .segments
        .iter()
        .map(|segment| segment.snowflake_id)
        .collect::<Vec<_>>(),
      segments
        .iter()
        .map(|segment| segment.snowflake_id.as_u64())
        .collect::<Vec<_>>()
    );
  }
}

#[tokio::test]
async fn concurrent_open_tail_reads_share_the_strong_suffix_scan() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let mut prefix = segment();
  prefix.window.window_start_unix_seconds = window_start;
  prefix.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(30));
  prefix
    .segment_index
    .insert(1, prefix.segment_index[&0].clone());
  let mut suffix = prefix.clone();
  suffix.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(90));
  let store = Arc::new(GatedMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![prefix, suffix],
    scan_started: Semaphore::new(0),
    release_scan: Semaphore::new(1),
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.limits.request_timeout = StdDuration::from_secs(5);
  config.limits.max_refills = 1;
  config.limits.max_waiters_per_key = 12;
  config.limits.max_waiters = 13;
  config.topics.get_mut("topic").unwrap().partition_count = 2;
  let cache = Arc::new(
    MetadataCache::new(store.clone(), config, None)
      .time_provider(Arc::new(ManualTimeProvider::new(timestamp(100)))),
  );
  let mut request = tail_request_with_bounds(
    vec![(0, 0), (1, 0)],
    MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG,
  );
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(85)).as_u64());
  assert!(matches!(
    cache.read(request.clone()).await.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  let _initial_scan = store.scan_started.acquire().await.unwrap();

  let first_cache = cache.clone();
  let first_request = request.clone();
  let first = tokio::spawn(async move { first_cache.read(first_request).await });
  let _suffix_scan = timeout(StdDuration::from_secs(1), store.scan_started.acquire())
    .await
    .unwrap()
    .unwrap();
  let second_cache = cache.clone();
  let mut narrower = request.clone();
  if let Some(read_metadata_window_request::Coverage::Tail(coverage)) = narrower.coverage.as_mut() {
    coverage.partition_bounds[1].min_snowflake =
      SnowflakeId::minimum_for_timestamp(timestamp(70)).as_u64();
  }
  let second = tokio::spawn(async move { second_cache.read(narrower).await });
  let mut readers = vec![(first, 2), (second, 1)];
  for _ in 0 .. 10 {
    let reader_cache = cache.clone();
    let reader_request = request.clone();
    readers.push((
      tokio::spawn(async move { reader_cache.read(reader_request).await }),
      2,
    ));
  }
  let joined = timeout(StdDuration::from_secs(1), async {
    while cache.snapshot().await.active_waiters < 12 {
      tokio::task::yield_now().await;
    }
  })
  .await;
  assert!(
    joined.is_ok(),
    "suffix scan waiters={}, scans={}",
    cache.snapshot().await.active_waiters,
    store.scans.load(Ordering::Relaxed)
  );
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
  assert_eq!(cache.snapshot().await.in_flight_refills, 1);
  let rejected = cache.read(request).await;
  let Some(read_metadata_window_response::Result::Failure(failure)) = rejected.result else {
    panic!("thirteenth suffix waiter must be rejected");
  };
  assert_eq!(
    failure.overload_reason.enum_value().unwrap(),
    MetadataReadOverloadReason::METADATA_READ_OVERLOAD_REASON_PER_KEY_WAITERS
  );
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
  store.release_scan.add_permits(1);
  for (read, prefix_partitions) in readers {
    let Some(read_metadata_window_response::Result::Success(response)) = read.await.unwrap().result
    else {
      panic!("coalesced strong suffix read must succeed");
    };
    assert_eq!(response.segments.len(), 2);
    assert!(response.retained_strong_coverage);
    assert_eq!(
      response.segments[0]
        .metadata
        .as_ref()
        .unwrap()
        .partitions
        .len(),
      prefix_partitions
    );
  }
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn retained_strong_prefix_respects_a_later_narrower_seal_and_horizon() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let mut prefix = segment();
  prefix.window.window_start_unix_seconds = window_start;
  prefix.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(30));
  let mut suffix = prefix.clone();
  suffix.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(70));
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![prefix, suffix],
  });
  let clock = Arc::new(ManualTimeProvider::new(timestamp(100)));
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(clock),
  );
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(85)).as_u64());
  let first = cache.read(request.clone()).await;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(65)).as_u64());
  request.requested_seal_horizon_ms = Some(45_000);
  let second = cache.read(request).await;
  let Some(read_metadata_window_response::Result::Success(first)) = first.result else {
    panic!("initial strong read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(second)) = second.result else {
    panic!("narrower strong read must succeed");
  };
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
  assert_eq!(
    second.sealed_before,
    Some(SnowflakeId::minimum_for_timestamp(timestamp(55)).as_u64())
  );
  assert_eq!(
    store.observed_bounds.lock()[1],
    second.sealed_before.map(SnowflakeId)
  );
  assert_eq!(second.segments.len(), 2);
  assert!(second.retained_strong_coverage);
  assert_eq!(second.sealed_at_unix_ms, first.sealed_at_unix_ms);
}

#[tokio::test]
async fn tail_floor_beyond_retained_strong_prefix_starts_a_fresh_scan() {
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: Vec::new(),
  });
  let cache = Arc::new(
    MetadataCache::new(
      store.clone(),
      cache_config(Duration::seconds(1), StdDuration::ZERO),
      None,
    )
    .time_provider(Arc::new(ManualTimeProvider::new(timestamp(100)))),
  );
  let mut request = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  request.window_start_unix_seconds = window_start;
  request.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(85)).as_u64());
  let first = cache.read(request.clone()).await;
  assert!(matches!(
    first.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  let floor = SnowflakeId::minimum_for_timestamp(timestamp(90));
  if let Some(read_metadata_window_request::Coverage::Tail(tail)) = request.coverage.as_mut() {
    tail.partition_bounds[0].min_snowflake = floor.as_u64();
  }
  let second = cache.read(request).await;
  assert!(matches!(
    second.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  assert_eq!(store.scans.load(Ordering::Relaxed), 2);
  assert_eq!(store.observed_bounds.lock()[1], Some(floor));
}

#[tokio::test]
async fn reports_whether_eventual_response_used_retained_coverage() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let cache = Arc::new(MetadataCache::new(
    store,
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    None,
  ));

  let first = cache
    .read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;
  let second = cache
    .read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;
  let Some(read_metadata_window_response::Result::Success(first)) = first.result else {
    panic!("first eventual read must succeed");
  };
  let Some(read_metadata_window_response::Result::Success(second)) = second.result else {
    panic!("second eventual read must succeed");
  };

  assert!(!first.retained_coverage);
  assert!(second.retained_coverage);
}

#[tokio::test]
async fn retains_full_recovery_in_its_separate_budget() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let cache = Arc::new(MetadataCache::new(
    store.clone(),
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    None,
  ));

  let first = cache.read(full_recovery_request()).await;
  let second = cache.read(full_recovery_request()).await;
  assert!(matches!(
    first.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  let Some(read_metadata_window_response::Result::Success(second)) = second.result else {
    panic!("second full recovery metadata read must succeed");
  };
  assert!(second.retained_coverage);
  assert_eq!(store.scans.load(Ordering::Relaxed), 1);
  let snapshot = cache.snapshot().await;
  assert_eq!(snapshot.tail_entry_count, 0);
  assert_eq!(snapshot.recovery_entry_count, 1);
  assert!(snapshot.recovery_retained_bytes > 0);
}

#[tokio::test]
async fn strong_prefix_and_eventual_entries_share_the_configured_total_budget() {
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.tail_max_bytes = 1_048_577;
  config.recovery_max_bytes = 2_097_155;
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let mut metadata = segment();
  metadata.window.window_start_unix_seconds = window_start;
  metadata.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(100));
  let cache = Arc::new(MetadataCache::new(
    Arc::new(CountingMetadataStore {
      scans: AtomicUsize::new(0),
      observed_bounds: Mutex::new(Vec::new()),
      segments: vec![metadata],
    }),
    config.clone(),
    None,
  ));
  let mut eventual = tail_request(
    0,
    MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
  );
  eventual.window_start_unix_seconds = window_start;
  cache.read(eventual).await;
  let mut recovery = full_recovery_request();
  recovery.window_start_unix_seconds = window_start;
  cache.read(recovery).await;
  let mut strong = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  strong.window_start_unix_seconds = window_start;
  strong.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  cache.read(strong).await;
  let snapshot = cache.snapshot().await;
  assert_eq!(snapshot.tail_entry_count, 1);
  assert_eq!(snapshot.recovery_entry_count, 1);
  assert_eq!(snapshot.strong_sealed_entry_count, 1);
  assert!(snapshot.strong_sealed_retained_bytes > 0);
  assert!(snapshot.tail_retained_bytes <= snapshot.tail_byte_budget);
  assert!(snapshot.recovery_retained_bytes <= snapshot.recovery_byte_budget);
  assert!(snapshot.strong_sealed_retained_bytes <= snapshot.strong_sealed_byte_budget);
  assert_eq!(snapshot.tail_byte_budget, 524_289);
  assert_eq!(snapshot.recovery_byte_budget, 1_048_578);
  assert_eq!(snapshot.strong_sealed_byte_budget, 1_572_865);
  assert_eq!(
    snapshot.tail_byte_budget + snapshot.recovery_byte_budget + snapshot.strong_sealed_byte_budget,
    config.tail_max_bytes + config.recovery_max_bytes
  );
}

#[tokio::test]
async fn strong_prefix_capacity_eviction_increments_evictions_total() {
  let collector = Collector::default();
  let metrics = Helper::new_with_collector(collector.clone());
  let window_start = 1_735_689_600;
  let timestamp = |offset| OffsetDateTime::from_unix_timestamp(window_start + offset).unwrap();
  let mut metadata = segment();
  metadata.window.window_start_unix_seconds = window_start;
  metadata.snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(30));
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.tail_max_bytes = u64::from(estimate_retained_bytes(&[metadata.clone()])) * 2;
  config.recovery_max_bytes = 0;
  let cache = Arc::new(
    MetadataCache::new_inner(
      Arc::new(CountingMetadataStore {
        scans: AtomicUsize::new(0),
        observed_bounds: Mutex::new(Vec::new()),
        segments: vec![metadata],
      }),
      config,
      None,
      &collector.scope("blob_stream_broker_test"),
    )
    .time_provider(Arc::new(ManualTimeProvider::new(timestamp(100)))),
  );

  let mut tail = tail_request(0, MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG);
  tail.window_start_unix_seconds = window_start;
  tail.requested_seal_before = Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  assert!(matches!(
    cache.read(tail).await.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  cache.run_maintenance().await;
  assert_eq!(cache.snapshot().await.strong_sealed_entry_count, 1);

  let mut recovery = full_recovery_request();
  recovery.consistency = MetadataReadConsistency::METADATA_READ_CONSISTENCY_STRONG.into();
  recovery.window_start_unix_seconds = window_start;
  recovery.requested_seal_before =
    Some(SnowflakeId::minimum_for_timestamp(timestamp(300)).as_u64());
  assert!(matches!(
    cache.read(recovery).await.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  cache.run_maintenance().await;
  assert_eq!(cache.snapshot().await.strong_sealed_entry_count, 1);
  metrics.assert_counter_eq(
    1,
    "blob_stream_broker_test:metadata_cache:evictions_total",
    &labels!(),
  );
}

#[tokio::test]
async fn recovery_cache_maintenance_evicts_invalidated_entries() {
  let collector = Collector::default();
  let metrics = Helper::new_with_collector(collector.clone());
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let cache = MetadataCache::new_with_metrics(
    Arc::new(CountingMetadataStore {
      scans: AtomicUsize::new(0),
      observed_bounds: Mutex::new(Vec::new()),
      segments: vec![segment()],
    }),
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    None,
    &shutdown_trigger.make_handle(),
    &collector.scope("blob_stream_broker_test"),
  );
  let request = full_recovery_request();
  let key = cache.validate_request(&request).unwrap().key;

  let response = cache.read(request).await;
  assert!(matches!(
    response.result,
    Some(read_metadata_window_response::Result::Success(_))
  ));
  metrics.assert_gauge_eq(
    1,
    "blob_stream_broker_test:metadata_cache:recovery_entries",
    &labels!(),
  );

  cache.eventual_recovery_entries.invalidate(&key).await;
  cache.run_maintenance().await;

  metrics.assert_gauge_eq(
    0,
    "blob_stream_broker_test:metadata_cache:recovery_entries",
    &labels!(),
  );
  metrics.assert_gauge_eq(
    0,
    "blob_stream_broker_test:metadata_cache:recovery_retained_bytes",
    &labels!(),
  );
  shutdown_trigger.shutdown().await;
}

#[tokio::test]
async fn rejects_response_item_limit_without_partial_success() {
  let mut second_segment = segment();
  second_segment.snowflake_id = SnowflakeId(101);
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment(), second_segment],
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.limits.max_response_items = 1;
  let cache = Arc::new(MetadataCache::new(store, config, None));

  let response = cache
    .read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;
  let Some(read_metadata_window_response::Result::Failure(failure)) = response.result else {
    panic!("item-limited metadata response must fail atomically");
  };
  assert_eq!(
    failure.status,
    MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED.into()
  );
  assert!(
    failure
      .error_message
      .contains("response exceeds configured item limit")
  );
}

#[tokio::test]
async fn rejects_oversized_cache_generation_without_retention() {
  let mut second_segment = segment();
  second_segment.snowflake_id = SnowflakeId(101);
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment(), second_segment],
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.limits.max_entry_items = 1;
  let cache = Arc::new(MetadataCache::new(store, config, None));

  let response = cache
    .read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;
  let Some(read_metadata_window_response::Result::Failure(failure)) = response.result else {
    panic!("oversized cache generation must fail atomically");
  };
  assert_eq!(
    failure.status,
    MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED.into()
  );
  assert_eq!(cache.snapshot().await.tail_entry_count, 0);
}

#[tokio::test]
async fn sanitizes_internal_storage_failures() {
  let cache = Arc::new(MetadataCache::new(
    Arc::new(FailingMetadataStore),
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    None,
  ));

  let response = cache
    .read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;
  let Some(read_metadata_window_response::Result::Failure(failure)) = response.result else {
    panic!("storage failure must return a failure response");
  };
  assert_eq!(
    failure.status,
    MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_FAILED.into()
  );
  assert_eq!(
    failure.error_message.to_string(),
    "metadata cache request failed"
  );
  assert!(!failure.error_message.contains("private-segment-metadata"));
}

#[tokio::test]
async fn records_cache_hit_miss_refill_and_response_metrics() {
  let collector = Collector::default();
  let metrics = Helper::new_with_collector(collector.clone());
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let cache = MetadataCache::new_with_metrics(
    store,
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    None,
    &shutdown_trigger.make_handle(),
    &collector.scope("blob_stream_broker_test"),
  );

  cache
    .read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;
  cache
    .read(tail_request(
      100,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;
  cache
    .read(tail_request(
      99,
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;

  metrics.assert_counter_eq(
    3,
    "blob_stream_broker_test:metadata_cache:requests_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    2,
    "blob_stream_broker_test:metadata_cache:storage_queries_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    2,
    "blob_stream_broker_test:metadata_cache:coalescing_window_requests_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    "blob_stream_broker_test:metadata_cache:tail_hits_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    2,
    "blob_stream_broker_test:metadata_cache:tail_refills_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    "blob_stream_broker_test:metadata_cache:invalidations_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    0,
    "blob_stream_broker_test:metadata_cache:evictions_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    3,
    "blob_stream_broker_test:metadata_cache:response_items_total",
    &labels!(),
  );
  metrics.assert_gauge_eq(
    1,
    "blob_stream_broker_test:metadata_cache:tail_entries",
    &labels!(),
  );
  metrics.assert_gauge_eq(
    0,
    "blob_stream_broker_test:metadata_cache:active_waiters",
    &labels!(),
  );
  let _snapshot = cache.snapshot().await;
  metrics.assert_gauge_eq(
    1,
    "blob_stream_broker_test:metadata_cache:tail_entries",
    &labels!(),
  );
  shutdown_trigger.shutdown().await;
}

#[tokio::test]
async fn rejects_partition_limit_before_metadata_store_scan() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.topics.get_mut("topic").unwrap().partition_count = 2;
  config.limits.max_request_partitions = 1;
  let cache = Arc::new(MetadataCache::new(store.clone(), config, None));

  let response = cache
    .read(tail_request_with_bounds(
      vec![(0, 100), (1, 100)],
      MetadataReadConsistency::METADATA_READ_CONSISTENCY_EVENTUAL,
    ))
    .await;
  let Some(read_metadata_window_response::Result::Failure(failure)) = response.result else {
    panic!("partition-limited metadata request must fail");
  };
  assert_eq!(
    failure.status,
    MetadataReadFailureStatus::METADATA_READ_FAILURE_STATUS_OVERLOADED.into()
  );
  assert_eq!(store.scans.load(Ordering::Relaxed), 0);
}
