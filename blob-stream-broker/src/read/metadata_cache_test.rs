#![allow(clippy::unwrap_used)]

use super::*;
use async_trait::async_trait;
use bd_runtime_config::loader::Loader;
use bd_server_stats::stats::Collector;
use bd_server_stats::test::util::stats::Helper;
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
    let _permit = self.release_scan.acquire().await.unwrap();
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
    coalescing_window,
    request_timeout: StdDuration::from_secs(1),
    max_waiters_per_key: DEFAULT_MAX_WAITERS_PER_KEY,
    max_waiters: DEFAULT_MAX_WAITERS,
    max_refills: DEFAULT_MAX_REFILLS,
    max_request_partitions: DEFAULT_MAX_REQUEST_PARTITIONS,
    max_response_items: DEFAULT_MAX_RESPONSE_ITEMS,
    max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
    max_entry_items: DEFAULT_MAX_ENTRY_ITEMS,
    topics: HashMap::from([(
      "topic".to_string(),
      TopicCacheContract {
        partition_count: 1,
        num_writers: 1,
        metadata_window_size: Duration::minutes(5),
        metadata_cache_max_age: maximum_age,
      },
    )]),
  }
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
  assert_eq!(config.max_waiters_per_key, 2);
  assert_eq!(config.max_waiters, 3);
  assert_eq!(config.max_refills, 4);
  assert_eq!(config.max_request_partitions, 5);
  assert_eq!(config.max_response_items, 6);
  assert_eq!(config.max_response_bytes, 7);
  assert_eq!(config.max_entry_items, 8);
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
      vec![BatchMetadata {
        seq_range: SeqRange { start: 0, end: 1 },
        byte_range: ByteRange { start: 0, end: 1 },
        payload_bytes: 1,
      }],
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
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let cache = Arc::new(MetadataCache::new_with_metrics(
    store.clone(),
    cache_config(Duration::seconds(1), StdDuration::from_millis(20)),
    &collector.scope("blob_stream_broker_test"),
  ));

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
async fn rejected_waiter_does_not_start_an_unowned_refill() {
  let store = Arc::new(GatedMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
    scan_started: Semaphore::new(0),
    release_scan: Semaphore::new(0),
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.max_waiters = 1;
  let cache = Arc::new(MetadataCache::new(store.clone(), config));

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
        vec![BatchMetadata {
          seq_range: SeqRange { start: 0, end: 1 },
          byte_range: ByteRange { start: 0, end: 1 },
          payload_bytes: 1,
        }],
      ),
      (
        1,
        vec![BatchMetadata {
          seq_range: SeqRange { start: 0, end: 1 },
          byte_range: ByteRange { start: 1, end: 2 },
          payload_bytes: 1,
        }],
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
  let cache = Arc::new(MetadataCache::new(store, config));

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
async fn reports_whether_eventual_response_used_retained_coverage() {
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let cache = Arc::new(MetadataCache::new(
    store,
    cache_config(Duration::seconds(1), StdDuration::ZERO),
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
async fn rejects_response_item_limit_without_partial_success() {
  let mut second_segment = segment();
  second_segment.snowflake_id = SnowflakeId(101);
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment(), second_segment],
  });
  let mut config = cache_config(Duration::seconds(1), StdDuration::ZERO);
  config.max_response_items = 1;
  let cache = Arc::new(MetadataCache::new(store, config));

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
  config.max_entry_items = 1;
  let cache = Arc::new(MetadataCache::new(store, config));

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
  let store = Arc::new(CountingMetadataStore {
    scans: AtomicUsize::new(0),
    observed_bounds: Mutex::new(Vec::new()),
    segments: vec![segment()],
  });
  let cache = Arc::new(MetadataCache::new_with_metrics(
    store,
    cache_config(Duration::seconds(1), StdDuration::ZERO),
    &collector.scope("blob_stream_broker_test"),
  ));

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

  metrics.assert_counter_eq(
    2,
    "blob_stream_broker_test:metadata_cache:requests_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    "blob_stream_broker_test:metadata_cache:storage_queries_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    "blob_stream_broker_test:metadata_cache:coalescing_window_requests_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    "blob_stream_broker_test:metadata_cache:tail_hits_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    "blob_stream_broker_test:metadata_cache:tail_refills_total",
    &labels!(),
  );
  metrics.assert_counter_eq(
    2,
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
  config.max_request_partitions = 1;
  let cache = Arc::new(MetadataCache::new(store.clone(), config));

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
