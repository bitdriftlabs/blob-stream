#![allow(clippy::unwrap_used)]

use super::diagnostics::MAX_METADATA_SOURCE_DETAILS;
use super::state::{RecoveryState, VirtualPartitionState};
use super::{
  BrokerBlobRangeQuery,
  BrokerMetadataQuery,
  ConsumerBatch,
  ConsumerBatchSource,
  ConsumerReader as BoundedConsumerReader,
  ConsumerReaderFastFrontierState,
  ConsumerReaderFastScanBoundState,
  ConsumerReaderImpl,
  ReadCapacity,
};
use crate::config::{
  ConsumerReadConfig,
  ConsumerReadRuntimeSettings,
  DEFAULT_MAX_CLOCK_SKEW,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG,
};
use crate::iterator::ConsumerSeekTarget;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bd_runtime_config::loader::Loader;
use bd_server_stats::stats::Collector;
use bd_server_stats::test::util::stats::Helper;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use bd_time::{SystemTimeProvider, TimeProvider};
use blob_stream_blob_store::{
  BlobCacheAdmission,
  BlobKey,
  BlobStore,
  BlobStoreError,
  BlobStoreResult,
  ByteRange,
  InMemoryBlobStore,
};
use blob_stream_metadata_store::{
  InMemoryMetadataStore,
  MetadataReadConsistency,
  MetadataStore,
  MetadataWriteResult,
  SegmentMetadata,
  encode_segment_metadata_v1,
};
use blob_stream_proto::protos::blobstream::v1::broker::{
  BlobRangeRequest,
  BlobRangeResult,
  BlobReadFailure,
  BlobReadFailureStatus,
  BlobReadSuccess,
  BrokerSegmentMetadata,
  MetadataReadSuccess,
  ReadBlobRangesRequest,
  ReadBlobRangesResponse,
  ReadMetadataWindowRequest,
  ReadMetadataWindowResponse,
  StoredRecordBatch,
  read_blob_ranges_response,
  read_metadata_window_request,
  read_metadata_window_response,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{
  BatchMetadata,
  CommittedCursor,
  CommittedSourceCheckpoint,
  Compression,
  CompressionCodec,
  Record,
  RecordBatch,
  SeqRange,
  SnowflakeId,
  ToProtoDuration,
  TopicWindowKey,
  VirtualPartitionId,
  Window,
  new_record,
};
use bytes::Bytes;
use parking_lot::Mutex;
use prometheus::labels;
use protobuf::Message;
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::Semaphore;
use tokio::time::{Duration, timeout};

const TEST_READ_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

fn timestamp(unix_seconds: i64) -> OffsetDateTime {
  OffsetDateTime::from_unix_timestamp(unix_seconds).expect("test timestamp is in range")
}

impl ConsumerReaderImpl {
  async fn read_available(&mut self, now_unix_seconds: i64) -> Result<Vec<super::ConsumerBatch>> {
    BoundedConsumerReader::read_available(
      self,
      timestamp(now_unix_seconds),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
    )
    .await
  }
}

fn batch_for_slicing_test(seq_range: SeqRange) -> ConsumerBatch {
  let records = (seq_range.start ..= seq_range.end)
    .map(|offset| new_record(vec![u8::try_from(offset).unwrap()], 0))
    .collect();
  ConsumerBatch {
    virtual_partition_id: 7,
    seq_range,
    source_checkpoint: CommittedSourceCheckpoint {
      window_start_unix_seconds: 900,
      snowflake_id: 1,
    },
    source: ConsumerBatchSource {
      blob_key: BlobKey::new("telemetry/900/1.bin"),
      metadata_published_at: OffsetDateTime::UNIX_EPOCH,
    },
    admission_scan: None,
    records,
  }
}

#[test]
fn fast_coverage_floor_seeds_only_an_uninitialized_fast_partition() {
  let initial_floor = timestamp(100);
  let completed_scan_floor = timestamp(200);
  let later_attempt_floor = timestamp(300);
  let mut state = VirtualPartitionState::Fast {
    cursor: Some(1),
    coverage_floor: None,
    last_scan: None,
  };

  state.seed_fast_coverage_floor(initial_floor);
  assert_eq!(state.fast_coverage_floor(), Some(initial_floor));

  state.set_fast_coverage_floor(completed_scan_floor);
  state.seed_fast_coverage_floor(later_attempt_floor);
  assert_eq!(state.fast_coverage_floor(), Some(completed_scan_floor));
}

struct RecordingMetadataStore {
  inner: InMemoryMetadataStore,
  scans: Mutex<Vec<(i64, Option<SnowflakeId>)>>,
  consistencies: Mutex<Vec<MetadataReadConsistency>>,
}

impl RecordingMetadataStore {
  fn new() -> Self {
    Self {
      inner: InMemoryMetadataStore::new(),
      scans: Mutex::new(Vec::new()),
      consistencies: Mutex::new(Vec::new()),
    }
  }
}

#[async_trait]
impl MetadataStore for RecordingMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[blob_stream_metadata_store::ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> MetadataWriteResult {
    self.inner.write_segment(metadata, fences, now_ts_ms).await
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    self.consistencies.lock().push(consistency);
    self
      .scans
      .lock()
      .push((window.window_start_unix_seconds, min_snowflake));
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await
  }
}

struct FailingMetadataStore;

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
    _consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    Err(anyhow!("injected DynamoDB dispatch error").context("injected DynamoDB service error"))
  }
}

//
// RecordingBrokerMetadataQuery
//

struct RecordingBrokerMetadataQuery {
  requests: Mutex<Vec<ReadMetadataWindowRequest>>,
}

//
// FixedBrokerMetadataQuery
//

struct FixedBrokerMetadataQuery {
  responses: HashMap<i64, ReadMetadataWindowResponse>,
}

enum BrokerMetadataResponse {
  Valid,
  TransportFailure,
  Malformed,
  StaleAtReceipt,
}

#[async_trait]
impl BrokerMetadataQuery for FixedBrokerMetadataQuery {
  async fn read_metadata_window(
    &self,
    request: ReadMetadataWindowRequest,
  ) -> Result<ReadMetadataWindowResponse> {
    self
      .responses
      .get(&request.window_start_unix_seconds)
      .cloned()
      .ok_or_else(|| anyhow!("missing scripted broker response"))
  }
}

fn broker_metadata_response(
  segments: &[SegmentMetadata],
  observed_at: OffsetDateTime,
) -> ReadMetadataWindowResponse {
  ReadMetadataWindowResponse {
    result: Some(read_metadata_window_response::Result::Success(
      MetadataReadSuccess {
        observed_at_unix_ms: observed_at.unix_timestamp() * 1_000,
        refill_floor: Some(0),
        generation: 1,
        retained_coverage: false,
        segments: segments
          .iter()
          .map(|segment| BrokerSegmentMetadata {
            snowflake_id: segment.snowflake_id.as_u64(),
            metadata: Some(encode_segment_metadata_v1(segment).unwrap()).into(),
            ..Default::default()
          })
          .collect(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

impl RecordingBrokerMetadataQuery {
  fn requests(&self) -> Vec<ReadMetadataWindowRequest> {
    self.requests.lock().clone()
  }
}

#[async_trait]
impl BrokerMetadataQuery for RecordingBrokerMetadataQuery {
  async fn read_metadata_window(
    &self,
    request: ReadMetadataWindowRequest,
  ) -> Result<ReadMetadataWindowResponse> {
    self.requests.lock().push(request);
    Err(anyhow!("injected broker metadata transport failure"))
  }
}

fn rejecting_broker_metadata_query() -> Arc<dyn BrokerMetadataQuery> {
  Arc::new(RecordingBrokerMetadataQuery {
    requests: Mutex::new(Vec::new()),
  })
}

//
// FixedBrokerBlobRangeQuery
//

struct FixedBrokerBlobRangeQuery {
  requests: Mutex<Vec<ReadBlobRangesRequest>>,
  responses: Mutex<VecDeque<ReadBlobRangesResponse>>,
}

impl FixedBrokerBlobRangeQuery {
  fn new(response: ReadBlobRangesResponse) -> Self {
    Self::with_responses([response])
  }

  fn with_responses(responses: impl IntoIterator<Item = ReadBlobRangesResponse>) -> Self {
    Self {
      requests: Mutex::new(Vec::new()),
      responses: Mutex::new(responses.into_iter().collect()),
    }
  }

  fn requests(&self) -> Vec<ReadBlobRangesRequest> {
    self.requests.lock().clone()
  }
}

#[async_trait]
impl BrokerBlobRangeQuery for FixedBrokerBlobRangeQuery {
  async fn read_blob_ranges(
    &self,
    request: ReadBlobRangesRequest,
  ) -> Result<ReadBlobRangesResponse> {
    self.requests.lock().push(request);
    self
      .responses
      .lock()
      .pop_front()
      .ok_or_else(|| anyhow!("test broker blob-range responses were exhausted"))
  }
}

fn rejecting_broker_blob_range_query() -> Arc<dyn BrokerBlobRangeQuery> {
  Arc::new(FixedBrokerBlobRangeQuery::with_responses(Vec::new()))
}

struct KeyedBrokerBlobRangeQuery {
  requests: Mutex<Vec<ReadBlobRangesRequest>>,
  responses: HashMap<String, ReadBlobRangesResponse>,
}

impl KeyedBrokerBlobRangeQuery {
  fn new(responses: impl IntoIterator<Item = (String, ReadBlobRangesResponse)>) -> Self {
    Self {
      requests: Mutex::new(Vec::new()),
      responses: responses.into_iter().collect(),
    }
  }

  fn requests(&self) -> Vec<ReadBlobRangesRequest> {
    self.requests.lock().clone()
  }
}

#[async_trait]
impl BrokerBlobRangeQuery for KeyedBrokerBlobRangeQuery {
  async fn read_blob_ranges(
    &self,
    request: ReadBlobRangesRequest,
  ) -> Result<ReadBlobRangesResponse> {
    let response = self
      .responses
      .get(request.blob_key.as_str())
      .cloned()
      .ok_or_else(|| anyhow!("test broker blob-range response is missing"))?;
    self.requests.lock().push(request);
    Ok(response)
  }
}

fn broker_blob_success(payloads: Vec<Bytes>) -> ReadBlobRangesResponse {
  ReadBlobRangesResponse {
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
  }
}

fn broker_blob_failure(status: BlobReadFailureStatus) -> ReadBlobRangesResponse {
  ReadBlobRangesResponse {
    result: Some(read_blob_ranges_response::Result::Failure(
      BlobReadFailure {
        status: status.into(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

//
// FailSecondRangeBlobStore
//

struct FailSecondRangeBlobStore {
  inner: InMemoryBlobStore,
  range_reads: AtomicUsize,
}

impl FailSecondRangeBlobStore {
  fn new() -> Self {
    Self {
      inner: InMemoryBlobStore::new(),
      range_reads: AtomicUsize::new(0),
    }
  }
}

#[async_trait]
impl BlobStore for FailSecondRangeBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    self.inner.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    if self.range_reads.fetch_add(1, Ordering::SeqCst) == 1 {
      return Err(BlobStoreError::Read {
        key: key.as_str().to_string(),
        source: anyhow::anyhow!("injected second range-read failure"),
      });
    }
    self.inner.get_range(key, range).await
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    self.inner.get_with_cache_admission(key, admission).await
  }
}

//
// MissingRangeBlobStore
//

struct MissingRangeBlobStore {
  inner: InMemoryBlobStore,
  missing_key: Mutex<Option<BlobKey>>,
}

impl MissingRangeBlobStore {
  fn new() -> Self {
    Self {
      inner: InMemoryBlobStore::new(),
      missing_key: Mutex::new(None),
    }
  }

  fn mark_missing(&self, key: BlobKey) {
    *self.missing_key.lock() = Some(key);
  }
}

#[async_trait]
impl BlobStore for MissingRangeBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    self.inner.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    if self.missing_key.lock().as_ref() == Some(key) {
      return Err(BlobStoreError::NotFound {
        key: key.as_str().to_string(),
      });
    }
    self.inner.get_range(key, range).await
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    if self.missing_key.lock().as_ref() == Some(key) {
      return Err(BlobStoreError::NotFound {
        key: key.as_str().to_string(),
      });
    }
    self.inner.get_with_cache_admission(key, admission).await
  }
}

//
// RecordingRangeBlobStore
//

struct RecordingRangeBlobStore {
  inner: InMemoryBlobStore,
  ranges: Mutex<Vec<ByteRange>>,
}

impl RecordingRangeBlobStore {
  fn new() -> Self {
    Self {
      inner: InMemoryBlobStore::new(),
      ranges: Mutex::new(Vec::new()),
    }
  }

  fn ranges(&self) -> Vec<ByteRange> {
    self.ranges.lock().clone()
  }
}

#[async_trait]
impl BlobStore for RecordingRangeBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    self.inner.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    self.ranges.lock().push(range.clone());
    self.inner.get_range(key, range).await
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    self.inner.get_with_cache_admission(key, admission).await
  }
}

//
// TruncatingRangeBlobStore
//

struct TruncatingRangeBlobStore {
  inner: InMemoryBlobStore,
}

impl TruncatingRangeBlobStore {
  fn new() -> Self {
    Self {
      inner: InMemoryBlobStore::new(),
    }
  }
}

#[async_trait]
impl BlobStore for TruncatingRangeBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    self.inner.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    let payload = self.inner.get_range(key, range).await?;
    Ok(payload.slice(.. payload.len().saturating_sub(1)))
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    self.inner.get_with_cache_admission(key, admission).await
  }
}

//
// BlockingRangeBlobStore
//

struct BlockingRangeBlobStore {
  inner: InMemoryBlobStore,
  active_reads: AtomicUsize,
  maximum_active_reads: AtomicUsize,
  started_reads: Arc<Semaphore>,
  released_reads: Arc<Semaphore>,
}

impl BlockingRangeBlobStore {
  fn new() -> Self {
    Self {
      inner: InMemoryBlobStore::new(),
      active_reads: AtomicUsize::new(0),
      maximum_active_reads: AtomicUsize::new(0),
      started_reads: Arc::new(Semaphore::new(0)),
      released_reads: Arc::new(Semaphore::new(0)),
    }
  }

  fn record_active_read(&self, active_reads: usize) {
    let mut maximum_active_reads = self.maximum_active_reads.load(Ordering::SeqCst);
    while active_reads > maximum_active_reads {
      match self.maximum_active_reads.compare_exchange(
        maximum_active_reads,
        active_reads,
        Ordering::SeqCst,
        Ordering::SeqCst,
      ) {
        Ok(_) => return,
        Err(current) => maximum_active_reads = current,
      }
    }
  }
}

#[async_trait]
impl BlobStore for BlockingRangeBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    self.inner.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    let active_reads = self
      .active_reads
      .fetch_add(1, Ordering::SeqCst)
      .saturating_add(1);
    self.record_active_read(active_reads);
    self.started_reads.add_permits(1);
    self
      .released_reads
      .acquire()
      .await
      .expect("test range-read release semaphore is open")
      .forget();
    let result = self.inner.get_range(key, range).await;
    self.active_reads.fetch_sub(1, Ordering::SeqCst);
    result
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    self.inner.get_with_cache_admission(key, admission).await
  }
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_consumer_test")
}

#[test]
fn reader_applies_live_feature_flag_updates_between_scan_passes() {
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag("blob_stream_consumer_prefetch_max_bytes", 16)
      .with_integer_flag("blob_stream_consumer_max_in_flight_batch_reads", 2),
  ));
  let reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      prefetch_max_bytes: Some(8),
      max_in_flight_batch_reads: Some(1),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  assert_eq!(
    reader.runtime_settings(),
    ConsumerReadRuntimeSettings {
      prefetch_max_bytes: 16,
      max_in_flight_batch_reads: 2,
      metadata_read_consistency: MetadataReadConsistency::Eventual,
      metadata_visibility_delay: time::Duration::milliseconds(2_000),
    }
  );

  feature_flags.update(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag("blob_stream_consumer_prefetch_max_bytes", 32)
      .with_integer_flag("blob_stream_consumer_max_in_flight_batch_reads", 4)
      .with_bool_flag("blob_stream_consumer_strong_metadata_reads", true),
  ));
  assert_eq!(
    reader.runtime_settings(),
    ConsumerReadRuntimeSettings {
      prefetch_max_bytes: 32,
      max_in_flight_batch_reads: 4,
      metadata_read_consistency: MetadataReadConsistency::Strong,
      metadata_visibility_delay: time::Duration::ZERO,
    }
  );
}

#[test]
fn eventual_broker_reads_include_cache_age_in_availability_horizon() {
  let reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap()
  .metadata_cache_max_age(TimeDuration::milliseconds(250));

  assert_eq!(
    reader
      .availability_horizon(reader.runtime_settings())
      .duration(),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG
      .saturating_add(DEFAULT_MAX_CLOCK_SKEW)
      .saturating_add(TimeDuration::milliseconds(2_000))
      .saturating_add(TimeDuration::milliseconds(250))
  );
}

#[test]
fn reader_rejects_negative_publication_lag() {
  let result = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    -TimeDuration::nanoseconds(1),
    None,
  );
  let Err(error) = result else {
    panic!("reader accepted a negative maximum metadata publication lag");
  };

  assert!(
    error
      .to_string()
      .contains("maximum metadata publication lag must not be negative")
  );
}

async fn write_segment(
  blob_store: &dyn BlobStore,
  metadata_store: &dyn MetadataStore,
  topic: &str,
  window_start: i64,
  snowflake_id: u64,
  virtual_partition_id: VirtualPartitionId,
  seq_range: SeqRange,
  records: Vec<Record>,
  compression: Compression,
) {
  write_segment_with_publication_time(
    blob_store,
    metadata_store,
    topic,
    window_start,
    snowflake_id,
    virtual_partition_id,
    seq_range,
    records,
    compression,
    window_start * 1_000,
  )
  .await;
}

async fn write_segment_with_publication_time(
  blob_store: &dyn BlobStore,
  metadata_store: &dyn MetadataStore,
  topic: &str,
  window_start: i64,
  snowflake_id: u64,
  virtual_partition_id: VirtualPartitionId,
  seq_range: SeqRange,
  records: Vec<Record>,
  compression: Compression,
  metadata_published_ts_ms: i64,
) {
  let batch = RecordBatch::new(virtual_partition_id, records.clone());
  let encoded = StoredRecordBatch {
    virtual_partition_id,
    records: records.clone(),
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap();
  let payload = match compression.codec {
    CompressionCodec::None => encoded,
    CompressionCodec::Zstd => {
      let level = compression.level.unwrap_or(3);
      zstd::stream::encode_all(Cursor::new(encoded), level).unwrap()
    },
  };

  let blob_key = BlobKey::new(format!(
    "{topic}/{window_start}/{snowflake_id}.{}",
    match compression.codec {
      CompressionCodec::None => "bin",
      CompressionCodec::Zstd => "zst",
    }
  ));

  blob_store
    .put(&blob_key, Bytes::from(payload.clone()))
    .await
    .unwrap();

  let summary = batch.summary().unwrap();
  let metadata = SegmentMetadata::new(
    TopicWindowKey {
      topic: topic.to_string(),
      window_start_unix_seconds: window_start,
    },
    SnowflakeId(snowflake_id),
    blob_key,
    compression,
    HashMap::from([(
      virtual_partition_id,
      vec![BatchMetadata {
        seq_range,
        byte_range: blob_stream_types::ByteRange {
          start: 0,
          end: payload.len() as u64,
        },
        payload_bytes: summary.payload_bytes,
      }],
    )]),
    OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(window_start * 1_000),
    OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(metadata_published_ts_ms),
  );

  metadata_store
    .write_segment(metadata, None, 0)
    .await
    .unwrap();
}

#[test]
fn discard_through_preserves_only_the_unseen_suffix() {
  let mut before_range = batch_for_slicing_test(SeqRange { start: 2, end: 4 });
  before_range.discard_through(1);
  assert_eq!(before_range.seq_range, SeqRange { start: 2, end: 4 });
  assert_eq!(before_range.records.len(), 3);

  let mut inside_range = batch_for_slicing_test(SeqRange { start: 1, end: 3 });
  inside_range.discard_through(2);
  assert_eq!(inside_range.seq_range, SeqRange { start: 3, end: 3 });
  assert_eq!(inside_range.records[0].payload, vec![3]);
}

#[tokio::test]
async fn seek_slices_a_multi_record_batch_and_suppresses_a_fully_consumed_batch() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 3 },
    vec![
      new_record(vec![1], 900_000),
      new_record(vec![2], 900_001),
      new_record(vec![3], 900_002),
    ],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  reader.seek(
    7,
    &ConsumerSeekTarget {
      offset: 1,
      window_start_unix_seconds: 900,
      snowflake_id: None,
    },
    timestamp(900),
  );
  let sliced = reader.read_available(900).await.unwrap();
  assert_eq!(sliced.len(), 1);
  assert_eq!(sliced[0].seq_range, SeqRange { start: 2, end: 3 });
  assert_eq!(
    sliced[0]
      .records
      .iter()
      .map(|record| record.payload.clone())
      .collect::<Vec<_>>(),
    vec![vec![2], vec![3]]
  );
  assert_eq!(reader.cursor(7), Some(3));

  reader.seek(
    7,
    &ConsumerSeekTarget {
      offset: 3,
      window_start_unix_seconds: 900,
      snowflake_id: None,
    },
    timestamp(900),
  );
  assert!(reader.read_available(900).await.unwrap().is_empty());
  assert_eq!(reader.cursor(7), Some(3));
}

#[tokio::test]
async fn reader_rejects_decoded_batch_with_mismatched_record_count() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 2 },
    vec![new_record(vec![1], 900_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let error = reader.read_available(900).await.unwrap_err();
  assert!(
    error
      .to_string()
      .contains("decoded record count 1 does not match sequence range 1..=2")
  );
}

async fn write_multi_partition_segment(
  blob_store: &dyn BlobStore,
  metadata_store: &dyn MetadataStore,
  topic: &str,
  window_start: i64,
  snowflake_id: u64,
  compression: Compression,
  batches: Vec<(VirtualPartitionId, SeqRange, Vec<Record>)>,
) -> u64 {
  let mut payload = Vec::new();
  let mut segment_index = HashMap::new();

  for (virtual_partition_id, seq_range, records) in batches {
    let batch = RecordBatch::new(virtual_partition_id, records.clone());
    let encoded = StoredRecordBatch {
      virtual_partition_id,
      records,
      ..Default::default()
    }
    .write_to_bytes()
    .unwrap();
    let encoded = match compression.codec {
      CompressionCodec::None => encoded,
      CompressionCodec::Zstd => {
        let level = compression.level.unwrap_or(3);
        zstd::stream::encode_all(Cursor::new(encoded), level).unwrap()
      },
    };
    let start = u64::try_from(payload.len()).unwrap();
    payload.extend_from_slice(&encoded);
    let end = u64::try_from(payload.len()).unwrap();

    segment_index
      .entry(virtual_partition_id)
      .or_insert_with(Vec::new)
      .push(BatchMetadata {
        seq_range,
        byte_range: blob_stream_types::ByteRange { start, end },
        payload_bytes: batch.summary().unwrap().payload_bytes,
      });
  }

  let payload_len = u64::try_from(payload.len()).unwrap();
  let blob_key = BlobKey::new(format!(
    "{topic}/{window_start}/{snowflake_id}.{}",
    match compression.codec {
      CompressionCodec::None => "bin",
      CompressionCodec::Zstd => "zst",
    }
  ));
  blob_store
    .put(&blob_key, Bytes::from(payload))
    .await
    .unwrap();
  metadata_store
    .write_segment(
      SegmentMetadata::new(
        TopicWindowKey {
          topic: topic.to_string(),
          window_start_unix_seconds: window_start,
        },
        SnowflakeId(snowflake_id),
        blob_key,
        compression,
        segment_index,
        OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(window_start * 1_000),
        OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(window_start * 1_000),
      ),
      None,
      0,
    )
    .await
    .unwrap();

  payload_len
}

async fn write_shared_blob_segments(
  blob_store: &dyn BlobStore,
  metadata_store: &dyn MetadataStore,
  topic: &str,
  window_start: i64,
  batches: Vec<(u64, VirtualPartitionId, SeqRange, Vec<Record>)>,
) -> (BlobKey, Vec<Bytes>) {
  let blob_key = BlobKey::new(format!("{topic}/{window_start}/shared.bin"));
  let mut blob = Vec::new();
  let mut batch_payloads = Vec::with_capacity(batches.len());

  for (snowflake_id, virtual_partition_id, seq_range, records) in batches {
    let record_batch = RecordBatch::new(virtual_partition_id, records.clone());
    let payload = Bytes::from(
      StoredRecordBatch {
        virtual_partition_id,
        records,
        ..Default::default()
      }
      .write_to_bytes()
      .unwrap(),
    );
    let start = u64::try_from(blob.len()).unwrap();
    blob.extend_from_slice(&payload);
    let end = u64::try_from(blob.len()).unwrap();
    metadata_store
      .write_segment(
        SegmentMetadata::new(
          TopicWindowKey {
            topic: topic.to_string(),
            window_start_unix_seconds: window_start,
          },
          SnowflakeId(snowflake_id),
          blob_key.clone(),
          Compression::none(),
          HashMap::from([(
            virtual_partition_id,
            vec![BatchMetadata {
              seq_range,
              byte_range: blob_stream_types::ByteRange { start, end },
              payload_bytes: record_batch.summary().unwrap().payload_bytes,
            }],
          )]),
          OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(window_start * 1_000),
          OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(window_start * 1_000),
        ),
        None,
        0,
      )
      .await
      .unwrap();
    batch_payloads.push(payload);
  }

  blob_store.put(&blob_key, Bytes::from(blob)).await.unwrap();
  (blob_key, batch_payloads)
}

#[tokio::test]
async fn metadata_source_diagnostics_retain_the_newest_snowflake_rows() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let source_count = u64::try_from(MAX_METADATA_SOURCE_DETAILS).unwrap() + 1;
  let metadata_batches = (1 ..= source_count)
    .map(|snowflake_id| {
      (
        snowflake_id,
        7,
        SeqRange {
          start: snowflake_id,
          end: snowflake_id,
        },
        vec![new_record(
          vec![u8::try_from(snowflake_id).unwrap()],
          900_000,
        )],
      )
    })
    .collect();
  let _ = write_shared_blob_segments(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    metadata_batches,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let batches = reader.read_available(950).await.unwrap();
  assert_eq!(batches.len(), usize::try_from(source_count).unwrap());
  let scan_state = reader.partition_scan_states();
  assert!(scan_state[0].metadata_sources_truncated);
  assert_eq!(
    scan_state[0].metadata_sources.len(),
    MAX_METADATA_SOURCE_DETAILS
  );
  assert_eq!(scan_state[0].metadata_sources[0].snowflake_id, 2);
  assert_eq!(
    scan_state[0]
      .metadata_sources
      .back()
      .expect("bounded source diagnostics are populated")
      .snowflake_id,
    source_count
  );
  assert_eq!(
    batches[0]
      .admission_scan
      .as_ref()
      .expect("accepted batch retains admission evidence")
      .metadata_sources[0]
      .snowflake_id,
    2
  );
}

#[tokio::test]
async fn visibility_delay_defers_newly_published_metadata() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 902_000)],
    Compression::none(),
    902_000,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let runtime_settings = reader.runtime_settings();
  // The exact eligibility time is 903.000 seconds. The hint rounds it to the first reader clock
  // second that cannot observe the row before the full one-second visibility delay has elapsed.
  let outcome = reader
    .read_available_with_capacity_and_settings(
      timestamp(902),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
      runtime_settings,
    )
    .await
    .unwrap();
  assert!(outcome.batches.is_empty());
  assert_eq!(
    outcome.next_visibility_eligible_at,
    Some(OffsetDateTime::from_unix_timestamp(903).unwrap())
  );
  let scan_state = reader.partition_scan_states();
  assert_eq!(scan_state.len(), 1);
  assert_eq!(scan_state[0].virtual_partition_id, 7);
  assert_eq!(scan_state[0].cursor_before, None);
  assert_eq!(scan_state[0].cursor_after, None);
  assert_eq!(scan_state[0].metadata_segments_seen, 1);
  assert_eq!(scan_state[0].metadata_segments_deferred_by_visibility, 1);
  assert!(!scan_state[0].metadata_sources_truncated);
  assert_eq!(scan_state[0].metadata_sources.len(), 1);
  assert_eq!(
    scan_state[0].metadata_sources[0].window_start_unix_seconds,
    900
  );
  assert_eq!(scan_state[0].metadata_sources[0].snowflake_id, 1);
  assert_eq!(
    scan_state[0].metadata_sources[0].metadata_published_at,
    OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(902_000)
  );
  assert_eq!(
    scan_state[0].metadata_sources[0].batch_ranges,
    vec![SeqRange { start: 1, end: 1 }]
  );
  assert_eq!(scan_state[0].batches_accepted, 0);
  let batches = reader.read_available(903).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 1, end: 1 });
  let admission_scan = batches[0]
    .admission_scan
    .as_ref()
    .expect("accepted batch must retain its finalized admission scan");
  assert_eq!(admission_scan.cursor_before, None);
  assert_eq!(admission_scan.cursor_after, Some(1));
  assert_eq!(admission_scan.metadata_sources.len(), 1);
  assert_eq!(admission_scan.metadata_sources[0].snowflake_id, 1);
  assert_eq!(
    admission_scan.metadata_sources[0].batch_ranges,
    vec![SeqRange { start: 1, end: 1 }]
  );
  let scan_state = reader.partition_scan_states();
  assert_eq!(scan_state[0].cursor_before, None);
  assert_eq!(scan_state[0].cursor_after, Some(1));
  assert_eq!(scan_state[0].batches_accepted, 1);
  assert_eq!(scan_state[0].records_accepted, 1);
}

#[tokio::test]
async fn strong_metadata_reads_accept_future_metadata_publication_timestamps() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 902_000)],
    Compression::none(),
    902_500,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let outcome = reader
    .read_available_with_capacity_and_settings(
      timestamp(902),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
      reader.runtime_settings(),
    )
    .await
    .unwrap();

  assert_eq!(outcome.batches.len(), 1);
  assert_eq!(outcome.next_visibility_eligible_at, None);
  assert_eq!(reader.cursor(7), Some(1));
  assert!(
    recording_metadata_store
      .consistencies
      .lock()
      .iter()
      .all(|consistency| *consistency == MetadataReadConsistency::Strong)
  );
  let scan_state = reader.partition_scan_states();
  assert_eq!(scan_state[0].metadata_segments_deferred_by_visibility, 0);
}

#[tokio::test]
async fn runtime_strong_metadata_reads_apply_on_the_next_scan() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));

  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 902_000)],
    Compression::none(),
    902_000,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();

  let eventual = reader
    .read_available_with_capacity_and_settings(
      timestamp(902),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert!(eventual.batches.is_empty());
  assert_eq!(reader.cursor(7), None);

  feature_flags.update(Arc::new(
    DefaultFeatureFlags::default()
      .with_bool_flag("blob_stream_consumer_strong_metadata_reads", true),
  ));
  let strong = reader
    .read_available_with_capacity_and_settings(
      timestamp(902),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
      reader.runtime_settings(),
    )
    .await
    .unwrap();

  assert_eq!(strong.batches.len(), 1);
  assert_eq!(strong.next_visibility_eligible_at, None);
  assert_eq!(reader.cursor(7), Some(1));
  assert!(
    recording_metadata_store
      .consistencies
      .lock()
      .iter()
      .rev()
      .take(2)
      .all(|consistency| *consistency == MetadataReadConsistency::Strong)
  );
}

#[test]
fn strong_metadata_reads_use_clock_skew_without_a_visibility_delay() {
  let reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(2_000).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let runtime_settings = reader.runtime_settings();
  let exact_tick = timestamp(1_000);
  let within_tick = exact_tick.saturating_add(TimeDuration::milliseconds(5));
  let exact_safe_timestamp = reader.fast_scan_safe_timestamp(exact_tick, runtime_settings);
  let within_tick_safe_timestamp = reader.fast_scan_safe_timestamp(within_tick, runtime_settings);

  assert_eq!(
    exact_safe_timestamp,
    timestamp(984).saturating_add(TimeDuration::milliseconds(990))
  );
  assert_eq!(
    ConsumerReaderImpl::snowflake_floor(exact_safe_timestamp),
    SnowflakeId::minimum_for_timestamp(exact_safe_timestamp)
  );
  assert_eq!(
    within_tick_safe_timestamp,
    timestamp(984).saturating_add(TimeDuration::milliseconds(995))
  );
  assert_eq!(
    ConsumerReaderImpl::snowflake_floor(within_tick_safe_timestamp),
    SnowflakeId::minimum_for_timestamp(exact_safe_timestamp)
  );
}

#[tokio::test]
async fn derived_horizon_retries_visibility_deferred_metadata_across_window_boundary() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 901_000)],
    Compression::none(),
    1_200_000,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(300_000).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    TimeDuration::seconds(30),
    None,
  )
  .unwrap();

  assert!(reader.read_available(1_200).await.unwrap().is_empty());
  let batches = reader.read_available(1_500).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 1, end: 1 });
}

#[tokio::test]
async fn retention_recovery_scans_from_checkpoint_before_fast_path() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();

  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    15_000,
    2,
    7,
    SeqRange { start: 5, end: 5 },
    vec![new_record(vec![5], 15_001_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    90_000,
    3,
    7,
    SeqRange { start: 6, end: 6 },
    vec![new_record(vec![6], 90_001_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 4,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 15_000,
        snowflake_id: 1,
      }),
    },
    Some(15_000_000),
    timestamp(90_000),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(90_000))
    .unwrap();

  let batches = reader.read_available(90_000).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 5, end: 5 });
  assert!(
    !metadata_store
      .scans
      .lock()
      .iter()
      .any(|(window_start, _)| *window_start == 90_000)
  );

  let VirtualPartitionState::Recovering { recovery_state, .. } =
    reader.virtual_partition_states.get(&7).unwrap()
  else {
    panic!("partition must remain in recovery before the cutover is scanned");
  };
  assert_eq!(recovery_state.cutover_window_start_unix_seconds, 90_000);
  assert_eq!(recovery_state.next_window_start_unix_seconds, 24_600);
}

#[tokio::test]
async fn recovery_scans_single_checkpoint_window_with_overlap_bound() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();
  let window_start = 1_700_000_100;
  let checkpoint_timestamp = OffsetDateTime::from_unix_timestamp(window_start + 120).unwrap();
  let checkpoint_snowflake = SnowflakeId::minimum_for_timestamp(checkpoint_timestamp);
  let expected_floor = SnowflakeId::minimum_for_timestamp(
    OffsetDateTime::from_unix_timestamp(window_start + 104)
      .unwrap()
      .saturating_add(time::Duration::milliseconds(990)),
  );

  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    window_start,
    checkpoint_snowflake.as_u64(),
    7,
    SeqRange { start: 10, end: 10 },
    vec![new_record(vec![10], window_start * 1_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    window_start,
    SnowflakeId::minimum_for_timestamp(checkpoint_timestamp + time::Duration::seconds(1)).as_u64(),
    7,
    SeqRange { start: 11, end: 11 },
    vec![new_record(vec![11], window_start * 1_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 10,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: window_start,
        snowflake_id: checkpoint_snowflake.as_u64(),
      }),
    },
    Some(window_start * 1_000),
    timestamp(window_start + 150),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(window_start))
    .unwrap();

  let batches = reader.read_available(window_start + 150).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 11, end: 11 });
  assert_eq!(
    *metadata_store.scans.lock(),
    vec![(window_start, Some(expected_floor))]
  );
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
}

#[test]
fn recovery_only_bounds_its_checkpoint_window() {
  let window_start = 1_700_000_100;
  let checkpoint_timestamp = OffsetDateTime::from_unix_timestamp(window_start + 120).unwrap();
  let checkpoint_snowflake = SnowflakeId::minimum_for_timestamp(checkpoint_timestamp);
  let expected_floor = SnowflakeId::minimum_for_timestamp(
    OffsetDateTime::from_unix_timestamp(window_start + 104)
      .unwrap()
      .saturating_add(time::Duration::milliseconds(990)),
  );
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 10,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: window_start,
        snowflake_id: checkpoint_snowflake.as_u64(),
      }),
    },
    Some(window_start * 1_000),
    timestamp(window_start + 300),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(window_start + 300))
    .unwrap();

  let (requests, recovery_scan) = reader
    .scan_requests(
      timestamp(window_start + 300),
      &[7],
      reader.runtime_settings(),
    )
    .unwrap();

  assert!(recovery_scan);
  assert_eq!(requests.len(), 2);
  assert_eq!(requests[0].window.window_start_unix_seconds, window_start);
  assert_eq!(requests[0].min_snowflake, Some(expected_floor));
  assert_eq!(
    requests[1].window.window_start_unix_seconds,
    window_start + 300
  );
  assert_eq!(requests[1].min_snowflake, None);
}

#[tokio::test]
async fn broker_recovery_uses_tail_for_checkpoint_and_full_recovery_afterward() {
  let window_start = 1_700_000_100;
  let checkpoint_timestamp = OffsetDateTime::from_unix_timestamp(window_start + 120).unwrap();
  let checkpoint_snowflake = SnowflakeId::minimum_for_timestamp(checkpoint_timestamp);
  let expected_floor = SnowflakeId::minimum_for_timestamp(
    OffsetDateTime::from_unix_timestamp(window_start + 104)
      .unwrap()
      .saturating_add(time::Duration::milliseconds(990)),
  );
  let broker_query = Arc::new(RecordingBrokerMetadataQuery {
    requests: Mutex::new(Vec::new()),
  });
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_bool_flag("blob_stream_consumer_strong_metadata_reads", true),
  ));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    broker_query.clone(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 10,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: window_start,
        snowflake_id: checkpoint_snowflake.as_u64(),
      }),
    },
    Some(window_start * 1_000),
    timestamp(window_start + 300),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(window_start + 300))
    .unwrap();

  assert!(
    reader
      .read_available(window_start + 300)
      .await
      .unwrap()
      .is_empty()
  );
  let requests = broker_query.requests();
  let checkpoint_request = requests
    .iter()
    .find(|request| request.window_start_unix_seconds == window_start)
    .expect("broker receives the checkpoint recovery window");
  let Some(read_metadata_window_request::Coverage::Tail(tail)) =
    checkpoint_request.coverage.as_ref()
  else {
    panic!("checkpoint recovery uses Tail coverage");
  };
  assert_eq!(
    tail
      .partition_bounds
      .iter()
      .map(|bound| (bound.virtual_partition_id, bound.min_snowflake))
      .collect::<HashMap<_, _>>(),
    HashMap::from([(7, expected_floor.as_u64())])
  );
  let recovery_request = requests
    .iter()
    .find(|request| request.window_start_unix_seconds == window_start + 300)
    .expect("broker receives the unbounded recovery window");
  let Some(read_metadata_window_request::Coverage::FullRecovery(recovery)) =
    recovery_request.coverage.as_ref()
  else {
    panic!("unbounded recovery uses Full Recovery coverage");
  };
  assert_eq!(recovery.virtual_partition_ids, vec![7]);
}

#[tokio::test]
async fn broker_recovery_delivery_preserves_batches_and_cursor() {
  let window_start = 1_700_000_100;
  let checkpoint_timestamp = OffsetDateTime::from_unix_timestamp(window_start + 120).unwrap();
  let checkpoint_snowflake = SnowflakeId::minimum_for_timestamp(checkpoint_timestamp);
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();
  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    window_start,
    checkpoint_snowflake.as_u64(),
    7,
    SeqRange { start: 11, end: 11 },
    vec![new_record(vec![11], window_start * 1_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    window_start + 300,
    SnowflakeId::minimum_for_timestamp(timestamp(window_start + 301)).as_u64(),
    7,
    SeqRange { start: 12, end: 12 },
    vec![new_record(vec![12], (window_start + 300) * 1_000)],
    Compression::none(),
  )
  .await;
  let checkpoint_window = TopicWindowKey {
    topic: "telemetry".to_string(),
    window_start_unix_seconds: window_start,
  };
  let recovery_window = TopicWindowKey {
    topic: "telemetry".to_string(),
    window_start_unix_seconds: window_start + 300,
  };
  let checkpoint_segments = metadata_store
    .scan_window_from_snowflake(&checkpoint_window, None, MetadataReadConsistency::Strong)
    .await
    .unwrap();
  let recovery_segments = metadata_store
    .scan_window_from_snowflake(&recovery_window, None, MetadataReadConsistency::Strong)
    .await
    .unwrap();
  metadata_store.scans.lock().clear();
  let mut recovery_response =
    broker_metadata_response(&recovery_segments, timestamp(window_start + 300));
  if let Some(read_metadata_window_response::Result::Success(success)) =
    recovery_response.result.as_mut()
  {
    success.refill_floor = None;
  }
  let broker_query = Arc::new(FixedBrokerMetadataQuery {
    responses: HashMap::from([
      (
        window_start,
        broker_metadata_response(&checkpoint_segments, timestamp(window_start + 300)),
      ),
      (window_start + 300, recovery_response),
    ]),
  });
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_bool_flag("blob_stream_consumer_strong_metadata_reads", true),
  ));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
    broker_query,
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 10,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: window_start,
        snowflake_id: checkpoint_snowflake.as_u64(),
      }),
    },
    Some(window_start * 1_000),
    timestamp(window_start + 300),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(window_start + 300))
    .unwrap();

  let batches = reader.read_available(window_start + 300).await.unwrap();
  assert_eq!(
    batches
      .iter()
      .map(|batch| batch.seq_range.end)
      .collect::<Vec<_>>(),
    vec![11, 12]
  );
  assert_eq!(reader.cursor(7), Some(12));
  assert!(metadata_store.scans.lock().is_empty());
}

#[tokio::test]
async fn assignment_activates_hydrated_state_and_removes_revoked_state() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 4,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 15_000,
        snowflake_id: 1,
      }),
    },
    Some(15_000_000),
    timestamp(90_000),
  );

  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::PendingRecovering { .. })
  ));
  assert_eq!(reader.cursor(7), Some(4));

  reader
    .set_assigned_virtual_partitions(&[7], timestamp(90_000))
    .unwrap();
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Recovering { .. })
  ));

  reader
    .set_assigned_virtual_partitions(&[], timestamp(90_000))
    .unwrap();
  assert!(!reader.virtual_partition_states.contains_key(&7));
  assert_eq!(reader.cursor(7), None);
}

#[tokio::test]
async fn retention_recovery_clamps_legacy_cursor_to_retention_floor() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 4,
      source_checkpoint: None,
    },
    Some(1_000),
    timestamp(90_000),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(90_000))
    .unwrap();

  assert!(reader.read_available(90_000).await.unwrap().is_empty());
  let scans = metadata_store.scans.lock();
  assert_eq!(scans.first(), Some(&(3_600, None)));
  assert_eq!(scans.len(), 32);
}

#[test]
fn fast_coverage_recovery_clamps_a_stale_floor_to_retention() {
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.virtual_partition_states.insert(
    7,
    VirtualPartitionState::Fast {
      cursor: Some(4),
      coverage_floor: Some(timestamp(1_000)),
      last_scan: None,
    },
  );

  let (requests, recovery_scan) = reader
    .scan_requests(timestamp(90_000), &[7], reader.runtime_settings())
    .unwrap();

  assert!(recovery_scan);
  assert_eq!(requests[0].window.window_start_unix_seconds, 3_600);
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Recovering { recovery_state, .. })
      if recovery_state.next_window_start_unix_seconds == 3_600
  ));
}

#[test]
fn fast_coverage_tail_stays_on_the_fast_path_at_window_rollover() {
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    TimeDuration::seconds(15),
    None,
  )
  .unwrap()
  .metadata_window_size(TimeDuration::seconds(300))
  .maximum_clock_skew(TimeDuration::ZERO);
  reader.virtual_partition_states.insert(
    7,
    VirtualPartitionState::Fast {
      cursor: Some(4),
      coverage_floor: Some(timestamp(1_199)),
      last_scan: None,
    },
  );

  let (requests, recovery_scan) = reader
    .scan_requests(timestamp(1_215), &[7], reader.runtime_settings())
    .unwrap();

  assert!(!recovery_scan);
  assert_eq!(
    requests
      .iter()
      .map(|request| request.window.window_start_unix_seconds)
      .collect::<Vec<_>>(),
    vec![900, 1_200]
  );
  assert_eq!(
    requests[0].fast_partition_bounds.get(&7),
    Some(&SnowflakeId::minimum_for_timestamp(timestamp(1_199)))
  );
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
}

#[test]
fn fast_coverage_tail_clamps_a_stale_floor_to_the_retained_window() {
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::seconds(200),
    TimeDuration::seconds(15),
    None,
  )
  .unwrap()
  .metadata_window_size(TimeDuration::seconds(300))
  .maximum_clock_skew(TimeDuration::ZERO);
  reader.virtual_partition_states.insert(
    7,
    VirtualPartitionState::Fast {
      cursor: Some(4),
      coverage_floor: Some(timestamp(600)),
      last_scan: None,
    },
  );

  let (requests, recovery_scan) = reader
    .scan_requests(timestamp(1_320), &[7], reader.runtime_settings())
    .unwrap();

  assert!(!recovery_scan);
  assert_eq!(
    requests
      .iter()
      .map(|request| request.window.window_start_unix_seconds)
      .collect::<Vec<_>>(),
    vec![900, 1_200]
  );
  assert_eq!(
    requests[0].fast_partition_bounds.get(&7),
    Some(&SnowflakeId::minimum_for_timestamp(timestamp(900)))
  );
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
}

#[tokio::test]
async fn fast_scan_bound_diagnostics_exclude_partitions_outside_a_targeted_tail() {
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    TimeDuration::seconds(15),
    None,
  )
  .unwrap()
  .metadata_window_size(TimeDuration::seconds(300))
  .maximum_clock_skew(TimeDuration::ZERO);
  reader.virtual_partition_states.insert(
    7,
    VirtualPartitionState::Fast {
      cursor: Some(4),
      coverage_floor: Some(timestamp(1_199)),
      last_scan: None,
    },
  );

  assert!(reader.read_available(1_215).await.unwrap().is_empty());
  let scan_states = reader.partition_scan_states();
  let tail_partition = scan_states
    .iter()
    .find(|state| state.virtual_partition_id == 7)
    .unwrap();
  let current_only_partition = scan_states
    .iter()
    .find(|state| state.virtual_partition_id == 8)
    .unwrap();
  assert_eq!(
    tail_partition
      .fast_scan_bounds
      .iter()
      .map(|bound| bound.window_start_unix_seconds)
      .collect::<Vec<_>>(),
    vec![900, 1_200]
  );
  assert_eq!(
    current_only_partition
      .fast_scan_bounds
      .iter()
      .map(|bound| bound.window_start_unix_seconds)
      .collect::<Vec<_>>(),
    vec![1_200]
  );
}

#[tokio::test]
async fn retention_recovery_crosses_multiple_scan_slices_before_fast_path() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let window_size_seconds = 300;
  let cutover_window_start = 24_000;

  for (window_offset, snowflake_id, sequence) in [(31, 2, 1), (32, 3, 2), (64, 4, 3), (80, 5, 4)] {
    let window_start = i64::from(window_offset) * window_size_seconds;
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      window_start,
      snowflake_id,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        window_start * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 0,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 1,
      }),
    },
    Some(0),
    timestamp(cutover_window_start),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(cutover_window_start))
    .unwrap();

  let first_slice = reader.read_available(cutover_window_start).await.unwrap();
  assert_eq!(first_slice.len(), 1);
  assert_eq!(first_slice[0].seq_range.end, 1);
  let second_slice = reader.read_available(cutover_window_start).await.unwrap();
  assert_eq!(second_slice.len(), 1);
  assert_eq!(second_slice[0].seq_range.end, 2);
  let final_slice = reader.read_available(cutover_window_start).await.unwrap();
  let recovered_sequences = first_slice
    .into_iter()
    .chain(second_slice)
    .chain(final_slice)
    .map(|batch| batch.seq_range.end)
    .collect::<Vec<_>>();
  assert_eq!(recovered_sequences, vec![1, 2, 3, 4]);
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
}

#[tokio::test]
async fn recovery_waits_for_visibility_deferred_window_before_advancing() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let cutover_window_start = 12_000;
  let deferred_window_start = 9_600;

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    9_300,
    2,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 9_300_000)],
    Compression::none(),
  )
  .await;
  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    deferred_window_start,
    3,
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![2], 9_600_000)],
    Compression::none(),
    cutover_window_start * 1_000,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 0,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 1,
      }),
    },
    Some(0),
    timestamp(cutover_window_start),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(cutover_window_start))
    .unwrap();

  assert_eq!(
    reader
      .read_available(cutover_window_start)
      .await
      .unwrap()
      .len(),
    1
  );
  assert!(
    reader
      .read_available(cutover_window_start)
      .await
      .unwrap()
      .is_empty()
  );
  let VirtualPartitionState::Recovering { recovery_state, .. } =
    reader.virtual_partition_states.get(&7).unwrap()
  else {
    panic!("visibility-deferred recovery window must remain pending");
  };
  assert_eq!(
    recovery_state.next_window_start_unix_seconds,
    deferred_window_start
  );
  assert!(
    reader
      .recovery_metadata_cache
      .keys()
      .all(|(partition_id, window_start, _)| {
        *partition_id != 7 || *window_start != deferred_window_start
      }),
    "visibility-deferred recovery metadata must be queried again after it becomes eligible"
  );

  let batches = reader
    .read_available(cutover_window_start + 1)
    .await
    .unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 2, end: 2 });
}

#[tokio::test]
async fn recovery_does_not_advance_cursor_past_visibility_deferred_window() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let first_recovery_window = 300;
  let later_recovery_window = 600;
  let cutover_window = 900;

  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    first_recovery_window,
    2,
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![2], first_recovery_window * 1_000)],
    Compression::none(),
    cutover_window * 1_000,
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    later_recovery_window,
    3,
    7,
    SeqRange { start: 3, end: 3 },
    vec![new_record(vec![3], later_recovery_window * 1_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 1,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: first_recovery_window,
        snowflake_id: 1,
      }),
    },
    Some(first_recovery_window * 1_000),
    timestamp(cutover_window),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(cutover_window))
    .unwrap();

  assert!(
    reader
      .read_available(cutover_window)
      .await
      .unwrap()
      .is_empty()
  );
  assert_eq!(reader.cursor(7), Some(1));

  let batches = reader.read_available(cutover_window + 1).await.unwrap();
  assert_eq!(
    batches
      .iter()
      .map(|batch| batch.seq_range.clone())
      .collect::<Vec<_>>(),
    vec![SeqRange { start: 2, end: 2 }, SeqRange { start: 3, end: 3 }]
  );
}

#[tokio::test]
async fn recovery_hands_active_window_visibility_deferral_to_fast() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let window_start = 900;
  let snowflake_id =
    SnowflakeId::minimum_for_timestamp(OffsetDateTime::from_unix_timestamp(window_start).unwrap());

  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    window_start,
    snowflake_id.as_u64(),
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![2], window_start * 1_000)],
    Compression::none(),
    window_start * 1_000,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 1,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: window_start,
        snowflake_id: snowflake_id.as_u64(),
      }),
    },
    Some(window_start * 1_000),
    timestamp(window_start),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(window_start))
    .unwrap();

  assert!(
    reader
      .read_available(window_start)
      .await
      .unwrap()
      .is_empty()
  );
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
  assert_eq!(reader.cursor(7), Some(1));

  let batches = reader.read_available(window_start + 1).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 2, end: 2 });
}

#[tokio::test]
async fn advances_cursor_and_dedupes_on_rescan() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 2 },
    vec![new_record(vec![1], 1000), new_record(vec![2], 1001)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let first = reader.read_available(950).await.unwrap();
  assert_eq!(first.len(), 1);
  assert_eq!(first[0].seq_range, SeqRange { start: 1, end: 2 });
  assert_eq!(reader.cursor(7), Some(2));

  let second = reader.read_available(950).await.unwrap();
  assert!(second.is_empty());
  assert_eq!(reader.cursor(7), Some(2));
}

#[tokio::test]
async fn failed_scan_restores_cursor_before_retrying_undelivered_batches() {
  let metrics = Helper::new();
  let metrics_scope = metrics
    .collector()
    .scope("blob_stream_consumer_failed_fallback_read_test");
  let blob_store: Arc<dyn BlobStore> = Arc::new(FailSecondRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  for sequence in 1 ..= 2 {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      900,
      sequence,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        1_000 + i64::try_from(sequence).unwrap(),
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope,
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert!(reader.read_available(950).await.is_err());
  assert_eq!(reader.cursor(7), None);
  metrics.assert_counter_eq(
    2,
    "blob_stream_consumer_failed_fallback_read_test:reader:fallback_blob_range_requests",
    &labels!(),
  );

  let batches = reader.read_available(950).await.unwrap();
  assert_eq!(
    batches
      .iter()
      .map(|batch| batch.seq_range.clone())
      .collect::<Vec<_>>(),
    vec![SeqRange { start: 1, end: 1 }, SeqRange { start: 2, end: 2 }]
  );
  assert_eq!(reader.cursor(7), Some(2));
}

#[tokio::test]
async fn missing_blob_range_is_counted_and_skipped() {
  let blob_store = Arc::new(MissingRangeBlobStore::new());
  let blob_store_dyn: Arc<dyn BlobStore> = blob_store.clone();
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  for sequence in 1 ..= 2 {
    write_segment(
      blob_store_dyn.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      900,
      sequence,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        1_000 + i64::try_from(sequence).unwrap(),
      )],
      Compression::none(),
    )
    .await;
  }
  blob_store.mark_missing(BlobKey::new("telemetry/900/1.bin"));

  let collector = Collector::default();
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store_dyn,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &collector.scope("blob_stream_consumer_test"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let batches = reader.read_available(950).await.unwrap();

  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 2, end: 2 });
  assert_eq!(reader.cursor(7), Some(2));
  let metrics = String::from_utf8(collector.prometheus_output()).unwrap();
  assert!(
    metrics.contains("blob_stream_consumer_test:reader:lost_records 1"),
    "{metrics}"
  );
}

#[tokio::test]
async fn metadata_scan_error_preserves_aws_source_chain() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(FailingMetadataStore);
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let Err(error) = reader.read_available(950).await else {
    panic!("metadata scan should fail");
  };
  let error_chain = format!("{error:#}");
  assert!(error_chain.contains("consumer metadata scan failed: topic=telemetry"));
  assert!(error_chain.contains("injected DynamoDB service error"));
  assert!(error_chain.contains("injected DynamoDB dispatch error"));
}

#[tokio::test]
async fn byte_capacity_defers_later_batches_until_the_next_scan() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  for sequence in 1 ..= 2 {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      900,
      sequence,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        1_000 + i64::try_from(sequence).unwrap(),
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let first_settings = reader.runtime_settings();
  let first = reader
    .read_available_with_capacity_and_settings(timestamp(950), ReadCapacity::new(1), first_settings)
    .await
    .unwrap();
  assert_eq!(first.batches.len(), 1);
  assert_eq!(first.batches[0].seq_range, SeqRange { start: 1, end: 1 });
  assert_eq!(reader.cursor(7), Some(1));

  let second_settings = reader.runtime_settings();
  let second = reader
    .read_available_with_capacity_and_settings(
      timestamp(950),
      ReadCapacity::new(1),
      second_settings,
    )
    .await
    .unwrap();
  assert_eq!(second.batches.len(), 1);
  assert_eq!(second.batches[0].seq_range, SeqRange { start: 2, end: 2 });
  assert_eq!(reader.cursor(7), Some(2));
}

#[tokio::test]
async fn recovery_capacity_resumes_at_the_first_deferred_window() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();
  let window_size_seconds = 300;

  for sequence in 1_u64 ..= 3 {
    let window_start = i64::try_from(sequence - 1).unwrap() * window_size_seconds;
    write_segment(
      blob_store.as_ref(),
      metadata_store_dyn.as_ref(),
      "telemetry",
      window_start,
      sequence,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        window_start * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 0,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 0,
      }),
    },
    Some(0),
    timestamp(600),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(600))
    .unwrap();

  let first = reader
    .read_available_with_capacity_and_settings(
      timestamp(600),
      ReadCapacity::new(1),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert_eq!(first.batches[0].seq_range, SeqRange { start: 1, end: 1 });
  let VirtualPartitionState::Recovering { recovery_state, .. } =
    reader.virtual_partition_states.get(&7).unwrap()
  else {
    panic!("capacity-deferred recovery must remain active");
  };
  assert_eq!(
    recovery_state.next_window_start_unix_seconds,
    window_size_seconds
  );

  metadata_store.scans.lock().clear();
  let second = reader
    .read_available_with_capacity_and_settings(
      timestamp(600),
      ReadCapacity::new(1),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert_eq!(second.batches[0].seq_range, SeqRange { start: 2, end: 2 });
  assert!(
    metadata_store
      .scans
      .lock()
      .iter()
      .all(|(window_start, _)| *window_start >= window_size_seconds),
    "recovery must not rescan windows before its capacity boundary"
  );
}

#[tokio::test]
async fn recovery_capacity_deferral_keeps_unprocessed_cutover_partitions_recovering() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let window_start = 900;

  for (partition_id, snowflake_id) in [(7, 1), (8, 2)] {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      window_start,
      snowflake_id,
      partition_id,
      SeqRange { start: 1, end: 1 },
      vec![new_record(
        vec![u8::try_from(partition_id).unwrap()],
        window_start * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  for partition_id in [7, 8] {
    reader.hydrate_cursor_with_source(
      partition_id,
      &CommittedCursor {
        virtual_partition_id: partition_id,
        seq_end: 0,
        source_checkpoint: Some(CommittedSourceCheckpoint {
          window_start_unix_seconds: window_start,
          snowflake_id: 0,
        }),
      },
      Some(window_start * 1_000),
      timestamp(window_start),
    );
  }
  reader
    .set_assigned_virtual_partitions(&[7, 8], timestamp(window_start))
    .unwrap();

  let first = reader
    .read_available_with_capacity_and_settings(
      timestamp(window_start),
      ReadCapacity::new(0),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert!(first.batches.is_empty());
  for partition_id in [7, 8] {
    assert!(matches!(
      reader.virtual_partition_states.get(&partition_id),
      Some(VirtualPartitionState::Recovering { recovery_state, .. })
        if recovery_state.next_window_start_unix_seconds == window_start
    ));
  }

  let resumed = reader
    .read_available_with_capacity_and_settings(
      timestamp(window_start),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert_eq!(
    resumed
      .batches
      .iter()
      .map(|batch| batch.virtual_partition_id)
      .collect::<Vec<_>>(),
    vec![7, 8]
  );
}

#[tokio::test]
async fn fresh_capacity_deferral_retries_the_initial_window_before_fast_path() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let window_start = 900;

  for (partition_id, snowflake_id) in [(7, 1), (8, 2)] {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      window_start,
      snowflake_id,
      partition_id,
      SeqRange { start: 1, end: 1 },
      vec![new_record(
        vec![u8::try_from(partition_id).unwrap()],
        window_start * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader
    .set_assigned_virtual_partitions(&[7, 8], timestamp(window_start))
    .unwrap();

  let first = reader
    .read_available_with_capacity_and_settings(
      timestamp(window_start),
      ReadCapacity::new(0),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert!(first.batches.is_empty());
  for partition_id in [7, 8] {
    assert!(matches!(
      reader.virtual_partition_states.get(&partition_id),
      Some(VirtualPartitionState::Fresh { .. })
    ));
  }

  let resumed = reader
    .read_available_with_capacity_and_settings(
      timestamp(window_start),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert_eq!(
    resumed
      .batches
      .iter()
      .map(|batch| batch.virtual_partition_id)
      .collect::<Vec<_>>(),
    vec![7, 8]
  );
}

#[tokio::test]
async fn mature_recovery_metadata_is_scanned_once_across_capacity_cycles() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();

  for sequence in 1_u64 ..= 3 {
    write_segment(
      blob_store.as_ref(),
      metadata_store_dyn.as_ref(),
      "telemetry",
      0,
      sequence,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        i64::try_from(sequence).unwrap() * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 0,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 0,
      }),
    },
    Some(0),
    timestamp(1_200),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(1_200))
    .unwrap();

  for expected_sequence in 1_u64 ..= 3 {
    let batches = reader
      .read_available_with_capacity_and_settings(
        timestamp(1_200),
        ReadCapacity::new(1),
        reader.runtime_settings(),
      )
      .await
      .unwrap();
    assert_eq!(batches.batches.len(), 1);
    assert_eq!(
      batches.batches[0].seq_range,
      SeqRange {
        start: expected_sequence,
        end: expected_sequence,
      }
    );
    assert_eq!(
      metadata_store
        .scans
        .lock()
        .iter()
        .filter(|(window_start, _)| *window_start == 0)
        .count(),
      1,
      "a mature recovery window must remain cached until every unread batch is consumed"
    );
  }

  assert!(
    reader
      .recovery_metadata_cache
      .keys()
      .all(|(partition_id, ..)| *partition_id != 7),
    "completed recovery must release cached metadata"
  );
}

#[tokio::test]
async fn mature_recovery_metadata_caches_after_visibility_deferral() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();
  let window_start = 900;
  let first_scan_time = 2_100;

  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    window_start,
    1,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], window_start * 1_000)],
    Compression::none(),
    window_start * 1_000,
  )
  .await;
  for sequence in 2_u64 ..= 3 {
    write_segment_with_publication_time(
      blob_store.as_ref(),
      metadata_store_dyn.as_ref(),
      "telemetry",
      window_start,
      sequence,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        window_start * 1_000,
      )],
      Compression::none(),
      first_scan_time * 1_000,
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 0,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: window_start,
        snowflake_id: 0,
      }),
    },
    Some(window_start * 1_000),
    timestamp(first_scan_time),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(first_scan_time))
    .unwrap();

  let first = reader
    .read_available_with_capacity_and_settings(
      timestamp(first_scan_time),
      ReadCapacity::new(1),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert_eq!(first.batches[0].seq_range, SeqRange { start: 1, end: 1 });
  assert!(
    reader
      .recovery_metadata_cache
      .keys()
      .all(|(partition_id, cached_window_start, _)| {
        *partition_id != 7 || *cached_window_start != window_start
      }),
    "visibility-deferred metadata must not be cached"
  );

  let visible_time = first_scan_time + 2;
  let second = reader
    .read_available_with_capacity_and_settings(
      timestamp(visible_time),
      ReadCapacity::new(1),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert_eq!(second.batches[0].seq_range, SeqRange { start: 2, end: 2 });

  let third = reader
    .read_available_with_capacity_and_settings(
      timestamp(visible_time),
      ReadCapacity::new(1),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert_eq!(third.batches[0].seq_range, SeqRange { start: 3, end: 3 });
  assert_eq!(
    metadata_store
      .scans
      .lock()
      .iter()
      .filter(|(scanned_window, _)| *scanned_window == window_start)
      .count(),
    2,
    "the visible retry must populate the cache for later capacity cycles"
  );
}

#[tokio::test]
async fn mature_recovery_metadata_survives_blob_read_failure() {
  let blob_store = Arc::new(FailSecondRangeBlobStore::new());
  let blob_store_dyn: Arc<dyn BlobStore> = blob_store.clone();
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();

  for sequence in 1_u64 ..= 2 {
    write_segment(
      blob_store_dyn.as_ref(),
      metadata_store_dyn.as_ref(),
      "telemetry",
      0,
      sequence,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        i64::try_from(sequence).unwrap() * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store_dyn,
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 0,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 0,
      }),
    },
    Some(0),
    timestamp(1_200),
  );
  reader
    .set_assigned_virtual_partitions(&[7], timestamp(1_200))
    .unwrap();

  assert!(
    reader
      .read_available_with_capacity_and_settings(
        timestamp(1_200),
        ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
        reader.runtime_settings(),
      )
      .await
      .is_err()
  );
  assert_eq!(
    metadata_store
      .scans
      .lock()
      .iter()
      .filter(|(window_start, _)| *window_start == 0)
      .count(),
    1
  );

  let retry = reader
    .read_available_with_capacity_and_settings(
      timestamp(1_200),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert_eq!(
    retry
      .batches
      .iter()
      .map(|batch| batch.seq_range.end)
      .collect::<Vec<_>>(),
    vec![1, 2]
  );
  assert_eq!(
    metadata_store
      .scans
      .lock()
      .iter()
      .filter(|(window_start, _)| *window_start == 0)
      .count(),
    1,
    "blob-read retries must reuse mature cached metadata"
  );
}

#[test]
fn recovery_metadata_cache_is_invalidated_by_lifecycle_resets() {
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  let cache_key = (7, 0, None);

  reader
    .recovery_metadata_cache
    .insert(cache_key, Arc::new([]));
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 0,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 0,
      }),
    },
    Some(0),
    timestamp(1_200),
  );
  assert!(reader.recovery_metadata_cache.is_empty());

  reader
    .recovery_metadata_cache
    .insert(cache_key, Arc::new([]));
  reader.set_cursor(7, 0);
  assert!(reader.recovery_metadata_cache.is_empty());

  reader
    .recovery_metadata_cache
    .insert(cache_key, Arc::new([]));
  reader
    .set_assigned_virtual_partitions(&[], timestamp(1_200))
    .unwrap();
  assert!(reader.recovery_metadata_cache.is_empty());
}

#[test]
fn recovery_planning_rotates_between_partitions() {
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  for (partition_id, window_start) in [(7, 0), (8, 9_000)] {
    reader.hydrate_cursor_with_source(
      partition_id,
      &CommittedCursor {
        virtual_partition_id: partition_id,
        seq_end: 0,
        source_checkpoint: Some(CommittedSourceCheckpoint {
          window_start_unix_seconds: window_start,
          snowflake_id: 0,
        }),
      },
      Some(window_start * 1_000),
      timestamp(9_000),
    );
  }
  reader
    .set_assigned_virtual_partitions(&[7, 8], timestamp(9_000))
    .unwrap();

  let (first_requests, _) = reader
    .scan_requests(timestamp(9_000), &[7, 8], reader.runtime_settings())
    .unwrap();
  assert!(
    first_requests
      .iter()
      .all(|request| request.eligibility.recovering_partitions == vec![7])
  );
  let (second_requests, _) = reader
    .scan_requests(timestamp(9_000), &[7, 8], reader.runtime_settings())
    .unwrap();
  assert!(
    second_requests
      .iter()
      .all(|request| request.eligibility.recovering_partitions == vec![8])
  );
}

#[tokio::test]
async fn coalesces_owned_ranges_from_one_segment() {
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let payload_len = write_multi_partition_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    Compression::zstd(3),
    vec![
      (
        7,
        SeqRange { start: 1, end: 2 },
        vec![new_record(vec![7], 1_001), new_record(vec![17], 1_004)],
      ),
      (
        7,
        SeqRange { start: 3, end: 3 },
        vec![new_record(vec![27], 1_005)],
      ),
      (
        8,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![8], 1_002)],
      ),
      (
        9,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![9], 1_003)],
      ),
    ],
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7, 9],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let batches = reader.read_available(950).await.unwrap();

  assert_eq!(
    batches
      .iter()
      .map(|batch| batch.virtual_partition_id)
      .collect::<Vec<_>>(),
    vec![7, 7, 9]
  );
  assert_eq!(batches[0].seq_range, SeqRange { start: 1, end: 2 });
  assert_eq!(
    batches[0]
      .records
      .iter()
      .map(|record| record.payload.as_ref())
      .collect::<Vec<_>>(),
    vec![&[7], &[17]]
  );
  assert!(Arc::ptr_eq(
    batches[0]
      .admission_scan
      .as_ref()
      .expect("first batch has admission evidence"),
    batches[1]
      .admission_scan
      .as_ref()
      .expect("second batch has admission evidence")
  ));
  assert_eq!(
    blob_store.ranges(),
    vec![ByteRange {
      start: 0,
      end: payload_len,
    }]
  );
}

#[tokio::test]
async fn broker_blob_cache_reads_ranges_without_object_store_access() {
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let (_, payloads) = write_shared_blob_segments(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    vec![(
      1,
      7,
      SeqRange { start: 1, end: 1 },
      vec![new_record(vec![7], 900_000)],
    )],
  )
  .await;
  let query = Arc::new(FixedBrokerBlobRangeQuery::new(broker_blob_success(
    payloads,
  )));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    query.clone(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  let batches = reader.read_available(950).await.unwrap();

  assert_eq!(batches.len(), 1);
  assert_eq!(query.requests().len(), 1);
  assert!(blob_store.ranges().is_empty());
}

#[tokio::test]
async fn broker_blob_cache_groups_same_key_plans_and_decodes_validated_ranges() {
  let metrics = Helper::new();
  let metrics_scope = metrics
    .collector()
    .scope("blob_stream_consumer_broker_blob_delivery_test");
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let (blob_key, payloads) = write_shared_blob_segments(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    vec![
      (
        1,
        7,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![7], 900_000)],
      ),
      (
        2,
        8,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![8], 900_001)],
      ),
    ],
  )
  .await;
  let first_len = u64::try_from(payloads[0].len()).unwrap();
  let second_len = u64::try_from(payloads[1].len()).unwrap();
  let query = Arc::new(FixedBrokerBlobRangeQuery::new(broker_blob_success(
    payloads,
  )));
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    query.clone(),
    &metrics_scope,
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  let batches = reader.read_available(950).await.unwrap();

  assert_eq!(
    batches
      .iter()
      .map(|batch| batch.virtual_partition_id)
      .collect::<Vec<_>>(),
    vec![7, 8]
  );
  assert!(blob_store.ranges().is_empty());
  assert_eq!(
    query.requests(),
    vec![ReadBlobRangesRequest {
      blob_key: blob_key.as_str().to_string().into(),
      ranges: vec![
        BlobRangeRequest {
          start: 0,
          end: first_len,
          ..Default::default()
        },
        BlobRangeRequest {
          start: first_len,
          end: first_len.saturating_add(second_len),
          ..Default::default()
        },
      ],
      ..Default::default()
    }]
  );
  let metric = "blob_stream_consumer_broker_blob_delivery_test:reader";
  metrics.assert_counter_eq(
    1,
    &format!("{metric}:broker_blob_range_requests"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    &format!("{metric}:broker_blob_range_successes"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    first_len.saturating_add(second_len),
    &format!("{metric}:broker_blob_range_bytes"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    0,
    &format!("{metric}:broker_blob_range_fallbacks"),
    &labels!(),
  );
  metrics.assert_histogram_count(
    1,
    &format!("{metric}:broker_blob_range_latency_seconds"),
    &labels!(),
  );
}

#[tokio::test]
async fn broker_blob_cache_handles_many_ranges_from_one_blob_key() {
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let batches = (1 ..= 16)
    .map(|partition_id| {
      (
        u64::from(partition_id),
        partition_id,
        SeqRange { start: 1, end: 1 },
        vec![new_record(
          vec![u8::try_from(partition_id).unwrap()],
          900_000,
        )],
      )
    })
    .collect();
  let (blob_key, payloads) = write_shared_blob_segments(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    batches,
  )
  .await;
  let query = Arc::new(FixedBrokerBlobRangeQuery::new(broker_blob_success(
    payloads,
  )));
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    (1 ..= 16).collect(),
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    query.clone(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  let batches = reader.read_available(950).await.unwrap();

  assert_eq!(batches.len(), 16);
  assert!(blob_store.ranges().is_empty());
  let requests = query.requests();
  assert_eq!(requests.len(), 1);
  assert_eq!(requests[0].blob_key.as_str(), blob_key.as_str());
  assert_eq!(requests[0].ranges.len(), 16);
}

#[tokio::test]
async fn corrupt_broker_blob_payload_retries_the_complete_group_directly() {
  let metrics = Helper::new();
  let metrics_scope = metrics
    .collector()
    .scope("blob_stream_consumer_broker_blob_fallback_test");
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let (_, payloads) = write_shared_blob_segments(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    vec![
      (
        1,
        7,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![7], 900_000)],
      ),
      (
        2,
        8,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![8], 900_001)],
      ),
    ],
  )
  .await;
  let fallback_bytes = payloads.iter().fold(0_u64, |total, payload| {
    total.saturating_add(u64::try_from(payload.len()).unwrap())
  });
  let query = Arc::new(FixedBrokerBlobRangeQuery::new(broker_blob_success(vec![
    Bytes::from(vec![0; payloads[0].len()]),
    payloads[1].clone(),
  ])));
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    query.clone(),
    &metrics_scope,
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  let batches = reader.read_available(950).await.unwrap();

  assert_eq!(batches.len(), 2);
  assert_eq!(query.requests().len(), 1);
  assert_eq!(blob_store.ranges().len(), 2);
  let metric = "blob_stream_consumer_broker_blob_fallback_test:reader";
  metrics.assert_counter_eq(
    1,
    &format!("{metric}:broker_blob_range_requests"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    0,
    &format!("{metric}:broker_blob_range_successes"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    &format!("{metric}:broker_blob_range_fallbacks"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    2,
    &format!("{metric}:fallback_blob_range_requests"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    fallback_bytes,
    &format!("{metric}:fallback_blob_range_bytes"),
    &labels!(),
  );
}

#[tokio::test]
async fn overloaded_broker_blob_response_retries_the_complete_group_directly() {
  let metrics = Helper::new();
  let metrics_scope = metrics
    .collector()
    .scope("blob_stream_consumer_broker_blob_overload_test");
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let (_, payloads) = write_shared_blob_segments(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    vec![
      (
        1,
        7,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![7], 900_000)],
      ),
      (
        2,
        8,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![8], 900_001)],
      ),
    ],
  )
  .await;
  let fallback_bytes = payloads.iter().fold(0_u64, |total, payload| {
    total.saturating_add(u64::try_from(payload.len()).unwrap())
  });
  let query = Arc::new(FixedBrokerBlobRangeQuery::new(broker_blob_failure(
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_OVERLOADED,
  )));
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    query.clone(),
    &metrics_scope,
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  let batches = reader.read_available(950).await.unwrap();

  assert_eq!(batches.len(), 2);
  assert_eq!(query.requests().len(), 1);
  assert_eq!(blob_store.ranges().len(), 2);
  let metric = "blob_stream_consumer_broker_blob_overload_test:reader";
  metrics.assert_counter_eq(
    1,
    &format!("{metric}:broker_blob_range_requests"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    &format!("{metric}:broker_blob_range_fallbacks"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    2,
    &format!("{metric}:fallback_blob_range_requests"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    fallback_bytes,
    &format!("{metric}:fallback_blob_range_bytes"),
    &labels!(),
  );
}

#[tokio::test]
async fn broker_blob_failure_preserves_direct_range_concurrency() {
  let blob_store = Arc::new(BlockingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let _ = write_shared_blob_segments(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    (1 ..= 4)
      .map(|partition_id| {
        (
          u64::from(partition_id),
          partition_id,
          SeqRange { start: 1, end: 1 },
          vec![new_record(
            vec![u8::try_from(partition_id).unwrap()],
            900_000,
          )],
        )
      })
      .collect(),
  )
  .await;
  let query = Arc::new(FixedBrokerBlobRangeQuery::new(broker_blob_failure(
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_OVERLOADED,
  )));
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      max_in_flight_batch_reads: Some(2),
      ..Default::default()
    },
    vec![1, 2, 3, 4],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    query.clone(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  let read_task = tokio::spawn(async move { reader.read_available(950).await });
  timeout(
    Duration::from_secs(1),
    blob_store.started_reads.acquire_many(2),
  )
  .await
  .expect("expected two concurrent fallback range reads")
  .expect("range-read start semaphore is open")
  .forget();
  assert_eq!(blob_store.maximum_active_reads.load(Ordering::SeqCst), 2);

  blob_store.released_reads.add_permits(4);
  assert_eq!(read_task.await.unwrap().unwrap().len(), 4);
  assert_eq!(query.requests().len(), 1);
  assert_eq!(blob_store.maximum_active_reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn distinct_blob_keys_use_distinct_broker_requests() {
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![7], 900_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    2,
    8,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![8], 900_001)],
    Compression::none(),
  )
  .await;
  let query = Arc::new(FixedBrokerBlobRangeQuery::with_responses(Vec::new()));
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    query.clone(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  let batches = reader.read_available(950).await.unwrap();

  let mut blob_keys = query
    .requests()
    .into_iter()
    .map(|request| request.blob_key.to_string())
    .collect::<Vec<_>>();
  blob_keys.sort();
  assert_eq!(batches.len(), 2);
  assert_eq!(
    blob_keys,
    vec!["telemetry/900/1.bin", "telemetry/900/2.bin"]
  );
  assert_eq!(blob_store.ranges().len(), 2);
}

#[tokio::test]
async fn broker_blob_cache_accepts_mixed_key_outcomes_without_direct_retry() {
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let delivered_records = vec![new_record(vec![7], 900_000)];
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 1 },
    delivered_records.clone(),
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    2,
    8,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![8], 900_001)],
    Compression::none(),
  )
  .await;
  let delivered_payload = Bytes::from(
    StoredRecordBatch {
      virtual_partition_id: 7,
      records: delivered_records,
      ..Default::default()
    }
    .write_to_bytes()
    .unwrap(),
  );
  let query = Arc::new(KeyedBrokerBlobRangeQuery::new([
    (
      "telemetry/900/1.bin".to_string(),
      broker_blob_success(vec![delivered_payload]),
    ),
    (
      "telemetry/900/2.bin".to_string(),
      broker_blob_failure(BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_NOT_FOUND),
    ),
  ]));
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    query.clone(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  let batches = reader.read_available(950).await.unwrap();

  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].virtual_partition_id, 7);
  assert_eq!(reader.cursor(7), Some(1));
  assert_eq!(reader.cursor(8), Some(1));
  assert_eq!(query.requests().len(), 2);
  assert!(blob_store.ranges().is_empty());
}

#[tokio::test]
async fn authoritative_broker_blob_not_found_skips_direct_retry() {
  let metrics = Helper::new();
  let metrics_scope = metrics
    .collector()
    .scope("blob_stream_consumer_broker_blob_not_found_test");
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let _ = write_shared_blob_segments(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    vec![(
      1,
      7,
      SeqRange { start: 1, end: 1 },
      vec![new_record(vec![7], 900_000)],
    )],
  )
  .await;
  let query = Arc::new(FixedBrokerBlobRangeQuery::new(ReadBlobRangesResponse {
    result: Some(read_blob_ranges_response::Result::Failure(
      BlobReadFailure {
        status: BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_NOT_FOUND.into(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default()));
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    query.clone(),
    &metrics_scope,
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();
  let batches = reader.read_available(950).await.unwrap();

  assert!(batches.is_empty());
  assert_eq!(reader.cursor(7), Some(1));
  assert_eq!(query.requests().len(), 1);
  assert!(blob_store.ranges().is_empty());
  let metric = "blob_stream_consumer_broker_blob_not_found_test:reader";
  metrics.assert_counter_eq(
    1,
    &format!("{metric}:broker_blob_range_requests"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    1,
    &format!("{metric}:broker_blob_range_not_found_groups"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    0,
    &format!("{metric}:broker_blob_range_fallbacks"),
    &labels!(),
  );
  metrics.assert_counter_eq(
    0,
    &format!("{metric}:fallback_blob_range_requests"),
    &labels!(),
  );
}

#[test]
fn recovery_planning_batches_active_cutover_partitions() {
  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  for partition_id in [7, 8] {
    reader.hydrate_cursor_with_source(
      partition_id,
      &CommittedCursor {
        virtual_partition_id: partition_id,
        seq_end: 0,
        source_checkpoint: Some(CommittedSourceCheckpoint {
          window_start_unix_seconds: 9_000,
          snowflake_id: 0,
        }),
      },
      Some(9_000 * 1_000),
      timestamp(9_000),
    );
  }
  reader
    .set_assigned_virtual_partitions(&[7, 8], timestamp(9_000))
    .unwrap();

  let (requests, recovery_scan) = reader
    .scan_requests(timestamp(9_000), &[7, 8], reader.runtime_settings())
    .unwrap();
  assert!(recovery_scan);
  assert_eq!(requests.len(), 1);
  assert_eq!(requests[0].eligibility.recovering_partitions, vec![7, 8]);
}

#[tokio::test]
async fn capacity_limited_segment_read_excludes_deferred_batches() {
  let blob_store = Arc::new(RecordingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let payload_len = write_multi_partition_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    Compression::none(),
    vec![
      (
        7,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![7], 1_001)],
      ),
      (
        8,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![8], 1_002)],
      ),
      (
        9,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![9], 1_003)],
      ),
    ],
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7, 9],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let settings = reader.runtime_settings();
  let first = reader
    .read_available_with_capacity_and_settings(timestamp(950), ReadCapacity::new(1), settings)
    .await
    .unwrap();
  assert_eq!(
    first
      .batches
      .iter()
      .map(|batch| batch.virtual_partition_id)
      .collect::<Vec<_>>(),
    vec![7]
  );
  assert!(
    first.batches[0]
      .admission_scan
      .as_ref()
      .expect("capacity-limited batch retains admission evidence")
      .metadata_sources_incomplete_by_capacity
  );
  let first_range = blob_store.ranges().pop().unwrap();
  assert_eq!(first_range.start, 0);
  assert!(first_range.end < payload_len);

  let settings = reader.runtime_settings();
  let second = reader
    .read_available_with_capacity_and_settings(timestamp(950), ReadCapacity::new(1), settings)
    .await
    .unwrap();
  assert_eq!(
    second
      .batches
      .iter()
      .map(|batch| batch.virtual_partition_id)
      .collect::<Vec<_>>(),
    vec![9]
  );
  let ranges = blob_store.ranges();
  assert_eq!(ranges.len(), 2);
  assert!(ranges[1].start > first_range.end);
}

#[tokio::test]
async fn failed_slice_in_segment_read_does_not_advance_cursor() {
  let blob_store = Arc::new(TruncatingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  write_multi_partition_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    Compression::none(),
    vec![
      (
        7,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![7], 1_001)],
      ),
      (
        7,
        SeqRange { start: 2, end: 2 },
        vec![new_record(vec![7], 1_002)],
      ),
    ],
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert!(reader.read_available(950).await.is_err());
  assert_eq!(reader.cursor(7), None);
}

#[tokio::test]
async fn bounded_parallel_reads_respect_configured_limit() {
  let blob_store = Arc::new(BlockingRangeBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  for sequence in 1 ..= 4 {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      900,
      sequence,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        1_000 + i64::try_from(sequence).unwrap(),
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      max_in_flight_batch_reads: Some(2),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let read_task = tokio::spawn(async move { reader.read_available(950).await });
  timeout(
    Duration::from_secs(1),
    blob_store.started_reads.acquire_many(2),
  )
  .await
  .expect("expected two concurrent range reads")
  .expect("range-read start semaphore is open")
  .forget();
  assert_eq!(blob_store.started_reads.available_permits(), 0);
  assert_eq!(blob_store.maximum_active_reads.load(Ordering::SeqCst), 2);

  blob_store.released_reads.add_permits(4);
  let batches = read_task.await.unwrap().unwrap();
  assert_eq!(batches.len(), 4);
  assert_eq!(blob_store.maximum_active_reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn catches_late_metadata_with_derived_candidate_horizon() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    600,
    1,
    11,
    SeqRange { start: 1, end: 2 },
    vec![new_record(vec![10], 700), new_record(vec![11], 701)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![11],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    TimeDuration::seconds(600),
    None,
  )
  .unwrap();

  let first = reader.read_available(900).await.unwrap();
  assert_eq!(first.len(), 1);
  assert_eq!(reader.cursor(11), Some(2));

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    600,
    2,
    11,
    SeqRange { start: 3, end: 4 },
    vec![new_record(vec![12], 702), new_record(vec![13], 703)],
    Compression::none(),
  )
  .await;

  let second = reader.read_available(1_200).await.unwrap();
  assert_eq!(second.len(), 1);
  assert_eq!(second[0].seq_range, SeqRange { start: 3, end: 4 });
  assert_eq!(reader.cursor(11), Some(4));
}

#[tokio::test]
async fn decodes_zstd_compressed_batches() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    9,
    3,
    SeqRange { start: 10, end: 10 },
    vec![new_record(vec![42, 43], 1_000)],
    Compression::zstd(3),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![3],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let batches = reader.read_available(950).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].records.len(), 1);
  assert_eq!(batches[0].records[0].payload, vec![42, 43]);
  assert_eq!(reader.cursor(3), Some(10));
}

#[tokio::test]
async fn fast_scan_uses_per_partition_inclusive_frontier() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    2,
    7,
    SeqRange { start: 1, end: 2 },
    vec![new_record(vec![1], 901), new_record(vec![2], 902)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  reader.read_available(901).await.unwrap();
  recording_metadata_store.scans.lock().clear();

  let batches = reader.read_available(902).await.unwrap();
  assert!(batches.is_empty());
  assert_eq!(
    *recording_metadata_store.scans.lock(),
    vec![(600, Some(SnowflakeId(0))), (900, Some(SnowflakeId(2)))]
  );
}

#[tokio::test]
async fn fast_scan_retains_coverage_after_capacity_stall() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();
  let window_start = Window::for_timestamp(timestamp(1_700_000_000), TimeDuration::seconds(300))
    .start
    .unix_timestamp();
  let scan_started_at = window_start.saturating_add(100);
  let first_source_at = scan_started_at.saturating_add(1);
  let second_source_at = scan_started_at.saturating_add(3);

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert!(
    reader
      .read_available(scan_started_at)
      .await
      .unwrap()
      .is_empty()
  );
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    window_start,
    SnowflakeId::minimum_for_timestamp(timestamp(first_source_at)).as_u64(),
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], first_source_at * 1_000)],
    Compression::none(),
  )
  .await;

  assert!(
    BoundedConsumerReader::read_available(
      &mut reader,
      timestamp(first_source_at),
      ReadCapacity::new(0),
    )
    .await
    .unwrap()
    .is_empty()
  );
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    window_start,
    SnowflakeId::minimum_for_timestamp(timestamp(second_source_at)).as_u64(),
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![2], second_source_at * 1_000)],
    Compression::none(),
  )
  .await;

  let batches = reader
    .read_available(scan_started_at.saturating_add(17))
    .await
    .unwrap();
  assert_eq!(
    batches
      .iter()
      .map(|batch| batch.seq_range.clone())
      .collect::<Vec<_>>(),
    vec![SeqRange { start: 1, end: 1 }, SeqRange { start: 2, end: 2 }]
  );
  assert_eq!(reader.cursor(7), Some(2));
}

#[tokio::test]
async fn fast_scan_catches_up_coverage_across_window_stall() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();
  let first_window_start =
    Window::for_timestamp(timestamp(1_700_000_000), TimeDuration::seconds(300))
      .start
      .unix_timestamp();
  let scan_started_at = first_window_start.saturating_add(100);
  let first_source_at = scan_started_at.saturating_add(1);
  let second_window_start = first_window_start.saturating_add(300);
  let second_source_at = second_window_start.saturating_add(190);

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert!(
    reader
      .read_available(scan_started_at)
      .await
      .unwrap()
      .is_empty()
  );
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    first_window_start,
    SnowflakeId::minimum_for_timestamp(timestamp(first_source_at)).as_u64(),
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], first_source_at * 1_000)],
    Compression::none(),
  )
  .await;
  assert!(
    BoundedConsumerReader::read_available(
      &mut reader,
      timestamp(first_source_at),
      ReadCapacity::new(0),
    )
    .await
    .unwrap()
    .is_empty()
  );
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    second_window_start,
    SnowflakeId::minimum_for_timestamp(timestamp(second_source_at)).as_u64(),
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![2], second_source_at * 1_000)],
    Compression::none(),
  )
  .await;

  let batches = reader
    .read_available(second_window_start.saturating_add(200))
    .await
    .unwrap();
  assert_eq!(
    batches
      .iter()
      .map(|batch| batch.seq_range.clone())
      .collect::<Vec<_>>(),
    vec![SeqRange { start: 1, end: 1 }, SeqRange { start: 2, end: 2 }]
  );
  assert_eq!(reader.cursor(7), Some(2));
}

#[tokio::test]
async fn fast_coverage_tail_retains_visibility_deferred_rows_below_the_fast_floor() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let first_window_start =
    Window::for_timestamp(timestamp(1_700_000_000), TimeDuration::seconds(300))
      .start
      .unix_timestamp();
  let initial_scan_at = first_window_start.saturating_add(100);
  let stalled_source_at = initial_scan_at.saturating_add(1);
  let cutover_window_start = first_window_start.saturating_add(300);
  let recovery_scan_at = cutover_window_start.saturating_add(200);
  let deferred_source_at = cutover_window_start.saturating_add(1);

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::seconds(1).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert!(
    reader
      .read_available(initial_scan_at)
      .await
      .unwrap()
      .is_empty()
  );
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    first_window_start,
    SnowflakeId::minimum_for_timestamp(timestamp(stalled_source_at)).as_u64(),
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], stalled_source_at * 1_000)],
    Compression::none(),
  )
  .await;
  assert!(
    BoundedConsumerReader::read_available(
      &mut reader,
      timestamp(stalled_source_at),
      ReadCapacity::new(0),
    )
    .await
    .unwrap()
    .is_empty()
  );
  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    cutover_window_start,
    SnowflakeId::minimum_for_timestamp(timestamp(deferred_source_at)).as_u64(),
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![2], deferred_source_at * 1_000)],
    Compression::none(),
    recovery_scan_at * 1_000,
  )
  .await;

  let recovered = reader.read_available(recovery_scan_at).await.unwrap();
  assert_eq!(recovered.len(), 1);
  assert_eq!(recovered[0].seq_range, SeqRange { start: 1, end: 1 });
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));

  let deferred = reader.read_available(recovery_scan_at + 1).await.unwrap();
  assert_eq!(deferred.len(), 1);
  assert_eq!(deferred[0].seq_range, SeqRange { start: 2, end: 2 });
  assert_eq!(reader.cursor(7), Some(2));
}

#[tokio::test]
async fn fast_scan_uses_lowest_partition_frontier_for_cross_partition_ordering() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    100,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![7], 901_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    8,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![8], 901_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert_eq!(reader.read_available(901).await.unwrap().len(), 2);
  recording_metadata_store.scans.lock().clear();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    2,
    8,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![9], 902_000)],
    Compression::none(),
  )
  .await;

  let batches = reader.read_available(902).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].virtual_partition_id, 8);
  assert!(
    recording_metadata_store
      .scans
      .lock()
      .contains(&(900, Some(SnowflakeId(1))))
  );
}

#[tokio::test]
async fn direct_fast_scan_reapplies_each_partition_effective_lower_bound() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();
  let window_start = 1_700_001_000;

  let _ = write_multi_partition_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    window_start,
    SnowflakeId::minimum_for_timestamp(timestamp(window_start + 10)).as_u64(),
    Compression::none(),
    vec![
      (
        7,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![7], (window_start + 10) * 1_000)],
      ),
      (
        8,
        SeqRange { start: 1, end: 1 },
        vec![new_record(vec![8], (window_start + 10) * 1_000)],
      ),
    ],
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    window_start,
    SnowflakeId::minimum_for_timestamp(timestamp(window_start + 35)).as_u64(),
    8,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![9], (window_start + 35) * 1_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    TimeDuration::seconds(15),
    None,
  )
  .unwrap()
  .metadata_window_size(TimeDuration::seconds(300))
  .maximum_clock_skew(TimeDuration::ZERO);
  reader.virtual_partition_states.insert(
    7,
    VirtualPartitionState::Fast {
      cursor: None,
      coverage_floor: Some(timestamp(window_start)),
      last_scan: None,
    },
  );
  reader.virtual_partition_states.insert(
    8,
    VirtualPartitionState::Fast {
      cursor: None,
      coverage_floor: None,
      last_scan: None,
    },
  );

  let batches = reader.read_available(window_start + 50).await.unwrap();
  assert_eq!(
    batches
      .iter()
      .map(|batch| (batch.virtual_partition_id, batch.seq_range.clone()))
      .collect::<Vec<_>>(),
    vec![
      (7, SeqRange { start: 1, end: 1 }),
      (8, SeqRange { start: 2, end: 2 })
    ]
  );
  assert_eq!(reader.cursor(8), Some(2));
  assert!(recording_metadata_store.scans.lock().contains(&(
    window_start,
    Some(SnowflakeId::minimum_for_timestamp(timestamp(window_start)))
  )));
}

#[tokio::test]
async fn broker_tail_request_keeps_each_fast_partition_frontier() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();
  let broker_query = Arc::new(RecordingBrokerMetadataQuery {
    requests: Mutex::new(Vec::new()),
  });
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_bool_flag("blob_stream_consumer_strong_metadata_reads", true),
  ));

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    100,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![7], 901_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    8,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![8], 901_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    broker_query.clone(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap();

  assert_eq!(reader.read_available(901).await.unwrap().len(), 2);
  broker_query.requests.lock().clear();
  recording_metadata_store.scans.lock().clear();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    2,
    8,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![9], 902_000)],
    Compression::none(),
  )
  .await;

  assert_eq!(reader.read_available(902).await.unwrap().len(), 1);
  let request = broker_query
    .requests()
    .into_iter()
    .find(|request| request.window_start_unix_seconds == 900)
    .expect("broker receives the active Fast window");
  let Some(read_metadata_window_request::Coverage::Tail(tail)) = request.coverage else {
    panic!("Fast broker request must use Tail coverage");
  };
  assert_eq!(
    tail
      .partition_bounds
      .iter()
      .map(|bound| (bound.virtual_partition_id, bound.min_snowflake))
      .collect::<HashMap<_, _>>(),
    HashMap::from([(7, 100), (8, 1)])
  );
  assert!(
    recording_metadata_store
      .scans
      .lock()
      .contains(&(900, Some(SnowflakeId(1))))
  );
}

async fn broker_offload_read(
  broker_response: BrokerMetadataResponse,
) -> (Vec<ConsumerBatch>, Option<u64>, Helper) {
  let metrics = Helper::new();
  let metrics_scope = metrics
    .collector()
    .scope("blob_stream_consumer_broker_metadata_offload_test");
  let blob_store = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let snowflake_id = SnowflakeId::minimum_for_timestamp(timestamp(901));
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    snowflake_id.as_u64(),
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![7], 901_000)],
    Compression::none(),
  )
  .await;
  let window = TopicWindowKey {
    topic: "telemetry".to_string(),
    window_start_unix_seconds: 900,
  };
  let direct_segments = metadata_store
    .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Strong)
    .await
    .unwrap();
  let mut empty_response = broker_metadata_response(&[], timestamp(901));
  let mut segments_response = broker_metadata_response(&direct_segments, timestamp(901));
  if matches!(broker_response, BrokerMetadataResponse::Malformed) {
    for response in [&mut empty_response, &mut segments_response] {
      if let Some(read_metadata_window_response::Result::Success(success)) =
        response.result.as_mut()
      {
        success.generation = 0;
      }
    }
  }
  let broker_query = Arc::new(FixedBrokerMetadataQuery {
    responses: match broker_response {
      BrokerMetadataResponse::Valid
      | BrokerMetadataResponse::Malformed
      | BrokerMetadataResponse::StaleAtReceipt => {
        HashMap::from([(600, empty_response), (900, segments_response)])
      },
      BrokerMetadataResponse::TransportFailure => HashMap::new(),
    },
  });
  let feature_flags = FakeLoader::new(Arc::new(DefaultFeatureFlags::default().with_bool_flag(
    "blob_stream_consumer_strong_metadata_reads",
    !matches!(broker_response, BrokerMetadataResponse::StaleAtReceipt),
  )));
  let time_provider: Arc<dyn TimeProvider> =
    if matches!(broker_response, BrokerMetadataResponse::StaleAtReceipt) {
      Arc::new(ManualTimeProvider::new(timestamp(902)))
    } else {
      Arc::new(SystemTimeProvider)
    };
  let reader = ConsumerReaderImpl::new(
    time_provider,
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    broker_query,
    rejecting_broker_blob_range_query(),
    &metrics_scope,
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    Some(feature_flags.snapshot_watch()),
  )
  .unwrap()
  .metadata_cache_max_age(TimeDuration::milliseconds(250));
  let mut reader = reader;
  let batches = reader.read_available(901).await.unwrap();
  (batches, reader.cursor(7), metrics)
}

#[tokio::test]
async fn broker_offload_metrics_distinguish_delivery_from_direct_fallback() {
  let metric = "blob_stream_consumer_broker_metadata_offload_test:reader";
  let (_, _, delivered_metrics) = broker_offload_read(BrokerMetadataResponse::Valid).await;
  delivered_metrics.assert_counter_eq(
    2,
    &format!("{metric}:broker_metadata_offload_requests"),
    &labels!(),
  );
  delivered_metrics.assert_counter_eq(
    2,
    &format!("{metric}:broker_metadata_offload_deliveries"),
    &labels!(),
  );
  delivered_metrics.assert_counter_eq(
    0,
    &format!("{metric}:broker_metadata_offload_fallbacks"),
    &labels!(),
  );

  let (_, _, fallback_metrics) =
    broker_offload_read(BrokerMetadataResponse::TransportFailure).await;
  fallback_metrics.assert_counter_eq(
    2,
    &format!("{metric}:broker_metadata_offload_requests"),
    &labels!(),
  );
  fallback_metrics.assert_counter_eq(
    0,
    &format!("{metric}:broker_metadata_offload_deliveries"),
    &labels!(),
  );
  fallback_metrics.assert_counter_eq(
    2,
    &format!("{metric}:broker_metadata_offload_fallbacks"),
    &labels!(),
  );

  let (_, _, stale_metrics) = broker_offload_read(BrokerMetadataResponse::StaleAtReceipt).await;
  stale_metrics.assert_counter_eq(
    2,
    &format!("{metric}:broker_metadata_offload_requests"),
    &labels!(),
  );
  stale_metrics.assert_counter_eq(
    0,
    &format!("{metric}:broker_metadata_offload_deliveries"),
    &labels!(),
  );
  stale_metrics.assert_counter_eq(
    2,
    &format!("{metric}:broker_metadata_offload_fallbacks"),
    &labels!(),
  );

  let (_, _, rejected_metrics) = broker_offload_read(BrokerMetadataResponse::Malformed).await;
  rejected_metrics.assert_counter_eq(
    2,
    &format!("{metric}:broker_metadata_offload_requests"),
    &labels!(),
  );
  rejected_metrics.assert_counter_eq(
    0,
    &format!("{metric}:broker_metadata_offload_deliveries"),
    &labels!(),
  );
  rejected_metrics.assert_counter_eq(
    2,
    &format!("{metric}:broker_metadata_offload_fallbacks"),
    &labels!(),
  );
}

#[tokio::test]
async fn seek_resets_partition_fast_frontier() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  for snowflake_id in 1 ..= 2 {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      900,
      snowflake_id,
      7,
      SeqRange {
        start: snowflake_id,
        end: snowflake_id,
      },
      vec![new_record(
        vec![u8::try_from(snowflake_id).unwrap()],
        901_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert_eq!(reader.read_available(901).await.unwrap().len(), 2);
  recording_metadata_store.scans.lock().clear();

  reader.set_cursor(7, 0);
  let rewound = reader.read_available(902).await.unwrap();

  assert_eq!(rewound.len(), 2);
  assert_eq!(rewound[0].seq_range, SeqRange { start: 1, end: 1 });
  assert!(
    recording_metadata_store
      .scans
      .lock()
      .contains(&(900, Some(SnowflakeId(0))))
  );
}

#[tokio::test]
async fn source_directed_seek_normalizes_target_window_and_handles_future_window() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  for (window_start, snowflake_id, sequence) in [(600, 1, 1), (1_200, 2, 2)] {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      window_start,
      snowflake_id,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        window_start * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let current = reader.read_available(1_200).await.unwrap();
  assert_eq!(current.len(), 1);
  assert_eq!(current[0].seq_range, SeqRange { start: 2, end: 2 });
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
  recording_metadata_store.scans.lock().clear();

  reader.seek(
    7,
    &ConsumerSeekTarget {
      offset: 0,
      window_start_unix_seconds: 601,
      snowflake_id: None,
    },
    timestamp(1_200),
  );
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Recovering {
      recovery_state: RecoveryState {
        next_window_start_unix_seconds: 600,
        cutover_window_start_unix_seconds: 1_200,
        first_window_start_unix_seconds: Some(600),
        first_window_min_snowflake: None,
        ..
      },
      ..
    })
  ));

  let recovered = reader.read_available(1_200).await.unwrap();
  assert_eq!(
    recovered
      .iter()
      .map(|batch| batch.seq_range.end)
      .collect::<Vec<_>>(),
    vec![1, 2]
  );
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
  {
    let scans = recording_metadata_store.scans.lock();
    assert!(scans.contains(&(600, None)));
    assert!(scans.contains(&(900, None)));
    assert!(scans.contains(&(1_200, None)));
  }
  recording_metadata_store.scans.lock().clear();

  reader.seek(
    7,
    &ConsumerSeekTarget {
      offset: 2,
      window_start_unix_seconds: 1_501,
      snowflake_id: None,
    },
    timestamp(1_200),
  );
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Recovering {
      recovery_state: RecoveryState {
        next_window_start_unix_seconds: 1_200,
        cutover_window_start_unix_seconds: 1_200,
        first_window_start_unix_seconds: None,
        first_window_min_snowflake: None,
        ..
      },
      ..
    })
  ));

  assert!(reader.read_available(1_200).await.unwrap().is_empty());
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
  let scans = recording_metadata_store.scans.lock();
  assert!(scans.contains(&(1_200, None)));
  assert!(!scans.contains(&(1_500, None)));
}

#[tokio::test]
async fn source_directed_seek_uses_the_checkpoint_snowflake_lower_bound() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();
  let window_start = 1_700_000_100;
  let checkpoint_timestamp = timestamp(window_start + 120);
  let checkpoint_snowflake = SnowflakeId::minimum_for_timestamp(checkpoint_timestamp);
  let expected_floor = SnowflakeId::minimum_for_timestamp(
    timestamp(window_start + 104).saturating_add(TimeDuration::milliseconds(990)),
  );

  for (snowflake_id, sequence) in [
    (
      SnowflakeId::minimum_for_timestamp(timestamp(window_start + 60)).as_u64(),
      1,
    ),
    (checkpoint_snowflake.as_u64(), 10),
    (
      SnowflakeId::minimum_for_timestamp(timestamp(window_start + 121)).as_u64(),
      11,
    ),
  ] {
    write_segment(
      blob_store.as_ref(),
      metadata_store_dyn.as_ref(),
      "telemetry",
      window_start,
      snowflake_id,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        window_start * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  reader.seek(
    7,
    &ConsumerSeekTarget {
      offset: 10,
      window_start_unix_seconds: window_start,
      snowflake_id: Some(checkpoint_snowflake.as_u64()),
    },
    checkpoint_timestamp,
  );
  let recovered = reader.read_available(window_start + 120).await.unwrap();

  assert_eq!(recovered.len(), 1);
  assert_eq!(recovered[0].seq_range, SeqRange { start: 11, end: 11 });
  assert!(
    metadata_store
      .scans
      .lock()
      .contains(&(window_start, Some(expected_floor)))
  );
}

#[tokio::test]
async fn fast_scan_uses_per_window_frontiers_across_candidate_window_boundary() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    100,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![7], 900_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    10,
    8,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![8], 900_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();
  assert_eq!(reader.read_available(900).await.unwrap().len(), 2);
  recording_metadata_store.scans.lock().clear();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    600,
    1,
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![9], 600_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    11,
    8,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![10], 900_000)],
    Compression::none(),
  )
  .await;

  let batches = reader.read_available(901).await.unwrap();
  let mut sequences = batches
    .into_iter()
    .map(|batch| batch.seq_range.end)
    .collect::<Vec<_>>();
  sequences.sort_unstable();
  assert_eq!(sequences, vec![2, 2]);
  assert_eq!(
    *recording_metadata_store.scans.lock(),
    vec![(600, Some(SnowflakeId(0))), (900, Some(SnowflakeId(10)))]
  );
}

#[tokio::test]
async fn fast_scan_omits_windows_before_the_safe_publication_floor() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();
  let now_unix_seconds = 1_700_000_000;
  let window_start = Window::for_timestamp(timestamp(now_unix_seconds), TimeDuration::seconds(300))
    .start
    .unix_timestamp();
  let now_unix_seconds = window_start.saturating_add(120);
  let safe_timestamp = OffsetDateTime::from_unix_timestamp(now_unix_seconds - 18)
    .unwrap()
    .saturating_add(time::Duration::milliseconds(990));
  let safe_floor = SnowflakeId::minimum_for_timestamp(safe_timestamp);

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert!(
    reader
      .read_available(now_unix_seconds)
      .await
      .unwrap()
      .is_empty()
  );
  assert_eq!(
    *metadata_store.scans.lock(),
    vec![(window_start, Some(safe_floor))]
  );
}

#[tokio::test]
async fn fast_scan_prunes_frontiers_for_windows_before_the_safe_publication_floor() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let current_window_start = 1_700_000_100;
  let previous_window_start = current_window_start - 300;
  let previous_snowflake = SnowflakeId::minimum_for_timestamp(
    OffsetDateTime::from_unix_timestamp(current_window_start - 10).unwrap(),
  );
  let current_snowflake = SnowflakeId::minimum_for_timestamp(
    OffsetDateTime::from_unix_timestamp(current_window_start).unwrap(),
  );

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    previous_window_start,
    previous_snowflake.as_u64(),
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(
      vec![1],
      current_window_start.saturating_mul(1_000),
    )],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert_eq!(
    reader
      .read_available(current_window_start)
      .await
      .unwrap()
      .len(),
    1
  );
  let scan_at_rollover = reader
    .partition_scan_states()
    .into_iter()
    .find(|state| state.virtual_partition_id == 7)
    .unwrap();
  assert_eq!(
    scan_at_rollover.fast_scan_bounds,
    vec![
      ConsumerReaderFastScanBoundState {
        window_start_unix_seconds: previous_window_start,
        floor_timestamp: OffsetDateTime::from_unix_timestamp(current_window_start - 16)
          .unwrap()
          .saturating_add(time::Duration::milliseconds(990)),
        time_floor: SnowflakeId::minimum_for_timestamp(
          OffsetDateTime::from_unix_timestamp(current_window_start - 16)
            .unwrap()
            .saturating_add(time::Duration::milliseconds(990)),
        ),
        observed_frontier: None,
        partition_lower_bound: SnowflakeId::minimum_for_timestamp(
          OffsetDateTime::from_unix_timestamp(current_window_start - 16)
            .unwrap()
            .saturating_add(time::Duration::milliseconds(990)),
        ),
        query_lower_bound: Some(SnowflakeId::minimum_for_timestamp(
          OffsetDateTime::from_unix_timestamp(current_window_start - 16)
            .unwrap()
            .saturating_add(time::Duration::milliseconds(990)),
        )),
      },
      ConsumerReaderFastScanBoundState {
        window_start_unix_seconds: current_window_start,
        floor_timestamp: OffsetDateTime::from_unix_timestamp(current_window_start).unwrap(),
        time_floor: current_snowflake,
        observed_frontier: None,
        partition_lower_bound: current_snowflake,
        query_lower_bound: Some(current_snowflake),
      },
    ]
  );
  assert_eq!(
    scan_at_rollover.fast_frontiers,
    vec![ConsumerReaderFastFrontierState {
      window_start_unix_seconds: previous_window_start,
      snowflake_id: previous_snowflake,
    }]
  );

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    current_window_start,
    current_snowflake.as_u64(),
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(
      vec![2],
      current_window_start.saturating_mul(1_000),
    )],
    Compression::none(),
  )
  .await;
  assert_eq!(
    reader
      .read_available(current_window_start + 1)
      .await
      .unwrap()
      .len(),
    1
  );
  let scan_after_previous_frontier = reader
    .partition_scan_states()
    .into_iter()
    .find(|state| state.virtual_partition_id == 7)
    .unwrap();
  assert_eq!(
    scan_after_previous_frontier
      .fast_scan_bounds
      .iter()
      .map(|bound| bound.observed_frontier)
      .collect::<Vec<_>>(),
    vec![Some(previous_snowflake), None]
  );
  let frontiers_at_rollover = reader
    .partition_scan_states()
    .into_iter()
    .find(|state| state.virtual_partition_id == 7)
    .unwrap()
    .fast_frontiers
    .clone();
  assert_eq!(
    frontiers_at_rollover,
    vec![
      ConsumerReaderFastFrontierState {
        window_start_unix_seconds: previous_window_start,
        snowflake_id: previous_snowflake,
      },
      ConsumerReaderFastFrontierState {
        window_start_unix_seconds: current_window_start,
        snowflake_id: current_snowflake,
      },
    ]
  );

  assert!(
    reader
      .read_available(current_window_start + 120)
      .await
      .unwrap()
      .is_empty()
  );
  let frontiers_after_cutoff = reader
    .partition_scan_states()
    .into_iter()
    .find(|state| state.virtual_partition_id == 7)
    .unwrap()
    .fast_frontiers
    .clone();
  assert_eq!(
    frontiers_after_cutoff,
    vec![ConsumerReaderFastFrontierState {
      window_start_unix_seconds: current_window_start,
      snowflake_id: current_snowflake,
    }]
  );
}

#[tokio::test]
async fn fast_scan_bounds_sparse_partitions_with_the_safe_publication_floor() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();
  let now_unix_seconds = 1_700_000_000;
  let window_start = Window::for_timestamp(timestamp(now_unix_seconds), TimeDuration::seconds(300))
    .start
    .unix_timestamp();
  let now_unix_seconds = window_start.saturating_add(120);
  let safe_timestamp = OffsetDateTime::from_unix_timestamp(now_unix_seconds - 18)
    .unwrap()
    .saturating_add(time::Duration::milliseconds(990));
  let safe_floor = SnowflakeId::minimum_for_timestamp(safe_timestamp);

  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    window_start,
    safe_floor.as_u64().saturating_sub(1 << 25),
    8,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], now_unix_seconds.saturating_mul(1_000))],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    window_start,
    safe_floor.as_u64(),
    8,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![2], now_unix_seconds.saturating_mul(1_000))],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new(
    Arc::new(SystemTimeProvider),
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  let batches = reader.read_available(now_unix_seconds).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].virtual_partition_id, 8);
  assert_eq!(batches[0].seq_range, SeqRange { start: 2, end: 2 });
  assert_eq!(
    *metadata_store.scans.lock(),
    vec![(window_start, Some(safe_floor))]
  );
  let sparse_partition_scan = reader
    .partition_scan_states()
    .into_iter()
    .find(|state| state.virtual_partition_id == 7)
    .unwrap();
  assert_eq!(
    sparse_partition_scan.fast_scan_bounds,
    vec![ConsumerReaderFastScanBoundState {
      window_start_unix_seconds: window_start,
      floor_timestamp: safe_timestamp,
      time_floor: safe_floor,
      observed_frontier: None,
      partition_lower_bound: safe_floor,
      query_lower_bound: Some(safe_floor),
    }]
  );

  metadata_store.scans.lock().clear();
  assert!(
    reader
      .read_available(now_unix_seconds.saturating_add(1))
      .await
      .unwrap()
      .is_empty()
  );
  assert_eq!(
    *metadata_store.scans.lock(),
    vec![(window_start, Some(safe_floor))]
  );
}
