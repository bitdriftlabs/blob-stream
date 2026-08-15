#![allow(clippy::unwrap_used)]

use super::state::{RecoveryState, VirtualPartitionState};
use super::{
  ConsumerReader as BoundedConsumerReader,
  ConsumerReaderFastFrontierState,
  ConsumerReaderFastScanBoundState,
  ConsumerReaderImpl,
  ReadCapacity,
};
use crate::config::{
  ConsumerReadConfig,
  ConsumerReadRuntimeSettings,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bd_runtime_config::loader::Loader;
use bd_server_stats::stats::Collector;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use blob_stream_blob_store::{
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
};
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
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
use protobuf::Message;
use std::collections::HashMap;
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
  assert_eq!(scan_state[0].batches_accepted, 0);
  let batches = reader.read_available(903).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 1, end: 1 });
  let scan_state = reader.partition_scan_states();
  assert_eq!(scan_state[0].cursor_before, None);
  assert_eq!(scan_state[0].cursor_after, Some(1));
  assert_eq!(scan_state[0].batches_accepted, 1);
  assert_eq!(scan_state[0].records_accepted, 1);
}

#[tokio::test]
async fn strong_metadata_reads_accept_metadata_at_its_publication_timestamp() {
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
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

  assert!(outcome.batches.is_empty());
  assert_eq!(
    outcome.next_visibility_eligible_at,
    Some(
      OffsetDateTime::from_unix_timestamp(902)
        .unwrap()
        .saturating_add(time::Duration::milliseconds(500))
    )
  );
  assert!(
    recording_metadata_store
      .consistencies
      .lock()
      .iter()
      .all(|consistency| *consistency == MetadataReadConsistency::Strong)
  );
  let scan_state = reader.partition_scan_states();
  assert_eq!(scan_state[0].metadata_segments_deferred_by_visibility, 1);

  let before_maturity = reader
    .read_available_with_capacity_and_settings(
      timestamp(902).saturating_add(TimeDuration::milliseconds(499)),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert!(before_maturity.batches.is_empty());

  let at_maturity = reader
    .read_available_with_capacity_and_settings(
      timestamp(902).saturating_add(TimeDuration::milliseconds(500)),
      ReadCapacity::new(TEST_READ_CAPACITY_BYTES),
      reader.runtime_settings(),
    )
    .await
    .unwrap();
  assert_eq!(at_maturity.batches.len(), 1);
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      metadata_visibility_delay: TimeDuration::milliseconds(300_000).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    metadata_store_dyn,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    metadata_store_dyn,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
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
async fn assignment_activates_hydrated_state_and_removes_revoked_state() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(window_size_seconds).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
  let window_size_seconds = 300;
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(window_size_seconds).into_proto(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
  let window_size_seconds = 300;
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(window_size_seconds).into_proto(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .unwrap();

  assert!(reader.read_available(950).await.is_err());
  assert_eq!(reader.cursor(7), None);

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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store_dyn,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(window_size_seconds).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      metadata_visibility_delay: TimeDuration::milliseconds(1_000).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store_dyn,
    metadata_store_dyn,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7, 9],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
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
    vec![7, 9]
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
  assert_eq!(
    blob_store.ranges(),
    vec![ByteRange {
      start: 0,
      end: payload_len,
    }]
  );
}

#[test]
fn recovery_planning_batches_active_cutover_partitions() {
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7, 9],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      max_in_flight_batch_reads: Some(2),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store.clone(),
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![11],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![3],
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
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
async fn historical_seek_recovers_recent_windows_then_returns_to_fast_path() {
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
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

  reader.seek(7, 0, timestamp(1_200));
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Recovering {
      recovery_state: RecoveryState {
        next_window_start_unix_seconds: 600,
        cutover_window_start_unix_seconds: 1_200,
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
  let scans = recording_metadata_store.scans.lock();
  assert!(scans.contains(&(600, None)));
  assert!(scans.contains(&(900, None)));
  assert!(scans.contains(&(1_200, None)));
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
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
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size: TimeDuration::seconds(300).into_proto(),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
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
  let next_safe_floor =
    SnowflakeId::minimum_for_timestamp(safe_timestamp.saturating_add(time::Duration::seconds(1)));
  assert_eq!(
    *metadata_store.scans.lock(),
    vec![(window_start, Some(next_safe_floor))]
  );
}
