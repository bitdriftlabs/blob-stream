use super::{ClusterHarness, IntegrationResources, ManualTimeProvider, TOPIC};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use bd_time::{OffsetDateTimeExt, TimeProvider};
use blob_stream_blob_store::{BlobKey, BlobStore};
use blob_stream_consumer::consumer::{
  BrokerBlobRangeQuery,
  BrokerMetadataQuery,
  ConsumerReaderImpl,
  GrpcBrokerMetadataQuery,
};
use blob_stream_consumer::iterator::{ConsumerIterator, ConsumerIteratorImpl, NextResult};
use blob_stream_consumer::{ConsumerReadConfig, DEFAULT_MAX_METADATA_PUBLICATION_LAG};
use blob_stream_metadata_store::{
  LeaseReleaseOutcome,
  MetadataReadConsistency,
  MetadataStore,
  MetadataWriteError,
  MetadataWriteResult,
  ProducerPartitionFence,
  ProducerPartitionLeaseStore,
  ProducerSequenceProgress,
  SegmentMetadata,
};
use blob_stream_producer::test::ProducerClientTestExt;
use blob_stream_producer::{ProducerClientImpl, ProducerRecord};
use blob_stream_proto::protos::blobstream::v1::broker::{
  ReadBlobRangesRequest,
  ReadBlobRangesResponse,
  ReadMetadataWindowRequest,
  ReadMetadataWindowResponse,
};
use blob_stream_types::{
  BatchMetadata,
  Compression,
  SeqRange,
  SnowflakeId,
  ToProtoDuration,
  TopicWindowKey,
  VirtualPartitionId,
  new_record,
  offset_datetime_from_unix_millis,
};
use bytes::Bytes;
use protobuf::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Instant, timeout};

//
// FenceInvalidatingMetadataStore
//

/// Rejects the next fenced metadata write by releasing its producer lease first.
pub struct FenceInvalidatingMetadataStore {
  inner: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  attempted_window: Mutex<Option<TopicWindowKey>>,
  rejected_fenced_write: AtomicBool,
}

impl FenceInvalidatingMetadataStore {
  #[must_use]
  pub fn new(
    inner: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  ) -> Self {
    Self {
      inner,
      lease_store,
      attempted_window: Mutex::new(None),
      rejected_fenced_write: AtomicBool::new(false),
    }
  }

  pub async fn attempted_window(&self) -> Option<TopicWindowKey> {
    self.attempted_window.lock().await.clone()
  }

  #[must_use]
  pub fn rejected_fenced_write(&self) -> bool {
    self.rejected_fenced_write.load(Ordering::Acquire)
  }
}

#[async_trait]
impl MetadataStore for FenceInvalidatingMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> MetadataWriteResult {
    let fence = fences
      .and_then(|fences| fences.first())
      .cloned()
      .ok_or_else(|| anyhow!("fenced metadata write did not include a producer lease fence"))?;
    *self.attempted_window.lock().await = Some(metadata.window.clone());

    let release = self
      .lease_store
      .release_lease(
        &fence.key,
        &fence.fence.holder_id,
        &fence.fence.lease_session_id,
        offset_datetime_from_unix_millis(now_ts_ms),
        ProducerSequenceProgress::default(),
      )
      .await?;
    if release != LeaseReleaseOutcome::Released {
      return Err(anyhow!("test fault could not invalidate producer lease: {release:?}").into());
    }

    let result = self.inner.write_segment(metadata, fences, now_ts_ms).await;
    if matches!(&result, Err(MetadataWriteError::ProducerLeaseFenceLost)) {
      self.rejected_fenced_write.store(true, Ordering::Release);
    }
    result
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await
  }
}

//
// DelayedVisibilityMetadataStore
//

type PendingMetadataWrite = (
  Instant,
  SegmentMetadata,
  Option<Vec<ProducerPartitionFence>>,
  i64,
);

/// Defers writes until their configured delay elapses and visibility is released.
pub struct DelayedVisibilityMetadataStore {
  inner: Arc<dyn MetadataStore>,
  delay: Duration,
  pending: Mutex<Vec<PendingMetadataWrite>>,
  visibility_held: AtomicBool,
}

impl DelayedVisibilityMetadataStore {
  #[must_use]
  pub fn new(inner: Arc<dyn MetadataStore>, delay: Duration) -> Self {
    Self {
      inner,
      delay,
      pending: Mutex::new(Vec::new()),
      visibility_held: AtomicBool::new(false),
    }
  }

  pub fn hold_visibility(&self) {
    self.visibility_held.store(true, Ordering::Release);
  }

  pub fn release_visibility(&self) {
    self.visibility_held.store(false, Ordering::Release);
  }

  async fn flush_visible_segments(&self) -> Result<()> {
    if self.visibility_held.load(Ordering::Acquire) {
      return Ok(());
    }

    let now = Instant::now();
    let mut pending = self.pending.lock().await;
    let mut ready = Vec::new();
    let mut future = Vec::new();

    for (visible_at, metadata, fences, now_ts_ms) in pending.drain(..) {
      if visible_at <= now {
        ready.push((metadata, fences, now_ts_ms));
      } else {
        future.push((visible_at, metadata, fences, now_ts_ms));
      }
    }
    *pending = future;
    drop(pending);

    for (metadata, fences, now_ts_ms) in ready {
      self
        .inner
        .write_segment(metadata, fences.as_deref(), now_ts_ms)
        .await?;
    }
    Ok(())
  }
}

#[async_trait]
impl MetadataStore for DelayedVisibilityMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> MetadataWriteResult {
    let mut pending = self.pending.lock().await;
    pending.push((
      Instant::now() + self.delay,
      metadata,
      fences.map(ToOwned::to_owned),
      now_ts_ms,
    ));
    Ok(())
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    self.flush_visible_segments().await?;
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await
  }
}

//
// CountingWindowMetadataStore
//

/// Counts metadata scans for one selected window while forwarding all store operations.
pub struct CountingWindowMetadataStore {
  inner: Arc<dyn MetadataStore>,
  counted_window_start: i64,
  scan_count: AtomicUsize,
}

impl CountingWindowMetadataStore {
  #[must_use]
  pub fn new(inner: Arc<dyn MetadataStore>, counted_window_start: i64) -> Self {
    Self {
      inner,
      counted_window_start,
      scan_count: AtomicUsize::new(0),
    }
  }

  #[must_use]
  pub fn scan_count(&self) -> usize {
    self.scan_count.load(Ordering::Acquire)
  }
}

#[async_trait]
impl MetadataStore for CountingWindowMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
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
    if window.window_start_unix_seconds == self.counted_window_start {
      self.scan_count.fetch_add(1, Ordering::AcqRel);
    }
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await
  }
}

//
// DeferredWindowPublicationMetadataStore
//

/// Rewrites publication time for one window after the underlying scan completes.
pub struct DeferredWindowPublicationMetadataStore {
  inner: Arc<dyn MetadataStore>,
  deferred_window_start: Arc<AtomicI64>,
  deferred_window_published_ts_ms: AtomicI64,
  deferred_window_scanned: AtomicBool,
  deferred_window_scan: Notify,
}

impl DeferredWindowPublicationMetadataStore {
  #[must_use]
  pub fn new(inner: Arc<dyn MetadataStore>, deferred_window_start: Arc<AtomicI64>) -> Self {
    Self {
      inner,
      deferred_window_start,
      deferred_window_published_ts_ms: AtomicI64::new(0),
      deferred_window_scanned: AtomicBool::new(false),
      deferred_window_scan: Notify::new(),
    }
  }

  pub fn set_deferred_window_published_ts_ms(&self, published_ts_ms: i64) {
    self
      .deferred_window_published_ts_ms
      .store(published_ts_ms, Ordering::Release);
  }

  pub async fn wait_until_deferred_window_scanned(&self) {
    loop {
      let notified = self.deferred_window_scan.notified();
      if self.deferred_window_scanned.load(Ordering::Acquire) {
        return;
      }
      notified.await;
    }
  }
}

#[async_trait]
impl MetadataStore for DeferredWindowPublicationMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
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
    let mut segments = self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await?;
    if window.window_start_unix_seconds == self.deferred_window_start.load(Ordering::Acquire) {
      for segment in &mut segments {
        segment.metadata_published_at = OffsetDateTime::UNIX_EPOCH
          + TimeDuration::milliseconds(
            self.deferred_window_published_ts_ms.load(Ordering::Acquire),
          );
      }
      self.deferred_window_scanned.store(true, Ordering::Release);
      self.deferred_window_scan.notify_waiters();
    }
    Ok(segments)
  }
}

//
// GatedMetadataStore
//

#[derive(Clone, Copy)]
enum MetadataScanGate {
  None,
  FirstScan,
  FirstTailScan,
}

/// Records metadata scan bounds and optionally blocks one initial or Tail scan.
pub struct GatedMetadataStore {
  inner: Arc<dyn MetadataStore>,
  gate: MetadataScanGate,
  scan_count: AtomicUsize,
  gated_scan_count: AtomicUsize,
  gate_armed: AtomicBool,
  scan_requests: Mutex<Vec<(TopicWindowKey, Option<SnowflakeId>)>>,
  scan_started: Notify,
  release_scan: Notify,
}

impl GatedMetadataStore {
  #[must_use]
  pub fn recording(inner: Arc<dyn MetadataStore>) -> Self {
    Self {
      inner,
      gate: MetadataScanGate::None,
      scan_count: AtomicUsize::new(0),
      gated_scan_count: AtomicUsize::new(0),
      gate_armed: AtomicBool::new(false),
      scan_requests: Mutex::new(Vec::new()),
      scan_started: Notify::new(),
      release_scan: Notify::new(),
    }
  }

  #[must_use]
  pub fn new(inner: Arc<dyn MetadataStore>) -> Self {
    Self {
      inner,
      gate: MetadataScanGate::FirstScan,
      scan_count: AtomicUsize::new(0),
      gated_scan_count: AtomicUsize::new(0),
      gate_armed: AtomicBool::new(true),
      scan_requests: Mutex::new(Vec::new()),
      scan_started: Notify::new(),
      release_scan: Notify::new(),
    }
  }

  #[must_use]
  pub fn gate_first_tail_scan(inner: Arc<dyn MetadataStore>) -> Self {
    Self {
      inner,
      gate: MetadataScanGate::FirstTailScan,
      scan_count: AtomicUsize::new(0),
      gated_scan_count: AtomicUsize::new(0),
      gate_armed: AtomicBool::new(true),
      scan_requests: Mutex::new(Vec::new()),
      scan_started: Notify::new(),
      release_scan: Notify::new(),
    }
  }

  pub async fn wait_for_first_scan(&self) {
    self.wait_for_scan(&self.scan_count).await;
  }

  pub async fn wait_for_gated_scan(&self) {
    self.wait_for_scan(&self.gated_scan_count).await;
  }

  pub async fn wait_for_gated_scan_count(&self, expected_count: usize) {
    loop {
      let notified = self.scan_started.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if self.gated_scan_count.load(Ordering::Acquire) >= expected_count {
        return;
      }
      notified.await;
    }
  }

  async fn wait_for_scan(&self, count: &AtomicUsize) {
    loop {
      let notified = self.scan_started.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if count.load(Ordering::Acquire) > 0 {
        return;
      }
      notified.await;
    }
  }

  pub fn release(&self) {
    self.release_scan.notify_waiters();
  }

  pub fn arm_next_scan(&self) {
    self.gate_armed.store(true, Ordering::Release);
  }

  pub async fn clear_scan_requests(&self) {
    self.scan_requests.lock().await.clear();
  }

  pub async fn scan_requests(&self) -> Vec<(TopicWindowKey, Option<SnowflakeId>)> {
    self.scan_requests.lock().await.clone()
  }

  #[must_use]
  pub fn scan_count(&self) -> usize {
    self.scan_count.load(Ordering::Acquire)
  }

  #[must_use]
  pub fn gated_scan_count(&self) -> usize {
    self.gated_scan_count.load(Ordering::Acquire)
  }
}

#[async_trait]
impl MetadataStore for GatedMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
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
    self.scan_count.fetch_add(1, Ordering::AcqRel);
    self
      .scan_requests
      .lock()
      .await
      .push((window.clone(), min_snowflake));
    let gates_scan = matches!(self.gate, MetadataScanGate::FirstScan)
      || (matches!(self.gate, MetadataScanGate::FirstTailScan) && min_snowflake.is_some());
    if gates_scan && self.gate_armed.swap(false, Ordering::AcqRel) {
      self.gated_scan_count.fetch_add(1, Ordering::AcqRel);
      let release = self.release_scan.notified();
      tokio::pin!(release);
      release.as_mut().enable();
      self.scan_started.notify_waiters();
      release.await;
    }
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await
  }
}

//
// Handcrafted Metadata
//

pub async fn produce_message_at_manual_time(
  cluster: &ClusterHarness,
  producer: &Arc<ProducerClientImpl>,
  manual_time: &ManualTimeProvider,
  key: Vec<u8>,
  id: &str,
) -> Result<blob_stream_producer::ProducerAck> {
  let event_timestamp_ms = manual_time.now().unix_timestamp_ms();
  let payload = id.as_bytes().to_vec();
  let producer = Arc::clone(producer);
  let produce_task = tokio::spawn(async move {
    producer
      .produce_one(ProducerRecord::new(
        TOPIC.into(),
        key,
        payload.into(),
        event_timestamp_ms,
      ))
      .await
  });

  timeout(Duration::from_secs(5), async {
    loop {
      let buffered = cluster
        .broker_state_snapshots()
        .await
        .iter()
        .any(|snapshot| {
          snapshot.topics.iter().any(|topic| {
            topic
              .local_partitions
              .iter()
              .any(|partition| partition.buffered_batch_count > 0)
          })
        });
      if buffered {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("producer request did not enter the broker buffer"))??;
  manual_time.advance(TimeDuration::seconds(60));

  produce_task
    .await
    .map_err(|error| anyhow!("manual-time producer task join error: {error}"))?
    .map_err(Into::into)
}

pub async fn write_recovery_segment(
  blob_store: &dyn BlobStore,
  metadata_store: &dyn MetadataStore,
  virtual_partition_id: VirtualPartitionId,
  window_start_unix_seconds: i64,
  snowflake_id: u64,
  sequence: u64,
  payload: &str,
  published_ts_ms: i64,
) -> Result<()> {
  let record = new_record(
    payload.as_bytes().to_vec(),
    window_start_unix_seconds.saturating_mul(1_000),
  );
  let encoded = blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch {
    virtual_partition_id,
    records: vec![record],
    ..Default::default()
  }
  .write_to_bytes()?;
  let blob_key = BlobKey::new(format!(
    "recovery/{window_start_unix_seconds}/{snowflake_id}.bin"
  ));
  blob_store
    .put(&blob_key, Bytes::from(encoded.clone()))
    .await?;

  metadata_store
    .write_segment(
      SegmentMetadata::new(
        TopicWindowKey {
          topic: TOPIC.to_string(),
          window_start_unix_seconds,
        },
        SnowflakeId(snowflake_id),
        blob_key,
        Compression::none(),
        HashMap::from([(
          virtual_partition_id,
          vec![BatchMetadata {
            seq_range: SeqRange {
              start: sequence,
              end: sequence,
            },
            byte_range: blob_stream_types::ByteRange {
              start: 0,
              end: u64::try_from(encoded.len())?,
            },
            payload_bytes: u64::try_from(encoded.len())?,
          }],
        )]),
        OffsetDateTime::UNIX_EPOCH
          + TimeDuration::milliseconds(window_start_unix_seconds.saturating_mul(1_000)),
        OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(published_ts_ms),
      ),
      None,
      0,
    )
    .await
    .map_err(anyhow::Error::new)
}

pub async fn write_recovery_segment_for_partitions(
  blob_store: &dyn BlobStore,
  metadata_store: &dyn MetadataStore,
  window_start_unix_seconds: i64,
  snowflake_id: u64,
  partitions: &[(VirtualPartitionId, u64, &str)],
) -> Result<()> {
  let mut encoded = Vec::new();
  let mut segment_index = HashMap::new();
  for (virtual_partition_id, sequence, payload) in partitions {
    let batch = blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch {
      virtual_partition_id: *virtual_partition_id,
      records: vec![new_record(
        payload.as_bytes().to_vec(),
        window_start_unix_seconds.saturating_mul(1_000),
      )],
      ..Default::default()
    };
    let batch_encoded = batch.write_to_bytes()?;
    let start = u64::try_from(encoded.len())?;
    encoded.extend_from_slice(&batch_encoded);
    let end = u64::try_from(encoded.len())?;
    segment_index.insert(
      *virtual_partition_id,
      vec![BatchMetadata {
        seq_range: SeqRange {
          start: *sequence,
          end: *sequence,
        },
        byte_range: blob_stream_types::ByteRange { start, end },
        payload_bytes: u64::try_from(batch_encoded.len())?,
      }],
    );
  }
  let blob_key = BlobKey::new(format!(
    "recovery/{window_start_unix_seconds}/{snowflake_id}-partitions.bin"
  ));
  blob_store.put(&blob_key, Bytes::from(encoded)).await?;
  let publication_time =
    OffsetDateTime::UNIX_EPOCH + TimeDuration::seconds(window_start_unix_seconds);
  metadata_store
    .write_segment(
      SegmentMetadata::new(
        TopicWindowKey {
          topic: TOPIC.to_string(),
          window_start_unix_seconds,
        },
        SnowflakeId(snowflake_id),
        blob_key,
        Compression::none(),
        segment_index,
        publication_time,
        publication_time,
      ),
      None,
      0,
    )
    .await
    .map_err(anyhow::Error::new)
}

pub async fn broker_metadata_cache_reader(
  cluster: &ClusterHarness,
  resources: &IntegrationResources,
  metadata_store: Arc<dyn MetadataStore>,
  strongly_consistent: bool,
  metadata_visibility_delay: TimeDuration,
) -> Result<ConsumerReaderImpl> {
  let discovery = Arc::new(cluster.producer_discovery());
  let broker_metadata_query = Arc::new(GrpcBrokerMetadataQuery::new(discovery).await?);
  ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      strongly_consistent_metadata_reads: Some(strongly_consistent),
      metadata_visibility_delay: metadata_visibility_delay.into_proto(),
      ..Default::default()
    },
    vec![0, 1],
    HashMap::new(),
    resources.blob_store(),
    metadata_store,
    broker_metadata_query,
    rejecting_broker_blob_range_query(),
    &Collector::default().scope("blob_stream_broker_metadata_cache_frontier_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
}

pub fn rejecting_broker_metadata_query() -> Arc<dyn BrokerMetadataQuery> {
  Arc::new(RejectingBrokerMetadataQuery)
}

pub fn rejecting_broker_blob_range_query() -> Arc<dyn BrokerBlobRangeQuery> {
  Arc::new(RejectingBrokerBlobRangeQuery)
}

struct RejectingBrokerMetadataQuery;

#[async_trait]
impl BrokerMetadataQuery for RejectingBrokerMetadataQuery {
  async fn read_metadata_window(
    &self,
    _request: ReadMetadataWindowRequest,
  ) -> Result<ReadMetadataWindowResponse> {
    Err(anyhow!("test broker metadata query is unavailable"))
  }
}

struct RejectingBrokerBlobRangeQuery;

#[async_trait]
impl BrokerBlobRangeQuery for RejectingBrokerBlobRangeQuery {
  async fn read_blob_ranges(
    &self,
    _request: ReadBlobRangesRequest,
  ) -> Result<ReadBlobRangesResponse> {
    Err(anyhow!("test broker blob-range query is unavailable"))
  }
}

pub async fn consume_one_record(mut consumer: ConsumerIteratorImpl) -> Result<String> {
  consumer.start()?;
  consume_next_record(&mut consumer).await
}

pub async fn consume_next_record(consumer: &mut ConsumerIteratorImpl) -> Result<String> {
  loop {
    match consumer.next().await? {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => {
        return String::from_utf8(record.record.payload.to_vec())
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"));
      },
    }
  }
}
