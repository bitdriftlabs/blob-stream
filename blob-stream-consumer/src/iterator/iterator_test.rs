#![allow(clippy::unwrap_used)]

use super::delivery::{BufferedBatch, DeliveryState};
use super::prefetch::{IdlePollBackoff, SeekTrace};
use super::shared::{ActivePartitionState, ConsumerIteratorMetrics};
use super::{
  ConsumerCoordinationSource,
  ConsumerDeliveryState,
  ConsumerIterator,
  ConsumerIteratorBuilder,
  ConsumerIteratorImpl,
  ConsumerLifecycleHooks,
  ConsumerSeekTarget,
  CoordinationSnapshot,
  NextResult,
};
use crate::config::{
  ConsumerGroupConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG,
  consumer_max_clock_skew,
};
use crate::consumer::{BrokerBlobRangeQuery, BrokerMetadataQuery};
use crate::diagnostics::{
  ConsumerAssignmentPlanSnapshot,
  ConsumerAssignmentPolicy,
  ConsumerCommittedCursorSnapshot,
  ConsumerGroupLeaseObservation,
  ConsumerLocalPartitionSnapshot,
  ConsumerPartitionAssignmentSnapshot,
  ConsumerPartitionReadMode,
  ConsumerSourceCheckpointSnapshot,
  ConsumerStateSnapshot,
};
use bd_server_stats::stats::Collector;
use bd_time::SystemTimeProvider;
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
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupMember,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLease,
  ConsumerGroupPlannerLeaseOutcome,
  ConsumerGroupReleaseOutcome,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
  InMemoryMetadataStore,
  MetadataReadConsistency,
  MetadataStore,
  MetadataWriteResult,
  SegmentMetadata,
};
use blob_stream_proto::protos::blobstream::v1::broker::{
  ReadBlobRangesRequest,
  ReadBlobRangesResponse,
  ReadMetadataWindowRequest,
  ReadMetadataWindowResponse,
  StoredRecordBatch,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{
  BatchMetadata,
  CommittedCursor,
  CommittedSourceCheckpoint,
  Compression,
  Record,
  RecordBatch,
  SeqRange,
  SnowflakeId,
  ToProtoDuration,
  TopicWindowKey,
  VirtualPartitionId,
  new_record,
  now_unix_millis,
  offset_datetime_from_unix_millis,
  unix_millis_from_offset_datetime,
};
use bytes::Bytes;
use parking_lot::Mutex;
use protobuf::Message;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{SystemTime, UNIX_EPOCH};
use time::macros::datetime;
use time::{Duration as TimeDuration, OffsetDateTime, UtcOffset};
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep, timeout};

struct RejectingBrokerMetadataQuery;

#[async_trait::async_trait]
impl BrokerMetadataQuery for RejectingBrokerMetadataQuery {
  async fn read_metadata_window(
    &self,
    _request: ReadMetadataWindowRequest,
  ) -> anyhow::Result<ReadMetadataWindowResponse> {
    Err(anyhow::anyhow!("test broker metadata query is unavailable"))
  }
}

struct RejectingBrokerBlobRangeQuery;

#[async_trait::async_trait]
impl BrokerBlobRangeQuery for RejectingBrokerBlobRangeQuery {
  async fn read_blob_ranges(
    &self,
    _request: ReadBlobRangesRequest,
  ) -> anyhow::Result<ReadBlobRangesResponse> {
    Err(anyhow::anyhow!(
      "test broker blob-range query is unavailable"
    ))
  }
}

fn rejecting_broker_metadata_query() -> Arc<dyn BrokerMetadataQuery> {
  Arc::new(RejectingBrokerMetadataQuery)
}

fn rejecting_broker_blob_range_query() -> Arc<dyn BrokerBlobRangeQuery> {
  Arc::new(RejectingBrokerBlobRangeQuery)
}

struct MutableCoordinationSource {
  snapshot: Arc<Mutex<CoordinationSnapshot>>,
}

struct CommitGateHooks {
  entered: Mutex<Option<oneshot::Sender<()>>>,
  release: Mutex<Option<oneshot::Receiver<()>>>,
}

struct RebalanceRecordingHooks {
  rebalance_applied_calls: AtomicUsize,
}

#[test]
fn seek_trace_records_cancellation_and_parents_recovery() {
  let (spans, ()) = bd_log::test::with_two_phase_test_otel("blob-stream-consumer-test", async {
    let seek_trace = SeekTrace::new(bd_log::otel_info_span!(
      "blob_stream.consumer.partition_seek",
      seek.outcome = tracing::field::Empty,
      otel.status_code = tracing::field::Empty,
    ));
    let recovery_span =
      seek_trace.in_scope(|| bd_log::otel_info_span!("blob_stream.consumer.partition_recovery"));
    drop(recovery_span);
    seek_trace.finish("cancelled");
  });

  let seek_span = spans
    .iter()
    .find(|span| span.name == "blob_stream.consumer.partition_seek")
    .expect("seek span should be exported");
  let recovery_span = spans
    .iter()
    .find(|span| span.name == "blob_stream.consumer.partition_recovery")
    .expect("recovery span should be exported");
  let outcome = seek_span
    .attributes
    .iter()
    .find(|attribute| attribute.key.as_str() == "seek.outcome")
    .map(|attribute| attribute.value.as_str());

  assert_eq!(outcome.as_deref(), Some("cancelled"));
  assert_eq!(format!("{:?}", seek_span.status), "Unset");
  assert_eq!(
    recovery_span.parent_span_id,
    seek_span.span_context.span_id()
  );
}

#[test]
fn lease_expiration_deadline_matches_store_millisecond_precision() {
  let now = OffsetDateTime::UNIX_EPOCH + TimeDuration::seconds(1) + TimeDuration::microseconds(999);
  let deadline = super::driver::persisted_lease_expires_at(now, TimeDuration::seconds(30)).unwrap();

  assert_eq!(deadline, offset_datetime_from_unix_millis(31_000));
  assert!(deadline < now.saturating_add(TimeDuration::seconds(30)));
}

#[async_trait::async_trait]
impl ConsumerLifecycleHooks for CommitGateHooks {
  async fn before_commit(&self, _member_id: &str, _generation: u64) {
    if let Some(entered) = self.entered.lock().take() {
      let _ = entered.send(());
    }
    let release = { self.release.lock().take() };
    if let Some(release) = release {
      let _ = release.await;
    }
  }
}

#[async_trait::async_trait]
impl ConsumerLifecycleHooks for RebalanceRecordingHooks {
  async fn rebalance_applied(
    &self,
    _member_id: &str,
    _generation: u64,
    _partitions: &[VirtualPartitionId],
  ) {
    self.rebalance_applied_calls.fetch_add(1, Ordering::Relaxed);
  }
}

struct BlockingCoordinationSource {
  snapshot: Arc<Mutex<CoordinationSnapshot>>,
  block_snapshots: AtomicBool,
  snapshot_calls: AtomicUsize,
  snapshot_started: Arc<tokio::sync::Notify>,
  snapshot_release: Arc<tokio::sync::Notify>,
}

impl BlockingCoordinationSource {
  fn new(snapshot: CoordinationSnapshot) -> Self {
    Self {
      snapshot: Arc::new(Mutex::new(snapshot)),
      block_snapshots: AtomicBool::new(false),
      snapshot_calls: AtomicUsize::new(0),
      snapshot_started: Arc::new(tokio::sync::Notify::new()),
      snapshot_release: Arc::new(tokio::sync::Notify::new()),
    }
  }
}

#[async_trait::async_trait]
impl ConsumerCoordinationSource for BlockingCoordinationSource {
  async fn snapshot(&self) -> anyhow::Result<CoordinationSnapshot> {
    self.snapshot_calls.fetch_add(1, Ordering::SeqCst);
    if self.block_snapshots.load(Ordering::SeqCst) {
      self.snapshot_started.notify_waiters();
      self.snapshot_release.notified().await;
    }
    Ok(self.snapshot.lock().clone())
  }
}

struct BlockingMembershipStore {
  inner: InMemoryConsumerGroupMembershipStore,
  block_heartbeats: AtomicBool,
  fail_heartbeats: AtomicBool,
  fail_deregistration: AtomicBool,
  heartbeat_calls: AtomicUsize,
  heartbeat_started: Arc<tokio::sync::Notify>,
  heartbeat_release: Arc<tokio::sync::Notify>,
}

struct BlockingBlobStore {
  inner: InMemoryBlobStore,
  block_reads: AtomicBool,
  read_started: tokio::sync::Notify,
  read_release: tokio::sync::Notify,
}

struct FailingReadBlobStore {
  inner: InMemoryBlobStore,
  failed_reads: AtomicUsize,
}

struct RecordingMetadataStore {
  inner: InMemoryMetadataStore,
  scans: AtomicUsize,
  scanned_windows: Mutex<Vec<i64>>,
}

struct FailingLeaseStore {
  inner: InMemoryConsumerGroupLeaseStore,
}

struct PartiallyFailingLeaseStore {
  inner: InMemoryConsumerGroupLeaseStore,
  failures_enabled: AtomicBool,
}

impl FailingLeaseStore {
  fn new() -> Self {
    Self {
      inner: InMemoryConsumerGroupLeaseStore::new(),
    }
  }
}

impl PartiallyFailingLeaseStore {
  fn new() -> Self {
    Self {
      inner: InMemoryConsumerGroupLeaseStore::new(),
      failures_enabled: AtomicBool::new(false),
    }
  }
}

#[async_trait::async_trait]
impl ConsumerGroupLeaseStore for FailingLeaseStore {
  async fn list_group_leases(
    &self,
    topic: &str,
    group_id: &str,
  ) -> anyhow::Result<Vec<ConsumerGroupLease>> {
    Err(anyhow::anyhow!(
      "injected lease lookup failure for {topic}/{group_id}"
    ))
  }

  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
  ) -> anyhow::Result<ConsumerGroupAssignmentOutcome> {
    self
      .inner
      .assign_partition(key, owner_id, generation, now, lease_duration)
      .await
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    committed_cursor: Option<CommittedCursor>,
  ) -> anyhow::Result<ConsumerGroupHeartbeatOutcome> {
    self
      .inner
      .heartbeat_partition(
        key,
        owner_id,
        generation,
        now,
        lease_duration,
        committed_cursor,
      )
      .await
  }

  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    committed_cursor: CommittedCursor,
  ) -> anyhow::Result<ConsumerGroupCommitOutcome> {
    self
      .inner
      .commit_cursor(key, owner_id, generation, now, committed_cursor)
      .await
  }

  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
  ) -> anyhow::Result<ConsumerGroupReleaseOutcome> {
    self
      .inner
      .release_partition(key, owner_id, generation, now)
      .await
  }
}

#[async_trait::async_trait]
impl ConsumerGroupLeaseStore for PartiallyFailingLeaseStore {
  async fn list_group_leases(
    &self,
    topic: &str,
    group_id: &str,
  ) -> anyhow::Result<Vec<ConsumerGroupLease>> {
    self.inner.list_group_leases(topic, group_id).await
  }

  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
  ) -> anyhow::Result<ConsumerGroupAssignmentOutcome> {
    self
      .inner
      .assign_partition(key, owner_id, generation, now, lease_duration)
      .await
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    committed_cursor: Option<CommittedCursor>,
  ) -> anyhow::Result<ConsumerGroupHeartbeatOutcome> {
    if self.failures_enabled.load(Ordering::SeqCst) {
      if key.virtual_partition_id == 0 {
        return Ok(ConsumerGroupHeartbeatOutcome::HeldByOther(
          ConsumerGroupLease {
            key: key.clone(),
            owner_id: "member-b".to_string(),
            generation: generation.saturating_add(1),
            lease_expiration_ts_ms: unix_millis_from_offset_datetime(
              now.saturating_add(lease_duration),
            )
            .unwrap(),
            last_heartbeat_ts_ms: unix_millis_from_offset_datetime(now).unwrap(),
            committed_cursor: None,
            committed_ts_ms: None,
          },
        ));
      }
      if key.virtual_partition_id == 1 {
        return Err(anyhow::anyhow!("injected heartbeat failure"));
      }
    }
    self
      .inner
      .heartbeat_partition(
        key,
        owner_id,
        generation,
        now,
        lease_duration,
        committed_cursor,
      )
      .await
  }

  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
    committed_cursor: CommittedCursor,
  ) -> anyhow::Result<ConsumerGroupCommitOutcome> {
    self
      .inner
      .commit_cursor(key, owner_id, generation, now, committed_cursor)
      .await
  }

  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now: OffsetDateTime,
  ) -> anyhow::Result<ConsumerGroupReleaseOutcome> {
    self
      .inner
      .release_partition(key, owner_id, generation, now)
      .await
  }
}

impl BlockingBlobStore {
  fn new() -> Self {
    Self {
      inner: InMemoryBlobStore::new(),
      block_reads: AtomicBool::new(false),
      read_started: tokio::sync::Notify::new(),
      read_release: tokio::sync::Notify::new(),
    }
  }
}

impl FailingReadBlobStore {
  fn new() -> Self {
    Self {
      inner: InMemoryBlobStore::new(),
      failed_reads: AtomicUsize::new(0),
    }
  }
}

#[async_trait::async_trait]
impl BlobStore for BlockingBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> anyhow::Result<()> {
    self.inner.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    if self.block_reads.load(Ordering::SeqCst) {
      self.read_started.notify_waiters();
      self.read_release.notified().await;
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

#[async_trait::async_trait]
impl BlobStore for FailingReadBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> anyhow::Result<()> {
    self.inner.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    self.failed_reads.fetch_add(1, Ordering::SeqCst);
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("injected blob read failure for {key:?} at {range:?}"),
    })
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    let _ = admission;
    self.failed_reads.fetch_add(1, Ordering::SeqCst);
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("injected blob cache-admission read failure for {key:?}"),
    })
  }
}

#[async_trait::async_trait]
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
  ) -> anyhow::Result<Vec<SegmentMetadata>> {
    self.scans.fetch_add(1, Ordering::SeqCst);
    self
      .scanned_windows
      .lock()
      .push(window.window_start_unix_seconds);
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await
  }
}

impl BlockingMembershipStore {
  fn new() -> Self {
    Self {
      inner: InMemoryConsumerGroupMembershipStore::new(),
      block_heartbeats: AtomicBool::new(false),
      fail_heartbeats: AtomicBool::new(false),
      fail_deregistration: AtomicBool::new(false),
      heartbeat_calls: AtomicUsize::new(0),
      heartbeat_started: Arc::new(tokio::sync::Notify::new()),
      heartbeat_release: Arc::new(tokio::sync::Notify::new()),
    }
  }
}

#[async_trait::async_trait]
impl ConsumerGroupMembershipStore for BlockingMembershipStore {
  async fn register_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    pod_id: Option<String>,
    now: OffsetDateTime,
    ttl: TimeDuration,
  ) -> anyhow::Result<()> {
    self
      .inner
      .register_member(topic, group_id, member_id, pod_id, now, ttl)
      .await
  }

  async fn heartbeat_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    pod_id: Option<String>,
    now: OffsetDateTime,
    ttl: TimeDuration,
  ) -> anyhow::Result<()> {
    self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
    if self.fail_heartbeats.load(Ordering::SeqCst) {
      return Err(anyhow::anyhow!("injected membership heartbeat failure"));
    }
    if self.block_heartbeats.load(Ordering::SeqCst) {
      self.heartbeat_started.notify_waiters();
      self.heartbeat_release.notified().await;
    }
    self
      .inner
      .heartbeat_member(topic, group_id, member_id, pod_id, now, ttl)
      .await
  }

  async fn deregister_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
  ) -> anyhow::Result<()> {
    if self.fail_deregistration.load(Ordering::SeqCst) {
      return Err(anyhow::anyhow!("injected deregistration failure"));
    }
    self
      .inner
      .deregister_member(topic, group_id, member_id)
      .await
  }

  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now: OffsetDateTime,
  ) -> anyhow::Result<Vec<ConsumerGroupMember>> {
    self.inner.list_active_members(topic, group_id, now).await
  }

  async fn get_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
  ) -> anyhow::Result<Option<ConsumerGroupAssignmentPlan>> {
    self.inner.get_assignment_plan(topic, group_id).await
  }

  async fn get_planner_lease(
    &self,
    topic: &str,
    group_id: &str,
  ) -> anyhow::Result<Option<ConsumerGroupPlannerLease>> {
    self.inner.get_planner_lease(topic, group_id).await
  }

  async fn acquire_or_renew_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
    now: OffsetDateTime,
    ttl: TimeDuration,
  ) -> anyhow::Result<ConsumerGroupPlannerLeaseOutcome> {
    self
      .inner
      .acquire_or_renew_planner(topic, group_id, member_id, planner_session_id, now, ttl)
      .await
  }

  async fn release_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
  ) -> anyhow::Result<bool> {
    self
      .inner
      .release_planner(topic, group_id, member_id, planner_session_id)
      .await
  }

  async fn publish_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
    now: OffsetDateTime,
    plan: ConsumerGroupAssignmentPlan,
  ) -> anyhow::Result<bool> {
    self
      .inner
      .publish_assignment_plan(topic, group_id, member_id, planner_session_id, now, plan)
      .await
  }
}

impl MutableCoordinationSource {
  fn new(snapshot: CoordinationSnapshot) -> Self {
    Self {
      snapshot: Arc::new(Mutex::new(snapshot)),
    }
  }

  fn update(&self, snapshot: CoordinationSnapshot) {
    *self.snapshot.lock() = snapshot;
  }
}

#[async_trait::async_trait]
impl ConsumerCoordinationSource for MutableCoordinationSource {
  async fn snapshot(&self) -> anyhow::Result<CoordinationSnapshot> {
    Ok(self.snapshot.lock().clone())
  }
}

#[test]
fn current_batch_for_fenced_partition_is_not_delivered() {
  let mut delivery_state = DeliveryState {
    current_batch: Some(BufferedBatch {
      virtual_partition_id: 7,
      next_offset: 1,
      source_checkpoint: CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 1,
      },
      remaining_payload_bytes: 1,
      records: vec![new_record(vec![1], 0)].into_iter(),
    }),
    ..Default::default()
  };

  assert!(
    delivery_state
      .try_take_next(
        &mut HashMap::new(),
        &ConsumerIteratorMetrics::new(&metrics_scope())
      )
      .is_none()
  );
  assert!(delivery_state.current_batch.is_none());
}

#[test]
fn current_batch_remaining_bytes_decrease_as_records_are_delivered() {
  let mut delivery_state = DeliveryState {
    current_batch: Some(BufferedBatch {
      virtual_partition_id: 7,
      next_offset: 1,
      source_checkpoint: CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 1,
      },
      remaining_payload_bytes: 3,
      records: vec![new_record(vec![1, 2, 3], 0)].into_iter(),
    }),
    ..Default::default()
  };

  let mut active_partitions = HashMap::from([(7, ActivePartitionState::default())]);
  assert!(matches!(
    delivery_state.try_take_next(
      &mut active_partitions,
      &ConsumerIteratorMetrics::new(&metrics_scope())
    ),
    Some(NextResult::Record(_))
  ));
  assert_eq!(delivery_state.retained_bytes(), 0);
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
  metadata_published_ts_ms: i64,
) {
  let batch = RecordBatch::new(virtual_partition_id, records.clone());
  let payload = StoredRecordBatch {
    virtual_partition_id,
    records,
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap();
  let blob_key = BlobKey::new(format!("{topic}/{window_start}/{snowflake_id}.bin"));

  blob_store
    .put(&blob_key, Bytes::from(payload.clone()))
    .await
    .unwrap();

  let summary = batch.summary().unwrap();
  metadata_store
    .write_segment(
      SegmentMetadata::new(
        TopicWindowKey {
          topic: topic.to_string(),
          window_start_unix_seconds: window_start,
        },
        SnowflakeId(snowflake_id),
        blob_key,
        Compression::none(),
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
        OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(metadata_published_ts_ms),
        OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(metadata_published_ts_ms),
      ),
      None,
      0,
    )
    .await
    .unwrap();
}

fn runtime_config() -> ConsumerRuntimeConfig {
  runtime_config_with_prefetch_max_bytes(None)
}

fn runtime_config_with_prefetch_max_bytes(
  prefetch_max_bytes: Option<u64>,
) -> ConsumerRuntimeConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".to_string().into();
  read.prefetch_max_bytes = prefetch_max_bytes;

  let mut group = ConsumerGroupConfig::new();
  group.topic = "telemetry".to_string().into();
  group.group_id = "group-a".to_string().into();
  group.member_id = "member-a".to_string().into();
  group.lease_duration = TimeDuration::seconds(1).into_proto();
  group.heartbeat_interval = TimeDuration::milliseconds(10).into_proto();
  group.rebalance_interval = TimeDuration::milliseconds(10).into_proto();

  let mut runtime = ConsumerRuntimeConfig::new();
  runtime.read = Some(read).into();
  runtime.group = Some(group).into();
  runtime
}

async fn build_iterator_with_clock_skew(
  maximum_clock_skew: TimeDuration,
) -> anyhow::Result<ConsumerIteratorImpl> {
  let runtime = runtime_config();
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  ConsumerIteratorBuilder::new(
    &runtime,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    Arc::new(InMemoryConsumerGroupLeaseStore::new()),
    Arc::new(InMemoryConsumerGroupMembershipStore::new()),
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .maximum_clock_skew(maximum_clock_skew)
  .build()
  .await
}

#[tokio::test]
async fn iterator_builder_rejects_negative_clock_skew() {
  let result = build_iterator_with_clock_skew(-TimeDuration::nanoseconds(1)).await;
  let Err(error) = result else {
    panic!("iterator builder accepted negative maximum clock skew");
  };

  assert!(
    error
      .to_string()
      .contains("consumer maximum clock skew must not be negative")
  );
}

#[tokio::test]
async fn idle_prefetch_worker_processes_hydration_command_without_clock_advance() {
  let time_provider = Arc::new(ManualTimeProvider::new(time::OffsetDateTime::UNIX_EPOCH));
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let concrete_lease_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> = concrete_lease_store.clone();
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![3],
  }));
  let coordination_source: Arc<dyn ConsumerCoordinationSource> = source.clone();
  let mut iterator = ConsumerIteratorBuilder::new(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    coordination_source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .time_provider(time_provider.clone())
  .build()
  .await
  .unwrap();

  iterator.start().unwrap();
  time_provider.wait_until_sleeping(2).await;

  let key = ConsumerGroupLeaseKey {
    topic: "telemetry".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 3,
  };
  concrete_lease_store
    .heartbeat_partition(
      &key,
      "member-a",
      1,
      OffsetDateTime::UNIX_EPOCH,
      TimeDuration::milliseconds(1_000),
      Some(CommittedCursor {
        virtual_partition_id: 3,
        seq_end: 42,
        source_checkpoint: Some(CommittedSourceCheckpoint {
          window_start_unix_seconds: 0,
          snowflake_id: 1,
        }),
      }),
    )
    .await
    .unwrap();

  source.update(CoordinationSnapshot {
    members: vec!["member-a".to_string(), "member-b".to_string()],
    virtual_partitions: vec![3],
  });
  time_provider.advance(time::Duration::milliseconds(10));
  timeout(Duration::from_secs(1), async {
    loop {
      let snapshot = iterator
        .diagnostics()
        .expect("consumer implementation provides diagnostics")
        .state_snapshot();
      if local_partition(&snapshot, 3).cursor == Some(42) {
        return;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("idle prefetch worker did not process the hydration command");

  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn visibility_deferred_empty_scan_waits_until_metadata_is_eligible() {
  let time_provider = Arc::new(ManualTimeProvider::new(
    time::OffsetDateTime::from_unix_timestamp(902).unwrap(),
  ));
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore {
    inner: InMemoryMetadataStore::new(),
    scans: AtomicUsize::new(0),
    scanned_windows: Mutex::new(Vec::new()),
  });
  let metadata_store_for_iterator: Arc<dyn MetadataStore> = metadata_store.clone();
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut runtime = runtime_config();
  runtime.read.as_mut().unwrap().metadata_visibility_delay = TimeDuration::seconds(1).into_proto();
  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    3,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 902_000)],
    902_000,
  )
  .await;

  let mut iterator = ConsumerIteratorBuilder::new(
    &runtime,
    blob_store,
    metadata_store_for_iterator,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .time_provider(time_provider.clone())
  .build()
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[3]).await;
  // The prefetch worker has scanned the deferred row and registered its exact visibility wait.
  // The coordination task accounts for the other manual-clock sleeper.
  timeout(Duration::from_secs(1), async {
    while metadata_store.scans.load(Ordering::SeqCst) < 1 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("expected initial metadata scan");
  time_provider.wait_until_sleeping(2).await;
  assert_eq!(metadata_store.scans.load(Ordering::SeqCst), 1);

  // The generic idle backoff is shorter than one second. Advancing to just before eligibility
  // proves the worker uses the visibility deadline instead of repeatedly scanning that tail.
  time_provider.advance(time::Duration::milliseconds(999));
  for _ in 0 .. 10 {
    tokio::task::yield_now().await;
  }
  assert_eq!(metadata_store.scans.load(Ordering::SeqCst), 1);

  // At the rounded deadline, the deferred row becomes visible and the retry can deliver it.
  time_provider.advance(time::Duration::milliseconds(1));
  timeout(Duration::from_secs(1), async {
    while metadata_store.scans.load(Ordering::SeqCst) < 2 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("expected metadata rescan at the visibility deadline");
  let next = timeout(Duration::from_secs(1), iterator.next())
    .await
    .unwrap()
    .unwrap();
  assert!(matches!(next, NextResult::Record(_)));

  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn iterator_builder_applies_configured_clock_skew_to_reader_scan_horizon() {
  let time_provider = Arc::new(ManualTimeProvider::new(
    OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(2_999),
  ));
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore {
    inner: InMemoryMetadataStore::new(),
    scans: AtomicUsize::new(0),
    scanned_windows: Mutex::new(Vec::new()),
  });
  let metadata_store_for_iterator: Arc<dyn MetadataStore> = metadata_store.clone();
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut runtime = runtime_config();
  let read = runtime.read.as_mut().unwrap();
  read.max_clock_skew = TimeDuration::milliseconds(1_001).into_proto();
  read.strongly_consistent_metadata_reads = Some(true);
  let maximum_clock_skew = consumer_max_clock_skew(read);

  let mut iterator = ConsumerIteratorBuilder::new(
    &runtime,
    blob_store,
    metadata_store_for_iterator,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    TimeDuration::ZERO,
    None,
  )
  .maximum_clock_skew(maximum_clock_skew)
  .metadata_window_size(TimeDuration::seconds(1))
  .time_provider(time_provider.clone())
  .build()
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[3]).await;
  timeout(Duration::from_secs(1), async {
    loop {
      if !metadata_store.scanned_windows.lock().is_empty() {
        return;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("expected the configured reader to scan metadata");
  time_provider.wait_until_sleeping(2).await;
  time_provider.advance(TimeDuration::milliseconds(250));
  timeout(Duration::from_secs(1), async {
    while metadata_store.scanned_windows.lock().len() < 3 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("expected the fast reader scan to cover the configured skew horizon");
  let mut scanned_windows = metadata_store.scanned_windows.lock().clone();
  scanned_windows.sort_unstable();
  scanned_windows.dedup();
  assert_eq!(scanned_windows, vec![2, 3]);

  Box::new(iterator).shutdown().await.unwrap();
}

async fn wait_for_prefetch_buffer_len(iterator: &ConsumerIteratorImpl, expected_min: usize) {
  for _ in 0 .. 40 {
    let len = iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .prefetch_buffered_batch_count;
    if len >= expected_min {
      return;
    }
    sleep(Duration::from_millis(25)).await;
  }

  let len = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot()
    .prefetch_buffered_batch_count;
  assert!(
    len >= expected_min,
    "prefetch buffer length {len} did not reach expected minimum {expected_min}"
  );
}

async fn wait_for_active_assignment(iterator: &ConsumerIteratorImpl, expected_partitions: &[u32]) {
  for _ in 0 .. 40 {
    let snapshot = iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot();
    if active_partition_ids(&snapshot) == expected_partitions {
      return;
    }
    sleep(Duration::from_millis(25)).await;
  }

  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  panic!(
    "active assignment {:?} did not reach expected assignment {expected_partitions:?}",
    active_partition_ids(&snapshot)
  );
}

async fn wait_for_reader_mode(
  iterator: &ConsumerIteratorImpl,
  virtual_partition_id: VirtualPartitionId,
  expected_mode: ConsumerPartitionReadMode,
) {
  for _ in 0 .. 40 {
    let snapshot = iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot();
    if local_partition(&snapshot, virtual_partition_id)
      .reader
      .as_ref()
      .is_some_and(|reader| reader.mode == expected_mode)
    {
      return;
    }
    sleep(Duration::from_millis(25)).await;
  }

  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert_eq!(
    local_partition(&snapshot, virtual_partition_id)
      .reader
      .as_ref()
      .map(|reader| reader.mode.clone()),
    Some(expected_mode)
  );
}

async fn wait_for_pending_revocation(iterator: &ConsumerIteratorImpl) {
  timeout(Duration::from_secs(1), async {
    loop {
      if iterator
        .diagnostics()
        .expect("consumer implementation provides diagnostics")
        .state_snapshot()
        .local
        .pending_revocation
      {
        return;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("expected pending revocation");
}

#[tokio::test]
async fn failed_membership_heartbeats_fence_at_lease_deadline() {
  let time_provider = Arc::new(ManualTimeProvider::new(time::OffsetDateTime::UNIX_EPOCH));
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store = Arc::new(BlockingMembershipStore::new());
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![0],
    }));
  let mut iterator = ConsumerIteratorBuilder::new(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store.clone(),
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .time_provider(time_provider.clone())
  .build()
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0]).await;
  time_provider.wait_until_sleeping(2).await;
  membership_store
    .fail_heartbeats
    .store(true, Ordering::SeqCst);

  time_provider.advance(time::Duration::milliseconds(999));
  timeout(Duration::from_secs(1), async {
    while membership_store.heartbeat_calls.load(Ordering::SeqCst) < 2 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("expected membership heartbeat failure before lease expiry");
  assert!(
    !iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .pending_revocation
  );

  time_provider.advance(time::Duration::milliseconds(1));
  wait_for_pending_revocation(&iterator).await;
  let NextResult::Revoked(revoked) = iterator.next().await.unwrap() else {
    panic!("expected revocation at membership lease deadline");
  };
  assert_eq!(revoked.partitions(), &[0]);
  revoked.complete().await;
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_heartbeat_failure_revokes_fenced_partition() {
  let time_provider = Arc::new(ManualTimeProvider::new(time::OffsetDateTime::UNIX_EPOCH));
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store = Arc::new(PartiallyFailingLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![0, 1],
    }));
  let mut iterator = ConsumerIteratorBuilder::new(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store.clone(),
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .time_provider(time_provider.clone())
  .build()
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0, 1]).await;
  iterator
    .shared_state
    .lock()
    .diagnostics
    .last_committed_cursors
    .insert(
      0,
      ConsumerCommittedCursorSnapshot {
        offset: 1,
        source_checkpoint: None,
        committed_at_ms: None,
      },
    );
  time_provider.wait_until_sleeping(2).await;
  lease_store.failures_enabled.store(true, Ordering::SeqCst);

  time_provider.advance(time::Duration::milliseconds(10));
  wait_for_pending_revocation(&iterator).await;
  let NextResult::Revoked(revoked) = iterator.next().await.unwrap() else {
    panic!("expected fenced partition revocation");
  };
  assert_eq!(revoked.partitions(), &[0]);
  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert_eq!(local_partition(&snapshot, 0).last_committed_offset, None);
  revoked.complete().await;
  Box::new(iterator).shutdown().await.unwrap();
}

fn active_partition_ids(snapshot: &ConsumerStateSnapshot) -> Vec<VirtualPartitionId> {
  snapshot
    .local
    .partitions
    .iter()
    .filter(|partition| partition.active)
    .map(|partition| partition.virtual_partition_id)
    .collect()
}

fn local_partition(
  snapshot: &ConsumerStateSnapshot,
  virtual_partition_id: VirtualPartitionId,
) -> &ConsumerLocalPartitionSnapshot {
  snapshot
    .local
    .partitions
    .iter()
    .find(|partition| partition.virtual_partition_id == virtual_partition_id)
    .unwrap()
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_consumer_test")
}

#[tokio::test]
async fn lifecycle_hook_gates_commit_until_released() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0],
  }));
  let (entered_tx, entered_rx) = oneshot::channel();
  let (release_tx, release_rx) = oneshot::channel();
  let hooks: Arc<dyn ConsumerLifecycleHooks> = Arc::new(CommitGateHooks {
    entered: Mutex::new(Some(entered_tx)),
    release: Mutex::new(Some(release_rx)),
  });

  let mut iterator = ConsumerIteratorBuilder::new(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .time_provider(Arc::new(SystemTimeProvider))
  .lifecycle_hooks(hooks)
  .build()
  .await
  .unwrap();
  iterator.start().unwrap();

  let mut commit = Box::pin(iterator.commit());
  let waker = Waker::noop();
  let mut context = Context::from_waker(waker);
  assert!(matches!(commit.as_mut().poll(&mut context), Poll::Pending));
  timeout(Duration::from_secs(1), entered_rx)
    .await
    .expect("commit did not reach the lifecycle hook")
    .expect("commit lifecycle hook sender was dropped");

  release_tx.send(()).unwrap();
  timeout(Duration::from_secs(1), commit.as_mut())
    .await
    .expect("commit did not finish after lifecycle hook release")
    .unwrap();
  drop(commit);
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn diagnostics_report_assignment_and_start_state() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0, 1],
  }));

  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  let diagnostics = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics");
  let snapshot = diagnostics.state_snapshot();
  assert_eq!(snapshot.generated_at.offset(), UtcOffset::UTC);
  assert_eq!(snapshot.topic, "telemetry");
  assert_eq!(snapshot.group_id, "group-a");
  assert_eq!(snapshot.member_id, "member-a");
  assert!(!snapshot.started);
  assert_eq!(active_partition_ids(&snapshot), vec![0, 1]);
  assert_eq!(
    snapshot
      .assignment_plan
      .as_ref()
      .map(|plan| (plan.version, plan.assignments.len())),
    Some((1, 2))
  );
  assert_eq!(snapshot.prefetch_buffered_batch_count, 0);
  assert_eq!(snapshot.prefetch_buffered_record_count, 0);
  assert_eq!(snapshot.prefetch_pending_batch_count, 0);
  assert_eq!(snapshot.prefetch_pending_record_count, 0);
  assert_eq!(snapshot.prefetch_pending_bytes, 0);
  assert_eq!(snapshot.local.partitions.len(), 2);
  assert!(snapshot.local.partitions.iter().all(|partition| {
    partition.owned
      && partition.active
      && !partition.pending_assignment
      && partition.last_scan.is_none()
      && partition.reader.as_ref().is_some_and(|reader| {
        reader.mode == ConsumerPartitionReadMode::Fresh
          && reader.recovery_next_window_start.is_some()
          && reader.recovery_cutover_window_start.is_none()
      })
  }));

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0, 1]).await;
  let started_snapshot = diagnostics.state_snapshot();
  assert!(started_snapshot.started);
  assert!(started_snapshot.prefetch_worker_running);

  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn state_response_includes_fresh_group_leases_and_other_member_commits() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let concrete_lease_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> = concrete_lease_store.clone();
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string(), "member-b".to_string()],
    virtual_partitions: vec![0, 1],
  }));
  let iterator = ConsumerIteratorImpl::from_config(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();
  let now_ts_ms = now_unix_millis();
  let member_b_key = ConsumerGroupLeaseKey {
    topic: "telemetry".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 1,
  };
  concrete_lease_store
    .assign_partition(
      member_b_key.clone(),
      "member-b".to_string(),
      7,
      offset_datetime_from_unix_millis(now_ts_ms),
      TimeDuration::milliseconds(10_000),
    )
    .await
    .unwrap();
  concrete_lease_store
    .heartbeat_partition(
      &member_b_key,
      "member-b",
      7,
      offset_datetime_from_unix_millis(now_ts_ms + 1),
      TimeDuration::milliseconds(10_000),
      Some(CommittedCursor {
        virtual_partition_id: 1,
        seq_end: 42,
        source_checkpoint: Some(CommittedSourceCheckpoint {
          window_start_unix_seconds: 1_000,
          snowflake_id: 99,
        }),
      }),
    )
    .await
    .unwrap();

  let response = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_response()
    .await;
  assert_eq!(
    response
      .state
      .assignment_plan
      .as_ref()
      .map(|plan| plan.version),
    Some(1)
  );
  let ConsumerGroupLeaseObservation::Fresh { partitions } = response.group_lease_observation else {
    panic!("expected fresh group lease observation");
  };
  assert_eq!(partitions.len(), 2);
  let member_b_partition = partitions
    .iter()
    .find(|partition| partition.virtual_partition_id == 1)
    .unwrap();
  assert_eq!(
    member_b_partition.desired_owner_id.as_deref(),
    Some("member-b")
  );
  assert_eq!(member_b_partition.owner_id.as_deref(), Some("member-b"));
  assert_eq!(member_b_partition.generation, Some(7));
  assert_eq!(member_b_partition.committed_offset, Some(42));
  assert_eq!(
    member_b_partition.committed_source_checkpoint,
    Some(ConsumerSourceCheckpointSnapshot {
      window_start: offset_datetime_from_unix_millis(1_000_000),
      snowflake_id: 99,
    })
  );
  let member_a_partition = partitions
    .iter()
    .find(|partition| partition.virtual_partition_id == 0)
    .unwrap();
  assert_eq!(
    member_a_partition.desired_owner_id.as_deref(),
    Some("member-a")
  );
  assert_eq!(member_a_partition.owner_id.as_deref(), Some("member-a"));
}

#[tokio::test]
async fn state_response_reports_lease_lookup_failure_without_blocking_local_diagnostics() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(FailingLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0],
  }));
  let iterator = ConsumerIteratorImpl::from_config(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();
  let diagnostics = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics");
  let response = diagnostics.state_response().await;

  assert!(matches!(
    response.group_lease_observation,
    ConsumerGroupLeaseObservation::LookupFailed { .. }
  ));
}

#[test]
fn group_lease_observation_includes_unleased_plan_partitions() {
  let plan = ConsumerAssignmentPlanSnapshot {
    version: 1,
    planner_member_id: "member-a".to_string(),
    policy: ConsumerAssignmentPolicy::FlatMember,
    members: vec!["member-a".to_string(), "member-b".to_string()],
    member_topology: vec![],
    pod_loads: vec![],
    assignments: vec![
      ConsumerPartitionAssignmentSnapshot {
        virtual_partition_id: 0,
        member_id: "member-a".to_string(),
        pod_id: None,
      },
      ConsumerPartitionAssignmentSnapshot {
        virtual_partition_id: 1,
        member_id: "member-b".to_string(),
        pod_id: None,
      },
    ],
    published_at: datetime!(2026-07-17 0:00 UTC),
  };
  let observation = crate::diagnostics::group_lease_observation(
    Some(&plan),
    vec![ConsumerGroupLease {
      key: ConsumerGroupLeaseKey {
        topic: "telemetry".to_string(),
        group_id: "group-a".to_string(),
        virtual_partition_id: 1,
      },
      owner_id: "member-b".to_string(),
      generation: 7,
      lease_expiration_ts_ms: 20_000,
      last_heartbeat_ts_ms: 10_000,
      committed_cursor: None,
      committed_ts_ms: None,
    }],
  );
  let ConsumerGroupLeaseObservation::Fresh { partitions } = observation else {
    panic!("expected fresh group lease observation");
  };
  assert_eq!(partitions.len(), 2);
  assert_eq!(partitions[0].virtual_partition_id, 0);
  assert_eq!(partitions[0].desired_owner_id.as_deref(), Some("member-a"));
  assert!(partitions[0].owner_id.is_none());
}

#[tokio::test]
async fn assignment_callback_replays_active_partitions() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0, 1],
  }));
  let assigned_partitions = Arc::new(AtomicUsize::new(0));

  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  let assignment_count = Arc::clone(&assigned_partitions);
  iterator.set_assignment_callback(Arc::new(move |partitions| {
    assignment_count.fetch_add(partitions.len(), Ordering::SeqCst);
  }));

  assert_eq!(assigned_partitions.load(Ordering::SeqCst), 2);
  Box::new(iterator).shutdown().await.unwrap();
}

#[test]
fn idle_poll_backoff_exponential_with_max_and_reset() {
  let mut backoff = IdlePollBackoff::new(
    TimeDuration::milliseconds(250),
    Some(TimeDuration::seconds(2)),
  );

  assert_eq!(backoff.next_delay(), TimeDuration::milliseconds(250));
  assert_eq!(backoff.next_delay(), TimeDuration::milliseconds(500));
  assert_eq!(backoff.next_delay(), TimeDuration::seconds(1));
  assert_eq!(backoff.next_delay(), TimeDuration::seconds(2));
  assert_eq!(backoff.next_delay(), TimeDuration::seconds(2));

  backoff.reset();
  assert_eq!(backoff.next_delay(), TimeDuration::milliseconds(250));
}

#[test]
fn idle_poll_backoff_with_base_max_remains_constant() {
  let mut backoff = IdlePollBackoff::new(
    TimeDuration::milliseconds(250),
    Some(TimeDuration::milliseconds(250)),
  );

  assert_eq!(backoff.next_delay(), TimeDuration::milliseconds(250));
  assert_eq!(backoff.next_delay(), TimeDuration::milliseconds(250));
  assert_eq!(backoff.next_delay(), TimeDuration::milliseconds(250));
}

#[tokio::test]
async fn next_returns_revocation_until_completed() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());

  let runtime = runtime_config();
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0, 1],
  }));

  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source.clone(),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0, 1]).await;

  source.update(CoordinationSnapshot {
    members: vec!["member-a".to_string(), "member-b".to_string()],
    virtual_partitions: vec![0, 1],
  });

  for _ in 0 .. 40 {
    if iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .pending_revocation
    {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }
  assert!(
    iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .pending_revocation,
    "expected autonomous rebalance to request revocation"
  );

  let revoked = iterator.next().await.unwrap();
  let revoked = match revoked {
    NextResult::Revoked(revoked) => revoked,
    NextResult::Record(_) => panic!("expected revocation callback"),
  };

  let revoked_partitions = revoked.partitions();
  assert_eq!(revoked_partitions.len(), 1);

  assert!(
    timeout(Duration::from_millis(50), iterator.next())
      .await
      .is_err()
  );

  revoked.complete().await;
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn next_does_not_lose_notification_between_state_check_and_wait() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0],
  }));
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();
  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0]).await;

  let (state_checked_tx, state_checked_rx) = oneshot::channel();
  let (release_tx, release_rx) = oneshot::channel();
  iterator.set_next_after_delivery_state_check_hook(state_checked_tx, release_rx);
  let shared_state = Arc::clone(&iterator.shared_state);
  let delivery_notify = Arc::clone(&iterator.delivery_notify);
  let mut next = Box::pin(iterator.next());
  let waker = Waker::noop();
  let mut context = Context::from_waker(waker);
  assert!(matches!(next.as_mut().poll(&mut context), Poll::Pending));
  state_checked_rx.await.unwrap();
  shared_state.lock().terminal_error = Some("injected test terminal error".to_string());
  delivery_notify.notify_waiters();
  release_tx.send(()).unwrap();

  let Err(error) = timeout(Duration::from_secs(1), next.as_mut())
    .await
    .unwrap()
  else {
    panic!("expected the injected terminal error");
  };
  assert!(error.to_string().contains("injected test terminal error"));
  drop(next);
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn commit_during_revocation_persists_revoked_partition_cursor() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let concrete_lease_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> = concrete_lease_store.clone();
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let now_window = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_secs()
    .try_into()
    .unwrap_or(i64::MAX)
    / 300)
    * 300;
  for virtual_partition_id in [0, 1] {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      now_window,
      u64::from(virtual_partition_id) + 1,
      virtual_partition_id,
      SeqRange { start: 1, end: 1 },
      vec![new_record(
        vec![u8::try_from(virtual_partition_id).unwrap()],
        now_window * 1_000,
      )],
    )
    .await;
  }

  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0, 1],
  }));
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source.clone(),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0, 1]).await;
  for _ in 0 .. 2 {
    let next = timeout(Duration::from_secs(1), iterator.next())
      .await
      .unwrap()
      .unwrap();
    let record = match next {
      NextResult::Record(record) => record,
      NextResult::Revoked(_) => panic!("expected record before revocation"),
    };
    iterator
      .store_offset(record.virtual_partition_id, record.offset)
      .unwrap();
  }

  source.update(CoordinationSnapshot {
    members: vec!["member-a".to_string(), "member-b".to_string()],
    virtual_partitions: vec![0, 1],
  });
  let revoked = timeout(Duration::from_secs(1), async {
    loop {
      let next = iterator.next().await.unwrap();
      if let NextResult::Revoked(revoked) = next {
        return revoked;
      }
    }
  })
  .await
  .unwrap();
  let revoked_partition_id = revoked.partitions()[0];

  iterator.commit().await.unwrap();
  let lease = concrete_lease_store
    .list_group_leases("telemetry", "group-a")
    .await
    .unwrap()
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == revoked_partition_id)
    .unwrap();
  assert_eq!(
    lease.committed_cursor.map(|cursor| cursor.seq_end),
    Some(1),
    "commit before revocation completion must persist the revoked partition cursor"
  );

  revoked.complete().await;
  timeout(Duration::from_secs(1), async {
    loop {
      let snapshot = iterator
        .diagnostics()
        .expect("consumer implementation provides diagnostics")
        .state_snapshot();
      if snapshot
        .local
        .partitions
        .iter()
        .all(|partition| partition.virtual_partition_id != revoked_partition_id)
      {
        return;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("revoked partition remained in local diagnostics");
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn next_delivers_records_and_commit_renews() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());

  let now_window = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_secs()
    .try_into()
    .unwrap_or(i64::MAX)
    / 300)
    * 300;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    now_window,
    1,
    3,
    SeqRange { start: 1, end: 2 },
    vec![
      new_record(vec![1, 2, 3], now_window * 1_000),
      new_record(vec![4, 5, 6], now_window * 1_000 + 1),
    ],
  )
  .await;

  let runtime = runtime_config();
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let collector = Collector::default();
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    collector.scope("blob_stream_consumer_test"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();

  let next = iterator.next().await.unwrap();
  let record = match next {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected batch"),
  };
  assert_eq!(record.virtual_partition_id, 3);
  assert_eq!(record.offset, 1);
  assert_eq!(record.record.payload.to_vec(), vec![1, 2, 3]);

  let next = iterator.next().await.unwrap();
  let record = match next {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected record"),
  };
  assert_eq!(record.virtual_partition_id, 3);
  assert_eq!(record.offset, 2);
  assert_eq!(record.record.payload.to_vec(), vec![4, 5, 6]);

  iterator
    .store_offset(record.virtual_partition_id, record.offset)
    .unwrap();
  let report = iterator.commit().await.unwrap();
  assert_eq!(report.renewed_partitions, vec![3]);
  assert!(report.fenced_partitions.is_empty());

  let state = iterator.diagnostics.state_snapshot();
  let partition = local_partition(&state, 3);
  assert_eq!(partition.pending_commit_offset, Some(2));
  assert_eq!(partition.last_committed_offset, Some(2));
  assert!(partition.last_committed_source_checkpoint.is_some());
  assert!(partition.last_committed_at.is_some());
  assert!(state.local.last_successful_heartbeat_at.is_some());
  let metrics = String::from_utf8(collector.prometheus_output()).unwrap();
  assert!(
    metrics.contains("blob_stream_consumer_test:consumer:iterator:cursor_commit_partitions 1"),
    "{metrics}"
  );
}

#[tokio::test]
async fn scheduled_heartbeats_do_not_depend_on_next_polling() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let concrete_membership_store = Arc::new(BlockingMembershipStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> = concrete_membership_store.clone();
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![0],
    }));
  let mut runtime = runtime_config();
  runtime.group.as_mut().unwrap().heartbeat_interval = TimeDuration::milliseconds(25).into_proto();
  runtime.group.as_mut().unwrap().rebalance_interval = TimeDuration::seconds(60).into_proto();
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  timeout(Duration::from_secs(1), async {
    while concrete_membership_store
      .heartbeat_calls
      .load(Ordering::SeqCst)
      == 0
    {
      sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();
  let heartbeat_calls_before = concrete_membership_store
    .heartbeat_calls
    .load(Ordering::SeqCst);

  timeout(Duration::from_secs(1), async {
    while concrete_membership_store
      .heartbeat_calls
      .load(Ordering::SeqCst)
      <= heartbeat_calls_before
    {
      sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();

  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn seek_waits_for_prefetch_scan_without_stalling_heartbeats() {
  let blob_store = Arc::new(BlockingBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let concrete_membership_store = Arc::new(BlockingMembershipStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> = concrete_membership_store.clone();
  let now_window = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_secs()
    .try_into()
    .unwrap_or(i64::MAX)
    / 300)
    * 300;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    now_window,
    1,
    3,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], now_window * 1_000)],
  )
  .await;
  blob_store.block_reads.store(true, Ordering::SeqCst);
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime_config(),
    blob_store.clone(),
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  let read_started = blob_store.read_started.notified();
  iterator.start().unwrap();
  timeout(Duration::from_secs(1), read_started).await.unwrap();

  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert!(snapshot.started);
  assert_eq!(active_partition_ids(&snapshot), vec![3]);

  let heartbeat_calls_before = concrete_membership_store
    .heartbeat_calls
    .load(Ordering::SeqCst);
  let mut seek = Box::pin(iterator.seek(
    3,
    ConsumerSeekTarget {
      offset: 0,
      window_start_unix_seconds: now_window,
      snowflake_id: None,
    },
  ));
  assert!(
    timeout(Duration::from_millis(50), seek.as_mut())
      .await
      .is_err(),
    "seek completed before the blocked prefetch scan reached its command boundary"
  );
  timeout(Duration::from_secs(1), async {
    loop {
      if concrete_membership_store
        .heartbeat_calls
        .load(Ordering::SeqCst)
        > heartbeat_calls_before
      {
        return;
      }
      sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();

  blob_store.read_release.notify_waiters();
  timeout(Duration::from_secs(1), seek)
    .await
    .unwrap()
    .unwrap();
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_releases_owned_partitions_when_deregistration_fails() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let concrete_lease_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> = concrete_lease_store.clone();
  let concrete_membership_store = Arc::new(BlockingMembershipStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> = concrete_membership_store.clone();

  let runtime = runtime_config();
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  assert_eq!(
    concrete_membership_store
      .inner
      .get_planner_lease("telemetry", "group-a")
      .await
      .unwrap()
      .map(|lease| lease.member_id),
    Some("member-a".to_string())
  );
  concrete_membership_store
    .fail_deregistration
    .store(true, Ordering::SeqCst);
  Box::new(iterator).shutdown().await.unwrap();
  assert!(
    concrete_membership_store
      .inner
      .get_planner_lease("telemetry", "group-a")
      .await
      .unwrap()
      .is_none()
  );
  assert_eq!(
    concrete_membership_store
      .inner
      .acquire_or_renew_planner(
        "telemetry",
        "group-a",
        "member-b",
        "member-b-session",
        offset_datetime_from_unix_millis(now_unix_millis()),
        TimeDuration::milliseconds(30_000)
      )
      .await
      .unwrap(),
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );

  let now_ts_ms = i64::try_from(
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis(),
  )
  .unwrap_or(i64::MAX);
  let key = ConsumerGroupLeaseKey {
    topic: "telemetry".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 3,
  };
  let reassigned = concrete_lease_store
    .assign_partition(
      key,
      "member-b".to_string(),
      2,
      offset_datetime_from_unix_millis(now_ts_ms),
      TimeDuration::milliseconds(1_000),
    )
    .await
    .unwrap();
  assert!(matches!(
    reassigned,
    ConsumerGroupAssignmentOutcome::Assigned { .. }
  ));
}

#[tokio::test]
async fn seek_interrupts_prefetch_read_retries_without_clock_advance() {
  let time_provider = Arc::new(ManualTimeProvider::new(
    time::OffsetDateTime::from_unix_timestamp(305).unwrap(),
  ));
  let blob_store = Arc::new(FailingReadBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    300,
    1,
    3,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 300_000)],
  )
  .await;
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut iterator = ConsumerIteratorBuilder::new(
    &runtime_config(),
    blob_store.clone(),
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .time_provider(time_provider.clone())
  .build()
  .await
  .unwrap();

  iterator.start().unwrap();
  timeout(Duration::from_secs(1), async {
    while blob_store.failed_reads.load(Ordering::SeqCst) < 2 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("prefetch worker did not retry the injected read failure");
  time_provider.wait_until_sleeping(2).await;

  timeout(
    Duration::from_secs(1),
    iterator.seek(
      3,
      ConsumerSeekTarget {
        offset: 0,
        window_start_unix_seconds: 300,
        snowflake_id: None,
      },
    ),
  )
  .await
  .expect("seek did not interrupt the prefetch retry backoff")
  .unwrap();
  Box::new(iterator).shutdown().await.unwrap();
}

#[test]
fn shutdown_span_reports_success_after_all_work_completes() {
  let (spans, ()) = bd_log::test::with_two_phase_test_otel("blob-stream-consumer-test", async {
    let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
    let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
    let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
      Arc::new(InMemoryConsumerGroupLeaseStore::new());
    let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
      Arc::new(InMemoryConsumerGroupMembershipStore::new());
    let source: Arc<dyn ConsumerCoordinationSource> =
      Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
        members: vec!["member-a".to_string()],
        virtual_partitions: vec![3],
      }));
    let mut iterator = ConsumerIteratorImpl::from_config(
      &runtime_config(),
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      source,
      rejecting_broker_metadata_query(),
      rejecting_broker_blob_range_query(),
      metrics_scope(),
      TimeDuration::days(1),
      DEFAULT_MAX_METADATA_PUBLICATION_LAG,
      None,
    )
    .await
    .unwrap();

    let mut driver = iterator
      .driver
      .take()
      .expect("iterator should own a driver");
    driver.start().unwrap();
    driver.shutdown().await.unwrap();
  });

  let shutdown_span = spans
    .iter()
    .find(|span| span.name == "blob_stream.consumer.shutdown")
    .expect("shutdown span should be exported");
  let attribute = |key: &str| {
    shutdown_span
      .attributes
      .iter()
      .find(|attribute| attribute.key.as_str() == key)
      .map(|attribute| attribute.value.as_str())
  };
  assert_eq!(
    attribute("shutdown.commit_outcome").as_deref(),
    Some("succeeded")
  );
  assert_eq!(
    attribute("shutdown.lease_release_outcome").as_deref(),
    Some("succeeded")
  );
  assert_eq!(
    attribute("shutdown.deregistration_outcome").as_deref(),
    Some("succeeded")
  );
  assert_eq!(
    attribute("shutdown.planner_release_outcome").as_deref(),
    Some("succeeded")
  );
  assert_eq!(format!("{:?}", shutdown_span.status), "Ok");
}

#[test]
fn shutdown_span_reports_best_effort_cleanup_failure() {
  let (spans, ()) = bd_log::test::with_two_phase_test_otel("blob-stream-consumer-test", async {
    let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
    let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
    let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
      Arc::new(InMemoryConsumerGroupLeaseStore::new());
    let concrete_membership_store = Arc::new(BlockingMembershipStore::new());
    let membership_store: Arc<dyn ConsumerGroupMembershipStore> = concrete_membership_store.clone();
    let source: Arc<dyn ConsumerCoordinationSource> =
      Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
        members: vec!["member-a".to_string()],
        virtual_partitions: vec![3],
      }));
    let mut iterator = ConsumerIteratorImpl::from_config(
      &runtime_config(),
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      source,
      rejecting_broker_metadata_query(),
      rejecting_broker_blob_range_query(),
      metrics_scope(),
      TimeDuration::days(1),
      DEFAULT_MAX_METADATA_PUBLICATION_LAG,
      None,
    )
    .await
    .unwrap();

    let mut driver = iterator
      .driver
      .take()
      .expect("iterator should own a driver");
    driver.start().unwrap();
    concrete_membership_store
      .fail_deregistration
      .store(true, Ordering::SeqCst);
    driver.shutdown().await.unwrap();
  });

  let shutdown_span = spans
    .iter()
    .find(|span| span.name == "blob_stream.consumer.shutdown")
    .expect("shutdown span should be exported");
  let attribute = |key: &str| {
    shutdown_span
      .attributes
      .iter()
      .find(|attribute| attribute.key.as_str() == key)
      .map(|attribute| attribute.value.as_str())
  };
  assert_eq!(
    attribute("shutdown.deregistration_outcome").as_deref(),
    Some("failed")
  );
  assert!(format!("{:?}", shutdown_span.status).starts_with("Error"));
  assert!(
    attribute("error.message")
      .is_some_and(|message| { message.contains("membership deregistration") })
  );
}

#[test]
fn revocation_handoff_span_reports_success_after_reassignment() {
  let (spans, ()) = bd_log::test::with_two_phase_test_otel("blob-stream-consumer-test", async {
    let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
    let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
    let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
      Arc::new(InMemoryConsumerGroupLeaseStore::new());
    let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
      Arc::new(InMemoryConsumerGroupMembershipStore::new());
    let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![0, 1],
    }));
    let hooks = Arc::new(RebalanceRecordingHooks {
      rebalance_applied_calls: AtomicUsize::new(0),
    });
    let mut iterator = ConsumerIteratorBuilder::new(
      &runtime_config(),
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      source.clone(),
      rejecting_broker_metadata_query(),
      rejecting_broker_blob_range_query(),
      metrics_scope(),
      TimeDuration::days(1),
      DEFAULT_MAX_METADATA_PUBLICATION_LAG,
      None,
    )
    .lifecycle_hooks(hooks.clone())
    .build()
    .await
    .unwrap();

    let mut driver = iterator
      .driver
      .take()
      .expect("iterator should own a driver");
    driver.start().unwrap();
    source.update(CoordinationSnapshot {
      members: vec!["member-a".to_string(), "member-b".to_string()],
      virtual_partitions: vec![0, 1],
    });
    driver
      .maybe_rebalance(offset_datetime_from_unix_millis(
        now_unix_millis().saturating_add(1_000),
      ))
      .await
      .unwrap();

    let revocation = driver
      .shared_state
      .lock()
      .delivery_state
      .pending_revocation
      .take()
      .expect("rebalance should request partition revocation");
    let NextResult::Revoked(revocation) = revocation else {
      panic!("expected revocation callback");
    };
    revocation.complete().await;
    assert!(
      driver
        .finish_pending_revocation_if_completed()
        .await
        .unwrap()
    );
    assert_eq!(hooks.rebalance_applied_calls.load(Ordering::Relaxed), 1);
    driver.shutdown().await.unwrap();
  });

  let handoff_span = spans
    .iter()
    .find(|span| span.name == "blob_stream.consumer.revocation_handoff")
    .expect("revocation handoff span should be exported");
  let attribute = |key: &str| {
    handoff_span
      .attributes
      .iter()
      .find(|attribute| attribute.key.as_str() == key)
      .map(|attribute| attribute.value.as_str())
  };
  assert_eq!(
    attribute("handoff.lease_release_outcome").as_deref(),
    Some("succeeded")
  );
  assert_eq!(
    attribute("handoff.assignment_outcome").as_deref(),
    Some("succeeded")
  );
  assert_eq!(format!("{:?}", handoff_span.status), "Ok");
}

#[tokio::test]
async fn seek_discards_prefetched_records_slices_the_resume_batch_and_rewinds_fast_frontier() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());

  let now_window = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_secs()
    .try_into()
    .unwrap_or(i64::MAX)
    / 300)
    * 300;

  for snowflake_id in 1 ..= 3 {
    let payload_byte = u8::try_from(snowflake_id).unwrap();
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      now_window,
      snowflake_id,
      7,
      SeqRange {
        start: (snowflake_id - 1) * 2 + 1,
        end: snowflake_id * 2,
      },
      vec![
        new_record(vec![payload_byte; 6], now_window * 1_000),
        new_record(vec![payload_byte; 6], now_window * 1_000 + 1),
      ],
    )
    .await;
  }

  // One batch carries 12 payload bytes, so this budget should hold only one batch at a time.
  let runtime = runtime_config_with_prefetch_max_bytes(Some(16));
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![7],
    }));

  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();
  iterator.start().unwrap();

  wait_for_prefetch_buffer_len(&iterator, 1).await;
  let buffered_before = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert_eq!(buffered_before.prefetch_buffered_batch_count, 1);
  assert_eq!(buffered_before.prefetch_buffered_record_count, 2);
  assert_eq!(buffered_before.prefetch_pending_batch_count, 0);
  assert_eq!(buffered_before.prefetch_pending_record_count, 0);

  let first = timeout(Duration::from_secs(2), iterator.next())
    .await
    .unwrap()
    .unwrap();
  let first_record = match first {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected batch"),
  };
  assert_eq!(first_record.virtual_partition_id, 7);
  assert_eq!(first_record.offset, 1);

  let in_flight_snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert_eq!(
    local_partition(&in_flight_snapshot, 7).prefetch_buffered_record_count,
    in_flight_snapshot.prefetch_buffered_record_count
  );

  timeout(
    Duration::from_secs(2),
    iterator.seek(
      7,
      ConsumerSeekTarget {
        offset: 1,
        window_start_unix_seconds: now_window,
        snowflake_id: None,
      },
    ),
  )
  .await
  .unwrap()
  .unwrap();
  assert_eq!(iterator.metrics.seeks.get(), 1);
  let rewound = timeout(Duration::from_secs(2), iterator.next())
    .await
    .unwrap()
    .unwrap();
  let rewound_record = match rewound {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected rewound record"),
  };
  assert_eq!(rewound_record.virtual_partition_id, 7);
  assert_eq!(rewound_record.offset, 2);
  for expected_offset in 3 ..= 6 {
    let next = timeout(Duration::from_secs(2), iterator.next())
      .await
      .unwrap()
      .unwrap();
    let NextResult::Record(record) = next else {
      panic!("expected replayed record");
    };
    assert_eq!(record.offset, expected_offset);
  }
  wait_for_reader_mode(&iterator, 7, ConsumerPartitionReadMode::Fast).await;
  let recovered_snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert_eq!(
    local_partition(&recovered_snapshot, 7)
      .reader
      .as_ref()
      .unwrap()
      .mode,
    ConsumerPartitionReadMode::Fast
  );

  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn seek_end_to_end_replays_current_and_earlier_windows_with_source_checkpoints() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let now_window = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_secs()
    .try_into()
    .unwrap_or(i64::MAX)
    / 300)
    * 300;
  let previous_window = now_window - 300;
  let previous_snowflake = SnowflakeId::minimum_for_timestamp(
    OffsetDateTime::from_unix_timestamp(previous_window + 1).unwrap(),
  );
  let current_snowflake = SnowflakeId::minimum_for_timestamp(
    OffsetDateTime::from_unix_timestamp(now_window + 1).unwrap(),
  );

  for (window_start, snowflake_id, seq_range) in [
    (
      previous_window,
      previous_snowflake.as_u64(),
      SeqRange { start: 1, end: 3 },
    ),
    (
      now_window,
      current_snowflake.as_u64(),
      SeqRange { start: 4, end: 6 },
    ),
  ] {
    let records = (seq_range.start ..= seq_range.end)
      .map(|offset| new_record(vec![u8::try_from(offset).unwrap(); 6], window_start * 1_000))
      .collect();
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      window_start,
      snowflake_id,
      7,
      seq_range,
      records,
    )
    .await;
  }

  // Each encoded batch carries 18 payload bytes, so a 16-byte prefetch limit exercises the
  // single-oversized-batch admission path before seek trims the delivered suffix.
  let runtime = runtime_config_with_prefetch_max_bytes(Some(16));
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![7],
    }));
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();
  iterator.start().unwrap();
  wait_for_prefetch_buffer_len(&iterator, 1).await;

  timeout(
    Duration::from_secs(2),
    iterator.seek(
      7,
      ConsumerSeekTarget {
        offset: 4,
        window_start_unix_seconds: now_window,
        snowflake_id: Some(current_snowflake.as_u64()),
      },
    ),
  )
  .await
  .unwrap()
  .unwrap();
  let mut current_window_replay = Vec::new();
  for _ in 0 .. 2 {
    let NextResult::Record(record) = timeout(Duration::from_secs(2), iterator.next())
      .await
      .unwrap()
      .unwrap()
    else {
      panic!("expected replayed current-window record");
    };
    current_window_replay.push(record.offset);
  }
  assert_eq!(current_window_replay, vec![5, 6]);

  timeout(
    Duration::from_secs(2),
    iterator.seek(
      7,
      ConsumerSeekTarget {
        offset: 0,
        window_start_unix_seconds: previous_window,
        snowflake_id: Some(previous_snowflake.as_u64()),
      },
    ),
  )
  .await
  .unwrap()
  .unwrap();
  let mut earlier_window_replay = Vec::new();
  for _ in 0 .. 6 {
    let NextResult::Record(record) = timeout(Duration::from_secs(2), iterator.next())
      .await
      .unwrap()
      .unwrap()
    else {
      panic!("expected replayed record after earlier-window seek");
    };
    earlier_window_replay.push(record.offset);
  }
  assert_eq!(earlier_window_replay, vec![1, 2, 3, 4, 5, 6]);

  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn revocation_drops_buffered_batches_for_revoked_partitions() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());

  let now_window = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_secs()
    .try_into()
    .unwrap_or(i64::MAX)
    / 300)
    * 300;

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    now_window,
    1,
    0,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], now_window * 1_000)],
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    now_window,
    2,
    1,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![2], now_window * 1_000)],
  )
  .await;

  let runtime = runtime_config();
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0, 1],
  }));

  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source.clone(),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_prefetch_buffer_len(&iterator, 2).await;

  source.update(CoordinationSnapshot {
    members: vec!["member-a".to_string(), "member-b".to_string()],
    virtual_partitions: vec![0, 1],
  });

  for _ in 0 .. 40 {
    if iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .pending_revocation
    {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }
  assert!(
    iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .pending_revocation,
    "expected autonomous rebalance to request revocation"
  );

  let revoked = iterator.next().await.unwrap();
  let revoked = match revoked {
    NextResult::Revoked(revoked) => revoked,
    NextResult::Record(_) => panic!("expected revocation callback"),
  };
  let revoked_partitions = revoked.partitions();
  assert_eq!(revoked_partitions.len(), 1);
  let revoked_partition = revoked_partitions[0];

  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert!(
    snapshot
      .local
      .partitions
      .iter()
      .find(|partition| partition.virtual_partition_id == revoked_partition)
      .is_none_or(|partition| partition.prefetch_buffered_batch_count == 0),
    "revoked partition batch was not removed from prefetch buffer"
  );

  revoked.complete().await;
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_next_does_not_restart_scheduled_heartbeat() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store = Arc::new(BlockingMembershipStore::new());
  membership_store
    .block_heartbeats
    .store(true, Ordering::SeqCst);
  let heartbeat_started = membership_store.heartbeat_started.clone();

  let mut runtime = runtime_config();
  runtime.group.as_mut().unwrap().heartbeat_interval = TimeDuration::seconds(60).into_proto();
  runtime.group.as_mut().unwrap().rebalance_interval = TimeDuration::seconds(60).into_proto();
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![0],
    }));
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store.clone(),
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  let heartbeat_wait = heartbeat_started.notified();
  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0]).await;
  timeout(Duration::from_secs(1), heartbeat_wait)
    .await
    .unwrap();

  for _ in 0 .. 8 {
    tokio::select! {
      () = tokio::task::yield_now() => {},
      _ = iterator.next() => panic!("cancelled poll unexpectedly completed"),
    }
  }

  membership_store
    .block_heartbeats
    .store(false, Ordering::SeqCst);
  membership_store.heartbeat_release.notify_waiters();
  for _ in 0 .. 40 {
    let snapshot = iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot();
    if snapshot.local.last_successful_heartbeat_at.is_some() {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }

  assert_eq!(membership_store.heartbeat_calls.load(Ordering::SeqCst), 1);
  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert!(snapshot.local.last_successful_heartbeat_at.is_some());
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_next_does_not_restart_rebalance() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(BlockingCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0],
  }));

  let mut runtime = runtime_config();
  runtime.group.as_mut().unwrap().heartbeat_interval = TimeDuration::seconds(60).into_proto();
  runtime.group.as_mut().unwrap().rebalance_interval = TimeDuration::seconds(60).into_proto();
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source.clone(),
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0]).await;
  source.block_snapshots.store(true, Ordering::SeqCst);
  let rebalance_started = source.snapshot_started.notified();

  tokio::select! {
    () = rebalance_started => {},
    _ = iterator.next() => panic!("cancelled poll unexpectedly completed"),
  }

  source.snapshot_release.notify_waiters();
  for _ in 0 .. 40 {
    if source.snapshot_calls.load(Ordering::SeqCst) == 2 {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }

  assert_eq!(source.snapshot_calls.load(Ordering::SeqCst), 2);
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_next_preserves_prefetched_record() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let now_window = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_secs()
    .try_into()
    .unwrap_or(i64::MAX)
    / 300)
    * 300;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    now_window,
    1,
    3,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1, 2, 3], now_window * 1_000)],
  )
  .await;

  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut iterator = ConsumerIteratorImpl::from_config(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    rejecting_broker_metadata_query(),
    rejecting_broker_blob_range_query(),
    metrics_scope(),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_prefetch_buffer_len(&iterator, 1).await;
  for _ in 0 .. 40 {
    if iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .delivery_state
      == ConsumerDeliveryState::Pending
    {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }
  assert_eq!(
    iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .delivery_state,
    ConsumerDeliveryState::Pending
  );

  tokio::select! {
    biased;
    () = async {} => {},
    _ = iterator.next() => panic!("cancelled poll unexpectedly received the ready record"),
  }

  let next = timeout(Duration::from_secs(1), iterator.next())
    .await
    .unwrap()
    .unwrap();
  let record = match next {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected record"),
  };
  assert_eq!(record.virtual_partition_id, 3);
  assert_eq!(record.offset, 1);
  assert_eq!(record.record.payload.to_vec(), vec![1, 2, 3]);
  Box::new(iterator).shutdown().await.unwrap();
}
