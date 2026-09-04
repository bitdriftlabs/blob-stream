#![allow(clippy::unwrap_used)]

use super::allocation::{
  AllocationTransitionDecision,
  LeaseExpirationUpdate,
  begin_allocation_transition,
};
use super::state::WriteState;
use super::{
  BrokerLifecycleHooks,
  TopicInfo,
  WriteConfig,
  WriteEngine,
  WriteEngineBuilder,
  WriteEngineImpl,
  WriteError,
  WriteRequest,
};
use anyhow::Result;
use async_trait::async_trait;
use bd_runtime_config::loader::Loader;
use bd_server_stats::stats::{Collector, Scope};
use bd_server_stats::test::util::stats::Helper;
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers_core::feature_flags::{DefaultFeatureFlags, FakeLoader};
use bd_time::TimeProvider;
use blob_stream_blob_store::{
  BlobCacheAdmission,
  BlobKey,
  BlobStore,
  BlobStoreError,
  BlobStoreResult,
  ByteRange,
  InMemoryBlobStore,
};
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  InMemoryConsumerGroupLeaseStore,
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  MetadataReadConsistency,
  MetadataStore,
  MetadataWriteResult,
  ProducerPartitionFence,
  ProducerPartitionLease,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  ProducerSequenceProgress,
  SegmentMetadata,
  SequenceReservationOutcome,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{
  CommittedCursor,
  CommittedSourceCheckpoint,
  CompressionCodec,
  SeqRange,
  SnowflakeId,
  VirtualPartitionId,
  Window,
  new_record,
  offset_datetime_from_unix_millis,
};
use bytes::Bytes;
use prometheus::labels;
use protobuf::Chars;
use serde_json::to_value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration as StdDuration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::{Semaphore, mpsc, watch};

const DEFAULT_TEST_METADATA_WINDOW_SIZE: TimeDuration = TimeDuration::minutes(5);

struct GatedBlobStore {
  entered_tx: mpsc::UnboundedSender<String>,
  release_first: Arc<Semaphore>,
  first_write: AtomicBool,
}

struct BlockingBlobStore {
  entered_tx: mpsc::UnboundedSender<String>,
  release: Arc<Semaphore>,
}

struct NotifyingBlobStore {
  entered_tx: mpsc::UnboundedSender<String>,
}

struct NotifyingAssignmentHook {
  published_tx: mpsc::UnboundedSender<()>,
}

//
// NotifyingLeaseLifecycleHook
//

/// Signals assignment publication and the one-time transition from draining to release retry.
struct NotifyingLeaseLifecycleHook {
  published_tx: mpsc::UnboundedSender<()>,
  before_release_tx: mpsc::UnboundedSender<()>,
}

struct GatedFirstMetadataStore {
  inner: Arc<InMemoryMetadataStore>,
  entered_tx: mpsc::UnboundedSender<()>,
  release_first: Arc<Semaphore>,
  first_write: AtomicBool,
}

struct BlockingFirstMetadataStore {
  inner: Arc<InMemoryMetadataStore>,
  entered_tx: mpsc::UnboundedSender<()>,
  release_first: Arc<Semaphore>,
  first_write: AtomicBool,
}

struct GatedReservationLeaseStore {
  inner: InMemoryProducerPartitionLeaseStore,
  entered_tx: mpsc::UnboundedSender<()>,
  release_first: Arc<Semaphore>,
  first_reservation: AtomicBool,
}

//
// GatedLeaseAcquireStore
//

/// Blocks one lease acquisition after the caller enables the gate.
struct GatedLeaseAcquireStore {
  inner: Arc<InMemoryProducerPartitionLeaseStore>,
  entered_tx: mpsc::UnboundedSender<()>,
  release: Arc<Semaphore>,
  acquire_and_reserve_attempts: AtomicUsize,
  block_next: AtomicBool,
}

//
// GatedLeaseReleaseStore
//

/// Blocks one lease release after the caller enables the gate.
struct GatedLeaseReleaseStore {
  inner: Arc<InMemoryProducerPartitionLeaseStore>,
  entered_tx: mpsc::UnboundedSender<()>,
  failed_tx: Option<mpsc::UnboundedSender<()>>,
  release: Arc<Semaphore>,
  block_next: AtomicBool,
  fail_next: AtomicBool,
}

struct FailsTopicMetadataStore {
  failed_topic: String,
  inner: Arc<InMemoryMetadataStore>,
}

#[async_trait]
impl MetadataStore for FailsTopicMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> MetadataWriteResult {
    if metadata.window.topic == self.failed_topic {
      return Err(anyhow::anyhow!("metadata write failed").into());
    }
    self.inner.write_segment(metadata, fences, now_ts_ms).await
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &blob_stream_types::TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await
  }
}

#[async_trait]
impl BlobStore for GatedBlobStore {
  async fn put(&self, key: &BlobKey, _payload: Bytes) -> Result<()> {
    self
      .entered_tx
      .send(key.as_str().to_string())
      .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
    if self.first_write.swap(false, Ordering::SeqCst) {
      self
        .release_first
        .acquire()
        .await
        .map_err(|_| anyhow::anyhow!("test gate closed"))?
        .forget();
    }
    Ok(())
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("reads are not used by this test: {range:?}"),
    })
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    let _ = admission;
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("cache admission reads are not used by this test"),
    })
  }
}

#[async_trait]
impl BlobStore for BlockingBlobStore {
  async fn put(&self, key: &BlobKey, _payload: Bytes) -> Result<()> {
    self
      .entered_tx
      .send(key.as_str().to_string())
      .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
    self
      .release
      .acquire()
      .await
      .map_err(|_| anyhow::anyhow!("test gate closed"))?
      .forget();
    Ok(())
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("reads are not used by this test: {range:?}"),
    })
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    let _ = admission;
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("cache admission reads are not used by this test"),
    })
  }
}

#[async_trait]
impl BlobStore for NotifyingBlobStore {
  async fn put(&self, key: &BlobKey, _payload: Bytes) -> Result<()> {
    self
      .entered_tx
      .send(key.as_str().to_string())
      .map_err(|_| anyhow::anyhow!("test receiver dropped"))
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("reads are not used by this test: {range:?}"),
    })
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    let _ = admission;
    Err(BlobStoreError::Read {
      key: key.as_str().to_string(),
      source: anyhow::anyhow!("cache admission reads are not used by this test"),
    })
  }
}

#[async_trait]
impl BrokerLifecycleHooks for NotifyingAssignmentHook {
  async fn assignment_published(&self) {
    let _ignored = self.published_tx.send(());
  }
}

#[async_trait]
impl BrokerLifecycleHooks for NotifyingLeaseLifecycleHook {
  async fn assignment_published(&self) {
    let _ignored = self.published_tx.send(());
  }

  async fn before_lease_release(&self, _topic: &str, _virtual_partition_id: VirtualPartitionId) {
    let _ignored = self.before_release_tx.send(());
  }
}

#[async_trait]
impl MetadataStore for GatedFirstMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> MetadataWriteResult {
    if self.first_write.swap(false, Ordering::SeqCst) {
      self
        .entered_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
      self
        .release_first
        .acquire()
        .await
        .map_err(|_| anyhow::anyhow!("test gate closed"))?
        .forget();
      return Err(anyhow::anyhow!("metadata write failed").into());
    }
    self.inner.write_segment(metadata, fences, now_ts_ms).await
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &blob_stream_types::TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await
  }
}

#[async_trait]
impl MetadataStore for BlockingFirstMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    now_ts_ms: i64,
  ) -> MetadataWriteResult {
    if self.first_write.swap(false, Ordering::SeqCst) {
      self
        .entered_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
      self
        .release_first
        .acquire()
        .await
        .map_err(|_| anyhow::anyhow!("test gate closed"))?
        .forget();
    }
    self.inner.write_segment(metadata, fences, now_ts_ms).await
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &blob_stream_types::TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
    consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake, consistency)
      .await
  }
}

#[async_trait]
impl ProducerPartitionLeaseStore for GatedReservationLeaseStore {
  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>> {
    self.inner.get_lease(key).await
  }

  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireOutcome> {
    self
      .inner
      .acquire_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
  }

  async fn acquire_lease_and_reserve_sequences(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    reservation_size: Option<u64>,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireAndReserveOutcome> {
    if reservation_size.is_some() {
      self
        .entered_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
      if self.first_reservation.swap(false, Ordering::SeqCst) {
        self
          .release_first
          .acquire()
          .await
          .map_err(|_| anyhow::anyhow!("test gate closed"))?
          .forget();
      }
    }
    self
      .inner
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        reservation_size,
        sequence_progress,
      )
      .await
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseHeartbeatOutcome> {
    self
      .inner
      .heartbeat_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    reservation_size: u64,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<SequenceReservationOutcome> {
    self
      .entered_tx
      .send(())
      .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
    if self.first_reservation.swap(false, Ordering::SeqCst) {
      self
        .release_first
        .acquire()
        .await
        .map_err(|_| anyhow::anyhow!("test gate closed"))?
        .forget();
    }
    self
      .inner
      .reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        reservation_size,
        sequence_progress,
      )
      .await
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseReleaseOutcome> {
    self
      .inner
      .release_lease(key, holder_id, lease_session_id, now, sequence_progress)
      .await
  }
}

#[async_trait]
impl ProducerPartitionLeaseStore for GatedLeaseAcquireStore {
  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>> {
    self.inner.get_lease(key).await
  }

  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireOutcome> {
    if self.block_next.swap(false, Ordering::AcqRel) {
      self
        .entered_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
      self
        .release
        .acquire()
        .await
        .map_err(|_| anyhow::anyhow!("test gate closed"))?
        .forget();
    }
    self
      .inner
      .acquire_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
  }

  async fn acquire_lease_and_reserve_sequences(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    reservation_size: Option<u64>,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireAndReserveOutcome> {
    self
      .acquire_and_reserve_attempts
      .fetch_add(1, Ordering::AcqRel);
    if self.block_next.swap(false, Ordering::AcqRel) {
      self
        .entered_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
      self
        .release
        .acquire()
        .await
        .map_err(|_| anyhow::anyhow!("test gate closed"))?
        .forget();
    }
    self
      .inner
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        reservation_size,
        sequence_progress,
      )
      .await
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseHeartbeatOutcome> {
    self
      .inner
      .heartbeat_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    reservation_size: u64,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<SequenceReservationOutcome> {
    self
      .inner
      .reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        reservation_size,
        sequence_progress,
      )
      .await
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseReleaseOutcome> {
    self
      .inner
      .release_lease(key, holder_id, lease_session_id, now, sequence_progress)
      .await
  }
}

#[async_trait]
impl ProducerPartitionLeaseStore for GatedLeaseReleaseStore {
  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>> {
    self.inner.get_lease(key).await
  }

  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireOutcome> {
    self
      .inner
      .acquire_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
  }

  async fn acquire_lease_and_reserve_sequences(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    reservation_size: Option<u64>,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseAcquireAndReserveOutcome> {
    self
      .inner
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        reservation_size,
        sequence_progress,
      )
      .await
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    lease_duration: TimeDuration,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseHeartbeatOutcome> {
    self
      .inner
      .heartbeat_lease(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        sequence_progress,
      )
      .await
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    reservation_size: u64,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<SequenceReservationOutcome> {
    self
      .inner
      .reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        reservation_size,
        sequence_progress,
      )
      .await
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    sequence_progress: ProducerSequenceProgress,
  ) -> Result<LeaseReleaseOutcome> {
    if self.fail_next.swap(false, Ordering::AcqRel) {
      if let Some(failed_tx) = &self.failed_tx {
        failed_tx
          .send(())
          .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
      }
      return Err(anyhow::anyhow!("injected lease release failure"));
    }
    if self.block_next.swap(false, Ordering::AcqRel) {
      self
        .entered_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
      self
        .release
        .acquire()
        .await
        .map_err(|_| anyhow::anyhow!("test gate closed"))?
        .forget();
    }
    self
      .inner
      .release_lease(key, holder_id, lease_session_id, now, sequence_progress)
      .await
  }
}

fn std_duration(duration: TimeDuration) -> StdDuration {
  StdDuration::try_from(duration).unwrap()
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_broker_test")
}

fn state_with_local_telemetry_assignment() -> Arc<parking_lot::Mutex<WriteState>> {
  let mut state = WriteState::default();
  // These allocation-focused tests bypass discovery, so they construct the same authoritative
  // local assignment that a production membership update would publish before allocation.
  state.publish_assignment(&[("telemetry".into(), 0)]);
  Arc::new(parking_lot::Mutex::new(state))
}

fn make_engine_with_membership(
  time_provider: Arc<ManualTimeProvider>,
  membership_rx: watch::Receiver<BrokerMembership>,
  shutdown_trigger_handle: bd_shutdown::ComponentShutdownTriggerHandle,
  partition_count: u32,
) -> Result<(
  Arc<WriteEngineImpl>,
  Arc<InMemoryProducerPartitionLeaseStore>,
)> {
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let engine = make_engine_with_membership_and_lease_store(
    time_provider,
    membership_rx,
    shutdown_trigger_handle,
    partition_count,
    &(lease_store.clone() as Arc<dyn ProducerPartitionLeaseStore>),
    None,
  )?;
  Ok((engine, lease_store))
}

fn make_engine_with_membership_and_lease_store(
  time_provider: Arc<ManualTimeProvider>,
  membership_rx: watch::Receiver<BrokerMembership>,
  shutdown_trigger_handle: bd_shutdown::ComponentShutdownTriggerHandle,
  partition_count: u32,
  lease_store: &Arc<dyn ProducerPartitionLeaseStore>,
  lifecycle_hooks: Option<Arc<dyn BrokerLifecycleHooks>>,
) -> Result<Arc<WriteEngineImpl>> {
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  let topics = HashMap::from([(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count,
      num_writers: 1,
      retention: TimeDuration::days(7),
      max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
      metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
    },
  )]);
  let scope = metrics_scope();
  let mut builder = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    Arc::clone(lease_store),
    "node-a".to_string(),
    shutdown_trigger_handle,
    &scope,
  )
  .membership_rx(membership_rx)
  .time_provider(time_provider);
  if let Some(lifecycle_hooks) = lifecycle_hooks {
    builder = builder.lifecycle_hooks(lifecycle_hooks);
  }
  let engine = builder.build()?;
  Ok(Arc::new(engine))
}

#[tokio::test]
async fn produce_rejects_pending_and_foreign_membership_without_creating_a_lease() -> Result<()> {
  let now = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::default());
  let (engine, lease_store) = make_engine_with_membership(
    now.clone(),
    membership_rx,
    shutdown_trigger.make_handle(),
    1,
  )?;
  let request = || WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1], 1)],
  };
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };

  // Pending discovery and an initialized snapshot without this node are both non-authoritative
  // for local writes. Neither may create state that could later acquire a lease.
  assert!(matches!(
    engine.produce_batch(request()).await,
    Err(WriteError::NotLeaseHolder { .. })
  ));
  assert!(engine.state.lock().partition_keys().is_empty());
  assert!(lease_store.get_lease(&key).await?.is_none());

  membership_tx.send(BrokerMembership::new(Vec::new()))?;
  tokio::task::yield_now().await;
  assert!(matches!(
    engine.produce_batch(request()).await,
    Err(WriteError::NotLeaseHolder { .. })
  ));
  assert!(engine.state.lock().partition_keys().is_empty());
  assert!(lease_store.get_lease(&key).await?.is_none());

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  tokio::task::yield_now().await;
  assert!(matches!(
    engine.produce_batch(request()).await,
    Err(WriteError::NotLeaseHolder { .. })
  ));
  assert!(engine.state.lock().partition_keys().is_empty());
  assert!(lease_store.get_lease(&key).await?.is_none());

  Ok(())
}

#[tokio::test]
async fn produce_rejects_stale_local_allocation_after_membership_handoff() -> Result<()> {
  let now = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let (engine, _lease_store) =
    make_engine_with_membership(now, membership_rx, shutdown_trigger.make_handle(), 1)?;
  let request = || WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1], 1)],
  };

  // The initial snapshot is authoritative at construction, so this write installs a local
  // lease/reservation. The next membership update must fence that cached allocation immediately.
  engine.produce_batch(request()).await?;
  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  for _ in 0 .. 100 {
    if engine
      .state
      .lock()
      .assignment_generation_for_partition("telemetry", 0)
      .is_none()
    {
      break;
    }
    tokio::task::yield_now().await;
  }

  assert!(matches!(
    engine.produce_batch(request()).await,
    Err(WriteError::NotLeaseHolder { .. })
  ));
  Ok(())
}

#[tokio::test]
async fn foreground_acquire_completed_after_membership_handoff_is_released() -> Result<()> {
  let initial_time = offset_datetime_from_unix_millis(1_700_000_000_000);
  let time_provider = Arc::new(ManualTimeProvider::new(initial_time));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let inner = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let lease_store = Arc::new(GatedLeaseAcquireStore {
    inner: inner.clone(),
    entered_tx,
    release: Arc::clone(&release),
    acquire_and_reserve_attempts: AtomicUsize::new(0),
    block_next: AtomicBool::new(true),
  });
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };
  let engine = make_engine_with_membership_and_lease_store(
    time_provider.clone(),
    membership_rx,
    shutdown_trigger.make_handle(),
    1,
    &(lease_store.clone() as Arc<dyn ProducerPartitionLeaseStore>),
    None,
  )?;

  // The engine's initial maintenance acquisition must complete before the test expires its lease.
  // Gate that acquisition directly rather than assuming it has run after a fixed number of yields.
  entered_rx
    .recv()
    .await
    .expect("initial maintenance acquire did not reach its lease-store gate");
  release.add_permits(1);
  engine
    .produce_batch(WriteRequest {
      topic: "telemetry".into(),
      virtual_partition_id: 0,
      records: vec![new_record(vec![1], 1)],
    })
    .await?;
  let initial_lease = inner
    .get_lease(&key)
    .await?
    .expect("initial maintenance lease was not acquired");
  time_provider.advance(initial_lease.lease_expiration_at - time_provider.now());
  lease_store.block_next.store(true, Ordering::Release);

  let foreground_engine = Arc::clone(&engine);
  let foreground = tokio::spawn(async move {
    foreground_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 1)],
      })
      .await
  });
  entered_rx
    .recv()
    .await
    .expect("foreground acquire did not reach its lease-store gate");

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  for _ in 0 .. 100 {
    if engine
      .state
      .lock()
      .assignment_generation_for_partition("telemetry", 0)
      .is_none()
    {
      break;
    }
    tokio::task::yield_now().await;
  }
  assert!(
    engine
      .state
      .lock()
      .assignment_generation_for_partition("telemetry", 0)
      .is_none(),
    "membership handoff did not withdraw foreground authorization"
  );

  release.add_permits(1);
  assert!(matches!(
    foreground.await.expect("foreground task must not panic"),
    Err(WriteError::NotLeaseHolder { .. })
  ));
  for _ in 0 .. 100 {
    if engine
      .state
      .lock()
      .partition_state("telemetry", 0)
      .is_none()
    {
      break;
    }
    tokio::task::yield_now().await;
  }
  assert!(
    engine
      .state
      .lock()
      .partition_state("telemetry", 0)
      .is_none(),
    "handoff release did not retire the stale foreground state"
  );
  assert!(matches!(
    inner
      .acquire_lease(
        key,
        "node-b".to_string(),
        "session-b".to_string(),
        time_provider.now(),
        TimeDuration::seconds(60),
        ProducerSequenceProgress::default(),
      )
      .await?,
    LeaseAcquireOutcome::Acquired(_)
  ));

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn membership_handoff_publishes_while_maintenance_acquire_is_in_flight() -> Result<()> {
  let now = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let (acquire_entered_tx, mut acquire_entered_rx) = mpsc::unbounded_channel();
  let (published_tx, mut published_rx) = mpsc::unbounded_channel();
  let acquire_release = Arc::new(Semaphore::new(0));
  let lease_store = Arc::new(GatedLeaseAcquireStore {
    inner: Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    entered_tx: acquire_entered_tx,
    release: Arc::clone(&acquire_release),
    acquire_and_reserve_attempts: AtomicUsize::new(0),
    block_next: AtomicBool::new(true),
  });
  let engine = make_engine_with_membership_and_lease_store(
    now,
    membership_rx,
    shutdown_trigger.make_handle(),
    1,
    &(lease_store as Arc<dyn ProducerPartitionLeaseStore>),
    Some(Arc::new(NotifyingAssignmentHook { published_tx })),
  )?;

  published_rx
    .recv()
    .await
    .expect("initial membership assignment was not published");
  acquire_entered_rx
    .recv()
    .await
    .expect("initial maintenance acquire did not reach its gate");

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  published_rx
    .recv()
    .await
    .expect("handoff membership assignment was blocked by maintenance acquire");
  assert!(
    engine
      .state
      .lock()
      .assignment_generation_for_partition("telemetry", 0)
      .is_none(),
    "handoff did not revoke admission while maintenance acquire was blocked"
  );

  acquire_release.add_permits(1);
  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn maintenance_success_clears_retry_after_foreground_acquisition() -> Result<()> {
  let initial_time = offset_datetime_from_unix_millis(1_700_000_000_000);
  let time_provider = Arc::new(ManualTimeProvider::new(initial_time));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (_membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let (acquire_entered_tx, mut acquire_entered_rx) = mpsc::unbounded_channel();
  let acquire_release = Arc::new(Semaphore::new(0));
  let inner = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let lease_store = Arc::new(GatedLeaseAcquireStore {
    inner: inner.clone(),
    entered_tx: acquire_entered_tx,
    release: Arc::clone(&acquire_release),
    acquire_and_reserve_attempts: AtomicUsize::new(0),
    block_next: AtomicBool::new(false),
  });
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };
  inner
    .acquire_lease(
      key.clone(),
      "old-node".to_string(),
      "old-session".to_string(),
      initial_time,
      TimeDuration::seconds(60),
      ProducerSequenceProgress::default(),
    )
    .await?;
  let engine = make_engine_with_membership_and_lease_store(
    time_provider.clone(),
    membership_rx,
    shutdown_trigger.make_handle(),
    1,
    &(lease_store.clone() as Arc<dyn ProducerPartitionLeaseStore>),
    None,
  )?;

  // The initial maintenance attempt is held by the old owner and schedules a logical retry.
  time_provider.wait_until_sleeping(2).await;
  assert_eq!(
    lease_store
      .acquire_and_reserve_attempts
      .load(Ordering::Acquire),
    1
  );
  inner
    .release_lease(
      &key,
      "old-node",
      "old-session",
      initial_time,
      ProducerSequenceProgress::default(),
    )
    .await?;
  engine
    .produce_batch(WriteRequest {
      topic: "telemetry".into(),
      virtual_partition_id: 0,
      records: vec![new_record(vec![1], 1)],
    })
    .await?;
  assert_eq!(
    lease_store
      .acquire_and_reserve_attempts
      .load(Ordering::Acquire),
    2
  );

  // The due maintenance retry renews the foreground lease. Block it so the completion and any
  // incorrectly scheduled follow-up call are observed through lifecycle gates.
  lease_store.block_next.store(true, Ordering::Release);
  time_provider.advance(TimeDuration::milliseconds(250));
  acquire_entered_rx
    .recv()
    .await
    .expect("maintenance retry did not reach its lease-store gate");
  lease_store.block_next.store(true, Ordering::Release);
  acquire_release.add_permits(1);
  assert!(
    tokio::time::timeout(StdDuration::from_millis(1), acquire_entered_rx.recv())
      .await
      .is_err(),
    "successful maintenance renewal immediately retried a stale schedule"
  );
  assert_eq!(
    lease_store
      .acquire_and_reserve_attempts
      .load(Ordering::Acquire),
    3
  );

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn newer_membership_revokes_admission_while_previous_handoff_drains() -> Result<()> {
  let now = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let node_a = BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  };
  let node_b = BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  };
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![node_a.clone()]));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let (published_tx, mut published_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay = TimeDuration::seconds(60);
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 2,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
        },
      )]),
      Arc::new(BlockingBlobStore {
        entered_tx,
        release: Arc::clone(&release),
      }),
      Arc::new(InMemoryMetadataStore::new()),
      lease_store,
      "node-a".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .membership_rx(membership_rx)
    .time_provider(now)
    .lifecycle_hooks(Arc::new(NotifyingAssignmentHook { published_tx }))
    .build()?,
  );
  published_rx
    .recv()
    .await
    .expect("initial membership assignment was not published");

  let split_membership = BrokerMembership::new(vec![node_a, node_b.clone()]);
  let split_owned =
    WriteEngineImpl::owned_virtual_partitions(&engine.topics, 0, "node-a", &split_membership);
  let retained_partition = split_owned
    .iter()
    .find_map(|(topic, partition)| (topic.as_str() == "telemetry").then_some(*partition))
    .expect("node-a must retain one of two partitions after scale-out");
  let draining_partition = (0 .. 2)
    .find(|partition| *partition != retained_partition)
    .expect("node-a must hand off one of two partitions after scale-out");

  let produce_engine = Arc::clone(&engine);
  let produce = tokio::spawn(async move {
    produce_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: draining_partition,
        records: vec![new_record(vec![1], 1)],
      })
      .await
  });
  receive_blob_write(&mut entered_rx).await;

  membership_tx.send(split_membership)?;
  published_rx
    .recv()
    .await
    .expect("scale-out membership assignment was not published");
  wait_for_partition_draining_start(&engine, draining_partition).await;

  // The blocked drain must not delay consuming this new snapshot and revoking the retained
  // partition's admission. The lifecycle event establishes that the latest snapshot was applied.
  membership_tx.send(BrokerMembership::new(vec![node_b]))?;
  published_rx
    .recv()
    .await
    .expect("newer membership assignment was not published");
  assert!(
    engine
      .state
      .lock()
      .assignment_generation_for_partition("telemetry", retained_partition)
      .is_none(),
    "newer membership did not revoke the retained partition admission"
  );

  release.add_permits(1);
  produce.await??;
  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn reassignment_waits_for_in_flight_release_before_readmission() -> Result<()> {
  let now = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let node_a = BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  };
  let node_b = BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  };
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![node_a.clone()]));
  let (release_entered_tx, mut release_entered_rx) = mpsc::unbounded_channel();
  let (published_tx, mut published_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let lease_store = Arc::new(GatedLeaseReleaseStore {
    inner: Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    entered_tx: release_entered_tx,
    failed_tx: None,
    release: Arc::clone(&release),
    block_next: AtomicBool::new(true),
    fail_next: AtomicBool::new(false),
  });
  let engine = make_engine_with_membership_and_lease_store(
    now,
    membership_rx,
    shutdown_trigger.make_handle(),
    1,
    &(lease_store.clone() as Arc<dyn ProducerPartitionLeaseStore>),
    Some(Arc::new(NotifyingAssignmentHook { published_tx })),
  )?;
  let request = || WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1], 1)],
  };

  published_rx
    .recv()
    .await
    .expect("initial membership assignment was not published");
  engine.produce_batch(request()).await?;

  membership_tx.send(BrokerMembership::new(vec![node_b.clone()]))?;
  published_rx
    .recv()
    .await
    .expect("handoff membership assignment was not published");
  release_entered_rx
    .recv()
    .await
    .expect("lease release did not reach its gate");

  membership_tx.send(BrokerMembership::new(vec![node_a]))?;
  published_rx
    .recv()
    .await
    .expect("restored membership assignment was not published");
  assert!(matches!(
    engine.produce_batch(request()).await,
    Err(WriteError::NotLeaseHolder { .. })
  ));

  release.add_permits(1);
  published_rx
    .recv()
    .await
    .expect("release completion did not republish the restored assignment");
  engine.produce_batch(request()).await?;

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn reassignment_waits_for_terminal_release_retry_before_readmission() -> Result<()> {
  let now = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let node_a = BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  };
  let node_b = BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  };
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![node_a.clone()]));
  let (release_entered_tx, mut release_entered_rx) = mpsc::unbounded_channel();
  let (release_failed_tx, mut release_failed_rx) = mpsc::unbounded_channel();
  let (published_tx, mut published_rx) = mpsc::unbounded_channel();
  let (before_release_tx, mut before_release_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let lease_store = Arc::new(GatedLeaseReleaseStore {
    inner: Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    entered_tx: release_entered_tx,
    failed_tx: Some(release_failed_tx),
    release: Arc::clone(&release),
    block_next: AtomicBool::new(false),
    fail_next: AtomicBool::new(true),
  });
  let engine = make_engine_with_membership_and_lease_store(
    now.clone(),
    membership_rx,
    shutdown_trigger.make_handle(),
    1,
    &(lease_store.clone() as Arc<dyn ProducerPartitionLeaseStore>),
    Some(Arc::new(NotifyingLeaseLifecycleHook {
      published_tx,
      before_release_tx,
    })),
  )?;
  let request = || WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1], 1)],
  };

  published_rx
    .recv()
    .await
    .expect("initial membership assignment was not published");
  engine.produce_batch(request()).await?;

  let sleep_registrations = now.sleep_registration_count();
  membership_tx.send(BrokerMembership::new(vec![node_b]))?;
  published_rx
    .recv()
    .await
    .expect("handoff membership assignment was not published");
  release_failed_rx
    .recv()
    .await
    .expect("lease release did not fail at its test gate");
  before_release_rx
    .recv()
    .await
    .expect("drained partition did not begin lease release");
  now
    .wait_for_sleep_registration_after(sleep_registrations)
    .await;

  lease_store.block_next.store(true, Ordering::Release);
  membership_tx.send(BrokerMembership::new(vec![node_a]))?;
  published_rx
    .recv()
    .await
    .expect("restored membership assignment was not published");
  assert!(matches!(
    engine.produce_batch(request()).await,
    Err(WriteError::NotLeaseHolder { .. })
  ));

  now.advance(TimeDuration::milliseconds(250));
  release_entered_rx
    .recv()
    .await
    .expect("retry release did not reach its gate");
  release.add_permits(1);
  published_rx
    .recv()
    .await
    .expect("terminal retry completion did not republish the restored assignment");
  assert!(
    before_release_rx.try_recv().is_err(),
    "retry repeated the drain-to-release lifecycle transition"
  );
  engine.produce_batch(request()).await?;

  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_lease_release() -> Result<()> {
  let now = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let (release_entered_tx, mut release_entered_rx) = mpsc::unbounded_channel();
  let (release_failed_tx, mut release_failed_rx) = mpsc::unbounded_channel();
  let (published_tx, _published_rx) = mpsc::unbounded_channel();
  let (before_release_tx, mut before_release_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let lease_store = Arc::new(GatedLeaseReleaseStore {
    inner: Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    entered_tx: release_entered_tx,
    failed_tx: Some(release_failed_tx),
    release: Arc::clone(&release),
    block_next: AtomicBool::new(false),
    fail_next: AtomicBool::new(true),
  });
  let engine = make_engine_with_membership_and_lease_store(
    now.clone(),
    membership_rx,
    shutdown_trigger.make_handle(),
    1,
    &(lease_store.clone() as Arc<dyn ProducerPartitionLeaseStore>),
    Some(Arc::new(NotifyingLeaseLifecycleHook {
      published_tx,
      before_release_tx,
    })),
  )?;
  let request = WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1], 1)],
  };
  engine.produce_batch(request).await?;

  let sleep_registrations = now.sleep_registration_count();
  lease_store.block_next.store(true, Ordering::Release);
  drop(membership_tx);
  let shutdown = tokio::spawn(async move { shutdown_trigger.shutdown().await });
  release_failed_rx
    .recv()
    .await
    .expect("shutdown release did not fail at its test gate");
  before_release_rx
    .recv()
    .await
    .expect("shutdown drain did not begin lease release");
  now
    .wait_for_sleep_registration_after(sleep_registrations)
    .await;
  assert!(
    !shutdown.is_finished(),
    "shutdown completed after a transient release failure"
  );

  // The retry reaches the store gate without starting another drain or lifecycle transition.
  now.advance(TimeDuration::milliseconds(250));
  release_entered_rx
    .recv()
    .await
    .expect("shutdown release retry did not reach its gate");
  assert!(
    !shutdown.is_finished(),
    "shutdown completed before the release retry"
  );
  assert!(
    before_release_rx.try_recv().is_err(),
    "shutdown release retry repeated the drain-to-release lifecycle transition"
  );

  release.add_permits(1);
  shutdown.await?;
  Ok(())
}

#[tokio::test]
async fn produce_rejects_partition_planned_for_another_healthy_broker() -> Result<()> {
  let now = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (_membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".into(),
      address: "10.0.0.1:8080".into(),
    },
    BrokerNode {
      node_id: "node-b".into(),
      address: "10.0.0.2:8080".into(),
    },
  ]));
  let (engine, lease_store) =
    make_engine_with_membership(now, membership_rx, shutdown_trigger.make_handle(), 2)?;
  let virtual_partition_id = (0 .. 2)
    .find(|partition| {
      engine
        .state
        .lock()
        .assignment_generation_for_partition("telemetry", *partition)
        .is_none()
    })
    .expect("two healthy brokers must split two virtual partitions");
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id,
  };

  // A local discovery member is not automatically authorized for every partition. This verifies
  // the foreground path uses its partition-specific assignment rather than only membership.
  assert!(matches!(
    engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id,
        records: vec![new_record(vec![1], 1)],
      })
      .await,
    Err(WriteError::NotLeaseHolder { .. })
  ));
  assert!(
    engine
      .state
      .lock()
      .partition_state("telemetry", virtual_partition_id)
      .is_none()
  );
  assert!(lease_store.get_lease(&key).await?.is_none());
  Ok(())
}

#[test]
fn foreground_exhaustion_doubles_the_adaptive_reservation_target() {
  let state = state_with_local_telemetry_assignment();
  let initial = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_000),
    false,
    4,
  );
  let AllocationTransitionDecision::Claimed(initial) = initial else {
    panic!("expected initial reservation transition");
  };
  let request = initial.reservation.expect("initial reservation request");
  assert_eq!(request.size, 4);
  assert!(matches!(
    request.reason,
    super::allocation::ReservationReason::Initial
  ));
  initial.transition.finish(
    LeaseExpirationUpdate::Set(Some(offset_datetime_from_unix_millis(2_000))),
    None,
    Some(SeqRange { start: 0, end: 3 }),
  );

  {
    let mut state = state.lock();
    let partition = state.partition_state_mut("telemetry", 0);
    assert_eq!(
      partition.seq_allocator.allocate(4),
      Some(SeqRange { start: 0, end: 3 })
    );
  }
  let refill = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_001),
    false,
    4,
  );
  let AllocationTransitionDecision::Claimed(refill) = refill else {
    panic!("expected foreground refill transition");
  };
  let request = refill.reservation.expect("foreground refill request");
  assert_eq!(request.size, 8);
  assert!(matches!(
    request.reason,
    super::allocation::ReservationReason::ForegroundExhaustion
  ));
}

#[test]
fn maintenance_top_up_extends_the_current_reservation() {
  let state = state_with_local_telemetry_assignment();
  let initial = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_000),
    true,
    100,
  );
  let AllocationTransitionDecision::Claimed(initial) = initial else {
    panic!("expected initial reservation transition");
  };
  initial.transition.finish_lease_maintenance(
    LeaseExpirationUpdate::Set(Some(offset_datetime_from_unix_millis(2_000))),
    None,
    Some(SeqRange { start: 0, end: 99 }),
    0,
  );

  {
    let mut state = state.lock();
    let partition = state.partition_state_mut("telemetry", 0);
    assert_eq!(
      partition.seq_allocator.allocate(74),
      Some(SeqRange { start: 0, end: 73 })
    );
    partition.records_allocated_since_lease_maintenance = 74;
  }

  let top_up = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_100),
    true,
    100,
  );
  let AllocationTransitionDecision::Claimed(top_up) = top_up else {
    panic!("expected maintenance top-up transition");
  };
  let request = top_up.reservation.expect("maintenance top-up request");
  assert_eq!(request.size, 100);
  assert!(matches!(
    request.reason,
    super::allocation::ReservationReason::MaintenanceTopUp
  ));
  top_up.transition.finish_lease_maintenance(
    LeaseExpirationUpdate::Set(Some(offset_datetime_from_unix_millis(2_100))),
    None,
    Some(SeqRange {
      start: 100,
      end: 199,
    }),
    top_up
      .records_allocated_since_last_maintenance
      .unwrap_or_default(),
  );

  let mut state = state.lock();
  let partition = state.partition_state_mut("telemetry", 0);
  assert_eq!(partition.seq_allocator.remaining_capacity(), 126);
  assert_eq!(partition.records_allocated_since_lease_maintenance, 0);
  assert_eq!(
    partition.seq_allocator.allocate(126),
    Some(SeqRange {
      start: 74,
      end: 199
    })
  );
}

#[test]
fn maintenance_high_utilization_doubles_the_adaptive_reservation_target() {
  let state = state_with_local_telemetry_assignment();
  let initial = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_000),
    true,
    100,
  );
  let AllocationTransitionDecision::Claimed(initial) = initial else {
    panic!("expected initial reservation transition");
  };
  initial.transition.finish_lease_maintenance(
    LeaseExpirationUpdate::Set(Some(offset_datetime_from_unix_millis(2_000))),
    None,
    Some(SeqRange { start: 0, end: 99 }),
    0,
  );

  {
    let mut state = state.lock();
    let partition = state.partition_state_mut("telemetry", 0);
    assert_eq!(
      partition.seq_allocator.allocate(75),
      Some(SeqRange { start: 0, end: 74 })
    );
    partition.records_allocated_since_lease_maintenance = 75;
  }

  let refill = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_100),
    true,
    100,
  );
  let AllocationTransitionDecision::Claimed(refill) = refill else {
    panic!("expected maintenance refill transition");
  };
  let request = refill
    .reservation
    .expect("maintenance high-utilization refill request");
  assert_eq!(request.size, 200);
  assert!(matches!(
    request.reason,
    super::allocation::ReservationReason::MaintenanceHighUtilization
  ));
}

#[test]
fn maintenance_high_utilization_waits_until_another_window_is_needed() {
  let state = state_with_local_telemetry_assignment();
  let initial = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_000),
    true,
    100,
  );
  let AllocationTransitionDecision::Claimed(initial) = initial else {
    panic!("expected initial reservation transition");
  };
  initial.transition.finish_lease_maintenance(
    LeaseExpirationUpdate::Set(Some(offset_datetime_from_unix_millis(2_000))),
    None,
    Some(SeqRange { start: 0, end: 199 }),
    0,
  );

  {
    let mut state = state.lock();
    let partition = state.partition_state_mut("telemetry", 0);
    assert_eq!(
      partition.seq_allocator.allocate(75),
      Some(SeqRange { start: 0, end: 74 })
    );
    partition.records_allocated_since_lease_maintenance = 75;
  }

  let maintenance = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_100),
    true,
    100,
  );
  let AllocationTransitionDecision::Claimed(maintenance) = maintenance else {
    panic!("expected lease-maintenance transition");
  };
  assert!(maintenance.reservation.is_none());
  assert_eq!(
    state
      .lock()
      .partition_state("telemetry", 0)
      .expect("partition state")
      .adaptive_reservation_size,
    Some(100)
  );
}

#[test]
fn lease_reacquisition_discards_stale_sequence_capacity() {
  let state = state_with_local_telemetry_assignment();
  let initial = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(1_000),
    false,
    10,
  );
  let AllocationTransitionDecision::Claimed(initial) = initial else {
    panic!("expected initial reservation transition");
  };
  initial.transition.finish(
    LeaseExpirationUpdate::Set(Some(offset_datetime_from_unix_millis(2_000))),
    None,
    Some(SeqRange { start: 0, end: 9 }),
  );

  {
    let mut state = state.lock();
    let partition = state.partition_state_mut("telemetry", 0);
    assert_eq!(
      partition.seq_allocator.allocate(5),
      Some(SeqRange { start: 0, end: 4 })
    );
  }

  let reacquire = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(2_000),
    false,
    10,
  );
  let AllocationTransitionDecision::Claimed(reacquire) = reacquire else {
    panic!("expected lease reacquisition transition");
  };
  assert!(reacquire.reservation.is_none());
  reacquire.transition.finish(
    LeaseExpirationUpdate::Set(Some(offset_datetime_from_unix_millis(3_000))),
    None,
    None,
  );

  let reservation = begin_allocation_transition(
    &state,
    "telemetry",
    0,
    1,
    offset_datetime_from_unix_millis(2_001),
    false,
    10,
  );
  let AllocationTransitionDecision::Claimed(reservation) = reservation else {
    panic!("expected reservation transition after reacquisition");
  };
  reservation.transition.finish(
    LeaseExpirationUpdate::Preserve,
    None,
    Some(SeqRange { start: 20, end: 29 }),
  );

  let mut state = state.lock();
  let partition = state.partition_state_mut("telemetry", 0);
  assert_eq!(
    partition.seq_allocator.allocate(1),
    Some(SeqRange { start: 20, end: 20 })
  );
}

#[test]
fn nonadjacent_reservation_replaces_remaining_capacity() {
  let mut allocator = super::state::SeqAllocator {
    reservation: Some(SeqRange { start: 0, end: 9 }),
    next_seq: 5,
    last_handed_out_seq: Some(4),
  };

  allocator.install_or_extend_reservation(SeqRange { start: 20, end: 29 });

  assert_eq!(allocator.allocate(1), Some(SeqRange { start: 20, end: 20 }));
}

fn make_engine(
  time_provider: Arc<ManualTimeProvider>,
  config: WriteConfig,
  shutdown_trigger_handle: bd_shutdown::ComponentShutdownTriggerHandle,
) -> Result<(Arc<WriteEngineImpl>, Arc<InMemoryMetadataStore>)> {
  make_engine_with_metadata_window_size(
    time_provider,
    config,
    DEFAULT_TEST_METADATA_WINDOW_SIZE,
    shutdown_trigger_handle,
  )
}

fn make_engine_with_metadata_window_size(
  time_provider: Arc<ManualTimeProvider>,
  config: WriteConfig,
  metadata_window_size: TimeDuration,
  shutdown_trigger_handle: bd_shutdown::ComponentShutdownTriggerHandle,
) -> Result<(Arc<WriteEngineImpl>, Arc<InMemoryMetadataStore>)> {
  let (engine, metadata_store, _lease_store) =
    make_engine_with_lease_store_and_metadata_window_size(
      time_provider,
      config,
      metadata_window_size,
      shutdown_trigger_handle,
    )?;
  Ok((engine, metadata_store))
}

fn make_two_partition_engine_with_metadata_window_size(
  time_provider: Arc<ManualTimeProvider>,
  config: WriteConfig,
  metadata_window_size: TimeDuration,
  shutdown_trigger_handle: bd_shutdown::ComponentShutdownTriggerHandle,
) -> Result<(Arc<WriteEngineImpl>, Arc<InMemoryMetadataStore>)> {
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count: 2,
      num_writers: 1,
      retention: TimeDuration::days(7),
      max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
      metadata_window_size,
    },
  );

  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let engine = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    metadata_store.clone(),
    Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    "test-node".to_string(),
    shutdown_trigger_handle,
    &metrics_scope(),
  )
  .time_provider(time_provider)
  .build()?;

  Ok((Arc::new(engine), metadata_store))
}

fn make_engine_with_lease_store(
  time_provider: Arc<ManualTimeProvider>,
  config: WriteConfig,
  shutdown_trigger_handle: bd_shutdown::ComponentShutdownTriggerHandle,
) -> Result<(
  Arc<WriteEngineImpl>,
  Arc<InMemoryMetadataStore>,
  Arc<InMemoryProducerPartitionLeaseStore>,
)> {
  make_engine_with_lease_store_and_metadata_window_size(
    time_provider,
    config,
    DEFAULT_TEST_METADATA_WINDOW_SIZE,
    shutdown_trigger_handle,
  )
}

fn make_engine_with_lease_store_and_metadata_window_size(
  time_provider: Arc<ManualTimeProvider>,
  config: WriteConfig,
  metadata_window_size: TimeDuration,
  shutdown_trigger_handle: bd_shutdown::ComponentShutdownTriggerHandle,
) -> Result<(
  Arc<WriteEngineImpl>,
  Arc<InMemoryMetadataStore>,
  Arc<InMemoryProducerPartitionLeaseStore>,
)> {
  let scope = metrics_scope();
  make_engine_with_lease_store_and_scope_and_metadata_window_size(
    time_provider,
    config,
    metadata_window_size,
    &scope,
    shutdown_trigger_handle,
  )
}

fn make_engine_with_lease_store_and_scope(
  time_provider: Arc<ManualTimeProvider>,
  config: WriteConfig,
  metrics_scope: &Scope,
  shutdown_trigger_handle: bd_shutdown::ComponentShutdownTriggerHandle,
) -> Result<(
  Arc<WriteEngineImpl>,
  Arc<InMemoryMetadataStore>,
  Arc<InMemoryProducerPartitionLeaseStore>,
)> {
  make_engine_with_lease_store_and_scope_and_metadata_window_size(
    time_provider,
    config,
    DEFAULT_TEST_METADATA_WINDOW_SIZE,
    metrics_scope,
    shutdown_trigger_handle,
  )
}

fn make_engine_with_lease_store_and_scope_and_metadata_window_size(
  time_provider: Arc<ManualTimeProvider>,
  config: WriteConfig,
  metadata_window_size: TimeDuration,
  metrics_scope: &Scope,
  shutdown_trigger_handle: bd_shutdown::ComponentShutdownTriggerHandle,
) -> Result<(
  Arc<WriteEngineImpl>,
  Arc<InMemoryMetadataStore>,
  Arc<InMemoryProducerPartitionLeaseStore>,
)> {
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count: 1,
      num_writers: 1,
      retention: TimeDuration::days(7),
      max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
      metadata_window_size,
    },
  );

  let blob_store = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());

  let engine = WriteEngineBuilder::new(
    config,
    topics,
    blob_store,
    metadata_store.clone(),
    lease_store.clone(),
    "test-node".to_string(),
    shutdown_trigger_handle,
    metrics_scope,
  )
  .time_provider(time_provider)
  .build()?;

  Ok((Arc::new(engine), metadata_store, lease_store))
}

async fn receive_blob_write(receiver: &mut mpsc::UnboundedReceiver<String>) -> String {
  for _ in 0 .. 100 {
    if let Ok(key) = receiver.try_recv() {
      return key;
    }
    tokio::task::yield_now().await;
  }
  panic!("expected blob write did not begin");
}

async fn wait_for_partition_draining_start(engine: &WriteEngineImpl, virtual_partition_id: u32) {
  for _ in 0 .. 100 {
    let draining = {
      let state = engine.state.lock();
      state
        .topics
        .get("telemetry")
        .and_then(|topic_state| topic_state.partitions.get(&virtual_partition_id))
        .is_some_and(|partition_state| partition_state.draining)
    };
    if draining {
      return;
    }
    tokio::time::sleep(StdDuration::from_millis(1)).await;
  }
  panic!("partition did not begin draining");
}

async fn wait_for_buffered_partitions(engine: &WriteEngineImpl, topics: &[&str]) {
  for _ in 0 .. 100 {
    let buffered = {
      let state = engine.state.lock();
      topics.iter().all(|topic| {
        state
          .partition_state(topic, 0)
          .is_some_and(|partition| !partition.buffer.batches.is_empty())
      })
    };
    if buffered {
      return;
    }
    tokio::task::yield_now().await;
  }
  panic!("partitions did not buffer accepted batches");
}

#[tokio::test]
async fn state_snapshot_reports_local_buffer_and_lease_state() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.writer_id = 0;
  config.flush_max_bytes = 1024;
  config.flush_max_delay = TimeDuration::milliseconds(60_000);

  let (engine, _metadata_store) =
    make_engine(time_provider, config, shutdown_trigger.make_handle())?;
  let pending_engine = Arc::clone(&engine);
  let pending_write = tokio::spawn(async move {
    pending_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1, 2, 3], 10)],
      })
      .await
  });

  tokio::task::yield_now().await;

  let snapshot = engine.state_snapshot().await;
  assert_eq!(
    snapshot.generated_at,
    offset_datetime_from_unix_millis(1_700_000_000_000)
  );
  assert_eq!(snapshot.holder_id, "test-node");
  assert_eq!(snapshot.writer_id, 0);
  assert_eq!(snapshot.max_segment_bytes, 64 * 1024 * 1024);
  assert_eq!(snapshot.effective_max_segment_bytes, 64 * 1024 * 1024);
  assert_eq!(snapshot.effective_flush_max_bytes, 1024);
  assert_eq!(
    snapshot.effective_flush_max_delay,
    StdDuration::from_secs(60)
  );
  assert_eq!(snapshot.membership.len(), 1);
  assert_eq!(snapshot.membership[0].node_id.as_str(), "test-node");
  assert_eq!(snapshot.membership[0].address.as_str(), "test-node");
  assert_eq!(snapshot.ownership.len(), 1);
  assert!(snapshot.ownership[0].assignment_is_local);
  assert_eq!(
    snapshot.ownership[0].lease_status,
    super::BrokerLeaseStatus::LocalActive
  );
  assert_eq!(snapshot.topics.len(), 1);

  let topic = &snapshot.topics[0];
  assert_eq!(topic.name.as_str(), "telemetry");
  assert_eq!(topic.partition_count, 1);
  assert_eq!(topic.num_writers, 1);
  assert_eq!(topic.local_partitions.len(), 1);

  let partition = &topic.local_partitions[0];
  assert_eq!(partition.virtual_partition_id, 0);
  assert_eq!(
    partition.lease_expires_at,
    Some(offset_datetime_from_unix_millis(1_700_000_030_000))
  );
  assert_eq!(partition.buffered_batch_count, 1);
  assert_eq!(partition.buffered_record_count, 1);
  assert_eq!(partition.buffered_bytes, 3);
  assert_eq!(
    partition.first_buffered_at,
    Some(offset_datetime_from_unix_millis(1_700_000_000_000))
  );
  assert_eq!(partition.next_sequence, 1);
  assert_eq!(
    partition
      .sequence_reservation
      .as_ref()
      .map(|reservation| (reservation.start, reservation.end)),
    Some((0, 9_999))
  );

  let state_dump = to_value(&snapshot)?;
  assert_eq!(state_dump["generated_at"], "2023-11-14T22:13:20Z");
  assert!(state_dump.get("generated_at_ts_ms").is_none());
  assert_eq!(state_dump["flush_max_delay"], "1m");
  assert_eq!(state_dump["effective_flush_max_bytes"], 1024);
  assert_eq!(state_dump["effective_flush_max_delay"], "1m");
  assert_eq!(state_dump["max_segment_bytes"], 64 * 1024 * 1024);
  assert_eq!(state_dump["effective_max_segment_bytes"], 64 * 1024 * 1024);
  assert_eq!(state_dump["topics"][0]["name"], "telemetry");
  assert_eq!(state_dump["ownership"][0]["topic"], "telemetry");
  assert_eq!(
    state_dump["topics"][0]["local_partitions"][0]["lease_expires_at"],
    "2023-11-14T22:13:50Z"
  );
  assert_eq!(
    state_dump["ownership"][0]["observed_lease"]["expires_at"],
    "2023-11-14T22:13:50Z"
  );

  pending_write.abort();
  let _ignored = pending_write.await;
  Ok(())
}

#[tokio::test]
async fn state_snapshot_reports_durable_leases_for_every_configured_partition() -> Result<()> {
  let now = offset_datetime_from_unix_millis(1_050);
  let time_provider = Arc::new(ManualTimeProvider::new(now));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.writer_id = 0;
  let producer_lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let consumer_lease_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let topics = HashMap::from([(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count: 2,
      num_writers: 2,
      retention: TimeDuration::days(7),
      max_metadata_publication_lag: TimeDuration::seconds(30),
      metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
    },
  )]);
  let producer_key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };
  producer_lease_store
    .acquire_lease(
      producer_key.clone(),
      "producer-a".to_string(),
      "producer-session-a".to_string(),
      now,
      TimeDuration::seconds(30),
      ProducerSequenceProgress::default(),
    )
    .await?;
  producer_lease_store
    .reserve_sequences(
      &producer_key,
      "producer-a",
      "producer-session-a",
      now,
      10,
      ProducerSequenceProgress::default(),
    )
    .await?;

  let active_group_a = ConsumerGroupLeaseKey {
    topic: "telemetry".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 2,
  };
  let active_group_b = ConsumerGroupLeaseKey {
    topic: "telemetry".to_string(),
    group_id: "group-b".to_string(),
    virtual_partition_id: 2,
  };
  let expired = ConsumerGroupLeaseKey {
    topic: "telemetry".to_string(),
    group_id: "group-c".to_string(),
    virtual_partition_id: 0,
  };
  consumer_lease_store
    .assign_partition(
      active_group_b.clone(),
      "consumer-b".to_string(),
      3,
      now,
      TimeDuration::seconds(30),
    )
    .await?;
  consumer_lease_store
    .assign_partition(
      active_group_a.clone(),
      "consumer-a".to_string(),
      2,
      now,
      TimeDuration::seconds(30),
    )
    .await?;
  consumer_lease_store
    .heartbeat_partition(
      &active_group_a,
      "consumer-a",
      2,
      now,
      TimeDuration::seconds(30),
      Some(CommittedCursor {
        virtual_partition_id: 2,
        seq_end: 42,
        source_checkpoint: Some(CommittedSourceCheckpoint {
          window_start_unix_seconds: 1_200,
          snowflake_id: 9,
        }),
      }),
    )
    .await?;
  consumer_lease_store
    .assign_partition(
      expired.clone(),
      "consumer-c".to_string(),
      1,
      now - TimeDuration::seconds(1),
      TimeDuration::milliseconds(100),
    )
    .await?;

  let engine = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    producer_lease_store,
    "test-node".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .consumer_lease_store(consumer_lease_store)
  .time_provider(time_provider)
  .build()?;

  let snapshot = engine.state_snapshot().await;
  assert_eq!(
    snapshot.durable_consumer_lease_scan.status,
    super::DurableStateLookupStatus::Present
  );
  assert_eq!(snapshot.durable_topics.len(), 1);
  let partitions = &snapshot.durable_topics[0].partitions;
  assert_eq!(
    partitions
      .iter()
      .map(|partition| partition.virtual_partition_id)
      .collect::<Vec<_>>(),
    vec![0, 1, 2, 3]
  );
  assert_eq!(
    partitions[0]
      .producer_lease
      .lease
      .as_ref()
      .unwrap()
      .max_allocated_seq,
    Some(9)
  );
  assert_eq!(
    partitions[0]
      .producer_lease
      .lease
      .as_ref()
      .unwrap()
      .lease_sequence_start,
    Some(0)
  );
  assert_eq!(
    partitions[0]
      .producer_lease
      .lease
      .as_ref()
      .unwrap()
      .last_handed_out_seq,
    None
  );
  assert!(
    partitions[0]
      .producer_lease
      .lease
      .as_ref()
      .unwrap()
      .sequence_progress_updated_at
      .is_some()
  );
  assert_eq!(
    partitions[1].producer_lease.status,
    super::DurableStateLookupStatus::Missing
  );
  assert_eq!(
    partitions[2]
      .consumer_leases
      .iter()
      .map(|lease| lease.group_id.as_str())
      .collect::<Vec<_>>(),
    vec!["group-a", "group-b"]
  );
  assert_eq!(partitions[2].consumer_leases[0].committed_seq_end, Some(42));
  assert_eq!(
    partitions[2].consumer_leases[0].lease_expires_at,
    offset_datetime_from_unix_millis(31_050)
  );
  assert_eq!(
    partitions[2].consumer_leases[0].last_heartbeat_at,
    offset_datetime_from_unix_millis(1_050)
  );
  assert_eq!(
    partitions[2].consumer_leases[0].committed_at,
    Some(offset_datetime_from_unix_millis(1_050))
  );
  assert_eq!(
    partitions[2].consumer_leases[0]
      .committed_source_checkpoint
      .as_ref()
      .unwrap()
      .snowflake_id,
    9
  );
  assert!(partitions[0].consumer_leases.is_empty());
  let state_dump = to_value(&snapshot)?;
  assert_eq!(
    state_dump["durable_topics"][0]["partitions"][2]["consumer_leases"][0]["lease_expires_at"],
    "1970-01-01T00:00:31.05Z"
  );
  assert_eq!(
    state_dump["durable_topics"][0]["partitions"][2]["consumer_leases"][0]["last_heartbeat_at"],
    "1970-01-01T00:00:01.05Z"
  );
  assert_eq!(
    state_dump["durable_topics"][0]["partitions"][2]["consumer_leases"][0]["committed_at"],
    "1970-01-01T00:00:01.05Z"
  );

  Ok(())
}

#[tokio::test(start_paused = true)]
async fn state_snapshot_reports_effective_runtime_policy() -> Result<()> {
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 128 * 1024 * 1024;
  config.flush_max_delay = TimeDuration::seconds(10);
  config.max_segment_bytes = 128 * 1024 * 1024;
  let feature_flags = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag("blob_stream_broker_flush_max_bytes", 32 * 1024 * 1024)
      .with_integer_flag("blob_stream_broker_flush_max_delay_ms", 500)
      .with_integer_flag("blob_stream_broker_max_segment_bytes", 32 * 1024 * 1024),
  ));
  let engine = WriteEngineBuilder::new(
    config,
    HashMap::from([(
      "telemetry".into(),
      TopicInfo {
        name: "telemetry".into(),
        partition_count: 1,
        num_writers: 1,
        retention: TimeDuration::days(7),
        max_metadata_publication_lag: TimeDuration::seconds(30),
        metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
      },
    )]),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    "test-node".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .time_provider(time_provider.clone())
  .feature_flags(Some(feature_flags.snapshot_watch()))
  .build()?;

  let snapshot = engine.state_snapshot().await;
  assert_eq!(snapshot.flush_max_bytes, 128 * 1024 * 1024);
  assert_eq!(snapshot.effective_flush_max_bytes, 32 * 1024 * 1024);
  assert_eq!(snapshot.flush_max_delay, StdDuration::from_secs(10));
  assert_eq!(
    snapshot.effective_flush_max_delay,
    StdDuration::from_millis(500)
  );
  assert_eq!(snapshot.max_segment_bytes, 128 * 1024 * 1024);
  assert_eq!(snapshot.effective_max_segment_bytes, 32 * 1024 * 1024);

  feature_flags.update(Arc::new(
    DefaultFeatureFlags::default()
      .with_integer_flag("blob_stream_broker_flush_max_bytes", 16 * 1024 * 1024)
      .with_integer_flag("blob_stream_broker_flush_max_delay_ms", 250)
      .with_integer_flag("blob_stream_broker_max_segment_bytes", 16 * 1024 * 1024),
  ));
  tokio::task::yield_now().await;

  let snapshot = engine.state_snapshot().await;
  assert_eq!(snapshot.effective_flush_max_bytes, 32 * 1024 * 1024);
  assert_eq!(
    snapshot.effective_flush_max_delay,
    StdDuration::from_millis(500)
  );

  time_provider.advance(TimeDuration::milliseconds(500));
  tokio::time::advance(StdDuration::from_millis(500)).await;
  tokio::task::yield_now().await;

  let snapshot = engine.state_snapshot().await;
  assert_eq!(snapshot.effective_flush_max_bytes, 16 * 1024 * 1024);
  assert_eq!(
    snapshot.effective_flush_max_delay,
    StdDuration::from_millis(250)
  );
  Ok(())
}

#[tokio::test]
async fn state_snapshot_reports_expired_observed_lease() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.writer_id = 0;

  let (engine, _metadata_store, lease_store) =
    make_engine_with_lease_store(time_provider, config, shutdown_trigger.make_handle())?;
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };
  lease_store
    .acquire_lease(
      key,
      "former-owner".to_string(),
      "former-owner-session".to_string(),
      offset_datetime_from_unix_millis(now_ms - 1_000),
      TimeDuration::milliseconds(100),
      ProducerSequenceProgress::default(),
    )
    .await?;

  let snapshot = engine.state_snapshot().await;
  let ownership = snapshot
    .ownership
    .first()
    .expect("local ownership row exists");
  assert_eq!(
    ownership.lease_status,
    super::BrokerLeaseStatus::UnleasedOrExpired
  );
  let observed_lease = ownership
    .observed_lease
    .as_ref()
    .expect("expired lease is reported");
  assert_eq!(observed_lease.holder_id, "former-owner");
  assert!(!observed_lease.is_active);

  Ok(())
}

#[tokio::test]
async fn successful_sequence_reservation_records_metrics() -> Result<()> {
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay = TimeDuration::milliseconds(60_000);
  config.lease_duration = TimeDuration::seconds(10);
  config.heartbeat_interval = TimeDuration::seconds(5);

  let collector = Collector::default();
  let scope = collector.scope("blob_stream_broker_test");
  let (engine, _metadata_store, _lease_store) = make_engine_with_lease_store_and_scope(
    time_provider,
    config,
    &scope,
    shutdown_trigger.make_handle(),
  )?;
  engine
    .produce_batch(WriteRequest {
      topic: "telemetry".into(),
      virtual_partition_id: 0,
      records: vec![new_record(vec![1], 10)],
    })
    .await?;

  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(
    metrics.contains("blob_stream_broker_test:write:sequence_reservations_total 1"),
    "{metrics}"
  );
  assert!(
    metrics.contains("blob_stream_broker_test:write:sequence_reservation_records_total 10000")
  );
  assert!(
    metrics.contains("blob_stream_broker_test:write:sequence_reservation_latency_seconds_count 1")
  );
  assert!(
    metrics.contains("blob_stream_broker_test:write:produce_requests_total 1"),
    "{metrics}"
  );
  Ok(())
}

#[tokio::test]
async fn fenced_sequence_reservations_do_not_record_failure_metrics() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let collector = Collector::default();
  let scope = collector.scope("blob_stream_broker_test");
  let (engine, _metadata_store, lease_store) = make_engine_with_lease_store_and_scope(
    time_provider,
    WriteConfig::with_defaults(),
    &scope,
    shutdown_trigger.make_handle(),
  )?;

  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };
  lease_store
    .acquire_lease(
      key,
      "other-broker".to_string(),
      "other-broker-session".to_string(),
      offset_datetime_from_unix_millis(now_ms),
      TimeDuration::milliseconds(30_000),
      ProducerSequenceProgress::default(),
    )
    .await?;
  let error = engine
    .produce_batch(WriteRequest {
      topic: "telemetry".into(),
      virtual_partition_id: 0,
      records: vec![new_record(vec![1], 10)],
    })
    .await
    .expect_err("active lease held by another broker should fence the produce request");
  assert!(matches!(error, super::WriteError::NotLeaseHolder { .. }));

  let error = engine
    .reserve_sequences(
      "telemetry",
      1,
      offset_datetime_from_unix_millis(now_ms),
      1,
      ProducerSequenceProgress::default(),
    )
    .await
    .expect_err("missing lease should fence the direct sequence reservation");
  assert!(matches!(error, super::WriteError::NotLeaseHolder { .. }));

  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(
    metrics.contains("blob_stream_broker_test:write:sequence_reservation_failures_total 0"),
    "{metrics}"
  );
  Ok(())
}

#[tokio::test]
async fn buffers_until_size_rollover() -> Result<()> {
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 10;
  config.flush_max_delay = TimeDuration::milliseconds(60_000);
  let metadata_window_size = TimeDuration::seconds(60);

  let (engine, metadata_store) = make_engine_with_metadata_window_size(
    time_provider.clone(),
    config.clone(),
    metadata_window_size,
    shutdown_trigger.make_handle(),
  )?;

  let request = WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1; 6], 10)],
  };

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move { first_engine.produce_batch(request).await });

  tokio::task::yield_now().await;
  assert!(!first.is_finished());

  let window = Window::for_timestamp(time_provider.now(), metadata_window_size);
  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert!(segments.is_empty());

  let request = WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![2; 6], 20)],
  };

  engine.produce_batch(request).await?;
  first.await??;

  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 1);
  assert_eq!(segments[0].segment_index[&0].len(), 1);
  assert_eq!(
    segments[0].segment_index[&0][0].seq_range,
    SeqRange { start: 0, end: 1 }
  );
  assert_eq!(segments[0].segment_index[&0][0].payload_bytes, 12);
  Ok(())
}

#[tokio::test]
async fn same_partition_requests_serialize_sequence_reservations() -> Result<()> {
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.reservation_size = 1;

  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release_first = Arc::new(Semaphore::new(0));
  let lease_store = Arc::new(GatedReservationLeaseStore {
    inner: InMemoryProducerPartitionLeaseStore::new(),
    entered_tx,
    release_first: Arc::clone(&release_first),
    first_reservation: AtomicBool::new(true),
  });
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
        },
      )]),
      Arc::new(InMemoryBlobStore::new()),
      Arc::new(InMemoryMetadataStore::new()),
      lease_store,
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider)
    .build()?,
  );

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  entered_rx.recv().await.expect("first reservation entered");

  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  assert!(
    tokio::time::timeout(StdDuration::from_millis(20), entered_rx.recv())
      .await
      .is_err(),
    "second reservation began before the first was installed"
  );

  release_first.add_permits(1);
  let first = first.await??;
  entered_rx.recv().await.expect("second reservation entered");
  let second = second.await??;
  assert_eq!(first.seq_range, SeqRange { start: 0, end: 0 });
  assert_eq!(second.seq_range, SeqRange { start: 1, end: 1 });
  Ok(())
}

#[tokio::test]
async fn cancelled_reservation_releases_allocation_transition() -> Result<()> {
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.reservation_size = 1;

  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let lease_store = Arc::new(GatedReservationLeaseStore {
    inner: InMemoryProducerPartitionLeaseStore::new(),
    entered_tx,
    release_first: Arc::new(Semaphore::new(0)),
    first_reservation: AtomicBool::new(true),
  });
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
        },
      )]),
      Arc::new(InMemoryBlobStore::new()),
      Arc::new(InMemoryMetadataStore::new()),
      lease_store,
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider)
    .build()?,
  );

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  entered_rx.recv().await.expect("first reservation entered");

  let snapshot = tokio::time::timeout(StdDuration::from_millis(20), engine.state_snapshot())
    .await
    .expect("state snapshot blocked behind reservation I/O");
  let partition = &snapshot.topics[0].local_partitions[0];
  assert!(partition.allocation_in_flight);
  assert_eq!(
    partition.allocation_started_at,
    Some(offset_datetime_from_unix_millis(1_700_000_000_000))
  );

  first.abort();
  assert!(first.await.is_err(), "reservation task should be cancelled");

  let response = tokio::time::timeout(
    StdDuration::from_millis(100),
    engine.produce_batch(WriteRequest {
      topic: "telemetry".into(),
      virtual_partition_id: 0,
      records: vec![new_record(vec![2], 20)],
    }),
  )
  .await
  .expect("allocation transition remained stuck after cancellation")?;
  assert_eq!(response.seq_range, SeqRange { start: 0, end: 0 });
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn flushes_on_time_rollover() -> Result<()> {
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1024;
  config.flush_max_delay = TimeDuration::milliseconds(500);
  let metadata_window_size = TimeDuration::seconds(60);

  let (engine, metadata_store) = make_engine_with_metadata_window_size(
    time_provider.clone(),
    config.clone(),
    metadata_window_size,
    shutdown_trigger.make_handle(),
  )?;

  let request = WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![3; 4], 30)],
  };

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move { first_engine.produce_batch(request).await });
  tokio::task::yield_now().await;
  assert!(!first.is_finished());

  let advance = config.flush_max_delay + TimeDuration::milliseconds(10);
  time_provider.advance(advance);
  tokio::time::advance(std_duration(advance)).await;
  tokio::task::yield_now().await;

  first.await??;

  let window = Window::for_timestamp(time_provider.now(), metadata_window_size);
  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 1);
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_coalesces_staggered_partitions_for_a_topic() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay = TimeDuration::milliseconds(10);
  let metadata_window_size = TimeDuration::seconds(60);

  let (engine, metadata_store) = make_two_partition_engine_with_metadata_window_size(
    time_provider.clone(),
    config.clone(),
    metadata_window_size,
    shutdown_trigger.make_handle(),
  )?;

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  for _ in 0 .. 100 {
    if engine
      .state
      .lock()
      .partition_state("telemetry", 0)
      .is_some()
    {
      break;
    }
    tokio::task::yield_now().await;
  }

  time_provider.advance(TimeDuration::milliseconds(5));
  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 1,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  for _ in 0 .. 100 {
    let buffered_batches = engine
      .state_snapshot()
      .await
      .topics
      .iter()
      .flat_map(|topic| &topic.local_partitions)
      .map(|partition| partition.buffered_batch_count)
      .sum::<usize>();
    if buffered_batches == 2 {
      break;
    }
    tokio::task::yield_now().await;
  }

  time_provider.advance(TimeDuration::milliseconds(5));
  tokio::time::advance(std_duration(config.flush_max_delay)).await;
  tokio::task::yield_now().await;

  first.await??;
  second.await??;

  let window = Window::for_timestamp(time_provider.now(), metadata_window_size);
  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 1);
  assert_eq!(segments[0].segment_index.len(), 2);
  assert!(segments[0].segment_index.contains_key(&0));
  assert!(segments[0].segment_index.contains_key(&1));

  let next_first_engine = Arc::clone(&engine);
  let next_first = tokio::spawn(async move {
    next_first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![3], 30)],
      })
      .await
  });
  for _ in 0 .. 100 {
    let buffered_batches = engine
      .state_snapshot()
      .await
      .topics
      .iter()
      .flat_map(|topic| &topic.local_partitions)
      .map(|partition| partition.buffered_batch_count)
      .sum::<usize>();
    if buffered_batches == 1 {
      break;
    }
    tokio::task::yield_now().await;
  }

  time_provider.advance(TimeDuration::milliseconds(5));
  let next_second_engine = Arc::clone(&engine);
  let next_second = tokio::spawn(async move {
    next_second_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 1,
        records: vec![new_record(vec![4], 40)],
      })
      .await
  });
  for _ in 0 .. 100 {
    let buffered_batches = engine
      .state_snapshot()
      .await
      .topics
      .iter()
      .flat_map(|topic| &topic.local_partitions)
      .map(|partition| partition.buffered_batch_count)
      .sum::<usize>();
    if buffered_batches == 2 {
      break;
    }
    tokio::task::yield_now().await;
  }

  time_provider.advance(TimeDuration::milliseconds(5));
  tokio::time::advance(std_duration(config.flush_max_delay)).await;
  tokio::task::yield_now().await;
  next_first.await??;
  next_second.await??;

  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 2);
  assert!(
    segments
      .iter()
      .all(|segment| segment.segment_index.len() == 2)
  );
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_due_flush_completes_before_later_byte_flush() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 2;
  config.flush_max_delay = TimeDuration::milliseconds(10);
  let metadata_window_size = TimeDuration::seconds(60);

  let (engine, metadata_store) = make_two_partition_engine_with_metadata_window_size(
    time_provider.clone(),
    config.clone(),
    metadata_window_size,
    shutdown_trigger.make_handle(),
  )?;
  time_provider.wait_until_sleeping(1).await;
  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  for _ in 0 .. 100 {
    if engine
      .state
      .lock()
      .partition_state("telemetry", 0)
      .is_some()
    {
      break;
    }
    tokio::task::yield_now().await;
  }

  time_provider.advance(TimeDuration::milliseconds(5));
  let peer_engine = Arc::clone(&engine);
  let peer = tokio::spawn(async move {
    peer_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 1,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  for _ in 0 .. 100 {
    let buffered_batches = engine
      .state_snapshot()
      .await
      .topics
      .iter()
      .flat_map(|topic| &topic.local_partitions)
      .map(|partition| partition.buffered_batch_count)
      .sum::<usize>();
    if buffered_batches == 2 {
      break;
    }
    tokio::task::yield_now().await;
  }

  time_provider.advance(TimeDuration::milliseconds(5));
  first.await??;
  peer.await??;

  let byte_due_engine = Arc::clone(&engine);
  let byte_due = tokio::spawn(async move {
    byte_due_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![3; 2], 30)],
      })
      .await
  });
  byte_due.await??;

  let window = Window::for_timestamp(time_provider.now(), metadata_window_size);
  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 2);
  assert!(segments.iter().any(|segment| {
    segment.segment_index.len() == 2
      && segment.segment_index.contains_key(&0)
      && segment.segment_index.contains_key(&1)
  }));
  assert!(
    segments.iter().any(|segment| {
      segment.segment_index.len() == 1 && segment.segment_index.contains_key(&0)
    })
  );
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn byte_flush_does_not_coalesce_buffered_topic_peers() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 2;
  config.flush_max_delay = TimeDuration::milliseconds(10);
  let metadata_window_size = TimeDuration::seconds(60);

  let (engine, metadata_store) = make_two_partition_engine_with_metadata_window_size(
    time_provider.clone(),
    config.clone(),
    metadata_window_size,
    shutdown_trigger.make_handle(),
  )?;
  let peer_engine = Arc::clone(&engine);
  let peer = tokio::spawn(async move {
    peer_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 1,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  for _ in 0 .. 100 {
    if engine
      .state
      .lock()
      .partition_state("telemetry", 1)
      .is_some()
    {
      break;
    }
    tokio::task::yield_now().await;
  }

  time_provider.advance(TimeDuration::milliseconds(5));
  engine
    .produce_batch(WriteRequest {
      topic: "telemetry".into(),
      virtual_partition_id: 0,
      records: vec![new_record(vec![2; 2], 20)],
    })
    .await?;
  assert!(!peer.is_finished());

  let window = Window::for_timestamp(time_provider.now(), metadata_window_size);
  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 1);
  assert_eq!(segments[0].segment_index.len(), 1);
  assert!(segments[0].segment_index.contains_key(&0));

  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;
  peer.await??;
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn flush_trigger_and_uploaded_object_metrics_are_recorded() -> Result<()> {
  let collector = Collector::default();
  let scope = collector.scope("blob_stream_broker_test");
  let shutdown_trigger = ComponentShutdownTrigger::default();

  let size_time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let mut size_config = WriteConfig::with_defaults();
  size_config.flush_max_bytes = 1;
  size_config.flush_max_delay = TimeDuration::milliseconds(60_000);
  let (size_engine, _metadata_store, _lease_store) = make_engine_with_lease_store_and_scope(
    size_time_provider,
    size_config,
    &scope,
    shutdown_trigger.make_handle(),
  )?;
  size_engine
    .produce_batch(WriteRequest {
      topic: "telemetry".into(),
      virtual_partition_id: 0,
      records: vec![new_record(vec![1; 16], 10)],
    })
    .await?;

  let delay_time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let mut delay_config = WriteConfig::with_defaults();
  delay_config.flush_max_bytes = 1_024;
  delay_config.flush_max_delay = TimeDuration::milliseconds(10);
  let (delay_engine, _metadata_store, _lease_store) = make_engine_with_lease_store_and_scope(
    Arc::clone(&delay_time_provider),
    delay_config.clone(),
    &scope,
    shutdown_trigger.make_handle(),
  )?;
  let delayed_write = tokio::spawn(async move {
    delay_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2; 16], 20)],
      })
      .await
  });
  tokio::task::yield_now().await;
  delay_time_provider.advance(delay_config.flush_max_delay);
  tokio::time::advance(std_duration(delay_config.flush_max_delay)).await;
  delayed_write.await??;

  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(
    metrics.contains("blob_stream_broker_test:write:flush_batches_max_bytes_total 1"),
    "{metrics}"
  );
  assert!(
    metrics.contains("blob_stream_broker_test:write:flush_batches_max_delay_total 1"),
    "{metrics}"
  );
  assert!(
    metrics.contains("blob_stream_broker_test:write:flush_uploaded_object_bytes_count 2"),
    "{metrics}"
  );
  assert!(
    metrics.contains("blob_stream_broker_test:write:flush_uploaded_object_bytes_total"),
    "{metrics}"
  );
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn metadata_publication_timeout_records_deadline_metric() -> Result<()> {
  let collector = Collector::default();
  let scope = collector.scope("blob_stream_broker_test");
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay = TimeDuration::milliseconds(60_000);

  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(1),
          metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
        },
      )]),
      Arc::new(BlockingBlobStore {
        entered_tx,
        release,
      }),
      Arc::new(InMemoryMetadataStore::new()),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &scope,
    )
    .time_provider(time_provider)
    .build()?,
  );

  let pending = tokio::spawn(async move {
    engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  receive_blob_write(&mut entered_rx).await;
  tokio::time::advance(StdDuration::from_millis(1)).await;
  assert!(pending.await?.is_err());

  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(
    metrics.contains(
      "blob_stream_broker_test:write:\
       metadata_publication_deadline_exhausted_while_persisting_total 1"
    ),
    "{metrics}"
  );
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_collects_later_plans_while_a_prior_plan_is_in_flight() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay = TimeDuration::milliseconds(10);

  let mut topics = HashMap::new();
  for topic in ["first", "second"] {
    topics.insert(
      topic.into(),
      TopicInfo {
        name: topic.into(),
        partition_count: 1,
        num_writers: 1,
        retention: TimeDuration::days(7),
        max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
        metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
      },
    );
  }

  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release_first = Arc::new(Semaphore::new(0));
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config.clone(),
      topics,
      Arc::new(GatedBlobStore {
        entered_tx,
        release_first: Arc::clone(&release_first),
        first_write: AtomicBool::new(true),
      }),
      Arc::new(InMemoryMetadataStore::new()),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider.clone())
    .build()?,
  );
  time_provider.wait_until_sleeping(1).await;

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "first".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  tokio::task::yield_now().await;
  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;
  assert!(
    receive_blob_write(&mut entered_rx)
      .await
      .starts_with("shared/")
  );

  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "second".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  tokio::task::yield_now().await;
  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;

  assert!(
    receive_blob_write(&mut entered_rx)
      .await
      .starts_with("shared/")
  );
  assert!(!first.is_finished());
  assert!(second.is_finished());

  release_first.add_permits(1);
  first.await??;
  second.await??;
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn same_partition_time_flushes_upload_in_parallel_and_publish_in_order() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay = TimeDuration::milliseconds(10);

  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release_first = Arc::new(Semaphore::new(0));
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config.clone(),
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: TimeDuration::seconds(60),
        },
      )]),
      Arc::new(GatedBlobStore {
        entered_tx,
        release_first: Arc::clone(&release_first),
        first_write: AtomicBool::new(true),
      }),
      metadata_store.clone(),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider.clone())
    .build()?,
  );

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  for _ in 0 .. 100 {
    if engine
      .state
      .lock()
      .partition_state("telemetry", 0)
      .is_some_and(|partition| partition.buffer.batches.len() == 1)
    {
      break;
    }
    tokio::task::yield_now().await;
  }
  let sleep_registrations = time_provider.sleep_registration_count();
  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;
  receive_blob_write(&mut entered_rx).await;
  time_provider
    .wait_for_sleep_registration_after(sleep_registrations)
    .await;

  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  for _ in 0 .. 100 {
    if engine
      .state
      .lock()
      .partition_state("telemetry", 0)
      .is_some_and(|partition| partition.buffer.batches.len() == 1)
    {
      break;
    }
    tokio::task::yield_now().await;
  }
  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;
  receive_blob_write(&mut entered_rx).await;
  assert!(!second.is_finished());

  let window = Window::for_timestamp(time_provider.now(), TimeDuration::seconds(60));
  assert!(
    metadata_store
      .scan_window_from_snowflake(
        &window.key("telemetry"),
        None,
        MetadataReadConsistency::Eventual,
      )
      .await?
      .is_empty()
  );

  release_first.add_permits(1);
  first.await??;
  second.await??;

  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  let mut ranges: Vec<_> = segments
    .iter()
    .flat_map(|segment| {
      segment.segment_index[&0]
        .iter()
        .map(|batch| batch.seq_range.clone())
    })
    .collect();
  ranges.sort_by_key(|range| range.start);
  assert_eq!(
    ranges,
    vec![SeqRange { start: 0, end: 0 }, SeqRange { start: 1, end: 1 }]
  );
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_predecessor_prevents_successor_metadata_publication() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  let (blob_entered_tx, mut blob_entered_rx) = mpsc::unbounded_channel();
  let (metadata_entered_tx, mut metadata_entered_rx) = mpsc::unbounded_channel();
  let release_first_metadata = Arc::new(Semaphore::new(0));
  let inner_metadata_store = Arc::new(InMemoryMetadataStore::new());
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: TimeDuration::seconds(60),
        },
      )]),
      Arc::new(NotifyingBlobStore {
        entered_tx: blob_entered_tx,
      }),
      Arc::new(GatedFirstMetadataStore {
        inner: inner_metadata_store.clone(),
        entered_tx: metadata_entered_tx,
        release_first: Arc::clone(&release_first_metadata),
        first_write: AtomicBool::new(true),
      }),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider.clone())
    .build()?,
  );

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  receive_blob_write(&mut blob_entered_rx).await;
  metadata_entered_rx
    .recv()
    .await
    .expect("first metadata write started");

  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  receive_blob_write(&mut blob_entered_rx).await;
  assert!(metadata_entered_rx.try_recv().is_err());

  release_first_metadata.add_permits(1);
  assert!(first.await?.is_err());
  assert!(second.await?.is_err());

  let window = Window::for_timestamp(time_provider.now(), TimeDuration::seconds(60));
  assert!(
    inner_metadata_store
      .scan_window_from_snowflake(
        &window.key("telemetry"),
        None,
        MetadataReadConsistency::Eventual,
      )
      .await?
      .is_empty()
  );
  Ok(())
}

#[tokio::test]
async fn successor_metadata_waits_for_successful_predecessor_publication() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  let (blob_entered_tx, mut blob_entered_rx) = mpsc::unbounded_channel();
  let (metadata_entered_tx, mut metadata_entered_rx) = mpsc::unbounded_channel();
  let release_first_metadata = Arc::new(Semaphore::new(0));
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: TimeDuration::seconds(60),
        },
      )]),
      Arc::new(NotifyingBlobStore {
        entered_tx: blob_entered_tx,
      }),
      Arc::new(BlockingFirstMetadataStore {
        inner: metadata_store.clone(),
        entered_tx: metadata_entered_tx,
        release_first: Arc::clone(&release_first_metadata),
        first_write: AtomicBool::new(true),
      }),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider.clone())
    .build()?,
  );
  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  receive_blob_write(&mut blob_entered_rx).await;
  metadata_entered_rx
    .recv()
    .await
    .expect("first metadata write should start");

  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  receive_blob_write(&mut blob_entered_rx).await;
  assert!(metadata_entered_rx.try_recv().is_err());

  release_first_metadata.add_permits(1);
  first.await??;
  second.await??;
  let window = Window::for_timestamp(time_provider.now(), TimeDuration::seconds(60));
  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 2);
  assert!(segments[0].snowflake_id < segments[1].snowflake_id);
  Ok(())
}

#[tokio::test]
async fn metadata_waiting_epochs_do_not_block_unrelated_blob_uploads() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  let (blob_entered_tx, mut blob_entered_rx) = mpsc::unbounded_channel();
  let (metadata_entered_tx, mut metadata_entered_rx) = mpsc::unbounded_channel();
  let release_first_metadata = Arc::new(Semaphore::new(0));
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let topics = ["telemetry", "independent"]
    .into_iter()
    .map(|topic| {
      (
        topic.into(),
        TopicInfo {
          name: topic.into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: TimeDuration::seconds(60),
        },
      )
    })
    .collect();
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      topics,
      Arc::new(NotifyingBlobStore {
        entered_tx: blob_entered_tx,
      }),
      Arc::new(GatedFirstMetadataStore {
        inner: metadata_store,
        entered_tx: metadata_entered_tx,
        release_first: Arc::clone(&release_first_metadata),
        first_write: AtomicBool::new(true),
      }),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider)
    .build()?,
  );

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  receive_blob_write(&mut blob_entered_rx).await;
  metadata_entered_rx
    .recv()
    .await
    .expect("first metadata write should start");

  let mut waiting_epochs = Vec::new();
  for record in 2 ..= 3 {
    let engine = Arc::clone(&engine);
    waiting_epochs.push(tokio::spawn(async move {
      engine
        .produce_batch(WriteRequest {
          topic: "telemetry".into(),
          virtual_partition_id: 0,
          records: vec![new_record(vec![record], i64::from(record))],
        })
        .await
    }));
    receive_blob_write(&mut blob_entered_rx).await;
  }

  let independent_engine = Arc::clone(&engine);
  let independent = tokio::spawn(async move {
    independent_engine
      .produce_batch(WriteRequest {
        topic: "independent".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![5], 50)],
      })
      .await
  });
  receive_blob_write(&mut blob_entered_rx).await;

  release_first_metadata.add_permits(1);
  assert!(first.await?.is_err());
  for epoch in waiting_epochs {
    assert!(epoch.await?.is_err());
  }
  independent.await??;
  Ok(())
}

#[tokio::test]
async fn membership_handoff_drains_pipelined_flushes_before_releasing_lease() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay = TimeDuration::milliseconds(60_000);

  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
        },
      )]),
      Arc::new(BlockingBlobStore {
        entered_tx,
        release: Arc::clone(&release),
      }),
      metadata_store.clone(),
      lease_store.clone(),
      "node-a".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .membership_rx(membership_rx)
    .time_provider(time_provider.clone())
    .build()?,
  );

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  receive_blob_write(&mut entered_rx).await;
  tokio::time::sleep(StdDuration::from_millis(20)).await;

  let buffered_engine = Arc::clone(&engine);
  let buffered = tokio::spawn(async move {
    buffered_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  tokio::task::yield_now().await;
  receive_blob_write(&mut entered_rx).await;
  assert!(!buffered.is_finished());

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  wait_for_partition_draining_start(&engine, 0).await;

  let second = engine
    .produce_batch(WriteRequest {
      topic: "telemetry".into(),
      virtual_partition_id: 0,
      records: vec![new_record(vec![3], 30)],
    })
    .await;
  assert!(matches!(
    second,
    Err(super::WriteError::NotLeaseHolder { .. })
  ));

  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };
  let held_by_a = lease_store
    .acquire_lease(
      key.clone(),
      "node-b".to_string(),
      "node-b-session".to_string(),
      offset_datetime_from_unix_millis(now_ms),
      TimeDuration::milliseconds(30_000),
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(held_by_a, LeaseAcquireOutcome::HeldByOther(_)));

  release.add_permits(1);
  first.await??;

  let still_held_by_a = lease_store
    .acquire_lease(
      key.clone(),
      "node-b".to_string(),
      "node-b-session".to_string(),
      offset_datetime_from_unix_millis(now_ms),
      TimeDuration::milliseconds(30_000),
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(
    still_held_by_a,
    LeaseAcquireOutcome::HeldByOther(_)
  ));

  release.add_permits(1);
  buffered.await??;

  let mut acquired_by_b = false;
  for _ in 0 .. 100 {
    let outcome = lease_store
      .acquire_lease(
        key.clone(),
        "node-b".to_string(),
        "node-b-session".to_string(),
        offset_datetime_from_unix_millis(now_ms),
        TimeDuration::milliseconds(30_000),
        ProducerSequenceProgress::default(),
      )
      .await?;
    if matches!(outcome, LeaseAcquireOutcome::Acquired(_)) {
      acquired_by_b = true;
      break;
    }
    tokio::task::yield_now().await;
  }
  assert!(acquired_by_b, "lease was not released after drain");

  let window = Window::for_timestamp(time_provider.now(), DEFAULT_TEST_METADATA_WINDOW_SIZE);
  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 2);
  Ok(())
}

#[tokio::test]
async fn component_shutdown_drains_in_flight_flush_before_releasing_lease() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay = TimeDuration::milliseconds(60_000);
  config.lease_duration = TimeDuration::seconds(10);
  config.heartbeat_interval = TimeDuration::seconds(5);

  let (_membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  }]));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
        },
      )]),
      Arc::new(BlockingBlobStore {
        entered_tx,
        release: Arc::clone(&release),
      }),
      Arc::new(InMemoryMetadataStore::new()),
      lease_store.clone(),
      "node-a".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .membership_rx(membership_rx)
    .time_provider(time_provider.clone())
    .build()?,
  );

  let produce_engine = Arc::clone(&engine);
  let produce = tokio::spawn(async move {
    produce_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  receive_blob_write(&mut entered_rx).await;

  let sleep_registrations = time_provider.sleep_registration_count();
  let shutdown = tokio::spawn(async move {
    shutdown_trigger.shutdown().await;
  });
  wait_for_partition_draining_start(&engine, 0).await;

  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
  };
  let held_by_a = lease_store
    .acquire_lease(
      key.clone(),
      "node-b".to_string(),
      "node-b-session".to_string(),
      offset_datetime_from_unix_millis(now_ms),
      TimeDuration::milliseconds(30_000),
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(held_by_a, LeaseAcquireOutcome::HeldByOther(_)));
  assert!(!shutdown.is_finished());

  let heartbeat_sleep = tokio::time::timeout(
    StdDuration::from_secs(1),
    time_provider.wait_for_sleep_registration_after(sleep_registrations),
  )
  .await
  .expect("drain did not schedule a heartbeat");
  time_provider.advance(TimeDuration::seconds(5));
  tokio::time::timeout(
    StdDuration::from_secs(1),
    time_provider.wait_for_sleep_registration_after(heartbeat_sleep),
  )
  .await
  .expect("drain heartbeat did not complete and reschedule");
  time_provider.advance(TimeDuration::seconds(6));

  let held_after_heartbeat = lease_store
    .acquire_lease(
      key.clone(),
      "node-b".to_string(),
      "node-b-session".to_string(),
      time_provider.now(),
      TimeDuration::milliseconds(30_000),
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(
    held_after_heartbeat,
    LeaseAcquireOutcome::HeldByOther(_)
  ));

  release.add_permits(1);
  produce.await??;
  tokio::time::timeout(StdDuration::from_secs(1), shutdown)
    .await
    .expect("component shutdown did not complete after flush drained")?;

  let acquired_by_b = lease_store
    .acquire_lease(
      key,
      "node-b".to_string(),
      "node-b-session".to_string(),
      time_provider.now(),
      TimeDuration::milliseconds(30_000),
      ProducerSequenceProgress::default(),
    )
    .await?;
  assert!(matches!(acquired_by_b, LeaseAcquireOutcome::Acquired(_)));
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_groups_ready_topics_into_one_durable_plan() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay = TimeDuration::milliseconds(10);

  let mut topics = HashMap::new();
  for topic in ["first", "second"] {
    topics.insert(
      topic.into(),
      TopicInfo {
        name: topic.into(),
        partition_count: 1,
        num_writers: 1,
        retention: TimeDuration::days(7),
        max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
        metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
      },
    );
  }

  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release_first = Arc::new(Semaphore::new(0));
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config.clone(),
      topics,
      Arc::new(GatedBlobStore {
        entered_tx,
        release_first: Arc::clone(&release_first),
        first_write: AtomicBool::new(true),
      }),
      Arc::new(InMemoryMetadataStore::new()),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider.clone())
    .build()?,
  );

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "first".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "second".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  tokio::task::yield_now().await;
  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;

  let blob_key = receive_blob_write(&mut entered_rx).await;
  assert!(blob_key.starts_with("shared/"));

  release_first.add_permits(1);
  first.await??;
  second.await??;
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_groups_topics_into_one_durable_plan() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let collector = Collector::default();
  let metrics = Helper::new_with_collector(collector.clone());
  let scope = collector.scope("blob_stream_broker_test");
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay = TimeDuration::milliseconds(10);

  let topics = (0 .. 5)
    .map(|index| {
      let topic: Chars = format!("topic-{index}").into();
      (
        topic.clone(),
        TopicInfo {
          name: topic,
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
          metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
        },
      )
    })
    .collect();
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config.clone(),
      topics,
      Arc::new(BlockingBlobStore {
        entered_tx,
        release: Arc::clone(&release),
      }),
      Arc::new(InMemoryMetadataStore::new()),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &scope,
    )
    .time_provider(time_provider.clone())
    .build()?,
  );

  let mut writes = Vec::new();
  for index in 0 .. 5 {
    let engine = Arc::clone(&engine);
    writes.push(tokio::spawn(async move {
      engine
        .produce_batch(WriteRequest {
          topic: format!("topic-{index}").into(),
          virtual_partition_id: 0,
          records: vec![new_record(vec![index], i64::from(index))],
        })
        .await
    }));
  }
  for _ in 0 .. 100 {
    let buffered_batches = engine
      .state_snapshot()
      .await
      .topics
      .iter()
      .flat_map(|topic| &topic.local_partitions)
      .map(|partition| partition.buffered_batch_count)
      .sum::<usize>();
    if buffered_batches == 5 {
      break;
    }
    tokio::task::yield_now().await;
  }

  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;

  let blob_key = receive_blob_write(&mut entered_rx).await;
  assert!(blob_key.starts_with("shared/"));
  metrics.assert_gauge_eq(
    1,
    "blob_stream_broker_test:write:active_flush_plans",
    &labels!(),
  );
  release.add_permits(1);

  for write in writes {
    write.await??;
  }
  metrics.assert_gauge_eq(
    0,
    "blob_stream_broker_test:write:active_flush_plans",
    &labels!(),
  );
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_notifies_only_the_plan_that_failed() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay = TimeDuration::milliseconds(10);

  let mut topics = HashMap::new();
  for topic in ["first", "second"] {
    topics.insert(
      topic.into(),
      TopicInfo {
        name: topic.into(),
        partition_count: 1,
        num_writers: 1,
        retention: TimeDuration::days(7),
        max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
        metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
      },
    );
  }

  let engine = Arc::new(
    WriteEngineBuilder::new(
      config.clone(),
      topics,
      Arc::new(InMemoryBlobStore::new()),
      Arc::new(FailsTopicMetadataStore {
        failed_topic: "second".to_string(),
        inner: Arc::new(InMemoryMetadataStore::new()),
      }),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider.clone())
    .build()?,
  );

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "first".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "second".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });

  wait_for_buffered_partitions(&engine, &["first", "second"]).await;
  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;

  first.await??;
  assert!(second.await?.is_err());
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_shares_one_object_across_topics() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay = TimeDuration::milliseconds(10);
  let topics = ["first", "second"]
    .into_iter()
    .map(|topic| {
      (
        topic.into(),
        TopicInfo {
          name: topic.into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::seconds(30),
          metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
        },
      )
    })
    .collect();
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config.clone(),
      topics,
      Arc::new(InMemoryBlobStore::new()),
      metadata_store.clone(),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider.clone())
    .build()?,
  );

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "first".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "second".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  wait_for_buffered_partitions(&engine, &["first", "second"]).await;
  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;
  first.await??;
  second.await??;

  let window = Window::for_timestamp(time_provider.now(), DEFAULT_TEST_METADATA_WINDOW_SIZE);
  let first_segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("first"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  let second_segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("second"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(first_segments.len(), 1);
  assert_eq!(second_segments.len(), 1);
  assert_eq!(first_segments[0].blob_key, second_segments[0].blob_key);
  assert!(first_segments[0].blob_key.as_str().starts_with("shared/"));
  assert!(first_segments[0].segment_index.contains_key(&0));
  assert!(second_segments[0].segment_index.contains_key(&0));
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_retains_all_topics_when_shared_object_reaches_segment_cap() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay = TimeDuration::milliseconds(10);
  config.max_segment_bytes = 1;
  let topics = ["first", "second"]
    .into_iter()
    .map(|topic| {
      (
        topic.into(),
        TopicInfo {
          name: topic.into(),
          partition_count: 1,
          num_writers: 1,
          retention: TimeDuration::days(7),
          max_metadata_publication_lag: TimeDuration::seconds(30),
          metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
        },
      )
    })
    .collect();
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config.clone(),
      topics,
      Arc::new(InMemoryBlobStore::new()),
      metadata_store.clone(),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &metrics_scope(),
    )
    .time_provider(time_provider.clone())
    .build()?,
  );
  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "first".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "second".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  wait_for_buffered_partitions(&engine, &["first", "second"]).await;
  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;
  first.await??;
  second.await??;

  let window = Window::for_timestamp(time_provider.now(), DEFAULT_TEST_METADATA_WINDOW_SIZE);
  let first_segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("first"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  let second_segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("second"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(first_segments.len(), 1);
  assert_eq!(second_segments.len(), 1);
  assert_ne!(first_segments[0].blob_key, second_segments[0].blob_key);
  assert!(first_segments[0].blob_key.as_str().starts_with("shared/"));
  assert!(second_segments[0].blob_key.as_str().starts_with("shared/"));
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_caps_serialized_object_size() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.max_segment_bytes = 1;
  config.flush_max_delay = TimeDuration::milliseconds(10);
  let metadata_window_size = TimeDuration::seconds(60);
  let (engine, metadata_store) = make_two_partition_engine_with_metadata_window_size(
    time_provider.clone(),
    config.clone(),
    metadata_window_size,
    shutdown_trigger.make_handle(),
  )?;

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 1,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  wait_for_buffered_partitions(&engine, &["telemetry"]).await;
  time_provider.advance(config.flush_max_delay);
  tokio::time::advance(std_duration(config.flush_max_delay)).await;
  first.await??;
  second.await??;

  let window = Window::for_timestamp(time_provider.now(), metadata_window_size);
  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 2);
  assert!(
    segments
      .iter()
      .all(|segment| segment.segment_index.len() == 1)
  );
  assert_ne!(segments[0].blob_key, segments[1].blob_key);
  Ok(())
}

#[tokio::test]
async fn assigns_monotonic_sequences() -> Result<()> {
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay = TimeDuration::milliseconds(60_000);

  let (engine, _metadata_store) = make_engine(
    time_provider.clone(),
    config,
    shutdown_trigger.make_handle(),
  )?;

  let request = WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![9; 2], 10), new_record(vec![9; 2], 11)],
  };

  let response = engine.produce_batch(request).await?;
  assert_eq!(response.seq_range, SeqRange { start: 0, end: 1 });

  let request = WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![10; 3], 12)],
  };

  let response = engine.produce_batch(request).await?;
  assert_eq!(response.seq_range, SeqRange { start: 2, end: 2 });
  Ok(())
}

#[tokio::test]
async fn writes_compressed_metadata() -> Result<()> {
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 5;
  config.flush_max_delay = TimeDuration::milliseconds(60_000);
  let metadata_window_size = TimeDuration::seconds(60);

  let (engine, metadata_store) = make_engine_with_metadata_window_size(
    time_provider.clone(),
    config.clone(),
    metadata_window_size,
    shutdown_trigger.make_handle(),
  )?;

  let request = WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![7; 6], 10)],
  };

  engine.produce_batch(request).await?;

  let window = Window::for_timestamp(time_provider.now(), metadata_window_size);
  let segments = metadata_store
    .scan_window_from_snowflake(
      &window.key("telemetry"),
      None,
      MetadataReadConsistency::Eventual,
    )
    .await?;
  assert_eq!(segments.len(), 1);

  assert_eq!(segments[0].compression.codec, CompressionCodec::Zstd);
  Ok(())
}

#[derive(Default)]
struct FailingMetadataStore;

#[async_trait]
impl MetadataStore for FailingMetadataStore {
  async fn write_segment(
    &self,
    _metadata: SegmentMetadata,
    _fences: Option<&[ProducerPartitionFence]>,
    _now_ts_ms: i64,
  ) -> MetadataWriteResult {
    Err(anyhow::anyhow!("metadata write failed").into())
  }

  async fn scan_window_from_snowflake(
    &self,
    _window: &blob_stream_types::TopicWindowKey,
    _min_snowflake: Option<SnowflakeId>,
    _consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    Ok(Vec::new())
  }
}

#[tokio::test]
async fn returns_error_when_flush_fails() -> Result<()> {
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".into(),
    TopicInfo {
      name: "telemetry".into(),
      partition_count: 1,
      num_writers: 1,
      retention: TimeDuration::days(7),
      max_metadata_publication_lag: TimeDuration::milliseconds(30_000),
      metadata_window_size: DEFAULT_TEST_METADATA_WINDOW_SIZE,
    },
  );

  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay = TimeDuration::milliseconds(60_000);

  let engine = WriteEngineBuilder::new(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(FailingMetadataStore),
    Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    "test-node".to_string(),
    shutdown_trigger.make_handle(),
    &metrics_scope(),
  )
  .time_provider(time_provider)
  .build()?;

  let request = WriteRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1, 2, 3], 10)],
  };

  let result = engine.produce_batch(request).await;
  assert!(result.is_err());
  Ok(())
}
