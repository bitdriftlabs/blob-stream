#![allow(clippy::unwrap_used)]

use super::allocation::{
  AllocationTransitionDecision,
  LeaseExpirationUpdate,
  begin_allocation_transition,
};
use super::state::WriteState;
use super::{
  TopicInfo,
  WriteConfig,
  WriteEngine,
  WriteEngineBuilder,
  WriteEngineImpl,
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
  SegmentMetadata,
  SequenceReservationOutcome,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{
  CompressionCodec,
  SeqRange,
  SnowflakeId,
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
use std::sync::atomic::{AtomicBool, Ordering};
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

struct GatedReservationLeaseStore {
  inner: InMemoryProducerPartitionLeaseStore,
  entered_tx: mpsc::UnboundedSender<()>,
  release_first: Arc<Semaphore>,
  first_reservation: AtomicBool,
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
  ) -> Result<LeaseAcquireOutcome> {
    self
      .inner
      .acquire_lease(key, holder_id, lease_session_id, now, lease_duration)
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
  ) -> Result<LeaseHeartbeatOutcome> {
    self
      .inner
      .heartbeat_lease(key, holder_id, lease_session_id, now, lease_duration)
      .await
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    reservation_size: u64,
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
      .reserve_sequences(key, holder_id, lease_session_id, now, reservation_size)
      .await
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
  ) -> Result<LeaseReleaseOutcome> {
    self
      .inner
      .release_lease(key, holder_id, lease_session_id, now)
      .await
  }
}

fn std_duration(duration: TimeDuration) -> StdDuration {
  StdDuration::try_from(duration).unwrap()
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_broker_test")
}

#[test]
fn foreground_exhaustion_doubles_the_adaptive_reservation_target() {
  let state = Arc::new(parking_lot::Mutex::new(WriteState::default()));
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
  let state = Arc::new(parking_lot::Mutex::new(WriteState::default()));
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
  let state = Arc::new(parking_lot::Mutex::new(WriteState::default()));
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
  let state = Arc::new(parking_lot::Mutex::new(WriteState::default()));
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
  let state = Arc::new(parking_lot::Mutex::new(WriteState::default()));
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

async fn wait_for_partition_draining_start(engine: &WriteEngineImpl) {
  for _ in 0 .. 100 {
    let draining = {
      let state = engine.state.lock();
      state
        .topics
        .get("telemetry")
        .and_then(|topic_state| topic_state.partitions.get(&0))
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
    .reserve_sequences("telemetry", 1, offset_datetime_from_unix_millis(now_ms), 1)
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
async fn same_partition_flush_waits_for_prior_plan_to_persist() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    now_ms,
  )));
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
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
  receive_blob_write(&mut entered_rx).await;

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
  tokio::task::yield_now().await;
  assert!(entered_rx.try_recv().is_err());
  assert!(!second.is_finished());

  release_first.add_permits(1);
  first.await??;
  receive_blob_write(&mut entered_rx).await;
  second.await??;

  let window = Window::for_timestamp(time_provider.now(), TimeDuration::seconds(60));
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

#[tokio::test]
async fn membership_handoff_drains_in_flight_flush_before_releasing_lease() -> Result<()> {
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
  assert!(!buffered.is_finished());

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  }]))?;
  wait_for_partition_draining_start(&engine).await;

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
    )
    .await?;
  assert!(matches!(held_by_a, LeaseAcquireOutcome::HeldByOther(_)));

  release.add_permits(1);
  first.await??;
  receive_blob_write(&mut entered_rx).await;

  let still_held_by_a = lease_store
    .acquire_lease(
      key.clone(),
      "node-b".to_string(),
      "node-b-session".to_string(),
      offset_datetime_from_unix_millis(now_ms),
      TimeDuration::milliseconds(30_000),
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
    .time_provider(time_provider)
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

  let shutdown = tokio::spawn(async move {
    shutdown_trigger.shutdown().await;
  });
  wait_for_partition_draining_start(&engine).await;

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
    )
    .await?;
  assert!(matches!(held_by_a, LeaseAcquireOutcome::HeldByOther(_)));
  assert!(!shutdown.is_finished());

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
      offset_datetime_from_unix_millis(now_ms),
      TimeDuration::milliseconds(30_000),
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
