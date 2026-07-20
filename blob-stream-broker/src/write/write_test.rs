#![allow(clippy::unwrap_used)]

use super::{TopicInfo, WriteConfig, WriteEngine, WriteEngineImpl, WriteRequest};
use anyhow::Result;
use async_trait::async_trait;
use bd_server_stats::stats::{Collector, Scope};
use bd_time::{OffsetDateTimeExt, TestTimeProvider, TimeProvider};
use blob_stream_blob_store::{BlobKey, BlobStore, ByteRange, InMemoryBlobStore};
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  MetadataStore,
  ProducerPartitionLease,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SegmentMetadata,
  SequenceReservationOutcome,
};
use blob_stream_types::{CompressionCodec, SeqRange, SnowflakeId, Window, new_record};
use bytes::Bytes;
use serde_json::to_value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration as StdDuration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::{Semaphore, mpsc, watch};

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
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()> {
    if metadata.window.topic == self.failed_topic {
      return Err(anyhow::anyhow!("metadata write failed"));
    }
    self.inner.write_segment(metadata).await
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &blob_stream_types::TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>> {
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake)
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

  async fn get_range(&self, _key: &BlobKey, _range: ByteRange) -> Result<Bytes> {
    Err(anyhow::anyhow!("reads are not used by this test"))
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

  async fn get_range(&self, _key: &BlobKey, _range: ByteRange) -> Result<Bytes> {
    Err(anyhow::anyhow!("reads are not used by this test"))
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
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseAcquireOutcome> {
    self
      .inner
      .acquire_lease(key, holder_id, now_ts_ms, lease_duration_ms)
      .await
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseHeartbeatOutcome> {
    self
      .inner
      .heartbeat_lease(key, holder_id, now_ts_ms, lease_duration_ms)
      .await
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
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
      .reserve_sequences(key, holder_id, now_ts_ms, reservation_size)
      .await
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
  ) -> Result<LeaseReleaseOutcome> {
    self.inner.release_lease(key, holder_id, now_ts_ms).await
  }
}

fn time_from_ms(ms: i64) -> OffsetDateTime {
  OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
    .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_broker_test")
}

fn make_engine(
  time_provider: Arc<TestTimeProvider>,
  config: WriteConfig,
) -> Result<(Arc<WriteEngineImpl>, Arc<InMemoryMetadataStore>)> {
  let (engine, metadata_store, _lease_store) = make_engine_with_lease_store(time_provider, config)?;
  Ok((engine, metadata_store))
}

fn make_engine_with_lease_store(
  time_provider: Arc<TestTimeProvider>,
  config: WriteConfig,
) -> Result<(
  Arc<WriteEngineImpl>,
  Arc<InMemoryMetadataStore>,
  Arc<InMemoryProducerPartitionLeaseStore>,
)> {
  let scope = metrics_scope();
  make_engine_with_lease_store_and_scope(time_provider, config, &scope)
}

fn make_engine_with_lease_store_and_scope(
  time_provider: Arc<TestTimeProvider>,
  config: WriteConfig,
  metrics_scope: &Scope,
) -> Result<(
  Arc<WriteEngineImpl>,
  Arc<InMemoryMetadataStore>,
  Arc<InMemoryProducerPartitionLeaseStore>,
)> {
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".to_string(),
    TopicInfo {
      name: "telemetry".to_string(),
      partition_count: 1,
      num_writers: 1,
      retention_days: 7,
      max_metadata_publication_lag_ms: 30_000,
    },
  );

  let blob_store = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());

  let engine = WriteEngineImpl::new_with_time_provider(
    config,
    topics,
    blob_store,
    metadata_store.clone(),
    lease_store.clone(),
    "test-node".to_string(),
    None,
    time_provider,
    metrics_scope,
  )?;

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

#[tokio::test]
async fn state_snapshot_reports_local_buffer_and_lease_state() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ms)));
  let mut config = WriteConfig::with_defaults();
  config.writer_id = 0;
  config.flush_max_bytes = 1024;
  config.flush_max_delay_ms = 60_000;

  let (engine, _metadata_store) = make_engine(time_provider, config)?;
  let pending_engine = Arc::clone(&engine);
  let pending_write = tokio::spawn(async move {
    pending_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".to_string(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1, 2, 3], 10)],
      })
      .await
  });

  tokio::task::yield_now().await;

  let snapshot = engine.state_snapshot().await;
  assert_eq!(snapshot.generated_at, "2023-11-14T22:13:20Z");
  assert_eq!(snapshot.holder_id, "test-node");
  assert_eq!(snapshot.writer_id, 0);
  assert_eq!(snapshot.membership.len(), 1);
  assert_eq!(snapshot.membership[0].node_id, "test-node");
  assert_eq!(snapshot.membership[0].address, "test-node");
  assert_eq!(snapshot.ownership.len(), 1);
  assert!(snapshot.ownership[0].assignment_is_local);
  assert_eq!(
    snapshot.ownership[0].lease_status,
    super::BrokerLeaseStatus::LocalActive
  );
  assert_eq!(snapshot.topics.len(), 1);

  let topic = &snapshot.topics[0];
  assert_eq!(topic.name, "telemetry");
  assert_eq!(topic.partition_count, 1);
  assert_eq!(topic.num_writers, 1);
  assert_eq!(topic.local_partitions.len(), 1);

  let partition = &topic.local_partitions[0];
  assert_eq!(partition.virtual_partition_id, 0);
  assert_eq!(
    partition.lease_expires_at.as_deref(),
    Some("2023-11-14T22:13:50Z")
  );
  assert_eq!(partition.buffered_batch_count, 1);
  assert_eq!(partition.buffered_record_count, 1);
  assert_eq!(partition.buffered_bytes, 3);
  assert_eq!(
    partition.first_buffered_at.as_deref(),
    Some("2023-11-14T22:13:20Z")
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
async fn state_snapshot_reports_expired_observed_lease() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ms)));
  let mut config = WriteConfig::with_defaults();
  config.writer_id = 0;

  let (engine, _metadata_store, lease_store) = make_engine_with_lease_store(time_provider, config)?;
  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
  };
  lease_store
    .acquire_lease(key, "former-owner".to_string(), now_ms - 1_000, 100)
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
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay_ms = 60_000;

  let collector = Collector::default();
  let scope = collector.scope("blob_stream_broker_test");
  let (engine, _metadata_store, _lease_store) =
    make_engine_with_lease_store_and_scope(time_provider, config, &scope)?;
  engine
    .produce_batch(WriteRequest {
      topic: "telemetry".to_string(),
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

  Ok(())
}

#[tokio::test]
async fn buffers_until_size_rollover() -> Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 10;
  config.flush_max_delay_ms = 60_000;
  config.window_size_seconds = 60;

  let (engine, metadata_store) = make_engine(time_provider.clone(), config.clone())?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1; 6], 10)],
  };

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move { first_engine.produce_batch(request).await });

  tokio::task::yield_now().await;
  assert!(!first.is_finished());

  let window = Window::for_timestamp(
    time_provider.now().unix_timestamp_ms() / 1_000,
    config.window_size_seconds,
  );
  let segments = metadata_store
    .scan_window_from_snowflake(&window.key("telemetry"), None)
    .await?;
  assert!(segments.is_empty());

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![2; 6], 20)],
  };

  engine.produce_batch(request).await?;
  first.await??;

  let segments = metadata_store
    .scan_window_from_snowflake(&window.key("telemetry"), None)
    .await?;
  assert_eq!(segments.len(), 1);
  assert_eq!(
    segments[0].segment_index[&0]
      .iter()
      .map(|batch| batch.summary.record_count)
      .sum::<u32>(),
    2
  );
  Ok(())
}

#[tokio::test]
async fn same_partition_requests_serialize_sequence_reservations() -> Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
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
  let engine = Arc::new(WriteEngineImpl::new_with_time_provider(
    config,
    HashMap::from([(
      "telemetry".to_string(),
      TopicInfo {
        name: "telemetry".to_string(),
        partition_count: 1,
        num_writers: 1,
        retention_days: 7,
        max_metadata_publication_lag_ms: 30_000,
      },
    )]),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store,
    "test-node".to_string(),
    None,
    time_provider,
    &metrics_scope(),
  )?);

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".to_string(),
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
        topic: "telemetry".to_string(),
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
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
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
  let engine = Arc::new(WriteEngineImpl::new_with_time_provider(
    config,
    HashMap::from([(
      "telemetry".to_string(),
      TopicInfo {
        name: "telemetry".to_string(),
        partition_count: 1,
        num_writers: 1,
        retention_days: 7,
        max_metadata_publication_lag_ms: 30_000,
      },
    )]),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    lease_store,
    "test-node".to_string(),
    None,
    time_provider,
    &metrics_scope(),
  )?);

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".to_string(),
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
    partition.allocation_started_at.as_deref(),
    Some("2023-11-14T22:13:20Z")
  );

  first.abort();
  assert!(first.await.is_err(), "reservation task should be cancelled");

  let response = tokio::time::timeout(
    StdDuration::from_millis(100),
    engine.produce_batch(WriteRequest {
      topic: "telemetry".to_string(),
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
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1024;
  config.flush_max_delay_ms = 500;
  config.window_size_seconds = 60;

  let (engine, metadata_store) = make_engine(time_provider.clone(), config.clone())?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![3; 4], 30)],
  };

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move { first_engine.produce_batch(request).await });
  tokio::task::yield_now().await;
  assert!(!first.is_finished());

  let advance = TimeDuration::milliseconds(config.flush_max_delay_ms + 10);
  time_provider.advance(advance);
  tokio::time::advance(StdDuration::from_millis(
    (config.flush_max_delay_ms + 10).cast_unsigned(),
  ))
  .await;
  tokio::task::yield_now().await;

  first.await??;

  let window = Window::for_timestamp(
    time_provider.now().unix_timestamp_ms() / 1_000,
    config.window_size_seconds,
  );
  let segments = metadata_store
    .scan_window_from_snowflake(&window.key("telemetry"), None)
    .await?;
  assert_eq!(segments.len(), 1);
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_collects_later_plans_while_a_prior_plan_is_in_flight() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ms)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay_ms = 10;

  let mut topics = HashMap::new();
  for topic in ["first", "second"] {
    topics.insert(
      topic.to_string(),
      TopicInfo {
        name: topic.to_string(),
        partition_count: 1,
        num_writers: 1,
        retention_days: 7,
        max_metadata_publication_lag_ms: 30_000,
      },
    );
  }

  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release_first = Arc::new(Semaphore::new(0));
  let engine = Arc::new(WriteEngineImpl::new_with_time_provider(
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
    None,
    time_provider.clone(),
    &metrics_scope(),
  )?);

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "first".to_string(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  tokio::task::yield_now().await;
  time_provider.advance(TimeDuration::milliseconds(config.flush_max_delay_ms));
  tokio::time::advance(StdDuration::from_millis(
    config.flush_max_delay_ms.cast_unsigned(),
  ))
  .await;
  assert!(receive_blob_write(&mut entered_rx).await.contains("first/"));

  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "second".to_string(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  tokio::task::yield_now().await;
  time_provider.advance(TimeDuration::milliseconds(config.flush_max_delay_ms));
  tokio::time::advance(StdDuration::from_millis(
    config.flush_max_delay_ms.cast_unsigned(),
  ))
  .await;

  assert!(
    receive_blob_write(&mut entered_rx)
      .await
      .contains("second/")
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
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ms)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay_ms = 10;

  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release_first = Arc::new(Semaphore::new(0));
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let engine = Arc::new(WriteEngineImpl::new_with_time_provider(
    config.clone(),
    HashMap::from([(
      "telemetry".to_string(),
      TopicInfo {
        name: "telemetry".to_string(),
        partition_count: 1,
        num_writers: 1,
        retention_days: 7,
        max_metadata_publication_lag_ms: 30_000,
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
    None,
    time_provider.clone(),
    &metrics_scope(),
  )?);

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".to_string(),
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
        topic: "telemetry".to_string(),
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

  let window = Window::for_timestamp(
    time_provider.now().unix_timestamp_ms() / 1_000,
    config.window_size_seconds,
  );
  let segments = metadata_store
    .scan_window_from_snowflake(&window.key("telemetry"), None)
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
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ms)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay_ms = 60_000;

  let (membership_tx, membership_rx) = watch::channel(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".to_string(),
    address: "10.0.0.1:8080".to_string(),
  }]));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let lease_store = Arc::new(InMemoryProducerPartitionLeaseStore::new());
  let engine = Arc::new(WriteEngineImpl::new_with_time_provider(
    config,
    HashMap::from([(
      "telemetry".to_string(),
      TopicInfo {
        name: "telemetry".to_string(),
        partition_count: 1,
        num_writers: 1,
        retention_days: 7,
        max_metadata_publication_lag_ms: 30_000,
      },
    )]),
    Arc::new(BlockingBlobStore {
      entered_tx,
      release: Arc::clone(&release),
    }),
    metadata_store.clone(),
    lease_store.clone(),
    "node-a".to_string(),
    Some(membership_rx),
    time_provider.clone(),
    &metrics_scope(),
  )?);

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "telemetry".to_string(),
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
        topic: "telemetry".to_string(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  tokio::task::yield_now().await;
  assert!(!buffered.is_finished());

  membership_tx.send(BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".to_string(),
    address: "10.0.0.2:8080".to_string(),
  }]))?;
  wait_for_partition_draining_start(&engine).await;

  let second = engine
    .produce_batch(WriteRequest {
      topic: "telemetry".to_string(),
      virtual_partition_id: 0,
      records: vec![new_record(vec![3], 30)],
    })
    .await;
  assert!(matches!(
    second,
    Err(super::WriteError::NotLeaseHolder { .. })
  ));

  let key = ProducerPartitionLeaseKey {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
  };
  let held_by_a = lease_store
    .acquire_lease(key.clone(), "node-b".to_string(), now_ms, 30_000)
    .await?;
  assert!(matches!(held_by_a, LeaseAcquireOutcome::HeldByOther(_)));

  release.add_permits(1);
  first.await??;
  receive_blob_write(&mut entered_rx).await;

  let still_held_by_a = lease_store
    .acquire_lease(key.clone(), "node-b".to_string(), now_ms, 30_000)
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
      .acquire_lease(key.clone(), "node-b".to_string(), now_ms, 30_000)
      .await?;
    if matches!(outcome, LeaseAcquireOutcome::Acquired(_)) {
      acquired_by_b = true;
      break;
    }
    tokio::task::yield_now().await;
  }
  assert!(acquired_by_b, "lease was not released after drain");

  let window = Window::for_timestamp(
    time_provider.now().unix_timestamp_ms() / 1_000,
    WriteConfig::with_defaults().window_size_seconds,
  );
  let segments = metadata_store
    .scan_window_from_snowflake(&window.key("telemetry"), None)
    .await?;
  assert_eq!(segments.len(), 2);
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn flush_scheduler_dispatches_independent_ready_plans_concurrently() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ms)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay_ms = 10;

  let mut topics = HashMap::new();
  for topic in ["first", "second"] {
    topics.insert(
      topic.to_string(),
      TopicInfo {
        name: topic.to_string(),
        partition_count: 1,
        num_writers: 1,
        retention_days: 7,
        max_metadata_publication_lag_ms: 30_000,
      },
    );
  }

  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release_first = Arc::new(Semaphore::new(0));
  let engine = Arc::new(WriteEngineImpl::new_with_time_provider(
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
    None,
    time_provider.clone(),
    &metrics_scope(),
  )?);

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "first".to_string(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "second".to_string(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });
  tokio::task::yield_now().await;
  time_provider.advance(TimeDuration::milliseconds(config.flush_max_delay_ms));
  tokio::time::advance(StdDuration::from_millis(
    config.flush_max_delay_ms.cast_unsigned(),
  ))
  .await;

  let first_blob_key = receive_blob_write(&mut entered_rx).await;
  let second_blob_key = receive_blob_write(&mut entered_rx).await;
  assert_ne!(first_blob_key, second_blob_key);

  release_first.add_permits(1);
  first.await??;
  second.await??;
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn flush_scheduler_rotates_topics_when_capacity_is_limited() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ms)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay_ms = 10;

  let topics = (0 .. 5)
    .map(|index| {
      let topic = format!("topic-{index}");
      (
        topic.clone(),
        TopicInfo {
          name: topic,
          partition_count: 1,
          num_writers: 1,
          retention_days: 7,
          max_metadata_publication_lag_ms: 30_000,
        },
      )
    })
    .collect();
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let engine = Arc::new(WriteEngineImpl::new_with_time_provider(
    config.clone(),
    topics,
    Arc::new(BlockingBlobStore {
      entered_tx,
      release: Arc::clone(&release),
    }),
    Arc::new(InMemoryMetadataStore::new()),
    Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    "test-node".to_string(),
    None,
    time_provider.clone(),
    &metrics_scope(),
  )?);

  let mut writes = Vec::new();
  for index in 0 .. 5 {
    let engine = Arc::clone(&engine);
    writes.push(tokio::spawn(async move {
      engine
        .produce_batch(WriteRequest {
          topic: format!("topic-{index}"),
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

  time_provider.advance(TimeDuration::milliseconds(config.flush_max_delay_ms));
  tokio::time::advance(StdDuration::from_millis(
    config.flush_max_delay_ms.cast_unsigned(),
  ))
  .await;

  let mut first_topics = Vec::new();
  for _ in 0 .. 4 {
    first_topics.push(receive_blob_write(&mut entered_rx).await);
  }
  first_topics.sort();
  assert!(
    first_topics
      .iter()
      .zip(0 .. 4)
      .all(|(key, expected_topic)| key.contains(&format!("topic-{expected_topic}/")))
  );
  release.add_permits(1);
  assert!(
    receive_blob_write(&mut entered_rx)
      .await
      .contains("topic-4/")
  );
  release.add_permits(4);

  for write in writes {
    write.await??;
  }
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_flush_notifies_only_the_plan_that_failed() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ms)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1_024;
  config.flush_max_delay_ms = 10;

  let mut topics = HashMap::new();
  for topic in ["first", "second"] {
    topics.insert(
      topic.to_string(),
      TopicInfo {
        name: topic.to_string(),
        partition_count: 1,
        num_writers: 1,
        retention_days: 7,
        max_metadata_publication_lag_ms: 30_000,
      },
    );
  }

  let engine = Arc::new(WriteEngineImpl::new_with_time_provider(
    config.clone(),
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(FailsTopicMetadataStore {
      failed_topic: "second".to_string(),
      inner: Arc::new(InMemoryMetadataStore::new()),
    }),
    Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    "test-node".to_string(),
    None,
    time_provider.clone(),
    &metrics_scope(),
  )?);

  let first_engine = Arc::clone(&engine);
  let first = tokio::spawn(async move {
    first_engine
      .produce_batch(WriteRequest {
        topic: "first".to_string(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 10)],
      })
      .await
  });
  let second_engine = Arc::clone(&engine);
  let second = tokio::spawn(async move {
    second_engine
      .produce_batch(WriteRequest {
        topic: "second".to_string(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![2], 20)],
      })
      .await
  });

  tokio::task::yield_now().await;
  time_provider.advance(TimeDuration::milliseconds(config.flush_max_delay_ms));
  tokio::time::advance(StdDuration::from_millis(
    config.flush_max_delay_ms.cast_unsigned(),
  ))
  .await;

  first.await??;
  assert!(second.await?.is_err());
  Ok(())
}

#[tokio::test]
async fn assigns_monotonic_sequences() -> Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay_ms = 60_000;

  let (engine, _metadata_store) = make_engine(time_provider.clone(), config)?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![9; 2], 10), new_record(vec![9; 2], 11)],
  };

  let response = engine.produce_batch(request).await?;
  assert_eq!(response.seq_range, SeqRange { start: 0, end: 1 });

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![10; 3], 12)],
  };

  let response = engine.produce_batch(request).await?;
  assert_eq!(response.seq_range, SeqRange { start: 2, end: 2 });
  Ok(())
}

#[tokio::test]
async fn writes_compressed_metadata() -> Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 5;
  config.flush_max_delay_ms = 60_000;
  config.window_size_seconds = 60;

  let (engine, metadata_store) = make_engine(time_provider.clone(), config.clone())?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![7; 6], 10)],
  };

  engine.produce_batch(request).await?;

  let window = Window::for_timestamp(
    time_provider.now().unix_timestamp_ms() / 1_000,
    config.window_size_seconds,
  );
  let segments = metadata_store
    .scan_window_from_snowflake(&window.key("telemetry"), None)
    .await?;
  assert_eq!(segments.len(), 1);

  let batch_metadata = segments[0].segment_index.get(&0).unwrap().first().unwrap();
  assert_eq!(batch_metadata.compression.codec, CompressionCodec::Zstd);
  Ok(())
}

#[derive(Default)]
struct FailingMetadataStore;

#[async_trait]
impl MetadataStore for FailingMetadataStore {
  async fn write_segment(&self, _metadata: SegmentMetadata) -> Result<()> {
    Err(anyhow::anyhow!("metadata write failed"))
  }

  async fn scan_window_from_snowflake(
    &self,
    _window: &blob_stream_types::TopicWindowKey,
    _min_snowflake: Option<SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>> {
    Ok(Vec::new())
  }
}

#[tokio::test]
async fn returns_error_when_flush_fails() -> Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(1_700_000_000_000)));
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".to_string(),
    TopicInfo {
      name: "telemetry".to_string(),
      partition_count: 1,
      num_writers: 1,
      retention_days: 7,
      max_metadata_publication_lag_ms: 30_000,
    },
  );

  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay_ms = 60_000;

  let engine = WriteEngineImpl::new_with_time_provider(
    config,
    topics,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(FailingMetadataStore),
    Arc::new(InMemoryProducerPartitionLeaseStore::new()),
    "test-node".to_string(),
    None,
    time_provider,
    &metrics_scope(),
  )?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1, 2, 3], 10)],
  };

  let result = engine.produce_batch(request).await;
  assert!(result.is_err());
  Ok(())
}
