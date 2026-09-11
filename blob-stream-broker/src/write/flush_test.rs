use super::{FlushContext, SnowflakeGenerator};
use crate::write::buffer::{
  BufferedBatch,
  FlushCompletionError,
  FlushPartition,
  FlushPartitionResult,
  FlushPlan,
  FlushPublicationDependency,
  FlushPublicationResult,
  FlushPublicationState,
  FlushTrigger,
  TopicFlushPlan,
};
use crate::write::metrics::WriteMetrics;
use crate::write::{BrokerLifecycleHooks, WriteConfig};
use anyhow::Result;
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use bd_server_stats::test::util::stats::Helper;
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
  MetadataWriteError,
  MetadataWriteResult,
  ProducerLeaseFence,
  ProducerPartitionFence,
  ProducerPartitionLeaseKey,
  SegmentMetadata,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{
  BatchSummary,
  SeqRange,
  SnowflakeId,
  VirtualPartitionId,
  Window,
  new_record,
};
use bytes::Bytes;
use prometheus::labels;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use time::OffsetDateTime;
use tokio::sync::{Semaphore, mpsc, watch};

async fn flush_plan_for_test(
  context: &FlushContext,
  plan: &mut FlushPlan,
  metrics: &WriteMetrics,
) -> Result<Vec<FlushPartitionResult>, crate::write::WriteError> {
  flush_plan_with_upload_permits_for_test(
    context,
    plan,
    metrics,
    Arc::new(Semaphore::new(crate::write::MAX_CONCURRENT_BLOB_UPLOADS)),
  )
  .await
}

async fn flush_plan_with_upload_permits_for_test(
  context: &FlushContext,
  plan: &mut FlushPlan,
  metrics: &WriteMetrics,
  blob_upload_permits: Arc<Semaphore>,
) -> Result<Vec<FlushPartitionResult>, crate::write::WriteError> {
  context
    .flush_plan_after(
      plan,
      metrics,
      &blob_upload_permits,
      &Arc::new(Semaphore::new(super::MAX_CONCURRENT_METADATA_WRITES)),
      &Arc::new(tokio::sync::Notify::new()),
    )
    .await
    .map(|completion| completion.partition_results)
}

struct BlockingMetadataStore {
  started_tx: mpsc::UnboundedSender<String>,
  release: Arc<Semaphore>,
}

struct LostFenceMetadataStore {
  calls: AtomicUsize,
}

struct FailingBlobStore;

struct BlockingBlobStore {
  started_tx: mpsc::UnboundedSender<()>,
  release: Arc<Semaphore>,
}

struct BlockingBeforeFlushPersistHook {
  started_tx: mpsc::UnboundedSender<()>,
  release: Arc<Semaphore>,
}

#[async_trait]
impl BrokerLifecycleHooks for BlockingBeforeFlushPersistHook {
  async fn before_flush_persist(&self, _topic: &str, _partitions: &[VirtualPartitionId]) {
    self
      .started_tx
      .send(())
      .expect("before flush persist receiver must remain available");
    self
      .release
      .acquire()
      .await
      .expect("before flush persist release gate must remain available")
      .forget();
  }
}

#[async_trait]
impl BlobStore for FailingBlobStore {
  async fn put(&self, _key: &BlobKey, _payload: Bytes) -> Result<()> {
    anyhow::bail!("blob write failed")
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
  async fn put(&self, _key: &BlobKey, _payload: Bytes) -> Result<()> {
    self
      .started_tx
      .send(())
      .map_err(|_| anyhow::anyhow!("blob upload receiver dropped"))?;
    self
      .release
      .acquire()
      .await
      .map_err(|_| anyhow::anyhow!("blob upload release gate closed"))?
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
impl MetadataStore for BlockingMetadataStore {
  async fn write_segment(
    &self,
    metadata: SegmentMetadata,
    _fences: Option<&[ProducerPartitionFence]>,
    _now_ts_ms: i64,
  ) -> MetadataWriteResult {
    self
      .started_tx
      .send(metadata.window.topic.clone())
      .map_err(|_| anyhow::anyhow!("metadata write receiver dropped"))?;
    self
      .release
      .acquire()
      .await
      .map_err(|_| anyhow::anyhow!("metadata write release gate closed"))?
      .forget();
    Ok(())
  }

  async fn scan_window_from_snowflake(
    &self,
    _window: &blob_stream_types::TopicWindowKey,
    _min_snowflake: Option<blob_stream_types::SnowflakeId>,
    _consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    Ok(Vec::new())
  }
}

#[async_trait]
impl MetadataStore for LostFenceMetadataStore {
  async fn write_segment(
    &self,
    _metadata: SegmentMetadata,
    fences: Option<&[ProducerPartitionFence]>,
    _now_ts_ms: i64,
  ) -> MetadataWriteResult {
    assert_eq!(
      fences,
      Some(&[ProducerPartitionFence {
        key: ProducerPartitionLeaseKey {
          topic: "telemetry".into(),
          virtual_partition_id: 4,
        },
        fence: lease_fence(),
      }] as &[ProducerPartitionFence])
    );
    self.calls.fetch_add(1, Ordering::Relaxed);
    Err(MetadataWriteError::ProducerLeaseFenceLost)
  }

  async fn scan_window_from_snowflake(
    &self,
    _window: &blob_stream_types::TopicWindowKey,
    _min_snowflake: Option<blob_stream_types::SnowflakeId>,
    _consistency: MetadataReadConsistency,
  ) -> Result<Vec<SegmentMetadata>> {
    Ok(Vec::new())
  }
}

fn lease_fence() -> ProducerLeaseFence {
  ProducerLeaseFence {
    holder_id: "broker-a".to_string(),
    lease_epoch: 1,
    lease_session_id: "session-a".to_string(),
  }
}

fn topic_flush_plan(
  topic: protobuf::Chars,
  virtual_partition_id: u32,
  payload: &[u8],
) -> TopicFlushPlan {
  TopicFlushPlan {
    topic,
    partitions: vec![flush_partition(virtual_partition_id, payload)],
    max_metadata_publication_lag: time::Duration::seconds(1),
    metadata_window_size: time::Duration::minutes(5),
    fenced_metadata_writes: false,
  }
}

fn flush_partition(virtual_partition_id: u32, payload: &[u8]) -> FlushPartition {
  FlushPartition {
    virtual_partition_id,
    lease_fence: None,
    batches: vec![BufferedBatch {
      records: vec![new_record(payload.to_vec(), 10)],
      summary: BatchSummary {
        record_count: 1,
        payload_bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
      },
      seq_range: SeqRange { start: 0, end: 0 },
      acceptance_fence: None,
      completion: None,
    }],
    trigger: FlushTrigger::MaxDelay,
    publication_predecessor: None,
    publication_state_tx: None,
  }
}

async fn receive_metadata_write(receiver: &mut mpsc::UnboundedReceiver<String>) -> String {
  for _ in 0 .. 100 {
    if let Ok(topic) = receiver.try_recv() {
      return topic;
    }
    tokio::task::yield_now().await;
  }
  panic!("expected metadata write did not begin");
}

async fn receive_blob_upload(receiver: &mut mpsc::UnboundedReceiver<()>) {
  for _ in 0 .. 100 {
    if receiver.try_recv().is_ok() {
      return;
    }
    tokio::task::yield_now().await;
  }
  panic!("expected blob upload did not begin");
}

#[tokio::test]
async fn default_machine_id_generator_initializes() -> Result<()> {
  let generator = SnowflakeGenerator::new()?;
  let time_provider = ManualTimeProvider::new(OffsetDateTime::now_utc());

  assert!(generator.next(&time_provider).await?.0.as_u64() > 0);
  Ok(())
}

#[tokio::test]
async fn explicit_machine_ids_produce_distinct_ids() -> Result<()> {
  let first = SnowflakeGenerator::with_machine_id(1)?;
  let second = SnowflakeGenerator::with_machine_id(2)?;
  let time_provider = ManualTimeProvider::new(OffsetDateTime::now_utc());

  assert_ne!(
    first.next(&time_provider).await?.0.as_u64(),
    second.next(&time_provider).await?.0.as_u64()
  );
  Ok(())
}

#[tokio::test]
async fn object_build_resnaps_time_after_sonyflake_sequence_overflow() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_099)?
    .saturating_add(time::Duration::milliseconds(995));
  let time_provider = Arc::new(ManualTimeProvider::new(now));
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    SnowflakeGenerator::with_machine_id(1)?,
    time_provider.clone(),
    None,
  );
  let mut topic = topic_flush_plan("telemetry".into(), 0, b"payload");
  topic.partitions = (0 .. 513)
    .map(|virtual_partition_id| flush_partition(virtual_partition_id, b"payload"))
    .collect();
  let plan = FlushPlan {
    topics: vec![topic],
    max_segment_bytes: 1,
    publication_completions: Vec::new(),
  };

  let collector = Collector::default();
  let metrics = WriteMetrics::new(&collector.scope("flush_test"));
  let objects = tokio::spawn(async move {
    let mut plan = plan;
    context.build_objects(&mut plan, &metrics).await
  });
  time_provider.wait_until_sleeping(1).await;
  time_provider.advance(time::Duration::milliseconds(10));
  let (objects, failures, _) = objects.await??;
  let last_topic = &objects.last().expect("513 bounded objects").topics[0];
  let refreshed_at = now.saturating_add(time::Duration::milliseconds(10));

  assert!(failures.is_empty());
  assert_eq!(objects.len(), 513);
  Helper::new_with_collector(collector).assert_counter_eq(
    512,
    "flush_test:write:flush_max_segment_size_splits_total",
    &labels!(),
  );
  assert_eq!(
    last_topic.envelope.snowflake_id.timestamp(),
    SnowflakeId::minimum_for_timestamp(refreshed_at).timestamp()
  );
  assert_eq!(last_topic.envelope.created_at, refreshed_at);
  assert_eq!(
    last_topic.envelope.window,
    Window::for_timestamp(refreshed_at, time::Duration::minutes(5)).key("telemetry")
  );
  Ok(())
}

#[test]
fn target_fill_boundaries_preserve_the_minimum_contiguous_object_count() {
  let target_fill_groups = |partition_bytes: &[u64], max_segment_bytes| {
    super::target_fill_object_groups(
      partition_bytes
        .iter()
        .copied()
        .map(|partition_bytes| (partition_bytes, partition_bytes))
        .collect(),
      max_segment_bytes,
    )
    .into_iter()
    .map(|group| {
      std::iter::once(group.first)
        .chain(group.remaining)
        .collect::<Vec<_>>()
    })
    .collect::<Vec<_>>()
  };

  assert!(target_fill_groups(&[], 100).is_empty());
  assert_eq!(
    target_fill_groups(&[50, 50, 50], 100),
    vec![vec![50, 50], vec![50]]
  );

  // This sequence was previously split into four objects by an early-close decision. The
  // suffix feasibility check retains the cap-minimum layout: [41, 56], [46], [61].
  assert_eq!(
    target_fill_groups(&[41, 56, 46, 61], 100),
    vec![vec![41, 56], vec![46], vec![61]]
  );
  assert_eq!(
    super::minimum_suffix_object_counts(&[41, 56, 46, 61], 100),
    vec![3, 3, 2, 1, 0]
  );

  // An earlier boundary remains useful when the suffix can still fit in its minimum three
  // objects. This retains four objects overall rather than falling back to next-fit packing.
  assert_eq!(
    target_fill_groups(&[3, 7, 1, 10, 1], 10),
    vec![vec![3], vec![7, 1], vec![10], vec![1]]
  );

  // An oversized partition is indivisible, but it must not cause a later bounded suffix to
  // inherit its conceptual cap units. The suffix remains [40, 60], [40].
  assert_eq!(
    target_fill_groups(&[120, 40, 60, 40], 100),
    vec![vec![120], vec![40, 60], vec![40]]
  );
}

#[tokio::test]
async fn object_build_balances_uneven_partitions_preserving_source_order() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let mut config = WriteConfig::with_defaults();
  config.compression = blob_stream_types::Compression::none();
  let context = FlushContext::new(
    config,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let mut telemetry = topic_flush_plan("telemetry".into(), 0, &[0; 100]);
  telemetry.partitions = vec![flush_partition(0, &[0; 100]), flush_partition(1, &[1; 100])];
  let mut diagnostics = topic_flush_plan("diagnostics".into(), 0, &[2; 10]);
  diagnostics.partitions = vec![flush_partition(2, &[2; 10]), flush_partition(3, &[3; 10])];
  let mut plan = FlushPlan {
    topics: vec![telemetry, diagnostics],
    max_segment_bytes: 220,
    publication_completions: Vec::new(),
  };
  let collector = Collector::default();
  let metrics = WriteMetrics::new(&collector.scope("flush_test"));

  let (objects, failures, _) = context.build_objects(&mut plan, &metrics).await?;

  assert!(failures.is_empty());
  assert_eq!(objects.len(), 2);
  assert!(objects.iter().all(|object| object.payload.len() <= 220));
  let partition_ids_in_serialized_order = |object: &super::PersistedObject| {
    let mut partitions = object
      .topics
      .iter()
      .flat_map(|topic| {
        topic
          .partitions
          .iter()
          .map(|(virtual_partition_id, partition)| {
            (
              partition.metadata.byte_range.start,
              topic.envelope.window.topic.clone(),
              *virtual_partition_id,
            )
          })
      })
      .collect::<Vec<_>>();
    partitions.sort_unstable();
    partitions
      .into_iter()
      .map(|(_, topic, virtual_partition_id)| (topic, virtual_partition_id))
      .collect::<Vec<_>>()
  };
  assert_eq!(
    partition_ids_in_serialized_order(&objects[0]),
    vec![("telemetry".to_string(), 0)]
  );
  assert_eq!(
    partition_ids_in_serialized_order(&objects[1]),
    vec![
      ("telemetry".to_string(), 1),
      ("diagnostics".to_string(), 2),
      ("diagnostics".to_string(), 3),
    ]
  );
  assert!(
    objects[0].payload.len() >= 100,
    "first object must contain the initial partition"
  );
  assert!(
    objects[1].payload.len() > objects[0].payload.len(),
    "target-fill must avoid the sequential full-object-plus-tiny-tail layout"
  );
  Helper::new_with_collector(collector).assert_counter_eq(
    1,
    "flush_test:write:flush_max_segment_size_splits_total",
    &labels!(),
  );
  Ok(())
}

#[tokio::test]
async fn oversized_partition_does_not_oversplit_later_partitions() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let mut config = WriteConfig::with_defaults();
  config.compression = blob_stream_types::Compression::none();
  let context = FlushContext::new(
    config,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let mut topic = topic_flush_plan("telemetry".into(), 0, &[0; 300]);
  topic.partitions = vec![
    flush_partition(0, &[0; 300]),
    flush_partition(1, &[1; 20]),
    flush_partition(2, &[2; 20]),
  ];
  let mut plan = FlushPlan {
    topics: vec![topic],
    max_segment_bytes: 100,
    publication_completions: Vec::new(),
  };
  let collector = Collector::default();
  let metrics = WriteMetrics::new(&collector.scope("flush_test"));

  let (objects, failures, _) = context.build_objects(&mut plan, &metrics).await?;

  assert!(failures.is_empty());
  assert_eq!(objects.len(), 2);
  assert!(objects[0].payload.len() > 100);
  assert!(objects[1].payload.len() <= 100);
  assert_eq!(objects[0].topics[0].partitions.len(), 1);
  assert_eq!(objects[1].topics[0].partitions.len(), 2);
  Helper::new_with_collector(collector).assert_counter_eq(
    1,
    "flush_test:write:flush_max_segment_size_splits_total",
    &labels!(),
  );
  Ok(())
}

#[tokio::test]
async fn every_object_uses_a_shared_blob_key() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let mut topic = topic_flush_plan("telemetry".into(), 0, b"first");
  topic.partitions.push(flush_partition(1, b"second"));
  let mut plan = FlushPlan {
    topics: vec![topic],
    max_segment_bytes: 64 * 1024 * 1024,
    publication_completions: Vec::new(),
  };

  let (objects, failures, _) = context
    .build_objects(
      &mut plan,
      &WriteMetrics::new(&Collector::default().scope("flush_test")),
    )
    .await?;

  assert!(failures.is_empty());
  assert_eq!(objects.len(), 1);
  assert_eq!(objects[0].topics.len(), 1);
  assert!(objects[0].blob_key.as_str().starts_with("shared/"));
  Ok(())
}

#[test]
fn shared_blob_keys_include_the_window_start() {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
  let window = Window::for_timestamp(now, time::Duration::minutes(5));
  let mut config = WriteConfig::with_defaults();
  config.compression = blob_stream_types::Compression::none();
  let context = FlushContext::new(
    config,
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    SnowflakeGenerator::with_machine_id(1).unwrap(),
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );

  assert_eq!(
    context.make_blob_key(&window, SnowflakeId(42)).as_str(),
    "shared/1699999800/42.bin"
  );
}

#[test]
fn merge_partition_batches_preserves_order_and_combines_metadata() -> Result<()> {
  let (virtual_partition_id, records, summary, seq_range) =
    FlushContext::merge_partition_batches(FlushPartition {
      virtual_partition_id: 4,
      lease_fence: Some(Arc::new(lease_fence())),
      batches: vec![
        BufferedBatch {
          records: vec![new_record(b"first".to_vec(), 10)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 5,
          },
          seq_range: SeqRange { start: 8, end: 8 },
          acceptance_fence: Some(Arc::new(lease_fence())),
          completion: None,
        },
        BufferedBatch {
          records: vec![new_record(b"second".to_vec(), 20)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 6,
          },
          seq_range: SeqRange { start: 9, end: 9 },
          acceptance_fence: Some(Arc::new(lease_fence())),
          completion: None,
        },
      ],
      trigger: FlushTrigger::MaxBytes,
      publication_predecessor: None,
      publication_state_tx: None,
    })?;

  assert_eq!(virtual_partition_id, 4);
  assert_eq!(seq_range, SeqRange { start: 8, end: 9 });
  assert_eq!(
    summary,
    BatchSummary {
      record_count: 2,
      payload_bytes: 11,
    }
  );
  assert_eq!(records[0].payload.as_ref(), b"first");
  assert_eq!(records[1].payload.as_ref(), b"second");
  Ok(())
}

#[test]
fn merge_partition_batches_rejects_noncontiguous_ranges() {
  let result = FlushContext::merge_partition_batches(FlushPartition {
    virtual_partition_id: 4,
    lease_fence: Some(Arc::new(lease_fence())),
    batches: vec![
      BufferedBatch {
        records: vec![new_record(b"first".to_vec(), 10)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 5,
        },
        seq_range: SeqRange { start: 8, end: 8 },
        acceptance_fence: Some(Arc::new(lease_fence())),
        completion: None,
      },
      BufferedBatch {
        records: vec![new_record(b"third".to_vec(), 20)],
        summary: BatchSummary {
          record_count: 1,
          payload_bytes: 5,
        },
        seq_range: SeqRange { start: 10, end: 10 },
        acceptance_fence: Some(Arc::new(lease_fence())),
        completion: None,
      },
    ],
    trigger: FlushTrigger::MaxBytes,
    publication_predecessor: None,
    publication_state_tx: None,
  });

  let Err(error) = result else {
    panic!("noncontiguous batches must fail to merge");
  };
  assert!(error.to_string().contains("noncontiguous ranges"));
}

#[tokio::test]
async fn lost_fence_does_not_fall_back_to_ordinary_metadata_write() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let time_provider = Arc::new(ManualTimeProvider::new(now));
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let publisher = Arc::new(LostFenceMetadataStore {
    calls: AtomicUsize::new(0),
  });
  let collector = Collector::default();
  let scope = collector.scope("flush_test");
  let config = WriteConfig::with_defaults();
  let context = FlushContext::new(
    config.clone(),
    Arc::new(InMemoryBlobStore::new()),
    publisher.clone(),
    SnowflakeGenerator::with_machine_id(1)?,
    time_provider,
    None,
  );
  let mut plan = FlushPlan {
    topics: vec![TopicFlushPlan {
      topic: "telemetry".into(),
      partitions: vec![FlushPartition {
        virtual_partition_id: 4,
        lease_fence: Some(Arc::new(lease_fence())),
        batches: vec![BufferedBatch {
          records: vec![new_record(b"payload".to_vec(), 10)],
          summary: BatchSummary {
            record_count: 1,
            payload_bytes: 7,
          },
          seq_range: SeqRange { start: 0, end: 0 },
          acceptance_fence: Some(Arc::new(lease_fence())),
          completion: None,
        }],
        trigger: FlushTrigger::MaxBytes,
        publication_predecessor: None,
        publication_state_tx: None,
      }],
      max_metadata_publication_lag: time::Duration::seconds(1),
      metadata_window_size: time::Duration::minutes(5),
      fenced_metadata_writes: true,
    }],
    max_segment_bytes: 64 * 1024 * 1024,
    publication_completions: Vec::new(),
  };

  let results = flush_plan_for_test(&context, &mut plan, &WriteMetrics::new(&scope)).await?;
  assert_eq!(results.len(), 1);
  assert_eq!(
    results[0].error,
    Some(crate::write::buffer::FlushCompletionError::LeaseFenceLost)
  );
  assert_eq!(publisher.calls.load(Ordering::Relaxed), 1);

  let window = Window::for_timestamp(now, time::Duration::minutes(5)).key("telemetry");
  assert!(
    metadata_store
      .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Eventual)
      .await?
      .is_empty()
  );
  Ok(())
}

#[tokio::test]
async fn shared_object_metadata_writes_start_concurrently() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let time_provider = Arc::new(ManualTimeProvider::new(now));
  let (started_tx, mut started_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(BlockingMetadataStore {
      started_tx,
      release: release.clone(),
    }),
    SnowflakeGenerator::with_machine_id(1)?,
    time_provider,
    None,
  );
  let collector = Collector::default();
  let metrics = WriteMetrics::new(&collector.scope("flush_test"));
  let plan = FlushPlan {
    topics: vec![
      topic_flush_plan("first".into(), 0, b"first"),
      topic_flush_plan("second".into(), 0, b"second"),
    ],
    max_segment_bytes: 64 * 1024 * 1024,
    publication_completions: Vec::new(),
  };
  let flush_context = context.clone();
  let flush_metrics = metrics.clone();
  let flush = tokio::spawn(async move {
    let mut plan = plan;
    flush_plan_for_test(&flush_context, &mut plan, &flush_metrics).await
  });

  let first_topic = receive_metadata_write(&mut started_rx).await;
  let second_topic = receive_metadata_write(&mut started_rx).await;
  assert_ne!(first_topic, second_topic);
  release.add_permits(2);

  assert_eq!(flush.await??.len(), 2);
  Ok(())
}

#[tokio::test]
async fn shared_blob_upload_failure_prevents_every_metadata_publication() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(FailingBlobStore),
    metadata_store.clone(),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let mut plan = FlushPlan {
    topics: vec![
      topic_flush_plan("first".into(), 0, b"first"),
      topic_flush_plan("second".into(), 0, b"second"),
    ],
    max_segment_bytes: 64 * 1024 * 1024,
    publication_completions: Vec::new(),
  };
  let collector = Collector::default();
  let metrics = WriteMetrics::new(&collector.scope("flush_test"));

  let results = flush_plan_for_test(&context, &mut plan, &metrics).await?;

  assert_eq!(results.len(), 2);
  assert!(results.iter().all(|result| result.error.is_some()));
  for topic in ["first", "second"] {
    let window = Window::for_timestamp(now, time::Duration::minutes(5)).key(topic);
    assert!(
      metadata_store
        .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Eventual)
        .await?
        .is_empty()
    );
  }
  Ok(())
}

#[tokio::test]
async fn failed_predecessor_does_not_reject_independent_shared_partition() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(InMemoryBlobStore::new()),
    metadata_store.clone(),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let (_state_tx, state_rx) = watch::channel(FlushPublicationState::Completed(
    FlushPublicationResult::Failed(FlushCompletionError::Internal),
  ));
  let mut topic = topic_flush_plan("telemetry".into(), 0, b"first");
  topic.partitions[0].publication_predecessor = Some(FlushPublicationDependency { state_rx });
  topic.partitions.push(flush_partition(1, b"second"));
  let mut plan = FlushPlan {
    topics: vec![topic],
    max_segment_bytes: 64 * 1024 * 1024,
    publication_completions: Vec::new(),
  };

  let results = flush_plan_for_test(
    &context,
    &mut plan,
    &WriteMetrics::new(&Collector::default().scope("flush_test")),
  )
  .await?;

  assert_eq!(results.len(), 2);
  assert!(results.iter().any(|result| {
    result.virtual_partition_id == 0 && result.error == Some(FlushCompletionError::Internal)
  }));
  assert!(
    results
      .iter()
      .any(|result| result.virtual_partition_id == 1 && result.error.is_none())
  );
  let window = Window::for_timestamp(now, time::Duration::minutes(5)).key("telemetry");
  let segments = metadata_store
    .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Eventual)
    .await?;
  assert_eq!(segments.len(), 1);
  assert_eq!(segments[0].segment_index.len(), 1);
  assert!(segments[0].segment_index.contains_key(&1));
  Ok(())
}

#[tokio::test]
async fn uploads_all_plan_objects_before_starting_metadata_publication() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let (metadata_started_tx, mut metadata_started_rx) = mpsc::unbounded_channel();
  let metadata_release = Arc::new(Semaphore::new(0));
  let (blob_started_tx, mut blob_started_rx) = mpsc::unbounded_channel();
  let blob_release = Arc::new(Semaphore::new(0));
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(BlockingBlobStore {
      started_tx: blob_started_tx,
      release: Arc::clone(&blob_release),
    }),
    Arc::new(BlockingMetadataStore {
      started_tx: metadata_started_tx,
      release: Arc::clone(&metadata_release),
    }),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let mut plan = FlushPlan {
    topics: vec![
      topic_flush_plan("first".into(), 0, b"first"),
      topic_flush_plan("second".into(), 0, b"second"),
    ],
    max_segment_bytes: 1,
    publication_completions: Vec::new(),
  };
  let metrics = WriteMetrics::new(&Collector::default().scope("flush_test"));
  let flush = tokio::spawn(async move { flush_plan_for_test(&context, &mut plan, &metrics).await });

  // Both uploads must start while the first remains held. A sequential implementation cannot
  // produce the second signal until the first permit is released.
  receive_blob_upload(&mut blob_started_rx).await;
  receive_blob_upload(&mut blob_started_rx).await;
  assert!(matches!(
    metadata_started_rx.try_recv(),
    Err(mpsc::error::TryRecvError::Empty)
  ));

  blob_release.add_permits(1);
  tokio::task::yield_now().await;
  assert!(matches!(
    metadata_started_rx.try_recv(),
    Err(mpsc::error::TryRecvError::Empty)
  ));
  assert!(!flush.is_finished());

  blob_release.add_permits(1);
  receive_metadata_write(&mut metadata_started_rx).await;
  metadata_release.add_permits(2);
  let results = flush.await??;
  assert_eq!(results.len(), 2);
  assert!(results.iter().all(|result| result.error.is_none()));
  Ok(())
}

#[tokio::test]
async fn blob_upload_permits_bound_concurrent_plans() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let (blob_started_tx, mut blob_started_rx) = mpsc::unbounded_channel();
  let blob_release = Arc::new(Semaphore::new(0));
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(BlockingBlobStore {
      started_tx: blob_started_tx,
      release: Arc::clone(&blob_release),
    }),
    Arc::new(InMemoryMetadataStore::new()),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let upload_permits = Arc::new(Semaphore::new(2));
  let metrics = WriteMetrics::new(&Collector::default().scope("flush_test"));
  let mut first_plan = FlushPlan {
    topics: vec![
      topic_flush_plan("first-a".into(), 0, b"first-a"),
      topic_flush_plan("first-b".into(), 0, b"first-b"),
    ],
    max_segment_bytes: 1,
    publication_completions: Vec::new(),
  };
  let mut second_plan = FlushPlan {
    topics: vec![
      topic_flush_plan("second-a".into(), 0, b"second-a"),
      topic_flush_plan("second-b".into(), 0, b"second-b"),
    ],
    max_segment_bytes: 1,
    publication_completions: Vec::new(),
  };
  let first_context = context.clone();
  let first_metrics = metrics.clone();
  let first_permits = Arc::clone(&upload_permits);
  let first_flush = tokio::spawn(async move {
    flush_plan_with_upload_permits_for_test(
      &first_context,
      &mut first_plan,
      &first_metrics,
      first_permits,
    )
    .await
  });
  let second_context = context.clone();
  let second_metrics = metrics.clone();
  let second_permits = Arc::clone(&upload_permits);
  let second_flush = tokio::spawn(async move {
    flush_plan_with_upload_permits_for_test(
      &second_context,
      &mut second_plan,
      &second_metrics,
      second_permits,
    )
    .await
  });

  receive_blob_upload(&mut blob_started_rx).await;
  receive_blob_upload(&mut blob_started_rx).await;
  tokio::task::yield_now().await;
  assert!(matches!(
    blob_started_rx.try_recv(),
    Err(mpsc::error::TryRecvError::Empty)
  ));

  blob_release.add_permits(2);
  receive_blob_upload(&mut blob_started_rx).await;
  receive_blob_upload(&mut blob_started_rx).await;
  blob_release.add_permits(2);

  assert_eq!(first_flush.await??.len(), 2);
  assert_eq!(second_flush.await??.len(), 2);
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn upload_deadline_includes_pre_upload_hook_delay() -> Result<()> {
  let test_started_at = tokio::time::Instant::now();
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let (hook_started_tx, mut hook_started_rx) = mpsc::unbounded_channel();
  let hook_release = Arc::new(Semaphore::new(0));
  let (blob_started_tx, _blob_started_rx) = mpsc::unbounded_channel();
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(BlockingBlobStore {
      started_tx: blob_started_tx,
      release: Arc::new(Semaphore::new(0)),
    }),
    metadata_store.clone(),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    Some(Arc::new(BlockingBeforeFlushPersistHook {
      started_tx: hook_started_tx,
      release: Arc::clone(&hook_release),
    })),
  );
  let mut topic = topic_flush_plan("telemetry".into(), 0, b"payload");
  topic.max_metadata_publication_lag = time::Duration::milliseconds(1);
  let mut plan = FlushPlan {
    topics: vec![topic],
    max_segment_bytes: 1,
    publication_completions: Vec::new(),
  };
  let metrics = WriteMetrics::new(&Collector::default().scope("flush_test"));
  let flush = tokio::spawn(async move { flush_plan_for_test(&context, &mut plan, &metrics).await });

  receive_blob_upload(&mut hook_started_rx).await;
  tokio::time::advance(std::time::Duration::from_millis(2)).await;
  assert_eq!(
    tokio::time::Instant::now().duration_since(test_started_at),
    std::time::Duration::from_millis(2)
  );
  hook_release.add_permits(1);

  let results = flush.await??;
  assert_eq!(results.len(), 1);
  assert_eq!(results[0].error, Some(FlushCompletionError::Internal));
  let window = Window::for_timestamp(now, time::Duration::minutes(5)).key("telemetry");
  assert!(
    metadata_store
      .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Eventual)
      .await?
      .is_empty()
  );
  Ok(())
}

#[tokio::test]
async fn capped_flush_keeps_published_object_results_when_later_object_expires() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(InMemoryBlobStore::new()),
    metadata_store.clone(),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let mut expired_topic = topic_flush_plan("second".into(), 0, b"second");
  expired_topic.max_metadata_publication_lag = time::Duration::ZERO;
  let mut plan = FlushPlan {
    topics: vec![topic_flush_plan("first".into(), 0, b"first"), expired_topic],
    max_segment_bytes: 1,
    publication_completions: Vec::new(),
  };
  let collector = Collector::default();
  let metrics = WriteMetrics::new(&collector.scope("flush_test"));

  let results = flush_plan_for_test(&context, &mut plan, &metrics).await?;

  assert_eq!(results.len(), 2);
  assert!(
    results
      .iter()
      .any(|result| { result.topic.as_str() == "first" && result.error.is_none() })
  );
  assert!(
    results
      .iter()
      .any(|result| { result.topic.as_str() == "second" && result.error.is_some() })
  );
  let first_window = Window::for_timestamp(now, time::Duration::minutes(5)).key("first");
  assert_eq!(
    metadata_store
      .scan_window_from_snowflake(&first_window, None, MetadataReadConsistency::Eventual)
      .await?
      .len(),
    1
  );
  Ok(())
}

#[tokio::test]
async fn shared_object_uses_each_topics_metadata_window() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_300)?;
  let metadata_store = Arc::new(InMemoryMetadataStore::new());
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(InMemoryBlobStore::new()),
    metadata_store.clone(),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let mut second_topic = topic_flush_plan("second".into(), 0, b"second");
  second_topic.metadata_window_size = time::Duration::minutes(10);
  let mut plan = FlushPlan {
    topics: vec![topic_flush_plan("first".into(), 0, b"first"), second_topic],
    max_segment_bytes: 64 * 1024 * 1024,
    publication_completions: Vec::new(),
  };
  let collector = Collector::default();
  let metrics = WriteMetrics::new(&collector.scope("flush_test"));

  flush_plan_for_test(&context, &mut plan, &metrics).await?;

  let first_window = Window::for_timestamp(now, time::Duration::minutes(5)).key("first");
  let second_window = Window::for_timestamp(now, time::Duration::minutes(10)).key("second");
  assert_ne!(
    first_window.window_start_unix_seconds,
    second_window.window_start_unix_seconds
  );
  let first_segments = metadata_store
    .scan_window_from_snowflake(&first_window, None, MetadataReadConsistency::Eventual)
    .await?;
  let second_segments = metadata_store
    .scan_window_from_snowflake(&second_window, None, MetadataReadConsistency::Eventual)
    .await?;
  assert_eq!(first_segments.len(), 1);
  assert_eq!(second_segments.len(), 1);
  assert_eq!(first_segments[0].blob_key, second_segments[0].blob_key);
  Ok(())
}

#[tokio::test]
async fn oversized_single_partition_object_is_counted() -> Result<()> {
  let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
  let collector = Collector::default();
  let metrics = Helper::new_with_collector(collector.clone());
  let context = FlushContext::new(
    WriteConfig::with_defaults(),
    Arc::new(InMemoryBlobStore::new()),
    Arc::new(InMemoryMetadataStore::new()),
    SnowflakeGenerator::with_machine_id(1)?,
    Arc::new(ManualTimeProvider::new(now)),
    None,
  );
  let mut plan = FlushPlan {
    topics: vec![topic_flush_plan(
      "telemetry".into(),
      0,
      b"oversized-payload",
    )],
    max_segment_bytes: 1,
    publication_completions: Vec::new(),
  };

  flush_plan_for_test(
    &context,
    &mut plan,
    &WriteMetrics::new(&collector.scope("flush_test")),
  )
  .await?;
  metrics.assert_counter_eq(
    1,
    "flush_test:write:flush_oversized_single_partition_objects_total",
    &labels!(),
  );
  Ok(())
}
