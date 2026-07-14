#![allow(clippy::unwrap_used)]

use super::{TopicInfo, WriteConfig, WriteEngine, WriteEngineImpl, WriteRequest};
use anyhow::Result;
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use bd_time::{OffsetDateTimeExt, TestTimeProvider, TimeProvider};
use blob_stream_blob_store::InMemoryBlobStore;
use blob_stream_metadata_store::{
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  MetadataStore,
  SegmentMetadata,
};
use blob_stream_types::{CompressionCodec, SeqRange, Window, new_record};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::{Duration as TimeDuration, OffsetDateTime};

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
  let mut topics = HashMap::new();
  topics.insert(
    "telemetry".to_string(),
    TopicInfo {
      name: "telemetry".to_string(),
      partition_count: 1,
      num_writers: 1,
      retention_days: 7,
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
    lease_store,
    "test-node".to_string(),
    None,
    time_provider,
    &metrics_scope(),
  )?;

  Ok((Arc::new(engine), metadata_store))
}

#[tokio::test]
async fn state_snapshot_reports_local_buffer_and_lease_state() -> Result<()> {
  let now_ms = 1_700_000_000_000;
  let time_provider = Arc::new(TestTimeProvider::new(time_from_ms(now_ms)));
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1024;
  config.flush_max_delay_ms = 60_000;
  config.writer_id = 42;

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
  assert_eq!(snapshot.schema_version, 1);
  assert_eq!(snapshot.generated_at_ts_ms, now_ms);
  assert_eq!(snapshot.holder_id, "test-node");
  assert_eq!(snapshot.writer_id, 42);
  assert_eq!(snapshot.topics.len(), 1);

  let topic = &snapshot.topics[0];
  assert_eq!(topic.name, "telemetry");
  assert_eq!(topic.partition_count, 1);
  assert_eq!(topic.num_writers, 1);
  assert_eq!(topic.local_partitions.len(), 1);

  let partition = &topic.local_partitions[0];
  assert_eq!(partition.virtual_partition_id, 0);
  assert!(partition.lease_expiration_ts_ms.is_some());
  assert_eq!(partition.buffered_batch_count, 1);
  assert_eq!(partition.buffered_record_count, 1);
  assert_eq!(partition.buffered_bytes, 3);
  assert_eq!(partition.first_buffered_ts_ms, Some(now_ms));
  assert_eq!(partition.next_sequence, 1);
  assert_eq!(
    partition
      .sequence_reservation
      .as_ref()
      .map(|reservation| (reservation.start, reservation.end)),
    Some((0, 999))
  );

  pending_write.abort();
  let _ignored = pending_write.await;
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
  let segments = metadata_store.scan_window(&window.key("telemetry")).await?;
  assert!(segments.is_empty());

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![2; 6], 20)],
  };

  engine.produce_batch(request).await?;
  first.await??;

  let segments = metadata_store.scan_window(&window.key("telemetry")).await?;
  assert_eq!(segments.len(), 1);
  assert_eq!(segments[0].record_count, 2);
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
  let segments = metadata_store.scan_window(&window.key("telemetry")).await?;
  assert_eq!(segments.len(), 1);
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
  let segments = metadata_store.scan_window(&window.key("telemetry")).await?;
  assert_eq!(segments.len(), 1);
  assert_eq!(segments[0].compression.codec, CompressionCodec::Zstd);

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

  async fn scan_window(
    &self,
    _window: &blob_stream_types::TopicWindowKey,
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
