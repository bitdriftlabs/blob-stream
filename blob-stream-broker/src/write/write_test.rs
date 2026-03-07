#![allow(clippy::unwrap_used)]

use super::{TopicInfo, WriteConfig, WriteEngine, WriteEngineImpl, WriteRequest};
use anyhow::Result;
use async_trait::async_trait;
use bd_time::{OffsetDateTimeExt, TestTimeProvider, TimeProvider};
use blob_stream_blob_store::InMemoryBlobStore;
use blob_stream_metadata_store::{
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  MetadataStore,
  SegmentMetadata,
};
use blob_stream_types::{Compression, CompressionCodec, Record, SeqRange, Window};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::{Duration as TimeDuration, OffsetDateTime};

fn time_from_ms(ms: i64) -> OffsetDateTime {
  OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
    .unwrap_or(OffsetDateTime::UNIX_EPOCH)
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
  )?;

  Ok((Arc::new(engine), metadata_store))
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
    records: vec![Record::new(vec![1; 6], 10)],
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
    .scan_window(&window.key("telemetry"), None)
    .await?;
  assert!(segments.is_empty());

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![2; 6], 20)],
  };

  engine.produce_batch(request).await?;
  first.await??;

  let segments = metadata_store
    .scan_window(&window.key("telemetry"), None)
    .await?;
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
    records: vec![Record::new(vec![3; 4], 30)],
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
    .scan_window(&window.key("telemetry"), None)
    .await?;
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
    records: vec![Record::new(vec![9; 2], 10), Record::new(vec![9; 2], 11)],
  };

  let response = engine.produce_batch(request).await?;
  assert_eq!(response.seq_range, SeqRange { start: 0, end: 1 });

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![10; 3], 12)],
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
  config.compression = Compression::zstd(1);

  let (engine, metadata_store) = make_engine(time_provider.clone(), config.clone())?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![7; 6], 10)],
  };

  engine.produce_batch(request).await?;

  let window = Window::for_timestamp(
    time_provider.now().unix_timestamp_ms() / 1_000,
    config.window_size_seconds,
  );
  let segments = metadata_store
    .scan_window(&window.key("telemetry"), None)
    .await?;
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
    _min_snowflake_id: Option<blob_stream_types::SnowflakeId>,
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
  )?;

  let request = WriteRequest {
    topic: "telemetry".to_string(),
    virtual_partition_id: 0,
    records: vec![Record::new(vec![1, 2, 3], 10)],
  };

  let result = engine.produce_batch(request).await;
  assert!(result.is_err());
  Ok(())
}
