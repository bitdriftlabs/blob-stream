#![allow(clippy::unwrap_used)]

use super::{ConsumerReadConfig, ConsumerReader, ConsumerReaderImpl};
use anyhow::Result;
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use blob_stream_blob_store::{BlobKey, BlobStore, InMemoryBlobStore};
use blob_stream_metadata_store::{InMemoryMetadataStore, MetadataStore, SegmentMetadata};
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  Compression,
  CompressionCodec,
  Record,
  RecordBatch,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
  new_record,
};
use bytes::Bytes;
use protobuf::Message;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::Mutex;

struct RecordingMetadataStore {
  inner: InMemoryMetadataStore,
  scans: Mutex<Vec<(i64, Option<SnowflakeId>)>>,
}

impl RecordingMetadataStore {
  fn new() -> Self {
    Self {
      inner: InMemoryMetadataStore::new(),
      scans: Mutex::new(Vec::new()),
    }
  }
}

#[async_trait]
impl MetadataStore for RecordingMetadataStore {
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()> {
    self.inner.write_segment(metadata).await
  }

  async fn scan_window_from_snowflake(
    &self,
    window: &TopicWindowKey,
    min_snowflake: Option<SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>> {
    self
      .scans
      .lock()
      .await
      .push((window.window_start_unix_seconds, min_snowflake));
    self
      .inner
      .scan_window_from_snowflake(window, min_snowflake)
      .await
  }
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_consumer_test")
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
    HashMap::from([(
      virtual_partition_id,
      vec![BatchMetadata {
        seq_range,
        byte_range: blob_stream_types::ByteRange {
          start: 0,
          end: payload.len() as u64,
        },
        summary,
        compression,
      }],
    )]),
    Compression::none(),
    records.len() as u64,
    records.first().map_or(0, |record| record.event_ts_ms),
    records.last().map_or(0, |record| record.event_ts_ms),
    None,
    window_start * 1_000,
  );

  metadata_store.write_segment(metadata).await.unwrap();
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
      window_size_seconds: Some(300),
      lookback_windows: Some(2),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
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
async fn catches_late_metadata_with_lookback_window() {
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
      window_size_seconds: Some(300),
      lookback_windows: Some(3),
      ..Default::default()
    },
    vec![11],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    &metrics_scope(),
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
      window_size_seconds: Some(300),
      lookback_windows: Some(2),
      ..Default::default()
    },
    vec![3],
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
  )
  .unwrap();

  let batches = reader.read_available(950).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].records.len(), 1);
  assert_eq!(batches[0].records[0].payload, vec![42, 43]);
  assert_eq!(reader.cursor(3), Some(10));
}

#[tokio::test]
async fn recovery_scan_catches_late_lower_snowflake_metadata() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

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
      window_size_seconds: Some(300),
      lookback_windows: Some(2),
      metadata_recovery_scan_interval_seconds: Some(60),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    &metrics_scope(),
  )
  .unwrap();

  let initial = reader.read_available(901).await.unwrap();
  assert_eq!(initial.len(), 1);
  assert_eq!(reader.metrics.metadata_recovery_scan_hits.get(), 1);
  assert_eq!(reader.metrics.metadata_recovery_scan_batches_read.get(), 1);

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 3, end: 4 },
    vec![new_record(vec![3], 903), new_record(vec![4], 904)],
    Compression::none(),
  )
  .await;

  let fast_scan = reader.read_available(902).await.unwrap();
  assert!(fast_scan.is_empty());
  assert_eq!(reader.metrics.metadata_recovery_scan_hits.get(), 1);

  let recovery_scan = reader.read_available(961).await.unwrap();
  assert_eq!(recovery_scan.len(), 1);
  assert_eq!(recovery_scan[0].seq_range, SeqRange { start: 3, end: 4 });
  assert_eq!(reader.metrics.metadata_recovery_scan_hits.get(), 2);
  assert_eq!(reader.metrics.metadata_recovery_scan_batches_read.get(), 2);
}

#[tokio::test]
async fn fast_scan_uses_current_window_inclusive_watermark() {
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
      window_size_seconds: Some(300),
      lookback_windows: Some(2),
      metadata_recovery_scan_interval_seconds: Some(60),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
  )
  .unwrap();

  reader.read_available(901).await.unwrap();
  recording_metadata_store.scans.lock().await.clear();

  let batches = reader.read_available(902).await.unwrap();
  assert!(batches.is_empty());
  assert_eq!(
    *recording_metadata_store.scans.lock().await,
    vec![(900, Some(SnowflakeId(2)))]
  );
}
