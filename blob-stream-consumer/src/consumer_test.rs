#![allow(clippy::unwrap_used)]

use super::{ConsumerReadConfig, ConsumerReader, ConsumerReaderImpl};
use blob_stream_blob_store::{BlobKey, BlobStore, InMemoryBlobStore};
use blob_stream_metadata_store::{InMemoryMetadataStore, MetadataStore, SegmentMetadata};
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
};
use bytes::Bytes;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

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
  let encoded = serde_json::to_vec(&batch).unwrap();
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
    vec![Record::new(vec![1], 1000), Record::new(vec![2], 1001)],
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
    vec![Record::new(vec![10], 700), Record::new(vec![11], 701)],
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
    vec![Record::new(vec![12], 702), Record::new(vec![13], 703)],
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
    vec![Record::new(vec![42, 43], 1_000)],
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
  )
  .unwrap();

  let batches = reader.read_available(950).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].records.len(), 1);
  assert_eq!(batches[0].records[0].payload, vec![42, 43]);
  assert_eq!(reader.cursor(3), Some(10));
}
