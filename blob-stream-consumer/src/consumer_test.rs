#![allow(clippy::unwrap_used)]

use super::state::{RecoveryState, VirtualPartitionState};
use super::{ConsumerReadConfig, ConsumerReader, ConsumerReaderImpl};
use crate::config::DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS;
use anyhow::Result;
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use blob_stream_blob_store::{BlobKey, BlobStore, InMemoryBlobStore};
use blob_stream_metadata_store::{InMemoryMetadataStore, MetadataStore, SegmentMetadata};
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  CommittedCursor,
  CommittedSourceCheckpoint,
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
use parking_lot::Mutex;
use protobuf::Message;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

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
  write_segment_with_publication_time(
    blob_store,
    metadata_store,
    topic,
    window_start,
    snowflake_id,
    virtual_partition_id,
    seq_range,
    records,
    compression,
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
  compression: Compression,
  metadata_published_ts_ms: i64,
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
    window_start * 1_000,
    metadata_published_ts_ms,
  );

  metadata_store.write_segment(metadata).await.unwrap();
}

#[tokio::test]
async fn visibility_delay_defers_newly_published_metadata() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 902_000)],
    Compression::none(),
    902_000,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      metadata_visibility_delay_ms: Some(1_000),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();

  assert!(reader.read_available(902).await.unwrap().is_empty());
  let batches = reader.read_available(903).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 1, end: 1 });
}

#[tokio::test]
async fn derived_horizon_retries_visibility_deferred_metadata_across_window_boundary() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());

  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 901_000)],
    Compression::none(),
    1_200_000,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      metadata_visibility_delay_ms: Some(300_000),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    &metrics_scope(),
    1,
    30_000,
  )
  .unwrap();

  assert!(reader.read_available(1_200).await.unwrap().is_empty());
  let batches = reader.read_available(1_500).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 1, end: 1 });
}

#[tokio::test]
async fn retention_recovery_scans_from_checkpoint_before_fast_path() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();

  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    15_000,
    2,
    7,
    SeqRange { start: 5, end: 5 },
    vec![new_record(vec![5], 15_001_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store_dyn.as_ref(),
    "telemetry",
    90_000,
    3,
    7,
    SeqRange { start: 6, end: 6 },
    vec![new_record(vec![6], 90_001_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    metadata_store_dyn,
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 4,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 15_000,
        snowflake_id: 1,
      }),
    },
    Some(15_000_000),
    90_000,
  );
  reader
    .set_assigned_virtual_partitions(&[7], 90_000)
    .unwrap();

  let batches = reader.read_available(90_000).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 5, end: 5 });
  assert!(
    !metadata_store
      .scans
      .lock()
      .iter()
      .any(|(window_start, _)| *window_start == 90_000)
  );

  let VirtualPartitionState::Recovering { recovery_state, .. } =
    reader.virtual_partition_states.get(&7).unwrap()
  else {
    panic!("partition must remain in recovery before the cutover is scanned");
  };
  assert_eq!(recovery_state.cutover_window_start_unix_seconds, 90_000);
  assert_eq!(recovery_state.next_window_start_unix_seconds, 24_600);
}

#[tokio::test]
async fn assignment_activates_hydrated_state_and_removes_revoked_state() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();

  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 4,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 15_000,
        snowflake_id: 1,
      }),
    },
    Some(15_000_000),
    90_000,
  );

  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::PendingRecovering { .. })
  ));
  assert_eq!(reader.cursor(7), Some(4));

  reader
    .set_assigned_virtual_partitions(&[7], 90_000)
    .unwrap();
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Recovering { .. })
  ));

  reader.set_assigned_virtual_partitions(&[], 90_000).unwrap();
  assert!(!reader.virtual_partition_states.contains_key(&7));
  assert_eq!(reader.cursor(7), None);
}

#[tokio::test]
async fn retention_recovery_clamps_legacy_cursor_to_retention_floor() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store_dyn: Arc<dyn MetadataStore> = metadata_store.clone();
  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    blob_store,
    metadata_store_dyn,
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 4,
      source_checkpoint: None,
    },
    Some(1_000),
    90_000,
  );
  reader
    .set_assigned_virtual_partitions(&[7], 90_000)
    .unwrap();

  assert!(reader.read_available(90_000).await.unwrap().is_empty());
  let scans = metadata_store.scans.lock();
  assert_eq!(scans.first(), Some(&(3_600, None)));
  assert_eq!(scans.len(), 32);
}

#[tokio::test]
async fn retention_recovery_crosses_multiple_scan_slices_before_fast_path() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let window_size_seconds = 300;
  let cutover_window_start = 24_000;

  for (window_offset, snowflake_id, sequence) in [(31, 2, 1), (32, 3, 2), (64, 4, 3), (80, 5, 4)] {
    let window_start = i64::from(window_offset) * window_size_seconds;
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      window_start,
      snowflake_id,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        window_start * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(window_size_seconds),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 0,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 1,
      }),
    },
    Some(0),
    cutover_window_start,
  );
  reader
    .set_assigned_virtual_partitions(&[7], cutover_window_start)
    .unwrap();

  let first_slice = reader.read_available(cutover_window_start).await.unwrap();
  assert_eq!(first_slice.len(), 1);
  assert_eq!(first_slice[0].seq_range.end, 1);
  let second_slice = reader.read_available(cutover_window_start).await.unwrap();
  assert_eq!(second_slice.len(), 1);
  assert_eq!(second_slice[0].seq_range.end, 2);
  let final_slice = reader.read_available(cutover_window_start).await.unwrap();
  let recovered_sequences = first_slice
    .into_iter()
    .chain(second_slice)
    .chain(final_slice)
    .map(|batch| batch.seq_range.end)
    .collect::<Vec<_>>();
  assert_eq!(recovered_sequences, vec![1, 2, 3, 4]);
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
}

#[tokio::test]
async fn recovery_waits_for_visibility_deferred_window_before_advancing() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let window_size_seconds = 300;
  let cutover_window_start = 12_000;
  let deferred_window_start = 9_600;

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    9_300,
    2,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], 9_300_000)],
    Compression::none(),
  )
  .await;
  write_segment_with_publication_time(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    deferred_window_start,
    3,
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![2], 9_600_000)],
    Compression::none(),
    cutover_window_start * 1_000,
  )
  .await;

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(window_size_seconds),
      metadata_visibility_delay_ms: Some(1_000),
      ..Default::default()
    },
    Vec::new(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();
  reader.hydrate_cursor_with_source(
    7,
    &CommittedCursor {
      virtual_partition_id: 7,
      seq_end: 0,
      source_checkpoint: Some(CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 1,
      }),
    },
    Some(0),
    cutover_window_start,
  );
  reader
    .set_assigned_virtual_partitions(&[7], cutover_window_start)
    .unwrap();

  assert_eq!(
    reader
      .read_available(cutover_window_start)
      .await
      .unwrap()
      .len(),
    1
  );
  assert!(
    reader
      .read_available(cutover_window_start)
      .await
      .unwrap()
      .is_empty()
  );
  let VirtualPartitionState::Recovering { recovery_state, .. } =
    reader.virtual_partition_states.get(&7).unwrap()
  else {
    panic!("visibility-deferred recovery window must remain pending");
  };
  assert_eq!(
    recovery_state.next_window_start_unix_seconds,
    deferred_window_start
  );

  let batches = reader
    .read_available(cutover_window_start + 1)
    .await
    .unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 2, end: 2 });
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

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
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
async fn catches_late_metadata_with_derived_candidate_horizon() {
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

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      ..Default::default()
    },
    vec![11],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    &metrics_scope(),
    1,
    600_000,
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

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      ..Default::default()
    },
    vec![3],
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();

  let batches = reader.read_available(950).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].records.len(), 1);
  assert_eq!(batches[0].records[0].payload, vec![42, 43]);
  assert_eq!(reader.cursor(3), Some(10));
}

#[tokio::test]
async fn fast_scan_uses_per_partition_inclusive_frontier() {
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

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();

  reader.read_available(901).await.unwrap();
  recording_metadata_store.scans.lock().clear();

  let batches = reader.read_available(902).await.unwrap();
  assert!(batches.is_empty());
  assert_eq!(
    *recording_metadata_store.scans.lock(),
    vec![(600, None), (900, Some(SnowflakeId(2)))]
  );
}

#[tokio::test]
async fn fast_scan_uses_lowest_partition_frontier_for_cross_partition_ordering() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    100,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![7], 901_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    1,
    8,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![8], 901_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();

  assert_eq!(reader.read_available(901).await.unwrap().len(), 2);
  recording_metadata_store.scans.lock().clear();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    2,
    8,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![9], 902_000)],
    Compression::none(),
  )
  .await;

  let batches = reader.read_available(902).await.unwrap();
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].virtual_partition_id, 8);
  assert!(
    recording_metadata_store
      .scans
      .lock()
      .contains(&(900, Some(SnowflakeId(1))))
  );
}

#[tokio::test]
async fn seek_resets_partition_fast_frontier() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  for snowflake_id in 1 ..= 2 {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      900,
      snowflake_id,
      7,
      SeqRange {
        start: snowflake_id,
        end: snowflake_id,
      },
      vec![new_record(
        vec![u8::try_from(snowflake_id).unwrap()],
        901_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();

  assert_eq!(reader.read_available(901).await.unwrap().len(), 2);
  recording_metadata_store.scans.lock().clear();

  reader.set_cursor(7, 0);
  let rewound = reader.read_available(902).await.unwrap();

  assert_eq!(rewound.len(), 2);
  assert_eq!(rewound[0].seq_range, SeqRange { start: 1, end: 1 });
  assert!(recording_metadata_store.scans.lock().contains(&(900, None)));
}

#[tokio::test]
async fn historical_seek_recovers_recent_windows_then_returns_to_fast_path() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  for (window_start, snowflake_id, sequence) in [(600, 1, 1), (1_200, 2, 2)] {
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      window_start,
      snowflake_id,
      7,
      SeqRange {
        start: sequence,
        end: sequence,
      },
      vec![new_record(
        vec![u8::try_from(sequence).unwrap()],
        window_start * 1_000,
      )],
      Compression::none(),
    )
    .await;
  }

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    vec![7],
    HashMap::new(),
    blob_store,
    metadata_store,
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();

  let current = reader.read_available(1_200).await.unwrap();
  assert_eq!(current.len(), 1);
  assert_eq!(current[0].seq_range, SeqRange { start: 2, end: 2 });
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
  recording_metadata_store.scans.lock().clear();

  reader.seek(7, 0, 1_200);
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Recovering {
      recovery_state: RecoveryState {
        next_window_start_unix_seconds: 600,
        cutover_window_start_unix_seconds: 1_200,
      },
      ..
    })
  ));

  let recovered = reader.read_available(1_200).await.unwrap();
  assert_eq!(
    recovered
      .iter()
      .map(|batch| batch.seq_range.end)
      .collect::<Vec<_>>(),
    vec![1, 2]
  );
  assert!(matches!(
    reader.virtual_partition_states.get(&7),
    Some(VirtualPartitionState::Fast { .. })
  ));
  let scans = recording_metadata_store.scans.lock();
  assert!(scans.contains(&(600, None)));
  assert!(scans.contains(&(900, None)));
  assert!(scans.contains(&(1_200, None)));
}

#[tokio::test]
async fn fast_scan_uses_per_window_frontiers_across_candidate_window_boundary() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let recording_metadata_store = Arc::new(RecordingMetadataStore::new());
  let metadata_store: Arc<dyn MetadataStore> = recording_metadata_store.clone();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    100,
    7,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![7], 900_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    10,
    8,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![8], 900_000)],
    Compression::none(),
  )
  .await;

  let mut reader = ConsumerReaderImpl::new_with_retention_and_publication_lag(
    ConsumerReadConfig {
      topic: "telemetry".to_string().into(),
      window_size_seconds: Some(300),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    vec![7, 8],
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
    &metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  )
  .unwrap();
  assert_eq!(reader.read_available(900).await.unwrap().len(), 2);
  recording_metadata_store.scans.lock().clear();

  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    600,
    1,
    7,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![9], 600_000)],
    Compression::none(),
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    900,
    11,
    8,
    SeqRange { start: 2, end: 2 },
    vec![new_record(vec![10], 900_000)],
    Compression::none(),
  )
  .await;

  let batches = reader.read_available(901).await.unwrap();
  let mut sequences = batches
    .into_iter()
    .map(|batch| batch.seq_range.end)
    .collect::<Vec<_>>();
  sequences.sort_unstable();
  assert_eq!(sequences, vec![2, 2]);
  assert_eq!(
    *recording_metadata_store.scans.lock(),
    vec![(600, None), (900, Some(SnowflakeId(10)))]
  );
}
