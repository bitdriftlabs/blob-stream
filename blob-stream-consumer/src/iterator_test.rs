#![allow(clippy::unwrap_used)]

use super::{
  ConsumerCoordinationSource,
  ConsumerIterator,
  ConsumerIteratorImpl,
  CoordinationSnapshot,
  NextResult,
};
use crate::config::{ConsumerGroupConfig, ConsumerReadConfig, ConsumerRuntimeConfig};
use blob_stream_blob_store::{BlobKey, BlobStore, InMemoryBlobStore};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseStore,
  InMemoryConsumerGroupLeaseStore,
  InMemoryMetadataStore,
  MetadataStore,
  SegmentMetadata,
};
use blob_stream_types::{
  BatchMetadata,
  Compression,
  Record,
  RecordBatch,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

struct MutableCoordinationSource {
  snapshot: Arc<Mutex<CoordinationSnapshot>>,
}

impl MutableCoordinationSource {
  fn new(snapshot: CoordinationSnapshot) -> Self {
    Self {
      snapshot: Arc::new(Mutex::new(snapshot)),
    }
  }

  async fn update(&self, snapshot: CoordinationSnapshot) {
    *self.snapshot.lock().await = snapshot;
  }
}

#[async_trait::async_trait]
impl ConsumerCoordinationSource for MutableCoordinationSource {
  async fn snapshot(&self) -> anyhow::Result<CoordinationSnapshot> {
    Ok(self.snapshot.lock().await.clone())
  }
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
  let batch = RecordBatch::new(virtual_partition_id, records.clone());
  let payload = serde_json::to_vec(&batch).unwrap();

  let blob_key = BlobKey::new(format!("{topic}/{window_start}/{snowflake_id}.bin"));

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
        compression: Compression::none(),
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

fn runtime_config() -> ConsumerRuntimeConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".to_string().into();
  read.window_size_seconds = Some(300);
  read.lookback_windows = Some(2);

  let mut group = ConsumerGroupConfig::new();
  group.topic = "telemetry".to_string().into();
  group.group_id = "group-a".to_string().into();
  group.member_id = "member-a".to_string().into();
  group.lease_duration_ms = Some(1_000);
  group.heartbeat_interval_ms = Some(10);
  group.rebalance_interval_ms = Some(10);

  let mut runtime = ConsumerRuntimeConfig::new();
  runtime.read = Some(read).into();
  runtime.group = Some(group).into();
  runtime
}

#[tokio::test]
async fn next_returns_revocation_until_completed() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());

  let runtime = runtime_config();
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0, 1],
  }));

  let mut iterator = ConsumerIteratorImpl::from_runtime_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    source.clone(),
  )
  .await
  .unwrap();

  iterator.start().unwrap();

  source
    .update(CoordinationSnapshot {
      members: vec!["member-a".to_string(), "member-b".to_string()],
      virtual_partitions: vec![0, 1],
    })
    .await;

  let revoked = iterator.next().await.unwrap();
  let revoked = match revoked {
    NextResult::Revoked(revoked) => revoked,
    NextResult::Batch(_) => panic!("expected revocation callback"),
  };

  let revoked_partitions = revoked.partitions();
  assert_eq!(revoked_partitions.len(), 1);

  let result = iterator.next().await;
  assert!(result.is_err());
  let error = result.err().unwrap();
  assert!(
    error
      .to_string()
      .contains("revocation callback must be completed")
  );

  revoked.complete().await;
}

#[tokio::test]
async fn next_delivers_batch_and_commit_renews() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());

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
    vec![Record::new(vec![1, 2, 3], now_window * 1_000)],
  )
  .await;

  let runtime = runtime_config();
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut iterator = ConsumerIteratorImpl::from_runtime_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    source,
  )
  .await
  .unwrap();

  iterator.start().unwrap();

  let next = iterator.next().await.unwrap();
  let batch = match next {
    NextResult::Batch(batch) => batch,
    NextResult::Revoked(_) => panic!("expected batch"),
  };
  assert_eq!(batch.virtual_partition_id, 3);
  assert_eq!(batch.records.len(), 1);

  iterator
    .store_offset(batch.virtual_partition_id, batch.seq_range.end)
    .unwrap();
  let report = iterator.commit().await.unwrap();
  assert_eq!(report.renewed_partitions, vec![3]);
  assert!(report.fenced_partitions.is_empty());
}
