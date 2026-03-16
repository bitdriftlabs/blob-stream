#![allow(clippy::unwrap_used)]

use super::{
  ConsumerCoordinationSource,
  ConsumerIterator,
  ConsumerIteratorImpl,
  CoordinationSnapshot,
  IdlePollBackoff,
  NextResult,
};
use crate::config::{ConsumerGroupConfig, ConsumerReadConfig, ConsumerRuntimeConfig};
use bd_server_stats::stats::Collector;
use blob_stream_blob_store::{BlobKey, BlobStore, InMemoryBlobStore};
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
  InMemoryMetadataStore,
  MetadataStore,
  SegmentMetadata,
};
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  Compression,
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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep, timeout};

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
  let payload = StoredRecordBatch {
    virtual_partition_id,
    records: records.clone(),
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap();

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
  runtime_config_with_prefetch_max_bytes(None)
}

fn runtime_config_with_prefetch_max_bytes(
  prefetch_max_bytes: Option<u64>,
) -> ConsumerRuntimeConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = "telemetry".to_string().into();
  read.window_size_seconds = Some(300);
  read.lookback_windows = Some(2);
  read.prefetch_max_bytes = prefetch_max_bytes;

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

async fn wait_for_prefetch_buffer_len(iterator: &ConsumerIteratorImpl, expected_min: usize) {
  for _ in 0 .. 40 {
    let len = iterator.prefetch_buffer.lock().await.batches.len();
    if len >= expected_min {
      return;
    }
    sleep(Duration::from_millis(25)).await;
  }

  let len = iterator.prefetch_buffer.lock().await.batches.len();
  assert!(
    len >= expected_min,
    "prefetch buffer length {len} did not reach expected minimum {expected_min}"
  );
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_consumer_test")
}

#[test]
fn idle_poll_backoff_exponential_with_max_and_reset() {
  let mut backoff = IdlePollBackoff::new(250, Some(2_000));

  assert_eq!(backoff.next_delay_ms(), 250);
  assert_eq!(backoff.next_delay_ms(), 500);
  assert_eq!(backoff.next_delay_ms(), 1_000);
  assert_eq!(backoff.next_delay_ms(), 2_000);
  assert_eq!(backoff.next_delay_ms(), 2_000);

  backoff.reset();
  assert_eq!(backoff.next_delay_ms(), 250);
}

#[test]
fn idle_poll_backoff_without_max_remains_constant() {
  let mut backoff = IdlePollBackoff::new(250, None);

  assert_eq!(backoff.next_delay_ms(), 250);
  assert_eq!(backoff.next_delay_ms(), 250);
  assert_eq!(backoff.next_delay_ms(), 250);
}

#[tokio::test]
async fn next_returns_revocation_until_completed() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());

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
    membership_store,
    source.clone(),
    metrics_scope(),
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
    NextResult::Record(_) => panic!("expected revocation callback"),
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
async fn next_delivers_records_and_commit_renews() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());

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
    vec![
      new_record(vec![1, 2, 3], now_window * 1_000),
      new_record(vec![4, 5, 6], now_window * 1_000 + 1),
    ],
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
    membership_store,
    source,
    metrics_scope(),
  )
  .await
  .unwrap();

  iterator.start().unwrap();

  let next = iterator.next().await.unwrap();
  let record = match next {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected batch"),
  };
  assert_eq!(record.virtual_partition_id, 3);
  assert_eq!(record.offset, 1);
  assert_eq!(record.record.payload.to_vec(), vec![1, 2, 3]);

  let next = iterator.next().await.unwrap();
  let record = match next {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected record"),
  };
  assert_eq!(record.virtual_partition_id, 3);
  assert_eq!(record.offset, 2);
  assert_eq!(record.record.payload.to_vec(), vec![4, 5, 6]);

  iterator
    .store_offset(record.virtual_partition_id, record.offset)
    .unwrap();
  let report = iterator.commit().await.unwrap();
  assert_eq!(report.renewed_partitions, vec![3]);
  assert!(report.fenced_partitions.is_empty());
}

#[tokio::test]
async fn shutdown_releases_owned_partitions() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let concrete_lease_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> = concrete_lease_store.clone();
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());

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
    membership_store,
    source,
    metrics_scope(),
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  Box::new(iterator).shutdown().await.unwrap();

  let now_ts_ms = i64::try_from(
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis(),
  )
  .unwrap_or(i64::MAX);
  let key = ConsumerGroupLeaseKey {
    topic: "telemetry".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 3,
  };
  let reassigned = concrete_lease_store
    .assign_partition(key, "member-b".to_string(), 2, now_ts_ms, 1_000)
    .await
    .unwrap();
  assert!(matches!(
    reassigned,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));
}

#[tokio::test]
async fn prefetch_soft_budget_pauses_and_resumes_after_drain() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());

  let now_window = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_secs()
    .try_into()
    .unwrap_or(i64::MAX)
    / 300)
    * 300;

  for snowflake_id in 1 ..= 3 {
    let payload_byte = u8::try_from(snowflake_id).unwrap();
    write_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      "telemetry",
      now_window,
      snowflake_id,
      7,
      SeqRange {
        start: (snowflake_id - 1) * 2 + 1,
        end: snowflake_id * 2,
      },
      vec![
        new_record(vec![payload_byte; 6], now_window * 1_000),
        new_record(vec![payload_byte; 6], now_window * 1_000 + 1),
      ],
    )
    .await;
  }

  // One batch carries 12 payload bytes, so this budget should hold only one batch at a time.
  let runtime = runtime_config_with_prefetch_max_bytes(Some(16));
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![7],
    }));

  let mut iterator = ConsumerIteratorImpl::from_runtime_config(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
  )
  .await
  .unwrap();
  iterator.start().unwrap();

  wait_for_prefetch_buffer_len(&iterator, 1).await;
  let buffered_before = iterator.prefetch_buffer.lock().await.batches.len();
  assert_eq!(buffered_before, 1);

  let first = timeout(Duration::from_secs(2), iterator.next())
    .await
    .unwrap()
    .unwrap();
  let first_record = match first {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected batch"),
  };
  assert_eq!(first_record.virtual_partition_id, 7);
  assert_eq!(first_record.offset, 1);

  // Drain should allow worker to admit another pending batch.
  wait_for_prefetch_buffer_len(&iterator, 1).await;
  let buffered_after = iterator.prefetch_buffer.lock().await.batches.len();
  assert_eq!(buffered_after, 1);

  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn revocation_drops_buffered_batches_for_revoked_partitions() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());

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
    0,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], now_window * 1_000)],
  )
  .await;
  write_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    "telemetry",
    now_window,
    2,
    1,
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![2], now_window * 1_000)],
  )
  .await;

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
    membership_store,
    source.clone(),
    metrics_scope(),
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_prefetch_buffer_len(&iterator, 2).await;

  source
    .update(CoordinationSnapshot {
      members: vec!["member-a".to_string(), "member-b".to_string()],
      virtual_partitions: vec![0, 1],
    })
    .await;

  let revoked = iterator.next().await.unwrap();
  let revoked = match revoked {
    NextResult::Revoked(revoked) => revoked,
    NextResult::Record(_) => panic!("expected revocation callback"),
  };
  let revoked_partitions = revoked.partitions();
  assert_eq!(revoked_partitions.len(), 1);
  let revoked_partition = revoked_partitions[0];

  {
    let buffer = iterator.prefetch_buffer.lock().await;
    assert!(
      buffer
        .batches
        .iter()
        .all(|batch| batch.virtual_partition_id != revoked_partition),
      "revoked partition batch was not removed from prefetch buffer"
    );
  }

  revoked.complete().await;
  Box::new(iterator).shutdown().await.unwrap();
}
