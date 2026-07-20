#![allow(clippy::unwrap_used)]

use super::delivery::{BufferedBatch, DeliveryState};
use super::prefetch::IdlePollBackoff;
use super::{
  ConsumerCoordinationSource,
  ConsumerDeliveryState,
  ConsumerIterator,
  ConsumerIteratorImpl,
  ConsumerIteratorMetrics,
  CoordinationSnapshot,
  NextResult,
};
use crate::config::{
  ConsumerGroupConfig,
  ConsumerReadConfig,
  ConsumerRuntimeConfig,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
};
use crate::diagnostics::{
  ConsumerAssignmentPlanSnapshot,
  ConsumerGroupLeaseObservation,
  ConsumerLocalPartitionSnapshot,
  ConsumerPartitionAssignmentSnapshot,
  ConsumerPartitionReadMode,
  ConsumerSourceCheckpointSnapshot,
  ConsumerStateSnapshot,
};
use bd_server_stats::stats::Collector;
use blob_stream_blob_store::{BlobKey, BlobStore, ByteRange, InMemoryBlobStore};
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLease,
  ConsumerGroupPlannerLeaseOutcome,
  ConsumerGroupReleaseOutcome,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
  InMemoryMetadataStore,
  MetadataStore,
  SegmentMetadata,
};
use blob_stream_proto::protos::blobstream::v1::broker::StoredRecordBatch;
use blob_stream_types::{
  BatchMetadata,
  CommittedCursor,
  CommittedSourceCheckpoint,
  Compression,
  Record,
  RecordBatch,
  SeqRange,
  SnowflakeId,
  TopicWindowKey,
  VirtualPartitionId,
  format_unix_timestamp_ms,
  new_record,
  now_unix_millis,
};
use bytes::Bytes;
use parking_lot::Mutex;
use protobuf::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, sleep, timeout};

struct MutableCoordinationSource {
  snapshot: Arc<Mutex<CoordinationSnapshot>>,
}

struct BlockingCoordinationSource {
  snapshot: Arc<Mutex<CoordinationSnapshot>>,
  block_snapshots: AtomicBool,
  snapshot_calls: AtomicUsize,
  snapshot_started: Arc<tokio::sync::Notify>,
  snapshot_release: Arc<tokio::sync::Notify>,
}

impl BlockingCoordinationSource {
  fn new(snapshot: CoordinationSnapshot) -> Self {
    Self {
      snapshot: Arc::new(Mutex::new(snapshot)),
      block_snapshots: AtomicBool::new(false),
      snapshot_calls: AtomicUsize::new(0),
      snapshot_started: Arc::new(tokio::sync::Notify::new()),
      snapshot_release: Arc::new(tokio::sync::Notify::new()),
    }
  }
}

#[async_trait::async_trait]
impl ConsumerCoordinationSource for BlockingCoordinationSource {
  async fn snapshot(&self) -> anyhow::Result<CoordinationSnapshot> {
    self.snapshot_calls.fetch_add(1, Ordering::SeqCst);
    if self.block_snapshots.load(Ordering::SeqCst) {
      self.snapshot_started.notify_waiters();
      self.snapshot_release.notified().await;
    }
    Ok(self.snapshot.lock().clone())
  }
}

struct BlockingMembershipStore {
  inner: InMemoryConsumerGroupMembershipStore,
  block_heartbeats: AtomicBool,
  fail_deregistration: AtomicBool,
  heartbeat_calls: AtomicUsize,
  heartbeat_started: Arc<tokio::sync::Notify>,
  heartbeat_release: Arc<tokio::sync::Notify>,
}

struct BlockingBlobStore {
  inner: InMemoryBlobStore,
  block_reads: AtomicBool,
  read_started: tokio::sync::Notify,
  read_release: tokio::sync::Notify,
}

struct FailingLeaseStore {
  inner: InMemoryConsumerGroupLeaseStore,
}

impl FailingLeaseStore {
  fn new() -> Self {
    Self {
      inner: InMemoryConsumerGroupLeaseStore::new(),
    }
  }
}

#[async_trait::async_trait]
impl ConsumerGroupLeaseStore for FailingLeaseStore {
  async fn list_group_leases(
    &self,
    topic: &str,
    group_id: &str,
  ) -> anyhow::Result<Vec<ConsumerGroupLease>> {
    Err(anyhow::anyhow!(
      "injected lease lookup failure for {topic}/{group_id}"
    ))
  }

  async fn assign_partition(
    &self,
    key: ConsumerGroupLeaseKey,
    owner_id: String,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> anyhow::Result<ConsumerGroupAssignmentOutcome> {
    self
      .inner
      .assign_partition(key, owner_id, generation, now_ts_ms, lease_duration_ms)
      .await
  }

  async fn heartbeat_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    lease_duration_ms: i64,
    committed_cursor: Option<CommittedCursor>,
  ) -> anyhow::Result<ConsumerGroupHeartbeatOutcome> {
    self
      .inner
      .heartbeat_partition(
        key,
        owner_id,
        generation,
        now_ts_ms,
        lease_duration_ms,
        committed_cursor,
      )
      .await
  }

  async fn commit_cursor(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
    committed_cursor: CommittedCursor,
  ) -> anyhow::Result<ConsumerGroupCommitOutcome> {
    self
      .inner
      .commit_cursor(key, owner_id, generation, now_ts_ms, committed_cursor)
      .await
  }

  async fn release_partition(
    &self,
    key: &ConsumerGroupLeaseKey,
    owner_id: &str,
    generation: u64,
    now_ts_ms: i64,
  ) -> anyhow::Result<ConsumerGroupReleaseOutcome> {
    self
      .inner
      .release_partition(key, owner_id, generation, now_ts_ms)
      .await
  }
}

impl BlockingBlobStore {
  fn new() -> Self {
    Self {
      inner: InMemoryBlobStore::new(),
      block_reads: AtomicBool::new(false),
      read_started: tokio::sync::Notify::new(),
      read_release: tokio::sync::Notify::new(),
    }
  }
}

#[async_trait::async_trait]
impl BlobStore for BlockingBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> anyhow::Result<()> {
    self.inner.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> anyhow::Result<Bytes> {
    if self.block_reads.load(Ordering::SeqCst) {
      self.read_started.notify_waiters();
      self.read_release.notified().await;
    }
    self.inner.get_range(key, range).await
  }
}

impl BlockingMembershipStore {
  fn new() -> Self {
    Self {
      inner: InMemoryConsumerGroupMembershipStore::new(),
      block_heartbeats: AtomicBool::new(false),
      fail_deregistration: AtomicBool::new(false),
      heartbeat_calls: AtomicUsize::new(0),
      heartbeat_started: Arc::new(tokio::sync::Notify::new()),
      heartbeat_release: Arc::new(tokio::sync::Notify::new()),
    }
  }
}

#[async_trait::async_trait]
impl ConsumerGroupMembershipStore for BlockingMembershipStore {
  async fn register_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> anyhow::Result<()> {
    if self.fail_deregistration.load(Ordering::SeqCst) {
      return Err(anyhow::anyhow!("injected deregistration failure"));
    }
    self
      .inner
      .register_member(topic, group_id, member_id, now_ts_ms, ttl_ms)
      .await
  }

  async fn heartbeat_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> anyhow::Result<()> {
    self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
    if self.block_heartbeats.load(Ordering::SeqCst) {
      self.heartbeat_started.notify_waiters();
      self.heartbeat_release.notified().await;
    }
    self
      .inner
      .heartbeat_member(topic, group_id, member_id, now_ts_ms, ttl_ms)
      .await
  }

  async fn deregister_member(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
  ) -> anyhow::Result<()> {
    self
      .inner
      .deregister_member(topic, group_id, member_id)
      .await
  }

  async fn list_active_members(
    &self,
    topic: &str,
    group_id: &str,
    now_ts_ms: i64,
  ) -> anyhow::Result<Vec<String>> {
    self
      .inner
      .list_active_members(topic, group_id, now_ts_ms)
      .await
  }

  async fn get_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
  ) -> anyhow::Result<Option<ConsumerGroupAssignmentPlan>> {
    self.inner.get_assignment_plan(topic, group_id).await
  }

  async fn get_planner_lease(
    &self,
    topic: &str,
    group_id: &str,
  ) -> anyhow::Result<Option<ConsumerGroupPlannerLease>> {
    self.inner.get_planner_lease(topic, group_id).await
  }

  async fn acquire_or_renew_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
    now_ts_ms: i64,
    ttl_ms: i64,
  ) -> anyhow::Result<ConsumerGroupPlannerLeaseOutcome> {
    self
      .inner
      .acquire_or_renew_planner(
        topic,
        group_id,
        member_id,
        planner_session_id,
        now_ts_ms,
        ttl_ms,
      )
      .await
  }

  async fn release_planner(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
  ) -> anyhow::Result<bool> {
    self
      .inner
      .release_planner(topic, group_id, member_id, planner_session_id)
      .await
  }

  async fn publish_assignment_plan(
    &self,
    topic: &str,
    group_id: &str,
    member_id: &str,
    planner_session_id: &str,
    now_ts_ms: i64,
    plan: ConsumerGroupAssignmentPlan,
  ) -> anyhow::Result<bool> {
    self
      .inner
      .publish_assignment_plan(
        topic,
        group_id,
        member_id,
        planner_session_id,
        now_ts_ms,
        plan,
      )
      .await
  }
}

impl MutableCoordinationSource {
  fn new(snapshot: CoordinationSnapshot) -> Self {
    Self {
      snapshot: Arc::new(Mutex::new(snapshot)),
    }
  }

  fn update(&self, snapshot: CoordinationSnapshot) {
    *self.snapshot.lock() = snapshot;
  }
}

#[async_trait::async_trait]
impl ConsumerCoordinationSource for MutableCoordinationSource {
  async fn snapshot(&self) -> anyhow::Result<CoordinationSnapshot> {
    Ok(self.snapshot.lock().clone())
  }
}

#[test]
fn current_batch_for_fenced_partition_is_not_delivered() {
  let mut delivery_state = DeliveryState {
    current_batch: Some(BufferedBatch {
      virtual_partition_id: 7,
      next_offset: 1,
      source_checkpoint: CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 1,
      },
      remaining_payload_bytes: 1,
      records: vec![new_record(vec![1], 0)].into_iter(),
    }),
    ..Default::default()
  };

  assert!(
    delivery_state
      .try_take_next(
        &HashSet::new(),
        &mut HashMap::new(),
        &ConsumerIteratorMetrics::new(&metrics_scope())
      )
      .is_none()
  );
  assert!(delivery_state.current_batch.is_none());
}

#[test]
fn current_batch_remaining_bytes_decrease_as_records_are_delivered() {
  let mut delivery_state = DeliveryState {
    current_batch: Some(BufferedBatch {
      virtual_partition_id: 7,
      next_offset: 1,
      source_checkpoint: CommittedSourceCheckpoint {
        window_start_unix_seconds: 0,
        snowflake_id: 1,
      },
      remaining_payload_bytes: 3,
      records: vec![new_record(vec![1, 2, 3], 0)].into_iter(),
    }),
    ..Default::default()
  };

  let active_assignment = HashSet::from([7]);
  assert!(matches!(
    delivery_state.try_take_next(
      &active_assignment,
      &mut HashMap::new(),
      &ConsumerIteratorMetrics::new(&metrics_scope())
    ),
    Some(NextResult::Record(_))
  ));
  assert_eq!(delivery_state.retained_bytes(), 0);
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
    window_start * 1_000,
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
    let len = iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .prefetch_buffered_batch_count;
    if len >= expected_min {
      return;
    }
    sleep(Duration::from_millis(25)).await;
  }

  let len = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot()
    .prefetch_buffered_batch_count;
  assert!(
    len >= expected_min,
    "prefetch buffer length {len} did not reach expected minimum {expected_min}"
  );
}

async fn wait_for_active_assignment(iterator: &ConsumerIteratorImpl, expected_partitions: &[u32]) {
  for _ in 0 .. 40 {
    let snapshot = iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot();
    if active_partition_ids(&snapshot) == expected_partitions {
      return;
    }
    sleep(Duration::from_millis(25)).await;
  }

  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  panic!(
    "active assignment {:?} did not reach expected assignment {expected_partitions:?}",
    active_partition_ids(&snapshot)
  );
}

async fn wait_for_reader_mode(
  iterator: &ConsumerIteratorImpl,
  virtual_partition_id: VirtualPartitionId,
  expected_mode: ConsumerPartitionReadMode,
) {
  for _ in 0 .. 40 {
    let snapshot = iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot();
    if local_partition(&snapshot, virtual_partition_id)
      .reader
      .as_ref()
      .is_some_and(|reader| reader.mode == expected_mode)
    {
      return;
    }
    sleep(Duration::from_millis(25)).await;
  }

  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert_eq!(
    local_partition(&snapshot, virtual_partition_id)
      .reader
      .as_ref()
      .map(|reader| reader.mode.clone()),
    Some(expected_mode)
  );
}

fn active_partition_ids(snapshot: &ConsumerStateSnapshot) -> Vec<VirtualPartitionId> {
  snapshot
    .local
    .partitions
    .iter()
    .filter(|partition| partition.active)
    .map(|partition| partition.virtual_partition_id)
    .collect()
}

fn local_partition(
  snapshot: &ConsumerStateSnapshot,
  virtual_partition_id: VirtualPartitionId,
) -> &ConsumerLocalPartitionSnapshot {
  snapshot
    .local
    .partitions
    .iter()
    .find(|partition| partition.virtual_partition_id == virtual_partition_id)
    .unwrap()
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_consumer_test")
}

#[tokio::test]
async fn diagnostics_report_assignment_and_start_state() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0, 1],
  }));

  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();

  let diagnostics = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics");
  let snapshot = diagnostics.state_snapshot();
  assert_eq!(snapshot.schema_version, 11);
  assert!(snapshot.generated_at.ends_with('Z'));
  assert_eq!(snapshot.topic, "telemetry");
  assert_eq!(snapshot.group_id, "group-a");
  assert_eq!(snapshot.member_id, "member-a");
  assert!(!snapshot.started);
  assert_eq!(active_partition_ids(&snapshot), vec![0, 1]);
  assert_eq!(
    snapshot
      .assignment_plan
      .as_ref()
      .map(|plan| (plan.version, plan.assignments.len())),
    Some((1, 2))
  );
  assert_eq!(snapshot.prefetch_buffered_batch_count, 0);
  assert_eq!(snapshot.prefetch_buffered_record_count, 0);
  assert_eq!(snapshot.prefetch_pending_batch_count, 0);
  assert_eq!(snapshot.prefetch_pending_record_count, 0);
  assert_eq!(snapshot.prefetch_pending_bytes, 0);
  assert_eq!(snapshot.local.partitions.len(), 2);
  assert!(snapshot.local.partitions.iter().all(|partition| {
    partition.owned
      && partition.active
      && !partition.pending_assignment
      && partition.reader.as_ref().is_some_and(|reader| {
        reader.mode == ConsumerPartitionReadMode::Fresh
          && reader.recovery_next_window_start.is_some()
          && reader.recovery_cutover_window_start.is_none()
      })
  }));

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0, 1]).await;
  let started_snapshot = diagnostics.state_snapshot();
  assert!(started_snapshot.started);
  assert!(started_snapshot.prefetch_worker_running);

  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn state_response_includes_fresh_group_leases_and_other_member_commits() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let concrete_lease_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> = concrete_lease_store.clone();
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string(), "member-b".to_string()],
    virtual_partitions: vec![0, 1],
  }));
  let iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();
  let now_ts_ms = now_unix_millis();
  let member_b_key = ConsumerGroupLeaseKey {
    topic: "telemetry".to_string(),
    group_id: "group-a".to_string(),
    virtual_partition_id: 1,
  };
  concrete_lease_store
    .assign_partition(
      member_b_key.clone(),
      "member-b".to_string(),
      7,
      now_ts_ms,
      10_000,
    )
    .await
    .unwrap();
  concrete_lease_store
    .heartbeat_partition(
      &member_b_key,
      "member-b",
      7,
      now_ts_ms + 1,
      10_000,
      Some(CommittedCursor {
        virtual_partition_id: 1,
        seq_end: 42,
        source_checkpoint: Some(CommittedSourceCheckpoint {
          window_start_unix_seconds: 1_000,
          snowflake_id: 99,
        }),
      }),
    )
    .await
    .unwrap();

  let response = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_response()
    .await;
  assert_eq!(response.state.schema_version, 12);
  assert_eq!(
    response
      .state
      .assignment_plan
      .as_ref()
      .map(|plan| plan.version),
    Some(1)
  );
  let ConsumerGroupLeaseObservation::Fresh { partitions } = response.group_lease_observation else {
    panic!("expected fresh group lease observation");
  };
  assert_eq!(partitions.len(), 2);
  let member_b_partition = partitions
    .iter()
    .find(|partition| partition.virtual_partition_id == 1)
    .unwrap();
  assert_eq!(
    member_b_partition.desired_owner_id.as_deref(),
    Some("member-b")
  );
  assert_eq!(member_b_partition.owner_id.as_deref(), Some("member-b"));
  assert_eq!(member_b_partition.generation, Some(7));
  assert_eq!(member_b_partition.committed_offset, Some(42));
  assert_eq!(
    member_b_partition.committed_source_checkpoint,
    Some(ConsumerSourceCheckpointSnapshot {
      window_start: format_unix_timestamp_ms(1_000_000),
      snowflake_id: 99,
    })
  );
  let member_a_partition = partitions
    .iter()
    .find(|partition| partition.virtual_partition_id == 0)
    .unwrap();
  assert_eq!(
    member_a_partition.desired_owner_id.as_deref(),
    Some("member-a")
  );
  assert_eq!(member_a_partition.owner_id.as_deref(), Some("member-a"));
}

#[tokio::test]
async fn state_response_reports_lease_lookup_failure_without_blocking_local_diagnostics() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> = Arc::new(FailingLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0],
  }));
  let iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();
  let diagnostics = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics");
  let local_snapshot = diagnostics.state_snapshot();
  let response = diagnostics.state_response().await;

  assert_eq!(local_snapshot.schema_version, 11);
  assert_eq!(response.state.schema_version, 12);
  assert!(matches!(
    response.group_lease_observation,
    ConsumerGroupLeaseObservation::LookupFailed { .. }
  ));
}

#[test]
fn group_lease_observation_includes_unleased_plan_partitions() {
  let plan = ConsumerAssignmentPlanSnapshot {
    version: 1,
    planner_member_id: "member-a".to_string(),
    members: vec!["member-a".to_string(), "member-b".to_string()],
    assignments: vec![
      ConsumerPartitionAssignmentSnapshot {
        virtual_partition_id: 0,
        member_id: "member-a".to_string(),
      },
      ConsumerPartitionAssignmentSnapshot {
        virtual_partition_id: 1,
        member_id: "member-b".to_string(),
      },
    ],
    published_at: "2026-07-17T00:00:00Z".to_string(),
  };
  let observation = crate::diagnostics::group_lease_observation(
    Some(&plan),
    vec![ConsumerGroupLease {
      key: ConsumerGroupLeaseKey {
        topic: "telemetry".to_string(),
        group_id: "group-a".to_string(),
        virtual_partition_id: 1,
      },
      owner_id: "member-b".to_string(),
      generation: 7,
      lease_expiration_ts_ms: 20_000,
      last_heartbeat_ts_ms: 10_000,
      committed_cursor: None,
      committed_ts_ms: None,
    }],
  );
  let ConsumerGroupLeaseObservation::Fresh { partitions } = observation else {
    panic!("expected fresh group lease observation");
  };
  assert_eq!(partitions.len(), 2);
  assert_eq!(partitions[0].virtual_partition_id, 0);
  assert_eq!(partitions[0].desired_owner_id.as_deref(), Some("member-a"));
  assert!(partitions[0].owner_id.is_none());
}

#[tokio::test]
async fn assignment_callback_replays_active_partitions() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0, 1],
  }));
  let assigned_partitions = Arc::new(AtomicUsize::new(0));

  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();

  let assignment_count = Arc::clone(&assigned_partitions);
  iterator.set_assignment_callback(Arc::new(move |partitions| {
    assignment_count.fetch_add(partitions.len(), Ordering::SeqCst);
  }));

  assert_eq!(assigned_partitions.load(Ordering::SeqCst), 2);
  Box::new(iterator).shutdown().await.unwrap();
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
fn idle_poll_backoff_with_base_max_remains_constant() {
  let mut backoff = IdlePollBackoff::new(250, Some(250));

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

  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source.clone(),
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0, 1]).await;

  source.update(CoordinationSnapshot {
    members: vec!["member-a".to_string(), "member-b".to_string()],
    virtual_partitions: vec![0, 1],
  });

  for _ in 0 .. 40 {
    if iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .pending_revocation
    {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }
  assert!(
    iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .pending_revocation,
    "expected autonomous rebalance to request revocation"
  );

  let revoked = iterator.next().await.unwrap();
  let revoked = match revoked {
    NextResult::Revoked(revoked) => revoked,
    NextResult::Record(_) => panic!("expected revocation callback"),
  };

  let revoked_partitions = revoked.partitions();
  assert_eq!(revoked_partitions.len(), 1);

  assert!(
    timeout(Duration::from_millis(50), iterator.next())
      .await
      .is_err()
  );

  revoked.complete().await;
  Box::new(iterator).shutdown().await.unwrap();
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
  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
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

  let state = iterator.diagnostics.state_snapshot();
  let partition = local_partition(&state, 3);
  assert_eq!(partition.pending_commit_offset, Some(2));
  assert_eq!(partition.last_committed_offset, Some(2));
  assert!(state.local.last_successful_heartbeat_at.is_some());
}

#[tokio::test]
async fn seek_waits_for_prefetch_scan_without_stalling_heartbeats() {
  let blob_store = Arc::new(BlockingBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let concrete_membership_store = Arc::new(BlockingMembershipStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> = concrete_membership_store.clone();
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
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1], now_window * 1_000)],
  )
  .await;
  blob_store.block_reads.store(true, Ordering::SeqCst);
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime_config(),
    blob_store.clone(),
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();

  let read_started = blob_store.read_started.notified();
  iterator.start().unwrap();
  timeout(Duration::from_secs(1), read_started).await.unwrap();

  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert!(snapshot.started);
  assert_eq!(active_partition_ids(&snapshot), vec![3]);

  let heartbeat_calls_before = concrete_membership_store
    .heartbeat_calls
    .load(Ordering::SeqCst);
  let mut seek = Box::pin(iterator.seek(3, 0));
  assert!(
    timeout(Duration::from_millis(50), seek.as_mut())
      .await
      .is_err(),
    "seek completed before the blocked prefetch scan reached its command boundary"
  );
  timeout(Duration::from_secs(1), async {
    loop {
      if concrete_membership_store
        .heartbeat_calls
        .load(Ordering::SeqCst)
        > heartbeat_calls_before
      {
        return;
      }
      sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();

  blob_store.read_release.notify_waiters();
  timeout(Duration::from_secs(1), seek)
    .await
    .unwrap()
    .unwrap();
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_releases_owned_partitions_when_deregistration_fails() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let concrete_lease_store = Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> = concrete_lease_store.clone();
  let concrete_membership_store = Arc::new(BlockingMembershipStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> = concrete_membership_store.clone();

  let runtime = runtime_config();
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  assert_eq!(
    concrete_membership_store
      .inner
      .get_planner_lease("telemetry", "group-a")
      .await
      .unwrap()
      .map(|lease| lease.member_id),
    Some("member-a".to_string())
  );
  concrete_membership_store
    .fail_deregistration
    .store(true, Ordering::SeqCst);
  Box::new(iterator).shutdown().await.unwrap();

  assert!(
    concrete_membership_store
      .inner
      .get_planner_lease("telemetry", "group-a")
      .await
      .unwrap()
      .is_none()
  );
  assert_eq!(
    concrete_membership_store
      .inner
      .acquire_or_renew_planner(
        "telemetry",
        "group-a",
        "member-b",
        "member-b-session",
        now_unix_millis(),
        30_000
      )
      .await
      .unwrap(),
    ConsumerGroupPlannerLeaseOutcome::Acquired
  );

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
async fn seek_discards_prefetched_records_and_rewinds_fast_frontier() {
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

  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();
  iterator.start().unwrap();

  wait_for_prefetch_buffer_len(&iterator, 1).await;
  let buffered_before = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert_eq!(buffered_before.prefetch_buffered_batch_count, 1);
  assert_eq!(buffered_before.prefetch_buffered_record_count, 2);
  assert_eq!(buffered_before.prefetch_pending_batch_count, 0);
  assert_eq!(buffered_before.prefetch_pending_record_count, 0);

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

  let in_flight_snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert_eq!(
    local_partition(&in_flight_snapshot, 7).prefetch_buffered_record_count,
    in_flight_snapshot.prefetch_buffered_record_count
  );

  timeout(Duration::from_secs(2), iterator.seek(7, 0))
    .await
    .unwrap()
    .unwrap();
  let rewound = timeout(Duration::from_secs(2), iterator.next())
    .await
    .unwrap()
    .unwrap();
  let rewound_record = match rewound {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected rewound record"),
  };
  assert_eq!(rewound_record.virtual_partition_id, 7);
  assert_eq!(rewound_record.offset, 1);
  for expected_offset in 2 ..= 6 {
    let next = timeout(Duration::from_secs(2), iterator.next())
      .await
      .unwrap()
      .unwrap();
    let NextResult::Record(record) = next else {
      panic!("expected replayed record");
    };
    assert_eq!(record.offset, expected_offset);
  }
  wait_for_reader_mode(&iterator, 7, ConsumerPartitionReadMode::Fast).await;
  let recovered_snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert_eq!(
    local_partition(&recovered_snapshot, 7)
      .reader
      .as_ref()
      .unwrap()
      .mode,
    ConsumerPartitionReadMode::Fast
  );

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

  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source.clone(),
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_prefetch_buffer_len(&iterator, 2).await;

  source.update(CoordinationSnapshot {
    members: vec!["member-a".to_string(), "member-b".to_string()],
    virtual_partitions: vec![0, 1],
  });

  for _ in 0 .. 40 {
    if iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .pending_revocation
    {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }
  assert!(
    iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .pending_revocation,
    "expected autonomous rebalance to request revocation"
  );

  let revoked = iterator.next().await.unwrap();
  let revoked = match revoked {
    NextResult::Revoked(revoked) => revoked,
    NextResult::Record(_) => panic!("expected revocation callback"),
  };
  let revoked_partitions = revoked.partitions();
  assert_eq!(revoked_partitions.len(), 1);
  let revoked_partition = revoked_partitions[0];

  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert!(
    snapshot
      .local
      .partitions
      .iter()
      .find(|partition| partition.virtual_partition_id == revoked_partition)
      .is_none_or(|partition| partition.prefetch_buffered_batch_count == 0),
    "revoked partition batch was not removed from prefetch buffer"
  );

  revoked.complete().await;
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_next_does_not_restart_scheduled_heartbeat() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store = Arc::new(BlockingMembershipStore::new());
  membership_store
    .block_heartbeats
    .store(true, Ordering::SeqCst);
  let heartbeat_started = membership_store.heartbeat_started.clone();

  let mut runtime = runtime_config();
  runtime.group.as_mut().unwrap().heartbeat_interval_ms = Some(60_000);
  runtime.group.as_mut().unwrap().rebalance_interval_ms = Some(60_000);
  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![0],
    }));
  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store.clone(),
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();

  let heartbeat_wait = heartbeat_started.notified();
  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0]).await;
  timeout(Duration::from_secs(1), heartbeat_wait)
    .await
    .unwrap();

  for _ in 0 .. 8 {
    tokio::select! {
      () = tokio::task::yield_now() => {},
      _ = iterator.next() => panic!("cancelled poll unexpectedly completed"),
    }
  }

  membership_store
    .block_heartbeats
    .store(false, Ordering::SeqCst);
  membership_store.heartbeat_release.notify_waiters();
  for _ in 0 .. 40 {
    let snapshot = iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot();
    if snapshot.local.last_successful_heartbeat_at.is_some() {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }

  assert_eq!(membership_store.heartbeat_calls.load(Ordering::SeqCst), 1);
  let snapshot = iterator
    .diagnostics()
    .expect("consumer implementation provides diagnostics")
    .state_snapshot();
  assert!(snapshot.local.last_successful_heartbeat_at.is_some());
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_next_does_not_restart_rebalance() {
  let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
  let metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let lease_store: Arc<dyn ConsumerGroupLeaseStore> =
    Arc::new(InMemoryConsumerGroupLeaseStore::new());
  let membership_store: Arc<dyn ConsumerGroupMembershipStore> =
    Arc::new(InMemoryConsumerGroupMembershipStore::new());
  let source = Arc::new(BlockingCoordinationSource::new(CoordinationSnapshot {
    members: vec!["member-a".to_string()],
    virtual_partitions: vec![0],
  }));

  let mut runtime = runtime_config();
  runtime.group.as_mut().unwrap().heartbeat_interval_ms = Some(60_000);
  runtime.group.as_mut().unwrap().rebalance_interval_ms = Some(60_000);
  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime,
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source.clone(),
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_active_assignment(&iterator, &[0]).await;
  source.block_snapshots.store(true, Ordering::SeqCst);
  let rebalance_started = source.snapshot_started.notified();

  tokio::select! {
    () = rebalance_started => {},
    _ = iterator.next() => panic!("cancelled poll unexpectedly completed"),
  }

  source.snapshot_release.notify_waiters();
  for _ in 0 .. 40 {
    if source.snapshot_calls.load(Ordering::SeqCst) == 2 {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }

  assert_eq!(source.snapshot_calls.load(Ordering::SeqCst), 2);
  Box::new(iterator).shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_next_preserves_prefetched_record() {
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
    SeqRange { start: 1, end: 1 },
    vec![new_record(vec![1, 2, 3], now_window * 1_000)],
  )
  .await;

  let source: Arc<dyn ConsumerCoordinationSource> =
    Arc::new(MutableCoordinationSource::new(CoordinationSnapshot {
      members: vec!["member-a".to_string()],
      virtual_partitions: vec![3],
    }));
  let mut iterator = ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
    &runtime_config(),
    blob_store,
    metadata_store,
    lease_store,
    membership_store,
    source,
    metrics_scope(),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )
  .await
  .unwrap();

  iterator.start().unwrap();
  wait_for_prefetch_buffer_len(&iterator, 1).await;
  for _ in 0 .. 40 {
    if iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .delivery_state
      == ConsumerDeliveryState::Pending
    {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }
  assert_eq!(
    iterator
      .diagnostics()
      .expect("consumer implementation provides diagnostics")
      .state_snapshot()
      .local
      .delivery_state,
    ConsumerDeliveryState::Pending
  );

  tokio::select! {
    biased;
    () = async {} => {},
    _ = iterator.next() => panic!("cancelled poll unexpectedly received the ready record"),
  }

  let next = timeout(Duration::from_secs(1), iterator.next())
    .await
    .unwrap()
    .unwrap();
  let record = match next {
    NextResult::Record(record) => record,
    NextResult::Revoked(_) => panic!("expected record"),
  };
  assert_eq!(record.virtual_partition_id, 3);
  assert_eq!(record.offset, 1);
  assert_eq!(record.record.payload.to_vec(), vec![1, 2, 3]);
  Box::new(iterator).shutdown().await.unwrap();
}
