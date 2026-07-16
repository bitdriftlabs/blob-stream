#[cfg(test)]
#[path = "./iterator_test.rs"]
mod tests;

use crate::config::{
  ConsumerGroupConfig,
  ConsumerRuntimeConfig,
  consumer_heartbeat_interval_ms,
  consumer_idle_poll_delay_ms,
  consumer_lease_duration_ms,
  consumer_max_idle_poll_delay_ms,
  consumer_prefetch_max_bytes,
  consumer_rebalance_interval_ms,
  validate_runtime_config,
};
use crate::consumer::{ConsumerBatch, ConsumerReader, ConsumerReaderImpl};
use crate::coordination::{
  ConsumerGroupCoordinator,
  ConsumerGroupCoordinatorImpl,
  HeartbeatReport,
  RebalanceReport,
};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use bd_log::warn_every;
use bd_server_stats::stats::Scope;
use blob_stream_blob_store::BlobStore;
use blob_stream_metadata_store::{
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  MetadataStore,
};
use blob_stream_types::{
  Record,
  VirtualPartitionId,
  format_unix_timestamp_ms,
  now_unix_millis,
  now_unix_seconds,
};
use log::{debug, info, trace};
use parking_lot::Mutex as ParkingLotMutex;
use prometheus::{Histogram, IntCounter, IntGauge};
use serde::Serialize;
use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use time::ext::NumericalDuration;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

//
// IdlePollBackoff
//

#[derive(Clone, Debug)]
struct IdlePollBackoff {
  base_delay_ms: u64,
  max_delay_ms: Option<u64>,
  current_delay_ms: u64,
}

impl IdlePollBackoff {
  fn new(base_delay_ms: u64, max_delay_ms: Option<u64>) -> Self {
    Self {
      base_delay_ms,
      max_delay_ms,
      current_delay_ms: base_delay_ms,
    }
  }

  fn next_delay_ms(&mut self) -> u64 {
    let delay_ms = self.current_delay_ms;
    if let Some(max_delay_ms) = self.max_delay_ms {
      self.current_delay_ms = self.current_delay_ms.saturating_mul(2).min(max_delay_ms);
    }
    delay_ms
  }

  fn reset(&mut self) {
    self.current_delay_ms = self.base_delay_ms;
  }
}

//
// ConsumerIteratorMetrics
//

#[derive(Clone)]
struct ConsumerIteratorMetrics {
  batches_delivered: IntCounter,
  records_delivered: IntCounter,
  retries: IntCounter,
  failures: IntCounter,
  revocations: IntCounter,
  prefetch_buffered_batches: IntGauge,
  prefetch_buffered_bytes: IntGauge,
  prefetch_paused_budget: IntCounter,
  prefetch_refill_cycles: IntCounter,
  heartbeat_calls: IntCounter,
  heartbeat_scheduled_calls: IntCounter,
  heartbeat_commit_calls: IntCounter,
  heartbeat_failures: IntCounter,
  heartbeat_committed_offsets: IntCounter,
  heartbeat_renewed_partitions: IntCounter,
  heartbeat_fenced_partitions: IntCounter,
  heartbeat_latency_seconds: Histogram,
  next_latency_seconds: Histogram,
  commit_latency_seconds: Histogram,
}

impl ConsumerIteratorMetrics {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("iterator");
    Self {
      batches_delivered: scope.counter("batches_delivered"),
      records_delivered: scope.counter("records_delivered"),
      retries: scope.counter("retries"),
      failures: scope.counter("failures"),
      revocations: scope.counter("revocations"),
      prefetch_buffered_batches: scope.gauge("prefetch_buffered_batches"),
      prefetch_buffered_bytes: scope.gauge("prefetch_buffered_bytes"),
      prefetch_paused_budget: scope.counter("prefetch_paused_budget"),
      prefetch_refill_cycles: scope.counter("prefetch_refill_cycles"),
      heartbeat_calls: scope.counter("heartbeat_calls"),
      heartbeat_scheduled_calls: scope.counter("heartbeat_scheduled_calls"),
      heartbeat_commit_calls: scope.counter("heartbeat_commit_calls"),
      heartbeat_failures: scope.counter("heartbeat_failures"),
      heartbeat_committed_offsets: scope.counter("heartbeat_committed_offsets"),
      heartbeat_renewed_partitions: scope.counter("heartbeat_renewed_partitions"),
      heartbeat_fenced_partitions: scope.counter("heartbeat_fenced_partitions"),
      heartbeat_latency_seconds: scope.histogram("heartbeat_latency_seconds"),
      next_latency_seconds: scope.histogram("next_latency_seconds"),
      commit_latency_seconds: scope.histogram("commit_latency_seconds"),
    }
  }
}

//
// HeartbeatTrigger
//

#[derive(Clone, Copy)]
enum HeartbeatTrigger {
  Scheduled,
  Commit,
}

impl HeartbeatTrigger {
  fn as_str(self) -> &'static str {
    match self {
      Self::Scheduled => "scheduled",
      Self::Commit => "commit",
    }
  }
}

//
// BufferedBatch
//

struct BufferedBatch {
  virtual_partition_id: VirtualPartitionId,
  next_offset: u64,
  records: std::vec::IntoIter<Record>,
}

//
// DeliveryState
//

#[derive(Default)]
struct DeliveryState {
  batches: VecDeque<ConsumerBatch>,
  buffered_bytes: u64,
  current_batch: Option<BufferedBatch>,
  pending_revocation: Option<NextResult>,
}

impl DeliveryState {
  fn try_take_next(
    &mut self,
    active_assignment: &HashSet<VirtualPartitionId>,
    metrics: &ConsumerIteratorMetrics,
  ) -> Option<NextResult> {
    if let Some(revocation) = self.pending_revocation.take() {
      return Some(revocation);
    }

    loop {
      if let Some(current_batch) = self.current_batch.as_mut() {
        if let Some(record) = current_batch.records.next() {
          let offset = current_batch.next_offset;
          current_batch.next_offset = current_batch.next_offset.saturating_add(1);
          metrics.records_delivered.inc();
          return Some(NextResult::Record(ConsumerRecord {
            virtual_partition_id: current_batch.virtual_partition_id,
            offset,
            record,
          }));
        }
        self.current_batch = None;
      }

      let batch = self.batches.pop_front()?;
      self.buffered_bytes = self
        .buffered_bytes
        .saturating_sub(prefetched_batch_bytes(&batch));
      update_worker_prefetch_metrics(metrics, self);
      if !active_assignment.contains(&batch.virtual_partition_id) {
        continue;
      }

      metrics.batches_delivered.inc();
      self.current_batch = Some(BufferedBatch {
        virtual_partition_id: batch.virtual_partition_id,
        next_offset: batch.seq_range.start,
        records: batch.records.into_iter(),
      });
    }
  }

  fn drop_revoked_partitions(&mut self, revoked: &HashSet<VirtualPartitionId>) {
    if self
      .current_batch
      .as_ref()
      .is_some_and(|batch| revoked.contains(&batch.virtual_partition_id))
    {
      self.current_batch = None;
    }

    self
      .batches
      .retain(|batch| !revoked.contains(&batch.virtual_partition_id));
    self.buffered_bytes = self.batches.iter().map(prefetched_batch_bytes).sum();
  }
}

//
// CoordinationSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Snapshot of membership and partition space used during rebalance.
pub struct CoordinationSnapshot {
  /// Active member ids in the consumer group.
  pub members: Vec<String>,
  /// Full partition space considered by the coordinator.
  pub virtual_partitions: Vec<VirtualPartitionId>,
}

//
// ConsumerCoordinationSource
//

#[async_trait]
/// Source of coordination snapshots for group rebalances.
pub trait ConsumerCoordinationSource: Send + Sync {
  /// Return current view of group members and partitions.
  async fn snapshot(&self) -> Result<CoordinationSnapshot>;
}

//
// RevokedPartitions
//

#[async_trait]
/// Handle for acknowledging completion of partition revocations.
pub trait RevokedPartitions: Send {
  /// Revoked partition ids.
  fn partitions(&self) -> Vec<VirtualPartitionId>;
  /// Signal that in-flight work for revoked partitions has drained.
  async fn complete(self: Box<Self>);
}

struct RevokedPartitionsImpl {
  revoked: Vec<VirtualPartitionId>,
  completion_tx: Option<oneshot::Sender<()>>,
  completion_notify: Arc<Notify>,
}

impl Drop for RevokedPartitionsImpl {
  fn drop(&mut self) {
    drop(self.completion_tx.take());
    self.completion_notify.notify_one();
  }
}

#[async_trait]
impl RevokedPartitions for RevokedPartitionsImpl {
  fn partitions(&self) -> Vec<VirtualPartitionId> {
    self.revoked.clone()
  }

  async fn complete(mut self: Box<Self>) {
    if let Some(completion_tx) = self.completion_tx.take() {
      let _ = completion_tx.send(());
      self.completion_notify.notify_one();
    }
  }
}

//
// NextResult
//

#[derive(Clone, Debug, PartialEq)]
/// A single decoded record surfaced by the iterator.
pub struct ConsumerRecord {
  /// Virtual partition that owns this record.
  pub virtual_partition_id: VirtualPartitionId,
  /// Inclusive sequence offset for this record.
  pub offset: u64,
  /// Decoded record payload and metadata.
  pub record: Record,
}

/// Result of polling the iterator.
pub enum NextResult {
  /// Next available record for an owned partition.
  Record(ConsumerRecord),
  /// Notification that partitions were revoked and must be drained.
  Revoked(Box<dyn RevokedPartitions>),
}

#[derive(Debug, PartialEq, Eq, Serialize)]
/// Delivery state shared by the consumer facade and driver.
pub enum ConsumerDeliveryState {
  /// No record or revocation event is currently available to a caller.
  Idle,
  /// A record or revocation event is available to the next caller poll.
  Pending,
}

//
// ConsumerStateSnapshot
//

#[derive(Debug, Serialize)]
pub struct ConsumerStateSnapshot {
  pub schema_version: u32,
  pub generated_at: String,
  pub topic: String,
  pub group_id: String,
  pub member_id: String,
  pub started: bool,
  pub coordinator_generation: u64,
  pub owned_partitions: Vec<VirtualPartitionId>,
  pub active_assignment: Vec<VirtualPartitionId>,
  pub pending_assignment: Option<Vec<VirtualPartitionId>>,
  pub pending_revocation: bool,
  pub delivery_state: ConsumerDeliveryState,
  pub pending_commits: Vec<ConsumerOffsetSnapshot>,
  pub staged_offsets: Vec<ConsumerOffsetSnapshot>,
  pub last_committed_offsets: Vec<ConsumerOffsetSnapshot>,
  pub last_successful_heartbeat_at: Option<String>,
  pub cursors: Vec<ConsumerOffsetSnapshot>,
  pub next_heartbeat_at: String,
  pub next_rebalance_at: String,
  pub prefetch_buffered_batch_count: usize,
  pub prefetch_buffered_bytes: u64,
  pub prefetch_buffered_partitions: Vec<VirtualPartitionId>,
  pub prefetch_max_bytes: u64,
  pub prefetch_worker_running: bool,
}

//
// ConsumerOffsetSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConsumerOffsetSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub offset: u64,
}

//
// ConsumerDiagnosticsRuntimeState
//

#[derive(Default)]
struct ConsumerDiagnosticsRuntimeState {
  coordinator_generation: u64,
  owned_partitions: Vec<VirtualPartitionId>,
  active_assignment: Vec<VirtualPartitionId>,
  pending_assignment: Option<Vec<VirtualPartitionId>>,
  pending_revocation: bool,
  last_committed_offsets: HashMap<VirtualPartitionId, u64>,
  last_successful_heartbeat_at_ms: Option<i64>,
  next_heartbeat_at_ms: i64,
  next_rebalance_at_ms: i64,
}

//
// ConsumerSharedState
//

/// Synchronous state jointly accessed by the driver, prefetch task, and public iterator facade.
#[derive(Default)]
struct ConsumerSharedState {
  active_assignment: HashSet<VirtualPartitionId>,
  pending_commits: HashMap<VirtualPartitionId, u64>,
  delivery_state: DeliveryState,
  terminal_error: Option<String>,
  diagnostics: ConsumerDiagnosticsRuntimeState,
}

//
// ConsumerDiagnostics
//

#[derive(Clone)]
pub struct ConsumerDiagnostics {
  group_config: ConsumerGroupConfig,
  reader: Arc<Mutex<ConsumerReaderImpl>>,
  shared_state: Arc<ParkingLotMutex<ConsumerSharedState>>,
  prefetch_max_bytes: u64,
  started: Arc<AtomicBool>,
  prefetch_shutdown: Arc<AtomicBool>,
}

impl ConsumerDiagnostics {
  #[must_use]
  pub async fn state_snapshot(&self) -> ConsumerStateSnapshot {
    let generated_at_ts_ms = now_unix_millis();
    let generated_at = format_unix_timestamp_ms(generated_at_ts_ms);
    let (
      mut owned_partitions,
      mut active_assignment,
      mut pending_assignment,
      pending_revocation,
      pending_commits,
      last_committed_offsets,
      last_successful_heartbeat_at_ms,
      coordinator_generation,
      next_heartbeat_at_ms,
      next_rebalance_at_ms,
      delivery_state,
      prefetch_buffered_batch_count,
      prefetch_buffered_bytes,
      prefetch_buffered_partitions,
    ) = {
      let shared_state = self.shared_state.lock();
      let runtime_state = &shared_state.diagnostics;
      let mut prefetch_buffered_partitions = shared_state
        .delivery_state
        .batches
        .iter()
        .map(|batch| batch.virtual_partition_id)
        .collect::<Vec<_>>();
      prefetch_buffered_partitions.sort_unstable();
      (
        runtime_state.owned_partitions.clone(),
        runtime_state.active_assignment.clone(),
        runtime_state.pending_assignment.clone(),
        runtime_state.pending_revocation,
        offsets_from_map(&shared_state.pending_commits),
        offsets_from_map(&runtime_state.last_committed_offsets),
        runtime_state.last_successful_heartbeat_at_ms,
        runtime_state.coordinator_generation,
        runtime_state.next_heartbeat_at_ms,
        runtime_state.next_rebalance_at_ms,
        if shared_state.delivery_state.pending_revocation.is_some()
          || shared_state.delivery_state.current_batch.is_some()
          || !shared_state.delivery_state.batches.is_empty()
        {
          ConsumerDeliveryState::Pending
        } else {
          ConsumerDeliveryState::Idle
        },
        shared_state.delivery_state.batches.len(),
        shared_state.delivery_state.buffered_bytes,
        prefetch_buffered_partitions,
      )
    };
    owned_partitions.sort_unstable();
    active_assignment.sort_unstable();
    if let Some(assignment) = &mut pending_assignment {
      assignment.sort_unstable();
    }

    let mut cursors = offsets_from_map(&self.reader.lock().await.cursors());

    ConsumerStateSnapshot {
      schema_version: 6,
      generated_at,
      topic: self.group_config.topic.to_string(),
      group_id: self.group_config.group_id.to_string(),
      member_id: self.group_config.member_id.to_string(),
      started: self.started.load(Ordering::Acquire),
      coordinator_generation,
      owned_partitions,
      active_assignment,
      pending_assignment,
      pending_revocation,
      delivery_state,
      pending_commits: pending_commits.clone(),
      staged_offsets: pending_commits,
      last_committed_offsets,
      last_successful_heartbeat_at: last_successful_heartbeat_at_ms.map(format_unix_timestamp_ms),
      cursors: std::mem::take(&mut cursors),
      next_heartbeat_at: format_unix_timestamp_ms(next_heartbeat_at_ms),
      next_rebalance_at: format_unix_timestamp_ms(next_rebalance_at_ms),
      prefetch_buffered_batch_count,
      prefetch_buffered_bytes,
      prefetch_buffered_partitions,
      prefetch_max_bytes: self.prefetch_max_bytes,
      prefetch_worker_running: self.started.load(Ordering::Acquire)
        && !self.prefetch_shutdown.load(Ordering::Acquire),
    }
  }

  #[cfg(feature = "admin")]
  pub fn admin_router(self) -> axum::Router {
    crate::admin::router(self)
  }
}

fn offsets_from_map(offsets: &HashMap<VirtualPartitionId, u64>) -> Vec<ConsumerOffsetSnapshot> {
  let mut snapshots = offsets
    .iter()
    .map(|(virtual_partition_id, offset)| ConsumerOffsetSnapshot {
      virtual_partition_id: *virtual_partition_id,
      offset: *offset,
    })
    .collect::<Vec<_>>();
  snapshots.sort_by_key(|snapshot| snapshot.virtual_partition_id);
  snapshots
}

//
// ConsumerIterator
//

#[async_trait]
/// High-level pull API used by applications.
pub trait ConsumerIterator: Send + Sync {
  /// Start iterator processing and initialize internal timers/state.
  fn start(&mut self) -> Result<()>;
  /// Poll for either a new batch or revocation event.
  async fn next(&mut self) -> Result<NextResult>;
  /// Stage an offset for commit on the next `commit`/heartbeat.
  fn store_offset(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()>;
  /// Flush staged offsets and heartbeat owned partitions.
  async fn commit(&mut self) -> Result<HeartbeatReport>;
  /// Shutdown iterator and release owned partitions.
  async fn shutdown(self: Box<Self>) -> Result<()>;
  /// Reposition read cursor for a partition.
  async fn seek(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()>;
  /// Returns a handle for observing this consumer's local runtime state.
  fn diagnostics(&self) -> Option<ConsumerDiagnostics> {
    None
  }
}

//
// ConsumerIteratorImpl
//

/// Long-lived owner of consumer coordination and prefetch lifecycle state.
struct ConsumerDriver {
  group_config: ConsumerGroupConfig,
  reader: Arc<Mutex<ConsumerReaderImpl>>,
  coordinator: Box<dyn ConsumerGroupCoordinator>,
  membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  coordination_source: Arc<dyn ConsumerCoordinationSource>,
  last_coordination_snapshot: Option<CoordinationSnapshot>,
  metrics: ConsumerIteratorMetrics,
  started: bool,
  shared_state: Arc<ParkingLotMutex<ConsumerSharedState>>,
  prefetch_max_bytes: u64,
  delivery_notify: Arc<Notify>,
  prefetch_space_notify: Arc<Notify>,
  prefetch_shutdown: Arc<AtomicBool>,
  prefetch_task: Option<JoinHandle<()>>,
  prefetch_idle_base_delay_ms: u64,
  prefetch_idle_max_delay_ms: Option<u64>,
  active_assignment: HashSet<VirtualPartitionId>,
  pending_assignment: Option<Vec<VirtualPartitionId>>,
  pending_revocation_completion: Option<oneshot::Receiver<()>>,
  revocation_notify: Arc<Notify>,
  next_heartbeat_at_ms: i64,
  next_rebalance_at_ms: i64,
  diagnostics: ConsumerDiagnostics,
}

//
// ConsumerDriverCommand
//

enum ConsumerDriverCommand {
  Commit {
    response: oneshot::Sender<Result<HeartbeatReport>>,
  },
  Seek {
    virtual_partition_id: VirtualPartitionId,
    offset: u64,
    response: oneshot::Sender<Result<()>>,
  },
  Shutdown {
    response: oneshot::Sender<Result<()>>,
  },
}

//
// ConsumerIteratorImpl
//

/// Default consumer iterator implementation.
pub struct ConsumerIteratorImpl {
  started: bool,
  diagnostics: ConsumerDiagnostics,
  shared_state: Arc<ParkingLotMutex<ConsumerSharedState>>,
  delivery_notify: Arc<Notify>,
  prefetch_space_notify: Arc<Notify>,
  metrics: ConsumerIteratorMetrics,
  command_tx: Option<mpsc::UnboundedSender<ConsumerDriverCommand>>,
  driver: Option<ConsumerDriver>,
  driver_task: Option<JoinHandle<()>>,
}

impl ConsumerIteratorImpl {
  /// Build an iterator from explicit runtime config and backend dependencies.
  pub async fn from_runtime_config(
    runtime: &ConsumerRuntimeConfig,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
    membership_store: Arc<dyn ConsumerGroupMembershipStore>,
    coordination_source: Arc<dyn ConsumerCoordinationSource>,
    metrics_scope: Scope,
  ) -> Result<Self> {
    validate_runtime_config(runtime)?;
    let read_config = runtime
      .read
      .as_ref()
      .ok_or_else(|| anyhow!("consumer read config is required"))?
      .clone();
    let group_config = runtime
      .group
      .as_ref()
      .ok_or_else(|| anyhow!("consumer group config is required"))?
      .clone();
    let idle_poll_delay_ms = consumer_idle_poll_delay_ms(&read_config);
    let max_idle_poll_delay_ms = Some(consumer_max_idle_poll_delay_ms(&read_config));
    let prefetch_max_bytes = consumer_prefetch_max_bytes(&read_config);

    let active_assignment = HashSet::new();
    let shared_state = Arc::new(ParkingLotMutex::new(ConsumerSharedState::default()));
    let reader = Arc::new(Mutex::new(ConsumerReaderImpl::new(
      read_config,
      Vec::new(),
      HashMap::new(),
      blob_store,
      metadata_store,
      &metrics_scope.scope("consumer"),
    )?));
    let coordinator = ConsumerGroupCoordinatorImpl::new(group_config.clone(), lease_store)?;
    let now_ts_ms = now_unix_millis();
    let delivery_notify = Arc::new(Notify::new());
    let prefetch_space_notify = Arc::new(Notify::new());
    let revocation_notify = Arc::new(Notify::new());
    let started = Arc::new(AtomicBool::new(false));
    let prefetch_shutdown = Arc::new(AtomicBool::new(false));
    let diagnostics = ConsumerDiagnostics {
      group_config: group_config.clone(),
      reader: Arc::clone(&reader),
      shared_state: Arc::clone(&shared_state),
      prefetch_max_bytes,
      started: Arc::clone(&started),
      prefetch_shutdown: Arc::clone(&prefetch_shutdown),
    };
    {
      let mut state = shared_state.lock();
      state.diagnostics.next_heartbeat_at_ms = now_ts_ms;
      state.diagnostics.next_rebalance_at_ms = now_ts_ms;
    }

    let mut driver = ConsumerDriver {
      group_config,
      reader,
      coordinator: Box::new(coordinator),
      membership_store,
      coordination_source,
      last_coordination_snapshot: None,
      metrics: ConsumerIteratorMetrics::new(&metrics_scope.scope("consumer")),
      started: false,
      shared_state: Arc::clone(&shared_state),
      prefetch_max_bytes,
      delivery_notify: Arc::clone(&delivery_notify),
      prefetch_space_notify: Arc::clone(&prefetch_space_notify),
      prefetch_shutdown,
      prefetch_task: None,
      prefetch_idle_base_delay_ms: idle_poll_delay_ms,
      prefetch_idle_max_delay_ms: max_idle_poll_delay_ms,
      active_assignment,
      pending_assignment: None,
      pending_revocation_completion: None,
      revocation_notify,
      next_heartbeat_at_ms: now_ts_ms,
      next_rebalance_at_ms: now_ts_ms,
      diagnostics,
    };

    driver
      .membership_store
      .register_member(
        &driver.group_config.topic,
        &driver.group_config.group_id,
        &driver.group_config.member_id,
        now_ts_ms,
        consumer_lease_duration_ms(&driver.group_config),
      )
      .await?;
    let snapshot = driver.coordination_source.snapshot().await?;
    driver.record_coordination_snapshot(&snapshot);
    let report = driver
      .coordinator
      .rebalance(snapshot.members, snapshot.virtual_partitions, now_ts_ms)
      .await?;
    let owned = driver.apply_rebalance_report(report).await?;
    info!(
      "consumer iterator bootstrapped: topic={}, group_id={}, member_id={}, owned={}",
      driver.group_config.topic,
      driver.group_config.group_id,
      driver.group_config.member_id,
      owned.len()
    );

    Ok(Self {
      started: false,
      diagnostics: driver.diagnostics.clone(),
      shared_state,
      delivery_notify,
      prefetch_space_notify,
      metrics: driver.metrics.clone(),
      command_tx: None,
      driver: Some(driver),
      driver_task: None,
    })
  }
}

impl ConsumerDriver {
  async fn apply_rebalance_report(
    &mut self,
    report: RebalanceReport,
  ) -> Result<Vec<VirtualPartitionId>> {
    let RebalanceReport {
      owned_partitions,
      committed_cursors,
    } = report;
    self.hydrate_cursors(committed_cursors).await;

    self.apply_assignment(&owned_partitions).await?;
    Ok(owned_partitions)
  }

  async fn hydrate_cursors(&self, committed_cursors: HashMap<VirtualPartitionId, u64>) {
    if committed_cursors.is_empty() {
      return;
    }

    let mut reader = self.reader.lock().await;
    for (partition_id, seq_end) in committed_cursors {
      reader.hydrate_cursor(partition_id, seq_end);
    }
  }

  fn record_coordination_snapshot(&mut self, snapshot: &CoordinationSnapshot) {
    let Some(previous) = self.last_coordination_snapshot.as_ref() else {
      info!(
        "consumer membership snapshot initialized: topic={}, group_id={}, member_id={}, \
         members={:?}, partitions={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        snapshot.members,
        snapshot.virtual_partitions
      );
      self.last_coordination_snapshot = Some(snapshot.clone());
      return;
    };

    if previous == snapshot {
      return;
    }

    if previous.members != snapshot.members {
      info!(
        "consumer membership changed: topic={}, group_id={}, member_id={}, previous_members={:?}, \
         members={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        previous.members,
        snapshot.members
      );
    }

    if previous.virtual_partitions != snapshot.virtual_partitions {
      info!(
        "consumer partition space changed: topic={}, group_id={}, member_id={}, \
         previous_partitions={:?}, partitions={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        previous.virtual_partitions,
        snapshot.virtual_partitions
      );
    }

    self.last_coordination_snapshot = Some(snapshot.clone());
  }

  async fn apply_assignment(&mut self, assignment: &[VirtualPartitionId]) -> Result<()> {
    let active_assignment = assignment.iter().copied().collect();
    self
      .apply_assignment_with_active_set(assignment, active_assignment)
      .await
  }

  async fn apply_assignment_with_active_set(
    &mut self,
    assignment: &[VirtualPartitionId],
    active_assignment: HashSet<VirtualPartitionId>,
  ) -> Result<()> {
    let assignment_changed = self.active_assignment != active_assignment;
    self
      .reader
      .lock()
      .await
      .set_assigned_virtual_partitions(assignment.to_owned())?;
    self.active_assignment = active_assignment;
    {
      let mut shared_state = self.shared_state.lock();
      shared_state
        .pending_commits
        .retain(|partition_id, _| self.active_assignment.contains(partition_id));
      shared_state
        .active_assignment
        .clone_from(&self.active_assignment);
      self.refresh_diagnostics_locked(&mut shared_state);
    }
    if assignment_changed {
      info!(
        "consumer assignment active: topic={}, group_id={}, member_id={}, partitions={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        assignment
      );
    }
    Ok(())
  }

  fn refresh_diagnostics(&self) {
    let mut shared_state = self.shared_state.lock();
    self.refresh_diagnostics_locked(&mut shared_state);
  }

  fn refresh_diagnostics_locked(&self, shared_state: &mut ConsumerSharedState) {
    let diagnostics = &mut shared_state.diagnostics;
    diagnostics.coordinator_generation = self.coordinator.generation();
    diagnostics.owned_partitions = self.coordinator.owned_partitions();
    diagnostics.active_assignment = self.active_assignment.iter().copied().collect();
    diagnostics
      .pending_assignment
      .clone_from(&self.pending_assignment);
    diagnostics.pending_revocation = self.pending_revocation_completion.is_some();
    diagnostics.next_heartbeat_at_ms = self.next_heartbeat_at_ms;
    diagnostics.next_rebalance_at_ms = self.next_rebalance_at_ms;
  }

  fn refresh_rebalance_diagnostics(&self) {
    let mut shared_state = self.shared_state.lock();
    let diagnostics = &mut shared_state.diagnostics;
    diagnostics.coordinator_generation = self.coordinator.generation();
    diagnostics.next_rebalance_at_ms = self.next_rebalance_at_ms;
  }

  async fn finish_pending_revocation_if_completed(&mut self) -> Result<bool> {
    let Some(recv) = self.pending_revocation_completion.as_mut() else {
      return Ok(true);
    };

    match recv.try_recv() {
      Ok(()) | Err(oneshot::error::TryRecvError::Closed) => {
        let assignment = self.pending_assignment.take().unwrap_or_default();
        self.pending_revocation_completion = None;
        self.apply_assignment(&assignment).await?;
        Ok(true)
      },
      Err(oneshot::error::TryRecvError::Empty) => Ok(false),
    }
  }

  async fn maybe_rebalance(&mut self, now_ts_ms: i64) -> Result<()> {
    if now_ts_ms < self.next_rebalance_at_ms {
      return Ok(());
    }

    let snapshot = self.coordination_source.snapshot().await?;
    self.record_coordination_snapshot(&snapshot);
    let RebalanceReport {
      owned_partitions: next_assignment,
      committed_cursors,
    } = self
      .coordinator
      .rebalance(snapshot.members, snapshot.virtual_partitions, now_ts_ms)
      .await?;

    self.next_rebalance_at_ms = now_ts_ms + consumer_rebalance_interval_ms(&self.group_config);

    let next_assignment_set = next_assignment.iter().copied().collect::<HashSet<_>>();
    let assignment_changed = self.active_assignment != next_assignment_set;
    self.hydrate_cursors(committed_cursors).await;

    if !assignment_changed {
      self.refresh_rebalance_diagnostics();
      return Ok(());
    }

    let revoked = self
      .active_assignment
      .difference(&next_assignment_set)
      .copied()
      .collect::<Vec<_>>();

    if revoked.is_empty() {
      self
        .apply_assignment_with_active_set(&next_assignment, next_assignment_set)
        .await?;
      return Ok(());
    }

    self.metrics.revocations.inc();
    let revoked_set = revoked.iter().copied().collect::<HashSet<_>>();
    let (completion_tx, completion_rx) = oneshot::channel();
    {
      let mut shared_state = self.shared_state.lock();
      let delivery_state = &mut shared_state.delivery_state;
      delivery_state.drop_revoked_partitions(&revoked_set);
      delivery_state.pending_revocation =
        Some(NextResult::Revoked(Box::new(RevokedPartitionsImpl {
          revoked: revoked.clone(),
          completion_tx: Some(completion_tx),
          completion_notify: Arc::clone(&self.revocation_notify),
        })));
      update_worker_prefetch_metrics(&self.metrics, delivery_state);
    }
    self.prefetch_space_notify.notify_waiters();

    self.pending_assignment = Some(next_assignment);
    self.pending_revocation_completion = Some(completion_rx);
    self.refresh_diagnostics();
    self.delivery_notify.notify_waiters();

    info!(
      "consumer revocation requested: topic={}, group_id={}, member_id={}, revoked={:?}",
      self.group_config.topic, self.group_config.group_id, self.group_config.member_id, revoked
    );

    Ok(())
  }

  fn spawn_prefetch_task(&mut self) {
    if self.prefetch_task.is_some() {
      return;
    }

    self.prefetch_shutdown.store(false, Ordering::Release);

    let reader = Arc::clone(&self.reader);
    let shared_state = Arc::clone(&self.shared_state);
    let delivery_notify = Arc::clone(&self.delivery_notify);
    let space_notify = Arc::clone(&self.prefetch_space_notify);
    let shutdown = Arc::clone(&self.prefetch_shutdown);
    let metrics = self.metrics.clone();
    let prefetch_max_bytes = self.prefetch_max_bytes;
    let base_idle_delay_ms = self.prefetch_idle_base_delay_ms;
    let max_idle_delay_ms = self.prefetch_idle_max_delay_ms;

    self.prefetch_task = Some(tokio::spawn(async move {
      run_prefetch_worker(
        reader,
        shared_state,
        delivery_notify,
        space_notify,
        shutdown,
        metrics,
        prefetch_max_bytes,
        base_idle_delay_ms,
        max_idle_delay_ms,
      )
      .await;
    }));
  }

  async fn stop_prefetch_task(&mut self) {
    self.prefetch_shutdown.store(true, Ordering::Release);
    self.delivery_notify.notify_waiters();
    self.prefetch_space_notify.notify_waiters();

    if let Some(handle) = self.prefetch_task.take() {
      let _ = handle.await;
    }
  }

  fn record_heartbeat_failure(&self, started_at: Instant) {
    self.metrics.heartbeat_failures.inc();
    self
      .metrics
      .heartbeat_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());
  }

  fn record_successful_heartbeat(
    &self,
    now_ts_ms: i64,
    report: &HeartbeatReport,
    pending_commits: &HashMap<VirtualPartitionId, u64>,
    started_at: Instant,
  ) {
    self
      .metrics
      .heartbeat_renewed_partitions
      .inc_by(u64::try_from(report.renewed_partitions.len()).unwrap_or(u64::MAX));
    self
      .metrics
      .heartbeat_fenced_partitions
      .inc_by(u64::try_from(report.fenced_partitions.len()).unwrap_or(u64::MAX));

    let committed_offsets = report
      .renewed_partitions
      .iter()
      .filter_map(|partition_id| {
        pending_commits
          .get(partition_id)
          .map(|offset| (*partition_id, *offset))
      })
      .collect::<Vec<_>>();
    self
      .metrics
      .heartbeat_committed_offsets
      .inc_by(u64::try_from(committed_offsets.len()).unwrap_or(u64::MAX));
    self
      .metrics
      .heartbeat_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());

    let mut shared_state = self.shared_state.lock();
    let diagnostics = &mut shared_state.diagnostics;
    for (partition_id, offset) in committed_offsets {
      diagnostics
        .last_committed_offsets
        .insert(partition_id, offset);
    }
    diagnostics.last_successful_heartbeat_at_ms = Some(now_ts_ms);
  }

  async fn heartbeat(
    &mut self,
    now_ts_ms: i64,
    trigger: HeartbeatTrigger,
  ) -> Result<HeartbeatReport> {
    let started_at = Instant::now();
    let pending_commits = self.shared_state.lock().pending_commits.clone();
    self.metrics.heartbeat_calls.inc();
    match trigger {
      HeartbeatTrigger::Scheduled => self.metrics.heartbeat_scheduled_calls.inc(),
      HeartbeatTrigger::Commit => self.metrics.heartbeat_commit_calls.inc(),
    }
    trace!(
      "consumer heartbeat started: topic={}, group_id={}, member_id={}, trigger={}, \
       generation={}, active_partitions={:?}, pending_commits={}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      trigger.as_str(),
      self.coordinator.generation(),
      self.active_assignment,
      pending_commits.len()
    );

    if let Err(error) = self
      .membership_store
      .heartbeat_member(
        &self.group_config.topic,
        &self.group_config.group_id,
        &self.group_config.member_id,
        now_ts_ms,
        consumer_lease_duration_ms(&self.group_config),
      )
      .await
    {
      debug!(
        "consumer membership heartbeat failed: topic={}, group_id={}, member_id={}, trigger={}, \
         generation={}, elapsed_ms={}, error={error}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        trigger.as_str(),
        self.coordinator.generation(),
        started_at.elapsed().as_millis()
      );
      self.record_heartbeat_failure(started_at);
      return Err(error);
    }

    let report = match self
      .coordinator
      .heartbeat_and_commit(now_ts_ms, &pending_commits)
      .await
    {
      Ok(report) => report,
      Err(error) => {
        debug!(
          "consumer coordinator heartbeat failed: topic={}, group_id={}, member_id={}, \
           trigger={}, generation={}, elapsed_ms={}, error={error}",
          self.group_config.topic,
          self.group_config.group_id,
          self.group_config.member_id,
          trigger.as_str(),
          self.coordinator.generation(),
          started_at.elapsed().as_millis()
        );
        self.record_heartbeat_failure(started_at);
        return Err(error);
      },
    };

    self.record_successful_heartbeat(now_ts_ms, &report, &pending_commits, started_at);

    if !report.fenced_partitions.is_empty() {
      for partition_id in &report.fenced_partitions {
        self.active_assignment.remove(partition_id);
      }
      {
        let mut shared_state = self.shared_state.lock();
        for partition_id in &report.fenced_partitions {
          shared_state.pending_commits.remove(partition_id);
        }
        shared_state
          .active_assignment
          .clone_from(&self.active_assignment);
        self.refresh_diagnostics_locked(&mut shared_state);
      }
      self
        .reader
        .lock()
        .await
        .set_assigned_virtual_partitions(self.active_assignment.iter().copied().collect())?;
      info!(
        "consumer heartbeat fenced partitions: topic={}, group_id={}, member_id={}, fenced={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        report.fenced_partitions
      );
    }

    self.next_heartbeat_at_ms = now_ts_ms + consumer_heartbeat_interval_ms(&self.group_config);
    self.refresh_diagnostics();
    let committed_offsets = report
      .renewed_partitions
      .iter()
      .filter(|partition_id| pending_commits.contains_key(partition_id))
      .count();
    trace!(
      "consumer heartbeat completed: topic={}, group_id={}, member_id={}, trigger={}, \
       generation={}, renewed={:?}, fenced={:?}, committed_offsets={}, elapsed_ms={}, \
       next_heartbeat_at_ms={}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      trigger.as_str(),
      self.coordinator.generation(),
      report.renewed_partitions,
      report.fenced_partitions,
      committed_offsets,
      started_at.elapsed().as_millis(),
      self.next_heartbeat_at_ms
    );
    Ok(report)
  }
}

fn prefetched_batch_bytes(batch: &ConsumerBatch) -> u64 {
  batch.records.iter().fold(0_u64, |acc, record| {
    acc.saturating_add(record.payload.len() as u64)
  })
}

fn update_worker_prefetch_metrics(metrics: &ConsumerIteratorMetrics, buffer: &DeliveryState) {
  metrics
    .prefetch_buffered_batches
    .set(i64::try_from(buffer.batches.len()).unwrap_or(i64::MAX));
  metrics
    .prefetch_buffered_bytes
    .set(i64::try_from(buffer.buffered_bytes).unwrap_or(i64::MAX));
}

async fn run_prefetch_worker(
  reader: Arc<Mutex<ConsumerReaderImpl>>,
  shared_state: Arc<ParkingLotMutex<ConsumerSharedState>>,
  delivery_notify: Arc<Notify>,
  prefetch_space_notify: Arc<Notify>,
  prefetch_shutdown: Arc<AtomicBool>,
  metrics: ConsumerIteratorMetrics,
  prefetch_max_bytes: u64,
  base_idle_delay_ms: u64,
  max_idle_delay_ms: Option<u64>,
) {
  let mut idle_poll_backoff = IdlePollBackoff::new(base_idle_delay_ms, max_idle_delay_ms);
  let mut pending = VecDeque::new();

  loop {
    if prefetch_shutdown.load(Ordering::Acquire) {
      return;
    }

    {
      let pending_remains = {
        let mut shared_state = shared_state.lock();
        let buffer = &mut shared_state.delivery_state;

        while let Some(batch) = pending.front() {
          let batch_bytes = prefetched_batch_bytes(batch);
          let would_cross = buffer
            .buffered_bytes
            .saturating_add(batch_bytes)
            .gt(&prefetch_max_bytes);

          // Soft target: allow one boundary crossing, then pause until space is available.
          if would_cross && !buffer.batches.is_empty() {
            metrics.prefetch_paused_budget.inc();
            break;
          }

          let Some(batch) = pending.pop_front() else {
            break;
          };
          buffer.buffered_bytes = buffer.buffered_bytes.saturating_add(batch_bytes);
          buffer.batches.push_back(batch);
        }

        if !buffer.batches.is_empty() {
          update_worker_prefetch_metrics(&metrics, buffer);
          delivery_notify.notify_waiters();
        }

        !pending.is_empty()
      };

      if pending_remains {
        let _ = tokio::time::timeout(
          std::time::Duration::from_millis(250),
          prefetch_space_notify.notified(),
        )
        .await;
        continue;
      }
    }

    let read_started_at = Instant::now();
    let mut read_attempt: u8 = 0;
    let batches = loop {
      let now_s = now_unix_seconds();
      let read_result = reader.lock().await.read_available(now_s).await;
      match read_result {
        Ok(batches) => break batches,
        Err(read_error) => {
          if read_attempt == 0 {
            read_attempt = 1;
            metrics.retries.inc();
            warn_every!(
              15.seconds(),
              "consumer prefetch read retrying after error: error={read_error}"
            );
            continue;
          }

          metrics.failures.inc();
          warn_every!(
            15.seconds(),
            "consumer prefetch read failed after retry: error={read_error}"
          );
          tokio::time::sleep(std::time::Duration::from_millis(base_idle_delay_ms)).await;
        },
      }
    };

    metrics
      .next_latency_seconds
      .observe(read_started_at.elapsed().as_secs_f64());

    if batches.is_empty() {
      let idle_delay_ms = idle_poll_backoff.next_delay_ms();
      tokio::time::sleep(std::time::Duration::from_millis(idle_delay_ms)).await;
      continue;
    }

    idle_poll_backoff.reset();
    metrics.prefetch_refill_cycles.inc();
    pending.extend(batches);
  }
}

impl ConsumerDriver {
  fn start(&mut self) -> Result<()> {
    ensure!(!self.started, "consumer iterator already started");
    self.started = true;
    self.diagnostics.started.store(true, Ordering::Release);
    self.spawn_prefetch_task();
    self.refresh_diagnostics();
    info!(
      "consumer iterator started: topic={}, group_id={}, member_id={}",
      self.group_config.topic, self.group_config.group_id, self.group_config.member_id
    );
    Ok(())
  }

  async fn run(mut self, mut command_rx: mpsc::UnboundedReceiver<ConsumerDriverCommand>) {
    if let Err(error) = self.start() {
      self.shared_state.lock().terminal_error = Some(error.to_string());
      self.diagnostics.started.store(false, Ordering::Release);
      self.delivery_notify.notify_waiters();
      return;
    }

    loop {
      let revocation_completed = match self.finish_pending_revocation_if_completed().await {
        Ok(completed) => completed,
        Err(error) => {
          self.shared_state.lock().terminal_error = Some(error.to_string());
          let _ = self.shutdown().await;
          self.delivery_notify.notify_waiters();
          return;
        },
      };
      let now_ts_ms = now_unix_millis();

      if revocation_completed && now_ts_ms >= self.next_rebalance_at_ms {
        match self.maybe_rebalance(now_ts_ms).await {
          Ok(()) => {},
          Err(error) => {
            warn_every!(
              15.seconds(),
              "consumer rebalance retrying after error: error={error}"
            );
            self.next_rebalance_at_ms = now_ts_ms.saturating_add(1_000);
            self.refresh_rebalance_diagnostics();
          },
        }
      }

      if now_ts_ms >= self.next_heartbeat_at_ms {
        trace!(
          "consumer scheduled heartbeat due: topic={}, group_id={}, member_id={}, generation={}, \
           now_ts_ms={}, due_at_ms={}, overdue_ms={}, active_partitions={:?}, pending_commits={}",
          self.group_config.topic,
          self.group_config.group_id,
          self.group_config.member_id,
          self.coordinator.generation(),
          now_ts_ms,
          self.next_heartbeat_at_ms,
          now_ts_ms.saturating_sub(self.next_heartbeat_at_ms),
          self.active_assignment,
          self.shared_state.lock().pending_commits.len()
        );
        if let Err(error) = self.heartbeat(now_ts_ms, HeartbeatTrigger::Scheduled).await {
          warn_every!(
            15.seconds(),
            "consumer scheduled heartbeat retrying after error: error={error}"
          );
          self.next_heartbeat_at_ms = now_ts_ms.saturating_add(1_000);
          self.refresh_diagnostics();
        }
      }

      let until_heartbeat_ms = (self.next_heartbeat_at_ms - now_ts_ms).max(0);
      let until_rebalance_ms = (self.next_rebalance_at_ms - now_ts_ms).max(0);
      let wait_ms = if revocation_completed {
        max(1_i64, until_heartbeat_ms.min(until_rebalance_ms)).cast_unsigned()
      } else {
        until_heartbeat_ms.clamp(1, 100).cast_unsigned()
      };
      tokio::select! {
        command = command_rx.recv() => {
          let Some(command) = command else {
            let _ = self.shutdown().await;
            self.delivery_notify.notify_waiters();
            return;
          };
          match command {
            ConsumerDriverCommand::Commit { response } => {
              let started_at = Instant::now();
              let result = self.commit().await;
              self.metrics.commit_latency_seconds.observe(started_at.elapsed().as_secs_f64());
              let _ = response.send(result);
            },
            ConsumerDriverCommand::Seek {
              virtual_partition_id,
              offset,
              response,
            } => {
              let _ = response.send(self.seek(virtual_partition_id, offset).await);
            },
            ConsumerDriverCommand::Shutdown { response } => {
              let _ = response.send(self.shutdown().await);
              self.delivery_notify.notify_waiters();
              return;
            },
          }
        },
        () = self.revocation_notify.notified(), if !revocation_completed => {},
        () = tokio::time::sleep(std::time::Duration::from_millis(wait_ms)) => {},
      }
    }
  }

  async fn commit(&mut self) -> Result<HeartbeatReport> {
    ensure!(
      self.started,
      "consumer iterator must be started before commit"
    );
    debug!(
      "consumer commit requested heartbeat: topic={}, group_id={}, member_id={}, generation={}, \
       active_partitions={:?}, pending_commits={:?}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      self.coordinator.generation(),
      self.active_assignment,
      self.shared_state.lock().pending_commits
    );
    self
      .heartbeat(now_unix_millis(), HeartbeatTrigger::Commit)
      .await
  }

  async fn shutdown(&mut self) -> Result<()> {
    if self.started {
      // Attempt commit and explicit lease release before membership deregistration.
      // Releasing leases proactively shortens rebalance convergence on graceful shutdown.
      let commit_result = self.commit().await;
      let release_result = self.coordinator.release_owned(now_unix_millis()).await;
      let _ = self
        .membership_store
        .deregister_member(
          &self.group_config.topic,
          &self.group_config.group_id,
          &self.group_config.member_id,
        )
        .await;
      self.stop_prefetch_task().await;
      self.started = false;
      self.diagnostics.started.store(false, Ordering::Release);
      self.refresh_diagnostics();
      info!(
        "consumer iterator shutdown: topic={}, group_id={}, member_id={}",
        self.group_config.topic, self.group_config.group_id, self.group_config.member_id
      );

      commit_result?;
      release_result?;
    }
    Ok(())
  }

  #[allow(clippy::needless_pass_by_ref_mut)]
  async fn seek(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()> {
    ensure!(
      self.active_assignment.contains(&virtual_partition_id),
      "cannot seek unassigned virtual partition {virtual_partition_id}"
    );
    self
      .reader
      .lock()
      .await
      .set_cursor(virtual_partition_id, offset);
    trace!(
      "consumer seek: topic={}, partition={}, offset={}",
      self.group_config.topic, virtual_partition_id, offset
    );
    Ok(())
  }
}

#[async_trait]
impl ConsumerIterator for ConsumerIteratorImpl {
  fn start(&mut self) -> Result<()> {
    ensure!(!self.started, "consumer iterator already started");

    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let driver = self
      .driver
      .take()
      .ok_or_else(|| anyhow!("consumer iterator driver is unavailable"))?;

    self.command_tx = Some(command_tx);
    self.started = true;
    self.diagnostics.started.store(true, Ordering::Release);
    self.driver_task = Some(tokio::spawn(async move {
      driver.run(command_rx).await;
    }));
    Ok(())
  }

  async fn next(&mut self) -> Result<NextResult> {
    ensure!(
      self.started,
      "consumer iterator must be started before next"
    );

    loop {
      let notified = self.delivery_notify.notified();
      let (next_result, terminal_error) = {
        let mut shared_state = self.shared_state.lock();
        let ConsumerSharedState {
          active_assignment,
          delivery_state,
          terminal_error,
          ..
        } = &mut *shared_state;
        (
          delivery_state.try_take_next(active_assignment, &self.metrics),
          terminal_error.clone(),
        )
      };
      if let Some(next_result) = next_result {
        self.prefetch_space_notify.notify_waiters();
        return Ok(next_result);
      }

      if let Some(error) = terminal_error {
        return Err(anyhow!("consumer driver stopped: {error}"));
      }

      notified.await;
    }
  }

  fn store_offset(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()> {
    let mut shared_state = self.shared_state.lock();
    ensure!(
      shared_state
        .active_assignment
        .contains(&virtual_partition_id),
      "cannot store cursor for unassigned virtual partition {virtual_partition_id}"
    );
    shared_state
      .pending_commits
      .insert(virtual_partition_id, offset);
    trace!("consumer stored offset: partition={virtual_partition_id}, offset={offset}");
    Ok(())
  }

  async fn commit(&mut self) -> Result<HeartbeatReport> {
    ensure!(
      self.started,
      "consumer iterator must be started before commit"
    );
    let (response_tx, response_rx) = oneshot::channel();
    self
      .command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer iterator command queue is unavailable"))?
      .send(ConsumerDriverCommand::Commit {
        response: response_tx,
      })
      .map_err(|_| anyhow!("consumer driver stopped before commit could be queued"))?;
    response_rx
      .await
      .map_err(|_| anyhow!("consumer driver stopped before commit completed"))?
  }

  async fn shutdown(mut self: Box<Self>) -> Result<()> {
    if !self.started {
      return Ok(());
    }

    let (response_tx, response_rx) = oneshot::channel();
    self
      .command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer iterator command queue is unavailable"))?
      .send(ConsumerDriverCommand::Shutdown {
        response: response_tx,
      })
      .map_err(|_| anyhow!("consumer driver stopped before shutdown could be queued"))?;
    let shutdown_result = response_rx
      .await
      .map_err(|_| anyhow!("consumer driver stopped before shutdown completed"))?;
    if let Some(driver_task) = self.driver_task.take() {
      let _ = driver_task.await;
    }
    self.started = false;
    shutdown_result
  }

  async fn seek(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()> {
    ensure!(
      self.started,
      "consumer iterator must be started before seek"
    );
    let (response_tx, response_rx) = oneshot::channel();
    self
      .command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer iterator command queue is unavailable"))?
      .send(ConsumerDriverCommand::Seek {
        virtual_partition_id,
        offset,
        response: response_tx,
      })
      .map_err(|_| anyhow!("consumer driver stopped before seek could be queued"))?;
    response_rx
      .await
      .map_err(|_| anyhow!("consumer driver stopped before seek completed"))?
  }

  fn diagnostics(&self) -> Option<ConsumerDiagnostics> {
    Some(self.diagnostics.clone())
  }
}
