#[cfg(test)]
#[path = "./iterator_test.rs"]
mod tests;

mod delivery;
mod prefetch;

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
use crate::consumer::ConsumerReaderImpl;
use crate::coordination::{
  ConsumerGroupCoordinator,
  ConsumerGroupCoordinatorImpl,
  HeartbeatReport,
  RebalanceReport,
  RecoveredCursor,
};
use crate::diagnostics::{
  ConsumerCommittedCursorSnapshot,
  ConsumerDiagnostics,
  ConsumerDiagnosticsRuntimeState,
  ConsumerStateSnapshot,
  assignment_plan_snapshot,
  emit_partition_handoff_snapshots,
};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use bd_log::warn_every;
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use bd_server_stats::stats::Scope;
use bd_time::{OffsetDateTimeExt, SystemTimeProvider, TimeProvider};
use blob_stream_blob_store::BlobStore;
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentPlan,
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  MetadataStore,
};
use blob_stream_types::{
  CommittedCursor,
  CommittedSourceCheckpoint,
  Record,
  VirtualPartitionId,
  format_unix_timestamp_ms,
};
use delivery::{
  DeliveredSourceRange,
  DeliveryState,
  prefetched_batch_bytes,
  update_total_prefetch_bytes,
  update_worker_prefetch_metrics,
};
use log::{debug, info, trace};
use parking_lot::Mutex;
use prefetch::{ConsumerReaderCommand, PrefetchWorker, record_reader_diagnostics};
use prometheus::{Histogram, IntCounter, IntGauge};
use serde::Serialize;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use time::ext::NumericalDuration;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{Span, field};

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
  rebalances_total: IntCounter,
  rebalance_failures_total: IntCounter,
  assignment_plans_applied_total: IntCounter,
  assignment_plan_rejections_total: IntCounter,
  assignment_applications_total: IntCounter,
  desired_partitions: IntGauge,
  owned_partitions: IntGauge,
  active_partitions: IntGauge,
  prefetch_buffered_batches: IntGauge,
  prefetch_buffered_bytes: IntGauge,
  prefetch_pending_batches: IntGauge,
  prefetch_pending_bytes: IntGauge,
  prefetch_total_bytes: IntGauge,
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
      rebalances_total: scope.counter("rebalances_total"),
      rebalance_failures_total: scope.counter("rebalance_failures_total"),
      assignment_plans_applied_total: scope.counter("assignment_plans_applied_total"),
      assignment_plan_rejections_total: scope.counter("assignment_plan_rejections_total"),
      assignment_applications_total: scope.counter("assignment_applications_total"),
      desired_partitions: scope.gauge("desired_partitions"),
      owned_partitions: scope.gauge("owned_partitions"),
      active_partitions: scope.gauge("active_partitions"),
      prefetch_buffered_batches: scope.gauge("prefetch_buffered_batches"),
      prefetch_buffered_bytes: scope.gauge("prefetch_buffered_bytes"),
      prefetch_pending_batches: scope.gauge("prefetch_pending_batches"),
      prefetch_pending_bytes: scope.gauge("prefetch_pending_bytes"),
      prefetch_total_bytes: scope.gauge("prefetch_total_bytes"),
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
// PendingCommit
//

#[derive(Clone, Debug)]
pub struct PendingCommit {
  pub offset: u64,
  source_checkpoint: CommittedSourceCheckpoint,
}

//
// ActivePartitionState
//

/// Shared iterator state for one commit-eligible virtual partition.
#[derive(Default)]
pub struct ActivePartitionState {
  pub(crate) pending_commit: Option<PendingCommit>,
  pub(crate) delivered_source_ranges: Vec<DeliveredSourceRange>,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Delivery state shared by the consumer facade and driver.
pub enum ConsumerDeliveryState {
  /// No record or revocation event is currently available to a caller.
  Idle,
  /// A record or revocation event is available to the next caller poll.
  Pending,
}

//
// ConsumerSharedState
//
/// Synchronous state jointly accessed by the driver, prefetch task, and public iterator facade.
#[derive(Default)]
pub struct ConsumerSharedState {
  pub(crate) active_partitions: HashMap<VirtualPartitionId, ActivePartitionState>,
  pub delivery_state: DeliveryState,
  terminal_error: Option<String>,
  pub diagnostics: ConsumerDiagnosticsRuntimeState,
}

//
// ConsumerIterator
//

/// Callback invoked when virtual partitions become active for this iterator.
pub type AssignmentCallback = Arc<dyn Fn(&[VirtualPartitionId]) + Send + Sync>;

//
// ConsumerLifecycleHooks
//

/// Test-oriented lifecycle observation points for consumer coordination transitions.
#[async_trait]
pub trait ConsumerLifecycleHooks: Send + Sync {
  /// Runs after a revocation becomes visible to the iterator caller.
  async fn revocation_emitted(
    &self,
    _member_id: &str,
    _generation: u64,
    _partitions: &[VirtualPartitionId],
  ) {
  }

  /// Runs after a prefetched batch becomes visible to the iterator caller.
  async fn prefetch_batch_buffered(
    &self,
    _member_id: &str,
    _virtual_partition_id: VirtualPartitionId,
  ) {
  }

  /// Runs immediately before the driver begins a consumer-group rebalance.
  async fn before_rebalance(&self, _member_id: &str, _generation: u64) {}

  /// Runs after a rebalance applies a new active assignment.
  async fn rebalance_applied(
    &self,
    _member_id: &str,
    _generation: u64,
    _partitions: &[VirtualPartitionId],
  ) {
  }

  /// Runs immediately before an explicit commit starts its heartbeat.
  async fn before_commit(&self, _member_id: &str, _generation: u64) {}

  /// Runs after shutdown's final commit attempt completes.
  async fn shutdown_commit_finished(&self, _member_id: &str, _generation: u64) {}

  /// Runs immediately before shutdown releases owned partition leases.
  async fn before_release_owned(&self, _member_id: &str, _generation: u64) {}

  /// Runs immediately before shutdown deregisters consumer membership.
  async fn before_deregister_member(&self, _member_id: &str, _generation: u64) {}
}

//
// NoopConsumerLifecycleHooks
//

/// Production lifecycle hooks that preserve normal consumer behavior.
#[derive(Default)]
pub struct NoopConsumerLifecycleHooks;

#[async_trait]
impl ConsumerLifecycleHooks for NoopConsumerLifecycleHooks {}

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
  /// Reposition a partition cursor after discarding buffered records read under the prior cursor.
  ///
  /// TODO: Accept a source checkpoint so historical seeks can target the exact source window.
  /// TODO: Slice a batch at the requested offset rather than redelivering its earlier records.
  async fn seek(&mut self, virtual_partition_id: VirtualPartitionId, offset: u64) -> Result<()>;
  /// Register a callback for active partition assignments.
  ///
  /// The callback replays the current assignment when nonempty, then receives only newly active
  /// partitions after each successful assignment change.
  fn set_assignment_callback(&mut self, callback: AssignmentCallback) {
    drop(callback);
  }
  /// Returns a handle for observing this consumer's local runtime state.
  fn diagnostics(&self) -> Option<ConsumerDiagnostics> {
    None
  }
}

//
// ConsumerDriver
//

/// Long-lived owner of consumer coordination and prefetch lifecycle state.
struct ConsumerDriver {
  group_config: ConsumerGroupConfig,
  reader: Option<ConsumerReaderImpl>,
  reader_command_tx: Option<mpsc::UnboundedSender<ConsumerReaderCommand>>,
  coordinator: Box<dyn ConsumerGroupCoordinator>,
  membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  coordination_source: Arc<dyn ConsumerCoordinationSource>,
  last_coordination_snapshot: Option<CoordinationSnapshot>,
  metrics: ConsumerIteratorMetrics,
  started: bool,
  shared_state: Arc<Mutex<ConsumerSharedState>>,
  delivery_notify: Arc<Notify>,
  prefetch_space_notify: Arc<Notify>,
  reader_command_notify: Arc<Notify>,
  prefetch_shutdown: Arc<AtomicBool>,
  prefetch_task: Option<JoinHandle<()>>,
  prefetch_idle_base_delay_ms: u64,
  prefetch_idle_max_delay_ms: Option<u64>,
  active_assignment: HashSet<VirtualPartitionId>,
  assignment_callback: Arc<Mutex<Option<AssignmentCallback>>>,
  pending_assignment: Option<Vec<VirtualPartitionId>>,
  pending_revocation_completion: Option<oneshot::Receiver<()>>,
  pending_revocation_partitions: Option<Vec<VirtualPartitionId>>,
  pending_revocation_snapshot: Option<ConsumerStateSnapshot>,
  pending_revocation_span: Option<Span>,
  revocation_notify: Arc<Notify>,
  lifecycle_hooks: Option<Arc<dyn ConsumerLifecycleHooks>>,
  time_provider: Arc<dyn TimeProvider>,
  membership_lease_expires_at_ms: i64,
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
  AbortForTest {
    response: oneshot::Sender<()>,
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
  shared_state: Arc<Mutex<ConsumerSharedState>>,
  delivery_notify: Arc<Notify>,
  prefetch_space_notify: Arc<Notify>,
  metrics: ConsumerIteratorMetrics,
  assignment_callback: Arc<Mutex<Option<AssignmentCallback>>>,
  command_tx: Option<mpsc::UnboundedSender<ConsumerDriverCommand>>,
  driver: Option<ConsumerDriver>,
  driver_task: Option<JoinHandle<()>>,
  #[cfg(test)]
  next_after_delivery_state_check_hook: Option<NextAfterDeliveryStateCheckHook>,
}

//
// ConsumerIteratorBuilder
//

/// Configures a consumer iterator and optional test-only runtime dependencies.
pub struct ConsumerIteratorBuilder<'a> {
  runtime: &'a ConsumerRuntimeConfig,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ConsumerGroupLeaseStore>,
  membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  coordination_source: Arc<dyn ConsumerCoordinationSource>,
  metrics_scope: Scope,
  retention_days: u32,
  maximum_metadata_publication_lag_ms: u64,
  feature_flags: Option<FeatureFlagsWatch>,
  time_provider: Arc<dyn TimeProvider>,
  lifecycle_hooks: Option<Arc<dyn ConsumerLifecycleHooks>>,
}

impl<'a> ConsumerIteratorBuilder<'a> {
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    runtime: &'a ConsumerRuntimeConfig,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
    membership_store: Arc<dyn ConsumerGroupMembershipStore>,
    coordination_source: Arc<dyn ConsumerCoordinationSource>,
    metrics_scope: Scope,
    retention_days: u32,
    maximum_metadata_publication_lag_ms: u64,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Self {
    Self {
      runtime,
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      coordination_source,
      metrics_scope,
      retention_days,
      maximum_metadata_publication_lag_ms,
      feature_flags,
      time_provider: Arc::new(SystemTimeProvider),
      lifecycle_hooks: None,
    }
  }

  #[must_use]
  pub fn time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
    self.time_provider = time_provider;
    self
  }

  #[must_use]
  pub fn lifecycle_hooks(mut self, lifecycle_hooks: Arc<dyn ConsumerLifecycleHooks>) -> Self {
    self.lifecycle_hooks = Some(lifecycle_hooks);
    self
  }
}

#[cfg(test)]
struct NextAfterDeliveryStateCheckHook {
  state_checked: oneshot::Sender<()>,
  release: oneshot::Receiver<()>,
}

impl ConsumerIteratorImpl {
  /// Stops local consumer work without committing offsets or releasing group state.
  ///
  /// This intentionally models a process crash for integration tests. Production callers must
  /// use [`ConsumerIterator::shutdown`] to release ownership gracefully.
  pub async fn abort_for_test(&mut self) -> Result<()> {
    ensure!(
      self.started,
      "consumer iterator must be started before aborting"
    );

    let (response_tx, response_rx) = oneshot::channel();
    self
      .command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer iterator command queue is unavailable"))?
      .send(ConsumerDriverCommand::AbortForTest {
        response: response_tx,
      })
      .map_err(|_| anyhow!("consumer driver stopped before test abort could be queued"))?;
    response_rx
      .await
      .map_err(|_| anyhow!("consumer driver stopped before test abort completed"))?;
    if let Some(driver_task) = self.driver_task.take() {
      let _ = driver_task.await;
    }
    self.started = false;
    self.command_tx = None;
    Ok(())
  }

  /// Build an iterator with recovery retention and an explicit metadata publication bound.
  #[allow(clippy::too_many_arguments)]
  pub async fn from_config(
    runtime: &ConsumerRuntimeConfig,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
    membership_store: Arc<dyn ConsumerGroupMembershipStore>,
    coordination_source: Arc<dyn ConsumerCoordinationSource>,
    metrics_scope: Scope,
    retention_days: u32,
    maximum_metadata_publication_lag_ms: u64,
    feature_flags: Option<FeatureFlagsWatch>,
  ) -> Result<Self> {
    ConsumerIteratorBuilder::new(
      runtime,
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      coordination_source,
      metrics_scope,
      retention_days,
      maximum_metadata_publication_lag_ms,
      feature_flags,
    )
    .build()
    .await
  }
}

impl ConsumerIteratorBuilder<'_> {
  /// Build an iterator from this configuration.
  pub async fn build(self) -> Result<ConsumerIteratorImpl> {
    let Self {
      runtime,
      blob_store,
      metadata_store,
      lease_store,
      membership_store,
      coordination_source,
      metrics_scope,
      retention_days,
      maximum_metadata_publication_lag_ms,
      feature_flags,
      time_provider,
      lifecycle_hooks,
    } = self;
    validate_runtime_config(runtime)?;
    ensure!(
      retention_days > 0,
      "consumer retention recovery requires topic retention_days greater than zero"
    );
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
    let assignment_callback = Arc::new(Mutex::new(None));
    let shared_state = Arc::new(Mutex::new(ConsumerSharedState::default()));
    let reader = ConsumerReaderImpl::new(
      read_config,
      Vec::new(),
      HashMap::new(),
      blob_store,
      metadata_store,
      &metrics_scope.scope("consumer"),
      retention_days,
      maximum_metadata_publication_lag_ms,
      feature_flags,
    )?;
    let coordinator = ConsumerGroupCoordinatorImpl::new(
      group_config.clone(),
      Arc::clone(&lease_store),
      Arc::clone(&membership_store),
    )?;
    let now_ts_ms = time_provider.now().unix_timestamp_ms();
    let membership_lease_duration_ms = consumer_lease_duration_ms(&group_config);
    let delivery_notify = Arc::new(Notify::new());
    let prefetch_space_notify = Arc::new(Notify::new());
    let reader_command_notify = Arc::new(Notify::new());
    let revocation_notify = Arc::new(Notify::new());
    let prefetch_shutdown = Arc::new(AtomicBool::new(false));
    let diagnostics = ConsumerDiagnostics::new(
      group_config.clone(),
      Arc::clone(&shared_state),
      prefetch_max_bytes,
      Arc::clone(&lease_store),
    );
    {
      let mut state = shared_state.lock();
      state.diagnostics.next_heartbeat_at_ms = now_ts_ms;
      state.diagnostics.next_rebalance_at_ms = now_ts_ms;
    }
    let mut driver = ConsumerDriver {
      group_config,
      reader: Some(reader),
      reader_command_tx: None,
      coordinator: Box::new(coordinator),
      membership_store,
      coordination_source,
      last_coordination_snapshot: None,
      metrics: ConsumerIteratorMetrics::new(&metrics_scope.scope("consumer")),
      started: false,
      shared_state: Arc::clone(&shared_state),
      delivery_notify: Arc::clone(&delivery_notify),
      prefetch_space_notify: Arc::clone(&prefetch_space_notify),
      reader_command_notify: Arc::clone(&reader_command_notify),
      prefetch_shutdown,
      prefetch_task: None,
      prefetch_idle_base_delay_ms: idle_poll_delay_ms,
      prefetch_idle_max_delay_ms: max_idle_poll_delay_ms,
      active_assignment,
      assignment_callback: Arc::clone(&assignment_callback),
      pending_assignment: None,
      pending_revocation_completion: None,
      pending_revocation_partitions: None,
      pending_revocation_snapshot: None,
      pending_revocation_span: None,
      revocation_notify,
      lifecycle_hooks,
      time_provider,
      membership_lease_expires_at_ms: now_ts_ms.saturating_add(membership_lease_duration_ms),
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
    driver.metrics.rebalances_total.inc();
    let report = driver
      .coordinator
      .rebalance(snapshot.members, snapshot.virtual_partitions, now_ts_ms)
      .await;
    let report = match report {
      Ok(report) => report,
      Err(error) => {
        driver.metrics.rebalance_failures_total.inc();
        return Err(error);
      },
    };
    driver.record_rebalance_metrics(&report);
    let owned = driver.apply_rebalance_report(report)?;
    info!(
      "consumer iterator bootstrapped: topic={}, group_id={}, member_id={}, owned={}",
      driver.group_config.topic,
      driver.group_config.group_id,
      driver.group_config.member_id,
      owned.len()
    );

    Ok(ConsumerIteratorImpl {
      started: false,
      diagnostics: driver.diagnostics.clone(),
      shared_state,
      delivery_notify,
      prefetch_space_notify,
      metrics: driver.metrics.clone(),
      assignment_callback,
      command_tx: None,
      driver: Some(driver),
      driver_task: None,
      #[cfg(test)]
      next_after_delivery_state_check_hook: None,
    })
  }
}

impl ConsumerIteratorImpl {
  #[cfg(test)]
  fn set_next_after_delivery_state_check_hook(
    &mut self,
    state_checked: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
  ) {
    self.next_after_delivery_state_check_hook = Some(NextAfterDeliveryStateCheckHook {
      state_checked,
      release,
    });
  }
}

impl ConsumerDriver {
  fn now_unix_millis(&self) -> i64 {
    self.time_provider.now().unix_timestamp_ms()
  }

  fn now_unix_seconds(&self) -> i64 {
    self.time_provider.now().unix_timestamp()
  }

  fn record_accepted_assignment_plan(&self, plan: Option<ConsumerGroupAssignmentPlan>) {
    {
      let mut shared_state = self.shared_state.lock();
      if let Some(plan) = plan {
        shared_state.diagnostics.assignment_plan = Some(assignment_plan_snapshot(plan));
      }
    }
  }

  fn record_rebalance_metrics(&self, report: &RebalanceReport) {
    if report.assignment_plan_applied {
      self.metrics.assignment_plans_applied_total.inc();
    }
    if report.rejected_assignment_plan_version.is_some() {
      self.metrics.assignment_plan_rejections_total.inc();
    }
    self
      .metrics
      .desired_partitions
      .set(i64::try_from(report.desired_partitions).unwrap_or(i64::MAX));
    self
      .metrics
      .owned_partitions
      .set(i64::try_from(report.owned_partitions.len()).unwrap_or(i64::MAX));
  }

  fn apply_rebalance_report(&mut self, report: RebalanceReport) -> Result<Vec<VirtualPartitionId>> {
    let RebalanceReport {
      owned_partitions,
      recovered_cursors,
      accepted_assignment_plan,
      ..
    } = report;
    self.record_accepted_assignment_plan(accepted_assignment_plan);
    self.hydrate_cursors(recovered_cursors, self.now_unix_seconds())?;

    self.apply_assignment(&owned_partitions)?;
    Ok(owned_partitions)
  }

  fn hydrate_cursors(
    &mut self,
    recovered_cursors: HashMap<VirtualPartitionId, RecoveredCursor>,
    now_unix_seconds: i64,
  ) -> Result<()> {
    if recovered_cursors.is_empty() {
      return Ok(());
    }

    {
      let mut shared_state = self.shared_state.lock();
      for (partition_id, recovered_cursor) in &recovered_cursors {
        shared_state.diagnostics.last_committed_cursors.insert(
          *partition_id,
          ConsumerCommittedCursorSnapshot {
            offset: recovered_cursor.committed_cursor.seq_end,
            source_checkpoint: recovered_cursor.committed_cursor.source_checkpoint.clone(),
            committed_at_ms: recovered_cursor.committed_ts_ms,
          },
        );
      }
    }

    if let Some(reader) = &mut self.reader {
      for (partition_id, recovered_cursor) in recovered_cursors {
        reader.hydrate_cursor_with_source(
          partition_id,
          &recovered_cursor.committed_cursor,
          recovered_cursor.committed_ts_ms,
          now_unix_seconds,
        );
      }
      record_reader_diagnostics(reader, &self.shared_state);
      return Ok(());
    }

    self
      .reader_command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer reader is unavailable"))?
      .send(ConsumerReaderCommand::HydrateCursors {
        recovered_cursors,
        now_unix_seconds,
      })
      .map_err(|_| anyhow!("consumer reader worker stopped"))?;
    self.reader_command_notify.notify_one();
    Ok(())
  }

  fn set_reader_assignment(
    &mut self,
    assignment: Vec<VirtualPartitionId>,
    now_unix_seconds: i64,
    handoff_phase: Option<&'static str>,
    release_delivery_fence: bool,
  ) -> Result<()> {
    if let Some(reader) = &mut self.reader {
      reader.set_assigned_virtual_partitions(&assignment, now_unix_seconds)?;
      record_reader_diagnostics(reader, &self.shared_state);
      if release_delivery_fence {
        self
          .shared_state
          .lock()
          .delivery_state
          .revocation_in_progress = false;
        self.delivery_notify.notify_waiters();
      }
      return Ok(());
    }

    self
      .reader_command_tx
      .as_ref()
      .ok_or_else(|| anyhow!("consumer reader is unavailable"))?
      .send(ConsumerReaderCommand::SetAssignment {
        assignment,
        now_unix_seconds,
        handoff_phase,
        release_delivery_fence,
      })
      .map_err(|_| anyhow!("consumer reader worker stopped"))?;
    self.reader_command_notify.notify_one();
    Ok(())
  }

  fn seek_reader(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    offset: u64,
    now_unix_seconds: i64,
    response: oneshot::Sender<Result<()>>,
  ) {
    if let Some(reader) = &mut self.reader {
      reader.seek(virtual_partition_id, offset, now_unix_seconds);
      record_reader_diagnostics(reader, &self.shared_state);
      let _ = response.send(Ok(()));
      return;
    }

    let Some(reader_command_tx) = self.reader_command_tx.as_ref() else {
      let _ = response.send(Err(anyhow!("consumer reader is unavailable")));
      return;
    };
    let command = ConsumerReaderCommand::Seek {
      virtual_partition_id,
      offset,
      now_unix_seconds,
      response,
    };
    if let Err(error) = reader_command_tx.send(command) {
      let ConsumerReaderCommand::Seek { response, .. } = error.0 else {
        unreachable!("only seek commands are sent through this path");
      };
      let _ = response.send(Err(anyhow!("consumer reader worker stopped")));
    } else {
      self.reader_command_notify.notify_one();
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

  fn apply_assignment(&mut self, assignment: &[VirtualPartitionId]) -> Result<()> {
    let active_assignment = assignment.iter().copied().collect();
    self.apply_assignment_with_active_set(assignment, active_assignment, false)
  }

  fn apply_assignment_after_revocation(&mut self, assignment: &[VirtualPartitionId]) -> Result<()> {
    let active_assignment = assignment.iter().copied().collect();
    self.apply_assignment_with_active_set(assignment, active_assignment, true)
  }

  fn apply_assignment_with_active_set(
    &mut self,
    assignment: &[VirtualPartitionId],
    active_assignment: HashSet<VirtualPartitionId>,
    release_delivery_fence: bool,
  ) -> Result<()> {
    let assignment_changed = self.active_assignment != active_assignment;
    let handoff_phase = assignment_changed.then_some(
      if self.active_assignment.is_empty() {
        "startup_assigned"
      } else {
        "rebalance_assigned"
      },
    );
    let reader_owned_by_driver = self.reader.is_some();
    let mut newly_assigned = active_assignment
      .difference(&self.active_assignment)
      .copied()
      .collect::<Vec<_>>();
    newly_assigned.sort_unstable();
    self.set_reader_assignment(
      assignment.to_owned(),
      self.now_unix_seconds(),
      if reader_owned_by_driver {
        None
      } else {
        handoff_phase
      },
      release_delivery_fence,
    )?;
    self.active_assignment = active_assignment;
    self
      .metrics
      .active_partitions
      .set(i64::try_from(self.active_assignment.len()).unwrap_or(i64::MAX));
    {
      let mut shared_state = self.shared_state.lock();
      shared_state
        .active_partitions
        .retain(|partition_id, _| self.active_assignment.contains(partition_id));
      for partition_id in &self.active_assignment {
        shared_state
          .active_partitions
          .entry(*partition_id)
          .or_default();
      }
      self.refresh_diagnostics_locked(&mut shared_state);
    }
    if reader_owned_by_driver && let Some(handoff_phase) = handoff_phase {
      let assignment_span = bd_log::otel_info_span!(
        "blob_stream.consumer.assignment",
        otel.kind = "internal",
        consumer.topic = %self.group_config.topic,
        consumer.group_id = %self.group_config.group_id,
        consumer.member_id = %self.group_config.member_id,
        consumer.generation = self.coordinator.generation(),
        assignment.phase = handoff_phase,
        assignment.partition_count = assignment.len(),
        otel.status_code = field::Empty,
      );
      emit_partition_handoff_snapshots(
        &self.diagnostics.state_snapshot(),
        assignment,
        handoff_phase,
        "assigned",
        "not_applicable",
        &assignment_span,
      );
      assignment_span.record("otel.status_code", "OK");
    }
    if assignment_changed {
      self.metrics.assignment_applications_total.inc();
      info!(
        "consumer assignment active: topic={}, group_id={}, member_id={}, partitions={:?}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        assignment
      );
    }
    if !newly_assigned.is_empty()
      && let Some(callback) = self.assignment_callback.lock().clone()
    {
      callback(&newly_assigned);
    }
    Ok(())
  }

  async fn fence_active_partitions_after_membership_failure(
    &mut self,
    now_ts_ms: i64,
  ) -> Result<()> {
    if self.pending_revocation_completion.is_some() {
      return Ok(());
    }

    let mut fenced = self.active_assignment.iter().copied().collect::<Vec<_>>();
    fenced.sort_unstable();
    if fenced.is_empty() {
      return Ok(());
    }

    let fenced_set = fenced.iter().copied().collect::<HashSet<_>>();
    self.metrics.revocations.inc();
    self.metrics.active_partitions.set(0);
    let (completion_tx, completion_rx) = oneshot::channel();
    {
      let mut shared_state = self.shared_state.lock();
      let pending_bytes = shared_state.diagnostics.prefetch_pending_bytes;
      shared_state.delivery_state.drop_partitions(&fenced_set);
      shared_state.delivery_state.pending_revocation =
        Some(NextResult::Revoked(Box::new(RevokedPartitionsImpl {
          revoked: fenced.clone(),
          completion_tx: Some(completion_tx),
          completion_notify: Arc::clone(&self.revocation_notify),
        })));
      shared_state.delivery_state.revocation_in_progress = true;
      update_worker_prefetch_metrics(&self.metrics, &shared_state.delivery_state);
      update_total_prefetch_bytes(&self.metrics, &shared_state.delivery_state, pending_bytes);
      self.refresh_diagnostics_locked(&mut shared_state);
    }
    self.set_reader_assignment(Vec::new(), now_ts_ms / 1_000, None, false)?;
    self.prefetch_space_notify.notify_waiters();
    self.pending_assignment = Some(Vec::new());
    self.pending_revocation_completion = Some(completion_rx);
    self.pending_revocation_partitions = Some(fenced.clone());
    self.refresh_diagnostics();
    self.delivery_notify.notify_waiters();
    if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
      lifecycle_hooks
        .revocation_emitted(
          &self.group_config.member_id,
          self.coordinator.generation(),
          &fenced,
        )
        .await;
    }
    info!(
      "consumer active partitions fenced after membership heartbeat failure: topic={}, \
       group_id={}, member_id={}, generation={}, fenced={fenced:?}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      self.coordinator.generation(),
    );
    Ok(())
  }

  fn refresh_diagnostics(&self) {
    let mut shared_state = self.shared_state.lock();
    self.refresh_diagnostics_locked(&mut shared_state);
  }

  fn refresh_diagnostics_locked(&self, shared_state: &mut ConsumerSharedState) {
    let diagnostics = &mut shared_state.diagnostics;
    diagnostics.accepted_assignment_plan_version = self.coordinator.generation();
    diagnostics.owned_partitions = self.coordinator.owned_partitions();
    diagnostics.active_assignment = self.active_assignment.iter().copied().collect();
    diagnostics
      .pending_assignment
      .clone_from(&self.pending_assignment);
    diagnostics.pending_revocation = self.pending_revocation_completion.is_some();
    diagnostics.next_heartbeat_at_ms = self.next_heartbeat_at_ms;
    diagnostics.next_rebalance_at_ms = self.next_rebalance_at_ms;
    diagnostics.started = self.started;
    diagnostics.prefetch_worker_running = self.prefetch_task.is_some();
  }

  fn refresh_rebalance_diagnostics(&self) {
    let mut shared_state = self.shared_state.lock();
    let diagnostics = &mut shared_state.diagnostics;
    diagnostics.accepted_assignment_plan_version = self.coordinator.generation();
    diagnostics.next_rebalance_at_ms = self.next_rebalance_at_ms;
    diagnostics.started = self.started;
    diagnostics.prefetch_worker_running = self.prefetch_task.is_some();
  }

  async fn finish_pending_revocation_if_completed(&mut self) -> Result<bool> {
    let Some(recv) = self.pending_revocation_completion.as_mut() else {
      return Ok(true);
    };

    match recv.try_recv() {
      Ok(()) | Err(oneshot::error::TryRecvError::Closed) => {
        let revoked = self
          .pending_revocation_partitions
          .take()
          .unwrap_or_default();
        let handoff_snapshot = self
          .pending_revocation_snapshot
          .take()
          .unwrap_or_else(|| self.diagnostics.state_snapshot());
        let handoff_span = self
          .pending_revocation_span
          .take()
          .unwrap_or_else(Span::none);
        let release_result = self
          .coordinator
          .release_partitions(&revoked, self.now_unix_millis())
          .await;
        match &release_result {
          Ok(released_partitions) => {
            let released_partitions = released_partitions.iter().copied().collect::<HashSet<_>>();
            let (released, not_released): (Vec<_>, Vec<_>) = revoked
              .iter()
              .copied()
              .partition(|partition_id| released_partitions.contains(partition_id));
            if !released.is_empty() {
              emit_partition_handoff_snapshots(
                &handoff_snapshot,
                &released,
                "revocation_release_result",
                "released",
                "not_attempted",
                &handoff_span,
              );
            }
            if !not_released.is_empty() {
              emit_partition_handoff_snapshots(
                &handoff_snapshot,
                &not_released,
                "revocation_release_result",
                "not_released",
                "not_attempted",
                &handoff_span,
              );
            }
          },
          Err(_) => emit_partition_handoff_snapshots(
            &handoff_snapshot,
            &revoked,
            "revocation_release_result",
            "failed",
            "not_attempted",
            &handoff_span,
          ),
        }
        handoff_span.record(
          "handoff.lease_release_outcome",
          if release_result.is_ok() {
            "succeeded"
          } else {
            "failed"
          },
        );
        if let Err(error) = release_result {
          handoff_span.record("handoff.assignment_outcome", "not_attempted");
          handoff_span.record("error.message", format!("lease release: {error:#}"));
          handoff_span.record("otel.status_code", "ERROR");
          return Err(error);
        }
        let assignment = self.pending_assignment.take().unwrap_or_default();
        self.pending_revocation_completion = None;
        match self.apply_assignment_after_revocation(&assignment) {
          Ok(()) => {
            if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
              lifecycle_hooks
                .rebalance_applied(
                  &self.group_config.member_id,
                  self.coordinator.generation(),
                  &assignment,
                )
                .await;
            }
            handoff_span.record("handoff.assignment_outcome", "succeeded");
            handoff_span.record("otel.status_code", "OK");
            Ok(true)
          },
          Err(error) => {
            handoff_span.record("handoff.assignment_outcome", "failed");
            handoff_span.record("error.message", format!("assignment: {error:#}"));
            handoff_span.record("otel.status_code", "ERROR");
            Err(error)
          },
        }
      },
      Err(oneshot::error::TryRecvError::Empty) => Ok(false),
    }
  }

  async fn maybe_rebalance(&mut self, now_ts_ms: i64) -> Result<()> {
    if now_ts_ms < self.next_rebalance_at_ms {
      return Ok(());
    }

    if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
      lifecycle_hooks
        .before_rebalance(&self.group_config.member_id, self.coordinator.generation())
        .await;
    }
    let snapshot = self.coordination_source.snapshot().await?;
    self.record_coordination_snapshot(&snapshot);
    self.metrics.rebalances_total.inc();
    let report = match self
      .coordinator
      .rebalance(snapshot.members, snapshot.virtual_partitions, now_ts_ms)
      .await
    {
      Ok(report) => report,
      Err(error) => {
        self.metrics.rebalance_failures_total.inc();
        return Err(error);
      },
    };
    self.record_rebalance_metrics(&report);
    let RebalanceReport {
      owned_partitions: next_assignment,
      recovered_cursors,
      accepted_assignment_plan,
      ..
    } = report;
    self.record_accepted_assignment_plan(accepted_assignment_plan);

    self.next_rebalance_at_ms = now_ts_ms + consumer_rebalance_interval_ms(&self.group_config);

    let next_assignment_set = next_assignment.iter().copied().collect::<HashSet<_>>();
    let assignment_changed = self.active_assignment != next_assignment_set;
    self.hydrate_cursors(recovered_cursors, now_ts_ms / 1_000)?;

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
      self.apply_assignment_with_active_set(&next_assignment, next_assignment_set, false)?;
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .rebalance_applied(
            &self.group_config.member_id,
            self.coordinator.generation(),
            &next_assignment,
          )
          .await;
      }
      return Ok(());
    }

    self.metrics.revocations.inc();
    let revoked_set = revoked.iter().copied().collect::<HashSet<_>>();
    let (completion_tx, completion_rx) = oneshot::channel();
    {
      let mut shared_state = self.shared_state.lock();
      let pending_bytes = shared_state.diagnostics.prefetch_pending_bytes;
      let delivery_state = &mut shared_state.delivery_state;
      delivery_state.drop_partitions(&revoked_set);
      delivery_state.pending_revocation =
        Some(NextResult::Revoked(Box::new(RevokedPartitionsImpl {
          revoked: revoked.clone(),
          completion_tx: Some(completion_tx),
          completion_notify: Arc::clone(&self.revocation_notify),
        })));
      delivery_state.revocation_in_progress = true;
      update_worker_prefetch_metrics(&self.metrics, delivery_state);
      update_total_prefetch_bytes(&self.metrics, delivery_state, pending_bytes);
    }
    self.prefetch_space_notify.notify_waiters();

    self.pending_assignment = Some(next_assignment);
    self.pending_revocation_completion = Some(completion_rx);
    self.pending_revocation_partitions = Some(revoked.clone());
    self.refresh_diagnostics();
    let handoff_snapshot = self.diagnostics.state_snapshot();
    let handoff_span = bd_log::otel_info_span!(
      "blob_stream.consumer.revocation_handoff",
      otel.kind = "internal",
      consumer.topic = %self.group_config.topic,
      consumer.group_id = %self.group_config.group_id,
      consumer.member_id = %self.group_config.member_id,
      consumer.generation = self.coordinator.generation(),
      handoff.revoked_partition_count = revoked.len(),
      handoff.lease_release_outcome = field::Empty,
      handoff.assignment_outcome = field::Empty,
      error.message = field::Empty,
      otel.status_code = field::Empty,
    );
    emit_partition_handoff_snapshots(
      &handoff_snapshot,
      &revoked,
      "revocation_pre_release",
      "awaiting_application_ack",
      "not_attempted",
      &handoff_span,
    );
    self.pending_revocation_snapshot = Some(handoff_snapshot);
    self.pending_revocation_span = Some(handoff_span);
    self.delivery_notify.notify_waiters();
    if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
      lifecycle_hooks
        .revocation_emitted(
          &self.group_config.member_id,
          self.coordinator.generation(),
          &revoked,
        )
        .await;
    }

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

    let reader = self
      .reader
      .take()
      .expect("consumer reader must be available before prefetch starts");
    let (reader_command_tx, reader_command_rx) = mpsc::unbounded_channel();
    self.reader_command_tx = Some(reader_command_tx);
    let shared_state = Arc::clone(&self.shared_state);
    let delivery_notify = Arc::clone(&self.delivery_notify);
    let space_notify = Arc::clone(&self.prefetch_space_notify);
    let reader_command_notify = Arc::clone(&self.reader_command_notify);
    let shutdown = Arc::clone(&self.prefetch_shutdown);
    let metrics = self.metrics.clone();
    let diagnostics = self.diagnostics.clone();
    let base_idle_delay_ms = self.prefetch_idle_base_delay_ms;
    let max_idle_delay_ms = self.prefetch_idle_max_delay_ms;
    let time_provider = Arc::clone(&self.time_provider);
    let lifecycle_hooks = self.lifecycle_hooks.clone();
    let member_id = self.group_config.member_id.to_string();

    self.prefetch_task = Some(tokio::spawn(async move {
      PrefetchWorker::new(
        reader,
        reader_command_rx,
        shared_state,
        diagnostics,
        delivery_notify,
        space_notify,
        reader_command_notify,
        shutdown,
        metrics,
        base_idle_delay_ms,
        max_idle_delay_ms,
        time_provider,
        lifecycle_hooks,
        member_id,
      )
      .run()
      .await;
    }));
  }

  async fn stop_prefetch_task(&mut self) {
    self.prefetch_shutdown.store(true, Ordering::Release);
    self.reader_command_tx = None;
    self.delivery_notify.notify_waiters();
    self.prefetch_space_notify.notify_waiters();

    if let Some(handle) = self.prefetch_task.take() {
      handle.abort();
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
    pending_commits: &HashMap<VirtualPartitionId, PendingCommit>,
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

    let committed_cursors = report
      .renewed_partitions
      .iter()
      .filter_map(|partition_id| {
        pending_commits
          .get(partition_id)
          .map(|commit| (*partition_id, commit.clone()))
      })
      .collect::<Vec<_>>();
    self
      .metrics
      .heartbeat_committed_offsets
      .inc_by(u64::try_from(committed_cursors.len()).unwrap_or(u64::MAX));
    self
      .metrics
      .heartbeat_latency_seconds
      .observe(started_at.elapsed().as_secs_f64());

    let mut shared_state = self.shared_state.lock();
    for (partition_id, committed_cursor) in committed_cursors {
      shared_state.diagnostics.last_committed_cursors.insert(
        partition_id,
        ConsumerCommittedCursorSnapshot {
          offset: committed_cursor.offset,
          source_checkpoint: Some(committed_cursor.source_checkpoint),
          committed_at_ms: Some(now_ts_ms),
        },
      );
      if let Some(partition_state) = shared_state.active_partitions.get_mut(&partition_id) {
        partition_state
          .delivered_source_ranges
          .retain(|range| range.end_offset > committed_cursor.offset);
      }
    }
    shared_state.diagnostics.last_successful_heartbeat_at_ms = Some(now_ts_ms);
  }

  async fn heartbeat(
    &mut self,
    now_ts_ms: i64,
    trigger: HeartbeatTrigger,
  ) -> Result<HeartbeatReport> {
    let started_at = Instant::now();
    let pending_commits = self
      .shared_state
      .lock()
      .active_partitions
      .iter()
      .filter_map(|(partition_id, state)| {
        state
          .pending_commit
          .as_ref()
          .map(|commit| (*partition_id, commit.clone()))
      })
      .collect::<HashMap<_, _>>();
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
         generation={}, elapsed_ms={}, error={error:#}",
        self.group_config.topic,
        self.group_config.group_id,
        self.group_config.member_id,
        trigger.as_str(),
        self.coordinator.generation(),
        started_at.elapsed().as_millis()
      );
      self.record_heartbeat_failure(started_at);
      if now_ts_ms >= self.membership_lease_expires_at_ms {
        self
          .fence_active_partitions_after_membership_failure(now_ts_ms)
          .await?;
      } else {
        debug!(
          "consumer retaining active partitions until membership lease expires: topic={}, \
           group_id={}, member_id={}, generation={}, expires_at={}",
          self.group_config.topic,
          self.group_config.group_id,
          self.group_config.member_id,
          self.coordinator.generation(),
          format_unix_timestamp_ms(self.membership_lease_expires_at_ms),
        );
      }
      return Err(error);
    }
    self.membership_lease_expires_at_ms =
      now_ts_ms.saturating_add(consumer_lease_duration_ms(&self.group_config));

    let committed_cursors = pending_commits
      .iter()
      .map(|(partition_id, commit)| {
        (
          *partition_id,
          CommittedCursor {
            virtual_partition_id: *partition_id,
            seq_end: commit.offset,
            source_checkpoint: Some(commit.source_checkpoint.clone()),
          },
        )
      })
      .collect();
    let report = match self
      .coordinator
      .heartbeat_and_commit(now_ts_ms, &committed_cursors)
      .await
    {
      Ok(report) => report,
      Err(error) => {
        debug!(
          "consumer coordinator heartbeat failed: topic={}, group_id={}, member_id={}, \
           trigger={}, generation={}, elapsed_ms={}, error={error:#}",
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
          shared_state.active_partitions.remove(partition_id);
        }
        self.refresh_diagnostics_locked(&mut shared_state);
      }
      self.set_reader_assignment(
        self.active_assignment.iter().copied().collect(),
        now_ts_ms / 1_000,
        None,
        false,
      )?;
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
       next_heartbeat_at={}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      trigger.as_str(),
      self.coordinator.generation(),
      report.renewed_partitions,
      report.fenced_partitions,
      committed_offsets,
      started_at.elapsed().as_millis(),
      format_unix_timestamp_ms(self.next_heartbeat_at_ms)
    );
    Ok(report)
  }
}

impl ConsumerDriver {
  fn start(&mut self) -> Result<()> {
    ensure!(!self.started, "consumer iterator already started");
    self.started = true;
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
      {
        let mut shared_state = self.shared_state.lock();
        shared_state.terminal_error = Some(format!("{error:#}"));
        shared_state.diagnostics.started = false;
        shared_state.diagnostics.prefetch_worker_running = false;
      }
      self.delivery_notify.notify_waiters();
      return;
    }

    loop {
      let revocation_completed = match self.finish_pending_revocation_if_completed().await {
        Ok(completed) => completed,
        Err(error) => {
          self.shared_state.lock().terminal_error = Some(format!("{error:#}"));
          let _ = self.shutdown().await;
          self.delivery_notify.notify_waiters();
          return;
        },
      };
      let now_ts_ms = self.now_unix_millis();

      let heartbeat_failed = if now_ts_ms >= self.next_heartbeat_at_ms {
        trace!(
          "consumer scheduled heartbeat due: topic={}, group_id={}, member_id={}, generation={}, \
           now={}, due_at={}, overdue_ms={}, active_partitions={:?}, pending_commits={}",
          self.group_config.topic,
          self.group_config.group_id,
          self.group_config.member_id,
          self.coordinator.generation(),
          format_unix_timestamp_ms(now_ts_ms),
          format_unix_timestamp_ms(self.next_heartbeat_at_ms),
          now_ts_ms.saturating_sub(self.next_heartbeat_at_ms),
          self.active_assignment,
          self
            .shared_state
            .lock()
            .active_partitions
            .values()
            .filter(|state| state.pending_commit.is_some())
            .count()
        );
        if let Err(error) = self.heartbeat(now_ts_ms, HeartbeatTrigger::Scheduled).await {
          warn_every!(
            15.seconds(),
            "consumer scheduled heartbeat retrying after error: error={error:#}"
          );
          self.next_heartbeat_at_ms = now_ts_ms.saturating_add(1_000);
          self.refresh_diagnostics();
          true
        } else {
          false
        }
      } else {
        false
      };

      if revocation_completed && now_ts_ms >= self.next_rebalance_at_ms {
        if heartbeat_failed {
          debug!(
            "consumer rebalance deferred after heartbeat failure: topic={}, group_id={}, \
             member_id={}, generation={}",
            self.group_config.topic,
            self.group_config.group_id,
            self.group_config.member_id,
            self.coordinator.generation(),
          );
        } else {
          match self.maybe_rebalance(now_ts_ms).await {
            Ok(()) => {},
            Err(error) => {
              warn_every!(
                15.seconds(),
                "consumer rebalance retrying after error: error={error:#}"
              );
              self.next_rebalance_at_ms = now_ts_ms.saturating_add(1_000);
              self.refresh_rebalance_diagnostics();
            },
          }
        }
      }

      let until_heartbeat_ms = (self.next_heartbeat_at_ms - now_ts_ms).max(0);
      let until_rebalance_ms = (self.next_rebalance_at_ms - now_ts_ms).max(0);
      let wait_ms = if revocation_completed {
        max(1_i64, until_heartbeat_ms.min(until_rebalance_ms)).cast_unsigned()
      } else {
        until_heartbeat_ms.clamp(1, 100).cast_unsigned()
      };
      let time_provider = Arc::clone(&self.time_provider);
      let wait_duration = time::Duration::milliseconds(i64::try_from(wait_ms).unwrap_or(i64::MAX));
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
            ConsumerDriverCommand::AbortForTest { response } => {
              self.stop_prefetch_task().await;
              self.started = false;
              self.refresh_diagnostics();
              let _ = response.send(());
              self.delivery_notify.notify_waiters();
              return;
            },
            ConsumerDriverCommand::Seek {
              virtual_partition_id,
              offset,
              response,
            } => {
              self.seek(virtual_partition_id, offset, response);
            },
            ConsumerDriverCommand::Shutdown { response } => {
              let _ = response.send(self.shutdown().await);
              self.delivery_notify.notify_waiters();
              return;
            },
          }
        },
        () = self.revocation_notify.notified(), if !revocation_completed => {},
        () = time_provider.sleep(wait_duration) => {},
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
       active_partitions={:?}, pending_commit_partitions={}",
      self.group_config.topic,
      self.group_config.group_id,
      self.group_config.member_id,
      self.coordinator.generation(),
      self.active_assignment,
      self
        .shared_state
        .lock()
        .active_partitions
        .values()
        .filter(|state| state.pending_commit.is_some())
        .count()
    );
    if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
      lifecycle_hooks
        .before_commit(&self.group_config.member_id, self.coordinator.generation())
        .await;
    }
    self
      .heartbeat(self.now_unix_millis(), HeartbeatTrigger::Commit)
      .await
  }

  async fn shutdown(&mut self) -> Result<()> {
    if self.started {
      // Attempt commit and explicit lease release before membership deregistration.
      // Releasing leases proactively shortens rebalance convergence on graceful shutdown.
      let shutdown_span = bd_log::otel_info_span!(
        "blob_stream.consumer.shutdown",
        otel.kind = "internal",
        consumer.topic = %self.group_config.topic,
        consumer.group_id = %self.group_config.group_id,
        consumer.member_id = %self.group_config.member_id,
        consumer.generation = self.coordinator.generation(),
        shutdown.commit_outcome = field::Empty,
        shutdown.lease_release_outcome = field::Empty,
        shutdown.deregistration_outcome = field::Empty,
        shutdown.planner_release_outcome = field::Empty,
        error.message = field::Empty,
        otel.status_code = field::Empty,
      );
      let commit_result = self.commit().await;
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .shutdown_commit_finished(&self.group_config.member_id, self.coordinator.generation())
          .await;
      }
      self.refresh_diagnostics();
      let mut handoff_partitions = self.coordinator.owned_partitions();
      handoff_partitions.extend(self.active_assignment.iter().copied());
      handoff_partitions.sort_unstable();
      handoff_partitions.dedup();
      let handoff_snapshot = self.diagnostics.state_snapshot();
      emit_partition_handoff_snapshots(
        &handoff_snapshot,
        &handoff_partitions,
        "shutdown_pre_release",
        "pending_release",
        if commit_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
        &shutdown_span,
      );
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .before_release_owned(&self.group_config.member_id, self.coordinator.generation())
          .await;
      }
      let release_result = self.coordinator.release_owned(self.now_unix_millis()).await;
      match &release_result {
        Ok(released_partitions) => {
          let released_partitions = released_partitions.iter().copied().collect::<HashSet<_>>();
          let (released, not_released): (Vec<_>, Vec<_>) = handoff_partitions
            .iter()
            .copied()
            .partition(|partition_id| released_partitions.contains(partition_id));
          if !released.is_empty() {
            emit_partition_handoff_snapshots(
              &handoff_snapshot,
              &released,
              "shutdown_release_result",
              "released",
              if commit_result.is_ok() {
                "succeeded"
              } else {
                "failed"
              },
              &shutdown_span,
            );
          }
          if !not_released.is_empty() {
            emit_partition_handoff_snapshots(
              &handoff_snapshot,
              &not_released,
              "shutdown_release_result",
              "not_released",
              if commit_result.is_ok() {
                "succeeded"
              } else {
                "failed"
              },
              &shutdown_span,
            );
          }
        },
        Err(_) => emit_partition_handoff_snapshots(
          &handoff_snapshot,
          &handoff_partitions,
          "shutdown_release_result",
          "failed",
          if commit_result.is_ok() {
            "succeeded"
          } else {
            "failed"
          },
          &shutdown_span,
        ),
      }
      if let Some(lifecycle_hooks) = &self.lifecycle_hooks {
        lifecycle_hooks
          .before_deregister_member(&self.group_config.member_id, self.coordinator.generation())
          .await;
      }
      let deregistration_result = self
        .membership_store
        .deregister_member(
          &self.group_config.topic,
          &self.group_config.group_id,
          &self.group_config.member_id,
        )
        .await;
      let planner_release_result = self
        .membership_store
        .release_planner(
          &self.group_config.topic,
          &self.group_config.group_id,
          &self.group_config.member_id,
          self.coordinator.planner_session_id(),
        )
        .await;
      if let Err(error) = &planner_release_result {
        debug!(
          "consumer planner release failed during shutdown: topic={}, group_id={}, member_id={}, \
           error={error:#}",
          self.group_config.topic, self.group_config.group_id, self.group_config.member_id
        );
      }
      if let Err(error) = &deregistration_result {
        debug!(
          "consumer membership deregistration failed during shutdown: topic={}, group_id={}, \
           member_id={}, error={error:#}",
          self.group_config.topic, self.group_config.group_id, self.group_config.member_id
        );
      }
      self.stop_prefetch_task().await;
      self.started = false;
      self.refresh_diagnostics();
      shutdown_span.record(
        "shutdown.commit_outcome",
        if commit_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
      );
      shutdown_span.record(
        "shutdown.lease_release_outcome",
        if release_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
      );
      shutdown_span.record(
        "shutdown.deregistration_outcome",
        if deregistration_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
      );
      shutdown_span.record(
        "shutdown.planner_release_outcome",
        if planner_release_result.is_ok() {
          "succeeded"
        } else {
          "failed"
        },
      );
      let error_message = [
        ("commit", commit_result.as_ref().err()),
        ("lease release", release_result.as_ref().err()),
        (
          "membership deregistration",
          deregistration_result.as_ref().err(),
        ),
        ("planner release", planner_release_result.as_ref().err()),
      ]
      .into_iter()
      .filter_map(|(operation, error)| error.map(|error| format!("{operation}: {error:#}")))
      .collect::<Vec<_>>()
      .join("; ");
      if !error_message.is_empty() {
        shutdown_span.record("error.message", error_message);
      }
      shutdown_span.record(
        "otel.status_code",
        if commit_result.is_ok()
          && release_result.is_ok()
          && deregistration_result.is_ok()
          && planner_release_result.is_ok()
        {
          "OK"
        } else {
          "ERROR"
        },
      );
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
  fn seek(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    offset: u64,
    response: oneshot::Sender<Result<()>>,
  ) {
    if !self.active_assignment.contains(&virtual_partition_id) {
      let _ = response.send(Err(anyhow!(
        "cannot seek unassigned virtual partition {virtual_partition_id}"
      )));
      return;
    }
    {
      let mut shared_state = self.shared_state.lock();
      shared_state
        .delivery_state
        .drop_partitions(&HashSet::from([virtual_partition_id]));
      if let Some(partition_state) = shared_state
        .active_partitions
        .get_mut(&virtual_partition_id)
      {
        partition_state.pending_commit = None;
        partition_state.delivered_source_ranges.clear();
      }
      update_worker_prefetch_metrics(&self.metrics, &shared_state.delivery_state);
      update_total_prefetch_bytes(
        &self.metrics,
        &shared_state.delivery_state,
        shared_state.diagnostics.prefetch_pending_bytes,
      );
    }
    self.prefetch_space_notify.notify_waiters();
    self.refresh_diagnostics();
    self.seek_reader(
      virtual_partition_id,
      offset,
      self.now_unix_seconds(),
      response,
    );
    trace!(
      "consumer seek: topic={}, partition={}, offset={}",
      self.group_config.topic, virtual_partition_id, offset
    );
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
    {
      let mut shared_state = self.shared_state.lock();
      shared_state.diagnostics.started = true;
      shared_state.diagnostics.prefetch_worker_running = true;
    }
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
      tokio::pin!(notified);
      notified.as_mut().enable();
      let (next_result, terminal_error) = {
        let mut shared_state = self.shared_state.lock();
        let pending_bytes = shared_state.diagnostics.prefetch_pending_bytes;
        let ConsumerSharedState {
          active_partitions,
          delivery_state,
          terminal_error,
          ..
        } = &mut *shared_state;
        let next_result = delivery_state.try_take_next(active_partitions, &self.metrics);
        update_worker_prefetch_metrics(&self.metrics, delivery_state);
        update_total_prefetch_bytes(&self.metrics, delivery_state, pending_bytes);
        (next_result, terminal_error.clone())
      };
      #[cfg(test)]
      if let Some(hook) = self.next_after_delivery_state_check_hook.take() {
        let _ = hook.state_checked.send(());
        let _ = hook.release.await;
      }
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
    let partition_state = shared_state
      .active_partitions
      .get_mut(&virtual_partition_id)
      .ok_or_else(|| {
        anyhow!("cannot store cursor for unassigned virtual partition {virtual_partition_id}")
      })?;
    let source_checkpoint = partition_state
      .delivered_source_ranges
      .iter()
      .find_map(|range| {
        (range.start_offset <= offset && offset <= range.end_offset)
          .then(|| range.source_checkpoint.clone())
      })
      .ok_or_else(|| {
        anyhow!(
          "cannot store offset {offset} for partition {virtual_partition_id}: offset was not \
           delivered"
        )
      })?;
    if let Some(staged) = &partition_state.pending_commit {
      ensure!(
        offset >= staged.offset,
        "cannot store offset {offset} below staged offset {} for partition {virtual_partition_id}",
        staged.offset
      );
    }
    partition_state.pending_commit = Some(PendingCommit {
      offset,
      source_checkpoint,
    });
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

  fn set_assignment_callback(&mut self, callback: AssignmentCallback) {
    let mut active_assignment = self
      .shared_state
      .lock()
      .active_partitions
      .keys()
      .copied()
      .collect::<Vec<_>>();
    active_assignment.sort_unstable();
    *self.assignment_callback.lock() = Some(Arc::clone(&callback));
    if !active_assignment.is_empty() {
      callback(&active_assignment);
    }
  }

  fn diagnostics(&self) -> Option<ConsumerDiagnostics> {
    Some(self.diagnostics.clone())
  }
}
