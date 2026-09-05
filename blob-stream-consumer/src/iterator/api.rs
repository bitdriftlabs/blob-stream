use crate::coordination::HeartbeatReport;
use crate::diagnostics::ConsumerDiagnostics;
use anyhow::Result;
use async_trait::async_trait;
use blob_stream_types::{CommittedSourceCheckpoint, Record, VirtualPartitionId};
use std::sync::Arc;
use tokio::sync::{Notify, oneshot};

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

pub(super) struct RevokedPartitionsImpl {
  pub(super) revoked: Vec<VirtualPartitionId>,
  pub(super) completion_tx: Option<oneshot::Sender<()>>,
  pub(super) completion_notify: Arc<Notify>,
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
  /// Metadata source that produced this record.
  pub source_checkpoint: CommittedSourceCheckpoint,
  /// Decoded record payload and metadata.
  pub record: Record,
}

//
// ConsumerSeekTarget
//

/// Caller-provided source location from which an explicit seek recovers retained history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerSeekTarget {
  /// Sequence offset to resume after discarding buffered records.
  pub offset: u64,
  /// Metadata window containing the recovery origin.
  pub window_start_unix_seconds: i64,
  /// Optional source segment identifier used to derive the first recovery-window lower bound.
  pub snowflake_id: Option<u64>,
}

/// Result of polling the iterator.
pub enum NextResult {
  /// Next available record for an owned partition.
  Record(ConsumerRecord),
  /// Notification that partitions were revoked and must be drained.
  Revoked(Box<dyn RevokedPartitions>),
}

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

  /// Runs when a full prefetch queue prevents the reader from beginning another scan.
  async fn prefetch_capacity_exhausted(
    &self,
    _member_id: &str,
    _buffered_partitions: &[VirtualPartitionId],
  ) {
  }

  /// Runs after the prefetch worker observes a partition leave recovery for the Fast path.
  async fn recovery_fast_path_active(
    &self,
    _member_id: &str,
    _generation: u64,
    _virtual_partition_id: VirtualPartitionId,
  ) {
  }

  /// Runs after an initial scan activates the Fast path without recovery state.
  async fn initial_fast_path_active(
    &self,
    _member_id: &str,
    _generation: u64,
    _virtual_partition_id: VirtualPartitionId,
  ) {
  }

  /// Runs immediately before a scheduled heartbeat renews membership and partition leases.
  async fn before_scheduled_heartbeat(&self, _member_id: &str, _generation: u64) {}

  /// Runs immediately before the driver begins a consumer-group rebalance.
  async fn before_rebalance(&self, _member_id: &str, _generation: u64) {}

  /// Runs after a changed group assignment is computed but before its revocations are published.
  async fn rebalance_plan_ready(
    &self,
    _member_id: &str,
    _generation: u64,
    _current_assignment: &[VirtualPartitionId],
    _next_assignment: &[VirtualPartitionId],
  ) {
  }

  /// Runs after a rebalance fails and before the driver schedules its retry.
  async fn rebalance_failed(&self, _member_id: &str, _generation: u64) {}

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
  /// Flush staged offsets without renewing unrelated partition leases.
  async fn commit(&mut self) -> Result<HeartbeatReport>;
  /// Shutdown iterator and release owned partitions.
  async fn shutdown(self: Box<Self>) -> Result<()>;
  /// Reposition a partition cursor after discarding buffered records read under the prior cursor.
  ///
  /// The next delivered record has an offset greater than `target.offset`, even when the target
  /// lies within a decoded batch.
  async fn seek(
    &mut self,
    virtual_partition_id: VirtualPartitionId,
    target: ConsumerSeekTarget,
  ) -> Result<()>;
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
