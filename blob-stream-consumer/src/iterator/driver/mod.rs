use super::api::RevokedPartitionsImpl;
use super::delivery::{update_total_prefetch_bytes, update_worker_prefetch_metrics};
use super::prefetch::{
  ConsumerReaderCommand,
  PrefetchWorker,
  SeekTrace,
  record_reader_diagnostics,
};
use super::shared::{ConsumerIteratorMetrics, PendingCommit};
use super::{
  AssignmentCallback,
  ConsumerCoordinationSource,
  ConsumerLifecycleHooks,
  ConsumerSeekTarget,
  ConsumerSharedState,
  CoordinationSnapshot,
};
use crate::config::{
  ConsumerGroupConfig,
  consumer_heartbeat_interval,
  consumer_lease_duration,
  consumer_rebalance_interval,
};
use crate::consumer::ConsumerReaderImpl;
use crate::coordination::{
  ConsumerGroupCoordinator,
  HeartbeatReport,
  RebalanceReport,
  RecoveredCursor,
};
use crate::diagnostics::{
  ConsumerCommittedCursorSnapshot,
  ConsumerDiagnostics,
  ConsumerStateSnapshot,
  assignment_plan_snapshot,
  emit_partition_handoff_snapshots,
};
use anyhow::{Result, anyhow, ensure};
use bd_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use bd_time::{OffsetDateTimeExt, TimeProvider};
use blob_stream_metadata_store::{ConsumerGroupAssignmentPlan, ConsumerGroupMembershipStore};
use blob_stream_types::{CommittedCursor, VirtualPartitionId, offset_datetime_from_unix_millis};
use log::{debug, info, trace};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{Span, field};

mod assignment;
mod diagnostics;
mod heartbeat;
mod lifecycle;
mod reader;
mod runtime;

const RETRY_INITIAL_DELAY: TimeDuration = TimeDuration::milliseconds(500);
pub(in crate::iterator) const RETRY_MAX_DELAY: TimeDuration = TimeDuration::seconds(30);

pub(in crate::iterator) fn retry_backoff() -> ExponentialBackoff {
  ExponentialBackoffBuilder::new_infinite()
    .with_initial_interval(RETRY_INITIAL_DELAY)
    .with_randomization_factor(0.5)
    .with_multiplier(2.0)
    .with_max_interval(RETRY_MAX_DELAY)
    .build()
}

pub(in crate::iterator) fn persisted_lease_expires_at(
  now: OffsetDateTime,
  lease_duration: TimeDuration,
) -> Result<OffsetDateTime> {
  let lease_duration_ms = i64::try_from(lease_duration.whole_milliseconds())
    .map_err(|_| anyhow!("consumer lease duration exceeds millisecond range"))?;
  let expires_at_ms = now
    .unix_timestamp_ms()
    .checked_add(lease_duration_ms)
    .ok_or_else(|| anyhow!("consumer lease expiration exceeds millisecond range"))?;
  Ok(offset_datetime_from_unix_millis(expires_at_ms))
}

//
// HeartbeatTrigger
//

#[derive(Clone, Copy)]
pub(in crate::iterator) enum HeartbeatTrigger {
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
// ConsumerDriver
//

/// Long-lived owner of consumer coordination and prefetch lifecycle state.
pub(in crate::iterator) struct ConsumerDriver {
  pub(in crate::iterator) group_config: ConsumerGroupConfig,
  pub(in crate::iterator) reader: Option<ConsumerReaderImpl>,
  pub(in crate::iterator) reader_command_tx: Option<mpsc::UnboundedSender<ConsumerReaderCommand>>,
  pub(in crate::iterator) coordinator: Box<dyn ConsumerGroupCoordinator>,
  pub(in crate::iterator) membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  pub(in crate::iterator) coordination_source: Arc<dyn ConsumerCoordinationSource>,
  pub(in crate::iterator) last_coordination_snapshot: Option<CoordinationSnapshot>,
  pub(in crate::iterator) metrics: ConsumerIteratorMetrics,
  pub(in crate::iterator) started: bool,
  pub(in crate::iterator) shared_state: Arc<Mutex<ConsumerSharedState>>,
  pub(in crate::iterator) delivery_notify: Arc<Notify>,
  pub(in crate::iterator) prefetch_space_notify: Arc<Notify>,
  pub(in crate::iterator) reader_command_notify: Arc<Notify>,
  pub(in crate::iterator) prefetch_shutdown: Arc<AtomicBool>,
  pub(in crate::iterator) prefetch_task: Option<JoinHandle<()>>,
  pub(in crate::iterator) prefetch_idle_base_delay: TimeDuration,
  pub(in crate::iterator) prefetch_idle_max_delay: Option<TimeDuration>,
  pub(in crate::iterator) active_assignment: HashSet<VirtualPartitionId>,
  pub(in crate::iterator) assignment_callback: Arc<Mutex<Option<AssignmentCallback>>>,
  pub(in crate::iterator) pending_assignment: Option<Vec<VirtualPartitionId>>,
  pub(in crate::iterator) pending_revocation_completion: Option<oneshot::Receiver<()>>,
  pub(in crate::iterator) pending_revocation_partitions: Option<Vec<VirtualPartitionId>>,
  pub(in crate::iterator) pending_revocation_snapshot: Option<ConsumerStateSnapshot>,
  pub(in crate::iterator) pending_revocation_span: Option<Span>,
  pub(in crate::iterator) revocation_notify: Arc<Notify>,
  pub(in crate::iterator) lifecycle_hooks: Option<Arc<dyn ConsumerLifecycleHooks>>,
  pub(in crate::iterator) time_provider: Arc<dyn TimeProvider>,
  pub(in crate::iterator) prefetch_time_provider: Arc<dyn TimeProvider>,
  pub(in crate::iterator) membership_lease_expires_at: OffsetDateTime,
  /// Earliest expiration among this driver's active partition leases.
  ///
  /// This driver-wide safety deadline bounds heartbeat retries so it never continues delivery
  /// beyond a lease that may no longer be valid.
  pub(in crate::iterator) active_partition_lease_expiration_deadline: OffsetDateTime,
  pub(in crate::iterator) next_heartbeat_at: OffsetDateTime,
  pub(in crate::iterator) next_rebalance_at: OffsetDateTime,
  pub(in crate::iterator) heartbeat_retry_backoff: ExponentialBackoff,
  pub(in crate::iterator) rebalance_retry_backoff: ExponentialBackoff,
  pub(in crate::iterator) diagnostics: ConsumerDiagnostics,
}

//
// ConsumerDriverCommand
//

pub(in crate::iterator) enum ConsumerDriverCommand {
  Commit {
    response: oneshot::Sender<Result<HeartbeatReport>>,
  },
  AbortForTest {
    response: oneshot::Sender<()>,
  },
  Seek {
    virtual_partition_id: VirtualPartitionId,
    target: ConsumerSeekTarget,
    response: oneshot::Sender<Result<()>>,
  },
  Shutdown {
    response: oneshot::Sender<Result<()>>,
  },
}
