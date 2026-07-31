use super::delivery::{DeliveredSourceRange, DeliveryState};
use crate::diagnostics::ConsumerDiagnosticsRuntimeState;
use bd_server_stats::stats::Scope;
use blob_stream_types::{CommittedSourceCheckpoint, VirtualPartitionId};
use parking_lot::Mutex;
use prometheus::{Histogram, IntCounter, IntGauge};
use serde::Serialize;
use std::collections::HashMap;

//
// ConsumerIteratorMetrics
//

#[derive(Clone)]
pub(super) struct ConsumerIteratorMetrics {
  pub(super) batches_delivered: IntCounter,
  pub(super) records_delivered: IntCounter,
  pub(super) retries: IntCounter,
  pub(super) failures: IntCounter,
  pub(super) revocations: IntCounter,
  pub(super) rebalances_total: IntCounter,
  pub(super) rebalance_failures_total: IntCounter,
  pub(super) assignment_plans_applied_total: IntCounter,
  pub(super) assignment_plan_rejections_total: IntCounter,
  pub(super) assignment_applications_total: IntCounter,
  pub(super) lease_claims_initial: IntCounter,
  pub(super) lease_claims_retained: IntCounter,
  pub(super) lease_claims_graceful_handoff: IntCounter,
  pub(super) lease_claims_expiry_takeover: IntCounter,
  pub(super) desired_partitions: IntGauge,
  pub(super) owned_partitions: IntGauge,
  pub(super) active_partitions: IntGauge,
  pub(super) prefetch_buffered_batches: IntGauge,
  pub(super) prefetch_buffered_bytes: IntGauge,
  pub(super) prefetch_pending_batches: IntGauge,
  pub(super) prefetch_pending_bytes: IntGauge,
  pub(super) prefetch_total_bytes: IntGauge,
  pub(super) prefetch_paused_budget: IntCounter,
  pub(super) prefetch_refill_cycles: IntCounter,
  pub(super) heartbeat_calls: IntCounter,
  pub(super) heartbeat_scheduled_calls: IntCounter,
  pub(super) heartbeat_commit_calls: IntCounter,
  pub(super) heartbeat_failures: IntCounter,
  pub(super) membership_heartbeat_failures: IntCounter,
  pub(super) lease_heartbeat_failures: IntCounter,
  pub(super) heartbeat_retry_attempts: IntCounter,
  pub(super) rebalance_retry_attempts: IntCounter,
  pub(super) heartbeat_committed_offsets: IntCounter,
  pub(super) heartbeat_renewed_partitions: IntCounter,
  pub(super) lease_renewed_partitions: IntCounter,
  pub(super) cursor_commit_partitions: IntCounter,
  pub(super) heartbeat_fenced_partitions: IntCounter,
  pub(super) heartbeat_latency_seconds: Histogram,
  pub(super) next_latency_seconds: Histogram,
  pub(super) commit_latency_seconds: Histogram,
}

impl ConsumerIteratorMetrics {
  pub(super) fn new(scope: &Scope) -> Self {
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
      lease_claims_initial: scope.counter("lease_claims_initial"),
      lease_claims_retained: scope.counter("lease_claims_retained"),
      lease_claims_graceful_handoff: scope.counter("lease_claims_graceful_handoff"),
      lease_claims_expiry_takeover: scope.counter("lease_claims_expiry_takeover"),
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
      membership_heartbeat_failures: scope.counter("membership_heartbeat_failures"),
      lease_heartbeat_failures: scope.counter("lease_heartbeat_failures"),
      heartbeat_retry_attempts: scope.counter("heartbeat_retry_attempts"),
      rebalance_retry_attempts: scope.counter("rebalance_retry_attempts"),
      heartbeat_committed_offsets: scope.counter("heartbeat_committed_offsets"),
      heartbeat_renewed_partitions: scope.counter("heartbeat_renewed_partitions"),
      lease_renewed_partitions: scope.counter("lease_renewed_partitions"),
      cursor_commit_partitions: scope.counter("cursor_commit_partitions"),
      heartbeat_fenced_partitions: scope.counter("heartbeat_fenced_partitions"),
      heartbeat_latency_seconds: scope.histogram("heartbeat_latency_seconds"),
      next_latency_seconds: scope.histogram("next_latency_seconds"),
      commit_latency_seconds: scope.histogram("commit_latency_seconds"),
    }
  }
}

//
// PendingCommit
//

#[derive(Clone, Debug)]
pub struct PendingCommit {
  pub offset: u64,
  pub(super) source_checkpoint: CommittedSourceCheckpoint,
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
// ConsumerDeliveryState
//

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
  pub(super) terminal_error: Option<String>,
  pub diagnostics: ConsumerDiagnosticsRuntimeState,
}

impl ConsumerSharedState {
  #[allow(dead_code)]
  fn assert_mutex_type(_: &Mutex<Self>) {}
}
