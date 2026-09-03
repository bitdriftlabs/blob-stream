use crate::config::ConsumerGroupConfig;
use crate::iterator::{ConsumerDeliveryState, ConsumerSharedState};
use blob_stream_metadata_store::{
  ConsumerGroupArmFreshStartOutcome,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupLease,
  ConsumerGroupLeaseStore,
};
use blob_stream_types::{
  CommittedSourceCheckpoint,
  VirtualPartitionId,
  now_unix_millis,
  offset_datetime_from_unix_millis,
  offset_datetime_from_unix_seconds,
};
use log::debug;
use parking_lot::Mutex;
use serde::{Serialize, Serializer};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tracing::Span;

//
// ConsumerStateResponse
//

#[derive(Debug, Serialize)]
pub struct ConsumerStateResponse {
  #[serde(flatten)]
  pub state: ConsumerStateSnapshot,
  pub group_lease_observation: ConsumerGroupLeaseObservation,
}

//
// ConsumerStateSnapshot
//

#[derive(Clone, Debug, Serialize)]
/// Immutable local state that is available without awaiting external work.
pub struct ConsumerStateSnapshot {
  #[serde(with = "time::serde::rfc3339")]
  pub generated_at: OffsetDateTime,
  pub topic: String,
  pub group_id: String,
  pub member_id: String,
  pub started: bool,
  /// Version of the assignment plan most recently accepted by this process.
  pub accepted_assignment_plan_version: u64,
  pub assignment_plan: Option<ConsumerAssignmentPlanSnapshot>,
  pub local: ConsumerLocalStateSnapshot,
  #[serde(with = "time::serde::rfc3339")]
  pub next_heartbeat_at: OffsetDateTime,
  #[serde(with = "time::serde::rfc3339")]
  pub next_rebalance_at: OffsetDateTime,
  pub prefetch_buffered_batch_count: usize,
  pub prefetch_buffered_record_count: usize,
  pub prefetch_buffered_bytes: u64,
  pub prefetch_pending_batch_count: usize,
  pub prefetch_pending_record_count: usize,
  pub prefetch_pending_bytes: u64,
  pub prefetch_max_bytes: u64,
  pub prefetch_worker_running: bool,
}

//
// ConsumerLocalStateSnapshot
//

#[derive(Clone, Debug, Serialize)]
/// Local state that applies to this member as a whole or is grouped by virtual partition.
pub struct ConsumerLocalStateSnapshot {
  pub pending_revocation: bool,
  pub delivery_state: ConsumerDeliveryState,
  #[serde(with = "time::serde::rfc3339::option")]
  pub last_successful_heartbeat_at: Option<OffsetDateTime>,
  pub partitions: Vec<ConsumerLocalPartitionSnapshot>,
}

//
// ConsumerLocalPartitionSnapshot
//

#[derive(Clone, Debug, Serialize)]
/// All local consumer state associated with one virtual partition.
pub struct ConsumerLocalPartitionSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub owned: bool,
  pub active: bool,
  pub pending_assignment: bool,
  pub pending_commit_offset: Option<u64>,
  pub last_committed_offset: Option<u64>,
  pub last_committed_source_checkpoint: Option<ConsumerSourceCheckpointSnapshot>,
  #[serde(with = "time::serde::rfc3339::option")]
  pub last_committed_at: Option<OffsetDateTime>,
  pub cursor: Option<u64>,
  pub reader: Option<ConsumerReaderStateSnapshot>,
  pub last_scan: Option<ConsumerReaderScanSnapshot>,
  pub prefetch_buffered_batch_count: usize,
  pub prefetch_buffered_record_count: usize,
}

//
// ConsumerReaderStateSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Reader scan state for a local virtual partition.
pub struct ConsumerReaderStateSnapshot {
  pub mode: ConsumerPartitionReadMode,
  #[serde(with = "time::serde::rfc3339::option")]
  pub recovery_next_window_start: Option<OffsetDateTime>,
  #[serde(with = "time::serde::rfc3339::option")]
  pub recovery_cutover_window_start: Option<OffsetDateTime>,
}

//
// ConsumerReaderScanSnapshot
//

/// Most recent successful scan outcome for one local reader partition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConsumerReaderScanSnapshot {
  #[serde(with = "time::serde::rfc3339")]
  pub completed_at: OffsetDateTime,
  #[serde(serialize_with = "serialize_rfc3339_timestamp_vec")]
  pub scanned_window_starts: Vec<OffsetDateTime>,
  pub scanned_window_starts_truncated: bool,
  pub fast_scan_bounds: Vec<ConsumerReaderFastScanBoundSnapshot>,
  pub fast_scan_bounds_truncated: bool,
  pub fast_frontiers: Vec<ConsumerReaderFastFrontierSnapshot>,
  pub fast_frontiers_truncated: bool,
  pub cursor_before: Option<u64>,
  pub cursor_after: Option<u64>,
  pub metadata_segments_seen: usize,
  pub metadata_segments_without_partition_batches: usize,
  pub metadata_batches_seen: usize,
  pub metadata_batches_skipped_by_cursor: usize,
  pub metadata_segments_skipped_by_frontier: usize,
  pub metadata_segments_deferred_by_visibility: usize,
  pub metadata_segments_blocked_by_visibility: usize,
  pub recovery_segments_handed_to_fast_by_visibility: usize,
  pub recovery_segments_blocked_by_visibility: usize,
  pub recovery_metadata_cache_hits: usize,
  pub recovery_metadata_cache_misses: usize,
  pub metadata_batches_deferred_by_capacity: usize,
  pub batches_accepted: usize,
  pub records_accepted: usize,
}

//
// ConsumerReaderFastScanBoundSnapshot
//

/// Human-readable pre-query Fast-path lower-bound inputs and resulting metadata-query lower bound.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConsumerReaderFastScanBoundSnapshot {
  #[serde(with = "time::serde::rfc3339")]
  pub window_start: OffsetDateTime,
  #[serde(with = "time::serde::rfc3339")]
  pub floor_timestamp: OffsetDateTime,
  pub time_floor_snowflake_id: u64,
  pub observed_frontier_snowflake_id: Option<u64>,
  pub partition_lower_bound_snowflake_id: u64,
  pub query_lower_bound_snowflake_id: Option<u64>,
}

//
// ConsumerReaderFastFrontierSnapshot
//

/// Sparse retained inclusive metadata frontier from a successful reader scan of one eligible
/// window.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConsumerReaderFastFrontierSnapshot {
  #[serde(with = "time::serde::rfc3339")]
  pub window_start: OffsetDateTime,
  pub snowflake_id: u64,
}

//
// ConsumerGroupLeaseObservation
//

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
/// Fresh group-wide lease observation for a state request, or its lookup failure.
pub enum ConsumerGroupLeaseObservation {
  Fresh {
    partitions: Vec<ConsumerGroupPartitionLeaseSnapshot>,
  },
  LookupFailed {
    error: String,
  },
}

//
// ConsumerGroupPartitionLeaseSnapshot
//

// Human-readable source checkpoint attached to a committed group lease.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConsumerSourceCheckpointSnapshot {
  #[serde(with = "time::serde::rfc3339")]
  pub window_start: OffsetDateTime,
  pub snowflake_id: u64,
}

//
// ConsumerFreshStartMarkerSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Durable cursor-recovery instruction awaiting a consumer restart or reassignment.
pub struct ConsumerFreshStartMarkerSnapshot {
  pub marker_id: String,
  pub source_checkpoint: ConsumerSourceCheckpointSnapshot,
  #[serde(with = "time::serde::rfc3339")]
  pub target_window_start: OffsetDateTime,
  #[serde(with = "time::serde::rfc3339")]
  pub armed_at: OffsetDateTime,
}

#[derive(Clone)]
pub struct ConsumerCommittedCursorSnapshot {
  pub offset: u64,
  pub source_checkpoint: Option<CommittedSourceCheckpoint>,
  pub committed_at_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
/// Desired and observed lease state for one consumer-group virtual partition.
pub struct ConsumerGroupPartitionLeaseSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub desired_owner_id: Option<String>,
  pub owner_id: Option<String>,
  pub generation: Option<u64>,
  #[serde(with = "time::serde::rfc3339::option")]
  pub lease_expiration_at: Option<OffsetDateTime>,
  #[serde(with = "time::serde::rfc3339::option")]
  pub last_heartbeat_at: Option<OffsetDateTime>,
  pub committed_offset: Option<u64>,
  pub committed_source_checkpoint: Option<ConsumerSourceCheckpointSnapshot>,
  #[serde(with = "time::serde::rfc3339::option")]
  pub committed_at: Option<OffsetDateTime>,
  pub fresh_start_marker: Option<ConsumerFreshStartMarkerSnapshot>,
}

//
// ConsumerAssignmentPlanSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Shared desired group ownership plan observed by this iterator.
pub struct ConsumerAssignmentPlanSnapshot {
  pub version: u64,
  pub planner_member_id: String,
  pub policy: ConsumerAssignmentPolicy,
  pub members: Vec<String>,
  pub member_topology: Vec<ConsumerMemberTopologySnapshot>,
  pub pod_loads: Vec<ConsumerPodLoadSnapshot>,
  pub assignments: Vec<ConsumerPartitionAssignmentSnapshot>,
  #[serde(with = "time::serde::rfc3339")]
  pub published_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerAssignmentPolicy {
  FlatMember,
  PodAware,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Physical topology registered by one member of a pod-aware plan.
pub struct ConsumerMemberTopologySnapshot {
  pub member_id: String,
  pub pod_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Aggregate partition load assigned to one physical pod.
pub struct ConsumerPodLoadSnapshot {
  pub pod_id: String,
  pub partition_count: usize,
}

//
// ConsumerPartitionAssignmentSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Desired shared-plan owner for one virtual partition.
pub struct ConsumerPartitionAssignmentSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub member_id: String,
  pub pod_id: Option<String>,
}

//
// ConsumerPartitionReadMode
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerPartitionReadMode {
  Fresh,
  Recovering,
  Fast,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConsumerOffsetSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConsumerReaderPartitionSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub mode: ConsumerPartitionReadMode,
  #[serde(with = "time::serde::rfc3339::option")]
  pub recovery_next_window_start: Option<OffsetDateTime>,
  #[serde(with = "time::serde::rfc3339::option")]
  pub recovery_cutover_window_start: Option<OffsetDateTime>,
}

#[derive(Clone, Default)]
pub struct ConsumerDiagnosticsRuntimeState {
  pub started: bool,
  pub prefetch_worker_running: bool,
  pub accepted_assignment_plan_version: u64,
  pub assignment_plan: Option<ConsumerAssignmentPlanSnapshot>,
  pub owned_partitions: Vec<VirtualPartitionId>,
  pub active_assignment: Vec<VirtualPartitionId>,
  pub pending_assignment: Option<Vec<VirtualPartitionId>>,
  pub pending_revocation: bool,
  pub last_committed_cursors: HashMap<VirtualPartitionId, ConsumerCommittedCursorSnapshot>,
  pub cursors: Vec<ConsumerOffsetSnapshot>,
  pub reader_partitions: Vec<ConsumerReaderPartitionSnapshot>,
  pub reader_partition_scans: Vec<(VirtualPartitionId, ConsumerReaderScanSnapshot)>,
  pub last_successful_heartbeat_at_ms: Option<i64>,
  pub next_heartbeat_at_ms: i64,
  pub next_rebalance_at_ms: i64,
  pub prefetch_pending_batch_count: usize,
  pub prefetch_pending_record_count: usize,
  pub prefetch_pending_bytes: u64,
}

#[derive(Clone)]
pub struct ConsumerDiagnostics {
  group_config: ConsumerGroupConfig,
  shared_state: Arc<Mutex<ConsumerSharedState>>,
  prefetch_max_bytes: u64,
  lease_store: Arc<dyn ConsumerGroupLeaseStore>,
  metadata_window_size: time::Duration,
}

impl ConsumerDiagnostics {
  pub fn new(
    group_config: ConsumerGroupConfig,
    shared_state: Arc<Mutex<ConsumerSharedState>>,
    prefetch_max_bytes: u64,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
    metadata_window_size: time::Duration,
  ) -> Self {
    Self {
      group_config,
      shared_state,
      prefetch_max_bytes,
      lease_store,
      metadata_window_size,
    }
  }

  #[must_use]
  pub fn state_snapshot(&self) -> ConsumerStateSnapshot {
    self.build_state_snapshot()
  }

  fn build_state_snapshot(&self) -> ConsumerStateSnapshot {
    let (
      runtime_state,
      pending_commit_state,
      buffered_batches,
      current_batch,
      prefetch_buffered_bytes,
      delivery_pending,
    ) = {
      let shared_state = self.shared_state.lock();
      (
        shared_state.diagnostics.clone(),
        shared_state
          .active_partitions
          .iter()
          .filter_map(|(partition_id, state)| {
            state
              .pending_commit
              .as_ref()
              .map(|commit| (*partition_id, commit.clone()))
          })
          .collect::<HashMap<_, _>>(),
        shared_state
          .delivery_state
          .batches
          .iter()
          .map(|batch| (batch.virtual_partition_id, batch.records.len()))
          .collect::<Vec<_>>(),
        shared_state
          .delivery_state
          .current_batch
          .as_ref()
          .map(|batch| (batch.virtual_partition_id, batch.records.len())),
        shared_state.delivery_state.buffered_bytes,
        shared_state.delivery_state.pending_revocation.is_some()
          || shared_state.delivery_state.current_batch.is_some()
          || !shared_state.delivery_state.batches.is_empty(),
      )
    };

    // Partition data is updated by several independent paths. Merge it by id here so diagnostics
    // present one complete local view per partition instead of parallel, correlated lists.
    let mut local_partitions = BTreeMap::new();
    for partition_id in runtime_state.owned_partitions {
      local_partition_snapshot(&mut local_partitions, partition_id).owned = true;
    }
    for partition_id in runtime_state.active_assignment {
      local_partition_snapshot(&mut local_partitions, partition_id).active = true;
    }
    for partition_id in runtime_state.pending_assignment.unwrap_or_default() {
      local_partition_snapshot(&mut local_partitions, partition_id).pending_assignment = true;
    }
    for (partition_id, commit) in pending_commit_state {
      local_partition_snapshot(&mut local_partitions, partition_id).pending_commit_offset =
        Some(commit.offset);
    }
    for (partition_id, committed_cursor) in runtime_state.last_committed_cursors {
      let partition = local_partition_snapshot(&mut local_partitions, partition_id);
      partition.last_committed_offset = Some(committed_cursor.offset);
      partition.last_committed_source_checkpoint = committed_cursor
        .source_checkpoint
        .as_ref()
        .map(source_checkpoint_snapshot);
      partition.last_committed_at = committed_cursor
        .committed_at_ms
        .map(offset_datetime_from_unix_millis);
    }
    for cursor in runtime_state.cursors {
      local_partition_snapshot(&mut local_partitions, cursor.virtual_partition_id).cursor =
        Some(cursor.offset);
    }
    for reader_partition in runtime_state.reader_partitions {
      let ConsumerReaderPartitionSnapshot {
        virtual_partition_id,
        mode,
        recovery_next_window_start,
        recovery_cutover_window_start,
      } = reader_partition;
      local_partition_snapshot(&mut local_partitions, virtual_partition_id).reader =
        Some(ConsumerReaderStateSnapshot {
          mode,
          recovery_next_window_start,
          recovery_cutover_window_start,
        });
    }
    for (partition_id, scan) in runtime_state.reader_partition_scans {
      local_partition_snapshot(&mut local_partitions, partition_id).last_scan = Some(scan);
    }
    for (partition_id, record_count) in &buffered_batches {
      let snapshot = local_partition_snapshot(&mut local_partitions, *partition_id);
      snapshot.prefetch_buffered_batch_count =
        snapshot.prefetch_buffered_batch_count.saturating_add(1);
      snapshot.prefetch_buffered_record_count = snapshot
        .prefetch_buffered_record_count
        .saturating_add(*record_count);
    }
    // The active batch has already left the queue, but its remaining records are still buffered
    // locally until `next()` yields them. Attribute them without changing the queued batch count.
    if let Some((partition_id, record_count)) = current_batch {
      let snapshot = local_partition_snapshot(&mut local_partitions, partition_id);
      snapshot.prefetch_buffered_record_count = snapshot
        .prefetch_buffered_record_count
        .saturating_add(record_count);
    }
    let prefetch_buffered_record_count = buffered_batches
      .iter()
      .map(|(_, record_count)| *record_count)
      .sum::<usize>()
      .saturating_add(current_batch.map_or(0, |(_, record_count)| record_count));

    ConsumerStateSnapshot {
      generated_at: offset_datetime_from_unix_millis(now_unix_millis()),
      topic: self.group_config.topic.to_string(),
      group_id: self.group_config.group_id.to_string(),
      member_id: self.group_config.member_id.to_string(),
      started: runtime_state.started,
      accepted_assignment_plan_version: runtime_state.accepted_assignment_plan_version,
      assignment_plan: runtime_state.assignment_plan,
      local: ConsumerLocalStateSnapshot {
        pending_revocation: runtime_state.pending_revocation,
        delivery_state: if delivery_pending {
          ConsumerDeliveryState::Pending
        } else {
          ConsumerDeliveryState::Idle
        },
        last_successful_heartbeat_at: runtime_state
          .last_successful_heartbeat_at_ms
          .map(offset_datetime_from_unix_millis),
        partitions: local_partitions.into_values().collect(),
      },
      next_heartbeat_at: offset_datetime_from_unix_millis(runtime_state.next_heartbeat_at_ms),
      next_rebalance_at: offset_datetime_from_unix_millis(runtime_state.next_rebalance_at_ms),
      prefetch_buffered_batch_count: buffered_batches.len(),
      prefetch_buffered_record_count,
      prefetch_buffered_bytes,
      prefetch_pending_batch_count: runtime_state.prefetch_pending_batch_count,
      prefetch_pending_record_count: runtime_state.prefetch_pending_record_count,
      prefetch_pending_bytes: runtime_state.prefetch_pending_bytes,
      prefetch_max_bytes: self.prefetch_max_bytes,
      prefetch_worker_running: runtime_state.prefetch_worker_running,
    }
  }

  pub async fn state_response(&self) -> ConsumerStateResponse {
    let mut state = self.build_state_snapshot();
    let group_lease_observation = match tokio::time::timeout(
      Duration::from_secs(1),
      self
        .lease_store
        .list_group_leases(&state.topic, &state.group_id),
    )
    .await
    {
      Ok(Ok(leases)) => group_lease_observation(state.assignment_plan.as_ref(), leases),
      Ok(Err(error)) => {
        debug!(
          "consumer state lease lookup failed: topic={}, group_id={}, error={error:#}",
          state.topic, state.group_id
        );
        ConsumerGroupLeaseObservation::LookupFailed {
          error: format!("{error:#}"),
        }
      },
      Err(_) => {
        debug!(
          "consumer state lease lookup timed out: topic={}, group_id={}",
          state.topic, state.group_id
        );
        ConsumerGroupLeaseObservation::LookupFailed {
          error: "consumer lease lookup timed out after 1 second".to_string(),
        }
      },
    };

    state.generated_at = offset_datetime_from_unix_millis(now_unix_millis());
    ConsumerStateResponse {
      state,
      group_lease_observation,
    }
  }

  pub fn admin_router(self) -> axum::Router {
    crate::admin::router(self)
  }

  pub async fn arm_next_window_fresh_start(
    &self,
    virtual_partition_ids: Vec<VirtualPartitionId>,
    dry_run: bool,
  ) -> Result<Vec<ConsumerArmFreshStartResult>, anyhow::Error> {
    let now = OffsetDateTime::now_utc();
    let existing_leases = if dry_run {
      Some(
        self
          .lease_store
          .list_group_leases(&self.group_config.topic, &self.group_config.group_id)
          .await?
          .into_iter()
          .map(|lease| (lease.key.virtual_partition_id, lease))
          .collect::<HashMap<_, _>>(),
      )
    } else {
      None
    };
    let mut results = Vec::with_capacity(virtual_partition_ids.len());
    for virtual_partition_id in virtual_partition_ids {
      let key = blob_stream_metadata_store::ConsumerGroupLeaseKey {
        topic: self.group_config.topic.to_string(),
        group_id: self.group_config.group_id.to_string(),
        virtual_partition_id,
      };
      if dry_run {
        results.push(ConsumerArmFreshStartResult::from_existing_lease(
          virtual_partition_id,
          existing_leases
            .as_ref()
            .and_then(|leases| leases.get(&virtual_partition_id).cloned()),
          self.metadata_window_size,
        )?);
        continue;
      }
      let marker_id = uuid::Uuid::new_v4().to_string();
      let outcome = self
        .lease_store
        .arm_next_window_fresh_start(&key, self.metadata_window_size, marker_id, now)
        .await?;
      results.push(ConsumerArmFreshStartResult::from_arm_outcome(
        virtual_partition_id,
        outcome,
      ));
    }
    Ok(results)
  }
}

//
// ConsumerArmFreshStartResult
//

#[derive(Debug, Serialize)]
/// One partition result from the fresh-start marker administration endpoint.
pub struct ConsumerArmFreshStartResult {
  pub virtual_partition_id: VirtualPartitionId,
  pub outcome: ConsumerArmFreshStartResultOutcome,
  pub target_window_start: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
/// Operator-visible outcome for one fresh-start marker request.
pub enum ConsumerArmFreshStartResultOutcome {
  Armed,
  AlreadyArmed,
  MissingSourceCheckpoint,
  MissingLease,
  WouldArm,
}

impl ConsumerArmFreshStartResult {
  fn from_arm_outcome(
    virtual_partition_id: VirtualPartitionId,
    outcome: ConsumerGroupArmFreshStartOutcome,
  ) -> Self {
    match outcome {
      ConsumerGroupArmFreshStartOutcome::Armed(lease) => Self {
        virtual_partition_id,
        outcome: ConsumerArmFreshStartResultOutcome::Armed,
        target_window_start: lease
          .fresh_start_marker
          .map(|marker| offset_datetime_from_unix_seconds(marker.target_window_start_unix_seconds)),
      },
      ConsumerGroupArmFreshStartOutcome::AlreadyArmed(lease) => Self {
        virtual_partition_id,
        outcome: ConsumerArmFreshStartResultOutcome::AlreadyArmed,
        target_window_start: lease
          .fresh_start_marker
          .map(|marker| offset_datetime_from_unix_seconds(marker.target_window_start_unix_seconds)),
      },
      ConsumerGroupArmFreshStartOutcome::MissingSourceCheckpoint(_) => Self {
        virtual_partition_id,
        outcome: ConsumerArmFreshStartResultOutcome::MissingSourceCheckpoint,
        target_window_start: None,
      },
      ConsumerGroupArmFreshStartOutcome::MissingLease => Self {
        virtual_partition_id,
        outcome: ConsumerArmFreshStartResultOutcome::MissingLease,
        target_window_start: None,
      },
    }
  }

  fn from_existing_lease(
    virtual_partition_id: VirtualPartitionId,
    lease: Option<ConsumerGroupLease>,
    metadata_window_size: time::Duration,
  ) -> Result<Self, anyhow::Error> {
    let Some(lease) = lease else {
      return Ok(Self {
        virtual_partition_id,
        outcome: ConsumerArmFreshStartResultOutcome::MissingLease,
        target_window_start: None,
      });
    };
    if let Some(marker) = lease.fresh_start_marker {
      return Ok(Self {
        virtual_partition_id,
        outcome: ConsumerArmFreshStartResultOutcome::AlreadyArmed,
        target_window_start: Some(offset_datetime_from_unix_seconds(
          marker.target_window_start_unix_seconds,
        )),
      });
    }
    let Some(source_checkpoint) = lease
      .committed_cursor
      .and_then(|cursor| cursor.source_checkpoint)
    else {
      return Ok(Self {
        virtual_partition_id,
        outcome: ConsumerArmFreshStartResultOutcome::MissingSourceCheckpoint,
        target_window_start: None,
      });
    };
    let target_window_start = source_checkpoint
      .window_start_unix_seconds
      .checked_add(metadata_window_size.whole_seconds())
      .ok_or_else(|| anyhow::anyhow!("fresh start target window overflow"))?;
    Ok(Self {
      virtual_partition_id,
      outcome: ConsumerArmFreshStartResultOutcome::WouldArm,
      target_window_start: Some(offset_datetime_from_unix_seconds(target_window_start)),
    })
  }
}

pub fn assignment_plan_snapshot(
  plan: ConsumerGroupAssignmentPlan,
) -> ConsumerAssignmentPlanSnapshot {
  let member_pods = plan
    .member_topology
    .as_ref()
    .map(|members| {
      members
        .iter()
        .filter_map(|member| {
          member
            .pod_id
            .as_ref()
            .map(|pod_id| (member.member_id.clone(), pod_id.clone()))
        })
        .collect::<BTreeMap<_, _>>()
    })
    .unwrap_or_default();
  let mut pod_loads = BTreeMap::new();
  for pod_id in member_pods.values() {
    pod_loads.entry(pod_id.clone()).or_insert(0_usize);
  }
  for assignment in &plan.assignments {
    if let Some(pod_id) = member_pods.get(&assignment.member_id) {
      *pod_loads.entry(pod_id.clone()).or_insert(0_usize) += 1;
    }
  }
  ConsumerAssignmentPlanSnapshot {
    version: plan.version,
    planner_member_id: plan.planner_member_id,
    policy: if plan.member_topology.is_some() {
      ConsumerAssignmentPolicy::PodAware
    } else {
      ConsumerAssignmentPolicy::FlatMember
    },
    members: plan.members,
    member_topology: member_pods
      .iter()
      .map(|(member_id, pod_id)| ConsumerMemberTopologySnapshot {
        member_id: member_id.clone(),
        pod_id: pod_id.clone(),
      })
      .collect(),
    pod_loads: pod_loads
      .into_iter()
      .map(|(pod_id, partition_count)| ConsumerPodLoadSnapshot {
        pod_id,
        partition_count,
      })
      .collect(),
    assignments: plan
      .assignments
      .into_iter()
      .map(|assignment| ConsumerPartitionAssignmentSnapshot {
        virtual_partition_id: assignment.virtual_partition_id,
        pod_id: member_pods.get(&assignment.member_id).cloned(),
        member_id: assignment.member_id,
      })
      .collect(),
    published_at: offset_datetime_from_unix_millis(plan.published_ts_ms),
  }
}

pub fn group_lease_observation(
  assignment_plan: Option<&ConsumerAssignmentPlanSnapshot>,
  leases: Vec<ConsumerGroupLease>,
) -> ConsumerGroupLeaseObservation {
  let desired_owners = assignment_plan
    .map(|plan| {
      plan
        .assignments
        .iter()
        .map(|assignment| {
          (
            assignment.virtual_partition_id,
            assignment.member_id.clone(),
          )
        })
        .collect::<HashMap<_, _>>()
    })
    .unwrap_or_default();
  let mut partitions = leases
    .into_iter()
    .map(|lease| {
      let virtual_partition_id = lease.key.virtual_partition_id;
      group_partition_lease_snapshot(
        virtual_partition_id,
        desired_owners.get(&virtual_partition_id).cloned(),
        Some(lease),
      )
    })
    .collect::<Vec<_>>();

  for (virtual_partition_id, desired_owner_id) in desired_owners {
    if !partitions
      .iter()
      .any(|partition| partition.virtual_partition_id == virtual_partition_id)
    {
      partitions.push(group_partition_lease_snapshot(
        virtual_partition_id,
        Some(desired_owner_id),
        None,
      ));
    }
  }
  partitions.sort_by_key(|partition| partition.virtual_partition_id);
  ConsumerGroupLeaseObservation::Fresh { partitions }
}

fn group_partition_lease_snapshot(
  virtual_partition_id: VirtualPartitionId,
  desired_owner_id: Option<String>,
  lease: Option<ConsumerGroupLease>,
) -> ConsumerGroupPartitionLeaseSnapshot {
  let Some(lease) = lease else {
    return ConsumerGroupPartitionLeaseSnapshot {
      virtual_partition_id,
      desired_owner_id,
      owner_id: None,
      generation: None,
      lease_expiration_at: None,
      last_heartbeat_at: None,
      committed_offset: None,
      committed_source_checkpoint: None,
      committed_at: None,
      fresh_start_marker: None,
    };
  };

  ConsumerGroupPartitionLeaseSnapshot {
    virtual_partition_id,
    desired_owner_id,
    owner_id: Some(lease.owner_id),
    generation: Some(lease.generation),
    lease_expiration_at: Some(offset_datetime_from_unix_millis(
      lease.lease_expiration_ts_ms,
    )),
    last_heartbeat_at: Some(offset_datetime_from_unix_millis(lease.last_heartbeat_ts_ms)),
    committed_offset: lease.committed_cursor.as_ref().map(|cursor| cursor.seq_end),
    committed_source_checkpoint: lease
      .committed_cursor
      .and_then(|cursor| cursor.source_checkpoint)
      .map(|checkpoint| ConsumerSourceCheckpointSnapshot {
        window_start: offset_datetime_from_unix_seconds(checkpoint.window_start_unix_seconds),
        snowflake_id: checkpoint.snowflake_id,
      }),
    committed_at: lease.committed_ts_ms.map(offset_datetime_from_unix_millis),
    fresh_start_marker: lease
      .fresh_start_marker
      .map(|marker| ConsumerFreshStartMarkerSnapshot {
        marker_id: marker.marker_id,
        source_checkpoint: source_checkpoint_snapshot(&marker.source_checkpoint),
        target_window_start: offset_datetime_from_unix_seconds(
          marker.target_window_start_unix_seconds,
        ),
        armed_at: offset_datetime_from_unix_millis(marker.armed_at_ts_ms),
      }),
  }
}

fn source_checkpoint_snapshot(
  checkpoint: &CommittedSourceCheckpoint,
) -> ConsumerSourceCheckpointSnapshot {
  ConsumerSourceCheckpointSnapshot {
    window_start: offset_datetime_from_unix_seconds(checkpoint.window_start_unix_seconds),
    snowflake_id: checkpoint.snowflake_id,
  }
}

fn local_partition_snapshot(
  snapshots: &mut BTreeMap<VirtualPartitionId, ConsumerLocalPartitionSnapshot>,
  virtual_partition_id: VirtualPartitionId,
) -> &mut ConsumerLocalPartitionSnapshot {
  snapshots
    .entry(virtual_partition_id)
    .or_insert(ConsumerLocalPartitionSnapshot {
      virtual_partition_id,
      owned: false,
      active: false,
      pending_assignment: false,
      pending_commit_offset: None,
      last_committed_offset: None,
      last_committed_source_checkpoint: None,
      last_committed_at: None,
      cursor: None,
      reader: None,
      last_scan: None,
      prefetch_buffered_batch_count: 0,
      prefetch_buffered_record_count: 0,
    })
}

fn serialize_rfc3339_timestamp_vec<S>(
  timestamps: &[OffsetDateTime],
  serializer: S,
) -> Result<S::Ok, S::Error>
where
  S: Serializer,
{
  timestamps
    .iter()
    .map(Rfc3339Timestamp)
    .collect::<Vec<_>>()
    .serialize(serializer)
}

struct Rfc3339Timestamp<'a>(&'a OffsetDateTime);

impl Serialize for Rfc3339Timestamp<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    time::serde::rfc3339::serialize(self.0, serializer)
  }
}

pub fn offsets_from_map(offsets: &HashMap<VirtualPartitionId, u64>) -> Vec<ConsumerOffsetSnapshot> {
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

pub fn emit_partition_handoff_snapshots(
  snapshot: &ConsumerStateSnapshot,
  partition_ids: &[VirtualPartitionId],
  phase: &str,
  outcome: &str,
  commit_outcome: &str,
  parent_span: &Span,
) {
  let partition_ids = partition_ids.iter().copied().collect::<HashSet<_>>();
  for partition in &snapshot.local.partitions {
    if !partition_ids.contains(&partition.virtual_partition_id) {
      continue;
    }
    let handoff_snapshot_json = match serde_json::to_string(partition) {
      Ok(snapshot) => snapshot,
      Err(error) => format!(r#"{{"serialization_error":"{error}"}}"#),
    };
    let cursor_key = handoff_cursor_key(snapshot, partition);
    let reader_mode = partition.reader.as_ref().map(|reader| &reader.mode);
    let recovery_next_window_start = partition
      .reader
      .as_ref()
      .and_then(|reader| reader.recovery_next_window_start.as_ref());
    let recovery_cutover_window_start = partition
      .reader
      .as_ref()
      .and_then(|reader| reader.recovery_cutover_window_start.as_ref());
    parent_span.in_scope(|| {
      // bd-log exports at most 16 attributes per span by default. Keep the scalar fields useful
      // for trace queries here and retain the complete per-partition state in the JSON attribute.
      let _handoff_span = bd_log::otel_info_span_if_parent!(
        "blob_stream.consumer.partition_handoff",
        otel.kind = "internal",
        handoff.phase = phase,
        handoff.outcome = outcome,
        handoff.commit_outcome = commit_outcome,
        handoff.cursor_key = %cursor_key,
        consumer.topic = %snapshot.topic,
        consumer.group_id = %snapshot.group_id,
        consumer.member_id = %snapshot.member_id,
        consumer.generation = snapshot.accepted_assignment_plan_version,
        messaging.partition = partition.virtual_partition_id,
        handoff.last_committed_offset = ?partition.last_committed_offset,
        handoff.reader_mode = ?reader_mode,
        handoff.recovery_next_window_start = ?recovery_next_window_start,
        handoff.recovery_cutover_window_start = ?recovery_cutover_window_start,
        handoff.snapshot_json = %handoff_snapshot_json,
      );
    });
  }
}

pub fn handoff_cursor_key(
  snapshot: &ConsumerStateSnapshot,
  partition: &ConsumerLocalPartitionSnapshot,
) -> String {
  let checkpoint = partition
    .last_committed_source_checkpoint
    .as_ref()
    .map_or_else(
      || "none".to_string(),
      |checkpoint| format!("{}:{}", checkpoint.window_start, checkpoint.snowflake_id),
    );
  format!(
    "{}:{}:{}:{}:{}",
    snapshot.topic,
    snapshot.group_id,
    partition.virtual_partition_id,
    partition
      .last_committed_offset
      .or(partition.cursor)
      .map_or_else(|| "none".to_string(), |offset| offset.to_string()),
    checkpoint
  )
}
