use crate::config::ConsumerGroupConfig;
use crate::iterator::{ConsumerDeliveryState, ConsumerSharedState};
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentPlan,
  ConsumerGroupLease,
  ConsumerGroupLeaseStore,
};
use blob_stream_types::{VirtualPartitionId, format_unix_timestamp_ms, now_unix_millis};
use log::debug;
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

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
  pub generated_at: String,
  pub topic: String,
  pub group_id: String,
  pub member_id: String,
  pub started: bool,
  /// Version of the assignment plan most recently accepted by this process.
  pub accepted_assignment_plan_version: u64,
  pub assignment_plan: Option<ConsumerAssignmentPlanSnapshot>,
  pub local: ConsumerLocalStateSnapshot,
  pub next_heartbeat_at: String,
  pub next_rebalance_at: String,
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
  pub last_successful_heartbeat_at: Option<String>,
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
  pub recovery_next_window_start: Option<String>,
  pub recovery_cutover_window_start: Option<String>,
}

//
// ConsumerReaderScanSnapshot
//

/// Most recent successful scan outcome for one local reader partition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConsumerReaderScanSnapshot {
  pub completed_at: String,
  pub scanned_window_starts: Vec<String>,
  pub fast_scan_bounds: Vec<ConsumerReaderFastScanBoundSnapshot>,
  pub fast_frontiers: Vec<ConsumerReaderFastFrontierSnapshot>,
  pub cursor_before: Option<u64>,
  pub cursor_after: Option<u64>,
  pub metadata_segments_seen: usize,
  pub metadata_segments_without_partition_batches: usize,
  pub metadata_batches_seen: usize,
  pub metadata_batches_skipped_by_cursor: usize,
  pub metadata_segments_skipped_by_frontier: usize,
  pub metadata_segments_deferred_by_visibility: usize,
  pub metadata_segments_blocked_by_visibility: usize,
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
  pub window_start: String,
  pub floor_timestamp: String,
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
  pub window_start: String,
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
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct ConsumerSourceCheckpointSnapshot {
  pub window_start: String,
  pub snowflake_id: u64,
}

#[derive(Debug, Serialize)]
/// Desired and observed lease state for one consumer-group virtual partition.
pub struct ConsumerGroupPartitionLeaseSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub desired_owner_id: Option<String>,
  pub owner_id: Option<String>,
  pub generation: Option<u64>,
  pub lease_expiration_at: Option<String>,
  pub last_heartbeat_at: Option<String>,
  pub committed_offset: Option<u64>,
  pub committed_source_checkpoint: Option<ConsumerSourceCheckpointSnapshot>,
  pub committed_at: Option<String>,
}

//
// ConsumerAssignmentPlanSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Shared desired group ownership plan observed by this iterator.
pub struct ConsumerAssignmentPlanSnapshot {
  pub version: u64,
  pub planner_member_id: String,
  pub members: Vec<String>,
  pub assignments: Vec<ConsumerPartitionAssignmentSnapshot>,
  pub published_at: String,
}

//
// ConsumerPartitionAssignmentSnapshot
//

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Desired shared-plan owner for one virtual partition.
pub struct ConsumerPartitionAssignmentSnapshot {
  pub virtual_partition_id: VirtualPartitionId,
  pub member_id: String,
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
  pub recovery_next_window_start: Option<String>,
  pub recovery_cutover_window_start: Option<String>,
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
  pub last_committed_offsets: HashMap<VirtualPartitionId, u64>,
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
}

impl ConsumerDiagnostics {
  pub fn new(
    group_config: ConsumerGroupConfig,
    shared_state: Arc<Mutex<ConsumerSharedState>>,
    prefetch_max_bytes: u64,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
  ) -> Self {
    Self {
      group_config,
      shared_state,
      prefetch_max_bytes,
      lease_store,
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
        shared_state.pending_commits.clone(),
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
    for (partition_id, offset) in runtime_state.last_committed_offsets {
      local_partition_snapshot(&mut local_partitions, partition_id).last_committed_offset =
        Some(offset);
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
      generated_at: format_unix_timestamp_ms(now_unix_millis()),
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
          .map(format_unix_timestamp_ms),
        partitions: local_partitions.into_values().collect(),
      },
      next_heartbeat_at: format_unix_timestamp_ms(runtime_state.next_heartbeat_at_ms),
      next_rebalance_at: format_unix_timestamp_ms(runtime_state.next_rebalance_at_ms),
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

    state.generated_at = format_unix_timestamp_ms(now_unix_millis());
    ConsumerStateResponse {
      state,
      group_lease_observation,
    }
  }

  pub fn admin_router(self) -> axum::Router {
    crate::admin::router(self)
  }
}

pub fn assignment_plan_snapshot(
  plan: ConsumerGroupAssignmentPlan,
) -> ConsumerAssignmentPlanSnapshot {
  ConsumerAssignmentPlanSnapshot {
    version: plan.version,
    planner_member_id: plan.planner_member_id,
    members: plan.members,
    assignments: plan
      .assignments
      .into_iter()
      .map(|assignment| ConsumerPartitionAssignmentSnapshot {
        virtual_partition_id: assignment.virtual_partition_id,
        member_id: assignment.member_id,
      })
      .collect(),
    published_at: format_unix_timestamp_ms(plan.published_ts_ms),
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
    };
  };

  ConsumerGroupPartitionLeaseSnapshot {
    virtual_partition_id,
    desired_owner_id,
    owner_id: Some(lease.owner_id),
    generation: Some(lease.generation),
    lease_expiration_at: Some(format_unix_timestamp_ms(lease.lease_expiration_ts_ms)),
    last_heartbeat_at: Some(format_unix_timestamp_ms(lease.last_heartbeat_ts_ms)),
    committed_offset: lease.committed_cursor.as_ref().map(|cursor| cursor.seq_end),
    committed_source_checkpoint: lease
      .committed_cursor
      .and_then(|cursor| cursor.source_checkpoint)
      .map(|checkpoint| ConsumerSourceCheckpointSnapshot {
        window_start: format_unix_timestamp_ms(
          checkpoint.window_start_unix_seconds.saturating_mul(1_000),
        ),
        snowflake_id: checkpoint.snowflake_id,
      }),
    committed_at: lease.committed_ts_ms.map(format_unix_timestamp_ms),
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
      cursor: None,
      reader: None,
      last_scan: None,
      prefetch_buffered_batch_count: 0,
      prefetch_buffered_record_count: 0,
    })
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
