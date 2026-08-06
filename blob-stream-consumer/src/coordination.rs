#[cfg(test)]
#[path = "./coordination_test.rs"]
mod tests;

#[path = "./coordination/assignment.rs"]
mod assignment;

use crate::config::{ConsumerGroupConfig, consumer_lease_duration_ms, validate_group_config};
use anyhow::{Error, Result};
pub use assignment::cooperative_sticky_assignment;
use assignment::{
  assignment_plan,
  assignment_plan_pod_ids,
  assignment_plan_policy,
  canonical_member_topology,
  cooperative_sticky_assignment_with_pods,
};
use async_trait::async_trait;
use bd_log::warn_every;
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLease,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupLeaseTransition,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLeaseOutcome,
  ConsumerGroupReleaseOutcome,
};
use blob_stream_types::{CommittedCursor, VirtualPartitionId, format_unix_timestamp_ms};
use futures::{StreamExt, stream};
use log::{debug, info, trace};

//
// LeaseClaimCounts
//

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Successful lease claim transitions observed while applying a rebalance.
pub struct LeaseClaimCounts {
  /// Claims for previously absent lease rows.
  pub initial: usize,
  /// Claims that retained the same owner.
  pub retained: usize,
  /// Claims following explicit release by the previous owner.
  pub graceful_handoffs: usize,
  /// Claims after a previous owner lease expired without explicit release.
  pub expiry_takeovers: usize,
}

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use time::ext::NumericalDuration;
use uuid::Uuid;

const MAX_CONCURRENT_PARTITION_LEASE_OPERATIONS: usize = 16;

#[derive(Clone, Copy)]
enum LeaseMaintenanceOperation {
  Heartbeat,
  CommitCursor,
}

enum LeaseMaintenanceOutcome {
  Renewed(ConsumerGroupLease),
  HeldByOther(ConsumerGroupLease),
  Expired,
}

//
// Coordination algorithm overview
//
// This module implements consumer-group coordination with a clear split between:
//
// 1) Desired ownership computation (shared, durable, cooperative sticky)
// 2) Actual ownership claim/renewal (backed by ConsumerGroupLeaseStore fencing semantics)
//
// High-level flow
//
// A) Rebalance phase (`rebalance`)
//    - The elected planner builds a desired partition->member map using cooperative sticky
//      assignment, then persists it as the shared plan.
//    - If desired map changed, bump generation. Generation is the fence token carried in assignment
//      and heartbeat operations.
//    - For partitions assigned to this member, call `assign_partition` in the lease store. Only
//      successful assignments become locally owned.
//
// B) Steady-state lease maintenance (`heartbeat_and_commit`)
//    - For each locally owned partition, send heartbeat with optional cursor commit.
//    - On `Renewed`, keep ownership.
//    - On `HeldByOther` or `Expired`, drop local ownership immediately (fenced).
//
// C) Shutdown release (`release_owned`)
//    - Best-effort explicit release for currently owned partitions.
//    - This shortens scale-in convergence by avoiding lease-expiry waits.
//
// Why this avoids split-brain ownership
//
// - Ownership is never trusted from local assignment alone; it is confirmed by lease-store outcomes
//   (`Assigned`/`Renewed`).
// - Generation fencing prevents stale owners from continuing to heartbeat/commit after a newer
//   rebalance claims the same partition.
// - Local `owned` state is derived from store outcomes and actively pruned on fencing events.
//
// Sticky/cooperative behavior
//
// - Previous placements are retained when member set still allows it (stickiness).
// - Only unassigned partitions are newly allocated first.
// - A bounded rebalance step moves the minimum number of partitions from overloaded to underloaded
//   members to reach a near-even distribution.
//
// This design uses the membership store for a durable desired plan and the lease store for
// correctness-critical ownership state (leases, generation checks, and ownership fences).

//
// HeartbeatReport
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Heartbeat result summarizing renewed and fenced partitions.
pub struct HeartbeatReport {
  /// Partitions successfully renewed for this member/generation.
  pub renewed_partitions: Vec<VirtualPartitionId>,
  /// Partitions lost due to fencing or lease expiry.
  pub fenced_partitions: Vec<VirtualPartitionId>,
}

//
// RecoveredCursor
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Committed source state recovered with a newly assigned lease.
pub struct RecoveredCursor {
  /// Cursor persisted by the previous lease owner.
  pub committed_cursor: CommittedCursor,
  /// Millisecond timestamp of the commit for legacy source-less cursors.
  pub committed_ts_ms: Option<i64>,
}

//
// RebalanceReport
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Rebalance result containing owned partitions and recovered cursor state.
pub struct RebalanceReport {
  /// Partitions currently owned after rebalance.
  pub owned_partitions: Vec<VirtualPartitionId>,
  /// Earliest expiration among leases in the current assignment.
  pub active_partition_lease_expiration_deadline_ms: Option<i64>,
  /// Last committed cursor state per owned partition, when present in lease store.
  pub recovered_cursors: HashMap<VirtualPartitionId, RecoveredCursor>,
  /// Valid persisted assignment plan accepted during this rebalance.
  pub accepted_assignment_plan: Option<ConsumerGroupAssignmentPlan>,
  /// Version of the valid persisted assignment plan accepted during this rebalance.
  pub accepted_assignment_plan_version: Option<u64>,
  /// Invalid persisted plan version rejected during this rebalance, when one was observed.
  pub rejected_assignment_plan_version: Option<u64>,
  /// Whether this coordinator advanced to a newly accepted assignment plan version.
  pub assignment_plan_applied: bool,
  /// Number of partitions the accepted plan assigned to this member before lease reconciliation.
  pub desired_partitions: usize,
  /// Successful lease claim transitions observed during this rebalance.
  pub lease_claim_counts: LeaseClaimCounts,
  /// Claim failure observed after other concurrent claims completed successfully.
  ///
  /// The successful work remains in this report and must be applied before retrying only the
  /// failed claim on the next rebalance.
  pub retry_error: Option<String>,
}

//
// AssignmentPlanValidationError
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// Reason a persisted assignment plan cannot safely be applied.
pub enum AssignmentPlanValidationError {
  /// The plan does not include any members.
  EmptyMembers,
  /// The recorded planner is not one of the plan members.
  PlannerNotMember,
  /// The plan has a different number of assignments than the configured partition inventory.
  AssignmentCountMismatch { expected: usize, actual: usize },
  /// An assignment refers to a member absent from the plan membership.
  AssignmentOwnerNotMember { member_id: String },
  /// More than one assignment refers to the same partition.
  DuplicatePartitionAssignment {
    virtual_partition_id: VirtualPartitionId,
  },
  /// The plan omits a configured partition.
  MissingExpectedPartition {
    virtual_partition_id: VirtualPartitionId,
  },
  /// Pod-aware topology does not match the active member list.
  TopologyMembersMismatch,
  /// A pod-aware topology record omitted a pod identifier.
  TopologyMemberWithoutPod { member_id: String },
  /// Members differ by more than one assigned partition.
  ImbalancedLoad { min_load: usize, max_load: usize },
}

//
// SharedAssignment
//

/// Result of resolving the persisted plan, including a rejected version when a planner repairs it.
struct SharedAssignment {
  plan: Option<ConsumerGroupAssignmentPlan>,
  rejected_assignment_plan_version: Option<u64>,
}

//
// ConsumerGroupCoordinator
//

#[async_trait]
/// Consumer-group coordinator interface over a lease-backed ownership store.
pub trait ConsumerGroupCoordinator: Send + Sync {
  /// Compute and apply ownership for current members and partition set.
  async fn rebalance(
    &mut self,
    members: Vec<String>,
    partitions: Vec<VirtualPartitionId>,
    now_ts_ms: i64,
  ) -> Result<RebalanceReport>;

  /// Heartbeat currently owned partitions and optionally commit cursors.
  async fn heartbeat_and_commit(
    &mut self,
    now_ts_ms: i64,
    cursors: &HashMap<VirtualPartitionId, CommittedCursor>,
  ) -> Result<HeartbeatReport>;

  /// Commit staged cursors without renewing unrelated partition leases.
  async fn commit_cursors(
    &mut self,
    now_ts_ms: i64,
    cursors: &HashMap<VirtualPartitionId, CommittedCursor>,
  ) -> Result<HeartbeatReport>;

  /// Release specific owned partitions after their consumer has completed revocation.
  async fn release_partitions(
    &mut self,
    partitions: &[VirtualPartitionId],
    now_ts_ms: i64,
  ) -> Result<Vec<VirtualPartitionId>>;

  /// Release all owned partitions (best-effort), usually during shutdown.
  async fn release_owned(&mut self, now_ts_ms: i64) -> Result<Vec<VirtualPartitionId>>;

  /// Return current coordinator generation.
  fn generation(&self) -> u64;
  /// Return locally owned partitions.
  fn owned_partitions(&self) -> Vec<VirtualPartitionId>;
  /// Return the session that fences this coordinator's planner lease.
  fn planner_session_id(&self) -> &str;
}

//
// ConsumerGroupCoordinatorImpl
//

/// Default lease-store-backed coordinator implementation.
pub struct ConsumerGroupCoordinatorImpl {
  config: ConsumerGroupConfig,
  lease_store: Arc<dyn ConsumerGroupLeaseStore>,
  membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  planner_session_id: String,
  generation: u64,
  owned: HashMap<VirtualPartitionId, ConsumerGroupLease>,
}

impl ConsumerGroupCoordinatorImpl {
  /// Create a coordinator from group configuration and shared membership/lease stores.
  pub fn new(
    config: ConsumerGroupConfig,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
    membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  ) -> Result<Self> {
    // Validate static configuration once at construction so runtime paths stay focused on
    // coordination logic.
    validate_group_config(&config)?;
    Ok(Self {
      config,
      lease_store,
      membership_store,
      planner_session_id: Uuid::new_v4().to_string(),
      generation: 0,
      owned: HashMap::new(),
    })
  }

  async fn shared_assignment(
    &self,
    members: &[String],
    partitions: &[VirtualPartitionId],
    now_ts_ms: i64,
  ) -> Result<SharedAssignment> {
    let current_plan = self
      .membership_store
      .get_assignment_plan(&self.config.topic, &self.config.group_id)
      .await?;
    let current_plan_validation_error = current_plan
      .as_ref()
      .and_then(|plan| assignment_plan_validation_error(plan, partitions));
    if let Some(plan) = current_plan.as_ref() {
      trace!(
        "consumer assignment plan read: topic={}, group_id={}, member_id={}, version={}, \
         planner_member_id={}, members={}, assignments={}",
        self.config.topic,
        self.config.group_id,
        self.config.member_id,
        plan.version,
        plan.planner_member_id,
        plan.members.len(),
        plan.assignments.len()
      );
      if let Some(reason) = current_plan_validation_error.as_ref() {
        log_assignment_plan_rejection(&self.config, plan, partitions, reason);
      }
    } else {
      trace!(
        "consumer assignment plan absent: topic={}, group_id={}, member_id={}",
        self.config.topic, self.config.group_id, self.config.member_id
      );
    }
    let current_plan_is_valid = current_plan.is_some() && current_plan_validation_error.is_none();
    let rejected_assignment_plan_version = current_plan
      .as_ref()
      .and_then(|plan| current_plan_validation_error.as_ref().map(|_| plan.version));
    let current_members = canonical_members(members, &self.config.member_id);
    let active_members = self
      .membership_store
      .list_active_members(&self.config.topic, &self.config.group_id, now_ts_ms)
      .await?;
    let member_topology = canonical_member_topology(
      &current_members,
      &active_members,
      &self.config.member_id,
      self.config.pod_id.as_deref(),
    );
    let topology_changed = current_plan.as_ref().is_some_and(|plan| {
      plan.members != current_members || plan.member_topology != member_topology
    });
    if let Some(plan) = current_plan.as_ref()
      && current_plan_is_valid
    {
      let planner_lease = self
        .membership_store
        .get_planner_lease(&self.config.topic, &self.config.group_id)
        .await?;
      let planner_is_active = planner_lease.as_ref().is_some_and(|lease| {
        lease.member_id == plan.planner_member_id && lease.lease_expiration_ts_ms > now_ts_ms
      });
      if planner_is_active && plan.planner_member_id != self.config.member_id.as_ref() {
        debug!(
          "consumer assignment plan accepted from active planner: topic={}, group_id={}, \
           member_id={}, version={}, planner_member_id={}",
          self.config.topic,
          self.config.group_id,
          self.config.member_id,
          plan.version,
          plan.planner_member_id
        );
        return Ok(SharedAssignment {
          plan: Some(plan.clone()),
          rejected_assignment_plan_version,
        });
      }
      if planner_is_active && !topology_changed {
        let outcome = self
          .membership_store
          .acquire_or_renew_planner(
            &self.config.topic,
            &self.config.group_id,
            &self.config.member_id,
            &self.planner_session_id,
            now_ts_ms,
            consumer_lease_duration_ms(&self.config),
          )
          .await?;
        if outcome == ConsumerGroupPlannerLeaseOutcome::Acquired {
          debug!(
            "consumer assignment plan retained by planner: topic={}, group_id={}, member_id={}, \
             version={}",
            self.config.topic, self.config.group_id, self.config.member_id, plan.version
          );
          return Ok(SharedAssignment {
            plan: Some(plan.clone()),
            rejected_assignment_plan_version,
          });
        }
      }
    }

    let planner_outcome = self
      .membership_store
      .acquire_or_renew_planner(
        &self.config.topic,
        &self.config.group_id,
        &self.config.member_id,
        &self.planner_session_id,
        now_ts_ms,
        consumer_lease_duration_ms(&self.config),
      )
      .await?;
    if planner_outcome == ConsumerGroupPlannerLeaseOutcome::Acquired {
      let previous_assignment = current_plan
        .as_ref()
        .map(plan_assignment_map)
        .unwrap_or_default();
      let assignment = member_topology.as_deref().map_or_else(
        || {
          cooperative_sticky_assignment(
            members,
            partitions,
            &previous_assignment,
            &self.config.member_id,
          )
        },
        |member_topology| {
          cooperative_sticky_assignment_with_pods(member_topology, partitions, &previous_assignment)
        },
      );
      let plan = assignment_plan(
        current_plan
          .as_ref()
          .map_or(1, |current| current.version.saturating_add(1)),
        members,
        partitions,
        &assignment,
        member_topology,
        &self.config.member_id,
        now_ts_ms,
      );
      let moved_partitions = plan
        .assignments
        .iter()
        .filter(|assignment| {
          previous_assignment.get(&assignment.virtual_partition_id) != Some(&assignment.member_id)
        })
        .count();
      let pod_ids = assignment_plan_pod_ids(&plan);
      debug!(
        "consumer assignment plan publishing: topic={}, group_id={}, member_id={}, version={}, \
         policy={}, pods={:?}, members={}, assignments={}, moved_partitions={}",
        self.config.topic,
        self.config.group_id,
        self.config.member_id,
        plan.version,
        assignment_plan_policy(&plan),
        pod_ids,
        plan.members.len(),
        plan.assignments.len(),
        moved_partitions
      );
      if self
        .membership_store
        .publish_assignment_plan(
          &self.config.topic,
          &self.config.group_id,
          &self.config.member_id,
          &self.planner_session_id,
          now_ts_ms,
          plan.clone(),
        )
        .await?
      {
        info!(
          "consumer assignment plan published: topic={}, group_id={}, member_id={}, version={}, \
           policy={}, pods={:?}, members={}, partitions={}, moved_partitions={}",
          self.config.topic,
          self.config.group_id,
          self.config.member_id,
          plan.version,
          assignment_plan_policy(&plan),
          assignment_plan_pod_ids(&plan),
          plan.members.len(),
          plan.assignments.len(),
          moved_partitions
        );
        return Ok(SharedAssignment {
          plan: Some(plan),
          rejected_assignment_plan_version,
        });
      }
      debug!(
        "consumer assignment plan publication not applied: topic={}, group_id={}, member_id={}, \
         version={}",
        self.config.topic, self.config.group_id, self.config.member_id, plan.version
      );
    } else {
      debug!(
        "consumer planner lease unavailable: topic={}, group_id={}, member_id={}",
        self.config.topic, self.config.group_id, self.config.member_id
      );
    }

    let refreshed_plan = self
      .membership_store
      .get_assignment_plan(&self.config.topic, &self.config.group_id)
      .await?;
    if let Some(plan) = refreshed_plan.as_ref()
      && let Some(reason) = assignment_plan_validation_error(plan, partitions)
    {
      log_assignment_plan_rejection(&self.config, plan, partitions, &reason);
      return Ok(SharedAssignment {
        plan: None,
        rejected_assignment_plan_version: rejected_assignment_plan_version.or(Some(plan.version)),
      });
    }
    if let Some(plan) = refreshed_plan.as_ref() {
      debug!(
        "consumer assignment plan accepted after refresh: topic={}, group_id={}, member_id={}, \
         version={}",
        self.config.topic, self.config.group_id, self.config.member_id, plan.version
      );
    }
    Ok(SharedAssignment {
      plan: refreshed_plan,
      rejected_assignment_plan_version,
    })
  }

  async fn maintain_partitions(
    &mut self,
    now_ts_ms: i64,
    cursors: &HashMap<VirtualPartitionId, CommittedCursor>,
    partitions: Vec<(VirtualPartitionId, u64)>,
    operation: LeaseMaintenanceOperation,
  ) -> Result<HeartbeatReport> {
    let mut renewed = Vec::new();
    let mut fenced = Vec::new();

    // Bound independent lease writes so a large assignment cannot overwhelm the lease store.
    // Reconcile every completed outcome before returning an error from another partition.
    let partition_count = partitions.len();
    let topic = self.config.topic.to_string();
    let group_id = self.config.group_id.to_string();
    let member_id = self.config.member_id.to_string();
    let lease_duration_ms = consumer_lease_duration_ms(&self.config);
    let outcomes = stream::iter(partitions.into_iter().map(|(partition_id, generation)| {
      let lease_store = Arc::clone(&self.lease_store);
      let key = ConsumerGroupLeaseKey {
        topic: topic.clone(),
        group_id: group_id.clone(),
        virtual_partition_id: partition_id,
      };
      let member_id = member_id.clone();
      let committed_cursor = cursors.get(&partition_id).cloned();
      async move {
        let outcome = match operation {
          LeaseMaintenanceOperation::Heartbeat => match lease_store
            .heartbeat_partition(
              &key,
              &member_id,
              generation,
              now_ts_ms,
              lease_duration_ms,
              committed_cursor,
            )
            .await?
          {
            ConsumerGroupHeartbeatOutcome::Renewed(lease) => {
              LeaseMaintenanceOutcome::Renewed(lease)
            },
            ConsumerGroupHeartbeatOutcome::HeldByOther(lease) => {
              LeaseMaintenanceOutcome::HeldByOther(lease)
            },
            ConsumerGroupHeartbeatOutcome::Expired => LeaseMaintenanceOutcome::Expired,
          },
          LeaseMaintenanceOperation::CommitCursor => match lease_store
            .commit_cursor(
              &key,
              &member_id,
              generation,
              now_ts_ms,
              committed_cursor.expect("cursor commits only target staged partitions"),
            )
            .await?
          {
            ConsumerGroupCommitOutcome::Committed(lease) => LeaseMaintenanceOutcome::Renewed(lease),
            ConsumerGroupCommitOutcome::HeldByOther(lease) => {
              LeaseMaintenanceOutcome::HeldByOther(lease)
            },
            ConsumerGroupCommitOutcome::Expired => LeaseMaintenanceOutcome::Expired,
          },
        };
        Ok::<_, Error>((partition_id, outcome))
      }
    }))
    .buffer_unordered(MAX_CONCURRENT_PARTITION_LEASE_OPERATIONS)
    .collect::<Vec<_>>()
    .await;

    let mut heartbeat_error = None;
    for result in outcomes {
      let (partition_id, outcome) = match result {
        Ok(outcome) => outcome,
        Err(error) => {
          if heartbeat_error.is_none() {
            heartbeat_error = Some(error);
          }
          continue;
        },
      };
      match outcome {
        LeaseMaintenanceOutcome::Renewed(lease) => {
          self.owned.insert(partition_id, lease);
          renewed.push(partition_id);
        },
        LeaseMaintenanceOutcome::HeldByOther(lease) => {
          self.owned.remove(&partition_id);
          fenced.push(partition_id);
          info!(
            "consumer lease fenced by another member: topic={}, group_id={}, partition={}, \
             member_id={}, generation={}, owner_id={}, owner_generation={}, lease_expires_at={}, \
             last_heartbeat_at={}",
            self.config.topic,
            self.config.group_id,
            partition_id,
            self.config.member_id,
            self.generation,
            lease.owner_id,
            lease.generation,
            format_unix_timestamp_ms(lease.lease_expiration_ts_ms),
            format_unix_timestamp_ms(lease.last_heartbeat_ts_ms)
          );
        },
        LeaseMaintenanceOutcome::Expired => {
          self.owned.remove(&partition_id);
          fenced.push(partition_id);
          info!(
            "consumer lease expired before heartbeat: topic={}, group_id={}, partition={}, \
             member_id={}, generation={}, now={}",
            self.config.topic,
            self.config.group_id,
            partition_id,
            self.config.member_id,
            self.generation,
            format_unix_timestamp_ms(now_ts_ms)
          );
        },
      }
    }

    if !fenced.is_empty() {
      info!(
        "consumer fenced partitions detected: topic={}, group_id={}, member_id={}, generation={}, \
         fenced={fenced:?}",
        self.config.topic, self.config.group_id, self.config.member_id, self.generation
      );
    }

    renewed.sort_unstable();
    fenced.sort_unstable();
    debug!(
      "consumer lease maintenance completed: topic={}, group_id={}, member_id={}, generation={}, \
       partitions={}, renewed={}, fenced={}, cursor_count={}",
      self.config.topic,
      self.config.group_id,
      self.config.member_id,
      self.generation,
      partition_count,
      renewed.len(),
      fenced.len(),
      cursors.len()
    );
    if let Some(error) = heartbeat_error {
      return Err(error);
    }
    Ok(HeartbeatReport {
      renewed_partitions: renewed,
      fenced_partitions: fenced,
    })
  }
}

#[async_trait]
impl ConsumerGroupCoordinator for ConsumerGroupCoordinatorImpl {
  async fn rebalance(
    &mut self,
    members: Vec<String>,
    partitions: Vec<VirtualPartitionId>,
    now_ts_ms: i64,
  ) -> Result<RebalanceReport> {
    // Desired ownership is accepted only from a valid persisted plan. During a planner transition
    // without one, this member makes no local ownership decision.
    let SharedAssignment {
      plan: shared_plan,
      rejected_assignment_plan_version,
    } = self
      .shared_assignment(&members, &partitions, now_ts_ms)
      .await?;
    let accepted_assignment_plan_version = shared_plan.as_ref().map(|plan| plan.version);
    let assignment_plan_applied =
      accepted_assignment_plan_version.is_some_and(|plan_version| self.generation != plan_version);
    if let Some(plan) = shared_plan.as_ref() {
      if assignment_plan_applied {
        info!(
          "consumer assignment plan accepted: topic={}, group_id={}, member_id={}, version={}, \
           planner_member_id={}, policy={}, pods={:?}, members={}, assignments={}",
          self.config.topic,
          self.config.group_id,
          self.config.member_id,
          plan.version,
          plan.planner_member_id,
          assignment_plan_policy(plan),
          assignment_plan_pod_ids(plan),
          plan.members.len(),
          plan.assignments.len()
        );
      }
      self.generation = plan.version;
    }
    let desired_assignment = shared_plan
      .as_ref()
      .map(plan_assignment_map)
      .unwrap_or_default();

    // Retain revoking leases so their staged cursors can be committed before the consumer
    // acknowledges revocation. Current assignments are reported separately below.
    let member_id = self.config.member_id.to_string();
    let desired_partitions = desired_assignment
      .values()
      .filter(|owner_id| *owner_id == &member_id)
      .count();
    let mut owned_partitions = self
      .owned
      .keys()
      .filter(|partition_id| {
        desired_assignment
          .get(partition_id)
          .is_some_and(|owner_id| owner_id == &member_id)
          && self
            .owned
            .get(partition_id)
            .is_some_and(|lease| lease.generation == self.generation)
      })
      .copied()
      .collect::<HashSet<_>>();
    let mut assignment_changed = owned_partitions.len() != self.owned.len();
    let topic = self.config.topic.to_string();
    let group_id = self.config.group_id.to_string();
    let generation = self.generation;
    let lease_duration_ms = consumer_lease_duration_ms(&self.config);
    let partitions_to_claim = partitions
      .into_iter()
      .filter(|partition_id| {
        desired_assignment
          .get(partition_id)
          .is_some_and(|owner_id| owner_id == &member_id)
          && self
            .owned
            .get(partition_id)
            .is_none_or(|lease| lease.generation != generation)
      })
      .collect::<Vec<_>>();
    let outcomes = stream::iter(partitions_to_claim)
      .map(|partition_id| {
        let lease_store = Arc::clone(&self.lease_store);
        let key = ConsumerGroupLeaseKey {
          topic: topic.clone(),
          group_id: group_id.clone(),
          virtual_partition_id: partition_id,
        };
        let member_id = member_id.clone();
        async move {
          let outcome = lease_store
            .assign_partition(key, member_id, generation, now_ts_ms, lease_duration_ms)
            .await?;
          Ok::<_, Error>((partition_id, outcome))
        }
      })
      .buffer_unordered(MAX_CONCURRENT_PARTITION_LEASE_OPERATIONS)
      .collect::<Vec<_>>()
      .await;

    let mut recovered_cursors = HashMap::new();
    let mut assignment_error = None;
    let mut lease_claim_counts = LeaseClaimCounts::default();
    for result in outcomes {
      let (partition_id, outcome) = match result {
        Ok(outcome) => outcome,
        Err(error) => {
          if assignment_error.is_none() {
            assignment_error = Some(error);
          }
          continue;
        },
      };
      // Track only partitions that the lease store actually granted to this member.
      match outcome {
        ConsumerGroupAssignmentOutcome::Assigned {
          lease, transition, ..
        } => {
          match transition {
            ConsumerGroupLeaseTransition::Initial => lease_claim_counts.initial += 1,
            ConsumerGroupLeaseTransition::Retained => lease_claim_counts.retained += 1,
            ConsumerGroupLeaseTransition::GracefulHandoff { .. } => {
              lease_claim_counts.graceful_handoffs += 1;
            },
            ConsumerGroupLeaseTransition::ExpiryTakeover {
              previous_owner_id,
              previous_generation,
              previous_last_heartbeat_ts_ms,
            } => {
              lease_claim_counts.expiry_takeovers += 1;
              info!(
                "consumer lease takeover after expiry: topic={}, group_id={}, partition={}, \
                 member_id={}, generation={}, previous_owner_id={}, previous_generation={}, \
                 previous_last_heartbeat_at={}",
                self.config.topic,
                self.config.group_id,
                partition_id,
                self.config.member_id,
                generation,
                previous_owner_id,
                previous_generation,
                format_unix_timestamp_ms(previous_last_heartbeat_ts_ms),
              );
            },
          }
          assignment_changed |= owned_partitions.insert(partition_id);
          self.owned.insert(partition_id, lease.clone());
          if let Some(committed_cursor) = lease.committed_cursor {
            recovered_cursors.insert(
              partition_id,
              RecoveredCursor {
                committed_cursor,
                committed_ts_ms: lease.committed_ts_ms,
              },
            );
          }
        },
        ConsumerGroupAssignmentOutcome::HeldByOther(lease) => {
          assignment_changed |= owned_partitions.remove(&partition_id);
          self.owned.remove(&partition_id);
          debug!(
            "consumer assignment held by another member: topic={}, group_id={}, partition={}, \
             member_id={}, generation={}, owner_id={}, owner_generation={}, lease_expires_at={}, \
             last_heartbeat_at={}",
            self.config.topic,
            self.config.group_id,
            partition_id,
            self.config.member_id,
            self.generation,
            lease.owner_id,
            lease.generation,
            format_unix_timestamp_ms(lease.lease_expiration_ts_ms),
            format_unix_timestamp_ms(lease.last_heartbeat_ts_ms)
          );
        },
      }
    }

    // Stable ordering helps deterministic tests and predictable downstream behavior.
    let mut owned = owned_partitions.into_iter().collect::<Vec<_>>();
    owned.sort_unstable();
    let active_partition_lease_expiration_deadline_ms = owned
      .iter()
      .filter_map(|partition_id| {
        self
          .owned
          .get(partition_id)
          .map(|lease| lease.lease_expiration_ts_ms)
      })
      .min();
    if assignment_changed {
      info!(
        "consumer rebalance applied: topic={}, group_id={}, member_id={}, \
         accepted_assignment_plan_version={accepted_assignment_plan_version:?}, owned={}",
        self.config.topic,
        self.config.group_id,
        self.config.member_id,
        owned.len()
      );
    }
    debug!(
      "consumer rebalance completed: topic={}, group_id={}, member_id={}, \
       accepted_assignment_plan_version={accepted_assignment_plan_version:?}, \
       rejected_assignment_plan_version={rejected_assignment_plan_version:?}, members={}, \
       desired_partitions={}, owned={}",
      self.config.topic,
      self.config.group_id,
      self.config.member_id,
      members.len(),
      desired_partitions,
      owned.len()
    );
    Ok(RebalanceReport {
      owned_partitions: owned,
      active_partition_lease_expiration_deadline_ms,
      recovered_cursors,
      accepted_assignment_plan: shared_plan,
      accepted_assignment_plan_version,
      rejected_assignment_plan_version,
      assignment_plan_applied,
      desired_partitions,
      lease_claim_counts,
      retry_error: assignment_error.map(|error| format!("{error:#}")),
    })
  }

  async fn heartbeat_and_commit(
    &mut self,
    now_ts_ms: i64,
    cursors: &HashMap<VirtualPartitionId, CommittedCursor>,
  ) -> Result<HeartbeatReport> {
    let owned = self
      .owned
      .iter()
      .map(|(partition_id, lease)| (*partition_id, lease.generation))
      .collect::<Vec<_>>();
    self
      .maintain_partitions(
        now_ts_ms,
        cursors,
        owned,
        LeaseMaintenanceOperation::Heartbeat,
      )
      .await
  }

  async fn commit_cursors(
    &mut self,
    now_ts_ms: i64,
    cursors: &HashMap<VirtualPartitionId, CommittedCursor>,
  ) -> Result<HeartbeatReport> {
    let owned = cursors
      .keys()
      .filter_map(|partition_id| {
        self
          .owned
          .get(partition_id)
          .map(|lease| (*partition_id, lease.generation))
      })
      .collect::<Vec<_>>();
    self
      .maintain_partitions(
        now_ts_ms,
        cursors,
        owned,
        LeaseMaintenanceOperation::CommitCursor,
      )
      .await
  }

  async fn release_partitions(
    &mut self,
    partitions: &[VirtualPartitionId],
    now_ts_ms: i64,
  ) -> Result<Vec<VirtualPartitionId>> {
    let mut released = Vec::new();

    for partition_id in partitions {
      let Some(lease) = self.owned.get(partition_id) else {
        continue;
      };
      let key = ConsumerGroupLeaseKey {
        topic: self.config.topic.to_string(),
        group_id: self.config.group_id.to_string(),
        virtual_partition_id: *partition_id,
      };

      let outcome = self
        .lease_store
        .release_partition(&key, &self.config.member_id, lease.generation, now_ts_ms)
        .await?;

      match outcome {
        ConsumerGroupReleaseOutcome::Released => {
          self.owned.remove(partition_id);
          released.push(*partition_id);
        },
        ConsumerGroupReleaseOutcome::HeldByOther(_) | ConsumerGroupReleaseOutcome::Expired => {
          self.owned.remove(partition_id);
        },
      }
    }

    released.sort_unstable();
    Ok(released)
  }

  async fn release_owned(&mut self, now_ts_ms: i64) -> Result<Vec<VirtualPartitionId>> {
    let owned = self.owned.keys().copied().collect::<Vec<_>>();
    self.release_partitions(&owned, now_ts_ms).await
  }

  fn generation(&self) -> u64 {
    self.generation
  }

  fn owned_partitions(&self) -> Vec<VirtualPartitionId> {
    let mut owned = self.owned.keys().copied().collect::<Vec<_>>();
    owned.sort_unstable();
    owned
  }

  fn planner_session_id(&self) -> &str {
    &self.planner_session_id
  }
}

//
// cooperative_sticky_assignment
//

fn assignment_plan_validation_error(
  plan: &ConsumerGroupAssignmentPlan,
  partitions: &[VirtualPartitionId],
) -> Option<AssignmentPlanValidationError> {
  if plan.members.is_empty() {
    return Some(AssignmentPlanValidationError::EmptyMembers);
  }
  if !plan
    .members
    .iter()
    .any(|member| member == &plan.planner_member_id)
  {
    return Some(AssignmentPlanValidationError::PlannerNotMember);
  }

  let mut expected_partitions = partitions.to_vec();
  expected_partitions.sort_unstable();
  expected_partitions.dedup();
  if plan.assignments.len() != expected_partitions.len() {
    return Some(AssignmentPlanValidationError::AssignmentCountMismatch {
      expected: expected_partitions.len(),
      actual: plan.assignments.len(),
    });
  }

  let member_set = plan.members.iter().collect::<HashSet<_>>();
  let mut assignment_partitions = HashSet::new();
  let mut load = plan
    .members
    .iter()
    .map(|member| (member, 0_usize))
    .collect::<HashMap<_, _>>();
  for assignment in &plan.assignments {
    if !member_set.contains(&assignment.member_id) {
      return Some(AssignmentPlanValidationError::AssignmentOwnerNotMember {
        member_id: assignment.member_id.clone(),
      });
    }
    if !assignment_partitions.insert(assignment.virtual_partition_id) {
      return Some(
        AssignmentPlanValidationError::DuplicatePartitionAssignment {
          virtual_partition_id: assignment.virtual_partition_id,
        },
      );
    }
    if let Some(member_load) = load.get_mut(&assignment.member_id) {
      *member_load += 1;
    }
  }
  if !expected_partitions
    .iter()
    .all(|partition| assignment_partitions.contains(partition))
  {
    let virtual_partition_id = expected_partitions
      .iter()
      .find(|partition| !assignment_partitions.contains(partition))
      .copied()
      .expect("assignment completeness check found no missing partition");
    return Some(AssignmentPlanValidationError::MissingExpectedPartition {
      virtual_partition_id,
    });
  }

  let Some(member_topology) = &plan.member_topology else {
    let min_load = load.values().min().copied().unwrap_or_default();
    let max_load = load.values().max().copied().unwrap_or_default();
    return (max_load.saturating_sub(min_load) > 1)
      .then_some(AssignmentPlanValidationError::ImbalancedLoad { min_load, max_load });
  };

  if member_topology.len() != plan.members.len() {
    return Some(AssignmentPlanValidationError::TopologyMembersMismatch);
  }
  let mut member_pods = HashMap::new();
  for member in member_topology {
    let Some(pod_id) = member.pod_id.as_ref() else {
      return Some(AssignmentPlanValidationError::TopologyMemberWithoutPod {
        member_id: member.member_id.clone(),
      });
    };
    if member_pods
      .insert(member.member_id.as_str(), pod_id.as_str())
      .is_some()
    {
      return Some(AssignmentPlanValidationError::TopologyMembersMismatch);
    }
  }
  if member_pods.len() != member_set.len()
    || !member_set
      .iter()
      .all(|member_id| member_pods.contains_key(member_id.as_str()))
  {
    return Some(AssignmentPlanValidationError::TopologyMembersMismatch);
  }

  let mut pod_loads = HashMap::<&str, usize>::new();
  let mut pod_member_loads = HashMap::<&str, Vec<usize>>::new();
  for member_id in &plan.members {
    let pod_id = member_pods
      .get(member_id.as_str())
      .expect("validated member topology has every member");
    pod_member_loads
      .entry(pod_id)
      .or_default()
      .push(*load.get(member_id).unwrap_or(&0));
    pod_loads.entry(pod_id).or_default();
  }
  for assignment in &plan.assignments {
    let pod_id = member_pods
      .get(assignment.member_id.as_str())
      .expect("validated assignment owner has topology");
    *pod_loads.entry(pod_id).or_default() += 1;
  }
  let min_pod_load = pod_loads.values().min().copied().unwrap_or_default();
  let max_pod_load = pod_loads.values().max().copied().unwrap_or_default();
  if max_pod_load.saturating_sub(min_pod_load) > 1 {
    return Some(AssignmentPlanValidationError::ImbalancedLoad {
      min_load: min_pod_load,
      max_load: max_pod_load,
    });
  }
  for member_loads in pod_member_loads.values() {
    let min_member_load = member_loads.iter().min().copied().unwrap_or_default();
    let max_member_load = member_loads.iter().max().copied().unwrap_or_default();
    if max_member_load.saturating_sub(min_member_load) > 1 {
      return Some(AssignmentPlanValidationError::ImbalancedLoad {
        min_load: min_member_load,
        max_load: max_member_load,
      });
    }
  }

  None
}

fn log_assignment_plan_rejection(
  config: &ConsumerGroupConfig,
  plan: &ConsumerGroupAssignmentPlan,
  partitions: &[VirtualPartitionId],
  reason: &AssignmentPlanValidationError,
) {
  warn_every!(
    15.seconds(),
    "consumer assignment plan rejected: topic={}, group_id={}, member_id={}, version={}, \
     planner_member_id={}, members={}, assignments={}, expected_partitions={}, reason={reason:?}",
    config.topic,
    config.group_id,
    config.member_id,
    plan.version,
    plan.planner_member_id,
    plan.members.len(),
    plan.assignments.len(),
    partitions.iter().collect::<HashSet<_>>().len()
  );
}

fn plan_assignment_map(plan: &ConsumerGroupAssignmentPlan) -> HashMap<VirtualPartitionId, String> {
  plan
    .assignments
    .iter()
    .map(|assignment| {
      (
        assignment.virtual_partition_id,
        assignment.member_id.clone(),
      )
    })
    .collect()
}

fn canonical_members(members: &[String], local_member_id: &str) -> Vec<String> {
  let mut canonical = members
    .iter()
    .filter(|member| !member.trim().is_empty())
    .cloned()
    .collect::<Vec<_>>();
  if !canonical.iter().any(|member| member == local_member_id) {
    canonical.push(local_member_id.to_string());
  }
  canonical.sort();
  canonical.dedup();
  canonical
}
