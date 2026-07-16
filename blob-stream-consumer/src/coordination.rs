#[cfg(test)]
#[path = "./coordination_test.rs"]
mod tests;

use crate::config::{ConsumerGroupConfig, consumer_lease_duration_ms, validate_group_config};
use anyhow::Result;
use async_trait::async_trait;
use blob_stream_metadata_store::{
  ConsumerGroupAssignment,
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  ConsumerGroupPlannerLeaseOutcome,
  ConsumerGroupReleaseOutcome,
};
use blob_stream_types::{CommittedCursor, VirtualPartitionId};
use log::{debug, info};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
  /// Last committed cursor state per owned partition, when present in lease store.
  pub recovered_cursors: HashMap<VirtualPartitionId, RecoveredCursor>,
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

  /// Release all owned partitions (best-effort), usually during shutdown.
  async fn release_owned(&mut self, now_ts_ms: i64) -> Result<Vec<VirtualPartitionId>>;

  /// Return current coordinator generation.
  fn generation(&self) -> u64;
  /// Return locally owned partitions.
  fn owned_partitions(&self) -> Vec<VirtualPartitionId>;
}

//
// ConsumerGroupCoordinatorImpl
//

/// Default lease-store-backed coordinator implementation.
pub struct ConsumerGroupCoordinatorImpl {
  config: ConsumerGroupConfig,
  lease_store: Arc<dyn ConsumerGroupLeaseStore>,
  membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  generation: u64,
  owned: HashSet<VirtualPartitionId>,
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
      generation: 0,
      owned: HashSet::new(),
    })
  }

  async fn shared_assignment(
    &self,
    members: &[String],
    partitions: &[VirtualPartitionId],
    now_ts_ms: i64,
  ) -> Result<Option<ConsumerGroupAssignmentPlan>> {
    let current_plan = self
      .membership_store
      .get_assignment_plan(&self.config.topic, &self.config.group_id)
      .await?;
    let current_plan_is_valid = current_plan
      .as_ref()
      .is_some_and(|plan| assignment_plan_is_valid(plan, partitions));
    let current_members = canonical_members(members, &self.config.member_id);
    let topology_changed = current_plan
      .as_ref()
      .is_some_and(|plan| plan.members != current_members);
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
        return Ok(Some(plan.clone()));
      }
      if planner_is_active && !topology_changed {
        let outcome = self
          .membership_store
          .acquire_or_renew_planner(
            &self.config.topic,
            &self.config.group_id,
            &self.config.member_id,
            now_ts_ms,
            consumer_lease_duration_ms(&self.config),
          )
          .await?;
        if outcome == ConsumerGroupPlannerLeaseOutcome::Acquired {
          return Ok(Some(plan.clone()));
        }
      }
    }

    let planner_outcome = self
      .membership_store
      .acquire_or_renew_planner(
        &self.config.topic,
        &self.config.group_id,
        &self.config.member_id,
        now_ts_ms,
        consumer_lease_duration_ms(&self.config),
      )
      .await?;
    if planner_outcome == ConsumerGroupPlannerLeaseOutcome::Acquired {
      let previous_assignment = current_plan
        .as_ref()
        .map(plan_assignment_map)
        .unwrap_or_default();
      let assignment = cooperative_sticky_assignment(
        members,
        partitions,
        &previous_assignment,
        &self.config.member_id,
      );
      let plan = assignment_plan(
        current_plan
          .as_ref()
          .map_or(1, |current| current.version.saturating_add(1)),
        members,
        partitions,
        &assignment,
        &self.config.member_id,
        now_ts_ms,
      );
      if self
        .membership_store
        .publish_assignment_plan(
          &self.config.topic,
          &self.config.group_id,
          &self.config.member_id,
          now_ts_ms,
          plan.clone(),
        )
        .await?
      {
        info!(
          "consumer assignment plan published: topic={}, group_id={}, member_id={}, version={}, \
           members={}, partitions={}",
          self.config.topic,
          self.config.group_id,
          self.config.member_id,
          plan.version,
          plan.members.len(),
          plan.assignments.len()
        );
        return Ok(Some(plan));
      }
    }

    let refreshed_plan = self
      .membership_store
      .get_assignment_plan(&self.config.topic, &self.config.group_id)
      .await?;
    Ok(refreshed_plan.filter(|plan| assignment_plan_is_valid(plan, partitions)))
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
    let shared_plan = self
      .shared_assignment(&members, &partitions, now_ts_ms)
      .await?;
    if let Some(plan) = shared_plan.as_ref() {
      self.generation = plan.version;
    }
    let desired_assignment = shared_plan
      .as_ref()
      .map(plan_assignment_map)
      .unwrap_or_default();

    // Keep previously owned partitions that are still locally desired, then reconcile each lease
    // outcome below. This avoids rebuilding the local ownership set on stable rebalances.
    let member_id = self.config.member_id.to_string();
    let mut assignment_changed = false;
    self.owned.retain(|partition_id| {
      let retain = desired_assignment
        .get(partition_id)
        .is_some_and(|owner_id| owner_id == &member_id);
      assignment_changed |= !retain;
      retain
    });
    let mut recovered_cursors = HashMap::new();
    for partition_id in partitions {
      let Some(owner_id) = desired_assignment.get(&partition_id) else {
        continue;
      };
      // Skip partitions assigned to other members.
      if owner_id != &member_id {
        continue;
      }

      let key = ConsumerGroupLeaseKey {
        topic: self.config.topic.to_string(),
        group_id: self.config.group_id.to_string(),
        virtual_partition_id: partition_id,
      };

      let outcome = self
        .lease_store
        .assign_partition(
          key,
          member_id.clone(),
          self.generation,
          now_ts_ms,
          consumer_lease_duration_ms(&self.config),
        )
        .await?;

      // Track only partitions that the lease store actually granted to this member.
      match outcome {
        ConsumerGroupAssignmentOutcome::Assigned(lease) => {
          assignment_changed |= self.owned.insert(partition_id);
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
          assignment_changed |= self.owned.remove(&partition_id);
          debug!(
            "consumer assignment held by another member: topic={}, group_id={}, partition={}, \
             member_id={}, generation={}, owner_id={}, owner_generation={}, \
             lease_expires_at_ms={}, last_heartbeat_at_ms={}",
            self.config.topic,
            self.config.group_id,
            partition_id,
            self.config.member_id,
            self.generation,
            lease.owner_id,
            lease.generation,
            lease.lease_expiration_ts_ms,
            lease.last_heartbeat_ts_ms
          );
        },
      }
    }

    // Stable ordering helps deterministic tests and predictable downstream behavior.
    let mut owned = self.owned.iter().copied().collect::<Vec<_>>();
    owned.sort_unstable();
    if assignment_changed {
      info!(
        "consumer rebalance applied: topic={}, group_id={}, member_id={}, generation={}, owned={}",
        self.config.topic,
        self.config.group_id,
        self.config.member_id,
        self.generation,
        owned.len()
      );
    }
    debug!(
      "consumer rebalance completed: topic={}, group_id={}, member_id={}, generation={}, \
       members={}, desired_partitions={}, owned={}",
      self.config.topic,
      self.config.group_id,
      self.config.member_id,
      self.generation,
      members.len(),
      desired_assignment.len(),
      owned.len()
    );
    Ok(RebalanceReport {
      owned_partitions: owned,
      recovered_cursors,
    })
  }

  async fn heartbeat_and_commit(
    &mut self,
    now_ts_ms: i64,
    cursors: &HashMap<VirtualPartitionId, CommittedCursor>,
  ) -> Result<HeartbeatReport> {
    let mut renewed = Vec::new();
    let mut fenced = Vec::new();

    // Snapshot owned partitions so we can mutate self.owned while iterating outcomes.
    let owned = self.owned.iter().copied().collect::<Vec<_>>();
    let owned_count = owned.len();
    for partition_id in owned {
      // Commit-on-heartbeat: include cursor when present, avoiding an additional write path.
      let committed_cursor = cursors.get(&partition_id).cloned();

      let key = ConsumerGroupLeaseKey {
        topic: self.config.topic.to_string(),
        group_id: self.config.group_id.to_string(),
        virtual_partition_id: partition_id,
      };

      let outcome = self
        .lease_store
        .heartbeat_partition(
          &key,
          &self.config.member_id,
          self.generation,
          now_ts_ms,
          consumer_lease_duration_ms(&self.config),
          committed_cursor,
        )
        .await?;

      match outcome {
        // Lease still valid for this member+generation.
        ConsumerGroupHeartbeatOutcome::Renewed(_) => renewed.push(partition_id),
        // Another owner/generation won or lease expired. Immediately drop local ownership.
        ConsumerGroupHeartbeatOutcome::HeldByOther(lease) => {
          self.owned.remove(&partition_id);
          fenced.push(partition_id);
          info!(
            "consumer lease fenced by another member: topic={}, group_id={}, partition={}, \
             member_id={}, generation={}, owner_id={}, owner_generation={}, \
             lease_expires_at_ms={}, last_heartbeat_at_ms={}",
            self.config.topic,
            self.config.group_id,
            partition_id,
            self.config.member_id,
            self.generation,
            lease.owner_id,
            lease.generation,
            lease.lease_expiration_ts_ms,
            lease.last_heartbeat_ts_ms
          );
        },
        ConsumerGroupHeartbeatOutcome::Expired => {
          self.owned.remove(&partition_id);
          fenced.push(partition_id);
          info!(
            "consumer lease expired before heartbeat: topic={}, group_id={}, partition={}, \
             member_id={}, generation={}, now_ts_ms={}",
            self.config.topic,
            self.config.group_id,
            partition_id,
            self.config.member_id,
            self.generation,
            now_ts_ms
          );
        },
      }
    }

    if !fenced.is_empty() {
      info!(
        "consumer fenced partitions detected: topic={}, group_id={}, member_id={}, generation={}, \
         fenced={:?}",
        self.config.topic, self.config.group_id, self.config.member_id, self.generation, fenced
      );
    }

    renewed.sort_unstable();
    fenced.sort_unstable();
    debug!(
      "consumer lease heartbeat completed: topic={}, group_id={}, member_id={}, generation={}, \
       owned_before={}, renewed={}, fenced={}, cursor_count={}",
      self.config.topic,
      self.config.group_id,
      self.config.member_id,
      self.generation,
      owned_count,
      renewed.len(),
      fenced.len(),
      cursors.len()
    );
    Ok(HeartbeatReport {
      renewed_partitions: renewed,
      fenced_partitions: fenced,
    })
  }

  async fn release_owned(&mut self, now_ts_ms: i64) -> Result<Vec<VirtualPartitionId>> {
    let owned = self.owned.iter().copied().collect::<Vec<_>>();
    let mut released = Vec::new();

    for partition_id in owned {
      let key = ConsumerGroupLeaseKey {
        topic: self.config.topic.to_string(),
        group_id: self.config.group_id.to_string(),
        virtual_partition_id: partition_id,
      };

      let outcome = self
        .lease_store
        .release_partition(&key, &self.config.member_id, self.generation, now_ts_ms)
        .await?;

      match outcome {
        ConsumerGroupReleaseOutcome::Released => {
          self.owned.remove(&partition_id);
          released.push(partition_id);
        },
        ConsumerGroupReleaseOutcome::HeldByOther(_) | ConsumerGroupReleaseOutcome::Expired => {
          self.owned.remove(&partition_id);
        },
      }
    }

    released.sort_unstable();
    Ok(released)
  }

  fn generation(&self) -> u64 {
    self.generation
  }

  fn owned_partitions(&self) -> Vec<VirtualPartitionId> {
    let mut owned = self.owned.iter().copied().collect::<Vec<_>>();
    owned.sort_unstable();
    owned
  }
}

//
// cooperative_sticky_assignment
//

#[must_use]
#[allow(clippy::implicit_hasher)]
/// Compute a cooperative sticky assignment.
///
/// The result maps each partition to an owner member id.
pub fn cooperative_sticky_assignment(
  members: &[String],
  partitions: &[VirtualPartitionId],
  previous_assignment: &HashMap<VirtualPartitionId, String>,
  local_member_id: &str,
) -> HashMap<VirtualPartitionId, String> {
  // Normalize membership input and ensure local member is represented so callers can compute
  // deterministic intent even if discovery omitted self temporarily.
  let mut deduped_members = members
    .iter()
    .filter(|member| !member.trim().is_empty())
    .cloned()
    .collect::<Vec<_>>();
  if !deduped_members
    .iter()
    .any(|member| member == local_member_id)
  {
    deduped_members.push(local_member_id.to_string());
  }
  deduped_members.sort();
  deduped_members.dedup();

  // Normalize partition input for deterministic assignment and test stability.
  let mut deduped_partitions = partitions.to_vec();
  deduped_partitions.sort_unstable();
  deduped_partitions.dedup();

  if deduped_members.is_empty() || deduped_partitions.is_empty() {
    return HashMap::new();
  }

  let mut assignments = HashMap::new();
  let member_set = deduped_members.iter().cloned().collect::<HashSet<_>>();
  // Load tracks partition counts per member for balancing decisions.
  let mut load = deduped_members
    .iter()
    .map(|member| (member.clone(), 0_usize))
    .collect::<HashMap<_, _>>();

  // Keep prior placements where possible to preserve stickiness.
  for partition_id in &deduped_partitions {
    if let Some(previous_owner) = previous_assignment.get(partition_id)
      && member_set.contains(previous_owner)
    {
      assignments.insert(*partition_id, previous_owner.clone());
      if let Some(member_load) = load.get_mut(previous_owner) {
        *member_load += 1;
      }
    }
  }

  // Assign any unassigned partitions to the least-loaded member.
  for partition_id in &deduped_partitions {
    if assignments.contains_key(partition_id) {
      continue;
    }
    let owner = least_loaded_member(&deduped_members, &load);
    assignments.insert(*partition_id, owner.clone());
    if let Some(member_load) = load.get_mut(&owner) {
      *member_load += 1;
    }
  }

  // Rebalance only if needed. Move the smallest number of partitions from overloaded members
  // to underloaded members to keep assignment cooperative and sticky.
  let member_count = deduped_members.len();
  let partition_count = deduped_partitions.len();
  let min_target = partition_count / member_count;
  let max_target = min_target + usize::from(!partition_count.is_multiple_of(member_count));

  loop {
    // Find current over/under loaded candidates according to target bounds.
    let over = deduped_members
      .iter()
      .find(|member| load.get(*member).copied().unwrap_or(0) > max_target)
      .cloned();
    let under = deduped_members
      .iter()
      .find(|member| load.get(*member).copied().unwrap_or(0) < min_target)
      .cloned();

    let (Some(over_member), Some(under_member)) = (over, under) else {
      break;
    };

    // Move exactly one partition at a time from over->under, then recompute. This keeps moves
    // minimal and predictable.
    if let Some(partition_to_move) = deduped_partitions
      .iter()
      .find(|partition_id| {
        assignments
          .get(partition_id)
          .is_some_and(|owner| owner == &over_member)
      })
      .copied()
    {
      assignments.insert(partition_to_move, under_member.clone());
      if let Some(over_load) = load.get_mut(&over_member) {
        *over_load = over_load.saturating_sub(1);
      }
      if let Some(under_load) = load.get_mut(&under_member) {
        *under_load += 1;
      }
    } else {
      break;
    }
  }

  assignments
}

fn least_loaded_member(members: &[String], load: &HashMap<String, usize>) -> String {
  // Ties break lexicographically for deterministic output across runs.
  let mut selected = members.first().cloned().unwrap_or_default();
  let mut selected_load = load.get(&selected).copied().unwrap_or(usize::MAX);

  for member in members.iter().skip(1) {
    let member_load = load.get(member).copied().unwrap_or(usize::MAX);
    if member_load < selected_load || (member_load == selected_load && member < &selected) {
      selected.clone_from(member);
      selected_load = member_load;
    }
  }

  selected
}

fn assignment_plan(
  version: u64,
  members: &[String],
  partitions: &[VirtualPartitionId],
  assignments: &HashMap<VirtualPartitionId, String>,
  local_member_id: &str,
  published_ts_ms: i64,
) -> ConsumerGroupAssignmentPlan {
  let members = canonical_members(members, local_member_id);
  let mut partitions = partitions.to_vec();
  partitions.sort_unstable();
  partitions.dedup();
  let assignments = partitions
    .into_iter()
    .filter_map(|virtual_partition_id| {
      assignments
        .get(&virtual_partition_id)
        .map(|member_id| ConsumerGroupAssignment {
          virtual_partition_id,
          member_id: member_id.clone(),
        })
    })
    .collect();

  ConsumerGroupAssignmentPlan {
    version,
    planner_member_id: local_member_id.to_string(),
    members,
    assignments,
    published_ts_ms,
  }
}

fn assignment_plan_is_valid(
  plan: &ConsumerGroupAssignmentPlan,
  partitions: &[VirtualPartitionId],
) -> bool {
  if plan.members.is_empty()
    || !plan
      .members
      .iter()
      .any(|member| member == &plan.planner_member_id)
  {
    return false;
  }

  let mut expected_partitions = partitions.to_vec();
  expected_partitions.sort_unstable();
  expected_partitions.dedup();
  if plan.assignments.len() != expected_partitions.len() {
    return false;
  }

  let member_set = plan.members.iter().collect::<HashSet<_>>();
  let mut assignment_partitions = HashSet::new();
  let mut load = plan
    .members
    .iter()
    .map(|member| (member, 0_usize))
    .collect::<HashMap<_, _>>();
  for assignment in &plan.assignments {
    if !member_set.contains(&assignment.member_id)
      || !assignment_partitions.insert(assignment.virtual_partition_id)
    {
      return false;
    }
    if let Some(member_load) = load.get_mut(&assignment.member_id) {
      *member_load += 1;
    }
  }
  if !expected_partitions
    .iter()
    .all(|partition| assignment_partitions.contains(partition))
  {
    return false;
  }

  let min_load = load.values().min().copied().unwrap_or_default();
  let max_load = load.values().max().copied().unwrap_or_default();
  max_load.saturating_sub(min_load) <= 1
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
