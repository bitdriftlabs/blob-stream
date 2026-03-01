// blob-stream - consumer group coordination
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./coordination_test.rs"]
mod tests;

use crate::config::{ConsumerGroupConfig, consumer_lease_duration_ms, validate_group_config};
use anyhow::Result;
use async_trait::async_trait;
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseStore,
};
use blob_stream_types::{CommittedCursor, VirtualPartitionId};
use log::info;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

//
// Coordination algorithm overview
//
// This module implements Milestone 10 consumer-group coordination with a clear split between:
//
// 1) Desired ownership computation (local, deterministic, cooperative sticky)
// 2) Actual ownership claim/renewal (backed by ConsumerGroupLeaseStore fencing semantics)
//
// High-level flow
//
// A) Rebalance phase (`rebalance`)
//    - Build a desired partition->member map using cooperative sticky assignment.
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
// This design intentionally reuses the lease store for all correctness-critical state (leases,
// generation checks, ownership fences) and keeps the coordinator focused on deterministic planning
// + orchestration.

//
// HeartbeatReport
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatReport {
  pub renewed_partitions: Vec<VirtualPartitionId>,
  pub fenced_partitions: Vec<VirtualPartitionId>,
}

//
// RebalanceReport
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebalanceReport {
  pub owned_partitions: Vec<VirtualPartitionId>,
  pub committed_cursors: HashMap<VirtualPartitionId, u64>,
}

//
// ConsumerGroupCoordinator
//

#[async_trait]
pub trait ConsumerGroupCoordinator: Send {
  async fn rebalance(
    &mut self,
    members: Vec<String>,
    partitions: Vec<VirtualPartitionId>,
    now_ts_ms: i64,
  ) -> Result<RebalanceReport>;

  async fn heartbeat_and_commit(
    &mut self,
    now_ts_ms: i64,
    cursors: &HashMap<VirtualPartitionId, u64>,
  ) -> Result<HeartbeatReport>;

  fn generation(&self) -> u64;
  fn owned_partitions(&self) -> Vec<VirtualPartitionId>;
}

//
// ConsumerGroupCoordinatorImpl
//

pub struct ConsumerGroupCoordinatorImpl {
  config: ConsumerGroupConfig,
  lease_store: Arc<dyn ConsumerGroupLeaseStore>,
  generation: u64,
  desired_assignment: HashMap<VirtualPartitionId, String>,
  owned: HashSet<VirtualPartitionId>,
}

impl ConsumerGroupCoordinatorImpl {
  pub fn new(
    config: ConsumerGroupConfig,
    lease_store: Arc<dyn ConsumerGroupLeaseStore>,
  ) -> Result<Self> {
    // Validate static configuration once at construction so runtime paths stay focused on
    // coordination logic.
    validate_group_config(&config)?;
    Ok(Self {
      config,
      lease_store,
      generation: 0,
      desired_assignment: HashMap::new(),
      owned: HashSet::new(),
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
    // Compute target assignment from current membership + partition set while preserving prior
    // placements when possible.
    let desired_assignment = cooperative_sticky_assignment(
      &members,
      &partitions,
      &self.desired_assignment,
      &self.config.member_id,
    );

    // Generation increments only when desired topology changes. This generation is later used by
    // lease operations to fence stale coordinators/owners.
    if self.desired_assignment != desired_assignment {
      self.generation = self.generation.saturating_add(1);
      info!(
        "consumer rebalance plan changed: topic={}, group_id={}, member_id={}, generation={}",
        self.config.topic, self.config.group_id, self.config.member_id, self.generation
      );
      self.desired_assignment = desired_assignment;
    }

    // Rebuild owned set from lease-store assignment outcomes for this pass.
    // We do not assume local desired ownership implies real ownership.
    self.owned.clear();
    let mut committed_cursors = HashMap::new();
    for partition_id in partitions {
      let Some(owner_id) = self.desired_assignment.get(&partition_id) else {
        continue;
      };
      // Skip partitions assigned to other members.
      if owner_id != self.config.member_id.to_string().as_str() {
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
          self.config.member_id.to_string(),
          self.generation,
          now_ts_ms,
          consumer_lease_duration_ms(&self.config),
        )
        .await?;

      // Track only partitions that the lease store actually granted to this member.
      if let ConsumerGroupAssignmentOutcome::Assigned(lease) = outcome {
        self.owned.insert(partition_id);
        if let Some(committed_cursor) = lease.committed_cursor {
          committed_cursors.insert(partition_id, committed_cursor.seq_end);
        }
      }
    }

    // Stable ordering helps deterministic tests and predictable downstream behavior.
    let mut owned = self.owned.iter().copied().collect::<Vec<_>>();
    owned.sort_unstable();
    info!(
      "consumer rebalance applied: topic={}, group_id={}, member_id={}, generation={}, owned={}",
      self.config.topic,
      self.config.group_id,
      self.config.member_id,
      self.generation,
      owned.len()
    );
    Ok(RebalanceReport {
      owned_partitions: owned,
      committed_cursors,
    })
  }

  async fn heartbeat_and_commit(
    &mut self,
    now_ts_ms: i64,
    cursors: &HashMap<VirtualPartitionId, u64>,
  ) -> Result<HeartbeatReport> {
    let mut renewed = Vec::new();
    let mut fenced = Vec::new();

    // Snapshot owned partitions so we can mutate self.owned while iterating outcomes.
    let owned = self.owned.iter().copied().collect::<Vec<_>>();
    for partition_id in owned {
      // Commit-on-heartbeat: include cursor when present, avoiding an additional write path.
      let committed_cursor = cursors
        .get(&partition_id)
        .copied()
        .map(|seq_end| CommittedCursor {
          virtual_partition_id: partition_id,
          seq_end,
        });

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
        ConsumerGroupHeartbeatOutcome::HeldByOther(_) | ConsumerGroupHeartbeatOutcome::Expired => {
          self.owned.remove(&partition_id);
          fenced.push(partition_id);
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
    Ok(HeartbeatReport {
      renewed_partitions: renewed,
      fenced_partitions: fenced,
    })
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
