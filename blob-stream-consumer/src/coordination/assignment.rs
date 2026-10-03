//! Sticky balanced placement followed by optional, movement-budgeted logical consolidation.
//!
//! First fix balanced capacities and reserve surviving ownership before placing mandatory moves.
//! Complete pod metadata selects pod-first placement, then worker placement within each pod;
//! incomplete metadata uses flat members. This produces one complete sticky baseline, not a pair
//! of competing plans. Optional repair exchanges worker owners without changing their loads.
//! Pod and worker repair passes share one allowance against that same final-worker baseline.
//! The coordinator enables repair only for a membership transition, never for a steady-state poll.
//!
//! These functions compute desired ownership only. The coordinator validates and publishes the
//! versioned plan, and partition leases separately fence active ownership and cooperative handoff.

#[cfg(test)]
#[path = "./assignment_test.rs"]
pub(super) mod tests;

use super::placement_repair;
use blob_stream_metadata_store::{
  ConsumerGroupAssignment,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupMember,
};
use blob_stream_types::VirtualPartitionId;
use serde_json::{Value, json};
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

type LogicalGroups = Vec<(u32, Vec<VirtualPartitionId>)>;

/// Flat-member entry point, also used when pod metadata is incomplete. Canonicalization removes
/// input-order effects and retains the local member across a temporarily missing heartbeat.
pub(super) fn cooperative_sticky_assignment(
  members: &[String],
  partitions: &[VirtualPartitionId],
  previous_assignment: &HashMap<VirtualPartitionId, String>,
  local_member_id: &str,
  logical_partition_count: u32,
  repair_percent: u8,
) -> HashMap<VirtualPartitionId, String> {
  let members = canonical_members(members, local_member_id);
  let mut assignment = assign_sticky_groups(
    &members,
    partitions,
    previous_assignment,
    logical_partition_count,
    None,
  );
  let budget = placement_repair::movement_budget(assignment.len(), repair_percent);
  if budget > 0 {
    let baseline = assignment.clone();
    let domains = members
      .iter()
      .map(|member| (member.clone(), member.clone()))
      .collect();
    repair_layer(
      &mut assignment,
      &baseline,
      &domains,
      logical_partition_count,
      budget,
    );
  }
  assignment
}

/// Build distinct logical groups and exact floor/ceiling owner capacities for one placement layer.
/// Callers supply canonical owners and a positive logical count validated by the coordinator.
fn colocated_inputs(
  owners: &[String],
  partitions: &[VirtualPartitionId],
  previous_assignment: &HashMap<VirtualPartitionId, String>,
  logical_partition_count: u32,
  residual_preference: Option<&BTreeMap<String, usize>>,
) -> (LogicalGroups, HashMap<String, usize>) {
  debug_assert!(logical_partition_count > 0);
  if owners.is_empty() || partitions.is_empty() {
    return (Vec::new(), HashMap::new());
  }

  // Writer domains share a logical ID modulo the topic's logical partition count. Deduplicate
  // virtual IDs before measuring load; duplicate inventory must not allocate phantom capacity.
  let mut groups = BTreeMap::<u32, Vec<VirtualPartitionId>>::new();
  for partition_id in partitions {
    groups
      .entry(partition_id % logical_partition_count)
      .or_default()
      .push(*partition_id);
  }
  for group in groups.values_mut() {
    group.sort_unstable();
    group.dedup();
  }
  let mut groups = groups.into_iter().collect::<Vec<_>>();
  // Consider larger groups first so small groups do not consume their scarce whole-group fits.
  // Logical ID resolves equal sizes deterministically, independent of inventory traversal order.
  groups.sort_by_key(|(logical_id, partitions)| (Reverse(partitions.len()), *logical_id));

  // Fix exact capacities before placement; their sum equals the canonical inventory.
  // Previous load counts only current inventory: removed partition IDs cannot win residual slots.
  let total = groups.iter().map(|(_, group)| group.len()).sum::<usize>();
  let mut capacity = owners
    .iter()
    .map(|owner| (owner.clone(), total / owners.len()))
    .collect::<HashMap<_, _>>();
  let mut previous_load = HashMap::<&str, usize>::new();
  for partition_id in groups.iter().flat_map(|(_, group)| group) {
    if let Some(previous_owner) = previous_assignment.get(partition_id) {
      *previous_load.entry(previous_owner.as_str()).or_default() += 1;
    }
  }
  let mut residual_owners = owners.to_vec();
  // Give remainder slots to smaller clusters first when that topology is available, then to
  // owners with more surviving inventory, then by ID. This chooses who gets the ceiling load;
  // it does not relax balance or attempt to equalize aggregate load across clusters.
  residual_owners.sort_by_key(|owner| {
    (
      residual_preference
        .and_then(|preference| preference.get(owner))
        .copied()
        .unwrap_or_default(),
      Reverse(
        previous_load
          .get(owner.as_str())
          .copied()
          .unwrap_or_default(),
      ),
      owner.clone(),
    )
  });
  for owner in residual_owners.iter().take(total % owners.len()) {
    *capacity.get_mut(owner).expect("active owner has capacity") += 1;
  }

  (groups, capacity)
}

/// Construct one complete sticky layer. Every eligible previous partition reserves its slot before
/// any orphan is placed, so an early orphan cannot displace a later survivor. Only capacity excess
/// is forced to move; co-location guides which slots to fill, not whether to evict a survivor.
fn assign_sticky_groups(
  owners: &[String],
  partitions: &[VirtualPartitionId],
  previous_assignment: &HashMap<VirtualPartitionId, String>,
  logical_partition_count: u32,
  residual_preference: Option<&BTreeMap<String, usize>>,
) -> HashMap<VirtualPartitionId, String> {
  if owners.is_empty() || partitions.is_empty() {
    return HashMap::new();
  }
  let (groups, capacity) = colocated_inputs(
    owners,
    partitions,
    previous_assignment,
    logical_partition_count,
    residual_preference,
  );
  retained_colocated_candidate(owners, &groups, previous_assignment, capacity)
}

/// Log each repair layer's bounded outcome. The baseline and allowance always describe the entire
/// worker assignment, so the budget cannot silently multiply with the number of pods or passes.
fn repair_layer(
  assignment: &mut HashMap<VirtualPartitionId, String>,
  baseline: &HashMap<VirtualPartitionId, String>,
  domains: &BTreeMap<String, String>,
  logical_partition_count: u32,
  budget: usize,
) {
  let summary = placement_repair::consolidate(
    assignment,
    baseline,
    domains,
    logical_partition_count,
    budget,
  );
  log::debug!(
    "consumer sticky placement repair: workers={}, partitions={}, attempted_groups={}, \
     consolidated_groups={}, occupancy_scans={}, optional_moves={}, movement_budget={}",
    domains.len(),
    assignment.len(),
    summary.attempted_groups,
    summary.consolidated_groups,
    summary.occupancy_scans,
    summary.optional_moves,
    budget,
  );
}

/// Reserve existing ownership before placing orphan/new partitions. Capacity violations drop only
/// the excess retained partitions; canonical group/partition traversal makes that choice stable.
fn retained_colocated_candidate(
  owners: &[String],
  groups: &[(u32, Vec<VirtualPartitionId>)],
  previous: &HashMap<VirtualPartitionId, String>,
  mut capacity: HashMap<String, usize>,
) -> HashMap<VirtualPartitionId, String> {
  let mut assignment = HashMap::new();
  // Reserve both whole groups and fragments on still-eligible owners. Processing every survivor
  // first prevents an early orphan from consuming capacity that a later group already uses.
  for partition_id in groups.iter().flat_map(|(_, group)| group) {
    if let Some(owner) = previous.get(partition_id)
      && let Some(remaining) = capacity.get_mut(owner)
      && *remaining > 0
    {
      assignment.insert(*partition_id, owner.clone());
      *remaining -= 1;
    }
  }
  // Fill gaps without relocating those reservations. A group can become whole only if all of its
  // retained partitions agree on one owner (or it has none) and that owner can fit every gap.
  for (_, group) in groups {
    let missing = group
      .iter()
      .filter(|partition_id| !assignment.contains_key(partition_id))
      .copied()
      .collect::<Vec<_>>();
    let whole_owner = owners.iter().find(|owner| {
      capacity[*owner] >= missing.len()
        && group.iter().all(|partition_id| {
          assignment
            .get(partition_id)
            .is_none_or(|assigned| assigned == *owner)
        })
    });
    for partition_id in missing {
      // If no compatible whole-group fit exists, consume the largest remaining free capacity in
      // canonical order. Optional consolidation may repair a split, but never changes feasibility.
      let owner = whole_owner.unwrap_or_else(|| {
        owners
          .iter()
          .filter(|owner| capacity[*owner] > 0)
          .min_by_key(|owner| (Reverse(capacity[*owner]), *owner))
          .expect("remaining capacity covers unassigned partitions")
      });
      assignment.insert(partition_id, owner.clone());
      *capacity.get_mut(owner).expect("active owner has capacity") -= 1;
    }
  }
  assignment
}

/// Assign pods first, then workers within each fixed pod placement. Previous topology comes from
/// the accepted plan so a departed worker's still-active pod can remain a retained pod owner.
pub(super) fn cooperative_sticky_assignment_with_topology(
  members: &[ConsumerGroupMember],
  previous_members: &[ConsumerGroupMember],
  partitions: &[VirtualPartitionId],
  previous_assignment: &HashMap<VirtualPartitionId, String>,
  logical_partition_count: u32,
  repair_percent: u8,
) -> HashMap<VirtualPartitionId, String> {
  let mut pod_members = BTreeMap::<String, Vec<String>>::new();
  // Preserve known old member-to-pod mappings, then overwrite live members with current metadata.
  // Missing old topology remains unknown; member IDs are never interpreted as pod identifiers.
  let mut member_pods = previous_members
    .iter()
    .filter_map(|member| {
      member
        .pod_id
        .as_ref()
        .map(|pod| (member.member_id.clone(), pod.clone()))
    })
    .collect::<HashMap<_, _>>();
  let mut pod_clusters = BTreeMap::new();
  let mut complete_cluster_topology = true;
  for member in members {
    let Some(pod_id) = member.pod_id.as_ref() else {
      // Pod-first balance requires every member's pod. An incomplete view falls back to flat
      // logical co-location, not an alternate sticky policy or a partially pod-aware assignment.
      let member_ids = members
        .iter()
        .map(|member| member.member_id.clone())
        .collect::<Vec<_>>();
      return cooperative_sticky_assignment(
        &member_ids,
        partitions,
        previous_assignment,
        member_ids.first().map_or("", String::as_str),
        logical_partition_count,
        repair_percent,
      );
    };
    pod_members
      .entry(pod_id.clone())
      .or_default()
      .push(member.member_id.clone());
    member_pods.insert(member.member_id.clone(), pod_id.clone());
    // Cluster metadata is optional independently of pod metadata. Any missing or conflicting
    // cluster disables only the smaller-cluster residual preference, not pod-aware placement.
    match (member.cluster_id.as_ref(), pod_clusters.get(pod_id)) {
      (Some(cluster_id), Some(existing_cluster_id)) if cluster_id == existing_cluster_id => {},
      (Some(cluster_id), None) => {
        pod_clusters.insert(pod_id.clone(), cluster_id.clone());
      },
      _ => complete_cluster_topology = false,
    }
  }
  for pod_member_ids in pod_members.values_mut() {
    pod_member_ids.sort();
    pod_member_ids.dedup();
  }
  let pod_ids = pod_members.keys().cloned().collect::<Vec<_>>();
  // Translate previous member ownership to this layer's pod ownership. A departed worker on an
  // active pod is retained here, but will be an orphan when placing that pod's remaining workers.
  let previous_pods = previous_assignment
    .iter()
    .filter_map(|(partition_id, member_id)| {
      member_pods
        .get(member_id)
        .map(|pod_id| (*partition_id, pod_id.clone()))
    })
    .collect();
  let residual_preference = complete_cluster_topology.then(|| {
    let counts = cluster_pod_counts(&pod_clusters);
    pod_ids
      .iter()
      .map(|pod_id| {
        (
          pod_id.clone(),
          pod_cluster_count(pod_id, Some(&pod_clusters), Some(&counts)),
        )
      })
      .collect::<BTreeMap<_, _>>()
  });
  // Stage 1 fixes pod capacities and placement. Worker packing below may not move a partition
  // across pods to improve its own score; pod co-location and balance take precedence.
  let partition_pods = assign_sticky_groups(
    &pod_ids,
    partitions,
    &previous_pods,
    logical_partition_count,
    residual_preference.as_ref(),
  );
  let mut assignments = HashMap::new();
  // Stage 2 completes the sticky worker map before any optional movement. Comparing repairs
  // against this full baseline prevents pod movement from hiding additional worker redistribution.
  for (pod_id, pod_member_ids) in &pod_members {
    let pod_partitions = partitions
      .iter()
      .filter(|partition_id| partition_pods.get(partition_id) == Some(pod_id))
      .copied()
      .collect::<Vec<_>>();
    assignments.extend(assign_sticky_groups(
      pod_member_ids,
      &pod_partitions,
      previous_assignment,
      logical_partition_count,
      None,
    ));
  }
  let budget = placement_repair::movement_budget(assignments.len(), repair_percent);
  if budget > 0 {
    let baseline = assignments.clone();
    let active_pods = members
      .iter()
      .map(|member| {
        (
          member.member_id.clone(),
          member_pods[&member.member_id].clone(),
        )
      })
      .collect();
    // Pod repair exchanges actual worker owners, preserving both levels' exact capacities. Once
    // that pass finishes, worker repair is confined to each pod and cannot undo its pod locality.
    repair_layer(
      &mut assignments,
      &baseline,
      &active_pods,
      logical_partition_count,
      budget,
    );
    for pod_member_ids in pod_members.values() {
      let workers = pod_member_ids
        .iter()
        .map(|member| (member.clone(), member.clone()))
        .collect();
      repair_layer(
        &mut assignments,
        &baseline,
        &workers,
        logical_partition_count,
        budget,
      );
    }
  }
  assignments
}

/// Serialize desired ownership in canonical order. This does not acquire leases or make an
/// incomplete map valid: missing entries remain absent for the coordinator's plan validator.
pub(super) fn assignment_plan(
  version: u64,
  members: &[String],
  partitions: &[VirtualPartitionId],
  assignments: &HashMap<VirtualPartitionId, String>,
  member_topology: Option<Vec<ConsumerGroupMember>>,
  local_member_id: &str,
  published_ts_ms: i64,
) -> ConsumerGroupAssignmentPlan {
  // Persist canonical vectors rather than the hash-map iteration order used during planning. This
  // makes plans byte-for-byte stable for diagnostics, validation, and peer planner comparison.
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

  // The persisted true marker is for reader compatibility, not a selectable runtime policy.
  // Following accepted plans and deciding when to publish a replacement belong upstream.
  ConsumerGroupAssignmentPlan {
    version,
    planner_member_id: local_member_id.to_string(),
    members,
    member_topology,
    colocate_logical_partitions: true,
    assignments,
    published_ts_ms,
  }
}

/// Match topology to the canonical member list. Pod awareness is all-or-nothing, while incomplete
/// cluster metadata is retained so placement can omit just the cluster residual preference.
pub(super) fn canonical_member_topology(
  members: &[String],
  active_members: &[ConsumerGroupMember],
  local_member_id: &str,
  local_pod_id: Option<&str>,
  local_cluster_id: Option<&str>,
) -> Option<Vec<ConsumerGroupMember>> {
  // A pod-aware plan is valid only when every planned member has a pod. Returning `None` for any
  // incomplete view keeps logical co-location over flat members.
  let mut member_topology = active_members
    .iter()
    .filter_map(|member| {
      member.pod_id.as_ref().map(|pod_id| {
        (
          member.member_id.as_str(),
          (pod_id.as_str(), member.cluster_id.as_deref()),
        )
      })
    })
    .collect::<HashMap<_, _>>();
  if let Some(local_pod_id) = local_pod_id {
    // Local configuration wins over a stale/missing membership heartbeat for this process.
    member_topology.insert(local_member_id, (local_pod_id, local_cluster_id));
  }

  // `collect::<Option<_>>()` naturally rejects a missing topology record for any canonical member.
  members
    .iter()
    .map(|member_id| {
      member_topology
        .get(member_id.as_str())
        .map(|(pod_id, cluster_id)| ConsumerGroupMember {
          member_id: member_id.clone(),
          pod_id: Some((*pod_id).to_string()),
          cluster_id: cluster_id.map(ToString::to_string),
        })
    })
    .collect()
}

/// Diagnostic topology label; both labels use the same logical co-location placement policy.
pub(super) fn assignment_plan_policy(plan: &ConsumerGroupAssignmentPlan) -> &'static str {
  if plan.member_topology.is_some() {
    "pod_aware"
  } else {
    "flat_member"
  }
}

/// Bounded counts of desired-owner changes, not observed lease handoffs. Orphan placement and
/// survivor movement form one classification; cross/intra/unknown pod movement forms another and
/// includes mandatory transfers from departed members when their previous pod is known.
pub(super) fn assignment_movement_summary(
  plan: &ConsumerGroupAssignmentPlan,
  previous: Option<&ConsumerGroupAssignmentPlan>,
) -> Value {
  let previous_owners = previous
    .into_iter()
    .flat_map(|plan| &plan.assignments)
    .map(|assignment| {
      (
        assignment.virtual_partition_id,
        assignment.member_id.as_str(),
      )
    })
    .collect::<HashMap<_, _>>();
  let old_pods = previous
    .into_iter()
    .flat_map(|plan| plan.member_topology.iter().flatten())
    .filter_map(|member| {
      member
        .pod_id
        .as_deref()
        .map(|pod| (member.member_id.as_str(), pod))
    })
    .collect::<HashMap<_, _>>();
  let new_pods = plan
    .member_topology
    .iter()
    .flatten()
    .filter_map(|member| {
      member
        .pod_id
        .as_deref()
        .map(|pod| (member.member_id.as_str(), pod))
    })
    .collect::<HashMap<_, _>>();
  let mut orphan_placements = 0;
  let mut survivor_moves = 0;
  let mut cross_pod_moves = 0;
  let mut intra_pod_moves = 0;
  let mut unknown_pod_moves = 0;
  for assignment in &plan.assignments {
    let old_owner = previous_owners
      .get(&assignment.virtual_partition_id)
      .copied();
    if old_owner == Some(assignment.member_id.as_str()) {
      continue;
    }
    if old_owner.is_some_and(|owner| plan.members.iter().any(|member| member == owner)) {
      survivor_moves += 1;
    } else {
      orphan_placements += 1;
    }
    // New partitions have no old location, so they count as orphan placements but not unknown-pod
    // moves. For reassignment, compare recorded old/new topology; never guess a missing pod.
    if let Some(old_owner) = old_owner {
      match (
        old_pods.get(old_owner),
        new_pods.get(assignment.member_id.as_str()),
      ) {
        (Some(old_pod), Some(new_pod)) if old_pod == new_pod => intra_pod_moves += 1,
        (Some(_), Some(_)) => cross_pod_moves += 1,
        _ => unknown_pod_moves += 1,
      }
    }
  }
  json!({
    "orphan_placements": orphan_placements, "survivor_moves": survivor_moves,
    "cross_pod_moves": cross_pod_moves, "intra_pod_moves": intra_pod_moves,
    "unknown_pod_moves": unknown_pod_moves,
  })
}

/// Canonical pod list for logs; multiple workers on a pod must not duplicate that pod's label.
pub(super) fn assignment_plan_pod_ids(plan: &ConsumerGroupAssignmentPlan) -> Vec<&str> {
  let mut pod_ids = plan
    .member_topology
    .iter()
    .flatten()
    .filter_map(|member| member.pod_id.as_deref())
    .collect::<Vec<_>>();
  pod_ids.sort_unstable();
  pod_ids.dedup();
  pod_ids
}

/// Count pods, not workers: the input has one entry per distinct pod regardless of worker count.
fn cluster_pod_counts(pod_clusters: &BTreeMap<String, String>) -> BTreeMap<String, usize> {
  let mut cluster_pod_counts = BTreeMap::new();
  for cluster_id in pod_clusters.values() {
    *cluster_pod_counts.entry(cluster_id.clone()).or_default() += 1;
  }
  cluster_pod_counts
}

/// Residual-preference rank. Missing topology contributes no preference instead of blocking a
/// placement; callers enable the cluster preference only after checking topology completeness.
fn pod_cluster_count(
  pod_id: &str,
  pod_clusters: Option<&BTreeMap<String, String>>,
  cluster_pod_counts: Option<&BTreeMap<String, usize>>,
) -> usize {
  pod_clusters
    .and_then(|pod_clusters| pod_clusters.get(pod_id))
    .and_then(|cluster_id| cluster_pod_counts.and_then(|counts| counts.get(cluster_id)))
    .copied()
    .unwrap_or_default()
}

/// Establish sorted unique eligible IDs, ignoring blank discovery entries. Nonblank IDs are
/// preserved verbatim rather than trimmed into a different membership identity.
fn canonical_members(members: &[String], local_member_id: &str) -> Vec<String> {
  // This shared canonicalization is also used when publishing plans, so membership discovery
  // ordering and a temporarily missing local heartbeat cannot produce divergent planner input.
  let mut canonical = members
    .iter()
    .filter(|member| !member.trim().is_empty())
    .cloned()
    .collect::<Vec<_>>();
  if !local_member_id.trim().is_empty() && !canonical.iter().any(|member| member == local_member_id)
  {
    canonical.push(local_member_id.to_string());
  }
  canonical.sort();
  canonical.dedup();
  canonical
}
