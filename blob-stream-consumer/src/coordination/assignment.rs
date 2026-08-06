use blob_stream_metadata_store::{
  ConsumerGroupAssignment,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupMember,
};
use blob_stream_types::VirtualPartitionId;
use std::collections::{BTreeMap, HashMap, HashSet};

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

  // Rebalance only if needed. A member at the integer-ceiling target may still need to donate
  // when another member has no partition, so compare the actual extreme loads instead of only
  // checking whether owners are above/below static target bounds.
  rebalance_member_loads(
    &deduped_members,
    &deduped_partitions,
    &mut assignments,
    &mut load,
  );

  assignments
}

pub(super) fn cooperative_sticky_assignment_with_pods(
  members: &[ConsumerGroupMember],
  partitions: &[VirtualPartitionId],
  previous_assignment: &HashMap<VirtualPartitionId, String>,
) -> HashMap<VirtualPartitionId, String> {
  let mut pod_members = BTreeMap::<String, Vec<String>>::new();
  let mut member_pods = HashMap::new();
  for member in members {
    let Some(pod_id) = member.pod_id.as_ref() else {
      let member_ids = members
        .iter()
        .map(|member| member.member_id.clone())
        .collect::<Vec<_>>();
      return cooperative_sticky_assignment(
        &member_ids,
        partitions,
        previous_assignment,
        member_ids.first().map_or("", String::as_str),
      );
    };
    pod_members
      .entry(pod_id.clone())
      .or_default()
      .push(member.member_id.clone());
    member_pods.insert(member.member_id.clone(), pod_id.clone());
  }
  for members in pod_members.values_mut() {
    members.sort();
    members.dedup();
  }
  if pod_members.is_empty() {
    return HashMap::new();
  }

  let mut partitions = partitions.to_vec();
  partitions.sort_unstable();
  partitions.dedup();
  let mut pod_load = pod_members
    .keys()
    .cloned()
    .map(|pod_id| (pod_id, 0_usize))
    .collect::<BTreeMap<_, _>>();
  let mut partition_pods = HashMap::new();

  // Preserve a partition's pod placement when its former owner remains active.
  for partition_id in &partitions {
    if let Some(pod_id) = previous_assignment
      .get(partition_id)
      .and_then(|member_id| member_pods.get(member_id))
    {
      partition_pods.insert(*partition_id, pod_id.clone());
      *pod_load.get_mut(pod_id).expect("active pod has load") += 1;
    }
  }
  for partition_id in &partitions {
    if partition_pods.contains_key(partition_id) {
      continue;
    }
    let pod_id = least_loaded_pod(&pod_load);
    partition_pods.insert(*partition_id, pod_id.clone());
    *pod_load.get_mut(&pod_id).expect("selected pod has load") += 1;
  }

  // Move the fewest partitions necessary to make aggregate pod loads differ by at most one.
  rebalance_pod_loads(&partitions, &mut partition_pods, &mut pod_load);

  let mut assignments = HashMap::new();
  for (pod_id, pod_member_ids) in &pod_members {
    let pod_partitions = partitions
      .iter()
      .filter(|partition_id| partition_pods.get(partition_id) == Some(pod_id))
      .copied()
      .collect::<Vec<_>>();
    assign_within_pod(
      pod_member_ids,
      &pod_partitions,
      previous_assignment,
      &member_pods,
      pod_id,
      &mut assignments,
    );
  }

  assignments
}

pub(super) fn assignment_plan(
  version: u64,
  members: &[String],
  partitions: &[VirtualPartitionId],
  assignments: &HashMap<VirtualPartitionId, String>,
  member_topology: Option<Vec<ConsumerGroupMember>>,
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
    member_topology,
    assignments,
    published_ts_ms,
  }
}

pub(super) fn canonical_member_topology(
  members: &[String],
  active_members: &[ConsumerGroupMember],
  local_member_id: &str,
  local_pod_id: Option<&str>,
) -> Option<Vec<ConsumerGroupMember>> {
  let mut pod_ids = active_members
    .iter()
    .filter_map(|member| {
      member
        .pod_id
        .as_ref()
        .map(|pod_id| (member.member_id.as_str(), pod_id.as_str()))
    })
    .collect::<HashMap<_, _>>();
  if let Some(local_pod_id) = local_pod_id {
    pod_ids.insert(local_member_id, local_pod_id);
  }

  members
    .iter()
    .map(|member_id| {
      pod_ids
        .get(member_id.as_str())
        .map(|pod_id| ConsumerGroupMember {
          member_id: member_id.clone(),
          pod_id: Some((*pod_id).to_string()),
        })
    })
    .collect()
}

pub(super) fn assignment_plan_policy(plan: &ConsumerGroupAssignmentPlan) -> &'static str {
  if plan.member_topology.is_some() {
    "pod_aware"
  } else {
    "flat_member"
  }
}

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

fn least_loaded_pod(pod_load: &BTreeMap<String, usize>) -> String {
  pod_load
    .iter()
    .min_by_key(|(pod_id, load)| (**load, *pod_id))
    .map(|(pod_id, _)| pod_id.clone())
    .unwrap_or_default()
}

fn rebalance_pod_loads(
  partitions: &[VirtualPartitionId],
  partition_pods: &mut HashMap<VirtualPartitionId, String>,
  pod_load: &mut BTreeMap<String, usize>,
) {
  loop {
    let over = pod_load
      .iter()
      .max_by_key(|(pod_id, load)| (**load, *pod_id))
      .map(|(pod_id, _)| pod_id.clone());
    let under = pod_load
      .iter()
      .min_by_key(|(pod_id, load)| (**load, *pod_id))
      .map(|(pod_id, _)| pod_id.clone());
    let (Some(over_pod), Some(under_pod)) = (over, under) else {
      break;
    };
    let over_load = pod_load.get(&over_pod).copied().unwrap_or_default();
    let under_load = pod_load.get(&under_pod).copied().unwrap_or_default();
    if over_load.saturating_sub(under_load) <= 1 {
      break;
    }
    let Some(partition_id) = partitions
      .iter()
      .find(|partition_id| partition_pods.get(partition_id) == Some(&over_pod))
      .copied()
    else {
      break;
    };
    partition_pods.insert(partition_id, under_pod.clone());
    *pod_load
      .get_mut(&over_pod)
      .expect("overloaded pod has load") -= 1;
    *pod_load
      .get_mut(&under_pod)
      .expect("underloaded pod has load") += 1;
  }
}

fn assign_within_pod(
  members: &[String],
  partitions: &[VirtualPartitionId],
  previous_assignment: &HashMap<VirtualPartitionId, String>,
  member_pods: &HashMap<String, String>,
  pod_id: &str,
  assignments: &mut HashMap<VirtualPartitionId, String>,
) {
  let member_set = members.iter().collect::<HashSet<_>>();
  let mut load = members
    .iter()
    .cloned()
    .map(|member_id| (member_id, 0_usize))
    .collect::<HashMap<_, _>>();
  for partition_id in partitions {
    if let Some(previous_member) = previous_assignment.get(partition_id)
      && member_set.contains(previous_member)
      && member_pods
        .get(previous_member)
        .is_some_and(|previous_pod| previous_pod == pod_id)
    {
      assignments.insert(*partition_id, previous_member.clone());
      *load
        .get_mut(previous_member)
        .expect("active member has load") += 1;
    }
  }
  for partition_id in partitions {
    if assignments.contains_key(partition_id) {
      continue;
    }
    let member_id = least_loaded_member(members, &load);
    assignments.insert(*partition_id, member_id.clone());
    *load.get_mut(&member_id).expect("selected member has load") += 1;
  }
  rebalance_member_loads(members, partitions, assignments, &mut load);
}

fn rebalance_member_loads(
  members: &[String],
  partitions: &[VirtualPartitionId],
  assignments: &mut HashMap<VirtualPartitionId, String>,
  load: &mut HashMap<String, usize>,
) {
  loop {
    let over = members
      .iter()
      .max_by_key(|member_id| load.get(*member_id).copied().unwrap_or_default())
      .cloned();
    let under = members
      .iter()
      .min_by_key(|member_id| load.get(*member_id).copied().unwrap_or_default())
      .cloned();
    let (Some(over_member), Some(under_member)) = (over, under) else {
      break;
    };
    let over_load = load.get(&over_member).copied().unwrap_or_default();
    let under_load = load.get(&under_member).copied().unwrap_or_default();
    if over_load.saturating_sub(under_load) <= 1 {
      break;
    }
    let Some(partition_id) = partitions
      .iter()
      .find(|partition_id| assignments.get(partition_id) == Some(&over_member))
      .copied()
    else {
      break;
    };
    assignments.insert(partition_id, under_member.clone());
    *load
      .get_mut(&over_member)
      .expect("overloaded member has load") -= 1;
    *load
      .get_mut(&under_member)
      .expect("underloaded member has load") += 1;
  }
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
