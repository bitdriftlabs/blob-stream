use blob_stream_metadata_store::{
  ConsumerGroupAssignment,
  ConsumerGroupAssignmentPlan,
  ConsumerGroupMember,
};
use blob_stream_types::VirtualPartitionId;
use std::collections::{BTreeMap, HashMap, HashSet};

//
// Consumer-group assignment algorithm
//
// Assignment is deterministic and cooperative: it first keeps every placement whose owner is
// still active, fills gaps from the least-loaded owner, then moves only enough partitions to make
// loads differ by at most one. Flat groups balance directly across members. Fully tagged groups
// balance in two stages: partitions are balanced across physical pods before being balanced across
// workers within each pod. That preserves worker concurrency without concentrating a group on a
// small number of pods.

#[must_use]
#[allow(clippy::implicit_hasher)]
/// Compute a cooperative sticky assignment.
///
/// The result maps each partition to an owner member ID. Normalization and lexicographic
/// tie-breaking make every planner derive the same plan from the same inputs.
pub fn cooperative_sticky_assignment(
  members: &[String],
  partitions: &[VirtualPartitionId],
  previous_assignment: &HashMap<VirtualPartitionId, String>,
  local_member_id: &str,
) -> HashMap<VirtualPartitionId, String> {
  // Normalize membership and ensure the planner represents itself even when membership discovery
  // has not observed its just-written heartbeat yet.
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

  // A plan has exactly one desired owner per virtual partition. Sorting and deduplicating prevents
  // duplicate input values from affecting that desired ownership or the movement decision below.
  let mut deduped_partitions = partitions.to_vec();
  deduped_partitions.sort_unstable();
  deduped_partitions.dedup();

  if deduped_members.is_empty() || deduped_partitions.is_empty() {
    return HashMap::new();
  }

  let mut assignments = HashMap::new();
  let member_set = deduped_members.iter().cloned().collect::<HashSet<_>>();
  // Keep a load entry for inactive owners out of the calculation so departed members cannot retain
  // a partition or affect the balancing target.
  let mut load = deduped_members
    .iter()
    .map(|member| (member.clone(), 0_usize))
    .collect::<HashMap<_, _>>();

  // Sticky retention is the first choice: reuse an active prior owner before considering a move.
  // A missing or departed owner leaves its partition for the least-loaded active member instead.
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

  // Deterministically give new and orphaned partitions to the currently least-loaded member.
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

  // Repair only actual imbalance. Comparing extreme loads, rather than static floor/ceiling
  // targets, handles oversubscribed groups where a member with the nominal ceiling must donate to
  // an empty member.
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
  // Build the physical topology up front. A partial topology is deliberately treated as a legacy
  // group: planning with only some pod IDs would make pod balancing depend on discovery timing.
  let mut pod_members = BTreeMap::<String, Vec<String>>::new();
  let mut member_pods = HashMap::new();
  let mut pod_clusters = BTreeMap::new();
  let mut complete_cluster_topology = true;
  for member in members {
    let Some(pod_id) = member.pod_id.as_ref() else {
      let member_ids = members
        .iter()
        .map(|member| member.member_id.clone())
        .collect::<Vec<_>>();
      // The flat planner retains backward compatibility while existing members roll out pod IDs.
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
    match (member.cluster_id.as_ref(), pod_clusters.get(pod_id)) {
      (Some(cluster_id), Some(existing_cluster_id)) if cluster_id == existing_cluster_id => {},
      (Some(cluster_id), None) => {
        pod_clusters.insert(pod_id.clone(), cluster_id.clone());
      },
      _ => complete_cluster_topology = false,
    }
  }
  // Each pod's member ordering determines deterministic within-pod tie breaking.
  for members in pod_members.values_mut() {
    members.sort();
    members.dedup();
  }
  if pod_members.is_empty() {
    return HashMap::new();
  }
  // Cluster metadata remains an optional refinement of pod-aware planning. A missing member value
  // or conflicting values for a shared pod disables only this tie-breaker during rollout.
  let pod_clusters = complete_cluster_topology.then_some(pod_clusters);
  let cluster_pod_counts = pod_clusters.as_ref().map(cluster_pod_counts);

  // The pod stage maps partitions to physical pods, then the worker stage below maps each pod's
  // partitions to its current member IDs.
  let mut partitions = partitions.to_vec();
  partitions.sort_unstable();
  partitions.dedup();
  let mut pod_load = pod_members
    .keys()
    .cloned()
    .map(|pod_id| (pod_id, 0_usize))
    .collect::<BTreeMap<_, _>>();
  let mut partition_pods = HashMap::new();

  // Preserve a partition's existing physical placement whenever that owner remains active. This
  // lets a pod add or lose workers without first moving partitions to another machine.
  // Assign new and orphaned partitions at the pod level before considering individual workers.
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
    let pod_id = least_loaded_pod(
      &pod_load,
      pod_clusters.as_ref(),
      cluster_pod_counts.as_ref(),
    );
    partition_pods.insert(*partition_id, pod_id.clone());
    *pod_load.get_mut(&pod_id).expect("selected pod has load") += 1;
  }

  // Move only enough partitions to make aggregate pod loads differ by at most one.
  rebalance_pod_loads(
    &partitions,
    &mut partition_pods,
    &mut pod_load,
    pod_clusters.as_ref(),
    cluster_pod_counts.as_ref(),
  );
  if let (Some(pod_clusters), Some(cluster_pod_counts)) =
    (pod_clusters.as_ref(), cluster_pod_counts.as_ref())
  {
    rebalance_cluster_tie_breaks(
      &partitions,
      &mut partition_pods,
      &mut pod_load,
      pod_clusters,
      cluster_pod_counts,
    );
  }

  let mut assignments = HashMap::new();
  // With stable pod ownership fixed, independently balance each pod's assigned partitions among
  // its workers. No worker balancing step can cross a physical pod boundary.
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
  local_cluster_id: Option<&str>,
) -> Option<Vec<ConsumerGroupMember>> {
  // A pod-aware plan is valid only when every planned member has a pod. Returning `None` for any
  // incomplete view explicitly selects the legacy flat policy until the rollout is complete.
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

fn cluster_pod_counts(pod_clusters: &BTreeMap<String, String>) -> BTreeMap<String, usize> {
  let mut cluster_pod_counts = BTreeMap::new();
  for cluster_id in pod_clusters.values() {
    *cluster_pod_counts.entry(cluster_id.clone()).or_default() += 1;
  }
  cluster_pod_counts
}

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

fn least_loaded_pod(
  pod_load: &BTreeMap<String, usize>,
  pod_clusters: Option<&BTreeMap<String, String>>,
  cluster_pod_counts: Option<&BTreeMap<String, usize>>,
) -> String {
  // Cluster pod count decides only equal-load choices; pod ID remains the final deterministic
  // tie-breaker so all planners make identical assignments from the same membership snapshot.
  pod_load
    .iter()
    .min_by_key(|(pod_id, load)| {
      (
        **load,
        pod_cluster_count(pod_id, pod_clusters, cluster_pod_counts),
        *pod_id,
      )
    })
    .map(|(pod_id, _)| pod_id.clone())
    .unwrap_or_default()
}

fn rebalance_cluster_tie_breaks(
  partitions: &[VirtualPartitionId],
  partition_pods: &mut HashMap<VirtualPartitionId, String>,
  pod_load: &mut BTreeMap<String, usize>,
  pod_clusters: &BTreeMap<String, String>,
  cluster_pod_counts: &BTreeMap<String, usize>,
) {
  // Preserve the primary pod balance: only shift a residual assignment from a k + 1 pod in a
  // larger cluster to a k pod in a smaller cluster. This is the minimum sticky correction that
  // realizes the cluster preference without turning it into a cluster-load balancing policy.
  loop {
    let transfer = pod_load
      .iter()
      .flat_map(|(under_pod, under_load)| {
        pod_load.iter().filter_map(move |(over_pod, over_load)| {
          let under_cluster_id = pod_clusters.get(under_pod)?;
          let over_cluster_id = pod_clusters.get(over_pod)?;
          let under_cluster_pod_count = cluster_pod_counts.get(under_cluster_id)?;
          let over_cluster_pod_count = cluster_pod_counts.get(over_cluster_id)?;
          (*over_load == *under_load + 1 && over_cluster_pod_count > under_cluster_pod_count)
            .then_some((under_cluster_pod_count, under_pod.clone(), over_pod.clone()))
        })
      })
      .min_by_key(|(under_cluster_pod_count, under_pod, over_pod)| {
        (
          *under_cluster_pod_count,
          under_pod.clone(),
          over_pod.clone(),
        )
      });
    let Some((_, under_pod, over_pod)) = transfer else {
      break;
    };
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

fn rebalance_pod_loads(
  partitions: &[VirtualPartitionId],
  partition_pods: &mut HashMap<VirtualPartitionId, String>,
  pod_load: &mut BTreeMap<String, usize>,
  pod_clusters: Option<&BTreeMap<String, String>>,
  cluster_pod_counts: Option<&BTreeMap<String, usize>>,
) {
  // Each iteration moves one partition from the most-loaded pod to the least-loaded pod. Since
  // prior placements are retained until this point, this is the minimum movement needed for the
  // current load extremes. Cluster size resolves otherwise equivalent moves so the residual stays
  // with a smaller cluster and does not require a corrective second handoff.
  loop {
    let over = pod_load
      .iter()
      .max_by_key(|(pod_id, load)| {
        (
          **load,
          pod_cluster_count(pod_id, pod_clusters, cluster_pod_counts),
          *pod_id,
        )
      })
      .map(|(pod_id, _)| pod_id.clone());
    let under = pod_load
      .iter()
      .min_by_key(|(pod_id, load)| {
        (
          **load,
          pod_cluster_count(pod_id, pod_clusters, cluster_pod_counts),
          *pod_id,
        )
      })
      .map(|(pod_id, _)| pod_id.clone());
    let (Some(over_pod), Some(under_pod)) = (over, under) else {
      break;
    };
    let over_load = pod_load.get(&over_pod).copied().unwrap_or_default();
    let under_load = pod_load.get(&under_pod).copied().unwrap_or_default();
    if over_load.saturating_sub(under_load) <= 1 {
      break;
    }
    // Partitions are sorted, so choose a reproducible donor when several moves are valid.
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
  // This mirrors flat sticky assignment, constrained to the partitions that the pod-level stage
  // already assigned to this physical pod.
  let member_set = members.iter().collect::<HashSet<_>>();
  let mut load = members
    .iter()
    .cloned()
    .map(|member_id| (member_id, 0_usize))
    .collect::<HashMap<_, _>>();
  // Retain a worker only when it remains active in the same pod. A retained member in another pod
  // is intentionally ignored because the pod-level stage has already chosen this partition's pod.
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
  // Newly arrived pod partitions and departed-worker partitions go to the least-loaded worker.
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
  // Like pod rebalancing, transfer one deterministically selected partition at a time until the
  // greatest and least member loads differ by no more than one.
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
    // The sorted partition list also keeps transfer selection stable for equal-load members.
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
  // Ties break lexicographically for deterministic output across independent planners.
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
  // This shared canonicalization is also used when publishing plans, so membership discovery
  // ordering and a temporarily missing local heartbeat cannot produce divergent planner input.
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
