#![allow(clippy::unwrap_used)]

use super::{
  assignment_movement_summary,
  assignment_plan,
  cooperative_sticky_assignment,
  cooperative_sticky_assignment_with_topology,
};
use crate::coordination::placement_repair;
use blob_stream_metadata_store::ConsumerGroupMember;
use blob_stream_types::VirtualPartitionId;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

// Baseline fixtures deliberately disable optional repair. The coordinator owns membership-change
// detection; pure placement tests exercise positive allowances explicitly rather than silently
// spending another allowance each time they replan an unchanged assignment.
pub(in crate::coordination) fn cooperative_colocated_assignment(
  owners: &[String],
  partitions: &[VirtualPartitionId],
  previous: &HashMap<VirtualPartitionId, String>,
  local_member_id: &str,
  logical_partition_count: u32,
) -> HashMap<VirtualPartitionId, String> {
  cooperative_sticky_assignment(
    owners,
    partitions,
    previous,
    local_member_id,
    logical_partition_count,
    0,
  )
}

fn cooperative_colocated_assignment_with_topology(
  members: &[ConsumerGroupMember],
  previous_members: &[ConsumerGroupMember],
  partitions: &[VirtualPartitionId],
  previous: &HashMap<VirtualPartitionId, String>,
  logical_partition_count: u32,
) -> HashMap<VirtualPartitionId, String> {
  cooperative_sticky_assignment_with_topology(
    members,
    previous_members,
    partitions,
    previous,
    logical_partition_count,
    0,
  )
}

// Most fixtures do not model departed topology separately. This adapter shares their membership
// view across both inputs; departure-specific tests call the production topology entry point.
pub(in crate::coordination) fn cooperative_colocated_assignment_with_pods(
  members: &[ConsumerGroupMember],
  partitions: &[VirtualPartitionId],
  previous_assignment: &HashMap<VirtualPartitionId, String>,
  logical_partition_count: u32,
) -> HashMap<VirtualPartitionId, String> {
  cooperative_colocated_assignment_with_topology(
    members,
    members,
    partitions,
    previous_assignment,
    logical_partition_count,
  )
}

fn score(
  assignment: &HashMap<u32, String>,
  previous: &HashMap<u32, String>,
  owners: &[String],
  logical_count: u32,
) -> (usize, usize, usize) {
  let mut groups = BTreeMap::<u32, BTreeSet<&String>>::new();
  for (partition, owner) in assignment {
    groups
      .entry(partition % logical_count)
      .or_default()
      .insert(owner);
  }
  (
    groups.values().filter(|group| group.len() > 1).count(),
    assignment
      .iter()
      .filter(|(partition, owner)| {
        previous
          .get(partition)
          .is_some_and(|old| owners.contains(old) && old != *owner)
      })
      .count(),
    groups
      .values()
      .map(|group| group.len().saturating_sub(2))
      .sum(),
  )
}

fn assert_balanced(assignment: &HashMap<u32, String>, owners: &[String], partitions: &[u32]) {
  assert_eq!(
    assignment.keys().copied().collect::<BTreeSet<_>>(),
    partitions.iter().copied().collect()
  );
  assert!(assignment.values().all(|owner| owners.contains(owner)));
  let loads = owners
    .iter()
    .map(|owner| {
      assignment
        .values()
        .filter(|assigned| *assigned == owner)
        .count()
    })
    .collect::<Vec<_>>();
  assert!(loads.iter().max().unwrap() - loads.iter().min().unwrap() <= 1);
}

fn owner_loads(assignment: &HashMap<u32, String>) -> BTreeMap<&String, usize> {
  let mut loads = BTreeMap::new();
  for owner in assignment.values() {
    *loads.entry(owner).or_default() += 1;
  }
  loads
}

#[test]
fn colocation_repair_rejects_equal_quality_and_impossible_capacity() {
  let owners = ["a", "b"].map(ToString::to_string);
  let domains = owners
    .iter()
    .map(|owner| (owner.clone(), owner.clone()))
    .collect();
  // Consolidating group 0 would break a whole group. Its canonical owner vector would improve,
  // but locality would not: equally good ownership is churn, not a valid optional repair.
  let baseline = HashMap::from([
    (0, "b".to_string()),
    (1, "a".to_string()),
    (2, "b".to_string()),
    (3, "a".to_string()),
    (4, "a".to_string()),
    (5, "b".to_string()),
  ]);
  let mut assignment = baseline.clone();
  let summary = placement_repair::consolidate(&mut assignment, &baseline, &domains, 3, 6);
  assert_eq!(assignment, baseline);
  assert_eq!(summary.attempted_groups, 1);
  assert_eq!(summary.consolidated_groups, 0);
  assert_eq!(summary.optional_moves, 0);
  // A single group larger than either owner's load has no return partitions. Even a full allowance
  // must not publish a partial attempt or violate capacity.
  let summary = placement_repair::consolidate(&mut assignment, &baseline, &domains, 1, 6);
  assert_eq!(assignment, baseline);
  assert_eq!(summary.consolidated_groups, 0);
}

#[test]
fn colocation_repair_flat_seeded_load_budget_quality_and_canonical_properties() {
  for seed in 1_u32 ..= 96 {
    let owners = (0 ..= seed % 7)
      .map(|owner| format!("owner-{owner}"))
      .collect::<Vec<_>>();
    let partitions = (0 .. 8 + seed % 57).collect::<Vec<_>>();
    let logical_count = 1 + seed % 13;
    let mut state = u64::from(seed);
    let previous = partitions
      .iter()
      .map(|partition| {
        state = state
          .wrapping_mul(6_364_136_223_846_793_005)
          .wrapping_add(1);
        (
          *partition,
          format!("owner-{}", state % (owners.len() as u64 + 1)),
        )
      })
      .collect::<HashMap<_, _>>();
    let baseline = cooperative_sticky_assignment(
      &owners,
      &partitions,
      &previous,
      &owners[0],
      logical_count,
      0,
    );
    for percent in [0, 1, 10, 25, 100] {
      let assignment = cooperative_sticky_assignment(
        &owners,
        &partitions,
        &previous,
        &owners[0],
        logical_count,
        percent,
      );
      assert_balanced(&assignment, &owners, &partitions);
      assert_eq!(
        owner_loads(&assignment),
        owner_loads(&baseline),
        "seed={seed}"
      );
      assert!(
        placement_repair::changed_partitions(&assignment, &baseline)
          <= placement_repair::movement_budget(partitions.len(), percent),
        "seed={seed}"
      );
      let before = score(&baseline, &previous, &owners, logical_count);
      let after = score(&assignment, &previous, &owners, logical_count);
      assert!((after.0, after.2) <= (before.0, before.2), "seed={seed}");
      if assignment != baseline {
        assert!((after.0, after.2) < (before.0, before.2), "seed={seed}");
      }
      let mut reordered_owners = owners.clone();
      reordered_owners.reverse();
      reordered_owners.push(owners[0].clone());
      let mut reordered_partitions = partitions.clone();
      reordered_partitions.reverse();
      reordered_partitions.extend_from_slice(&partitions);
      assert_eq!(
        assignment,
        cooperative_sticky_assignment(
          &reordered_owners,
          &reordered_partitions,
          &previous,
          &owners[0],
          logical_count,
          percent
        ),
        "seed={seed}"
      );
    }
  }
}

#[test]
fn colocation_repair_topology_shares_one_full_worker_baseline_allowance() {
  let members = (0 .. 4)
    .map(|worker| ConsumerGroupMember {
      member_id: format!("worker-{worker}"),
      pod_id: Some(format!("pod-{}", worker / 2)),
      cluster_id: None,
    })
    .collect::<Vec<_>>();
  let owners = members
    .iter()
    .map(|member| member.member_id.clone())
    .collect::<Vec<_>>();
  let pods = ["pod-0", "pod-1"].map(ToString::to_string);
  let member_pods = members
    .iter()
    .map(|member| (member.member_id.clone(), member.pod_id.clone().unwrap()))
    .collect::<HashMap<_, _>>();
  for seed in 0_u32 ..= 48 {
    let partitions = (0 .. if seed == 0 { 16 } else { 8 + seed }).collect::<Vec<_>>();
    let previous = partitions
      .iter()
      .map(|partition| {
        let worker = if seed == 0 {
          partition / 4
        } else {
          (partition * (seed + 1) + partition / 4) % 4
        };
        (*partition, owners[usize::try_from(worker).unwrap()].clone())
      })
      .collect::<HashMap<_, _>>();
    let logical_count = if seed == 0 { 4 } else { 1 + seed % 11 };
    let baseline = cooperative_sticky_assignment_with_topology(
      &members,
      &members,
      &partitions,
      &previous,
      logical_count,
      0,
    );
    for percent in [0, 10, 25, 50, 100] {
      let assignment = cooperative_sticky_assignment_with_topology(
        &members,
        &members,
        &partitions,
        &previous,
        logical_count,
        percent,
      );
      let moves = placement_repair::changed_partitions(&assignment, &baseline);
      assert!(
        moves <= placement_repair::movement_budget(partitions.len(), percent),
        "seed={seed}"
      );
      assert_eq!(
        assignment.keys().copied().collect::<BTreeSet<_>>(),
        partitions.iter().copied().collect()
      );
      assert_eq!(
        owner_loads(&assignment),
        owner_loads(&baseline),
        "seed={seed}"
      );
      let baseline_pods = baseline
        .iter()
        .map(|(partition, worker)| (*partition, member_pods[worker].clone()))
        .collect();
      let actual_pods = assignment
        .iter()
        .map(|(partition, worker)| (*partition, member_pods[worker].clone()))
        .collect();
      assert_balanced(&actual_pods, &pods, &partitions);
      let before = score(&baseline_pods, &HashMap::new(), &pods, logical_count);
      let after = score(&actual_pods, &HashMap::new(), &pods, logical_count);
      assert!((after.0, after.2) <= (before.0, before.2), "seed={seed}");
      // Pod consolidation consumes four slots here. Worker passes also have opportunities, but
      // cannot get a fresh four-slot allowance for each pod or placement layer.
      if seed == 0 && percent == 25 {
        assert_eq!(moves, 4);
        assert!((after.0, after.2) < (before.0, before.2));
      }
      let mut reordered_members = members.clone();
      reordered_members.reverse();
      let mut reordered_partitions = partitions.clone();
      reordered_partitions.reverse();
      reordered_partitions.extend_from_slice(&partitions);
      assert_eq!(
        assignment,
        cooperative_sticky_assignment_with_topology(
          &reordered_members,
          &members,
          &reordered_partitions,
          &previous,
          logical_count,
          percent
        ),
        "seed={seed}"
      );
    }
  }
}

fn oracle_score(
  owners: &[String],
  partitions: &[u32],
  previous: &HashMap<u32, String>,
  logical_count: u32,
  target: &HashMap<u32, String>,
) -> (usize, usize, usize) {
  let capacities = owners
    .iter()
    .map(|owner| {
      target
        .values()
        .filter(|assigned| *assigned == owner)
        .count()
    })
    .collect::<Vec<_>>();
  let mut best = (usize::MAX, usize::MAX, usize::MAX);
  let alternatives = owners.len().pow(u32::try_from(partitions.len()).unwrap());
  for mut choice in 0 .. alternatives {
    let mut assignment = HashMap::new();
    let mut remaining = capacities.clone();
    for partition in partitions {
      let owner = choice % owners.len();
      choice /= owners.len();
      if remaining[owner] == 0 {
        break;
      }
      remaining[owner] -= 1;
      assignment.insert(*partition, owners[owner].clone());
    }
    if assignment.len() == partitions.len() {
      best = best.min(score(&assignment, previous, owners, logical_count));
    }
  }
  best
}

#[test]
fn colocated_assignment_counterexample_matches_exhaustive_optimum() {
  let owners = ["pod-01", "pod-02", "pod-03"].map(ToString::to_string);
  let partitions = (0 .. 8).collect::<Vec<_>>();
  let previous = partitions
    .iter()
    .map(|partition| (*partition, format!("pod-{:02}", partition % 4)))
    .collect::<HashMap<_, _>>();
  let assignment = cooperative_colocated_assignment(&owners, &partitions, &previous, &owners[0], 4);
  assert_eq!(score(&assignment, &previous, &owners, 4), (1, 0, 0));
  assert_eq!(
    score(&assignment, &previous, &owners, 4),
    oracle_score(&owners, &partitions, &previous, 4, &assignment)
  );
}

#[test]
fn colocated_assignment_seeded_properties_and_small_oracle_gaps() {
  let mut gaps = Vec::new();
  for seed in 1_u64 ..= 96 {
    let bounded_seed = u32::try_from(seed).unwrap();
    let owner_count = 1 + usize::try_from(bounded_seed % 4).unwrap();
    let owners = (0 .. owner_count)
      .map(|owner| format!("owner-{owner}"))
      .collect::<Vec<_>>();
    let count = 1 + bounded_seed % 14;
    let logical_count = 1 + bounded_seed % 6;
    let partitions = (0 .. count)
      .filter(|partition| (u64::from(*partition) + seed) % 5 != 0)
      .collect::<Vec<_>>();
    let mut state = seed;
    let previous = partitions
      .iter()
      .map(|partition| {
        state = state
          .wrapping_mul(6_364_136_223_846_793_005)
          .wrapping_add(1);
        (
          *partition,
          format!("owner-{}", (state >> 32) as usize % (owner_count + 1)),
        )
      })
      .collect::<HashMap<_, _>>();
    let assignment =
      cooperative_colocated_assignment(&owners, &partitions, &previous, &owners[0], logical_count);
    assert_balanced(&assignment, &owners, &partitions);
    assert_eq!(
      assignment,
      cooperative_colocated_assignment(
        &owners,
        &partitions,
        &assignment,
        &owners[0],
        logical_count
      ),
      "unstable seed={seed}"
    );
    let mut shuffled_owners = owners.clone();
    shuffled_owners.reverse();
    shuffled_owners.push(owners[0].clone());
    shuffled_owners.push(" ".to_string());
    let mut shuffled_partitions = partitions.clone();
    shuffled_partitions.reverse();
    shuffled_partitions.extend_from_slice(&partitions);
    let mut stale_previous = previous.clone();
    for partition in count + 100 .. count + 120 {
      stale_previous.insert(partition, owners[0].clone());
    }
    assert_eq!(
      assignment,
      cooperative_colocated_assignment(
        &shuffled_owners,
        &shuffled_partitions,
        &stale_previous,
        &owners[0],
        logical_count
      ),
      "noncanonical seed={seed}"
    );
    if partitions.len() <= 8 && owners.len() <= 3 {
      let optimum = oracle_score(&owners, &partitions, &previous, logical_count, &assignment);
      let actual = score(&assignment, &previous, &owners, logical_count);
      assert!(optimum <= actual);
      if optimum != actual {
        gaps.push((seed, actual, optimum));
      }
    }
  }
  eprintln!("bounded planner small-oracle gaps: {gaps:?}");
}

#[test]
fn colocated_assignment_empty_inventory_and_missing_local_heartbeat() {
  assert!(
    cooperative_colocated_assignment(&[" ".to_string()], &[0], &HashMap::new(), "", 4).is_empty()
  );
  assert!(cooperative_colocated_assignment(&[], &[], &HashMap::new(), "local", 4).is_empty());
  assert_eq!(
    cooperative_colocated_assignment(&[], &[0, 4, 0], &HashMap::new(), "local", 4),
    HashMap::from([(0, "local".to_string()), (4, "local".to_string())])
  );
  assert!(cooperative_colocated_assignment_with_pods(&[], &[0], &HashMap::new(), 4).is_empty());
}

#[test]
fn colocated_assignment_worker_departure_preserves_known_surviving_pod() {
  let old_members = [
    ("worker-a", "pod-a"),
    ("worker-departed", "pod-a"),
    ("worker-b", "pod-b"),
    ("worker-c", "pod-c"),
  ]
  .map(|(member, pod)| ConsumerGroupMember {
    member_id: member.to_string(),
    pod_id: Some(pod.to_string()),
    cluster_id: None,
  });
  let members = old_members
    .iter()
    .filter(|member| member.member_id != "worker-departed")
    .cloned()
    .collect::<Vec<_>>();
  let partitions = (0 .. 8).collect::<Vec<_>>();
  let previous = HashMap::from([
    (0, "worker-departed"),
    (1, "worker-a"),
    (5, "worker-a"),
    (2, "worker-b"),
    (4, "worker-b"),
    (6, "worker-b"),
    (3, "worker-c"),
    (7, "worker-c"),
  ])
  .into_iter()
  .map(|(partition, owner)| (partition, owner.to_string()))
  .collect::<HashMap<_, _>>();
  let assignment = cooperative_colocated_assignment_with_topology(
    &members,
    &old_members,
    &partitions,
    &previous,
    4,
  );
  assert_eq!(assignment[&0], "worker-a");
  for partition in 1 .. 8 {
    assert_eq!(assignment[&partition], previous[&partition]);
  }
  assert_eq!(
    assignment,
    cooperative_colocated_assignment_with_topology(&members, &members, &partitions, &assignment, 4)
  );
}

#[test]
fn colocated_assignment_event_shape_topology_and_membership_variations() {
  let partitions = (0 .. 45)
    .flat_map(|logical| [logical, logical + 64, logical + 128])
    .chain([63])
    .collect::<Vec<_>>();
  for departure in [0_usize, 10, 20] {
    for workers in [1_usize, 2] {
      let old_members = (0 .. 21)
        .flat_map(|pod| {
          (0 .. workers).map(move |worker| ConsumerGroupMember {
            member_id: format!("member-{pod:02}-{worker}"),
            pod_id: Some(format!("pod-{:02}", 20 - pod)),
            cluster_id: Some(if pod < 7 { "small" } else { "large" }.to_string()),
          })
        })
        .collect::<Vec<_>>();
      let previous = partitions
        .iter()
        .map(|partition| {
          let logical = partition % 64;
          let pod = if logical < 42 {
            logical as usize / 2
          } else if logical == 63 {
            9
          } else {
            (logical as usize - 42) * 3 + (partition / 64) as usize
          };
          (
            *partition,
            format!("member-{pod:02}-{}", logical as usize % workers),
          )
        })
        .collect::<HashMap<_, _>>();
      let members = old_members
        .iter()
        .filter(|member| {
          !member
            .member_id
            .starts_with(&format!("member-{departure:02}-"))
        })
        .cloned()
        .collect::<Vec<_>>();
      let assignment = cooperative_colocated_assignment_with_topology(
        &members,
        &old_members,
        &partitions,
        &previous,
        64,
      );
      let pods = members
        .iter()
        .map(|member| (member.member_id.clone(), member.pod_id.clone().unwrap()))
        .collect::<HashMap<_, _>>();
      let pod_owners = pods
        .values()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
      let partition_pods = assignment
        .iter()
        .map(|(partition, member)| (*partition, pods[member].clone()))
        .collect::<HashMap<_, _>>();
      assert_balanced(&partition_pods, &pod_owners, &partitions);
      assert_eq!(
        score(&partition_pods, &HashMap::new(), &pod_owners, 64).0,
        5
      );
      for pod in &pod_owners {
        let pod_workers = members
          .iter()
          .filter(|member| member.pod_id.as_ref() == Some(pod))
          .map(|member| member.member_id.clone())
          .collect::<Vec<_>>();
        let pod_partitions = partition_pods
          .iter()
          .filter_map(|(partition, assigned)| (assigned == pod).then_some(*partition))
          .collect::<Vec<_>>();
        let worker_assignment = assignment
          .iter()
          .filter(|(_, member)| pod_workers.contains(member))
          .map(|(partition, member)| (*partition, member.clone()))
          .collect();
        assert_balanced(&worker_assignment, &pod_workers, &pod_partitions);
      }
      let mut reversed_members = members.clone();
      reversed_members.reverse();
      reversed_members.extend_from_slice(&members);
      let mut reversed_inventory = partitions.clone();
      reversed_inventory.reverse();
      reversed_inventory.extend_from_slice(&partitions);
      assert_eq!(
        assignment,
        cooperative_colocated_assignment_with_topology(
          &reversed_members,
          &old_members,
          &reversed_inventory,
          &previous,
          64
        )
      );
      assert_eq!(
        assignment,
        cooperative_colocated_assignment_with_topology(
          &members,
          &members,
          &partitions,
          &assignment,
          64
        )
      );
    }
  }
}

#[test]
fn colocated_assignment_full_layout_departure_join_cycles() {
  let partitions = (0 .. 192).collect::<Vec<_>>();
  let owners = (0 .. 21)
    .map(|owner| format!("pod-{owner:02}"))
    .collect::<Vec<_>>();
  let started = Instant::now();
  for percent in [0, 10] {
    let optional_allowance = if percent == 0 { 0 } else { 19 };
    let mut previous =
      cooperative_colocated_assignment(&owners, &partitions, &HashMap::new(), &owners[0], 64);
    for departed in [0, 10, 20] {
      let survivors = owners
        .iter()
        .filter(|owner| **owner != owners[departed])
        .cloned()
        .collect::<Vec<_>>();
      let orphan_count = previous
        .values()
        .filter(|owner| **owner == owners[departed])
        .count();
      let baseline =
        cooperative_colocated_assignment(&survivors, &partitions, &previous, &survivors[0], 64);
      assert_balanced(&baseline, &survivors, &partitions);
      assert_eq!(
        placement_repair::changed_partitions(&baseline, &previous),
        orphan_count
      );
      assert_eq!(score(&baseline, &previous, &survivors, 64).1, 0);

      let assignment = cooperative_sticky_assignment(
        &survivors,
        &partitions,
        &previous,
        &survivors[0],
        64,
        percent,
      );
      assert_balanced(&assignment, &survivors, &partitions);
      assert_eq!(owner_loads(&assignment), owner_loads(&baseline));
      assert!(placement_repair::changed_partitions(&assignment, &baseline) <= optional_allowance);
      assert!(
        placement_repair::changed_partitions(&assignment, &previous)
          <= orphan_count + optional_allowance
      );
      let before = score(&baseline, &previous, &survivors, 64);
      let after = score(&assignment, &previous, &survivors, 64);
      assert!((after.0, after.2) <= (before.0, before.2));
      assert_eq!(
        assignment,
        cooperative_colocated_assignment(&survivors, &partitions, &assignment, &survivors[0], 64)
      );

      // A returning empty owner needs nine partitions. No other transfers are required to rebalance
      // the 20-to-21-owner layout; optional repair is measured against that complete join baseline.
      let join_baseline =
        cooperative_colocated_assignment(&owners, &partitions, &assignment, &owners[0], 64);
      assert_balanced(&join_baseline, &owners, &partitions);
      assert_eq!(
        placement_repair::changed_partitions(&join_baseline, &assignment),
        9
      );
      assert_eq!(
        join_baseline
          .values()
          .filter(|owner| **owner == owners[departed])
          .count(),
        9
      );
      assert!(join_baseline.iter().all(|(partition, owner)| {
        assignment.get(partition) == Some(owner) || owner == &owners[departed]
      }));
      let joined =
        cooperative_sticky_assignment(&owners, &partitions, &assignment, &owners[0], 64, percent);
      assert_balanced(&joined, &owners, &partitions);
      assert_eq!(owner_loads(&joined), owner_loads(&join_baseline));
      assert!(placement_repair::changed_partitions(&joined, &join_baseline) <= optional_allowance);
      assert!(placement_repair::changed_partitions(&joined, &assignment) <= 9 + optional_allowance);
      let before = score(&join_baseline, &assignment, &owners, 64);
      let after = score(&joined, &assignment, &owners, 64);
      assert!((after.0, after.2) <= (before.0, before.2));
      assert_eq!(
        joined,
        cooperative_colocated_assignment(&owners, &partitions, &joined, &owners[0], 64)
      );
      previous = joined;
    }
  }
  eprintln!(
    "192-partition departure/join fixture elapsed: {:?}",
    started.elapsed()
  );
}

#[test]
fn colocated_assignment_partial_pod_topology_keeps_logical_policy() {
  let owners = ["owner-a", "owner-b"].map(ToString::to_string);
  let members = vec![
    ConsumerGroupMember {
      member_id: owners[0].clone(),
      pod_id: Some("pod-a".to_string()),
      cluster_id: None,
    },
    ConsumerGroupMember {
      member_id: owners[1].clone(),
      pod_id: None,
      cluster_id: None,
    },
  ];
  let partitions = (0 .. 8).collect::<Vec<_>>();
  let assignment =
    cooperative_colocated_assignment_with_pods(&members, &partitions, &HashMap::new(), 4);
  assert_eq!(
    assignment,
    cooperative_colocated_assignment(&owners, &partitions, &HashMap::new(), &owners[0], 4)
  );
  for logical in 0 .. 4 {
    assert_eq!(assignment.get(&logical), assignment.get(&(logical + 4)));
  }
  let mut shuffled = members;
  shuffled.reverse();
  assert_eq!(
    assignment,
    cooperative_colocated_assignment_with_pods(&shuffled, &partitions, &HashMap::new(), 4)
  );
}

#[test]
fn colocation_repair_whole_groups_scan_inventory_once() {
  let owners = (0 .. 21)
    .map(|owner| format!("owner-{owner:02}"))
    .collect::<Vec<_>>();
  let domains = owners
    .iter()
    .map(|owner| (owner.clone(), owner.clone()))
    .collect();
  for (logical_count, copies) in [(768, 1), (1_536, 1), (3_072, 1), (1_029, 3)] {
    let partitions = (0 .. logical_count * copies).collect::<Vec<_>>();
    let baseline = partitions
      .iter()
      .map(|partition| {
        (
          *partition,
          owners[(*partition % logical_count) as usize % owners.len()].clone(),
        )
      })
      .collect::<HashMap<_, _>>();
    assert_balanced(&baseline, &owners, &partitions);
    let mut assignment = baseline.clone();
    let summary = placement_repair::consolidate(
      &mut assignment,
      &baseline,
      &domains,
      logical_count,
      placement_repair::movement_budget(partitions.len(), 10),
    );
    assert_eq!(assignment, baseline);
    assert_eq!(summary.attempted_groups, 0);
    assert_eq!(summary.consolidated_groups, 0);
    assert_eq!(summary.optional_moves, 0);
    assert_eq!(summary.occupancy_scans, 1);
  }
}

#[test]
fn colocation_repair_atomic_exchange_respects_allowance() {
  let owners = ["owner-a", "owner-b"].map(ToString::to_string);
  let domains = owners
    .iter()
    .map(|owner| (owner.clone(), owner.clone()))
    .collect();
  let initial = (0 .. 8)
    .map(|partition| (partition, owners[(partition / 4) as usize].clone()))
    .collect::<HashMap<_, _>>();
  // Individual exchanges cannot close either four-partition group. The complete consolidation
  // changes four owners, so every smaller allowance must reject the entire proposal atomically.
  for budget in 0 .. 4 {
    let mut unchanged = initial.clone();
    let summary = placement_repair::consolidate(&mut unchanged, &initial, &domains, 2, budget);
    assert_eq!(summary.consolidated_groups, 0);
    assert_eq!(unchanged, initial);
  }
  let mut repaired = initial.clone();
  let summary = placement_repair::consolidate(&mut repaired, &initial, &domains, 2, 4);
  assert_eq!(summary.consolidated_groups, 1);
  assert_eq!(summary.occupancy_scans, 2);
  assert_eq!(summary.optional_moves, 4);
  assert_eq!(score(&repaired, &initial, &owners, 2), (0, 4, 0));
  assert_balanced(&repaired, &owners, &(0 .. 8).collect::<Vec<_>>());
}

#[test]
fn colocation_repair_percentage_budget_is_bounded_and_overflow_safe() {
  for (partitions, percent, expected) in [
    (0, 10, 0),
    (1, 10, 1),
    (2, 10, 2),
    (19, 10, 2),
    (136, 10, 13),
    (136, 0, 0),
    (31, 100, 31),
    (usize::MAX, 10, usize::MAX / 10),
  ] {
    assert_eq!(
      placement_repair::movement_budget(partitions, percent),
      expected
    );
  }
}

#[test]
fn colocation_repair_large_assignment_preserves_capacity() {
  let owners = (0 .. 21)
    .map(|owner| format!("owner-{owner:02}"))
    .collect::<Vec<_>>();
  let domains = owners
    .iter()
    .map(|owner| (owner.clone(), owner.clone()))
    .collect();
  let partitions = (0 .. 192).collect::<Vec<_>>();
  let initial = partitions
    .iter()
    .map(|partition| {
      (
        *partition,
        owners[*partition as usize % owners.len()].clone(),
      )
    })
    .collect::<HashMap<_, _>>();
  for budget in [1, 16, 64] {
    let mut assignment = initial.clone();
    let summary = placement_repair::consolidate(&mut assignment, &initial, &domains, 64, budget);
    assert!(summary.optional_moves <= budget);
    assert_balanced(&assignment, &owners, &partitions);
    let actual = score(&assignment, &initial, &owners, 64);
    let before = score(&initial, &initial, &owners, 64);
    assert!((actual.0, actual.2) <= (before.0, before.2));
    for owner in &owners {
      assert_eq!(
        assignment
          .values()
          .filter(|assigned| *assigned == owner)
          .count(),
        initial
          .values()
          .filter(|assigned| *assigned == owner)
          .count()
      );
    }
  }
}

#[test]
fn placement_movement_summary_distinguishes_orphans_and_known_pod_moves() {
  let old_owners = ["a", "b", "departed"].map(ToString::to_string);
  let old_topology = old_owners
    .iter()
    .map(|member| ConsumerGroupMember {
      member_id: member.clone(),
      pod_id: Some(if member == "b" { "pod-b" } else { "pod-a" }.to_string()),
      cluster_id: None,
    })
    .collect::<Vec<_>>();
  let mut previous = assignment_plan(
    1,
    &old_owners,
    &[0, 1, 2],
    &HashMap::from([
      (0, "departed".to_string()),
      (1, "a".to_string()),
      (2, "b".to_string()),
    ]),
    Some(old_topology.clone()),
    "a",
    0,
  );
  let owners = ["a", "b"].map(ToString::to_string);
  let topology = old_topology
    .into_iter()
    .filter(|member| member.member_id != "departed")
    .collect();
  let plan = assignment_plan(
    2,
    &owners,
    &[0, 1, 2, 3],
    &HashMap::from([
      (0, "a".to_string()),
      (1, "b".to_string()),
      (2, "b".to_string()),
      (3, "a".to_string()),
    ]),
    Some(topology),
    "a",
    0,
  );
  assert_eq!(
    assignment_movement_summary(&plan, Some(&previous)),
    serde_json::json!({
      "orphan_placements": 2, "survivor_moves": 1, "cross_pod_moves": 1,
      "intra_pod_moves": 1, "unknown_pod_moves": 0,
    })
  );
  previous.member_topology = None;
  assert_eq!(
    assignment_movement_summary(&plan, Some(&previous)),
    serde_json::json!({
      "orphan_placements": 2, "survivor_moves": 1, "cross_pod_moves": 0,
      "intra_pod_moves": 0, "unknown_pod_moves": 2,
    })
  );
}
