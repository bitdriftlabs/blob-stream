//! One deterministic consolidation attempt per split logical group, with no recursive search.
//!
//! The input is a complete sticky assignment. A proposal collects one group's partitions on the
//! domain already holding most of them, exchanging other partitions back to their source workers.
//! Exchanging actual worker owners preserves every worker's load and therefore every pod's load.
//! A domain can be a pod or an individual worker; the same pass serves both placement layers.
//!
//! Proposals are atomic: only the complete result is scored and accepted. This permits useful
//! multi-partition repairs without exploring permutations or temporarily accepting a worse plan.
//! All passes compare movement against the same full-plan sticky baseline. The allowance limits
//! final changed virtual partitions, not swap operations or changes at each topology layer.
//!
//! This is deliberately a one-pass heuristic, not an optimizer: it may miss a better destination,
//! a different combination of returns, or an improvement to an earlier group after later repairs.
//! Repeated deployments do not guarantee convergence to optimal co-location. Movement allowance
//! and computational work are separate bounds; complete scoring and copying still scale with the
//! inventory, but candidate count cannot expand into recursive or permutation-based search.

use blob_stream_types::VirtualPartitionId;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

type Assignment = HashMap<VirtualPartitionId, String>;
type Groups = Vec<(u32, Vec<VirtualPartitionId>)>;
type DomainCounts<'a> = BTreeMap<u32, BTreeMap<&'a String, usize>>;

//
// RepairSummary
//

#[derive(Debug, Default)]
pub(super) struct RepairSummary {
  pub attempted_groups: usize,
  pub consolidated_groups: usize,
  pub occupancy_scans: usize,
  pub optional_moves: usize,
}

/// Derive a plan-wide allowance from canonical inventory, rounding the percentage down. Zero
/// disables repair; otherwise two slots permit the smallest capacity-preserving exchange. Divide
/// before multiplying so even a large inventory cannot overflow percentage arithmetic.
pub(super) fn movement_budget(partitions: usize, percent: u8) -> usize {
  debug_assert!(percent <= 100);
  if percent == 0 || partitions == 0 {
    return 0;
  }
  let percent = usize::from(percent);
  (partitions / 100 * percent + partitions % 100 * percent / 100)
    .max(2)
    .min(partitions)
}

/// Count unique final ownership differences, rather than accumulating intermediate exchanges.
/// Reusing a changed partition does not spend another slot; changing it back releases its slot.
pub(super) fn changed_partitions(assignment: &Assignment, baseline: &Assignment) -> usize {
  assignment
    .iter()
    .filter(|(partition, owner)| baseline.get(partition) != Some(*owner))
    .count()
}

/// Domains describe the workers participating in this pass. A pod pass includes every worker;
/// a worker pass includes only one pod, so partitions elsewhere cannot become return candidates.
pub(super) fn consolidate(
  assignment: &mut Assignment,
  baseline: &Assignment,
  domains: &BTreeMap<String, String>,
  logical_count: u32,
  budget: usize,
) -> RepairSummary {
  debug_assert!(logical_count > 0);
  debug_assert_eq!(assignment.len(), baseline.len());
  let mut summary = RepairSummary::default();
  if budget == 0 {
    return summary;
  }
  let mut groups = BTreeMap::<u32, Vec<VirtualPartitionId>>::new();
  for (partition, worker) in assignment.iter() {
    if domains.contains_key(worker) {
      groups
        .entry(partition % logical_count)
        .or_default()
        .push(*partition);
    }
  }
  let mut groups = groups.into_iter().collect::<Groups>();
  for (_, partitions) in &mut groups {
    partitions.sort_unstable();
  }
  groups.sort_by_key(|(logical, partitions)| (Reverse(partitions.len()), *logical));

  // Keep the accepted assignment's complete occupancy and score. Rejected proposals cannot change
  // either, and already-whole groups need no inventory rescans as this pass visits them.
  let mut counts = domain_counts(&groups, assignment, domains, &mut summary);
  let mut current_quality = quality(&counts);
  // A single canonical pass bounds candidate construction by the inventory and group count. We
  // never revisit earlier groups, try alternative destinations, or enumerate return combinations.
  for (logical, group) in &groups {
    let group_counts = &counts[logical];
    if group_counts.len() <= 1 {
      continue;
    }
    summary.attempted_groups += 1;
    let destination = group_counts
      .iter()
      .min_by_key(|(domain, count)| (Reverse(**count), *domain))
      .map(|(domain, _)| *domain)
      .expect("split group has an occupied domain");
    let mut proposal = assignment.clone();
    let missing = group
      .iter()
      .filter(|partition| &domains[&assignment[*partition]] != destination)
      .copied()
      .collect::<Vec<_>>();

    // Pick one return partition for each incoming partition. Prefer removing a fragment over
    // breaking a whole group, then prefer a return whose group already occupies the source domain.
    // Counts are taken before construction and intentionally remain fixed during this proposal.
    // These are deterministic hints, not correctness assumptions: the full proposal's score below
    // accounts for every group it helps or harms, including interactions between return choices.
    let mut complete = true;
    for partition in missing {
      let source_worker = proposal[&partition].clone();
      let source_domain = &domains[&source_worker];
      let return_partition = proposal
        .iter()
        .filter(|(candidate, worker)| {
          *candidate % logical_count != *logical && domains.get(*worker) == Some(destination)
        })
        .min_by_key(|(candidate, _)| {
          let return_counts = &counts[&(*candidate % logical_count)];
          (
            return_counts.len() == 1,
            Reverse(
              return_counts
                .get(source_domain)
                .copied()
                .unwrap_or_default(),
            ),
            **candidate,
          )
        })
        .map(|(candidate, _)| *candidate);
      let Some(return_partition) = return_partition else {
        // The destination has insufficient total capacity for this group. Discard the proposal,
        // including any earlier exchanges, rather than publishing a partial consolidation.
        complete = false;
        break;
      };
      let destination_worker = proposal[&return_partition].clone();
      proposal.insert(partition, destination_worker);
      proposal.insert(return_partition, source_worker);
    }
    if !complete || changed_partitions(&proposal, baseline) > budget {
      continue;
    }

    // Only locality is an optional-repair objective. Movement is a hard allowance, and canonical
    // order is only a construction tie-break: equally good plans do not justify operational churn.
    // Copies make rejection trivial and keep the accepted assignment untouched until validation.
    let proposal_counts = domain_counts(&groups, &proposal, domains, &mut summary);
    let proposal_quality = quality(&proposal_counts);
    if proposal_quality < current_quality {
      *assignment = proposal;
      counts = proposal_counts;
      current_quality = proposal_quality;
      summary.consolidated_groups += 1;
    }
  }
  summary.optional_moves = changed_partitions(assignment, baseline);
  summary
}

/// Recompute complete candidate occupancy without maintaining partial-exchange or rollback state.
/// Count these scans so no-op work can be checked independently of machine timing.
fn domain_counts<'a>(
  groups: &Groups,
  assignment: &Assignment,
  domains: &'a BTreeMap<String, String>,
  summary: &mut RepairSummary,
) -> DomainCounts<'a> {
  summary.occupancy_scans += 1;
  groups
    .iter()
    .map(|(logical, partitions)| {
      let mut counts = BTreeMap::new();
      for partition in partitions {
        *counts.entry(&domains[&assignment[partition]]).or_default() += 1;
      }
      (*logical, counts)
    })
    .collect()
}

/// Fewer split groups wins; at equal split count, fewer owners beyond the first split wins.
/// Survivor preservation belongs to the sticky baseline and the explicit movement allowance.
fn quality(counts: &DomainCounts<'_>) -> (usize, usize) {
  counts.values().fold((0, 0), |(splits, fragments), owners| {
    (
      splits + usize::from(owners.len() > 1),
      fragments + owners.len().saturating_sub(2),
    )
  })
}
