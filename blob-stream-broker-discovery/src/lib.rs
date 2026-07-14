//! Broker discovery abstractions and implementations.
//!
//! Use this crate to obtain broker membership snapshots and to map virtual partitions to owners
//! with rendezvous hashing.

#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

pub mod k8s;
pub mod r#static;

use anyhow::Result;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use tokio::sync::watch;

//
// BrokerNode
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// A single broker node in membership state.
pub struct BrokerNode {
  /// Stable broker identity used for rendezvous hashing.
  pub node_id: String,
  /// Network address used by producer/broker clients.
  pub address: String,
}

//
// BrokerMembership
//

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Current broker membership view.
pub struct BrokerMembership {
  /// Known broker nodes.
  pub nodes: Vec<BrokerNode>,
}

impl BrokerMembership {
  /// Build a membership set from nodes.
  #[must_use]
  pub fn new(nodes: Vec<BrokerNode>) -> Self {
    Self { nodes }
  }
}

//
// BrokerPartition
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// A virtual partition that requires a broker assignment.
pub struct BrokerPartition {
  /// Topic containing the virtual partition.
  pub topic: String,
  /// Virtual partition ID, including the producer writer offset.
  pub virtual_partition_id: u32,
}

//
// writer_virtual_partitions
//

#[must_use]
/// Build the virtual-partition inventory for one configured producer writer.
pub fn writer_virtual_partitions(
  topics: impl IntoIterator<Item = (String, u32, u32)>,
  writer_id: u32,
) -> Vec<BrokerPartition> {
  let mut partitions = Vec::new();
  for (topic, partition_count, num_writers) in topics {
    assert!(
      writer_id < num_writers,
      "writer_id {writer_id} is outside topic {topic} writer range {num_writers}"
    );
    let virtual_partition_start = writer_id
      .checked_mul(partition_count)
      .expect("topic virtual partition offset exceeds u32");
    let virtual_partition_end = virtual_partition_start
      .checked_add(partition_count)
      .expect("topic virtual partition count exceeds u32");
    for virtual_partition_id in virtual_partition_start .. virtual_partition_end {
      partitions.push(BrokerPartition {
        topic: topic.clone(),
        virtual_partition_id,
      });
    }
  }
  partitions
}

//
// balanced_assignment
//

#[must_use]
/// Assign every requested virtual partition to a broker while keeping loads within one partition.
pub fn balanced_assignment(
  partitions: impl IntoIterator<Item = BrokerPartition>,
  membership: &BrokerMembership,
) -> BTreeMap<BrokerPartition, BrokerNode> {
  let partitions = partitions.into_iter().collect::<BTreeSet<_>>();
  let nodes = canonical_nodes(membership);
  if nodes.is_empty() {
    return BTreeMap::new();
  }

  let mut loads = nodes
    .iter()
    .map(|node| (node.node_id.as_str(), 0_usize))
    .collect::<HashMap<_, _>>();
  let mut assignments = BTreeMap::new();

  for partition in partitions {
    let minimum_load = loads.values().copied().min().unwrap_or_default();
    let owner = nodes
      .iter()
      .filter(|node| loads.get(node.node_id.as_str()) == Some(&minimum_load))
      .max_by(|left, right| {
        rendezvous_score(&partition, &left.node_id)
          .cmp(&rendezvous_score(&partition, &right.node_id))
          .then_with(|| right.node_id.cmp(&left.node_id))
      })
      .expect("non-empty canonical membership");

    *loads.entry(owner.node_id.as_str()).or_default() += 1;
    assignments.insert(partition, owner.clone());
  }

  assignments
}

fn canonical_nodes(membership: &BrokerMembership) -> Vec<BrokerNode> {
  let mut nodes = membership.nodes.clone();
  nodes.sort_unstable_by(|left, right| {
    left
      .node_id
      .cmp(&right.node_id)
      .then_with(|| left.address.cmp(&right.address))
  });
  nodes.dedup_by(|left, right| left.node_id == right.node_id);
  nodes
}

fn rendezvous_score(partition: &BrokerPartition, node_id: &str) -> u64 {
  let mut hasher = DefaultHasher::new();
  partition.topic.hash(&mut hasher);
  partition.virtual_partition_id.hash(&mut hasher);
  node_id.hash(&mut hasher);
  hasher.finish()
}

//
// BrokerDiscovery
//

#[async_trait]
/// Discovery source that provides watchable membership updates.
pub trait BrokerDiscovery: Send + Sync {
  /// Return a watch receiver seeded with current membership and updated on membership changes.
  async fn watch_membership(&self) -> Result<watch::Receiver<BrokerMembership>>;
}
