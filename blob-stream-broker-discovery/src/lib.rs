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
use std::cmp::Reverse;
use std::collections::hash_map::DefaultHasher;
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
// owner_for_partition
//

#[must_use]
/// Resolve the owner node for a virtual partition using rendezvous hashing.
pub fn owner_for_partition<'a>(
  topic: &str,
  virtual_partition_id: u32,
  membership: &'a BrokerMembership,
) -> Option<&'a BrokerNode> {
  // Rendezvous (highest-random-weight) hashing: for each candidate node, hash the tuple
  // (topic, virtual_partition_id, node_id) and choose the node with the highest score.
  // We model "highest" with Reverse(score) + min_by_key so ordering is deterministic and
  // stable for equal membership views across producers and brokers.
  membership
    .nodes
    .iter()
    .map(|node| {
      // We intentionally hash node_id (not address) so ownership only changes when logical
      // membership changes, not when endpoint strings are reformatted.
      let mut hasher = DefaultHasher::new();
      topic.hash(&mut hasher);
      virtual_partition_id.hash(&mut hasher);
      node.node_id.hash(&mut hasher);
      (Reverse(hasher.finish()), node)
    })
    .min_by_key(|(score, _)| *score)
    .map(|(_, node)| node)
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
