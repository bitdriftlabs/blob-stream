// blob-stream - broker discovery
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

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
pub struct BrokerNode {
  pub node_id: String,
  pub address: String,
}

//
// BrokerMembership
//

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BrokerMembership {
  pub nodes: Vec<BrokerNode>,
}

impl BrokerMembership {
  #[must_use]
  pub fn new(nodes: Vec<BrokerNode>) -> Self {
    Self { nodes }
  }
}

//
// owner_for_partition
//

#[must_use]
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
pub trait BrokerDiscovery: Send + Sync {
  async fn watch_membership(&self) -> Result<watch::Receiver<BrokerMembership>>;
}
