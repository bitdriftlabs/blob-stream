//! Broker discovery abstractions and implementations.
//!
//! Use this crate to obtain broker membership snapshots and to map virtual partitions to owners
//! with rendezvous hashing.

#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

pub mod k8s;
pub mod r#static;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use blob_stream_proto::protos::blobstream::v1::config::BrokerDiscoveryConfig;
use protobuf::Chars;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Maximum startup delay while waiting for discovery's authoritative initial snapshot.
pub const INITIAL_MEMBERSHIP_TIMEOUT: Duration = Duration::from_secs(10);

//
// BrokerNode
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// A single broker node in membership state.
pub struct BrokerNode {
  /// Stable broker identity used for rendezvous hashing.
  pub node_id: Chars,
  /// Network address used by producer/broker clients.
  pub address: Chars,
}

//
// BrokerMembership
//

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Current broker membership view.
pub enum BrokerMembership {
  /// Discovery has not delivered its initial membership snapshot.
  #[default]
  Pending,
  /// An authoritative membership snapshot, which may contain no nodes.
  Initialized(Vec<BrokerNode>),
}

impl BrokerMembership {
  /// Build an initialized membership snapshot from nodes.
  #[must_use]
  pub fn new(nodes: Vec<BrokerNode>) -> Self {
    Self::Initialized(nodes)
  }

  #[must_use]
  /// Return nodes from an initialized snapshot, or `None` while discovery is pending.
  pub fn nodes(&self) -> Option<&[BrokerNode]> {
    match self {
      Self::Pending => None,
      Self::Initialized(nodes) => Some(nodes),
    }
  }
}

//
// BrokerPartition
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// A virtual partition that requires a broker assignment.
pub struct BrokerPartition {
  /// Topic containing the virtual partition.
  pub topic: Chars,
  /// Virtual partition ID, including the producer writer offset.
  pub virtual_partition_id: u32,
}

//
// writer_virtual_partitions
//

#[must_use]
/// Build the virtual-partition inventory for one configured producer writer.
pub fn writer_virtual_partitions(
  topics: impl IntoIterator<Item = (Chars, u32, u32)>,
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

/// Build the configured static or Kubernetes discovery source used by all broker clients.
pub fn discovery_from_config(config: &BrokerDiscoveryConfig) -> Result<Arc<dyn BrokerDiscovery>> {
  if config.has_static() {
    let nodes = config
      .static_()
      .nodes
      .iter()
      .map(|node| BrokerNode {
        node_id: node.node_id.clone(),
        address: node.address.clone(),
      })
      .collect();
    return Ok(Arc::new(r#static::StaticBrokerDiscovery::new(nodes)));
  }
  if config.has_k8s_service() {
    let k8s = config.k8s_service();
    return Ok(Arc::new(k8s::K8sServiceBrokerDiscovery::new(
      k8s.namespace.to_string(),
      k8s.service_name.to_string(),
    )));
  }
  Err(anyhow::anyhow!(
    "broker discovery backend is required (static or k8s_service)"
  ))
}

/// Select one stable local broker owner for a metadata window.
#[must_use]
pub fn metadata_window_owner(
  topic: &str,
  window_start_unix_seconds: i64,
  membership: &BrokerMembership,
) -> Option<BrokerNode> {
  canonical_nodes(membership)
    .into_iter()
    .max_by(|left, right| {
      metadata_window_score(topic, window_start_unix_seconds, &left.node_id)
        .cmp(&metadata_window_score(
          topic,
          window_start_unix_seconds,
          &right.node_id,
        ))
        .then_with(|| right.node_id.cmp(&left.node_id))
    })
}

/// Select one stable local broker owner for an immutable blob key.
#[must_use]
pub fn blob_key_owner(blob_key: &str, membership: &BrokerMembership) -> Option<BrokerNode> {
  canonical_nodes(membership)
    .into_iter()
    .max_by(|left, right| {
      blob_key_score(blob_key, &left.node_id)
        .cmp(&blob_key_score(blob_key, &right.node_id))
        .then_with(|| right.node_id.cmp(&left.node_id))
    })
}

fn canonical_nodes(membership: &BrokerMembership) -> Vec<BrokerNode> {
  let mut nodes = membership.nodes().unwrap_or_default().to_vec();
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

fn metadata_window_score(topic: &str, window_start_unix_seconds: i64, node_id: &str) -> u64 {
  let mut hasher = DefaultHasher::new();
  topic.hash(&mut hasher);
  window_start_unix_seconds.hash(&mut hasher);
  node_id.hash(&mut hasher);
  hasher.finish()
}

fn blob_key_score(blob_key: &str, node_id: &str) -> u64 {
  let mut hasher = DefaultHasher::new();
  blob_key.hash(&mut hasher);
  node_id.hash(&mut hasher);
  hasher.finish()
}

//
// BrokerDiscovery
//

#[async_trait]
/// Discovery source that provides watchable membership updates.
pub trait BrokerDiscovery: Send + Sync {
  /// Return a watch receiver updated with membership changes.
  ///
  /// A receiver may initially contain [`BrokerMembership::Pending`]. Callers must wait for an
  /// [`BrokerMembership::Initialized`] snapshot before treating membership as authoritative.
  async fn watch_membership(&self) -> Result<watch::Receiver<BrokerMembership>>;
}

/// Wait for the first authoritative membership snapshot from a discovery watch.
pub async fn wait_for_initialized_membership(
  membership_rx: &mut watch::Receiver<BrokerMembership>,
) -> Result<BrokerMembership> {
  loop {
    // Pending is not an authoritative empty membership, so routes must not be created from it.
    let membership = membership_rx.borrow_and_update().clone();
    if matches!(membership, BrokerMembership::Initialized(_)) {
      return Ok(membership);
    }

    membership_rx
      .changed()
      .await
      .map_err(|_| anyhow!("broker discovery closed before initial membership was available"))?;
  }
}
