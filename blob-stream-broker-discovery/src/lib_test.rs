#![allow(clippy::unwrap_used)]

use super::r#static::StaticBrokerDiscovery;
use super::{
  BrokerDiscovery,
  BrokerMembership,
  BrokerNode,
  BrokerPartition,
  balanced_assignment,
  writer_virtual_partitions,
};
use anyhow::Result;
use std::collections::HashMap;

#[test]
fn membership_distinguishes_pending_from_known_empty() {
  assert!(BrokerMembership::default().nodes().is_none());
  assert_eq!(
    BrokerMembership::new(Vec::new()).nodes(),
    Some([].as_slice())
  );
}

#[tokio::test]
async fn static_discovery_emits_membership() -> Result<()> {
  let nodes = vec![
    BrokerNode {
      node_id: "node-a".to_string(),
      address: "10.0.0.1:8080".to_string(),
    },
    BrokerNode {
      node_id: "node-b".to_string(),
      address: "10.0.0.2:8080".to_string(),
    },
  ];

  let discovery = StaticBrokerDiscovery::new(nodes.clone());
  let receiver = discovery.watch_membership().await?;

  let expected = BrokerMembership::new(nodes);
  assert_eq!(*receiver.borrow(), expected);
  assert_eq!(
    receiver.borrow().nodes(),
    Some(expected.nodes().unwrap_or_default())
  );
  Ok(())
}

#[test]
fn balanced_assignment_is_fair_and_independent_of_input_order() {
  let node_a = BrokerNode {
    node_id: "node-a".to_string(),
    address: "10.0.0.1:8080".to_string(),
  };
  let node_b = BrokerNode {
    node_id: "node-b".to_string(),
    address: "10.0.0.2:8080".to_string(),
  };
  let partitions = vec![
    BrokerPartition {
      topic: "client_reports".into(),
      virtual_partition_id: 0,
    },
    BrokerPartition {
      topic: "client_reports".into(),
      virtual_partition_id: 1,
    },
    BrokerPartition {
      topic: "dif_uploads".into(),
      virtual_partition_id: 0,
    },
    BrokerPartition {
      topic: "dif_uploads".into(),
      virtual_partition_id: 1,
    },
    BrokerPartition {
      topic: "insights".into(),
      virtual_partition_id: 0,
    },
    BrokerPartition {
      topic: "insights".into(),
      virtual_partition_id: 1,
    },
    BrokerPartition {
      topic: "logging".into(),
      virtual_partition_id: 0,
    },
    BrokerPartition {
      topic: "logging".into(),
      virtual_partition_id: 1,
    },
  ];

  let assignment = balanced_assignment(
    partitions.clone(),
    &BrokerMembership::new(vec![node_a.clone(), node_b.clone()]),
  );
  let reversed_assignment = balanced_assignment(
    partitions.into_iter().rev(),
    &BrokerMembership::new(vec![node_b, node_a]),
  );

  assert_eq!(assignment, reversed_assignment);
  assert_eq!(assignment.len(), 8);

  let loads = assignment.values().fold(HashMap::new(), |mut loads, node| {
    *loads.entry(node.node_id.as_str()).or_insert(0_usize) += 1;
    loads
  });
  assert_eq!(loads.get("node-a"), Some(&4));
  assert_eq!(loads.get("node-b"), Some(&4));
}

#[test]
fn writer_virtual_partitions_selects_only_the_requested_writer_range() {
  let partitions = writer_virtual_partitions([("telemetry".into(), 2, 3)], 1);

  assert_eq!(
    partitions,
    vec![
      BrokerPartition {
        topic: "telemetry".into(),
        virtual_partition_id: 2,
      },
      BrokerPartition {
        topic: "telemetry".into(),
        virtual_partition_id: 3,
      },
    ]
  );
}
