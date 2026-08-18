#![allow(clippy::unwrap_used)]

use super::r#static::StaticBrokerDiscovery;
use super::{
  BrokerDiscovery,
  BrokerMembership,
  BrokerNode,
  BrokerPartition,
  balanced_assignment,
  metadata_window_owner,
  wait_for_initialized_membership,
  writer_virtual_partitions,
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::watch;

#[test]
fn membership_distinguishes_pending_from_known_empty() {
  assert!(BrokerMembership::default().nodes().is_none());
  assert_eq!(
    BrokerMembership::new(Vec::new()).nodes(),
    Some([].as_slice())
  );
}

#[tokio::test]
async fn initialized_membership_wait_returns_authoritative_snapshot() -> Result<()> {
  let expected = BrokerMembership::new(Vec::new());
  let (_sender, mut receiver) = watch::channel(expected.clone());

  assert_eq!(
    wait_for_initialized_membership(&mut receiver).await?,
    expected
  );
  Ok(())
}

#[tokio::test]
async fn initialized_membership_wait_fails_when_pending_watch_closes() {
  let (sender, mut receiver) = watch::channel(BrokerMembership::Pending);
  drop(sender);

  assert!(
    wait_for_initialized_membership(&mut receiver)
      .await
      .is_err()
  );
}

#[tokio::test]
async fn static_discovery_emits_membership() -> Result<()> {
  let nodes = vec![
    BrokerNode {
      node_id: "node-a".into(),
      address: "10.0.0.1:8080".into(),
    },
    BrokerNode {
      node_id: "node-b".into(),
      address: "10.0.0.2:8080".into(),
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
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  };
  let node_b = BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
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
fn metadata_window_owner_is_stable_for_reordered_and_empty_membership() {
  let node_a = BrokerNode {
    node_id: "node-a".into(),
    address: "10.0.0.1:8080".into(),
  };
  let node_b = BrokerNode {
    node_id: "node-b".into(),
    address: "10.0.0.2:8080".into(),
  };
  let node_c = BrokerNode {
    node_id: "node-c".into(),
    address: "10.0.0.3:8080".into(),
  };
  let ordered = BrokerMembership::new(vec![node_a.clone(), node_b.clone(), node_c.clone()]);
  let reordered = BrokerMembership::new(vec![node_c, node_a, node_b]);

  assert_eq!(
    metadata_window_owner("telemetry", 1_700_000_000, &ordered),
    metadata_window_owner("telemetry", 1_700_000_000, &reordered)
  );
  assert_eq!(
    metadata_window_owner("telemetry", 1_700_000_000, &BrokerMembership::Pending),
    None
  );
  assert_eq!(
    metadata_window_owner(
      "telemetry",
      1_700_000_000,
      &BrokerMembership::new(Vec::new()),
    ),
    None
  );
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
