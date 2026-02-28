// blob-stream - broker discovery tests
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::r#static::StaticBrokerDiscovery;
use super::{BrokerDiscovery, BrokerMembership, BrokerNode, owner_for_partition};
use anyhow::Result;

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
  Ok(())
}

#[test]
fn owner_for_partition_is_stable() {
  let membership = BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".to_string(),
      address: "10.0.0.1:8080".to_string(),
    },
    BrokerNode {
      node_id: "node-b".to_string(),
      address: "10.0.0.2:8080".to_string(),
    },
  ]);

  let owner_one = owner_for_partition("telemetry", 42, &membership).unwrap();
  let owner_two = owner_for_partition("telemetry", 42, &membership).unwrap();

  assert_eq!(owner_one, owner_two);
}

#[test]
fn owner_for_partition_none_when_membership_empty() {
  let membership = BrokerMembership::new(Vec::new());
  assert!(owner_for_partition("telemetry", 42, &membership).is_none());
}
