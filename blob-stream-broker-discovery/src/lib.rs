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
// BrokerDiscovery
//

#[async_trait]
pub trait BrokerDiscovery: Send + Sync {
  async fn watch_membership(&self) -> Result<watch::Receiver<BrokerMembership>>;
}
