// blob-stream - static broker discovery
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{BrokerDiscovery, BrokerMembership, BrokerNode};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

//
// StaticBrokerDiscovery
//

#[derive(Clone, Debug)]
pub struct StaticBrokerDiscovery {
  nodes: Vec<BrokerNode>,
}

impl StaticBrokerDiscovery {
  #[must_use]
  pub fn new(nodes: Vec<BrokerNode>) -> Self {
    Self { nodes }
  }
}

#[async_trait]
impl BrokerDiscovery for StaticBrokerDiscovery {
  async fn watch_membership(&self) -> Result<watch::Receiver<BrokerMembership>> {
    let membership = BrokerMembership::new(self.nodes.clone());
    let (tx, rx) = watch::channel(membership);
    drop(tx);
    Ok(rx)
  }
}
