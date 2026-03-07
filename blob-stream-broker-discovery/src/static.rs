use crate::{BrokerDiscovery, BrokerMembership, BrokerNode};
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
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
    debug!(
      "static discovery watch initialized with {} node(s)",
      self.nodes.len()
    );
    let membership = BrokerMembership::new(self.nodes.clone());
    let (tx, rx) = watch::channel(membership);
    drop(tx);
    Ok(rx)
  }
}
