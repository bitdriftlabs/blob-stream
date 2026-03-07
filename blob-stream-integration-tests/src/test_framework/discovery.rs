use anyhow::Result;
use async_trait::async_trait;
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, BrokerNode};
use tokio::sync::watch;

//
// DynamicBrokerDiscovery
//

#[derive(Clone)]
pub struct DynamicBrokerDiscovery {
  tx: watch::Sender<BrokerMembership>,
}

impl DynamicBrokerDiscovery {
  pub fn new(nodes: Vec<BrokerNode>) -> Self {
    let (tx, _rx) = watch::channel(BrokerMembership::new(nodes));
    Self { tx }
  }

  pub fn update_nodes(&self, nodes: Vec<BrokerNode>) {
    let _ignored = self.tx.send(BrokerMembership::new(nodes));
  }
}

#[async_trait]
impl BrokerDiscovery for DynamicBrokerDiscovery {
  async fn watch_membership(&self) -> Result<watch::Receiver<BrokerMembership>> {
    Ok(self.tx.subscribe())
  }
}
