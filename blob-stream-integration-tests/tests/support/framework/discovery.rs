use anyhow::Result;
use async_trait::async_trait;
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, BrokerNode};
use blob_stream_consumer::{ConsumerCoordinationSource, CoordinationSnapshot};
use blob_stream_types::VirtualPartitionId;
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

//
// DynamicCoordinationSource
//

#[derive(Clone)]
pub struct DynamicCoordinationSource {
  tx: watch::Sender<CoordinationSnapshot>,
}

impl DynamicCoordinationSource {
  pub fn new(members: Vec<String>, virtual_partitions: Vec<VirtualPartitionId>) -> Self {
    let snapshot = CoordinationSnapshot {
      members,
      virtual_partitions,
    };
    let (tx, _rx) = watch::channel(snapshot);
    Self { tx }
  }

  pub fn update_members(&self, members: Vec<String>) {
    let current = self.tx.borrow().clone();
    let snapshot = CoordinationSnapshot {
      members,
      virtual_partitions: current.virtual_partitions,
    };
    let _previous = self.tx.send_replace(snapshot);
  }
}

#[async_trait]
impl ConsumerCoordinationSource for DynamicCoordinationSource {
  async fn snapshot(&self) -> Result<CoordinationSnapshot> {
    Ok(self.tx.borrow().clone())
  }
}
