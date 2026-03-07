use super::discovery::DynamicBrokerDiscovery;
use super::event_log::{TestEvent, TestEventLog, TestEventMatcher};
use super::resources::IntegrationResources;
use super::transport::{
  BrokerEndpoint,
  BrokerEndpointBinding,
  BrokerTransport,
  GrpcTcpTransport,
  InMemoryTestTransport,
  NetworkFaultController,
};
use crate::framework::{PARTITION_COUNT, SECOND_TOPIC, TOPIC};
use anyhow::{Result, anyhow};
use blob_stream::grpc::make_broker_router;
use blob_stream::metrics::BrokerMetrics;
use blob_stream::write::{TopicInfo, WriteConfig, WriteEngine, WriteEngineImpl};
use blob_stream_blob_store::BlobStore;
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{MetadataStore, ProducerPartitionLeaseStore};
use blob_stream_producer::{ProducerClientImpl, ProducerConfig, ProducerTopicConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;

//
// BrokerHandle
//

struct BrokerHandle {
  node: BrokerNode,
  shutdown_tx: Option<oneshot::Sender<()>>,
  serve_task: JoinHandle<()>,
}

impl BrokerHandle {
  async fn shutdown(&mut self) -> Result<()> {
    if let Some(tx) = self.shutdown_tx.take() {
      let _ = tx.send(());
    }

    let join_result = timeout(Duration::from_secs(5), &mut self.serve_task)
      .await
      .map_err(|_| anyhow!("broker graceful shutdown timed out"))?;
    join_result.map_err(|error| anyhow!("broker task join failed: {error}"))?;

    Ok(())
  }
}

//
// ClusterHarness
//

pub struct ClusterHarness {
  brokers: Vec<BrokerHandle>,
  #[allow(dead_code)]
  event_log: TestEventLog,
  producer_discovery: DynamicBrokerDiscovery,
  broker_membership_tx: watch::Sender<BrokerMembership>,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  topic_num_writers: u32,
  transport: Arc<dyn BrokerTransport>,
}

//
// ClusterHarnessBuilder
//

pub struct ClusterHarnessBuilder<'a> {
  resources: &'a IntegrationResources,
  broker_count: usize,
  metadata_store: Option<Arc<dyn MetadataStore>>,
  topic_num_writers: u32,
  transport: Arc<dyn BrokerTransport>,
}

impl ClusterHarnessBuilder<'_> {
  pub fn metadata_store(mut self, metadata_store: Arc<dyn MetadataStore>) -> Self {
    self.metadata_store = Some(metadata_store);
    self
  }

  pub fn topic_num_writers(mut self, topic_num_writers: u32) -> Self {
    self.topic_num_writers = topic_num_writers;
    self
  }

  #[allow(dead_code)]
  pub fn transport(mut self, transport: Arc<dyn BrokerTransport>) -> Self {
    self.transport = transport;
    self
  }

  #[allow(dead_code)]
  pub fn in_memory_transport(mut self) -> Self {
    self.transport = Arc::new(InMemoryTestTransport::new());
    self
  }

  pub async fn start(self) -> Result<ClusterHarness> {
    // The metadata store can be overridden by tests that need wrapped behavior.
    let metadata_store = self
      .metadata_store
      .unwrap_or_else(|| self.resources.metadata_store());

    ClusterHarness::start_from_builder(
      self.resources,
      self.broker_count,
      metadata_store,
      self.topic_num_writers,
      self.transport,
    )
    .await
  }
}

impl ClusterHarness {
  pub fn builder(
    resources: &IntegrationResources,
    broker_count: usize,
  ) -> ClusterHarnessBuilder<'_> {
    ClusterHarnessBuilder {
      resources,
      broker_count,
      metadata_store: None,
      topic_num_writers: 1,
      transport: Arc::new(GrpcTcpTransport),
    }
  }

  async fn start_from_builder(
    resources: &IntegrationResources,
    broker_count: usize,
    metadata_store: Arc<dyn MetadataStore>,
    topic_num_writers: u32,
    transport: Arc<dyn BrokerTransport>,
  ) -> Result<Self> {
    if topic_num_writers == 0 {
      return Err(anyhow!("topic_num_writers must be greater than zero"));
    }

    let event_log = TestEventLog::default();
    transport.install_event_log(event_log.clone()).await?;
    resources
      .store_fault_controller()
      .attach_event_log(event_log.clone())
      .await;

    let mut endpoints = Vec::with_capacity(broker_count);
    for broker_index in 0 .. broker_count {
      let holder_id = format!("broker-{broker_index}");
      endpoints.push(transport.bind_endpoint(&holder_id).await?);
    }

    let nodes: Vec<BrokerNode> = endpoints
      .iter()
      .map(|endpoint| endpoint.node.clone())
      .collect();

    let initial_nodes = vec![
      nodes
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("broker list cannot be empty"))?,
    ];
    let producer_discovery = DynamicBrokerDiscovery::new(initial_nodes.clone());
    let (broker_membership_tx, _broker_membership_rx) =
      watch::channel(BrokerMembership::new(initial_nodes));

    let blob_store = resources.blob_store();
    let lease_store = resources.producer_lease_store();

    let mut harness = Self {
      brokers: Vec::with_capacity(broker_count),
      event_log,
      producer_discovery,
      broker_membership_tx,
      blob_store,
      metadata_store,
      lease_store,
      topic_num_writers,
      transport,
    };

    for endpoint in endpoints {
      let broker = harness.spawn_broker(endpoint, topic_num_writers)?;
      harness.brokers.push(broker);
    }

    Ok(harness)
  }

  pub fn producer_discovery(&self) -> DynamicBrokerDiscovery {
    self.producer_discovery.clone()
  }

  #[allow(dead_code)]
  pub fn event_log(&self) -> TestEventLog {
    self.event_log.clone()
  }

  #[allow(dead_code)]
  pub async fn wait_for_event(
    &self,
    matcher: &TestEventMatcher,
    timeout: Duration,
  ) -> Result<TestEvent> {
    self.event_log.wait_for_event(matcher, timeout).await
  }

  #[allow(dead_code)]
  pub async fn assert_event_sequence_contains(&self, expected: &[TestEventMatcher]) -> Result<()> {
    self
      .event_log
      .assert_event_sequence_contains(expected)
      .await
  }

  #[allow(dead_code)]
  pub fn network_fault_controller(&self) -> Option<NetworkFaultController> {
    self.transport.fault_controller()
  }

  #[allow(dead_code)]
  pub async fn create_producer(
    &self,
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
  ) -> Result<ProducerClientImpl> {
    let discovery: Arc<dyn BrokerDiscovery> = Arc::new(self.producer_discovery());
    if let Some(transport) = self.transport.producer_transport() {
      ProducerClientImpl::new_with_transport(config, topics, discovery, transport).await
    } else {
      ProducerClientImpl::new(config, topics, discovery).await
    }
  }

  pub fn set_active_nodes(&self, nodes: Vec<BrokerNode>) {
    self.producer_discovery.update_nodes(nodes.clone());
    let _ = self.broker_membership_tx.send(BrokerMembership::new(nodes));
  }

  pub fn live_nodes(&self) -> Vec<BrokerNode> {
    self
      .brokers
      .iter()
      .map(|broker| broker.node.clone())
      .collect()
  }

  fn sync_membership(&self) {
    let nodes = self.live_nodes();
    self.producer_discovery.update_nodes(nodes.clone());
    let _ = self.broker_membership_tx.send(BrokerMembership::new(nodes));
  }

  fn spawn_broker(&self, endpoint: BrokerEndpoint, topic_num_writers: u32) -> Result<BrokerHandle> {
    let node = endpoint.node;
    let write_engine = build_write_engine(
      node.node_id.clone(),
      self.broker_membership_tx.subscribe(),
      Arc::clone(&self.blob_store),
      Arc::clone(&self.metadata_store),
      Arc::clone(&self.lease_store),
      topic_num_writers,
    )?;

    self
      .transport
      .register_write_engine(&node.node_id, Arc::clone(&write_engine))?;

    let metrics = BrokerMetrics::new();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    // TCP bindings run the real axum server; in-memory bindings only wait for shutdown.
    let serve_task = match endpoint.binding {
      BrokerEndpointBinding::Tcp(listener) => {
        let router = make_broker_router(write_engine, &metrics);
        tokio::spawn(async move {
          let result = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
              let _ = shutdown_rx.await;
            })
            .await;

          if let Err(error) = result {
            panic!("broker serve failed: {error}");
          }
        })
      },
      BrokerEndpointBinding::InMemory => tokio::spawn(async move {
        let _ = shutdown_rx.await;
      }),
    };

    Ok(BrokerHandle {
      node,
      shutdown_tx: Some(shutdown_tx),
      serve_task,
    })
  }

  pub async fn remove_broker_by_id(&mut self, node_id: &str) -> Result<()> {
    if self.brokers.len() <= 1 {
      return Err(anyhow!("at least one broker must remain active"));
    }

    let index = self
      .brokers
      .iter()
      .position(|broker| broker.node.node_id == node_id)
      .ok_or_else(|| anyhow!("broker node not found: {node_id}"))?;

    let mut removed = self.brokers.remove(index);
    removed.shutdown().await?;
    self.sync_membership();
    Ok(())
  }

  pub async fn restart_broker_by_id(&mut self, node_id: &str) -> Result<BrokerNode> {
    let index = self
      .brokers
      .iter()
      .position(|broker| broker.node.node_id == node_id)
      .ok_or_else(|| anyhow!("broker node not found: {node_id}"))?;

    let old_node = self.brokers[index].node.clone();
    let mut removed = self.brokers.remove(index);
    removed.shutdown().await?;

    let endpoint = self.transport.bind_endpoint(&old_node.node_id).await?;
    let restarted_node = endpoint.node.clone();
    let restarted_broker = self.spawn_broker(endpoint, self.topic_num_writers)?;
    self.brokers.push(restarted_broker);

    self.sync_membership();
    Ok(restarted_node)
  }

  pub async fn shutdown(&mut self) {
    for broker in &mut self.brokers {
      let _ = broker.shutdown().await;
    }
  }
}

fn build_write_engine(
  holder_id: String,
  membership_rx: watch::Receiver<BrokerMembership>,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  topic_num_writers: u32,
) -> Result<Arc<dyn WriteEngine>> {
  let mut topics = HashMap::new();
  for topic in [TOPIC, SECOND_TOPIC] {
    topics.insert(
      topic.to_string(),
      TopicInfo {
        name: topic.to_string(),
        partition_count: PARTITION_COUNT,
        num_writers: topic_num_writers,
      },
    );
  }

  let mut config = WriteConfig::with_defaults();
  config.flush_max_delay_ms = 10;
  config.flush_max_bytes = 1024;
  config.reservation_size = 64;
  config.writer_id = 0;

  let engine = WriteEngineImpl::new(
    config,
    topics,
    blob_store,
    metadata_store,
    lease_store,
    holder_id,
    Some(membership_rx),
  )?;

  Ok(Arc::new(engine))
}
