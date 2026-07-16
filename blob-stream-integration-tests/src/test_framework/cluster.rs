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
use crate::test_framework::{PARTITION_COUNT, SECOND_TOPIC, TOPIC};
use anyhow::{Result, anyhow};
use bd_server_stats::stats::Collector;
use blob_stream_blob_store::BlobStore;
use blob_stream_broker::grpc::make_broker_router;
use blob_stream_broker::metrics::BrokerMetrics;
use blob_stream_broker::write::{
  BrokerLeaseStatus,
  BrokerStateSnapshot,
  TopicInfo,
  WriteConfig,
  WriteEngine,
  WriteEngineImpl,
};
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, BrokerNode};
use blob_stream_metadata_store::{MetadataStore, ProducerPartitionLeaseStore};
use blob_stream_producer::{ProducerClientImpl, ProducerConfig, ProducerTopicConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

//
// BrokerHandle
//

struct BrokerHandle {
  node: BrokerNode,
  write_engine: Arc<dyn WriteEngine>,
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
  machine_ids: HashMap<String, u16>,
  event_log: TestEventLog,
  producer_discovery: DynamicBrokerDiscovery,
  broker_membership_tx: watch::Sender<BrokerMembership>,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  partition_count: u32,
  topic_num_writers: u32,
  broker_flush_max_delay: Duration,
  transport: Arc<dyn BrokerTransport>,
}

//
// ClusterHarnessBuilder
//

pub struct ClusterHarnessBuilder<'a> {
  resources: &'a IntegrationResources,
  broker_count: usize,
  blob_store: Option<Arc<dyn BlobStore>>,
  metadata_store: Option<Arc<dyn MetadataStore>>,
  partition_count: u32,
  topic_num_writers: u32,
  broker_flush_max_delay: Duration,
  start_with_all_nodes: bool,
  transport: Arc<dyn BrokerTransport>,
}

impl ClusterHarnessBuilder<'_> {
  pub fn blob_store(mut self, blob_store: Arc<dyn BlobStore>) -> Self {
    self.blob_store = Some(blob_store);
    self
  }

  pub fn metadata_store(mut self, metadata_store: Arc<dyn MetadataStore>) -> Self {
    self.metadata_store = Some(metadata_store);
    self
  }

  pub fn topic_num_writers(mut self, topic_num_writers: u32) -> Self {
    self.topic_num_writers = topic_num_writers;
    self
  }

  pub fn partition_count(mut self, partition_count: u32) -> Self {
    self.partition_count = partition_count;
    self
  }

  pub fn broker_flush_max_delay(mut self, broker_flush_max_delay: Duration) -> Self {
    self.broker_flush_max_delay = broker_flush_max_delay;
    self
  }

  pub fn start_with_all_nodes(mut self) -> Self {
    self.start_with_all_nodes = true;
    self
  }

  pub fn in_memory_transport(mut self) -> Self {
    self.transport = Arc::new(InMemoryTestTransport::new());
    self
  }

  pub async fn start(self) -> Result<ClusterHarness> {
    let blob_store = self
      .blob_store
      .unwrap_or_else(|| self.resources.blob_store());
    // The metadata store can be overridden by tests that need wrapped behavior.
    let metadata_store = self
      .metadata_store
      .unwrap_or_else(|| self.resources.metadata_store());

    ClusterHarness::start_from_builder(
      self.resources,
      self.broker_count,
      blob_store,
      metadata_store,
      self.partition_count,
      self.topic_num_writers,
      self.broker_flush_max_delay,
      self.start_with_all_nodes,
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
      blob_store: None,
      metadata_store: None,
      partition_count: PARTITION_COUNT,
      topic_num_writers: 1,
      broker_flush_max_delay: Duration::from_millis(10),
      start_with_all_nodes: false,
      transport: Arc::new(GrpcTcpTransport),
    }
  }

  async fn start_from_builder(
    resources: &IntegrationResources,
    broker_count: usize,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    partition_count: u32,
    topic_num_writers: u32,
    broker_flush_max_delay: Duration,
    start_with_all_nodes: bool,
    transport: Arc<dyn BrokerTransport>,
  ) -> Result<Self> {
    if partition_count == 0 {
      return Err(anyhow!("partition_count must be greater than zero"));
    }
    if topic_num_writers == 0 {
      return Err(anyhow!("topic_num_writers must be greater than zero"));
    }
    if broker_flush_max_delay.is_zero() {
      return Err(anyhow!("broker_flush_max_delay must be greater than zero"));
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
    let machine_ids = nodes
      .iter()
      .enumerate()
      .map(|(index, node)| {
        let machine_id = u16::try_from(index)
          .map_err(|_| anyhow!("broker count exceeds Sonyflake machine ID capacity"))?;
        Ok((node.node_id.clone(), machine_id))
      })
      .collect::<Result<HashMap<_, _>>>()?;

    let initial_nodes = if start_with_all_nodes {
      nodes
    } else {
      vec![
        nodes
          .first()
          .cloned()
          .ok_or_else(|| anyhow!("broker list cannot be empty"))?,
      ]
    };
    let producer_discovery = DynamicBrokerDiscovery::new(initial_nodes.clone());
    let (broker_membership_tx, _broker_membership_rx) =
      watch::channel(BrokerMembership::new(initial_nodes));

    let lease_store = resources.producer_lease_store();

    let mut harness = Self {
      brokers: Vec::with_capacity(broker_count),
      machine_ids,
      event_log,
      producer_discovery,
      broker_membership_tx,
      blob_store,
      metadata_store,
      lease_store,
      partition_count,
      topic_num_writers,
      broker_flush_max_delay,
      transport,
    };

    for endpoint in endpoints {
      let broker = harness.spawn_broker(endpoint, partition_count, topic_num_writers)?;
      harness.brokers.push(broker);
    }

    if !start_with_all_nodes {
      harness.wait_for_initial_lease_assignment().await?;
    }
    Ok(harness)
  }

  async fn wait_for_initial_lease_assignment(&self) -> Result<()> {
    let initial_broker = self
      .brokers
      .first()
      .ok_or_else(|| anyhow!("broker list cannot be empty"))?;
    let expected_partition_count = self.partition_count as usize * 2;
    let deadline = Instant::now() + Duration::from_secs(15);

    loop {
      let snapshots = self.broker_state_snapshots().await;
      let initial_broker_ready = snapshots
        .iter()
        .find(|snapshot| snapshot.holder_id == initial_broker.node.node_id)
        .is_some_and(|snapshot| {
          snapshot.ownership.len() == expected_partition_count
            && snapshot.ownership.iter().all(|ownership| {
              ownership.assignment_is_local
                && ownership.lease_status == BrokerLeaseStatus::LocalActive
            })
        });
      if initial_broker_ready {
        return Ok(());
      }
      if Instant::now() >= deadline {
        return Err(anyhow!(
          "initial broker lease assignment did not converge: {snapshots:#?}"
        ));
      }
      sleep(Duration::from_millis(10)).await;
    }
  }

  pub fn producer_discovery(&self) -> DynamicBrokerDiscovery {
    self.producer_discovery.clone()
  }

  pub fn event_log(&self) -> TestEventLog {
    self.event_log.clone()
  }

  pub async fn wait_for_event(
    &self,
    matcher: &TestEventMatcher,
    timeout: Duration,
  ) -> Result<TestEvent> {
    self.event_log.wait_for_event(matcher, timeout).await
  }

  pub fn network_fault_controller(&self) -> Option<NetworkFaultController> {
    self.transport.fault_controller()
  }

  pub async fn create_producer(
    &self,
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
  ) -> Result<ProducerClientImpl> {
    let discovery: Arc<dyn BrokerDiscovery> = Arc::new(self.producer_discovery());
    let metrics_scope = Collector::default().scope("blob_stream_producer_it");
    if let Some(transport) = self.transport.producer_transport() {
      ProducerClientImpl::new_with_transport(config, topics, discovery, transport, metrics_scope)
        .await
    } else {
      ProducerClientImpl::new(config, topics, discovery, metrics_scope).await
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

  pub async fn broker_state_snapshots(&self) -> Vec<BrokerStateSnapshot> {
    let mut snapshots = Vec::with_capacity(self.brokers.len());
    for broker in &self.brokers {
      snapshots.push(broker.write_engine.state_snapshot().await);
    }
    snapshots.sort_by(|left, right| left.holder_id.cmp(&right.holder_id));
    snapshots
  }

  fn sync_membership(&self) {
    let nodes = self.live_nodes();
    self.producer_discovery.update_nodes(nodes.clone());
    let _ = self.broker_membership_tx.send(BrokerMembership::new(nodes));
  }

  fn spawn_broker(
    &self,
    endpoint: BrokerEndpoint,
    partition_count: u32,
    topic_num_writers: u32,
  ) -> Result<BrokerHandle> {
    let node = endpoint.node;
    let machine_id = *self
      .machine_ids
      .get(&node.node_id)
      .ok_or_else(|| anyhow!("missing machine ID for broker {}", node.node_id))?;
    let write_engine = build_write_engine(
      node.node_id.clone(),
      machine_id,
      self.broker_membership_tx.subscribe(),
      Arc::clone(&self.blob_store),
      Arc::clone(&self.metadata_store),
      Arc::clone(&self.lease_store),
      partition_count,
      topic_num_writers,
      self.broker_flush_max_delay,
    )?;

    self
      .transport
      .register_write_engine(&node.node_id, Arc::clone(&write_engine))?;

    let metrics = BrokerMetrics::new();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    // TCP bindings run the real axum server; in-memory bindings only wait for shutdown.
    let serve_task = match endpoint.binding {
      BrokerEndpointBinding::Tcp(listener) => {
        let router = make_broker_router(Arc::clone(&write_engine), &metrics);
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
      write_engine,
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

    // Drop the retired write engine before starting its same-ID replacement. Its lease-assignment
    // loop performs asynchronous cleanup on drop; keeping it alive could release a lease that the
    // replacement has just acquired.
    drop(removed);

    let endpoint = self.transport.bind_endpoint(&old_node.node_id).await?;
    let restarted_node = endpoint.node.clone();
    let restarted_broker =
      self.spawn_broker(endpoint, self.partition_count, self.topic_num_writers)?;
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
  machine_id: u16,
  membership_rx: watch::Receiver<BrokerMembership>,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  partition_count: u32,
  topic_num_writers: u32,
  broker_flush_max_delay: Duration,
) -> Result<Arc<dyn WriteEngine>> {
  let mut topics = HashMap::new();
  for topic in [TOPIC, SECOND_TOPIC] {
    topics.insert(
      topic.to_string(),
      TopicInfo {
        name: topic.to_string(),
        partition_count,
        num_writers: topic_num_writers,
        retention_days: 7,
        max_metadata_publication_lag_ms: 30_000,
      },
    );
  }

  let mut config = WriteConfig::with_defaults();
  config.writer_id = 0;
  config.flush_max_delay_ms = i64::try_from(broker_flush_max_delay.as_millis())
    .map_err(|_| anyhow!("broker_flush_max_delay exceeds milliseconds as i64"))?;
  config.flush_max_bytes = 1024;
  config.reservation_size = 64;

  let engine = WriteEngineImpl::new_with_snowflake_machine_id(
    config,
    topics,
    blob_store,
    metadata_store,
    lease_store,
    holder_id,
    machine_id,
    Some(membership_rx),
    &Collector::default().scope("blob_stream_broker_it"),
  )?;

  Ok(Arc::new(engine))
}
