use super::discovery::DynamicBrokerDiscovery;
use super::event_log::{TestEvent, TestEventLog, TestEventMatcher};
use super::lifecycle::TestLifecycleHooks;
use super::resources::IntegrationResources;
use super::store_faults::{
  FaultInjectedBlobStore,
  FaultInjectedConsumerGroupLeaseStore,
  FaultInjectedConsumerGroupMembershipStore,
  FaultInjectedMetadataStore,
  FaultInjectedProducerPartitionLeaseStore,
  StoreFaultController,
};
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
use bd_shutdown::{ComponentShutdownTrigger, ComponentShutdownTriggerHandle};
use bd_time::{SystemTimeProvider, TimeProvider};
use blob_stream_blob_store::{BlobStore, InMemoryBlobStore};
use blob_stream_broker::grpc::make_broker_router;
use blob_stream_broker::metrics::BrokerMetrics;
use blob_stream_broker::write::{
  BrokerLeaseStatus,
  BrokerStateSnapshot,
  TopicInfo,
  WriteConfig,
  WriteEngine,
  WriteEngineBuilder,
};
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, BrokerNode};
use blob_stream_consumer::iterator::{ConsumerIteratorBuilder, ConsumerIteratorImpl};
use blob_stream_consumer::{
  ConsumerRuntimeConfig,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG,
  MembershipCoordinationSource,
};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseStore,
  ConsumerGroupMembershipStore,
  InMemoryConsumerGroupLeaseStore,
  InMemoryConsumerGroupMembershipStore,
  InMemoryMetadataStore,
  InMemoryProducerPartitionLeaseStore,
  MetadataStore,
  ProducerPartitionLeaseStore,
};
use blob_stream_producer::{
  BrokerTransport as ProducerBrokerTransport,
  GrpcBrokerTransport,
  ProducerClientBuilder,
  ProducerClientImpl,
  ProducerConfig,
  ProducerTopicConfig,
};
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
  listener_shutdown_tx: Option<oneshot::Sender<()>>,
  broker_shutdown_trigger: Option<ComponentShutdownTrigger>,
  serve_task: JoinHandle<()>,
}

impl BrokerHandle {
  async fn shutdown(&mut self) -> Result<()> {
    if let Some(tx) = self.listener_shutdown_tx.take() {
      let _ = tx.send(());
    }

    let join_result = timeout(Duration::from_secs(5), &mut self.serve_task)
      .await
      .map_err(|_| anyhow!("broker graceful shutdown timed out"))?;
    join_result.map_err(|error| anyhow!("broker task join failed: {error}"))?;

    if let Some(shutdown_trigger) = self.broker_shutdown_trigger.take() {
      shutdown_trigger.shutdown().await;
    }

    Ok(())
  }
}

//
// ClusterHarness
//

pub struct ClusterHarness {
  brokers: Vec<BrokerHandle>,
  machine_ids: HashMap<protobuf::Chars, u16>,
  event_log: TestEventLog,
  producer_discovery: DynamicBrokerDiscovery,
  broker_membership_tx: watch::Sender<BrokerMembership>,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  consumer_lease_store: Arc<dyn ConsumerGroupLeaseStore>,
  consumer_membership_store: Arc<dyn ConsumerGroupMembershipStore>,
  partition_count: u32,
  topic_num_writers: u32,
  broker_flush_max_delay: Duration,
  fenced_metadata_writes: bool,
  transport: Arc<dyn BrokerTransport>,
  lifecycle_hooks: TestLifecycleHooks,
  broker_time_provider: Arc<dyn TimeProvider>,
  consumer_time_provider: Arc<dyn TimeProvider>,
  store_fault_controller: Option<StoreFaultController>,
}

//
// InMemoryClusterHarnessBuilder
//

/// Builds a cluster with no external storage dependencies.
pub struct InMemoryClusterHarnessBuilder {
  broker_count: usize,
  metadata_store: Option<Arc<dyn MetadataStore>>,
  partition_count: u32,
  topic_num_writers: u32,
  broker_flush_max_delay: Duration,
  start_with_all_nodes: bool,
  broker_time_provider: Arc<dyn TimeProvider>,
  consumer_time_provider: Arc<dyn TimeProvider>,
}

impl InMemoryClusterHarnessBuilder {
  #[must_use]
  pub fn metadata_store(mut self, metadata_store: Arc<dyn MetadataStore>) -> Self {
    self.metadata_store = Some(metadata_store);
    self
  }

  #[must_use]
  pub fn partition_count(mut self, partition_count: u32) -> Self {
    self.partition_count = partition_count;
    self
  }

  #[must_use]
  pub fn topic_num_writers(mut self, topic_num_writers: u32) -> Self {
    self.topic_num_writers = topic_num_writers;
    self
  }

  #[must_use]
  pub fn broker_flush_max_delay(mut self, broker_flush_max_delay: Duration) -> Self {
    self.broker_flush_max_delay = broker_flush_max_delay;
    self
  }

  #[must_use]
  pub fn start_with_all_nodes(mut self) -> Self {
    self.start_with_all_nodes = true;
    self
  }

  #[must_use]
  pub fn broker_time_provider(mut self, broker_time_provider: Arc<dyn TimeProvider>) -> Self {
    self.broker_time_provider = broker_time_provider;
    self
  }

  #[must_use]
  pub fn consumer_time_provider(mut self, consumer_time_provider: Arc<dyn TimeProvider>) -> Self {
    self.consumer_time_provider = consumer_time_provider;
    self
  }

  pub async fn start(self) -> Result<ClusterHarness> {
    let store_fault_controller = StoreFaultController::default();
    let blob_store: Arc<dyn BlobStore> = Arc::new(FaultInjectedBlobStore::new(
      Arc::new(InMemoryBlobStore::new()),
      store_fault_controller.clone(),
    ));
    let metadata_store: Arc<dyn MetadataStore> = Arc::new(FaultInjectedMetadataStore::new(
      self
        .metadata_store
        .unwrap_or_else(|| Arc::new(InMemoryMetadataStore::new())),
      store_fault_controller.clone(),
    ));
    let lease_store: Arc<dyn ProducerPartitionLeaseStore> =
      Arc::new(FaultInjectedProducerPartitionLeaseStore::new(
        Arc::new(InMemoryProducerPartitionLeaseStore::new()),
        store_fault_controller.clone(),
      ));
    let consumer_lease_store: Arc<dyn ConsumerGroupLeaseStore> =
      Arc::new(FaultInjectedConsumerGroupLeaseStore::new(
        Arc::new(InMemoryConsumerGroupLeaseStore::new()),
        store_fault_controller.clone(),
      ));
    let consumer_membership_store: Arc<dyn ConsumerGroupMembershipStore> =
      Arc::new(FaultInjectedConsumerGroupMembershipStore::new(
        Arc::new(InMemoryConsumerGroupMembershipStore::new()),
        store_fault_controller.clone(),
      ));

    ClusterHarness::start_from_builder(
      Some(store_fault_controller),
      self.broker_count,
      blob_store,
      metadata_store,
      lease_store,
      consumer_lease_store,
      consumer_membership_store,
      self.partition_count,
      self.topic_num_writers,
      self.broker_flush_max_delay,
      false,
      self.start_with_all_nodes,
      Arc::new(InMemoryTestTransport::new()),
      self.broker_time_provider,
      self.consumer_time_provider,
    )
    .await
  }
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
  fenced_metadata_writes: bool,
  start_with_all_nodes: bool,
  transport: Arc<dyn BrokerTransport>,
  broker_time_provider: Arc<dyn TimeProvider>,
  consumer_time_provider: Arc<dyn TimeProvider>,
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

  #[must_use]
  pub fn fenced_metadata_writes(mut self) -> Self {
    self.fenced_metadata_writes = true;
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

  #[must_use]
  pub fn broker_time_provider(mut self, broker_time_provider: Arc<dyn TimeProvider>) -> Self {
    self.broker_time_provider = broker_time_provider;
    self
  }

  #[must_use]
  pub fn consumer_time_provider(mut self, consumer_time_provider: Arc<dyn TimeProvider>) -> Self {
    self.consumer_time_provider = consumer_time_provider;
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
    let lease_store = self.resources.producer_lease_store();
    let consumer_lease_store = self.resources.consumer_lease_store();
    let consumer_membership_store = self.resources.consumer_membership_store();

    ClusterHarness::start_from_builder(
      Some(self.resources.store_fault_controller()),
      self.broker_count,
      blob_store,
      metadata_store,
      lease_store,
      consumer_lease_store,
      consumer_membership_store,
      self.partition_count,
      self.topic_num_writers,
      self.broker_flush_max_delay,
      self.fenced_metadata_writes,
      self.start_with_all_nodes,
      self.transport,
      self.broker_time_provider,
      self.consumer_time_provider,
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
      fenced_metadata_writes: false,
      start_with_all_nodes: false,
      transport: Arc::new(GrpcTcpTransport),
      broker_time_provider: Arc::new(SystemTimeProvider),
      consumer_time_provider: Arc::new(SystemTimeProvider),
    }
  }

  #[must_use]
  pub fn in_memory(broker_count: usize) -> InMemoryClusterHarnessBuilder {
    InMemoryClusterHarnessBuilder {
      broker_count,
      metadata_store: None,
      partition_count: PARTITION_COUNT,
      topic_num_writers: 1,
      broker_flush_max_delay: Duration::from_millis(10),
      start_with_all_nodes: false,
      broker_time_provider: Arc::new(SystemTimeProvider),
      consumer_time_provider: Arc::new(SystemTimeProvider),
    }
  }

  async fn start_from_builder(
    store_fault_controller: Option<StoreFaultController>,
    broker_count: usize,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore>,
    lease_store: Arc<dyn ProducerPartitionLeaseStore>,
    consumer_lease_store: Arc<dyn ConsumerGroupLeaseStore>,
    consumer_membership_store: Arc<dyn ConsumerGroupMembershipStore>,
    partition_count: u32,
    topic_num_writers: u32,
    broker_flush_max_delay: Duration,
    fenced_metadata_writes: bool,
    start_with_all_nodes: bool,
    transport: Arc<dyn BrokerTransport>,
    broker_time_provider: Arc<dyn TimeProvider>,
    consumer_time_provider: Arc<dyn TimeProvider>,
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
    if let Some(store_fault_controller) = &store_fault_controller {
      store_fault_controller
        .attach_event_log(event_log.clone())
        .await;
    }

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

    let lifecycle_hooks = TestLifecycleHooks::default();

    let mut harness = Self {
      brokers: Vec::with_capacity(broker_count),
      machine_ids,
      event_log,
      producer_discovery,
      broker_membership_tx,
      blob_store,
      metadata_store,
      lease_store,
      consumer_lease_store,
      consumer_membership_store,
      partition_count,
      topic_num_writers,
      broker_flush_max_delay,
      fenced_metadata_writes,
      transport,
      lifecycle_hooks,
      broker_time_provider,
      consumer_time_provider,
      store_fault_controller,
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
        .find(|snapshot| snapshot.holder_id == initial_broker.node.node_id.as_str())
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

  pub fn lifecycle_hooks(&self) -> TestLifecycleHooks {
    self.lifecycle_hooks.clone()
  }

  pub async fn create_consumer(
    &self,
    runtime: &ConsumerRuntimeConfig,
  ) -> Result<ConsumerIteratorImpl> {
    let group = runtime
      .group
      .as_ref()
      .ok_or_else(|| anyhow!("consumer runtime config requires a group config"))?;
    let coordination_source = Arc::new(
      MembershipCoordinationSource::new(
        group.topic.to_string(),
        group.group_id.to_string(),
        group.member_id.to_string(),
        (0 .. self.partition_count).collect(),
        Arc::clone(&self.consumer_membership_store),
      )
      .time_provider(Arc::clone(&self.consumer_time_provider)),
    );
    ConsumerIteratorBuilder::new(
      runtime,
      Arc::clone(&self.blob_store),
      Arc::clone(&self.metadata_store),
      Arc::clone(&self.consumer_lease_store),
      Arc::clone(&self.consumer_membership_store),
      coordination_source,
      Collector::default().scope("blob_stream_consumer_it"),
      time::Duration::days(1),
      DEFAULT_MAX_METADATA_PUBLICATION_LAG,
      None,
    )
    .lifecycle_hooks(Arc::new(self.lifecycle_hooks.clone()))
    .time_provider(Arc::clone(&self.consumer_time_provider))
    .build()
    .await
  }

  pub fn consumer_lease_store(&self) -> Arc<dyn ConsumerGroupLeaseStore> {
    Arc::clone(&self.consumer_lease_store)
  }

  pub fn consumer_membership_store(&self) -> Arc<dyn ConsumerGroupMembershipStore> {
    Arc::clone(&self.consumer_membership_store)
  }

  pub fn store_fault_controller(&self) -> Option<StoreFaultController> {
    self.store_fault_controller.clone()
  }

  pub async fn wait_for_event(
    &self,
    matcher: &TestEventMatcher,
    timeout: Duration,
  ) -> Result<TestEvent> {
    self.event_log.wait_for_event(matcher, timeout).await
  }

  pub async fn wait_for_event_after(
    &self,
    matcher: &TestEventMatcher,
    after_sequence: Option<u64>,
    timeout: Duration,
  ) -> Result<TestEvent> {
    self
      .event_log
      .wait_for_event_after(matcher, after_sequence, timeout)
      .await
  }

  pub fn network_fault_controller(&self) -> Option<NetworkFaultController> {
    self.transport.fault_controller()
  }

  pub async fn create_producer(
    &self,
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
  ) -> Result<ProducerClientImpl> {
    self.producer_builder(config, topics).build().await
  }

  pub fn producer_builder(
    &self,
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
  ) -> ProducerClientBuilder {
    let discovery: Arc<dyn BrokerDiscovery> = Arc::new(self.producer_discovery());
    let metrics_scope = Collector::default().scope("blob_stream_producer_it");
    let transport: Arc<dyn ProducerBrokerTransport> = self
      .transport
      .producer_transport()
      .unwrap_or_else(|| Arc::new(GrpcBrokerTransport::new(config.clone())));
    ProducerClientBuilder::new(config, topics, discovery, transport, metrics_scope)
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

  pub fn write_engine_by_id(&self, node_id: &str) -> Result<Arc<dyn WriteEngine>> {
    self
      .brokers
      .iter()
      .find(|broker| broker.node.node_id.as_str() == node_id)
      .map(|broker| Arc::clone(&broker.write_engine))
      .ok_or_else(|| anyhow!("broker node not found: {node_id}"))
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
    let broker_shutdown_trigger = ComponentShutdownTrigger::default();
    let write_engine = build_write_engine(
      node.node_id.to_string(),
      machine_id,
      self.broker_membership_tx.subscribe(),
      Arc::clone(&self.blob_store),
      Arc::clone(&self.metadata_store),
      Arc::clone(&self.lease_store),
      partition_count,
      topic_num_writers,
      self.broker_flush_max_delay,
      self.fenced_metadata_writes,
      broker_shutdown_trigger.make_handle(),
      Arc::new(self.lifecycle_hooks.clone()),
      Arc::clone(&self.broker_time_provider),
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
      listener_shutdown_tx: Some(shutdown_tx),
      broker_shutdown_trigger: Some(broker_shutdown_trigger),
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
      .position(|broker| broker.node.node_id.as_str() == node_id)
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
      .position(|broker| broker.node.node_id.as_str() == node_id)
      .ok_or_else(|| anyhow!("broker node not found: {node_id}"))?;

    let old_node = self.brokers[index].node.clone();
    let mut removed = self.brokers.remove(index);
    removed.shutdown().await?;

    // The explicit broker component shutdown completed before the replacement starts, so the
    // retired engine cannot release a lease after its replacement has acquired it.
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
  fenced_metadata_writes: bool,
  shutdown_trigger_handle: ComponentShutdownTriggerHandle,
  lifecycle_hooks: Arc<dyn blob_stream_broker::write::BrokerLifecycleHooks>,
  time_provider: Arc<dyn TimeProvider>,
) -> Result<Arc<dyn WriteEngine>> {
  let mut topics = HashMap::new();
  for topic in [TOPIC, SECOND_TOPIC] {
    topics.insert(
      topic.into(),
      TopicInfo {
        name: topic.into(),
        partition_count,
        num_writers: topic_num_writers,
        retention: time::Duration::days(7),
        max_metadata_publication_lag: time::Duration::milliseconds(30_000),
      },
    );
  }

  let mut config = WriteConfig::with_defaults();
  config.writer_id = 0;
  config.flush_max_delay = time::Duration::try_from(broker_flush_max_delay)
    .map_err(|_| anyhow!("broker_flush_max_delay exceeds time::Duration bounds"))?;
  config.flush_max_bytes = 1024;
  config.reservation_size = 64;
  config.fenced_metadata_writes = fenced_metadata_writes;
  let metrics_scope = Collector::default().scope("blob_stream_broker_it");
  let engine = WriteEngineBuilder::new(
    config,
    topics,
    blob_store,
    metadata_store,
    lease_store,
    holder_id,
    shutdown_trigger_handle,
    &metrics_scope,
  )
  .machine_id(machine_id)
  .membership_rx(membership_rx)
  .time_provider(time_provider)
  .lifecycle_hooks(lifecycle_hooks)
  .build()?;

  Ok(Arc::new(engine))
}
