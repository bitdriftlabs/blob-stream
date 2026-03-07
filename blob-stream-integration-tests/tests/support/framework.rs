use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::types::{
  AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use aws_sdk_s3::Client as S3Client;
use blob_stream::grpc::make_broker_router;
use blob_stream::metrics::BrokerMetrics;
use blob_stream::write::{TopicInfo, WriteConfig, WriteEngine, WriteEngineImpl};
use blob_stream_blob_store::{BlobStore, InMemoryBlobStore};
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, BrokerNode};
use blob_stream_consumer::{
  ConsumerCoordinationSource, ConsumerGroupConfig, ConsumerReadConfig, ConsumerReader,
  ConsumerReaderImpl, ConsumerRuntimeConfig, CoordinationSnapshot,
};
use blob_stream_metadata_store::{
  ConsumerGroupLeaseStore, DynamoConsumerGroupLeaseStore, DynamoMetadataStore,
  DynamoProducerPartitionLeaseStore, MetadataStore, ProducerPartitionLeaseStore,
};
use blob_stream_producer::{
  ProducerClient, ProducerClientImpl, ProducerCompression, ProducerConfig, ProducerRecord,
  ProducerTopicConfig,
};
use blob_stream_types::VirtualPartitionId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use uuid::Uuid;

const DYNAMO_ENDPOINT: &str = "http://localhost:8000";
const S3_ENDPOINT: &str = "http://localhost:4566";
const AWS_REGION: &str = "us-east-1";

pub const TOPIC: &str = "telemetry";
pub const SECOND_TOPIC: &str = "telemetry-secondary";
pub const PARTITION_COUNT: u32 = 16;
pub const WINDOW_SIZE_SECONDS: i64 = 300;

#[cfg(test)]
#[ctor::ctor]
fn global_init() {
  bd_test_helpers::test_global_init();
}

pub struct IntegrationResources {
  dynamo: DynamoClient,
  s3: S3Client,
  blob_store: Arc<dyn BlobStore>,
  bucket: String,
  metadata_table: String,
  producer_lease_table: String,
  consumer_lease_table: String,
}

impl IntegrationResources {
  pub async fn create() -> Result<Self> {
    configure_aws_env();

    let dynamo_config = aws_config::defaults(BehaviorVersion::latest())
      .endpoint_url(DYNAMO_ENDPOINT)
      .load()
      .await;
    let dynamo = DynamoClient::new(&dynamo_config);

    let shared_s3 = aws_config::defaults(BehaviorVersion::latest())
      .endpoint_url(S3_ENDPOINT)
      .load()
      .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&shared_s3)
      .force_path_style(true)
      .build();
    let s3 = S3Client::from_conf(s3_config);

    wait_for_dependencies(&dynamo, &s3).await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let metadata_table = format!("blob_segments_it_{suffix}");
    let producer_lease_table = format!("producer_leases_it_{suffix}");
    let consumer_lease_table = format!("consumer_leases_it_{suffix}");
    let bucket = format!("blob-stream-it-{}", Uuid::new_v4().simple());

    create_table_pk_sk(&dynamo, &metadata_table).await?;
    create_table_pk_only(&dynamo, &producer_lease_table).await?;
    create_table_pk_sk(&dynamo, &consumer_lease_table).await?;
    create_bucket(&s3, &bucket).await?;
    verify_s3_roundtrip(&s3, &bucket).await?;

    let blob_store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());

    Ok(Self {
      dynamo,
      s3,
      blob_store,
      bucket,
      metadata_table,
      producer_lease_table,
      consumer_lease_table,
    })
  }

  pub fn blob_store(&self) -> Arc<dyn BlobStore> {
    Arc::clone(&self.blob_store)
  }

  pub fn metadata_store(&self) -> Arc<dyn MetadataStore> {
    Arc::new(DynamoMetadataStore::new(
      self.dynamo.clone(),
      self.metadata_table.clone(),
    ))
  }

  pub fn producer_lease_store(&self) -> Arc<dyn ProducerPartitionLeaseStore> {
    Arc::new(DynamoProducerPartitionLeaseStore::new(
      self.dynamo.clone(),
      self.producer_lease_table.clone(),
    ))
  }

  pub fn consumer_lease_store(&self) -> Arc<dyn ConsumerGroupLeaseStore> {
    Arc::new(DynamoConsumerGroupLeaseStore::new(
      self.dynamo.clone(),
      self.consumer_lease_table.clone(),
    ))
  }

  pub async fn cleanup(&self) {
    let _ = self
      .dynamo
      .delete_table()
      .table_name(&self.metadata_table)
      .send()
      .await;
    let _ = self
      .dynamo
      .delete_table()
      .table_name(&self.producer_lease_table)
      .send()
      .await;
    let _ = self
      .dynamo
      .delete_table()
      .table_name(&self.consumer_lease_table)
      .send()
      .await;

    if let Ok(objects) = self.s3.list_objects_v2().bucket(&self.bucket).send().await
      && let Some(contents) = objects.contents
    {
      for object in contents {
        if let Some(key) = object.key {
          let _ = self
            .s3
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        }
      }
    }

    let _ = self.s3.delete_bucket().bucket(&self.bucket).send().await;
  }
}

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

    timeout(Duration::from_secs(5), &mut self.serve_task)
      .await
      .context("broker graceful shutdown timed out")?
      .context("broker task join failed")?;

    Ok(())
  }
}

pub struct ClusterHarness {
  brokers: Vec<BrokerHandle>,
  producer_discovery: DynamicBrokerDiscovery,
  broker_membership_tx: watch::Sender<BrokerMembership>,
  blob_store: Arc<dyn BlobStore>,
  metadata_store: Arc<dyn MetadataStore>,
  lease_store: Arc<dyn ProducerPartitionLeaseStore>,
  topic_num_writers: u32,
}

pub struct ClusterHarnessBuilder<'a> {
  resources: &'a IntegrationResources,
  broker_count: usize,
  metadata_store: Option<Arc<dyn MetadataStore>>,
  topic_num_writers: u32,
}

impl<'a> ClusterHarnessBuilder<'a> {
  pub fn metadata_store(mut self, metadata_store: Arc<dyn MetadataStore>) -> Self {
    self.metadata_store = Some(metadata_store);
    self
  }

  pub fn topic_num_writers(mut self, topic_num_writers: u32) -> Self {
    self.topic_num_writers = topic_num_writers;
    self
  }

  pub async fn start(self) -> Result<ClusterHarness> {
    let metadata_store = self
      .metadata_store
      .unwrap_or_else(|| self.resources.metadata_store());

    ClusterHarness::start_from_builder(
      self.resources,
      self.broker_count,
      metadata_store,
      self.topic_num_writers,
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
    }
  }

  async fn start_from_builder(
    resources: &IntegrationResources,
    broker_count: usize,
    metadata_store: Arc<dyn MetadataStore>,
    topic_num_writers: u32,
  ) -> Result<Self> {
    if topic_num_writers == 0 {
      return Err(anyhow!("topic_num_writers must be greater than zero"));
    }

    let mut listeners = Vec::with_capacity(broker_count);
    let mut nodes = Vec::with_capacity(broker_count);

    for broker_index in 0..broker_count {
      let holder_id = format!("broker-{broker_index}");
      let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
      let addr = listener.local_addr()?;
      let node = BrokerNode {
        node_id: holder_id,
        address: addr.to_string(),
      };
      listeners.push(listener);
      nodes.push(node);
    }

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
      producer_discovery,
      broker_membership_tx,
      blob_store,
      metadata_store,
      lease_store,
      topic_num_writers,
    };

    for (listener, node) in listeners.into_iter().zip(nodes) {
      let broker = harness.spawn_broker(listener, node, topic_num_writers)?;
      harness.brokers.push(broker);
    }

    Ok(harness)
  }

  pub fn producer_discovery(&self) -> DynamicBrokerDiscovery {
    self.producer_discovery.clone()
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

  fn spawn_broker(
    &self,
    listener: tokio::net::TcpListener,
    node: BrokerNode,
    topic_num_writers: u32,
  ) -> Result<BrokerHandle> {
    let write_engine = build_write_engine(
      node.node_id.clone(),
      self.broker_membership_tx.subscribe(),
      Arc::clone(&self.blob_store),
      Arc::clone(&self.metadata_store),
      Arc::clone(&self.lease_store),
      topic_num_writers,
    )?;

    let metrics = BrokerMetrics::new();
    let router = make_broker_router(write_engine, &metrics);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serve_task = tokio::spawn(async move {
      let result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
          let _ = shutdown_rx.await;
        })
        .await;

      if let Err(error) = result {
        panic!("broker serve failed: {error}");
      }
    });

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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let restarted_node = BrokerNode {
      node_id: old_node.node_id,
      address: listener.local_addr()?.to_string(),
    };
    let restarted_broker =
      self.spawn_broker(listener, restarted_node.clone(), self.topic_num_writers)?;
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

pub fn producer_config(max_retries: u32) -> ProducerConfig {
  producer_config_with_writer_id(max_retries, 0)
}

pub fn producer_config_with_writer_id(max_retries: u32, writer_id: u32) -> ProducerConfig {
  let mut config = ProducerConfig::new();
  config.writer_id = Some(writer_id);
  config.max_batch_records = Some(1);
  config.max_batch_bytes = Some(1_024);
  config.flush_max_delay_ms = Some(5);
  config.max_retries = Some(max_retries);
  config.retry_base_delay_ms = Some(10);
  config.retry_max_delay_ms = Some(50);
  config.connect_timeout_ms = Some(100);
  config.request_timeout_ms = Some(2_000);
  config.max_request_concurrency = Some(32);
  config.compression = Some(ProducerCompression::PRODUCER_COMPRESSION_NONE.into());
  config
}

pub fn producer_topic() -> ProducerTopicConfig {
  producer_topic_named_with_writers(TOPIC, 1)
}

pub fn producer_topic_named(topic: &str) -> ProducerTopicConfig {
  producer_topic_named_with_writers(topic, 1)
}

pub fn producer_topic_named_with_writers(topic: &str, num_writers: u32) -> ProducerTopicConfig {
  ProducerTopicConfig {
    name: topic.to_string().into(),
    partition_count: PARTITION_COUNT,
    num_writers,
    retention_days: 0,
    ..Default::default()
  }
}

pub fn consumer_runtime_config(member_id: &str) -> ConsumerRuntimeConfig {
  let mut read = ConsumerReadConfig::new();
  read.topic = TOPIC.to_string().into();
  read.window_size_seconds = Some(WINDOW_SIZE_SECONDS);
  read.lookback_windows = Some(10);

  let mut group = ConsumerGroupConfig::new();
  group.topic = TOPIC.to_string().into();
  group.group_id = "integration-group".to_string().into();
  group.member_id = member_id.to_string().into();
  group.lease_duration_ms = Some(2_000);
  group.heartbeat_interval_ms = Some(200);
  group.rebalance_interval_ms = Some(200);

  let mut runtime = ConsumerRuntimeConfig::new();
  runtime.read = Some(read).into();
  runtime.group = Some(group).into();
  runtime
}

pub async fn produce_message(
  producer: &ProducerClientImpl,
  key: Vec<u8>,
  id: &str,
) -> Result<blob_stream_producer::ProducerAck> {
  produce_message_for_topic(producer, TOPIC, key, id).await
}

pub async fn produce_message_for_topic(
  producer: &ProducerClientImpl,
  topic: &str,
  key: Vec<u8>,
  id: &str,
) -> Result<blob_stream_producer::ProducerAck> {
  let ack = producer
    .produce(ProducerRecord::new(
      topic,
      key,
      id.as_bytes().to_vec(),
      now_unix_millis(),
    ))
    .await?;
  Ok(ack)
}

pub async fn drain_reader_until(
  reader: &mut ConsumerReaderImpl,
  consumed_ids: &mut HashSet<String>,
  expected: usize,
  deadline: Instant,
) -> Result<()> {
  while consumed_ids.len() < expected {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded: expected={expected}, consumed={}",
        consumed_ids.len()
      ));
    }

    let mut progressed = false;
    let batches = reader.read_available(now_unix_seconds()).await?;
    for batch in batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload).context("consumer payload was not utf-8")?;
        consumed_ids.insert(id);
      }
      progressed = true;
    }

    if !progressed {
      sleep(Duration::from_millis(50)).await;
    }
  }

  Ok(())
}

fn configure_aws_env() {
  unsafe {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", AWS_REGION);
  }
}

async fn wait_for_dependencies(dynamo: &DynamoClient, s3: &S3Client) -> Result<()> {
  let started = Instant::now();
  let timeout_at = Duration::from_secs(30);

  loop {
    let dynamo_ready = dynamo.list_tables().send().await.is_ok();
    let s3_ready = s3.list_buckets().send().await.is_ok();

    if dynamo_ready && s3_ready {
      return Ok(());
    }

    if started.elapsed() >= timeout_at {
      return Err(anyhow!(
        "compose dependencies not ready: dynamo_ready={dynamo_ready}, s3_ready={s3_ready}"
      ));
    }

    sleep(Duration::from_millis(300)).await;
  }
}

async fn create_table_pk_sk(client: &DynamoClient, table_name: &str) -> Result<()> {
  client
    .create_table()
    .table_name(table_name)
    .attribute_definitions(
      AttributeDefinition::builder()
        .attribute_name("pk")
        .attribute_type(ScalarAttributeType::S)
        .build()?,
    )
    .attribute_definitions(
      AttributeDefinition::builder()
        .attribute_name("sk")
        .attribute_type(ScalarAttributeType::S)
        .build()?,
    )
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("pk")
        .key_type(KeyType::Hash)
        .build()?,
    )
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("sk")
        .key_type(KeyType::Range)
        .build()?,
    )
    .billing_mode(BillingMode::PayPerRequest)
    .send()
    .await?;

  wait_for_table_active(client, table_name).await
}

async fn create_table_pk_only(client: &DynamoClient, table_name: &str) -> Result<()> {
  client
    .create_table()
    .table_name(table_name)
    .attribute_definitions(
      AttributeDefinition::builder()
        .attribute_name("pk")
        .attribute_type(ScalarAttributeType::S)
        .build()?,
    )
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("pk")
        .key_type(KeyType::Hash)
        .build()?,
    )
    .billing_mode(BillingMode::PayPerRequest)
    .send()
    .await?;

  wait_for_table_active(client, table_name).await
}

async fn wait_for_table_active(client: &DynamoClient, table_name: &str) -> Result<()> {
  for _ in 0..40 {
    let response = client.describe_table().table_name(table_name).send().await;
    if let Ok(response) = response
      && let Some(status) = response.table().and_then(|table| table.table_status())
      && status.as_str() == "ACTIVE"
    {
      return Ok(());
    }

    sleep(Duration::from_millis(150)).await;
  }

  Err(anyhow!("table {table_name} did not become active"))
}

async fn create_bucket(client: &S3Client, bucket: &str) -> Result<()> {
  client.create_bucket().bucket(bucket).send().await?;
  Ok(())
}

async fn verify_s3_roundtrip(client: &S3Client, bucket: &str) -> Result<()> {
  let key = format!("integration-healthcheck-{}", Uuid::new_v4().simple());
  let payload = b"ok".to_vec();

  client
    .put_object()
    .bucket(bucket)
    .key(&key)
    .body(payload.clone().into())
    .send()
    .await?;

  let response = client.get_object().bucket(bucket).key(&key).send().await?;
  let body = response.body.collect().await?;
  if body.into_bytes().to_vec() != payload {
    return Err(anyhow!("s3 healthcheck payload mismatch"));
  }

  client
    .delete_object()
    .bucket(bucket)
    .key(&key)
    .send()
    .await?;

  Ok(())
}

fn now_unix_millis() -> i64 {
  i64::try_from(
    std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .expect("clock is before unix epoch")
      .as_millis(),
  )
  .expect("unix millis exceeds i64")
}

pub fn now_unix_seconds() -> i64 {
  now_unix_millis() / 1_000
}
