#[cfg(test)]
#[path = "./producer_test.rs"]
mod tests;

use crate::config::{
  ProducerConfig,
  ProducerRuntimeConfig,
  ProducerTopicConfig,
  compression_as_grpc,
  into_discovery,
  producer_compression,
  producer_connect_timeout_ms,
  producer_flush_max_delay_ms,
  producer_max_batch_bytes,
  producer_max_batch_records,
  producer_max_request_concurrency,
  producer_max_retries,
  producer_request_timeout_ms,
  producer_retry_base_delay_ms,
  producer_retry_max_delay_ms,
  producer_writer_id,
  validate_producer_config,
  validate_runtime_config,
  validate_topic_config,
};
use anyhow::{Result, anyhow, bail, ensure};
use async_trait::async_trait;
use bd_grpc::client::Client as GrpcClient;
use bd_grpc::service::ServiceMethod;
use bd_log::warn_every;
use bd_server_stats::stats::Scope;
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, owner_for_partition};
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchRequest,
  ProduceBatchResponse,
  ProduceStatus,
  Record,
};
use blob_stream_types::{VirtualPartitionId, virtual_partition_for_key};
use log::{debug, trace};
use prometheus::{Histogram, IntCounter};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use time::Duration as TimeDuration;
use time::ext::NumericalDuration;
use tokio::sync::{Mutex, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval};

// TODO(mattklein123): Consider adding disk buffering of segments.

//
// ProducerMetrics
//

#[derive(Clone)]
struct ProducerMetrics {
  records_enqueued: IntCounter,
  batches_sent: IntCounter,
  records_sent: IntCounter,
  retries: IntCounter,
  failures: IntCounter,
  no_brokers: IntCounter,
  send_latency_seconds: Histogram,
}

impl ProducerMetrics {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("producer");
    Self {
      records_enqueued: scope.counter("records_enqueued"),
      batches_sent: scope.counter("batches_sent"),
      records_sent: scope.counter("records_sent"),
      retries: scope.counter("retries"),
      failures: scope.counter("failures"),
      no_brokers: scope.counter("no_brokers"),
      send_latency_seconds: scope.histogram("send_latency_seconds"),
    }
  }
}

//
// ProducerRecord
//

#[derive(Clone, Debug)]
pub struct ProducerRecord {
  pub topic: String,
  pub record_key: Vec<u8>,
  pub payload: Vec<u8>,
  pub event_ts_ms: i64,
}

impl ProducerRecord {
  #[must_use]
  pub fn new(
    topic: impl Into<String>,
    record_key: Vec<u8>,
    payload: Vec<u8>,
    event_ts_ms: i64,
  ) -> Self {
    Self {
      topic: topic.into(),
      record_key,
      payload,
      event_ts_ms,
    }
  }
}

//
// ProducerAck
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerAck {
  pub topic: String,
  pub virtual_partition_id: VirtualPartitionId,
  pub attempts: u32,
}

//
// ProducerError
//

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProducerError {
  #[error("unknown topic: {0}")]
  UnknownTopic(String),
  #[error("no brokers available")]
  NoBrokersAvailable,
  #[error("producer retries exhausted: {0}")]
  RetriesExhausted(String),
  #[error("broker rejected request: {0}")]
  Rejected(String),
  #[error("producer shutdown")]
  Shutdown,
}

//
// ProducerClient
//

#[async_trait]
pub trait ProducerClient: Send + Sync {
  async fn produce(&self, record: ProducerRecord) -> Result<ProducerAck, ProducerError>;
  async fn flush(&self) -> Result<(), ProducerError>;
}

//
// BrokerTransport
//

#[async_trait]
pub trait BrokerTransport: Send + Sync {
  async fn produce_batch(
    &self,
    broker_address: &str,
    request: ProduceBatchRequest,
  ) -> Result<ProduceBatchResponse>;
}

//
// GrpcBrokerTransport
//

pub struct GrpcBrokerTransport {
  config: ProducerConfig,
}

impl GrpcBrokerTransport {
  #[must_use]
  pub fn new(config: ProducerConfig) -> Self {
    Self { config }
  }
}

#[async_trait]
impl BrokerTransport for GrpcBrokerTransport {
  async fn produce_batch(
    &self,
    broker_address: &str,
    request: ProduceBatchRequest,
  ) -> Result<ProduceBatchResponse> {
    let connect_timeout = TimeDuration::milliseconds(producer_connect_timeout_ms(&self.config));
    let client = GrpcClient::new_http(
      broker_address,
      connect_timeout,
      producer_max_request_concurrency(&self.config),
    )?;
    let service_method = ServiceMethod::<ProduceBatchRequest, ProduceBatchResponse>::new(
      "BrokerService",
      "ProduceBatch",
    );
    let request_timeout = TimeDuration::milliseconds(producer_request_timeout_ms(&self.config));
    let response = client
      .unary(
        &service_method,
        None,
        request,
        request_timeout,
        compression_as_grpc(producer_compression(&self.config)),
      )
      .await
      .map_err(|error| anyhow!(error.to_string()))?;
    Ok(response)
  }
}

//
// ProducerClientImpl
//

pub struct ProducerClientImpl {
  config: ProducerConfig,
  topics: HashMap<String, ProducerTopicConfig>,
  membership_rx: watch::Receiver<BrokerMembership>,
  transport: Arc<dyn BrokerTransport>,
  metrics: Arc<ProducerMetrics>,
  state: Arc<Mutex<ProducerState>>,
  flush_task: JoinHandle<()>,
}

impl ProducerClientImpl {
  pub async fn new(
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
    discovery: Arc<dyn BrokerDiscovery>,
    metrics_scope: Scope,
  ) -> Result<Self> {
    let transport: Arc<dyn BrokerTransport> = Arc::new(GrpcBrokerTransport::new(config.clone()));
    Self::new_with_transport(config, topics, discovery, transport, metrics_scope).await
  }

  pub async fn from_runtime_config(
    runtime: ProducerRuntimeConfig,
    metrics_scope: Scope,
  ) -> Result<Self> {
    validate_runtime_config(&runtime)?;
    let producer = runtime
      .producer
      .as_ref()
      .ok_or_else(|| anyhow!("producer config is required"))?
      .clone();
    let discovery = runtime
      .discovery
      .as_ref()
      .ok_or_else(|| anyhow!("producer discovery config is required"))?
      .clone();
    let topics = runtime.topics.clone();
    let discovery = into_discovery(&discovery)?;
    let transport: Arc<dyn BrokerTransport> = Arc::new(GrpcBrokerTransport::new(producer.clone()));
    Self::new_with_transport(producer, topics, discovery, transport, metrics_scope).await
  }

  pub async fn new_with_transport(
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
    discovery: Arc<dyn BrokerDiscovery>,
    transport: Arc<dyn BrokerTransport>,
    metrics_scope: Scope,
  ) -> Result<Self> {
    Self::new_with_transport_and_scope(config, topics, discovery, transport, metrics_scope).await
  }

  pub async fn new_with_transport_and_scope(
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
    discovery: Arc<dyn BrokerDiscovery>,
    transport: Arc<dyn BrokerTransport>,
    metrics_scope: Scope,
  ) -> Result<Self> {
    validate_producer_config(&config)?;
    let writer_id = producer_writer_id(&config);

    let mut topic_map = HashMap::new();
    for topic in topics {
      validate_topic_config(&topic)?;
      ensure!(
        writer_id < topic.num_writers,
        "writer_id {} must be less than num_writers {} for topic {}",
        writer_id,
        topic.num_writers,
        topic.name
      );
      let topic_name = topic.name.to_string();
      if topic_map.contains_key(&topic_name) {
        bail!("duplicate topic config: {topic_name}");
      }
      topic_map.insert(topic_name, topic);
    }

    ensure!(
      !topic_map.is_empty(),
      "at least one topic must be configured"
    );

    let membership_rx = discovery.watch_membership().await?;
    let state = Arc::new(Mutex::new(ProducerState::default()));
    let metrics = Arc::new(ProducerMetrics::new(&metrics_scope));

    log::info!(
      "producer initialized: writer_id={}, topics={}, flush_max_delay_ms={}, \
       max_batch_records={}, max_batch_bytes={}",
      writer_id,
      topic_map.len(),
      producer_flush_max_delay_ms(&config),
      producer_max_batch_records(&config),
      producer_max_batch_bytes(&config)
    );

    let flush_task = Self::spawn_flush_loop(
      Arc::clone(&state),
      Arc::clone(&transport),
      config.clone(),
      topic_map.clone(),
      membership_rx.clone(),
      Arc::clone(&metrics),
    );

    Ok(Self {
      config,
      topics: topic_map,
      membership_rx,
      transport,
      metrics,
      state,
      flush_task,
    })
  }

  fn spawn_flush_loop(
    state: Arc<Mutex<ProducerState>>,
    transport: Arc<dyn BrokerTransport>,
    config: ProducerConfig,
    topics: HashMap<String, ProducerTopicConfig>,
    membership_rx: watch::Receiver<BrokerMembership>,
    metrics: Arc<ProducerMetrics>,
  ) -> JoinHandle<()> {
    // The flush loop handles time-based flushes so producers can efficiently batch sparse traffic.
    tokio::spawn(async move {
      let tick_ms = (producer_flush_max_delay_ms(&config) / 2).max(10);
      let mut ticker = interval(Duration::from_millis(tick_ms));

      loop {
        ticker.tick().await;
        let batches = {
          let mut guard = state.lock().await;
          guard.collect_ready_batches(producer_flush_max_delay_ms(&config))
        };

        if !batches.is_empty() {
          trace!(
            "producer flush loop found {} ready batch(es)",
            batches.len()
          );
        }

        for batch in batches {
          let result = send_batch_with_retry(
            &config,
            &topics,
            &membership_rx,
            transport.as_ref(),
            &batch,
            &metrics,
          )
          .await;
          notify_waiters(batch.waiters, &result);
        }
      }
    })
  }
}

impl Drop for ProducerClientImpl {
  fn drop(&mut self) {
    debug!("producer dropping; aborting flush loop task");
    self.flush_task.abort();
  }
}

#[async_trait]
impl ProducerClient for ProducerClientImpl {
  async fn produce(&self, record: ProducerRecord) -> Result<ProducerAck, ProducerError> {
    let topic = self
      .topics
      .get(&record.topic)
      .ok_or_else(|| ProducerError::UnknownTopic(record.topic.clone()))?;

    let virtual_partition_id = compute_virtual_partition_id(
      &record.record_key,
      topic.partition_count,
      producer_writer_id(&self.config),
    );

    let (tx, rx) = oneshot::channel();
    let topic = record.topic.clone();
    let payload_len = record.payload.len();

    let maybe_batch = {
      let mut guard = self.state.lock().await;
      guard.push_record(
        BufferedRecord {
          topic,
          virtual_partition_id,
          proto_record: Record {
            payload: record.payload.into(),
            event_ts_ms: record.event_ts_ms,
            ..Default::default()
          },
          waiter: tx,
        },
        producer_max_batch_records(&self.config) as usize,
        producer_max_batch_bytes(&self.config) as usize,
      )
    };

    self.metrics.records_enqueued.inc();
    trace!(
      "record buffered: topic={}, virtual_partition_id={}, payload_bytes={}",
      record.topic, virtual_partition_id, payload_len
    );

    if let Some(batch) = maybe_batch {
      trace!(
        "size flush triggered: topic={}, virtual_partition_id={}, records={}",
        batch.topic,
        batch.virtual_partition_id,
        batch.records.len()
      );
      let result = send_batch_with_retry(
        &self.config,
        &self.topics,
        &self.membership_rx,
        self.transport.as_ref(),
        &batch,
        &self.metrics,
      )
      .await;
      notify_waiters(batch.waiters, &result);
    }

    rx.await
      .map_or(Err(ProducerError::Shutdown), |result| result)
  }

  async fn flush(&self) -> Result<(), ProducerError> {
    let batches = {
      let mut guard = self.state.lock().await;
      guard.drain_all_batches()
    };

    let mut first_error: Option<ProducerError> = None;
    for batch in batches {
      let result = send_batch_with_retry(
        &self.config,
        &self.topics,
        &self.membership_rx,
        self.transport.as_ref(),
        &batch,
        &self.metrics,
      )
      .await;
      if let Err(error) = &result
        && first_error.is_none()
      {
        first_error = Some(error.clone());
      }
      notify_waiters(batch.waiters, &result);
    }

    if let Some(error) = first_error {
      return Err(error);
    }
    Ok(())
  }
}

fn notify_waiters(
  waiters: Vec<oneshot::Sender<Result<ProducerAck, ProducerError>>>,
  result: &Result<ProducerAck, ProducerError>,
) {
  for waiter in waiters {
    let _ = waiter.send(result.clone());
  }
}

async fn send_batch_with_retry(
  config: &ProducerConfig,
  topics: &HashMap<String, ProducerTopicConfig>,
  membership_rx: &watch::Receiver<BrokerMembership>,
  transport: &dyn BrokerTransport,
  batch: &BufferedBatch,
  metrics: &ProducerMetrics,
) -> Result<ProducerAck, ProducerError> {
  let _topic = topics
    .get(&batch.topic)
    .ok_or_else(|| ProducerError::UnknownTopic(batch.topic.clone()))?;

  let mut attempt: u32 = 0;
  let mut previous_owner: Option<String> = None;
  let started_at = Instant::now();
  loop {
    let membership = membership_rx.borrow().clone();
    let Some(broker) = owner_for_partition(&batch.topic, batch.virtual_partition_id, &membership)
    else {
      metrics.no_brokers.inc();
      metrics.failures.inc();
      metrics
        .send_latency_seconds
        .observe(started_at.elapsed().as_secs_f64());
      warn_every!(
        15.seconds(),
        "producer no broker owner: topic={}, virtual_partition_id={}, membership_nodes={}",
        batch.topic,
        batch.virtual_partition_id,
        membership.nodes.len()
      );
      return Err(ProducerError::NoBrokersAvailable);
    };

    if previous_owner
      .as_deref()
      .is_some_and(|old| old != broker.node_id)
    {
      debug!(
        "producer routing changed after retry: topic={}, virtual_partition_id={}, from={}, to={}",
        batch.topic,
        batch.virtual_partition_id,
        previous_owner.as_deref().unwrap_or_default(),
        broker.node_id
      );
    }
    previous_owner = Some(broker.node_id.clone());

    trace!(
      "send attempt: topic={}, virtual_partition_id={}, attempt={}, broker={}",
      batch.topic,
      batch.virtual_partition_id,
      attempt.saturating_add(1),
      broker.address
    );

    let request = ProduceBatchRequest {
      topic: batch.topic.clone().into(),
      virtual_partition_id: batch.virtual_partition_id,
      records: batch.records.clone(),
      ..Default::default()
    };

    let response = transport.produce_batch(&broker.address, request).await;
    let current_error = match response {
      Ok(response) => {
        let status = response.status.enum_value_or_default();
        match status {
          ProduceStatus::PRODUCE_STATUS_OK => {
            metrics.batches_sent.inc();
            metrics.records_sent.inc_by(batch.records.len() as u64);
            metrics
              .send_latency_seconds
              .observe(started_at.elapsed().as_secs_f64());
            return Ok(ProducerAck {
              topic: batch.topic.clone(),
              virtual_partition_id: batch.virtual_partition_id,
              attempts: attempt.saturating_add(1),
            });
          },
          ProduceStatus::PRODUCE_STATUS_UNKNOWN_TOPIC => {
            return Err(ProducerError::UnknownTopic(batch.topic.clone()));
          },
          ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER
          | ProduceStatus::PRODUCE_STATUS_OVERLOADED => {
            if response.error_message.is_empty() {
              format!("broker status: {status:?}")
            } else {
              response.error_message.to_string()
            }
          },
        }
      },
      Err(error) => error.to_string(),
    };

    if attempt >= producer_max_retries(config) {
      metrics.failures.inc();
      metrics
        .send_latency_seconds
        .observe(started_at.elapsed().as_secs_f64());
      warn_every!(
        15.seconds(),
        "producer retries exhausted: topic={}, virtual_partition_id={}, attempts={}, error={}",
        batch.topic,
        batch.virtual_partition_id,
        attempt.saturating_add(1),
        current_error
      );
      return Err(ProducerError::RetriesExhausted(current_error));
    }

    let delay_ms = retry_delay_ms(config, attempt);
    metrics.retries.inc();
    warn_every!(
      15.seconds(),
      "producer retrying batch: topic={}, virtual_partition_id={}, attempt={}, delay_ms={}, \
       error={}",
      batch.topic,
      batch.virtual_partition_id,
      attempt.saturating_add(1),
      delay_ms,
      current_error
    );
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    attempt = attempt.saturating_add(1);
  }
}

fn retry_delay_ms(config: &ProducerConfig, attempt: u32) -> u64 {
  let exponential =
    producer_retry_base_delay_ms(config).saturating_mul(2u64.saturating_pow(attempt));
  exponential.min(producer_retry_max_delay_ms(config))
}

fn compute_virtual_partition_id(
  record_key: &[u8],
  partition_count: u32,
  writer_id: u32,
) -> VirtualPartitionId {
  virtual_partition_for_key(record_key, partition_count, writer_id)
}

//
// BufferedRecord
//

struct BufferedRecord {
  topic: String,
  virtual_partition_id: VirtualPartitionId,
  proto_record: Record,
  waiter: oneshot::Sender<Result<ProducerAck, ProducerError>>,
}
//
// BufferedBatch
//

struct BufferedBatch {
  topic: String,
  virtual_partition_id: VirtualPartitionId,
  records: Vec<Record>,
  waiters: Vec<oneshot::Sender<Result<ProducerAck, ProducerError>>>,
}

//
// PartitionBuffer
//

#[derive(Default)]
struct PartitionBuffer {
  records: Vec<Record>,
  waiters: Vec<oneshot::Sender<Result<ProducerAck, ProducerError>>>,
  buffered_bytes: usize,
  first_buffered_at: Option<Instant>,
}

impl PartitionBuffer {
  fn push(&mut self, record: Record, waiter: oneshot::Sender<Result<ProducerAck, ProducerError>>) {
    if self.first_buffered_at.is_none() {
      self.first_buffered_at = Some(Instant::now());
    }
    self.buffered_bytes = self.buffered_bytes.saturating_add(record.payload.len());
    self.records.push(record);
    self.waiters.push(waiter);
  }

  fn should_flush_by_size(&self, max_batch_records: usize, max_batch_bytes: usize) -> bool {
    self.records.len() >= max_batch_records || self.buffered_bytes >= max_batch_bytes
  }

  fn should_flush_by_time(&self, flush_max_delay_ms: u64) -> bool {
    self
      .first_buffered_at
      .is_some_and(|first| first.elapsed() >= Duration::from_millis(flush_max_delay_ms))
  }

  fn take_batch(
    &mut self,
    topic: &str,
    virtual_partition_id: VirtualPartitionId,
  ) -> Option<BufferedBatch> {
    if self.records.is_empty() {
      return None;
    }

    let records = std::mem::take(&mut self.records);
    let waiters = std::mem::take(&mut self.waiters);
    self.buffered_bytes = 0;
    self.first_buffered_at = None;

    Some(BufferedBatch {
      topic: topic.to_string(),
      virtual_partition_id,
      records,
      waiters,
    })
  }
}

//
// ProducerState
//

#[derive(Default)]
struct ProducerState {
  buffers: HashMap<(String, VirtualPartitionId), PartitionBuffer>,
}

impl ProducerState {
  fn push_record(
    &mut self,
    record: BufferedRecord,
    max_batch_records: usize,
    max_batch_bytes: usize,
  ) -> Option<BufferedBatch> {
    let key = (record.topic.clone(), record.virtual_partition_id);
    let buffer = self.buffers.entry(key.clone()).or_default();
    buffer.push(record.proto_record, record.waiter);

    if buffer.should_flush_by_size(max_batch_records, max_batch_bytes) {
      return buffer.take_batch(&key.0, key.1);
    }

    None
  }

  fn collect_ready_batches(&mut self, flush_max_delay_ms: u64) -> Vec<BufferedBatch> {
    let mut ready = Vec::new();

    for ((topic, virtual_partition_id), buffer) in &mut self.buffers {
      if !buffer.should_flush_by_time(flush_max_delay_ms) {
        continue;
      }

      if let Some(batch) = buffer.take_batch(topic, *virtual_partition_id) {
        ready.push(batch);
      }
    }

    ready
  }

  fn drain_all_batches(&mut self) -> Vec<BufferedBatch> {
    let mut batches = Vec::new();
    for ((topic, virtual_partition_id), buffer) in &mut self.buffers {
      if let Some(batch) = buffer.take_batch(topic, *virtual_partition_id) {
        batches.push(batch);
      }
    }
    batches
  }
}
