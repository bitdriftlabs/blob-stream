// blob-stream - producer client
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./producer_test.rs"]
mod tests;

use anyhow::{Result, anyhow, bail, ensure};
use async_trait::async_trait;
use bd_grpc::client::Client as GrpcClient;
use bd_grpc::compression::Compression;
use bd_grpc::service::ServiceMethod;
use blob_stream_broker_discovery::{BrokerDiscovery, BrokerMembership, owner_for_partition};
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchRequest,
  ProduceBatchResponse,
  ProduceStatus,
  Record,
};
use blob_stream_types::{VirtualPartitionId, virtual_partition_for_key};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use time::Duration as TimeDuration;
use tokio::sync::{Mutex, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval};

const DEFAULT_MAX_BATCH_RECORDS: usize = 1_000;
const DEFAULT_MAX_BATCH_BYTES: usize = 1_048_576;
const DEFAULT_FLUSH_MAX_DELAY_MS: u64 = 200;
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_RETRY_BASE_DELAY_MS: u64 = 25;
const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 1_000;
const DEFAULT_CONNECT_TIMEOUT_MS: i64 = 2_000;
const DEFAULT_REQUEST_TIMEOUT_MS: i64 = 5_000;
const DEFAULT_MAX_REQUEST_CONCURRENCY: u64 = 64;

//
// ProducerCompression
//

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProducerCompression {
  #[default]
  None,
  Snappy,
}

impl ProducerCompression {
  fn as_grpc(self) -> Compression {
    match self {
      Self::None => Compression::None,
      Self::Snappy => Compression::Snappy,
    }
  }
}

//
// ProducerConfig
//

#[derive(Clone, Debug)]
pub struct ProducerConfig {
  pub writer_id: u32,
  pub max_batch_records: usize,
  pub max_batch_bytes: usize,
  pub flush_max_delay_ms: u64,
  pub max_retries: u32,
  pub retry_base_delay_ms: u64,
  pub retry_max_delay_ms: u64,
  pub connect_timeout_ms: i64,
  pub request_timeout_ms: i64,
  pub max_request_concurrency: u64,
  pub compression: ProducerCompression,
}

impl ProducerConfig {
  #[must_use]
  pub fn with_defaults() -> Self {
    Self {
      writer_id: 0,
      max_batch_records: DEFAULT_MAX_BATCH_RECORDS,
      max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
      flush_max_delay_ms: DEFAULT_FLUSH_MAX_DELAY_MS,
      max_retries: DEFAULT_MAX_RETRIES,
      retry_base_delay_ms: DEFAULT_RETRY_BASE_DELAY_MS,
      retry_max_delay_ms: DEFAULT_RETRY_MAX_DELAY_MS,
      connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
      request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
      max_request_concurrency: DEFAULT_MAX_REQUEST_CONCURRENCY,
      compression: ProducerCompression::None,
    }
  }

  fn validate(&self) -> Result<()> {
    ensure!(
      self.max_batch_records > 0,
      "producer.max_batch_records must be greater than zero"
    );
    ensure!(
      self.max_batch_bytes > 0,
      "producer.max_batch_bytes must be greater than zero"
    );
    ensure!(
      self.flush_max_delay_ms > 0,
      "producer.flush_max_delay_ms must be greater than zero"
    );
    ensure!(
      self.retry_base_delay_ms > 0,
      "producer.retry_base_delay_ms must be greater than zero"
    );
    ensure!(
      self.retry_max_delay_ms > 0,
      "producer.retry_max_delay_ms must be greater than zero"
    );
    ensure!(
      self.connect_timeout_ms > 0,
      "producer.connect_timeout_ms must be greater than zero"
    );
    ensure!(
      self.request_timeout_ms > 0,
      "producer.request_timeout_ms must be greater than zero"
    );
    ensure!(
      self.max_request_concurrency > 0,
      "producer.max_request_concurrency must be greater than zero"
    );
    Ok(())
  }
}

//
// ProducerTopicConfig
//

#[derive(Clone, Debug)]
pub struct ProducerTopicConfig {
  pub name: String,
  pub partition_count: u32,
  pub num_writers: u32,
}

impl ProducerTopicConfig {
  fn validate(&self) -> Result<()> {
    ensure!(!self.name.trim().is_empty(), "topic name is required");
    ensure!(
      self.partition_count > 0,
      "partition_count must be greater than zero"
    );
    ensure!(
      self.num_writers > 0,
      "num_writers must be greater than zero"
    );
    Ok(())
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
    let connect_timeout = TimeDuration::milliseconds(self.config.connect_timeout_ms);
    let client = GrpcClient::new_http(
      broker_address,
      connect_timeout,
      self.config.max_request_concurrency,
    )?;
    let service_method = ServiceMethod::<ProduceBatchRequest, ProduceBatchResponse>::new(
      "BrokerService",
      "ProduceBatch",
    );
    let request_timeout = TimeDuration::milliseconds(self.config.request_timeout_ms);
    let response = client
      .unary(
        &service_method,
        None,
        request,
        request_timeout,
        self.config.compression.as_grpc(),
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
  state: Arc<Mutex<ProducerState>>,
  flush_task: JoinHandle<()>,
}

impl ProducerClientImpl {
  pub async fn new(
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
    discovery: Arc<dyn BrokerDiscovery>,
  ) -> Result<Self> {
    let transport: Arc<dyn BrokerTransport> = Arc::new(GrpcBrokerTransport::new(config.clone()));
    Self::new_with_transport(config, topics, discovery, transport).await
  }

  pub async fn new_with_transport(
    config: ProducerConfig,
    topics: Vec<ProducerTopicConfig>,
    discovery: Arc<dyn BrokerDiscovery>,
    transport: Arc<dyn BrokerTransport>,
  ) -> Result<Self> {
    config.validate()?;

    let mut topic_map = HashMap::new();
    for topic in topics {
      topic.validate()?;
      ensure!(
        config.writer_id < topic.num_writers,
        "writer_id {} must be less than num_writers {} for topic {}",
        config.writer_id,
        topic.num_writers,
        topic.name
      );
      if topic_map.contains_key(&topic.name) {
        bail!("duplicate topic config: {}", topic.name);
      }
      topic_map.insert(topic.name.clone(), topic);
    }

    ensure!(
      !topic_map.is_empty(),
      "at least one topic must be configured"
    );

    let membership_rx = discovery.watch_membership().await?;
    let state = Arc::new(Mutex::new(ProducerState::default()));

    let flush_task = Self::spawn_flush_loop(
      Arc::clone(&state),
      Arc::clone(&transport),
      config.clone(),
      topic_map.clone(),
      membership_rx.clone(),
    );

    Ok(Self {
      config,
      topics: topic_map,
      membership_rx,
      transport,
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
  ) -> JoinHandle<()> {
    // The flush loop handles time-based flushes so producers can efficiently batch sparse traffic.
    tokio::spawn(async move {
      let tick_ms = (config.flush_max_delay_ms / 2).max(10);
      let mut ticker = interval(Duration::from_millis(tick_ms));

      loop {
        ticker.tick().await;
        let batches = {
          let mut guard = state.lock().await;
          guard.collect_ready_batches(config.flush_max_delay_ms)
        };

        for batch in batches {
          let result =
            send_batch_with_retry(&config, &topics, &membership_rx, transport.as_ref(), &batch)
              .await;
          notify_waiters(batch.waiters, &result);
        }
      }
    })
  }
}

impl Drop for ProducerClientImpl {
  fn drop(&mut self) {
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
      self.config.writer_id,
    );

    let (tx, rx) = oneshot::channel();

    let maybe_batch = {
      let mut guard = self.state.lock().await;
      guard.push_record(
        BufferedRecord {
          topic: record.topic,
          virtual_partition_id,
          proto_record: Record {
            payload: record.payload.into(),
            event_ts_ms: record.event_ts_ms,
            ..Default::default()
          },
          waiter: tx,
        },
        self.config.max_batch_records,
        self.config.max_batch_bytes,
      )
    };

    if let Some(batch) = maybe_batch {
      let result = send_batch_with_retry(
        &self.config,
        &self.topics,
        &self.membership_rx,
        self.transport.as_ref(),
        &batch,
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
) -> Result<ProducerAck, ProducerError> {
  let _topic = topics
    .get(&batch.topic)
    .ok_or_else(|| ProducerError::UnknownTopic(batch.topic.clone()))?;

  let mut attempt: u32 = 0;
  loop {
    let membership = membership_rx.borrow().clone();
    let broker = owner_for_partition(&batch.topic, batch.virtual_partition_id, &membership)
      .ok_or(ProducerError::NoBrokersAvailable)?;

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

    if attempt >= config.max_retries {
      return Err(ProducerError::RetriesExhausted(current_error));
    }

    let delay_ms = retry_delay_ms(config, attempt);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    attempt = attempt.saturating_add(1);
  }
}

fn retry_delay_ms(config: &ProducerConfig, attempt: u32) -> u64 {
  let exponential = config
    .retry_base_delay_ms
    .saturating_mul(2u64.saturating_pow(attempt));
  exponential.min(config.retry_max_delay_ms)
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
