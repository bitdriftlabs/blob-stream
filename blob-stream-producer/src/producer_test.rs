#![allow(clippy::unwrap_used)]

use super::{
  ProducerClient,
  ProducerClientImpl,
  ProducerError,
  ProducerRecord,
  broker_assignment,
  compute_virtual_partition_id,
  retry_delay_ms,
};
use crate::config::{producer_config_with_defaults, producer_writer_id};
use crate::{ProducerCompression, ProducerConfig, ProducerTopicConfig};
use anyhow::anyhow;
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode, BrokerPartition};
use blob_stream_proto::protos::blobstream::v1::broker::{ProduceBatchResponse, ProduceStatus};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

struct TestBrokerDiscovery {
  membership: BrokerMembership,
}

impl TestBrokerDiscovery {
  fn new(membership: BrokerMembership) -> Self {
    Self { membership }
  }
}

#[async_trait]
impl blob_stream_broker_discovery::BrokerDiscovery for TestBrokerDiscovery {
  async fn watch_membership(&self) -> anyhow::Result<watch::Receiver<BrokerMembership>> {
    let (tx, rx) = watch::channel(self.membership.clone());
    drop(tx);
    Ok(rx)
  }
}

#[derive(Clone, Debug, PartialEq)]
struct SentBatch {
  broker_address: String,
  request: blob_stream_proto::protos::blobstream::v1::broker::ProduceBatchRequest,
}

#[derive(Default)]
struct FakeBrokerTransport {
  sent: Mutex<Vec<SentBatch>>,
  responses: Mutex<VecDeque<std::result::Result<ProduceBatchResponse, anyhow::Error>>>,
}

impl FakeBrokerTransport {
  async fn enqueue_response(
    &self,
    response: std::result::Result<ProduceBatchResponse, anyhow::Error>,
  ) {
    self.responses.lock().await.push_back(response);
  }
}

#[async_trait]
impl super::BrokerTransport for FakeBrokerTransport {
  async fn produce_batch(
    &self,
    broker_address: &str,
    request: blob_stream_proto::protos::blobstream::v1::broker::ProduceBatchRequest,
  ) -> anyhow::Result<ProduceBatchResponse> {
    self.sent.lock().await.push(SentBatch {
      broker_address: broker_address.to_string(),
      request,
    });

    self.responses.lock().await.pop_front().unwrap_or_else(|| {
      Ok(ProduceBatchResponse {
        status: ProduceStatus::PRODUCE_STATUS_OK.into(),
        ..Default::default()
      })
    })
  }
}

fn topic_config() -> ProducerTopicConfig {
  ProducerTopicConfig {
    name: "telemetry".into(),
    partition_count: 16,
    num_writers: 2,
    retention_days: 0,
    ..Default::default()
  }
}

fn default_config() -> ProducerConfig {
  let mut config = producer_config_with_defaults();
  config.writer_id = Some(1);
  config.max_batch_records = Some(1);
  config.max_batch_bytes = Some(1_024);
  config.flush_max_delay_ms = Some(1_000);
  config.max_retries = Some(4);
  config.retry_base_delay_ms = Some(1);
  config.retry_max_delay_ms = Some(8);
  config.connect_timeout_ms = Some(1_000);
  config.request_timeout_ms = Some(1_000);
  config.max_request_concurrency = Some(16);
  config.compression = Some(ProducerCompression::PRODUCER_COMPRESSION_SNAPPY.into());
  config
}

fn membership() -> BrokerMembership {
  BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".to_string(),
      address: "a:8080".to_string(),
    },
    BrokerNode {
      node_id: "node-b".to_string(),
      address: "b:8080".to_string(),
    },
  ])
}

fn metrics_scope() -> bd_server_stats::stats::Scope {
  Collector::default().scope("blob_stream_producer_test")
}

#[tokio::test]
async fn diagnostics_report_buffered_partition_state() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  let producer = Arc::new(
    ProducerClientImpl::new_with_transport(
      config,
      vec![topic_config()],
      discovery,
      transport,
      metrics_scope(),
    )
    .await
    .unwrap(),
  );

  let pending_producer = Arc::clone(&producer);
  let pending_produce = tokio::spawn(async move {
    pending_producer
      .produce(ProducerRecord::new(
        "telemetry",
        b"diagnostics-key".to_vec(),
        vec![1, 2, 3],
        100,
      ))
      .await
  });

  tokio::task::yield_now().await;

  let snapshot = producer
    .diagnostics()
    .expect("producer implementation provides diagnostics")
    .state_snapshot()
    .await;
  assert_eq!(snapshot.schema_version, 2);
  assert_eq!(snapshot.writer_id, 1);
  assert_eq!(snapshot.max_batch_records, 2);
  assert_eq!(
    snapshot.brokers,
    vec![
      super::ProducerBrokerSnapshot {
        node_id: "node-a".to_string(),
        address: "a:8080".to_string(),
      },
      super::ProducerBrokerSnapshot {
        node_id: "node-b".to_string(),
        address: "b:8080".to_string(),
      },
    ]
  );
  assert_eq!(snapshot.topics.len(), 1);
  assert_eq!(snapshot.topics[0].name, "telemetry");
  assert_eq!(snapshot.route_map.len(), 16);
  let writer_one_partition = snapshot
    .route_map
    .iter()
    .find(|route| route.virtual_partition_id == 16)
    .expect("writer one partition should be present in the route map");
  assert_eq!(writer_one_partition.producer_writer_id, 1);
  assert_eq!(writer_one_partition.logical_partition_id, 0);
  assert!(writer_one_partition.selected_broker.is_some());
  assert_eq!(snapshot.partition_buffers.len(), 1);
  assert_eq!(snapshot.partition_buffers[0].topic, "telemetry");
  assert_eq!(snapshot.partition_buffers[0].buffered_record_count, 1);
  assert_eq!(snapshot.partition_buffers[0].pending_ack_count, 1);
  assert_eq!(snapshot.partition_buffers[0].buffered_bytes, 3);
  assert!(
    snapshot.partition_buffers[0]
      .oldest_buffered_age_ms
      .is_some()
  );

  pending_produce.abort();
  let _ignored = pending_produce.await;
}

#[tokio::test]
async fn routes_to_expected_broker() {
  let config = default_config();
  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  let topic = topic_config();
  let producer = ProducerClientImpl::new_with_transport(
    config.clone(),
    vec![topic.clone()],
    discovery,
    transport.clone(),
    metrics_scope(),
  )
  .await
  .unwrap();

  let key = b"device-123".to_vec();
  let ack = producer
    .produce(ProducerRecord::new(
      "telemetry",
      key.clone(),
      vec![1, 2, 3],
      100,
    ))
    .await
    .unwrap();

  let expected_partition = compute_virtual_partition_id(&key, 16, producer_writer_id(&config));
  assert_eq!(ack.virtual_partition_id, expected_partition);
  let discovered_membership = membership();
  let expected_assignment = broker_assignment(
    &HashMap::from([("telemetry".to_string(), topic)]),
    producer_writer_id(&config),
    &discovered_membership,
  );
  let expected_owner = expected_assignment
    .get(&BrokerPartition {
      topic: "telemetry".to_string(),
      virtual_partition_id: expected_partition,
    })
    .unwrap();

  let sent = transport.sent.lock().await;
  assert_eq!(sent.len(), 1);
  assert_eq!(sent[0].broker_address, expected_owner.address);
  assert_eq!(sent[0].request.virtual_partition_id, expected_partition);
}

#[tokio::test]
async fn retries_transient_status_until_success() {
  let config = default_config();
  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());

  transport
    .enqueue_response(Ok(ProduceBatchResponse {
      status: ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER.into(),
      error_message: "lease moved".into(),
      ..Default::default()
    }))
    .await;
  transport
    .enqueue_response(Ok(ProduceBatchResponse {
      status: ProduceStatus::PRODUCE_STATUS_OVERLOADED.into(),
      error_message: "busy".into(),
      ..Default::default()
    }))
    .await;
  transport
    .enqueue_response(Ok(ProduceBatchResponse {
      status: ProduceStatus::PRODUCE_STATUS_OK.into(),
      ..Default::default()
    }))
    .await;

  let producer = ProducerClientImpl::new_with_transport(
    config,
    vec![topic_config()],
    discovery,
    transport.clone(),
    metrics_scope(),
  )
  .await
  .unwrap();

  let ack = producer
    .produce(ProducerRecord::new(
      "telemetry",
      b"retry-key".to_vec(),
      vec![7],
      100,
    ))
    .await
    .unwrap();

  assert_eq!(ack.attempts, 3);
  assert_eq!(transport.sent.lock().await.len(), 3);
}

#[tokio::test]
async fn batches_by_partition_and_acks_waiters() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(10_000);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  let producer = Arc::new(
    ProducerClientImpl::new_with_transport(
      config,
      vec![topic_config()],
      discovery,
      transport.clone(),
      metrics_scope(),
    )
    .await
    .unwrap(),
  );

  let first = {
    let producer = Arc::clone(&producer);
    tokio::spawn(async move {
      producer
        .produce(ProducerRecord::new(
          "telemetry",
          b"same-key".to_vec(),
          vec![1],
          100,
        ))
        .await
    })
  };

  let second = {
    let producer = Arc::clone(&producer);
    tokio::spawn(async move {
      producer
        .produce(ProducerRecord::new(
          "telemetry",
          b"same-key".to_vec(),
          vec![2],
          101,
        ))
        .await
    })
  };

  let ack_one = first.await.unwrap().unwrap();
  let ack_two = second.await.unwrap().unwrap();
  assert_eq!(ack_one.virtual_partition_id, ack_two.virtual_partition_id);

  let sent = transport.sent.lock().await;
  assert_eq!(sent.len(), 1);
  assert_eq!(sent[0].request.records.len(), 2);
}

#[tokio::test]
async fn surfaces_retry_exhaustion_for_transport_errors() {
  let mut config = default_config();
  config.max_retries = Some(2);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  transport
    .enqueue_response(Err(anyhow!("network down")))
    .await;
  transport
    .enqueue_response(Err(anyhow!("network down")))
    .await;
  transport
    .enqueue_response(Err(anyhow!("network down")))
    .await;

  let producer = ProducerClientImpl::new_with_transport(
    config,
    vec![topic_config()],
    discovery,
    transport,
    metrics_scope(),
  )
  .await
  .unwrap();

  let error = producer
    .produce(ProducerRecord::new(
      "telemetry",
      b"retry-fail".to_vec(),
      vec![1],
      0,
    ))
    .await
    .unwrap_err();
  assert!(matches!(error, ProducerError::RetriesExhausted(_)));
}

#[test]
fn retry_backoff_is_exponential_and_capped() {
  let config = default_config();
  assert_eq!(retry_delay_ms(&config, 0), 1);
  assert_eq!(retry_delay_ms(&config, 1), 2);
  assert_eq!(retry_delay_ms(&config, 2), 4);
  assert_eq!(retry_delay_ms(&config, 3), 8);
  assert_eq!(retry_delay_ms(&config, 4), 8);
}
