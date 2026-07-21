#![allow(clippy::unwrap_used)]

use super::{
  BrokerTransport,
  GrpcBrokerTransport,
  ProducerClient,
  ProducerClientImpl,
  ProducerError,
  ProducerRecord,
  ProducerRetryReason,
  RetryClock,
  broker_assignment,
  compute_virtual_partition_id,
  next_retry_delay,
  producer_retry_backoff,
  send_batch_with_retry_and_retry_control,
};
use crate::config::{producer_config_with_defaults, producer_writer_id, validate_producer_config};
use crate::{ProducerCompression, ProducerConfig, ProducerTopicConfig};
use anyhow::anyhow;
use async_trait::async_trait;
use bd_server_stats::stats::Collector;
use blob_stream_broker_discovery::{BrokerMembership, BrokerNode, BrokerPartition};
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchRequest,
  ProduceBatchResponse,
  ProduceStatus,
};
use blob_stream_types::VirtualPartitionId;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, mpsc, watch};
use tokio::time::{Instant, timeout};

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
    request_timeout: Duration,
  ) -> anyhow::Result<ProduceBatchResponse> {
    assert!(!request_timeout.is_zero());
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

struct GatedBrokerTransport {
  entered_tx: mpsc::UnboundedSender<VirtualPartitionId>,
  release: Arc<Semaphore>,
}

#[async_trait]
impl super::BrokerTransport for GatedBrokerTransport {
  async fn produce_batch(
    &self,
    _broker_address: &str,
    request: ProduceBatchRequest,
    request_timeout: Duration,
  ) -> anyhow::Result<ProduceBatchResponse> {
    assert!(!request_timeout.is_zero());
    self
      .entered_tx
      .send(request.virtual_partition_id)
      .map_err(|_| anyhow!("test receiver dropped"))?;
    self
      .release
      .acquire()
      .await
      .map_err(|_| anyhow!("test gate closed"))?
      .forget();
    Ok(ProduceBatchResponse {
      status: ProduceStatus::PRODUCE_STATUS_OK.into(),
      ..Default::default()
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
  config.retry_deadline_ms = Some(1_000);
  config.connect_timeout_ms = Some(1_000);
  config.request_timeout_ms = Some(1_000);
  config.max_request_concurrency = Some(16);
  config.compression = Some(ProducerCompression::PRODUCER_COMPRESSION_SNAPPY.into());
  config
}

#[test]
fn grpc_transport_reuses_client_for_broker_address() {
  let transport = GrpcBrokerTransport::new(default_config());

  let first = transport.client_for_address("127.0.0.1:8080").unwrap();
  let second = transport.client_for_address("127.0.0.1:8080").unwrap();
  let other = transport.client_for_address("127.0.0.1:8081").unwrap();

  assert!(Arc::ptr_eq(&first, &second));
  assert!(!Arc::ptr_eq(&first, &other));
}

#[test]
fn grpc_transport_trait_dispatch_evicts_clients_for_removed_brokers() {
  let concrete_transport = Arc::new(GrpcBrokerTransport::new(default_config()));
  let removed = concrete_transport.client_for_address("a:8080").unwrap();
  let retained = concrete_transport.client_for_address("b:8080").unwrap();
  let transport: Arc<dyn BrokerTransport> = concrete_transport.clone();

  transport.reconcile_membership(&BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".to_string(),
    address: "b:8080".to_string(),
  }]));

  let retained_after_reconcile = concrete_transport.client_for_address("b:8080").unwrap();
  let recreated = concrete_transport.client_for_address("a:8080").unwrap();

  assert!(Arc::ptr_eq(&retained, &retained_after_reconcile));
  assert!(!Arc::ptr_eq(&removed, &recreated));
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

fn keys_for_distinct_partitions() -> (Vec<u8>, Vec<u8>) {
  let first = b"first-partition-key".to_vec();
  let first_partition = compute_virtual_partition_id(&first, 16, 1);
  for index in 0 .. u32::MAX {
    let second = format!("second-partition-key-{index}").into_bytes();
    if compute_virtual_partition_id(&second, 16, 1) != first_partition {
      return (first, second);
    }
  }
  panic!("failed to find a key for a distinct partition");
}

async fn wait_for_buffered_partitions(producer: &ProducerClientImpl, expected: usize) {
  loop {
    let snapshot = producer
      .diagnostics()
      .expect("producer implementation provides diagnostics")
      .state_snapshot();
    let buffered_partitions = snapshot
      .partition_buffers
      .iter()
      .filter(|buffer| buffer.buffered_record_count > 0)
      .count();
    if buffered_partitions == expected {
      return;
    }
    tokio::task::yield_now().await;
  }
}

fn enqueue_distinct_partition_batches(
  producer: &Arc<ProducerClientImpl>,
) -> Vec<tokio::task::JoinHandle<Result<super::ProducerAck, ProducerError>>> {
  let (first_key, second_key) = keys_for_distinct_partitions();
  vec![first_key, second_key]
    .into_iter()
    .map(|key| {
      let producer = Arc::clone(producer);
      tokio::spawn(async move {
        producer
          .produce(ProducerRecord::new("telemetry", key, vec![1], 100))
          .await
      })
    })
    .collect()
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
    .state_snapshot();
  assert!(snapshot.generated_at.ends_with('Z'));
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
  let retry_summary = producer
    .diagnostics()
    .expect("producer implementation provides diagnostics")
    .retry_summary();
  assert_eq!(
    retry_summary.reason_counts,
    BTreeMap::from([
      (ProducerRetryReason::NotLeaseHolder, 1),
      (ProducerRetryReason::Overloaded, 1),
    ])
  );
  assert_eq!(retry_summary.samples.len(), 2);
  assert_eq!(
    retry_summary.samples[0].reason,
    ProducerRetryReason::NotLeaseHolder
  );
  assert_eq!(retry_summary.samples[0].detail, "lease moved");
  assert_eq!(
    retry_summary.samples[1].reason,
    ProducerRetryReason::Overloaded
  );
  assert_eq!(retry_summary.samples[1].detail, "busy");
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

#[tokio::test]
async fn retry_deadline_bounds_a_blocked_transport_attempt() {
  let mut config = default_config();
  config.request_timeout_ms = Some(1_000);
  config.retry_deadline_ms = Some(20);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::new(Semaphore::new(0)),
  });
  let producer = ProducerClientImpl::new_with_transport(
    config,
    vec![topic_config()],
    discovery,
    transport,
    metrics_scope(),
  )
  .await
  .unwrap();

  let produce = producer.produce(ProducerRecord::new(
    "telemetry",
    b"deadline".to_vec(),
    vec![1],
    0,
  ));
  let error = timeout(Duration::from_millis(200), produce)
    .await
    .expect("producer request should respect retry deadline")
    .unwrap_err();

  assert!(entered_rx.recv().await.is_some());
  assert!(matches!(error, ProducerError::RetriesExhausted(_)));
}

#[tokio::test]
async fn flush_returns_batch_failure_after_notifying_waiters() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);
  config.max_retries = Some(0);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  transport
    .enqueue_response(Err(anyhow!("network down")))
    .await;
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
        b"flush-error-key".to_vec(),
        vec![1],
        0,
      ))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 1).await;

  assert!(matches!(
    producer.flush().await,
    Err(ProducerError::RetriesExhausted(_))
  ));
  assert!(matches!(
    pending_produce.await.unwrap(),
    Err(ProducerError::RetriesExhausted(_))
  ));
}

#[tokio::test]
async fn flush_dispatches_distinct_partition_batches_concurrently() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);
  config.max_request_concurrency = Some(2);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::clone(&release),
  });
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

  let pending_produces = enqueue_distinct_partition_batches(&producer);
  wait_for_buffered_partitions(producer.as_ref(), 2).await;

  let flush_producer = Arc::clone(&producer);
  let flush = tokio::spawn(async move { flush_producer.flush().await });
  let first_partition = entered_rx
    .recv()
    .await
    .expect("first batch entered transport");
  let second_partition = entered_rx
    .recv()
    .await
    .expect("second batch entered transport");
  assert_ne!(first_partition, second_partition);

  release.add_permits(2);
  flush.await.unwrap().unwrap();
  for pending_produce in pending_produces {
    pending_produce.await.unwrap().unwrap();
  }
}

#[tokio::test]
async fn timed_flush_dispatches_distinct_partition_batches_concurrently() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(10);
  config.max_request_concurrency = Some(2);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::clone(&release),
  });
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

  let pending_produces = enqueue_distinct_partition_batches(&producer);
  let first_partition = entered_rx
    .recv()
    .await
    .expect("first batch entered transport");
  let second_partition = entered_rx
    .recv()
    .await
    .expect("second batch entered transport");
  assert_ne!(first_partition, second_partition);

  release.add_permits(2);
  for pending_produce in pending_produces {
    pending_produce.await.unwrap().unwrap();
  }
}

#[tokio::test]
async fn timed_flush_collects_later_batches_while_a_prior_batch_is_in_flight() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(10);
  config.max_request_concurrency = Some(2);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::clone(&release),
  });
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
  let (first_key, second_key) = keys_for_distinct_partitions();

  let first_producer = Arc::clone(&producer);
  let first = tokio::spawn(async move {
    first_producer
      .produce(ProducerRecord::new("telemetry", first_key, vec![1], 100))
      .await
  });
  let first_partition = entered_rx
    .recv()
    .await
    .expect("first batch entered transport");

  let second_producer = Arc::clone(&producer);
  let second = tokio::spawn(async move {
    second_producer
      .produce(ProducerRecord::new("telemetry", second_key, vec![2], 101))
      .await
  });
  let second_partition = timeout(Duration::from_millis(100), entered_rx.recv()).await;

  release.add_permits(2);
  first.await.unwrap().unwrap();
  second.await.unwrap().unwrap();

  assert_ne!(
    first_partition,
    second_partition
      .expect("later batch entered transport before the first batch completed")
      .expect("transport remained available")
  );
}

#[tokio::test]
async fn timed_flush_keeps_ready_batches_buffered_when_dispatches_are_full() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(10);
  config.max_request_concurrency = Some(1);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::clone(&release),
  });
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
  let (first_key, second_key) = keys_for_distinct_partitions();

  let first_producer = Arc::clone(&producer);
  let first = tokio::spawn(async move {
    first_producer
      .produce(ProducerRecord::new("telemetry", first_key, vec![1], 100))
      .await
  });
  entered_rx
    .recv()
    .await
    .expect("first batch entered transport");

  let second_producer = Arc::clone(&producer);
  let second = tokio::spawn(async move {
    second_producer
      .produce(ProducerRecord::new("telemetry", second_key, vec![2], 101))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 1).await;
  tokio::time::sleep(Duration::from_millis(30)).await;
  wait_for_buffered_partitions(producer.as_ref(), 1).await;

  release.add_permits(1);
  first.await.unwrap().unwrap();
  entered_rx
    .recv()
    .await
    .expect("second batch entered transport");
  release.add_permits(1);
  second.await.unwrap().unwrap();
}

#[tokio::test]
async fn timed_flush_rotates_ready_partitions_when_capacity_is_limited() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(10);
  config.max_request_concurrency = Some(1);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::clone(&release),
  });
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
  let (first_key, second_key) = keys_for_distinct_partitions();
  let first_partition = compute_virtual_partition_id(&first_key, 16, 1);
  let second_partition = compute_virtual_partition_id(&second_key, 16, 1);

  let first_producer = Arc::clone(&producer);
  let initial_key = first_key.clone();
  let first = tokio::spawn(async move {
    first_producer
      .produce(ProducerRecord::new("telemetry", initial_key, vec![1], 100))
      .await
  });
  assert_eq!(
    entered_rx
      .recv()
      .await
      .expect("first batch entered transport"),
    first_partition
  );

  let waiting_producer = Arc::clone(&producer);
  let waiting = tokio::spawn(async move {
    waiting_producer
      .produce(ProducerRecord::new("telemetry", second_key, vec![2], 101))
      .await
  });
  let hot_producer = Arc::clone(&producer);
  let hot = tokio::spawn(async move {
    hot_producer
      .produce(ProducerRecord::new("telemetry", first_key, vec![3], 102))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 2).await;
  tokio::time::sleep(Duration::from_millis(30)).await;

  release.add_permits(1);
  assert_eq!(
    entered_rx
      .recv()
      .await
      .expect("waiting partition entered transport"),
    second_partition
  );
  release.add_permits(1);
  assert_eq!(
    entered_rx
      .recv()
      .await
      .expect("hot partition entered transport"),
    first_partition
  );
  release.add_permits(1);

  first.await.unwrap().unwrap();
  waiting.await.unwrap().unwrap();
  hot.await.unwrap().unwrap();
}

#[tokio::test]
async fn producer_dispatch_respects_the_shared_concurrency_limit() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);
  config.max_request_concurrency = Some(1);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::clone(&release),
  });
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

  let pending_produces = enqueue_distinct_partition_batches(&producer);
  wait_for_buffered_partitions(producer.as_ref(), 2).await;

  let flush_producer = Arc::clone(&producer);
  let flush = tokio::spawn(async move { flush_producer.flush().await });
  let first_partition = entered_rx
    .recv()
    .await
    .expect("first batch entered transport");
  assert!(entered_rx.try_recv().is_err());

  release.add_permits(1);
  let second_partition = entered_rx
    .recv()
    .await
    .expect("second batch entered transport");
  assert_ne!(first_partition, second_partition);
  release.add_permits(1);

  flush.await.unwrap().unwrap();
  for pending_produce in pending_produces {
    pending_produce.await.unwrap().unwrap();
  }
}

struct FixedRetryClock {
  now: parking_lot::Mutex<Instant>,
  sleeps: parking_lot::Mutex<Vec<Duration>>,
}

impl FixedRetryClock {
  fn new(now: Instant) -> Self {
    Self {
      now: parking_lot::Mutex::new(now),
      sleeps: parking_lot::Mutex::new(Vec::new()),
    }
  }
}

#[async_trait]
impl RetryClock for FixedRetryClock {
  fn now(&self) -> Instant {
    *self.now.lock()
  }

  async fn sleep(&self, duration: Duration) {
    self.sleeps.lock().push(duration);
    *self.now.lock() += duration;
  }
}

#[test]
fn retry_backoff_respects_configured_maximum() {
  let mut config = default_config();
  config.retry_base_delay_ms = Some(1);
  config.retry_max_delay_ms = Some(8);
  let mut backoff = producer_retry_backoff(&config);

  for _ in 0 .. 10 {
    assert!(next_retry_delay(&mut backoff, Duration::from_millis(8)) <= Duration::from_millis(8));
  }
}

#[test]
fn rejects_retry_backoff_with_base_above_maximum() {
  let mut config = default_config();
  config.retry_base_delay_ms = Some(9);
  config.retry_max_delay_ms = Some(8);

  let error = validate_producer_config(&config).unwrap_err();
  assert!(error.to_string().contains("must not exceed"));
}

#[tokio::test]
async fn retry_deadline_clips_the_retry_delay() {
  let mut config = default_config();
  config.max_retries = Some(4);
  config.retry_base_delay_ms = Some(100);
  config.retry_max_delay_ms = Some(100);
  config.retry_deadline_ms = Some(10);

  let transport = FakeBrokerTransport::default();
  transport
    .enqueue_response(Err(anyhow!("network down")))
    .await;
  let clock = FixedRetryClock::new(Instant::now());
  let mut retry_backoff = producer_retry_backoff(&config);
  let membership = watch::channel(membership()).1;
  let topics = HashMap::from([("telemetry".to_string(), topic_config())]);
  let batch = super::BufferedBatch {
    topic: "telemetry".to_string(),
    virtual_partition_id: 16,
    records: Vec::new(),
    waiters: Vec::new(),
  };

  let result = send_batch_with_retry_and_retry_control(
    &config,
    &topics,
    &membership,
    &transport,
    &batch,
    &super::ProducerMetrics::new(&metrics_scope()),
    &super::ProducerRetryDiagnostics::default(),
    &clock,
    &mut retry_backoff,
  )
  .await;

  assert!(matches!(result, Err(ProducerError::RetriesExhausted(_))));
  assert_eq!(transport.sent.lock().await.len(), 1);
  assert_eq!(*clock.sleeps.lock(), vec![Duration::from_millis(10)]);
}
