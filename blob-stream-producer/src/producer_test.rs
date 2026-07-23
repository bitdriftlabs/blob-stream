#![allow(clippy::unwrap_used)]

use super::diagnostics::ProducerBrokerSnapshot;
use super::retry::{
  RetryClock,
  next_retry_delay,
  producer_retry_backoff,
  send_batch_with_retry,
  wait_for_not_lease_holder_retry,
};
use super::routing::{
  ProducerRoutes,
  broker_assignment,
  group_batches_by_broker,
  record_fits_grouped_request,
};
use super::state::BufferedBatch;
use super::{
  BrokerTransport,
  GrpcBrokerTransport,
  ProducerClient,
  ProducerClientImpl,
  ProducerError,
  ProducerRecord,
  ProducerRetryDiagnostics,
  ProducerRetryReason,
  compute_virtual_partition_id,
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
  ProduceBatchesRequest,
  ProduceBatchesResponse,
  ProduceStatus,
  Record,
};
use blob_stream_types::{MAX_PRODUCE_BATCHES_REQUEST_BYTES, VirtualPartitionId};
use bytes::Bytes;
use protobuf::Chars;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};
use tokio::time::{Instant, timeout};

struct TestBrokerDiscovery {
  membership_rx: watch::Receiver<BrokerMembership>,
}

#[test]
fn producer_record_retains_payload_allocation() {
  let payload = vec![1, 2, 3];
  let payload_pointer = payload.as_ptr();
  let record = ProducerRecord::new("telemetry".into(), Vec::new(), payload.into(), 100);

  assert_eq!(record.payload.as_ptr(), payload_pointer);

  let payload = Bytes::from_static(b"payload");
  let payload_pointer = payload.as_ptr();
  let record = ProducerRecord::new("telemetry".into(), Vec::new(), payload, 100);

  assert_eq!(record.payload.as_ptr(), payload_pointer);
}

impl TestBrokerDiscovery {
  fn new(membership: BrokerMembership) -> Self {
    let (tx, membership_rx) = watch::channel(membership);
    drop(tx);
    Self { membership_rx }
  }

  fn with_updates(membership: BrokerMembership) -> (Self, watch::Sender<BrokerMembership>) {
    let (tx, membership_rx) = watch::channel(membership);
    (Self { membership_rx }, tx)
  }
}

#[async_trait]
impl blob_stream_broker_discovery::BrokerDiscovery for TestBrokerDiscovery {
  async fn watch_membership(&self) -> anyhow::Result<watch::Receiver<BrokerMembership>> {
    Ok(self.membership_rx.clone())
  }
}

#[derive(Clone, Debug, PartialEq)]
struct SentBatch {
  broker_address: Chars,
  batches: Vec<ProduceBatchRequest>,
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
  async fn produce_batches(
    &self,
    broker_address: &Chars,
    request: ProduceBatchesRequest,
    request_timeout: Duration,
  ) -> anyhow::Result<ProduceBatchesResponse> {
    assert!(!request_timeout.is_zero());
    let batch_count = request.batches.len();
    self.sent.lock().await.push(SentBatch {
      broker_address: broker_address.clone(),
      batches: request.batches,
    });

    self.responses.lock().await.pop_front().map_or_else(
      || {
        Ok(ProduceBatchesResponse {
          results: (0 .. batch_count)
            .map(|_| ProduceBatchResponse {
              status: ProduceStatus::PRODUCE_STATUS_OK.into(),
              ..Default::default()
            })
            .collect(),
          ..Default::default()
        })
      },
      |response| {
        response.map(|result| ProduceBatchesResponse {
          results: vec![result],
          ..Default::default()
        })
      },
    )
  }
}

struct MembershipUpdateTransport {
  sent: Mutex<Vec<SentBatch>>,
  membership_tx: watch::Sender<BrokerMembership>,
  updated_membership: BrokerMembership,
}

#[async_trait]
impl super::BrokerTransport for MembershipUpdateTransport {
  async fn produce_batches(
    &self,
    broker_address: &Chars,
    request: ProduceBatchesRequest,
    request_timeout: Duration,
  ) -> anyhow::Result<ProduceBatchesResponse> {
    assert!(!request_timeout.is_zero());
    let attempt = {
      let mut sent = self.sent.lock().await;
      sent.push(SentBatch {
        broker_address: broker_address.clone(),
        batches: request.batches,
      });
      sent.len()
    };

    if attempt == 1 {
      self
        .membership_tx
        .send(self.updated_membership.clone())
        .map_err(|_| anyhow!("producer membership receiver dropped"))?;
      return Ok(ProduceBatchesResponse {
        results: vec![ProduceBatchResponse {
          status: ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER.into(),
          error_message: "lease moved".into(),
          ..Default::default()
        }],
        ..Default::default()
      });
    }

    Ok(ProduceBatchesResponse {
      results: vec![ProduceBatchResponse {
        status: ProduceStatus::PRODUCE_STATUS_OK.into(),
        ..Default::default()
      }],
      ..Default::default()
    })
  }
}

struct GatedBrokerTransport {
  entered_tx: mpsc::UnboundedSender<VirtualPartitionId>,
  release: Arc<Semaphore>,
}

struct GroupedRetryGateTransport {
  retry_entered_tx: mpsc::UnboundedSender<VirtualPartitionId>,
  release: Arc<Semaphore>,
}

#[async_trait]
impl super::BrokerTransport for GroupedRetryGateTransport {
  async fn produce_batches(
    &self,
    _broker_address: &Chars,
    request: ProduceBatchesRequest,
    request_timeout: Duration,
  ) -> anyhow::Result<ProduceBatchesResponse> {
    assert!(!request_timeout.is_zero());
    if request.batches.len() > 1 {
      return Ok(ProduceBatchesResponse {
        results: request
          .batches
          .iter()
          .map(|_| ProduceBatchResponse {
            status: ProduceStatus::PRODUCE_STATUS_OVERLOADED.into(),
            error_message: "busy".into(),
            ..Default::default()
          })
          .collect(),
        ..Default::default()
      });
    }

    let batch = request
      .batches
      .first()
      .expect("retry request must contain one batch");
    self
      .retry_entered_tx
      .send(batch.virtual_partition_id)
      .map_err(|_| anyhow!("test receiver dropped"))?;
    self
      .release
      .acquire()
      .await
      .map_err(|_| anyhow!("test gate closed"))?
      .forget();
    Ok(ProduceBatchesResponse {
      results: vec![ProduceBatchResponse {
        status: ProduceStatus::PRODUCE_STATUS_OK.into(),
        ..Default::default()
      }],
      ..Default::default()
    })
  }
}

#[async_trait]
impl super::BrokerTransport for GatedBrokerTransport {
  async fn produce_batches(
    &self,
    _broker_address: &Chars,
    request: ProduceBatchesRequest,
    request_timeout: Duration,
  ) -> anyhow::Result<ProduceBatchesResponse> {
    assert!(!request_timeout.is_zero());
    for batch in &request.batches {
      self
        .entered_tx
        .send(batch.virtual_partition_id)
        .map_err(|_| anyhow!("test receiver dropped"))?;
    }
    self
      .release
      .acquire()
      .await
      .map_err(|_| anyhow!("test gate closed"))?
      .forget();
    Ok(ProduceBatchesResponse {
      results: request
        .batches
        .iter()
        .map(|_| ProduceBatchResponse {
          status: ProduceStatus::PRODUCE_STATUS_OK.into(),
          ..Default::default()
        })
        .collect(),
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

  let first = transport
    .client_for_address(&"127.0.0.1:8080".into())
    .unwrap();
  let second = transport
    .client_for_address(&"127.0.0.1:8080".into())
    .unwrap();
  let other = transport
    .client_for_address(&"127.0.0.1:8081".into())
    .unwrap();

  assert!(Arc::ptr_eq(&first, &second));
  assert!(!Arc::ptr_eq(&first, &other));
}

#[test]
fn grpc_transport_trait_dispatch_evicts_clients_for_removed_brokers() {
  let concrete_transport = Arc::new(GrpcBrokerTransport::new(default_config()));
  let removed = concrete_transport
    .client_for_address(&"a:8080".into())
    .unwrap();
  let retained = concrete_transport
    .client_for_address(&"b:8080".into())
    .unwrap();
  let transport: Arc<dyn BrokerTransport> = concrete_transport.clone();

  transport.reconcile_membership(&BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "b:8080".into(),
  }]));

  let retained_after_reconcile = concrete_transport
    .client_for_address(&"b:8080".into())
    .unwrap();
  let recreated = concrete_transport
    .client_for_address(&"a:8080".into())
    .unwrap();

  assert!(Arc::ptr_eq(&retained, &retained_after_reconcile));
  assert!(!Arc::ptr_eq(&removed, &recreated));
}

fn membership() -> BrokerMembership {
  BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".into(),
      address: "a:8080".into(),
    },
    BrokerNode {
      node_id: "node-b".into(),
      address: "b:8080".into(),
    },
  ])
}

fn single_broker_membership() -> BrokerMembership {
  BrokerMembership::new(vec![BrokerNode {
    node_id: "node-a".into(),
    address: "a:8080".into(),
  }])
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
          .produce(ProducerRecord::new(
            "telemetry".into(),
            key,
            vec![1].into(),
            100,
          ))
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
    ProducerClientImpl::new(
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
        "telemetry".into(),
        b"diagnostics-key".to_vec(),
        vec![1, 2, 3].into(),
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
      ProducerBrokerSnapshot {
        node_id: "node-a".into(),
        address: "a:8080".into(),
      },
      ProducerBrokerSnapshot {
        node_id: "node-b".into(),
        address: "b:8080".into(),
      },
    ]
  );
  assert_eq!(snapshot.topics.len(), 1);
  assert_eq!(snapshot.topics[0].name.as_str(), "telemetry");
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
  assert_eq!(snapshot.partition_buffers[0].topic.as_str(), "telemetry");
  assert_eq!(snapshot.partition_buffers[0].buffered_record_count, 1);
  assert_eq!(snapshot.partition_buffers[0].pending_ack_count, 1);
  assert_eq!(snapshot.partition_buffers[0].buffered_bytes, 3);

  pending_produce.abort();
  let _ignored = pending_produce.await;
}

#[tokio::test]
async fn routes_to_expected_broker() {
  let config = default_config();
  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  let topic = topic_config();
  let producer = ProducerClientImpl::new(
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
      "telemetry".into(),
      key.clone(),
      vec![1, 2, 3].into(),
      100,
    ))
    .await
    .unwrap();

  let expected_partition = compute_virtual_partition_id(&key, 16, producer_writer_id(&config));
  assert_eq!(ack.virtual_partition_id, expected_partition);
  let discovered_membership = membership();
  let expected_assignment = broker_assignment(
    &HashMap::from([("telemetry".into(), topic)]),
    producer_writer_id(&config),
    &discovered_membership,
  );
  let expected_owner = expected_assignment
    .get(&BrokerPartition {
      topic: "telemetry".into(),
      virtual_partition_id: expected_partition,
    })
    .unwrap();

  let sent = transport.sent.lock().await;
  assert_eq!(sent.len(), 1);
  assert_eq!(sent[0].broker_address, expected_owner.address);
  assert_eq!(sent[0].batches[0].virtual_partition_id, expected_partition);
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

  let producer = ProducerClientImpl::new(
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
      "telemetry".into(),
      b"retry-key".to_vec(),
      vec![7].into(),
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

#[test]
fn retry_diagnostics_retains_most_recent_samples() {
  let diagnostics = ProducerRetryDiagnostics::default();
  let topic: Chars = "telemetry".into();

  for attempt in 1 ..= 21 {
    diagnostics.record(
      ProducerRetryReason::Overloaded,
      &topic,
      0,
      attempt,
      format!("retry-{attempt}"),
    );
  }

  let summary = diagnostics.summary();
  assert_eq!(
    summary.reason_counts,
    BTreeMap::from([(ProducerRetryReason::Overloaded, 21)])
  );
  assert_eq!(summary.samples.len(), 20);
  assert_eq!(summary.samples.front().unwrap().attempt, 2);
  assert_eq!(summary.samples.front().unwrap().detail, "retry-2");
  assert_eq!(summary.samples.back().unwrap().attempt, 21);
  assert_eq!(summary.samples.back().unwrap().detail, "retry-21");
}

#[tokio::test]
async fn not_lease_holder_waits_longer_when_membership_is_unchanged() {
  let config = default_config();
  let transport = FakeBrokerTransport::default();
  transport
    .enqueue_response(Ok(ProduceBatchResponse {
      status: ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER.into(),
      ..Default::default()
    }))
    .await;
  let clock = FixedRetryClock::new(Instant::now());
  let membership = watch::channel(membership()).1;
  let topics = HashMap::from([("telemetry".into(), topic_config())]);
  let routes = ProducerRoutes::new(&config, &topics, &membership.borrow());
  let dispatch_permits = Arc::new(Semaphore::new(1));
  let batch = BufferedBatch {
    topic: "telemetry".into(),
    virtual_partition_id: 16,
    records: Vec::new(),
    waiters: Vec::new(),
  };

  let result = send_batch_with_retry(
    &config,
    &topics,
    &routes,
    &membership,
    &transport,
    &batch,
    &dispatch_permits,
    &super::ProducerMetrics::new(&metrics_scope()),
    &super::ProducerRetryDiagnostics::default(),
    None,
    0,
    &clock,
    clock.now(),
  )
  .await;

  assert!(result.is_ok());
  assert_eq!(transport.sent.lock().await.len(), 2);
  let sleeps = clock.sleeps.lock();
  assert_eq!(sleeps.len(), 1);
  assert!((Duration::from_millis(125) ..= Duration::from_millis(375)).contains(&sleeps[0]));
}

#[tokio::test]
async fn not_lease_holder_membership_update_refreshes_cached_route_for_retry() {
  let (discovery, membership_tx) = TestBrokerDiscovery::with_updates(membership());
  let updated_membership = BrokerMembership::new(vec![BrokerNode {
    node_id: "node-c".into(),
    address: "c:8080".into(),
  }]);
  let transport = Arc::new(MembershipUpdateTransport {
    sent: Mutex::new(Vec::new()),
    membership_tx,
    updated_membership,
  });
  let producer = ProducerClientImpl::new(
    default_config(),
    vec![topic_config()],
    Arc::new(discovery),
    transport.clone(),
    metrics_scope(),
  )
  .await
  .unwrap();

  let ack = producer
    .produce(ProducerRecord::new(
      "telemetry".into(),
      b"membership-update-key".to_vec(),
      vec![7].into(),
      100,
    ))
    .await
    .unwrap();

  assert_eq!(ack.attempts, 2);
  let sent = transport.sent.lock().await;
  assert_eq!(sent.len(), 2);
  assert_ne!(sent[0].broker_address, sent[1].broker_address);
  assert_eq!(sent[1].broker_address.as_str(), "c:8080");
}

#[tokio::test]
async fn membership_update_refreshes_routes_without_flushing_partial_batches() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);

  let (discovery, membership_tx) = TestBrokerDiscovery::with_updates(membership());
  let transport = Arc::new(FakeBrokerTransport::default());
  let producer = Arc::new(
    ProducerClientImpl::new(
      config,
      vec![topic_config()],
      Arc::new(discovery),
      transport.clone(),
      metrics_scope(),
    )
    .await
    .unwrap(),
  );
  let key = b"membership-refresh-without-flush".to_vec();
  let virtual_partition_id = compute_virtual_partition_id(&key, 16, 1);

  let first_producer = Arc::clone(&producer);
  let first_key = key.clone();
  let first = tokio::spawn(async move {
    first_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        first_key,
        vec![1].into(),
        100,
      ))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 1).await;

  membership_tx
    .send(BrokerMembership::new(vec![BrokerNode {
      node_id: "node-c".into(),
      address: "c:8080".into(),
    }]))
    .unwrap();
  timeout(Duration::from_secs(1), async {
    loop {
      let snapshot = producer
        .diagnostics()
        .expect("producer implementation provides diagnostics")
        .state_snapshot();
      let selected_broker = snapshot
        .route_map
        .iter()
        .find(|route| route.virtual_partition_id == virtual_partition_id)
        .and_then(|route| route.selected_broker.as_ref());
      if selected_broker.is_some_and(|broker| broker.address.as_str() == "c:8080") {
        return;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .expect("membership update should refresh cached routes");

  assert!(transport.sent.lock().await.is_empty());

  let second_producer = Arc::clone(&producer);
  let second = tokio::spawn(async move {
    second_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        key,
        vec![2].into(),
        101,
      ))
      .await
  });

  first.await.unwrap().unwrap();
  second.await.unwrap().unwrap();
  let sent = transport.sent.lock().await;
  assert_eq!(sent.len(), 1);
  assert_eq!(sent[0].broker_address.as_str(), "c:8080");
  assert_eq!(sent[0].batches[0].records.len(), 2);
}

#[tokio::test]
async fn membership_settle_delay_is_clipped_to_retry_deadline() {
  let (membership_tx, mut membership_rx) = watch::channel(membership());
  membership_tx
    .send(BrokerMembership::new(vec![BrokerNode {
      node_id: "node-c".into(),
      address: "c:8080".into(),
    }]))
    .unwrap();
  let clock = FixedRetryClock::new(Instant::now());
  let retry_deadline = clock.now() + Duration::from_millis(50);

  assert!(
    wait_for_not_lease_holder_retry(
      &mut membership_rx,
      &clock,
      Duration::from_millis(250),
      retry_deadline,
    )
    .await
  );
  assert_eq!(*clock.sleeps.lock(), vec![Duration::from_millis(50)]);
}

#[tokio::test]
async fn not_lease_holder_refresh_uses_the_latest_membership_snapshot() {
  let config = default_config();
  let topics = HashMap::from([("telemetry".into(), topic_config())]);
  let (membership_tx, membership_rx) = watch::channel(membership());
  let mut membership_updates = membership_rx.clone();
  let first_update = BrokerMembership::new(vec![BrokerNode {
    node_id: "node-b".into(),
    address: "b:8080".into(),
  }]);
  let latest_update = BrokerMembership::new(vec![BrokerNode {
    node_id: "node-c".into(),
    address: "c:8080".into(),
  }]);
  membership_tx.send(first_update).unwrap();
  let clock = MembershipUpdateRetryClock::new(membership_tx, latest_update);
  let routes = ProducerRoutes::new(&config, &topics, &membership_rx.borrow());

  assert!(
    wait_for_not_lease_holder_retry(
      &mut membership_updates,
      &clock,
      Duration::from_millis(250),
      clock.now() + Duration::from_secs(1),
    )
    .await
  );
  routes.refresh(&config, &topics, &membership_rx.borrow());

  let broker = routes
    .owner(&BrokerPartition {
      topic: "telemetry".into(),
      virtual_partition_id: 16,
    })
    .expect("latest membership assigns the partition");
  assert_eq!(broker.address.as_str(), "c:8080");
}

#[tokio::test]
async fn batches_by_partition_and_acks_waiters() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(10_000);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  let producer = Arc::new(
    ProducerClientImpl::new(
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
          "telemetry".into(),
          b"same-key".to_vec(),
          vec![1].into(),
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
          "telemetry".into(),
          b"same-key".to_vec(),
          vec![2].into(),
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
  assert_eq!(sent[0].batches[0].records.len(), 2);
}

#[test]
fn splits_batches_across_the_grouped_request_limit() {
  let config = default_config();
  let topics = HashMap::from([("telemetry".into(), topic_config())]);
  let routes = ProducerRoutes::new(&config, &topics, &single_broker_membership());
  let record = Record {
    payload: vec![0; MAX_PRODUCE_BATCHES_REQUEST_BYTES / 2].into(),
    ..Default::default()
  };
  assert!(record_fits_grouped_request(
    &"telemetry".into(),
    16,
    &record
  ));
  let (first_waiter, _first_result) = oneshot::channel();
  let (second_waiter, _second_result) = oneshot::channel();

  let grouped = group_batches_by_broker(
    &routes,
    vec![BufferedBatch {
      topic: "telemetry".into(),
      virtual_partition_id: 16,
      records: vec![record.clone(), record],
      waiters: vec![first_waiter, second_waiter],
    }],
  );

  assert!(grouped.unassigned.is_empty());
  assert_eq!(grouped.groups.len(), 2);
  assert!(
    grouped
      .groups
      .iter()
      .all(|group| group.batches.len() == 1 && group.batches[0].records.len() == 1)
  );
}

#[tokio::test]
async fn rejects_records_larger_than_the_grouped_request_limit() {
  let producer = ProducerClientImpl::new(
    default_config(),
    vec![topic_config()],
    Arc::new(TestBrokerDiscovery::new(single_broker_membership())),
    Arc::new(FakeBrokerTransport::default()),
    metrics_scope(),
  )
  .await
  .unwrap();

  let error = producer
    .produce(ProducerRecord::new(
      "telemetry".into(),
      b"oversized".to_vec(),
      vec![0; MAX_PRODUCE_BATCHES_REQUEST_BYTES].into(),
      100,
    ))
    .await
    .unwrap_err();

  assert!(matches!(error, ProducerError::Rejected(_)));
}

#[tokio::test]
async fn dropped_size_triggering_produce_does_not_cancel_batch_dispatch() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::clone(&release),
  });
  let producer = Arc::new(
    ProducerClientImpl::new(
      config,
      vec![topic_config()],
      discovery,
      transport,
      metrics_scope(),
    )
    .await
    .unwrap(),
  );

  let first_producer = Arc::clone(&producer);
  let first = tokio::spawn(async move {
    first_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        b"same-key".to_vec(),
        vec![1].into(),
        100,
      ))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 1).await;

  let second_producer = Arc::clone(&producer);
  let second = tokio::spawn(async move {
    second_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        b"same-key".to_vec(),
        vec![2].into(),
        101,
      ))
      .await
  });
  entered_rx
    .recv()
    .await
    .expect("size-triggered batch entered transport");

  second.abort();
  let _ignored = second.await;
  release.add_permits(1);

  timeout(Duration::from_secs(1), first)
    .await
    .expect("remaining waiter should be notified after the triggering caller drops")
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn flush_groups_distinct_partition_batches_for_one_broker() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);

  let discovery = Arc::new(TestBrokerDiscovery::new(BrokerMembership::new(vec![
    BrokerNode {
      node_id: "node-a".into(),
      address: "a:8080".into(),
    },
  ])));
  let transport = Arc::new(FakeBrokerTransport::default());
  let producer = Arc::new(
    ProducerClientImpl::new(
      config,
      vec![topic_config()],
      discovery,
      transport.clone(),
      metrics_scope(),
    )
    .await
    .unwrap(),
  );
  let pending_produces = enqueue_distinct_partition_batches(&producer);
  wait_for_buffered_partitions(producer.as_ref(), 2).await;

  producer.flush().await.unwrap();
  for pending_produce in pending_produces {
    pending_produce.await.unwrap().unwrap();
  }

  let sent = transport.sent.lock().await;
  assert_eq!(sent.len(), 1);
  assert_eq!(sent[0].batches.len(), 2);
}

#[tokio::test]
async fn surfaces_retry_exhaustion_for_transport_errors() {
  let mut config = default_config();
  config.retry_deadline_ms = Some(10);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  for _ in 0 .. 16 {
    transport
      .enqueue_response(Err(anyhow!("network down")))
      .await;
  }

  let producer = ProducerClientImpl::new(
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
      "telemetry".into(),
      b"retry-fail".to_vec(),
      vec![1].into(),
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
  let producer = ProducerClientImpl::new(
    config,
    vec![topic_config()],
    discovery,
    transport,
    metrics_scope(),
  )
  .await
  .unwrap();

  let produce = producer.produce(ProducerRecord::new(
    "telemetry".into(),
    b"deadline".to_vec(),
    vec![1].into(),
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
  config.retry_deadline_ms = Some(10);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  for _ in 0 .. 16 {
    transport
      .enqueue_response(Err(anyhow!("network down")))
      .await;
  }
  let producer = Arc::new(
    ProducerClientImpl::new(
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
        "telemetry".into(),
        b"flush-error-key".to_vec(),
        vec![1].into(),
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
async fn flush_returns_no_brokers_after_notifying_waiters() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);

  let discovery = Arc::new(TestBrokerDiscovery::new(BrokerMembership::new(vec![])));
  let producer = Arc::new(
    ProducerClientImpl::new(
      config,
      vec![topic_config()],
      discovery,
      Arc::new(FakeBrokerTransport::default()),
      metrics_scope(),
    )
    .await
    .unwrap(),
  );

  let pending_producer = Arc::clone(&producer);
  let pending_produce = tokio::spawn(async move {
    pending_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        b"flush-no-brokers-key".to_vec(),
        vec![1].into(),
        0,
      ))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 1).await;

  assert!(matches!(
    producer.flush().await,
    Err(ProducerError::NoBrokersAvailable)
  ));
  assert!(matches!(
    pending_produce.await.unwrap(),
    Err(ProducerError::NoBrokersAvailable)
  ));
}

#[tokio::test]
async fn flush_dispatches_distinct_partition_batches_concurrently() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);
  config.max_request_concurrency = Some(2);

  let discovery = Arc::new(TestBrokerDiscovery::new(membership()));
  let collector = Collector::default();
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::clone(&release),
  });
  let producer = Arc::new(
    ProducerClientImpl::new(
      config,
      vec![topic_config()],
      discovery,
      transport,
      collector.scope("blob_stream_producer_test"),
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
  let metrics = String::from_utf8(collector.prometheus_output()).unwrap();
  assert!(metrics.contains("blob_stream_producer_test:producer:active_requests 2"));

  release.add_permits(2);
  flush.await.unwrap().unwrap();
  let metrics = String::from_utf8(collector.prometheus_output()).unwrap();
  assert!(metrics.contains("blob_stream_producer_test:producer:active_requests 0"));
  for pending_produce in pending_produces {
    pending_produce.await.unwrap().unwrap();
  }
}

#[tokio::test]
async fn flush_retries_grouped_batches_concurrently() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);

  let discovery = Arc::new(TestBrokerDiscovery::new(single_broker_membership()));
  let (retry_entered_tx, mut retry_entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GroupedRetryGateTransport {
    retry_entered_tx,
    release: Arc::clone(&release),
  });
  let producer = Arc::new(
    ProducerClientImpl::new(
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
  let first_retry = timeout(Duration::from_secs(1), retry_entered_rx.recv())
    .await
    .expect("first retry should enter transport")
    .expect("transport should remain available");
  let second_retry = timeout(Duration::from_secs(1), retry_entered_rx.recv())
    .await
    .expect("second retry should enter before the first retry completes")
    .expect("transport should remain available");
  assert_ne!(first_retry, second_retry);

  release.add_permits(2);
  flush.await.unwrap().unwrap();
  for pending_produce in pending_produces {
    pending_produce.await.unwrap().unwrap();
  }
}

#[tokio::test]
async fn retries_respect_the_shared_concurrency_limit() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);
  config.max_request_concurrency = Some(1);

  let discovery = Arc::new(TestBrokerDiscovery::new(single_broker_membership()));
  let (retry_entered_tx, mut retry_entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GroupedRetryGateTransport {
    retry_entered_tx,
    release: Arc::clone(&release),
  });
  let producer = Arc::new(
    ProducerClientImpl::new(
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
  let first_retry = timeout(Duration::from_secs(1), retry_entered_rx.recv())
    .await
    .expect("first retry should enter transport")
    .expect("transport should remain available");
  assert!(
    timeout(Duration::from_millis(100), retry_entered_rx.recv())
      .await
      .is_err(),
    "second retry must wait for the shared request permit"
  );

  release.add_permits(1);
  let second_retry = timeout(Duration::from_secs(1), retry_entered_rx.recv())
    .await
    .expect("second retry should enter after the first completes")
    .expect("transport should remain available");
  assert_ne!(first_retry, second_retry);
  release.add_permits(1);

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
    ProducerClientImpl::new(
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
    ProducerClientImpl::new(
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
      .produce(ProducerRecord::new(
        "telemetry".into(),
        first_key,
        vec![1].into(),
        100,
      ))
      .await
  });
  let first_partition = entered_rx
    .recv()
    .await
    .expect("first batch entered transport");

  let second_producer = Arc::clone(&producer);
  let second = tokio::spawn(async move {
    second_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        second_key,
        vec![2].into(),
        101,
      ))
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
async fn size_triggered_flush_packs_all_buffered_partitions() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);

  let discovery = Arc::new(TestBrokerDiscovery::new(single_broker_membership()));
  let transport = Arc::new(FakeBrokerTransport::default());
  let producer = Arc::new(
    ProducerClientImpl::new(
      config,
      vec![topic_config()],
      discovery,
      transport.clone(),
      metrics_scope(),
    )
    .await
    .unwrap(),
  );
  let (first_key, second_key) = keys_for_distinct_partitions();

  let hot_producer = Arc::clone(&producer);
  let initial_hot_key = first_key.clone();
  let first_hot = tokio::spawn(async move {
    hot_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        initial_hot_key,
        vec![1].into(),
        100,
      ))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 1).await;

  let peer_producer = Arc::clone(&producer);
  let peer = tokio::spawn(async move {
    peer_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        second_key,
        vec![2].into(),
        101,
      ))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 2).await;

  let hot_producer = Arc::clone(&producer);
  let second_hot = tokio::spawn(async move {
    hot_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        first_key,
        vec![3].into(),
        102,
      ))
      .await
  });

  first_hot.await.unwrap().unwrap();
  peer.await.unwrap().unwrap();
  second_hot.await.unwrap().unwrap();

  let sent = transport.sent.lock().await;
  assert_eq!(sent.len(), 1);
  assert_eq!(sent[0].batches.len(), 2);
}

#[tokio::test]
async fn dispatch_completion_does_not_flush_a_later_partial_batch() {
  let mut config = default_config();
  config.max_batch_records = Some(2);
  config.flush_max_delay_ms = Some(60_000);

  let discovery = Arc::new(TestBrokerDiscovery::new(single_broker_membership()));
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let transport = Arc::new(GatedBrokerTransport {
    entered_tx,
    release: Arc::clone(&release),
  });
  let producer = Arc::new(
    ProducerClientImpl::new(
      config,
      vec![topic_config()],
      discovery,
      transport,
      metrics_scope(),
    )
    .await
    .unwrap(),
  );
  let key = b"completion-does-not-flush".to_vec();

  let first_producer = Arc::clone(&producer);
  let first_key = key.clone();
  let first = tokio::spawn(async move {
    first_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        first_key,
        vec![1].into(),
        100,
      ))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 1).await;

  let second_producer = Arc::clone(&producer);
  let second = tokio::spawn(async move {
    second_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        key,
        vec![2].into(),
        101,
      ))
      .await
  });
  entered_rx
    .recv()
    .await
    .expect("full batch entered transport");

  let later_producer = Arc::clone(&producer);
  let later = tokio::spawn(async move {
    later_producer
      .produce(ProducerRecord::new(
        "telemetry".into(),
        b"later-partial-batch".to_vec(),
        vec![3].into(),
        102,
      ))
      .await
  });
  wait_for_buffered_partitions(producer.as_ref(), 1).await;

  release.add_permits(1);
  assert!(
    timeout(Duration::from_millis(100), entered_rx.recv())
      .await
      .is_err(),
    "dispatch completion must not flush a partial later batch"
  );

  let flush_producer = Arc::clone(&producer);
  let flush = tokio::spawn(async move { flush_producer.flush().await });
  entered_rx
    .recv()
    .await
    .expect("explicit flush entered transport");
  release.add_permits(1);
  flush.await.unwrap().unwrap();
  first.await.unwrap().unwrap();
  second.await.unwrap().unwrap();
  later.await.unwrap().unwrap();
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
    ProducerClientImpl::new(
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

struct MembershipUpdateRetryClock {
  now: Instant,
  update: parking_lot::Mutex<Option<(watch::Sender<BrokerMembership>, BrokerMembership)>>,
}

impl MembershipUpdateRetryClock {
  fn new(membership_tx: watch::Sender<BrokerMembership>, membership: BrokerMembership) -> Self {
    Self {
      now: Instant::now(),
      update: parking_lot::Mutex::new(Some((membership_tx, membership))),
    }
  }
}

#[async_trait]
impl RetryClock for MembershipUpdateRetryClock {
  fn now(&self) -> Instant {
    self.now
  }

  async fn sleep(&self, _duration: Duration) {
    if let Some((membership_tx, membership)) = self.update.lock().take() {
      membership_tx
        .send(membership)
        .expect("membership receiver remains open");
    }
  }
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
  config.retry_base_delay_ms = Some(100);
  config.retry_max_delay_ms = Some(100);
  config.retry_deadline_ms = Some(10);

  let transport = FakeBrokerTransport::default();
  transport
    .enqueue_response(Err(anyhow!("network down")))
    .await;
  let clock = FixedRetryClock::new(Instant::now());
  let membership = watch::channel(membership()).1;
  let topics = HashMap::from([("telemetry".into(), topic_config())]);
  let routes = ProducerRoutes::new(&config, &topics, &membership.borrow());
  let dispatch_permits = Arc::new(Semaphore::new(1));
  let batch = BufferedBatch {
    topic: "telemetry".into(),
    virtual_partition_id: 16,
    records: Vec::new(),
    waiters: Vec::new(),
  };

  let result = send_batch_with_retry(
    &config,
    &topics,
    &routes,
    &membership,
    &transport,
    &batch,
    &dispatch_permits,
    &super::ProducerMetrics::new(&metrics_scope()),
    &super::ProducerRetryDiagnostics::default(),
    None,
    0,
    &clock,
    clock.now(),
  )
  .await;

  assert!(matches!(result, Err(ProducerError::RetriesExhausted(_))));
  assert_eq!(transport.sent.lock().await.len(), 1);
  assert_eq!(*clock.sleeps.lock(), vec![Duration::from_millis(10)]);
}
