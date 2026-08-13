use anyhow::Result;
use bd_server_stats::stats::Collector;
use bd_time::TimeProvider;
use blob_stream_broker::write::BrokerLeaseStatus;
use blob_stream_consumer::consumer::ConsumerReaderImpl;
use blob_stream_consumer::iterator::{ConsumerIterator, ConsumerIteratorImpl, NextResult};
use blob_stream_consumer::{ConsumerReadConfig, DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS};
use blob_stream_integration_tests::test_framework as framework;
use blob_stream_metadata_store::ConsumerGroupMember;
use blob_stream_producer::{ProducerClient, ProducerClientImpl, ProducerError, ProducerRecord};
use blob_stream_types::{logical_partition_for_key, virtual_partition_for_logical};
use framework::{
  ClusterHarness,
  IntegrationResources,
  ManualProducerRetryClock,
  ManualTimeProvider,
  NetworkFault,
  NetworkFaultRule,
  NetworkOperation,
  PARTITION_COUNT,
  StoreFaultAction,
  StoreFaultDomain,
  StoreFaultOperation,
  StoreFaultRule,
  TOPIC,
  TestConsumerReader,
  TestEventMatcher,
  WINDOW_SIZE_SECONDS,
  append_reader_delivery_traces,
  consumer_runtime_config,
  drain_reader_until_with_trace,
  produce_message,
  producer_config,
  producer_topic,
  producer_topic_named_with_partition_count,
  reader_delivery_counts,
  rescan_reader_with_trace,
};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::time::{Instant, timeout};

fn member_ids(members: &[ConsumerGroupMember]) -> Vec<String> {
  members
    .iter()
    .map(|member| member.member_id.clone())
    .collect()
}

fn metrics_scope(component: &str) -> bd_server_stats::stats::Scope {
  Collector::default().scope(component)
}

async fn complete_produce_with_manual_retries(
  produce: impl Future<Output = Result<blob_stream_producer::ProducerAck, ProducerError>>,
  retry_clock: &ManualProducerRetryClock,
  timeout_message: &str,
) -> Result<blob_stream_producer::ProducerAck> {
  tokio::pin!(produce);
  let ack = timeout(Duration::from_secs(5), async {
    loop {
      tokio::select! {
        result = &mut produce => return result,
        () = retry_clock.wait_until_sleeping() => {
          assert!(
            retry_clock.advance_to_next_sleep(),
            "producer retry backoff was not registered"
          );
          tokio::task::yield_now().await;
        }
      }
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("{timeout_message}"))??;
  Ok(ack)
}

async fn wait_for_producer_route(
  producer: &ProducerClientImpl,
  node_id: &str,
  address: &str,
  deadline: Instant,
) -> Result<()> {
  loop {
    let snapshot = producer
      .diagnostics()
      .expect("producer diagnostics must be available")
      .state_snapshot();
    let routes_converged = !snapshot.route_map.is_empty()
      && snapshot.route_map.iter().all(|route| {
        route.selected_broker.as_ref().is_some_and(|broker| {
          broker.node_id.as_str() == node_id && broker.address.as_str() == address
        })
      });
    if routes_converged {
      return Ok(());
    }
    if Instant::now() >= deadline {
      return Err(anyhow::anyhow!(
        "producer route did not converge to {node_id} at {address}: {snapshot:#?}"
      ));
    }
    tokio::task::yield_now().await;
  }
}

async fn wait_for_broker_lease_ownership(
  cluster: &ClusterHarness,
  node_id: &str,
  topic: &str,
  virtual_partition_ids: &[u32],
  deadline: Instant,
) -> Result<()> {
  loop {
    let snapshots = cluster.broker_state_snapshots().await;
    let broker_owns_partitions = snapshots
      .iter()
      .find(|snapshot| snapshot.holder_id == node_id)
      .is_some_and(|snapshot| {
        virtual_partition_ids.iter().all(|virtual_partition_id| {
          snapshot.ownership.iter().any(|ownership| {
            ownership.topic.as_str() == topic
              && ownership.virtual_partition_id == *virtual_partition_id
              && ownership.assignment_is_local
              && ownership.lease_status == BrokerLeaseStatus::LocalActive
          })
        })
      });
    if broker_owns_partitions {
      return Ok(());
    }
    if Instant::now() >= deadline {
      return Err(anyhow::anyhow!(
        "broker {node_id} did not acquire producer leases for {topic} partitions \
         {virtual_partition_ids:?}: {snapshots:#?}"
      ));
    }
    tokio::task::yield_now().await;
  }
}

async fn wait_for_group_offsets_committed(
  cluster: &ClusterHarness,
  group_id: &str,
  maximum_offsets: &HashMap<u32, u64>,
  boundary: &str,
) -> Result<()> {
  timeout(Duration::from_secs(5), async {
    loop {
      let leases = cluster
        .consumer_lease_store()
        .list_group_leases(TOPIC, group_id)
        .await?;
      if maximum_offsets
        .iter()
        .all(|(partition_id, maximum_offset)| {
          leases
            .iter()
            .find(|lease| lease.key.virtual_partition_id == *partition_id)
            .is_some_and(|lease| {
              lease.committed_cursor.as_ref().is_some_and(|cursor| {
                cursor.seq_end >= *maximum_offset && cursor.source_checkpoint.is_some()
              })
            })
        })
      {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("{boundary}"))??;
  Ok(())
}

async fn wait_for_member_to_own_group_leases(
  cluster: &ClusterHarness,
  group_id: &str,
  member_id: &str,
  expected_lease_count: usize,
  boundary: &str,
) -> Result<()> {
  timeout(Duration::from_secs(5), async {
    loop {
      let leases = cluster
        .consumer_lease_store()
        .list_group_leases(TOPIC, group_id)
        .await?;
      if leases.len() == expected_lease_count
        && leases.iter().all(|lease| lease.owner_id == member_id)
      {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("{boundary}"))??;
  Ok(())
}

async fn start_consumer_after_rebalance(
  cluster: &ClusterHarness,
  runtime: &blob_stream_consumer::ConsumerRuntimeConfig,
  member_id: &str,
  consumer_time: &ManualTimeProvider,
) -> Result<ConsumerIteratorImpl> {
  let mut rebalance_gate = cluster
    .lifecycle_hooks()
    .arm_consumer(
      framework::LifecycleEvent::ConsumerBeforeRebalance,
      member_id,
      None,
      None,
    )
    .await?;
  let mut consumer = cluster.create_consumer(runtime).await?;
  consumer.start()?;
  consumer_time.advance(TimeDuration::seconds(1));
  timeout(Duration::from_secs(5), rebalance_gate.wait_until_reached())
    .await
    .map_err(|_| anyhow::anyhow!("{member_id} did not begin its initial rebalance"))??;
  rebalance_gate.release()?;
  Ok(consumer)
}

// High-level: validates producer retry behavior under deterministic dropped transport requests.
#[tokio::test]
async fn network_drop_produce_retry_no_loss() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .start()
    .await?;

  let controller = cluster
    .network_fault_controller()
    .expect("in-memory transport should expose a fault controller");
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: None,
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::Drop,
      remaining_hits: Some(2),
    })
    .await;

  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(producer_config(), vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;

  let mut expected_ids = HashSet::new();
  let mut produced_partitions = HashSet::new();
  let first_id = "fit-001-0";
  let first_produce = producer.produce(ProducerRecord::new(
    TOPIC.into(),
    b"fit-001-key-0".to_vec(),
    first_id.as_bytes().to_vec().into(),
    framework::now_unix_seconds() * 1_000,
  ));
  tokio::pin!(first_produce);
  let fault_matcher = TestEventMatcher {
    category: Some("transport".to_string()),
    operation: Some("produce_batch".to_string()),
    key_contains: None,
    status: Some("fault_applied".to_string()),
  };
  let first_fault = timeout(Duration::from_secs(5), async {
    tokio::select! {
      event = cluster.wait_for_event_after(&fault_matcher, None, Duration::from_secs(5)) => event,
      result = &mut first_produce => Err(anyhow::anyhow!(
        "produce completed before the injected transport drop: {result:?}"
      )),
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("producer did not issue the injected transport request"))??;
  let _second_fault = cluster
    .wait_for_event_after(
      &fault_matcher,
      Some(first_fault.sequence),
      Duration::from_secs(5),
    )
    .await?;
  timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
    .await
    .map_err(|_| anyhow::anyhow!("producer did not enter retry backoff after transport drop"))?;
  assert!(
    retry_clock.advance_to_next_sleep(),
    "transport-drop retry backoff was not registered"
  );
  let first_ack = timeout(Duration::from_secs(5), &mut first_produce)
    .await
    .map_err(|_| anyhow::anyhow!("producer did not recover after retry clock advance"))??;
  assert_eq!(
    first_ack.attempts, 3,
    "two injected transport drops must require the explicitly released retry"
  );
  expected_ids.insert(first_id.to_string());
  produced_partitions.insert(first_ack.virtual_partition_id);

  for message_id in 1 .. 12 {
    let id = format!("fit-001-{message_id}");
    let key = format!("fit-001-key-{message_id}").into_bytes();
    let ack = produce_message(&producer, key, &id).await?;
    assert_eq!(
      ack.attempts, 1,
      "post-fault request must not retry after the fault budget is consumed"
    );
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    produced_partitions.into_iter().collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let deliveries = drain_reader_until_with_trace(
    &mut reader,
    expected_ids.len(),
    framework::now_unix_seconds().saturating_add(3),
    Instant::now() + Duration::from_secs(15),
  )
  .await?;
  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "dropped pre-persistence requests must be read exactly once: {deliveries:?}"
  );

  let _scan_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("metadata_scan_window".to_string()),
        key_contains: None,
        status: Some("ok".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;

  cluster.shutdown().await;
  resources.cleanup().await;

  Ok(())
}

// High-level: validates that a response lost after broker persistence produces two committed,
// duplicate logical records through the in-process producer, broker, and consumer-group path.
#[tokio::test]
async fn network_response_loss_after_persistence_retries_with_duplicate_batch() -> Result<()> {
  let mut cluster = ClusterHarness::in_memory(1).start().await?;
  let controller = cluster
    .network_fault_controller()
    .expect("in-memory transport should expose a fault controller");
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: None,
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::DropResponse,
      remaining_hits: Some(1),
    })
    .await;

  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(producer_config(), vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;
  let hooks = cluster.lifecycle_hooks();
  let mut persisted_gate = hooks
    .arm(framework::LifecycleEvent::BrokerMetadataPersisted)
    .await?;
  let produce = produce_message(
    &producer,
    b"fit-response-loss-key".to_vec(),
    "fit-response-loss",
  );
  tokio::pin!(produce);

  timeout(Duration::from_secs(5), async {
    tokio::select! {
      result = &mut produce => Err(anyhow::anyhow!(
        "produce completed before the broker metadata-persisted gate: {result:?}"
      )),
      reached = persisted_gate.wait_until_reached() => reached,
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("broker did not persist metadata for response-loss request"))??;

  persisted_gate.release()?;
  let ack = timeout(Duration::from_secs(5), &mut produce)
    .await
    .map_err(|_| anyhow::anyhow!("producer did not recover after response loss"))??;
  assert!(
    ack.attempts == 2,
    "expected exactly one retry after broker-completed response loss, got {} attempts",
    ack.attempts
  );

  let mut runtime = consumer_runtime_config("fit-response-loss-member");
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow::anyhow!("response-loss consumer read config missing"))?
    .metadata_visibility_delay_ms = Some(0);
  let group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("response-loss consumer group config missing"))?;
  let mut prefetch_gate = cluster
    .lifecycle_hooks()
    .arm(framework::LifecycleEvent::ConsumerPrefetchBatchBuffered)
    .await?;
  let mut consumer = cluster.create_consumer(&runtime).await?;
  consumer.start()?;

  timeout(Duration::from_secs(5), prefetch_gate.wait_until_reached())
    .await
    .map_err(|_| anyhow::anyhow!("consumer did not prefetch the retried response-loss batch"))??;
  prefetch_gate.release()?;

  let mut delivered_offsets = Vec::new();
  let mut delivered_id_counts = HashMap::new();
  while delivered_offsets.len() < 2 {
    let next = timeout(Duration::from_secs(5), consumer.next())
      .await
      .map_err(|_| anyhow::anyhow!("consumer did not deliver the next response-loss record"))??;
    match next {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => {
        let id = String::from_utf8(record.record.payload.to_vec())?;
        *delivered_id_counts.entry(id).or_insert(0_usize) += 1;
        delivered_offsets.push((record.virtual_partition_id, record.offset));
        consumer.store_offset(record.virtual_partition_id, record.offset)?;
        consumer.commit().await?;
      },
    }
  }

  assert_eq!(
    delivered_id_counts,
    HashMap::from([("fit-response-loss".to_string(), 2)]),
    "at-least-once response loss must deliver the application record twice"
  );
  assert!(
    delivered_offsets
      .iter()
      .all(|(partition_id, _)| *partition_id == ack.virtual_partition_id),
    "response-loss records must remain on the acknowledged virtual partition"
  );
  assert_eq!(
    delivered_offsets
      .iter()
      .map(|(_, offset)| *offset)
      .collect::<Vec<_>>(),
    vec![0, 1],
    "duplicate attempts must have consecutive delivery offsets"
  );

  let leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  let committed_lease = leases
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == ack.virtual_partition_id)
    .ok_or_else(|| anyhow::anyhow!("missing committed consumer group lease"))?;
  assert_eq!(committed_lease.owner_id, group.member_id.as_str());
  assert!(
    committed_lease
      .committed_cursor
      .as_ref()
      .is_some_and(|cursor| { cursor.seq_end == 1 && cursor.source_checkpoint.is_some() }),
    "consumer group must durably commit the second duplicate delivery with its source checkpoint: \
     {committed_lease:?}"
  );

  assert!(
    cluster
      .event_log()
      .snapshot()
      .await
      .iter()
      .any(|event| event.status == "response_dropped"),
    "expected transport event log to record post-persistence response loss"
  );

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: validates a post-persistence response loss remains an explicit exactly-twice
// producer contract when the retry crosses a deterministic broker ownership handoff.
#[tokio::test]
async fn response_loss_retry_during_broker_handoff_preserves_group_delivery_contract() -> Result<()>
{
  let mut cluster = ClusterHarness::in_memory(2).start().await?;
  let nodes = cluster.live_nodes();
  assert_eq!(
    nodes.len(),
    2,
    "expected two brokers for the handoff scenario"
  );
  let active_node = nodes[0].clone();
  let standby_node = nodes[1].clone();
  cluster.set_active_nodes(vec![active_node.clone()]);

  let controller = cluster
    .network_fault_controller()
    .ok_or_else(|| anyhow::anyhow!("in-memory transport missing fault controller"))?;
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: Some(active_node.node_id.to_string()),
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::DropResponse,
      remaining_hits: Some(1),
    })
    .await;

  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(producer_config(), vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;
  wait_for_producer_route(
    &producer,
    &active_node.node_id,
    &active_node.address,
    Instant::now() + Duration::from_secs(5),
  )
  .await?;

  let id = "fit-response-loss-handoff";
  let key = b"fit-response-loss-handoff-key".to_vec();
  let virtual_partition_id = virtual_partition_for_logical(
    logical_partition_for_key(&key, PARTITION_COUNT),
    PARTITION_COUNT,
    0,
  );
  let hooks = cluster.lifecycle_hooks();
  let mut metadata_persisted = hooks
    .arm_broker_for_partition(
      framework::LifecycleEvent::BrokerMetadataPersisted,
      virtual_partition_id,
    )
    .await?;
  let produce = producer.produce(ProducerRecord::new(
    TOPIC.into(),
    key,
    id.as_bytes().to_vec().into(),
    framework::now_unix_seconds() * 1_000,
  ));
  tokio::pin!(produce);

  timeout(Duration::from_secs(5), async {
    tokio::select! {
      result = &mut produce => Err(anyhow::anyhow!(
        "produce completed before the active broker metadata-persisted boundary: {result:?}"
      )),
      reached = metadata_persisted.wait_until_reached() => reached,
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("active broker did not durably persist the first request"))??;

  // Move discovery before releasing A's persisted request, but hold A's lease release. The
  // producer's immediate response-loss retry is then routed to B and waits for B's ownership.
  let mut lease_released = hooks
    .arm_broker_for_partition(
      framework::LifecycleEvent::BrokerLeaseReleased,
      virtual_partition_id,
    )
    .await?;
  cluster.set_active_nodes(vec![standby_node.clone()]);
  wait_for_producer_route(
    &producer,
    &standby_node.node_id,
    &standby_node.address,
    Instant::now() + Duration::from_secs(5),
  )
  .await?;
  metadata_persisted.release()?;
  let response_dropped = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("transport".to_string()),
        operation: Some("produce_batch".to_string()),
        key_contains: Some(active_node.node_id.to_string()),
        status: Some("response_dropped".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;
  assert!(
    response_dropped
      .detail
      .as_deref()
      .is_some_and(|detail| detail.contains("broker_completed=true")),
    "response-loss event did not prove broker persistence: {response_dropped:?}"
  );

  timeout(Duration::from_secs(5), lease_released.wait_until_reached())
    .await
    .map_err(|_| anyhow::anyhow!("former broker did not release the response-loss partition"))??;
  lease_released.release()?;
  wait_for_broker_lease_ownership(
    &cluster,
    &standby_node.node_id,
    TOPIC,
    &[virtual_partition_id],
    Instant::now() + Duration::from_secs(5),
  )
  .await?;

  let ack = timeout(Duration::from_secs(5), &mut produce)
    .await
    .map_err(|_| anyhow::anyhow!("producer did not complete after broker handoff"))??;
  assert_eq!(
    ack.attempts, 2,
    "response loss across broker ownership handoff must take exactly two producer attempts"
  );
  assert_eq!(ack.virtual_partition_id, virtual_partition_id);

  let mut runtime = consumer_runtime_config("fit-response-loss-handoff-member");
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow::anyhow!("handoff consumer read config missing"))?
    .metadata_visibility_delay_ms = Some(0);
  let group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("handoff consumer group config missing"))?;
  let mut prefetch_gate = hooks
    .arm_prefetch_for_partition(virtual_partition_id)
    .await?;
  let mut consumer = cluster.create_consumer(&runtime).await?;
  consumer.start()?;
  timeout(Duration::from_secs(5), prefetch_gate.wait_until_reached())
    .await
    .map_err(|_| anyhow::anyhow!("group consumer did not prefetch the handoff records"))??;
  prefetch_gate.release()?;

  let mut delivered_offsets = Vec::new();
  let mut delivered_id_counts = HashMap::new();
  while delivered_offsets.len() < 2 {
    match timeout(Duration::from_secs(5), consumer.next())
      .await
      .map_err(|_| anyhow::anyhow!("group consumer did not deliver the next handoff record"))??
    {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => {
        let delivered_id = String::from_utf8(record.record.payload.to_vec())?;
        *delivered_id_counts.entry(delivered_id).or_insert(0_usize) += 1;
        delivered_offsets.push((record.virtual_partition_id, record.offset));
        consumer.store_offset(record.virtual_partition_id, record.offset)?;
        consumer.commit().await?;
      },
    }
  }
  assert_eq!(
    delivered_id_counts,
    HashMap::from([(id.to_string(), 2)]),
    "response loss across broker handoff must retain the exactly-twice delivery contract"
  );
  assert!(
    delivered_offsets
      .iter()
      .all(|(partition_id, _)| *partition_id == virtual_partition_id),
    "handoff retry changed the acknowledged virtual partition: {delivered_offsets:?}"
  );
  assert!(
    delivered_offsets[0].1 < delivered_offsets[1].1,
    "handoff retry must preserve increasing delivery offsets: {delivered_offsets:?}"
  );
  let maximum_offset = delivered_offsets
    .iter()
    .map(|(_, offset)| *offset)
    .max()
    .ok_or_else(|| anyhow::anyhow!("handoff test did not retain delivery offsets"))?;

  let committed_lease = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == virtual_partition_id)
    .ok_or_else(|| anyhow::anyhow!("missing handoff consumer lease"))?;
  assert!(
    committed_lease.owner_id == group.member_id.as_str()
      && committed_lease
        .committed_cursor
        .as_ref()
        .is_some_and(|cursor| {
          cursor.seq_end >= maximum_offset && cursor.source_checkpoint.is_some()
        }),
    "group did not durably commit the maximum handoff delivery offset: {committed_lease:?}"
  );

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: validates that deterministic delay+reorder transport faults preserve data and
// per-partition sequence monotonicity during reads.
#[tokio::test]
async fn network_delay_and_reorder_preserves_cursor_monotonicity() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .start()
    .await?;

  let controller = cluster
    .network_fault_controller()
    .expect("in-memory transport should expose a fault controller");
  let scheduler = controller.enable_manual_scheduling().await;
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: None,
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::Delay(Duration::from_millis(20)),
      remaining_hits: Some(64),
    })
    .await;
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: None,
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::Reorder {
        delay: Duration::from_millis(35),
      },
      remaining_hits: Some(64),
    })
    .await;

  let first_producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;
  let second_producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  for pair in 0 .. 24 {
    let first_id = format!("fit-002-{}", pair * 2);
    let second_id = format!("fit-002-{}", (pair * 2) + 1);
    let key = format!("fit-002-key-{}", pair % 12).into_bytes();
    let produce_pair = async {
      tokio::try_join!(
        produce_message(&first_producer, key.clone(), &first_id),
        produce_message(&second_producer, key, &second_id),
      )
    };
    tokio::pin!(produce_pair);
    tokio::select! {
      result = &mut produce_pair => {
        return Err(anyhow::anyhow!(
          "delay/reorder pair completed before both transport delays registered: {result:?}"
        ));
      },
      () = scheduler.wait_until_sleeping(2) => {},
    }
    scheduler.advance();
    timeout(Duration::from_secs(5), &mut produce_pair)
      .await
      .map_err(|_| {
        anyhow::anyhow!("delay/reorder pair did not complete after scheduler advance")
      })??;
    expected_ids.insert(first_id.clone());
    expected_ids.insert(second_id.clone());
  }

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      ..Default::default()
    },
    (0 .. framework::PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let mut deliveries = Vec::new();
  let mut last_seq_end_by_partition = HashMap::new();
  let reader_now = framework::now_unix_seconds().saturating_add(3);
  let deadline = Instant::now() + Duration::from_secs(45);

  while reader_delivery_counts(&deliveries).len() < expected_ids.len() {
    if Instant::now() >= deadline {
      anyhow::bail!(
        "all manually scheduled delay/reorder pairs completed but the reader did not observe \
         every record: expected={}, consumed={}",
        expected_ids.len(),
        reader_delivery_counts(&deliveries).len()
      );
    }

    let batches = reader.read_available(reader_now).await?;
    if batches.is_empty() {
      tokio::task::yield_now().await;
      continue;
    }

    for batch in batches {
      if let Some(previous_end) = last_seq_end_by_partition.get(&batch.virtual_partition_id) {
        assert!(
          batch.seq_range.end > *previous_end,
          "seq_end did not increase for partition {}: previous={}, current={}",
          batch.virtual_partition_id,
          previous_end,
          batch.seq_range.end
        );
      }
      last_seq_end_by_partition.insert(batch.virtual_partition_id, batch.seq_range.end);
      append_reader_delivery_traces(vec![batch], &mut deliveries)?;
    }
  }

  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "delay/reorder transport must not duplicate reader delivery: {deliveries:?}"
  );
  assert!(
    !last_seq_end_by_partition.is_empty(),
    "expected to observe at least one partition cursor"
  );

  let events = cluster.event_log().snapshot().await;
  assert!(
    events.windows(2).any(|events| {
      events[0].status == "reorder_released"
        && events[0].detail.as_deref() == Some("role=second")
        && events[1].status == "reorder_released"
        && events[1].detail.as_deref() == Some("role=first")
    }),
    "expected an in-memory transport reorder to release the second request before the first"
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates active-broker partition fault handling with deterministic reroute to a
// standby broker and no-loss completion.
#[tokio::test]
async fn network_partition_active_broker_takeover() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .start()
    .await?;

  let nodes = cluster.live_nodes();
  assert_eq!(nodes.len(), 2, "expected a two-broker cluster");
  let active_node = nodes[0].clone();
  let standby_node = nodes[1].clone();
  cluster.set_active_nodes(vec![active_node.clone()]);

  let controller = cluster
    .network_fault_controller()
    .expect("in-memory transport should expose a fault controller");
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: Some(active_node.node_id.to_string()),
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::Partition,
      remaining_hits: Some(2),
    })
    .await;

  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(producer_config(), vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;

  let mut expected_ids = HashSet::new();
  let mut produced_partitions = HashSet::new();
  let first_id = "fit-003-pre-reroute-0";
  let first_produce = producer.produce(ProducerRecord::new(
    TOPIC.into(),
    b"fit-003-key-pre-0".to_vec(),
    first_id.as_bytes().to_vec().into(),
    framework::now_unix_seconds() * 1_000,
  ));
  tokio::pin!(first_produce);
  let fault_matcher = TestEventMatcher {
    category: Some("transport".to_string()),
    operation: Some("produce_batch".to_string()),
    key_contains: Some(active_node.node_id.to_string()),
    status: Some("fault_applied".to_string()),
  };
  let first_fault = timeout(Duration::from_secs(5), async {
    tokio::select! {
      event = cluster.wait_for_event_after(&fault_matcher, None, Duration::from_secs(5)) => event,
      result = &mut first_produce => Err(anyhow::anyhow!(
        "produce completed before the active-broker partition fault: {result:?}"
      )),
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("producer did not reach the active-broker partition fault"))??;
  let _second_fault = cluster
    .wait_for_event_after(
      &fault_matcher,
      Some(first_fault.sequence),
      Duration::from_secs(5),
    )
    .await?;
  timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
    .await
    .map_err(|_| anyhow::anyhow!("producer did not enter backoff after partition faults"))?;
  assert!(
    retry_clock.advance_to_next_sleep(),
    "partition-fault retry backoff was not registered"
  );
  let first_ack = timeout(Duration::from_secs(5), &mut first_produce)
    .await
    .map_err(|_| anyhow::anyhow!("producer did not recover after partition retry advance"))??;
  assert_eq!(
    first_ack.attempts, 3,
    "two active-broker partition faults must require the explicitly released retry"
  );
  expected_ids.insert(first_id.to_string());
  produced_partitions.insert(first_ack.virtual_partition_id);

  for message_id in 1 .. 12 {
    let id = format!("fit-003-pre-reroute-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-003-key-pre-{message_id}").into_bytes(),
      &id,
    )
    .await?;
    assert_eq!(
      ack.attempts, 1,
      "pre-reroute request must not retry after the partition-fault budget is consumed"
    );
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  // Deterministically reroute producer discovery to the standby node after the active-node
  // partition fault has been exercised.
  cluster.set_active_nodes(vec![standby_node.clone()]);
  timeout(Duration::from_secs(5), async {
    loop {
      let snapshot = producer
        .diagnostics()
        .expect("producer diagnostics must be available")
        .state_snapshot();
      let routes_converged = !snapshot.route_map.is_empty()
        && snapshot.route_map.iter().all(|route| {
          route.selected_broker.as_ref().is_some_and(|broker| {
            broker.node_id == standby_node.node_id && broker.address == standby_node.address
          })
        });
      if routes_converged {
        return;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("producer routes did not converge to the standby broker"))?;
  let first_post_id = "fit-003-post-reroute-12";
  let first_post_ack = complete_produce_with_manual_retries(
    producer.produce(ProducerRecord::new(
      TOPIC.into(),
      b"fit-003-key-post-12".to_vec(),
      first_post_id.as_bytes().to_vec().into(),
      framework::now_unix_seconds() * 1_000,
    )),
    &retry_clock,
    "producer did not reroute after active-broker switch",
  )
  .await?;
  expected_ids.insert(first_post_id.to_string());
  produced_partitions.insert(first_post_ack.virtual_partition_id);

  for message_id in 13 .. 36 {
    let id = format!("fit-003-post-reroute-{message_id}");
    let ack = complete_produce_with_manual_retries(
      producer.produce(ProducerRecord::new(
        TOPIC.into(),
        format!("fit-003-key-post-{message_id}").into_bytes(),
        id.as_bytes().to_vec().into(),
        framework::now_unix_seconds() * 1_000,
      )),
      &retry_clock,
      "post-reroute producer request did not complete",
    )
    .await?;
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    produced_partitions.into_iter().collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let deliveries = drain_reader_until_with_trace(
    &mut reader,
    expected_ids.len(),
    framework::now_unix_seconds().saturating_add(3),
    Instant::now() + Duration::from_secs(30),
  )
  .await?;
  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "partitioned broker retries must be read exactly once: {deliveries:?}"
  );

  let _standby_ok_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("transport".to_string()),
        operation: Some("produce_batch".to_string()),
        key_contains: Some(standby_node.node_id.to_string()),
        status: Some("ok".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates retry exhaustion at the configured deadline and deterministic recovery
// once the transport fault window is consumed.
#[tokio::test]
async fn producer_retry_deadline_respected_after_transport_failures() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .start()
    .await?;

  let controller = cluster
    .network_fault_controller()
    .expect("in-memory transport should expose a fault controller");
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: None,
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::Drop,
      remaining_hits: Some(3),
    })
    .await;

  let mut config = producer_config();
  config.retry_deadline_ms = Some(500);
  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(config, vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;

  let exhausted = producer.produce(ProducerRecord::new(
    TOPIC.into(),
    b"fit-004-timeout".to_vec(),
    b"fit-004-timeout".to_vec().into(),
    framework::now_unix_seconds() * 1_000,
  ));
  tokio::pin!(exhausted);
  let fault_matcher = TestEventMatcher {
    category: Some("transport".to_string()),
    operation: Some("produce_batch".to_string()),
    key_contains: None,
    status: Some("fault_applied".to_string()),
  };
  let first_fault = timeout(Duration::from_secs(5), async {
    tokio::select! {
      event = cluster.wait_for_event_after(&fault_matcher, None, Duration::from_secs(5)) => event,
      result = &mut exhausted => Err(anyhow::anyhow!(
        "produce completed before its first transport fault: {result:?}"
      )),
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("producer did not issue its first transport request"))??;
  timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
    .await
    .map_err(|_| anyhow::anyhow!("producer did not enter its first retry backoff"))?;
  assert!(
    retry_clock.advance_to_next_sleep(),
    "first retry backoff was not registered"
  );

  let second_fault = cluster
    .wait_for_event_after(
      &fault_matcher,
      Some(first_fault.sequence),
      Duration::from_secs(5),
    )
    .await?;
  timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
    .await
    .map_err(|_| anyhow::anyhow!("producer did not enter its second retry backoff"))?;
  assert!(
    retry_clock.advance_to_next_sleep(),
    "second retry backoff was not registered"
  );

  let third_fault = cluster
    .wait_for_event_after(
      &fault_matcher,
      Some(second_fault.sequence),
      Duration::from_secs(5),
    )
    .await?;
  retry_clock.advance(Duration::from_millis(500));

  let exhausted = timeout(Duration::from_secs(5), &mut exhausted)
    .await
    .map_err(|_| anyhow::anyhow!("producer did not exhaust retries after deadline advance"))?;
  assert!(
    matches!(exhausted, Err(ProducerError::RetriesExhausted(_))),
    "expected retries exhausted after transport faults, got {exhausted:?}"
  );
  assert!(
    third_fault.sequence > second_fault.sequence,
    "third transport fault must be distinct from the prior retry"
  );

  let recovery_ack = produce_message(
    &producer,
    b"fit-004-recovery-key".to_vec(),
    "fit-004-recovery-message",
  )
  .await?;
  assert_eq!(
    recovery_ack.attempts, 1,
    "expected recovery produce to succeed without retries after fault budget was consumed"
  );

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      ..Default::default()
    },
    vec![recovery_ack.virtual_partition_id],
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let deliveries = drain_reader_until_with_trace(
    &mut reader,
    1,
    framework::now_unix_seconds().saturating_add(3),
    Instant::now() + Duration::from_secs(15),
  )
  .await?;
  assert_eq!(
    reader_delivery_counts(&deliveries),
    HashMap::from([("fit-004-recovery-message".to_string(), 1)]),
    "recovery after retry exhaustion must be read exactly once"
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates transient blob put failures recover via retries with no final data loss.
#[tokio::test]
async fn s3_put_transient_failures_recover_without_loss() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .blob_store(resources.s3_blob_store())
    .start()
    .await?;

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Blob,
      operation: StoreFaultOperation::BlobPut,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "transient put failure".to_string(),
      },
      remaining_hits: Some(3),
    })
    .await;

  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(producer_config(), vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;

  let mut expected_ids = HashSet::new();
  let mut produced_partitions = HashSet::new();
  let first_id = "fit-005-0";
  let first_produce = producer.produce(ProducerRecord::new(
    TOPIC.into(),
    b"fit-005-key-0".to_vec(),
    first_id.as_bytes().to_vec().into(),
    framework::now_unix_seconds() * 1_000,
  ));
  tokio::pin!(first_produce);
  let fault_matcher = TestEventMatcher {
    category: Some("store".to_string()),
    operation: Some("blob_put".to_string()),
    key_contains: None,
    status: Some("fault_applied".to_string()),
  };
  let mut previous_fault = None;
  for retry_number in 1 ..= 3 {
    let fault = timeout(Duration::from_secs(5), async {
      tokio::select! {
        event = cluster.wait_for_event_after(
          &fault_matcher,
          previous_fault,
          Duration::from_secs(5),
        ) => event,
        result = &mut first_produce => Err(anyhow::anyhow!(
          "produce completed before blob-put fault {retry_number}: {result:?}"
        )),
      }
    })
    .await
    .map_err(|_| anyhow::anyhow!("producer did not reach blob-put fault {retry_number}"))??;
    previous_fault = Some(fault.sequence);
    timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
      .await
      .map_err(|_| anyhow::anyhow!("producer did not enter blob-put retry {retry_number}"))?;
    assert!(
      retry_clock.advance_to_next_sleep(),
      "blob-put retry {retry_number} was not registered"
    );
  }
  let first_ack = timeout(Duration::from_secs(5), &mut first_produce)
    .await
    .map_err(|_| anyhow::anyhow!("producer did not recover after blob-put retries"))??;
  assert_eq!(
    first_ack.attempts, 4,
    "three blob-put failures must require three explicit retries"
  );
  expected_ids.insert(first_id.to_string());
  produced_partitions.insert(first_ack.virtual_partition_id);

  for message_id in 1 .. 24 {
    let id = format!("fit-005-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-005-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    assert_eq!(
      ack.attempts, 1,
      "post-fault blob-put request must not retry after the fault budget is consumed"
    );
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    produced_partitions.into_iter().collect(),
    HashMap::new(),
    resources.s3_blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let deliveries = drain_reader_until_with_trace(
    &mut reader,
    expected_ids.len(),
    framework::now_unix_seconds().saturating_add(3),
    Instant::now() + Duration::from_secs(30),
  )
  .await?;
  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "transient blob-put retries must be read exactly once: {deliveries:?}"
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates transient blob get failures during consume recover via reader re-scan.
#[tokio::test]
async fn s3_get_failures_consumer_rescan_recovers() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .blob_store(resources.s3_blob_store())
    .start()
    .await?;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  let mut produced_partitions = HashSet::new();
  for message_id in 0 .. 30 {
    let id = format!("fit-006-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-006-key-{}", message_id % 10).into_bytes(),
      &id,
    )
    .await?;
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Blob,
      operation: StoreFaultOperation::BlobGetRange,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "transient get failure".to_string(),
      },
      remaining_hits: Some(5),
    })
    .await;

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    produced_partitions.into_iter().collect(),
    HashMap::new(),
    resources.s3_blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let mut deliveries = Vec::new();
  let reader_now = framework::now_unix_seconds().saturating_add(1);
  for attempt in 0 .. 5 {
    let mut fault_error = None;
    for _ in 0 .. 20 {
      match rescan_reader_with_trace(&mut reader, reader_now, &mut deliveries).await {
        Ok(0) => {},
        Ok(batch_count) => {
          anyhow::bail!(
            "reader returned {batch_count} batches before scripted blob fault {attempt}"
          );
        },
        Err(error) => {
          fault_error = Some(error);
          break;
        },
      }
    }
    let error = fault_error.ok_or_else(|| {
      anyhow::anyhow!("reader did not reach scripted blob fault {attempt} after direct rescans")
    })?;
    assert!(
      error.to_string().contains("transient get failure"),
      "blob rescan {attempt} failed for an unexpected reason: {error:#}"
    );
  }
  rescan_reader_with_trace(&mut reader, reader_now, &mut deliveries).await?;

  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "blob-get rescan must not duplicate reader delivery: {deliveries:?}"
  );

  let _store_fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("blob_get_range".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates a conclusively missing blob is counted as loss without blocking reads.
#[tokio::test]
async fn s3_get_not_found_consumer_skips_lost_data() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .blob_store(resources.s3_blob_store())
    .start()
    .await?;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  let mut produced_partitions = HashSet::new();
  for message_id in 0 .. 30 {
    let id = format!("fit-006-not-found-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-006-not-found-key-{}", message_id % 10).into_bytes(),
      &id,
    )
    .await?;
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Blob,
      operation: StoreFaultOperation::BlobGetRange,
      key_pattern: None,
      action: StoreFaultAction::NotFound,
      remaining_hits: Some(1),
    })
    .await;

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    produced_partitions.into_iter().collect(),
    HashMap::new(),
    resources.s3_blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_not_found_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let mut deliveries = Vec::new();
  let reader_now = framework::now_unix_seconds().saturating_add(1);
  for _ in 0 .. 20 {
    rescan_reader_with_trace(&mut reader, reader_now, &mut deliveries).await?;
    if !deliveries.is_empty() {
      break;
    }
  }

  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert!(
    !delivered_id_counts.is_empty(),
    "consumer made no progress after a missing blob"
  );
  assert!(
    delivered_id_counts.len() < expected_ids.len(),
    "missing blob should remove at least one expected record: {deliveries:?}"
  );
  assert!(
    delivered_id_counts
      .keys()
      .all(|delivered_id| expected_ids.contains(delivered_id)),
    "consumer returned an unexpected record after skipping a missing blob: {deliveries:?}"
  );

  let _store_fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("blob_get_range".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates metadata-write retry exhaustion and recovery with a group-level durable
// acknowledgement oracle.
#[tokio::test]
async fn metadata_write_fail_then_retry_ack_semantics() -> Result<()> {
  let mut cluster = ClusterHarness::in_memory(1).start().await?;
  let fault_controller = cluster
    .store_fault_controller()
    .expect("in-memory harness should expose a store fault controller");
  fault_controller
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Metadata,
      operation: StoreFaultOperation::MetadataWriteSegment,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "transient metadata write failure".to_string(),
      },
      remaining_hits: Some(3),
    })
    .await;

  let mut config = producer_config();
  config.retry_deadline_ms = Some(50);
  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(config, vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;

  let failed_produce = producer.produce(ProducerRecord::new(
    TOPIC.into(),
    b"fit-007-key".to_vec(),
    b"fit-007-failed".to_vec().into(),
    framework::now_unix_seconds() * 1_000,
  ));
  tokio::pin!(failed_produce);

  let metadata_fault_matcher = TestEventMatcher {
    category: Some("store".to_string()),
    operation: Some("metadata_write_segment".to_string()),
    key_contains: None,
    status: Some("fault_applied".to_string()),
  };
  let first_fault = timeout(Duration::from_secs(5), async {
    tokio::select! {
      event = cluster.wait_for_event_after(
        &metadata_fault_matcher,
        None,
        Duration::from_secs(5),
      ) => event,
      result = &mut failed_produce => Err(anyhow::anyhow!(
        "produce completed before the first metadata write fault: {result:?}"
      )),
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("producer did not issue its first metadata write"))??;
  timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
    .await
    .map_err(|_| anyhow::anyhow!("producer did not enter its first metadata retry backoff"))?;
  assert!(
    retry_clock.advance_to_next_sleep(),
    "first metadata retry backoff was not registered"
  );

  let second_fault = cluster
    .wait_for_event_after(
      &metadata_fault_matcher,
      Some(first_fault.sequence),
      Duration::from_secs(5),
    )
    .await?;
  timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
    .await
    .map_err(|_| anyhow::anyhow!("producer did not enter its second metadata retry backoff"))?;
  assert!(
    retry_clock.advance_to_next_sleep(),
    "second metadata retry backoff was not registered"
  );

  let third_fault = cluster
    .wait_for_event_after(
      &metadata_fault_matcher,
      Some(second_fault.sequence),
      Duration::from_secs(5),
    )
    .await?;
  timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
    .await
    .map_err(|_| anyhow::anyhow!("producer did not enter its final metadata retry backoff"))?;
  retry_clock.advance(Duration::from_millis(50));

  let failed_ack = timeout(Duration::from_secs(5), &mut failed_produce)
    .await
    .map_err(|_| anyhow::anyhow!("producer did not exhaust retries after deadline advance"))?;
  assert!(
    matches!(failed_ack, Err(ProducerError::RetriesExhausted(_))),
    "expected retry exhaustion after three metadata write failures, got {failed_ack:?}"
  );
  assert!(
    third_fault.sequence > second_fault.sequence,
    "third metadata fault must be distinct from the prior retry"
  );

  let success_id = "fit-007-success";
  let marker_id = "fit-007-marker";
  let success_ack = produce_message(&producer, b"fit-007-key".to_vec(), success_id).await?;
  let marker_ack = produce_message(&producer, b"fit-007-key".to_vec(), marker_id).await?;
  assert_eq!(
    marker_ack.virtual_partition_id, success_ack.virtual_partition_id,
    "same-key recovery marker must remain on the failed request partition"
  );

  let mut runtime = consumer_runtime_config("fit-007-member");
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow::anyhow!("metadata retry consumer read config missing"))?
    .metadata_visibility_delay_ms = Some(0);
  let group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("metadata retry consumer group config missing"))?;
  let mut consumer = cluster.create_consumer(&runtime).await?;
  consumer.start()?;

  let (marker_partition, marker_offset, delivery_counts, delivered_ids) =
    timeout(Duration::from_secs(5), async {
      let mut delivery_counts = HashMap::new();
      let mut delivered_ids = Vec::new();
      loop {
        match consumer.next().await? {
          NextResult::Revoked(revoked) => revoked.complete().await,
          NextResult::Record(record) => {
            let id = String::from_utf8(record.record.payload.to_vec())?;
            *delivery_counts.entry(id.clone()).or_insert(0usize) += 1;
            delivered_ids.push(id.clone());
            consumer.store_offset(record.virtual_partition_id, record.offset)?;
            consumer.commit().await?;
            if id == marker_id {
              return Ok::<_, anyhow::Error>((
                record.virtual_partition_id,
                record.offset,
                delivery_counts,
                delivered_ids,
              ));
            }
          },
        }
      }
    })
    .await
    .map_err(|_| anyhow::anyhow!("consumer group did not receive recovery marker"))??;
  assert_eq!(
    marker_partition, success_ack.virtual_partition_id,
    "recovery marker must remain on the original virtual partition"
  );
  assert_eq!(
    delivered_ids,
    vec![success_id.to_string(), marker_id.to_string()],
    "failed metadata write became visible before the recovery marker"
  );
  assert_eq!(
    delivery_counts,
    HashMap::from([(success_id.to_string(), 1), (marker_id.to_string(), 1)]),
    "recovery traffic must be delivered exactly once through the marker"
  );

  let leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  let committed_lease = leases
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == success_ack.virtual_partition_id)
    .ok_or_else(|| anyhow::anyhow!("missing committed recovery consumer lease"))?;
  assert_eq!(committed_lease.owner_id, group.member_id.as_str());
  assert!(
    committed_lease
      .committed_cursor
      .as_ref()
      .is_some_and(|cursor| {
        cursor.seq_end >= marker_offset && cursor.source_checkpoint.is_some()
      }),
    "consumer group must durably commit the recovery marker: {committed_lease:?}"
  );

  Box::new(consumer).shutdown().await?;

  cluster.shutdown().await;
  Ok(())
}

// High-level: validates stale metadata scan windows do not regress cursor progress or duplicate
// terminal consumption.
#[tokio::test]
async fn metadata_scan_stale_visibility_no_duplicate_progress() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .start()
    .await?;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  for message_id in 0 .. 36 {
    let id = format!("fit-008-{message_id}");
    produce_message(
      &producer,
      format!("fit-008-key-{}", message_id % 12).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Metadata,
      operation: StoreFaultOperation::MetadataScanWindow,
      key_pattern: None,
      action: StoreFaultAction::StaleRead,
      remaining_hits: Some(8),
    })
    .await;

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      metadata_visibility_delay_ms: Some(0),
      ..Default::default()
    },
    (0 .. framework::PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let mut deliveries = Vec::new();
  let reader_now = framework::now_unix_seconds().saturating_add(1);
  for _ in 0 .. 8 {
    rescan_reader_with_trace(&mut reader, reader_now, &mut deliveries).await?;
  }

  let fault_events = resources.store_fault_controller().events().await;
  let stale_scan_fault_count = fault_events
    .iter()
    .filter(|event| {
      event.domain == StoreFaultDomain::Metadata
        && event.operation == StoreFaultOperation::MetadataScanWindow
        && matches!(event.action, Some(StoreFaultAction::StaleRead))
    })
    .count();
  assert_eq!(
    stale_scan_fault_count, 8,
    "stale metadata scan did not exhaust its scripted fault budget: {fault_events:?}"
  );

  rescan_reader_with_trace(&mut reader, reader_now, &mut deliveries).await?;

  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "stale metadata scan duplicated reader delivery before catch-up: {deliveries:?}"
  );
  let cursors_after_catchup = reader.cursors();

  // Re-scan repeatedly after full catch-up to ensure duplicate scans do not regress cursors.
  for _ in 0 .. 6 {
    let before_count = deliveries.len();
    let batches = reader.read_available(framework::now_unix_seconds()).await?;
    append_reader_delivery_traces(batches, &mut deliveries)?;
    assert_eq!(
      deliveries.len(),
      before_count,
      "post-catchup scan should not surface duplicate records"
    );
    assert_eq!(
      reader.cursors(),
      cursors_after_catchup,
      "cursor state regressed after stale metadata scan"
    );
    tokio::task::yield_now().await;
  }

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates producer lease conflict injection still converges after deterministic
// broker rerouting, preserving full write progress without accepted split writes.
#[tokio::test]
async fn producer_lease_store_conflicts_then_broker_reroute_preserves_progress() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .start()
    .await?;

  let nodes = cluster.live_nodes();
  assert_eq!(nodes.len(), 2, "expected a two-broker cluster");
  let active_node = nodes[0].clone();
  let standby_node = nodes[1].clone();

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ProducerLease,
      operation: StoreFaultOperation::ProducerAcquireLease,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "simulated lease conflict".to_string(),
      },
      remaining_hits: Some(4),
    })
    .await;

  cluster.set_active_nodes(vec![active_node.clone()]);
  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(producer_config(), vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;

  let mut expected_ids = HashSet::new();
  let mut produced_partitions = HashSet::new();

  let lease_fault_matcher = TestEventMatcher {
    category: Some("store".to_string()),
    operation: Some("producer_acquire_lease".to_string()),
    key_contains: None,
    status: Some("fault_applied".to_string()),
  };
  let mut previous_fault_sequence = None;
  for conflict_index in 1 ..= 4 {
    let fault = cluster
      .wait_for_event_after(
        &lease_fault_matcher,
        previous_fault_sequence,
        Duration::from_secs(5),
      )
      .await
      .map_err(|error| {
        anyhow::anyhow!("broker did not reach lease conflict {conflict_index}: {error}")
      })?;
    previous_fault_sequence = Some(fault.sequence);

    cluster.set_active_nodes(vec![active_node.clone()]);
  }

  let acquired_event = cluster
    .wait_for_event_after(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("producer_acquire_lease".to_string()),
        key_contains: None,
        status: Some("ok".to_string()),
      },
      previous_fault_sequence,
      Duration::from_secs(5),
    )
    .await?;
  assert!(
    acquired_event.sequence > previous_fault_sequence.unwrap(),
    "broker lease acquisition did not follow the complete conflict script"
  );

  for message_id in 0 .. 16 {
    let id = format!("fit-009-pre-reroute-{message_id}");
    let ack = complete_produce_with_manual_retries(
      producer.produce(ProducerRecord::new(
        TOPIC.into(),
        format!("fit-009-key-pre-{message_id}").into_bytes(),
        id.as_bytes().to_vec().into(),
        framework::now_unix_seconds() * 1_000,
      )),
      &retry_clock,
      "pre-reroute producer request did not complete",
    )
    .await?;
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  let lease_fault_events = resources.store_fault_controller().events().await;
  let injected_conflicts = lease_fault_events
    .iter()
    .filter(|event| {
      event.domain == StoreFaultDomain::ProducerLease
        && event.operation == StoreFaultOperation::ProducerAcquireLease
        && matches!(event.action, Some(StoreFaultAction::Fail { .. }))
    })
    .count();
  assert_eq!(
    injected_conflicts, 4,
    "expected all scripted producer lease conflicts before rerouting: {lease_fault_events:?}"
  );

  // First converge discovery and broker ownership, then explicitly drive any producer retry that
  // arises while the standby takes over the complete partition set.
  // The former broker releases telemetry partitions in sorted order. Gate the final release so
  // the successor's reconciliation happens only after the entire target topic is available.
  let handoff_partition = PARTITION_COUNT - 1;
  let mut lease_released_gate = cluster
    .lifecycle_hooks()
    .arm_broker_for_partition(
      framework::LifecycleEvent::BrokerLeaseReleased,
      handoff_partition,
    )
    .await?;
  cluster.set_active_nodes(vec![standby_node.clone()]);
  wait_for_producer_route(
    &producer,
    &standby_node.node_id,
    &standby_node.address,
    Instant::now() + Duration::from_secs(5),
  )
  .await?;
  timeout(
    Duration::from_secs(5),
    lease_released_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow::anyhow!("former broker did not release the final handoff partition"))??;
  // A membership update while the old leases were still held can make the successor observe
  // `HeldByOther`; publish the stable membership again to drive its post-release reconciliation.
  cluster.set_active_nodes(vec![standby_node.clone()]);
  let all_partitions = (0 .. PARTITION_COUNT).collect::<Vec<_>>();
  wait_for_broker_lease_ownership(
    &cluster,
    &standby_node.node_id,
    TOPIC,
    &all_partitions,
    Instant::now() + Duration::from_secs(5),
  )
  .await?;
  lease_released_gate.release()?;

  for message_id in 16 .. 40 {
    let id = format!("fit-009-post-reroute-{message_id}");
    let ack = complete_produce_with_manual_retries(
      producer.produce(ProducerRecord::new(
        TOPIC.into(),
        format!("fit-009-key-post-{message_id}").into_bytes(),
        id.as_bytes().to_vec().into(),
        framework::now_unix_seconds() * 1_000,
      )),
      &retry_clock,
      "post-reroute producer request did not complete",
    )
    .await?;
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      ..Default::default()
    },
    produced_partitions.into_iter().collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let deliveries = drain_reader_until_with_trace(
    &mut reader,
    expected_ids.len(),
    framework::now_unix_seconds().saturating_add(3),
    Instant::now() + Duration::from_secs(45),
  )
  .await?;
  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "producer lease reroute must be read exactly once: {deliveries:?}"
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates a live owner is fenced after cursor and heartbeat failures and its
// replacement recovers normal consumer delivery and cursor commits.
#[tokio::test]
async fn consumer_lease_store_heartbeat_failover() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let consumer_time = Arc::new(ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .in_memory_transport()
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut runtime_a = consumer_runtime_config("fit-010-a");
  let mut runtime_b = consumer_runtime_config("fit-010-b");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow::anyhow!("fit-010 consumer read config missing"))?
      .metadata_visibility_delay_ms = Some(0);
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("fit-010 consumer group config missing"))?;
  let hooks = cluster.lifecycle_hooks();
  let key = b"fit-010-key".to_vec();
  let before_id = "fit-010-before-failure";
  let before_ack = produce_message(&producer, key.clone(), before_id).await?;
  consumer_time.set_time(OffsetDateTime::now_utc());
  consumer_time.advance(TimeDuration::seconds(1));
  let mut initial_rebalance_gate = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerBeforeRebalance,
      "fit-010-a",
      None,
      None,
    )
    .await?;
  let mut before_prefetch_gate = hooks
    .arm_prefetch_for_partition(before_ack.virtual_partition_id)
    .await?;
  let mut consumer_a = cluster.create_consumer(&runtime_a).await?;
  consumer_a.start()?;
  timeout(
    Duration::from_secs(5),
    initial_rebalance_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow::anyhow!("initial owner did not begin its initial rebalance"))??;
  initial_rebalance_gate.release()?;
  timeout(
    Duration::from_secs(5),
    before_prefetch_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow::anyhow!("initial owner did not prefetch its assigned record"))??;
  before_prefetch_gate.release()?;
  let before_delivery = timeout(Duration::from_secs(5), async {
    loop {
      match consumer_a.next().await? {
        NextResult::Revoked(revoked) => revoked.complete().await,
        NextResult::Record(record) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          consumer_a.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_a.commit().await?;
          if id == before_id {
            return Ok::<_, anyhow::Error>((record.virtual_partition_id, record.offset));
          }
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("initial owner did not consume its pre-failure record"))??;
  assert_eq!(before_delivery.0, before_ack.virtual_partition_id);

  let owner_a_lease = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == before_delivery.0)
    .ok_or_else(|| anyhow::anyhow!("missing initial owner lease for heartbeat handoff"))?;
  assert!(
    owner_a_lease.owner_id == "fit-010-a"
      && owner_a_lease
        .committed_cursor
        .as_ref()
        .is_some_and(|cursor| {
          cursor.seq_end >= before_delivery.1 && cursor.source_checkpoint.is_some()
        }),
    "initial owner did not durably commit its pre-failure record: {owner_a_lease:?}"
  );

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerCommitCursor,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "fault active owner cursor commit".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;
  assert!(
    consumer_a.commit().await.is_err(),
    "expected active owner cursor commit to fail"
  );
  let heartbeat_fault_events = resources
    .store_fault_controller()
    .events()
    .await
    .into_iter()
    .filter(|event| {
      event.operation == StoreFaultOperation::ConsumerCommitCursor && event.action.is_some()
    })
    .count();
  assert_eq!(
    heartbeat_fault_events, 1,
    "missing active-owner cursor commit fault"
  );

  let mut scheduled_heartbeat_gate = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerBeforeScheduledHeartbeat,
      "fit-010-a",
      None,
      None,
    )
    .await?;
  let membership_fault_id = resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerMembershipHeartbeat,
      key_pattern: Some(format!("{}#{}#fit-010-a", group.topic, group.group_id)),
      action: StoreFaultAction::Fail {
        message: "fence active owner membership".to_string(),
      },
      remaining_hits: None,
    })
    .await;
  framework::advance_manual_time_until_lifecycle_gate(
    &consumer_time,
    &mut scheduled_heartbeat_gate,
    "scheduled membership heartbeat did not reach its lifecycle boundary",
  )
  .await?;
  scheduled_heartbeat_gate.release()?;
  timeout(Duration::from_secs(5), async {
    loop {
      tokio::task::yield_now().await;
      let membership_fault_applied = resources
        .store_fault_controller()
        .events()
        .await
        .iter()
        .any(|event| {
          event.operation == StoreFaultOperation::ConsumerMembershipHeartbeat
            && event.action.is_some()
        });
      if membership_fault_applied {
        return;
      }
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("scheduled membership heartbeat did not consume the fault"))?;

  // A's last successful lease and membership renewal is now fixed in logical time. Advancing past
  // the lease duration makes the handoff depend on normal expiry rather than a wall-clock delay.
  consumer_time.advance(TimeDuration::seconds(3));
  tokio::task::yield_now().await;

  let mut consumer_b = cluster.create_consumer(&runtime_b).await?;
  consumer_b.start()?;
  let active_members = cluster
    .consumer_membership_store()
    .list_active_members(
      group.topic.as_str(),
      group.group_id.as_str(),
      consumer_time.now().unix_timestamp() * 1_000,
    )
    .await?;
  assert_eq!(member_ids(&active_members), vec!["fit-010-b".to_string()]);
  let takeover_leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  assert_eq!(takeover_leases.len(), PARTITION_COUNT as usize);
  assert!(
    takeover_leases
      .iter()
      .all(|lease| lease.owner_id == "fit-010-b" && lease.generation > owner_a_lease.generation),
    "replacement did not take every expired owner lease: {takeover_leases:?}"
  );

  let after_id = "fit-010-after-failure";
  let after_ack = produce_message(&producer, key, after_id).await?;
  assert_eq!(after_ack.virtual_partition_id, before_delivery.0);
  let after_delivery = timeout(Duration::from_secs(5), async {
    loop {
      consumer_time.advance(TimeDuration::seconds(1));
      tokio::task::yield_now().await;

      if let Ok(Ok(NextResult::Record(record))) =
        timeout(Duration::from_millis(50), consumer_a.next()).await
      {
        let id = String::from_utf8(record.record.payload.to_vec())?;
        anyhow::bail!(
          "former owner delivered {id} after replacement acquired its leases: {takeover_leases:?}"
        );
      }

      match timeout(Duration::from_millis(250), consumer_b.next()).await {
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          consumer_b.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_b.commit().await?;
          if id == after_id {
            return Ok::<_, anyhow::Error>((record.virtual_partition_id, record.offset));
          }
        },
        Ok(Err(error)) => return Err(error),
        Err(_) => {},
      }
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("replacement did not deliver post-fencing work"))??;
  assert_eq!(after_delivery.0, before_delivery.0);

  let final_lease = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == after_delivery.0)
    .ok_or_else(|| anyhow::anyhow!("missing replacement lease after post-fencing delivery"))?;
  assert!(
    final_lease.owner_id == "fit-010-b"
      && final_lease.generation > owner_a_lease.generation
      && final_lease.committed_cursor.as_ref().is_some_and(|cursor| {
        cursor.seq_end >= after_delivery.1 && cursor.source_checkpoint.is_some()
      }),
    "replacement did not retain a durable post-fencing cursor: {final_lease:?}"
  );

  consumer_a.abort_for_test().await?;
  resources
    .store_fault_controller()
    .disable_fault(membership_fault_id)
    .await;
  Box::new(consumer_b).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates a failed final shutdown checkpoint releases ownership and preserves the
// staged record for exactly-once replacement delivery.
#[tokio::test]
async fn graceful_shutdown_final_commit_failure_redelivers_staged_record() -> Result<()> {
  let consumer_time = Arc::new(ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::in_memory(1)
    .partition_count(1)
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(
      producer_config(),
      vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
    )
    .await?;
  let runtime_a = consumer_runtime_config("fit-shutdown-commit-a");
  let runtime_b = consumer_runtime_config("fit-shutdown-commit-b");
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("shutdown commit group config missing"))?;
  let group_id = group.group_id.to_string();
  let topic = group.topic.to_string();
  let hooks = cluster.lifecycle_hooks();
  let staged_id = "fit-shutdown-commit-staged";
  let key = b"fit-shutdown-commit-key".to_vec();
  let staged_ack = produce_message(&producer, key.clone(), staged_id).await?;

  let mut consumer_a = start_consumer_after_rebalance(
    &cluster,
    &runtime_a,
    "fit-shutdown-commit-a",
    &consumer_time,
  )
  .await?;
  let staged_offset = timeout(Duration::from_secs(5), async {
    loop {
      match timeout(Duration::from_millis(250), consumer_a.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow::anyhow!("initial consumer next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          assert_eq!(record.virtual_partition_id, staged_ack.virtual_partition_id);
          assert_eq!(
            String::from_utf8(record.record.payload.to_vec())?,
            staged_id
          );
          consumer_a.store_offset(record.virtual_partition_id, record.offset)?;
          return Ok::<_, anyhow::Error>(record.offset);
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("initial consumer did not stage the shutdown record"))??;

  let lease_before_shutdown = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == staged_ack.virtual_partition_id)
    .ok_or_else(|| anyhow::anyhow!("missing initial shutdown lease"))?;
  assert!(
    lease_before_shutdown.committed_cursor.is_none(),
    "staged record became durable before shutdown: {lease_before_shutdown:?}"
  );

  let controller = cluster
    .store_fault_controller()
    .ok_or_else(|| anyhow::anyhow!("in-memory cluster missing store fault controller"))?;
  controller
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerCommitCursor,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "fail shutdown final cursor commit".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;

  let mut before_commit = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerBeforeCommit,
      "fit-shutdown-commit-a",
      None,
      None,
    )
    .await?;
  let mut commit_finished = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerShutdownCommitFinished,
      "fit-shutdown-commit-a",
      None,
      None,
    )
    .await?;
  let mut before_release = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerBeforeReleaseOwned,
      "fit-shutdown-commit-a",
      None,
      None,
    )
    .await?;
  let mut before_deregister = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerBeforeDeregisterMember,
      "fit-shutdown-commit-a",
      None,
      None,
    )
    .await?;
  let shutdown_task = tokio::spawn(async move { Box::new(consumer_a).shutdown().await });

  timeout(Duration::from_secs(5), before_commit.wait_until_reached())
    .await
    .map_err(|_| anyhow::anyhow!("shutdown did not reach the final commit boundary"))??;
  before_commit.release()?;
  timeout(Duration::from_secs(5), commit_finished.wait_until_reached())
    .await
    .map_err(|_| anyhow::anyhow!("shutdown did not complete its failed final commit"))??;
  let lease_after_failed_commit = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == staged_ack.virtual_partition_id)
    .ok_or_else(|| anyhow::anyhow!("missing lease after failed final commit"))?;
  assert!(
    lease_after_failed_commit.committed_cursor.is_none(),
    "failed shutdown commit advanced the durable cursor: {lease_after_failed_commit:?}"
  );
  commit_finished.release()?;

  timeout(Duration::from_secs(5), before_release.wait_until_reached())
    .await
    .map_err(|_| {
      anyhow::anyhow!("shutdown did not continue to lease release after commit failure")
    })??;
  let lease_before_release = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == staged_ack.virtual_partition_id)
    .ok_or_else(|| anyhow::anyhow!("missing lease before shutdown release"))?;
  assert_eq!(lease_before_release.owner_id, "fit-shutdown-commit-a");
  assert!(lease_before_release.committed_cursor.is_none());
  before_release.release()?;

  timeout(
    Duration::from_secs(5),
    before_deregister.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow::anyhow!("shutdown did not continue to deregistration after release"))??;
  let logical_now_ms = i64::try_from(consumer_time.now().unix_timestamp_nanos() / 1_000_000)?;
  let released_lease = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == staged_ack.virtual_partition_id)
    .ok_or_else(|| anyhow::anyhow!("missing released lease after failed final commit"))?;
  assert!(
    released_lease.lease_expiration_ts_ms <= logical_now_ms,
    "shutdown did not release the lease after final commit failure: {released_lease:?}"
  );
  before_deregister.release()?;
  let shutdown_error = shutdown_task
    .await
    .map_err(|error| anyhow::anyhow!("shutdown task failed: {error}"))?
    .expect_err("shutdown must return the failed final checkpoint");
  assert!(
    shutdown_error
      .to_string()
      .contains("fail shutdown final cursor commit"),
    "shutdown returned an unexpected final checkpoint error: {shutdown_error:#}"
  );
  assert_eq!(
    controller
      .events()
      .await
      .iter()
      .filter(|event| {
        event.operation == StoreFaultOperation::ConsumerCommitCursor && event.action.is_some()
      })
      .count(),
    1,
    "shutdown did not consume exactly one final-checkpoint fault"
  );
  assert!(
    cluster
      .consumer_membership_store()
      .list_active_members(&topic, &group_id, logical_now_ms)
      .await?
      .is_empty(),
    "shutdown must deregister the member despite a failed final checkpoint"
  );

  let mut consumer_b = start_consumer_after_rebalance(
    &cluster,
    &runtime_b,
    "fit-shutdown-commit-b",
    &consumer_time,
  )
  .await?;
  wait_for_member_to_own_group_leases(
    &cluster,
    &group_id,
    "fit-shutdown-commit-b",
    1,
    "replacement did not acquire the released lease",
  )
  .await?;
  let marker_id = "fit-shutdown-commit-marker";
  let marker_ack = produce_message(&producer, key, marker_id).await?;
  assert_eq!(
    marker_ack.virtual_partition_id,
    staged_ack.virtual_partition_id
  );

  let mut replacement_counts = HashMap::new();
  let marker_offset = timeout(Duration::from_secs(5), async {
    loop {
      match timeout(Duration::from_millis(250), consumer_b.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow::anyhow!("replacement consumer next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          *replacement_counts.entry(id.clone()).or_insert(0_usize) += 1;
          if id != staged_id && id != marker_id {
            return Err(anyhow::anyhow!(
              "replacement delivered unexpected record: {id}"
            ));
          }
          if id == staged_id {
            assert_eq!(record.offset, staged_offset);
          }
          consumer_b.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_b.commit().await?;
          if id == marker_id {
            if replacement_counts.get(staged_id) != Some(&1) {
              return Err(anyhow::anyhow!(
                "replacement reached marker before exactly one staged-record recovery: \
                 {replacement_counts:?}"
              ));
            }
            return Ok::<_, anyhow::Error>(record.offset);
          }
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("replacement did not drain the staged recovery and marker"))??;
  assert_eq!(
    replacement_counts,
    HashMap::from([(staged_id.to_string(), 1), (marker_id.to_string(), 1)]),
    "replacement must deliver the failed-checkpoint record exactly once: {replacement_counts:?}"
  );

  let replacement_lease = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == marker_ack.virtual_partition_id)
    .ok_or_else(|| anyhow::anyhow!("missing replacement lease"))?;
  assert!(
    replacement_lease.owner_id == "fit-shutdown-commit-b"
      && replacement_lease
        .committed_cursor
        .as_ref()
        .is_some_and(|cursor| {
          cursor.seq_end >= marker_offset && cursor.source_checkpoint.is_some()
        }),
    "replacement did not durably checkpoint recovered work: {replacement_lease:?}"
  );

  Box::new(consumer_b).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: validates a graceful shutdown release failure retains only the failed lease and
// prevents a replacement from acquiring it until the original lease expires in logical time.
#[tokio::test]
async fn graceful_shutdown_release_fault_fences_replacement_until_expiry() -> Result<()> {
  let consumer_time = Arc::new(ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::in_memory(1)
    .partition_count(1)
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let runtime_a = consumer_runtime_config("fit-015-release-a");
  let runtime_b = consumer_runtime_config("fit-015-release-b");
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("release-fault consumer group config missing"))?;
  let group_id = group.group_id.to_string();
  let topic = group.topic.to_string();
  let hooks = cluster.lifecycle_hooks();

  let consumer_a =
    start_consumer_after_rebalance(&cluster, &runtime_a, "fit-015-release-a", &consumer_time)
      .await?;
  wait_for_member_to_own_group_leases(
    &cluster,
    &group_id,
    "fit-015-release-a",
    1,
    "initial member did not acquire all group leases",
  )
  .await?;

  let controller = cluster
    .store_fault_controller()
    .ok_or_else(|| anyhow::anyhow!("in-memory cluster missing store fault controller"))?;
  controller
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerReleasePartition,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "release shutdown lease".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;

  let mut before_release = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerBeforeReleaseOwned,
      "fit-015-release-a",
      None,
      None,
    )
    .await?;
  let shutdown_task = tokio::spawn(async move { Box::new(consumer_a).shutdown().await });
  timeout(Duration::from_secs(5), before_release.wait_until_reached())
    .await
    .map_err(|_| anyhow::anyhow!("shutdown did not reach the release boundary"))??;

  let leases_before_release = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?;
  assert!(
    leases_before_release
      .iter()
      .all(|lease| lease.owner_id == "fit-015-release-a"),
    "release began before the shutdown boundary was released: {leases_before_release:?}"
  );
  before_release.release()?;
  let shutdown_error = shutdown_task
    .await
    .map_err(|error| anyhow::anyhow!("release-fault shutdown task failed: {error}"))?
    .expect_err("shutdown must report the injected release failure");
  assert!(
    shutdown_error
      .to_string()
      .contains("release shutdown lease"),
    "shutdown returned an unexpected release failure: {shutdown_error:#}"
  );

  let logical_now_ms = i64::try_from(consumer_time.now().unix_timestamp_nanos() / 1_000_000)?;
  let residual_leases = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?;
  let held_leases = residual_leases
    .iter()
    .filter(|lease| {
      lease.owner_id == "fit-015-release-a" && lease.lease_expiration_ts_ms > logical_now_ms
    })
    .collect::<Vec<_>>();
  assert_eq!(
    held_leases.len(),
    1,
    "one-shot release failure must retain exactly one unexpired A lease: {residual_leases:?}"
  );
  assert!(
    cluster
      .consumer_membership_store()
      .list_active_members(&topic, &group_id, logical_now_ms)
      .await?
      .is_empty(),
    "successful deregistration must remove A despite its release failure"
  );
  assert_eq!(
    controller
      .events()
      .await
      .iter()
      .filter(|event| {
        event.operation == StoreFaultOperation::ConsumerReleasePartition && event.action.is_some()
      })
      .count(),
    1,
    "shutdown did not consume exactly one release fault"
  );

  let mut consumer_b =
    start_consumer_after_rebalance(&cluster, &runtime_b, "fit-015-release-b", &consumer_time)
      .await?;
  let held_partition = held_leases[0].key.virtual_partition_id;
  let pre_expiry_lease = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == held_partition)
    .ok_or_else(|| anyhow::anyhow!("missing held lease before logical expiry"))?;
  assert_eq!(
    pre_expiry_lease.owner_id, "fit-015-release-a",
    "replacement acquired the failed-release lease before logical expiry: {pre_expiry_lease:?}"
  );

  let mut post_expiry_rebalance = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerBeforeRebalance,
      "fit-015-release-b",
      None,
      None,
    )
    .await?;
  consumer_time.advance(TimeDuration::seconds(3));
  consumer_b.commit().await?;
  timeout(
    Duration::from_secs(5),
    post_expiry_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow::anyhow!("replacement did not begin a post-expiry rebalance"))??;
  post_expiry_rebalance.release()?;
  wait_for_member_to_own_group_leases(
    &cluster,
    &group_id,
    "fit-015-release-b",
    1,
    "replacement did not recover the failed-release lease after logical expiry",
  )
  .await?;
  Box::new(consumer_b).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: validates a graceful shutdown deregistration failure preserves the active member
// until logical expiry, so a replacement cannot take the whole group prematurely.
#[tokio::test]
async fn graceful_shutdown_deregistration_fault_requires_expiry_before_full_replacement_ownership()
-> Result<()> {
  let consumer_time = Arc::new(ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::in_memory(1)
    .partition_count(1)
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let runtime_a = consumer_runtime_config("fit-015-deregister-a");
  let runtime_b = consumer_runtime_config("fit-015-deregister-b");
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("deregistration-fault consumer group config missing"))?;
  let group_id = group.group_id.to_string();
  let topic = group.topic.to_string();
  let hooks = cluster.lifecycle_hooks();

  let consumer_a =
    start_consumer_after_rebalance(&cluster, &runtime_a, "fit-015-deregister-a", &consumer_time)
      .await?;
  wait_for_member_to_own_group_leases(
    &cluster,
    &group_id,
    "fit-015-deregister-a",
    1,
    "initial member did not acquire all group leases",
  )
  .await?;

  let controller = cluster
    .store_fault_controller()
    .ok_or_else(|| anyhow::anyhow!("in-memory cluster missing store fault controller"))?;
  controller
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerDeregisterMember,
      key_pattern: Some(format!("{topic}#{group_id}#fit-015-deregister-a")),
      action: StoreFaultAction::Fail {
        message: "deregister shutdown member".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;

  let mut before_deregister = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerBeforeDeregisterMember,
      "fit-015-deregister-a",
      None,
      None,
    )
    .await?;
  let shutdown_task = tokio::spawn(async move { Box::new(consumer_a).shutdown().await });
  timeout(
    Duration::from_secs(5),
    before_deregister.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow::anyhow!("shutdown did not reach the deregistration boundary"))??;

  let logical_now_ms = i64::try_from(consumer_time.now().unix_timestamp_nanos() / 1_000_000)?;
  let leases_before_deregister = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?;
  assert!(
    leases_before_deregister
      .iter()
      .all(|lease| lease.lease_expiration_ts_ms <= logical_now_ms),
    "all leases must release before deregistration begins: {leases_before_deregister:?}"
  );
  before_deregister.release()?;
  shutdown_task
    .await
    .map_err(|error| anyhow::anyhow!("deregistration-fault shutdown task failed: {error}"))??;

  assert_eq!(
    member_ids(
      &cluster
        .consumer_membership_store()
        .list_active_members(&topic, &group_id, logical_now_ms)
        .await?,
    ),
    vec!["fit-015-deregister-a".to_string()],
    "one-shot deregistration failure must leave A active until its lease expires"
  );
  assert_eq!(
    controller
      .events()
      .await
      .iter()
      .filter(|event| {
        event.operation == StoreFaultOperation::ConsumerDeregisterMember && event.action.is_some()
      })
      .count(),
    1,
    "shutdown did not consume exactly one deregistration fault"
  );

  let consumer_b =
    start_consumer_after_rebalance(&cluster, &runtime_b, "fit-015-deregister-b", &consumer_time)
      .await?;
  let members_before_expiry = cluster
    .consumer_membership_store()
    .list_active_members(&topic, &group_id, logical_now_ms)
    .await?;
  assert_eq!(
    member_ids(&members_before_expiry),
    vec![
      "fit-015-deregister-a".to_string(),
      "fit-015-deregister-b".to_string(),
    ],
    "replacement must observe the residual member before logical expiry"
  );
  let leases_before_expiry = cluster
    .consumer_lease_store()
    .list_group_leases(&topic, &group_id)
    .await?;
  assert!(
    leases_before_expiry
      .iter()
      .any(|lease| lease.owner_id != "fit-015-deregister-b"),
    "replacement took the complete group while the failed deregistration remained active: \
     {leases_before_expiry:?}"
  );

  consumer_time.advance(TimeDuration::seconds(3));
  wait_for_member_to_own_group_leases(
    &cluster,
    &group_id,
    "fit-015-deregister-b",
    1,
    "replacement did not recover complete group ownership after member expiry",
  )
  .await?;
  Box::new(consumer_b).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: validates bootstrap consumer rebalance converges under transient membership and
// consumer-lease faults while preserving the full consumed record set.
#[tokio::test]
async fn bootstrap_rebalance_with_membership_and_lease_faults() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .in_memory_transport()
    .start()
    .await?;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut runtime_a = consumer_runtime_config("fit-015-a");
  let mut runtime_b = consumer_runtime_config("fit-015-b");
  // Keep the lease horizon above the complete transient-failure script so all seven faults
  // exercise retry convergence rather than escalating into unintended member expiry.
  for runtime in [&mut runtime_a, &mut runtime_b] {
    let group = runtime
      .group
      .as_mut()
      .ok_or_else(|| anyhow::anyhow!("fit-015 consumer group config missing"))?;
    group.lease_duration_ms = Some(10_000);
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("fit-015-a group config missing"))?;
  let mut consumer_a = Box::new(cluster.create_consumer(&runtime_a).await?);
  let mut consumer_b = Box::new(cluster.create_consumer(&runtime_b).await?);

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerPublishAssignmentPlan,
      key_pattern: Some(format!("{TOPIC}#integration-group#fit-015-a")),
      action: StoreFaultAction::Fail {
        message: "transient planner publication failure".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;
  let fault_matcher = TestEventMatcher {
    category: Some("store".to_string()),
    operation: None,
    key_contains: None,
    status: Some("fault_applied".to_string()),
  };
  let expected_fault_counts = HashMap::from([
    ("consumer_publish_assignment_plan".to_string(), 1_usize),
    ("consumer_assign_partition".to_string(), 2_usize),
    ("consumer_heartbeat_partition".to_string(), 2_usize),
    ("consumer_membership_heartbeat".to_string(), 2_usize),
  ]);

  // The planner failure establishes the rebalance retry before the lease and heartbeat failures
  // become eligible. This keeps the recovery path causal rather than scheduler-dependent.
  let hooks = cluster.lifecycle_hooks();
  let mut planner_failure_gate = hooks
    .arm_consumer(
      framework::LifecycleEvent::ConsumerRebalanceFailed,
      "fit-015-a",
      None,
      None,
    )
    .await?;
  consumer_a.start()?;
  consumer_b.start()?;
  let planner_fault = cluster
    .wait_for_event_after(&fault_matcher, None, Duration::from_secs(10))
    .await?;
  assert_eq!(
    planner_fault.operation, "consumer_publish_assignment_plan",
    "expected the planner fault before enabling later recovery faults: {planner_fault:?}"
  );
  timeout(
    Duration::from_secs(5),
    planner_failure_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow::anyhow!("planner failure did not reach the rebalance retry boundary"))??;

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerAssignPartition,
      key_pattern: Some(format!("{TOPIC}#integration-group#8")),
      action: StoreFaultAction::Fail {
        message: "transient assign failure".to_string(),
      },
      remaining_hits: Some(2),
    })
    .await;
  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerHeartbeatPartition,
      key_pattern: Some(format!("{TOPIC}#integration-group#8")),
      action: StoreFaultAction::Fail {
        message: "transient heartbeat failure".to_string(),
      },
      remaining_hits: Some(2),
    })
    .await;
  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerMembershipHeartbeat,
      key_pattern: Some(format!("{TOPIC}#integration-group#fit-015-a")),
      action: StoreFaultAction::Fail {
        message: "transient membership heartbeat failure".to_string(),
      },
      remaining_hits: Some(2),
    })
    .await;
  planner_failure_gate.release()?;

  let mut observed_fault_counts =
    HashMap::from([("consumer_publish_assignment_plan".to_string(), 1_usize)]);
  let mut previous_fault_sequence = Some(planner_fault.sequence);
  let mut saw_revocation = false;
  while observed_fault_counts != expected_fault_counts {
    let fault_wait = cluster.wait_for_event_after(
      &fault_matcher,
      previous_fault_sequence,
      Duration::from_secs(10),
    );
    tokio::pin!(fault_wait);
    let fault = tokio::select! {
      fault = &mut fault_wait => fault?,
      next_result = consumer_a.next() => {
        let NextResult::Revoked(revoked) = next_result? else {
          return Err(anyhow::anyhow!(
            "consumer A delivered a record before the fault script completed"
          ));
        };
        saw_revocation = true;
        revoked.complete().await;
        continue;
      },
      next_result = consumer_b.next() => {
        let NextResult::Revoked(revoked) = next_result? else {
          return Err(anyhow::anyhow!(
            "consumer B delivered a record before the fault script completed"
          ));
        };
        saw_revocation = true;
        revoked.complete().await;
        continue;
      },
    };
    let Some(expected_count) = expected_fault_counts.get(&fault.operation) else {
      return Err(anyhow::anyhow!(
        "unexpected store fault during consumer recovery: {fault:?}"
      ));
    };
    let observed_count = observed_fault_counts
      .entry(fault.operation.clone())
      .or_insert(0_usize);
    *observed_count += 1;
    assert!(
      *observed_count <= *expected_count,
      "consumer fault exceeded its scripted budget: {fault:?}"
    );
    previous_fault_sequence = Some(fault.sequence);
  }

  let mut expected_ids = HashSet::new();
  for message_id in 0 .. 24 {
    let id = format!("it-015-{message_id}");
    let _ack = produce_message(
      &producer,
      format!("it-015-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  let mut delivered_id_counts = HashMap::new();
  let mut max_offsets = HashMap::<u32, u64>::new();
  timeout(Duration::from_secs(12), async {
    while delivered_id_counts.len() < expected_ids.len() {
      let (consumer, next_result) = tokio::select! {
        result = consumer_a.next() => (&mut consumer_a, result),
        result = consumer_b.next() => (&mut consumer_b, result),
      };

      match next_result? {
        NextResult::Revoked(revoked) => {
          saw_revocation = true;
          revoked.complete().await;
        },
        NextResult::Record(record) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          max_offsets
            .entry(record.virtual_partition_id)
            .and_modify(|offset| *offset = (*offset).max(record.offset))
            .or_insert(record.offset);
          *delivered_id_counts.entry(id).or_insert(0_usize) += 1;

          consumer.store_offset(record.virtual_partition_id, record.offset)?;
          consumer.commit().await?;
        },
      }
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| {
    anyhow::anyhow!(
      "faulted consumers did not drain all records: expected={}, consumed={}",
      expected_ids.len(),
      delivered_id_counts.len()
    )
  })??;
  let delivered_ids = delivered_id_counts.keys().cloned().collect::<HashSet<_>>();
  assert_eq!(delivered_ids, expected_ids);
  assert!(
    delivered_id_counts.values().all(|count| *count >= 1),
    "faulted consumer delivery lost an expected ID: {delivered_id_counts:?}"
  );

  let fault_events = resources.store_fault_controller().events().await;
  let fault_count = |operation| {
    fault_events
      .iter()
      .filter(|event| event.operation == operation && event.action.is_some())
      .count()
  };
  assert_eq!(
    fault_count(StoreFaultOperation::ConsumerPublishAssignmentPlan),
    1,
    "unexpected planner fault trace: {fault_events:?}"
  );
  assert_eq!(
    fault_count(StoreFaultOperation::ConsumerAssignPartition),
    2,
    "unexpected assignment fault trace: {fault_events:?}"
  );
  assert_eq!(
    fault_count(StoreFaultOperation::ConsumerHeartbeatPartition),
    2,
    "unexpected partition-heartbeat fault trace: {fault_events:?}"
  );
  assert_eq!(
    fault_count(StoreFaultOperation::ConsumerMembershipHeartbeat),
    2,
    "unexpected membership-heartbeat fault trace: {fault_events:?}"
  );

  let event_trace = cluster.event_log().snapshot().await;
  assert!(
    event_trace.iter().all(|event| {
      event.status != "fault_applied"
        || matches!(
          event.operation.as_str(),
          "consumer_publish_assignment_plan"
            | "consumer_assign_partition"
            | "consumer_heartbeat_partition"
            | "consumer_membership_heartbeat"
        )
    }),
    "unexpected fault after planner phase: {event_trace:?}"
  );

  wait_for_group_offsets_committed(
    &cluster,
    group.group_id.as_str(),
    &max_offsets,
    "faulted consumers did not durably commit every observed delivery",
  )
  .await?;

  let leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  let active_members = cluster
    .consumer_membership_store()
    .list_active_members(
      group.topic.as_str(),
      group.group_id.as_str(),
      OffsetDateTime::now_utc().unix_timestamp() * 1_000,
    )
    .await?;
  assert_eq!(leases.len(), PARTITION_COUNT as usize);
  assert!(
    leases.iter().all(|lease| active_members
      .iter()
      .any(|member| member.member_id == lease.owner_id)),
    "faulted consumer group has leases outside active membership: members={active_members:?}, \
     leases={leases:?}"
  );
  assert!(
    leases.iter().any(|lease| lease.committed_cursor.is_some()),
    "faulted consumer group did not durably commit any consumed records"
  );
  for (virtual_partition_id, offset) in max_offsets {
    let lease = leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == virtual_partition_id)
      .ok_or_else(|| {
        anyhow::anyhow!(
          "missing final lease for consumed partition {virtual_partition_id}: {leases:?}"
        )
      })?;
    assert!(
      lease
        .committed_cursor
        .as_ref()
        .is_some_and(|cursor| { cursor.seq_end >= offset && cursor.source_checkpoint.is_some() }),
      "faulted recovery did not retain the committed cursor for partition {virtual_partition_id}: \
       {lease:?}"
    );
  }

  let _ = consumer_a.shutdown().await;
  let _ = consumer_b.shutdown().await;
  cluster.shutdown().await;
  resources.cleanup().await;
  assert!(
    saw_revocation,
    "expected at least one revocation while rebalancing under transient faults"
  );
  Ok(())
}

// High-level: validates producer publication retries through combined transport and metadata
// faults, then uses a direct reader solely to verify durable visibility.
#[tokio::test]
async fn combined_network_and_metadata_faults_preserve_producer_publication() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .start()
    .await?;

  let network_controller = cluster
    .network_fault_controller()
    .expect("in-memory transport should expose a fault controller");
  network_controller
    .enable_fault(NetworkFaultRule {
      target_node_id: None,
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::Drop,
      remaining_hits: Some(1),
    })
    .await;

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Metadata,
      operation: StoreFaultOperation::MetadataWriteSegment,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "scripted metadata write failure".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;

  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(producer_config(), vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;

  let mut expected_ids = HashSet::new();
  let mut produced_partitions = HashSet::new();
  let first_id = "fit-011-0";
  let first_produce = producer.produce(ProducerRecord::new(
    TOPIC.into(),
    b"fit-011-key-0".to_vec(),
    first_id.as_bytes().to_vec().into(),
    framework::now_unix_seconds() * 1_000,
  ));
  tokio::pin!(first_produce);
  let transport_fault_matcher = TestEventMatcher {
    category: Some("transport".to_string()),
    operation: Some("produce_batch".to_string()),
    key_contains: None,
    status: Some("fault_applied".to_string()),
  };
  let transport_fault = timeout(Duration::from_secs(5), async {
    tokio::select! {
      event = cluster.wait_for_event_after(
        &transport_fault_matcher,
        None,
        Duration::from_secs(5),
      ) => event,
      result = &mut first_produce => Err(anyhow::anyhow!(
        "first produce completed before the scripted transport fault: {result:?}"
      )),
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("producer did not issue its scripted transport request"))??;

  let metadata_fault_matcher = TestEventMatcher {
    category: Some("store".to_string()),
    operation: Some("metadata_write_segment".to_string()),
    key_contains: None,
    status: Some("fault_applied".to_string()),
  };
  let metadata_fault = cluster
    .wait_for_event_after(&metadata_fault_matcher, None, Duration::from_secs(5))
    .await?;
  timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
    .await
    .map_err(|_| anyhow::anyhow!("producer did not enter combined-fault retry backoff"))?;
  assert!(
    retry_clock.advance_to_next_sleep(),
    "combined-fault retry backoff was not registered"
  );
  let first_ack = timeout(Duration::from_secs(5), &mut first_produce)
    .await
    .map_err(|_| anyhow::anyhow!("producer did not recover from scripted combined faults"))??;
  assert!(
    first_ack.attempts > 1,
    "scripted combined faults must cause at least one retry"
  );
  assert!(
    metadata_fault.sequence > transport_fault.sequence,
    "metadata fault must follow the scripted transport fault"
  );
  produced_partitions.insert(first_ack.virtual_partition_id);
  expected_ids.insert(first_id.to_string());

  for message_id in 1 .. 48 {
    let id = format!("fit-011-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-011-key-{}", message_id % 16).into_bytes(),
      &id,
    )
    .await?;
    assert_eq!(
      ack.attempts, 1,
      "recovery produces must not retry after the scripted fault budget is consumed"
    );
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      ..Default::default()
    },
    produced_partitions.into_iter().collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let deliveries = drain_reader_until_with_trace(
    &mut reader,
    expected_ids.len(),
    framework::now_unix_seconds().saturating_add(3),
    Instant::now() + Duration::from_secs(45),
  )
  .await?;
  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "combined producer faults must be read exactly once: {deliveries:?}"
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates a scripted transport-fault trace is stable across identical runs.
#[tokio::test]
async fn scripted_transport_faults_have_stable_event_trace() -> Result<()> {
  let first = run_scripted_transport_fault_scenario().await?;
  let second = run_scripted_transport_fault_scenario().await?;

  assert_eq!(
    first.normalized_transport_trace, second.normalized_transport_trace,
    "normalized transport event traces diverged across identical scripted runs"
  );
  assert_eq!(
    first.total_produced, second.total_produced,
    "terminal produced counts diverged"
  );
  assert_eq!(
    first.total_consumed, second.total_consumed,
    "terminal consumed counts diverged"
  );
  assert_eq!(
    first.max_attempts, second.max_attempts,
    "terminal retry profile diverged"
  );

  Ok(())
}

struct Fit012Outcome {
  normalized_transport_trace: Vec<String>,
  total_produced: usize,
  total_consumed: usize,
  max_attempts: u32,
}

async fn run_scripted_transport_fault_scenario() -> Result<Fit012Outcome> {
  let mut cluster = ClusterHarness::in_memory(2).start().await?;

  let controller = cluster
    .network_fault_controller()
    .expect("in-memory transport should expose a fault controller");
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: None,
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::Drop,
      remaining_hits: Some(2),
    })
    .await;

  let retry_clock = Arc::new(ManualProducerRetryClock::new(Instant::now()));
  let producer = cluster
    .producer_builder(producer_config(), vec![producer_topic()])
    .retry_clock(retry_clock.clone())
    .build()
    .await?;

  let mut expected_ids = HashSet::new();
  let first_id = "fit-012-0";
  let first_produce = producer.produce(ProducerRecord::new(
    TOPIC.into(),
    b"fit-012-key-0".to_vec(),
    first_id.as_bytes().to_vec().into(),
    framework::now_unix_seconds() * 1_000,
  ));
  tokio::pin!(first_produce);
  let fault_matcher = TestEventMatcher {
    category: Some("transport".to_string()),
    operation: Some("produce_batch".to_string()),
    key_contains: None,
    status: Some("fault_applied".to_string()),
  };
  let _first_fault = timeout(Duration::from_secs(5), async {
    tokio::select! {
      event = cluster.wait_for_event_after(&fault_matcher, None, Duration::from_secs(5)) => event,
      result = &mut first_produce => Err(anyhow::anyhow!(
        "first produce completed before the first scripted transport fault: {result:?}"
      )),
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("producer did not issue its first scripted transport request"))??;
  timeout(Duration::from_secs(5), retry_clock.wait_until_sleeping())
    .await
    .map_err(|_| anyhow::anyhow!("producer did not enter its first scripted retry backoff"))?;
  assert!(
    retry_clock.advance_to_next_sleep(),
    "first scripted retry backoff was not registered"
  );

  let first_ack = timeout(Duration::from_secs(5), &mut first_produce)
    .await
    .map_err(|_| anyhow::anyhow!("producer did not recover after scripted transport faults"))??;
  assert!(
    first_ack.attempts > 1,
    "scripted transport fault must cause at least one retry"
  );
  expected_ids.insert(first_id.to_string());

  let mut max_attempts = first_ack.attempts;
  for message_id in 1 .. 24 {
    let id = format!("fit-012-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-012-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    max_attempts = max(max_attempts, ack.attempts);
    expected_ids.insert(id);
  }

  let mut runtime = consumer_runtime_config("fit-012-member");
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow::anyhow!("scripted trace consumer read config missing"))?
    .metadata_visibility_delay_ms = Some(0);
  let group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("scripted trace consumer group config missing"))?;
  let mut consumer = cluster.create_consumer(&runtime).await?;
  consumer.start()?;
  let mut delivery_counts = HashMap::new();
  let mut maximum_offsets = HashMap::<u32, u64>::new();
  timeout(Duration::from_secs(5), async {
    while delivery_counts.len() < expected_ids.len() {
      match consumer.next().await? {
        NextResult::Revoked(revoked) => revoked.complete().await,
        NextResult::Record(record) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          *delivery_counts.entry(id).or_insert(0usize) += 1;
          maximum_offsets
            .entry(record.virtual_partition_id)
            .and_modify(|offset| *offset = (*offset).max(record.offset))
            .or_insert(record.offset);
          consumer.store_offset(record.virtual_partition_id, record.offset)?;
        },
      }
    }
    consumer.commit().await?;
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow::anyhow!("group consumer did not drain scripted transport records"))??;
  assert_eq!(
    delivery_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivery_counts.values().all(|count| *count == 1),
    "scripted transport fault duplicated group delivery: {delivery_counts:?}"
  );

  let leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  for (partition_id, maximum_offset) in maximum_offsets {
    let lease = leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == partition_id)
      .ok_or_else(|| {
        anyhow::anyhow!("missing scripted transport lease for partition {partition_id}")
      })?;
    assert!(
      lease.committed_cursor.as_ref().is_some_and(|cursor| {
        cursor.seq_end >= maximum_offset && cursor.source_checkpoint.is_some()
      }),
      "scripted transport consumer did not durably commit partition {partition_id}: {lease:?}"
    );
  }

  let mut trace: Vec<String> = cluster
    .event_log()
    .snapshot()
    .await
    .into_iter()
    .filter(|event| event.category == "transport" && event.operation == "produce_batch")
    .map(|event| {
      format!(
        "{}|{}|{}|{}",
        event.category,
        event.operation,
        event.status,
        event.key.unwrap_or_default()
      )
    })
    .collect();
  trace.sort_unstable();

  let outcome = Fit012Outcome {
    normalized_transport_trace: trace,
    total_produced: expected_ids.len(),
    total_consumed: delivery_counts.len(),
    max_attempts,
  };

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  Ok(outcome)
}
