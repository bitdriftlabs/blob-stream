use anyhow::Result;
use bd_server_stats::stats::Collector;
use bd_time::TimeProvider;
use blob_stream_consumer::{
  ConsumerIterator,
  ConsumerReadConfig,
  ConsumerReaderImpl,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  NextResult,
};
use blob_stream_integration_tests::test_framework as framework;
use blob_stream_producer::{ProducerClient, ProducerError, ProducerRecord};
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
  reader_delivery_counts,
  rescan_reader_with_trace,
};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::time::{Instant, timeout};

fn metrics_scope(component: &str) -> bd_server_stats::stats::Scope {
  Collector::default().scope(component)
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
    retry_clock.advance_to_next_sleep().await,
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

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
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
    retry_clock.advance_to_next_sleep().await,
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
  let first_post_id = "fit-003-post-reroute-12";
  let first_post_produce = producer.produce(ProducerRecord::new(
    TOPIC.into(),
    b"fit-003-key-post-12".to_vec(),
    first_post_id.as_bytes().to_vec().into(),
    framework::now_unix_seconds() * 1_000,
  ));
  tokio::pin!(first_post_produce);
  let first_post_ack = timeout(Duration::from_secs(5), async {
    tokio::select! {
      result = &mut first_post_produce => result,
      () = retry_clock.wait_until_sleeping() => {
        assert!(
          retry_clock.advance_to_next_sleep().await,
          "membership-settlement retry was not registered"
        );
        (&mut first_post_produce).await
      }
    }
  })
  .await
  .map_err(|_| anyhow::anyhow!("producer did not reroute after active-broker switch"))??;
  expected_ids.insert(first_post_id.to_string());
  produced_partitions.insert(first_post_ack.virtual_partition_id);

  for message_id in 13 .. 36 {
    let id = format!("fit-003-post-reroute-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-003-key-post-{message_id}").into_bytes(),
      &id,
    )
    .await?;
    assert_eq!(
      ack.attempts, 1,
      "post-reroute request must not retry after producer routes settle"
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
    retry_clock.advance_to_next_sleep().await,
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
    retry_clock.advance_to_next_sleep().await,
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
      retry_clock.advance_to_next_sleep().await,
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
    retry_clock.advance_to_next_sleep().await,
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
    retry_clock.advance_to_next_sleep().await,
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
  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
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
    let ack = produce_message(
      &producer,
      format!("fit-009-key-pre-{message_id}").into_bytes(),
      &id,
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

  // Deterministically transition traffic to standby once the complete lease-conflict script has
  // been applied.
  cluster.set_active_nodes(vec![standby_node.clone()]);

  for message_id in 16 .. 40 {
    let id = format!("fit-009-post-reroute-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-009-key-post-{message_id}").into_bytes(),
      &id,
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

// High-level: validates a live owner is fenced after heartbeat failures and its replacement
// recovers normal consumer delivery and cursor commits.
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
  let mut before_prefetch_gate = hooks
    .arm(framework::LifecycleEvent::ConsumerPrefetchBatchBuffered)
    .await?;
  let mut consumer_a = cluster.create_consumer(&runtime_a).await?;
  consumer_a.start()?;
  timeout(Duration::from_secs(5), consumer_time.wait_until_sleeping(2))
    .await
    .map_err(|_| anyhow::anyhow!("initial owner did not park its driver and prefetch worker"))?;

  let key = b"fit-010-key".to_vec();
  let before_id = "fit-010-before-failure";
  let before_ack = produce_message(&producer, key.clone(), before_id).await?;
  consumer_time.advance(TimeDuration::seconds(1));
  tokio::task::yield_now().await;
  timeout(
    Duration::from_secs(5),
    before_prefetch_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow::anyhow!("initial owner did not prefetch its pre-failure record"))??;
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
      operation: StoreFaultOperation::ConsumerHeartbeatPartition,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "fault active owner partition heartbeat".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;
  assert!(
    consumer_a.commit().await.is_err(),
    "expected active owner heartbeat to fail"
  );
  let heartbeat_fault_events = resources
    .store_fault_controller()
    .events()
    .await
    .into_iter()
    .filter(|event| {
      event.operation == StoreFaultOperation::ConsumerHeartbeatPartition && event.action.is_some()
    })
    .count();
  assert_eq!(
    heartbeat_fault_events, 1,
    "missing active-owner heartbeat fault"
  );

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
  assert!(
    consumer_a.commit().await.is_err(),
    "expected active owner membership heartbeat to fail after partition heartbeat failure"
  );

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
  assert_eq!(active_members, vec!["fit-010-b".to_string()]);
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

// High-level: validates bootstrap consumer rebalance converges under transient membership and
// consumer-lease faults while preserving the full consumed record set.
#[tokio::test]
async fn bootstrap_rebalance_with_membership_and_lease_faults() -> Result<()> {
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

  let runtime_a = consumer_runtime_config("fit-015-a");
  let runtime_b = consumer_runtime_config("fit-015-b");
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
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "transient planner publication failure".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;
  consumer_a.start()?;
  consumer_b.start()?;

  let planner_fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("consumer_publish_assignment_plan".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(3),
    )
    .await?;

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerAssignPartition,
      key_pattern: None,
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
      key_pattern: None,
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
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "transient membership heartbeat failure".to_string(),
      },
      remaining_hits: Some(2),
    })
    .await;

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

  let mut saw_revocation = false;
  let mut delivered_id_counts = HashMap::new();
  let mut max_offsets = HashMap::<u32, u64>::new();
  timeout(Duration::from_secs(12), async {
    while delivered_id_counts.len() < expected_ids.len() {
      consumer_time.advance(TimeDuration::seconds(1));
      tokio::task::yield_now().await;

      for consumer in [&mut consumer_a, &mut consumer_b] {
        let next = timeout(Duration::from_millis(250), consumer.next()).await;
        let Ok(Ok(next_result)) = next else {
          continue;
        };

        match next_result {
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
        || event.sequence <= planner_fault_event.sequence
        || matches!(
          event.operation.as_str(),
          "consumer_assign_partition"
            | "consumer_heartbeat_partition"
            | "consumer_membership_heartbeat"
        )
    }),
    "unexpected fault after planner phase: {event_trace:?}"
  );

  let leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  let active_members = cluster
    .consumer_membership_store()
    .list_active_members(
      group.topic.as_str(),
      group.group_id.as_str(),
      consumer_time.now().unix_timestamp() * 1_000,
    )
    .await?;
  assert_eq!(leases.len(), PARTITION_COUNT as usize);
  assert!(
    leases
      .iter()
      .all(|lease| active_members.contains(&lease.owner_id)),
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
    retry_clock.advance_to_next_sleep().await,
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
    retry_clock.advance_to_next_sleep().await,
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

  let trace = cluster
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
