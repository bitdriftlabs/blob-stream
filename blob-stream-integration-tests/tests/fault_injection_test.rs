use anyhow::Result;
use bd_server_stats::stats::Collector;
use blob_stream_consumer::{
  ConsumerIterator,
  ConsumerIteratorImpl,
  ConsumerReadConfig,
  ConsumerReaderImpl,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
  MembershipCoordinationSource,
  NextResult,
};
use blob_stream_integration_tests::test_framework as framework;
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
};
use blob_stream_producer::{ProducerClient, ProducerError, ProducerRecord};
use framework::{
  ClusterHarness,
  IntegrationResources,
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
  consumer_runtime_config,
  drain_reader_until,
  produce_message,
  producer_config,
  producer_topic,
};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

fn metrics_scope(component: &str) -> bd_server_stats::stats::Scope {
  Collector::default().scope(component)
}

// High-level: validates producer retry behavior under deterministic dropped transport requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
      remaining_hits: Some(1),
    })
    .await;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  let mut seen_retry = false;
  let mut produced_partitions = HashSet::new();

  for message_id in 0 .. 12 {
    let id = format!("fit-001-{message_id}");
    let key = format!("fit-001-key-{message_id}").into_bytes();
    let ack = produce_message(&producer, key, &id).await?;
    if ack.attempts > 1 {
      seen_retry = true;
    }
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  assert!(
    seen_retry,
    "expected at least one retry after injected drop fault"
  );

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

  let mut consumed_ids = HashSet::new();
  drain_reader_until(
    &mut reader,
    &mut consumed_ids,
    expected_ids.len(),
    Instant::now() + Duration::from_secs(15),
  )
  .await?;

  assert_eq!(consumed_ids, expected_ids);

  let _fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("transport".to_string()),
        operation: Some("produce_batch".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;

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

// High-level: validates that deterministic delay+reorder transport faults preserve data and
// per-partition sequence monotonicity during reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn network_delay_and_reorder_preserves_cursor_monotonicity() -> Result<()> {
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

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  for message_id in 0 .. 48 {
    let id = format!("fit-002-{message_id}");
    produce_message(
      &producer,
      format!("fit-002-key-{}", message_id % 12).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
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

  let mut consumed_ids = HashSet::new();
  let mut last_seq_end_by_partition = HashMap::new();
  let deadline = Instant::now() + Duration::from_secs(45);

  while consumed_ids.len() < expected_ids.len() {
    if Instant::now() >= deadline {
      anyhow::bail!(
        "deadline exceeded while validating delay/reorder monotonicity: expected={}, consumed={}",
        expected_ids.len(),
        consumed_ids.len()
      );
    }

    let batches = reader.read_available(framework::now_unix_seconds()).await?;
    if batches.is_empty() {
      sleep(Duration::from_millis(50)).await;
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

      for record in batch.records {
        let id = String::from_utf8(record.payload.to_vec())?;
        consumed_ids.insert(id);
      }
    }
  }

  assert_eq!(consumed_ids, expected_ids);
  assert!(
    !last_seq_end_by_partition.is_empty(),
    "expected to observe at least one partition cursor"
  );

  let _fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("transport".to_string()),
        operation: Some("produce_batch".to_string()),
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

// High-level: validates active-broker partition fault handling with deterministic reroute to a
// standby broker and no-loss completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

  let controller = cluster
    .network_fault_controller()
    .expect("in-memory transport should expose a fault controller");
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: Some(active_node.node_id.clone()),
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::Partition,
      remaining_hits: Some(2),
    })
    .await;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  let mut seen_retry = false;
  let mut produced_partitions = HashSet::new();

  for message_id in 0 .. 12 {
    let id = format!("fit-003-pre-reroute-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-003-key-pre-{message_id}").into_bytes(),
      &id,
    )
    .await?;
    if ack.attempts > 1 {
      seen_retry = true;
    }
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  // Deterministically reroute producer discovery to the standby node after the active-node
  // partition fault has been exercised.
  cluster.set_active_nodes(vec![standby_node.clone()]);

  for message_id in 12 .. 36 {
    let id = format!("fit-003-post-reroute-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-003-key-post-{message_id}").into_bytes(),
      &id,
    )
    .await?;
    if ack.attempts > 1 {
      seen_retry = true;
    }
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  assert!(
    seen_retry,
    "expected retries while active broker was partitioned before reroute"
  );

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

  let mut consumed_ids = HashSet::new();
  drain_reader_until(
    &mut reader,
    &mut consumed_ids,
    expected_ids.len(),
    Instant::now() + Duration::from_secs(30),
  )
  .await?;

  assert_eq!(consumed_ids, expected_ids);

  let _active_fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("transport".to_string()),
        operation: Some("produce_batch".to_string()),
        key_contains: Some(active_node.node_id.clone()),
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;

  let _standby_ok_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("transport".to_string()),
        operation: Some("produce_batch".to_string()),
        key_contains: Some(standby_node.node_id.clone()),
        status: Some("ok".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates timeout fault retry exhaustion at the configured deadline and verifies
// deterministic recovery once the fault window is consumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_response_timeout_retry_deadline_respected() -> Result<()> {
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
      fault: NetworkFault::Timeout(Duration::from_millis(200)),
      // The 500 ms producer deadline expires while the third timeout is in flight.
      remaining_hits: Some(3),
    })
    .await;

  let mut config = producer_config();
  config.retry_deadline_ms = Some(500);
  let producer = cluster
    .create_producer(config, vec![producer_topic()])
    .await?;

  let exhausted = producer
    .produce(ProducerRecord::new(
      TOPIC,
      b"fit-004-timeout".to_vec(),
      b"fit-004-timeout".to_vec(),
      framework::now_unix_seconds() * 1_000,
    ))
    .await;
  assert!(
    matches!(exhausted, Err(ProducerError::RetriesExhausted(_))),
    "expected retries exhausted after timeout fault, got {exhausted:?}"
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

  let mut consumed_ids = HashSet::new();
  drain_reader_until(
    &mut reader,
    &mut consumed_ids,
    1,
    Instant::now() + Duration::from_secs(15),
  )
  .await?;
  assert!(consumed_ids.contains("fit-004-recovery-message"));

  let _fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("transport".to_string()),
        operation: Some("produce_batch".to_string()),
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

// High-level: validates transient blob put failures recover via retries with no final data loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_put_transient_failures_recover_without_loss() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
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

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  let mut seen_retry = false;
  let mut produced_partitions = HashSet::new();
  for message_id in 0 .. 24 {
    let id = format!("fit-005-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-005-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    if ack.attempts > 1 {
      seen_retry = true;
    }
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  assert!(
    seen_retry,
    "expected retries after transient blob put failures were injected"
  );

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

  let mut consumed_ids = HashSet::new();
  drain_reader_until(
    &mut reader,
    &mut consumed_ids,
    expected_ids.len(),
    Instant::now() + Duration::from_secs(30),
  )
  .await?;
  assert_eq!(consumed_ids, expected_ids);

  let _store_fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("blob_put".to_string()),
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

// High-level: validates transient blob get failures during consume recover via reader re-scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_get_failures_consumer_rescan_recovers() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
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

  let mut consumed_ids = HashSet::new();
  let mut saw_read_error = false;
  let deadline = Instant::now() + Duration::from_secs(45);
  while consumed_ids.len() < expected_ids.len() {
    if Instant::now() >= deadline {
      anyhow::bail!(
        "deadline exceeded while recovering from blob get faults: expected={}, consumed={}",
        expected_ids.len(),
        consumed_ids.len()
      );
    }

    match reader.read_available(framework::now_unix_seconds()).await {
      Ok(batches) if batches.is_empty() => {
        sleep(Duration::from_millis(50)).await;
      },
      Ok(batches) => {
        for batch in batches {
          for record in batch.records {
            let id = String::from_utf8(record.payload.to_vec())?;
            consumed_ids.insert(id);
          }
        }
      },
      Err(_error) => {
        saw_read_error = true;
        sleep(Duration::from_millis(50)).await;
      },
    }
  }

  assert!(
    saw_read_error,
    "expected at least one read error during transient blob get failures"
  );
  assert_eq!(consumed_ids, expected_ids);

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

// High-level: validates producer acknowledgements are only returned once metadata write succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_write_fail_then_retry_ack_semantics() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .in_memory_transport()
    .start()
    .await?;

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Metadata,
      operation: StoreFaultOperation::MetadataWriteSegment,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "transient metadata write failure".to_string(),
      },
      // The 50 ms producer deadline expires after retrying the three immediate failures.
      remaining_hits: Some(3),
    })
    .await;

  let mut config = producer_config();
  config.retry_deadline_ms = Some(50);
  let producer = cluster
    .create_producer(config, vec![producer_topic()])
    .await?;

  let failed_ack = producer
    .produce(ProducerRecord::new(
      TOPIC,
      b"fit-007-failed-key".to_vec(),
      b"fit-007-failed".to_vec(),
      framework::now_unix_seconds() * 1_000,
    ))
    .await;
  assert!(
    failed_ack.is_err(),
    "expected first produce to fail when metadata writes are faulted"
  );

  let success_ack = produce_message(
    &producer,
    b"fit-007-success-key".to_vec(),
    "fit-007-success",
  )
  .await?;

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      ..Default::default()
    },
    vec![success_ack.virtual_partition_id],
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    &metrics_scope("blob_stream_consumer_it"),
    1,
    DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
    None,
  )?;

  let mut consumed_ids = HashSet::new();
  drain_reader_until(
    &mut reader,
    &mut consumed_ids,
    1,
    Instant::now() + Duration::from_secs(15),
  )
  .await?;

  assert!(consumed_ids.contains("fit-007-success"));
  assert!(
    !consumed_ids.contains("fit-007-failed"),
    "failed produce must not have produced a consumable phantom acknowledgement"
  );

  let _fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("metadata_write_segment".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;

  let _success_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("metadata_write_segment".to_string()),
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

// High-level: validates stale metadata scan windows do not regress cursor progress or duplicate
// terminal consumption.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

  let mut consumed_ids = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(45);
  while consumed_ids.len() < expected_ids.len() {
    if Instant::now() >= deadline {
      anyhow::bail!(
        "deadline exceeded while recovering from stale metadata scans: expected={}, consumed={}",
        expected_ids.len(),
        consumed_ids.len()
      );
    }

    let batches = reader.read_available(framework::now_unix_seconds()).await?;
    if batches.is_empty() {
      sleep(Duration::from_millis(50)).await;
      continue;
    }

    for batch in batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload.to_vec())?;
        consumed_ids.insert(id);
      }
    }
  }

  assert_eq!(consumed_ids, expected_ids);
  let cursors_after_catchup = reader.cursors();

  // Re-scan repeatedly after full catch-up to ensure duplicate scans do not regress cursors.
  for _ in 0 .. 6 {
    let before_count = consumed_ids.len();
    let batches = reader.read_available(framework::now_unix_seconds()).await?;
    for batch in batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload.to_vec())?;
        consumed_ids.insert(id);
      }
    }
    assert_eq!(
      consumed_ids.len(),
      before_count,
      "post-catchup scan should not surface additional records"
    );
    assert_eq!(
      reader.cursors(),
      cursors_after_catchup,
      "cursor state regressed after stale metadata scan"
    );
    sleep(Duration::from_millis(20)).await;
  }

  let _fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("metadata_scan_window".to_string()),
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

// High-level: validates producer lease conflict injection still converges after deterministic
// reroute, preserving full write progress without accepted split writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_lease_store_conflict_then_expiry_takeover() -> Result<()> {
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
  let mut seen_retry = false;
  let mut produced_partitions = HashSet::new();

  for message_id in 0 .. 16 {
    let id = format!("fit-009-pre-takeover-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-009-key-pre-{message_id}").into_bytes(),
      &id,
    )
    .await?;
    if ack.attempts > 1 {
      seen_retry = true;
    }
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  // Deterministically transition traffic to standby to exercise takeover behavior once initial
  // lease-conflict faults have been applied.
  cluster.set_active_nodes(vec![standby_node.clone()]);

  for message_id in 16 .. 40 {
    let id = format!("fit-009-post-takeover-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-009-key-post-{message_id}").into_bytes(),
      &id,
    )
    .await?;
    if ack.attempts > 1 {
      seen_retry = true;
    }
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }

  assert!(
    seen_retry,
    "expected retries while producer lease conflicts were injected"
  );

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

  let mut consumed_ids = HashSet::new();
  drain_reader_until(
    &mut reader,
    &mut consumed_ids,
    expected_ids.len(),
    Instant::now() + Duration::from_secs(45),
  )
  .await?;
  assert_eq!(consumed_ids, expected_ids);

  let _conflict_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("producer_acquire_lease".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;

  let _acquired_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("producer_acquire_lease".to_string()),
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

// High-level: validates consumer lease heartbeat failures trigger ownership failover while
// committed progress remains monotonic across owners.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_lease_store_heartbeat_failover() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let lease_store = resources.consumer_lease_store();

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::ConsumerLease,
      operation: StoreFaultOperation::ConsumerHeartbeatPartition,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "drop active owner heartbeat".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;

  let key = ConsumerGroupLeaseKey {
    topic: TOPIC.to_string(),
    group_id: "fit-010-group".to_string(),
    virtual_partition_id: 3,
  };
  let lease_duration_ms = 100_i64;
  let t0 = 1_000_000_i64;

  let assigned_a = lease_store
    .assign_partition(
      key.clone(),
      "member-a".to_string(),
      1,
      t0,
      lease_duration_ms,
    )
    .await?;
  assert!(matches!(
    assigned_a,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));

  // Heartbeats from member-a are faulted; progress should not advance yet.
  let failed_heartbeat_a = lease_store
    .heartbeat_partition(
      &key,
      "member-a",
      1,
      t0 + 10,
      lease_duration_ms,
      Some(blob_stream_types::CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 10,
        source_checkpoint: None,
      }),
    )
    .await;
  assert!(
    failed_heartbeat_a.is_err(),
    "expected heartbeat failure for active owner"
  );

  let takeover = lease_store
    .assign_partition(
      key.clone(),
      "member-b".to_string(),
      2,
      t0 + 250,
      lease_duration_ms,
    )
    .await?;
  let ConsumerGroupAssignmentOutcome::Assigned(lease_b) = takeover else {
    anyhow::bail!("expected member-b takeover assignment");
  };
  assert_eq!(lease_b.owner_id, "member-b");
  assert_eq!(lease_b.generation, 2);

  let renewed_b = lease_store
    .heartbeat_partition(
      &key,
      "member-b",
      2,
      t0 + 260,
      lease_duration_ms,
      Some(blob_stream_types::CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 25,
        source_checkpoint: None,
      }),
    )
    .await?;
  let ConsumerGroupHeartbeatOutcome::Renewed(renewed_lease) = renewed_b else {
    anyhow::bail!("expected takeover owner heartbeat renewal");
  };
  assert_eq!(renewed_lease.owner_id, "member-b");
  assert_eq!(renewed_lease.generation, 2);
  assert_eq!(
    renewed_lease.committed_cursor,
    Some(blob_stream_types::CommittedCursor {
      virtual_partition_id: key.virtual_partition_id,
      seq_end: 25,
      source_checkpoint: None,
    })
  );

  let stale_heartbeat = lease_store
    .heartbeat_partition(
      &key,
      "member-a",
      1,
      t0 + 270,
      lease_duration_ms,
      Some(blob_stream_types::CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 999,
        source_checkpoint: None,
      }),
    )
    .await?;
  assert!(matches!(
    stale_heartbeat,
    ConsumerGroupHeartbeatOutcome::HeldByOther(_)
  ));

  let commit_b = lease_store
    .commit_cursor(
      &key,
      "member-b",
      2,
      t0 + 280,
      blob_stream_types::CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 30,
        source_checkpoint: None,
      },
    )
    .await?;
  let ConsumerGroupCommitOutcome::Committed(committed) = commit_b else {
    anyhow::bail!("expected committed cursor from takeover owner");
  };
  assert_eq!(committed.owner_id, "member-b");
  assert_eq!(committed.generation, 2);
  assert_eq!(
    committed.committed_cursor,
    Some(blob_stream_types::CommittedCursor {
      virtual_partition_id: key.virtual_partition_id,
      seq_end: 30,
      source_checkpoint: None,
    })
  );

  let stale_commit = lease_store
    .commit_cursor(
      &key,
      "member-a",
      1,
      t0 + 290,
      blob_stream_types::CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 5,
        source_checkpoint: None,
      },
    )
    .await?;
  assert!(matches!(
    stale_commit,
    ConsumerGroupCommitOutcome::HeldByOther(_)
  ));

  let _fault_event = resources
    .store_fault_controller()
    .events()
    .await
    .into_iter()
    .find(|event| {
      event.operation == StoreFaultOperation::ConsumerHeartbeatPartition && event.action.is_some()
    })
    .ok_or_else(|| anyhow::anyhow!("missing consumer heartbeat fault event"))?;

  resources.cleanup().await;
  Ok(())
}

// High-level: validates bootstrap consumer rebalance converges under transient membership and
// consumer-lease faults while preserving the full consumed record set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_rebalance_with_membership_and_lease_faults() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .in_memory_transport()
    .start()
    .await?;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let runtime_a = consumer_runtime_config("fit-015-a");
  let runtime_b = consumer_runtime_config("fit-015-b");
  let runtime_a_group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("fit-015-a group config missing"))?;
  let runtime_b_group = runtime_b
    .group
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("fit-015-b group config missing"))?;

  let blob_store = resources.blob_store();
  let metadata_store = resources.metadata_store();
  let lease_store = resources.consumer_lease_store();
  let membership_store = resources.consumer_membership_store();

  let mut consumer_a = Box::new(
    ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
      &runtime_a,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&lease_store),
      Arc::clone(&membership_store),
      Arc::new(MembershipCoordinationSource::new(
        runtime_a_group.topic.to_string(),
        runtime_a_group.group_id.to_string(),
        runtime_a_group.member_id.to_string(),
        (0 .. PARTITION_COUNT).collect(),
        Arc::clone(&membership_store),
      )),
      metrics_scope("blob_stream_consumer_it"),
      1,
      DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
      None,
    )
    .await?,
  );
  let mut consumer_b = Box::new(
    ConsumerIteratorImpl::from_runtime_config_with_retention_and_publication_lag(
      &runtime_b,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&lease_store),
      Arc::clone(&membership_store),
      Arc::new(MembershipCoordinationSource::new(
        runtime_b_group.topic.to_string(),
        runtime_b_group.group_id.to_string(),
        runtime_b_group.member_id.to_string(),
        (0 .. PARTITION_COUNT).collect(),
        Arc::clone(&membership_store),
      )),
      metrics_scope("blob_stream_consumer_it"),
      1,
      DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
      None,
    )
    .await?,
  );

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
  let deadline = Instant::now() + Duration::from_secs(6);
  let mut consumed_ids = HashSet::new();
  while consumed_ids.len() < expected_ids.len() {
    if Instant::now() >= deadline {
      break;
    }

    for consumer in [&mut consumer_a, &mut consumer_b] {
      let next = tokio::time::timeout(Duration::from_secs(2), consumer.next()).await;
      let Ok(next) = next else {
        continue;
      };

      let Ok(next_result) = next else {
        // Transient injected faults can surface here; retry on next poll.
        continue;
      };

      match next_result {
        NextResult::Revoked(revoked) => {
          saw_revocation = true;
          revoked.complete().await;
        },
        NextResult::Record(record) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          consumed_ids.insert(id);

          consumer.store_offset(record.virtual_partition_id, record.offset)?;
          let _ = consumer.commit().await;
        },
      }
    }

    sleep(Duration::from_millis(20)).await;
  }

  if consumed_ids.len() < expected_ids.len() {
    let mut reader = ConsumerReaderImpl::new(
      ConsumerReadConfig {
        topic: TOPIC.to_string().into(),
        window_size_seconds: Some(WINDOW_SIZE_SECONDS),
        ..Default::default()
      },
      (0 .. PARTITION_COUNT).collect(),
      HashMap::new(),
      resources.blob_store(),
      resources.metadata_store(),
      &metrics_scope("blob_stream_consumer_it"),
      1,
      DEFAULT_MAX_METADATA_PUBLICATION_LAG_MS,
      None,
    )?;
    drain_reader_until(
      &mut reader,
      &mut consumed_ids,
      expected_ids.len(),
      Instant::now() + Duration::from_secs(3),
    )
    .await?;
  }
  assert_eq!(consumed_ids, expected_ids);

  let _assign_fault = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("consumer_assign_partition".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(3),
    )
    .await?;
  let _heartbeat_fault = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("consumer_heartbeat_partition".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(3),
    )
    .await?;
  let _membership_fault = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("consumer_membership_heartbeat".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(3),
    )
    .await?;
  let _planner_publish_fault = cluster
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
  let _ownership_ok = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("consumer_assign_partition".to_string()),
        key_contains: None,
        status: Some("ok".to_string()),
      },
      Duration::from_secs(3),
    )
    .await?;

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

// High-level: validates combined transport and metadata faults converge to complete final
// consumption with no loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_network_and_metadata_faults_end_to_end() -> Result<()> {
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
      remaining_hits: Some(2),
    })
    .await;

  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Metadata,
      operation: StoreFaultOperation::MetadataWriteSegment,
      key_pattern: None,
      action: StoreFaultAction::Delay(Duration::from_millis(150)),
      remaining_hits: Some(6),
    })
    .await;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  let mut produced_partitions = HashSet::new();
  let mut saw_retry = false;

  for message_id in 0 .. 48 {
    let id = format!("fit-011-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-011-key-{}", message_id % 16).into_bytes(),
      &id,
    )
    .await?;
    if ack.attempts > 1 {
      saw_retry = true;
    }
    produced_partitions.insert(ack.virtual_partition_id);
    expected_ids.insert(id);
  }
  assert!(
    saw_retry,
    "expected retries under combined network and metadata faults"
  );

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

  let mut consumed_ids = HashSet::new();
  drain_reader_until(
    &mut reader,
    &mut consumed_ids,
    expected_ids.len(),
    Instant::now() + Duration::from_secs(45),
  )
  .await?;
  assert_eq!(consumed_ids, expected_ids);

  let _transport_fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("transport".to_string()),
        operation: Some("produce_batch".to_string()),
        key_contains: None,
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;
  let _metadata_fault_event = cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("metadata_write_segment".to_string()),
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

// High-level: validates deterministic replay by running the same scripted transport-fault
// scenario twice and asserting normalized traces and terminal outcomes are identical.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deterministic_replay_same_seed_same_event_trace() -> Result<()> {
  let first = run_fit_012_scenario().await?;
  let second = run_fit_012_scenario().await?;

  assert_eq!(
    first.normalized_transport_trace, second.normalized_transport_trace,
    "normalized transport event traces diverged across identical deterministic runs"
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

async fn run_fit_012_scenario() -> Result<Fit012Outcome> {
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
  controller
    .enable_fault(NetworkFaultRule {
      target_node_id: None,
      operation: NetworkOperation::ProduceBatch,
      fault: NetworkFault::Delay(Duration::from_millis(20)),
      remaining_hits: Some(6),
    })
    .await;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut expected_ids = HashSet::new();
  let mut produced_partitions = HashSet::new();
  let mut max_attempts = 1_u32;
  for message_id in 0 .. 24 {
    let id = format!("fit-012-{message_id}");
    let ack = produce_message(
      &producer,
      format!("fit-012-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    max_attempts = max(max_attempts, ack.attempts);
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

  let mut consumed_ids = HashSet::new();
  drain_reader_until(
    &mut reader,
    &mut consumed_ids,
    expected_ids.len(),
    Instant::now() + Duration::from_secs(30),
  )
  .await?;
  assert_eq!(consumed_ids, expected_ids);

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
    total_consumed: consumed_ids.len(),
    max_attempts,
  };

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(outcome)
}
