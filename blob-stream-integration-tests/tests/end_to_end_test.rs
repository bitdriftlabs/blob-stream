// blob-stream - milestone 13 end-to-end integration tests
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

#[path = "./support/framework.rs"]
mod framework;

use anyhow::{Result, anyhow};
use blob_stream_broker_discovery::BrokerDiscovery;
use blob_stream_consumer::{
  ConsumerIterator, ConsumerIteratorImpl, ConsumerReadConfig, ConsumerReader, ConsumerReaderImpl,
  NextResult,
};
use blob_stream_metadata_store::{
  ConsumerGroupAssignmentOutcome, ConsumerGroupCommitOutcome, ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey, MetadataStore, SegmentMetadata,
};
use blob_stream_producer::{ProducerClient, ProducerRecord};
use blob_stream_types::{
  CommittedCursor, logical_partition_for_key, virtual_partition_for_logical,
};
use framework::{
  ClusterHarness, DynamicCoordinationSource, IntegrationResources, PARTITION_COUNT, SECOND_TOPIC,
  TOPIC, WINDOW_SIZE_SECONDS, consumer_runtime_config, drain_reader_until, now_unix_seconds,
  produce_message, produce_message_for_topic, producer_config, producer_config_with_writer_id,
  producer_topic, producer_topic_named, producer_topic_named_with_writers,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::time::{Instant, sleep, timeout};

struct DelayedVisibilityMetadataStore {
  inner: Arc<dyn MetadataStore>,
  delay: Duration,
  pending: Mutex<Vec<(Instant, SegmentMetadata)>>,
}

impl DelayedVisibilityMetadataStore {
  fn new(inner: Arc<dyn MetadataStore>, delay: Duration) -> Self {
    Self {
      inner,
      delay,
      pending: Mutex::new(Vec::new()),
    }
  }

  async fn flush_visible_segments(&self) -> Result<()> {
    let now = Instant::now();
    let mut pending = self.pending.lock().await;
    let mut ready = Vec::new();
    let mut future = Vec::new();

    for (visible_at, metadata) in pending.drain(..) {
      if visible_at <= now {
        ready.push(metadata);
      } else {
        future.push((visible_at, metadata));
      }
    }
    *pending = future;
    drop(pending);

    for metadata in ready {
      self.inner.write_segment(metadata).await?;
    }
    Ok(())
  }
}

#[async_trait::async_trait]
impl MetadataStore for DelayedVisibilityMetadataStore {
  async fn write_segment(&self, metadata: SegmentMetadata) -> Result<()> {
    let mut pending = self.pending.lock().await;
    pending.push((Instant::now() + self.delay, metadata));
    Ok(())
  }

  async fn scan_window(
    &self,
    window: &blob_stream_types::TopicWindowKey,
    min_snowflake_id: Option<blob_stream_types::SnowflakeId>,
  ) -> Result<Vec<SegmentMetadata>> {
    self.flush_visible_segments().await?;
    self.inner.scan_window(window, min_snowflake_id).await
  }
}

enum ConsumerTaskEvent {
  Batch { ids: Vec<String> },
  Revoked { ack: oneshot::Sender<()> },
}

async fn run_consumer_task(
  mut consumer: Box<ConsumerIteratorImpl>,
  mut stop_rx: watch::Receiver<bool>,
  event_tx: mpsc::UnboundedSender<ConsumerTaskEvent>,
) -> Result<()> {
  consumer.start()?;

  loop {
    if *stop_rx.borrow() {
      break;
    }

    tokio::select! {
      changed = stop_rx.changed() => {
        if changed.is_ok() && *stop_rx.borrow() {
          break;
        }
      }
      next_result = timeout(Duration::from_millis(200), consumer.next()) => {
        let Ok(Ok(next_result)) = next_result else {
          continue;
        };

        match next_result {
          NextResult::Revoked(revoked) => {
            let (ack_tx, ack_rx) = oneshot::channel();
            let _ = event_tx.send(ConsumerTaskEvent::Revoked { ack: ack_tx });
            if ack_rx.await.is_err() {
              break;
            }
            revoked.complete().await;
          },
          NextResult::Batch(batch) => {
            let mut ids = Vec::with_capacity(batch.records.len());
            for record in batch.records {
              let id = String::from_utf8(record.payload)
                .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
              ids.push(id);
            }

            consumer.store_offset(batch.virtual_partition_id, batch.seq_range.end)?;
            let _ = consumer.commit().await?;
            let _ = event_tx.send(ConsumerTaskEvent::Batch { ids });
          },
        }
      }
    }
  }

  let _ = consumer.shutdown().await;
  Ok(())
}

fn handle_consumer_event(
  event: ConsumerTaskEvent,
  consumed_ids: &mut HashSet<String>,
  revocation_count: &mut usize,
) {
  match event {
    ConsumerTaskEvent::Batch { ids } => {
      for id in ids {
        consumed_ids.insert(id);
      }
    },
    ConsumerTaskEvent::Revoked { ack } => {
      *revocation_count += 1;
      let _ = ack.send(());
    },
  }
}

// High-level: verifies the baseline single-broker produce/read path and duplicate-scan dedupe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_broker_single_record_end_to_end() -> Result<()> {
  // Step 1: Start isolated test infrastructure and one in-process broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  // Step 2: Build a producer against dynamic broker discovery.
  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = blob_stream_producer::ProducerClientImpl::new(
    producer_config(8),
    vec![producer_topic()],
    Arc::clone(&discovery),
  )
  .await?;

  // Step 3: Produce one record and verify the broker accepted it.
  let ack = produce_message(&producer, b"smoke-key".to_vec(), "smoke-0").await?;
  assert!(ack.attempts >= 1);

  // Step 4: Read from the exact virtual partition and assert the record is visible.
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    vec![ack.virtual_partition_id],
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
  )?;

  let mut observed_ids = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(30);
  drain_reader_until(&mut reader, &mut observed_ids, 1, deadline).await?;
  assert!(observed_ids.contains("smoke-0"));

  // Step 5: Re-scan to verify dedupe behavior after cursor advancement.
  let duplicate_scan = reader.read_available(now_unix_seconds()).await?;
  assert!(duplicate_scan.is_empty());

  // Step 6: Tear down broker and dependency resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies progress across consumer-group rebalance and broker failover under load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autoscaling_rebalance_and_failover_preserves_progress() -> Result<()> {
  // Step 1: Start a multi-broker harness and pin producer routing to one active broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 3).start().await?;
  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());

  let initial_nodes = cluster.live_nodes();
  let active_node = initial_nodes
    .first()
    .cloned()
    .ok_or_else(|| anyhow!("expected at least one broker"))?;
  cluster.set_active_nodes(vec![active_node.clone()]);

  // Step 2: Create producers and a reader that tracks all virtual partitions.
  let mut producers = Vec::new();
  for _ in 0..4 {
    producers.push(
      blob_stream_producer::ProducerClientImpl::new(
        producer_config(8),
        vec![producer_topic()],
        Arc::clone(&discovery),
      )
      .await?,
    );
  }

  let blob_store = resources.blob_store();
  let metadata_store = resources.metadata_store();
  let consumer_lease_store = resources.consumer_lease_store();
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(20),
      ..Default::default()
    },
    (0..PARTITION_COUNT).collect(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
  )?;

  // Step 3: Produce and drain phase 1 traffic.
  let mut expected_ids = HashSet::new();
  for message_id in 0..32 {
    let id = format!("phase1-{message_id}");
    let producer = &producers[message_id % producers.len()];
    produce_message(
      producer,
      format!("key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  let mut consumed_ids = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(30);
  drain_reader_until(&mut reader, &mut consumed_ids, expected_ids.len(), deadline).await?;

  // Step 4: Simulate consumer scale-out and assert revocation callback is emitted.
  let coordination = DynamicCoordinationSource::new(
    vec!["consumer-0".to_string(), "consumer-1".to_string()],
    (0..PARTITION_COUNT).collect(),
  );
  let runtime_0 = consumer_runtime_config("consumer-0");
  let runtime_1 = consumer_runtime_config("consumer-1");
  let mut consumer_0 = Box::new(
    ConsumerIteratorImpl::from_runtime_config(
      &runtime_0,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&consumer_lease_store),
      Arc::new(coordination.clone()),
    )
    .await?,
  );
  let mut consumer_1 = Box::new(
    ConsumerIteratorImpl::from_runtime_config(
      &runtime_1,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&consumer_lease_store),
      Arc::new(coordination.clone()),
    )
    .await?,
  );
  consumer_0.start()?;
  consumer_1.start()?;
  coordination.update_members(vec![
    "consumer-0".to_string(),
    "consumer-1".to_string(),
    "consumer-2".to_string(),
  ]);

  let mut saw_revocation = false;
  let rebalance_deadline = Instant::now() + Duration::from_secs(10);
  while Instant::now() < rebalance_deadline {
    for consumer in [&mut consumer_0, &mut consumer_1] {
      let next_result = timeout(Duration::from_secs(2), consumer.next()).await;
      if let Ok(Ok(next_result)) = next_result {
        match next_result {
          NextResult::Revoked(revoked) => {
            revoked.complete().await;
            saw_revocation = true;
            break;
          },
          NextResult::Batch(batch) => {
            consumer.store_offset(batch.virtual_partition_id, batch.seq_range.end)?;
            let _ = consumer.commit().await?;
          },
        }
      }
    }
    if saw_revocation {
      break;
    }
  }
  assert!(
    saw_revocation,
    "expected revocation after consumer membership change"
  );

  let _ = consumer_0.shutdown().await;
  let _ = consumer_1.shutdown().await;

  // Step 5: Fail over producer traffic to a different broker and verify progress is preserved.
  let live_nodes = cluster.live_nodes();
  let failover_node = live_nodes
    .iter()
    .find(|node| node.node_id != active_node.node_id)
    .cloned()
    .ok_or_else(|| anyhow!("expected secondary broker for failover"))?;
  cluster.set_active_nodes(vec![failover_node.clone()]);
  cluster.remove_broker_by_id(&active_node.node_id).await?;

  for message_id in 0..32 {
    let id = format!("phase2-{message_id}");
    let producer = &producers[message_id % producers.len()];
    produce_message(
      producer,
      format!("key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  let mut post_failover_reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    (0..PARTITION_COUNT).collect(),
    HashMap::new(),
    Arc::clone(&blob_store),
    Arc::clone(&metadata_store),
  )?;

  let mut post_failover_consumed_ids = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(30);
  drain_reader_until(
    &mut post_failover_reader,
    &mut post_failover_consumed_ids,
    expected_ids.len(),
    deadline,
  )
  .await?;
  assert_eq!(post_failover_consumed_ids, expected_ids);

  // Step 6: Clean up all spawned resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies cursor monotonicity and dedupe behavior for repeated scans on one broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_broker_cursor_monotonicity_and_dedup() -> Result<()> {
  // Step 1: Start a single-broker harness and use normal broker discovery.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let live_nodes = cluster.live_nodes();
  let live_node = live_nodes
    .first()
    .cloned()
    .ok_or_else(|| anyhow!("expected one live broker"))?;

  let producer_discovery: Arc<dyn BrokerDiscovery> =
    Arc::new(framework::DynamicBrokerDiscovery::new(vec![live_node]));

  let producer = blob_stream_producer::ProducerClientImpl::new(
    producer_config(12),
    vec![producer_topic()],
    Arc::clone(&producer_discovery),
  )
  .await?;

  // Step 2: Produce directly without test-level retries. Retries are handled by producer internals.
  let first_ack = produce_message(&producer, b"stable-key".to_vec(), "fault-0").await?;
  assert!(first_ack.attempts >= 1);

  // Step 3: Read the produced data and validate dedupe/cursor monotonicity on repeated scans.
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    (0..PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
  )?;

  let mut observed_ids = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(30);
  drain_reader_until(&mut reader, &mut observed_ids, 1, deadline).await?;

  let duplicate_scan = reader.read_available(now_unix_seconds()).await?;
  assert!(duplicate_scan.is_empty());
  let cursors_after_duplicate_scan = reader.cursors();
  let duplicate_scan_again = reader.read_available(now_unix_seconds()).await?;
  assert!(duplicate_scan_again.is_empty());
  assert_eq!(reader.cursors(), cursors_after_duplicate_scan);

  assert_eq!(observed_ids.len(), 1);
  assert!(observed_ids.contains("fault-0"));

  // Step 4: Clean up resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies consumer restart resumes from committed offsets without replaying old data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_restart_resume_from_committed_offsets() -> Result<()> {
  // Step 1: Start isolated test infrastructure and a single broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = blob_stream_producer::ProducerClientImpl::new(
    producer_config(8),
    vec![producer_topic()],
    Arc::clone(&discovery),
  )
  .await?;

  // Step 2: Produce phase 1 records.
  let mut phase1_expected = HashSet::new();
  for message_id in 0..24 {
    let id = format!("restart-phase1-{message_id}");
    produce_message(
      &producer,
      format!("restart-key-{}", message_id % 6).into_bytes(),
      &id,
    )
    .await?;
    phase1_expected.insert(id);
  }

  // Step 3: Consume phase 1 with a group member and commit offsets as batches are processed.
  let coordination = DynamicCoordinationSource::new(
    vec!["consumer-0".to_string()],
    (0..PARTITION_COUNT).collect(),
  );
  let runtime = consumer_runtime_config("consumer-0");
  let blob_store = resources.blob_store();
  let metadata_store = resources.metadata_store();
  let consumer_lease_store = resources.consumer_lease_store();

  let mut consumer = Box::new(
    ConsumerIteratorImpl::from_runtime_config(
      &runtime,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&consumer_lease_store),
      Arc::new(coordination.clone()),
    )
    .await?,
  );
  consumer.start()?;

  let mut phase1_consumed = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(30);
  while phase1_consumed.len() < phase1_expected.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while consuming phase1: expected={}, consumed={}",
        phase1_expected.len(),
        phase1_consumed.len()
      ));
    }

    let next_result = timeout(Duration::from_secs(2), consumer.next()).await;
    let Ok(Ok(next_result)) = next_result else {
      continue;
    };

    match next_result {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Batch(batch) => {
        for record in batch.records {
          let id = String::from_utf8(record.payload)
            .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
          phase1_consumed.insert(id);
        }

        consumer.store_offset(batch.virtual_partition_id, batch.seq_range.end)?;
        let _ = consumer.commit().await?;
      },
    }
  }
  assert_eq!(phase1_consumed, phase1_expected);

  // Step 4: Restart the consumer and produce phase 2 records after the restart boundary.
  let _ = consumer.shutdown().await;

  let mut phase2_expected = HashSet::new();
  for message_id in 0..24 {
    let id = format!("restart-phase2-{message_id}");
    produce_message(
      &producer,
      format!("restart-key-{}", message_id % 6).into_bytes(),
      &id,
    )
    .await?;
    phase2_expected.insert(id);
  }

  // Step 5: New iterator with same member/group should resume from committed cursors.
  let mut resumed_consumer = Box::new(
    ConsumerIteratorImpl::from_runtime_config(
      &runtime,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&consumer_lease_store),
      Arc::new(coordination),
    )
    .await?,
  );
  resumed_consumer.start()?;

  let mut phase2_consumed = HashSet::new();
  let mut replayed_phase1 = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(30);
  while phase2_consumed.len() < phase2_expected.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while consuming phase2: expected={}, consumed={}",
        phase2_expected.len(),
        phase2_consumed.len()
      ));
    }

    let next_result = timeout(Duration::from_secs(2), resumed_consumer.next()).await;
    let Ok(Ok(next_result)) = next_result else {
      continue;
    };

    match next_result {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Batch(batch) => {
        for record in batch.records {
          let id = String::from_utf8(record.payload)
            .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
          if phase1_expected.contains(&id) {
            replayed_phase1.insert(id.clone());
          }
          if phase2_expected.contains(&id) {
            phase2_consumed.insert(id);
          }
        }

        resumed_consumer.store_offset(batch.virtual_partition_id, batch.seq_range.end)?;
        let _ = resumed_consumer.commit().await?;
      },
    }
  }

  assert!(
    replayed_phase1.is_empty(),
    "resumed consumer replayed phase1 records: {replayed_phase1:?}"
  );
  assert_eq!(phase2_consumed, phase2_expected);

  // Step 6: Clean up all resources.
  let _ = resumed_consumer.shutdown().await;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies no data loss while consumer-group membership changes from 2 -> 3 -> 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_rebalance_continuous_traffic_no_loss() -> Result<()> {
  // Step 1: Start isolated infrastructure and one broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = blob_stream_producer::ProducerClientImpl::new(
    producer_config(8),
    vec![producer_topic()],
    Arc::clone(&discovery),
  )
  .await?;

  // Step 2: Start three iterators and drive active members through 2 -> 3 -> 1.
  let coordination = DynamicCoordinationSource::new(
    vec!["consumer-0".to_string(), "consumer-1".to_string()],
    (0..PARTITION_COUNT).collect(),
  );

  let runtime_0 = consumer_runtime_config("consumer-0");
  let runtime_1 = consumer_runtime_config("consumer-1");
  let runtime_2 = consumer_runtime_config("consumer-2");

  let blob_store = resources.blob_store();
  let metadata_store = resources.metadata_store();
  let consumer_lease_store = resources.consumer_lease_store();

  let consumer_0 = Box::new(
    ConsumerIteratorImpl::from_runtime_config(
      &runtime_0,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&consumer_lease_store),
      Arc::new(coordination.clone()),
    )
    .await?,
  );
  let consumer_1 = Box::new(
    ConsumerIteratorImpl::from_runtime_config(
      &runtime_1,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&consumer_lease_store),
      Arc::new(coordination.clone()),
    )
    .await?,
  );
  let consumer_2 = Box::new(
    ConsumerIteratorImpl::from_runtime_config(
      &runtime_2,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&consumer_lease_store),
      Arc::new(coordination.clone()),
    )
    .await?,
  );

  let (event_tx, mut event_rx) = mpsc::unbounded_channel();
  let (stop_tx, stop_rx) = watch::channel(false);

  let consumer_0_task = tokio::spawn(run_consumer_task(
    consumer_0,
    stop_rx.clone(),
    event_tx.clone(),
  ));
  let consumer_1_task = tokio::spawn(run_consumer_task(
    consumer_1,
    stop_rx.clone(),
    event_tx.clone(),
  ));
  let consumer_2_task = tokio::spawn(run_consumer_task(consumer_2, stop_rx, event_tx));

  // Step 3: Produce in phases and gate phase transitions on revocation barriers.
  let mut expected_ids = HashSet::new();
  let mut consumed_ids = HashSet::new();
  let mut revocation_count = 0usize;

  for message_id in 0..24 {
    let id = format!("rebalance-{message_id}");
    produce_message(
      &producer,
      format!("rebalance-key-{}", message_id % 12).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  let phase1_start_consumed = consumed_ids.len();
  let phase1_deadline = Instant::now() + Duration::from_secs(10);
  while consumed_ids.len() == phase1_start_consumed {
    if Instant::now() >= phase1_deadline {
      return Err(anyhow!(
        "deadline exceeded waiting for phase1 consumption progress: consumed={}",
        consumed_ids.len()
      ));
    }

    let event = timeout(Duration::from_millis(500), event_rx.recv()).await;
    let Ok(Some(event)) = event else {
      continue;
    };
    handle_consumer_event(event, &mut consumed_ids, &mut revocation_count);
  }

  coordination.update_members(vec![
    "consumer-0".to_string(),
    "consumer-1".to_string(),
    "consumer-2".to_string(),
  ]);

  let scale_out_target = revocation_count + 1;
  let scale_out_deadline = Instant::now() + Duration::from_secs(10);
  while revocation_count < scale_out_target {
    if Instant::now() >= scale_out_deadline {
      return Err(anyhow!(
        "deadline exceeded waiting for scale-out revocation: revocations={revocation_count}"
      ));
    }

    let event = timeout(Duration::from_millis(500), event_rx.recv()).await;
    let Ok(Some(event)) = event else {
      continue;
    };
    handle_consumer_event(event, &mut consumed_ids, &mut revocation_count);
  }

  for message_id in 24..48 {
    let id = format!("rebalance-{message_id}");
    produce_message(
      &producer,
      format!("rebalance-key-{}", message_id % 12).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  let phase2_start_consumed = consumed_ids.len();
  let phase2_deadline = Instant::now() + Duration::from_secs(10);
  while consumed_ids.len() == phase2_start_consumed {
    if Instant::now() >= phase2_deadline {
      return Err(anyhow!(
        "deadline exceeded waiting for phase2 consumption progress: consumed={}",
        consumed_ids.len()
      ));
    }

    let event = timeout(Duration::from_millis(500), event_rx.recv()).await;
    let Ok(Some(event)) = event else {
      continue;
    };
    handle_consumer_event(event, &mut consumed_ids, &mut revocation_count);
  }

  coordination.update_members(vec!["consumer-0".to_string()]);

  let scale_in_start_revocations = revocation_count;
  let scale_in_start_consumed = consumed_ids.len();
  let scale_in_deadline = Instant::now() + Duration::from_secs(10);
  while revocation_count == scale_in_start_revocations
    && consumed_ids.len() == scale_in_start_consumed
  {
    if Instant::now() >= scale_in_deadline {
      return Err(anyhow!(
        "deadline exceeded waiting for post scale-in progress: revocations={}, consumed={}",
        revocation_count,
        consumed_ids.len()
      ));
    }

    let event = timeout(Duration::from_millis(500), event_rx.recv()).await;
    let Ok(Some(event)) = event else {
      continue;
    };
    handle_consumer_event(event, &mut consumed_ids, &mut revocation_count);
  }

  for message_id in 48..72 {
    let id = format!("rebalance-{message_id}");
    produce_message(
      &producer,
      format!("rebalance-key-{}", message_id % 12).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  // Step 4: Drain until all produced ids are consumed.
  let deadline = Instant::now() + Duration::from_secs(45);
  while consumed_ids.len() < expected_ids.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while draining rebalance test: expected={}, consumed={}",
        expected_ids.len(),
        consumed_ids.len()
      ));
    }

    let event = timeout(Duration::from_millis(500), event_rx.recv()).await;
    let Ok(Some(event)) = event else {
      continue;
    };
    handle_consumer_event(event, &mut consumed_ids, &mut revocation_count);
  }

  assert_eq!(consumed_ids, expected_ids);

  // Ensure rebalance revocations were observed and acknowledged.
  assert!(
    revocation_count >= scale_out_target,
    "expected at least one revocation after scale-out, observed={revocation_count}"
  );

  // Step 5: Clean up all resources.
  let _ = stop_tx.send(true);
  let consumer_0_result = consumer_0_task
    .await
    .map_err(|error| anyhow!("consumer-0 task join error: {error}"))?;
  let consumer_1_result = consumer_1_task
    .await
    .map_err(|error| anyhow!("consumer-1 task join error: {error}"))?;
  let consumer_2_result = consumer_2_task
    .await
    .map_err(|error| anyhow!("consumer-2 task join error: {error}"))?;
  consumer_0_result?;
  consumer_1_result?;
  consumer_2_result?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies progress and no-loss continuity while the active broker is restarted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_broker_restart_continuity() -> Result<()> {
  // Step 1: Start two brokers and pin producer traffic to a single active broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2).start().await?;

  let live_nodes = cluster.live_nodes();
  let mut active_node = live_nodes
    .first()
    .cloned()
    .ok_or_else(|| anyhow!("expected active broker"))?;
  let standby_node = live_nodes
    .iter()
    .find(|node| node.node_id != active_node.node_id)
    .cloned()
    .ok_or_else(|| anyhow!("expected standby broker"))?;
  cluster.set_active_nodes(vec![active_node.clone()]);

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = blob_stream_producer::ProducerClientImpl::new(
    producer_config(12),
    vec![producer_topic()],
    Arc::clone(&discovery),
  )
  .await?;

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    (0..PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
  )?;

  // Step 2: Produce continuously, restart the active broker mid-stream, and continue producing.
  let total_messages = 64;
  let restart_at = 28;
  let mut expected_ids = HashSet::new();
  let mut consumed_ids = HashSet::new();

  for message_id in 0..total_messages {
    if message_id == restart_at {
      // Keep routing available while the active node is down, then switch back to restarted node.
      cluster.set_active_nodes(vec![standby_node.clone()]);
      active_node = cluster.restart_broker_by_id(&active_node.node_id).await?;
      cluster.set_active_nodes(vec![active_node.clone()]);
    }

    let id = format!("restart-active-{message_id}");
    let produce_result = timeout(
      Duration::from_secs(8),
      produce_message(
        &producer,
        format!("restart-active-key-{}", message_id % 8).into_bytes(),
        &id,
      ),
    )
    .await;
    let ack = produce_result
      .map_err(|_| anyhow!("timed out producing during broker restart test at {message_id}"))??;
    assert!(ack.attempts >= 1);

    expected_ids.insert(id);

    if message_id % 4 == 0 {
      let batches = reader.read_available(now_unix_seconds()).await?;
      for batch in batches {
        for record in batch.records {
          let id = String::from_utf8(record.payload)
            .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
          consumed_ids.insert(id);
        }
      }
    }
  }

  // Step 3: Drain to completion and verify no produced IDs are lost.
  let deadline = Instant::now() + Duration::from_secs(45);
  drain_reader_until(&mut reader, &mut consumed_ids, expected_ids.len(), deadline).await?;
  assert_eq!(consumed_ids, expected_ids);

  // Step 4: Clean up all resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies per-partition sequence ends advance strictly as batches are consumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_partition_sequence_monotonicity() -> Result<()> {
  // Step 1: Start isolated infrastructure and a single broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = blob_stream_producer::ProducerClientImpl::new(
    producer_config(8),
    vec![producer_topic()],
    Arc::clone(&discovery),
  )
  .await?;

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    (0..PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
  )?;

  // Step 2: Produce a stream of records and track which virtual partitions were targeted.
  let total_messages = 96;
  let mut expected_ids = HashSet::new();
  let mut produced_counts_by_partition = HashMap::new();

  for message_id in 0..total_messages {
    let id = format!("seq-monotonic-{message_id}");
    let ack = produce_message(
      &producer,
      format!("seq-key-{}", message_id % 12).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);

    let partition_count = produced_counts_by_partition
      .entry(ack.virtual_partition_id)
      .or_insert(0usize);
    *partition_count += 1;
  }

  // Step 3: Consume all records while asserting each partition's seq_end strictly increases.
  let mut consumed_ids = HashSet::new();
  let mut last_seq_end_by_partition = HashMap::new();
  let mut observed_batches_by_partition = HashMap::new();
  let deadline = Instant::now() + Duration::from_secs(45);

  while consumed_ids.len() < expected_ids.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while validating sequence monotonicity: expected={}, consumed={}",
        expected_ids.len(),
        consumed_ids.len()
      ));
    }

    let batches = reader.read_available(now_unix_seconds()).await?;
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
      let observed_batches = observed_batches_by_partition
        .entry(batch.virtual_partition_id)
        .or_insert(0usize);
      *observed_batches += 1;

      for record in batch.records {
        let id = String::from_utf8(record.payload)
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        consumed_ids.insert(id);
      }
    }
  }

  assert_eq!(consumed_ids, expected_ids);

  let multi_batch_partition_count = observed_batches_by_partition
    .values()
    .filter(|&&count| count > 1)
    .count();
  assert!(
    multi_batch_partition_count > 0,
    "expected at least one partition to observe multiple batches"
  );

  // Ensure the monotonicity check covered all partitions that received produced traffic.
  for partition in produced_counts_by_partition.keys() {
    assert!(
      last_seq_end_by_partition.contains_key(partition),
      "missing consumed batches for partition {partition}"
    );
  }

  // Step 4: Clean up all resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies strict read-path isolation when producing/consuming two distinct topics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_topic_isolation() -> Result<()> {
  // Step 1: Start isolated infrastructure and one broker that serves both test topics.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = blob_stream_producer::ProducerClientImpl::new(
    producer_config(8),
    vec![
      producer_topic_named(TOPIC),
      producer_topic_named(SECOND_TOPIC),
    ],
    Arc::clone(&discovery),
  )
  .await?;

  let mut topic_a_reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    (0..PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
  )?;

  let mut topic_b_reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: SECOND_TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    (0..PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
  )?;

  // Step 2: Produce interleaved traffic to both topics.
  let total_per_topic = 32;
  let mut topic_a_expected = HashSet::new();
  let mut topic_b_expected = HashSet::new();

  for message_id in 0..total_per_topic {
    let topic_a_id = format!("topic-a-{message_id}");
    let topic_b_id = format!("topic-b-{message_id}");

    produce_message_for_topic(
      &producer,
      TOPIC,
      format!("topic-a-key-{}", message_id % 8).into_bytes(),
      &topic_a_id,
    )
    .await?;
    produce_message_for_topic(
      &producer,
      SECOND_TOPIC,
      format!("topic-b-key-{}", message_id % 8).into_bytes(),
      &topic_b_id,
    )
    .await?;

    topic_a_expected.insert(topic_a_id);
    topic_b_expected.insert(topic_b_id);
  }

  // Step 3: Drain both readers and assert there is no cross-topic leakage.
  let mut topic_a_consumed = HashSet::new();
  let mut topic_b_consumed = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(45);

  while topic_a_consumed.len() < topic_a_expected.len()
    || topic_b_consumed.len() < topic_b_expected.len()
  {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while draining multi-topic test: topic_a={}/{}, topic_b={}/{}",
        topic_a_consumed.len(),
        topic_a_expected.len(),
        topic_b_consumed.len(),
        topic_b_expected.len()
      ));
    }

    let topic_a_batches = topic_a_reader.read_available(now_unix_seconds()).await?;
    for batch in topic_a_batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload)
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        assert!(
          id.starts_with("topic-a-"),
          "topic-a reader observed cross-topic payload: {id}"
        );
        topic_a_consumed.insert(id);
      }
    }

    let topic_b_batches = topic_b_reader.read_available(now_unix_seconds()).await?;
    for batch in topic_b_batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload)
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        assert!(
          id.starts_with("topic-b-"),
          "topic-b reader observed cross-topic payload: {id}"
        );
        topic_b_consumed.insert(id);
      }
    }

    if topic_a_consumed.len() < topic_a_expected.len()
      || topic_b_consumed.len() < topic_b_expected.len()
    {
      sleep(Duration::from_millis(50)).await;
    }
  }

  assert_eq!(topic_a_consumed, topic_a_expected);
  assert_eq!(topic_b_consumed, topic_b_expected);
  assert!(topic_a_consumed.is_disjoint(&topic_b_consumed));

  // Step 4: Clean up all resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies boundary payload acceptance and multi-record batch delivery semantics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_boundary_and_batching_behavior() -> Result<()> {
  // Step 1: Start isolated infrastructure and configure producer for observable batching.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let mut config = producer_config(8);
  config.max_batch_records = Some(8);
  config.max_batch_bytes = Some(4_096);
  config.flush_max_delay_ms = Some(50);

  let producer = Arc::new(
    blob_stream_producer::ProducerClientImpl::new(
      config,
      vec![producer_topic()],
      Arc::clone(&discovery),
    )
    .await?,
  );

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    (0..PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
  )?;

  // Step 2: Produce boundary payload sizes, including true empty and near-limit payloads.
  let boundary_payloads = vec![Vec::new(), b"s".to_vec(), vec![b'n'; 900]];
  let mut expected_payload_counts: HashMap<Vec<u8>, usize> = HashMap::new();

  for (index, payload) in boundary_payloads.into_iter().enumerate() {
    let event_ts_ms = i64::try_from(
      std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is before unix epoch")
        .as_millis(),
    )
    .expect("unix millis exceeds i64");
    let ack = producer
      .produce(ProducerRecord::new(
        TOPIC,
        format!("boundary-key-{index}").into_bytes(),
        payload.clone(),
        event_ts_ms,
      ))
      .await?;
    assert!(ack.attempts >= 1);
    *expected_payload_counts.entry(payload).or_insert(0) += 1;
  }

  // Step 3: Produce concurrent small records on one key to force in-producer batching.
  let mut produce_tasks = Vec::new();
  for message_id in 0..24 {
    let producer = Arc::clone(&producer);
    let payload = format!("batching-{message_id}").into_bytes();
    *expected_payload_counts.entry(payload.clone()).or_insert(0) += 1;

    produce_tasks.push(tokio::spawn(async move {
      let event_ts_ms = i64::try_from(
        std::time::SystemTime::now()
          .duration_since(std::time::UNIX_EPOCH)
          .expect("clock is before unix epoch")
          .as_millis(),
      )
      .expect("unix millis exceeds i64");
      producer
        .produce(ProducerRecord::new(
          TOPIC,
          b"batched-key".to_vec(),
          payload,
          event_ts_ms,
        ))
        .await
    }));
  }

  for task in produce_tasks {
    let ack = task.await??;
    assert!(ack.attempts >= 1);
  }
  producer.flush().await?;

  // Step 4: Drain and assert exact payload recovery plus at least one multi-record batch.
  let expected_total = expected_payload_counts.values().sum::<usize>();
  let mut consumed_payload_counts: HashMap<Vec<u8>, usize> = HashMap::new();
  let mut consumed_total = 0usize;
  let mut saw_multi_record_batch = false;
  let deadline = Instant::now() + Duration::from_secs(45);

  while consumed_total < expected_total {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while draining payload/batching test: expected={expected_total}, \
         consumed={consumed_total}"
      ));
    }

    let batches = reader.read_available(now_unix_seconds()).await?;
    if batches.is_empty() {
      sleep(Duration::from_millis(50)).await;
      continue;
    }

    for batch in batches {
      if batch.records.len() > 1 {
        saw_multi_record_batch = true;
      }

      for record in batch.records {
        *consumed_payload_counts.entry(record.payload).or_insert(0) += 1;
        consumed_total += 1;
      }
    }
  }

  assert_eq!(consumed_payload_counts, expected_payload_counts);
  assert!(
    saw_multi_record_batch,
    "expected at least one consumed batch with multiple records"
  );

  // Step 5: Clean up all resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies delayed metadata visibility is recovered by re-scan without data loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_metadata_cross_window_no_loss() -> Result<()> {
  // Step 1: Start isolated infrastructure with a metadata store that delays visibility.
  let resources = IntegrationResources::create().await?;
  let delayed_metadata_store: Arc<dyn MetadataStore> = Arc::new(
    DelayedVisibilityMetadataStore::new(resources.metadata_store(), Duration::from_secs(2)),
  );
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .metadata_store(Arc::clone(&delayed_metadata_store))
    .start()
    .await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = blob_stream_producer::ProducerClientImpl::new(
    producer_config(8),
    vec![producer_topic()],
    Arc::clone(&discovery),
  )
  .await?;

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    (0..PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    Arc::clone(&delayed_metadata_store),
  )?;

  // Step 2: Produce traffic while metadata remains temporarily invisible to readers.
  let mut expected_ids = HashSet::new();
  for message_id in 0..24 {
    let id = format!("delayed-meta-{message_id}");
    produce_message(
      &producer,
      format!("delayed-meta-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  // Before delayed visibility elapses, scans should still be empty.
  let early_scan_deadline = Instant::now() + Duration::from_millis(900);
  while Instant::now() < early_scan_deadline {
    let early_batches = reader.read_available(now_unix_seconds()).await?;
    assert!(
      early_batches.is_empty(),
      "expected empty scan before delayed metadata becomes visible"
    );
    sleep(Duration::from_millis(100)).await;
  }

  // Step 3: Re-scans should eventually discover all delayed metadata with no loss.
  let mut consumed_ids = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(20);
  drain_reader_until(&mut reader, &mut consumed_ids, expected_ids.len(), deadline).await?;

  assert_eq!(consumed_ids, expected_ids);
  let duplicate_scan = reader.read_available(now_unix_seconds()).await?;
  assert!(duplicate_scan.is_empty());

  // Step 4: Clean up all resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies stale owner heartbeats/commits are fenced after generation changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_generation_fencing_rejects_stale_commit() -> Result<()> {
  // Step 1: Start isolated resources and get a real consumer lease store.
  let resources = IntegrationResources::create().await?;
  let lease_store = resources.consumer_lease_store();

  let key = ConsumerGroupLeaseKey {
    topic: TOPIC.to_string(),
    group_id: "generation-fencing-group".to_string(),
    virtual_partition_id: 7,
  };

  let initial_generation = 1_u64;
  let takeover_generation = 2_u64;
  let lease_duration_ms = 100_i64;
  let start_ts_ms = 1_000_000_i64;

  let initial_assignment = lease_store
    .assign_partition(
      key.clone(),
      "member-a".to_string(),
      initial_generation,
      start_ts_ms,
      lease_duration_ms,
    )
    .await?;
  assert!(matches!(
    initial_assignment,
    ConsumerGroupAssignmentOutcome::Assigned(_)
  ));

  let initial_heartbeat = lease_store
    .heartbeat_partition(
      &key,
      "member-a",
      initial_generation,
      start_ts_ms + 10,
      lease_duration_ms,
      Some(CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 10,
      }),
    )
    .await?;
  let ConsumerGroupHeartbeatOutcome::Renewed(lease) = initial_heartbeat else {
    return Err(anyhow!("expected initial owner heartbeat renewal"));
  };
  assert_eq!(lease.owner_id, "member-a");
  assert_eq!(lease.generation, initial_generation);
  assert_eq!(
    lease.committed_cursor,
    Some(CommittedCursor {
      virtual_partition_id: key.virtual_partition_id,
      seq_end: 10,
    })
  );

  // Step 2: Simulate rebalance takeover after lease expiry with a newer generation.
  let takeover_assignment = lease_store
    .assign_partition(
      key.clone(),
      "member-b".to_string(),
      takeover_generation,
      start_ts_ms + 250,
      lease_duration_ms,
    )
    .await?;
  let ConsumerGroupAssignmentOutcome::Assigned(takeover_lease) = takeover_assignment else {
    return Err(anyhow!("expected takeover assignment by new generation"));
  };
  assert_eq!(takeover_lease.owner_id, "member-b");
  assert_eq!(takeover_lease.generation, takeover_generation);

  let takeover_heartbeat = lease_store
    .heartbeat_partition(
      &key,
      "member-b",
      takeover_generation,
      start_ts_ms + 260,
      lease_duration_ms,
      Some(CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 20,
      }),
    )
    .await?;
  let ConsumerGroupHeartbeatOutcome::Renewed(active_lease) = takeover_heartbeat else {
    return Err(anyhow!("expected active owner heartbeat renewal"));
  };
  assert_eq!(active_lease.owner_id, "member-b");
  assert_eq!(active_lease.generation, takeover_generation);
  assert_eq!(
    active_lease.committed_cursor,
    Some(CommittedCursor {
      virtual_partition_id: key.virtual_partition_id,
      seq_end: 20,
    })
  );

  // Step 3: Stale owner attempts to heartbeat/commit and must be fenced.
  let stale_heartbeat = lease_store
    .heartbeat_partition(
      &key,
      "member-a",
      initial_generation,
      start_ts_ms + 270,
      lease_duration_ms,
      Some(CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 999,
      }),
    )
    .await?;
  let ConsumerGroupHeartbeatOutcome::HeldByOther(stale_heartbeat_lease) = stale_heartbeat else {
    return Err(anyhow!("expected stale owner heartbeat to be fenced"));
  };
  assert_eq!(stale_heartbeat_lease.owner_id, "member-b");
  assert_eq!(stale_heartbeat_lease.generation, takeover_generation);
  assert_eq!(
    stale_heartbeat_lease.committed_cursor,
    Some(CommittedCursor {
      virtual_partition_id: key.virtual_partition_id,
      seq_end: 20,
    })
  );

  let stale_commit = lease_store
    .commit_cursor(
      &key,
      "member-a",
      initial_generation,
      start_ts_ms + 280,
      CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 999,
      },
    )
    .await?;
  let ConsumerGroupCommitOutcome::HeldByOther(stale_commit_lease) = stale_commit else {
    return Err(anyhow!("expected stale owner commit to be fenced"));
  };
  assert_eq!(stale_commit_lease.owner_id, "member-b");
  assert_eq!(stale_commit_lease.generation, takeover_generation);
  assert_eq!(
    stale_commit_lease.committed_cursor,
    Some(CommittedCursor {
      virtual_partition_id: key.virtual_partition_id,
      seq_end: 20,
    })
  );

  // Step 4: Active owner remains authoritative and can advance cursor monotonically.
  let active_commit = lease_store
    .commit_cursor(
      &key,
      "member-b",
      takeover_generation,
      start_ts_ms + 290,
      CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 21,
      },
    )
    .await?;
  let ConsumerGroupCommitOutcome::Committed(committed_lease) = active_commit else {
    return Err(anyhow!("expected active owner commit to succeed"));
  };
  assert_eq!(committed_lease.owner_id, "member-b");
  assert_eq!(committed_lease.generation, takeover_generation);
  assert_eq!(
    committed_lease.committed_cursor,
    Some(CommittedCursor {
      virtual_partition_id: key.virtual_partition_id,
      seq_end: 21,
    })
  );

  let stale_after_commit = lease_store
    .heartbeat_partition(
      &key,
      "member-a",
      initial_generation,
      start_ts_ms + 300,
      lease_duration_ms,
      Some(CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 1_000,
      }),
    )
    .await?;
  let ConsumerGroupHeartbeatOutcome::HeldByOther(stale_after_commit_lease) = stale_after_commit
  else {
    return Err(anyhow!(
      "expected stale owner heartbeat to remain fenced after active commit"
    ));
  };
  assert_eq!(stale_after_commit_lease.owner_id, "member-b");
  assert_eq!(stale_after_commit_lease.generation, takeover_generation);
  assert_eq!(
    stale_after_commit_lease.committed_cursor,
    Some(CommittedCursor {
      virtual_partition_id: key.virtual_partition_id,
      seq_end: 21,
    })
  );

  // Step 5: Clean up resources.
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies multi-writer virtual partitions are merged without loss or cursor regressions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_writer_virtual_partition_merge_correctness() -> Result<()> {
  fn key_for_logical_partition(logical_partition_id: u32) -> Vec<u8> {
    for candidate in 0_u32..50_000 {
      let key = format!("logical-key-{logical_partition_id}-{candidate}").into_bytes();
      if logical_partition_for_key(&key, PARTITION_COUNT) == logical_partition_id {
        return key;
      }
    }

    panic!("failed to find key for logical partition {logical_partition_id} within search budget");
  }

  // Step 1: Start one broker with a topic configured for two writers.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .topic_num_writers(2)
    .start()
    .await?;
  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());

  // Step 2: Create producers for writer 0 and writer 1 on the same topic.
  let producer_writer_0 = blob_stream_producer::ProducerClientImpl::new(
    producer_config_with_writer_id(8, 0),
    vec![producer_topic_named_with_writers(TOPIC, 2)],
    Arc::clone(&discovery),
  )
  .await?;
  let producer_writer_1 = blob_stream_producer::ProducerClientImpl::new(
    producer_config_with_writer_id(8, 1),
    vec![producer_topic_named_with_writers(TOPIC, 2)],
    Arc::clone(&discovery),
  )
  .await?;

  let virtual_partition_ids: Vec<u32> = (0..(PARTITION_COUNT * 2)).collect();
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      window_size_seconds: Some(WINDOW_SIZE_SECONDS),
      lookback_windows: Some(10),
      ..Default::default()
    },
    virtual_partition_ids,
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
  )?;

  // Step 3: Produce records into matching logical partitions from both writers.
  let logical_partitions = [1_u32, 5_u32, 9_u32, 13_u32];
  let records_per_writer_per_partition = 6;

  let mut expected_ids = HashSet::new();
  let mut expected_counts_by_partition: HashMap<u32, usize> = HashMap::new();

  for logical_partition_id in logical_partitions {
    let key = key_for_logical_partition(logical_partition_id);

    for sequence in 0..records_per_writer_per_partition {
      let writer_0_id = format!("mw-w0-l{logical_partition_id}-s{sequence}");
      let writer_1_id = format!("mw-w1-l{logical_partition_id}-s{sequence}");

      let ack_0 =
        produce_message_for_topic(&producer_writer_0, TOPIC, key.clone(), &writer_0_id).await?;
      let ack_1 =
        produce_message_for_topic(&producer_writer_1, TOPIC, key.clone(), &writer_1_id).await?;

      assert_eq!(
        ack_0.virtual_partition_id,
        virtual_partition_for_logical(logical_partition_id, PARTITION_COUNT, 0),
        "writer 0 routed to unexpected partition"
      );
      assert_eq!(
        ack_1.virtual_partition_id,
        virtual_partition_for_logical(logical_partition_id, PARTITION_COUNT, 1),
        "writer 1 routed to unexpected partition"
      );

      *expected_counts_by_partition
        .entry(ack_0.virtual_partition_id)
        .or_insert(0) += 1;
      *expected_counts_by_partition
        .entry(ack_1.virtual_partition_id)
        .or_insert(0) += 1;

      expected_ids.insert(writer_0_id);
      expected_ids.insert(writer_1_id);
    }
  }

  // Step 4: Drain reads and assert exact recovery plus per-partition cursor monotonicity.
  let mut consumed_ids = HashSet::new();
  let mut consumed_counts_by_partition: HashMap<u32, usize> = HashMap::new();
  let mut last_seq_end_by_partition: HashMap<u32, u64> = HashMap::new();

  let deadline = Instant::now() + Duration::from_secs(45);
  while consumed_ids.len() < expected_ids.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while draining multi-writer fan-in: expected={}, consumed={}",
        expected_ids.len(),
        consumed_ids.len()
      ));
    }

    let batches = reader.read_available(now_unix_seconds()).await?;
    if batches.is_empty() {
      sleep(Duration::from_millis(50)).await;
      continue;
    }

    for batch in batches {
      if let Some(previous_seq_end) = last_seq_end_by_partition.get(&batch.virtual_partition_id) {
        assert!(
          batch.seq_range.end > *previous_seq_end,
          "seq_end regressed for partition {}: prev={}, current={}",
          batch.virtual_partition_id,
          previous_seq_end,
          batch.seq_range.end
        );
      }
      last_seq_end_by_partition.insert(batch.virtual_partition_id, batch.seq_range.end);

      *consumed_counts_by_partition
        .entry(batch.virtual_partition_id)
        .or_insert(0) += batch.records.len();

      for record in batch.records {
        let id = String::from_utf8(record.payload)
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        consumed_ids.insert(id);
      }
    }
  }

  assert_eq!(consumed_ids, expected_ids);
  assert_eq!(consumed_counts_by_partition, expected_counts_by_partition);

  for partition_id in expected_counts_by_partition.keys() {
    assert!(
      last_seq_end_by_partition.contains_key(partition_id),
      "missing cursor progression for partition {partition_id}"
    );
  }

  // Step 5: Verify duplicate scans are empty and clean up resources.
  let duplicate_scan = reader.read_available(now_unix_seconds()).await?;
  assert!(duplicate_scan.is_empty());

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies takeover after lease expiry without changing coordination membership.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_expiry_takeover_preserves_progress() -> Result<()> {
  // Step 1: Start isolated infra and produce data up front.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = blob_stream_producer::ProducerClientImpl::new(
    producer_config(8),
    vec![producer_topic()],
    Arc::clone(&discovery),
  )
  .await?;

  let mut expected_ids = HashSet::new();
  for message_id in 0..48 {
    let id = format!("lease-expiry-{message_id}");
    produce_message(
      &producer,
      format!("lease-expiry-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  // Step 2: Seed all group leases to a stale owner that stops heartbeating.
  let lease_store = resources.consumer_lease_store();
  let now_ts_ms = i64::try_from(
    std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .expect("clock is before unix epoch")
      .as_millis(),
  )
  .expect("unix millis exceeds i64");
  let stale_lease_duration_ms = 800_i64;

  for partition_id in 0..PARTITION_COUNT {
    let key = ConsumerGroupLeaseKey {
      topic: TOPIC.to_string(),
      group_id: "integration-group".to_string(),
      virtual_partition_id: partition_id,
    };

    let assignment = lease_store
      .assign_partition(
        key,
        "stale-member".to_string(),
        1,
        now_ts_ms,
        stale_lease_duration_ms,
      )
      .await?;

    assert!(matches!(
      assignment,
      ConsumerGroupAssignmentOutcome::Assigned(_)
    ));
  }

  // Step 3: Start one live member without any coordination membership updates.
  let coordination = DynamicCoordinationSource::new(
    vec!["consumer-live".to_string()],
    (0..PARTITION_COUNT).collect(),
  );
  let runtime = consumer_runtime_config("consumer-live");

  let blob_store = resources.blob_store();
  let metadata_store = resources.metadata_store();
  let mut consumer = Box::new(
    ConsumerIteratorImpl::from_runtime_config(
      &runtime,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&lease_store),
      Arc::new(coordination),
    )
    .await?,
  );
  consumer.start()?;

  // Step 4: After stale lease expiry, live member should take over and drain all records.
  let mut consumed_ids = HashSet::new();
  let deadline = Instant::now() + Duration::from_secs(45);

  while consumed_ids.len() < expected_ids.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while draining lease-expiry takeover: expected={}, consumed={}",
        expected_ids.len(),
        consumed_ids.len()
      ));
    }

    let next_result = timeout(Duration::from_secs(2), consumer.next()).await;
    let Ok(Ok(next_result)) = next_result else {
      continue;
    };

    match next_result {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Batch(batch) => {
        for record in batch.records {
          let id = String::from_utf8(record.payload)
            .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
          consumed_ids.insert(id);
        }

        consumer.store_offset(batch.virtual_partition_id, batch.seq_range.end)?;
        let _ = consumer.commit().await?;
      },
    }
  }

  assert_eq!(consumed_ids, expected_ids);

  // Step 5: Clean up resources.
  let _ = consumer.shutdown().await;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}
