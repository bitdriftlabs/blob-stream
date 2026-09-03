use anyhow::{Result, anyhow};
use bd_server_stats::stats::{Collector, Scope};
use bd_time::{OffsetDateTimeExt, TimeProvider};
use blob_stream_blob_store::BlobKey;
use blob_stream_broker::write::{BrokerLeaseStatus, WriteRequest};
use blob_stream_broker_discovery::BrokerDiscovery;
use blob_stream_consumer::consumer::{
  ConsumerReaderImpl,
  GrpcBrokerBlobRangeQuery,
  GrpcBrokerMetadataQuery,
};
use blob_stream_consumer::iterator::{ConsumerIterator, ConsumerIteratorImpl, NextResult};
use blob_stream_consumer::{
  ConsumerBootstrapConfig,
  ConsumerBootstrapIteratorBuilder,
  ConsumerPartitionReadMode,
  ConsumerReadConfig,
  DEFAULT_MAX_METADATA_PUBLICATION_LAG,
  MembershipCoordinationSource,
};
use blob_stream_integration_tests::test_framework::{self as framework, TestConsumerReader};
use blob_stream_metadata_store::{
  ConsumerGroupArmFreshStartOutcome,
  ConsumerGroupAssignmentOutcome,
  ConsumerGroupCommitOutcome,
  ConsumerGroupHeartbeatOutcome,
  ConsumerGroupLeaseKey,
  ConsumerGroupLeaseTransition,
  ConsumerGroupMember,
  InMemoryMetadataStore,
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  MAX_FENCED_METADATA_PARTITIONS,
  MetadataReadConsistency,
  MetadataStore,
  ProducerPartitionFence,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SegmentMetadata,
  SequenceReservationOutcome,
};
use blob_stream_producer::test::ProducerClientTestExt;
use blob_stream_producer::{
  GrpcBrokerTransport,
  ProducerClient,
  ProducerClientImpl,
  ProducerConfig,
  ProducerRecord,
  ProducerTopicConfig,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{
  BatchMetadata,
  CommittedCursor,
  CommittedSourceCheckpoint,
  Compression,
  SeqRange,
  SnowflakeId,
  ToProtoDuration,
  TopicWindowKey,
  VirtualPartitionId,
  Window,
  logical_partition_for_key,
  new_record,
  now_unix_millis,
  offset_datetime_from_unix_millis,
  virtual_partition_for_logical,
};
use framework::{
  ClusterHarness,
  ConsumerDeliveryTrace,
  ConsumerDeliveryTraces,
  ConsumerTaskEvent,
  ControlledConsumer,
  CountingWindowMetadataStore,
  DeferredWindowPublicationMetadataStore,
  DelayedVisibilityMetadataStore,
  FenceInvalidatingMetadataStore,
  IntegrationResources,
  LifecycleEvent,
  PARTITION_COUNT,
  SECOND_TOPIC,
  StoreFaultAction,
  StoreFaultDomain,
  StoreFaultOperation,
  StoreFaultRule,
  TOPIC,
  TestEventMatcher,
  WINDOW_SIZE_SECONDS,
  append_reader_delivery_traces,
  consumer_bootstrap_config,
  consumer_runtime_config,
  delivery_counts,
  delivery_members,
  drain_reader_until_with_trace,
  handle_consumer_event_with_offsets,
  handle_consumer_event_with_trace,
  maximum_delivery_offsets,
  now_unix_seconds,
  poll_consumer_once,
  produce_message,
  produce_message_at_manual_time,
  produce_message_for_topic,
  producer_config,
  producer_config_with_writer_id,
  producer_topic,
  producer_topic_named,
  producer_topic_named_with_partition_count,
  producer_topic_named_with_writers,
  reader_delivery_counts,
  run_consumer_task,
  stop_aware_revocation_consumer,
  wait_for_group_offsets_committed,
  write_recovery_segment,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::{Barrier, mpsc, watch};
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

fn fenced_metadata_segment(
  snowflake_id: u64,
  virtual_partition_ids: impl IntoIterator<Item = VirtualPartitionId>,
) -> SegmentMetadata {
  let segment_index = virtual_partition_ids
    .into_iter()
    .map(|virtual_partition_id| {
      (
        virtual_partition_id,
        vec![BatchMetadata {
          seq_range: SeqRange { start: 0, end: 0 },
          byte_range: blob_stream_types::ByteRange { start: 0, end: 1 },
          payload_bytes: 1,
        }],
      )
    })
    .collect();
  SegmentMetadata::new(
    TopicWindowKey {
      topic: TOPIC.to_string(),
      window_start_unix_seconds: 0,
    },
    SnowflakeId(snowflake_id),
    BlobKey::new(format!("fenced-metadata/{snowflake_id}.bin")),
    Compression::none(),
    segment_index,
    OffsetDateTime::UNIX_EPOCH,
    OffsetDateTime::UNIX_EPOCH,
  )
}

async fn acquire_producer_fence(
  lease_store: &dyn ProducerPartitionLeaseStore,
  virtual_partition_id: VirtualPartitionId,
  holder_id: &str,
  lease_session_id: &str,
  now_ts_ms: i64,
) -> Result<ProducerPartitionFence> {
  let key = ProducerPartitionLeaseKey {
    topic: TOPIC.into(),
    virtual_partition_id,
  };
  let outcome = lease_store
    .acquire_lease(
      key.clone(),
      holder_id.to_string(),
      lease_session_id.to_string(),
      offset_datetime_from_unix_millis(now_ts_ms),
      TimeDuration::milliseconds(100),
    )
    .await?;
  let LeaseAcquireOutcome::Acquired(lease) = outcome else {
    return Err(anyhow!("expected producer lease acquisition"));
  };
  Ok(ProducerPartitionFence {
    key,
    fence: lease.fence,
  })
}

#[tokio::test]
async fn dynamo_fenced_metadata_write_requires_current_producer_lease() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = resources.metadata_store();
  let lease_store = resources.producer_lease_store();
  let original_fence =
    acquire_producer_fence(lease_store.as_ref(), 0, "broker-a", "session-a", 1_000).await?;
  let published = fenced_metadata_segment(1, [0]);
  metadata_store
    .write_segment(
      published.clone(),
      Some(std::slice::from_ref(&original_fence)),
      1_000,
    )
    .await?;

  let replacement_fence =
    acquire_producer_fence(lease_store.as_ref(), 0, "broker-b", "session-b", 1_100).await?;
  let error = metadata_store
    .write_segment(
      fenced_metadata_segment(2, [0]),
      Some(std::slice::from_ref(&original_fence)),
      1_100,
    )
    .await
    .map_err(anyhow::Error::new)
    .expect_err("stale fence must reject metadata publication");
  assert!(error.to_string().contains("producer lease fence was lost"));

  let replacement = fenced_metadata_segment(3, [0]);
  metadata_store
    .write_segment(
      replacement.clone(),
      Some(std::slice::from_ref(&replacement_fence)),
      1_100,
    )
    .await?;
  assert_eq!(
    metadata_store
      .scan_window_from_snowflake(&published.window, None, MetadataReadConsistency::Strong)
      .await?,
    vec![published, replacement]
  );

  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn dynamo_fenced_metadata_write_requires_fences_matching_segment_partitions() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = resources.metadata_store();
  let lease_store = resources.producer_lease_store();
  let unrelated_fence =
    acquire_producer_fence(lease_store.as_ref(), 1, "broker-a", "session-a", 1_000).await?;
  let metadata = fenced_metadata_segment(1, [0]);

  let error = metadata_store
    .write_segment(
      metadata.clone(),
      Some(std::slice::from_ref(&unrelated_fence)),
      1_000,
    )
    .await
    .expect_err("fences for other partitions must reject metadata publication");
  assert!(
    error
      .to_string()
      .contains("fences matching segment partitions")
  );
  assert!(
    metadata_store
      .scan_window_from_snowflake(&metadata.window, None, MetadataReadConsistency::Strong)
      .await?
      .is_empty(),
    "fence validation must reject metadata before it is persisted"
  );

  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn dynamo_fenced_metadata_write_is_atomic_across_multiple_producer_leases() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = resources.metadata_store();
  let lease_store = resources.producer_lease_store();
  let first_fence =
    acquire_producer_fence(lease_store.as_ref(), 0, "broker-a", "session-a", 1_000).await?;
  let stale_fence =
    acquire_producer_fence(lease_store.as_ref(), 1, "broker-a", "session-a", 1_000).await?;
  acquire_producer_fence(lease_store.as_ref(), 1, "broker-b", "session-b", 1_100).await?;

  let metadata = fenced_metadata_segment(1, [0, 1]);
  let error = metadata_store
    .write_segment(metadata.clone(), Some(&[first_fence, stale_fence]), 1_100)
    .await
    .expect_err("one stale partition fence must reject the whole transaction");
  assert!(error.to_string().contains("producer lease fence was lost"));
  assert!(
    metadata_store
      .scan_window_from_snowflake(&metadata.window, None, MetadataReadConsistency::Strong)
      .await?
      .is_empty(),
    "failed transaction must not persist its metadata put"
  );

  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn dynamo_fenced_metadata_write_enforces_transaction_item_boundaries() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = resources.metadata_store();
  let lease_store = resources.producer_lease_store();
  let partition_count = u32::try_from(MAX_FENCED_METADATA_PARTITIONS)
    .expect("DynamoDB transaction partition limit fits in u32");
  let mut fences = Vec::with_capacity(MAX_FENCED_METADATA_PARTITIONS);
  for virtual_partition_id in 0 .. partition_count {
    fences.push(
      acquire_producer_fence(
        lease_store.as_ref(),
        virtual_partition_id,
        "broker-a",
        "session-a",
        1_000,
      )
      .await?,
    );
  }

  let valid_metadata = fenced_metadata_segment(1, 0 .. partition_count);
  metadata_store
    .write_segment(valid_metadata.clone(), Some(&fences), 1_000)
    .await?;

  let extra_fence = acquire_producer_fence(
    lease_store.as_ref(),
    partition_count,
    "broker-a",
    "session-a",
    1_000,
  )
  .await?;
  let mut oversized_fences = fences.clone();
  oversized_fences.push(extra_fence);
  let error = metadata_store
    .write_segment(
      fenced_metadata_segment(2, 0 ..= partition_count),
      Some(&oversized_fences),
      1_000,
    )
    .await
    .expect_err("100 producer fences exceed DynamoDB's transaction item limit");
  assert!(error.to_string().contains("at most 99 partitions"));

  let duplicate_fences = [fences[0].clone(), fences[0].clone()];
  let error = metadata_store
    .write_segment(
      fenced_metadata_segment(3, [0]),
      Some(&duplicate_fences),
      1_000,
    )
    .await
    .expect_err("duplicate lease keys must be rejected before the transaction");
  assert!(error.to_string().contains("duplicate producer lease key"));
  assert_eq!(
    metadata_store
      .scan_window_from_snowflake(
        &valid_metadata.window,
        None,
        MetadataReadConsistency::Strong,
      )
      .await?,
    vec![valid_metadata]
  );

  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn dynamo_producer_leases_fence_stale_broker_sessions() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let lease_store = resources.producer_lease_store();
  let key = ProducerPartitionLeaseKey {
    topic: TOPIC.into(),
    virtual_partition_id: 0,
  };

  let first = lease_store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-1".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
    )
    .await?;
  let LeaseAcquireOutcome::Acquired(first) = first else {
    return Err(anyhow!("expected initial lease acquisition"));
  };
  assert_eq!(first.fence.lease_epoch, 1);

  let renewal = lease_store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-1".to_string(),
      offset_datetime_from_unix_millis(1_050),
      TimeDuration::milliseconds(100),
    )
    .await?;
  let LeaseAcquireOutcome::Acquired(renewal) = renewal else {
    return Err(anyhow!("expected same-session lease renewal"));
  };
  assert_eq!(renewal.fence.lease_epoch, 1);

  let live_takeover = lease_store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-2".to_string(),
      offset_datetime_from_unix_millis(1_100),
      TimeDuration::milliseconds(100),
    )
    .await?;
  assert!(matches!(live_takeover, LeaseAcquireOutcome::HeldByOther(_)));

  let takeover = lease_store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-2".to_string(),
      offset_datetime_from_unix_millis(1_150),
      TimeDuration::milliseconds(100),
    )
    .await?;
  let LeaseAcquireOutcome::Acquired(takeover) = takeover else {
    return Err(anyhow!("expected expired lease takeover"));
  };
  assert_eq!(takeover.fence.lease_epoch, 2);

  let heartbeat = lease_store
    .heartbeat_lease(
      &key,
      "broker-a",
      "session-1",
      offset_datetime_from_unix_millis(1_150),
      TimeDuration::milliseconds(100),
    )
    .await?;
  assert!(matches!(heartbeat, LeaseHeartbeatOutcome::HeldByOther(_)));
  let reservation = lease_store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-1",
      offset_datetime_from_unix_millis(1_150),
      1,
    )
    .await?;
  assert!(matches!(
    reservation,
    SequenceReservationOutcome::HeldByOther(_)
  ));
  let release = lease_store
    .release_lease(
      &key,
      "broker-a",
      "session-1",
      offset_datetime_from_unix_millis(1_150),
    )
    .await?;
  assert!(matches!(release, LeaseReleaseOutcome::HeldByOther(_)));

  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn dynamo_sequence_reservation_rejects_stale_same_holder_session_before_overflow()
-> Result<()> {
  let resources = IntegrationResources::create().await?;
  let lease_store = resources.producer_lease_store();
  let key = ProducerPartitionLeaseKey {
    topic: TOPIC.into(),
    virtual_partition_id: 0,
  };

  let initial = lease_store
    .acquire_lease_and_reserve_sequences(
      key.clone(),
      "broker-a".to_string(),
      "session-1".to_string(),
      offset_datetime_from_unix_millis(1_000),
      TimeDuration::milliseconds(100),
      Some(u64::MAX),
    )
    .await?;
  assert!(matches!(
    initial,
    LeaseAcquireAndReserveOutcome::Acquired { .. }
  ));

  let takeover = lease_store
    .acquire_lease(
      key.clone(),
      "broker-a".to_string(),
      "session-2".to_string(),
      offset_datetime_from_unix_millis(1_100),
      TimeDuration::milliseconds(100),
    )
    .await?;
  assert!(matches!(takeover, LeaseAcquireOutcome::Acquired(_)));

  let reservation = lease_store
    .reserve_sequences(
      &key,
      "broker-a",
      "session-1",
      offset_datetime_from_unix_millis(1_100),
      2,
    )
    .await?;
  assert!(matches!(
    reservation,
    SequenceReservationOutcome::HeldByOther(_)
  ));

  resources.cleanup().await;
  Ok(())
}

async fn new_producer(
  config: ProducerConfig,
  topics: Vec<ProducerTopicConfig>,
  discovery: Arc<dyn BrokerDiscovery>,
  metrics_scope: Scope,
) -> Result<ProducerClientImpl> {
  let transport = Arc::new(GrpcBrokerTransport::new(config.clone()));
  ProducerClientImpl::new(config, topics, discovery, transport, metrics_scope).await
}

#[tokio::test]
async fn consumer_task_stops_while_waiting_for_revocation_ack() -> Result<()> {
  let (consumer, revocation_completed) = stop_aware_revocation_consumer();
  let (event_tx, mut event_rx) = mpsc::unbounded_channel();
  let (stop_tx, stop_rx) = watch::channel(false);
  let task = tokio::spawn(run_consumer_task(consumer, stop_rx, event_tx));

  let event = timeout(Duration::from_secs(1), event_rx.recv())
    .await
    .map_err(|_| anyhow!("consumer task did not request revocation acknowledgement"))?
    .ok_or_else(|| anyhow!("consumer task stopped before requesting revocation acknowledgement"))?;
  let ConsumerTaskEvent::Revoked { ack } = event else {
    return Err(anyhow!(
      "consumer task emitted a batch instead of a revocation"
    ));
  };

  stop_tx
    .send(true)
    .map_err(|_| anyhow!("consumer task stopped before receiving stop signal"))?;
  timeout(Duration::from_secs(1), task)
    .await
    .map_err(|_| anyhow!("consumer task did not stop while revocation acknowledgement was held"))?
    .map_err(|error| anyhow!("consumer task join error: {error}"))??;
  assert!(revocation_completed.load(Ordering::Acquire));
  drop(ack);
  Ok(())
}

// High-level: verifies the baseline single-broker produce/read path and duplicate-scan dedupe.
#[tokio::test]
async fn single_broker_single_record_end_to_end() -> Result<()> {
  // Step 1: Start isolated test infrastructure and one in-process broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  // Step 2: Build a producer against dynamic broker discovery.
  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = new_producer(
    producer_config(),
    vec![producer_topic()],
    Arc::clone(&discovery),
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;

  // Step 3: Produce one record and verify the broker accepted it.
  let ack = produce_message(&producer, b"smoke-key".to_vec(), "smoke-0").await?;
  assert!(ack.attempts >= 1);

  // Step 4: Read from the exact virtual partition and assert the record is visible.
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![ack.virtual_partition_id],
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;

  let deadline = Instant::now() + Duration::from_secs(30);
  let initial_deliveries = drain_reader_until_with_trace(
    &mut reader,
    1,
    now_unix_seconds().saturating_add(3),
    deadline,
  )
  .await?;
  assert_eq!(
    reader_delivery_counts(&initial_deliveries),
    HashMap::from([("smoke-0".to_string(), 1_usize)]),
    "initial reader delivery must contain exactly one smoke record: {initial_deliveries:?}"
  );

  // Step 5: Re-scan to verify dedupe behavior after cursor advancement.
  let duplicate_scan = reader.read_available(now_unix_seconds()).await?;
  assert!(duplicate_scan.is_empty());

  // Step 6: Tear down broker and dependency resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn fenced_metadata_write_fails_when_lease_is_invalidated_before_dynamo_transaction()
-> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(FenceInvalidatingMetadataStore::new(
    resources.metadata_store(),
    resources.producer_lease_store(),
  ));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .metadata_store(metadata_store.clone())
    .fenced_metadata_writes()
    .in_memory_transport()
    .start()
    .await?;
  let mut config = producer_config();
  config.retry_deadline = TimeDuration::milliseconds(100).into_proto();
  let producer = cluster
    .create_producer(config, vec![producer_topic()])
    .await?;
  let error = produce_message(&producer, b"fenced-metadata-fault".to_vec(), "fenced-fault")
    .await
    .expect_err("metadata publication must fail after its producer lease is invalidated");
  assert!(
    format!("{error:#}").contains("producer retries exhausted"),
    "unexpected produce failure: {error:#}"
  );
  assert!(
    metadata_store.rejected_fenced_write(),
    "Dynamo must reject metadata publication with the invalidated producer lease fence"
  );

  let attempted_window = metadata_store
    .attempted_window()
    .await
    .expect("metadata publication was attempted");
  let segments = resources
    .metadata_store()
    .scan_window_from_snowflake(&attempted_window, None, MetadataReadConsistency::Strong)
    .await?;
  assert!(
    segments.is_empty(),
    "a rejected fenced write must not persist segment metadata: {segments:#?}"
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_coalesces_same_partition_requests_into_one_consumer_batch() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let broker_time = Arc::new(framework::ManualTimeProvider::new(
    offset_datetime_from_unix_millis(1_700_000_000_000),
  ));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .broker_flush_max_delay(Duration::from_mins(1))
    .broker_time_provider(broker_time.clone())
    .start()
    .await?;
  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = Arc::new(
    new_producer(
      producer_config(),
      vec![producer_topic()],
      discovery,
      metrics_scope("blob_stream_producer_it"),
    )
    .await?,
  );

  let key = b"coalesced-partition".to_vec();
  let virtual_partition_id = virtual_partition_for_logical(
    logical_partition_for_key(&key, PARTITION_COUNT),
    PARTITION_COUNT,
    0,
  );
  let first_producer = Arc::clone(&producer);
  let first_key = key.clone();
  let first = tokio::spawn(async move {
    first_producer
      .produce_one(ProducerRecord::new(
        TOPIC.into(),
        first_key,
        b"first".to_vec().into(),
        now_unix_millis(),
      ))
      .await
  });

  let buffered_deadline = Instant::now() + Duration::from_secs(2);
  loop {
    let buffered = cluster
      .broker_state_snapshots()
      .await
      .iter()
      .any(|snapshot| {
        snapshot
          .topics
          .iter()
          .flat_map(|topic| &topic.local_partitions)
          .any(|partition| {
            partition.virtual_partition_id == virtual_partition_id
              && partition.buffered_batch_count == 1
          })
      });
    if buffered {
      break;
    }
    if Instant::now() >= buffered_deadline {
      return Err(anyhow!("first request did not enter the broker buffer"));
    }
    tokio::task::yield_now().await;
  }

  let second_producer = Arc::clone(&producer);
  let second = tokio::spawn(async move {
    second_producer
      .produce_one(ProducerRecord::new(
        TOPIC.into(),
        key,
        b"second".to_vec().into(),
        now_unix_millis(),
      ))
      .await
  });
  let buffered_deadline = Instant::now() + Duration::from_secs(2);
  loop {
    let buffered = cluster
      .broker_state_snapshots()
      .await
      .iter()
      .any(|snapshot| {
        snapshot
          .topics
          .iter()
          .flat_map(|topic| &topic.local_partitions)
          .any(|partition| {
            partition.virtual_partition_id == virtual_partition_id
              && partition.buffered_batch_count == 2
          })
      });
    if buffered {
      break;
    }
    if Instant::now() >= buffered_deadline {
      return Err(anyhow!("second request did not join the broker buffer"));
    }
    tokio::task::yield_now().await;
  }

  let mut flush_gate = cluster
    .lifecycle_hooks()
    .arm_broker_for_partition(
      LifecycleEvent::BrokerBeforeFlushPersist,
      virtual_partition_id,
    )
    .await?;
  broker_time.advance(TimeDuration::seconds(60));
  flush_gate.wait_until_reached().await?;
  flush_gate.release()?;

  let first = first
    .await
    .map_err(|error| anyhow!("first request join error: {error}"))??;
  let second = second
    .await
    .map_err(|error| anyhow!("second request join error: {error}"))??;
  assert_eq!(first.virtual_partition_id, virtual_partition_id);
  assert_eq!(second.virtual_partition_id, virtual_partition_id);

  let broker_now = broker_time.now().unix_timestamp();
  let reader_now = broker_now.saturating_add(1);
  let window = Window::for_timestamp(
    broker_time.now(),
    TimeDuration::seconds(WINDOW_SIZE_SECONDS),
  )
  .key(TOPIC);
  let metadata_deadline = Instant::now() + Duration::from_secs(5);
  let segments = loop {
    let segments = resources
      .metadata_store()
      .scan_window_from_snowflake(&window, None, MetadataReadConsistency::Eventual)
      .await?;
    if !segments.is_empty() {
      break segments;
    }
    if Instant::now() >= metadata_deadline {
      return Err(anyhow!("coalesced segment metadata did not become visible"));
    }
    tokio::task::yield_now().await;
  };
  assert_eq!(segments.len(), 1);
  assert_eq!(segments[0].segment_index[&virtual_partition_id].len(), 1);
  assert_eq!(
    segments[0].segment_index[&virtual_partition_id][0].seq_range,
    SeqRange { start: 0, end: 1 }
  );

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![virtual_partition_id],
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;
  let consumer_deadline = Instant::now() + Duration::from_secs(5);
  let batches = loop {
    let batches = reader.read_available(reader_now).await?;
    if !batches.is_empty() {
      break batches;
    }
    if Instant::now() >= consumer_deadline {
      return Err(anyhow!(
        "coalesced segment did not become visible to the consumer"
      ));
    }
    tokio::task::yield_now().await;
  };
  assert_eq!(batches.len(), 1);
  assert_eq!(batches[0].seq_range, SeqRange { start: 0, end: 1 });
  assert_eq!(
    batches[0]
      .records
      .iter()
      .map(|record| record.payload.as_ref())
      .collect::<Vec<_>>(),
    vec![b"first".as_slice(), b"second".as_slice()]
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// Ensures real broker lease reconciliation uses the shared fair plan rather than independently
// selecting a broker per virtual partition.
#[tokio::test]
async fn broker_state_converges_to_balanced_local_partition_ownership() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 2)
    .partition_count(4)
    .in_memory_transport()
    .start()
    .await?;

  let live_nodes = cluster.live_nodes();
  cluster.set_active_nodes(live_nodes);

  let deadline = Instant::now() + Duration::from_secs(15);
  let snapshots = loop {
    let snapshots = cluster.broker_state_snapshots().await;
    let converged = snapshots.len() == 2
      && snapshots.iter().all(|snapshot| {
        snapshot.writer_id == 0
          && snapshot.membership.len() == 2
          && snapshot.ownership.len() == 8
          && snapshot
            .ownership
            .iter()
            .filter(|ownership| ownership.assignment_is_local)
            .all(|ownership| ownership.lease_status == BrokerLeaseStatus::LocalActive)
          && snapshot
            .ownership
            .iter()
            .filter(|ownership| !ownership.assignment_is_local)
            .all(|ownership| ownership.lease_status == BrokerLeaseStatus::RemoteActive)
          && snapshot
            .ownership
            .iter()
            .filter(|ownership| ownership.assignment_is_local)
            .count()
            == 4
      });
    if converged {
      break snapshots;
    }
    if Instant::now() >= deadline {
      return Err(anyhow!("broker ownership did not converge: {snapshots:#?}"));
    }
    tokio::task::yield_now().await;
  };

  for snapshot in snapshots {
    let local_ownership = snapshot
      .ownership
      .iter()
      .filter(|ownership| ownership.assignment_is_local)
      .count();
    assert_eq!(
      local_ownership, 4,
      "broker {} ownership",
      snapshot.holder_id
    );
  }

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies progress across consumer-group rebalance and broker failover under load.
#[tokio::test]
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

  // Step 2: Create producers and start the initial group members.
  let mut producers = Vec::new();
  for _ in 0 .. 4 {
    producers.push(
      new_producer(
        producer_config(),
        vec![producer_topic()],
        Arc::clone(&discovery),
        metrics_scope("blob_stream_producer_it"),
      )
      .await?,
    );
  }

  let mut runtime_0 = consumer_runtime_config("consumer-0");
  let mut runtime_1 = consumer_runtime_config("consumer-1");
  let mut runtime_2 = consumer_runtime_config("consumer-2");
  for runtime in [&mut runtime_0, &mut runtime_1, &mut runtime_2] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("consumer read config missing"))?
      .strongly_consistent_metadata_reads = Some(true);
  }

  let (event_tx, mut event_rx) = mpsc::unbounded_channel();
  let (stop_tx_0, stop_rx_0) = watch::channel(false);
  let (stop_tx_1, stop_rx_1) = watch::channel(false);
  let (stop_tx_2, stop_rx_2) = watch::channel(false);
  let consumer_0_task = tokio::spawn(run_consumer_task(
    Box::new(cluster.create_consumer(&runtime_0).await?),
    stop_rx_0,
    event_tx.clone(),
  ));
  let consumer_1_task = tokio::spawn(run_consumer_task(
    Box::new(cluster.create_consumer(&runtime_1).await?),
    stop_rx_1,
    event_tx.clone(),
  ));

  // Step 3: Produce and drain phase 1 traffic through the consumer group.
  let mut expected_ids = HashSet::new();
  for message_id in 0 .. 32 {
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

  let mut delivery_traces = ConsumerDeliveryTraces::new();
  let mut revocation_count = 0usize;
  timeout(Duration::from_secs(20), async {
    while delivery_traces.len() < expected_ids.len() {
      let event = event_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("initial consumer tasks stopped before phase 1 drained"))?;
      handle_consumer_event_with_trace(event, &mut delivery_traces, &mut revocation_count);
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("consumer group did not drain phase 1 traffic"))??;

  // Step 4: Simulate consumer scale-out by starting a third member and assert revocation.
  let scale_out_target = revocation_count + 1;
  let consumer_2_task = tokio::spawn(run_consumer_task(
    Box::new(cluster.create_consumer(&runtime_2).await?),
    stop_rx_2,
    event_tx.clone(),
  ));
  timeout(Duration::from_secs(10), async {
    while revocation_count < scale_out_target {
      let event = event_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("consumer tasks stopped before scale-out revocation"))?;
      handle_consumer_event_with_trace(event, &mut delivery_traces, &mut revocation_count);
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("consumer group did not revoke a partition after scale-out"))??;
  assert!(
    revocation_count >= scale_out_target,
    "expected revocation after consumer membership change"
  );

  // Step 5: Fail over producer traffic while the consumer group remains active.
  let live_nodes = cluster.live_nodes();
  let failover_node = live_nodes
    .iter()
    .find(|node| node.node_id != active_node.node_id)
    .cloned()
    .ok_or_else(|| anyhow!("expected secondary broker for failover"))?;
  cluster.set_active_nodes(vec![failover_node.clone()]);
  cluster.remove_broker_by_id(&active_node.node_id).await?;

  for message_id in 0 .. 32 {
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

  timeout(Duration::from_secs(30), async {
    while delivery_traces.len() < expected_ids.len() {
      let event = event_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("consumer tasks stopped before failover traffic drained"))?;
      handle_consumer_event_with_trace(event, &mut delivery_traces, &mut revocation_count);
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("consumer group did not drain traffic after broker failover"))??;
  let delivered_id_counts = delivery_counts(&delivery_traces);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count >= 1),
    "autoscale and broker failover require at-least-once delivery: \
     counts={delivered_id_counts:?}, traces={delivery_traces:?}"
  );
  assert!(
    delivery_members(&delivery_traces).iter().all(|member_id| [
      "consumer-0",
      "consumer-1",
      "consumer-2"
    ]
    .contains(&member_id.as_str())),
    "autoscale delivery came from a non-group member: {delivery_traces:?}"
  );

  let maximum_offsets = maximum_delivery_offsets(&delivery_traces);
  let leases = timeout(Duration::from_secs(5), async {
    loop {
      let leases = cluster
        .consumer_lease_store()
        .list_group_leases(TOPIC, "integration-group")
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
        return Ok::<_, anyhow::Error>(leases);
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("consumer group did not durably commit every observed delivery"))??;
  assert_eq!(leases.len(), PARTITION_COUNT as usize);
  assert!(
    leases.iter().all(|lease| {
      ["consumer-0", "consumer-1", "consumer-2"].contains(&lease.owner_id.as_str())
    }),
    "consumer group has unexpected owners after broker failover: {leases:?}"
  );
  for (partition_id, maximum_offset) in maximum_offsets {
    let lease = leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == partition_id)
      .ok_or_else(|| anyhow!("missing failover lease for partition {partition_id}"))?;
    assert!(
      lease.committed_cursor.as_ref().is_some_and(|cursor| {
        cursor.seq_end >= maximum_offset && cursor.source_checkpoint.is_some()
      }),
      "consumer group did not durably commit partition {partition_id}: {lease:?}"
    );
  }

  // Step 6: Clean up all spawned resources.
  let _ = stop_tx_0.send(true);
  let _ = stop_tx_1.send(true);
  let _ = stop_tx_2.send(true);
  consumer_0_task
    .await
    .map_err(|error| anyhow!("consumer-0 task join error: {error}"))??;
  consumer_1_task
    .await
    .map_err(|error| anyhow!("consumer-1 task join error: {error}"))??;
  consumer_2_task
    .await
    .map_err(|error| anyhow!("consumer-2 task join error: {error}"))??;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies cursor monotonicity and dedupe behavior for repeated scans on one broker.
#[tokio::test]
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

  let producer = new_producer(
    producer_config(),
    vec![producer_topic()],
    Arc::clone(&producer_discovery),
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;

  // Step 2: Produce directly without test-level retries. Retries are handled by producer internals.
  let first_ack = produce_message(&producer, b"stable-key".to_vec(), "fault-0").await?;
  assert!(first_ack.attempts >= 1);

  // Step 3: Read the produced data and validate dedupe/cursor monotonicity on repeated scans.
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    (0 .. PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;

  let deadline = Instant::now() + Duration::from_secs(30);
  let initial_deliveries = drain_reader_until_with_trace(
    &mut reader,
    1,
    now_unix_seconds().saturating_add(3),
    deadline,
  )
  .await?;
  assert_eq!(
    reader_delivery_counts(&initial_deliveries),
    HashMap::from([("fault-0".to_string(), 1_usize)]),
    "initial reader delivery must contain exactly one fault record: {initial_deliveries:?}"
  );

  let duplicate_scan = reader.read_available(now_unix_seconds()).await?;
  assert!(duplicate_scan.is_empty());
  let cursors_after_duplicate_scan = reader.cursors();
  let duplicate_scan_again = reader.read_available(now_unix_seconds()).await?;
  assert!(duplicate_scan_again.is_empty());
  assert_eq!(reader.cursors(), cursors_after_duplicate_scan);

  // Step 4: Clean up resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies consumer restart resumes from committed offsets without replaying old data.
#[tokio::test]
async fn consumer_restart_resume_from_committed_offsets() -> Result<()> {
  // Step 1: Start isolated test infrastructure and a single broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = new_producer(
    producer_config(),
    vec![producer_topic()],
    Arc::clone(&discovery),
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;

  // Step 2: Produce phase 1 records.
  let mut phase1_expected = HashSet::new();
  for message_id in 0 .. 24 {
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
  let runtime = consumer_runtime_config("consumer-0");
  let runtime_group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("consumer group config missing"))?;
  let consumer_membership_store = resources.consumer_membership_store();

  let mut consumer = Box::new(cluster.create_consumer(&runtime).await?);
  consumer.start()?;

  let mut phase1_delivery_counts = HashMap::new();
  let mut phase1_max_offsets = HashMap::<VirtualPartitionId, u64>::new();
  let deadline = Instant::now() + Duration::from_secs(30);
  while phase1_delivery_counts.len() < phase1_expected.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while consuming phase1: expected={}, consumed={}",
        phase1_expected.len(),
        phase1_delivery_counts.len()
      ));
    }

    let next_result = timeout(Duration::from_secs(2), consumer.next()).await;
    let Ok(Ok(next_result)) = next_result else {
      continue;
    };

    match next_result {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => {
        let id = String::from_utf8(record.record.payload.to_vec())
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        if !phase1_expected.contains(&id) {
          return Err(anyhow!("unexpected phase1 record: {id}"));
        }
        *phase1_delivery_counts.entry(id).or_insert(0_usize) += 1;
        phase1_max_offsets
          .entry(record.virtual_partition_id)
          .and_modify(|offset| *offset = (*offset).max(record.offset))
          .or_insert(record.offset);

        consumer.store_offset(record.virtual_partition_id, record.offset)?;
      },
    }
  }
  assert_eq!(
    phase1_delivery_counts
      .keys()
      .cloned()
      .collect::<HashSet<_>>(),
    phase1_expected
  );
  assert!(
    phase1_delivery_counts.values().all(|count| *count == 1),
    "restart phase 1 must deliver every record exactly once: {phase1_delivery_counts:?}"
  );
  let _ = consumer.commit().await?;

  let committed_leases = cluster
    .consumer_lease_store()
    .list_group_leases(
      runtime_group.topic.as_str(),
      runtime_group.group_id.as_str(),
    )
    .await?;
  for (partition_id, maximum_offset) in &phase1_max_offsets {
    let lease = committed_leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == *partition_id)
      .ok_or_else(|| anyhow!("missing phase1 lease for partition {partition_id}"))?;
    assert!(
      lease.committed_cursor.as_ref().is_some_and(|cursor| {
        cursor.seq_end >= *maximum_offset && cursor.source_checkpoint.is_some()
      }),
      "phase 1 did not durably commit partition {partition_id}: {lease:?}"
    );
  }

  // Step 4: Verify graceful shutdown commits before release, then deregisters after release.
  let hooks = cluster.lifecycle_hooks();
  let mut before_commit = hooks.arm(LifecycleEvent::ConsumerBeforeCommit).await?;
  let mut shutdown_commit_finished = hooks
    .arm(LifecycleEvent::ConsumerShutdownCommitFinished)
    .await?;
  let mut before_release = hooks
    .arm(LifecycleEvent::ConsumerBeforeReleaseOwned)
    .await?;
  let mut before_deregister = hooks
    .arm(LifecycleEvent::ConsumerBeforeDeregisterMember)
    .await?;
  let shutdown_task = tokio::spawn(async move { consumer.shutdown().await });

  timeout(Duration::from_secs(5), before_commit.wait_until_reached())
    .await
    .map_err(|_| anyhow!("shutdown did not reach the final commit boundary"))??;
  before_commit.release()?;
  timeout(
    Duration::from_secs(5),
    shutdown_commit_finished.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("shutdown did not finish the final commit"))??;

  let leases_after_shutdown_commit = cluster
    .consumer_lease_store()
    .list_group_leases(
      runtime_group.topic.as_str(),
      runtime_group.group_id.as_str(),
    )
    .await?;
  assert_eq!(
    leases_after_shutdown_commit
      .iter()
      .map(|lease| (&lease.key, &lease.owner_id, &lease.committed_cursor))
      .collect::<Vec<_>>(),
    committed_leases
      .iter()
      .map(|lease| (&lease.key, &lease.owner_id, &lease.committed_cursor))
      .collect::<Vec<_>>(),
    "final commit changed durable ownership or rewound a committed cursor"
  );
  shutdown_commit_finished.release()?;

  timeout(Duration::from_secs(5), before_release.wait_until_reached())
    .await
    .map_err(|_| anyhow!("shutdown did not reach the lease release boundary"))??;
  let active_members = consumer_membership_store
    .list_active_members(
      runtime_group.topic.as_str(),
      runtime_group.group_id.as_str(),
      offset_datetime_from_unix_millis(now_unix_millis()),
    )
    .await?;
  assert_eq!(
    member_ids(&active_members),
    vec![runtime_group.member_id.to_string()]
  );
  before_release.release()?;

  timeout(
    Duration::from_secs(5),
    before_deregister.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("shutdown did not reach the member deregistration boundary"))??;
  let released_leases = cluster
    .consumer_lease_store()
    .list_group_leases(
      runtime_group.topic.as_str(),
      runtime_group.group_id.as_str(),
    )
    .await?;
  assert!(
    released_leases
      .iter()
      .all(|lease| lease.lease_expiration_ts_ms <= now_unix_millis()),
    "consumer membership deregistration began before owned leases were released"
  );
  before_deregister.release()?;
  shutdown_task
    .await
    .map_err(|error| anyhow!("consumer shutdown task failed: {error}"))??;

  assert!(
    consumer_membership_store
      .list_active_members(
        runtime_group.topic.as_str(),
        runtime_group.group_id.as_str(),
        offset_datetime_from_unix_millis(now_unix_millis()),
      )
      .await?
      .is_empty(),
    "consumer remained registered after graceful shutdown"
  );

  let mut phase2_expected = HashSet::new();
  for message_id in 0 .. 24 {
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
  let mut resumed_consumer = Box::new(cluster.create_consumer(&runtime).await?);
  resumed_consumer.start()?;

  let mut phase2_delivery_counts = HashMap::new();
  let mut phase2_max_offsets = HashMap::<VirtualPartitionId, u64>::new();
  let mut replayed_phase1_counts = HashMap::new();
  let deadline = Instant::now() + Duration::from_secs(30);
  while phase2_delivery_counts.len() < phase2_expected.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while consuming phase2: expected={}, consumed={}",
        phase2_expected.len(),
        phase2_delivery_counts.len()
      ));
    }

    let next_result = timeout(Duration::from_secs(2), resumed_consumer.next()).await;
    let Ok(Ok(next_result)) = next_result else {
      continue;
    };

    match next_result {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => {
        let id = String::from_utf8(record.record.payload.to_vec())
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        if phase1_expected.contains(&id) {
          *replayed_phase1_counts.entry(id.clone()).or_insert(0_usize) += 1;
        } else if phase2_expected.contains(&id) {
          *phase2_delivery_counts.entry(id).or_insert(0_usize) += 1;
          phase2_max_offsets
            .entry(record.virtual_partition_id)
            .and_modify(|offset| *offset = (*offset).max(record.offset))
            .or_insert(record.offset);
        } else {
          return Err(anyhow!("unexpected phase2 record: {id}"));
        }

        resumed_consumer.store_offset(record.virtual_partition_id, record.offset)?;
      },
    }
  }

  assert!(
    replayed_phase1_counts.is_empty(),
    "resumed consumer replayed phase1 records: {replayed_phase1_counts:?}"
  );
  assert_eq!(
    phase2_delivery_counts
      .keys()
      .cloned()
      .collect::<HashSet<_>>(),
    phase2_expected
  );
  assert!(
    phase2_delivery_counts.values().all(|count| *count == 1),
    "restart phase 2 must deliver every record exactly once: {phase2_delivery_counts:?}"
  );
  let _ = resumed_consumer.commit().await?;

  let resumed_leases = cluster
    .consumer_lease_store()
    .list_group_leases(
      runtime_group.topic.as_str(),
      runtime_group.group_id.as_str(),
    )
    .await?;
  for (partition_id, maximum_offset) in &phase2_max_offsets {
    let lease = resumed_leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == *partition_id)
      .ok_or_else(|| anyhow!("missing phase2 lease for partition {partition_id}"))?;
    assert!(
      lease.committed_cursor.as_ref().is_some_and(|cursor| {
        cursor.seq_end >= *maximum_offset && cursor.source_checkpoint.is_some()
      }),
      "phase 2 did not durably commit partition {partition_id}: {lease:?}"
    );
  }

  // Step 6: Clean up all resources.
  let _ = resumed_consumer.shutdown().await;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies graceful shutdown durably commits a staged offset before releasing its
// lease, so a restarted group member does not replay the record.
#[tokio::test]
async fn graceful_shutdown_final_checkpoint_commits_staged_record_before_release() -> Result<()> {
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(OffsetDateTime::now_utc()));
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

  let mut runtime_a = consumer_runtime_config("shutdown-final-commit-a");
  let mut runtime_b = consumer_runtime_config("shutdown-final-commit-b");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("shutdown final-commit read config missing"))?
      .strongly_consistent_metadata_reads = Some(true);
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("shutdown final-commit group config missing"))?;
  let hooks = cluster.lifecycle_hooks();
  let staged_id = "shutdown-final-commit-staged";
  let staged_key = b"shutdown-final-commit-key".to_vec();
  let staged_ack = produce_message(&producer, staged_key.clone(), staged_id).await?;

  let mut initial_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "shutdown-final-commit-a",
      None,
      None,
    )
    .await?;
  let mut consumer_a = Box::new(cluster.create_consumer(&runtime_a).await?);
  consumer_a.start()?;
  consumer_time.advance(TimeDuration::seconds(1));
  timeout(
    Duration::from_secs(5),
    initial_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("initial consumer did not reach its rebalance boundary"))??;
  initial_rebalance.release()?;

  let staged_offset = timeout(Duration::from_secs(5), async {
    loop {
      match timeout(Duration::from_millis(250), consumer_a.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow!("initial consumer next failed: {error}")),
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
  .map_err(|_| anyhow!("initial consumer did not stage the shutdown record"))??;

  let lease_before_shutdown = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == staged_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing initial consumer lease"))?;
  assert_eq!(lease_before_shutdown.owner_id, group.member_id.as_str());
  assert!(
    lease_before_shutdown.committed_cursor.is_none(),
    "staging an offset must not persist it before shutdown's final commit: \
     {lease_before_shutdown:?}"
  );

  let mut before_commit = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeCommit,
      "shutdown-final-commit-a",
      None,
      None,
    )
    .await?;
  let mut commit_finished = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerShutdownCommitFinished,
      "shutdown-final-commit-a",
      None,
      None,
    )
    .await?;
  let mut before_release = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeReleaseOwned,
      "shutdown-final-commit-a",
      None,
      None,
    )
    .await?;
  let shutdown_task = tokio::spawn(async move { consumer_a.shutdown().await });

  timeout(Duration::from_secs(5), before_commit.wait_until_reached())
    .await
    .map_err(|_| anyhow!("shutdown did not reach its final commit boundary"))??;
  let lease_before_final_commit = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == staged_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing lease before shutdown final commit"))?;
  assert!(lease_before_final_commit.committed_cursor.is_none());
  before_commit.release()?;

  timeout(Duration::from_secs(5), commit_finished.wait_until_reached())
    .await
    .map_err(|_| anyhow!("shutdown did not finish its final commit"))??;
  let lease_after_final_commit = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == staged_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing lease after shutdown final commit"))?;
  assert!(
    lease_after_final_commit
      .committed_cursor
      .as_ref()
      .is_some_and(|cursor| {
        cursor.seq_end == staged_offset && cursor.source_checkpoint.is_some()
      }),
    "shutdown final commit did not persist the staged offset: {lease_after_final_commit:?}"
  );
  assert_eq!(lease_after_final_commit.owner_id, group.member_id.as_str());
  commit_finished.release()?;

  timeout(Duration::from_secs(5), before_release.wait_until_reached())
    .await
    .map_err(|_| anyhow!("shutdown did not reach its release boundary"))??;
  let lease_before_release = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == staged_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing lease before shutdown release"))?;
  assert!(
    lease_before_release
      .committed_cursor
      .as_ref()
      .is_some_and(|cursor| {
        cursor.seq_end == staged_offset && cursor.source_checkpoint.is_some()
      }),
    "shutdown release began before the staged offset was durable: {lease_before_release:?}"
  );
  before_release.release()?;
  shutdown_task
    .await
    .map_err(|error| anyhow!("shutdown task failed: {error}"))??;

  let logical_now_ms = i64::try_from(consumer_time.now().unix_timestamp_nanos() / 1_000_000)?;
  assert!(
    cluster
      .consumer_membership_store()
      .list_active_members(
        group.topic.as_str(),
        group.group_id.as_str(),
        offset_datetime_from_unix_millis(logical_now_ms),
      )
      .await?
      .is_empty(),
    "successful shutdown must deregister the original member"
  );

  let mut restart_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "shutdown-final-commit-b",
      None,
      None,
    )
    .await?;
  let mut consumer_b = Box::new(cluster.create_consumer(&runtime_b).await?);
  consumer_b.start()?;
  consumer_time.advance(TimeDuration::seconds(1));
  timeout(
    Duration::from_secs(5),
    restart_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("restarted consumer did not reach its rebalance boundary"))??;
  restart_rebalance.release()?;

  let marker_id = "shutdown-final-commit-marker";
  let marker_ack = produce_message(&producer, staged_key, marker_id).await?;
  assert_eq!(
    marker_ack.virtual_partition_id,
    staged_ack.virtual_partition_id
  );
  let mut restarted_counts = HashMap::new();
  let marker_offset = timeout(Duration::from_secs(5), async {
    loop {
      match timeout(Duration::from_millis(250), consumer_b.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow!("restarted consumer next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          *restarted_counts.entry(id.clone()).or_insert(0_usize) += 1;
          if id == staged_id {
            return Err(anyhow!(
              "restarted consumer replayed staged shutdown record"
            ));
          }
          if id != marker_id {
            return Err(anyhow!(
              "restarted consumer delivered unexpected record: {id}"
            ));
          }
          consumer_b.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_b.commit().await?;
          return Ok::<_, anyhow::Error>(record.offset);
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("restarted consumer did not receive the marker record"))??;
  assert_eq!(
    restarted_counts,
    HashMap::from([(marker_id.to_string(), 1)]),
    "restarted consumer delivery contract changed: {restarted_counts:?}"
  );

  let final_lease = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == marker_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing restarted consumer lease"))?;
  assert!(
    final_lease.committed_cursor.as_ref().is_some_and(|cursor| {
      cursor.seq_end >= marker_offset && cursor.source_checkpoint.is_some()
    }),
    "restarted consumer did not durably commit its marker: {final_lease:?}"
  );

  Box::new(consumer_b).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies a persisted checkpoint reuses immutable historical metadata across
// capacity-limited prefetch cycles.
#[tokio::test]
async fn iterator_reuses_mature_recovery_metadata_across_prefetch_capacity_cycles() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let blob_store = resources.blob_store();
  let metadata_store = resources.metadata_store();
  let lease_store = resources.consumer_lease_store();
  let membership_store = resources.consumer_membership_store();
  let now_ts_ms = now_unix_millis();
  let current_window_start = Window::for_timestamp(
    offset_datetime_from_unix_millis(now_ts_ms),
    TimeDuration::seconds(WINDOW_SIZE_SECONDS),
  )
  .start
  .unix_timestamp();
  let recovery_window_start = current_window_start - (2 * WINDOW_SIZE_SECONDS);
  let published_ts_ms = now_ts_ms.saturating_sub(10_000);
  let recovery_checkpoint_snowflake =
    SnowflakeId::minimum_for_timestamp(OffsetDateTime::from_unix_timestamp(recovery_window_start)?);

  for (snowflake_id, sequence, payload) in [
    (
      recovery_checkpoint_snowflake.as_u64(),
      1,
      "recovery-checkpoint",
    ),
    (
      recovery_checkpoint_snowflake.as_u64().saturating_add(1),
      2,
      "recovery-cache-first",
    ),
    (
      recovery_checkpoint_snowflake.as_u64().saturating_add(2),
      3,
      "recovery-cache-second",
    ),
    (
      recovery_checkpoint_snowflake.as_u64().saturating_add(3),
      4,
      "recovery-cache-third",
    ),
  ] {
    write_recovery_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      0,
      recovery_window_start,
      snowflake_id,
      sequence,
      payload,
      published_ts_ms,
    )
    .await?;
  }

  let recovery_window = TopicWindowKey {
    topic: TOPIC.to_string(),
    window_start_unix_seconds: recovery_window_start,
  };
  let visibility_deadline = Instant::now() + Duration::from_secs(5);
  loop {
    let visible = metadata_store
      .scan_window_from_snowflake(
        &recovery_window,
        Some(SnowflakeId(
          recovery_checkpoint_snowflake.as_u64().saturating_add(3),
        )),
        MetadataReadConsistency::Eventual,
      )
      .await?
      .iter()
      .any(|segment| {
        segment.snowflake_id
          == SnowflakeId(recovery_checkpoint_snowflake.as_u64().saturating_add(3))
      });
    if visible {
      break;
    }
    if Instant::now() >= visibility_deadline {
      return Err(anyhow!(
        "deadline exceeded waiting for recovery metadata: window_start={recovery_window_start}"
      ));
    }
    tokio::task::yield_now().await;
  }

  let lease_key = ConsumerGroupLeaseKey {
    topic: TOPIC.to_string(),
    group_id: "integration-group".to_string(),
    virtual_partition_id: 0,
  };
  lease_store
    .assign_partition(
      lease_key.clone(),
      "previous-owner".to_string(),
      1,
      offset_datetime_from_unix_millis(now_ts_ms),
      TimeDuration::milliseconds(2_000),
    )
    .await?;
  lease_store
    .commit_cursor(
      &lease_key,
      "previous-owner",
      1,
      offset_datetime_from_unix_millis(now_ts_ms),
      CommittedCursor {
        virtual_partition_id: 0,
        seq_end: 1,
        source_checkpoint: Some(CommittedSourceCheckpoint {
          window_start_unix_seconds: recovery_window_start,
          snowflake_id: recovery_checkpoint_snowflake.as_u64(),
        }),
      },
    )
    .await?;
  let _ = lease_store
    .release_partition(
      &lease_key,
      "previous-owner",
      1,
      offset_datetime_from_unix_millis(now_ts_ms),
    )
    .await?;

  let counting_metadata_store = Arc::new(CountingWindowMetadataStore::new(
    Arc::clone(&metadata_store),
    recovery_window_start,
  ));
  let consumer_metadata_store: Arc<dyn MetadataStore> = counting_metadata_store.clone();
  let mut runtime = consumer_runtime_config("recovery-cache-member");
  let read = runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("recovery reader config missing"))?;
  read.strongly_consistent_metadata_reads = Some(true);
  read.prefetch_max_bytes = Some(1);
  let runtime_group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("recovery group config missing"))?;
  let mut iterator = Box::new(
    ConsumerIteratorImpl::from_config(
      &runtime,
      Arc::clone(&blob_store),
      consumer_metadata_store,
      Arc::clone(&lease_store),
      Arc::clone(&membership_store),
      Arc::new(MembershipCoordinationSource::new(
        runtime_group.topic.to_string(),
        runtime_group.group_id.to_string(),
        runtime_group.member_id.to_string(),
        (0 .. PARTITION_COUNT).collect(),
        Arc::clone(&membership_store),
      )),
      framework::rejecting_broker_metadata_query(),
      framework::rejecting_broker_blob_range_query(),
      metrics_scope("blob_stream_consumer_it"),
      TimeDuration::days(1),
      DEFAULT_MAX_METADATA_PUBLICATION_LAG,
      None,
    )
    .await?,
  );
  iterator.start()?;

  let expected = HashSet::from([
    "recovery-cache-first".to_string(),
    "recovery-cache-second".to_string(),
    "recovery-cache-third".to_string(),
  ]);
  let mut delivery_counts = HashMap::new();
  let mut maximum_offset = None;
  let deadline = Instant::now() + Duration::from_secs(15);
  while delivery_counts.len() < expected.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded recovering cached metadata records: counts={delivery_counts:?}"
      ));
    }
    let next_result = timeout(Duration::from_secs(2), iterator.next()).await;
    let Ok(Ok(next_result)) = next_result else {
      continue;
    };
    match next_result {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => {
        let payload = String::from_utf8(record.record.payload.to_vec())
          .map_err(|error| anyhow!("recovery payload was not utf-8: {error}"))?;
        if !expected.contains(&payload) {
          return Err(anyhow!("unexpected recovery payload: {payload}"));
        }
        *delivery_counts.entry(payload).or_insert(0_usize) += 1;
        maximum_offset =
          Some(maximum_offset.map_or(record.offset, |offset: u64| offset.max(record.offset)));
      },
    }
  }

  assert!(
    delivery_counts.values().all(|count| *count == 1),
    "cached recovery must deliver each unread record exactly once: {delivery_counts:?}"
  );
  assert_eq!(
    counting_metadata_store.scan_count(),
    1,
    "a mature recovery window must be queried once across prefetch capacity cycles"
  );
  iterator.store_offset(0, maximum_offset.expect("recovery delivered records"))?;
  let _ = iterator.commit().await?;
  iterator.shutdown().await?;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies persisted recovery advances through capacity boundaries and rotates to an
// independent partition before resuming a dense historical recovery.
#[tokio::test]
async fn iterator_recovers_persisted_checkpoint_across_multiple_recovery_slices() -> Result<()> {
  const RECOVERY_WINDOW_COUNT: i64 = 80;

  let resources = IntegrationResources::create().await?;
  let blob_store = resources.blob_store();
  let metadata_store = resources.metadata_store();
  let lease_store = resources.consumer_lease_store();
  let membership_store = resources.consumer_membership_store();
  let now_ts_ms = now_unix_millis();
  let current_window_start = Window::for_timestamp(
    offset_datetime_from_unix_millis(now_ts_ms),
    TimeDuration::seconds(WINDOW_SIZE_SECONDS),
  )
  .start
  .unix_timestamp();
  let recovery_start = current_window_start - (RECOVERY_WINDOW_COUNT * WINDOW_SIZE_SECONDS);
  let published_ts_ms = now_ts_ms.saturating_sub(10_000);
  let other_partition_checkpoint_snowflake =
    SnowflakeId::minimum_for_timestamp(OffsetDateTime::from_unix_timestamp(current_window_start)?);
  let other_partition_record_snowflake = SnowflakeId(
    other_partition_checkpoint_snowflake
      .as_u64()
      .saturating_add(1),
  );

  // Partition 0 begins with a dense historical recovery. Its first unread batch fills the tiny
  // prefetch budget, forcing the following window to become the recovery boundary.
  for (window_offset, snowflake_id, sequence, payload) in [
    (0, 1, 1, "recovery-checkpoint"),
    (1, 2, 2, "recovery-capacity-first"),
    (2, 3, 3, "recovery-capacity-deferred"),
    (31, 4, 4, "recovery-first-slice"),
    (32, 5, 5, "recovery-second-slice"),
    (64, 6, 6, "recovery-final-slice"),
    (80, 7, 7, "recovery-cutover"),
  ] {
    write_recovery_segment(
      blob_store.as_ref(),
      metadata_store.as_ref(),
      0,
      recovery_start + (window_offset * WINDOW_SIZE_SECONDS),
      snowflake_id,
      sequence,
      payload,
      published_ts_ms,
    )
    .await?;
  }

  // Partition 1 has independent, current-window recovery that must not wait for partition 0's
  // historical slice to drain.
  write_recovery_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    1,
    current_window_start,
    other_partition_checkpoint_snowflake.as_u64(),
    1,
    "recovery-other-checkpoint",
    published_ts_ms,
  )
  .await?;
  write_recovery_segment(
    blob_store.as_ref(),
    metadata_store.as_ref(),
    1,
    current_window_start,
    other_partition_record_snowflake.as_u64(),
    2,
    "recovery-other-partition",
    published_ts_ms,
  )
  .await?;

  // Dynamo scans are eventually consistent. Establish the fixture before starting recovery so
  // this test exercises bounded traversal rather than timing-dependent metadata visibility.
  for (window_offset, snowflake_id) in [(0, 1), (1, 2), (2, 3), (31, 4), (32, 5), (64, 6), (80, 7)]
  {
    let window = TopicWindowKey {
      topic: TOPIC.to_string(),
      window_start_unix_seconds: recovery_start + (window_offset * WINDOW_SIZE_SECONDS),
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
      let visible = metadata_store
        .scan_window_from_snowflake(
          &window,
          Some(SnowflakeId(snowflake_id)),
          MetadataReadConsistency::Eventual,
        )
        .await?
        .iter()
        .any(|segment| segment.snowflake_id == SnowflakeId(snowflake_id));
      if visible {
        break;
      }
      if Instant::now() >= deadline {
        return Err(anyhow!(
          "deadline exceeded waiting for recovery metadata: window_start={}, \
           snowflake_id={snowflake_id}",
          window.window_start_unix_seconds
        ));
      }
      tokio::task::yield_now().await;
    }
  }

  for snowflake_id in [
    other_partition_checkpoint_snowflake.as_u64(),
    other_partition_record_snowflake.as_u64(),
  ] {
    let window = TopicWindowKey {
      topic: TOPIC.to_string(),
      window_start_unix_seconds: current_window_start,
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
      let visible = metadata_store
        .scan_window_from_snowflake(
          &window,
          Some(SnowflakeId(snowflake_id)),
          MetadataReadConsistency::Eventual,
        )
        .await?
        .iter()
        .any(|segment| segment.snowflake_id == SnowflakeId(snowflake_id));
      if visible {
        break;
      }
      if Instant::now() >= deadline {
        return Err(anyhow!(
          "deadline exceeded waiting for recovery metadata: window_start={}, \
           snowflake_id={snowflake_id}",
          window.window_start_unix_seconds
        ));
      }
      tokio::task::yield_now().await;
    }
  }

  for (partition_id, source_window_start, snowflake_id) in [
    (0, recovery_start, 1),
    (
      1,
      current_window_start,
      other_partition_checkpoint_snowflake.as_u64(),
    ),
  ] {
    let lease_key = ConsumerGroupLeaseKey {
      topic: TOPIC.to_string(),
      group_id: "integration-group".to_string(),
      virtual_partition_id: partition_id,
    };
    lease_store
      .assign_partition(
        lease_key.clone(),
        "previous-owner".to_string(),
        1,
        offset_datetime_from_unix_millis(now_ts_ms),
        TimeDuration::milliseconds(2_000),
      )
      .await?;
    lease_store
      .commit_cursor(
        &lease_key,
        "previous-owner",
        1,
        offset_datetime_from_unix_millis(now_ts_ms),
        CommittedCursor {
          virtual_partition_id: partition_id,
          seq_end: 1,
          source_checkpoint: Some(CommittedSourceCheckpoint {
            window_start_unix_seconds: source_window_start,
            snowflake_id,
          }),
        },
      )
      .await?;
    let _ = lease_store
      .release_partition(
        &lease_key,
        "previous-owner",
        1,
        offset_datetime_from_unix_millis(now_ts_ms),
      )
      .await?;
  }

  let mut runtime = consumer_runtime_config("recovery-member");
  let read = runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("recovery reader config missing"))?;
  read.strongly_consistent_metadata_reads = Some(true);
  read.prefetch_max_bytes = Some(1);
  let runtime_group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("recovery group config missing"))?;
  let mut iterator = Box::new(
    ConsumerIteratorImpl::from_config(
      &runtime,
      Arc::clone(&blob_store),
      Arc::clone(&metadata_store),
      Arc::clone(&lease_store),
      Arc::clone(&membership_store),
      Arc::new(MembershipCoordinationSource::new(
        runtime_group.topic.to_string(),
        runtime_group.group_id.to_string(),
        runtime_group.member_id.to_string(),
        (0 .. PARTITION_COUNT).collect(),
        Arc::clone(&membership_store),
      )),
      framework::rejecting_broker_metadata_query(),
      framework::rejecting_broker_blob_range_query(),
      metrics_scope("blob_stream_consumer_it"),
      TimeDuration::days(1),
      DEFAULT_MAX_METADATA_PUBLICATION_LAG,
      None,
    )
    .await?,
  );
  iterator.start()?;

  let expected = HashSet::from([
    "recovery-capacity-first".to_string(),
    "recovery-capacity-deferred".to_string(),
    "recovery-first-slice".to_string(),
    "recovery-second-slice".to_string(),
    "recovery-final-slice".to_string(),
    "recovery-cutover".to_string(),
    "recovery-other-partition".to_string(),
  ]);
  let mut recovery_delivery_counts = HashMap::new();
  let mut recovery_delivery_order = Vec::new();
  let mut recovery_max_offsets = HashMap::<VirtualPartitionId, u64>::new();
  let deadline = Instant::now() + Duration::from_secs(15);
  while recovery_delivery_counts.len() < expected.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded recovering retained records: counts={recovery_delivery_counts:?}"
      ));
    }

    let next_result = timeout(Duration::from_secs(2), iterator.next()).await;
    let Ok(Ok(next_result)) = next_result else {
      continue;
    };
    match next_result {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => {
        let payload = String::from_utf8(record.record.payload.to_vec())
          .map_err(|error| anyhow!("recovery payload was not utf-8: {error}"))?;
        assert_ne!(payload, "recovery-checkpoint");
        if !expected.contains(&payload) {
          return Err(anyhow!("unexpected recovery payload: {payload}"));
        }
        recovery_delivery_order.push(payload.clone());
        *recovery_delivery_counts.entry(payload).or_insert(0_usize) += 1;
        recovery_max_offsets
          .entry(record.virtual_partition_id)
          .and_modify(|offset| *offset = (*offset).max(record.offset))
          .or_insert(record.offset);
      },
    }
  }

  assert_eq!(
    recovery_delivery_counts
      .keys()
      .cloned()
      .collect::<HashSet<_>>(),
    expected
  );
  assert!(
    recovery_delivery_counts.values().all(|count| *count == 1),
    "checkpoint recovery must deliver every unread record exactly once: \
     {recovery_delivery_counts:?}"
  );
  let delivery_position = |payload: &str| {
    recovery_delivery_order
      .iter()
      .position(|observed| observed == payload)
      .ok_or_else(|| anyhow!("missing expected recovery payload: {payload}"))
  };
  assert!(
    delivery_position("recovery-capacity-first")? < delivery_position("recovery-other-partition")?
      && delivery_position("recovery-other-partition")?
        < delivery_position("recovery-capacity-deferred")?,
    "independent recovery must run before the capacity-deferred historical window: \
     {recovery_delivery_order:?}"
  );
  for (partition_id, offset) in &recovery_max_offsets {
    iterator.store_offset(*partition_id, *offset)?;
  }
  let _ = iterator.commit().await?;

  let recovered_leases = lease_store
    .list_group_leases(
      runtime_group.topic.as_str(),
      runtime_group.group_id.as_str(),
    )
    .await?;
  for (partition_id, maximum_offset) in &recovery_max_offsets {
    let lease = recovered_leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == *partition_id)
      .ok_or_else(|| anyhow!("missing recovered lease for partition {partition_id}"))?;
    assert!(
      lease.owner_id == runtime_group.member_id.as_str()
        && lease.committed_cursor.as_ref().is_some_and(|cursor| {
          cursor.seq_end >= *maximum_offset && cursor.source_checkpoint.is_some()
        }),
      "recovery iterator did not durably checkpoint partition {partition_id}: {lease:?}"
    );
  }
  iterator.shutdown().await?;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies a live group restart recovers retained downtime windows before entering
// Fast mode, without replaying its durable source checkpoint.
#[tokio::test]
async fn live_group_restart_recovers_retained_history_before_fast_path() -> Result<()> {
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(
    OffsetDateTime::from_unix_timestamp(1_700_000_000)?,
  ));
  let mut cluster = ClusterHarness::in_memory(1)
    .partition_count(1)
    .broker_flush_max_delay(Duration::from_mins(1))
    .broker_time_provider(consumer_time.clone())
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = Arc::new(
    cluster
      .create_producer(
        producer_config(),
        vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
      )
      .await?,
  );

  let mut runtime_a = consumer_runtime_config("retained-history-a");
  let mut runtime_b = consumer_runtime_config("retained-history-b");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("retained-history read config missing"))?
      .strongly_consistent_metadata_reads = Some(true);
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("retained-history group config missing"))?;
  let group_topic = group.topic.to_string();
  let group_id = group.group_id.to_string();
  let hooks = cluster.lifecycle_hooks();

  let phase1_id = "retained-history-checkpoint";
  let phase1_ack = produce_message_at_manual_time(
    &cluster,
    &producer,
    consumer_time.as_ref(),
    b"retained-history-key".to_vec(),
    phase1_id,
  )
  .await?;

  let mut initial_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "retained-history-a",
      None,
      None,
    )
    .await?;
  let mut consumer_a = ControlledConsumer::new(
    "retained-history-a",
    cluster.create_consumer(&runtime_a).await?,
  );
  consumer_a.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  timeout(
    Duration::from_secs(5),
    initial_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("initial retained-history member did not rebalance"))??;
  initial_rebalance.release()?;

  let phase1_offset = timeout(Duration::from_secs(5), async {
    loop {
      match timeout(Duration::from_millis(250), consumer_a.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow!("initial retained-history next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          assert_eq!(
            String::from_utf8(record.record.payload.to_vec())?,
            phase1_id
          );
          assert_eq!(record.virtual_partition_id, phase1_ack.virtual_partition_id);
          consumer_a.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_a.commit().await?;
          return Ok::<_, anyhow::Error>(record.offset);
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("initial member did not commit its source checkpoint"))??;

  let checkpoint_lease = cluster
    .consumer_lease_store()
    .list_group_leases(group_topic.as_str(), group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == phase1_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing retained-history checkpoint lease"))?;
  assert!(
    checkpoint_lease
      .committed_cursor
      .as_ref()
      .is_some_and(|cursor| {
        cursor.seq_end == phase1_offset && cursor.source_checkpoint.is_some()
      }),
    "initial member did not durably persist its source checkpoint: {checkpoint_lease:?}"
  );

  consumer_a.shutdown().await?;
  let logical_now_ms = consumer_time.now().unix_timestamp_ms();
  assert!(
    cluster
      .consumer_membership_store()
      .list_active_members(
        group_topic.as_str(),
        group_id.as_str(),
        offset_datetime_from_unix_millis(logical_now_ms),
      )
      .await?
      .is_empty(),
    "graceful shutdown must leave the group before retained history is published"
  );

  let mut downtime_ids = HashSet::new();
  for window_index in 1 ..= 3 {
    consumer_time.advance(TimeDuration::seconds(WINDOW_SIZE_SECONDS + 1));
    let id = format!("retained-history-downtime-{window_index}");
    produce_message_at_manual_time(
      &cluster,
      &producer,
      consumer_time.as_ref(),
      b"retained-history-key".to_vec(),
      &id,
    )
    .await?;
    downtime_ids.insert(id);
  }

  let mut restart_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "retained-history-b",
      None,
      None,
    )
    .await?;
  let mut fast_path_active = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRecoveryFastPathActive,
      "retained-history-b",
      Some(0),
      None,
    )
    .await?;
  let mut consumer_b = ControlledConsumer::new(
    "retained-history-b",
    cluster.create_consumer(&runtime_b).await?,
  );
  consumer_b.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  timeout(
    Duration::from_secs(5),
    restart_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("replacement retained-history member did not rebalance"))??;
  restart_rebalance.release()?;

  let mut downtime_delivery_counts = HashMap::new();
  let mut downtime_max_offsets = HashMap::<VirtualPartitionId, u64>::new();
  {
    let fast_wait = fast_path_active.wait_until_reached();
    tokio::pin!(fast_wait);
    timeout(Duration::from_secs(10), async {
      loop {
        tokio::select! {
          result = &mut fast_wait => return result,
          next_result = consumer_b.next() => match next_result? {
            NextResult::Revoked(revoked) => revoked.complete().await,
            NextResult::Record(record) => {
              let id = String::from_utf8(record.record.payload.to_vec())?;
              if id == phase1_id {
                return Err(anyhow!("replacement member replayed its committed source checkpoint"));
              }
              if !downtime_ids.contains(&id) {
                return Err(anyhow!("replacement member delivered unexpected retained record: {id}"));
              }
              *downtime_delivery_counts.entry(id).or_insert(0_usize) += 1;
              downtime_max_offsets
                .entry(record.virtual_partition_id)
                .and_modify(|offset| *offset = (*offset).max(record.offset))
                .or_insert(record.offset);
              consumer_b.store_offset(record.virtual_partition_id, record.offset)?;
              consumer_b.commit().await?;
            },
          },
          () = tokio::task::yield_now() => {
            consumer_time.advance(TimeDuration::seconds(1));
          },
        }
      }
    })
    .await
    .map_err(|_| anyhow!("replacement member did not reach the Fast recovery boundary"))??;
  }
  fast_path_active.release()?;

  timeout(Duration::from_secs(10), async {
    while downtime_delivery_counts.len() < downtime_ids.len() {
      match timeout(Duration::from_millis(250), consumer_b.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow!("replacement retained-history next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          if id == phase1_id {
            return Err(anyhow!(
              "replacement member replayed its committed source checkpoint"
            ));
          }
          if !downtime_ids.contains(&id) {
            return Err(anyhow!(
              "replacement member delivered unexpected retained record: {id}"
            ));
          }
          *downtime_delivery_counts.entry(id).or_insert(0_usize) += 1;
          downtime_max_offsets
            .entry(record.virtual_partition_id)
            .and_modify(|offset| *offset = (*offset).max(record.offset))
            .or_insert(record.offset);
          consumer_b.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_b.commit().await?;
        },
      }
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("replacement member did not drain retained history"))??;

  assert_eq!(
    downtime_delivery_counts
      .keys()
      .cloned()
      .collect::<HashSet<_>>(),
    downtime_ids
  );
  assert!(
    downtime_delivery_counts.values().all(|count| *count == 1),
    "retained-history restart must deliver every downtime record exactly once: \
     {downtime_delivery_counts:?}"
  );
  wait_for_group_offsets_committed(
    &cluster,
    &downtime_max_offsets,
    "replacement member did not durably checkpoint retained history",
  )
  .await?;

  consumer_b.shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies a restart consumes a durable fresh-start marker instead of skipping a
// newly published sequence range below a poisoned cursor.
#[tokio::test]
async fn consumer_restart_fresh_start_marker_replaces_poisoned_cursor() -> Result<()> {
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(
    OffsetDateTime::from_unix_timestamp(1_700_200_000)?,
  ));
  let mut cluster = ClusterHarness::in_memory(1)
    .partition_count(1)
    .broker_flush_max_delay(Duration::from_mins(1))
    .broker_time_provider(consumer_time.clone())
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = Arc::new(
    cluster
      .create_producer(
        producer_config(),
        vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
      )
      .await?,
  );
  let mut runtime_a = consumer_runtime_config("fresh-start-owner");
  let mut runtime_b = consumer_runtime_config("fresh-start-replacement");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("fresh-start consumer read config missing"))?
      .strongly_consistent_metadata_reads = Some(true);
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("fresh-start group config missing"))?;
  let group_topic = group.topic.to_string();
  let group_id = group.group_id.to_string();

  let checkpoint_id = "fresh-start-checkpoint";
  let checkpoint_ack = produce_message_at_manual_time(
    &cluster,
    &producer,
    consumer_time.as_ref(),
    b"fresh-start-key".to_vec(),
    checkpoint_id,
  )
  .await?;
  let mut consumer_a = ControlledConsumer::new(
    "fresh-start-owner",
    cluster.create_consumer(&runtime_a).await?,
  );
  consumer_a.start()?;
  let checkpoint_offset = timeout(Duration::from_secs(5), async {
    loop {
      match timeout(Duration::from_millis(250), consumer_a.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow!("fresh-start owner next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          assert_eq!(
            String::from_utf8(record.record.payload.to_vec())?,
            checkpoint_id
          );
          consumer_a.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_a.commit().await?;
          return Ok::<_, anyhow::Error>(record.offset);
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("fresh-start owner did not commit checkpoint"))??;

  let lease_store = cluster.consumer_lease_store();
  let checkpoint_lease = lease_store
    .list_group_leases(&group_topic, &group_id)
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == checkpoint_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing fresh-start checkpoint lease"))?;
  let source_checkpoint = checkpoint_lease
    .committed_cursor
    .as_ref()
    .and_then(|cursor| cursor.source_checkpoint.clone())
    .ok_or_else(|| anyhow!("fresh-start checkpoint is missing source state"))?;
  assert_eq!(
    checkpoint_lease
      .committed_cursor
      .as_ref()
      .map(|cursor| cursor.seq_end),
    Some(checkpoint_offset)
  );

  let poisoned_cursor = CommittedCursor {
    virtual_partition_id: checkpoint_ack.virtual_partition_id,
    seq_end: checkpoint_offset.saturating_add(10_000),
    source_checkpoint: Some(source_checkpoint),
  };
  let ConsumerGroupCommitOutcome::Committed(poisoned_lease) = lease_store
    .commit_cursor(
      &checkpoint_lease.key,
      "fresh-start-owner",
      checkpoint_lease.generation,
      consumer_time.now(),
      poisoned_cursor.clone(),
    )
    .await?
  else {
    panic!("expected poisoned cursor commit");
  };
  assert_eq!(
    poisoned_lease.committed_cursor,
    Some(poisoned_cursor.clone())
  );

  let ConsumerGroupArmFreshStartOutcome::Armed(armed_lease) = lease_store
    .arm_next_window_fresh_start(
      &checkpoint_lease.key,
      TimeDuration::seconds(WINDOW_SIZE_SECONDS),
      "fresh-start-marker".to_string(),
      consumer_time.now(),
    )
    .await?
  else {
    panic!("expected fresh-start marker to be armed");
  };
  let marker = armed_lease
    .fresh_start_marker
    .ok_or_else(|| anyhow!("fresh-start marker was not persisted"))?;

  consumer_a.shutdown().await?;
  let seconds_to_target = marker
    .target_window_start_unix_seconds
    .saturating_sub(consumer_time.now().unix_timestamp())
    .saturating_add(1);
  consumer_time.advance(TimeDuration::seconds(seconds_to_target));
  let recovery_id = "fresh-start-recovered";
  let recovery_ack = produce_message_at_manual_time(
    &cluster,
    &producer,
    consumer_time.as_ref(),
    b"fresh-start-key".to_vec(),
    recovery_id,
  )
  .await?;

  let hooks = cluster.lifecycle_hooks();
  let mut replacement_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "fresh-start-replacement",
      None,
      None,
    )
    .await?;
  let mut consumer_b = ControlledConsumer::new(
    "fresh-start-replacement",
    cluster.create_consumer(&runtime_b).await?,
  );
  consumer_b.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  timeout(
    Duration::from_secs(5),
    replacement_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("fresh-start replacement did not reach rebalance"))??;
  replacement_rebalance.release()?;

  let mut recovery_counts = HashMap::new();
  let recovery_offset = timeout(Duration::from_secs(5), async {
    loop {
      match timeout(Duration::from_millis(250), consumer_b.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow!("fresh-start replacement next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          if id == checkpoint_id {
            return Err(anyhow!("fresh-start replacement replayed checkpoint"));
          }
          if id != recovery_id {
            return Err(anyhow!(
              "fresh-start replacement delivered unexpected record: {id}"
            ));
          }
          *recovery_counts.entry(id).or_insert(0_usize) += 1;
          assert!(
            record.offset < poisoned_cursor.seq_end,
            "marker recovery must deliver sequence below poisoned cursor"
          );
          consumer_b.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_b.commit().await?;
          return Ok::<_, anyhow::Error>(record.offset);
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("fresh-start replacement did not deliver recovery record"))??;
  assert_eq!(recovery_counts.get(recovery_id), Some(&1));
  assert_eq!(
    recovery_ack.virtual_partition_id,
    checkpoint_ack.virtual_partition_id
  );

  let final_lease = lease_store
    .list_group_leases(&group_topic, &group_id)
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == checkpoint_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing fresh-start replacement lease"))?;
  assert!(final_lease.fresh_start_marker.is_none());
  assert!(final_lease.committed_cursor.as_ref().is_some_and(|cursor| {
    cursor.seq_end == recovery_offset && cursor.source_checkpoint.is_some()
  }));

  consumer_b.shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies a historical window published after its data window holds recovery before
// later windows until the configured metadata visibility delay elapses.
#[tokio::test]
async fn live_group_recovery_waits_for_historical_metadata_visibility_delay() -> Result<()> {
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(
    OffsetDateTime::from_unix_timestamp(1_700_100_000)?,
  ));
  let deferred_window_start = Arc::new(AtomicI64::new(i64::MIN));
  let base_metadata_store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::new());
  let deferred_metadata_store = Arc::new(DeferredWindowPublicationMetadataStore::new(
    base_metadata_store,
    deferred_window_start.clone(),
  ));
  let metadata_store: Arc<dyn MetadataStore> = deferred_metadata_store.clone();
  let mut cluster = ClusterHarness::in_memory(1)
    .metadata_store(metadata_store)
    .partition_count(1)
    .broker_flush_max_delay(Duration::from_mins(1))
    .broker_time_provider(consumer_time.clone())
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = Arc::new(
    cluster
      .create_producer(
        producer_config(),
        vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
      )
      .await?,
  );

  let mut runtime_a = consumer_runtime_config("deferred-history-a");
  let mut runtime_b = consumer_runtime_config("deferred-history-b");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("deferred-history read config missing"))?
      .metadata_visibility_delay = TimeDuration::milliseconds(1_000).into_proto();
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("deferred-history group config missing"))?;
  let group_topic = group.topic.to_string();
  let group_id = group.group_id.to_string();
  let hooks = cluster.lifecycle_hooks();

  let checkpoint_id = "deferred-history-checkpoint";
  let checkpoint_ack = produce_message_at_manual_time(
    &cluster,
    &producer,
    consumer_time.as_ref(),
    b"deferred-history-key".to_vec(),
    checkpoint_id,
  )
  .await?;
  let mut initial_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "deferred-history-a",
      None,
      None,
    )
    .await?;
  let mut consumer_a = ControlledConsumer::new(
    "deferred-history-a",
    cluster.create_consumer(&runtime_a).await?,
  );
  consumer_a.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  timeout(
    Duration::from_secs(5),
    initial_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("initial deferred-history member did not rebalance"))??;
  initial_rebalance.release()?;

  let checkpoint_offset = timeout(Duration::from_secs(5), async {
    loop {
      match timeout(Duration::from_millis(250), consumer_a.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow!("initial deferred-history next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          assert_eq!(
            String::from_utf8(record.record.payload.to_vec())?,
            checkpoint_id
          );
          assert_eq!(
            record.virtual_partition_id,
            checkpoint_ack.virtual_partition_id
          );
          consumer_a.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_a.commit().await?;
          return Ok::<_, anyhow::Error>(record.offset);
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("initial member did not commit the deferred-history checkpoint"))??;
  consumer_a.shutdown().await?;

  let before_id = "deferred-history-before";
  let deferred_id = "deferred-history-middle";
  let after_id = "deferred-history-after";
  consumer_time.advance(TimeDuration::seconds(WINDOW_SIZE_SECONDS + 1));
  produce_message_at_manual_time(
    &cluster,
    &producer,
    consumer_time.as_ref(),
    b"deferred-history-key".to_vec(),
    before_id,
  )
  .await?;
  consumer_time.advance(TimeDuration::seconds(WINDOW_SIZE_SECONDS + 1));
  deferred_window_start.store(
    Window::for_timestamp(
      consumer_time.now(),
      TimeDuration::seconds(WINDOW_SIZE_SECONDS),
    )
    .start
    .unix_timestamp(),
    Ordering::Release,
  );
  produce_message_at_manual_time(
    &cluster,
    &producer,
    consumer_time.as_ref(),
    b"deferred-history-key".to_vec(),
    deferred_id,
  )
  .await?;
  consumer_time.advance(TimeDuration::seconds(WINDOW_SIZE_SECONDS + 1));
  produce_message_at_manual_time(
    &cluster,
    &producer,
    consumer_time.as_ref(),
    b"deferred-history-key".to_vec(),
    after_id,
  )
  .await?;

  let mut restart_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "deferred-history-b",
      None,
      None,
    )
    .await?;
  let mut consumer_b = ControlledConsumer::new(
    "deferred-history-b",
    cluster.create_consumer(&runtime_b).await?,
  );
  // Set the late-publication time before starting B because the driver can begin a reader pass
  // while its rebalance hook is still held.
  let deferred_window_published_ts_ms = consumer_time.now().unix_timestamp().saturating_mul(1_000);
  deferred_metadata_store.set_deferred_window_published_ts_ms(deferred_window_published_ts_ms);
  consumer_b.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  timeout(
    Duration::from_secs(5),
    restart_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("replacement deferred-history member did not rebalance"))??;
  restart_rebalance.release()?;

  let mut delivery_order = Vec::new();
  let mut delivery_counts = HashMap::new();
  let mut staged_before_release = Vec::new();
  timeout(
    Duration::from_secs(10),
    deferred_metadata_store.wait_until_deferred_window_scanned(),
  )
  .await
  .map_err(|_| anyhow!("consumer did not scan the deferred historical window"))?;
  timeout(Duration::from_secs(10), async {
    loop {
      match consumer_b.next().await? {
        NextResult::Revoked(revoked) => revoked.complete().await,
        NextResult::Record(record) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          if id == checkpoint_id || id == deferred_id || id == after_id {
            return Err(anyhow!(
              "deferred recovery delivered {id} before its visibility deadline"
            ));
          }
          if id != before_id {
            return Err(anyhow!(
              "unexpected deferred-history record before deadline: {id}"
            ));
          }
          *delivery_counts.entry(id.clone()).or_insert(0_usize) += 1;
          delivery_order.push(id);
          staged_before_release.push((record.virtual_partition_id, record.offset));
          return Ok::<_, anyhow::Error>(());
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("consumer did not deliver the visible historical window"))??;

  let deferred_snapshot = consumer_b
    .iterator()
    .diagnostics()
    .ok_or_else(|| anyhow!("concrete consumer did not provide diagnostics"))?
    .state_snapshot();
  let deferred_partition = deferred_snapshot
    .local
    .partitions
    .iter()
    .find(|partition| partition.virtual_partition_id == checkpoint_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing deferred-history diagnostics partition"))?;
  assert_eq!(
    deferred_partition
      .reader
      .as_ref()
      .map(|reader| &reader.mode),
    Some(&ConsumerPartitionReadMode::Recovering),
    "deferred historical metadata must keep the partition in recovery: {deferred_snapshot:?}"
  );
  let cursor_while_deferred = cluster
    .consumer_lease_store()
    .list_group_leases(group_topic.as_str(), group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == checkpoint_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing deferred-history lease"))?;
  assert!(
    cursor_while_deferred
      .committed_cursor
      .as_ref()
      .is_some_and(|cursor| {
        cursor.seq_end == checkpoint_offset && cursor.source_checkpoint.is_some()
      }),
    "deferred historical recovery advanced the durable cursor: {cursor_while_deferred:?}"
  );
  assert!(
    !delivery_counts.contains_key(deferred_id) && !delivery_counts.contains_key(after_id),
    "a deferred window must block all later recovery delivery: {delivery_counts:?}"
  );

  let visibility_deadline_ms = deferred_window_published_ts_ms.saturating_add(1_000);
  let milliseconds_until_visibility_deadline = visibility_deadline_ms
    .checked_sub(consumer_time.now().unix_timestamp_ms())
    .ok_or_else(|| anyhow!("deferred-history visibility deadline has already elapsed"))?;
  assert!(
    milliseconds_until_visibility_deadline > 0,
    "deferred-history visibility deadline must remain in the future"
  );
  // Advance to the final millisecond before the absolute deadline, rather than measuring from
  // when the worker began its post-scan sleep.
  consumer_time.advance(TimeDuration::milliseconds(
    milliseconds_until_visibility_deadline - 1,
  ));
  assert!(
    timeout(Duration::from_millis(100), consumer_b.next())
      .await
      .is_err(),
    "consumer delivered a record before the metadata visibility deadline"
  );
  for (partition_id, offset) in staged_before_release {
    consumer_b.store_offset(partition_id, offset)?;
  }
  if delivery_counts.contains_key(before_id) {
    consumer_b.commit().await?;
  }
  consumer_time.advance(TimeDuration::milliseconds(1));

  let expected_ids = HashSet::from([
    before_id.to_string(),
    deferred_id.to_string(),
    after_id.to_string(),
  ]);
  let mut maximum_offsets = HashMap::<VirtualPartitionId, u64>::new();
  timeout(Duration::from_secs(10), async {
    while delivery_counts.len() < expected_ids.len() {
      match timeout(Duration::from_millis(250), consumer_b.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => return Err(anyhow!("replacement deferred-history next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          if !expected_ids.contains(&id) {
            return Err(anyhow!(
              "unexpected deferred-history record after release: {id}"
            ));
          }
          *delivery_counts.entry(id.clone()).or_insert(0_usize) += 1;
          delivery_order.push(id);
          maximum_offsets
            .entry(record.virtual_partition_id)
            .and_modify(|offset| *offset = (*offset).max(record.offset))
            .or_insert(record.offset);
          consumer_b.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_b.commit().await?;
        },
      }
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("deferred recovery did not drain after visibility release"))??;

  assert_eq!(delivery_order, vec![before_id, deferred_id, after_id]);
  assert_eq!(
    delivery_counts,
    HashMap::from([
      (before_id.to_string(), 1_usize),
      (deferred_id.to_string(), 1_usize),
      (after_id.to_string(), 1_usize),
    ])
  );
  if maximum_offsets.is_empty() {
    maximum_offsets.insert(checkpoint_ack.virtual_partition_id, checkpoint_offset);
  }
  wait_for_group_offsets_committed(
    &cluster,
    &maximum_offsets,
    "deferred recovery did not durably checkpoint the released windows",
  )
  .await?;

  consumer_b.shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies no data loss while consumer-group membership changes from 2 -> 3 -> 1.
#[tokio::test]
async fn group_rebalance_continuous_traffic_no_loss() -> Result<()> {
  // Step 1: Start isolated infrastructure and one broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = new_producer(
    producer_config(),
    vec![producer_topic()],
    Arc::clone(&discovery),
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;

  // Step 2: Start two iterators, scale out to three, then scale in to one.

  let mut runtime_0 = consumer_runtime_config("consumer-0");
  let mut runtime_1 = consumer_runtime_config("consumer-1");
  let mut runtime_2 = consumer_runtime_config("consumer-2");
  for (member_id, runtime) in [
    ("consumer-0", &mut runtime_0),
    ("consumer-1", &mut runtime_1),
    ("consumer-2", &mut runtime_2),
  ] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("{member_id} read config missing"))?
      .strongly_consistent_metadata_reads = Some(true);
  }

  let consumer_lease_store = resources.consumer_lease_store();
  let hooks = cluster.lifecycle_hooks();

  let consumer_0 = Box::new(cluster.create_consumer(&runtime_0).await?);
  let consumer_1 = Box::new(cluster.create_consumer(&runtime_1).await?);
  let mut initial_rebalance_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "consumer-0",
      None,
      None,
    )
    .await?;
  let mut initial_revocation_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRevocationEmitted,
      "consumer-0",
      None,
      None,
    )
    .await?;

  let (event_tx, mut event_rx) = mpsc::unbounded_channel();
  let (stop_tx_0, stop_rx_0) = watch::channel(false);
  let (stop_tx_1, stop_rx_1) = watch::channel(false);
  let (stop_tx_2, stop_rx_2) = watch::channel(false);

  let consumer_0_task = tokio::spawn(run_consumer_task(consumer_0, stop_rx_0, event_tx.clone()));
  let consumer_1_task = tokio::spawn(run_consumer_task(consumer_1, stop_rx_1, event_tx.clone()));

  // Establish the advertised two-member state before adding C2. C0 owns the initial bootstrap
  // assignment; C1's construction plans the first split, which C0 must explicitly hand off.
  timeout(
    Duration::from_secs(10),
    initial_rebalance_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("consumer-0 did not begin the initial two-member rebalance"))??;
  initial_rebalance_gate.release()?;
  timeout(
    Duration::from_secs(10),
    initial_revocation_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("consumer-0 did not revoke partitions for consumer-1"))??;
  let mut initial_consumer_1_assignment_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRebalanceApplied,
      "consumer-1",
      None,
      None,
    )
    .await?;
  initial_revocation_gate.release()?;
  let initial_revocation_event = timeout(Duration::from_secs(10), event_rx.recv())
    .await
    .map_err(|_| anyhow!("consumer-0 did not surface its initial revocation"))?
    .ok_or_else(|| anyhow!("consumer tasks stopped before the initial revocation"))?;
  let ConsumerTaskEvent::Revoked {
    ack: initial_revocation_ack,
  } = initial_revocation_event
  else {
    return Err(anyhow!(
      "consumer-0 did not surface a revocation before the initial handoff"
    ));
  };
  initial_revocation_ack
    .send(())
    .map_err(|()| anyhow!("consumer-0 stopped before acknowledging its initial revocation"))?;
  timeout(
    Duration::from_secs(10),
    initial_consumer_1_assignment_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("consumer-1 did not apply the initial partition handoff"))??;
  initial_consumer_1_assignment_gate.release()?;
  timeout(Duration::from_secs(10), async {
    loop {
      let leases = consumer_lease_store
        .list_group_leases(TOPIC, "integration-group")
        .await?;
      let owners = leases
        .iter()
        .map(|lease| lease.owner_id.as_str())
        .collect::<HashSet<_>>();
      if owners.contains("consumer-0") && owners.contains("consumer-1") {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("initial consumers did not both own partitions before scale-out"))??;

  // Step 3: Produce in phases and require the joining member's persisted assignment before
  // publishing phase two.
  let mut expected_ids = HashSet::new();
  let mut delivery_traces = ConsumerDeliveryTraces::new();
  let mut revocation_count = 0usize;

  for message_id in 0 .. 24 {
    let id = format!("rebalance-{message_id}");
    produce_message(
      &producer,
      format!("rebalance-key-{}", message_id % 12).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  // C0 and C1 must receive acknowledgements for their revocations before C2 can apply the
  // scale-out assignment. Hold C2 at that applied boundary while this test drains those events.
  let mut scale_out_consumer_2_assignment_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRebalanceApplied,
      "consumer-2",
      None,
      None,
    )
    .await?;
  let consumer_2 = Box::new(cluster.create_consumer(&runtime_2).await?);
  let consumer_2_task = tokio::spawn(run_consumer_task(consumer_2, stop_rx_2, event_tx.clone()));

  timeout(Duration::from_secs(10), async {
    let scale_out_assignment_wait = scale_out_consumer_2_assignment_gate.wait_until_reached();
    tokio::pin!(scale_out_assignment_wait);
    loop {
      tokio::select! {
        result = &mut scale_out_assignment_wait => return result,
        event = event_rx.recv() => {
          let event = event.ok_or_else(|| {
            anyhow!("all consumer tasks stopped before consumer-2 applied its scale-out assignment")
          })?;
          handle_consumer_event_with_trace(event, &mut delivery_traces, &mut revocation_count);
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("consumer-2 did not apply its scale-out assignment"))??;
  scale_out_consumer_2_assignment_gate.release()?;
  let scale_out_leases = consumer_lease_store
    .list_group_leases(TOPIC, "integration-group")
    .await?;
  assert!(
    scale_out_leases
      .iter()
      .any(|lease| lease.owner_id == "consumer-2"),
    "consumer-2 applied its scale-out assignment without owning a partition: {scale_out_leases:?}"
  );

  for message_id in 24 .. 48 {
    let id = format!("rebalance-{message_id}");
    produce_message(
      &producer,
      format!("rebalance-key-{}", message_id % 12).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  let _ = stop_tx_1.send(true);
  let _ = stop_tx_2.send(true);

  let consumer_1_result = consumer_1_task
    .await
    .map_err(|error| anyhow!("consumer-1 task join error: {error}"))?;
  consumer_1_result?;
  let consumer_2_result = consumer_2_task
    .await
    .map_err(|error| anyhow!("consumer-2 task join error: {error}"))?;
  consumer_2_result?;

  timeout(Duration::from_secs(20), async {
    loop {
      let leases = consumer_lease_store
        .list_group_leases(TOPIC, "integration-group")
        .await?;
      if leases.len() == PARTITION_COUNT as usize
        && leases.iter().all(|lease| lease.owner_id == "consumer-0")
      {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("consumer-0 did not acquire every partition after scale-in"))??;

  for message_id in 48 .. 72 {
    let id = format!("rebalance-{message_id}");
    produce_message(
      &producer,
      format!("rebalance-key-{}", message_id % 12).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  // Step 4: Only the remaining group member can establish end-state no-loss.
  timeout(Duration::from_secs(20), async {
    while delivery_traces.len() < expected_ids.len() {
      let event = event_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("all consumer tasks stopped before draining records"))?;
      handle_consumer_event_with_trace(event, &mut delivery_traces, &mut revocation_count);
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| {
    anyhow!(
      "group consumers did not drain all records after scale-in: expected={}, consumed={}",
      expected_ids.len(),
      delivery_traces.len()
    )
  })??;

  let delivered_id_counts = delivery_counts(&delivery_traces);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count >= 1),
    "group rebalance requires at-least-once delivery: counts={delivered_id_counts:?}, \
     traces={delivery_traces:?}"
  );
  assert!(
    delivery_members(&delivery_traces).iter().all(|member_id| [
      "consumer-0",
      "consumer-1",
      "consumer-2"
    ]
    .contains(&member_id.as_str())),
    "group rebalance delivery came from a non-group member: {delivery_traces:?}"
  );

  let maximum_offsets = maximum_delivery_offsets(&delivery_traces);
  wait_for_group_offsets_committed(
    &cluster,
    &maximum_offsets,
    "group rebalance consumers did not durably commit every observed delivery",
  )
  .await?;
  let leases = consumer_lease_store
    .list_group_leases(TOPIC, "integration-group")
    .await?;
  assert_eq!(leases.len(), PARTITION_COUNT as usize);
  assert!(
    leases.iter().all(|lease| lease.owner_id == "consumer-0"),
    "surviving consumer did not own every partition after scale-in: {leases:?}"
  );
  for (partition_id, maximum_offset) in maximum_offsets {
    let lease = leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == partition_id)
      .ok_or_else(|| anyhow!("missing rebalance lease for partition {partition_id}"))?;
    assert!(
      lease.committed_cursor.as_ref().is_some_and(|cursor| {
        cursor.seq_end >= maximum_offset && cursor.source_checkpoint.is_some()
      }),
      "group rebalance did not durably commit partition {partition_id}: {lease:?}"
    );
  }

  // Ensure rebalance revocations were observed and acknowledged.
  assert!(
    revocation_count >= 1,
    "expected at least one revocation after scale-out, observed={revocation_count}"
  );

  // Step 5: Clean up all resources.
  let _ = stop_tx_0.send(true);
  let consumer_0_result = consumer_0_task
    .await
    .map_err(|error| anyhow!("consumer-0 task join error: {error}"))?;
  consumer_0_result?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
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
      return Err(anyhow!(
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
    let broker_owns_partition = snapshots
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
    if broker_owns_partition {
      return Ok(());
    }
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "broker {node_id} did not acquire producer leases for {topic} partitions \
         {virtual_partition_ids:?}: {snapshots:#?}"
      ));
    }
    tokio::task::yield_now().await;
  }
}

// High-level: verifies progress and no-loss continuity while the active broker is restarted.
#[tokio::test]
async fn active_broker_restart_continuity() -> Result<()> {
  // Step 1: Start two brokers and pin producer traffic to a single active broker.
  // This scenario validates broker/group coordination, so it does not need shared external
  // storage services that can perturb its lifecycle timing.
  let mut cluster = ClusterHarness::in_memory(2).start().await?;

  let live_nodes = cluster.live_nodes();
  let mut active_node = live_nodes
    .first()
    .cloned()
    .ok_or_else(|| anyhow!("expected active broker"))?;
  cluster.set_active_nodes(vec![active_node.clone()]);

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;
  wait_for_producer_route(
    &producer,
    &active_node.node_id,
    &active_node.address,
    Instant::now() + Duration::from_secs(10),
  )
  .await?;

  let mut runtime_0 = consumer_runtime_config("restart-consumer-0");
  let mut runtime_1 = consumer_runtime_config("restart-consumer-1");
  let mut runtime_2 = consumer_runtime_config("restart-consumer-2");
  for runtime in [&mut runtime_0, &mut runtime_1, &mut runtime_2] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("restart consumer read config missing"))?
      .strongly_consistent_metadata_reads = Some(true);
  }
  let (event_tx, mut event_rx) = mpsc::unbounded_channel();
  let (stop_tx_0, stop_rx_0) = watch::channel(false);
  let (stop_tx_1, stop_rx_1) = watch::channel(false);
  let (stop_tx_2, stop_rx_2) = watch::channel(false);
  let consumer_0_task = tokio::spawn(run_consumer_task(
    Box::new(cluster.create_consumer(&runtime_0).await?),
    stop_rx_0,
    event_tx.clone(),
  ));
  let consumer_1_task = tokio::spawn(run_consumer_task(
    Box::new(cluster.create_consumer(&runtime_1).await?),
    stop_rx_1,
    event_tx.clone(),
  ));
  let mut consumer_2_task = None;

  // Step 2: Produce continuously, restart the active broker mid-stream, and continue producing.
  let total_messages = 64;
  let restart_at = 28;
  let restart_partitions = (0 .. 8)
    .map(|key_index| {
      let record_key = format!("restart-active-key-{key_index}").into_bytes();
      virtual_partition_for_logical(
        logical_partition_for_key(&record_key, PARTITION_COUNT),
        PARTITION_COUNT,
        0,
      )
    })
    .collect::<Vec<_>>();
  wait_for_broker_lease_ownership(
    &cluster,
    &active_node.node_id,
    TOPIC,
    &restart_partitions,
    Instant::now() + Duration::from_secs(10),
  )
  .await?;
  let mut expected_ids = HashSet::new();
  let mut delivered_id_counts = HashMap::new();
  let mut last_offsets = HashMap::new();
  let mut revocation_count = 0;
  let mut produced_partitions = HashSet::new();

  for message_id in 0 .. total_messages {
    if message_id == restart_at {
      let in_flight_id = "restart-active-in-flight";
      let in_flight_partition = restart_partitions[0];
      let write_engine = cluster.write_engine_by_id(&active_node.node_id)?;
      let hooks = cluster.lifecycle_hooks();
      let mut before_flush_gate = hooks
        .arm_broker_for_partition(
          LifecycleEvent::BrokerBeforeFlushPersist,
          in_flight_partition,
        )
        .await?;
      let mut metadata_persisted_gate = hooks
        .arm_broker_for_partition(LifecycleEvent::BrokerMetadataPersisted, in_flight_partition)
        .await?;
      // Gate a partition owned by the active broker so unrelated broker lifecycle work cannot
      // satisfy this restart boundary.
      let mut drain_gate = hooks
        .arm_broker_for_partition(LifecycleEvent::BrokerLeaseDrainStarted, in_flight_partition)
        .await?;
      let mut joining_rebalance_gate = hooks
        .arm_consumer(
          LifecycleEvent::ConsumerBeforeRebalance,
          "restart-consumer-2",
          None,
          None,
        )
        .await?;
      let in_flight_produce = write_engine.produce_batch(WriteRequest {
        topic: TOPIC.into(),
        virtual_partition_id: in_flight_partition,
        records: vec![new_record(
          in_flight_id.as_bytes().to_vec(),
          now_unix_seconds() * 1_000,
        )],
      });
      tokio::pin!(in_flight_produce);
      timeout(Duration::from_secs(5), async {
        tokio::select! {
          result = &mut in_flight_produce => Err(anyhow!(
            "accepted in-flight produce completed before the flush persistence boundary: {result:?}"
          )),
          reached = before_flush_gate.wait_until_reached() => reached,
        }
      })
      .await
      .map_err(|_| anyhow!("accepted in-flight produce did not reach the flush boundary"))??;
      let joining_consumer = Box::new(cluster.create_consumer(&runtime_2).await?);
      let restarting_node_id = active_node.node_id.clone();
      let mut restart = Box::pin(cluster.restart_broker_by_id(&restarting_node_id));
      timeout(Duration::from_secs(5), async {
        tokio::select! {
          result = &mut restart => Err(anyhow!(
            "active broker restart completed before its lease-drain lifecycle event: {result:?}"
          )),
          reached = drain_gate.wait_until_reached() => reached,
        }
      })
      .await
      .map_err(|_| anyhow!("active broker did not begin draining before restart"))??;
      consumer_2_task = Some(tokio::spawn(run_consumer_task(
        joining_consumer,
        stop_rx_2.clone(),
        event_tx.clone(),
      )));
      timeout(
        Duration::from_secs(5),
        joining_rebalance_gate.wait_until_reached(),
      )
      .await
      .map_err(|_| anyhow!("joining consumer did not begin rebalancing during broker drain"))??;
      joining_rebalance_gate.release()?;
      before_flush_gate.release()?;
      timeout(
        Duration::from_secs(5),
        metadata_persisted_gate.wait_until_reached(),
      )
      .await
      .map_err(|_| anyhow!("accepted in-flight produce did not persist metadata during drain"))??;
      metadata_persisted_gate.release()?;
      let in_flight_ack = timeout(Duration::from_secs(5), &mut in_flight_produce)
        .await
        .map_err(|_| anyhow!("accepted in-flight produce did not complete after persistence"))??;
      assert!(!in_flight_ack.seq_range.is_empty());
      expected_ids.insert(in_flight_id.to_string());
      produced_partitions.insert(in_flight_partition);
      drain_gate.release()?;
      let restarted_node = timeout(Duration::from_secs(10), &mut restart)
        .await
        .map_err(|_| anyhow!("active broker restart did not complete after drain release"))??;
      drop(restart);
      active_node = restarted_node;
      cluster.set_active_nodes(vec![active_node.clone()]);
      wait_for_producer_route(
        &producer,
        &active_node.node_id,
        &active_node.address,
        Instant::now() + Duration::from_secs(10),
      )
      .await?;
      wait_for_broker_lease_ownership(
        &cluster,
        &active_node.node_id,
        TOPIC,
        &restart_partitions,
        Instant::now() + Duration::from_secs(10),
      )
      .await?;
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
    produced_partitions.insert(ack.virtual_partition_id);

    while let Ok(event) = event_rx.try_recv() {
      handle_consumer_event_with_offsets(
        event,
        &mut delivered_id_counts,
        &mut last_offsets,
        &mut revocation_count,
      );
    }
  }

  // Step 3: The active consumer group, not a standalone reader, establishes no-loss continuity.
  timeout(Duration::from_secs(30), async {
    while delivered_id_counts.len() < expected_ids.len() {
      let event = event_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("restart consumer tasks stopped before traffic drained"))?;
      handle_consumer_event_with_offsets(
        event,
        &mut delivered_id_counts,
        &mut last_offsets,
        &mut revocation_count,
      );
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("consumer group did not drain broker restart traffic"))??;
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "broker restart unexpectedly duplicated group deliveries: {delivered_id_counts:?}"
  );
  assert!(
    revocation_count >= 1,
    "expected membership movement to revoke at least one active consumer during broker restart"
  );

  wait_for_group_offsets_committed(
    &cluster,
    &last_offsets,
    "restart consumers did not durably commit every observed delivery",
  )
  .await?;
  let leases = cluster
    .consumer_lease_store()
    .list_group_leases(TOPIC, "integration-group")
    .await?;
  assert!(leases.iter().all(|lease| {
    lease.owner_id == "restart-consumer-0"
      || lease.owner_id == "restart-consumer-1"
      || lease.owner_id == "restart-consumer-2"
  }));
  for partition_id in produced_partitions {
    let lease = leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == partition_id)
      .ok_or_else(|| anyhow!("missing restart consumer lease for partition {partition_id}"))?;
    let last_offset = last_offsets
      .get(&partition_id)
      .ok_or_else(|| anyhow!("missing delivered cursor for partition {partition_id}"))?;
    assert!(
      lease.committed_cursor.as_ref().is_some_and(|cursor| {
        cursor.seq_end >= *last_offset && cursor.source_checkpoint.is_some()
      }),
      "restart consumer did not durably commit partition {partition_id}"
    );
  }

  // Step 4: Clean up all resources.
  let _ = stop_tx_0.send(true);
  let _ = stop_tx_1.send(true);
  let _ = stop_tx_2.send(true);
  consumer_0_task
    .await
    .map_err(|error| anyhow!("restart consumer-0 task join error: {error}"))??;
  consumer_1_task
    .await
    .map_err(|error| anyhow!("restart consumer-1 task join error: {error}"))??;
  consumer_2_task
    .ok_or_else(|| anyhow!("joining consumer task was not started"))?
    .await
    .map_err(|error| anyhow!("restart consumer-2 task join error: {error}"))??;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies graceful restart does not release a producer lease before accepted work
// has drained.
#[tokio::test]
async fn graceful_broker_restart_waits_for_partition_drain_before_lease_release() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;
  let node = cluster
    .live_nodes()
    .into_iter()
    .next()
    .ok_or_else(|| anyhow!("expected a broker node"))?;
  let mut runtime = consumer_runtime_config("restart-drain-consumer");
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("restart drain consumer read config missing"))?
    .strongly_consistent_metadata_reads = Some(true);
  let group = runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("restart drain consumer group config missing"))?;
  let mut consumer = cluster.create_consumer(&runtime).await?;
  consumer.start()?;

  let accepted_id = "restart-drain-accepted";
  let accepted_key = b"restart-drain-key".to_vec();
  let accepted_partition = virtual_partition_for_logical(
    logical_partition_for_key(&accepted_key, PARTITION_COUNT),
    PARTITION_COUNT,
    0,
  );
  let write_engine = cluster.write_engine_by_id(&node.node_id)?;
  let hooks = cluster.lifecycle_hooks();
  let mut before_flush_gate = hooks
    .arm_broker_for_partition(
      framework::LifecycleEvent::BrokerBeforeFlushPersist,
      accepted_partition,
    )
    .await?;
  let mut metadata_persisted_gate = hooks
    .arm_broker_for_partition(
      framework::LifecycleEvent::BrokerMetadataPersisted,
      accepted_partition,
    )
    .await?;
  let mut drain_started_gate = hooks
    .arm_broker_for_partition(
      framework::LifecycleEvent::BrokerLeaseDrainStarted,
      accepted_partition,
    )
    .await?;
  let mut drained_gate = hooks
    .arm_broker_for_partition(
      framework::LifecycleEvent::BrokerPartitionDrained,
      accepted_partition,
    )
    .await?;
  let mut before_release_gate = hooks
    .arm_broker_for_partition(
      framework::LifecycleEvent::BrokerBeforeLeaseRelease,
      accepted_partition,
    )
    .await?;
  let restarting_node_id = node.node_id.clone();
  let accepted_produce = write_engine.produce_batch(WriteRequest {
    topic: TOPIC.into(),
    virtual_partition_id: accepted_partition,
    records: vec![new_record(
      accepted_id.as_bytes().to_vec(),
      now_unix_seconds() * 1_000,
    )],
  });
  tokio::pin!(accepted_produce);

  timeout(Duration::from_secs(5), async {
    tokio::select! {
      result = &mut accepted_produce => Err(anyhow!(
        "accepted produce completed before the flush persistence boundary: {result:?}"
      )),
      reached = before_flush_gate.wait_until_reached() => reached,
    }
  })
  .await
  .map_err(|_| anyhow!("accepted record did not reach the blocked flush boundary"))??;

  let mut restart = Box::pin(cluster.restart_broker_by_id(&restarting_node_id));
  timeout(Duration::from_secs(5), async {
    tokio::select! {
      result = &mut restart => Err(anyhow!(
        "broker restart completed before starting the lease drain: {result:?}"
      )),
      reached = drain_started_gate.wait_until_reached() => reached,
    }
  })
  .await
  .map_err(|_| anyhow!("broker did not start draining the blocked partition"))??;
  drain_started_gate.release()?;

  let later_result = write_engine
    .produce_batch(WriteRequest {
      topic: TOPIC.into(),
      virtual_partition_id: accepted_partition,
      records: vec![new_record(
        b"restart-drain-later".to_vec(),
        now_unix_seconds() * 1_000,
      )],
    })
    .await;
  assert!(
    matches!(
      later_result,
      Err(blob_stream_broker::write::WriteError::NotLeaseHolder { .. })
    ),
    "draining broker accepted later same-partition work before its blocked work drained: \
     {later_result:?}"
  );

  before_flush_gate.release()?;
  timeout(Duration::from_secs(5), async {
    tokio::select! {
      result = &mut restart => Err(anyhow!(
        "broker restart completed before its accepted work reached metadata persistence: {result:?}"
      )),
      reached = metadata_persisted_gate.wait_until_reached() => reached,
    }
  })
  .await
  .map_err(|_| {
    anyhow!("broker did not persist accepted work metadata after persistence unblocked")
  })??;
  metadata_persisted_gate.release()?;
  timeout(Duration::from_secs(5), &mut accepted_produce)
    .await
    .map_err(|_| anyhow!("accepted produce did not complete after metadata persistence"))??;
  timeout(Duration::from_secs(5), async {
    tokio::select! {
      result = &mut restart => Err(anyhow!(
        "broker restart completed before its blocked accepted work drained: {result:?}"
      )),
      reached = drained_gate.wait_until_reached() => reached,
    }
  })
  .await
  .map_err(|_| anyhow!("broker did not drain its accepted work after persistence unblocked"))??;

  drained_gate.release()?;
  timeout(
    Duration::from_secs(5),
    before_release_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("broker did not reach before-lease-release after drain completion"))??;
  before_release_gate.release()?;
  let restarted_node = timeout(Duration::from_secs(10), &mut restart)
    .await
    .map_err(|_| anyhow!("broker restart did not complete after lease-release gate opened"))??;
  drop(restart);
  assert_eq!(restarted_node.node_id, node.node_id);

  let delivered = timeout(Duration::from_secs(10), async {
    loop {
      match consumer.next().await? {
        NextResult::Revoked(revoked) => revoked.complete().await,
        NextResult::Record(record) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          consumer.store_offset(record.virtual_partition_id, record.offset)?;
          consumer.commit().await?;
          if id == accepted_id {
            return Ok::<_, anyhow::Error>((record.virtual_partition_id, record.offset));
          }
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("live consumer did not deliver the accepted record after restart"))??;
  assert_eq!(delivered.0, accepted_partition);
  let lease = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == delivered.0)
    .ok_or_else(|| anyhow!("missing live consumer lease after drain restart"))?;
  assert!(
    lease
      .committed_cursor
      .as_ref()
      .is_some_and(|cursor| cursor.seq_end >= delivered.1 && cursor.source_checkpoint.is_some())
  );

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies per-partition sequence ends advance strictly as batches are consumed.
#[tokio::test]
async fn per_partition_sequence_monotonicity() -> Result<()> {
  // Step 1: Start isolated infrastructure and a single broker.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = new_producer(
    producer_config(),
    vec![producer_topic()],
    Arc::clone(&discovery),
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    (0 .. PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;

  // Step 2: Produce a stream of records and track which virtual partitions were targeted.
  let total_messages = 96;
  let mut expected_ids = HashSet::new();
  let mut produced_counts_by_partition = HashMap::new();
  for message_id in 0 .. total_messages {
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
  let mut deliveries = Vec::new();
  let mut last_seq_end_by_partition = HashMap::new();
  let mut observed_batches_by_partition = HashMap::new();
  let reader_now = now_unix_seconds().saturating_add(3);
  let deadline = Instant::now() + Duration::from_secs(45);

  while reader_delivery_counts(&deliveries).len() < expected_ids.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while validating sequence monotonicity: expected={}, consumed={}",
        expected_ids.len(),
        reader_delivery_counts(&deliveries).len()
      ));
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
      let observed_batches = observed_batches_by_partition
        .entry(batch.virtual_partition_id)
        .or_insert(0usize);
      *observed_batches += 1;
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
    "sequence monotonicity reader observed duplicate delivery: {deliveries:?}"
  );
  let mut delivered_counts_by_partition = HashMap::new();
  for delivery in &deliveries {
    *delivered_counts_by_partition
      .entry(delivery.virtual_partition_id)
      .or_insert(0usize) += 1;
  }
  assert_eq!(delivered_counts_by_partition, produced_counts_by_partition);

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
#[tokio::test]
async fn multi_topic_isolation() -> Result<()> {
  // Step 1: Start isolated infrastructure and one broker that serves both test topics.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = new_producer(
    producer_config(),
    vec![
      producer_topic_named(TOPIC),
      producer_topic_named(SECOND_TOPIC),
    ],
    Arc::clone(&discovery),
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;

  let mut topic_a_reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    (0 .. PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;

  let mut topic_b_reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: SECOND_TOPIC.to_string().into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    (0 .. PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;

  // Step 2: Produce interleaved traffic to both topics.
  let total_per_topic = 32;
  let mut topic_a_expected = HashSet::new();
  let mut topic_b_expected = HashSet::new();

  for message_id in 0 .. total_per_topic {
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
  let mut topic_a_delivery_counts = HashMap::new();
  let mut topic_b_delivery_counts = HashMap::new();
  let reader_now = now_unix_seconds().saturating_add(3);
  let deadline = Instant::now() + Duration::from_secs(45);

  while topic_a_delivery_counts.len() < topic_a_expected.len()
    || topic_b_delivery_counts.len() < topic_b_expected.len()
  {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while draining multi-topic test: topic_a={}/{}, topic_b={}/{}",
        topic_a_delivery_counts.len(),
        topic_a_expected.len(),
        topic_b_delivery_counts.len(),
        topic_b_expected.len()
      ));
    }

    let topic_a_batches = topic_a_reader.read_available(reader_now).await?;
    for batch in topic_a_batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload.to_vec())
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        assert!(
          id.starts_with("topic-a-"),
          "topic-a reader observed cross-topic payload: {id}"
        );
        *topic_a_delivery_counts.entry(id).or_insert(0_usize) += 1;
      }
    }

    let topic_b_batches = topic_b_reader.read_available(reader_now).await?;
    for batch in topic_b_batches {
      for record in batch.records {
        let id = String::from_utf8(record.payload.to_vec())
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        assert!(
          id.starts_with("topic-b-"),
          "topic-b reader observed cross-topic payload: {id}"
        );
        *topic_b_delivery_counts.entry(id).or_insert(0_usize) += 1;
      }
    }

    if topic_a_delivery_counts.len() < topic_a_expected.len()
      || topic_b_delivery_counts.len() < topic_b_expected.len()
    {
      tokio::task::yield_now().await;
    }
  }

  assert_eq!(
    topic_a_delivery_counts
      .keys()
      .cloned()
      .collect::<HashSet<_>>(),
    topic_a_expected
  );
  assert_eq!(
    topic_b_delivery_counts
      .keys()
      .cloned()
      .collect::<HashSet<_>>(),
    topic_b_expected
  );
  assert!(
    topic_a_delivery_counts.values().all(|count| *count == 1)
      && topic_b_delivery_counts.values().all(|count| *count == 1),
    "topic-isolation readers duplicated delivery: topic_a={topic_a_delivery_counts:?}, \
     topic_b={topic_b_delivery_counts:?}"
  );
  assert!(
    topic_a_delivery_counts
      .keys()
      .all(|id| !topic_b_delivery_counts.contains_key(id))
  );

  // Step 4: Clean up all resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn shared_cross_topic_blob_pulls_forward_a_fresh_topic() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let broker_time = Arc::new(ManualTimeProvider::new(offset_datetime_from_unix_millis(
    1_700_000_000_000,
  )));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .broker_flush_max_delay(Duration::from_secs(1))
    .broker_time_provider(broker_time.clone())
    .start()
    .await?;
  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = Arc::new(
    new_producer(
      producer_config(),
      vec![
        producer_topic_named_with_partition_count(TOPIC, 1, 1),
        producer_topic_named_with_partition_count(SECOND_TOPIC, 1, 1),
      ],
      Arc::clone(&discovery),
      metrics_scope("blob_stream_producer_it"),
    )
    .await?,
  );

  let first_producer = Arc::clone(&producer);
  let first = tokio::spawn(async move {
    first_producer
      .produce_one(ProducerRecord::new(
        TOPIC.into(),
        b"shared-first-key".to_vec(),
        b"shared-first".to_vec().into(),
        1_700_000_000_000,
      ))
      .await
  });
  timeout(Duration::from_secs(5), async {
    loop {
      let first_buffered = cluster
        .broker_state_snapshots()
        .await
        .into_iter()
        .flat_map(|snapshot| snapshot.topics)
        .any(|topic| {
          topic.name.as_str() == TOPIC
            && topic
              .local_partitions
              .iter()
              .any(|partition| partition.buffered_batch_count > 0)
        });
      if first_buffered {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("first topic write did not enter the broker buffer"))??;
  broker_time.wait_until_sleeping(1).await;
  broker_time.advance(TimeDuration::milliseconds(900));

  let second_producer = Arc::clone(&producer);
  let second = tokio::spawn(async move {
    second_producer
      .produce_one(ProducerRecord::new(
        SECOND_TOPIC.into(),
        b"shared-second-key".to_vec(),
        b"shared-second".to_vec().into(),
        1_700_000_000_000,
      ))
      .await
  });

  timeout(Duration::from_secs(5), async {
    loop {
      let buffered_topics = cluster
        .broker_state_snapshots()
        .await
        .into_iter()
        .flat_map(|snapshot| snapshot.topics)
        .filter(|topic| {
          topic
            .local_partitions
            .iter()
            .any(|partition| partition.buffered_batch_count > 0)
        })
        .map(|topic| topic.name.to_string())
        .collect::<HashSet<_>>();
      if buffered_topics.contains(TOPIC) && buffered_topics.contains(SECOND_TOPIC) {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("both topic writes did not enter the broker buffer"))??;
  broker_time.advance(TimeDuration::milliseconds(100));
  first.await??;
  second.await??;

  let window = Window::for_timestamp(
    broker_time.now(),
    TimeDuration::seconds(WINDOW_SIZE_SECONDS),
  );
  let first_segments = resources
    .metadata_store()
    .scan_window_from_snowflake(&window.key(TOPIC), None, MetadataReadConsistency::Strong)
    .await?;
  let second_segments = resources
    .metadata_store()
    .scan_window_from_snowflake(
      &window.key(SECOND_TOPIC),
      None,
      MetadataReadConsistency::Strong,
    )
    .await?;
  assert_eq!(first_segments.len(), 1);
  assert_eq!(second_segments.len(), 1);
  assert_eq!(first_segments[0].blob_key, second_segments[0].blob_key);
  assert!(first_segments[0].blob_key.as_str().starts_with("shared/"));

  let mut first_reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![0],
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    Arc::new(GrpcBrokerMetadataQuery::new(Arc::new(cluster.producer_discovery())).await?),
    Arc::new(GrpcBrokerBlobRangeQuery::new(Arc::new(cluster.producer_discovery())).await?),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;
  let mut second_reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: SECOND_TOPIC.into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![0],
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    Arc::new(GrpcBrokerMetadataQuery::new(Arc::new(cluster.producer_discovery())).await?),
    Arc::new(GrpcBrokerBlobRangeQuery::new(Arc::new(cluster.producer_discovery())).await?),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;
  let reader_now = broker_time.now().unix_timestamp().saturating_add(3);
  let first_payloads = first_reader
    .read_available(reader_now)
    .await?
    .into_iter()
    .flat_map(|batch| batch.records)
    .map(|record| record.payload.to_vec())
    .collect::<Vec<_>>();
  let second_payloads = second_reader
    .read_available(reader_now)
    .await?
    .into_iter()
    .flat_map(|batch| batch.records)
    .map(|record| record.payload.to_vec())
    .collect::<Vec<_>>();
  assert_eq!(first_payloads, [b"shared-first"]);
  assert_eq!(second_payloads, [b"shared-second"]);

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies shared-object metadata rows publish independently when one topic retries.
#[tokio::test]
async fn shared_object_metadata_failure_is_isolated_and_retries() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let broker_time = Arc::new(framework::ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .broker_flush_max_delay(Duration::from_secs(1))
    .broker_time_provider(broker_time.clone())
    .start()
    .await?;
  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Metadata,
      operation: StoreFaultOperation::MetadataWriteSegment,
      key_pattern: Some(SECOND_TOPIC.to_string()),
      action: StoreFaultAction::Fail {
        message: "secondary topic metadata write failure".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = Arc::new(
    new_producer(
      producer_config(),
      vec![
        producer_topic_named_with_partition_count(TOPIC, 1, 1),
        producer_topic_named_with_partition_count(SECOND_TOPIC, 1, 1),
      ],
      discovery,
      metrics_scope("blob_stream_producer_it"),
    )
    .await?,
  );
  let first_producer = Arc::clone(&producer);
  let first = tokio::spawn(async move {
    first_producer
      .produce_one(ProducerRecord::new(
        TOPIC.into(),
        b"partial-first-key".to_vec(),
        b"partial-first".to_vec().into(),
        1_700_000_000_000,
      ))
      .await
  });
  let second_producer = Arc::clone(&producer);
  let second = tokio::spawn(async move {
    second_producer
      .produce_one(ProducerRecord::new(
        SECOND_TOPIC.into(),
        b"partial-second-key".to_vec(),
        b"partial-second".to_vec().into(),
        1_700_000_000_000,
      ))
      .await
  });

  timeout(Duration::from_secs(5), async {
    loop {
      let buffered_topics = cluster
        .broker_state_snapshots()
        .await
        .into_iter()
        .flat_map(|snapshot| snapshot.topics)
        .filter(|topic| {
          topic
            .local_partitions
            .iter()
            .any(|partition| partition.buffered_batch_count > 0)
        })
        .map(|topic| topic.name.to_string())
        .collect::<HashSet<_>>();
      if buffered_topics.contains(TOPIC) && buffered_topics.contains(SECOND_TOPIC) {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("both topic writes did not enter the broker buffer"))??;
  broker_time.advance(TimeDuration::seconds(1));

  cluster
    .wait_for_event(
      &TestEventMatcher {
        category: Some("store".to_string()),
        operation: Some("metadata_write_segment".to_string()),
        key_contains: Some(SECOND_TOPIC.to_string()),
        status: Some("fault_applied".to_string()),
      },
      Duration::from_secs(5),
    )
    .await?;
  first.await??;

  let window = Window::for_timestamp(
    broker_time.now(),
    TimeDuration::seconds(WINDOW_SIZE_SECONDS),
  );
  let first_segments = resources
    .metadata_store()
    .scan_window_from_snowflake(&window.key(TOPIC), None, MetadataReadConsistency::Strong)
    .await?;
  let second_segments = resources
    .metadata_store()
    .scan_window_from_snowflake(
      &window.key(SECOND_TOPIC),
      None,
      MetadataReadConsistency::Strong,
    )
    .await?;
  assert_eq!(first_segments.len(), 1);
  assert!(first_segments[0].blob_key.as_str().starts_with("shared/"));
  assert!(
    second_segments.is_empty(),
    "the failed topic must not publish metadata before its retry"
  );

  timeout(Duration::from_secs(5), async {
    loop {
      let secondary_buffered = cluster
        .broker_state_snapshots()
        .await
        .into_iter()
        .flat_map(|snapshot| snapshot.topics)
        .any(|topic| {
          topic.name.as_str() == SECOND_TOPIC
            && topic
              .local_partitions
              .iter()
              .any(|partition| partition.buffered_batch_count > 0)
        });
      if secondary_buffered {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("secondary topic retry did not re-enter the broker buffer"))??;
  broker_time.advance(TimeDuration::seconds(1));
  let second_ack = second.await??;
  assert!(
    second_ack.attempts > 1,
    "secondary metadata failure must cause a producer retry"
  );

  let first_segments = resources
    .metadata_store()
    .scan_window_from_snowflake(&window.key(TOPIC), None, MetadataReadConsistency::Strong)
    .await?;
  let second_segments = resources
    .metadata_store()
    .scan_window_from_snowflake(
      &window.key(SECOND_TOPIC),
      None,
      MetadataReadConsistency::Strong,
    )
    .await?;
  assert_eq!(first_segments.len(), 1);
  assert_eq!(second_segments.len(), 1);
  assert_ne!(first_segments[0].blob_key, second_segments[0].blob_key);

  let mut second_reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: SECOND_TOPIC.into(),
      strongly_consistent_metadata_reads: Some(true),
      ..Default::default()
    },
    vec![0],
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;
  let reader_now = broker_time.now().unix_timestamp().saturating_add(3);
  let second_payloads = second_reader
    .read_available(reader_now)
    .await?
    .into_iter()
    .flat_map(|batch| batch.records)
    .map(|record| record.payload.to_vec())
    .collect::<Vec<_>>();
  assert_eq!(second_payloads, [b"partial-second"]);

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies boundary payload acceptance and multi-record batch delivery semantics.
#[tokio::test]
async fn payload_boundary_and_batching_behavior() -> Result<()> {
  // Step 1: Start isolated infrastructure and configure producer for observable batching.
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1).start().await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let mut config = producer_config();
  config.max_batch_records = Some(8);
  config.max_batch_bytes = Some(4_096);
  config.flush_max_delay = TimeDuration::milliseconds(50).into_proto();

  let boundary_producer = Arc::new(
    new_producer(
      config,
      vec![producer_topic()],
      Arc::clone(&discovery),
      metrics_scope("blob_stream_producer_it"),
    )
    .await?,
  );
  let mut batching_config = producer_config();
  batching_config.max_batch_records = Some(8);
  batching_config.max_batch_bytes = Some(4_096);
  batching_config.flush_max_delay = TimeDuration::milliseconds(1_000).into_proto();
  let batching_producer = Arc::new(
    new_producer(
      batching_config,
      vec![producer_topic()],
      Arc::clone(&discovery),
      metrics_scope("blob_stream_producer_it"),
    )
    .await?,
  );

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      ..Default::default()
    },
    (0 .. PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
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
    let ack = boundary_producer
      .produce_one(ProducerRecord::new(
        TOPIC.into(),
        format!("boundary-key-{index}").into_bytes(),
        payload.clone().into(),
        event_ts_ms,
      ))
      .await?;
    assert!(ack.attempts >= 1);
    *expected_payload_counts.entry(payload).or_insert(0) += 1;
  }

  // Step 3: Produce concurrent small records on one key to force in-producer batching.
  let mut produce_tasks = Vec::new();
  let batching_barrier = Arc::new(Barrier::new(25));
  for message_id in 0 .. 24 {
    let producer = Arc::clone(&batching_producer);
    let barrier = Arc::clone(&batching_barrier);
    let payload = format!("batching-{message_id}").into_bytes();
    *expected_payload_counts.entry(payload.clone()).or_insert(0) += 1;

    produce_tasks.push(tokio::spawn(async move {
      barrier.wait().await;
      let event_ts_ms = i64::try_from(
        std::time::SystemTime::now()
          .duration_since(std::time::UNIX_EPOCH)
          .expect("clock is before unix epoch")
          .as_millis(),
      )
      .expect("unix millis exceeds i64");
      producer
        .produce_one(ProducerRecord::new(
          TOPIC.into(),
          b"batched-key".to_vec(),
          payload.into(),
          event_ts_ms,
        ))
        .await
    }));
  }
  batching_barrier.wait().await;

  for task in produce_tasks {
    let ack = task.await??;
    assert!(ack.attempts >= 1);
  }

  // Step 4: Drain and assert exact payload recovery plus at least one multi-record batch.
  let expected_total = expected_payload_counts.values().sum::<usize>();
  let mut consumed_payload_counts: HashMap<Vec<u8>, usize> = HashMap::new();
  let mut consumed_total = 0usize;
  let mut saw_multi_record_batch = false;
  let reader_now = now_unix_seconds().saturating_add(3);
  let deadline = Instant::now() + Duration::from_secs(45);

  while consumed_total < expected_total {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while draining payload/batching test: expected={expected_total}, \
         consumed={consumed_total}"
      ));
    }

    let batches = reader.read_available(reader_now).await?;
    if batches.is_empty() {
      tokio::task::yield_now().await;
      continue;
    }

    for batch in batches {
      if batch.records.len() > 1 {
        saw_multi_record_batch = true;
      }

      for record in batch.records {
        *consumed_payload_counts
          .entry(record.payload.to_vec())
          .or_insert(0) += 1;
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
#[tokio::test]
async fn delayed_metadata_cross_window_no_loss() -> Result<()> {
  // Step 1: Start isolated infrastructure with a metadata store that delays visibility.
  let resources = IntegrationResources::create().await?;
  let delayed_metadata_store = Arc::new(DelayedVisibilityMetadataStore::new(
    resources.metadata_store(),
    Duration::ZERO,
  ));
  // Hold visibility before producers start so the initial reader scans cannot race metadata writes.
  delayed_metadata_store.hold_visibility();
  let metadata_store: Arc<dyn MetadataStore> = delayed_metadata_store.clone();
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .metadata_store(Arc::clone(&metadata_store))
    .start()
    .await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = new_producer(
    producer_config(),
    vec![producer_topic()],
    Arc::clone(&discovery),
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;

  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      ..Default::default()
    },
    (0 .. PARTITION_COUNT).collect(),
    HashMap::new(),
    resources.blob_store(),
    metadata_store,
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;

  // Step 2: Produce traffic while metadata remains temporarily invisible to readers.
  let mut expected_ids = HashSet::new();
  for message_id in 0 .. 24 {
    let id = format!("delayed-meta-{message_id}");
    produce_message(
      &producer,
      format!("delayed-meta-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  // Visibility remains held until the test releases the explicit metadata boundary.
  let early_batches = reader.read_available(now_unix_seconds()).await?;
  assert!(
    early_batches.is_empty(),
    "expected empty scan while delayed metadata visibility is held"
  );

  // Step 3: Make the delayed metadata visible so re-scans can recover it without loss.
  delayed_metadata_store.release_visibility();
  let deadline = Instant::now() + Duration::from_secs(20);
  let deliveries = drain_reader_until_with_trace(
    &mut reader,
    expected_ids.len(),
    now_unix_seconds().saturating_add(3),
    deadline,
  )
  .await?;
  let delivered_id_counts = reader_delivery_counts(&deliveries);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count == 1),
    "delayed metadata reader observed duplicate delivery: {deliveries:?}"
  );
  let duplicate_scan = reader.read_available(now_unix_seconds()).await?;
  assert!(duplicate_scan.is_empty());

  // Step 4: Clean up all resources.
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates prefetch-buffered iteration still converges with delayed metadata and
// rebalance, preserving no-loss semantics.
#[tokio::test]
async fn prefetch_rebalance_delayed_metadata_no_loss() -> Result<()> {
  const PREFETCH_DELAYED_PHASE1_MESSAGES: usize = 8;
  const PREFETCH_DELAYED_PHASE2_MESSAGES: usize = 8;

  // Step 1: Start isolated infrastructure with delayed metadata visibility.
  let resources = IntegrationResources::create().await?;
  let delayed_metadata_store = Arc::new(DelayedVisibilityMetadataStore::new(
    resources.metadata_store(),
    Duration::ZERO,
  ));
  delayed_metadata_store.hold_visibility();
  let metadata_store: Arc<dyn MetadataStore> = delayed_metadata_store.clone();
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .metadata_store(metadata_store)
    .start()
    .await?;

  let discovery: Arc<dyn BrokerDiscovery> = Arc::new(cluster.producer_discovery());
  let producer = new_producer(
    producer_config(),
    vec![producer_topic()],
    Arc::clone(&discovery),
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;

  // Step 2: Build two consumers with explicit small prefetch RAM budgets.
  let mut runtime_0 = consumer_runtime_config("prefetch-consumer-0");
  let mut runtime_1 = consumer_runtime_config("prefetch-consumer-1");
  runtime_0
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("prefetch consumer-0 read config missing"))?
    .prefetch_max_bytes = Some(1_024);
  runtime_1
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("prefetch consumer-1 read config missing"))?
    .prefetch_max_bytes = Some(1_024);

  let consumer_lease_store = resources.consumer_lease_store();
  let consumer_0 = Box::new(cluster.create_consumer(&runtime_0).await?);

  let (event_tx, mut event_rx) = mpsc::unbounded_channel();
  let (stop_tx_0, stop_rx_0) = watch::channel(false);
  let (stop_tx_1, stop_rx_1) = watch::channel(false);

  let hooks = cluster.lifecycle_hooks();
  let mut prefetch_buffered = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerPrefetchBatchBuffered,
      "prefetch-consumer-0",
      None,
      None,
    )
    .await?;
  let consumer_0_task = tokio::spawn(run_consumer_task(consumer_0, stop_rx_0, event_tx.clone()));

  // C0 receives its bootstrap assignment during construction, before runtime lifecycle hooks run.
  // Prove that durable starting state before making C1's join the scale-out boundary.
  timeout(Duration::from_secs(6), async {
    loop {
      let leases = consumer_lease_store
        .list_group_leases(TOPIC, "integration-group")
        .await?;
      if leases.len() == PARTITION_COUNT as usize
        && leases
          .iter()
          .all(|lease| lease.owner_id == "prefetch-consumer-0")
      {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("prefetch consumer-0 did not own the bootstrap assignment"))??;

  // Step 3: Produce phase 1 and wait for initial consumption progress.
  let mut expected_ids = HashSet::new();
  let mut delivery_traces = ConsumerDeliveryTraces::new();
  let mut revocation_count = 0usize;

  for message_id in 0 .. PREFETCH_DELAYED_PHASE1_MESSAGES {
    let id = format!("prefetch-delayed-{message_id}");
    produce_message(
      &producer,
      format!("prefetch-delayed-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  delayed_metadata_store.release_visibility();
  timeout(
    Duration::from_secs(6),
    prefetch_buffered.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("consumer did not buffer a prefetched phase 1 batch"))??;
  prefetch_buffered.release()?;

  // Step 4: Scale out from C0's verified bootstrap assignment and continue producing under delay.
  let mut scale_out_rebalance_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "prefetch-consumer-0",
      None,
      None,
    )
    .await?;
  let consumer_1 = Box::new(cluster.create_consumer(&runtime_1).await?);
  let consumer_1_task = tokio::spawn(run_consumer_task(consumer_1, stop_rx_1, event_tx.clone()));

  timeout(
    Duration::from_secs(6),
    scale_out_rebalance_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("prefetch group did not reach the scale-out rebalance boundary"))??;
  scale_out_rebalance_gate.release()?;

  for message_id in PREFETCH_DELAYED_PHASE1_MESSAGES
    .. (PREFETCH_DELAYED_PHASE1_MESSAGES + PREFETCH_DELAYED_PHASE2_MESSAGES)
  {
    let id = format!("prefetch-delayed-{message_id}");
    produce_message(
      &producer,
      format!("prefetch-delayed-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  // Step 5: Only the live group consumers can establish end-state no-loss.
  timeout(Duration::from_secs(12), async {
    while delivery_traces.len() < expected_ids.len() {
      let event = event_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("all consumer tasks stopped before draining records"))?;
      handle_consumer_event_with_trace(event, &mut delivery_traces, &mut revocation_count);
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| {
    anyhow!(
      "group consumers did not drain delayed prefetched records: expected={}, consumed={}",
      expected_ids.len(),
      delivery_traces.len()
    )
  })??;

  let delivered_id_counts = delivery_counts(&delivery_traces);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count >= 1),
    "prefetch rebalance requires at-least-once delivery: counts={delivered_id_counts:?}, \
     traces={delivery_traces:?}"
  );
  assert!(
    delivery_members(&delivery_traces).iter().all(|member_id| [
      "prefetch-consumer-0",
      "prefetch-consumer-1"
    ]
    .contains(&member_id.as_str())),
    "prefetch delivery came from a non-group member: {delivery_traces:?}"
  );
  assert!(
    revocation_count >= 1,
    "expected at least one revocation during prefetch rebalance test"
  );
  let maximum_offsets = maximum_delivery_offsets(&delivery_traces);
  wait_for_group_offsets_committed(
    &cluster,
    &maximum_offsets,
    "prefetch rebalance consumers did not durably commit every observed delivery",
  )
  .await?;
  let leases = consumer_lease_store
    .list_group_leases(TOPIC, "integration-group")
    .await?;
  assert_eq!(leases.len(), PARTITION_COUNT as usize);
  assert!(
    leases.iter().all(|lease| {
      lease.owner_id == "prefetch-consumer-0" || lease.owner_id == "prefetch-consumer-1"
    }),
    "prefetch group leases have unexpected owners after rebalance: {leases:?}"
  );
  for (partition_id, maximum_offset) in maximum_offsets {
    let lease = leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == partition_id)
      .ok_or_else(|| anyhow!("missing prefetch lease for partition {partition_id}"))?;
    assert!(
      lease.committed_cursor.as_ref().is_some_and(|cursor| {
        cursor.seq_end >= maximum_offset && cursor.source_checkpoint.is_some()
      }),
      "prefetch rebalance did not durably commit partition {partition_id}: {lease:?}"
    );
  }

  // Step 6: Clean up all resources.
  let _ = stop_tx_0.send(true);
  let _ = stop_tx_1.send(true);
  let consumer_0_result = consumer_0_task
    .await
    .map_err(|error| anyhow!("prefetch consumer-0 task join error: {error}"))?;
  consumer_0_result?;
  let consumer_1_result = consumer_1_task
    .await
    .map_err(|error| anyhow!("prefetch consumer-1 task join error: {error}"))?;
  consumer_1_result?;

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies graceful shutdown drops prefetched work that was never application
// delivered, allowing a restarted group member to deliver and durably commit it once.
#[tokio::test]
async fn graceful_shutdown_recovers_prefetched_undelivered_record() -> Result<()> {
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(OffsetDateTime::now_utc()));
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
  let mut runtime_a = consumer_runtime_config("prefetch-shutdown-a");
  let mut runtime_b = consumer_runtime_config("prefetch-shutdown-b");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    let read = runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("prefetch-shutdown read config missing"))?;
    read.strongly_consistent_metadata_reads = Some(true);
    read.prefetch_max_bytes = Some(1_024);
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("prefetch-shutdown group config missing"))?;
  let group_topic = group.topic.to_string();
  let group_id = group.group_id.to_string();
  let hooks = cluster.lifecycle_hooks();

  let mut prefetch_gate = hooks.arm_prefetch_for_partition(0).await?;
  let mut initial_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "prefetch-shutdown-a",
      None,
      None,
    )
    .await?;
  let mut consumer_a = cluster.create_consumer(&runtime_a).await?;
  consumer_a.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  timeout(
    Duration::from_secs(5),
    initial_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("initial prefetch-shutdown member did not rebalance"))??;
  initial_rebalance.release()?;

  let target_id = "prefetch-shutdown-undelivered";
  let target_ack = produce_message(&producer, b"prefetch-shutdown-key".to_vec(), target_id).await?;
  assert_eq!(target_ack.virtual_partition_id, 0);
  {
    let prefetch_wait = prefetch_gate.wait_until_reached();
    tokio::pin!(prefetch_wait);
    timeout(Duration::from_secs(5), async {
      loop {
        tokio::select! {
          result = &mut prefetch_wait => return result,
          () = tokio::task::yield_now() => {
            consumer_time.advance(TimeDuration::milliseconds(200));
          },
        }
      }
    })
    .await
    .map_err(|_| anyhow!("consumer did not buffer the undispatched record"))??;
  }
  let prefetch_snapshot = consumer_a
    .diagnostics()
    .ok_or_else(|| anyhow!("concrete consumer did not provide diagnostics"))?
    .state_snapshot();
  assert!(
    prefetch_snapshot.prefetch_buffered_record_count >= 1,
    "prefetch gate must pause only after buffering the record: {prefetch_snapshot:?}"
  );
  prefetch_gate.release()?;

  let mut before_release = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeReleaseOwned,
      "prefetch-shutdown-a",
      None,
      None,
    )
    .await?;
  let shutdown_task = tokio::spawn(async move { Box::new(consumer_a).shutdown().await });
  timeout(Duration::from_secs(5), before_release.wait_until_reached())
    .await
    .map_err(|_| anyhow!("shutdown did not reach the lease-release boundary"))??;
  let lease_before_release = cluster
    .consumer_lease_store()
    .list_group_leases(group_topic.as_str(), group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == target_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing prefetch-shutdown lease"))?;
  assert_eq!(lease_before_release.owner_id, "prefetch-shutdown-a");
  assert!(
    lease_before_release.committed_cursor.is_none(),
    "application-undelivered prefetched work must not create a durable cursor: \
     {lease_before_release:?}"
  );
  before_release.release()?;
  shutdown_task
    .await
    .map_err(|error| anyhow!("prefetch-shutdown task join error: {error}"))??;
  assert!(
    cluster
      .consumer_membership_store()
      .list_active_members(group_topic.as_str(), group_id.as_str(), consumer_time.now(),)
      .await?
      .is_empty(),
    "graceful shutdown must deregister the original member"
  );

  let mut restart_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "prefetch-shutdown-b",
      None,
      None,
    )
    .await?;
  let mut consumer_b = cluster.create_consumer(&runtime_b).await?;
  consumer_b.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  timeout(
    Duration::from_secs(5),
    restart_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("replacement prefetch-shutdown member did not rebalance"))??;
  restart_rebalance.release()?;

  let mut delivery_counts = HashMap::new();
  let recovered_offset = timeout(Duration::from_secs(5), async {
    loop {
      match timeout(Duration::from_millis(250), consumer_b.next()).await {
        Err(_) => {
          consumer_time.advance(TimeDuration::seconds(1));
          tokio::task::yield_now().await;
        },
        Ok(Err(error)) => {
          return Err(anyhow!(
            "replacement prefetch-shutdown next failed: {error}"
          ));
        },
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          if id != target_id {
            return Err(anyhow!(
              "replacement member delivered unexpected record: {id}"
            ));
          }
          *delivery_counts.entry(id).or_insert(0_usize) += 1;
          consumer_b.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_b.commit().await?;
          return Ok::<_, anyhow::Error>(record.offset);
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("replacement member did not deliver prefetched work"))??;
  assert_eq!(
    delivery_counts,
    HashMap::from([(target_id.to_string(), 1_usize)]),
    "prefetched work must be application-delivered exactly once after restart"
  );

  let recovered_lease = cluster
    .consumer_lease_store()
    .list_group_leases(group_topic.as_str(), group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == target_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing recovered prefetch-shutdown lease"))?;
  assert!(
    recovered_lease
      .committed_cursor
      .as_ref()
      .is_some_and(|cursor| {
        cursor.seq_end == recovered_offset && cursor.source_checkpoint.is_some()
      }),
    "replacement member did not durably checkpoint recovered prefetched work: {recovered_lease:?}"
  );

  Box::new(consumer_b).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies that a record buffered by the former owner cannot escape after its
// partition is revoked, and the replacement owner becomes the only post-revocation deliverer.
#[tokio::test]
async fn prefetch_rebalance_revocation_fences_buffered_record() -> Result<()> {
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::in_memory(1)
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = Arc::new(
    cluster
      .create_producer(producer_config(), vec![producer_topic()])
      .await?,
  );

  let target_partition = 0;
  let target_key = (0 .. 1_024)
    .find_map(|key_index| {
      let key = format!("prefetch-fence-key-{key_index}").into_bytes();
      let partition = virtual_partition_for_logical(
        logical_partition_for_key(&key, PARTITION_COUNT),
        PARTITION_COUNT,
        0,
      );
      (partition == target_partition).then_some(key)
    })
    .ok_or_else(|| anyhow!("could not construct key for prefetch fence partition"))?;

  let mut runtime_a = consumer_runtime_config("prefetch-fence-a");
  let mut runtime_b = consumer_runtime_config("prefetch-fence-b");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    let read = runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("prefetch fence read config missing"))?;
    read.strongly_consistent_metadata_reads = Some(true);
    read.prefetch_max_bytes = Some(1_024);
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("prefetch fence group config missing"))?;

  let hooks = cluster.lifecycle_hooks();
  let mut prefetch_gate = hooks.arm_prefetch_for_partition(target_partition).await?;
  let mut initial_rebalance_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "prefetch-fence-a",
      None,
      None,
    )
    .await?;
  let mut owner_a = cluster.create_consumer(&runtime_a).await?;
  owner_a.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  timeout(
    Duration::from_secs(5),
    initial_rebalance_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("former owner did not reach its initial rebalance boundary"))??;
  initial_rebalance_gate.release()?;
  let initial_lease = timeout(Duration::from_secs(5), async {
    loop {
      let lease = cluster
        .consumer_lease_store()
        .list_group_leases(group.topic.as_str(), group.group_id.as_str())
        .await?
        .into_iter()
        .find(|lease| lease.key.virtual_partition_id == target_partition);
      if let Some(lease) = lease.filter(|lease| lease.owner_id == "prefetch-fence-a") {
        return Ok::<_, anyhow::Error>(lease);
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("former owner did not acquire the target-partition lease"))??;

  let target_id = "prefetch-fence-record";
  let target_ack = produce_message(&producer, target_key, target_id).await?;
  assert_eq!(target_ack.virtual_partition_id, target_partition);

  {
    let prefetch_wait = prefetch_gate.wait_until_reached();
    tokio::pin!(prefetch_wait);
    timeout(Duration::from_secs(5), async {
      loop {
        tokio::select! {
          result = &mut prefetch_wait => return result,
          () = tokio::task::yield_now() => {
            consumer_time.advance(TimeDuration::milliseconds(200));
          },
        }
      }
    })
    .await
    .map_err(|_| anyhow!("former owner did not buffer the target partition"))??;
  }

  let mut revocation_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRevocationEmitted,
      "prefetch-fence-a",
      Some(target_partition),
      None,
    )
    .await?;
  let mut scale_out_rebalance_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "prefetch-fence-a",
      None,
      None,
    )
    .await?;
  let mut owner_b = cluster.create_consumer(&runtime_b).await?;
  owner_b.start()?;
  timeout(Duration::from_secs(5), async {
    loop {
      let active_members = cluster
        .consumer_membership_store()
        .list_active_members(
          group.topic.as_str(),
          group.group_id.as_str(),
          consumer_time.now(),
        )
        .await?;
      if active_members
        .iter()
        .any(|member| member.member_id == "prefetch-fence-a")
        && active_members
          .iter()
          .any(|member| member.member_id == "prefetch-fence-b")
      {
        return Ok::<_, anyhow::Error>(());
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("both prefetch-fence members did not become active"))??;
  framework::advance_manual_time_until_lifecycle_gate(
    &consumer_time,
    &mut scale_out_rebalance_gate,
    "former owner did not begin its scale-out rebalance",
  )
  .await?;
  scale_out_rebalance_gate.release()?;
  timeout(Duration::from_secs(5), revocation_gate.wait_until_reached())
    .await
    .map_err(|_| anyhow!("former owner did not emit a revocation after scale-out"))??;
  revocation_gate.release()?;

  let revoked = timeout(Duration::from_secs(5), owner_a.next())
    .await
    .map_err(|_| anyhow!("former owner did not surface its pending revocation"))??;
  let NextResult::Revoked(revoked) = revoked else {
    return Err(anyhow!(
      "former owner delivered a record before its revocation"
    ));
  };
  assert!(
    revoked.partitions().contains(&target_partition),
    "target partition must be included in the former owner's revocation"
  );
  revoked.complete().await;
  prefetch_gate.release()?;

  timeout(Duration::from_secs(5), async {
    loop {
      consumer_time.advance(TimeDuration::milliseconds(200));
      tokio::task::yield_now().await;
      let replacement_lease = cluster
        .consumer_lease_store()
        .list_group_leases(group.topic.as_str(), group.group_id.as_str())
        .await?
        .into_iter()
        .find(|lease| lease.key.virtual_partition_id == target_partition)
        .ok_or_else(|| anyhow!("missing replacement lease for target partition"))?;
      if replacement_lease.owner_id == "prefetch-fence-b"
        && replacement_lease.generation > initial_lease.generation
      {
        return Ok::<_, anyhow::Error>(());
      }
    }
  })
  .await
  .map_err(|_| anyhow!("replacement owner did not acquire the revoked partition"))??;

  let mut owner_a_rebalance_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "prefetch-fence-a",
      None,
      None,
    )
    .await?;
  tokio::task::yield_now().await;
  consumer_time.advance(TimeDuration::seconds(1));
  timeout(
    Duration::from_secs(5),
    owner_a_rebalance_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("former owner did not reach its post-revocation rebalance"))??;

  // A delivery from the former owner is an observable failure. B's delivery is the only success
  // boundary after B owns the replacement lease. A is paused before a later rebalance can
  // legitimately reassign the partition while B's prefetch worker is driven with manual time.
  let replacement_record = timeout(Duration::from_secs(5), async {
    loop {
      consumer_time.advance(TimeDuration::seconds(1));
      tokio::task::yield_now().await;

      tokio::select! {
        owner_a_result = owner_a.next() => {
          match owner_a_result? {
            NextResult::Record(record) => {
              let diagnostics = owner_a
                .diagnostics()
                .expect("concrete consumer provides diagnostics")
                .state_snapshot();
              return Err(anyhow!(
                "former owner delivered revoked partition {} at offset {}: {diagnostics:#?}",
                record.virtual_partition_id,
                record.offset,
              ));
            },
            NextResult::Revoked(revoked) => revoked.complete().await,
          }
        },
        owner_b_result = owner_b.next() => {
          match owner_b_result? {
            NextResult::Revoked(revoked) => revoked.complete().await,
            NextResult::Record(record) => return Ok(record),
          }
        },
        () = tokio::task::yield_now() => {},
      }
    }
  })
  .await
  .map_err(|_| anyhow!("replacement owner did not receive the revoked partition record"))??;
  assert_eq!(replacement_record.virtual_partition_id, target_partition);
  assert_eq!(
    String::from_utf8(replacement_record.record.payload.to_vec())?,
    target_id
  );
  owner_b.store_offset(
    replacement_record.virtual_partition_id,
    replacement_record.offset,
  )?;
  owner_b.commit().await?;
  owner_a_rebalance_gate.release()?;

  let lease = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == target_partition)
    .ok_or_else(|| anyhow!("missing replacement lease for target partition"))?;
  assert_eq!(
    lease.owner_id,
    runtime_b.group.as_ref().unwrap().member_id.as_str()
  );
  assert!(
    lease
      .committed_cursor
      .is_some_and(|cursor| cursor.source_checkpoint.is_some())
  );

  Box::new(owner_a).shutdown().await?;
  Box::new(owner_b).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies stale owner heartbeats/commits are fenced after generation changes.
#[tokio::test]
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
      offset_datetime_from_unix_millis(start_ts_ms),
      TimeDuration::milliseconds(lease_duration_ms),
    )
    .await?;
  assert!(matches!(
    initial_assignment,
    ConsumerGroupAssignmentOutcome::Assigned { .. }
  ));

  let initial_heartbeat = lease_store
    .heartbeat_partition(
      &key,
      "member-a",
      initial_generation,
      offset_datetime_from_unix_millis(start_ts_ms + 10),
      TimeDuration::milliseconds(lease_duration_ms),
      Some(CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 10,
        source_checkpoint: None,
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
      source_checkpoint: None,
    })
  );

  // Step 2: Simulate rebalance takeover after lease expiry with a newer generation.
  let takeover_assignment = lease_store
    .assign_partition(
      key.clone(),
      "member-b".to_string(),
      takeover_generation,
      offset_datetime_from_unix_millis(start_ts_ms + 250),
      TimeDuration::milliseconds(lease_duration_ms),
    )
    .await?;
  let ConsumerGroupAssignmentOutcome::Assigned {
    lease: takeover_lease,
    transition,
    ..
  } = takeover_assignment
  else {
    return Err(anyhow!("expected takeover assignment by new generation"));
  };
  assert_eq!(takeover_lease.owner_id, "member-b");
  assert_eq!(takeover_lease.generation, takeover_generation);
  assert!(matches!(
    transition,
    ConsumerGroupLeaseTransition::ExpiryTakeover {
      previous_owner_id,
      previous_generation,
      ..
    } if previous_owner_id == "member-a" && previous_generation == initial_generation
  ));

  let takeover_heartbeat = lease_store
    .heartbeat_partition(
      &key,
      "member-b",
      takeover_generation,
      offset_datetime_from_unix_millis(start_ts_ms + 260),
      TimeDuration::milliseconds(lease_duration_ms),
      Some(CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 20,
        source_checkpoint: None,
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
      source_checkpoint: None,
    })
  );

  // Step 3: Stale owner attempts to heartbeat/commit and must be fenced.
  let stale_heartbeat = lease_store
    .heartbeat_partition(
      &key,
      "member-a",
      initial_generation,
      offset_datetime_from_unix_millis(start_ts_ms + 270),
      TimeDuration::milliseconds(lease_duration_ms),
      Some(CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 999,
        source_checkpoint: None,
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
      source_checkpoint: None,
    })
  );

  let stale_commit = lease_store
    .commit_cursor(
      &key,
      "member-a",
      initial_generation,
      offset_datetime_from_unix_millis(start_ts_ms + 280),
      CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 999,
        source_checkpoint: None,
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
      source_checkpoint: None,
    })
  );

  // Step 4: Active owner remains authoritative and can advance cursor monotonically.
  let active_commit = lease_store
    .commit_cursor(
      &key,
      "member-b",
      takeover_generation,
      offset_datetime_from_unix_millis(start_ts_ms + 290),
      CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 21,
        source_checkpoint: None,
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
      source_checkpoint: None,
    })
  );

  let stale_after_commit = lease_store
    .heartbeat_partition(
      &key,
      "member-a",
      initial_generation,
      offset_datetime_from_unix_millis(start_ts_ms + 300),
      TimeDuration::milliseconds(lease_duration_ms),
      Some(CommittedCursor {
        virtual_partition_id: key.virtual_partition_id,
        seq_end: 1_000,
        source_checkpoint: None,
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
      source_checkpoint: None,
    })
  );

  // Step 5: Clean up resources.
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies an iterator commit is fenced after ownership changes and its staged
// record is redelivered from the durable cursor.
#[tokio::test]
async fn live_consumer_commit_race_is_fenced_and_redelivered() -> Result<()> {
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::in_memory(1)
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut runtime_a = consumer_runtime_config("commit-race-owner");
  let mut runtime_b = consumer_runtime_config("commit-race-replacement");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("commit-race consumer read config missing"))?
      .strongly_consistent_metadata_reads = Some(true);
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("commit-race group config missing"))?;

  let id = "commit-race-staged";
  let ack = produce_message(&producer, b"commit-race-key".to_vec(), id).await?;

  let mut owner_a = ControlledConsumer::new(
    runtime_a.group.as_ref().unwrap().member_id.as_str(),
    cluster.create_consumer(&runtime_a).await?,
  );
  owner_a.start()?;
  let (target_partition, staged_offset) = timeout(Duration::from_secs(5), async {
    loop {
      consumer_time.advance(TimeDuration::seconds(1));
      tokio::task::yield_now().await;

      match timeout(Duration::from_millis(250), owner_a.next()).await {
        Err(_) => {},
        Ok(Err(error)) => return Err(anyhow!("commit-race owner next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          assert_eq!(record.virtual_partition_id, ack.virtual_partition_id);
          assert_eq!(String::from_utf8(record.record.payload.to_vec())?, id);
          owner_a.store_offset(record.virtual_partition_id, record.offset)?;
          return Ok::<_, anyhow::Error>((record.virtual_partition_id, record.offset));
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("commit-race owner did not stage the record"))??;

  let initial_generation = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == target_partition)
    .ok_or_else(|| anyhow!("missing initial commit-race lease"))?
    .generation;

  let mut commit_gate = cluster
    .lifecycle_hooks()
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeCommit,
      runtime_a.group.as_ref().unwrap().member_id.as_str(),
      None,
      None,
    )
    .await?;
  let mut owner_b = ControlledConsumer::new(
    runtime_b.group.as_ref().unwrap().member_id.as_str(),
    cluster.create_consumer(&runtime_b).await?,
  );
  let (stale_report, takeover_generation) = {
    let stale_commit = owner_a.commit();
    tokio::pin!(stale_commit);
    timeout(Duration::from_secs(5), async {
      tokio::select! {
        result = &mut stale_commit => Err(anyhow!(
          "owner commit completed before the ownership-loss gate: {result:?}"
        )),
        result = commit_gate.wait_until_reached() => result,
      }
    })
    .await
    .map_err(|_| anyhow!("owner commit did not reach the pre-commit gate"))??;

    owner_b.start()?;
    let takeover_generation = timeout(Duration::from_secs(5), async {
      for _ in 0 .. 8 {
        consumer_time.advance(TimeDuration::seconds(1));
        tokio::task::yield_now().await;

        if let Some(lease) = cluster
          .consumer_lease_store()
          .list_group_leases(group.topic.as_str(), group.group_id.as_str())
          .await?
          .into_iter()
          .find(|lease| {
            lease.key.virtual_partition_id == target_partition
              && lease.owner_id == runtime_b.group.as_ref().unwrap().member_id.as_str()
              && lease.generation > initial_generation
          })
        {
          return Ok::<_, anyhow::Error>(lease.generation);
        }
      }
      Err(anyhow!("replacement did not acquire the staged partition"))
    })
    .await
    .map_err(|_| anyhow!("ownership did not move to the replacement"))??;

    commit_gate.release()?;
    let stale_report = timeout(Duration::from_secs(5), &mut stale_commit)
      .await
      .map_err(|_| anyhow!("stale owner commit did not resume after gate release"))??;
    (stale_report, takeover_generation)
  };
  assert!(stale_report.renewed_partitions.is_empty());
  assert!(stale_report.fenced_partitions.contains(&target_partition));

  let lease_before_redelivery = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == target_partition)
    .ok_or_else(|| anyhow!("missing replacement lease after stale commit"))?;
  assert_eq!(lease_before_redelivery.generation, takeover_generation);
  assert!(lease_before_redelivery.committed_cursor.is_none());

  owner_a.abort_for_test().await?;
  drop(owner_a);

  timeout(Duration::from_secs(5), async {
    loop {
      consumer_time.advance(TimeDuration::seconds(1));
      tokio::task::yield_now().await;

      match timeout(Duration::from_millis(250), owner_b.next()).await {
        Err(_) => {},
        Ok(Err(error)) => return Err(anyhow!("commit-race replacement next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let delivered_id = String::from_utf8(record.record.payload.to_vec())?;
          assert_eq!(record.virtual_partition_id, target_partition);
          assert_eq!(record.offset, staged_offset);
          assert_eq!(delivered_id, id);
          owner_b.store_offset(record.virtual_partition_id, record.offset)?;
          owner_b.commit().await?;
          return Ok::<_, anyhow::Error>(());
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("replacement did not redeliver the fenced record"))??;
  assert_eq!(
    delivery_counts(owner_b.delivery_traces()),
    HashMap::from([(id.to_string(), 1)])
  );

  let recovered_lease = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?
    .into_iter()
    .find(|lease| lease.key.virtual_partition_id == target_partition)
    .ok_or_else(|| anyhow!("missing replacement lease after redelivery"))?;
  assert_eq!(
    recovered_lease.owner_id,
    runtime_b.group.as_ref().unwrap().member_id.as_str()
  );
  assert!(recovered_lease.generation >= takeover_generation);
  assert!(
    recovered_lease
      .committed_cursor
      .is_some_and(|cursor| cursor.seq_end == staged_offset && cursor.source_checkpoint.is_some())
  );

  owner_b.shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies multi-writer virtual partitions are merged without loss or cursor
// regressions.
#[tokio::test]
async fn multi_writer_virtual_partition_merge_correctness() -> Result<()> {
  fn key_for_logical_partition(logical_partition_id: u32) -> Vec<u8> {
    for candidate in 0_u32 .. 50_000 {
      let key = format!("logical-key-{logical_partition_id}-{candidate}").into_bytes();
      if logical_partition_for_key(&key, PARTITION_COUNT) == logical_partition_id {
        return key;
      }
    }

    panic!("failed to find key for logical partition {logical_partition_id} within search budget");
  }

  // Step 1: Start a broker pool for each independent producer writer domain. Both pools share
  // durable storage so the consumer can merge their disjoint virtual partitions.
  let resources = IntegrationResources::create().await?;
  let mut writer_0_cluster = ClusterHarness::builder(&resources, 1)
    .topic_num_writers(2)
    .start()
    .await?;
  let mut writer_1_cluster = ClusterHarness::builder(&resources, 1)
    .topic_num_writers(2)
    .broker_writer_id(1)
    .machine_id_offset(512)
    .start()
    .await?;
  let writer_0_discovery: Arc<dyn BrokerDiscovery> =
    Arc::new(writer_0_cluster.producer_discovery());
  let writer_1_discovery: Arc<dyn BrokerDiscovery> =
    Arc::new(writer_1_cluster.producer_discovery());

  // Step 2: Create producers for writer 0 and writer 1 on the same topic.
  let producer_writer_0 = new_producer(
    producer_config_with_writer_id(0),
    vec![producer_topic_named_with_writers(TOPIC, 2)],
    writer_0_discovery,
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;
  let producer_writer_1 = new_producer(
    producer_config_with_writer_id(1),
    vec![producer_topic_named_with_writers(TOPIC, 2)],
    writer_1_discovery,
    metrics_scope("blob_stream_producer_it"),
  )
  .await?;

  let virtual_partition_ids: Vec<u32> = (0 .. (PARTITION_COUNT * 2)).collect();
  let mut reader = ConsumerReaderImpl::new(
    ConsumerReadConfig {
      topic: TOPIC.to_string().into(),
      ..Default::default()
    },
    virtual_partition_ids,
    HashMap::new(),
    resources.blob_store(),
    resources.metadata_store(),
    framework::rejecting_broker_metadata_query(),
    framework::rejecting_broker_blob_range_query(),
    &metrics_scope("blob_stream_consumer_it"),
    TimeDuration::days(1),
    DEFAULT_MAX_METADATA_PUBLICATION_LAG,
    None,
  )?;

  // Step 3: Produce records into matching logical partitions from both writers.
  let logical_partitions = [1_u32, 5_u32, 9_u32, 13_u32];
  let records_per_writer_per_partition = 6;

  let mut expected_ids = HashSet::new();
  let mut expected_counts_by_partition: HashMap<u32, usize> = HashMap::new();

  for logical_partition_id in logical_partitions {
    let key = key_for_logical_partition(logical_partition_id);

    for sequence in 0 .. records_per_writer_per_partition {
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
  let mut consumed_id_counts = HashMap::new();
  let mut consumed_counts_by_partition: HashMap<u32, usize> = HashMap::new();
  let mut last_seq_end_by_partition: HashMap<u32, u64> = HashMap::new();

  let reader_now = now_unix_seconds().saturating_add(3);
  let deadline = Instant::now() + Duration::from_secs(45);
  while consumed_id_counts.len() < expected_ids.len() {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded while draining multi-writer fan-in: expected={}, consumed={}",
        expected_ids.len(),
        consumed_id_counts.len()
      ));
    }

    let batches = reader.read_available(reader_now).await?;
    if batches.is_empty() {
      tokio::task::yield_now().await;
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
        let id = String::from_utf8(record.payload.to_vec())
          .map_err(|error| anyhow!("consumer payload was not utf-8: {error}"))?;
        *consumed_id_counts.entry(id).or_insert(0_usize) += 1;
      }
    }
  }

  assert_eq!(
    consumed_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    consumed_id_counts.values().all(|count| *count == 1),
    "multi-writer reader duplicated delivery: {consumed_id_counts:?}"
  );
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

  writer_0_cluster.shutdown().await;
  writer_1_cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: verifies a real member crash expires through normal coordination, preserves
// committed progress, and lets a replacement recover the staged record under a new generation.
#[tokio::test]
async fn lease_expiry_takeover_preserves_progress() -> Result<()> {
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::in_memory(1)
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut runtime_a = consumer_runtime_config("expiry-owner");
  let mut runtime_b = consumer_runtime_config("expiry-replacement");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("lease-expiry consumer read config missing"))?
      .strongly_consistent_metadata_reads = Some(true);
  }
  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("lease-expiry consumer group config missing"))?;

  let mut owner_a = cluster.create_consumer(&runtime_a).await?;
  owner_a.start()?;

  let mut committed_ids = HashSet::new();
  for message_id in 0 .. 24 {
    let id = format!("lease-expiry-committed-{message_id}");
    produce_message(
      &producer,
      format!("lease-expiry-key-{}", message_id % PARTITION_COUNT).into_bytes(),
      &id,
    )
    .await?;
    committed_ids.insert(id);
  }

  let mut committed_counts = HashMap::new();
  timeout(Duration::from_secs(5), async {
    while committed_counts.len() < committed_ids.len() {
      consumer_time.advance(TimeDuration::seconds(1));
      tokio::task::yield_now().await;

      match timeout(Duration::from_millis(250), owner_a.next()).await {
        Err(_) => {},
        Ok(Err(error)) => return Err(anyhow!("expiry owner next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          if !committed_ids.contains(&id) {
            return Err(anyhow!("unexpected pre-crash record: {id}"));
          }
          *committed_counts.entry(id).or_insert(0usize) += 1;
          owner_a.store_offset(record.virtual_partition_id, record.offset)?;
          owner_a.commit().await?;
        },
      }
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("expiry owner did not commit the initial phase"))??;
  assert!(committed_counts.values().all(|count| *count == 1));

  let initial_leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  assert_eq!(initial_leases.len(), PARTITION_COUNT as usize);
  assert!(
    initial_leases
      .iter()
      .all(|lease| lease.owner_id == runtime_a.group.as_ref().unwrap().member_id.as_str())
  );
  let initial_generations = initial_leases
    .iter()
    .map(|lease| (lease.key.virtual_partition_id, lease.generation))
    .collect::<HashMap<_, _>>();

  let mut recovery_ids = HashSet::new();
  for message_id in 0 .. 16 {
    let id = format!("lease-expiry-recovery-{message_id}");
    produce_message(
      &producer,
      format!("lease-expiry-key-{}", message_id % PARTITION_COUNT).into_bytes(),
      &id,
    )
    .await?;
    recovery_ids.insert(id);
  }

  owner_a.abort_for_test().await?;
  drop(owner_a);

  // Expire A's membership and partition leases before B observes the group. Gate B after it has
  // admitted its first replacement batch, so delivery assertions do not race the prefetch worker.
  consumer_time.advance(TimeDuration::seconds(3));
  let hooks = cluster.lifecycle_hooks();
  let mut replacement_prefetch_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerPrefetchBatchBuffered,
      "expiry-replacement",
      None,
      None,
    )
    .await?;
  let mut owner_b = cluster.create_consumer(&runtime_b).await?;
  owner_b.start()?;
  framework::advance_manual_time_until_lifecycle_gate(
    &consumer_time,
    &mut replacement_prefetch_gate,
    "replacement owner did not buffer a post-crash batch",
  )
  .await?;
  replacement_prefetch_gate.release()?;

  let mut replacement_counts = HashMap::new();
  let mut recovered_offsets = HashMap::new();
  timeout(Duration::from_secs(5), async {
    while replacement_counts.len() < recovery_ids.len() {
      // The first prefetch batch is gated above; later recovery batches can wait on the
      // consumer clock before their next refill cycle.
      consumer_time.advance(TimeDuration::seconds(1));
      tokio::task::yield_now().await;

      match timeout(Duration::from_millis(250), owner_b.next()).await {
        // A normal empty poll only means the prefetch worker has not buffered the next batch yet.
        Err(_) => {},
        Ok(Err(error)) => return Err(anyhow!("replacement owner next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          if committed_ids.contains(&id) {
            return Err(anyhow!("replacement replayed committed record: {id}"));
          }
          if !recovery_ids.contains(&id) {
            return Err(anyhow!("unexpected replacement record: {id}"));
          }
          *replacement_counts.entry(id).or_insert(0usize) += 1;
          recovered_offsets.insert(record.virtual_partition_id, record.offset);
          owner_b.store_offset(record.virtual_partition_id, record.offset)?;
          owner_b.commit().await?;
        },
      }
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("replacement owner did not recover the post-crash phase"))??;
  assert!(replacement_counts.values().all(|count| *count == 1));

  let active_members = cluster
    .consumer_membership_store()
    .list_active_members(
      group.topic.as_str(),
      group.group_id.as_str(),
      consumer_time.now(),
    )
    .await?;
  assert_eq!(
    member_ids(&active_members),
    vec![runtime_b.group.as_ref().unwrap().member_id.to_string()]
  );

  let replacement_leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  assert!(replacement_leases.iter().all(|lease| {
    lease.owner_id == runtime_b.group.as_ref().unwrap().member_id.as_str()
      && lease.generation > initial_generations[&lease.key.virtual_partition_id]
  }));
  for (partition_id, recovered_offset) in &recovered_offsets {
    let recovery_lease = replacement_leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == *partition_id)
      .ok_or_else(|| anyhow!("missing replacement recovery lease for partition {partition_id}"))?;
    assert!(
      recovery_lease
        .committed_cursor
        .as_ref()
        .is_some_and(|cursor| {
          cursor.seq_end >= *recovered_offset && cursor.source_checkpoint.is_some()
        }),
      "replacement owner did not durably checkpoint recovered partition {partition_id}"
    );
  }

  let (&stale_partition, _) = recovered_offsets
    .iter()
    .next()
    .ok_or_else(|| anyhow!("replacement did not deliver a recovery partition"))?;
  let recovery_lease = replacement_leases
    .iter()
    .find(|lease| lease.key.virtual_partition_id == stale_partition)
    .ok_or_else(|| anyhow!("missing replacement lease for stale-heartbeat check"))?;

  let stale_heartbeat = cluster
    .consumer_lease_store()
    .heartbeat_partition(
      &recovery_lease.key,
      runtime_a.group.as_ref().unwrap().member_id.as_str(),
      initial_generations[&stale_partition],
      consumer_time.now(),
      TimeDuration::milliseconds(2_000),
      recovery_lease.committed_cursor.clone(),
    )
    .await?;
  assert!(matches!(
    stale_heartbeat,
    ConsumerGroupHeartbeatOutcome::HeldByOther(_)
  ));

  Box::new(owner_b).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies a replacement owner recovers exactly the delivered but uncommitted work
// after an abrupt owner loss, while durable committed progress remains skipped.
#[tokio::test]
async fn consumer_crash_recovery_redelivers_only_uncommitted_record() -> Result<()> {
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::in_memory(1)
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut runtime_a = consumer_runtime_config("crash-owner");
  let mut runtime_b = consumer_runtime_config("replacement-owner");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("crash recovery consumer read config missing"))?
      .strongly_consistent_metadata_reads = Some(true);
  }

  let committed_id = "crash-recovery-committed";
  let staged_id = "crash-recovery-staged";
  let committed_ack =
    produce_message(&producer, b"crash-recovery-key".to_vec(), committed_id).await?;
  let staged_ack = produce_message(&producer, b"crash-recovery-key".to_vec(), staged_id).await?;
  assert_eq!(
    committed_ack.virtual_partition_id, staged_ack.virtual_partition_id,
    "records for the same key must remain on one partition"
  );

  let group = runtime_a
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("crash recovery group config missing"))?;
  let mut owner_a = cluster.create_consumer(&runtime_a).await?;
  owner_a.start()?;

  let mut first_generation = None;
  let mut staged_offset = None;
  let mut owner_a_delivery_counts = HashMap::new();
  timeout(Duration::from_secs(5), async {
    while staged_offset.is_none() {
      consumer_time.advance(TimeDuration::seconds(1));
      tokio::task::yield_now().await;

      match timeout(Duration::from_millis(250), owner_a.next()).await {
        Err(_) => {},
        Ok(Err(error)) => return Err(anyhow!("crash owner next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          *owner_a_delivery_counts.entry(id.clone()).or_insert(0usize) += 1;
          owner_a.store_offset(record.virtual_partition_id, record.offset)?;

          if id == committed_id {
            owner_a.commit().await?;
            let leases = cluster
              .consumer_lease_store()
              .list_group_leases(group.topic.as_str(), group.group_id.as_str())
              .await?;
            first_generation = leases
              .iter()
              .find(|lease| lease.key.virtual_partition_id == record.virtual_partition_id)
              .map(|lease| lease.generation);
          } else if id == staged_id {
            staged_offset = Some(record.offset);
          } else {
            return Err(anyhow!("unexpected crash recovery payload: {id}"));
          }
        },
      }
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("crash owner did not deliver the staged record"))??;
  assert_eq!(owner_a_delivery_counts.get(committed_id), Some(&1));
  assert_eq!(owner_a_delivery_counts.get(staged_id), Some(&1));
  let first_generation =
    first_generation.ok_or_else(|| anyhow!("missing crash owner generation"))?;
  let staged_offset = staged_offset.ok_or_else(|| anyhow!("missing staged record offset"))?;

  // Stop local work without the normal graceful commit/release/deregistration sequence.
  owner_a.abort_for_test().await?;
  drop(owner_a);

  let mut owner_b = cluster.create_consumer(&runtime_b).await?;
  owner_b.start()?;
  let mut replacement_delivery_counts = HashMap::new();
  timeout(Duration::from_secs(5), async {
    loop {
      consumer_time.advance(TimeDuration::seconds(1));
      tokio::task::yield_now().await;

      match timeout(Duration::from_millis(250), owner_b.next()).await {
        Err(_) => {},
        Ok(Err(error)) => return Err(anyhow!("replacement owner next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          *replacement_delivery_counts
            .entry(id.clone())
            .or_insert(0usize) += 1;
          owner_b.store_offset(record.virtual_partition_id, record.offset)?;
          owner_b.commit().await?;
          if id == staged_id {
            return Ok::<_, anyhow::Error>(());
          }
        },
      }
    }
  })
  .await
  .map_err(|_| anyhow!("replacement owner did not recover the staged record"))??;

  assert_eq!(replacement_delivery_counts.get(committed_id), None);
  assert_eq!(replacement_delivery_counts.get(staged_id), Some(&1));

  let leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  let recovered_lease = leases
    .iter()
    .find(|lease| lease.key.virtual_partition_id == staged_ack.virtual_partition_id)
    .ok_or_else(|| anyhow!("missing replacement owner lease"))?;
  assert_eq!(
    recovered_lease.owner_id,
    runtime_b.group.as_ref().unwrap().member_id.as_str()
  );
  assert!(
    recovered_lease.generation > first_generation,
    "replacement owner must receive a newer generation"
  );
  assert_eq!(
    recovered_lease
      .committed_cursor
      .as_ref()
      .map(|cursor| cursor.seq_end),
    Some(staged_offset),
    "replacement must durably commit the recovered record"
  );
  assert!(
    recovered_lease
      .committed_cursor
      .as_ref()
      .is_some_and(|cursor| cursor.source_checkpoint.is_some()),
    "replacement commit must retain the recovered record source checkpoint"
  );

  let stale_heartbeat = cluster
    .consumer_lease_store()
    .heartbeat_partition(
      &recovered_lease.key,
      runtime_a.group.as_ref().unwrap().member_id.as_str(),
      first_generation,
      consumer_time.now(),
      TimeDuration::milliseconds(2_000),
      recovered_lease.committed_cursor.clone(),
    )
    .await?;
  assert!(
    matches!(
      stale_heartbeat,
      ConsumerGroupHeartbeatOutcome::HeldByOther(_)
    ),
    "crashed owner generation must remain fenced after replacement takeover"
  );

  Box::new(owner_b).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies recovery hands an active-window visibility deferral to Fast instead of
// tail-chasing it until the metadata window closes.
#[tokio::test]
async fn consumer_restart_hands_active_window_visibility_deferral_to_fast() -> Result<()> {
  let broker_start = OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
  let broker_time = Arc::new(framework::ManualTimeProvider::new(broker_start));
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(
    broker_start + TimeDuration::seconds(10),
  ));
  let mut cluster = ClusterHarness::in_memory(1)
    .broker_flush_max_delay(Duration::from_secs(1))
    .broker_time_provider(broker_time.clone())
    .consumer_time_provider(consumer_time.clone())
    .start()
    .await?;
  let producer = Arc::new(
    cluster
      .create_producer(producer_config(), vec![producer_topic()])
      .await?,
  );

  let mut owner_runtime = consumer_runtime_config("visibility-owner");
  let mut replacement_runtime = consumer_runtime_config("visibility-replacement");
  for runtime in [&mut owner_runtime, &mut replacement_runtime] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("visibility recovery read config missing"))?
      .metadata_visibility_delay = TimeDuration::milliseconds(5_000).into_proto();
    runtime
      .group
      .as_mut()
      .ok_or_else(|| anyhow!("visibility recovery group config missing"))?
      .lease_duration = TimeDuration::milliseconds(20_000).into_proto();
  }
  let group = owner_runtime
    .group
    .as_ref()
    .ok_or_else(|| anyhow!("visibility recovery group config missing"))?;
  let key = b"visibility-recovery-key".to_vec();
  let hooks = cluster.lifecycle_hooks();

  let virtual_partition_id = virtual_partition_for_logical(
    logical_partition_for_key(&key, PARTITION_COUNT),
    PARTITION_COUNT,
    0,
  );
  let mut first_flush = hooks
    .arm_broker_for_partition(
      LifecycleEvent::BrokerBeforeFlushPersist,
      virtual_partition_id,
    )
    .await?;
  let first_producer = producer.clone();
  let first_key = key.clone();
  let first_publish = tokio::spawn(async move {
    produce_message(&first_producer, first_key, "visibility-checkpoint").await
  });
  timeout(Duration::from_secs(5), async {
    loop {
      let buffered = cluster
        .broker_state_snapshots()
        .await
        .iter()
        .any(|snapshot| {
          snapshot
            .topics
            .iter()
            .flat_map(|topic| &topic.local_partitions)
            .any(|partition| {
              partition.virtual_partition_id == virtual_partition_id
                && partition.buffered_batch_count == 1
            })
        });
      if buffered {
        return;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("checkpoint record did not enter the broker buffer"))?;
  broker_time.advance(TimeDuration::seconds(1));
  timeout(Duration::from_secs(5), first_flush.wait_until_reached())
    .await
    .map_err(|_| anyhow!("checkpoint record did not reach the broker flush boundary"))??;
  first_flush.release()?;
  let checkpoint_ack = first_publish
    .await
    .map_err(|error| anyhow!("checkpoint producer task failed: {error}"))??;
  assert_eq!(checkpoint_ack.virtual_partition_id, virtual_partition_id);

  let mut owner = cluster.create_consumer(&owner_runtime).await?;
  owner.start()?;
  let mut owner_counts = HashMap::new();
  let mut checkpoint_offset = None;
  timeout(Duration::from_secs(5), async {
    while checkpoint_offset.is_none() {
      tokio::task::yield_now().await;
      match timeout(Duration::from_millis(250), owner.next()).await {
        Err(_) => {},
        Ok(Err(error)) => return Err(anyhow!("checkpoint owner next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          *owner_counts.entry(id.clone()).or_insert(0usize) += 1;
          if id != "visibility-checkpoint" {
            return Err(anyhow!("unexpected checkpoint owner record: {id}"));
          }
          owner.store_offset(record.virtual_partition_id, record.offset)?;
          owner.commit().await?;
          checkpoint_offset = Some(record.offset);
        },
      }
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("checkpoint owner did not deliver its record"))??;
  assert_eq!(
    owner_counts,
    HashMap::from([("visibility-checkpoint".to_string(), 1)])
  );
  let checkpoint_offset = checkpoint_offset.ok_or_else(|| anyhow!("missing checkpoint offset"))?;

  let owner_leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  let owner_lease = owner_leases
    .iter()
    .find(|lease| lease.key.virtual_partition_id == virtual_partition_id)
    .ok_or_else(|| anyhow!("missing checkpoint owner lease"))?;
  let owner_generation = owner_lease.generation;
  assert!(
    owner_lease.committed_cursor.as_ref().is_some_and(|cursor| {
      cursor.seq_end >= checkpoint_offset && cursor.source_checkpoint.is_some()
    }),
    "checkpoint owner did not durably persist its source checkpoint: {owner_lease:?}"
  );
  Box::new(owner).shutdown().await?;

  // Keep the post-checkpoint row in the same metadata window, but later than the replacement's
  // visibility cutoff. Recovery must hand it to Fast instead of waiting for the window to close.
  let mut deferred_flush = hooks
    .arm_broker_for_partition(
      LifecycleEvent::BrokerBeforeFlushPersist,
      virtual_partition_id,
    )
    .await?;
  let deferred_producer = producer.clone();
  let deferred_key = key.clone();
  let deferred_publish = tokio::spawn(async move {
    produce_message(&deferred_producer, deferred_key, "visibility-deferred").await
  });
  timeout(Duration::from_secs(5), async {
    loop {
      let buffered = cluster
        .broker_state_snapshots()
        .await
        .iter()
        .any(|snapshot| {
          snapshot
            .topics
            .iter()
            .flat_map(|topic| &topic.local_partitions)
            .any(|partition| {
              partition.virtual_partition_id == virtual_partition_id
                && partition.buffered_batch_count == 1
            })
        });
      if buffered {
        return;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("deferred record did not enter the broker buffer"))?;
  broker_time.advance(TimeDuration::seconds(11));
  timeout(Duration::from_secs(5), deferred_flush.wait_until_reached())
    .await
    .map_err(|_| anyhow!("deferred record did not reach the broker flush boundary"))??;
  deferred_flush.release()?;
  let deferred_ack = deferred_publish
    .await
    .map_err(|error| anyhow!("deferred producer task failed: {error}"))??;
  assert_eq!(deferred_ack.virtual_partition_id, virtual_partition_id);

  let mut fast_path_active = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRecoveryFastPathActive,
      "visibility-replacement",
      Some(virtual_partition_id),
      None,
    )
    .await?;
  let mut replacement = cluster.create_consumer(&replacement_runtime).await?;
  replacement.start()?;
  consumer_time.advance(TimeDuration::seconds(1));
  timeout(
    Duration::from_secs(5),
    fast_path_active.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("replacement recovery did not activate Fast before visibility"))??;
  fast_path_active.release()?;

  let replacement_leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  let replacement_lease = replacement_leases
    .iter()
    .find(|lease| lease.key.virtual_partition_id == virtual_partition_id)
    .ok_or_else(|| anyhow!("missing replacement visibility lease"))?;
  assert_eq!(
    replacement_lease.owner_id,
    replacement_runtime
      .group
      .as_ref()
      .unwrap()
      .member_id
      .as_str()
  );
  assert!(
    replacement_lease.generation > owner_generation,
    "replacement must hold a newer generation"
  );
  assert_eq!(
    replacement_lease
      .committed_cursor
      .as_ref()
      .map(|cursor| cursor.seq_end),
    Some(checkpoint_offset),
    "Fast handoff must not advance the durable cursor past deferred metadata"
  );

  let mut deferred_prefetch_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerPrefetchBatchBuffered,
      "visibility-replacement",
      Some(virtual_partition_id),
      None,
    )
    .await?;
  consumer_time.advance(TimeDuration::seconds(7));
  framework::advance_manual_time_until_lifecycle_gate(
    &consumer_time,
    &mut deferred_prefetch_gate,
    "Fast path did not buffer the deferred record after visibility",
  )
  .await?;
  deferred_prefetch_gate.release()?;
  let mut replacement_counts = HashMap::new();
  let mut deferred_offset = None;
  timeout(Duration::from_secs(5), async {
    while deferred_offset.is_none() {
      tokio::task::yield_now().await;
      match timeout(Duration::from_millis(250), replacement.next()).await {
        Err(_) => {
          return Err(anyhow!(
            "Fast path did not deliver its buffered deferred record"
          ));
        },
        Ok(Err(error)) => return Err(anyhow!("replacement next failed: {error}")),
        Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
        Ok(Ok(NextResult::Record(record))) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          *replacement_counts.entry(id.clone()).or_insert(0usize) += 1;
          if id != "visibility-deferred" {
            return Err(anyhow!("unexpected replacement record: {id}"));
          }
          replacement.store_offset(record.virtual_partition_id, record.offset)?;
          replacement.commit().await?;
          deferred_offset = Some(record.offset);
        },
      }
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("Fast path did not deliver the deferred record after visibility"))??;
  assert_eq!(
    replacement_counts,
    HashMap::from([("visibility-deferred".to_string(), 1)])
  );
  let deferred_offset = deferred_offset.ok_or_else(|| anyhow!("missing deferred offset"))?;

  let final_leases = cluster
    .consumer_lease_store()
    .list_group_leases(group.topic.as_str(), group.group_id.as_str())
    .await?;
  let final_lease = final_leases
    .iter()
    .find(|lease| lease.key.virtual_partition_id == virtual_partition_id)
    .ok_or_else(|| anyhow!("missing final visibility lease"))?;
  assert!(
    final_lease.committed_cursor.as_ref().is_some_and(|cursor| {
      cursor.seq_end >= deferred_offset && cursor.source_checkpoint.is_some()
    }),
    "replacement did not durably commit the deferred record: {final_lease:?}"
  );

  Box::new(replacement).shutdown().await?;
  cluster.shutdown().await;
  Ok(())
}

// High-level: validates group membership discovers new members and rebalances on scale-out.
#[tokio::test]
async fn dynamic_membership_scale_out_rebalances() -> Result<()> {
  // This exercises group coordination rather than backend serialization; keep all participants
  // on the harness's shared in-memory stores to make membership convergence deterministic.
  let mut cluster = ClusterHarness::in_memory(1).start().await?;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;
  let consumer_lease_store = cluster.consumer_lease_store();

  let runtime_a = consumer_runtime_config("bootstrap-a");
  let runtime_b = consumer_runtime_config("bootstrap-b");

  // Produce upfront so rebalance happens while data is already available to consume.
  let mut expected_ids = HashSet::new();
  for message_id in 0 .. 24 {
    let id = format!("it-013-{message_id}");
    produce_message(
      &producer,
      format!("it-013-key-{}", message_id % 8).into_bytes(),
      &id,
    )
    .await?;
    expected_ids.insert(id);
  }

  let mut consumer_a = cluster.create_consumer(&runtime_a).await?;
  let mut consumer_b = cluster.create_consumer(&runtime_b).await?;
  consumer_a.start()?;
  consumer_b.start()?;

  // Convergence signals:
  // - `saw_revocation`: assignment changed after second member joined.
  // - progress/renewal flags: each member did useful work (consumed or renewed ownership).
  let mut saw_revocation = false;
  let mut consumer_a_progress = false;
  let mut consumer_b_progress = false;
  let mut consumer_a_renewed = false;
  let mut consumer_b_renewed = false;
  let mut delivery_traces_a = ConsumerDeliveryTraces::new();
  let mut delivery_traces_b = ConsumerDeliveryTraces::new();
  let deadline = Instant::now() + Duration::from_secs(8);
  while !saw_revocation
    || !(consumer_a_progress || consumer_a_renewed)
    || !(consumer_b_progress || consumer_b_renewed)
  {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "deadline exceeded in IT-013: revocation={saw_revocation}, \
         consumer_a_progress={consumer_a_progress}, consumer_b_progress={consumer_b_progress}, \
         consumer_a_renewed={consumer_a_renewed}, consumer_b_renewed={consumer_b_renewed}"
      ));
    }

    // Observe whichever bootstrap consumer has work without delaying it behind an idle peer.
    tokio::select! {
      result = poll_consumer_once(&mut consumer_a, "bootstrap-a", &mut delivery_traces_a) => {
        let (progress, revoked) = result?;
        saw_revocation |= revoked;
        consumer_a_progress |= progress;
      }
      result = poll_consumer_once(&mut consumer_b, "bootstrap-b", &mut delivery_traces_b) => {
        let (progress, revoked) = result?;
        saw_revocation |= revoked;
        consumer_b_progress |= progress;
      }
    }

    // Each completed poll is an explicit convergence boundary. Commit both iterators here to
    // exercise their heartbeat/renew paths even when one member received no records.
    let report_a = consumer_a.commit().await?;
    let report_b = consumer_b.commit().await?;
    consumer_a_renewed |= !report_a.renewed_partitions.is_empty();
    consumer_b_renewed |= !report_b.renewed_partitions.is_empty();
  }

  assert!(saw_revocation, "expected revocation during scale-out");
  assert!(
    (consumer_a_progress || consumer_a_renewed) && (consumer_b_progress || consumer_b_renewed),
    "expected both consumers to make forward progress"
  );

  // Only the active group members may establish the final no-loss result.
  let drain_deadline = Instant::now() + Duration::from_secs(12);
  while delivery_traces_a
    .keys()
    .chain(delivery_traces_b.keys())
    .collect::<HashSet<_>>()
    .len()
    < expected_ids.len()
  {
    if Instant::now() >= drain_deadline {
      return Err(anyhow!(
        "members did not drain scale-out traffic: expected={}, consumed_by_a={}, consumed_by_b={}",
        expected_ids.len(),
        delivery_traces_a.len(),
        delivery_traces_b.len()
      ));
    }

    // Await whichever already-started member has work instead of spending up to two
    // seconds polling an idle member before polling the other.
    tokio::select! {
      result = poll_consumer_once(&mut consumer_a, "bootstrap-a", &mut delivery_traces_a) => {
        result?;
      }
      result = poll_consumer_once(&mut consumer_b, "bootstrap-b", &mut delivery_traces_b) => {
        result?;
      }
    }
  }
  let mut delivery_traces = delivery_traces_a;
  for (id, deliveries) in delivery_traces_b {
    delivery_traces.entry(id).or_default().extend(deliveries);
  }
  let delivered_id_counts = delivery_counts(&delivery_traces);
  assert_eq!(
    delivered_id_counts.keys().cloned().collect::<HashSet<_>>(),
    expected_ids
  );
  assert!(
    delivered_id_counts.values().all(|count| *count >= 1),
    "scale-out requires at-least-once delivery: counts={delivered_id_counts:?}, \
     traces={delivery_traces:?}"
  );
  assert!(
    delivery_members(&delivery_traces)
      .iter()
      .all(|member_id| ["bootstrap-a", "bootstrap-b"].contains(&member_id.as_str())),
    "scale-out delivery came from a non-group member: {delivery_traces:?}"
  );

  let leases = consumer_lease_store
    .list_group_leases(TOPIC, "integration-group")
    .await?;
  for (partition_id, maximum_offset) in maximum_delivery_offsets(&delivery_traces) {
    let lease = leases
      .iter()
      .find(|lease| lease.key.virtual_partition_id == partition_id)
      .ok_or_else(|| anyhow!("missing lease for partition {partition_id}"))?;
    assert!(
      lease.committed_cursor.as_ref().is_some_and(|cursor| {
        cursor.seq_end >= maximum_offset && cursor.source_checkpoint.is_some()
      }),
      "scale-out did not durably commit partition {partition_id}: {lease:?}"
    );
  }

  // Clean shutdown to avoid dangling members across tests.
  let _ = Box::new(consumer_a).shutdown().await;
  let _ = Box::new(consumer_b).shutdown().await;
  cluster.shutdown().await;
  Ok(())
}

// High-level: verifies workers on the same physical pod share an aggregate partition budget.
#[tokio::test]
async fn consumer_group_balances_partitions_across_configured_pods() -> Result<()> {
  let mut cluster = ClusterHarness::in_memory(1).start().await?;
  let workers = [
    ("pod-a-worker-0", "pod-a"),
    ("pod-a-worker-1", "pod-a"),
    ("pod-b-worker-0", "pod-b"),
    ("pod-b-worker-1", "pod-b"),
    ("pod-c-worker-0", "pod-c"),
    ("pod-c-worker-1", "pod-c"),
  ];
  let worker_pods = workers.iter().copied().collect::<HashMap<_, _>>();
  let (event_tx, mut event_rx) = mpsc::unbounded_channel();
  let mut stop_txs = Vec::new();
  let mut consumer_tasks = Vec::new();

  for (member_id, pod_id) in workers {
    let mut runtime = consumer_runtime_config(member_id);
    runtime
      .group
      .as_mut()
      .ok_or_else(|| anyhow!("pod balancing consumer group config missing"))?
      .pod_id = Some(pod_id.to_string().into());
    let consumer = Box::new(cluster.create_consumer(&runtime).await?);
    let (stop_tx, stop_rx) = watch::channel(false);
    stop_txs.push(stop_tx);
    consumer_tasks.push(tokio::spawn(run_consumer_task(
      consumer,
      stop_rx,
      event_tx.clone(),
    )));
  }
  drop(event_tx);

  let leases = timeout(Duration::from_secs(10), async {
    loop {
      while let Ok(event) = event_rx.try_recv() {
        if let ConsumerTaskEvent::Revoked { ack } = event {
          ack
            .send(())
            .map_err(|()| anyhow!("pod balancing consumer stopped before revocation completed"))?;
        }
      }

      let leases = cluster
        .consumer_lease_store()
        .list_group_leases(TOPIC, "integration-group")
        .await?;
      let owners = leases
        .iter()
        .map(|lease| lease.owner_id.as_str())
        .collect::<HashSet<_>>();
      if leases.len() == PARTITION_COUNT as usize
        && owners.len() == workers.len()
        && owners.iter().all(|owner| worker_pods.contains_key(owner))
      {
        return Ok::<_, anyhow::Error>(leases);
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("pod-aware consumer group did not converge"))??;

  let mut pod_loads = HashMap::<&str, usize>::new();
  let mut member_loads = HashMap::<&str, usize>::new();
  for lease in &leases {
    let pod_id = worker_pods
      .get(lease.owner_id.as_str())
      .ok_or_else(|| anyhow!("lease owner lacks configured pod: {lease:?}"))?;
    *pod_loads.entry(*pod_id).or_default() += 1;
    *member_loads.entry(lease.owner_id.as_str()).or_default() += 1;
  }
  let mut sorted_pod_loads = pod_loads.values().copied().collect::<Vec<_>>();
  sorted_pod_loads.sort_unstable();
  assert_eq!(sorted_pod_loads, vec![5, 5, 6]);
  for pod_id in ["pod-a", "pod-b", "pod-c"] {
    let worker_loads = workers
      .iter()
      .filter(|(_, worker_pod_id)| *worker_pod_id == pod_id)
      .map(|(member_id, _)| {
        member_loads
          .get(member_id)
          .copied()
          .ok_or_else(|| anyhow!("worker {member_id} did not receive a lease"))
      })
      .collect::<Result<Vec<_>>>()?;
    let min_load = worker_loads.iter().min().copied().unwrap_or_default();
    let max_load = worker_loads.iter().max().copied().unwrap_or_default();
    assert!(
      max_load - min_load <= 1,
      "workers on {pod_id} are not balanced: {worker_loads:?}"
    );
  }

  let plan = cluster
    .consumer_membership_store()
    .get_assignment_plan(TOPIC, "integration-group")
    .await?
    .ok_or_else(|| anyhow!("pod-aware consumer group did not persist an assignment plan"))?;
  assert_eq!(
    plan.member_topology,
    Some(
      workers
        .iter()
        .map(|(member_id, pod_id)| ConsumerGroupMember {
          member_id: (*member_id).to_string(),
          pod_id: Some((*pod_id).to_string()),
        })
        .collect()
    )
  );

  for stop_tx in stop_txs {
    let _ = stop_tx.send(true);
  }
  for task in consumer_tasks {
    task
      .await
      .map_err(|error| anyhow!("pod balancing consumer task join error: {error}"))??;
  }
  cluster.shutdown().await;
  Ok(())
}

// High-level: validates a bootstrap consumer gracefully restarts through the production S3 and
// Dynamo configuration path without replaying committed data.
#[tokio::test]
async fn bootstrap_graceful_restart_resumes_durable_s3_dynamo_progress() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .blob_store(resources.s3_blob_store())
    .start()
    .await?;
  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;
  let config = consumer_bootstrap_config("bootstrap-restart", &resources);
  let hooks = cluster.lifecycle_hooks();

  let mut phase1_expected = HashSet::new();
  for message_id in 0 .. 12 {
    let id = format!("bootstrap-restart-phase1-{message_id}");
    produce_message(
      &producer,
      format!("bootstrap-restart-key-{}", message_id % 4).into_bytes(),
      &id,
    )
    .await?;
    phase1_expected.insert(id);
  }

  let mut initial_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "bootstrap-restart",
      None,
      None,
    )
    .await?;
  let mut consumer = ConsumerBootstrapIteratorBuilder::new(
    ConsumerBootstrapConfig::from_proto_config(&config)?,
    metrics_scope("blob_stream_consumer_it"),
    None,
  )
  .lifecycle_hooks(Arc::new(hooks.clone()))
  .build()
  .await?;
  consumer.start()?;
  timeout(
    Duration::from_secs(5),
    initial_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("bootstrap consumer did not reach its initial rebalance boundary"))??;
  initial_rebalance.release()?;

  let mut phase1_traces = ConsumerDeliveryTraces::new();
  let phase1_deadline = Instant::now() + Duration::from_secs(20);
  while phase1_traces.len() < phase1_expected.len() {
    if Instant::now() >= phase1_deadline {
      return Err(anyhow!(
        "bootstrap consumer did not drain phase 1: expected={}, received={}",
        phase1_expected.len(),
        phase1_traces.len()
      ));
    }
    let _ = poll_consumer_once(&mut consumer, "bootstrap-restart", &mut phase1_traces).await?;
  }
  assert_eq!(
    phase1_traces.keys().cloned().collect::<HashSet<_>>(),
    phase1_expected
  );
  assert!(
    delivery_counts(&phase1_traces)
      .values()
      .all(|count| *count == 1),
    "bootstrap restart phase 1 must be exactly once: {phase1_traces:?}"
  );
  let phase1_offsets = maximum_delivery_offsets(&phase1_traces);
  wait_for_group_offsets_committed(
    &cluster,
    &phase1_offsets,
    "bootstrap phase 1 cursors did not durably commit",
  )
  .await?;

  let mut before_commit = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeCommit,
      "bootstrap-restart",
      None,
      None,
    )
    .await?;
  let mut commit_finished = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerShutdownCommitFinished,
      "bootstrap-restart",
      None,
      None,
    )
    .await?;
  let mut before_release = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeReleaseOwned,
      "bootstrap-restart",
      None,
      None,
    )
    .await?;
  let mut before_deregister = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeDeregisterMember,
      "bootstrap-restart",
      None,
      None,
    )
    .await?;
  let shutdown_task = tokio::spawn(async move { Box::new(consumer).shutdown().await });
  timeout(Duration::from_secs(5), before_commit.wait_until_reached())
    .await
    .map_err(|_| anyhow!("bootstrap shutdown did not reach its final commit boundary"))??;
  before_commit.release()?;
  timeout(Duration::from_secs(5), commit_finished.wait_until_reached())
    .await
    .map_err(|_| anyhow!("bootstrap shutdown did not complete its final commit"))??;
  commit_finished.release()?;
  timeout(Duration::from_secs(5), before_release.wait_until_reached())
    .await
    .map_err(|_| anyhow!("bootstrap shutdown did not reach its release boundary"))??;
  before_release.release()?;
  timeout(
    Duration::from_secs(5),
    before_deregister.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("bootstrap shutdown did not reach its deregistration boundary"))??;
  before_deregister.release()?;
  shutdown_task
    .await
    .map_err(|error| anyhow!("bootstrap shutdown task failed: {error}"))??;

  assert!(
    resources
      .consumer_membership_store()
      .list_active_members(
        TOPIC,
        "integration-group",
        offset_datetime_from_unix_millis(now_unix_millis()),
      )
      .await?
      .is_empty(),
    "bootstrap member remained registered after graceful shutdown"
  );

  let mut phase2_expected = HashSet::new();
  for message_id in 0 .. 12 {
    let id = format!("bootstrap-restart-phase2-{message_id}");
    produce_message(
      &producer,
      format!("bootstrap-restart-key-{}", message_id % 4).into_bytes(),
      &id,
    )
    .await?;
    phase2_expected.insert(id);
  }

  let mut restart_rebalance = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "bootstrap-restart",
      None,
      None,
    )
    .await?;
  let mut restarted_consumer = ConsumerBootstrapIteratorBuilder::new(
    ConsumerBootstrapConfig::from_proto_config(&config)?,
    metrics_scope("blob_stream_consumer_it"),
    None,
  )
  .lifecycle_hooks(Arc::new(hooks))
  .build()
  .await?;
  restarted_consumer.start()?;
  timeout(
    Duration::from_secs(5),
    restart_rebalance.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("restarted bootstrap consumer did not reach its rebalance boundary"))??;
  restart_rebalance.release()?;

  let mut phase2_traces = ConsumerDeliveryTraces::new();
  let mut replayed_phase1 = ConsumerDeliveryTraces::new();
  let phase2_deadline = Instant::now() + Duration::from_secs(20);
  while phase2_traces.len() < phase2_expected.len() {
    if Instant::now() >= phase2_deadline {
      return Err(anyhow!(
        "restarted bootstrap consumer did not drain phase 2: expected={}, received={}",
        phase2_expected.len(),
        phase2_traces.len()
      ));
    }
    match timeout(Duration::from_secs(2), restarted_consumer.next()).await {
      Err(_) => {},
      Ok(Err(error)) => return Err(anyhow!("restarted bootstrap next failed: {error}")),
      Ok(Ok(NextResult::Revoked(revoked))) => revoked.complete().await,
      Ok(Ok(NextResult::Record(record))) => {
        let id = String::from_utf8(record.record.payload.to_vec())?;
        let target_traces = if phase1_expected.contains(&id) {
          &mut replayed_phase1
        } else if phase2_expected.contains(&id) {
          &mut phase2_traces
        } else {
          return Err(anyhow!(
            "restarted bootstrap consumer delivered unexpected record: {id}"
          ));
        };
        target_traces
          .entry(id)
          .or_default()
          .push(ConsumerDeliveryTrace {
            member_id: "bootstrap-restart".to_string(),
            virtual_partition_id: record.virtual_partition_id,
            offset: record.offset,
          });
        restarted_consumer.store_offset(record.virtual_partition_id, record.offset)?;
        restarted_consumer.commit().await?;
      },
    }
  }
  assert!(
    replayed_phase1.is_empty(),
    "restarted bootstrap consumer replayed committed phase 1 data: {replayed_phase1:?}"
  );
  assert_eq!(
    phase2_traces.keys().cloned().collect::<HashSet<_>>(),
    phase2_expected
  );
  assert!(
    delivery_counts(&phase2_traces)
      .values()
      .all(|count| *count == 1),
    "bootstrap restart phase 2 must be exactly once: {phase2_traces:?}"
  );
  let phase2_offsets = maximum_delivery_offsets(&phase2_traces);
  wait_for_group_offsets_committed(
    &cluster,
    &phase2_offsets,
    "bootstrap phase 2 cursors did not durably commit",
  )
  .await?;

  Box::new(restarted_consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

// High-level: validates bootstrap scale-in by recovering records published for the crashed
// member's assigned partitions after deterministic membership and lease expiry.
#[tokio::test]
async fn bootstrap_dynamic_membership_scale_in_after_expiry() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(OffsetDateTime::now_utc()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .blob_store(resources.s3_blob_store())
    .start()
    .await?;

  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;
  let membership_store = resources.consumer_membership_store();
  let hooks = cluster.lifecycle_hooks();
  let config_a = consumer_bootstrap_config("bootstrap-a", &resources);
  let config_b = consumer_bootstrap_config("bootstrap-b", &resources);

  // Bootstrap construction performs an initial rebalance, so A must be fully assigned before B
  // exists. Otherwise, concurrent bootstrap can make A's scale-out revocation scheduler-dependent.
  let mut consumer_a = Box::new(
    ConsumerBootstrapIteratorBuilder::new(
      ConsumerBootstrapConfig::from_proto_config(&config_a)?,
      metrics_scope("blob_stream_consumer_it"),
      None,
    )
    .time_provider(consumer_time.clone())
    .lifecycle_hooks(Arc::new(hooks.clone()))
    .build()
    .await?,
  );

  let initial_leases = resources
    .consumer_lease_store()
    .list_group_leases(TOPIC, "integration-group")
    .await?;
  assert_eq!(initial_leases.len(), PARTITION_COUNT as usize);
  assert!(
    initial_leases
      .iter()
      .all(|lease| lease.owner_id == "bootstrap-a"),
    "bootstrap member A did not own every initial partition: {initial_leases:?}"
  );

  let mut initial_revocation_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRevocationEmitted,
      "bootstrap-a",
      None,
      None,
    )
    .await?;
  let mut bootstrap_b_rebalance_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRebalanceApplied,
      "bootstrap-b",
      None,
      None,
    )
    .await?;
  let mut consumer_b = Box::new(
    ConsumerBootstrapIteratorBuilder::new(
      ConsumerBootstrapConfig::from_proto_config(&config_b)?,
      metrics_scope("blob_stream_consumer_it"),
      None,
    )
    .time_provider(consumer_time.clone())
    .lifecycle_hooks(Arc::new(hooks.clone()))
    .build()
    .await?,
  );
  consumer_a.start()?;
  consumer_b.start()?;
  consumer_time.advance(TimeDuration::seconds(1));
  timeout(
    Duration::from_secs(5),
    initial_revocation_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("bootstrap member A did not revoke partitions after scale-out"))??;
  initial_revocation_gate.release()?;

  let initial_revocation = timeout(Duration::from_secs(5), consumer_a.next())
    .await
    .map_err(|_| anyhow!("bootstrap member A did not surface its scale-out revocation"))??;
  let NextResult::Revoked(initial_revocation) = initial_revocation else {
    return Err(anyhow!(
      "bootstrap member A delivered a record before its scale-out revocation"
    ));
  };
  initial_revocation.complete().await;
  consumer_a.commit().await?;
  consumer_b.commit().await?;

  // A's revocation is applied immediately, while B's next rebalance is scheduled against the
  // manual clock and held at the member-scoped assignment boundary.
  consumer_time.advance(TimeDuration::seconds(1));
  timeout(
    Duration::from_secs(5),
    bootstrap_b_rebalance_gate.wait_until_reached(),
  )
  .await
  .map_err(|_| anyhow!("bootstrap member B did not apply its scale-out assignment"))??;
  bootstrap_b_rebalance_gate.release()?;
  let crashed_partitions = resources
    .consumer_lease_store()
    .list_group_leases(TOPIC, "integration-group")
    .await?
    .into_iter()
    .filter(|lease| lease.owner_id == "bootstrap-b")
    .map(|lease| lease.key.virtual_partition_id)
    .collect::<Vec<_>>();
  assert!(
    !crashed_partitions.is_empty(),
    "bootstrap member B did not acquire partitions before its crash"
  );

  let mut post_crash_ids = HashSet::new();
  for partition_id in &crashed_partitions {
    let key = (0 .. 4_096)
      .find_map(|key_index| {
        let key = format!("it-014-key-{partition_id}-{key_index}").into_bytes();
        let mapped_partition = virtual_partition_for_logical(
          logical_partition_for_key(&key, PARTITION_COUNT),
          PARTITION_COUNT,
          0,
        );
        (mapped_partition == *partition_id).then_some(key)
      })
      .ok_or_else(|| anyhow!("could not construct key for crashed partition {partition_id}"))?;
    let id = format!("it-014-post-crash-{partition_id}");
    let ack = produce_message(&producer, key, &id).await?;
    assert_eq!(ack.virtual_partition_id, *partition_id);
    post_crash_ids.insert(id);
  }

  // B never polls this phase. Its prefetch worker may observe it, but only A can surface it after
  // the abrupt loss and normal membership/lease expiry takeover.
  let mut recovery_assignment_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRebalanceApplied,
      "bootstrap-a",
      None,
      None,
    )
    .await?;
  let mut recovery_rebalance_gate = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerBeforeRebalance,
      "bootstrap-a",
      None,
      None,
    )
    .await?;
  let mut recovery_prefetch_gates = Vec::with_capacity(crashed_partitions.len());
  for partition_id in &crashed_partitions {
    recovery_prefetch_gates.push(hooks.arm_prefetch_for_partition(*partition_id).await?);
  }
  consumer_b.abort_for_test().await?;
  drop(consumer_b);

  framework::advance_manual_time_until_lifecycle_gate(
    &consumer_time,
    &mut recovery_rebalance_gate,
    "surviving bootstrap member did not rebalance after membership expiry",
  )
  .await?;
  recovery_rebalance_gate.release()?;
  framework::advance_manual_time_until_lifecycle_gate(
    &consumer_time,
    &mut recovery_assignment_gate,
    "surviving bootstrap member did not apply its expiry assignment",
  )
  .await?;
  recovery_assignment_gate.release()?;

  // Assignment commands wake the prefetch worker directly. Each scoped gate proves the worker
  // processed the assignment and found data for the reclaimed partition.
  for mut prefetch_gate in recovery_prefetch_gates {
    framework::advance_manual_time_until_lifecycle_gate(
      &consumer_time,
      &mut prefetch_gate,
      "surviving bootstrap member did not prefetch a reclaimed partition",
    )
    .await?;
    prefetch_gate.release()?;
  }

  let mut recovered_counts = HashMap::new();
  timeout(Duration::from_secs(5), async {
    while recovered_counts.len() < post_crash_ids.len() {
      match consumer_a.next().await? {
        NextResult::Revoked(revoked) => revoked.complete().await,
        NextResult::Record(record) => {
          let id = String::from_utf8(record.record.payload.to_vec())?;
          if !post_crash_ids.contains(&id) {
            return Err(anyhow!(
              "surviving member delivered unexpected post-crash record: {id}"
            ));
          }
          *recovered_counts.entry(id).or_insert(0usize) += 1;
          consumer_a.store_offset(record.virtual_partition_id, record.offset)?;
          consumer_a.commit().await?;
        },
      }
    }
    Ok::<_, anyhow::Error>(())
  })
  .await
  .map_err(|_| anyhow!("surviving bootstrap member did not drain the post-crash phase"))??;
  assert!(recovered_counts.values().all(|count| *count == 1));

  let active_members = membership_store
    .list_active_members(TOPIC, "integration-group", consumer_time.now())
    .await?;
  assert_eq!(member_ids(&active_members), vec!["bootstrap-a".to_string()]);

  let leases = resources
    .consumer_lease_store()
    .list_group_leases(TOPIC, "integration-group")
    .await?;
  assert!(
    leases.iter().all(|lease| lease.owner_id == "bootstrap-a"),
    "surviving bootstrap member did not own every partition after scale-in: {leases:?}"
  );
  assert!(leases.iter().all(|lease| {
    !crashed_partitions.contains(&lease.key.virtual_partition_id)
      || lease
        .committed_cursor
        .as_ref()
        .is_some_and(|cursor| cursor.source_checkpoint.is_some())
  }));

  consumer_a.shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}
