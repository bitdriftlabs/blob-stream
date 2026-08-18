use anyhow::{Result, anyhow};
use blob_stream_consumer::iterator::{ConsumerIterator, NextResult};
use blob_stream_integration_tests::test_framework::{
  self as framework,
  ClusterHarness,
  GatedMetadataStore,
  IntegrationResources,
  LifecycleEvent,
  TOPIC,
  TestConsumerReader,
  WINDOW_SIZE_SECONDS,
  broker_metadata_cache_reader,
  consume_next_record,
  consume_one_record,
  consumer_runtime_config,
  now_unix_seconds,
  produce_message,
  produce_message_at_manual_time,
  producer_config,
  producer_topic,
  producer_topic_named_with_partition_count,
  write_recovery_segment,
  write_recovery_segment_for_partitions,
};
use blob_stream_metadata_store::MetadataStore;
use blob_stream_types::ToProtoDuration;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::time::timeout;

#[tokio::test]
async fn broker_metadata_cache_delivers_real_broker_multi_group_initial_recovery() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::new(resources.metadata_store()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .metadata_store(metadata_store.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(producer_config(), vec![producer_topic()])
    .await?;

  let mut runtime_a = consumer_runtime_config("cache-member-a");
  runtime_a
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("cache consumer A group config missing"))?
    .group_id = "cache-group-a".into();
  let mut runtime_b = consumer_runtime_config("cache-member-b");
  runtime_b
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("cache consumer B group config missing"))?
    .group_id = "cache-group-b".into();

  produce_message(
    &producer,
    b"broker-cache-collapse".to_vec(),
    "cache-collapse",
  )
  .await?;
  let consumer_a = Box::new(
    cluster
      .create_broker_metadata_cache_consumer(&runtime_a, false)
      .await?,
  );
  let task_a = tokio::spawn(consume_one_record(*consumer_a));
  metadata_store.wait_for_first_scan().await;
  let consumer_b = Box::new(
    cluster
      .create_broker_metadata_cache_consumer(&runtime_b, false)
      .await?,
  );
  let task_b = tokio::spawn(consume_one_record(*consumer_b));
  timeout(Duration::from_secs(5), async {
    while cluster.metadata_cache_active_waiters().await < 2 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("second cached consumer did not join the in-flight refill"))?;
  metadata_store.release();

  let first_id = task_a
    .await
    .map_err(|error| anyhow!("cache consumer A task join error: {error}"))??;
  let second_id = task_b
    .await
    .map_err(|error| anyhow!("cache consumer B task join error: {error}"))??;
  assert_eq!(first_id, "cache-collapse");
  assert_eq!(second_id, "cache-collapse");

  assert!(
    metadata_store.scan_count() >= 1,
    "initial recovery must query metadata before delivering the record"
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_real_transport_uses_equal_fast_frontier() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::recording(resources.metadata_store()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(2)
    .metadata_store(metadata_store.clone())
    .start()
    .await?;
  let mut reader = broker_metadata_cache_reader(
    &cluster,
    &resources,
    Arc::clone(&metadata_store) as Arc<dyn MetadataStore>,
    true,
    TimeDuration::ZERO,
  )
  .await?;
  let now = now_unix_seconds();
  assert!(reader.read_available(now).await?.is_empty());
  let (window, initial_floor) = metadata_store
    .scan_requests()
    .await
    .into_iter()
    .filter_map(|(window, min_snowflake)| min_snowflake.map(|floor| (window, floor)))
    .max_by_key(|(window, _)| window.window_start_unix_seconds)
    .ok_or_else(|| anyhow!("initial Fast reader did not issue an active Tail scan"))?;
  metadata_store.clear_scan_requests().await;
  write_recovery_segment_for_partitions(
    resources.blob_store().as_ref(),
    metadata_store.as_ref(),
    window.window_start_unix_seconds,
    initial_floor.as_u64(),
    &[(0, 1, "equal-frontier-zero"), (1, 1, "equal-frontier-one")],
  )
  .await?;

  let initial_batches = reader.read_available(now).await?;
  assert_eq!(
    initial_batches.len(),
    2,
    "initial equal-frontier Tail read did not deliver both partitions; scans={:?}",
    metadata_store.scan_requests().await
  );
  metadata_store.clear_scan_requests().await;
  write_recovery_segment(
    resources.blob_store().as_ref(),
    metadata_store.as_ref(),
    1,
    window.window_start_unix_seconds,
    initial_floor.as_u64().saturating_add(1),
    2,
    "equal-frontier-later",
    window.window_start_unix_seconds.saturating_mul(1_000),
  )
  .await?;

  assert_eq!(reader.read_available(now).await?.len(), 1);
  assert!(
    metadata_store
      .scan_requests()
      .await
      .contains(&(window, Some(initial_floor))),
    "equal Fast frontiers must issue a Tail query at their shared inclusive bound"
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_real_transport_uses_lowest_mixed_fast_frontier() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::recording(resources.metadata_store()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(2)
    .metadata_store(metadata_store.clone())
    .start()
    .await?;
  let mut reader = broker_metadata_cache_reader(
    &cluster,
    &resources,
    Arc::clone(&metadata_store) as Arc<dyn MetadataStore>,
    true,
    TimeDuration::ZERO,
  )
  .await?;
  let now = now_unix_seconds();
  assert!(reader.read_available(now).await?.is_empty());
  let (window, initial_floor) = metadata_store
    .scan_requests()
    .await
    .into_iter()
    .filter_map(|(window, min_snowflake)| min_snowflake.map(|floor| (window, floor)))
    .max_by_key(|(window, _)| window.window_start_unix_seconds)
    .ok_or_else(|| anyhow!("initial Fast reader did not issue an active Tail scan"))?;
  metadata_store.clear_scan_requests().await;
  write_recovery_segment(
    resources.blob_store().as_ref(),
    metadata_store.as_ref(),
    0,
    window.window_start_unix_seconds,
    initial_floor.as_u64().saturating_add(100),
    1,
    "mixed-frontier-high",
    window.window_start_unix_seconds.saturating_mul(1_000),
  )
  .await?;
  write_recovery_segment(
    resources.blob_store().as_ref(),
    metadata_store.as_ref(),
    1,
    window.window_start_unix_seconds,
    initial_floor.as_u64(),
    1,
    "mixed-frontier-low",
    window.window_start_unix_seconds.saturating_mul(1_000),
  )
  .await?;

  assert_eq!(reader.read_available(now).await?.len(), 2);
  metadata_store.clear_scan_requests().await;
  write_recovery_segment(
    resources.blob_store().as_ref(),
    metadata_store.as_ref(),
    1,
    window.window_start_unix_seconds,
    initial_floor.as_u64().saturating_add(1),
    2,
    "mixed-frontier-later",
    window.window_start_unix_seconds.saturating_mul(1_000),
  )
  .await?;

  assert_eq!(reader.read_available(now).await?.len(), 1);
  assert!(
    metadata_store
      .scan_requests()
      .await
      .contains(&(window, Some(initial_floor))),
    "mixed Fast frontiers must collapse to the lowest inclusive Tail bound"
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_real_transport_defers_late_eventual_metadata() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::recording(resources.metadata_store()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(2)
    .metadata_store(metadata_store.clone())
    .metadata_cache_max_age(TimeDuration::ZERO)
    .start()
    .await?;
  let mut reader = broker_metadata_cache_reader(
    &cluster,
    &resources,
    Arc::clone(&metadata_store) as Arc<dyn MetadataStore>,
    false,
    TimeDuration::seconds(1),
  )
  .await?;
  let first_read_at = now_unix_seconds();
  let maturity_read_at = first_read_at.saturating_add(1);
  assert!(reader.read_available(first_read_at).await?.is_empty());
  let window = metadata_store
    .scan_requests()
    .await
    .into_iter()
    .filter_map(|(window, min_snowflake)| min_snowflake.map(|_| window))
    .max_by_key(|window| window.window_start_unix_seconds)
    .ok_or_else(|| anyhow!("initial eventual reader did not issue an active Tail scan"))?;
  metadata_store.clear_scan_requests().await;
  write_recovery_segment(
    resources.blob_store().as_ref(),
    metadata_store.as_ref(),
    0,
    window.window_start_unix_seconds,
    u64::MAX,
    1,
    "late-eventual-metadata",
    maturity_read_at.saturating_mul(1_000),
  )
  .await?;

  assert!(
    reader.read_available(first_read_at).await?.is_empty(),
    "late metadata must remain deferred before its declared publication time"
  );
  assert!(
    metadata_store
      .scan_requests()
      .await
      .iter()
      .any(|(scanned_window, min_snowflake)| {
        scanned_window == &window && min_snowflake.is_some()
      }),
    "zero cache age must refill the broker after the earlier empty observation"
  );
  let mut mature_batches = Vec::new();
  for read_at in maturity_read_at ..= maturity_read_at.saturating_add(2) {
    mature_batches = reader.read_available(read_at).await?;
    if !mature_batches.is_empty() {
      break;
    }
  }
  assert_eq!(
    mature_batches.len(),
    1,
    "late eventual metadata did not deliver after its visibility eligibility boundary; scans={:?}",
    metadata_store.scan_requests().await,
  );

  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

async fn assert_broker_metadata_cache_collapses_multi_group_fast_tail(
  strongly_consistent: bool,
) -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::gate_first_tail_scan(
    resources.metadata_store(),
  ));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .metadata_store(metadata_store.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(
      producer_config(),
      vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
    )
    .await?;
  let hooks = cluster.lifecycle_hooks();

  let mut runtime_a = consumer_runtime_config("cache-fast-member-a");
  runtime_a
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("cache Fast consumer A group config missing"))?
    .group_id = "cache-fast-group-a".into();
  let mut runtime_b = consumer_runtime_config("cache-fast-member-b");
  runtime_b
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("cache Fast consumer B group config missing"))?
    .group_id = "cache-fast-group-b".into();
  for runtime in [&mut runtime_a, &mut runtime_b] {
    let read = runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("cache Fast consumer read config missing"))?;
    read.metadata_visibility_delay = TimeDuration::ZERO.into_proto();
    read.strongly_consistent_metadata_reads = Some(strongly_consistent);
  }

  let mut initial_fast_a = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerInitialFastPathActive,
      "cache-fast-member-a",
      Some(0),
      None,
    )
    .await?;
  let mut initial_fast_b = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerInitialFastPathActive,
      "cache-fast-member-b",
      Some(0),
      None,
    )
    .await?;
  let mut consumer_a = cluster
    .create_broker_metadata_cache_consumer(&runtime_a, false)
    .await?;
  let mut consumer_b = cluster
    .create_broker_metadata_cache_consumer(&runtime_b, false)
    .await?;
  consumer_a.start()?;
  consumer_b.start()?;

  timeout(Duration::from_secs(5), initial_fast_a.wait_until_reached())
    .await
    .map_err(|_| anyhow!("Fast consumer A did not complete its initial scan"))??;
  timeout(Duration::from_secs(5), initial_fast_b.wait_until_reached())
    .await
    .map_err(|_| anyhow!("Fast consumer B did not complete its initial scan"))??;
  initial_fast_a.release()?;
  initial_fast_b.release()?;

  timeout(Duration::from_secs(5), metadata_store.wait_for_gated_scan())
    .await
    .map_err(|_| anyhow!("Fast consumers did not start their Tail scan"))?;
  timeout(Duration::from_secs(5), async {
    while cluster.metadata_cache_active_waiters().await < 2 {
      tokio::task::yield_now().await;
    }
  })
  .await
  .map_err(|_| anyhow!("Fast consumers did not join the in-flight Tail refill"))?;
  produce_message(
    &producer,
    b"cache-fast-collapse".to_vec(),
    "cache-fast-collapse",
  )
  .await?;
  metadata_store.release();

  let first_id = consume_next_record(&mut consumer_a).await?;
  let second_id = consume_next_record(&mut consumer_b).await?;
  assert_eq!(first_id, "cache-fast-collapse");
  assert_eq!(second_id, "cache-fast-collapse");
  assert_eq!(
    metadata_store.gated_scan_count(),
    1,
    "compatible Fast Tail reads must share one broker metadata scan"
  );
  Box::new(consumer_a).shutdown().await?;
  Box::new(consumer_b).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_collapses_real_broker_multi_group_fast_tail() -> Result<()> {
  Box::pin(assert_broker_metadata_cache_collapses_multi_group_fast_tail(false)).await
}

#[tokio::test]
async fn broker_metadata_cache_collapses_real_broker_concurrent_strong_fast_tail() -> Result<()> {
  Box::pin(assert_broker_metadata_cache_collapses_multi_group_fast_tail(true)).await
}

#[tokio::test]
async fn broker_metadata_cache_shadow_oracle_uses_direct_authority() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::new(resources.metadata_store()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .metadata_store(metadata_store.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(
      producer_config(),
      vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
    )
    .await?;
  let mut runtime = consumer_runtime_config("cache-shadow-member");
  runtime
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("shadow consumer group config missing"))?
    .group_id = "cache-shadow-group".into();
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("shadow consumer read config missing"))?
    .metadata_visibility_delay = TimeDuration::ZERO.into_proto();

  let mut consumer = cluster
    .create_broker_metadata_cache_consumer(&runtime, true)
    .await?;
  consumer.start()?;
  timeout(Duration::from_secs(5), metadata_store.wait_for_first_scan())
    .await
    .map_err(|_| anyhow!("shadow consumer did not begin its broker metadata scan"))?;
  assert_eq!(
    cluster.metadata_cache_active_waiters().await,
    1,
    "shadow mode must issue a broker metadata query before its direct oracle scan"
  );
  produce_message(&producer, b"cache-shadow".to_vec(), "cache-shadow").await?;
  metadata_store.release();

  assert_eq!(consume_next_record(&mut consumer).await?, "cache-shadow");
  assert_eq!(
    metadata_store.scan_count(),
    4,
    "shadow mode must compare broker and direct-authoritative results for both initial windows"
  );

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_unavailable_owner_falls_back_to_direct_metadata() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::new(resources.metadata_store()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .metadata_store(metadata_store.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(
      producer_config(),
      vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
    )
    .await?;
  let metadata_discovery = framework::DynamicBrokerDiscovery::new(Vec::new());
  let mut runtime = consumer_runtime_config("cache-owner-loss-member");
  runtime
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("owner-loss consumer group config missing"))?
    .group_id = "cache-owner-loss-group".into();
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("owner-loss consumer read config missing"))?
    .metadata_visibility_delay = TimeDuration::ZERO.into_proto();

  let mut consumer = cluster
    .create_broker_metadata_cache_consumer_with_discovery(
      &runtime,
      false,
      Arc::new(metadata_discovery.clone()),
    )
    .await?;
  consumer.start()?;
  timeout(Duration::from_secs(5), metadata_store.wait_for_first_scan())
    .await
    .map_err(|_| anyhow!("unavailable-owner consumer did not begin direct recovery"))?;
  assert_eq!(
    cluster.metadata_cache_active_waiters().await,
    0,
    "unavailable discovery owners must prevent broker cache admission"
  );
  produce_message(&producer, b"cache-owner-loss".to_vec(), "cache-owner-loss").await?;
  metadata_store.release();

  assert_eq!(
    consume_next_record(&mut consumer).await?,
    "cache-owner-loss"
  );

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_owner_churn_falls_back_to_direct_metadata() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::gate_first_tail_scan(
    resources.metadata_store(),
  ));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .metadata_store(metadata_store.clone())
    .start()
    .await?;
  let producer = cluster
    .create_producer(
      producer_config(),
      vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
    )
    .await?;
  let live_node = cluster
    .live_nodes()
    .into_iter()
    .next()
    .ok_or_else(|| anyhow!("owner churn test expected one live broker"))?;
  let metadata_discovery = framework::DynamicBrokerDiscovery::new(vec![live_node]);
  let hooks = cluster.lifecycle_hooks();
  let mut runtime = consumer_runtime_config("cache-owner-churn-member");
  runtime
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("owner churn consumer group config missing"))?
    .group_id = "cache-owner-churn-group".into();
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("owner churn consumer read config missing"))?
    .metadata_visibility_delay = TimeDuration::ZERO.into_proto();

  let mut initial_fast = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerInitialFastPathActive,
      "cache-owner-churn-member",
      Some(0),
      None,
    )
    .await?;
  let mut consumer = cluster
    .create_broker_metadata_cache_consumer_with_discovery(
      &runtime,
      false,
      Arc::new(metadata_discovery.clone()),
    )
    .await?;
  consumer.start()?;
  timeout(Duration::from_secs(5), initial_fast.wait_until_reached())
    .await
    .map_err(|_| anyhow!("owner churn consumer did not complete its initial scan"))??;

  metadata_discovery.update_nodes(Vec::new());
  produce_message(
    &producer,
    b"cache-owner-churn".to_vec(),
    "cache-owner-churn",
  )
  .await?;
  initial_fast.release()?;
  timeout(Duration::from_secs(5), metadata_store.wait_for_gated_scan())
    .await
    .map_err(|_| anyhow!("owner churn consumer did not begin its direct Tail scan"))?;
  assert_eq!(
    cluster.metadata_cache_active_waiters().await,
    0,
    "removed metadata owner must prevent broker cache admission for the next Tail read"
  );
  metadata_store.release();

  assert_eq!(
    consume_next_record(&mut consumer).await?,
    "cache-owner-churn"
  );

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_deadline_falls_back_to_direct_metadata() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::gate_first_tail_scan(
    resources.metadata_store(),
  ));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .metadata_store(metadata_store.clone())
    .metadata_cache_timing(
      TimeDuration::milliseconds(10),
      TimeDuration::milliseconds(100),
    )
    .start()
    .await?;
  let producer = cluster
    .create_producer(
      producer_config(),
      vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
    )
    .await?;
  let hooks = cluster.lifecycle_hooks();
  let mut runtime = consumer_runtime_config("cache-deadline-member");
  runtime
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("cache-deadline consumer group config missing"))?
    .group_id = "cache-deadline-group".into();
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("cache-deadline consumer read config missing"))?
    .metadata_visibility_delay = TimeDuration::ZERO.into_proto();

  let mut initial_fast = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerInitialFastPathActive,
      "cache-deadline-member",
      Some(0),
      None,
    )
    .await?;
  let mut consumer = cluster
    .create_broker_metadata_cache_consumer(&runtime, false)
    .await?;
  consumer.start()?;
  timeout(Duration::from_secs(5), initial_fast.wait_until_reached())
    .await
    .map_err(|_| anyhow!("cache-deadline consumer did not complete its initial scan"))??;
  produce_message(&producer, b"cache-deadline".to_vec(), "cache-deadline").await?;
  initial_fast.release()?;

  timeout(Duration::from_secs(5), metadata_store.wait_for_gated_scan())
    .await
    .map_err(|_| anyhow!("cache-deadline broker Tail scan did not begin"))?;
  assert_eq!(
    cluster.metadata_cache_active_waiters().await,
    1,
    "held broker Tail request must occupy one cache waiter before timing out"
  );
  assert_eq!(consume_next_record(&mut consumer).await?, "cache-deadline");
  assert!(
    metadata_store.scan_count() >= 2,
    "deadline fallback must execute a direct metadata scan while the broker scan is held"
  );
  metadata_store.release();

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_pressure_reloads_evicted_tail_entry() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let metadata_store = Arc::new(GatedMetadataStore::gate_first_tail_scan(
    resources.metadata_store(),
  ));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .metadata_store(metadata_store.clone())
    .metadata_cache_max_bytes(1)
    .start()
    .await?;
  let producer = cluster
    .create_producer(
      producer_config(),
      vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
    )
    .await?;
  let hooks = cluster.lifecycle_hooks();
  let mut runtime = consumer_runtime_config("cache-pressure-member");
  runtime
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("cache-pressure consumer group config missing"))?
    .group_id = "cache-pressure-group".into();
  runtime
    .read
    .as_mut()
    .ok_or_else(|| anyhow!("cache-pressure consumer read config missing"))?
    .metadata_visibility_delay = TimeDuration::ZERO.into_proto();

  let mut initial_fast = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerInitialFastPathActive,
      "cache-pressure-member",
      Some(0),
      None,
    )
    .await?;
  let mut consumer = cluster
    .create_broker_metadata_cache_consumer(&runtime, false)
    .await?;
  consumer.start()?;
  timeout(Duration::from_secs(5), initial_fast.wait_until_reached())
    .await
    .map_err(|_| anyhow!("cache-pressure consumer did not complete its initial scan"))??;

  produce_message(
    &producer,
    b"cache-pressure".to_vec(),
    "cache-pressure-first",
  )
  .await?;
  initial_fast.release()?;
  timeout(Duration::from_secs(5), metadata_store.wait_for_gated_scan())
    .await
    .map_err(|_| anyhow!("cache-pressure first Tail refill did not begin"))?;
  assert_eq!(
    cluster.metadata_cache_active_waiters().await,
    1,
    "first Tail request must be admitted by the broker cache"
  );
  metadata_store.release();
  assert_eq!(
    consume_next_record(&mut consumer).await?,
    "cache-pressure-first"
  );

  metadata_store.arm_next_scan();
  produce_message(
    &producer,
    b"cache-pressure".to_vec(),
    "cache-pressure-second",
  )
  .await?;
  timeout(
    Duration::from_secs(5),
    metadata_store.wait_for_gated_scan_count(2),
  )
  .await
  .map_err(|_| anyhow!("evicted Tail entry was not reloaded through the broker cache"))?;
  assert_eq!(
    cluster.metadata_cache_active_waiters().await,
    1,
    "an over-budget Tail generation must reload rather than use retained coverage"
  );
  metadata_store.release();
  assert_eq!(
    consume_next_record(&mut consumer).await?,
    "cache-pressure-second"
  );

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_delivers_active_recovery_after_restart() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .start()
    .await?;
  let producer = cluster
    .create_producer(
      producer_config(),
      vec![producer_topic_named_with_partition_count(TOPIC, 1, 1)],
    )
    .await?;
  let mut runtime_a = consumer_runtime_config("cache-recovery-member-a");
  runtime_a
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("cache recovery consumer A group config missing"))?
    .group_id = "cache-recovery-group".into();
  let mut runtime_b = consumer_runtime_config("cache-recovery-member-b");
  runtime_b
    .group
    .as_mut()
    .ok_or_else(|| anyhow!("cache recovery consumer B group config missing"))?
    .group_id = "cache-recovery-group".into();
  for runtime in [&mut runtime_a, &mut runtime_b] {
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("cache recovery consumer read config missing"))?
      .metadata_visibility_delay = TimeDuration::ZERO.into_proto();
  }

  produce_message(
    &producer,
    b"cache-recovery".to_vec(),
    "cache-recovery-checkpoint",
  )
  .await?;
  let mut first_consumer = cluster
    .create_broker_metadata_cache_consumer(&runtime_a, false)
    .await?;
  first_consumer.start()?;
  let checkpoint = loop {
    match first_consumer.next().await? {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => break record,
    }
  };
  assert_eq!(
    String::from_utf8(checkpoint.record.payload.to_vec())?,
    "cache-recovery-checkpoint"
  );
  first_consumer.store_offset(checkpoint.virtual_partition_id, checkpoint.offset)?;
  first_consumer.commit().await?;
  Box::new(first_consumer).shutdown().await?;

  produce_message(
    &producer,
    b"cache-recovery".to_vec(),
    "cache-recovery-pending",
  )
  .await?;
  let mut replacement_consumer = cluster
    .create_broker_metadata_cache_consumer(&runtime_b, false)
    .await?;
  replacement_consumer.start()?;
  assert_eq!(
    consume_next_record(&mut replacement_consumer).await?,
    "cache-recovery-pending",
    "active recovery must not replay the committed source checkpoint"
  );

  Box::new(replacement_consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}

#[tokio::test]
async fn broker_metadata_cache_recovers_retained_historical_windows() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let consumer_time = Arc::new(framework::ManualTimeProvider::new(
    OffsetDateTime::from_unix_timestamp(1_700_200_000)?,
  ));
  let metadata_store = Arc::new(GatedMetadataStore::new(resources.metadata_store()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .metadata_store(metadata_store.clone())
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
  let mut runtime_a = consumer_runtime_config("cache-history-member-a");
  let mut runtime_b = consumer_runtime_config("cache-history-member-b");
  for runtime in [&mut runtime_a, &mut runtime_b] {
    let group = runtime
      .group
      .as_mut()
      .ok_or_else(|| anyhow!("cache history consumer group config missing"))?;
    group.group_id = "cache-history-group".into();
    runtime
      .read
      .as_mut()
      .ok_or_else(|| anyhow!("cache history consumer read config missing"))?
      .metadata_visibility_delay = TimeDuration::ZERO.into_proto();
  }

  produce_message_at_manual_time(
    &cluster,
    &producer,
    consumer_time.as_ref(),
    b"cache-history".to_vec(),
    "cache-history-checkpoint",
  )
  .await?;
  let mut first_consumer = cluster
    .create_broker_metadata_cache_consumer(&runtime_a, false)
    .await?;
  first_consumer.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  metadata_store.wait_for_gated_scan().await;
  assert!(
    cluster.metadata_cache_active_waiters().await >= 1,
    "initial recovery must be admitted by the broker metadata cache"
  );
  metadata_store.release();
  let checkpoint = loop {
    match first_consumer.next().await? {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => break record,
    }
  };
  assert_eq!(
    String::from_utf8(checkpoint.record.payload.to_vec())?,
    "cache-history-checkpoint"
  );
  first_consumer.store_offset(checkpoint.virtual_partition_id, checkpoint.offset)?;
  first_consumer.commit().await?;
  Box::new(first_consumer).shutdown().await?;

  let historical_ids = HashSet::from([
    "cache-history-window-one".to_string(),
    "cache-history-window-two".to_string(),
  ]);
  for historical_id in &historical_ids {
    consumer_time.advance(TimeDuration::seconds(WINDOW_SIZE_SECONDS + 1));
    produce_message_at_manual_time(
      &cluster,
      &producer,
      consumer_time.as_ref(),
      b"cache-history".to_vec(),
      historical_id,
    )
    .await?;
  }

  metadata_store.arm_next_scan();
  let mut replacement_consumer = cluster
    .create_broker_metadata_cache_consumer(&runtime_b, false)
    .await?;
  replacement_consumer.start()?;
  consumer_time.advance(TimeDuration::milliseconds(200));
  metadata_store.wait_for_gated_scan_count(2).await;
  assert!(
    cluster.metadata_cache_active_waiters().await >= 1,
    "historical recovery must query metadata through the broker cache"
  );
  metadata_store.release();

  let mut delivered_ids = HashSet::new();
  while delivered_ids.len() < historical_ids.len() {
    match replacement_consumer.next().await? {
      NextResult::Revoked(revoked) => revoked.complete().await,
      NextResult::Record(record) => {
        let id = String::from_utf8(record.record.payload.to_vec())?;
        assert_ne!(
          id, "cache-history-checkpoint",
          "historical recovery replayed the committed source checkpoint"
        );
        assert!(
          historical_ids.contains(&id),
          "historical recovery delivered an unexpected record: {id}"
        );
        assert!(
          delivered_ids.insert(id),
          "historical recovery redelivered a record"
        );
        replacement_consumer.store_offset(record.virtual_partition_id, record.offset)?;
      },
    }
  }
  replacement_consumer.commit().await?;
  Box::new(replacement_consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}
