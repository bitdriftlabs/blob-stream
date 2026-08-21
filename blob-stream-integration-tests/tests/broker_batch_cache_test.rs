use anyhow::Result;
use async_trait::async_trait;
use blob_stream_blob_store::{BlobCacheAdmission, BlobKey, BlobStore, BlobStoreResult, ByteRange};
use blob_stream_consumer::iterator::ConsumerIterator;
use blob_stream_integration_tests::test_framework::{
  self as framework,
  ClusterHarness,
  IntegrationResources,
  StoreFaultAction,
  StoreFaultDomain,
  StoreFaultOperation,
  StoreFaultRule,
  consume_next_record,
  consumer_runtime_config,
  produce_message,
  producer_config,
  producer_topic_named_with_partition_count,
};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingBlobStore {
  inner: Arc<dyn BlobStore>,
  full_reads: AtomicUsize,
  range_reads: AtomicUsize,
}

impl CountingBlobStore {
  fn new(inner: Arc<dyn BlobStore>) -> Self {
    Self {
      inner,
      full_reads: AtomicUsize::new(0),
      range_reads: AtomicUsize::new(0),
    }
  }

  fn reset_reads(&self) {
    self.full_reads.store(0, Ordering::Relaxed);
    self.range_reads.store(0, Ordering::Relaxed);
  }
}

#[async_trait]
impl BlobStore for CountingBlobStore {
  async fn put(&self, key: &BlobKey, payload: Bytes) -> Result<()> {
    self.inner.put(key, payload).await
  }

  async fn get_range(&self, key: &BlobKey, range: ByteRange) -> BlobStoreResult<Bytes> {
    self.range_reads.fetch_add(1, Ordering::Relaxed);
    self.inner.get_range(key, range).await
  }

  async fn get_with_cache_admission(
    &self,
    key: &BlobKey,
    admission: &BlobCacheAdmission,
  ) -> BlobStoreResult<Bytes> {
    self.full_reads.fetch_add(1, Ordering::Relaxed);
    self.inner.get_with_cache_admission(key, admission).await
  }
}

#[tokio::test]
async fn broker_batch_cache_uses_real_transport_then_falls_back_after_owner_loss() -> Result<()> {
  let resources = IntegrationResources::create().await?;
  let blob_store = Arc::new(CountingBlobStore::new(resources.blob_store()));
  let mut cluster = ClusterHarness::builder(&resources, 1)
    .partition_count(1)
    .blob_store(Arc::clone(&blob_store) as Arc<dyn BlobStore>)
    .start()
    .await?;
  let producer = cluster
    .create_producer(
      producer_config(),
      vec![producer_topic_named_with_partition_count("telemetry", 1, 1)],
    )
    .await?;
  let discovery = framework::DynamicBrokerDiscovery::new(cluster.live_nodes());
  let mut runtime = consumer_runtime_config("broker-batch-cache-member");
  runtime
    .group
    .as_mut()
    .expect("test runtime has group configuration")
    .group_id = "broker-batch-cache-group".into();
  let mut consumer = cluster
    .create_broker_blob_cache_consumer_with_discovery(&runtime, Arc::new(discovery.clone()))
    .await?;
  consumer.start()?;

  produce_message(&producer, b"broker-batch-cache".to_vec(), "cached").await?;
  assert_eq!(consume_next_record(&mut consumer).await?, "cached");
  assert!(
    blob_store.full_reads.load(Ordering::Relaxed) > 0,
    "broker blob-range delivery must read a full immutable blob"
  );
  assert_eq!(
    blob_store.range_reads.load(Ordering::Relaxed),
    0,
    "successful broker blob-range delivery must not direct-read ranges"
  );

  blob_store.reset_reads();
  resources
    .store_fault_controller()
    .enable_fault(StoreFaultRule {
      domain: StoreFaultDomain::Blob,
      operation: StoreFaultOperation::BlobGetWithCacheAdmission,
      key_pattern: None,
      action: StoreFaultAction::Fail {
        message: "injected cache admission read failure".to_string(),
      },
      remaining_hits: Some(1),
    })
    .await;
  produce_message(
    &producer,
    b"broker-cache-fault".to_vec(),
    "fallback-after-fault",
  )
  .await?;
  assert_eq!(
    consume_next_record(&mut consumer).await?,
    "fallback-after-fault"
  );
  assert!(
    blob_store.range_reads.load(Ordering::Relaxed) > 0,
    "a cache admission fault must fall back to direct range reads"
  );
  assert!(resources.store_fault_controller().events().await.iter().any(|event| {
    event.operation == StoreFaultOperation::BlobGetWithCacheAdmission
      && event.action.as_ref().is_some_and(|action| {
        matches!(action, StoreFaultAction::Fail { message } if message == "injected cache admission read failure")
      })
  }));

  blob_store.reset_reads();
  discovery.update_nodes(Vec::new());
  produce_message(&producer, b"direct-fallback".to_vec(), "fallback").await?;
  assert_eq!(consume_next_record(&mut consumer).await?, "fallback");
  assert_eq!(
    blob_store.full_reads.load(Ordering::Relaxed),
    0,
    "removed broker owner must prevent cache reads"
  );
  assert!(
    blob_store.range_reads.load(Ordering::Relaxed) > 0,
    "removed broker owner must retry the complete group from direct storage"
  );

  Box::new(consumer).shutdown().await?;
  cluster.shutdown().await;
  resources.cleanup().await;
  Ok(())
}
