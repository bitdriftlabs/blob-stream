use super::{BrokerGrpc, BrokerGrpcMetrics, blob_read_request_config, produce_request_config};
use crate::read::blob_cache::{BlobCache, BlobCacheConfig, MAX_BLOB_READ_REQUEST_BYTES};
use crate::read::metadata_cache::{MetadataCache, MetadataCacheConfig};
use crate::write::memory_pressure::{MemoryPressureController, MemoryPressureSample};
use crate::write::{AdmissionController, TopicInfo, WriteConfig, WriteEngineBuilder};
use anyhow::Result;
use async_trait::async_trait;
use bd_grpc::Handler;
use bd_server_stats::stats::Collector;
use bd_server_stats::test::util::stats::Helper;
use bd_shutdown::ComponentShutdownTrigger;
use blob_stream_blob_store::{BlobStore, InMemoryBlobStore};
use blob_stream_metadata_store::{InMemoryMetadataStore, InMemoryProducerPartitionLeaseStore};
use blob_stream_proto::protos::blobstream::v1::broker::{
  BlobRangeRequest,
  BlobReadFailureStatus,
  ProduceBatchRequest,
  ProduceBatchesRequest,
  ProduceStatus,
  ReadBlobRangesRequest,
  read_blob_ranges_response,
};
use blob_stream_proto::protos::blobstream::v1::config::{BrokerConfig, RuntimeConfig, TopicConfig};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{MAX_PRODUCE_BATCHES_REQUEST_BYTES, SeqRange, ToProtoDuration, new_record};
use http::{Extensions, HeaderMap};
use prometheus::labels;
use std::collections::HashMap;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::sync::{Semaphore, mpsc};

fn metadata_cache() -> Arc<MetadataCache> {
  let mut runtime = RuntimeConfig::new();
  runtime.broker = Some(BrokerConfig::new()).into();
  let mut topic = TopicConfig::new();
  topic.name = "telemetry".into();
  topic.partition_count = 1;
  topic.num_writers = 1;
  topic.metadata_window_size = Duration::minutes(5).into_proto();
  runtime.topics.push(topic);
  let config = MetadataCacheConfig::from_runtime_config(&runtime, None)
    .expect("test metadata cache config is valid");
  Arc::new(MetadataCache::new(
    Arc::new(InMemoryMetadataStore::new()),
    config,
  ))
}

struct OverloadedAdmissionController;

impl AdmissionController for OverloadedAdmissionController {
  fn is_overloaded(&self) -> bool {
    true
  }
}

struct PartialResponseWriteEngine;

#[async_trait]
impl crate::write::WriteEngine for PartialResponseWriteEngine {
  async fn produce_batch(
    &self,
    request: crate::write::WriteRequest,
  ) -> Result<crate::write::WriteResponse, crate::write::WriteError> {
    if request.topic.as_str() == "unknown" {
      return Err(crate::write::WriteError::UnknownTopic(request.topic));
    }
    Ok(crate::write::WriteResponse {
      seq_range: SeqRange { start: 1, end: 1 },
    })
  }

  fn produce_request_timeout(&self) -> std::time::Duration {
    std::time::Duration::from_secs(1)
  }

  async fn state_snapshot(&self) -> crate::write::BrokerStateSnapshot {
    unreachable!("test write engine does not expose state")
  }
}

struct GatedWriteEngine {
  entered_tx: mpsc::UnboundedSender<u32>,
  release: Arc<Semaphore>,
}

struct FailingWriteEngine {
  fence_lost: bool,
}

#[async_trait]
impl crate::write::WriteEngine for GatedWriteEngine {
  async fn produce_batch(
    &self,
    request: crate::write::WriteRequest,
  ) -> Result<crate::write::WriteResponse, crate::write::WriteError> {
    self
      .entered_tx
      .send(request.virtual_partition_id)
      .map_err(|_| crate::write::WriteError::Overloaded("test receiver dropped".to_string()))?;
    self
      .release
      .acquire()
      .await
      .map_err(|_| crate::write::WriteError::Overloaded("test gate closed".to_string()))?
      .forget();
    Ok(crate::write::WriteResponse {
      seq_range: SeqRange { start: 1, end: 1 },
    })
  }

  fn produce_request_timeout(&self) -> std::time::Duration {
    std::time::Duration::from_secs(1)
  }

  async fn state_snapshot(&self) -> crate::write::BrokerStateSnapshot {
    unreachable!("test write engine does not expose state")
  }
}

#[async_trait]
impl crate::write::WriteEngine for FailingWriteEngine {
  async fn produce_batch(
    &self,
    _request: crate::write::WriteRequest,
  ) -> Result<crate::write::WriteResponse, crate::write::WriteError> {
    if self.fence_lost {
      Err(crate::write::WriteError::LeaseFenceLost)
    } else {
      Err(crate::write::WriteError::Internal(anyhow::anyhow!(
        "DynamoDB write failed for table private-metadata-table"
      )))
    }
  }

  fn produce_request_timeout(&self) -> std::time::Duration {
    std::time::Duration::from_secs(1)
  }

  async fn state_snapshot(&self) -> crate::write::BrokerStateSnapshot {
    unreachable!("test write engine does not expose state")
  }
}

#[test]
fn limits_produce_request_bytes() {
  assert_eq!(
    produce_request_config().max_request_bytes,
    MAX_PRODUCE_BATCHES_REQUEST_BYTES
  );
  assert_eq!(
    produce_request_config().max_decoded_request_bytes,
    MAX_PRODUCE_BATCHES_REQUEST_BYTES
  );
}

#[test]
fn limits_blob_read_request_bytes() {
  assert_eq!(
    blob_read_request_config().max_request_bytes,
    MAX_BLOB_READ_REQUEST_BYTES
  );
  assert_eq!(
    blob_read_request_config().max_decoded_request_bytes,
    MAX_BLOB_READ_REQUEST_BYTES
  );
}

#[tokio::test]
async fn serves_blob_ranges_from_the_grpc_handler() -> Result<()> {
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let scope = Collector::default().scope("blob_stream_broker_test");
  let store = Arc::new(InMemoryBlobStore::new());
  store
    .put(
      &blob_stream_blob_store::BlobKey::from("topic/blob"),
      bytes::Bytes::from_static(b"abcdefgh"),
    )
    .await?;
  let cache = Arc::new(BlobCache::new(
    store,
    BlobCacheConfig::from_broker_config(&BrokerConfig::new(), None)?,
    MemoryPressureController::new_for_test_with_sample(
      MemoryPressureSample {
        allocated_bytes: 0,
        limit_bytes: 1_000,
      },
      &scope,
    ),
    &scope,
  ));
  let grpc = BrokerGrpc::new_with_blob_cache(
    Arc::new(PartialResponseWriteEngine),
    metadata_cache(),
    cache,
    &scope,
  );

  let response = <BrokerGrpc as Handler<ReadBlobRangesRequest, _>>::handle(
    &grpc,
    HeaderMap::new(),
    Extensions::new(),
    ReadBlobRangesRequest {
      blob_key: "topic/blob".into(),
      ranges: vec![BlobRangeRequest {
        start: 2,
        end: 5,
        ..Default::default()
      }],
      ..Default::default()
    },
  )
  .await?;

  let Some(read_blob_ranges_response::Result::Success(success)) = response.result else {
    panic!("expected blob range success");
  };
  assert_eq!(success.ranges[0].payload, bytes::Bytes::from_static(b"cde"));
  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn grpc_blob_handler_preserves_typed_cache_failures() -> Result<()> {
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let scope = Collector::default().scope("blob_stream_broker_test");
  let cache = Arc::new(BlobCache::new(
    Arc::new(InMemoryBlobStore::new()),
    BlobCacheConfig::from_broker_config(&BrokerConfig::new(), None)?,
    MemoryPressureController::new_for_test_with_sample(
      MemoryPressureSample {
        allocated_bytes: 0,
        limit_bytes: 1_000,
      },
      &scope,
    ),
    &scope,
  ));
  let grpc = BrokerGrpc::new_with_blob_cache(
    Arc::new(PartialResponseWriteEngine),
    metadata_cache(),
    cache,
    &scope,
  );

  let invalid = <BrokerGrpc as Handler<ReadBlobRangesRequest, _>>::handle(
    &grpc,
    HeaderMap::new(),
    Extensions::new(),
    ReadBlobRangesRequest {
      blob_key: "topic/blob".into(),
      ranges: vec![BlobRangeRequest {
        start: 2,
        end: 2,
        ..Default::default()
      }],
      ..Default::default()
    },
  )
  .await?;
  let missing = <BrokerGrpc as Handler<ReadBlobRangesRequest, _>>::handle(
    &grpc,
    HeaderMap::new(),
    Extensions::new(),
    ReadBlobRangesRequest {
      blob_key: "topic/missing".into(),
      ranges: vec![BlobRangeRequest {
        start: 0,
        end: 1,
        ..Default::default()
      }],
      ..Default::default()
    },
  )
  .await?;

  let Some(read_blob_ranges_response::Result::Failure(invalid)) = invalid.result else {
    panic!("expected invalid range failure");
  };
  assert_eq!(
    invalid.status,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_BAD_REQUEST.into()
  );
  let Some(read_blob_ranges_response::Result::Failure(missing)) = missing.result else {
    panic!("expected missing blob failure");
  };
  assert_eq!(
    missing.status,
    BlobReadFailureStatus::BLOB_READ_FAILURE_STATUS_NOT_FOUND.into()
  );
  assert_eq!(missing.error_message.as_str(), "blob not found");
  shutdown_trigger.shutdown().await;
  Ok(())
}

#[test]
fn records_produce_request_timeouts() {
  let collector = Collector::default();
  let metrics = BrokerGrpcMetrics::new(&collector.scope("blob_stream_broker_test"));

  metrics.record_timeout();

  let output = String::from_utf8(collector.prometheus_output()).expect("metrics output is UTF-8");
  assert!(output.contains("blob_stream_broker_test:grpc:request_timeouts_total 1"));
}

#[tokio::test]
async fn sanitizes_internal_write_errors_and_preserves_fence_loss() -> Result<()> {
  let scope = Collector::default().scope("blob_stream_broker_test");
  let request = ProduceBatchRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    records: vec![new_record(vec![1], 1)],
    ..Default::default()
  };

  let internal_grpc = BrokerGrpc::new(
    Arc::new(FailingWriteEngine { fence_lost: false }),
    metadata_cache(),
    &scope,
  );
  let internal_response = internal_grpc
    .handle(HeaderMap::new(), Extensions::new(), request.clone())
    .await?;
  assert_eq!(
    internal_response.status,
    ProduceStatus::PRODUCE_STATUS_OVERLOADED.into()
  );
  assert_eq!(
    internal_response.error_message.as_str(),
    "internal write failure"
  );
  assert!(
    !internal_response
      .error_message
      .contains("private-metadata-table")
  );

  let fence_grpc = BrokerGrpc::new(
    Arc::new(FailingWriteEngine { fence_lost: true }),
    metadata_cache(),
    &scope,
  );
  let fence_response = fence_grpc
    .handle(HeaderMap::new(), Extensions::new(), request)
    .await?;
  assert_eq!(
    fence_response.status,
    ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER.into()
  );
  assert_eq!(
    fence_response.error_message.as_str(),
    "producer lease fence was lost"
  );
  Ok(())
}

#[tokio::test]
async fn returns_overloaded_when_admission_controller_rejects() -> Result<()> {
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let collector = Collector::default();
  let scope = collector.scope("blob_stream_broker_test");
  let engine = Arc::new(
    WriteEngineBuilder::new(
      WriteConfig::with_defaults(),
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: Duration::days(7),
          max_metadata_publication_lag: Duration::seconds(30),
          metadata_window_size: Duration::minutes(5),
        },
      )]),
      Arc::new(InMemoryBlobStore::new()),
      Arc::new(InMemoryMetadataStore::new()),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &scope,
    )
    .admission(Arc::new(OverloadedAdmissionController))
    .time_provider(Arc::new(ManualTimeProvider::new(
      OffsetDateTime::UNIX_EPOCH,
    )))
    .build()?,
  );
  let grpc = BrokerGrpc::new(engine, metadata_cache(), &scope);

  let response = grpc
    .handle(
      HeaderMap::new(),
      Extensions::new(),
      ProduceBatchRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1], 1)],
        ..Default::default()
      },
    )
    .await?;

  assert_eq!(
    response.status,
    ProduceStatus::PRODUCE_STATUS_OVERLOADED.into()
  );
  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(metrics.contains("blob_stream_broker_test:write:produce_rejected_records_total 1"));
  assert!(metrics.contains("blob_stream_broker_test:write:produce_rejected_payload_bytes_total 1"));
  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn successful_batches_record_accepted_write_volume() -> Result<()> {
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let collector = Collector::default();
  let scope = collector.scope("blob_stream_broker_test");
  let mut config = WriteConfig::with_defaults();
  config.flush_max_bytes = 1;
  config.flush_max_delay = Duration::seconds(60);
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: Duration::days(7),
          max_metadata_publication_lag: Duration::seconds(30),
          metadata_window_size: Duration::minutes(5),
        },
      )]),
      Arc::new(InMemoryBlobStore::new()),
      Arc::new(InMemoryMetadataStore::new()),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &scope,
    )
    .time_provider(Arc::new(ManualTimeProvider::new(
      OffsetDateTime::UNIX_EPOCH,
    )))
    .build()?,
  );
  let grpc = BrokerGrpc::new(engine, metadata_cache(), &scope);

  let response = grpc
    .handle(
      HeaderMap::new(),
      Extensions::new(),
      ProduceBatchRequest {
        topic: "telemetry".into(),
        virtual_partition_id: 0,
        records: vec![new_record(vec![1, 2, 3], 1)],
        ..Default::default()
      },
    )
    .await?;

  assert_eq!(response.status, ProduceStatus::PRODUCE_STATUS_OK.into());
  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(metrics.contains("blob_stream_broker_test:write:produce_records_total 1"));
  assert!(metrics.contains("blob_stream_broker_test:write:produce_payload_bytes_total 3"));
  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn empty_logical_batches_return_bad_request() -> Result<()> {
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let collector = Collector::default();
  let scope = collector.scope("blob_stream_broker_test");
  let engine = Arc::new(
    WriteEngineBuilder::new(
      WriteConfig::with_defaults(),
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention: Duration::days(7),
          max_metadata_publication_lag: Duration::seconds(30),
          metadata_window_size: Duration::minutes(5),
        },
      )]),
      Arc::new(InMemoryBlobStore::new()),
      Arc::new(InMemoryMetadataStore::new()),
      Arc::new(InMemoryProducerPartitionLeaseStore::new()),
      "test-node".to_string(),
      shutdown_trigger.make_handle(),
      &scope,
    )
    .time_provider(Arc::new(ManualTimeProvider::new(
      OffsetDateTime::UNIX_EPOCH,
    )))
    .build()?,
  );
  let grpc = BrokerGrpc::new(engine, metadata_cache(), &scope);
  let empty_batch = ProduceBatchRequest {
    topic: "telemetry".into(),
    virtual_partition_id: 0,
    ..Default::default()
  };

  let response = grpc
    .handle(HeaderMap::new(), Extensions::new(), empty_batch.clone())
    .await?;
  assert_eq!(
    response.status,
    ProduceStatus::PRODUCE_STATUS_BAD_REQUEST.into()
  );

  let response = <BrokerGrpc as Handler<ProduceBatchesRequest, _>>::handle(
    &grpc,
    HeaderMap::new(),
    Extensions::new(),
    ProduceBatchesRequest {
      batches: vec![empty_batch],
      ..Default::default()
    },
  )
  .await?;
  assert_eq!(response.results.len(), 1);
  assert_eq!(
    response.results[0].status,
    ProduceStatus::PRODUCE_STATUS_BAD_REQUEST.into()
  );

  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(metrics.contains("blob_stream_broker_test:grpc:responses_bad_request_total 2"));
  shutdown_trigger.shutdown().await;
  Ok(())
}

#[tokio::test]
async fn produces_batched_results_in_request_order() -> Result<()> {
  let scope = Collector::default().scope("blob_stream_broker_test");
  let grpc = BrokerGrpc::new(
    Arc::new(PartialResponseWriteEngine),
    metadata_cache(),
    &scope,
  );

  let response = <BrokerGrpc as Handler<ProduceBatchesRequest, _>>::handle(
    &grpc,
    HeaderMap::new(),
    Extensions::new(),
    ProduceBatchesRequest {
      batches: vec![
        ProduceBatchRequest {
          topic: "telemetry".into(),
          virtual_partition_id: 0,
          records: vec![new_record(vec![1], 1)],
          ..Default::default()
        },
        ProduceBatchRequest {
          topic: "unknown".into(),
          virtual_partition_id: 0,
          records: vec![new_record(vec![2], 2)],
          ..Default::default()
        },
      ],
      ..Default::default()
    },
  )
  .await?;

  assert_eq!(response.results.len(), 2);
  assert_eq!(
    response.results[0].status,
    ProduceStatus::PRODUCE_STATUS_OK.into()
  );
  assert_eq!(
    response.results[1].status,
    ProduceStatus::PRODUCE_STATUS_UNKNOWN_TOPIC.into()
  );
  Ok(())
}

#[tokio::test]
async fn starts_all_batches_in_grouped_request_concurrently() -> Result<()> {
  let collector = Collector::default();
  let metrics = Helper::new_with_collector(collector.clone());
  let scope = collector.scope("blob_stream_broker_test");
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let batch_count = 17;
  let grpc = Arc::new(BrokerGrpc::new(
    Arc::new(GatedWriteEngine {
      entered_tx,
      release: Arc::clone(&release),
    }),
    metadata_cache(),
    &scope,
  ));
  let request = ProduceBatchesRequest {
    batches: (0 .. batch_count)
      .map(|virtual_partition_id| ProduceBatchRequest {
        topic: "telemetry".into(),
        virtual_partition_id,
        records: vec![new_record(vec![1], 1)],
        ..Default::default()
      })
      .collect(),
    ..Default::default()
  };

  let grpc_task = Arc::clone(&grpc);
  let handle = tokio::spawn(async move {
    <BrokerGrpc as Handler<ProduceBatchesRequest, _>>::handle(
      grpc_task.as_ref(),
      HeaderMap::new(),
      Extensions::new(),
      request,
    )
    .await
  });

  for virtual_partition_id in 0 .. batch_count {
    assert_eq!(entered_rx.recv().await, Some(virtual_partition_id));
  }
  assert!(entered_rx.try_recv().is_err());
  metrics.assert_gauge_eq(
    17,
    "blob_stream_broker_test:grpc:active_batches",
    &labels!(),
  );

  release.add_permits(batch_count as usize);

  let response = handle.await??;
  assert_eq!(response.results.len(), batch_count as usize);
  assert!(
    response
      .results
      .iter()
      .all(|response| { response.status == ProduceStatus::PRODUCE_STATUS_OK.into() })
  );

  metrics.assert_gauge_eq(0, "blob_stream_broker_test:grpc:active_batches", &labels!());
  metrics.assert_histogram_count(
    1,
    "blob_stream_broker_test:grpc:grouped_request_batches",
    &labels!(),
  );
  metrics.assert_histogram_count(
    1,
    "blob_stream_broker_test:grpc:grouped_request_latency_seconds",
    &labels!(),
  );
  Ok(())
}
