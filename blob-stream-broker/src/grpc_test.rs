use super::{
  BrokerGrpc,
  BrokerGrpcMetrics,
  MAX_CONCURRENT_BATCHES_PER_GROUPED_REQUEST,
  produce_request_config,
};
use crate::write::{AdmissionController, TopicInfo, WriteConfig, WriteEngineBuilder};
use anyhow::Result;
use async_trait::async_trait;
use bd_grpc::Handler;
use bd_server_stats::stats::Collector;
use bd_shutdown::ComponentShutdownTrigger;
use blob_stream_blob_store::InMemoryBlobStore;
use blob_stream_metadata_store::{InMemoryMetadataStore, InMemoryProducerPartitionLeaseStore};
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchRequest,
  ProduceBatchesRequest,
  ProduceStatus,
};
use blob_stream_test_utils::ManualTimeProvider;
use blob_stream_types::{MAX_PRODUCE_BATCHES_REQUEST_BYTES, SeqRange, new_record};
use http::{Extensions, HeaderMap};
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::{Semaphore, mpsc};

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

#[test]
fn limits_decoded_produce_request_bytes() {
  assert_eq!(
    produce_request_config().max_decoded_request_bytes,
    MAX_PRODUCE_BATCHES_REQUEST_BYTES
  );
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
          retention_days: 7,
          max_metadata_publication_lag_ms: 30_000,
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
  let grpc = BrokerGrpc::new(engine, &scope);

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
  config.flush_max_delay_ms = 60_000;
  let engine = Arc::new(
    WriteEngineBuilder::new(
      config,
      HashMap::from([(
        "telemetry".into(),
        TopicInfo {
          name: "telemetry".into(),
          partition_count: 1,
          num_writers: 1,
          retention_days: 7,
          max_metadata_publication_lag_ms: 30_000,
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
  let grpc = BrokerGrpc::new(engine, &scope);

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
          retention_days: 7,
          max_metadata_publication_lag_ms: 30_000,
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
  let grpc = BrokerGrpc::new(engine, &scope);
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
  let grpc = BrokerGrpc::new(Arc::new(PartialResponseWriteEngine), &scope);

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
async fn limits_concurrent_batches_per_grouped_request() -> Result<()> {
  let collector = Collector::default();
  let scope = collector.scope("blob_stream_broker_test");
  let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
  let release = Arc::new(Semaphore::new(0));
  let max_concurrent_batches = u32::try_from(MAX_CONCURRENT_BATCHES_PER_GROUPED_REQUEST)
    .expect("grouped batch concurrency limit fits in u32");
  let grpc = Arc::new(BrokerGrpc::new(
    Arc::new(GatedWriteEngine {
      entered_tx,
      release: Arc::clone(&release),
    }),
    &scope,
  ));
  let request = ProduceBatchesRequest {
    batches: (0 ..= max_concurrent_batches)
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

  for virtual_partition_id in 0 .. max_concurrent_batches {
    assert_eq!(entered_rx.recv().await, Some(virtual_partition_id));
  }
  assert!(entered_rx.try_recv().is_err());
  let metrics = String::from_utf8(collector.prometheus_output())?;
  assert!(metrics.contains("blob_stream_broker_test:grpc:active_batches 16"));

  release.add_permits(MAX_CONCURRENT_BATCHES_PER_GROUPED_REQUEST);
  assert_eq!(entered_rx.recv().await, Some(max_concurrent_batches));
  release.add_permits(1);

  assert_eq!(
    handle.await??.results.len(),
    MAX_CONCURRENT_BATCHES_PER_GROUPED_REQUEST + 1
  );
  Ok(())
}
