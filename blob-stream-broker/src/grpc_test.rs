use super::{
  BrokerGrpc,
  BrokerGrpcMetrics,
  MAX_DECODED_PRODUCE_REQUEST_BYTES,
  produce_request_config,
};
use crate::write::{AdmissionController, TopicInfo, WriteConfig, WriteEngineImpl};
use anyhow::Result;
use bd_grpc::Handler;
use bd_server_stats::stats::Collector;
use bd_shutdown::ComponentShutdownTrigger;
use bd_time::TestTimeProvider;
use blob_stream_blob_store::InMemoryBlobStore;
use blob_stream_metadata_store::{InMemoryMetadataStore, InMemoryProducerPartitionLeaseStore};
use blob_stream_proto::protos::blobstream::v1::broker::{ProduceBatchRequest, ProduceStatus};
use blob_stream_types::new_record;
use http::{Extensions, HeaderMap};
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;

struct OverloadedAdmissionController;

impl AdmissionController for OverloadedAdmissionController {
  fn is_overloaded(&self) -> bool {
    true
  }
}

#[test]
fn limits_decoded_produce_request_bytes() {
  assert_eq!(
    produce_request_config().max_decoded_request_bytes,
    MAX_DECODED_PRODUCE_REQUEST_BYTES
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
  let scope = Collector::default().scope("blob_stream_broker_test");
  let engine = Arc::new(WriteEngineImpl::new(
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
    None,
    None,
    Arc::new(OverloadedAdmissionController),
    shutdown_trigger.make_handle(),
    Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH)),
    &scope,
  )?);
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
  shutdown_trigger.shutdown().await;
  Ok(())
}
